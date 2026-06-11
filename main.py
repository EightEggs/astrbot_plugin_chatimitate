"""
AstrBot ChatImitate Plugin - Main Module
"""

import asyncio
import random
import re
import time

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, AtAll, Face, Image, Plain
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

from . import db
from .ban import ReplyBanner
from .config import ChatImitateConfig
from .model import Chat


class ChatImitatePlugin(Star):

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = ChatImitateConfig(config)
        self._stop_event = asyncio.Event()
        self._bg_task: asyncio.Task | None = None

    async def initialize(self):
        if not self.config.validate():
            logger.error("chatimitate: config validation failed, please check config")
            return

        data_dir = StarTools.get_data_dir(self.name)
        await db.init_db(data_dir)
        self._bg_task = asyncio.create_task(self._periodic_maintenance())
        logger.info("chatimitate: plugin initialized")

    async def terminate(self):
        self._stop_event.set()
        if self._bg_task:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning("chatimitate: bg task cancel failed: %s", e)

        try:
            await Chat.sync()
        except Exception as e:
            logger.warning("chatimitate: final sync failed: %s", e, exc_info=True)

        if db.db_manager:
            try:
                await db.db_manager.close()
            except Exception as e:
                logger.warning("chatimitate: db close failed: %s", e, exc_info=True)
        logger.info("chatimitate: plugin terminated")

    async def _periodic_maintenance(self):
        last_cleanup_day: int | None = None

        while not self._stop_event.is_set():
            try:
                await Chat.sync()
            except Exception:
                logger.warning("chatimitate: periodic sync failed", exc_info=True)

            today = int(time.strftime("%Y%m%d"))
            if last_cleanup_day != today:
                try:
                    await Chat.clearup_context(self.config.cleanup_expired_days)
                    last_cleanup_day = today
                except Exception:
                    logger.warning("chatimitate: clearup_context failed", exc_info=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.config.save_time_threshold)
            except asyncio.TimeoutError:
                continue

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        if event.get_sender_id() == event.get_self_id():
            return

        disable_result = await ReplyBanner.handle_admin_disable(event)
        if disable_result.handled:
            return

        chat = Chat(event, self.config)

        try:
            await chat.learn()
            async for msg in chat.answer():
                message_chain = self._build_message_chain(msg)
                if message_chain:
                    await event.send(message_chain)
                    await asyncio.sleep(random.randint(1, 3))
        except Exception as e:
            logger.warning("chatimitate: learn/answer failed: %s", e, exc_info=True)

    _MSG_PATTERN = re.compile(
        r"(\[图片:(.+?)\]|\[at:(.+?)\]|\[face:(\d+)\]|\[语音\]|\[视频\]|\[文件\])"
    )

    def _build_message_chain(self, msg: str) -> MessageChain | None:
        if not msg:
            return None

        components = []
        last_end = 0

        for match in self._MSG_PATTERN.finditer(msg):
            start, end = match.span()

            if start > last_end:
                text = msg[last_end:start]
                if text.strip():
                    components.append(Plain(text))

            component = self._parse_special_tag(match)
            if component:
                components.append(component)

            last_end = end

        if last_end < len(msg):
            remaining = msg[last_end:]
            if remaining.strip():
                components.append(Plain(remaining))

        return MessageChain(components) if components else None

    def _parse_special_tag(self, match: re.Match) -> object | None:
        full_match = match.group(0)

        if full_match.startswith("[图片:"):
            image_url = match.group(2).strip()
            if not image_url:
                return None
            if image_url.startswith(("http://", "https://")):
                return Image.fromURL(image_url)
            return None

        if full_match.startswith("[at:"):
            qq_id = match.group(3)
            return AtAll() if qq_id == "all" else At(qq=qq_id)

        if full_match.startswith("[face:"):
            return Face(id=int(match.group(4)))

        return None
