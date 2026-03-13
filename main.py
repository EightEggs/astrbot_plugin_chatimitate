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
        self.context = context
        self.config = ChatImitateConfig(config)
        self._stop_event = asyncio.Event()
        self._bg_task: asyncio.Task | None = None

    async def initialize(self):
        """异步初始化"""
        if not self.config.validate():
            logger.error("chatimitate: 配置验证失败，请检查配置")
            return

        data_dir = StarTools.get_data_dir(self.name)
        await db.init_db(data_dir)
        self._bg_task = asyncio.create_task(self._periodic_maintenance())
        logger.info("chatimitate: plugin initialized")

    async def terminate(self):
        """插件销毁"""
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
        """定期维护任务"""
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
        """处理群消息"""
        if event.get_sender_id() == event.get_self_id():
            return

        # 处理管理员禁用命令
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

    def _build_message_chain(self, msg: str) -> MessageChain | None:
        """构建消息链"""
        if not msg:
            return None

        components = []

        if not msg.startswith("["):
            if msg.strip():
                components.append(Plain(msg))
                return MessageChain(components)
            return None

        if msg.startswith("[图片:"):
            match = re.match(r"\[图片:(.+?)\]$", msg)
            if match:
                image_url = match.group(1)
                if image_url.startswith("http://") or image_url.startswith("https://"):
                    components.append(Image(file=image_url, url=image_url))
                else:
                    components.append(Image(file=image_url))
                return MessageChain(components)
            return None
        elif msg.startswith("[语音]"):
            return None
        elif msg.startswith("[视频]"):
            return None
        elif msg.startswith("[文件]"):
            return None
        elif msg.startswith("[at:"):
            match = re.match(r"\[at:(.+?)\]$", msg)
            if match:
                qq_id = match.group(1)
                if qq_id == "all":
                    components.append(AtAll())
                else:
                    components.append(At(qq=qq_id))
                return MessageChain(components)
            return None
        elif msg.startswith("[face:"):
            match = re.match(r"\[face:(\d+)\]$", msg)
            if match:
                face_id = int(match.group(1))
                components.append(Face(id=face_id))
                return MessageChain(components)
            return None
        else:
            if msg.strip():
                components.append(Plain(msg))
                return MessageChain(components)
            return None
