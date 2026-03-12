"""
AstrBot ChatImitate Plugin - Main Module
聊天模仿插件主入口 - 增强版，支持多媒体消息回复
"""

import asyncio
import random
import re
import time

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain
from astrbot.api.star import Context, Star
from astrbot.core.message.message_event_result import MessageChain

from .db import init_db
from .model import Chat


class ChatImitate(Star):
    """聊天模仿插件"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._stop_event = asyncio.Event()
        self._bg_task: asyncio.Task | None = None

    async def initialize(self):
        """异步初始化"""
        await init_db(self.name)

        try:
            await Chat.update_global_blacklist()
        except Exception:
            logger.warning("chatimitate: update_global_blacklist failed", exc_info=True)

        self._bg_task = asyncio.create_task(self._periodic_maintenance())

    async def terminate(self):
        """异步销毁"""
        self._stop_event.set()
        if self._bg_task is not None:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass

        try:
            await Chat.sync()
        except Exception:
            logger.warning("chatimitate: final sync failed", exc_info=True)

        try:
            from . import db as db_mod

            if db_mod.db_manager:
                await db_mod.db_manager.close()
                logger.debug("chatimitate: db connection closed")
        except Exception:
            logger.debug("chatimitate: db close failed", exc_info=True)

    async def _periodic_maintenance(self) -> None:
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
                    await Chat.clearup_context()
                    last_cleanup_day = today
                except Exception:
                    logger.warning("chatimitate: clearup_context failed", exc_info=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=3600)
            except asyncio.TimeoutError:
                continue

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """处理群消息"""
        if event.get_sender_id() == event.get_self_id():
            return

        chat = Chat(event, self.config)

        try:
            await chat.learn()
        except Exception:
            logger.warning("chatimitate: learn failed", exc_info=True)

        try:
            answers = await chat.answer()
        except Exception:
            logger.warning("chatimitate: answer failed", exc_info=True)
            return

        if not answers:
            return

        async for msg in answers:
            message_chain = self._parse_message(msg)
            if message_chain:
                await event.send(message_chain)
                await asyncio.sleep(random.randint(1, 3))

    def _parse_message(self, msg: str) -> MessageChain | None:
        """
        解析回复消息，支持 CQ 码和图片

        Args:
            msg: 消息字符串，可能包含 CQ 码

        Returns:
            MessageChain 对象，包含解析后的消息组件
        """
        if not msg:
            return None

        components = []

        # 检测是否包含 CQ 码
        if "[CQ:" not in msg:
            # 纯文本消息
            components.append(Plain(msg))
            return MessageChain(components)

        # 解析 CQ 码
        pattern = r"\[CQ:([^,\]]+)(?:,([^\]]*))?\]"
        last_end = 0

        for match in re.finditer(pattern, msg):
            # 添加 CQ 码之前的纯文本
            if match.start() > last_end:
                text = msg[last_end : match.start()].strip()
                if text:
                    components.append(Plain(text))

            cq_type = match.group(1)
            cq_params_str = match.group(2)

            # 解析 CQ 参数
            cq_params = {}
            if cq_params_str:
                for param in cq_params_str.split(","):
                    if "=" in param:
                        key, value = param.split("=", 1)
                        cq_params[key] = value

            # 根据类型创建对应的消息组件
            if cq_type == "image":
                file_url = cq_params.get("file", "")
                if file_url:
                    components.append(Image(file=file_url, url=file_url))
            elif cq_type == "face":
                face_id = cq_params.get("id", "")
                if face_id:
                    from astrbot.api.message_components import Face

                    components.append(Face(id=int(face_id)))
            elif cq_type == "record":
                file_path = cq_params.get("file", "")
                if file_path:
                    from astrbot.api.message_components import Record

                    components.append(Record(file=file_path))
            elif cq_type == "video":
                file_path = cq_params.get("file", "")
                if file_path:
                    from astrbot.api.message_components import Video

                    components.append(Video(file=file_path))
            elif cq_type == "at":
                qq = cq_params.get("qq", "")
                if qq:
                    from astrbot.api.message_components import At

                    if qq == "all":
                        components.append(At(qq="all"))
                    else:
                        components.append(At(qq=int(qq)))
            else:
                # 未知类型的 CQ 码，保留为纯文本
                components.append(Plain(match.group(0)))

            last_end = match.end()

        # 添加剩余的纯文本
        if last_end < len(msg):
            text = msg[last_end:].strip()
            if text:
                components.append(Plain(text))

        return MessageChain(components) if components else None
