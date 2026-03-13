"""
AstrBot ChatImitate Plugin - Main Module
"""

import asyncio
import random
import time

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, Face, Image, Plain, Reply
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

from . import db
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

        if await self._handle_admin_disable(event):
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
        import re

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
                    from astrbot.api.message_components import AtAll
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

    async def _handle_admin_disable(self, event: AstrMessageEvent) -> bool:
        """处理管理员禁用回复"""
        message_chain = event.get_messages()
        if not message_chain:
            return False

        reply_component = None
        command_text = ""

        for comp in message_chain:
            if isinstance(comp, Reply):
                reply_component = comp
            elif isinstance(comp, Plain):
                command_text = comp.text.strip()

        if not reply_component:
            return False

        if not self._is_disable_command(command_text):
            return False

        if not event.is_admin():
            await event.send(MessageChain([Plain("权限不足，只有管理员可以禁用回复")]))
            return False

        try:
            await self._disable_reply(reply_component, event)
            await event.send(MessageChain([Plain("已禁用该回复")]))
            return True
        except Exception as e:
            logger.error("chatimitate: failed to disable reply: %s", e, exc_info=True)
            await event.send(MessageChain([Plain("禁用回复失败")]))
            return True

    def _is_disable_command(self, text: str) -> bool:
        """检查是否是禁用命令"""
        disable_commands = [
            "禁止说这", "禁止说这个", "禁用这个", "禁用这",
            "不要说这个", "不许说这个", "禁止", "禁用",
            "屏蔽", "停用", "disable", "ban",
        ]
        text_lower = text.lower()
        return any(cmd in text_lower for cmd in disable_commands)

    async def _disable_reply(self, reply: Reply, event: AstrMessageEvent):
        """禁用引用的回复"""
        if not db.db_operations:
            raise Exception("数据库未初始化")

        group_id = event.get_group_id()
        reply_content = getattr(reply, "message_str", "")

        if not reply_content:
            raise ValueError("无法获取被引用回复的内容")

        context_id = await self._find_context_by_reply(reply_content)

        if not context_id:
            logger.warning("chatimitate: 未找到包含回复 '%s' 的上下文", reply_content[:50])
            return

        await db.db_operations.disable_reply(
            context_id=context_id,
            keywords=reply_content,
            group_id=group_id,
            reason="管理员禁用",
        )

        logger.info("chatimitate: 在群组 %s 中禁用回复 '%s'", group_id, reply_content[:50])

    async def _find_context_by_reply(self, reply_content: str) -> int | None:
        """根据回复内容查找 context_id"""
        if not db.db_operations:
            return None
        return await db.db_operations.find_context_by_reply(reply_content)
