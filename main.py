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
from astrbot.api.message_components import Image, Plain, Reply
from astrbot.api.star import Context, Star
from astrbot.core.message.message_event_result import MessageChain

from . import db
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

        # 检查是否是管理员在禁用回复
        if await self._handle_admin_disable(event):
            return

        chat = Chat(event, self.config)

        try:
            await chat.learn()

            # 直接使用 async for 遍历 answer() 生成器
            async for msg in chat.answer():
                message_chain = self._parse_message(msg)
                if message_chain:
                    await event.send(message_chain)
                    await asyncio.sleep(random.randint(1, 3))
        except Exception as e:
            logger.warning("chatimitate: learn/answer failed: %s", e, exc_info=True)

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

    async def _handle_admin_disable(self, event: AstrMessageEvent) -> bool:
        """
        处理管理员禁用回复命令

        场景：管理员引用机器人的回复，然后说"禁止说这" → 在该群禁用这条回复
        """
        message_chain = event.get_messages()
        if not message_chain:
            return False

        # 检查消息中是否包含引用回复组件
        reply_component = None
        command_text = ""

        for comp in message_chain:
            if isinstance(comp, Reply):
                reply_component = comp
            elif isinstance(comp, Plain):
                command_text = comp.text.strip()

        # 如果没有引用回复组件，不是禁用命令
        if not reply_component:
            return False

        # 检查命令文本是否是禁用指令
        if not self._is_disable_command(command_text):
            return False

        # 检查是否是管理员（使用 AstrBot API）
        if not event.is_admin():
            await event.send(MessageChain([Plain("权限不足，只有管理员可以禁用回复")]))
            return False

        # 执行禁用操作
        try:
            await self._disable_reply(reply_component, event)
            await event.send(MessageChain([Plain("✅ 已禁用该回复")]))
            return True
        except Exception as e:
            logger.error("chatimitate: failed to disable reply: %s", e, exc_info=True)
            await event.send(MessageChain([Plain("❌ 禁用回复失败")]))
            return True  # 返回 True 表示已经处理了这个命令

    def _is_disable_command(self, text: str) -> bool:
        """检查是否是禁用命令"""
        disable_commands = [
            "禁止说这",
            "禁止说这个",
            "禁用这个",
            "禁用这",
            "不要说这个",
            "不许说这个",
            "禁止",
            "禁用",
            "屏蔽",
            "阻止",
            "停用",
            "disable",
            "ban",
        ]
        text_lower = text.lower()
        return any(cmd in text_lower for cmd in disable_commands)

    async def _disable_reply(self, reply: Reply, event: AstrMessageEvent):
        """在数据库中禁用引用的回复"""
        if not db.db_operations:
            raise Exception("数据库未初始化")

        group_id = event.get_group_id()
        reply_message = str(getattr(reply, "text", "") or "")

        # 从 reply 对象中获取引用的消息 ID
        reply_id = getattr(reply, "id", None)

        # 方法 1：通过消息 ID 查找（如果 reply 对象包含此信息）
        if reply_id:
            # 尝试从数据库中查找包含此回复的 context_id
            # 这需要遍历所有 trigger_keywords 和它们的 replies
            # 为简化，我们使用关键词匹配

            # 从 trigger_keywords 表中查找所有记录
            # 然后检查它们的 reply_contents 是否包含该回复
            pass

        # 方法 2：使用回复内容作为关键词查找
        # 这是更可靠的方法
        if reply_message:
            # 从数据库中查找包含该回复的触发关键词
            # 由于我们不知道具体的 context_id，需要遍历查找
            # 这里我们使用一个简化的方法：
            # 1. 查找所有 trigger_keywords
            # 2. 对于每个 trigger_keyword，查找其 reply_contents
            # 3. 如果找到匹配的回复，记录 context_id 和 reply 的 keywords

            # 由于数据库操作的限制，我们使用一个更直接的方法：
            # 直接在禁用表中记录该回复内容，使用回复内容本身作为关键词
            # 这样在检查时，会匹配到相同的关键词并禁用

            # 获取回复的关键词（从消息内容提取）
            keywords = reply_message.strip()
            if keywords:
                # 首先尝试查找包含该回复的 context_id
                # 这里我们需要一个辅助方法来查找
                context_id = await self._find_context_by_reply(keywords)

                if context_id:
                    # 找到了对应的 context_id，禁用该回复
                    await db.db_operations.disable_reply(
                        context_id=context_id,
                        keywords=keywords,
                        group_id=group_id,
                        reason="管理员禁用",
                    )
                    logger.info(
                        "chatimitate: disabled reply '%s' in group %s (context_id=%s)",
                        keywords,
                        group_id,
                        context_id,
                    )
                else:
                    # 没有找到对应的 context_id，记录日志
                    logger.warning(
                        "chatimitate: could not find context for reply '%s' in group %s",
                        keywords,
                        group_id,
                    )

    async def _find_context_by_reply(self, reply_message: str) -> int | None:
        """
        根据回复内容查找对应的 context_id

        这是一个辅助方法，用于找到包含指定回复的触发关键词
        """
        if not db.db_operations:
            return None

        # 使用数据库提供的查找方法
        return await db.db_operations.find_context_by_reply(reply_message)
