"""
AstrBot ChatImitate Plugin - Main Module
"""

import asyncio
import random
import time

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, Face, Image, Plain, Reply
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
        """
        定期维护任务
        """
        last_cleanup_day: int | None = None

        # 从配置中读取保存间隔（秒）
        storage_config = self.config.get("storage", {})
        save_interval = storage_config.get("save_time_threshold", 300)

        while not self._stop_event.is_set():
            try:
                # 同步数据到数据库（频率由 save_interval 控制）
                await Chat.sync()
            except Exception:
                logger.warning("chatimitate: periodic sync failed", exc_info=True)

            # 每天执行一次过期数据清理
            today = int(time.strftime("%Y%m%d"))
            if last_cleanup_day != today:
                try:
                    cleanup_days = storage_config.get("cleanup_expired_days", 15)
                    await Chat.clearup_context(cleanup_days)
                    last_cleanup_day = today
                except Exception:
                    logger.warning("chatimitate: clearup_context failed", exc_info=True)

            # 等待配置的间隔时间
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=save_interval)
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
                message_chain = self._build_message_chain(msg)
                if message_chain:
                    await event.send(message_chain)
                    await asyncio.sleep(random.randint(1, 3))
        except Exception as e:
            logger.warning("chatimitate: learn/answer failed: %s", e, exc_info=True)

    def _build_message_chain(self, msg: str) -> MessageChain | None:
        """
        Args:
            msg: 消息字符串（可能是纯文本或多媒体标记）

        Returns:
            MessageChain 对象，包含消息组件
        """
        if not msg:
            return None

        components = []

        # 纯文本消息（不包含任何标记）
        if not msg.startswith("["):
            # 只发送非空的纯文本
            if msg.strip():
                components.append(Plain(msg))
                return MessageChain(components)
            return None

        # 处理多媒体消息标记
        # 格式：[类型：内容]
        if msg.startswith("[图片:"):
            # 图片消息，从标记中提取 URL 或 hash
            # 格式：[图片:url] 或 [图片:hash]
            image_url = msg[5:-1] if msg.endswith("]") else msg[5:]
            if image_url:
                # 如果是 http 或 https 开头，作为 URL 处理
                if image_url.startswith("http://") or image_url.startswith("https://"):
                    components.append(Image(file=image_url, url=image_url))
                else:
                    # 否则作为文件路径或 base64 处理
                    components.append(Image(file=image_url))
                return MessageChain(components)
            # 如果没有有效 URL，不发送任何内容
            return None
        elif msg.startswith("[语音]"):
            # 语音消息不发送占位符
            return None
        elif msg.startswith("[视频]"):
            # 视频消息不发送占位符
            return None
        elif msg.startswith("[文件]"):
            # 文件消息不发送占位符
            return None
        elif msg.startswith("[at:"):
            # At 消息，格式：[at:qq_id]
            qq_id = msg[4:-1]  # 提取 qq_id
            if qq_id == "all":
                from astrbot.api.message_components import AtAll

                components.append(AtAll())
            else:
                components.append(At(qq=qq_id))
            return MessageChain(components)
        elif msg.startswith("[face:"):
            # 表情消息，格式：[face:id]
            face_id = msg[6:-1]
            if face_id.isdigit():
                components.append(Face(id=int(face_id)))
                return MessageChain(components)
            return None
        else:
            # 未知格式，作为纯文本处理
            if msg.strip():
                components.append(Plain(msg))
                return MessageChain(components)
            return None

    async def _handle_admin_disable(self, event: AstrMessageEvent) -> bool:
        """
        处理管理员禁用回复命令

        使用场景：
        1. 管理员引用机器人的某条回复
        2. 发送包含"禁止"、"禁用"等关键词的消息
        3. 系统会在该群组中永久禁止这条具体的回复内容

        示例：
        - 管理员引用回复"今天天气不错"，然后说"禁止说这个"
        - 结果：机器人在该群不会再回复"今天天气不错"这句话
        """
        message_chain = event.get_messages()
        if not message_chain:
            return False

        # 提取引用回复和命令文本
        reply_component = None
        command_text = ""

        for comp in message_chain:
            if isinstance(comp, Reply):
                reply_component = comp
            elif isinstance(comp, Plain):
                command_text = comp.text.strip()

        # 必须包含引用回复
        if not reply_component:
            return False

        # 检查是否是禁用命令
        if not self._is_disable_command(command_text):
            return False

        # 验证管理员权限
        if not event.is_admin():
            await event.send(MessageChain([Plain("权限不足，只有管理员可以禁用回复")]))
            return False

        # 执行禁用
        try:
            await self._disable_reply(reply_component, event)
            await event.send(MessageChain([Plain("✅ 已禁用该回复")]))
            return True
        except Exception as e:
            logger.error("chatimitate: failed to disable reply: %s", e, exc_info=True)
            await event.send(MessageChain([Plain("❌ 禁用回复失败")]))
            return True

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
            "停用",
            "disable",
            "ban",
        ]
        text_lower = text.lower()
        return any(cmd in text_lower for cmd in disable_commands)

    async def _disable_reply(self, reply: Reply, event: AstrMessageEvent):
        """
        在数据库中禁用引用的回复

        逻辑：
        1. 获取被引用回复的完整内容（reply.message_str）
        2. 在数据库中查找包含这条回复的触发关键词
        3. 在该群组的禁用列表中添加这条回复
        """
        if not db.db_operations:
            raise Exception("数据库未初始化")

        group_id = event.get_group_id()

        # 获取被引用回复的完整内容
        # reply.message_str 是 Reply 组件提供的标准属性，包含被引用消息的纯文本
        reply_content = getattr(reply, "message_str", "")

        if not reply_content:
            raise ValueError("无法获取被引用回复的内容")

        # 查找包含这条回复的触发关键词
        context_id = await self._find_context_by_reply(reply_content)

        if not context_id:
            logger.warning(
                "chatimitate: 未找到包含回复 '%s' 的上下文，无法禁用",
                reply_content[:50],
            )
            return

        # 在数据库中禁用这条回复
        # 注意：我们禁用的是这条具体的回复内容，不是关键词
        await db.db_operations.disable_reply(
            context_id=context_id,
            keywords=reply_content,  # 直接使用回复内容本身作为标识
            group_id=group_id,
            reason="管理员禁用",
        )

        logger.info(
            "chatimitate: 在群组 %s 中禁用回复 '%s'",
            group_id,
            reply_content[:50],
        )

    async def _find_context_by_reply(self, reply_content: str) -> int | None:
        """
        根据回复内容查找对应的 context_id

        这是精确匹配，不是关键词匹配
        只要回复内容中包含指定的文本，就会找到对应的 context_id
        """
        if not db.db_operations:
            return None

        return await db.db_operations.find_context_by_reply(reply_content)
