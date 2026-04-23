"""
AstrBot ChatImitate Plugin - Ban/Disable Module
管理员禁用回复功能模块
"""

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import At, AtAll, Image, Plain, Reply
from astrbot.core.message.message_event_result import MessageChain

from . import db


class DisableStatus:
    """禁用操作状态枚举"""

    SUCCESS = "success"
    NOT_FOUND = "not_found"
    FAILED = "failed"


class DisableResult:
    """禁用处理结果"""

    def __init__(self, is_handled: bool, status: str | None = None):
        self.handled = is_handled
        self.status = status

    @classmethod
    def create_handled(cls, status: str | None = None) -> "DisableResult":
        return cls(True, status)

    @classmethod
    def create_not_handled(cls) -> "DisableResult":
        return cls(False, None)


class ReplyBanner:
    """回复禁用管理器"""

    # 精确匹配命令
    EXACT_COMMANDS = [
        "禁止",
        "禁用",
        "屏蔽",
        "停用",
        "disable",
        "ban",
    ]

    # 前缀匹配命令
    PREFIX_COMMANDS = [
        "禁止说",
        "禁止这个",
        "禁用这个",
        "不要说这个",
        "不许说这个",
    ]

    @classmethod
    async def handle_admin_disable(cls, event: AstrMessageEvent) -> DisableResult:
        """处理管理员禁用回复，返回明确的三态结果"""
        message_chain = event.get_messages()
        if not message_chain:
            return DisableResult.create_not_handled()

        reply_component = None
        command_parts = []

        for comp in message_chain:
            if isinstance(comp, Reply):
                reply_component = comp
            elif isinstance(comp, Plain):
                text = comp.text.strip()
                if text:
                    command_parts.append(text)

        if not reply_component:
            return DisableResult.create_not_handled()

        command_text = " ".join(command_parts)

        if not cls._is_disable_command(command_text):
            return DisableResult.create_not_handled()

        if not event.is_admin():
            await event.send(MessageChain([Plain("权限不足，只有管理员可以禁用回复")]))
            return DisableResult.create_handled()

        try:
            result = await cls._disable_reply(reply_component, event)
            if result == DisableStatus.SUCCESS:
                await event.send(MessageChain([Plain("已禁用该回复")]))
            elif result == DisableStatus.NOT_FOUND:
                await event.send(
                    MessageChain([Plain("未找到匹配的回复内容，无法禁用")])
                )
            else:
                await event.send(MessageChain([Plain("禁用回复失败")]))
            return DisableResult.create_handled()
        except Exception as e:
            logger.error("chatimitate: failed to disable reply: %s", e, exc_info=True)
            await event.send(MessageChain([Plain("禁用回复失败")]))
            return DisableResult.create_handled()

    @classmethod
    def _is_disable_command(cls, text: str) -> bool:
        """检查是否是禁用命令 - 使用精确匹配避免误触发"""
        if not text:
            return False

        text_lower = text.lower().strip()

        # 精确匹配
        for cmd in cls.EXACT_COMMANDS:
            if text_lower == cmd:
                return True

        # 前缀匹配
        for cmd in cls.PREFIX_COMMANDS:
            if text_lower.startswith(cmd):
                return True

        return False

    @classmethod
    async def _disable_reply(cls, reply: Reply, event: AstrMessageEvent) -> str:
        """禁用引用的回复，返回明确的状态"""
        if not db.db_operations:
            raise Exception("数据库未初始化")

        group_id = event.get_group_id()
        reply_content = cls._extract_reply_content(reply)

        if not reply_content:
            raise ValueError("无法获取被引用回复的内容")

        context_id = await cls._find_context_by_reply(reply_content)

        if not context_id:
            logger.warning(
                "chatimitate: 未找到包含回复 '%s' 的上下文", reply_content[:50]
            )
            return DisableStatus.NOT_FOUND

        success = await db.db_operations.disable_reply(
            context_id=context_id,
            keywords=reply_content,
            group_id=group_id,
            reason="管理员禁用",
        )

        if success:
            logger.info(
                "chatimitate: 在群组 %s 中禁用回复 '%s'", group_id, reply_content[:50]
            )
            return DisableStatus.SUCCESS
        else:
            return DisableStatus.FAILED

    @classmethod
    def _extract_reply_content(cls, reply: Reply) -> str:
        """从 Reply 组件中提取回复内容，支持图片、文本和 At 消息"""
        if reply.message_str and reply.message_str.strip():
            return reply.message_str.strip()

        if reply.chain:
            parts = []
            for comp in reply.chain:
                if isinstance(comp, Plain):
                    if comp.text and comp.text.strip():
                        parts.append(comp.text.strip())
                elif isinstance(comp, Image):
                    image_url = getattr(comp, "url", None) or getattr(comp, "file", "")
                    if image_url:
                        parts.append(f"[图片:{image_url}]")
                elif isinstance(comp, At):
                    qq_id = str(comp.qq) if comp.qq else ""
                    if qq_id:
                        if qq_id == "all":
                            parts.append("[at:all]")
                        else:
                            parts.append(f"[at:{qq_id}]")
                elif isinstance(comp, AtAll):
                    parts.append("[at:all]")
            if parts:
                return "".join(parts)

        return ""

    @classmethod
    async def _find_context_by_reply(cls, reply_content: str) -> int | None:
        """根据回复内容查找 context_id"""
        if not db.db_operations:
            return None
        return await db.db_operations.find_context_by_reply(reply_content)
