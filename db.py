"""
AstrBot ChatImitate Plugin - Database Module
精简优化的数据库模块，仅保留聊天学习核心功能

数据库表结构说明：
===================
1. chat_messages（聊天记录表）
   作用：存储群聊历史消息，用于学习人类的聊天模式
   示例：用户 A 说"早上好" → 记录到数据库

2. trigger_keywords（触发关键词表）
   作用：存储什么情况下会触发回复（上下文）
   示例：当有人提到"天气"时 → 触发回复逻辑

3. reply_contents（回复内容表）
   作用：存储具体的回复内容（回复什么）
   示例：就回复"今天天气不错"
   关系：一条 trigger_keywords 可以对应多条 reply_contents

4. disabled_replies（禁用回复表）
   作用：存储某个群禁止使用的回复
   示例：管理员引用机器人的回复说"禁止说这个" → 在该群禁用这条回复
   说明：只保留这一个禁用表，每个群可以有自己的禁用列表
"""

import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

import aiosqlite

from astrbot.api import logger
from astrbot.api.star import StarTools

# 消息分隔符 - 用于在数据库中存储消息列表
MESSAGE_SEPARATOR = "\x00"  # 使用 NULL 字符作为分隔符，避免与正常文本冲突


def serialize_messages(messages: list[str]) -> str:
    """将消息列表序列化为字符串存储"""
    return MESSAGE_SEPARATOR.join(messages)


def deserialize_messages(data: str) -> list[str]:
    """将存储的字符串反序列化为消息列表"""
    if not data:
        return []
    return data.split(MESSAGE_SEPARATOR)


@dataclass
class ChatMessage:
    """聊天记录数据模型 - 存储群聊历史消息"""

    group_id: str  # 群组 ID
    user_id: str  # 发送者 ID
    raw_message: str  # 原始消息的结构化描述（用于调试）
    is_plain_text: bool = True  # 是否为纯文本
    plain_text: str = ""  # 纯文本内容
    keywords: str = ""  # 提取的关键词
    time: int = field(default_factory=lambda: int(time.time()))  # 发送时间


@dataclass
class TriggerKeyword:
    """触发关键词数据模型 - 存储什么情况下会触发回复"""

    keywords: str  # 触发关键词
    time: int = field(default_factory=lambda: int(time.time()))  # 最后触发时间
    trigger_count: int = 1  # 触发次数（学习深度）
    clear_time: int = 0  # 清理标记时间
    # 关联的回复内容
    replies: list["ReplyContent"] = field(default_factory=list)
    # 关联的禁用记录
    disabled: list["DisabledReply"] = field(default_factory=list)


@dataclass
class ReplyContent:
    """回复内容数据模型 - 存储具体的回复内容"""

    keywords: str  # 回复的关键词（用于匹配）
    group_id: str  # 来源群组 ID
    count: int = 1  # 使用次数（越大约常用）
    time: int = field(default_factory=lambda: int(time.time()))  # 最后使用时间
    messages: list[str] = field(default_factory=list)  # 实际回复内容列表
    topical: int = 0  # 话题相关度权重


@dataclass
class DisabledReply:
    """禁用回复数据模型 - 存储临时被禁用的回复"""

    keywords: str  # 被禁用的关键词
    group_id: str  # 禁用来源群组
    reason: str  # 禁用原因（预留）
    time: int = field(default_factory=lambda: int(time.time()))  # 禁用时间


class DatabaseManager:
    """异步 SQLite 数据库管理器"""

    def __init__(self, plugin_name: str = "astrbot_plugin_chatimitate"):
        data_path = StarTools.get_data_dir(plugin_name)
        data_path.mkdir(parents=True, exist_ok=True)
        self.db_path = data_path / "chatimitate.db"
        self._connection: aiosqlite.Connection | None = None

    async def get_connection(self) -> aiosqlite.Connection:
        """获取异步数据库连接"""
        if self._connection is None:
            self._connection = await aiosqlite.connect(self.db_path)
            await self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.row_factory = aiosqlite.Row
        return self._connection

    async def initialize(self):
        """初始化数据库表"""
        conn = await self.get_connection()

        # 表结构定义（使用清晰易懂的表名）
        tables = [
            # 1. 聊天记录表 - 存储群聊历史消息
            """CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,           -- 群组 ID
                user_id TEXT NOT NULL,            -- 发送者 ID
                raw_message TEXT NOT NULL,        -- 原始消息的结构化描述
                is_plain_text INTEGER DEFAULT 1,  -- 是否为纯文本
                plain_text TEXT NOT NULL,         -- 纯文本内容
                keywords TEXT NOT NULL,           -- 提取的关键词
                time INTEGER DEFAULT (strftime('%s', 'now')),  -- 发送时间
                created_at INTEGER DEFAULT (strftime('%s', 'now'))  -- 创建时间
            )""",
            # 2. 触发关键词表 - 存储什么情况下会触发回复
            """CREATE TABLE IF NOT EXISTS trigger_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keywords TEXT UNIQUE NOT NULL,    -- 触发关键词（唯一）
                time INTEGER DEFAULT (strftime('%s', 'now')),  -- 最后触发时间
                trigger_count INTEGER DEFAULT 1,  -- 触发次数（学习深度）
                clear_time INTEGER DEFAULT 0,     -- 清理标记时间
                created_at INTEGER DEFAULT (strftime('%s', 'now')),  -- 创建时间
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))   -- 更新时间
            )""",
            # 3. 回复内容表 - 存储具体的回复内容
            """CREATE TABLE IF NOT EXISTS reply_contents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,      -- 关联的触发关键词 ID
                keywords TEXT NOT NULL,           -- 回复的关键词
                group_id TEXT NOT NULL,           -- 来源群组 ID
                count INTEGER DEFAULT 1,          -- 使用次数（越大约常用）
                time INTEGER DEFAULT (strftime('%s', 'now')),  -- 最后使用时间
                messages TEXT DEFAULT '',         -- 实际回复内容列表（使用分隔符存储）
                topical INTEGER DEFAULT 0,        -- 话题相关度权重
                created_at INTEGER DEFAULT (strftime('%s', 'now')),  -- 创建时间
                FOREIGN KEY (context_id) REFERENCES trigger_keywords (id) ON DELETE CASCADE
            )""",
            # 4. 禁用回复表 - 存储临时被禁用的回复
            """CREATE TABLE IF NOT EXISTS disabled_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,      -- 关联的触发关键词 ID
                keywords TEXT NOT NULL,           -- 被禁用的关键词
                group_id TEXT NOT NULL,           -- 禁用来源群组
                reason TEXT NOT NULL,             -- 禁用原因（预留）
                time INTEGER DEFAULT (strftime('%s', 'now')),  -- 禁用时间
                created_at INTEGER DEFAULT (strftime('%s', 'now')),  -- 创建时间
                FOREIGN KEY (context_id) REFERENCES trigger_keywords (id) ON DELETE CASCADE
            )""",
        ]

        for table_sql in tables:
            await conn.execute(table_sql)

        # 索引优化
        indexes = [
            # 聊天记录索引
            "CREATE INDEX IF NOT EXISTS idx_chat_messages_time ON chat_messages(time)",
            "CREATE INDEX IF NOT EXISTS idx_chat_messages_group ON chat_messages(group_id, time)",
            # 触发关键词索引
            "CREATE INDEX IF NOT EXISTS idx_trigger_keywords_keywords ON trigger_keywords(keywords)",
            "CREATE INDEX IF NOT EXISTS idx_trigger_keywords_trigger ON trigger_keywords(trigger_count, time)",
            # 回复内容索引
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_context ON reply_contents(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_group_keywords ON reply_contents(group_id, keywords)",
            # 禁用回复索引
            "CREATE INDEX IF NOT EXISTS idx_disabled_replies_context ON disabled_replies(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_disabled_replies_group ON disabled_replies(group_id)",
        ]

        for index_sql in indexes:
            await conn.execute(index_sql)

        await conn.commit()

    async def close(self):
        """关闭数据库连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None


class DatabaseOperations:
    """异步数据库操作类"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def save_message(self, message: ChatMessage) -> int:
        """保存聊天记录到数据库"""
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """INSERT INTO chat_messages
            (group_id, user_id, raw_message, is_plain_text, plain_text, keywords, time)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                message.group_id,
                message.user_id,
                message.raw_message,
                1 if message.is_plain_text else 0,
                message.plain_text,
                message.keywords,
                message.time,
            ),
        )
        await conn.commit()
        return cursor.lastrowid or 0

    async def get_messages_by_group(
        self, group_id: str, limit: int = 100
    ) -> list[ChatMessage]:
        """获取群组最近的聊天记录"""
        conn = await self.db.get_connection()
        async with conn.execute(
            """SELECT * FROM chat_messages
            WHERE group_id = ?
            ORDER BY time DESC
            LIMIT ?""",
            (group_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                ChatMessage(
                    group_id=str(row["group_id"]),
                    user_id=str(row["user_id"]),
                    raw_message=row["raw_message"],
                    is_plain_text=bool(row["is_plain_text"]),
                    plain_text=row["plain_text"],
                    keywords=row["keywords"],
                    time=row["time"],
                )
                for row in rows
            ]

    async def get_trigger_keyword(self, keywords: str) -> TriggerKeyword | None:
        """获取触发关键词及其关联的回复"""
        conn = await self.db.get_connection()

        # 查询触发关键词
        async with conn.execute(
            "SELECT * FROM trigger_keywords WHERE keywords = ?", (keywords,)
        ) as cursor:
            trigger_row = await cursor.fetchone()
            if not trigger_row:
                return None

        # 查询关联的回复内容
        replies = []
        async with conn.execute(
            "SELECT * FROM reply_contents WHERE context_id = ?", (trigger_row["id"],)
        ) as cursor:
            async for row in cursor:
                messages_data = deserialize_messages(row["messages"])
                replies.append(
                    ReplyContent(
                        keywords=row["keywords"],
                        group_id=str(row["group_id"]),
                        count=row["count"],
                        time=row["time"],
                        messages=messages_data,
                        topical=row["topical"],
                    )
                )

        # 查询关联的禁用记录
        disabled = []
        async with conn.execute(
            "SELECT * FROM disabled_replies WHERE context_id = ?", (trigger_row["id"],)
        ) as cursor:
            async for row in cursor:
                disabled.append(
                    DisabledReply(
                        keywords=row["keywords"],
                        group_id=str(row["group_id"]),
                        reason=row["reason"],
                        time=row["time"],
                    )
                )

        return TriggerKeyword(
            keywords=trigger_row["keywords"],
            time=trigger_row["time"],
            trigger_count=trigger_row["trigger_count"],
            replies=replies,
            disabled=disabled,
            clear_time=trigger_row["clear_time"],
        )

    async def save_trigger_keyword(self, trigger: TriggerKeyword) -> None:
        """保存触发关键词及其关联的回复"""
        conn = await self.db.get_connection()

        # 保存触发关键词
        await conn.execute(
            """INSERT OR REPLACE INTO trigger_keywords
            (keywords, time, trigger_count, clear_time, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))""",
            (trigger.keywords, trigger.time, trigger.trigger_count, trigger.clear_time),
        )

        # 获取触发关键词 ID
        async with conn.execute(
            "SELECT id FROM trigger_keywords WHERE keywords = ? AND updated_at = (SELECT updated_at FROM trigger_keywords WHERE keywords = ?)",
            (trigger.keywords, trigger.keywords),
        ) as cursor:
            context_id_row = await cursor.fetchone()
            context_id = context_id_row["id"] if context_id_row else None

        if context_id:
            # 为了高效，我们只更新变化的部分
            # 首先查询现有的回复
            existing_replies = {}
            async with conn.execute(
                "SELECT keywords, id FROM reply_contents WHERE context_id = ?",
                (context_id,),
            ) as cursor:
                async for row in cursor:
                    existing_replies[row["keywords"]] = row["id"]

            # 批量处理回复内容 - 只更新/插入必要的内容
            for reply in trigger.replies:
                if reply.keywords in existing_replies:
                    # 更新现有回复
                    await conn.execute(
                        """UPDATE reply_contents SET
                        group_id = ?, count = ?, time = ?, messages = ?, topical = ?, updated_at = strftime('%s', 'now')
                        WHERE id = ?""",
                        (
                            reply.group_id,
                            reply.count,
                            reply.time,
                            serialize_messages(reply.messages),
                            reply.topical,
                            existing_replies[reply.keywords],
                        ),
                    )
                else:
                    # 插入新回复
                    await conn.execute(
                        """INSERT INTO reply_contents
                        (context_id, keywords, group_id, count, time, messages, topical, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'), strftime('%s', 'now'))""",
                        (
                            context_id,
                            reply.keywords,
                            reply.group_id,
                            reply.count,
                            reply.time,
                            serialize_messages(reply.messages),
                            reply.topical,
                        ),
                    )

            # 删除不再需要的回复
            current_reply_keywords = {reply.keywords for reply in trigger.replies}
            for existing_keyword, existing_id in existing_replies.items():
                if existing_keyword not in current_reply_keywords:
                    await conn.execute(
                        "DELETE FROM reply_contents WHERE id = ?", (existing_id,)
                    )

            # 批量处理禁用记录 - 类似的方式
            existing_disabled = {}
            async with conn.execute(
                "SELECT keywords, id FROM disabled_replies WHERE context_id = ?",
                (context_id,),
            ) as cursor:
                async for row in cursor:
                    existing_disabled[row["keywords"]] = row["id"]

            # 批量处理禁用记录
            for disabled in trigger.disabled:
                if disabled.keywords in existing_disabled:
                    # 更新现有禁用记录
                    await conn.execute(
                        """UPDATE disabled_replies SET
                        group_id = ?, reason = ?, time = ?, updated_at = strftime('%s', 'now')
                        WHERE id = ?""",
                        (
                            disabled.group_id,
                            disabled.reason,
                            disabled.time,
                            existing_disabled[disabled.keywords],
                        ),
                    )
                else:
                    # 插入新禁用记录
                    await conn.execute(
                        """INSERT INTO disabled_replies
                        (context_id, keywords, group_id, reason, time, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, strftime('%s', 'now'), strftime('%s', 'now'))""",
                        (
                            context_id,
                            disabled.keywords,
                            disabled.group_id,
                            disabled.reason,
                            disabled.time,
                        ),
                    )

            # 删除不再需要的禁用记录
            current_disabled_keywords = {
                disabled.keywords for disabled in trigger.disabled
            }
            for existing_keyword, existing_id in existing_disabled.items():
                if existing_keyword not in current_disabled_keywords:
                    await conn.execute(
                        "DELETE FROM disabled_replies WHERE id = ?", (existing_id,)
                    )

        await conn.commit()

    async def disable_reply(
        self, context_id: int, keywords: str, group_id: str, reason: str = ""
    ) -> int:
        """在指定群组禁用某个回复"""
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """INSERT INTO disabled_replies
            (context_id, keywords, group_id, reason, time)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))""",
            (context_id, keywords, group_id, reason),
        )
        await conn.commit()
        return cursor.lastrowid or 0

    async def get_disabled_replies_by_group(
        self, group_id: str
    ) -> AsyncGenerator[DisabledReply, None]:
        """获取指定群组的所有禁用回复（生成器）"""
        conn = await self.db.get_connection()
        async with conn.execute(
            "SELECT * FROM disabled_replies WHERE group_id = ?", (group_id,)
        ) as cursor:
            async for row in cursor:
                yield DisabledReply(
                    keywords=row["keywords"],
                    group_id=str(row["group_id"]),
                    reason=row["reason"],
                    time=row["time"],
                )

    async def clear_expired_triggers(self, expiration: int) -> int:
        """清理过期的触发关键词"""
        conn = await self.db.get_connection()

        await conn.execute("BEGIN")
        try:
            # 删除长期未使用的触发关键词
            await conn.execute(
                """DELETE FROM trigger_keywords
                WHERE time < ?
                  AND trigger_count < ?
                  AND id NOT IN (
                      SELECT DISTINCT context_id FROM reply_contents
                      WHERE count > 1 OR time > ?
                  )""",
                (expiration, 3, expiration),
            )

            # 标记高频触发关键词为已清理
            async with conn.execute(
                "SELECT id FROM trigger_keywords WHERE trigger_count > 100 OR clear_time < ?",
                (expiration,),
            ) as cursor:
                rows = await cursor.fetchall()

            trigger_ids = [row["id"] for row in rows]
            if trigger_ids:
                placeholders = ",".join(["?"] * len(trigger_ids))
                # 删除低频回复，保留高频回复
                await conn.execute(
                    f"""DELETE FROM reply_contents
                    WHERE context_id IN ({placeholders})
                      AND NOT (count > 1 OR time > ?)""",
                    (*trigger_ids, expiration),
                )
                # 更新清理时间
                await conn.execute(
                    f"""UPDATE trigger_keywords
                    SET clear_time = ?, updated_at = strftime('%s', 'now')
                    WHERE id IN ({placeholders})""",
                    (int(time.time()), *trigger_ids),
                )

            await conn.commit()
            return len(trigger_ids)
        except Exception:
            await conn.rollback()
            raise

    async def get_all_disabled_groups(self) -> AsyncGenerator[str, None]:
        """获取所有有禁用记录的群组的生成器"""
        conn = await self.db.get_connection()
        async with conn.execute(
            "SELECT DISTINCT group_id FROM disabled_replies"
        ) as cursor:
            async for row in cursor:
                yield str(row["group_id"])

    async def find_context_by_reply(self, reply_message: str) -> int | None:
        """
        根据回复内容查找对应的 context_id

        Args:
            reply_message: 回复内容

        Returns:
            context_id 如果找到，否则 None
        """
        conn = await self.db.get_connection()

        # 方法 1：直接在 reply_contents 表中查找包含该回复的记录
        # 这是最准确的方法
        async with conn.execute(
            """SELECT context_id, messages FROM reply_contents"""
        ) as cursor:
            async for row in cursor:
                messages_str = row["messages"]
                if messages_str:
                    messages = deserialize_messages(messages_str)
                    # 检查回复内容是否在 messages 列表中
                    for msg in messages:
                        if msg and reply_message in msg:
                            return row["context_id"]

        # 方法 2：如果方法 1 没找到，尝试使用回复内容作为关键词查找
        # 这是备选方案
        async with conn.execute(
            """SELECT id FROM trigger_keywords WHERE keywords = ?""", (reply_message,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["id"]

        return None


# 全局数据库实例
db_manager: DatabaseManager | None = None
db_operations: DatabaseOperations | None = None


async def init_db(plugin_name: str = "astrbot_plugin_chatimitate") -> None:
    """初始化数据库"""
    global db_manager, db_operations

    db_manager = DatabaseManager(plugin_name)
    await db_manager.initialize()

    db_operations = DatabaseOperations(db_manager)

    logger.info("chatimitate: database initialized successfully")


__all__ = [
    # 数据模型
    "ChatMessage",  # 聊天记录
    "TriggerKeyword",  # 触发关键词
    "ReplyContent",  # 回复内容
    "DisabledReply",  # 禁用回复
    # 数据库管理
    "DatabaseManager",
    "DatabaseOperations",
    "db_manager",
    "db_operations",
    "init_db",
]
