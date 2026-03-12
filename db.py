"""
AstrBot ChatImitate Plugin - Database Module
数据库模块 - 使用 SQLite 存储聊天记录和学习数据
"""

import time
from dataclasses import dataclass, field
from pathlib import Path

import aiosqlite

from astrbot.api import logger

# 消息分隔符 - 用于在数据库中存储消息列表
MESSAGE_SEPARATOR = "\x00"


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
    """聊天记录数据模型"""

    group_id: str
    user_id: str
    raw_message: str
    is_plain_text: bool = True
    plain_text: str = ""
    keywords: str = ""
    time: int = field(default_factory=lambda: int(time.time()))


@dataclass
class ReplyContent:
    """回复内容数据模型"""

    keywords: str
    group_id: str
    count: int = 1
    time: int = field(default_factory=lambda: int(time.time()))
    messages: list[str] = field(default_factory=list)
    topical: int = 0


@dataclass
class DisabledReply:
    """禁用回复数据模型"""

    keywords: str
    group_id: str
    reason: str = ""
    time: int = field(default_factory=lambda: int(time.time()))


@dataclass
class TriggerKeyword:
    """触发关键词数据模型"""

    keywords: str
    time: int = field(default_factory=lambda: int(time.time()))
    trigger_count: int = 1
    clear_time: int = 0
    replies: list[ReplyContent] = field(default_factory=list)
    disabled: list[DisabledReply] = field(default_factory=list)


class DatabaseManager:
    """异步 SQLite 数据库管理器"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "chatimitate.db"
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

        tables = [
            # 聊天记录表
            """CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                raw_message TEXT NOT NULL,
                is_plain_text INTEGER DEFAULT 1,
                plain_text TEXT NOT NULL,
                keywords TEXT NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            # 触发关键词表
            """CREATE TABLE IF NOT EXISTS trigger_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keywords TEXT UNIQUE NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                trigger_count INTEGER DEFAULT 1,
                clear_time INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            # 回复内容表
            """CREATE TABLE IF NOT EXISTS reply_contents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                group_id TEXT NOT NULL,
                count INTEGER DEFAULT 1,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                messages TEXT DEFAULT '',
                topical INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (context_id) REFERENCES trigger_keywords (id) ON DELETE CASCADE
            )""",
            # 禁用回复表
            """CREATE TABLE IF NOT EXISTS disabled_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                group_id TEXT NOT NULL,
                reason TEXT DEFAULT '',
                time INTEGER DEFAULT (strftime('%s', 'now')),
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (context_id) REFERENCES trigger_keywords (id) ON DELETE CASCADE
            )""",
        ]

        for table_sql in tables:
            await conn.execute(table_sql)

        # 创建索引
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_chat_messages_time ON chat_messages(time)",
            "CREATE INDEX IF NOT EXISTS idx_chat_messages_group ON chat_messages(group_id, time)",
            "CREATE INDEX IF NOT EXISTS idx_trigger_keywords_keywords ON trigger_keywords(keywords)",
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_context ON reply_contents(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_group_keywords ON reply_contents(group_id, keywords)",
            "CREATE INDEX IF NOT EXISTS idx_disabled_replies_context ON disabled_replies(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_disabled_replies_group ON disabled_replies(group_id)",
        ]

        for index_sql in indexes:
            await conn.execute(index_sql)

        await conn.commit()
        logger.info("chatimitate: database initialized")

    async def close(self):
        """关闭数据库连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None


class DatabaseOperations:
    """数据库操作类"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def save_message(self, message: ChatMessage) -> int:
        """保存聊天记录"""
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

    async def get_trigger_keyword(self, keywords: str) -> TriggerKeyword | None:
        """获取触发关键词及其关联数据"""
        conn = await self.db.get_connection()

        async with conn.execute(
            "SELECT * FROM trigger_keywords WHERE keywords = ?", (keywords,)
        ) as cursor:
            trigger_row = await cursor.fetchone()
            if not trigger_row:
                return None

        # 获取回复内容
        replies = []
        async with conn.execute(
            "SELECT * FROM reply_contents WHERE context_id = ?", (trigger_row["id"],)
        ) as cursor:
            async for row in cursor:
                replies.append(
                    ReplyContent(
                        keywords=row["keywords"],
                        group_id=str(row["group_id"]),
                        count=row["count"],
                        time=row["time"],
                        messages=deserialize_messages(row["messages"]),
                        topical=row["topical"],
                    )
                )

        # 获取禁用记录
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
        """保存触发关键词及其关联数据"""
        conn = await self.db.get_connection()

        # 保存触发关键词
        await conn.execute(
            """INSERT OR REPLACE INTO trigger_keywords
            (keywords, time, trigger_count, clear_time, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))""",
            (trigger.keywords, trigger.time, trigger.trigger_count, trigger.clear_time),
        )

        # 获取ID
        async with conn.execute(
            "SELECT id FROM trigger_keywords WHERE keywords = ?",
            (trigger.keywords,),
        ) as cursor:
            row = await cursor.fetchone()
            context_id = row["id"] if row else None

        if not context_id:
            return

        # 获取现有回复
        existing_replies = {}
        async with conn.execute(
            "SELECT id, keywords FROM reply_contents WHERE context_id = ?",
            (context_id,),
        ) as cursor:
            async for row in cursor:
                existing_replies[row["keywords"]] = row["id"]

        # 更新/插入回复
        for reply in trigger.replies:
            if reply.keywords in existing_replies:
                await conn.execute(
                    """UPDATE reply_contents SET
                    count = ?, time = ?, messages = ?, topical = ?, updated_at = strftime('%s', 'now')
                    WHERE id = ?""",
                    (
                        reply.count,
                        reply.time,
                        serialize_messages(reply.messages),
                        reply.topical,
                        existing_replies[reply.keywords],
                    ),
                )
            else:
                await conn.execute(
                    """INSERT INTO reply_contents
                    (context_id, keywords, group_id, count, time, messages, topical)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
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
        current_keywords = {r.keywords for r in trigger.replies}
        for kw, rid in existing_replies.items():
            if kw not in current_keywords:
                await conn.execute("DELETE FROM reply_contents WHERE id = ?", (rid,))

        # 处理禁用记录
        existing_disabled = {}
        async with conn.execute(
            "SELECT id, keywords FROM disabled_replies WHERE context_id = ?",
            (context_id,),
        ) as cursor:
            async for row in cursor:
                existing_disabled[row["keywords"]] = row["id"]

        for disabled in trigger.disabled:
            if disabled.keywords in existing_disabled:
                await conn.execute(
                    """UPDATE disabled_replies SET
                    group_id = ?, reason = ?, time = ?
                    WHERE id = ?""",
                    (
                        disabled.group_id,
                        disabled.reason,
                        disabled.time,
                        existing_disabled[disabled.keywords],
                    ),
                )
            else:
                await conn.execute(
                    """INSERT INTO disabled_replies
                    (context_id, keywords, group_id, reason, time)
                    VALUES (?, ?, ?, ?, ?)""",
                    (
                        context_id,
                        disabled.keywords,
                        disabled.group_id,
                        disabled.reason,
                        disabled.time,
                    ),
                )

        # 删除不再需要的禁用记录
        current_disabled = {d.keywords for d in trigger.disabled}
        for kw, did in existing_disabled.items():
            if kw not in current_disabled:
                await conn.execute("DELETE FROM disabled_replies WHERE id = ?", (did,))

        await conn.commit()

    async def disable_reply(
        self, context_id: int, keywords: str, group_id: str, reason: str = ""
    ) -> int:
        """禁用某个回复"""
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """INSERT INTO disabled_replies
            (context_id, keywords, group_id, reason, time)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))""",
            (context_id, keywords, group_id, reason),
        )
        await conn.commit()
        return cursor.lastrowid or 0

    async def find_context_by_reply(self, reply_message: str) -> int | None:
        """根据回复内容查找 context_id"""
        conn = await self.db.get_connection()

        async with conn.execute(
            "SELECT context_id, messages FROM reply_contents"
        ) as cursor:
            async for row in cursor:
                messages = deserialize_messages(row["messages"])
                for msg in messages:
                    if msg and reply_message in msg:
                        return row["context_id"]

        # 备选：通过关键词查找
        async with conn.execute(
            "SELECT id FROM trigger_keywords WHERE keywords = ?", (reply_message,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["id"]

        return None

    async def clear_expired_triggers(self, expiration: int, min_trigger_count: int = 3) -> int:
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
                (expiration, min_trigger_count, expiration),
            )

            # 清理高频触发关键词的低频回复
            async with conn.execute(
                "SELECT id FROM trigger_keywords WHERE trigger_count > 100 OR clear_time < ?",
                (expiration,),
            ) as cursor:
                rows = await cursor.fetchall()

            trigger_ids = [row["id"] for row in rows]
            if trigger_ids:
                placeholders = ",".join(["?"] * len(trigger_ids))
                await conn.execute(
                    f"""DELETE FROM reply_contents
                    WHERE context_id IN ({placeholders})
                      AND NOT (count > 1 OR time > ?)""",
                    (*trigger_ids, expiration),
                )
                await conn.execute(
                    f"""UPDATE trigger_keywords
                    SET clear_time = ?
                    WHERE id IN ({placeholders})""",
                    (int(time.time()), *trigger_ids),
                )

            await conn.commit()
            return len(trigger_ids)
        except Exception:
            await conn.rollback()
            raise


# 全局实例
db_manager: DatabaseManager | None = None
db_operations: DatabaseOperations | None = None


async def init_db(data_dir: Path) -> None:
    """初始化数据库"""
    global db_manager, db_operations

    db_manager = DatabaseManager(data_dir)
    await db_manager.initialize()
    db_operations = DatabaseOperations(db_manager)

    logger.info("chatimitate: database initialized")
