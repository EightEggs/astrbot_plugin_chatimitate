"""
AstrBot ChatImitate Plugin - Database Module
精简优化的数据库模块，仅保留聊天学习核心功能
"""

import json
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

import aiosqlite

from astrbot.api import logger
from astrbot.api.star import StarTools


@dataclass
class Message:
    """消息数据模型"""

    group_id: str
    user_id: str
    bot_id: str
    raw_message: str
    is_plain_text: bool = True
    plain_text: str = ""
    keywords: str = ""
    time: int = field(default_factory=lambda: int(time.time()))


@dataclass
class Answer:
    """回复数据模型"""

    keywords: str
    group_id: str
    count: int = 1
    time: int = field(default_factory=lambda: int(time.time()))
    messages: list[str] = field(default_factory=list)
    topical: int = 0


@dataclass
class Ban:
    """禁用回复数据模型"""

    keywords: str
    group_id: str
    reason: str
    time: int = field(default_factory=lambda: int(time.time()))


@dataclass
class Context:
    """上下文数据模型"""

    keywords: str
    time: int = field(default_factory=lambda: int(time.time()))
    trigger_count: int = 1
    answers: list[Answer] = field(default_factory=list)
    ban: list[Ban] = field(default_factory=list)
    clear_time: int = 0


@dataclass
class BlackList:
    """黑名单数据模型"""

    group_id: str
    answers: list[str] = field(default_factory=list)
    answers_reserve: list[str] = field(default_factory=list)


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

        tables = [
            """CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                raw_message TEXT NOT NULL,
                is_plain_text INTEGER DEFAULT 1,
                plain_text TEXT NOT NULL,
                keywords TEXT NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            """CREATE TABLE IF NOT EXISTS contexts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keywords TEXT UNIQUE NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                trigger_count INTEGER DEFAULT 1,
                clear_time INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            """CREATE TABLE IF NOT EXISTS answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                group_id TEXT NOT NULL,
                count INTEGER DEFAULT 1,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                messages TEXT DEFAULT '[]',
                topical INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (context_id) REFERENCES contexts (id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                group_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (context_id) REFERENCES contexts (id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT UNIQUE NOT NULL,
                answers TEXT DEFAULT '[]',
                answers_reserve TEXT DEFAULT '[]',
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
        ]

        for table_sql in tables:
            await conn.execute(table_sql)

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(time)",
            "CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id, time)",
            "CREATE INDEX IF NOT EXISTS idx_contexts_keywords ON contexts(keywords)",
            "CREATE INDEX IF NOT EXISTS idx_contexts_trigger ON contexts(trigger_count, time)",
            "CREATE INDEX IF NOT EXISTS idx_answers_context ON answers(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_answers_group_keywords ON answers(group_id, keywords)",
            "CREATE INDEX IF NOT EXISTS idx_bans_context ON bans(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_blacklist_group ON blacklist(group_id)",
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

    @staticmethod
    def _json_serialize(data: object) -> str:
        """序列化 JSON 数据"""
        return json.dumps(data, ensure_ascii=False)

    @staticmethod
    def _json_deserialize(json_str: str) -> object:
        """反序列化 JSON 数据"""
        return json.loads(json_str) if json_str else None

    async def save_message(self, message: Message) -> int:
        """保存消息"""
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """INSERT INTO messages
            (group_id, user_id, bot_id, raw_message, is_plain_text, plain_text, keywords, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                message.group_id,
                message.user_id,
                message.bot_id,
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
    ) -> list[Message]:
        """获取群组最近消息"""
        conn = await self.db.get_connection()
        async with conn.execute(
            """SELECT * FROM messages
            WHERE group_id = ?
            ORDER BY time DESC
            LIMIT ?""",
            (group_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                Message(
                    group_id=str(row["group_id"]),
                    user_id=str(row["user_id"]),
                    bot_id=str(row["bot_id"]),
                    raw_message=row["raw_message"],
                    is_plain_text=bool(row["is_plain_text"]),
                    plain_text=row["plain_text"],
                    keywords=row["keywords"],
                    time=row["time"],
                )
                for row in rows
            ]

    async def get_context(self, keywords: str) -> Context | None:
        """获取上下文"""
        conn = await self.db.get_connection()

        async with conn.execute(
            "SELECT * FROM contexts WHERE keywords = ?", (keywords,)
        ) as cursor:
            context_row = await cursor.fetchone()
            if not context_row:
                return None

        answers = []
        async with conn.execute(
            "SELECT * FROM answers WHERE context_id = ?", (context_row["id"],)
        ) as cursor:
            async for row in cursor:
                answers.append(
                    Answer(
                        keywords=row["keywords"],
                        group_id=str(row["group_id"]),
                        count=row["count"],
                        time=row["time"],
                        messages=self._json_deserialize(row["messages"]),  # type: ignore
                    )
                )

        bans = []
        async with conn.execute(
            "SELECT * FROM bans WHERE context_id = ?", (context_row["id"],)
        ) as cursor:
            async for row in cursor:
                bans.append(
                    Ban(
                        keywords=row["keywords"],
                        group_id=str(row["group_id"]),
                        reason=row["reason"],
                        time=row["time"],
                    )
                )

        return Context(
            keywords=context_row["keywords"],
            time=context_row["time"],
            trigger_count=context_row["trigger_count"],
            answers=answers,
            ban=bans,
            clear_time=context_row["clear_time"],
        )

    async def save_context(self, context: Context) -> None:
        """保存上下文"""
        conn = await self.db.get_connection()

        await conn.execute(
            """INSERT OR REPLACE INTO contexts
            (keywords, time, trigger_count, clear_time, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))""",
            (context.keywords, context.time, context.trigger_count, context.clear_time),
        )

        async with conn.execute(
            "SELECT id FROM contexts WHERE keywords = ?", (context.keywords,)
        ) as cursor:
            context_id_row = await cursor.fetchone()
            context_id = context_id_row["id"] if context_id_row else None

        if context_id:
            await conn.execute(
                "DELETE FROM answers WHERE context_id = ?", (context_id,)
            )
            await conn.execute("DELETE FROM bans WHERE context_id = ?", (context_id,))

            if context.answers:
                await conn.executemany(
                    """INSERT INTO answers
                    (context_id, keywords, group_id, count, time, messages, topical)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            context_id,
                            answer.keywords,
                            answer.group_id,
                            answer.count,
                            answer.time,
                            self._json_serialize(answer.messages),
                            answer.topical,
                        )
                        for answer in context.answers
                    ],
                )

            if context.ban:
                await conn.executemany(
                    """INSERT INTO bans
                    (context_id, keywords, group_id, reason, time)
                    VALUES (?, ?, ?, ?, ?)""",
                    [
                        (context_id, ban.keywords, ban.group_id, ban.reason, ban.time)
                        for ban in context.ban
                    ],
                )

        await conn.commit()

    async def get_blacklist(self, group_id: str) -> BlackList | None:
        """获取黑名单"""
        conn = await self.db.get_connection()
        async with conn.execute(
            "SELECT * FROM blacklist WHERE group_id = ?", (group_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return BlackList(
                    group_id=str(row["group_id"]),
                    answers=self._json_deserialize(row["answers"]),  # type: ignore
                    answers_reserve=self._json_deserialize(row["answers_reserve"]),  # type: ignore
                )
        return None

    async def save_blacklist(self, blacklist: BlackList) -> None:
        """保存黑名单"""
        conn = await self.db.get_connection()
        await conn.execute(
            """INSERT OR REPLACE INTO blacklist
            (group_id, answers, answers_reserve, updated_at)
            VALUES (?, ?, ?, strftime('%s', 'now'))""",
            (
                blacklist.group_id,
                self._json_serialize(blacklist.answers),
                self._json_serialize(blacklist.answers_reserve),
            ),
        )
        await conn.commit()

    async def clear_expired_contexts(self, expiration: int) -> int:
        """清理过期的上下文"""
        conn = await self.db.get_connection()

        await conn.execute("BEGIN")
        try:
            await conn.execute(
                """DELETE FROM contexts
                WHERE time < ?
                  AND trigger_count < ?
                  AND id NOT IN (
                      SELECT DISTINCT context_id FROM answers
                      WHERE count > 1 OR time > ?
                  )""",
                (expiration, 3, expiration),
            )

            async with conn.execute(
                "SELECT id FROM contexts WHERE trigger_count > 100 OR clear_time < ?",
                (expiration,),
            ) as cursor:
                rows = await cursor.fetchall()

            context_ids = [row["id"] for row in rows]
            if context_ids:
                placeholders = ",".join(["?"] * len(context_ids))
                await conn.execute(
                    f"""DELETE FROM answers
                    WHERE context_id IN ({placeholders})
                      AND NOT (count > 1 OR time > ?)""",
                    (*context_ids, expiration),
                )
                await conn.execute(
                    f"""UPDATE contexts
                    SET clear_time = ?, updated_at = strftime('%s', 'now')
                    WHERE id IN ({placeholders})""",
                    (int(time.time()), *context_ids),
                )

            await conn.commit()
            return len(context_ids)
        except Exception:
            await conn.rollback()
            raise

    async def get_all_blacklist_groups(self) -> AsyncGenerator[str, None]:
        """获取所有黑名单群组的生成器"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT DISTINCT group_id FROM blacklist") as cursor:
            async for row in cursor:
                yield str(row["group_id"])


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
    "Message",
    "Answer",
    "Ban",
    "Context",
    "BlackList",
    "DatabaseManager",
    "DatabaseOperations",
    "db_manager",
    "db_operations",
    "init_db",
]
