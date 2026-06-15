"""
AstrBot ChatImitate Plugin - Database Module
"""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path

import aiosqlite

from astrbot.api import logger


def compute_image_hash(image_url: str) -> str:
    """Compute a short hash for an image URL."""
    url_parts = image_url.split("/")[-1] if "/" in image_url else image_url
    return hashlib.md5(url_parts.encode()).hexdigest()[:16]


MESSAGE_SEPARATOR = "\x00"


def serialize_messages(messages: list[str]) -> str:
    """Serialize a list of messages to a single string."""
    return MESSAGE_SEPARATOR.join(messages)


def deserialize_messages(data: str) -> list[str]:
    """Deserialize a stored string back to a list of messages."""
    if not data:
        return []
    return data.split(MESSAGE_SEPARATOR)


@dataclass
class ChatMessage:
    """Chat message data model."""

    group_id: str
    user_id: str
    raw_message: str
    is_plain_text: bool = True
    plain_text: str = ""
    keywords: str = ""
    time: int = field(default_factory=lambda: int(time.time()))


@dataclass
class ReplyContent:
    """Reply content data model."""

    keywords: str
    group_id: str
    count: int = 1
    time: int = field(default_factory=lambda: int(time.time()))
    messages: list[str] = field(default_factory=list)
    topical: int = 0


@dataclass
class DisabledReply:
    """Disabled reply data model."""

    keywords: str
    group_id: str
    reason: str = ""
    time: int = field(default_factory=lambda: int(time.time()))


@dataclass
class TriggerKeyword:
    """Trigger keyword data model."""

    keywords: str
    time: int = field(default_factory=lambda: int(time.time()))
    trigger_count: int = 1
    clear_time: int = 0
    replies: list[ReplyContent] = field(default_factory=list)
    disabled: list[DisabledReply] = field(default_factory=list)


class DatabaseManager:
    """Async SQLite database manager."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "chatimitate.db"
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def get_connection(self) -> aiosqlite.Connection:
        """Get async database connection with health check."""
        if self._connection is not None:
            try:
                await self._connection.execute("SELECT 1")
                return self._connection
            except (aiosqlite.Error, AttributeError):
                logger.warning("chatimitate: stale DB connection, reconnecting")
                try:
                    await self._connection.close()
                except Exception:
                    pass
                self._connection = None

        self._connection = await aiosqlite.connect(self.db_path)
        await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.row_factory = aiosqlite.Row
        return self._connection

    async def initialize(self):
        """Initialize database tables."""
        conn = await self.get_connection()

        tables = [
            """CREATE TABLE IF NOT EXISTS trigger_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keywords TEXT UNIQUE NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                trigger_count INTEGER DEFAULT 1,
                clear_time INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
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

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_trigger_keywords_keywords ON trigger_keywords(keywords)",
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_context ON reply_contents(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_group_keywords ON reply_contents(group_id, keywords)",
            "CREATE INDEX IF NOT EXISTS idx_disabled_replies_context ON disabled_replies(context_id)",
            "CREATE INDEX IF NOT EXISTS idx_disabled_replies_group ON disabled_replies(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_reply_contents_messages ON reply_contents(messages)",
        ]

        for index_sql in indexes:
            await conn.execute(index_sql)

        await conn.commit()

    async def close(self):
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None


class DatabaseOperations:
    """Database operation methods."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def get_trigger_keyword(self, keywords: str) -> TriggerKeyword | None:
        """Get trigger keyword and its associated data.

        Uses a single transaction to ensure read consistency across
        trigger_keywords, reply_contents, and disabled_replies tables.
        """
        conn = await self.db.get_connection()

        await conn.execute("BEGIN DEFERRED")
        try:
            async with conn.execute(
                "SELECT * FROM trigger_keywords WHERE keywords = ?", (keywords,)
            ) as cursor:
                trigger_row = await cursor.fetchone()
                if not trigger_row:
                    await conn.commit()
                    return None

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

            await conn.commit()

            return TriggerKeyword(
                keywords=trigger_row["keywords"],
                time=trigger_row["time"],
                trigger_count=trigger_row["trigger_count"],
                replies=replies,
                disabled=disabled,
                clear_time=trigger_row["clear_time"],
            )
        except Exception:
            await conn.rollback()
            raise

    async def save_trigger_keyword(self, trigger: TriggerKeyword) -> None:
        """Save trigger keyword and associated data with transaction protection."""
        async with self.db._lock:
            conn = await self.db.get_connection()

            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    """INSERT INTO trigger_keywords
                    (keywords, time, trigger_count, clear_time, updated_at)
                    VALUES (?, ?, ?, ?, strftime('%s', 'now'))
                    ON CONFLICT(keywords) DO UPDATE SET
                    time = excluded.time,
                    trigger_count = excluded.trigger_count,
                    clear_time = excluded.clear_time,
                    updated_at = excluded.updated_at
                    RETURNING id""",
                    (trigger.keywords, trigger.time, trigger.trigger_count, trigger.clear_time),
                )
                row = await cursor.fetchone()
                context_id = row[0] if row else None

                if not context_id:
                    await conn.rollback()
                    return

                existing_replies = {}
                async with conn.execute(
                    "SELECT id, keywords FROM reply_contents WHERE context_id = ?",
                    (context_id,),
                ) as reply_cursor:
                    async for reply_row in reply_cursor:
                        existing_replies[reply_row["keywords"]] = reply_row["id"]

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

                current_keywords = {r.keywords for r in trigger.replies}
                for kw, rid in existing_replies.items():
                    if kw not in current_keywords:
                        await conn.execute("DELETE FROM reply_contents WHERE id = ?", (rid,))

                existing_disabled = {}
                async with conn.execute(
                    "SELECT id, keywords FROM disabled_replies WHERE context_id = ?",
                    (context_id,),
                ) as disabled_cursor:
                    async for disabled_row in disabled_cursor:
                        existing_disabled[disabled_row["keywords"]] = disabled_row["id"]

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

                current_disabled = {d.keywords for d in trigger.disabled}
                for kw, did in existing_disabled.items():
                    if kw not in current_disabled:
                        await conn.execute("DELETE FROM disabled_replies WHERE id = ?", (did,))

                await conn.commit()
            except Exception:
                await conn.rollback()
                logger.error("chatimitate: failed to save trigger batch", exc_info=True)
                return

    async def disable_reply(
        self, context_id: int, keywords: str, group_id: str, reason: str = ""
    ) -> bool:
        """Disable a reply with deduplication check."""
        conn = await self.db.get_connection()

        async with self.db._lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    """SELECT 1 FROM disabled_replies
                    WHERE context_id = ? AND keywords = ? AND group_id = ?""",
                    (context_id, keywords, group_id),
                ) as cursor:
                    if await cursor.fetchone():
                        await conn.rollback()
                        return True

                await conn.execute(
                    """INSERT INTO disabled_replies
                    (context_id, keywords, group_id, reason, time)
                    VALUES (?, ?, ?, ?, strftime('%s', 'now'))""",
                    (context_id, keywords, group_id, reason),
                )
                await conn.commit()
                return True
            except Exception:
                await conn.rollback()
                logger.error("chatimitate: failed to disable reply", exc_info=True)
                return False

    async def find_context_by_reply(self, reply_message: str) -> int | None:
        """Find context_id by reply content using exact matching."""
        conn = await self.db.get_connection()

        escaped_message = reply_message.replace("%", "\\%").replace("_", "\\_")

        async with conn.execute(
            "SELECT context_id, messages FROM reply_contents WHERE messages LIKE ? ESCAPE '\\'",
            (f"%{escaped_message}%",)
        ) as cursor:
            async for row in cursor:
                messages = deserialize_messages(row["messages"])
                for msg in messages:
                    if msg == reply_message:
                        return row["context_id"]

        async with conn.execute(
            "SELECT context_id FROM reply_contents WHERE keywords = ?", (reply_message,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["context_id"]

        async with conn.execute(
            "SELECT id FROM trigger_keywords WHERE keywords = ?", (reply_message,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["id"]

        return None

    async def clear_expired_triggers(self, expiration: int, min_trigger_count: int = 3) -> int:
        """Clear expired trigger keywords."""
        async with self.db._lock:
            conn = await self.db.get_connection()

            await conn.execute("BEGIN IMMEDIATE")
            try:
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
                logger.error("chatimitate: failed to clear expired triggers", exc_info=True)
                return 0


# Global instances
db_manager: DatabaseManager | None = None
db_operations: DatabaseOperations | None = None


async def init_db(data_dir: Path) -> None:
    """Initialize database."""
    global db_manager, db_operations

    db_manager = DatabaseManager(data_dir)
    await db_manager.initialize()
    db_operations = DatabaseOperations(db_manager)

    logger.info("chatimitate: database initialized")
