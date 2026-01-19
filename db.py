import time
import json
import aiosqlite
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import BaseModel, Field
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from astrbot.api import logger


class DatabaseManager:
    """异步SQLite数据库管理器"""
    
    def __init__(self, plugin_name: str = "astrbot_plugin_chatimitate"):
        # 获取AstrBot数据目录并构建插件数据路径
        data_path_str = get_astrbot_data_path()
        plugin_data_path = Path(data_path_str) / "plugin_data" / plugin_name
        plugin_data_path.mkdir(parents=True, exist_ok=True)
        
        # 设置数据库文件路径
        self.db_path = plugin_data_path / "chatimitate.db"
        self._connection = None
    
    async def get_connection(self) -> aiosqlite.Connection:
        """获取异步数据库连接"""
        if self._connection is None:
            self._connection = await aiosqlite.connect(self.db_path)
            # 启用外键约束
            await self._connection.execute("PRAGMA foreign_keys = ON")
            # 设置行工厂
            self._connection.row_factory = aiosqlite.Row
        return self._connection
    
    async def initialize(self):
        """初始化数据库表"""
        conn = await self.get_connection()
        
        # 定义表结构
        table_definitions = [
            # bot_config表
            """CREATE TABLE IF NOT EXISTS bot_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account INTEGER UNIQUE NOT NULL,
                admins TEXT DEFAULT '[]',
                auto_accept BOOLEAN DEFAULT FALSE,
                security BOOLEAN DEFAULT FALSE,
                taken_name TEXT DEFAULT '{}',
                disabled_plugins TEXT DEFAULT '[]',
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            
            # group_config表
            """CREATE TABLE IF NOT EXISTS group_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER UNIQUE NOT NULL,
                roulette_mode INTEGER DEFAULT 1,
                banned BOOLEAN DEFAULT FALSE,
                disabled_plugins TEXT DEFAULT '[]',
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            
            # user_config表
            """CREATE TABLE IF NOT EXISTS user_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                banned BOOLEAN DEFAULT FALSE,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            
            # messages表
            """CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                bot_id INTEGER NOT NULL,
                raw_message TEXT NOT NULL,
                is_plain_text BOOLEAN DEFAULT TRUE,
                plain_text TEXT NOT NULL,
                keywords TEXT NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            
            # contexts表
            """CREATE TABLE IF NOT EXISTS contexts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keywords TEXT UNIQUE NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                trigger_count INTEGER DEFAULT 1,
                clear_time INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            
            # answers表
            """CREATE TABLE IF NOT EXISTS answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                count INTEGER DEFAULT 1,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                messages TEXT DEFAULT '[]',
                topical INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (context_id) REFERENCES contexts (id) ON DELETE CASCADE
            )""",
            
            # bans表
            """CREATE TABLE IF NOT EXISTS bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                time INTEGER DEFAULT (strftime('%s', 'now')),
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (context_id) REFERENCES contexts (id) ON DELETE CASCADE
            )""",
            
            # blacklist表
            """CREATE TABLE IF NOT EXISTS blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER UNIQUE NOT NULL,
                answers TEXT DEFAULT '[]',
                answers_reserve TEXT DEFAULT '[]',
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )""",
            
            # image_cache表
            """CREATE TABLE IF NOT EXISTS image_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date INTEGER NOT NULL,
                cq_code TEXT UNIQUE NOT NULL,
                base64_data TEXT,
                ref_times INTEGER DEFAULT 1,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )"""
        ]
        
        # 创建表
        for table_sql in table_definitions:
            await conn.execute(table_sql)
        
        # 创建索引
        index_definitions = [
            ("idx_messages_time", "CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(time)"),
            ("idx_contexts_keywords", "CREATE INDEX IF NOT EXISTS idx_contexts_keywords ON contexts(keywords)"),
            ("idx_contexts_trigger_count", "CREATE INDEX IF NOT EXISTS idx_contexts_trigger_count ON contexts(trigger_count)"),
            ("idx_contexts_time", "CREATE INDEX IF NOT EXISTS idx_contexts_time ON contexts(time)"),
            ("idx_answers_group_keywords", "CREATE INDEX IF NOT EXISTS idx_answers_group_keywords ON answers(group_id, keywords)"),
            ("idx_blacklist_group", "CREATE INDEX IF NOT EXISTS idx_blacklist_group ON blacklist(group_id)"),
            ("idx_image_cache_cq_code", "CREATE INDEX IF NOT EXISTS idx_image_cache_cq_code ON image_cache(cq_code)")
        ]
        
        for index_name, index_sql in index_definitions:
            await conn.execute(index_sql)
        
        await conn.commit()
    
    async def close(self):
        """关闭数据库连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None


# 数据模型类
class BotConfig(BaseModel):
    account: str
    admins: List[str] = Field(default_factory=list)
    auto_accept: bool = False
    security: bool = False
    taken_name: Dict[str, str] = Field(default_factory=dict)
    disabled_plugins: List[str] = Field(default_factory=list)


class GroupConfig(BaseModel):
    group_id: str
    roulette_mode: int = 1
    banned: bool = False
    disabled_plugins: List[str] = Field(default_factory=list)


class UserConfig(BaseModel):
    user_id: str
    banned: bool = False


class Message(BaseModel):
    group_id: str
    user_id: str
    bot_id: str
    raw_message: str
    is_plain_text: bool = True
    plain_text: str
    keywords: str
    time: int = Field(default_factory=lambda: int(time.time()))


class Ban(BaseModel):
    keywords: str
    group_id: str
    reason: str
    time: int = Field(default_factory=lambda: int(time.time()))


class Answer(BaseModel):
    keywords: str
    group_id: str
    count: int = 1
    time: int = Field(default_factory=lambda: int(time.time()))
    messages: List[str] = Field(default_factory=list)
    topical: int = 0


class Context(BaseModel):
    keywords: str
    time: int = Field(default_factory=lambda: int(time.time()))
    trigger_count: int = 1
    answers: List[Answer] = Field(default_factory=list)
    ban: List[Ban] = Field(default_factory=list)
    clear_time: int = 0


class BlackList(BaseModel):
    group_id: str
    answers: List[str] = Field(default_factory=list)
    answers_reserve: List[str] = Field(default_factory=list)


class ImageCache(BaseModel):
    date: int = Field(default_factory=lambda: int(str(datetime.now().date()).replace("-", "")))
    cq_code: str
    base64_data: Optional[str] = None
    ref_times: int = 1


class DatabaseOperations:
    """异步数据库操作类"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def _json_serialize(self, data: Any) -> str:
        """序列化JSON数据"""
        return json.dumps(data, ensure_ascii=False)
    
    def _json_deserialize(self, json_str: str) -> Any:
        """反序列化JSON数据"""
        if json_str:
            return json.loads(json_str)
        return None
    
    # BotConfig操作
    async def get_bot_config(self, account: str) -> Optional[BotConfig]:
        """获取机器人配置"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT * FROM bot_config WHERE account = ?", (account,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return BotConfig(
                    account=str(row['account']),
                    admins=self._json_deserialize(row['admins']),
                    auto_accept=bool(row['auto_accept']),
                    security=bool(row['security']),
                    taken_name=self._json_deserialize(row['taken_name']),
                    disabled_plugins=self._json_deserialize(row['disabled_plugins'])
                )
        return None
    
    async def save_bot_config(self, config: BotConfig) -> None:
        """保存机器人配置"""
        conn = await self.db.get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO bot_config 
            (account, admins, auto_accept, security, taken_name, disabled_plugins, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
        """, (
            config.account,
            self._json_serialize(config.admins),
            config.auto_accept,
            config.security,
            self._json_serialize(config.taken_name),
            self._json_serialize(config.disabled_plugins)
        ))
        await conn.commit()
    
    # GroupConfig操作
    async def get_group_config(self, group_id: str) -> Optional[GroupConfig]:
        """获取群组配置"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT * FROM group_config WHERE group_id = ?", (group_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return GroupConfig(
                    group_id=str(row['group_id']),
                    roulette_mode=row['roulette_mode'],
                    banned=bool(row['banned']),
                    disabled_plugins=self._json_deserialize(row['disabled_plugins'])
                )
        return None
    
    async def save_group_config(self, config: GroupConfig) -> None:
        """保存群组配置"""
        conn = await self.db.get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO group_config 
            (group_id, roulette_mode, banned, disabled_plugins, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))
        """, (
            config.group_id,
            config.roulette_mode,
            config.banned,
            self._json_serialize(config.disabled_plugins)
        ))
        await conn.commit()
    
    # UserConfig操作
    async def get_user_config(self, user_id: str) -> Optional[UserConfig]:
        """获取用户配置"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT * FROM user_config WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return UserConfig(
                    user_id=str(row['user_id']),
                    banned=bool(row['banned'])
                )
        return None
    
    async def save_user_config(self, config: UserConfig) -> None:
        """保存用户配置"""
        conn = await self.db.get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO user_config 
            (user_id, banned, updated_at)
            VALUES (?, ?, strftime('%s', 'now'))
        """, (config.user_id, config.banned))
        await conn.commit()
    
    # Message操作
    async def save_message(self, message: Message) -> int:
        """保存消息"""
        conn = await self.db.get_connection()
        cursor = await conn.execute("""
            INSERT INTO messages 
            (group_id, user_id, bot_id, raw_message, is_plain_text, plain_text, keywords, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            message.group_id,
            message.user_id,
            message.bot_id,
            message.raw_message,
            message.is_plain_text,
            message.plain_text,
            message.keywords,
            message.time
        ))
        await conn.commit()
        return cursor.lastrowid or 0
    
    async def get_messages_by_time_range(self, start_time: int, end_time: int) -> List[Message]:
        """根据时间范围获取消息"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT * FROM messages WHERE time BETWEEN ? AND ? ORDER BY time", (start_time, end_time)) as cursor:
            rows = await cursor.fetchall()
            messages = []
            for row in rows:
                messages.append(Message(
                    group_id=str(row['group_id']),
                    user_id=str(row['user_id']),
                    bot_id=str(row['bot_id']),
                    raw_message=row['raw_message'],
                    is_plain_text=bool(row['is_plain_text']),
                    plain_text=row['plain_text'],
                    keywords=row['keywords'],
                    time=row['time']
                ))
            return messages
    
    # Context操作
    async def get_context(self, keywords: str) -> Optional[Context]:
        """获取上下文"""
        conn = await self.db.get_connection()
        
        # 获取context基本信息
        async with conn.execute("SELECT * FROM contexts WHERE keywords = ?", (keywords,)) as cursor:
            context_row = await cursor.fetchone()
            if not context_row:
                return None
        
        # 获取关联的answers
        answers = []
        async with conn.execute("SELECT * FROM answers WHERE context_id = ?", (context_row['id'],)) as cursor:
            async for row in cursor:
                answers.append(Answer(
                    keywords=row['keywords'],
                    group_id=str(row['group_id']),
                    count=row['count'],
                    time=row['time'],
                    messages=self._json_deserialize(row['messages']),
                    topical=row['topical']
                ))
        
        # 获取关联的bans
        bans = []
        async with conn.execute("SELECT * FROM bans WHERE context_id = ?", (context_row['id'],)) as cursor:
            async for row in cursor:
                bans.append(Ban(
                    keywords=row['keywords'],
                    group_id=str(row['group_id']),
                    reason=row['reason'],
                    time=row['time']
                ))
        
        return Context(
            keywords=context_row['keywords'],
            time=context_row['time'],
            trigger_count=context_row['trigger_count'],
            answers=answers,
            ban=bans,
            clear_time=context_row['clear_time']
        )
    
    async def save_context(self, context: Context) -> None:
        """保存上下文"""
        conn = await self.db.get_connection()
        
        # 保存或更新context
        await conn.execute("""
            INSERT OR REPLACE INTO contexts 
            (keywords, time, trigger_count, clear_time, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))
        """, (context.keywords, context.time, context.trigger_count, context.clear_time))
        
        # 获取context ID
        async with conn.execute("SELECT id FROM contexts WHERE keywords = ?", (context.keywords,)) as cursor:
            context_id_row = await cursor.fetchone()
            context_id = context_id_row['id'] if context_id_row else None
        
        # 删除旧的关联数据
        await conn.execute("DELETE FROM answers WHERE context_id = ?", (context_id,))
        await conn.execute("DELETE FROM bans WHERE context_id = ?", (context_id,))
        
        # 保存answers
        for answer in context.answers:
            await conn.execute("""
                INSERT INTO answers 
                (context_id, keywords, group_id, count, time, messages, topical)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                context_id, answer.keywords, answer.group_id, answer.count, 
                answer.time, self._json_serialize(answer.messages), answer.topical
            ))
        
        # 保存bans
        for ban in context.ban:
            await conn.execute("""
                INSERT INTO bans 
                (context_id, keywords, group_id, reason, time)
                VALUES (?, ?, ?, ?, ?)
            """, (context_id, ban.keywords, ban.group_id, ban.reason, ban.time))
        
        await conn.commit()
    
    # BlackList操作
    async def get_blacklist(self, group_id: str) -> Optional[BlackList]:
        """获取黑名单"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT * FROM blacklist WHERE group_id = ?", (group_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return BlackList(
                    group_id=str(row['group_id']),
                    answers=self._json_deserialize(row['answers']),
                    answers_reserve=self._json_deserialize(row['answers_reserve'])
                )
        return None
    
    async def save_blacklist(self, blacklist: BlackList) -> None:
        """保存黑名单"""
        conn = await self.db.get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO blacklist 
            (group_id, answers, answers_reserve, updated_at)
            VALUES (?, ?, ?, strftime('%s', 'now'))
        """, (
            blacklist.group_id,
            self._json_serialize(blacklist.answers),
            self._json_serialize(blacklist.answers_reserve)
        ))
        await conn.commit()
    
    # ImageCache操作
    async def get_image_cache(self, cq_code: str) -> Optional[ImageCache]:
        """获取图片缓存"""
        conn = await self.db.get_connection()
        async with conn.execute("SELECT * FROM image_cache WHERE cq_code = ?", (cq_code,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return ImageCache(
                    date=row['date'],
                    cq_code=row['cq_code'],
                    base64_data=row['base64_data'],
                    ref_times=row['ref_times']
                )
        return None
    
    async def save_image_cache(self, cache: ImageCache) -> None:
        """保存图片缓存"""
        conn = await self.db.get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO image_cache 
            (date, cq_code, base64_data, ref_times, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))
        """, (cache.date, cache.cq_code, cache.base64_data, cache.ref_times))
        await conn.commit()


# 全局数据库实例
db_manager = None
db_operations = None


async def init_db(plugin_name: str = "astrbot_plugin_chatimitate"):
    """初始化数据库"""
    global db_manager, db_operations
    
    # 创建数据库管理器实例
    db_manager = DatabaseManager(plugin_name)
    await db_manager.initialize()
    if db_manager is None:
        logger.error("chatimitate: initialize db_manager failed")
        return
    
    # 创建数据库操作实例
    db_operations = DatabaseOperations(db_manager)
    if db_operations is None:
        logger.error("chatimitate: initialize db_operations failed")
        return

    logger.info("chatimitate: init_db done")


__all__ = [
    "BotConfig",
    "GroupConfig", 
    "UserConfig",
    "Message",
    "Ban",
    "Answer",
    "Context",
    "BlackList",
    "ImageCache",
    "db_operations",
    "init_db"
]