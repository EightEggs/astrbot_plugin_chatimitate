"""
AstrBot ChatImitate Plugin - Core Logic Module
核心聊天学习和回复逻辑 - 增强版
支持图片、语音、视频等多媒体消息
"""

import asyncio
import hashlib
import random
import time
from collections import defaultdict, deque
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from functools import cached_property

import jieba_next.analyse as jieba_analyse
import pypinyin

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import (
    At,
    Face,
    File,
    Image,
    Plain,
    Record,
    Reply,
    Video,
)

from . import db
from .db import ChatMessage, ReplyContent, TriggerKeyword

# 向后兼容的别名
MessageModel = ChatMessage
Answer = ReplyContent
Context = TriggerKeyword


@dataclass
class ChatData:
    """聊天数据结构 - 完全结构化版本"""

    group_id: str
    user_id: str
    plain_text: str
    time: int
    bot_id: str
    message_type: str = "text"  # text, image, record, video, file, mixed
    image_hash: str | None = None  # 图片哈希，用于去重
    image_url: str | None = None  # 图片 URL
    is_reply: bool = False  # 是否是回复消息
    has_media_content: bool = False  # 是否包含多媒体内容
    _event: AstrMessageEvent | None = None  # 原始事件对象，用于结构化检查
    _keywords_size: int = 2

    def has_media(self) -> bool:
        """检查消息是否包含多媒体内容（非纯文本）"""
        return self.has_media_content

    @cached_property
    def is_plain_text(self) -> bool:
        return self.message_type == "text" and len(self.plain_text) != 0

    @cached_property
    def is_image(self) -> bool:
        return self.message_type in ("image", "mixed") and self.image_url is not None

    @cached_property
    def is_record(self) -> bool:
        return self.message_type == "record"

    @cached_property
    def is_video(self) -> bool:
        return self.message_type == "video"

    @cached_property
    def is_file(self) -> bool:
        return self.message_type == "file"

    @cached_property
    def _keywords_list(self) -> list[str]:
        if not self.is_plain_text and len(self.plain_text) == 0:
            return []
        try:
            # jieba_analyse.extract_tags 返回的是 list[tuple[str, float]]
            # 格式：[('关键词', 权重), ...]
            keywords = jieba_analyse.extract_tags(
                self.plain_text, topK=ChatData._keywords_size, withWeight=True
            )
            # 提取关键词部分
            return [
                item[0] if isinstance(item, (list, tuple)) else str(item)
                for item in keywords
            ]
        except Exception as e:
            logger.warning("chatimitate: jieba keyword extraction failed: %s", e)
            # 如果提取失败，返回整个文本作为关键词
            return [self.plain_text] if self.plain_text else []

    @cached_property
    def keywords_len(self) -> int:
        return len(self._keywords_list)

    @cached_property
    def keywords(self) -> str:
        if self.is_image and not self.is_plain_text:
            return f"[图片]{self.image_hash or ''}"
        if self.is_record:
            return "[语音]"
        if self.is_video:
            return "[视频]"
        if self.is_file:
            return "[文件]"
        if not self.is_plain_text and len(self.plain_text) == 0:
            # 非纯文本且无纯文本内容时，返回消息类型标记
            return f"[{self.message_type}]"
        return (
            " ".join(self._keywords_list) if self.keywords_len > 0 else self.plain_text
        )

    @cached_property
    def keywords_pinyin(self) -> str:
        return "".join(
            [
                item[0]
                for item in pypinyin.pinyin(
                    self.keywords, style=pypinyin.NORMAL, errors="default"
                )
            ]
        ).lower()

    @cached_property
    def to_me(self) -> bool:
        """检查消息是否是发送给机器人的"""
        if hasattr(self, "_event") and self._event:
            message_chain = self._event.get_messages()
            for comp in message_chain:
                if isinstance(comp, At):
                    if str(comp.qq) == str(self.bot_id) or str(comp.qq) == "all":
                        return True
        # 检查纯文本是否以 bot 开头
        return self.plain_text.strip().lower().startswith("bot")


class ChatConfig:
    """聊天配置管理类 - 支持嵌套配置"""

    def __init__(self, plugin_config: AstrBotConfig):
        # 调试相关
        self.debug_message_format = getattr(
            plugin_config, "debug_message_format", False
        )

        # 从嵌套的 object 中读取配置
        # learning 分组
        learning_config = plugin_config.get("learning", {})
        self.answer_threshold = learning_config.get("answer_threshold", 3)
        self.answer_threshold_weights = learning_config.get(
            "answer_threshold_weights", [7, 23, 70]
        )
        self.topics_size = learning_config.get("topics_size", 16)
        self.topics_importance = learning_config.get("topics_importance", 10000)
        self.cross_group_threshold = learning_config.get("cross_group_threshold", 2)
        self.duplicate_reply = learning_config.get("duplicate_reply", 10)
        self.split_probability = learning_config.get("split_probability", 0.5)

        # storage 分组
        storage_config = plugin_config.get("storage", {})
        self.save_time_threshold = storage_config.get("save_time_threshold", 300)
        self.save_count_threshold = storage_config.get("save_count_threshold", 50)
        self.save_reserved_size = storage_config.get("save_reserved_size", 100)
        self.cleanup_expired_days = storage_config.get("cleanup_expired_days", 15)

        # media 分组
        media_config = plugin_config.get("media", {})
        self.enable_image_learning = media_config.get("enable_image_learning", True)
        self.image_similarity_threshold = media_config.get(
            "image_similarity_threshold", 0.8
        )

    @property
    def answer_threshold_choice_list(self) -> list[int]:
        return list(
            range(
                self.answer_threshold - len(self.answer_threshold_weights) + 1,
                self.answer_threshold + 1,
            )
        )


class ChatStateManager:
    """聊天状态管理器 - 管理内存缓存"""

    def __init__(self, config: ChatConfig):
        self.config = config
        self._reply_lock = asyncio.Lock()
        self._message_lock = asyncio.Lock()
        self._topics_lock = asyncio.Lock()

        self._reply_dict: defaultdict[str, defaultdict[str, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._message_dict: defaultdict[str, list[MessageModel]] = defaultdict(list)
        self._recent_topics: defaultdict[str, deque[str]] = defaultdict(
            lambda: deque(maxlen=config.topics_size)
        )
        self._recent_speak: defaultdict[str, deque[str]] = defaultdict(
            lambda: deque(maxlen=config.duplicate_reply)
        )

        self._blacklist_answer: defaultdict[str, set[str]] = defaultdict(set)
        self._blacklist_answer_reserve: defaultdict[str, set[str]] = defaultdict(set)

        self._late_save_time: int = 0

    async def _sync(self, cur_time: int = int(time.time())):
        """持久化消息到数据库"""
        if db.db_operations is None:
            logger.warning("db_operations not initialized, skipping sync")
            return

        async with self._message_lock:
            save_list = [
                msg
                for group_msgs in self._message_dict.values()
                for msg in group_msgs
                if msg.time > self._late_save_time
            ]
            if not save_list:
                return

            new_dict = {
                group_id: group_msgs[-self.config.save_reserved_size :]
                for group_id, group_msgs in self._message_dict.items()
            }
            self._message_dict.clear()
            self._message_dict.update(new_dict)
            self._late_save_time = cur_time

        for msg in save_list:
            await db.db_operations.save_message(msg)

    def get_group_bot_replies(self, group_id: str, bot_id: str) -> list[dict]:
        return self._reply_dict[group_id][bot_id]

    def get_group_messages(self, group_id: str) -> list[MessageModel]:
        return self._message_dict[group_id]

    async def add_reply(self, group_id: str, bot_id: str, reply_data: dict) -> None:
        async with self._reply_lock:
            self._reply_dict[group_id][bot_id].append(reply_data)
            if len(self._reply_dict[group_id][bot_id]) > self.config.save_reserved_size:
                self._reply_dict[group_id][bot_id] = self._reply_dict[group_id][bot_id][
                    -self.config.save_reserved_size :
                ]

    async def add_message(self, group_id: str, message: MessageModel) -> None:
        async with self._message_lock:
            self._message_dict[group_id].append(message)

    async def add_topics(self, group_id: str, topics: list[str]) -> None:
        async with self._topics_lock:
            self._recent_topics[group_id].extend(
                [k for k in topics if not k.startswith("bot")]
            )


class Chat:
    """聊天学习和回复核心类 - 增强版"""

    BLACKLIST_FLAG: int = 114514
    SPEAK_FLAG: str = "[Bot: Speak]"
    REPLY_FLAG: str = "[Bot: Reply]"

    def __init__(
        self, data: ChatData | AstrMessageEvent, plugin_config: AstrBotConfig
    ) -> None:
        self.config = ChatConfig(plugin_config)
        # 使用全局状态管理器
        self.state = get_global_state_manager(self.config)

        if isinstance(data, AstrMessageEvent):
            plain_text, message_type, image_info, is_reply, has_media = (
                self._extract_message_content(data)
            )
            self.chat_data = ChatData(
                group_id=data.get_group_id(),
                user_id=data.get_sender_id(),
                plain_text=plain_text,
                time=data.message_obj.timestamp,
                bot_id=data.get_self_id(),
                message_type=message_type,
                image_hash=image_info.get("hash") if image_info else None,
                image_url=image_info.get("url") if image_info else None,
                is_reply=is_reply,
                has_media_content=has_media,
                _event=data,
            )
            if self.config.debug_message_format:
                self._log_message_structure(data)
        else:
            self.chat_data = data

    def _log_message_structure(self, event: AstrMessageEvent) -> None:
        """记录消息结构用于调试"""
        try:
            message_obj = event.message_obj
            message_chain = getattr(message_obj, "message", [])

            chain_info = []
            for idx, comp in enumerate(message_chain):
                comp_info = {
                    "index": idx,
                    "type": type(comp).__name__,
                }
                if isinstance(comp, Plain):
                    comp_info["text"] = comp.text[:100] if comp.text else ""
                elif isinstance(comp, Image):
                    comp_info["file"] = comp.file[:50] if comp.file else ""
                    comp_info["url"] = comp.url[:50] if comp.url else ""
                elif isinstance(comp, Face):
                    comp_info["id"] = comp.id
                elif isinstance(comp, Record):
                    comp_info["file"] = comp.file[:50] if comp.file else ""
                elif isinstance(comp, Video):
                    comp_info["file"] = comp.file[:50] if comp.file else ""
                elif isinstance(comp, File):
                    comp_info["name"] = comp.name[:50] if comp.name else ""
                chain_info.append(comp_info)

            log_data = {
                "message_id": getattr(message_obj, "message_id", "N/A"),
                "message_type": getattr(message_obj, "type", "N/A"),
                "message_chain": chain_info,
                "message_str": getattr(message_obj, "message_str", "N/A"),
            }
            logger.info(
                "chatimitate DEBUG: Message structure:\n%s",
                str(log_data),
            )
        except Exception as e:
            logger.warning("chatimitate DEBUG: Failed to log message structure: %s", e)

    def _extract_message_content(
        self, event: AstrMessageEvent
    ) -> tuple[str, str, dict | None, bool, bool]:
        """
        提取消息内容 - 完全结构化版本，支持多媒体

        Returns:
            tuple: (plain_text, message_type, image_info, is_reply, has_media)
                - plain_text: 纯文本内容
                - message_type: 消息类型 (text, image, record, video, file, mixed)
                - image_info: 图片信息 {hash, url}
                - is_reply: 是否是回复消息
                - has_media: 是否包含多媒体内容
        """
        plain_text_parts = []
        message_chain = event.get_messages()

        has_image = False
        has_record = False
        has_video = False
        has_file = False
        is_reply = False
        image_info: dict | None = None

        if not message_chain:
            plain_text = event.get_message_str() or ""
            return plain_text, "text", None, False, False

        for comp in message_chain:
            if isinstance(comp, Plain):
                text = comp.text.strip()
                if text:
                    plain_text_parts.append(text)
            elif isinstance(comp, Reply):
                # 使用结构化方式检测回复
                is_reply = True
            elif isinstance(comp, Image):
                has_image = True
                image_url = comp.url or comp.file or ""
                if image_url and not image_info:
                    # 提取图片哈希或 URL
                    image_info = {
                        "url": image_url,
                        "hash": self._compute_image_hash(image_url),
                    }
            elif isinstance(comp, Record):
                has_record = True
            elif isinstance(comp, Video):
                has_video = True
            elif isinstance(comp, File):
                has_file = True

        plain_text = " ".join(plain_text_parts)

        if not plain_text:
            plain_text = event.get_message_str() or ""

        # 确定消息类型
        message_type = self._determine_message_type(
            has_image, has_record, has_video, has_file, bool(plain_text)
        )

        # 判断是否包含媒体内容
        has_media = has_image or has_record or has_video or has_file

        return plain_text, message_type, image_info, is_reply, has_media

    def _determine_message_type(
        self,
        has_image: bool,
        has_record: bool,
        has_video: bool,
        has_file: bool,
        has_text: bool,
    ) -> str:
        """确定消息的主要类型"""
        types = []
        if has_image:
            types.append("image")
        if has_record:
            types.append("record")
        if has_video:
            types.append("video")
        if has_file:
            types.append("file")

        if len(types) == 0:
            return "text" if has_text else "text"
        elif len(types) == 1:
            return types[0] if not has_text else "mixed"
        else:
            return "mixed"

    def _compute_image_hash(self, image_url: str) -> str:
        """计算图片哈希，用于去重"""
        # 从 URL 提取关键信息生成哈希
        url_parts = image_url.split("/")[-1] if "/" in image_url else image_url
        return hashlib.md5(url_parts.encode()).hexdigest()[:16]

    def _build_raw_message_description(self) -> str:
        """
        构建原始消息的结构化描述（用于调试和日志）

        由于我们使用结构化组件，不再存储 CQ 码字符串，
        此方法生成一个人类可读的消息描述。
        """
        parts = []

        # 添加纯文本部分
        if self.chat_data.plain_text:
            parts.append(self.chat_data.plain_text[:50])  # 限制长度

        # 添加媒体类型标记
        if self.chat_data.is_image:
            parts.append(
                f"[图片:{self.chat_data.image_url or self.chat_data.image_hash}]"
            )
        elif self.chat_data.is_record:
            parts.append("[语音]")
        elif self.chat_data.is_video:
            parts.append("[视频]")
        elif self.chat_data.is_file:
            parts.append("[文件]")

        return " ".join(parts) if parts else ""

    async def learn(self) -> bool:
        """学习消息 - 增强版"""
        # 检查消息是否为空（纯文本和多媒体内容都为空）
        if (
            len(self.chat_data.plain_text.strip()) == 0
            and not self.chat_data.has_media()
        ):
            return False

        # 检查是否启用图片学习
        if self.chat_data.is_image and not self.config.enable_image_learning:
            return False

        if db.db_operations is None:
            logger.error("chatimitate: db not initialized")
            return False

        group_id = self.chat_data.group_id
        group_msgs = self.state.get_group_messages(group_id)

        if group_msgs:
            group_pre_msg = group_msgs[-1]
            await self._context_insert(group_pre_msg)

            user_id = self.chat_data.user_id
            if group_pre_msg and group_pre_msg.user_id != user_id:
                for msg in reversed(group_msgs[-3:]):
                    if msg.user_id == user_id:
                        await self._context_insert(msg)
                        break

        await self._message_insert()
        return True

    async def answer(self) -> AsyncGenerator[str, None]:
        """回复消息 - 增强版"""
        # 不过滤短消息，因为可能是图片或语音
        if self.chat_data.is_plain_text and len(self.chat_data.plain_text) < 2:
            return

        results = await self._context_find()
        if not results:
            return

        answer_list, answer_keywords = results
        group_id = self.chat_data.group_id
        bot_id = self.chat_data.bot_id

        await self.state.add_reply(
            group_id,
            bot_id,
            {
                "time": int(time.time()),
                "pre_plain_text": self.chat_data.plain_text,
                "pre_keywords": self.chat_data.keywords,
                "reply": self.REPLY_FLAG,
                "reply_keywords": self.REPLY_FLAG,
            },
        )

        for item in answer_list:
            await self.state.add_reply(
                group_id,
                bot_id,
                {
                    "time": int(time.time()),
                    "pre_plain_text": self.chat_data.plain_text,
                    "pre_keywords": self.chat_data.keywords,
                    "reply": item,
                    "reply_keywords": answer_keywords,
                },
            )

            if not self.chat_data.has_media():
                await self.state.add_topics(
                    group_id,
                    [k for k in answer_keywords.split(" ") if not k.startswith("bot")],
                )

            await self.state.add_topics(group_id, self.chat_data._keywords_list)
            yield item

    async def _message_insert(self):
        """插入消息到缓存并检查是否需要持久化"""
        group_id = self.chat_data.group_id

        # 构建原始消息的结构化描述（用于调试和日志）
        raw_message_desc = self._build_raw_message_description()

        await self.state.add_message(
            group_id,
            MessageModel(
                group_id=group_id,
                user_id=self.chat_data.user_id,
                raw_message=raw_message_desc,
                is_plain_text=self.chat_data.is_plain_text,
                plain_text=self.chat_data.plain_text,
                keywords=self.chat_data.keywords,
                time=self.chat_data.time,
            ),
        )

        if self.chat_data.is_plain_text:
            await self.state.add_topics(group_id, self.chat_data._keywords_list)

        cur_time = self.chat_data.time
        if self.state._late_save_time == 0:
            self.state._late_save_time = cur_time - 1
            logger.debug(
                "chatimitate: 首次记录消息，设置保存时间标记为 %s",
                self.state._late_save_time,
            )
            return

        # 检查是否需要保存到数据库
        group_msgs = self.state.get_group_messages(group_id)
        msg_count = len(group_msgs)
        time_diff = cur_time - self.state._late_save_time

        if msg_count > self.config.save_count_threshold:
            logger.info(
                "chatimitate: 消息数量达到阈值 (%d > %d)，保存到数据库",
                msg_count,
                self.config.save_count_threshold,
            )
            await self.state._sync(cur_time)
        elif time_diff > self.config.save_time_threshold:
            logger.info(
                "chatimitate: 距离上次保存超过阈值 (%d 秒 > %d 秒)，保存到数据库",
                time_diff,
                self.config.save_time_threshold,
            )
            await self.state._sync(cur_time)
        else:
            # 调试日志，显示当前缓存状态
            logger.debug(
                "chatimitate: 消息已缓存 (%d 条，距离上次保存 %d 秒)，未达到保存阈值",
                msg_count,
                time_diff,
            )

    async def _context_insert(self, pre_msg: MessageModel | None):
        """插入上下文关系"""
        if not pre_msg or db.db_operations is None:
            return

        plain_text = self.chat_data.plain_text
        if pre_msg.plain_text == plain_text:
            return

        # 使用结构化方式检查是否有回复组件
        if self.chat_data.is_reply:
            return

        keywords = self.chat_data.keywords
        group_id = self.chat_data.group_id
        pre_keywords = pre_msg.keywords
        cur_time = self.chat_data.time

        context = await db.db_operations.get_trigger_keyword(pre_keywords)
        if context:
            answer_index = next(
                (
                    idx
                    for idx, answer in enumerate(context.replies)
                    if answer.group_id == group_id and answer.keywords == keywords
                ),
                -1,
            )
            if answer_index != -1:
                context.replies[answer_index].count += 1
                context.replies[answer_index].time = cur_time
                if self.chat_data.is_plain_text:
                    context.replies[answer_index].messages.append(plain_text)
            else:
                context.replies.append(
                    Answer(
                        keywords=keywords,
                        group_id=group_id,
                        count=1,
                        time=cur_time,
                        messages=[plain_text],
                    )
                )
            context.time = cur_time
            context.trigger_count += 1
            await db.db_operations.save_trigger_keyword(context)
        else:
            context = Context(
                keywords=pre_keywords,
                time=cur_time,
                trigger_count=1,
                replies=[
                    Answer(
                        keywords=keywords,
                        group_id=group_id,
                        count=1,
                        time=cur_time,
                        messages=[plain_text],
                    )
                ],
            )
            await db.db_operations.save_trigger_keyword(context)

    async def _context_find(self) -> tuple[list[str], str] | None:
        """查找上下文并生成回复"""
        group_id = self.chat_data.group_id
        keywords = self.chat_data.keywords
        bot_id = self.chat_data.bot_id

        if db.db_operations is None:
            return None

        context = await db.db_operations.get_trigger_keyword(keywords)
        if not context:
            return None

        answer_count_threshold = random.choices(
            self.config.answer_threshold_choice_list,
            weights=self.config.answer_threshold_weights,
        )[0]
        if self.chat_data.keywords_len == ChatData._keywords_size:
            answer_count_threshold -= 1

        cross_group_threshold = (
            1 if self.chat_data.to_me else self.config.cross_group_threshold
        )

        ban_keywords = await self._find_ban_keywords(context, group_id)

        candidate_answers: dict[str, Answer] = {}
        other_group_cache: dict[str, Answer] = {}
        answers_count: defaultdict[str, int] = defaultdict(int)

        group_bot_replies = self.state.get_group_bot_replies(group_id, bot_id)
        recent_replies = [
            r["reply_keywords"]
            for r in group_bot_replies[-self.config.duplicate_reply :]
        ]
        recent_message = [
            m.raw_message
            for m in self.state.get_group_messages(group_id)[
                -self.config.duplicate_reply :
            ]
        ]

        def candidate_append(dst: dict[str, Answer], answer: Answer):
            answer_key = answer.keywords
            # 检查是否是纯文本关键词（不是多媒体消息）
            is_pure_text = not answer_key.startswith("[")
            if is_pure_text:
                topics = self.state._recent_topics[group_id]
                for key in answer_key.split(" "):
                    if key in topics:
                        answer.topical += topics.count(key)

            if answer_key not in dst:
                dst[answer_key] = answer
            else:
                pre_answer = dst[answer_key]
                pre_answer.count += answer.count
                pre_answer.messages += answer.messages

        for answer in context.replies:
            if answer.count < answer_count_threshold:
                continue

            answer_key = answer.keywords
            if (
                answer_key in ban_keywords
                or answer_key in recent_replies
                or answer_key == keywords
            ):
                continue

            sample_msg = answer.messages[0]
            # 优化：支持多媒体消息类型检测
            if self.chat_data.is_image and not sample_msg.startswith("[图片]"):
                continue
            if sample_msg.startswith("bot") and (
                not self.chat_data.to_me or len(sample_msg) <= 6
            ):
                continue
            if "\n" in sample_msg:
                continue
            if sample_msg.strip().isdigit():
                continue
            if answer.count < 3 and sample_msg in recent_message:
                continue

            if answer.group_id == group_id:
                candidate_append(candidate_answers, answer)
            elif sample_msg.startswith("[at:"):
                # 结构化 At 检测
                continue
            else:
                answers_count[answer_key] += 1
                cur_count = answers_count[answer_key]
                if cur_count < cross_group_threshold:
                    candidate_append(other_group_cache, answer)
                elif cur_count == cross_group_threshold:
                    if cur_count > 1:
                        candidate_append(
                            candidate_answers, other_group_cache[answer_key]
                        )
                    candidate_append(candidate_answers, answer)
                else:
                    candidate_append(candidate_answers, answer)

        if not candidate_answers:
            return None

        weights = [
            min(answer.count, 10) + answer.topical * self.config.topics_importance
            for answer in candidate_answers.values()
        ]
        final_answer = random.choices(
            list(candidate_answers.values()), weights=weights
        )[0]
        answer_str = random.choice(final_answer.messages).removeprefix("bot")

        logger.info(
            "chatimitate: selected answer keywords=%s msg_preview=%s",
            final_answer.keywords,
            (answer_str[:60] + "…") if len(answer_str) > 60 else answer_str,
        )

        if (
            0 < answer_str.count(",") <= 3
            and not answer_str.startswith("[")
            and random.random() < self.config.split_probability
        ):
            return answer_str.split(","), final_answer.keywords
        return [answer_str], final_answer.keywords

    @staticmethod
    async def clearup_context(expired_days: int = 15) -> None:
        """
        清理过期上下文

        Args:
            expired_days: 多少天内的数据需要保留，超出这个天数的低频数据会被清理
        """
        cur_time = int(time.time())
        expiration = cur_time - expired_days * 24 * 3600

        if db.db_operations is None:
            return

        await db.db_operations.clear_expired_triggers(expiration)

    @staticmethod
    async def _find_ban_keywords(
        context: "TriggerKeyword | None", group_id: str
    ) -> set[str]:
        """查找禁用的关键词（从 disabled_replies 表）"""
        ban_keywords: set[str] = set()

        # 检查上下文关联的禁用记录
        if context is not None and hasattr(context, "disabled"):
            for disabled in context.disabled:
                # 如果是当前群的禁用记录，直接加入
                if disabled.group_id == group_id:
                    ban_keywords.add(disabled.keywords)

        return ban_keywords

    @staticmethod
    async def sync():
        """
        同步数据到数据库（使用全局状态管理器）
        """
        if db.db_operations is None:
            logger.warning("chatimitate: db_operations not initialized, skipping sync")
            return

        global _global_state_manager
        if _global_state_manager is None:
            logger.debug(
                "chatimitate: global state manager not initialized, skipping sync"
            )
            return

        # 使用全局状态管理器进行同步
        await _global_state_manager._sync()


# 全局状态管理器（单例模式）
_global_state_manager: ChatStateManager | None = None
_global_config: ChatConfig | None = None


def get_global_state_manager(config: ChatConfig) -> ChatStateManager:
    """获取或创建全局状态管理器（单例模式）"""
    global _global_state_manager, _global_config

    if _global_state_manager is None or _global_config != config:
        _global_state_manager = ChatStateManager(config)
        _global_config = config

    return _global_state_manager
