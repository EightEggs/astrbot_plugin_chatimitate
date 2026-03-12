"""
AstrBot ChatImitate Plugin - Core Logic Module
核心聊天学习逻辑模块 - 适配AstrBot框架
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

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import At, File, Image, Plain, Record, Reply, Video

from . import db
from .config import ChatImitateConfig
from .db import ChatMessage, ReplyContent, TriggerKeyword


@dataclass
class ChatData:
    """聊天数据结构"""

    group_id: str
    user_id: str
    plain_text: str
    time: int
    bot_id: str
    message_type: str = "text"
    image_hash: str | None = None
    image_url: str | None = None
    is_reply: bool = False
    has_media_content: bool = False
    _event: AstrMessageEvent | None = None
    _keywords_size: int = 2

    @cached_property
    def is_plain_text(self) -> bool:
        return self.message_type == "text" and len(self.plain_text) != 0

    @cached_property
    def is_image(self) -> bool:
        return self.message_type in ("image", "mixed") and self.image_url is not None

    @cached_property
    def _keywords_list(self) -> list[str]:
        if not self.is_plain_text and len(self.plain_text) == 0:
            return []
        try:
            keywords = jieba_analyse.extract_tags(
                self.plain_text, topK=ChatData._keywords_size, withWeight=True
            )
            return [
                item[0] if isinstance(item, (list, tuple)) else str(item)
                for item in keywords
            ]
        except Exception:
            return [self.plain_text] if self.plain_text else []

    @cached_property
    def keywords_len(self) -> int:
        return len(self._keywords_list)

    @cached_property
    def keywords(self) -> str:
        if self.is_image and not self.is_plain_text:
            return f"[图片]{self.image_hash or ''}"
        if not self.is_plain_text and len(self.plain_text) == 0:
            return f"[{self.message_type}]"
        return " ".join(self._keywords_list) if self.keywords_len > 0 else self.plain_text

    @cached_property
    def to_me(self) -> bool:
        if self._event:
            message_chain = self._event.get_messages()
            for comp in message_chain:
                if isinstance(comp, At):
                    if str(comp.qq) == str(self.bot_id) or str(comp.qq) == "all":
                        return True
        return self.plain_text.strip().lower().startswith("bot")


class ChatStateManager:
    """内存状态管理器"""

    def __init__(self, config: ChatImitateConfig):
        self.config = config
        self._reply_lock = asyncio.Lock()
        self._message_lock = asyncio.Lock()
        self._topics_lock = asyncio.Lock()

        self._reply_dict: defaultdict[str, defaultdict[str, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._message_dict: defaultdict[str, list[ChatMessage]] = defaultdict(list)
        self._recent_topics: defaultdict[str, deque[str]] = defaultdict(
            lambda: deque(maxlen=config.topics_size)
        )
        self._recent_speak: defaultdict[str, deque[str]] = defaultdict(
            lambda: deque(maxlen=config.duplicate_reply)
        )

        self._blacklist_answer: defaultdict[str, set[str]] = defaultdict(set)
        self._blacklist_answer_reserve: defaultdict[str, set[str]] = defaultdict(set)

        self._late_save_time: int = 0

    async def _sync(self, cur_time: int | None = None):
        """持久化消息到数据库"""
        if db.db_operations is None:
            logger.warning("chatimitate: db_operations not initialized")
            return

        if cur_time is None:
            cur_time = int(time.time())

        async with self._message_lock:
            save_list = [
                msg
                for group_msgs in self._message_dict.values()
                for msg in group_msgs
                if msg.time >= self._late_save_time
            ]
            if not save_list:
                return

            self._late_save_time = max(msg.time for msg in save_list) + 1

            new_dict = {
                group_id: group_msgs[-self.config.save_reserved_size:]
                for group_id, group_msgs in self._message_dict.items()
            }
            self._message_dict.clear()
            self._message_dict.update(new_dict)

        for msg in save_list:
            await db.db_operations.save_message(msg)

    def get_group_bot_replies(self, group_id: str, bot_id: str) -> list[dict]:
        return self._reply_dict[group_id][bot_id]

    def get_group_messages(self, group_id: str) -> list[ChatMessage]:
        return self._message_dict[group_id]

    async def add_reply(self, group_id: str, bot_id: str, reply_data: dict) -> None:
        async with self._reply_lock:
            self._reply_dict[group_id][bot_id].append(reply_data)
            if len(self._reply_dict[group_id][bot_id]) > self.config.save_reserved_size:
                self._reply_dict[group_id][bot_id] = self._reply_dict[group_id][bot_id][
                    -self.config.save_reserved_size:
                ]

    async def add_message(self, group_id: str, message: ChatMessage) -> None:
        async with self._message_lock:
            self._message_dict[group_id].append(message)

    async def add_topics(self, group_id: str, topics: list[str]) -> None:
        async with self._topics_lock:
            self._recent_topics[group_id].extend(
                [k for k in topics if not k.startswith("bot")]
            )


class Chat:
    """聊天学习和回复核心类"""

    REPLY_FLAG: str = "__REPLY_MARKER__"

    def __init__(self, data: ChatData | AstrMessageEvent, plugin_config: ChatImitateConfig) -> None:
        self.config = plugin_config
        self.state = get_global_state_manager(self.config)

        if isinstance(data, AstrMessageEvent):
            plain_text, message_type, image_info, is_reply, has_media = self._extract_message_content(data)
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
        else:
            self.chat_data = data

    def _extract_message_content(
        self, event: AstrMessageEvent
    ) -> tuple[str, str, dict | None, bool, bool]:
        """提取消息内容"""
        plain_text_parts = []
        message_chain = event.get_messages()

        has_image = has_record = has_video = has_file = False
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
                is_reply = True
            elif isinstance(comp, Image):
                has_image = True
                image_url = comp.url or comp.file or ""
                if image_url and not image_info:
                    image_info = {"url": image_url, "hash": self._compute_image_hash(image_url)}
            elif isinstance(comp, Record):
                has_record = True
            elif isinstance(comp, Video):
                has_video = True
            elif isinstance(comp, File):
                has_file = True

        plain_text = " ".join(plain_text_parts)
        if not plain_text:
            plain_text = event.get_message_str() or ""

        message_type = self._determine_message_type(has_image, has_record, has_video, has_file, bool(plain_text))
        has_media = has_image or has_record or has_video or has_file

        return plain_text, message_type, image_info, is_reply, has_media

    def _determine_message_type(self, has_image: bool, has_record: bool, has_video: bool, has_file: bool, has_text: bool) -> str:
        """确定消息类型"""
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
        return "mixed"

    def _compute_image_hash(self, image_url: str) -> str:
        """计算图片哈希"""
        url_parts = image_url.split("/")[-1] if "/" in image_url else image_url
        return hashlib.md5(url_parts.encode()).hexdigest()[:16]

    def _build_raw_message_description(self) -> str:
        """构建原始消息描述"""
        parts = []
        if self.chat_data.plain_text:
            parts.append(self.chat_data.plain_text[:50])
        if self.chat_data.is_image:
            parts.append(f"[图片:{self.chat_data.image_url or self.chat_data.image_hash}]")
        elif self.chat_data.has_media_content:
            parts.append(f"[{self.chat_data.message_type}]")
        return " ".join(parts) if parts else ""

    async def learn(self) -> bool:
        """学习消息"""
        if len(self.chat_data.plain_text.strip()) == 0 and not self.chat_data.has_media_content:
            return False

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

        await self._message_insert()
        return True

    async def answer(self) -> AsyncGenerator[str, None]:
        """生成回复"""
        if self.chat_data.is_plain_text and len(self.chat_data.plain_text) < 2:
            return

        results = await self._context_find()
        if not results:
            return

        answer_list, answer_keywords = results
        group_id = self.chat_data.group_id
        bot_id = self.chat_data.bot_id

        await self.state.add_reply(
            group_id, bot_id,
            {"time": int(time.time()), "pre_plain_text": self.chat_data.plain_text,
             "pre_keywords": self.chat_data.keywords, "reply": self.REPLY_FLAG,
             "reply_keywords": self.REPLY_FLAG}
        )

        for item in answer_list:
            await self.state.add_reply(
                group_id, bot_id,
                {"time": int(time.time()), "pre_plain_text": self.chat_data.plain_text,
                 "pre_keywords": self.chat_data.keywords, "reply": item,
                 "reply_keywords": answer_keywords}
            )

            if not self.chat_data.has_media_content:
                await self.state.add_topics(
                    group_id, [k for k in answer_keywords.split(" ") if not k.startswith("bot")]
                )

            await self.state.add_topics(group_id, self.chat_data._keywords_list)
            yield item

    async def _message_insert(self):
        """插入消息到缓存"""
        group_id = self.chat_data.group_id
        raw_message_desc = self._build_raw_message_description()

        await self.state.add_message(
            group_id,
            ChatMessage(
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

        group_msgs = self.state.get_group_messages(group_id)
        msg_count = len(group_msgs)
        time_diff = cur_time - self.state._late_save_time

        if msg_count > self.config.save_count_threshold:
            await self.state._sync(cur_time)
        elif time_diff > self.config.save_time_threshold:
            await self.state._sync(cur_time)

    async def _context_insert(self, pre_msg: ChatMessage | None):
        """插入上下文关系"""
        if not pre_msg or db.db_operations is None:
            return

        plain_text = self.chat_data.plain_text
        if pre_msg.plain_text == plain_text:
            return

        if self.chat_data.is_reply:
            return

        keywords = self.chat_data.keywords
        group_id = self.chat_data.group_id
        pre_keywords = pre_msg.keywords
        cur_time = self.chat_data.time

        reply_content = plain_text
        if self.chat_data.is_image and self.chat_data.image_hash:
            reply_content = f"[图片:{self.chat_data.image_hash}]"
        elif self.chat_data.is_image and self.chat_data.image_url:
            reply_content = f"[图片:{self.chat_data.image_url}]"

        context = await db.db_operations.get_trigger_keyword(pre_keywords)
        if context:
            existing_reply = next(
                (answer for answer in context.replies
                 if answer.group_id == group_id and answer.keywords == keywords),
                None
            )

            if existing_reply:
                existing_reply.count += 1
                existing_reply.time = cur_time
                if reply_content and reply_content not in existing_reply.messages:
                    existing_reply.messages.append(reply_content)
            else:
                context.replies.append(
                    ReplyContent(keywords=keywords, group_id=group_id, count=1,
                               time=cur_time, messages=[reply_content])
                )

            context.time = cur_time
            context.trigger_count += 1
            await db.db_operations.save_trigger_keyword(context)
        else:
            context = TriggerKeyword(
                keywords=pre_keywords,
                time=cur_time,
                trigger_count=1,
                replies=[
                    ReplyContent(keywords=keywords, group_id=group_id, count=1,
                                time=cur_time, messages=[reply_content])
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

        cross_group_threshold = 1 if self.chat_data.to_me else self.config.cross_group_threshold

        ban_keywords = await self._find_ban_keywords(context, group_id)

        candidate_answers: dict[str, ReplyContent] = {}
        other_group_cache: dict[str, ReplyContent] = {}
        answers_count: defaultdict[str, int] = defaultdict(int)

        group_bot_replies = self.state.get_group_bot_replies(group_id, bot_id)
        recent_replies = [r["reply_keywords"] for r in group_bot_replies[-self.config.duplicate_reply:]]
        recent_message = [
            m.raw_message for m in self.state.get_group_messages(group_id)[-self.config.duplicate_reply:]
        ]

        def candidate_append(dst: dict[str, ReplyContent], answer: ReplyContent):
            answer_key = answer.keywords
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
            if answer_key in ban_keywords or answer_key in recent_replies or answer_key == keywords:
                continue

            sample_msg = answer.messages[0]
            if self.chat_data.is_image and not sample_msg.startswith("[图片]"):
                continue
            if sample_msg.startswith("bot") and (not self.chat_data.to_me or len(sample_msg) <= 6):
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
                continue
            else:
                answers_count[answer_key] += 1
                cur_count = answers_count[answer_key]
                if cur_count < cross_group_threshold:
                    candidate_append(other_group_cache, answer)
                elif cur_count == cross_group_threshold:
                    if cur_count > 1:
                        candidate_append(candidate_answers, other_group_cache[answer_key])
                    candidate_append(candidate_answers, answer)
                else:
                    candidate_append(candidate_answers, answer)

        if not candidate_answers:
            return None

        weights = [
            min(answer.count, 10) + answer.topical * self.config.topics_importance
            for answer in candidate_answers.values()
        ]
        final_answer = random.choices(list(candidate_answers.values()), weights=weights)[0]
        answer_str = random.choice(final_answer.messages).removeprefix("bot")

        if (
            0 < answer_str.count(",") <= 3
            and not answer_str.startswith("[")
            and random.random() < self.config.split_probability
        ):
            return answer_str.split(","), final_answer.keywords
        return [answer_str], final_answer.keywords

    @staticmethod
    async def clearup_context(expired_days: int = 15) -> None:
        """清理过期上下文"""
        cur_time = int(time.time())
        expiration = cur_time - expired_days * 24 * 3600

        if db.db_operations is None:
            return

        await db.db_operations.clear_expired_triggers(expiration)

    @staticmethod
    async def _find_ban_keywords(context: TriggerKeyword | None, group_id: str) -> set[str]:
        """查找禁用的关键词"""
        ban_keywords: set[str] = set()

        if context is not None and hasattr(context, "disabled"):
            for disabled in context.disabled:
                if disabled.group_id == group_id:
                    ban_keywords.add(disabled.keywords)

        return ban_keywords

    @staticmethod
    async def sync():
        """同步数据到数据库"""
        global _global_state_manager
        if _global_state_manager is None or db.db_operations is None:
            return
        await _global_state_manager._sync()


_global_state_manager: ChatStateManager | None = None
_global_config: ChatImitateConfig | None = None


def get_global_state_manager(config: ChatImitateConfig) -> ChatStateManager:
    """获取全局状态管理器（单例）"""
    global _global_state_manager, _global_config

    if _global_state_manager is None or _global_config != config:
        _global_state_manager = ChatStateManager(config)
        _global_config = config

    return _global_state_manager
