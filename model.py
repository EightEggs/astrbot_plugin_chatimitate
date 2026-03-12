"""
AstrBot ChatImitate Plugin - Core Logic Module
核心聊天学习和回复逻辑 - 增强版
支持图片、语音、视频等多媒体消息
"""

import asyncio
import hashlib
import random
import re
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
    Face,
    File,
    Image,
    Plain,
    Record,
    Video,
)

from . import db
from .db import Answer, Context
from .db import Message as MessageModel


@dataclass
class ChatData:
    """聊天数据结构 - 增强版"""

    group_id: str
    user_id: str
    raw_message: str
    plain_text: str
    time: int
    bot_id: str
    message_type: str = "text"  # text, image, record, video, file, mixed
    image_hash: str | None = None  # 图片哈希，用于去重
    image_url: str | None = None  # 图片 URL
    _keywords_size: int = 2

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
        return [
            tag
            for tag, _ in jieba_analyse.extract_tags(
                self.plain_text, topK=ChatData._keywords_size
            )
        ]

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
            return self.raw_message
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
        bot_id = str(self.bot_id)
        # 检查是否包含 At 组件
        if (
            f"[CQ:at,qq={bot_id}]" in self.raw_message
            or "[CQ:at,qq=all]" in self.raw_message
        ):
            return True
        return self.plain_text.strip().lower().startswith("bot")


class ChatConfig:
    """聊天配置管理类 - 精简版"""

    def __init__(self, plugin_config: AstrBotConfig):
        # 调试相关
        self.debug_message_format = getattr(
            plugin_config, "debug_message_format", False
        )

        # 回复控制相关
        self.answer_threshold = getattr(plugin_config, "answer_threshold", 3)
        self.answer_threshold_weights = getattr(
            plugin_config, "answer_threshold_weights", [7, 23, 70]
        )
        self.topics_size = getattr(plugin_config, "topics_size", 16)
        self.topics_importance = getattr(plugin_config, "topics_importance", 10000)
        self.cross_group_threshold = getattr(plugin_config, "cross_group_threshold", 2)
        self.repeat_threshold = getattr(plugin_config, "repeat_threshold", 3)
        self.duplicate_reply = getattr(plugin_config, "duplicate_reply", 10)
        self.split_probability = getattr(plugin_config, "split_probability", 0.5)

        # 数据保存相关
        self.save_time_threshold = getattr(plugin_config, "save_time_threshold", 3600)
        self.save_count_threshold = getattr(plugin_config, "save_count_threshold", 1000)
        self.save_reserved_size = getattr(plugin_config, "save_reserved_size", 100)

        # 图片学习相关
        self.enable_image_learning = getattr(
            plugin_config, "enable_image_learning", True
        )
        self.image_similarity_threshold = getattr(
            plugin_config, "image_similarity_threshold", 0.8
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

        if isinstance(data, AstrMessageEvent):
            plain_text, raw_message, message_type, image_info = (
                self._extract_message_content(data)
            )
            self.chat_data = ChatData(
                group_id=data.get_group_id(),
                user_id=data.get_sender_id(),
                raw_message=raw_message,
                plain_text=plain_text,
                time=data.message_obj.timestamp,
                bot_id=data.get_self_id(),
                message_type=message_type,
                image_hash=image_info.get("hash") if image_info else None,
                image_url=image_info.get("url") if image_info else None,
            )
            if self.config.debug_message_format:
                self._log_message_structure(data)
        else:
            self.chat_data = data

        self.state = ChatStateManager(self.config)

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
    ) -> tuple[str, str, str, dict | None]:
        """
        提取消息内容 - 增强版，支持多媒体

        Returns:
            tuple: (plain_text, raw_message, message_type, image_info)
                - plain_text: 纯文本内容
                - raw_message: 格式化的消息字符串
                - message_type: 消息类型 (text, image, record, video, file, mixed)
                - image_info: 图片信息 {hash, url}
        """
        plain_text_parts = []
        raw_message_parts = []
        message_chain = event.get_messages()

        has_image = False
        has_record = False
        has_video = False
        has_file = False
        image_info: dict | None = None

        if not message_chain:
            plain_text = event.get_message_str() or ""
            raw_message = (
                str(event.message_obj.raw_message)
                if event.message_obj.raw_message
                else plain_text
            )
            return plain_text, raw_message, "text", None

        for comp in message_chain:
            if isinstance(comp, Plain):
                text = comp.text.strip()
                if text:
                    plain_text_parts.append(text)
                    raw_message_parts.append(text)
            elif isinstance(comp, Image):
                has_image = True
                image_url = comp.url or comp.file or ""
                if image_url:
                    # 规范化 URL
                    normalized_url = re.sub(r"\.image,.+?\]", ".image]", image_url)
                    raw_message_parts.append(f"[CQ:image,file={normalized_url}]")

                    # 提取图片哈希或 URL
                    if not image_info:
                        image_info = {
                            "url": image_url,
                            "hash": self._compute_image_hash(image_url),
                        }
            elif isinstance(comp, Face):
                raw_message_parts.append(f"[CQ:face,id={comp.id}]")
            elif isinstance(comp, Record):
                has_record = True
                file_path = comp.file or ""
                if file_path:
                    raw_message_parts.append(f"[CQ:record,file={file_path}]")
            elif isinstance(comp, Video):
                has_video = True
                file_path = getattr(comp, "file", None) or getattr(comp, "url", None) or ""
                if file_path:
                    raw_message_parts.append(f"[CQ:video,file={file_path}]")
            elif isinstance(comp, File):
                has_file = True
                file_name = comp.name or "unknown"
                if file_name:
                    raw_message_parts.append(f"[CQ:file,name={file_name}]")

        plain_text = " ".join(plain_text_parts)
        raw_message = "".join(raw_message_parts)

        if not raw_message:
            raw_message = (
                str(event.message_obj.raw_message)
                if event.message_obj.raw_message
                else ""
            )
            raw_message = re.sub(r"\.image,.+?\]", ".image]", raw_message)

        if not plain_text:
            plain_text = event.get_message_str() or ""

        # 确定消息类型
        message_type = self._determine_message_type(
            has_image, has_record, has_video, has_file, bool(plain_text)
        )

        return plain_text, raw_message, message_type, image_info

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

    async def learn(self) -> bool:
        """学习消息 - 增强版"""
        if len(self.chat_data.raw_message.strip()) == 0:
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

    async def answer(self) -> AsyncGenerator[str, None] | None:
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
                "pre_raw_message": self.chat_data.raw_message,
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
                    "pre_raw_message": self.chat_data.raw_message,
                    "pre_keywords": self.chat_data.keywords,
                    "reply": item,
                    "reply_keywords": answer_keywords,
                },
            )

            if "[CQ:" not in item:
                await self.state.add_topics(
                    group_id,
                    [k for k in answer_keywords.split(" ") if not k.startswith("bot")],
                )

            await self.state.add_topics(group_id, self.chat_data._keywords_list)
            yield item

    async def _message_insert(self):
        """插入消息到缓存并检查是否需要持久化"""
        group_id = self.chat_data.group_id

        await self.state.add_message(
            group_id,
            MessageModel(
                group_id=group_id,
                user_id=self.chat_data.user_id,
                bot_id=self.chat_data.bot_id,
                raw_message=self.chat_data.raw_message,
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
            return

        if (
            len(self.state.get_group_messages(group_id))
            > self.config.save_count_threshold
        ):
            await self._sync(cur_time)
        elif cur_time - self.state._late_save_time > self.config.save_time_threshold:
            await self._sync(cur_time)

    async def _sync(self, cur_time: int = int(time.time())):
        """持久化消息到数据库"""
        if db.db_operations is None:
            logger.warning("db_operations not initialized, skipping sync")
            return

        async with self.state._message_lock:
            save_list = [
                msg
                for group_msgs in self.state._message_dict.values()
                for msg in group_msgs
                if msg.time > self.state._late_save_time
            ]
            if not save_list:
                return

            new_dict = {
                group_id: group_msgs[-self.config.save_reserved_size :]
                for group_id, group_msgs in self.state._message_dict.items()
            }
            self.state._message_dict.clear()
            self.state._message_dict.update(new_dict)
            self.state._late_save_time = cur_time

        for msg in save_list:
            await db.db_operations.save_message(msg)

    async def _context_insert(self, pre_msg: MessageModel | None):
        """插入上下文关系"""
        if not pre_msg or db.db_operations is None:
            return

        plain_text = self.chat_data.plain_text
        if pre_msg.plain_text == plain_text or "[CQ:reply," in plain_text:
            return

        keywords = self.chat_data.keywords
        group_id = self.chat_data.group_id
        pre_keywords = pre_msg.keywords
        cur_time = self.chat_data.time

        context = await db.db_operations.get_context(pre_keywords)
        if context:
            answer_index = next(
                (
                    idx
                    for idx, answer in enumerate(context.answers)
                    if answer.group_id == group_id and answer.keywords == keywords
                ),
                -1,
            )
            if answer_index != -1:
                context.answers[answer_index].count += 1
                context.answers[answer_index].time = cur_time
                if self.chat_data.is_plain_text:
                    context.answers[answer_index].messages.append(plain_text)
            else:
                context.answers.append(
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
            await db.db_operations.save_context(context)
        else:
            context = Context(
                keywords=pre_keywords,
                time=cur_time,
                trigger_count=1,
                answers=[
                    Answer(
                        keywords=keywords,
                        group_id=group_id,
                        count=1,
                        time=cur_time,
                        messages=[plain_text],
                    )
                ],
            )
            await db.db_operations.save_context(context)

    async def _context_find(self) -> tuple[list[str], str] | None:
        """查找上下文并生成回复"""
        group_id = self.chat_data.group_id
        plain_text = self.chat_data.plain_text
        keywords = self.chat_data.keywords
        bot_id = self.chat_data.bot_id

        # 复读检测
        if group_id in self.state._message_dict:
            group_msgs = self.state._message_dict[group_id]
            if len(group_msgs) >= self.config.repeat_threshold and all(
                item.plain_text == plain_text
                for item in group_msgs[-self.config.repeat_threshold + 1 :]
            ):
                group_bot_replies = self.state.get_group_bot_replies(group_id, bot_id)
                if (
                    len(group_bot_replies)
                    and group_bot_replies[-1]["reply"] != plain_text
                ):
                    return [plain_text], keywords
                return None

        if db.db_operations is None:
            return None

        context = await db.db_operations.get_context(keywords)
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
            if "[CQ:" not in answer_key and not answer_key.startswith("["):
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

        for answer in context.answers:
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
            if (
                self.chat_data.is_image
                and "[CQ:" not in sample_msg
                and not sample_msg.startswith("[图片]")
            ):
                continue
            if sample_msg.startswith("bot") and (
                not self.chat_data.to_me or len(sample_msg) <= 6
            ):
                continue
            if sample_msg.startswith("[CQ:xml"):
                continue
            if "\n" in sample_msg:
                continue
            if "[CQ:" not in sample_msg and sample_msg.strip().isdigit():
                continue
            if answer.count < 3 and sample_msg in recent_message:
                continue

            if answer.group_id == group_id:
                candidate_append(candidate_answers, answer)
            elif "[CQ:at,qq=" in sample_msg:
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
            and "[CQ:" not in answer_str
            and not answer_str.startswith("[")
            and random.random() < self.config.split_probability
        ):
            return answer_str.split(","), final_answer.keywords
        return [answer_str], final_answer.keywords

    @staticmethod
    async def update_global_blacklist() -> None:
        """更新全局黑名单"""
        await Chat._select_blacklist()

        keywords_dict: defaultdict[str, int] = defaultdict(int)
        global_blacklist: set[str] = set()

        for keywords_list in Chat._blacklist_answer().values():
            for keywords in keywords_list:
                keywords_dict[keywords] += 1
                if keywords_dict[keywords] == 2:
                    global_blacklist.add(keywords)

        Chat._blacklist_answer()[str(Chat.BLACKLIST_FLAG)] |= global_blacklist

    @staticmethod
    def _blacklist_answer() -> defaultdict[str, set[str]]:
        from . import model as model_mod

        if hasattr(model_mod, "_global_blacklist"):
            return model_mod._global_blacklist  # type: ignore
        else: 
            return defaultdict(set)

    @staticmethod
    async def _select_blacklist() -> None:
        if db.db_operations is None:
            return

        blacklist_dict = Chat._blacklist_answer()
        reserve_dict = defaultdict(set)

        async for group_id in db.db_operations.get_all_blacklist_groups():
            blacklist = await db.db_operations.get_blacklist(group_id)
            if blacklist:
                if blacklist.answers:
                    blacklist_dict[group_id] |= set(blacklist.answers)
                if blacklist.answers_reserve:
                    reserve_dict[group_id] |= set(blacklist.answers_reserve)

        for group_id, answers in reserve_dict.items():
            if group_id in blacklist_dict:
                answers -= blacklist_dict[group_id]
            blacklist_dict[f"reserve_{group_id}"] = answers

    @staticmethod
    async def clearup_context() -> None:
        """清理过期上下文"""
        cur_time = int(time.time())
        expiration = cur_time - 15 * 24 * 3600

        if db.db_operations is None:
            return

        await db.db_operations.clear_expired_contexts(expiration)

    @staticmethod
    async def _find_ban_keywords(context: Context | None, group_id: str) -> set[str]:
        """查找禁用的关键词"""
        blacklist_dict = Chat._blacklist_answer()
        ban_keywords = blacklist_dict[str(Chat.BLACKLIST_FLAG)] | blacklist_dict[group_id]

        if context is not None and context.ban:
            ban_count: defaultdict[str, int] = defaultdict(int)
            for ban in context.ban:
                ban_key = ban.keywords
                if ban.group_id in {group_id, Chat.BLACKLIST_FLAG}:
                    ban_keywords.add(ban_key)
                else:
                    ban_count[ban_key] += 1
                    if ban_count[ban_key] == 2:
                        ban_keywords.add(ban_key)

        return ban_keywords

    @staticmethod
    async def sync():
        """同步数据到数据库"""
        if db.db_operations is None:
            return

        chat_instance = Chat.__new__(Chat)
        chat_instance.state = ChatStateManager(Chat.__new__(Chat).config)
        await chat_instance._sync()
