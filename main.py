import asyncio
import random
import time

from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.api import logger, AstrBotConfig
from .emoji import get_random_emoji
from .model import Chat

class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._stop_event = asyncio.Event()
        self._bg_task: asyncio.Task | None = None

    async def initialize(self):
        """异步的插件初始化方法，当实例化该插件类之后会自动调用"""
        # 初始化数据库，传递插件名称
        from .db import init_db
        await init_db(self.name)

        # 初始化一次全局黑名单
        try:
            await Chat.update_global_blacklist()
        except Exception:
            logger.warning("chatimitate: update_global_blacklist failed", exc_info=True)

        # 后台定期持久化/清理
        self._bg_task = asyncio.create_task(self._periodic_maintenance())

    async def terminate(self):
        """异步的插件销毁方法，当插件被卸载/停用时会自动调用"""



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


    async def _periodic_maintenance(self) -> None:
        """Periodic sync/cleanup loop.

        - Sync: every hour
        - Cleanup: once per day
        """

        last_cleanup_day: int | None = None
        while not self._stop_event.is_set():
            try:
                await Chat.sync()
            except Exception:
                logger.warning("chatimitate: periodic sync failed", exc_info=True)

            today = int(time.strftime("%Y%m%d"))
            if last_cleanup_day != today:
                try:
                    await Chat.clearup_context()
                    last_cleanup_day = today
                except Exception:
                    logger.warning("chatimitate: clearup_context failed", exc_info=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=3600)
            except asyncio.TimeoutError:
                continue


    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """学习群消息并尝试回复。"""

        # 不处理自己发的消息，避免自我学习/循环
        if event.get_sender_id() == event.get_self_id():
            return

        chat = Chat(event, self.config)

        # 先学习
        try:
            await chat.learn()
        except Exception:
            logger.warning("chatimitate: learn failed", exc_info=True)

        # 可选：概率表情回应（平台不支持时通常会降级为发送 emoji 文本）
        try:
            if getattr(self.config, "enable_reaction", True) and getattr(
                self.config, "enable_probability_reaction", True
            ):
                prob = float(getattr(self.config, "reaction_probability", 0.1))
                if prob > 0 and random.random() < prob and hasattr(event, "react"):
                    await event.react(get_random_emoji())
        except Exception:
            # 反应失败不影响主流程
            logger.debug("chatimitate: react failed", exc_info=True)

        # 再尝试回复
        try:
            answers = await chat.answer()
        except Exception:
            logger.warning("chatimitate: answer failed", exc_info=True)
            return

        if not answers:
            return

        async for msg in answers:
            # AstrBotMessage -> str 通常可直接发送（NapCat/OneBot CQ 码兼容）
            yield event.plain_result(str(msg))
            await asyncio.sleep(random.randint(1, 3))
