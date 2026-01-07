import time
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star
from astrbot.api import logger, AstrBotConfig
from .model import ChatData, Chat

class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

    async def initialize(self):
        """异步的插件初始化方法，当实例化该插件类之后会自动调用"""
        test_data: ChatData = ChatData(
            group_id='1234567',
            user_id='1111111',
            raw_message="完了又有新bug",
            plain_text="完了又有新bug",
            time=time.time(),
            bot_id='0',
        )

        test_chat: Chat = Chat(test_data, self.config)

    @filter.command("helloworld")
    async def helloworld(self, event: AstrMessageEvent):
        """这是一个 hello world 指令"""  # 这是 handler 的描述，将会被解析方便用户了解插件内容。建议填写。
        user_name = event.get_sender_name()
        message_str = event.message_str  # 用户发的纯文本消息字符串
        message_chain = (
            event.get_messages()
        )  # 用户所发的消息的消息链 # from astrbot.api.message_components import *
        logger.info(message_chain)
        yield event.plain_result(
            f"Hello, {user_name}, 你发了 {message_str}!"
        )  # 发送一条纯文本消息

    async def terminate(self):
        """异步的插件销毁方法，当插件被卸载/停用时会自动调用"""


'''
import asyncio
import random
import re
import time

from nonebot import get_bot, get_driver, logger, on_message, on_notice
from nonebot.adapters import Bot
from nonebot.adapters.onebot.v11 import GroupMessageEvent, GroupRecallNoticeEvent, Message, MessageSegment, permission
from nonebot.exception import ActionFailed
from nonebot.permission import SUPERUSER, Permission
from nonebot.plugin import PluginMetadata
from nonebot.rule import Rule, keyword, to_me
from nonebot.typing import T_State
from nonebot_plugin_apscheduler import scheduler

from src.common.config import BotConfig
from src.common.utils.array2cqcode import try_convert_to_cqcode
from src.common.utils.media_cache import get_image, insert_image

from .emoji import reaction_msg
from .model import Chat

__plugin_meta__ = PluginMetadata()
message_id_lock = asyncio.Lock()
message_id_dict = {}

driver = get_driver()


@driver.on_startup
async def startup():
    await Chat.update_global_blacklist()


@driver.on_shutdown
async def shutdown():
    await Chat.sync()


async def is_shutup(self_id: int, group_id: int) -> bool:
    info = await get_bot(str(self_id)).call_api(
        "get_group_member_info",
        **{
            "user_id": self_id,
            "group_id": group_id,
        },
    )
    flag: bool = info["shut_up_timestamp"] > time.time()

    logger.info(f"bot [{self_id}] in group [{group_id}] is shutup: {flag}")

    return flag


async def post_proc(message: Message, self_id: int, group_id: int) -> Message:
    new_msg = Message()
    for seg in message:
        if seg.type == "at":
            try:
                info = await get_bot(str(self_id)).call_api(
                    "get_group_member_info",
                    **{
                        "user_id": seg.data["qq"],
                        "group_id": group_id,
                    },
                )
            except ActionFailed:  # 群员不存在
                continue
            nick_name = info["card"] or info["nickname"]
            new_msg += f"@{nick_name}"
        elif seg.type == "image":
            cq_code = str(seg)
            base64_data = await get_image(cq_code)
            if base64_data:
                new_msg += MessageSegment.image(file=base64_data)
            else:
                new_msg += seg
        else:
            new_msg += seg

    if not await Chat.reply_post_proc(str(message), str(new_msg), self_id, group_id):
        logger.warning(
            f"bot [{self_id}] post_proc failed in group [{group_id}]: [{str(message)[:30]}] -> [{str(new_msg)[:30]}]"
        )

    return new_msg


any_msg = on_message(
    priority=15,
    block=False,
    permission=permission.GROUP,
)


@any_msg.handle()
async def _(bot: Bot, event: GroupMessageEvent):
    to_learn = True
    # 多账号登陆，且在同一群中时；避免一条消息被处理多次
    async with message_id_lock:
        message_id = event.message_id
        group_id = event.group_id
        if group_id in message_id_dict:
            if message_id in message_id_dict[group_id]:
                to_learn = False
        else:
            message_id_dict[group_id] = []

        group_message = message_id_dict[group_id]
        group_message.append(message_id)
        if len(group_message) > 100:
            group_message = group_message[:-10]

    chat: Chat = Chat(event)

    answers = None
    config = BotConfig(event.self_id, event.group_id)
    if await config.is_cooldown("repeat"):
        answers = await chat.answer()

    if to_learn:
        for seg in event.message:
            if seg.type == "image":
                await insert_image(seg)

        await chat.learn()

    if not answers:
        return

    await config.refresh_cooldown("repeat")
    delay = random.randint(2, 5)
    async for item in answers:
        msg = await post_proc(item, event.self_id, event.group_id)
        logger.info(f"bot [{event.self_id}] ready to send [{str(msg)[:30]}] to group [{event.group_id}]")

        await asyncio.sleep(delay)
        await config.refresh_cooldown("repeat")
        try:
            await any_msg.send(msg)
        except ActionFailed:
            if not await BotConfig(event.self_id).security():
                continue

            # 自动删除失效消息。若 bot 处于风控期，请勿开启该功能
            shutup = await is_shutup(event.self_id, event.group_id)
            if not shutup:  # 说明这条消息失效了
                logger.info(f"bot [{event.self_id}] ready to ban [{str(item)}] in group [{event.group_id}]")
                await Chat.ban(event.group_id, event.self_id, str(item), "ActionFailed")
                break
        delay = random.randint(1, 3)


async def is_config_admin(event: GroupMessageEvent) -> bool:
    return await BotConfig(event.self_id).is_admin_of_bot(event.user_id)


IsAdmin = permission.GROUP_OWNER | permission.GROUP_ADMIN | SUPERUSER | Permission(is_config_admin)


async def is_reply(event: GroupMessageEvent) -> bool:
    return bool(event.reply)


ban_msg = on_message(
    rule=to_me() & keyword("不可以") & Rule(is_reply),
    priority=5,
    block=True,
    permission=IsAdmin,
)


@ban_msg.handle()
async def _(bot: Bot, event: GroupMessageEvent):
    if "[CQ:reply," not in try_convert_to_cqcode(event.raw_message):
        return False

    raw_message = ""
    for item in event.reply.message:  # type: ignore
        raw_reply = str(item)
        # 去掉图片消息中的 url, subType 等字段
        raw_message += re.sub(r"(\[CQ\:.+)(?:,url=*)(\])", r"\1\2", raw_reply)

    logger.info(f"bot [{event.self_id}] ready to ban [{raw_message}] in group [{event.group_id}]")

    try:
        await bot.delete_msg(message_id=event.reply.message_id)  # type: ignore
    except ActionFailed:
        logger.warning(f"bot [{event.self_id}] failed to delete [{raw_message}] in group [{event.group_id}]")

    if await Chat.ban(event.group_id, event.self_id, raw_message, str(event.user_id)):
        await ban_msg.finish("这对角可能会不小心撞倒些家具，我会尽量小心。")


async def is_admin_recall_self_msg(bot: Bot, event: GroupRecallNoticeEvent):
    # 好像不需要这句
    # if event.notice_type != "group_recall":
    #     return False
    self_id = event.self_id
    user_id = event.user_id
    group_id = event.group_id
    operator_id = event.operator_id
    if self_id != user_id:
        return False
    # 如果是自己撤回的就不用管
    if operator_id == self_id:
        return False
    operator_info = await bot.get_group_member_info(group_id=group_id, user_id=operator_id)
    return operator_info["role"] == "owner" or operator_info["role"] == "admin"


ban_recalled_msg = on_notice(
    rule=Rule(is_admin_recall_self_msg),
    priority=5,
    block=True,
)


@ban_recalled_msg.handle()
async def _(bot: Bot, event: GroupRecallNoticeEvent, state: T_State):
    try:
        msg = await bot.get_msg(message_id=event.message_id)
    except ActionFailed:
        logger.warning(f"bot [{event.self_id}] failed to get msg [{event.message_id}]")
        return

    raw_message = ""
    # 使用get_msg得到的消息不是消息序列，使用正则生成一个迭代对象
    for item in re.compile(r"\[[^\]]*\]|\w+").findall(try_convert_to_cqcode(msg["message"])):
        raw_reply = str(item)
        # 去掉图片消息中的 url, subType 等字段
        raw_message += re.sub(r"(\[CQ\:.+)(?:,url=*)(\])", r"\1\2", raw_reply)

    logger.info(f"bot [{event.self_id}] ready to ban [{raw_message}] in group [{event.group_id}]")

    if await Chat.ban(event.group_id, event.self_id, raw_message, str(f"recall by {event.operator_id}")):
        await ban_recalled_msg.finish("这对角可能会不小心撞倒些家具，我会尽量小心。")


async def message_is_ban(bot: Bot, event: GroupMessageEvent, state: T_State) -> bool:
    return event.get_plaintext().strip() == "不可以发这个"


ban_msg_latest = on_message(
    rule=to_me() & Rule(message_is_ban),
    priority=5,
    block=True,
    permission=IsAdmin,
)


@ban_msg_latest.handle()
async def _(bot: Bot, event: GroupMessageEvent, state: T_State):
    logger.info(f"bot [{event.self_id}] ready to ban latest reply in group [{event.group_id}]")

    try:
        await bot.delete_msg(message_id=event.reply.message_id)  # type: ignore
    except ActionFailed:
        logger.warning(
            f"bot [{event.self_id}] failed to delete latest reply [{event.raw_message}] in group [{event.group_id}]"
        )

    if await Chat.ban(event.group_id, event.self_id, "", str(event.user_id)):
        await ban_msg_latest.finish("这对角可能会不小心撞倒些家具，我会尽量小心。")


@scheduler.scheduled_job("interval", seconds=60)
async def speak_up():
    ret = await Chat.speak()
    if not ret:
        return

    bot_id, group_id, messages, target_id = ret

    for msg in messages:
        logger.info(f"bot [{bot_id}] ready to speak [{msg}] to group [{group_id}]")
        await get_bot(str(bot_id)).call_api(
            "send_group_msg",
            **{
                "message": msg,
                "group_id": group_id,
            },
        )
        if target_id:
            await get_bot(str(bot_id)).call_api(
                "group_poke",
                **{
                    "user_id": target_id,
                    "group_id": group_id,
                },
            )
        await asyncio.sleep(random.randint(2, 5))


@scheduler.scheduled_job("cron", hour=4)
async def update_data():
    await Chat.sync()
    await Chat.clearup_context()
'''