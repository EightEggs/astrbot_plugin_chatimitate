import random
import re
from dataclasses import dataclass
from typing import Any

EMOJI_IDS: tuple[int, ...] = (
    4,
    5,
    8,
    9,
    10,
    12,
    14,
    16,
    21,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    32,
    33,
    34,
    38,
    39,
    41,
    42,
    43,
    49,
    53,
    60,
    63,
    66,
    74,
    75,
    76,
    78,
    79,
    85,
    89,
    96,
    97,
    98,
    99,
    100,
    101,
    102,
    103,
    104,
    106,
    109,
    111,
    116,
    118,
    120,
    122,
    123,
    124,
    125,
    129,
    144,
    147,
    171,
    173,
    174,
    175,
    176,
    179,
    180,
    181,
    182,
    183,
    201,
    203,
    212,
    214,
    219,
    222,
    227,
    232,
    240,
    243,
    246,
    262,
    264,
    265,
    266,
    267,
    268,
    269,
    270,
    271,
    272,
    273,
    278,
    281,
    282,
    284,
    285,
    287,
    289,
    290,
    293,
    294,
    297,
    298,
    299,
    305,
    306,
    307,
    314,
    315,
    318,
    319,
    320,
    322,
    324,
    326,
    9728,
    9749,
    9786,
    10024,
    10060,
    10068,
    127801,
    127817,
    127822,
    127827,
    127836,
    127838,
    127847,
    127866,
    127867,
    127881,
    128027,
    128046,
    128051,
    128053,
    128074,
    128076,
    128077,
    128079,
    128089,
    128102,
    128104,
    128147,
    128157,
    128164,
    128166,
    128168,
    128170,
    128235,
    128293,
    128513,
    128514,
    128516,
    128522,
    128524,
    128527,
    128530,
    128531,
    128532,
    128536,
    128538,
    128540,
    128541,
    128557,
    128560,
    128563,
)  # 官方文档就这么多

# QQ/OneBot `face` id 通常是较小的整数。列表里混有较大的 unicode 值时，
# 在 NapCat/QQ 上作为 reaction 的 face-id 可能不生效。
QQ_FACE_IDS: tuple[int, ...] = tuple(x for x in EMOJI_IDS if x <= 400)


# 更通用的「reaction emoji」集合（跨平台更可能可用）
UNICODE_REACTIONS: tuple[str, ...] = (
    "👍",
    "❤️",
    "😂",
    "🤣",
    "🥹",
    "🤔",
    "😅",
    "😭",
    "😡",
    "🎉",
)


def get_random_face_id() -> int:
    """Return a random QQ/OneBot `face` id."""

    # 优先选 QQ 常见 face id（更稳）
    if QQ_FACE_IDS:
        return random.choice(QQ_FACE_IDS)
    return random.choice(EMOJI_IDS)


def format_cq_face(face_id: int) -> str:
    """Format a CQ `face` segment."""

    return f"[CQ:face,id={int(face_id)}]"


_CQ_FACE_RE = re.compile(r"\[CQ:face,(?:[^\]]*,)?id=(\d+)(?:,[^\]]*)?\]")


def extract_cq_face_ids(raw_message: str) -> list[int]:
    """Extract CQ face ids from a raw message string."""

    return [int(x) for x in _CQ_FACE_RE.findall(raw_message or "")]


def has_cq_face(raw_message: str) -> bool:
    """Check whether a raw message contains any CQ face segment."""

    return bool(_CQ_FACE_RE.search(raw_message or ""))


def get_random_unicode_emoji() -> str:
    """Return a random unicode emoji for reaction."""

    return random.choice(UNICODE_REACTIONS)


def _unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it in seen:
            continue
        seen.add(it)
        out.append(it)
    return out


def should_trigger_reaction(config: Any) -> bool:
    """Decide whether to trigger a reaction based on plugin config.

    Expected config keys (from `_conf_schema.json`):
    - enable_reaction (bool)
    - enable_probability_reaction (bool)
    - reaction_probability (float)
    """

    if not getattr(config, "enable_reaction", True):
        return False
    if not getattr(config, "enable_probability_reaction", True):
        return False
    try:
        prob = float(getattr(config, "reaction_probability", 0.1))
    except Exception:
        prob = 0.0
    if prob <= 0:
        return False
    return random.random() < prob


def choose_reaction_emoji(
    raw_message: str | None = None,
    *,
    prefer_same_face: bool = True,
) -> str:
    """Choose an emoji for reacting.

    - If `prefer_same_face` and the message contains CQ `face`, reuse one of them.
    - Otherwise fall back to a unicode reaction emoji.
    """

    if prefer_same_face and raw_message:
        face_ids = extract_cq_face_ids(raw_message)
        if face_ids:
            return format_cq_face(random.choice(face_ids))

    return get_random_unicode_emoji()


def get_reaction_candidates(raw_message: str | None, config: Any) -> list[str]:
    """Return reaction candidates in priority order.

    QQ/NapCat/OneBot 场景下，`event.react()` 很多实现更偏向“emoji id”(数字字符串)。
    因此默认优先返回 face-id（同款优先），最后再回退到 unicode emoji。

    Config keys:
    - reply_with_same_emoji (bool): 有同款 face 时优先用同款
    - reaction_prefer_unicode (bool): 为 true 时把 unicode 放在更前
    - reaction_enable_unicode_fallback (bool): 是否允许 unicode 回退
    """

    prefer_same = bool(getattr(config, "reply_with_same_emoji", True))
    prefer_unicode = bool(getattr(config, "reaction_prefer_unicode", False))
    enable_unicode_fallback = bool(getattr(config, "reaction_enable_unicode_fallback", True))

    candidates: list[str] = []

    # 1) 同款优先：从消息里提取 CQ face id，优先用 id 字符串
    if prefer_same and raw_message:
        face_ids = extract_cq_face_ids(raw_message)
        if face_ids:
            candidates.append(str(random.choice(face_ids)))

    # 2) QQ 兼容：随机一个 face id（数字字符串）
    candidates.append(str(get_random_face_id()))

    # 3) Unicode 回退（有些端支持更好）
    if enable_unicode_fallback:
        candidates.append(get_random_unicode_emoji())

    if prefer_unicode and enable_unicode_fallback:
        # unicode 提前，但仍保留 face-id 作为候选
        candidates = [candidates[-1]] + candidates[:-1]

    return _unique_preserve_order(candidates)


@dataclass
class ReactionCache:
    """In-memory cache to avoid repeated reacting to same message id."""

    ttl_seconds: int = 3600
    _seen: dict[str, float] = None  # type: ignore

    def __post_init__(self) -> None:
        if self._seen is None:
            self._seen = {}

    def seen(self, key: str, now: float) -> bool:
        self.cleanup(now)
        return key in self._seen

    def mark(self, key: str, now: float) -> None:
        self.cleanup(now)
        self._seen[key] = now

    def cleanup(self, now: float) -> None:
        if not self._seen:
            return
        expired_before = now - self.ttl_seconds
        for k in list(self._seen.keys()):
            if self._seen[k] < expired_before:
                del self._seen[k]
