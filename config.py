"""
AstrBot ChatImitate Plugin - Configuration Module
"""

from astrbot.api import AstrBotConfig


def _parse_bool(value) -> bool:
    """Parse boolean value, handling string representations like 'false'."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() not in ("false", "0", "no", "off", "")
    return bool(value)


class ChatImitateConfig:
    """Configuration manager for ChatImitate plugin."""

    DEFAULT_THRESHOLD_WEIGHTS = [7, 23, 70]

    def __init__(self, plugin_config: AstrBotConfig):
        # Learning mechanism config
        learning_config = plugin_config.get("learning", {})
        self.answer_threshold: int = int(learning_config.get("answer_threshold", 3))
        self.answer_threshold_weights: list[int] = self._ensure_list_int(
            learning_config.get("answer_threshold_weights", self.DEFAULT_THRESHOLD_WEIGHTS)
        )
        self.topics_size: int = int(learning_config.get("topics_size", 16))
        self.topics_importance: int = int(learning_config.get("topics_importance", 10000))
        self.cross_group_threshold: int = int(learning_config.get("cross_group_threshold", 2))
        self.duplicate_reply: int = int(learning_config.get("duplicate_reply", 10))
        self.split_probability: float = float(learning_config.get("split_probability", 0.5))

        # Storage config
        storage_config = plugin_config.get("storage", {})
        self.save_reserved_size: int = int(storage_config.get("save_reserved_size", 100))
        self.cleanup_expired_days: int = int(storage_config.get("cleanup_expired_days", 32))

        # Media config
        media_config = plugin_config.get("media", {})
        self.enable_image_learning: bool = _parse_bool(
            media_config.get("enable_image_learning", True)
        )

    def _ensure_list_int(self, value) -> list[int]:
        """Ensure value is a list of ints with fallback."""
        if not isinstance(value, list):
            return list(self.DEFAULT_THRESHOLD_WEIGHTS)

        result = []
        for item in value:
            try:
                result.append(int(item))
            except (ValueError, TypeError):
                continue

        return result if result else list(self.DEFAULT_THRESHOLD_WEIGHTS)

    @property
    def answer_threshold_choice_list(self) -> list[int]:
        """Generate threshold choice range based on weights list length.

        Example: answer_threshold=3, weights=[7,23,70] (3 weights)
        Returns: [1, 2, 3] (from 3-3+1=1 to 3)
        """
        start = self.answer_threshold - len(self.answer_threshold_weights) + 1
        start = max(1, start)
        return list(range(start, self.answer_threshold + 1))

    def validate(self) -> bool:
        """Validate configuration values."""
        if len(self.answer_threshold_weights) != len(self.answer_threshold_choice_list):
            return False

        if not 0 <= self.split_probability <= 1:
            return False

        if any(
            v <= 0
            for v in [
                self.answer_threshold,
                self.topics_size,
                self.topics_importance,
                self.cross_group_threshold,
                self.duplicate_reply,
                self.save_reserved_size,
                self.cleanup_expired_days,
            ]
        ):
            return False

        return True
