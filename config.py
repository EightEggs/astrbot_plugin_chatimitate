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

    def __init__(self, plugin_config: AstrBotConfig):
        # Learning mechanism config
        learning_config = plugin_config.get("learning", {})
        self.answer_threshold: int = int(learning_config.get("answer_threshold", 3))
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

    def validate(self) -> bool:
        """Validate configuration values."""
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
