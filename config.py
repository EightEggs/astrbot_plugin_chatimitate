"""
AstrBot ChatImitate Plugin - Configuration Module
"""

from astrbot.api import AstrBotConfig


class ChatImitateConfig:
    """聊天模仿插件配置管理类"""

    def __init__(self, plugin_config: AstrBotConfig):
        # 调试配置
        self.debug_message_format: bool = bool(plugin_config.get(
            "debug_message_format", False
        ))

        # 学习机制配置 (learning 分组)
        learning_config = plugin_config.get("learning", {})
        self.answer_threshold: int = int(learning_config.get("answer_threshold", 3))
        self.answer_threshold_weights: list[int] = self._ensure_list_int(
            learning_config.get("answer_threshold_weights", [7, 23, 70])
        )
        self.topics_size: int = int(learning_config.get("topics_size", 16))
        self.topics_importance: int = int(learning_config.get("topics_importance", 10000))
        self.cross_group_threshold: int = int(learning_config.get("cross_group_threshold", 2))
        self.duplicate_reply: int = int(learning_config.get("duplicate_reply", 10))
        self.split_probability: float = float(learning_config.get("split_probability", 0.5))

        # 数据存储配置 (storage 分组)
        storage_config = plugin_config.get("storage", {})
        self.save_time_threshold: int = int(storage_config.get(
            "save_time_threshold", 300
        ))
        self.save_count_threshold: int = int(storage_config.get(
            "save_count_threshold", 50
        ))
        self.save_reserved_size: int = int(storage_config.get("save_reserved_size", 100))
        self.cleanup_expired_days: int = int(storage_config.get(
            "cleanup_expired_days", 15
        ))

        # 多媒体支持配置 (media 分组)
        media_config = plugin_config.get("media", {})
        self.enable_image_learning: bool = bool(media_config.get(
            "enable_image_learning", True
        ))
        self.image_similarity_threshold: float = float(media_config.get(
            "image_similarity_threshold", 0.8
        ))

    def _ensure_list_int(self, value) -> list[int]:
        """确保值是整数列表"""
        if not isinstance(value, list):
            return [7, 23, 70]
        return [int(x) for x in value]

    @property
    def answer_threshold_choice_list(self) -> list[int]:
        """根据权重列表生成阈值选择范围

        例如: answer_threshold=3, weights=[7,23,70] (3个权重)
        返回: [1, 2, 3] (从 3-3+1=1 到 3)
        """
        return list(
            range(
                self.answer_threshold - len(self.answer_threshold_weights) + 1,
                self.answer_threshold + 1,
            )
        )

    def validate(self) -> bool:
        """验证配置有效性"""
        # 验证权重列表长度与阈值范围匹配
        if len(self.answer_threshold_weights) != len(self.answer_threshold_choice_list):
            return False

        # 验证概率值在合理范围内
        if not 0 <= self.split_probability <= 1:
            return False

        if not 0 <= self.image_similarity_threshold <= 1:
            return False

        # 验证正数配置
        if any(
            v <= 0
            for v in [
                self.answer_threshold,
                self.topics_size,
                self.topics_importance,
                self.cross_group_threshold,
                self.duplicate_reply,
                self.save_time_threshold,
                self.save_count_threshold,
                self.save_reserved_size,
                self.cleanup_expired_days,
            ]
        ):
            return False

        return True
