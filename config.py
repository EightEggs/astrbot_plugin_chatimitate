"""
AstrBot ChatImitate Plugin - Configuration Module
配置管理模块 - 从 _conf_schema.json 解析配置
"""

from astrbot.api import AstrBotConfig


class ChatImitateConfig:
    """聊天模仿插件配置管理类

    严格遵循 _conf_schema.json 的结构，提供类型安全的配置访问
    """

    def __init__(self, plugin_config: AstrBotConfig):
        # 调试配置
        self.debug_message_format: bool = plugin_config.get(
            "debug_message_format", False
        )

        # 学习机制配置 (learning 分组)
        learning_config = plugin_config.get("learning", {})
        self.answer_threshold: int = learning_config.get("answer_threshold", 3)
        self.answer_threshold_weights: list[int] = learning_config.get(
            "answer_threshold_weights", [7, 23, 70]
        )
        self.topics_size: int = learning_config.get("topics_size", 16)
        self.topics_importance: int = learning_config.get("topics_importance", 10000)
        self.cross_group_threshold: int = learning_config.get(
            "cross_group_threshold", 2
        )
        self.duplicate_reply: int = learning_config.get("duplicate_reply", 10)
        self.split_probability: float = learning_config.get("split_probability", 0.5)

        # 数据存储配置 (storage 分组)
        storage_config = plugin_config.get("storage", {})
        self.save_time_threshold: int = storage_config.get(
            "save_time_threshold", 300
        )
        self.save_count_threshold: int = storage_config.get(
            "save_count_threshold", 50
        )
        self.save_reserved_size: int = storage_config.get("save_reserved_size", 100)
        self.cleanup_expired_days: int = storage_config.get(
            "cleanup_expired_days", 15
        )

        # 多媒体支持配置 (media 分组)
        media_config = plugin_config.get("media", {})
        self.enable_image_learning: bool = media_config.get(
            "enable_image_learning", True
        )

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
