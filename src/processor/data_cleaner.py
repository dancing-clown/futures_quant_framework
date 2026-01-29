# -*- coding: utf-8 -*-
"""数据清洗模块。

将各源解析后的行情做去重与必选字段校验，保证写入存储的数据一致可用。
"""
from typing import List, Dict, Any

from src.utils import futures_logger, DataCleanError


class DataCleaner:
    """行情数据清洗器：去重、必选字段校验。"""

    def __init__(self, clean_config: Dict[str, Any] = None):
        """初始化清洗器。

        Args:
            clean_config: 清洗配置，含 max_seen_size（去重缓存上限，超过则清空）。
        """
        self.seen_data: Dict = {}
        _config = clean_config or {}
        self._max_seen_size = int(_config.get("max_seen_size", 10000))

    def clean(self, data_list: List[Dict]) -> List[Dict]:
        """对解析后的行情列表做去重与校验。

        Args:
            data_list: 标准化行情字典列表（需含 symbol、datetime、last_price 等）。

        Returns:
            通过校验且未重复的行情列表。

        Raises:
            DataCleanError: 单条数据缺少必选字段或格式异常时抛出。
        """
        cleaned_list = []
        for data in data_list:
            try:
                key = (data["symbol"], data["datetime"])
            except KeyError as e:
                raise DataCleanError(f"清洗数据缺少必选字段: {e}") from e
            if key in self.seen_data:
                continue
            if not data.get("last_price"):
                continue
            self.seen_data[key] = True
            cleaned_list.append(data)
        if len(self.seen_data) > self._max_seen_size:
            self.seen_data.clear()
            futures_logger.debug("去重缓存已清空（超过 max_seen_size）")
        return cleaned_list
