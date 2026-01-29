# -*- coding: utf-8 -*-
"""文件存储模块。

按天按合约将标准化行情写入本地 CSV，路径与格式由配置决定。
"""
import os
import csv
from typing import List, Dict

from src.utils import futures_logger
from src.utils.exceptions import StorageError


class FileStorage:
    """本地文件存储实现：按天按合约追加 CSV。"""

    def __init__(self, base_path: str = "data/market_data"):
        """初始化存储目录。

        Args:
            base_path: 存储根目录（相对项目根或绝对路径）。
        """
        self.base_path = base_path
        if not os.path.exists(base_path):
            os.makedirs(base_path)

    def save(self, data_list: List[Dict]) -> None:
        """将标准化行情按天按合约追加写入 CSV。

        Args:
            data_list: 标准化行情字典列表，需含 symbol、datetime 等字段。

        Raises:
            StorageError: 单条写入失败（如路径无权限、磁盘满）时抛出。
        """
        if not data_list:
            return
        for data in data_list:
            try:
                if isinstance(data.get("datetime"), str):
                    from datetime import datetime
                    data["datetime"] = datetime.fromisoformat(data["datetime"])
                date_str = data["datetime"].strftime("%Y%m%d")
                symbol = data.get("symbol", "unknown")
                file_path = os.path.join(self.base_path, f"{symbol}_{date_str}.csv")
                file_exists = os.path.exists(file_path)
                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=data.keys())
                    if not file_exists:
                        writer.writeheader()
                    save_data = data.copy()
                    save_data["datetime"] = data["datetime"].isoformat()
                    writer.writerow(save_data)
                futures_logger.debug(
                    f"已保存数据到: {file_path} - {symbol}, 价格: {data.get('last_price', 0)}"
                )
            except OSError as e:
                futures_logger.error(f"保存数据失败: {e}", exc_info=True)
                raise StorageError(f"文件写入失败: {e}") from e
            except Exception as e:
                futures_logger.error(f"保存数据失败: {e}", exc_info=True)
                raise StorageError(f"保存数据失败: {e}") from e
