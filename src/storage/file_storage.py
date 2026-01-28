# -*- coding: utf-8 -*-
"""文件存储模块"""
import os
import csv
from typing import List, Dict
from src.utils import futures_logger

class FileStorage:
    """本地文件存储实现"""
    
    def __init__(self, base_path: str = "data/market_data"):
        self.base_path = base_path
        if not os.path.exists(base_path):
            os.makedirs(base_path)

    def save(self, data_list: List[Dict]):
        """按天按合约保存为 CSV"""
        for data in data_list:
            date_str = data['datetime'].strftime("%Y%m%d")
            symbol = data['symbol']
            file_path = os.path.join(self.base_path, f"{symbol}_{date_str}.csv")
            
            file_exists = os.path.exists(file_path)
            with open(file_path, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                if not file_exists:
                    writer.writeheader()
                save_data = data.copy()
                save_data['datetime'] = data['datetime'].isoformat()
                writer.writerow(save_data)
