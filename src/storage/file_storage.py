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
        if not data_list:
            return
            
        for data in data_list:
            try:
                # 确保 datetime 是 datetime 对象
                if isinstance(data.get('datetime'), str):
                    from datetime import datetime
                    data['datetime'] = datetime.fromisoformat(data['datetime'])
                
                date_str = data['datetime'].strftime("%Y%m%d")
                symbol = data.get('symbol', 'unknown')
                file_path = os.path.join(self.base_path, f"{symbol}_{date_str}.csv")
                
                file_exists = os.path.exists(file_path)
                with open(file_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=data.keys())
                    if not file_exists:
                        writer.writeheader()
                    save_data = data.copy()
                    save_data['datetime'] = data['datetime'].isoformat()
                    writer.writerow(save_data)
                
                futures_logger.info(f"已保存数据到: {file_path} - {symbol}, 价格: {data.get('last_price', 0)}")
            except Exception as e:
                futures_logger.error(f"保存数据失败: {e}", exc_info=True)
