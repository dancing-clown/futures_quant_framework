# -*- coding: utf-8 -*-
"""数据清洗模块"""
from typing import List, Dict
from src.utils import futures_logger

class DataCleaner:
    """行情数据清洗器"""
    
    def __init__(self):
        self.seen_data = {}

    def clean(self, data_list: List[Dict]) -> List[Dict]:
        """基础清洗逻辑：去重、校验"""
        cleaned_list = []
        for data in data_list:
            key = (data['symbol'], data['datetime'])
            if key in self.seen_data:
                continue
            
            if not data.get('last_price'):
                continue
                
            self.seen_data[key] = True
            cleaned_list.append(data)
            
        if len(self.seen_data) > 10000:
            self.seen_data.clear()
            
        return cleaned_list
