# -*- coding: utf-8 -*-
"""正瀛 ZMQ 采集器实现
继承 BaseFuturesCollector，实现具体的采集逻辑
"""
import queue
from typing import List, Dict
from src.collector.base_collector import BaseFuturesCollector
from src.api.zy_zmq_api import ZYZmqApi
from src.processor.data_parser import DataParser
from src.utils import futures_logger

class ZYZmqCollector(BaseFuturesCollector):
    """正瀛 ZMQ 行情采集器"""
    
    def __init__(self, market_sources: Dict):
        super().__init__(market_sources)
        zy_config = market_sources.get("ZY_ZMQ", {})
        self.api = ZYZmqApi(
            dce_address=zy_config.get("dce_address", ""),
            czce_address=zy_config.get("czce_address", "")
        )
        self.data_queue = queue.Queue()

    def init_connections(self) -> bool:
        """初始化 ZMQ 连接"""
        return self.api.connect()

    def subscribe_market(self) -> bool:
        """订阅行情"""
        futures_logger.info("ZY ZMQ 订阅完成 (全量订阅)")
        return True

    def collect_data(self) -> List[Dict]:
        """采集数据"""
        data_list = []
        while not self.data_queue.empty():
            try:
                raw_msg = self.data_queue.get_nowait()
                std_data = DataParser.parse_raw_data(raw_msg)
                if std_data:
                    data_list.append(std_data)
            except queue.Empty:
                break
        return data_list

    def close_connections(self) -> None:
        """释放资源"""
        self.api.close()

    def on_data_received(self, raw_msg: Dict):
        """数据接收回调"""
        self.data_queue.put(raw_msg)
