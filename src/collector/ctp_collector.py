# -*- coding: utf-8 -*-
"""CTP 行情采集器实现
继承 BaseFuturesCollector，实现具体的采集逻辑
"""
import queue
from typing import List, Dict
from src.collector.base_collector import BaseFuturesCollector
from src.api.ctp_api import CtpMarketApi
from src.processor.data_parser import DataParser
from src.utils import futures_logger

class CTPCollector(BaseFuturesCollector):
    """CTP 行情采集器"""
    
    def __init__(self, market_sources: Dict):
        super().__init__(market_sources)
        ctp_config = market_sources.get("ctp", {})
        self.api = CtpMarketApi(
            broker_id=ctp_config.get("broker_id", ""),
            investor_id=ctp_config.get("investor_id", ""),
            password=ctp_config.get("password", ""),
            front_address=ctp_config.get("host", ""),
            pybind_path=ctp_config.get("pybind_path")
        )
        self.subscribe_codes = ctp_config.get("subscribe_codes", [])
        self.data_queue = queue.Queue()

    def init_connections(self) -> bool:
        """初始化 CTP 连接"""
        if self.api.connect(self.on_data_received):
            return self.api.login()
        return False

    def subscribe_market(self) -> bool:
        """订阅行情"""
        return self.api.subscribe(self.subscribe_codes)

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
