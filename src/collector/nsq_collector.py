# -*- coding: utf-8 -*-
"""NSQ-DCE 行情采集器实现（Linux only）

结构与 CTP/正瀛采集器保持一致：
- init_connections()：初始化 API 并注册回调，将原始数据投递到队列
- collect_data()：从队列取数据，通过 DataParser 标准化后返回
"""

import queue
from typing import List, Dict

from src.collector.base_collector import BaseFuturesCollector
from src.api.nsq_api import NsqMarketApi
from src.processor.data_parser import DataParser
from src.utils import futures_logger
from src.utils.exceptions import DataParseError


class NSQCollector(BaseFuturesCollector):
    """NSQ-DCE 行情采集器（仅 Linux）"""

    def __init__(self, market_sources: Dict):
        super().__init__(market_sources)
        nsq_cfg = market_sources.get("nsq_dce_net_api", {})
        self.api = NsqMarketApi(
            config_path=nsq_cfg.get("config_path"),
            username=nsq_cfg.get("username"),
            password=nsq_cfg.get("password"),
            sdk_config_path=nsq_cfg.get("sdk_config_path"),
            log_path=nsq_cfg.get("log_path"),
            markets=nsq_cfg.get("markets", "dce"),
            pybind_path=nsq_cfg.get("pybind_path"),
        )
        self.data_queue: queue.Queue = queue.Queue()

    def init_connections(self) -> bool:
        """初始化 NSQ 连接（Linux only）"""
        return self.api.connect(self.on_data_received)

    def subscribe_market(self) -> bool:
        """订阅行情（NSQ 订阅由其 SDK/配置驱动，这里保持接口一致）"""
        futures_logger.info("NSQ 订阅完成（由 nsq 配置驱动）")
        return True

    def collect_data(self) -> List[Dict]:
        """采集数据：队列 -> DataParser -> 标准化数据"""
        data_list: List[Dict] = []
        while not self.data_queue.empty():
            try:
                raw_msg = self.data_queue.get_nowait()
                std_data = DataParser.parse_raw_data(raw_msg)
                if std_data:
                    data_list.append(std_data)
            except queue.Empty:
                break
            except DataParseError as e:
                futures_logger.warning(f"NSQ 数据解析失败，跳过本条: {e}")
            except Exception as e:
                futures_logger.error(f"NSQ 数据解析异常: {e}", exc_info=True)
        return data_list

    def close_connections(self) -> None:
        self.api.close()

    def on_data_received(self, raw_msg: Dict):
        """数据接收回调：入队"""
        self.data_queue.put(raw_msg)

