# -*- coding: utf-8 -*-
"""GFEX ExaNIC 行情采集器（Linux only）

与 CTP/正瀛/NSQ 采集器结构一致：使用 GfexExanicApi（exanic_pybind 调用 ExaNIC C SDK）
接收 L2 帧，入队后由 collect_data 经 DataParser 标准化输出。
"""

import queue
from typing import List, Dict

from src.collector.base_collector import BaseFuturesCollector
from src.api.gfex_exanic_api import GfexExanicApi
from src.processor.data_parser import DataParser
from src.utils import futures_logger
from src.utils.exceptions import DataParseError


class GfexCollector(BaseFuturesCollector):
    """GFEX ExaNIC 行情采集器（仅 Linux，依赖 exanic_pybind）"""

    def __init__(self, market_sources: Dict):
        super().__init__(market_sources)
        cfg = market_sources.get("hs_future_gfex_api", {}) or market_sources.get("gfex", {})
        self.api = GfexExanicApi(
            nic_name=cfg.get("nic_name", "exanic0"),
            port_number=int(cfg.get("port_number", 1)),
            buffer_number=int(cfg.get("buffer_number", 0)),
            pybind_path=cfg.get("pybind_path"),
            frame_buffer_size=int(cfg.get("frame_buffer_size", 2048)),
        )
        self.data_queue: queue.Queue = queue.Queue()

    def init_connections(self) -> bool:
        """初始化 ExaNIC 连接并启动接收线程"""
        return self.api.connect(self.on_data_received)

    def subscribe_market(self) -> bool:
        """GFEX 由网卡直连，无单独订阅"""
        futures_logger.info("GFEX ExaNIC 订阅完成（网卡直连）")
        return True

    def collect_data(self) -> List[Dict]:
        """从队列取原始消息，经 DataParser 标准化后返回"""
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
                futures_logger.warning(f"GFEX 数据解析失败，跳过本条: {e}")
            except Exception as e:
                futures_logger.error(f"GFEX 数据解析异常: {e}", exc_info=True)
        return data_list

    def close_connections(self) -> None:
        self.api.close()

    def on_data_received(self, raw_msg: Dict) -> None:
        """API 接收线程回调：入队"""
        self.data_queue.put(raw_msg)
