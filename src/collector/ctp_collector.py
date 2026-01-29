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
        # 登录信息为可选（如果未提供则使用匿名登录）
        broker_id = ctp_config.get("broker_id", "")
        investor_id = ctp_config.get("investor_id", "")
        password = ctp_config.get("password", "")
        self.api = CtpMarketApi(
            front_address=ctp_config.get("host", ""),
            flow_path=ctp_config.get("flow_path", "./flow/"),
            pybind_path=ctp_config.get("pybind_path"),
            subscribe_symbols=ctp_config.get("subscribe_codes", []),
            broker_id=broker_id if broker_id else None,
            investor_id=investor_id if investor_id else None,
            password=password if password else None
        )
        self.subscribe_codes = ctp_config.get("subscribe_codes", [])
        self.data_queue = queue.Queue()

    def init_connections(self) -> bool:
        """初始化 CTP 连接
        注意：connect 方法会自动在连接成功后调用 login（通过 OnFrontConnected 回调）
        """
        return self.api.connect(self.on_data_received, auto_subscribe=True)

    def subscribe_market(self) -> bool:
        """订阅行情
        注意：如果 auto_subscribe=True，订阅会在登录成功后自动执行
        这里主要检查订阅状态，如果还未订阅则手动订阅
        """
        # 如果已经登录，可以手动订阅（如果自动订阅失败）
        if self.api.is_logged_in:
            return self.api.subscribe(self.subscribe_codes)
        # 否则等待自动订阅（通过 OnRspUserLogin 回调）
        return True

    def collect_data(self) -> List[Dict]:
        """采集数据"""
        data_list = []
        queue_size = self.data_queue.qsize()
        
        if queue_size > 0:
            futures_logger.info(f"从队列中采集数据，队列大小: {queue_size}")
        else:
            # 即使队列为空，也偶尔打印一下状态（避免日志过多）
            import random
            if random.random() < 0.01:  # 1% 的概率打印
                futures_logger.debug(f"CTP 采集器检查队列，当前队列大小: {queue_size}")
        
        processed_count = 0
        while not self.data_queue.empty():
            try:
                raw_msg = self.data_queue.get_nowait()
                processed_count += 1
                futures_logger.debug(f"从队列取出消息 {processed_count}，类型: {raw_msg.get('type', 'unknown')}")
                std_data = DataParser.parse_raw_data(raw_msg)
                if std_data:
                    futures_logger.info(f"解析成功: {std_data.get('symbol', 'unknown')}, 价格: {std_data.get('last_price', 0)}")
                    data_list.append(std_data)
                else:
                    futures_logger.warning(f"数据解析返回 None，原始消息类型: {raw_msg.get('type', 'unknown')}")
            except queue.Empty:
                break
            except Exception as e:
                futures_logger.error(f"数据解析异常: {e}", exc_info=True)
        
        if processed_count > 0:
            futures_logger.info(f"本次处理了 {processed_count} 条消息，成功解析 {len(data_list)} 条")
        return data_list

    def close_connections(self) -> None:
        """释放资源"""
        self.api.close()

    def on_data_received(self, raw_msg: Dict):
        """数据接收回调"""
        try:
            futures_logger.debug(f"CTP 数据接收回调: {raw_msg.get('type', 'unknown')}")
            self.data_queue.put(raw_msg)
        except Exception as e:
            futures_logger.error(f"数据接收回调异常: {e}", exc_info=True)
