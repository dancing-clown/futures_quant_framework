# -*- coding: utf-8 -*-
"""异步行情采集器实现
负责管理多个具体的行情采集器（如 ZYZmqCollector, CTPCollector），实现并发采集
"""
import asyncio
import threading
from typing import List, Dict
from src.collector.base_collector import BaseFuturesCollector
from src.collector.zy_collector import ZYZmqCollector
from src.collector.ctp_collector import CTPCollector
from src.utils import futures_logger

class AsyncFuturesCollector(BaseFuturesCollector):
    """异步行情采集器（分发器）"""
    
    def __init__(self, market_sources: Dict):
        super().__init__(market_sources)
        self.collectors: List[BaseFuturesCollector] = []
        self._init_sub_collectors()

    def _init_sub_collectors(self):
        """根据配置初始化子采集器"""
        if self.market_sources.get("ZY_ZMQ", {}).get("enable"):
            self.collectors.append(ZYZmqCollector(self.market_sources))
        
        if self.market_sources.get("ctp", {}).get("enable"):
            self.collectors.append(CTPCollector(self.market_sources))

    def init_connections(self) -> bool:
        """初始化所有子采集器的连接"""
        all_success = True
        for collector in self.collectors:
            if not collector.init_connections():
                futures_logger.error(f"采集器 {collector.__class__.__name__} 连接失败")
                all_success = False
        return all_success

    def subscribe_market(self) -> bool:
        """订阅所有子采集器的行情"""
        all_success = True
        for collector in self.collectors:
            if not collector.subscribe_market():
                all_success = False
        return all_success

    def collect_data(self) -> List[Dict]:
        """汇总所有子采集器的数据"""
        all_data = []
        for collector in self.collectors:
            all_data.extend(collector.collect_data())
        return all_data

    def close_connections(self) -> None:
        """关闭所有子采集器的连接"""
        for collector in self.collectors:
            collector.close_connections()

    async def run_forever(self, on_data_callback):
        """
        启动异步任务运行所有采集器
        
        CTP 采集器使用回调机制，数据通过回调放入队列，这里定期从队列中取数据
        ZY ZMQ 采集器使用异步接收，需要启动异步任务
        """
        tasks = []
        
        # ZY ZMQ 采集器使用异步接收
        for collector in self.collectors:
            if isinstance(collector, ZYZmqCollector):
                tasks.append(collector.api.start_receiving(collector.on_data_received))
        
        # CTP 采集器需要在单独线程中调用 Join() 保持连接和处理回调
        for collector in self.collectors:
            if isinstance(collector, CTPCollector) and collector.api and collector.api.api:
                def run_ctp_join():
                    try:
                        # Join() 会阻塞，直到连接断开
                        collector.api.join()
                    except Exception as e:
                        futures_logger.error(f"CTP Join 异常: {e}", exc_info=True)
                
                join_thread = threading.Thread(target=run_ctp_join, daemon=True)
                join_thread.start()
                futures_logger.info("已启动 CTP Join 线程")
        
        # 数据分发循环（从所有采集器的队列中取数据）
        async def dispatch_loop():
            while True:
                data = self.collect_data()
                if data:
                    await on_data_callback(data)
                # CTP 回调是同步的，需要定期检查队列
                await asyncio.sleep(0.1)  # 100ms 检查一次

        tasks.append(dispatch_loop())
        await asyncio.gather(*tasks)
