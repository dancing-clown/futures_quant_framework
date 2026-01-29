# -*- coding: utf-8 -*-
"""异步行情采集器实现
负责管理多个具体的行情采集器（如 ZYZmqCollector, CTPCollector），实现并发采集
"""
import asyncio
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
        self._running = True  # 运行标志，用于控制循环退出
        self._init_sub_collectors()

    def _init_sub_collectors(self):
        """根据配置初始化子采集器"""
        if self.market_sources.get("zhengyi_zmq", {}).get("enable"):
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

    def stop(self) -> None:
        """停止采集器运行"""
        self._running = False
        futures_logger.info("采集器已收到停止信号")

    def close_connections(self) -> None:
        """关闭所有子采集器的连接"""
        self.stop()  # 先停止运行标志

        # 关闭所有采集器的连接
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
        
        # 数据分发循环（从所有采集器的队列中取数据）
        # 这是核心的数据处理循环，定期从队列中取出数据并处理
        async def dispatch_loop():
            loop_count = 0
            futures_logger.info("数据分发循环开始运行")
            try:
                while self._running:
                    try:
                        # 从所有采集器的队列中采集数据
                        data = self.collect_data()
                        if data:
                            futures_logger.info(f"分发 {len(data)} 条数据到回调")
                            await on_data_callback(data)
                        
                        # CTP 回调是同步的，需要定期检查队列
                        # 100ms 检查一次，确保及时处理队列中的数据
                        await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        futures_logger.info("数据分发循环被取消")
                        raise
                    except Exception as e:
                        futures_logger.error(f"数据分发循环异常: {e}", exc_info=True)
                        await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                futures_logger.info("数据分发循环已取消")
                raise
            except Exception as e:
                futures_logger.error(f"数据分发循环外层异常: {e}", exc_info=True)
                raise

        # 将数据分发循环添加到任务列表
        tasks.append(dispatch_loop())
        futures_logger.info(f"已启动数据分发循环，总任务数: {len(tasks)}")
        
        try:
            # 使用 return_exceptions=True 避免一个任务异常导致所有任务停止
            # 这会运行所有任务（包括 dispatch_loop），直到它们完成或被取消
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # 检查是否有异常
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    futures_logger.error(f"异步任务 {i} 异常: {result}", exc_info=True)
        except KeyboardInterrupt:
            futures_logger.info("收到中断信号，正在退出...")
            self.stop()
        except Exception as e:
            futures_logger.error(f"异步任务异常: {e}", exc_info=True)
            self.stop()
