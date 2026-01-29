# -*- coding: utf-8 -*-
"""行情采集基类模块
定义统一的采集器抽象接口，所有采集器子类必须实现抽象方法，保证接口一致性
"""
from abc import ABC, abstractmethod
from typing import List, Dict
from src.utils import futures_logger, MarketSourceError

class BaseFuturesCollector(ABC):
    """期货行情采集器基类（抽象类），定义统一采集接口。"""

    def __init__(self, market_sources: Dict):
        """初始化采集器。

        Args:
            market_sources: 行情源配置（从 main_config.yaml 的 market_sources 读取）。

        Raises:
            MarketSourceError: 未启用任何行情源时抛出。
        """
        self.market_sources = market_sources
        # 启用的行情源列表
        self.enabled_sources = [k for k, v in market_sources.items() if v.get("enable", False)]
        futures_logger.info(f"初始化采集器，启用行情源：{self.enabled_sources}")
        if not self.enabled_sources:
            raise MarketSourceError("未启用任何行情源，请检查配置文件")

    @abstractmethod
    def init_connections(self) -> bool:
        """初始化所有启用行情源的连接。

        Returns:
            全部连接成功返回 True，否则 False。
        """
        pass

    @abstractmethod
    def subscribe_market(self) -> bool:
        """订阅指定合约/交易所的行情。

        Returns:
            全部订阅成功返回 True，否则 False。
        """
        pass

    @abstractmethod
    def collect_data(self) -> List[Dict]:
        """采集行情数据（核心方法）。

        Returns:
            标准化行情数据列表（符合 FUTURES_BASE_FIELDS）。
        """
        pass

    @abstractmethod
    def close_connections(self) -> None:
        """关闭所有行情源连接，释放资源。"""
        pass

    def __enter__(self):
        """上下文管理器入口：初始化连接并订阅。

        Raises:
            MarketSourceError: 初始化或订阅失败时抛出。
        """
        if self.init_connections() and self.subscribe_market():
            futures_logger.info("采集器上下文初始化成功")
            return self
        else:
            raise MarketSourceError("采集器上下文初始化失败")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口：关闭连接并释放资源。"""
        self.close_connections()
        futures_logger.info("采集器上下文已关闭，连接释放完成")