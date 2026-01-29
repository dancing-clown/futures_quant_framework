# -*- coding: utf-8 -*-
"""采集器基类单元测试
测试 BaseFuturesCollector 抽象接口及启用行情源校验
"""
import pytest

from src.collector.base_collector import BaseFuturesCollector
from src.utils import MarketSourceError


class ConcreteCollector(BaseFuturesCollector):
    """用于测试的具象采集器"""

    def init_connections(self):
        return True

    def subscribe_market(self):
        return True

    def collect_data(self):
        return []

    def close_connections(self):
        pass


class TestBaseFuturesCollector:
    """BaseFuturesCollector 单元测试"""

    def test_no_enabled_source_raises(self):
        """测试未启用任何行情源时抛出 MarketSourceError"""
        market_sources = {"ctp": {"enable": False}, "zhengyi_zmq": {"enable": False}}
        with pytest.raises(MarketSourceError):
            ConcreteCollector(market_sources)

    def test_with_enabled_source_ok(self):
        """测试至少启用一个行情源时初始化成功"""
        market_sources = {"ctp": {"enable": True}}
        c = ConcreteCollector(market_sources)
        assert c.enabled_sources == ["ctp"]

    def test_context_manager_enter_exit(self):
        """测试上下文管理器进入与退出"""
        market_sources = {"ctp": {"enable": True}}
        with ConcreteCollector(market_sources) as c:
            assert c.init_connections() is True
            assert c.subscribe_market() is True
        # __exit__ 会调用 close_connections
