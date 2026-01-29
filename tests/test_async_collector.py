# -*- coding: utf-8 -*-
"""异步采集器单元测试
测试 AsyncFuturesCollector 初始化、连接、汇总数据、停止等逻辑
"""
import pytest
from unittest.mock import Mock, MagicMock, patch

from src.collector.async_collector import AsyncFuturesCollector


class TestAsyncFuturesCollector:
    """AsyncFuturesCollector 单元测试"""

    def test_init_only_ctp_enabled(self):
        """测试仅启用 CTP 时只创建 CTPCollector"""
        market_sources = {"ctp": {"enable": True}, "zhengyi_zmq": {"enable": False}}
        collector = AsyncFuturesCollector(market_sources)
        assert len(collector.collectors) == 1
        assert collector.collectors[0].__class__.__name__ == "CTPCollector"

    def test_init_only_zy_enabled(self):
        """测试仅启用正瀛 ZMQ 时只创建 ZYZmqCollector"""
        market_sources = {
            "ctp": {"enable": False},
            "zhengyi_zmq": {"enable": True, "dce_address": "tcp://127.0.0.1:23333", "czce_address": ""},
        }
        collector = AsyncFuturesCollector(market_sources)
        assert len(collector.collectors) == 1
        assert collector.collectors[0].__class__.__name__ == "ZYZmqCollector"

    def test_init_connections_all_success(self):
        """测试所有子采集器连接成功时返回 True"""
        market_sources = {"ctp": {"enable": True}}
        with patch("src.collector.async_collector.CTPCollector") as MockCTP:
            mock_inst = Mock()
            mock_inst.init_connections.return_value = True
            MockCTP.return_value = mock_inst
            collector = AsyncFuturesCollector(market_sources)
            result = collector.init_connections()
            assert result is True

    def test_collect_data_aggregates(self):
        """测试 collect_data 汇总所有子采集器数据"""
        market_sources = {"ctp": {"enable": True}, "zhengyi_zmq": {"enable": True, "dce_address": "", "czce_address": ""}}
        with patch("src.collector.async_collector.CTPCollector") as MockCTP, patch(
            "src.collector.async_collector.ZYZmqCollector"
        ) as MockZY:
            mock_ctp = Mock()
            mock_ctp.collect_data.return_value = [{"a": 1}]
            mock_zy = Mock()
            mock_zy.collect_data.return_value = [{"b": 2}]
            MockCTP.return_value = mock_ctp
            MockZY.return_value = mock_zy
            collector = AsyncFuturesCollector(market_sources)
            data = collector.collect_data()
            assert len(data) == 2
            # 子采集器顺序可能为 ctp+zy 或 zy+ctp，只校验汇总结果
            assert {"a": 1} in data
            assert {"b": 2} in data

    def test_stop_sets_running_false(self):
        """测试 stop 将 _running 设为 False"""
        market_sources = {"ctp": {"enable": True}}
        with patch("src.collector.async_collector.CTPCollector"):
            collector = AsyncFuturesCollector(market_sources)
            assert collector._running is True
            collector.stop()
            assert collector._running is False
