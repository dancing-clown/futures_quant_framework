# -*- coding: utf-8 -*-
"""正瀛 ZMQ 采集器单元测试
测试 ZYZmqCollector 初始化、队列、collect_data 与 DataParser 配合等
"""
import queue
import pytest
from unittest.mock import Mock, patch

from src.collector.zy_collector import ZYZmqCollector


class TestZYZmqCollector:
    """ZYZmqCollector 单元测试"""

    @pytest.fixture
    def market_sources(self):
        return {
            "zhengyi_zmq": {
                "enable": True,
                "dce_address": "tcp://127.0.0.1:23333",
                "czce_address": "tcp://127.0.0.1:23355",
            }
        }

    def test_init_sets_queue_and_api(self, market_sources):
        """测试初始化时创建队列并设置 api"""
        with patch("src.collector.zy_collector.ZYZmqApi"):
            c = ZYZmqCollector(market_sources)
            assert isinstance(c.data_queue, queue.Queue)

    def test_on_data_received_puts_to_queue(self, market_sources):
        """测试 on_data_received 将消息放入队列"""
        with patch("src.collector.zy_collector.ZYZmqApi"):
            c = ZYZmqCollector(market_sources)
            c.on_data_received({"type": "DCE_L1", "data": None})
            assert c.data_queue.qsize() == 1
            msg = c.data_queue.get_nowait()
            assert msg["type"] == "DCE_L1"

    def test_collect_data_empty_queue(self, market_sources):
        """测试队列为空时 collect_data 返回空列表"""
        with patch("src.collector.zy_collector.ZYZmqApi"):
            c = ZYZmqCollector(market_sources)
            assert c.collect_data() == []

    def test_collect_data_parses_and_returns_std_data(self, market_sources):
        """测试从队列取数据经 DataParser 解析后返回"""
        with patch("src.collector.zy_collector.ZYZmqApi"), patch(
            "src.collector.zy_collector.DataParser"
        ) as MockParser:
            MockParser.parse_raw_data.return_value = {"symbol": "y2505", "last_price": 7500.0}
            c = ZYZmqCollector(market_sources)
            c.data_queue.put({"type": "DCE_L1", "data": None})
            result = c.collect_data()
            assert len(result) == 1
            assert result[0]["symbol"] == "y2505"

    def test_subscribe_market_returns_true(self, market_sources):
        """测试 subscribe_market 返回 True（全量订阅）"""
        with patch("src.collector.zy_collector.ZYZmqApi"):
            c = ZYZmqCollector(market_sources)
            assert c.subscribe_market() is True
