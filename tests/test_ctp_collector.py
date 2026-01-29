# -*- coding: utf-8 -*-
"""CTP 采集器单元测试
测试 CTPCollector 初始化、队列回调、collect_data 与 DataParser 配合等
"""
import queue
import pytest
from unittest.mock import Mock, patch, MagicMock

from src.collector.ctp_collector import CTPCollector


class TestCTPCollector:
    """CTPCollector 单元测试"""

    @pytest.fixture
    def market_sources(self):
        return {
            "ctp": {
                "enable": True,
                "host": "tcp://182.254.243.31:40011",
                "flow_path": "./flow/",
                "subscribe_codes": ["rb2505"],
            }
        }

    def test_init_sets_queue_and_api(self, market_sources):
        """测试初始化时创建队列并设置 api"""
        with patch("src.collector.ctp_collector.CtpMarketApi"):
            c = CTPCollector(market_sources)
            assert isinstance(c.data_queue, queue.Queue)
            assert c.subscribe_codes == ["rb2505"]

    def test_on_data_received_puts_to_queue(self, market_sources):
        """测试 on_data_received 将消息放入队列"""
        with patch("src.collector.ctp_collector.CtpMarketApi"):
            c = CTPCollector(market_sources)
            c.on_data_received({"type": "CTP_TICK", "data": None})
            assert c.data_queue.qsize() == 1
            msg = c.data_queue.get_nowait()
            assert msg["type"] == "CTP_TICK"

    def test_collect_data_empty_queue(self, market_sources):
        """测试队列为空时 collect_data 返回空列表"""
        with patch("src.collector.ctp_collector.CtpMarketApi"):
            c = CTPCollector(market_sources)
            result = c.collect_data()
            assert result == []

    def test_collect_data_parses_and_returns_std_data(self, market_sources):
        """测试从队列取数据经 DataParser 解析后返回标准化数据"""
        with patch("src.collector.ctp_collector.CtpMarketApi"), patch(
            "src.collector.ctp_collector.DataParser"
        ) as MockParser:
            MockParser.parse_raw_data.return_value = {"symbol": "rb2505", "last_price": 3500.0}
            c = CTPCollector(market_sources)
            c.data_queue.put({"type": "CTP_TICK", "data": MagicMock()})
            result = c.collect_data()
            assert len(result) == 1
            assert result[0]["symbol"] == "rb2505"
            assert result[0]["last_price"] == 3500.0

    def test_close_connections_calls_api_close(self, market_sources):
        """测试 close_connections 调用 api.close"""
        with patch("src.collector.ctp_collector.CtpMarketApi") as MockApi:
            mock_api_inst = Mock()
            MockApi.return_value = mock_api_inst
            c = CTPCollector(market_sources)
            c.close_connections()
            mock_api_inst.close.assert_called_once()
