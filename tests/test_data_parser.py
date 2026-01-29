# -*- coding: utf-8 -*-
"""数据解析模块单元测试
测试 DataParser.parse_raw_data 及 CTP/DCE/CZCE 解析逻辑
"""
import datetime
import pytest
from unittest.mock import MagicMock

from src.processor.data_parser import DataParser, FUTURES_BASE_FIELDS


class TestDataParser:
    """DataParser 单元测试"""

    def test_parse_raw_data_none_data(self):
        """测试 data 为空时返回 None"""
        result = DataParser.parse_raw_data({"type": "CTP_TICK", "data": None})
        assert result is None

    def test_parse_raw_data_empty_msg(self):
        """测试无 data 键时返回 None"""
        result = DataParser.parse_raw_data({"type": "CTP_TICK"})
        assert result is None

    def test_parse_raw_data_unknown_type(self):
        """测试未知 type 时返回 None"""
        result = DataParser.parse_raw_data({"type": "UNKNOWN", "data": MagicMock()})
        assert result is None

    def test_parse_ctp_tick_success(self):
        """测试 CTP Tick 解析成功"""
        mock_obj = MagicMock()
        mock_obj.InstrumentID = "rb2505"
        mock_obj.ExchangeID = "SHFE"
        mock_obj.UpdateTime = "09:30:00"
        mock_obj.ActionDay = "20250129"
        mock_obj.UpdateMillisec = 500
        mock_obj.LastPrice = 3500.0
        mock_obj.Volume = 1000
        mock_obj.OpenInterest = 50000.0
        mock_obj.BidPrice1 = 3499.0
        mock_obj.BidVolume1 = 100
        mock_obj.AskPrice1 = 3501.0
        mock_obj.AskVolume1 = 200
        mock_obj.OpenPrice = 3490.0
        mock_obj.HighestPrice = 3510.0
        mock_obj.LowestPrice = 3485.0
        mock_obj.PreClosePrice = 3488.0
        mock_obj.PreSettlementPrice = 3486.0

        raw = {"type": "CTP_TICK", "data": mock_obj}
        result = DataParser.parse_raw_data(raw)
        assert result is not None
        assert result["symbol"] == "rb2505"
        assert result["exchange"] == "SHFE"
        assert result["last_price"] == 3500.0
        assert result["volume"] == 1000
        for field in FUTURES_BASE_FIELDS:
            assert field in result

    def test_futures_base_fields_defined(self):
        """测试 FUTURES_BASE_FIELDS 包含核心字段"""
        assert "symbol" in FUTURES_BASE_FIELDS
        assert "exchange" in FUTURES_BASE_FIELDS
        assert "last_price" in FUTURES_BASE_FIELDS
        assert "datetime" in FUTURES_BASE_FIELDS
