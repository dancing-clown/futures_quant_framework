# -*- coding: utf-8 -*-
"""工具模块单元测试
测试 utils 中的异常类、common_tools 中的时间/合约解析/校验函数
"""
import datetime
import pytest

from src.utils.exceptions import (
    FuturesBaseError,
    MarketSourceError,
    DataParseError,
    StorageError,
    CollectError,
)
from src.utils.common_tools import (
    dt2timestamp,
    timestamp2dt,
    parse_futures_code,
    check_data_validity,
    FUTURES_BASE_FIELDS,
)


class TestExceptions:
    """自定义异常单元测试"""

    def test_futures_base_error_message(self):
        """测试基础异常消息"""
        err = FuturesBaseError("test message")
        assert err.message == "test message"
        assert "FuturesBaseError" in str(err)

    def test_market_source_error_inheritance(self):
        """测试 MarketSourceError 继承"""
        err = MarketSourceError("连接失败")
        assert isinstance(err, FuturesBaseError)
        assert err.message == "连接失败"

    def test_data_parse_error(self):
        """测试 DataParseError"""
        err = DataParseError("字段缺失")
        assert "DataParseError" in str(err)

    def test_storage_error(self):
        """测试 StorageError"""
        err = StorageError("写入失败")
        assert err.message == "写入失败"

    def test_collect_error(self):
        """测试 CollectError"""
        err = CollectError("采集超时")
        assert isinstance(err, FuturesBaseError)


class TestCommonTools:
    """common_tools 单元测试"""

    def test_dt2timestamp_datetime(self):
        """测试 datetime 转毫秒时间戳"""
        dt = datetime.datetime(2025, 1, 29, 9, 30, 0)
        ts = dt2timestamp(dt)
        assert isinstance(ts, int)
        assert ts > 0

    def test_dt2timestamp_string(self):
        """测试时间字符串转毫秒时间戳"""
        ts = dt2timestamp("2025-01-29 09:30:00")
        assert isinstance(ts, int)

    def test_timestamp2dt(self):
        """测试毫秒时间戳转时间字符串"""
        ts = 1738114200000  # 2025-01-29 09:30:00 左右（视时区）
        s = timestamp2dt(ts)
        assert isinstance(s, str)
        assert "2025" in s or "2024" in s

    def test_parse_futures_code_valid(self):
        """测试合法合约代码解析"""
        result = parse_futures_code("rb2505")
        assert result is not None
        assert result["symbol"] == "rb"
        assert result["year"] == "2025"
        assert result["month"] == "05"
        assert result["full_code"] == "rb2505"

    def test_parse_futures_code_invalid_length(self):
        """测试数字部分长度不为 4 时返回 None"""
        assert parse_futures_code("rb205") is None
        assert parse_futures_code("rb25051") is None

    def test_check_data_validity_pass(self):
        """测试字段完整时校验通过"""
        data = {"code": "rb2505", "price": 3500.0, "volume": 100}
        assert check_data_validity(data, ["code", "price", "volume"]) is True

    def test_check_data_validity_missing_field(self):
        """测试缺失必选字段时校验失败"""
        data = {"code": "rb2505", "price": 3500.0}
        assert check_data_validity(data, ["code", "price", "volume"]) is False

    def test_check_data_validity_none_value(self):
        """测试必选字段值为 None 时校验失败"""
        data = {"code": "rb2505", "price": None, "volume": 100}
        assert check_data_validity(data, ["code", "price", "volume"]) is False

    def test_futures_base_fields_defined(self):
        """测试 FUTURES_BASE_FIELDS 定义"""
        assert "exchange" in FUTURES_BASE_FIELDS
        assert "code" in FUTURES_BASE_FIELDS
        assert "price" in FUTURES_BASE_FIELDS
        assert "timestamp" in FUTURES_BASE_FIELDS
