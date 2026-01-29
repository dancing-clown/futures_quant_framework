# -*- coding: utf-8 -*-
"""数据清洗模块单元测试
测试 DataCleaner.clean 去重、校验、容量清理等逻辑
"""
import datetime
import pytest

from src.processor.data_cleaner import DataCleaner


class TestDataCleaner:
    """DataCleaner 单元测试"""

    @pytest.fixture
    def cleaner(self):
        return DataCleaner()

    def _make_record(self, symbol="rb2505", dt=None, last_price=3500.0):
        dt = dt or datetime.datetime.now()
        return {
            "symbol": symbol,
            "datetime": dt,
            "last_price": last_price,
            "exchange": "SHFE",
        }

    def test_clean_empty_list(self, cleaner):
        """测试空列表返回空列表"""
        assert cleaner.clean([]) == []

    def test_clean_deduplication(self, cleaner):
        """测试去重：相同 symbol+datetime 只保留一条"""
        dt = datetime.datetime(2025, 1, 29, 9, 30, 0)
        data_list = [
            self._make_record("rb2505", dt, 3500.0),
            self._make_record("rb2505", dt, 3501.0),
        ]
        result = cleaner.clean(data_list)
        assert len(result) == 1
        assert result[0]["last_price"] == 3500.0

    def test_clean_filter_no_last_price(self, cleaner):
        """测试无 last_price 的记录被过滤"""
        data_list = [
            self._make_record("rb2505", datetime.datetime.now(), 3500.0),
            self._make_record("au2506", datetime.datetime.now(), None),
        ]
        result = cleaner.clean(data_list)
        assert len(result) == 1
        assert result[0]["symbol"] == "rb2505"

    def test_clean_multiple_symbols(self, cleaner):
        """测试不同合约、不同时间均保留"""
        base = datetime.datetime(2025, 1, 29, 9, 30, 0)
        data_list = [
            self._make_record("rb2505", base, 3500.0),
            self._make_record("au2506", base.replace(minute=31), 520.0),
        ]
        result = cleaner.clean(data_list)
        assert len(result) == 2
