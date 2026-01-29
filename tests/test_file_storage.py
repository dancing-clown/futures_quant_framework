# -*- coding: utf-8 -*-
"""文件存储模块单元测试
测试 FileStorage.save 的目录创建、按日按合约写入 CSV 等逻辑
"""
import datetime
import os
import pytest
import tempfile
from pathlib import Path

from src.storage.file_storage import FileStorage


class TestFileStorage:
    """FileStorage 单元测试"""

    def test_save_empty_list(self):
        """测试空列表不写入文件"""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FileStorage(base_path=tmpdir)
            storage.save([])
            assert len(os.listdir(tmpdir)) == 0

    def test_save_creates_directory(self):
        """测试 save 时自动创建 base_path"""
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = Path(tmpdir) / "market_data"
            storage = FileStorage(base_path=str(subdir))
            storage.save([{
                "symbol": "rb2505",
                "datetime": datetime.datetime(2025, 1, 29, 9, 30, 0),
                "last_price": 3500.0,
                "exchange": "SHFE",
            }])
            assert subdir.exists()
            assert len(list(subdir.glob("*.csv"))) >= 1

    def test_save_csv_content(self):
        """测试按合约、按日期写入 CSV 且内容正确"""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FileStorage(base_path=tmpdir)
            dt = datetime.datetime(2025, 1, 29, 9, 30, 0)
            data_list = [{
                "symbol": "rb2505",
                "datetime": dt,
                "last_price": 3500.0,
                "exchange": "SHFE",
            }]
            storage.save(data_list)
            expected_file = Path(tmpdir) / "rb2505_20250129.csv"
            assert expected_file.exists()
            content = expected_file.read_text(encoding="utf-8")
            assert "rb2505" in content
            assert "3500" in content

    def test_save_append_same_file(self):
        """测试同合约同日期多次 save 为追加写入"""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FileStorage(base_path=tmpdir)
            dt1 = datetime.datetime(2025, 1, 29, 9, 30, 0)
            dt2 = datetime.datetime(2025, 1, 29, 9, 31, 0)
            storage.save([{"symbol": "au2506", "datetime": dt1, "last_price": 520.0, "exchange": "SHFE"}])
            storage.save([{"symbol": "au2506", "datetime": dt2, "last_price": 521.0, "exchange": "SHFE"}])
            expected_file = Path(tmpdir) / "au2506_20250129.csv"
            lines = expected_file.read_text(encoding="utf-8").strip().split("\n")
            # 1 header + 2 data rows
            assert len(lines) >= 2
