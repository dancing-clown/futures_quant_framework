# -*- coding: utf-8 -*-
"""正瀛 ZMQ API 单元测试
测试 ZYZmqApi 初始化、connect、close、_parse_raw_data 等
"""
import ctypes
import pytest
from unittest.mock import Mock, patch, MagicMock

from src.api.zy_zmq_api import ZYZmqApi, DCEL1_Quotation, CZCEL2_Quotation


class TestZYZmqApi:
    """ZYZmqApi 单元测试"""

    def test_init_default_addresses(self):
        """测试默认地址为空"""
        api = ZYZmqApi()
        assert api.dce_address == ""
        assert api.czce_address == ""
        assert api.is_running is False

    def test_init_with_addresses(self):
        """测试带地址初始化"""
        api = ZYZmqApi(dce_address="tcp://127.0.0.1:23333", czce_address="tcp://127.0.0.1:23355")
        assert api.dce_address == "tcp://127.0.0.1:23333"
        assert api.czce_address == "tcp://127.0.0.1:23355"

    @patch("src.api.zy_zmq_api.zmq.Context")
    def test_connect_creates_sockets(self, mock_context):
        """测试 connect 时创建 SUB 并连接（mock zmq）"""
        mock_ctx = MagicMock()
        mock_context.return_value = mock_ctx
        mock_socket = MagicMock()
        mock_ctx.socket.return_value = mock_socket
        api = ZYZmqApi(dce_address="tcp://127.0.0.1:23333")
        result = api.connect()
        assert result is True
        assert api.dce_sub is not None
        assert api.is_running is True
        mock_socket.connect.assert_called_once_with("tcp://127.0.0.1:23333")

    def test_close_sets_running_false(self):
        """测试 close 将 is_running 设为 False 并 term context"""
        api = ZYZmqApi()
        api.context = MagicMock()
        api.dce_sub = None
        api.czce_sub = None
        api.close()
        assert api.is_running is False
        api.context.term.assert_called_once()

    def test_parse_raw_data_dce_l1(self):
        """测试 DCE L1 原始字节解析为 dict"""
        api = ZYZmqApi()
        size = ctypes.sizeof(DCEL1_Quotation)
        buf = (ctypes.c_char * size)()
        data = bytes(buf)
        result = api._parse_raw_data(data, "DCE")
        assert result is not None
        assert result["type"] == "DCE_L1"
        assert "data" in result

    def test_parse_raw_data_czce_l1(self):
        """测试 CZCE L1 原始字节解析为 dict"""
        api = ZYZmqApi()
        size = ctypes.sizeof(CZCEL2_Quotation)
        buf = (ctypes.c_char * size)()
        data = bytes(buf)
        result = api._parse_raw_data(data, "CZCE")
        assert result is not None
        assert result["type"] == "CZCE_L1"
        assert "data" in result

    def test_parse_raw_data_unknown_exchange(self):
        """测试未知交易所返回 None"""
        api = ZYZmqApi()
        assert api._parse_raw_data(b"xxx", "UNKNOWN") is None
