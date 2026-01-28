# -*- coding: utf-8 -*-
"""CTP API 单元测试
测试 ctp_api.py 中的核心功能
"""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.ctp_api import CtpMarketApi, CtpSpiWrapper, setup_ctp_path


class TestCtpMarketApi:
    """CTP Market API 单元测试"""
    
    def test_api_initialization_with_anonymous_login(self):
        """测试匿名登录初始化"""
        api = CtpMarketApi(
            front_address="tcp://182.254.243.31:40011",
            flow_path="./test_flow/"
        )
        assert api.front_address == "tcp://182.254.243.31:40011"
        assert api.use_anonymous_login is True
        assert api.broker_id is None
        assert api.investor_id is None
        assert api.password is None
    
    def test_api_initialization_with_credentials(self):
        """测试认证登录初始化"""
        api = CtpMarketApi(
            front_address="tcp://182.254.243.31:40011",
            broker_id="9999",
            investor_id="test_user",
            password="test_password"
        )
        assert api.front_address == "tcp://182.254.243.31:40011"
        assert api.use_anonymous_login is False
        assert api.broker_id == "9999"
        assert api.investor_id == "test_user"
        assert api.password == "test_password"
    
    def test_api_initialization_with_subscribe_symbols(self):
        """测试带订阅列表的初始化"""
        symbols = ["rb2505", "au2506", "cu2505"]
        api = CtpMarketApi(
            front_address="tcp://182.254.243.31:40011",
            subscribe_symbols=symbols
        )
        assert api.subscribe_symbols == symbols
    
    @patch('src.api.ctp_api.ctp_pybind', None)
    def test_connect_without_pybind(self):
        """测试在没有 pybind 模块时的连接"""
        api = CtpMarketApi(front_address="tcp://182.254.243.31:40011")
        callback = Mock()
        result = api.connect(callback, auto_subscribe=False)
        assert result is False
    
    @patch('src.api.ctp_api.ctp_pybind')
    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_connect_success(self, mock_makedirs, mock_exists, mock_ctp_pybind):
        """测试成功连接"""
        mock_exists.return_value = True
        mock_api = MagicMock()
        mock_ctp_pybind.CThostFtdcMdApi.return_value = mock_api
        
        api = CtpMarketApi(front_address="tcp://182.254.243.31:40011")
        callback = Mock()
        result = api.connect(callback, auto_subscribe=False)
        
        assert result is True
        assert api.api is not None
        assert api.spi is not None
        mock_api.RegisterSpi.assert_called_once()
        mock_api.RegisterFront.assert_called_once_with("tcp://182.254.243.31:40011")
        mock_api.Init.assert_called_once()
    
    @patch('src.api.ctp_api.ctp_pybind', None)
    def test_login_without_pybind(self):
        """测试在没有 pybind 模块时的登录"""
        api = CtpMarketApi(front_address="tcp://182.254.243.31:40011")
        result = api.login()
        assert result is False
    
    @patch('src.api.ctp_api.ctp_pybind')
    def test_anonymous_login(self, mock_ctp_pybind):
        """测试匿名登录"""
        mock_api = MagicMock()
        mock_api.ReqUserLogin.return_value = 0
        mock_ctp_pybind.CThostFtdcReqUserLoginField.return_value = MagicMock()
        
        api = CtpMarketApi(front_address="tcp://182.254.243.31:40011")
        api.api = mock_api
        
        result = api.login()
        
        assert result is True
        mock_api.ReqUserLogin.assert_called_once()
        # 验证登录字段为空（匿名登录）
        login_field = mock_api.ReqUserLogin.call_args[0][0]
        assert login_field is not None
    
    @patch('src.api.ctp_api.ctp_pybind')
    def test_authenticated_login(self, mock_ctp_pybind):
        """测试认证登录"""
        mock_api = MagicMock()
        mock_api.ReqUserLogin.return_value = 0
        mock_login_field = MagicMock()
        mock_ctp_pybind.CThostFtdcReqUserLoginField.return_value = mock_login_field
        
        api = CtpMarketApi(
            front_address="tcp://182.254.243.31:40011",
            broker_id="9999",
            investor_id="test_user",
            password="test_password"
        )
        api.api = mock_api
        
        result = api.login()
        
        assert result is True
        assert mock_login_field.BrokerID == "9999"
        assert mock_login_field.UserID == "test_user"
        assert mock_login_field.Password == "test_password"
        mock_api.ReqUserLogin.assert_called_once()
    
    @patch('src.api.ctp_api.ctp_pybind')
    def test_subscribe_without_api(self, mock_ctp_pybind):
        """测试在没有 API 实例时的订阅"""
        api = CtpMarketApi(front_address="tcp://182.254.243.31:40011")
        result = api.subscribe(["rb2505"])
        assert result is False
    
    @patch('src.api.ctp_api.ctp_pybind')
    def test_subscribe_success(self, mock_ctp_pybind):
        """测试成功订阅"""
        mock_api = MagicMock()
        mock_api.SubscribeMarketData.return_value = 0
        
        api = CtpMarketApi(
            front_address="tcp://182.254.243.31:40011",
            subscribe_symbols=["rb2505", "au2506"]
        )
        api.api = mock_api
        api.is_logged_in = True
        
        result = api.subscribe()
        
        assert result is True
        mock_api.SubscribeMarketData.assert_called_once_with(["rb2505", "au2506"])
    
    def test_close(self):
        """测试关闭连接"""
        api = CtpMarketApi(front_address="tcp://182.254.243.31:40011")
        api.api = MagicMock()
        api.is_connected = True
        api.is_logged_in = True
        
        api.close()
        
        assert api.api is None
        assert api.is_connected is False
        assert api.is_logged_in is False


class TestCtpSpiWrapper:
    """CTP SPI Wrapper 单元测试"""
    
    @patch('src.api.ctp_api.ctp_pybind', None)
    def test_spi_initialization_without_pybind(self):
        """测试在没有 pybind 时的 SPI 初始化"""
        api_instance = Mock()
        callback = Mock()
        spi = CtpSpiWrapper(api_instance, callback)
        assert spi.api_instance == api_instance
        assert spi.callback == callback
        assert spi.is_logged_in is False
    
    def test_on_front_connected(self):
        """测试前置连接成功回调"""
        api_instance = Mock()
        api_instance.login = Mock()
        callback = Mock()
        spi = CtpSpiWrapper(api_instance, callback)
        
        spi.OnFrontConnected()
        
        api_instance.login.assert_called_once()
    
    def test_on_front_disconnected(self):
        """测试前置连接断开回调"""
        api_instance = Mock()
        callback = Mock()
        spi = CtpSpiWrapper(api_instance, callback)
        spi.is_logged_in = True
        
        spi.OnFrontDisconnected(1001)
        
        assert spi.is_logged_in is False
    
    def test_on_rsp_user_login_success(self):
        """测试登录成功回调"""
        api_instance = Mock()
        api_instance.subscribe = Mock()
        api_instance.subscribe_symbols = ["rb2505"]
        callback = Mock()
        spi = CtpSpiWrapper(api_instance, callback)
        spi.subscribe_symbols = ["rb2505"]
        
        # 模拟登录响应
        rsp_info = Mock()
        rsp_info.ErrorID = 0
        rsp_user_login = Mock()
        
        spi.OnRspUserLogin(rsp_user_login, rsp_info, 1, True)
        
        assert spi.is_logged_in is True
        api_instance.subscribe.assert_called_once()
    
    def test_on_rsp_user_login_failure(self):
        """测试登录失败回调"""
        api_instance = Mock()
        callback = Mock()
        spi = CtpSpiWrapper(api_instance, callback)
        
        # 模拟登录失败响应
        rsp_info = Mock()
        rsp_info.ErrorID = 1
        rsp_info.ErrorMsg = "Login failed"
        
        spi.OnRspUserLogin(None, rsp_info, 1, True)
        
        assert spi.is_logged_in is False
    
    def test_on_rtn_depth_market_data(self):
        """测试行情数据推送回调"""
        api_instance = Mock()
        callback = Mock()
        spi = CtpSpiWrapper(api_instance, callback)
        
        market_data = Mock()
        spi.OnRtnDepthMarketData(market_data)
        
        callback.assert_called_once()
        call_args = callback.call_args[0][0]
        assert call_args['type'] == 'CTP_TICK'
        assert call_args['data'] == market_data


class TestSetupCtpPath:
    """CTP 路径设置测试"""
    
    def test_setup_ctp_path_with_custom_path(self):
        """测试自定义路径设置"""
        custom_path = "/custom/path"
        # 这个测试主要验证函数不会抛出异常
        setup_ctp_path(custom_path)
        # 如果路径不存在，应该不会添加到 sys.path
        # 这里主要测试函数可执行性
    
    def test_setup_ctp_path_with_none(self):
        """测试 None 路径设置"""
        setup_ctp_path(None)
        # 主要验证函数不会抛出异常
