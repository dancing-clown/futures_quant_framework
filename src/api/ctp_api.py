# -*- coding: utf-8 -*-
"""CTP 行情接口封装
使用 pybind11 生成的 ctp_pybind 模块实现具体的 CTP 行情对接
"""
import os
import sys
from typing import Optional, Callable, List
from src.utils import futures_logger

def setup_ctp_path(custom_path: Optional[str] = None):
    """自动将 pybind 编译路径加入 sys.path"""
    search_paths = [
        custom_path,
        os.path.join(os.getcwd(), "extern_libs/ctp_pybind/build"),
        os.path.join(os.path.dirname(__file__), "../../extern_libs/ctp_pybind/build")
    ]
    for path in search_paths:
        if path and os.path.exists(path) and path not in sys.path:
            sys.path.append(path)
            futures_logger.debug(f"已添加 CTP Pybind 搜索路径: {path}")

# 初始化时尝试设置路径
setup_ctp_path()

try:
    # 尝试导入 pybind11 模块
    import ctp_pybind
except ImportError:
    ctp_pybind = None
    # 延迟到使用时再打印警告

BaseSpi = ctp_pybind.CThostFtdcMdSpi if ctp_pybind else object

class CtpSpiWrapper(BaseSpi):
    """CTP 回调处理包装类"""
    def __init__(self, callback: Callable):
        if ctp_pybind:
            super().__init__()
        self.callback = callback

    def OnFrontConnected(self):
        futures_logger.info("CTP 前置连接成功")

    def OnFrontDisconnected(self, nReason: int):
        futures_logger.warning(f"CTP 前置连接断开, 原因: {nReason}")

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            futures_logger.info("CTP 登录成功")
        else:
            error_msg = pRspInfo.ErrorMsg if pRspInfo else "Unknown Error"
            futures_logger.error(f"CTP 登录失败: {error_msg}")

    def OnRtnDepthMarketData(self, pDepthMarketData):
        if self.callback and pDepthMarketData:
            self.callback({"type": "CTP_TICK", "data": pDepthMarketData})

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        if pRspInfo:
            futures_logger.error(f"CTP 响应错误: {pRspInfo.ErrorMsg}")

class CtpMarketApi:
    """
    CTP 行情接口封装类
    """
    def __init__(self, broker_id: str, investor_id: str, password: str, front_address: str, 
                 flow_path: str = "./flow/", pybind_path: Optional[str] = None):
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password
        self.front_address = front_address
        self.flow_path = flow_path
        
        if pybind_path:
            setup_ctp_path(pybind_path)
            global ctp_pybind
            if not ctp_pybind:
                try:
                    import ctp_pybind
                except ImportError:
                    pass
        
        if not os.path.exists(self.flow_path):
            os.makedirs(self.flow_path)
            
        self.api = None
        self.spi = None
        self.is_connected = False

    def connect(self, callback: Callable):
        """连接并初始化 CTP"""
        if not ctp_pybind:
            futures_logger.error("ctp_pybind 模块不可用")
            return False
            
        try:
            self.api = ctp_pybind.CThostFtdcMdApi(self.flow_path)
            self.spi = CtpSpiWrapper(callback)
            
            self.api.RegisterSpi(self.spi)
            self.api.RegisterFront(self.front_address)
            self.api.Init()
            
            futures_logger.info(f"CTP API 已初始化，正在连接: {self.front_address}")
            return True
        except Exception as e:
            futures_logger.error(f"CTP 连接异常: {e}")
            return False

    def login(self):
        """执行登录"""
        if not self.api:
            return False
            
        login_field = ctp_pybind.CThostFtdcReqUserLoginField()
        login_field.BrokerID = self.broker_id
        login_field.UserID = self.investor_id
        login_field.Password = self.password
        
        ret = self.api.ReqUserLogin(login_field, 1)
        return ret == 0

    def subscribe(self, symbols: List[str]):
        """订阅行情"""
        if not self.api:
            return False
        ret = self.api.SubscribeMarketData(symbols)
        return ret == 0

    def close(self):
        """释放资源"""
        if self.api:
            # pybind11 包装的析构函数会自动调用 Release
            self.api = None
        self.is_connected = False
        futures_logger.info("CTP API 已释放")
