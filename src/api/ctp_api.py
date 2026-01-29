# -*- coding: utf-8 -*-
"""CTP 行情接口封装
使用 pybind11 生成的 ctp_pybind 模块实现具体的 CTP 行情对接
"""
import os
import sys
import threading
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
    # 导出 CTP 数据结构，供其他模块使用
    CThostFtdcDepthMarketDataField = ctp_pybind.CThostFtdcDepthMarketDataField
    CThostFtdcReqUserLoginField = ctp_pybind.CThostFtdcReqUserLoginField
    CThostFtdcRspUserLoginField = ctp_pybind.CThostFtdcRspUserLoginField
    CThostFtdcRspInfoField = ctp_pybind.CThostFtdcRspInfoField
except ImportError:
    ctp_pybind = None
    # 延迟到使用时再打印警告
    # 定义占位符类，避免导入错误
    class CThostFtdcDepthMarketDataField:
        pass
    class CThostFtdcReqUserLoginField:
        pass
    class CThostFtdcRspUserLoginField:
        pass
    class CThostFtdcRspInfoField:
        pass

BaseSpi = ctp_pybind.CThostFtdcMdSpi if ctp_pybind else object

class CtpSpiWrapper(BaseSpi):
    """CTP 回调处理包装类"""
    def __init__(self, api_instance, callback: Callable):
        if ctp_pybind:
            super().__init__()
        self.api_instance = api_instance  # 持有 API 实例引用，用于回调中调用登录和订阅
        self.callback = callback
        self.is_logged_in = False
        self.subscribe_symbols = []

    def OnFrontConnected(self):
        """前置连接成功回调，自动执行登录（参考 Rust 版本的 on_front_connected）"""
        futures_logger.info("CTP 前置连接成功，开始登录...")
        if self.api_instance:
            self.api_instance.login()

    def OnFrontDisconnected(self, nReason: int):
        """前置连接断开回调"""
        futures_logger.warning(f"CTP 前置连接断开, 原因: {nReason}")
        self.is_logged_in = False

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登录响应回调，登录成功后自动订阅（参考 Rust 版本的 on_rsp_user_login）"""
        if pRspInfo and pRspInfo.ErrorID == 0:
            self.is_logged_in = True
            if self.api_instance:
                self.api_instance.is_logged_in = True
            if pRspUserLogin:
                trading_day = getattr(pRspUserLogin, 'TradingDay', '')
                login_time = getattr(pRspUserLogin, 'LoginTime', '')
                broker_id = getattr(pRspUserLogin, 'BrokerID', '')
                user_id = getattr(pRspUserLogin, 'UserID', '')
                futures_logger.info(
                    f"CTP 登录成功 - TradingDay: {trading_day}, LoginTime: {login_time}, "
                    f"BrokerID: {broker_id}, UserID: {user_id}"
                )
            else:
                futures_logger.info("CTP 登录成功")
            
            # 登录成功后自动订阅（参考 Rust 版本逻辑）
            if self.api_instance:
                symbols_to_subscribe = self.subscribe_symbols or self.api_instance.subscribe_symbols
                if symbols_to_subscribe:
                    futures_logger.info(f"开始订阅行情: {symbols_to_subscribe}")
                    self.api_instance.subscribe(symbols_to_subscribe)
                else:
                    futures_logger.warning("订阅列表为空，无法自动订阅")
        else:
            error_msg = pRspInfo.ErrorMsg if pRspInfo else "Unknown Error"
            error_id = pRspInfo.ErrorID if pRspInfo else -1
            futures_logger.error(f"CTP 登录失败 - ErrorID: {error_id}, ErrorMsg: {error_msg}")

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        """订阅行情响应回调"""
        if pRspInfo:
            instrument_id = getattr(pSpecificInstrument, 'InstrumentID', '') if pSpecificInstrument else '未知合约'
            if pRspInfo.ErrorID == 0:
                futures_logger.info(f"订阅成功: {instrument_id}")
            else:
                futures_logger.warning(
                    f"订阅失败 - InstrumentID: {instrument_id}, "
                    f"ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}"
                )
        else:
            instrument_id = getattr(pSpecificInstrument, 'InstrumentID', '') if pSpecificInstrument else ''
            futures_logger.info(f"订阅响应: {instrument_id} (无错误信息)")

    def OnRtnDepthMarketData(self, pDepthMarketData):
        """行情数据推送回调"""
        if self.callback and pDepthMarketData:
            try:
                import threading
                thread_id = threading.current_thread().ident
                instrument_id = getattr(pDepthMarketData, 'InstrumentID', '')
                last_price = getattr(pDepthMarketData, 'LastPrice', 0.0)
                futures_logger.info(f"收到行情数据: {instrument_id}, 最新价: {last_price}, 线程ID: {thread_id}")
                # 调用回调，将数据放入队列
                self.callback({"type": "CTP_TICK", "data": pDepthMarketData})
                futures_logger.debug(f"行情数据回调已调用，数据已放入队列: {instrument_id}")
            except Exception as e:
                futures_logger.error(f"处理行情数据回调异常: {e}", exc_info=True)
        else:
            futures_logger.warning("行情数据推送回调异常，callback 或 pDepthMarketData 为空")

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        """错误响应回调"""
        if pRspInfo:
            futures_logger.error(
                f"CTP 响应错误 - ErrorID: {pRspInfo.ErrorID}, "
                f"ErrorMsg: {pRspInfo.ErrorMsg}, RequestID: {nRequestID}"
            )
        else:
            futures_logger.warning("错误响应回调异常，pRspInfo 为空")

class CtpMarketApi:
    """
    CTP 行情接口封装类
    """
    def __init__(self, front_address: str, flow_path: str, 
                 pybind_path: Optional[str] = None,
                 subscribe_symbols: Optional[List[str]] = None,
                 broker_id: Optional[str] = None,
                 investor_id: Optional[str] = None,
                 password: Optional[str] = None):
        self.front_address = front_address
        self.flow_path = flow_path
        self.subscribe_symbols = subscribe_symbols or []
        # 登录信息（可选，如果未提供则使用匿名登录）
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password
        self.use_anonymous_login = not (broker_id and investor_id and password)
        
        if pybind_path:
            setup_ctp_path(pybind_path)
            global ctp_pybind
            if not ctp_pybind:
                try:
                    import ctp_pybind
                except ImportError:
                    pass
        
        if not os.path.exists(self.flow_path):
            os.makedirs(self.flow_path, exist_ok=True)
            
        self.api = None
        self.spi = None
        self.is_connected = False
        self.is_logged_in = False
        self._lock = threading.Lock()

    def connect(self, callback: Callable, auto_subscribe: bool = True):
        """连接并初始化 CTP
        Args:
            callback: 行情数据回调函数
            auto_subscribe: 是否在登录成功后自动订阅（默认 True）
        """
        if not ctp_pybind:
            futures_logger.error("ctp_pybind 模块不可用，请先编译 ctp_pybind")
            return False
            
        try:
            self.api = ctp_pybind.CThostFtdcMdApi(self.flow_path)
            self.spi = CtpSpiWrapper(self, callback)
            
            # 设置订阅列表（在登录成功后会使用）
            if auto_subscribe:
                self.spi.subscribe_symbols = self.subscribe_symbols.copy() if self.subscribe_symbols else []
                futures_logger.debug(f"设置自动订阅列表: {self.spi.subscribe_symbols}")
            
            self.api.RegisterSpi(self.spi)
            futures_logger.debug(f"注册 SPI")
            futures_logger.debug(f"注册前台地址: {self.front_address}")
            self.api.RegisterFront(self.front_address)
            futures_logger.debug(f"注册前台地址完成: {self.front_address}")
            self.api.Init()
            futures_logger.debug(f"初始化 API")
            
            futures_logger.info(f"CTP API 已初始化，正在连接: {self.front_address}")
            futures_logger.info(f"订阅列表: {self.subscribe_symbols}")
            self.is_connected = True
            return True
        except Exception as e:
            futures_logger.error(f"CTP 连接异常: {e}", exc_info=True)
            return False

    def login(self):
        """执行登录（通常在 OnFrontConnected 回调中自动调用）
        如果未提供登录信息，则使用空字段（匿名登录）
        """
        if not self.api:
            futures_logger.error("CTP API 未初始化，无法登录")
            return False
        
        if not ctp_pybind:
            futures_logger.error("ctp_pybind 模块不可用")
            return False
            
        try:
            login_field = ctp_pybind.CThostFtdcReqUserLoginField()
            
            # 如果提供了登录信息则使用，否则使用空字段（匿名登录）
            if not self.use_anonymous_login:
                login_field.BrokerID = self.broker_id
                login_field.UserID = self.investor_id
                login_field.Password = self.password
                futures_logger.info(f"使用认证登录 - BrokerID: {self.broker_id}, UserID: {self.investor_id}")
            else:
                # 使用空字段，匿名登录
                futures_logger.info("使用匿名登录（空登录字段）")
            
            ret = self.api.ReqUserLogin(login_field, 1)
            if ret == 0:
                login_type = "认证登录" if not self.use_anonymous_login else "匿名登录"
                futures_logger.info(f"登录请求已发送（{login_type}），返回值: {ret}")
                return True
            else:
                futures_logger.error(f"登录请求发送失败，返回值: {ret}")
                return False
        except Exception as e:
            futures_logger.error(f"登录异常: {e}", exc_info=True)
            return False

    def subscribe(self, symbols: Optional[List[str]] = None):
        """订阅行情
        Args:
            symbols: 要订阅的合约列表，如果为 None 则使用初始化时设置的列表
        """
        if not self.api:
            futures_logger.error("CTP API 未初始化，无法订阅")
            return False
        
        if not self.is_logged_in:
            futures_logger.warning("尚未登录成功，订阅请求将在登录成功后自动执行")
            # 更新订阅列表，等待登录成功后自动订阅
            if symbols:
                self.subscribe_symbols = symbols
            if self.spi:
                self.spi.subscribe_symbols = self.subscribe_symbols
            return False
        
        symbols_to_subscribe = symbols or self.subscribe_symbols
        if not symbols_to_subscribe:
            futures_logger.warning("订阅列表为空，跳过订阅")
            return False
        
        try:
            ret = self.api.SubscribeMarketData(symbols_to_subscribe)
            if ret == 0:
                futures_logger.info(f"订阅请求已发送: {symbols_to_subscribe}")
                return True
            else:
                futures_logger.error(f"订阅请求发送失败，返回值: {ret}")
                return False
        except Exception as e:
            futures_logger.error(f"订阅异常: {e}", exc_info=True)
            return False

    def get_api_version(self):
        """获取 API 版本"""
        if self.api:
            try:
                return self.api.GetApiVersion()
            except Exception as e:
                futures_logger.error(f"获取 API 版本异常: {e}")
                return None
        return None

    def close(self):
        """释放资源"""
        with self._lock:
            if self.api:
                try:
                    # pybind11 包装的析构函数会自动调用 Release
                    self.api = None
                except Exception as e:
                    futures_logger.error(f"释放 API 资源异常: {e}")
            self.is_connected = False
            self.is_logged_in = False
            futures_logger.info("CTP API 已释放")
