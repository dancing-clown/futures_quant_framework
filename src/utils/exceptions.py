# -*- coding: utf-8 -*-
"""期货业务自定义异常模块
定义贴合期货行情业务的异常类型，方便问题定位和统一异常处理
"""

class FuturesBaseError(BaseException):
    """期货框架基础异常类，所有自定义异常均继承此类"""
    def __init__(self, message: str = "期货框架基础异常", *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.message = message

    def __str__(self):
        return f"[{self.__class__.__name__}]: {self.message}"

class MarketSourceError(FuturesBaseError):
    """行情源异常：如连接失败、接口返回错误、订阅失败等"""
    pass

class DataParseError(FuturesBaseError):
    """数据解析异常：如多源数据格式不统一、解析字段缺失等"""
    pass

class DataCleanError(FuturesBaseError):
    """数据清洗异常：如时间戳乱序、数值异常无法修复等"""
    pass

class StorageError(FuturesBaseError):
    """数据存储异常：如数据库连接失败、文件写入错误、Redis缓存失败等"""
    pass

class CollectError(FuturesBaseError):
    """行情采集异常：如采集超时、重试失败、网络断开等"""
    pass