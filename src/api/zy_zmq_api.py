# -*- coding: utf-8 -*-
"""正瀛 ZMQ 行情接口封装
实现基于 ZMQ SUB 模式的大商所、郑商所行情接收
"""
import ctypes
import zmq
import asyncio
from typing import Optional, Callable
from src.utils import futures_logger

# 常量定义
LEVEL_FIVE = 5

# --- 大商所 (DCE) 结构体定义 ---
class DCE_BuySellLevelInfo3(ctypes.Structure):
    _fields_ = [
        ("Price", ctypes.c_double),
        ("Volume", ctypes.c_uint64),
        ("ImplyQty", ctypes.c_uint64),
    ]

class DCEL1_Quotation(ctypes.Structure):
    _fields_ = [
        ("LocalTimeStamp", ctypes.c_int),
        ("QuotationFlag", ctypes.c_char * 4),
        ("TradeDate", ctypes.c_int),
        ("Time", ctypes.c_int),
        ("Symbol", ctypes.c_char * 130),
        ("RoutineNo", ctypes.c_uint64),
        ("SecurityName", ctypes.c_char * 180),
        ("PreClosePrice", ctypes.c_double),
        ("PreSettlePrice", ctypes.c_double),
        ("PreTotalPosition", ctypes.c_uint64),
        ("OpenPrice", ctypes.c_double),
        ("PriceUpLimit", ctypes.c_double),
        ("PriceDownLimit", ctypes.c_double),
        ("LastPrice", ctypes.c_double),
        ("AveragePrice", ctypes.c_double),
        ("HighPrice", ctypes.c_double),
        ("LowPrice", ctypes.c_double),
        ("LifeHigh", ctypes.c_double),
        ("LifeLow", ctypes.c_double),
        ("LastMatchQty", ctypes.c_uint64),
        ("TotalVolume", ctypes.c_uint64),
        ("TotalAmount", ctypes.c_double),
        ("TotalPosition", ctypes.c_uint64),
        ("InterestChg", ctypes.c_int64),
        ("BuyPrice01", ctypes.c_double),
        ("BuyVolume01", ctypes.c_uint64),
        ("BidImplyQty01", ctypes.c_uint64),
        ("SellPrice01", ctypes.c_double),
        ("SellVolume01", ctypes.c_uint64),
        ("AskImplyQty01", ctypes.c_uint64),
        ("ClosePrice", ctypes.c_double),
        ("SettlePrice", ctypes.c_double),
        ("BatchNo", ctypes.c_uint64),
    ]

class DCEL2_LevelQuotation(ctypes.Structure):
    _fields_ = [
        ("LocalTimeStamp", ctypes.c_int),
        ("QuotationFlag", ctypes.c_char * 4),
        ("TradeDate", ctypes.c_int),
        ("Time", ctypes.c_int),
        ("Symbol", ctypes.c_char * 130),
        ("RoutineNo", ctypes.c_uint64),
        ("MBLQuotBuyNum", ctypes.c_uint),
        ("BuyLevel", DCE_BuySellLevelInfo3 * LEVEL_FIVE),
        ("MBLQuotSellNum", ctypes.c_uint),
        ("SellLevel", DCE_BuySellLevelInfo3 * LEVEL_FIVE),
        ("BatchNo", ctypes.c_uint64),
    ]

# --- 郑商所 (CZCE) 结构体定义 ---
class CZCE_BuySellLevelInfo(ctypes.Structure):
    _fields_ = [
        ("Price", ctypes.c_int),
        ("Volume", ctypes.c_int),
        ("TotalOrderNo", ctypes.c_int),
    ]

class CZCEL2_Quotation(ctypes.Structure):
    _fields_ = [
        ("LocalTimeStamp", ctypes.c_int),
        ("QuotationFlag", ctypes.c_char * 4),
        ("TradeDate", ctypes.c_uint),
        ("Symbol", ctypes.c_char * 40),
        ("Time", ctypes.c_longlong),
        ("PriceSize", ctypes.c_int),
        ("OpenPrice", ctypes.c_int),
        ("LastPrice", ctypes.c_int),
        ("AveragePrice", ctypes.c_int),
        ("HighPrice", ctypes.c_int),
        ("LowPrice", ctypes.c_int),
        ("LifeHigh", ctypes.c_int),
        ("LifeLow", ctypes.c_int),
        ("TotalVolume", ctypes.c_int),
        ("TotalAmount", ctypes.c_longlong),
        ("TotalPosition", ctypes.c_int),
        ("SettlePrice", ctypes.c_int),
        ("TotalBuyOrderVolume", ctypes.c_int),
        ("WtAvgBuyPrice", ctypes.c_int),
        ("TotalSellOrderVolume", ctypes.c_int),
        ("WtAvgSellPrice", ctypes.c_int),
        ("DeriveBidPrice", ctypes.c_int),
        ("DeriveAskPrice", ctypes.c_int),
        ("DeriveBidLot", ctypes.c_int),
        ("DeriveAskLot", ctypes.c_int),
    ]

class CZCEL2_LevelQuotation(ctypes.Structure):
    _fields_ = [
        ("LocalTimeStamp", ctypes.c_int),
        ("QuotationFlag", ctypes.c_char * 4),
        ("TradeDate", ctypes.c_uint),
        ("Symbol", ctypes.c_char * 40),
        ("Time", ctypes.c_longlong),
        ("PriceSize", ctypes.c_int),
        ("BuyLevel", CZCE_BuySellLevelInfo * LEVEL_FIVE),
        ("SellLevel", CZCE_BuySellLevelInfo * LEVEL_FIVE),
    ]

class ZYZmqApi:
    """正瀛 ZMQ 行情接口类"""
    def __init__(self, dce_address: str = "", czce_address: str = ""):
        self.dce_address = dce_address
        self.czce_address = czce_address
        self.context = zmq.Context()
        self.dce_sub = None
        self.czce_sub = None
        self.is_running = False

    def connect(self):
        """连接 ZMQ 地址"""
        try:
            if self.dce_address:
                self.dce_sub = self.context.socket(zmq.SUB)
                self.dce_sub.connect(self.dce_address)
                self.dce_sub.setsockopt_string(zmq.SUBSCRIBE, "")
                futures_logger.info(f"已连接大商所 ZMQ: {self.dce_address}")
            
            if self.czce_address:
                self.czce_sub = self.context.socket(zmq.SUB)
                self.czce_sub.connect(self.czce_address)
                self.czce_sub.setsockopt_string(zmq.SUBSCRIBE, "")
                futures_logger.info(f"已连接郑商所 ZMQ: {self.czce_address}")
            
            self.is_running = True
            return True
        except Exception as e:
            futures_logger.error(f"ZMQ 连接失败: {e}")
            return False

    def close(self):
        """关闭连接"""
        self.is_running = False
        if self.dce_sub:
            self.dce_sub.close()
        if self.czce_sub:
            self.czce_sub.close()
        self.context.term()
        futures_logger.info("ZMQ 连接已关闭")

    async def start_receiving(self, callback: Callable):
        """异步开始接收数据"""
        if not self.is_running:
            futures_logger.error("API 未连接，无法接收数据")
            return

        tasks = []
        if self.dce_sub:
            tasks.append(self._receive_loop(self.dce_sub, "DCE", callback))
        if self.czce_sub:
            tasks.append(self._receive_loop(self.czce_sub, "CZCE", callback))
        
        await asyncio.gather(*tasks)

    async def _receive_loop(self, socket, exchange, callback):
        """内部接收循环"""
        futures_logger.info(f"开始接收 {exchange} 行情...")
        while self.is_running:
            try:
                # 非阻塞尝试接收
                if socket.poll(timeout=100):
                    data = socket.recv()
                    parsed_data = self._parse_raw_data(data, exchange)
                    if parsed_data:
                        callback(parsed_data)
                else:
                    await asyncio.sleep(0.01)
            except Exception as e:
                futures_logger.error(f"{exchange} 接收循环异常: {e}")
                await asyncio.sleep(1)

    def _parse_raw_data(self, data: bytes, exchange: str) -> Optional[dict]:
        """解析原始字节数据"""
        try:
            if exchange == "DCE":
                if len(data) == ctypes.sizeof(DCEL1_Quotation):
                    struct_obj = DCEL1_Quotation.from_buffer_copy(data)
                    return {"type": "DCE_L1", "data": struct_obj}
                elif len(data) == ctypes.sizeof(DCEL2_LevelQuotation):
                    struct_obj = DCEL2_LevelQuotation.from_buffer_copy(data)
                    return {"type": "DCE_L2", "data": struct_obj}
            elif exchange == "CZCE":
                if len(data) == ctypes.sizeof(CZCEL2_Quotation):
                    struct_obj = CZCEL2_Quotation.from_buffer_copy(data)
                    return {"type": "CZCE_L1", "data": struct_obj}
                elif len(data) == ctypes.sizeof(CZCEL2_LevelQuotation):
                    struct_obj = CZCEL2_LevelQuotation.from_buffer_copy(data)
                    return {"type": "CZCE_L2", "data": struct_obj}
            return None
        except Exception as e:
            futures_logger.error(f"解析 {exchange} 数据失败: {e}")
            return None
