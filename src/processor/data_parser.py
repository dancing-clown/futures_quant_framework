# -*- coding: utf-8 -*-
"""行情数据标准化解析模块
负责将各源原始数据转换为框架统一格式 (FUTURES_BASE_FIELDS)
"""
import datetime
from typing import Dict, Optional
from src.api.zy_zmq_api import DCEL1_Quotation, CZCEL2_Quotation
from src.api.ctp_api import CThostFtdcDepthMarketDataField

# 框架统一基础字段定义
FUTURES_BASE_FIELDS = [
    "symbol", "exchange", "last_price", "volume", "open_interest",
    "datetime", "bid_price_1", "bid_volume_1", "ask_price_1", "ask_volume_1",
    "open_price", "high_price", "low_price", "pre_close", "pre_settlement"
]

class DataParser:
    """行情数据解析器"""
    
    @staticmethod
    def parse_raw_data(raw_msg: Dict) -> Optional[Dict]:
        """解析原始数据，自动识别源类型"""
        msg_type = raw_msg.get("type")
        obj = raw_msg.get("data")
        
        if msg_type == "DCE_L1":
            return DataParser._parse_dce_l1(obj)
        elif msg_type == "CZCE_L1":
            return DataParser._parse_czce_l1(obj)
        elif msg_type == "CTP_TICK":
            return DataParser._parse_ctp_tick(obj)
        return None

    @staticmethod
    def _parse_dce_l1(obj: DCEL1_Quotation) -> Dict:
        """解析大商所 L1"""
        symbol = obj.Symbol.decode('gbk').strip()
        time_str = str(obj.Time).zfill(9)
        date_str = str(obj.TradeDate)
        dt = datetime.datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S%f")
        
        return {
            "symbol": symbol,
            "exchange": "DCE",
            "last_price": obj.LastPrice,
            "volume": int(obj.TotalVolume),
            "open_interest": float(obj.TotalPosition),
            "datetime": dt,
            "bid_price_1": obj.BuyPrice01,
            "bid_volume_1": int(obj.BuyVolume01),
            "ask_price_1": obj.SellPrice01,
            "ask_volume_1": int(obj.SellVolume01),
            "open_price": obj.OpenPrice,
            "high_price": obj.HighPrice,
            "low_price": obj.LowPrice,
            "pre_close": obj.PreClosePrice,
            "pre_settlement": obj.PreSettlePrice
        }

    @staticmethod
    def _parse_czce_l1(obj: CZCEL2_Quotation) -> Dict:
        """解析郑商所 L1"""
        symbol = obj.Symbol.decode('gbk').strip()
        time_val = obj.Time // 1000
        time_str = str(time_val).zfill(9)
        date_str = str(obj.TradeDate)
        dt = datetime.datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S%f")
        scale = 10 ** obj.PriceSize
        
        return {
            "symbol": symbol,
            "exchange": "CZCE",
            "last_price": obj.LastPrice / scale,
            "volume": int(obj.TotalVolume),
            "open_interest": float(obj.TotalPosition),
            "datetime": dt,
            "bid_price_1": obj.DeriveBidPrice / scale if obj.DeriveBidPrice else 0.0,
            "bid_volume_1": int(obj.DeriveBidLot),
            "ask_price_1": obj.DeriveAskPrice / scale if obj.DeriveAskPrice else 0.0,
            "ask_volume_1": int(obj.DeriveAskLot),
            "open_price": obj.OpenPrice / scale,
            "high_price": obj.HighPrice / scale,
            "low_price": obj.LowPrice / scale,
            "pre_close": 0.0,
            "pre_settlement": obj.SettlePrice / scale
        }

    @staticmethod
    def _parse_ctp_tick(obj: CThostFtdcDepthMarketDataField) -> Dict:
        """解析 CTP Tick
        注意：pybind11 绑定的字符串属性已经是 Python str 类型，不需要 decode
        """
        # pybind11 返回的是 std::string，已经是 Python str，不需要 decode
        symbol = str(obj.InstrumentID).strip() if obj.InstrumentID else ""
        # CTP UpdateTime: HH:MM:SS, UpdateMillisec: 500
        time_str = str(obj.UpdateTime) if obj.UpdateTime else "00:00:00"
        date_str = str(obj.ActionDay) if obj.ActionDay else ""  # 业务日期
        ms = int(obj.UpdateMillisec) if hasattr(obj, 'UpdateMillisec') else 0
        
        # 解析时间
        try:
            if date_str and time_str:
                dt = datetime.datetime.strptime(f"{date_str} {time_str}.{ms:03d}", "%Y%m%d %H:%M:%S.%f")
            else:
                # 如果时间信息不完整，使用当前时间
                dt = datetime.datetime.now()
        except (ValueError, AttributeError) as e:
            # 时间解析失败，使用当前时间
            dt = datetime.datetime.now()
        
        exchange = str(obj.ExchangeID).strip() if obj.ExchangeID else ""
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "last_price": float(obj.LastPrice) if obj.LastPrice else 0.0,
            "volume": int(obj.Volume) if obj.Volume else 0,
            "open_interest": float(obj.OpenInterest) if obj.OpenInterest else 0.0,
            "datetime": dt,
            "bid_price_1": float(obj.BidPrice1) if obj.BidPrice1 else 0.0,
            "bid_volume_1": int(obj.BidVolume1) if obj.BidVolume1 else 0,
            "ask_price_1": float(obj.AskPrice1) if obj.AskPrice1 else 0.0,
            "ask_volume_1": int(obj.AskVolume1) if obj.AskVolume1 else 0,
            "open_price": float(obj.OpenPrice) if obj.OpenPrice else 0.0,
            "high_price": float(obj.HighestPrice) if obj.HighestPrice else 0.0,
            "low_price": float(obj.LowestPrice) if obj.LowestPrice else 0.0,
            "pre_close": float(obj.PreClosePrice) if obj.PreClosePrice else 0.0,
            "pre_settlement": float(obj.PreSettlementPrice) if obj.PreSettlementPrice else 0.0
        }
