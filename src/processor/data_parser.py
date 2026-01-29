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
        try:
            msg_type = raw_msg.get("type")
            obj = raw_msg.get("data")
            
            if not obj:
                return None
            
            if msg_type == "DCE_L1":
                return DataParser._parse_dce_l1(obj)
            elif msg_type == "CZCE_L1":
                return DataParser._parse_czce_l1(obj)
            elif msg_type == "CTP_TICK":
                # CTP 类型可能为 None（如果 ctp_pybind 未加载），但对象存在就可以解析
                return DataParser._parse_ctp_tick(obj)
            elif msg_type == "NSQ_DEPTH":
                return DataParser._parse_nsq_depth(obj)
            return None
        except Exception as e:
            from src.utils import futures_logger
            futures_logger.error(f"数据解析异常: {e}", exc_info=True)
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
    def _parse_ctp_tick(obj) -> Dict:
        """解析 CTP Tick
        注意：pybind11 绑定的字符串属性已经是 Python str 类型，不需要 decode
        """
        from src.utils import futures_logger
        
        try:
            # pybind11 返回的是 std::string，已经是 Python str，不需要 decode
            symbol = str(obj.InstrumentID).strip() if hasattr(obj, 'InstrumentID') and obj.InstrumentID else ""
            # CTP UpdateTime: HH:MM:SS, UpdateMillisec: 500
            time_str = str(obj.UpdateTime) if hasattr(obj, 'UpdateTime') and obj.UpdateTime else "00:00:00"
            date_str = str(obj.ActionDay) if hasattr(obj, 'ActionDay') and obj.ActionDay else ""  # 业务日期
            ms = int(obj.UpdateMillisec) if hasattr(obj, 'UpdateMillisec') else 0
            
            # 解析时间
            try:
                if date_str and time_str and len(date_str) == 8:
                    dt = datetime.datetime.strptime(f"{date_str} {time_str}.{ms:03d}", "%Y%m%d %H:%M:%S.%f")
                else:
                    # 如果时间信息不完整，使用当前时间
                    dt = datetime.datetime.now()
            except (ValueError, AttributeError) as e:
                # 时间解析失败，使用当前时间
                futures_logger.warning(f"时间解析失败，使用当前时间: {e}")
                dt = datetime.datetime.now()
            
            exchange = str(obj.ExchangeID).strip() if hasattr(obj, 'ExchangeID') and obj.ExchangeID else ""
            
            result = {
                "symbol": symbol,
                "exchange": exchange,
                "last_price": float(obj.LastPrice) if hasattr(obj, 'LastPrice') and obj.LastPrice else 0.0,
                "volume": int(obj.Volume) if hasattr(obj, 'Volume') and obj.Volume else 0,
                "open_interest": float(obj.OpenInterest) if hasattr(obj, 'OpenInterest') and obj.OpenInterest else 0.0,
                "datetime": dt,
                "bid_price_1": float(obj.BidPrice1) if hasattr(obj, 'BidPrice1') and obj.BidPrice1 else 0.0,
                "bid_volume_1": int(obj.BidVolume1) if hasattr(obj, 'BidVolume1') and obj.BidVolume1 else 0,
                "ask_price_1": float(obj.AskPrice1) if hasattr(obj, 'AskPrice1') and obj.AskPrice1 else 0.0,
                "ask_volume_1": int(obj.AskVolume1) if hasattr(obj, 'AskVolume1') and obj.AskVolume1 else 0,
                "open_price": float(obj.OpenPrice) if hasattr(obj, 'OpenPrice') and obj.OpenPrice else 0.0,
                "high_price": float(obj.HighestPrice) if hasattr(obj, 'HighestPrice') and obj.HighestPrice else 0.0,
                "low_price": float(obj.LowestPrice) if hasattr(obj, 'LowestPrice') and obj.LowestPrice else 0.0,
                "pre_close": float(obj.PreClosePrice) if hasattr(obj, 'PreClosePrice') and obj.PreClosePrice else 0.0,
                "pre_settlement": float(obj.PreSettlementPrice) if hasattr(obj, 'PreSettlementPrice') and obj.PreSettlementPrice else 0.0
            }
            return result
        except Exception as e:
            futures_logger.error(f"解析 CTP Tick 异常: {e}", exc_info=True)
            return None

    @staticmethod
    def _parse_nsq_depth(obj) -> Optional[Dict]:
        """解析 NSQ Depth（来自 nsq-dce-net-api bridge/SDK）

        期望 obj 为 dict（或具备同名属性的对象），字段参考 `nsq-dce-net-api/src/bridge/nsq_bridge.cpp`：
        - InstrumentID, ExchangeID, LastPrice, TradeVolume, OpenInterest
        - BidPrice[0], BidVolume[0], AskPrice[0], AskVolume[0]
        - OpenPrice, HighestPrice, LowestPrice, PreClosePrice, PreSettlementPrice
        - ActionDay(YYYYMMDD), UpdateTime(HH:MM:SS 或 HHMMSSmmm 等)
        """
        from src.utils import futures_logger

        def _get(name: str, default=None):
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)

        try:
            symbol = _get("InstrumentID", "") or _get("instrument_id", "")
            exchange = _get("ExchangeID", "") or _get("exchange_id", "")
            last_price = float(_get("LastPrice", 0.0) or 0.0)
            volume = int(_get("TradeVolume", 0) or 0)
            open_interest = float(_get("OpenInterest", 0.0) or 0.0)

            # bid/ask 1（兼容数组或独立字段）
            bid_price_arr = _get("BidPrice", None)
            bid_vol_arr = _get("BidVolume", None)
            ask_price_arr = _get("AskPrice", None)
            ask_vol_arr = _get("AskVolume", None)

            def _arr0(v, default=0.0):
                if v is None:
                    return default
                try:
                    return v[0]
                except Exception:
                    return default

            bid_price_1 = float(_arr0(bid_price_arr, _get("BidPrice1", 0.0) or 0.0) or 0.0)
            bid_volume_1 = int(_arr0(bid_vol_arr, _get("BidVolume1", 0) or 0) or 0)
            ask_price_1 = float(_arr0(ask_price_arr, _get("AskPrice1", 0.0) or 0.0) or 0.0)
            ask_volume_1 = int(_arr0(ask_vol_arr, _get("AskVolume1", 0) or 0) or 0)

            open_price = float(_get("OpenPrice", 0.0) or 0.0)
            high_price = float(_get("HighestPrice", 0.0) or 0.0)
            low_price = float(_get("LowestPrice", 0.0) or 0.0)
            pre_close = float(_get("PreClosePrice", 0.0) or 0.0)
            pre_settlement = float(_get("PreSettlementPrice", 0.0) or 0.0)

            # 时间
            action_day = _get("ActionDay", "") or _get("TradingDay", "")
            update_time = _get("UpdateTime", "") or ""
            # normalize
            action_day = str(action_day).strip()
            update_time = str(update_time).strip()
            dt = datetime.datetime.now()
            try:
                if len(action_day) == 8 and ":" in update_time:
                    dt = datetime.datetime.strptime(f"{action_day} {update_time}", "%Y%m%d %H:%M:%S")
                elif len(action_day) == 8 and update_time.isdigit() and len(update_time) >= 6:
                    # e.g. HHMMSS or HHMMSSmmm
                    hh = update_time[0:2]
                    mm = update_time[2:4]
                    ss = update_time[4:6]
                    ms = update_time[6:9] if len(update_time) >= 9 else "000"
                    dt = datetime.datetime.strptime(f"{action_day} {hh}:{mm}:{ss}.{ms}", "%Y%m%d %H:%M:%S.%f")
            except Exception as e:
                futures_logger.warning(f"NSQ 时间解析失败，使用当前时间: {e}")
                dt = datetime.datetime.now()

            return {
                "symbol": str(symbol).strip(),
                "exchange": str(exchange).strip(),
                "last_price": last_price,
                "volume": volume,
                "open_interest": open_interest,
                "datetime": dt,
                "bid_price_1": bid_price_1,
                "bid_volume_1": bid_volume_1,
                "ask_price_1": ask_price_1,
                "ask_volume_1": ask_volume_1,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "pre_close": pre_close,
                "pre_settlement": pre_settlement,
            }
        except Exception as e:
            futures_logger.error(f"解析 NSQ Depth 异常: {e}", exc_info=True)
            return None
