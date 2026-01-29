# -*- coding: utf-8 -*-
"""行情数据标准化解析模块
负责将各源原始数据转换为框架统一格式 (FUTURES_BASE_FIELDS)
"""
import datetime
import re
from typing import Dict, Optional
from src.api.zy_zmq_api import DCEL1_Quotation, CZCEL2_Quotation
from src.api.ctp_api import CThostFtdcDepthMarketDataField
from src.utils.exceptions import DataParseError

# 合约品种代码 -> 交易所（用于 CTP ExchangeID 为空时补全）
# 上期所 SHFE、大商所 DCE、郑商所 CZCE、上海能源 INE、广期所 GFEX
_SYMBOL_TO_EXCHANGE = {
    "cu": "SHFE", "al": "SHFE", "zn": "SHFE", "pb": "SHFE", "ni": "SHFE",
    "sn": "SHFE", "au": "SHFE", "ag": "SHFE", "rb": "SHFE", "hc": "SHFE",
    "ss": "SHFE", "bu": "SHFE", "ru": "SHFE", "br": "SHFE", "sp": "SHFE",
    "fu": "SHFE", "wr": "SHFE",
    "c": "DCE", "cs": "DCE", "a": "DCE", "b": "DCE", "m": "DCE", "y": "DCE",
    "p": "DCE", "fb": "DCE", "bb": "DCE", "jd": "DCE", "rr": "DCE", "lh": "DCE",
    "l": "DCE", "v": "DCE", "pp": "DCE", "jm": "DCE", "j": "DCE", "i": "DCE",
    "eg": "DCE", "eb": "DCE", "pg": "DCE",
    "sr": "CZCE", "cf": "CZCE", "wh": "CZCE", "pm": "CZCE", "ri": "CZCE",
    "lr": "CZCE", "jr": "CZCE", "rm": "CZCE", "rs": "CZCE", "oi": "CZCE",
    "cy": "CZCE", "ap": "CZCE", "cj": "CZCE", "pk": "CZCE", "zc": "CZCE",
    "ta": "CZCE", "ma": "CZCE", "fg": "CZCE", "sf": "CZCE", "sm": "CZCE",
    "ur": "CZCE", "sa": "CZCE", "pf": "CZCE", "px": "CZCE", "sh": "CZCE",
    "bc": "INE", "sc": "INE", "lu": "INE", "nr": "INE", "ec": "INE",
    "si": "GFEX", "lc": "GFEX", "ps": "GFEX",
}

# 框架统一基础字段定义
FUTURES_BASE_FIELDS = [
    "symbol", "exchange", "last_price", "volume", "open_interest",
    "datetime", "bid_price_1", "bid_volume_1", "ask_price_1", "ask_volume_1",
    "open_price", "high_price", "low_price", "pre_close", "pre_settlement"
]

def _infer_exchange_from_symbol(symbol: str) -> str:
    """根据合约代码推断交易所（仅当 CTP 等源未返回 ExchangeID 时用于补全）。

    Args:
        symbol: 合约代码，如 zn2603、y2605。

    Returns:
        交易所代码（SHFE/DCE/CZCE/INE/GFEX），无法推断时返回空字符串。
    """
    if not symbol or not symbol.strip():
        return ""
    s = symbol.strip().lower()
    # 去掉末尾数字，得到品种代码（如 zn2603 -> zn, y2605 -> y）
    prefix = re.sub(r"[0-9]+$", "", s)
    if not prefix:
        return ""
    return _SYMBOL_TO_EXCHANGE.get(prefix, "")


class DataParser:
    """行情数据解析器"""

    @staticmethod
    def parse_raw_data(raw_msg: Dict) -> Optional[Dict]:
        """解析原始数据，按 type 自动识别源并转换为统一格式。

        Args:
            raw_msg: 含 "type" 与 "data" 的原始消息字典。

        Returns:
            标准化行情字典（符合 FUTURES_BASE_FIELDS），未知类型或无数据时返回 None。

        Raises:
            DataParseError: 解析过程发生异常时抛出。
        """
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
            elif msg_type == "GFEX_L2":
                return DataParser._parse_gfex_l2(obj)
            return None
        except Exception as e:
            from src.utils import futures_logger
            futures_logger.error(f"数据解析异常: {e}", exc_info=True)
            raise DataParseError(f"数据解析失败: {e}") from e

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
        """解析 CTP Tick（pybind 绑定的字符串属性一般为 Python str，按需 decode）。"""
        from src.utils import futures_logger

        try:
            symbol = str(obj.InstrumentID).strip() if hasattr(obj, "InstrumentID") and obj.InstrumentID else ""
            time_str = str(obj.UpdateTime) if hasattr(obj, "UpdateTime") and obj.UpdateTime else "00:00:00"
            date_str = str(obj.ActionDay) if hasattr(obj, "ActionDay") and obj.ActionDay else ""
            ms = int(obj.UpdateMillisec) if hasattr(obj, "UpdateMillisec") else 0

            try:
                if date_str and time_str and len(date_str) == 8:
                    dt = datetime.datetime.strptime(
                        f"{date_str} {time_str}.{ms:03d}", "%Y%m%d %H:%M:%S.%f"
                    )
                else:
                    dt = datetime.datetime.now()
            except (ValueError, AttributeError) as e:
                futures_logger.warning(f"时间解析失败，使用当前时间: {e}")
                dt = datetime.datetime.now()

            exchange = str(obj.ExchangeID).strip() if hasattr(obj, "ExchangeID") and obj.ExchangeID else ""
            if not exchange and symbol:
                exchange = _infer_exchange_from_symbol(symbol)

            result = {
                "symbol": symbol,
                "exchange": exchange,
                "last_price": float(obj.LastPrice) if hasattr(obj, "LastPrice") and obj.LastPrice else 0.0,
                "volume": int(obj.Volume) if hasattr(obj, "Volume") and obj.Volume else 0,
                "open_interest": float(obj.OpenInterest) if hasattr(obj, "OpenInterest") and obj.OpenInterest else 0.0,
                "datetime": dt,
                "bid_price_1": float(obj.BidPrice1) if hasattr(obj, "BidPrice1") and obj.BidPrice1 else 0.0,
                "bid_volume_1": int(obj.BidVolume1) if hasattr(obj, "BidVolume1") and obj.BidVolume1 else 0,
                "ask_price_1": float(obj.AskPrice1) if hasattr(obj, "AskPrice1") and obj.AskPrice1 else 0.0,
                "ask_volume_1": int(obj.AskVolume1) if hasattr(obj, "AskVolume1") and obj.AskVolume1 else 0,
                "open_price": float(obj.OpenPrice) if hasattr(obj, "OpenPrice") and obj.OpenPrice else 0.0,
                "high_price": float(obj.HighestPrice) if hasattr(obj, "HighestPrice") and obj.HighestPrice else 0.0,
                "low_price": float(obj.LowestPrice) if hasattr(obj, "LowestPrice") and obj.LowestPrice else 0.0,
                "pre_close": float(obj.PreClosePrice) if hasattr(obj, "PreClosePrice") and obj.PreClosePrice else 0.0,
                "pre_settlement": float(obj.PreSettlementPrice) if hasattr(obj, "PreSettlementPrice") and obj.PreSettlementPrice else 0.0,
            }
            return result
        except Exception as e:
            # 解析失败时按数组打印 ActionDay 原始数据，便于排查
            try:
                action_day_val = getattr(obj, "ActionDay", None)
                if action_day_val is not None:
                    action_day_arr = list(bytes(action_day_val))
                    futures_logger.error(
                        "解析 CTP Tick 异常: %s；ActionDay(array): %s",
                        e,
                        action_day_arr,
                        exc_info=True,
                    )
                else:
                    futures_logger.error(f"解析 CTP Tick 异常: {e}；ActionDay: None", exc_info=True)
            except Exception as log_ex:
                futures_logger.error(
                    f"解析 CTP Tick 异常: {e}；打印 ActionDay 时再次异常: {log_ex}",
                    exc_info=True,
                )
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

    @staticmethod
    def _parse_gfex_l2(obj) -> Optional[Dict]:
        """解析 GFEX L2（来自 hs-future-gfex-api / ExaNIC，NanoGfexL2MdType 转成的 dict）"""
        from src.utils import futures_logger

        def _get(name: str, default=None):
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)

        try:
            symbol = (_get("contract_name") or "").strip()
            last_price = float(_get("last_price", 0.0) or 0.0)
            volume = int(_get("match_total_qty", 0) or 0)
            open_interest = float(_get("open_interest", 0) or 0)
            bid_price_1 = float(_get("bid1_px", 0.0) or 0.0)
            bid_volume_1 = int(_get("bid1_vol", 0) or 0)
            ask_price_1 = float(_get("ask1_px", 0.0) or 0.0)
            ask_volume_1 = int(_get("ask1_vol", 0) or 0)

            gen_time = (_get("gen_time") or "").strip()
            dt = datetime.datetime.now()
            try:
                if gen_time and len(gen_time) >= 8:
                    # 可能为 "HH:MM:SS" 或 "HH:MM:SS.mmm"，用今日日期
                    today = datetime.datetime.now().strftime("%Y%m%d")
                    if "." in gen_time:
                        dt = datetime.datetime.strptime(f"{today} {gen_time}", "%Y%m%d %H:%M:%S.%f")
                    else:
                        dt = datetime.datetime.strptime(f"{today} {gen_time}", "%Y%m%d %H:%M:%S")
            except Exception as e:
                futures_logger.warning(f"GFEX gen_time 解析失败，使用当前时间: {e}")

            return {
                "symbol": symbol,
                "exchange": "GFEX",
                "last_price": last_price,
                "volume": volume,
                "open_interest": open_interest,
                "datetime": dt,
                "bid_price_1": bid_price_1,
                "bid_volume_1": bid_volume_1,
                "ask_price_1": ask_price_1,
                "ask_volume_1": ask_volume_1,
                "open_price": 0.0,
                "high_price": 0.0,
                "low_price": 0.0,
                "pre_close": 0.0,
                "pre_settlement": 0.0,
            }
        except Exception as e:
            futures_logger.error(f"解析 GFEX L2 异常: {e}", exc_info=True)
            return None
