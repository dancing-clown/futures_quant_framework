# -*- coding: utf-8 -*-
"""通用工具函数模块
封装期货行情开发高频使用的工具函数，如时间格式转换、合约代码解析、数据校验等
"""
import time
import datetime
from typing import Optional, Union
import pandas as pd
from .logger import futures_logger

def dt2timestamp(dt: Union[datetime.datetime, str], fmt: str = "%Y-%m-%d %H:%M:%S") -> int:
    """
    时间对象/字符串转毫秒级时间戳（期货行情通用时间戳格式）
    :param dt: 时间对象或字符串
    :param fmt: 时间字符串格式
    :return: 毫秒级时间戳
    """
    try:
        if isinstance(dt, str):
            dt = datetime.datetime.strptime(dt, fmt)
        # 转秒级再转毫秒级
        return int(time.mktime(dt.timetuple()) * 1000 + dt.microsecond // 1000)
    except Exception as e:
        futures_logger.error(f"时间转换失败，dt={dt}, fmt={fmt}, 错误：{str(e)}")
        raise

def timestamp2dt(ts: int, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    毫秒级时间戳转指定格式的时间字符串
    :param ts: 毫秒级时间戳
    :param fmt: 目标时间格式
    :return: 时间字符串
    """
    try:
        # 毫秒转秒
        ts_sec = ts // 1000
        return datetime.datetime.fromtimestamp(ts_sec).strftime(fmt)
    except Exception as e:
        futures_logger.error(f"时间戳转时间失败，ts={ts}, 错误：{str(e)}")
        raise

def parse_futures_code(code: str) -> Optional[dict]:
    """
    解析期货合约代码（如rb2405 -> 品种：rb，年份：24，月份：05）
    :param code: 期货合约代码
    :return: 解析结果字典，失败返回None
    """
    try:
        # 提取数字部分（年份+月份）
        num_part = ""
        symbol_part = ""
        for char in code:
            if char.isdigit():
                num_part += char
            else:
                symbol_part += char
        if len(num_part) != 4:
            futures_logger.warning(f"合约代码格式异常，无法解析：{code}")
            return None
        year = f"20{num_part[:2]}"
        month = num_part[2:]
        return {
            "symbol": symbol_part,  # 品种代码
            "year": year,           # 合约年份
            "month": month,         # 合约月份
            "full_code": code       # 完整合约代码
        }
    except Exception as e:
        futures_logger.error(f"解析合约代码失败，code={code}, 错误：{str(e)}")
        return None

def check_data_validity(data: dict, required_fields: list) -> bool:
    """
    校验行情数据字段完整性（多源数据解析后必检）
    :param data: 行情数据字典
    :param required_fields: 必选字段列表（如["code", "price", "volume", "timestamp"]）
    :return: 校验通过返回True，否则False
    """
    missing_fields = [f for f in required_fields if f not in data or data[f] is None]
    if missing_fields:
        futures_logger.warning(f"行情数据缺失必选字段：{missing_fields}，数据：{data}")
        return False
    return True

# 期货行情必选基础字段（所有行情源解析后均需包含）
FUTURES_BASE_FIELDS = [
    "exchange",  # 交易所（SHFE/INE/DCE/CZCE/GFEX）
    "code",      # 合约代码
    "price",     # 最新价
    "volume",    # 成交量
    "open_interest",  # 持仓量
    "bid1",      # 买一价
    "ask1",      # 卖一价
    "timestamp"  # 毫秒级时间戳
]