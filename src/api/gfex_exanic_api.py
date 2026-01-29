# -*- coding: utf-8 -*-
"""GFEX ExaNIC 行情接口（Python 层 + pybind 封装）

通过 exanic_pybind 调用 ExaNIC C SDK（pybind11 封装），本模块为 Python 侧封装：
连接、接收线程、L2 帧解析（NanoGfexL2MdType）与回调。

- 仅支持 Linux（依赖 /dev/exanic*、mmap、ioctl）
- 数据结构与 hs-future-gfex-api/src/bridge/gf_bridge.hpp 中的 NanoGfexL2MdType 一致（pack 1）
"""

from __future__ import annotations

import os
import sys
import platform
import struct
import threading
import time
from typing import Callable, Optional, Dict, Any

from src.utils import futures_logger, MarketSourceError

# 延迟导入 exanic_pybind，便于非 Linux 或未编译时给出明确错误
_exanic_pybind = None


def _ensure_linux() -> None:
    if platform.system().lower() != "linux":
        raise MarketSourceError(
            "GFEX ExaNIC 行情源仅支持 Linux；当前系统为 "
            f"{platform.system()}。请于 Linux 下使用 exanic_pybind。"
        )


def _get_exanic_pybind():
    """按需加载 exanic_pybind（仅 Linux）。"""
    global _exanic_pybind
    if _exanic_pybind is not None:
        return _exanic_pybind
    _ensure_linux()
    try:
        import exanic_pybind as m
        _exanic_pybind = m
        return m
    except ImportError as e:
        raise ImportError(
            "未找到 exanic_pybind。请在 Linux 下编译 extern_libs/exanic_pybind，"
            "并通过 GFEX 配置项 pybind_path 或环境变量 GFEX_EXANIC_PYBIND_PATH "
            "将 build 目录加入 PYTHONPATH。"
        ) from e


# --- 与 gf_bridge.hpp 一致的 L2 行情结构（pack 1），用 struct 解析 ---
_GFEX_L2_FMT = (
    "<"  # little-endian
    "I"   # flag
    "20s" # contract_name
    "d"   # last_price
    "II"  # last_match_qty, match_total_qty
    "d"   # turn_over
    "Ii"  # open_interest, open_interest_change
    "16s" # gen_time
    "dIdIdIdIdI"  # bid1_px, bid1_vol, ... bid5
    "dIdIdIdIdI"  # ask1_px, ask1_vol, ... ask5
    "iiiii"       # buy_imply_qty_1..5
    "iiiii"       # sell_imply_qty_1..5
)
NANO_GFEX_L2_SIZE = struct.calcsize(_GFEX_L2_FMT)


def _parse_nano_l2_raw(buf: bytes) -> Optional[Dict[str, Any]]:
    """将 exanic 收到的一帧原始字节解析为 NanoGfexL2MdType 对应的 dict"""
    if len(buf) < NANO_GFEX_L2_SIZE:
        return None
    try:
        t = struct.unpack(_GFEX_L2_FMT, buf[:NANO_GFEX_L2_SIZE])
        contract_name = t[1].decode("utf-8", errors="ignore").strip("\x00").strip()
        gen_time = t[8].decode("utf-8", errors="ignore").strip("\x00").strip()
        return {
            "flag": t[0],
            "contract_name": contract_name,
            "last_price": t[2],
            "last_match_qty": t[3],
            "match_total_qty": t[4],
            "turn_over": t[5],
            "open_interest": t[6],
            "open_interest_change": t[7],
            "gen_time": gen_time,
            "bid1_px": t[9], "bid1_vol": t[10], "bid2_px": t[11], "bid2_vol": t[12],
            "bid3_px": t[13], "bid3_vol": t[14], "bid4_px": t[15], "bid4_vol": t[16],
            "bid5_px": t[17], "bid5_vol": t[18],
            "ask1_px": t[19], "ask1_vol": t[20], "ask2_px": t[21], "ask2_vol": t[22],
            "ask3_px": t[23], "ask3_vol": t[24], "ask4_px": t[25], "ask4_vol": t[26],
            "ask5_px": t[27], "ask5_vol": t[28],
            "buy_imply_qty_1": t[29], "buy_imply_qty_2": t[30], "buy_imply_qty_3": t[31],
            "buy_imply_qty_4": t[32], "buy_imply_qty_5": t[33],
            "sell_imply_qty_1": t[34], "sell_imply_qty_2": t[35], "sell_imply_qty_3": t[36],
            "sell_imply_qty_4": t[37], "sell_imply_qty_5": t[38],
        }
    except Exception as e:
        futures_logger.debug(f"GFEX L2 解析异常: {e}")
        return None


class GfexExanicApi:
    """GFEX ExaNIC API（基于 exanic_pybind 调用 C SDK）"""

    def __init__(
        self,
        nic_name: str,
        port_number: int = 1,
        buffer_number: int = 0,
        pybind_path: Optional[str] = None,
    ):
        _ensure_linux()
        self.nic_name = nic_name
        self.port_number = port_number
        self.buffer_number = buffer_number
        self._pybind_path = pybind_path
        self._api = None  # exanic_pybind 模块
        self._nic_cap = None  # capsule
        self._rx_cap = None
        self._callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def _load_pybind(self):
        """按需将 pybind_path / 环境路径加入 sys.path 并加载 exanic_pybind。"""
        if self._api is not None:
            return self._api
        search_paths = []
        if self._pybind_path:
            search_paths.append(os.path.abspath(self._pybind_path))
        env_path = os.getenv("GFEX_EXANIC_PYBIND_PATH")
        if env_path:
            search_paths.append(os.path.abspath(env_path))
        for path in search_paths:
            if path and os.path.exists(path) and path not in sys.path:
                sys.path.insert(0, path)
                futures_logger.debug(f"已添加 GFEX ExaNIC Pybind 搜索路径: {path}")
        self._api = _get_exanic_pybind()
        return self._api

    def connect(self, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """打开网卡、申请 RX 缓冲区并启动接收线程，收到一帧即调用 callback(data_dict)。"""
        _ensure_linux()
        api = self._load_pybind()
        nic = api.acquire_handle(self.nic_name)
        if nic is None:
            msg = api.get_last_error() or "unknown"
            futures_logger.error(f"exanic_acquire_handle 失败: {msg}")
            return False
        self._nic_cap = nic
        rx = api.acquire_rx_buffer(nic, self.port_number, self.buffer_number)
        if rx is None:
            api.release_handle(nic)
            self._nic_cap = None
            msg = api.get_last_error() or "unknown"
            futures_logger.error(f"exanic_acquire_rx_buffer 失败: {msg}")
            return False
        self._rx_cap = rx
        self._callback = callback
        self._running = True
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()
        futures_logger.info("GFEX ExaNIC 已连接并启动接收线程")
        return True

    def _receive_loop(self) -> None:
        api = self._api
        rx = self._rx_cap
        if not api or rx is None:
            return
        while self._running:
            raw = api.receive_frame(rx, 2048)
            if not raw:
                time.sleep(0.0001)
                continue
            if len(raw) >= NANO_GFEX_L2_SIZE:
                data = _parse_nano_l2_raw(raw)
                if data and self._callback:
                    self._callback({"type": "GFEX_L2", "data": data})

    def close(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        api = self._api
        if api and self._rx_cap is not None:
            api.release_rx_buffer(self._rx_cap)
            self._rx_cap = None
        if api and self._nic_cap is not None:
            api.release_handle(self._nic_cap)
            self._nic_cap = None
        self._api = None
        self._callback = None
        futures_logger.info("GFEX ExaNIC 已关闭")
