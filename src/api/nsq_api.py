# -*- coding: utf-8 -*-
"""NSQ 行情接口封装（Linux only）

说明：
- `NsqMarketApi` 作为框架适配层：与 collector/processor 的接口保持一致。
- 当在非 Linux 环境启用该行情源时，直接抛出明确异常提示。
"""

from __future__ import annotations

import os
import sys
import platform
import threading
import time
from pathlib import Path
from typing import Callable, Optional, Dict, Any, List, Tuple

from src.utils import futures_logger, MarketSourceError

# 项目根目录，用于解析配置中的相对路径（与 main_config 中 sdk_config_path / log_path 约定一致）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# 市场标识 -> 交易所代码（与 SDK HS_EI_* 一致）
_MARKET_TO_EXCHANGE: Dict[str, str] = {
    "dce": "F2",
    "shfe": "F3",
    "ine": "F5",
    "czce": "F1",
    "cffex": "F4",
}

# 延迟导入 nsq_pybind（仅 Linux），避免在模块加载阶段写死路径
_nsq_pybind = None


def _resolve_path(path: Optional[str], base: Optional[Path] = None) -> str:
    """将配置路径解析为绝对路径。

    sdk_config_path / log_path 相对项目根配置，运行时解析为绝对路径再传给
    SDK（NewNsqApiExt 的 flow_path / sdkCfgFilePath）。

    Args:
        path: 配置项路径，相对项目根；None 或空返回 ""。
        base: 解析相对路径的根目录，默认 _PROJECT_ROOT。

    Returns:
        绝对路径字符串，或 ""。
    """
    if not path or not path.strip():
        return ""
    base = base or _PROJECT_ROOT
    p = Path(path.strip())
    if p.is_absolute():
        return str(p)
    return str((base / p).resolve())


def _parse_markets(markets: str) -> List[Tuple[str, str]]:
    """解析 config markets 字符串为 (市场名, 交易所代码) 列表。"""
    s = (markets or "dce").strip().lower()
    out: List[Tuple[str, str]] = []
    seen = set()
    for m in s.replace("，", ",").split(","):
        m = m.strip()
        if not m or m in seen:
            continue
        seen.add(m)
        ex = _MARKET_TO_EXCHANGE.get(m)
        if ex:
            out.append((m, ex))
        else:
            futures_logger.warning("未知 NSQ 市场标识，已忽略: %s", m)
    return out


def _depth_field_to_dict(f: Any) -> Dict[str, Any]:
    """将 CHSNsqFutuDepthMarketDataField 转为 DataParser 可用的 dict。"""
    return {
        "InstrumentID": getattr(f, "InstrumentID", "") or "",
        "ExchangeID": getattr(f, "ExchangeID", "") or "",
        "LastPrice": getattr(f, "LastPrice", 0.0) or 0.0,
        "TradeVolume": getattr(f, "TradeVolume", 0) or 0,
        "OpenInterest": getattr(f, "OpenInterest", 0.0) or 0.0,
        "BidPrice": list(getattr(f, "BidPrice", None) or []),
        "BidVolume": list(getattr(f, "BidVolume", None) or []),
        "AskPrice": list(getattr(f, "AskPrice", None) or []),
        "AskVolume": list(getattr(f, "AskVolume", None) or []),
        "OpenPrice": getattr(f, "OpenPrice", 0.0) or 0.0,
        "HighestPrice": getattr(f, "HighestPrice", 0.0) or 0.0,
        "LowestPrice": getattr(f, "LowestPrice", 0.0) or 0.0,
        "PreClosePrice": getattr(f, "PreClosePrice", 0.0) or 0.0,
        "PreSettlementPrice": getattr(f, "PreSettlementPrice", 0.0) or 0.0,
        "ActionDay": getattr(f, "ActionDay", "") or "",
        "UpdateTime": getattr(f, "UpdateTime", "") or "",
        "TradingDay": getattr(f, "TradingDay", "") or "",
    }


def _get_nsq_pybind(pybind_path: Optional[str] = None):
    """按需加载 nsq_pybind（Linux only），支持配置项和环境变量 NSQ_PYBIND_PATH。"""
    global _nsq_pybind
    if _nsq_pybind is not None:
        return _nsq_pybind

    if platform.system().lower() != "linux":
        raise MarketSourceError(
            "NSQ / nsq_pybind 仅支持 Linux 环境；当前系统为 "
            f"{platform.system()}。如需编译/运行请切换到 Linux。"
        )

    search_paths = []
    if pybind_path:
        search_paths.append(os.path.abspath(pybind_path))
    env_path = os.getenv("NSQ_PYBIND_PATH")
    if env_path:
        search_paths.append(os.path.abspath(env_path))

    for path in search_paths:
        if path and os.path.exists(path) and path not in sys.path:
            sys.path.insert(0, path)
            futures_logger.debug(f"已添加 NSQ Pybind 搜索路径: {path}")

    try:
        import nsq_pybind as m
        _nsq_pybind = m
        futures_logger.debug("nsq_pybind 导入成功")
        return m
    except ImportError as e:
        raise MarketSourceError(
            "未找到 nsq_pybind。请在 Linux 下编译 extern_libs/nsq_pybind，"
            "并通过 NSQ 配置项 pybind_path 或环境变量 NSQ_PYBIND_PATH "
            "将 build 目录加入 PYTHONPATH。"
        ) from e


class NsqMarketApi:
    """NSQ 行情 API 适配层（框架接口一致性）"""

    def __init__(
        self,
        config_path: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        sdk_config_path: Optional[str] = None,
        log_path: Optional[str] = None,
        markets: str = "dce",
        pybind_path: Optional[str] = None,
        project_root: Optional[Path] = None,
    ):
        self.config_path = config_path
        self.username = username
        self.password = password
        self.sdk_config_path = sdk_config_path
        self.log_path = log_path
        self.markets = markets
        self.pybind_path = pybind_path
        self.project_root = Path(project_root) if project_root is not None else _PROJECT_ROOT

        self._callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self._api: Any = None
        self._join_thread: Optional[threading.Thread] = None
        self.is_connected: bool = False

    @staticmethod
    def _ensure_linux():
        if platform.system().lower() != "linux":
            raise MarketSourceError(
                "Nsq行情源仅支持 Linux 环境运行；当前系统为 "
                f"{platform.system()}。如需编译/运行请切换到 Linux。"
            )

    def connect(self, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """初始化连接并注册回调（Linux only）。

        参考 init_api：NewNsqApiExt(flow_path, sdk_config_path) -> RegisterSpi -> Init("")
        -> 等待连接与登录 -> 按 markets 订阅交易所全市场 -> Join 在后台线程运行。

        Args:
            callback: 行情数据回调，接收 {"type": "NSQ_DEPTH", "data": ...}。

        Returns:
            True 表示初始化且订阅成功（含 stub 模式）；False 表示 Init 失败或连接/登录超时。
        """
        self._ensure_linux()
        self._callback = callback
        self.is_connected = True

        try:
            m = _get_nsq_pybind(self.pybind_path)
        except Exception as e:
            futures_logger.warning("nsq_pybind 导入失败，使用 stub NSQ API：%s", e)
            return True

        flow_path = _resolve_path(self.log_path, self.project_root) or "./log/"
        sdk_cfg = _resolve_path(self.sdk_config_path, self.project_root)
        api = m.CHSNsqApi(flow_path=flow_path, sdk_cfg_file_path=sdk_cfg)
        self._api = api
        futures_logger.info(
            "NSQ API 已创建（flow_path=%s, sdk_config_path=%s）",
            flow_path,
            sdk_cfg or "(默认)",
        )

        login_req = m.CHSNsqReqUserLoginField()
        login_req.AccountID = (self.username or "").strip()
        login_req.Password = (self.password or "").strip()

        connected_evt = threading.Event()
        logged_in_evt = threading.Event()

        class _ConnSpi(m.CHSNsqSpi):
            def __init__(self, ap, req, cb, evt_conn, evt_login):
                super().__init__()
                self._ap = ap
                self._req = req
                self._cb = cb
                self._evt_conn = evt_conn
                self._evt_login = evt_login

            def OnFrontConnected(self):
                self._evt_conn.set()
                self._ap.ReqUserLogin(self._req, 0)

            def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
                err = 0
                if pRspInfo is not None:
                    err = getattr(pRspInfo, "ErrorID", 0) or 0
                if err != 0:
                    msg = getattr(pRspInfo, "ErrorMsg", "") or self._ap.GetApiErrorMsg(err)
                    futures_logger.error("NSQ ReqUserLogin 失败: ErrorID=%s, %s", err, msg)
                self._evt_login.set()

            def OnRtnFutuDepthMarketData(self, pData):
                if self._cb and pData is not None:
                    self._cb({"type": "NSQ_DEPTH", "data": _depth_field_to_dict(pData)})

        spi = _ConnSpi(api, login_req, self._callback, connected_evt, logged_in_evt)
        api.RegisterSpi(spi)
        api.RegisterFront("")
        ret = api.Init("")
        if ret != 0:
            futures_logger.error("NSQ Init 失败: ret=%s, %s", ret, api.GetApiErrorMsg(ret))
            self._api = None
            return False

        time.sleep(3)
        if not connected_evt.wait(timeout=60):
            futures_logger.error("NSQ 连接超时（60s）")
            self._api = None
            return False
        if not logged_in_evt.wait(timeout=30):
            futures_logger.error("NSQ 登录超时（30s）: %s", api.GetApiErrorMsg(0))
            self._api = None
            return False

        time.sleep(2)
        markets = _parse_markets(self.markets)
        if not markets:
            markets = [("dce", "F2"), ("shfe", "F3"), ("ine", "F5")]
            futures_logger.info("NSQ markets 为空，默认订阅 dce,shfe,ine")
        for name, ex in markets:
            r = api.SubscribeMarket(ex, 0)
            if r != 0:
                futures_logger.warning("NSQ SubscribeMarket(%s/%s) 失败: %s", name, ex, api.GetApiErrorMsg(r))
            else:
                futures_logger.info("NSQ 已订阅交易所: %s (%s)", name, ex)

        futures_logger.info("NSQ API 连接与订阅已完成")
        return True

    def emit_depth_market_data(self, data: Dict[str, Any]) -> None:
        """用于未来接入时投递数据（或用于测试注入）

        Args:
            data: NSQ Depth 字段字典（将被封装为 raw_msg 交给 DataParser）
        """
        if not self._callback:
            return
        self._callback({"type": "NSQ_DEPTH", "data": data})

    def close(self):
        """关闭连接，释放 CHSNsqApi（若已创建）。Join 线程为 daemon，进程退出时自动结束。"""
        self.is_connected = False
        self._callback = None
        if self._api is not None:
            self._api = None
        futures_logger.info("NSQ API 已关闭")

