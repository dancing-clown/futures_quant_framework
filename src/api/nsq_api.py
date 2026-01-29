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
from typing import Callable, Optional, Dict, Any

from src.utils import futures_logger, MarketSourceError

# 延迟导入 nsq_pybind（仅 Linux），避免在模块加载阶段写死路径
_nsq_pybind = None


def _get_nsq_pybind(pybind_path: Optional[str] = None):
    """按需加载 nsq_pybind（Linux only），支持配置项和环境变量 NSQ_PYBIND_PATH。"""
    global _nsq_pybind
    if _nsq_pybind is not None:
        return _nsq_pybind

    if platform.system().lower() != "linux":
        raise MarketSourceError(
            "nsq-dce-net-api / nsq_pybind 仅支持 Linux 环境；当前系统为 "
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
    ):
        self.config_path = config_path
        self.username = username
        self.password = password
        self.sdk_config_path = sdk_config_path
        self.log_path = log_path
        self.markets = markets
        self.pybind_path = pybind_path

        self._callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self.is_connected: bool = False

    @staticmethod
    def _ensure_linux():
        if platform.system().lower() != "linux":
            raise MarketSourceError(
                "nsq-dce-net-api 行情源仅支持 Linux 环境运行；当前系统为 "
                f"{platform.system()}。如需编译/运行请切换到 Linux。"
            )

    def connect(self, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """初始化连接并注册回调（Linux only）。

        当前版本仅做平台校验与回调注册；若 nsq_pybind 可用则尝试导入并打调试日志。

        Args:
            callback: 行情数据回调，接收 {"type": "NSQ_DEPTH", "data": ...}。

        Returns:
            始终返回 True（stub 模式）。
        """
        self._ensure_linux()
        self._callback = callback
        self.is_connected = True

        # 若 nsq_pybind 已提供且配置了 pybind_path/环境变量，则尝试导入验证路径是否正确
        try:
            _get_nsq_pybind(self.pybind_path)
        except Exception as e:
            # 暂不强制依赖 nsq_pybind，只记录日志，方便在纯 Python stub 模式下运行
            futures_logger.warning(f"nsq_pybind 导入失败（当前仍使用 stub NSQ API）：{e}")

        futures_logger.info("NSQ API 已初始化（待接入实际行情读取逻辑）")
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
        """关闭连接"""
        self.is_connected = False
        self._callback = None
        futures_logger.info("NSQ API 已关闭")

