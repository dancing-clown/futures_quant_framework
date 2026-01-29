# -*- coding: utf-8 -*-
"""NSQ-DCE 行情接口封装（Linux only）

说明：
- `nsq-dce-net-api/` 项目本身仅支持 Linux 环境（依赖其 SDK / Rust Bridge）。
- 本仓库中的 `NsqMarketApi` 作为框架适配层：与 collector/processor 的接口保持一致。
- 当在非 Linux 环境启用该行情源时，直接抛出明确异常提示。

后续在 Linux 上可将这里扩展为：
- 通过本地共享内存/消息队列读取 `nsq-dce-net-api` 产出的行情
- 或通过其 bridge 产出的结构体/二进制数据进行解析
"""

from __future__ import annotations

import platform
from typing import Callable, Optional, Dict, Any

from src.utils import futures_logger, MarketSourceError


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
    ):
        self.config_path = config_path
        self.username = username
        self.password = password
        self.sdk_config_path = sdk_config_path
        self.log_path = log_path
        self.markets = markets

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
        """初始化连接并注册回调（Linux only）

        当前版本仅做平台校验与回调注册，实际接入逻辑需在 Linux 环境下补齐。
        """
        self._ensure_linux()
        self._callback = callback
        self.is_connected = True
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

