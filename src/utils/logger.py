# -*- coding: utf-8 -*-
"""全局日志工具模块
封装logging，实现全项目统一的日志配置，支持控制台+文件输出、日志分割
"""
import os
import logging
from logging.handlers import RotatingFileHandler
import yaml
from pathlib import Path

# 读取配置文件
CONFIG_PATH = Path(__file__).parent.parent / "config" / "main_config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# 确保日志目录存在（路径相对于项目根目录）
# 获取项目根目录（src/utils 的父目录的父目录）
_project_root = Path(__file__).parent.parent.parent
LOG_FILE_PATH = _project_root / CONFIG["logger"]["file_path"]
LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_futures_logger(name: str = __name__) -> logging.Logger:
    """
    获取期货框架全局日志器
    :param name: 日志器名称，通常传__name__即可
    :return: 配置好的logging.Logger实例
    """
    # 初始化日志器
    logger = logging.getLogger(name)
    logger.setLevel(CONFIG["logger"]["level"])
    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 定义日志格式
    formatter = logging.Formatter(CONFIG["logger"]["format"])

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器（带日志分割）
    file_handler = RotatingFileHandler(
        filename=LOG_FILE_PATH,
        maxBytes=CONFIG["logger"]["max_bytes"],
        backupCount=CONFIG["logger"]["backup_count"],
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

# 全局日志器实例（全项目可直接导入使用）
futures_logger = get_futures_logger("futures_quant_framework")