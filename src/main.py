# -*- coding: utf-8 -*-
"""期货行情框架项目入口文件
统一加载配置、初始化模块、调度启动

集成测试说明：
- 通过修改 src/config/main_config.yaml 来配置不同的行情源进行测试
- 支持 CTP、正瀛 ZMQ、广发等多种行情源
- 运行方式：
  - 从项目根目录运行: python3 src/main.py
  - 或使用模块方式: python3 -m src.main
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径，确保可以导入 src 模块
# 获取项目根目录（main.py 的父目录的父目录）
_project_root = Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import yaml
import asyncio
import argparse
import signal
from src.utils import futures_logger, MarketSourceError, DataCleanError, StorageError
from src.collector.async_collector import AsyncFuturesCollector
from src.processor.data_cleaner import DataCleaner
from src.storage.file_storage import FileStorage

CONFIG_FILE = Path(__file__).parent / "config" / "main_config.yaml"

# 全局采集器实例，用于信号处理
_collector_instance = None

def load_config(config_file: Path = None) -> dict:
    """加载主配置文件。

    Args:
        config_file: 配置文件路径，默认为 src/config/main_config.yaml。

    Returns:
        解析后的配置字典。

    Raises:
        SystemExit: 文件不存在或 YAML 解析失败时退出码 1。
    """
    config_path = config_file or CONFIG_FILE
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        futures_logger.info(f"配置文件加载成功: {config_path}")
        futures_logger.info(f"项目名称：{config['project']['name']}")
        return config
    except FileNotFoundError:
        futures_logger.critical(f"配置文件不存在: {config_path}")
        raise SystemExit(1)
    except Exception as e:
        futures_logger.critical(f"配置文件加载失败，错误：{str(e)}")
        raise SystemExit(1)

async def process_data_callback(data_list, cleaner, storage):
    """数据处理回调：清洗后写入存储。

    Args:
        data_list: 标准化行情列表。
        cleaner: DataCleaner 实例。
        storage: FileStorage 实例。
    """
    try:
        cleaned_data = cleaner.clean(data_list)
        if cleaned_data:
            futures_logger.debug(f"处理 {len(cleaned_data)} 条清洗后的数据")
            storage.save(cleaned_data)
    except DataCleanError as e:
        futures_logger.warning(f"数据清洗异常，跳过本批: {e}")
    except StorageError as e:
        futures_logger.error(f"存储写入失败: {e}", exc_info=True)
    except Exception as e:
        futures_logger.error(f"数据处理回调异常: {e}", exc_info=True)

async def main_async(config_file: Path = None) -> None:
    """异步主循环：加载配置、初始化采集/清洗/存储并进入分发循环。

    Args:
        config_file: 配置文件路径，用于集成测试时指定不同配置；默认使用 main_config.yaml。
    """
    global _collector_instance
    
    config = load_config(config_file)
    
    market_sources = config["market_sources"]
    
    # 显示启用的行情源
    enabled_sources = [name for name, source in market_sources.items() 
                      if source.get("enable", False)]
    futures_logger.info(f"启用的行情源: {', '.join(enabled_sources) if enabled_sources else '无'}")
    
    collector = AsyncFuturesCollector(market_sources, config.get("collect"))
    _collector_instance = collector  # 设置全局实例供信号处理器使用
    processor_config = config.get("processor", {})
    cleaner = DataCleaner(processor_config.get("clean", {}))
    storage_config = config.get("storage", {}).get("file", {})
    storage = FileStorage(base_path=storage_config.get("base_path", "data/market_data"))
    
    try:
        if collector.init_connections():
            futures_logger.info("连接初始化完成，等待登录和订阅...")
            # 等待一小段时间，让连接建立和登录完成（CTP 使用回调机制）
            await asyncio.sleep(2)
            
            # 再次尝试订阅（如果自动订阅失败）
            collector.subscribe_market()
            
            futures_logger.info("系统初始化完成，进入主运行循环...")
            futures_logger.info("按 Ctrl+C 退出程序")
            
            # 创建数据处理回调函数
            # 这个回调函数会被 dispatch_loop 定期调用，处理从队列中采集的数据
            async def data_callback(data_list):
                try:
                    futures_logger.debug(f"数据回调被调用，收到 {len(data_list)} 条数据")
                    await process_data_callback(data_list, cleaner, storage)
                except Exception as e:
                    futures_logger.error(f"数据回调处理异常: {e}", exc_info=True)
            
            # 启动主循环，这会启动 dispatch_loop 来定期从队列中取数据
            futures_logger.info("正在启动数据采集和分发循环...")
            await collector.run_forever(data_callback)
            futures_logger.info("数据采集和分发循环已退出")
        else:
            futures_logger.error("系统初始化失败，请检查配置和网络连接")
    except KeyboardInterrupt:
        futures_logger.info("用户手动终止程序")
    except Exception as e:
        futures_logger.error(f"运行异常：{e}", exc_info=True)
    finally:
        collector.close_connections()
        futures_logger.info("程序已退出，资源已释放")

def signal_handler(sig, frame) -> None:
    """信号处理器：收到 SIGINT/SIGTERM 时停止采集并关闭连接。"""
    global _collector_instance
    futures_logger.info("\n收到退出信号 (Ctrl+C)，正在关闭连接...")
    if _collector_instance:
        _collector_instance.stop()
        _collector_instance.close_connections()
    import sys
    sys.exit(0)

if __name__ == "__main__":
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    parser = argparse.ArgumentParser(description='期货行情多源整合框架 - 集成测试')
    parser.add_argument(
        '-c', '--config',
        type=str,
        help='配置文件路径（默认: src/config/main_config.yaml）'
    )
    
    args = parser.parse_args()
    config_file = Path(args.config) if args.config else None
    
    try:
        asyncio.run(main_async(config_file))
    except KeyboardInterrupt:
        pass
    except SystemExit:
        pass
