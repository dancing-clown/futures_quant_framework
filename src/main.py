# -*- coding: utf-8 -*-
"""期货行情框架项目入口文件
统一加载配置、初始化模块、调度启动
"""
import yaml
import asyncio
from pathlib import Path
from src.utils import futures_logger, MarketSourceError
from src.collector.async_collector import AsyncFuturesCollector
from src.processor.data_cleaner import DataCleaner
from src.storage.file_storage import FileStorage

CONFIG_FILE = Path(__file__).parent / "config" / "main_config.yaml"

def load_config() -> dict:
    """加载主配置文件"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        futures_logger.info(f"配置文件加载成功，项目名称：{config['project']['name']}")
        return config
    except Exception as e:
        futures_logger.critical(f"配置文件加载失败，错误：{str(e)}")
        raise SystemExit(1)

async def process_data_callback(data_list, cleaner, storage):
    """数据处理回调"""
    cleaned_data = cleaner.clean(data_list)
    if cleaned_data:
        storage.save(cleaned_data)

async def main_async():
    """异步主循环"""
    config = load_config()
    
    market_sources = config["market_sources"]
    collector = AsyncFuturesCollector(market_sources)
    cleaner = DataCleaner()
    storage = FileStorage()
    
    try:
        if collector.init_connections() and collector.subscribe_market():
            futures_logger.info("系统初始化完成，进入主运行循环...")
            await collector.run_forever(
                lambda d: process_data_callback(d, cleaner, storage)
            )
    except KeyboardInterrupt:
        futures_logger.info("用户手动终止程序")
    except Exception as e:
        futures_logger.error(f"运行异常：{e}", exc_info=True)
    finally:
        collector.close_connections()

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
