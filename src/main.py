# -*- coding: utf-8 -*-
"""期货行情框架项目入口文件
统一加载配置、初始化模块、调度启动，所有业务逻辑均在各子模块中实现
"""
import yaml
from pathlib import Path
from src.utils import futures_logger, MarketSourceError
from src.collector.base_collector import BaseFuturesCollector
from src.collector.async_collector import AsyncFuturesCollector  # 后续实现后取消注释
# from src.collector.sync_collector import SyncFuturesCollector

# 配置文件路径
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

def init_collector(config: dict) -> BaseFuturesCollector:
    """初始化采集器（根据配置选择异步/同步模式）"""
    collect_mode = config["collect"]["mode"]
    market_sources = config["market_sources"]
    try:
        if collect_mode == "async":
            # 异步采集器（后续实现）
            collector = AsyncFuturesCollector(market_sources)
        elif collect_mode == "sync":
            # 同步采集器（后续实现）
            collector = None  # 先占位，后续实现
        else:
            raise MarketSourceError(f"不支持的采集模式：{collect_mode}，仅支持async/sync")
        futures_logger.info(f"采集器初始化成功，模式：{collect_mode}")
        return collector
    except Exception as e:
        futures_logger.critical(f"采集器初始化失败，错误：{str(e)}")
        raise SystemExit(1)

def main():
    """项目主函数"""
    # 1. 加载配置
    config = load_config()
    # 2. 初始化采集器（示例，后续可扩展：数据处理/存储调度）
    collector = init_collector(config)
    # 3. 采集数据（示例，后续可封装为循环/异步任务）
    with collector:
        while True:
            try:
                data = collector.collect_data()
                if data:
                    futures_logger.info(f"成功采集{len(data)}条行情数据，第一条：{data[0]}")
                    # 后续扩展：调用数据处理模块 -> 调用数据存储模块
                # 同步采集间隔（异步采集无需）
                # time.sleep(config["collect"]["interval"])
            except KeyboardInterrupt:
                futures_logger.info("用户手动终止程序")
                break
            except Exception as e:
                futures_logger.error(f"采集主循环异常，错误：{str(e)}", exc_info=True)
                continue

if __name__ == "__main__":
    main()