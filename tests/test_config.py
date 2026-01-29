# -*- coding: utf-8 -*-
"""配置加载单元测试
测试 main.py 中的 load_config 行为（通过临时 YAML 或 mock）
"""
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.main import load_config, CONFIG_FILE


class TestLoadConfig:
    """配置加载单元测试"""

    def test_load_config_with_default_path(self):
        """测试使用默认配置文件路径加载"""
        # 依赖项目内存在的 main_config.yaml
        if not CONFIG_FILE.exists():
            pytest.skip("默认配置文件不存在，跳过")
        config = load_config()
        assert "project" in config
        assert "name" in config["project"]
        assert "market_sources" in config

    def test_load_config_with_custom_yaml(self):
        """测试使用自定义 YAML 文件加载"""
        content = """
project:
  name: "测试项目"
  version: "0.0.1"
market_sources:
  ctp:
    enable: false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(content)
            tmp_path = Path(f.name)
        try:
            config = load_config(tmp_path)
            assert config["project"]["name"] == "测试项目"
            assert config["market_sources"]["ctp"]["enable"] is False
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_load_config_file_not_found(self):
        """测试配置文件不存在时抛出 SystemExit"""
        with pytest.raises(SystemExit):
            load_config(Path("/nonexistent/config.yaml"))
