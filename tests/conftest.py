# -*- coding: utf-8 -*-
"""pytest 共享配置与 fixture
统一添加项目根目录到 sys.path，供所有测试模块导入 src
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
