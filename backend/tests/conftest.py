# -*- coding: utf-8 -*-
"""
conftest.py — 告警引擎 / API 服务最小测试夹具
========================================================================
目标：测试集可独立运行——不依赖 InfluxDB / MQTT 真实服务。

做法：
    1. 把 backend 目录加入 sys.path，使测试可直接 import api_server / alert_engine；
    2. 规则库指向系统临时目录，避免污染开发目录下的 rules.db；
    3. 若本机未安装 flask-socketio / influxdb（最小验证环境常见情况），
       注入仅含被测代码所用接口的最小桩模块——被测逻辑（规则校验、阈值判定）
       均不触及这两个库的真实行为，桩只保证 import 不失败。
"""
import os
import sys
import tempfile
import types
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# 独立临时规则库路径（api_server 在 import 时读取该环境变量）
os.environ.setdefault(
    "RULES_DB_PATH",
    os.path.join(tempfile.gettempdir(), "iiot_test_rules.db"),
)


def _install_stub(module_name: str) -> None:
    """三方库缺失时注入最小桩模块；已安装则不动真实库。"""
    try:
        __import__(module_name)
        return
    except ImportError:
        pass

    if module_name == "flask_socketio":
        stub = types.ModuleType("flask_socketio")

        class SocketIO:  # noqa: 同名桩，仅覆盖被测代码用到的接口
            def __init__(self, app=None, **kwargs):
                self.app = app

            def emit(self, *args, **kwargs):
                pass

            def run(self, *args, **kwargs):
                pass

            def on(self, event):
                def decorator(fn):
                    return fn
                return decorator

        stub.SocketIO = SocketIO
        sys.modules[module_name] = stub

    elif module_name == "influxdb":
        stub = types.ModuleType("influxdb")

        class InfluxDBClient:  # noqa: 同名桩，所有网络操作显式失败
            def __init__(self, *args, **kwargs):
                pass

            def ping(self):
                raise ConnectionError("stub: 测试环境不连接 InfluxDB")

            def query(self, query, *args, **kwargs):
                raise ConnectionError("stub: 测试环境不连接 InfluxDB")

            def write_points(self, points, *args, **kwargs):
                pass

            def create_database(self, dbname):
                pass

        stub.InfluxDBClient = InfluxDBClient
        sys.modules[module_name] = stub


for _mod in ("flask_socketio", "influxdb"):
    _install_stub(_mod)
