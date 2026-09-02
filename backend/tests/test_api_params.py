# -*- coding: utf-8 -*-
"""
test_api_params.py — /api/history 与 /api/alerts 数值参数校验测试
========================================================================
背景（复审报告11 第二轮 P2-2，第一轮 P2-9 的残留一半）：
    start/end 曾只挡 ValueError——
    - /api/history?start=nan：nan 使 end<=start 恒为 False 绕过检查，
      int(nan) 抛 ValueError → 裸 500；
    - /api/alerts?start=inf：int(inf) 抛 OverflowError → 裸 500。
修复后：非数字 / NaN / ±Inf 统一 400，合法有限数值放行进入查询阶段
（alerts 的非数字 start 亦由"静默回退默认值"收口为 400）。
不依赖真实 InfluxDB：校验失败在查询前返回 400；放行用例以 monkeypatch
替换 api_server.influx 为桩，断言 200 证明合法参数未被误杀。
"""
import pytest

import api_server
from api_server import app, init_rules_db


class _FakeResult:
    """query 结果桩：无数据点。"""

    def get_points(self, measurement=None):
        return []


class _FakeInflux:
    """InfluxDB 客户端桩：查询成功返回空结果集。"""

    def query(self, sql, *args, **kwargs):
        return _FakeResult()


@pytest.fixture()
def client():
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


_ENDPOINT_FIELD = [
    ("/api/history", "start"),
    ("/api/history", "end"),
    ("/api/alerts", "start"),
]


@pytest.mark.parametrize("endpoint,field", _ENDPOINT_FIELD)
@pytest.mark.parametrize("bad", ["nan", "NaN", "inf", "-inf", "Infinity", "abc", ""])
def test_non_finite_timestamp_rejected(client, endpoint, field, bad):
    """NaN/±Inf/非数字/空串一律 400（修复前 nan/inf 为裸 500）。"""
    resp = client.get(f"{endpoint}?{field}={bad}")
    assert resp.status_code == 400, \
        f"{endpoint}?{field}={bad!r} 应 400，实得 {resp.status_code}"
    payload = resp.get_json()
    assert payload["code"] == 400


def test_history_valid_range_passes(client, monkeypatch):
    """合法有限数值通过校验进入查询阶段（不误杀）。"""
    monkeypatch.setattr(api_server, "influx", _FakeInflux())
    resp = client.get("/api/history?start=1756800000&end=1756803600")
    assert resp.status_code == 200
    assert resp.get_json()["code"] == 0


def test_alerts_valid_start_passes(client, monkeypatch):
    """alerts 合法 start 同样放行（不再静默回退默认值）。"""
    monkeypatch.setattr(api_server, "influx", _FakeInflux())
    resp = client.get("/api/alerts?start=1756800000")
    assert resp.status_code == 200
    assert resp.get_json()["code"] == 0


def test_history_end_not_after_start_rejected(client):
    """end<=start 仍 400（既有语义保持）。"""
    resp = client.get("/api/history?start=1756803600&end=1756800000")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == 400
