# -*- coding: utf-8 -*-
"""
test_api_rules.py — /api/rules 阈值类型校验测试
========================================================================
背景（审查报告11 P1-1）：POST/PUT /api/rules 曾将 min_value/max_value
原样入库（SQLite 允许字符串），导致告警引擎数值比较时 TypeError 崩溃。
本组用例锁定 API 层校验：非数值阈值一律 400，数值/省略正常入库。
"""
import gc
import os
import sqlite3

import pytest

from api_server import RULES_DB_PATH, app, init_rules_db


def _reset_rules_db() -> None:
    """重建默认规则库：优先删文件重建；Windows 下残留句柄未释放时退化为清表重置
    （sqlite_sequence 一并清空，保证自增 id 从 1 开始，用例间完全独立）。"""
    try:
        if os.path.exists(RULES_DB_PATH):
            os.remove(RULES_DB_PATH)
    except PermissionError:
        conn = sqlite3.connect(RULES_DB_PATH)
        try:
            conn.execute("DELETE FROM alert_rules")
            conn.execute("DELETE FROM sqlite_sequence WHERE name='alert_rules'")
            conn.commit()
        finally:
            conn.close()
    init_rules_db()


@pytest.fixture()
def client():
    """每个用例使用全新的临时规则库 + Flask test client（不启动网络服务）。"""
    _reset_rules_db()
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client
    # teardown 不强制删除文件（连接句柄释放时机不定，见 _reset_rules_db），
    # 垃圾回收只是尽力而为，真正的隔离由下个用例 setup 时的重置保证
    gc.collect()


def _create(client, **overrides):
    body = {"metric": "temperature", "min_value": 0, "max_value": 60,
            "level": "warning"}
    body.update(overrides)
    return client.post("/api/rules", json=body)


def test_create_rule_numeric_ok(client):
    """数值阈值正常创建并可在列表中查到。"""
    resp = _create(client, metric="temperature", min_value=0.0, max_value=60.5)
    assert resp.status_code == 201
    payload = resp.get_json()
    assert payload["code"] == 0
    rules = client.get("/api/rules").get_json()["data"]
    rule = next(r for r in rules if r["metric"] == "temperature")
    assert rule["min_value"] == 0.0 and rule["max_value"] == 60.5


def test_create_rule_omit_thresholds_ok(client):
    """省略阈值（None 表示不检查）合法。"""
    resp = client.post("/api/rules", json={"metric": "temperature"})
    assert resp.status_code == 201


@pytest.mark.parametrize("bad_value", ["60", "", [60], {"v": 60}, True])
def test_create_rule_non_numeric_threshold_rejected(client, bad_value):
    """字符串/列表/字典/布尔等非数值阈值一律 400，不入库。"""
    for field in ("min_value", "max_value"):
        resp = _create(client, metric="pressure", **{field: bad_value})
        assert resp.status_code == 400, f"{field}={bad_value!r} 应被拒绝"
        assert resp.get_json()["code"] == 400
    # 确认脏数据未入库
    rules = client.get("/api/rules").get_json()["data"]
    assert all(r["metric"] != "pressure" for r in rules)


def test_update_rule_string_threshold_rejected_and_unchanged(client):
    """PUT 传字符串阈值返回 400，且原规则值保持不变。"""
    resp = client.put("/api/rules/1", json={"max_value": "240"})
    assert resp.status_code == 400
    rules = client.get("/api/rules").get_json()["data"]
    rule1 = next(r for r in rules if r["id"] == 1)
    assert rule1["max_value"] == 240.0  # 默认电压规则，未被脏值覆盖


def test_update_rule_numeric_ok(client):
    """数值阈值正常更新。"""
    resp = client.put("/api/rules/1", json={"max_value": 235.5})
    assert resp.status_code == 200
    rules = client.get("/api/rules").get_json()["data"]
    rule1 = next(r for r in rules if r["id"] == 1)
    assert rule1["max_value"] == 235.5


def test_update_rule_missing_id_404(client):
    """规则不存在返回 404。"""
    resp = client.put("/api/rules/9999", json={"max_value": 1})
    assert resp.status_code == 404
