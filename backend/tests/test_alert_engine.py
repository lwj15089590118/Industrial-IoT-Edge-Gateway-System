# -*- coding: utf-8 -*-
"""
test_alert_engine.py — 告警引擎阈值判定的类型守卫测试
========================================================================
背景（审查报告11 P1-1）：SQLite 动态类型允许字符串 min/max 入库，
旧实现 `value > max_v` 直接抛 TypeError 杀死告警引擎进程。
本组用例锁定修复后的行为：非法规则跳过、计数告警、绝不抛异常。
"""
import pytest

import alert_engine


@pytest.fixture(autouse=True)
def _reset_guard_state():
    """每个用例前后清空类型守卫的计数与去重状态，保证相互独立。"""
    alert_engine._invalid_rule_skip_count.clear()
    alert_engine._invalid_rule_reported.clear()
    yield
    alert_engine._invalid_rule_skip_count.clear()
    alert_engine._invalid_rule_reported.clear()


def test_check_rule_numeric_min_max():
    """正常数值规则：越上限 / 越下限 / 正常区间三种分支。"""
    rule = {"id": 1, "metric": "voltage", "min_value": 200.0,
            "max_value": 240.0, "level": "critical"}
    ok, msg = alert_engine.check_rule(rule, 250.0)
    assert ok and "超上限" in msg
    ok, msg = alert_engine.check_rule(rule, 190.0)
    assert ok and "低于下限" in msg
    ok, msg = alert_engine.check_rule(rule, 220.0)
    assert not ok and msg == ""


def test_check_rule_none_side_ignored():
    """None 表示对应方向不检查：仅上限规则在区间内不告警。"""
    rule = {"id": 2, "metric": "current", "min_value": None,
            "max_value": 15.0, "level": "warning"}
    assert alert_engine.check_rule(rule, 10.0) == (False, "")


@pytest.mark.parametrize("bad_field", ["min_value", "max_value"])
def test_check_rule_string_threshold_does_not_raise(bad_field):
    """字符串阈值（历史脏数据）：旧实现抛 TypeError，修复后跳过并返回不越限。"""
    rule = {"id": 3, "metric": "voltage", "max_value": None, "min_value": None,
            "level": "warning"}
    rule[bad_field] = "240"
    ok, msg = alert_engine.check_rule(rule, 250.0)
    assert ok is False and msg == ""


def test_check_rule_bool_threshold_skipped():
    """布尔阈值虽是 int 子类，但语义非法，应与脏数据同样处理。"""
    rule = {"id": 4, "metric": "voltage", "min_value": None,
            "max_value": True, "level": "warning"}
    ok, _ = alert_engine.check_rule(rule, 0.5)
    assert ok is False


def test_check_rule_int_threshold_valid():
    """整型阈值合法（SQLite 动态类型下 int/float 均可能出现）。"""
    rule = {"id": 5, "metric": "voltage", "min_value": None,
            "max_value": 240, "level": "warning"}
    ok, msg = alert_engine.check_rule(rule, 250.0)
    assert ok and "超上限" in msg


def test_invalid_rule_skip_counted():
    """非法规则每次评估都必须计数（计数告警），供运维观测脏规则频率。"""
    rule = {"id": 99, "metric": "voltage", "min_value": "200",
            "max_value": None, "level": "warning"}
    alert_engine.check_rule(rule, 220.0)
    first = alert_engine._invalid_rule_skip_count[99]
    assert first == 1
    alert_engine.check_rule(rule, 220.0)
    assert alert_engine._invalid_rule_skip_count[99] == first + 1


def test_valid_rule_not_counted_as_invalid():
    """合法规则不进入非法计数。"""
    rule = {"id": 6, "metric": "voltage", "min_value": 200.0,
            "max_value": 240.0, "level": "warning"}
    alert_engine.check_rule(rule, 220.0)
    assert 6 not in alert_engine._invalid_rule_skip_count
