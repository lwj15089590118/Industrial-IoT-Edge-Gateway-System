# -*- coding: utf-8 -*-
"""
alert_engine.py — 工业物联网边缘网关系统 · 告警引擎
========================================================================
职责：
    1. 周期性从 SQLite 加载告警规则（与 api_server.py 共用 rules.db，
       前端改规则后下一轮检测立即生效）；
    2. 查询 InfluxDB 最近一个采集窗口的测点均值作为判定输入；
    3. 执行阈值检测：电压超限（上下限）、电流超限（上限）、
       以及任意规则配置的 min/max 组合；
    4. 告警去抖（状态机）：同一规则持续越限只触发一次告警，
       必须先恢复到正常区间才允许再次触发，避免告警风暴；
    5. 触发后双通道输出：
       a) 写入 InfluxDB 告警表（measurement=alerts），供历史查询；
       b) 发布 MQTT 主题 factory/line1/alerts，由 api_server 转发 WebSocket 给前端。

依赖：influxdb paho-mqtt
运行：python alert_engine.py（需 api_server 已初始化 rules.db）
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# ---------------------------------------------------------------------------
# 运行配置（环境变量可覆盖）
# ---------------------------------------------------------------------------
INFLUX_HOST = os.getenv("INFLUX_HOST", "127.0.0.1")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB = os.getenv("INFLUX_DB", "factory_metrics")

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "factory/line1/alerts")

RULES_DB_PATH = os.getenv("RULES_DB_PATH",
                          os.path.join(os.path.dirname(__file__), "rules.db"))

CHECK_INTERVAL = 2   # 检测周期（秒）：对 1 秒级采集数据足够灵敏
LOOKBACK_WINDOW = 5  # 判定窗口（秒）：取最近 N 秒均值，过滤单点毛刺

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("alert-engine")

# ---------------------------------------------------------------------------
# 存储连接
# ---------------------------------------------------------------------------
influx = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)

_mqtt_client = None  # 惰性初始化的 MQTT 发布客户端


def get_mqtt_client() -> mqtt.Client:
    """获取（必要时建立）MQTT 客户端；失败时返回 None，降级为仅写库。"""
    global _mqtt_client
    if _mqtt_client is None:
        client = mqtt.Client(client_id="alert-engine")
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()  # 后台心跳线程，维持长连接
            _mqtt_client = client
            log.info("[MQTT] 告警发布通道已连接 %s:%d", MQTT_HOST, MQTT_PORT)
        except Exception as exc:
            log.warning("[MQTT] 告警发布通道连接失败（仅写库模式）: %s", exc)
            _mqtt_client = None
    return _mqtt_client


# ---------------------------------------------------------------------------
# 规则加载与判定
# ---------------------------------------------------------------------------
def load_rules() -> list:
    """从 SQLite 加载全部启用中的告警规则。"""
    if not os.path.exists(RULES_DB_PATH):
        log.warning("规则库 %s 不存在，请先启动 api_server.py 初始化", RULES_DB_PATH)
        return []
    with sqlite3.connect(RULES_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM alert_rules WHERE enabled=1 ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_metric_value(metric: str):
    """取某测点最近 LOOKBACK_WINDOW 秒的均值；无数据返回 None。"""
    sql = (
        f"SELECT mean(\"{metric}\") AS v FROM \"metrics\" "
        f"WHERE time > now() - {LOOKBACK_WINDOW}s"
    )
    try:
        result = influx.query(sql)
        points = list(result.get_points(measurement="metrics"))
        return points[0]["v"] if points and points[0]["v"] is not None else None
    except Exception as exc:
        log.error("InfluxDB 查询失败(%s): %s", metric, exc)
        return None


def _is_number(v) -> bool:
    """数值判定：bool 是 int 的子类，True/False 参与阈值比较会造成语义混乱，显式排除。"""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


# 类型守卫计数：rule_id -> 连续跳过次数（非法规则每次评估 +1，用于观测脏规则频率）
_invalid_rule_skip_count = {}
# 已首次告警过的非法规则集合：避免同一规则每 2 秒刷一条错误日志
_invalid_rule_reported = set()


def report_invalid_rule(rule: dict, bad_fields: list) -> None:
    """非法规则计数告警：首次发现打 error 日志，之后每累计 30 次再提醒一次，
    避免日志风暴同时保证脏规则可被观测到。"""
    rid = rule.get("id", "?")
    _invalid_rule_skip_count[rid] = _invalid_rule_skip_count.get(rid, 0) + 1
    count = _invalid_rule_skip_count[rid]
    if rid not in _invalid_rule_reported:
        _invalid_rule_reported.add(rid)
        log.error("规则 #%s 的 %s 含非数值 %r，已跳过该规则判定——请通过 /api/rules 修正",
                  rid, "/".join(bad_fields),
                  {f: rule.get(f) for f in bad_fields})
    elif count % 30 == 0:
        log.warning("规则 #%s 非法（%s 非数值），已累计跳过 %d 次判定",
                    rid, "/".join(bad_fields), count)


def check_rule(rule: dict, value: float):
    """
    对单个规则执行阈值判定。
    返回 (是否越限, 告警消息)；min/max 为 None 表示对应方向不检查。
    防御性类型守卫：SQLite 动态类型允许任意类型入库（历史脏数据/旧版本接口写入），
    若 min/max 非数值，`value > max_v` 会抛 TypeError 并杀死引擎进程——
    此处拦截：非法规则跳过判定并计数告警，绝不向外抛异常。
    """
    metric = rule["metric"]
    min_v, max_v = rule["min_value"], rule["max_value"]
    bad_fields = [name for name, v in (("min_value", min_v), ("max_value", max_v))
                  if v is not None and not _is_number(v)]
    if bad_fields:
        report_invalid_rule(rule, bad_fields)
        return False, ""
    if max_v is not None and value > max_v:
        return True, f"{metric} 超上限: 当前值 {value:.2f} > {max_v}"
    if min_v is not None and value < min_v:
        return True, f"{metric} 低于下限: 当前值 {value:.2f} < {min_v}"
    return False, ""


# ---------------------------------------------------------------------------
# 告警输出：写 InfluxDB 告警表 + MQTT 广播
# ---------------------------------------------------------------------------
def emit_alert(rule: dict, value: float, message: str) -> None:
    """告警双通道输出：时序库留存 + MQTT 实时广播。"""
    alert_event = {
        "rule_id": rule["id"],
        "metric": rule["metric"],
        "value": round(value, 3),
        "level": rule["level"],           # warning / critical
        "message": message,
        "fired_at": datetime.now().isoformat(timespec="seconds"),
    }
    # 1) 写入 InfluxDB 告警表（时间线保留 30 天，随保留策略过期）
    point = [{
        "measurement": "alerts",
        "time": int(time.time() * 1000),
        "tags": {"metric": rule["metric"], "level": rule["level"]},
        "fields": {
            "rule_id": int(rule["id"]),
            "value": float(value),
            "message": message,
        },
    }]
    try:
        influx.write_points(point, time_precision="ms")
    except Exception as exc:
        log.error("告警写入 InfluxDB 失败: %s", exc)

    # 2) MQTT 广播，api_server 订阅后经 WebSocket 推送给前端
    client = get_mqtt_client()
    if client is not None:
        try:
            client.publish(TOPIC_ALERTS, json.dumps(alert_event), qos=1)
        except Exception as exc:
            log.error("告警 MQTT 广播失败: %s", exc)

    log.warning("【%s】%s", rule["level"].upper(), message)


# ---------------------------------------------------------------------------
# 主循环：规则状态机（active 集合内的规则需先恢复才能再次触发）
# ---------------------------------------------------------------------------
def run_engine() -> None:
    active = {}  # rule_id -> 触发时的消息，用于判断"是否已处于告警态"
    log.info("告警引擎开始运行：检测周期 %ds，判定窗口 %ds", CHECK_INTERVAL,
             LOOKBACK_WINDOW)
    while True:
        try:
            rules = load_rules()  # 每轮重新加载，前端改规则即时生效
        except Exception as exc:
            # 规则库偶发异常（如 SQLite 锁冲突、文件被占用）不应让引擎线程死亡，
            # 记日志后跳过本轮，下一周期重试
            log.error("加载告警规则失败，跳过本轮检测: %s", exc)
            time.sleep(CHECK_INTERVAL)
            continue
        live_ids = {r["id"] for r in rules}

        # 规则被删除/停用时，清除其告警态（相当于自动"恢复"）
        for rid in list(active):
            if rid not in live_ids:
                log.info("规则 #%d 已删除/停用，告警态复位", rid)
                del active[rid]

        for rule in rules:
            try:
                # 单条规则判定全程异常隔离：任何一条规则出错（脏数据/类型问题）
                # 只跳过该规则，不允许杀死整个引擎进程
                metric = rule["metric"]
                value = fetch_metric_value(metric)
                if value is None:
                    continue  # 无数据（链路中断或测点名错误），跳过本轮

                violated, message = check_rule(rule, value)
                if violated and rule["id"] not in active:
                    # 正常 -> 越限：触发告警并进入告警态
                    emit_alert(rule, value, message)
                    active[rule["id"]] = message
                elif not violated and rule["id"] in active:
                    # 越限 -> 恢复：仅记日志并退出告警态，允许下次再触发
                    log.info("规则 #%d(%s) 已恢复正常区间，告警解除", rule["id"], metric)
                    del active[rule["id"]]
            except Exception as exc:
                log.error("规则 #%s 判定异常，已跳过: %s", rule.get("id", "?"), exc)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("告警引擎启动：InfluxDB=%s:%d/%s MQTT=%s:%d",
             INFLUX_HOST, INFLUX_PORT, INFLUX_DB, MQTT_HOST, MQTT_PORT)
    log.info("=" * 60)
    # 等待 InfluxDB 就绪（容器编排下其它服务可能还在启动）
    for _ in range(30):
        try:
            influx.ping()
            break
        except Exception:
            log.info("等待 InfluxDB 就绪...")
            time.sleep(2)
    run_engine()
