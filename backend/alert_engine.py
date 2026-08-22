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


def check_rule(rule: dict, value: float):
    """
    对单个规则执行阈值判定。
    返回 (是否越限, 告警消息)；min/max 为 None 表示对应方向不检查。
    """
    metric = rule["metric"]
    min_v, max_v = rule["min_value"], rule["max_value"]
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
        rules = load_rules()  # 每轮重新加载，前端改规则即时生效
        live_ids = {r["id"] for r in rules}

        # 规则被删除/停用时，清除其告警态（相当于自动"恢复"）
        for rid in list(active):
            if rid not in live_ids:
                log.info("规则 #%d 已删除/停用，告警态复位", rid)
                del active[rid]

        for rule in rules:
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
