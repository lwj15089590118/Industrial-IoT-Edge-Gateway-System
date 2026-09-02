# -*- coding: utf-8 -*-
"""
api_server.py — 工业物联网边缘网关系统 · 后端 API 服务
========================================================================
职责：
    1. MQTT 订阅：消费网关上报的主题 factory/line1/metrics，
       解析后写入 InfluxDB 时序数据库，同时缓存最新快照；
    2. REST API：
       - GET  /api/health              健康检查
       - GET  /api/realtime            最新一帧实时数据
       - GET  /api/history             历史数据（按测点 + 时间范围）
       - GET  /api/alerts              告警记录列表
       - GET  /api/rules               告警规则列表
       - POST /api/rules               新增告警规则
       - PUT  /api/rules/<id>          修改告警规则
       - DELETE /api/rules/<id>        删除告警规则
    3. WebSocket（flask-socketio，与 REST 同端口 5000）：
       - 事件 realtime_data：每秒向前端推送最新一帧测点数据
       - 事件 new_alert：告警引擎检测到新告警时实时推送给前端
       （告警引擎通过 MQTT 主题 factory/line1/alerts 广播，本服务订阅后转发）

依赖：flask flask-cors flask-socketio paho-mqtt influxdb
运行：python api_server.py
"""

import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO
from influxdb import InfluxDBClient

# ---------------------------------------------------------------------------
# 运行配置（支持环境变量覆盖，便于 Docker 注入；默认值用于本地开发）
# ---------------------------------------------------------------------------
INFLUX_HOST = os.getenv("INFLUX_HOST", "127.0.0.1")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB = os.getenv("INFLUX_DB", "factory_metrics")

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_METRICS = os.getenv("TOPIC_METRICS", "factory/line1/metrics")  # 网关数据主题
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "factory/line1/alerts")     # 告警广播主题

RULES_DB_PATH = os.getenv("RULES_DB_PATH", os.path.join(os.path.dirname(__file__), "rules.db"))
API_PORT = int(os.getenv("API_PORT", "5000"))
# 默认仅监听本机回环地址（安全默认值）；容器部署经 API_HOST=0.0.0.0 覆盖，
# 使 compose 内其它服务与端口映射可达。
API_HOST = os.getenv("API_HOST", "127.0.0.1")
# CORS 白名单：环境变量 CORS_ORIGINS，逗号分隔来源（安全默认值：仅放行本机前端开发服务器）。
# 设为 "*" 可显式放开所有来源，仅限本地调试，生产环境应配置具体的前端站点地址。
CORS_ORIGINS = [o.strip() for o in os.getenv(
    "CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",") if o.strip()]

# 输入白名单（防注入）：
#   测点名：字母/数字/下划线，首字符不能是数字（power_factor 等合法名含下划线）
#   聚合窗口：InfluxQL time 字面量，如 10s / 30s / 5m / 1h / 1d
METRIC_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
WINDOW_RE = re.compile(r"^[1-9][0-9]*[smhdw]$")

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("api-server")

# ---------------------------------------------------------------------------
# Flask 应用与插件初始化
# ---------------------------------------------------------------------------
app = Flask(__name__)
if "*" in CORS_ORIGINS:
    # 显式放开所有来源（仅调试用）：保持旧行为
    CORS(app)
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")
else:
    # 默认白名单模式：仅允许 CORS_ORIGINS 中列出的前端站点跨域访问
    CORS(app, origins=CORS_ORIGINS)
    socketio = SocketIO(app, cors_allowed_origins=CORS_ORIGINS, async_mode="threading")

# 最新一帧数据快照（线程共享；dict 赋值原子性足够，无需加锁）
latest_snapshot = {"ts": None, "data": None}


# ---------------------------------------------------------------------------
# InfluxDB 连接与数据写入
# ---------------------------------------------------------------------------
influx = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)


def influx_ready() -> bool:
    """探测 InfluxDB 是否可用（用于健康检查接口）。"""
    try:
        influx.ping()
        return True
    except Exception:
        return False


def write_metrics(payload: dict) -> None:
    """把一帧网关数据写入 InfluxDB measurement=metrics。
    时间戳缺失/非法时跳过本帧（InfluxDB 以时间为索引，坏帧不能落库）。"""
    ts = payload.get("timestamp")
    if not isinstance(ts, (int, float)):
        log.warning("网关数据缺少合法 timestamp，本帧丢弃: %r", ts)
        return
    points = [{
        "measurement": "metrics",
        "time": ts,                            # 网关侧的 Unix 毫秒时间戳
        "tags": {"device_id": payload.get("device_id", "unknown")},
        "fields": {k: float(v) for k, v in payload.get("values", {}).items()
                   if isinstance(v, (int, float))},
    }]
    try:
        influx.write_points(points, time_precision="ms")
    except Exception as exc:
        log.error("写入 InfluxDB 失败: %s", exc)


# ---------------------------------------------------------------------------
# SQLite：告警规则表（结构化配置数据用关系库更合适，时序库只管测点）
# ---------------------------------------------------------------------------
def init_rules_db() -> None:
    """初始化规则库：不存在则建表并写入两条默认规则。"""
    with sqlite3.connect(RULES_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_rules (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                metric     TEXT    NOT NULL,   -- 测点名，如 voltage / current
                min_value  REAL,               -- 下限（NULL 表示不检查下限）
                max_value  REAL,               -- 上限（NULL 表示不检查上限）
                level      TEXT DEFAULT 'warning',  -- warning / critical
                enabled    INTEGER DEFAULT 1,  -- 1 启用 0 停用
                created_at TEXT
            )
        """)
        # 仅在空表时插入默认规则，避免重复
        count = conn.execute("SELECT COUNT(*) FROM alert_rules").fetchone()[0]
        if count == 0:
            now = datetime.now().isoformat(timespec="seconds")
            conn.executemany(
                "INSERT INTO alert_rules(metric, min_value, max_value, level, enabled, created_at)"
                " VALUES (?, ?, ?, ?, 1, ?)",
                [
                    ("voltage", 200.0, 240.0, "critical", now),  # 电压安全区间
                    ("current", None, 15.0, "warning", now),     # 电流上限告警
                ],
            )
            log.info("已初始化默认告警规则：电压 200~240V / 电流 <15A")


def db() -> sqlite3.Connection:
    """取一个打开的规则库连接（row_factory 让查询结果可用列名取值）。"""
    conn = sqlite3.connect(RULES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _valid_threshold(v) -> bool:
    """min/max 阈值合法性：仅允许数值或 None（省略）。
    布尔值虽是 int 子类，但 true/false 入库会造成语义混乱，一并拒绝。
    背景告警引擎会用该值做 `value > max_v` 数值比较，字符串入库会在
    引擎侧抛 TypeError（见 alert_engine.check_rule 的类型守卫），必须在此拦截。"""
    return v is None or (isinstance(v, (int, float)) and not isinstance(v, bool))


def _valid_enabled(v) -> bool:
    """enabled 开关合法性：仅接受 bool（true/false）或整数 0/1，其余拒绝（400）。
    旧实现 int(body.get("enabled", 1)) 直接强转：字符串 "yes" 抛 ValueError、
    显式 null 抛 TypeError（均为裸 500）、浮点 1.5 被静默截断入库——
    与 min/max 的 _valid_threshold 同一风格：类型不对直接拒绝，不做强转。"""
    if isinstance(v, bool):
        return True
    return isinstance(v, int) and v in (0, 1)


def _parse_unix_ts(raw, default):
    """把 start/end 查询参数解析为有限浮点数（Unix 秒）。
    缺省（未传）返回 default；非数字或 NaN/±Inf 返回 None，由调用方统一 400。
    必须在此拦截浮点边缘值：NaN 使一切比较为 False，会绕过 end<=start 检查，
    随后 int(nan) 抛 ValueError、int(inf) 抛 OverflowError（均为裸 500）。"""
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


# ---------------------------------------------------------------------------
# MQTT 订阅：消费网关数据与告警广播，并转发给 WebSocket
# ---------------------------------------------------------------------------
def on_mqtt_connect(client, userdata, flags, rc):
    """连接成功回调：订阅数据与告警两个主题。"""
    if rc == 0:
        log.info("[MQTT] 已连接 Broker %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe([(TOPIC_METRICS, 1), (TOPIC_ALERTS, 1)])
        log.info("[MQTT] 已订阅主题: %s, %s", TOPIC_METRICS, TOPIC_ALERTS)
    else:
        log.warning("[MQTT] 连接失败，code=%d，5 秒后重试", rc)


def on_mqtt_message(client, userdata, msg):
    """消息分发：metrics 主题落库并广播；alerts 主题仅广播。"""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log.warning("[MQTT] 报文解析失败: %s", exc)
        return

    if msg.topic == TOPIC_METRICS:
        # 1) 缓存最新快照 2) 写 InfluxDB 3) WebSocket 推给前端
        latest_snapshot["ts"] = time.time()
        latest_snapshot["data"] = payload
        write_metrics(payload)
        socketio.emit("realtime_data", payload)
    elif msg.topic == TOPIC_ALERTS:
        # 告警引擎产生的告警事件，直接转发给前端告警列表
        log.info("[MQTT] 收到告警广播: %s", payload.get("message"))
        socketio.emit("new_alert", payload)


def start_mqtt_subscriber() -> None:
    """后台线程：运行 MQTT 订阅循环（断线由 paho 自动重连）。"""
    client = mqtt.Client(client_id="api-server-subscriber")
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message
    client.reconnect_delay_set(min_delay=2, max_delay=30)
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception as exc:
            log.error("[MQTT] 连接异常: %s，5 秒后重试", exc)
            time.sleep(5)


# ---------------------------------------------------------------------------
# REST API：健康检查 / 实时数据
# ---------------------------------------------------------------------------
@app.route("/api/health")
def api_health():
    """健康检查：报告各依赖组件状态。"""
    return jsonify({
        "status": "ok",
        "influxdb": "up" if influx_ready() else "down",
        "mqtt": "subscribed",
        "latest_data_age": (time.time() - latest_snapshot["ts"])
                           if latest_snapshot["ts"] else None,
    })


@app.route("/api/realtime")
def api_realtime():
    """返回最新一帧网关数据（内存缓存，响应极快）。"""
    if latest_snapshot["data"] is None:
        return jsonify({"code": 404, "msg": "尚未收到任何网关数据"}), 404
    return jsonify({"code": 0, "data": latest_snapshot["data"]})


# ---------------------------------------------------------------------------
# REST API：历史数据查询（按测点 + 时间范围）
# ---------------------------------------------------------------------------
@app.route("/api/history")
def api_history():
    """
    查询历史数据。
    参数：
        metric  测点名（必填），如 voltage
        start   起始时间（可选）ISO 或 Unix 秒，默认 1 小时前
        end     截止时间（可选），默认当前
        window  聚合窗口（可选）如 30s / 5m，默认 10s，防止大范围查询拖垮前端
    """
    metric = request.args.get("metric", "voltage")
    if not METRIC_RE.match(metric):
        # 防注入：测点名仅允许字母/数字/下划线（旧 isalnum 校验会错杀 power_factor
        # 这类带下划线的合法测点，却又放行会生成非法查询的 "*"，一并修正）
        return jsonify({"code": 400, "msg": "非法的 metric 参数（仅限字母/数字/下划线）"}), 400
    window = request.args.get("window", "10s")
    if not WINDOW_RE.match(window):
        # 防注入：窗口值直接拼入 InfluxQL，必须严格匹配 time 字面量格式
        return jsonify({"code": 400, "msg": "非法的 window 参数（示例：10s/30s/5m/1h）"}), 400

    now = time.time()
    start = _parse_unix_ts(request.args.get("start"), now - 3600)
    end = _parse_unix_ts(request.args.get("end"), now)
    if start is None or end is None:
        # 非数字 / NaN / ±Inf 统一 400（nan 曾绕过 end<=start 检查后 int(nan) 裸 500）
        return jsonify({"code": 400, "msg": "start/end 需为有限数值（Unix 时间戳）"}), 400
    if end <= start:
        return jsonify({"code": 400, "msg": "end 必须大于 start"}), 400

    sql = (
        f"SELECT mean(\"{metric}\") AS \"{metric}\" "
        f"FROM \"metrics\" WHERE time >= {int(start)}s AND time <= {int(end)}s "
        f"GROUP BY time({window}) fill(null) ORDER BY time ASC"
    )
    try:
        result = influx.query(sql)
    except Exception as exc:
        return jsonify({"code": 500, "msg": f"InfluxDB 查询失败: {exc}"}), 500

    points = [
        {"time": p["time"], "value": p[metric]}
        for p in result.get_points(measurement="metrics")
    ]
    return jsonify({
        "code": 0,
        "metric": metric,
        "window": window,
        "count": len(points),
        "data": points,
    })


# ---------------------------------------------------------------------------
# REST API：告警记录（存于 InfluxDB measurement=alerts）
# ---------------------------------------------------------------------------
@app.route("/api/alerts")
def api_alerts():
    """查询最近的告警记录，默认最近 1 小时，最多 200 条。"""
    now = time.time()
    start = _parse_unix_ts(request.args.get("start"), now - 3600)
    if start is None:
        # 非数字 / NaN / ±Inf 统一 400（inf/nan 曾使 int(start) 抛
        # OverflowError/ValueError 裸 500；非数字曾静默回退默认值，现一并收口）
        return jsonify({"code": 400, "msg": "start 需为有限数值（Unix 时间戳）"}), 400
    sql = (
        f"SELECT * FROM \"alerts\" WHERE time >= {int(start)}s "
        f"ORDER BY time DESC LIMIT 200"
    )
    try:
        result = influx.query(sql)
    except Exception as exc:
        return jsonify({"code": 500, "msg": f"InfluxDB 查询失败: {exc}"}), 500
    rows = [{"time": p["time"], **{k: v for k, v in p.items() if k != "time"}}
            for p in result.get_points(measurement="alerts")]
    return jsonify({"code": 0, "count": len(rows), "data": rows})


# ---------------------------------------------------------------------------
# REST API：告警规则配置（CRUD，存于 SQLite）
# ---------------------------------------------------------------------------
@app.route("/api/rules", methods=["GET"])
def list_rules():
    """列出全部告警规则。"""
    with db() as conn:
        rows = conn.execute("SELECT * FROM alert_rules ORDER BY id").fetchall()
    return jsonify({"code": 0, "data": [dict(r) for r in rows]})


@app.route("/api/rules", methods=["POST"])
def create_rule():
    """新增告警规则。请求体示例：
    {"metric": "temperature", "min_value": 0, "max_value": 60, "level": "warning"}
    """
    body = request.get_json(silent=True) or {}
    metric = body.get("metric")
    if not metric or not METRIC_RE.match(str(metric)):
        return jsonify({"code": 400, "msg": "metric 必填且仅限字母/数字/下划线"}), 400
    level = body.get("level", "warning")
    if level not in ("warning", "critical"):
        return jsonify({"code": 400, "msg": "level 仅支持 warning/critical"}), 400
    if not (_valid_threshold(body.get("min_value"))
            and _valid_threshold(body.get("max_value"))):
        # 阈值必须为数值或省略：字符串等非数值入库会导致告警引擎比较时崩溃
        return jsonify({"code": 400, "msg": "min_value/max_value 必须为数值或省略"}), 400
    enabled = body.get("enabled", 1)
    if not _valid_enabled(enabled):
        # 仅接受 bool 或 0/1 整数（显式传 null 也拒绝），不做 int() 强转
        return jsonify({"code": 400, "msg": "enabled 仅接受 bool 或 0/1 整数"}), 400
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO alert_rules(metric, min_value, max_value, level, enabled, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (metric, body.get("min_value"), body.get("max_value"),
             level, 1 if enabled else 0,
             datetime.now().isoformat(timespec="seconds")),
        )
        new_id = cur.lastrowid
    log.info("新增告警规则 #%d: %s", new_id, body)
    return jsonify({"code": 0, "data": {"id": new_id}}), 201


@app.route("/api/rules/<int:rule_id>", methods=["PUT"])
def update_rule(rule_id):
    """修改告警规则（字段可选，未传的字段保持不变）。
    metric/level 与新增接口同样校验——规则值会进入 InfluxQL 查询与判定逻辑，
    放行非法值等于向后端埋注入点。"""
    body = request.get_json(silent=True) or {}
    new_metric = body.get("metric", None)
    if new_metric is not None and not METRIC_RE.match(str(new_metric)):
        return jsonify({"code": 400, "msg": "metric 仅限字母/数字/下划线"}), 400
    new_level = body.get("level", None)
    if new_level is not None and new_level not in ("warning", "critical"):
        return jsonify({"code": 400, "msg": "level 仅支持 warning/critical"}), 400
    for field in ("min_value", "max_value"):
        if field in body and not _valid_threshold(body[field]):
            # 仅校验显式传入的阈值字段（未传字段保持原值）
            return jsonify({"code": 400, "msg": f"{field} 必须为数值或省略"}), 400
    if "enabled" in body and not _valid_enabled(body["enabled"]):
        # 显式传入的 enabled 同样严格校验（含 null），未传字段保持原值
        return jsonify({"code": 400, "msg": "enabled 仅接受 bool 或 0/1 整数"}), 400
    with db() as conn:
        row = conn.execute("SELECT * FROM alert_rules WHERE id=?", (rule_id,)).fetchone()
        if row is None:
            return jsonify({"code": 404, "msg": "规则不存在"}), 404
        conn.execute(
            "UPDATE alert_rules SET metric=?, min_value=?, max_value=?, level=?, enabled=?"
            " WHERE id=?",
            (
                str(new_metric) if new_metric is not None else row["metric"],
                body.get("min_value", row["min_value"]),
                body.get("max_value", row["max_value"]),
                new_level if new_level is not None else row["level"],
                1 if body.get("enabled", row["enabled"]) else 0,
                rule_id,
            ),
        )
    return jsonify({"code": 0, "msg": "已更新"})


@app.route("/api/rules/<int:rule_id>", methods=["DELETE"])
def delete_rule(rule_id):
    """删除告警规则。"""
    with db() as conn:
        cur = conn.execute("DELETE FROM alert_rules WHERE id=?", (rule_id,))
        if cur.rowcount == 0:
            return jsonify({"code": 404, "msg": "规则不存在"}), 404
    return jsonify({"code": 0, "msg": "已删除"})


# ---------------------------------------------------------------------------
# WebSocket 事件（客户端连接/断开日志）
# ---------------------------------------------------------------------------
@socketio.on("connect")
def on_ws_connect():
    log.info("[WS] 前端已连接: %s", request.sid)


@socketio.on("disconnect")
def on_ws_disconnect():
    log.info("[WS] 前端已断开: %s", request.sid)


# ---------------------------------------------------------------------------
# 程序入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("后端 API 服务启动：REST+WebSocket 端口 %d", API_PORT)
    log.info("InfluxDB=%s:%d/%s  MQTT=%s:%d", INFLUX_HOST, INFLUX_PORT, INFLUX_DB,
             MQTT_HOST, MQTT_PORT)
    log.info("=" * 60)

    init_rules_db()
    # 兜底：若 InfluxDB 尚未建库（如首启竞态），这里再尝试创建一次
    try:
        influx.create_database(INFLUX_DB)
    except Exception as exc:
        log.warning("InfluxDB 建库检查失败（可忽略，若容器已初始化）: %s", exc)

    # MQTT 订阅线程 + SocketIO 服务（阻塞运行）
    threading.Thread(target=start_mqtt_subscriber, daemon=True).start()
    # host 默认 127.0.0.1（容器部署经 API_HOST=0.0.0.0 覆盖）；
    # allow_unsafe_werkzeug=True 是 flask-socketio 在 Werkzeug 开发服务器上的必需参数，
    # 开发服务器仅适合演示/内网基线，生产应换 gunicorn+gevent 并前置 Nginx 反代（见 README 安全说明）。
    socketio.run(app, host=API_HOST, port=API_PORT, debug=False,
                 allow_unsafe_werkzeug=True)
