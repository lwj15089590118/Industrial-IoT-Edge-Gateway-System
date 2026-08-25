# API 接口文档

> 服务：backend/api_server.py（Flask）
> 基础地址：`http://<host>:5000`
> 协议：REST（HTTP/1.1）+ WebSocket（Socket.IO，与 REST 同端口）
> 内容类型：`application/json; charset=utf-8`

---

## 1. 通用约定

### 1.1 响应结构

所有 REST 接口返回统一信封结构：

```json
{
  "code": 0,          // 0=成功；非 0 见错误码表
  "msg": "ok",        // 错误描述（成功时可省略）
  "data": { }         // 业务数据（错误时无此字段）
}
```

### 1.2 错误码

| code | HTTP 状态 | 含义 |
|------|-----------|------|
| 0 | 200/201 | 成功 |
| 400 | 400 | 参数错误（如非法 metric、时间戳格式错误） |
| 404 | 404 | 资源不存在（规则不存在 / 尚无实时数据） |
| 500 | 500 | 服务端错误（InfluxDB 不可用等） |

### 1.3 跨域

服务端已启用 CORS（`allow_origins: *`），前端开发服务器（localhost:3000）可直接访问。

---

## 2. 实时数据接口

### 2.1 GET /api/health — 健康检查

探活接口，报告自身及依赖组件状态。

**请求示例**

```bash
curl http://localhost:5000/api/health
```

**响应示例**

```json
{
  "code": 0,
  "status": "ok",
  "influxdb": "up",
  "mqtt": "subscribed",
  "latest_data_age": 0.42
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| influxdb | string | InfluxDB 连通性：up / down |
| mqtt | string | MQTT 订阅状态 |
| latest_data_age | float | 距最近一帧数据的秒数（null 表示未收到过数据） |

### 2.2 GET /api/realtime — 最新一帧实时数据

返回内存缓存的网关最新上报（约 1 秒内），响应极快，适合轮询兜底。

**请求示例**

```bash
curl http://localhost:5000/api/realtime
```

**响应示例**

```json
{
  "code": 0,
  "data": {
    "device_id": "edge-gateway-01",
    "timestamp": 1761244800123,
    "values": {
      "voltage": 220.3, "current": 10.05, "power_factor": 0.95,
      "temperature": 35.6, "active_power": 2200.0, "frequency": 50.0,
      "reactive_power": 700.0, "apparent_power": 2310.0,
      "energy_total": 5000.0, "status_word": 5.0
    },
    "units": { "voltage": "V", "current": "A", "temperature": "°C" },
    "quality": "good"
  }
}
```

**错误**：尚未收到任何数据时返回 404 `{code:404, msg:"尚未收到任何网关数据"}`。

---

## 3. 历史数据接口

### 3.1 GET /api/history — 按测点与时间范围查询

**Query 参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| metric | string | 是 | voltage | 测点名，仅限字母数字（防注入）：voltage/current/power_factor/temperature/active_power/frequency/reactive_power/apparent_power/energy_total/status_word |
| start | int(Unix秒) | 否 | 1 小时前 | 起始时间 |
| end | int(Unix秒) | 否 | 当前时间 | 截止时间 |
| window | string | 否 | 10s | InfluxQL 聚合窗口，如 10s / 30s / 5m |

**请求示例**

```bash
# 查询最近 15 分钟电压，10 秒聚合
curl "http://localhost:5000/api/history?metric=voltage&window=10s&start=$(date -d '-15 min' +%s)"
```

**响应示例**

```json
{
  "code": 0,
  "metric": "voltage",
  "window": "10s",
  "count": 2,
  "data": [
    { "time": "2026-08-22T08:00:00Z", "value": 220.31 },
    { "time": "2026-08-22T08:00:10Z", "value": 219.85 }
  ]
}
```

> 说明：无数据的窗口 `value` 为 null（fill(null)），前端需跳过绘制。

### 3.2 GET /api/alerts — 告警记录查询

**Query 参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| start | int(Unix秒) | 否 | 1 小时前 | 起始时间 |

**请求示例**

```bash
curl "http://localhost:5000/api/alerts?start=1761241200"
```

**响应示例**

```json
{
  "code": 0,
  "count": 1,
  "data": [
    {
      "time": "2026-08-22T08:05:12Z",
      "metric": "voltage",
      "level": "critical",
      "rule_id": 1,
      "value": 196.2,
      "message": "voltage 低于下限: 当前值 196.20 < 200"
    }
  ]
}
```

> 按时间倒序，最多返回 200 条。

---

## 4. 告警规则配置接口

规则存储于 SQLite（`backend/rules.db`），字段结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 规则 ID（自增主键） |
| metric | string | 监控测点名 |
| min_value | float/null | 下限（null 不检查） |
| max_value | float/null | 上限（null 不检查） |
| level | string | warning / critical |
| enabled | int | 1 启用 / 0 停用 |
| created_at | string | 创建时间 ISO 格式 |

### 4.1 GET /api/rules — 规则列表

```bash
curl http://localhost:5000/api/rules
```

```json
{
  "code": 0,
  "data": [
    { "id": 1, "metric": "voltage", "min_value": 200.0, "max_value": 240.0,
      "level": "critical", "enabled": 1, "created_at": "2026-08-22T09:00:00" },
    { "id": 2, "metric": "current", "min_value": null, "max_value": 15.0,
      "level": "warning", "enabled": 1, "created_at": "2026-08-22T09:00:00" }
  ]
}
```

### 4.2 POST /api/rules — 新增规则

**请求体**

```bash
curl -X POST http://localhost:5000/api/rules \
  -H "Content-Type: application/json" \
  -d '{"metric":"temperature","min_value":0,"max_value":60,"level":"warning"}'
```

| 字段 | 必填 | 校验规则 |
|------|------|----------|
| metric | 是 | 仅限字母/数字/下划线，且首字符不能为数字（如 power_factor） |
| level | 否 | 默认 warning，仅支持 warning/critical |
| min_value / max_value | 否 | 数值或省略（null 表示不检查） |

**响应** `201`

```json
{ "code": 0, "data": { "id": 3 } }
```

### 4.3 PUT /api/rules/{id} — 修改规则

未传字段保持原值。

```bash
curl -X PUT http://localhost:5000/api/rules/1 \
  -H "Content-Type: application/json" \
  -d '{"max_value": 235.0}'
```

**响应**

```json
{ "code": 0, "msg": "已更新" }
```

### 4.4 DELETE /api/rules/{id} — 删除规则

```bash
curl -X DELETE http://localhost:5000/api/rules/3
```

**响应**

```json
{ "code": 0, "msg": "已删除" }
```

> 规则变更无需重启：告警引擎每 2 秒重新加载规则表，改动即时生效。

---

## 5. WebSocket 接口（Socket.IO）

连接地址：`ws://<host>:5000`（Socket.IO 协议，路径默认 `/socket.io/`，支持 WebSocket 与 polling 降级）。

**前端接入示例**

```javascript
import { io } from "socket.io-client";
const socket = io("http://localhost:5000", {
  transports: ["websocket", "polling"],
});
socket.on("realtime_data", (frame) => console.log(frame));
socket.on("new_alert", (alert) => console.log(alert));
```

### 5.1 事件 realtime_data（服务端 → 客户端）

网关每秒一帧，载荷与 `GET /api/realtime` 的 `data` 完全一致（见 §2.2）。

### 5.2 事件 new_alert（服务端 → 客户端）

告警引擎检测到新告警时广播（经 MQTT `factory/line1/alerts` 中转）：

```json
{
  "rule_id": 2,
  "metric": "current",
  "value": 16.84,
  "level": "warning",
  "message": "current 超上限: 当前值 16.84 > 15",
  "fired_at": "2026-08-22T09:12:33"
}
```

### 5.3 客户端事件（connect / disconnect）

前端无需主动发送任何事件；连接与断开仅用于服务端日志，断线后 Socket.IO 自动重连。

---

## 6. 调试工具速查

| 工具 | 命令 |
|------|------|
| 接口批量自测 | `curl -s localhost:5000/api/health \| python -m json.tool` |
| 观察总线原始报文 | `mosquitto_sub -h localhost -t 'factory/#' -v` |
| 手动注入一条告警 | `mosquitto_pub -h localhost -t factory/line1/alerts -m '{"level":"warning","message":"手动测试","metric":"voltage","fired_at":"2026-08-22T10:00:00"}'` |
| 直查数据库 | `influx -execute 'SELECT * FROM alerts ORDER BY time DESC LIMIT 5' -database factory_metrics` |
