/**
 * ============================================================================
 * App.js — 工业物联网边缘网关系统 · React 前端仪表盘
 * ----------------------------------------------------------------------------
 * 页面结构：
 *   1. 顶部标题栏：系统名称 + 数据链路状态灯（WebSocket 是否在线）
 *   2. 实时数据仪表盘：电压 / 电流 / 功率因数 / 温度 四张卡片，
 *      数据来自 WebSocket 事件 realtime_data（网关每秒一帧）；
 *   3. 历史趋势图：Chart.js 折线图，按测点切换、按时间范围切换，
 *      数据来自 REST GET /api/history；
 *   4. 告警列表：WebSocket 事件 new_alert 实时滚动插入，
 *      页面加载时先拉取 REST GET /api/alerts 补齐历史告警；
 *   5. 告警规则面板：展示当前生效的阈值规则（GET /api/rules）。
 *
 * 依赖：react-chartjs-2(chart.js) socket.io-client
 * ============================================================================
 */

import React, { useEffect, useRef, useState } from "react";
import { io } from "socket.io-client";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import { Line } from "react-chartjs-2";

// 注册 Chart.js 组件（chart.js v4 按需注册）
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  Filler
);

// 后端地址：开发环境（npm start）与生产环境（同源部署）均可用
const API_BASE =
  process.env.REACT_APP_API_BASE || window.location.origin.replace(/:\d+$/, ":5000");

/** 测点元数据：卡片与趋势图共用（名称、单位、颜色、安全区间） */
const METRIC_META = {
  voltage:      { label: "电压",     unit: "V",   color: "#3b82f6", min: 200, max: 240, digits: 1 },
  current:      { label: "电流",     unit: "A",   color: "#10b981", min: 0,   max: 15,  digits: 2 },
  power_factor: { label: "功率因数", unit: "",    color: "#f59e0b", min: 0.9, max: 1,   digits: 3 },
  temperature:  { label: "温度",     unit: "°C",  color: "#ef4444", min: 0,   max: 60,  digits: 1 },
};

/** 历史时间范围选项（小时）
 *  label 显示文本，seconds 用于换算 /api/history 的起止时间 */
const TIME_RANGES = [
  { label: "近15分钟", hours: 0.25 },
  { label: "近1小时", hours: 1 },
  { label: "近6小时", hours: 6 },
];

/**
 * MetricCard — 单个实时数据卡片
 * @param meta  测点元数据
 * @param value 最新工程量（可能为 null 表示暂无数据）
 */
function MetricCard({ meta, value }) {
  const safe = value !== null && value >= meta.min && value <= meta.max;
  return (
    <div
      style={{
        ...styles.card,
        borderTop: `4px solid ${meta.color}`,
        boxShadow: safe
          ? "0 2px 8px rgba(0,0,0,0.08)"
          : "0 0 12px rgba(239,68,68,0.55)", // 越限时红色高亮
      }}
    >
      <div style={styles.cardTitle}>
        {meta.label}
        <span style={styles.cardUnit}>{meta.unit}</span>
      </div>
      <div
        style={{
          ...styles.cardValue,
          color: safe ? meta.color : "#ef4444",
        }}
      >
        {value === null ? "--" : value.toFixed(meta.digits)}
      </div>
      <div style={styles.cardRange}>
        安全区间 {meta.min} ~ {meta.max}
      </div>
    </div>
  );
}

/**
 * AlertList — 告警滚动列表（最新在最上，超过 50 条自动截断）
 */
function AlertList({ alerts }) {
  return (
    <div style={{ ...styles.panel, height: 320 }}>
      <div style={styles.panelTitle}>🔔 告警列表（实时滚动）</div>
      <div style={styles.alertScroll}>
        {alerts.length === 0 && <div style={styles.empty}>暂无告警，运行正常</div>}
        {alerts.map((a, i) => (
          <div key={i} style={styles.alertItem(a.level)}>
            <span style={styles.alertBadge(a.level)}>
              {a.level === "critical" ? "严重" : "警告"}
            </span>
            <span style={{ flex: 1 }}>{a.message}</span>
            <span style={styles.alertTime}>{(a.fired_at || "").slice(11)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

/**
 * 根组件：负责数据流（WebSocket + REST 轮询）与页面编排
 */
export default function App() {
  // 实时测点值：{ voltage: 220.1, current: 10.02, ... }
  const [realtime, setRealtime] = useState(null);
  // WebSocket 连接状态（驱动顶部状态灯）
  const [wsOnline, setWsOnline] = useState(false);
  // 历史趋势数据点：[{time, value}]
  const [history, setHistory] = useState([]);
  // 当前趋势图展示的测点与时间范围
  const [metric, setMetric] = useState("voltage");
  const [rangeHours, setRangeHours] = useState(1);
  // 告警数组与规则数组
  const [alerts, setAlerts] = useState([]);
  const [rules, setRules] = useState([]);
  // 保存 socket 引用，避免重连泄漏
  const socketRef = useRef(null);

  // ------------------------------------------------------------------
  // 副作用 1：建立 WebSocket 连接，订阅 realtime_data / new_alert 事件
  // ------------------------------------------------------------------
  useEffect(() => {
    const socket = io(API_BASE, {
      transports: ["websocket", "polling"], // WebSocket 优先，降级轮询
      reconnectionAttempts: Infinity,
    });
    socketRef.current = socket;

    socket.on("connect", () => setWsOnline(true));
    socket.on("disconnect", () => setWsOnline(false));

    // 每秒一帧实时数据：直接刷新卡片
    socket.on("realtime_data", (payload) => {
      setRealtime(payload.values || null);
    });

    // 新告警：滚动插入列表头部
    socket.on("new_alert", (alert) => {
      setAlerts((prev) => [alert, ...prev].slice(0, 50));
    });

    return () => socket.close(); // 组件卸载时断开连接
  }, []);

  // ------------------------------------------------------------------
  // 副作用 2：拉取历史告警与规则列表（仅页面加载时执行一次）
  // ------------------------------------------------------------------
  useEffect(() => {
    fetch(`${API_BASE}/api/alerts`)
      .then((r) => r.json())
      .then((d) => d.data && setAlerts(d.data.map(flatAlert).slice(0, 50)))
      .catch(() => {});
    fetch(`${API_BASE}/api/rules`)
      .then((r) => r.json())
      .then((d) => d.data && setRules(d.data))
      .catch(() => {});
  }, []);

  // ------------------------------------------------------------------
  // 副作用 3：拉取历史趋势（测点/范围切换时 + 每 10 秒自动刷新）
  // ------------------------------------------------------------------
  useEffect(() => {
    let timer;
    const load = () => {
      const end = Math.floor(Date.now() / 1000);
      const start = end - Math.floor(rangeHours * 3600);
      // 大范围自动放宽聚合窗口，避免数据点过多卡顿
      const window = rangeHours >= 6 ? "5m" : rangeHours >= 1 ? "30s" : "10s";
      fetch(`${API_BASE}/api/history?metric=${metric}&start=${start}&end=${end}&window=${window}`)
        .then((r) => r.json())
        .then((d) => d.data && setHistory(d.data))
        .catch(() => {});
    };
    load();
    timer = setInterval(load, 10000);
    return () => clearInterval(timer);
  }, [metric, rangeHours]);

  // Chart.js 数据装配：时间格式化为 HH:MM:SS
  const chartData = {
    labels: history.map((p) => new Date(p.time).toLocaleTimeString("zh-CN")),
    datasets: [
      {
        label: `${METRIC_META[metric].label} (${METRIC_META[metric].unit})`,
        data: history.map((p) => p.value),
        borderColor: METRIC_META[metric].color,
        backgroundColor: `${METRIC_META[metric].color}22`,
        fill: true,
        tension: 0.3, // 平滑曲线
        pointRadius: 0,
        borderWidth: 2,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    animation: false, // 数据频繁刷新，关闭动画避免闪烁
    scales: {
      y: { grid: { color: "#e5e7eb" } },
      x: { ticks: { maxTicksLimit: 10 }, grid: { display: false } },
    },
    plugins: { legend: { display: false } },
  };

  return (
    <div style={styles.page}>
      {/* ---------- 顶部标题栏 ---------- */}
      <header style={styles.header}>
        <h1 style={styles.title}>🏭 工业物联网边缘网关监控平台</h1>
        <span style={styles.statusDot(wsOnline)} />
        <span style={styles.statusText}>{wsOnline ? "实时链路在线" : "连接中断..."}</span>
      </header>

      {/* ---------- 四张实时数据卡片 ---------- */}
      <section style={styles.cardRow}>
        {Object.entries(METRIC_META).map(([key, meta]) => (
          <MetricCard key={key} meta={meta} value={realtime ? realtime[key] : null} />
        ))}
      </section>

      {/* ---------- 历史趋势图 ---------- */}
      <section style={styles.panel}>
        <div style={styles.panelToolbar}>
          <span style={styles.panelTitle}>📈 历史趋势图</span>
          <div>
            {Object.entries(METRIC_META).map(([key, meta]) => (
              <button
                key={key}
                onClick={() => setMetric(key)}
                style={styles.tabBtn(metric === key)}
              >
                {meta.label}
              </button>
            ))}
            <span style={{ margin: "0 8px" }} />
            {TIME_RANGES.map((r) => (
              <button
                key={r.hours}
                onClick={() => setRangeHours(r.hours)}
                style={styles.tabBtn(rangeHours === r.hours)}
              >
                {r.label}
              </button>
            ))}
          </div>
        </div>
        <div style={{ height: 320 }}>
          {history.length > 0 ? (
            <Line data={chartData} options={chartOptions} />
          ) : (
            <div style={styles.empty}>暂无历史数据（等待网关采集入库）</div>
          )}
        </div>
      </section>

      {/* ---------- 告警列表 + 规则面板 ---------- */}
      <section style={styles.bottomRow}>
        <AlertList alerts={alerts} />
        <div style={{ ...styles.panel, height: 320 }}>
          <div style={styles.panelTitle}>⚙️ 生效中的告警规则</div>
          <div style={styles.alertScroll}>
            {rules.length === 0 && <div style={styles.empty}>尚未加载规则</div>}
            {rules.map((r) => (
              <div key={r.id} style={styles.ruleItem}>
                <span style={styles.alertBadge(r.level)}>
                  {r.level === "critical" ? "严重" : "警告"}
                </span>
                <span style={{ fontWeight: 600 }}>
                  {METRIC_META[r.metric]?.label || r.metric}
                </span>
                <span style={{ color: "#6b7280", marginLeft: 8 }}>
                  {r.min_value ?? "-∞"} ~ {r.max_value ?? "+∞"}
                </span>
                <span style={{ marginLeft: "auto", color: r.enabled ? "#059669" : "#9ca3af" }}>
                  {r.enabled ? "启用" : "停用"}
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <footer style={styles.footer}>
        数据链路：模拟PLC → Modbus/TCP → Go网关 → MQTT → InfluxDB → 本仪表盘
      </footer>
    </div>
  );
}

/**
 * 把 /api/alerts 返回的 InfluxDB 行转换为前端告警条目统一格式
 * （历史告警来自时序库，字段在 fields 里；实时告警来自 MQTT，结构已对齐）
 */
function flatAlert(row) {
  return {
    metric: row.metric,
    level: row.level,
    message: row.message,
    fired_at: row.time,
  };
}

/**
 * 内联样式表：保持单文件自包含，无需额外 CSS 文件
 */
const styles = {
  page: {
    fontFamily: '"PingFang SC","Microsoft YaHei",sans-serif',
    background: "#f3f4f6",
    minHeight: "100vh",
    margin: 0,
    padding: 24,
    boxSizing: "border-box",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 10,
    marginBottom: 20,
  },
  title: { fontSize: 22, margin: 0, color: "#111827" },
  statusDot: (online) => ({
    width: 12,
    height: 12,
    borderRadius: "50%",
    marginLeft: 16,
    background: online ? "#10b981" : "#ef4444",
    boxShadow: `0 0 6px ${online ? "#10b981" : "#ef4444"}`,
  }),
  statusText: { color: "#6b7280", fontSize: 13 },
  cardRow: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
    gap: 16,
    marginBottom: 16,
  },
  card: {
    background: "#fff",
    borderRadius: 12,
    padding: "18px 20px",
  },
  cardTitle: { color: "#6b7280", fontSize: 14, fontWeight: 600 },
  cardUnit: { marginLeft: 6, fontSize: 12, color: "#9ca3af" },
  cardValue: { fontSize: 38, fontWeight: 700, margin: "10px 0 6px" },
  cardRange: { fontSize: 12, color: "#9ca3af" },
  panel: {
    background: "#fff",
    borderRadius: 12,
    padding: "16px 20px",
    marginBottom: 16,
    overflow: "hidden",
  },
  panelToolbar: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8,
    flexWrap: "wrap",
    gap: 8,
  },
  panelTitle: { fontWeight: 700, color: "#111827", fontSize: 15, marginBottom: 8 },
  tabBtn: (active) => ({
    border: "1px solid #d1d5db",
    background: active ? "#2563eb" : "#fff",
    color: active ? "#fff" : "#374151",
    borderRadius: 6,
    padding: "4px 10px",
    fontSize: 12,
    cursor: "pointer",
    marginRight: 4,
  }),
  bottomRow: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(360px, 1fr))",
    gap: 16,
  },
  alertScroll: { overflowY: "auto", maxHeight: 250 },
  alertItem: (level) => ({
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "8px 10px",
    marginBottom: 6,
    borderRadius: 8,
    background: level === "critical" ? "#fef2f2" : "#fffbeb",
    fontSize: 13,
    animation: "fadeIn 0.4s",
  }),
  alertBadge: (level) => ({
    background: level === "critical" ? "#dc2626" : "#d97706",
    color: "#fff",
    borderRadius: 4,
    padding: "1px 6px",
    fontSize: 12,
    flexShrink: 0,
  }),
  alertTime: { color: "#9ca3af", fontSize: 12, flexShrink: 0 },
  ruleItem: {
    display: "flex",
    alignItems: "center",
    padding: "8px 10px",
    marginBottom: 6,
    borderRadius: 8,
    background: "#f9fafb",
    fontSize: 13,
  },
  empty: {
    color: "#9ca3af",
    textAlign: "center",
    padding: "60px 0",
    fontSize: 13,
  },
  footer: {
    textAlign: "center",
    color: "#9ca3af",
    fontSize: 12,
    marginTop: 8,
  },
};
