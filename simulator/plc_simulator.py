# -*- coding: utf-8 -*-
"""
plc_simulator.py — 模拟 PLC（可编程逻辑控制器）程序
========================================================================
职责：
    1. 模拟一台工厂数采 PLC，持续生成 10 个电力/环境测点的随机工业数据；
    2. 数据按物理规律生成（正弦波动 + 高斯噪声 + 偶发异常尖峰），
       而非纯随机数，尽量贴近真实工厂时序特征；
    3. 开启 Modbus/TCP 服务端（端口 502），将测点值写入保持寄存器区，
       供 Go 网关轮询读取；
    4. 控制台周期性打印寄存器快照，便于观察数据变化。

寄存器映射（与 gateway/config.yaml 的地址表严格一致）：
    地址 0: 电压         0.1 V     工程量 = 原始值 * 0.1
    地址 1: 电流         0.01 A
    地址 2: 功率因数     0.001
    地址 3: 温度         0.1 °C
    地址 4: 有功功率     1 W
    地址 5: 电网频率     0.1 Hz
    地址 6: 无功功率     1 var
    地址 7: 视在功率     1 VA
    地址 8: 累计电能     0.1 kWh
    地址 9: 设备状态字   位图（bit0 运行 / bit1 故障 / bit2 通讯）

依赖：pymodbus==2.5.3（2.x 同步 API；3.x 接口不同，请勿混用）
运行：python plc_simulator.py
"""

import logging
import math
import random
import threading
import time

from pymodbus.server.sync import StartTcpServer
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusSlaveContext,
    ModbusServerContext,
)
from pymodbus.device import ModbusDeviceIdentification

# ---------------------------------------------------------------------------
# 全局配置
# ---------------------------------------------------------------------------
MODBUS_HOST = "0.0.0.0"   # 监听所有网卡，Docker 容器内网关才能访问
MODBUS_PORT = 502          # Modbus/TCP 标准端口
UPDATE_INTERVAL = 1.0      # 寄存器刷新周期（秒），与网关采集周期一致
UNIT_ID = 0x01             # 从站号，与网关配置 unit_id 对应

# 日志格式：时间 + 级别 + 消息
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("plc-simulator")


# ---------------------------------------------------------------------------
# 工业数据模型：模拟配电柜的电气量与环境量
# ---------------------------------------------------------------------------
class PlantDataModel:
    """按物理规律演化的一组测点，每次 tick() 前进一步。"""

    def __init__(self):
        # 时间步计数器，用于正弦相位推进
        self._tick = 0
        # 基准工况（额定值附近波动）
        self._voltage_base = 220.0   # V
        self._current_base = 10.0    # A
        self._pf_base = 0.95         # 功率因数
        self._temp_base = 35.0       # °C 柜内基线温度
        self._energy_kwh = 12345.6   # 累计电能初始值（kWh）
        self._fault_flag = False     # 模拟偶发故障位

    def tick(self):
        """推进一个时间步，返回 10 个测点的工程量字典。"""
        self._tick += 1
        t = self._tick

        # ---- 电压：220V 基线 + 慢正弦(电网波动) + 高斯噪声，1% 概率电压跌落尖峰 ----
        voltage = (
            self._voltage_base
            + 3.0 * math.sin(2 * math.pi * t / 60.0)   # 周期约 1 分钟的缓变
            + random.gauss(0, 0.5)                       # 测量噪声
        )
        if random.random() < 0.01:                       # 偶发异常事件，用于触发前端告警
            voltage -= random.uniform(25.0, 45.0)
            log.warning("模拟异常事件：电压跌落至 %.1f V", voltage)

        # ---- 电流：随负载波动的正弦 + 噪声，2% 概率过流冲击 ----
        current = (
            self._current_base
            + 2.5 * math.sin(2 * math.pi * t / 30.0)
            + random.gauss(0, 0.3)
        )
        if random.random() < 0.02:
            current += random.uniform(6.0, 10.0)
            log.warning("模拟异常事件：电流冲击至 %.2f A", current)

        # ---- 功率因数：0.90~0.99 之间随机游走 ----
        self._pf_base += random.gauss(0, 0.005)
        self._pf_base = min(0.99, max(0.90, self._pf_base))
        power_factor = self._pf_base

        # ---- 温度：昼夜节律正弦 + 缓慢漂移 + 噪声 ----
        temperature = (
            self._temp_base
            + 4.0 * math.sin(2 * math.pi * t / 120.0)   # 约 2 分钟一个"昼夜"周期
            + t * 0.002                                    # 极缓慢升温趋势
            + random.gauss(0, 0.2)
        )

        # ---- 频率：50Hz ± 0.05Hz 微小抖动 ----
        frequency = 50.0 + random.gauss(0, 0.03)

        # ---- 派生电气量（按单相近似公式换算，保证数据自洽） ----
        active_power = voltage * current * power_factor          # 有功 W
        apparent_power = voltage * current                        # 视在 VA
        reactive_power = math.sqrt(max(0.0, apparent_power ** 2 - active_power ** 2))  # 无功 var

        # ---- 累计电能：按本周期平均功率累加 ----
        self._energy_kwh += active_power * UPDATE_INTERVAL / 3600.0 / 1000.0

        # ---- 设备状态字：bit0=运行 bit1=故障 bit2=通讯，5% 概率瞬时故障 ----
        self._fault_flag = random.random() < 0.05
        status_word = 0b0001 | (0b0010 if self._fault_flag else 0) | 0b0100

        return {
            "voltage": voltage,
            "current": current,
            "power_factor": power_factor,
            "temperature": temperature,
            "active_power": active_power,
            "frequency": frequency,
            "reactive_power": reactive_power,
            "apparent_power": apparent_power,
            "energy_total": self._energy_kwh,
            "status_word": float(status_word),
        }


# ---------------------------------------------------------------------------
# 工程量 -> Modbus 寄存器原始值的编码规则（与网关 scale 配对，成反运算）
# ---------------------------------------------------------------------------
def encode_registers(snapshot: dict) -> list:
    """把工程量字典编码为 10 个 16 位寄存器原始值列表。"""
    raw = [
        int(round(snapshot["voltage"] / 0.1)),          # 地址 0
        int(round(snapshot["current"] / 0.01)),         # 地址 1
        int(round(snapshot["power_factor"] / 0.001)),   # 地址 2
        int(round(snapshot["temperature"] / 0.1)),      # 地址 3
        int(round(snapshot["active_power"] / 1)),       # 地址 4
        int(round(snapshot["frequency"] - 45.0)),       # 地址 5：网关侧 offset=45 还原
        int(round(snapshot["reactive_power"] / 1)),     # 地址 6
        int(round(snapshot["apparent_power"] / 1)),     # 地址 7
        int(round(snapshot["energy_total"] / 0.1)),     # 地址 8
        int(snapshot["status_word"]),                   # 地址 9
    ]
    return [v & 0xFFFF for v in raw]  # 限幅到 16 位无符号


# ---------------------------------------------------------------------------
# Modbus 服务端：初始化数据区 + 后台刷新线程
# ---------------------------------------------------------------------------
def build_server_context() -> ModbusServerContext:
    """
    构造 Modbus 服务端数据上下文。
    zero_mode=True 使寄存器地址与配置表直接对应（不加 40001 偏移）。
    """
    slave = ModbusSlaveContext(
        di=ModbusSequentialDataBlock(0, [0] * 16),             # 离散输入（未使用）
        co=ModbusSequentialDataBlock(0, [0] * 16),             # 线圈（未使用）
        hr=ModbusSequentialDataBlock(0, [0] * 16),             # 保持寄存器：核心数据区
        ir=ModbusSequentialDataBlock(0, [0] * 16),             # 输入寄存器（未使用）
        zero_mode=True,
    )
    return ModbusServerContext(slaves=slave, single=True)


def start_updater(context: ModbusServerContext, stop_event: threading.Event):
    """后台线程：周期性生成数据并写入保持寄存器（功能码 3 对应 hr 区）。"""
    model = PlantDataModel()
    while not stop_event.is_set():
        snapshot = model.tick()
        raw_values = encode_registers(snapshot)
        # 单从站模式：context[0]；功能码 3 = holding register；从地址 0 开始写入
        context[0].setValues(3, 0, raw_values)
        log.info(
            "寄存器已更新 -> 电压=%.1fV 电流=%.2fA 功率因数=%.3f 温度=%.1f°C 功率=%.0fW",
            snapshot["voltage"], snapshot["current"],
            snapshot["power_factor"], snapshot["temperature"],
            snapshot["active_power"],
        )
        stop_event.wait(UPDATE_INTERVAL)


def main():
    log.info("=" * 60)
    log.info("模拟 PLC 启动中：Modbus/TCP 服务端 %s:%d", MODBUS_HOST, MODBUS_PORT)
    log.info("=" * 60)

    context = build_server_context()
    stop_event = threading.Event()

    # 启动数据刷新线程（daemon：主线程退出时自动结束）
    updater = threading.Thread(
        target=start_updater, args=(context, stop_event), daemon=True
    )
    updater.start()

    # 设备识别信息（Modbus 标准设备对象，客户端读 Device Identification 时可见）
    identity = ModbusDeviceIdentification()
    identity.VendorName = "IIoT-Simulator"
    identity.ProductCode = "PLC-SIM-100"
    identity.VendorUrl = "https://example.com"
    identity.ProductName = "Industrial IoT Edge Gateway System"
    identity.ModelName = "Virtual Distribution Cabinet"
    identity.MajorMinorRevision = "1.0.0"

    try:
        # 阻塞启动 Modbus/TCP 服务端，等待网关连接
        StartTcpServer(
            context=context,
            identity=identity,
            address=(MODBUS_HOST, MODBUS_PORT),
        )
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C，模拟 PLC 停止")
    finally:
        stop_event.set()


if __name__ == "__main__":
    main()
