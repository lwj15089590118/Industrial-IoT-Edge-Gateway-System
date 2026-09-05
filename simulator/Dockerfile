# ============================================================================
# Dockerfile — 模拟 PLC（Modbus/TCP 服务端）
# 构建上下文：simulator/ 目录
# ============================================================================
FROM python:3.10-slim

# 固定 pymodbus 2.x：本程序使用 2.x 同步服务端 API，3.x 接口不兼容
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir pymodbus==2.5.3

WORKDIR /app
COPY plc_simulator.py .

# Modbus/TCP 标准端口 502（容器内 root 可绑定特权端口）
EXPOSE 502

ENV PYTHONUNBUFFERED=1
CMD ["python", "plc_simulator.py"]
