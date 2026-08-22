# ============================================================================
# Dockerfile — 后端服务（Flask API + 告警引擎共用一个镜像）
# 构建上下文：backend/ 目录
#   - api-server 服务：默认 CMD，运行 api_server.py
#   - alert-engine 服务：compose 中覆盖 command，运行 alert_engine.py
# ============================================================================
FROM python:3.10-slim

RUN pip install --no-cache-dir \
    flask==3.0.0 \
    flask-cors==4.0.0 \
    flask-socketio==5.3.6 \
    simple-websocket==1.0.0 \
    paho-mqtt==1.6.1 \
    influxdb==5.3.1

WORKDIR /app
COPY api_server.py alert_engine.py ./

ENV PYTHONUNBUFFERED=1 \
    INFLUX_HOST=influxdb \
    MQTT_HOST=mosquitto

EXPOSE 5000

# 默认启动 API 服务；告警引擎容器由 compose 覆盖此命令
CMD ["python", "api_server.py"]
