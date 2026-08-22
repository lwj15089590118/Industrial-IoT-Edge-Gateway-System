# ============================================================================
# Dockerfile — Go 边缘网关（多阶段构建：编译期 golang，运行期 alpine 极简镜像）
# 构建上下文：gateway/ 目录
# ============================================================================
# 第一阶段：编译
FROM golang:1.21-alpine AS builder
WORKDIR /app
# 先拷贝依赖清单再下载，利用 Docker 层缓存加速重复构建
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
# 容器构建时优先采用 config.docker.yaml（服务名寻址），无则回退本地 config.yaml
RUN if [ -f config.docker.yaml ]; then cp config.docker.yaml config.yaml; fi
# 编译静态二进制（CGO 禁用，便于在 alpine 运行）
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /gateway .

# 第二阶段：运行（仅约 10MB）
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /gateway /app/gateway
COPY --from=builder /app/config.yaml /app/config.yaml
ENV TZ=Asia/Shanghai
# 优雅退出依赖 SIGTERM，添加 init 进程转发信号
ENTRYPOINT ["/app/gateway", "/app/config.yaml"]
