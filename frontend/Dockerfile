# ============================================================================
# Dockerfile — React 前端（多阶段：node 构建 + nginx 托管静态文件）
# 构建上下文：frontend/ 目录
# ============================================================================
# 第一阶段：npm install + 构建
FROM node:18-alpine AS builder
WORKDIR /app
# 先拷贝依赖清单，利用层缓存
COPY package.json ./
# 国内构建加固：npmmirror 源
RUN npm config set registry https://registry.npmmirror.com
RUN npm install --no-audit --no-fund
COPY . .
# 不注入 REACT_APP_API_BASE：运行时前端按浏览器地址自动推导后端 :5000
RUN npm run build

# 第二阶段：nginx 托管
FROM nginx:1.25-alpine
COPY --from=builder /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
