# 构建阶段
FROM golang:1.25-alpine AS builder

WORKDIR /app
ENV GOPROXY="https://goproxy.cn,direct"
ENV GOSUMDB="sum.golang.google.cn"
# 安装依赖
RUN apk add --no-cache git

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server ./cmd/server

# 运行阶段
FROM alpine:latest

WORKDIR /app

# 安装 CA 证书（Go 程序 HTTPS 请求需要）
RUN apk add --no-cache ca-certificates

# 从构建阶段复制二进制文件
COPY --from=builder /app/server .

# 暴露端口
EXPOSE 8080

# 运行
CMD ["./server"]
