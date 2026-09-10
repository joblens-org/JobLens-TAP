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

# 运行阶段：alma9（安装 python3，供 /scripts 下 restd 触发脚本执行——
# 触发脚本 ihep-condor.py / ihep-slurm.py 为 python3，经标准库调用 restd 服务查询作业节点）
FROM almalinux:9

WORKDIR /app

# python3：触发脚本运行时依赖
# ca-certificates：Go 程序 HTTPS 请求（ES / management API）需要
RUN dnf -y install python3 ca-certificates && dnf clean all

# 从构建阶段复制二进制文件
COPY --from=builder /app/server .

# 暴露端口
EXPOSE 8080

# 运行
CMD ["./server"]
