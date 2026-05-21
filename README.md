# JobLens TAP

计算集群可观测性数据中台（Telemetry Access Point）

> **English**: [README_EN.md](./README_EN.md)

## 项目概述

JobLens TAP 是一个高性能、无状态的计算集群可观测性数据查询网关，屏蔽底层多个 Elasticsearch 集群的差异，提供统一的数据查询出口。支持多集群并行查询、时序聚合、任务摘要、字段别名映射以及采集触发等功能。

### 核心特性

- **多集群透明路由**: 自动从管理 API 获取集群元数据（cluster → index 映射、端点、凭证），支持 `*` 通配和逗号分隔的多集群并行查询
- **字段别名系统**: 通过外部注册文件（`collector-registry.json`）管理别名 → ES 字段路径映射，新增采集器和指标无需改代码，支持 SIGHUP 热重载
- **采集器注册中心**: 内置 cpumem/io/net/gpu 采集器支持，采集器列表和索引命名规则可配置
- **惰性 ES 连接**: 不在启动时连接 ES，首次查询时才创建客户端（双检锁保证并发安全）
- **游标分页**: 基于 `search_after` 实现高效连续分页
- **优雅关闭**: SIGINT/SIGTERM 优雅退出，SIGHUP 热重载采集器注册文件
- **扁平化输出**: 去除 ES 嵌套元数据，返回纯净业务字段

## 技术栈

- **语言**: Go 1.25
- **Web 框架**: Gin v1.11
- **ES 客户端**: go-elasticsearch v8
- **配置管理**: 环境变量（caarlos0/env）

## 快速开始

### 环境要求

- Go 1.25+
- Elasticsearch 集群（通过管理 API 提供集群元数据）

### 本地开发

```bash
# 下载依赖
make deps

# 编译
make build

# 运行测试
make test

# 短测试模式（跳过 ES 集成测试）
go test -v -short ./...

# 本地运行
export TAP_MANAGEMENT_API_URL="https://your-management-api.example.com"
export TAP_COLLECTOR_REGISTRY_PATH="./collector-registry.json"
make run
```

### Docker 构建

```bash
# 构建二进制
make build

# 编译后二进制位于 bin/server
```

## API 概览

所有接口统一返回 `{code, message, data, meta}` 结构。

### 端点列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 服务健康检查 |
| GET | `/ready` | 就绪探针（检查 ES 集群连接状态） |
| GET | `/data/raw` | 原始数据查询（日志级别采样点） |
| GET | `/data/timeseries` | 时序聚合查询（图表级别） |
| GET | `/data/summary` | 任务摘要查询 |
| GET | `/data/check-job` | Job 数据存在性检查（轻量级，size=0） |
| GET | `/schema` | Schema 发现（字段与集群元数据） |
| GET | `/skill` | Skill API 文档（供可视化管线消费） |
| POST | `/collect` | 触发作业采集（自动查询节点信息） |
| POST | `/collect/direct` | 直接触发采集（用户提供节点信息） |

### 快速示例

```bash
# 健康检查
curl http://localhost:8080/health

# 原始数据查询
curl "http://localhost:8080/data/raw?cluster=sz01&job=172.0&from=now-1h&fields=cpu,mem"

# 时序聚合查询（多指标）
curl "http://localhost:8080/data/timeseries?cluster=sz01&job=172.0&metric=cpu,mem&interval=1m&from=now-1h&by=host"

# 任务摘要
curl "http://localhost:8080/data/summary?cluster=sz01&job=172.0"

# Job 数据存在性检查
curl "http://localhost:8080/data/check-job?cluster_name=sz01&job_id=172.0"

# Schema 发现
curl "http://localhost:8080/schema?cluster=sz01"
```

> 完整 API 文档参见 [API.md](./API.md)

## 配置说明

所有配置通过环境变量加载，无需配置文件。

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `TAP_PORT` | 否 | `8080` | 服务端口 |
| `TAP_LOG_LEVEL` | 否 | `info` | 日志级别 |
| `TAP_READ_TIMEOUT` | 否 | `30s` | HTTP 读超时 |
| `TAP_WRITE_TIMEOUT` | 否 | `30s` | HTTP 写超时 |
| `TAP_MANAGEMENT_API_URL` | **是** | - | 管理 API 地址，集群元数据来源（路径 `{URL}/api/clusters/scheme`） |
| `TAP_MANAGEMENT_CACHE_TTL` | 否 | `5m` | 集群信息缓存 TTL |
| `TAP_MAX_SIZE` | 否 | `10000` | 单次查询最大返回数 |
| `TAP_DEFAULT_SIZE` | 否 | `100` | 单次查询默认返回数 |
| `TAP_MAX_TIME_RANGE_DAYS` | 否 | `7` | 最大查询时间范围（天） |
| `TAP_DEFAULT_INTERVAL` | 否 | `1m` | 时序查询默认聚合粒度 |
| `TAP_COLLECTOR_REGISTRY_PATH` | 推荐 | - | 采集器注册文件路径（JSON），支持 SIGHUP 热重载 |
| `TAP_DEFAULT_COLLECTORS` | 否 | `cpumem,io,net` | 默认采集器列表（仅注册文件未设置时生效，已废弃） |
| `TAP_SERVICE_REGISTRY_URL` | 否 | - | 服务注册中心地址（采集触发用） |
| `TAP_SERVICE_REGISTRY_TIMEOUT` | 否 | `5s` | 注册中心查询超时 |
| `TAP_AGENT_RETRY_INITIAL_DELAY` | 否 | `500ms` | Agent 重试初始延迟 |
| `TAP_AGENT_RETRY_MAX_ATTEMPTS` | 否 | `3` | Agent 最大重试次数 |
| `TAP_AGENT_RETRY_MULTIPLIER` | 否 | `2.0` | Agent 重试退避因子 |
| `TAP_SKILL_API_BASE_URL` | 否 | - | Skill API 基础 URL |

### 采集器注册文件

`TAP_COLLECTOR_REGISTRY_PATH` 指向的注册文件格式如下（项目根目录提供了 `collector-registry.json` 示例）：

```json
{
  "version": 1,
  "collectors": [
    {
      "name": "cpumem",
      "description": "CPU和内存采集器",
      "aliases": [
        {"alias": "cpu", "es_field": "data.summary.cpuPercent", "type": "float"},
        {"alias": "mem", "es_field": "data.summary.mem_rss_kb", "type": "long"}
      ]
    }
  ],
  "global_aliases": [
    {"alias": "host", "es_field": "hostname.keyword", "type": "keyword"},
    {"alias": "time", "es_field": "@timestamp", "type": "date"}
  ]
}
```

- `collectors[].name`: 采集器名称，决定默认索引命名 `{name}_collector_{date}`
- `collectors[].aliases`: 采集器专属字段别名映射
- `global_aliases`: 所有采集器共享的全局别名

## 项目结构

```
JobLens-TAP/
├── cmd/server/              # 程序入口
├── internal/
│   ├── config/              # 环境变量配置加载
│   ├── cluster/             # 集群元数据管理器（管理 API 拉取 + 缓存）
│   ├── handler/             # Gin HTTP 处理器
│   ├── service/             # 业务逻辑层
│   ├── repository/          # ES 客户端管理（惰性创建，双检锁）
│   ├── model/               # 数据模型、采集器注册中心
│   ├── middleware/           # Gin 中间件（Recovery → Logger → ErrorHandler）
│   └── skill/               # Skill 接口模板
├── tests/                   # 所有测试（外部测试包）
├── pkg/utils/               # 预留工具包
├── collector-registry.json  # 采集器注册文件示例
├── Makefile                 # 构建脚本
├── API.md                   # API 接口文档
├── design.md                # 设计文档（仅供参考）
└── README.md
```

### 中间件链顺序（不可变更）

```
Recovery → Logger → ErrorHandler
```

## 开发

### 常用命令

```bash
make deps          # 下载依赖 + tidy
make build         # 编译到 bin/server
make run           # go run ./cmd/server
make test          # 运行全部测试
make test-coverage # 测试覆盖率报告
make fmt           # go fmt ./...
make lint          # golangci-lint run ./...
make clean         # 清理 bin/ 和 go clean
```

### 测试说明

- 所有测试位于 `tests/` 目录，使用外部测试包（`package tests`）
- 集成测试需要真实 ES 集群，需设置 `TEST_ES_URL`、`TEST_CLUSTER_ID`、`TEST_JOB_ID`、`TEST_ES_USERNAME`、`TEST_ES_PASSWORD` 环境变量
- ES 不可达时 `TestMain` 静默退出（code 0），不阻塞 CI
- 使用 `-short` 跳过集成测试

## License

[Apache License 2.0](./LICENSE)
