# AGENTS.md — JobLens TAP 开发指南

## 项目定位
计算集群可观测性数据中台，屏蔽多个 ES 集群差异，提供统一数据查询出口。
API 文档见 `API.md`，设计文档见 `design.md`。

## 快速命令

```bash
make deps          # 下载依赖 + tidy
make build         # 编译到 bin/server
make run           # go run ./cmd/server（需设置环境变量）
make test          # 运行全部测试: go test -v ./...
make test-coverage # 测试覆盖率报告
make fmt           # go fmt ./...
make lint          # golangci-lint run ./...
make clean         # 清理 bin/ 和 go clean
```

**运行单个测试包**:
```bash
go test -v ./tests -run TestParserService_ParseTime
```

**短测试模式**（跳过需要 ES 的集成测试）:
```bash
go test -v -short ./...
```

## 架构要点

- **入口**: `cmd/server/main.go` — 唯一二进制入口
- **模块路径**: `github.com/joblens/tap`
- **Go 版本**: 1.25（以 `go.mod` 为准，非 README 中的 1.22）
- **配置**: 完全通过环境变量加载（`caarlos0/env`），无 YAML 配置文件。配置定义在 `internal/config/config.go`

### 环境变量关键项

| 变量 | 说明 |
|------|------|
| `TAP_PORT` | 服务端口（默认 8080） |
| `TAP_MANAGEMENT_API_URL` | **必填**，集群元数据来源，路径为 `{URL}/api/clusters/scheme` |
| `TAP_SERVICE_REGISTRY_URL` | 服务注册中心地址（采集触发用） |
| `TAP_MANAGEMENT_CACHE_TTL` | 集群信息缓存 TTL（默认 5m） |
| `TAP_MAX_SIZE` | 单次查询最大返回数（默认 10000） |
| `TAP_MAX_TIME_RANGE_DAYS` | 最大查询时间范围（默认 7） |
| `TAP_COLLECTOR_REGISTRY_PATH` | **推荐**，采集器注册文件路径（JSON 格式）。设置后 `TAP_DEFAULT_COLLECTORS` 被忽略。支持 SIGHUP 热重载。 |
| `TAP_DEFAULT_COLLECTORS` | 默认采集器列表（已废弃，建议用 `TAP_COLLECTOR_REGISTRY_PATH`）。仅在未设置注册文件时生效，默认 `cpumem,io,net` |

> **注意**: README 中提到的 `TAP_CLUSTERS` 环境变量已废弃。集群元数据现在通过管理 API 动态拉取（`internal/cluster/manager.go`）。

### 包结构约定

```
cmd/server/main.go        # 唯一入口
internal/
  config/                 # 环境变量配置加载
  cluster/                # 集群元数据管理器（从管理 API 拉取 + 缓存）
  handler/                # Gin HTTP 处理器
  service/                # 业务逻辑（查询、索引、解析、扁平化、采集触发）
  repository/             # ES 客户端管理（惰性创建，双检锁）
  model/                  # 请求/响应模型、采集器注册中心、ES 字段别名
  middleware/              # Gin 中间件（Logger、Error、Recovery）
tests/                    # 所有测试（仅外部测试包）
pkg/utils/                # 预留，当前为空
```

### 关键实现细节

- **ES 客户端是惰性创建的**: 不在启动时连接 ES，而是首次查询时创建（`repository/es.go:45`）。集群元数据通过 `cluster.Manager` 从管理 API 获取。
- **TLS 证书跳过验证**: ES 连接设置 `InsecureSkipVerify: true`（`repository/es.go:99`）。
- **采集器注册**: 采集器列表和字段别名通过 `collector-registry.json` 外部注册文件管理。设置 `TAP_COLLECTOR_REGISTRY_PATH` 环境变量后，新增采集器（如 `gpu`）和指标别名无需改代码，只需编辑注册文件并发送 `SIGHUP` 热重载。注册文件未设置时回退到 `TAP_DEFAULT_COLLECTORS` + 内置硬编码别名。
- **索引命名**: 通过注册文件的 `index_pattern` 字段渲染，默认 `{collector}_collector_{date}`（如 `cpumem_collector_2026.04.02`），在 `service/index.go:30` 通过 `Registry.RenderIndexName()` 生成。
- **字段别名映射**: 定义在 `internal/model/registry.go` 的 `CollectorRegistry` 中，启动时从注册文件加载。别名 → ES 路径的映射由注册中心统一管理，旧 `internal/model/es.go` 中的 `FieldAliasMap` 全局变量已废弃。
- **响应格式**: 统一 `{code, message, data, meta}` 结构（`model.Response`）。
- **comment 语言**: 所有注释用中文。

### 中间件链顺序（不可变更）

`main.go` 中路由注册顺序: `Recovery → Logger → ErrorHandler`

## 测试注意事项

- **测试目录**: 所有测试在 `tests/` 下，使用 `package tests`（外部测试包）。
- **集成测试依赖真实 ES**: 需要设置环境变量 `TEST_ES_URL`、`TEST_CLUSTER_ID`、`TEST_JOB_ID`、`TEST_ES_USERNAME`、`TEST_ES_PASSWORD`。ES 不可达时 `TestMain` 会静默退出（code 0）。
- **短测试模式**: `-short` 会跳过所有需要 ES 连接的测试。
- **测试入口**: `TestMain` 在 `tests/integration_test.go` 中初始化全局 `esManager`、`appCfg`、`querySvc`。

## 旧文档与代码不一致点

- `README.md` 中 `TAP_CLUSTERS` 配置方式已废弃，实际使用 `TAP_MANAGEMENT_API_URL`
- `design.md` 中采集器名称为 `process`，实际代码用 `cpumem`
- `design.md` 中索引格式为 `obs_{cluster}_{collector}_{date}`，实际为 `{collector}_collector_{date}`
- 以代码和 `API.md` 为准，`design.md`/`implementation_checklist.md` 仅供参考设计意图
