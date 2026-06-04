# AGENTS.md — JobLens TAP 开发指南

## 项目定位
计算集群可观测性数据中台，屏蔽多个 ES 集群差异，提供统一数据查询出口。
API 文档见 `API.md`，设计文档 `design.md` 仅供参考设计意图（多处与代码不一致）。

## 快速命令

```bash
make deps          # go mod download + tidy
make build         # 编译到 bin/server（注入 GitCommit/BuildTime 通过 ldflags）
make run           # go run ./cmd/server（需设置 TAP_MANAGEMENT_API_URL）
make test          # go test -v ./...（跑全部测试，包括 internal 和 tests 目录）
make test-coverage # 测试覆盖率报告 → coverage.html
make fmt           # go fmt ./...
make lint          # golangci-lint run ./...
make clean         # 清理 bin/ 和 go clean
```

**运行单个测试包**:
```bash
go test -v ./tests -run TestParserService_ParseTime
go test -v ./internal/model -run TestBuildDefaultRegistry
```

**短测试模式**（跳过需 ES 的集成测试）:
```bash
go test -v -short ./...
```

## 架构要点

- **入口**: `cmd/server/main.go` — 唯一二进制入口
- **模块路径**: `github.com/joblens/tap`，Go 版本见 `go.mod`（当前 1.25）
- **配置**: 完全通过环境变量（`caarlos0/env`），无 YAML 文件。配置结构定义在 `internal/config/config.go`
- **无 CI/Dockerfile**：当前仓库无 CI workflow 或 Docker 配置

### 环境变量关键项

| 变量 | 说明 |
|------|------|
| `TAP_MANAGEMENT_API_URL` | **必填**，集群元数据来源（`{URL}/api/clusters/scheme`） |
| `TAP_PORT` | 服务端口（默认 8080） |
| `TAP_COLLECTOR_REGISTRY_PATH` | **推荐**，采集器注册文件路径（JSON），支持 SIGHUP 热重载 |
| `TAP_MAX_SIZE` | 单次查询最大返回数（默认 10000） |
| `TAP_MAX_TIME_RANGE_DAYS` | 最大查询时间范围（默认 7） |

> `TAP_CLUSTERS` 已废弃（改用管理 API），`TAP_DEFAULT_COLLECTORS` 已废弃（改用注册文件）。完整配置项见 `internal/config/config.go`。

### 包结构

```
cmd/server/main.go        # 唯一入口
internal/
  config/                 # 环境变量配置加载
  cluster/                # 集群元数据管理器（管理 API 拉取 + 缓存 + 后台刷新）
  handler/                # Gin HTTP 处理器（health/raw/timeseries/summary/schema/check/collect/skill）
  service/                # 业务逻辑（查询、索引、解析、扁平化、采集触发、Agent 重试）
  repository/             # ES 客户端管理（惰性创建，双检锁）
  model/                  # 请求/响应模型、CollectorRegistry、向后兼容包装
  middleware/              # Gin 中间件（Recovery → Logger → ErrorHandler）
  skill/                  # embedded API skill 文档模板渲染
tests/                    # 黑盒集成测试（package tests）
pkg/utils/                # 预留，当前为空
```

### 关键实现细节

- **ES 客户端惰性创建**: 启动时不连 ES，首次查询时创建（`repository/es.go`），`InsecureSkipVerify: true`
- **采集器注册中心**: 字段别名和索引命名由 `model.CollectorRegistry` 管理。设置 `TAP_COLLECTOR_REGISTRY_PATH` 后新增采集器无需改代码 → 编辑 `collector-registry.json` → `SIGHUP` 热重载即可
- **索引命名**: 默认 `{collector}_collector_{date}`（如 `cpumem_collector_2026.04.02`），可通过注册文件 `index_pattern` 自定义
- **响应格式**: 统一 `{code, message, data, meta}`（`model.Response`）
- **注释语言**: 全部用中文
- **GIN_MODE**: 默认设为 `release`，开发调试需显式设置 `GIN_MODE=debug`

### 中间件链（不可变更顺序）

`main.go` 路由注册: `Recovery → Logger → ErrorHandler`

### 版本信息

编译时通过 ldflags 注入（Makefile 自动处理）:
```
-X main.GitCommit=$(git rev-parse --short HEAD)
-X main.BuildTime=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
```
`main.Version` 手动维护（当前 0.1.0）。

## 测试注意事项

- **两类测试**:
  - `internal/**/*_test.go`：白盒单元测试，使用自身包名（如 `package model`），不依赖 ES
  - `tests/`：黑盒集成测试，`package tests`，依赖真实 ES（通过 `TEST_ES_*` 环境变量配置）
- **集成测试环境变量**: `TEST_ES_URL`、`TEST_CLUSTER_ID`、`TEST_JOB_ID`、`TEST_ES_USERNAME`、`TEST_ES_PASSWORD`
- **ES 不可达时 `TestMain` 静默退出**（code 0），不阻塞 CI
- **`-short` 跳过所有集成测试**

## 旧文档不一致（以代码和 API.md 为准）

- `README.md` 的 `TAP_CLUSTERS` 已废弃 → 实际用 `TAP_MANAGEMENT_API_URL`
- `design.md` 采集器名 `process` → 实际用 `cpumem`
- `design.md` 索引格式 `obs_{cluster}_{collector}_{date}` → 实际 `{collector}_collector_{date}`
