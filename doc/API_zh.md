# JobLens-TAP API 接口文档

**版本**: v1.1
**基础路径**: `/`

> **English**: [API.md](./API.md)

---

## 目录

- [概述](#概述)
- [统一响应结构](#统一响应结构)
- [端点列表](#端点列表)
  - [1. 健康检查](#1-健康检查)
  - [2. 原始数据查询](#2-原始数据查询)
  - [3. 时序数据查询](#3-时序数据查询)
  - [4. 任务摘要查询](#4-任务摘要查询)
  - [5. Schema 发现](#5-schema-发现)
  - [6. Job 数据存在性检查](#6-job-数据存在性检查)
  - [7. 触发作业采集](#7-触发作业采集)
- [字段别名映射](#字段别名映射)
- [使用示例](#使用示例)

---

## 概述

JobLens-TAP 是一个计算集群可观测性数据中台，提供统一的数据查询接口。

### 设计原则

- **无状态**: 服务不存储会话或缓存
- **透明路由**: 自动处理 Cluster → Index 的映射
- **字段别名**: 使用语义别名映射 ES 嵌套字段
- **扁平化输出**: 去除 ES 元数据，保留纯净业务字段

---

## 统一响应结构

所有接口返回统一的响应格式：

```json
{
  "code": 0,
  "message": "success",
  "data": { ... },
  "meta": {
    "query_time_ms": 45,
    "clusters_queried": ["htcondor01"],
    "indices_hit": ["cpumem_collector_2026.04.02", "cpumem_collector_2026.04.03"]
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int | 状态码，0 表示成功 |
| `message` | string | 状态消息 |
| `data` | object | 响应数据 |
| `meta` | object | 元信息（包含实际查询的索引列表和查询耗时） |

### 状态码说明

| Code | 说明 |
|------|------|
| 0 | 成功 |
| 400 | 请求参数错误 |
| 500 | 服务器内部错误 |

### HTTP 状态码说明

| HTTP Status | 说明 |
|-------------|------|
| 200 | 请求成功 |
| 207 | 部分成功（采集接口 `partial_success`） |
| 400 | 请求参数错误 |
| 500 | 服务器内部错误 |
| 503 | 服务不可用（就绪探针检测到 ES 集群不健康） |

---

## 端点列表

### 1. 健康检查

#### 1.1 服务健康检查

```
GET /health
```

**响应示例**:

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "status": "healthy",
    "time": "2026-05-04T15:33:01Z"
  }
}
```

#### 1.2 就绪探针

```
GET /ready
```

检查 ES 集群连接状态。

**成功响应** (HTTP 200):

```json
{
  "code": 0,
  "message": "ready",
  "data": {
    "htcondor01": "healthy",
    "INKSlurm": "healthy"
  }
}
```

**失败响应** (HTTP 503):

当部分或全部 ES 集群连接异常时返回：

```json
{
  "code": 1,
  "message": "some clusters are unavailable",
  "data": {
    "htcondor01": "unhealthy: connection refused",
    "INKSlurm": "healthy"
  }
}
```

---

### 2. 原始数据查询

拉取原始采样点数据（日志级别）。

```
GET /data/raw
```

#### 请求参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `cluster` | string | 是 | - | 集群ID，支持多值（逗号分隔）、通配（`*`）、或带标签（`cluster_name:cluster_tag`） |
| `job` | string | 是 | - | 原生 JobID 字符串（如 Condor `"172.0"`、Slurm `"67890"`），直接匹配 `job_info.NativeJobID` |
| `from` | string | 条件必填 | - | 起始时间（ISO8601 或相对时间），`full_range=true` 时可不填 |
| `to` | string | 否 | `now` | 结束时间 |
| `full_range` | bool | 否 | `false` | 自动发现 Job 完整时间范围并返回全部数据 |
| `collector` | string | 否 | 空 | 采集器类型（`cpumem`/`io`/`net`），空则查询全部 |
| `fields` | string | 否 | 空 | 字段白名单（逗号分隔），支持别名或原始 ES 路径 |
| `size` | int | 否 | `100` | 单批次数量，最大 10000 |
| `cursor` | string | 否 | 空 | 分页游标 |
| `flatten` | bool | 否 | `true` | 是否扁平化嵌套结构 |

> **注意**: `from` 在 `full_range=true` 时可选，否则必填。

#### `cluster` 参数格式

| 格式 | 示例 | 说明 |
|------|------|------|
| 单集群 | `htcondor01` | 查询指定集群 |
| 带标签 | `htcondor01:htcondor02@htcondor02.ihep.ac.cn` | 精确指定集群标签（ES routing） |
| 多集群 | `htcondor01,INKSlurm` | 逗号分隔，并行查询 |
| 通配 | `*` | 查询所有集群 |

> **单 Tag 自动优化**: 当未指定 `cluster_tag` 且目标集群只有一个 tag 时，系统自动使用该 tag 做 routing 加速查询。

#### 时间格式支持

| 格式 | 示例 | 说明 |
|------|------|------|
| ISO8601 | `2026-04-05T10:00:00Z` | 完整时间戳 |
| 相对时间 | `now-1h` | 当前时间减 1 小时 |
| 简化格式 | `1h`, `1d` | 等同于 `now-1h`, `now-1d` |

#### 响应结构

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "records": [
      {
        "cluster": "htcondor01",
        "collector": "cpumem",
        "time": "2026-05-03T23:20:03+0800",
        "host": "htcondor03.ihep.ac.cn",
        "job": "172.0",
        "cpu": 0.0,
        "mem": 3200,
        "name": "python-train",
        "fields": {
          "cpu": 0.0,
          "mem": 3200
        }
      }
    ],
    "pagination": {
      "has_more": true,
      "next_cursor": "eyJ0aW1lIjoxN...",
      "returned": 100,
      "total": 606
    },
    "indices_resolved": ["cpumem_collector_2026.05.02", "cpumem_collector_2026.05.03"]
  },
  "meta": {
    "query_time_ms": 14,
    "clusters_queried": ["htcondor01"],
    "indices_hit": ["cpumem_collector_2026.05.03"]
  }
}
```

#### Record 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `cluster` | string | 集群ID |
| `collector` | string | 采集器类型（从索引名自动提取） |
| `time` | string | 时间戳 |
| `host` | string | 主机名 |
| `job` | any | 原生 JobID（优先返回 `job_info.NativeJobID` 字符串，旧数据 fallback 到 `JobID` 数值） |
| `cpu` | float64 | CPU 使用率（快捷字段，来自 `data.summary.cpuPercent`） |
| `mem` | int64 | 内存使用量 KB（快捷字段，来自 `data.summary.mem_rss_kb`） |
| `name` | string | 进程名（快捷字段，来自 `data.summary.name.keyword`） |
| `io_bytes` | int64 | IO 字节数（快捷字段，来自 `data.summary.read_bytes`） |
| `fields` | map | 扁平化后的字段集合（`flatten=true` 时返回） |
| `data` | map | 嵌套原始数据结构（`flatten=false` 时返回） |

#### 响应元信息字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `data.indices_resolved` | []string | 各服务解析出的具体索引列表 |
| `meta.indices_hit` | []string | ES 实际命中数据的索引列表 |
| `meta.query_time_ms` | int | 查询耗时（毫秒） |
| `meta.clusters_queried` | []string | 被查询的集群列表 |

#### 分页说明

- 使用 `search_after` 实现游标分页，按 `@timestamp` 降序排序
- 多集群查询时，返回的游标指向特定集群，后续请求仅继续该集群的数据获取
- `next_cursor` 包含集群名、排序值和查询哈希，用于验证分页连续性

---

### 3. 时序数据查询

获取时序聚合数据（图表级别），支持多 metric 查询。**仅支持单集群查询**。

```
GET /data/timeseries
```

#### 请求参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `cluster` | string | 是 | - | 集群ID（仅支持单集群） |
| `job` | string | 是 | - | 原生 JobID 字符串，直接匹配 `job_info.NativeJobID` |
| `metric` | string | 是 | - | 指标名（支持逗号分隔多值，如 `cpu,mem`）。采集器自动从别名映射推断 |
| `interval` | string | 是 | - | 分桶粒度（`10s`/`1m`/`5m`/`1h`） |
| `from` | string | 是 | - | 起始时间 |
| `to` | string | 否 | `now` | 结束时间 |
| `agg` | string | 否 | `avg` | 聚合方式（`avg`/`max`/`min`/`sum`） |
| `by` | string | 否 | 空 | 分组维度（`host`/`collector`） |

> **注意**: 指定多个集群（如 `cluster=a,b`）将返回 400 错误。

#### 响应结构

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "metrics": ["cpu", "mem"],
    "interval": "1m",
    "timerange": {
      "from": "2026-05-03T09:00:00Z",
      "to": "2026-05-03T10:00:00Z"
    },
    "records": [
      { "metric": "cpu", "label": "htcondor03.ihep.ac.cn", "timestamp": "2026-05-03T09:00:00Z", "value": 45.2 },
      { "metric": "cpu", "label": "htcondor03.ihep.ac.cn", "timestamp": "2026-05-03T09:01:00Z", "value": 46.1 },
      { "metric": "cpu", "label": "htcondor03.ihep.ac.cn", "timestamp": "2026-05-03T09:02:00Z", "value": 44.8 },
      { "metric": "mem", "label": "htcondor03.ihep.ac.cn", "timestamp": "2026-05-03T09:00:00Z", "value": 1024000 },
      { "metric": "mem", "label": "htcondor03.ihep.ac.cn", "timestamp": "2026-05-03T09:01:00Z", "value": 1024500 },
      { "metric": "mem", "label": "htcondor03.ihep.ac.cn", "timestamp": "2026-05-03T09:02:00Z", "value": 1025000 }
    ],
    "stats": {
      "cpu": {
        "global_max": 98.5,
        "global_avg": 45.2
      },
      "mem": {
        "global_max": 2048000,
        "global_avg": 1024000
      }
    }
  },
  "meta": {
    "query_time_ms": 120,
    "clusters_queried": ["htcondor01"],
    "indices_hit": []
  }
}
```

> **注意**: `meta.indices_hit` 在时序查询中返回空数组，因为时序接口使用聚合查询，不返回具体命中的索引列表。

#### Record 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `metric` | string | 指标名 |
| `label` | string | 分组标签（按 host/collector 分组时），无分组时为空字符串 |
| `timestamp` | string | ISO8601 时间戳 |
| `value` | float64 | 指标值 |

---

### 4. 任务摘要查询

获取任务级统计摘要。**仅支持单集群查询**。

```
GET /data/summary
```

#### 请求参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `cluster` | string | 是 | - | 集群ID（仅支持单集群） |
| `job` | string | 是 | - | 原生 JobID 字符串，直接匹配 `job_info.NativeJobID` |
| `collectors` | string | 否 | 空 | 指定采集器（逗号分隔）。**注意：当前版本该参数未生效，始终使用默认采集器列表** |

> **注意**: 指定多个集群（如 `cluster=a,b`）将返回 400 错误。

#### 响应结构

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "job": "172.0",
    "cluster": "htcondor01",
    "time": {
      "first_seen": "2026-05-03T23:20:03+0800",
      "last_seen": "2026-05-04T00:30:00+0800",
      "duration_sec": 3600
    },
    "scope": {
      "hosts": [],
      "collectors": ["cpumem", "io", "net"],
      "samples_count": 0
    },
    "stats": {
      "cpu": {
        "max": 98.5, "avg": 45.2, "p99": 89.0
      },
      "mem": {
        "max_kb": 2048000, "avg_kb": 1024000
      },
      "io": {
        "total_bytes": 1073741824
      }
    }
  }
}
```

> **注意**: `scope.hosts` 当前版本返回空数组（主机名列表暂未采集）；`scope.samples_count` 当前版本固定为 0；`scope.collectors` 返回默认采集器列表而非实际命中数据的采集器。

---

### 5. Schema 发现

发现可用字段与集群元数据。

```
GET /schema
```

#### 请求参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `cluster` | string | 否 | 空 | 指定集群ID，空则返回全部 |
| `collector` | string | 否 | 空 | 指定采集器，空则返回全部 |

#### 响应结构

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "clusters": [
      {
        "id": "htcondor01",
        "endpoint": "https://omat4htc-es.ihep.ac.cn:443",
        "collectors": ["htcondor02@htcondor02.ihep.ac.cn"],
        "fields": ["cpu", "mem", "mem_peak", "name", "host", "io_bytes"],
        "indices": {
          "pattern": "{collector}_collector_{yyyy.MM.dd}",
          "retention_days": 30,
          "last_updated": ""
        }
      }
    ],
    "common_aliases": {
      "cpu": "data.summary.cpuPercent",
      "mem": "data.summary.mem_rss_kb"
    }
  }
}
```

> **注意**: `clusters[].collectors` 返回的是集群标签（Tags）列表，用于 ES routing，不是采集器类型列表。

---

### 6. Job 数据存在性检查

快速检查指定集群中是否存在某个 Job 的监控数据。使用 ES `_search` API（`size=0` + `min/max` 聚合）做轻量级查询，不返回文档体，对集群负担最小。

```
GET /data/check-job
```

#### 请求参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `cluster_name` | string | 是 | - | 集群名称（如 `htcondor01`） |
| `cluster_tag` | string | 否 | 空 | 集群标签（如 `htcondor02@htcondor02.ihep.ac.cn`），不传则不过滤 tag |
| `job_id` | string | 是 | - | 原生 JobID 字符串（如 Condor `"172.0"`、Slurm `"67890"`），匹配 `job_info.NativeJobID` |

#### 响应结构

**存在数据**:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "exists": true,
    "count": 606,
    "job_id": "172.0",
    "cluster": "htcondor01",
    "time": {
      "first_seen": "2026-05-03T23:20:03+08:00",
      "last_seen": "2026-05-04T00:30:00+08:00",
      "duration_sec": 3600
    }
  }
}
```

**不存在数据**:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "exists": false,
    "count": 0,
    "job_id": "999.0",
    "cluster": "htcondor01"
  }
}
```

#### 响应字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `exists` | bool | 是否存在数据 |
| `count` | int | 匹配的文档数量 |
| `job_id` | string | 请求的 JobID |
| `cluster` | string | 集群名称 |
| `time` | object | 数据时间范围，count>0 时返回 |
| `time.first_seen` | string | 最早数据时间戳 |
| `time.last_seen` | string | 最晚数据时间戳 |
| `time.duration_sec` | int | 数据跨度（秒） |

#### 性能说明

- 使用 ES `_search` API（`size=0` + `min/max` 聚合），不返回文档体，响应极快
- 查询条件仅匹配 keyword 字段（倒排索引），无需 `@timestamp` 范围过滤
- 自动穿越所有采集器索引（`cpumem_collector_*`、`io_collector_*`、`net_collector_*`）
- 当集群只有一个 tag 时，自动使用该 tag 做 routing 加速

#### 使用示例

```bash
# 检查 HTCondor Job 数据是否存在
curl "http://localhost:8080/data/check-job?cluster_name=htcondor01&job_id=172.0"

# 指定 cluster_tag 精确过滤
curl "http://localhost:8080/data/check-job?cluster_name=htcondor01&cluster_tag=htcondor02@htcondor02.ihep.ac.cn&job_id=172.0"

# 检查 Slurm Job 数据是否存在
curl "http://localhost:8080/data/check-job?cluster_name=INKSlurm&job_id=67890"
```

#### 错误响应

```json
{
  "code": 500,
  "message": "check job failed: cluster not found: unknown_cluster"
}
```

---

### 7. 触发作业采集

#### 7.1 自动查询触发

通过脚本查询集群获取节点信息，再触发采集。

```
POST /collect
```

#### 请求体

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_name` | string | 是 | 集群名称（如 `htcondor01`） |
| `cluster_tag` | string | 是 | 集群标签（如 `htcondor02@htcondor02.ihep.ac.cn`） |
| `job_id` | string | 是 | 原生 JobID 字符串（如 `"172.0"`），内部转为 uint64 发给 Agent |
| `collector` | string | 是 | 采集器名称，支持逗号分隔多值（如 `cpumem,io,net`）。每个值会自动追加 `_collector` 后缀后传给 Agent |

**请求示例（单采集器）**:

```json
{
  "cluster_name": "htcondor01",
  "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
  "job_id": "172.0",
  "collector": "cpumem"
}
```

**请求示例（多采集器）**:

```json
{
  "cluster_name": "htcondor01",
  "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
  "job_id": "172.0",
  "collector": "cpumem,io,net"
}
```

#### 响应结构

**成功响应** (HTTP 200):

```json
{
  "code": 0,
  "message": "collection triggered successfully on 1 node(s)",
  "data": {
    "status": "success",
    "cluster_name": "htcondor01",
    "job_id": "172.0",
    "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
    "node_type": "htcondor",
    "node_name": "htcondor03.ihep.ac.cn",
    "slot": "slot1@htcondor03",
    "job_info": {
      "cluster_id": "htcondor01",
      "job_id": "172.0",
      "node_name": "htcondor03.ihep.ac.cn",
      "slot": "slot1@htcondor03",
      "universe": "vanilla",
      "cmd": "/path/to/executable",
      "args": "",
      "status": "Running"
    },
    "agent_response": {
      "status": "queued",
      "task_id": "abc-123"
    },
    "message": "collection triggered successfully on 1 node(s)"
  }
}
```

**部分成功响应** (HTTP 207 Multi-Status):

```json
{
  "code": 0,
  "message": "collection triggered on 1/2 node(s)",
  "data": {
    "status": "partial_success",
    "cluster_name": "INKSlurm",
    "job_id": "67890",
    "cluster_tag": "slurm_cluster_2",
    "node_type": "slurm",
    "node_name": "node-01",
    "job_info": {
      "cluster_id": "INKSlurm",
      "job_id": "67890",
      "node_name": "node-01",
      "node_list": "node-01,node-02",
      "partition": "gpu",
      "job_state": "RUNNING"
    },
    "agent_response": [
      {
        "node_name": "node-01",
        "status": "queued",
        "task_id": "def-456"
      }
    ],
    "message": "collection triggered on 1/2 node(s)"
  }
}
```

#### 响应字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | string | 状态：`success`（全部成功）/ `partial_success`（部分成功）/ `failed`（全部失败） |
| `cluster_name` | string | 集群名称 |
| `job_id` | string | 原生 JobID 字符串 |
| `cluster_tag` | string | 集群标签 |
| `node_type` | string | 集群类型：`htcondor` / `slurm` |
| `node_name` | string | 主节点名称 |
| `slot` | string | HTCondor 槽位，`omitempty`（仅 HTCondor，成功时有值） |
| `job_info` | object/null | 作业信息（根据集群类型动态结构）。成功时返回对象，失败时返回 `null`。所有子字段始终存在（无数据时为空字符串） |
| `agent_response` | object/array | Agent 返回的响应（仅 `success`/`partial_success` 时返回），单节点为对象，多节点为数组 |
| `message` | string | 消息说明 |

#### 错误处理

该接口采用分层错误处理策略，即使部分步骤失败也会返回完整响应，调用方可以通过 `status` 字段判断整体状态。

**失败响应** (HTTP 200，但 `status` 为 `failed`):

```json
{
  "code": 0,
  "message": "script execution failed: cluster not found: unknown",
  "data": {
    "status": "failed",
    "cluster_name": "unknown",
    "job_id": "172.0",
    "cluster_tag": "unknown",
    "node_type": "unknown",
    "node_name": "",
    "job_info": null,
    "message": "script execution failed: cluster not found: unknown"
  }
}
```

> **注意**: `status` 为 `failed` 时，`agent_response` 不返回；`job_info` 为 `null`；`node_name` 为空字符串。

#### 重试机制

- **注册中心 Fallback**：当注册中心查询失败时，自动使用默认 URL 格式 `http://{node_name}:{default_port}`
- **Agent 重试**：采用指数退避策略，初始延迟 500ms，退避因子 2.0，最多重试 3 次
- **失败重试记录**：每次重试都会记录日志，便于排查问题

---

#### 7.2 直接触发采集

跳过脚本查询，用户直接提供节点信息触发采集。

```
POST /collect/direct
```

##### 请求体

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_name` | string | 是 | 集群名称（如 `htcondor01`） |
| `cluster_tag` | string | 是 | 集群标签 |
| `job_id` | string | 是 | 原生 JobID 字符串（如 `"172.0"`） |
| `collector` | string | 是 | 采集器名称，支持逗号分隔多值（如 `cpumem,io,net`）。每个值会自动追加 `_collector` 后缀后传给 Agent |
| `node` | string | 是 | 节点主机名 |
| `slot` | string | 条件必填 | HTCondor 槽位（如 `slot1@node01`），`htcondor` 集群必填，`slurm` 集群忽略 |

**请求示例（HTCondor，单采集器）**:
```json
{
  "cluster_name": "htcondor01",
  "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
  "job_id": "172.0",
  "collector": "cpumem",
  "node": "htcondor03.ihep.ac.cn",
  "slot": "slot1@htcondor03"
}
```

**请求示例（HTCondor，多采集器）**:
```json
{
  "cluster_name": "htcondor01",
  "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
  "job_id": "172.0",
  "collector": "cpumem,io,net",
  "node": "htcondor03.ihep.ac.cn",
  "slot": "slot1@htcondor03"
}
```

**请求示例（Slurm）**:
```json
{
  "cluster_name": "INKSlurm",
  "cluster_tag": "slurm_cluster_2",
  "job_id": "67890",
  "collector": "cpumem",
  "node": "gpu-node-05"
}
```

##### 响应结构

**成功响应（HTCondor）** (HTTP 200):
```json
{
  "code": 0,
  "message": "collection triggered successfully",
  "data": {
    "status": "success",
    "cluster_name": "htcondor01",
    "job_id": "172.0",
    "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
    "node_type": "htcondor",
    "node_name": "htcondor03.ihep.ac.cn",
    "slot": "slot1@htcondor03",
    "job_info": {
      "cluster_id": "htcondor01",
      "job_id": "172.0",
      "node_name": "htcondor03.ihep.ac.cn",
      "slot": "slot1@htcondor03",
      "universe": "",
      "cmd": "",
      "args": "",
      "status": ""
    },
    "agent_response": {
      "status": "queued",
      "task_id": "abc-123"
    },
    "message": "collection triggered successfully"
  }
}
```

**失败响应（参数错误）** (HTTP 400):
```json
{
  "code": 400,
  "message": "slot is required for htcondor cluster"
}
```

**失败响应（Agent 错误）** (HTTP 200，但 `status` 为 `failed`):
```json
{
  "code": 0,
  "message": "agent request failed: ...",
  "data": {
    "status": "failed",
    "cluster_name": "htcondor01",
    "job_id": "172.0",
    "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
    "node_type": "htcondor",
    "node_name": "htcondor03.ihep.ac.cn",
    "slot": "slot1@htcondor03",
    "job_info": {
      "cluster_id": "htcondor01",
      "job_id": "172.0",
      "node_name": "htcondor03.ihep.ac.cn",
      "slot": "slot1@htcondor03",
      "universe": "",
      "cmd": "",
      "args": "",
      "status": ""
    },
    "message": "agent request failed: ..."
  }
}
```

##### 与 7.1 自动触发对比

| 维度 | 自动触发 | 直接触发 |
|------|----------|----------|
| 依赖外部脚本 | 是 | 否 |
| 节点信息获取 | 自动查询集群 | 用户直接提供 |
| 多节点支持 | 支持（脚本返回 NodeList） | 单节点 |
| 适用场景 | 知道 JobID 但不知道节点 | 知道节点和 JobID |

---

## 字段别名映射

| 别名 | ES 字段路径 | 类型 | 所属采集器 |
|------|-------------|------|------------|
| `cpu` | `data.summary.cpuPercent` | float | cpumem |
| `mem` | `data.summary.mem_rss_kb` | long | cpumem |
| `mem_peak` | `data.summary.mem_peak_rss_kb` | long | cpumem |
| `name` | `data.summary.name.keyword` | keyword | cpumem |
| `host` | `hostname.keyword` | keyword | (通用) |
| `io_bytes` | `data.summary.read_bytes` | long | io |
| `time` | `@timestamp` | date | (通用) |

---

## 使用示例

### cURL 示例

```bash
# Condor 格式 JobID 查询（带 "."）
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&from=now-2d&collector=cpumem&fields=cpu,mem,host"

# Slurm 格式 JobID 查询
curl "http://localhost:8080/data/raw?cluster=INKSlurm&job=67890&from=now-1h&fields=cpu,mem,host"

# 带 cluster_tag 精确查询
curl "http://localhost:8080/data/raw?cluster=htcondor01:htcondor02@htcondor02.ihep.ac.cn&job=172.0&from=now-1h"

# 查询 Job 完整时间范围的原始数据（自动发现起止时间）
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&full_range=true&size=100&fields=cpu,mem"

# 查询完整范围但保留嵌套结构（不扁平化）
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&full_range=true&flatten=false&fields=data.summary.cpuPercent"

# 查 CPU 和内存曲线（1 分钟粒度，按主机分组）
curl "http://localhost:8080/data/timeseries?cluster=htcondor01&job=172.0&metric=cpu,mem&interval=1m&from=now-1h&by=host"

# 查任务摘要
curl "http://localhost:8080/data/summary?cluster=htcondor01&job=172.0"

# 发现可用字段
curl "http://localhost:8080/schema?cluster=htcondor01"

# 多集群查询
curl "http://localhost:8080/data/raw?cluster=htcondor01,INKSlurm&job=172.0&from=now-1h&fields=cpu"

# 通配所有集群
curl "http://localhost:8080/data/raw?cluster=*&job=172.0&from=now-1h&fields=cpu"

# 触发单采集器采集
curl -X POST "http://localhost:8080/collect" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"htcondor01","cluster_tag":"htcondor02@htcondor02.ihep.ac.cn","job_id":"172.0","collector":"cpumem"}'

# 触发多采集器采集
curl -X POST "http://localhost:8080/collect" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"htcondor01","cluster_tag":"htcondor02@htcondor02.ihep.ac.cn","job_id":"172.0","collector":"cpumem,io,net"}'

# 直接触发多采集器采集（跳过脚本查询）
curl -X POST "http://localhost:8080/collect/direct" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"htcondor01","cluster_tag":"htcondor02@htcondor02.ihep.ac.cn","job_id":"172.0","collector":"cpumem,io","node":"htcondor03.ihep.ac.cn","slot":"slot1@htcondor03"}'
```

### 分页查询示例

```bash
# 第一次请求
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&from=now-2d&size=100"

# 使用游标获取下一页
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&from=now-2d&size=100&cursor=eyJ0aW1lIjoxN..."
```

### Python 示例

```python
import requests

BASE_URL = "http://localhost:8080"

# 查询原始数据
def query_raw(cluster, job, from_time=None, fields=None, size=100, full_range=False):
    params = {
        "cluster": cluster,
        "job": job,
        "size": size
    }
    if full_range:
        params["full_range"] = "true"
    else:
        params["from"] = from_time
    if fields:
        params["fields"] = ",".join(fields)

    resp = requests.get(f"{BASE_URL}/data/raw", params=params)
    return resp.json()

# 查询 Job 完整数据
def query_raw_full(cluster, job, fields=None, size=100):
    return query_raw(cluster, job, full_range=True, fields=fields, size=size)

# 查询时序数据（多 metric）
def query_timeseries(cluster, job, metrics, interval, from_time, by=None):
    params = {
        "cluster": cluster,
        "job": job,
        "metric": ",".join(metrics),
        "interval": interval,
        "from": from_time
    }
    if by:
        params["by"] = by

    resp = requests.get(f"{BASE_URL}/data/timeseries", params=params)
    return resp.json()

# 使用示例
if __name__ == "__main__":
    # Condor job ID "172.0"
    # 查询 CPU 和内存数据
    result = query_raw("htcondor01", "172.0", "now-2d", fields=["cpu", "mem", "host"])
    print(f"Total records: {result['data']['pagination']['total']}")

    # 查询时序曲线
    ts_result = query_timeseries("htcondor01", "172.0", ["cpu", "mem"], "1m", "now-1h", by="host")
    for record in ts_result["data"]["records"][:5]:
        print(f"{record['metric']} @ {record['label']} {record['timestamp']}: {record['value']}")

    # Slurm job ID "67890"
    result = query_raw("INKSlurm", "67890", "now-1h", fields=["cpu", "mem"])
    print(f"Slurm job records: {result['data']['pagination']['total']}")
```

### Grafana JSON API 数据源配置

1. 添加 JSON API 数据源
2. 配置 URL: `http://localhost:8080`
3. 创建 Dashboard，使用以下查询：
   - Raw: `/data/raw?cluster=$cluster&job=$job&from=${__from:date:iso}&to=${__to:date:iso}&fields=cpu,mem`
   - Timeseries: `/data/timeseries?cluster=$cluster&job=$job&metric=cpu&interval=${__interval}&from=${__from:date:iso}`

> **注意**: Grafana 变量 `$job` 需设为字符串类型（如 `"172.0"`），采集器由 metric 别名映射自动推断。

---

---

## JobID 匹配机制（v1.1 更新）

### NativeJobID 统一匹配

所有查询接口的 `job` 参数统一使用原生 JobID 字符串，直接匹配 ES 文档中 `job_info.NativeJobID.keyword` 字段。

| 调度器 | 前端传值示例 | ES 字段 | 匹配方式 |
|--------|------------|---------|---------|
| Condor | `"172.0"` (cluster_id.proc_id) | `job_info.NativeJobID` | keyword 精确匹配 |
| Slurm | `"67890"` (纯数字) | `job_info.NativeJobID` | keyword 精确匹配 |

### 旧字段兼容

ES 文档中仍保留以下字段用于旧数据兼容：
- `job_info.JobID` — Slurm 格式整数
- `job_info.sub_attr.cluster_id` + `job_info.sub_attr.proc_id` — Condor 拆解格式

响应中 `record.job` 字段优先返回 `NativeJobID`（string），旧数据回退到 `JobID`（int64）。

### 迁移说明

从 v1.0 到 v1.1 的主要变化：
- `job` 参数类型从 `int64` 改为 `string`（前端传 `"172.0"` 而非 `172`）
- 不再需要区分 Condor/Slurm 格式，统一通过 `NativeJobID` 匹配
- 采集端需确保写入 `job_info.NativeJobID` 字段（旧字段保留不变）

---

## 错误响应示例

```json
{
  "code": 400,
  "message": "invalid request: Key: 'RawQueryRequest.Cluster' Error:Field validation for 'Cluster' failed on the 'required' tag"
}
```

```json
{
  "code": 400,
  "message": "from parameter is required when full_range is not set"
}
```

```json
{
  "code": 400,
  "message": "timeseries query only supports single cluster, please specify one cluster"
}
```

```json
{
  "code": 500,
  "message": "query failed: cluster not found: unknown_cluster"
}
```
