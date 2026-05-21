**计算集群可观测性数据中台架构设计报告书**

**版本**：v1.0  
**日期**：2026-04-03  
**状态**：设计定稿（Design Finalized）

---

## 1. 项目概述

### 1.1 背景
现有计算集群可观测性系统采用多采集器架构（Process/IO/Net），数据存储于Elasticsearch（ES）。由于存在多计算集群且JobID非全局唯一，需构建数据中台以屏蔽物理存储差异，提供统一数据出口。

### 1.2 设计约束
- **纯粹数据管道**：无权限验证、无业务逻辑、无状态缓存
- **极简交互**：拒绝复杂DSL，采用RESTful Query Parameter模式
- **透明路由**：自动处理Cluster→Index的映射，支持多集群并行查询

### 1.3 目标
提供不超过3个核心数据端点，使客户端能在10秒内构造有效请求，直接获取可用于展示的结构化数据。

---

## 2. 架构设计原则

| 原则 | 说明 |
|------|------|
| **无状态（Stateless）** | 服务不存储会话、缓存或Job映射，任何实例均可处理任意请求 |
| **透明路由（Transparent Routing）** | 客户端只需知晓`cluster_id`，中台自动解析为物理Index Pattern |
| **字段别名（Field Aliasing）** | 客户端使用`cpu/mem/host`等语义别名，中台映射至ES嵌套字段路径 |
| **扁平化输出（Flattened Output）** | 响应结构去除ES元数据（`_index`, `_score`），保留纯净业务字段 |
| **渐进式查询（Progressive Query）** | 提供Raw（原始）、Timeseries（时序）、Summary（聚合）三级抽象 |

---

## 3. 接口规范（API Specification）

### 3.1 端点总览

| 端点 | 方法 | 用途 | 复杂度 |
|------|------|------|--------|
| `/data/raw` | GET | 拉取原始采样点（日志级） | O(n) |
| `/data/timeseries` | GET | 获取时序聚合（图表级） | O(n/buckets) |
| `/data/summary` | GET | 获取任务级统计摘要（聚合级） | O(1) |
| `/schema` | GET | 发现可用字段与集群元数据 | - |

### 3.2 详细规范

#### 3.2.1 原始数据接口（Raw Logs）
**端点**：`GET /data/raw`

**请求参数**：
```properties
cluster      required  string    集群ID，支持多值（sz01,bj02）或通配（*）
job          required  long      JobID（不提供则返回该集群全部数据，慎用）
from         required  string    起始时间（ISO8601或相对时间：now-1h, 1d）
to           optional  string    结束时间（默认：now）
collector    optional  string    采集器类型（process/io/net），默认全部
fields       optional  string    字段白名单（逗号分隔），支持别名：cpu,mem,name,io,host,time
size         optional  integer   单批次数量（默认100，最大10000）
cursor       optional  string    分页游标（首次请求省略，后续传递next_cursor）
flatten      optional  boolean   是否扁平化嵌套结构（默认true）
```

**响应结构**：
```json
{
  "records": [
    {
      "cluster": "sz01",
      "collector": "process",
      "time": "2026-04-02T22:15:00Z",
      "host": "gpu-node-05",
      "job": 12345,
      "cpu": 45.2,
      "mem": 1024000,
      "name": "python-train"
    }
  ],
  "pagination": {
    "has_more": true,
    "next_cursor": "eyJ0aW1lIjoxNz...",
    "returned": 100,
    "total": 2500
  },
  "meta": {
    "query_time_ms": 45,
    "clusters_queried": ["sz01"],
    "indices_hit": ["obs_sz01_process_2026.04.02"]
  }
}
```

**字段别名映射表**（中台维护）：
| 别名 | ES字段路径 | 类型 |
|------|-----------|------|
| `cpu` | `data.process_data.cpuPercent` | float |
| `mem` | `data.process_data.mem_rss_kb` | long |
| `mem_peak` | `data.process_data.mem_peak_rss_kb` | long |
| `name` | `data.process_data.name.keyword` | keyword |
| `host` | `hostname.keyword` | keyword |
| `io_bytes` | `data.io_stats.bytes` | long |
| `io_file` | `data.io_stats.filename.keyword` | keyword |
| `time` | `@timestamp` | date |

#### 3.2.2 时序聚合接口（Time Series）
**端点**：`GET /data/timeseries`

**请求参数**：
```properties
cluster      required  string    集群ID（建议单集群，多集群会增加对齐复杂度）
job          required  long      JobID
metric       required  string    指标名（cpu/mem/io_bytes等，使用上述别名）
interval     required  string    分桶粒度（10s, 1m, 5m, 1h）
from         required  string    起始时间
to           optional  string    结束时间
agg          optional  string    聚合方式：avg/max/min/sum（默认avg）
by           optional  string    分组维度：host/collector（可选，默认不分组）
```

**响应结构**：
```json
{
  "metric": "cpu",
  "interval": "1m",
  "timerange": {
    "from": "2026-04-02T22:00:00Z",
    "to": "2026-04-02T23:00:00Z"
  },
  "series": [
    {
      "label": "gpu-node-05",
      "timestamps": ["22:00", "22:01", "22:02", "..."],
      "values": [45.2, 46.1, 44.8, "..."]
    }
  ],
  "stats": {
    "global_max": 98.5,
    "global_avg": 45.2
  }
}
```

#### 3.2.3 任务摘要接口（Job Summary）
**端点**：`GET /data/summary`

**请求参数**：
```properties
cluster      required  string    集群ID
job          required  long      JobID
collectors   optional  string    指定采集器（默认全部），影响stats返回的字段
```

**响应结构**：
```json
{
  "job": 12345,
  "cluster": "sz01",
  "time": {
    "first_seen": "2026-04-02T22:00:00Z",
    "last_seen": "2026-04-02T23:00:00Z",
    "duration_sec": 3600
  },
  "scope": {
    "hosts": ["gpu-node-05", "gpu-node-06"],
    "collectors": ["process", "io"],
    "samples_count": 720
  },
  "stats": {
    "process": {
      "cpu": {"max": 98.5, "avg": 45.2, "p99": 89.0},
      "mem": {"max_kb": 2048000, "avg_kb": 1024000}
    },
    "io": {
      "total_bytes": 1073741824,
      "top_files": [
        {"file": "/data/model.ckpt", "bytes": 536870912}
      ]
    }
  }
}
```

#### 3.2.4 Schema发现接口（Metadata Discovery）
**端点**：`GET /schema`

**请求参数**：
```properties
cluster      optional  string    指定集群ID（不提供则返回全部集群）
collector    optional  string    指定采集器
```

**响应结构**：
```json
{
  "clusters": [
    {
      "id": "sz01",
      "endpoint": "http://es-sz01:9200",
      "collectors": ["process", "io", "net"],
      "fields": ["cpu", "mem", "io_bytes", "host"],
      "indices": {
        "pattern": "obs_sz01_{collector}_{yyyy.MM.dd}",
        "retention_days": 30,
        "last_updated": "2026-04-03T00:00:00Z"
      }
    }
  ],
  "common_aliases": {
    "cpu": "data.process_data.cpuPercent",
    "mem": "data.process_data.mem_rss_kb"
  }
}
```

---

## 4. 存储层规范（Storage Layer）

### 4.1 索引命名约定
**强制格式**：`obs_{cluster_id}_{collector_type}_{yyyy.MM.dd}`

**示例**：
- `obs_sz01_process_2026.04.02`
- `obs_bj02_io_2026.04.02`
- `obs_sz01_net_2026.04.02`

**说明**：
- `cluster_id`：小写字母+数字，长度≤10
- `collector_type`：`process` | `io` | `net`
- 日期格式固定为`yyyy.MM.dd`，与ES Index Lifecycle Management兼容

### 4.2 文档字段规范
每个文档必须包含以下**路由字段**（供中台识别与过滤）：
```json
{
  "@timestamp": "2026-04-02T22:15:00Z",
  "cluster_id": "sz01",              // 新增字段，用于索引路由
  "collector": "process",            // 采集器标识
  "hostname": "gpu-node-05",
  "job_info": {
    "JobID": 12345
  }
}
```

### 4.3 集群注册配置（静态配置）
中台通过启动配置文件或环境变量感知集群，**不依赖动态服务发现**：

```yaml
# config.yaml
clusters:
  sz01:
    es_url: http://localhost:9200
    index_prefix: obs_sz01
    collectors: [process, io, net]
    timezone: Asia/Shanghai
  
  bj02:
    es_url: http://bj02-es.internal:9200
    index_prefix: obs_bj02
    collectors: [process, io]

query_defaults:
  max_size: 10000
  default_interval: 1m
  max_time_range_days: 7  # 限制单次查询范围，防止过载
```

---

## 5. 开发实施路线图（Roadmap）

### Phase 1：核心数据出口（Week 1-2）
**目标**：实现Raw接口，支持单集群查询

**任务清单**：
1. 实现Index路由逻辑（cluster_id → index_pattern解析）
2. 实现时间解析器（now-1h等相对时间转换）
3. 实现字段别名映射系统
4. 实现ES查询构造器（将简单参数转为ES DSL）
5. 实现响应扁平化与清洗（去除ES元数据，注入cluster字段）
6. 实现基础分页（Search After/Cursor）

**验收标准**：
```bash
curl "http://localhost:8080/data/raw?cluster=sz01&job=12345&from=now-1h&fields=cpu,mem"
# 返回有效JSON，包含cpu/mem数值，cluster字段为sz01
```

### Phase 2：聚合与分析（Week 3）
**目标**：实现Timeseries与Summary接口

**任务清单**：
1. 实现时间分桶聚合逻辑（Date Histogram）
2. 实现分组维度支持（by=host）
3. 实现任务级统计计算（Duration/Max/Avg/P99）
4. 实现多指标联合查询优化（如Summary同时查process+io）

**验收标准**：
- Timeseries接口返回可直接绘图的数组结构
- Summary接口在Job结束时能正确计算duration

### Phase 3：多集群联邦（Week 4）
**目标**：支持多集群并行查询与结果合并

**任务清单**：
1. 实现异步并行查询器（Async HTTP Client）
2. 实现结果流式合并（保持时间序）
3. 实现跨集群分页协调（各集群独立cursor，中台聚合）
4. 实现集群通配查询（cluster=*）

**验收标准**：
```bash
curl "http://localhost:8080/data/raw?cluster=sz01,bj02&job=12345&fields=cluster,cpu"
# 返回数据包含cluster字段区分来源，且按时间排序
```

### Phase 4：Schema与工具（Week 5）
**目标**：Schema发现与开发辅助

**任务清单**：
1. 实现Schema端点（读取ES Mapping生成字段列表）
2. 实现别名自动补全建议
3. 编写客户端SDK示例（Python/Bash）
4. 编写接口文档与示例集合

**交付物**：
- 在线Schema浏览器
- 命令行查询工具（wrapper脚本）

### Phase 5：优化与逃生舱（Week 6）
**目标**：性能优化与高级功能

**任务清单**：
1. 实现查询结果压缩（gzip）
2. 实现原生ES查询逃生舱（POST /query/native，透传ES DSL）
3. 实现简单查询缓存（可选，仅对Summary端点，TTL=60s）
4. 压力测试与连接池调优

---

## 6. 技术实现要点

### 6.1 查询构造示例（Raw端点）
输入参数：
```
cluster=sz01, job=12345, from=now-1h, fields=cpu,mem
```

构造逻辑：
1. **Index解析**：`sz01` + `process` → `obs_sz01_process_2026.04.02,obs_sz01_process_2026.04.01`（跨日期）
2. **字段映射**：`cpu`→`data.process_data.cpuPercent`, `mem`→`data.process_data.mem_rss_kb`
3. **ES DSL生成**：
```json
{
  "_source": ["data.process_data.cpuPercent", "data.process_data.mem_rss_kb", "@timestamp", "hostname"],
  "query": {
    "bool": {
      "filter": [
        {"term": {"job_info.JobID": 12345}},
        {"range": {"@timestamp": {"gte": "2026-04-02T23:00:00Z"}}},
        {"term": {"cluster_id": "sz01"}}
      ]
    }
  },
  "sort": [{"@timestamp": "desc"}],
  "size": 100
}
```

### 6.2 多集群查询策略
**并行模式**：对每个集群发起独立HTTP请求，使用`asyncio.gather`或类似机制并发执行。

**结果合并**：
- **Raw**：按`@timestamp`归并排序（K-way merge）
- **Timeseries**：各集群独立分桶，中台不做跨集群聚合（保持简单）
- **Summary**：返回各集群独立统计对象，不合并数值

### 6.3 游标分页实现
采用ES的`search_after`机制，游标编码为Base64的JSON：
```json
{
  "cluster": "sz01",
  "search_after": ["2026-04-02T22:00:00Z", 12345],
  "query_hash": "sha256_of_query_params"
}
```
中台不存储任何状态，游标即状态。

---

## 7. 风险与回退方案

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| **ES集群不可达** | 单集群查询失败 | 多集群查询时，部分失败返回部分数据+错误标记，不整体失败 |
| **JobID冲突（跨集群）** | 查询返回多个同名Job数据 | 客户端通过`cluster`参数精确路由；返回数据中强制包含`cluster`字段用于区分 |
| **大数据量查询OOM** | 中台内存溢出 | 限制`size`参数上限（10,000）；强制使用cursor分页；禁止深度分页（from+size）|
| **字段别名歧义** | 不同采集器同名字段冲突（如io.bytes vs net.bytes） | 别名加入采集器前缀（`io_bytes`/`net_bytes`）；Schema端点明确提示 |
| **时间范围过大** | 查询超时 | 限制单次查询最大时间跨度（如7天），超范围返回400错误 |

**逃生舱保留**：若极简接口无法满足复杂查询需求，保留端点：
```http
POST /query/native
Body: <原生ES Query DSL>
```
直接透传至指定集群ES，返回原始ES响应（不做扁平化）。

---

## 8. 交付物清单

1. **中台服务代码**：Go/Java/Python实现，提供上述4个端点
2. **配置文件模板**：`config.yaml`示例与配置项说明
3. **客户端示例**：
   - Bash/curl示例（运维快速排查）
   - Python脚本（数据分析）
   - Grafana数据源配置（JSON API数据源）
4. **接口文档**：OpenAPI/Swagger规范文件
5. **部署文档**：Dockerfile与Kubernetes部署YAML



**附录：快速参考卡（Quick Reference Card）**

```bash
# 查最近1小时原始数据
GET /data/raw?cluster=sz01&job=12345&from=now-1h&fields=cpu,mem,host

# 查CPU曲线（1分钟粒度）
GET /data/timeseries?cluster=sz01&job=12345&metric=cpu&interval=1m&from=now-1h

# 查任务摘要
GET /data/summary?cluster=sz01&job=12345

# 发现可用字段
GET /schema?cluster=sz01
```