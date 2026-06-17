# 管理 API 接口规范

> **English**: [management-api.md](./management-api.md)

JobLens-TAP 启动时通过 `TAP_MANAGEMENT_API_URL` 环境变量指向的管理 API 获取集群元数据。本文档定义管理 API 需要实现的接口规范。

---

## 1. 获取集群 Scheme

### 请求

```
GET {TAP_MANAGEMENT_API_URL}/api/clusters/scheme
```

**请求头**:

| Header | 值 | 说明 |
|--------|-----|------|
| `Accept` | `application/json` | 要求 JSON 响应 |

**超时**: 10 秒

### 响应体结构

#### 顶层结构

```json
{
  "clusters": [ ... ],
  "total": 3
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `clusters` | array | 是 | 集群元数据列表 |
| `total` | int | 是 | 集群总数 |

#### clusters[] 元素结构

```json
{
  "cluster_name": "htcondor01",
  "cluster_type": "condor",
  "tags": ["htcondor02@htcondor02.ihep.ac.cn"],
  "alias": "SZ-HTCondor",
  "enabled": true,
  "extra": {
    "es_url": "https://es-cluster.example.com:9200",
    "es_username": "elastic",
    "es_password": "changeme",
    "script_path": "/opt/joblens/scripts/query_htcondor.sh",
    "default_node_port": 8080
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_name` | string | 是 | 集群唯一标识，TAP 内部以此作为 cluster ID |
| `cluster_type` | string | 是 | 集群类型：`"condor"`（HTCondor）或 `"slurm"`（Slurm） |
| `tags` | string[] | 否 | ES routing 标签列表。查询时用于指定具体 tag 做路由加速；单 tag 集群会自动使用 |
| `alias` | string | 否 | 显示别名，在 Schema 发现接口中优先作为 ID 展示 |
| `enabled` | bool | 是 | 是否启用。`false` 的集群会被管理器过滤掉 |
| `extra` | object | 是 | 扩展字段，详见下方 |

#### extra 扩展字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `es_url` | string | 是 | - | ES 集群端点 URL（含协议和端口）。启动时会检查该字段，为空则跳过该集群 |
| `es_username` | string | 否 | - | ES Basic Auth 用户名 |
| `es_password` | string | 否 | - | ES Basic Auth 密码 |
| `script_path` | string | 否 | - | 采集触发脚本路径（`POST /collect` 自动查询节点时使用） |
| `default_node_port` | int | 否 | `8080` | 注册中心回退时的默认 Agent 端口 |

#### 字段拉平（NormalizeExtra）

TAP 在加载后调用 `NormalizeExtra()` 将 `extra` 中的关键字段提升到 `ClusterMeta` 结构体顶层，便于内部使用。调用方无需关心此步骤，只需确保 `extra` 中包含上述字段即可。

---

## 2. 完整示例

### 请求

```bash
curl -H "Accept: application/json" \
  "https://management-api.example.com/api/clusters/scheme"
```

### 响应

```json
{
  "clusters": [
    {
      "cluster_name": "htcondor01",
      "cluster_type": "condor",
      "tags": ["htcondor02@htcondor02.ihep.ac.cn"],
      "alias": "SZ-HTCondor",
      "enabled": true,
      "extra": {
        "es_url": "https://es-htcondor01.example.com:9200",
        "es_username": "elastic",
        "es_password": "secret123",
        "script_path": "/opt/joblens/scripts/query_htcondor.sh",
        "default_node_port": 8080
      }
    },
    {
      "cluster_name": "INKSlurm",
      "cluster_type": "slurm",
      "tags": ["slurm_cluster_1", "slurm_cluster_2"],
      "alias": "BJ-Slurm",
      "enabled": true,
      "extra": {
        "es_url": "https://es-slurm.example.com:9200",
        "es_username": "elastic",
        "es_password": "secret456",
        "script_path": "/opt/joblens/scripts/query_slurm.sh",
        "default_node_port": 9090
      }
    },
    {
      "cluster_name": "offline-cluster",
      "cluster_type": "condor",
      "tags": [],
      "alias": "",
      "enabled": false,
      "extra": {
        "es_url": "https://es-offline.example.com:9200"
      }
    }
  ],
  "total": 3
}
```

> **注意**: `offline-cluster` 的 `enabled` 为 `false`，TAP 不会为其创建 ES 客户端或处理查询请求。实际有效的集群数为 2。

---

## 3. TAP 消费行为

| 阶段 | 行为 |
|------|------|
| 启动 | `InitialFetch()` 阻塞拉取，失败则退出（`os.Exit(1)`） |
| 后台刷新 | 按 `TAP_MANAGEMENT_CACHE_TTL`（默认 5m）周期刷新 |
| 惰性加载 | 查询未知集群时触发按需拉取（30s 最小间隔防抖） |
| 别名匹配 | 集群名查找失败时，尝试通过 `alias` 字段匹配 |
| 过滤 | `es_url` 为空的集群被跳过；`enabled=false` 的集群被忽略 |

## 4. 错误处理

| 场景 | 行为 |
|------|------|
| API 不可达 | 启动时直接退出；后台刷新记录 WARN 日志 |
| HTTP 非 200 | 记录错误日志和响应体，跳过本次刷新 |
| JSON 解析失败 | 记录错误日志，跳过本次刷新 |
| 某集群 `es_url` 为空 | 跳过该集群，记录 WARN 日志 |
