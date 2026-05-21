# 集群查询脚本说明

## 概述

本项目支持通过脚本查询集群作业信息，用于触发 Metrics 采集。目前支持 HTCondor 和 Slurm 两种集群类型。

## 脚本要求

### 通用要求

1. **位置**: 脚本路径需要在配置文件中通过 `script_path` 指定
2. **参数**: 脚本接收一个参数：`job_id`
3. **输出**: 必须输出 JSON 格式的数据到 stdout
4. **退出码**: 成功返回 0，失败返回非 0

### HTCondor 脚本

#### 文件名
`query_htcondor_job.sh`

#### 输出格式

```json
{
  "node_name": "compute-01.example.com",
  "slot": "slot1@compute-01.example.com",
  "job_info": {
    "cluster_id": "htcondor-01",
    "job_id": 12345,
    "node_name": "compute-01.example.com",
    "slot": "slot1@compute-01.example.com",
    "universe": "vanilla",
    "cmd": "/path/to/executable",
    "args": "arg1 arg2",
    "status": "Running"
  }
}
```

#### 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `node_name` | string | 是 | 作业所在节点的名称 |
| `slot` | string | 是 | HTCondor 特有：作业所在的槽位 |
| `job_info` | object | 是 | 作业详细信息 |

#### job_info 字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_id` | string | 是 | 集群 ID |
| `job_id` | int | 是 | 作业 ID |
| `node_name` | string | 是 | 节点名称 |
| `slot` | string | 是 | 槽位 |
| `universe` | string | 否 | HTCondor universe 类型 |
| `cmd` | string | 否 | 执行命令 |
| `args` | string | 否 | 命令参数 |
| `status` | string | 否 | 作业状态 |

#### 实现参考

使用 `condor_q` 或 `condor_history` 查询作业信息：

```bash
#!/bin/bash
JOB_ID=$1

# 查询作业所在的节点和槽位
NODE_INFO=$(condor_q -format "%s " Machine -format "%s\n" RemoteSlotID -constraint "ClusterId==${JOB_ID}")

# 查询作业详细信息
JOB_DETAILS=$(condor_q -long -constraint "ClusterId==${JOB_ID}")

# 解析并输出 JSON
# ...
```

### Slurm 脚本

#### 文件名
`query_slurm_job.sh`

#### 输出格式

```json
{
  "node_name": "node-01.example.com",
  "job_info": {
    "cluster_id": "slurm-01",
    "job_id": 67890,
    "node_name": "node-01.example.com",
    "node_list": "node-01,node-02",
    "partition": "gpu",
    "job_state": "RUNNING"
  }
}
```

#### 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `node_name` | string | 是 | 作业所在主节点的名称 |
| `job_info` | object | 是 | 作业详细信息 |

#### job_info 字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_id` | string | 是 | 集群 ID |
| `job_id` | int | 是 | 作业 ID |
| `node_name` | string | 是 | 主节点名称 |
| `node_list` | string | 否 | 所有节点列表 |
| `partition` | string | 否 | 分区名称 |
| `job_state` | string | 否 | 作业状态 |

#### 实现参考

使用 `squeue` 或 `scontrol` 查询作业信息：

```bash
#!/bin/bash
JOB_ID=$1

# 方法1: 使用 scontrol
JOB_INFO=$(scontrol show job ${JOB_ID})
NODE_NAME=$(echo "$JOB_INFO" | grep -oP "Nodes=\K[^ ]+")
JOB_STATE=$(echo "$JOB_INFO" | grep -oP "JobState=\K[^ ]+")
PARTITION=$(echo "$JOB_INFO" | grep -oP "Partition=\K[^ ]+")

# 方法2: 使用 squeue
# squeue -j ${JOB_ID} -o "%.20i %.9P %.50j %.8u %.2t %.10M %.6D %R"
```

## 注册中心 API

### 端点

```
GET /services/by-host/{hostname}
```

### 请求参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `hostname` | string | 主机名（URL 路径参数） |

### 响应格式

返回服务列表：

```json
[
  {
    "service_id": "agent-001",
    "host": "compute-01.example.com",
    "port": 8080,
    "base_url": "http://compute-01.example.com:8080",
    "name": "joblens-agent",
    "version": "1.0.0",
    "mode": "default",
    "role": "worker",
    "registered_at": "2026-04-07T10:00:00Z",
    "last_heartbeat": "2026-04-07T11:30:00Z",
    "status": "healthy",
    "directory_path": "/joblens/services/agent-001",
    "metadata": {}
  }
]
```

### Fallback 策略

当注册中心不可用时，系统会自动使用默认 URL 格式：

```
http://{node_name}:{default_port}
```

其中 `default_port` 从集群配置中的 `default_node_port` 字段获取。

## 配置示例

```bash
export TAP_CLUSTERS='[
  {
    "id": "htcondor-01",
    "type": "htcondor",
    "script_path": "/opt/scripts/query_htcondor_job.sh",
    "default_node_port": 8080,
    "es_url": "http://es:9200"
  },
  {
    "id": "slurm-01",
    "type": "slurm",
    "script_path": "/opt/scripts/query_slurm_job.sh",
    "default_node_port": 9090,
    "es_url": "http://es:9200"
  }
]'
export TAP_SERVICE_REGISTRY_URL="http://service-registry:8080"
```

## 测试脚本

### HTCondor 脚本测试

```bash
./scripts/query_htcondor_job.sh.example 12345
```

### Slurm 脚本测试

```bash
./scripts/query_slurm_job.sh.example 67890
```

## 错误处理

脚本在遇到错误时应：

1. 输出错误信息到 stderr
2. 返回非 0 退出码

例如：

```bash
if [ -z "$NODE_NAME" ]; then
    echo "Error: Failed to get node name for job $JOB_ID" >&2
    exit 1
fi
```

## 安全建议

1. **权限控制**: 脚本应只具有必要的执行权限
2. **输入验证**: 验证 JobID 参数的有效性
3. **敏感信息**: 避免在脚本中硬编码密码或密钥
4. **日志记录**: 记录脚本执行日志以便排查问题