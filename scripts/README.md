# 集群查询脚本说明

## 概述

本项目支持通过脚本查询集群作业信息，用于触发 Metrics 采集。目前支持 HTCondor 和 Slurm 两种集群类型。

## 脚本要求

### 通用要求

1. **位置**: 脚本路径需要在集群配置中通过 `script_path` 指定
2. **参数**: 脚本接收两个参数：`job_id` 和 `cluster_tag`
   - `$1` (`job_id`): 原生 JobID 字符串（如 Condor `"172.0"`、Slurm `"67890"`）
   - `$2` (`cluster_tag`): 集群标签（如 `"htcondor02@example.com"`），可选，用于过滤特定队列/分区
3. **输出**: 必须输出 JSON 格式的数据到 stdout
4. **退出码**: 成功返回 0，失败返回非 0。错误信息可通过 stdout JSON 中的 `error` 字段返回

### 输出格式

```json
{
  "job_id": "string",
  "nodes": ["node1", "node2"],
  "slot": "slot1@node1",
  "error": "",
  "job_info": {}
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `job_id` | string | 是 | 查询的 JobID |
| `nodes` | []string | 是 | 作业所在的节点列表 |
| `slot` | string | HTCondor必填 | HTCondor 槽位（Slurm 可省略） |
| `error` | string | 否 | 错误信息，非空时触发服务端错误处理 |
| `job_info` | any | 否 | 作业详细信息，可选，由调度器类型决定结构 |

### HTCondor 脚本

#### 文件名
`query_htcondor_job.sh`

#### 输出格式

```json
{
  "job_id": "12345.0",
  "nodes": ["compute-01.example.com"],
  "slot": "slot1@compute-01.example.com",
  "job_info": {
    "cluster_id": "htcondor-01",
    "job_id": "12345.0",
    "node_name": "compute-01.example.com",
    "slot": "slot1@compute-01.example.com",
    "universe": "vanilla",
    "cmd": "/path/to/executable",
    "args": "arg1 arg2",
    "status": "Running"
  }
}
```

#### job_info 字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_id` | string | 否 | 集群 ID |
| `job_id` | string | 否 | 作业 ID |
| `node_name` | string | 否 | 节点名称 |
| `slot` | string | 否 | 槽位 |
| `universe` | string | 否 | HTCondor universe 类型 |
| `cmd` | string | 否 | 执行命令 |
| `args` | string | 否 | 命令参数 |
| `status` | string | 否 | 作业状态 |

#### 实现参考

使用 `condor_q` 或 `condor_history` 查询作业信息：

```bash
#!/bin/bash
JOB_ID=$1
CLUSTER_TAG=$2

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
  "job_id": "67890",
  "nodes": ["node-01", "node-02"],
  "job_info": {
    "cluster_id": "slurm-01",
    "job_id": "67890",
    "node_name": "node-01",
    "node_list": "node-01,node-02",
    "partition": "gpu",
    "job_state": "RUNNING"
  }
}
```

#### job_info 字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cluster_id` | string | 否 | 集群 ID |
| `job_id` | string | 否 | 作业 ID |
| `node_name` | string | 否 | 主节点名称 |
| `node_list` | string | 否 | 所有节点列表（逗号分隔） |
| `partition` | string | 否 | 分区名称 |
| `job_state` | string | 否 | 作业状态 |

#### 实现参考

使用 `squeue` 或 `scontrol` 查询作业信息：

```bash
#!/bin/bash
JOB_ID=$1
CLUSTER_TAG=$2

# 方法1: 使用 scontrol
JOB_INFO=$(scontrol show job ${JOB_ID})
NODE_NAME=$(echo "$JOB_INFO" | grep -oP "Nodes=\K[^ ]+")
NODE_LIST=$(echo "$JOB_INFO" | grep -oP "NodeList=\K[^ ]+")
JOB_STATE=$(echo "$JOB_INFO" | grep -oP "JobState=\K[^ ]+")
PARTITION=$(echo "$JOB_INFO" | grep -oP "Partition=\K[^ ]+")

# 方法2: 使用 squeue
# squeue -j ${JOB_ID} -o "%.20i %.9P %.50j %.8u %.2t %.10M %.6D %R"
```

## 错误处理

脚本在遇到错误时应：

1. 通过 stdout 输出 JSON 格式错误信息（`error` 字段）
2. 返回非 0 退出码（或返回 0 + `error` 字段，服务端都会处理）

示例：

```json
{
  "error": "job not found: 99999"
}
```

或输出错误到 stderr 并返回非零退出码：

```bash
if [ -z "$NODE_NAME" ]; then
    echo "Error: Failed to get node name for job $JOB_ID" >&2
    exit 1
fi
```

## 测试脚本

```bash
# HTCondor
./scripts/query_htcondor_job.sh.example "12345.0" "htcondor-cluster"

# Slurm
./scripts/query_slurm_job.sh.example "67890" "slurm-cluster"
```

## 安全建议

1. **权限控制**: 脚本应只具有必要的执行权限
2. **输入验证**: 验证 JobID 参数的有效性
3. **敏感信息**: 避免在脚本中硬编码密码或密钥
4. **日志记录**: 记录脚本执行日志以便排查问题
