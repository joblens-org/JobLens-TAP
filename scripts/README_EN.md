# Cluster Query Script Guide

> **中文文档**: [scripts/README.md](./README.md)

## Overview

This project supports querying cluster job information via shell scripts, used to trigger metrics collection. Currently supports HTCondor and Slurm cluster types.

## Script Requirements

### General Requirements

1. **Location**: Script path must be specified in the cluster configuration via `script_path`
2. **Arguments**: Scripts accept two arguments: `job_id` and `cluster_tag`
   - `$1` (`job_id`): Native JobID string (e.g., Condor `"172.0"`, Slurm `"67890"`)
   - `$2` (`cluster_tag`): Cluster tag (e.g., `"htcondor02@example.com"`), optional, for filtering specific queue/partition
3. **Output**: Must output JSON-formatted data to stdout
4. **Exit Code**: 0 for success, non-zero for failure. Error messages can be returned via the `error` field in stdout JSON

### Output Format

```json
{
  "job_id": "string",
  "nodes": ["node1", "node2"],
  "slot": "slot1@node1",
  "error": "",
  "job_info": {}
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `job_id` | string | Yes | Queried JobID |
| `nodes` | []string | Yes | List of nodes running the job |
| `slot` | string | HTCondor only | HTCondor slot identifier (omit for Slurm) |
| `error` | string | No | Error message; triggers server-side error handling when non-empty |
| `job_info` | any | No | Job details; optional, structure varies by scheduler type |

### HTCondor Script

#### Filename
`query_htcondor_job.sh`

#### Output Format

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

#### job_info Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cluster_id` | string | No | Cluster ID |
| `job_id` | string | No | Job ID |
| `node_name` | string | No | Node name |
| `slot` | string | No | Slot |
| `universe` | string | No | HTCondor universe type |
| `cmd` | string | No | Executable command |
| `args` | string | No | Command arguments |
| `status` | string | No | Job status |

#### Implementation Reference

Use `condor_q` or `condor_history` to query job information:

```bash
#!/bin/bash
JOB_ID=$1
CLUSTER_TAG=$2

# Query node and slot for the job
NODE_INFO=$(condor_q -format "%s " Machine -format "%s\n" RemoteSlotID -constraint "ClusterId==${JOB_ID}")

# Query job details
JOB_DETAILS=$(condor_q -long -constraint "ClusterId==${JOB_ID}")

# Parse and output JSON
# ...
```

### Slurm Script

#### Filename
`query_slurm_job.sh`

#### Output Format

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

#### job_info Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cluster_id` | string | No | Cluster ID |
| `job_id` | string | No | Job ID |
| `node_name` | string | No | Primary node name |
| `node_list` | string | No | All node list (comma-separated) |
| `partition` | string | No | Partition name |
| `job_state` | string | No | Job state |

#### Implementation Reference

Use `squeue` or `scontrol` to query job information:

```bash
#!/bin/bash
JOB_ID=$1
CLUSTER_TAG=$2

# Method 1: Using scontrol
JOB_INFO=$(scontrol show job ${JOB_ID})
NODE_NAME=$(echo "$JOB_INFO" | grep -oP "Nodes=\K[^ ]+")
NODE_LIST=$(echo "$JOB_INFO" | grep -oP "NodeList=\K[^ ]+")
JOB_STATE=$(echo "$JOB_INFO" | grep -oP "JobState=\K[^ ]+")
PARTITION=$(echo "$JOB_INFO" | grep -oP "Partition=\K[^ ]+")

# Method 2: Using squeue
# squeue -j ${JOB_ID} -o "%.20i %.9P %.50j %.8u %.2t %.10M %.6D %R"
```

## Error Handling

When encountering errors, scripts should:

1. Output error information in JSON format via stdout (`error` field)
2. Return a non-zero exit code (or 0 + `error` field — both are handled by the server)

Example:

```json
{
  "error": "job not found: 99999"
}
```

Or output errors to stderr and return non-zero exit code:

```bash
if [ -z "$NODE_NAME" ]; then
    echo "Error: Failed to get node name for job $JOB_ID" >&2
    exit 1
fi
```

## Testing Scripts

```bash
# HTCondor
./scripts/query_htcondor_job.sh.example "12345.0" "htcondor-cluster"

# Slurm
./scripts/query_slurm_job.sh.example "67890" "slurm-cluster"
```

## Security Recommendations

1. **Permission Control**: Scripts should only have the necessary execution permissions
2. **Input Validation**: Validate the validity of JobID parameters
3. **Sensitive Information**: Avoid hardcoding passwords or keys in scripts
4. **Logging**: Log script execution for troubleshooting purposes
