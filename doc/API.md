# JobLens-TAP API Reference

**Version**: v1.1
**Base Path**: `/`

> **中文文档**: [API_zh.md](./API_zh.md)

---

## Table of Contents

- [Overview](#overview)
- [Unified Response Structure](#unified-response-structure)
- [Endpoints](#endpoints)
  - [1. Health Check](#1-health-check)
  - [2. Raw Data Query](#2-raw-data-query)
  - [3. Time-Series Query](#3-time-series-query)
  - [4. Job Summary Query](#4-job-summary-query)
  - [5. Schema Discovery](#5-schema-discovery)
  - [6. Job Data Existence Check](#6-job-data-existence-check)
  - [7. Trigger Collection](#7-trigger-collection)
- [Field Alias Mapping](#field-alias-mapping)
- [Usage Examples](#usage-examples)

---

## Overview

JobLens-TAP is a compute cluster observability data hub providing a unified data query interface.

### Design Principles

- **Stateless**: No sessions or caches stored by the service
- **Transparent Routing**: Automatic Cluster → Index mapping
- **Field Aliases**: Semantic aliases map to ES nested fields
- **Flattened Output**: ES metadata removed; clean business fields returned

---

## Unified Response Structure

All endpoints return a unified response format:

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

| Field | Type | Description |
|-------|------|-------------|
| `code` | int | Status code. `0` = success |
| `message` | string | Status message |
| `data` | object | Response payload |
| `meta` | object | Metadata (indices hit, query duration) |

### Response Status Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 400 | Bad request (invalid parameters) |
| 500 | Internal server error |

### HTTP Status Codes

| HTTP Status | Meaning |
|-------------|---------|
| 200 | Success |
| 207 | Partial success (`partial_success` on collection endpoints) |
| 400 | Bad request (invalid parameters) |
| 500 | Internal server error |
| 503 | Service unavailable (ES clusters unhealthy per readiness probe) |

---

## Endpoints

### 1. Health Check

#### 1.1 Service Health

```
GET /health
```

**Example Response**:

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

#### 1.2 Readiness Probe

```
GET /ready
```

Checks ES cluster connectivity.

**Success Response** (HTTP 200):

```json
{
  "code": 0,
  "message": "ready",
  "data": {
    "clusters": {
      "htcondor01": "healthy",
      "INKSlurm": "healthy"
    },
    "version": "0.1.0",
    "commit": "11787df",
    "build": "2026-06-17T11:19:12Z"
  }
}
```

**Failure Response** (HTTP 503):

Returned when some or all ES clusters are unreachable:

```json
{
  "code": 1,
  "message": "some clusters are unavailable",
  "data": {
    "clusters": {
      "htcondor01": "unhealthy: connection refused",
      "INKSlurm": "healthy"
    },
    "version": "0.1.0",
    "commit": "11787df",
    "build": "2026-06-17T11:19:12Z"
  }
}
```

---

### 2. Raw Data Query

Fetches raw sample-point data (log level).

```
GET /data/raw
```

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cluster` | string | Yes | - | Cluster ID. Supports multiple (comma-separated), wildcard (`*`), or tagged (`cluster_name:cluster_tag`) |
| `job` | string | Yes | - | Native JobID string (e.g. Condor `"172.0"`, Slurm `"67890"`). Matches `job_info.NativeJobID` |
| `from` | string | Conditional | - | Start time (ISO8601 or relative). Optional when `full_range=true` |
| `to` | string | No | `now` | End time |
| `full_range` | bool | No | `false` | Auto-discover Job time range and return all data |
| `collector` | string | No | empty | Collector type (`cpumem`/`io`/`net`). Empty = query all |
| `fields` | string | No | empty | Field whitelist (comma-separated). Supports aliases or raw ES paths |
| `size` | int | No | `100` | Batch size. Max 10000 |
| `cursor` | string | No | empty | Pagination cursor |
| `flatten` | bool | No | `true` | Flatten nested structures |

> **Note**: `from` is required unless `full_range=true`.

#### `cluster` Parameter Formats

| Format | Example | Description |
|--------|---------|-------------|
| Single cluster | `htcondor01` | Query one cluster |
| Tagged | `htcondor01:htcondor02@htcondor02.ihep.ac.cn` | Exact cluster tag (ES routing) |
| Multi-cluster | `htcondor01,INKSlurm` | Comma-separated, parallel query |
| Wildcard | `*` | Query all clusters |

> **Single Tag Optimization**: When `cluster_tag` is not specified and the target cluster has only one tag, routing is auto-applied for faster queries.

#### Time Format Support

| Format | Example | Description |
|--------|---------|-------------|
| ISO8601 | `2026-04-05T10:00:00Z` | Full timestamp |
| Relative | `now-1h` | Current time minus 1 hour |
| Shorthand | `1h`, `1d` | Equivalent to `now-1h`, `now-1d` |

#### Response Structure

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

#### Record Fields

| Field | Type | Description |
|-------|------|-------------|
| `cluster` | string | Cluster ID |
| `collector` | string | Collector type (auto-extracted from index name) |
| `time` | string | Timestamp |
| `host` | string | Hostname |
| `job` | any | Native JobID (prefers `job_info.NativeJobID` string; legacy data falls back to `JobID` int) |
| `cpu` | float64 | CPU usage percentage (shortcut: `data.summary.cpuPercent`) |
| `mem` | int64 | Memory usage KB (shortcut: `data.summary.mem_rss_kb`) |
| `name` | string | Process name (shortcut: `data.summary.name.keyword`) |
| `io_bytes` | int64 | IO bytes (shortcut: `data.summary.read_bytes`) |
| `fields` | map | Flattened field set (returned when `flatten=true`) |
| `data` | map | Nested raw structure (returned when `flatten=false`) |

#### Response Meta Fields

| Field | Type | Description |
|-------|------|-------------|
| `data.indices_resolved` | []string | Resolved index names per query |
| `meta.indices_hit` | []string | Indices actually hit by ES |
| `meta.query_time_ms` | int | Query duration in milliseconds |
| `meta.clusters_queried` | []string | Clusters queried |

#### Pagination

- Uses `search_after` for cursor-based pagination, sorted by `@timestamp` descending
- For multi-cluster queries, the returned cursor points to a specific cluster; subsequent requests continue from that cluster only
- `next_cursor` contains cluster name, sort values, and query hash for pagination continuity validation

---

### 3. Time-Series Query

Fetches time-series aggregation data (chart level). Supports multi-metric queries. **Single cluster only**.

```
GET /data/timeseries
```

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cluster` | string | Yes | - | Cluster ID (single cluster only) |
| `job` | string | Yes | - | Native JobID string. Matches `job_info.NativeJobID` |
| `metric` | string | Yes | - | Metric name (comma-separated for multiple, e.g. `cpu,mem`). Collector is auto-inferred from alias mapping |
| `interval` | string | Yes | - | Bucket interval (`10s`/`1m`/`5m`/`1h`) |
| `from` | string | Yes | - | Start time |
| `to` | string | No | `now` | End time |
| `agg` | string | No | `avg` | Aggregation method (`avg`/`max`/`min`/`sum`) |
| `by` | string | No | empty | Grouping dimension (`host`/`collector`) |

> **Note**: Specifying multiple clusters (e.g. `cluster=a,b`) returns a 400 error.

#### Response Structure

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

> **Note**: `meta.indices_hit` returns an empty array for time-series queries, since aggregation queries don't return specific hit indices.

#### Record Fields

| Field | Type | Description |
|-------|------|-------------|
| `metric` | string | Metric name |
| `label` | string | Group label (when grouped by host/collector); empty string when ungrouped |
| `timestamp` | string | ISO8601 timestamp |
| `value` | float64 | Metric value |

---

### 4. Job Summary Query

Retrieves job-level statistical summary. **Single cluster only**.

```
GET /data/summary
```

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cluster` | string | Yes | - | Cluster ID (single cluster only) |
| `job` | string | Yes | - | Native JobID string. Matches `job_info.NativeJobID` |
| `collectors` | string | No | empty | Collector filter (comma-separated). **Note: this parameter is currently not used — default collectors are always applied** |

> **Note**: Specifying multiple clusters (e.g. `cluster=a,b`) returns a 400 error.

#### Response Structure

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

> **Note**: `scope.hosts` currently returns an empty array (hostname list not collected yet); `scope.samples_count` is fixed at 0; `scope.collectors` returns the default collector list rather than collectors with actual data hits.

---

### 5. Schema Discovery

Discover available fields and cluster metadata.

```
GET /schema
```

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cluster` | string | No | empty | Filter by cluster ID. Empty = return all |
| `collector` | string | No | empty | Filter by collector name. Empty = return all |

#### Response Structure

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "clusters": [
      {
        "id": "SZ-HTCondor",
        "type": "condor",
        "alias": "SZ-HTCondor",
        "enabled": true,
        "collectors": ["cpumem", "io", "net", "gpu"]
      }
    ],
    "collectors": [
      {
        "name": "cpumem",
        "description": "CPU & Memory collector",
        "index_pattern": "{name}_collector_{yyyy.MM.dd}",
        "aliases": [
          {"alias": "cpu", "es_field": "data.summary.cpuPercent", "type": "float"},
          {"alias": "mem", "es_field": "data.summary.mem_rss_kb", "type": "long"}
        ]
      }
    ],
    "common_aliases": {
      "host": "hostname.keyword",
      "time": "@timestamp"
    }
  }
}
```

> **Note**: `clusters[].id` uses the cluster's `alias` if set, otherwise the `cluster_name`. `clusters[].collectors` lists collector names from the Registry, not ES routing tags.

---

### 6. Job Data Existence Check

Quickly checks whether monitoring data exists for a given Job in a specified cluster. Uses the ES `_search` API (`size=0` + `min`/`max` aggregations) for lightweight queries — no document bodies returned; minimal cluster impact.

```
GET /data/check-job
```

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cluster_name` | string | Yes | - | Cluster name (e.g. `htcondor01`) |
| `cluster_tag` | string | No | empty | Cluster tag (e.g. `htcondor02@htcondor02.ihep.ac.cn`). Omit to not filter by tag |
| `job_id` | string | Yes | - | Native JobID string (e.g. Condor `"172.0"`, Slurm `"67890"`). Matches `job_info.NativeJobID` |

#### Response Structure

**Data Exists**:

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

**No Data**:

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

#### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `exists` | bool | Whether data exists |
| `count` | int | Number of matching documents |
| `job_id` | string | Requested JobID |
| `cluster` | string | Cluster name |
| `time` | object | Data time range (returned when `count > 0`) |
| `time.first_seen` | string | Earliest data timestamp |
| `time.last_seen` | string | Latest data timestamp |
| `time.duration_sec` | int | Data span in seconds |

#### Performance

- Uses ES `_search` API (`size=0` + `min`/`max` aggregations); no document bodies returned — extremely fast
- Query filters on keyword fields (inverted index) only; no `@timestamp` range filter needed
- Automatically scans across all collector indices (`cpumem_collector_*`, `io_collector_*`, `net_collector_*`)
- When a cluster has only one tag, routing is auto-applied for faster queries

#### Example

```bash
# Check if HTCondor Job data exists
curl "http://localhost:8080/data/check-job?cluster_name=htcondor01&job_id=172.0"

# With specific cluster_tag
curl "http://localhost:8080/data/check-job?cluster_name=htcondor01&cluster_tag=htcondor02@htcondor02.ihep.ac.cn&job_id=172.0"

# Check Slurm Job data
curl "http://localhost:8080/data/check-job?cluster_name=INKSlurm&job_id=67890"
```

#### Error Response

```json
{
  "code": 500,
  "message": "check job failed: cluster not found: unknown_cluster"
}
```

---

### 7. Trigger Collection

#### 7.1 Auto-Discovery Trigger

Queries the cluster via script to discover node info, then triggers collection.

```
POST /collect
```

##### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cluster_name` | string | Yes | Cluster name (e.g. `htcondor01`) |
| `cluster_tag` | string | Yes | Cluster tag (e.g. `htcondor02@htcondor02.ihep.ac.cn`) |
| `job_id` | string | Yes | Native JobID string (e.g. `"172.0"`). Converted to uint64 internally when sent to Agent |
| `collector` | string | Yes | Collector name(s), comma-separated (e.g. `cpumem,io,net`). Each value gets `_collector` suffix appended before sending to Agent |

**Example (single collector)**:

```json
{
  "cluster_name": "htcondor01",
  "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
  "job_id": "172.0",
  "collector": "cpumem"
}
```

**Example (multi-collector)**:

```json
{
  "cluster_name": "htcondor01",
  "cluster_tag": "htcondor02@htcondor02.ihep.ac.cn",
  "job_id": "172.0",
  "collector": "cpumem,io,net"
}
```

##### Response Structure

**Success** (HTTP 200):

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

**Partial Success** (HTTP 207 Multi-Status):

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

##### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Status: `success` (all succeeded) / `partial_success` (partial) / `failed` (all failed) |
| `cluster_name` | string | Cluster name |
| `job_id` | string | Native JobID string |
| `cluster_tag` | string | Cluster tag |
| `node_type` | string | Cluster type: `htcondor` / `slurm` |
| `node_name` | string | Primary node name |
| `slot` | string | HTCondor slot (`omitempty`; only for HTCondor, has value on success) |
| `job_info` | object/null | Job info (dynamic structure by cluster type). Object on success, `null` on failure. All sub-fields always present (empty string when no data) |
| `agent_response` | object/array | Agent response (only on `success`/`partial_success`). Single node = object; multi-node = array |
| `message` | string | Status message |

##### Error Handling

This endpoint uses layered error handling: even if some steps fail, a complete response is returned. Callers should check the `status` field to determine the overall state.

**Failure Response** (HTTP 200, but `status: "failed"`):

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

> **Note**: When `status` is `failed`, `agent_response` is omitted; `job_info` is `null`; `node_name` is an empty string.

##### Retry Mechanism

- **Registry Fallback**: When registry lookup fails, falls back to default URL format `http://{node_name}:{default_port}`
- **Agent Retry**: Exponential backoff: initial delay 500ms, multiplier 2.0, max 3 attempts
- **Retry Logging**: Each retry is logged for troubleshooting

---

#### 7.2 Direct Trigger

Skips script-based node discovery. User provides node info directly.

```
POST /collect/direct
```

##### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cluster_name` | string | Yes | Cluster name (e.g. `htcondor01`) |
| `cluster_tag` | string | Yes | Cluster tag |
| `job_id` | string | Yes | Native JobID string (e.g. `"172.0"`) |
| `collector` | string | Yes | Collector name(s), comma-separated (e.g. `cpumem,io,net`). Each value gets `_collector` suffix appended |
| `node` | string | Yes | Node hostname |
| `slot` | string | Conditional | HTCondor slot (e.g. `slot1@node01`). Required for `htcondor` clusters; ignored for `slurm` |

**Example (HTCondor, single collector)**:
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

**Example (HTCondor, multi-collector)**:
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

**Example (Slurm)**:
```json
{
  "cluster_name": "INKSlurm",
  "cluster_tag": "slurm_cluster_2",
  "job_id": "67890",
  "collector": "cpumem",
  "node": "gpu-node-05"
}
```

##### Response Structure

**Success (HTCondor)** (HTTP 200):
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

**Error (validation)** (HTTP 400):
```json
{
  "code": 400,
  "message": "slot is required for htcondor cluster"
}
```

**Error (agent)** (HTTP 200, but `status: "failed"`):
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
    "job_info": { ... },
    "message": "agent request failed: ..."
  }
}
```

##### Auto vs Direct Comparison

| Dimension | Auto Trigger | Direct Trigger |
|-----------|-------------|----------------|
| External script dependency | Yes | No |
| Node info acquisition | Auto-queries cluster | User provides directly |
| Multi-node support | Supported (script returns NodeList) | Single node only |
| Use case | Known JobID, unknown node | Known node and JobID |

---

## Field Alias Mapping

| Alias | ES Field Path | Type | Collector |
|-------|--------------|------|-----------|
| `cpu` | `data.summary.cpuPercent` | float | cpumem |
| `mem` | `data.summary.mem_rss_kb` | long | cpumem |
| `mem_peak` | `data.summary.mem_peak_rss_kb` | long | cpumem |
| `name` | `data.summary.name.keyword` | keyword | cpumem |
| `host` | `hostname.keyword` | keyword | (global) |
| `io_bytes` | `data.summary.read_bytes` | long | io |
| `time` | `@timestamp` | date | (global) |

---

## Usage Examples

### cURL

```bash
# Condor JobID query (with ".")
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&from=now-2d&collector=cpumem&fields=cpu,mem,host"

# Slurm JobID query
curl "http://localhost:8080/data/raw?cluster=INKSlurm&job=67890&from=now-1h&fields=cpu,mem,host"

# Tagged exact query
curl "http://localhost:8080/data/raw?cluster=htcondor01:htcondor02@htcondor02.ihep.ac.cn&job=172.0&from=now-1h"

# Full time range raw data (auto-discovery)
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&full_range=true&size=100&fields=cpu,mem"

# Full range with nested structure preserved
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&full_range=true&flatten=false&fields=data.summary.cpuPercent"

# CPU and memory time-series (1m interval, by host)
curl "http://localhost:8080/data/timeseries?cluster=htcondor01&job=172.0&metric=cpu,mem&interval=1m&from=now-1h&by=host"

# Job summary
curl "http://localhost:8080/data/summary?cluster=htcondor01&job=172.0"

# Schema discovery
curl "http://localhost:8080/schema?cluster=htcondor01"

# Multi-cluster query
curl "http://localhost:8080/data/raw?cluster=htcondor01,INKSlurm&job=172.0&from=now-1h&fields=cpu"

# Wildcard all clusters
curl "http://localhost:8080/data/raw?cluster=*&job=172.0&from=now-1h&fields=cpu"

# Trigger single collector
curl -X POST "http://localhost:8080/collect" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"htcondor01","cluster_tag":"htcondor02@htcondor02.ihep.ac.cn","job_id":"172.0","collector":"cpumem"}'

# Trigger multi-collector
curl -X POST "http://localhost:8080/collect" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"htcondor01","cluster_tag":"htcondor02@htcondor02.ihep.ac.cn","job_id":"172.0","collector":"cpumem,io,net"}'

# Direct trigger (multi-collector, skip script)
curl -X POST "http://localhost:8080/collect/direct" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"htcondor01","cluster_tag":"htcondor02@htcondor02.ihep.ac.cn","job_id":"172.0","collector":"cpumem,io","node":"htcondor03.ihep.ac.cn","slot":"slot1@htcondor03"}'
```

### Pagination

```bash
# First request
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&from=now-2d&size=100"

# Next page via cursor
curl "http://localhost:8080/data/raw?cluster=htcondor01&job=172.0&from=now-2d&size=100&cursor=eyJ0aW1lIjoxN..."
```

### Python

```python
import requests

BASE_URL = "http://localhost:8080"

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

def query_raw_full(cluster, job, fields=None, size=100):
    return query_raw(cluster, job, full_range=True, fields=fields, size=size)

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

if __name__ == "__main__":
    # Condor job ID "172.0"
    result = query_raw("htcondor01", "172.0", "now-2d", fields=["cpu", "mem", "host"])
    print(f"Total records: {result['data']['pagination']['total']}")

    # Time-series curves
    ts_result = query_timeseries("htcondor01", "172.0", ["cpu", "mem"], "1m", "now-1h", by="host")
    for record in ts_result["data"]["records"][:5]:
        print(f"{record['metric']} @ {record['label']} {record['timestamp']}: {record['value']}")

    # Slurm job ID "67890"
    result = query_raw("INKSlurm", "67890", "now-1h", fields=["cpu", "mem"])
    print(f"Slurm job records: {result['data']['pagination']['total']}")
```

### Grafana JSON API Datasource

1. Add a JSON API datasource
2. Configure URL: `http://localhost:8080`
3. Create a Dashboard with queries:
   - Raw: `/data/raw?cluster=$cluster&job=$job&from=${__from:date:iso}&to=${__to:date:iso}&fields=cpu,mem`
   - Timeseries: `/data/timeseries?cluster=$cluster&job=$job&metric=cpu&interval=${__interval}&from=${__from:date:iso}`

> **Note**: Grafana variable `$job` must be a string type (e.g. `"172.0"`). Collector is auto-inferred from metric alias mapping.

---

## JobID Matching (v1.1)

### NativeJobID Unified Matching

All query endpoints use the native JobID string in the `job` parameter, which matches the `job_info.NativeJobID.keyword` field in ES documents directly.

| Scheduler | Frontend Value | ES Field | Match Method |
|-----------|---------------|----------|--------------|
| Condor | `"172.0"` (cluster_id.proc_id) | `job_info.NativeJobID` | keyword exact match |
| Slurm | `"67890"` (numeric) | `job_info.NativeJobID` | keyword exact match |

### Legacy Field Compatibility

ES documents still contain these fields for legacy data compatibility:
- `job_info.JobID` — Slurm-format integer
- `job_info.sub_attr.cluster_id` + `job_info.sub_attr.proc_id` — Condor decomposed format

The response `record.job` field prefers `NativeJobID` (string), falling back to `JobID` (int64) for legacy data.

### Migration from v1.0

Key changes from v1.0 to v1.1:
- `job` parameter type changed from `int64` to `string` (frontend passes `"172.0"` instead of `172`)
- No need to distinguish Condor/Slurm formats; unified matching via `NativeJobID`
- Collection side must ensure `job_info.NativeJobID` is written (legacy fields retained unchanged)

---

## Error Response Examples

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
