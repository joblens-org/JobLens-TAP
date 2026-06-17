# Management API Specification

JobLens-TAP fetches cluster metadata from a management API configured via the `TAP_MANAGEMENT_API_URL` environment variable. This document defines the API contract the management service must implement.

> **中文文档**: [management-api_zh.md](./management-api_zh.md)

---

## 1. Fetch Cluster Scheme

### Request

```
GET {TAP_MANAGEMENT_API_URL}/api/clusters/scheme
```

**Headers**:

| Header | Value | Description |
|--------|-------|-------------|
| `Accept` | `application/json` | Expects JSON response |

**Timeout**: 10 seconds

### Response Body Structure

#### Top-Level

```json
{
  "clusters": [ ... ],
  "total": 3
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `clusters` | array | Yes | List of cluster metadata |
| `total` | int | Yes | Total number of clusters |

#### clusters[] Element

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

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cluster_name` | string | Yes | Unique cluster identifier used internally by TAP as the cluster ID |
| `cluster_type` | string | Yes | Cluster type: `"condor"` (HTCondor) or `"slurm"` (Slurm) |
| `tags` | string[] | No | ES routing tag list. Used in queries to specify a tag for routing optimization; single-tag clusters are auto-routed |
| `alias` | string | No | Display alias; used as the preferred ID in the Schema discovery endpoint |
| `enabled` | bool | Yes | Whether this cluster is enabled. `false` clusters are filtered out by the manager |
| `extra` | object | Yes | Extension fields (see below) |

#### extra Extension Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `es_url` | string | Yes | - | ES cluster endpoint URL (with protocol and port). Clusters with empty `es_url` are skipped at startup |
| `es_username` | string | No | - | ES Basic Auth username |
| `es_password` | string | No | - | ES Basic Auth password |
| `script_path` | string | No | - | Collection trigger script path (used by `POST /collect` for auto node discovery) |
| `default_node_port` | int | No | `8080` | Fallback agent port when service registry lookup fails |

#### NormalizeExtra (Field Promotion)

After loading, TAP calls `NormalizeExtra()` to promote key fields from `extra` into the `ClusterMeta` struct for internal convenience. Callers do not need to handle this — simply include the fields above in `extra`.

---

## 2. Full Example

### Request

```bash
curl -H "Accept: application/json" \
  "https://management-api.example.com/api/clusters/scheme"
```

### Response

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

> **Note**: `offline-cluster` has `enabled: false` — TAP will not create an ES client or handle queries for it. The effective cluster count is 2.

---

## 3. TAP Consumption Behavior

| Phase | Behavior |
|-------|----------|
| Startup | `InitialFetch()` blocks; exits on failure (`os.Exit(1)`) |
| Background Refresh | Periodic refresh every `TAP_MANAGEMENT_CACHE_TTL` (default: 5m) |
| Lazy Load | On-demand fetch triggered when querying an unknown cluster (30s minimum cooldown) |
| Alias Matching | Falls back to `alias` field matching when cluster name lookup fails |
| Filtering | Clusters with empty `es_url` are skipped; `enabled=false` clusters are ignored |

## 4. Error Handling

| Scenario | Behavior |
|----------|----------|
| API unreachable | Fatal at startup; WARN log on background refresh |
| HTTP non-200 | Log error and response body; skip this refresh cycle |
| JSON parse failure | Log error; skip this refresh cycle |
| Cluster `es_url` empty | Skip that cluster; log WARN |
