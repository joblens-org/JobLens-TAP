# JobLens TAP

Compute Cluster Observability Data Hub (Telemetry Access Point)

> **中文文档**: [README.md](./README.md)

## Overview

JobLens TAP is a high-performance, stateless observability data query gateway for compute clusters. It abstracts away differences between multiple underlying Elasticsearch clusters and provides a unified data query interface. Features include multi-cluster parallel queries, time-series aggregation, job summaries, field alias mapping, and collection triggering.

### Key Features

- **Multi-Cluster Transparent Routing**: Automatically fetches cluster metadata (cluster → index mapping, endpoints, credentials) from a management API. Supports `*` wildcard and comma-separated multi-cluster parallel queries.
- **Field Alias System**: Manages alias → ES field path mappings via an external registry file (`collector-registry.json`). Adding new collectors and metrics requires no code changes. Supports SIGHUP hot-reload.
- **Collector Registry**: Built-in support for cpumem/io/net/gpu collectors with configurable index naming patterns.
- **Lazy ES Connections**: ES clients are created on first query, not at startup (double-checked locking for concurrency safety).
- **Cursor-Based Pagination**: Efficient continuous pagination via `search_after`.
- **Graceful Shutdown**: SIGINT/SIGTERM graceful exit, SIGHUP hot-reload of the collector registry file.
- **Flattened Output**: Strips ES nested metadata, returning clean business fields.

## Tech Stack

- **Language**: Go 1.25
- **Web Framework**: Gin v1.11
- **ES Client**: go-elasticsearch v8
- **Configuration**: Environment variables (caarlos0/env)

## Quick Start

### Prerequisites

- Go 1.25+
- Elasticsearch cluster (cluster metadata provided via management API)

### Local Development

```bash
# Download dependencies
make deps

# Build
make build

# Run tests
make test

# Short test mode (skip ES integration tests)
go test -v -short ./...

# Run locally
export TAP_MANAGEMENT_API_URL="https://your-management-api.example.com"
export TAP_COLLECTOR_REGISTRY_PATH="./collector-registry.json"
make run
```

### Docker Build

```bash
# Build binary
make build

# Binary output at bin/server
```

## API Overview

All endpoints return a unified `{code, message, data, meta}` response structure.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health check |
| GET | `/ready` | Readiness probe (checks ES cluster connectivity) |
| GET | `/data/raw` | Raw data query (log-level sample points) |
| GET | `/data/timeseries` | Time-series aggregation query (chart level) |
| GET | `/data/summary` | Job summary query |
| GET | `/data/check-job` | Job data existence check (lightweight, size=0) |
| GET | `/schema` | Schema discovery (fields and cluster metadata) |
| GET | `/skill` | Skill API documentation (for visualization pipeline) |
| POST | `/collect` | Trigger job collection (auto-discovers node info) |
| POST | `/collect/direct` | Direct collection trigger (user-provided node info) |

### Quick Examples

```bash
# Health check
curl http://localhost:8080/health

# Raw data query
curl "http://localhost:8080/data/raw?cluster=sz01&job=172.0&from=now-1h&fields=cpu,mem"

# Time-series aggregation (multi-metric)
curl "http://localhost:8080/data/timeseries?cluster=sz01&job=172.0&metric=cpu,mem&interval=1m&from=now-1h&by=host"

# Job summary
curl "http://localhost:8080/data/summary?cluster=sz01&job=172.0"

# Job data existence check
curl "http://localhost:8080/data/check-job?cluster_name=sz01&job_id=172.0"

# Schema discovery
curl "http://localhost:8080/schema?cluster=sz01"
```

> See [API.md](./API.md) (Chinese) for the complete API reference.

## Configuration

All configuration is loaded via environment variables — no config files required.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TAP_PORT` | No | `8080` | Server port |
| `TAP_LOG_LEVEL` | No | `info` | Log level |
| `TAP_READ_TIMEOUT` | No | `30s` | HTTP read timeout |
| `TAP_WRITE_TIMEOUT` | No | `30s` | HTTP write timeout |
| `TAP_MANAGEMENT_API_URL` | **Yes** | - | Management API URL for cluster metadata (path: `{URL}/api/clusters/scheme`) |
| `TAP_MANAGEMENT_CACHE_TTL` | No | `5m` | Cluster info cache TTL |
| `TAP_MAX_SIZE` | No | `10000` | Maximum records per query |
| `TAP_DEFAULT_SIZE` | No | `100` | Default records per query |
| `TAP_MAX_TIME_RANGE_DAYS` | No | `7` | Maximum time range in days |
| `TAP_DEFAULT_INTERVAL` | No | `1m` | Default aggregation interval for timeseries |
| `TAP_COLLECTOR_REGISTRY_PATH` | Recommended | - | Path to collector registry JSON file, supports SIGHUP hot-reload |
| `TAP_DEFAULT_COLLECTORS` | No | `cpumem,io,net` | Default collector list (deprecated, used only when registry file is not set) |
| `TAP_SERVICE_REGISTRY_URL` | No | - | Service registry URL (for collection triggers) |
| `TAP_SERVICE_REGISTRY_TIMEOUT` | No | `5s` | Service registry query timeout |
| `TAP_AGENT_RETRY_INITIAL_DELAY` | No | `500ms` | Agent retry initial delay |
| `TAP_AGENT_RETRY_MAX_ATTEMPTS` | No | `3` | Agent max retry attempts |
| `TAP_AGENT_RETRY_MULTIPLIER` | No | `2.0` | Agent retry backoff multiplier |
| `TAP_SKILL_API_BASE_URL` | No | - | Skill API base URL |

### Collector Registry File

The registry file pointed to by `TAP_COLLECTOR_REGISTRY_PATH` follows this format (a sample `collector-registry.json` is provided in the project root):

```json
{
  "version": 1,
  "collectors": [
    {
      "name": "cpumem",
      "description": "CPU & Memory collector",
      "aliases": [
        {"alias": "cpu", "es_field": "data.summary.cpuPercent", "type": "float"},
        {"alias": "mem", "es_field": "data.summary.mem_rss_kb", "type": "long"}
      ]
    }
  ],
  "global_aliases": [
    {"alias": "host", "es_field": "hostname.keyword", "type": "keyword"},
    {"alias": "time", "es_field": "@timestamp", "type": "date"}
  ]
}
```

- `collectors[].name`: Collector name, determines the default index naming pattern `{name}_collector_{date}`
- `collectors[].aliases`: Collector-specific field alias mappings
- `global_aliases`: Global aliases shared across all collectors

## Project Structure

```
JobLens-TAP/
├── cmd/server/              # Application entry point
├── internal/
│   ├── config/              # Environment-based configuration loading
│   ├── cluster/             # Cluster metadata manager (management API fetch + cache)
│   ├── handler/             # Gin HTTP handlers
│   ├── service/             # Business logic layer
│   ├── repository/          # ES client manager (lazy init, double-checked locking)
│   ├── model/               # Data models, collector registry
│   ├── middleware/           # Gin middleware (Recovery → Logger → ErrorHandler)
│   └── skill/               # Skill API templates
├── tests/                   # All tests (external test package)
├── pkg/utils/               # Reserved utility package
├── collector-registry.json  # Sample collector registry file
├── Makefile                 # Build scripts
├── API.md                   # API reference (Chinese)
├── design.md                # Design documentation (reference only)
└── README.md
```

### Middleware Chain Order (immutable)

```
Recovery → Logger → ErrorHandler
```

## Development

### Common Commands

```bash
make deps          # Download dependencies + tidy
make build         # Build to bin/server
make run           # go run ./cmd/server
make test          # Run all tests
make test-coverage # Test coverage report
make fmt           # go fmt ./...
make lint          # golangci-lint run ./...
make clean         # Clean bin/ and go cache
```

### Testing

- All tests are located in the `tests/` directory, using an external test package (`package tests`)
- Integration tests require a real ES cluster with env vars: `TEST_ES_URL`, `TEST_CLUSTER_ID`, `TEST_JOB_ID`, `TEST_ES_USERNAME`, `TEST_ES_PASSWORD`
- When ES is unreachable, `TestMain` exits silently (code 0) without blocking CI
- Use `-short` flag to skip integration tests

## License

[Apache License 2.0](./LICENSE)
