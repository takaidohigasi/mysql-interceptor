# MySQL Interceptor

A MySQL proxy that transparently forwards traffic between clients and a backend MySQL server, with SQL logging, traffic replay, and response comparison capabilities.

## Architecture

```
                          +----------------------+
                          |  MySQL Interceptor   |
                          |                      |
Client --(TLS optional)-->|  Proxy  --> Logger   |--(TLS optional)--> Backend MySQL
                          |    |                 |
                          |    +--> Shadow Send --+--> Shadow MySQL (real-time compare)
                          |                      |
                          +----------------------+

Offline Replay:  Log Files --> Replayer --> Target MySQL --> Comparison Report
```

## Features

- **Transparent proxying** - Full MySQL protocol support (queries, prepared statements, COM_PING)
- **TLS support** - Configurable independently on client-side and backend-side
- **SQL logging** - Async JSON-lines logging with rotation, enable/disable via config hot-reload
- **Real-time shadow traffic** - Duplicate live queries to a shadow server, compare responses inline
- **Offline replay** - Replay recorded queries from log files against a target server
- **Response comparison** - Compare content (rows, columns, errors) and timing between servers
- **Checkpoint tracking** - Resume replay from last position, auto-delete completed log files
- **Benchmarking** - Compare latency with and without proxy (p50/p95/p99 stats)

## Quick Start

### Build

```bash
make build
```

### Run the proxy

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your backend MySQL address and credentials
./bin/mysql-interceptor serve --config config.yaml
```

### Connect through the proxy

```bash
mysql -h 127.0.0.1 -P 3307 -u <user> -p
```

### Docker

```bash
docker build -t mysql-interceptor .
docker run -v ./config.yaml:/etc/mysql-interceptor/config.yaml mysql-interceptor
```

## Configuration

See [config.example.yaml](config.example.yaml) for all options. Key sections:

### Proxy & Backend

```yaml
proxy:
  listen_addr: "0.0.0.0:3307"

backend:
  addr: "127.0.0.1:3306"
  user: "root"
  password: "secret"
```

### TLS

TLS is configurable independently on both sides:

```yaml
tls:
  client_side:           # client --> proxy
    enabled: true
    cert_file: "/path/to/proxy.crt"
    key_file: "/path/to/proxy.key"
  backend_side:          # proxy --> backend
    enabled: true
    ca_file: "/path/to/ca.crt"
    skip_verify: false
```

### SQL Logging

Logs all queries as JSON lines with rotation. Enable/disable at runtime by editing the config file (watched via fsnotify):

```yaml
logging:
  enabled: true          # hot-reloadable
  output_dir: "./logs"
  file_prefix: "queries"
  rotation:
    max_size_mb: 100
    max_age_days: 7
    max_backups: 5
    compress: true
```

Log entry format:

```json
{
  "timestamp": "2026-04-17T12:00:00.123Z",
  "session_id": 42,
  "source_ip": "10.0.0.5",
  "user": "app_user",
  "database": "mydb",
  "query_type": "query",
  "query": "SELECT * FROM users WHERE id = 1",
  "response_time_ms": 2.34,
  "rows_affected": 0,
  "rows_returned": 1
}
```

### Shadow Traffic (Real-time)

Duplicate live queries to a shadow server and compare responses:

```yaml
replay:
  mode: "shadow"
  shadow:
    target_addr: "mysql-shadow:3306"
    target_user: "shadow_user"
    target_password: "secret"
    readonly: true       # only replay SELECT queries
    timeout: 5s
    max_concurrent: 100

comparison:
  output_file: "./logs/diff-report.jsonl"
  ignore_columns: ["updated_at"]
  time_threshold_ms: 100
```

### Offline Replay

Replay recorded queries from log files against a target server:

```bash
./bin/mysql-interceptor replay --config config.yaml
```

```yaml
replay:
  mode: "offline"
  offline:
    input_dir: "./logs"
    file_pattern: "queries-*.jsonl"
    target_addr: "mysql-staging:3306"
    target_user: "replay_user"
    target_password: "secret"
    speed_factor: 1.0          # 1.0 = real-time, 2.0 = double speed
    concurrency: 10
    checkpoint_file: "./logs/.replay-checkpoint.json"
    auto_delete_completed: false
```

The replayer tracks its position in a checkpoint file. On restart, it resumes from where it left off. When `auto_delete_completed` is true, log files are deleted after they are fully replayed.

### Benchmarking

Compare response time with and without the proxy:

```bash
# Start the proxy first, then run:
./bin/mysql-interceptor bench --config config.yaml
```

```yaml
bench:
  queries:
    - "SELECT 1"
    - "SELECT * FROM users LIMIT 10"
  concurrency: 4
  iterations: 1000
  warmup_iterations: 50
```

Output:

```
=== Benchmark Results ===

Query: SELECT 1
---
  Direct: avg=234us  p50=210us  p95=450us  p99=890us  min=180us  max=1.2ms  stddev=95us  errors=0
  Proxy : avg=312us  p50=280us  p95=620us  p99=1.1ms  min=220us  max=1.8ms  stddev=130us  errors=0
Overhead (p50): +33.3%
Overhead (p99): +23.6%
```

## Comparison Report

The diff report (JSONL) shows per-query comparison results:

```json
{
  "query": "SELECT * FROM orders WHERE user_id = 1",
  "session_id": 1,
  "match": false,
  "differences": [
    {
      "type": "error",
      "original": "",
      "replay": "Error 1146 (42S02): Table 'test_db.orders' doesn't exist"
    }
  ],
  "original_time_ms": 2.0,
  "replay_time_ms": 1.5,
  "time_diff_ms": -0.5,
  "time_diff_exceeded": false
}
```

Difference types: `error`, `row_count`, `column_count`, `column_name`, `cell_value`, `affected_rows`

## Testing

### Unit tests

```bash
go test ./internal/...
```

### Integration tests (requires 2 MySQL instances)

```bash
# Start two MySQL servers (e.g., via docker)
docker run -d --name mysql1 -p 3306:3306 -e MYSQL_ROOT_PASSWORD=rootpass -e MYSQL_DATABASE=test_db mysql:8.0
docker run -d --name mysql2 -p 3307:3306 -e MYSQL_ROOT_PASSWORD=rootpass -e MYSQL_DATABASE=test_db mysql:8.0

# Initialize schemas
mysql -h 127.0.0.1 -P 3306 -u root -prootpass test_db < test/testdata/schema_primary.sql
mysql -h 127.0.0.1 -P 3307 -u root -prootpass test_db < test/testdata/schema_secondary.sql

# Run tests
MYSQL1_ADDR=127.0.0.1:3306 MYSQL2_ADDR=127.0.0.1:3307 go test -v ./test/...
```

The integration tests verify:
- Query forwarding through the proxy
- Replay against two servers with **divergent schemas** (one has a table, the other doesn't)
- Comparison correctly detects error vs success differences
- Offline replay pipeline with checkpoint tracking

## CI

GitHub Actions runs on every push/PR:
- **Unit tests** - all `./internal/...` tests
- **Integration tests** - 2 MySQL service containers with divergent schemas
- **Docker build** - verifies the container image builds correctly

## License

See [LICENSE](LICENSE).
