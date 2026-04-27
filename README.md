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

- **Transparent proxying** - Full MySQL protocol support (text queries, prepared statements, COM_PING, field list)
- **TLS support** - Configurable independently on client/backend/shadow/offline-replay sides
- **SQL logging** - Async JSON-lines logging with rotation, enable/disable via config hot-reload, optional arg redaction
- **Real-time shadow traffic** - Duplicate live queries to a shadow server, compare responses inline (always read-only)
- **Offline replay** - Replay recorded queries from log files against a target server (always read-only)
- **Response comparison** - Compare content (rows, columns, errors) and timing between servers
- **Query digest stats** - Aggregate avg/p95/p99 response times grouped by query digest (bounded memory via reservoir sampling)
- **Checkpoint tracking** - Resume replay from last position, auto-delete completed log files, periodic progress saves
- **Graceful shutdown** - Drain active sessions on SIGTERM with configurable timeout, force-close after
- **Metrics endpoint** - `/healthz` + `/metrics` HTTP endpoint (JSON counters; stdlib-only, no Prometheus dep)
- **Structured logging** - slog-based operational logs with JSON or text format via `LOG_FORMAT` / `LOG_LEVEL`
- **Benchmarking** - Compare latency with and without proxy (p50/p95/p99 stats, Markdown output)

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
  users:
    - username: "app_user"
      password: "app_pw"

backend:
  addr: "127.0.0.1:3306"
```

`proxy.users` is required. Each session's backend connection is opened
with the credentials the client logged in with, so per-user GRANTs on
the backend apply consistently. There is no global `backend.user` /
`backend.password` — those would be a single shared identity, which
defeats the per-user model.

### Environment variable interpolation

Any string in the config file may reference an environment variable using
`${VAR}` syntax. References are expanded at load time (and on each
hot-reload), which lets you keep credentials in env vars / Secret Manager
instead of committing them to the config file:

```yaml
backend:
  addr: "tidb.internal.example:3306"
  user: "${MYSQL_USER}"
  password: "${MYSQL_PASSWORD}"

replay:
  shadow:
    target_addr: "tidb-shadow.internal.example:3306"
    target_user: "${MYSQL_USER}"
    target_password: "${MYSQL_PASSWORD}"
```

Notes:
- Only the `${VAR}` form is expanded. Bare `$VAR` is left as-is so SQL
  fragments like `SELECT $1` or `SET @counter = ...` aren't mangled.
- Referencing an unset variable causes `Load()` to fail with all missing
  names listed at once.

### Multiple users

Add as many `(username, password)` pairs to `proxy.users` as you need.
Clients can authenticate as any of them, and each session's outbound
backend (and shadow) connection uses the same credentials. The SQL log
records the actual authenticated username.

```yaml
proxy:
  users:
    - username: "app_user"
      password: "app_pw"
    - username: "ro_user"
      password: "ro_pw"
    - username: "${MYSQL_REPLICATION_USER}"   # works with env-var expansion
      password: "${MYSQL_REPLICATION_PASSWORD}"
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
  redact_args: false     # set true to redact prepared-statement bind values
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
    enabled: true           # hot-reloadable: set false to pause without restart
    target_addr: "mysql-shadow:3306"
    target_user: "shadow_user"
    target_password: "secret"
    readonly: true          # always enforced — only SELECT queries
    timeout: 5s
    max_concurrent: 100

    # Source-IP filter (optional, hot-reloadable).
    # Empty lists = no restriction. Exclude wins over allow.
    allowed_source_cidrs:
      - "10.0.0.0/8"        # only shadow traffic from internal app subnet
    excluded_source_cidrs:
      - "10.0.5.0/24"       # but never shadow queries from DBA hosts

comparison:
  output_file: "./logs/diff-report.jsonl"
  ignore_columns: ["updated_at"]
  time_threshold_ms: 100
```

**Session-pinned shadow:** each primary session gets its own dedicated shadow connection. Queries flow serially from the primary session to its own shadow queue and execute in order on the pinned connection. This means session-scoped state — temporary tables, session variables, transactions — is preserved:

```sql
-- All three statements go to the same shadow connection:
CREATE TEMPORARY TABLE scratch (id INT);     -- tracked in the session
INSERT INTO scratch VALUES (1);              -- forwarded (target is a tracked temp)
SELECT * FROM scratch;                       -- forwarded, sees the data
```

The handler tracks temporary tables the primary creates and, for DML/DDL against them, forwards the mutation to the shadow. Writes to *persistent* tables are still rejected.

**Filter evaluation** (per query, in order):

1. `shadow.enabled: false` → skipped (counter: `shadow_disabled`)
2. `shadow.sample_rate` roll fails → skipped (counter: `shadow_sampled_out`)
3. Source IP matches any `excluded_source_cidrs` → filtered (counter: `shadow_filtered_by_cidr`)
4. `allowed_source_cidrs` is non-empty and source IP doesn't match → filtered (same counter)
5. Query category is not session-safe (DML/DDL against a non-temp table, GRANT, CALL, LOAD DATA, etc.) → skipped (counter: `shadow_skipped`)
6. Queue full → dropped (counter: `shadow_dropped`)
7. Otherwise → enqueued on the pinned shadow session for execution

**Throttling under load:** `sample_rate` is a simple way to cap shadow overhead. `0.1` sends ~10% of queries to the shadow server. Combined with hot-reload, you can dial it down during high-traffic windows:

```bash
yq -i '.replay.shadow.sample_rate = 0.1' config.yaml   # cut shadow to 10%
# later:
yq -i '.replay.shadow.sample_rate = 1.0' config.yaml   # back to full
```

All four filter stages are observable via `/metrics`, so you can verify that a CIDR change is actually rejecting the intended traffic before trusting it.

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

### Query digest stats

After replay/shadow runs, the comparison report includes a per-digest summary:

```
=== Query Digest Summary (2 unique digests) ===

Digest                                       Count  Match   Diff | Orig Avg  Orig P95  Orig P99 | Rply Avg  Rply P95  Rply P99
select * from users where id = ?               150    150      0 |   2.34ms    5.10ms    8.90ms |   3.12ms    6.80ms   12.10ms
select * from orders where user_id = ?          50     48      2 |   1.50ms    3.20ms    4.80ms |   1.80ms    4.00ms    6.20ms
```

SQL literals (numbers, strings, `IN (...)` lists) are replaced with `?` to group identical query patterns. Percentiles use reservoir sampling (10k samples per digest) — memory stays bounded regardless of how long the proxy runs.

## Observability

Enable the metrics endpoint in your config:

```yaml
proxy:
  metrics_addr: "127.0.0.1:9090"    # "" to disable
```

Endpoints:
- `GET /healthz` — 200 OK (liveness)
- `GET /metrics` — Prometheus/OpenMetrics text format (compatible with Datadog `openmetrics` check and Prometheus scrapers)
- `GET /metrics.json` — same metrics, JSON format, for human debugging
- `GET /debug/vars` — Go runtime stats via expvar

Available metrics:
  - **Sessions:** `active_sessions`, `total_sessions`
  - **Queries:** `queries_handled`, `query_errors`
  - **Logger:** `logger_dropped` (entries dropped when the async buffer was full)
  - **Shadow:** `shadow_enabled` (gauge), `shadow_active_sessions` (gauge), `shadow_queries_replayed`, `shadow_disabled` (rejected by toggle), `shadow_sampled_out` (dropped by `sample_rate`), `shadow_filtered_by_cidr` (rejected by CIDR filter), `shadow_skipped` (not session-safe), `shadow_dropped` (queue full or connection timeout)
  - **Comparisons:** `comparisons_total`, `comparisons_matched`, `comparisons_differed`, `comparisons_ignored`, `comparisons_digest_count` (gauge), `comparisons_digest_overflow`
  - **Runtime (gauges):** `heap_alloc_bytes`, `heap_inuse_bytes`, `heap_idle_bytes`, `heap_sys_bytes`, `heap_objects`, `stack_inuse_bytes`, `sys_bytes`, `num_goroutines`, `gc_cycles_total`, `gc_pause_ns_total`

### Datadog integration

On Kubernetes, the Datadog agent can auto-discover the proxy via pod annotations:

```yaml
metadata:
  annotations:
    ad.datadoghq.com/mysql-interceptor.check_names: '["openmetrics"]'
    ad.datadoghq.com/mysql-interceptor.init_configs: '[{}]'
    ad.datadoghq.com/mysql-interceptor.instances: |
      [
        {
          "openmetrics_endpoint": "http://%%host%%:9090/metrics",
          "namespace": "mysql_interceptor",
          "metrics": [".*"]
        }
      ]
```

(Replace `mysql-interceptor` with your container name. Port 9090 matches the default `proxy.metrics_addr`.)

Operational logs go to stderr via Go's `slog`:

```bash
# Default: text output, info level
./bin/mysql-interceptor serve --config config.yaml

# Structured JSON at debug level
LOG_FORMAT=json LOG_LEVEL=debug ./bin/mysql-interceptor serve --config config.yaml
```

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
- **Unit tests** - `./internal/...` tests with `-race`
- **Integration tests** - 2 MySQL service containers with divergent schemas, including proxy round-trip and prepared-statement tests
- **Docker build** - verifies the container image builds correctly

On tag push (`v*`), the release workflow runs GoReleaser to build multi-platform binaries and a Docker image, then runs the benchmark against a fresh MySQL and appends the latency table to the GitHub Release body.

## License

See [LICENSE](LICENSE).
