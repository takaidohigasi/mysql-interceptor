# Changelog

All notable changes to mysql-interceptor are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/),
and the project adheres to [Semantic Versioning](https://semver.org/) once it
reaches 1.0 (everything before is 0.y.z with breaking changes possible between
minor versions).

<a id="unreleased"></a>
## Unreleased

### Added

- **Environment variable interpolation in config** — any string in
  `config.yaml` can now reference an env var as `${VAR}`, expanded at load
  time and on each hot-reload. Lets operators keep credentials in env vars
  / Secret Manager instead of committing them. Bare `$VAR` (no braces) is
  intentionally left untouched so SQL fragments like `SELECT $1` survive
  unchanged. Referencing an unset variable fails the load with all missing
  names listed.
- **`proxy.users`** — the proxy now authenticates clients against a
  configured `(username, password)` list. The matched credentials are
  reused for the outbound backend connection (and the shadow connection,
  when shadow mode is on) so per-user GRANTs on the backend apply
  consistently, and the SQL log records the actual authenticated
  username instead of a single shared one.

### Removed

- **Single-user mode.** `backend.user` / `backend.password` are no
  longer read from YAML — they were a single shared identity for every
  client, which defeated per-user GRANTs and per-user audit. `proxy.users`
  is now required (at least one entry). To migrate:

  ```diff
   backend:
     addr: "tidb.internal:3306"
  -  user: "${MYSQL_USER}"
  -  password: "${MYSQL_PASSWORD}"
  +proxy:
  +  users:
  +    - username: "${MYSQL_USER}"
  +      password: "${MYSQL_PASSWORD}"
  ```

<a id="v0.0.3"></a>
## v0.0.3

_Released 2026-04-17._

### Highlights

- **Session-aware shadow replay** — one dedicated shadow connection per primary
  session, with temp-table tracking so DML against session-local temp tables
  is now correctly forwarded instead of being rejected as "non-SELECT".
- **Shadow source-IP CIDR filter** — new `shadow.allowed_source_cidrs` and
  `shadow.excluded_source_cidrs` let operators restrict shadow forwarding to
  specific networks (e.g. internal app subnet only, not DBA hosts).
- **Prometheus-format `/metrics`** — Datadog's `openmetrics` check and any
  Prometheus scraper now work out of the box. JSON still available at
  `/metrics.json` for human debugging.
- **Shadow throughput control** — `shadow.sample_rate` lets operators dial
  shadow overhead up or down at runtime (e.g. 10% during peak traffic).
- **Runtime gauges in `/metrics`** — heap, goroutine, and GC stats are now
  first-class metrics alongside the app counters.

### Added

- `ShadowSession` type with per-session pinned backend connection, bounded
  per-session queue (default 64), and temp-table tracking.
- Query classifier (`Classify`, `IsSafeForShadowSession`) with seven categories;
  replaces the old binary read-only check for shadow.
- Table-name extractors (`ExtractTempTableName`, `ExtractDMLTargetTable`) that
  handle schema-qualified and backtick-quoted identifiers.
- `shadow.allowed_source_cidrs`, `shadow.excluded_source_cidrs` — hot-reloadable
  CIDR filter lists evaluated as "exclude wins over allow".
- `shadow.sample_rate` — float 0.0–1.0 for sampling-based throttling,
  hot-reloadable.
- `comparison.max_unique_digests` — caps the per-digest stats map to prevent
  unbounded memory growth on ad-hoc workloads.
- `logging.queue_size`, `shadow.queue_size`, `offline.scanner_buffer_size_bytes`
  — previously hard-coded constants are now configurable.
- New metrics:
  - `shadow_active_sessions` (gauge)
  - `shadow_filtered_by_cidr`, `shadow_sampled_out`, `shadow_disabled` (counters)
  - `comparisons_digest_count` (gauge), `comparisons_digest_overflow` (counter)
  - `heap_alloc_bytes`, `heap_inuse_bytes`, `heap_idle_bytes`, `heap_sys_bytes`,
    `heap_objects`, `stack_inuse_bytes`, `sys_bytes`, `num_goroutines`,
    `gc_cycles_total`, `gc_pause_ns_total`
- `/metrics.json` endpoint for the structured JSON view.
- Docs: Datadog annotation example in the README.

### Changed

- **`/metrics` now serves Prometheus text exposition format** (was JSON).
  Move scrapers that relied on JSON to `/metrics.json`.
- Shadow queries are now routed through per-session connections. Filter
  evaluation order: `enabled` → `sample_rate` → CIDR → session-safe category
  → queue. Non-SELECT statements that are session-scoped (SET, BEGIN/COMMIT,
  temp-table DDL) now pass; DML against persistent tables still rejected.
- `USE <db>` is now forwarded to the shadow session so its pinned connection
  follows the primary's current database across COM_INIT_DB changes.

### Removed

- Shared shadow worker pool / `max_concurrent` as a throughput cap. Shadow
  concurrency now follows the number of active primary sessions, which is
  itself capped by `proxy.max_connections`.
- `shadow.async` config field (it was never consulted; shadow has always been
  async).

### Notes for operators

- If you have Datadog scrapers or Prometheus jobs already pointed at
  `/metrics` expecting JSON, update them to parse Prometheus text, or repoint
  at `/metrics.json`.
- Shadow now opens one connection per active primary session. With the
  default `proxy.max_connections: 1000`, the shadow server may see up to
  ~1000 connections. Size accordingly or lower `proxy.max_connections` /
  `shadow.sample_rate` if that's more than your shadow can handle.
- Temp-table DML forwarding is conservative: multi-table forms (UPDATE/DELETE
  with JOIN) are rejected even if one table is a tracked temp. This avoids
  accidentally mutating persistent tables if a typo targets the wrong one.

<a id="v0.0.2"></a>
## v0.0.2

_Released 2026-04-17._

### Highlights

- **Prepared statement pass-through** — unblocks most modern MySQL clients
  (Go `database/sql`, JDBC, PDO) that default to the binary protocol.
- **Real-time shadow traffic** with response comparison, ignore-query
  patterns, and per-digest latency stats.
- **Graceful shutdown** with session drain + fix for two close-on-channel
  panics discovered under `-race`.
- **`/healthz` + `/metrics` endpoint** (JSON format in v0.0.2; became
  Prometheus in v0.0.3).
- **CI** with `-race` on every push; release workflow auto-runs bench and
  appends a latency table to the Release body.

### Added

- Prepared statement support (`HandleStmtPrepare/Execute/Close`) via
  pass-through to the backend's `*client.Stmt`.
- Real-time shadow traffic with per-session routing, response comparison
  engine, and JSONL diff report.
- Ignore-pattern whitelist (`comparison.ignore_queries`) — regex list that
  marks matching queries as `ignored` rather than `differed`.
- Per-digest latency stats (avg / p95 / p99) with reservoir sampling so
  memory stays bounded regardless of workload duration.
- Graceful SIGINT handling in offline replay: saves checkpoint before exit
  so the next run resumes cleanly.
- `/healthz` and `/metrics` HTTP endpoints on `proxy.metrics_addr`.
- `LOG_LEVEL` / `LOG_FORMAT` env vars for structured logging (slog).
- Shadow `timeout` and `readonly` config (readonly always enforced).
- `logging.redact_args` — opt-in redaction of prepared-stmt bind values in
  the SQL log.
- Checkpoint file for offline replay with auto-resume and optional
  auto-delete of completed input files.
- Query digest normalizer that groups parameterized variants together.
- Bench tool that runs identical queries direct-vs-proxy and reports
  latency overhead (appended to each GitHub Release).

### Fixed

- Two `send-on-closed-channel` panics (`Logger`, `ShadowSender`) discovered
  under `-race`.
- `MaxConnections` config is now actually enforced.
- Accept loop backs off exponentially on listener errors instead of
  spinning.
- Byte-slice rendering in captured rows (MySQL VARCHAR columns no longer
  show up as `[97 108 105 99 101]` in diff reports).
- Backend connect now uses a 10-second timeout.
- Offline replay checkpoint is saved periodically (every 5s) during replay,
  not only at file end.
- Shadow queries now `USE <db>` first so unqualified table references hit
  the right schema.
- Digest strips SQL comments so trace-annotated queries group correctly.

<a id="v0.0.1"></a>
## v0.0.1

_Released 2026-04-17._

Initial release. Basic MySQL proxy with:

- Transparent query forwarding to a single backend
- Optional TLS on both client-side and backend-side (independent)
- Async JSONL SQL logging with rotation and hot-reloadable enable/disable
- Offline replay from log files with checkpoint tracking
- Benchmark tool comparing direct vs proxy latency
- GoReleaser-driven multi-platform release (linux/darwin/windows × amd64/arm64)
