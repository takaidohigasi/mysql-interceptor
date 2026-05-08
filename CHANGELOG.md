# Changelog

All notable changes to mysql-interceptor are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/),
and the project adheres to [Semantic Versioning](https://semver.org/) once it
reaches 1.0 (everything before is 0.y.z with breaking changes possible between
minor versions).

<a id="v0.0.7"></a>
## v0.0.7

_Released 2026-05-08._

Comparison hot-path overhaul plus sensitive-column redaction. Four
stacked perf PRs progressively eliminate allocator and lock pressure
on `Engine.Compare → Reporter.Record`; on the diff-emit path the
steady-state allocation count drops from ~5 allocs/op to 0 and per-
record latency falls roughly **35–60%** in microbench. No wire-format
or config changes are required to opt in — every change preserves
byte-identical JSONL output and existing operator workflows.

### Highlights

- **Sensitive column redaction in diff records.** New
  `comparison.redact_columns` allow-list and `comparison.redact_all_values`
  global override let operators learn _that_ a security-relevant column
  drifted (the `type` / `column` / `row` / timing fields stay intact)
  without leaking the data into `comparison.output_file`. Sibling to
  the existing `logging.redact_args` for prepared-statement bind values
  in the audit log. (#22)
- **Comparison hot-path is now zero-allocation steady-state.** Four
  layered changes, each measured in isolation:
  1. **Sharded `DigestStats`** — replaces the single `sync.Mutex` + map
     with 32 cache-line-aligned shards (#24).
  2. **Async reporter** — `sync.Mutex`-around-`Encode` becomes a
     buffered channel + single writer goroutine; producers
     JSON-marshal in parallel (#24).
  3. **`bufio.Writer`** wrapping the report file — coalesces ~22
     records per syscall, replacing ~10k Write syscalls/sec at
     sustained 5%-diff-rate × 200k qps with ~450/sec (#25).
  4. **Hand-rolled `appendJSON`** for `CompareResult` /
     `Difference` / `HeartbeatRecord` — drops `encoding/json`
     reflection + interface dispatch from the hot path. Wire format
     is byte-identical to `json.Encoder` with `SetEscapeHTML(false)`,
     pinned by a compatibility test (#25).
  5. **`*CompareResult` pool + Differences slice reuse** —
     `Engine.Compare` pulls a zeroed entry from a pool; callers
     release after `Reporter.Record`. The Differences slice keeps
     its capacity across pool round-trips (#25).
- **`comparisons_report_dropped` metric.** New counter exposes diff or
  heartbeat records dropped because the async writer's channel was
  full (consumer fell behind producers). Dropping is preferred to
  back-pressuring the proxy hot path; the metric lets operators tell
  when the safety valve is firing. (#24)
- **Capture-after-filter on the shadow path.** `ShadowQuery` now
  carries an optional `Capture func()` callback that
  `ShadowSession.Send` invokes only after the `enabled` /
  `sample_rate` / CIDR / category gates pass. Today ~20% of sends
  fail the category check (DML against persistent tables); the
  primary-side `captureResult` walk is now skipped for those. With
  `sample_rate < 1.0` or CIDR filters the savings are larger. (#24)

### Added

- **`comparison.redact_columns`** — list of column names whose
  `cell_value` diff payloads have `original` / `replay` replaced with
  `"<redacted>"`. The diff record is still emitted (so operators see
  _that_ the column drifted) — only the values are masked. Other
  diff types (`column_count`, `row_count`, `affected_rows`,
  `column_name`) carry counts or schema names, not user data, and
  are unaffected. (#22)
- **`comparison.redact_all_values`** — global override. When `true`,
  every `cell_value` AND `error` diff is redacted regardless of
  column. Defense-in-depth for environments where any value leak is
  unacceptable, or as a fallback against an incomplete
  `redact_columns` list. (#22)
- **`comparisons_report_dropped`** metric (counter) — diff /
  heartbeat records dropped because the async reporter writer queue
  was full. (#24)
- **`AcquireCompareResult` / `ReleaseCompareResult`** — package-level
  helpers in `internal/compare`. Engine.Compare pulls from the pool;
  callers release after `Reporter.Record` consumes the result. Tests
  that construct `*CompareResult` literally don't need to use
  these — short-lived test allocations are cheap. (#25)
- **`ShadowQuery.Capture`** — optional `func() *compare.CapturedResult`
  callback that defers materialization of `OrigResult` until after
  `Send`'s gate checks. Producers should set `Capture` OR
  `OrigResult`, not both. Direct callers (offline replay, tests) can
  still pre-fill `OrigResult` and leave `Capture` nil for the
  original eager-capture behavior. (#24)

### Changed

- **`Reporter` write path is now async.** `Reporter.Close` is
  idempotent (`sync.Once`-guarded) and drains the writer goroutine
  before closing the underlying file. Concurrent `Record` calls
  during `Close` remain unsafe by contract — existing usage
  (`ShadowSender.Close` + `bgWG`-gated teardown,
  `OfflineReplayer.Run` deferred close) already serializes
  correctly. (#24)
- **Reporter writes are buffered.** File-backed sinks now wrap the
  underlying file with a 16 KiB `bufio.Writer`; auto-flush when the
  buffer fills + 250 ms ticker bound the latency for sparse traffic.
  Stdout sinks (`output_file = ""` or `"-"`) skip bufio so human
  tails see records immediately. (#25)
- **`DigestStats.Record` reuses the precomputed digest.**
  `Engine.Compare` already populates `result.QueryDigest`;
  `DigestStats.Record` no longer recomputes it on every call. The
  defensive fallback preserves behavior for callers (mostly tests)
  that bypass `Engine.Compare`. (#23)

### Trade-offs to be aware of

- The new bufio writer means a process crash (panic, SIGKILL without
  defer) can lose up to **16 KiB or 250 ms of records**, whichever
  comes first. The pre-bufio behavior already lost records on OS
  crashes (no `fsync` is performed); this widens the window slightly
  on _process_ crashes. The diff report is a diagnostic / audit log,
  not a transaction log; the bounded loss window is acceptable for
  this workload. Operators who need stronger durability should
  forward `comparison.output_file` through a sidecar that fsyncs.

### Notes for operators

- Wire format on `comparison.output_file` is unchanged. Hand-rolled
  encoding emits byte-identical bytes to `json.Encoder` with
  `SetEscapeHTML(false)` — the existing
  `TestAppendJSON_*_MatchesEncodingJSON` tests pin this against a
  representative corpus including UTF-8, control characters, U+2028
  / U+2029, and HTML chars.
- No config migration required for the perf changes.
- For redaction, start with `comparison.redact_columns: ["hashed_password",
  "email", ...]`. Consider `comparison.redact_all_values: true` in
  environments where any value leak is unacceptable.
- The `comparisons_report_dropped` metric should normally be 0. A
  sustained nonzero rate indicates the report file is too slow for
  the diff arrival rate — investigate disk throughput or reduce
  `comparison.log_matches`.

<a id="v0.0.6"></a>
## v0.0.6

_Released 2026-05-08._

Patch release for two latent shadow-session bugs that the v0.0.5
shadow E2E test surfaced under `-race`.

### Fixed

- **Data race in shadow session timeout / ctx-cancel paths.** When
  the per-query timeout fired (or the session ctx was cancelled),
  `ShadowSession.processQuery` called `ss.conn.Close()` while the
  Execute goroutine was still inside `client.Conn.Execute`. go-mysql's
  `*client.Conn` is not safe for concurrent Close-while-Execute —
  `Close` clears `packet.Conn.Sequence` at the same time
  `writeCommand` mutates it — and the race detector flagged this on
  `TestShadowE2E_TempTableInsertForwardedPersistentInsertNot` after
  the v0.0.5 release. The fix uses `net.Conn.SetDeadline`
  (goroutine-safe per stdlib) to abort the in-flight Execute, drains
  the per-query goroutine, and only then calls `Close`. The same
  drain-before-close sequence also fixes a secondary race in
  `ShadowSession.Close()` that codex review flagged: with every exit
  path from `processQuery` now draining the Execute goroutine,
  `<-ss.done` once again means "everything is quiet". Latent since
  the shadow timeout feature shipped; surfaced by the v0.0.5 E2E
  test. (#19)
- **Dropped queries on shadow session shutdown.** Two loss paths in
  `ShadowSession`: (1) `run()`'s outer `select` could exit via
  `<-ctx.Done()` while `queryCh` still held buffered queries — Go's
  `select` is pseudo-random over ready cases — silently losing every
  remaining audit record; (2) `processQuery`'s ctx-cancel arm
  unconditionally aborted the Execute goroutine even when Execute
  had already finished, throwing the result away. The fix adds a
  non-blocking `drainOnShutdown` pass in `run()` after `ctx.Done()`,
  and a non-blocking peek at `done` in `processQuery`'s ctx-cancel
  arm so completed-but-not-yet-recorded results are preserved.
  `abortInFlightExec` now returns the (deadline-induced) result so
  in-flight queries that get aborted are recorded as error diff
  lines instead of silently dropped. Latent since the shadow session
  machinery shipped; #19's drain-before-close shifted timing enough
  to make the flake visible on
  `TestShadowE2E_TempTableInsertForwardedPersistentInsertNot`
  post-merge. (#21)

<a id="v0.0.5"></a>
## v0.0.5

_Released 2026-05-08._

### Highlights

- **Comparison output is now diff-focused.** Matched and ignored records
  are suppressed inline by default (use `comparison.log_matches: true`
  to keep the old "log every comparison" behavior). A new periodic
  `"type":"heartbeat"` line, written through the same JSONL stream,
  carries window counts so operators can still tell the proxy is alive
  when traffic is mostly clean. In dev this drops pod-stdout volume
  from ~100% of comparisons to ~6% mismatches plus one heartbeat per
  minute.
- **Per-digest latency summary on a timer.** The shadow sender already
  collected per-digest avg/p95/p99 latency for primary vs. replay; it's
  now logged via `slog` on a configurable cadence (`comparison.summary_interval`,
  default `1h`) instead of only at shutdown — useful on long-running
  pods where waiting for shutdown isn't practical.
- **`cluster` label on metrics.** Set `proxy.cluster` and every line on
  `/metrics` is rendered as `metric_name{cluster="<value>"} <value>`.
  An importable Datadog dashboard with a `$cluster` template variable
  ships in `dashboards/datadog-mysql-interceptor.json`.
- **User identity on diff records.** When a comparison turns up a real
  divergence, the JSONL line now carries `"user":"..."` so operators
  can answer "whose query was that?" without cross-referencing the audit
  log. Set only on `match=false && !ignored` records to keep the
  output focused.
- **`AGENTS.md` at the repo root.** Captures the verification checklist
  that mirrors CI line-for-line, the repository map, conventions, and
  a "Common Pitfalls" section so agents (Claude / Codex / etc.) don't
  repeat past CI failures (gofmt re-alignment, heredoc escaping,
  stacked-PR base mistakes).

### Added

- **`comparison.summary_interval`** — cadence at which the shadow sender
  logs the cumulative per-digest summary via `slog`. Default `1h`.
  Negative disables the periodic log; the existing shutdown summary
  still fires regardless. Only shadow mode honors this setting; offline
  replay prints its summary at completion as before. (#13)
- **`comparison.log_matches`** — when `false` (default), only diffs are
  written inline; matched and ignored comparisons are summarized by the
  heartbeat instead of one line per query. Set `true` for a full audit
  trail. Shadow mode only — offline replay always writes a complete
  report regardless. (#14)
- **`comparison.heartbeat_interval`** — cadence (default `1m`, negative
  disables) at which the reporter writes a `"type":"heartbeat"` line to
  `comparison.output_file` summarizing the previous window
  (`window_total` / `window_matched` / `window_differed` /
  `window_ignored` since the last tick, plus `cumulative_total` /
  `cumulative_differed`). (#14)
- **`proxy.cluster`** — optional config field. When set, every line on
  `/metrics` is rendered as `metric_name{cluster="<value>"} <value>`,
  so a single dashboard can break stats down per database cluster via a
  template variable. Empty (default) emits unlabeled metrics —
  byte-identical to the pre-change output for single-cluster
  deployments. (#15)
- **`dashboards/datadog-mysql-interceptor.json`** — importable Datadog
  dashboard with a `$cluster` template variable. Panels: diff fraction,
  comparison rate breakdown, throughput, sessions, shadow drops/skips,
  errors, Go runtime. Metric prefix assumes `mysql_interceptor.`;
  adjust if your scrape config uses a different namespace. (#15)
- **User identity on diff records** — `CompareResult.User` is set from
  the inbound handshake (shadow mode) or `LogEntry.User` (offline mode)
  whenever the result is a real divergence (`match=false &&
  !ignored`). Matched and ignored records leave it empty
  (`json:"user,omitempty"` keeps the field out of the output). (#16)
- **End-to-end shadow temp-table test** — verifies INSERTs against a
  temporary table are forwarded to the shadow while INSERTs against
  persistent tables are not. (#13)
- **`AGENTS.md`** — repo-root agent guide modeled on the
  [tidb AGENTS.md](https://github.com/pingcap/tidb/blob/master/AGENTS.md):
  verification checklist mirroring CI, repository map, task→validation
  matrix, conventions, PR rules, and an enumerated "Common Pitfalls"
  section listing the actual CI failures hit during the v0.0.5 work.
  (#17)

### Changed

- **Default comparison output is now diff-only.** Existing operators
  who relied on every match landing in `output_file` should set
  `comparison.log_matches: true` to keep prior behavior. The diff
  stream is otherwise unchanged.
- **`metrics.NewServer` signature** — now takes `metrics.Labels{}` as
  a second argument. Internal-only API; the only call site
  (`cmd/mysql-interceptor/main.go`) is updated. Single-cluster
  deployments can pass `metrics.Labels{}` to keep byte-identical output.
- **`compare.Engine.Compare` signature** — now takes a `user string`
  parameter between `query` and `sessionID`. Internal-only API; pass
  `""` if no user is known.
- **Reporter constructor refactor** — new canonical
  `compare.NewReporterFromOptions(ReporterOptions{...})`. The existing
  `NewReporter(outputFile)` and `NewReporterWithDigestCap(outputFile,
  cap)` keep working and delegate to it.

### Fixed

- **Shadow sender shutdown ordering** — the periodic summary and
  heartbeat goroutines are now tracked on a `sync.WaitGroup`, and
  `Close()` waits on it before emitting the final
  `"shadow sender closed"` log line. Without this, a tick that fired
  just before cancellation could log after shutdown. (#13 follow-up)
- **Offline replay always writes a full report** — the new
  `LogMatches=false` default suppresses matched/ignored entries from
  shadow output, but offline replay's report file *is* the output.
  `NewOfflineReplayer` now hard-codes `LogMatches=true` regardless of
  config so the report stays complete. (#14 follow-up)

### Migration notes

If you currently scrape `/metrics` and were relying on the unlabeled
output:

- Leaving `proxy.cluster` unset preserves byte-identical output.
- Setting `proxy.cluster: "<name>"` adds `{cluster="<name>"}` to every
  line. Update Prometheus / Datadog queries to either ignore the new
  tag or filter on it.

If you were tailing `comparison.output_file` and counting JSONL lines
as "comparisons":

- After upgrading, mismatch lines are unchanged but matched/ignored
  lines disappear by default. Filter heartbeat records with
  `jq -c 'select(.type=="heartbeat")'` for window counts, or set
  `comparison.log_matches: true` to restore the old behavior.

<a id="v0.0.4"></a>
## v0.0.4

_Released 2026-04-27._

### Highlights

- **Multi-user authentication.** The proxy now accepts a list of
  `(username, password)` pairs in `proxy.users` and reuses the matched
  credentials for the outbound backend (and shadow) connection, so
  per-user GRANTs apply consistently and the SQL log records the actual
  authenticated user. `proxy.users` is **required** — single-user mode
  via `backend.user` / `backend.password` has been removed.
- **Env-var interpolation in config.** Any `${VAR}` in `config.yaml` is
  expanded at load time (and on each hot-reload), so credentials can
  live in env vars / Secret Manager.
- **`proxy.max_session_lifetime`** — hot-reloadable cap on session
  age, with ±10% jitter, that closes sessions at the next safe boundary
  (between commands, only when the backend is not in a transaction).
  Lets the client reconnect and rebalance onto the current backend pool
  after the backend autoscales.

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
- **`proxy.max_session_lifetime`** — caps how long a client session may
  remain open, with ±10% per-session jitter. After the deadline elapses
  the proxy closes the session at the next safe boundary (between
  commands, only when the backend is not in a transaction), letting the
  client reconnect and rebalance onto the current backend pool. Useful
  when the backend autoscales and existing pinned connections would
  otherwise concentrate load on the older nodes. Hot-reloadable; 0
  (default) disables. New metrics `sessions_closed_max_lifetime` and
  `sessions_lifetime_postponed` expose the close decisions.

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

### Notes for operators

- This is a breaking change for any deployment relying on
  `backend.user` / `backend.password`. Update `config.yaml` to set
  `proxy.users` before upgrading.
- The `${VAR}` expansion combines naturally with the new `proxy.users`:
  store credentials in env vars / Secret Manager and reference them
  from `users:` entries.
- `proxy.max_session_lifetime` is **off by default**. Enable it (e.g.
  `1h`) when the backend autoscales and you want existing sessions to
  rebalance onto new nodes without a restart.

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
