# AGENTS.md

This file provides guidance to agents working in this repository.

## Purpose and Precedence

- MUST means required.
- SHOULD means recommended unless there is a concrete reason to deviate.
- MAY means optional.
- Root `AGENTS.md` defines repository-wide defaults. Deeper-path `AGENTS.md`
  files (if any) take precedence for that subtree.

## Non-negotiables

1. **Correctness first.** This proxy sits in the SQL data path; small changes
   can alter wire-protocol behavior, session state, transactional semantics, or
   shadow traffic safety.
2. **Keep diffs minimal.** Avoid unrelated refactors, broad renames, or
   formatting-only churn unless explicitly requested.
3. **No speculative behavior.** Do not invent APIs, defaults, or test
   workflows; verify against the code.
4. **Leave verifiable evidence.** Run targeted checks before claiming done and
   report the exact commands.

## Pre-flight Checklist

1. Restate the task goal and the acceptance criteria.
2. Locate the owning subsystem (`Repository Map`) and the closest existing
   tests.
3. Pick the smallest valid validation set (`Task → Validation Matrix`).
4. Match the change to the verification profile (`Verification Before Claiming
   Done`) before claiming completion or opening a PR.

## Verification Before Claiming Done

These are the exact checks CI runs (see `.github/workflows/ci.yml`). Local
agents MUST run them and verify clean output **before** claiming completion or
pushing for review. Skipping any of these has been the cause of every CI
failure on this repo to date.

| Check | Command | Notes |
| --- | --- | --- |
| **gofmt** (mandatory) | `gofmt -l .` | MUST print **no output**. If any file is listed, run `gofmt -w <files>` and re-check. Adding/removing struct fields commonly re-shuffles tag alignment — inspect with `gofmt -d <file>` first. |
| **go vet** | `go vet ./...` | MUST exit 0. |
| **golangci-lint** | `golangci-lint run` (v2.0) | MUST exit 0. Linters enabled live in `.golangci.yml`. |
| **build** | `go build ./...` | MUST exit 0. |
| **unit tests** | `go test -race -count=1 ./internal/...` | MUST exit 0. Race detector is mandatory in CI. |
| **integration tests** (`./test/...`) | `go test -race -count=1 -timeout=120s ./test/...` | Need two MySQL servers reachable at `MYSQL1_ADDR` (default `127.0.0.1:3306`) and `MYSQL2_ADDR` (default `127.0.0.1:3307`). Tests use `skipIfNoMySQL` to skip locally when servers aren't reachable; CI provides them via service containers. |

If you cannot run an integration test locally (e.g., no two MySQL servers
available), say so explicitly when reporting results — do not claim coverage
you didn't achieve.

## Repository Map (Entry Points)

| Path | Purpose |
| --- | --- |
| `cmd/mysql-interceptor/` | Main binary; subcommands `serve`, `replay`, `bench`. |
| `internal/proxy/` | TCP listener, per-connection lifecycle, MySQL handshake bridging. `server.go` is the entrypoint; `handler.go` implements the go-mysql `server.Handler` for query forwarding + shadow + audit logging. |
| `internal/backend/` | Backend connection factory (`Connect`) and a small connection pool used by offline replay. |
| `internal/replay/` | Shadow sender (`shadow.go`, `session.go`) and offline replayer (`offline.go`). Query category classification (`filter.go`) gates what is safe to forward. Checkpoint tracking lives in `checkpoint.go`. |
| `internal/compare/` | Response comparison engine (`engine.go`), per-digest stats (`digest_stats.go`), reporter (`report.go`). `result.go` defines `CompareResult`, the JSONL line format. |
| `internal/logging/` | Async query log writer with rotation. |
| `internal/config/` | YAML config types, defaults (`applyDefaults`), validation, and hot-reload watcher (`watcher.go`). |
| `internal/metrics/` | `:9090` HTTP server exposing `/healthz`, `/metrics` (Prometheus/OpenMetrics), `/metrics.json`, `/debug/vars`. Metrics are unlabeled by default; pass `Labels{Cluster: ...}` to add a `cluster` label. |
| `internal/tlsutil/` | TLS config builders for client-side and backend-side. |
| `test/` | Integration tests against real MySQL servers (proxy forwarding, shadow comparison, offline replay, prepared statements, graceful shutdown). |
| `dashboards/` | Importable monitoring dashboards (currently a Datadog dashboard for the `cluster` label). |

## Task → Validation Matrix

Use the smallest set that still proves correctness; don't run integration tests
for a doc-only change.

| Change scope | Minimum validation |
| --- | --- |
| Proxy / handshake / session lifecycle (`internal/proxy/`) | gofmt + vet + lint + `go test -race ./internal/proxy/...` + integration if behavior is observable from a real client. |
| Shadow / offline replay (`internal/replay/`) | gofmt + vet + lint + `go test -race ./internal/replay/...` + integration when shadow safety changes. |
| Compare engine / reporter (`internal/compare/`) | gofmt + vet + lint + `go test -race ./internal/compare/...`. |
| Config / YAML schema (`internal/config/`) | gofmt + vet + lint + `go test ./internal/config/...`. Update `config.example.yaml` when adding fields. |
| Metrics module (`internal/metrics/`) | gofmt + vet + lint + `go test ./internal/metrics/...`. Update `dashboards/` when adding metrics. |
| Pure refactor / docs / config example | gofmt + vet + lint. |
| Anything touching JSONL log format or comparison output schema | The above + an integration test asserting the new field/key. |

## Conventions

### Go style

- Follow nearby code first; the repo is small and the style is consistent.
- `gofmt` is mandatory (no whitespace-only nits get past CI).
- `slog` is the structured logger; do not introduce `log.Printf`.
- Concurrency primitives are atomics + channels + `sync.WaitGroup` where
  appropriate. Prefer atomic counters over mutex-guarded ints; the metrics
  package follows this pattern.
- New goroutines that are spawned in a constructor MUST have a clean teardown
  path tied to a context or a `sync.WaitGroup` consumed by `Close()`.
- Backwards-compatibility for the JSONL output format matters. Do not rename
  existing JSON keys silently. Use `json:"...,omitempty"` for new optional
  fields so empty values don't bloat the diff stream.

### Comments

- Default to writing no comments. Explain *why*, not *what*.
- Keep doc comments on exported symbols and on tricky invariants
  (concurrency guarantees, lifecycle ordering, MySQL protocol quirks).

### Tests

- Unit tests live next to the code they test; integration tests in `test/`.
- Race detector is required: write tests so `-race` would catch real bugs
  (don't use random sleeps to paper over ordering).
- Use `t.TempDir()` and short timeouts for tests that involve filesystem or
  goroutine timing — never write to a hard-coded path.

### Configuration

- New YAML fields go in `internal/config/config.go`; document them in the
  struct comment, add a default in `applyDefaults` if needed, and add an
  example line to `config.example.yaml`.
- "Negative disables, 0 → default" is the established convention for
  duration fields (see `SummaryInterval`, `HeartbeatInterval`).

### Metric labels and dashboards

- The metrics endpoint is unlabeled by default; pass `metrics.Labels{...}`
  when constructing the server to attach labels (see `proxy.cluster`).
- When adding a new metric, also update `dashboards/datadog-mysql-interceptor.json`
  if it's user-visible. Mention the metric prefix assumption
  (`mysql_interceptor.`) in the dashboard description.

## PR Rules

- **Title**: short and scoped. Match the touched subsystem when obvious
  (`compare: ...`, `replay: ...`, `metrics: ...`, `proxy: ...`).
- **Description**: brief Summary + Test plan. Use a `<<'EOF'` heredoc
  when generating via `gh pr create` so backticks and quotes are preserved
  literally — escaping inside a single-quoted heredoc inserts the literal
  backslash and shows up as `\`` in the rendered body.
- **Force-push**: prefer follow-up commits. If a force-push is unavoidable,
  use `--force-with-lease`. Do not force-push branches with active reviews
  without coordinating.
- **Stacked PRs**: when a feature naturally builds on an open PR, target the
  open PR's branch as the base and call out the dependency in the
  description. Don't bundle unrelated concerns into one PR.

## Common Pitfalls (from prior CI failures)

- **gofmt after struct field additions.** Adding a field with a longer name or
  longer tag in the middle of a struct can re-trigger column re-alignment on
  the surrounding fields. Always run `gofmt -l .` before push.
- **Backtick escaping in `gh pr create` bodies.** Single-quoted heredocs
  (`<<'EOF'`) preserve content literally — adding `\` before backticks or
  quotes inside one inserts the literal backslash into the PR body.
- **Branching off the wrong base for stacked work.** If a new feature depends
  on fields/methods introduced by an open PR, branch off that PR's head and
  target the same branch as your base. Cherry-picking onto `main` will
  conflict.
- **Test changes that drop coverage of the user-facing JSONL format.** When
  you change `CompareResult` or `LogEntry`, update both the corresponding
  unit test in `internal/compare/` (or `internal/logging/`) AND any
  consumer-facing assertions in `test/integration_test.go`.

## Agent Output Contract

When reporting completion or readiness, include:

1. The exact commands you ran (verbatim from the table above).
2. The result of each (e.g. `gofmt -l .` → no output; `go test -race ./...` →
   `ok`).
3. If any check was skipped (e.g., integration tests with no MySQL locally),
   say so explicitly.
4. The branch name and any open PR URLs.

Don't claim "all tests pass" or "ready for review" without a `Ready` profile
run (gofmt + vet + lint + build + race-enabled unit tests). Integration tests
are recommended but acceptable to skip when infrastructure isn't available
locally — say so when reporting.
