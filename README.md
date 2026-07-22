# Codex usage

`codex-usage` is a local-first browser application for exploring Codex sessions,
activity, token usage, and estimated cost. It reads the active and archived Codex
JSONL histories on this Mac, stores normalized facts in SQLite, and serves a
React interface from the same Rust process.

## Shape

- `src/` — Rust ingestion, SQLite queries, pricing, and Axum API/server.
- `migrations/0001_initial.sql` — the complete SQLite schema for a fresh projection.
- `frontend/` — React/TypeScript application.
- `tests/fixtures/` — small real-data-derived histories used to validate lineage and
  rich event representation.

The JSONL histories remain the source of truth. The database keeps distinct
thread, rollout, turn, agent, event, tool, and usage identities. Fork lineage
remains stored, while copied inherited records are not projected as new
messages, events, tools, or usage.

The SQLite file is a compact query projection, not a second copy of the
histories. Tool calls retain only their identity, type, status, timing, lineage,
and usage attribution. Tool arguments, results, raw payloads, generated images,
and message attachments are never copied into SQLite or exposed by the API.
Authored and captured text, assistant messages, and reasoning summaries remain
available in Activity. The session ID links back to the full session in Codex
when detailed source inspection is needed.

User-visible task names come from Codex's append-only `session_index.jsonl`.
Generated titles and later renames update without replaying the corresponding
rollout. A rollout rename or compact first prompt is used only when that index
does not contain the thread.

Observed model IDs are retained verbatim; prices and explicit aliases are joined
when totals are read, so adding a missing price or alias updates historical
estimates immediately.

## Run

Run the application from the repository root. Startup rejects other working
directories, and there is no installed-binary workflow.
Rust and Node versions are pinned in `rust-toolchain.toml` and
`frontend/.node-version`.

```sh
npm --prefix frontend ci
npm --prefix frontend run build
cargo run
```

`cargo run` starts the server and continuous ingestion. Run `cargo run -- ingest`
for a one-shot ingestion pass. The default UI is served at
<http://127.0.0.1:5610>. Use `--db`, `--sessions`, and `--archive` to run against
an isolated database or fixture roots. Both `cargo run` and
`cargo run -- serve` honor the `CODEX_USAGE_*` environment variables shown by
`cargo run -- serve --help`.
Manual prices and model aliases are saved in `codex-usage.pricing.json` beside
the database. Use `--pricing-config` to choose another path.

The SQLite projection is disposable. After a schema change, stop the server,
rebuild `codex-usage.db` from the JSONL histories with `cargo run -- ingest`,
then start the server again. The pricing JSON sidecar is independent of that
rebuild.

## Checks

```sh
cargo test
cargo check --all-targets --all-features
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
npm --prefix frontend test
npm --prefix frontend run test:e2e:functional
npm --prefix frontend run test:e2e:performance
npm --prefix frontend run lint
npm --prefix frontend run build
```

The ordinary debug suite skips the large-session Activity performance gates. Run
both explicitly when changing Activity queries, attribution, rollups, or indexes.
They cover a 500,000-event/tool session, deep numeric and cursor pages, and a
separate 500,000-fact usage-heavy turn:

```sh
cargo test --release --lib activity_large_session_query_and_assembly_stays_within_regression_budget -- --ignored --nocapture
cargo test --release --lib activity_usage_heavy_queries_stay_under_one_second -- --nocapture
```

The browser suite builds and launches the real application against a temporary
SQLite database, copied fixture session roots, and a loopback-only pricing
server. It never reads the live Codex histories or `codex-usage.db`. Install its
Chromium runtime once with `npm --prefix frontend run test:e2e:install` when
Playwright requests it.
