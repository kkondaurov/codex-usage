# Codex Usage Architecture Refactor

This is the living execution plan for decomposing `src/api.rs` and
`src/ingest.rs`. Update the progress checklist and decision log after every
completed gate. This is a working document for the refactor, not user-facing
project documentation.

## Stable baseline

- Git tag: `v0.1.0`
- Commit: `1911d4aa33826bd467749a96a24ed36b41b865cb`
- Branch: `main`
- Baseline behavior: the tagged application is the rollback point.
- Live data policy: do not rebuild, mutate, or benchmark against the user's
  live projection. Use disposable databases and fixture roots.
- Commit policy: do not commit or push refactor work unless the user explicitly
  asks.

## End-state rules

1. `src/api.rs` and `src/ingest.rs` disappear.
2. There is no permanent forwarding facade and no global service locator.
3. The central application module only constructs dependencies and merges
   already-built feature routers.
4. Features do not import another feature's handlers or transport DTOs.
5. Shared code names a real concept (`costing`, `usage`, `storage`, `calendar`,
   `conversation`, or `read_runtime`); there is no `common`, `helpers`, or
   generic query drawer.
6. HTTP transport contains no SQL. Query/persistence modules contain no Axum.
7. Ingestion source mechanics contain no SQLite or projection behavior.
8. Protocol decoding contains no filesystem or SQLite behavior.
9. Projection handlers operate through a constrained projection context rather
   than arbitrary orchestration or filesystem access.
10. Schema, projector generation, checkpoint serialization, API behavior, and
    query algorithms remain unchanged unless a separately characterized change
    is explicitly required.

## Intended module ownership

```text
app
├── web              server, HTTP boundary, errors, read runtime
├── calendar         civil-time primitives, not feature bucket policy
├── conversation     shared message/tool display interpretation
├── system           status and settings
├── pricing          catalog, mutations, manual store, refresh
├── sessions         catalog, list, summary
├── activity         root pages, detail, groups, previews, attribution
└── analytics        separate overview and stats slices, prewarm

system ────────> usage ───────> costing
sessions ──────> usage ───────> costing
activity ──────> usage ───────> costing
analytics ─────> usage ───────> costing
pricing ──────────────────────> costing

ingestion coordinator
├── source catalog and handoff planning
├── file ingestor
│   ├── source snapshot / bounded JSONL / fingerprints
│   ├── checkpoint policy
│   ├── protocol decoder
│   └── projection transaction
├── reconciliation
├── session-title import
└── scan-attempt recording
```

## Step discipline

For every extraction:

1. Identify or add an external characterization test against the current code.
2. Prove the test fails under one temporary representative mutation when test
   sensitivity is uncertain, then revert that mutation.
3. Move one cohesive boundary without mixing in algorithm cleanup.
4. Run focused unit tests and the relevant external contract suite.
5. Run the full Rust suite and static gates before beginning the next numbered
   stage.
6. Run browser and performance gates immediately after route composition,
   Analytics, Sessions, or Activity changes.
7. Stop and correct the boundary if canonical output changes, visibility widens
   unnecessarily, or a new module reaches backward into a legacy god module.

Existing black-box tests stay outside the implementation being moved wherever
possible. Pure value and parsing tests may move with their owning module.

## Progress

### Stage 0 — Behavioral oracles and architecture guardrails

Status: **complete**

- [x] Record the baseline Rust test inventory and cold-performance sample.
      Rust inventory recorded: 318 tests discovered, 315 normal tests passing,
      and 3 ignored release-scale Activity gates. Frontend baseline: 230 unit
      tests passing, lint clean, production build clean. Functional production
      Chromium baseline: 13/13 passing. Cold sample pending.
- [x] Expand corpus validation to a deterministic full logical projection
      snapshot covering threads, rollouts, agents, turns, messages, events,
      tools, usage, activity index, rollups, and checkpoints.
- [x] Add incremental-versus-clean projection equivalence for append, completed
      tail, rewrite, handoff, deletion, and parent/child discovery order.
- [x] Add reviewed black-box API response fixtures for System, Sessions,
      Summary, Activity, Analytics, and Pricing.
- [x] Add HTTP-level executor-class and single-read-snapshot wiring tests.
- [x] Add an enforceable dependency policy for the target module graph.
- [x] Prove representative tests detect deliberate boundary, thread-scope,
      lifecycle, fingerprint, and executor-class regressions.
- [x] Run the complete baseline gate with all new protections.

Gate:

- `cargo fmt --all -- --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- Frontend unit, lint, and production build
- Functional Chromium E2E
- Five-sample cold browser performance
- Three release-scale Rust Activity gates

### Stage 1 — Shared foundations

Status: **complete**

- [x] Extract Web server/boundary/error/read-runtime infrastructure.
- [x] Extract SQLite storage/executor/lock ownership.
- [x] Move the bounded worker pool into Storage as `StorageExecutor`; delete
      the obsolete root module and preserve one shared control/heavy/light
      capacity policy.
- [x] Move database-operation advisory locking into Storage as `DatabaseLock`;
      preserve canonical path identity and pre-open scanner ownership, keep
      the manual-pricing file lock Pricing-owned, and delete the obsolete root
      module.
- [x] Move `Db`, SQLite path/file/WAL mechanics, schema lifecycle, and their
      tests behind `storage::Db`; delete the obsolete root database module and
      preserve the temporary manual-pricing ownership cycle explicitly.
- [x] Split SQLite connection/file/snapshot mechanics from private migration,
      runtime-index, and bundled-price seeding ownership without changing SQL
      or `Db::open_with_pricing_config` initialization order.
- [x] Extract exact Costing and scoped Usage capabilities.
- [x] Enforce the inward dependency direction before extraction: Costing
      cannot import Usage, Storage, Web, or feature slices; Usage cannot import
      Web or feature slices. Register totals under Usage rather than Costing.
- [x] Move exact USD amount and price-rate primitives into Costing; delete the
      obsolete root modules without forwarding facades.
- [x] Remove `Overview*` naming from shared price and rollup semantics.
- [x] Move pure price intervals, alias resolution, fixed-point scalar pricing,
      and group boundary classification into opaque `costing::PriceBook`;
      keep SQL loading feature-owned and remove all `OverviewPrice*` symbols.
- [x] Move serialized exact totals and opaque fact/group accumulation into
      Usage; preserve cached-token clamping, saturation, partial-unpriced cost
      suppression, and the existing JSON contract without compatibility
      aliases.
- [x] Move stable price-book loading and raw half-open global/thread range
      totals into `usage::reader`, preserving supplied-connection snapshot
      affinity and leaving all feature selections and rollups with their
      current owners.
- [x] Keep feature-owned selection SQL out of shared Usage: Sessions retains
      `selected_sessions` / sort-cost machinery; Activity retains
      `selected_activity_*` attribution and exceptional-fact fallback.
- [x] Establish Activity-owned attribution before shared rollups: selected
      root/kind exceptional pricing and fractional-offset local-day splitting
      now live under `activity::attribution`; generic Usage has no Activity
      temp-table knowledge.
- [x] Make the shared direction `usage -> costing`; Costing accepts scalar
      billable-token values and never imports Usage DTOs.
- [x] Replace `ApiState` with state-bound feature routers. A construction-only
      `AppDependencies` may be consumed in `app.rs`, but handlers never receive
      it and it is not Clone.
- [x] Give all read-only handlers one `ReadRuntime { database, executor }` that
      owns cancellation and a single snapshot. Create the executor once and
      share it with ReadRuntime and Pricing; do not default one per feature.
- [x] Preserve the temporary `Database` / manual-pricing ownership cycle
      mechanically in Stage 1, make it explicit, and remove it in Stage 2.

Gate: runtime/API contracts, full Rust suite, browser functional suite, and cold
performance.

### Stage 2 — Pricing and System vertical slices

Status: **complete**

Execution order:

1. Move the manual sidecar and remote synchronization behind Pricing without
   changing behavior or the temporary database cycle.
2. Make `Db` a pure storage handle and hydrate the Pricing-owned sidecar
   explicitly during startup while process ownership is already held.
3. Extract `PricingMutations`, preserving async serialization before blocking
   work and the existing sidecar/SQLite commit order.
4. Extract catalog persistence and state-bound Pricing routes.
5. Extract Status and Settings as independent System capabilities; neither may
   import Pricing or Analytics.

- [x] Extract price/alias catalog routes and queries.
- [x] Move the complete manual sidecar implementation and its tests from the
      obsolete root module into `pricing/manual_store.rs`; remove the root
      compatibility module while preserving the temporary database cycle.
- [x] Move remote synchronization into `pricing/sync.rs` and turn Pricing's
      module root into declarations and narrow feature exports only.
- [x] Move the authoritative JSON sidecar into Pricing-owned
      `manual_store.rs`, remote refresh into `sync.rs`, and async serialized
      write orchestration into `PricingMutations` without changing lock order.
- [x] Make `Db` a pure path/storage handle and break the
      `Db` / `ManualPricingStore` cycle through explicit startup hydration
      while the scanner lease is already held.
- [x] Extract state-bound Pricing routes after catalog/mutation seams exist;
      retain one shared StorageExecutor and current work classes.
- [x] Extract Status and Settings independently. System must not import
      Pricing or Analytics; Settings derives its own transport summary from
      shared Usage totals.

Gate: pricing CRUD/repricing, alias invariants, refresh failure, lock contention,
control-lane availability, shutdown contracts, Settings frontend tests, and
functional E2E.

### Stage 3 — Sessions vertical slice

Status: **complete**

- [x] Establish the two neutral prerequisites first: chrono-only civil-time
      primitives in root `calendar.rs`, and transport pagination mechanics in
      `web/pagination.rs`. Feature range/bucket policy remains feature-owned.
- [x] Extract shared message/tool display interpretation into the neutral
      `conversation/display.rs` concept first; Sessions and Activity may both
      depend on it, but neither may import the other.
- [x] Give Analytics its own top-session read model, then extract Sessions
      catalog/existence/header reads without exporting a cross-feature DTO.
- [x] Give Activity its own visibility and root-rollout reads before Sessions
      catalog extraction; do not retain an Activity-to-Sessions dependency.
- [x] Extract list/search/filter/sort/pagination as one same-connection unit;
      its four TEMP tables and exact 39-digit cost sort key never cross a
      runtime boundary.
- [x] Extract one-snapshot summary/model/agent/tool assembly after stable Usage
      rollups exist, then move the state-bound routes. The detailed pre-cut
      characterization and ownership checklist lives in
      `.agents/stage3-sessions-summary.md`.

Gate: every list mode, exact cost sorting, pagination, summary snapshot
consistency, lineage isolation, frontend tests, functional E2E, and performance.

### Stage 4 — Analytics vertical slice

Status: **complete**

Detailed ownership, pre-cut characterization, extraction order, and gates are
recorded in `.agents/stage4-analytics.md`.

- [x] Consume the established root `calendar.rs` primitives while keeping all
      Analytics range, bucket, label, occupied-year, and skipped-date policy
      inside the appropriate Overview or Stats slice.
- [x] Give Analytics its own top-session read model and extract Overview domain
      then Overview SQL/read assembly; run cold Overview performance
      immediately after the persistence move.
- [x] Extract Stats domain with exact fixed-point grand-total reduction, then
      its distinct normal/broad SQL strategies and occupied-year assembly.
- [x] Move state-bound Analytics routes and prewarming last. Keep Overview and
      Stats as separate vertical slices; do not invent a generic repository or
      response pipeline.

Gate: every range, DST/fractional-offset cases, exact totals, heatmap continuity,
top rankings, statement/index budgets, prewarming parity, functional E2E, and
five-sample cold performance under one second.

### Stage 5 — Activity vertical slice

Status: **complete**

Detailed ownership, pre-cut characterization, extraction order, and gates are
recorded in `.agents/stage5-activity.md`.

- [x] Absorb the existing activity index and both scoped cursor contracts into
      Activity first; delete the root `activity_index.rs` immediately rather
      than forwarding through it.
- [x] Introduce the honest same-connection `PreparedSelection` concept for the
      five general root/turn/descendant/agent TEMP tables; split previews and
      groups from that selection. Event attribution returns scalar/keyed
      totals and never mutates a root-page DTO.
- [x] Extract root-page and day-summary assembly behind a private batch and
      explicit `RootExchange` result; retain Activity-owned existence/lineage.
- [x] Move the event index, collection cursor, recursive read model, and
      five-table same-connection prepared selection into Activity ownership.
- [x] Add exact raw-wire, dispatch-precedence, page-independent day summary,
      review-group cursor/totals, and TEMP-state characterization.
- [x] Extract keyed root/group usage attribution without response-model
      mutation; preserve exceptional fixed-point pricing and NULL-turn facts.
- [x] Extract bounded modern/legacy previews and DTO-free event attribution.
- [x] Extract synthetic groups as one same-connection TEMP-table unit with
      lazy placeholders, bounded detail paging, and page-independent totals.
- [x] Extract detail dispatch after completing root-page assembly.
- [x] Move the state-bound routes last and remove every Activity symbol from
      the legacy API module.
- [x] Remove every Activity dependency on legacy API helpers.

Gate: cursor stability, thread/lineage scoping, lazy child pagination, bounded
preview reads, cross-midnight duration, attribution independent of pagination,
functional E2E, cold performance, and all release-scale gates.

### Stage 6 — Ingestion source mechanics

Status: **complete**

- [x] Freeze serialized compatibility first: add golden tests for
      `CursorState` defaults/field names, chunked-fingerprint encoding, and the
      complete deterministic Catalog tie-break matrix.
- [x] Extract protocol/checkpoint/catalog values without moving algorithms.
      `protocol.rs` owns `CursorState`, `OwnerMeta`, and `SessionMetadata`;
      persistence DTOs remain distinct from source mechanics.
- [x] Extract bounded JSONL reading, file identity, captured extents, and an
      opaque one-descriptor `SourceSnapshot` into `ingest/source.rs`. It must
      not expose its `File`; all owner revalidation, bounded reads, and
      committed-prefix hashing continue through the captured descriptor.
- [x] Extract chunk fingerprints, rolling audits, append verification, and
      shrink-confirmation policy into `ingest/checkpoints.rs`. It may depend
      on Source and `CursorState`, but not Storage, SQLite, or Projection.
- [x] Move only checkpoint reads and the autocommit pending-shrink marker into
      `ingest/checkpoint_store.rs`. Path-conflict clearing, unchanged marking,
      final checkpoint upsert, and accepted-shrink deletion remain inside the
      projection transaction until Stage 7.
- [x] Extract deterministic candidate selection, pure topology resolution,
      empty-placeholder protection, handoff planning, and finally root
      discovery into `ingest/catalog.rs`; decoded owners, persisted extents,
      topology SQL, and opened-snapshot handoff evidence are supplied inputs.
- [x] Keep `process_file` as the temporary orchestration bridge until Protocol
      and Projection exist; preserve open/snapshot/transaction/read/checkpoint
      ordering and every race-test hook. Discovery remains advisory; the
      opened snapshot is authoritative.

Gate: partial tails, append/rewrite, inode replacement, fingerprint budgets,
archive handoff, root-change protection, truthfulness contracts, and full
projection equivalence.

### Stage 7 — Ingestion protocol and projection

Status: **complete**

- [x] Move `CursorState` first, preserving exact serde defaults, field names,
      scalar types, and projector generation. Stage 6 established this wire
      boundary before source/checkpoint extraction.
- [x] Move pure timestamp/identifier/token/content/duration helpers first.
- [x] Introduce pure streaming family records carrying timestamp plus typed
      intents. Protocol performs no filesystem, SQLite, or database-dependent
      lifecycle/tool decisions; cursor state is advanced only after successful
      decode/projection.
- [x] Extract handlers one semantic family at a time—Usage first, then
      Conversation, Tools, Metadata, and Agent/Lifecycle—running each focused
      family gate plus the canonical projection oracle after every cut.
- [x] Centralize lifecycle rematerialization after Agent owns the complete
      lifecycle policy. Rollout removal remains the next named Projection cut.
- [x] Split surviving metadata recomputation into an immediately-applied
      projection `RemovalImpact`, source-owned owner reads in existing
      path/rollout order, and a named metadata reset fed back into the same
      IMMEDIATE transaction. Projection never opens surviving JSONL paths.
- [x] Move final checkpoint save and observation rematerialization behind
      named Projection operations without changing their order; leave
      `process_file` as the Stage 8 composition bridge.
- [x] Introduce opaque, non-`Deref` `ProjectionConnection`, `ProjectionTx`, and
      `ProjectionContext` only after removal and checkpoint writes have named
      Projection operations. Use no raw connection/transaction escape hatch;
      file orchestration owns the lifetime, not SQLite access.

Gate: fixture corpus, malformed/unknown records, timestamp/redaction/identifier
bounds, cumulative usage, tools, lifecycle ordering, discovery-order
permutations, SQLite integrity/FK checks, and canonical projection identity.

### Stage 8 — Ingestion orchestration and reconciliation

Status: **complete**

Detailed ownership, compile-green checkpoint order, sensitivity proofs, and
gates are recorded in `.agents/stage8-orchestration.md`.

- [x] Extract the independent session-title importer, then the bounded durable
      attempt/projector-publication state machine; remove generic metadata
      writes from orchestration.
- [x] Extract pure reconciliation planning plus atomic ProjectionTx
      application, retaining root-signature protection and incomplete-root
      semantics.
- [x] Rebuild FileIngestor around SourceSnapshot, checkpoint policy, Protocol,
      ProjectionTx, and persistence while preserving one-file transaction and
      committed-prefix checkpoint order.
- [x] Extract coordinator-owned scan, one-shot, recovery, and lifetime-lease
      use cases, retaining the two-pass publication protocol under one process
      lock.
- [x] Extract the background Scanner worker without changing lease lifetime,
      cancellation slices, bounded shutdown, or failed-cycle truthfulness.
- [x] Delete the legacy ingestion bridge.

Gate: locks, projector generation, rollback, incremental-versus-clean equality,
runtime shutdown, and live-scanner browser E2E against disposable data.

### Stage 9 — Final composition and cleanup

Status: **complete**

- [x] Merge independent feature routers in the application composition root.
- [x] Delete `src/api.rs`, `src/ingest.rs`, `ApiState`, and temporary bridges.
- [x] Split oversized test files only after production behavior is stable.
- [x] Confirm no schema, migration, or projector-generation change occurred.
- [x] Run every final verification gate.
- [x] Inspect the production binary and UI through the disposable browser
      harness. Do not start the stopped live application merely for inspection:
      `serve --no-ingest` avoids scanning but can still write startup metadata
      and pricing state, which would violate the refactor's live-data policy.

Final evidence is recorded in `.agents/stage9-final.md`.

## Decision log

| Date | Decision |
|---|---|
| 2026-07-25 | Use `v0.1.0` as the stable rollback point. |
| 2026-07-25 | Reject permanent thin facades; delete both giant modules. |
| 2026-07-25 | Characterize first, extract second, clean up third. |
| 2026-07-25 | Do not change schema/projector semantics or rebuild live data. |
| 2026-07-25 | Final runtime inspection uses the real production binary and UI with disposable projection data. The live app stays stopped because the CLI has no strictly read-only serve mode. |
| 2026-07-25 | The projection oracle exposed a pre-existing rewrite asymmetry: changing a root rollout CWD updates the rollout but not the existing thread; record it for a separate semantic decision rather than changing behavior during this refactor. |
| 2026-07-25 | Shared Usage owns only stable global/thread/turn/agent/effort scopes. Feature temporary tables and selection semantics stay inside Sessions or Activity rather than becoming hidden coupling in a prettier file. |
| 2026-07-25 | Web uses state-bound feature routers and a cohesive ReadRuntime, not a renamed global ApiState. Application dependencies exist only during composition and are consumed before handlers are built. |
| 2026-07-25 | Web extraction preserves the exact outer layer order: browser boundary, tracing, then the API-only error contract. Static SPA failures must not be normalized as API JSON. |
| 2026-07-25 | ReadRuntime owns one database plus the single shared StorageExecutor, exposes one cancellable snapshot operation returning anyhow results, and contains no Axum transport behavior. |
| 2026-07-25 | Storage is established in compile-green steps: move Db intact behind storage::Db, then separate connection/file mechanics from schema migration and seeding without reordering startup. |
| 2026-07-25 | Stage 6 gives bounded bytes/identity to Source, fingerprint and audit policy to Checkpoints, and source-choice/topology policy to Catalog; process_file remains orchestration until the later typed seams exist. |
| 2026-07-25 | Stage 7 uses typed protocol intent into a non-Deref ProjectionTx. Streaming and the transaction/checkpoint scope remain orchestration-owned so malformed complete records, semantic rollback, and partial tails keep existing behavior. |
| 2026-07-25 | Stage 8 has five orchestration concepts: Coordinator, FileIngestor, Reconciliation, AttemptRecorder, and SessionTitleImporter. The root ingest module becomes declarations/stable re-exports only after every algorithm has moved. |
| 2026-07-25 | Stage 2 makes Db a pure storage handle and composes ManualPricingStore explicitly after open. PricingMutations owns async serialization and blocking write orchestration; System remains independent from Pricing and Analytics. |
| 2026-07-25 | Stage 3 keeps list TEMP tables on one supplied connection and gives Analytics its own top-session read model. SessionRow and display helpers do not become cross-feature cupboards. |
| 2026-07-25 | Stage 4 shares only civil-time primitives plus Usage/Costing. Overview and Stats retain their deliberately different query algorithms, DTOs, and performance gates. |
| 2026-07-25 | Stage 5 keeps cursor codecs, bounded previews, TEMP-table groups, and attribution inside Activity. Tool payloads remain absent; Activity owns its lineage/existence reads and never imports Sessions. |
| 2026-07-25 | Storage migration/seeding SQL remains verbatim and private; only `storage::seed_fallback_prices` is crate-visible for the existing Pricing refresh path. Database mechanics retain eight tests and migration/schema/seeding retains ten tests. |
| 2026-07-25 | Shared Usage uses owner-carrying Global, Thread, Turn, Agent, and Effort scopes. Connection-taking dispatch and exceptional SQL live in `usage/rollups.rs`; the module root remains a SQL-free ownership boundary rather than hiding persistence types behind aliases. |
| 2026-07-25 | Web error normalization and browser trust enforcement are independent transport capabilities. Their extraction preserves API-only JSON normalization, full Host/Origin authority matching, outermost boundary placement, existing CSP values, and early-rejection behavior. |
| 2026-07-25 | Web owns application-shell routing and process-independent server lifetime. The temporary API monolith builds only relative feature routes; Web owns `/api/v1`, SPA assets, tracing/boundary layers, readiness, signals, and bounded drain. The frontend path is no longer API state. |
| 2026-07-25 | ReadRuntime binds one database to the one injected StorageExecutor and owns cancellable deferred read snapshots. Cancellation is armed before executor wait, remains sticky between SQLite statements, and is transport-agnostic; handlers map errors only at the HTTP boundary. |
| 2026-07-25 | Application composition consumes a non-Clone `AppDependencies`, creates one shared ReadRuntime, and merges five state-satisfied feature routers. No handler receives construction state, and API fallback/error normalization is applied once after the merge. |
| 2026-07-25 | Stage 2 will move Pricing ownership before changing construction: file moves first, explicit hydration second, mutation orchestration third, then catalog/routes and independent System extraction. Each cut retains its own full Rust gate. |
| 2026-07-25 | Startup prepares one canonical, non-Clone `DatabaseLocation`, validates exactly one Pricing-owned sidecar, opens pure SQLite storage, and then hydrates Pricing explicitly. `DatabaseLocation::open` repeats hard-link and future-schema preflight so preparation cannot become permission to mutate stale identity. |
| 2026-07-25 | `PricingMutations` owns the injected database, manual sidecar, shared executor, and one async serialization gate. It acquires the gate before `WorkClass::Light` capacity and moves the owned guard into blocking work so request cancellation cannot let another writer overtake a still-running mutation. HTTP parsing/error mapping and sidecar/SQLite durability remain with their actual owners. |
| 2026-07-25 | Pricing read persistence returns typed internal records from `catalog.rs`; private transport conversion, validation, work-class selection, mutations, and refresh HTTP live in SQL-free `routes.rs`. Application composition mounts `pricing::router` directly, and production `api.rs` contains no Pricing surface. |
| 2026-07-25 | Sessions and Activity share only neutral `conversation/display.rs` interpretation; neither imports the other. Analytics owns a separate top-session read model, and Activity owns its own visibility/root-rollout reads. A shared `SessionRow` or repository would recreate the coupling. |
| 2026-07-25 | Root `calendar.rs` owns chrono-only civil primitives. Overview and Stats remain separate persistence-owning slices, may not import each other, and share no generic `analytics/aggregates.rs`; exact query/index shapes and fixed-point reduction remain slice-owned. |
| 2026-07-25 | Stage 6 preserves the one-open-descriptor authority boundary: discovery probes are advisory, while owner/handoff revalidation, bounded JSONL, and committed-prefix hashing use opaque `SourceSnapshot`. Checkpoint reads and pending-shrink markers may move early; checkpoint writes coupled to normalized projection may not. |
| 2026-07-25 | Stage 7 uses typed protocol intents and opaque non-`Deref` projection transactions. Usage moves before the native fork gate can be disturbed, and checkpoint upsert remains atomic with row projection and precedes equal-timestamp agent rematerialization. |
| 2026-07-25 | Stage 7 moves named removal/checkpoint operations before constructing the opaque Projection transaction. Reversing that order would force a raw SQLite escape hatch and create a fictional boundary. |
| 2026-07-25 | Rollout removal remains an immediate three-part operation inside one IMMEDIATE transaction: Projection returns ordered surviving-source evidence, Source reads owner metadata, and Projection resets thread metadata. Projection never opens JSONL and Source never imports SQLite. |
| 2026-07-25 | Stage 3 establishes neutral calendar primitives and Web-owned pagination before Sessions routes move. Sessions owns its four same-connection TEMP tables and exact cost-sort key; neither prerequisite may absorb feature range or query policy. |
| 2026-07-25 | Neutral Calendar owns exact UTC/civil-time canonicalization with no crate dependencies; Web alone owns JS-safe pagination validation. Conversation owns display interpretation with no crate dependencies, and its legacy implementations were deleted rather than forwarded. |
| 2026-07-25 | Analytics top-session persistence owns an internal rank input and exact 16-field record; HTTP conversion stays transport-owned. Activity owns visibility and root-rollout reads and no longer invokes Sessions lifetime pricing merely to establish scope. Sessions and Activity deliberately retain separate rollout selectors. |
| 2026-07-25 | System owns independent Status and Settings persistence behind SQL-free routes. Settings obtains database metadata and its read snapshot from one ReadRuntime capability, preserving one-database identity; the transport field named `pricing` remains only an external compatibility name for System-owned cost coverage. |
| 2026-07-25 | Analytics transport and startup prewarm moved only after both slices owned domain and persistence. Routes remain SQL-free and Heavy; prewarm retains one deferred snapshot with Overview before Stats. Production `api.rs` now contains no Analytics behavior. |

## Stage ledger

| Stage | Started | Completed | Verification summary |
|---|---|---|---|
| 0 | 2026-07-25 | 2026-07-25 | 331 Rust tests discovered; 328 normal passed and 3 release gates passed explicitly. Frontend 230/230, lint/build, Chromium 13/13, and five-sample cold performance passed. |
| 1 | 2026-07-25 | 2026-07-25 | Storage, exact Costing, scoped Usage, Activity-owned exceptional attribution, Web errors/boundary/server/ReadRuntime, and state-bound application composition complete. Rust: fmt, all-target check, strict Clippy, 258 library tests plus every integration suite passed; 3 intentional release-scale ignores remained explicit. Frontend 230/230, lint/build, Chromium 13/13. Five-sample cold gate: 845ms worst render and 591ms worst API; every sample remained below one second. |
| 2 | 2026-07-25 | 2026-07-25 | Pricing and System are independent mounted vertical slices. Pricing owns sidecar/sync/mutations/catalog/routes; System owns SQL-free routes plus separate Status and Settings read models, imports neither Pricing nor Analytics/Ingest, and binds Settings metadata to the same ReadRuntime database as its snapshot. Production `api.rs` retains neither surface. Final gates: fmt, all-target/all-feature check, strict Clippy, 266 unit tests plus every integration suite passed with 3 intentional release-scale ignores; frontend 230/230, lint/build, and final Chromium 13/13 passed. No live database or reingestion. |
| 3 | 2026-07-25 | 2026-07-25 | Sessions is now a directly mounted vertical slice owning catalog, same-connection list selection, one-snapshot summary persistence, and SQL-free transport. Production exposes only its router; legacy API ownership is gone, Activity remains independent, and architecture guards prevent persistence leakage. Final Rust gates passed: fmt, all-target check, strict Clippy, 273 of 276 unit tests with 3 intentional release-scale ignores, 41 API contracts, 19 architecture contracts, and 18 runtime contracts including adversarial WAL snapshot tests. Frontend 230/230, lint/build, Chromium 13/13, and five cold cost-sorted Sessions samples at 220-224 ms render and 140-144 ms API all passed. No live database or reingestion. |
| 4 | 2026-07-25 | 2026-07-25 | Overview and Stats independently own domain, persistence, SQL-free routes, and the ordered deferred startup prewarm; production `api.rs` retains no Analytics surface. Final gates: fmt, all-target check, strict Clippy, 293 of 296 unit tests with 3 intentional release ignores, every integration suite, frontend 230/230, lint/build, Chromium 13/13, and five cold samples under one second (850ms worst render, 606ms worst annual API). No live database or reingestion. |
| 5 | 2026-07-25 | 2026-07-25 | Activity owns model, cursors, same-connection selection, attribution, previews, groups, root/day pages, detail, SQL-free routes, and isolated regressions. The 12,366-line `src/api.rs` was deleted. Final gates: 301/304 normal Rust library tests plus all three explicit release-scale gates, every integration suite, frontend 230/230, lint/build, Chromium 13/13, and five cold Activity samples at 113–118 ms render / 44–80 ms API. No live database or reingestion. |
| 6 | 2026-07-25 | 2026-07-25 | Protocol values, descriptor-safe Source, pure checkpoint/fingerprint policy, narrow checkpoint persistence, and filesystem-only Catalog discovery/selection are complete. The bridge retains SQL, opened-snapshot readiness, projection, reports, reconciliation, and transaction ordering. Final gate: 470 normal Rust tests passed, 3 intentional release-scale ignores; fmt, all-target check, strict Clippy, ingestion units 121/121, architecture 38/38, and external ingestion 43/43 passed. No live database or reingestion. |
| 7 | 2026-07-25 | 2026-07-25 | Protocol owns one pure closed record router; Projection owns one opaque transaction and exhaustive atomic dispatcher. The bridge retains only Stage 8 orchestration. Sensitivity caught fork-gate usage drift and live-cursor publication on SQL failure. Final gate: 463 normal library tests passed, 3 intentional ignores, every integration/runtime/architecture suite passed, and fmt/check/strict Clippy were clean. No live database or reingestion. |
| 8 | 2026-07-25 | 2026-07-25 | Ingestion is fully decomposed into Source, Checkpoints, CheckpointStore, Catalog, Protocol, Projection, Attempt, OwnerReader, FileIngestor, Reconciliation, SessionTitles, Coordinator, and Scanner. The legacy `src/ingest.rs` bridge is deleted; declaration-only `src/ingest/mod.rs` preserves three direct public seams and no forwarding logic. Full Rust/static gates pass with 485 normal library tests, 3 intentional ignores, 57 architecture contracts, and all ingestion/runtime integrations. No live database or reingestion. |
| 9 | 2026-07-25 | 2026-07-25 | Final composition and cleanup complete. Both monoliths and all compatibility scaffolding are absent; architecture contracts describe only the final graph; oversized Activity and ingestion test carriers are split. Static Rust gates pass, 485 normal library tests plus all 177 integration tests pass, all three release-scale Activity gates pass, frontend 230/230 plus lint/build/typecheck pass, Chromium functional E2E passes 13/13, and five-sample cold performance stays under one second on every surface (854ms worst render, 580ms worst API). Corpus projection integrity/FK checks pass 18/18. Schema, migration, runtime indexes, dependency manifests, and projector generation are unchanged from `v0.1.0`; no live database or reingestion. |
