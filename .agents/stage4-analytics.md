# Stage 4 Analytics cut

Status: **complete; Stage 4 is fully green**

Overview and Stats are separate product/read-model slices. They may share only
the root Calendar primitives and lower Costing/Usage capabilities. There is no
generic aggregate repository, bucket module, or `analytics/aggregates.rs`.

## Pre-cut safety

- Split the mixed grouped-pricing oracle into explicit Overview and Stats
  characterization tests.
- Strengthen the Stats exact-total test beyond `i64`/JavaScript-sensitive
  numerator ranges.
- Preserve the existing route, timezone, repricing, query-plan, statement
  budget, browser, and cold-performance oracles.

## End state

```text
src/analytics/
├── mod.rs
├── routes.rs
├── prewarm.rs
├── overview/
│   ├── mod.rs
│   └── read.rs
└── stats/
    ├── mod.rs
    └── read.rs
```

- `overview/mod.rs` owns Overview result types and pure period/day/delta policy.
- `overview/read.rs` owns Overview SQL, assembly, annual activity/projects, and
  the independent top-session read model. Fold and delete the intermediate
  `analytics/top_sessions.rs` here.
- `stats/mod.rs` owns typed range/anchor/bucket/label/exact-total policy.
- `stats/read.rs` owns occupied-year discovery, grouped pricing, session-count
  strategies, and Stats assembly.
- `routes.rs` owns request validation, Axum handlers, Heavy snapshot selection,
  and error mapping only.
- `prewarm.rs` preserves the deferred transaction and Overview-before-Stats
  warm-query order.

## Extraction order

1. Move pure Overview types/policy; focused pure/architecture gates.
2. Move Overview persistence, fold top sessions, then immediately run the
   focused API/query-plan and five-sample cold performance gates.
3. Move pure Stats types/policy; focused timezone/architecture gates.
4. Move Stats persistence, then immediately run repricing/query-budget and
   five-sample performance gates.
5. Move routes and prewarm; mount `analytics::router` from `app.rs`, call
   Analytics prewarm from `main.rs`, and remove all Analytics ownership from
   `api.rs` in the same cut.
6. Run the full Rust, frontend, Chromium functional, and isolated cold
   performance gates. Do not touch the live database or reingest.

## Architectural tripwires

- Overview and Stats may not import each other.
- Domain roots contain no Axum/SQL; route code contains no SQL/Connection.
- Each read module owns its own SQL markers, bounds, grouping, and performance
  tests.
- No generic Analytics cupboard or persisted heatmap/cache is introduced.
- Final `api.rs` contains no Analytics DTO, handler, SQL, prewarm, or router.

## Accepted checkpoints

### Overview domain and persistence

- `overview/mod.rs` owns pure response, period, day, delta, and ranking policy.
- `overview/read.rs` owns summary and annual SQL, activity/project assembly,
  exceptional repricing, and the independent 16-field top-session record.
- The temporary `analytics/top_sessions.rs` has been folded and deleted.
- `api.rs` retains only Overview transport and shared prewarm responsibilities.
- Private read tests exercise production summary/annual readers, exact grouped
  repricing, sparse and event-only activity, covering indexes, and the
  one-statement annual usage budget.

Verification: formatting, all-target/all-feature check, strict Clippy, 282 of
285 unit tests with the same three intentional release gates ignored, and all
integration suites passed. Five disposable cold samples remained below the
one-second product target: 254ms worst summary render, 863ms worst annual
render, and 643ms worst annual API. No live database or reingestion.

### Stats domain

- `stats/mod.rs` owns typed ranges, buckets, exact response DTOs, canonical
  anchor and label policy, DST-safe bucket construction, sparse all-time year
  policy, skipped-civil-date handling, and exact saturating `i128` totals.
- `api.rs` retains only transport validation and connection-bound Stats work
  for the next persistence checkpoint.
- Overview and Stats have bidirectional architecture guards preventing sibling
  imports; the Stats domain cannot import Axum, SQLite, Web, or Storage.

Six temporary mutations proved the focused guards: duplicate-hour threshold,
zero-duration bucket admission, mixed-year insertion, fixed-point narrowing,
Monday anchoring, and a forbidden SQLite dependency. Every mutation failed its
intended test and was reverted. The clean full Rust gate passed 288 of 291 unit
tests with the same three intentional ignores, plus 41 API, 21 architecture,
17 corpus, 20 ingest-edge, 6 ingest-truthfulness, 18 runtime, and the bind
contract tests. No live database or reingestion.

### Stats persistence

- `stats/read.rs` is the sole owner of Stats response assembly, sparse occupied
  local-year discovery, canonical SQL bounds, grouped and exceptional pricing,
  and the broad/ordinary session-count strategies.
- `stats/mod.rs` exposes only the typed `read_on(Connection, StatsRange,
  NaiveDate)` seam; its parent domain remains SQL-free and independent of
  Overview.
- `api.rs` retains the Stats query DTO, request validation/handler, and the
  Overview-before-Stats startup prewarm order, but no Stats SQL or read helper.
- Persistence tests live beside the reader and cover statement budgets, both
  session strategies, all three activity kinds in occupied-year discovery,
  sparse/future years, grouped repricing, indexed plans, cross-kind session
  deduplication, camelCase SQL bounds, and exact `i128` assembly beyond `i64`.

Six temporary mutations proved those boundaries: broken SQL-bound JSON keys,
missing message-only year discovery, inverted broad/ordinary strategy,
bypassed exceptional repricing, `UNION ALL` session duplication, and `i64`
cost narrowing. Every mutation failed its intended oracle and was reverted.
The independent full Rust gate passed 293 of 296 unit tests with the same three
intentional release-scale ignores, plus 41 API, 22 architecture, 17 corpus, 20
ingest-edge, 6 ingest-truthfulness, 18 runtime, and the bind contract tests.

The first disposable cold run had two isolated machine-contention spikes while
all medians remained within budget. A clean idle rerun passed every one of five
samples: 848ms worst annual Overview render, 841ms worst Stats-year render,
873ms worst all-time Stats render, and 613ms worst annual Overview API. No live
database or reingestion.

### Routes and startup prewarm

- `analytics/routes.rs` owns the three relative Analytics routes, query DTOs,
  exact public date/year validation, Heavy snapshot selection, and HTTP error
  mapping. It contains no SQL or direct SQLite access.
- `analytics/prewarm.rs` owns the startup read warmup on one connection and one
  deferred transaction, preserving the established Overview-before-Stats
  order.
- `app.rs` mounts `analytics::router` directly and `main.rs` invokes the
  Analytics prewarm in the original startup slot. Production `api.rs` no longer
  owns any Overview or Stats transport, validation, persistence, or prewarm
  behavior.
- The black-box transport oracle now fixes every Analytics validation message
  and proves that malformed or out-of-range all-time anchors remain ignored.
  The architecture oracle enforces the route set, Heavy work class, SQL-free
  transport, one deferred prewarm transaction, ordered warmup, and exact
  composition/startup wiring.

Four temporary mutations proved the final seam: a Light route, reversed warmup
order, Immediate warmup transaction, and omitted Stats route each failed its
intended oracle and were reverted. Final verification passed formatting,
all-target/all-feature check, strict Clippy, 293 of 296 unit tests with the same
three intentional release gates ignored, 42 API, 23 architecture, 17 corpus,
20 ingest-edge, 6 ingest-truthfulness, 18 runtime, and the bind contract tests.
Frontend verification passed 230 unit tests, lint, production build, and 13/13
Chromium functional flows. Five disposable cold samples all stayed below one
second: 850ms worst Overview annual render, 836ms worst Stats all-time render,
832ms worst Stats-year render, and 606ms worst annual Overview API. No live
database or reingestion.
