# Stage 5 Activity cut

Status: **complete**

Activity needs real ownership, not a façade around the current batch. The key
missing concept is a prepared, same-connection selection that separates root
selection from previews, groups, and attribution.

## Target tree and dependency direction

```text
model -> index / selection -> attribution -> previews -> groups
      -> root_page -> detail -> routes
```

- `model.rs`: transport-neutral records only.
- `index.rs`: absorb root `activity_index.rs` and its cursor/keyset tests;
  delete the old root file in the same cut.
- `selection.rs`: own `PreparedSelection<'conn>` and the five general
  connection-local selection/temp-table structures. Its lifetime pins all
  consumers to the same SQLite connection.
- `attribution.rs`: exact keyed/scalar usage and local-day totals; never mutate
  response DTOs.
- `previews.rs`: bounded modern/legacy child paging, hydration, and previews.
- `groups.rs`: group metadata/detail/paging and sole ownership of
  `selected_activity_group_turns`.
- `root_page.rs`: keep visibility/rollout ownership and add final root/day
  assembly only after selection, attribution, previews, and groups exist.
- `detail.rs`: detail dispatch and precedence, without HTTP.
- `routes.rs`: parsing, exact errors, recursive DTO conversion, and one Heavy
  snapshot per handler; no SQL.

Sessions and Activity never import one another. Shared presentation comes only
from Conversation; Costing and Usage remain lower-level dependencies.

## Pre-cut characterization

- [x] Exact collection cursor JSON/backward compatibility/wrong-collection error.
- [x] Detail-dispatch collisions and reserved-prefix precedence.
- [x] Pagination-independent day summaries.
- [x] Multi-page review-group cursors and page-invariant totals.
- [x] Exact raw Activity response/null shape.
- [x] Same-connection selection/temp-table ownership, including reused empty
      selections and connection locality.
- Cold Activity page and first-expansion browser baseline.

Each new oracle gets one deliberate sensitivity mutation and restoration.

## Safe order

1. Characterization plus model/selection architecture roles.
2. Transport-neutral model records with temporary API conversion.
3. Move index and delete root `activity_index.rs`.
4. Extract prepared selection.
5. Return keyed/scalar attribution values instead of mutating DTOs.
6. Extract previews/legacy child paging.
7. Extract groups.
8. Complete root/day assembly.
9. Extract detail dispatch.
10. Move routes, mount `activity::router`, and remove all Activity ownership
    from `api.rs`.

## Accepted checkpoints

- `activity/model.rs` owns the four recursive read records; the external raw
  JSON oracle pins nullability, paging-field omission, and exact key names.
- `activity/index.rs` owns the event-index cursor and queries; the obsolete
  root `activity_index.rs` has been deleted.
- `activity/cursor.rs` owns the distinct collection cursor protocol and its
  backward-compatible camelCase wire format.
- `activity/selection.rs` owns `ActivityRootScope`, `PreparedSelection`, and
  all five general TEMP tables. Root-list and root-detail reads explicitly
  prepare the selection and pass the lifetime-pinned connection onward.
- Empty preparation always clears prior TEMP rows. A deliberate early-return
  mutation failed the same-connection regression test and was restored.
- Reserved-ID and review-group cursor mutations both failed their new
  black-box tests and were restored.
- `activity/attribution.rs` now returns keyed root/group `UsageTotals` through
  `SelectedActivityUsage`; it owns exceptional fixed-point pricing and sparse
  NULL-turn interval attribution without importing or mutating response DTOs.
  A deliberate review/agent key inversion failed the lazy review-group total
  oracle and was restored.
- `activity/previews.rs` now owns bounded modern and legacy hydration, the
  legacy pseudo-root, source-line-stable collection paging, numeric-page
  compatibility, selected-only message decoding, and event preview paging.
  Event usage stays DTO-free in Attribution and is attached by keyed ordinal.
- A deliberate equal-timestamp source-line ordering inversion failed the
  legacy cursor/order oracle and was restored. The focused architecture,
  query-budget, bounded-body, legacy-root, and event-attribution gates pass
  under strict Clippy.
- `activity/groups.rs` now owns descendant summaries, lazy placeholders,
  group-detail selection, cursor paging, bounded hydration, page-independent
  usage/status/labels/duration, and every reference to
  `selected_activity_group_turns`. Root-neighbor lookup moved behind the
  characterized `ActivityRootScope` boundary in `selection.rs`.
- The first root-scope extraction added one redundant lookup and tripped the
  fixed statement-budget oracle (22 versus 21). `from_known_on` restored the
  previous budget before acceptance. Group cursor, reserved-ID precedence,
  reused-agent attribution, thread scoping, lazy paging, strict Clippy, and
  the 100k-descendant release gate all pass; the latter completed in
  0.80–0.90 seconds per operation against a 3-second budget.
- `activity/root_page.rs` now owns visibility, rollout selection, bounded root
  hydration, batch counts/usage, lazy group placeholders, legacy fallback,
  page assembly, occupied-day discovery, cross-midnight interval union, and
  day totals. Root detail consumes the explicit `RootExchange` read result;
  `ActivityBatch` is private and does not leak back through a façade.
- Root-page ownership, constant statement budgets, cross-midnight occupancy,
  selected-page isolation, exact raw JSON shape, and NULL-turn attribution all
  pass under strict Clippy. No schema, projector marker, or live data changed.
- `activity/detail.rs` now owns collection/event cursor validation, reserved-ID
  dispatch precedence, exact turn/event hydration, root exchange composition,
  legacy detail paging, group detail delegation, and scalar event/turn usage.
  Transport supplies only `DetailPage`; detail imports no Axum, Web, Sessions,
  or legacy API capability.
- The focused architecture suite, 21 normal Activity library regressions, 13
  black-box Activity API contracts, strict Clippy, and the explicit 100k
  descendant release gate pass. The release gate remained bounded at
  0.79–0.90 seconds per list/detail/group operation against its 3-second
  budget. No schema, projector marker, live database, or ingestion changed.
- `activity/routes.rs` now owns the two SQL-free HTTP handlers and is mounted
  directly as `activity::router`. The 22 persistence regressions live in
  `activity/regression.rs`; the module root exports only the router.
- The 12,366-line legacy `src/api.rs` has been deleted. Architecture contracts
  require its absence and forbid Activity from importing Sessions or another
  feature slice.
- Final acceptance: 301/304 Rust library tests passed with the three intended
  scale gates exercised explicitly; all 45 API, 32 architecture, 17 corpus,
  20 ingestion-edge, 6 truthfulness, 1 bind, and 18 runtime contracts passed.
  Frontend 230/230, lint, production build, Chromium 13/13, and five cold
  browser samples passed. Activity root render was 113–114 ms (API 75–80 ms)
  and first expansion 113–118 ms (API 44–50 ms), all below one second.

## Verification cadence

Focused module/API/architecture/format gates after every cut; full Rust,
Clippy, and all-target checks after each cohesive cluster. Run the explicit
100k-descendant, 500k-tool, and 500k-usage release gates after
selection/attribution and again at the end. Final frontend, Chromium, lazy
paging/cursor/lineage/size, and cold Activity performance gates are mandatory.
