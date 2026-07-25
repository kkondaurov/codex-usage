# Stage 3 Sessions summary cut

Status: **complete**

This is the next cut after Sessions list persistence. It moves one coherent
summary read model, then moves all Sessions transport as a single route slice.

## Safety work before production moves

1. [x] Assert the exact raw JSON shape: five top-level keys, 22 session keys,
   nine model keys, ten agent keys, and four tool keys.
2. [x] Characterize root-rollout priority when no same-ID rollout exists: a
   parentless root beats an earlier child, with `started_at, id` tie ordering.
3. [x] Characterize breakdown ordering and bounds: model effort separation and
   ordering, agent label precedence/order, tool count/name ordering, and the
   100-row cap.
4. [x] Prove sensitivity by temporarily inverting root-rollout priority and one
   breakdown tie/bound; restore production before proceeding.

The priority inversion selected the deliberately wrong child prompt; changing
the tool cap from 100 to 99 failed on the exact bound. Both mutations were
reverted. The post-characterization full Rust gate passed 273 of 276 unit tests
with the three intentional release-scale ignores, plus 41 API contracts and
every other integration suite.

The persistence cut then passed strict Clippy, 273 of 276 unit tests with the
three intentional release-scale ignores, 41 API contracts, 18 architecture
contracts, and every other integration suite. An independent semantic audit
found no field, visibility, ordering, pricing, error-precedence, or snapshot
drift.

## Persistence boundary

Create `src/sessions/summary.rs` with one supplied-connection entry point:

```rust
pub(crate) fn read_summary_on(
    connection: &Connection,
    thread_id: &str,
) -> Result<Option<SummaryRecord>>
```

Move the existing detail/root-rollout, model, agent-total, agent-summary, and
tool-summary queries without changing policy. All helpers use the same
`&Connection`. The module may depend on Sessions catalog plus neutral
Conversation, Usage, and Costing; it may not know about Axum, Serde, Web,
Storage runtime, Activity, or Analytics.

## Route boundary

After list and summary persistence are independent, move Sessions DTOs,
validation, list/projects/summary handlers, conversions, and router together
to `src/sessions/routes.rs`. Mount `sessions::router` directly from `app.rs`.
No forwarding Sessions facade remains in `api.rs`.

## Gates

- Focused API summary, runtime snapshot, fixed-point, and architecture tests.
- Format, all-target/all-feature check, strict Clippy, full Rust suite.
- After routes move: frontend unit/lint/build, Chromium functional suite, and
  five-sample cold Sessions performance gate under the existing one-second
  hard limit.

## Completion evidence

Sessions is mounted directly from `app.rs`; production `api.rs` retains only
Activity-owned `/sessions/{id}/activity` paths. The module root exports only
the router in production, while route code imports sibling persistence
directly and is guarded against SQL, `rusqlite`, database handles, and storage
runtime ownership.

The final checkpoint passed formatting, all-target compilation, strict Clippy,
273 of 276 unit tests with three intentional release-scale ignores, 41 API
contracts, 19 architecture contracts, and 18 runtime contracts. The runtime
suite includes deliberately torn-response oracles plus concurrent WAL tests
for both summary totals and the combined list/project-catalog response.
Frontend 230/230, lint/build, Chromium functional 13/13, and five cold
cost-sorted Sessions samples at 220-224 ms render and 140-144 ms API passed.
No live database was opened or rebuilt.
