# Stage 9 Final Verification

## Outcome

The architecture refactor is complete.

- `src/api.rs` and `src/ingest.rs` are deleted.
- `AppDependencies` is construction-only and consumed by `app.rs` while
  merging independent state-bound feature routers.
- System, Pricing, Sessions, Activity, Overview, and Stats own their transport
  and persistence boundaries without importing sibling feature handlers or
  DTOs.
- Ingestion is decomposed into Source, Checkpoints, CheckpointStore, Catalog,
  Protocol, Projection, Attempt, OwnerReader, FileIngestor, Reconciliation,
  SessionTitles, Coordinator, and Scanner.
- `src/ingest/mod.rs` is declaration-only apart from three stable direct
  exports and test-only imports.
- Architecture contracts contain no deleted-module fallbacks, ghost role
  entries, staged allowlists, or vacuous assertions against absent code.
- Activity and ingestion regression carriers are split by behavior after
  production behavior stabilized.

## Stable-data invariants

Compared with tag `v0.1.0`:

- `migrations/0001_initial.sql` is byte-identical.
- The migration registry remains exactly version 1.
- Required runtime-index SQL is byte-identical.
- `PROJECTOR_GENERATION` remains 1.
- Cargo and frontend dependency manifests and the Rust toolchain pin are
  unchanged.
- Package contents include every new source, test, frontend, migration, and
  fixture file and exclude the live projection and dependency directories.

No live database was opened, rebuilt, reingested, or benchmarked. The stopped
live application was not started for inspection because `serve --no-ingest`
still permits startup metadata and pricing writes. Production runtime behavior
was instead exercised through the real binary and built frontend with isolated
temporary databases and ephemeral ports.

## Final gates

### Rust and architecture

- `cargo fmt --all -- --check`: passed.
- `cargo check --all-targets --all-features`: passed.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- `cargo test --all-features`: passed.
  - Library: 485 passed, 3 intentional release-scale ignores.
  - Integration: 177 passed across API, architecture, corpus, ingestion,
    loopback, and runtime contracts.
- Architecture contract: 57/57 passed.
- Corpus acceptance, serial: 18/18 passed, including SQLite
  `integrity_check` and foreign-key checks for every manifest projection.
- `git diff --check`: passed.
- `cargo package --list --allow-dirty`: contains the complete refactored tree.

### Release-scale Activity

- 100,000 descendants: list 968ms, root detail 796ms, group page 901ms;
  3-second budget passed.
- 500,000 events: median list 375ms, median first detail 415ms, median deep
  numeric page 448ms, median deep cursor 406ms; 2.5-second slowest-sample
  budget passed (1.40s slowest sample).
- 500,000 usage-fact heavy queries: one-second gate passed.

### Frontend and production browser

- Frontend unit tests: 230/230 passed.
- ESLint: passed.
- Production TypeScript/Vite build: passed.
- E2E TypeScript typecheck: passed.
- Production Chromium functional suite: 13/13 passed.
- Five-cold-context production browser performance: passed on every surface.
  - Worst render: 854ms, annual Overview.
  - Worst Stats render: 829ms, year.
  - Sessions sorted by cost: 221ms worst render.
  - Activity root: 124ms worst render.
  - Activity first expansion: 119ms worst render.
  - Worst API: 580ms, annual Overview.

## Final boundary audit

An independent read-only audit found no production architecture blocker. The
largest remaining production files are cohesive feature modules rather than
cross-domain owners. No additional production split is justified by line count
alone.
