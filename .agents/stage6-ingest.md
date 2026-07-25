# Stage 6 — Ingestion source mechanics

Status: **complete**

## Purpose

Extract source discovery, bounded JSONL reading, source snapshots,
fingerprint/checkpoint policy, and catalog selection from `src/ingest.rs`
without changing ingestion behavior.

This stage deliberately leaves projection writes and `process_file` in the
legacy bridge. The bridge may depend on extracted modules; extracted modules
must never depend on the bridge.

## Exit criteria

- Serialized checkpoint state remains byte-for-byte compatible.
- Source files are read through one authoritative open descriptor.
- Append, rewrite, shrink, inode replacement, partial-tail, and oversized-line
  behavior is unchanged.
- Fingerprint audit behavior and byte budgets are unchanged.
- Candidate selection, archive handoff, topology, and reconciliation
  protection remain deterministic and unchanged.
- All external ingestion contracts and the canonical projection oracle pass.
- No schema, projector-generation, live database rebuild, or reingestion.
- `process_file` remains the sole temporary orchestration/projection bridge.
- No extracted module reaches back into `src/ingest.rs`.

## Current bridge

During Stage 6, `src/ingest.rs` continues to own scan-attempt orchestration,
transaction boundaries, `process_file`, projection writes/removals,
generation completion, reconciliation, title synchronization, and public
ingestion entry points/reports. No redesign of those responsibilities belongs
in this stage.

## Target tree

```text
src/ingest.rs                         temporary bridge
src/ingest/protocol/mod.rs            pure protocol values
src/ingest/protocol/state.rs          CursorState, then TokenUsage
src/ingest/protocol/metadata.rs       OwnerMeta and SessionMetadata
src/ingest/source.rs                  bounded reads and SourceSnapshot
src/ingest/checkpoints.rs             fingerprint and append/rewrite policy
src/ingest/checkpoint_store.rs        temporary SQLite checkpoint adapter
src/ingest/catalog.rs                 discovery, topology, selection, handoff
```

Dependency direction is protocol → source → checkpoints, with the checkpoint
store as a persistence adapter and Catalog as pure discovery/selection policy.
The legacy bridge may consume all of them; none may import the bridge.

## Architecture tripwires

- Protocol imports no filesystem, SQLite, Storage, API, or orchestration.
- Source imports no SQLite, Storage, Projection, or API.
- Checkpoints imports no SQLite, Storage, Projection, or API.
- Checkpoint store contains no filesystem traversal or projection algorithm.
- Catalog opens no database connection and performs no projection writes.
- `SourceSnapshot` does not expose `File`, `Deref`, `AsRef<File>`, or
  `into_inner`.
- `process_file` remains in `src/ingest.rs` until Stage 8.
- Final checkpoint writes remain inside the existing projection transaction.
- The pending-shrink marker remains the only checkpoint mutation outside it.

## Non-negotiable behavior

### Source snapshot

- Open once; capture identity, size, timestamps, and readable extent from that
  descriptor.
- Read owner metadata, fingerprints, and JSONL from the same descriptor.
- Rename-over after open never projects replacement contents under the old
  owner; appended bytes beyond the captured extent wait for the next scan.
- Opened owner and handoff evidence are revalidated against discovery hints.

### JSONL boundaries

- Complete malformed JSON advances the checkpoint and reports the current
  failure; semantic projection errors roll back the file transaction.
- Incomplete tails advance neither offset, line, nor fingerprint.
- Oversized complete lines are drained/reported without corrupting later rows.
- Existing 32 MiB record/metadata limits remain unchanged.

### Fingerprints

- Chunk size remains 1 MiB and the wire prefix remains
  `chunked-sha256-v1:`.
- The content digest includes size, chunk size, and hashes, but not audit cursor
  or audit completion time.
- Append proves the prior committed extent; same-size, earlier-chunk+append,
  stable-shrink, audit progression, and read budgets remain unchanged.
- A pending shrink is never treated as a committed projection checkpoint.

### Catalog and transactions

Candidate preference remains: complete, then larger, then active, then
lexically smaller path. Readiness is a prerequisite rather than a tie-break;
incomplete/empty correlated candidates continue protecting existing owners.

Transaction order stays: claim IMMEDIATE writer ownership; revalidate the
snapshot; parse/project; update committed checkpoint; rematerialize; commit;
reconcile only after confirmed catalog state; synchronize titles; finalize the
attempt.

## Checkpoint 0 — Characterization only

- [x] `cursor_state_wire_format_is_exact_and_backward_compatible`
  - exact field names/order;
  - missing `projector_generation` defaults to zero;
  - populated state round-trips;
  - malformed-state fallback remains unchanged.
- [x] Sensitivity proof: temporarily remove the generation default or rename
      `turn_context_seen`; the new test must fail, then restore it.
- [x] `chunked_fingerprint_wire_format_and_content_digest_are_stable`
  - exact prefix/field order;
  - 1 MiB chunks, 64-char hashes, valid chunk count/audit bounds;
  - digest unchanged by audit-only metadata.
- [x] Sensitivity proof: temporarily include audit metadata in the content
      digest or rename `chunk_bytes`; the test must fail, then restore it.
- [x] `catalog_preference_matrix_is_complete_and_permutation_invariant`
  - every comparison axis with higher priorities held equal;
  - reverse comparisons are opposites;
  - every permutation yields the same winner;
  - readiness and empty/handoff protection are applied before preference.
- [x] Sensitivity proof: reverse active/archive or lexical preference; the test
      must fail, then restore it.
- [x] Record ingestion unit/external-suite counts and run the external baseline.

## Extraction checkpoints

1. **CursorState** — move only `CursorState` and its golden test into
   `protocol/state.rs`; no alias in the bridge and no algorithm change.
2. **Protocol values** — move TokenUsage, OwnerMeta, and SessionMetadata; remove
   an empty root model only in the same accepted cut.
3. **Source primitives** — bounded line reader, then file identity, then opaque
   SourceSnapshot; accept each separately.
4. **Checkpoint policy** — move fingerprint values, encoding, append/rewrite,
   shrink, and audit decisions without opening paths or querying SQLite.
5. **Checkpoint persistence** — move reads and pending-shrink markers only;
   keep final checkpoint writes and projection-coupled deletion in the bridge.
6. **Catalog** — move preference, topology, empty protection, handoff, then
   discovery, with persisted evidence supplied explicitly.
7. **Bridge integration** — make `src/ingest.rs` consume all extracted source
   mechanics while retaining projection/orchestration ownership.

## Required oracles

Source races/bounds:

- `append_during_projection_waits_for_the_next_captured_extent`
- `file_projection_claims_writer_before_read_snapshot_can_go_stale`
- `rename_over_after_open_never_projects_the_replacement_under_the_old_owner`
- `bounded_line_reader_drains_complete_records_and_marks_incomplete_tails`
- `handoff_revalidates_the_opened_snapshot_before_replacing_projection`
- `oversized_incomplete_tail_waits_then_complete_record_is_drained_and_reported`

Fingerprint policy:

- `growing_chunk_checkpoint_reads_only_the_tail_and_suffix`
- `rewrite_in_earlier_chunk_plus_append_forces_projection_rebuild`
- `continuously_growing_file_advances_audit_until_old_rewrite_is_found`
- `every_growing_file_advances_its_audit_when_background_budget_is_exhausted`
- `periodic_chunk_audit_is_bounded_and_detects_rewrites`
- `stable_same_path_shrink_is_accepted_on_repeat`

External suites:

- `tests/corpus_acceptance.rs`: 17 tests.
- `tests/ingest_edge_contracts.rs`: 20 tests.
- `tests/ingest_truthfulness.rs`: 6 tests.
- Current ingestion unit behavior: 97 tests before relocation.

## Verification cadence

After each small cut, run the focused module namespace, architecture contract,
the three external ingestion suites, and the canonical projection oracle. At
each cohesive checkpoint, run formatting, all-target/all-feature check, strict
Clippy, and the full Rust suite.

Do not run `cargo run` against the live database during Stage 6. Browser and
release-scale application gates return when orchestration moves in Stage 8.

## Stop conditions

Restore the current cut before proceeding if a characterization expectation,
serialized state, fingerprint byte budget, catalog winner, transaction order,
or canonical projection changes; if a Source API leaks its descriptor; if an
extracted module imports the bridge; or if a migration, projector bump, live
rebuild, or reingestion appears necessary.

## Accepted checkpoints

- [x] Checkpoint 0 — characterization
- [x] Checkpoint 1 — CursorState
- [x] Checkpoint 2 — protocol values
- [x] Checkpoint 3 — source primitives
- [x] Checkpoint 4 — checkpoint policy
- [x] Checkpoint 5 — checkpoint persistence
- [x] Checkpoint 6 — catalog
- [x] Checkpoint 7 — bridge integration

For every accepted checkpoint, record files moved/created, tests added or
relocated, sensitivity proof, focused/external/full results, and deliberately
retained bridge debt.

## Accepted evidence

### Checkpoint 0 — characterization

- Added three direct contracts to the legacy ingestion unit module before any
  production move. The ingestion unit inventory is now 100 tests.
- Cursor sensitivity: removing `#[serde(default)]` from
  `projector_generation` failed the golden on the legacy payload with
  `missing field projector_generation`; the attribute was restored.
- Fingerprint sensitivity: adding `audit_completed_at` to the content digest
  failed the exact digest golden; the mutation was restored.
- Catalog sensitivity: preferring archived over active candidates failed the
  pair matrix at candidate 1 versus 2; the comparator was restored.
- Restored full library gate: 304 passed, 3 intentional release-scale tests
  ignored.
- External ingestion gates: corpus 17/17, edge contracts 20/20, truthfulness
  6/6. The corpus run includes the canonical projection drift oracle.
- No schema, projector generation, live database, or ingestion source was
  changed. The only retained bridge debt is the predeclared Stage 6 ownership
  in `src/ingest.rs`.

### Checkpoint 1 — CursorState

- Created `src/ingest/protocol/mod.rs` and
  `src/ingest/protocol/state.rs`; moved the type and its exact golden together.
- The bridge imports the real moved type through the protocol module; no
  compatibility type or encoding wrapper was introduced.
- Added explicit architecture roles for the protocol module root and state.
- Focused golden 1/1, architecture 32/32, external ingestion 43/43, full Rust
  suite 443 normal tests passed with 3 intentional release-scale tests ignored.
- Formatting, all-target/all-feature check, and strict Clippy passed.
- Persisted bytes, generation fallback, schema, projector generation, and live
  data remained unchanged.

### Checkpoint 2 — protocol values

- Moved `TokenUsage` and its componentwise-delta characterization into
  `protocol/state.rs`; deleted the obsolete root `src/model.rs` rather than
  retaining a forwarding facade.
- TokenUsage sensitivity: reversing the cached-input decrease comparison
  failed the new five-field characterization; the mutation was restored.
- Moved the definition-only `OwnerMeta` and `SessionMetadata` vocabulary into
  `protocol/metadata.rs`. Parsing, normalization, filesystem inspection, and
  persistence deliberately remain outside the protocol boundary.
- Strengthened protocol roles to reject HTTP, SQL, filesystem, IO, path, and
  projection ownership. Added single-owner and deleted-facade assertions.
- Focused protocol 2/2, metadata-related unit 8/8, architecture 34/34,
  external ingestion 43/43, and full Rust 446/446 normal tests passed; 3
  intentional release-scale tests remained ignored.
- Formatting, all-target/all-feature check, and strict Clippy passed. No live
  database or source corpus was opened.

### Checkpoint 3 — source primitives

- Created `src/ingest/source.rs` as the single owner of bounded JSONL reads,
  `FileIdentity`, `CapturedExtent`, `SourceSnapshot`, and its opaque bounded
  JSONL reader. The bridge imports these real types; it contains no duplicate
  definitions or `_from_file` bypasses.
- Moved the bounded-line contract with its implementation and added an exact
  eight-byte boundary case. Changing `<= limit` to `< limit` failed that case;
  the comparison was restored.
- Added a direct FileIdentity contract separating full metadata equality from
  same-file continuity. Requiring ctime equality in `same_file` failed the
  characterization; device/inode semantics were restored.
- Added six fast source contracts covering bounded lines, identity semantics,
  append-after-capture, rename-over, out-of-range reads, and in-place
  truncation. Removing the captured-extent `Take` admitted appended bytes and
  failed the append contract; the cap was restored.
- Converted owner parsing, handoff revalidation, all descriptor-level
  fingerprint reads, and projection streaming to the same `SourceSnapshot`
  opened authoritatively by `process_file`. The before-open, after-snapshot,
  IMMEDIATE transaction, after-transaction-read, projection, fingerprint,
  checkpoint, rematerialization, and commit order remains unchanged.
- Strengthened the source role against SQL, HTTP, Storage, Projection, and
  upward feature dependencies; removed the unused alternate `ingestion/*`
  manifest tree. Added descriptor-escape and single-owner tripwires. A
  temporary `as_file() -> &mut File` method failed the architecture contract;
  it was removed.
- Focused source contracts 6/6, 11 race/fingerprint oracles, architecture
  35/35, and external ingestion 43/43 passed. The full all-feature/all-target
  Rust suite passed 452 normal tests with 3 intentional performance tests
  ignored. Formatting, all-target/all-feature check, and strict Clippy passed.
- No schema, projector generation, live database, or source corpus was changed.
  Raw file access remains only in the session-title importer and advisory
  completeness probe, which are deliberately outside the authoritative source
  snapshot cut.

### Checkpoint 4 — checkpoint policy

- Created `src/ingest/checkpoints.rs` as the single owner of checkpoint values,
  exact chunked-fingerprint encoding, content identity, full/prefix hashing,
  append verification, rolling audit state/budgets, shrink confirmation, and
  pure unchanged/append/shrink decisions. It receives an already-opened
  `SourceSnapshot`; it cannot open paths or reach SQLite.
- Moved `SourceCheckpoint` with the policy it describes. SQLite row loading
  remains in the bridge as the temporary persistence adapter, while the
  persisted value itself is independent of rusqlite.
- Added ten direct contracts covering wire/digest stability, parse and audit
  boundaries, one-pass full hashing, prefix-tail repair, partial and aligned
  append reads, prior-tail mutation rejection, audit budget/state transitions,
  pending-shrink identity, and the complete checkpoint decision matrix.
- Sensitivity proofs all failed as intended: rejecting a cursor exactly equal
  to the chunk count broke the parse boundary; delaying an audit by one second
  broke the exact interval boundary; using raw encoded fingerprint bytes for
  pending-shrink identity broke audit-only equivalence. Every mutation was
  restored before the accepted gates.
- Production `process_file` now calls the moved pure decisions and descriptor
  algorithms. The legacy path-opening fingerprint wrappers and duplicate
  bridge tests/definitions were deleted; the handoff compatibility path now
  opens one explicit `SourceSnapshot` and passes it inward.
- Added an `IngestCheckpoints` architecture role and single-owner/private-module
  tripwires. Checkpoints may depend only on Protocol and Source and are barred
  from filesystem opening, SQL, HTTP, Storage, Projection, Catalog,
  checkpoint-store, and orchestration ownership.
- Focused checkpoint contracts 10/10, ingestion units 94/94, architecture
  36/36, and external ingestion contracts 43/43 passed. The full
  all-feature/all-target Rust suite passed 460 normal tests with 3 intentional
  performance tests ignored. Formatting, all-target/all-feature check, and
  strict Clippy passed.
- No schema, projector generation, live database, source corpus, rebuild, or
  reingestion was involved. Checkpoint SQL reads and the pending-shrink marker
  deliberately remain bridge debt for Checkpoint 5.

### Checkpoint 5 — checkpoint persistence

- Created `src/ingest/checkpoint_store.rs` as the narrow supplied-connection
  persistence adapter for checkpoint reads and the pending same-source shrink
  marker. It opens no database or filesystem resource and owns no transaction.
- Moved the shared `source_files` row mapper plus owner/path checkpoint lookup,
  pending-shrink key construction, exact-repeat confirmation, and owner-scoped
  marker clearing. The bridge imports the real functions; duplicate wrappers
  and definitions were deleted.
- Added three disposable-database contracts covering exact row/default mapping,
  audit-insensitive shrink identity and clear/reset behavior, caller-transaction
  participation, repeat-read write suppression, owner-scoped deletion,
  rollback behavior, and malformed-marker repair.
- Sensitivity proofs all failed as intended: panicking on malformed cursor state
  broke tolerant checkpoint loading; denying an exact shrink repeat broke the
  confirmation contract; broad marker deletion broke owner isolation. Every
  mutation was restored before the accepted gates.
- Added an `IngestCheckpointStore` architecture role and a single-owner,
  no-transaction-ownership contract. Final checkpoint upsert, unchanged
  marking, path-conflict deletion, accepted-shrink deletion, rematerialization,
  and commit ordering remain explicitly in the projection bridge.
- Focused store contracts 3/3, ingestion units 116/116, architecture 37/37,
  and external ingestion contracts 43/43 passed. The full all-feature/all-target
  Rust suite passed 464 normal tests with 3 intentional performance tests
  ignored. Formatting, all-target/all-feature check, and strict Clippy passed.
- No schema, projector generation, live database, source corpus, rebuild, or
  reingestion was involved. Catalog discovery and selection remain the next
  temporary bridge debt.

### Checkpoints 6 and 7 — Catalog and bridge integration

- Created `src/ingest/catalog.rs` as the single owner of decoded source
  candidates, deterministic preference, pure owner topology, persisted-source
  correlation, exact/correlated empty protection, readiness-before-preference
  planning, newline completeness, and root JSONL discovery.
- Catalog receives decoded `OwnerMeta`, persisted extent values, explicit
  existing-owner topology, and bridge-computed readiness booleans. It owns no
  SQLite, transaction, projection, report, reconciliation, runtime, or opened
  `SourceSnapshot` behavior.
- Kept `load_selected_source_extents`, `load_existing_owner_threads`, both
  descriptor-backed handoff readiness functions, `process_file`, root-attempt
  reporting, signature adoption, reconciliation, and title synchronization in
  the coordinator bridge.
- Moved the generic UUID-shape predicate to Protocol metadata so Catalog path
  correlation and bridge UUIDv7 validation share one real implementation.
- Added direct contracts for discovery classification, UUID/unique-filename
  correlation, exact versus correlated empty policy, complete-graph topology,
  and the Catalog selection planner. The planner contract covers
  readiness-before-preference, mixed/all-unready protection, exact-empty
  deferral, correlated protection without deferral, comparator wiring, initial
  protection, and deterministic path ordering.
- Sensitivity proofs all failed as intended: admitting empty sources, guessing
  an ambiguous filename owner, deferring correlated empties, ignoring persisted
  topology, admitting unready candidates, protecting only all-unready owners,
  and removing path ordering each broke its direct contract. Every mutation
  was restored before the accepted gates.
- Added and incrementally strengthened the `IngestCatalog` architecture role.
  It permits only Protocol plus legitimate discovery filesystem dependencies,
  single-owns every moved symbol, and explicitly anchors SQL, snapshots,
  projection, reports, reconciliation, and orchestration in the bridge.
- Catalog direct contracts 6/6, ingestion units 121/121, architecture 38/38,
  and external ingestion contracts 43/43 passed. The full all-feature/all-target
  Rust suite passed 470 normal tests with 3 intentional performance tests
  ignored. Formatting, all-target/all-feature check, and strict Clippy passed.
- No schema, projector generation, live database, source corpus, rebuild, or
  reingestion was involved. Stage 7 may now extract decoding and projection
  behind the accepted source/checkpoint/catalog inputs.
