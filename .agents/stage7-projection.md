# Stage 7 — Ingestion protocol and projection

Status: **complete**

## Purpose

Separate pure Codex JSONL interpretation from SQLite projection while
preserving the exact streaming and transaction semantics of the current
projector.

This stage is an ownership refactor, not a projector rewrite. It must not
change the schema, projector generation, serialized cursor state, normalized
rows, source checkpoint bytes, or the order in which a record can observe the
effects of preceding records.

## Starting point

- `src/ingest.rs` remains the temporary Stage 8 coordinator/file-ingestor
  bridge.
- Stage 6 already moved `CursorState`, `TokenUsage`, owner/session metadata
  values, bounded source reads, checkpoint policy/persistence, and Catalog.
- The remaining record path is one interleaved graph:
  `project_record` → response/event handlers → lifecycle, event, message,
  tool, owner, and agent SQL.
- Every record projection descendant currently receives the same supplied
  `rusqlite::Transaction`; none opens a database independently.

## Non-negotiable semantic boundary

Projection is streaming, not batch-oriented:

```text
bounded source record
    -> decode one record
    -> apply its intent(s) in the current IMMEDIATE transaction
    -> advance to the next record
```

Do not decode a complete file and apply later. Later source records query rows
written by earlier records for feedback admission, canonical message/reasoning
deduplication, turn lifecycle precedence, tool matching, and promoted-agent
authority.

The file transaction order remains:

1. claim the IMMEDIATE writer;
2. revalidate the captured source snapshot;
3. clear path conflicts/rebuild state when required;
4. upsert the owner;
5. decode and apply records sequentially;
6. fingerprint the committed prefix;
7. save the final source checkpoint;
8. clear a confirmed shrink marker;
9. rematerialize the current promoted agent and observed children;
10. commit.

Complete malformed JSON still advances the source offset and records a file
failure. Semantic decode/projection errors still roll back the entire file.
Incomplete tails still advance nothing.

## Target ownership

```text
src/ingest.rs                              temporary Stage 8 bridge
src/ingest/protocol/
├── mod.rs                                 narrow protocol exports
├── state.rs                               exact persisted CursorState only
├── timestamp.rs                           source timestamp normalization
├── identifiers.rs                         bounded relational identifiers/UUIDs
├── content.rs                             content, redaction, envelopes, metadata
├── duration.rs                            duration decoding and bounds
├── metadata.rs                            owner/session metadata decoding
├── tokens.rs                              token snapshots, validation, deltas
├── wire.rs                                borrowed source envelope vocabulary
├── intent.rs                              owned DecodedRecord/intent families
└── decode/                                pure family decoders
    ├── mod.rs
    ├── response.rs
    ├── event.rs
    └── tool.rs

src/ingest/projection/
├── mod.rs                                 concrete projection entry points
├── connection.rs                          opaque connection/transaction boundary
├── checkpoint.rs                          named source checkpoint operations
├── record.rs                              sequential intent dispatcher
├── usage.rs                               usage facts and model attribution
├── conversation.rs                        messages/reasoning/event reconciliation
├── tools.rs                               tool lifecycle/matching precedence
├── metadata.rs                            thread/rollout metadata authority
├── lifecycle.rs                           turns, owners, agents, observations
└── removal.rs                             rollout removal/rematerialization
```

The final tree may combine files when two names prove to be one cohesive
policy, but it may not replace these boundaries with generic `common`,
`helpers`, repository, sink, or raw-SQL escape modules.

## Opaque SQLite boundary

- `ProjectionConnection` is the only projection-side adapter around a supplied
  mutable SQLite connection.
- `ProjectionTx` privately owns the IMMEDIATE transaction.
- `ProjectionContext` couples one `ProjectionTx` borrow with the mutable
  `CursorState` required for sequential record application.
- None implements `Deref`, `DerefMut`, `AsRef`, `AsMut`, or `into_inner`.
- There is no public generic `execute`, `query`, `prepare`, closure-based raw
  access, or SQL string parameter.
- Named operations expose domain outcomes, never `rusqlite::Row`, Statement,
  Connection, or Transaction.
- The Stage 8 bridge may retain its raw `Connection` only for pre-transaction
  checkpoint/catalog work until `FileIngestor` moves. It must use the opaque
  boundary for the projection transaction itself.

## Protocol boundary

- Protocol imports no filesystem, SQLite, Storage, HTTP, or projection code.
- `DecodedRecord` contains the canonical timestamp, source line, and typed
  intent family. It never owns an unbounded raw tool/image payload.
- Pure decoding may advance deterministic cursor state such as the last
  timestamp, cumulative token scope, explicit turn/model/effort, and native
  fork boundary.
- Projection-dependent cursor decisions—implicit turn selection, feedback
  admission/reopen, latest-open tool matching, lifecycle precedence, and agent
  authority—remain in Projection.
- Unknown records preserve the current bounded/redacted metadata behavior.

## Extraction checkpoints

### 0. Characterization before movement

- [x] Add direct contracts for canonical timestamp normalization/rejection,
      identifier bounds, content/redaction/envelope classification, duration
      bounds, and token snapshot/reset/delta semantics.
- [x] Identify and retain the direct streaming-order contract proving record
      N+1 observes record N in the same file transaction.
- [x] Add or identify row-level family oracles for Usage, Conversation, Tools,
      Metadata, and Agent/Lifecycle.
- [x] Prove representative new tests fail under temporary timestamp, token,
      duration, and streaming-order mutations; restore every mutation.

### 1. Pure protocol leaves

- [x] Move timestamp normalization, identifiers/UUID helpers,
      content/redaction/envelope handling, duration bounds, and their direct
      tests into protocol-owned files.
- [x] Move token parsing/validation and cumulative delta policy out of the
      bridge while preserving the exact `CursorState` wire shape.
- [x] Move owner/session metadata decoding so source scanning only frames and
      locates records; Protocol interprets them.
- [x] Delete every moved implementation from the bridge; add no forwarding
      aliases.

### 2. Typed wire and intent seam

- [x] Introduce borrowed `WireRecord` classification without retaining source
      bytes beyond one loop iteration.
- [x] Introduce owned `DecodedRecord` and the smallest typed intent families
      needed by the next projection cut.
- [x] Decode/apply one record at a time. Do not create a file-sized intent
      collection or a generic raw-`Value` intent.
- [x] Establish a pure projected-event value before moving event SQL.

### 3. Usage and ordinary record projection

- [x] Move Usage first, preserving null scope reset, total-only hints,
      cumulative decreases, legacy unattributed cut-off, integer bounds,
      turn creation, model attribution, and owner touching.
- [x] Move DB-read-free ordinary event/turn-context/state/plan/compaction and
      unknown-record projection through typed intents.
- [x] Run the canonical projection oracle after each family.

### 4. Conversation and Tools

- [x] Move message/reasoning/final-response projection with its projection-
      owned admission, reopen, canonical reconciliation, and dedupe policy.
- [x] Move tool start/complete/enrich handling, including exact-or-latest-open
      matching and terminal-state precedence.
- [x] Keep payload suppression and redaction protocol-owned; keep database
      facts and precedence Projection-owned.

### 5. Metadata and Agent/Lifecycle

- [x] Move root metadata/title authority, turn start/complete/abort/rollback,
      implicit interruption, and final completion precedence.
- [x] Move observed-agent promotion/authority last, after Projection owns all
      native lifecycle evidence and equal-timestamp ordering.
- [x] Centralize agent observation rematerialization only after the complete
      lifecycle policy is in one module.

### 6. Removal and checkpoint prerequisites

- [x] Move final checkpoint upsert, confirmed-shrink deletion, unchanged-file
      metadata update, and post-checkpoint rematerialization behind named
      Projection operations.
- [x] Split rollout removal into an immediately-applied projection removal
      plus typed ordered surviving-source evidence. Source reads owner metadata
      from those paths; Projection applies the resulting metadata reset in the
      same IMMEDIATE transaction.
- [x] Projection never opens JSONL; Source never imports SQLite.
- [x] Move normalized rollout deletion behind an owned `RemovalImpact` before
      introducing the opaque transaction. Otherwise removal/reconciliation
      would require a raw SQLite escape hatch through the new boundary.

### 7. Opaque projection transaction and bridge integration

- [x] Checkpoint 7A: introduce the opaque connection/transaction types and
      convert every existing named Projection operation without otherwise
      changing record decoding or dispatch.
- [x] Checkpoint 7B: move the complete typed record decoder and sequential
      dispatcher across that boundary, then delete the bridge dispatcher.
- [x] Introduce single-owner `ProjectionConnection`, `ProjectionTx`, and
      `ProjectionContext` only after every remaining transaction-coupled
      removal/checkpoint consumer has a named Projection operation.
- [x] Move the record transaction under the opaque boundary without changing
      its begin/read/write/checkpoint/rematerialize/commit order.
- [x] Make record-level cursor changes atomic by applying them to a cloned
      candidate state and publishing that state only after projection succeeds.
- [x] Add architecture contracts banning raw access traits, generic SQL
      methods, filesystem imports, and `Transaction` outside Projection for
      moved responsibilities.
- [x] Replace the final `project_record` bridge call with direct
      `protocol::decode_record` plus `ProjectionContext::apply` and delete the
      legacy descendant graph.
- [x] Leave only Stage 8 coordination/file-ingestion/reconciliation/title/
      attempt-state responsibilities in `src/ingest.rs`.

## Required behavior oracles

Every checkpoint runs the relevant direct unit tests plus:

- canonical corpus logical projection identity and SQLite integrity/FK checks;
- incremental-versus-clean equivalence and discovery-order permutations;
- malformed complete record rollback/reporting and incomplete-tail behavior;
- cumulative/reset/duplicate usage and model attribution;
- message/reasoning/final canonicalization and metadata-free feedback;
- tool exact/latest-open matching, completion precedence, and payload absence;
- turn interruption/start/final/abort/rollback ordering;
- native/promoted agent authority and equal-timestamp observation ordering;
- rollout removal, surviving metadata recomputation, and checkpoint atomicity;
- writer-before-read and captured-source race contracts.

## Verification cadence

After each small cut:

1. focused protocol/projection unit tests;
2. `tests/architecture_contract.rs`;
3. the relevant ingestion external contract(s);
4. `tests/corpus_acceptance.rs`, including the canonical projection oracle.

After every cohesive checkpoint:

- `cargo fmt --all -- --check`;
- `cargo check --all-targets --all-features`;
- `cargo clippy --all-targets --all-features -- -D warnings`;
- full Rust test suite.

No browser or live-server gate is needed until Stage 8 because Stage 7 changes
no HTTP/runtime composition. No command may rebuild or ingest the user's live
database.

## Stop conditions

Restore the current cut before proceeding if normalized rows, serialized
cursor/checkpoint bytes, transaction order, source-line/timestamp identity,
projection-dependent precedence, or canonical projection output changes; if a
protocol module imports SQLite/filesystem; if Projection imports Source or
opens a path; if raw SQLite escapes the opaque boundary; or if a migration,
projector bump, live rebuild, or reingestion appears necessary.

## Accepted checkpoints

- [x] Checkpoint 0 — characterization
- [x] Checkpoint 1 — pure protocol leaves
- [x] Checkpoint 2 — typed wire and intent seam
- [x] Checkpoint 3 — Usage and ordinary records
- [x] Checkpoint 4 — Conversation and Tools
- [x] Checkpoint 5 — Metadata and Agent/Lifecycle
- [x] Checkpoint 6 — removal and checkpoint prerequisites
- [ ] Checkpoint 7 — opaque projection transaction and bridge integration

For every accepted checkpoint, record files moved/created, tests added or
relocated, sensitivity proof, focused/external/full results, and deliberately
retained Stage 8 bridge debt below.

## Accepted evidence

### Checkpoint 0 — characterization

- Added direct exact contracts for UTC offset/fraction normalization and year
  bounds, Unicode-counted identifiers and unsafe content, nested content and
  attachment omission, redaction/envelope classification, every duration
  representation and bound, and accepted/rejected token snapshots.
- Retained `metadata_free_feedback_after_provisional_final_stays_on_native_turn`
  as the concrete same-file streaming oracle: its later feedback record must
  observe the preceding final-message lifecycle writes.
- Mapped the existing family oracles before movement: cumulative/reset Usage;
  messages/reasoning/finals; tool terminal precedence; owner/turn/agent
  lifecycle; removal/rematerialization; source checkpoint rollback; and the
  canonical all-table corpus projection.
- Sensitivity proofs all failed as intended and were restored: noncanonical
  timestamp formatting, omitted output tokens in derived totals, truncated
  nanosecond rounding, and disabling feedback reopen against the current
  transaction.
- Restored focused characterizations: 5/5 plus the streaming oracle. The
  subsequent protocol modules retain expanded direct tests with the moved
  code.

### Checkpoint 1 — pure protocol leaves

- Created single-owner Protocol modules for timestamp, identifier/UUID,
  content/redaction/envelope, duration, token accounting, cursor state, and
  owner/session metadata interpretation. `src/ingest.rs` retains none of the
  moved implementations or forwarding aliases.
- The bounded scanner now parses one JSON value and delegates owner topology
  and authored metadata interpretation to `decode_owner_record`; it retains
  only JSONL framing and the path-specific missing-metadata error.
- Kept lexical `std::path::Path` use for deriving a project name from `cwd`,
  while architecture rules continue to reject filesystem I/O, `PathBuf`,
  source snapshots, SQLite, HTTP, and Projection dependencies in Protocol.
- Direct Protocol contracts: 26/26 leaf tests passed. The prepared typed Usage
  decoder adds 7 further tests but remains outside production until Checkpoint
  2 is accepted.
- Focused/external gates passed: architecture 38/38; corpus acceptance 17/17;
  ingestion edge contracts 20/20; ingestion truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (348 library tests passed, 3 ignored,
  plus 145 integration/runtime/architecture tests). The five pricing fixture
  tests required loopback permission and then passed unchanged.
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched.

### Checkpoint 2 — typed wire and Usage projection seam

- Added borrowed `WireRecord`, owned `DecodedRecord`, deterministic
  `CursorTransition`, and the first typed `ProtocolIntent`. Intents contain no
  raw `Value`, source bytes, payload JSON, image data, or unbounded tool data.
- Usage now follows the required streaming path: decode one record, apply its
  transition to a cloned candidate state, write through Projection in the
  current supplied transaction, and publish the candidate state only after
  projection succeeds. No file-sized intent collection exists.
- Moved Usage persistence, the legacy model-attribution cutoff, event identity,
  turn upsert, and owner timestamp touching into `ingest/projection`. The
  bridge no longer contains token accounting or `usage_facts` SQL.
- Added 7 direct typed-decoder tests, one exact row-level projection test, and
  a structural contract enforcing the no-raw-intent boundary, single Usage SQL
  owner, and transition/project/publish order.
- Focused/external gates passed: all 153 ingestion unit tests; architecture
  39/39; corpus acceptance 17/17; ingestion edge contracts 20/20; ingestion
  truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (357 library tests passed, 3 ignored,
  plus 146 integration/runtime/architecture tests).
- The pure projected-event type remains deliberately pending until ordinary
  event families move; Checkpoint 2 is complete for its first consumer rather
  than pretending to generalize a shape before that consumer exists.
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched.

### Checkpoint 3 — Usage and ordinary record projection

- Added a bounded typed `ProjectedEvent` whose call identity and compact
  compaction/subagent/unknown metadata contain no raw source `Value` or
  payload bytes. Protocol owns shaping and Projection owns typed metadata JSON
  serialization plus the single generic `events` insert.
- Rewired every existing generic event producer directly through
  `shape_projected_event` and `projection::apply_event`; deleted the bridge's
  `insert_event`, compaction, subagent-metadata, and unknown-metadata helpers.
  The only direct bridge event insert left is the distinct implicit-turn-
  interruption fact, which will move with lifecycle authority in Checkpoint 5.
- Added four direct shaping tests, two exact Projection/trigger tests, and an
  architecture contract for the typed boundary and deleted bridge ownership.
- Incremental gate passed: 6 direct event tests, 93 ingestion unit tests,
  architecture 40/40, corpus 17/17, ingestion edge 20/20, and ingestion
  truthfulness 6/6.
- Added a closed ordinary-record decoder and Projection owner for turn context,
  thread settings, plan/review/compaction events, intentional no-ops, and
  bounded unknown records. Conversation, tool, metadata, lifecycle, and agent
  families are explicitly reserved rather than being swallowed by the unknown
  fallback.
- Ordinary intents contain no raw JSON; the largest projected-event variant is
  boxed. Projection applies the deterministic transition to a candidate,
  performs exact event/turn/touch writes, and publishes the cursor only after
  every write succeeds.
- Deleted the duplicate bridge arms for settings, plans, review state, image/
  dynamic request no-ops, and the generic unknown fallback. Added seven pure
  decoder tests, five exact row/ordering/rollback tests, and a structural
  contract enforcing reservation, admission order, typed ownership, and
  project-before-publish semantics.
- Focused/external gates passed: 14 ordinary seam tests; 93 ingestion unit
  tests; architecture 41/41; corpus acceptance 17/17; ingestion edge 20/20;
  ingestion truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (375 library tests passed, 3 ignored,
  plus 148 integration/runtime/architecture tests).
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched. Checkpoint 3 is accepted.

### Checkpoint 4 — Conversation and Tools

- Added closed typed decoders and projection owners for Conversation and
  Tools. The bridge now delegates canonical/user/assistant messages,
  reasoning, final-answer completion, tool starts, tool enrichment, and tool
  terminal envelopes without retaining raw response payloads in an intent.
- Preserved Conversation's projection-dependent rules inside Projection:
  metadata-free feedback admission/reopen, strict sub-second canonical
  reconciliation, redaction-aware reasoning equality, explicit terminal-turn
  authority, source message identity, and root-only title fallback.
- Preserved Tool matching and authority exactly: explicit call identity wins,
  otherwise the latest open same-rollout/name row is eligible only when the
  raw name survived projection unchanged; terminal failure cannot regress,
  later starts cannot reopen terminal rows, and activity identity remains the
  source identity rather than the matched row identity.
- Deleted the corresponding bridge response-item handlers and message/tool
  persistence helpers. Architecture contracts now require both families to
  cross typed no-raw seams, keep their SQL in Projection, retain routing order,
  and leave no duplicate helper/SQL owner in `src/ingest.rs`.
- Added direct boundary tests for exact one-second reconciliation, redacted
  canonical reasoning, explicit terminal-turn authority, deferred invalid
  message IDs, completion-before-start, exact-versus-latest-open matching,
  redaction-changed tool names, cross-turn/rollout matching, rollback, and
  cursor publication.
- Sensitivity was proved and restored: widening Conversation's strict
  reconciliation window made the exact-boundary oracle fail; bypassing the
  redaction/name guard made the Tool fallback oracle fail. Both production
  predicates were restored and the focused tests passed again.
- Focused/external gates passed: 204 ingestion unit tests; architecture 43/43;
  corpus acceptance 17/17; ingestion edge 20/20; ingestion truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (408 library tests passed, 3 intentional
  release-scale ignores, plus 150 integration/runtime/architecture tests).
  The five localhost Pricing fixtures required loopback permission and then
  passed unchanged.
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched. Checkpoint 4 is accepted.

### Checkpoint 5 — Metadata and Agent/Lifecycle

- Added closed typed decoder and Projection owners for root/session metadata,
  thread state, native turn lifecycle, native agent ownership, and parent-side
  agent observations. The bridge now performs only family admission/routing;
  it retains no lifecycle SQL, owner upsert, observation authority, or
  rematerialization implementation.
- Preserved the exact owner order: thread metadata, rollout metadata, native
  agent promotion, then thread-bound recomputation. Observation projection
  still persists its event before reconciling agent state, touches the owner,
  and publishes cursor state only after all projection work succeeds.
- Centralized promoted-agent authority and rematerialization in
  `projection/agents.rs`. Synthetic-only rows are removed first, surviving
  native rollout/turn/agent state is restored, and observations replay in the
  stable timestamp/source-path/source-line/event-ID order using the required
  activity-owner index.
- Added a seven-test external lifecycle authority contract plus direct module
  tests covering implicit predecessor interruption, every explicit terminal
  state, equal-timestamp native/parent authority, event-before-reconciliation,
  synthetic observation tie-breaking, rollback, and native restoration.
- Sensitivity was proved and restored: removing the explicit-terminal
  predecessor guard rewrote a completed turn to interrupted and failed the
  external oracle; weakening strict newer-native promotion precedence from
  `>` to `>=` failed the direct equality matrix. Both production predicates
  were restored and the complete focused gate passed.
- An independent read-only audit found no duplicate policy or ordering defect.
  It verified that the remaining `task_started` parsing is the intentional
  fork-admission gate and that removal/checkpoint composition is correctly
  deferred to Checkpoints 6 and 7.
- Focused/external gates passed: agent projection 7/7; lifecycle authority
  7/7; architecture 47/47; corpus acceptance 17/17; ingestion edge contracts
  20/20; ingestion truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (443 normal library tests passed, 3
  intentional release-scale ignores, and every integration/runtime/
  architecture suite passed). The five localhost Pricing fixtures required
  loopback permission and then passed unchanged.
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched. Checkpoint 5 is accepted.

### Checkpoint 6 — removal and checkpoint prerequisites

- Added `projection/checkpoint.rs` with owned scalar DTOs and named operations
  for path-conflict lookup, exact source checkpoint save/deletion, unchanged
  metadata refresh, confirmed-shrink clearing, and post-checkpoint lifecycle
  replay. It accepts no `Path`, `FileIdentity`, source snapshot, raw SQL, or
  generic access callback from callers.
- Added `projection/removal.rs` with owned `RemovalImpact` and ordered
  surviving-source evidence. Normalized rollout rows and derived synthetic
  agents are removed immediately; affected agents and exact thread bounds are
  rebuilt before the impact returns. Projection never opens a surviving path.
- The temporary bridge now performs only the genuine cross-boundary
  composition: apply Projection removal, read surviving owners in returned
  order through the descriptor-backed source decoder, then apply the typed
  metadata reset in the same IMMEDIATE transaction.
- Preserved the exact file tail order: save checkpoint, clear a confirmed
  shrink only when applicable, rematerialize the current promoted rollout,
  replay observed children after the durable source path exists, then commit.
  Source checkpoint deletion remains independently named so reconciliation
  and path replacement can retain the same transaction.
- Added ten direct transaction tests covering exact checkpoint fields/error
  accumulation, unchanged metadata/fingerprint/archive behavior, path
  conflict identity, checkpoint-deletion commit/rollback, owner-scoped shrink
  clearing, current-before-children replay, normalized-row removal,
  surviving metadata field precedence, and whole-operation rollback.
- Sensitivity was proved and restored twice: reversing surviving source-path
  order failed the metadata evidence oracle; replaying observed children
  before the current rollout failed the trigger-recorded lifecycle-order
  oracle. Both production orders were restored.
- Focused/external gates passed: Projection 53/53; architecture 48/48; corpus
  acceptance 17/17; ingestion edge contracts 20/20; lifecycle authority 7/7;
  ingestion truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (453 normal library tests passed, 3
  intentional release-scale ignores, and every integration/runtime/
  architecture suite passed). The localhost Pricing fixtures passed with
  loopback permission.
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched. Checkpoint 6 is accepted.

### Checkpoint 7A — opaque projection transaction

- Added the sole raw SQLite adapter, `projection/connection.rs`, with named
  deferred and IMMEDIATE transaction starts. `ProjectionTx` exposes its raw
  field only inside the Projection namespace and offers no deref, generic SQL,
  closure, or inner-value escape hatch to ingestion orchestration.
- Converted all 57 existing Projection transaction consumers and their direct
  tests to the opaque transaction while preserving the four original lock
  behaviors: file projection and reconciliation remain IMMEDIATE; unchanged
  metadata refresh and session-title import remain deferred.
- The bridge now calls named transaction starts and named Projection domain
  operations only. Feature SQL remains in its owning Projection module rather
  than collapsing into the adapter.
- Added an architecture role and sensitivity contract that rejects raw
  `Connection`/`Transaction` ownership outside the adapter, raw constructors
  inside feature projections, bridge access to `.sqlite`, and representative
  deref/generic-query escape APIs.
- Direct/focused gates passed: Projection 54/54; architecture 49/49; corpus
  acceptance 17/17; ingestion edge contracts 20/20; lifecycle authority 7/7;
  ingestion truthfulness 6/6.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (454 normal library tests passed, 3
  intentional release-scale ignores, and every integration/runtime/
  architecture suite passed).
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched. Checkpoint 7A is accepted; decoder/dispatcher
  movement remains isolated in Checkpoint 7B.

### Checkpoint 7B — unified record routing and atomic dispatch

- Added the sole pure record router in `protocol/decode/record.rs`. It returns
  one closed `DecodedRecord` variant and owns family precedence, canonical
  timestamp validation, the fork-native admission gate, early session metadata
  admission, response-before-event tool classification, and the ordinary
  fallback. Protocol retains no SQLite or filesystem dependency.
- Added `projection/record.rs` with exhaustive dispatch through one
  `ProjectionContext`. It clones the complete cursor, applies all transition
  and SQL work to that candidate, and publishes the candidate only after the
  record succeeds. Cursor-only records execute no SQL.
- Deleted the bridge `project_record` graph. The file loop now performs exactly
  one `decode_record` followed by one `ProjectionTx::context(...).apply(...)`
  while retaining the same IMMEDIATE transaction, record streaming,
  checkpoint, rematerialization, and commit order.
- Added a black-box fork ordering contract proving inherited cumulative usage
  establishes the baseline before native admission, so the first native fact
  contains only the exact delta. Added direct projection failures proving a
  failed write cannot advance the serialized cursor, cumulative counters, or
  timestamp.
- Sensitivity was proved and restored twice. Moving usage behind the fork gate
  changed the expected native delta from 56 to 176 tokens and failed the new
  external oracle. Applying directly to the live cursor made the forced usage
  failure advance cumulative state and failed the atomicity oracle.
- Architecture now registers the unified router and dispatcher, rejects raw
  `Value` at the typed intent seam, requires exhaustive variant dispatch, and
  forbids the bridge from retaining individual family decode/apply calls.
- Focused gates passed: Protocol 81/81; Projection 57/57; architecture 50/50;
  corpus acceptance 17/17; ingestion edge contracts 20/20; lifecycle authority
  7/7; ingestion truthfulness 6/6; protocol ordering 1/1; generation/publication
  characterization 4/4.
- Cohesive gate passed: formatting, all-target/all-feature check, strict
  Clippy, and the complete Rust suite (463 normal library tests passed, 3
  intentional release-scale ignores, and every integration/runtime/
  architecture suite passed).
- No schema, projector generation, live database, migration, rebuild, or
  reingestion was touched. Stage 7 is accepted.
