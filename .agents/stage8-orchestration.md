# Stage 8 — Ingestion orchestration and reconciliation

Status: **complete; Checkpoints 0–7 accepted**

## Purpose

Delete the remaining `src/ingest.rs` bridge by separating the real use cases
without introducing forwarding functions, generic metadata writes, or a
renamed coordinator god object. Preserve the existing locking, two-pass
publication, source-snapshot, and one-file transaction semantics.

No checkpoint may read, rebuild, or reingest the user's live database. All
behavioral and browser verification uses disposable roots and projections.

## Target tree

```text
src/ingest/
├── mod.rs                 declarations and narrow public re-exports only
├── coordinator.rs         scan cycle, one-shot sequence, reports, root adoption
├── scanner.rs             background worker and bounded shutdown
├── attempt.rs             durable attempt state and generation publication
├── session_titles.rs      session_index.jsonl import
├── reconciliation.rs      missing-source planning and atomic removal
├── owner_reader.rs        descriptor-backed owner decoding
├── file_ingestor.rs       one-file source/checkpoint/projection transaction
└── tests.rs               unchanged test-only regression carrier
```

Existing `catalog`, `source`, `checkpoints`, `checkpoint_store`, `protocol`,
and `projection` modules retain their ownership. The final `mod.rs` contains no
SQL or forwarding algorithms.

## Checkpoint 0 — Freeze the orchestration oracle

- [x] Move the inline `#[cfg(test)] mod tests` mechanically to
      `src/ingest/tests.rs`, preserving test paths and behavior.
- [x] Add an external contract proving `scan_once` may update per-source
      projector state but never publishes the global projector generation.
- [x] Prove `scan_one_shot` publishes only after its complete bounded sequence
      succeeds.
- [x] Correct the staged architecture rule so a normal directory declaration
      `pub mod ingest;` is required rather than mistaken for a legacy facade.

Accepted evidence:

- 95 ingestion unit/regression tests pass from the extracted test carrier;
- 4 external generation-publication contracts pass;
- the publication-from-`scan_once` mutation fails at the intended contract;
- all 50 architecture contracts pass;
- formatting, all-target checks, strict Clippy, and the full Rust suite pass;
- one SIGTERM runtime test timed out only during the contended full run, then
  passed alone and as part of the complete 18-test runtime suite.

Sensitivity: temporarily publish the generation from `scan_once`; the new
external contract must fail, then pass after restoration.

## Checkpoint 1 — SessionTitleImporter — complete

Move `IndexedTitle`, `sync_session_index_titles`, `discover_session_index`, and
`session_index_candidates` together.

Preserve:

- only configured root parents are searched; ambient `CODEX_HOME` is ignored;
- bounded reads skip oversized, malformed, and incomplete records;
- maximum `(updated_micros, line_number)` wins;
- titles are redacted and bounded;
- open/read failures remain warnings rather than scan failures;
- import still runs after reconciliation/root adoption and before attempt
  finalization, including scans where rollout files were unchanged.

Sensitivity: invert the replacement comparator and require
`session_index_title_wins_and_refreshes_without_rollout_reingestion` to fail.

Gate: title tests, architecture, corpus, full Rust/static gate.

Accepted evidence:

- configured-root discovery and importer policy now live only in
  `src/ingest/session_titles.rs`; the coordinator passes paths rather than its
  root aggregate, avoiding a future ownership cycle;
- 9 focused title tests cover refresh, configured scope, equal-instant
  line-order precedence, UTC normalization, redaction/bounds, oversized and
  malformed records, incomplete tails, read failures, and open failures;
- dropping the line-number tie-break fails the equal-instant oracle, then the
  restored implementation passes;
- all 51 architecture contracts, 17 corpus contracts, 4 generation contracts,
  formatting, all-target checks, strict Clippy, and the full Rust suite pass;
- the full suite reports 467 passing library tests and 3 intentional ignored
  release-scale Activity gates, with every integration/runtime suite passing.

## Checkpoint 2 — AttemptRecorder — complete

Move projector generation/currentness, named ingest-state and root-signature
operations, attempt finalization, interrupted-state recovery, and the control
state used by error finalizers. Delete generic `set_meta`; Coordinator owns
error control flow and report serialization, while Attempt exposes named
operations only.

Preserve:

- an empty projection is vacuously current;
- a nonempty projection requires both the global marker and every source
  checkpoint generation;
- only success advances `last_ingest_at` and clears `last_ingest_error`;
- failure records attempt/report/error state without advancing success time;
- finalizer failure never replaces the triggering error;
- root-signature adoption remains one autocommit control-state change.

Sensitivity: remove the stale-checkpoint guard; then write `last_ingest_at` on
failure. The generation and truthfulness contracts must fail independently.

Gate: generation, recovery, truthfulness, architecture, corpus, full gate.

Accepted evidence:

- `src/ingest/attempt.rs` single-owns named attempt state, root-signature,
  recovery, projector-generation publication, and currentness operations;
- orchestration retains locks, timestamps/report serialization, and both
  triggering-error finalizer decision trees; the generic metadata writer is
  gone;
- direct oracles freeze stale-checkpoint publication rejection,
  failure-then-success metadata truthfulness, and root adoption durability
  across a later finalizer failure;
- deleting the stale-checkpoint guard and writing last success on failure each
  fail their intended oracle, then pass after restoration;
- all 52 architecture contracts, 17 corpus contracts, generation and
  truthfulness suites, formatting, all-target checks, strict Clippy, and the
  full Rust suite pass;
- the full suite reports 470 passing library tests and 3 intentional ignored
  release-scale Activity gates, with every integration/runtime suite passing.

## Checkpoint 3 — Reconciliation

Create a pure ordered plan from observed paths, protected owner IDs, enumerated
roots, incomplete roots, and persisted source rows. Apply that plan in one
IMMEDIATE projection transaction using Stage 7's typed removal impact,
descriptor-backed surviving owner reads, metadata reset, checkpoint deletion,
and abandoned-thread cleanup.

Preserve:

- no enumerated/incomplete roots means no-op;
- observed paths and protected owners survive;
- sources below an incomplete root survive;
- sources outside configured roots are removable only after root-signature
  confirmation;
- failure in one fully enumerated root does not preserve unrelated deletions;
- existing candidate order is loaded before acquiring the IMMEDIATE writer.

Sensitivity: remove incomplete-root protection, then reconcile during the
first adoption scan. The traversal and changed-root contracts must fail.

Gate: deletion, malformed-neighbor, incomplete-root, root-change,
rematerialization, architecture, corpus, full gate.

Accepted evidence:

- `src/ingest/reconciliation.rs` owns the pure ordered removal plan and its
  one-transaction application; the candidate query is Projection-owned and
  loaded before the IMMEDIATE writer transaction begins;
- `src/ingest/owner_reader.rs` separately owns bounded descriptor-backed owner
  decoding, so Reconciliation contains neither SQL nor raw JSONL parsing;
- focused contracts cover a mixed complete/incomplete-root scan, rollback of
  every removal when the second checkpoint deletion fails, orphan fallback to
  the checkpoint thread ID, root adoption, and malformed-neighbor behavior;
- removing incomplete-root protection fails the traversal oracle at the
  intended assertion, then passes after restoration;
- all 54 architecture contracts, the reconciliation/root/corpus suites,
  formatting, all-target checks, strict Clippy, and the full Rust suite pass;
- the full suite reports 475 passing library tests and 3 intentional ignored
  release-scale Activity gates, with every integration suite passing;
- no live database was opened and no source was reingested.

## Checkpoint 4 — OwnerReader and FileIngestor

OwnerReader combines descriptor-bounded Source framing with pure Protocol
owner decoding. FileIngestor owns `process_file`, unchanged marking,
descriptor-backed handoff readiness, persistence adapters, file race hooks,
and private `FileReport`. One scan supplies a shared fingerprint-audit budget.

Frozen file order:

1. open one descriptor;
2. decode owner from that descriptor and validate discovery ownership;
3. validate handoff continuity through the same descriptor;
4. read checkpoint and pending-shrink state;
5. choose unchanged/audit/append/rebuild policy;
6. begin IMMEDIATE before the first transactional read;
7. clear a path conflict or rebuilding rollout;
8. upsert the owner;
9. stream each complete record through Protocol and Projection;
10. durably consume complete oversized/malformed records as failures while an
    incomplete tail advances nothing;
11. fingerprint exactly the committed prefix;
12. save the checkpoint;
13. clear the confirmed-shrink marker;
14. rematerialize the current promoted agent and observed children after the
    source path is durable;
15. commit.

Sensitivity: use a deferred transaction, reopen after capture, separate the
checkpoint commit, and advance an incomplete tail. Each existing race or
equivalence oracle must fail under its representative mutation.

Gate: append/rewrite/shrink/handoff/race/rollback, edge, corpus, architecture,
full gate.

Accepted evidence:

- `src/ingest/file_ingestor.rs` owns one descriptor-bound file projection and
  one scan-shared fingerprint audit budget; the scan bridge selects candidates
  and reuses exactly one `FileIngestor`;
- selected checkpoint extents are CheckpointStore-owned and durable
  rollout/thread anchors are Projection-owned, leaving FileIngestor free of
  SQL literals while it retains their connection-scoped composition;
- two new replacement oracles reject discovery-to-open owner changes and prove
  an owner/path replacement plus checkpoint failure rolls the old checkpoint
  and normalized projection back atomically before a clean retry;
- representative mutations prove the tests catch a deferred writer
  transaction, removed owner revalidation, projection committed separately
  from its checkpoint, and an incomplete tail advancing the durable
  checkpoint; all guards pass again after restoration;
- all 104 ingestion regressions, 17 corpus contracts, 20 edge contracts,
  lifecycle/generation suites, and 55 architecture contracts pass;
- formatting, all-target/all-feature checks, strict Clippy, and the complete
  Rust suite pass: 479 library tests, 3 intentional ignored Activity gates,
  and every integration/runtime suite;
- no live database was opened and no source was reingested.

## Checkpoint 5 — Coordinator

Move `IngestRoots`, reports, the scan-cycle body, `scan_once`, bounded
one-shot/leased variants, and the public recovery wrapper.

One cycle remains:

```text
begin attempt
→ enumerate roots
→ inspect and choose candidates
→ resolve topology
→ ingest selected files in path order
→ reconcile or adopt root signature
→ import session titles
→ finalize attempt
```

One-shot remains:

```text
validate lifetime lease
→ acquire one ingest process lock
→ first scan
→ optional confirmation scan under the same lock
→ publish projector generation
→ release lock
```

The guard covers both passes and publication. A second-pass/publication failure
retains the first-pass report and leaves the marker stale.

Sensitivity: release the lock between passes, publish before confirmation, and
reconcile on first adoption. The existing two-pass/root contracts must fail.

Gate: root/two-pass/report/truthfulness/generation, architecture, corpus, full
gate.

Accepted 2026-07-25:

- `src/ingest/coordinator.rs` now solely owns scan-cycle sequencing, lifetime
  lease validation, process-lock scope, two-pass root adoption, truthful
  finalization, and projector-generation publication;
- one-shot ingestion holds one process lock across both passes and publishes
  global currentness only after a successful confirmation pass;
- focused regressions prove that a lease cannot be reused with another
  database, a failed changed-root scan cannot replace the established root
  signature or projection, and a failed confirmation retains the truthful
  first-pass report while leaving the global generation stale;
- representative mutations prove the contracts catch a dropped/reacquired
  process lock and premature generation publication; all guards pass again
  after restoration;
- all 56 architecture contracts pass, and the complete Rust gate passes with
  482 normal library tests, 3 intentional ignored Activity gates, every
  integration/runtime suite, formatting, all-target checks, and strict Clippy;
- no live database was opened and no source was reingested.

## Checkpoint 6 — Scanner

Move `ScannerHandle`, `spawn_scanner`, its leased variant, and shutdown tests.
The lifetime `IngestScannerLease` remains Coordinator-owned so Scanner depends
on Coordinator rather than the reverse.

Preserve pre-open acquisition, canonical database identity, lease lifetime in
the worker, complete one-shot cycles, 250 ms cancellation slices, bounded
shutdown without joining blocked work, and truthful failed-cycle state.

Sensitivity: call `scan_once`, drop the lease before spawning, and join blocked
work. Generation, competing-root, and bounded-shutdown contracts must fail.

Accepted 2026-07-25:

- `src/ingest/scanner.rs` now solely owns `ScannerHandle`, background thread
  lifetime, cancellation/reaping, polling cadence, and best-effort failed-cycle
  marking;
- Coordinator retains the lifetime lease type, canonical database identity,
  process lock, complete one-shot/two-pass protocol, reconciliation, and
  generation publication; Scanner depends on those operations and not the
  reverse;
- new regressions prove canonical lease identity through a symlink alias,
  synchronous rejection of a different database before spawning, durable
  failed-cycle state, and lease release inside the bounded cancellation wake
  window;
- representative mutations prove the tests catch replacing complete one-shot
  cycles with `scan_once`, dropping the lifetime lease before worker handoff,
  and delaying bounded shutdown; all guards pass again after restoration;
- all 57 architecture contracts pass, and the complete Rust gate passes with
  485 normal library tests, 3 intentional ignored Activity gates, every
  integration/runtime suite, formatting, all-target checks, and strict Clippy;
- no live database was opened and no source was reingested.

## Checkpoint 7 — Delete the bridge

- [x] Add declaration-only `src/ingest/mod.rs` and delete `src/ingest.rs`.
- [x] Remove the staged legacy allowlist and register every new architecture
      role.
- [x] Keep `pub mod ingest;` in `lib.rs` and direct stable re-exports; add no
      compatibility forwarding functions.
- [x] Assert normalized SQL remains Projection-owned and attempt/generation SQL
      remains Attempt-owned.
- [x] Assert Coordinator, Scanner, FileIngestor, Reconciliation, Session Titles,
      and OwnerReader contain no production SQL.

Accepted 2026-07-25:

- the legacy `src/ingest.rs` bridge is deleted; `src/ingest/mod.rs` contains
  only 13 private module declarations, three direct stable re-export seams,
  and the existing test carrier;
- `lib.rs` remains exactly `pub mod ingest;`, all production and integration
  call sites retain the same public API, and `pub(in crate::ingest)` ownership
  paths remain unchanged;
- the staged allowlist is gone and the permanent architecture contract now
  requires the legacy bridge to remain absent, the declaration-only module root
  to remain present, and every extracted role/SQL boundary to stay active;
- all 281 ingestion tests, generation/lifecycle/protocol/truthfulness/edge and
  corpus suites, 57 architecture contracts, and the complete Rust gate pass;
  the final Rust gate contains 485 normal library tests and 3 intentional
  ignored Activity gates, with formatting, all-target/all-feature checks,
  strict Clippy, and diff hygiene clean;
- no live database was opened and no source was reingested.

## Final gate

- formatting, all-target/all-feature check, strict Clippy, and full Rust suite;
- canonical logical projection identity plus SQLite integrity/FK checks;
- every incremental-versus-clean and discovery-order permutation;
- truthfulness, lifecycle authority, edge, locking, shutdown, and runtime
  contracts;
- disposable CLI one-shot and live-scanner browser E2E;
- no schema, migration, projector-generation, live database, or reingestion
  change.

Bridge edits are strictly sequential: subagents may audit or run independent
verification, but only one writer changes root ingestion orchestration at a
time.
