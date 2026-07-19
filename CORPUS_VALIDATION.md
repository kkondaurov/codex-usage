# Corpus Validation

This is the acceptance contract for ingestion and browser representation. The fixtures are compact excerpts of real Codex histories, not fabricated scale data. They are small enough for deterministic tests and sharp enough to catch the July 15 replay failure.

Machine-readable counts live in `tests/fixtures/corpus/manifest.json`.

## What Was Preserved

- Physical rollout IDs, thread IDs, parent/fork IDs, turn IDs, call IDs, timestamps, model IDs, reasoning effort, lifecycle shapes, and the token counters needed by each assertion.
- Modern durable messages, progress updates, reasoning summaries, tool variants, tool results, subagent lifecycle and messages, goals, compaction, world-state, null usage, aborts, and an unknown future event.
- Legacy pre-envelope records whose session header is followed by top-level message, reasoning, function-call, function-output, and state records.
- Active and archived roots as separate directories.

The fixtures replace the local username with `/Users/example`, shorten prompts and tool output, replace encrypted/image blobs with explicit sanitized placeholders, and remove large environment payloads. No token counter or lineage field involved in an assertion was changed.

## Source Evidence

Sanitized source excerpts live under `tests/fixtures/corpus/`; the manifest
records every case, expected identity, count, and total used by the acceptance
suite. The repository intentionally does not retain machine-specific source
paths or usernames.

The original parent and both replay archives each contain 17,121 `token_count` records. Their 17,120 non-null ordered cumulative-total values have the same SHA-256:

```text
b47844ad2edaf4fcaf3aa8fea9e37bd5feb458f65f137a1e6953cdf977a56a72
```

That is stronger than a similar title or timestamp: it proves the archives carry the same copied token sequence. Their top-level timestamps were rewritten, so timestamp deduplication cannot solve this.

In the full root fork, native work starts at line 101,896. In the sampled child it starts at line 33,984. Everything before those boundaries is inherited history. The compact fixtures move the same boundary to line 8 so the invariant is cheap to test.

## Fixture Contracts

### Replay Spike

Ingest `replay_spike/active` and `replay_spike/archived` together.

- Five physical sources are discovered.
- The May parent, July root, and July child contain native work. The two archived forks are replay-only.
- The first `session_meta` owns the physical file. Nested parent metadata never reassigns it.
- `forked_from_id` records lineage; it does not make all later records inherited. Native events after the fork boundary still belong to the new rollout.
- A replayed legacy UUIDv4 task does not open a UUIDv7 fork merely because its random text sorts after the rollout ID. The native boundary requires a turn whose UUIDv7 timestamp is at or after the rollout UUIDv7 timestamp.
- Replay-only sources create no visible Session row, Turn, Tool call, message, usage fact, or cost.
- The July thread rolls the root and Russell child into one user-visible session with two native usage facts.
- July 15 totals are exactly 275,900 input, 90,880 cached input, 1,082 output, 735 reasoning output, and 276,982 total tokens.
- At the fixture price, July cost is exactly `$1.003000`. The May parent remains on May 4 at `$0.147563`.
- Ingesting in any file-discovery order produces the same result.

### Rich Trace

Ingest `rich_trace/active`.

- One thread and one turn are visible.
- Three durable messages survive at full fixture length: user prompt, assistant update, and final answer.
- The Activity view contains one reasoning summary, one assistant update, four tool actions (`exec`, web search, tool search, image generation), one child-agent branch with start/message/complete, one goal update, one compaction boundary, and one final answer.
- The paired `custom_tool_call` and output become one completed tool invocation. Completion-only MCP, web-search, patch, and image-generation records with their own call IDs become separate, fully named calls with status and duration retained. Arguments, results, raw payloads, and image bytes remain only in the source JSONL.
- Two `last_token_usage` records produce two facts totaling 84,383 input, 50,688 cached input, 736 output, 469 reasoning output, and 85,119 total tokens. Cumulative totals are reconciliation data, not a second charge.
- The unknown `future_trace_marker` is retained internally and does not break the turn or appear in the default Activity view.

### Explicit Turn Steering

Modern response items can carry `internal_chat_message_metadata_passthrough.turn_id` while a long turn is still running. Mid-turn user steering, subagent completion notices, and the reasoning events that follow them remain attached to that explicit native turn. A second user-role message is not, by itself, a new turn boundary.

### Metadata-Free Active Feedback

Older traces can omit explicit turn metadata even when several user feedback messages arrive during one native task. The native lifecycle is authoritative: a `final_answer` response is only provisional while its explicit `task_started` remains open, so later feedback and downstream assistant/tool/usage records stay on that native turn until `task_complete`, abort, rollback, or interruption. Once that turn is terminal, a later metadata-free user message does create a `legacy-turn:*` boundary. If a different native task starts first, the unfinished prior task is projected as interrupted. The live Valencia trace exercises this shape with three feedback messages in 2 ms, one assistant response addressing all three, and one native task completion.

Some older source orders place injected `AGENTS.md` and `<environment_context>` transport envelopes immediately before `task_started`, then place the actual human prompt after `task_started` but before `turn_context`. Those envelopes are not conversation messages, and the real prompt must still attach to the newly opened native turn. This prevents both context-only phantom turns and native turns with their initial prompt missing.

Repeated transport states for one tool call are also lifecycle updates, not separate visible actions. Storage retains every source event, `tool_calls` retains the final joined state, and Activity renders one row per `(rollout_id, call_id)`.

### Legacy V0

Ingest `legacy_v0/active`.

- The unwrapped header establishes rollout identity and the timestamp inherited by otherwise untimestamped records.
- One implicit turn contains one user message, two assistant messages, one reasoning summary, and one paired `shell` call/result.
- The session is visible with zero tokens and zero cost.
- A missing modern envelope is not an excuse to discard the history.

### Repeated Rate-Limit Snapshots

Ingest `rate_limit_duplicates/active`.

- Repeated rate-limit records with the same cumulative token snapshot do not create a second charge.
- Three forward cumulative increments become exactly three usage facts totaling 1,290 input, 205 cached input, 78 output, 46 reasoning output, and 1,368 total tokens.

### Sparse And Pricing

Ingest `sparse_pricing/active` and `sparse_pricing/archived`.

- The interrupted session remains visible with its prompt, aborted status, zero usage, and zero cost. A null `token_count.info` is not a usage fact.
- The guardian rollout remains visible even though the compact fixture has usage but no durable user message. It has 25,607 total tokens attributed to observed model ID `codex-auto-review`.
- With a `gpt-5.5` price but no alias, known cost is `$0`, unpriced usage is 25,607 tokens, and pricing is incomplete.
- Insert `codex-auto-review -> gpt-5.5` into `model_aliases`. Without reingestion or rewriting usage, cost becomes exactly `$0.098976`, unpriced usage becomes zero, and pricing becomes complete.
- The observed model ID remains `codex-auto-review` in trace data after the alias resolves pricing.

## Automated Tests

The backend suite should load expected values from the manifest rather than duplicating numbers in Rust.

1. Ingest every fixture JSONL with zero parse errors.
2. Ingest each case into a fresh SQLite database and assert the manifest counts and sums.
3. Reingest the replay corpus and checkpoint/rewrite scenarios; assert no table count, token sum, tool count, or message count changes.
4. Reverse root/child filename sort order; compare a deterministic database projection dump.
5. Move a fixture from active to archived between runs. Its rollout and facts remain stable and are not duplicated.
6. Copy a file to the other root while both paths exist. Physical rollout identity still yields one set of native facts.
7. End a file with a partial final line. The partial record is held, then ingested once after completion.
8. Insert one malformed complete line between valid lines in a temporary fixture. The failure is reported, later valid records are retained, and restart behavior is deterministic.
9. Retain the rich fixture's unknown event payload without changing user-visible totals.
10. Reset cumulative usage and switch models between two native `last_token_usage` records. Each delta keeps its own model and is charged once.
11. Repeat identical message text legitimately on two source lines. Both messages survive because identity is source provenance, not content.
12. Repeat a tool record with the same `(rollout_id, call_id)`. It remains one invocation; a distinct call ID remains distinct even with identical arguments.
13. Query totals before and after adding a model price or alias. The historical result changes immediately without running ingestion.
14. Send multiple user-role steering and subagent-notification messages with the same explicit native turn ID. They and adjacent reasoning remain on one completed turn, with no synthetic legacy turn.
15. Send several metadata-free feedback messages while a native turn is running, followed by one combined assistant response and native completion. They remain on the native turn; a metadata-free message after a terminal turn still opens a synthetic turn.
16. Repeat lifecycle states for one tool call ID. Storage preserves the source event positions while Activity presents one final metadata-only tool lifecycle.
17. Emit a provisional final answer inside an explicit native lifecycle, then send metadata-free feedback before the native completion. The final answer does not prematurely close the turn, and no overlapping synthetic turn is created.
18. Place `AGENTS.md` and environment transport envelopes before `task_started`, then the human prompt before `turn_context`. The envelopes are filtered and the prompt remains on the native turn.
19. Start a different native task while the prior one is still open. The prior task becomes interrupted and only the new task remains running.
20. Project tool identity, namespace, status, timing, lineage, and usage attribution without storing tool arguments, results, raw payloads, generated images, or attachment bytes.
21. Keep authored and captured text in `messages.content`; represent an image-only message with a small payload-free omission marker so message counts and chronology remain intact.
22. Load the latest title per thread from Codex's append-only `session_index.jsonl`, choosing timestamp before file order, ignoring a malformed trailing record, and applying a later rename even when every rollout file is unchanged.

## Browser Assertions

The data contract is deep; the product presentation stays calm.

- Overview and Stats show corrected totals without exposing ingestion internals.
- Sessions includes zero-usage, message-only, usage-only, active, aborted, legacy, and unknown-price sessions. Replay-only physical files never become rows.
- Session Summary shows model/effort mix, session totals, tool counts, agent counts, outcome, and status.
- Activity starts collapsed at the turn level. Expanding the rich turn reveals updates, reasoning summaries, metadata-only tool lifecycles, the Russell branch, goal change, compaction boundary, and final answer in order.
- The session ID is a `codex://threads/{session_id}` link to the full-fidelity Codex session; the dashboard does not expose tool or attachment payloads.
- Multiple user feedback messages after a provisional final answer remain in the same explicit native task, transport context stays hidden, and repeated tool lifecycle states render once rather than as duplicate actions.
- Usage attributes facts to root versus child agent and preserves the raw observed model ID.
- The unknown-price state is restrained in Overview/Sessions. Settings is dedicated to price data, including unresolved observed IDs and alias actions.
- Adding the alias updates Overview, Sessions, Summary, Usage, and Stats on refresh without rebuilding data.

## Live-Corpus Gate

Do not replace the real corpus with a synthetic benchmark. Before handoff, run
a clean ingest against the configured active and archived Codex roots.

The full July thread has seven rollouts with native work: the root, four large children, and two small guardian children. Applying cumulative-counter deltas after each native boundary yields:

```text
input                 59,017,056
cached input          56,506,112
output                   212,618
reasoning output         100,728
total                  59,229,674
```

At the configured live prices that thread costs `$47.186316`. Thirteen repeated post-boundary telemetry snapshots have identical cumulative counters and therefore add no second charge; blindly summing their `last_token_usage` values inflates the result. The two pure archives contribute zero. The whole 09:00 bucket can be higher because other legitimate sessions share the hour, but it must not contain the former 19.4-billion-token replay artifact. The May parent remains a May session, and all child branches must be inspectable under the July root.

Finally, pick at least one real instance of every fixture family in the running browser and compare the API rows to the source JSONL IDs and counters above. Completion requires both correct storage and faithful UI, not one without the other.

Record live projection counts, integrity checks, schema version, storage size,
and performance measurements in the current verification report rather than
freezing an environment-specific snapshot in this contract.
