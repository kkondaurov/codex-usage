# Stage 0 Baseline Evidence

The architectural refactor is measured against annotated tag `v0.1.0`
(`1911d4aa33826bd467749a96a24ed36b41b865cb`). This file records the external
test inventory before production modules move; it is not user-facing project
documentation.

## Rust test inventory at `v0.1.0`

The tagged tree contains **318 Rust tests**: 241 library unit tests and 77
integration tests. The normal test run reports 315 passed and 3 ignored
release-scale performance gates.

| Suite | Tests |
|---|---:|
| `src/activity_index.rs` | 6 |
| `src/api.rs` | 56 |
| `src/config.rs` | 8 |
| `src/db.rs` | 18 |
| `src/db_executor.rs` | 3 |
| `src/fixed_price.rs` | 3 |
| `src/ingest.rs` | 97 |
| `src/manual_pricing.rs` | 21 |
| `src/money.rs` | 2 |
| `src/pricing.rs` | 21 |
| `src/process_lock.rs` | 4 |
| `src/redaction.rs` | 2 |
| `tests/api_contract.rs` | 26 |
| `tests/corpus_acceptance.rs` | 13 |
| `tests/ingest_edge_contracts.rs` | 20 |
| `tests/ingest_truthfulness.rs` | 6 |
| `tests/local_bind_contract.rs` | 1 |
| `tests/runtime_contract.rs` | 11 |

Inventory command:

```sh
git grep -n -E '^[[:space:]]*#\[(tokio::)?test' v0.1.0 -- 'src/*.rs' 'tests/*.rs'
```

## Stage 0 additions owned by runtime/architecture protection

- `tests/runtime_contract.rs`: HTTP control-lane wiring and concurrent-WAL
  session-summary snapshot consistency.
- `tests/architecture_contract.rs`: target-module dependency boundaries that
  activate as the new module directories appear.

Use `cargo test --all-features -- --list` after each extraction to keep the
inventory visible. Test count is evidence that tests were not silently lost,
not a substitute for the behavioral oracle and gate results in the main plan.

## Cold browser performance at Stage 0

The five-sample disposable scale-data gate passed. Median cold render times
were 225 ms for the Overview summary, 841 ms for the Overview heatmap/top
lists, 220-223 ms for Stats day/week/month, 829 ms for Stats year, 319 ms for
Stats all, 201 ms for Overview-to-Stats navigation, and 227 ms for Sessions
sorted by cost. Every render sample stayed below the 1,000 ms product target;
the slowest was 914 ms. The corresponding slowest API sample was 638 ms.

The three release-scale Activity gates also passed: the 500k combined paths
were 369-442 ms at median depending on path, the 500k usage-heavy gate stayed
under one second, and the 100k-descendant paths completed in 786-888 ms.
