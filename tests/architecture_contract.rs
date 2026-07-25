use std::{
    fs,
    path::{Path, PathBuf},
};

const FEATURE_ROOTS: &[&str] = &["system", "pricing", "sessions", "activity", "analytics"];
const COSTING_FORBIDDEN_DEPENDENCIES: &[&str] = &[
    "usage",
    "storage",
    "web",
    "system",
    "pricing",
    "sessions",
    "activity",
    "analytics",
];
const USAGE_FORBIDDEN_DEPENDENCIES: &[&str] = &[
    "web",
    "system",
    "pricing",
    "sessions",
    "activity",
    "analytics",
];
const WEB_FORBIDDEN_DEPENDENCIES: &[&str] = &[
    "api",
    "ingest",
    "ingestion",
    "system",
    "pricing",
    "sessions",
    "activity",
    "analytics",
];
const TARGET_ROOTS: &[&str] = &[
    "web",
    "calendar",
    "conversation",
    "storage",
    "costing",
    "usage",
    "system",
    "pricing",
    "sessions",
    "activity",
    "analytics",
    "ingest",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    ModuleRoot,
    Composition,
    Transport,
    Runtime,
    Persistence,
    Domain,
    Regression,
    IngestSource,
    IngestProtocol,
    IngestCheckpoints,
    IngestCheckpointStore,
    IngestCatalog,
    IngestAttempt,
    IngestOwnerReader,
    IngestFileIngestor,
    IngestCoordinator,
    IngestScanner,
    IngestProjectionConnection,
    IngestProjection,
    IngestOrchestration,
}

// This is the architecture manifest. Every target-module file must be assigned
// one role here. Unknown files fail instead of quietly inheriting a rule from a
// convenient filename.
const ROLE_ASSIGNMENTS: &[(&str, Role)] = &[
    ("src/app.rs", Role::Composition),
    ("src/web/mod.rs", Role::ModuleRoot),
    ("src/web/server.rs", Role::Transport),
    ("src/web/boundary.rs", Role::Transport),
    ("src/web/error.rs", Role::Transport),
    ("src/web/pagination.rs", Role::Transport),
    ("src/web/read_runtime.rs", Role::Runtime),
    ("src/calendar.rs", Role::Domain),
    ("src/conversation/mod.rs", Role::ModuleRoot),
    ("src/conversation/display.rs", Role::Domain),
    ("src/storage/mod.rs", Role::ModuleRoot),
    ("src/storage/database.rs", Role::Persistence),
    ("src/storage/migrations.rs", Role::Persistence),
    ("src/storage/executor.rs", Role::Runtime),
    ("src/storage/locks.rs", Role::Persistence),
    ("src/costing/mod.rs", Role::ModuleRoot),
    ("src/costing/amount.rs", Role::Domain),
    ("src/costing/rate.rs", Role::Domain),
    ("src/costing/price_book.rs", Role::Domain),
    ("src/usage/mod.rs", Role::ModuleRoot),
    ("src/usage/totals.rs", Role::Domain),
    ("src/usage/reader.rs", Role::Persistence),
    ("src/usage/rollups.rs", Role::Persistence),
    ("src/system/mod.rs", Role::ModuleRoot),
    ("src/system/routes.rs", Role::Transport),
    ("src/system/status.rs", Role::Persistence),
    ("src/system/settings.rs", Role::Persistence),
    ("src/pricing/mod.rs", Role::ModuleRoot),
    ("src/pricing/routes.rs", Role::Transport),
    ("src/pricing/catalog.rs", Role::Persistence),
    ("src/pricing/mutations.rs", Role::Runtime),
    ("src/pricing/manual_store.rs", Role::Persistence),
    ("src/pricing/sync.rs", Role::Runtime),
    ("src/sessions/mod.rs", Role::ModuleRoot),
    ("src/sessions/routes.rs", Role::Transport),
    ("src/sessions/catalog.rs", Role::Persistence),
    ("src/sessions/list.rs", Role::Persistence),
    ("src/sessions/summary.rs", Role::Persistence),
    ("src/activity/mod.rs", Role::ModuleRoot),
    ("src/activity/cursor.rs", Role::Domain),
    ("src/activity/model.rs", Role::Domain),
    ("src/activity/routes.rs", Role::Transport),
    ("src/activity/regression.rs", Role::Regression),
    (
        "src/activity/regression/attribution_scale.rs",
        Role::Regression,
    ),
    ("src/activity/regression/day_queries.rs", Role::Regression),
    (
        "src/activity/regression/pagination_previews.rs",
        Role::Regression,
    ),
    ("src/activity/root_page.rs", Role::Persistence),
    ("src/activity/selection.rs", Role::Persistence),
    ("src/activity/detail.rs", Role::Persistence),
    ("src/activity/groups.rs", Role::Persistence),
    ("src/activity/attribution.rs", Role::Persistence),
    ("src/activity/previews.rs", Role::Persistence),
    ("src/activity/index.rs", Role::Persistence),
    ("src/analytics/mod.rs", Role::ModuleRoot),
    ("src/analytics/routes.rs", Role::Transport),
    ("src/analytics/prewarm.rs", Role::Runtime),
    ("src/analytics/overview/mod.rs", Role::Domain),
    ("src/analytics/overview/read.rs", Role::Persistence),
    ("src/analytics/stats/mod.rs", Role::Domain),
    ("src/analytics/stats/read.rs", Role::Persistence),
    ("src/ingest/mod.rs", Role::ModuleRoot),
    ("src/ingest/tests.rs", Role::Regression),
    ("src/ingest/tests/archive_handoff.rs", Role::Regression),
    ("src/ingest/tests/attempts.rs", Role::Regression),
    ("src/ingest/tests/conversation.rs", Role::Regression),
    ("src/ingest/tests/file_projection.rs", Role::Regression),
    ("src/ingest/tests/lifecycle.rs", Role::Regression),
    ("src/ingest/tests/orchestration.rs", Role::Regression),
    ("src/ingest/tests/reconciliation.rs", Role::Regression),
    ("src/ingest/tests/session_identity.rs", Role::Regression),
    ("src/ingest/tests/support.rs", Role::Regression),
    ("src/ingest/tests/usage_accounting.rs", Role::Regression),
    ("src/ingest/protocol/mod.rs", Role::IngestProtocol),
    ("src/ingest/protocol/content.rs", Role::IngestProtocol),
    ("src/ingest/protocol/decode/mod.rs", Role::IngestProtocol),
    ("src/ingest/protocol/decode/agents.rs", Role::IngestProtocol),
    (
        "src/ingest/protocol/decode/conversation.rs",
        Role::IngestProtocol,
    ),
    (
        "src/ingest/protocol/decode/lifecycle.rs",
        Role::IngestProtocol,
    ),
    (
        "src/ingest/protocol/decode/metadata.rs",
        Role::IngestProtocol,
    ),
    (
        "src/ingest/protocol/decode/ordinary.rs",
        Role::IngestProtocol,
    ),
    ("src/ingest/protocol/decode/record.rs", Role::IngestProtocol),
    (
        "src/ingest/protocol/decode/thread_state.rs",
        Role::IngestProtocol,
    ),
    ("src/ingest/protocol/decode/tools.rs", Role::IngestProtocol),
    ("src/ingest/protocol/decode/usage.rs", Role::IngestProtocol),
    ("src/ingest/protocol/duration.rs", Role::IngestProtocol),
    ("src/ingest/protocol/event.rs", Role::IngestProtocol),
    ("src/ingest/protocol/identifiers.rs", Role::IngestProtocol),
    ("src/ingest/protocol/intent.rs", Role::IngestProtocol),
    ("src/ingest/protocol/metadata.rs", Role::IngestProtocol),
    ("src/ingest/protocol/state.rs", Role::IngestProtocol),
    ("src/ingest/protocol/timestamp.rs", Role::IngestProtocol),
    ("src/ingest/protocol/tokens.rs", Role::IngestProtocol),
    ("src/ingest/protocol/wire.rs", Role::IngestProtocol),
    ("src/ingest/projection/mod.rs", Role::IngestProjection),
    ("src/ingest/projection/agents.rs", Role::IngestProjection),
    (
        "src/ingest/projection/checkpoint.rs",
        Role::IngestProjection,
    ),
    (
        "src/ingest/projection/connection.rs",
        Role::IngestProjectionConnection,
    ),
    (
        "src/ingest/projection/conversation.rs",
        Role::IngestProjection,
    ),
    ("src/ingest/projection/metadata.rs", Role::IngestProjection),
    ("src/ingest/projection/events.rs", Role::IngestProjection),
    ("src/ingest/projection/lifecycle.rs", Role::IngestProjection),
    ("src/ingest/projection/ordinary.rs", Role::IngestProjection),
    ("src/ingest/projection/record.rs", Role::IngestProjection),
    ("src/ingest/projection/removal.rs", Role::IngestProjection),
    (
        "src/ingest/projection/thread_state.rs",
        Role::IngestProjection,
    ),
    (
        "src/ingest/projection/topology.rs",
        Role::IngestProjectionConnection,
    ),
    ("src/ingest/projection/tools.rs", Role::IngestProjection),
    ("src/ingest/projection/usage.rs", Role::IngestProjection),
    ("src/ingest/source.rs", Role::IngestSource),
    ("src/ingest/checkpoints.rs", Role::IngestCheckpoints),
    (
        "src/ingest/checkpoint_store.rs",
        Role::IngestCheckpointStore,
    ),
    ("src/ingest/catalog.rs", Role::IngestCatalog),
    ("src/ingest/attempt.rs", Role::IngestAttempt),
    ("src/ingest/owner_reader.rs", Role::IngestOwnerReader),
    ("src/ingest/file_ingestor.rs", Role::IngestFileIngestor),
    ("src/ingest/coordinator.rs", Role::IngestCoordinator),
    ("src/ingest/scanner.rs", Role::IngestScanner),
    ("src/ingest/reconciliation.rs", Role::IngestOrchestration),
    ("src/ingest/session_titles.rs", Role::IngestOrchestration),
];

fn manifest_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if !root.is_dir() {
        return files;
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let mut entries = fs::read_dir(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|error| panic!("failed to enumerate {}: {error}", path.display()));
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn relative(path: &Path) -> String {
    path.strip_prefix(manifest_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn source(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}

fn ingest_composition_source(src: &Path) -> String {
    source(&src.join("ingest/mod.rs"))
}

fn forbidden_hits(contents: &str, forbidden: &[&str]) -> Vec<String> {
    forbidden
        .iter()
        .filter(|needle| contents.contains(**needle))
        .map(|needle| (*needle).to_owned())
        .collect()
}

fn forbidden_sql_table_hits(contents: &str, tables: &[&str]) -> Vec<String> {
    tables
        .iter()
        .filter(|table| {
            ["FROM", "JOIN", "INTO", "UPDATE", "DELETE FROM"]
                .iter()
                .any(|operation| contents.contains(&format!("{operation} {table}")))
        })
        .map(|table| (*table).to_owned())
        .collect()
}

fn production_module_source(contents: &str) -> &str {
    if contents.trim_start().starts_with("#![cfg(test)]") {
        return &contents[..0];
    }
    contents
        .split_once("#[cfg(test)]\nmod tests {")
        .map_or(contents, |(production, _)| production)
}

fn role_violations(role: Role, contents: &str) -> Vec<String> {
    const SQL_OWNERSHIP: &[&str] = &[
        "rusqlite",
        ".prepare(",
        ".query_row(",
        ".execute(",
        "SELECT ",
        "INSERT INTO ",
        "UPDATE ",
        "DELETE FROM ",
    ];
    const HTTP: &[&str] = &["axum::", "use axum"];
    let contents = production_module_source(contents);
    match role {
        Role::Transport => forbidden_hits(contents, SQL_OWNERSHIP),
        Role::Persistence | Role::Runtime => forbidden_hits(contents, HTTP),
        Role::Domain | Role::ModuleRoot => {
            let mut forbidden = SQL_OWNERSHIP.to_vec();
            forbidden.extend_from_slice(HTTP);
            forbidden_hits(contents, &forbidden)
        }
        Role::Composition => forbidden_hits(contents, SQL_OWNERSHIP),
        Role::Regression => {
            if contents.trim().is_empty() {
                Vec::new()
            } else {
                vec!["production code in a regression-only module".into()]
            }
        }
        Role::IngestSource => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    "projection::",
                    "ProjectionContext",
                    "ProjectionTx",
                ],
            );
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "storage",
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "checkpoints",
                    "checkpoint_store",
                    "catalog",
                    "projection",
                    "coordinator",
                    "reconciliation",
                ],
            ));
            violations
        }
        Role::IngestProtocol => {
            let mut forbidden = SQL_OWNERSHIP.to_vec();
            forbidden.extend_from_slice(HTTP);
            forbidden.extend_from_slice(&[
                "std::fs",
                "tokio::fs",
                "std::io",
                "tokio::io",
                "PathBuf",
                "File::open",
                "SourceSnapshot",
                "source::",
                "projection::",
                "ProjectionContext",
                "ProjectionTx",
            ]);
            forbidden_hits(contents, &forbidden)
        }
        Role::IngestCheckpoints => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                &sanitized,
                &[
                    "rusqlite",
                    "axum::",
                    "use axum",
                    ".prepare(",
                    ".query_row(",
                    ".execute(",
                    "std::fs",
                    "tokio::fs",
                    "std::io",
                    "tokio::io",
                    "std::path",
                    "File::open",
                    "ProjectionContext",
                    "ProjectionTx",
                ],
            );
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "storage",
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "File",
                    "OpenOptions",
                    "Path",
                    "PathBuf",
                    "Metadata",
                    "Connection",
                    "Transaction",
                    "Db",
                    "DatabaseLock",
                    "checkpoint_store",
                    "projection",
                    "catalog",
                    "coordinator",
                    "reconciliation",
                    "process_file",
                    "scan_once",
                ],
            ));
            violations
        }
        Role::IngestCheckpointStore => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    "std::fs",
                    "tokio::fs",
                    "std::io",
                    "tokio::io",
                    "File::open",
                    "ProjectionContext",
                    "ProjectionTx",
                    ".transaction(",
                    ".transaction_with_behavior(",
                ],
            );
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "File",
                    "OpenOptions",
                    "Path",
                    "Metadata",
                    "WalkDir",
                    "SourceSnapshot",
                    "BoundedLine",
                    "CapturedJsonlReader",
                    "DatabaseLock",
                    "Transaction",
                    "TransactionBehavior",
                    "projection",
                    "coordinator",
                    "reconciliation",
                    "process_file",
                    "scan_once",
                    "collect_jsonl",
                    "project_record",
                    "mark_file_unchanged",
                ],
            ));
            violations.extend(forbidden_hits(
                contents,
                &[
                    "INSERT INTO source_files",
                    "UPDATE source_files",
                    "DELETE FROM source_files",
                ],
            ));
            violations.extend(forbidden_sql_table_hits(
                contents,
                &[
                    "threads",
                    "rollouts",
                    "events",
                    "messages",
                    "turns",
                    "tool_calls",
                    "usage_facts",
                    "agent_runs",
                    "usage_activity_rollups",
                ],
            ));
            violations
        }
        Role::IngestCatalog => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(contents, SQL_OWNERSHIP);
            violations.extend(forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    "tracing",
                    "chrono",
                    "serde",
                    "sha2",
                    "tokio::",
                    ".connect(",
                    ".prepare(",
                    ".query_row(",
                    ".execute(",
                    ".transaction(",
                    ".transaction_with_behavior(",
                    "source::",
                    "checkpoints::",
                    "checkpoint_store::",
                    "projection::",
                    "coordinator::",
                    "reconciliation::",
                    "ProjectionContext",
                    "ProjectionTx",
                    "std::thread",
                ],
            ));
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "storage",
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "costing",
                    "usage",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "Db",
                    "DatabaseLock",
                    "Connection",
                    "Transaction",
                    "TransactionBehavior",
                    "SourceSnapshot",
                    "ChunkedFingerprint",
                    "SourceCheckpoint",
                    "CheckpointStore",
                    "checkpoint_store",
                    "projection",
                    "coordinator",
                    "reconciliation",
                    "process_file",
                    "scan_once",
                    "scan_once_locked",
                    "scan_once_started",
                    "set_meta",
                    "finish_scan_meta",
                    "reconcile_missing",
                    "ScannerHandle",
                    "IngestScannerLease",
                    "Arc",
                    "AtomicBool",
                    "JoinHandle",
                ],
            ));
            violations
        }
        Role::IngestAttempt => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    "std::fs",
                    "tokio::fs",
                    "std::io",
                    "tokio::io",
                    "std::path",
                    "File::open",
                    "std::thread",
                    "tokio::task",
                    "ProjectionContext",
                    "ProjectionTx",
                    "projection::",
                    "canonical_utc_timestamp",
                    "serde_json",
                    "chrono",
                ],
            );
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "costing",
                    "usage",
                    "calendar",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "File",
                    "OpenOptions",
                    "Path",
                    "PathBuf",
                    "WalkDir",
                    "SourceSnapshot",
                    "BoundedLine",
                    "ScannerHandle",
                    "IngestScannerLease",
                    "DatabaseLock",
                    "Arc",
                    "AtomicBool",
                    "JoinHandle",
                    "ScanReport",
                    "Utc",
                ],
            ));
            violations.extend(forbidden_sql_table_hits(
                contents,
                &[
                    "activity_event_index",
                    "rollouts",
                    "events",
                    "messages",
                    "turns",
                    "tool_calls",
                    "usage_facts",
                    "agent_runs",
                    "usage_activity_rollups",
                    "usage_global_totals",
                    "model_prices",
                    "model_aliases",
                    "schema_migrations",
                ],
            ));
            violations.extend(forbidden_hits(
                contents,
                &[
                    "INSERT INTO source_files",
                    "UPDATE source_files",
                    "DELETE FROM source_files",
                    "INSERT INTO threads",
                    "UPDATE threads",
                    "DELETE FROM threads",
                ],
            ));
            violations
        }
        Role::IngestOwnerReader => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(contents, SQL_OWNERSHIP);
            violations.extend(forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    ".connect(",
                    ".transaction(",
                    ".transaction_with_behavior(",
                    "projection::",
                    "ProjectionConnection",
                    "ProjectionContext",
                    "ProjectionTx",
                ],
            ));
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "storage",
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "costing",
                    "usage",
                    "calendar",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "Db",
                    "DatabaseLock",
                    "Connection",
                    "Transaction",
                    "TransactionBehavior",
                    "ProjectionConnection",
                    "ProjectionContext",
                    "ProjectionTx",
                    "CheckpointStore",
                    "ScannerHandle",
                    "IngestScannerLease",
                ],
            ));
            violations
        }
        Role::IngestFileIngestor => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                contents,
                &[
                    "SELECT ",
                    "INSERT INTO ",
                    "UPDATE ",
                    "DELETE FROM ",
                    "CREATE ",
                    "DROP ",
                    "ALTER ",
                    "PRAGMA ",
                    "REPLACE INTO ",
                    "VACUUM ",
                ],
            );
            violations.extend(forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    ".prepare(",
                    ".query_row(",
                    ".execute(",
                    ".execute_batch(",
                    "std::thread",
                    "tokio::",
                ],
            ));
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "costing",
                    "usage",
                    "calendar",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "DatabaseLock",
                    "AttemptRecorder",
                    "IngestRoots",
                    "ScanReport",
                    "ScanOutcome",
                    "ScannerHandle",
                    "IngestScannerLease",
                    "AtomicBool",
                    "JoinHandle",
                ],
            ));
            if token_hits(&sanitized, &["thread"])
                .iter()
                .any(|hit| hit == "thread")
            {
                violations.push("std::thread".to_owned());
            }
            violations
        }
        Role::IngestCoordinator => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                contents,
                &[
                    "rusqlite",
                    "SELECT ",
                    "INSERT INTO ",
                    "UPDATE ",
                    "DELETE FROM ",
                    "CREATE ",
                    "DROP ",
                    "ALTER ",
                    "PRAGMA ",
                    "REPLACE INTO ",
                    "VACUUM ",
                ],
            );
            violations.extend(forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    ".prepare(",
                    ".query_row(",
                    ".execute(",
                    ".execute_batch(",
                    "std::io",
                    "tokio::",
                    "std::thread",
                    "thread::spawn",
                ],
            ));
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "costing",
                    "usage",
                    "conversation",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "ProjectionTx",
                    "BoundedLine",
                    "CapturedJsonlReader",
                    "SourceSnapshot",
                    "ScannerHandle",
                    "JoinHandle",
                    "Arc",
                    "Ordering",
                    "File",
                    "OpenOptions",
                    "Value",
                ],
            ));
            violations
        }
        Role::IngestScanner => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                contents,
                &[
                    "rusqlite",
                    "SELECT ",
                    "INSERT INTO ",
                    "UPDATE ",
                    "DELETE FROM ",
                    "CREATE ",
                    "DROP ",
                    "ALTER ",
                    "PRAGMA ",
                    "REPLACE INTO ",
                    "VACUUM ",
                ],
            );
            violations.extend(forbidden_hits(
                &sanitized,
                &[
                    "axum::",
                    "use axum",
                    "tokio::",
                    "std::fs",
                    "tokio::fs",
                    "std::io",
                    "tokio::io",
                    ".connect(",
                    ".prepare(",
                    ".query_row(",
                    ".execute(",
                    ".execute_batch(",
                    "projection::",
                    "catalog::",
                    "reconciliation::",
                    "protocol::",
                    "source::",
                    "checkpoint_store::",
                    "checkpoints::",
                    "owner_reader::",
                    "file_ingestor::",
                    "canonicalize_storage_path",
                    "collect_jsonl",
                    "read_owner",
                    "reconcile_missing",
                ],
            ));
            violations.extend(dependency_direction_violations(
                &sanitized,
                &[
                    "web",
                    "system",
                    "pricing",
                    "sessions",
                    "activity",
                    "analytics",
                    "costing",
                    "usage",
                    "calendar",
                    "conversation",
                ],
            ));
            violations.extend(token_hits(
                &sanitized,
                &[
                    "Connection",
                    "Transaction",
                    "TransactionBehavior",
                    "ProjectionConnection",
                    "ProjectionContext",
                    "ProjectionTx",
                    "ReconciliationCandidate",
                    "SourceSnapshot",
                    "BoundedLine",
                    "CapturedJsonlReader",
                    "OwnerMeta",
                    "CursorState",
                    "SourceCandidate",
                    "FileIngestor",
                    "FileReport",
                    "DatabaseLock",
                    "WalkDir",
                    "File",
                    "OpenOptions",
                    "Path",
                    "PathBuf",
                    "ScanReport",
                    "ScanOutcome",
                ],
            ));
            violations
        }
        Role::IngestProjectionConnection => forbidden_hits(
            contents,
            &["std::fs", "tokio::fs", "source::", "SourceSnapshot"],
        ),
        Role::IngestProjection => {
            let sanitized = code_without_comments_and_literals(contents);
            let mut violations = forbidden_hits(
                &sanitized,
                &[
                    "std::fs",
                    "tokio::fs",
                    "source::",
                    "SourceSnapshot",
                    ".transaction(",
                    ".transaction_with_behavior(",
                ],
            );
            violations.extend(token_hits(
                &sanitized,
                &["Connection", "Transaction", "TransactionBehavior"],
            ));
            violations
        }
        Role::IngestOrchestration => forbidden_hits(contents, SQL_OWNERSHIP),
    }
}

fn target_files() -> Vec<PathBuf> {
    let src = manifest_root().join("src");
    let mut files = TARGET_ROOTS
        .iter()
        .flat_map(|root| rust_files(&src.join(root)))
        .collect::<Vec<_>>();
    let app = src.join("app.rs");
    if app.is_file() {
        files.push(app);
    }
    for root in TARGET_ROOTS {
        let module = src.join(format!("{root}.rs"));
        if module.is_file() && !src.join(root).is_dir() {
            files.push(module);
        }
    }
    files.sort();
    files.dedup();
    files
}

fn mask_range(bytes: &mut [u8], start: usize, end: usize) {
    for byte in &mut bytes[start..end] {
        if !matches!(*byte, b'\n' | b'\r') {
            *byte = b' ';
        }
    }
}

fn raw_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        return None;
    }

    let mut cursor = match bytes.get(start..) {
        Some([b'r', ..]) => start + 1,
        Some([b'b' | b'c', b'r', ..]) => start + 2,
        _ => return None,
    };
    let hash_start = cursor;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    let hash_count = cursor - hash_start;
    if bytes.get(cursor) != Some(&b'"') {
        return None;
    }
    cursor += 1;

    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && bytes
                .get(cursor + 1..cursor + 1 + hash_count)
                .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'))
        {
            return Some(cursor + 1 + hash_count);
        }
        cursor += 1;
    }
    Some(bytes.len())
}

fn code_without_comments_and_literals(contents: &str) -> String {
    let source = contents.as_bytes();
    let mut masked = source.to_vec();
    let mut cursor = 0;

    while cursor < source.len() {
        if source.get(cursor..cursor + 2) == Some(b"//") {
            let end = source[cursor + 2..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(source.len(), |offset| cursor + 2 + offset);
            mask_range(&mut masked, cursor, end);
            cursor = end;
            continue;
        }

        if source.get(cursor..cursor + 2) == Some(b"/*") {
            let start = cursor;
            cursor += 2;
            let mut depth = 1_u32;
            while cursor < source.len() && depth > 0 {
                if source.get(cursor..cursor + 2) == Some(b"/*") {
                    depth += 1;
                    cursor += 2;
                } else if source.get(cursor..cursor + 2) == Some(b"*/") {
                    depth -= 1;
                    cursor += 2;
                } else {
                    cursor += 1;
                }
            }
            mask_range(&mut masked, start, cursor);
            continue;
        }

        if let Some(end) = raw_string_end(source, cursor) {
            mask_range(&mut masked, cursor, end);
            cursor = end;
            continue;
        }

        if source[cursor] == b'"' {
            let start = cursor;
            cursor += 1;
            while cursor < source.len() {
                match source[cursor] {
                    b'\\' => cursor = (cursor + 2).min(source.len()),
                    b'"' => {
                        cursor += 1;
                        break;
                    }
                    _ => cursor += 1,
                }
            }
            mask_range(&mut masked, start, cursor);
            continue;
        }

        cursor += 1;
    }

    String::from_utf8(masked).expect("masking Rust source must preserve UTF-8")
}

fn dependency_tokens(contents: &str) -> Vec<String> {
    let code = code_without_comments_and_literals(contents);
    let bytes = code.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        if bytes[cursor] == b'r'
            && bytes.get(cursor + 1) == Some(&b'#')
            && bytes
                .get(cursor + 2)
                .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
        {
            cursor += 2;
        }

        if bytes[cursor].is_ascii_alphabetic() || bytes[cursor] == b'_' {
            let start = cursor;
            cursor += 1;
            while bytes
                .get(cursor)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                cursor += 1;
            }
            tokens.push(code[start..cursor].to_owned());
            continue;
        }

        if bytes.get(cursor..cursor + 2) == Some(b"::") {
            tokens.push("::".to_owned());
            cursor += 2;
            continue;
        }

        if matches!(bytes[cursor], b'{' | b'}' | b',') {
            tokens.push(char::from(bytes[cursor]).to_string());
        }
        cursor += 1;
    }
    tokens
}

fn token_hits(contents: &str, forbidden: &[&str]) -> Vec<String> {
    let tokens = dependency_tokens(contents);
    forbidden
        .iter()
        .filter(|needle| tokens.iter().any(|token| token == *needle))
        .map(|needle| (*needle).to_owned())
        .collect()
}

fn crate_dependency_roots(contents: &str) -> Vec<String> {
    let tokens = dependency_tokens(contents);
    let mut roots = Vec::new();
    let mut cursor = 0;

    while cursor + 2 < tokens.len() {
        if tokens[cursor] != "crate" || tokens[cursor + 1] != "::" {
            cursor += 1;
            continue;
        }

        if tokens[cursor + 2] != "{" {
            roots.push(tokens[cursor + 2].clone());
            cursor += 3;
            continue;
        }

        let mut group_cursor = cursor + 3;
        let mut depth = 1_u32;
        let mut expect_root = true;
        while group_cursor < tokens.len() && depth > 0 {
            match tokens[group_cursor].as_str() {
                "{" => depth += 1,
                "}" => depth -= 1,
                "," if depth == 1 => expect_root = true,
                token if depth == 1 && expect_root => {
                    if !matches!(token, "self" | "super" | "crate") {
                        roots.push(token.to_owned());
                    }
                    expect_root = false;
                }
                _ => {}
            }
            group_cursor += 1;
        }
        cursor = group_cursor;
    }

    roots.sort();
    roots.dedup();
    roots
}

fn dependency_direction_violations(contents: &str, forbidden: &[&str]) -> Vec<String> {
    let roots = crate_dependency_roots(contents);
    forbidden
        .iter()
        .filter(|root| roots.iter().any(|candidate| candidate == *root))
        .map(|root| (*root).to_owned())
        .collect()
}

fn imports_feature(contents: &str, feature: &str) -> bool {
    crate_dependency_roots(contents)
        .iter()
        .any(|root| root == feature)
}

fn assigned_role(path: &Path) -> Option<Role> {
    let relative = relative(path);
    let roles = ROLE_ASSIGNMENTS
        .iter()
        .filter_map(|(candidate, role)| (*candidate == relative).then_some(*role))
        .collect::<Vec<_>>();
    assert!(
        roles.len() <= 1,
        "{relative} has multiple architecture roles: {roles:?}"
    );
    roles.first().copied()
}

#[test]
fn every_target_module_has_one_explicit_role_and_obeys_it() {
    for path in target_files() {
        let role = assigned_role(&path).unwrap_or_else(|| {
            panic!(
                "{} has no architecture role; register the boundary intentionally in ROLE_ASSIGNMENTS",
                relative(&path)
            )
        });
        let violations = role_violations(role, &source(&path));
        assert!(
            violations.is_empty(),
            "{} violates its {role:?} boundary via: {}",
            relative(&path),
            violations.join(", ")
        );
    }
}

#[test]
fn architecture_policy_detects_representative_boundary_regressions() {
    assert_eq!(
        role_violations(
            Role::Transport,
            "use rusqlite::Connection; const SQL: &str = \"SELECT * FROM threads\";"
        ),
        ["rusqlite", "SELECT "]
    );
    assert_eq!(
        role_violations(Role::Persistence, "use axum::Json;"),
        ["axum::", "use axum"]
    );
    assert!(
        role_violations(
            Role::Runtime,
            "fn refresh() {}\n#[cfg(test)]\nmod tests { use axum::Router; }"
        )
        .is_empty(),
        "test-only fixture dependencies do not change production ownership"
    );
    assert_eq!(
        role_violations(
            Role::IngestProtocol,
            "use axum::Json; use std::fs; use std::io; use rusqlite::Connection; struct ProjectionTx;"
        ),
        [
            "rusqlite",
            "axum::",
            "use axum",
            "std::fs",
            "std::io",
            "ProjectionTx"
        ]
    );
    assert_eq!(
        role_violations(
            Role::IngestSource,
            "use crate::storage::Db; use axum::Router; struct ProjectionTx;"
        ),
        ["axum::", "use axum", "ProjectionTx", "storage"]
    );
    assert!(
        role_violations(
            Role::IngestSource,
            r#"// use crate::storage::Db;
               const NOTE: &str = "use axum::Router; struct ProjectionTx;";"#,
        )
        .is_empty(),
        "comments and string fixtures do not change ingestion-source ownership"
    );
    assert_eq!(
        role_violations(
            Role::IngestProjection,
            "use rusqlite::{Connection, Transaction, TransactionBehavior};\nfn begin(connection: &mut Connection) { connection.transaction(); }"
        ),
        [
            ".transaction(",
            "Connection",
            "Transaction",
            "TransactionBehavior"
        ],
        "feature projections may own SQL but may not own raw transaction types or constructors"
    );

    let checkpoint_violations = role_violations(
        Role::IngestCheckpoints,
        r#"
            use crate::{
                ingest::{
                    catalog::SourceCatalog,
                    checkpoint_store::CheckpointStore,
                    coordinator::Coordinator,
                    projection::ProjectionTx,
                },
                storage::Db,
            };
            use axum::Router;
            use rusqlite::Connection;
            use std::fs::File;
            use std::path::PathBuf;

            fn scan_once() {}
        "#,
    );
    for expected in [
        "rusqlite",
        "axum::",
        "use axum",
        "ProjectionTx",
        "storage",
        "File",
        "PathBuf",
        "Connection",
        "Db",
        "checkpoint_store",
        "projection",
        "catalog",
        "coordinator",
        "scan_once",
    ] {
        assert!(
            checkpoint_violations
                .iter()
                .any(|actual| actual == expected),
            "checkpoint role failed to reject `{expected}`: {checkpoint_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestCheckpoints,
            r#"
                use crate::ingest::{
                    protocol::CursorState,
                    source::{FileIdentity, SourceSnapshot},
                };

                fn inspect(_source: &mut SourceSnapshot, _state: &CursorState) {}
            "#,
        )
        .is_empty(),
        "checkpoint policy must be allowed to depend inward on Source and Protocol"
    );
    assert!(
        role_violations(
            Role::IngestCheckpoints,
            r#"// use rusqlite::Connection;
               const NOTE: &str = "use crate::ingest::catalog::SourceCatalog;";"#,
        )
        .is_empty(),
        "comments and string fixtures do not change checkpoint ownership"
    );

    let checkpoint_store_violations = role_violations(
        Role::IngestCheckpointStore,
        r#"
            use crate::{
                ingest::{
                    catalog::SelectedSourceExtent,
                    coordinator::Coordinator,
                    projection::ProjectionTx,
                },
                storage::Db,
            };
            use axum::Router;
            use rusqlite::{Connection, Transaction};
            use std::{fs::File, path::PathBuf};

            const WRITE: &str = "UPDATE source_files SET byte_offset=0";
            const PROJECTION_WRITE: &str = "DELETE FROM rollouts";
            fn process_file() {}
        "#,
    );
    for expected in [
        "axum::",
        "use axum",
        "ProjectionTx",
        "File",
        "Transaction",
        "projection",
        "coordinator",
        "process_file",
        "UPDATE source_files",
        "rollouts",
    ] {
        assert!(
            checkpoint_store_violations
                .iter()
                .any(|actual| actual == expected),
            "checkpoint-store role failed to reject `{expected}`: {checkpoint_store_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestCheckpointStore,
            r#"
                use crate::ingest::{
                    catalog::SelectedSourceExtent,
                    checkpoints::{PendingSourceShrink, SourceCheckpoint},
                    protocol::CursorState,
                    source::FileIdentity,
                };
                use crate::storage::Db;
                use rusqlite::{Connection, OptionalExtension, params};
                use std::path::PathBuf;

                fn load_extents(db: &Db) -> SelectedSourceExtent {
                    let _connection = db.connect().unwrap();
                    SelectedSourceExtent {
                        path: PathBuf::from("source.jsonl"),
                        raw_size: 1,
                        committed_size: 1,
                        fingerprint: String::new(),
                    }
                }

                fn load_checkpoint(connection: &Connection) {
                    connection.query_row(
                        "SELECT parse_state_json FROM source_files WHERE rollout_id=?1",
                        ["owner"],
                        |_| Ok(()),
                    );
                }

                fn record_pending_shrink(connection: &Connection) {
                    connection.execute(
                        "INSERT INTO app_meta(key,value) VALUES(?1,?2)",
                        params!["pending_source_shrink:owner", "{}"],
                    );
                }
            "#,
        )
        .is_empty(),
        "checkpoint store must allow selected-extent persistence, supplied-connection checkpoint reads, and pending-shrink markers"
    );

    let catalog_violations = role_violations(
        Role::IngestCatalog,
        r#"
            use crate::{
                ingest::{
                    checkpoint_store::CheckpointStore,
                    checkpoints::ChunkedFingerprint,
                    projection::ProjectionTx,
                    source::SourceSnapshot,
                },
                storage::Db,
            };
            use axum::Router;
            use rusqlite::Connection;
            use tokio::task::JoinHandle;
            use walkdir::WalkDir;

            const QUERY: &str = "SELECT * FROM source_files";
            fn scan_once() {}
        "#,
    );
    for expected in [
        "rusqlite",
        "SELECT ",
        "axum::",
        "use axum",
        "tokio::",
        "source::",
        "checkpoints::",
        "checkpoint_store::",
        "projection::",
        "ProjectionTx",
        "storage",
        "Db",
        "Connection",
        "SourceSnapshot",
        "ChunkedFingerprint",
        "CheckpointStore",
        "checkpoint_store",
        "projection",
        "scan_once",
        "JoinHandle",
    ] {
        assert!(
            catalog_violations.iter().any(|actual| actual == expected),
            "Catalog role failed to reject `{expected}`: {catalog_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestCatalog,
            r#"
                use crate::ingest::protocol::OwnerMeta;
                use anyhow::{Context, Result};
                use std::{
                    cmp::Ordering,
                    fs::File,
                    io::Read,
                    path::{Path, PathBuf},
                };
                use walkdir::WalkDir;

                struct SourceCandidate {
                    path: PathBuf,
                    owner: OwnerMeta,
                }

                fn source_candidate_preference(
                    _left: &SourceCandidate,
                    _right: &SourceCandidate,
                ) -> Ordering {
                    Ordering::Equal
                }

                fn discover(root: &Path) -> Result<()> {
                    let _file = File::open(root).context("open discovery root")?;
                    let _walker = WalkDir::new(root);
                    Ok(())
                }
            "#,
        )
        .is_empty(),
        "Catalog must allow discovery filesystem dependencies and the inward ingestion protocol"
    );

    let attempt_violations = role_violations(
        Role::IngestAttempt,
        r#"
            use crate::{
                calendar::canonical_utc_timestamp,
                ingest::{projection::ProjectionTx, source::SourceSnapshot},
                storage::{DatabaseLock, Db},
            };
            use std::fs::File;
            use std::path::PathBuf;
            use std::sync::Arc;
            use std::thread;

            const PROJECTION_WRITE: &str = "UPDATE rollouts SET archived=1";
            const CHECKPOINT_WRITE: &str = "UPDATE source_files SET byte_offset=0";
            fn serialize(_report: ScanReport) { let _ = serde_json::to_string(&_report); }
        "#,
    );
    for expected in [
        "std::fs",
        "std::path",
        "std::thread",
        "ProjectionTx",
        "projection::",
        "canonical_utc_timestamp",
        "serde_json",
        "calendar",
        "File",
        "PathBuf",
        "SourceSnapshot",
        "DatabaseLock",
        "Arc",
        "ScanReport",
        "rollouts",
        "UPDATE source_files",
    ] {
        assert!(
            attempt_violations.iter().any(|actual| actual == expected),
            "Attempt role failed to reject `{expected}`: {attempt_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestAttempt,
            r#"
                use crate::storage::Db;
                use rusqlite::{OptionalExtension, TransactionBehavior, params};

                fn inspect(db: &Db) {
                    let _ = db.connect().unwrap().query_row(
                        "SELECT EXISTS(SELECT 1 FROM source_files)", [], |_| Ok(true)
                    );
                    let _ = db.connect().unwrap().query_row(
                        "SELECT EXISTS(SELECT 1 FROM threads)", [], |_| Ok(true)
                    );
                    let _ = params!["ingest_state", "scanning"];
                }
            "#,
        )
        .is_empty(),
        "Attempt must be allowed to own named app_meta state and read projection currentness"
    );

    let owner_reader_violations = role_violations(
        Role::IngestOwnerReader,
        r#"
            use crate::{
                ingest::projection::{ProjectionConnection, ProjectionTx},
                storage::{DatabaseLock, Db},
            };
            use rusqlite::{Connection, Transaction};

            const QUERY: &str = "SELECT rollout_id FROM source_files";
        "#,
    );
    for expected in [
        "rusqlite",
        "SELECT ",
        "ProjectionConnection",
        "ProjectionTx",
        "storage",
        "Db",
        "DatabaseLock",
        "Connection",
        "Transaction",
    ] {
        assert!(
            owner_reader_violations
                .iter()
                .any(|actual| actual == expected),
            "OwnerReader role failed to reject `{expected}`: {owner_reader_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestOwnerReader,
            r#"
                use crate::ingest::{
                    protocol::{OwnerMeta, decode_owner_record},
                    source::{BoundedLine, SourceSnapshot},
                };
                use anyhow::Result;
                use serde_json::Value;
                use std::path::Path;

                fn read_owner(
                    _snapshot: &mut SourceSnapshot,
                    _path: &Path,
                ) -> Result<OwnerMeta> {
                    unimplemented!()
                }
            "#,
        )
        .is_empty(),
        "OwnerReader must be allowed to compose only Source framing and Protocol decoding"
    );

    let file_ingestor_violations = role_violations(
        Role::IngestFileIngestor,
        r#"
            use crate::{
                activity::ActivityRow,
                ingest::attempt::AttemptRecorder,
                storage::{DatabaseLock, Db},
            };
            use axum::Router;
            use std::{sync::atomic::AtomicBool, thread};
            use tokio::task::JoinHandle;

            const QUERY: &str = "UPDATE source_files SET byte_offset=0";
            fn run(_roots: IngestRoots, _report: ScanReport) {}
        "#,
    );
    for expected in [
        "axum::",
        "use axum",
        "UPDATE ",
        "std::thread",
        "tokio::",
        "activity",
        "DatabaseLock",
        "AttemptRecorder",
        "IngestRoots",
        "ScanReport",
        "AtomicBool",
        "JoinHandle",
    ] {
        assert!(
            file_ingestor_violations
                .iter()
                .any(|actual| actual == expected),
            "FileIngestor role failed to reject `{expected}`: {file_ingestor_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestFileIngestor,
            r#"
                use crate::{
                    ingest::{
                        checkpoints::FingerprintAuditBudget,
                        projection::ProjectionConnection,
                        source::SourceSnapshot,
                    },
                    storage::Db,
                };
                use rusqlite::Connection;

                struct FileIngestor<'a> {
                    db: &'a Db,
                    audit_budget: FingerprintAuditBudget,
                }

                fn refresh(_connection: &mut Connection, _snapshot: &mut SourceSnapshot) {
                    let _ = ProjectionConnection::new(_connection);
                }
            "#,
        )
        .is_empty(),
        "FileIngestor may compose named ingestion adapters and carry a supplied SQLite connection without owning SQL"
    );

    let coordinator_violations = role_violations(
        Role::IngestCoordinator,
        r#"
            use crate::{
                activity::ActivityRow,
                ingest::{
                    projection::ProjectionTx,
                    source::{BoundedLine, CapturedJsonlReader, SourceSnapshot},
                },
            };
            use axum::Router;
            use rusqlite::Connection;
            use serde_json::Value;
            use std::{fs::File, sync::Arc, thread};
            use tokio::task::JoinHandle;

            const QUERY: &str = "SELECT * FROM source_files";
            struct ScannerHandle;
            fn spawn() { thread::spawn(|| {}); }
        "#,
    );
    for expected in [
        "rusqlite",
        "SELECT ",
        "axum::",
        "use axum",
        "tokio::",
        "activity",
        "ProjectionTx",
        "BoundedLine",
        "CapturedJsonlReader",
        "SourceSnapshot",
        "ScannerHandle",
        "JoinHandle",
        "Arc",
        "File",
        "Value",
    ] {
        assert!(
            coordinator_violations
                .iter()
                .any(|actual| actual == expected),
            "Coordinator role failed to reject `{expected}`: {coordinator_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestCoordinator,
            r#"
                use crate::{
                    calendar::canonical_utc_timestamp,
                    storage::{DatabaseLock, Db},
                };
                use anyhow::Result;
                use chrono::Utc;
                use serde::{Deserialize, Serialize};
                use std::{
                    collections::{HashMap, HashSet},
                    path::{Path, PathBuf},
                    sync::atomic::AtomicBool,
                    time::Duration,
                };

                fn coordinate(_db: &Db, _path: &Path) -> Result<()> { Ok(()) }
            "#,
        )
        .is_empty(),
        "Coordinator must be allowed to compose named ingestion adapters, durable attempt state, and lock ownership"
    );

    let scanner_violations = role_violations(
        Role::IngestScanner,
        r#"
            use crate::{
                activity::ActivityRow,
                ingest::{
                    catalog::SourceCandidate,
                    projection::ProjectionTx,
                    protocol::OwnerMeta,
                    reconciliation::ReconciliationPlan,
                    source::SourceSnapshot,
                },
                storage::{DatabaseLock, Db},
            };
            use axum::Router;
            use rusqlite::Connection;
            use std::fs::File;
            use std::path::PathBuf;

            const QUERY: &str = "DELETE FROM source_files";
            fn inspect(_db: &Db, _source: &mut SourceSnapshot) {
                let _ = _db.connect();
            }
        "#,
    );
    for expected in [
        "rusqlite",
        "DELETE FROM ",
        "axum::",
        "use axum",
        "std::fs",
        ".connect(",
        "projection::",
        "catalog::",
        "reconciliation::",
        "protocol::",
        "source::",
        "activity",
        "Connection",
        "ProjectionTx",
        "SourceSnapshot",
        "OwnerMeta",
        "SourceCandidate",
        "DatabaseLock",
        "File",
        "PathBuf",
    ] {
        assert!(
            scanner_violations.iter().any(|actual| actual == expected),
            "Scanner role failed to reject `{expected}`: {scanner_violations:?}"
        );
    }
    assert!(
        role_violations(
            Role::IngestScanner,
            r#"
                use super::{
                    attempt::AttemptRecorder,
                    coordinator::{
                        IngestRoots, IngestScannerLease, scan_one_shot_with_lease,
                    },
                };
                use crate::storage::Db;
                use anyhow::{Context, Result};
                use std::{
                    sync::{Arc, atomic::{AtomicBool, Ordering}},
                    time::Duration,
                };

                fn worker(
                    _db: Db,
                    _roots: IngestRoots,
                    _lease: IngestScannerLease,
                ) -> Result<()> {
                    let _cancelled = Arc::new(AtomicBool::new(false));
                    let _ = scan_one_shot_with_lease(&_db, &_roots, &_lease);
                    let _ = AttemptRecorder::new(&_db).mark_cycle_failed();
                    Ok(())
                }
            "#,
        )
        .is_empty(),
        "Scanner must be allowed to own thread lifetime and invoke only Coordinator and Attempt boundaries"
    );
}

#[test]
fn architecture_policy_detects_grouped_dependency_direction_regressions() {
    let illegal_costing = r#"
        use crate::{
            usage::{UsageAccumulator, UsageTotals},
            storage::Database,
            sessions::{self, SessionRow},
        };

        fn serve() {
            crate::web::serve();
        }
    "#;
    assert_eq!(
        dependency_direction_violations(illegal_costing, COSTING_FORBIDDEN_DEPENDENCIES),
        ["usage", "storage", "web", "sessions"]
    );

    let illegal_usage = r#"
        use crate::{analytics::StatsBucket, web::{self, ApiError}};

        fn refresh() {
            crate::pricing::refresh();
        }
    "#;
    assert_eq!(
        dependency_direction_violations(illegal_usage, USAGE_FORBIDDEN_DEPENDENCIES),
        ["web", "pricing", "analytics"]
    );

    let harmless_mentions = r###"
        use crate::costing::{self, sessions::InternalTestFixture};
        const EXAMPLE: &str = r#"use crate::{usage::UsageTotals, web::serve};"#;
        // use crate::sessions::SessionRow;
        /* outer comment /* use crate::storage::Database; */ still a comment */
    "###;
    assert!(
        dependency_direction_violations(harmless_mentions, COSTING_FORBIDDEN_DEPENDENCIES)
            .is_empty()
    );
}

#[test]
fn target_architecture_has_no_generic_junk_drawer_modules() {
    let src = manifest_root().join("src");
    let forbidden = ["common.rs", "helpers.rs", "utils.rs"];
    let offenders = rust_files(&src)
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| forbidden.contains(&name))
        })
        .collect::<Vec<_>>();
    assert!(
        offenders.is_empty(),
        "shared code must name the concept it owns; forbidden modules: {}",
        offenders
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

#[test]
fn target_feature_slices_do_not_import_sibling_features() {
    let src = manifest_root().join("src");
    let legacy_api_present = src.join("api.rs").is_file();
    for feature in FEATURE_ROOTS {
        for path in rust_files(&src.join(feature)) {
            let contents = source(&path);
            let contents = production_module_source(&contents);
            if legacy_api_present {
                assert!(
                    !imports_feature(contents, "api"),
                    "{} reaches back into the deleted crate::api module",
                    relative(&path)
                );
            }
            for sibling in FEATURE_ROOTS
                .iter()
                .filter(|candidate| candidate != &feature)
            {
                assert!(
                    !imports_feature(contents, sibling),
                    "{} imports sibling feature `{sibling}`; feature slices may share only named lower-level capabilities",
                    relative(&path)
                );
            }
        }
    }
}

#[test]
fn costing_and_usage_dependencies_point_inward() {
    let src = manifest_root().join("src");
    for (layer, forbidden) in [
        ("costing", COSTING_FORBIDDEN_DEPENDENCIES),
        ("usage", USAGE_FORBIDDEN_DEPENDENCIES),
    ] {
        for path in rust_files(&src.join(layer)) {
            let violations = dependency_direction_violations(&source(&path), forbidden);
            assert!(
                violations.is_empty(),
                "{} imports outward from `{layer}` into: {}; shared lower layers may not depend on transport or feature slices",
                relative(&path),
                violations.join(", ")
            );
        }
    }
}

#[test]
fn calendar_and_web_dependencies_remain_neutral() {
    let root = manifest_root();
    let calendar = root.join("src/calendar.rs");
    let calendar_dependencies = crate_dependency_roots(&source(&calendar));
    assert!(
        calendar_dependencies.is_empty(),
        "{} imports crate-owned behavior into neutral Calendar primitives: {}",
        relative(&calendar),
        calendar_dependencies.join(", ")
    );

    for path in rust_files(&root.join("src/web")) {
        let violations =
            dependency_direction_violations(&source(&path), WEB_FORBIDDEN_DEPENDENCIES);
        assert!(
            violations.is_empty(),
            "{} imports deleted API or feature behavior into Web infrastructure: {}",
            relative(&path),
            violations.join(", ")
        );
    }
}

#[test]
fn conversation_display_is_a_neutral_dependency_with_real_ownership() {
    let root = manifest_root();
    let conversation_root = root.join("src/conversation");
    for path in rust_files(&conversation_root) {
        let dependencies = crate_dependency_roots(&source(&path));
        assert!(
            dependencies.is_empty(),
            "{} imports crate-owned behavior into neutral Conversation display: {}",
            relative(&path),
            dependencies.join(", ")
        );
    }

    let module = code_without_comments_and_literals(&source(&conversation_root.join("mod.rs")));
    assert!(
        module.contains("mod display"),
        "Conversation must own display interpretation"
    );
    assert!(
        !module.contains("fn ") && !module.contains("struct ") && !module.contains("impl "),
        "the Conversation module root must remain declarations only"
    );
}

#[test]
fn analytics_routes_and_prewarm_own_transport_and_startup_runtime() {
    let root = manifest_root();
    let analytics_root = root.join("src/analytics");
    let module_source = source(&analytics_root.join("mod.rs"));
    let module = code_without_comments_and_literals(production_module_source(&module_source));
    for declaration in ["mod overview", "mod prewarm", "mod routes", "mod stats"] {
        assert!(
            module.contains(declaration),
            "Analytics module root is missing `{declaration}`"
        );
    }
    assert!(
        module_source.contains("pub use prewarm::prewarm_current_year_analytics;")
            && module_source.contains("pub(crate) use routes::router;"),
        "Analytics must expose only its public startup prewarm and crate-owned router seams"
    );
    assert!(
        !module.contains("fn ") && !module.contains("struct ") && !module.contains("impl "),
        "Analytics module root must remain declarations and narrow re-exports only"
    );

    let routes_path = analytics_root.join("routes.rs");
    let routes_source = source(&routes_path);
    let routes_production = production_module_source(&routes_source);
    let routes = code_without_comments_and_literals(routes_production);
    assert_eq!(assigned_role(&routes_path), Some(Role::Transport));
    for owned in [
        "fn router",
        "struct OverviewYearQuery",
        "async fn overview",
        "async fn overview_year",
        "struct StatsQuery",
        "async fn stats",
        "fn parse_date",
        "fn validate_public_year",
    ] {
        assert!(
            routes.contains(owned),
            "Analytics transport does not own `{owned}`"
        );
    }
    for route in [
        ".route(\"/overview\"",
        ".route(\"/overview/year\"",
        ".route(\"/stats\"",
    ] {
        assert!(
            routes_production.contains(route),
            "Analytics router is missing `{route}`"
        );
    }
    assert_eq!(
        routes_production
            .matches(".snapshot(WorkClass::Heavy")
            .count(),
        3,
        "each Analytics handler must use exactly one Heavy snapshot"
    );
    for reader in ["read_summary_on", "read_year_on", "read_stats_on"] {
        assert!(
            routes.contains(reader),
            "Analytics transport does not delegate through `{reader}`"
        );
    }
    assert_eq!(
        routes_production
            .matches(".map_err(ApiError::internal)?")
            .count(),
        3,
        "each Analytics handler must map snapshot failures at the HTTP boundary"
    );
    for forbidden in [
        "rusqlite",
        "Connection",
        "Transaction",
        "Db",
        ".prepare(",
        ".query_row(",
        ".execute(",
        "SELECT ",
        "WITH ",
        "INSERT INTO ",
        "UPDATE ",
        "DELETE FROM ",
    ] {
        assert!(
            !routes_production.contains(forbidden),
            "Analytics transport owns persistence/runtime behavior via `{forbidden}`"
        );
    }
    for forbidden in ["api", "app", "prewarm"] {
        assert!(
            !imports_feature(routes_production, forbidden),
            "Analytics routes import forbidden boundary `{forbidden}`"
        );
    }

    let prewarm_path = analytics_root.join("prewarm.rs");
    let prewarm_source = source(&prewarm_path);
    let prewarm_production = production_module_source(&prewarm_source);
    let prewarm = code_without_comments_and_literals(prewarm_production);
    assert_eq!(assigned_role(&prewarm_path), Some(Role::Runtime));
    for owned in [
        "fn prewarm_current_year_analytics",
        "fn prewarm_current_year_analytics_on",
    ] {
        assert!(
            prewarm.contains(owned),
            "Analytics runtime does not own `{owned}`"
        );
    }
    assert_eq!(
        prewarm_production.matches("db.connect()?").count(),
        1,
        "prewarm must open exactly one connection"
    );
    assert_eq!(
        prewarm_production
            .matches("TransactionBehavior::Deferred")
            .count(),
        1,
        "prewarm must own exactly one deferred read transaction"
    );
    assert_eq!(
        prewarm_production.matches("transaction.commit()?").count(),
        1,
        "prewarm must commit its one read snapshot exactly once"
    );
    let overview_call = prewarm_production
        .find("read_year_on(connection, year, &start, &end)?")
        .expect("prewarm must execute Overview's current-year reader");
    let stats_call = prewarm_production
        .find("read_stats_on(connection, StatsRange::Year, anchor)?")
        .expect("prewarm must execute Stats' current-year reader");
    assert!(
        overview_call < stats_call,
        "prewarm must preserve Overview-before-Stats cache warming"
    );
    for forbidden in [
        "axum",
        "ReadRuntime",
        "StorageExecutor",
        "WorkClass",
        "ApiError",
        "Router",
    ] {
        assert!(
            !dependency_tokens(&prewarm)
                .iter()
                .any(|token| token == forbidden),
            "Analytics prewarm imports transport/application behavior `{forbidden}`"
        );
    }
    for forbidden in ["api", "app", "web"] {
        assert!(
            !imports_feature(prewarm_production, forbidden),
            "Analytics prewarm imports forbidden boundary `{forbidden}`"
        );
    }

    let app_source = source(&root.join("src/app.rs"));
    assert!(
        app_source.contains(".merge(analytics::router(reads.clone()))")
            && !app_source.contains("api::analytics_router"),
        "application composition must mount the Analytics-owned router directly"
    );
    let main_source = source(&root.join("src/main.rs"));
    let serve_source = main_source
        .split_once("Command::Serve(args) => {")
        .expect("main contains Serve startup")
        .1;
    let configured_db = serve_source
        .find("open_configured_db(&common)?")
        .expect("serve startup opens configured storage");
    let stale_projection = serve_source
        .find("projector_generation_is_current(&db)?")
        .expect("serve startup validates projector generation");
    let prewarm_call = serve_source
        .find("analytics::prewarm_current_year_analytics(&db)?")
        .expect("serve startup invokes Analytics prewarm directly");
    let executor = serve_source
        .find("let executor = StorageExecutor::default();")
        .expect("serve startup constructs the shared executor");
    let scanner = serve_source
        .find("ingest::spawn_scanner_with_lease(")
        .expect("serve startup launches the scanner after prewarm");
    assert!(
        configured_db < stale_projection
            && stale_projection < prewarm_call
            && prewarm_call < executor
            && prewarm_call < scanner,
        "Analytics prewarm moved outside its post-recovery, pre-worker startup slot"
    );
    let lib_source = source(&root.join("src/lib.rs"));
    let lib_lines = lib_source.lines().map(str::trim).collect::<Vec<_>>();
    assert!(
        lib_lines.contains(&"pub mod analytics;") && !lib_lines.contains(&"mod analytics;"),
        "the binary must enter Analytics through its public module"
    );
}

#[test]
fn analytics_overview_read_owns_persistence_and_independent_top_session_shape() {
    let root = manifest_root();
    let analytics_root = root.join("src/analytics");
    let module = code_without_comments_and_literals(&source(&analytics_root.join("mod.rs")));
    assert!(
        module.contains("mod overview") && !module.contains("top_sessions"),
        "Analytics must expose Overview as the sole owner of its read model"
    );
    assert!(
        !module.contains("fn ") && !module.contains("struct ") && !module.contains("impl "),
        "the Analytics module root must remain declarations only"
    );
    assert!(
        !analytics_root.join("top_sessions.rs").exists(),
        "top-session persistence must be folded into Overview"
    );

    let overview_source = source(&analytics_root.join("overview/mod.rs"));
    let overview = code_without_comments_and_literals(production_module_source(&overview_source));
    assert!(
        overview.contains("mod read"),
        "Overview must declare its read model"
    );
    assert!(
        overview.contains("use read::{read_summary_on, read_year_on}"),
        "Overview must expose only its two request-scoped readers"
    );

    let read_source = source(&analytics_root.join("overview/read.rs"));
    let read = code_without_comments_and_literals(production_module_source(&read_source));
    for owned in [
        "fn read_summary_on",
        "fn read_year_on",
        "struct TopSessionRecord",
        "fn read_top_sessions_on",
        "OVERVIEW_SUMMARY_USAGE_SQL",
        "OVERVIEW_SUMMARY_SESSIONS_SQL",
        "OVERVIEW_SUMMARY_MESSAGES_SQL",
        "OVERVIEW_YEAR_USAGE_SQL",
        "OVERVIEW_YEAR_MESSAGE_ACTIVITY_SQL",
        "OVERVIEW_EVENT_DAY_SEEK_SQL",
    ] {
        assert!(
            read.contains(owned),
            "Overview persistence does not own '{owned}'"
        );
    }
    let record_tail = read
        .split_once("struct TopSessionRecord")
        .expect("TopSessionRecord declaration exists")
        .1;
    let record_body = record_tail
        .split_once('{')
        .expect("TopSessionRecord has a body")
        .1
        .split_once('}')
        .expect("TopSessionRecord body terminates")
        .0;
    assert_eq!(
        record_body
            .lines()
            .filter(|line| line.contains(':'))
            .count(),
        16,
        "Overview's private top-session record must retain its exact independent field set"
    );

    let tokens = dependency_tokens(&read);
    for forbidden in [
        "SessionRow",
        "session_from_row",
        "ApiError",
        "ReadRuntime",
        "WorkClass",
        "axum",
    ] {
        assert!(
            !tokens.iter().any(|token| token == forbidden),
            "Overview persistence reuses boundary or sibling behavior '{forbidden}'"
        );
    }
    let dependencies = crate_dependency_roots(&read);
    for forbidden in ["api", "web", "storage", "stats", "activity", "sessions"] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Overview persistence imports forbidden sibling or boundary '{forbidden}'"
        );
    }

    let overview_sql_markers = [
        "OVERVIEW_SUMMARY_USAGE_SQL",
        "OVERVIEW_SUMMARY_SESSIONS_SQL",
        "OVERVIEW_SUMMARY_MESSAGES_SQL",
        "OVERVIEW_YEAR_USAGE_SQL",
        "OVERVIEW_YEAR_MESSAGE_ACTIVITY_SQL",
        "OVERVIEW_EVENT_DAY_SEEK_SQL",
    ];
    for path in rust_files(&root.join("src")) {
        if relative(&path) == "src/analytics/overview/read.rs" {
            continue;
        }
        let candidate_source = source(&path);
        let candidate =
            code_without_comments_and_literals(production_module_source(&candidate_source));
        for marker in overview_sql_markers {
            assert!(
                !candidate.contains(marker),
                "{} duplicates Overview SQL ownership via {marker}",
                relative(&path)
            );
        }
    }
    assert!(
        overview.contains("struct TopSessionResponse")
            && overview.contains("Vec<TopSessionResponse>"),
        "the Overview domain must own its top-session result shape"
    );
}

#[test]
fn analytics_overview_owns_pure_policy_without_stats_or_legacy_coupling() {
    let root = manifest_root();
    let overview_path = root.join("src/analytics/overview/mod.rs");
    let overview_source = source(&overview_path);
    let overview = code_without_comments_and_literals(production_module_source(&overview_source));

    for owned in [
        "struct PeriodSummary",
        "struct OverviewPeriods",
        "struct HeatmapDay",
        "struct ProjectDriver",
        "struct OverviewResponse",
        "struct OverviewYearResponse",
        "struct TopSessionResponse",
        "struct OverviewPeriodBound",
        "struct OverviewDayBucket",
        "struct OverviewUsageAggregate",
        "fn overview_summary_bounds",
        "fn overview_period_summary",
        "fn overview_year_days",
        "fn rank_overview_year_projects",
        "fn rank_overview_year_sessions",
    ] {
        assert!(
            overview.contains(owned),
            "Analytics Overview does not own `{owned}`"
        );
    }

    let tokens = dependency_tokens(&overview);
    for forbidden in [
        "StatsBucket",
        "StatsBucketAggregate",
        "push_nonempty_stats_bucket",
        "axum",
        "rusqlite",
        "Connection",
        "Transaction",
        "ReadRuntime",
        "WorkClass",
        "ApiError",
    ] {
        assert!(
            !tokens.iter().any(|token| token == forbidden),
            "Analytics Overview reuses forbidden sibling or boundary behavior `{forbidden}`"
        );
    }
    let dependencies = crate_dependency_roots(&overview);
    for forbidden in ["api", "web", "storage", "stats", "activity", "sessions"] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Analytics Overview imports forbidden sibling or legacy feature `{forbidden}`"
        );
    }
}

#[test]
fn analytics_stats_owns_pure_policy_without_overview_or_legacy_coupling() {
    let root = manifest_root();
    let analytics_root = root.join("src/analytics");
    let module = code_without_comments_and_literals(&source(&analytics_root.join("mod.rs")));
    assert!(
        module.contains("mod overview") && module.contains("mod stats"),
        "Analytics must declare its independent Overview and Stats domains"
    );
    assert!(
        !module.contains("fn ") && !module.contains("struct ") && !module.contains("impl "),
        "the Analytics module root must remain declarations only"
    );

    let stats_source = source(&analytics_root.join("stats/mod.rs"));
    let stats = code_without_comments_and_literals(production_module_source(&stats_source));
    assert!(
        stats.contains("mod read"),
        "Stats must declare its private persistence reader"
    );
    assert!(
        stats_source.contains("pub(crate) use read::read_on;")
            && !stats_source.contains("pub(crate) use read::{"),
        "Stats must narrowly expose only read_on from persistence"
    );
    for owned in [
        "enum StatsRange",
        "struct StatsBucket",
        "struct StatsBucketAggregate",
        "struct StatsRow",
        "struct StatsResponse",
        "fn canonical_stats_anchor",
        "fn stats_range_label",
        "fn stats_buckets",
        "fn stats_totals_from_aggregates",
        "fn disambiguate_repeated_labels",
        "fn push_nonempty_stats_bucket",
    ] {
        assert!(
            stats.contains(owned),
            "Analytics Stats does not own `{owned}`"
        );
    }

    let stats_tokens = dependency_tokens(&stats);
    for forbidden in [
        "OverviewResponse",
        "OverviewYearResponse",
        "PeriodSummary",
        "OverviewPeriods",
        "axum",
        "rusqlite",
        "Connection",
        "Transaction",
        "ReadRuntime",
        "WorkClass",
        "ApiError",
        "SqlBucketBounds",
    ] {
        assert!(
            !stats_tokens.iter().any(|token| token == forbidden),
            "Analytics Stats reuses forbidden sibling, boundary, or persistence behavior `{forbidden}`"
        );
    }
    let stats_dependencies = crate_dependency_roots(&stats);
    for forbidden in [
        "api", "web", "storage", "overview", "pricing", "sessions", "activity",
    ] {
        assert!(
            !stats_dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Analytics Stats imports forbidden sibling, boundary, or persistence feature `{forbidden}`"
        );
    }

    let overview_source = source(&analytics_root.join("overview/mod.rs"));
    let overview = code_without_comments_and_literals(production_module_source(&overview_source));
    let overview_tokens = dependency_tokens(&overview);
    for forbidden in [
        "StatsRange",
        "StatsBucket",
        "StatsBucketAggregate",
        "StatsRow",
        "StatsResponse",
        "canonical_stats_anchor",
        "stats_range_label",
        "stats_buckets",
        "stats_totals_from_aggregates",
    ] {
        assert!(
            !overview_tokens.iter().any(|token| token == forbidden),
            "Analytics Overview crosses into Stats via `{forbidden}`"
        );
    }
}

#[test]
fn analytics_stats_read_owns_persistence_without_sibling_or_boundary_coupling() {
    let root = manifest_root();
    let read_path = root.join("src/analytics/stats/read.rs");
    let read_source = source(&read_path);
    let read = code_without_comments_and_literals(production_module_source(&read_source));

    assert_eq!(
        ROLE_ASSIGNMENTS
            .iter()
            .filter(|(path, _)| *path == "src/analytics/stats/read.rs")
            .copied()
            .collect::<Vec<_>>(),
        vec![("src/analytics/stats/read.rs", Role::Persistence)],
        "Stats read persistence must have exactly one explicit Persistence assignment"
    );

    for owned in [
        "fn read_on",
        "fn stats_buckets_on",
        "fn occupied_local_years_on",
        "struct SqlBucketBounds",
        "fn stats_buckets_are_broad",
        "fn stats_exceptional_group_cost_on",
        "fn query_stats_bucket_aggregates_on",
        "STATS_BUCKET_USAGE_SQL",
        "STATS_BUCKET_SESSIONS_SQL",
        "STATS_FEW_BUCKET_SESSIONS_SQL",
    ] {
        assert!(
            read.contains(owned),
            "Stats persistence does not own `{owned}`"
        );
    }

    let visible_items = production_module_source(&read_source)
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("pub ") || line.starts_with("pub("))
        .collect::<Vec<_>>();
    assert_eq!(
        visible_items,
        vec!["pub(crate) fn read_on("],
        "read_on must be the Stats persistence module's only visible item"
    );
    assert!(
        read.contains("StatsRange") && read.contains("StatsResponse"),
        "Stats persistence must expose a typed StatsRange-to-StatsResponse seam"
    );

    let tokens = dependency_tokens(&read);
    for forbidden in [
        "OverviewResponse",
        "OverviewYearResponse",
        "PeriodSummary",
        "OverviewPeriods",
        "ApiError",
        "ReadRuntime",
        "WorkClass",
        "Db",
        "Transaction",
        "axum",
    ] {
        assert!(
            !tokens.iter().any(|token| token == forbidden),
            "Stats persistence reuses sibling or boundary behavior `{forbidden}`"
        );
    }
    let dependencies = crate_dependency_roots(&read);
    for forbidden in [
        "api", "web", "storage", "overview", "activity", "sessions", "pricing", "system",
    ] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Stats persistence imports forbidden sibling or boundary `{forbidden}`"
        );
    }
    assert!(
        dependencies.iter().all(|dependency| {
            [
                "MAX_PUBLIC_YEAR",
                "MIN_PUBLIC_YEAR",
                "calendar",
                "costing",
                "usage",
            ]
            .contains(&dependency.as_str())
        }),
        "Stats persistence may import only public-year bounds plus Calendar, Costing, and Usage: {dependencies:?}"
    );

    let stats_sql_markers = [
        "STATS_BUCKET_USAGE_SQL",
        "STATS_BUCKET_SESSIONS_SQL",
        "STATS_FEW_BUCKET_SESSIONS_SQL",
    ];
    for path in rust_files(&root.join("src")) {
        if relative(&path) == "src/analytics/stats/read.rs" {
            continue;
        }
        let candidate_source = source(&path);
        let candidate =
            code_without_comments_and_literals(production_module_source(&candidate_source));
        for marker in stats_sql_markers {
            assert!(
                !candidate.contains(marker),
                "{} duplicates Stats SQL ownership via {marker}",
                relative(&path)
            );
        }
    }
}

#[test]
fn sessions_routes_own_transport_and_are_mounted_directly() {
    let root = manifest_root();
    let sessions_root = root.join("src/sessions");
    let module_source = source(&sessions_root.join("mod.rs"));
    let module_production_source = module_source
        .split_once("#[cfg(test)]")
        .map_or(module_source.as_str(), |(production, _)| production);
    let module = code_without_comments_and_literals(&module_source);
    assert!(
        module.contains("mod routes") && module.contains("use routes::router"),
        "Sessions must declare and narrowly expose its transport router"
    );
    assert!(
        module_production_source.contains("pub(crate) use routes::router;")
            && !module_production_source.contains("pub(crate) use catalog")
            && !module_production_source.contains("pub(crate) use list")
            && !module_production_source.contains("pub(crate) use summary"),
        "production Sessions callers must enter through the router, not module-root persistence re-exports"
    );

    let routes_source = source(&sessions_root.join("routes.rs"));
    let routes_production_source = production_module_source(&routes_source);
    let routes = code_without_comments_and_literals(routes_production_source);
    for owned in [
        "fn router",
        "struct SessionRow",
        "struct SessionsQuery",
        "struct SessionsResponse",
        "struct ProjectsResponse",
        "struct ModelUsage",
        "struct AgentSummary",
        "struct ToolSummary",
        "struct SessionSummaryResponse",
        "struct SessionDetail",
        "async fn sessions",
        "async fn projects",
        "async fn session_summary",
        "fn query_bounds",
        "fn parse_boundary",
    ] {
        assert!(
            routes.contains(owned),
            "Sessions transport does not own `{owned}`"
        );
    }
    for route in [
        ".route(\"/projects\"",
        ".route(\"/sessions\"",
        ".route(\"/sessions/{id}/summary\"",
    ] {
        assert!(
            routes_production_source.contains(route),
            "Sessions router is missing `{route}`"
        );
    }
    for forbidden in [
        "rusqlite",
        ".prepare(",
        ".query_row(",
        ".execute(",
        ".execute_batch(",
        "SELECT ",
        "WITH ",
        "INSERT INTO ",
        "UPDATE ",
        "DELETE FROM ",
        "CREATE ",
        "DROP ",
        "ALTER ",
        "PRAGMA ",
        "REPLACE INTO ",
        "VACUUM ",
    ] {
        assert!(
            !routes_production_source.contains(forbidden),
            "Sessions transport owns persistence via `{forbidden}`"
        );
    }
    let route_tokens = dependency_tokens(&routes);
    for forbidden in [
        "Db",
        "Connection",
        "Transaction",
        "StorageExecutor",
        "rusqlite",
        "params",
        "OptionalExtension",
    ] {
        assert!(
            !route_tokens.iter().any(|token| token == forbidden),
            "Sessions transport imports persistence/runtime primitive `{forbidden}`"
        );
    }
    assert!(
        !imports_feature(&routes_source, "api"),
        "Sessions routes reach back into the deleted API module"
    );

    let list_handler = routes
        .split_once("async fn sessions")
        .expect("Sessions list handler exists")
        .1
        .split_once("struct ProjectsResponse")
        .expect("Sessions list handler ends before projects transport")
        .0;
    assert_eq!(
        dependency_tokens(list_handler)
            .iter()
            .filter(|token| token.as_str() == "snapshot")
            .count(),
        1,
        "Sessions list and its project options must share one read snapshot"
    );
    assert!(
        list_handler.contains("read_session_page_on")
            && list_handler.contains("read_projects_on")
            && list_handler.contains("WorkClass::Heavy"),
        "Sessions list must assemble rows and projects in one Heavy snapshot"
    );
    let summary_handler = routes
        .split_once("async fn session_summary")
        .expect("Sessions summary handler exists")
        .1
        .split_once("fn query_bounds")
        .expect("Sessions summary handler ends before date validation")
        .0;
    assert_eq!(
        dependency_tokens(summary_handler)
            .iter()
            .filter(|token| token.as_str() == "snapshot")
            .count(),
        1,
        "Sessions summary must retain one read snapshot"
    );
    assert!(
        summary_handler.contains("read_summary_on") && summary_handler.contains("WorkClass::Heavy"),
        "Sessions summary must use the Sessions read model in one Heavy snapshot"
    );

    let app = code_without_comments_and_literals(&source(&root.join("src/app.rs")));
    assert!(
        app.contains("sessions::router") && !app.contains("api::sessions_router"),
        "the composition root must mount the Sessions-owned router directly"
    );
}

#[test]
fn sessions_catalog_has_independent_persistence_ownership() {
    let root = manifest_root();
    let sessions_root = root.join("src/sessions");
    let module = code_without_comments_and_literals(&source(&sessions_root.join("mod.rs")));
    assert!(
        module.contains("mod catalog"),
        "Sessions must declare catalog persistence ownership"
    );
    assert!(
        !module.contains("fn ") && !module.contains("struct ") && !module.contains("impl "),
        "the Sessions module root must remain declarations and narrow re-exports only"
    );

    let catalog_source = source(&sessions_root.join("catalog.rs"));
    let catalog = code_without_comments_and_literals(production_module_source(&catalog_source));
    for owned in [
        "struct SessionRecord",
        "fn read_session_on",
        "fn read_projects_on",
    ] {
        assert!(
            catalog.contains(owned),
            "Sessions catalog persistence does not own `{owned}`"
        );
    }
    let record_tail = catalog
        .split_once("struct SessionRecord")
        .expect("SessionRecord declaration exists")
        .1;
    let record_body = record_tail
        .split_once('{')
        .expect("SessionRecord has a body")
        .1
        .split_once('}')
        .expect("SessionRecord body terminates")
        .0;
    let field_count = record_body
        .lines()
        .filter(|line| line.trim_start().starts_with("pub(crate)") && line.contains(':'))
        .count();
    assert_eq!(
        field_count, 15,
        "Sessions catalog record must retain its exact transport-neutral field set"
    );

    let catalog_tokens = dependency_tokens(&catalog);
    for forbidden in [
        "axum",
        "serde",
        "Serialize",
        "Deserialize",
        "SessionRow",
        "SessionsResponse",
        "SessionSummaryResponse",
        "ProjectsResponse",
        "query_session_root_rollout_id_on",
        "query_sessions_on",
        "session_from_row",
        "cost_numerator_from_row",
        "normalize_search_text",
        "populate_session_search_matches_on",
        "SessionsQuery",
        "page",
        "page_size",
    ] {
        assert!(
            !catalog_tokens.iter().any(|token| token == forbidden),
            "Sessions catalog leaks transport, summary, or list ownership via `{forbidden}`"
        );
    }
    for forbidden in [
        "session_candidates",
        "session_sort_costs",
        "selected_sessions",
        "session_search_matches",
    ] {
        assert!(
            !catalog_source.contains(forbidden),
            "Sessions catalog absorbs list temp-table ownership via `{forbidden}`"
        );
    }
    let dependencies = crate_dependency_roots(&catalog);
    for forbidden in ["api", "activity", "analytics"] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Sessions catalog imports sibling/legacy feature `{forbidden}`"
        );
    }
}

#[test]
fn sessions_list_has_independent_persistence_ownership() {
    let root = manifest_root();
    let sessions_root = root.join("src/sessions");
    let module = code_without_comments_and_literals(&source(&sessions_root.join("mod.rs")));
    assert!(
        module.contains("mod list"),
        "Sessions must declare list persistence ownership"
    );

    let list_source = source(&sessions_root.join("list.rs"));
    let list_production_source = production_module_source(&list_source);
    let list = code_without_comments_and_literals(list_production_source);
    for owned in [
        "const MAX_SEARCH_CHARS",
        "enum SessionListSort",
        "struct SessionListRequest",
        "struct SessionListRecord",
        "struct SessionListPage",
        "fn populate_session_sort_costs_on",
        "fn utc_hour_floor",
        "fn utc_hour_ceil",
        "fn accumulate_session_boundary_usage_on",
        "fn sortable_cost_numerator",
        "fn query_selected_session_totals_on",
        "fn read_session_page_on",
        "fn normalize_search_text",
        "fn populate_session_search_matches_on",
        "fn session_from_row",
        "fn cost_numerator_from_row",
    ] {
        assert!(
            list.contains(owned),
            "Sessions list persistence does not own `{owned}`"
        );
    }
    assert!(
        !list.contains("fn query_sessions_on"),
        "Sessions list persistence retains the legacy query name"
    );
    let record_tail = list
        .split_once("struct SessionListRecord")
        .expect("SessionListRecord declaration exists")
        .1;
    let record_body = record_tail
        .split_once('{')
        .expect("SessionListRecord has a body")
        .1
        .split_once('}')
        .expect("SessionListRecord body terminates")
        .0;
    let field_count = record_body
        .lines()
        .filter(|line| line.trim_start().starts_with("pub(crate)") && line.contains(':'))
        .count();
    assert_eq!(
        field_count, 15,
        "Sessions list record must retain its exact transport-neutral field set"
    );

    let list_tokens = dependency_tokens(&list);
    for forbidden in [
        "axum",
        "serde",
        "Serialize",
        "Deserialize",
        "SessionRow",
        "SessionsResponse",
        "SessionRecord",
        "read_session_on",
        "read_projects_on",
        "WorkClass",
        "ReadRuntime",
        "Db",
        "snapshot",
        "catalog",
    ] {
        assert!(
            !list_tokens.iter().any(|token| token == forbidden),
            "Sessions list persistence leaks transport, catalog, or runtime ownership via `{forbidden}`"
        );
    }
    let dependencies = crate_dependency_roots(&list);
    for forbidden in [
        "api",
        "web",
        "storage",
        "activity",
        "analytics",
        "pricing",
        "system",
    ] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Sessions list imports forbidden feature/runtime dependency `{forbidden}`"
        );
    }

    for table in [
        "session_candidates",
        "session_sort_costs",
        "selected_sessions",
        "session_search_matches",
    ] {
        assert!(
            list_production_source.contains(&format!("CREATE TEMP TABLE IF NOT EXISTS {table}")),
            "Sessions list does not own TEMP table `{table}`"
        );
    }
    assert!(
        list_production_source.contains("format!(\"{:039}\"")
            && list_production_source.contains("000000000000000000000000000000000000000",),
        "Sessions list must retain the exact 39-digit i128 cost sort key"
    );

    let catalog_source = source(&sessions_root.join("catalog.rs"));
    let catalog_production_source = production_module_source(&catalog_source);
    let catalog = code_without_comments_and_literals(catalog_production_source);
    for moved in [
        "fn populate_session_sort_costs_on",
        "fn utc_hour_floor",
        "fn utc_hour_ceil",
        "fn accumulate_session_boundary_usage_on",
        "fn sortable_cost_numerator",
        "fn query_selected_session_totals_on",
        "fn query_sessions_on",
        "fn normalize_search_text",
        "fn populate_session_search_matches_on",
        "fn session_from_row",
        "fn cost_numerator_from_row",
    ] {
        assert!(
            !catalog.contains(moved),
            "Sessions catalog absorbs moved list ownership `{moved}`"
        );
    }
    for table in [
        "session_candidates",
        "session_sort_costs",
        "selected_sessions",
        "session_search_matches",
    ] {
        assert!(
            !catalog_production_source.contains(table),
            "Sessions catalog absorbs Sessions list TEMP table `{table}`"
        );
        for path in rust_files(&root.join("src")) {
            if path == sessions_root.join("list.rs") {
                continue;
            }
            let contents = source(&path);
            assert!(
                !production_module_source(&contents).contains(table),
                "{} duplicates Sessions list TEMP table `{table}`",
                relative(&path)
            );
        }
    }
}

#[test]
fn sessions_summary_has_independent_persistence_ownership() {
    let root = manifest_root();
    let sessions_root = root.join("src/sessions");
    let module = code_without_comments_and_literals(&source(&sessions_root.join("mod.rs")));
    assert!(
        module.contains("mod summary"),
        "Sessions must declare summary persistence ownership"
    );

    let summary_source = source(&sessions_root.join("summary.rs"));
    let summary = code_without_comments_and_literals(production_module_source(&summary_source));
    for owned in [
        "struct ModelUsageRecord",
        "struct AgentSummaryRecord",
        "struct ToolSummaryRecord",
        "struct SessionDetailRecord",
        "struct SessionSummaryRecord",
        "fn read_summary_on",
        "fn read_session_detail_on",
        "fn read_session_root_rollout_id_on",
        "fn read_model_usage_on",
        "fn read_agent_totals_on",
        "fn read_agent_summary_on",
        "fn read_tool_summary_on",
    ] {
        assert!(
            summary.contains(owned),
            "Sessions summary persistence does not own `{owned}`"
        );
    }
    assert!(
        summary.contains("connection: &Connection"),
        "Sessions summary reads must remain scoped to one supplied snapshot connection"
    );

    let summary_tokens = dependency_tokens(&summary);
    for forbidden in [
        "axum",
        "serde",
        "Serialize",
        "Deserialize",
        "SessionRow",
        "ModelUsage",
        "AgentSummary",
        "ToolSummary",
        "SessionDetail",
        "SessionSummaryResponse",
        "SessionListPage",
        "SessionListRecord",
        "SessionListRequest",
        "SessionListSort",
        "read_session_page_on",
        "MAX_SEARCH_CHARS",
        "ReadRuntime",
        "WorkClass",
        "Db",
        "StorageExecutor",
        "Transaction",
        "snapshot",
        "connect",
    ] {
        assert!(
            !summary_tokens.iter().any(|token| token == forbidden),
            "Sessions summary leaks transport, list, or runtime ownership via `{forbidden}`"
        );
    }
    let dependencies = crate_dependency_roots(&summary);
    for forbidden in ["api", "activity", "analytics", "web", "storage"] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Sessions summary imports forbidden feature/runtime dependency `{forbidden}`"
        );
    }

    let routes_source = source(&sessions_root.join("routes.rs"));
    let routes = code_without_comments_and_literals(production_module_source(&routes_source));
    assert!(
        dependency_tokens(&routes)
            .iter()
            .any(|token| token == "read_summary_on"),
        "the Sessions summary route must call the Sessions-owned read model"
    );
    let list_source = source(&sessions_root.join("list.rs"));
    let list = code_without_comments_and_literals(production_module_source(&list_source));
    for leaked in [
        "SessionSummaryRecord",
        "read_summary_on",
        "read_session_detail_on",
        "read_model_usage_on",
        "read_agent_summary_on",
        "read_tool_summary_on",
    ] {
        assert!(
            !dependency_tokens(&list).iter().any(|token| token == leaked),
            "Sessions list absorbs summary ownership `{leaked}`"
        );
    }
}

#[test]
fn activity_root_page_has_independent_persistence_ownership() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module = code_without_comments_and_literals(&source(&activity_root.join("mod.rs")));
    assert!(
        module.contains("mod root_page"),
        "Activity must declare root-page persistence ownership"
    );

    let root_page_source = source(&activity_root.join("root_page.rs"));
    let root_page = code_without_comments_and_literals(production_module_source(&root_page_source));
    for owned in [
        "fn visible_thread_exists_on",
        "fn root_rollout_id_on",
        "struct ActivityRootAggregate",
        "struct ActivityBatch",
        "struct RootExchange",
        "fn read_exchange",
        "fn read_page_on",
        "fn query_activity_day_summaries_batched",
        "fn record_activity_row",
        "fn parse_activity_thread_bounds",
        "fn bounded_activity_interval",
        "fn insert_activity_interval_dates",
        "fn activity_day_window",
        "fn activity_union_duration",
    ] {
        assert!(
            root_page.contains(owned),
            "Activity root-page persistence does not own `{owned}`"
        );
    }
    let root_page_tokens = dependency_tokens(&root_page);
    for forbidden in ["SessionRow", "query_session_on"] {
        assert!(
            !root_page_tokens.iter().any(|token| token == forbidden),
            "Activity root-page persistence reuses Sessions behavior `{forbidden}`"
        );
    }
    let dependencies = crate_dependency_roots(&root_page);
    for forbidden in ["api", "sessions"] {
        assert!(
            !dependencies
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity root-page persistence imports sibling/legacy feature `{forbidden}`"
        );
    }

    let routes_source = source(&activity_root.join("routes.rs"));
    let routes = code_without_comments_and_literals(production_module_source(&routes_source));
    let activity_handlers = routes
        .split_once("async fn session_activity(")
        .expect("Activity routes exist")
        .1;
    assert!(
        !dependency_tokens(activity_handlers)
            .iter()
            .any(|token| token == "query_session_on"),
        "Activity route guards reach through Sessions and compute full lifetime pricing"
    );
    assert_eq!(
        dependency_tokens(activity_handlers)
            .iter()
            .filter(|token| token.as_str() == "visible_thread_exists_on")
            .count(),
        2,
        "both Activity routes must use the Activity-owned visibility predicate"
    );
    let route_tokens = dependency_tokens(&routes);
    assert!(
        !route_tokens
            .iter()
            .any(|token| token == "query_session_root_rollout_id_on"),
        "src/api.rs must not retain Sessions summary root-rollout persistence"
    );
    assert!(
        !route_tokens
            .iter()
            .any(|token| token == "query_root_rollout_id"),
        "the generic cross-feature root-rollout query must be removed"
    );
    for moved in [
        "fn visible_thread_exists_on",
        "fn root_rollout_id_on",
        "struct ActivityRootAggregate",
        "struct ActivityBatch",
        "fn query_activity_on",
        "fn query_activity_day_summaries_batched",
        "fn record_activity_row",
        "fn parse_activity_thread_bounds",
        "fn bounded_activity_interval",
        "fn insert_activity_interval_dates",
        "fn activity_day_window",
        "fn activity_union_duration",
    ] {
        assert!(
            !routes.contains(moved),
            "Activity routes retain root-page persistence via `{moved}`"
        );
    }
}

#[test]
fn activity_routes_own_transport_and_are_mounted_directly() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module_source = source(&activity_root.join("mod.rs"));
    let module = code_without_comments_and_literals(&module_source);
    assert!(
        module.contains("mod routes") && module.contains("pub(crate) use routes::router"),
        "Activity must expose only its state-bound router"
    );
    for leaked in [
        "ActivityItem",
        "ActivityResponse",
        "PreparedSelection",
        "ActivityRootScope",
        "query_activity",
    ] {
        assert!(
            !dependency_tokens(&module)
                .iter()
                .any(|token| token == leaked),
            "Activity module root leaks internal ownership `{leaked}`"
        );
    }

    let routes_source = source(&activity_root.join("routes.rs"));
    let routes = code_without_comments_and_literals(production_module_source(&routes_source));
    for owned in [
        "fn router",
        "struct PageQuery",
        "struct ActivityDetailQuery",
        "async fn session_activity",
        "async fn session_activity_detail",
    ] {
        assert!(
            routes.contains(owned),
            "Activity routes do not own `{owned}`"
        );
    }
    for path in [
        ".route(\"/sessions/{id}/activity\"",
        "\"/sessions/{id}/activity/{event_id}\"",
    ] {
        assert!(
            routes_source.contains(path),
            "Activity route is missing `{path}`"
        );
    }
    assert_eq!(
        routes.matches("snapshot(WorkClass::Heavy").count(),
        2,
        "each Activity handler must own one Heavy read snapshot"
    );
    assert_eq!(
        dependency_tokens(&routes)
            .iter()
            .filter(|token| token.as_str() == "visible_thread_exists_on")
            .count(),
        3,
        "Activity routes must import one visibility predicate and call it in both handlers"
    );
    for forbidden in [
        "rusqlite",
        "Connection",
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
    ] {
        assert!(
            !dependency_tokens(&routes)
                .iter()
                .any(|token| token == forbidden)
                && !routes_source.contains(forbidden),
            "Activity transport owns persistence via `{forbidden}`"
        );
    }
    for forbidden in ["api", "sessions"] {
        assert!(
            !crate_dependency_roots(&routes)
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity routes import sibling/legacy feature `{forbidden}`"
        );
    }
    let validate = routes
        .find("validate_detail_cursor_for")
        .expect("detail cursor validation exists");
    let snapshot = routes[validate..]
        .find("snapshot")
        .map(|offset| validate + offset)
        .expect("detail snapshot exists");
    assert!(
        validate < snapshot,
        "Activity detail cursor validation must happen before reserving a worker snapshot"
    );

    let app = code_without_comments_and_literals(&source(&root.join("src/app.rs")));
    assert!(
        app.contains("activity::router") && !app.contains("api::"),
        "application composition must mount Activity directly"
    );
    let lib = code_without_comments_and_literals(&source(&root.join("src/lib.rs")));
    assert!(
        !lib.contains("mod api"),
        "lib.rs retains the deleted API module"
    );
}

#[test]
fn activity_previews_owns_bounded_hydration_and_child_paging() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module = code_without_comments_and_literals(&source(&activity_root.join("mod.rs")));
    assert!(
        module.contains("mod previews"),
        "Activity must declare preview persistence ownership"
    );

    let previews_source = source(&activity_root.join("previews.rs"));
    let previews = code_without_comments_and_literals(production_module_source(&previews_source));
    for owned in [
        "const ACTIVITY_PREVIEW_CHARS",
        "const ACTIVITY_MESSAGE_PARSE_BYTES",
        "struct ActivityChildrenPage",
        "fn read_legacy_root",
        "fn query_legacy_activity_children_page",
        "fn query_legacy_message_child_rows",
        "fn query_activity_child_previews_cursor_page",
        "fn query_activity_child_preview_rows",
        "fn activity_content_from_edges",
        "fn bounded_preview",
        "fn normalize_activity_kind",
    ] {
        assert!(
            previews.contains(owned),
            "Activity previews does not own `{owned}`"
        );
    }
    for forbidden in ["api", "sessions", "web", "storage"] {
        assert!(
            !crate_dependency_roots(&previews)
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity previews imports forbidden feature/runtime dependency `{forbidden}`"
        );
    }
    for forbidden in ["axum", "Router", "ReadRuntime", "ApiError"] {
        assert!(
            !dependency_tokens(&previews)
                .iter()
                .any(|token| token == forbidden),
            "Activity previews contains transport dependency `{forbidden}`"
        );
    }
    assert!(
        dependency_tokens(&previews)
            .iter()
            .any(|token| token == "event_totals_on"),
        "preview hydration must consume keyed Activity attribution"
    );

    let attribution = code_without_comments_and_literals(production_module_source(&source(
        &activity_root.join("attribution.rs"),
    )));
    assert!(
        !dependency_tokens(&attribution)
            .iter()
            .any(|token| token == "ActivityItem"),
        "Activity attribution must not mutate preview read models"
    );
}

#[test]
fn activity_groups_owns_descendant_summaries_and_lazy_placeholders() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let groups_source = source(&activity_root.join("groups.rs"));
    let groups_production = production_module_source(&groups_source);
    let groups = code_without_comments_and_literals(groups_production);
    for owned in [
        "struct ActivityDescendantGroup",
        "struct GroupSummaries",
        "fn load",
        "fn counts",
        "fn placeholders",
        "fn read_detail_on",
        "fn prepare_activity_group_turns",
        "struct ActivityGroupChildRef",
        "fn query_activity_group_child_page_on",
        "fn query_activity_group_child_rows",
        "fn activity_group_child_from_row",
        "fn query_activity_page_turn_totals_on",
        "fn query_activity_group_totals_on",
        "fn query_activity_group_status_on",
        "fn query_activity_group_labels_on",
        "fn query_activity_group_duration_on",
        "struct ActivityDurationAccumulator",
        "fn agent_labels_preview",
        "fn parse_id",
    ] {
        assert!(
            groups.contains(owned),
            "Activity groups does not own `{owned}`"
        );
    }
    for forbidden in [
        "api",
        "sessions",
        "web",
        "storage",
        "root_page",
        "detail",
        "routes",
    ] {
        assert!(
            !crate_dependency_roots(&groups)
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity groups imports forbidden outward dependency `{forbidden}`"
        );
    }
    for forbidden in ["axum", "Router", "ReadRuntime", "ApiError"] {
        assert!(
            !dependency_tokens(&groups)
                .iter()
                .any(|token| token == forbidden),
            "Activity groups contains transport dependency `{forbidden}`"
        );
    }

    assert!(
        groups_production.contains("selected_activity_group_turns"),
        "Activity groups must own its detail selection table"
    );
}

#[test]
fn activity_detail_owns_dispatch_and_exact_item_hydration() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module = code_without_comments_and_literals(&source(&activity_root.join("mod.rs")));
    assert!(
        module.contains("mod detail") && !module.contains("pub(crate) mod detail"),
        "Activity must privately own detail dispatch and hydration"
    );

    let detail_source = source(&activity_root.join("detail.rs"));
    let detail = code_without_comments_and_literals(production_module_source(&detail_source));
    for owned in [
        "struct DetailPage",
        "fn validate_cursor_for",
        "fn read_on",
        "EventUsageKey",
        "event_total_on",
        "turn_totals_on",
        "read_legacy_detail_on",
        "read_group_detail_on",
        "read_exchange",
    ] {
        assert!(
            dependency_tokens(&detail)
                .iter()
                .any(|token| token == owned)
                || detail.contains(owned),
            "Activity detail does not own or consume `{owned}`"
        );
    }
    for forbidden in ["api", "sessions", "web", "routes"] {
        assert!(
            !crate_dependency_roots(&detail)
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity detail imports forbidden outward dependency `{forbidden}`"
        );
    }
    for forbidden in ["axum", "Router", "ReadRuntime", "ApiError", "Json"] {
        assert!(
            !dependency_tokens(&detail)
                .iter()
                .any(|token| token == forbidden),
            "Activity detail contains transport dependency `{forbidden}`"
        );
    }

    let legacy = detail_source
        .find("item_id == legacy_activity_id")
        .expect("legacy detail dispatch exists");
    let reserved = detail_source
        .find("parse_group_id(item_id)")
        .expect("reserved-group detail dispatch exists");
    let turn = detail_source
        .find("if let Some(mut turn)")
        .expect("turn detail lookup exists");
    let event = detail_source
        .find("let event = connection")
        .expect("event detail lookup exists");
    assert!(
        legacy < reserved && reserved < turn && turn < event,
        "detail dispatch must reserve legacy/group IDs before turn and event lookup"
    );
}

#[test]
fn activity_index_is_owned_by_activity_without_a_root_facade() {
    let root = manifest_root();
    assert!(
        !root.join("src/activity_index.rs").exists(),
        "the legacy root Activity-index module must be deleted in the ownership move"
    );

    let lib = code_without_comments_and_literals(&source(&root.join("src/lib.rs")));
    assert!(
        !lib.contains("mod activity_index"),
        "lib.rs must not retain a root Activity-index facade"
    );
    let activity_root = root.join("src/activity");
    let module_source = source(&activity_root.join("mod.rs"));
    let module = code_without_comments_and_literals(&module_source);
    assert!(
        module.contains("mod index") && !module.contains("pub(crate) mod index"),
        "Activity must privately own its index implementation"
    );
    assert!(
        !dependency_tokens(&module)
            .iter()
            .any(|token| token == "validate_activity_index_cursor_for"),
        "Activity root must not expose the private event-cursor validator"
    );
    let detail = code_without_comments_and_literals(production_module_source(&source(
        &activity_root.join("detail.rs"),
    )));
    assert!(
        dependency_tokens(&detail)
            .iter()
            .any(|token| token == "validate_index_cursor_for"),
        "Activity detail must validate event cursors through the private index seam"
    );
    for retired_seam in ["IndexedActivityEvent", "query_activity_index_page"] {
        assert!(
            !dependency_tokens(&module)
                .iter()
                .any(|token| token == retired_seam),
            "Activity root still exposes internal preview/index seam `{retired_seam}`"
        );
    }

    let index_source = source(&activity_root.join("index.rs"));
    let index = code_without_comments_and_literals(production_module_source(&index_source));
    for owned in [
        "struct ActivityCursor",
        "struct IndexedActivityEvent",
        "struct ActivityIndexPage",
        "fn validate_cursor_for",
        "fn query_page",
        "fn query_after_cursor",
        "fn query_at_offset",
        "fn encode_cursor",
        "fn decode_cursor",
    ] {
        assert!(
            index.contains(owned),
            "Activity index does not own `{owned}`"
        );
    }
    for forbidden in ["api", "ingest", "sessions", "web"] {
        assert!(
            !crate_dependency_roots(&index)
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity index imports forbidden feature `{forbidden}`"
        );
    }
    for forbidden in ["axum", "Router", "Json"] {
        assert!(
            !dependency_tokens(&index)
                .iter()
                .any(|token| token == forbidden),
            "Activity index contains transport dependency `{forbidden}`"
        );
    }
}

#[test]
fn activity_model_owns_only_recursive_read_records() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module = code_without_comments_and_literals(&source(&activity_root.join("mod.rs")));
    assert!(
        module.contains("mod model"),
        "Activity must declare read-model ownership"
    );

    let model_source = source(&activity_root.join("model.rs"));
    let model = code_without_comments_and_literals(production_module_source(&model_source));
    for owned in [
        "struct ActivityItem",
        "struct ActivityCounts",
        "struct ActivityResponse",
        "struct ActivityDaySummary",
    ] {
        assert!(
            model.contains(owned),
            "Activity model does not own `{owned}`"
        );
    }
    assert_eq!(
        model.matches("struct ").count(),
        4,
        "Activity model must not become a drawer for query, cursor, or SQL row structs"
    );
    for forbidden in [
        "PageQuery",
        "ActivityDetailQuery",
        "ActivityCollectionCursor",
        "ActivityRootScope",
        "ActivityBatch",
        "ActivityChildrenPage",
        "Connection",
        "ReadRuntime",
        "ApiError",
        "Router",
        "Json",
        "rusqlite",
    ] {
        assert!(
            !dependency_tokens(&model)
                .iter()
                .any(|token| token == forbidden),
            "Activity model absorbed non-record ownership `{forbidden}`"
        );
    }
    for forbidden in ["api", "sessions", "web", "storage"] {
        assert!(
            !crate_dependency_roots(&model)
                .iter()
                .any(|dependency| dependency == forbidden),
            "Activity model imports forbidden dependency `{forbidden}`"
        );
    }
}

#[test]
fn activity_keeps_collection_and_event_cursor_protocols_distinct() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module = code_without_comments_and_literals(&source(&activity_root.join("mod.rs")));
    assert!(
        module.contains("mod cursor"),
        "Activity must declare collection-cursor ownership"
    );

    let cursor_source = source(&activity_root.join("cursor.rs"));
    let cursor = code_without_comments_and_literals(production_module_source(&cursor_source));
    for owned in [
        "struct ActivityCollectionCursor",
        "fn encode_activity_collection_cursor",
        "fn decode_activity_collection_cursor_for",
    ] {
        assert!(
            cursor.contains(owned),
            "Activity collection cursor does not own `{owned}`"
        );
    }
    for forbidden in ["Connection", "rusqlite", "Router", "Json", "ReadRuntime"] {
        assert!(
            !dependency_tokens(&cursor)
                .iter()
                .any(|token| token == forbidden),
            "Activity collection cursor absorbed `{forbidden}`"
        );
    }
    assert!(
        cursor_source.contains("rename_all = \"camelCase\""),
        "collection cursors must preserve their camelCase wire contract"
    );

    let index_source = source(&activity_root.join("index.rs"));
    let index = code_without_comments_and_literals(production_module_source(&index_source));
    assert!(
        index.contains("struct ActivityCursor")
            && !index_source.contains("rename_all = \"camelCase\""),
        "event cursors must remain the distinct snake_case index protocol"
    );
}

#[test]
fn activity_selection_owns_general_temp_table_preparation() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module = code_without_comments_and_literals(&source(&activity_root.join("mod.rs")));
    assert!(
        module.contains("mod selection") && !module.contains("pub(crate) mod selection"),
        "Activity must privately own prepared selection"
    );

    let selection_source = source(&activity_root.join("selection.rs"));
    let selection = code_without_comments_and_literals(production_module_source(&selection_source));
    for owned in [
        "struct ActivityRootScope",
        "struct PreparedSelection",
        "fn prepare",
    ] {
        assert!(
            selection.contains(owned),
            "Activity selection does not own `{owned}`"
        );
    }
    for table in [
        "selected_activity_roots",
        "selected_activity_turns",
        "selected_activity_descendants",
        "activity_explicit_agents",
        "selected_activity_agent_intervals",
    ] {
        for operation in ["CREATE TEMP TABLE IF NOT EXISTS", "DELETE FROM"] {
            assert!(
                selection_source.contains(&format!("{operation} {table}"))
                    || selection_source
                        .contains(&format!("{operation}\n+                 {table}")),
                "Activity selection does not own `{operation} {table}`"
            );
        }
    }
    for forbidden in [
        "selected_activity_group_turns",
        "axum",
        "Router",
        "Json",
        "ReadRuntime",
        "load_price_book_on",
    ] {
        assert!(
            !dependency_tokens(&selection)
                .iter()
                .any(|token| token == forbidden)
                && !selection_source.contains(forbidden),
            "Activity selection absorbed unrelated ownership `{forbidden}`"
        );
    }

    let root_page = source(&activity_root.join("root_page.rs"));
    let detail = source(&activity_root.join("detail.rs"));
    assert_eq!(
        root_page.matches("PreparedSelection::prepare").count(),
        1,
        "root-list reads must prepare one same-connection selection"
    );
    assert_eq!(
        detail.matches("PreparedSelection::prepare").count(),
        1,
        "root-detail reads must prepare one same-connection selection"
    );
}

#[test]
fn activity_attribution_returns_keyed_totals_without_mutating_read_models() {
    let root = manifest_root();
    let activity_root = root.join("src/activity");
    let module_source = source(&activity_root.join("mod.rs"));
    let module = code_without_comments_and_literals(&module_source);
    assert!(
        !dependency_tokens(&module)
            .iter()
            .any(|token| token == "SelectedActivityUsage"),
        "Activity root must not expose its internal keyed attribution result"
    );
    assert!(
        !dependency_tokens(&module)
            .iter()
            .any(|token| token == "ActivitySelection"),
        "the exceptional-pricing scalar leaked beyond attribution ownership"
    );

    let attribution_source = source(&activity_root.join("attribution.rs"));
    let attribution =
        code_without_comments_and_literals(production_module_source(&attribution_source));
    for owned in [
        "struct SelectedActivityUsage",
        "fn load",
        "fn root_totals",
        "fn group_totals",
        "struct ActivitySelection",
        "fn selected_rollup_cost_on",
    ] {
        assert!(
            attribution.contains(owned),
            "Activity attribution does not own `{owned}`"
        );
    }
    for consumer in ["root_page.rs", "groups.rs"] {
        let consumer = code_without_comments_and_literals(production_module_source(&source(
            &activity_root.join(consumer),
        )));
        assert!(
            dependency_tokens(&consumer)
                .iter()
                .any(|token| token == "SelectedActivityUsage"),
            "Activity `{consumer}` must consume keyed attribution directly"
        );
    }
    for forbidden in [
        "ActivityItem",
        "ActivityCounts",
        "ActivityResponse",
        "ActivityDaySummary",
        "ActivityRootAggregate",
        "ActivityDescendantGroup",
        "Router",
        "Json",
        "ReadRuntime",
    ] {
        assert!(
            !dependency_tokens(&attribution)
                .iter()
                .any(|token| token == forbidden),
            "Activity attribution mutates or imports unrelated model `{forbidden}`"
        );
    }
}

#[test]
fn storage_dependencies_point_inward() {
    let root = manifest_root();
    for path in rust_files(&root.join("src/storage")) {
        let violations = dependency_direction_violations(&source(&path), FEATURE_ROOTS);
        assert!(
            violations.is_empty(),
            "{} imports feature-layer ownership into storage: {}",
            relative(&path),
            violations.join(", ")
        );
    }

    let database =
        code_without_comments_and_literals(&source(&root.join("src/storage/database.rs")));
    for removed in [
        "ManualPricingStore",
        "open_with_pricing_config",
        "manual_pricing",
    ] {
        assert!(
            !database.contains(removed),
            "storage database retains Pricing ownership via `{removed}`"
        );
    }
}

#[test]
fn stage_one_application_state_is_construction_only() {
    let root = manifest_root();

    let app = code_without_comments_and_literals(&source(&root.join("src/app.rs")));
    assert!(app.contains("pub struct AppDependencies"));
    assert!(
        app.contains("pub fn build(self)"),
        "building the application must consume its dependencies"
    );
    assert!(
        !app.contains("derive(Clone") && !app.contains("impl Clone for AppDependencies"),
        "AppDependencies is construction state, not a cloneable service locator"
    );
    assert!(
        !app.contains("StorageExecutor::default"),
        "the composition root must not hide executor construction"
    );

    for path in rust_files(&root.join("src")) {
        let relative = relative(&path);
        if matches!(relative.as_str(), "src/app.rs" | "src/main.rs") {
            continue;
        }
        assert!(
            !code_without_comments_and_literals(&source(&path)).contains("AppDependencies"),
            "{relative} imports the construction-only AppDependencies"
        );
    }
}

#[test]
fn pricing_implementation_is_owned_only_by_the_pricing_module() {
    let root = manifest_root();
    assert!(
        !root.join("src/manual_pricing.rs").exists(),
        "the legacy root manual-pricing module must not survive as a compatibility facade"
    );
    assert!(
        !root.join("src/pricing.rs").exists(),
        "the legacy Pricing root must not survive as a compatibility facade"
    );
    let lib = code_without_comments_and_literals(&source(&root.join("src/lib.rs")));
    assert!(
        !lib.contains("mod manual_pricing"),
        "src/lib.rs still declares the legacy root manual-pricing module"
    );
    let pricing = code_without_comments_and_literals(&source(&root.join("src/pricing/mod.rs")));
    assert!(
        pricing.contains("mod catalog"),
        "Pricing must own price and alias catalog persistence"
    );
    assert!(
        pricing.contains("mod manual_store"),
        "Pricing must own the manual sidecar implementation"
    );
    assert!(
        pricing.contains("mod mutations"),
        "Pricing must own manual-mutation orchestration"
    );
    assert!(
        pricing.contains("mod routes"),
        "Pricing must own its state-bound HTTP routes"
    );
    assert!(
        pricing.contains("mod sync"),
        "Pricing must own the remote synchronization implementation"
    );
    assert!(
        !pricing.contains("fn ") && !pricing.contains("struct ") && !pricing.contains("impl "),
        "the Pricing module root must remain declarations and narrow re-exports only"
    );
    for path in rust_files(&root.join("src")) {
        assert!(
            !code_without_comments_and_literals(&source(&path)).contains("crate::manual_pricing"),
            "{} imports the removed root manual-pricing module",
            relative(&path)
        );
    }

    let app = code_without_comments_and_literals(&source(&root.join("src/app.rs")));
    assert!(
        app.contains("pricing::router") && !app.contains("api::pricing_router"),
        "the composition root must mount the Pricing-owned router directly"
    );

    let catalog_source = source(&root.join("src/pricing/catalog.rs"));
    let catalog = code_without_comments_and_literals(production_module_source(&catalog_source));
    for forbidden in ["axum", "serde", "ReadRuntime"] {
        assert!(
            !catalog.contains(forbidden),
            "the Pricing catalog leaks transport ownership via `{forbidden}`"
        );
    }
    for transport in [
        "PriceRow",
        "AliasRow",
        "UnknownModelRow",
        "PricesResponse",
        "AliasesResponse",
        "PriceMetadataResponse",
        "PriceModelIdsResponse",
    ] {
        assert!(
            !pricing.contains(transport),
            "Pricing exports the transport DTO `{transport}` from its module root"
        );
    }
}

#[test]
fn system_implementation_is_owned_only_by_the_system_module() {
    let root = manifest_root();
    let system_root = root.join("src/system");
    for path in rust_files(&system_root) {
        let violations =
            dependency_direction_violations(&source(&path), &["pricing", "analytics", "ingest"]);
        assert!(
            violations.is_empty(),
            "{} imports an upper-level feature into System: {}",
            relative(&path),
            violations.join(", ")
        );
    }

    let system = code_without_comments_and_literals(&source(&system_root.join("mod.rs")));
    for owned in ["mod routes", "mod settings", "mod status"] {
        assert!(
            system.contains(owned),
            "System does not declare its `{owned}` ownership"
        );
    }
    assert!(
        !system.contains("fn ") && !system.contains("struct ") && !system.contains("impl "),
        "the System module root must remain declarations and a narrow router re-export"
    );
    let routes_source = source(&system_root.join("routes.rs"));
    let routes = code_without_comments_and_literals(production_module_source(&routes_source));
    assert!(
        !routes.contains("Db"),
        "System routes must derive database metadata and snapshots from one ReadRuntime"
    );

    let app = code_without_comments_and_literals(&source(&root.join("src/app.rs")));
    assert!(
        app.contains("system::router") && !app.contains("api::system_router"),
        "the composition root must mount the System-owned router directly"
    );
}

#[test]
fn ingest_token_usage_is_owned_by_protocol_tokens_without_a_root_facade() {
    let root = manifest_root();
    let src = root.join("src");
    assert!(
        !src.join("model.rs").exists(),
        "the moved ingestion protocol value must not retain a root model facade"
    );
    let lib = code_without_comments_and_literals(&source(&src.join("lib.rs")));
    assert!(
        !lib.contains("mod model"),
        "the crate root still declares the deleted generic model module"
    );

    let tokens =
        code_without_comments_and_literals(&source(&src.join("ingest/protocol/tokens.rs")));
    assert!(
        tokens.contains("struct TokenUsage"),
        "ingestion protocol tokens must own TokenUsage"
    );
    let owners = rust_files(&src)
        .into_iter()
        .filter(|path| {
            let contents = source(path);
            production_module_source(&contents).contains("struct TokenUsage")
        })
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(owners, vec!["src/ingest/protocol/tokens.rs"]);

    let composition_source = ingest_composition_source(&src);
    let composition =
        code_without_comments_and_literals(production_module_source(&composition_source));
    assert!(!composition.contains("struct TokenUsage"));
    assert!(!composition.contains("model::TokenUsage"));
    assert!(!composition.contains("crate::model"));
}

#[test]
fn ingest_protocol_values_are_pure_and_single_owned() {
    let root = manifest_root();
    let src = root.join("src");
    let protocol_root = src.join("ingest/protocol");
    for path in rust_files(&protocol_root) {
        let contents = source(&path);
        let production = production_module_source(&contents);
        let dependencies = crate_dependency_roots(production);
        let allowed_dependencies = [
            "ingest",
            "calendar",
            "redaction",
            "MAX_PUBLIC_YEAR",
            "MIN_PUBLIC_YEAR",
            "MAX_USAGE_TOKENS_PER_FACT",
        ];
        assert!(
            dependencies
                .iter()
                .all(|dependency| allowed_dependencies.contains(&dependency.as_str())),
            "{} imports a non-neutral crate dependency into pure ingestion protocol: {}",
            relative(&path),
            dependencies.join(", ")
        );
        assert!(!production.contains("crate::ingest::"));
        assert!(!production.contains("super::super::projection"));
        assert!(!production.contains("super::super::source"));
    }

    let module = code_without_comments_and_literals(&source(&protocol_root.join("mod.rs")));
    for owned in [
        "mod content",
        "mod decode",
        "mod duration",
        "mod identifiers",
        "mod intent",
        "mod metadata",
        "mod state",
        "mod timestamp",
        "mod tokens",
        "mod wire",
    ] {
        assert!(
            module.contains(owned),
            "protocol root does not declare `{owned}`"
        );
    }
    for exported in ["CursorState", "OwnerMeta"] {
        assert!(
            module.contains(exported),
            "protocol root does not narrowly expose `{exported}` to ingestion"
        );
    }

    let expected = [
        ("CursorState", "src/ingest/protocol/state.rs"),
        ("TokenUsage", "src/ingest/protocol/tokens.rs"),
        ("OwnerMeta", "src/ingest/protocol/metadata.rs"),
        ("SessionMetadata", "src/ingest/protocol/metadata.rs"),
    ];
    for (name, expected_owner) in expected {
        let needle = format!("struct {name}");
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(&needle)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(owners, [expected_owner], "unexpected owners for {name}");
    }
}

#[test]
fn ingest_record_routing_and_projection_dispatch_are_single_owned() {
    let src = manifest_root().join("src");
    let protocol_record_path = src.join("ingest/protocol/decode/record.rs");
    let projection_record_path = src.join("ingest/projection/record.rs");
    assert_eq!(
        assigned_role(&protocol_record_path),
        Some(Role::IngestProtocol)
    );
    assert_eq!(
        assigned_role(&projection_record_path),
        Some(Role::IngestProjection)
    );

    let protocol_record_source = source(&protocol_record_path);
    let raw_protocol_record = production_module_source(&protocol_record_source);
    let protocol_record = code_without_comments_and_literals(raw_protocol_record);
    assert!(raw_protocol_record.contains("fn decode_record("));
    assert!(raw_protocol_record.contains("value: &Value"));
    assert!(raw_protocol_record.contains("Result<DecodedRecord>"));
    for forbidden in [
        "rusqlite",
        "ProjectionContext",
        "ProjectionTx",
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
    ] {
        assert!(
            !raw_protocol_record.contains(forbidden),
            "the closed Protocol router crosses into Projection via `{forbidden}`"
        );
    }

    let decode_record_owners = rust_files(&src)
        .into_iter()
        .filter(|path| production_module_source(&source(path)).contains("fn decode_record("))
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(
        decode_record_owners,
        ["src/ingest/protocol/decode/record.rs"],
        "the complete Protocol router must have one owner"
    );

    let family_decoders = [
        (
            "decode_usage_record(",
            "src/ingest/protocol/decode/usage.rs",
        ),
        (
            "decode_session_metadata_record(",
            "src/ingest/protocol/decode/metadata.rs",
        ),
        (
            "decode_title_event_record(",
            "src/ingest/protocol/decode/metadata.rs",
        ),
        (
            "decode_ordinary_record(",
            "src/ingest/protocol/decode/ordinary.rs",
        ),
        (
            "decode_thread_state_record(",
            "src/ingest/protocol/decode/thread_state.rs",
        ),
        (
            "decode_conversation_record(",
            "src/ingest/protocol/decode/conversation.rs",
        ),
        (
            "decode_response_tool_record(",
            "src/ingest/protocol/decode/tools.rs",
        ),
        (
            "decode_event_tool_record(",
            "src/ingest/protocol/decode/tools.rs",
        ),
        (
            "decode_lifecycle_record(",
            "src/ingest/protocol/decode/lifecycle.rs",
        ),
        (
            "decode_agent_record(",
            "src/ingest/protocol/decode/agents.rs",
        ),
    ];
    for (decoder, family_owner) in family_decoders {
        let owners = rust_files(&src.join("ingest/protocol/decode"))
            .into_iter()
            .filter(|path| production_module_source(&source(path)).contains(decoder))
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        let mut expected = vec![
            family_owner.to_owned(),
            "src/ingest/protocol/decode/record.rs".to_owned(),
        ];
        expected.sort();
        assert_eq!(
            owners, expected,
            "`{decoder}` must be declared by its family and called only by the closed router"
        );
    }

    let mut previous = protocol_record
        .find("fn decode_record(")
        .expect("Protocol must own the closed record router");
    for step in [
        "decode_usage_record(",
        "if routing_state.forked && !routing_state.native_started",
        "CursorOnlyReason::InheritedForkReplay",
        "decode_session_metadata_record(",
        "CursorOnlyReason::AwaitingNativeStart",
        "decode_title_event_record(",
        "decode_ordinary_record(",
        "decode_thread_state_record(",
        "decode_conversation_record(",
        "decode_response_tool_record(",
        "decode_event_tool_record(",
        "decode_lifecycle_record(",
        "decode_agent_record(",
    ] {
        let current = protocol_record[previous..]
            .find(step)
            .map(|offset| previous + offset)
            .unwrap_or_else(|| panic!("Protocol record routing lost `{step}`"));
        assert!(
            current >= previous,
            "Protocol record routing reordered `{step}`"
        );
        previous = current + step.len();
    }

    let intent_source = source(&src.join("ingest/protocol/intent.rs"));
    let intent = code_without_comments_and_literals(production_module_source(&intent_source));
    for forbidden in ["serde_json", "Value", "payload_json", "Vec<u8>"] {
        assert!(
            !intent.contains(forbidden),
            "typed protocol records retain raw source material via `{forbidden}`"
        );
    }

    let projection_record_source = source(&projection_record_path);
    let raw_projection_record = production_module_source(&projection_record_source);
    let projection_record = code_without_comments_and_literals(raw_projection_record);
    for forbidden in ["serde_json", "Value", "WireRecord", "decode_record("] {
        assert!(
            !projection_record.contains(forbidden),
            "Projection record dispatch reaches back into raw Protocol via `{forbidden}`"
        );
    }
    for required in [
        "struct ProjectionContext",
        "match record",
        "DecodedRecord::Usage",
        "DecodedRecord::CursorOnly",
        "DecodedRecord::Metadata",
        "DecodedRecord::Ordinary",
        "DecodedRecord::ThreadState",
        "DecodedRecord::Conversation",
        "DecodedRecord::Tool",
        "DecodedRecord::Lifecycle",
        "DecodedRecord::Agent",
    ] {
        assert!(
            projection_record.contains(required),
            "Projection record dispatch does not own `{required}`"
        );
    }
    let compact_projection = projection_record.split_whitespace().collect::<String>();
    assert!(
        !compact_projection.contains("_=>"),
        "DecodedRecord dispatch must remain exhaustive instead of accepting a wildcard"
    );
    let prepare = projection_record
        .find("let mut candidate = self.state.clone()")
        .expect("ProjectionContext must prepare one complete candidate cursor");
    let dispatch = projection_record
        .find("apply_to_candidate(self.tx, &mut candidate, &record)")
        .expect("ProjectionContext must dispatch the typed record against the candidate cursor");
    let publish = projection_record
        .find("*self.state = candidate")
        .expect("ProjectionContext must publish only after dispatch succeeds");
    assert!(
        prepare < dispatch && dispatch < publish,
        "ProjectionContext must prepare, dispatch, then publish cursor state"
    );

    let context_owners = rust_files(&src)
        .into_iter()
        .filter(|path| production_module_source(&source(path)).contains("struct ProjectionContext"))
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(
        context_owners,
        ["src/ingest/projection/record.rs"],
        "ProjectionContext must have one owner"
    );
    let dispatch_owners = rust_files(&src.join("ingest/projection"))
        .into_iter()
        .filter(|path| production_module_source(&source(path)).contains("DecodedRecord::"))
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(
        dispatch_owners,
        ["src/ingest/projection/record.rs"],
        "DecodedRecord variants must be dispatched in one Projection module"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for removed in [
        "project_record",
        "decode_usage_record",
        "decode_session_metadata_record",
        "decode_title_event_record",
        "decode_ordinary_record",
        "decode_thread_state_record",
        "decode_conversation_record",
        "decode_response_tool_record",
        "decode_event_tool_record",
        "decode_lifecycle_record",
        "decode_agent_record",
        "apply_record",
        "apply_metadata_record",
        "apply_ordinary_record",
        "apply_thread_state_record",
        "apply_conversation_record",
        "apply_tool_record",
        "apply_lifecycle_record",
        "apply_agent_record",
    ] {
        assert!(
            !composition.contains(removed),
            "the ingestion module root retains record routing or dispatch via `{removed}`"
        );
    }
    assert!(
        !composition.contains("decode_record(") && !composition.contains(".apply(decoded)"),
        "scan coordination must not retain per-record routing"
    );
    let file_ingestor_source = source(&src.join("ingest/file_ingestor.rs"));
    let file_ingestor =
        code_without_comments_and_literals(production_module_source(&file_ingestor_source));
    assert_eq!(
        file_ingestor.matches("decode_record(").count(),
        1,
        "FileIngestor must decode each source record through one Protocol call"
    );
    assert_eq!(
        file_ingestor.matches(".apply(decoded)").count(),
        1,
        "FileIngestor must project each decoded record through one context call"
    );
    let decode = file_ingestor
        .find("decode_record(&state, source_line, &value)")
        .expect("FileIngestor must call the closed Protocol router");
    let context = file_ingestor[decode..]
        .find(".context(&mut state)")
        .map(|offset| decode + offset)
        .expect("the projection transaction must create a cursor context");
    let apply = file_ingestor[context..]
        .find(".apply(decoded)")
        .map(|offset| context + offset)
        .expect("ProjectionContext must receive the typed decoded record");
    assert!(
        decode < context && context < apply,
        "FileIngestor must decode once, create ProjectionContext, then apply once"
    );
}

#[test]
fn ingest_usage_crosses_a_typed_protocol_projection_seam() {
    let src = manifest_root().join("src");
    let intent =
        code_without_comments_and_literals(&source(&src.join("ingest/protocol/intent.rs")));
    for forbidden in ["serde_json", "Value", "payload_json", "Vec<u8>"] {
        assert!(
            !intent.contains(forbidden),
            "typed protocol intents retain raw source material via `{forbidden}`"
        );
    }

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for removed in [
        "decode_token_accounting",
        "MODEL_ATTRIBUTION_REQUIRED_FROM_YEAR",
    ] {
        assert!(
            !composition.contains(removed),
            "the ingestion module root retains moved Usage policy via `{removed}`"
        );
    }
    assert!(!raw_composition.contains("INSERT INTO usage_facts"));

    let usage_source = source(&src.join("ingest/projection/usage.rs"));
    let raw_usage = production_module_source(&usage_source);
    let usage = code_without_comments_and_literals(raw_usage);
    assert!(usage_source.contains("INSERT INTO usage_facts"));
    assert!(!usage.contains("serde_json"));
    assert!(!usage.contains("SourceSnapshot"));

    let usage_insert_owners = rust_files(&src)
        .into_iter()
        .filter(|path| production_module_source(&source(path)).contains("INSERT INTO usage_facts"))
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(
        usage_insert_owners,
        ["src/ingest/projection/usage.rs"],
        "Usage fact persistence must have one Projection owner"
    );
}

#[test]
fn ingest_events_cross_a_bounded_typed_protocol_projection_seam() {
    let src = manifest_root().join("src");
    let protocol_source = source(&src.join("ingest/protocol/event.rs"));
    let event_start = protocol_source
        .find("struct ProjectedEvent")
        .expect("Protocol must own the typed projected-event value");
    let event_end = protocol_source[event_start..]
        .find("enum ProjectedCallId")
        .map(|offset| event_start + offset)
        .expect("the typed call identity must follow ProjectedEvent");
    let event_value = &protocol_source[event_start..event_end];
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !event_value.contains(forbidden),
            "ProjectedEvent retains raw source material via `{forbidden}`"
        );
    }
    assert!(protocol_source.contains("fn shape_projected_event"));
    assert!(!protocol_source.contains("rusqlite"));
    assert!(!protocol_source.contains("INSERT INTO events"));

    let projection_source = source(&src.join("ingest/projection/events.rs"));
    let raw_projection = production_module_source(&projection_source);
    assert!(raw_projection.contains("INSERT OR IGNORE INTO events("));
    assert!(!raw_projection.contains("EventDraft"));
    assert!(!raw_projection.contains("SourceSnapshot"));

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for removed in [
        "fn insert_event",
        "fn compact_projected_metadata",
        "fn compact_unknown_metadata",
        "fn compact_compaction",
    ] {
        assert!(
            !composition.contains(removed),
            "the ingestion module root retains moved event policy via `{removed}`"
        );
    }
    assert_eq!(
        raw_composition
            .matches("INSERT OR IGNORE INTO events(")
            .count(),
        0,
        "the ingestion module root must not retain specialized event insertion"
    );
    let lifecycle_source = source(&src.join("ingest/projection/lifecycle.rs"));
    let raw_lifecycle = production_module_source(&lifecycle_source);
    assert_eq!(
        raw_lifecycle
            .matches("INSERT OR IGNORE INTO events(")
            .count(),
        1,
        "Lifecycle Projection must own the specialized implicit-interruption evidence"
    );
    assert!(raw_lifecycle.contains("'Turn interrupted'"));
}

#[test]
fn ingest_ordinary_records_cross_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/ordinary.rs"));
    let value_start = decoder_source
        .find("struct DecodedOrdinaryRecord")
        .expect("Protocol must own the decoded ordinary-record value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_ordinary_record")
        .map(|offset| value_start + offset)
        .expect("ordinary-record values must precede their decoder");
    let typed_values = &decoder_source[value_start..value_end];
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !typed_values.contains(forbidden),
            "ordinary-record intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("INSERT INTO"));
    for reserved in [
        "is_deferred_top_level_kind",
        "is_deferred_event_kind",
        "task_started",
        "sub_agent_activity",
        "exec_command_end",
    ] {
        assert!(
            decoder_source.contains(reserved),
            "ordinary decoding does not explicitly reserve `{reserved}` for its future owner"
        );
    }

    let projection_source = source(&src.join("ingest/projection/ordinary.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot"] {
        assert!(
            !raw_projection.contains(forbidden),
            "ordinary Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    assert!(raw_projection.contains("UPDATE turns SET model=COALESCE"));
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Projection must prepare a candidate cursor");
    let project = raw_projection
        .find("match &record.intent")
        .expect("Projection must apply the typed intent");
    let touch = raw_projection
        .find("lifecycle::touch_owner")
        .expect("ordinary records must retain owner activity semantics");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Projection must publish its candidate after successful writes");
    assert!(
        prepare < project && project < touch && touch < publish,
        "ordinary cursor state must be prepared, projected, touched, then published"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    for removed in [
        "thread_settings_applied",
        "item_completed",
        "entered_review_mode",
        "view_image_tool_call",
    ] {
        assert!(
            !raw_composition.contains(removed),
            "the ingestion module root retains moved ordinary policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_thread_state_records_cross_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/thread_state.rs"));
    let value_start = decoder_source
        .find("struct DecodedThreadStateRecord")
        .expect("Protocol must own the decoded thread-state value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_thread_state_record")
        .map(|offset| value_start + offset)
        .expect("thread-state values must precede their decoder");
    let typed_values = code_without_comments_and_literals(&decoder_source[value_start..value_end]);
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !typed_values.contains(forbidden),
            "thread-state intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("SELECT "));
    assert!(decoder_source.contains("shape_projected_event"));

    let projection_source = source(&src.join("ingest/projection/thread_state.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot", "Value"] {
        assert!(
            !raw_projection.contains(forbidden),
            "Thread-state Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    assert!(raw_projection.contains("kind='goal'"));
    assert!(raw_projection.contains("julianday(timestamp)-julianday(?2)"));
    assert!(raw_projection.contains("<1.0"));
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Thread-state Projection must prepare a candidate cursor");
    let project = raw_projection
        .find("match &record.intent")
        .expect("Thread-state Projection must apply the typed intent");
    let touch = raw_projection
        .find("lifecycle::touch_owner")
        .expect("thread-state records must retain owner activity semantics");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Thread-state Projection must publish after successful writes");
    assert!(
        prepare < project && project < touch && touch < publish,
        "thread-state cursor state must be prepared, projected, touched, then published"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    for removed in [
        "\"thread_goal_updated\"",
        "\"context_compacted\"",
        "kind='goal'",
        "julianday(timestamp)-julianday",
    ] {
        assert!(
            !raw_composition.contains(removed),
            "the ingestion module root retains moved thread-state policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_metadata_and_title_records_cross_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/metadata.rs"));
    let value_start = decoder_source
        .find("struct DecodedMetadataRecord")
        .expect("Protocol must own the decoded metadata-record value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_session_metadata_record")
        .map(|offset| value_start + offset)
        .expect("metadata values must precede their decoder");
    let typed_values = code_without_comments_and_literals(&decoder_source[value_start..value_end]);
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !typed_values.contains(forbidden),
            "metadata intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("UPDATE threads"));

    let projection_source = source(&src.join("ingest/projection/metadata.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot", "Value"] {
        assert!(
            !raw_projection.contains(forbidden),
            "Metadata Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    assert!(raw_projection.contains("root_metadata_seen=MAX(root_metadata_seen,?1)"));
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Metadata Projection must prepare a candidate cursor");
    let project = raw_projection
        .find("match &record.intent")
        .expect("Metadata Projection must apply the typed intent");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Metadata Projection must publish after successful writes");
    assert!(
        prepare < project && project < publish,
        "metadata cursor state must be prepared, projected, then published"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    for removed in [
        "fn update_owner_metadata",
        "title_updated_at IS NULL OR title_updated_at<=",
        "title=COALESCE(title",
    ] {
        assert!(
            !raw_composition.contains(removed),
            "the ingestion module root retains moved Metadata policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_conversation_records_cross_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/conversation.rs"));
    let value_start = decoder_source
        .find("struct DecodedConversationRecord")
        .expect("Protocol must own the decoded conversation-record value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_conversation_record")
        .map(|offset| value_start + offset)
        .expect("conversation values must precede their decoder");
    let typed_values = code_without_comments_and_literals(&decoder_source[value_start..value_end]);
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !typed_values.contains(forbidden),
            "conversation intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("INSERT INTO"));

    let projection_source = source(&src.join("ingest/projection/conversation.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot", "Value"] {
        assert!(
            !raw_projection.contains(forbidden),
            "conversation Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    assert!(raw_projection.contains("INSERT OR IGNORE INTO messages("));
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Conversation Projection must prepare a candidate cursor");
    let project = raw_projection
        .find("match &record.intent")
        .expect("Conversation Projection must apply the typed intent");
    let touch = raw_projection
        .find("lifecycle::touch_owner")
        .expect("conversation records must retain owner activity semantics");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Conversation Projection must publish after successful writes");
    assert!(
        prepare < project && project < touch && touch < publish,
        "conversation cursor state must be prepared, projected, touched, then published"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for removed in [
        "fn project_response_item",
        "fn turn_accepts_metadata_free_feedback",
        "fn reopen_provisionally_completed_turn",
        "fn complete_turn_from_final",
        "INSERT OR IGNORE INTO messages(",
    ] {
        assert!(
            !raw_composition.contains(removed) && !composition.contains(removed),
            "the ingestion module root retains moved Conversation policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_native_lifecycle_crosses_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/lifecycle.rs"));
    let value_start = decoder_source
        .find("struct DecodedLifecycleRecord")
        .expect("Protocol must own the decoded lifecycle-record value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_lifecycle_record")
        .map(|offset| value_start + offset)
        .expect("lifecycle values must precede their decoder");
    let typed_values = code_without_comments_and_literals(&decoder_source[value_start..value_end]);
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !typed_values.contains(forbidden),
            "lifecycle intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("INSERT INTO"));

    let projection_source = source(&src.join("ingest/projection/lifecycle.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot", "Value"] {
        assert!(
            !raw_projection.contains(forbidden),
            "Lifecycle Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    for owned in [
        "fn turn_has_open_native_lifecycle",
        "fn complete_turn_from_final",
        "fn record_implicit_turn_interruption",
        "UPDATE turns SET completed_at=?1,status='completed'",
        "UPDATE turns SET completed_at=?1,status=?2",
    ] {
        assert!(
            raw_projection.contains(owned),
            "Lifecycle Projection does not own `{owned}`"
        );
    }
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Lifecycle Projection must prepare a candidate cursor");
    let project = raw_projection
        .find("match &record.intent")
        .expect("Lifecycle Projection must apply the typed intent");
    let touch = raw_projection
        .find("touch_owner(tx, &candidate")
        .expect("Lifecycle Projection must touch owner activity after lifecycle writes");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Lifecycle Projection must publish after successful writes");
    assert!(
        prepare < project && project < touch && touch < publish,
        "lifecycle cursor state must be prepared, projected, touched, then published"
    );

    let conversation_source = source(&src.join("ingest/projection/conversation.rs"));
    let raw_conversation = production_module_source(&conversation_source);
    assert!(!raw_conversation.contains("fn turn_has_open_native_lifecycle"));
    assert!(!raw_conversation.contains("fn complete_turn_from_final"));
    assert!(raw_conversation.contains("lifecycle::turn_has_open_native_lifecycle"));
    assert!(raw_conversation.contains("lifecycle::complete_turn_from_final"));

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for removed in [
        "fn turn_has_open_native_lifecycle",
        "fn record_implicit_turn_interruption",
        "\"task_started\" =>",
        "\"task_complete\" =>",
        "\"turn_aborted\" =>",
        "\"thread_rolled_back\" =>",
    ] {
        assert!(
            !raw_composition.contains(removed) && !composition.contains(removed),
            "the ingestion module root retains moved lifecycle policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_agent_observations_cross_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/agents.rs"));
    let value_start = decoder_source
        .find("struct DecodedAgentRecord")
        .expect("Protocol must own the decoded agent-record value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_agent_record")
        .map(|offset| value_start + offset)
        .expect("agent-record values must precede their decoder");
    let typed_values = code_without_comments_and_literals(&decoder_source[value_start..value_end]);
    for forbidden in ["serde_json", "Value", "payload", "source_bytes"] {
        assert!(
            !typed_values.contains(forbidden),
            "agent-record intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("INSERT INTO"));
    for owned in [
        "sub_agent_activity",
        "agent_thread_id",
        "ObservedAgentActivity",
        "PROJECTED_SESSION_PATH_CHARS",
    ] {
        assert!(
            decoder_source.contains(owned),
            "Agent Protocol does not own `{owned}`"
        );
    }

    let projection_source = source(&src.join("ingest/projection/agents.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot", "Value"] {
        assert!(
            !raw_projection.contains(forbidden),
            "Agent Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    for owned in [
        "INSERT INTO agent_runs(",
        "SELECT rollout_id,status,started_at",
        "parent_terminal_is_authoritative",
        "json_extract(payload_json,'$.agent_thread_id')",
        "started_at=MIN(started_at,?1)",
        "fn upsert_native_run(",
        "fn rematerialize_surviving_observation(",
        "fn rematerialize_observed_children(",
        "INDEXED BY idx_events_activity_owner",
    ] {
        assert!(
            raw_projection.contains(owned),
            "Agent Projection does not own `{owned}`"
        );
    }
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Agent Projection must prepare a candidate cursor");
    let event = raw_projection
        .find("events::apply(")
        .expect("Agent Projection must persist observation evidence");
    let reconcile = raw_projection
        .find("apply_observation(")
        .expect("Agent Projection must reconcile child lifecycle after event persistence");
    let touch = raw_projection
        .find("lifecycle::touch_owner")
        .expect("Agent Projection must touch owner activity");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Agent Projection must publish after successful writes");
    assert!(
        prepare < event && event < reconcile && reconcile < touch && touch < publish,
        "agent state must be prepared, observed, reconciled, touched, then published"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for removed in [
        "fn project_event_message(",
        "fn upsert_observed_agent(",
        "fn rematerialize_surviving_agent_observation(",
        "fn rematerialize_observed_children(",
        "fn restore_promoted_agent_native_state(",
        "fn upsert_owner(",
        "fn recompute_thread_bounds(",
        "sub_agent_activity",
    ] {
        assert!(
            !raw_composition.contains(removed) && !composition.contains(removed),
            "the ingestion module root retains moved agent policy via `{removed}`"
        );
    }

    let metadata_source = source(&src.join("ingest/projection/metadata.rs"));
    let raw_metadata = production_module_source(&metadata_source);
    assert!(raw_metadata.contains("fn upsert_owner("));
    assert!(raw_metadata.contains("agents::upsert_native_run(tx, owner)"));
    assert!(raw_metadata.contains("fn recompute_thread_bounds("));
}

#[test]
fn ingest_tool_records_cross_a_closed_typed_projection_seam() {
    let src = manifest_root().join("src");
    let decoder_source = source(&src.join("ingest/protocol/decode/tools.rs"));
    let value_start = decoder_source
        .find("struct DecodedToolRecord")
        .expect("Protocol must own the decoded tool-record value");
    let value_end = decoder_source[value_start..]
        .find("fn decode_response_tool_record")
        .map(|offset| value_start + offset)
        .expect("tool values must precede their decoders");
    let typed_values = code_without_comments_and_literals(&decoder_source[value_start..value_end]);
    for forbidden in [
        "serde_json",
        "Value",
        "payload",
        "arguments",
        "output",
        "source_bytes",
    ] {
        assert!(
            !typed_values.contains(forbidden),
            "tool intents retain raw source material via `{forbidden}`"
        );
    }
    assert!(!decoder_source.contains("rusqlite"));
    assert!(!decoder_source.contains("INSERT INTO"));

    let projection_source = source(&src.join("ingest/projection/tools.rs"));
    let raw_projection = production_module_source(&projection_source);
    for forbidden in ["serde_json", "EventDraft", "SourceSnapshot", "Value"] {
        assert!(
            !raw_projection.contains(forbidden),
            "Tools Projection reaches across its typed boundary via `{forbidden}`"
        );
    }
    assert!(raw_projection.contains("INSERT INTO tool_calls("));
    let prepare = raw_projection
        .find("record.transition.apply_to(&mut candidate)")
        .expect("Tools Projection must prepare a candidate cursor");
    let project = raw_projection
        .find("match &record.intent")
        .expect("Tools Projection must apply the typed intent");
    let touch = raw_projection
        .find("lifecycle::touch_owner")
        .expect("tool records must retain owner activity semantics");
    let publish = raw_projection
        .find("*state = candidate")
        .expect("Tools Projection must publish after successful writes");
    assert!(
        prepare < project && project < touch && touch < publish,
        "tool cursor state must be prepared, projected, touched, then published"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    for removed in [
        "fn upsert_tool_call",
        "fn complete_tool_call",
        "fn enrich_tool_call",
        "INSERT INTO tool_calls(",
    ] {
        assert!(
            !raw_composition.contains(removed),
            "the ingestion module root retains moved Tools policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_source_primitives_are_single_owned_and_descriptor_safe() {
    let root = manifest_root();
    let src = root.join("src");
    let source_path = src.join("ingest/source.rs");
    assert_eq!(
        assigned_role(&source_path),
        Some(Role::IngestSource),
        "the bounded reader must live in the declared ingestion-source boundary"
    );

    let source_module =
        code_without_comments_and_literals(production_module_source(&source(&source_path)));
    for owned in [
        "enum BoundedLine",
        "fn read_bounded_line",
        "struct FileIdentity",
        "fn file_identity",
        "struct CapturedExtent",
        "struct SourceSnapshot",
        "struct CapturedJsonlReader",
    ] {
        assert!(
            source_module.contains(owned),
            "ingestion source does not own `{owned}`"
        );
    }

    let composition_source = ingest_composition_source(&src);
    let composition =
        code_without_comments_and_literals(production_module_source(&composition_source));
    assert!(composition.contains("mod source"));
    assert!(!composition.contains("enum BoundedLine"));
    assert!(!composition.contains("fn read_bounded_line"));
    assert!(!composition.contains("struct FileIdentity"));
    assert!(!composition.contains("fn file_identity"));
    assert!(!composition.contains("struct SourceSnapshot"));
    assert!(
        !composition.contains("_from_file"),
        "the ingestion module root still bypasses SourceSnapshot through a legacy File helper"
    );
    let file_ingestor_source = source(&src.join("ingest/file_ingestor.rs"));
    let file_ingestor =
        code_without_comments_and_literals(production_module_source(&file_ingestor_source));
    assert!(
        file_ingestor.contains("SourceSnapshot::open"),
        "descriptor capture must remain in FileIngestor"
    );
    assert!(
        !file_ingestor.contains("_from_file"),
        "FileIngestor bypasses SourceSnapshot through a legacy File helper"
    );

    let owners = rust_files(&src)
        .into_iter()
        .filter(|path| {
            let contents = source(path);
            production_module_source(&contents).contains("struct SourceSnapshot")
        })
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(owners, ["src/ingest/source.rs"]);

    let visible_signatures = source_module
        .lines()
        .fold((Vec::<String>::new(), None::<String>), |mut state, line| {
            let trimmed = line.trim_start();
            if state.1.is_none() && (trimmed.starts_with("pub ") || trimmed.starts_with("pub(")) {
                state.1 = Some(trimmed.to_owned());
            } else if let Some(signature) = &mut state.1 {
                signature.push(' ');
                signature.push_str(trimmed);
            }
            if state.1.as_ref().is_some_and(|signature| {
                signature.contains('{') || signature.contains(';') || signature.contains(',')
            }) {
                state.0.push(state.1.take().unwrap());
            }
            state
        })
        .0;
    for signature in &visible_signatures {
        assert!(
            !dependency_tokens(signature)
                .iter()
                .any(|token| token == "File"),
            "ingestion source exposes its descriptor through `{signature}`"
        );
        for escape in ["into_inner", "get_ref", "get_mut", "as_file", "file_mut"] {
            assert!(
                !dependency_tokens(signature)
                    .iter()
                    .any(|token| token == escape),
                "ingestion source exposes descriptor escape `{escape}` through `{signature}`"
            );
        }
    }

    let source_tokens = dependency_tokens(&source_module);
    for (index, token) in source_tokens.iter().enumerate() {
        if token != "impl" {
            continue;
        }
        let header = source_tokens[index..]
            .iter()
            .take_while(|token| token.as_str() != "{")
            .collect::<Vec<_>>();
        if !header
            .iter()
            .any(|token| token.as_str() == "SourceSnapshot")
        {
            continue;
        }
        for escape_trait in ["Deref", "DerefMut", "AsRef", "AsMut", "Borrow", "BorrowMut"] {
            assert!(
                !header.iter().any(|token| token.as_str() == escape_trait),
                "SourceSnapshot implements descriptor escape trait `{escape_trait}`"
            );
        }
    }

    assert!(!composition.contains("pub mod source"));
    assert!(!composition.contains("pub use source"));
}

#[test]
fn ingest_checkpoint_policy_is_single_owned_and_private_to_ingestion() {
    let root = manifest_root();
    let src = root.join("src");
    let checkpoints_path = src.join("ingest/checkpoints.rs");
    assert_eq!(
        assigned_role(&checkpoints_path),
        Some(Role::IngestCheckpoints),
        "checkpoint policy must live in its declared ingestion boundary"
    );

    let checkpoints_source = source(&checkpoints_path);
    let checkpoints =
        code_without_comments_and_literals(production_module_source(&checkpoints_source));
    let owned_types = [
        ("struct", "SourceCheckpoint"),
        ("struct", "PendingSourceShrink"),
        ("struct", "ChunkedFingerprint"),
        ("struct", "FingerprintAuditBudget"),
        ("struct", "FullFingerprint"),
        ("enum", "FingerprintAudit"),
    ];
    for (kind, name) in owned_types {
        let declaration = format!("{kind} {name} {{");
        assert!(
            checkpoints.contains(&declaration),
            "ingestion checkpoints does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(&declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/checkpoints.rs"],
            "unexpected owners for {name}"
        );
    }

    let owned_functions = [
        "source_content_digest",
        "full_content_fingerprints_from_snapshot",
        "extend_chunked_fingerprint_from_snapshot",
        "audit_chunked_fingerprint_from_snapshot",
        "audit_growing_chunked_fingerprint_from_snapshot",
        "fingerprint_for_prefix_from_snapshot",
        "stored_fingerprint_matches",
    ];
    for name in owned_functions {
        let declaration = format!("fn {name}");
        assert!(
            checkpoints.contains(&declaration),
            "ingestion checkpoints does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(&declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/checkpoints.rs"],
            "unexpected owners for {name}"
        );
    }

    let crate_dependencies = crate_dependency_roots(&checkpoints);
    assert!(
        crate_dependencies
            .iter()
            .all(|dependency| dependency == "ingest"),
        "checkpoint policy imports an outward crate boundary: {}",
        crate_dependencies.join(", ")
    );

    let composition_source = ingest_composition_source(&src);
    let composition =
        code_without_comments_and_literals(production_module_source(&composition_source));
    assert!(
        composition.contains("mod checkpoints"),
        "the ingestion module root does not declare its private checkpoint module"
    );
    for (kind, name) in owned_types {
        assert!(
            !composition.contains(&format!("{kind} {name}")),
            "the ingestion module root duplicates checkpoint type `{name}`"
        );
        assert!(
            !composition.contains(&format!("type {name}")),
            "the ingestion module root aliases checkpoint type `{name}`"
        );
    }
    for name in owned_functions {
        assert!(
            !composition.contains(&format!("fn {name}")),
            "the ingestion module root wraps checkpoint policy `{name}`"
        );
    }
    assert!(!composition.contains("pub mod checkpoints"));
    assert!(!composition.contains("pub use checkpoints"));
}

#[test]
fn ingest_checkpoint_store_is_narrow_single_owned_and_non_transactional() {
    let root = manifest_root();
    let src = root.join("src");
    let store_path = src.join("ingest/checkpoint_store.rs");
    assert_eq!(
        assigned_role(&store_path),
        Some(Role::IngestCheckpointStore),
        "checkpoint persistence must live in its declared ingestion adapter"
    );

    let store_source = source(&store_path);
    let store = code_without_comments_and_literals(production_module_source(&store_source));
    let owned_functions = [
        "load_selected_source_extents",
        "load_checkpoint",
        "load_checkpoint_by_path",
        "pending_source_shrink_key",
        "clear_pending_source_shrink",
        "same_source_shrink_was_observed",
    ];
    for name in owned_functions {
        let declaration = format!("fn {name}(");
        assert!(
            store.contains(&declaration),
            "checkpoint store does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(&declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/checkpoint_store.rs"],
            "unexpected owners for {name}"
        );
    }

    let crate_dependencies = crate_dependency_roots(&store);
    assert!(
        crate_dependencies
            .iter()
            .all(|dependency| matches!(dependency.as_str(), "ingest" | "storage")),
        "checkpoint store imports an outward crate boundary: {}",
        crate_dependencies.join(", ")
    );
    assert!(
        crate_dependencies
            .iter()
            .any(|dependency| dependency == "storage")
            && store.contains("catalog::SelectedSourceExtent")
            && store.contains("path::PathBuf"),
        "checkpoint store must own the typed database-to-selected-extent adapter"
    );

    let raw_store = production_module_source(&store_source);
    assert!(
        raw_store.contains("FROM source_files"),
        "checkpoint store does not own its bounded checkpoint reads"
    );
    assert!(
        raw_store.contains(
            "SELECT rollout_id,path,size_bytes,byte_offset,content_fingerprint FROM source_files"
        ) && store.contains("fn load_selected_source_extents(")
            && store.contains("SelectedSourceExtent"),
        "checkpoint store does not own selected-source extent persistence"
    );
    assert!(
        raw_store.contains("pending_source_shrink")
            && raw_store.contains("INSERT INTO app_meta")
            && raw_store.contains("DELETE FROM app_meta"),
        "checkpoint store does not own the complete autocommit pending-shrink marker"
    );
    for forbidden in [
        "INSERT INTO source_files",
        "UPDATE source_files",
        "DELETE FROM source_files",
        "mark_file_unchanged",
        "clear_rollout",
        "project_record",
        "rematerialize_surviving_observation",
        "rematerialize_observed_children",
        "has_stale_projector_checkpoints",
        "advance_projector_generation",
    ] {
        assert!(
            !raw_store.contains(forbidden),
            "checkpoint store absorbed transaction-coupled ownership via `{forbidden}`"
        );
    }
    let projection_tables = forbidden_sql_table_hits(
        raw_store,
        &[
            "threads",
            "rollouts",
            "events",
            "messages",
            "turns",
            "tool_calls",
            "usage_facts",
            "agent_runs",
            "usage_activity_rollups",
        ],
    );
    assert!(
        projection_tables.is_empty(),
        "checkpoint store queries normalized projection tables: {}",
        projection_tables.join(", ")
    );

    let composition_source = ingest_composition_source(&src);
    let composition =
        code_without_comments_and_literals(production_module_source(&composition_source));
    assert!(
        composition.contains("mod checkpoint_store"),
        "the ingestion module root does not declare its private checkpoint-store module"
    );
    for name in owned_functions {
        assert!(
            !composition.contains(&format!("fn {name}(")),
            "the ingestion module root wraps checkpoint-store operation `{name}`"
        );
    }
    assert!(!composition.contains("pub mod checkpoint_store"));
    assert!(!composition.contains("pub use checkpoint_store"));

    let raw_composition = production_module_source(&composition_source);
    let file_ingestor_source = source(&src.join("ingest/file_ingestor.rs"));
    let raw_file_ingestor = production_module_source(&file_ingestor_source);
    for retained in [
        "fn mark_file_unchanged(",
        "save_source_checkpoint(",
        "clear_confirmed_shrink(",
        "rematerialize_after_checkpoint(",
        "transaction.commit()",
    ] {
        assert!(
            raw_file_ingestor.contains(retained),
            "transaction-coupled ingestion anchor left FileIngestor via `{retained}`"
        );
        assert!(
            !raw_composition.contains(retained),
            "scan coordination retains FileIngestor ownership via `{retained}`"
        );
    }
    for moved in [
        "fn clear_rollout(",
        "INSERT INTO source_files(",
        "UPDATE source_files SET",
        "UPDATE rollouts SET archived=?1",
        "DELETE FROM source_files WHERE rollout_id=?1",
        "rematerialize_observed_children(",
    ] {
        assert!(
            !raw_composition.contains(moved),
            "the ingestion module root retained moved checkpoint/removal ownership via `{moved}`"
        );
    }
}

#[test]
fn ingest_checkpoint_and_removal_use_named_projection_operations() {
    let src = manifest_root().join("src");
    let checkpoint_path = src.join("ingest/projection/checkpoint.rs");
    let removal_path = src.join("ingest/projection/removal.rs");
    assert_eq!(
        assigned_role(&checkpoint_path),
        Some(Role::IngestProjection)
    );
    assert_eq!(assigned_role(&removal_path), Some(Role::IngestProjection));

    let checkpoint_source = source(&checkpoint_path);
    let raw_checkpoint = production_module_source(&checkpoint_source);
    for owned in [
        "struct SourceCheckpointWrite",
        "struct UnchangedSourceUpdate",
        "struct PathConflict",
        "fn save_source_checkpoint(",
        "fn mark_source_unchanged(",
        "fn find_path_conflict(",
        "fn delete_source_checkpoint(",
        "fn clear_confirmed_shrink(",
        "fn rematerialize_after_checkpoint(",
        "INSERT INTO source_files(",
        "UPDATE source_files SET",
        "DELETE FROM source_files WHERE rollout_id=?1",
    ] {
        assert!(
            raw_checkpoint.contains(owned),
            "Checkpoint Projection does not own `{owned}`"
        );
    }
    for forbidden in [
        "std::path",
        "PathBuf",
        "FileIdentity",
        "SourceSnapshot",
        "peek_owner",
    ] {
        assert!(
            !raw_checkpoint.contains(forbidden),
            "Checkpoint Projection reaches into source mechanics via `{forbidden}`"
        );
    }
    let current = raw_checkpoint
        .find("agents::rematerialize_surviving_observation(tx, rollout_id)")
        .expect("checkpoint rematerialization must restore the current rollout");
    let children = raw_checkpoint
        .find("agents::rematerialize_observed_children(tx, rollout_id)")
        .expect("checkpoint rematerialization must replay observed children");
    assert!(
        current < children,
        "current rollout must rematerialize before children"
    );

    let removal_source = source(&removal_path);
    let raw_removal = production_module_source(&removal_source);
    for owned in [
        "struct RemovalImpact",
        "ordered_source_paths",
        "fn remove_rollout(",
        "fn delete_thread_if_abandoned(",
        "fn apply_thread_metadata_reset(",
        "ORDER BY sf.path,r.id",
    ] {
        assert!(
            raw_removal.contains(owned),
            "Removal Projection does not own `{owned}`"
        );
    }
    for forbidden in [
        "std::fs",
        "std::path",
        "Path::",
        "SourceSnapshot",
        "peek_owner",
        "File::open",
    ] {
        assert!(
            !raw_removal.contains(forbidden),
            "Removal Projection opens surviving source evidence via `{forbidden}`"
        );
    }

    let file_ingestor_source = source(&src.join("ingest/file_ingestor.rs"));
    let raw_file_ingestor = production_module_source(&file_ingestor_source);
    let save = raw_file_ingestor
        .find("save_source_checkpoint(")
        .expect("file projection must save its checkpoint");
    let clear = raw_file_ingestor[save..]
        .find("clear_confirmed_shrink(")
        .map(|offset| save + offset)
        .expect("confirmed shrink must clear after checkpoint save");
    let rematerialize = raw_file_ingestor[clear..]
        .find("rematerialize_after_checkpoint(")
        .map(|offset| clear + offset)
        .expect("agent replay must follow the durable checkpoint path");
    let commit = raw_file_ingestor[rematerialize..]
        .find(".commit()")
        .map(|offset| rematerialize + offset)
        .expect("file projection must commit after rematerialization");
    assert!(
        save < clear && clear < rematerialize && rematerialize < commit,
        "checkpoint save, confirmed-shrink clear, rematerialization, and commit reordered"
    );
}

#[test]
fn ingest_projection_transaction_is_opaque_outside_its_connection_adapter() {
    let src = manifest_root().join("src");
    let projection = src.join("ingest/projection");
    let connection_path = projection.join("connection.rs");
    assert_eq!(
        assigned_role(&connection_path),
        Some(Role::IngestProjectionConnection)
    );

    let connection_source = source(&connection_path);
    let connection =
        code_without_comments_and_literals(production_module_source(&connection_source));
    for owned in [
        "struct ProjectionConnection",
        "struct ProjectionTx",
        "begin_file_projection",
        "begin_reconciliation",
        "begin_metadata_refresh",
        "begin_title_import",
        "transaction_with_behavior(TransactionBehavior::Immediate)",
        "fn commit(self)",
    ] {
        assert!(
            connection.contains(owned),
            "Projection connection adapter does not own `{owned}`"
        );
    }
    for escape in [
        "Deref",
        "DerefMut",
        "AsRef",
        "AsMut",
        "Borrow",
        "BorrowMut",
        "into_inner",
        "get_ref",
        "get_mut",
        "fn raw(",
        "fn execute(",
        "fn query(",
        "fn query_row(",
        "fn prepare(",
    ] {
        assert!(
            !connection.contains(escape),
            "Projection transaction exposes raw SQLite through `{escape}`"
        );
    }

    for path in rust_files(&projection) {
        if path == connection_path {
            continue;
        }
        let contents = source(&path);
        let production = production_module_source(&contents);
        let raw_types = token_hits(
            &code_without_comments_and_literals(production),
            &["Connection", "Transaction", "TransactionBehavior"],
        );
        assert!(
            raw_types.is_empty(),
            "{} owns raw SQLite transaction types: {}",
            relative(&path),
            raw_types.join(", ")
        );
    }

    let composition_source = ingest_composition_source(&src);
    let composition =
        code_without_comments_and_literals(production_module_source(&composition_source));
    assert!(
        !composition.contains(".sqlite"),
        "ingestion orchestration reached through the opaque ProjectionTx field"
    );
    let file_ingestor_source = source(&src.join("ingest/file_ingestor.rs"));
    let file_ingestor =
        code_without_comments_and_literals(production_module_source(&file_ingestor_source));
    assert!(
        !file_ingestor.contains(".sqlite"),
        "FileIngestor reached through the opaque ProjectionTx field"
    );
    for begin in ["begin_file_projection()", "begin_metadata_refresh()"] {
        assert!(
            file_ingestor.contains(begin),
            "FileIngestor does not use named transaction start `{begin}`"
        );
        assert!(
            !composition.contains(begin),
            "scan coordination retains FileIngestor transaction start `{begin}`"
        );
    }

    let reconciliation_source = source(&src.join("ingest/reconciliation.rs"));
    let reconciliation =
        code_without_comments_and_literals(production_module_source(&reconciliation_source));
    assert!(
        !reconciliation.contains(".sqlite"),
        "reconciliation reached through the opaque ProjectionTx field"
    );
    assert!(
        reconciliation.contains("begin_reconciliation()"),
        "reconciliation must use its named Projection transaction start"
    );

    let session_titles_source = source(&src.join("ingest/session_titles.rs"));
    let session_titles =
        code_without_comments_and_literals(production_module_source(&session_titles_source));
    assert!(
        !session_titles.contains(".sqlite"),
        "session-title import reached through the opaque ProjectionTx field"
    );
    assert!(
        session_titles.contains("begin_title_import()"),
        "session-title import must use its named Projection transaction start"
    );
}

#[test]
fn ingest_owner_reader_single_owns_descriptor_bounded_owner_decoding() {
    let src = manifest_root().join("src");
    let owner_reader_path = src.join("ingest/owner_reader.rs");
    assert_eq!(
        assigned_role(&owner_reader_path),
        Some(Role::IngestOwnerReader),
        "OwnerReader must live in its pure Source/Protocol boundary"
    );

    let owner_reader_source = source(&owner_reader_path);
    let raw_owner_reader = production_module_source(&owner_reader_source);
    let owner_reader = code_without_comments_and_literals(raw_owner_reader);
    let violations = role_violations(Role::IngestOwnerReader, raw_owner_reader);
    assert!(
        violations.is_empty(),
        "OwnerReader crossed its pure read boundary via: {}",
        violations.join(", ")
    );
    assert!(
        crate_dependency_roots(&owner_reader)
            .iter()
            .all(|dependency| dependency == "ingest"),
        "OwnerReader may depend only inward on Source and Protocol"
    );
    for import in raw_owner_reader
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use super::")
                || import.starts_with("use crate::ingest")
                || import.starts_with("use anyhow::")
                || import.starts_with("use serde_json::")
                || import.starts_with("use std::"),
            "OwnerReader adds an unintended dependency via `{import}`"
        );
    }
    for required in [
        "SourceSnapshot",
        "next_bounded_line",
        "MAX_JSONL_LINE_BYTES",
        "decode_owner_record",
    ] {
        assert!(
            owner_reader.contains(required),
            "OwnerReader no longer composes descriptor-bounded Source framing with `{required}`"
        );
    }

    for (declaration, name) in [
        ("fn read_owner(", "read_owner"),
        ("fn read_owner_from_snapshot(", "read_owner_from_snapshot"),
        ("fn read_available_owners(", "read_available_owners"),
    ] {
        assert!(
            owner_reader.contains(declaration),
            "OwnerReader does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/owner_reader.rs"],
            "unexpected production owners for {name}"
        );
    }

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod owner_reader"),
        "ingestion composition does not declare its private OwnerReader module"
    );
    assert!(!composition.contains("pub mod owner_reader"));
    assert!(!composition.contains("pub use owner_reader"));
    for removed in [
        "fn read_owner(",
        "fn read_owner_from_snapshot(",
        "fn read_available_owners(",
        "type OwnerReader",
    ] {
        assert!(
            !composition.contains(removed),
            "ingestion composition retains or wraps OwnerReader policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_file_ingestor_single_owns_descriptor_transaction_and_scan_budget() {
    let src = manifest_root().join("src");
    let file_ingestor_path = src.join("ingest/file_ingestor.rs");
    assert_eq!(
        assigned_role(&file_ingestor_path),
        Some(Role::IngestFileIngestor),
        "FileIngestor must live in its dedicated per-file application boundary"
    );

    let file_ingestor_source = source(&file_ingestor_path);
    let raw_file_ingestor = production_module_source(&file_ingestor_source);
    let file_ingestor = code_without_comments_and_literals(raw_file_ingestor);
    let violations = role_violations(Role::IngestFileIngestor, raw_file_ingestor);
    assert!(
        violations.is_empty(),
        "FileIngestor absorbed SQL, HTTP, runtime, or scan-attempt ownership via: {}",
        violations.join(", ")
    );
    assert!(
        crate_dependency_roots(&file_ingestor)
            .iter()
            .all(|dependency| matches!(dependency.as_str(), "ingest" | "storage")),
        "FileIngestor may depend only on inward ingestion adapters and the database handle"
    );
    for import in raw_file_ingestor
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use super::")
                || import.starts_with("use crate::ingest")
                || import.starts_with("use crate::storage")
                || import.starts_with("use anyhow::")
                || import.starts_with("use chrono::")
                || import.starts_with("use rusqlite::")
                || import.starts_with("use serde::")
                || import.starts_with("use serde_json::")
                || import.starts_with("use std::")
                || import.starts_with("use tracing::"),
            "FileIngestor adds an unintended dependency via `{import}`"
        );
    }

    for owned in [
        "struct FileIngestor",
        "struct FileReport",
        "fn process(",
        "fn mark_file_unchanged(",
        "fn source_path_switch_is_ready(",
        "fn source_path_switch_is_ready_from_snapshot(",
    ] {
        assert!(
            file_ingestor.contains(owned),
            "FileIngestor does not own `{owned}`"
        );
    }
    for (declaration, name) in [
        ("struct FileIngestor", "FileIngestor"),
        ("struct FileReport", "FileReport"),
        ("fn mark_file_unchanged(", "mark_file_unchanged"),
        (
            "fn source_path_switch_is_ready(",
            "source_path_switch_is_ready",
        ),
        (
            "fn source_path_switch_is_ready_from_snapshot(",
            "source_path_switch_is_ready_from_snapshot",
        ),
    ] {
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/file_ingestor.rs"],
            "unexpected production owners for {name}"
        );
    }
    for path in rust_files(&src) {
        assert!(
            !production_module_source(&source(&path)).contains("fn process_file("),
            "{} retains the pre-extraction process_file entrypoint",
            relative(&path)
        );
    }

    let process = file_ingestor
        .split_once("fn process(")
        .map(|(_, suffix)| suffix)
        .expect("FileIngestor must expose its per-file operation");
    let open = process
        .find("SourceSnapshot::open(path)")
        .expect("per-file ingestion must open one source descriptor");
    let owner = process
        .find("read_owner_from_snapshot(&mut snapshot, path)")
        .expect("per-file ingestion must decode ownership from its opened descriptor");
    let owner_validation = process
        .find("owner.owner_id != resolved_owner.owner_id")
        .expect("per-file ingestion must validate discovery ownership");
    let handoff = process
        .find("source_path_switch_is_ready_from_snapshot(")
        .expect("per-file ingestion must revalidate handoff continuity on the same descriptor");
    let checkpoint = process
        .find("load_checkpoint_by_path(")
        .expect("per-file ingestion must load checkpoint state after descriptor validation");
    let begin = process
        .find("begin_file_projection()")
        .expect("per-file ingestion must claim its named IMMEDIATE transaction");
    let first_transactional_read = process
        .find("find_path_conflict(")
        .expect("per-file ingestion must perform its first transactional read through Projection");
    let upsert = process
        .find("upsert_owner(")
        .expect("per-file ingestion must upsert the validated owner");
    let stream = process
        .find(".jsonl_from(")
        .expect("per-file ingestion must stream the captured source extent");
    let save = process
        .find("save_source_checkpoint(")
        .expect("per-file ingestion must save its durable checkpoint");
    let shrink = process
        .find("clear_confirmed_shrink(")
        .expect("per-file ingestion must clear a confirmed shrink after checkpoint persistence");
    let rematerialize = process.find("rematerialize_after_checkpoint(").expect(
        "per-file ingestion must rematerialize lifecycle state after checkpoint persistence",
    );
    let commit = process
        .find(".commit()")
        .expect("per-file ingestion must commit its complete projection once");
    assert!(
        open < owner
            && owner < owner_validation
            && owner_validation < handoff
            && handoff < checkpoint
            && checkpoint < begin
            && begin < first_transactional_read
            && first_transactional_read < upsert
            && upsert < stream
            && stream < save
            && save < shrink
            && shrink < rematerialize
            && rematerialize < commit,
        "FileIngestor reordered descriptor ownership, handoff, transaction, checkpoint, or rematerialization"
    );
    assert_eq!(
        process.matches("SourceSnapshot::open(path)").count(),
        1,
        "the per-file operation must derive all source decisions from one opened descriptor"
    );

    assert!(
        file_ingestor.contains("audit_budget: FingerprintAuditBudget"),
        "FileIngestor must carry the scan-wide fingerprint audit budget"
    );
    assert_eq!(
        raw_file_ingestor
            .matches("FingerprintAuditBudget::default()")
            .count(),
        1,
        "the shared fingerprint audit budget must be initialized exactly once"
    );
    let constructor = file_ingestor
        .split_once("fn new(")
        .map(|(_, suffix)| suffix)
        .expect("FileIngestor must have an explicit constructor");
    let constructor_end = constructor
        .find("fn process(")
        .expect("FileIngestor constructor must precede its operation");
    assert!(
        constructor[..constructor_end].contains("FingerprintAuditBudget::default()"),
        "the audit budget must be created with the scan-scoped FileIngestor"
    );
    assert!(
        !process.contains("FingerprintAuditBudget::default()"),
        "the shared fingerprint audit budget must not reset per file"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod file_ingestor"),
        "ingestion composition does not declare its private FileIngestor boundary"
    );
    assert!(!composition.contains("pub mod file_ingestor"));
    assert!(!composition.contains("pub use file_ingestor"));
    let coordinator_source = source(&src.join("ingest/coordinator.rs"));
    let raw_coordinator = production_module_source(&coordinator_source);
    let coordinator = code_without_comments_and_literals(raw_coordinator);
    assert!(
        coordinator.contains("file_ingestor::{FileIngestor, FileReport}"),
        "Coordinator must consume the extracted FileIngestor directly"
    );
    for removed in [
        "struct FileIngestor",
        "struct FileReport",
        "fn process_file(",
        "fn mark_file_unchanged(",
        "fn source_path_switch_is_ready(",
        "fn source_path_switch_is_ready_from_snapshot(",
        "FingerprintAuditBudget",
    ] {
        assert!(
            !composition.contains(removed) && !coordinator.contains(removed),
            "ingestion composition or Coordinator retains per-file ownership via `{removed}`"
        );
    }

    assert_eq!(
        coordinator.matches("FileIngestor::new(db)").count(),
        1,
        "one scan must construct exactly one FileIngestor"
    );
    let construct = coordinator
        .find("FileIngestor::new(db)")
        .expect("scan coordination must construct FileIngestor");
    let loop_start = coordinator
        .find("for candidate in selected")
        .expect("scan coordination must retain deterministic selected-source order");
    let loop_body = &coordinator[loop_start..];
    assert!(
        construct < loop_start && loop_body.contains(".process("),
        "FileIngestor must be constructed once before, then reused within, the selected-source loop"
    );
    assert!(
        !loop_body.contains("FileIngestor::new(")
            && !loop_body.contains("FingerprintAuditBudget::default()"),
        "per-file processing must not reconstruct its ingestor or reset the shared audit budget"
    );
}

#[test]
fn ingest_coordinator_single_owns_scan_cycles_without_becoming_a_worker_or_adapter() {
    let src = manifest_root().join("src");
    let coordinator_path = src.join("ingest/coordinator.rs");
    assert_eq!(
        assigned_role(&coordinator_path),
        Some(Role::IngestCoordinator),
        "scan-cycle composition must live in its dedicated Coordinator boundary"
    );

    let coordinator_source = source(&coordinator_path);
    let raw_coordinator = production_module_source(&coordinator_source);
    let coordinator = code_without_comments_and_literals(raw_coordinator);
    let violations = role_violations(Role::IngestCoordinator, raw_coordinator);
    assert!(
        violations.is_empty(),
        "Coordinator absorbed SQL, transport, raw JSONL, or scanner-worker ownership via: {}",
        violations.join(", ")
    );
    assert!(
        crate_dependency_roots(&coordinator)
            .iter()
            .all(|dependency| matches!(dependency.as_str(), "calendar" | "storage")),
        "Coordinator reached beyond its calendar and storage application boundaries"
    );
    for import in raw_coordinator
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use super::")
                || import.starts_with("use crate::calendar")
                || import.starts_with("use crate::storage")
                || import.starts_with("use crate::{")
                || import.starts_with("use anyhow::")
                || import.starts_with("use chrono::")
                || import.starts_with("use serde::")
                || import.starts_with("use std::"),
            "Coordinator adds an unintended dependency via `{import}`"
        );
    }

    for (declaration, name) in [
        ("struct IngestRoots", "IngestRoots"),
        ("struct ScanReport", "ScanReport"),
        ("struct ScanOutcome", "ScanOutcome"),
        ("struct IngestScannerLease", "IngestScannerLease"),
        ("fn scan_once(", "scan_once"),
        ("fn scan_one_shot(", "scan_one_shot"),
        ("fn scan_one_shot_with_lease(", "scan_one_shot_with_lease"),
        ("fn scan_once_locked(", "scan_once_locked"),
        ("fn scan_once_started(", "scan_once_started"),
        (
            "fn finalize_unexpected_scan_error(",
            "finalize_unexpected_scan_error",
        ),
        (
            "fn finalize_scan_sequence_error(",
            "finalize_scan_sequence_error",
        ),
        ("fn recover_interrupted_scan(", "recover_interrupted_scan"),
        ("fn set_scan_after_start_hook(", "set_scan_after_start_hook"),
        ("fn run_scan_after_start_hook(", "run_scan_after_start_hook"),
    ] {
        assert!(
            coordinator.contains(declaration),
            "Coordinator does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/coordinator.rs"],
            "unexpected production owners for {name}"
        );
    }

    for forbidden in [
        "struct ScannerHandle",
        "fn spawn_scanner(",
        "fn spawn_scanner_with_lease(",
        "std::thread::spawn",
        "std::thread::sleep",
    ] {
        assert!(
            !coordinator.contains(forbidden),
            "Coordinator absorbed Scanner worker ownership via `{forbidden}`"
        );
    }

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod coordinator"),
        "ingestion composition does not declare its private Coordinator boundary"
    );
    assert!(!composition.contains("pub mod coordinator"));
    assert!(
        raw_composition.contains("pub use coordinator::{"),
        "stable ingestion entrypoints must be direct Coordinator re-exports"
    );
    for exported in [
        "IngestRoots",
        "IngestScannerLease",
        "ScanReport",
        "recover_interrupted_scan",
        "scan_once",
        "scan_one_shot",
        "scan_one_shot_with_lease",
    ] {
        assert!(
            raw_composition
                .split_once("pub use coordinator::{")
                .and_then(|(_, suffix)| suffix.split_once("};"))
                .is_some_and(|(exports, _)| exports.contains(exported)),
            "ingestion composition does not directly re-export `{exported}`"
        );
    }
    for removed in [
        "struct IngestRoots",
        "struct ScanReport",
        "struct ScanOutcome",
        "struct IngestScannerLease",
        "fn scan_once(",
        "fn scan_one_shot(",
        "fn scan_one_shot_with_lease(",
        "fn scan_once_locked(",
        "fn scan_once_started(",
        "fn finalize_unexpected_scan_error(",
        "fn finalize_scan_sequence_error(",
        "fn recover_interrupted_scan(",
        "fn set_scan_after_start_hook(",
        "fn run_scan_after_start_hook(",
    ] {
        assert!(
            !composition.contains(removed),
            "ingestion composition retains or wraps Coordinator policy via `{removed}`"
        );
    }

    let locked = coordinator
        .split_once("fn scan_once_locked(")
        .and_then(|(_, suffix)| suffix.split_once("fn scan_once_started("))
        .map(|(body, _)| body)
        .expect("Coordinator must keep a distinct locked scan-cycle entrypoint");
    let begin = locked
        .find("AttemptRecorder::new(db).begin()")
        .expect("scan cycle must begin its durable attempt first");
    let started = locked
        .find("scan_once_started(db, roots)")
        .expect("scan cycle must enter enumeration after attempt start");
    assert!(
        begin < started,
        "source work began before durable attempt start"
    );

    let cycle = coordinator
        .split_once("fn scan_once_started(")
        .and_then(|(_, suffix)| suffix.split_once("fn finalize_unexpected_scan_error("))
        .map(|(body, _)| body)
        .expect("Coordinator must keep one scan-cycle body");
    let hook = cycle
        .find("run_scan_after_start_hook(db)")
        .expect("test scan-start hook must remain at the orchestration seam");
    let enumerate = cycle
        .find("collect_jsonl(")
        .expect("scan cycle must enumerate configured roots");
    let inspect = cycle
        .find("read_owner(&path)")
        .expect("scan cycle must inspect discovered source ownership");
    let choose = cycle
        .find("plan_catalog_selection(")
        .expect("scan cycle must choose candidates through Catalog");
    let topology = cycle
        .find("resolve_owner_topology(")
        .expect("scan cycle must resolve selected owner topology");
    let ingest = cycle
        .find("for candidate in selected")
        .expect("scan cycle must ingest selected sources in their planned order");
    let reconcile = cycle
        .find("reconcile_missing(")
        .expect("equal-root scan must invoke Reconciliation");
    let adopt = cycle
        .find(".adopt_root_signature(")
        .expect("changed-root scan must adopt its clean signature");
    let titles = cycle
        .find("sync_session_index_titles(")
        .expect("scan cycle must import session titles");
    let finish = cycle
        .find(".finish(")
        .expect("scan cycle must finalize its durable attempt");
    assert!(
        hook < enumerate
            && enumerate < inspect
            && inspect < choose
            && choose < topology
            && topology < ingest
            && ingest < reconcile
            && ingest < adopt
            && reconcile < titles
            && adopt < titles
            && titles < finish,
        "scan cycle no longer follows begin, enumerate, inspect, choose, topology, ingest, reconcile/adopt, titles, finalize"
    );

    let one_shot = coordinator
        .split_once("fn scan_one_shot_with_lease_and_between_pass")
        .and_then(|(_, suffix)| suffix.split_once("fn scan_once_locked("))
        .map(|(body, _)| body)
        .expect("Coordinator must keep one bounded one-shot sequence");
    let validate = one_shot
        .find("lease.require_database(db)")
        .expect("one-shot must validate its lifetime lease");
    let lock = one_shot
        .find("DatabaseLock::acquire(db,")
        .expect("one-shot must acquire one process lock");
    let first = one_shot
        .find("scan_once_locked(db, roots)")
        .expect("one-shot must perform its first scan");
    let confirmation = one_shot[first + 1..]
        .find("scan_once_locked(db, roots)")
        .map(|offset| first + 1 + offset)
        .expect("one-shot must retain its optional confirmation scan");
    let publication = one_shot
        .find(".publish_projector_generation()")
        .expect("one-shot must publish only the complete projector generation");
    assert_eq!(
        one_shot.matches("DatabaseLock::acquire(db,").count(),
        1,
        "one-shot must retain one process lock across both scans and publication"
    );
    assert!(
        validate < lock && lock < first && first < confirmation && confirmation < publication,
        "one-shot reordered lease validation, process locking, confirmation, or publication: validate={validate}, lock={lock}, first={first}, confirmation={confirmation}, publication={publication}"
    );

    let recovery = coordinator
        .split_once("fn recover_interrupted_scan(")
        .map(|(_, suffix)| suffix)
        .expect("Coordinator must own the public recovery wrapper");
    let recovery_lock = recovery
        .find("DatabaseLock::acquire(db,")
        .expect("recovery must acquire the process lock");
    let recovery_transition = recovery
        .find(".recover_interrupted_state()")
        .expect("recovery must invoke Attempt's named transition");
    assert!(
        recovery_lock < recovery_transition,
        "recovery inspected durable state before acquiring the process lock"
    );
}

#[test]
fn ingest_scanner_single_owns_the_background_worker_without_absorbing_coordination() {
    let src = manifest_root().join("src");
    let scanner_path = src.join("ingest/scanner.rs");
    assert!(
        scanner_path.is_file(),
        "Checkpoint 6 requires the Scanner boundary at {}",
        scanner_path.display()
    );
    assert_eq!(
        assigned_role(&scanner_path),
        Some(Role::IngestScanner),
        "background polling must live in its dedicated Scanner boundary"
    );

    let scanner_source = source(&scanner_path);
    let raw_scanner = production_module_source(&scanner_source);
    let scanner = code_without_comments_and_literals(raw_scanner);
    let violations = role_violations(Role::IngestScanner, raw_scanner);
    assert!(
        violations.is_empty(),
        "Scanner absorbed SQL, transport, parsing, Projection, Catalog, or Reconciliation ownership via: {}",
        violations.join(", ")
    );
    assert!(
        crate_dependency_roots(&scanner)
            .iter()
            .all(|dependency| dependency == "storage"),
        "Scanner reached beyond the storage application boundary"
    );
    for import in raw_scanner
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use super::")
                || import.starts_with("use crate::storage")
                || import.starts_with("use anyhow::")
                || import.starts_with("use std::"),
            "Scanner adds an unintended dependency via `{import}`"
        );
    }

    for (declaration, name) in [
        ("struct ScannerHandle", "ScannerHandle"),
        ("fn spawn_scanner(", "spawn_scanner"),
        ("fn spawn_scanner_with_lease(", "spawn_scanner_with_lease"),
    ] {
        assert!(
            scanner.contains(declaration),
            "Scanner does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/scanner.rs"],
            "unexpected production owners for {name}"
        );
    }

    assert!(
        scanner.contains("attempt::AttemptRecorder")
            && scanner.contains("coordinator::{")
            && scanner.contains("IngestRoots")
            && scanner.contains("IngestScannerLease")
            && scanner.contains("scan_one_shot_with_lease"),
        "Scanner must consume Attempt failure marking and Coordinator lifecycle operations directly"
    );
    for forbidden in [
        "struct IngestRoots",
        "struct IngestScannerLease",
        "struct ScanReport",
        "struct ScanOutcome",
        "fn scan_once(",
        "fn scan_one_shot(",
        "fn scan_one_shot_with_lease(",
        "fn recover_interrupted_scan(",
        ".begin()",
        ".root_signature()",
        ".adopt_root_signature(",
        ".finish(",
        ".recover_interrupted_state()",
        ".publish_projector_generation()",
    ] {
        assert!(
            !scanner.contains(forbidden),
            "Scanner absorbed Coordinator or Attempt lifecycle policy via `{forbidden}`"
        );
    }
    for retained in [
        "IngestScannerLease::acquire(&db)",
        "lease.require_database(&db)",
        "std::thread::spawn(move ||",
        "scan_one_shot_with_lease(&db, &roots, &lease)",
        "AttemptRecorder::new(&db).mark_cycle_failed()",
        "Duration::from_millis(250)",
    ] {
        assert!(
            scanner.contains(retained),
            "Scanner lost its bounded worker contract via `{retained}`"
        );
    }
    assert!(
        !scanner.contains("scan_once(&db") && !scanner.contains("DatabaseLock"),
        "Scanner bypasses Coordinator's complete one-shot or lease-owned locking semantics"
    );

    let coordinator_source = source(&src.join("ingest/coordinator.rs"));
    let coordinator =
        code_without_comments_and_literals(production_module_source(&coordinator_source));
    for retained in [
        "struct IngestScannerLease",
        "fn scan_one_shot_with_lease(",
        "fn recover_interrupted_scan(",
    ] {
        assert!(
            coordinator.contains(retained),
            "Coordinator lost lifecycle ownership via `{retained}`"
        );
    }
    assert!(
        !coordinator.contains("mod scanner")
            && !coordinator.contains("scanner::")
            && !coordinator.contains("struct ScannerHandle"),
        "Coordinator depends upward on Scanner"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod scanner"),
        "the ingestion module root does not declare its private Scanner boundary"
    );
    assert!(!composition.contains("pub mod scanner"));
    assert!(
        raw_composition.contains("pub use scanner::{"),
        "stable Scanner entrypoints must be direct re-exports"
    );
    for exported in ["ScannerHandle", "spawn_scanner", "spawn_scanner_with_lease"] {
        assert!(
            raw_composition
                .split_once("pub use scanner::{")
                .and_then(|(_, suffix)| suffix.split_once("};"))
                .is_some_and(|(exports, _)| exports.contains(exported)),
            "the ingestion module root does not directly re-export `{exported}`"
        );
    }
    for removed in [
        "struct ScannerHandle",
        "fn spawn_scanner(",
        "fn spawn_scanner_with_lease(",
        "std::thread::spawn",
        "std::thread::sleep",
        ".mark_cycle_failed(",
    ] {
        assert!(
            !composition.contains(removed),
            "the ingestion module root retains or wraps Scanner policy via `{removed}`"
        );
    }
}

#[test]
fn ingest_reconciliation_single_owns_the_ordered_removal_plan() {
    let src = manifest_root().join("src");
    let reconciliation_path = src.join("ingest/reconciliation.rs");
    assert_eq!(
        assigned_role(&reconciliation_path),
        Some(Role::IngestOrchestration),
        "Reconciliation must live in its declared orchestration boundary"
    );

    let reconciliation_source = source(&reconciliation_path);
    let raw_reconciliation = production_module_source(&reconciliation_source);
    let reconciliation = code_without_comments_and_literals(raw_reconciliation);
    let violations = role_violations(Role::IngestOrchestration, raw_reconciliation);
    assert!(
        violations.is_empty(),
        "Reconciliation must use named Projection operations rather than SQL: {}",
        violations.join(", ")
    );
    assert!(
        crate_dependency_roots(&reconciliation)
            .iter()
            .all(|dependency| matches!(dependency.as_str(), "ingest" | "storage")),
        "Reconciliation reached beyond ingestion and the database handle"
    );
    for import in raw_reconciliation
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use super::")
                || import.starts_with("use crate::ingest")
                || import.starts_with("use crate::storage")
                || import.starts_with("use anyhow::")
                || import.starts_with("use std::"),
            "Reconciliation adds an unintended dependency via `{import}`"
        );
    }

    for (declaration, name) in [
        (
            "fn reset_thread_metadata_from_sources(",
            "reset_thread_metadata_from_sources",
        ),
        ("fn reconcile_missing(", "reconcile_missing"),
    ] {
        assert!(
            reconciliation.contains(declaration),
            "Reconciliation does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/reconciliation.rs"],
            "unexpected production owners for {name}"
        );
    }
    for named_operation in [
        "reconciliation_candidates()",
        "begin_reconciliation()",
        "remove_rollout(",
        "read_available_owners(",
        "apply_thread_metadata_reset(",
        "delete_source_checkpoint(",
        "delete_thread_if_abandoned(",
        ".commit()",
    ] {
        assert!(
            reconciliation.contains(named_operation),
            "Reconciliation lost named operation `{named_operation}`"
        );
    }
    let candidates = reconciliation
        .find("reconciliation_candidates()")
        .expect("Reconciliation must load its ordered candidate snapshot");
    let begin = reconciliation
        .find("begin_reconciliation()")
        .expect("Reconciliation must claim one IMMEDIATE writer");
    assert!(
        candidates < begin,
        "Reconciliation must load its existing candidate order before acquiring the IMMEDIATE writer"
    );
    let apply = reconciliation
        .split_once("fn reconcile_missing(")
        .map(|(_, suffix)| suffix)
        .expect("Reconciliation must expose its caller entrypoint");
    let remove = apply
        .find("remove_rollout(")
        .expect("Reconciliation must remove each selected rollout");
    let reset = apply
        .find("reset_thread_metadata_from_sources(")
        .expect("Reconciliation must rebuild surviving root metadata");
    let checkpoint = apply
        .find("delete_source_checkpoint(")
        .expect("Reconciliation must delete the removed source checkpoint");
    let abandoned = apply
        .find("delete_thread_if_abandoned(")
        .expect("Reconciliation must clean up an abandoned thread");
    let commit = apply
        .find(".commit()")
        .expect("Reconciliation must commit its complete plan atomically");
    assert!(
        remove < reset && reset < checkpoint && checkpoint < abandoned && abandoned < commit,
        "removal, surviving metadata, checkpoint cleanup, abandoned-thread cleanup, and commit reordered"
    );

    let connection_path = src.join("ingest/projection/connection.rs");
    let connection_source = source(&connection_path);
    let raw_connection = production_module_source(&connection_source);
    let connection = code_without_comments_and_literals(raw_connection);
    for (declaration, name) in [
        ("struct ReconciliationCandidate", "ReconciliationCandidate"),
        ("fn reconciliation_candidates(", "reconciliation_candidates"),
    ] {
        assert!(
            connection.contains(declaration),
            "Projection connection does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/projection/connection.rs"],
            "unexpected production owners for {name}"
        );
    }
    for field in ["rollout_id", "path", "root_thread_id"] {
        assert!(
            connection.contains(field),
            "Projection reconciliation candidates lost `{field}` evidence"
        );
    }
    assert!(
        raw_connection.contains("FROM source_files")
            && connection.contains("query_map")
            && connection.contains("ReconciliationCandidate"),
        "Projection must own the complete reconciliation candidate query and row mapping"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    for module in ["mod owner_reader", "mod reconciliation"] {
        assert!(
            composition.contains(module),
            "ingestion composition does not declare its private `{module}` boundary"
        );
    }
    for exported in [
        "pub mod owner_reader",
        "pub use owner_reader",
        "pub mod reconciliation",
        "pub use reconciliation",
    ] {
        assert!(
            !composition.contains(exported),
            "ingestion composition leaks a private boundary through `{exported}`"
        );
    }
    for removed in [
        "struct ReconciliationCandidate",
        "type ReconciliationCandidate",
        "fn reconciliation_candidates(",
        "fn reconcile_missing(",
        "fn reset_thread_metadata_from_sources(",
        "fn read_owner(",
        "fn read_owner_from_snapshot(",
        "fn read_available_owners(",
    ] {
        assert!(
            !composition.contains(removed),
            "ingestion composition retains or wraps extracted policy via `{removed}`"
        );
    }

    let coordinator_source = source(&src.join("ingest/coordinator.rs"));
    let raw_coordinator = production_module_source(&coordinator_source);
    let scan = raw_coordinator
        .find("let previous_signature")
        .map(|offset| &raw_coordinator[offset..])
        .expect("scan orchestration must retain root-signature comparison");
    let equal = scan
        .find("if previous_signature.as_deref() == Some(root_signature.as_str())")
        .expect("scan orchestration must reconcile only an equal root signature");
    let reconcile = scan
        .find("reconcile_missing(")
        .expect("equal-signature scan must invoke Reconciliation");
    let changed = scan
        .find("else if report.files_failed == 0")
        .expect("changed-signature scan must require a clean adoption pass");
    let adopt = scan
        .find(".adopt_root_signature(")
        .expect("changed-signature scan must adopt the clean root set");
    let titles = scan
        .find("sync_session_index_titles(")
        .expect("scan orchestration must retain title import");
    let finalize = scan
        .find(".finish(")
        .expect("scan orchestration must retain attempt finalization");
    assert!(
        equal < reconcile
            && reconcile < changed
            && changed < adopt
            && adopt < titles
            && titles < finalize,
        "equal-signature reconciliation and changed-signature adoption must remain distinct and precede titles/finalization"
    );
}

#[test]
fn ingest_session_titles_single_owns_import_policy_and_stays_in_scan_order() {
    let src = manifest_root().join("src");
    let session_titles_path = src.join("ingest/session_titles.rs");
    assert_eq!(
        assigned_role(&session_titles_path),
        Some(Role::IngestOrchestration),
        "session-title import must live in its declared orchestration boundary"
    );

    let session_titles_source = source(&session_titles_path);
    let raw_session_titles = production_module_source(&session_titles_source);
    let session_titles = code_without_comments_and_literals(raw_session_titles);
    assert!(
        forbidden_hits(
            &session_titles,
            &[
                "rusqlite",
                ".prepare(",
                ".query_row(",
                ".execute(",
                "SELECT ",
                "INSERT INTO ",
                "UPDATE ",
                "DELETE FROM ",
            ],
        )
        .is_empty(),
        "session-title orchestration must use named Projection operations rather than SQL"
    );
    assert_eq!(
        crate_dependency_roots(&session_titles),
        ["calendar", "storage"],
        "session-title import reached beyond its calendar and database boundaries"
    );
    for import in raw_session_titles
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use super::")
                || import.starts_with("use crate::")
                || import.starts_with("use anyhow::")
                || import.starts_with("use chrono::")
                || import.starts_with("use serde_json::")
                || import.starts_with("use std::"),
            "session-title import adds an unintended dependency via `{import}`"
        );
    }
    for sibling in ["projection::", "protocol::", "source::"] {
        assert!(
            session_titles.contains(sibling),
            "session-title import must reach `{sibling}` through its narrow ingestion seam"
        );
    }
    for forbidden in [
        "catalog::",
        "checkpoints::",
        "checkpoint_store::",
        "coordinator::",
        "reconciliation::",
        "web::",
        "system::",
        "pricing::",
        "sessions::",
        "activity::",
        "analytics::",
    ] {
        assert!(
            !session_titles.contains(forbidden),
            "session-title import crossed an unintended boundary via `{forbidden}`"
        );
    }

    for (declaration, name) in [
        ("struct IndexedTitle", "IndexedTitle"),
        ("fn sync_session_index_titles(", "sync_session_index_titles"),
        ("fn discover_session_index(", "discover_session_index"),
        ("fn session_index_candidates(", "session_index_candidates"),
    ] {
        assert!(
            session_titles.contains(declaration),
            "session-title module does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/session_titles.rs"],
            "unexpected production owners for {name}"
        );
    }

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod session_titles"),
        "ingestion composition does not declare its private session-title module"
    );
    for removed in [
        "struct IndexedTitle",
        "fn sync_session_index_titles(",
        "fn discover_session_index(",
        "fn session_index_candidates(",
        "type IndexedTitle",
    ] {
        assert!(
            !composition.contains(removed),
            "ingestion composition retains session-title policy via `{removed}`"
        );
    }
    assert!(!composition.contains("pub mod session_titles"));
    assert!(!composition.contains("pub use session_titles"));

    let coordinator_source = source(&src.join("ingest/coordinator.rs"));
    let raw_coordinator = production_module_source(&coordinator_source);
    let scan = raw_coordinator
        .find("let mut root_signature_adopted")
        .map(|offset| &raw_coordinator[offset..])
        .expect("scan orchestration must retain root-signature adoption");
    let reconcile = scan
        .find("reconcile_missing(")
        .expect("scan orchestration must retain reconciliation");
    let adopt = scan
        .find(".adopt_root_signature(")
        .expect("scan orchestration must retain root adoption");
    let titles = scan
        .find("sync_session_index_titles(")
        .expect("scan orchestration must directly invoke session-title import");
    let finalize = scan
        .find(".finish(")
        .expect("scan orchestration must retain attempt finalization");
    assert!(
        reconcile < titles && adopt < titles && titles < finalize,
        "session-title import must follow reconciliation or root adoption and precede finalization"
    );
}

#[test]
fn ingest_attempt_single_owns_durable_control_state_without_absorbing_control_flow() {
    let src = manifest_root().join("src");
    let attempt_path = src.join("ingest/attempt.rs");
    assert_eq!(
        assigned_role(&attempt_path),
        Some(Role::IngestAttempt),
        "durable attempt state must live in its declared ingestion adapter"
    );

    let attempt_source = source(&attempt_path);
    let raw_attempt = production_module_source(&attempt_source);
    let attempt = code_without_comments_and_literals(raw_attempt);
    assert_eq!(
        crate_dependency_roots(&attempt),
        ["storage"],
        "Attempt must depend only on the database boundary"
    );
    for import in raw_attempt
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use crate::storage")
                || import.starts_with("use anyhow::")
                || import.starts_with("use rusqlite::"),
            "Attempt adds an unintended dependency via `{import}`"
        );
    }

    for (declaration, name) in [
        ("struct AttemptRecorder", "AttemptRecorder"),
        (
            "fn projector_generation_is_current(",
            "projector_generation_is_current",
        ),
        (
            "fn has_stale_projector_checkpoints(",
            "has_stale_projector_checkpoints",
        ),
        ("const PROJECTOR_GENERATION", "PROJECTOR_GENERATION"),
    ] {
        assert!(
            attempt.contains(declaration),
            "Attempt does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/attempt.rs"],
            "unexpected production owners for {name}"
        );
    }
    for method in [
        "fn new(",
        "fn begin(",
        "fn state(",
        "fn root_signature(",
        "fn adopt_root_signature(",
        "fn finish(",
        "fn recover_interrupted_state(",
        "fn mark_cycle_failed(",
        "fn publish_projector_generation(",
    ] {
        assert!(
            attempt.contains(method),
            "AttemptRecorder does not expose its named operation `{method}`"
        );
    }
    assert!(
        !attempt.contains("fn set_meta("),
        "Attempt must expose named state transitions rather than a generic metadata writer"
    );

    for key in [
        "projector_generation",
        "ingest_state",
        "ingest_root_signature",
        "last_ingest_attempt_at",
        "last_scan_report",
        "last_ingest_error",
        "last_ingest_at",
    ] {
        let double_quoted = format!("\"{key}\"");
        let single_quoted = format!("'{key}'");
        assert!(
            raw_attempt.contains(&double_quoted) || raw_attempt.contains(&single_quoted),
            "Attempt does not own the `{key}` control key"
        );
        let owners = rust_files(&src.join("ingest"))
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                let production = production_module_source(&contents);
                production.contains(&double_quoted) || production.contains(&single_quoted)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/attempt.rs"],
            "ingestion control key `{key}` leaked outside Attempt"
        );
    }

    assert!(
        raw_attempt.contains("app_meta")
            && raw_attempt.contains("FROM source_files")
            && raw_attempt.contains("FROM threads"),
        "Attempt must own durable control metadata and both currentness reads"
    );
    for forbidden in [
        "INSERT INTO source_files",
        "UPDATE source_files",
        "DELETE FROM source_files",
        "INSERT INTO threads",
        "UPDATE threads",
        "DELETE FROM threads",
    ] {
        assert!(
            !raw_attempt.contains(forbidden),
            "Attempt mutates projection state through `{forbidden}`"
        );
    }
    let normalized_tables = forbidden_sql_table_hits(
        raw_attempt,
        &[
            "activity_event_index",
            "rollouts",
            "events",
            "messages",
            "turns",
            "tool_calls",
            "usage_facts",
            "agent_runs",
            "usage_activity_rollups",
            "usage_global_totals",
            "model_prices",
            "model_aliases",
            "schema_migrations",
        ],
    );
    assert!(
        normalized_tables.is_empty(),
        "Attempt reaches normalized projection tables: {}",
        normalized_tables.join(", ")
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod attempt"),
        "ingestion composition does not declare its private Attempt adapter"
    );
    assert!(
        raw_composition.contains("pub use attempt::projector_generation_is_current"),
        "ingestion composition must directly re-export the stable currentness query"
    );
    for removed in [
        "struct AttemptRecorder",
        "fn has_stale_projector_checkpoints(",
        "fn advance_projector_generation(",
        "fn finish_scan_meta(",
        "fn set_meta(",
        "const PROJECTOR_GENERATION_KEY",
    ] {
        assert!(
            !composition.contains(removed),
            "ingestion composition retains Attempt ownership via `{removed}`"
        );
    }
    let coordinator_source = source(&src.join("ingest/coordinator.rs"));
    let raw_coordinator = production_module_source(&coordinator_source);
    let coordinator = code_without_comments_and_literals(raw_coordinator);
    for operation in [
        "AttemptRecorder::new(db)",
        ".begin()",
        ".state()",
        ".root_signature()",
        ".adopt_root_signature(",
        ".finish(",
        ".recover_interrupted_state()",
        ".publish_projector_generation()",
    ] {
        assert!(
            coordinator.contains(operation),
            "Coordinator does not invoke named Attempt operation `{operation}`"
        );
    }
    let scanner_source = source(&src.join("ingest/scanner.rs"));
    let scanner = code_without_comments_and_literals(production_module_source(&scanner_source));
    assert!(
        scanner.contains(".mark_cycle_failed("),
        "Scanner must retain its named failed-cycle truthfulness transition"
    );
    assert!(
        !composition.contains(".mark_cycle_failed("),
        "the ingestion module root retains Scanner failure policy"
    );
    assert!(
        !coordinator.contains(".mark_cycle_failed("),
        "Coordinator must not absorb Scanner's failed-cycle transition"
    );
    for retained in [
        "fn finalize_unexpected_scan_error(",
        "fn finalize_scan_sequence_error(",
        "fn recover_interrupted_scan(",
        "canonical_utc_timestamp(Utc::now())",
        "serde_json::to_string(",
    ] {
        assert!(
            raw_coordinator.contains(retained),
            "error control, time, or report serialization left Coordinator via `{retained}`"
        );
    }
    let recovery = raw_coordinator
        .split_once("fn recover_interrupted_scan(")
        .map(|(_, suffix)| suffix)
        .expect("orchestrator must retain the public recovery wrapper");
    let acquire = recovery
        .find("DatabaseLock::acquire(db, \"ingest\")")
        .expect("recovery wrapper must acquire the lifetime process lock");
    let recover = recovery
        .find(".recover_interrupted_state()")
        .expect("recovery wrapper must invoke the named Attempt transition");
    assert!(
        acquire < recover,
        "recovery inspected durable state before acquiring the process lock"
    );
    for key in [
        "projector_generation",
        "ingest_state",
        "ingest_root_signature",
        "last_ingest_attempt_at",
        "last_scan_report",
        "last_ingest_error",
        "last_ingest_at",
    ] {
        let double_quoted = format!("\"{key}\"");
        let single_quoted = format!("'{key}'");
        assert!(
            !raw_composition.contains(&double_quoted)
                && !raw_composition.contains(&single_quoted)
                && !raw_coordinator.contains(&double_quoted)
                && !raw_coordinator.contains(&single_quoted),
            "ingestion composition or Coordinator writes Attempt control key `{key}` directly"
        );
    }
    assert!(
        !raw_composition.contains("app_meta") && !raw_coordinator.contains("app_meta"),
        "ingestion composition or Coordinator retains raw control-state SQL"
    );
}

#[test]
fn ingest_catalog_cut_five_single_owns_discovery_selection_and_topology_policy() {
    let root = manifest_root();
    let src = root.join("src");
    let catalog_path = src.join("ingest/catalog.rs");
    assert_eq!(
        assigned_role(&catalog_path),
        Some(Role::IngestCatalog),
        "Catalog candidate policy must live in its declared ingestion boundary"
    );

    let catalog_source = source(&catalog_path);
    let raw_catalog = production_module_source(&catalog_source);
    let catalog = code_without_comments_and_literals(raw_catalog);
    for import in raw_catalog
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("use "))
    {
        assert!(
            import.starts_with("use std::")
                || import.starts_with("use super::protocol")
                || import.starts_with("use crate::ingest::protocol")
                || import.starts_with("use anyhow::")
                || import.starts_with("use walkdir::"),
            "Catalog cut 5 imports a dependency beyond discovery and Protocol via `{import}`"
        );
    }
    assert!(
        catalog.contains("OwnerMeta"),
        "Catalog candidates must retain their decoded protocol owner"
    );

    let owned = [
        ("struct SourceCandidate", "SourceCandidate"),
        ("struct PendingEmptyOwners", "PendingEmptyOwners"),
        ("struct SelectedSourceExtent", "SelectedSourceExtent"),
        ("struct SourceHandoffIndex", "SourceHandoffIndex"),
        ("struct PreparedSourceCandidate", "PreparedSourceCandidate"),
        ("struct CatalogSelectionPlan", "CatalogSelectionPlan"),
        ("fn source_file_name_key(", "source_file_name_key"),
        ("fn source_is_complete(", "source_is_complete"),
        ("fn collect_jsonl(", "collect_jsonl"),
        (
            "fn owners_with_pending_empty_sources(",
            "owners_with_pending_empty_sources",
        ),
        (
            "fn rollout_id_from_source_path(",
            "rollout_id_from_source_path",
        ),
        (
            "fn source_candidate_preference(",
            "source_candidate_preference",
        ),
        ("fn resolve_owner_topology(", "resolve_owner_topology"),
        ("fn resolve_owner_thread(", "resolve_owner_thread"),
        ("fn plan_catalog_selection(", "plan_catalog_selection"),
    ];
    for (declaration, name) in owned {
        assert!(
            catalog.contains(declaration),
            "Catalog cut 5 does not own `{declaration}`"
        );
        let owners = rust_files(&src)
            .into_iter()
            .filter(|path| {
                let contents = source(path);
                production_module_source(&contents).contains(declaration)
            })
            .map(|path| relative(&path))
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            ["src/ingest/catalog.rs"],
            "unexpected owners for {name}"
        );
    }
    assert!(
        !catalog.contains("pub fn resolve_owner_thread(")
            && !catalog.contains("pub(super) fn resolve_owner_thread(")
            && !catalog.contains("pub(crate) fn resolve_owner_thread("),
        "Catalog's recursive topology helper must remain private"
    );

    let crate_dependencies = crate_dependency_roots(&catalog);
    assert!(
        crate_dependencies
            .iter()
            .all(|dependency| dependency == "ingest"),
        "Catalog cut 5 imports an upward crate boundary: {}",
        crate_dependencies.join(", ")
    );

    for forbidden in [
        "IngestRoots",
        "ScanReport",
        "FileReport",
        "fn reconcile_missing(",
        "fn sync_session_index_titles(",
        "files_failed",
        "AttemptRecorder",
    ] {
        assert!(
            !catalog.contains(forbidden),
            "Catalog discovery absorbed coordinator-owned scan policy via `{forbidden}`"
        );
    }
    for forbidden in [
        "no ingest roots are configured",
        "ingest_root_signature",
        "app_meta",
    ] {
        assert!(
            !raw_catalog.contains(forbidden),
            "Catalog discovery absorbed coordinator-owned root state via `{forbidden}`"
        );
    }

    let uuid_owners = rust_files(&src)
        .into_iter()
        .filter(|path| {
            let contents = source(path);
            production_module_source(&contents).contains("fn looks_like_uuid(")
        })
        .map(|path| relative(&path))
        .collect::<Vec<_>>();
    assert_eq!(
        uuid_owners,
        ["src/ingest/protocol/identifiers.rs"],
        "looks_like_uuid must remain single-owned by Protocol identifiers"
    );

    let composition_source = ingest_composition_source(&src);
    let raw_composition = production_module_source(&composition_source);
    let composition = code_without_comments_and_literals(raw_composition);
    assert!(
        composition.contains("mod catalog"),
        "the ingestion module root does not declare its private Catalog module"
    );
    assert!(
        !composition.contains("struct SourceCandidate"),
        "the ingestion module root duplicates SourceCandidate"
    );
    assert!(
        !composition.contains("type SourceCandidate"),
        "the ingestion module root aliases SourceCandidate"
    );
    assert!(
        !composition.contains("fn source_candidate_preference("),
        "the ingestion module root wraps Catalog candidate preference"
    );
    assert!(
        !composition.contains("fn resolve_owner_topology("),
        "the ingestion module root wraps Catalog topology resolution"
    );
    assert!(
        !composition.contains("fn resolve_owner_thread("),
        "the ingestion module root duplicates Catalog's recursive topology helper"
    );
    for (declaration, name) in [
        ("struct PendingEmptyOwners", "PendingEmptyOwners"),
        ("struct SelectedSourceExtent", "SelectedSourceExtent"),
        ("struct SourceHandoffIndex", "SourceHandoffIndex"),
        ("struct PreparedSourceCandidate", "PreparedSourceCandidate"),
        ("struct CatalogSelectionPlan", "CatalogSelectionPlan"),
        ("fn source_file_name_key(", "source_file_name_key"),
        ("fn source_is_complete(", "source_is_complete"),
        ("fn collect_jsonl(", "collect_jsonl"),
        (
            "fn owners_with_pending_empty_sources(",
            "owners_with_pending_empty_sources",
        ),
        (
            "fn rollout_id_from_source_path(",
            "rollout_id_from_source_path",
        ),
        ("fn plan_catalog_selection(", "plan_catalog_selection"),
    ] {
        assert!(
            !composition.contains(declaration),
            "the ingestion module root duplicates or wraps Catalog-owned {name}"
        );
    }
    assert!(
        raw_catalog
            .lines()
            .map(str::trim_start)
            .filter(|line| line.starts_with("use "))
            .any(|line| line.contains("protocol") && line.contains("looks_like_uuid")),
        "Catalog must import looks_like_uuid from Protocol"
    );
    assert!(
        !composition.contains("fn looks_like_uuid("),
        "the ingestion module root must import, rather than define, looks_like_uuid"
    );
    assert!(
        !composition.contains("fn load_selected_source_extents(")
            && !composition.contains("load_selected_source_extents(db)?"),
        "ingestion composition must neither own nor invoke selected-source extent persistence"
    );
    let checkpoint_store_source = source(&src.join("ingest/checkpoint_store.rs"));
    let raw_checkpoint_store = production_module_source(&checkpoint_store_source);
    assert!(
        raw_checkpoint_store.contains("fn load_selected_source_extents(")
            && raw_checkpoint_store.contains(
                "SELECT rollout_id,path,size_bytes,byte_offset,content_fingerprint FROM source_files"
            ),
        "CheckpointStore must own the selected-source extent adapter and its exact committed-state query"
    );
    let file_ingestor_source = source(&src.join("ingest/file_ingestor.rs"));
    let file_ingestor =
        code_without_comments_and_literals(production_module_source(&file_ingestor_source));
    let coordinator_source = source(&src.join("ingest/coordinator.rs"));
    let raw_coordinator = production_module_source(&coordinator_source);
    let coordinator = code_without_comments_and_literals(raw_coordinator);
    assert!(
        !coordinator.contains("fn load_selected_source_extents(")
            && coordinator.contains("load_selected_source_extents(db)?"),
        "Coordinator must call, not own, selected-source extent persistence"
    );
    let scan_coordination = raw_coordinator
        .split_once("pub struct IngestRoots")
        .map(|(_, suffix)| code_without_comments_and_literals(suffix))
        .expect("Coordinator must retain scan coordination after test-only support items");
    for retained in [
        "fn source_path_switch_is_ready(",
        "fn source_path_switch_is_ready_from_snapshot(",
        "SourceSnapshot::open",
        "full_content_fingerprints_from_snapshot",
        "stored_fingerprint_matches",
    ] {
        assert!(
            file_ingestor.contains(retained),
            "descriptor-backed fingerprint readiness left FileIngestor via `{retained}`"
        );
        assert!(
            !scan_coordination.contains(retained),
            "scan coordination retains FileIngestor readiness ownership via `{retained}`"
        );
    }
    assert!(
        catalog.contains("ready: bool"),
        "Catalog selection must consume explicit readiness evidence rather than opening sources"
    );
    assert!(
        coordinator.contains("file_ingestor.source_path_switch_is_ready(&candidate, extent)"),
        "scan coordination must obtain descriptor-backed readiness from FileIngestor before Catalog selection"
    );
    for retained in [
        "struct IngestRoots",
        "struct ScanReport",
        "report.files_failed",
        "let mut failures = Vec::new()",
        "report.files_seen = files.len() as u64",
        ".finish(",
    ] {
        assert!(
            coordinator.contains(retained),
            "scan coordination left Coordinator via `{retained}`"
        );
    }
    assert!(
        raw_coordinator.contains("no ingest roots are configured"),
        "scan coordination no longer reports the missing-roots failure"
    );
    for retained in [".root_signature()", ".adopt_root_signature("] {
        assert!(
            coordinator.contains(retained),
            "root lifecycle control flow left Coordinator via `{retained}`"
        );
    }
    assert!(
        !raw_composition.contains("ingest_root_signature")
            && !raw_composition.contains("app_meta")
            && !raw_coordinator.contains("ingest_root_signature")
            && !raw_coordinator.contains("app_meta"),
        "scan coordination bypasses Attempt's named durable-state operations"
    );
    assert!(
        !composition.contains("fn load_existing_owner_threads(")
            && !composition.contains("load_existing_owner_threads(db)?")
            && !coordinator.contains("fn load_existing_owner_threads(")
            && coordinator.contains("load_existing_owner_threads(db)?"),
        "scan coordination must call, not own, durable rollout topology persistence"
    );
    let topology_path = src.join("ingest/projection/topology.rs");
    assert_eq!(
        assigned_role(&topology_path),
        Some(Role::IngestProjectionConnection),
        "durable rollout topology must live in a raw-connection Projection adapter"
    );
    let topology_source = source(&topology_path);
    let raw_topology = production_module_source(&topology_source);
    assert!(
        raw_topology.contains("fn load_existing_owner_threads(")
            && raw_topology.contains("SELECT id,thread_id FROM rollouts"),
        "Projection topology must own the durable rollout-to-thread adapter and its exact query"
    );
    assert!(!composition.contains("pub mod catalog"));
    assert!(!composition.contains("pub use catalog"));
}

#[test]
fn ingestion_module_root_is_final_declaration_only_composition() {
    let root = manifest_root();
    let legacy_api = root.join("src/api.rs");
    assert!(
        !legacy_api.exists(),
        "the final module graph must not contain {}",
        legacy_api.display()
    );
    let legacy_ingest = root.join("src/ingest.rs");
    assert!(
        !legacy_ingest.exists(),
        "the final module graph must not contain {}",
        legacy_ingest.display()
    );

    let module_root = root.join("src/ingest/mod.rs");
    assert!(
        module_root.is_file(),
        "the final ingestion module is missing {}",
        module_root.display()
    );
    assert_eq!(
        assigned_role(&module_root),
        Some(Role::ModuleRoot),
        "the final ingestion composition must retain its explicit ModuleRoot role"
    );
    let module_source = source(&module_root);
    let production = production_module_source(&module_source);
    let module = code_without_comments_and_literals(production);
    let declarations = module.lines().map(str::trim).collect::<Vec<_>>();
    for private_module in [
        "attempt",
        "catalog",
        "checkpoint_store",
        "checkpoints",
        "coordinator",
        "file_ingestor",
        "owner_reader",
        "projection",
        "protocol",
        "reconciliation",
        "scanner",
        "session_titles",
        "source",
    ] {
        let declaration = format!("mod {private_module};");
        assert!(
            declarations.contains(&declaration.as_str()),
            "the ingestion module root is missing `{declaration}`"
        );
        assert!(
            !declarations.contains(&format!("pub mod {private_module};").as_str()),
            "the ingestion module root exposes private boundary `{private_module}`"
        );
    }
    assert!(
        production.contains("pub use attempt::projector_generation_is_current;")
            && production.contains("pub use coordinator::{")
            && production.contains("pub use scanner::{"),
        "stable ingestion entrypoints must remain direct owner-module re-exports"
    );
    assert_eq!(
        module
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("pub use "))
            .count(),
        3,
        "the ingestion module root must expose only its three stable re-export seams"
    );
    for algorithm in ["fn ", "struct ", "enum ", "impl ", "trait ", "type "] {
        assert!(
            !module.contains(algorithm),
            "the ingestion module root contains forwarding or implementation logic via `{algorithm}`"
        );
    }

    for required in [
        "src/app.rs",
        "src/web",
        "src/system",
        "src/pricing",
        "src/sessions",
        "src/activity",
        "src/analytics",
    ] {
        let path = root.join(required);
        let module_file = path.with_extension("rs");
        assert!(
            path.is_dir() || module_file.is_file(),
            "end-state composition is missing {required}"
        );
    }
    assert!(
        root.join("src/ingest").is_dir(),
        "final composition is missing the ingestion module tree"
    );
    let lib = fs::read_to_string(root.join("src/lib.rs")).unwrap();
    let lib_lines = lib.lines().map(str::trim).collect::<Vec<_>>();
    assert!(!lib_lines.contains(&"mod api;"));
    assert!(
        lib_lines.contains(&"pub mod ingest;"),
        "the final ingestion tree must retain its exact public module declaration"
    );
    assert!(!lib_lines.contains(&"mod ingest;"));
    for path in rust_files(&root.join("src")) {
        assert!(
            !source(&path).contains("ApiState"),
            "{} retains the legacy global service locator",
            relative(&path)
        );
    }
}
