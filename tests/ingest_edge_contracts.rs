use codex_usage::{
    db::Db,
    ingest::{IngestRoots, scan_once},
};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

const ROOT: &str = "019f64aa-0000-7000-8000-000000000000";
const ROOT_TURN: &str = "019f64ab-0000-7000-8000-000000000000";
const CHILD: &str = "019f64ac-0000-7000-8000-000000000000";
const CHILD_TURN: &str = "019f64ad-0000-7000-8000-000000000000";

struct Harness {
    _temp: TempDir,
    db: Db,
    active: PathBuf,
    archive: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        fs::create_dir_all(&active).unwrap();
        fs::create_dir_all(&archive).unwrap();
        let db = Db::open(temp.path().join("data/codex-usage.db")).unwrap();
        Self {
            _temp: temp,
            db,
            active,
            archive,
        }
    }

    fn roots(&self) -> IngestRoots {
        IngestRoots {
            active: Some(self.active.clone()),
            archive: Some(self.archive.clone()),
        }
    }
}

fn write_jsonl(path: &Path, records: &[Value]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(path).unwrap();
    for record in records {
        writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
    }
}

fn root_meta(timestamp: &str, owner: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": owner,
            "session_id": owner,
            "cwd": "/tmp/ingest-contracts",
            "source": "vscode"
        }
    })
}

fn child_meta(timestamp: &str, owner: &str, parent: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": owner,
            "session_id": parent,
            "cwd": "/tmp/ingest-contracts",
            "source": {
                "subagent": {
                    "thread_spawn": {
                        "parent_thread_id": parent,
                        "parent_rollout_id": parent,
                        "agent_path": "/root/contract-child",
                        "agent_nickname": "Turing"
                    }
                }
            }
        }
    })
}

fn authoritative_root_meta(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": ROOT,
            "session_id": ROOT,
            "cwd": "/tmp/root-authoritative",
            "source": "vscode",
            "thread_source": "user",
            "git": {
                "repository_url": "https://example.test/root.git",
                "branch": "root-main"
            }
        }
    })
}

fn conflicting_child_meta(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": CHILD,
            "session_id": ROOT,
            "cwd": "/tmp/child-metadata",
            "thread_source": "subagent",
            "git": {
                "repository_url": "https://example.test/child.git",
                "branch": "child-branch"
            },
            "source": {
                "subagent": {
                    "thread_spawn": {
                        "parent_thread_id": ROOT,
                        "parent_rollout_id": ROOT,
                        "agent_path": "/root/metadata-child",
                        "agent_nickname": "Noether"
                    }
                }
            }
        }
    })
}

fn task(timestamp: &str, turn: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {"type": "task_started", "turn_id": turn}
    })
}

fn context(timestamp: &str, turn: &str, model: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "turn_context",
        "payload": {"turn_id": turn, "model": model, "effort": "high"}
    })
}

fn usage(
    timestamp: &str,
    total_input: u64,
    total_output: u64,
    last_input: u64,
    last_output: u64,
) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": total_input,
                    "cached_input_tokens": 0,
                    "output_tokens": total_output,
                    "reasoning_output_tokens": 0,
                    "total_tokens": total_input + total_output
                },
                "last_token_usage": {
                    "input_tokens": last_input,
                    "cached_input_tokens": 0,
                    "output_tokens": last_output,
                    "reasoning_output_tokens": 0,
                    "total_tokens": last_input + last_output
                }
            }
        }
    })
}

fn assistant_message(timestamp: &str, text: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "assistant",
            "phase": "commentary",
            "content": [{"type": "output_text", "text": text}]
        }
    })
}

fn tool_call(timestamp: &str, call_id: &str, command: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "call_id": call_id,
            "name": "exec_command",
            "arguments": json!({"cmd": command}).to_string()
        }
    })
}

fn tool_output(timestamp: &str, call_id: &str, output: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "call_id": call_id,
            "output": output
        }
    })
}

fn scalar(connection: &Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn canonical_projection(connection: &Connection) -> Vec<String> {
    let sql = r#"
        SELECT row_value FROM (
            SELECT 'thread|' || id || '|' || COALESCE(title,'') || '|' ||
                   COALESCE(cwd,'') || '|' || started_at || '|' || last_event_at AS row_value
            FROM threads
            UNION ALL
            SELECT 'rollout|' || id || '|' || thread_id || '|' ||
                   COALESCE(parent_rollout_id,'') || '|' || COALESCE(agent_path,'') || '|' ||
                   archived
            FROM rollouts
            UNION ALL
            SELECT 'agent|' || id || '|' || thread_id || '|' ||
                   COALESCE(parent_rollout_id,'') || '|' || COALESCE(agent_path,'') || '|' ||
                   COALESCE(nickname,'') || '|' || status
            FROM agent_runs
            UNION ALL
            SELECT 'turn|' || id || '|' || thread_id || '|' || rollout_id || '|' ||
                   status || '|' || COALESCE(model,'') || '|' || COALESCE(effort,'')
            FROM turns
            UNION ALL
            SELECT 'message|' || id || '|' || thread_id || '|' || rollout_id || '|' ||
                   COALESCE(turn_id,'') || '|' || timestamp || '|' || role || '|' || content ||
                   '|' || source_line
            FROM messages
            UNION ALL
            SELECT 'event|' || id || '|' || thread_id || '|' || rollout_id || '|' ||
                   COALESCE(turn_id,'') || '|' || timestamp || '|' || source_line || '|' || kind ||
                   '|' || COALESCE(role,'') || '|' || COALESCE(label,'') || '|' ||
                   COALESCE(status,'') || '|' || COALESCE(call_id,'')
            FROM events
            UNION ALL
            SELECT 'tool|' || id || '|' || call_id || '|' || thread_id || '|' || rollout_id ||
                   '|' || COALESCE(turn_id,'') || '|' || COALESCE(namespace,'') || '|' || name ||
                   '|' || status || '|' || started_at || '|' || COALESCE(completed_at,'') || '|' ||
                   COALESCE(duration_ms,'')
            FROM tool_calls
            UNION ALL
            SELECT 'usage|' || id || '|' || thread_id || '|' || rollout_id || '|' ||
                   COALESCE(turn_id,'') || '|' || timestamp || '|' || source_line || '|' || model ||
                   '|' || COALESCE(effort,'') || '|' || input_tokens || '|' || output_tokens || '|' ||
                   total_tokens
            FROM usage_facts
        ) ORDER BY row_value
    "#;
    connection
        .prepare(sql)
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap()
}

#[test]
fn simultaneous_active_and_archive_copies_have_one_normalized_projection() {
    let harness = Harness::new();
    let records = vec![
        root_meta("2026-07-15T09:00:00Z", ROOT),
        task("2026-07-15T09:00:01Z", ROOT_TURN),
        context("2026-07-15T09:00:01Z", ROOT_TURN, "gpt-copy"),
        assistant_message("2026-07-15T09:00:02Z", "A single durable projection."),
        usage("2026-07-15T09:00:03Z", 100, 10, 100, 10),
    ];
    let active_path = harness.active.join("z-active-copy.jsonl");
    let archive_path = harness.archive.join("a-archive-copy.jsonl");
    write_jsonl(&active_path, &records);
    write_jsonl(&archive_path, &records);

    let first = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(first.files_seen, 2);
    assert_eq!(first.files_ingested, 1);
    assert_eq!(first.files_unchanged, 0);
    assert_eq!(first.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM source_files"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM threads"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM rollouts"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM messages"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM usage_facts"), 1);
    assert_eq!(
        scalar(
            &connection,
            "SELECT COALESCE(SUM(total_tokens),0) FROM usage_facts"
        ),
        110
    );
    let selected_source: (String, i64) = connection
        .query_row("SELECT path,archived FROM source_files", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(Path::new(&selected_source.0), active_path);
    assert_eq!(
        selected_source.1, 0,
        "the active copy is canonical while both exist"
    );
    let before = canonical_projection(&connection);
    drop(connection);

    let second = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(second.files_failed, 0);
    assert_eq!(second.files_seen, 2);
    assert_eq!(second.files_ingested, 0);
    assert_eq!(second.files_unchanged, 1);
    assert_eq!(second.records_read, 0);
    assert_eq!(
        canonical_projection(&harness.db.connect().unwrap()),
        before,
        "repeated discovery of both copies must not multiply the projection"
    );
}

#[test]
fn larger_complete_archive_copy_wins_over_smaller_active_copy() {
    let harness = Harness::new();
    let prefix = vec![
        root_meta("2026-07-15T09:00:00Z", ROOT),
        task("2026-07-15T09:00:01Z", ROOT_TURN),
        context("2026-07-15T09:00:01Z", ROOT_TURN, "gpt-copy"),
        assistant_message("2026-07-15T09:00:02Z", "Present in both copies."),
    ];
    let mut archive_records = prefix.clone();
    archive_records.push(assistant_message(
        "2026-07-15T09:00:03Z",
        "Only the larger complete copy has this record.",
    ));
    let active_path = harness.active.join("active-smaller.jsonl");
    let archive_path = harness.archive.join("archive-larger.jsonl");
    write_jsonl(&active_path, &prefix);
    write_jsonl(&archive_path, &archive_records);

    let first = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(first.files_seen, 2);
    assert_eq!(first.files_ingested, 1);
    assert_eq!(first.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    let selected_source: (String, i64) = connection
        .query_row("SELECT path,archived FROM source_files", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(Path::new(&selected_source.0), archive_path);
    assert_eq!(selected_source.1, 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM messages"), 2);
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM messages
             WHERE content='Only the larger complete copy has this record.'"
        ),
        1
    );
    drop(connection);

    let second = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(second.files_seen, 2);
    assert_eq!(second.files_ingested, 0);
    assert_eq!(second.files_unchanged, 1);
    assert_eq!(second.records_read, 0);
}

fn ingest_topology(root_name: &str, child_name: &str) -> Vec<String> {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join(root_name),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            context("2026-07-15T09:00:01Z", ROOT_TURN, "gpt-root"),
            assistant_message("2026-07-15T09:00:02Z", "Root work."),
            usage("2026-07-15T09:00:03Z", 100, 10, 100, 10),
        ],
    );
    write_jsonl(
        &harness.active.join(child_name),
        &[
            child_meta("2026-07-15T09:01:00Z", CHILD, ROOT),
            task("2026-07-15T09:01:01Z", CHILD_TURN),
            context("2026-07-15T09:01:01Z", CHILD_TURN, "gpt-child"),
            assistant_message("2026-07-15T09:01:02Z", "Child work."),
            usage("2026-07-15T09:01:03Z", 50, 5, 50, 5),
        ],
    );
    let report = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(report.files_failed, 0);
    canonical_projection(&harness.db.connect().unwrap())
}

#[test]
fn projection_is_deterministic_when_source_names_reverse_discovery_order() {
    let root_first = ingest_topology("a-root.jsonl", "z-child.jsonl");
    let child_first = ingest_topology("z-root.jsonl", "a-child.jsonl");
    assert_eq!(child_first, root_first);
}

fn ingest_owner_metadata(
    root_name: &str,
    child_name: &str,
) -> (String, String, String, String, String, String, i64) {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join(root_name),
        &[authoritative_root_meta("2026-07-15T09:00:00Z")],
    );
    write_jsonl(
        &harness.active.join(child_name),
        &[conflicting_child_meta("2026-07-15T09:01:00Z")],
    );

    let report = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(report.files_failed, 0);
    harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT cwd,project,repository_url,branch,source,source_json,root_metadata_seen
             FROM threads WHERE id=?1",
            [ROOT],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap()
}

#[test]
fn native_root_metadata_is_authoritative_in_both_discovery_orders() {
    let expected = (
        "/tmp/root-authoritative".into(),
        "root-authoritative".into(),
        "https://example.test/root.git".into(),
        "root-main".into(),
        "vscode".into(),
        "\"vscode\"".into(),
        1,
    );
    assert_eq!(
        ingest_owner_metadata("a-root.jsonl", "z-child.jsonl"),
        expected
    );
    assert_eq!(
        ingest_owner_metadata("z-root.jsonl", "a-child.jsonl"),
        expected
    );
}

#[test]
fn cumulative_reset_after_model_switch_materializes_separate_model_facts() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("model-reset.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            context("2026-07-15T09:00:01Z", ROOT_TURN, "gpt-before-reset"),
            usage("2026-07-15T09:00:02Z", 100, 10, 100, 10),
            context("2026-07-15T09:00:03Z", ROOT_TURN, "gpt-after-reset"),
            usage("2026-07-15T09:00:04Z", 20, 3, 20, 3),
            usage("2026-07-15T09:00:05Z", 27, 5, 7, 2),
        ],
    );

    let report = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(report.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    let facts = connection
        .prepare(
            "SELECT model,input_tokens,output_tokens,total_tokens
             FROM usage_facts ORDER BY source_line",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        facts,
        vec![
            ("gpt-before-reset".into(), 100, 10, 110),
            ("gpt-after-reset".into(), 20, 3, 23),
            ("gpt-after-reset".into(), 7, 2, 9),
        ]
    );
    let by_model = connection
        .prepare(
            "SELECT model,SUM(input_tokens),SUM(output_tokens),SUM(total_tokens)
             FROM usage_facts GROUP BY model ORDER BY model",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        by_model,
        vec![
            ("gpt-after-reset".into(), 27, 5, 32),
            ("gpt-before-reset".into(), 100, 10, 110),
        ]
    );
}

#[test]
fn identical_message_text_on_distinct_source_lines_survives_twice() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("repeated-text.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            context("2026-07-15T09:00:01Z", ROOT_TURN, "gpt-repeat"),
            assistant_message("2026-07-15T09:00:02Z", "Same words, separate updates."),
            assistant_message("2026-07-15T09:00:03Z", "Same words, separate updates."),
        ],
    );

    let report = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(report.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    let messages = connection
        .prepare(
            "SELECT id,source_line,content FROM messages
             WHERE rollout_id=?1 ORDER BY source_line",
        )
        .unwrap()
        .query_map([ROOT], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        messages,
        vec![
            (
                format!("{ROOT}:4"),
                4,
                "Same words, separate updates.".into()
            ),
            (
                format!("{ROOT}:5"),
                5,
                "Same words, separate updates.".into()
            ),
        ]
    );
}

#[test]
fn duplicate_tool_call_id_upserts_once_while_distinct_id_is_preserved() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("tool-identities.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            context("2026-07-15T09:00:01Z", ROOT_TURN, "gpt-tools"),
            tool_call("2026-07-15T09:00:02.000Z", "call-shared", "pwd"),
            tool_call("2026-07-15T09:00:02.001Z", "call-shared", "pwd"),
            tool_call("2026-07-15T09:00:02.002Z", "call-distinct", "pwd"),
            tool_output("2026-07-15T09:00:03.000Z", "call-shared", "/tmp"),
            tool_output("2026-07-15T09:00:03.001Z", "call-shared", "/tmp"),
            tool_output("2026-07-15T09:00:03.002Z", "call-distinct", "/tmp"),
        ],
    );

    let report = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(report.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    let tools = connection
        .prepare(
            "SELECT call_id,name,status FROM tool_calls
             WHERE rollout_id=?1 ORDER BY call_id",
        )
        .unwrap()
        .query_map([ROOT], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        tools,
        vec![
            (
                "call-distinct".into(),
                "exec_command".into(),
                "completed".into()
            ),
            (
                "call-shared".into(),
                "exec_command".into(),
                "completed".into()
            ),
        ]
    );
}
