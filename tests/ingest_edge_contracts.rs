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
            "thread_name": "Stale root title",
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
            "thread_name": "Child rollout is not a root title authority",
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

fn task_complete(timestamp: &str, turn: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {"type": "task_complete", "turn_id": turn}
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

fn assistant_message_with_id(timestamp: &str, id: &str, text: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "response_item",
        "payload": {
            "type": "message",
            "id": id,
            "role": "assistant",
            "phase": "final_answer",
            "content": [{"type": "output_text", "text": text}]
        }
    })
}

fn projected_message_id(rollout_id: &str, source_id: &str) -> String {
    format!("message:{}", json!([rollout_id, source_id]))
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

fn subagent_activity(
    timestamp: &str,
    agent_thread_id: &str,
    agent_path: &str,
    kind: &str,
) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {
            "type": "sub_agent_activity",
            "agent_thread_id": agent_thread_id,
            "agent_path": agent_path,
            "kind": kind
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

#[test]
fn message_ids_are_normalized_consistently_across_activity_relations() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("message-id.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            json!({
                "timestamp": "2026-07-15T09:00:02Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": " final-message ",
                    "role": "assistant",
                    "phase": "final_answer",
                    "content": [{"type": "output_text", "text": "Finished."}]
                }
            }),
        ],
    );

    scan_once(&harness.db, &harness.roots()).unwrap();
    let joined: (String, String, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT m.id,e.call_id,m.content
             FROM events e
             JOIN messages m
               ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
             WHERE e.kind='final'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        joined,
        (
            projected_message_id(ROOT, "final-message"),
            projected_message_id(ROOT, "final-message"),
            "Finished.".into(),
        )
    );
}

#[test]
fn repeated_source_message_ids_are_scoped_across_rollouts_in_one_thread() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("a-root.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            assistant_message_with_id("2026-07-15T09:00:02Z", "reused-message", "Root response."),
        ],
    );
    write_jsonl(
        &harness.active.join("z-child.jsonl"),
        &[
            child_meta("2026-07-15T09:00:03Z", CHILD, ROOT),
            task("2026-07-15T09:00:04Z", CHILD_TURN),
            assistant_message_with_id("2026-07-15T09:00:05Z", "reused-message", "Child response."),
        ],
    );

    assert_eq!(
        scan_once(&harness.db, &harness.roots())
            .unwrap()
            .files_failed,
        0
    );
    let rows = harness
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT m.rollout_id,m.id,e.call_id,m.content
             FROM messages m
             JOIN events e
               ON e.rollout_id=m.rollout_id
              AND e.call_id=m.id
              AND e.kind='final'
             WHERE m.thread_id=?1
             ORDER BY m.rollout_id",
        )
        .unwrap()
        .query_map([ROOT], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                ROOT.into(),
                projected_message_id(ROOT, "reused-message"),
                projected_message_id(ROOT, "reused-message"),
                "Root response.".into(),
            ),
            (
                CHILD.into(),
                projected_message_id(CHILD, "reused-message"),
                projected_message_id(CHILD, "reused-message"),
                "Child response.".into(),
            ),
        ]
    );
}

#[test]
fn repeated_source_message_ids_are_scoped_across_threads() {
    const OTHER_ROOT: &str = "019f64ae-0000-7000-8000-000000000000";
    const OTHER_TURN: &str = "019f64af-0000-7000-8000-000000000000";
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("a-root.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            assistant_message_with_id(
                "2026-07-15T09:00:02Z",
                "reused-message",
                "First thread response.",
            ),
        ],
    );
    write_jsonl(
        &harness.active.join("z-other-root.jsonl"),
        &[
            root_meta("2026-07-15T09:00:03Z", OTHER_ROOT),
            task("2026-07-15T09:00:04Z", OTHER_TURN),
            assistant_message_with_id(
                "2026-07-15T09:00:05Z",
                "reused-message",
                "Second thread response.",
            ),
        ],
    );

    assert_eq!(
        scan_once(&harness.db, &harness.roots())
            .unwrap()
            .files_failed,
        0
    );
    let rows = harness
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT m.thread_id,m.id,e.call_id,m.content
             FROM messages m
             JOIN events e
               ON e.thread_id=m.thread_id
              AND e.rollout_id=m.rollout_id
              AND e.call_id=m.id
              AND e.kind='final'
             ORDER BY m.thread_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                ROOT.into(),
                projected_message_id(ROOT, "reused-message"),
                projected_message_id(ROOT, "reused-message"),
                "First thread response.".into(),
            ),
            (
                OTHER_ROOT.into(),
                projected_message_id(OTHER_ROOT, "reused-message"),
                projected_message_id(OTHER_ROOT, "reused-message"),
                "Second thread response.".into(),
            ),
        ]
    );
}

#[test]
fn oversized_message_identifier_rolls_back_the_file_projection() {
    let harness = Harness::new();
    let oversized_id = "m".repeat(257);
    write_jsonl(
        &harness.active.join("oversized-message-id.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            task("2026-07-15T09:00:01Z", ROOT_TURN),
            json!({
                "timestamp": "2026-07-15T09:00:02Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": oversized_id,
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Finished."}]
                }
            }),
        ],
    );

    let error = scan_once(&harness.db, &harness.roots()).unwrap_err();
    assert!(
        format!("{error:#}").contains("message id exceeds the 256-character identifier limit"),
        "unexpected error: {error:#}"
    );
    let connection = harness.db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM rollouts"), 0);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM messages"), 0);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM events"), 0);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM source_files"), 0);
}

fn ingest_owner_metadata(
    root_name: &str,
    child_name: &str,
) -> (String, String, String, String, String, Option<String>, i64) {
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
        None,
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

#[derive(Debug, PartialEq)]
struct ProjectedThreadMetadata {
    id: String,
    title: Option<String>,
    cwd: Option<String>,
    project: Option<String>,
    repository_url: Option<String>,
    branch: Option<String>,
    source: Option<String>,
    thread_source: Option<String>,
    source_json: Option<String>,
    started_at: String,
    last_event_at: String,
    title_updated_at: Option<String>,
    root_metadata_seen: i64,
}

fn projected_thread_metadata(connection: &Connection) -> ProjectedThreadMetadata {
    connection
        .query_row(
            "SELECT id,title,cwd,project,repository_url,branch,source,thread_source,source_json,
                    started_at,last_event_at,title_updated_at,root_metadata_seen
             FROM threads WHERE id=?1",
            [ROOT],
            |row| {
                Ok(ProjectedThreadMetadata {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    cwd: row.get(2)?,
                    project: row.get(3)?,
                    repository_url: row.get(4)?,
                    branch: row.get(5)?,
                    source: row.get(6)?,
                    thread_source: row.get(7)?,
                    source_json: row.get(8)?,
                    started_at: row.get(9)?,
                    last_event_at: row.get(10)?,
                    title_updated_at: row.get(11)?,
                    root_metadata_seen: row.get(12)?,
                })
            },
        )
        .unwrap()
}

#[test]
fn removing_root_rollout_recomputes_thread_metadata_from_surviving_child() {
    let incremental = Harness::new();
    let root_path = incremental.active.join("a-root.jsonl");
    write_jsonl(
        &root_path,
        &[authoritative_root_meta("2026-07-15T09:00:00Z")],
    );
    write_jsonl(
        &incremental.active.join("z-child.jsonl"),
        &[conflicting_child_meta("2026-07-15T09:01:00Z")],
    );
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );
    fs::remove_file(root_path).unwrap();
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );

    let fresh = Harness::new();
    write_jsonl(
        &fresh.active.join("z-child.jsonl"),
        &[conflicting_child_meta("2026-07-15T09:01:00Z")],
    );
    assert_eq!(
        scan_once(&fresh.db, &fresh.roots()).unwrap().files_failed,
        0
    );

    assert_eq!(
        projected_thread_metadata(&incremental.db.connect().unwrap()),
        projected_thread_metadata(&fresh.db.connect().unwrap())
    );
    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );
}

#[test]
fn removing_promoted_child_restores_surviving_parent_agent_observation() {
    let root_records = [
        root_meta("2026-07-15T09:00:00Z", ROOT),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/contract-child",
            "completed",
        ),
    ];
    let child_records = [child_meta("2026-07-15T09:01:00Z", CHILD, ROOT)];

    let incremental = Harness::new();
    write_jsonl(&incremental.active.join("a-root.jsonl"), &root_records);
    let child_path = incremental.active.join("z-child.jsonl");
    write_jsonl(&child_path, &child_records);
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );
    fs::remove_file(child_path).unwrap();
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );

    let fresh = Harness::new();
    write_jsonl(&fresh.active.join("a-root.jsonl"), &root_records);
    assert_eq!(
        scan_once(&fresh.db, &fresh.roots()).unwrap().files_failed,
        0
    );

    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );
    let restored: (Option<String>, String, String, Option<String>, String) = incremental
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT rollout_id,parent_rollout_id,status,completed_at,agent_path
             FROM agent_runs WHERE id=?1",
            [CHILD],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        restored,
        (
            None,
            ROOT.into(),
            "completed".into(),
            Some("2026-07-15T09:00:01.000000000Z".into()),
            "/root/contract-child".into(),
        )
    );
}

#[test]
fn removing_only_parent_restores_promoted_child_native_metadata() {
    let parent_records = [
        root_meta("2026-07-15T09:00:00Z", ROOT),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/from-removed-parent",
            "started",
        ),
    ];
    let child_records = [
        conflicting_child_meta("2026-07-15T09:00:02Z"),
        task("2026-07-15T09:00:03Z", CHILD_TURN),
    ];

    let incremental = Harness::new();
    let removed_parent = incremental.active.join("a-parent.jsonl");
    write_jsonl(&removed_parent, &parent_records);
    write_jsonl(&incremental.active.join("z-child.jsonl"), &child_records);
    scan_once(&incremental.db, &incremental.roots()).unwrap();
    fs::remove_file(removed_parent).unwrap();
    scan_once(&incremental.db, &incremental.roots()).unwrap();

    let fresh = Harness::new();
    write_jsonl(&fresh.active.join("z-child.jsonl"), &child_records);
    scan_once(&fresh.db, &fresh.roots()).unwrap();

    let projected_agent = |connection: &Connection| {
        connection
            .query_row(
                "SELECT thread_id,parent_rollout_id,agent_path,nickname,started_at
                 FROM agent_runs WHERE id=?1 AND rollout_id=?1",
                [CHILD],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .unwrap()
    };
    let incremental_agent = projected_agent(&incremental.db.connect().unwrap());
    let fresh_agent = projected_agent(&fresh.db.connect().unwrap());

    assert_eq!(incremental_agent, fresh_agent);
    assert_eq!(
        incremental_agent,
        (
            ROOT.into(),
            Some(ROOT.into()),
            Some("/root/metadata-child".into()),
            Some("Noether".into()),
            "2026-07-15T09:00:02.000000000Z".into(),
        )
    );
    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );
}

#[test]
fn removing_one_parent_rematerializes_a_child_observed_by_another_parent() {
    const PARENT_A: &str = "019f64aa-0000-7000-8000-0000000000a1";
    const PARENT_B: &str = "019f64aa-0000-7000-8000-0000000000b1";
    let parent_a_records = [
        root_meta("2026-07-15T09:00:00Z", PARENT_A),
        subagent_activity(
            "2026-07-15T09:00:02Z",
            CHILD,
            "/root/from-parent-a",
            "completed",
        ),
    ];
    let parent_b_records = [
        root_meta("2026-07-15T09:00:00Z", PARENT_B),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/from-parent-b",
            "started",
        ),
    ];

    let incremental = Harness::new();
    write_jsonl(
        &incremental.active.join("a-parent-b.jsonl"),
        &parent_b_records,
    );
    let removed_parent = incremental.active.join("z-parent-a.jsonl");
    write_jsonl(&removed_parent, &parent_a_records);
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );
    fs::remove_file(removed_parent).unwrap();
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );

    let fresh = Harness::new();
    write_jsonl(&fresh.active.join("a-parent-b.jsonl"), &parent_b_records);
    assert_eq!(
        scan_once(&fresh.db, &fresh.roots()).unwrap().files_failed,
        0
    );

    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );
    let surviving_child: (Option<String>, String, String, Option<String>, String) = incremental
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT rollout_id,parent_rollout_id,status,completed_at,agent_path
             FROM agent_runs WHERE id=?1",
            [CHILD],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        surviving_child,
        (
            None,
            PARENT_B.into(),
            "running".into(),
            None,
            "/root/from-parent-b".into(),
        )
    );
}

#[test]
fn removing_earlier_non_winning_parent_can_raise_synthetic_child_start() {
    const EARLIER_PARENT: &str = "019f64aa-0000-7000-8000-0000000000c1";
    const LATER_PARENT: &str = "019f64aa-0000-7000-8000-0000000000d1";
    let earlier_records = [
        root_meta("2026-07-15T09:00:00Z", EARLIER_PARENT),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/from-earlier-parent",
            "started",
        ),
    ];
    let later_records = [
        root_meta("2026-07-15T09:00:00Z", LATER_PARENT),
        subagent_activity(
            "2026-07-15T09:00:02Z",
            CHILD,
            "/root/from-later-parent",
            "completed",
        ),
    ];

    let incremental = Harness::new();
    let removed_parent = incremental.active.join("a-earlier-parent.jsonl");
    write_jsonl(&removed_parent, &earlier_records);
    write_jsonl(
        &incremental.active.join("z-later-parent.jsonl"),
        &later_records,
    );
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );
    let before_removal: (String, String) = incremental
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT parent_rollout_id,started_at FROM agent_runs WHERE id=?1",
            [CHILD],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        before_removal,
        (LATER_PARENT.into(), "2026-07-15T09:00:01.000000000Z".into(),)
    );

    fs::remove_file(removed_parent).unwrap();
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );

    let fresh = Harness::new();
    write_jsonl(&fresh.active.join("z-later-parent.jsonl"), &later_records);
    assert_eq!(
        scan_once(&fresh.db, &fresh.roots()).unwrap().files_failed,
        0
    );
    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );

    let rebuilt: (String, String, String, Option<String>, String) = incremental
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT parent_rollout_id,started_at,status,completed_at,agent_path
             FROM agent_runs WHERE id=?1",
            [CHILD],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        rebuilt,
        (
            LATER_PARENT.into(),
            "2026-07-15T09:00:02.000000000Z".into(),
            "completed".into(),
            Some("2026-07-15T09:00:02.000000000Z".into()),
            "/root/from-later-parent".into(),
        )
    );
}

#[test]
fn equal_timestamp_parent_observations_converge_after_incremental_discovery() {
    const PARENT_A: &str = "019f64aa-0000-7000-8000-0000000000a3";
    const PARENT_B: &str = "019f64aa-0000-7000-8000-0000000000b3";
    let parent_a_records = [
        root_meta("2026-07-15T09:00:00Z", PARENT_A),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/from-parent-a",
            "completed",
        ),
    ];
    let parent_b_records = [
        root_meta("2026-07-15T09:00:00Z", PARENT_B),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/from-parent-b",
            "started",
        ),
    ];

    let incremental = Harness::new();
    write_jsonl(
        &incremental.active.join("z-parent-b.jsonl"),
        &parent_b_records,
    );
    scan_once(&incremental.db, &incremental.roots()).unwrap();
    write_jsonl(
        &incremental.active.join("a-parent-a.jsonl"),
        &parent_a_records,
    );
    scan_once(&incremental.db, &incremental.roots()).unwrap();

    let fresh = Harness::new();
    write_jsonl(&fresh.active.join("a-parent-a.jsonl"), &parent_a_records);
    write_jsonl(&fresh.active.join("z-parent-b.jsonl"), &parent_b_records);
    scan_once(&fresh.db, &fresh.roots()).unwrap();

    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );
    let child: (String, String, String, Option<String>) = incremental
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT parent_rollout_id,agent_path,status,completed_at
             FROM agent_runs WHERE id=?1",
            [CHILD],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        child,
        (
            PARENT_B.into(),
            "/root/from-parent-b".into(),
            "running".into(),
            None,
        )
    );
}

#[test]
fn removing_terminal_parent_restores_promoted_child_from_surviving_running_parent() {
    const PARENT_A: &str = "019f64aa-0000-7000-8000-0000000000a2";
    const PARENT_B: &str = "019f64aa-0000-7000-8000-0000000000b2";
    let parent_a_records = [
        root_meta("2026-07-15T09:00:00Z", PARENT_A),
        subagent_activity(
            "2026-07-15T09:00:04Z",
            CHILD,
            "/root/from-parent-a",
            "completed",
        ),
    ];
    let parent_b_records = [
        root_meta("2026-07-15T09:00:00Z", PARENT_B),
        subagent_activity(
            "2026-07-15T09:00:01Z",
            CHILD,
            "/root/from-parent-b",
            "started",
        ),
    ];
    let child_records = [
        child_meta("2026-07-15T09:00:02Z", CHILD, PARENT_B),
        task("2026-07-15T09:00:03Z", CHILD_TURN),
    ];

    let incremental = Harness::new();
    write_jsonl(
        &incremental.active.join("a-parent-b.jsonl"),
        &parent_b_records,
    );
    write_jsonl(&incremental.active.join("m-child.jsonl"), &child_records);
    let removed_parent = incremental.active.join("z-parent-a.jsonl");
    write_jsonl(&removed_parent, &parent_a_records);
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );
    fs::remove_file(removed_parent).unwrap();
    assert_eq!(
        scan_once(&incremental.db, &incremental.roots())
            .unwrap()
            .files_failed,
        0
    );

    let fresh = Harness::new();
    write_jsonl(&fresh.active.join("a-parent-b.jsonl"), &parent_b_records);
    write_jsonl(&fresh.active.join("m-child.jsonl"), &child_records);
    assert_eq!(
        scan_once(&fresh.db, &fresh.roots()).unwrap().files_failed,
        0
    );

    assert_eq!(
        canonical_projection(&incremental.db.connect().unwrap()),
        canonical_projection(&fresh.db.connect().unwrap())
    );
    let child: (String, Option<String>, String, String, Option<String>) = incremental
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                status,
                completed_at,
                agent_path,
                (SELECT status FROM turns WHERE id=?2),
                (SELECT completed_at FROM turns WHERE id=?2)
             FROM agent_runs WHERE id=?1 AND rollout_id=?1",
            [CHILD, CHILD_TURN],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        child,
        (
            "running".into(),
            None,
            "/root/from-parent-b".into(),
            "running".into(),
            None,
        )
    );
}

fn ingest_promoted_child_lifecycle(
    parent_name: &str,
    child_name: &str,
) -> ((String, Option<String>), (String, Option<String>)) {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join(parent_name),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            subagent_activity(
                "2026-07-15T09:00:03Z",
                CHILD,
                "/root/contract-child",
                "completed",
            ),
        ],
    );
    write_jsonl(
        &harness.active.join(child_name),
        &[
            child_meta("2026-07-15T09:00:01Z", CHILD, ROOT),
            task("2026-07-15T09:00:02Z", CHILD_TURN),
        ],
    );
    assert_eq!(
        scan_once(&harness.db, &harness.roots())
            .unwrap()
            .files_failed,
        0
    );
    harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                status,
                completed_at,
                (SELECT status FROM turns WHERE id=?2),
                (SELECT completed_at FROM turns WHERE id=?2)
             FROM agent_runs WHERE id=?1",
            [CHILD, CHILD_TURN],
            |row| Ok(((row.get(0)?, row.get(1)?), (row.get(2)?, row.get(3)?))),
        )
        .unwrap()
}

#[test]
fn terminal_parent_closes_an_open_promoted_child_in_both_discovery_orders() {
    let terminal = (
        "completed".into(),
        Some("2026-07-15T09:00:03.000000000Z".into()),
    );
    let expected = (terminal.clone(), terminal);
    assert_eq!(
        ingest_promoted_child_lifecycle("a-parent.jsonl", "z-child.jsonl"),
        expected
    );
    assert_eq!(
        ingest_promoted_child_lifecycle("z-parent.jsonl", "a-child.jsonl"),
        expected
    );
}

#[test]
fn native_child_terminal_and_newer_running_evidence_remain_authoritative() {
    let completed = Harness::new();
    write_jsonl(
        &completed.active.join("a-child.jsonl"),
        &[
            child_meta("2026-07-15T09:00:01Z", CHILD, ROOT),
            task("2026-07-15T09:00:02Z", CHILD_TURN),
            task_complete("2026-07-15T09:00:03Z", CHILD_TURN),
        ],
    );
    write_jsonl(
        &completed.active.join("z-parent.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            subagent_activity(
                "2026-07-15T09:00:04Z",
                CHILD,
                "/root/contract-child",
                "interrupted",
            ),
        ],
    );
    scan_once(&completed.db, &completed.roots()).unwrap();
    let completed_state: (String, Option<String>, String, Option<String>) = completed
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                status,
                completed_at,
                (SELECT status FROM turns WHERE id=?2),
                (SELECT completed_at FROM turns WHERE id=?2)
             FROM agent_runs WHERE id=?1",
            [CHILD, CHILD_TURN],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        completed_state,
        (
            "completed".into(),
            Some("2026-07-15T09:00:03.000000000Z".into()),
            "completed".into(),
            Some("2026-07-15T09:00:03.000000000Z".into()),
        )
    );

    let newer_native = Harness::new();
    write_jsonl(
        &newer_native.active.join("a-parent.jsonl"),
        &[
            root_meta("2026-07-15T09:00:00Z", ROOT),
            subagent_activity(
                "2026-07-15T09:00:03Z",
                CHILD,
                "/root/contract-child",
                "completed",
            ),
        ],
    );
    write_jsonl(
        &newer_native.active.join("z-child.jsonl"),
        &[
            child_meta("2026-07-15T09:00:01Z", CHILD, ROOT),
            task("2026-07-15T09:00:04Z", CHILD_TURN),
        ],
    );
    scan_once(&newer_native.db, &newer_native.roots()).unwrap();
    let newer_state: (String, Option<String>, String, Option<String>) = newer_native
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                status,
                completed_at,
                (SELECT status FROM turns WHERE id=?2),
                (SELECT completed_at FROM turns WHERE id=?2)
             FROM agent_runs WHERE id=?1",
            [CHILD, CHILD_TURN],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        newer_state,
        ("running".into(), None, "running".into(), None)
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
