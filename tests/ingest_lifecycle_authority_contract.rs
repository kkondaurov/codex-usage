use codex_usage::{
    ingest::{IngestRoots, scan_once},
    storage::Db,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

const ROOT: &str = "019f9000-0000-7000-8000-000000000001";
const FIRST_TURN: &str = "019f9000-0000-7000-8000-000000000002";
const SECOND_TURN: &str = "019f9000-0000-7000-8000-000000000003";
const CHILD: &str = "019f9000-0000-7000-8000-000000000004";
const CHILD_TURN: &str = "019f9000-0000-7000-8000-000000000005";

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

    fn write(&self, name: &str, records: &[Value]) -> PathBuf {
        let path = self.active.join(name);
        write_jsonl(&path, records);
        path
    }

    fn scan(&self) {
        let report = scan_once(&self.db, &self.roots()).unwrap();
        assert_eq!(report.files_failed, 0);
    }
}

fn write_jsonl(path: &Path, records: &[Value]) {
    let mut file = File::create(path).unwrap();
    for record in records {
        writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
    }
}

fn root_meta(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": ROOT,
            "session_id": ROOT,
            "cwd": "/tmp/lifecycle-authority-contract",
            "source": "vscode"
        }
    })
}

fn child_meta(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": CHILD,
            "session_id": ROOT,
            "cwd": "/tmp/lifecycle-authority-contract",
            "source": {
                "subagent": {
                    "thread_spawn": {
                        "parent_thread_id": ROOT,
                        "parent_rollout_id": ROOT,
                        "agent_path": "/root/lifecycle-child",
                        "agent_nickname": "Lovelace"
                    }
                }
            }
        }
    })
}

fn task_started(timestamp: &str, turn_id: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {"type": "task_started", "turn_id": turn_id}
    })
}

fn assistant_update(timestamp: &str, text: &str) -> Value {
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

fn task_complete(
    timestamp: &str,
    turn_id: Option<&str>,
    last_message: &str,
    duration_ms: i64,
    time_to_first_token_ms: i64,
) -> Value {
    let mut payload = json!({
        "type": "task_complete",
        "last_agent_message": last_message,
        "duration_ms": duration_ms,
        "time_to_first_token_ms": time_to_first_token_ms
    });
    if let Some(turn_id) = turn_id {
        payload["turn_id"] = Value::String(turn_id.into());
    }
    json!({"timestamp": timestamp, "type": "event_msg", "payload": payload})
}

fn terminal_event(timestamp: &str, kind: &str, turn_id: Option<&str>) -> Value {
    let mut payload = json!({"type": kind, "reason": "contract fixture", "num_turns": 1});
    if let Some(turn_id) = turn_id {
        payload["turn_id"] = Value::String(turn_id.into());
    }
    json!({"timestamp": timestamp, "type": "event_msg", "payload": payload})
}

fn subagent_activity(timestamp: &str, child_id: Option<&str>, path: &str, kind: &str) -> Value {
    let mut payload = json!({
        "type": "sub_agent_activity",
        "agent_path": path,
        "kind": kind
    });
    if let Some(child_id) = child_id {
        payload["agent_thread_id"] = Value::String(child_id.into());
    }
    json!({"timestamp": timestamp, "type": "event_msg", "payload": payload})
}

fn scalar(connection: &Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn task_started_implicitly_interrupts_only_the_open_predecessor_with_stable_identity() {
    let harness = Harness::new();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TABLE lifecycle_order_audit(
                 event_id TEXT PRIMARY KEY,
                 rollout_last_event_at_before_event TEXT NOT NULL
             );
             CREATE TRIGGER audit_lifecycle_event_before_owner_touch
             AFTER INSERT ON events
             WHEN NEW.kind='turn_started'
               OR (NEW.kind='state' AND NEW.label='Turn interrupted')
             BEGIN
                 INSERT INTO lifecycle_order_audit
                 SELECT NEW.id,last_event_at FROM rollouts WHERE id=NEW.rollout_id;
             END;",
        )
        .unwrap();
    harness.write(
        "root.jsonl",
        &[
            root_meta("2026-07-25T10:00:00Z"),
            task_started("2026-07-25T10:00:01Z", FIRST_TURN),
            assistant_update("2026-07-25T10:00:02Z", "The first task is still open."),
            task_started("2026-07-25T10:05:00Z", SECOND_TURN),
        ],
    );

    harness.scan();
    let connection = harness.db.connect().unwrap();
    let first: (String, Option<String>) = connection
        .query_row(
            "SELECT status,completed_at FROM turns WHERE id=?1",
            [FIRST_TURN],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        first,
        (
            "interrupted".into(),
            Some("2026-07-25T10:05:00.000000000Z".into())
        )
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT status FROM turns WHERE id=?1",
                [SECOND_TURN],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "running"
    );

    let implicit: (String, String, String, i64, String, String, String, i64) = connection
        .query_row(
            "SELECT id,turn_id,timestamp,source_line,kind,label,status,native
             FROM events WHERE id=?1",
            [format!("{ROOT}:4:implicit-interrupt")],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        implicit,
        (
            format!("{ROOT}:4:implicit-interrupt"),
            FIRST_TURN.into(),
            "2026-07-25T10:05:00.000000000Z".into(),
            4,
            "state".into(),
            "Turn interrupted".into(),
            "interrupted".into(),
            1,
        )
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT turn_id FROM events WHERE id=?1 AND kind='turn_started'",
                [format!("{ROOT}:4")],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        SECOND_TURN
    );

    let observed_order = connection
        .prepare(
            "SELECT event_id,rollout_last_event_at_before_event
             FROM lifecycle_order_audit ORDER BY rowid",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        observed_order,
        vec![
            (format!("{ROOT}:2"), "2026-07-25T10:00:00.000000000Z".into()),
            (
                format!("{ROOT}:4:implicit-interrupt"),
                "2026-07-25T10:00:02.000000000Z".into()
            ),
            (format!("{ROOT}:4"), "2026-07-25T10:00:02.000000000Z".into()),
        ]
    );
}

#[test]
fn new_task_does_not_rewrite_any_explicit_terminal_predecessor() {
    let cases = [
        ("task_complete", "completed"),
        ("turn_aborted", "interrupted"),
        ("thread_rolled_back", "rolled_back"),
    ];
    for (kind, expected_status) in cases {
        let harness = Harness::new();
        let terminal = match kind {
            "task_complete" => {
                task_complete("2026-07-25T10:00:02Z", Some(FIRST_TURN), "Done.", 100, 20)
            }
            _ => terminal_event("2026-07-25T10:00:02Z", kind, Some(FIRST_TURN)),
        };
        harness.write(
            &format!("{kind}.jsonl"),
            &[
                root_meta("2026-07-25T10:00:00Z"),
                task_started("2026-07-25T10:00:01Z", FIRST_TURN),
                terminal,
                task_started("2026-07-25T10:00:03Z", SECOND_TURN),
            ],
        );

        harness.scan();
        let connection = harness.db.connect().unwrap();
        let terminal_state: (String, Option<String>) = connection
            .query_row(
                "SELECT status,completed_at FROM turns WHERE id=?1",
                [FIRST_TURN],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(terminal_state.0, expected_status, "case {kind}");
        assert_eq!(
            terminal_state.1.as_deref(),
            Some("2026-07-25T10:00:02.000000000Z"),
            "case {kind}"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE id=?1",
                    [format!("{ROOT}:4:implicit-interrupt")],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0,
            "case {kind}"
        );
    }
}

#[test]
fn task_complete_targets_current_or_explicit_turn_and_projects_all_fields() {
    let harness = Harness::new();
    harness.write(
        "complete.jsonl",
        &[
            root_meta("2026-07-25T10:00:00Z"),
            task_started("2026-07-25T10:00:01Z", FIRST_TURN),
            task_complete(
                "2026-07-25T10:00:02Z",
                None,
                "Current turn complete.",
                1_234,
                123,
            ),
            task_started("2026-07-25T10:00:03Z", SECOND_TURN),
            task_complete(
                "2026-07-25T10:00:04Z",
                Some(SECOND_TURN),
                "Explicit turn complete.",
                5_678,
                456,
            ),
        ],
    );

    harness.scan();
    let connection = harness.db.connect().unwrap();
    let turns = connection
        .prepare(
            "SELECT id,status,completed_at,last_agent_message,duration_ms,time_to_first_token_ms
             FROM turns ORDER BY started_at",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        turns,
        vec![
            (
                FIRST_TURN.into(),
                "completed".into(),
                Some("2026-07-25T10:00:02.000000000Z".into()),
                Some("Current turn complete.".into()),
                Some(1_234),
                Some(123),
            ),
            (
                SECOND_TURN.into(),
                "completed".into(),
                Some("2026-07-25T10:00:04.000000000Z".into()),
                Some("Explicit turn complete.".into()),
                Some(5_678),
                Some(456),
            ),
        ]
    );
    let events = connection
        .prepare(
            "SELECT id,turn_id,body,status,duration_ms
             FROM events WHERE kind='turn_completed' ORDER BY source_line",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        events,
        vec![
            (
                format!("{ROOT}:3"),
                FIRST_TURN.into(),
                Some("Current turn complete.".into()),
                Some("completed".into()),
                Some(1_234),
            ),
            (
                format!("{ROOT}:5"),
                SECOND_TURN.into(),
                Some("Explicit turn complete.".into()),
                Some("completed".into()),
                Some(5_678),
            ),
        ]
    );
}

#[test]
fn abort_and_rollback_without_turn_id_target_the_current_turn() {
    for (kind, expected_status) in [
        ("turn_aborted", "interrupted"),
        ("thread_rolled_back", "rolled_back"),
    ] {
        let harness = Harness::new();
        harness.write(
            &format!("{kind}.jsonl"),
            &[
                root_meta("2026-07-25T10:00:00Z"),
                task_started("2026-07-25T10:00:01Z", FIRST_TURN),
                terminal_event("2026-07-25T10:00:02Z", kind, None),
            ],
        );

        harness.scan();
        let connection = harness.db.connect().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM turns WHERE id=?1",
                    [FIRST_TURN],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            expected_status,
            "turn status for {kind}"
        );
        assert_eq!(
            connection
                .query_row("SELECT status FROM agent_runs WHERE id=?1", [ROOT], |row| {
                    row.get::<_, String>(0)
                },)
                .unwrap(),
            expected_status,
            "agent status for {kind}"
        );
        let event: (String, String, String, String) = connection
            .query_row(
                "SELECT id,turn_id,label,status FROM events WHERE id=?1",
                [format!("{ROOT}:3")],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            event,
            (
                format!("{ROOT}:3"),
                FIRST_TURN.into(),
                kind.into(),
                expected_status.into(),
            ),
            "event for {kind}"
        );
    }
}

#[test]
fn synthetic_agent_uses_earliest_start_and_latest_equal_timestamp_observation() {
    let harness = Harness::new();
    harness.write(
        "synthetic.jsonl",
        &[
            root_meta("2026-07-25T10:00:00Z"),
            subagent_activity(
                "2026-07-25T10:00:01Z",
                Some(CHILD),
                "/root/first",
                "started",
            ),
            subagent_activity(
                "2026-07-25T10:00:02Z",
                Some(CHILD),
                "/root/completed",
                "completed",
            ),
            subagent_activity(
                "2026-07-25T10:00:02Z",
                Some(CHILD),
                "/root/latest-equal-time",
                "interacted",
            ),
            subagent_activity("2026-07-25T10:00:03Z", None, "/root/event-only", "started"),
        ],
    );

    harness.scan();
    let connection = harness.db.connect().unwrap();
    let child: (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
    ) = connection
        .query_row(
            "SELECT thread_id,parent_rollout_id,started_at,status,completed_at,agent_path
             FROM agent_runs WHERE id=?1",
            [CHILD],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        child,
        (
            ROOT.into(),
            ROOT.into(),
            "2026-07-25T10:00:01.000000000Z".into(),
            "running".into(),
            None,
            Some("/root/latest-equal-time".into()),
        )
    );
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM agent_runs"), 2);
    let event_only: (String, Option<String>, String, String, String) = connection
        .query_row(
            "SELECT id,turn_id,kind,body,status FROM events WHERE id=?1",
            [format!("{ROOT}:5")],
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
        event_only,
        (
            format!("{ROOT}:5"),
            None,
            "subagent".into(),
            "/root/event-only".into(),
            "started".into(),
        )
    );
}

fn promoted_state(
    parent_terminal_at: &str,
    child_started_at: &str,
    complete_child: bool,
) -> (String, Option<String>, String, Option<String>) {
    let harness = Harness::new();
    harness.write(
        "a-parent.jsonl",
        &[
            root_meta("2026-07-25T10:00:00Z"),
            subagent_activity(
                parent_terminal_at,
                Some(CHILD),
                "/root/parent-observed",
                "interrupted",
            ),
        ],
    );
    let mut child_records = vec![
        child_meta("2026-07-25T10:00:01Z"),
        task_started(child_started_at, CHILD_TURN),
    ];
    if complete_child {
        child_records.push(task_complete(
            "2026-07-25T10:00:03Z",
            Some(CHILD_TURN),
            "Child finished natively.",
            10,
            1,
        ));
    }
    harness.write("z-child.jsonl", &child_records);
    harness.scan();
    harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                status,completed_at,
                (SELECT status FROM turns WHERE id=?2),
                (SELECT completed_at FROM turns WHERE id=?2)
             FROM agent_runs WHERE id=?1 AND rollout_id=?1",
            [CHILD, CHILD_TURN],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
}

#[test]
fn promoted_agent_authority_resolves_equal_and_later_native_or_parent_evidence() {
    let equal_parent_closes = promoted_state("2026-07-25T10:00:02Z", "2026-07-25T10:00:02Z", false);
    assert_eq!(
        equal_parent_closes,
        (
            "interrupted".into(),
            Some("2026-07-25T10:00:02.000000000Z".into()),
            "interrupted".into(),
            Some("2026-07-25T10:00:02.000000000Z".into()),
        )
    );

    let later_native_reopens =
        promoted_state("2026-07-25T10:00:02Z", "2026-07-25T10:00:02.001Z", false);
    assert_eq!(
        later_native_reopens,
        ("running".into(), None, "running".into(), None)
    );

    let later_parent_closes =
        promoted_state("2026-07-25T10:00:02.001Z", "2026-07-25T10:00:02Z", false);
    assert_eq!(
        later_parent_closes,
        (
            "interrupted".into(),
            Some("2026-07-25T10:00:02.001000000Z".into()),
            "interrupted".into(),
            Some("2026-07-25T10:00:02.001000000Z".into()),
        )
    );

    let explicit_child_terminal_wins =
        promoted_state("2026-07-25T10:00:04Z", "2026-07-25T10:00:02Z", true);
    assert_eq!(
        explicit_child_terminal_wins,
        (
            "completed".into(),
            Some("2026-07-25T10:00:03.000000000Z".into()),
            "completed".into(),
            Some("2026-07-25T10:00:03.000000000Z".into()),
        )
    );
}

#[test]
fn lifecycle_projection_error_rolls_back_cursor_and_checkpoint_before_clean_retry() {
    let harness = Harness::new();
    let path = harness.write(
        "bad-agent.jsonl",
        &[
            root_meta("2026-07-25T10:00:00Z"),
            task_started("2026-07-25T10:00:01Z", FIRST_TURN),
            subagent_activity(
                "2026-07-25T10:00:02Z",
                Some(&"x".repeat(257)),
                "/root/too-large",
                "started",
            ),
        ],
    );

    let error = scan_once(&harness.db, &harness.roots()).unwrap_err();
    assert!(
        format!("{error:#}")
            .contains("subagent thread id exceeds the 256-character identifier limit"),
        "unexpected error: {error:#}"
    );
    let connection = harness.db.connect().unwrap();
    for table in [
        "threads",
        "rollouts",
        "agent_runs",
        "turns",
        "events",
        "source_files",
    ] {
        assert_eq!(
            scalar(&connection, &format!("SELECT COUNT(*) FROM {table}")),
            0,
            "{table} escaped the failed file transaction"
        );
    }
    drop(connection);

    write_jsonl(
        &path,
        &[
            root_meta("2026-07-25T10:00:00Z"),
            task_started("2026-07-25T10:00:01Z", FIRST_TURN),
            subagent_activity(
                "2026-07-25T10:00:02Z",
                Some(CHILD),
                "/root/retried",
                "started",
            ),
        ],
    );
    harness.scan();
    let connection = harness.db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM turns"), 1);
    assert_eq!(
        connection
            .query_row(
                "SELECT line_number FROM source_files WHERE rollout_id=?1",
                [ROOT],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        3
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT status FROM agent_runs WHERE id=?1",
                [CHILD],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "running"
    );
}
