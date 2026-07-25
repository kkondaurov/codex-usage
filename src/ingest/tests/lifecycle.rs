#![cfg(test)]

use super::super::*;
use super::support::*;
use rusqlite::params;
use std::cell::RefCell;

thread_local! {
    static TRACED_PROMOTED_AGENT_LIFECYCLE_SQL: RefCell<Option<String>> =
        const { RefCell::new(None) };
}

fn capture_promoted_agent_lifecycle_sql(sql: &str) {
    if sql.contains("FROM events")
        && !sql.contains("FROM turns")
        && sql.contains("kind='turn_started'")
        && sql.contains("ORDER BY timestamp DESC")
    {
        TRACED_PROMOTED_AGENT_LIFECYCLE_SQL.with(|captured| {
            *captured.borrow_mut() = Some(sql.to_owned());
        });
    }
}

#[test]
fn parent_observations_preserve_promoted_agent_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let mut connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('parent','thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z'),
                    ('child','thread','2026-07-01T00:10:00Z','2026-07-01T00:20:00Z');
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,completed_at,status
                 ) VALUES(
                    'child','thread','child','parent','2026-07-01T00:10:00Z',
                    '2026-07-01T00:20:00Z','completed'
                 );",
        )
        .unwrap();

    let transaction = ProjectionConnection::new(&mut connection)
        .begin_metadata_refresh()
        .unwrap();
    apply_agent_observation(
        &transaction,
        "child",
        "thread",
        "parent",
        Some("/root/promoted"),
        "2026-07-01T00:30:00Z",
        ObservedAgentActivity::Interrupted,
    )
    .unwrap();
    transaction.commit().unwrap();

    let lifecycle: (String, Option<String>, Option<String>, String) = connection
        .query_row(
            "SELECT status,completed_at,agent_path,rollout_id
                 FROM agent_runs WHERE id='child'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(lifecycle.0, "completed");
    assert_eq!(lifecycle.1.as_deref(), Some("2026-07-01T00:20:00Z"));
    assert_eq!(lifecycle.2.as_deref(), Some("/root/promoted"));
    assert_eq!(lifecycle.3, "child");
}

#[test]
fn promoted_agent_lifecycle_lookup_uses_activity_owner_index() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let mut connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z');
                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,started_at,last_event_at
                 ) VALUES(
                    'child','thread','parent','2026-07-01T00:10:00Z',
                    '2026-07-01T00:20:00Z'
                 );
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,status
                 ) VALUES(
                    'child','thread','child','parent','2026-07-01T00:10:00Z','running'
                 );",
        )
        .unwrap();

    TRACED_PROMOTED_AGENT_LIFECYCLE_SQL.with(|captured| {
        *captured.borrow_mut() = None;
    });
    connection.trace(Some(capture_promoted_agent_lifecycle_sql));
    let transaction = ProjectionConnection::new(&mut connection)
        .begin_metadata_refresh()
        .unwrap();
    rematerialize_surviving_observation(&transaction, "child").unwrap();
    transaction.commit().unwrap();
    connection.trace(None);

    let sql = TRACED_PROMOTED_AGENT_LIFECYCLE_SQL
        .with(|captured| captured.borrow_mut().take())
        .expect("promoted-agent lifecycle query was not traced");
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let plan = connection
        .prepare(&explain)
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let plan_text = plan.join("\n");
    assert!(
        plan.iter().any(|detail| {
            detail.contains("SEARCH events USING INDEX idx_events_activity_owner")
        }),
        "promoted-agent lifecycle lookup did not use the activity-owner index:\n{plan_text}"
    );
    assert!(
        !plan.iter().any(|detail| detail.contains("SCAN events")),
        "promoted-agent lifecycle lookup full-scanned events:\n{plan_text}"
    );
}

#[test]
fn synthetic_agent_observations_map_lifecycle_states_consistently() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let mut connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z');",
        )
        .unwrap();
    let transaction = ProjectionConnection::new(&mut connection)
        .begin_metadata_refresh()
        .unwrap();
    apply_agent_observation(
        &transaction,
        "interrupted-child",
        "thread",
        "parent",
        None,
        "2026-07-01T00:10:00Z",
        ObservedAgentActivity::Running,
    )
    .unwrap();
    apply_agent_observation(
        &transaction,
        "interrupted-child",
        "thread",
        "parent",
        None,
        "2026-07-01T00:20:00Z",
        ObservedAgentActivity::Interrupted,
    )
    .unwrap();
    apply_agent_observation(
        &transaction,
        "completed-child",
        "thread",
        "parent",
        None,
        "2026-07-01T00:40:00Z",
        ObservedAgentActivity::Completed,
    )
    .unwrap();
    apply_agent_observation(
        &transaction,
        "rolled-back-child",
        "thread",
        "parent",
        None,
        "2026-07-01T00:50:00Z",
        ObservedAgentActivity::RolledBack,
    )
    .unwrap();
    apply_agent_observation(
        &transaction,
        "running-child",
        "thread",
        "parent",
        None,
        "2026-07-01T00:55:00Z",
        ObservedAgentActivity::Interrupted,
    )
    .unwrap();
    apply_agent_observation(
        &transaction,
        "running-child",
        "thread",
        "parent",
        None,
        "2026-07-01T01:00:00Z",
        ObservedAgentActivity::Running,
    )
    .unwrap();
    transaction.commit().unwrap();

    let states = connection
        .prepare(
            "SELECT id,status,completed_at FROM agent_runs
                 ORDER BY id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        states,
        vec![
            (
                "completed-child".into(),
                "completed".into(),
                Some("2026-07-01T00:40:00Z".into()),
            ),
            (
                "interrupted-child".into(),
                "interrupted".into(),
                Some("2026-07-01T00:20:00Z".into()),
            ),
            (
                "rolled-back-child".into(),
                "rolled_back".into(),
                Some("2026-07-01T00:50:00Z".into()),
            ),
            ("running-child".into(), "running".into(), None),
        ]
    );
}

#[test]
fn projected_durations_are_bounded_at_parse_and_schema_boundaries() {
    assert_eq!(
        duration_ms(Some(&serde_json::json!(MAX_STORED_DURATION_MS))),
        Some(MAX_STORED_DURATION_MS)
    );
    assert_eq!(
        duration_ms(Some(&serde_json::json!(MAX_STORED_DURATION_MS + 1))),
        None
    );
    assert_eq!(raw_duration_ms(Some(&serde_json::json!(-1))), None);

    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000012";
    let turn = "019f64ab-0000-7000-8000-000000000012";
    write_fixture(
        &sessions.join("duration.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:02Z",
                "type":"event_msg",
                "payload":{
                    "type":"task_complete","turn_id":turn,
                    "duration_ms":MAX_STORED_DURATION_MS + 1
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:03Z",
                "type":"event_msg",
                "payload":{
                    "type":"exec_command_end","call_id":"oversized-call",
                    "duration_ms":MAX_STORED_DURATION_MS + 1,"exit_code":0
                }
            }),
        ],
    );
    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();

    let connection = db.connect().unwrap();
    let stored: (Option<i64>, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT
                    (SELECT duration_ms FROM turns WHERE id=?1),
                    (SELECT duration_ms FROM events
                     WHERE rollout_id=?2 AND kind='tool_completed'),
                    (SELECT duration_ms FROM tool_calls
                     WHERE rollout_id=?2 AND call_id='oversized-call')",
            params![turn, owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(stored, (None, None, None));

    let oversized = MAX_STORED_DURATION_MS + 1;
    assert!(
        connection
            .execute(
                "INSERT INTO turns(
                        id,thread_id,rollout_id,started_at,status,duration_ms
                     ) VALUES('oversized-turn',?1,?1,?2,'completed',?3)",
                params![owner, "2026-07-15T09:00:04Z", oversized],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "INSERT INTO events(
                        id,thread_id,rollout_id,timestamp,source_line,kind,duration_ms
                     ) VALUES('oversized-event',?1,?1,?2,99,'tool_completed',?3)",
                params![owner, "2026-07-15T09:00:04Z", oversized],
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "INSERT INTO tool_calls(
                        id,call_id,thread_id,rollout_id,started_at,name,status,duration_ms
                     ) VALUES(
                        'oversized-tool','oversized-tool',?1,?1,?2,
                        'exec_command','completed',?3
                     )",
                params![owner, "2026-07-15T09:00:04Z", oversized],
            )
            .is_err()
    );
}
