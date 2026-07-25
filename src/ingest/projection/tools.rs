use super::super::protocol::{
    CursorState, DecodedToolRecord, ToolComplete, ToolCompletion, ToolEnrich, ToolIntent,
    ToolStart, ToolTerminal,
};
use super::{events, lifecycle};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};

/// Apply one typed tool record and publish its cursor transition only after
/// the complete row/event/owner projection succeeds.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedToolRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    if record.ensure_turn {
        lifecycle::ensure_turn(tx, &candidate, &record.timestamp)?;
    }

    let mut event = record.event.clone();
    match &record.intent {
        ToolIntent::Start(start) => apply_start(tx, &candidate, &record.timestamp, start)?,
        ToolIntent::Complete(complete) => {
            apply_completion(tx, &candidate, &record.timestamp, complete)?
        }
        ToolIntent::Enrich(enrich) => apply_enrichment(tx, &candidate, &record.timestamp, enrich)?,
        ToolIntent::Terminal(terminal) => {
            let completion_is_call = apply_terminal(tx, &candidate, &record.timestamp, terminal)?;
            if completion_is_call && let Some(event) = event.as_mut() {
                event.kind = "tool_call".into();
            }
        }
        ToolIntent::Noop => {}
    }

    if let Some(event) = event.as_ref() {
        events::apply(tx, &candidate, record.source_line, &record.timestamp, event)?;
    }
    lifecycle::touch_owner(tx, &candidate, &record.timestamp)?;
    *state = candidate;
    Ok(())
}

fn apply_start(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    start: &ToolStart,
) -> Result<()> {
    upsert_start(tx, state, timestamp, start)?;
    if let Some(completion) = start.completion.as_ref() {
        complete(tx, state, timestamp, &start.call_id, completion)?;
    }
    Ok(())
}

fn apply_completion(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    complete_intent: &ToolComplete,
) -> Result<()> {
    complete(
        tx,
        state,
        timestamp,
        &complete_intent.call_id,
        &complete_intent.completion,
    )
}

fn apply_enrichment(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    enrich: &ToolEnrich,
) -> Result<()> {
    let completion_status = enrich
        .completion
        .status
        .expect("enrichment decoders always supply a terminal status")
        .as_str();
    tx.sqlite.execute(
        "INSERT INTO tool_calls(
            id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
            completed_at,name,status,duration_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8,?9,?10)
         ON CONFLICT(rollout_id,call_id) DO UPDATE SET
            completed_at=COALESCE(tool_calls.completed_at,excluded.completed_at),
            name=CASE WHEN tool_calls.name='unknown' THEN excluded.name ELSE tool_calls.name END,
            status=CASE
                WHEN tool_calls.status IN ('failed','cancelled','canceled') THEN tool_calls.status
                WHEN excluded.status IN ('failed','cancelled','canceled') THEN excluded.status
                WHEN tool_calls.status='completed' THEN tool_calls.status
                WHEN excluded.status='completed' THEN excluded.status
                ELSE tool_calls.status END,
            duration_ms=COALESCE(excluded.duration_ms,tool_calls.duration_ms)",
        params![
            format!("{}:{}", state.owner_id, enrich.call_id),
            enrich.call_id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            enrich.name,
            completion_status,
            enrich.completion.duration_ms,
        ],
    )?;
    Ok(())
}

/// Apply a completion envelope with the current exact-ID, then latest-open
/// same-rollout/name matching policy. Returns whether this envelope is itself
/// the durable call (rather than a completion of an existing row).
fn apply_terminal(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    terminal: &ToolTerminal,
) -> Result<bool> {
    let exact_exists = if let Some(call_id) = terminal.explicit_call_id.as_deref() {
        tx.sqlite.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM tool_calls WHERE rollout_id=?1 AND call_id=?2
             )",
            params![state.owner_id, call_id],
            |row| row.get::<_, i64>(0),
        )? != 0
    } else {
        false
    };

    let mut resolved_call_id = terminal.explicit_call_id.clone();
    let mut matched_existing = exact_exists;
    if !exact_exists
        && terminal.fallback_name_matches_projected_form
        && let Some(name) = terminal.name.as_deref()
        && let Some(existing) = tx
            .sqlite
            .query_row(
                "SELECT call_id FROM tool_calls WHERE rollout_id=?1 AND name=?2
                 AND completed_at IS NULL ORDER BY started_at DESC LIMIT 1",
                params![state.owner_id, name],
                |row| row.get::<_, String>(0),
            )
            .optional()?
    {
        resolved_call_id = Some(existing);
        matched_existing = true;
    }

    if let Some(call_id) = resolved_call_id.as_deref() {
        if let Some(name) = terminal.name.as_deref() {
            upsert_start(
                tx,
                state,
                timestamp,
                &ToolStart {
                    call_id: call_id.to_owned(),
                    name: name.to_owned(),
                    namespace: terminal.namespace.clone(),
                    status: terminal.start_status.clone(),
                    completion: None,
                },
            )?;
        }
        complete(tx, state, timestamp, call_id, &terminal.completion)?;
    }

    Ok(!matched_existing && resolved_call_id.is_some() && terminal.name.is_some())
}

fn upsert_start(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    start: &ToolStart,
) -> Result<()> {
    tx.sqlite.execute(
        "INSERT INTO tool_calls(
            id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
            namespace,name,status
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
         ON CONFLICT(rollout_id,call_id) DO UPDATE SET
            namespace=COALESCE(excluded.namespace,tool_calls.namespace),
            name=excluded.name,
            turn_id=COALESCE(tool_calls.turn_id,excluded.turn_id),
            status=CASE
                WHEN tool_calls.status IN ('failed','cancelled','canceled') THEN tool_calls.status
                WHEN excluded.status IN ('failed','cancelled','canceled') THEN excluded.status
                WHEN tool_calls.status='completed' THEN tool_calls.status
                WHEN excluded.status='completed' THEN excluded.status
                ELSE tool_calls.status END",
        params![
            format!("{}:{}", state.owner_id, start.call_id),
            start.call_id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            start.namespace,
            start.name,
            start.status,
        ],
    )?;
    Ok(())
}

fn complete(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    call_id: &str,
    completion: &ToolCompletion,
) -> Result<()> {
    let status_is_authoritative = completion.status.is_some();
    let completion_status = completion
        .status
        .map(|status| status.as_str())
        .unwrap_or("completed");
    let tool_name = completion.name_hint.as_deref().unwrap_or("unknown");
    tx.sqlite.execute(
        "INSERT INTO tool_calls(
            id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
            completed_at,name,status,duration_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8,?9,?10)
         ON CONFLICT(rollout_id,call_id) DO UPDATE SET
            completed_at=CASE WHEN ?11 THEN excluded.completed_at
                              ELSE COALESCE(tool_calls.completed_at,excluded.completed_at) END,
            name=CASE WHEN tool_calls.name='unknown' THEN excluded.name ELSE tool_calls.name END,
            status=CASE
                WHEN tool_calls.status IN ('failed','cancelled','canceled') THEN tool_calls.status
                WHEN excluded.status IN ('failed','cancelled','canceled') THEN excluded.status
                WHEN tool_calls.status='completed' THEN tool_calls.status
                WHEN excluded.status='completed' THEN excluded.status
                ELSE tool_calls.status END,
            duration_ms=CASE WHEN ?11 AND excluded.duration_ms IS NOT NULL
                             THEN excluded.duration_ms
                             ELSE COALESCE(tool_calls.duration_ms,excluded.duration_ms) END",
        params![
            format!("{}:{call_id}", state.owner_id),
            call_id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            tool_name,
            completion_status,
            completion.duration_ms,
            status_is_authoritative,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{
        ProjectedCallId, ProjectedEvent, ToolStateTransition, ToolTerminalStatus,
    };
    use super::*;
    use rusqlite::Connection;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-1".into()),
            current_model: Some("gpt-test".into()),
            current_effort: Some("high".into()),
            last_timestamp: Some("2026-07-25T08:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        create_projection_schema(&connection, true);
        connection
    }

    fn create_projection_schema(connection: &Connection, include_rollouts: bool) {
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    agent_run_id TEXT NOT NULL,started_at TEXT NOT NULL,status TEXT NOT NULL,
                    model TEXT,effort TEXT
                 );
                 CREATE TABLE tool_calls(
                    id TEXT PRIMARY KEY,call_id TEXT NOT NULL,thread_id TEXT NOT NULL,
                    rollout_id TEXT NOT NULL,turn_id TEXT,agent_run_id TEXT,
                    started_at TEXT NOT NULL,completed_at TEXT,namespace TEXT,
                    name TEXT NOT NULL,status TEXT NOT NULL,duration_ms INTEGER,
                    UNIQUE(rollout_id,call_id)
                 );
                 CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 INSERT INTO threads(id,last_event_at)
                 VALUES('thread-1','2026-07-25T08:00:00.000000000Z');",
            )
            .unwrap();
        if include_rollouts {
            connection
                .execute_batch(
                    "CREATE TABLE rollouts(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                     INSERT INTO rollouts(id,last_event_at)
                     VALUES('rollout-1','2026-07-25T08:00:00.000000000Z');",
                )
                .unwrap();
        }
    }

    fn record(line: u64, timestamp: &str, intent: ToolIntent) -> DecodedToolRecord {
        DecodedToolRecord {
            source_line: line,
            timestamp: timestamp.into(),
            transition: ToolStateTransition {
                last_timestamp: timestamp.into(),
                current_turn: Some("turn-1".into()),
            },
            ensure_turn: false,
            intent,
            event: None,
        }
    }

    fn terminal(
        line: u64,
        timestamp: &str,
        explicit_call_id: Option<&str>,
        name: Option<&str>,
        event_call_id: Option<&str>,
    ) -> DecodedToolRecord {
        let mut record = record(
            line,
            timestamp,
            ToolIntent::Terminal(ToolTerminal {
                explicit_call_id: explicit_call_id.map(str::to_owned),
                name: name.map(str::to_owned),
                namespace: Some("server".into()),
                fallback_name_matches_projected_form: true,
                start_status: "running".into(),
                completion: ToolCompletion {
                    status: Some(ToolTerminalStatus::Completed),
                    duration_ms: Some(17),
                    name_hint: name.map(str::to_owned),
                },
            }),
        );
        record.event = Some(ProjectedEvent {
            kind: "tool_completed".into(),
            role: None,
            label: name.map(str::to_owned),
            body: None,
            status: Some("completed".into()),
            tool_name: name.map(str::to_owned),
            call_id: event_call_id.map(|value| ProjectedCallId::Source(value.into())),
            duration_ms: Some(17),
            metadata: None,
        });
        record
    }

    fn insert_open(connection: &Connection, call_id: &str, name: &str, started_at: &str) {
        connection
            .execute(
                "INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
                    name,status
                 ) VALUES(?1,?2,'thread-1','rollout-1','turn-1','rollout-1',?3,?4,'running')",
                params![format!("rollout-1:{call_id}"), call_id, started_at, name],
            )
            .unwrap();
    }

    #[test]
    fn explicit_identity_beats_a_newer_open_same_name_row() {
        let mut connection = connection();
        insert_open(&connection, "exact", "apply_patch", "2026-07-25T09:00:00Z");
        insert_open(&connection, "newer", "apply_patch", "2026-07-25T09:30:00Z");
        let decoded = terminal(
            11,
            "2026-07-25T10:00:00.000000000Z",
            Some("exact"),
            Some("apply_patch"),
            Some("exact"),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at FROM tool_calls WHERE call_id='exact'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .unwrap(),
            (
                "completed".into(),
                Some("2026-07-25T10:00:00.000000000Z".into())
            )
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at FROM tool_calls WHERE call_id='newer'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .unwrap(),
            ("running".into(), None)
        );
    }

    #[test]
    fn missing_exact_row_falls_back_but_event_keeps_source_identity() {
        let mut connection = connection();
        insert_open(
            &connection,
            "resolved-open",
            "apply_patch",
            "2026-07-25T09:30:00Z",
        );
        let decoded = terminal(
            12,
            "2026-07-25T10:01:00.000000000Z",
            Some("source-missing"),
            Some("apply_patch"),
            Some("source-missing"),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM tool_calls WHERE call_id='resolved-open'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM tool_calls", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT kind,call_id FROM events WHERE id='rollout-1:12'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("tool_completed".into(), "source-missing".into())
        );
    }

    #[test]
    fn absent_source_identity_falls_back_without_borrowing_the_row_id_for_activity() {
        let mut connection = connection();
        insert_open(
            &connection,
            "latest-open",
            "web_search_call",
            "2026-07-25T09:30:00Z",
        );
        let decoded = terminal(
            19,
            "2026-07-25T10:01:30.000000000Z",
            None,
            Some("web_search_call"),
            None,
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM tool_calls WHERE call_id='latest-open'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT call_id FROM events WHERE id='rollout-1:19'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn redaction_changed_name_skips_latest_open_fallback() {
        let mut connection = connection();
        insert_open(
            &connection,
            "existing-open",
            "run [embedded attachment]",
            "2026-07-25T09:30:00Z",
        );
        let mut decoded = terminal(
            20,
            "2026-07-25T10:01:30.000000000Z",
            Some("source-missing"),
            Some("run [embedded attachment]"),
            Some("source-missing"),
        );
        let ToolIntent::Terminal(terminal) = &mut decoded.intent else {
            panic!("expected terminal");
        };
        terminal.fallback_name_matches_projected_form = false;

        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM tool_calls WHERE call_id='existing-open'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "running"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM tool_calls WHERE call_id='source-missing'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT kind FROM events WHERE id='rollout-1:20'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "tool_call"
        );
    }

    #[test]
    fn latest_open_fallback_crosses_turns_but_not_rollouts() {
        let mut connection = connection();
        connection
            .execute(
                "INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
                    name,status
                 ) VALUES(
                    'rollout-1:previous-turn','previous-turn','thread-1','rollout-1',
                    'turn-previous','rollout-1','2026-07-25T09:30:00Z',
                    'apply_patch','running'
                 )",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
                    name,status
                 ) VALUES(
                    'rollout-2:newer-other-rollout','newer-other-rollout','thread-1',
                    'rollout-2','turn-1','rollout-2','2026-07-25T09:45:00Z',
                    'apply_patch','running'
                 )",
                [],
            )
            .unwrap();
        let decoded = terminal(
            21,
            "2026-07-25T10:02:00.000000000Z",
            None,
            Some("apply_patch"),
            None,
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM tool_calls WHERE call_id='previous-turn'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM tool_calls WHERE call_id='newer-other-rollout'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "running"
        );
    }

    #[test]
    fn completion_before_start_creates_a_call_event_and_later_start_cannot_regress_it() {
        let mut connection = connection();
        let completion = terminal(
            13,
            "2026-07-25T10:02:00.000000000Z",
            Some("completion-only"),
            Some("apply_patch"),
            Some("completion-only"),
        );
        let later_start = record(
            14,
            "2026-07-25T10:03:00.000000000Z",
            ToolIntent::Start(ToolStart {
                call_id: "completion-only".into(),
                name: "apply_patch".into(),
                namespace: None,
                status: "running".into(),
                completion: None,
            }),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &completion).unwrap();
        apply(&transaction, &mut cursor, &later_start).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at,duration_ms FROM tool_calls
                     WHERE call_id='completion-only'",
                    [],
                    |row| Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    )),
                )
                .unwrap(),
            (
                "completed".into(),
                Some("2026-07-25T10:02:00.000000000Z".into()),
                Some(17),
            )
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT kind FROM events WHERE id='rollout-1:13'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "tool_call"
        );
    }

    #[test]
    fn failed_terminal_state_survives_weaker_completion_and_later_start() {
        let mut connection = connection();
        let failed = record(
            15,
            "2026-07-25T10:04:00.000000000Z",
            ToolIntent::Enrich(ToolEnrich {
                call_id: "failed-call".into(),
                name: "exec_command".into(),
                completion: ToolCompletion {
                    status: Some(ToolTerminalStatus::Failed),
                    duration_ms: Some(8),
                    name_hint: Some("exec_command".into()),
                },
            }),
        );
        let weak_completion = record(
            16,
            "2026-07-25T10:05:00.000000000Z",
            ToolIntent::Complete(ToolComplete {
                call_id: "failed-call".into(),
                completion: ToolCompletion {
                    status: None,
                    duration_ms: None,
                    name_hint: None,
                },
            }),
        );
        let later_start = record(
            17,
            "2026-07-25T10:06:00.000000000Z",
            ToolIntent::Start(ToolStart {
                call_id: "failed-call".into(),
                name: "exec_command".into(),
                namespace: None,
                status: "running".into(),
                completion: None,
            }),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &failed).unwrap();
        apply(&transaction, &mut cursor, &weak_completion).unwrap();
        apply(&transaction, &mut cursor, &later_start).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at,duration_ms FROM tool_calls
                     WHERE call_id='failed-call'",
                    [],
                    |row| Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    )),
                )
                .unwrap(),
            ("failed".into(), "2026-07-25T10:04:00.000000000Z".into(), 8,)
        );
    }

    #[test]
    fn cursor_publication_waits_for_every_write_and_transaction_rollback_removes_partial_rows() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_projection_schema(&connection, false);
        let decoded = record(
            18,
            "2026-07-25T10:07:00.000000000Z",
            ToolIntent::Start(ToolStart {
                call_id: "rollback-call".into(),
                name: "exec_command".into(),
                namespace: None,
                status: "running".into(),
                completion: None,
            }),
        );
        let mut cursor = state();
        let original = cursor.clone();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        assert!(apply(&transaction, &mut cursor, &decoded).is_err());
        assert_eq!(
            serde_json::to_value(&cursor).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        transaction.rollback().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM tool_calls", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
