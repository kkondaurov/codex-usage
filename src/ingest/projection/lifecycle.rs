use super::super::protocol::{
    CursorState, DecodedLifecycleRecord, LifecycleIntent, TaskComplete, TaskStarted,
    TerminalLifecycle,
};
use super::events;
use anyhow::Result;
use rusqlite::params;

/// Apply one typed native lifecycle record and publish its cursor transition
/// only after every lifecycle row, event, and owner touch succeeds.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedLifecycleRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    match &record.intent {
        LifecycleIntent::TaskStarted(started) => {
            apply_task_started(tx, &candidate, record, started)?
        }
        LifecycleIntent::TaskComplete(complete) => {
            apply_task_complete(tx, &candidate, record, complete)?
        }
        LifecycleIntent::Terminal(terminal) => apply_terminal(tx, &candidate, record, terminal)?,
    }

    touch_owner(tx, &candidate, &record.timestamp)?;
    *state = candidate;
    Ok(())
}

fn apply_task_started(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    record: &DecodedLifecycleRecord,
    started: &TaskStarted,
) -> Result<()> {
    if let Some(previous_turn) = started.previous_turn.as_deref()
        && state.current_turn.as_deref() != Some(previous_turn)
        && turn_has_open_native_lifecycle(tx, previous_turn)
    {
        tx.sqlite.execute(
            "UPDATE turns
             SET completed_at=?1,status='interrupted'
             WHERE id=?2 AND status='running'",
            params![record.timestamp, previous_turn],
        )?;
        record_implicit_turn_interruption(
            tx,
            state,
            record.source_line,
            previous_turn,
            &record.timestamp,
        )?;
    }
    ensure_turn(tx, state, &record.timestamp)?;
    tx.sqlite.execute(
        "UPDATE agent_runs SET status='running',completed_at=NULL WHERE id=?1",
        [&state.owner_id],
    )?;
    events::apply(
        tx,
        state,
        record.source_line,
        &record.timestamp,
        &started.event,
    )?;
    Ok(())
}

fn apply_task_complete(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    record: &DecodedLifecycleRecord,
    complete: &TaskComplete,
) -> Result<()> {
    ensure_turn(tx, state, &record.timestamp)?;
    tx.sqlite.execute(
        "UPDATE turns SET completed_at=?1,status='completed',last_agent_message=?2,
            duration_ms=?3,time_to_first_token_ms=?4 WHERE id=?5",
        params![
            record.timestamp,
            complete.last_agent_message,
            complete.duration_ms,
            complete.time_to_first_token_ms,
            state.current_turn,
        ],
    )?;
    tx.sqlite.execute(
        "UPDATE agent_runs SET status='completed',completed_at=?1 WHERE id=?2",
        params![record.timestamp, state.owner_id],
    )?;
    events::apply(
        tx,
        state,
        record.source_line,
        &record.timestamp,
        &complete.event,
    )?;
    Ok(())
}

fn apply_terminal(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    record: &DecodedLifecycleRecord,
    terminal: &TerminalLifecycle,
) -> Result<()> {
    ensure_turn(tx, state, &record.timestamp)?;
    tx.sqlite.execute(
        "UPDATE turns SET completed_at=?1,status=?2 WHERE id=?3",
        params![record.timestamp, terminal.kind.status(), state.current_turn],
    )?;
    tx.sqlite.execute(
        "UPDATE agent_runs SET completed_at=?1,status=?2 WHERE id=?3",
        params![record.timestamp, terminal.kind.status(), state.owner_id],
    )?;
    events::apply(
        tx,
        state,
        record.source_line,
        &record.timestamp,
        &terminal.event,
    )?;
    Ok(())
}

fn record_implicit_turn_interruption(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    source_line: u64,
    turn_id: &str,
    timestamp: &str,
) -> Result<()> {
    // A new native task is durable evidence that the previous open task was
    // interrupted. Its stable identity lets incremental projection and a
    // clean replay converge on the same lifecycle evidence.
    tx.sqlite.execute(
        "INSERT OR IGNORE INTO events(
            id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
            kind,label,status,native
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,'state','Turn interrupted','interrupted',1)",
        params![
            format!("{}:{source_line}:implicit-interrupt", state.owner_id),
            state.thread_id,
            state.owner_id,
            turn_id,
            state.owner_id,
            timestamp,
            source_line as i64,
        ],
    )?;
    Ok(())
}

pub(super) fn turn_has_open_native_lifecycle(tx: &super::ProjectionTx<'_>, turn_id: &str) -> bool {
    tx.sqlite
        .query_row(
            "SELECT EXISTS(
             SELECT 1 FROM events
             WHERE turn_id=?1 AND kind='turn_started'
         ) AND NOT EXISTS(
             SELECT 1 FROM events
             WHERE turn_id=?1
               AND (
                   kind='turn_completed'
                   OR (kind='state' AND status IN ('interrupted','rolled_back'))
               )
         )",
            [turn_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        != 0
}

pub(super) fn complete_turn_from_final(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    content: &str,
) -> Result<()> {
    let Some(turn_id) = state.current_turn.as_deref() else {
        return Ok(());
    };
    if turn_has_open_native_lifecycle(tx, turn_id) {
        tx.sqlite.execute(
            "UPDATE turns SET last_agent_message=?1 WHERE id=?2",
            params![content, turn_id],
        )?;
        return Ok(());
    }
    tx.sqlite.execute(
        "UPDATE turns
         SET completed_at=?1,status='completed',last_agent_message=?2
         WHERE id=?3 AND status='running'",
        params![timestamp, content, turn_id],
    )?;
    Ok(())
}

pub(in crate::ingest) fn ensure_turn(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
) -> Result<()> {
    let Some(turn_id) = state.current_turn.as_deref() else {
        return Ok(());
    };
    tx.sqlite.execute(
        "INSERT INTO turns(
            id,thread_id,rollout_id,agent_run_id,started_at,status,model,effort
         ) VALUES(?1,?2,?3,?4,?5,'running',?6,?7)
         ON CONFLICT(id) DO UPDATE SET
            model=COALESCE(excluded.model,turns.model),
            effort=COALESCE(excluded.effort,turns.effort)",
        params![
            turn_id,
            state.thread_id,
            state.owner_id,
            state.owner_id,
            timestamp,
            state.current_model,
            state.current_effort,
        ],
    )?;
    Ok(())
}

pub(in crate::ingest) fn touch_owner(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET last_event_at=MAX(last_event_at,?1) WHERE id=?2",
        params![timestamp, state.thread_id],
    )?;
    tx.sqlite.execute(
        "UPDATE rollouts SET last_event_at=MAX(last_event_at,?1) WHERE id=?2",
        params![timestamp, state.owner_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{
        LifecycleStateTransition, ProjectedEvent, TerminalLifecycleKind,
    };
    use super::*;
    use rusqlite::Connection;

    fn setup() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE rollouts(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE agent_runs(
                    id TEXT PRIMARY KEY,status TEXT NOT NULL,completed_at TEXT
                 );
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    agent_run_id TEXT,started_at TEXT NOT NULL,completed_at TEXT,
                    status TEXT NOT NULL,model TEXT,effort TEXT,last_agent_message TEXT,
                    duration_ms INTEGER,time_to_first_token_ms INTEGER
                 );
                 CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 INSERT INTO threads VALUES('thread-1','2026-07-25T09:00:00.000000000Z');
                 INSERT INTO rollouts VALUES('rollout-1','2026-07-25T09:00:00.000000000Z');
                 INSERT INTO agent_runs VALUES('rollout-1','running',NULL);",
            )
            .unwrap();
        connection
    }

    fn state(turn: &str) -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some(turn.into()),
            turn_context_seen: true,
            current_model: Some("gpt-test".into()),
            current_effort: Some("high".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn event(kind: &str, label: &str, status: &str) -> ProjectedEvent {
        ProjectedEvent {
            kind: kind.into(),
            role: None,
            label: Some(label.into()),
            body: None,
            status: Some(status.into()),
            tool_name: None,
            call_id: None,
            duration_ms: None,
            metadata: None,
        }
    }

    fn record(
        line: u64,
        timestamp: &str,
        turn: &str,
        context_seen: bool,
        intent: LifecycleIntent,
    ) -> DecodedLifecycleRecord {
        DecodedLifecycleRecord {
            source_line: line,
            timestamp: timestamp.into(),
            transition: LifecycleStateTransition {
                last_timestamp: timestamp.into(),
                native_started: true,
                current_turn: Some(turn.into()),
                turn_context_seen: context_seen,
            },
            intent,
        }
    }

    #[test]
    fn task_start_interrupts_only_open_native_predecessor_before_starting_successor() {
        let mut connection = setup();
        connection
            .execute_batch(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status
                 ) VALUES(
                    'turn-1','thread-1','rollout-1','rollout-1',
                    '2026-07-25T09:00:01.000000000Z','running'
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,
                    source_line,kind,label,status,native
                 ) VALUES(
                    'rollout-1:1','thread-1','rollout-1','turn-1','rollout-1',
                    '2026-07-25T09:00:01.000000000Z',1,
                    'turn_started','Turn started','running',1
                 );",
            )
            .unwrap();
        let mut cursor = state("turn-1");
        let record = record(
            7,
            "2026-07-25T10:00:00.000000000Z",
            "turn-2",
            false,
            LifecycleIntent::TaskStarted(Box::new(TaskStarted {
                previous_turn: Some("turn-1".into()),
                event: event("turn_started", "Turn started", "running"),
            })),
        );

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &record).unwrap();
        transaction.commit().unwrap();

        assert_eq!(cursor.current_turn.as_deref(), Some("turn-2"));
        assert!(!cursor.turn_context_seen);
        assert_eq!(
            connection
                .query_row("SELECT status FROM turns WHERE id='turn-1'", [], |row| {
                    row.get::<_, String>(0)
                },)
                .unwrap(),
            "interrupted"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT turn_id FROM events WHERE id='rollout-1:7:implicit-interrupt'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "turn-1"
        );
        assert_eq!(
            connection
                .query_row("SELECT status FROM turns WHERE id='turn-2'", [], |row| {
                    row.get::<_, String>(0)
                },)
                .unwrap(),
            "running"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT turn_id FROM events WHERE id='rollout-1:7'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "turn-2"
        );
    }

    #[test]
    fn completion_and_terminal_projection_write_exact_turn_agent_and_event_state() {
        let mut connection = setup();
        let mut cursor = state("turn-1");
        let complete = record(
            2,
            "2026-07-25T10:00:02.000000000Z",
            "turn-1",
            true,
            LifecycleIntent::TaskComplete(Box::new(TaskComplete {
                last_agent_message: Some("done".into()),
                duration_ms: Some(1234),
                time_to_first_token_ms: Some(56),
                event: ProjectedEvent {
                    body: Some("done".into()),
                    duration_ms: Some(1234),
                    ..event("turn_completed", "Turn completed", "completed")
                },
            })),
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &complete).unwrap();
        transaction.commit().unwrap();

        let stored = connection
            .query_row(
                "SELECT status,last_agent_message,duration_ms,time_to_first_token_ms
                 FROM turns WHERE id='turn-1'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(stored, ("completed".into(), "done".into(), 1234, 56));
        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM agent_runs WHERE id='rollout-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );

        let terminal = record(
            3,
            "2026-07-25T10:00:03.000000000Z",
            "turn-1",
            true,
            LifecycleIntent::Terminal(Box::new(TerminalLifecycle {
                kind: TerminalLifecycleKind::RolledBack,
                event: event("state", "thread_rolled_back", "rolled_back"),
            })),
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &terminal).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT status FROM turns WHERE id='turn-1'", [], |row| {
                    row.get::<_, String>(0)
                },)
                .unwrap(),
            "rolled_back"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT label,status FROM events WHERE id='rollout-1:3'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("thread_rolled_back".into(), "rolled_back".into())
        );
    }

    #[test]
    fn projection_failure_does_not_publish_candidate_cursor() {
        let mut connection = setup();
        connection.execute("DROP TABLE agent_runs", []).unwrap();
        let mut cursor = state("turn-1");
        let before = cursor.clone();
        let record = record(
            4,
            "2026-07-25T10:00:04.000000000Z",
            "turn-2",
            false,
            LifecycleIntent::TaskStarted(Box::new(TaskStarted {
                previous_turn: Some("turn-1".into()),
                event: event("turn_started", "Turn started", "running"),
            })),
        );

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        assert!(apply(&transaction, &mut cursor, &record).is_err());
        transaction.rollback().unwrap();

        assert_eq!(cursor.current_turn, before.current_turn);
        assert_eq!(cursor.turn_context_seen, before.turn_context_seen);
        assert_eq!(cursor.last_timestamp, before.last_timestamp);
    }

    #[test]
    fn final_message_updates_but_does_not_close_an_open_native_turn() {
        let mut connection = setup();
        connection
            .execute_batch(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status
                 ) VALUES(
                    'turn-1','thread-1','rollout-1','rollout-1',
                    '2026-07-25T09:00:01.000000000Z','running'
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,
                    source_line,kind,label,status,native
                 ) VALUES(
                    'rollout-1:1','thread-1','rollout-1','turn-1','rollout-1',
                    '2026-07-25T09:00:01.000000000Z',1,
                    'turn_started','Turn started','running',1
                 );",
            )
            .unwrap();
        let cursor = state("turn-1");
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        complete_turn_from_final(
            &transaction,
            &cursor,
            "2026-07-25T10:00:00.000000000Z",
            "provisional final",
        )
        .unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status,last_agent_message FROM turns WHERE id='turn-1'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("running".into(), "provisional final".into())
        );
    }
}
