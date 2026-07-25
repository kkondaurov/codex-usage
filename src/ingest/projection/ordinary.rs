use super::super::protocol::{CursorState, DecodedOrdinaryRecord, OrdinaryIntent, OrdinaryNoop};
use super::{events, lifecycle};
use anyhow::Result;
use rusqlite::params;

/// Apply one already-decoded ordinary record and publish its cursor transition
/// only after every projection write succeeds.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedOrdinaryRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    match &record.intent {
        OrdinaryIntent::TurnContext => apply_turn_context(tx, &candidate, &record.timestamp)?,
        OrdinaryIntent::ThreadSettingsApplied => apply_thread_settings(tx, &candidate)?,
        OrdinaryIntent::Event(event) => {
            events::apply(tx, &candidate, record.source_line, &record.timestamp, event)?
        }
        OrdinaryIntent::Noop(noop) => apply_noop(*noop),
    }

    // Ordinary records are admitted only after the native-record gate. Even
    // source shapes that intentionally project no detail still advance owner
    // activity, exactly as the legacy post-dispatch touch did.
    lifecycle::touch_owner(tx, &candidate, &record.timestamp)?;
    *state = candidate;
    Ok(())
}

fn apply_turn_context(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
) -> Result<()> {
    lifecycle::ensure_turn(tx, state, timestamp)?;
    tx.sqlite.execute(
        "UPDATE turns SET model=COALESCE(?1,model), effort=COALESCE(?2,effort)
         WHERE id=?3",
        params![
            state.current_model,
            state.current_effort,
            state.current_turn
        ],
    )?;
    Ok(())
}

fn apply_thread_settings(tx: &super::ProjectionTx<'_>, state: &CursorState) -> Result<()> {
    if state.current_turn.is_some() {
        tx.sqlite.execute(
            "UPDATE turns SET model=COALESCE(?1,model),effort=COALESCE(?2,effort)
             WHERE id=?3",
            params![
                state.current_model,
                state.current_effort,
                state.current_turn
            ],
        )?;
    }
    Ok(())
}

fn apply_noop(noop: OrdinaryNoop) {
    match noop {
        OrdinaryNoop::WorldState
        | OrdinaryNoop::InterAgentCommunicationMetadata
        | OrdinaryNoop::ViewImageToolCall
        | OrdinaryNoop::DynamicToolCallRequest
        | OrdinaryNoop::NonPlanItemCompleted
        | OrdinaryNoop::GhostSnapshot => {}
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{OrdinaryStateTransition, ProjectedEvent};
    use super::*;
    use rusqlite::Connection;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-old".into()),
            current_model: Some("gpt-old".into()),
            current_effort: Some("medium".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn transition(
        timestamp: &str,
        turn: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        turn_context_seen: bool,
    ) -> OrdinaryStateTransition {
        OrdinaryStateTransition {
            last_timestamp: timestamp.into(),
            current_turn: turn.map(str::to_owned),
            turn_context_seen,
            current_model: model.map(str::to_owned),
            current_effort: effort.map(str::to_owned),
        }
    }

    fn record(
        timestamp: &str,
        transition: OrdinaryStateTransition,
        intent: OrdinaryIntent,
    ) -> DecodedOrdinaryRecord {
        DecodedOrdinaryRecord {
            source_line: 17,
            timestamp: timestamp.into(),
            transition,
            intent,
        }
    }

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(
                    id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE rollouts(
                    id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    agent_run_id TEXT NOT NULL,started_at TEXT NOT NULL,status TEXT NOT NULL,
                    model TEXT,effort TEXT
                 );
                 CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 CREATE TABLE event_insert_observations(thread_last_event_at TEXT NOT NULL);
                 CREATE TRIGGER observe_event_before_insert
                 BEFORE INSERT ON events
                 BEGIN
                    INSERT INTO event_insert_observations(thread_last_event_at)
                    SELECT last_event_at FROM threads WHERE id=NEW.thread_id;
                 END;
                 INSERT INTO threads(id,last_event_at)
                 VALUES('thread-1','2026-07-25T08:00:00.000000000Z');
                 INSERT INTO rollouts(id,last_event_at)
                 VALUES('rollout-1','2026-07-25T08:00:00.000000000Z');",
            )
            .unwrap();
        connection
    }

    #[test]
    fn turn_context_ensures_and_updates_the_turn_then_touches_both_owners() {
        let mut connection = connection();
        let timestamp = "2026-07-25T10:00:00.000000000Z";
        let decoded = record(
            timestamp,
            transition(
                timestamp,
                Some("turn-new"),
                Some("gpt-new"),
                Some("high"),
                true,
            ),
            OrdinaryIntent::TurnContext,
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(cursor.current_turn.as_deref(), Some("turn-new"));
        assert_eq!(cursor.current_model.as_deref(), Some("gpt-new"));
        assert_eq!(cursor.current_effort.as_deref(), Some("high"));
        assert!(cursor.turn_context_seen);
        assert_eq!(cursor.last_timestamp.as_deref(), Some(timestamp));
        assert_eq!(cursor.cumulative.total_tokens, 0);
        assert_eq!(
            connection
                .query_row(
                    "SELECT thread_id,rollout_id,agent_run_id,started_at,status,model,effort
                     FROM turns WHERE id='turn-new'",
                    [],
                    |row| Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    )),
                )
                .unwrap(),
            (
                "thread-1".into(),
                "rollout-1".into(),
                "rollout-1".into(),
                timestamp.into(),
                "running".into(),
                Some("gpt-new".into()),
                Some("high".into()),
            )
        );
        assert_owner_timestamps(&connection, timestamp);
    }

    #[test]
    fn thread_settings_updates_only_an_existing_current_turn_and_touches_owners() {
        let mut connection = connection();
        connection
            .execute(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status,model,effort
                 ) VALUES('turn-old','thread-1','rollout-1','rollout-1',?1,'running','old','low')",
                ["2026-07-25T08:00:00.000000000Z"],
            )
            .unwrap();
        let timestamp = "2026-07-25T10:01:00.000000000Z";
        let decoded = record(
            timestamp,
            transition(
                timestamp,
                Some("turn-old"),
                Some("gpt-new"),
                Some("xhigh"),
                false,
            ),
            OrdinaryIntent::ThreadSettingsApplied,
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
                    "SELECT model,effort FROM turns WHERE id='turn-old'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("gpt-new".into(), "xhigh".into())
        );
        assert_owner_timestamps(&connection, timestamp);
    }

    #[test]
    fn event_is_inserted_before_the_owner_touch_and_uses_candidate_state() {
        let mut connection = connection();
        let timestamp = "2026-07-25T10:02:00.000000000Z";
        let decoded = record(
            timestamp,
            transition(
                timestamp,
                Some("turn-new"),
                Some("gpt-new"),
                Some("high"),
                true,
            ),
            OrdinaryIntent::Event(Box::new(ProjectedEvent {
                kind: "plan".into(),
                role: None,
                label: Some("Plan".into()),
                body: Some("Inspect, implement, verify.".into()),
                status: Some("completed".into()),
                tool_name: None,
                call_id: None,
                duration_ms: None,
                metadata: None,
            })),
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
                    "SELECT turn_id,model,effort,kind,label,body,status
                     FROM events WHERE id='rollout-1:17'",
                    [],
                    |row| Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    )),
                )
                .unwrap(),
            (
                Some("turn-new".into()),
                Some("gpt-new".into()),
                Some("high".into()),
                "plan".into(),
                Some("Plan".into()),
                Some("Inspect, implement, verify.".into()),
                Some("completed".into()),
            )
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT thread_last_event_at FROM event_insert_observations",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "2026-07-25T08:00:00.000000000Z"
        );
        assert_owner_timestamps(&connection, timestamp);
    }

    #[test]
    fn intentional_noop_still_touches_owners_but_inserts_no_event() {
        let mut connection = connection();
        let timestamp = "2026-07-25T10:03:00.000000000Z";
        let decoded = record(
            timestamp,
            transition(
                timestamp,
                Some("turn-old"),
                Some("gpt-old"),
                Some("medium"),
                false,
            ),
            OrdinaryIntent::Noop(OrdinaryNoop::WorldState),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_owner_timestamps(&connection, timestamp);
    }

    #[test]
    fn projection_failure_does_not_publish_the_candidate_cursor() {
        let mut connection = connection();
        connection.execute("DROP TABLE events", []).unwrap();
        let timestamp = "2026-07-25T10:04:00.000000000Z";
        let decoded = record(
            timestamp,
            transition(
                timestamp,
                Some("turn-new"),
                Some("gpt-new"),
                Some("high"),
                true,
            ),
            OrdinaryIntent::Event(Box::new(ProjectedEvent {
                kind: "plan".into(),
                role: None,
                label: Some("Plan".into()),
                body: None,
                status: Some("completed".into()),
                tool_name: None,
                call_id: None,
                duration_ms: None,
                metadata: None,
            })),
        );
        let mut cursor = state();
        let before = serde_json::to_string(&cursor).unwrap();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        assert!(apply(&transaction, &mut cursor, &decoded).is_err());
        assert_eq!(serde_json::to_string(&cursor).unwrap(), before);
    }

    fn assert_owner_timestamps(connection: &Connection, timestamp: &str) {
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_event_at FROM threads WHERE id='thread-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            timestamp
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_event_at FROM rollouts WHERE id='rollout-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            timestamp
        );
    }
}
