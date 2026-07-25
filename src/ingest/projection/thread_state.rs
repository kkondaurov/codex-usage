use super::super::protocol::{
    CursorState, DecodedThreadStateRecord, GoalUpdate, ThreadStateIntent,
};
use super::{events, lifecycle};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};

/// Apply one typed thread-state record and publish its cursor transition only
/// after duplicate policy, event persistence, and owner activity all succeed.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedThreadStateRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    match &record.intent {
        ThreadStateIntent::Goal(update) => {
            if let Some(update) = update.as_deref()
                && goal_changed(tx, &candidate.thread_id, update)?
            {
                events::apply(
                    tx,
                    &candidate,
                    record.source_line,
                    &record.timestamp,
                    &update.event,
                )?;
            }
        }
        ThreadStateIntent::Compaction(event) => {
            if !has_nearby_compaction(tx, &candidate.thread_id, &record.timestamp)? {
                events::apply(tx, &candidate, record.source_line, &record.timestamp, event)?;
            }
        }
    }

    lifecycle::touch_owner(tx, &candidate, &record.timestamp)?;
    *state = candidate;
    Ok(())
}

fn goal_changed(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
    update: &GoalUpdate,
) -> Result<bool> {
    let previous = tx
        .sqlite
        .query_row(
            "SELECT body,status FROM events WHERE thread_id=?1 AND kind='goal'
             ORDER BY timestamp DESC,source_line DESC LIMIT 1",
            [thread_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?;
    Ok(previous.as_ref().is_none_or(|(body, status)| {
        body.as_deref() != update.comparison_body.as_deref()
            || status.as_deref() != update.comparison_status.as_deref()
    }))
}

fn has_nearby_compaction(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
    timestamp: &str,
) -> Result<bool> {
    let duplicate: i64 = tx.sqlite.query_row(
        "SELECT EXISTS(SELECT 1 FROM events WHERE thread_id=?1 AND kind='compaction'
         AND ABS((julianday(timestamp)-julianday(?2))*86400.0)<1.0)",
        params![thread_id, timestamp],
        |row| row.get(0),
    )?;
    Ok(duplicate != 0)
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{
        PROJECTED_EVENT_BODY_CHARS, ProjectedEvent, ThreadStateTransition,
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
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
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
                 CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 CREATE TABLE event_insert_observations(
                    thread_last_event_at TEXT NOT NULL,
                    rollout_last_event_at TEXT NOT NULL
                 );
                 CREATE TRIGGER observe_thread_state_event_before_insert
                 BEFORE INSERT ON events
                 BEGIN
                    INSERT INTO event_insert_observations(
                        thread_last_event_at,rollout_last_event_at
                    ) SELECT threads.last_event_at,rollouts.last_event_at
                      FROM threads JOIN rollouts
                      WHERE threads.id=NEW.thread_id AND rollouts.id=NEW.rollout_id;
                 END;
                 INSERT INTO threads(id,last_event_at)
                 VALUES('thread-1','2026-07-25T08:00:00.000000000Z');
                 INSERT INTO rollouts(id,last_event_at)
                 VALUES('rollout-1','2026-07-25T08:00:00.000000000Z');",
            )
            .unwrap();
        connection
    }

    fn event(kind: &str, body: Option<&str>, status: Option<&str>) -> ProjectedEvent {
        ProjectedEvent {
            kind: kind.into(),
            role: None,
            label: Some(if kind == "goal" {
                "Goal updated".into()
            } else {
                "Context compacted".into()
            }),
            body: body.map(str::to_owned),
            status: status.map(str::to_owned),
            tool_name: None,
            call_id: None,
            duration_ms: None,
            metadata: None,
        }
    }

    fn record(line: u64, timestamp: &str, intent: ThreadStateIntent) -> DecodedThreadStateRecord {
        DecodedThreadStateRecord {
            source_line: line,
            timestamp: timestamp.into(),
            transition: ThreadStateTransition {
                last_timestamp: timestamp.into(),
            },
            intent,
        }
    }

    fn goal(
        line: u64,
        timestamp: &str,
        comparison_body: Option<&str>,
        comparison_status: Option<&str>,
        stored_body: Option<&str>,
        stored_status: Option<&str>,
    ) -> DecodedThreadStateRecord {
        record(
            line,
            timestamp,
            ThreadStateIntent::Goal(Some(Box::new(GoalUpdate {
                comparison_body: comparison_body.map(str::to_owned),
                comparison_status: comparison_status.map(str::to_owned),
                event: event("goal", stored_body, stored_status),
            }))),
        )
    }

    fn apply_record(
        connection: &mut Connection,
        cursor: &mut CursorState,
        record: &DecodedThreadStateRecord,
    ) {
        let transaction = crate::ingest::projection::ProjectionConnection::new(connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, cursor, record).unwrap();
        transaction.commit().unwrap();
    }

    #[test]
    fn goal_dedupe_compares_previous_body_and_status_before_touching_and_publishing() {
        let mut connection = connection();
        let mut cursor = state();
        let active = goal(
            17,
            "2026-07-25T10:00:00.000000000Z",
            Some("Build the projection."),
            Some("active"),
            Some("Build the projection."),
            Some("active"),
        );
        apply_record(&mut connection, &mut cursor, &active);

        let heartbeat = goal(
            18,
            "2026-07-25T10:00:01.000000000Z",
            Some("Build the projection."),
            Some("active"),
            Some("Build the projection."),
            Some("active"),
        );
        apply_record(&mut connection, &mut cursor, &heartbeat);

        let completed = goal(
            19,
            "2026-07-25T10:00:02.000000000Z",
            Some("Build the projection."),
            Some("complete"),
            Some("Build the projection."),
            Some("complete"),
        );
        apply_record(&mut connection, &mut cursor, &completed);

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events WHERE kind='goal'", [], |row| {
                    row.get::<_, i64>(0)
                },)
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT GROUP_CONCAT(status,',') FROM events
                     WHERE kind='goal' ORDER BY timestamp,source_line",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "active,complete"
        );
        assert_eq!(
            cursor.last_timestamp.as_deref(),
            Some("2026-07-25T10:00:02.000000000Z")
        );
        assert_owner_timestamps(&connection, "2026-07-25T10:00:02.000000000Z");
        assert_eq!(
            connection
                .query_row(
                    "SELECT thread_last_event_at,rollout_last_event_at
                     FROM event_insert_observations ORDER BY rowid LIMIT 1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            (
                "2026-07-25T08:00:00.000000000Z".into(),
                "2026-07-25T08:00:00.000000000Z".into(),
            )
        );
    }

    #[test]
    fn goal_comparison_deliberately_uses_unbounded_pre_shape_values() {
        let mut connection = connection();
        let mut cursor = state();
        let stored = format!("{}…", "g".repeat(PROJECTED_EVENT_BODY_CHARS));
        let comparison = "g".repeat(PROJECTED_EVENT_BODY_CHARS + 200);

        for (line, timestamp) in [
            (20, "2026-07-25T10:01:00.000000000Z"),
            (21, "2026-07-25T10:01:01.000000000Z"),
        ] {
            let record = goal(
                line,
                timestamp,
                Some(&comparison),
                Some("active"),
                Some(&stored),
                Some("active"),
            );
            apply_record(&mut connection, &mut cursor, &record);
        }

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events WHERE kind='goal'", [], |row| {
                    row.get::<_, i64>(0)
                },)
                .unwrap(),
            2,
            "the legacy comparison occurs before event-body bounding"
        );
    }

    #[test]
    fn compaction_dedupe_is_strictly_sub_second_and_still_touches_every_record() {
        let mut connection = connection();
        let mut cursor = state();
        for (line, timestamp) in [
            (30, "2026-07-25T10:02:00.000000000Z"),
            (31, "2026-07-25T10:02:00.500000000Z"),
            (32, "2026-07-25T10:02:01.010000000Z"),
        ] {
            let record = record(
                line,
                timestamp,
                ThreadStateIntent::Compaction(Box::new(event(
                    "compaction",
                    Some("Conversation context was compacted."),
                    None,
                ))),
            );
            apply_record(&mut connection, &mut cursor, &record);
        }

        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='compaction'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
        assert_eq!(
            cursor.last_timestamp.as_deref(),
            Some("2026-07-25T10:02:01.010000000Z")
        );
        assert_owner_timestamps(&connection, "2026-07-25T10:02:01.010000000Z");
    }

    #[test]
    fn empty_goal_is_touch_only_and_projection_failure_does_not_publish_cursor() {
        let mut connection = connection();
        let mut cursor = state();
        let empty = record(
            40,
            "2026-07-25T10:03:00.000000000Z",
            ThreadStateIntent::Goal(None),
        );
        apply_record(&mut connection, &mut cursor, &empty);
        assert_eq!(
            cursor.last_timestamp.as_deref(),
            Some("2026-07-25T10:03:00.000000000Z")
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );

        connection.execute("DROP TABLE events", []).unwrap();
        let before = cursor.clone();
        let failing = goal(
            41,
            "2026-07-25T10:04:00.000000000Z",
            Some("new"),
            Some("active"),
            Some("new"),
            Some("active"),
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        assert!(apply(&transaction, &mut cursor, &failing).is_err());
        assert_eq!(cursor.last_timestamp, before.last_timestamp);
        assert_eq!(cursor.current_turn, before.current_turn);
    }

    fn assert_owner_timestamps(connection: &Connection, expected: &str) {
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_event_at FROM threads WHERE id='thread-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            expected
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_event_at FROM rollouts WHERE id='rollout-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            expected
        );
    }
}
