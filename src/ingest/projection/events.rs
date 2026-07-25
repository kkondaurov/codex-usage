use super::super::protocol::{
    CompactionMetadata, CursorState, MetadataScalar, ProjectedCallId, ProjectedEvent,
    ProjectedEventMetadata, UnknownMetadata,
};
use super::event_id;
use crate::redaction::serialize_redacted_json;
use anyhow::Result;
use rusqlite::params;
use serde_json::{Map, Value};

pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    source_line: u64,
    timestamp: &str,
    event: &ProjectedEvent,
) -> Result<()> {
    let call_id = event.call_id.as_ref().map(projected_call_id).transpose()?;
    let payload_json = event.metadata.as_ref().map(metadata_json).transpose()?;
    tx.sqlite.execute(
        "INSERT OR IGNORE INTO events(
            id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
            kind,role,label,body,status,tool_name,call_id,duration_ms,model,effort,payload_json,native
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,1)",
        params![
            event_id(state, source_line),
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            source_line as i64,
            event.kind,
            event.role,
            event.label,
            event.body,
            event.status,
            event.tool_name,
            call_id,
            event.duration_ms,
            state.current_model,
            state.current_effort,
            payload_json,
        ],
    )?;
    Ok(())
}

pub(super) fn projected_call_id(call_id: &ProjectedCallId) -> Result<String> {
    match call_id {
        ProjectedCallId::Source(call_id) => Ok(call_id.clone()),
        ProjectedCallId::Message {
            rollout_id,
            source_id,
        } => Ok(format!(
            "message:{}",
            serde_json::to_string(&[rollout_id, source_id])?
        )),
    }
}

fn metadata_json(metadata: &ProjectedEventMetadata) -> Result<String> {
    let value = match metadata {
        ProjectedEventMetadata::Compaction(metadata) => compaction_json(metadata),
        ProjectedEventMetadata::Subagent(metadata) => {
            let mut object = Map::new();
            object.insert(
                "agent_thread_id".into(),
                Value::String(metadata.agent_thread_id.clone()),
            );
            Value::Object(object)
        }
        ProjectedEventMetadata::Unknown(metadata) => unknown_json(metadata),
    };
    Ok(serialize_redacted_json(&value)?)
}

fn compaction_json(metadata: &CompactionMetadata) -> Value {
    let mut object = Map::new();
    if let Some(count) = metadata.replacement_history_count {
        object.insert("replacement_history_count".into(), Value::from(count));
    }
    insert_scalar(&mut object, "window_number", &metadata.window_number);
    insert_scalar(&mut object, "first_window_id", &metadata.first_window_id);
    insert_scalar(
        &mut object,
        "previous_window_id",
        &metadata.previous_window_id,
    );
    insert_scalar(&mut object, "window_id", &metadata.window_id);
    Value::Object(object)
}

fn unknown_json(metadata: &UnknownMetadata) -> Value {
    let mut object = Map::new();
    insert_scalar(&mut object, "type", &metadata.event_type);
    insert_scalar(&mut object, "schema_version", &metadata.schema_version);
    insert_scalar(&mut object, "version", &metadata.version);
    insert_scalar(&mut object, "id", &metadata.id);
    insert_scalar(&mut object, "call_id", &metadata.call_id);
    insert_scalar(&mut object, "status", &metadata.status);
    Value::Object(object)
}

fn insert_scalar(object: &mut Map<String, Value>, key: &str, scalar: &Option<MetadataScalar>) {
    let Some(scalar) = scalar else {
        return;
    };
    let value = match scalar {
        MetadataScalar::Boolean(value) => Value::Bool(*value),
        MetadataScalar::Number(value) => Value::Number(value.clone()),
        MetadataScalar::String(value) => Value::String(value.clone()),
    };
    object.insert(key.into(), value);
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::SubagentMetadata;
    use super::*;
    use rusqlite::Connection;

    #[derive(Debug, PartialEq, Eq)]
    struct StoredEvent {
        id: String,
        thread_id: String,
        rollout_id: String,
        turn_id: Option<String>,
        agent_run_id: Option<String>,
        timestamp: String,
        source_line: i64,
        kind: String,
        role: Option<String>,
        label: Option<String>,
        body: Option<String>,
        status: Option<String>,
        tool_name: Option<String>,
        call_id: Option<String>,
        duration_ms: Option<i64>,
        model: Option<String>,
        effort: Option<String>,
        payload_json: Option<String>,
        native: i64,
    }

    #[test]
    fn typed_event_projection_writes_exact_row_and_activity_identity() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 CREATE TABLE activity_event_index(
                    event_id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,turn_key TEXT NOT NULL,
                    timestamp TEXT NOT NULL,source_line INTEGER NOT NULL,
                    canonical_key TEXT NOT NULL,
                    UNIQUE(thread_id,turn_key,canonical_key)
                 );
                 CREATE TRIGGER project_activity_event_after_insert
                 AFTER INSERT ON events
                 WHEN NEW.kind NOT IN ('turn_started','system','tool_output','tool_completed')
                 BEGIN
                    INSERT INTO activity_event_index(
                        event_id,thread_id,turn_key,timestamp,source_line,canonical_key
                    ) VALUES(
                        NEW.id,NEW.thread_id,COALESCE(NEW.turn_id,''),NEW.timestamp,
                        NEW.source_line,'event:' || NEW.id
                    );
                 END;",
            )
            .unwrap();
        let state = CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-1".into()),
            current_model: Some("gpt-test".into()),
            current_effort: Some("high".into()),
            ..CursorState::default()
        };
        let event = ProjectedEvent {
            kind: "subagent".into(),
            role: None,
            label: Some("spawn".into()),
            body: Some("/root/child".into()),
            status: Some("started".into()),
            tool_name: None,
            call_id: None,
            duration_ms: Some(17),
            metadata: Some(ProjectedEventMetadata::Subagent(SubagentMetadata {
                agent_thread_id: "child-thread".into(),
            })),
        };

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(
            &transaction,
            &state,
            23,
            "2026-07-25T10:00:00.000000000Z",
            &event,
        )
        .unwrap();
        transaction.commit().unwrap();

        let stored = connection
            .query_row("SELECT * FROM events", [], |row| {
                Ok(StoredEvent {
                    id: row.get(0)?,
                    thread_id: row.get(1)?,
                    rollout_id: row.get(2)?,
                    turn_id: row.get(3)?,
                    agent_run_id: row.get(4)?,
                    timestamp: row.get(5)?,
                    source_line: row.get(6)?,
                    kind: row.get(7)?,
                    role: row.get(8)?,
                    label: row.get(9)?,
                    body: row.get(10)?,
                    status: row.get(11)?,
                    tool_name: row.get(12)?,
                    call_id: row.get(13)?,
                    duration_ms: row.get(14)?,
                    model: row.get(15)?,
                    effort: row.get(16)?,
                    payload_json: row.get(17)?,
                    native: row.get(18)?,
                })
            })
            .unwrap();
        assert_eq!(
            stored,
            StoredEvent {
                id: "rollout-1:23".into(),
                thread_id: "thread-1".into(),
                rollout_id: "rollout-1".into(),
                turn_id: Some("turn-1".into()),
                agent_run_id: Some("rollout-1".into()),
                timestamp: "2026-07-25T10:00:00.000000000Z".into(),
                source_line: 23,
                kind: "subagent".into(),
                role: None,
                label: Some("spawn".into()),
                body: Some("/root/child".into()),
                status: Some("started".into()),
                tool_name: None,
                call_id: None,
                duration_ms: Some(17),
                model: Some("gpt-test".into()),
                effort: Some("high".into()),
                payload_json: Some(r#"{"agent_thread_id":"child-thread"}"#.into()),
                native: 1,
            }
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT event_id,thread_id,turn_key,timestamp,source_line,canonical_key
                     FROM activity_event_index",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .unwrap(),
            (
                "rollout-1:23".into(),
                "thread-1".into(),
                "turn-1".into(),
                "2026-07-25T10:00:00.000000000Z".into(),
                23,
                "event:rollout-1:23".into(),
            )
        );
    }

    #[test]
    fn typed_metadata_serialization_matches_the_legacy_compact_json_shapes() {
        let metadata = ProjectedEventMetadata::Compaction(CompactionMetadata {
            replacement_history_count: Some(4),
            window_number: Some(MetadataScalar::Number(9.into())),
            first_window_id: Some(MetadataScalar::String("first".into())),
            previous_window_id: None,
            window_id: Some(MetadataScalar::Boolean(true)),
        });
        assert_eq!(
            metadata_json(&metadata).unwrap(),
            r#"{"first_window_id":"first","replacement_history_count":4,"window_id":true,"window_number":9}"#
        );
    }
}
