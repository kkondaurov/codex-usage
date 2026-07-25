use super::super::protocol::{
    ConversationIntent, ConversationNoop, CursorState, DecodedConversationRecord, MessageActivity,
    MessageIntent, MessageRole, ProjectedEvent, message_event,
};
use super::{event_id, events, lifecycle, metadata};
use anyhow::Result;
use rusqlite::params;

/// Apply one typed conversation record and publish its cursor transition only
/// after every projection query and write succeeds.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedConversationRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    match &record.intent {
        ConversationIntent::Message(message) => apply_message(tx, &mut candidate, record, message)?,
        ConversationIntent::CanonicalReasoning(reasoning) => apply_canonical_reasoning(
            tx,
            &candidate,
            record,
            &reasoning.event,
            reasoning.body_matches_projected_form,
        )?,
        ConversationIntent::SubagentMessage(event) => {
            lifecycle::ensure_turn(tx, &candidate, &record.timestamp)?;
            events::apply(tx, &candidate, record.source_line, &record.timestamp, event)?;
        }
        ConversationIntent::LegacyReasoning(event) => {
            apply_legacy_reasoning(tx, &candidate, record, event)?
        }
        ConversationIntent::LegacyAssistantUpdate(event) => {
            apply_legacy_assistant_update(tx, &candidate, record, event)?
        }
        ConversationIntent::Noop(noop) => apply_noop(*noop),
    }

    // Conversation rows and Activity events precede the owner activity touch,
    // matching the legacy post-dispatch order.
    lifecycle::touch_owner(tx, &candidate, &record.timestamp)?;
    *state = candidate;
    Ok(())
}

fn apply_noop(noop: ConversationNoop) {
    match noop {
        ConversationNoop::EmptyOrUnsupportedMessage
        | ConversationNoop::TurnAbortEnvelope
        | ConversationNoop::TransportContextEnvelope => {}
    }
}

fn apply_message(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedConversationRecord,
    message: &MessageIntent,
) -> Result<()> {
    if message.role == MessageRole::User
        && !state.turn_context_seen
        && !message.allow_implicit_turn
        && !state
            .current_turn
            .as_deref()
            .is_some_and(|turn_id| lifecycle::turn_has_open_native_lifecycle(tx, turn_id))
    {
        return Ok(());
    }

    if message.role == MessageRole::User && !message.has_explicit_turn {
        let current_accepts_feedback = state
            .current_turn
            .as_deref()
            .is_some_and(|turn_id| turn_accepts_metadata_free_feedback(tx, turn_id));
        if current_accepts_feedback {
            if let Some(turn_id) = state.current_turn.as_deref() {
                reopen_provisionally_completed_turn(tx, turn_id)?;
            }
        } else {
            state.current_turn = Some(format!(
                "{}:legacy-turn:{}",
                state.owner_id, record.source_line
            ));
            lifecycle::ensure_turn(tx, state, &record.timestamp)?;
        }
    }

    lifecycle::ensure_turn(tx, state, &record.timestamp)?;
    // Resolve only after admission. Ignored metadata-free native user records
    // historically did not validate their message ID.
    let source_id = message.source_id.resolve()?;
    let id = source_id
        .map(|source_id| {
            events::projected_call_id(&super::super::protocol::ProjectedCallId::Message {
                rollout_id: state.owner_id.clone(),
                source_id: source_id.to_owned(),
            })
        })
        .transpose()?
        .unwrap_or_else(|| event_id(state, record.source_line));
    tx.sqlite.execute(
        "INSERT OR IGNORE INTO messages(
            id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            record.timestamp,
            message.role.as_str(),
            message.content,
            record.source_line as i64,
        ],
    )?;

    if let Some(title) = message.title_fallback.as_deref() {
        metadata::apply_legacy_prompt_title_fallback(tx, &state.thread_id, title)?;
    }
    if message.role == MessageRole::Assistant {
        tx.sqlite.execute(
            "DELETE FROM events WHERE rollout_id=?1 AND turn_id IS ?2
             AND kind='update' AND label='Assistant update'
             AND ABS((julianday(timestamp)-julianday(?3))*86400.0)<1.0
             AND (body=?4 OR body LIKE ?4 || '%' OR ?4 LIKE body || '%')",
            params![
                state.owner_id,
                state.current_turn,
                record.timestamp,
                message.content
            ],
        )?;
    }

    let event = message_event(state, message, source_id);
    events::apply(tx, state, record.source_line, &record.timestamp, &event)?;
    if message.activity == MessageActivity::Final {
        lifecycle::complete_turn_from_final(tx, state, &record.timestamp, &message.content)?;
    }
    Ok(())
}

fn apply_canonical_reasoning(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    record: &DecodedConversationRecord,
    event: &ProjectedEvent,
    body_matches_projected_form: bool,
) -> Result<()> {
    lifecycle::ensure_turn(tx, state, &record.timestamp)?;
    tx.sqlite.execute(
        "DELETE FROM events WHERE rollout_id=?1 AND turn_id IS ?2
         AND kind='reasoning' AND label='Reasoning'
         AND ((?5=1 AND body=?3)
              OR ABS((julianday(timestamp)-julianday(?4))*86400.0)<1.0)",
        params![
            state.owner_id,
            state.current_turn,
            event.body,
            record.timestamp,
            body_matches_projected_form as i64,
        ],
    )?;
    events::apply(tx, state, record.source_line, &record.timestamp, event)?;
    Ok(())
}

fn apply_legacy_reasoning(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    record: &DecodedConversationRecord,
    event: &ProjectedEvent,
) -> Result<()> {
    let duplicate: i64 = tx.sqlite.query_row(
        "SELECT EXISTS(SELECT 1 FROM events
         WHERE rollout_id=?1 AND turn_id IS ?2 AND kind='reasoning' AND body=?3)",
        params![state.owner_id, state.current_turn, event.body],
        |row| row.get(0),
    )?;
    if duplicate == 0 {
        events::apply(tx, state, record.source_line, &record.timestamp, event)?;
    }
    Ok(())
}

fn apply_legacy_assistant_update(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    record: &DecodedConversationRecord,
    event: &ProjectedEvent,
) -> Result<()> {
    let canonical: i64 = tx.sqlite.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages
         WHERE rollout_id=?1 AND turn_id IS ?2 AND role='assistant'
         AND ABS((julianday(timestamp)-julianday(?3))*86400.0)<1.0
         AND (content=?4 OR content LIKE ?4 || '%' OR ?4 LIKE content || '%'))",
        params![
            state.owner_id,
            state.current_turn,
            record.timestamp,
            event.body
        ],
        |row| row.get(0),
    )?;
    if canonical == 0 {
        events::apply(tx, state, record.source_line, &record.timestamp, event)?;
    }
    Ok(())
}

fn turn_accepts_metadata_free_feedback(tx: &super::ProjectionTx<'_>, turn_id: &str) -> bool {
    let running = tx
        .sqlite
        .query_row(
            "SELECT status='running' FROM turns WHERE id=?1",
            [turn_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        != 0;
    running || lifecycle::turn_has_open_native_lifecycle(tx, turn_id)
}

fn reopen_provisionally_completed_turn(tx: &super::ProjectionTx<'_>, turn_id: &str) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE turns
         SET status='running',completed_at=NULL
         WHERE id=?1 AND status='completed'
           AND EXISTS(
               SELECT 1 FROM events
               WHERE turn_id=?1 AND kind='turn_started'
           )
           AND NOT EXISTS(
               SELECT 1 FROM events
               WHERE turn_id=?1
                 AND (
                     kind='turn_completed'
                     OR (kind='state' AND status IN ('interrupted','rolled_back'))
                 )
           )",
        [turn_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{
        ConversationStateTransition, DeferredMessageId, MessageActivity, MessageRole,
        decode_conversation_record,
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
            turn_context_seen: true,
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn record(line: u64, timestamp: &str, intent: ConversationIntent) -> DecodedConversationRecord {
        DecodedConversationRecord {
            source_line: line,
            timestamp: timestamp.into(),
            transition: ConversationStateTransition {
                last_timestamp: timestamp.into(),
                current_turn: Some("turn-1".into()),
            },
            intent,
        }
    }

    fn message(role: MessageRole, activity: MessageActivity, content: &str) -> ConversationIntent {
        ConversationIntent::Message(Box::new(MessageIntent {
            role,
            activity,
            content: content.into(),
            source_id: DeferredMessageId::Valid(Some("message-1".into())),
            has_explicit_turn: role == MessageRole::Assistant,
            allow_implicit_turn: false,
            title_fallback: None,
        }))
    }

    fn event(kind: &str, label: &str, body: &str) -> ProjectedEvent {
        ProjectedEvent {
            kind: kind.into(),
            role: Some("assistant".into()),
            label: Some(label.into()),
            body: Some(body.into()),
            status: None,
            tool_name: None,
            call_id: None,
            duration_ms: None,
            metadata: None,
        }
    }

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(
                    id TEXT PRIMARY KEY,title TEXT,title_updated_at TEXT,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE rollouts(
                    id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    agent_run_id TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT,
                    status TEXT NOT NULL,model TEXT,effort TEXT,last_agent_message TEXT
                 );
                 CREATE TABLE messages(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,timestamp TEXT NOT NULL,role TEXT NOT NULL,content TEXT NOT NULL,
                    source_line INTEGER NOT NULL
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

    fn insert_turn(connection: &Connection, status: &str, completed_at: Option<&str>) {
        connection
            .execute(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,completed_at,status,model,effort
                 ) VALUES('turn-1','thread-1','rollout-1','rollout-1',?1,?2,?3,'gpt-test','high')",
                params!["2026-07-25T08:00:00.000000000Z", completed_at, status],
            )
            .unwrap();
    }

    fn insert_native_start(connection: &Connection) {
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,label,status,native
                 ) VALUES('start','thread-1','rollout-1','turn-1','rollout-1',?1,1,
                          'turn_started','Turn started','running',1)",
                ["2026-07-25T08:00:00.000000000Z"],
            )
            .unwrap();
        connection
            .execute("DELETE FROM event_insert_observations", [])
            .unwrap();
    }

    #[test]
    fn metadata_free_feedback_reopens_provisional_native_final_on_the_same_turn() {
        let mut connection = connection();
        insert_turn(
            &connection,
            "completed",
            Some("2026-07-25T09:30:00.000000000Z"),
        );
        insert_native_start(&connection);
        let timestamp = "2026-07-25T10:00:00.000000000Z";
        let decoded = record(
            17,
            timestamp,
            message(MessageRole::User, MessageActivity::Message, "More feedback"),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(cursor.current_turn.as_deref(), Some("turn-1"));
        assert_eq!(cursor.last_timestamp.as_deref(), Some(timestamp));
        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at FROM turns WHERE id='turn-1'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .unwrap(),
            ("running".into(), None)
        );
        assert_eq!(
            connection
                .query_row("SELECT turn_id,role,content FROM messages", [], |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?
                )),)
                .unwrap(),
            ("turn-1".into(), "user".into(), "More feedback".into())
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
    fn canonical_final_removes_legacy_update_but_native_lifecycle_stays_authoritative() {
        let mut connection = connection();
        insert_turn(&connection, "running", None);
        insert_native_start(&connection);
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,role,label,body,native
                 ) VALUES('legacy','thread-1','rollout-1','turn-1','rollout-1',?1,2,
                          'update','assistant','Assistant update','The result is ready',1)",
                ["2026-07-25T10:00:00.100000000Z"],
            )
            .unwrap();
        connection
            .execute("DELETE FROM event_insert_observations", [])
            .unwrap();
        let timestamp = "2026-07-25T10:00:00.500000000Z";
        let decoded = record(
            17,
            timestamp,
            message(
                MessageRole::Assistant,
                MessageActivity::Final,
                "The result is ready.",
            ),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events WHERE id='legacy'", [], |row| {
                    row.get::<_, i64>(0)
                },)
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at,last_agent_message FROM turns WHERE id='turn-1'",
                    [],
                    |row| Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    )),
                )
                .unwrap(),
            ("running".into(), None, Some("The result is ready.".into()))
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT kind,call_id FROM events WHERE id='rollout-1:17'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            (
                "final".into(),
                format!("message:{}", serde_json::json!(["rollout-1", "message-1"]))
            )
        );
    }

    #[test]
    fn canonical_final_at_the_exact_one_second_boundary_keeps_the_legacy_update() {
        let mut connection = connection();
        insert_turn(&connection, "running", None);
        insert_native_start(&connection);
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,role,label,body,native
                 ) VALUES('legacy-boundary','thread-1','rollout-1','turn-1','rollout-1',?1,2,
                          'update','assistant','Assistant update','Boundary answer',1)",
                ["2026-07-25T10:00:04.100000000Z"],
            )
            .unwrap();
        connection
            .execute("DELETE FROM event_insert_observations", [])
            .unwrap();
        let timestamp = "2026-07-25T10:00:05.100000000Z";
        let decoded = record(
            17,
            timestamp,
            message(
                MessageRole::Assistant,
                MessageActivity::Final,
                "Boundary answer.",
            ),
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
                    "SELECT COUNT(*) FROM events WHERE id='legacy-boundary'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1,
            "the reconciliation window is strictly less than one second"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind IN ('update','final')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
    }

    #[test]
    fn canonical_reasoning_reconciles_legacy_and_legacy_update_respects_canonical_message() {
        let mut connection = connection();
        insert_turn(&connection, "running", None);
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,role,label,body,native
                 ) VALUES('legacy-reason','thread-1','rollout-1','turn-1','rollout-1',?1,2,
                          'reasoning','assistant','Reasoning','Inspect first.',1)",
                ["2026-07-25T10:00:00.100000000Z"],
            )
            .unwrap();
        let canonical_reason = decode_conversation_record(
            &state(),
            17,
            &serde_json::json!({
                "type":"response_item",
                "timestamp":"2026-07-25T10:00:00.500000000Z",
                "payload":{
                    "type":"reasoning","id":"reason-1",
                    "summary":[{"type":"summary_text","text":"Inspect first."}]
                }
            }),
        )
        .unwrap()
        .unwrap();
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &canonical_reason).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events
                     WHERE kind='reasoning' AND label='Reasoning summary'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );

        let legacy_reason = record(
            18,
            "2026-07-25T10:00:01.500000000Z",
            ConversationIntent::LegacyReasoning(Box::new(event(
                "reasoning",
                "Reasoning",
                "Inspect first.",
            ))),
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &legacy_reason).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='reasoning'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1,
            "legacy reasoning must not duplicate its canonical counterpart"
        );

        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
                 ) VALUES('canonical','thread-1','rollout-1','turn-1',?1,
                          'assistant','Canonical answer.',20)",
                ["2026-07-25T10:01:00.100000000Z"],
            )
            .unwrap();
        let legacy_update = record(
            21,
            "2026-07-25T10:01:00.500000000Z",
            ConversationIntent::LegacyAssistantUpdate(Box::new(event(
                "update",
                "Assistant update",
                "Canonical answer",
            ))),
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &legacy_update).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='update'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn redacted_canonical_reasoning_does_not_gain_raw_body_reconciliation() {
        let mut connection = connection();
        insert_turn(&connection, "running", None);
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,role,label,body,native
                 ) VALUES('legacy-redacted','thread-1','rollout-1','turn-1','rollout-1',?1,2,
                          'reasoning','assistant','Reasoning',
                          'Inspect [embedded attachment]',1)",
                ["2026-07-25T10:00:00.000000000Z"],
            )
            .unwrap();
        let decoded = decode_conversation_record(
            &state(),
            17,
            &serde_json::json!({
                "type":"response_item",
                "timestamp":"2026-07-25T10:02:00.000000000Z",
                "payload":{
                    "type":"reasoning","id":"reason-redacted",
                    "summary":[{
                        "type":"summary_text",
                        "text":"Inspect data:image/png;base64,REASONING_SECRET"
                    }]
                }
            }),
        )
        .unwrap()
        .unwrap();
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &decoded).unwrap();
        transaction.commit().unwrap();

        let rows = connection
            .prepare(
                "SELECT label,body FROM events WHERE kind='reasoning'
                 ORDER BY source_line,id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("Reasoning".into(), "Inspect [embedded attachment]".into()),
                (
                    "Reasoning summary".into(),
                    "Inspect [embedded attachment]".into(),
                ),
            ],
            "the legacy raw-body predicate did not equate redacted source text with projected text"
        );
    }

    #[test]
    fn late_final_cannot_change_an_explicit_terminal_turn() {
        let mut connection = connection();
        insert_turn(
            &connection,
            "interrupted",
            Some("2026-07-25T10:00:00.000000000Z"),
        );
        connection
            .execute(
                "UPDATE turns SET last_agent_message='Authoritative terminal message'
                 WHERE id='turn-1'",
                [],
            )
            .unwrap();
        insert_native_start(&connection);
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,label,status,native
                 ) VALUES('abort','thread-1','rollout-1','turn-1','rollout-1',?1,2,
                          'state','turn_aborted','interrupted',1)",
                ["2026-07-25T10:00:00.000000000Z"],
            )
            .unwrap();
        connection
            .execute("DELETE FROM event_insert_observations", [])
            .unwrap();
        let decoded = record(
            17,
            "2026-07-25T10:01:00.000000000Z",
            message(
                MessageRole::Assistant,
                MessageActivity::Final,
                "A late legacy final.",
            ),
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
                    "SELECT status,completed_at,last_agent_message FROM turns WHERE id='turn-1'",
                    [],
                    |row| Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    )),
                )
                .unwrap(),
            (
                "interrupted".into(),
                Some("2026-07-25T10:00:00.000000000Z".into()),
                Some("Authoritative terminal message".into()),
            )
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='final'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1,
            "the late message remains visible without rewriting terminal lifecycle"
        );
    }

    #[test]
    fn top_level_title_fallback_never_overwrites_an_existing_title() {
        let mut connection = connection();
        connection
            .execute(
                "UPDATE threads SET title='Existing',title_updated_at=NULL WHERE id='thread-1'",
                [],
            )
            .unwrap();
        let top_level = record(
            17,
            "2026-07-25T10:00:00.000000000Z",
            ConversationIntent::Message(Box::new(MessageIntent {
                role: MessageRole::User,
                activity: MessageActivity::Message,
                content: "Top-level prompt".into(),
                source_id: DeferredMessageId::Valid(None),
                has_explicit_turn: false,
                allow_implicit_turn: true,
                title_fallback: Some("Top-level prompt".into()),
            })),
        );
        let mut cursor = state();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &top_level).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT title FROM threads", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "Existing"
        );
    }

    #[test]
    fn ignored_unadmitted_user_message_does_not_resolve_invalid_id_and_failure_keeps_cursor() {
        let mut connection = connection();
        let mut cursor = state();
        cursor.turn_context_seen = false;
        cursor.current_turn = None;
        let ignored = DecodedConversationRecord {
            source_line: 17,
            timestamp: "2026-07-25T10:00:00.000000000Z".into(),
            transition: ConversationStateTransition {
                last_timestamp: "2026-07-25T10:00:00.000000000Z".into(),
                current_turn: None,
            },
            intent: ConversationIntent::Message(Box::new(MessageIntent {
                role: MessageRole::User,
                activity: MessageActivity::Message,
                content: "ignored".into(),
                source_id: DeferredMessageId::Invalid("must remain deferred".into()),
                has_explicit_turn: false,
                allow_implicit_turn: false,
                title_fallback: None,
            })),
        };
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &ignored).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            cursor.last_timestamp.as_deref(),
            Some(ignored.timestamp.as_str())
        );

        connection.execute("DROP TABLE events", []).unwrap();
        cursor.turn_context_seen = true;
        cursor.current_turn = Some("turn-1".into());
        insert_turn(&connection, "running", None);
        let before = serde_json::to_string(&cursor).unwrap();
        let failing = record(
            18,
            "2026-07-25T10:01:00.000000000Z",
            message(MessageRole::Assistant, MessageActivity::Final, "will fail"),
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        assert!(apply(&transaction, &mut cursor, &failing).is_err());
        assert_eq!(serde_json::to_string(&cursor).unwrap(), before);
        drop(transaction);
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM messages", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0,
            "the message insert before the failed event must roll back"
        );
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
