use super::super::protocol::{CursorState, DecodedRecord};
use super::{
    ProjectionTx, agents, conversation, lifecycle, metadata, ordinary, thread_state, tools, usage,
};
use anyhow::Result;

/// Atomic boundary between the pure protocol and its SQLite projection.
///
/// Every record is applied to a complete candidate cursor. The live cursor is
/// published only after all SQL and family-specific projection work succeeds.
pub(in crate::ingest) struct ProjectionContext<'transaction, 'state, 'connection> {
    tx: &'transaction ProjectionTx<'connection>,
    state: &'state mut CursorState,
}

impl<'connection> ProjectionTx<'connection> {
    pub(in crate::ingest) fn context<'transaction, 'state>(
        &'transaction self,
        state: &'state mut CursorState,
    ) -> ProjectionContext<'transaction, 'state, 'connection> {
        ProjectionContext { tx: self, state }
    }
}

impl ProjectionContext<'_, '_, '_> {
    pub(in crate::ingest) fn apply(self, record: DecodedRecord) -> Result<()> {
        let mut candidate = self.state.clone();
        apply_to_candidate(self.tx, &mut candidate, &record)?;
        *self.state = candidate;
        Ok(())
    }
}

fn apply_to_candidate(
    tx: &ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedRecord,
) -> Result<()> {
    match record {
        DecodedRecord::Usage(record) => {
            record.transition.apply_to(state);
            usage::apply(
                tx,
                state,
                record.source_line,
                &record.timestamp,
                &record.intent,
            )?;
        }
        DecodedRecord::CursorOnly(record) => record.transition.apply_to(state),
        DecodedRecord::Metadata(record) => metadata::apply(tx, state, record)?,
        DecodedRecord::Ordinary(record) => ordinary::apply(tx, state, record)?,
        DecodedRecord::ThreadState(record) => thread_state::apply(tx, state, record)?,
        DecodedRecord::Conversation(record) => conversation::apply(tx, state, record)?,
        DecodedRecord::Tool(record) => tools::apply(tx, state, record)?,
        DecodedRecord::Lifecycle(record) => lifecycle::apply(tx, state, record)?,
        DecodedRecord::Agent(record) => agents::apply(tx, state, record)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{
        CursorOnlyReason, CursorState, DecodedOrdinaryRecord, DecodedRecord, OrdinaryIntent,
        OrdinaryStateTransition, decode_record,
    };
    use super::super::ProjectionConnection;
    use rusqlite::Connection;

    const OLD_TIMESTAMP: &str = "2026-07-25T09:00:00.000000000Z";
    const NEW_TIMESTAMP: &str = "2026-07-25T10:00:00.000000000Z";

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            native_started: true,
            current_turn: Some("turn-old".into()),
            current_model: Some("gpt-old".into()),
            current_effort: Some("medium".into()),
            last_timestamp: Some(OLD_TIMESTAMP.into()),
            ..CursorState::default()
        }
    }

    fn core_connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE rollouts(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    agent_run_id TEXT NOT NULL,started_at TEXT NOT NULL,status TEXT NOT NULL,
                    model TEXT,effort TEXT
                 );
                 CREATE TABLE usage_facts(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,model TEXT NOT NULL,effort TEXT,
                    input_tokens INTEGER NOT NULL,cached_input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,reasoning_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL,native INTEGER NOT NULL
                 );
                 INSERT INTO threads VALUES('thread-1','2026-07-25T08:00:00.000000000Z');
                 INSERT INTO rollouts VALUES('rollout-1','2026-07-25T08:00:00.000000000Z');",
            )
            .unwrap();
        connection
    }

    #[test]
    fn sql_failure_preserves_exact_cursor_and_transaction_rollback_removes_prior_rows() {
        let mut connection = core_connection();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_rollout_touch
                 BEFORE UPDATE ON rollouts
                 BEGIN
                    SELECT RAISE(ABORT,'forced projection failure');
                 END;",
            )
            .unwrap();
        let record = DecodedRecord::Ordinary(DecodedOrdinaryRecord {
            source_line: 17,
            timestamp: NEW_TIMESTAMP.into(),
            transition: OrdinaryStateTransition {
                last_timestamp: NEW_TIMESTAMP.into(),
                current_turn: Some("turn-new".into()),
                turn_context_seen: true,
                current_model: Some("gpt-new".into()),
                current_effort: Some("high".into()),
            },
            intent: OrdinaryIntent::TurnContext,
        });
        let mut cursor = state();
        let before = serde_json::to_vec(&cursor).unwrap();

        let transaction = ProjectionConnection::new(&mut connection)
            .begin_file_projection()
            .unwrap();
        let error = transaction.context(&mut cursor).apply(record).unwrap_err();

        assert!(error.to_string().contains("forced projection failure"));
        assert_eq!(serde_json::to_vec(&cursor).unwrap(), before);
        assert_eq!(
            transaction
                .sqlite
                .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1,
            "the failure must occur after a projection row was written"
        );
        transaction.rollback().unwrap();

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT last_event_at FROM threads", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "2026-07-25T08:00:00.000000000Z"
        );
    }

    #[test]
    fn usage_failure_preserves_cumulative_tokens_and_timestamp() {
        let mut connection = core_connection();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_usage
                 BEFORE INSERT ON usage_facts
                 BEGIN
                    SELECT RAISE(ABORT,'forced usage failure');
                 END;",
            )
            .unwrap();
        let mut cursor = state();
        let before = serde_json::to_vec(&cursor).unwrap();
        let before_cumulative = cursor.cumulative;
        let before_timestamp = cursor.last_timestamp.clone();
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":NEW_TIMESTAMP,
            "payload":{"type":"token_count","info":{
                "total_token_usage":{
                    "input_tokens":11,
                    "cached_input_tokens":7,
                    "output_tokens":5,
                    "reasoning_output_tokens":3,
                    "total_tokens":16
                }
            }}
        });
        let record = decode_record(&cursor, 23, &value).unwrap();

        let transaction = ProjectionConnection::new(&mut connection)
            .begin_file_projection()
            .unwrap();
        let error = transaction.context(&mut cursor).apply(record).unwrap_err();

        assert!(error.to_string().contains("forced usage failure"));
        assert_eq!(cursor.cumulative, before_cumulative);
        assert_eq!(cursor.last_timestamp, before_timestamp);
        assert_eq!(serde_json::to_vec(&cursor).unwrap(), before);
        transaction.rollback().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn cursor_only_changes_only_its_allowed_state_without_sql() {
        let mut connection = Connection::open_in_memory().unwrap();
        let mut cursor = state();
        cursor.forked = true;
        cursor.native_started = false;
        let mut expected = cursor.clone();
        expected.last_timestamp = Some(NEW_TIMESTAMP.into());
        let value = serde_json::json!({
            "type":"response_item",
            "timestamp":NEW_TIMESTAMP,
            "payload":{"type":"message","role":"user","content":[]}
        });
        let record = decode_record(&cursor, 29, &value).unwrap();
        assert!(matches!(
            record,
            DecodedRecord::CursorOnly(ref record)
                if record.reason == CursorOnlyReason::InheritedForkReplay
                    && record.source_line == 29
                    && record.timestamp == NEW_TIMESTAMP
        ));

        let transaction = ProjectionConnection::new(&mut connection)
            .begin_file_projection()
            .unwrap();
        transaction.context(&mut cursor).apply(record).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            serde_json::to_vec(&cursor).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
    }
}
