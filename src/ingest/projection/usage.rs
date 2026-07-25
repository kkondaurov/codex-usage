use super::super::protocol::{CursorState, UsageIntent, checked_token_count};
use super::{
    event_id,
    lifecycle::{ensure_turn, touch_owner},
};
use anyhow::Result;
use rusqlite::params;

const MODEL_ATTRIBUTION_REQUIRED_FROM_YEAR: i32 = 2026;

pub(super) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    line: u64,
    timestamp: &str,
    intent: &UsageIntent,
) -> Result<()> {
    let Some(usage) = intent.usage else {
        return Ok(());
    };
    let ignore_legacy_unattributed_usage = state.current_model.is_none()
        && timestamp
            .get(..4)
            .and_then(|year| year.parse::<i32>().ok())
            .is_some_and(|year| year < MODEL_ATTRIBUTION_REQUIRED_FROM_YEAR);
    if !usage.is_zero() && !ignore_legacy_unattributed_usage {
        let input_tokens = checked_token_count(usage.input_tokens, "input_tokens", line)?;
        let cached_input_tokens =
            checked_token_count(usage.cached_input_tokens, "cached_input_tokens", line)?;
        let output_tokens = checked_token_count(usage.output_tokens, "output_tokens", line)?;
        let reasoning_tokens = checked_token_count(
            usage.reasoning_output_tokens,
            "reasoning_output_tokens",
            line,
        )?;
        let total_tokens = checked_token_count(usage.total_tokens, "total_tokens", line)?;
        ensure_turn(tx, state, timestamp)?;
        tx.sqlite.execute(
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                model,effort,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,1)
             ON CONFLICT(id) DO NOTHING",
            params![
                event_id(state, line),
                state.thread_id,
                state.owner_id,
                state.current_turn,
                state.owner_id,
                timestamp,
                line as i64,
                state.current_model.as_deref().unwrap_or("unknown"),
                state.current_effort,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                reasoning_tokens,
                total_tokens,
            ],
        )?;
    }
    touch_owner(tx, state, timestamp)
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{CursorState, DecodedRecord, decode_usage_record};
    use rusqlite::Connection;

    #[test]
    fn typed_usage_projection_writes_the_exact_fact_turn_and_owner_bounds() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE rollouts(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT,rollout_id TEXT,agent_run_id TEXT,
                    started_at TEXT,status TEXT,model TEXT,effort TEXT
                 );
                 CREATE TABLE usage_facts(
                    id TEXT PRIMARY KEY,thread_id TEXT,rollout_id TEXT,turn_id TEXT,
                    agent_run_id TEXT,timestamp TEXT,source_line INTEGER,model TEXT,
                    effort TEXT,input_tokens INTEGER,cached_input_tokens INTEGER,
                    output_tokens INTEGER,reasoning_tokens INTEGER,total_tokens INTEGER,
                    native INTEGER
                 );
                 INSERT INTO threads VALUES('thread-1','2026-07-25T09:00:00.000000000Z');
                 INSERT INTO rollouts VALUES('rollout-1','2026-07-25T09:00:00.000000000Z');",
            )
            .unwrap();
        let state = CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            native_started: true,
            current_turn: Some("turn-1".into()),
            current_model: Some("gpt-test".into()),
            current_effort: Some("high".into()),
            ..CursorState::default()
        };
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T12:00:00+02:00",
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
        let decoded =
            DecodedRecord::Usage(decode_usage_record(&state, 17, &value).unwrap().unwrap());
        let mut candidate = state;

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        transaction.context(&mut candidate).apply(decoded).unwrap();
        transaction.commit().unwrap();

        let identity = connection
            .query_row(
                "SELECT id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,
                        source_line,model,effort
                 FROM usage_facts",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            identity,
            (
                "rollout-1:17".into(),
                "thread-1".into(),
                "rollout-1".into(),
                "turn-1".into(),
                "rollout-1".into(),
                "2026-07-25T10:00:00.000000000Z".into(),
                17,
                "gpt-test".into(),
                "high".into(),
            )
        );
        let counts = connection
            .query_row(
                "SELECT input_tokens,cached_input_tokens,output_tokens,
                        reasoning_tokens,total_tokens,native
                 FROM usage_facts",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(counts, (11, 7, 5, 3, 16, 1));
        assert_eq!(
            connection
                .query_row("SELECT last_event_at FROM threads", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "2026-07-25T10:00:00.000000000Z"
        );
        assert_eq!(
            connection
                .query_row("SELECT model || ':' || effort FROM turns", [], |row| row
                    .get::<_, String>(
                    0
                ))
                .unwrap(),
            "gpt-test:high"
        );
    }
}
