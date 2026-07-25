use super::super::{
    identifiers::{is_owner_native_turn, normalized_relational_identifier},
    intent::{CursorOnlyReason, CursorOnlyTransition, DecodedCursorOnlyRecord, DecodedRecord},
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use super::{
    decode_agent_record, decode_conversation_record, decode_event_tool_record,
    decode_lifecycle_record, decode_ordinary_record, decode_response_tool_record,
    decode_session_metadata_record, decode_thread_state_record, decode_title_event_record,
    decode_usage_record,
};
use anyhow::{Result, anyhow};
use serde_json::Value;

/// Decode one source value into exactly one closed protocol record.
///
/// This is the sole owner of family precedence and native-record admission.
/// It is deliberately pure: callers apply the returned transition and
/// projection intent only after their database work succeeds.
pub(in crate::ingest) fn decode_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<DecodedRecord> {
    // Token accounting precedes native admission. Inherited snapshots still
    // establish the cumulative baseline needed by the first native delta.
    if let Some(decoded) = decode_usage_record(state, line, value)? {
        return Ok(DecodedRecord::Usage(decoded));
    }

    let wire = WireRecord::new(value);
    let timestamp = record_timestamp(state, line, wire)?;
    let mut routing_state = state.clone();

    // Forked rollouts can begin with a replay of their parent's history. Only
    // this owner's native task-start opens the projection gate.
    if routing_state.forked && !routing_state.native_started {
        if wire.outer_type() == Some("event_msg") && wire.payload_type() == Some("task_started") {
            let turn_id = normalized_relational_identifier(
                wire.payload()
                    .and_then(|payload| payload.get("turn_id"))
                    .and_then(Value::as_str),
                "turn id",
            )?;
            if turn_id
                .as_deref()
                .is_some_and(|turn_id| is_owner_native_turn(&routing_state.owner_id, turn_id))
            {
                routing_state.native_started = true;
            }
        }
        if !routing_state.native_started {
            return Ok(cursor_only(
                line,
                timestamp,
                CursorOnlyReason::InheritedForkReplay,
            ));
        }
    }

    // Session metadata is admitted before the general native-start gate. This
    // preserves metadata for root/non-forked records whose native task has not
    // started yet, without admitting their ordinary conversation history.
    if let Some(decoded) = decode_session_metadata_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::Metadata(decoded));
    }
    if !routing_state.native_started {
        return Ok(cursor_only(
            line,
            timestamp,
            CursorOnlyReason::AwaitingNativeStart,
        ));
    }

    if let Some(decoded) = decode_title_event_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::Metadata(decoded));
    }
    if let Some(decoded) = decode_ordinary_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::Ordinary(decoded));
    }
    if let Some(decoded) = decode_thread_state_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::ThreadState(decoded));
    }
    if let Some(decoded) = decode_conversation_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::Conversation(decoded));
    }

    // Legacy tool calls are top-level records; canonical response items wrap
    // the same shape in `payload`. Passing the whole legacy value is required
    // to retain its type and identifiers.
    let response_payload = if wire.outer_type() == Some("response_item") {
        wire.payload().unwrap_or(&Value::Null)
    } else {
        value
    };
    if let Some(decoded) =
        decode_response_tool_record(&routing_state, line, &timestamp, response_payload)?
    {
        return Ok(DecodedRecord::Tool(decoded));
    }
    if wire.outer_type() == Some("event_msg")
        && let Some(decoded) = decode_event_tool_record(
            &routing_state,
            line,
            &timestamp,
            wire.payload().unwrap_or(&Value::Null),
        )?
    {
        return Ok(DecodedRecord::Tool(decoded));
    }
    if let Some(decoded) = decode_lifecycle_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::Lifecycle(decoded));
    }
    if let Some(decoded) = decode_agent_record(&routing_state, line, value)? {
        return Ok(DecodedRecord::Agent(decoded));
    }

    unreachable!("family decoders must claim every admitted source record")
}

fn record_timestamp(state: &CursorState, line: u64, wire: WireRecord<'_>) -> Result<String> {
    match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp),
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp")),
    }
}

fn cursor_only(line: u64, timestamp: String, reason: CursorOnlyReason) -> DecodedRecord {
    DecodedRecord::CursorOnly(DecodedCursorOnlyRecord {
        source_line: line,
        transition: CursorOnlyTransition {
            last_timestamp: timestamp.clone(),
        },
        timestamp,
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RecordKind {
        Usage,
        Metadata,
        Ordinary,
        ThreadState,
        Conversation,
        Tool,
        Lifecycle,
        Agent,
    }

    fn kind(record: &DecodedRecord) -> RecordKind {
        match record {
            DecodedRecord::Usage(_) => RecordKind::Usage,
            DecodedRecord::Metadata(_) => RecordKind::Metadata,
            DecodedRecord::Ordinary(_) => RecordKind::Ordinary,
            DecodedRecord::ThreadState(_) => RecordKind::ThreadState,
            DecodedRecord::Conversation(_) => RecordKind::Conversation,
            DecodedRecord::Tool(_) => RecordKind::Tool,
            DecodedRecord::Lifecycle(_) => RecordKind::Lifecycle,
            DecodedRecord::Agent(_) => RecordKind::Agent,
            DecodedRecord::CursorOnly(record) => {
                panic!("unexpected cursor-only record: {:?}", record.reason)
            }
        }
    }

    fn admitted_state() -> CursorState {
        CursorState {
            owner_id: "019f64aa-0000-7000-8000-000000000000".into(),
            thread_id: "019f64aa-0000-7000-8000-000000000000".into(),
            current_turn: Some("turn-1".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn timestamped(kind: &str, payload: Value) -> Value {
        serde_json::json!({
            "type": kind,
            "timestamp": "2026-07-25T10:00:00Z",
            "payload": payload,
        })
    }

    #[test]
    fn routes_every_family_in_precedence_order() {
        let owner = admitted_state().owner_id;
        let cases = vec![
            (
                "usage before every gate",
                timestamped(
                    "event_msg",
                    serde_json::json!({"type":"token_count","info":null}),
                ),
                RecordKind::Usage,
            ),
            (
                "session metadata",
                timestamped("session_meta", serde_json::json!({"id":owner})),
                RecordKind::Metadata,
            ),
            (
                "title metadata before ordinary events",
                timestamped(
                    "event_msg",
                    serde_json::json!({"type":"thread_name_updated","thread_name":"New title"}),
                ),
                RecordKind::Metadata,
            ),
            (
                "unknown fallback",
                timestamped("mystery", serde_json::json!({"value":1})),
                RecordKind::Ordinary,
            ),
            (
                "thread state",
                timestamped(
                    "event_msg",
                    serde_json::json!({
                        "type":"thread_goal_updated",
                        "goal":{"objective":"Ship it","status":"in_progress"}
                    }),
                ),
                RecordKind::ThreadState,
            ),
            (
                "conversation",
                timestamped(
                    "response_item",
                    serde_json::json!({
                        "type":"message",
                        "role":"assistant",
                        "content":[{"type":"output_text","text":"Hello"}]
                    }),
                ),
                RecordKind::Conversation,
            ),
            (
                "response tool",
                timestamped(
                    "response_item",
                    serde_json::json!({
                        "type":"function_call",
                        "call_id":"call-1",
                        "name":"exec"
                    }),
                ),
                RecordKind::Tool,
            ),
            (
                "event tool",
                timestamped(
                    "event_msg",
                    serde_json::json!({"type":"exec_command_end","call_id":"call-1"}),
                ),
                RecordKind::Tool,
            ),
            (
                "lifecycle",
                timestamped(
                    "event_msg",
                    serde_json::json!({"type":"task_complete","turn_id":"turn-1"}),
                ),
                RecordKind::Lifecycle,
            ),
            (
                "agent observation",
                timestamped(
                    "event_msg",
                    serde_json::json!({
                        "type":"sub_agent_activity",
                        "agent_thread_id":"agent-1",
                        "kind":"completed"
                    }),
                ),
                RecordKind::Agent,
            ),
        ];

        for (name, value, expected) in cases {
            let decoded = decode_record(&admitted_state(), 17, &value).unwrap();
            assert_eq!(kind(&decoded), expected, "{name}");
        }
    }

    #[test]
    fn legacy_top_level_tool_records_keep_their_type_and_route_to_tools() {
        let value = serde_json::json!({
            "type":"function_call",
            "timestamp":"2026-07-25T10:00:00Z",
            "call_id":"legacy-call",
            "name":"exec"
        });

        let decoded = decode_record(&admitted_state(), 29, &value).unwrap();
        let DecodedRecord::Tool(tool) = decoded else {
            panic!("legacy top-level tool was not routed to tools");
        };
        assert_eq!(tool.source_line, 29);
    }

    #[test]
    fn native_task_started_opens_the_fork_gate_without_mutating_input_state() {
        let state = CursorState {
            forked: true,
            native_started: false,
            ..admitted_state()
        };
        let value = timestamped(
            "event_msg",
            serde_json::json!({
                "type":"task_started",
                "turn_id":"019f64ab-0000-7000-8000-000000000000"
            }),
        );

        let decoded = decode_record(&state, 37, &value).unwrap();
        let DecodedRecord::Lifecycle(lifecycle) = decoded else {
            panic!("native task start was not admitted as lifecycle");
        };
        assert!(lifecycle.transition.native_started);
        assert!(!state.native_started);
    }

    #[test]
    fn inherited_fork_replay_is_an_explicit_cursor_only_record() {
        let state = CursorState {
            forked: true,
            native_started: false,
            ..admitted_state()
        };
        let value = timestamped(
            "event_msg",
            serde_json::json!({
                "type":"task_started",
                "turn_id":"019f64a9-0000-7000-8000-000000000000"
            }),
        );

        let decoded = decode_record(&state, 41, &value).unwrap();
        let DecodedRecord::CursorOnly(cursor) = decoded else {
            panic!("inherited task start was admitted");
        };
        assert_eq!(cursor.source_line, 41);
        assert_eq!(cursor.reason, CursorOnlyReason::InheritedForkReplay);
        assert_eq!(cursor.transition.last_timestamp, cursor.timestamp);
        assert!(!state.native_started);
    }

    #[test]
    fn invalid_timestamp_is_rejected_even_when_fork_replay_would_be_skipped() {
        let state = CursorState {
            forked: true,
            native_started: false,
            ..admitted_state()
        };
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"definitely-not-a-timestamp",
            "payload":{"type":"agent_message","message":"inherited"}
        });

        assert!(decode_record(&state, 43, &value).is_err());
    }

    #[test]
    fn session_metadata_precedes_the_nonfork_native_gate() {
        let state = CursorState {
            native_started: false,
            ..admitted_state()
        };
        let metadata = timestamped(
            "session_meta",
            serde_json::json!({"id":state.owner_id,"cwd":"/tmp/project"}),
        );
        assert!(matches!(
            decode_record(&state, 47, &metadata).unwrap(),
            DecodedRecord::Metadata(_)
        ));

        let ordinary = timestamped("mystery", serde_json::json!({"value":1}));
        let DecodedRecord::CursorOnly(cursor) = decode_record(&state, 48, &ordinary).unwrap()
        else {
            panic!("pre-native ordinary record was admitted");
        };
        assert_eq!(cursor.reason, CursorOnlyReason::AwaitingNativeStart);
    }
}
