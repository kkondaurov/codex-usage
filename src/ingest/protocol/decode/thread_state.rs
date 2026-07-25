use super::super::{
    event::{EventDraft, ProjectedEvent, shape_projected_event},
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use crate::redaction::redact_data_urls;
use anyhow::{Result, anyhow};
use serde_json::Value;

/// One database-independent thread-state event.
///
/// Goal and compaction source payloads are inspected only while decoding. The
/// durable intent carries the shaped event plus the exact normalized fields
/// needed by Projection's database-dependent duplicate checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedThreadStateRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: ThreadStateTransition,
    pub(in crate::ingest) intent: ThreadStateIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ThreadStateTransition {
    pub(in crate::ingest) last_timestamp: String,
}

impl ThreadStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum ThreadStateIntent {
    Goal(Option<Box<GoalUpdate>>),
    Compaction(Box<ProjectedEvent>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct GoalUpdate {
    /// These values deliberately precede ProjectedEvent's size bounding.
    ///
    /// The legacy duplicate policy compared the last stored row with the
    /// redacted objective and raw status before shaping the next event.
    pub(in crate::ingest) comparison_body: Option<String>,
    pub(in crate::ingest) comparison_status: Option<String>,
    pub(in crate::ingest) event: ProjectedEvent,
}

pub(in crate::ingest) fn decode_thread_state_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedThreadStateRecord>> {
    let wire = WireRecord::new(value);
    if wire.outer_type() != Some("event_msg") {
        return Ok(None);
    }
    let payload = wire.payload().unwrap_or(&Value::Null);
    let timestamp = match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp)?,
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp"))?,
    };

    let intent = match wire.payload_type() {
        Some("thread_goal_updated") => {
            let goal = payload.get("goal").unwrap_or(payload);
            let objective = goal
                .get("objective")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(redact_data_urls);
            let status = goal
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let update = if objective.is_some() || status.is_some() {
                Some(Box::new(GoalUpdate {
                    comparison_body: objective.clone(),
                    comparison_status: status.clone(),
                    event: shape_projected_event(
                        state,
                        EventDraft {
                            kind: "goal",
                            role: None,
                            label: Some("Goal updated"),
                            body: objective.as_deref(),
                            status: status.as_deref(),
                            tool_name: None,
                            duration_ms: None,
                            payload,
                        },
                    )?,
                }))
            } else {
                None
            };
            ThreadStateIntent::Goal(update)
        }
        Some("context_compacted") => {
            ThreadStateIntent::Compaction(Box::new(shape_projected_event(
                state,
                EventDraft {
                    kind: "compaction",
                    role: None,
                    label: Some("Context compacted"),
                    body: None,
                    status: None,
                    tool_name: None,
                    duration_ms: None,
                    payload,
                },
            )?))
        }
        _ => return Ok(None),
    };

    Ok(Some(DecodedThreadStateRecord {
        source_line: line,
        transition: ThreadStateTransition {
            last_timestamp: timestamp.clone(),
        },
        timestamp,
        intent,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::super::event::{
        CompactionMetadata, MetadataScalar, PROJECTED_EVENT_BODY_CHARS, ProjectedEventMetadata,
    };
    use super::*;
    use serde_json::Number;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-1".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    #[test]
    fn goal_decoding_keeps_exact_comparison_fields_but_only_a_bounded_shaped_event() {
        let long_objective = format!(
            "{} data:image/png;base64,PRIVATE",
            "g".repeat(PROJECTED_EVENT_BODY_CHARS + 200)
        );
        let decoded = decode_thread_state_record(
            &state(),
            17,
            &serde_json::json!({
                "type":"event_msg",
                "timestamp":"2026-07-25T12:15:30.125+02:30",
                "payload":{
                    "type":"thread_goal_updated",
                    "goal":{
                        "objective":format!("  {long_objective}  "),
                        "status":"active"
                    },
                    "transport_blob":"discard"
                }
            }),
        )
        .unwrap()
        .unwrap();

        assert_eq!(decoded.timestamp, "2026-07-25T09:45:30.125000000Z");
        let ThreadStateIntent::Goal(Some(update)) = decoded.intent else {
            panic!("expected a goal update")
        };
        assert_eq!(
            update.comparison_body.as_deref(),
            Some(
                format!(
                    "{} [embedded attachment]",
                    "g".repeat(PROJECTED_EVENT_BODY_CHARS + 200)
                )
                .as_str()
            )
        );
        assert_eq!(update.comparison_status.as_deref(), Some("active"));
        assert_eq!(update.event.kind, "goal");
        assert_eq!(update.event.label.as_deref(), Some("Goal updated"));
        assert_eq!(
            update.event.body.unwrap().chars().count(),
            PROJECTED_EVENT_BODY_CHARS + 1
        );
        assert_eq!(update.event.status.as_deref(), Some("active"));
        assert_eq!(update.event.metadata, None);
    }

    #[test]
    fn empty_goal_is_a_typed_touch_only_record() {
        let decoded = decode_thread_state_record(
            &state(),
            18,
            &serde_json::json!({
                "type":"event_msg",
                "payload":{"type":"thread_goal_updated","goal":{"objective":"  "}}
            }),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            decoded.intent,
            ThreadStateIntent::Goal(None),
            "empty goal heartbeats remain claimed but project no event"
        );
        assert_eq!(decoded.timestamp, "2026-07-25T09:00:00.000000000Z");
    }

    #[test]
    fn context_compaction_keeps_only_the_shaped_summary_and_typed_metadata() {
        let decoded = decode_thread_state_record(
            &state(),
            19,
            &serde_json::json!({
                "type":"event_msg",
                "timestamp":"2026-07-25T10:00:00Z",
                "payload":{
                    "type":"context_compacted",
                    "message":"  Continue from the verified handoff.  ",
                    "replacement_history":[1,2,3],
                    "window_number":4,
                    "first_window_id":"window-1",
                    "transport_blob":"discard"
                }
            }),
        )
        .unwrap()
        .unwrap();

        let ThreadStateIntent::Compaction(event) = decoded.intent else {
            panic!("expected a compaction event")
        };
        assert_eq!(event.kind, "compaction");
        assert_eq!(event.label.as_deref(), Some("Context compacted"));
        assert_eq!(
            event.body.as_deref(),
            Some("Continue from the verified handoff.")
        );
        assert_eq!(
            event.metadata,
            Some(ProjectedEventMetadata::Compaction(CompactionMetadata {
                replacement_history_count: Some(3),
                window_number: Some(MetadataScalar::Number(Number::from(4))),
                first_window_id: Some(MetadataScalar::String("window-1".into())),
                previous_window_id: None,
                window_id: None,
            }))
        );
    }

    #[test]
    fn decoder_claims_only_the_two_event_message_families() {
        for value in [
            serde_json::json!({
                "type":"compacted",
                "timestamp":"2026-07-25T10:00:00Z",
                "payload":{"message":"owned by ordinary"}
            }),
            serde_json::json!({
                "type":"event_msg",
                "timestamp":"2026-07-25T10:00:00Z",
                "payload":{"type":"task_started"}
            }),
            serde_json::json!({
                "type":"response_item",
                "timestamp":"2026-07-25T10:00:00Z",
                "payload":{"type":"thread_goal_updated"}
            }),
        ] {
            assert!(
                decode_thread_state_record(&state(), 20, &value)
                    .unwrap()
                    .is_none()
            );
        }
    }
}
