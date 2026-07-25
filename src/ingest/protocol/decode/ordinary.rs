use super::super::{
    event::{EventDraft, ProjectedEvent, shape_projected_event},
    identifiers::normalized_relational_identifier,
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use anyhow::{Result, anyhow};
use serde_json::Value;

/// A database-independent ordinary source record.
///
/// This is deliberately a closed list. Returning `None` leaves conversation,
/// tool, metadata, lifecycle, agent, and future event families to their
/// authoritative decoders instead of letting an eager unknown-record branch
/// steal them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedOrdinaryRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: OrdinaryStateTransition,
    pub(in crate::ingest) intent: OrdinaryIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct OrdinaryStateTransition {
    pub(in crate::ingest) last_timestamp: String,
    pub(in crate::ingest) current_turn: Option<String>,
    pub(in crate::ingest) turn_context_seen: bool,
    pub(in crate::ingest) current_model: Option<String>,
    pub(in crate::ingest) current_effort: Option<String>,
}

impl OrdinaryStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
        state.current_turn.clone_from(&self.current_turn);
        state.turn_context_seen = self.turn_context_seen;
        state.current_model.clone_from(&self.current_model);
        state.current_effort.clone_from(&self.current_effort);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum OrdinaryIntent {
    TurnContext,
    ThreadSettingsApplied,
    Event(Box<ProjectedEvent>),
    Noop(OrdinaryNoop),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum OrdinaryNoop {
    WorldState,
    InterAgentCommunicationMetadata,
    ViewImageToolCall,
    DynamicToolCallRequest,
    NonPlanItemCompleted,
    GhostSnapshot,
}

pub(in crate::ingest) fn decode_ordinary_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedOrdinaryRecord>> {
    let wire = WireRecord::new(value);
    let payload = wire.payload().unwrap_or(&Value::Null);

    let family = match wire.outer_type() {
        Some("turn_context") => OrdinaryFamily::TurnContext(payload),
        Some("compacted") => OrdinaryFamily::Compacted(payload),
        Some("world_state") => OrdinaryFamily::Noop(OrdinaryNoop::WorldState),
        Some("inter_agent_communication_metadata") => {
            OrdinaryFamily::Noop(OrdinaryNoop::InterAgentCommunicationMetadata)
        }
        Some("response_item") => match wire.payload_type() {
            Some("ghost_snapshot") => OrdinaryFamily::Noop(OrdinaryNoop::GhostSnapshot),
            Some(kind) if is_deferred_response_kind(kind) => return Ok(None),
            kind => OrdinaryFamily::Unknown {
                label: kind.unwrap_or("unknown"),
                payload,
            },
        },
        Some("event_msg") => match wire.payload_type() {
            Some("thread_settings_applied") => OrdinaryFamily::ThreadSettings(payload),
            Some("item_completed") => OrdinaryFamily::ItemCompleted(payload),
            Some("entered_review_mode") => OrdinaryFamily::ReviewMode {
                payload,
                entered: true,
            },
            Some("exited_review_mode") => OrdinaryFamily::ReviewMode {
                payload,
                entered: false,
            },
            Some("view_image_tool_call") => OrdinaryFamily::Noop(OrdinaryNoop::ViewImageToolCall),
            Some("dynamic_tool_call_request") => {
                OrdinaryFamily::Noop(OrdinaryNoop::DynamicToolCallRequest)
            }
            Some(kind) if is_deferred_event_kind(kind) => return Ok(None),
            kind => OrdinaryFamily::Unknown {
                label: kind.unwrap_or("unknown"),
                payload,
            },
        },
        Some(kind) if is_deferred_top_level_kind(kind) => return Ok(None),
        Some(kind) => OrdinaryFamily::Unknown {
            label: kind,
            payload: value.get("payload").unwrap_or(value),
        },
        None => OrdinaryFamily::Unknown {
            label: "unknown",
            payload: value.get("payload").unwrap_or(value),
        },
    };

    let timestamp = match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp)?,
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp"))?,
    };
    let mut transition = OrdinaryStateTransition::from_state(state, timestamp.clone());

    let intent = match family {
        OrdinaryFamily::TurnContext(payload) => {
            transition.current_turn = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )?
            .or_else(|| state.current_turn.clone());
            transition.turn_context_seen = true;
            transition.current_model = payload
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_model.clone());
            transition.current_effort = payload
                .get("effort")
                .or_else(|| payload.get("reasoning_effort"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_effort.clone());
            OrdinaryIntent::TurnContext
        }
        OrdinaryFamily::ThreadSettings(payload) => {
            let settings = payload.get("thread_settings").unwrap_or(payload);
            transition.current_model = settings
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_model.clone());
            transition.current_effort = settings
                .get("reasoning_effort")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_effort.clone());
            OrdinaryIntent::ThreadSettingsApplied
        }
        OrdinaryFamily::ItemCompleted(payload) => {
            let item = payload.get("item").unwrap_or(&Value::Null);
            if item.get("type").and_then(Value::as_str) == Some("Plan") {
                OrdinaryIntent::Event(Box::new(shape_projected_event(
                    state,
                    EventDraft {
                        kind: "plan",
                        role: None,
                        label: Some("Plan"),
                        body: item.get("text").and_then(Value::as_str),
                        status: Some("completed"),
                        tool_name: None,
                        duration_ms: None,
                        payload,
                    },
                )?))
            } else {
                OrdinaryIntent::Noop(OrdinaryNoop::NonPlanItemCompleted)
            }
        }
        OrdinaryFamily::ReviewMode { payload, entered } => {
            let (label, status) = if entered {
                ("Entered review mode", "active")
            } else {
                ("Exited review mode", "completed")
            };
            OrdinaryIntent::Event(Box::new(shape_projected_event(
                state,
                EventDraft {
                    kind: "state",
                    role: None,
                    label: Some(label),
                    body: None,
                    status: Some(status),
                    tool_name: None,
                    duration_ms: None,
                    payload,
                },
            )?))
        }
        OrdinaryFamily::Compacted(payload) => {
            OrdinaryIntent::Event(Box::new(shape_projected_event(
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
        OrdinaryFamily::Unknown { label, payload } => {
            OrdinaryIntent::Event(Box::new(shape_projected_event(
                state,
                EventDraft {
                    kind: "system",
                    role: None,
                    label: Some(label),
                    body: None,
                    status: None,
                    tool_name: None,
                    duration_ms: None,
                    payload,
                },
            )?))
        }
        OrdinaryFamily::Noop(noop) => OrdinaryIntent::Noop(noop),
    };

    Ok(Some(DecodedOrdinaryRecord {
        source_line: line,
        timestamp,
        transition,
        intent,
    }))
}

impl OrdinaryStateTransition {
    fn from_state(state: &CursorState, last_timestamp: String) -> Self {
        Self {
            last_timestamp,
            current_turn: state.current_turn.clone(),
            turn_context_seen: state.turn_context_seen,
            current_model: state.current_model.clone(),
            current_effort: state.current_effort.clone(),
        }
    }
}

enum OrdinaryFamily<'a> {
    TurnContext(&'a Value),
    ThreadSettings(&'a Value),
    ItemCompleted(&'a Value),
    ReviewMode { payload: &'a Value, entered: bool },
    Compacted(&'a Value),
    Unknown { label: &'a str, payload: &'a Value },
    Noop(OrdinaryNoop),
}

fn is_deferred_top_level_kind(kind: &str) -> bool {
    matches!(
        kind,
        "session_meta"
            | "message"
            | "agent_message"
            | "reasoning"
            | "function_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "tool_search_call"
            | "tool_search_output"
            | "web_search_call"
            | "image_generation_call"
    )
}

fn is_deferred_response_kind(kind: &str) -> bool {
    matches!(
        kind,
        "message"
            | "agent_message"
            | "reasoning"
            | "function_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "tool_search_call"
            | "tool_search_output"
            | "web_search_call"
            | "image_generation_call"
    )
}

fn is_deferred_event_kind(kind: &str) -> bool {
    matches!(
        kind,
        "token_count"
            | "task_started"
            | "task_complete"
            | "user_message"
            | "agent_reasoning"
            | "agent_message"
            | "sub_agent_activity"
            | "thread_goal_updated"
            | "context_compacted"
            | "thread_name_updated"
            | "turn_aborted"
            | "thread_rolled_back"
            | "exec_command_end"
            | "dynamic_tool_call_response"
            | "mcp_tool_call_end"
            | "patch_apply_end"
            | "web_search_end"
            | "image_generation_end"
    )
}

#[cfg(test)]
mod tests {
    use super::super::super::event::{CompactionMetadata, MetadataScalar, ProjectedEventMetadata};
    use super::*;
    use serde_json::Number;

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

    fn decode(value: Value) -> DecodedOrdinaryRecord {
        decode_ordinary_record(&state(), 17, &value)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn turn_context_canonicalizes_timestamp_and_carries_the_exact_next_state() {
        let decoded = decode(serde_json::json!({
            "type":"turn_context",
            "timestamp":"2026-07-25T12:15:30.125+02:30",
            "payload":{
                "turn_id":"  turn-new  ",
                "model":"gpt-new",
                "reasoning_effort":"high"
            }
        }));

        assert_eq!(decoded.timestamp, "2026-07-25T09:45:30.125000000Z");
        assert_eq!(decoded.transition.last_timestamp, decoded.timestamp);
        assert_eq!(decoded.transition.current_turn.as_deref(), Some("turn-new"));
        assert!(decoded.transition.turn_context_seen);
        assert_eq!(decoded.transition.current_model.as_deref(), Some("gpt-new"));
        assert_eq!(decoded.transition.current_effort.as_deref(), Some("high"));
        assert_eq!(decoded.intent, OrdinaryIntent::TurnContext);
    }

    #[test]
    fn settings_support_nested_and_flat_shapes_without_changing_turn_context_state() {
        let nested = decode(serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"thread_settings_applied",
                "thread_settings":{"model":"gpt-nested","reasoning_effort":"low"}
            }
        }));
        assert_eq!(
            nested.transition.current_model.as_deref(),
            Some("gpt-nested")
        );
        assert_eq!(nested.transition.current_effort.as_deref(), Some("low"));
        assert!(!nested.transition.turn_context_seen);

        let flat = decode(serde_json::json!({
            "type":"event_msg",
            "payload":{
                "type":"thread_settings_applied",
                "model":"gpt-flat",
                "reasoning_effort":"xhigh"
            }
        }));
        assert_eq!(flat.timestamp, "2026-07-25T09:00:00.000000000Z");
        assert_eq!(flat.transition.current_model.as_deref(), Some("gpt-flat"));
        assert_eq!(flat.transition.current_effort.as_deref(), Some("xhigh"));
        assert_eq!(flat.intent, OrdinaryIntent::ThreadSettingsApplied);
    }

    #[test]
    fn item_completed_is_always_claimed_but_only_a_plan_becomes_an_event() {
        let plan = decode(serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"item_completed",
                "item":{"type":"Plan","text":"Inspect, implement, verify."},
                "secret":"discard me"
            }
        }));
        let OrdinaryIntent::Event(event) = plan.intent else {
            panic!("plan must become a typed event");
        };
        assert_eq!(event.kind, "plan");
        assert_eq!(event.label.as_deref(), Some("Plan"));
        assert_eq!(event.body.as_deref(), Some("Inspect, implement, verify."));
        assert_eq!(event.status.as_deref(), Some("completed"));
        assert_eq!(event.metadata, None);

        let non_plan = decode(serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{"type":"item_completed","item":{"type":"Todo"}}
        }));
        assert_eq!(
            non_plan.intent,
            OrdinaryIntent::Noop(OrdinaryNoop::NonPlanItemCompleted)
        );
    }

    #[test]
    fn review_compaction_and_explicit_noops_are_typed_without_raw_json() {
        let entered = decode(serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{"type":"entered_review_mode","transport_blob":"discard"}
        }));
        let OrdinaryIntent::Event(event) = entered.intent else {
            panic!("review mode must become a typed event");
        };
        assert_eq!(event.kind, "state");
        assert_eq!(event.label.as_deref(), Some("Entered review mode"));
        assert_eq!(event.status.as_deref(), Some("active"));

        let compacted = decode(serde_json::json!({
            "type":"compacted",
            "timestamp":"2026-07-25T10:00:01Z",
            "payload":{
                "message":" Summary ",
                "replacement_history":[1,2],
                "window_number":3,
                "secret":"discard"
            }
        }));
        let OrdinaryIntent::Event(event) = compacted.intent else {
            panic!("compaction must become a typed event");
        };
        assert_eq!(event.kind, "compaction");
        assert_eq!(event.body.as_deref(), Some("Summary"));
        assert_eq!(
            event.metadata,
            Some(ProjectedEventMetadata::Compaction(CompactionMetadata {
                replacement_history_count: Some(2),
                window_number: Some(MetadataScalar::Number(Number::from(3))),
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }))
        );

        for (value, expected) in [
            (
                serde_json::json!({
                    "type":"world_state","timestamp":"2026-07-25T10:00:00Z"
                }),
                OrdinaryNoop::WorldState,
            ),
            (
                serde_json::json!({
                    "type":"event_msg","timestamp":"2026-07-25T10:00:00Z",
                    "payload":{"type":"view_image_tool_call"}
                }),
                OrdinaryNoop::ViewImageToolCall,
            ),
        ] {
            assert_eq!(decode(value).intent, OrdinaryIntent::Noop(expected));
        }
    }

    #[test]
    fn response_item_fallbacks_are_claimed_without_retaining_payloads() {
        let ghost = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{"type":"ghost_snapshot","snapshot":{"large":"discard"}}
        }));
        assert_eq!(
            ghost.intent,
            OrdinaryIntent::Noop(OrdinaryNoop::GhostSnapshot)
        );

        let unknown = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T10:00:01Z",
            "payload":{
                "type":"future_response_item",
                "schema_version":7,
                "secret":{"large":"discard"}
            }
        }));
        let OrdinaryIntent::Event(event) = unknown.intent else {
            panic!("unknown response item must become a typed system event");
        };
        assert_eq!(event.kind, "system");
        assert_eq!(event.label.as_deref(), Some("future_response_item"));
        assert!(event.body.is_none());
        assert!(matches!(
            event.metadata,
            Some(ProjectedEventMetadata::Unknown(_))
        ));
    }

    #[test]
    fn every_current_deferred_family_is_not_stolen() {
        for value in [
            serde_json::json!({"type":"session_meta","payload":{"id":"rollout-1"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message"}}),
            serde_json::json!({"type":"message","role":"user"}),
            serde_json::json!({"type":"agent_message"}),
            serde_json::json!({"type":"reasoning"}),
            serde_json::json!({"type":"function_call"}),
            serde_json::json!({"type":"function_call_output"}),
            serde_json::json!({"type":"custom_tool_call"}),
            serde_json::json!({"type":"custom_tool_call_output"}),
            serde_json::json!({"type":"tool_search_call"}),
            serde_json::json!({"type":"tool_search_output"}),
            serde_json::json!({"type":"web_search_call"}),
            serde_json::json!({"type":"image_generation_call"}),
        ] {
            assert_eq!(
                decode_ordinary_record(&state(), 23, &value).unwrap(),
                None,
                "unexpectedly claimed {value}"
            );
        }

        for kind in [
            "token_count",
            "task_started",
            "task_complete",
            "user_message",
            "agent_reasoning",
            "agent_message",
            "sub_agent_activity",
            "thread_goal_updated",
            "context_compacted",
            "thread_name_updated",
            "turn_aborted",
            "thread_rolled_back",
            "exec_command_end",
            "dynamic_tool_call_response",
            "mcp_tool_call_end",
            "patch_apply_end",
            "web_search_end",
            "image_generation_end",
        ] {
            let value = serde_json::json!({"type":"event_msg","payload":{"type":kind}});
            assert_eq!(
                decode_ordinary_record(&state(), 23, &value).unwrap(),
                None,
                "unexpectedly claimed event_msg.{kind}"
            );
        }
    }

    #[test]
    fn future_top_level_and_event_message_become_bounded_typed_system_events() {
        for value in [
            serde_json::json!({
                "type":"future_top_level",
                "timestamp":"2026-07-25T10:00:00Z",
                "payload":{"version":2,"secret":{"large":"discard"}}
            }),
            serde_json::json!({
                "type":"event_msg",
                "timestamp":"2026-07-25T10:00:00Z",
                "payload":{"type":"future_event","schema_version":3,"body":"discard"}
            }),
        ] {
            let expected_label = if value["type"] == "event_msg" {
                "future_event"
            } else {
                "future_top_level"
            };
            let decoded = decode(value);
            let OrdinaryIntent::Event(event) = decoded.intent else {
                panic!("unknown record must become a typed system event");
            };
            assert_eq!(event.kind, "system");
            assert_eq!(event.label.as_deref(), Some(expected_label));
            assert!(event.body.is_none());
            assert!(matches!(
                event.metadata,
                Some(ProjectedEventMetadata::Unknown(_))
            ));
        }
    }

    #[test]
    fn claimed_record_without_timestamp_uses_prior_or_reports_the_exact_boundary_error() {
        let value = serde_json::json!({"type":"world_state"});
        let decoded = decode_ordinary_record(&state(), 29, &value)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.timestamp, "2026-07-25T09:00:00.000000000Z");

        let error = decode_ordinary_record(&CursorState::default(), 29, &value).unwrap_err();
        assert_eq!(
            error.to_string(),
            "source line 29 has no timestamp and no prior timestamp"
        );
    }
}
