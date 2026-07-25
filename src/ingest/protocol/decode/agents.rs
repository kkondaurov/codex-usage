use super::super::{
    content::normalized_metadata_value,
    event::{EventDraft, ProjectedEvent, shape_projected_event},
    identifiers::normalized_relational_identifier,
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use anyhow::{Result, anyhow};
use serde_json::Value;

const PROJECTED_SESSION_PATH_CHARS: usize = 4 * 1024;

/// One database-independent parent observation of a child agent.
///
/// Raw source JSON is consumed while decoding the record. Projection receives
/// only normalized identity/path fields, a closed lifecycle value, and the
/// already-shaped event that preserves the observation's durable lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedAgentRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: AgentStateTransition,
    pub(in crate::ingest) observation: AgentObservation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct AgentStateTransition {
    pub(in crate::ingest) last_timestamp: String,
}

impl AgentStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct AgentObservation {
    pub(in crate::ingest) agent_id: Option<String>,
    pub(in crate::ingest) agent_path: Option<String>,
    pub(in crate::ingest) activity: ObservedAgentActivity,
    pub(in crate::ingest) event: ProjectedEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum ObservedAgentActivity {
    Running,
    Completed,
    Interrupted,
    RolledBack,
}

impl ObservedAgentActivity {
    pub(in crate::ingest) fn from_source_kind(kind: Option<&str>) -> Self {
        match kind {
            Some("completed") => Self::Completed,
            Some("interrupted") => Self::Interrupted,
            Some("rolled_back") => Self::RolledBack,
            _ => Self::Running,
        }
    }

    pub(in crate::ingest) fn status(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
            Self::RolledBack => "rolled_back",
        }
    }

    pub(in crate::ingest) fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

pub(in crate::ingest) fn decode_agent_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedAgentRecord>> {
    let wire = WireRecord::new(value);
    if wire.outer_type() != Some("event_msg") || wire.payload_type() != Some("sub_agent_activity") {
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
    let agent_id = normalized_relational_identifier(
        payload.get("agent_thread_id").and_then(Value::as_str),
        "subagent thread id",
    )?;
    let agent_path = normalized_metadata_value(
        payload.get("agent_path").and_then(Value::as_str),
        PROJECTED_SESSION_PATH_CHARS,
    );
    let activity =
        ObservedAgentActivity::from_source_kind(payload.get("kind").and_then(Value::as_str));
    let event = shape_projected_event(
        state,
        EventDraft {
            kind: "subagent",
            role: None,
            label: payload.get("kind").and_then(Value::as_str),
            body: payload.get("agent_path").and_then(Value::as_str),
            status: payload.get("kind").and_then(Value::as_str),
            tool_name: None,
            duration_ms: None,
            payload,
        },
    )?;

    Ok(Some(DecodedAgentRecord {
        source_line: line,
        timestamp: timestamp.clone(),
        transition: AgentStateTransition {
            last_timestamp: timestamp,
        },
        observation: AgentObservation {
            agent_id,
            agent_path,
            activity,
            event,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::super::super::event::{
        PROJECTED_EVENT_BODY_CHARS, ProjectedEventMetadata, SubagentMetadata,
    };
    use super::*;

    fn state() -> CursorState {
        CursorState {
            owner_id: "parent-rollout".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-1".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    #[test]
    fn decodes_normalized_agent_identity_path_activity_and_event_lineage() {
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T12:00:00+02:00",
            "payload":{
                "type":"sub_agent_activity",
                "agent_thread_id":"  child-1  ",
                "agent_path":"  /root/storage  ",
                "kind":"interrupted"
            }
        });

        let decoded = decode_agent_record(&state(), 17, &value).unwrap().unwrap();

        assert_eq!(decoded.source_line, 17);
        assert_eq!(decoded.timestamp, "2026-07-25T10:00:00.000000000Z");
        assert_eq!(decoded.transition.last_timestamp, decoded.timestamp);
        assert_eq!(decoded.observation.agent_id.as_deref(), Some("child-1"));
        assert_eq!(
            decoded.observation.agent_path.as_deref(),
            Some("/root/storage")
        );
        assert_eq!(
            decoded.observation.activity,
            ObservedAgentActivity::Interrupted
        );
        assert_eq!(decoded.observation.event.kind, "subagent");
        assert_eq!(
            decoded.observation.event.body.as_deref(),
            Some("  /root/storage  ")
        );
        assert_eq!(
            decoded.observation.event.metadata,
            Some(ProjectedEventMetadata::Subagent(SubagentMetadata {
                agent_thread_id: "child-1".into(),
            }))
        );
    }

    #[test]
    fn missing_agent_identity_still_decodes_an_event_only_observation() {
        let value = serde_json::json!({
            "type":"event_msg",
            "payload":{
                "type":"sub_agent_activity",
                "agent_path":"/root/unidentified",
                "kind":"completed"
            }
        });

        let decoded = decode_agent_record(&state(), 18, &value).unwrap().unwrap();

        assert_eq!(decoded.timestamp, "2026-07-25T09:00:00.000000000Z");
        assert_eq!(decoded.observation.agent_id, None);
        assert_eq!(
            decoded.observation.activity,
            ObservedAgentActivity::Completed
        );
        assert_eq!(decoded.observation.event.metadata, None);
    }

    #[test]
    fn unknown_activity_remains_running_while_the_event_preserves_its_source_label() {
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"sub_agent_activity",
                "agent_thread_id":"child-1",
                "kind":"interacted"
            }
        });

        let decoded = decode_agent_record(&state(), 19, &value).unwrap().unwrap();

        assert_eq!(decoded.observation.activity, ObservedAgentActivity::Running);
        assert_eq!(
            decoded.observation.event.label.as_deref(),
            Some("interacted")
        );
        assert_eq!(
            decoded.observation.event.status.as_deref(),
            Some("interacted")
        );
    }

    #[test]
    fn bounds_agent_row_path_separately_from_the_visible_event_body() {
        let path = format!("/root/{}", "x".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000));
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"sub_agent_activity",
                "agent_thread_id":"child-1",
                "agent_path":path,
                "kind":"started"
            }
        });

        let decoded = decode_agent_record(&state(), 20, &value).unwrap().unwrap();

        assert_eq!(
            decoded
                .observation
                .agent_path
                .as_deref()
                .unwrap()
                .chars()
                .count(),
            PROJECTED_SESSION_PATH_CHARS + 1
        );
        assert_eq!(
            decoded
                .observation
                .event
                .body
                .as_deref()
                .unwrap()
                .chars()
                .count(),
            PROJECTED_EVENT_BODY_CHARS + 1
        );
    }

    #[test]
    fn declines_foreign_records_and_rejects_missing_initial_time() {
        assert!(
            decode_agent_record(
                &state(),
                21,
                &serde_json::json!({
                    "type":"event_msg",
                    "timestamp":"2026-07-25T10:00:00Z",
                    "payload":{"type":"task_started"}
                })
            )
            .unwrap()
            .is_none()
        );

        let mut cursor = state();
        cursor.last_timestamp = None;
        let error = decode_agent_record(
            &cursor,
            22,
            &serde_json::json!({
                "type":"event_msg",
                "payload":{"type":"sub_agent_activity","agent_thread_id":"child-1"}
            }),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source line 22 has no timestamp")
        );
    }
}
