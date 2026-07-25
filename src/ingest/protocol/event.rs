use super::{
    content::redact_and_bound,
    duration::bounded_duration_ms,
    identifiers::{PROJECTED_IDENTIFIER_CHARS, normalized_relational_identifier},
    state::CursorState,
};
use crate::redaction::redact_data_urls;
use anyhow::Result;
use serde_json::{Number, Value};

const UNKNOWN_METADATA_STRING_CHARS: usize = 256;
pub(in crate::ingest) const PROJECTED_EVENT_LABEL_CHARS: usize = 512;
pub(in crate::ingest) const PROJECTED_EVENT_BODY_CHARS: usize = 16 * 1024;
const COMPACTION_SUMMARY_CHARS: usize = 16 * 1024;
const COMPACTION_IDENTIFIER_CHARS: usize = 256;

/// The borrowed source fields needed to shape one durable event projection.
///
/// `payload` is inspected only while constructing [`ProjectedEvent`]. No raw
/// JSON survives across the protocol/projection boundary.
pub(in crate::ingest) struct EventDraft<'a> {
    pub(in crate::ingest) kind: &'a str,
    pub(in crate::ingest) role: Option<&'a str>,
    pub(in crate::ingest) label: Option<&'a str>,
    pub(in crate::ingest) body: Option<&'a str>,
    pub(in crate::ingest) status: Option<&'a str>,
    pub(in crate::ingest) tool_name: Option<&'a str>,
    pub(in crate::ingest) duration_ms: Option<i64>,
    pub(in crate::ingest) payload: &'a Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) struct ProjectedEvent {
    pub(in crate::ingest) kind: String,
    pub(in crate::ingest) role: Option<String>,
    pub(in crate::ingest) label: Option<String>,
    pub(in crate::ingest) body: Option<String>,
    pub(in crate::ingest) status: Option<String>,
    pub(in crate::ingest) tool_name: Option<String>,
    pub(in crate::ingest) call_id: Option<ProjectedCallId>,
    pub(in crate::ingest) duration_ms: Option<i64>,
    pub(in crate::ingest) metadata: Option<ProjectedEventMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) enum ProjectedCallId {
    Source(String),
    Message {
        rollout_id: String,
        source_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) enum ProjectedEventMetadata {
    Compaction(CompactionMetadata),
    Subagent(SubagentMetadata),
    Unknown(UnknownMetadata),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::ingest) struct CompactionMetadata {
    pub(in crate::ingest) replacement_history_count: Option<u64>,
    pub(in crate::ingest) window_number: Option<MetadataScalar>,
    pub(in crate::ingest) first_window_id: Option<MetadataScalar>,
    pub(in crate::ingest) previous_window_id: Option<MetadataScalar>,
    pub(in crate::ingest) window_id: Option<MetadataScalar>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) struct SubagentMetadata {
    pub(in crate::ingest) agent_thread_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::ingest) struct UnknownMetadata {
    pub(in crate::ingest) event_type: Option<MetadataScalar>,
    pub(in crate::ingest) schema_version: Option<MetadataScalar>,
    pub(in crate::ingest) version: Option<MetadataScalar>,
    pub(in crate::ingest) id: Option<MetadataScalar>,
    pub(in crate::ingest) call_id: Option<MetadataScalar>,
    pub(in crate::ingest) status: Option<MetadataScalar>,
}

impl UnknownMetadata {
    pub(in crate::ingest) fn is_empty(&self) -> bool {
        self.event_type.is_none()
            && self.schema_version.is_none()
            && self.version.is_none()
            && self.id.is_none()
            && self.call_id.is_none()
            && self.status.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) enum MetadataScalar {
    Boolean(bool),
    Number(Number),
    String(String),
}

pub(in crate::ingest) fn shape_projected_event(
    state: &CursorState,
    draft: EventDraft<'_>,
) -> Result<ProjectedEvent> {
    let compact_metadata_kind = matches!(draft.kind, "subagent" | "goal" | "plan" | "state");
    let message_payload = draft.payload.get("type").and_then(Value::as_str) == Some("message");
    let raw_call_id = (!compact_metadata_kind)
        .then(|| {
            if message_payload {
                draft.payload.get("id").and_then(Value::as_str)
            } else {
                draft
                    .payload
                    .get("call_id")
                    .or_else(|| draft.payload.get("id"))
                    .and_then(Value::as_str)
            }
        })
        .flatten();
    let call_id =
        normalized_relational_identifier(raw_call_id, "event call id")?.map(|source_id| {
            if message_payload {
                ProjectedCallId::Message {
                    rollout_id: state.owner_id.clone(),
                    source_id,
                }
            } else {
                ProjectedCallId::Source(source_id)
            }
        });

    let compaction = (draft.kind == "compaction").then(|| compact_compaction(draft.payload));
    let normalized_body = if let Some((summary, _)) = compaction.as_ref() {
        Some(summary.as_str())
    } else if matches!(
        draft.kind,
        "message" | "final" | "tool_call" | "tool_output" | "tool_completed"
    ) {
        None
    } else {
        draft.body
    };

    let label = draft.label.map(|value| {
        if compact_metadata_kind {
            redact_and_bound(value, PROJECTED_EVENT_LABEL_CHARS)
        } else {
            redact_data_urls(value)
        }
    });
    let body = normalized_body.map(|value| {
        if compact_metadata_kind {
            redact_and_bound(value, PROJECTED_EVENT_BODY_CHARS)
        } else {
            redact_data_urls(value)
        }
    });
    let status = draft.status.map(|value| {
        if compact_metadata_kind {
            redact_and_bound(value, PROJECTED_IDENTIFIER_CHARS)
        } else {
            redact_data_urls(value)
        }
    });
    let tool_name = draft.tool_name.map(redact_data_urls);

    let metadata = if let Some((_, metadata)) = compaction {
        Some(ProjectedEventMetadata::Compaction(metadata))
    } else if draft.kind == "system" {
        compact_unknown_metadata(draft.payload).map(ProjectedEventMetadata::Unknown)
    } else if draft.kind == "subagent" {
        compact_subagent_metadata(draft.payload)?.map(ProjectedEventMetadata::Subagent)
    } else {
        None
    };

    Ok(ProjectedEvent {
        kind: draft.kind.to_owned(),
        role: draft.role.map(str::to_owned),
        label,
        body,
        status,
        tool_name,
        call_id,
        duration_ms: bounded_duration_ms(draft.duration_ms),
        metadata,
    })
}

fn compact_subagent_metadata(payload: &Value) -> Result<Option<SubagentMetadata>> {
    let Some(agent_thread_id) = normalized_relational_identifier(
        payload.get("agent_thread_id").and_then(Value::as_str),
        "subagent thread id",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(SubagentMetadata { agent_thread_id }))
}

fn compact_unknown_metadata(payload: &Value) -> Option<UnknownMetadata> {
    let payload = payload.as_object()?;
    let metadata = UnknownMetadata {
        event_type: payload.get("type").and_then(unknown_metadata_scalar),
        schema_version: payload
            .get("schema_version")
            .and_then(unknown_metadata_scalar),
        version: payload.get("version").and_then(unknown_metadata_scalar),
        id: payload.get("id").and_then(unknown_metadata_scalar),
        call_id: payload.get("call_id").and_then(unknown_metadata_scalar),
        status: payload.get("status").and_then(unknown_metadata_scalar),
    };
    (!metadata.is_empty()).then_some(metadata)
}

fn unknown_metadata_scalar(value: &Value) -> Option<MetadataScalar> {
    match value {
        Value::Bool(value) => Some(MetadataScalar::Boolean(*value)),
        Value::Number(value) => Some(MetadataScalar::Number(value.clone())),
        Value::String(value) => Some(MetadataScalar::String(
            redact_data_urls(value)
                .chars()
                .take(UNKNOWN_METADATA_STRING_CHARS)
                .collect(),
        )),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn compact_compaction(payload: &Value) -> (String, CompactionMetadata) {
    let summary = payload
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Conversation context was compacted.");
    let mut summary_chars = summary.chars();
    let mut summary = summary_chars
        .by_ref()
        .take(COMPACTION_SUMMARY_CHARS)
        .collect::<String>();
    if summary_chars.next().is_some() {
        summary.push('…');
    }
    let summary = redact_data_urls(&summary);

    let metadata = CompactionMetadata {
        replacement_history_count: payload
            .get("replacement_history")
            .and_then(Value::as_array)
            .map(|values| values.len() as u64),
        window_number: payload
            .get("window_number")
            .and_then(compaction_metadata_scalar),
        first_window_id: payload
            .get("first_window_id")
            .and_then(compaction_metadata_scalar),
        previous_window_id: payload
            .get("previous_window_id")
            .and_then(compaction_metadata_scalar),
        window_id: payload
            .get("window_id")
            .and_then(compaction_metadata_scalar),
    };
    (summary, metadata)
}

fn compaction_metadata_scalar(value: &Value) -> Option<MetadataScalar> {
    match value {
        Value::Bool(value) => Some(MetadataScalar::Boolean(*value)),
        Value::Number(value) => Some(MetadataScalar::Number(value.clone())),
        Value::String(value) => {
            let compact = value
                .chars()
                .take(COMPACTION_IDENTIFIER_CHARS)
                .collect::<String>();
            Some(MetadataScalar::String(redact_data_urls(&compact)))
        }
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            ..CursorState::default()
        }
    }

    #[test]
    fn plan_shaping_bounds_visible_fields_and_discards_source_metadata() {
        let label = format!("{}data:image/png;base64,PRIVATE", "l".repeat(510));
        let body = "b".repeat(PROJECTED_EVENT_BODY_CHARS + 1);
        let status = "s".repeat(PROJECTED_IDENTIFIER_CHARS + 1);
        let payload = serde_json::json!({
            "type":"item_completed",
            "id":"source-event-id",
            "secret":"must not survive"
        });
        let event = shape_projected_event(
            &state(),
            EventDraft {
                kind: "plan",
                role: None,
                label: Some(&label),
                body: Some(&body),
                status: Some(&status),
                tool_name: None,
                duration_ms: None,
                payload: &payload,
            },
        )
        .unwrap();

        assert!(!event.label.as_ref().unwrap().contains("PRIVATE"));
        assert_eq!(event.label.as_ref().unwrap().chars().count(), 513);
        assert_eq!(
            event.body.unwrap().chars().count(),
            PROJECTED_EVENT_BODY_CHARS + 1
        );
        assert_eq!(
            event.status.unwrap().chars().count(),
            PROJECTED_IDENTIFIER_CHARS + 1
        );
        assert_eq!(event.call_id, None);
        assert_eq!(event.metadata, None);
    }

    #[test]
    fn compaction_shaping_keeps_only_summary_and_bounded_typed_metadata() {
        let long_window = format!("{}data:image/png;base64,PRIVATE", "w".repeat(250));
        let payload = serde_json::json!({
            "message":"  Summary data:image/png;base64,SECRET  ",
            "replacement_history":[{"large":"payload"}, 2, 3],
            "window_number":7,
            "first_window_id":true,
            "previous_window_id":{"ignored":"object"},
            "window_id":long_window,
            "unrelated":"must not survive"
        });
        let event = shape_projected_event(
            &state(),
            EventDraft {
                kind: "compaction",
                role: None,
                label: Some("Context compacted"),
                body: Some("ignored body"),
                status: None,
                tool_name: None,
                duration_ms: None,
                payload: &payload,
            },
        )
        .unwrap();

        assert_eq!(event.body.as_deref(), Some("Summary [embedded attachment]"));
        assert_eq!(
            event.metadata,
            Some(ProjectedEventMetadata::Compaction(CompactionMetadata {
                replacement_history_count: Some(3),
                window_number: Some(MetadataScalar::Number(Number::from(7))),
                first_window_id: Some(MetadataScalar::Boolean(true)),
                previous_window_id: None,
                window_id: Some(MetadataScalar::String("w".repeat(250) + "data:i")),
            }))
        );
    }

    #[test]
    fn unknown_system_shaping_keeps_only_the_scalar_allowlist() {
        let payload = serde_json::json!({
            "type":"future_event",
            "schema_version":3,
            "version":false,
            "id":"data:image/png;base64,PRIVATE",
            "call_id":["ignored"],
            "status":"s".repeat(300),
            "body":"must not survive"
        });
        let event = shape_projected_event(
            &state(),
            EventDraft {
                kind: "system",
                role: None,
                label: Some("future_event"),
                body: None,
                status: None,
                tool_name: None,
                duration_ms: None,
                payload: &payload,
            },
        )
        .unwrap();

        assert_eq!(event.call_id, None);
        assert_eq!(
            event.metadata,
            Some(ProjectedEventMetadata::Unknown(UnknownMetadata {
                event_type: Some(MetadataScalar::String("future_event".into())),
                schema_version: Some(MetadataScalar::Number(Number::from(3))),
                version: Some(MetadataScalar::Boolean(false)),
                id: Some(MetadataScalar::String("[embedded attachment]".into())),
                call_id: None,
                status: Some(MetadataScalar::String("s".repeat(256))),
            }))
        );
    }

    #[test]
    fn subagent_shaping_bounds_display_fields_and_retains_only_valid_thread_identity() {
        let payload = serde_json::json!({
            "id":"ignored-as-call-id",
            "agent_thread_id":"  child-thread  ",
            "details":"must not survive"
        });
        let event = shape_projected_event(
            &state(),
            EventDraft {
                kind: "subagent",
                role: None,
                label: Some("spawn data:image/png;base64,PRIVATE"),
                body: Some("/root/child"),
                status: Some("started"),
                tool_name: None,
                duration_ms: None,
                payload: &payload,
            },
        )
        .unwrap();

        assert_eq!(event.label.as_deref(), Some("spawn [embedded attachment]"));
        assert_eq!(event.body.as_deref(), Some("/root/child"));
        assert_eq!(event.status.as_deref(), Some("started"));
        assert_eq!(event.call_id, None);
        assert_eq!(
            event.metadata,
            Some(ProjectedEventMetadata::Subagent(SubagentMetadata {
                agent_thread_id: "child-thread".into(),
            }))
        );
    }
}
