use super::super::{
    content::{compact_title, redact_and_bound},
    event::{EventDraft, PROJECTED_EVENT_BODY_CHARS, ProjectedEvent, shape_projected_event},
    metadata::{SessionMetadata, normalized_session_metadata},
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use anyhow::{Result, anyhow};
use serde_json::Value;

/// One database-independent metadata or thread-title record.
///
/// Source JSON is borrowed only during decoding. The durable intent contains
/// normalized metadata fields and a shaped Activity event, never the source
/// object itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedMetadataRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: MetadataStateTransition,
    pub(in crate::ingest) intent: MetadataIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct MetadataStateTransition {
    pub(in crate::ingest) last_timestamp: String,
}

impl MetadataStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum MetadataIntent {
    Owner(Box<MetadataUpdate>),
    RootUserTitle(Option<String>),
    ThreadName {
        title: Option<String>,
        event: Option<Box<ProjectedEvent>>,
    },
    IgnoredSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct MetadataUpdate {
    pub(in crate::ingest) is_root: bool,
    pub(in crate::ingest) fields: SessionMetadata,
    pub(in crate::ingest) title: Option<String>,
}

/// Decode only the early-admission session metadata family.
///
/// This split is intentional: session metadata is admitted before the native
/// record gate, while title events retain their later post-native routing
/// slot. Keeping those entry points separate makes that order explicit for the
/// caller instead of relying on Projection to rediscover admission policy.
pub(in crate::ingest) fn decode_session_metadata_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedMetadataRecord>> {
    let wire = WireRecord::new(value);
    let legacy = wire.outer_type().is_none()
        && value.get("id").and_then(Value::as_str) == Some(state.owner_id.as_str());
    let source = match wire.outer_type() {
        Some("session_meta") => wire.payload().unwrap_or(&Value::Null),
        None if legacy => value,
        _ => return Ok(None),
    };
    let timestamp = record_timestamp(state, line, &wire)?;
    let intent = if source.get("id").and_then(Value::as_str) == Some(state.owner_id.as_str()) {
        let is_root = state.owner_id == state.thread_id;
        MetadataIntent::Owner(Box::new(MetadataUpdate {
            is_root,
            fields: normalized_session_metadata(source),
            title: is_root
                .then(|| normalized_title(source.get("thread_name")))
                .flatten(),
        }))
    } else {
        // Canonical session_meta records for another owner were historically
        // consumed (and advanced the cursor) without changing this owner.
        MetadataIntent::IgnoredSession
    };
    Ok(Some(record(line, timestamp, intent)))
}

/// Decode post-native title-bearing event records.
pub(in crate::ingest) fn decode_title_event_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedMetadataRecord>> {
    let wire = WireRecord::new(value);
    if wire.outer_type() != Some("event_msg") {
        return Ok(None);
    }
    let source = wire.payload().unwrap_or(&Value::Null);
    let timestamp = record_timestamp(state, line, &wire)?;
    let intent = match wire.payload_type() {
        Some("user_message") => MetadataIntent::RootUserTitle(
            (state.owner_id == state.thread_id)
                .then(|| {
                    source
                        .get("message")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(compact_title)
                })
                .flatten(),
        ),
        Some("thread_name_updated") => {
            let title = normalized_title(source.get("thread_name").or_else(|| source.get("name")));
            let event = title
                .as_deref()
                .map(|title| {
                    shape_projected_event(
                        state,
                        EventDraft {
                            kind: "state",
                            role: None,
                            label: Some("Thread renamed"),
                            body: Some(title),
                            status: None,
                            tool_name: None,
                            duration_ms: None,
                            payload: source,
                        },
                    )
                })
                .transpose()?
                .map(Box::new);
            MetadataIntent::ThreadName { title, event }
        }
        _ => return Ok(None),
    };
    Ok(Some(record(line, timestamp, intent)))
}

fn record(line: u64, timestamp: String, intent: MetadataIntent) -> DecodedMetadataRecord {
    DecodedMetadataRecord {
        source_line: line,
        transition: MetadataStateTransition {
            last_timestamp: timestamp.clone(),
        },
        timestamp,
        intent,
    }
}

fn record_timestamp(state: &CursorState, line: u64, wire: &WireRecord<'_>) -> Result<String> {
    match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp),
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp")),
    }
}

fn normalized_title(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| redact_and_bound(value, PROJECTED_EVENT_BODY_CHARS))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(owner: &str, thread: &str) -> CursorState {
        CursorState {
            owner_id: owner.into(),
            thread_id: thread.into(),
            native_started: true,
            ..CursorState::default()
        }
    }

    #[test]
    fn owner_metadata_is_normalized_and_omitted_root_fields_remain_explicitly_empty() {
        let state = state("thread-1", "thread-1");
        let decoded = decode_session_metadata_record(
            &state,
            7,
            &serde_json::json!({
                "timestamp":"2026-07-25T10:00:00+02:00",
                "type":"session_meta",
                "payload":{
                    "id":"thread-1",
                    "session_id":"thread-1",
                    "thread_name":"  Named session  "
                }
            }),
        )
        .unwrap()
        .unwrap();

        assert_eq!(decoded.timestamp, "2026-07-25T08:00:00.000000000Z");
        let MetadataIntent::Owner(update) = decoded.intent else {
            panic!("expected owner metadata")
        };
        assert!(update.is_root);
        assert_eq!(update.title.as_deref(), Some("Named session"));
        assert_eq!(update.fields, SessionMetadata::default());
    }

    #[test]
    fn child_title_candidates_carry_no_title_authority() {
        let state = state("child-1", "thread-1");
        let decoded = decode_title_event_record(
            &state,
            8,
            &serde_json::json!({
                "timestamp":"2026-07-25T08:00:01Z",
                "type":"event_msg",
                "payload":{"type":"user_message","message":"Child prompt"}
            }),
        )
        .unwrap()
        .unwrap();
        assert_eq!(decoded.intent, MetadataIntent::RootUserTitle(None));
    }

    #[test]
    fn thread_rename_is_bounded_and_shaped_without_retaining_source_json() {
        let state = state("thread-1", "thread-1");
        let decoded = decode_title_event_record(
            &state,
            9,
            &serde_json::json!({
                "timestamp":"2026-07-25T08:00:02Z",
                "type":"event_msg",
                "payload":{
                    "type":"thread_name_updated",
                    "thread_name":"Before data:image/png;base64,private after"
                }
            }),
        )
        .unwrap()
        .unwrap();
        let MetadataIntent::ThreadName { title, event } = decoded.intent else {
            panic!("expected rename")
        };
        assert_eq!(title.as_deref(), Some("Before [embedded attachment] after"));
        assert_eq!(event.unwrap().body, title);
    }
}
