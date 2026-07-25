use super::super::{
    content::value_to_text,
    duration::raw_duration_ms,
    event::{EventDraft, ProjectedEvent, shape_projected_event},
    identifiers::normalized_relational_identifier,
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use crate::redaction::redact_data_urls;
use anyhow::{Result, anyhow};
use serde_json::Value;

/// One database-independent native turn lifecycle record.
///
/// The source JSON is borrowed only while this value is decoded. Projection
/// receives normalized identifiers, bounded/redacted event fields, and the
/// exact scalar lifecycle facts it needs; no raw payload crosses the seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedLifecycleRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: LifecycleStateTransition,
    pub(in crate::ingest) intent: LifecycleIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct LifecycleStateTransition {
    pub(in crate::ingest) last_timestamp: String,
    pub(in crate::ingest) native_started: bool,
    pub(in crate::ingest) current_turn: Option<String>,
    pub(in crate::ingest) turn_context_seen: bool,
}

impl LifecycleStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
        state.native_started = self.native_started;
        state.current_turn.clone_from(&self.current_turn);
        state.turn_context_seen = self.turn_context_seen;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum LifecycleIntent {
    TaskStarted(Box<TaskStarted>),
    TaskComplete(Box<TaskComplete>),
    Terminal(Box<TerminalLifecycle>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct TaskStarted {
    pub(in crate::ingest) previous_turn: Option<String>,
    pub(in crate::ingest) event: ProjectedEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct TaskComplete {
    pub(in crate::ingest) last_agent_message: Option<String>,
    pub(in crate::ingest) duration_ms: Option<i64>,
    pub(in crate::ingest) time_to_first_token_ms: Option<i64>,
    pub(in crate::ingest) event: ProjectedEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum TerminalLifecycleKind {
    Aborted,
    RolledBack,
}

impl TerminalLifecycleKind {
    pub(in crate::ingest) fn source_label(self) -> &'static str {
        match self {
            Self::Aborted => "turn_aborted",
            Self::RolledBack => "thread_rolled_back",
        }
    }

    pub(in crate::ingest) fn status(self) -> &'static str {
        match self {
            Self::Aborted => "interrupted",
            Self::RolledBack => "rolled_back",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct TerminalLifecycle {
    pub(in crate::ingest) kind: TerminalLifecycleKind,
    pub(in crate::ingest) event: ProjectedEvent,
}

pub(in crate::ingest) fn decode_lifecycle_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedLifecycleRecord>> {
    let wire = WireRecord::new(value);
    if wire.outer_type() != Some("event_msg") {
        return Ok(None);
    }
    let kind = match wire.payload_type() {
        Some("task_started") => LifecycleFamily::TaskStarted,
        Some("task_complete") => LifecycleFamily::TaskComplete,
        Some("turn_aborted") => LifecycleFamily::Terminal(TerminalLifecycleKind::Aborted),
        Some("thread_rolled_back") => LifecycleFamily::Terminal(TerminalLifecycleKind::RolledBack),
        _ => return Ok(None),
    };
    let payload = wire.payload().unwrap_or(&Value::Null);
    let timestamp = match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp)?,
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp"))?,
    };
    let explicit_turn = normalized_relational_identifier(
        payload.get("turn_id").and_then(Value::as_str),
        "turn id",
    )?;
    let current_turn = explicit_turn.clone().or_else(|| state.current_turn.clone());
    let mut transition = LifecycleStateTransition {
        last_timestamp: timestamp.clone(),
        native_started: state.native_started,
        current_turn,
        turn_context_seen: state.turn_context_seen,
    };

    let intent = match kind {
        LifecycleFamily::TaskStarted => {
            transition.turn_context_seen = false;
            let event = shape_projected_event(
                state,
                EventDraft {
                    kind: "turn_started",
                    role: None,
                    label: Some("Turn started"),
                    body: None,
                    status: Some("running"),
                    tool_name: None,
                    duration_ms: None,
                    payload,
                },
            )?;
            LifecycleIntent::TaskStarted(Box::new(TaskStarted {
                previous_turn: state.current_turn.clone(),
                event,
            }))
        }
        LifecycleFamily::TaskComplete => {
            let last_agent_message = payload
                .get("last_agent_message")
                .and_then(Value::as_str)
                .map(redact_data_urls);
            let duration_ms = raw_duration_ms(payload.get("duration_ms"));
            let time_to_first_token_ms = payload
                .get("time_to_first_token_ms")
                .and_then(Value::as_i64);
            let event = shape_projected_event(
                state,
                EventDraft {
                    kind: "turn_completed",
                    role: None,
                    label: Some("Turn completed"),
                    body: last_agent_message.as_deref(),
                    status: Some("completed"),
                    tool_name: None,
                    duration_ms,
                    payload,
                },
            )?;
            LifecycleIntent::TaskComplete(Box::new(TaskComplete {
                last_agent_message,
                duration_ms,
                time_to_first_token_ms,
                event,
            }))
        }
        LifecycleFamily::Terminal(kind) => {
            let body = value_to_text(payload);
            let event = shape_projected_event(
                state,
                EventDraft {
                    kind: "state",
                    role: None,
                    label: Some(kind.source_label()),
                    body: body.as_deref(),
                    status: Some(kind.status()),
                    tool_name: None,
                    duration_ms: None,
                    payload,
                },
            )?;
            LifecycleIntent::Terminal(Box::new(TerminalLifecycle { kind, event }))
        }
    };

    Ok(Some(DecodedLifecycleRecord {
        source_line: line,
        timestamp,
        transition,
        intent,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleFamily {
    TaskStarted,
    TaskComplete,
    Terminal(TerminalLifecycleKind),
}

#[cfg(test)]
mod tests {
    use super::super::super::{
        duration::MAX_STORED_DURATION_MS, event::PROJECTED_EVENT_BODY_CHARS,
    };
    use super::*;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-1".into()),
            turn_context_seen: true,
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    #[test]
    fn task_started_normalizes_turn_and_carries_the_previous_cursor_fact() {
        let decoded = decode_lifecycle_record(
            &state(),
            7,
            &serde_json::json!({
                "timestamp":"2026-07-25T10:00:01+02:00",
                "type":"event_msg",
                "payload":{"type":"task_started","turn_id":"  turn-2  "}
            }),
        )
        .unwrap()
        .unwrap();

        assert_eq!(decoded.timestamp, "2026-07-25T08:00:01.000000000Z");
        assert_eq!(decoded.transition.current_turn.as_deref(), Some("turn-2"));
        assert!(!decoded.transition.turn_context_seen);
        let LifecycleIntent::TaskStarted(started) = decoded.intent else {
            panic!("expected task start");
        };
        assert_eq!(started.previous_turn.as_deref(), Some("turn-1"));
        assert_eq!(started.event.kind, "turn_started");
        assert_eq!(started.event.status.as_deref(), Some("running"));
    }

    #[test]
    fn completion_redacts_message_bounds_duration_and_keeps_raw_integer_ttft() {
        let decoded = decode_lifecycle_record(
            &state(),
            8,
            &serde_json::json!({
                "type":"event_msg",
                "payload":{
                    "type":"task_complete",
                    "last_agent_message":"done data:image/png;base64,PRIVATE",
                    "duration_ms":MAX_STORED_DURATION_MS + 1,
                    "time_to_first_token_ms":-17
                }
            }),
        )
        .unwrap()
        .unwrap();

        assert_eq!(decoded.timestamp, "2026-07-25T09:00:00.000000000Z");
        let LifecycleIntent::TaskComplete(complete) = decoded.intent else {
            panic!("expected task completion");
        };
        assert_eq!(
            complete.last_agent_message.as_deref(),
            Some("done [embedded attachment]")
        );
        assert_eq!(complete.duration_ms, None);
        assert_eq!(complete.time_to_first_token_ms, Some(-17));
        assert_eq!(
            complete.event.body.as_deref(),
            Some("done [embedded attachment]")
        );
    }

    #[test]
    fn terminal_events_cross_the_boundary_redacted_and_bounded() {
        let reason = format!("{} data:image/png;base64,PRIVATE", "x".repeat(20_000));
        let decoded = decode_lifecycle_record(
            &state(),
            9,
            &serde_json::json!({
                "timestamp":"2026-07-25T10:00:03Z",
                "type":"event_msg",
                "payload":{"type":"thread_rolled_back","reason":reason}
            }),
        )
        .unwrap()
        .unwrap();

        let LifecycleIntent::Terminal(terminal) = decoded.intent else {
            panic!("expected terminal lifecycle");
        };
        assert_eq!(terminal.kind, TerminalLifecycleKind::RolledBack);
        assert_eq!(terminal.event.status.as_deref(), Some("rolled_back"));
        let body = terminal.event.body.unwrap();
        assert!(body.chars().count() <= PROJECTED_EVENT_BODY_CHARS + 1);
        assert!(!body.contains("base64,PRIVATE"));
    }

    #[test]
    fn invalid_explicit_turn_is_rejected_before_projection() {
        let error = decode_lifecycle_record(
            &state(),
            10,
            &serde_json::json!({
                "timestamp":"2026-07-25T10:00:03Z",
                "type":"event_msg",
                "payload":{"type":"turn_aborted","turn_id":"x".repeat(257)}
            }),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("turn id exceeds the 256-character")
        );
    }
}
