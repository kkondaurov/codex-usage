use super::super::{
    content::{
        compact_title, extract_content, has_omitted_attachment, is_transport_context_envelope,
        is_turn_abort_envelope, redact_and_bound,
    },
    event::{
        EventDraft, PROJECTED_EVENT_BODY_CHARS, ProjectedCallId, ProjectedEvent,
        shape_projected_event,
    },
    identifiers::normalized_relational_identifier,
    state::CursorState,
    timestamp::canonical_source_timestamp,
    wire::WireRecord,
};
use crate::redaction::redact_data_urls;
use anyhow::{Result, anyhow};
use serde_json::Value;

/// One database-independent conversation record.
///
/// Source JSON is borrowed only while this value is built. The owned intent
/// retains durable conversation text and bounded identifiers, never the raw
/// response/event payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedConversationRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: ConversationStateTransition,
    pub(in crate::ingest) intent: ConversationIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ConversationStateTransition {
    pub(in crate::ingest) last_timestamp: String,
    pub(in crate::ingest) current_turn: Option<String>,
}

impl ConversationStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
        state.current_turn.clone_from(&self.current_turn);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum ConversationIntent {
    Message(Box<MessageIntent>),
    CanonicalReasoning(Box<CanonicalReasoningIntent>),
    SubagentMessage(Box<ProjectedEvent>),
    LegacyReasoning(Box<ProjectedEvent>),
    LegacyAssistantUpdate(Box<ProjectedEvent>),
    Noop(ConversationNoop),
}

/// Canonical reasoning plus the one bounded fact needed to reproduce the
/// legacy reconciliation predicate without retaining its raw source text.
///
/// The stable projector compared the unredacted extracted body with an
/// already-projected legacy body. Body equality can therefore participate in
/// reconciliation only when projecting the canonical body changed nothing;
/// near-timestamp reconciliation remains independent of this flag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct CanonicalReasoningIntent {
    pub(in crate::ingest) event: ProjectedEvent,
    pub(in crate::ingest) body_matches_projected_form: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum ConversationNoop {
    EmptyOrUnsupportedMessage,
    TurnAbortEnvelope,
    TransportContextEnvelope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    pub(in crate::ingest) fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum MessageActivity {
    Message,
    Update,
    Final,
}

impl MessageActivity {
    pub(in crate::ingest) fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Update => "update",
            Self::Final => "final",
        }
    }
}

/// Message IDs are validated by Protocol but their error is intentionally
/// deferred until Projection admits the message. A metadata-free user record
/// can be ignored by the native-turn gate; the legacy implementation never
/// rejected an invalid ID on that ignored path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum DeferredMessageId {
    Valid(Option<String>),
    Invalid(String),
}

impl DeferredMessageId {
    pub(in crate::ingest) fn resolve(&self) -> Result<Option<&str>> {
        match self {
            Self::Valid(value) => Ok(value.as_deref()),
            Self::Invalid(error) => Err(anyhow!(error.clone())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct MessageIntent {
    pub(in crate::ingest) role: MessageRole,
    pub(in crate::ingest) activity: MessageActivity,
    pub(in crate::ingest) content: String,
    pub(in crate::ingest) source_id: DeferredMessageId,
    pub(in crate::ingest) has_explicit_turn: bool,
    pub(in crate::ingest) allow_implicit_turn: bool,
    pub(in crate::ingest) title_fallback: Option<String>,
}

pub(in crate::ingest) fn decode_conversation_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedConversationRecord>> {
    let wire = WireRecord::new(value);
    let family = match wire.outer_type() {
        Some("response_item") => {
            let payload = wire.payload().unwrap_or(&Value::Null);
            response_family(payload, false)
        }
        Some("message" | "agent_message" | "reasoning") => response_family(value, true),
        Some("event_msg") => {
            let payload = wire.payload().unwrap_or(&Value::Null);
            match wire.payload_type() {
                Some("agent_reasoning") => Some(ConversationFamily::LegacyReasoning(payload)),
                Some("agent_message") => Some(ConversationFamily::LegacyAssistantUpdate(payload)),
                _ => None,
            }
        }
        _ => None,
    };
    let Some(family) = family else {
        return Ok(None);
    };

    let timestamp = match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp)?,
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp"))?,
    };
    let mut transition = ConversationStateTransition {
        last_timestamp: timestamp.clone(),
        current_turn: state.current_turn.clone(),
    };

    let intent = match family {
        ConversationFamily::Message {
            payload,
            allow_implicit_turn,
        } => decode_message(state, payload, allow_implicit_turn, &mut transition)?,
        ConversationFamily::CanonicalReasoning(payload) => {
            apply_explicit_turn(payload, &mut transition)?;
            let body = extract_content(
                payload
                    .get("summary")
                    .or_else(|| payload.get("content"))
                    .unwrap_or(&Value::Null),
            );
            if body.is_empty() {
                ConversationIntent::Noop(ConversationNoop::EmptyOrUnsupportedMessage)
            } else {
                let event = shape_projected_event(
                    state,
                    EventDraft {
                        kind: "reasoning",
                        role: Some("assistant"),
                        label: Some("Reasoning summary"),
                        body: Some(&body),
                        status: None,
                        tool_name: None,
                        duration_ms: None,
                        payload,
                    },
                )?;
                let body_matches_projected_form = event.body.as_deref() == Some(body.as_str());
                ConversationIntent::CanonicalReasoning(Box::new(CanonicalReasoningIntent {
                    event,
                    body_matches_projected_form,
                }))
            }
        }
        ConversationFamily::SubagentMessage(payload) => {
            apply_explicit_turn(payload, &mut transition)?;
            let body = extract_content(
                payload
                    .get("content")
                    .or_else(|| payload.get("message"))
                    .unwrap_or(&Value::Null),
            );
            let author = payload
                .get("author")
                .and_then(Value::as_str)
                .unwrap_or("agent");
            let recipient = payload
                .get("recipient")
                .and_then(Value::as_str)
                .unwrap_or("agent");
            let label = format!("{author} → {recipient}");
            ConversationIntent::SubagentMessage(Box::new(shape_projected_event(
                state,
                EventDraft {
                    kind: "subagent",
                    role: None,
                    label: Some(&label),
                    body: (!body.is_empty()).then_some(body.as_str()),
                    status: None,
                    tool_name: None,
                    duration_ms: None,
                    payload,
                },
            )?))
        }
        ConversationFamily::LegacyReasoning(payload) => {
            let body = payload
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| redact_and_bound(value, PROJECTED_EVENT_BODY_CHARS));
            match body {
                Some(body) => ConversationIntent::LegacyReasoning(Box::new(shape_projected_event(
                    state,
                    EventDraft {
                        kind: "reasoning",
                        role: Some("assistant"),
                        label: Some("Reasoning"),
                        body: Some(&body),
                        status: None,
                        tool_name: None,
                        duration_ms: None,
                        payload,
                    },
                )?)),
                None => ConversationIntent::Noop(ConversationNoop::EmptyOrUnsupportedMessage),
            }
        }
        ConversationFamily::LegacyAssistantUpdate(payload) => {
            match payload.get("message").and_then(Value::as_str) {
                Some(body) => {
                    let body = redact_data_urls(body);
                    ConversationIntent::LegacyAssistantUpdate(Box::new(shape_projected_event(
                        state,
                        EventDraft {
                            kind: "update",
                            role: Some("assistant"),
                            label: Some("Assistant update"),
                            body: Some(&body),
                            status: None,
                            tool_name: None,
                            duration_ms: None,
                            payload,
                        },
                    )?))
                }
                None => ConversationIntent::Noop(ConversationNoop::EmptyOrUnsupportedMessage),
            }
        }
    };

    Ok(Some(DecodedConversationRecord {
        source_line: line,
        timestamp,
        transition,
        intent,
    }))
}

fn response_family(payload: &Value, allow_implicit_turn: bool) -> Option<ConversationFamily<'_>> {
    match payload.get("type").and_then(Value::as_str) {
        Some("message") => Some(ConversationFamily::Message {
            payload,
            allow_implicit_turn,
        }),
        Some("reasoning") => Some(ConversationFamily::CanonicalReasoning(payload)),
        Some("agent_message") => Some(ConversationFamily::SubagentMessage(payload)),
        _ => None,
    }
}

fn decode_message(
    state: &CursorState,
    payload: &Value,
    allow_implicit_turn: bool,
    transition: &mut ConversationStateTransition,
) -> Result<ConversationIntent> {
    let explicit_turn = apply_explicit_turn(payload, transition)?;
    let role = match payload
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("assistant")
    {
        "user" => MessageRole::User,
        "assistant" => MessageRole::Assistant,
        _ => {
            return Ok(ConversationIntent::Noop(
                ConversationNoop::EmptyOrUnsupportedMessage,
            ));
        }
    };
    let source_content = payload.get("content").unwrap_or(&Value::Null);
    let mut content = redact_data_urls(&extract_content(source_content));
    if content.is_empty() && has_omitted_attachment(source_content) {
        content = "[Attachment omitted]".to_owned();
    }
    if content.is_empty() {
        return Ok(ConversationIntent::Noop(
            ConversationNoop::EmptyOrUnsupportedMessage,
        ));
    }
    if role == MessageRole::User && is_turn_abort_envelope(&content) {
        return Ok(ConversationIntent::Noop(
            ConversationNoop::TurnAbortEnvelope,
        ));
    }
    if role == MessageRole::User && is_transport_context_envelope(&content) {
        return Ok(ConversationIntent::Noop(
            ConversationNoop::TransportContextEnvelope,
        ));
    }

    let source_id = match normalized_relational_identifier(
        payload.get("id").and_then(Value::as_str),
        "message id",
    ) {
        Ok(value) => DeferredMessageId::Valid(value),
        Err(error) => DeferredMessageId::Invalid(error.to_string()),
    };
    let activity = match role {
        MessageRole::User => MessageActivity::Message,
        MessageRole::Assistant
            if payload.get("phase").and_then(Value::as_str) == Some("commentary") =>
        {
            MessageActivity::Update
        }
        MessageRole::Assistant => MessageActivity::Final,
    };
    let title_fallback =
        (role == MessageRole::User && allow_implicit_turn && state.owner_id == state.thread_id)
            .then(|| compact_title(&content));

    Ok(ConversationIntent::Message(Box::new(MessageIntent {
        role,
        activity,
        content,
        source_id,
        has_explicit_turn: explicit_turn,
        allow_implicit_turn,
        title_fallback,
    })))
}

fn apply_explicit_turn(
    payload: &Value,
    transition: &mut ConversationStateTransition,
) -> Result<bool> {
    let explicit_turn = normalized_relational_identifier(
        payload
            .get("internal_chat_message_metadata_passthrough")
            .and_then(|value| value.get("turn_id"))
            .and_then(Value::as_str),
        "turn id",
    )?;
    if let Some(turn_id) = explicit_turn.as_ref() {
        transition.current_turn = Some(turn_id.clone());
    }
    Ok(explicit_turn.is_some())
}

enum ConversationFamily<'a> {
    Message {
        payload: &'a Value,
        allow_implicit_turn: bool,
    },
    CanonicalReasoning(&'a Value),
    SubagentMessage(&'a Value),
    LegacyReasoning(&'a Value),
    LegacyAssistantUpdate(&'a Value),
}

pub(in crate::ingest) fn message_event(
    state: &CursorState,
    message: &MessageIntent,
    source_id: Option<&str>,
) -> ProjectedEvent {
    ProjectedEvent {
        kind: message.activity.as_str().to_owned(),
        role: Some(message.role.as_str().to_owned()),
        label: None,
        body: (message.activity == MessageActivity::Update).then(|| message.content.clone()),
        status: None,
        tool_name: None,
        call_id: source_id.map(|source_id| ProjectedCallId::Message {
            rollout_id: state.owner_id.clone(),
            source_id: source_id.to_owned(),
        }),
        duration_ms: None,
        metadata: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-old".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn decode(value: Value) -> DecodedConversationRecord {
        decode_conversation_record(&state(), 17, &value)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn canonical_message_is_pure_redacted_typed_data_with_explicit_turn_state() {
        let decoded = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T12:15:30.125+02:30",
            "payload":{
                "type":"message",
                "id":"message-1",
                "role":"assistant",
                "phase":"final_answer",
                "content":[{"type":"output_text","text":"done data:image/png;base64,SECRET"}],
                "internal_chat_message_metadata_passthrough":{"turn_id":" turn-new "},
                "transport_blob":{"secret":"must not cross"}
            }
        }));

        assert_eq!(decoded.timestamp, "2026-07-25T09:45:30.125000000Z");
        assert_eq!(decoded.transition.current_turn.as_deref(), Some("turn-new"));
        let ConversationIntent::Message(message) = decoded.intent else {
            panic!("message record must decode to a typed message");
        };
        assert_eq!(message.role, MessageRole::Assistant);
        assert_eq!(message.activity, MessageActivity::Final);
        assert_eq!(message.content, "done [embedded attachment]");
        assert_eq!(
            message.source_id,
            DeferredMessageId::Valid(Some("message-1".into()))
        );
        assert!(message.has_explicit_turn);
        assert!(!message.allow_implicit_turn);
        assert_eq!(message.title_fallback, None);
    }

    #[test]
    fn top_level_prompt_carries_only_its_legacy_title_fallback() {
        let mut root_state = state();
        root_state.thread_id = root_state.owner_id.clone();
        let top_level = decode_conversation_record(
            &root_state,
            17,
            &serde_json::json!({
            "type":"message",
            "timestamp":"2026-07-25T10:00:00Z",
            "role":"user",
            "content":[{"type":"input_text","text":"  First prompt  "}]
            }),
        )
        .unwrap()
        .unwrap();
        let ConversationIntent::Message(message) = top_level.intent else {
            panic!("top-level message must decode as a message");
        };
        assert!(message.allow_implicit_turn);
        assert_eq!(message.title_fallback.as_deref(), Some("First prompt"));
    }

    #[test]
    fn filtered_envelopes_and_late_identifier_errors_are_explicit_no_raw_intents() {
        let filtered = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"message","role":"user",
                "content":[{"type":"input_text","text":"<turn_aborted>x</turn_aborted>"}]
            }
        }));
        assert_eq!(
            filtered.intent,
            ConversationIntent::Noop(ConversationNoop::TurnAbortEnvelope)
        );

        let oversized = "x".repeat(257);
        let invalid_id = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"message","id":oversized,"role":"user",
                "content":[{"type":"input_text","text":"feedback"}]
            }
        }));
        let ConversationIntent::Message(message) = invalid_id.intent else {
            panic!("message should retain a deferred identifier result");
        };
        assert_eq!(
            message.source_id,
            DeferredMessageId::Invalid(
                "message id exceeds the 256-character identifier limit".into()
            )
        );
    }

    #[test]
    fn reasoning_updates_and_subagent_messages_are_bounded_typed_events() {
        let canonical = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"reasoning","id":"reason-1",
                "summary":[{"type":"summary_text","text":"Inspect first."}],
                "ignored":{"huge":"payload"}
            }
        }));
        let ConversationIntent::CanonicalReasoning(reasoning) = canonical.intent else {
            panic!("canonical reasoning must become a typed event");
        };
        assert!(reasoning.body_matches_projected_form);
        let event = reasoning.event;
        assert_eq!(event.kind, "reasoning");
        assert_eq!(event.label.as_deref(), Some("Reasoning summary"));
        assert_eq!(event.body.as_deref(), Some("Inspect first."));
        assert_eq!(
            event.call_id,
            Some(ProjectedCallId::Source("reason-1".into()))
        );

        let legacy = decode(serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:01Z",
            "payload":{"type":"agent_reasoning","text":"x".repeat(20_000)}
        }));
        let ConversationIntent::LegacyReasoning(event) = legacy.intent else {
            panic!("legacy reasoning must become a typed event");
        };
        assert_eq!(event.label.as_deref(), Some("Reasoning"));
        assert_eq!(event.body.as_ref().unwrap().chars().count(), 16_385);

        let subagent = decode(serde_json::json!({
            "type":"agent_message",
            "timestamp":"2026-07-25T10:00:02Z",
            "author":"parent","recipient":"child","message":"delegate",
            "agent_thread_id":"child-1"
        }));
        let ConversationIntent::SubagentMessage(event) = subagent.intent else {
            panic!("response agent message must become a subagent event");
        };
        assert_eq!(event.label.as_deref(), Some("parent → child"));
        assert_eq!(event.body.as_deref(), Some("delegate"));
    }

    #[test]
    fn canonical_reasoning_exposes_only_a_boolean_when_projection_changes_its_body() {
        let decoded = decode(serde_json::json!({
            "type":"response_item",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"reasoning","id":"reason-redacted",
                "summary":[{
                    "type":"summary_text",
                    "text":"Inspect data:image/png;base64,REASONING_SECRET"
                }]
            }
        }));
        let ConversationIntent::CanonicalReasoning(reasoning) = decoded.intent else {
            panic!("canonical reasoning must become a typed intent");
        };

        assert!(!reasoning.body_matches_projected_form);
        assert_eq!(
            reasoning.event.body.as_deref(),
            Some("Inspect [embedded attachment]")
        );
        assert!(!format!("{reasoning:?}").contains("REASONING_SECRET"));
    }

    #[test]
    fn unrelated_response_event_and_tool_families_are_not_stolen() {
        for value in [
            serde_json::json!({"type":"response_item","payload":{"type":"function_call"}}),
            serde_json::json!({"type":"event_msg","payload":{"type":"task_started"}}),
            serde_json::json!({"type":"event_msg","payload":{"type":"user_message"}}),
            serde_json::json!({"type":"turn_context","payload":{"turn_id":"turn-1"}}),
        ] {
            assert_eq!(
                decode_conversation_record(&state(), 17, &value).unwrap(),
                None,
                "unexpectedly claimed {value}"
            );
        }
    }
}
