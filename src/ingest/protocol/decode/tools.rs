use super::super::{
    duration::{duration_ms, raw_duration_ms},
    event::{EventDraft, ProjectedEvent, shape_projected_event},
    identifiers::normalized_relational_identifier,
    state::CursorState,
    timestamp::canonical_source_timestamp,
};
use crate::redaction::redact_data_urls;
use anyhow::Result;
use serde_json::Value;

/// One database-independent tool record.
///
/// Raw arguments, command text, tool output, image data, and the source JSON
/// never cross this boundary. `event` contains only the durable metadata that
/// the Activity projection already exposes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedToolRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: ToolStateTransition,
    pub(in crate::ingest) ensure_turn: bool,
    pub(in crate::ingest) intent: ToolIntent,
    pub(in crate::ingest) event: Option<ProjectedEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ToolStateTransition {
    pub(in crate::ingest) last_timestamp: String,
    pub(in crate::ingest) current_turn: Option<String>,
}

impl ToolStateTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
        state.current_turn.clone_from(&self.current_turn);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum ToolIntent {
    Start(ToolStart),
    Complete(ToolComplete),
    Enrich(ToolEnrich),
    Terminal(ToolTerminal),
    Noop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ToolStart {
    /// The durable `tool_calls` identity. Web/image starts deliberately prefer
    /// `id`, while their Activity event keeps the source protocol's
    /// `call_id`-first identity.
    pub(in crate::ingest) call_id: String,
    pub(in crate::ingest) name: String,
    pub(in crate::ingest) namespace: Option<String>,
    pub(in crate::ingest) status: String,
    pub(in crate::ingest) completion: Option<ToolCompletion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ToolComplete {
    pub(in crate::ingest) call_id: String,
    pub(in crate::ingest) completion: ToolCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ToolEnrich {
    pub(in crate::ingest) call_id: String,
    pub(in crate::ingest) name: String,
    pub(in crate::ingest) completion: ToolCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ToolTerminal {
    /// The source identity, if supplied. Projection may instead resolve the
    /// latest open same-rollout/name call. The paired event always keeps this
    /// raw source identity rather than the resolved row identity.
    pub(in crate::ingest) explicit_call_id: Option<String>,
    pub(in crate::ingest) name: Option<String>,
    pub(in crate::ingest) namespace: Option<String>,
    /// Legacy latest-open matching compared the raw source name against rows
    /// whose names had already been redacted. If redaction changed the name,
    /// that lookup intentionally did not match. Carry the comparison outcome
    /// without retaining the raw source string.
    pub(in crate::ingest) fallback_name_matches_projected_form: bool,
    pub(in crate::ingest) start_status: String,
    pub(in crate::ingest) completion: ToolCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ToolCompletion {
    /// `None` preserves the weaker response-output semantics: complete a
    /// missing/open row without replacing an already authoritative terminal
    /// timestamp or duration.
    pub(in crate::ingest) status: Option<ToolTerminalStatus>,
    pub(in crate::ingest) duration_ms: Option<i64>,
    pub(in crate::ingest) name_hint: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum ToolTerminalStatus {
    Completed,
    Failed,
}

impl ToolTerminalStatus {
    pub(in crate::ingest) fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Decode response-item and legacy top-level tool shapes.
pub(in crate::ingest) fn decode_response_tool_record(
    state: &CursorState,
    line: u64,
    timestamp: &str,
    payload: &Value,
) -> Result<Option<DecodedToolRecord>> {
    let Some(kind) = payload.get("type").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !matches!(
        kind,
        "function_call"
            | "custom_tool_call"
            | "tool_search_call"
            | "function_call_output"
            | "custom_tool_call_output"
            | "tool_search_output"
            | "web_search_call"
            | "image_generation_call"
    ) {
        return Ok(None);
    }

    let timestamp = canonical_source_timestamp(timestamp)?;
    let explicit_turn = normalized_relational_identifier(
        payload
            .get("internal_chat_message_metadata_passthrough")
            .and_then(|value| value.get("turn_id"))
            .and_then(Value::as_str),
        "turn id",
    )?;
    let transition = ToolStateTransition {
        last_timestamp: timestamp.clone(),
        current_turn: explicit_turn.or_else(|| state.current_turn.clone()),
    };

    let (intent, event) = match kind {
        "function_call" | "custom_tool_call" | "tool_search_call" => {
            let call_id = normalized_relational_identifier(
                payload
                    .get("call_id")
                    .or_else(|| payload.get("id"))
                    .and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_else(|| format!("line-{line}"));
            let name =
                projected_tool_text(payload.get("name").and_then(Value::as_str).unwrap_or(kind));
            let namespace = payload
                .get("namespace")
                .and_then(Value::as_str)
                .map(projected_tool_text);
            // The legacy row projection stored source status verbatim. The
            // paired event below still applies event-side redaction.
            let status = payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("running")
                .to_owned();
            let event = tool_event(
                state,
                payload,
                "tool_call",
                Some(&name),
                payload.get("status").and_then(Value::as_str),
                Some(&name),
                None,
            )?;
            (
                ToolIntent::Start(ToolStart {
                    call_id,
                    name,
                    namespace,
                    status,
                    completion: None,
                }),
                Some(event),
            )
        }
        "function_call_output" | "custom_tool_call_output" | "tool_search_output" => {
            let call_id = normalized_relational_identifier(
                payload
                    .get("call_id")
                    .or_else(|| payload.get("id"))
                    .and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_else(|| "unknown".to_owned());
            let event = tool_event(
                state,
                payload,
                "tool_output",
                Some("Tool result"),
                Some("completed"),
                None,
                None,
            )?;
            (
                ToolIntent::Complete(ToolComplete {
                    call_id,
                    completion: ToolCompletion {
                        status: None,
                        duration_ms: None,
                        name_hint: None,
                    },
                }),
                Some(event),
            )
        }
        "web_search_call" | "image_generation_call" => {
            // Preserve the source protocol's deliberate identity asymmetry:
            // the row prefers `id`, whereas shape_projected_event uses
            // `call_id` first for the Activity event.
            let call_id = normalized_relational_identifier(
                payload
                    .get("id")
                    .or_else(|| payload.get("call_id"))
                    .and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_else(|| format!("line-{line}"));
            let name = projected_tool_text(kind);
            let source_status = payload.get("status").and_then(Value::as_str);
            let status = source_status.unwrap_or("running").to_owned();
            let completion = (source_status == Some("completed")).then(|| ToolCompletion {
                status: Some(ToolTerminalStatus::Completed),
                duration_ms: None,
                name_hint: Some(name.clone()),
            });
            let event = tool_event(
                state,
                payload,
                "tool_call",
                Some(&name),
                source_status,
                Some(&name),
                None,
            )?;
            (
                ToolIntent::Start(ToolStart {
                    call_id,
                    name,
                    namespace: None,
                    status,
                    completion,
                }),
                Some(event),
            )
        }
        _ => unreachable!("response tool family was closed above"),
    };

    Ok(Some(DecodedToolRecord {
        source_line: line,
        timestamp,
        transition,
        ensure_turn: true,
        intent,
        event,
    }))
}

/// Decode event-message terminal envelopes. Missing explicit IDs are still
/// claimed: the record advances owner activity, while Projection decides
/// whether a latest-open row can be matched.
pub(in crate::ingest) fn decode_event_tool_record(
    state: &CursorState,
    line: u64,
    timestamp: &str,
    payload: &Value,
) -> Result<Option<DecodedToolRecord>> {
    let Some(kind) = payload.get("type").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !matches!(
        kind,
        "exec_command_end"
            | "dynamic_tool_call_response"
            | "mcp_tool_call_end"
            | "patch_apply_end"
            | "web_search_end"
            | "image_generation_end"
    ) {
        return Ok(None);
    }

    let timestamp = canonical_source_timestamp(timestamp)?;
    let transition = ToolStateTransition {
        last_timestamp: timestamp.clone(),
        current_turn: state.current_turn.clone(),
    };

    let (intent, event) = match kind {
        "exec_command_end" => {
            let Some(call_id) = normalized_relational_identifier(
                payload.get("call_id").and_then(Value::as_str),
                "tool call id",
            )?
            else {
                return Ok(Some(noop_record(line, timestamp, transition)));
            };
            let completion = ToolCompletion {
                status: Some(terminal_status(exec_failed(payload))),
                duration_ms: decoded_duration(payload),
                name_hint: Some("exec_command".into()),
            };
            let event = tool_event(
                state,
                payload,
                "tool_completed",
                Some("exec_command"),
                completion.status.map(ToolTerminalStatus::as_str),
                Some("exec_command"),
                completion.duration_ms,
            )?;
            (
                ToolIntent::Enrich(ToolEnrich {
                    call_id,
                    name: "exec_command".into(),
                    completion,
                }),
                Some(event),
            )
        }
        "dynamic_tool_call_response" => {
            let Some(call_id) = normalized_relational_identifier(
                payload.get("call_id").and_then(Value::as_str),
                "tool call id",
            )?
            else {
                return Ok(Some(noop_record(line, timestamp, transition)));
            };
            let source_name = payload.get("tool").and_then(Value::as_str);
            let row_name = projected_tool_text(source_name.unwrap_or("dynamic_tool"));
            let event_name = source_name.map(projected_tool_text);
            let completion = ToolCompletion {
                status: Some(terminal_status(dynamic_failed(payload))),
                duration_ms: decoded_duration(payload),
                name_hint: Some(row_name.clone()),
            };
            let event = tool_event(
                state,
                payload,
                "tool_completed",
                event_name.as_deref(),
                completion.status.map(ToolTerminalStatus::as_str),
                event_name.as_deref(),
                completion.duration_ms,
            )?;
            (
                ToolIntent::Enrich(ToolEnrich {
                    call_id,
                    name: row_name,
                    completion,
                }),
                Some(event),
            )
        }
        "mcp_tool_call_end" | "patch_apply_end" | "web_search_end" | "image_generation_end" => {
            let explicit_call_id = normalized_relational_identifier(
                payload.get("call_id").and_then(Value::as_str),
                "tool call id",
            )?;
            let invocation = payload.get("invocation");
            let source_name = match kind {
                "mcp_tool_call_end" => invocation
                    .and_then(|value| value.get("tool"))
                    .and_then(Value::as_str),
                "patch_apply_end" => Some("apply_patch"),
                "web_search_end" => Some("web_search_call"),
                "image_generation_end" => Some("image_generation_call"),
                _ => None,
            };
            let name = source_name.map(projected_tool_text);
            let fallback_name_matches_projected_form = source_name
                .zip(name.as_deref())
                .is_none_or(|(source, projected)| source == projected);
            let namespace = invocation
                .and_then(|value| value.get("server"))
                .and_then(Value::as_str)
                .map(projected_tool_text);
            let start_status = payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("running")
                .to_owned();
            let completion = ToolCompletion {
                status: Some(terminal_status(generic_terminal_failed(payload))),
                duration_ms: decoded_duration(payload),
                name_hint: name.clone(),
            };
            let event_label = name.as_deref().unwrap_or(kind);
            let event = tool_event(
                state,
                payload,
                "tool_completed",
                Some(event_label),
                completion.status.map(ToolTerminalStatus::as_str),
                name.as_deref(),
                completion.duration_ms,
            )?;
            (
                ToolIntent::Terminal(ToolTerminal {
                    explicit_call_id,
                    name,
                    namespace,
                    fallback_name_matches_projected_form,
                    start_status,
                    completion,
                }),
                Some(event),
            )
        }
        _ => unreachable!("event tool family was closed above"),
    };

    Ok(Some(DecodedToolRecord {
        source_line: line,
        timestamp,
        transition,
        ensure_turn: false,
        intent,
        event,
    }))
}

fn noop_record(
    source_line: u64,
    timestamp: String,
    transition: ToolStateTransition,
) -> DecodedToolRecord {
    DecodedToolRecord {
        source_line,
        timestamp,
        transition,
        ensure_turn: false,
        intent: ToolIntent::Noop,
        event: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn tool_event(
    state: &CursorState,
    payload: &Value,
    kind: &str,
    label: Option<&str>,
    status: Option<&str>,
    tool_name: Option<&str>,
    duration_ms: Option<i64>,
) -> Result<ProjectedEvent> {
    shape_projected_event(
        state,
        EventDraft {
            kind,
            role: None,
            label,
            body: None,
            status,
            tool_name,
            duration_ms,
            payload,
        },
    )
}

fn projected_tool_text(value: &str) -> String {
    redact_data_urls(value)
}

fn decoded_duration(payload: &Value) -> Option<i64> {
    duration_ms(payload.get("duration")).or_else(|| raw_duration_ms(payload.get("duration_ms")))
}

fn terminal_status(failed: bool) -> ToolTerminalStatus {
    if failed {
        ToolTerminalStatus::Failed
    } else {
        ToolTerminalStatus::Completed
    }
}

fn exec_failed(payload: &Value) -> bool {
    payload.get("status").and_then(Value::as_str) == Some("failed")
        || payload
            .get("exit_code")
            .and_then(Value::as_i64)
            .is_some_and(|value| value != 0)
        || has_error(payload)
}

fn dynamic_failed(payload: &Value) -> bool {
    payload.get("success").and_then(Value::as_bool) == Some(false) || has_error(payload)
}

fn generic_terminal_failed(payload: &Value) -> bool {
    payload.get("success").and_then(Value::as_bool) == Some(false)
        || matches!(
            payload.get("status").and_then(Value::as_str),
            Some("failed" | "cancelled" | "canceled")
        )
        || has_error(payload)
}

fn has_error(payload: &Value) -> bool {
    payload.get("error").is_some_and(|value| !value.is_null())
}

#[cfg(test)]
mod tests {
    use super::super::super::event::ProjectedCallId;
    use super::*;

    fn state() -> CursorState {
        CursorState {
            owner_id: "rollout-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("turn-old".into()),
            last_timestamp: Some("2026-07-25T08:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    #[test]
    fn response_starts_preserve_ids_names_namespace_status_and_turn_transition() {
        let payload = serde_json::json!({
            "type":"function_call",
            "call_id":"call-preferred",
            "id":"id-secondary",
            "name":"exec_command",
            "namespace":"functions",
            "status":"in_progress",
            "arguments":{"secret":"must not cross"},
            "internal_chat_message_metadata_passthrough":{"turn_id":"turn-new"}
        });
        let decoded =
            decode_response_tool_record(&state(), 11, "2026-07-25T12:00:00+02:00", &payload)
                .unwrap()
                .unwrap();
        assert_eq!(decoded.timestamp, "2026-07-25T10:00:00.000000000Z");
        assert_eq!(decoded.transition.current_turn.as_deref(), Some("turn-new"));
        assert!(decoded.ensure_turn);
        let ToolIntent::Start(start) = decoded.intent else {
            panic!("expected start");
        };
        assert_eq!(start.call_id, "call-preferred");
        assert_eq!(start.name, "exec_command");
        assert_eq!(start.namespace.as_deref(), Some("functions"));
        assert_eq!(start.status, "in_progress");
        assert!(start.completion.is_none());
        let event = decoded.event.unwrap();
        assert_eq!(
            event.call_id,
            Some(ProjectedCallId::Source("call-preferred".into()))
        );
        assert_eq!(event.body, None);
        assert_eq!(event.metadata, None);
    }

    #[test]
    fn legacy_unbounded_row_fields_remain_distinct_from_event_redaction() {
        let name = format!("tool-{}", "n".repeat(900));
        let namespace = format!("namespace-{}", "s".repeat(700));
        let status = format!("working data:image/png;base64,aGVsbG8= {}", "x".repeat(800));
        let decoded = decode_response_tool_record(
            &state(),
            12,
            "2026-07-25T10:00:00Z",
            &serde_json::json!({
                "type":"function_call",
                "call_id":"call-long",
                "name":name,
                "namespace":namespace,
                "status":status
            }),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Start(start) = decoded.intent else {
            panic!("expected start");
        };
        assert_eq!(start.name, name);
        assert_eq!(start.namespace.as_deref(), Some(namespace.as_str()));
        assert_eq!(start.status, status);

        let event = decoded.event.unwrap();
        assert_eq!(event.label.as_deref(), Some(name.as_str()));
        assert_eq!(event.tool_name.as_deref(), Some(name.as_str()));
        assert_ne!(event.status.as_deref(), Some(status.as_str()));
        assert!(!event.status.unwrap().contains("data:image"));
    }

    #[test]
    fn starts_and_outputs_keep_their_distinct_missing_id_fallbacks() {
        let start = decode_response_tool_record(
            &state(),
            41,
            "2026-07-25T10:00:00Z",
            &serde_json::json!({"type":"custom_tool_call","name":"dynamic"}),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Start(start) = start.intent else {
            panic!("expected start");
        };
        assert_eq!(start.call_id, "line-41");

        let output = decode_response_tool_record(
            &state(),
            42,
            "2026-07-25T10:00:01Z",
            &serde_json::json!({"type":"custom_tool_call_output","output":"private"}),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Complete(output) = output.intent else {
            panic!("expected completion");
        };
        assert_eq!(output.call_id, "unknown");
        assert!(output.completion.status.is_none());
    }

    #[test]
    fn web_and_image_rows_prefer_id_while_events_prefer_call_id() {
        for kind in ["web_search_call", "image_generation_call"] {
            let decoded = decode_response_tool_record(
                &state(),
                17,
                "2026-07-25T10:00:00Z",
                &serde_json::json!({
                    "type":kind,"id":"row-id","call_id":"event-call","status":"completed"
                }),
            )
            .unwrap()
            .unwrap();
            let ToolIntent::Start(start) = decoded.intent else {
                panic!("expected start");
            };
            assert_eq!(start.call_id, "row-id");
            assert_eq!(
                start.completion.unwrap().status,
                Some(ToolTerminalStatus::Completed)
            );
            assert_eq!(
                decoded.event.unwrap().call_id,
                Some(ProjectedCallId::Source("event-call".into()))
            );
        }
    }

    #[test]
    fn exec_dynamic_and_generic_failures_keep_the_exact_source_rules() {
        let exec = decode_event_tool_record(
            &state(),
            20,
            "2026-07-25T10:00:00Z",
            &serde_json::json!({
                "type":"exec_command_end","call_id":"exec-1","exit_code":7,
                "duration":{"secs":1,"nanos":1}
            }),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Enrich(exec) = exec.intent else {
            panic!("expected exec enrichment");
        };
        assert_eq!(exec.completion.status, Some(ToolTerminalStatus::Failed));
        assert_eq!(exec.completion.duration_ms, Some(1_001));

        let dynamic = decode_event_tool_record(
            &state(),
            21,
            "2026-07-25T10:00:01Z",
            &serde_json::json!({
                "type":"dynamic_tool_call_response","call_id":"dynamic-1",
                "success":false,"tool":"node_repl"
            }),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Enrich(dynamic) = dynamic.intent else {
            panic!("expected dynamic enrichment");
        };
        assert_eq!(dynamic.name, "node_repl");
        assert_eq!(dynamic.completion.status, Some(ToolTerminalStatus::Failed));

        let generic = decode_event_tool_record(
            &state(),
            22,
            "2026-07-25T10:00:02Z",
            &serde_json::json!({
                "type":"mcp_tool_call_end","call_id":"mcp-1","status":"cancelled",
                "invocation":{"server":"docs","tool":"search"}
            }),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Terminal(generic) = generic.intent else {
            panic!("expected generic terminal");
        };
        assert_eq!(generic.name.as_deref(), Some("search"));
        assert_eq!(generic.namespace.as_deref(), Some("docs"));
        assert_eq!(generic.completion.status, Some(ToolTerminalStatus::Failed));
    }

    #[test]
    fn generic_terminal_event_retains_raw_source_call_identity() {
        let decoded = decode_event_tool_record(
            &state(),
            23,
            "2026-07-25T10:00:00Z",
            &serde_json::json!({
                "type":"patch_apply_end","call_id":"source-call","success":true,
                "payload":{"large":"discarded"}
            }),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Terminal(terminal) = decoded.intent else {
            panic!("expected generic terminal");
        };
        assert_eq!(terminal.explicit_call_id.as_deref(), Some("source-call"));
        assert_eq!(terminal.name.as_deref(), Some("apply_patch"));
        let event = decoded.event.unwrap();
        assert_eq!(
            event.call_id,
            Some(ProjectedCallId::Source("source-call".into()))
        );
        assert_eq!(event.body, None);
        assert_eq!(event.metadata, None);
    }

    #[test]
    fn redacted_terminal_name_disables_legacy_latest_open_fallback() {
        let source_name = "run data:image/png;base64,aGVsbG8=";
        let decoded = decode_event_tool_record(
            &state(),
            25,
            "2026-07-25T10:00:00Z",
            &serde_json::json!({
                "type":"mcp_tool_call_end",
                "call_id":"source-call",
                "invocation":{"server":"tools","tool":source_name}
            }),
        )
        .unwrap()
        .unwrap();
        let ToolIntent::Terminal(terminal) = decoded.intent else {
            panic!("expected generic terminal");
        };
        assert_eq!(terminal.name.as_deref(), Some("run [embedded attachment]"));
        assert!(!terminal.fallback_name_matches_projected_form);
    }

    #[test]
    fn missing_direct_terminal_id_is_a_typed_noop_not_an_unknown_record() {
        let decoded = decode_event_tool_record(
            &state(),
            24,
            "2026-07-25T10:00:00Z",
            &serde_json::json!({"type":"exec_command_end","exit_code":0}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(decoded.intent, ToolIntent::Noop);
        assert!(decoded.event.is_none());
        assert!(!decoded.ensure_turn);
    }
}
