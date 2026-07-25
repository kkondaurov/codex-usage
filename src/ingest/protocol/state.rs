use super::tokens::TokenUsage;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(in crate::ingest) struct CursorState {
    #[serde(default)]
    pub(in crate::ingest) projector_generation: u64,
    pub(in crate::ingest) owner_id: String,
    pub(in crate::ingest) thread_id: String,
    pub(in crate::ingest) parent_rollout_id: Option<String>,
    pub(in crate::ingest) parent_thread_id: Option<String>,
    pub(in crate::ingest) agent_path: Option<String>,
    pub(in crate::ingest) agent_nickname: Option<String>,
    pub(in crate::ingest) forked: bool,
    pub(in crate::ingest) native_started: bool,
    pub(in crate::ingest) current_turn: Option<String>,
    pub(in crate::ingest) turn_context_seen: bool,
    pub(in crate::ingest) current_model: Option<String>,
    pub(in crate::ingest) current_effort: Option<String>,
    pub(in crate::ingest) last_timestamp: Option<String>,
    pub(in crate::ingest) cumulative: TokenUsage,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_state_wire_format_is_exact_and_backward_compatible() {
        let state = CursorState {
            projector_generation: 7,
            owner_id: "owner-1".into(),
            thread_id: "thread-1".into(),
            parent_rollout_id: Some("parent-rollout".into()),
            parent_thread_id: None,
            agent_path: Some("/root/child".into()),
            agent_nickname: None,
            forked: true,
            native_started: false,
            current_turn: Some("turn-1".into()),
            turn_context_seen: true,
            current_model: None,
            current_effort: Some("high".into()),
            last_timestamp: Some("2026-07-25T10:20:30.123Z".into()),
            cumulative: TokenUsage {
                input_tokens: 11,
                cached_input_tokens: 7,
                output_tokens: 5,
                reasoning_output_tokens: 3,
                total_tokens: 16,
            },
        };
        let expected = r#"{"projector_generation":7,"owner_id":"owner-1","thread_id":"thread-1","parent_rollout_id":"parent-rollout","parent_thread_id":null,"agent_path":"/root/child","agent_nickname":null,"forked":true,"native_started":false,"current_turn":"turn-1","turn_context_seen":true,"current_model":null,"current_effort":"high","last_timestamp":"2026-07-25T10:20:30.123Z","cumulative":{"input_tokens":11,"cached_input_tokens":7,"output_tokens":5,"reasoning_output_tokens":3,"total_tokens":16}}"#;

        assert_eq!(serde_json::to_string(&state).unwrap(), expected);

        let legacy = expected.replacen(r#""projector_generation":7,"#, "", 1);
        let mut decoded: CursorState = serde_json::from_str(&legacy).unwrap();
        assert_eq!(decoded.projector_generation, 0);
        decoded.projector_generation = 7;
        assert_eq!(serde_json::to_string(&decoded).unwrap(), expected);
    }
}
