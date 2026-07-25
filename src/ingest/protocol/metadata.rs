use super::{
    content::normalized_metadata_value,
    identifiers::{
        PROJECTED_IDENTIFIER_CHARS, normalized_relational_identifier, required_metadata_identifier,
    },
    timestamp::canonical_source_timestamp,
};
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::path::Path;

const PROJECTED_EVENT_LABEL_CHARS: usize = 512;
const PROJECTED_SESSION_PATH_CHARS: usize = 4 * 1024;

#[derive(Clone, Debug)]
pub(in crate::ingest) struct OwnerMeta {
    pub(in crate::ingest) owner_id: String,
    pub(in crate::ingest) thread_id: String,
    pub(in crate::ingest) parent_rollout_id: Option<String>,
    pub(in crate::ingest) parent_thread_id: Option<String>,
    pub(in crate::ingest) agent_path: Option<String>,
    pub(in crate::ingest) agent_nickname: Option<String>,
    pub(in crate::ingest) is_subagent: bool,
    pub(in crate::ingest) forked: bool,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) cwd: Option<String>,
    pub(in crate::ingest) project: Option<String>,
    pub(in crate::ingest) repository_url: Option<String>,
    pub(in crate::ingest) branch: Option<String>,
    pub(in crate::ingest) source: Option<String>,
    pub(in crate::ingest) thread_source: Option<String>,
    pub(in crate::ingest) source_json: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::ingest) struct SessionMetadata {
    pub(in crate::ingest) cwd: Option<String>,
    pub(in crate::ingest) project: Option<String>,
    pub(in crate::ingest) repository_url: Option<String>,
    pub(in crate::ingest) branch: Option<String>,
    pub(in crate::ingest) source: Option<String>,
    pub(in crate::ingest) thread_source: Option<String>,
}

/// Decode one JSONL value when it is an owner-defining session record.
///
/// File discovery and bounded JSONL streaming deliberately remain outside this
/// module. This decoder owns only the protocol interpretation, normalization,
/// and topology precedence of a single already-parsed value.
pub(in crate::ingest) fn decode_owner_record(value: &Value) -> Result<Option<OwnerMeta>> {
    let legacy_meta =
        value.get("type").is_none() && value.get("id").and_then(Value::as_str).is_some();
    if value.get("type").and_then(Value::as_str) != Some("session_meta") && !legacy_meta {
        return Ok(None);
    }

    let payload = value.get("payload").unwrap_or(value);
    let owner_id =
        required_metadata_identifier(payload.get("id").and_then(Value::as_str), "rollout id")?;
    let subagent = payload
        .get("source")
        .and_then(|value| value.get("subagent"));
    let spawn = subagent.and_then(|value| value.get("thread_spawn"));
    let explicit_thread_id = normalized_relational_identifier(
        payload.get("session_id").and_then(Value::as_str),
        "session thread id",
    )?;
    let spawn_parent_thread_id = normalized_relational_identifier(
        spawn
            .and_then(|value| value.get("parent_thread_id"))
            .and_then(Value::as_str),
        "parent thread id",
    )?;
    // Older child rollouts omit session_id. Their parent thread is the only
    // top-level ownership signal and must not become a fake session.
    let thread_id = explicit_thread_id
        .clone()
        .or_else(|| spawn_parent_thread_id.clone())
        .unwrap_or_else(|| owner_id.clone());
    let parent_thread_id =
        spawn_parent_thread_id.or_else(|| (owner_id != thread_id).then(|| thread_id.clone()));
    let forked_from_id = normalized_relational_identifier(
        payload.get("forked_from_id").and_then(Value::as_str),
        "fork parent rollout id",
    )?;
    let spawn_parent_rollout_id = normalized_relational_identifier(
        spawn
            .and_then(|value| value.get("parent_rollout_id"))
            .and_then(Value::as_str),
        "spawn parent rollout id",
    )?;
    let parent_rollout_id = forked_from_id
        .or(spawn_parent_rollout_id)
        .or_else(|| parent_thread_id.clone());
    let agent_path = spawn
        .and_then(|value| value.get("agent_path"))
        .and_then(Value::as_str)
        .and_then(|value| normalized_metadata_value(Some(value), PROJECTED_SESSION_PATH_CHARS));
    let agent_nickname = spawn
        .and_then(|value| value.get("agent_nickname"))
        .and_then(Value::as_str)
        .and_then(|value| normalized_metadata_value(Some(value), PROJECTED_EVENT_LABEL_CHARS))
        .or_else(|| {
            subagent
                .and_then(|value| value.get("other"))
                .and_then(Value::as_str)
                .and_then(|value| {
                    normalized_metadata_value(Some(value), PROJECTED_EVENT_LABEL_CHARS)
                })
        });
    let metadata = normalized_session_metadata(payload);
    let is_subagent = spawn.is_some()
        || explicit_thread_id
            .as_deref()
            .is_some_and(|value| value != owner_id);
    let forked = owner_id != thread_id || spawn.is_some() || parent_rollout_id.is_some();
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .or_else(|| payload.get("timestamp").and_then(Value::as_str))
        .ok_or_else(|| anyhow!("first session_meta has no timestamp"))?;

    Ok(Some(OwnerMeta {
        owner_id,
        thread_id,
        parent_rollout_id,
        parent_thread_id,
        agent_path,
        agent_nickname,
        is_subagent,
        forked,
        timestamp: canonical_source_timestamp(timestamp)?,
        cwd: metadata.cwd,
        project: metadata.project,
        repository_url: metadata.repository_url,
        branch: metadata.branch,
        source: metadata.source,
        thread_source: metadata.thread_source,
        // Topology and authored labels have dedicated fields. The source
        // object can carry arbitrarily large transport context and has no
        // remaining query consumer, so it is intentionally not retained.
        source_json: None,
    }))
}

pub(in crate::ingest) fn normalized_session_metadata(payload: &Value) -> SessionMetadata {
    let cwd = normalized_metadata_value(
        payload.get("cwd").and_then(Value::as_str),
        PROJECTED_SESSION_PATH_CHARS,
    );
    let project = cwd
        .as_deref()
        .and_then(|value| Path::new(value).file_name()?.to_str())
        .and_then(|value| normalized_metadata_value(Some(value), PROJECTED_EVENT_LABEL_CHARS));
    let git = payload.get("git").unwrap_or(&Value::Null);
    let subagent = payload
        .get("source")
        .and_then(|value| value.get("subagent"));
    let source = normalized_metadata_value(
        payload.get("source").and_then(Value::as_str),
        PROJECTED_IDENTIFIER_CHARS,
    )
    .or_else(|| subagent.map(|_| "subagent".to_owned()));
    SessionMetadata {
        cwd,
        project,
        repository_url: normalized_metadata_value(
            git.get("repository_url").and_then(Value::as_str),
            PROJECTED_SESSION_PATH_CHARS,
        ),
        branch: normalized_metadata_value(
            git.get("branch").and_then(Value::as_str),
            PROJECTED_IDENTIFIER_CHARS,
        ),
        source,
        thread_source: normalized_metadata_value(
            payload.get("thread_source").and_then(Value::as_str),
            PROJECTED_IDENTIFIER_CHARS,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_session_metadata_decodes_and_normalizes() {
        let owner = decode_owner_record(&serde_json::json!({
            "type": "session_meta",
            "timestamp": "2026-07-15T10:11:12.123+02:30",
            "payload": {
                "id": "root-rollout",
                "cwd": " /Users/example/project ",
                "git": {
                    "repository_url": " https://example.test/project.git ",
                    "branch": " main "
                },
                "source": "cli",
                "thread_source": "desktop"
            }
        }))
        .unwrap()
        .unwrap();

        assert_eq!(owner.owner_id, "root-rollout");
        assert_eq!(owner.thread_id, "root-rollout");
        assert_eq!(owner.parent_rollout_id, None);
        assert_eq!(owner.parent_thread_id, None);
        assert!(!owner.is_subagent);
        assert!(!owner.forked);
        assert_eq!(owner.timestamp, "2026-07-15T07:41:12.123000000Z");
        assert_eq!(owner.cwd.as_deref(), Some("/Users/example/project"));
        assert_eq!(owner.project.as_deref(), Some("project"));
        assert_eq!(
            owner.repository_url.as_deref(),
            Some("https://example.test/project.git")
        );
        assert_eq!(owner.branch.as_deref(), Some("main"));
        assert_eq!(owner.source.as_deref(), Some("cli"));
        assert_eq!(owner.thread_source.as_deref(), Some("desktop"));
        assert_eq!(owner.source_json, None);
    }

    #[test]
    fn legacy_root_metadata_is_supported() {
        let owner = decode_owner_record(&serde_json::json!({
            "id": "legacy-rollout",
            "timestamp": "2026-07-15T07:41:12Z",
            "cwd": "/tmp/legacy"
        }))
        .unwrap()
        .unwrap();

        assert_eq!(owner.owner_id, "legacy-rollout");
        assert_eq!(owner.thread_id, "legacy-rollout");
        assert_eq!(owner.project.as_deref(), Some("legacy"));
        assert!(!owner.is_subagent);
        assert!(!owner.forked);
        assert!(
            decode_owner_record(&serde_json::json!({"type":"response_item"}))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn spawn_child_uses_parent_thread_without_inventing_a_session() {
        let owner = decode_owner_record(&serde_json::json!({
            "type": "session_meta",
            "timestamp": "2026-07-15T07:41:12Z",
            "payload": {
                "id": "child-rollout",
                "source": {"subagent": {"thread_spawn": {
                    "parent_thread_id": "parent-thread",
                    "parent_rollout_id": "parent-rollout",
                    "agent_path": "review/backend",
                    "agent_nickname": "backend reviewer"
                }}}
            }
        }))
        .unwrap()
        .unwrap();

        assert_eq!(owner.thread_id, "parent-thread");
        assert_eq!(owner.parent_thread_id.as_deref(), Some("parent-thread"));
        assert_eq!(owner.parent_rollout_id.as_deref(), Some("parent-rollout"));
        assert_eq!(owner.agent_path.as_deref(), Some("review/backend"));
        assert_eq!(owner.agent_nickname.as_deref(), Some("backend reviewer"));
        assert_eq!(owner.source.as_deref(), Some("subagent"));
        assert!(owner.is_subagent);
        assert!(owner.forked);
    }

    #[test]
    fn explicit_session_and_fork_parent_have_precedence_over_spawn_fallbacks() {
        let owner = decode_owner_record(&serde_json::json!({
            "type": "session_meta",
            "timestamp": "2026-07-15T07:41:12Z",
            "payload": {
                "id": "child-rollout",
                "session_id": "explicit-thread",
                "forked_from_id": "explicit-fork-parent",
                "source": {"subagent": {
                    "other": "fallback nickname",
                    "thread_spawn": {
                        "parent_thread_id": "spawn-parent-thread",
                        "parent_rollout_id": "spawn-parent-rollout"
                    }
                }}
            }
        }))
        .unwrap()
        .unwrap();

        assert_eq!(owner.thread_id, "explicit-thread");
        assert_eq!(
            owner.parent_thread_id.as_deref(),
            Some("spawn-parent-thread")
        );
        assert_eq!(
            owner.parent_rollout_id.as_deref(),
            Some("explicit-fork-parent")
        );
        assert_eq!(owner.agent_nickname.as_deref(), Some("fallback nickname"));
        assert!(owner.is_subagent);
        assert!(owner.forked);
    }

    #[test]
    fn required_and_relational_identifiers_fail_closed() {
        let missing = decode_owner_record(&serde_json::json!({
            "type": "session_meta",
            "timestamp": "2026-07-15T07:41:12Z",
            "payload": {}
        }))
        .unwrap_err();
        assert_eq!(missing.to_string(), "first session_meta has no rollout id");

        for (field, label) in [
            ("id", "rollout id"),
            ("session_id", "session thread id"),
            ("forked_from_id", "fork parent rollout id"),
        ] {
            let mut payload = serde_json::json!({"id":"safe-rollout"});
            payload[field] = Value::String("unsafe\nidentifier".into());
            let error = decode_owner_record(&serde_json::json!({
                "type": "session_meta",
                "timestamp": "2026-07-15T07:41:12Z",
                "payload": payload
            }))
            .unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("{label} contains invalid identifier content")
            );
        }
    }

    #[test]
    fn authored_metadata_is_trimmed_bounded_and_redacted() {
        let long_path = format!("/tmp/{}", "x".repeat(PROJECTED_SESSION_PATH_CHARS + 100));
        let owner = decode_owner_record(&serde_json::json!({
            "type": "session_meta",
            "payload": {
                "id": "child-rollout",
                "timestamp": "2026-07-15T07:41:12Z",
                "cwd": long_path,
                "git": {
                    "repository_url": "data:text/plain;base64,private",
                    "branch": " data:image/png;base64,private "
                },
                "thread_source": " data:application/json;base64,private ",
                "source": {"subagent": {"thread_spawn": {
                    "parent_thread_id": "parent-thread",
                    "agent_path": "data:text/plain;base64,private",
                    "agent_nickname": " data:image/png;base64,private "
                }}}
            }
        }))
        .unwrap()
        .unwrap();

        assert_eq!(
            owner.cwd.as_ref().unwrap().chars().count(),
            PROJECTED_SESSION_PATH_CHARS + 1
        );
        assert!(owner.cwd.as_ref().unwrap().ends_with('…'));
        assert_eq!(
            owner.repository_url.as_deref(),
            Some("[embedded attachment]")
        );
        assert_eq!(owner.branch.as_deref(), Some("[embedded attachment]"));
        assert_eq!(
            owner.thread_source.as_deref(),
            Some("[embedded attachment]")
        );
        assert_eq!(owner.agent_path.as_deref(), Some("[embedded attachment]"));
        assert_eq!(
            owner.agent_nickname.as_deref(),
            Some("[embedded attachment]")
        );
        assert_eq!(owner.source_json, None);
    }
}
