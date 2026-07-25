#![cfg(test)]

use super::super::{File, Path, Value};
use std::io::Write;

pub(super) fn projected_message_id(rollout_id: &str, source_id: &str) -> String {
    format!("message:{}", serde_json::json!([rollout_id, source_id]))
}

pub(super) fn write_fixture(path: &Path, lines: &[Value]) {
    let mut file = File::create(path).unwrap();
    for line in lines {
        writeln!(file, "{}", serde_json::to_string(line).unwrap()).unwrap();
    }
}

pub(super) fn meta(timestamp: &str, owner: &str, thread: &str, fork: bool) -> Value {
    let source = if fork {
        serde_json::json!({"subagent":{"thread_spawn":{
            "parent_thread_id":thread,"agent_path":"/root/child","agent_nickname":"Newton"
        }}})
    } else {
        Value::String("vscode".into())
    };
    serde_json::json!({"timestamp":timestamp,"type":"session_meta","payload":{
        "id":owner,"session_id":thread,"cwd":"/tmp/project","source":source
    }})
}

pub(super) fn task(timestamp: &str, turn: &str) -> Value {
    serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
        "type":"task_started","turn_id":turn
    }})
}

pub(super) fn root_fork_meta(timestamp: &str, owner: &str, parent: &str) -> Value {
    serde_json::json!({"timestamp":timestamp,"type":"session_meta","payload":{
        "id":owner,"session_id":owner,"forked_from_id":parent,
        "cwd":"/tmp/project","source":"vscode"
    }})
}

pub(super) fn legacy_child_meta(timestamp: &str, owner: &str, parent: &str) -> Value {
    serde_json::json!({"timestamp":timestamp,"type":"session_meta","payload":{
        "id":owner,"forked_from_id":parent,"cwd":"/tmp/project",
        "source":{"subagent":{"thread_spawn":{
            "parent_thread_id":parent,"agent_path":"/root/reviewer",
            "agent_nickname":"Ramanujan"
        }}}
    }})
}

pub(super) fn context(timestamp: &str, turn: &str, model: &str) -> Value {
    serde_json::json!({"timestamp":timestamp,"type":"turn_context","payload":{
        "turn_id":turn,"model":model,"effort":"high"
    }})
}

pub(super) fn usage(timestamp: &str, input: u64) -> Value {
    serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
        "type":"token_count","info":{
            "total_token_usage":{"input_tokens":input,"cached_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0,"total_tokens":input+1},
            "last_token_usage":{"input_tokens":input,"cached_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0,"total_tokens":input+1}
        }
    }})
}
