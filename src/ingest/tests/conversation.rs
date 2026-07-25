#![cfg(test)]

use super::super::*;
use super::support::*;

#[test]
fn explicit_turn_metadata_keeps_mid_turn_user_messages_on_native_turn() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let explicit_user_message = |timestamp: &str, text: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"message","role":"user","content":[{
                "type":"input_text","text":text
            }],
            "internal_chat_message_metadata_passthrough":{"turn_id":turn}
        }})
    };
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            explicit_user_message("2026-07-15T09:00:02Z", "Start the research."),
            explicit_user_message(
                "2026-07-15T09:00:03Z",
                "Use the signed-in built-in browser.",
            ),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"Adapting the browser research."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04.100Z","type":"response_item","payload":{
                "type":"reasoning","summary":[{
                    "type":"summary_text","text":"Adapting the browser research."
                }],
                "internal_chat_message_metadata_passthrough":{"turn_id":turn}
            }}),
            explicit_user_message(
                "2026-07-15T09:00:05Z",
                "<subagent_notification>{\"status\":\"completed\"}</subagent_notification>",
            ),
            serde_json::json!({"timestamp":"2026-07-15T09:00:06Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"Integrating the subagent result."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:07Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":"Research complete."
                }],
                "internal_chat_message_metadata_passthrough":{"turn_id":turn}
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:08Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":turn,"last_agent_message":"Research complete."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let turn_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
        .unwrap();
    let legacy_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE id LIKE '%:legacy-turn:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let user_messages: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE turn_id=?1 AND role='user'",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    let reasoning_events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND kind='reasoning'",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    let status: String = connection
        .query_row("SELECT status FROM turns WHERE id=?1", [turn], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(turn_count, 1);
    assert_eq!(legacy_count, 0);
    assert_eq!(user_messages, 3);
    assert_eq!(reasoning_events, 2);
    assert_eq!(status, "completed");
}

#[test]
fn metadata_free_feedback_stays_on_running_native_turn() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019db16f-0000-7000-8000-000000000000";
    let turn = "019db170-0000-7000-8000-000000000000";
    let user_message = |timestamp: &str, text: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"message","role":"user","content":[{
                "type":"input_text","text":text
            }]
        }})
    };
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-04-21T19:06:18Z", owner, owner, false),
            task("2026-04-21T19:06:18.100Z", turn),
            context("2026-04-21T19:06:18.100Z", turn, "gpt-5.4"),
            user_message("2026-04-21T19:06:18.200Z", "Create a Valencia comic."),
            user_message(
                "2026-04-21T19:27:28.444Z",
                "Use a T-shirt and clearly blue jeans.",
            ),
            user_message("2026-04-21T19:27:28.445Z", "Keep the comic wordless."),
            user_message(
                "2026-04-21T19:27:28.446Z",
                "Reduce the protagonist appearances.",
            ),
            serde_json::json!({"timestamp":"2026-04-21T19:29:04Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"commentary","content":[{
                    "type":"output_text","text":"Understood: blue jeans, no captions, and fewer protagonist appearances."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-04-21T19:29:05Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"Applying the combined feedback."
            }}),
            usage("2026-04-21T19:29:06Z", 42_000),
            serde_json::json!({"timestamp":"2026-04-21T19:54:20Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":"The revised Valencia comic is complete."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-04-21T19:54:20.100Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":turn,
                "last_agent_message":"The revised Valencia comic is complete."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let turn_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
        .unwrap();
    let legacy_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE id LIKE '%:legacy-turn:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let user_messages: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE turn_id=?1 AND role='user'",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    let reasoning_events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND kind='reasoning'",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    let usage_facts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM usage_facts WHERE turn_id=?1",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    let status: String = connection
        .query_row("SELECT status FROM turns WHERE id=?1", [turn], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(turn_count, 1);
    assert_eq!(legacy_count, 0);
    assert_eq!(user_messages, 4);
    assert_eq!(reasoning_events, 1);
    assert_eq!(usage_facts, 1);
    assert_eq!(status, "completed");
}

#[test]
fn metadata_free_feedback_after_provisional_final_stays_on_native_turn() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019cc496-0000-7000-8000-000000000000";
    let turn = "019cc4e2-0000-7000-8000-000000000000";
    let user_message = |timestamp: &str, text: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"message","role":"user","content":[{
                "type":"input_text","text":text
            }]
        }})
    };
    let final_message = |timestamp: &str, text: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"message","role":"assistant","phase":"final_answer","content":[{
                "type":"output_text","text":text
            }]
        }})
    };
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-03-06T20:41:20Z", owner, owner, false),
            task("2026-03-06T20:41:21Z", turn),
            context("2026-03-06T20:41:21Z", turn, "gpt-5.4"),
            user_message("2026-03-06T20:41:22Z", "Watch the batch."),
            final_message("2026-03-06T21:17:56.273Z", "The deep dive is complete."),
            user_message(
                "2026-03-06T21:17:56.274Z",
                "Please repair the previous takeaway and continue watching.",
            ),
            serde_json::json!({"timestamp":"2026-03-06T21:19:21Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"commentary","content":[{
                    "type":"output_text","text":"Repairing it and continuing the watch."
                }]
            }}),
            final_message("2026-03-06T22:41:02.668Z", "The batch is stable."),
            serde_json::json!({"timestamp":"2026-03-06T22:41:02.669Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":turn,
                "last_agent_message":"The batch is stable."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let turn_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
        .unwrap();
    let user_messages: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE turn_id=?1 AND role='user'",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    let state: (String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT status,completed_at,last_agent_message FROM turns WHERE id=?1",
            [turn],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    assert_eq!(turn_count, 1);
    assert_eq!(user_messages, 2);
    assert_eq!(state.0, "completed");
    assert_eq!(state.1.as_deref(), Some("2026-03-06T22:41:02.669000000Z"));
    assert_eq!(state.2.as_deref(), Some("The batch is stable."));
}

#[test]
fn old_order_context_envelopes_do_not_hide_the_following_human_prompt() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019c443a-0000-7000-8000-000000000000";
    let first_turn = "019c443b-0000-7000-8000-000000000000";
    let second_turn = "019c5e03-0000-7000-8000-000000000000";
    let user_message = |timestamp: &str, text: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"message","role":"user","content":[{
                "type":"input_text","text":text
            }]
        }})
    };
    let final_message = |timestamp: &str, text: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"message","role":"assistant","phase":"final_answer","content":[{
                "type":"output_text","text":text
            }]
        }})
    };
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-02-14T20:00:00Z", owner, owner, false),
            task("2026-02-14T20:00:01Z", first_turn),
            context("2026-02-14T20:00:01Z", first_turn, "gpt-5.4"),
            user_message("2026-02-14T20:00:02Z", "Finish the first task."),
            final_message("2026-02-14T20:00:03Z", "First task complete."),
            serde_json::json!({"timestamp":"2026-02-14T20:00:03.100Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":first_turn,
                "last_agent_message":"First task complete."
            }}),
            user_message(
                "2026-02-14T21:17:08.962Z",
                "# AGENTS.md instructions for /Users/example/project\n\n<INSTRUCTIONS>\nUse the project rules.\n</INSTRUCTIONS>",
            ),
            user_message(
                "2026-02-14T21:17:08.962Z",
                "<environment_context>\n  <cwd>/Users/example/project</cwd>\n  <shell>zsh</shell>\n</environment_context>",
            ),
            task("2026-02-14T21:17:08.962Z", second_turn),
            user_message(
                "2026-02-14T21:17:08.963Z",
                "This is the actual second human prompt.",
            ),
            context("2026-02-14T21:17:08.964Z", second_turn, "gpt-5.4"),
            final_message("2026-02-14T21:17:09Z", "Second task complete."),
            serde_json::json!({"timestamp":"2026-02-14T21:17:09.100Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":second_turn,
                "last_agent_message":"Second task complete."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE id LIKE '%:legacy-turn:%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let second_prompt: String = connection
        .query_row(
            "SELECT content FROM messages WHERE turn_id=?1 AND role='user'",
            [second_turn],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(second_prompt, "This is the actual second human prompt.");
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM messages
                     WHERE content LIKE '# AGENTS.md instructions for %'
                        OR content LIKE '<environment_context>%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn a_new_native_task_interrupts_an_unfinished_previous_task() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019cb02f-0000-7000-8000-000000000000";
    let first_turn = "019cb030-0000-7000-8000-000000000000";
    let second_turn = "019cb031-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-03-02T20:00:00Z", owner, owner, false),
            task("2026-03-02T20:00:01Z", first_turn),
            context("2026-03-02T20:00:01Z", first_turn, "gpt-5.4"),
            serde_json::json!({"timestamp":"2026-03-02T20:00:02Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":"Begin the first task."
                }]
            }}),
            task("2026-03-02T20:05:00Z", second_turn),
            context("2026-03-02T20:05:00Z", second_turn, "gpt-5.4"),
            serde_json::json!({"timestamp":"2026-03-02T20:05:01Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":"Second task complete."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-03-02T20:05:01.100Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":second_turn,
                "last_agent_message":"Second task complete."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let first_state: (String, Option<String>) = connection
        .query_row(
            "SELECT status,completed_at FROM turns WHERE id=?1",
            [first_turn],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(first_state.0, "interrupted");
    assert_eq!(
        first_state.1.as_deref(),
        Some("2026-03-02T20:05:00.000000000Z")
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT status FROM turns WHERE id=?1",
                [second_turn],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "completed"
    );
}

#[test]
fn explicit_abort_after_final_answer_remains_authoritative() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":"A final result that is subsequently interrupted."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                "type":"turn_aborted","turn_id":turn,"reason":"interrupted"
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let state: (String, Option<String>) = connection
        .query_row(
            "SELECT status,completed_at FROM turns WHERE id=?1",
            [turn],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state.0, "interrupted");
    assert_eq!(state.1.as_deref(), Some("2026-07-15T09:00:03.000000000Z"));
}

#[test]
fn thread_rollback_is_preserved_as_its_own_terminal_state() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"thread_rolled_back","num_turns":1
            }}),
        ],
    );
    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let turn_status: String = connection
        .query_row("SELECT status FROM turns WHERE id=?1", [turn], |row| {
            row.get(0)
        })
        .unwrap();
    let agent_status: String = connection
        .query_row(
            "SELECT status FROM agent_runs WHERE id=?1",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    let event_status: String = connection
        .query_row(
            "SELECT status FROM events WHERE label='thread_rolled_back'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(turn_status, "rolled_back");
    assert_eq!(agent_status, "rolled_back");
    assert_eq!(event_status, "rolled_back");
}

#[test]
fn recommended_plugins_runtime_bundle_is_not_projected_as_a_user_message() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f6768-0000-7000-8000-000000000000";
    let turn = "019f6769-0000-7000-8000-000000000000";
    let transport_bundle = r#"<recommended_plugins>
Here is a list of plugins available to the runtime.
</recommended_plugins>
# AGENTS.md instructions for /tmp/project
<INSTRUCTIONS>
Use the project rules.
</INSTRUCTIONS>
<environment_context>
  <cwd>/tmp/project</cwd>
  <shell>zsh</shell>
</environment_context>"#;
    let actual_prompt = r#"# Applications mentioned by the user:

<appshot app="Ghostty">Terminal evidence.</appshot>

## My request for Codex:
Trace the real first prompt."#;
    let mixed_request =
        format!("{transport_bundle}\n\n## My request for Codex:\nKeep this real user request.");

    assert!(is_transport_context_envelope(transport_bundle));
    assert!(!is_transport_context_envelope(&mixed_request));

    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T20:13:11.982Z", owner, owner, false),
            task("2026-07-15T20:13:11.982Z", turn),
            serde_json::json!({"timestamp":"2026-07-15T20:13:12.003Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":transport_bundle
                }]
            }}),
            context("2026-07-15T20:13:12.003Z", turn, "gpt-5.6-sol"),
            serde_json::json!({"timestamp":"2026-07-15T20:13:12.074Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":actual_prompt
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T20:13:12.086Z","type":"event_msg","payload":{
                "type":"user_message","message":actual_prompt
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let messages: Vec<String> = connection
        .prepare("SELECT content FROM messages WHERE rollout_id=?1 ORDER BY source_line")
        .unwrap()
        .query_map([owner], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(messages, vec![actual_prompt.to_owned()]);
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND kind='message'",
                [owner],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE rollout_id=?1",
                [owner],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE rollout_id=?1 AND id LIKE '%:legacy-turn:%'",
                [owner],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn canonical_activity_suppresses_transport_context_and_abort_envelopes() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:00.001Z", turn),
            serde_json::json!({"timestamp":"2026-07-15T09:00:00.002Z","type":"response_item","payload":{
                "type":"message","role":"developer","content":[{
                    "type":"input_text","text":"Injected developer context."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:00.003Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":"# AGENTS.md instructions\nInjected runtime context."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:00.004Z","type":"response_item","payload":{
                "type":"ghost_snapshot","snapshot":{"checkpoint":"internal"}
            }}),
            context("2026-07-15T09:00:01.001Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.000Z","type":"response_item","payload":{
                "type":"message","id":"user-canonical","role":"user","content":[{
                    "type":"input_text","text":"Build the faithful projector."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.014Z","type":"event_msg","payload":{
                "type":"user_message","message":"Build the faithful projector."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"response_item","payload":{
                "type":"message","id":"assistant-canonical","role":"assistant","phase":"final_answer",
                "content":[{"type":"output_text","text":"The projector is ready. [citation metadata]"}]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.011Z","type":"event_msg","payload":{
                "type":"agent_message","message":"The projector is ready."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04.000Z","type":"event_msg","payload":{
                "type":"dynamic_tool_call_request","call_id":"dynamic-1","tool":"dynamic_tool"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04.001Z","type":"response_item","payload":{
                "type":"custom_tool_call","call_id":"dynamic-1","name":"dynamic_tool","input":"{}"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05.000Z","type":"event_msg","payload":{
                "type":"view_image_tool_call","call_id":"image-view-1","path":"/tmp/example.png"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05.001Z","type":"response_item","payload":{
                "type":"function_call","call_id":"image-view-1","name":"view_image",
                "arguments":"{\"path\":\"/tmp/example.png\"}"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:06Z","type":"event_msg","payload":{
                "type":"item_completed","item":{"type":"Plan","text":"Inspect, implement, verify."}
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:07Z","type":"event_msg","payload":{
                "type":"entered_review_mode"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:08Z","type":"event_msg","payload":{
                "type":"exited_review_mode"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:09Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":"<turn_aborted>Interrupted.</turn_aborted>"
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:09Z","type":"event_msg","payload":{
                "type":"turn_aborted","turn_id":turn,"reason":"interrupted"
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let title: String = connection
        .query_row("SELECT title FROM threads WHERE id=?1", [owner], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(title, "Build the faithful projector.");
    let turns: Vec<(String, String)> = connection
        .prepare("SELECT id,status FROM turns WHERE rollout_id=?1 ORDER BY id")
        .unwrap()
        .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(turns, vec![(turn.into(), "interrupted".into())]);
    let messages: Vec<String> = connection
        .prepare("SELECT content FROM messages WHERE rollout_id=?1 ORDER BY timestamp")
        .unwrap()
        .query_map([owner], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        messages,
        vec![
            "Build the faithful projector.".to_owned(),
            "The projector is ready. [citation metadata]".to_owned(),
        ]
    );
    let noise: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND (
                    label IN ('ghost_snapshot','dynamic_tool_call_request','view_image_tool_call')
                    OR label='Assistant update')",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(noise, 0);
    let tool_calls: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tool_calls WHERE rollout_id=?1",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tool_calls, 2);
    let plan: (String, String, Option<String>) = connection
        .query_row(
            "SELECT body,status,payload_json FROM events
                 WHERE rollout_id=?1 AND kind='plan'",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(plan.0, "Inspect, implement, verify.");
    assert_eq!(plan.1, "completed");
    assert!(plan.2.is_none());
    let review_states: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND kind='state'
                 AND label IN ('Entered review mode','Exited review mode')",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(review_states, 2);
}

#[test]
fn canonical_conversation_records_replace_only_the_matching_legacy_activity() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.000Z","type":"event_msg","payload":{
                "type":"agent_message","message":"Working now."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.100Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"commentary","content":[{
                    "type":"output_text","text":"Working now. More detail."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"Transport thought."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.100Z","type":"response_item","payload":{
                "type":"reasoning","summary":[{
                    "type":"summary_text","text":"Canonical thought."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04.500Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"Distinct later thought."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let updates = connection
        .prepare(
            "SELECT source_line,label,body FROM events
                 WHERE rollout_id=?1 AND kind='update' ORDER BY source_line",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        updates,
        vec![(5, None, Some("Working now. More detail.".into()))]
    );
    let reasoning = connection
        .prepare(
            "SELECT source_line,label,body FROM events
                 WHERE rollout_id=?1 AND kind='reasoning' ORDER BY source_line",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        reasoning,
        vec![
            (
                7,
                Some("Reasoning summary".into()),
                Some("Canonical thought.".into()),
            ),
            (
                8,
                Some("Reasoning".into()),
                Some("Distinct later thought.".into()),
            ),
        ]
    );
}

#[test]
fn terminal_tool_state_survives_late_output_and_completion_before_start() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.000Z","type":"response_item","payload":{
                "type":"function_call","call_id":"exec-reverse","name":"exec_command",
                "arguments":"{\"cmd\":\"git bad-command\"}"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.100Z","type":"event_msg","payload":{
                "type":"exec_command_end","call_id":"exec-reverse","exit_code":128,"status":"failed",
                "duration":{"secs":0,"nanos":7000000},"aggregated_output":"secondary failure output"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02.101Z","type":"response_item","payload":{
                "type":"function_call_output","call_id":"exec-reverse",
                "output":"canonical failure output: Process exited with code 128"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"event_msg","payload":{
                "type":"image_generation_end","call_id":"image-reverse","status":"generating",
                "duration":{"secs":0,"nanos":42000000},"result":"generated image"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.002Z","type":"response_item","payload":{
                "type":"image_generation_call","id":"image-reverse","status":"generating"
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let tools: Vec<(String, String, String, i64)> = connection
        .prepare(
            "SELECT call_id,name,status,duration_ms FROM tool_calls
                 WHERE rollout_id=?1 ORDER BY call_id",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        tools,
        vec![
            (
                "exec-reverse".into(),
                "exec_command".into(),
                "failed".into(),
                7
            ),
            (
                "image-reverse".into(),
                "image_generation_call".into(),
                "completed".into(),
                42
            ),
        ]
    );
}

#[test]
fn terminal_tool_matching_prefers_exact_id_then_latest_open_without_rewriting_event_identity() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let tool_start = |timestamp: &str, call_id: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
            "type":"function_call","call_id":call_id,"name":"apply_patch","arguments":"*** Begin Patch"
        }})
    };
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            tool_start("2026-07-15T09:00:02Z", "exact-open"),
            tool_start("2026-07-15T09:00:03Z", "newest-open"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                "type":"patch_apply_end","call_id":"exact-open","status":"completed",
                "duration":{"secs":0,"nanos":4000000}
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"event_msg","payload":{
                "type":"patch_apply_end","call_id":"source-fallback","status":"completed",
                "duration":{"secs":0,"nanos":5000000}
            }}),
            tool_start("2026-07-15T09:00:06Z", "still-running"),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let tools = connection
        .prepare(
            "SELECT call_id,status,completed_at,duration_ms FROM tool_calls
                 WHERE rollout_id=?1 ORDER BY call_id",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        tools,
        vec![
            (
                "exact-open".into(),
                "completed".into(),
                Some("2026-07-15T09:00:04.000000000Z".into()),
                Some(4),
            ),
            (
                "newest-open".into(),
                "completed".into(),
                Some("2026-07-15T09:00:05.000000000Z".into()),
                Some(5),
            ),
            ("still-running".into(), "running".into(), None, None),
        ]
    );
    let terminal_events = connection
        .prepare(
            "SELECT source_line,kind,call_id FROM events
                 WHERE rollout_id=?1 AND source_line IN (6,7) ORDER BY source_line",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        terminal_events,
        vec![
            (6, "tool_completed".into(), Some("exact-open".into())),
            (7, "tool_completed".into(), Some("source-fallback".into()),),
        ]
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE rollout_id=?1 AND call_id='source-fallback'",
                [owner],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn nested_settings_and_terminal_tool_metadata_are_projected_without_duplicates() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-old"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"thread_settings_applied","thread_settings":{
                    "model":"gpt-nested","reasoning_effort":"xhigh"
                }
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"I need to inspect the projector."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03.001Z","type":"response_item","payload":{
                "type":"reasoning","id":"reason-1","summary":[{
                    "type":"summary_text","text":"Inspect the projector."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                "type":"agent_reasoning","text":"A standalone legacy thought."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"response_item","payload":{
                "type":"function_call","call_id":"call-exec","name":"exec_command","arguments":"{\"cmd\":\"false\"}"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05.100Z","type":"response_item","payload":{
                "type":"function_call_output","call_id":"call-exec","output":"canonical exec output"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05.200Z","type":"event_msg","payload":{
                "type":"exec_command_end","call_id":"call-exec","exit_code":1,
                "duration":{"secs":0,"nanos":1500000},"aggregated_output":"secondary exec output"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:06Z","type":"response_item","payload":{
                "type":"custom_tool_call","call_id":"call-dynamic","name":"dynamic_tool","input":"{}"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:06.100Z","type":"response_item","payload":{
                "type":"custom_tool_call_output","call_id":"call-dynamic","output":"canonical dynamic output"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:06.200Z","type":"event_msg","payload":{
                "type":"dynamic_tool_call_response","call_id":"call-dynamic","tool":"dynamic_tool",
                "success":false,"error":"boom","duration":{"secs":0,"nanos":62030375}
            }}),
            usage("2026-07-15T09:00:07Z", 100),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let usage_projection: (String, String) = connection
        .query_row(
            "SELECT model,effort FROM usage_facts WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(usage_projection, ("gpt-nested".into(), "xhigh".into()));
    let turn_projection: (String, String) = connection
        .query_row(
            "SELECT model,effort FROM turns WHERE id=?1",
            [turn],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(turn_projection, ("gpt-nested".into(), "xhigh".into()));

    let tools = connection
        .prepare(
            "SELECT call_id,status,duration_ms FROM tool_calls
                 WHERE rollout_id=?1 ORDER BY call_id",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        tools,
        vec![
            ("call-dynamic".into(), "failed".into(), 63,),
            ("call-exec".into(), "failed".into(), 2,),
        ]
    );
    let reasoning: Vec<(String, String)> = connection
        .prepare(
            "SELECT label,body FROM events WHERE rollout_id=?1 AND kind='reasoning'
                 ORDER BY timestamp,source_line",
        )
        .unwrap()
        .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        reasoning,
        vec![
            ("Reasoning summary".into(), "Inspect the projector.".into()),
            ("Reasoning".into(), "A standalone legacy thought.".into()),
        ]
    );
}

#[test]
fn goal_heartbeats_collapse_to_meaningful_lifecycle_changes() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let goal = |timestamp: &str, status: &str, tokens: u64, seconds: u64| {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"thread_goal_updated","threadId":owner,"turnId":turn,"goal":{
                "threadId":owner,"objective":"Build faithful ingestion.","status":status,
                "tokensUsed":tokens,"timeUsedSeconds":seconds
            }
        }})
    };
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            goal("2026-07-15T09:00:02Z", "active", 100, 10),
            goal("2026-07-15T09:00:03Z", "active", 200, 20),
            goal("2026-07-15T09:00:04Z", "active", 300, 30),
            goal("2026-07-15T09:00:05Z", "complete", 400, 40),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let goals: Vec<(String, String, Option<String>)> = connection
        .prepare(
            "SELECT body,status,payload_json FROM events
                 WHERE thread_id=?1 AND kind='goal' ORDER BY timestamp,source_line",
        )
        .unwrap()
        .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(goals.len(), 2);
    assert_eq!(goals[0].0, "Build faithful ingestion.");
    assert_eq!(goals[0].1, "active");
    assert!(goals[0].2.is_none());
    assert_eq!(goals[1].0, "Build faithful ingestion.");
    assert_eq!(goals[1].1, "complete");
    assert!(goals[1].2.is_none());
}

#[test]
fn compaction_projection_keeps_summary_and_order_without_replacement_history() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let sentinel = "raw-replacement-history".repeat(25_000);
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"agent_message","message":"Before compaction."
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"compacted","payload":{
                "message":"  Handoff: continue with the verified plan.  ",
                "replacement_history":[sentinel,{"role":"assistant","content":"raw only"}],
                "window_number":2,"first_window_id":"window-1",
                "previous_window_id":"window-1","window_id":"window-2"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                "type":"agent_message","message":"After compaction."
            }}),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();

    let connection = db.connect().unwrap();
    let events = connection
        .prepare(
            "SELECT kind,body,COALESCE(payload_json,'') FROM events
                 WHERE thread_id=?1 AND kind IN ('update','compaction')
                 ORDER BY timestamp,source_line",
        )
        .unwrap()
        .query_map([owner], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.0.as_str())
            .collect::<Vec<_>>(),
        vec!["update", "compaction", "update"]
    );
    assert_eq!(events[1].1, "Handoff: continue with the verified plan.");
    assert!(!events[1].2.contains("raw-replacement-history"));
    assert!(events[1].2.len() < 256);
    let metadata: Value = serde_json::from_str(&events[1].2).unwrap();
    assert_eq!(metadata["replacement_history_count"], 2);
    assert_eq!(metadata["window_number"], 2);
    assert_eq!(metadata["first_window_id"], "window-1");
    assert_eq!(metadata["previous_window_id"], "window-1");
    assert_eq!(metadata["window_id"], "window-2");
    assert!(metadata.get("replacement_history").is_none());
}
