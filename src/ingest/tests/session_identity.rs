#![cfg(test)]

use super::super::*;
use super::support::*;
use std::io::Write;

#[test]
fn session_index_title_wins_and_refreshes_without_rollout_reingestion() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let thread = "019f7000-0000-7000-8000-000000000001";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[meta("2026-07-16T08:00:00Z", thread, thread, false)],
    );
    std::fs::write(
            temp.path().join("session_index.jsonl"),
            format!(
                "{{\"id\":\"{thread}\",\"thread_name\":\"Newest real title\",\"updated_at\":\"2026-07-16T08:05:00Z\"}}\n\
                 {{\"id\":\"{thread}\",\"thread_name\":\"Older title later in file\",\"updated_at\":\"2026-07-16T08:04:00Z\"}}\n\
                 {{not-json\n"
            ),
        )
        .unwrap();

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions.clone()),
            archive: None,
        },
    )
    .unwrap();
    let connection = db.connect().unwrap();
    let first: String = connection
        .query_row("SELECT title FROM threads WHERE id=?1", [thread], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(first, "Newest real title");
    drop(connection);

    let mut index = std::fs::OpenOptions::new()
        .append(true)
        .open(temp.path().join("session_index.jsonl"))
        .unwrap();
    writeln!(
            index,
            "{{\"id\":\"{thread}\",\"thread_name\":\"Renamed while idle\",\"updated_at\":\"2026-07-16T08:06:00Z\"}}"
        )
        .unwrap();
    drop(index);

    let report = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    assert_eq!(report.files_ingested, 0);
    assert_eq!(report.files_unchanged, 1);
    let connection = db.connect().unwrap();
    let renamed: String = connection
        .query_row("SELECT title FROM threads WHERE id=?1", [thread], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(renamed, "Renamed while idle");
}

#[test]
fn session_index_equal_instant_prefers_later_line_and_normalizes_title() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let thread = "019f7000-0000-7000-8000-000000000003";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[meta("2026-07-16T08:00:00Z", thread, thread, false)],
    );
    let later_title = format!(
        "before data:image/png;base64,SESSION_INDEX_SECRET after {}",
        "z".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000)
    );
    std::fs::write(
        temp.path().join("session_index.jsonl"),
        format!(
            "{{\"id\":\"{thread}\",\"thread_name\":\"Earlier line\",\"updated_at\":\"2026-07-16T10:05:00+02:00\"}}\n\
             {{not-json\n\
             {}\n\
             {{\"id\":\"{thread}\",\"thread_name\":\"Incomplete tail\"",
            serde_json::json!({
                "id": thread,
                "thread_name": later_title,
                "updated_at": "2026-07-16T08:05:00Z"
            })
        ),
    )
    .unwrap();

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();

    let connection = db.connect().unwrap();
    let (title, updated_at): (String, String) = connection
        .query_row(
            "SELECT title,title_updated_at FROM threads WHERE id=?1",
            [thread],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let prefix = "before [embedded attachment] after ";
    let expected_title = format!(
        "{}{}…",
        prefix,
        "z".repeat(PROJECTED_EVENT_BODY_CHARS - prefix.chars().count())
    );
    assert_eq!(title, expected_title);
    assert_eq!(updated_at, "2026-07-16T08:05:00.000000000Z");
}

#[test]
fn session_index_candidates_are_scoped_to_configured_root_parents() {
    let temp = tempfile::tempdir().unwrap();
    let codex_root = temp.path().join("isolated-codex-home");
    let roots = IngestRoots {
        active: Some(codex_root.join("sessions")),
        archive: Some(codex_root.join("archived_sessions")),
    };

    assert_eq!(
        session_index_candidates(roots.active.as_deref(), roots.archive.as_deref()),
        vec![codex_root]
    );
}

#[test]
fn session_index_discovery_ignores_ambient_codex_home() {
    use std::process::Command;

    let temp = tempfile::tempdir().unwrap();
    let ambient = temp.path().join("ambient-codex-home");
    let configured = temp.path().join("isolated-corpus");
    std::fs::create_dir_all(configured.join("sessions")).unwrap();
    std::fs::create_dir_all(&ambient).unwrap();
    std::fs::write(ambient.join("session_index.jsonl"), b"ambient\n").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("ingest::tests::session_index_scope_child")
        .arg("--nocapture")
        .env("CODEX_HOME", &ambient)
        .env("HOME", temp.path().join("ambient-home"))
        .env("CODEX_USAGE_CONFIGURED_CORPUS", &configured)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn session_index_scope_child() {
    let Ok(configured) = std::env::var("CODEX_USAGE_CONFIGURED_CORPUS") else {
        return;
    };
    let roots = IngestRoots {
        active: Some(PathBuf::from(configured).join("sessions")),
        archive: None,
    };
    assert_eq!(
        discover_session_index(roots.active.as_deref(), roots.archive.as_deref()),
        None
    );
}

#[test]
fn session_meta_thread_name_precedes_prompt_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let thread = "019f7000-0000-7000-8000-000000000002";
    let turn = "019f7001-0000-7000-8000-000000000002";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            serde_json::json!({"timestamp":"2026-07-16T08:00:00Z","type":"session_meta","payload":{
                "id":thread,"session_id":thread,"cwd":"/tmp/project","source":"vscode",
                "thread_name":"Metadata title"
            }}),
            task("2026-07-16T08:00:01Z", turn),
            serde_json::json!({"timestamp":"2026-07-16T08:00:02Z","type":"event_msg","payload":{
                "type":"user_message","message":"A very long first prompt that is only a fallback"
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
        .query_row("SELECT title FROM threads WHERE id=?1", [thread], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(title, "Metadata title");
}

#[test]
fn fork_replay_prefix_is_excluded_until_owner_native_turn() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("fork.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let parent = "019df47e-0000-7000-8000-000000000000";
    let inherited_turn = "019df500-0000-7000-8000-000000000000";
    let inherited_legacy_turn = "392fc773-e404-46d6-8764-595914ed82f6";
    let native_turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &file,
        &[
            root_fork_meta("2026-07-15T09:00:00Z", owner, parent),
            meta("2026-07-15T09:00:00Z", parent, parent, false),
            task("2026-07-15T09:00:01Z", inherited_turn),
            context("2026-07-15T09:00:01Z", inherited_turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 2_753_402_716),
            task("2026-07-15T09:00:02.100Z", inherited_legacy_turn),
            context("2026-07-15T09:00:02.100Z", inherited_legacy_turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02.200Z", 2_900_000_000),
            task("2026-07-15T09:00:03Z", native_turn),
            root_fork_meta("2026-07-15T09:00:03Z", owner, parent),
            context("2026-07-15T09:00:03Z", native_turn, "gpt-5.5"),
            usage("2026-07-15T09:00:04Z", 41_000),
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
    let (count, input): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*),COALESCE(SUM(input_tokens),0) FROM usage_facts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(input, 41_000);
}

#[test]
fn source_timestamps_stay_inside_the_queryable_calendar_domain() {
    assert_eq!(
        canonical_source_timestamp("9998-12-31T23:59:59Z").unwrap(),
        "9998-12-31T23:59:59.000000000Z"
    );
    assert!(canonical_source_timestamp("1969-12-31T23:59:59Z").is_err());
    assert!(canonical_source_timestamp("9999-01-01T00:00:00Z").is_err());
}

#[test]
fn uuid7_boundary_validates_shape_and_uses_timestamp_not_random_suffix() {
    let owner = "019f64aa-ffff-7fff-bfff-ffffffffffff";
    let same_millisecond_turn = "019f64aa-ffff-7000-8000-000000000000";
    assert!(is_owner_native_turn(owner, same_millisecond_turn));
    assert!(is_owner_native_turn(
        owner,
        "019F64AA-FFFF-7000-8000-000000000000"
    ));
    assert!(!is_owner_native_turn(
        owner,
        "392fc773-e404-46d6-8764-595914ed82f6"
    ));
    assert!(!is_owner_native_turn(
        owner,
        "019f64ab-0000-7zzz-8000-000000000000"
    ));
    assert!(!is_owner_native_turn(
        owner,
        "019f64ab-0000-7000-0000-000000000000"
    ));
}

#[test]
fn legacy_child_without_session_id_groups_under_parent_thread() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let root = "019cc9e7-0000-7000-8000-000000000000";
    let child = "019cc9e9-0000-7000-8000-000000000000";
    let grandchild = "019cc9eb-0000-7000-8000-000000000000";
    let root_turn = "019cc9e8-0000-7000-8000-000000000000";
    let child_turn = "019cc9ea-0000-7000-8000-000000000000";
    let grandchild_turn = "019cc9ec-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("z-root.jsonl"),
        &[
            meta("2026-03-07T21:00:00Z", root, root, false),
            task("2026-03-07T21:00:01Z", root_turn),
            context("2026-03-07T21:00:01Z", root_turn, "gpt-5.5"),
            usage("2026-03-07T21:00:02Z", 100),
        ],
    );
    write_fixture(
        &sessions.join("m-child.jsonl"),
        &[
            legacy_child_meta("2026-03-07T21:07:53Z", child, root),
            task("2026-03-07T21:07:54Z", child_turn),
            context("2026-03-07T21:07:54Z", child_turn, "gpt-5.5"),
            usage("2026-03-07T21:07:55Z", 50),
        ],
    );
    write_fixture(
        &sessions.join("a-grandchild.jsonl"),
        &[
            legacy_child_meta("2026-03-07T21:08:53Z", grandchild, child),
            task("2026-03-07T21:08:54Z", grandchild_turn),
            context("2026-03-07T21:08:54Z", grandchild_turn, "gpt-5.5"),
            usage("2026-03-07T21:08:55Z", 25),
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
    let (threads, rollouts, usage_facts, input): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM threads),(SELECT COUNT(*) FROM rollouts),
                        (SELECT COUNT(*) FROM usage_facts),
                        (SELECT COALESCE(SUM(input_tokens),0) FROM usage_facts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!((threads, rollouts, usage_facts, input), (1, 3, 3, 175));
    let child_projection: (String, String, String) = connection
        .query_row(
            "SELECT r.thread_id,a.thread_id,COALESCE(a.nickname,'')
                 FROM rollouts r JOIN agent_runs a ON a.id=r.id WHERE r.id=?1",
            [child],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        child_projection,
        (root.into(), root.into(), "Ramanujan".into())
    );
    let grandchild_thread: String = connection
        .query_row(
            "SELECT thread_id FROM rollouts WHERE id=?1",
            [grandchild],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(grandchild_thread, root);
}

#[test]
fn imported_parent_does_not_absorb_a_top_level_root_fork() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let parent = "019df47e-0000-7000-8000-000000000000";
    let fork = "019f64aa-0000-7000-8000-000000000000";
    let parent_turn = "019df47f-0000-7000-8000-000000000000";
    let fork_turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("z-parent.jsonl"),
        &[
            meta("2026-05-04T21:00:00Z", parent, parent, false),
            task("2026-05-04T21:00:01Z", parent_turn),
            context("2026-05-04T21:00:01Z", parent_turn, "gpt-5.5"),
            usage("2026-05-04T21:00:02Z", 100),
        ],
    );
    write_fixture(
        &sessions.join("a-fork.jsonl"),
        &[
            root_fork_meta("2026-07-15T09:00:00Z", fork, parent),
            task("2026-07-15T09:00:01Z", fork_turn),
            context("2026-07-15T09:00:01Z", fork_turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 50),
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
    let threads: i64 = connection
        .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
        .unwrap();
    let fork_usage_thread: String = connection
        .query_row(
            "SELECT thread_id FROM usage_facts WHERE rollout_id=?1",
            [fork],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(threads, 2);
    assert_eq!(fork_usage_thread, fork);
}

#[test]
fn rename_and_abort_events_update_durable_thread_and_turn_state() {
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
                "type":"thread_name_updated","thread_name":"First title"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                "type":"thread_name_updated","thread_name":"Summarize last 10 emails"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
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
    let turn_state: (String, Option<String>) = connection
        .query_row(
            "SELECT status,completed_at FROM turns WHERE id=?1",
            [turn],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let agent_status: String = connection
        .query_row(
            "SELECT status FROM agent_runs WHERE id=?1",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(title, "Summarize last 10 emails");
    assert_eq!(turn_state.0, "interrupted");
    assert!(turn_state.1.is_some());
    assert_eq!(agent_status, "interrupted");
}

#[test]
fn final_assistant_message_completes_legacy_turn_without_task_complete() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let previous_turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", previous_turn),
            context("2026-07-15T09:00:01Z", previous_turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":"Start the first request."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                "type":"task_complete","turn_id":previous_turn,
                "last_agent_message":"First request complete."
            }}),
            // Some interleaved/legacy traces have no task_started or
            // turn_context for the follow-up. The projector creates a
            // stable synthetic turn from the user message's source line.
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":"Now generate the images."
                }]
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":"The five images are ready."
                }]
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
    let legacy_turn = format!("{owner}:legacy-turn:6");
    let state: (String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT status,completed_at,last_agent_message FROM turns WHERE id=?1",
            [&legacy_turn],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state.0, "completed");
    assert_eq!(state.1.as_deref(), Some("2026-07-15T09:00:05.000000000Z"));
    assert_eq!(state.2.as_deref(), Some("The five images are ready."));
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND kind='turn_completed'",
                [&legacy_turn],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn attachment_only_messages_keep_metadata_while_tool_payloads_are_omitted() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000099";
    let turn = "019f64ab-0000-7000-8000-000000000099";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:02Z",
                "type":"response_item",
                "payload":{
                    "type":"message",
                    "role":"user",
                    "content":[{
                        "type":"input_image",
                        "image_url":"data:image/png;base64,IMAGE_BASE64_SENTINEL"
                    }]
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:03Z",
                "type":"response_item",
                "payload":{
                    "type":"custom_tool_call",
                    "call_id":"metadata-only-call",
                    "name":"exec",
                    "input":"TOOL_INPUT_SENTINEL"
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:04Z",
                "type":"response_item",
                "payload":{
                    "type":"custom_tool_call_output",
                    "call_id":"metadata-only-call",
                    "output":"TOOL_OUTPUT_SENTINEL data:image/png;base64,TOOL_BASE64_SENTINEL"
                }
            }),
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
    let message: (String, String) = connection
        .query_row(
            "SELECT content,timestamp FROM messages WHERE thread_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(message.0, "[Attachment omitted]");
    assert_eq!(message.1, "2026-07-15T09:00:02.000000000Z");

    let tool: (String, String, String, String, Option<String>, Option<i64>) = connection
        .query_row(
            "SELECT call_id,name,status,started_at,completed_at,duration_ms
                 FROM tool_calls WHERE thread_id=?1",
            [owner],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(tool.0, "metadata-only-call");
    assert_eq!(tool.1, "exec");
    assert_eq!(tool.2, "completed");
    assert_eq!(tool.3, "2026-07-15T09:00:03.000000000Z");
    assert_eq!(tool.4.as_deref(), Some("2026-07-15T09:00:04.000000000Z"));
    assert_eq!(tool.5, None);

    let tool_payload_columns: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('tool_calls')
                 WHERE name IN ('input','output')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let message_payload_columns: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('messages')
                 WHERE name='content_json'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tool_payload_columns, 0);
    assert_eq!(message_payload_columns, 0);

    let retained_payload_sentinels: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM (
                    SELECT content AS value FROM messages
                    UNION ALL SELECT COALESCE(body,'') FROM events
                    UNION ALL SELECT COALESCE(payload_json,'') FROM events
                 ) WHERE value LIKE '%TOOL_INPUT_SENTINEL%'
                    OR value LIKE '%TOOL_OUTPUT_SENTINEL%'
                    OR value LIKE '%BASE64_SENTINEL%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_payload_sentinels, 0);
}

#[test]
fn embedded_data_urls_are_redacted_before_visible_text_and_json_are_persisted() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000098";
    let turn = "019f64ab-0000-7000-8000-000000000098";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:02Z",
                "type":"event_msg",
                "payload":{
                    "type":"user_message",
                    "message":"Inspect data:image/png;base64,TITLE_BASE64_SENTINEL please"
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:03Z",
                "type":"response_item",
                "payload":{
                    "type":"message",
                    "role":"user",
                    "content":[{
                        "type":"input_text",
                        "text":"Please inspect data:image/png;base64,MESSAGE_BASE64_SENTINEL now"
                    }]
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:04Z",
                "type":"event_msg",
                "payload":{
                    "type":"agent_reasoning",
                    "text":"Reasoning around data:image/png;base64,REASONING_BASE64_SENTINEL"
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:05Z",
                "type":"response_item",
                "payload":{
                    "type":"agent_message",
                    "author":"data:image/png;base64,LABEL_BASE64_SENTINEL",
                    "recipient":"parent",
                    "message":"Evidence data:image/png;base64,SUBAGENT_BASE64_SENTINEL"
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:06Z",
                "type":"event_msg",
                "payload":{
                    "type":"thread_goal_updated",
                    "goal":{
                        "objective":"Check data:image/png;base64,GOAL_BASE64_SENTINEL",
                        "evidence":{"image":"data:image/png;base64,PAYLOAD_BASE64_SENTINEL"}
                    }
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:07Z",
                "type":"event_msg",
                "payload":{
                    "type":"task_complete",
                    "turn_id":turn,
                    "last_agent_message":"Done data:image/png;base64,FINAL_BASE64_SENTINEL"
                }
            }),
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
    let message: String = connection
        .query_row(
            "SELECT content FROM messages WHERE thread_id=?1",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    let last_agent_message: String = connection
        .query_row(
            "SELECT last_agent_message FROM turns WHERE id=?1",
            [turn],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(title, "Inspect [embedded attachment] please");
    assert_eq!(message, "Please inspect [embedded attachment] now");
    assert_eq!(last_agent_message, "Done [embedded attachment]");

    let goal_payload: Option<String> = connection
        .query_row(
            "SELECT payload_json FROM events WHERE thread_id=?1 AND kind='goal'",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    assert!(goal_payload.is_none());

    let retained_data_urls: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM (
                    SELECT COALESCE(title,'') AS value FROM threads
                    UNION ALL SELECT content FROM messages
                    UNION ALL SELECT COALESCE(last_agent_message,'') FROM turns
                    UNION ALL SELECT COALESCE(label,'') FROM events
                    UNION ALL SELECT COALESCE(body,'') FROM events
                    UNION ALL SELECT COALESCE(tool_name,'') FROM events
                    UNION ALL SELECT COALESCE(payload_json,'') FROM events
                 ) WHERE lower(value) LIKE '%data:image%'
                    OR value LIKE '%BASE64_SENTINEL%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_data_urls, 0);
}

#[test]
fn session_metadata_is_normalized_at_discovery_and_update_boundaries() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000096";
    let embedded = "data:image/png;base64,METADATA_SENTINEL";
    let long_path = format!(
        "/tmp/{embedded} {}",
        "p".repeat(PROJECTED_SESSION_PATH_CHARS + 1_000)
    );
    let long_repository = format!(
        "{embedded} {}",
        "r".repeat(PROJECTED_SESSION_PATH_CHARS + 1_000)
    );
    let long_branch = format!(
        "{embedded} {}",
        "b".repeat(PROJECTED_IDENTIFIER_CHARS + 1_000)
    );
    let long_source = format!(
        "{embedded} {}",
        "s".repeat(PROJECTED_IDENTIFIER_CHARS + 1_000)
    );
    let long_thread_source = format!(
        "{embedded} {}",
        "t".repeat(PROJECTED_IDENTIFIER_CHARS + 1_000)
    );
    let long_title = format!(
        "{embedded} {}",
        "n".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000)
    );
    write_fixture(
        &sessions.join("metadata.jsonl"),
        &[
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:00Z",
                "type":"session_meta",
                "payload":{
                    "id":owner,
                    "session_id":owner,
                    "cwd":"/tmp/initial",
                    "source":"vscode",
                    "thread_source":"user",
                    "git":{
                        "repository_url":"https://example.test/initial",
                        "branch":"initial"
                    }
                }
            }),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:01Z",
                "type":"session_meta",
                "payload":{
                    "id":owner,
                    "session_id":owner,
                    "thread_name":long_title,
                    "cwd":long_path,
                    "source":long_source,
                    "thread_source":long_thread_source,
                    "git":{
                        "repository_url":long_repository,
                        "branch":long_branch
                    }
                }
            }),
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
    let metadata: (String, String, String, String, String, String, String) = connection
        .query_row(
            "SELECT title,cwd,project,repository_url,branch,source,thread_source
                 FROM threads WHERE id=?1",
            [owner],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap();
    assert!(metadata.0.chars().count() <= PROJECTED_EVENT_BODY_CHARS + 1);
    assert!(metadata.1.chars().count() <= PROJECTED_SESSION_PATH_CHARS + 1);
    assert!(metadata.2.chars().count() <= PROJECTED_EVENT_LABEL_CHARS + 1);
    assert!(metadata.3.chars().count() <= PROJECTED_SESSION_PATH_CHARS + 1);
    assert!(metadata.4.chars().count() <= PROJECTED_IDENTIFIER_CHARS + 1);
    assert!(metadata.5.chars().count() <= PROJECTED_IDENTIFIER_CHARS + 1);
    assert!(metadata.6.chars().count() <= PROJECTED_IDENTIFIER_CHARS + 1);
    for value in [
        &metadata.0,
        &metadata.1,
        &metadata.2,
        &metadata.3,
        &metadata.4,
        &metadata.5,
        &metadata.6,
    ] {
        assert!(!value.to_ascii_lowercase().contains("data:image"));
        assert!(!value.contains("METADATA_SENTINEL"));
    }

    let oversized_id = format!("{}{}", "i".repeat(PROJECTED_IDENTIFIER_CHARS), embedded);
    let owner_path = temp.path().join("owner-only.jsonl");
    write_fixture(
        &owner_path,
        &[serde_json::json!({
            "timestamp":"2026-07-15T09:00:00Z",
            "type":"session_meta",
            "payload":{
                "id":oversized_id.clone(),
                "session_id":oversized_id,
                "cwd":"/tmp/project",
                "source":{
                    "subagent":{
                        "thread_spawn":{
                            "parent_thread_id":format!("parent-{embedded}"),
                            "parent_rollout_id":format!("rollout-{embedded}"),
                            "agent_path":format!("/root/{embedded}"),
                            "agent_nickname":format!("nickname-{embedded}")
                        }
                    }
                }
            }
        })],
    );
    let error = read_owner(&owner_path).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeds the 256-character identifier limit"),
        "unexpected identifier error: {error:#}"
    );
}

#[test]
fn oversized_relational_identifiers_are_rejected_instead_of_colliding() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let shared_prefix = "i".repeat(PROJECTED_IDENTIFIER_CHARS);
    for (name, suffix) in [("a.jsonl", "-first"), ("b.jsonl", "-second")] {
        let owner = format!("{shared_prefix}{suffix}");
        write_fixture(
            &sessions.join(name),
            &[serde_json::json!({
                "timestamp":"2026-07-15T09:00:00Z",
                "type":"session_meta",
                "payload":{
                    "id":owner,
                    "session_id":owner,
                    "cwd":"/tmp/project",
                    "source":"vscode"
                }
            })],
        );
    }

    let error = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("exceeds the 256-character identifier limit"),
        "unexpected identifier error: {error:#}"
    );
    let connection = db.connect().unwrap();
    let projection: (i64, i64, i64) = connection
        .query_row(
            "SELECT
                    (SELECT COUNT(*) FROM threads),
                    (SELECT COUNT(*) FROM rollouts),
                    (SELECT COUNT(*) FROM source_files)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(projection, (0, 0, 0));
}

#[test]
fn oversized_turn_identifier_rolls_back_its_relational_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-0000000000f6";
    let turn = format!("{}-turn", "t".repeat(PROJECTED_IDENTIFIER_CHARS));
    write_fixture(
        &sessions.join("oversized-turn.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", &turn),
        ],
    );

    let error = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("turn id exceeds the 256-character identifier limit"),
        "unexpected identifier error: {error:#}"
    );
    let projection: (i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    (SELECT COUNT(*) FROM rollouts),
                    (SELECT COUNT(*) FROM turns),
                    (SELECT COUNT(*) FROM events),
                    (SELECT COUNT(*) FROM source_files)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(projection, (0, 0, 0, 0));
}

#[test]
fn lifecycle_metadata_is_allowlisted_bounded_and_kept_out_of_session_source_json() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000097";
    let turn = "019f64ab-0000-7000-8000-000000000097";
    let child = "019f64ac-0000-7000-8000-000000000097";
    let hostile = format!(
        "data:image/png;base64,HIDDEN_METADATA_SENTINEL{}",
        "x".repeat(200_000)
    );
    let long_goal = format!(
        "Keep this authored goal. {} {hostile}",
        "g".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000)
    );
    let long_plan = format!(
        "Keep this authored plan. {} {hostile}",
        "p".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000)
    );
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            serde_json::json!({"timestamp":"2026-07-15T09:00:00Z","type":"session_meta","payload":{
                "id":owner,"session_id":owner,"cwd":"/tmp/project",
                "source":{"kind":"cli","transport_blob":hostile}
            }}),
            task("2026-07-15T09:00:01Z", turn),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"sub_agent_activity","kind":"completed","agent_thread_id":child,
                "agent_path":"/root/reviewer","transport_blob":hostile
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                "type":"thread_goal_updated","goal":{"objective":long_goal,"status":"active"},
                "transport_blob":hostile
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                "type":"item_completed","item":{"type":"Plan","text":long_plan},
                "transport_blob":hostile
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"event_msg","payload":{
                "type":"entered_review_mode","transport_blob":hostile
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
    let source_json: Option<String> = connection
        .query_row(
            "SELECT source_json FROM threads WHERE id=?1",
            [owner],
            |row| row.get(0),
        )
        .unwrap();
    assert!(source_json.is_none());
    let events: Vec<(String, Option<String>, Option<String>)> = connection
        .prepare(
            "SELECT kind,body,payload_json FROM events
                 WHERE rollout_id=?1 AND kind IN ('subagent','goal','plan','state')
                 ORDER BY timestamp,source_line",
        )
        .unwrap()
        .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(events.len(), 4);
    let subagent_payload: Value = serde_json::from_str(events[0].2.as_deref().unwrap()).unwrap();
    assert_eq!(
        subagent_payload,
        serde_json::json!({"agent_thread_id": child})
    );
    for event in &events[1..] {
        assert!(event.2.is_none(), "{} payload must be omitted", event.0);
    }
    assert!(
        events[1]
            .1
            .as_deref()
            .unwrap()
            .starts_with("Keep this authored goal.")
    );
    assert!(
        events[2]
            .1
            .as_deref()
            .unwrap()
            .starts_with("Keep this authored plan.")
    );
    assert!(events[1].1.as_deref().unwrap().chars().count() <= PROJECTED_EVENT_BODY_CHARS + 1);
    assert!(events[2].1.as_deref().unwrap().chars().count() <= PROJECTED_EVENT_BODY_CHARS + 1);
    let retained_hostile: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM (
                    SELECT COALESCE(source_json,'') value FROM threads
                    UNION ALL SELECT COALESCE(payload_json,'') FROM events
                    UNION ALL SELECT COALESCE(body,'') FROM events
                 ) WHERE value LIKE '%HIDDEN_METADATA_SENTINEL%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_hostile, 0);
}
