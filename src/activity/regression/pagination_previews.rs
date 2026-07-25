#![cfg(test)]

use super::*;

#[test]
fn legacy_activity_page_decodes_only_selected_message_bodies() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('bounded-legacy','Bounded legacy',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('bounded-legacy','bounded-legacy',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES(
                'bounded-legacy-first','bounded-legacy','bounded-legacy',
                '2026-07-01T00:00:00.000000000Z','user','First request',1
             );",
        )
        .unwrap();
    for index in 0..200_i64 {
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES(?1,'bounded-legacy','bounded-legacy',?2,'user',?3,?4)",
                params![
                    format!("bounded-legacy-unselected-{index:03}"),
                    format!("2026-07-01T00:05:{:02}.{:03}Z", index / 10, index % 10),
                    rusqlite::types::Value::Blob(vec![0x80]),
                    index + 2,
                ],
            )
            .unwrap();
    }
    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES(
                'bounded-legacy-latest','bounded-legacy','bounded-legacy',
                '2026-07-01T00:10:00.000000000Z','assistant','Latest answer',?1
             )",
            [202_i64],
        )
        .unwrap();

    let detail =
        query_activity_detail_page_on(&connection, "bounded-legacy", "legacy:bounded-legacy", 1, 1)
            .unwrap()
            .unwrap();
    assert_eq!(detail.child_total, Some(202));
    assert_eq!(detail.children.len(), 1);
    assert_eq!(
        detail.children[0].id,
        "legacy-message:bounded-legacy-latest"
    );
    assert_eq!(detail.children[0].body.as_deref(), Some("Latest answer"));
}

#[test]
fn legacy_message_previews_are_bounded_and_preserve_wrapped_requests() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('large-legacy','Large legacy',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('large-legacy','large-legacy',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z',0);",
        )
        .unwrap();
    let wrapped_user = format!(
        "# Applications mentioned by the user:\n{}\n\n## My request for Codex:\nKeep the tail request visible",
        "context ".repeat(ACTIVITY_MESSAGE_PARSE_BYTES as usize)
    );
    let assistant = "🙂".repeat(ACTIVITY_MESSAGE_PARSE_BYTES as usize * 2);
    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES('large-legacy-user','large-legacy','large-legacy',
                      '2026-07-01T00:00:00.000000000Z','user',?1,1)",
            [&wrapped_user],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES('large-legacy-assistant','large-legacy','large-legacy',
                      '2026-07-01T00:01:00.000000000Z','assistant',?1,2)",
            [&assistant],
        )
        .unwrap();

    let items = query_legacy_message_child_rows(
        &connection,
        "large-legacy",
        &[
            "large-legacy-user".to_owned(),
            "large-legacy-assistant".to_owned(),
        ],
    )
    .unwrap();
    assert_eq!(
        items["large-legacy-user"].body.as_deref(),
        Some("Keep the tail request visible")
    );
    let assistant_preview = items["large-legacy-assistant"].body.as_deref().unwrap();
    assert_eq!(
        assistant_preview.chars().count(),
        ACTIVITY_PREVIEW_CHARS as usize + 1
    );
    assert!(assistant_preview.ends_with('…'));
    assert!(
        assistant_preview.len()
            <= ACTIVITY_PREVIEW_CHARS as usize * char::MAX.len_utf8() + '…'.len_utf8()
    );
}

#[test]
fn legacy_activity_root_previews_read_only_bounded_message_edges() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('large-legacy-root','Large legacy root',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:02:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('large-legacy-root','large-legacy-root',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:02:00.000000000Z',0);",
        )
        .unwrap();

    let body_bytes = ACTIVITY_MESSAGE_PARSE_BYTES as usize * 4;
    let mut context_only = vec![b'x'; body_bytes];
    let context_prefix = b"# Applications mentioned by the user:\n";
    context_only[..context_prefix.len()].copy_from_slice(context_prefix);
    context_only[body_bytes / 2] = 0x80;

    let mut wrapped_request = vec![b'y'; body_bytes];
    wrapped_request[..context_prefix.len()].copy_from_slice(context_prefix);
    wrapped_request[body_bytes / 2] = 0x80;
    let request_suffix = b"\n\n## My request for Codex:\nKeep the bounded root request";
    wrapped_request[body_bytes - request_suffix.len()..].copy_from_slice(request_suffix);

    let mut assistant = vec![b'z'; body_bytes];
    let assistant_prefix = b"Latest data:image/png;base64,ZmFrZQ== answer ";
    assistant[..assistant_prefix.len()].copy_from_slice(assistant_prefix);
    assistant[body_bytes / 2] = 0x80;

    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES('large-legacy-context','large-legacy-root','large-legacy-root',
                      '2026-07-01T00:00:00.000000000Z','user',?1,1)",
            [context_only],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES('large-legacy-request','large-legacy-root','large-legacy-root',
                      '2026-07-01T00:01:00.000000000Z','user',?1,2)",
            [wrapped_request],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES('large-legacy-answer','large-legacy-root','large-legacy-root',
                      '2026-07-01T00:02:00.000000000Z','assistant',?1,3)",
            [assistant],
        )
        .unwrap();

    let item = query_legacy_activity_item(&connection, "large-legacy-root", "large-legacy-root")
        .unwrap()
        .unwrap();
    assert_eq!(item.label.as_deref(), Some("Keep the bounded root request"));
    let answer = item.body.as_deref().unwrap();
    assert!(answer.starts_with("Latest [embedded attachment] answer"));
    assert!(!answer.contains("data:image"));
    assert_eq!(answer.chars().count(), ACTIVITY_PREVIEW_CHARS as usize + 1);
    assert!(answer.ends_with('…'));
}

#[test]
fn modern_activity_root_previews_read_bounded_edges() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    seed_activity_roots(&connection, "large-modern", 1);
    let wrapped_user = format!(
        "# Applications mentioned by the user:\n{}\n\n## My request for Codex:\nKeep the bounded modern tail",
        "context ".repeat(ACTIVITY_MESSAGE_PARSE_BYTES as usize)
    );
    connection
        .execute(
            "UPDATE events SET body=?1 WHERE thread_id='large-modern' AND id='user-0'",
            [&wrapped_user],
        )
        .unwrap();

    let response = query_activity_on(&connection, "large-modern", 1, 1).unwrap();
    assert_eq!(response.items.len(), 1);
    assert_eq!(
        response.items[0].label.as_deref(),
        Some("Keep the bounded modern tail")
    );
}

#[test]
fn synthetic_group_page_decodes_only_selected_turn_bodies() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('bounded-group','Bounded group',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('bounded-group','bounded-group',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('bounded-group-root','bounded-group','bounded-group',
                    '2026-07-01T00:00:00.000000000Z','completed');
             INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
             ) VALUES('bounded-group-agent','bounded-group','bounded-group','bounded-group',
                      '2026-07-01T00:01:00.000000000Z',
                      '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
             ) VALUES('bounded-group-agent','bounded-group','bounded-group-agent',
                      'bounded-group','Bounded agent',
                      '2026-07-01T00:01:00.000000000Z','completed');",
        )
        .unwrap();
    for index in 0..200_i64 {
        connection
            .execute(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
                 ) VALUES(?1,'bounded-group','bounded-group-agent','bounded-group-agent',
                          ?2,'completed',?3)",
                params![
                    format!("bounded-group-unselected-{index:03}"),
                    format!("2026-07-01T00:05:{:02}.{:03}Z", index / 10, index % 10),
                    rusqlite::types::Value::Blob(vec![0x80]),
                ],
            )
            .unwrap();
    }
    connection
        .execute(
            "INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
             ) VALUES('bounded-group-latest','bounded-group','bounded-group-agent',
                      'bounded-group-agent','2026-07-01T00:10:00.000000000Z',
                      'completed','Latest child')",
            [],
        )
        .unwrap();

    let detail = query_activity_detail_page_on(
        &connection,
        "bounded-group",
        "group:agents:bounded-group-root",
        1,
        1,
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.child_total, Some(201));
    assert_eq!(detail.children.len(), 1);
    assert_eq!(detail.children[0].id, "bounded-group-latest");
    assert_eq!(detail.children[0].body.as_deref(), Some("Latest child"));

    let root =
        query_activity_detail_page_on(&connection, "bounded-group", "bounded-group-root", 1, 1)
            .unwrap()
            .unwrap();
    let group = root
        .children
        .iter()
        .find(|child| child.kind == "agent_group")
        .expect("root detail must retain the lazy agent-group placeholder");
    assert!(group.children.is_empty());
    assert_eq!(group.child_total, Some(201));
}

#[test]
fn legacy_activity_cursor_is_stable_across_inserts_and_seeks_deep() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('legacy-cursor','Legacy cursor',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('legacy-cursor','legacy-cursor',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z',0);
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<2047
             )
             INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             )
             SELECT printf('legacy-cursor-message-%04d',value),
                    'legacy-cursor','legacy-cursor',
                    printf('2026-07-01T00:00:00.%09dZ',value),
                    'assistant',printf('Message %d',value),value+1
             FROM sequence;",
        )
        .unwrap();

    let item_id = "legacy:legacy-cursor";
    let first =
        query_activity_detail_cursor_page_on(&connection, "legacy-cursor", item_id, 1, 2, None)
            .unwrap()
            .unwrap();
    assert_eq!(
        first
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        [
            "legacy-message:legacy-cursor-message-2047",
            "legacy-message:legacy-cursor-message-2046"
        ]
    );
    let first_cursor = first.child_next_cursor.unwrap();
    let numeric_second = query_activity_detail_page_on(&connection, "legacy-cursor", item_id, 2, 2)
        .unwrap()
        .unwrap();
    let cursor_second = query_activity_detail_cursor_page_on(
        &connection,
        "legacy-cursor",
        item_id,
        2,
        2,
        Some(&first_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        cursor_second
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        numeric_second
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>()
    );

    connection
        .execute(
            "INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES('legacy-cursor-newer','legacy-cursor','legacy-cursor',
                      '2026-07-01T00:00:00.999999999Z','assistant','Newer',3000)",
            [],
        )
        .unwrap();
    let after_insert = query_activity_detail_cursor_page_on(
        &connection,
        "legacy-cursor",
        item_id,
        2,
        2,
        Some(&first_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        after_insert
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        [
            "legacy-message:legacy-cursor-message-2045",
            "legacy-message:legacy-cursor-message-2044"
        ],
        "a newer insertion must not repeat or displace older cursor results"
    );

    let deep_cursor = encode_activity_collection_cursor(
        "legacy-cursor",
        item_id,
        "2026-07-01T00:00:00.000000010Z",
        Some(11),
        "legacy-message:legacy-cursor-message-0010",
    )
    .unwrap();
    let deep = query_activity_detail_cursor_page_on(
        &connection,
        "legacy-cursor",
        item_id,
        2040,
        1,
        Some(&deep_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        deep.children[0].id,
        "legacy-message:legacy-cursor-message-0009"
    );
}

#[test]
fn legacy_activity_orders_equal_timestamps_by_source_line() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('legacy-source-order','Legacy source order',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:00:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('legacy-source-order','legacy-source-order',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:00:00.000000000Z',0);
             INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
             ) VALUES
                ('legacy-source-z','legacy-source-order','legacy-source-order',
                 '2026-07-01T00:00:00.000000000Z','assistant','Earlier line',1),
                ('legacy-source-a','legacy-source-order','legacy-source-order',
                 '2026-07-01T00:00:00.000000000Z','assistant','Later line',2);",
        )
        .unwrap();

    let item_id = "legacy:legacy-source-order";
    let first = query_activity_detail_cursor_page_on(
        &connection,
        "legacy-source-order",
        item_id,
        1,
        1,
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(first.children[0].id, "legacy-message:legacy-source-a");
    let cursor = first.child_next_cursor.unwrap();
    let decoded =
        decode_activity_collection_cursor_for(&cursor, "legacy-source-order", item_id).unwrap();
    assert_eq!(decoded.source_line, Some(2));
    let second = query_activity_detail_cursor_page_on(
        &connection,
        "legacy-source-order",
        item_id,
        2,
        1,
        Some(&cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(second.children[0].id, "legacy-message:legacy-source-z");

    let old_cursor = serde_json::json!({
        "version": 1,
        "threadId": "legacy-source-order",
        "itemId": item_id,
        "timestamp": "2026-07-01T00:00:00.000000000Z",
        "sortId": "legacy-message:legacy-source-z"
    })
    .to_string();
    let from_old_cursor = query_activity_detail_cursor_page_on(
        &connection,
        "legacy-source-order",
        item_id,
        2,
        1,
        Some(&old_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        from_old_cursor.children[0].id,
        "legacy-message:legacy-source-a"
    );
}

#[test]
fn synthetic_group_cursor_is_stable_across_inserts_and_seeks_deep() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('group-cursor','Group cursor',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('group-cursor','group-cursor',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('group-cursor-root','group-cursor','group-cursor',
                    '2026-07-01T00:00:00.000000000Z','completed');
             INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
             ) VALUES('group-cursor-agent','group-cursor','group-cursor','group-cursor',
                      '2026-07-01T00:00:00.000000000Z',
                      '2026-07-01T00:01:00.000000000Z',0);
             INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
             ) VALUES('group-cursor-agent','group-cursor','group-cursor-agent',
                      'group-cursor','Cursor agent',
                      '2026-07-01T00:00:00.000000000Z','completed');
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<2047
             )
             INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
             )
             SELECT printf('group-cursor-turn-%04d',value),
                    'group-cursor','group-cursor-agent','group-cursor-agent',
                    printf('2026-07-01T00:00:00.%09dZ',value),
                    'completed',printf('Child %d',value)
             FROM sequence;",
        )
        .unwrap();

    let item_id = "group:agents:group-cursor-root";
    let first =
        query_activity_detail_cursor_page_on(&connection, "group-cursor", item_id, 1, 2, None)
            .unwrap()
            .unwrap();
    assert_eq!(
        first
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["group-cursor-turn-2047", "group-cursor-turn-2046"]
    );
    let first_cursor = first.child_next_cursor.unwrap();
    let numeric_second = query_activity_detail_page_on(&connection, "group-cursor", item_id, 2, 2)
        .unwrap()
        .unwrap();
    let cursor_second = query_activity_detail_cursor_page_on(
        &connection,
        "group-cursor",
        item_id,
        2,
        2,
        Some(&first_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        cursor_second
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        numeric_second
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>()
    );

    connection
        .execute(
            "INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
             ) VALUES('group-cursor-newer','group-cursor','group-cursor-agent',
                      'group-cursor-agent','2026-07-01T00:00:00.999999999Z',
                      'completed','Newer')",
            [],
        )
        .unwrap();
    let after_insert = query_activity_detail_cursor_page_on(
        &connection,
        "group-cursor",
        item_id,
        2,
        2,
        Some(&first_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        after_insert
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["group-cursor-turn-2045", "group-cursor-turn-2044"],
        "a newer group turn must not repeat or displace older cursor results"
    );

    let deep_cursor = encode_activity_collection_cursor(
        "group-cursor",
        item_id,
        "2026-07-01T00:00:00.000000010Z",
        None,
        "group-cursor-turn-0010",
    )
    .unwrap();
    let deep = query_activity_detail_cursor_page_on(
        &connection,
        "group-cursor",
        item_id,
        2040,
        1,
        Some(&deep_cursor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(deep.children[0].id, "group-cursor-turn-0009");
}

#[test]
fn synthetic_group_totals_saturate_after_per_turn_sql_groups() {
    const MAX_ROLLUP_TOKENS: u64 = 9_007_199_254_740_991;
    const CHILDREN: u64 = 1_025;

    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('group-overflow','Group overflow',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T01:00:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('group-overflow','group-overflow',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T01:00:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('group-overflow-root','group-overflow','group-overflow',
                    '2026-07-01T00:00:00.000000000Z','completed');
             INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
             ) VALUES('group-overflow-agent','group-overflow','group-overflow','group-overflow',
                      '2026-07-01T00:01:00.000000000Z',
                      '2026-07-01T01:00:00.000000000Z',0);
             INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
             ) VALUES('group-overflow-agent','group-overflow','group-overflow-agent',
                      'group-overflow','Overflow agent',
                      '2026-07-01T00:01:00.000000000Z','completed');
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<1024
             )
             INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,status
             )
             SELECT printf('group-overflow-turn-%04d',value),
                    'group-overflow','group-overflow-agent','group-overflow-agent',
                    printf('2026-07-01T00:01:00.%09dZ',value),'completed'
             FROM sequence;
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<1024
             )
             INSERT INTO usage_activity_rollups(
                thread_id,rollout_id,turn_key,activity_hour,model,fact_count,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
             )
             SELECT 'group-overflow','group-overflow-agent',
                    printf('group-overflow-turn-%04d',value),
                    '2026-07-01T00:00:00.000000000Z','overflow-unpriced',1,
                    9007199254740991,0,0,0,9007199254740991
             FROM sequence;",
        )
        .unwrap();

    let detail = query_activity_detail_page_on(
        &connection,
        "group-overflow",
        "group:agents:group-overflow-root",
        1,
        1,
    )
    .unwrap()
    .unwrap();
    let totals = detail.usage.unwrap();
    assert_eq!(totals.input_tokens, MAX_ROLLUP_TOKENS * CHILDREN);
    assert_eq!(totals.total_tokens, MAX_ROLLUP_TOKENS * CHILDREN);
    assert_eq!(totals.unpriced_tokens, MAX_ROLLUP_TOKENS * CHILDREN);
    assert!(totals.cost_usd.is_none());
}

#[test]
fn activity_child_page_is_turn_scoped_and_deduplicates_tool_lifecycles_deterministically() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('activity-child-scope','Child scope',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('activity-child-scope','activity-child-scope',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES
                ('selected-turn','activity-child-scope','activity-child-scope',
                 '2026-07-01T00:00:00.000000000Z','completed'),
                ('other-turn','activity-child-scope','activity-child-scope',
                 '2026-07-01T00:05:00.000000000Z','completed');
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,call_id,native
             ) VALUES
                ('tool-z','activity-child-scope','activity-child-scope','selected-turn',
                 '2026-07-01T00:00:01.000000000Z',10,'tool_call','selected-call',1),
                ('tool-b','activity-child-scope','activity-child-scope','selected-turn',
                 '2026-07-01T00:00:01.000000000Z',5,'tool_call','selected-call',1),
                ('tool-a','activity-child-scope','activity-child-scope','selected-turn',
                 '2026-07-01T00:00:01.000000000Z',5,'tool_call','selected-call',1);
             INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,turn_id,started_at,name,status
             ) VALUES(
                'selected-tool','selected-call','activity-child-scope',
                'activity-child-scope','selected-turn',
                '2026-07-01T00:00:01.000000000Z','exec','completed'
             );
             WITH RECURSIVE sequence(value) AS (
                SELECT 0
                UNION ALL
                SELECT value + 1 FROM sequence WHERE value + 1 < 64
             )
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,call_id,native
             )
             SELECT printf('other-tool-%02d',value),
                    'activity-child-scope','activity-child-scope','other-turn',
                    '2026-07-01T00:05:01.000000000Z',value + 100,'tool_call',
                    printf('other-call-%02d',value),1
             FROM sequence;",
        )
        .unwrap();

    let page = query_activity_child_previews_page(
        &connection,
        "activity-child-scope",
        "selected-turn",
        1,
        25,
    )
    .unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, "tool-a");
    assert_eq!(page.items[0].tool_name.as_deref(), Some("exec"));

    connection
        .execute_batch(
            "INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
             ) VALUES(
                'assistant-owner','activity-child-scope','activity-child-scope',
                'selected-turn','2026-07-01T00:00:02.000000000Z',6,
                'assistant','assistant','Scoped response',1
             );
             INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                total_tokens,native
             ) VALUES
                ('tool-usage','activity-child-scope','activity-child-scope','selected-turn',
                 '2026-07-01T00:00:02.100000000Z',6,'gpt-5.5',8,0,3,0,11,1),
                ('assistant-usage','activity-child-scope','activity-child-scope','selected-turn',
                 '2026-07-01T00:00:02.200000000Z',7,'gpt-5.5',17,0,5,0,22,1);",
        )
        .unwrap();

    let canonical_tool =
        query_activity_detail_page_on(&connection, "activity-child-scope", "tool-a", 1, 25)
            .unwrap()
            .unwrap();
    assert_eq!(canonical_tool.usage.unwrap().total_tokens, 11);

    let duplicate_tool =
        query_activity_detail_page_on(&connection, "activity-child-scope", "tool-b", 1, 25)
            .unwrap()
            .unwrap();
    assert!(duplicate_tool.usage.is_none());

    let assistant = query_activity_detail_page_on(
        &connection,
        "activity-child-scope",
        "assistant-owner",
        1,
        25,
    )
    .unwrap()
    .unwrap();
    assert_eq!(assistant.usage.unwrap().total_tokens, 22);
}

#[test]
fn activity_usage_ownership_is_independent_of_child_pagination() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('paged-usage-owner','Paged usage owner',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('paged-usage-owner','paged-usage-owner',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('paged-owner-turn','paged-usage-owner','paged-usage-owner',
                    '2026-07-01T00:00:00.000000000Z','completed');
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
             ) VALUES
                ('user-owner','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                 '2026-07-01T00:00:01.000000000Z',1,'user','user','Question',1),
                ('owner-a','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                 '2026-07-01T00:00:10.000000000Z',10,'assistant','assistant','A',1),
                ('owner-b','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                 '2026-07-01T00:00:11.000000000Z',11,'assistant','assistant','B',1);
             INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
             ) VALUES
                ('usage-after-user','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                 '2026-07-01T00:00:02.000000000Z',2,'gpt-5.5',0,0,7,0,7,1),
                ('usage-a','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                 '2026-07-01T00:00:11.100000000Z',11,'gpt-5.5',0,0,11,0,11,1),
                ('usage-b','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                 '2026-07-01T00:00:12.000000000Z',12,'gpt-5.5',0,0,22,0,22,1);",
        )
        .unwrap();

    let all = query_activity_child_previews_page(
        &connection,
        "paged-usage-owner",
        "paged-owner-turn",
        1,
        100,
    )
    .unwrap()
    .items;
    let usage = |id: &str| {
        all.iter()
            .find(|item| item.id == id)
            .and_then(|item| item.usage.as_ref())
            .map(|usage| usage.total_tokens)
            .unwrap()
    };
    assert!(
        all.iter()
            .find(|item| item.id == "user-owner")
            .unwrap()
            .usage
            .is_none(),
        "user messages must not own adjacent model usage"
    );
    assert_eq!(usage("owner-a"), 11);
    assert_eq!(usage("owner-b"), 22);

    let page_one = query_activity_child_previews_page(
        &connection,
        "paged-usage-owner",
        "paged-owner-turn",
        1,
        1,
    )
    .unwrap();
    let page_two = query_activity_child_previews_page(
        &connection,
        "paged-usage-owner",
        "paged-owner-turn",
        2,
        1,
    )
    .unwrap();
    assert_eq!(page_one.items[0].id, "owner-b");
    assert_eq!(page_one.items[0].usage.as_ref().unwrap().total_tokens, 22);
    assert_eq!(page_two.items[0].id, "owner-a");
    assert_eq!(page_two.items[0].usage.as_ref().unwrap().total_tokens, 11);

    let direct = query_activity_detail_on(&connection, "paged-usage-owner", "owner-a")
        .unwrap()
        .unwrap();
    assert_eq!(direct.usage.unwrap().total_tokens, 11);
    let direct_user = query_activity_detail_on(&connection, "paged-usage-owner", "user-owner")
        .unwrap()
        .unwrap();
    assert!(direct_user.usage.is_none());
}

#[tokio::test]
async fn activity_detail_rejects_malformed_and_wrong_scope_cursors() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('cursor-thread','Cursor thread',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('cursor-thread','cursor-thread',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status) VALUES
                ('cursor-turn-a','cursor-thread','cursor-thread',
                 '2026-07-01T00:00:00.000000000Z','completed'),
                ('cursor-turn-b','cursor-thread','cursor-thread',
                 '2026-07-01T00:01:00.000000000Z','completed');
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,
                kind,role,body,native
             ) VALUES
                ('cursor-event-a','cursor-thread','cursor-thread','cursor-turn-a',
                 '2026-07-01T00:00:01.000000000Z',1,
                 'assistant','assistant','First',1),
                ('cursor-event-b','cursor-thread','cursor-thread','cursor-turn-a',
                 '2026-07-01T00:00:02.000000000Z',2,
                 'assistant','assistant','Second',1);",
        )
        .unwrap();
    let first_page = query_activity_child_previews_cursor_page(
        &connection,
        "cursor-thread",
        "cursor-turn-a",
        1,
        1,
        None,
    )
    .unwrap();
    let cursor = first_page
        .next_cursor
        .expect("a two-row first page must expose a continuation cursor");
    drop(connection);

    let state = ReadRuntime::new(db, StorageExecutor::default());
    let malformed = session_activity_detail(
        State(state.clone()),
        AxumPath(("cursor-thread".into(), "cursor-turn-a".into())),
        Query(ActivityDetailQuery {
            child_page: Some(2),
            child_page_size: Some(1),
            child_cursor: Some("not a cursor".into()),
        }),
    )
    .await;
    let Err(malformed) = malformed else {
        panic!("malformed Activity cursor was accepted");
    };
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    let wrong_scope = session_activity_detail(
        State(state),
        AxumPath(("cursor-thread".into(), "cursor-turn-b".into())),
        Query(ActivityDetailQuery {
            child_page: Some(2),
            child_page_size: Some(1),
            child_cursor: Some(cursor),
        }),
    )
    .await;
    let Err(wrong_scope) = wrong_scope else {
        panic!("Activity cursor from another turn was accepted");
    };
    assert_eq!(wrong_scope.status(), StatusCode::BAD_REQUEST);
}
