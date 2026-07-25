use super::{
    attribution::{EventUsageOwner, event_totals_on},
    cursor::{decode_activity_collection_cursor_for, encode_activity_collection_cursor},
    index::{IndexedActivityEvent, query_page as query_activity_index_page},
    model::ActivityItem,
};
use crate::{
    conversation::display::{tool_name_for_display, user_request_for_display},
    redaction::redact_data_urls,
    usage::{TotalsScope, read_totals_on},
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::collections::HashMap;

pub(crate) const ACTIVITY_PREVIEW_CHARS: i64 = 240;
// Legacy messages are preview-only in Activity. Keep enough bytes from both
// ends to preserve wrapper metadata and the final `My request` section without
// ever materializing an entire JSONL payload.
pub(crate) const ACTIVITY_MESSAGE_PARSE_BYTES: i64 = 16 * 1024;
pub(crate) const ACTIVITY_MESSAGE_PARSE_EDGE_BYTES: i64 = ACTIVITY_MESSAGE_PARSE_BYTES / 2;
const LEGACY_ACTIVITY_PREFIX: &str = "legacy:";

pub(crate) fn legacy_activity_id(thread_id: &str) -> String {
    format!("{LEGACY_ACTIVITY_PREFIX}{thread_id}")
}

pub(crate) fn read_legacy_root(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
) -> Result<Option<ActivityItem>> {
    let exists: i64 = connection.query_row(
        "SELECT
            EXISTS(SELECT 1 FROM messages WHERE thread_id=?1)
          + EXISTS(SELECT 1 FROM events WHERE thread_id=?1)
          + EXISTS(SELECT 1 FROM tool_calls WHERE thread_id=?1)
          + EXISTS(SELECT 1 FROM usage_facts WHERE thread_id=?1)",
        [thread_id],
        |row| row.get(0),
    )?;
    if exists == 0 {
        return Ok(None);
    }

    let has_messages = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE thread_id=?1)",
        [thread_id],
        |row| row.get::<_, i64>(0),
    )? != 0;
    let mut first_user = None;
    let mut statement = connection.prepare(
        "SELECT CASE WHEN length(CAST(content AS BLOB))<=?2
                     THEN CAST(content AS BLOB)
                     ELSE substr(CAST(content AS BLOB),1,?3) END,
                CASE WHEN length(CAST(content AS BLOB))<=?2 THEN NULL
                     ELSE substr(CAST(content AS BLOB),-?3) END
         FROM messages
         WHERE thread_id=?1 AND role='user'
         ORDER BY timestamp,source_line,id",
    )?;
    let mut rows = statement.query(params![
        thread_id,
        ACTIVITY_MESSAGE_PARSE_BYTES,
        ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
    ])?;
    while let Some(row) = rows.next()? {
        let content = activity_content_from_edges(
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Option<Vec<u8>>>(1)?,
        );
        if let Some(display) = user_request_for_display(&content) {
            first_user = Some(display);
            break;
        }
    }
    let latest_assistant = connection
        .query_row(
            "SELECT CASE WHEN length(CAST(content AS BLOB))<=?2
                         THEN CAST(content AS BLOB)
                         ELSE substr(CAST(content AS BLOB),1,?3) END,
                    CASE WHEN length(CAST(content AS BLOB))<=?2 THEN NULL
                         ELSE substr(CAST(content AS BLOB),-?3) END
             FROM messages
             WHERE thread_id=?1 AND role='assistant'
             ORDER BY timestamp DESC,source_line DESC,id DESC LIMIT 1",
            params![
                thread_id,
                ACTIVITY_MESSAGE_PARSE_BYTES,
                ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
            ],
            |row| {
                Ok(activity_content_from_edges(
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                ))
            },
        )
        .optional()?
        .map(|content| redact_data_urls(&content));

    let timestamp = connection.query_row(
        "SELECT COALESCE(
            (SELECT MIN(value) FROM (
                SELECT MIN(timestamp) value FROM messages WHERE thread_id=?1
                UNION ALL SELECT MIN(timestamp) FROM usage_facts WHERE thread_id=?1
                UNION ALL SELECT MIN(timestamp) FROM events WHERE thread_id=?1
                UNION ALL SELECT MIN(started_at) FROM tool_calls WHERE thread_id=?1
             ) WHERE value IS NOT NULL),
            (SELECT started_at FROM threads WHERE id=?1))",
        [thread_id],
        |row| row.get::<_, String>(0),
    )?;
    let totals = read_totals_on(connection, None, None, TotalsScope::Thread { thread_id })?;
    let thread_title = connection
        .query_row(
            "SELECT NULLIF(trim(title),'') FROM threads WHERE id=?1",
            [thread_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    Ok(Some(ActivityItem {
        id: legacy_activity_id(thread_id),
        turn_id: None,
        rollout_id: root_rollout_id.to_owned(),
        agent_run_id: None,
        agent_label: None,
        timestamp,
        kind: "exchange".into(),
        role: Some("user".into()),
        label: Some(
            bounded_preview(first_user)
                .or_else(|| bounded_preview(thread_title))
                .unwrap_or_else(|| {
                    if totals.total_tokens > 0 {
                        "Usage activity".into()
                    } else {
                        "Conversation".into()
                    }
                }),
        ),
        body: bounded_preview(latest_assistant),
        status: None,
        tool_name: None,
        duration_ms: None,
        model: None,
        effort: None,
        has_details: has_messages
            || connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE thread_id=?1)
                      OR EXISTS(SELECT 1 FROM tool_calls WHERE thread_id=?1)",
                [thread_id],
                |row| row.get::<_, i64>(0),
            )? != 0,
        children: Vec::new(),
        child_page: None,
        child_page_size: None,
        child_total: None,
        child_has_more: None,
        child_next_cursor: None,
        usage: Some(totals),
        counts: None,
    }))
}

pub(crate) fn read_legacy_detail_on(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    requested_page: u64,
    page_size: u64,
    child_cursor: Option<&str>,
) -> Result<Option<ActivityItem>> {
    let Some(mut item) = read_legacy_root(connection, thread_id, root_rollout_id)? else {
        return Ok(None);
    };
    let children = query_legacy_activity_children_page(
        connection,
        thread_id,
        requested_page,
        page_size,
        child_cursor,
    )?;
    item.children = children.items;
    item.child_page = Some(children.page);
    item.child_page_size = Some(children.page_size);
    item.child_total = Some(children.total);
    item.child_has_more = Some(children.has_more);
    item.child_next_cursor = children.next_cursor;
    Ok(Some(item))
}

const LEGACY_ACTIVITY_CHILDREN_CTE: &str = "WITH canonical_event_ids(event_id) AS MATERIALIZED (
         SELECT substr(MIN(printf('%020d%s',source_line,event_id)),21)
         FROM activity_event_index
         WHERE thread_id=?1
         GROUP BY canonical_key
     ),
     canonical_events AS MATERIALIZED (
         SELECT projected.event_id,projected.timestamp,projected.source_line
         FROM canonical_event_ids canonical
         JOIN activity_event_index projected ON projected.event_id=canonical.event_id
     ),
     visible_messages AS MATERIALIZED (
         SELECT m.id,m.timestamp,m.source_line
         FROM messages m
         WHERE m.thread_id=?1
           AND NOT EXISTS(
               SELECT 1
               FROM canonical_events projected
               JOIN events e ON e.id=projected.event_id AND e.thread_id=?1
               LEFT JOIN messages event_message
                 ON event_message.id=COALESCE(e.call_id,e.id)
                AND event_message.thread_id=e.thread_id
               WHERE e.id=m.id
                  OR (
                       projected.timestamp=m.timestamp
                       AND e.kind<>'tool_call'
                       AND length(trim(COALESCE(
                           NULLIF(e.body,''),NULLIF(event_message.content,'')
                       )))<=?2
                       AND trim(COALESCE(
                           NULLIF(e.body,''),NULLIF(event_message.content,'')
                       ))=trim(m.content)
                  )
           )
     ) ";

#[derive(Debug)]
struct LegacyActivityChildRef {
    message: bool,
    id: String,
    source_line: i64,
    timestamp: String,
    sort_id: String,
}

pub(crate) fn query_legacy_activity_children_page(
    connection: &Connection,
    thread_id: &str,
    requested_page: u64,
    page_size: u64,
    child_cursor: Option<&str>,
) -> Result<ActivityChildrenPage> {
    let item_id = legacy_activity_id(thread_id);
    let cursor = child_cursor
        .map(|value| decode_activity_collection_cursor_for(value, thread_id, &item_id))
        .transpose()?;
    let total_sql = format!(
        "{LEGACY_ACTIVITY_CHILDREN_CTE}
         SELECT (SELECT COUNT(*) FROM canonical_events)
              + (SELECT COUNT(*) FROM visible_messages)"
    );
    let total = connection
        .query_row(
            &total_sql,
            params![thread_id, ACTIVITY_PREVIEW_CHARS],
            |row| row.get::<_, i64>(0),
        )?
        .max(0) as u64;
    let total_pages = total.div_ceil(page_size).max(1);
    let page = if cursor.is_some() {
        requested_page.max(1)
    } else {
        requested_page.max(1).min(total_pages)
    };
    let offset = page
        .saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64;
    let fetch_limit = page_size.saturating_add(1).min(i64::MAX as u64) as i64;
    let collection_sql = "SELECT child_kind,child_id,source_line,timestamp,sort_id FROM (
             SELECT 0 child_kind,event_id child_id,timestamp,source_line,event_id sort_id
             FROM canonical_events
             UNION ALL
             SELECT 1 child_kind,id child_id,timestamp,source_line,
                    'legacy-message:' || id sort_id
             FROM visible_messages
         )";
    let row_from_sql = |row: &Row<'_>| {
        Ok(LegacyActivityChildRef {
            message: row.get::<_, i64>(0)? != 0,
            id: row.get(1)?,
            source_line: row.get(2)?,
            timestamp: row.get(3)?,
            sort_id: row.get(4)?,
        })
    };
    // Continue pre-source-line cursors with their original ordering. Switching
    // ordering halfway through a collection can skip equal-timestamp records.
    let legacy_cursor_order = cursor
        .as_ref()
        .is_some_and(|cursor| cursor.source_line.is_none());
    let mut selected = if let Some(cursor) = cursor.as_ref() {
        if let Some(source_line) = cursor.source_line {
            let page_sql = format!(
                "{LEGACY_ACTIVITY_CHILDREN_CTE}
                 {collection_sql}
                 WHERE (timestamp,source_line,sort_id)<(?3,?4,?5)
                 ORDER BY timestamp DESC,source_line DESC,sort_id DESC
                 LIMIT ?6"
            );
            connection
                .prepare(&page_sql)?
                .query_map(
                    params![
                        thread_id,
                        ACTIVITY_PREVIEW_CHARS,
                        cursor.timestamp,
                        source_line,
                        cursor.sort_id,
                        fetch_limit
                    ],
                    row_from_sql,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            let page_sql = format!(
                "{LEGACY_ACTIVITY_CHILDREN_CTE}
                 {collection_sql}
                 WHERE (timestamp,sort_id)<(?3,?4)
                 ORDER BY timestamp DESC,sort_id DESC
                 LIMIT ?5"
            );
            connection
                .prepare(&page_sql)?
                .query_map(
                    params![
                        thread_id,
                        ACTIVITY_PREVIEW_CHARS,
                        cursor.timestamp,
                        cursor.sort_id,
                        fetch_limit
                    ],
                    row_from_sql,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        }
    } else {
        let page_sql = format!(
            "{LEGACY_ACTIVITY_CHILDREN_CTE}
             {collection_sql}
             ORDER BY timestamp DESC,source_line DESC,sort_id DESC
             LIMIT ?3 OFFSET ?4"
        );
        connection
            .prepare(&page_sql)?
            .query_map(
                params![thread_id, ACTIVITY_PREVIEW_CHARS, fetch_limit, offset],
                row_from_sql,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let has_more = selected.len() as u64 > page_size;
    if has_more {
        selected.truncate(page_size as usize);
    }
    let next_cursor = if has_more {
        selected
            .last()
            .map(|child| {
                encode_activity_collection_cursor(
                    thread_id,
                    &item_id,
                    &child.timestamp,
                    (!legacy_cursor_order).then_some(child.source_line),
                    &child.sort_id,
                )
            })
            .transpose()?
    } else {
        None
    };

    let indexed = selected
        .iter()
        .filter(|child| !child.message)
        .map(|child| IndexedActivityEvent {
            event_id: child.id.clone(),
            source_line: child.source_line,
        })
        .collect::<Vec<_>>();
    let event_items = query_activity_child_preview_rows(connection, thread_id, &indexed)?
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<HashMap<_, _>>();
    let message_ids = selected
        .iter()
        .filter(|child| child.message)
        .map(|child| child.id.clone())
        .collect::<Vec<_>>();
    let message_items = query_legacy_message_child_rows(connection, thread_id, &message_ids)?;

    let mut items = Vec::with_capacity(selected.len());
    for child in selected {
        let item = if child.message {
            message_items.get(&child.id)
        } else {
            event_items.get(&child.id)
        };
        if let Some(item) = item {
            items.push(item.clone());
        }
    }
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more,
        next_cursor,
    })
}

pub(crate) fn query_legacy_message_child_rows(
    connection: &Connection,
    thread_id: &str,
    message_ids: &[String],
) -> Result<HashMap<String, ActivityItem>> {
    if message_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let requested = serde_json::to_string(message_ids)?;
    let mut statement = connection.prepare(
        "SELECT m.id,m.rollout_id,m.turn_id,m.timestamp,m.role,
                CASE WHEN length(CAST(m.content AS BLOB))<=?3
                     THEN CAST(m.content AS BLOB)
                     ELSE substr(CAST(m.content AS BLOB),1,?4) END,
                CASE WHEN length(CAST(m.content AS BLOB))<=?3 THEN NULL
                     ELSE substr(CAST(m.content AS BLOB),-?4) END
         FROM json_each(?1) requested
         JOIN messages m ON m.id=requested.value AND m.thread_id=?2",
    )?;
    let rows = statement.query_map(
        params![
            requested,
            thread_id,
            ACTIVITY_MESSAGE_PARSE_BYTES,
            ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
        ],
        |row| {
            let id = row.get::<_, String>(0)?;
            let role = row.get::<_, String>(4)?;
            let head = row.get::<_, Vec<u8>>(5)?;
            let tail = row.get::<_, Option<Vec<u8>>>(6)?;
            let content = activity_content_from_edges(head, tail);
            let body = if role == "user" {
                bounded_preview(user_request_for_display(&content))
            } else {
                bounded_preview(Some(content))
            };
            Ok(ActivityItem {
                id: format!("legacy-message:{id}"),
                turn_id: row.get(2)?,
                rollout_id: row.get(1)?,
                agent_run_id: None,
                agent_label: None,
                timestamp: row.get(3)?,
                kind: if role == "user" { "user" } else { "final" }.into(),
                role: Some(role),
                label: None,
                body,
                status: None,
                tool_name: None,
                duration_ms: None,
                model: None,
                effort: None,
                has_details: false,
                children: Vec::new(),
                child_page: None,
                child_page_size: None,
                child_total: None,
                child_has_more: None,
                child_next_cursor: None,
                usage: None,
                counts: None,
            })
        },
    )?;
    let mut items = HashMap::with_capacity(message_ids.len());
    for row in rows {
        let item = row?;
        let id = item
            .id
            .strip_prefix("legacy-message:")
            .unwrap_or(&item.id)
            .to_owned();
        items.insert(id, item);
    }
    Ok(items)
}

pub(crate) struct ActivityChildrenPage {
    pub(crate) items: Vec<ActivityItem>,
    pub(crate) page: u64,
    pub(crate) page_size: u64,
    pub(crate) total: u64,
    pub(crate) has_more: bool,
    pub(crate) next_cursor: Option<String>,
}

#[cfg(test)]
pub(crate) fn query_activity_child_previews_page(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: &str,
    page: u64,
    page_size: u64,
) -> Result<ActivityChildrenPage> {
    query_activity_child_previews_cursor_page(connection, thread_id, turn_id, page, page_size, None)
}

pub(crate) fn query_activity_child_previews_cursor_page(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: &str,
    page: u64,
    page_size: u64,
    cursor: Option<&str>,
) -> Result<ActivityChildrenPage> {
    let requested_page = page.max(1);
    let fallback_offset = requested_page.saturating_sub(1).saturating_mul(page_size);
    let mut indexed = query_activity_index_page(
        connection,
        thread_id,
        turn_id,
        page_size,
        cursor,
        fallback_offset,
    )?;
    let total = indexed.total;
    let total_pages = total.div_ceil(page_size).max(1);
    // Numeric pages remain a compatibility path for old bookmarks and direct
    // tests. The browser uses the opaque cursor, so ordinary Load More calls
    // never walk an OFFSET proportional to the complete turn history.
    let page = if cursor.is_some() {
        requested_page
    } else {
        requested_page.min(total_pages)
    };
    if cursor.is_none() && page != requested_page {
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        indexed =
            query_activity_index_page(connection, thread_id, turn_id, page_size, None, offset)?;
    }
    let items = query_activity_child_preview_rows(connection, thread_id, &indexed.events)?;
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more: indexed.next_cursor.is_some(),
        next_cursor: indexed.next_cursor,
    })
}

fn query_activity_child_preview_rows(
    connection: &rusqlite::Connection,
    thread_id: &str,
    indexed: &[IndexedActivityEvent],
) -> Result<Vec<ActivityItem>> {
    if indexed.is_empty() {
        return Ok(Vec::new());
    }
    let requested = serde_json::to_string(
        &indexed
            .iter()
            .enumerate()
            .map(|(ordinal, event)| {
                serde_json::json!({
                    "ordinal": ordinal,
                    "eventId": event.event_id,
                    "sourceLine": event.source_line,
                })
            })
            .collect::<Vec<_>>(),
    )?;
    let mut statement = connection.prepare(
        "WITH selected AS MATERIALIZED (
             SELECT CAST(key AS INTEGER) ordinal,
                    json_extract(value,'$.eventId') event_id,
                    CAST(json_extract(value,'$.sourceLine') AS INTEGER) source_line
             FROM json_each(?1)
         )
         SELECT e.id,e.turn_id,e.rollout_id,e.agent_run_id,e.timestamp,e.kind,e.role,e.label,
                CASE WHEN e.kind='user' OR e.role='user' THEN
                    COALESCE(NULLIF(e.body,''),NULLIF(m.content,''))
                WHEN e.kind='tool_call' THEN NULL
                ELSE NULLIF(substr(COALESCE(NULLIF(e.body,''),NULLIF(m.content,'')),1,?3),'') END,
                COALESCE(tc.status,e.status),COALESCE(tc.name,e.tool_name),
                COALESCE(tc.duration_ms,e.duration_ms,
                    CASE WHEN tc.completed_at IS NOT NULL THEN
                        CAST(ROUND((julianday(tc.completed_at)-julianday(tc.started_at))*86400000.0)
                            AS INTEGER)
                    END),e.model,e.effort,
                CASE WHEN e.body IS NOT NULL OR m.content IS NOT NULL THEN 1 ELSE 0 END,
                a.nickname,a.agent_path,tc.namespace,selected.source_line
         FROM selected
         JOIN events e ON e.id=selected.event_id AND e.thread_id=?2
         LEFT JOIN messages m
           ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
         LEFT JOIN tool_calls tc
           ON tc.rollout_id=e.rollout_id AND tc.call_id=e.call_id
          AND tc.thread_id=e.thread_id
         LEFT JOIN agent_runs a
           ON a.id=e.agent_run_id AND a.thread_id=e.thread_id
         ORDER BY selected.ordinal",
    )?;
    let rows = statement.query_map(
        params![requested, thread_id, ACTIVITY_PREVIEW_CHARS],
        |row| {
            let stored_kind: String = row.get(5)?;
            let role: Option<String> = row.get(6)?;
            let stored_tool_name = row.get::<_, Option<String>>(10)?;
            let tool_namespace = row.get::<_, Option<String>>(17)?;
            let kind = normalize_activity_kind(&stored_kind, role.as_deref());
            let body = row.get::<_, Option<String>>(8)?;
            let body = if kind == "user" {
                bounded_preview(body.and_then(|value| user_request_for_display(&value)))
            } else {
                bounded_preview(body)
            };
            Ok((
                ActivityItem {
                    id: row.get(0)?,
                    turn_id: row.get(1)?,
                    rollout_id: row.get(2)?,
                    agent_run_id: row.get(3)?,
                    agent_label: row
                        .get::<_, Option<String>>(15)?
                        .or(row.get::<_, Option<String>>(16)?),
                    timestamp: row.get(4)?,
                    kind,
                    role,
                    label: row.get(7)?,
                    body,
                    status: row.get(9)?,
                    tool_name: stored_tool_name
                        .map(|name| tool_name_for_display(tool_namespace.as_deref(), &name)),
                    duration_ms: row.get(11)?,
                    model: row.get(12)?,
                    effort: row.get(13)?,
                    has_details: row.get::<_, i64>(14)? != 0,
                    children: Vec::new(),
                    child_page: None,
                    child_page_size: None,
                    child_total: None,
                    child_has_more: None,
                    child_next_cursor: None,
                    usage: None,
                    counts: None,
                },
                row.get::<_, i64>(18)?,
            ))
        },
    )?;
    let mut items = Vec::new();
    for row in rows {
        let (item, source_line) = row?;
        items.push((item, source_line));
    }
    let owners = items
        .iter()
        .enumerate()
        .filter(|(_, (item, _))| {
            matches!(
                item.kind.as_str(),
                "assistant" | "update" | "final" | "reasoning" | "tool" | "subagent"
            )
        })
        .map(|(ordinal, (item, source_line))| EventUsageOwner {
            ordinal,
            rollout_id: item.rollout_id.clone(),
            turn_id: item.turn_id.clone(),
            source_line: *source_line,
        })
        .collect::<Vec<_>>();
    for (ordinal, totals) in event_totals_on(connection, thread_id, &owners)? {
        if let Some((item, _)) = items.get_mut(ordinal) {
            item.usage = Some(totals);
        }
    }
    Ok(items.into_iter().map(|(item, _)| item).collect())
}

pub(crate) fn activity_content_from_edges(head: Vec<u8>, tail: Option<Vec<u8>>) -> String {
    let mut content = String::from_utf8_lossy(&head).into_owned();
    if let Some(tail) = tail {
        content.push_str("\n…\n");
        content.push_str(&String::from_utf8_lossy(&tail));
    }
    content
}

pub(crate) fn bounded_preview(value: Option<String>) -> Option<String> {
    let value = redact_data_urls(value?.trim());
    if value.is_empty() {
        return None;
    }
    let mut chars = value.chars();
    let mut preview = chars
        .by_ref()
        .take(ACTIVITY_PREVIEW_CHARS as usize)
        .collect::<String>();
    if chars.next().is_some() {
        preview.push('…');
    }
    Some(preview)
}

pub(crate) fn normalize_activity_kind(kind: &str, role: Option<&str>) -> String {
    match kind {
        "message" if role == Some("user") => "user",
        "message" => "final",
        "turn_completed" => "final",
        "tool_call" => "tool",
        "tool_output" | "tool_completed" => "tool_result",
        "state" => "system",
        other => other,
    }
    .to_owned()
}
