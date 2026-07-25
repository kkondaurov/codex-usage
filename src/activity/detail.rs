use super::{
    attribution::{EventUsageKey, event_total_on, turn_totals_on},
    cursor::decode_activity_collection_cursor_for,
    groups::{parse_id as parse_group_id, read_detail_on as read_group_detail_on},
    index::validate_cursor_for as validate_index_cursor_for,
    model::ActivityItem,
    previews::{
        ACTIVITY_PREVIEW_CHARS, bounded_preview, legacy_activity_id, normalize_activity_kind,
        query_activity_child_previews_cursor_page, read_legacy_detail_on,
    },
    root_page::{read_exchange, root_rollout_id_on},
    selection::{ActivityRootScope, PreparedSelection},
};
use crate::{conversation::display::tool_name_for_display, redaction::redact_data_urls};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) struct DetailPage<'a> {
    pub(crate) page: u64,
    pub(crate) page_size: u64,
    pub(crate) cursor: Option<&'a str>,
}

pub(crate) fn validate_cursor_for(value: &str, thread_id: &str, item_id: &str) -> Result<()> {
    if item_id == legacy_activity_id(thread_id) || parse_group_id(item_id).is_some() {
        decode_activity_collection_cursor_for(value, thread_id, item_id).map(|_| ())
    } else {
        validate_index_cursor_for(value, thread_id, item_id)
    }
}

pub(crate) fn read_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    page: DetailPage<'_>,
) -> Result<Option<ActivityItem>> {
    let root_rollout_id = root_rollout_id_on(connection, thread_id)?;
    if item_id == legacy_activity_id(thread_id) {
        return read_legacy_detail_on(
            connection,
            thread_id,
            &root_rollout_id,
            page.page,
            page.page_size,
            page.cursor,
        );
    }
    if let Some((reviews, root_turn_id)) = parse_group_id(item_id) {
        return read_group_detail_on(
            connection,
            thread_id,
            &root_rollout_id,
            root_turn_id,
            reviews,
            page.page,
            page.page_size,
            page.cursor,
        );
    }
    if let Some(mut turn) = connection
        .query_row(
            "SELECT t.id,t.rollout_id,t.agent_run_id,t.started_at,t.status,t.model,t.effort,
                    NULLIF(substr(t.last_agent_message,1,?3),'') last_agent_message,
                    t.duration_ms,a.nickname,a.agent_path
             FROM turns t LEFT JOIN agent_runs a
               ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
             WHERE t.thread_id=?1 AND t.id=?2",
            params![thread_id, item_id, ACTIVITY_PREVIEW_CHARS + 1],
            |row| {
                let agent_label = row
                    .get::<_, Option<String>>(9)?
                    .or(row.get::<_, Option<String>>(10)?);
                Ok(ActivityItem {
                    id: row.get(0)?,
                    turn_id: row.get(0)?,
                    rollout_id: row.get(1)?,
                    agent_run_id: row.get(2)?,
                    agent_label: agent_label.clone(),
                    timestamp: row.get(3)?,
                    kind: "system".into(),
                    role: None,
                    label: Some(
                        agent_label
                            .map(|value| format!("{value} · Turn"))
                            .unwrap_or_else(|| "Turn".into()),
                    ),
                    body: bounded_preview(row.get::<_, Option<String>>(7)?),
                    status: row.get(4)?,
                    tool_name: None,
                    duration_ms: row.get(8)?,
                    model: row.get(5)?,
                    effort: row.get(6)?,
                    has_details: true,
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
        )
        .optional()?
    {
        if turn.rollout_id == root_rollout_id {
            let root_scope = ActivityRootScope::from_known_on(
                connection,
                thread_id,
                &root_rollout_id,
                item_id,
                turn.timestamp.clone(),
            )?;
            let selection = PreparedSelection::prepare(
                connection,
                thread_id,
                &root_rollout_id,
                std::slice::from_ref(&root_scope),
            )?;
            let exchange = read_exchange(&selection, item_id, &root_rollout_id, page.page_size)?;
            turn.kind = "exchange".into();
            turn.role = Some("user".into());
            turn.label =
                Some(bounded_preview(exchange.request).unwrap_or_else(|| "Conversation".into()));
            turn.counts = Some(exchange.counts);
            turn.usage = Some(exchange.usage);
            let children = query_activity_child_previews_cursor_page(
                connection,
                thread_id,
                item_id,
                page.page,
                page.page_size,
                page.cursor,
            )?;
            turn.children = children.items;
            turn.children.extend(exchange.groups);
            turn.child_page = Some(children.page);
            turn.child_page_size = Some(children.page_size);
            turn.child_total = Some(children.total);
            turn.child_has_more = Some(children.has_more);
            turn.child_next_cursor = children.next_cursor;
            turn.children.sort_by(|left, right| {
                right
                    .timestamp
                    .cmp(&left.timestamp)
                    .then_with(|| right.id.cmp(&left.id))
            });
        } else {
            turn.kind = if turn.model.as_deref() == Some("codex-auto-review") {
                "review".into()
            } else {
                "subagent".into()
            };
            turn.label = Some(turn.agent_label.clone().unwrap_or_else(|| {
                if turn.kind == "review" {
                    "Automated review".into()
                } else {
                    "Agent response".into()
                }
            }));
            let children = query_activity_child_previews_cursor_page(
                connection,
                thread_id,
                item_id,
                page.page,
                page.page_size,
                page.cursor,
            )?;
            turn.children = children.items;
            turn.child_page = Some(children.page);
            turn.child_page_size = Some(children.page_size);
            turn.child_total = Some(children.total);
            turn.child_has_more = Some(children.has_more);
            turn.child_next_cursor = children.next_cursor;
            turn.usage = Some(turn_totals_on(connection, thread_id, item_id)?);
        }
        // Prefer the final event in chronological position. Keep
        // last_agent_message only when no child carries a final body.
        if turn.children.iter().any(|child| {
            child.kind == "final"
                && child
                    .body
                    .as_deref()
                    .is_some_and(|body| !body.trim().is_empty())
        }) {
            turn.body = None;
        }
        turn.has_details = turn.body.is_some() || !turn.children.is_empty();
        return Ok(Some(turn));
    }

    let event = connection
        .query_row(
            "SELECT e.id,e.turn_id,e.rollout_id,e.agent_run_id,e.timestamp,e.kind,e.role,e.label,
                    COALESCE(e.body,m.content),COALESCE(tc.status,e.status),
                    COALESCE(tc.name,e.tool_name),COALESCE(
                        tc.duration_ms,e.duration_ms,
                        CASE WHEN tc.completed_at IS NOT NULL THEN
                            CAST(ROUND((julianday(tc.completed_at)-julianday(tc.started_at))*86400000.0)
                                AS INTEGER)
                        END),
                    e.model,e.effort,a.nickname,a.agent_path,tc.namespace,e.source_line,
                    e.call_id
             FROM events e
             LEFT JOIN messages m
               ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
             LEFT JOIN tool_calls tc
               ON tc.rollout_id=e.rollout_id AND tc.call_id=e.call_id
              AND tc.thread_id=e.thread_id
             LEFT JOIN agent_runs a
               ON a.id=e.agent_run_id AND a.thread_id=e.thread_id
             WHERE e.thread_id=?1 AND e.id=?2",
            params![thread_id, item_id],
            |row| {
                let stored_kind: String = row.get(5)?;
                let role: Option<String> = row.get(6)?;
                let stored_tool_name = row.get::<_, Option<String>>(10)?;
                let body = if stored_kind == "tool_call" {
                    None
                } else {
                    row.get::<_, Option<String>>(8)?
                        .map(|value| redact_data_urls(&value))
                        .filter(|value| !value.is_empty())
                };
                let agent_label = row
                    .get::<_, Option<String>>(14)?
                    .or(row.get::<_, Option<String>>(15)?);
                let tool_namespace = row.get::<_, Option<String>>(16)?;
                let kind = normalize_activity_kind(&stored_kind, role.as_deref());
                Ok((
                    ActivityItem {
                        id: row.get(0)?,
                        turn_id: row.get(1)?,
                        rollout_id: row.get(2)?,
                        agent_run_id: row.get(3)?,
                        agent_label,
                        timestamp: row.get(4)?,
                        kind,
                        role,
                        label: row.get(7)?,
                        has_details: body.is_some(),
                        body,
                        status: row.get(9)?,
                        tool_name: stored_tool_name
                            .map(|name| tool_name_for_display(tool_namespace.as_deref(), &name)),
                        duration_ms: row.get(11)?,
                        model: row.get(12)?,
                        effort: row.get(13)?,
                        children: Vec::new(),
                        child_page: None,
                        child_page_size: None,
                        child_total: None,
                        child_has_more: None,
                        child_next_cursor: None,
                        usage: None,
                        counts: None,
                    },
                    stored_kind,
                    row.get::<_, i64>(17)?,
                    row.get::<_, Option<String>>(18)?,
                ))
            },
        )
        .optional()?;
    let Some((mut item, stored_kind, source_line, call_id)) = event else {
        return Ok(None);
    };
    if item.turn_id.is_some() {
        item.usage = event_total_on(
            connection,
            thread_id,
            EventUsageKey {
                id: &item.id,
                rollout_id: &item.rollout_id,
                turn_id: item.turn_id.as_deref(),
                kind: &item.kind,
                stored_kind: &stored_kind,
                source_line,
                call_id: call_id.as_deref(),
            },
        )?;
    }
    Ok(Some(item))
}

#[cfg(test)]
pub(crate) fn read_default_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
) -> Result<Option<ActivityItem>> {
    read_numeric_page_on(connection, thread_id, item_id, 1, 250)
}

#[cfg(test)]
pub(crate) fn read_numeric_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    page: u64,
    page_size: u64,
) -> Result<Option<ActivityItem>> {
    read_on(
        connection,
        thread_id,
        item_id,
        DetailPage {
            page,
            page_size,
            cursor: None,
        },
    )
}

#[cfg(test)]
pub(crate) fn read_cursor_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    page: u64,
    page_size: u64,
    cursor: Option<&str>,
) -> Result<Option<ActivityItem>> {
    read_on(
        connection,
        thread_id,
        item_id,
        DetailPage {
            page,
            page_size,
            cursor,
        },
    )
}
