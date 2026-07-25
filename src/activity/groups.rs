use super::{
    attribution::SelectedActivityUsage,
    cursor::{decode_activity_collection_cursor_for, encode_activity_collection_cursor},
    model::ActivityItem,
    previews::{ACTIVITY_PREVIEW_CHARS, ActivityChildrenPage, bounded_preview},
    selection::{ActivityRootScope, PreparedSelection},
};
use crate::usage::{
    RollupScope, UsageAccumulator, UsageTotals, load_price_book_on, price_hourly_rollup_on,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, Row, params};
use std::collections::HashMap;

pub(crate) const ACTIVITY_AGENT_LABEL_PREVIEW_LIMIT: i64 = 8;

#[derive(Clone, Default)]
pub(crate) struct ActivityDescendantGroup {
    pub(crate) turn_count: u64,
    pub(crate) timestamp: String,
    pub(crate) status: String,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) labels: Vec<String>,
    pub(crate) label_count: u64,
    pub(crate) usage: UsageTotals,
}

#[derive(Clone, Default)]
struct RootGroups {
    agent_count: u64,
    review_count: u64,
    agents: Option<ActivityDescendantGroup>,
    reviews: Option<ActivityDescendantGroup>,
}

impl RootGroups {
    fn group_mut(&mut self, reviews: bool) -> &mut ActivityDescendantGroup {
        if reviews {
            self.reviews
                .get_or_insert_with(ActivityDescendantGroup::default)
        } else {
            self.agents
                .get_or_insert_with(ActivityDescendantGroup::default)
        }
    }

    fn group(&self, reviews: bool) -> Option<&ActivityDescendantGroup> {
        if reviews {
            self.reviews.as_ref()
        } else {
            self.agents.as_ref()
        }
    }
}

#[derive(Default)]
pub(crate) struct GroupSummaries {
    roots: HashMap<String, RootGroups>,
}

impl GroupSummaries {
    pub(crate) fn load(
        selection: &PreparedSelection<'_>,
        usage: &SelectedActivityUsage,
    ) -> Result<Self> {
        selection.with_connection(|connection| {
            let mut summaries = Self::default();
            if !selection.has_descendants() {
                return Ok(summaries);
            }

            let mut statement = connection.prepare(
                "SELECT root_turn_id,
                        COUNT(DISTINCT CASE WHEN review=0 THEN agent_key END),
                        COALESCE(SUM(review=1),0)
                 FROM selected_activity_descendants
                 GROUP BY root_turn_id",
            )?;
            for row in statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?.max(0) as u64,
                    row.get::<_, i64>(2)?.max(0) as u64,
                ))
            })? {
                let (root_turn_id, agent_count, review_count) = row?;
                let root = summaries.roots.entry(root_turn_id).or_default();
                root.agent_count = agent_count;
                root.review_count = review_count;
            }

            let mut statement = connection.prepare(
                "SELECT root_turn_id,review,COUNT(*),MAX(started_at),
                        COALESCE(MAX(status='running'),0),
                        COALESCE(MAX(status NOT IN ('completed','success','allowed')),0)
                 FROM selected_activity_descendants
                 GROUP BY root_turn_id,review",
            )?;
            for row in statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, i64>(2)?.max(0) as u64,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)? != 0,
                    row.get::<_, i64>(5)? != 0,
                ))
            })? {
                let (root_turn_id, reviews, turn_count, timestamp, running, attention) = row?;
                let group = summaries
                    .roots
                    .entry(root_turn_id)
                    .or_default()
                    .group_mut(reviews);
                group.turn_count = turn_count;
                group.timestamp = timestamp;
                group.status = if running {
                    "running"
                } else if attention {
                    "attention"
                } else {
                    "completed"
                }
                .into();
            }

            let mut statement = connection.prepare(
                "WITH latest_labels AS (
                     SELECT root_turn_id,agent_label,started_at,turn_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY root_turn_id,agent_label
                                ORDER BY started_at DESC,turn_id DESC
                            ) label_rank
                     FROM selected_activity_descendants
                     WHERE review=0 AND agent_label IS NOT NULL
                 ), ranked_labels AS (
                     SELECT root_turn_id,agent_label,started_at,turn_id,
                            COUNT(*) OVER (PARTITION BY root_turn_id) label_count,
                            ROW_NUMBER() OVER (
                                PARTITION BY root_turn_id
                                ORDER BY started_at DESC,turn_id DESC
                            ) preview_rank
                     FROM latest_labels WHERE label_rank=1
                 )
                 SELECT root_turn_id,agent_label,label_count
                 FROM ranked_labels WHERE preview_rank<=?1
                 ORDER BY root_turn_id,preview_rank",
            )?;
            for row in statement.query_map([ACTIVITY_AGENT_LABEL_PREVIEW_LIMIT], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?.max(0) as u64,
                ))
            })? {
                let (root_turn_id, label, label_count) = row?;
                let group = summaries
                    .roots
                    .entry(root_turn_id)
                    .or_default()
                    .group_mut(false);
                group.labels.push(label);
                group.label_count = label_count;
            }

            let mut statement = connection.prepare(
                "SELECT root_turn_id,review,started_at,duration_ms
                 FROM selected_activity_descendants
                 WHERE duration_ms IS NOT NULL
                 ORDER BY root_turn_id,review,started_at,turn_id",
            )?;
            let mut durations = HashMap::<(String, bool), ActivityDurationAccumulator>::new();
            for row in statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })? {
                let (root_turn_id, reviews, started_at, duration_ms) = row?;
                durations
                    .entry((root_turn_id, reviews))
                    .or_default()
                    .add(&started_at, duration_ms);
            }
            for ((root_turn_id, reviews), duration) in durations {
                summaries
                    .roots
                    .entry(root_turn_id)
                    .or_default()
                    .group_mut(reviews)
                    .duration_ms = duration.finish();
            }

            for (root_turn_id, root) in &mut summaries.roots {
                if let Some(group) = root.agents.as_mut() {
                    group.usage = usage.group_totals(root_turn_id, false);
                }
                if let Some(group) = root.reviews.as_mut() {
                    group.usage = usage.group_totals(root_turn_id, true);
                }
            }
            Ok(summaries)
        })
    }

    pub(crate) fn counts(&self, root_turn_id: &str) -> (u64, u64) {
        self.roots
            .get(root_turn_id)
            .map(|root| (root.agent_count, root.review_count))
            .unwrap_or_default()
    }

    pub(crate) fn placeholders(
        &self,
        root_turn_id: &str,
        root_rollout_id: &str,
        child_page_size: u64,
    ) -> Vec<ActivityItem> {
        let Some(root) = self.roots.get(root_turn_id) else {
            return Vec::new();
        };
        let mut groups = Vec::new();
        if let Some(agent_group) = root.group(false).filter(|group| group.turn_count > 0) {
            groups.push(ActivityItem {
                id: format!("group:agents:{root_turn_id}"),
                turn_id: Some(root_turn_id.to_owned()),
                rollout_id: root_rollout_id.to_owned(),
                agent_run_id: None,
                agent_label: None,
                timestamp: agent_group.timestamp.clone(),
                kind: "agent_group".into(),
                role: None,
                label: Some(format!("Agents · {}", root.agent_count)),
                body: agent_labels_preview(&agent_group.labels, agent_group.label_count),
                status: Some(agent_group.status.clone()),
                tool_name: None,
                duration_ms: agent_group.duration_ms,
                model: None,
                effort: None,
                has_details: true,
                children: Vec::new(),
                child_page: Some(1),
                child_page_size: Some(child_page_size),
                child_total: Some(agent_group.turn_count),
                child_has_more: Some(true),
                child_next_cursor: None,
                usage: Some(agent_group.usage.clone()),
                counts: None,
            });
        }
        if let Some(review_group) = root.group(true).filter(|group| group.turn_count > 0) {
            groups.push(ActivityItem {
                id: format!("group:reviews:{root_turn_id}"),
                turn_id: Some(root_turn_id.to_owned()),
                rollout_id: root_rollout_id.to_owned(),
                agent_run_id: None,
                agent_label: None,
                timestamp: review_group.timestamp.clone(),
                kind: "review_group".into(),
                role: None,
                label: Some(format!("Automated reviews · {}", root.review_count)),
                body: None,
                status: Some(review_group.status.clone()),
                tool_name: None,
                duration_ms: review_group.duration_ms,
                model: None,
                effort: None,
                has_details: true,
                children: Vec::new(),
                child_page: Some(1),
                child_page_size: Some(child_page_size),
                child_total: Some(review_group.turn_count),
                child_has_more: Some(true),
                child_next_cursor: None,
                usage: Some(review_group.usage.clone()),
                counts: None,
            });
        }
        groups
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_detail_on(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    root_turn_id: &str,
    reviews: bool,
    child_page: u64,
    child_page_size: u64,
    child_cursor: Option<&str>,
) -> Result<Option<ActivityItem>> {
    let Some(root_scope) =
        ActivityRootScope::load_on(connection, thread_id, root_rollout_id, root_turn_id)?
    else {
        return Ok(None);
    };
    prepare_activity_group_turns(connection, thread_id, root_rollout_id, &root_scope, reviews)?;
    let child_total = connection
        .query_row(
            "SELECT COUNT(*) FROM selected_activity_group_turns",
            [],
            |row| row.get::<_, i64>(0),
        )?
        .max(0) as u64;
    if child_total == 0 {
        return Ok(None);
    }
    let item_id = format!(
        "group:{}:{root_turn_id}",
        if reviews { "reviews" } else { "agents" }
    );
    let children = query_activity_group_child_page_on(
        connection,
        thread_id,
        &item_id,
        child_page,
        child_page_size,
        child_total,
        child_cursor,
    )?;
    let timestamp = connection.query_row(
        "SELECT t.started_at
         FROM selected_activity_group_turns selected
         JOIN turns t ON t.id=selected.turn_id
         ORDER BY t.started_at DESC,t.id DESC LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )?;
    let status = query_activity_group_status_on(connection)?;
    let duration_ms = query_activity_group_duration_on(connection)?;
    let usage = query_activity_group_totals_on(connection, thread_id)?;
    let (label, body) = if reviews {
        (format!("Automated reviews · {child_total}"), None)
    } else {
        let agent_count = connection
            .query_row(
                "SELECT COUNT(DISTINCT agent_key)
                 FROM selected_activity_group_turns",
                [],
                |row| row.get::<_, i64>(0),
            )?
            .max(0) as u64;
        (
            format!("Agents · {agent_count}"),
            query_activity_group_labels_on(connection)?,
        )
    };
    Ok(Some(ActivityItem {
        id: item_id,
        turn_id: Some(root_turn_id.to_owned()),
        rollout_id: root_rollout_id.to_owned(),
        agent_run_id: None,
        agent_label: None,
        timestamp,
        kind: if reviews {
            "review_group".into()
        } else {
            "agent_group".into()
        },
        role: None,
        label: Some(label),
        body,
        status: Some(status),
        tool_name: None,
        duration_ms,
        model: None,
        effort: None,
        has_details: true,
        children: children.items,
        child_page: Some(children.page),
        child_page_size: Some(children.page_size),
        child_total: Some(children.total),
        child_has_more: Some(children.has_more),
        child_next_cursor: children.next_cursor,
        usage: Some(usage),
        counts: None,
    }))
}

fn prepare_activity_group_turns(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    root: &ActivityRootScope,
    reviews: bool,
) -> Result<()> {
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS selected_activity_group_turns(
             turn_id TEXT PRIMARY KEY,
             agent_key TEXT NOT NULL,
             started_at TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS idx_selected_activity_group_turns_time
             ON selected_activity_group_turns(started_at DESC,turn_id DESC);
         DELETE FROM selected_activity_group_turns;",
    )?;
    connection.execute(
        "WITH links AS MATERIALIZED (
             SELECT json_extract(event.payload_json,'$.agent_thread_id') agent_key,
                    event.turn_id root_turn_id,event.timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY json_extract(event.payload_json,'$.agent_thread_id')
                        ORDER BY event.timestamp,event.source_line,event.id
                    ) link_rank,
                    LEAD(event.timestamp) OVER (
                        PARTITION BY json_extract(event.payload_json,'$.agent_thread_id')
                        ORDER BY event.timestamp,event.source_line,event.id
                    ) next_linked_at
             FROM events event
             JOIN turns root_turn
               ON root_turn.id=event.turn_id AND root_turn.thread_id=event.thread_id
             WHERE event.thread_id=?1 AND event.kind='subagent'
               AND root_turn.rollout_id=?2
               AND json_extract(event.payload_json,'$.agent_thread_id') IS NOT NULL
               AND EXISTS(
                    SELECT 1 FROM turns descendant
                    WHERE descendant.thread_id=?1 AND descendant.rollout_id<>?2
                    LIMIT 1
               )
         ),
         explicit_agents AS MATERIALIZED (
             SELECT DISTINCT agent_key FROM links
         ),
         selected_intervals AS MATERIALIZED (
             SELECT agent_key,
                    CASE WHEN link_rank=1 THEN NULL ELSE timestamp END linked_at,
                    next_linked_at
             FROM links WHERE root_turn_id=?3
         )
         INSERT INTO selected_activity_group_turns(turn_id,agent_key,started_at)
         SELECT t.id,COALESCE(t.agent_run_id,t.rollout_id),t.started_at
         FROM turns t
         WHERE t.thread_id=?1 AND t.rollout_id<>?2
           AND (COALESCE(t.model='codex-auto-review',0)=?6)
           AND (
                (
                    EXISTS(
                        SELECT 1 FROM explicit_agents explicit
                        WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                    )
                    AND EXISTS(
                        SELECT 1 FROM selected_intervals selected
                        WHERE selected.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                          AND (selected.linked_at IS NULL OR t.started_at>=selected.linked_at)
                          AND (selected.next_linked_at IS NULL
                               OR t.started_at<selected.next_linked_at)
                    )
                )
                OR (
                    NOT EXISTS(
                        SELECT 1 FROM explicit_agents explicit
                        WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                    )
                    AND t.started_at>=?4
                    AND (?5 IS NULL OR t.started_at<?5)
                )
           )",
        params![
            thread_id,
            root_rollout_id,
            root.id,
            root.started_at,
            root.next_started_at,
            i64::from(reviews)
        ],
    )?;
    Ok(())
}

struct ActivityGroupChildRef {
    id: String,
    timestamp: String,
}

fn query_activity_group_child_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    requested_page: u64,
    page_size: u64,
    total: u64,
    child_cursor: Option<&str>,
) -> Result<ActivityChildrenPage> {
    let cursor = child_cursor
        .map(|value| decode_activity_collection_cursor_for(value, thread_id, item_id))
        .transpose()?;
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
    let mut selected = if let Some(cursor) = cursor.as_ref() {
        let mut statement = connection.prepare(
            "SELECT selected.turn_id,selected.started_at
             FROM selected_activity_group_turns selected
                  INDEXED BY idx_selected_activity_group_turns_time
             WHERE (selected.started_at,selected.turn_id)<(?1,?2)
             ORDER BY selected.started_at DESC,selected.turn_id DESC
             LIMIT ?3",
        )?;
        statement
            .query_map(
                params![cursor.timestamp, cursor.sort_id, fetch_limit],
                |row| {
                    Ok(ActivityGroupChildRef {
                        id: row.get(0)?,
                        timestamp: row.get(1)?,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
    } else {
        let mut statement = connection.prepare(
            "SELECT selected.turn_id,selected.started_at
             FROM selected_activity_group_turns selected
                  INDEXED BY idx_selected_activity_group_turns_time
             ORDER BY selected.started_at DESC,selected.turn_id DESC
             LIMIT ?1 OFFSET ?2",
        )?;
        statement
            .query_map(params![fetch_limit, offset], |row| {
                Ok(ActivityGroupChildRef {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                })
            })?
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
                    item_id,
                    &child.timestamp,
                    None,
                    &child.id,
                )
            })
            .transpose()?
    } else {
        None
    };
    let mut items = query_activity_group_child_rows(connection, thread_id, &selected)?;
    query_activity_page_turn_totals_on(connection, thread_id, &mut items)?;
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more,
        next_cursor,
    })
}

fn query_activity_group_child_rows(
    connection: &Connection,
    thread_id: &str,
    selected: &[ActivityGroupChildRef],
) -> Result<Vec<ActivityItem>> {
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let requested = serde_json::to_string(
        &selected
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
    )?;
    let mut statement = connection.prepare(
        "SELECT t.id,t.rollout_id,t.agent_run_id,t.started_at,t.status,t.model,t.effort,
                NULLIF(substr(t.last_agent_message,1,?3),''),t.duration_ms,
                a.nickname,a.agent_path
         FROM json_each(?1) requested
         JOIN turns t ON t.id=requested.value AND t.thread_id=?2
         LEFT JOIN agent_runs a ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
         ORDER BY CAST(requested.key AS INTEGER)",
    )?;
    statement
        .query_map(
            params![requested, thread_id, ACTIVITY_PREVIEW_CHARS],
            activity_group_child_from_row,
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn activity_group_child_from_row(row: &Row<'_>) -> rusqlite::Result<ActivityItem> {
    let model = row.get::<_, Option<String>>(5)?;
    let review = model.as_deref() == Some("codex-auto-review");
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
        kind: if review { "review" } else { "subagent" }.into(),
        role: None,
        label: Some(agent_label.unwrap_or_else(|| {
            if review {
                "Automated review".into()
            } else {
                "Agent response".into()
            }
        })),
        body: bounded_preview(row.get(7)?),
        status: row.get(4)?,
        tool_name: None,
        duration_ms: row.get(8)?,
        model,
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
}

fn query_activity_page_turn_totals_on(
    connection: &Connection,
    thread_id: &str,
    items: &mut [ActivityItem],
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let requested = serde_json::to_string(
        &items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
    )?;
    let price_book = load_price_book_on(connection)?;
    let mut totals_by_turn = HashMap::<String, UsageAccumulator>::new();
    let mut statement = connection.prepare(
        "SELECT r.turn_key,r.activity_hour,r.model,
                COALESCE(SUM(r.input_tokens),0),
                COALESCE(SUM(r.cached_input_tokens),0),
                COALESCE(SUM(r.output_tokens),0),
                COALESCE(SUM(r.reasoning_tokens),0),
                COALESCE(SUM(r.total_tokens),0)
         FROM json_each(?1) requested
         JOIN usage_activity_rollups r
           ON r.thread_id=?2 AND r.turn_key=requested.value
         GROUP BY r.turn_key,r.activity_hour,r.model",
    )?;
    let mut rows = statement.query(params![requested, thread_id])?;
    while let Some(row) = rows.next()? {
        let turn_id = row.get::<_, String>(0)?;
        let activity_hour = row.get::<_, String>(1)?;
        let model = row.get::<_, String>(2)?;
        let input_tokens = row.get::<_, i64>(3)?.max(0);
        let cached_input_tokens = row.get::<_, i64>(4)?.max(0);
        let output_tokens = row.get::<_, i64>(5)?.max(0);
        let reasoning_tokens = row.get::<_, i64>(6)?.max(0);
        let total_tokens = row.get::<_, i64>(7)?.max(0) as u64;
        let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
            connection,
            &price_book,
            RollupScope::Turn {
                thread_id,
                turn_id: &turn_id,
            },
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals_by_turn.entry(turn_id).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    for item in items {
        item.usage = Some(totals_by_turn.remove(&item.id).unwrap_or_default().finish());
    }
    Ok(())
}

fn query_activity_group_totals_on(connection: &Connection, thread_id: &str) -> Result<UsageTotals> {
    let price_book = load_price_book_on(connection)?;
    let mut totals = UsageAccumulator::default();
    let mut statement = connection.prepare(
        "SELECT r.turn_key,r.activity_hour,r.model,
                COALESCE(SUM(r.input_tokens),0),
                COALESCE(SUM(r.cached_input_tokens),0),
                COALESCE(SUM(r.output_tokens),0),
                COALESCE(SUM(r.reasoning_tokens),0),
                COALESCE(SUM(r.total_tokens),0)
         FROM selected_activity_group_turns selected
         JOIN usage_activity_rollups r
           ON r.thread_id=?1 AND r.turn_key=selected.turn_id
         GROUP BY r.turn_key,r.activity_hour,r.model",
    )?;
    let mut rows = statement.query([thread_id])?;
    while let Some(row) = rows.next()? {
        let turn_id = row.get::<_, String>(0)?;
        let activity_hour = row.get::<_, String>(1)?;
        let model = row.get::<_, String>(2)?;
        let input_tokens = row.get::<_, i64>(3)?.max(0);
        let cached_input_tokens = row.get::<_, i64>(4)?.max(0);
        let output_tokens = row.get::<_, i64>(5)?.max(0);
        let reasoning_tokens = row.get::<_, i64>(6)?.max(0);
        let total_tokens = row.get::<_, i64>(7)?.max(0) as u64;
        let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
            connection,
            &price_book,
            RollupScope::Turn {
                thread_id,
                turn_id: &turn_id,
            },
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals.add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    Ok(totals.finish())
}

fn query_activity_group_status_on(connection: &Connection) -> Result<String> {
    let (running, attention) = connection.query_row(
        "SELECT COALESCE(MAX(t.status='running'),0),
                COALESCE(MAX(t.status NOT IN ('completed','success','allowed')),0)
         FROM selected_activity_group_turns selected
         JOIN turns t ON t.id=selected.turn_id",
        [],
        |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
    )?;
    Ok(if running {
        "running"
    } else if attention {
        "attention"
    } else {
        "completed"
    }
    .into())
}

fn query_activity_group_labels_on(connection: &Connection) -> Result<Option<String>> {
    let mut statement = connection.prepare(
        "WITH latest_labels AS (
             SELECT COALESCE(a.nickname,a.agent_path) label,t.started_at,t.id,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(a.nickname,a.agent_path)
                        ORDER BY t.started_at DESC,t.id DESC
                    ) label_rank
             FROM selected_activity_group_turns selected
             JOIN turns t ON t.id=selected.turn_id
             LEFT JOIN agent_runs a ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
         ), ranked_labels AS (
             SELECT label,started_at,id,COUNT(*) OVER () label_count,
                    ROW_NUMBER() OVER (ORDER BY started_at DESC,id DESC) preview_rank
             FROM latest_labels WHERE label IS NOT NULL AND label_rank=1
         )
         SELECT label,label_count FROM ranked_labels
         WHERE preview_rank<=?1 ORDER BY preview_rank",
    )?;
    let labels = statement
        .query_map([ACTIVITY_AGENT_LABEL_PREVIEW_LIMIT], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let label_count = labels.first().map_or(0, |(_, count)| *count);
    let labels = labels
        .into_iter()
        .map(|(label, _)| label)
        .collect::<Vec<_>>();
    Ok(agent_labels_preview(&labels, label_count))
}

fn query_activity_group_duration_on(connection: &Connection) -> Result<Option<i64>> {
    let mut statement = connection.prepare(
        "SELECT t.started_at,t.duration_ms
         FROM selected_activity_group_turns selected
         JOIN turns t ON t.id=selected.turn_id
         WHERE t.duration_ms IS NOT NULL
         ORDER BY t.started_at,t.id",
    )?;
    let mut rows = statement.query([])?;
    let mut current = None::<(DateTime<Utc>, DateTime<Utc>)>;
    let mut total_ms = 0_i64;
    while let Some(row) = rows.next()? {
        let started_at = row.get::<_, String>(0)?;
        let duration_ms = row.get::<_, i64>(1)?.max(0);
        let Some(start) = DateTime::parse_from_rfc3339(&started_at)
            .ok()
            .map(|value| value.with_timezone(&Utc))
        else {
            continue;
        };
        let Some(duration) = Duration::try_milliseconds(duration_ms) else {
            continue;
        };
        let Some(end) = start.checked_add_signed(duration) else {
            continue;
        };
        match current {
            Some((current_start, current_end)) if start <= current_end => {
                current = Some((current_start, current_end.max(end)));
            }
            Some((current_start, current_end)) => {
                total_ms =
                    total_ms.saturating_add((current_end - current_start).num_milliseconds());
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    Ok(current.map(|(start, end)| total_ms.saturating_add((end - start).num_milliseconds())))
}

#[derive(Default)]
struct ActivityDurationAccumulator {
    current: Option<(DateTime<Utc>, DateTime<Utc>)>,
    total_ms: i64,
}

impl ActivityDurationAccumulator {
    fn add(&mut self, started_at: &str, duration_ms: i64) {
        let Ok(started_at) = DateTime::parse_from_rfc3339(started_at) else {
            return;
        };
        let started_at = started_at.with_timezone(&Utc);
        let Some(duration) = Duration::try_milliseconds(duration_ms.max(0)) else {
            return;
        };
        let Some(ended_at) = started_at.checked_add_signed(duration) else {
            return;
        };
        if let Some((current_start, current_end)) = &mut self.current {
            if started_at <= *current_end {
                *current_end = (*current_end).max(ended_at);
                return;
            }
            self.total_ms = self
                .total_ms
                .saturating_add((*current_end - *current_start).num_milliseconds());
        }
        self.current = Some((started_at, ended_at));
    }

    fn finish(self) -> Option<i64> {
        let (started_at, ended_at) = self.current?;
        Some(
            self.total_ms
                .saturating_add((ended_at - started_at).num_milliseconds()),
        )
    }
}

pub(crate) fn agent_labels_preview(labels: &[String], label_count: u64) -> Option<String> {
    if labels.is_empty() {
        return None;
    }
    let mut preview = labels.join(" · ");
    let omitted = label_count.saturating_sub(labels.len() as u64);
    if omitted > 0 {
        preview.push_str(&format!(" · +{omitted} more"));
    }
    Some(preview)
}

pub(crate) fn parse_id(item_id: &str) -> Option<(bool, &str)> {
    item_id
        .strip_prefix("group:agents:")
        .map(|root_turn_id| (false, root_turn_id))
        .or_else(|| {
            item_id
                .strip_prefix("group:reviews:")
                .map(|root_turn_id| (true, root_turn_id))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_id_parser_reserves_only_known_namespaces() {
        assert_eq!(parse_id("group:agents:root"), Some((false, "root")));
        assert_eq!(parse_id("group:reviews:root"), Some((true, "root")));
        assert_eq!(parse_id("group:other:root"), None);
        assert_eq!(parse_id("group:agents:"), Some((false, "")));
    }

    #[test]
    fn label_preview_reports_omitted_unique_labels() {
        assert_eq!(agent_labels_preview(&[], 0), None);
        assert_eq!(
            agent_labels_preview(&["Ada".into(), "Lin".into()], 4),
            Some("Ada · Lin · +2 more".into())
        );
    }
}
