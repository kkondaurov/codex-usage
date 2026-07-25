use super::{
    attribution::{LocalDayRollup, SelectedActivityUsage, thread_rollup_local_days_on},
    groups::GroupSummaries,
    model::{ActivityCounts, ActivityDaySummary, ActivityItem, ActivityResponse},
    previews::{
        ACTIVITY_MESSAGE_PARSE_BYTES, ACTIVITY_MESSAGE_PARSE_EDGE_BYTES, ACTIVITY_PREVIEW_CHARS,
        activity_content_from_edges, bounded_preview, read_legacy_root,
    },
    selection::{ActivityRootScope, PreparedSelection},
};
use crate::{
    calendar::local_midnight,
    conversation::display::user_request_for_display,
    usage::{
        RollupScope, UsageAccumulator, UsageTotals, load_price_book_on, price_hourly_rollup_on,
    },
};
use anyhow::Result;
use chrono::{DateTime, Duration, Local, NaiveDate, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{HashMap, HashSet};

pub(crate) fn visible_thread_exists_on(connection: &Connection, thread_id: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM threads t WHERE t.id=?1 AND (
                EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
                OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
                OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id)
            )
         )",
        [thread_id],
        |row| row.get::<_, i64>(0).map(|visible| visible != 0),
    )?)
}

pub(crate) fn root_rollout_id_on(connection: &Connection, thread_id: &str) -> Result<String> {
    Ok(connection
        .query_row(
            "SELECT id FROM rollouts WHERE thread_id=?1
             ORDER BY CASE
                 WHEN id=?1 THEN 0
                 WHEN parent_rollout_id IS NULL AND parent_thread_id IS NULL THEN 1
                 ELSE 2 END,
                 started_at,id
             LIMIT 1",
            [thread_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_else(|| thread_id.to_owned()))
}

#[derive(Clone, Default)]
struct ActivityRootAggregate {
    counts: ActivityCounts,
    usage: UsageTotals,
}

#[derive(Default)]
struct ActivityBatch {
    user_messages: HashMap<String, Vec<String>>,
    roots: HashMap<String, ActivityRootAggregate>,
    groups: GroupSummaries,
}

impl ActivityBatch {
    fn load(selection: &PreparedSelection<'_>) -> Result<Self> {
        selection.with_connection(|connection| {
            let thread_id = selection.thread_id();
            let mut batch = Self::default();
            for root_turn_id in selection.root_turn_ids() {
                batch.roots.entry(root_turn_id.clone()).or_default();
            }
            if selection.root_turn_ids().is_empty() {
                return Ok(batch);
            }

            let mut statement = connection.prepare(
                "WITH user_events AS (
                 SELECT e.turn_id,e.timestamp,e.source_line,e.id,
                        CAST(COALESCE(NULLIF(e.body,''),NULLIF(m.content,'')) AS BLOB) content
                 FROM events e
                 JOIN selected_activity_roots selected ON selected.turn_id=e.turn_id
                 LEFT JOIN messages m
                   ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
                 WHERE e.thread_id=?1 AND e.turn_id IS NOT NULL
                   AND (e.kind='user' OR e.role='user')
             )
             SELECT turn_id,
                    CASE WHEN length(content)<=?2 THEN content
                         ELSE substr(content,1,?3) END,
                    CASE WHEN length(content)<=?2 THEN NULL
                         ELSE substr(content,-?3) END
             FROM user_events WHERE content IS NOT NULL
             ORDER BY timestamp,source_line,id",
            )?;
            let rows = statement.query_map(
                params![
                    thread_id,
                    ACTIVITY_MESSAGE_PARSE_BYTES,
                    ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                    ))
                },
            )?;
            let mut seen_messages = HashMap::<String, HashSet<String>>::new();
            for row in rows {
                let (turn_id, head, tail) = row?;
                let content = activity_content_from_edges(head, tail);
                if let Some(message) = user_request_for_display(&content)
                    && seen_messages
                        .entry(turn_id.clone())
                        .or_default()
                        .insert(message.clone())
                {
                    batch
                        .user_messages
                        .entry(turn_id)
                        .or_default()
                        .push(message);
                }
            }

            let mut statement = connection.prepare(
                "SELECT turn_id,SUM(model_calls) FROM (
                 SELECT u.turn_id,COUNT(*) model_calls FROM usage_facts u
                 JOIN selected_activity_roots selected ON selected.turn_id=u.turn_id
                 WHERE u.thread_id=?1 GROUP BY u.turn_id
                 UNION ALL
                 SELECT selected.turn_id,COUNT(*) model_calls
                 FROM selected_activity_roots selected
                 JOIN usage_facts u
                   ON u.thread_id=?1 AND u.turn_id IS NULL
                  AND (selected.open_left=1 OR u.timestamp>=selected.started_at)
                  AND (selected.next_started_at IS NULL
                       OR u.timestamp<selected.next_started_at)
                 GROUP BY selected.turn_id
             ) GROUP BY turn_id",
            )?;
            for row in statement.query_map([thread_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })? {
                let (turn_id, count) = row?;
                batch.roots.entry(turn_id).or_default().counts.model_calls = count.max(0) as u64;
            }

            let mut statement = connection.prepare(
                "SELECT root_turn_id,SUM(tool_calls) FROM (
                 SELECT selected.root_turn_id,COUNT(*) tool_calls
                 FROM selected_activity_turns selected
                 JOIN tool_calls tc ON tc.turn_id=selected.turn_id
                 WHERE tc.thread_id=?1
                 GROUP BY selected.root_turn_id
                 UNION ALL
                 SELECT selected.turn_id,COUNT(*)
                 FROM selected_activity_roots selected
                 JOIN tool_calls tc
                   ON tc.thread_id=?1
                  AND (selected.open_left=1 OR tc.started_at>=selected.started_at)
                  AND (selected.next_started_at IS NULL
                       OR tc.started_at<selected.next_started_at)
                 LEFT JOIN turns linked
                   ON linked.id=tc.turn_id AND linked.thread_id=tc.thread_id
                 WHERE linked.id IS NULL
                 GROUP BY selected.turn_id
             ) GROUP BY root_turn_id",
            )?;
            for row in statement.query_map([thread_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })? {
                let (root_turn_id, count) = row?;
                batch
                    .roots
                    .entry(root_turn_id)
                    .or_default()
                    .counts
                    .tool_calls = count.max(0) as u64;
            }

            let usage = SelectedActivityUsage::load(selection)?;
            batch.groups = GroupSummaries::load(selection, &usage)?;
            for root_turn_id in selection.root_turn_ids() {
                let root = batch.roots.entry(root_turn_id.clone()).or_default();
                root.usage = usage.root_totals(root_turn_id);
                let (agent_runs, reviews) = batch.groups.counts(root_turn_id);
                root.counts.agent_runs = agent_runs;
                root.counts.reviews = reviews;
            }
            Ok(batch)
        })
    }

    fn counts(&self, root_turn_id: &str) -> ActivityCounts {
        let mut counts = self
            .roots
            .get(root_turn_id)
            .map(|root| root.counts.clone())
            .unwrap_or_default();
        counts.follow_ups = self
            .user_messages
            .get(root_turn_id)
            .map_or(0, |messages| messages.len().saturating_sub(1) as u64);
        counts
    }

    fn exchange_totals(&self, root_turn_id: &str) -> UsageTotals {
        self.roots
            .get(root_turn_id)
            .map(|root| root.usage.clone())
            .unwrap_or_default()
    }
}

pub(crate) struct RootExchange {
    pub(crate) request: Option<String>,
    pub(crate) counts: ActivityCounts,
    pub(crate) usage: UsageTotals,
    pub(crate) groups: Vec<ActivityItem>,
}

pub(crate) fn read_exchange(
    selection: &PreparedSelection<'_>,
    root_turn_id: &str,
    root_rollout_id: &str,
    child_page_size: u64,
) -> Result<RootExchange> {
    let batch = ActivityBatch::load(selection)?;
    Ok(RootExchange {
        request: batch
            .user_messages
            .get(root_turn_id)
            .and_then(|messages| messages.first())
            .cloned(),
        counts: batch.counts(root_turn_id),
        usage: batch.exchange_totals(root_turn_id),
        groups: batch
            .groups
            .placeholders(root_turn_id, root_rollout_id, child_page_size),
    })
}

pub(crate) fn read_page_on(
    connection: &Connection,
    thread_id: &str,
    page: u64,
    page_size: u64,
) -> Result<ActivityResponse> {
    let root_rollout_id = root_rollout_id_on(connection, thread_id)?;
    let total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM turns WHERE thread_id=?1 AND rollout_id=?2",
        params![thread_id, root_rollout_id],
        |row| row.get(0),
    )?;
    let days = query_activity_day_summaries_batched(connection, thread_id)?;
    if total == 0 {
        let item = read_legacy_root(connection, thread_id, &root_rollout_id)?;
        let legacy_total = u64::from(item.is_some());
        let total_pages = legacy_total.div_ceil(page_size);
        let page = if total_pages == 0 {
            1
        } else {
            page.min(total_pages)
        };
        return Ok(ActivityResponse {
            items: if page == 1 {
                item.into_iter().collect()
            } else {
                Vec::new()
            },
            days,
            page,
            page_size,
            total: legacy_total,
            total_pages,
        });
    }
    let total = total.max(0) as u64;
    let total_pages = total.div_ceil(page_size);
    let page = page.min(total_pages.max(1));
    let mut statement = connection.prepare(
        "WITH root_turns AS (
             SELECT t.*,
                    LEAD(t.started_at) OVER (ORDER BY t.started_at,t.id) next_started_at,
                    ROW_NUMBER() OVER (ORDER BY t.started_at,t.id)=1 open_left
             FROM turns t
             WHERE t.thread_id=?1 AND t.rollout_id=?2
         )
         SELECT t.id,t.rollout_id,t.agent_run_id,t.started_at,t.status,t.model,t.effort,
                NULLIF(substr(t.last_agent_message,1,?5),''),t.duration_ms,a.nickname,a.agent_path,
                CASE WHEN t.last_agent_message IS NOT NULL OR EXISTS(
                    SELECT 1 FROM events e WHERE e.thread_id=t.thread_id AND e.turn_id=t.id
                    AND e.kind NOT IN ('turn_started','system','tool_output','tool_completed')
                ) THEN 1 ELSE 0 END,t.next_started_at,t.open_left
         FROM root_turns t LEFT JOIN agent_runs a
           ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
         ORDER BY t.started_at DESC,t.id DESC LIMIT ?3 OFFSET ?4",
    )?;
    let turn_rows = statement
        .query_map(
            params![
                thread_id,
                root_rollout_id,
                page_size as i64,
                page.saturating_sub(1)
                    .saturating_mul(page_size)
                    .min(i64::MAX as u64) as i64,
                ACTIVITY_PREVIEW_CHARS,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, i64>(11)? != 0,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, i64>(13)? != 0,
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let root_scopes = turn_rows
        .iter()
        .map(|turn| ActivityRootScope {
            id: turn.0.clone(),
            started_at: turn.3.clone(),
            next_started_at: turn.12.clone(),
            open_left: turn.13,
        })
        .collect::<Vec<_>>();
    let selection =
        PreparedSelection::prepare(connection, thread_id, &root_rollout_id, &root_scopes)?;
    let batch = ActivityBatch::load(&selection)?;
    let mut items = Vec::with_capacity(turn_rows.len());
    for (
        turn_id,
        rollout_id,
        agent_run_id,
        started_at,
        status,
        model,
        effort,
        last_message,
        duration,
        agent_nickname,
        agent_path,
        has_details,
        _next_started_at,
        _open_left,
    ) in turn_rows
    {
        let messages = batch
            .user_messages
            .get(&turn_id)
            .cloned()
            .unwrap_or_default();
        let counts = batch.counts(&turn_id);
        let totals = batch.exchange_totals(&turn_id);
        let label = bounded_preview(messages.first().cloned()).unwrap_or_else(|| {
            if model.as_deref() == Some("codex-auto-review") {
                "Automated review".to_owned()
            } else {
                "Conversation".to_owned()
            }
        });
        items.push(ActivityItem {
            id: turn_id.clone(),
            turn_id: Some(turn_id),
            rollout_id,
            agent_run_id,
            agent_label: agent_nickname.or(agent_path),
            timestamp: started_at,
            kind: "exchange".into(),
            role: Some("user".into()),
            label: Some(label),
            body: bounded_preview(last_message),
            status: Some(status),
            tool_name: None,
            duration_ms: duration,
            model,
            effort,
            has_details,
            children: Vec::new(),
            child_page: None,
            child_page_size: None,
            child_total: None,
            child_has_more: None,
            child_next_cursor: None,
            usage: Some(totals),
            counts: Some(counts),
        });
    }
    Ok(ActivityResponse {
        items,
        days,
        page,
        page_size,
        total,
        total_pages,
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn query_activity_day_summaries_batched(
    connection: &Connection,
    thread_id: &str,
) -> Result<Vec<ActivityDaySummary>> {
    let raw_thread_bounds = connection.query_row(
        "SELECT started_at,last_event_at FROM threads WHERE id=?1",
        [thread_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let thread_bounds = parse_activity_thread_bounds(&raw_thread_bounds.0, &raw_thread_bounds.1);
    let intervals_may_cross_local_date = match thread_bounds {
        Some((start, end)) => {
            let occupied_end = end
                .checked_sub_signed(Duration::milliseconds(1))
                .unwrap_or(end);
            start.with_timezone(&Local).date_naive()
                != occupied_end.with_timezone(&Local).date_naive()
        }
        None => true,
    };
    let mut dates = HashSet::new();
    let mut turn_intervals = Vec::new();
    let mut statement = connection.prepare(
        "SELECT started_at,completed_at,duration_ms
         FROM turns INDEXED BY idx_turns_thread_time
         WHERE thread_id=?1",
    )?;
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<i64>>(2)?,
        ))
    })? {
        let (raw_start, raw_end, duration_ms) = row?;
        record_activity_row(
            &mut dates,
            &mut turn_intervals,
            true,
            &raw_start,
            raw_end.as_deref(),
            duration_ms,
            thread_bounds,
        );
    }
    drop(statement);

    // Point activity is overwhelmingly concentrated in a handful of calendar days, but a
    // large session can contain hundreds of thousands of rows on each one. Seek the first
    // indexed timestamp at or after each local midnight, record that occupied date, then
    // jump straight to the next midnight. This makes the collapsed Activity query
    // proportional to occupied days instead of raw events/tool calls and also avoids the
    // temporary B-trees created by GROUP BY date(...,'localtime').
    let mut statement = connection.prepare(
        "SELECT MIN(activity_at) FROM (
             SELECT (
                 SELECT timestamp FROM events INDEXED BY idx_events_thread_time
                 WHERE thread_id=?1 AND timestamp>=?2
                 ORDER BY timestamp,source_line LIMIT 1
             ) activity_at
             UNION ALL
             SELECT (
                 SELECT timestamp FROM messages INDEXED BY idx_messages_thread_time
                 WHERE thread_id=?1 AND timestamp>=?2
                 ORDER BY timestamp LIMIT 1
             )
             UNION ALL
             SELECT (
                 SELECT started_at FROM tool_calls INDEXED BY idx_tools_thread_time
                 WHERE thread_id=?1 AND started_at>=?2
                 ORDER BY started_at LIMIT 1
             )
         )
         WHERE activity_at IS NOT NULL",
    )?;
    let mut cursor = "0001-01-01T00:00:00".to_owned();
    loop {
        let next_timestamp = statement
            .query_row(params![thread_id, cursor], |row| {
                row.get::<_, Option<String>>(0)
            })?
            .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok());
        let Some(next_timestamp) = next_timestamp else {
            break;
        };
        let date = next_timestamp.with_timezone(&Local).date_naive();
        dates.insert(date);
        let Some(next_date) = date.succ_opt() else {
            break;
        };
        let next_cursor = local_midnight(next_date)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        if next_cursor <= cursor {
            break;
        }
        cursor = next_cursor;
    }
    drop(statement);

    // Exact interval handling is only needed when the trustworthy thread bounds cross a
    // local date. The ordinary (and pathological dense) one-day case performs no interval
    // scan at all; multi-day sessions keep the previous exact cross-midnight behavior.
    if intervals_may_cross_local_date {
        let mut statement = connection.prepare(
            "SELECT timestamp,duration_ms
             FROM events INDEXED BY idx_events_thread_time
             WHERE thread_id=?1 AND COALESCE(duration_ms,0)>0",
        )?;
        let rows = statement.query_map([thread_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })?;
        for row in rows {
            let (raw_start, duration_ms) = row?;
            record_activity_row(
                &mut dates,
                &mut turn_intervals,
                false,
                &raw_start,
                None,
                duration_ms,
                thread_bounds,
            );
        }

        let mut statement = connection.prepare(
            "SELECT started_at,completed_at,duration_ms
             FROM tool_calls INDEXED BY idx_tools_thread_time
             WHERE thread_id=?1
               AND (completed_at IS NOT NULL OR COALESCE(duration_ms,0)>0)",
        )?;
        let rows = statement.query_map([thread_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        for row in rows {
            let (raw_start, raw_end, duration_ms) = row?;
            record_activity_row(
                &mut dates,
                &mut turn_intervals,
                false,
                &raw_start,
                raw_end.as_deref(),
                duration_ms,
                thread_bounds,
            );
        }
    }

    let price_book = load_price_book_on(connection)?;
    let mut totals_by_date = HashMap::<NaiveDate, UsageAccumulator>::new();
    let mut statement = connection.prepare(
        "SELECT activity_hour,model,
                COALESCE(SUM(input_tokens),0),
                COALESCE(SUM(cached_input_tokens),0),
                COALESCE(SUM(output_tokens),0),
                COALESCE(SUM(reasoning_tokens),0),
                COALESCE(SUM(total_tokens),0)
         FROM usage_activity_rollups
         WHERE thread_id=?1
         GROUP BY activity_hour,model",
    )?;
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?.max(0),
            row.get::<_, i64>(3)?.max(0),
            row.get::<_, i64>(4)?.max(0),
            row.get::<_, i64>(5)?.max(0),
            row.get::<_, i64>(6)?.max(0) as u64,
        ))
    })? {
        let (
            activity_hour,
            model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        ) = row?;
        match thread_rollup_local_days_on(
            connection,
            &price_book,
            thread_id,
            &activity_hour,
            &model,
        )? {
            LocalDayRollup::Single(date) => {
                let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
                    connection,
                    &price_book,
                    RollupScope::Thread { thread_id },
                    &activity_hour,
                    &model,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    total_tokens,
                )?;
                dates.insert(date);
                totals_by_date.entry(date).or_default().add_group(
                    input_tokens as u64,
                    cached_input_tokens as u64,
                    output_tokens as u64,
                    reasoning_tokens as u64,
                    total_tokens,
                    known_cost_numerator,
                    unpriced_tokens,
                );
            }
            LocalDayRollup::Split(split_totals) => {
                for (date, totals) in split_totals {
                    dates.insert(date);
                    totals_by_date.entry(date).or_default().merge(totals);
                }
            }
        }
    }

    let mut dates = dates.into_iter().collect::<Vec<_>>();
    dates.sort_unstable_by(|left, right| right.cmp(left));
    Ok(dates
        .into_iter()
        .filter_map(|date| {
            let (start, end) = activity_day_window(date)?;
            Some(ActivityDaySummary {
                date: date.to_string(),
                duration_ms: activity_union_duration(&turn_intervals, start, end),
                totals: totals_by_date.remove(&date).unwrap_or_default().finish(),
            })
        })
        .collect())
}

fn record_activity_row(
    dates: &mut HashSet<NaiveDate>,
    turn_intervals: &mut Vec<(DateTime<Utc>, DateTime<Utc>)>,
    is_turn: bool,
    raw_start: &str,
    raw_end: Option<&str>,
    duration_ms: Option<i64>,
    thread_bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
) {
    let Ok(start) = DateTime::parse_from_rfc3339(raw_start) else {
        return;
    };
    let start = start.with_timezone(&Utc);
    let end = raw_end
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .or_else(|| {
            duration_ms.and_then(|value| {
                Duration::try_milliseconds(value.max(0))
                    .and_then(|duration| start.checked_add_signed(duration))
            })
        })
        .unwrap_or(start);

    if end > start {
        if let Some((bounded_start, bounded_end)) =
            bounded_activity_interval(start, end, thread_bounds)
        {
            if is_turn {
                turn_intervals.push((bounded_start, bounded_end));
            }
            insert_activity_interval_dates(dates, bounded_start, bounded_end);
        } else {
            // Preserve the existence of malformed or uncorroborated activity without
            // trusting it to manufacture an unbounded run of inferred calendar days.
            dates.insert(start.with_timezone(&Local).date_naive());
        }
    } else {
        dates.insert(start.with_timezone(&Local).date_naive());
    }
}

// A single model/tool operation occupying more than a year is corrupt telemetry for this
// application. Long-lived threads remain fully supported: their real point events still add
// every occupied date, while an implausible interval cannot manufacture a huge response.
const MAX_ACTIVITY_INTERVAL_DAYS: i64 = 366;

fn parse_activity_thread_bounds(
    raw_start: &str,
    raw_end: &str,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let Ok(start) = DateTime::parse_from_rfc3339(raw_start) else {
        return None;
    };
    let Ok(end) = DateTime::parse_from_rfc3339(raw_end) else {
        return None;
    };
    let start = start.with_timezone(&Utc);
    let end = end.with_timezone(&Utc);
    (end > start).then_some((start, end))
}

fn bounded_activity_interval(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    thread_bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let (thread_start, thread_end) = thread_bounds?;
    let start = start.max(thread_start);
    let end = end.min(thread_end);
    if end <= start || end.signed_duration_since(start).num_days() > MAX_ACTIVITY_INTERVAL_DAYS {
        return None;
    }
    Some((start, end))
}

fn insert_activity_interval_dates(
    dates: &mut HashSet<NaiveDate>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) {
    let mut date = start.with_timezone(&Local).date_naive();
    let occupied_end = end
        .checked_sub_signed(Duration::milliseconds(1))
        .unwrap_or(end);
    let end_date = occupied_end.with_timezone(&Local).date_naive();
    loop {
        dates.insert(date);
        if date >= end_date {
            break;
        }
        let Some(next_date) = date.succ_opt() else {
            break;
        };
        date = next_date;
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn activity_day_window(date: NaiveDate) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let next_date = date.succ_opt()?;
    let start = local_midnight(date);
    let end = local_midnight(next_date);
    (start < end).then_some((start, end))
}

fn activity_union_duration(
    source_intervals: &[(DateTime<Utc>, DateTime<Utc>)],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> u64 {
    let mut intervals = source_intervals
        .iter()
        .filter_map(|(interval_start, interval_end)| {
            let interval_start = (*interval_start).max(start);
            let interval_end = (*interval_end).min(end);
            if interval_end > interval_start {
                Some((interval_start, interval_end))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    intervals.sort_by_key(|interval| interval.0);
    let mut total_ms = 0_i64;
    let mut current: Option<(DateTime<Utc>, DateTime<Utc>)> = None;
    for (interval_start, interval_end) in intervals {
        match current {
            Some((current_start, current_end)) if interval_start <= current_end => {
                current = Some((current_start, current_end.max(interval_end)));
            }
            Some((current_start, current_end)) => {
                total_ms =
                    total_ms.saturating_add((current_end - current_start).num_milliseconds());
                current = Some((interval_start, interval_end));
            }
            None => current = Some((interval_start, interval_end)),
        }
    }
    if let Some((current_start, current_end)) = current {
        total_ms = total_ms.saturating_add((current_end - current_start).num_milliseconds());
    }
    total_ms.max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::root_rollout_id_on;
    use crate::storage::Db;

    #[test]
    fn activity_root_rollout_selection_prefers_exact_then_lineage_free_then_any() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('exact-thread','Exact','2026-07-01T00:00:00Z','2026-07-01T03:00:00Z'),
                    ('root-thread','Root','2026-07-01T00:00:00Z','2026-07-01T03:00:00Z'),
                    ('child-thread','Child','2026-07-01T00:00:00Z','2026-07-01T03:00:00Z'),
                    ('fallback-thread','Fallback','2026-07-01T00:00:00Z','2026-07-01T03:00:00Z');

                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
                 ) VALUES
                    ('exact-early-root','exact-thread',NULL,NULL,
                     '2026-07-01T00:00:00Z','2026-07-01T00:30:00Z',0),
                    ('exact-thread','exact-thread','parent-rollout','parent-thread',
                     '2026-07-01T03:00:00Z','2026-07-01T03:30:00Z',0),
                    ('root-older-child','root-thread','parent-rollout','parent-thread',
                     '2026-07-01T00:00:00Z','2026-07-01T00:30:00Z',0),
                    ('root-z','root-thread',NULL,NULL,
                     '2026-07-01T01:00:00Z','2026-07-01T01:30:00Z',0),
                    ('root-a','root-thread',NULL,NULL,
                     '2026-07-01T01:00:00Z','2026-07-01T01:30:00Z',0),
                    ('child-z','child-thread','parent-rollout','parent-thread',
                     '2026-07-01T01:00:00Z','2026-07-01T01:30:00Z',0),
                    ('child-a','child-thread','parent-rollout','parent-thread',
                     '2026-07-01T01:00:00Z','2026-07-01T01:30:00Z',0);",
            )
            .unwrap();

        assert_eq!(
            root_rollout_id_on(&connection, "exact-thread").unwrap(),
            "exact-thread"
        );
        assert_eq!(
            root_rollout_id_on(&connection, "root-thread").unwrap(),
            "root-a"
        );
        assert_eq!(
            root_rollout_id_on(&connection, "child-thread").unwrap(),
            "child-a"
        );
        assert_eq!(
            root_rollout_id_on(&connection, "fallback-thread").unwrap(),
            "fallback-thread"
        );
    }
}
