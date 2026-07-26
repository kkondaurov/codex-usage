use crate::{
    calendar::canonical_utc_timestamp,
    costing::{PriceBook, UsdAmount},
    usage::{RollupScope, UsageAccumulator, load_price_book_on, price_hourly_rollup_on},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Timelike, Utc};
use rusqlite::{Connection, Row, params};
use std::collections::HashMap;
use unicode_normalization::UnicodeNormalization;

pub(crate) const MAX_SEARCH_CHARS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SessionListSort {
    Recent,
    Cost,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SessionListRequest {
    pub(crate) start: Option<String>,
    pub(crate) end: Option<String>,
    pub(crate) project: Option<String>,
    pub(crate) search: Option<String>,
    pub(crate) sort: SessionListSort,
    pub(crate) page: u64,
    pub(crate) page_size: u64,
}

pub(crate) struct SessionListRecord {
    pub(crate) id: String,
    pub(crate) started_at: String,
    pub(crate) last_event_at: String,
    pub(crate) title: String,
    pub(crate) project: String,
    pub(crate) branch: Option<String>,
    pub(crate) message_count: u64,
    pub(crate) turn_count: u64,
    pub(crate) agent_count: u64,
    pub(crate) tool_count: u64,
    pub(crate) total_tokens: u64,
    pub(crate) cost_usd: Option<UsdAmount>,
    pub(crate) unpriced_tokens: u64,
    pub(crate) lifetime_cost_usd: Option<UsdAmount>,
    pub(crate) lifetime_unpriced_tokens: u64,
}

pub(crate) struct SessionListPage {
    pub(crate) items: Vec<SessionListRecord>,
    pub(crate) page: u64,
    pub(crate) page_size: u64,
    pub(crate) total: u64,
    pub(crate) total_pages: u64,
}

fn populate_session_sort_costs_on(
    connection: &Connection,
    start: Option<&str>,
    end: Option<&str>,
    project: Option<&str>,
    q_filter: Option<&str>,
) -> Result<()> {
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS session_sort_costs(
             thread_id TEXT PRIMARY KEY,
             total_tokens INTEGER NOT NULL,
             cost_numerator TEXT NOT NULL,
             unpriced_tokens INTEGER NOT NULL
         ) WITHOUT ROWID;
         DELETE FROM session_sort_costs;",
    )?;
    let price_book = load_price_book_on(connection)?;
    if start.is_none() && end.is_none() {
        let groups = {
            let mut statement = connection.prepare(
                "SELECT r.thread_id,r.activity_hour,r.model,
                        COALESCE(SUM(r.input_tokens),0),
                        COALESCE(SUM(r.cached_input_tokens),0),
                        COALESCE(SUM(r.output_tokens),0),
                        COALESCE(SUM(r.reasoning_tokens),0),
                        COALESCE(SUM(r.total_tokens),0)
                 FROM usage_activity_rollups r
                 JOIN threads t ON t.id=r.thread_id
                 WHERE (?1 IS NULL OR t.project=?1)
                   AND (?2 IS NULL OR EXISTS(
                        SELECT 1 FROM session_search_matches search
                        WHERE search.thread_id=t.id
                   ))
                 GROUP BY r.thread_id,r.activity_hour,r.model",
            )?;
            statement
                .query_map(params![project, q_filter], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?.max(0),
                        row.get::<_, i64>(4)?.max(0),
                        row.get::<_, i64>(5)?.max(0),
                        row.get::<_, i64>(6)?.max(0),
                        row.get::<_, i64>(7)?.max(0) as u64,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut aggregates = HashMap::<String, UsageAccumulator>::new();
        for (
            thread_id,
            activity_hour,
            model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        ) in groups
        {
            let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
                connection,
                &price_book,
                RollupScope::Thread {
                    thread_id: &thread_id,
                },
                &activity_hour,
                &model,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                total_tokens,
            )?;
            aggregates.entry(thread_id).or_default().add_group(
                input_tokens as u64,
                cached_input_tokens as u64,
                output_tokens as u64,
                reasoning_tokens as u64,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
            );
        }
        let mut insert = connection.prepare(
            "INSERT INTO session_sort_costs(
                 thread_id,total_tokens,cost_numerator,unpriced_tokens
             ) VALUES(?1,?2,?3,?4)",
        )?;
        for (thread_id, aggregate) in aggregates {
            insert.execute(params![
                thread_id,
                aggregate.total_tokens().min(i64::MAX as u64) as i64,
                sortable_cost_numerator(aggregate.known_cost_numerator()),
                aggregate.unpriced_tokens().min(i64::MAX as u64) as i64,
            ])?;
        }
        return Ok(());
    }
    let start_timestamp = start
        .map(DateTime::parse_from_rfc3339)
        .transpose()
        .context("invalid bounded session-sort start")?
        .map(|value| value.with_timezone(&Utc));
    let end_timestamp = end
        .map(DateTime::parse_from_rfc3339)
        .transpose()
        .context("invalid bounded session-sort end")?
        .map(|value| value.with_timezone(&Utc));
    let rollup_start = start_timestamp
        .map(utc_hour_ceil)
        .transpose()?
        .map(canonical_utc_timestamp);
    let rollup_end = end_timestamp
        .map(utc_hour_floor)
        .transpose()?
        .map(canonical_utc_timestamp);
    let has_complete_hours = !matches!(
        (&rollup_start, &rollup_end),
        (Some(start), Some(end)) if start >= end
    );
    let mut aggregates = HashMap::<String, UsageAccumulator>::new();
    if has_complete_hours {
        let groups = {
            let mut statement = connection.prepare(
                "SELECT r.thread_id,r.activity_hour,r.model,
                        COALESCE(SUM(r.input_tokens),0),
                        COALESCE(SUM(r.cached_input_tokens),0),
                        COALESCE(SUM(r.output_tokens),0),
                        COALESCE(SUM(r.reasoning_tokens),0),
                        COALESCE(SUM(r.total_tokens),0)
                 FROM usage_activity_rollups r
                 JOIN threads t ON t.id=r.thread_id
                 WHERE (?1 IS NULL OR r.activity_hour>=?1)
                   AND (?2 IS NULL OR r.activity_hour<?2)
                   AND (?3 IS NULL OR t.project=?3)
                   AND (?4 IS NULL OR EXISTS(
                        SELECT 1 FROM session_search_matches search
                        WHERE search.thread_id=t.id
                   ))
                 GROUP BY r.thread_id,r.activity_hour,r.model",
            )?;
            statement
                .query_map(
                    params![rollup_start, rollup_end, project, q_filter],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?.max(0),
                            row.get::<_, i64>(4)?.max(0),
                            row.get::<_, i64>(5)?.max(0),
                            row.get::<_, i64>(6)?.max(0),
                            row.get::<_, i64>(7)?.max(0) as u64,
                        ))
                    },
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (
            thread_id,
            activity_hour,
            model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        ) in groups
        {
            let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
                connection,
                &price_book,
                RollupScope::Thread {
                    thread_id: &thread_id,
                },
                &activity_hour,
                &model,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                total_tokens,
            )?;
            aggregates.entry(thread_id).or_default().add_group(
                input_tokens as u64,
                cached_input_tokens as u64,
                output_tokens as u64,
                reasoning_tokens as u64,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
            );
        }
    }

    let mut raw_windows = Vec::<(DateTime<Utc>, DateTime<Utc>)>::new();
    if let Some(start) = start_timestamp {
        let mut boundary_end = utc_hour_ceil(start)?;
        if let Some(end) = end_timestamp {
            boundary_end = boundary_end.min(end);
        }
        if boundary_end > start {
            raw_windows.push((start, boundary_end));
        }
    }
    if let Some(end) = end_timestamp {
        let mut boundary_start = utc_hour_floor(end)?;
        if let Some(start) = start_timestamp {
            boundary_start = boundary_start.max(start);
        }
        if end > boundary_start {
            raw_windows.push((boundary_start, end));
        }
    }
    raw_windows.sort_unstable_by_key(|(start, _)| *start);
    let mut merged_windows = Vec::<(DateTime<Utc>, DateTime<Utc>)>::new();
    for (start, end) in raw_windows {
        if let Some((_, previous_end)) = merged_windows.last_mut()
            && start <= *previous_end
        {
            *previous_end = (*previous_end).max(end);
        } else {
            merged_windows.push((start, end));
        }
    }
    for (start, end) in merged_windows {
        accumulate_session_boundary_usage_on(
            connection,
            &price_book,
            project,
            q_filter,
            &canonical_utc_timestamp(start),
            &canonical_utc_timestamp(end),
            &mut aggregates,
        )?;
    }
    let mut insert = connection.prepare(
        "INSERT INTO session_sort_costs(
             thread_id,total_tokens,cost_numerator,unpriced_tokens
         ) VALUES(?1,?2,?3,?4)",
    )?;
    for (thread_id, aggregate) in aggregates {
        insert.execute(params![
            thread_id,
            aggregate.total_tokens().min(i64::MAX as u64) as i64,
            sortable_cost_numerator(aggregate.known_cost_numerator()),
            aggregate.unpriced_tokens().min(i64::MAX as u64) as i64,
        ])?;
    }
    Ok(())
}

fn utc_hour_floor(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    value
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .context("UTC timestamp cannot be rounded to an hour")
}

fn utc_hour_ceil(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let floor = utc_hour_floor(value)?;
    if floor == value {
        Ok(floor)
    } else {
        floor
            .checked_add_signed(Duration::hours(1))
            .context("UTC timestamp has no following hour")
    }
}

#[allow(clippy::too_many_arguments)]
fn accumulate_session_boundary_usage_on(
    connection: &Connection,
    price_book: &PriceBook,
    project: Option<&str>,
    q_filter: Option<&str>,
    start: &str,
    end: &str,
    aggregates: &mut HashMap<String, UsageAccumulator>,
) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT u.thread_id,u.timestamp,u.model,
                u.input_tokens,u.cached_input_tokens,u.output_tokens,
                u.reasoning_tokens,u.total_tokens
         FROM usage_facts u INDEXED BY idx_usage_time
         JOIN threads t ON t.id=u.thread_id
         WHERE u.timestamp>=?1 AND u.timestamp<?2
           AND (?3 IS NULL OR t.project=?3)
           AND (?4 IS NULL OR EXISTS(
                SELECT 1 FROM session_search_matches search
                WHERE search.thread_id=t.id
           ))
         ORDER BY u.timestamp",
    )?;
    let mut rows = statement.query(params![start, end, project, q_filter])?;
    while let Some(row) = rows.next()? {
        let thread_id = row.get::<_, String>(0)?;
        let timestamp = row.get::<_, String>(1)?;
        let model = row.get::<_, String>(2)?;
        aggregates.entry(thread_id).or_default().add_fact(
            price_book,
            &timestamp,
            &model,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        );
    }
    Ok(())
}

fn sortable_cost_numerator(value: i128) -> String {
    // SQLite INTEGER stops at i64, while valid token/price products need i128.
    // Every cost is nonnegative, so one fixed-width decimal string gives us an
    // exact lexicographic sort key without converting arithmetic to REAL.
    format!("{:039}", value.max(0))
}

fn query_selected_session_totals_on(
    connection: &Connection,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<HashMap<String, UsageAccumulator>> {
    let price_book = load_price_book_on(connection)?;
    let mut totals = HashMap::<String, UsageAccumulator>::new();
    if start.is_none() && end.is_none() {
        let groups = {
            let mut statement = connection.prepare(
                "SELECT r.thread_id,r.activity_hour,r.model,
                        COALESCE(SUM(r.input_tokens),0),
                        COALESCE(SUM(r.cached_input_tokens),0),
                        COALESCE(SUM(r.output_tokens),0),
                        COALESCE(SUM(r.reasoning_tokens),0),
                        COALESCE(SUM(r.total_tokens),0)
                 FROM usage_activity_rollups r
                 JOIN selected_sessions s ON s.id=r.thread_id
                 GROUP BY r.thread_id,r.activity_hour,r.model",
            )?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?.max(0),
                        row.get::<_, i64>(4)?.max(0),
                        row.get::<_, i64>(5)?.max(0),
                        row.get::<_, i64>(6)?.max(0),
                        row.get::<_, i64>(7)?.max(0) as u64,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (
            thread_id,
            activity_hour,
            model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        ) in groups
        {
            let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
                connection,
                &price_book,
                RollupScope::Thread {
                    thread_id: &thread_id,
                },
                &activity_hour,
                &model,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                total_tokens,
            )?;
            totals.entry(thread_id).or_default().add_group(
                input_tokens as u64,
                cached_input_tokens as u64,
                output_tokens as u64,
                reasoning_tokens as u64,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
            );
        }
        return Ok(totals);
    }

    let mut statement = connection.prepare(
        "SELECT u.thread_id,u.timestamp,u.model,u.input_tokens,u.cached_input_tokens,
                u.output_tokens,u.reasoning_tokens,u.total_tokens
         FROM usage_facts u JOIN selected_sessions s ON s.id=u.thread_id
         WHERE (?1 IS NULL OR u.timestamp>=?1) AND (?2 IS NULL OR u.timestamp<?2)
         ORDER BY u.thread_id,u.timestamp,u.id",
    )?;
    let mut rows = statement.query(params![start, end])?;
    while let Some(row) = rows.next()? {
        let thread_id = row.get::<_, String>(0)?;
        let timestamp = row.get::<_, String>(1)?;
        let model = row.get::<_, String>(2)?;
        totals.entry(thread_id).or_default().add_fact(
            &price_book,
            &timestamp,
            &model,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        );
    }
    Ok(totals)
}

pub(crate) fn read_session_page_on(
    connection: &Connection,
    request: &SessionListRequest,
) -> Result<SessionListPage> {
    let start = request.start.as_deref();
    let end = request.end.as_deref();
    let project = request.project.as_deref();
    let q_filter = request
        .search
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    anyhow::ensure!(
        q_filter.is_none_or(|value| value.chars().count() <= MAX_SEARCH_CHARS),
        "session search exceeds the {MAX_SEARCH_CHARS}-character limit"
    );
    let bounded = start.is_some() || end.is_some();
    let offset = request
        .page
        .saturating_sub(1)
        .saturating_mul(request.page_size)
        .min(i64::MAX as u64) as i64;

    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS session_candidates(
             thread_id TEXT PRIMARY KEY,last_activity TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE TEMP TABLE IF NOT EXISTS selected_sessions(
             id TEXT PRIMARY KEY,started_at TEXT NOT NULL,last_event_at TEXT NOT NULL,
             title TEXT NOT NULL,project TEXT NOT NULL,branch TEXT,
             total_tokens INTEGER,unpriced_tokens INTEGER
         ) WITHOUT ROWID;
         CREATE TEMP TABLE IF NOT EXISTS session_search_matches(
             thread_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM session_candidates;
         DELETE FROM selected_sessions;
         DELETE FROM session_search_matches;",
    )?;
    populate_session_search_matches_on(connection, q_filter)?;

    if bounded {
        connection.execute(
            "INSERT INTO session_candidates(thread_id,last_activity)
             SELECT thread_id,MAX(timestamp) FROM (
                SELECT thread_id,timestamp FROM events
                 WHERE (?1 IS NULL OR timestamp>=?1) AND (?2 IS NULL OR timestamp<?2)
                UNION ALL SELECT thread_id,timestamp FROM usage_facts
                 WHERE (?1 IS NULL OR timestamp>=?1) AND (?2 IS NULL OR timestamp<?2)
                UNION ALL SELECT thread_id,timestamp FROM messages
                 WHERE (?1 IS NULL OR timestamp>=?1) AND (?2 IS NULL OR timestamp<?2)
             ) GROUP BY thread_id",
            params![start, end],
        )?;
    }

    let metadata_filter = r#"
        AND (?1 IS NULL OR t.project=?1)
        AND (?2 IS NULL OR EXISTS(
             SELECT 1 FROM session_search_matches search
             WHERE search.thread_id=t.id
        ))
    "#;
    let total_sql = if bounded {
        format!(
            "SELECT COUNT(*) FROM session_candidates c
             JOIN threads t ON t.id=c.thread_id WHERE 1=1 {metadata_filter}"
        )
    } else {
        format!(
            "SELECT COUNT(*) FROM threads t WHERE (
                EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
                OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
                OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id)
             ) {metadata_filter}"
        )
    };
    let total: i64 =
        connection.query_row(&total_sql, params![project, q_filter], |row| row.get(0))?;

    if request.sort == SessionListSort::Cost {
        populate_session_sort_costs_on(connection, start, end, project, q_filter)?;
        let source = if bounded {
            "session_candidates c JOIN threads t ON t.id=c.thread_id"
        } else {
            "threads t"
        };
        let last_event = if bounded {
            "c.last_activity"
        } else {
            "t.last_event_at"
        };
        let visibility = if bounded {
            ""
        } else {
            "AND (EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
               OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
               OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id))"
        };
        let sql = format!(
            r#"
            INSERT INTO selected_sessions
            SELECT t.id,t.started_at,{last_event},COALESCE(t.title,'Untitled session'),
                COALESCE(t.project,'—'),t.branch,
                COALESCE(u.total_tokens,0),COALESCE(u.unpriced_tokens,0)
            FROM {source} LEFT JOIN session_sort_costs u ON u.thread_id=t.id
            WHERE (?1 IS NULL OR t.project=?1)
              AND (?2 IS NULL OR EXISTS(
                   SELECT 1 FROM session_search_matches search
                   WHERE search.thread_id=t.id
              ))
              {visibility}
            ORDER BY CASE WHEN COALESCE(u.unpriced_tokens,0)=0 THEN 0 ELSE 1 END,
                CASE WHEN COALESCE(u.unpriced_tokens,0)=0
                     THEN COALESCE(u.cost_numerator,'000000000000000000000000000000000000000')
                     ELSE '' END DESC,
                CASE WHEN COALESCE(u.unpriced_tokens,0)>0
                     THEN COALESCE(u.total_tokens,0) ELSE 0 END DESC,
                {last_event} DESC,t.id DESC
            LIMIT ?3 OFFSET ?4
            "#
        );
        connection.execute(
            &sql,
            params![project, q_filter, request.page_size as i64, offset],
        )?;
    } else {
        let (source, last_event, visibility) = if bounded {
            (
                "session_candidates c JOIN threads t ON t.id=c.thread_id",
                "c.last_activity",
                "",
            )
        } else {
            (
                "threads t",
                "t.last_event_at",
                "AND (EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
                   OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
                   OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id))",
            )
        };
        let sql = format!(
            r#"
            INSERT INTO selected_sessions(
                id,started_at,last_event_at,title,project,branch,
                total_tokens,unpriced_tokens
            )
            SELECT t.id,t.started_at,{last_event},COALESCE(t.title,'Untitled session'),
                COALESCE(t.project,'—'),t.branch,NULL,NULL
            FROM {source}
            WHERE (?1 IS NULL OR t.project=?1)
              AND (?2 IS NULL OR EXISTS(
                   SELECT 1 FROM session_search_matches search
                   WHERE search.thread_id=t.id
              ))
              {visibility}
            ORDER BY {last_event} DESC,t.id DESC LIMIT ?3 OFFSET ?4
            "#
        );
        connection.execute(
            &sql,
            params![project, q_filter, request.page_size as i64, offset],
        )?;
    }

    // Everything below this point is bounded by the selected page. In the
    // default recent view, no corpus-wide event/message/tool aggregate is run.
    let order = if request.sort == SessionListSort::Cost {
        "CASE WHEN COALESCE(s.unpriced_tokens,0)=0 THEN 0 ELSE 1 END,
         CASE WHEN COALESCE(s.unpriced_tokens,0)=0
              THEN COALESCE(
                   (SELECT exact.cost_numerator FROM session_sort_costs exact
                    WHERE exact.thread_id=s.id),
                   '000000000000000000000000000000000000000'
              ) ELSE '' END DESC,
         CASE WHEN COALESCE(s.unpriced_tokens,0)>0
              THEN COALESCE(s.total_tokens,0) ELSE 0 END DESC,
         s.last_event_at DESC,s.id DESC"
    } else {
        "s.last_event_at DESC,s.id DESC"
    };
    let sql = format!(
        r#"
        WITH message_page AS (
            SELECT m.thread_id,COUNT(*) value FROM selected_sessions s
            JOIN messages m ON m.thread_id=s.id
            WHERE (?1 IS NULL OR m.timestamp>=?1) AND (?2 IS NULL OR m.timestamp<?2)
            GROUP BY m.thread_id
        ), turn_page AS (
            SELECT t.thread_id,COUNT(*) value FROM selected_sessions s
            JOIN turns t ON t.thread_id=s.id
            WHERE (?1 IS NULL OR t.started_at>=?1) AND (?2 IS NULL OR t.started_at<?2)
            GROUP BY t.thread_id
        ), tool_page AS (
            SELECT tc.thread_id,COUNT(*) value FROM selected_sessions s
            JOIN tool_calls tc ON tc.thread_id=s.id
            WHERE (?1 IS NULL OR tc.started_at>=?1) AND (?2 IS NULL OR tc.started_at<?2)
            GROUP BY tc.thread_id
        ), agent_page AS (
            SELECT a.thread_id,COUNT(*) value FROM selected_sessions s
            JOIN agent_runs a ON a.thread_id=s.id
            WHERE a.id<>a.thread_id
              AND (?1 IS NULL OR a.started_at>=?1)
              AND (?2 IS NULL OR a.started_at<?2)
            GROUP BY a.thread_id
        )
        SELECT s.id,s.started_at,s.last_event_at,s.title,s.project,s.branch,
            COALESCE(m.value,0),COALESCE(t.value,0),COALESCE(a.value,0),COALESCE(tc.value,0),
            0,'0',0,'0',0
        FROM selected_sessions s
        LEFT JOIN message_page m ON m.thread_id=s.id
        LEFT JOIN turn_page t ON t.thread_id=s.id
        LEFT JOIN tool_page tc ON tc.thread_id=s.id
        LEFT JOIN agent_page a ON a.thread_id=s.id
        ORDER BY {order}
        "#
    );
    let page_totals = query_selected_session_totals_on(connection, start, end)?;
    let lifetime_totals = if start.is_none() && end.is_none() {
        page_totals.clone()
    } else {
        query_selected_session_totals_on(connection, None, None)?
    };
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![start, end], session_from_row)?;
    let mut items = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    for item in &mut items {
        let totals = page_totals
            .get(&item.id)
            .cloned()
            .unwrap_or_default()
            .finish();
        let lifetime = lifetime_totals
            .get(&item.id)
            .cloned()
            .unwrap_or_default()
            .finish();
        item.total_tokens = totals.total_tokens;
        item.cost_usd = totals.cost_usd;
        item.unpriced_tokens = totals.unpriced_tokens;
        item.lifetime_cost_usd = lifetime.cost_usd;
        item.lifetime_unpriced_tokens = lifetime.unpriced_tokens;
    }
    let total = total.max(0) as u64;
    Ok(SessionListPage {
        items,
        page: request.page,
        page_size: request.page_size,
        total,
        total_pages: total.div_ceil(request.page_size),
    })
}

fn normalize_search_text(value: &str) -> String {
    value
        .nfkc()
        .flat_map(char::to_lowercase)
        .collect::<String>()
}

fn populate_session_search_matches_on(connection: &Connection, query: Option<&str>) -> Result<()> {
    let Some(query) = query else {
        return Ok(());
    };
    let needle = normalize_search_text(query.trim());
    let mut select = connection.prepare(
        "SELECT id,COALESCE(title,''),COALESCE(project,''),COALESCE(branch,'') FROM threads",
    )?;
    let mut insert =
        connection.prepare("INSERT OR IGNORE INTO session_search_matches(thread_id) VALUES(?1)")?;
    let mut rows = select.query([])?;
    while let Some(row) = rows.next()? {
        let id = row.get::<_, String>(0)?;
        let matches = (0..4).any(|index| {
            let value = if index == 0 {
                id.as_str()
            } else {
                row.get_ref(index)
                    .ok()
                    .and_then(|value| value.as_str().ok())
                    .unwrap_or("")
            };
            normalize_search_text(value).contains(&needle)
        });
        if matches {
            insert.execute([&id])?;
        }
    }
    Ok(())
}

fn session_from_row(row: &Row<'_>) -> rusqlite::Result<SessionListRecord> {
    let total_tokens = row.get::<_, i64>(10)?.max(0) as u64;
    let known_cost_numerator = cost_numerator_from_row(row, 11)?;
    let unpriced_tokens = row.get::<_, i64>(12)?.max(0) as u64;
    let lifetime_known_cost_numerator = cost_numerator_from_row(row, 13)?;
    let lifetime_unpriced_tokens = row.get::<_, i64>(14)?.max(0) as u64;
    Ok(SessionListRecord {
        id: row.get(0)?,
        started_at: row.get(1)?,
        last_event_at: row.get(2)?,
        title: row.get(3)?,
        project: row.get(4)?,
        branch: row.get(5)?,
        message_count: row.get::<_, i64>(6)?.max(0) as u64,
        turn_count: row.get::<_, i64>(7)?.max(0) as u64,
        agent_count: row.get::<_, i64>(8)?.max(0) as u64,
        tool_count: row.get::<_, i64>(9)?.max(0) as u64,
        total_tokens,
        cost_usd: (unpriced_tokens == 0)
            .then_some(UsdAmount::from_cost_numerator(known_cost_numerator)),
        unpriced_tokens,
        lifetime_cost_usd: (lifetime_unpriced_tokens == 0).then_some(
            UsdAmount::from_cost_numerator(lifetime_known_cost_numerator),
        ),
        lifetime_unpriced_tokens,
    })
}

fn cost_numerator_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<i128> {
    let value = row.get::<_, String>(index)?;
    value.parse::<i128>().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{SessionListRequest, SessionListSort, read_session_page_on};

    #[test]
    fn session_cost_sort_and_record_keep_fixed_point_differences() {
        let temp = tempfile::tempdir().unwrap();
        let db = crate::storage::Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                 ) VALUES(
                    'precise-sort','1970-01-01T00:00:00.000000000Z',
                    1000000000,1000000000,1,
                    'USD','manual'
                 );
                 INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('a-higher','Higher','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T12:00:00.000000000Z'),
                    ('z-lower','Lower','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T12:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('higher-rollout','a-higher','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T12:00:00.000000000Z'),
                    ('lower-rollout','z-lower','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T12:00:00.000000000Z');
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES
                    ('higher-usage-1','a-higher','higher-rollout',
                     '2026-07-15T12:00:00.000000000Z',1,'precise-sort',
                     3199999999,0,1,0,3200000000),
                    ('higher-usage-2','a-higher','higher-rollout',
                     '2026-07-15T12:00:00.000000000Z',2,'precise-sort',
                     3199999999,0,0,0,3199999999),
                    ('higher-usage-3','a-higher','higher-rollout',
                     '2026-07-15T12:00:00.000000000Z',3,'precise-sort',
                     3199999999,0,0,0,3199999999),
                    ('lower-usage-1','z-lower','lower-rollout',
                     '2026-07-15T12:00:00.000000000Z',1,'precise-sort',
                     3199999999,0,0,0,3199999999),
                    ('lower-usage-2','z-lower','lower-rollout',
                     '2026-07-15T12:00:00.000000000Z',2,'precise-sort',
                     3199999999,0,0,0,3199999999),
                    ('lower-usage-3','z-lower','lower-rollout',
                     '2026-07-15T12:00:00.000000000Z',3,'precise-sort',
                     3199999999,0,0,0,3199999999);",
            )
            .unwrap();

        let first_page = read_session_page_on(
            &connection,
            &SessionListRequest {
                start: None,
                end: None,
                project: None,
                search: None,
                sort: SessionListSort::Cost,
                page: 1,
                page_size: 1,
            },
        )
        .unwrap();
        let second_page = read_session_page_on(
            &connection,
            &SessionListRequest {
                start: None,
                end: None,
                project: None,
                search: None,
                sort: SessionListSort::Cost,
                page: 2,
                page_size: 1,
            },
        )
        .unwrap();
        assert_eq!(first_page.total, 2);
        assert_eq!(first_page.total_pages, 2);
        assert_eq!(second_page.total, 2);
        assert_eq!(second_page.total_pages, 2);
        assert_eq!(first_page.items.len(), 1);
        assert_eq!(second_page.items.len(), 1);
        assert_eq!(first_page.items[0].id, "a-higher");
        assert_eq!(second_page.items[0].id, "z-lower");

        let higher = first_page.items[0].cost_usd.unwrap().cost_numerator();
        let lower = second_page.items[0].cost_usd.unwrap().cost_numerator();
        assert!(higher > i64::MAX as i128);
        assert!(lower > i64::MAX as i128);
        assert_eq!(higher - lower, 1);
    }
}
