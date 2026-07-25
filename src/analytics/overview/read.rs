use super::{
    HeatmapDay, OverviewDayBucket, OverviewPeriods, OverviewResponse, OverviewSessionRank,
    OverviewUsageAggregate, OverviewYearResponse, ProjectDriver, TopSessionResponse,
    overview_period_summary, overview_summary_bounds, overview_year_days,
    rank_overview_year_projects, rank_overview_year_sessions,
};
use crate::{
    calendar::canonical_utc_timestamp,
    costing::{PriceBook, UsdAmount},
    usage::{TotalsScope, UsageTotals, load_price_book_on, read_all_time_totals_on},
};
use anyhow::{Context, Result};
use chrono::Local;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlBucketBounds {
    ordinal: usize,
    start_at: String,
    end_at: String,
}

const OVERVIEW_SUMMARY_USAGE_SQL: &str = "SELECT timestamp,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
         FROM usage_facts INDEXED BY idx_usage_time
         WHERE timestamp>=?1 AND timestamp<?2
         ORDER BY timestamp";

const OVERVIEW_SUMMARY_SESSIONS_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         ), current_bounds AS MATERIALIZED (
            SELECT MAX(CASE WHEN ordinal=0 THEN start_at END) today_start,
                   MAX(CASE WHEN ordinal=2 THEN start_at END) week_start,
                   MAX(CASE WHEN ordinal=4 THEN start_at END) month_start,
                   MIN(CASE WHEN ordinal IN (0,2,4) THEN start_at END) scan_start,
                   MAX(CASE WHEN ordinal=0 THEN end_at END) end_at
            FROM bounds
         ), latest AS MATERIALIZED (
            SELECT t.id,
                   MAX(
                     COALESCE((SELECT MAX(e.timestamp)
                               FROM events e INDEXED BY idx_events_thread_time
                               WHERE e.thread_id=t.id
                                 AND e.timestamp>=b.scan_start
                                 AND e.timestamp<b.end_at),''),
                     COALESCE((SELECT MAX(u.timestamp)
                               FROM usage_facts u INDEXED BY idx_usage_thread_time
                               WHERE u.thread_id=t.id
                                 AND u.timestamp>=b.scan_start
                                 AND u.timestamp<b.end_at),''),
                     COALESCE((SELECT MAX(m.timestamp)
                               FROM messages m INDEXED BY idx_messages_thread_time
                               WHERE m.thread_id=t.id
                                 AND m.timestamp>=b.scan_start
                                 AND m.timestamp<b.end_at),'')
                   ) last_at,
                   b.today_start,b.week_start,b.month_start,b.end_at
            FROM threads t CROSS JOIN current_bounds b
         )
         SELECT COALESCE(SUM(last_at>=today_start AND last_at<end_at),0),
                COALESCE(SUM(last_at>=week_start AND last_at<end_at),0),
                COALESCE(SUM(last_at>=month_start AND last_at<end_at),0)
         FROM latest";

const OVERVIEW_SUMMARY_MESSAGES_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         ), current_bounds AS MATERIALIZED (
            SELECT MAX(CASE WHEN ordinal=0 THEN start_at END) today_start,
                   MAX(CASE WHEN ordinal=2 THEN start_at END) week_start,
                   MAX(CASE WHEN ordinal=4 THEN start_at END) month_start,
                   MIN(CASE WHEN ordinal IN (0,2,4) THEN start_at END) scan_start,
                   MAX(CASE WHEN ordinal=0 THEN end_at END) end_at
            FROM bounds
         )
         SELECT COALESCE(SUM(m.timestamp>=b.today_start AND m.timestamp<b.end_at),0),
                COALESCE(SUM(m.timestamp>=b.week_start AND m.timestamp<b.end_at),0),
                COALESCE(SUM(m.timestamp>=b.month_start AND m.timestamp<b.end_at),0)
         FROM current_bounds b
         LEFT JOIN messages m INDEXED BY idx_messages_time_thread
           ON m.timestamp>=b.scan_start AND m.timestamp<b.end_at";

const OVERVIEW_YEAR_USAGE_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         )
         SELECT /* overview-year-usage */
                b.ordinal,u.thread_id,u.model,
                COALESCE(SUM(u.input_tokens-MIN(u.input_tokens,u.cached_input_tokens)),0),
                COALESCE(SUM(MIN(u.input_tokens,u.cached_input_tokens)),0),
                COALESCE(SUM(u.output_tokens),0),
                COALESCE(SUM(u.total_tokens),0),
                MIN(u.timestamp),MAX(u.timestamp)
         FROM bounds b
         JOIN usage_facts u INDEXED BY idx_usage_overview_year
           ON u.timestamp>=b.start_at AND u.timestamp<b.end_at
         GROUP BY b.ordinal,u.thread_id,u.model
         ORDER BY b.ordinal";

const OVERVIEW_YEAR_MESSAGE_ACTIVITY_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         )
         SELECT b.ordinal,m.thread_id,COUNT(*) message_count
         FROM bounds b
         JOIN messages m
           ON m.timestamp>=b.start_at AND m.timestamp<b.end_at
         GROUP BY b.ordinal,m.thread_id
         ORDER BY b.ordinal,m.thread_id";

const OVERVIEW_EVENT_DAY_SEEK_SQL: &str = "SELECT timestamp FROM events
         WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3
         ORDER BY timestamp LIMIT 1";

pub(crate) fn read_summary_on(connection: &Connection) -> Result<OverviewResponse> {
    let period_bounds = overview_summary_bounds(Local::now().date_naive());
    let bounds = period_bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| SqlBucketBounds {
            ordinal,
            start_at: bound.start_timestamp(),
            end_at: bound.end_timestamp(),
        })
        .collect::<Vec<_>>();
    let usage = read_summary_usage_on(connection, &bounds)?;
    let session_counts = read_summary_sessions_on(connection, &bounds)?;
    let message_counts = read_summary_messages_on(connection, &bounds)?;
    let updated_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='last_ingest_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(OverviewResponse {
        updated_at,
        periods: OverviewPeriods {
            today: overview_period_summary(
                "Today",
                &period_bounds[0],
                usage[0].clone(),
                &usage[1],
                session_counts[0],
                message_counts[0],
            ),
            week: overview_period_summary(
                "This week",
                &period_bounds[2],
                usage[2].clone(),
                &usage[3],
                session_counts[1],
                message_counts[1],
            ),
            month: overview_period_summary(
                "This month",
                &period_bounds[4],
                usage[4].clone(),
                &usage[5],
                session_counts[2],
                message_counts[2],
            ),
        },
    })
}

fn summary_bounds_json(bounds: &[SqlBucketBounds]) -> Result<String> {
    anyhow::ensure!(
        bounds.len() == 6,
        "overview summary requires six period bounds"
    );
    Ok(serde_json::to_string(bounds)?)
}

fn read_summary_usage_on(
    connection: &Connection,
    bounds: &[SqlBucketBounds],
) -> Result<Vec<UsageTotals>> {
    anyhow::ensure!(
        bounds.len() == 6,
        "overview summary requires six period bounds"
    );
    let range_start = bounds
        .iter()
        .map(|bound| bound.start_at.as_str())
        .min()
        .context("overview summary requires a range start")?;
    let range_end = bounds
        .iter()
        .map(|bound| bound.end_at.as_str())
        .max()
        .context("overview summary requires a range end")?;
    let price_book = load_price_book_on(connection)?;
    let mut totals = vec![UsageTotals::default(); bounds.len()];
    let mut cost_numerators = vec![0i128; bounds.len()];
    let mut statement = connection.prepare(OVERVIEW_SUMMARY_USAGE_SQL)?;
    let mut rows = statement.query(params![range_start, range_end])?;
    while let Some(row) = rows.next()? {
        let timestamp = row.get_ref(0)?.as_str()?;
        let model = row.get_ref(1)?.as_str()?;
        let input_tokens = row.get::<_, i64>(2)?.max(0);
        let cached_input_tokens = row.get::<_, i64>(3)?.max(0);
        let billed_cached_tokens = input_tokens.min(cached_input_tokens);
        let output_tokens = row.get::<_, i64>(4)?.max(0);
        let reasoning_tokens = row.get::<_, i64>(5)?.max(0);
        let total_tokens = row.get::<_, i64>(6)?.max(0);
        let row_cost = price_book.price_at(model, timestamp).map(|(_, price)| {
            price.cost_numerator(
                input_tokens - billed_cached_tokens,
                billed_cached_tokens,
                output_tokens,
            )
        });
        for (ordinal, bound) in bounds.iter().enumerate() {
            if timestamp < bound.start_at.as_str() || timestamp >= bound.end_at.as_str() {
                continue;
            }
            let aggregate = &mut totals[ordinal];
            aggregate.input_tokens = aggregate.input_tokens.saturating_add(input_tokens as u64);
            aggregate.cached_input_tokens = aggregate
                .cached_input_tokens
                .saturating_add(cached_input_tokens as u64);
            aggregate.output_tokens = aggregate.output_tokens.saturating_add(output_tokens as u64);
            aggregate.reasoning_tokens = aggregate
                .reasoning_tokens
                .saturating_add(reasoning_tokens as u64);
            aggregate.total_tokens = aggregate.total_tokens.saturating_add(total_tokens as u64);
            if let Some(row_cost) = row_cost {
                cost_numerators[ordinal] = cost_numerators[ordinal].saturating_add(row_cost);
            } else {
                aggregate.unpriced_tokens = aggregate
                    .unpriced_tokens
                    .saturating_add(total_tokens as u64);
            }
        }
    }
    for (aggregate, cost_numerator) in totals.iter_mut().zip(cost_numerators) {
        aggregate.known_cost_numerator = cost_numerator;
        *aggregate = std::mem::take(aggregate).finish();
    }
    Ok(totals)
}

fn read_summary_sessions_on(
    connection: &Connection,
    bounds: &[SqlBucketBounds],
) -> Result<[u64; 3]> {
    let bounds = summary_bounds_json(bounds)?;
    connection
        .query_row(OVERVIEW_SUMMARY_SESSIONS_SQL, [bounds], |row| {
            Ok([
                row.get::<_, i64>(0)?.max(0) as u64,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
            ])
        })
        .map_err(Into::into)
}

fn read_summary_messages_on(
    connection: &Connection,
    bounds: &[SqlBucketBounds],
) -> Result<[u64; 3]> {
    let bounds = summary_bounds_json(bounds)?;
    connection
        .query_row(OVERVIEW_SUMMARY_MESSAGES_SQL, [bounds], |row| {
            Ok([
                row.get::<_, i64>(0)?.max(0) as u64,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
            ])
        })
        .map_err(Into::into)
}

type OverviewYearUsage = (
    Vec<OverviewUsageAggregate>,
    HashMap<String, OverviewUsageAggregate>,
    Vec<HashSet<String>>,
);

fn exceptional_group_cost_on(
    connection: &Connection,
    price_book: &PriceBook,
    start: &str,
    end: &str,
    thread_id: &str,
    model: &str,
) -> Result<(i128, u64)> {
    let mut statement = connection.prepare(
        "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
         FROM usage_facts
         WHERE timestamp>=?1 AND timestamp<?2 AND thread_id=?3 AND model=?4",
    )?;
    let mut rows = statement.query(params![start, end, thread_id, model])?;
    let mut cost_numerator = 0i128;
    let mut unpriced_tokens = 0u64;
    while let Some(row) = rows.next()? {
        let timestamp = row.get_ref(0)?.as_str()?;
        let input_tokens = row.get::<_, i64>(1)?.max(0);
        let cached_tokens = input_tokens.min(row.get::<_, i64>(2)?.max(0));
        let output_tokens = row.get::<_, i64>(3)?.max(0);
        let total_tokens = row.get::<_, i64>(4)?.max(0) as u64;
        if let Some((_, price)) = price_book.price_at(model, timestamp) {
            cost_numerator = cost_numerator.saturating_add(price.cost_numerator(
                input_tokens - cached_tokens,
                cached_tokens,
                output_tokens,
            ));
        } else {
            unpriced_tokens = unpriced_tokens.saturating_add(total_tokens);
        }
    }
    Ok((cost_numerator, unpriced_tokens))
}

fn read_year_usage_on(
    connection: &Connection,
    buckets: &[OverviewDayBucket],
) -> Result<OverviewYearUsage> {
    let bounds = buckets
        .iter()
        .enumerate()
        .map(|(ordinal, bucket)| SqlBucketBounds {
            ordinal,
            start_at: canonical_utc_timestamp(bucket.start),
            end_at: canonical_utc_timestamp(bucket.end),
        })
        .collect::<Vec<_>>();
    let bounds_json = serde_json::to_string(&bounds)?;
    let price_book = load_price_book_on(connection)?;
    let mut daily = vec![OverviewUsageAggregate::default(); buckets.len()];
    let mut sessions = HashMap::<String, OverviewUsageAggregate>::new();
    let mut activity_sessions = vec![HashSet::<String>::new(); buckets.len()];
    let groups = {
        let mut statement = connection.prepare(OVERVIEW_YEAR_USAGE_SQL)?;
        statement
            .query_map([bounds_json], |row| {
                Ok((
                    row.get::<_, i64>(0)?.max(0) as usize,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?.max(0),
                    row.get::<_, i64>(4)?.max(0),
                    row.get::<_, i64>(5)?.max(0),
                    row.get::<_, i64>(6)?.max(0) as u64,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (
        day_index,
        thread_id,
        model,
        uncached_input_tokens,
        cached_input_tokens,
        output_tokens,
        total_tokens,
        first_timestamp,
        last_timestamp,
    ) in groups
    {
        if day_index >= buckets.len() {
            anyhow::bail!("overview usage query returned an invalid bucket ordinal");
        }
        let first_price = price_book.price_at(&model, &first_timestamp);
        let last_price = price_book.price_at(&model, &last_timestamp);
        let has_price_boundary =
            price_book.group_has_price_boundary(&model, &first_timestamp, &last_timestamp);
        let (known_cost_numerator, unpriced_tokens) = match (first_price, last_price) {
            (Some((first_index, price)), Some((last_index, _)))
                if first_index == last_index && !has_price_boundary =>
            {
                (
                    price.cost_numerator(uncached_input_tokens, cached_input_tokens, output_tokens),
                    0,
                )
            }
            (None, None)
                if price_book.group_has_no_price(&model, &first_timestamp, &last_timestamp) =>
            {
                (0, total_tokens)
            }
            _ => exceptional_group_cost_on(
                connection,
                &price_book,
                &bounds[day_index].start_at,
                &bounds[day_index].end_at,
                &thread_id,
                &model,
            )?,
        };
        daily[day_index].add_sums(
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
            &last_timestamp,
        );
        activity_sessions[day_index].insert(thread_id.clone());
        sessions.entry(thread_id).or_default().add_sums(
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
            &last_timestamp,
        );
    }
    Ok((daily, sessions, activity_sessions))
}

fn read_year_activity_on(
    connection: &Connection,
    buckets: &[OverviewDayBucket],
    activity_sessions: &mut [HashSet<String>],
) -> Result<Vec<u64>> {
    let bounds = buckets
        .iter()
        .enumerate()
        .map(|(ordinal, bucket)| SqlBucketBounds {
            ordinal,
            start_at: canonical_utc_timestamp(bucket.start),
            end_at: canonical_utc_timestamp(bucket.end),
        })
        .collect::<Vec<_>>();
    let bounds_json = serde_json::to_string(&bounds)?;
    let mut statement = connection.prepare(OVERVIEW_YEAR_MESSAGE_ACTIVITY_SQL)?;
    let mut rows = statement.query([bounds_json])?;
    let mut message_counts = vec![0u64; buckets.len()];
    while let Some(row) = rows.next()? {
        let ordinal = row.get::<_, i64>(0)?.max(0) as usize;
        if ordinal >= buckets.len() {
            anyhow::bail!("overview activity query returned an invalid bucket ordinal");
        }
        activity_sessions[ordinal].insert(row.get::<_, String>(1)?);
        message_counts[ordinal] =
            message_counts[ordinal].saturating_add(row.get::<_, i64>(2)?.max(0) as u64);
    }
    let thread_ids = {
        let mut statement = connection.prepare("SELECT id FROM threads")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let year_end = bounds
        .last()
        .context("overview activity requires at least one bucket")?
        .end_at
        .clone();
    let mut statement = connection.prepare(OVERVIEW_EVENT_DAY_SEEK_SQL)?;
    for thread_id in thread_ids {
        let mut cursor = bounds[0].start_at.clone();
        loop {
            let timestamp = statement
                .query_row(params![thread_id, cursor, year_end], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?;
            let Some(timestamp) = timestamp else {
                break;
            };
            let ordinal = bounds.partition_point(|bound| bound.end_at <= timestamp);
            if ordinal >= bounds.len() || timestamp < bounds[ordinal].start_at {
                anyhow::bail!("overview event query returned an out-of-range timestamp");
            }
            activity_sessions[ordinal].insert(thread_id.clone());
            bounds[ordinal].end_at.clone_into(&mut cursor);
        }
    }
    Ok(message_counts)
}

fn read_year_projects_on(
    connection: &Connection,
    sessions: &HashMap<String, OverviewUsageAggregate>,
) -> Result<Vec<ProjectDriver>> {
    let mut statement = connection.prepare("SELECT id,COALESCE(project,'—') FROM threads")?;
    let projects = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()?;
    Ok(rank_overview_year_projects(sessions, &projects))
}

pub(crate) fn read_year_on(
    connection: &Connection,
    year: i32,
    start: &str,
    end: &str,
) -> Result<OverviewYearResponse> {
    let buckets = overview_year_days(year)?;
    let (daily_usage, session_usage, mut activity_sessions) =
        read_year_usage_on(connection, &buckets)?;
    let message_counts = read_year_activity_on(connection, &buckets, &mut activity_sessions)?;
    let heatmap = buckets
        .iter()
        .zip(daily_usage)
        .zip(message_counts)
        .zip(activity_sessions)
        .map(|(((bucket, usage), message_count), sessions)| HeatmapDay {
            date: bucket.date.clone(),
            cost_usd: usage.cost_usd(),
            session_count: sessions.len() as u64,
            message_count,
            total_tokens: usage.total_tokens,
        })
        .collect();
    let top_projects = read_year_projects_on(connection, &session_usage)?;
    let ranked_sessions = rank_overview_year_sessions(&session_usage);
    let top_sessions = read_top_sessions_on(connection, start, end, &ranked_sessions)?
        .into_iter()
        .map(TopSessionResponse::from)
        .collect();
    Ok(OverviewYearResponse {
        year,
        heatmap,
        top_projects,
        top_sessions,
    })
}

#[derive(Clone, Debug)]
struct TopSessionRecord {
    id: String,
    root_thread_id: String,
    started_at: String,
    last_event_at: String,
    title: String,
    project: String,
    branch: Option<String>,
    message_count: u64,
    turn_count: u64,
    agent_count: u64,
    tool_count: u64,
    total_tokens: u64,
    cost_usd: Option<UsdAmount>,
    unpriced_tokens: u64,
    lifetime_cost_usd: Option<UsdAmount>,
    lifetime_unpriced_tokens: u64,
}

impl From<TopSessionRecord> for TopSessionResponse {
    fn from(record: TopSessionRecord) -> Self {
        Self {
            id: record.id,
            root_thread_id: record.root_thread_id,
            started_at: record.started_at,
            last_event_at: record.last_event_at,
            title: record.title,
            project: record.project,
            branch: record.branch,
            message_count: record.message_count,
            turn_count: record.turn_count,
            agent_count: record.agent_count,
            tool_count: record.tool_count,
            total_tokens: record.total_tokens,
            cost_usd: record.cost_usd,
            unpriced_tokens: record.unpriced_tokens,
            lifetime_cost_usd: record.lifetime_cost_usd,
            lifetime_unpriced_tokens: record.lifetime_unpriced_tokens,
        }
    }
}

fn read_top_sessions_on(
    connection: &Connection,
    start: &str,
    end: &str,
    ranked: &[OverviewSessionRank],
) -> Result<Vec<TopSessionRecord>> {
    let mut rows = Vec::with_capacity(ranked.len());
    let mut statement = connection.prepare(
        "SELECT t.id,t.started_at,
                COALESCE((SELECT MAX(activity_at) FROM (
                    SELECT MAX(timestamp) activity_at FROM events
                     WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3
                    UNION ALL SELECT MAX(timestamp) FROM usage_facts
                     WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3
                    UNION ALL SELECT MAX(timestamp) FROM messages
                     WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3
                )),t.last_event_at),
                COALESCE(t.title,'Untitled session'),COALESCE(t.project,'—'),t.branch,
                (SELECT COUNT(*) FROM messages
                 WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3),
                (SELECT COUNT(*) FROM turns
                 WHERE thread_id=?1 AND started_at>=?2 AND started_at<?3),
                (SELECT COUNT(*) FROM agent_runs
                 WHERE thread_id=?1 AND id<>thread_id
                   AND started_at>=?2 AND started_at<?3),
                (SELECT COUNT(*) FROM tool_calls
                 WHERE thread_id=?1 AND started_at>=?2 AND started_at<?3),
                ?4,?5,?6,'0',0
         FROM threads t
         WHERE t.id=?1",
    )?;
    for rank in ranked {
        let mut row = statement.query_row(
            params![
                rank.thread_id,
                start,
                end,
                rank.total_tokens.min(i64::MAX as u64) as i64,
                rank.known_cost_numerator.to_string(),
                rank.unpriced_tokens.min(i64::MAX as u64) as i64
            ],
            decode_top_session_record,
        )?;
        let lifetime = read_all_time_totals_on(
            connection,
            TotalsScope::Thread {
                thread_id: &rank.thread_id,
            },
        )?;
        row.lifetime_cost_usd = lifetime.cost_usd;
        row.lifetime_unpriced_tokens = lifetime.unpriced_tokens;
        rows.push(row);
    }
    Ok(rows)
}

fn decode_top_session_record(row: &Row<'_>) -> rusqlite::Result<TopSessionRecord> {
    let id: String = row.get(0)?;
    let total_tokens = row.get::<_, i64>(10)?.max(0) as u64;
    let known_cost_numerator = parse_cost_numerator(row, 11)?;
    let unpriced_tokens = row.get::<_, i64>(12)?.max(0) as u64;
    let lifetime_known_cost_numerator = parse_cost_numerator(row, 13)?;
    let lifetime_unpriced_tokens = row.get::<_, i64>(14)?.max(0) as u64;
    Ok(TopSessionRecord {
        root_thread_id: id.clone(),
        id,
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

fn parse_cost_numerator(row: &Row<'_>, index: usize) -> rusqlite::Result<i128> {
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
    use super::{
        OVERVIEW_EVENT_DAY_SEEK_SQL, OVERVIEW_SUMMARY_MESSAGES_SQL, OVERVIEW_SUMMARY_SESSIONS_SQL,
        OVERVIEW_SUMMARY_USAGE_SQL, OVERVIEW_YEAR_USAGE_SQL, SqlBucketBounds,
        read_summary_messages_on, read_summary_sessions_on, read_summary_usage_on,
        read_year_activity_on, read_year_on, read_year_usage_on,
    };
    use crate::{
        analytics::overview::{OverviewPeriodBound, overview_period_summary, overview_year_days},
        storage::Db,
        usage::{TotalsScope, read_totals_on},
    };
    use chrono::{DateTime, Utc};
    use rusqlite::params;
    use std::{
        collections::HashSet,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    static TRACE_LOCK: Mutex<()> = Mutex::new(());
    static OVERVIEW_USAGE_QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_overview_usage_query(sql: &str) {
        if sql.contains("overview-year-usage") {
            OVERVIEW_USAGE_QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn summary_bounds() -> Vec<SqlBucketBounds> {
        [
            ("2026-08-01", "2026-08-02"),
            ("2026-07-31", "2026-08-01"),
            ("2026-07-27", "2026-08-02"),
            ("2026-07-20", "2026-07-27"),
            ("2026-08-01", "2026-08-02"),
            ("2026-07-01", "2026-08-01"),
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, (start, end))| SqlBucketBounds {
            ordinal,
            start_at: format!("{start}T00:00:00.000000000Z"),
            end_at: format!("{end}T00:00:00.000000000Z"),
        })
        .collect()
    }

    #[test]
    fn summary_readers_match_global_totals_and_nested_activity() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_prices(
                    model_id,effective_from,effective_to,
                    input_microusd_per_million,cached_input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES(
                    'summary-model','1970-01-01T00:00:00.000000000Z',NULL,
                    1000000,500000,2000000,'USD','manual'
                 );
                 INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('current','Current','2026-08-01T10:00:00.000000000Z',
                     '2026-08-01T11:00:00.000000000Z'),
                    ('prior','Prior','2026-07-31T10:00:00.000000000Z',
                     '2026-07-31T11:00:00.000000000Z'),
                    ('event-only','Event only','2026-07-29T10:00:00.000000000Z',
                     '2026-07-29T10:00:00.000000000Z'),
                    ('previous-week','Previous week','2026-07-21T10:00:00.000000000Z',
                     '2026-07-21T10:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('current-rollout','current','2026-08-01T10:00:00.000000000Z',
                     '2026-08-01T11:00:00.000000000Z',0),
                    ('prior-rollout','prior','2026-07-31T10:00:00.000000000Z',
                     '2026-07-31T11:00:00.000000000Z',0),
                    ('event-rollout','event-only','2026-07-29T10:00:00.000000000Z',
                     '2026-07-29T10:00:00.000000000Z',0),
                    ('previous-rollout','previous-week','2026-07-21T10:00:00.000000000Z',
                     '2026-07-21T10:00:00.000000000Z',0);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES
                    ('current-usage','current','current-rollout',
                     '2026-08-01T10:00:00.000000000Z',1,'summary-model',100,20,10,3,110,1),
                    ('prior-usage','prior','prior-rollout',
                     '2026-07-31T10:00:00.000000000Z',1,'missing-model',200,40,20,6,220,1),
                    ('previous-usage','previous-week','previous-rollout',
                     '2026-07-21T10:00:00.000000000Z',1,'summary-model',50,10,5,2,55,1);
                 INSERT INTO events(id,thread_id,rollout_id,timestamp,source_line,kind,native)
                 VALUES('event-only-1','event-only','event-rollout',
                        '2026-07-29T10:00:00.000000000Z',1,'state',1);
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES
                    ('current-message','current','current-rollout',
                     '2026-08-01T11:00:00.000000000Z','user','current',2),
                    ('prior-message','prior','prior-rollout',
                     '2026-07-31T11:00:00.000000000Z','user','prior',2);",
            )
            .unwrap();

        let bounds = summary_bounds();
        let actual = read_summary_usage_on(&connection, &bounds).unwrap();
        for (bound, actual) in bounds.iter().zip(&actual) {
            let expected = read_totals_on(
                &connection,
                Some(&bound.start_at),
                Some(&bound.end_at),
                TotalsScope::Global,
            )
            .unwrap();
            assert_eq!(actual.input_tokens, expected.input_tokens);
            assert_eq!(actual.cached_input_tokens, expected.cached_input_tokens);
            assert_eq!(actual.output_tokens, expected.output_tokens);
            assert_eq!(actual.reasoning_tokens, expected.reasoning_tokens);
            assert_eq!(actual.total_tokens, expected.total_tokens);
            assert_eq!(actual.unpriced_tokens, expected.unpriced_tokens);
            assert_eq!(actual.cost_usd, expected.cost_usd);
        }
        assert!(actual[0].pricing_complete);
        assert!(!actual[1].pricing_complete);
        assert!(!actual[2].pricing_complete);
        assert_eq!(
            read_summary_sessions_on(&connection, &bounds).unwrap(),
            [1, 3, 1]
        );
        assert_eq!(
            read_summary_messages_on(&connection, &bounds).unwrap(),
            [1, 2, 1]
        );

        let period_bound = OverviewPeriodBound {
            start: DateTime::parse_from_rfc3339(&bounds[0].start_at)
                .unwrap()
                .with_timezone(&Utc),
            end: DateTime::parse_from_rfc3339(&bounds[0].end_at)
                .unwrap()
                .with_timezone(&Utc),
        };
        let incomplete_delta =
            overview_period_summary("Today", &period_bound, actual[0].clone(), &actual[1], 1, 1);
        assert_eq!(incomplete_delta.delta_cost_usd, None);
        assert_eq!(incomplete_delta.delta_percent, None);
        let priced_delta =
            overview_period_summary("Today", &period_bound, actual[0].clone(), &actual[3], 1, 1);
        assert!(priced_delta.delta_cost_usd.is_some());
        assert!(priced_delta.delta_percent.is_some());
    }

    #[test]
    fn annual_activity_counts_message_and_event_only_days_exactly() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('event-only','Event only','2026-07-15T10:00:00.000000000Z',
                     '2026-07-16T10:00:00.000000000Z'),
                    ('message-only','Message only','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T12:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('event-rollout','event-only','2026-07-15T10:00:00.000000000Z',
                     '2026-07-16T10:00:00.000000000Z',0),
                    ('message-rollout','message-only','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T12:01:00.000000000Z',0);
                 INSERT INTO events(id,thread_id,rollout_id,timestamp,source_line,kind,native)
                 VALUES
                    ('event-1','event-only','event-rollout',
                     '2026-07-15T10:00:00.000000000Z',1,'state',1),
                    ('event-2','event-only','event-rollout',
                     '2026-07-15T11:00:00.000000000Z',2,'state',1),
                    ('event-3','event-only','event-rollout',
                     '2026-07-16T10:00:00.000000000Z',3,'state',1);
                 INSERT INTO messages(id,thread_id,rollout_id,timestamp,role,content,source_line)
                 VALUES
                    ('message-1','message-only','message-rollout',
                     '2026-07-15T12:00:00.000000000Z','user','one',1),
                    ('message-2','message-only','message-rollout',
                     '2026-07-15T12:01:00.000000000Z','assistant','two',2);",
            )
            .unwrap();

        let buckets = overview_year_days(2026).unwrap();
        let mut sessions = vec![HashSet::new(); buckets.len()];
        let messages = read_year_activity_on(&connection, &buckets, &mut sessions).unwrap();
        let july_15 = buckets
            .iter()
            .position(|bucket| bucket.date == "2026-07-15")
            .unwrap();
        let july_16 = buckets
            .iter()
            .position(|bucket| bucket.date == "2026-07-16")
            .unwrap();
        assert_eq!(messages[july_15], 2);
        assert_eq!(messages[july_16], 0);
        assert_eq!(
            sessions[july_15],
            HashSet::from(["event-only".into(), "message-only".into()])
        );
        assert_eq!(sessions[july_16], HashSet::from(["event-only".into()]));
    }

    fn grouped_pricing_fixture() -> (tempfile::TempDir, rusqlite::Connection) {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_prices(
                    model_id,effective_from,effective_to,
                    input_microusd_per_million,cached_input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES
                    ('boundary-model','2026-01-01T00:00:00.000000000Z',NULL,
                     1000000,1000000,1000000,'USD','manual'),
                    ('boundary-model','2026-07-15T12:00:00.000000000Z',
                     '2026-07-15T13:00:00.000000000Z',
                     2000000,2000000,2000000,'USD','manual'),
                    ('gap-model','2026-01-01T00:00:00.000000000Z',
                     '2026-07-15T12:00:00.000000000Z',
                     1000000,1000000,1000000,'USD','manual'),
                    ('gap-model','2026-07-15T13:00:00.000000000Z',NULL,
                     3000000,3000000,3000000,'USD','manual');
                 INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('boundary-thread','Boundary','2026-07-15T11:00:00.000000000Z',
                     '2026-07-15T14:00:00.000000000Z'),
                    ('gap-thread','Gap','2026-07-15T11:30:00.000000000Z',
                     '2026-07-15T13:30:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('boundary-rollout','boundary-thread','2026-07-15T11:00:00.000000000Z',
                     '2026-07-15T14:00:00.000000000Z',0),
                    ('gap-rollout','gap-thread','2026-07-15T11:30:00.000000000Z',
                     '2026-07-15T13:30:00.000000000Z',0);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES
                    ('boundary-1','boundary-thread','boundary-rollout',
                     '2026-07-15T11:00:00.000000000Z',1,'boundary-model',100,0,10,0,110,1),
                    ('boundary-2','boundary-thread','boundary-rollout',
                     '2026-07-15T12:30:00.000000000Z',2,'boundary-model',100,0,10,0,110,1),
                    ('boundary-3','boundary-thread','boundary-rollout',
                     '2026-07-15T14:00:00.000000000Z',3,'boundary-model',100,0,10,0,110,1),
                    ('gap-1','gap-thread','gap-rollout',
                     '2026-07-15T11:30:00.000000000Z',1,'gap-model',100,0,10,0,110,1),
                    ('gap-2','gap-thread','gap-rollout',
                     '2026-07-15T12:30:00.000000000Z',2,'gap-model',100,0,10,0,110,1),
                    ('gap-3','gap-thread','gap-rollout',
                     '2026-07-15T13:30:00.000000000Z',3,'gap-model',100,0,10,0,110,1);",
            )
            .unwrap();
        (temp, connection)
    }

    #[test]
    fn grouped_pricing_matches_priced_usage_across_boundaries_and_gaps() {
        let (_temp, connection) = grouped_pricing_fixture();
        let buckets = overview_year_days(2026).unwrap();
        let (_, sessions, _) = read_year_usage_on(&connection, &buckets).unwrap();
        for thread_id in ["boundary-thread", "gap-thread"] {
            let expected = connection
                .query_row(
                    "SELECT COALESCE(SUM(cost_numerator),0),
                            COALESCE(SUM(CASE WHEN price_known=0
                                              THEN total_tokens ELSE 0 END),0),
                            COALESCE(SUM(total_tokens),0)
                     FROM priced_usage WHERE thread_id=?1",
                    [thread_id],
                    |row| {
                        Ok((
                            i128::from(row.get::<_, i64>(0)?),
                            row.get::<_, i64>(1)?.max(0) as u64,
                            row.get::<_, i64>(2)?.max(0) as u64,
                        ))
                    },
                )
                .unwrap();
            let actual = &sessions[thread_id];
            assert_eq!(actual.known_cost_numerator, expected.0);
            assert_eq!(actual.unpriced_tokens, expected.1);
            assert_eq!(actual.total_tokens, expected.2);
        }
        assert_eq!(sessions["gap-thread"].unpriced_tokens, 110);

        connection
            .execute(
                "INSERT INTO model_prices(
                    model_id,effective_from,effective_to,
                    input_microusd_per_million,cached_input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES(
                    'gap-model','2026-07-15T12:00:00.000000000Z',
                    '2026-07-15T13:00:00.000000000Z',
                    4000000,4000000,4000000,'USD','manual'
                 )",
                [],
            )
            .unwrap();
        let (_, repriced, _) = read_year_usage_on(&connection, &buckets).unwrap();
        assert_eq!(repriced["gap-thread"].unpriced_tokens, 0);
        let expected_cost = connection
            .query_row(
                "SELECT COALESCE(SUM(cost_numerator),0)
                 FROM priced_usage WHERE thread_id='gap-thread'",
                [],
                |row| Ok(i128::from(row.get::<_, i64>(0)?)),
            )
            .unwrap();
        assert_eq!(repriced["gap-thread"].known_cost_numerator, expected_cost);
    }

    #[test]
    fn annual_read_keeps_gapless_empty_days_and_sparse_activity() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let empty = read_year_on(
            &connection,
            2026,
            "2025-12-31T23:00:00.000000000Z",
            "2026-12-31T23:00:00.000000000Z",
        )
        .unwrap();
        assert_eq!(empty.heatmap.len(), 365);
        assert!(empty.heatmap.iter().all(|day| {
            day.cost_usd == Some(crate::costing::UsdAmount::ZERO)
                && day.session_count == 0
                && day.message_count == 0
                && day.total_tokens == 0
        }));

        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('sparse-thread','Sparse','2026-07-15T12:00:00.000000000Z',
                        '2026-07-15T12:02:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('sparse-rollout','sparse-thread','2026-07-15T12:00:00.000000000Z',
                        '2026-07-15T12:02:00.000000000Z',0);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES('sparse-usage','sparse-thread','sparse-rollout',
                          '2026-07-15T12:01:00.000000000Z',1,'gpt-5.5',
                          100,50,10,2,110,1);
                 INSERT INTO messages(id,thread_id,rollout_id,timestamp,role,content,source_line)
                 VALUES('sparse-message','sparse-thread','sparse-rollout',
                        '2026-07-15T12:02:00.000000000Z','user','Sparse',2);",
            )
            .unwrap();
        let sparse = read_year_on(
            &connection,
            2026,
            "2025-12-31T23:00:00.000000000Z",
            "2026-12-31T23:00:00.000000000Z",
        )
        .unwrap();
        let populated = sparse
            .heatmap
            .iter()
            .find(|day| day.date == "2026-07-15")
            .unwrap();
        assert_eq!(populated.session_count, 1);
        assert_eq!(populated.message_count, 1);
        assert_eq!(populated.total_tokens, 110);
        assert_eq!(
            sparse
                .heatmap
                .iter()
                .filter(|day| day.total_tokens > 0)
                .count(),
            1
        );
    }

    #[test]
    fn summary_queries_keep_their_bounded_indexes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let usage_plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {OVERVIEW_SUMMARY_USAGE_SQL}"))
            .unwrap()
            .query_map(
                [
                    "2026-07-01T00:00:00.000000000Z",
                    "2026-08-02T00:00:00.000000000Z",
                ],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(usage_plan.contains("idx_usage_time"), "{usage_plan}");
        assert!(!usage_plan.contains("priced_usage"), "{usage_plan}");

        let bounds = serde_json::to_string(&summary_bounds()).unwrap();
        let session_plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {OVERVIEW_SUMMARY_SESSIONS_SQL}"
            ))
            .unwrap()
            .query_map([bounds.clone()], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        for index in [
            "idx_events_thread_time",
            "idx_usage_thread_time",
            "idx_messages_thread_time",
        ] {
            assert!(
                session_plan.contains(index),
                "missing {index}:\n{session_plan}"
            );
        }
        let message_plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {OVERVIEW_SUMMARY_MESSAGES_SQL}"
            ))
            .unwrap()
            .query_map([bounds], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            message_plan.contains("idx_messages_time_thread"),
            "{message_plan}"
        );
    }

    #[test]
    fn annual_usage_uses_covering_day_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let annual_bounds = serde_json::to_string(&[SqlBucketBounds {
            ordinal: 0,
            start_at: "2025-12-31T23:00:00.000000000Z".into(),
            end_at: "2026-12-31T23:00:00.000000000Z".into(),
        }])
        .unwrap();
        let annual_plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {OVERVIEW_YEAR_USAGE_SQL}"))
            .unwrap()
            .query_map([annual_bounds], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            annual_plan.contains("USING COVERING INDEX idx_usage_overview_year"),
            "{annual_plan}"
        );
    }

    #[test]
    fn event_day_seek_uses_thread_time_index() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let event_plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {OVERVIEW_EVENT_DAY_SEEK_SQL}"))
            .unwrap()
            .query_map(
                params![
                    "thread",
                    "2026-01-01T00:00:00.000000000Z",
                    "2027-01-01T00:00:00.000000000Z"
                ],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            event_plan.contains("idx_events_thread_time"),
            "{event_plan}"
        );
    }

    #[test]
    fn annual_usage_plan_executes_once_per_read() {
        let _guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection.trace(Some(count_overview_usage_query));
        OVERVIEW_USAGE_QUERY_COUNT.store(0, Ordering::SeqCst);
        let response = read_year_on(
            &connection,
            2026,
            "2025-12-31T23:00:00.000000000Z",
            "2026-12-31T23:00:00.000000000Z",
        )
        .unwrap();
        assert_eq!(response.heatmap.len(), 365);
        assert_eq!(OVERVIEW_USAGE_QUERY_COUNT.swap(0, Ordering::SeqCst), 1);
        connection.trace(None);
    }
}
