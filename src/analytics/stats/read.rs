use super::{
    StatsBucket, StatsBucketAggregate, StatsRange, StatsResponse, StatsRow, stats_buckets,
    stats_range_label, stats_totals_from_aggregates,
};
use crate::{
    MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR,
    calendar::{canonical_utc_timestamp, local_midnight},
    costing::PriceBook,
    usage::{UsageTotals, load_price_book_on},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, Local, NaiveDate};
use rusqlite::{Connection, params};
use serde::Serialize;
use std::collections::BTreeSet;

pub(crate) fn read_on(
    connection: &Connection,
    range: StatsRange,
    display_anchor: NaiveDate,
) -> Result<StatsResponse> {
    let buckets = stats_buckets_on(connection, range, display_anchor)?;
    let aggregates = query_stats_bucket_aggregates_on(connection, &buckets)?;
    let totals = stats_totals_from_aggregates(&aggregates);
    let rows = buckets
        .into_iter()
        .zip(aggregates)
        .map(|(bucket, aggregate)| StatsRow {
            period_start: bucket.start.to_rfc3339(),
            period_end: bucket.end.to_rfc3339(),
            label: bucket.label,
            session_count: aggregate.session_count,
            totals: aggregate.totals,
        })
        .collect::<Vec<_>>();
    let label = stats_range_label(range, display_anchor);
    let trend = rows.iter().map(|row| row.totals.cost_usd).collect();
    Ok(StatsResponse {
        range: range.as_str().into(),
        anchor: display_anchor.to_string(),
        label,
        totals,
        rows,
        trend,
    })
}

fn stats_buckets_on(
    connection: &Connection,
    range: StatsRange,
    anchor: NaiveDate,
) -> Result<Vec<StatsBucket>> {
    let years = if range == StatsRange::All {
        occupied_local_years_on(connection)?
    } else {
        BTreeSet::new()
    };
    stats_buckets(range, anchor, years)
}

fn occupied_local_years_on(connection: &Connection) -> Result<BTreeSet<i32>> {
    let mut years = BTreeSet::new();
    let mut next_activity = connection.prepare(
        "SELECT MIN(timestamp) FROM (
            SELECT MIN(timestamp) timestamp FROM events WHERE timestamp>=?1
            UNION ALL SELECT MIN(timestamp) FROM usage_facts WHERE timestamp>=?1
            UNION ALL SELECT MIN(timestamp) FROM messages WHERE timestamp>=?1
         )",
    )?;
    let mut lower_bound = format!("{MIN_PUBLIC_YEAR:04}-01-01T00:00:00.000000000Z");
    loop {
        let timestamp =
            next_activity.query_row([&lower_bound], |row| row.get::<_, Option<String>>(0))?;
        let Some(timestamp) = timestamp else {
            break;
        };
        let year = DateTime::parse_from_rfc3339(&timestamp)
            .with_context(|| format!("invalid stored activity timestamp {timestamp}"))?
            .with_timezone(&Local)
            .year();
        anyhow::ensure!(
            (MIN_PUBLIC_YEAR - 1..=MAX_PUBLIC_YEAR + 1).contains(&year),
            "stored activity local year is outside the supported range"
        );
        years.insert(year.clamp(MIN_PUBLIC_YEAR, MAX_PUBLIC_YEAR));
        if year >= MAX_PUBLIC_YEAR {
            break;
        }
        let next = NaiveDate::from_ymd_opt(year + 1, 1, 1).context("invalid year")?;
        let next_bound = canonical_utc_timestamp(local_midnight(next));
        anyhow::ensure!(
            next_bound > lower_bound,
            "all-time year scan did not advance"
        );
        lower_bound = next_bound;
    }
    Ok(years)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlBucketBounds {
    ordinal: usize,
    start_at: String,
    end_at: String,
}

// Enumerating the small observed-model set lets every bucket/model aggregate
// use idx_usage_model_time directly. This avoids both a whole-range GROUP BY
// spill and priced_usage's correlated price lookup for every individual fact.
const STATS_BUCKET_USAGE_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         ), models AS MATERIALIZED (
            SELECT DISTINCT model FROM usage_facts
         ), bucket_model_values AS MATERIALIZED (
            SELECT b.ordinal,m.model,
                   (SELECT json_array(
                        COALESCE(SUM(u.input_tokens),0),
                        COALESCE(SUM(u.cached_input_tokens),0),
                        COALESCE(SUM(u.output_tokens),0),
                        COALESCE(SUM(u.reasoning_tokens),0),
                        COALESCE(SUM(u.total_tokens),0),
                        COALESCE(SUM(u.input_tokens-
                                     MIN(u.input_tokens,u.cached_input_tokens)),0),
                        COALESCE(SUM(MIN(u.input_tokens,u.cached_input_tokens)),0),
                        MIN(u.timestamp),MAX(u.timestamp)
                    )
                    FROM usage_facts u
                    WHERE u.model=m.model
                      AND u.timestamp>=b.start_at AND u.timestamp<b.end_at) usage
            FROM bounds b CROSS JOIN models m
         )
         SELECT ordinal,model,
                json_extract(usage,'$[0]'),
                json_extract(usage,'$[1]'),
                json_extract(usage,'$[2]'),
                json_extract(usage,'$[3]'),
                json_extract(usage,'$[4]'),
                json_extract(usage,'$[5]'),
                json_extract(usage,'$[6]'),
                json_extract(usage,'$[7]'),
                json_extract(usage,'$[8]')
         FROM bucket_model_values
         WHERE json_extract(usage,'$[7]') IS NOT NULL
         ORDER BY ordinal,model";

// For ordinary Stats ranges, each source index is walked once per disjoint
// bucket and UNION supplies exact session identity across all activity types.
const STATS_BUCKET_SESSIONS_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         )
         SELECT b.ordinal,
                   (SELECT COUNT(*)
                    FROM (
                        SELECT e.thread_id FROM events e INDEXED BY idx_events_time_thread
                        WHERE e.timestamp>=b.start_at AND e.timestamp<b.end_at
                        UNION
                        SELECT u.thread_id FROM usage_facts u INDEXED BY idx_usage_time_thread
                        WHERE u.timestamp>=b.start_at AND u.timestamp<b.end_at
                        UNION
                        SELECT m.thread_id FROM messages m INDEXED BY idx_messages_time_thread
                        WHERE m.timestamp>=b.start_at AND m.timestamp<b.end_at
                    )) session_count
         FROM bounds b
         ORDER BY ordinal";

// Broad Stats buckets (calendar months and years) contain many repeated events
// per session. On that shape, an indexed existence probe per thread avoids
// reading hundreds of thousands of duplicate activity rows merely to recover
// a few thousand session identities. Narrow hourly/daily buckets continue to
// use the bounded range-union query above.
const STATS_FEW_BUCKET_SESSIONS_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         )
         SELECT b.ordinal,
                (SELECT COUNT(*) FROM threads t
                 WHERE EXISTS (
                    SELECT 1 FROM events e INDEXED BY idx_events_thread_time
                    WHERE e.thread_id=t.id
                      AND e.timestamp>=b.start_at AND e.timestamp<b.end_at
                 ) OR EXISTS (
                    SELECT 1 FROM usage_facts u INDEXED BY idx_usage_thread_time
                    WHERE u.thread_id=t.id
                      AND u.timestamp>=b.start_at AND u.timestamp<b.end_at
                 ) OR EXISTS (
                    SELECT 1 FROM messages m INDEXED BY idx_messages_thread_time
                    WHERE m.thread_id=t.id
                      AND m.timestamp>=b.start_at AND m.timestamp<b.end_at
                 )) session_count
         FROM bounds b
         ORDER BY ordinal";

fn stats_buckets_are_broad(buckets: &[StatsBucket]) -> bool {
    buckets.len() <= 2
        || buckets
            .iter()
            .all(|bucket| bucket.end.signed_duration_since(bucket.start) >= Duration::days(20))
}

fn stats_exceptional_group_cost_on(
    connection: &Connection,
    price_book: &PriceBook,
    start: &str,
    end: &str,
    model: &str,
) -> Result<(i128, u64)> {
    let mut statement = connection.prepare(
        "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
         FROM usage_facts
         WHERE model=?1 AND timestamp>=?2 AND timestamp<?3",
    )?;
    let mut rows = statement.query(params![model, start, end])?;
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

fn query_stats_bucket_aggregates_on(
    connection: &Connection,
    buckets: &[StatsBucket],
) -> Result<Vec<StatsBucketAggregate>> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
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
    let mut aggregates = (0..buckets.len())
        .map(|_| StatsBucketAggregate {
            totals: UsageTotals::default(),
            session_count: 0,
            known_cost_numerator: 0,
        })
        .collect::<Vec<_>>();
    let groups = {
        let mut statement = connection.prepare(STATS_BUCKET_USAGE_SQL)?;
        statement
            .query_map([bounds_json.clone()], |row| {
                Ok((
                    row.get::<_, i64>(0)?.max(0) as usize,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?.max(0) as u64,
                    row.get::<_, i64>(3)?.max(0) as u64,
                    row.get::<_, i64>(4)?.max(0) as u64,
                    row.get::<_, i64>(5)?.max(0) as u64,
                    row.get::<_, i64>(6)?.max(0) as u64,
                    row.get::<_, i64>(7)?.max(0),
                    row.get::<_, i64>(8)?.max(0),
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (
        ordinal,
        model,
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
        uncached_billed_tokens,
        cached_billed_tokens,
        first_timestamp,
        last_timestamp,
    ) in groups
    {
        let aggregate = aggregates
            .get_mut(ordinal)
            .context("stats usage query returned an invalid bucket ordinal")?;
        let first_price = price_book.price_at(&model, &first_timestamp);
        let last_price = price_book.price_at(&model, &last_timestamp);
        let has_price_boundary =
            price_book.group_has_price_boundary(&model, &first_timestamp, &last_timestamp);
        let (known_cost_numerator, unpriced_tokens) = match (first_price, last_price) {
            (Some((first_index, price)), Some((last_index, _)))
                if first_index == last_index && !has_price_boundary =>
            {
                (
                    price.cost_numerator(
                        uncached_billed_tokens,
                        cached_billed_tokens,
                        output_tokens.min(i64::MAX as u64) as i64,
                    ),
                    0,
                )
            }
            (None, None)
                if price_book.group_has_no_price(&model, &first_timestamp, &last_timestamp) =>
            {
                (0, total_tokens)
            }
            _ => stats_exceptional_group_cost_on(
                connection,
                &price_book,
                &bounds[ordinal].start_at,
                &bounds[ordinal].end_at,
                &model,
            )?,
        };
        aggregate.totals.input_tokens = aggregate.totals.input_tokens.saturating_add(input_tokens);
        aggregate.totals.cached_input_tokens = aggregate
            .totals
            .cached_input_tokens
            .saturating_add(cached_input_tokens);
        aggregate.totals.output_tokens =
            aggregate.totals.output_tokens.saturating_add(output_tokens);
        aggregate.totals.reasoning_tokens = aggregate
            .totals
            .reasoning_tokens
            .saturating_add(reasoning_tokens);
        aggregate.totals.total_tokens = aggregate.totals.total_tokens.saturating_add(total_tokens);
        aggregate.known_cost_numerator = aggregate
            .known_cost_numerator
            .saturating_add(known_cost_numerator);
        aggregate.totals.unpriced_tokens = aggregate
            .totals
            .unpriced_tokens
            .saturating_add(unpriced_tokens);
    }

    let session_sql = if stats_buckets_are_broad(buckets) {
        STATS_FEW_BUCKET_SESSIONS_SQL
    } else {
        STATS_BUCKET_SESSIONS_SQL
    };
    let mut statement = connection.prepare(session_sql)?;
    let session_rows = statement
        .query_map([bounds_json], |row| {
            Ok((
                row.get::<_, i64>(0)?.max(0) as usize,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if session_rows.len() != buckets.len()
        || session_rows
            .iter()
            .enumerate()
            .any(|(expected, (ordinal, _))| expected != *ordinal)
    {
        anyhow::bail!("stats bucket aggregate query returned incomplete bounds");
    }
    for ((_, session_count), aggregate) in session_rows.into_iter().zip(&mut aggregates) {
        aggregate.session_count = session_count;
        aggregate.totals.known_cost_numerator = aggregate.known_cost_numerator;
        aggregate.totals = std::mem::take(&mut aggregate.totals).finish();
    }
    Ok(aggregates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Db;
    use rusqlite::params;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    static TRACE_LOCK: Mutex<()> = Mutex::new(());
    static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);
    static BROAD_SESSION_QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);
    static ORDINARY_SESSION_QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_stats_query(sql: &str) {
        let sql = sql.trim_start();
        if sql.starts_with("SELECT") || sql.starts_with("WITH") {
            QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if sql.contains("idx_events_thread_time") {
            BROAD_SESSION_QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if sql.contains("idx_events_time_thread") {
            ORDINARY_SESSION_QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn seed_thread_rollout(connection: &Connection, id: &str, timestamp: &str) {
        connection
            .execute(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES(?1,?1,?2,?2)",
                params![id, timestamp],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES(?1,?1,?2,?2,0)",
                params![id, timestamp],
            )
            .unwrap();
    }

    #[test]
    fn sql_bucket_bounds_serialize_with_canonical_camel_case_keys() {
        assert_eq!(
            serde_json::to_value(SqlBucketBounds {
                ordinal: 7,
                start_at: "2026-07-01T00:00:00.000000000Z".into(),
                end_at: "2026-08-01T00:00:00.000000000Z".into(),
            })
            .unwrap(),
            serde_json::json!({
                "ordinal": 7,
                "startAt": "2026-07-01T00:00:00.000000000Z",
                "endAt": "2026-08-01T00:00:00.000000000Z"
            })
        );
    }

    #[test]
    fn stats_bucket_queries_have_constant_statement_budgets() {
        let _trace_guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection.trace(Some(count_stats_query));

        QUERY_COUNT.store(0, Ordering::SeqCst);

        let anchor = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let stats = read_on(&connection, StatsRange::Month, anchor).unwrap();
        assert_eq!(stats.rows.len(), 31);
        assert_eq!(QUERY_COUNT.swap(0, Ordering::SeqCst), 4);

        let stats = read_on(&connection, StatsRange::All, anchor).unwrap();
        assert_eq!(stats.rows.len(), 1);
        assert_eq!(QUERY_COUNT.swap(0, Ordering::SeqCst), 5);
        connection.trace(None);
    }

    #[test]
    fn broad_and_ordinary_ranges_execute_their_owned_session_strategy() {
        let _trace_guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection.trace(Some(count_stats_query));
        let anchor = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();

        BROAD_SESSION_QUERY_COUNT.store(0, Ordering::SeqCst);
        ORDINARY_SESSION_QUERY_COUNT.store(0, Ordering::SeqCst);
        read_on(&connection, StatsRange::Year, anchor).unwrap();
        assert_eq!(BROAD_SESSION_QUERY_COUNT.swap(0, Ordering::SeqCst), 1);
        assert_eq!(ORDINARY_SESSION_QUERY_COUNT.swap(0, Ordering::SeqCst), 0);

        read_on(&connection, StatsRange::Month, anchor).unwrap();
        assert_eq!(BROAD_SESSION_QUERY_COUNT.swap(0, Ordering::SeqCst), 0);
        assert_eq!(ORDINARY_SESSION_QUERY_COUNT.swap(0, Ordering::SeqCst), 1);

        connection.trace(None);
    }

    #[test]
    fn occupied_years_include_each_activity_kind_independently() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();

        seed_thread_rollout(&connection, "event-only", "2024-06-15T12:00:00.000000000Z");
        seed_thread_rollout(&connection, "usage-only", "2025-06-15T12:00:00.000000000Z");
        seed_thread_rollout(
            &connection,
            "message-only",
            "2026-06-15T12:00:00.000000000Z",
        );
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,native
                 ) VALUES('event','event-only','event-only',?1,1,'state',1)",
                ["2024-06-15T12:00:00.000000000Z"],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES('usage','usage-only','usage-only',?1,1,'unpriced',
                          1,0,0,0,1,1)",
                ["2025-06-15T12:00:00.000000000Z"],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('message','message-only','message-only',?1,'user','hello',1)",
                ["2026-06-15T12:00:00.000000000Z"],
            )
            .unwrap();

        assert_eq!(
            occupied_local_years_on(&connection).unwrap(),
            BTreeSet::from([2024, 2025, 2026])
        );
    }

    #[test]
    fn ordinary_session_rows_deduplicate_activity_kinds_and_repeated_events() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let timestamp = "2026-07-15T12:15:00.000000000Z";
        seed_thread_rollout(&connection, "one-thread", timestamp);
        connection
            .execute_batch(
                "INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,native
                 ) VALUES
                    ('event-1','one-thread','one-thread',
                     '2026-07-15T12:15:00.000000000Z',1,'state',1),
                    ('event-2','one-thread','one-thread',
                     '2026-07-15T12:16:00.000000000Z',2,'state',1);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES(
                    'usage','one-thread','one-thread',
                    '2026-07-15T12:15:00.000000000Z',3,'unpriced',
                    1,0,0,0,1,1
                 );
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES(
                    'message','one-thread','one-thread',
                    '2026-07-15T12:15:00.000000000Z','user','hello',4
                 );",
            )
            .unwrap();

        let stats = read_on(
            &connection,
            StatsRange::Day,
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap(),
        )
        .unwrap();
        assert_eq!(
            stats.rows.iter().map(|row| row.session_count).sum::<u64>(),
            1
        );
        assert_eq!(
            stats
                .rows
                .iter()
                .filter(|row| row.session_count == 1)
                .count(),
            1
        );
    }

    #[test]
    fn persistence_assembly_preserves_cost_numerator_above_i64_and_js_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let timestamp = "2026-07-15T12:15:00.000000000Z";
        seed_thread_rollout(&connection, "exact-thread", timestamp);
        connection
            .execute_batch(
                "INSERT INTO model_prices(
                    model_id,effective_from,effective_to,
                    input_microusd_per_million,cached_input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES(
                    'exact-model','1970-01-01T00:00:00.000000000Z',NULL,
                    1000000000,1000000000,1000000000,'USD','manual'
                 );
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES
                    ('exact-usage-1','exact-thread','exact-thread',
                    '2026-07-15T12:15:00.000000000Z',1,'exact-model',
                    4000000000,0,0,0,4000000000,1),
                    ('exact-usage-2','exact-thread','exact-thread',
                    '2026-07-15T12:16:00.000000000Z',2,'exact-model',
                    4000000000,0,0,0,4000000000,1),
                    ('exact-usage-3','exact-thread','exact-thread',
                    '2026-07-15T12:17:00.000000000Z',3,'exact-model',
                    4000000000,0,0,0,4000000000,1);",
            )
            .unwrap();

        let stats = read_on(
            &connection,
            StatsRange::Day,
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap(),
        )
        .unwrap();
        let expected = 12_000_000_000_000_000_000i128;
        assert!(expected > i128::from(i64::MAX));
        assert!(expected > 9_007_199_254_740_991);
        assert_eq!(stats.totals.known_cost_numerator, expected);
        assert_eq!(stats.totals.cost_usd.unwrap().cost_numerator(), expected);
        let active = stats
            .rows
            .iter()
            .position(|row| row.totals.total_tokens > 0)
            .unwrap();
        assert_eq!(stats.rows[active].totals.known_cost_numerator, expected);
        assert_eq!(stats.trend[active].unwrap().cost_numerator(), expected);
    }

    #[test]
    fn broad_stats_buckets_use_thread_index_probes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let anchor = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();

        let year = super::stats_buckets_on(&connection, StatsRange::Year, anchor).unwrap();
        assert!(super::stats_buckets_are_broad(&year));

        let month = super::stats_buckets_on(&connection, StatsRange::Month, anchor).unwrap();
        assert!(!super::stats_buckets_are_broad(&month));

        let week = super::stats_buckets_on(&connection, StatsRange::Week, anchor).unwrap();
        assert!(!super::stats_buckets_are_broad(&week));
    }

    #[test]
    fn all_time_stats_use_sparse_occupied_years_for_far_future_data() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('past-thread','Past','2025-01-01T00:00:00.000000000Z',
                     '2025-01-01T00:00:00.000000000Z'),
                    ('future-thread','Future','2500-01-01T00:00:00.000000000Z',
                     '2500-01-01T00:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('past-rollout','past-thread','2025-01-01T00:00:00.000000000Z',
                     '2025-01-01T00:00:00.000000000Z',0),
                    ('future-rollout','future-thread','2500-01-01T00:00:00.000000000Z',
                     '2500-01-01T00:00:00.000000000Z',0);
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,native
                 ) VALUES
                    ('past-event','past-thread','past-rollout',
                     '2025-01-01T00:00:00.000000000Z',1,'state',1),
                    ('future-event','future-thread','future-rollout',
                     '2500-01-01T00:00:00.000000000Z',1,'state',1);",
            )
            .unwrap();

        let buckets = super::stats_buckets_on(
            &connection,
            StatsRange::All,
            NaiveDate::from_ymd_opt(2026, 7, 19).unwrap(),
        )
        .unwrap();
        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.label.as_str())
                .collect::<Vec<_>>(),
            ["2025", "2026", "2500"]
        );
    }

    #[test]
    fn all_time_stats_include_future_only_and_mixed_future_data() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('future-thread','Future','2027-01-01T00:00:00Z','2027-01-01T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('future-rollout','future-thread','2027-01-01T00:00:00Z','2027-01-01T00:00:00Z',0);
                 INSERT INTO events(id,thread_id,rollout_id,timestamp,source_line,kind,native) VALUES
                    ('future-event','future-thread','future-rollout','2027-01-01T00:00:00Z',1,'state',1);",
            )
            .unwrap();
        let anchor = NaiveDate::from_ymd_opt(2026, 7, 19).unwrap();

        let future_only = super::stats_buckets_on(&connection, StatsRange::All, anchor).unwrap();
        assert_eq!(
            future_only
                .iter()
                .map(|bucket| bucket.label.as_str())
                .collect::<Vec<_>>(),
            ["2027"]
        );

        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('past-thread','Past','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('past-rollout','past-thread','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z',0);
                 INSERT INTO events(id,thread_id,rollout_id,timestamp,source_line,kind,native) VALUES
                    ('past-event','past-thread','past-rollout','2025-01-01T00:00:00Z',1,'state',1);",
            )
            .unwrap();
        let mixed = super::stats_buckets_on(&connection, StatsRange::All, anchor).unwrap();
        assert_eq!(
            mixed
                .iter()
                .map(|bucket| bucket.label.as_str())
                .collect::<Vec<_>>(),
            ["2025", "2026", "2027"]
        );
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
                    ('boundary-rollout','boundary-thread',
                     '2026-07-15T11:00:00.000000000Z',
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
    fn stats_grouped_pricing_matches_priced_usage_across_boundaries_and_gaps() {
        let (_temp, connection) = grouped_pricing_fixture();

        let expected = connection
            .query_row(
                "SELECT COALESCE(SUM(cost_numerator),0),
                        COALESCE(SUM(CASE WHEN price_known=0
                                          THEN total_tokens ELSE 0 END),0),
                        COALESCE(SUM(input_tokens),0),
                        COALESCE(SUM(cached_input_tokens),0),
                        COALESCE(SUM(output_tokens),0),
                        COALESCE(SUM(reasoning_tokens),0),
                        COALESCE(SUM(total_tokens),0)
                 FROM priced_usage",
                [],
                |row| {
                    Ok((
                        i128::from(row.get::<_, i64>(0)?),
                        row.get::<_, i64>(1)?.max(0) as u64,
                        row.get::<_, i64>(2)?.max(0) as u64,
                        row.get::<_, i64>(3)?.max(0) as u64,
                        row.get::<_, i64>(4)?.max(0) as u64,
                        row.get::<_, i64>(5)?.max(0) as u64,
                        row.get::<_, i64>(6)?.max(0) as u64,
                    ))
                },
            )
            .unwrap();
        let stats = read_on(
            &connection,
            StatsRange::Year,
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap(),
        )
        .unwrap();
        assert_eq!(stats.totals.known_cost_numerator, expected.0);
        assert_eq!(stats.totals.unpriced_tokens, expected.1);
        assert_eq!(stats.totals.input_tokens, expected.2);
        assert_eq!(stats.totals.cached_input_tokens, expected.3);
        assert_eq!(stats.totals.output_tokens, expected.4);
        assert_eq!(stats.totals.reasoning_tokens, expected.5);
        assert_eq!(stats.totals.total_tokens, expected.6);
        assert_eq!(stats.totals.cost_usd, None);
        assert!(!stats.totals.pricing_complete);

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
        let repriced_expected = connection
            .query_row(
                "SELECT COALESCE(SUM(cost_numerator),0),
                        COALESCE(SUM(CASE WHEN price_known=0
                                          THEN total_tokens ELSE 0 END),0),
                        COALESCE(SUM(input_tokens),0),
                        COALESCE(SUM(cached_input_tokens),0),
                        COALESCE(SUM(output_tokens),0),
                        COALESCE(SUM(reasoning_tokens),0),
                        COALESCE(SUM(total_tokens),0)
                 FROM priced_usage",
                [],
                |row| {
                    Ok((
                        i128::from(row.get::<_, i64>(0)?),
                        row.get::<_, i64>(1)?.max(0) as u64,
                        row.get::<_, i64>(2)?.max(0) as u64,
                        row.get::<_, i64>(3)?.max(0) as u64,
                        row.get::<_, i64>(4)?.max(0) as u64,
                        row.get::<_, i64>(5)?.max(0) as u64,
                        row.get::<_, i64>(6)?.max(0) as u64,
                    ))
                },
            )
            .unwrap();
        let repriced = read_on(
            &connection,
            StatsRange::Year,
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap(),
        )
        .unwrap();
        assert_eq!(repriced.totals.known_cost_numerator, repriced_expected.0);
        assert_eq!(repriced.totals.unpriced_tokens, repriced_expected.1);
        assert_eq!(repriced.totals.input_tokens, repriced_expected.2);
        assert_eq!(repriced.totals.cached_input_tokens, repriced_expected.3);
        assert_eq!(repriced.totals.output_tokens, repriced_expected.4);
        assert_eq!(repriced.totals.reasoning_tokens, repriced_expected.5);
        assert_eq!(repriced.totals.total_tokens, repriced_expected.6);
        assert_eq!(repriced.totals.unpriced_tokens, 0);
        assert_eq!(
            repriced.totals.cost_usd.unwrap().cost_numerator(),
            repriced_expected.0
        );
        assert!(repriced.totals.pricing_complete);
    }

    #[test]
    fn stats_bucket_query_uses_indexed_ranges_without_grouping_spills() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let bounds_json = serde_json::to_string(&[SqlBucketBounds {
            ordinal: 0,
            start_at: "2026-07-01T00:00:00.000000000Z".into(),
            end_at: "2026-08-01T00:00:00.000000000Z".into(),
        }])
        .unwrap();
        let explain = format!("EXPLAIN QUERY PLAN {STATS_BUCKET_USAGE_SQL}");
        let mut statement = connection.prepare(&explain).unwrap();
        let usage_plan = statement
            .query_map([bounds_json.clone()], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");

        assert!(
            usage_plan.contains("CORRELATED SCALAR SUBQUERY"),
            "stats buckets are not evaluated as bounded indexed ranges:\n{usage_plan}"
        );
        assert!(
            usage_plan.contains("idx_usage_model_time"),
            "stats usage facts are not model/time-indexed:\n{usage_plan}"
        );
        assert!(
            !usage_plan.contains("USE TEMP B-TREE FOR GROUP BY"),
            "stats usage groups spill to a temp table:\n{usage_plan}"
        );

        let explain = format!("EXPLAIN QUERY PLAN {STATS_BUCKET_SESSIONS_SQL}");
        let mut statement = connection.prepare(&explain).unwrap();
        let session_plan = statement
            .query_map([bounds_json.clone()], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            session_plan.contains("idx_events_time_thread"),
            "stats event sessions are not read from a bounded covering index:\n{session_plan}"
        );
        assert!(
            session_plan.contains("idx_usage_time_thread"),
            "stats usage sessions are not read from a bounded covering index:\n{session_plan}"
        );
        assert!(
            session_plan.contains("idx_messages_time_thread"),
            "stats message sessions are not read from a bounded covering index:\n{session_plan}"
        );
        assert!(
            !session_plan.contains("SCAN t"),
            "ordinary stats ranges probe the entire thread table per bucket:\n{session_plan}"
        );

        let explain = format!("EXPLAIN QUERY PLAN {STATS_FEW_BUCKET_SESSIONS_SQL}");
        let mut statement = connection.prepare(&explain).unwrap();
        let broad_session_plan = statement
            .query_map([bounds_json], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            broad_session_plan.contains("idx_events_thread_time"),
            "broad stats event sessions are not probed by thread/time:\n{broad_session_plan}"
        );
        assert!(
            broad_session_plan.contains("idx_usage_thread_time"),
            "broad stats usage sessions are not probed by thread/time:\n{broad_session_plan}"
        );
        assert!(
            broad_session_plan.contains("idx_messages_thread_time"),
            "broad stats message sessions are not probed by thread/time:\n{broad_session_plan}"
        );
    }
}
