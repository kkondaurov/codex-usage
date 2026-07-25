use super::selection::PreparedSelection;
use crate::{
    calendar::canonical_utc_timestamp,
    costing::PriceBook,
    usage::{
        RollupScope, UsageAccumulator, UsageTotals, load_price_book_on, price_hourly_rollup_on,
    },
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Local, NaiveDate, TimeZone, Utc};
use rusqlite::{Connection, params};
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub(crate) struct ActivitySelection<'a> {
    root_turn_id: &'a str,
    usage_kind: i64,
}

impl<'a> ActivitySelection<'a> {
    pub(crate) fn new(root_turn_id: &'a str, usage_kind: i64) -> Self {
        Self {
            root_turn_id,
            usage_kind,
        }
    }
}

pub(crate) enum LocalDayRollup {
    Single(NaiveDate),
    Split(HashMap<NaiveDate, UsageAccumulator>),
}

pub(crate) struct SelectedActivityUsage {
    roots: HashMap<String, UsageAccumulator>,
    groups: HashMap<(String, bool), UsageAccumulator>,
}

pub(crate) struct EventUsageOwner {
    pub(crate) ordinal: usize,
    pub(crate) rollout_id: String,
    pub(crate) turn_id: Option<String>,
    pub(crate) source_line: i64,
}

pub(crate) struct EventUsageKey<'a> {
    pub(crate) id: &'a str,
    pub(crate) rollout_id: &'a str,
    pub(crate) turn_id: Option<&'a str>,
    pub(crate) kind: &'a str,
    pub(crate) stored_kind: &'a str,
    pub(crate) source_line: i64,
    pub(crate) call_id: Option<&'a str>,
}

pub(crate) fn event_total_on(
    connection: &Connection,
    thread_id: &str,
    owner: EventUsageKey<'_>,
) -> Result<Option<UsageTotals>> {
    if !matches!(
        owner.kind,
        "assistant" | "update" | "final" | "reasoning" | "tool" | "subagent"
    ) {
        return Ok(None);
    }
    let visible = if owner.stored_kind == "tool_call" && owner.call_id.is_some() {
        connection.query_row(
            "SELECT NOT EXISTS(
                 SELECT 1 FROM events earlier
                 WHERE earlier.thread_id=?1 AND earlier.rollout_id=?2
                   AND earlier.turn_id IS ?3 AND earlier.kind='tool_call'
                   AND earlier.call_id=?4
                   AND (earlier.source_line<?5
                        OR (earlier.source_line=?5 AND earlier.id<?6))
             )",
            params![
                thread_id,
                owner.rollout_id,
                owner.turn_id,
                owner.call_id,
                owner.source_line,
                owner.id
            ],
            |row| row.get::<_, i64>(0),
        )? != 0
    } else if owner.stored_kind == "turn_completed" {
        connection.query_row(
            "SELECT NOT EXISTS(
                 SELECT 1
                 FROM events final_event
                 LEFT JOIN messages final_message
                   ON final_message.id=COALESCE(final_event.call_id,final_event.id)
                  AND final_message.thread_id=final_event.thread_id
                 WHERE final_event.thread_id=?1 AND final_event.turn_id IS ?2
                   AND final_event.kind='final'
                   AND trim(COALESCE(
                       final_event.body,final_message.content,''
                   ))<>''
             )",
            params![thread_id, owner.turn_id],
            |row| row.get::<_, i64>(0),
        )? != 0
    } else {
        true
    };
    if !visible {
        return Ok(None);
    }

    let next_source_line = owner.source_line.saturating_add(1);
    let following_owner = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM events e
             WHERE e.thread_id=?1 AND e.rollout_id=?2 AND e.turn_id IS ?3
               AND e.source_line=?4
               AND (
                    e.kind IN (
                        'assistant','update','final','reasoning','tool_call','subagent',
                        'turn_completed'
                    )
                    OR (e.kind='message' AND COALESCE(e.role,'')<>'user')
               )
               AND (e.kind<>'turn_completed' OR NOT EXISTS(
                    SELECT 1
                    FROM events final_event
                    LEFT JOIN messages final_message
                      ON final_message.id=COALESCE(final_event.call_id,final_event.id)
                     AND final_message.thread_id=final_event.thread_id
                    WHERE final_event.thread_id=e.thread_id
                      AND final_event.turn_id=e.turn_id
                      AND final_event.kind='final'
                      AND trim(COALESCE(
                          final_event.body,final_message.content,''
                      ))<>''
               ))
               AND (e.kind<>'tool_call' OR e.call_id IS NULL OR NOT EXISTS(
                    SELECT 1 FROM events earlier
                    WHERE earlier.thread_id=e.thread_id
                      AND earlier.rollout_id=e.rollout_id
                      AND earlier.turn_id IS e.turn_id
                      AND earlier.kind='tool_call' AND earlier.call_id=e.call_id
                      AND (earlier.source_line<e.source_line
                           OR (earlier.source_line=e.source_line AND earlier.id<e.id))
               ))
             LIMIT 1
         )",
        params![thread_id, owner.rollout_id, owner.turn_id, next_source_line],
        |row| row.get::<_, i64>(0),
    )? != 0;
    let second_source_line = owner.source_line.saturating_add(2);
    let price_book = load_price_book_on(connection)?;
    let mut statement = connection.prepare(
        "SELECT timestamp,model,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens
         FROM usage_facts
         WHERE thread_id=?1 AND rollout_id=?2 AND turn_id IS ?3
           AND (source_line=?4 OR (source_line=?5 AND ?6=0))
         ORDER BY source_line,id",
    )?;
    let mut rows = statement.query(params![
        thread_id,
        owner.rollout_id,
        owner.turn_id,
        next_source_line,
        second_source_line,
        i64::from(following_owner)
    ])?;
    let mut totals = UsageAccumulator::default();
    let mut usage_rows = 0u64;
    while let Some(row) = rows.next()? {
        let timestamp = row.get::<_, String>(0)?;
        let model = row.get::<_, String>(1)?;
        totals.add_fact(
            &price_book,
            &timestamp,
            &model,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        );
        usage_rows += 1;
    }
    Ok((usage_rows > 0).then(|| totals.finish()))
}

pub(crate) fn turn_totals_on(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
) -> Result<UsageTotals> {
    let price_book = load_price_book_on(connection)?;
    let groups = {
        let mut statement = connection.prepare(
            "SELECT activity_hour,model,
                    COALESCE(SUM(input_tokens),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(output_tokens),0),
                    COALESCE(SUM(reasoning_tokens),0),
                    COALESCE(SUM(total_tokens),0)
             FROM usage_activity_rollups INDEXED BY idx_usage_activity_rollups_turn
             WHERE thread_id=?1 AND turn_key=?2
             GROUP BY activity_hour,model",
        )?;
        statement
            .query_map(params![thread_id, turn_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?.max(0),
                    row.get::<_, i64>(3)?.max(0),
                    row.get::<_, i64>(4)?.max(0),
                    row.get::<_, i64>(5)?.max(0),
                    row.get::<_, i64>(6)?.max(0) as u64,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut totals = UsageAccumulator::default();
    for (
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
            RollupScope::Turn { thread_id, turn_id },
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

pub(crate) fn event_totals_on(
    connection: &Connection,
    thread_id: &str,
    owners: &[EventUsageOwner],
) -> Result<HashMap<usize, UsageTotals>> {
    if owners.is_empty() {
        return Ok(HashMap::new());
    }
    let requested = serde_json::to_string(
        &owners
            .iter()
            .map(|owner| {
                serde_json::json!({
                    "ordinal": owner.ordinal,
                    "rolloutId": owner.rollout_id,
                    "turnId": owner.turn_id,
                    "sourceLine": owner.source_line,
                })
            })
            .collect::<Vec<_>>(),
    )?;
    let mut statement = connection.prepare(
        "WITH requested AS MATERIALIZED (
             SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                    json_extract(value,'$.rolloutId') rollout_id,
                    json_extract(value,'$.turnId') turn_id,
                    CAST(json_extract(value,'$.sourceLine') AS INTEGER) source_line
             FROM json_each(?1)
         )
         SELECT requested.ordinal,p.timestamp,p.model,p.input_tokens,
                p.cached_input_tokens,p.output_tokens,p.reasoning_tokens,p.total_tokens
         FROM requested
         JOIN usage_facts p
           ON p.thread_id=?2 AND p.rollout_id=requested.rollout_id
          AND p.turn_id IS requested.turn_id
          AND (
               p.source_line=requested.source_line+1
               OR (
                    p.source_line=requested.source_line+2
                    AND NOT EXISTS(
                        SELECT 1 FROM events e
                        WHERE e.thread_id=?2
                          AND e.rollout_id=requested.rollout_id
                          AND e.turn_id IS requested.turn_id
                          AND e.source_line=requested.source_line+1
                          AND (
                               e.kind IN (
                                   'assistant','update','final','reasoning','tool_call',
                                   'subagent','turn_completed'
                               )
                               OR (e.kind='message' AND COALESCE(e.role,'')<>'user')
                          )
                          AND (e.kind<>'turn_completed' OR NOT EXISTS(
                               SELECT 1
                               FROM events final_event
                               LEFT JOIN messages final_message
                                 ON final_message.id=COALESCE(final_event.call_id,final_event.id)
                                AND final_message.thread_id=final_event.thread_id
                               WHERE final_event.thread_id=e.thread_id
                                 AND final_event.turn_id=e.turn_id
                                 AND final_event.kind='final'
                                 AND trim(COALESCE(
                                     final_event.body,final_message.content,''
                                 ))<>''
                          ))
                          AND (e.kind<>'tool_call' OR e.call_id IS NULL OR NOT EXISTS(
                               SELECT 1 FROM events earlier
                               WHERE earlier.thread_id=e.thread_id
                                 AND earlier.rollout_id=e.rollout_id
                                 AND earlier.turn_id IS e.turn_id
                                 AND earlier.kind='tool_call'
                                 AND earlier.call_id=e.call_id
                                 AND (earlier.source_line<e.source_line
                                      OR (earlier.source_line=e.source_line
                                          AND earlier.id<e.id))
                          ))
                        LIMIT 1
                    )
               )
          )
         ORDER BY requested.ordinal,p.source_line,p.id",
    )?;
    let rows = statement.query_map(params![requested, thread_id], |row| {
        Ok((
            row.get::<_, i64>(0)?.max(0) as usize,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    let price_book = load_price_book_on(connection)?;
    let mut totals_by_ordinal = HashMap::<usize, UsageAccumulator>::new();
    for row in rows {
        let (
            ordinal,
            timestamp,
            model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        ) = row?;
        totals_by_ordinal.entry(ordinal).or_default().add_fact(
            &price_book,
            &timestamp,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        );
    }
    Ok(totals_by_ordinal
        .into_iter()
        .map(|(ordinal, totals)| (ordinal, totals.finish()))
        .collect())
}

impl SelectedActivityUsage {
    pub(crate) fn load(selection: &PreparedSelection<'_>) -> Result<Self> {
        selection.with_connection(|connection| {
            let thread_id = selection.thread_id();
            let price_book = load_price_book_on(connection)?;
            let mut attribution = Self {
                roots: HashMap::new(),
                groups: HashMap::new(),
            };
            let mut statement = connection.prepare(
                "SELECT selected.root_turn_id,selected.usage_kind,
                        r.activity_hour,r.model,
                        COALESCE(SUM(r.input_tokens),0),
                        COALESCE(SUM(r.cached_input_tokens),0),
                        COALESCE(SUM(r.output_tokens),0),
                        COALESCE(SUM(r.reasoning_tokens),0),
                        COALESCE(SUM(r.total_tokens),0)
                 FROM selected_activity_turns selected
                 JOIN usage_activity_rollups r
                   ON r.thread_id=?1 AND r.turn_key=selected.turn_id
                 GROUP BY selected.root_turn_id,selected.usage_kind,
                          r.activity_hour,r.model",
            )?;
            let mut rows = statement.query([thread_id])?;
            while let Some(row) = rows.next()? {
                let root_turn_id = row.get::<_, String>(0)?;
                let usage_kind = row.get::<_, i64>(1)?;
                let activity_hour = row.get::<_, String>(2)?;
                let model = row.get::<_, String>(3)?;
                let input_tokens = row.get::<_, i64>(4)?.max(0);
                let cached_input_tokens = row.get::<_, i64>(5)?.max(0);
                let output_tokens = row.get::<_, i64>(6)?.max(0);
                let reasoning_tokens = row.get::<_, i64>(7)?.max(0);
                let total_tokens = row.get::<_, i64>(8)?.max(0) as u64;
                let (known_cost_numerator, unpriced_tokens) = selected_rollup_cost_on(
                    connection,
                    &price_book,
                    ActivitySelection::new(&root_turn_id, usage_kind),
                    thread_id,
                    &activity_hour,
                    &model,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    total_tokens,
                )?;
                attribution
                    .roots
                    .entry(root_turn_id.clone())
                    .or_default()
                    .add_group(
                        input_tokens as u64,
                        cached_input_tokens as u64,
                        output_tokens as u64,
                        reasoning_tokens as u64,
                        total_tokens,
                        known_cost_numerator,
                        unpriced_tokens,
                    );
                if usage_kind != 0 {
                    attribution
                        .groups
                        .entry((root_turn_id, usage_kind == 2))
                        .or_default()
                        .add_group(
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

            // NULL-turn usage has no stable relational owner. Attribute only
            // the sparse interval slice selected for each root exchange.
            let mut statement = connection.prepare(
                "SELECT selected.turn_id,u.timestamp,u.model,u.input_tokens,
                        u.cached_input_tokens,u.output_tokens,u.reasoning_tokens,u.total_tokens
                 FROM selected_activity_roots selected
                 JOIN usage_facts u INDEXED BY idx_usage_turn_model_time
                   ON u.thread_id=?1 AND u.turn_id IS NULL
                  AND (selected.open_left=1 OR u.timestamp>=selected.started_at)
                  AND (selected.next_started_at IS NULL
                       OR u.timestamp<selected.next_started_at)",
            )?;
            for row in statement.query_map([thread_id], |row| {
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
            })? {
                let (
                    turn_id,
                    timestamp,
                    model,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    reasoning_tokens,
                    total_tokens,
                ) = row?;
                let cached_input_tokens = input_tokens.min(cached_input_tokens);
                let (known_cost_numerator, unpriced_tokens) = price_book
                    .price_at(&model, &timestamp)
                    .map_or((0, total_tokens), |(_, price)| {
                        (
                            price.cost_numerator(
                                input_tokens - cached_input_tokens,
                                cached_input_tokens,
                                output_tokens,
                            ),
                            0,
                        )
                    });
                attribution.roots.entry(turn_id).or_default().add_group(
                    input_tokens as u64,
                    cached_input_tokens as u64,
                    output_tokens as u64,
                    reasoning_tokens as u64,
                    total_tokens,
                    known_cost_numerator,
                    unpriced_tokens,
                );
            }
            Ok(attribution)
        })
    }

    pub(crate) fn root_totals(&self, root_turn_id: &str) -> UsageTotals {
        self.roots
            .get(root_turn_id)
            .cloned()
            .unwrap_or_default()
            .finish()
    }

    pub(crate) fn group_totals(&self, root_turn_id: &str, reviews: bool) -> UsageTotals {
        self.groups
            .get(&(root_turn_id.to_owned(), reviews))
            .cloned()
            .unwrap_or_default()
            .finish()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn selected_rollup_cost_on(
    connection: &Connection,
    price_book: &PriceBook,
    selection: ActivitySelection<'_>,
    thread_id: &str,
    activity_hour: &str,
    model: &str,
    input_tokens: i64,
    cached_input_tokens: i64,
    output_tokens: i64,
    total_tokens: u64,
) -> Result<(i128, u64)> {
    let (start, end) = rollup_hour_window(activity_hour)?;
    let first_timestamp = canonical_utc_timestamp(start);
    let last_timestamp = canonical_utc_timestamp(
        end.checked_sub_signed(Duration::nanoseconds(1))
            .unwrap_or(start),
    );
    let cached_input_tokens = input_tokens.min(cached_input_tokens.max(0));
    let uncached_input_tokens = input_tokens.saturating_sub(cached_input_tokens);
    let first_price = price_book.price_at(model, &first_timestamp);
    let last_price = price_book.price_at(model, &last_timestamp);
    let has_price_boundary =
        price_book.group_has_price_boundary(model, &first_timestamp, &last_timestamp);
    match (first_price, last_price) {
        (Some((first_index, price)), Some((last_index, _)))
            if first_index == last_index && !has_price_boundary =>
        {
            Ok((
                price.cost_numerator(uncached_input_tokens, cached_input_tokens, output_tokens),
                0,
            ))
        }
        (None, None) if price_book.group_has_no_price(model, &first_timestamp, &last_timestamp) => {
            Ok((0, total_tokens))
        }
        _ => selected_exceptional_cost_on(
            connection,
            price_book,
            selection,
            thread_id,
            model,
            &first_timestamp,
            &canonical_utc_timestamp(end),
        ),
    }
}

fn selected_exceptional_cost_on(
    connection: &Connection,
    price_book: &PriceBook,
    selection: ActivitySelection<'_>,
    thread_id: &str,
    model: &str,
    start_at: &str,
    end_at: &str,
) -> Result<(i128, u64)> {
    let mut statement = connection.prepare(
        "SELECT u.timestamp,u.input_tokens,u.cached_input_tokens,
                u.output_tokens,u.total_tokens
         FROM usage_facts u INDEXED BY idx_usage_thread_model_time
         JOIN selected_activity_turns selected ON selected.turn_id=u.turn_id
         WHERE u.thread_id=?1 AND u.model=?2
           AND u.timestamp>=?3 AND u.timestamp<?4
           AND selected.root_turn_id=?5 AND selected.usage_kind=?6",
    )?;
    let mut rows = statement.query(params![
        thread_id,
        model,
        start_at,
        end_at,
        selection.root_turn_id,
        selection.usage_kind
    ])?;
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

pub(crate) fn thread_rollup_local_days_on(
    connection: &Connection,
    price_book: &PriceBook,
    thread_id: &str,
    activity_hour: &str,
    model: &str,
) -> Result<LocalDayRollup> {
    let (hour_start, hour_end) = rollup_hour_window(activity_hour)?;
    let (start_date, end_date) = rollup_bucket_dates(hour_start, hour_end, &Local);
    if start_date == end_date {
        return Ok(LocalDayRollup::Single(start_date));
    }

    // Sub-hour UTC offsets can place local midnight inside a UTC-hour bucket.
    // Only that boundary bucket falls back to raw indexed rows; every full
    // bucket remains compact and time-zone independent.
    let start_at = canonical_utc_timestamp(hour_start);
    let end_at = canonical_utc_timestamp(hour_end);
    let mut statement = connection.prepare(
        "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens
         FROM usage_facts INDEXED BY idx_usage_thread_model_time
         WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4",
    )?;
    let mut rows = statement.query(params![thread_id, model, start_at, end_at])?;
    let mut totals = HashMap::<NaiveDate, UsageAccumulator>::new();
    while let Some(row) = rows.next()? {
        let timestamp = row.get_ref(0)?.as_str()?;
        let parsed = DateTime::parse_from_rfc3339(timestamp)
            .with_context(|| format!("invalid usage timestamp {timestamp}"))?;
        let date = parsed.with_timezone(&Local).date_naive();
        let input_tokens = row.get::<_, i64>(1)?.max(0);
        let cached_input_tokens = input_tokens.min(row.get::<_, i64>(2)?.max(0));
        let output_tokens = row.get::<_, i64>(3)?.max(0);
        let reasoning_tokens = row.get::<_, i64>(4)?.max(0);
        let total_tokens = row.get::<_, i64>(5)?.max(0) as u64;
        let (known_cost_numerator, unpriced_tokens) =
            price_book
                .price_at(model, timestamp)
                .map_or((0, total_tokens), |(_, price)| {
                    (
                        price.cost_numerator(
                            input_tokens - cached_input_tokens,
                            cached_input_tokens,
                            output_tokens,
                        ),
                        0,
                    )
                });
        totals.entry(date).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    Ok(LocalDayRollup::Split(totals))
}

fn rollup_hour_window(activity_hour: &str) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let start = DateTime::parse_from_rfc3339(activity_hour)
        .with_context(|| format!("invalid UTC usage rollup hour {activity_hour}"))?
        .with_timezone(&Utc);
    let end = start
        .checked_add_signed(Duration::hours(1))
        .context("usage rollup hour has no successor")?;
    Ok((start, end))
}

fn rollup_bucket_dates<Tz: TimeZone>(
    hour_start: DateTime<Utc>,
    hour_end: DateTime<Utc>,
    timezone: &Tz,
) -> (NaiveDate, NaiveDate) {
    let occupied_end = hour_end
        .checked_sub_signed(Duration::nanoseconds(1))
        .unwrap_or(hour_start);
    (
        hour_start.with_timezone(timezone).date_naive(),
        occupied_end.with_timezone(timezone).date_naive(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costing::PriceInterval;
    use chrono::FixedOffset;

    const START: &str = "2026-07-01T18:00:00.000000000Z";

    #[test]
    fn utc_rollup_hour_detects_midnight_in_fractional_offset_zones() {
        let nepal = FixedOffset::east_opt(5 * 3600 + 45 * 60).unwrap();
        let (hour_start, hour_end) = rollup_hour_window(START).unwrap();
        let (start_date, end_date) = rollup_bucket_dates(hour_start, hour_end, &nepal);

        assert_eq!(start_date.to_string(), "2026-07-01");
        assert_eq!(end_date.to_string(), "2026-07-02");
    }

    #[test]
    fn selected_exceptional_pricing_stays_scoped_to_root_and_usage_kind() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE usage_facts(
                    thread_id TEXT NOT NULL,
                    turn_id TEXT,
                    timestamp TEXT NOT NULL,
                    model TEXT NOT NULL,
                    input_tokens INTEGER NOT NULL,
                    cached_input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL
                 );
                 CREATE INDEX idx_usage_thread_model_time
                    ON usage_facts(thread_id,model,timestamp);
                 CREATE TEMP TABLE selected_activity_turns(
                    turn_id TEXT PRIMARY KEY,
                    root_turn_id TEXT NOT NULL,
                    usage_kind INTEGER NOT NULL
                 ) WITHOUT ROWID;
                 INSERT INTO selected_activity_turns VALUES
                    ('selected-a','root-a',1),
                    ('selected-b','root-b',1),
                    ('selected-review','root-a',2);
                 INSERT INTO usage_facts VALUES
                    ('thread','selected-a','2026-07-01T18:15:00.000000000Z',
                     'model',1,0,0,1),
                    ('thread','selected-a','2026-07-01T18:45:00.000000000Z',
                     'model',1,0,0,1),
                    ('thread','selected-b','2026-07-01T18:45:00.000000000Z',
                     'model',100,0,0,100),
                    ('thread','selected-review','2026-07-01T18:45:00.000000000Z',
                     'model',100,0,0,100);",
            )
            .unwrap();
        let book = PriceBook::new(
            HashMap::new(),
            HashMap::from([(
                "model".to_owned(),
                vec![
                    PriceInterval::new(
                        START.to_owned(),
                        Some("2026-07-01T18:30:00.000000000Z".to_owned()),
                        1,
                        None,
                        1,
                    ),
                    PriceInterval::new(
                        "2026-07-01T18:30:00.000000000Z".to_owned(),
                        None,
                        10,
                        None,
                        10,
                    ),
                ],
            )]),
        );

        let totals = selected_rollup_cost_on(
            &connection,
            &book,
            ActivitySelection::new("root-a", 1),
            "thread",
            START,
            "model",
            2,
            0,
            0,
            2,
        )
        .unwrap();

        assert_eq!(totals, (11, 0));
    }
}
