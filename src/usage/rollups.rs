use super::{UsageAccumulator, UsageTotals, reader};
use crate::{calendar::canonical_utc_timestamp, costing::PriceBook};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, Row, params};

#[derive(Clone, Copy)]
pub(crate) enum TotalsScope<'a> {
    Global,
    Thread { thread_id: &'a str },
}

impl<'a> TotalsScope<'a> {
    fn thread_id(self) -> Option<&'a str> {
        match self {
            Self::Global => None,
            Self::Thread { thread_id } => Some(thread_id),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RollupScope<'a> {
    Global,
    Thread {
        thread_id: &'a str,
    },
    Turn {
        thread_id: &'a str,
        turn_id: &'a str,
    },
    Agent {
        thread_id: &'a str,
        agent_run_id: &'a str,
    },
    Effort {
        thread_id: &'a str,
        effort: Option<&'a str>,
    },
}

pub(crate) fn read_totals_on(
    connection: &Connection,
    start_at: Option<&str>,
    end_at: Option<&str>,
    scope: TotalsScope<'_>,
) -> Result<UsageTotals> {
    if start_at.is_none() && end_at.is_none() {
        read_all_time_totals_on(connection, scope)
    } else {
        reader::read_range_totals_on(connection, start_at, end_at, scope.thread_id())
    }
}

pub(crate) fn read_all_time_totals_on(
    connection: &Connection,
    scope: TotalsScope<'_>,
) -> Result<UsageTotals> {
    let groups = match scope {
        TotalsScope::Global => {
            let mut statement = connection.prepare(
                "SELECT activity_hour,model,
                        COALESCE(SUM(input_tokens),0),
                        COALESCE(SUM(cached_input_tokens),0),
                        COALESCE(SUM(output_tokens),0),
                        COALESCE(SUM(reasoning_tokens),0),
                        COALESCE(SUM(total_tokens),0)
                 FROM usage_activity_rollups
                 GROUP BY activity_hour,model",
            )?;
            statement
                .query_map([], hourly_group_from_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        }
        TotalsScope::Thread { thread_id } => {
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
            statement
                .query_map([thread_id], hourly_group_from_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        }
    };
    let price_book = reader::load_price_book_on(connection)?;
    let rollup_scope = match scope {
        TotalsScope::Global => RollupScope::Global,
        TotalsScope::Thread { thread_id } => RollupScope::Thread { thread_id },
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
            rollup_scope,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn price_hourly_rollup_on(
    connection: &Connection,
    price_book: &PriceBook,
    scope: RollupScope<'_>,
    activity_hour: &str,
    model: &str,
    input_tokens: i64,
    cached_input_tokens: i64,
    output_tokens: i64,
    total_tokens: u64,
) -> Result<(i128, u64)> {
    let (start, end) = hour_window(activity_hour)?;
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
        _ => exceptional_cost_on(
            connection,
            price_book,
            scope,
            model,
            &first_timestamp,
            &canonical_utc_timestamp(end),
        ),
    }
}

fn exceptional_cost_on(
    connection: &Connection,
    price_book: &PriceBook,
    scope: RollupScope<'_>,
    model: &str,
    start_at: &str,
    end_at: &str,
) -> Result<(i128, u64)> {
    let sql = match scope {
        RollupScope::Global => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_model_time
             WHERE model=?1 AND timestamp>=?2 AND timestamp<?3"
        }
        RollupScope::Thread { .. } => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_thread_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4"
        }
        RollupScope::Turn { .. } => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_turn_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND turn_id=?5"
        }
        RollupScope::Agent { .. } => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_agent_run
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND agent_run_id=?5"
        }
        RollupScope::Effort { .. } => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_thread_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND effort IS ?5"
        }
    };
    let mut statement = connection.prepare(sql)?;
    let mut rows = match scope {
        RollupScope::Global => statement.query(params![model, start_at, end_at])?,
        RollupScope::Thread { thread_id } => {
            statement.query(params![thread_id, model, start_at, end_at])?
        }
        RollupScope::Turn { thread_id, turn_id } => {
            statement.query(params![thread_id, model, start_at, end_at, turn_id])?
        }
        RollupScope::Agent {
            thread_id,
            agent_run_id,
        } => statement.query(params![thread_id, model, start_at, end_at, agent_run_id])?,
        RollupScope::Effort { thread_id, effort } => {
            statement.query(params![thread_id, model, start_at, end_at, effort])?
        }
    };
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

type HourlyGroup = (String, String, i64, i64, i64, i64, u64);

fn hourly_group_from_row(row: &Row<'_>) -> rusqlite::Result<HourlyGroup> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get::<_, i64>(2)?.max(0),
        row.get::<_, i64>(3)?.max(0),
        row.get::<_, i64>(4)?.max(0),
        row.get::<_, i64>(5)?.max(0),
        row.get::<_, i64>(6)?.max(0) as u64,
    ))
}

fn hour_window(activity_hour: &str) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let start = DateTime::parse_from_rfc3339(activity_hour)
        .with_context(|| format!("invalid UTC usage rollup hour {activity_hour}"))?
        .with_timezone(&Utc);
    let end = start
        .checked_add_signed(Duration::hours(1))
        .context("usage rollup hour has no successor")?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costing::PriceInterval;
    use std::collections::HashMap;

    const HOUR: &str = "2026-07-01T18:00:00.000000000Z";

    #[test]
    fn exceptional_pricing_keeps_every_owner_scope_isolated() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE usage_facts(
                    thread_id TEXT NOT NULL,
                    turn_id TEXT,
                    agent_run_id TEXT,
                    effort TEXT,
                    timestamp TEXT NOT NULL,
                    model TEXT NOT NULL,
                    input_tokens INTEGER NOT NULL,
                    cached_input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL
                 );
                 CREATE INDEX idx_usage_model_time
                    ON usage_facts(model,timestamp);
                 CREATE INDEX idx_usage_thread_model_time
                    ON usage_facts(thread_id,model,timestamp);
                 CREATE INDEX idx_usage_turn_model_time
                    ON usage_facts(thread_id,turn_id,model,timestamp);
                 CREATE INDEX idx_usage_agent_run
                    ON usage_facts(agent_run_id);
                 INSERT INTO usage_facts VALUES
                    ('thread-a','turn-a','agent-a','high',
                     '2026-07-01T18:15:00.000000000Z','model',1,0,0,1),
                    ('thread-a','turn-a','agent-a','high',
                     '2026-07-01T18:45:00.000000000Z','model',1,0,0,1),
                    ('thread-b','turn-a','agent-a','high',
                     '2026-07-01T18:45:00.000000000Z','model',2,0,0,2),
                    ('thread-a','turn-b','agent-a','high',
                     '2026-07-01T18:45:00.000000000Z','model',4,0,0,4),
                    ('thread-a','turn-a','agent-b','high',
                     '2026-07-01T18:45:00.000000000Z','model',8,0,0,8),
                    ('thread-a','turn-a','agent-a','low',
                     '2026-07-01T18:45:00.000000000Z','model',16,0,0,16),
                    ('thread-a','turn-a','agent-a',NULL,
                     '2026-07-01T18:45:00.000000000Z','model',32,0,0,32),
                    ('thread-a','turn-a','agent-a','high',
                     '2026-07-01T19:00:00.000000000Z','model',64,0,0,64);",
            )
            .unwrap();
        let price_book = PriceBook::new(
            HashMap::new(),
            HashMap::from([(
                "model".to_owned(),
                vec![
                    PriceInterval::new(
                        HOUR.to_owned(),
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

        let cases = [
            (RollupScope::Global, 64, 631),
            (
                RollupScope::Thread {
                    thread_id: "thread-a",
                },
                62,
                611,
            ),
            (
                RollupScope::Turn {
                    thread_id: "thread-a",
                    turn_id: "turn-a",
                },
                58,
                571,
            ),
            (
                RollupScope::Agent {
                    thread_id: "thread-a",
                    agent_run_id: "agent-a",
                },
                54,
                531,
            ),
            (
                RollupScope::Effort {
                    thread_id: "thread-a",
                    effort: Some("high"),
                },
                14,
                131,
            ),
            (
                RollupScope::Effort {
                    thread_id: "thread-a",
                    effort: None,
                },
                32,
                320,
            ),
        ];
        for (scope, input_tokens, expected_cost) in cases {
            assert_eq!(
                price_hourly_rollup_on(
                    &connection,
                    &price_book,
                    scope,
                    HOUR,
                    "model",
                    input_tokens,
                    0,
                    0,
                    input_tokens as u64,
                )
                .unwrap(),
                (expected_cost, 0),
            );
        }
    }
}
