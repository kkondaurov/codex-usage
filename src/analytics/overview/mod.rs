use crate::{
    calendar::{canonical_utc_timestamp, local_midnight},
    costing::UsdAmount,
    usage::UsageTotals,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, Months, NaiveDate, Utc};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;
use std::{cmp::Ordering, collections::HashMap};

mod read;

pub(crate) use read::{read_summary_on, read_year_on};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeriodSummary {
    pub(crate) label: String,
    pub(crate) start: String,
    pub(crate) end: String,
    pub(crate) session_count: u64,
    pub(crate) message_count: u64,
    pub(crate) totals: UsageTotals,
    pub(crate) delta_cost_usd: Option<UsdAmount>,
    pub(crate) delta_percent: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OverviewPeriods {
    pub(crate) today: PeriodSummary,
    pub(crate) week: PeriodSummary,
    pub(crate) month: PeriodSummary,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HeatmapDay {
    pub(crate) date: String,
    pub(crate) cost_usd: Option<UsdAmount>,
    pub(crate) session_count: u64,
    pub(crate) message_count: u64,
    pub(crate) total_tokens: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProjectDriver {
    project: String,
    cost_usd: Option<UsdAmount>,
    share: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OverviewResponse {
    pub(crate) updated_at: Option<String>,
    pub(crate) periods: OverviewPeriods,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OverviewYearResponse {
    pub(crate) year: i32,
    pub(crate) heatmap: Vec<HeatmapDay>,
    pub(crate) top_projects: Vec<ProjectDriver>,
    pub(crate) top_sessions: Vec<TopSessionResponse>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopSessionResponse {
    pub(crate) id: String,
    pub(crate) root_thread_id: String,
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

#[derive(Clone, Debug)]
pub(crate) struct OverviewPeriodBound {
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
}

impl OverviewPeriodBound {
    pub(crate) fn start_timestamp(&self) -> String {
        canonical_utc_timestamp(self.start)
    }

    pub(crate) fn end_timestamp(&self) -> String {
        canonical_utc_timestamp(self.end)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OverviewDayBucket {
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
    pub(crate) date: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OverviewUsageAggregate {
    pub(crate) total_tokens: u64,
    pub(crate) known_cost_numerator: i128,
    pub(crate) unpriced_tokens: u64,
    pub(crate) last_timestamp: String,
}

impl OverviewUsageAggregate {
    pub(crate) fn add_aggregate(&mut self, other: &Self) {
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.known_cost_numerator = self
            .known_cost_numerator
            .saturating_add(other.known_cost_numerator);
        self.unpriced_tokens = self.unpriced_tokens.saturating_add(other.unpriced_tokens);
        if other.last_timestamp > self.last_timestamp {
            other.last_timestamp.clone_into(&mut self.last_timestamp);
        }
    }

    pub(crate) fn add_sums(
        &mut self,
        total_tokens: u64,
        known_cost_numerator: i128,
        unpriced_tokens: u64,
        timestamp: &str,
    ) {
        self.total_tokens = self.total_tokens.saturating_add(total_tokens);
        self.known_cost_numerator = self
            .known_cost_numerator
            .saturating_add(known_cost_numerator);
        self.unpriced_tokens = self.unpriced_tokens.saturating_add(unpriced_tokens);
        if timestamp > self.last_timestamp.as_str() {
            timestamp.clone_into(&mut self.last_timestamp);
        }
    }

    pub(crate) fn cost_usd(&self) -> Option<UsdAmount> {
        (self.unpriced_tokens == 0)
            .then_some(UsdAmount::from_cost_numerator(self.known_cost_numerator))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OverviewSessionRank {
    pub(crate) thread_id: String,
    pub(crate) total_tokens: u64,
    pub(crate) known_cost_numerator: i128,
    pub(crate) unpriced_tokens: u64,
}

pub(crate) fn overview_summary_bounds(today: NaiveDate) -> [OverviewPeriodBound; 6] {
    let today_start = local_midnight(today);
    let tomorrow = local_midnight(today + Duration::days(1));
    let week_date = today - Duration::days(today.weekday().num_days_from_monday() as i64);
    let week_start = local_midnight(week_date);
    let month_date = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);
    let month_start = local_midnight(month_date);
    let previous_month_date = month_date
        .checked_sub_months(Months::new(1))
        .unwrap_or(month_date);
    let previous_month_start = local_midnight(previous_month_date);
    let previous_week_start = local_midnight(week_date - Duration::days(7));
    let previous_day_start = local_midnight(today - Duration::days(1));
    [
        OverviewPeriodBound {
            start: today_start,
            end: tomorrow,
        },
        OverviewPeriodBound {
            start: previous_day_start,
            end: today_start,
        },
        OverviewPeriodBound {
            start: week_start,
            end: tomorrow,
        },
        OverviewPeriodBound {
            start: previous_week_start,
            end: week_start,
        },
        OverviewPeriodBound {
            start: month_start,
            end: tomorrow,
        },
        OverviewPeriodBound {
            start: previous_month_start,
            end: month_start,
        },
    ]
}

pub(crate) fn overview_period_summary(
    label: &str,
    bounds: &OverviewPeriodBound,
    totals: UsageTotals,
    previous: &UsageTotals,
    session_count: u64,
    message_count: u64,
) -> PeriodSummary {
    let current_cost = totals.cost_usd.map(UsdAmount::cost_numerator);
    let previous_cost = previous.cost_usd.map(UsdAmount::cost_numerator);
    let delta_cost_usd = current_cost
        .zip(previous_cost)
        .map(|(current, prior)| UsdAmount::from_cost_numerator(current - prior));
    let delta_percent = current_cost
        .zip(previous_cost)
        .and_then(|(current, prior)| exact_ratio_percent(current - prior, prior));
    PeriodSummary {
        label: label.into(),
        start: bounds.start_timestamp(),
        end: bounds.end_timestamp(),
        session_count,
        message_count,
        totals,
        delta_cost_usd,
        delta_percent,
    }
}

fn exact_ratio_percent(numerator: i128, denominator: i128) -> Option<f64> {
    if denominator <= 0 {
        return None;
    }
    (Decimal::from_i128_with_scale(numerator, 0) / Decimal::from_i128_with_scale(denominator, 0)
        * Decimal::from(100))
    .to_f64()
}

pub(crate) fn overview_year_days(year: i32) -> Result<Vec<OverviewDayBucket>> {
    let mut buckets = Vec::new();
    let mut date = NaiveDate::from_ymd_opt(year, 1, 1).context("invalid year")?;
    let limit = NaiveDate::from_ymd_opt(year + 1, 1, 1).context("invalid year")?;
    while date < limit {
        let next_date = date + Duration::days(1);
        push_nonempty_day(
            &mut buckets,
            local_midnight(date),
            local_midnight(next_date),
            date.to_string(),
        );
        date = next_date;
    }
    Ok(buckets)
}

fn push_nonempty_day(
    buckets: &mut Vec<OverviewDayBucket>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    date: String,
) {
    if start < end {
        buckets.push(OverviewDayBucket { start, end, date });
    }
}

pub(crate) fn rank_overview_year_projects(
    sessions: &HashMap<String, OverviewUsageAggregate>,
    projects: &HashMap<String, String>,
) -> Vec<ProjectDriver> {
    let mut by_project = HashMap::<String, OverviewUsageAggregate>::new();
    for (thread_id, usage) in sessions {
        let project = projects.get(thread_id).map(String::as_str).unwrap_or("—");
        if let Some(aggregate) = by_project.get_mut(project) {
            aggregate.add_aggregate(usage);
        } else {
            by_project.insert(project.to_owned(), usage.clone());
        }
    }
    let total_priced_cost_numerator = by_project
        .values()
        .filter(|usage| usage.unpriced_tokens == 0)
        .map(|usage| usage.known_cost_numerator)
        .sum::<i128>();
    let mut ranked = by_project.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_project, left), (right_project, right)| {
        let price_order = (left.unpriced_tokens > 0).cmp(&(right.unpriced_tokens > 0));
        let value_order = if left.unpriced_tokens == 0 && right.unpriced_tokens == 0 {
            right.known_cost_numerator.cmp(&left.known_cost_numerator)
        } else if left.unpriced_tokens > 0 && right.unpriced_tokens > 0 {
            right.total_tokens.cmp(&left.total_tokens)
        } else {
            Ordering::Equal
        };
        price_order
            .then(value_order)
            .then_with(|| left_project.cmp(right_project))
    });
    ranked
        .into_iter()
        .take(3)
        .map(|(project, usage)| ProjectDriver {
            project,
            cost_usd: usage.cost_usd(),
            share: usage.cost_usd().and_then(|_| {
                if total_priced_cost_numerator > 0 {
                    (Decimal::from_i128_with_scale(usage.known_cost_numerator, 0)
                        / Decimal::from_i128_with_scale(total_priced_cost_numerator, 0))
                    .to_f64()
                } else {
                    Some(0.0)
                }
            }),
        })
        .collect()
}

pub(crate) fn rank_overview_year_sessions(
    sessions: &HashMap<String, OverviewUsageAggregate>,
) -> Vec<OverviewSessionRank> {
    let mut ranked = sessions.iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_id, left), (right_id, right)| {
        let price_order = (left.unpriced_tokens > 0).cmp(&(right.unpriced_tokens > 0));
        let value_order = if left.unpriced_tokens == 0 && right.unpriced_tokens == 0 {
            right.known_cost_numerator.cmp(&left.known_cost_numerator)
        } else if left.unpriced_tokens > 0 && right.unpriced_tokens > 0 {
            right.total_tokens.cmp(&left.total_tokens)
        } else {
            Ordering::Equal
        };
        price_order
            .then(value_order)
            .then_with(|| right.last_timestamp.cmp(&left.last_timestamp))
            .then_with(|| right_id.cmp(left_id))
    });
    ranked
        .into_iter()
        .take(3)
        .map(|(thread_id, usage)| OverviewSessionRank {
            thread_id: thread_id.clone(),
            total_tokens: usage.total_tokens,
            known_cost_numerator: usage.known_cost_numerator,
            unpriced_tokens: usage.unpriced_tokens,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        OverviewDayBucket, OverviewUsageAggregate, overview_period_summary,
        overview_summary_bounds, overview_year_days, push_nonempty_day,
        rank_overview_year_projects, rank_overview_year_sessions,
    };
    use crate::{costing::UsdAmount, usage::UsageTotals};
    use chrono::{Datelike, NaiveDate, TimeZone, Utc};
    use std::collections::HashMap;

    fn aggregate(
        total_tokens: u64,
        known_cost_numerator: i128,
        unpriced_tokens: u64,
        last_timestamp: &str,
    ) -> OverviewUsageAggregate {
        OverviewUsageAggregate {
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
            last_timestamp: last_timestamp.into(),
        }
    }

    #[test]
    fn summary_bounds_keep_current_and_previous_periods_in_fixed_slots() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let bounds = overview_summary_bounds(today);
        let local_dates = bounds
            .iter()
            .map(|bound| {
                (
                    bound.start.with_timezone(&chrono::Local).date_naive(),
                    bound.end.with_timezone(&chrono::Local).date_naive(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            local_dates,
            [
                (today, NaiveDate::from_ymd_opt(2026, 8, 2).unwrap()),
                (NaiveDate::from_ymd_opt(2026, 7, 31).unwrap(), today),
                (
                    NaiveDate::from_ymd_opt(2026, 7, 27).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 8, 2).unwrap(),
                ),
                (
                    NaiveDate::from_ymd_opt(2026, 7, 20).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 7, 27).unwrap(),
                ),
                (today, NaiveDate::from_ymd_opt(2026, 8, 2).unwrap()),
                (NaiveDate::from_ymd_opt(2026, 7, 1).unwrap(), today),
            ]
        );
    }

    #[test]
    fn period_summary_requires_complete_pricing_for_deltas() {
        let bounds = &overview_summary_bounds(NaiveDate::from_ymd_opt(2026, 8, 1).unwrap())[0];
        let current = UsageTotals {
            known_cost_numerator: 300,
            ..UsageTotals::default()
        }
        .finish();
        let previous = UsageTotals {
            known_cost_numerator: 200,
            ..UsageTotals::default()
        }
        .finish();
        let summary = overview_period_summary("Today", bounds, current, &previous, 2, 3);
        assert_eq!(summary.delta_cost_usd.unwrap().cost_numerator(), 100);
        assert_eq!(summary.delta_percent, Some(50.0));

        let incomplete = UsageTotals {
            known_cost_numerator: 300,
            unpriced_tokens: 1,
            ..UsageTotals::default()
        }
        .finish();
        let summary = overview_period_summary("Today", bounds, incomplete, &previous, 2, 3);
        assert_eq!(summary.delta_cost_usd, None);
        assert_eq!(summary.delta_percent, None);
    }

    #[test]
    fn annual_days_are_gapless_and_omit_zero_duration_civil_dates() {
        let leap = overview_year_days(2024).unwrap();
        assert_eq!(leap.len(), 366);
        assert_eq!(leap.first().unwrap().date, "2024-01-01");
        assert_eq!(leap.last().unwrap().date, "2024-12-31");
        assert!(leap.windows(2).all(|days| days[0].end == days[1].start));

        let boundary = Utc.with_ymd_and_hms(2011, 12, 30, 10, 0, 0).unwrap();
        let mut buckets = Vec::<OverviewDayBucket>::new();
        push_nonempty_day(&mut buckets, boundary, boundary, "2011-12-30".into());
        assert!(buckets.is_empty());
    }

    #[test]
    fn aggregates_saturate_and_hide_partial_costs() {
        let mut value = aggregate(u64::MAX, i128::MAX, 0, "2026-01-01T00:00:00Z");
        value.add_sums(1, 1, 1, "2026-01-02T00:00:00Z");
        assert_eq!(value.total_tokens, u64::MAX);
        assert_eq!(value.known_cost_numerator, i128::MAX);
        assert_eq!(value.unpriced_tokens, 1);
        assert_eq!(value.last_timestamp, "2026-01-02T00:00:00Z");
        assert_eq!(value.cost_usd(), None);
    }

    #[test]
    fn project_ranking_keeps_priced_leaders_before_unpriced_usage() {
        let sessions = HashMap::from([
            ("a".into(), aggregate(10, 100, 0, "2026-01-01T00:00:00Z")),
            ("b".into(), aggregate(20, 300, 0, "2026-01-01T00:00:00Z")),
            (
                "c".into(),
                aggregate(1_000, 9_000, 1, "2026-01-01T00:00:00Z"),
            ),
            ("d".into(), aggregate(30, 200, 0, "2026-01-01T00:00:00Z")),
        ]);
        let projects = HashMap::from([
            ("a".into(), "alpha".into()),
            ("b".into(), "beta".into()),
            ("c".into(), "unpriced".into()),
            ("d".into(), "alpha".into()),
        ]);
        let ranked = rank_overview_year_projects(&sessions, &projects);
        assert_eq!(
            ranked
                .iter()
                .map(|row| row.project.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta", "unpriced"]
        );
        assert_eq!(
            ranked[0].cost_usd,
            Some(UsdAmount::from_cost_numerator(300))
        );
        assert_eq!(ranked[0].share, Some(0.5));
        assert_eq!(ranked[1].share, Some(0.5));
        assert_eq!(ranked[2].cost_usd, None);
        assert_eq!(ranked[2].share, None);
    }

    #[test]
    fn session_ranking_uses_price_state_value_recency_and_id() {
        let sessions = HashMap::from([
            ("priced-low".into(), aggregate(10, 100, 0, "2026-01-02")),
            ("priced-new-a".into(), aggregate(20, 200, 0, "2026-01-03")),
            ("priced-new-z".into(), aggregate(20, 200, 0, "2026-01-03")),
            ("unpriced".into(), aggregate(9_999, 9_999, 1, "2026-01-04")),
        ]);
        let ranked = rank_overview_year_sessions(&sessions);
        assert_eq!(
            ranked
                .iter()
                .map(|row| row.thread_id.as_str())
                .collect::<Vec<_>>(),
            ["priced-new-z", "priced-new-a", "priced-low"]
        );
    }

    #[test]
    fn invalid_annual_year_is_rejected() {
        let error = overview_year_days(i32::MAX).unwrap_err();
        assert!(error.to_string().contains("invalid year"));
    }

    #[test]
    fn period_bound_timestamps_remain_canonical_utc() {
        let bounds = overview_summary_bounds(NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
        assert!(bounds.iter().all(|bound| {
            bound.start_timestamp().ends_with('Z') && bound.end_timestamp().ends_with('Z')
        }));
        assert_eq!(bounds[0].start.year(), 2026);
    }
}
