use crate::{
    MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR, calendar::local_midnight, costing::UsdAmount,
    usage::UsageTotals,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, Local, Months, NaiveDate, Utc};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

mod read;

pub(crate) use read::read_on;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatsRange {
    Day,
    Week,
    Month,
    Year,
    All,
}

impl StatsRange {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Year => "year",
            Self::All => "all",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatsBucket {
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
    pub(crate) label: String,
}

#[derive(Debug, Default)]
pub(crate) struct StatsBucketAggregate {
    pub(crate) totals: UsageTotals,
    pub(crate) session_count: u64,
    pub(crate) known_cost_numerator: i128,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatsRow {
    pub(crate) period_start: String,
    pub(crate) period_end: String,
    pub(crate) label: String,
    pub(crate) session_count: u64,
    #[serde(flatten)]
    pub(crate) totals: UsageTotals,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatsResponse {
    pub(crate) range: String,
    pub(crate) anchor: String,
    pub(crate) label: String,
    pub(crate) totals: UsageTotals,
    pub(crate) rows: Vec<StatsRow>,
    pub(crate) trend: Vec<Option<UsdAmount>>,
}

pub(crate) fn canonical_stats_anchor(range: StatsRange, anchor: NaiveDate) -> NaiveDate {
    match range {
        StatsRange::Week => anchor - Duration::days(anchor.weekday().num_days_from_monday() as i64),
        StatsRange::Month => NaiveDate::from_ymd_opt(anchor.year(), anchor.month(), 1)
            .expect("a valid date has a valid first day of its month"),
        StatsRange::Year => NaiveDate::from_ymd_opt(anchor.year(), 1, 1)
            .expect("a valid date has a valid first day of its year"),
        StatsRange::Day | StatsRange::All => anchor,
    }
}

pub(crate) fn stats_range_label(range: StatsRange, anchor: NaiveDate) -> String {
    let anchor = canonical_stats_anchor(range, anchor);
    match range {
        StatsRange::Day => anchor.format("%b %-d, %Y").to_string(),
        StatsRange::Week => format!("Week of {}", anchor.format("%b %-d, %Y")),
        StatsRange::Month => anchor.format("%B %Y").to_string(),
        StatsRange::Year => anchor.year().to_string(),
        StatsRange::All => "All time".into(),
    }
}

pub(crate) fn stats_buckets(
    range: StatsRange,
    anchor: NaiveDate,
    mut occupied_local_years: BTreeSet<i32>,
) -> Result<Vec<StatsBucket>> {
    let anchor = canonical_stats_anchor(range, anchor);
    let mut buckets = Vec::new();
    match range {
        StatsRange::Day => {
            let start = local_midnight(anchor);
            let end = local_midnight(anchor + Duration::days(1));
            let mut cursor = start;
            while cursor < end {
                let next = (cursor + Duration::hours(1)).min(end);
                buckets.push(StatsBucket {
                    start: cursor,
                    end: next,
                    label: cursor.with_timezone(&Local).format("%H:%M").to_string(),
                });
                cursor = next;
            }
            let labels = disambiguate_repeated_labels(
                buckets
                    .iter()
                    .map(|bucket| {
                        (
                            bucket.label.clone(),
                            bucket.start.with_timezone(&Local).format("%:z").to_string(),
                        )
                    })
                    .collect(),
            );
            for (bucket, disambiguated) in buckets.iter_mut().zip(labels) {
                bucket.label = disambiguated;
            }
        }
        StatsRange::Week => {
            for offset in 0..7 {
                let date = anchor + Duration::days(offset);
                push_nonempty_stats_bucket(
                    &mut buckets,
                    local_midnight(date),
                    local_midnight(date + Duration::days(1)),
                    date.format("%a %-d").to_string(),
                );
            }
        }
        StatsRange::Month => {
            let mut date = anchor;
            let end = date
                .checked_add_months(Months::new(1))
                .context("invalid month")?;
            while date < end {
                push_nonempty_stats_bucket(
                    &mut buckets,
                    local_midnight(date),
                    local_midnight(date + Duration::days(1)),
                    date.format("%Y-%m-%d").to_string(),
                );
                date += Duration::days(1);
            }
        }
        StatsRange::Year => {
            for month in 1..=12 {
                let date =
                    NaiveDate::from_ymd_opt(anchor.year(), month, 1).context("invalid year")?;
                let next = date
                    .checked_add_months(Months::new(1))
                    .context("invalid month")?;
                push_nonempty_stats_bucket(
                    &mut buckets,
                    local_midnight(date),
                    local_midnight(next),
                    date.format("%b").to_string(),
                );
            }
        }
        StatsRange::All => {
            if occupied_local_years.is_empty()
                || occupied_local_years
                    .iter()
                    .any(|year| *year <= anchor.year())
            {
                occupied_local_years.insert(anchor.year());
            }
            let public_start = NaiveDate::from_ymd_opt(MIN_PUBLIC_YEAR, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .context("invalid public timestamp lower boundary")?
                .and_utc();
            let public_end = NaiveDate::from_ymd_opt(MAX_PUBLIC_YEAR + 1, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .context("invalid public timestamp upper boundary")?
                .and_utc();
            for year in occupied_local_years {
                let date = NaiveDate::from_ymd_opt(year, 1, 1).context("invalid year")?;
                let next = NaiveDate::from_ymd_opt(year + 1, 1, 1).context("invalid year")?;
                let start = if year == MIN_PUBLIC_YEAR {
                    public_start
                } else {
                    local_midnight(date)
                };
                let end = if year == MAX_PUBLIC_YEAR {
                    public_end
                } else {
                    local_midnight(next)
                };
                push_nonempty_stats_bucket(&mut buckets, start, end, year.to_string());
            }
        }
    }
    Ok(buckets)
}

pub(crate) fn stats_totals_from_aggregates(aggregates: &[StatsBucketAggregate]) -> UsageTotals {
    let total_cost_numerator = aggregates.iter().fold(0i128, |total, row| {
        total.saturating_add(row.known_cost_numerator)
    });
    let mut totals = aggregates
        .iter()
        .fold(UsageTotals::default(), |mut total, row| {
            total.input_tokens = total.input_tokens.saturating_add(row.totals.input_tokens);
            total.cached_input_tokens = total
                .cached_input_tokens
                .saturating_add(row.totals.cached_input_tokens);
            total.output_tokens = total.output_tokens.saturating_add(row.totals.output_tokens);
            total.reasoning_tokens = total
                .reasoning_tokens
                .saturating_add(row.totals.reasoning_tokens);
            total.total_tokens = total.total_tokens.saturating_add(row.totals.total_tokens);
            total.unpriced_tokens = total
                .unpriced_tokens
                .saturating_add(row.totals.unpriced_tokens);
            total
        });
    totals.known_cost_numerator = total_cost_numerator;
    totals.finish()
}

fn disambiguate_repeated_labels(labels: Vec<(String, String)>) -> Vec<String> {
    let mut counts = HashMap::<String, usize>::new();
    for (label, _) in &labels {
        *counts.entry(label.clone()).or_default() += 1;
    }
    labels
        .into_iter()
        .map(|(label, suffix)| {
            if counts.get(&label).copied().unwrap_or_default() > 1 {
                format!("{label} ({suffix})")
            } else {
                label
            }
        })
        .collect()
}

fn push_nonempty_stats_bucket(
    buckets: &mut Vec<StatsBucket>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    label: String,
) {
    // A political timezone change can delete an entire civil date. Such a date
    // has no UTC interval and must not become a zero-duration analytical bucket.
    if start < end {
        buckets.push(StatsBucket { start, end, label });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn bucket_labels(buckets: &[StatsBucket]) -> Vec<&str> {
        buckets.iter().map(|bucket| bucket.label.as_str()).collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn aggregate(
        input_tokens: u64,
        cached_input_tokens: u64,
        output_tokens: u64,
        reasoning_tokens: u64,
        total_tokens: u64,
        known_cost_numerator: i128,
        unpriced_tokens: u64,
        session_count: u64,
    ) -> StatsBucketAggregate {
        StatsBucketAggregate {
            totals: UsageTotals {
                input_tokens,
                cached_input_tokens,
                output_tokens,
                reasoning_tokens,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
                ..UsageTotals::default()
            }
            .finish(),
            session_count,
            known_cost_numerator,
        }
    }

    #[test]
    fn ranges_have_exact_transport_names() {
        assert_eq!(
            [
                StatsRange::Day,
                StatsRange::Week,
                StatsRange::Month,
                StatsRange::Year,
                StatsRange::All,
            ]
            .map(StatsRange::as_str),
            ["day", "week", "month", "year", "all"]
        );
    }

    #[test]
    fn canonical_anchors_and_range_labels_are_exact() {
        let anchor = date(2026, 7, 15);
        for (range, expected_anchor, expected_label) in [
            (StatsRange::Day, date(2026, 7, 15), "Jul 15, 2026"),
            (StatsRange::Week, date(2026, 7, 13), "Week of Jul 13, 2026"),
            (StatsRange::Month, date(2026, 7, 1), "July 2026"),
            (StatsRange::Year, date(2026, 1, 1), "2026"),
            (StatsRange::All, date(2026, 7, 15), "All time"),
        ] {
            assert_eq!(canonical_stats_anchor(range, anchor), expected_anchor);
            assert_eq!(stats_range_label(range, anchor), expected_label);
        }
    }

    #[test]
    fn calendar_ranges_have_exact_bucket_counts_and_labels() {
        let day = stats_buckets(StatsRange::Day, date(2026, 1, 15), BTreeSet::new()).unwrap();
        assert_eq!(day.len(), 24);
        assert_eq!(day.first().unwrap().label, "00:00");
        assert_eq!(day.last().unwrap().label, "23:00");
        assert_eq!(
            day.first().unwrap().start,
            local_midnight(date(2026, 1, 15))
        );
        assert_eq!(day.last().unwrap().end, local_midnight(date(2026, 1, 16)));

        let week = stats_buckets(StatsRange::Week, date(2026, 7, 15), BTreeSet::new()).unwrap();
        assert_eq!(week.len(), 7);
        assert_eq!(
            bucket_labels(&week),
            [
                "Mon 13", "Tue 14", "Wed 15", "Thu 16", "Fri 17", "Sat 18", "Sun 19"
            ]
        );

        let month = stats_buckets(StatsRange::Month, date(2024, 2, 19), BTreeSet::new()).unwrap();
        assert_eq!(month.len(), 29);
        assert_eq!(month.first().unwrap().label, "2024-02-01");
        assert_eq!(month.last().unwrap().label, "2024-02-29");

        let year = stats_buckets(StatsRange::Year, date(2026, 7, 15), BTreeSet::new()).unwrap();
        assert_eq!(year.len(), 12);
        assert_eq!(
            bucket_labels(&year),
            [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
            ]
        );
    }

    #[test]
    fn all_time_sparse_years_follow_the_current_year_injection_rule() {
        let anchor = date(2026, 7, 19);

        let empty = stats_buckets(StatsRange::All, anchor, BTreeSet::new()).unwrap();
        assert_eq!(bucket_labels(&empty), ["2026"]);

        let future_only =
            stats_buckets(StatsRange::All, anchor, BTreeSet::from([2027, 2500])).unwrap();
        assert_eq!(bucket_labels(&future_only), ["2027", "2500"]);

        let mixed = stats_buckets(StatsRange::All, anchor, BTreeSet::from([2025, 2500])).unwrap();
        assert_eq!(bucket_labels(&mixed), ["2025", "2026", "2500"]);
    }

    #[test]
    fn all_time_public_edge_years_use_exact_utc_outer_bounds() {
        let buckets = stats_buckets(
            StatsRange::All,
            date(2026, 7, 19),
            BTreeSet::from([MIN_PUBLIC_YEAR, MAX_PUBLIC_YEAR]),
        )
        .unwrap();
        let first = buckets
            .iter()
            .find(|bucket| bucket.label == MIN_PUBLIC_YEAR.to_string())
            .unwrap();
        let last = buckets
            .iter()
            .find(|bucket| bucket.label == MAX_PUBLIC_YEAR.to_string())
            .unwrap();

        assert_eq!(
            first.start,
            date(MIN_PUBLIC_YEAR, 1, 1)
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
        );
        assert_eq!(
            last.end,
            date(MAX_PUBLIC_YEAR + 1, 1, 1)
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
        );
    }

    #[test]
    fn stats_grand_total_preserves_and_saturates_i128_fixed_point_exactly() {
        let aggregates = (0..10)
            .map(|_| aggregate(0, 0, 0, 0, 1, 1_000_000_000_000_000_001, 0, 1))
            .collect::<Vec<_>>();

        let totals = stats_totals_from_aggregates(&aggregates);
        assert_eq!(totals.known_cost_numerator, 10_000_000_000_000_000_010);
        assert!(totals.known_cost_numerator > i128::from(i64::MAX));
        assert!(totals.known_cost_numerator > 9_007_199_254_740_991);
        assert_eq!(
            totals.cost_usd.unwrap().decimal_string(),
            "10000000.00000000001"
        );

        let saturated = stats_totals_from_aggregates(&[
            aggregate(0, 0, 0, 0, 0, i128::MAX, 0, 0),
            aggregate(0, 0, 0, 0, 0, 1, 0, 0),
        ]);
        assert_eq!(saturated.known_cost_numerator, i128::MAX);
    }

    #[test]
    fn stats_dtos_serialize_exactly_with_row_aligned_trend() {
        let first = aggregate(10, 4, 3, 2, 17, 2_000_000_000_000, 0, 2);
        let second = aggregate(5, 0, 1, 0, 6, 0, 6, 1);
        let totals = stats_totals_from_aggregates(&[
            aggregate(10, 4, 3, 2, 17, 2_000_000_000_000, 0, 2),
            aggregate(5, 0, 1, 0, 6, 0, 6, 1),
        ]);
        let rows = vec![
            StatsRow {
                period_start: "2026-07-15T00:00:00+00:00".into(),
                period_end: "2026-07-15T01:00:00+00:00".into(),
                label: "00:00".into(),
                session_count: first.session_count,
                totals: first.totals,
            },
            StatsRow {
                period_start: "2026-07-15T01:00:00+00:00".into(),
                period_end: "2026-07-15T02:00:00+00:00".into(),
                label: "01:00".into(),
                session_count: second.session_count,
                totals: second.totals,
            },
        ];
        let trend = rows.iter().map(|row| row.totals.cost_usd).collect();
        let response = StatsResponse {
            range: "day".into(),
            anchor: "2026-07-15".into(),
            label: "Jul 15, 2026".into(),
            totals,
            rows,
            trend,
        };

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "range": "day",
                "anchor": "2026-07-15",
                "label": "Jul 15, 2026",
                "totals": {
                    "inputTokens": 15,
                    "cachedInputTokens": 4,
                    "outputTokens": 4,
                    "reasoningTokens": 2,
                    "totalTokens": 23,
                    "blendedTokens": 15,
                    "costUsd": null,
                    "unpricedTokens": 6,
                    "pricingComplete": false
                },
                "rows": [
                    {
                        "periodStart": "2026-07-15T00:00:00+00:00",
                        "periodEnd": "2026-07-15T01:00:00+00:00",
                        "label": "00:00",
                        "sessionCount": 2,
                        "inputTokens": 10,
                        "cachedInputTokens": 4,
                        "outputTokens": 3,
                        "reasoningTokens": 2,
                        "totalTokens": 17,
                        "blendedTokens": 9,
                        "costUsd": "2.00",
                        "unpricedTokens": 0,
                        "pricingComplete": true
                    },
                    {
                        "periodStart": "2026-07-15T01:00:00+00:00",
                        "periodEnd": "2026-07-15T02:00:00+00:00",
                        "label": "01:00",
                        "sessionCount": 1,
                        "inputTokens": 5,
                        "cachedInputTokens": 0,
                        "outputTokens": 1,
                        "reasoningTokens": 0,
                        "totalTokens": 6,
                        "blendedTokens": 6,
                        "costUsd": null,
                        "unpricedTokens": 6,
                        "pricingComplete": false
                    }
                ],
                "trend": ["2.00", null]
            })
        );
    }

    #[test]
    fn stats_omit_civil_dates_without_a_utc_interval() {
        let boundary = DateTime::parse_from_rfc3339("2011-12-30T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut buckets = Vec::new();
        push_nonempty_stats_bucket(&mut buckets, boundary, boundary, "2011-12-30".into());
        assert!(buckets.is_empty());
    }

    #[test]
    fn duplicate_hour_labels_receive_offsets_while_unique_labels_stay_plain() {
        assert_eq!(
            disambiguate_repeated_labels(vec![
                ("01:00".into(), "+02:00".into()),
                ("02:00".into(), "+02:00".into()),
                ("02:00".into(), "+01:00".into()),
                ("03:00".into(), "+01:00".into()),
            ]),
            ["01:00", "02:00 (+02:00)", "02:00 (+01:00)", "03:00"]
        );
    }
}
