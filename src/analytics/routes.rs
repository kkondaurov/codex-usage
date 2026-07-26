use super::{
    overview::{OverviewResponse, OverviewYearResponse, read_summary_on, read_year_on},
    stats::{StatsRange, StatsResponse, canonical_stats_anchor, read_on as read_stats_on},
};
use crate::{
    MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR,
    calendar::{canonical_utc_timestamp, local_midnight},
    storage::WorkClass,
    web::{
        ReadRuntime, SingleFlight,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use chrono::{Datelike, Local, NaiveDate};
use serde::Deserialize;

#[derive(Clone)]
struct AnalyticsState {
    reads: ReadRuntime,
    overview: SingleFlight<NaiveDate, OverviewResponse>,
    overview_year: SingleFlight<i32, OverviewYearResponse>,
    stats: SingleFlight<(StatsRange, NaiveDate), StatsResponse>,
}

impl AnalyticsState {
    fn new(reads: ReadRuntime) -> Self {
        Self {
            reads,
            overview: SingleFlight::default(),
            overview_year: SingleFlight::default(),
            stats: SingleFlight::default(),
        }
    }
}

pub(crate) fn router(reads: ReadRuntime) -> Router {
    Router::new()
        .route("/overview", get(overview))
        .route("/overview/year", get(overview_year))
        .route("/stats", get(stats))
        .with_state(AnalyticsState::new(reads))
}

#[derive(Debug, Deserialize)]
struct OverviewYearQuery {
    year: Option<i32>,
}

async fn overview(State(state): State<AnalyticsState>) -> ApiResult<Json<OverviewResponse>> {
    let today = Local::now().date_naive();
    let reads = state.reads.clone();
    Ok(Json(
        state
            .overview
            .run(today, move || async move {
                reads
                    .snapshot(WorkClass::Heavy, move |connection| {
                        read_summary_on(connection, today)
                    })
                    .await
            })
            .await
            .map_err(ApiError::internal)?,
    ))
}

async fn overview_year(
    State(state): State<AnalyticsState>,
    Query(query): Query<OverviewYearQuery>,
) -> ApiResult<Json<OverviewYearResponse>> {
    let year = query.year.unwrap_or_else(|| Local::now().year());
    validate_public_year(year)?;
    let start = canonical_utc_timestamp(local_midnight(
        NaiveDate::from_ymd_opt(year, 1, 1).ok_or_else(|| ApiError::bad_request("invalid year"))?,
    ));
    let end = canonical_utc_timestamp(local_midnight(
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .ok_or_else(|| ApiError::bad_request("invalid year"))?,
    ));
    let reads = state.reads.clone();
    Ok(Json(
        state
            .overview_year
            .run(year, move || async move {
                reads
                    .snapshot(WorkClass::Heavy, move |connection| {
                        read_year_on(connection, year, &start, &end)
                    })
                    .await
            })
            .await
            .map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Deserialize)]
struct StatsQuery {
    range: Option<String>,
    anchor: Option<String>,
}

async fn stats(
    State(state): State<AnalyticsState>,
    Query(query): Query<StatsQuery>,
) -> ApiResult<Json<StatsResponse>> {
    let range = match query.range.as_deref().unwrap_or("day") {
        "day" => StatsRange::Day,
        "week" => StatsRange::Week,
        "month" => StatsRange::Month,
        "year" => StatsRange::Year,
        "all" => StatsRange::All,
        _ => {
            return Err(ApiError::bad_request(
                "range must be day, week, month, year, or all",
            ));
        }
    };
    // All-time is data-derived and ends in the later of the current year or
    // the latest observed data year. An anchor has no semantic meaning for it;
    // ignoring it also prevents a caller from manufacturing thousands of
    // empty future or past year buckets.
    let anchor = if range == StatsRange::All {
        Local::now().date_naive()
    } else {
        query
            .anchor
            .as_deref()
            .map(parse_date)
            .transpose()?
            .unwrap_or_else(|| Local::now().date_naive())
    };
    let display_anchor = canonical_stats_anchor(range, anchor);
    // Weekly anchors are canonicalized to Monday. The first few days of 1970
    // would otherwise normalize into 1969 and leak a response outside the
    // public date domain even though the request itself passed validation.
    if range == StatsRange::Week && display_anchor.year() < MIN_PUBLIC_YEAR {
        return Err(ApiError::bad_request(
            "weekly anchor must be on or after 1970-01-05",
        ));
    }
    validate_public_year(display_anchor.year())?;
    let reads = state.reads.clone();
    Ok(Json(
        state
            .stats
            .run((range, display_anchor), move || async move {
                reads
                    .snapshot(WorkClass::Heavy, move |connection| {
                        read_stats_on(connection, range, display_anchor)
                    })
                    .await
            })
            .await
            .map_err(ApiError::internal)?,
    ))
}

fn parse_date(value: &str) -> ApiResult<NaiveDate> {
    let exact_shape = value.len() == 10
        && value.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        });
    if !exact_shape {
        return Err(ApiError::bad_request("expected a YYYY-MM-DD date"));
    }
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| ApiError::bad_request("expected a YYYY-MM-DD date"))?;
    validate_public_year(date.year())?;
    Ok(date)
}

fn validate_public_year(year: i32) -> ApiResult<()> {
    if (MIN_PUBLIC_YEAR..=MAX_PUBLIC_YEAR).contains(&year) {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "year must be between {MIN_PUBLIC_YEAR} and {MAX_PUBLIC_YEAR}"
        )))
    }
}
