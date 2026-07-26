use super::{
    catalog::{SessionRecord, read_projects_on},
    list::{
        MAX_SEARCH_CHARS, SessionListPage, SessionListRecord, SessionListRequest, SessionListSort,
        read_session_page_on,
    },
    summary::{
        AgentSummaryRecord, ModelUsageRecord, SessionDetailRecord, SessionSummaryRecord,
        ToolSummaryRecord, read_summary_on,
    },
};
use crate::{
    MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR,
    calendar::{canonical_utc_timestamp, local_midnight},
    costing::UsdAmount,
    storage::WorkClass,
    usage::UsageTotals,
    web::{
        ReadRuntime, SingleFlight,
        error::{ApiError, ApiResult},
        pagination::{clamped_page_size, validated_page},
    },
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct SessionsState {
    reads: ReadRuntime,
    lists: SingleFlight<SessionListRequest, SessionsResponse>,
}

impl SessionsState {
    fn new(reads: ReadRuntime) -> Self {
        Self {
            reads,
            lists: SingleFlight::default(),
        }
    }
}

pub(crate) fn router(reads: ReadRuntime) -> Router {
    Router::new()
        .route("/projects", get(projects))
        .route("/sessions", get(sessions))
        .route("/sessions/{id}/summary", get(session_summary))
        .with_state(SessionsState::new(reads))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionRow {
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

impl From<SessionRecord> for SessionRow {
    fn from(record: SessionRecord) -> Self {
        let root_thread_id = record.id.clone();
        Self {
            id: record.id,
            root_thread_id,
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

impl From<SessionListRecord> for SessionRow {
    fn from(record: SessionListRecord) -> Self {
        let root_thread_id = record.id.clone();
        Self {
            id: record.id,
            root_thread_id,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionsQuery {
    q: Option<String>,
    date: Option<String>,
    start: Option<String>,
    end: Option<String>,
    project: Option<String>,
    sort: Option<String>,
    page: Option<u64>,
    page_size: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionsResponse {
    items: Vec<SessionRow>,
    projects: Vec<String>,
    page: u64,
    page_size: u64,
    total: u64,
    total_pages: u64,
}

async fn sessions(
    State(state): State<SessionsState>,
    Query(query): Query<SessionsQuery>,
) -> ApiResult<Json<SessionsResponse>> {
    let (start, end) = query_bounds(
        query.date.as_deref(),
        query.start.as_deref(),
        query.end.as_deref(),
    )?;
    let page = validated_page(query.page)?;
    let page_size = clamped_page_size(query.page_size, 50, 200);
    let project = query.project;
    let search = query.q;
    if search
        .as_deref()
        .is_some_and(|value| value.chars().count() > MAX_SEARCH_CHARS)
    {
        return Err(ApiError::bad_request(format!(
            "session search must be at most {MAX_SEARCH_CHARS} characters"
        )));
    }
    let sort = query.sort.unwrap_or_else(|| "recent".into());
    if !matches!(sort.as_str(), "recent" | "cost") {
        return Err(ApiError::bad_request("sort must be recent or cost"));
    }
    let sort = if sort == "cost" {
        SessionListSort::Cost
    } else {
        SessionListSort::Recent
    };
    let request = SessionListRequest {
        start,
        end,
        project,
        search,
        sort,
        page,
        page_size,
    };
    let reads = state.reads.clone();
    let response = state
        .lists
        .run(request.clone(), move || async move {
            reads
                .snapshot(WorkClass::Heavy, move |connection| {
                    let SessionListPage {
                        items,
                        page,
                        page_size,
                        total,
                        total_pages,
                    } = read_session_page_on(connection, &request)?;
                    Ok(SessionsResponse {
                        items: items.into_iter().map(SessionRow::from).collect(),
                        projects: read_projects_on(connection)?,
                        page,
                        page_size,
                        total,
                        total_pages,
                    })
                })
                .await
        })
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(response))
}

#[derive(Debug, Serialize)]
struct ProjectsResponse {
    items: Vec<String>,
}

async fn projects(State(state): State<SessionsState>) -> ApiResult<Json<ProjectsResponse>> {
    Ok(Json(ProjectsResponse {
        items: state
            .reads
            .snapshot(WorkClass::Heavy, read_projects_on)
            .await
            .map_err(ApiError::internal)?,
    }))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelUsage {
    model: String,
    effort: Option<String>,
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    total_tokens: u64,
    cost_usd: Option<UsdAmount>,
    unpriced_tokens: u64,
}

impl From<ModelUsageRecord> for ModelUsage {
    fn from(record: ModelUsageRecord) -> Self {
        Self {
            model: record.model,
            effort: record.effort,
            input_tokens: record.input_tokens,
            cached_input_tokens: record.cached_input_tokens,
            output_tokens: record.output_tokens,
            reasoning_tokens: record.reasoning_tokens,
            total_tokens: record.total_tokens,
            cost_usd: record.cost_usd,
            unpriced_tokens: record.unpriced_tokens,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentSummary {
    id: String,
    label: String,
    path: Option<String>,
    nickname: Option<String>,
    status: String,
    turn_count: u64,
    tool_count: u64,
    total_tokens: u64,
    cost_usd: Option<UsdAmount>,
    unpriced_tokens: u64,
}

impl From<AgentSummaryRecord> for AgentSummary {
    fn from(record: AgentSummaryRecord) -> Self {
        Self {
            id: record.id,
            label: record.label,
            path: record.path,
            nickname: record.nickname,
            status: record.status,
            turn_count: record.turn_count,
            tool_count: record.tool_count,
            total_tokens: record.total_tokens,
            cost_usd: record.cost_usd,
            unpriced_tokens: record.unpriced_tokens,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolSummary {
    tool: String,
    count: u64,
    failed_count: u64,
    total_duration_ms: u64,
}

impl From<ToolSummaryRecord> for ToolSummary {
    fn from(record: ToolSummaryRecord) -> Self {
        Self {
            tool: record.tool,
            count: record.count,
            failed_count: record.failed_count,
            total_duration_ms: record.total_duration_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionSummaryResponse {
    session: SessionDetail,
    totals: UsageTotals,
    models: Vec<ModelUsage>,
    agents: Vec<AgentSummary>,
    tool_summary: Vec<ToolSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionDetail {
    #[serde(flatten)]
    row: SessionRow,
    cwd: Option<String>,
    source: Option<String>,
    first_prompt: Option<String>,
    latest_result: Option<String>,
    completed_at: Option<String>,
    status: String,
}

impl From<SessionDetailRecord> for SessionDetail {
    fn from(record: SessionDetailRecord) -> Self {
        Self {
            row: record.row.into(),
            cwd: record.cwd,
            source: record.source,
            first_prompt: record.first_prompt,
            latest_result: record.latest_result,
            completed_at: record.completed_at,
            status: record.status,
        }
    }
}

impl From<SessionSummaryRecord> for SessionSummaryResponse {
    fn from(record: SessionSummaryRecord) -> Self {
        Self {
            session: record.session.into(),
            totals: record.totals,
            models: record.models.into_iter().map(Into::into).collect(),
            agents: record.agents.into_iter().map(Into::into).collect(),
            tool_summary: record.tool_summary.into_iter().map(Into::into).collect(),
        }
    }
}

async fn session_summary(
    State(state): State<SessionsState>,
    Path(id): Path<String>,
) -> ApiResult<Json<SessionSummaryResponse>> {
    state
        .reads
        .snapshot(WorkClass::Heavy, move |connection| {
            read_summary_on(connection, &id)
                .map(|summary| summary.map(SessionSummaryResponse::from))
        })
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("session not found"))
}

fn query_bounds(
    date: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> ApiResult<(Option<String>, Option<String>)> {
    if date.is_some() && (start.is_some() || end.is_some()) {
        return Err(ApiError::bad_request(
            "date cannot be combined with start or end",
        ));
    }
    if let Some(date) = date {
        let date = parse_date(date)?;
        return Ok((
            Some(canonical_utc_timestamp(local_midnight(date))),
            Some(canonical_utc_timestamp(local_midnight(
                date + Duration::days(1),
            ))),
        ));
    }
    let start = start
        .map(|value| parse_boundary(value, false))
        .transpose()?;
    let end = end.map(|value| parse_boundary(value, true)).transpose()?;
    if let (Some(start), Some(end)) = (&start, &end)
        && start >= end
    {
        return Err(ApiError::bad_request("start must be before end"));
    }
    Ok((start, end))
}

fn parse_boundary(value: &str, inclusive_date_end: bool) -> ApiResult<String> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        validate_public_year(timestamp.year())?;
        return Ok(canonical_utc_timestamp(timestamp.with_timezone(&Utc)));
    }
    let date = parse_date(value)?;
    let date = if inclusive_date_end {
        date + Duration::days(1)
    } else {
        date
    };
    Ok(canonical_utc_timestamp(local_midnight(date)))
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

#[cfg(test)]
mod tests {
    use super::query_bounds;
    use axum::http::StatusCode;

    #[test]
    fn date_filter_rejects_an_ambiguous_explicit_range() {
        let error =
            query_bounds(Some("2026-07-22"), Some("2026-07-01"), Some("2026-07-31")).unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.message(), "date cannot be combined with start or end");
    }
}
