use crate::{
    config::PricingConfig,
    db::Db,
    db_executor::{DbExecutor, WorkClass},
    fixed_price::PriceMicros,
    ingest::{IngestRoots, ScanReport},
    manual_pricing::{ManualAlias, ManualPrice, MutationError},
    model::Totals,
    pricing,
    redaction::redact_data_urls,
};
use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    body::Body as AxumBody,
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderName, HeaderValue, Method, Request, StatusCode, Uri,
        header::{HOST, ORIGIN},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, Months, NaiveDate, SecondsFormat, TimeZone,
    Utc,
};
use rusqlite::{
    Connection, InterruptHandle, OptionalExtension, Row, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct ApiState {
    pub db: Db,
    pub roots: IngestRoots,
    pub frontend: PathBuf,
    pub pricing: PricingConfig,
    pub executor: DbExecutor,
    pub pricing_sync: pricing::PricingSync,
}

impl ApiState {
    pub fn new(db: Db, roots: IngestRoots, frontend: PathBuf, pricing: PricingConfig) -> Self {
        Self::with_executor(db, roots, frontend, pricing, DbExecutor::default())
    }

    pub fn with_executor(
        db: Db,
        roots: IngestRoots,
        frontend: PathBuf,
        pricing: PricingConfig,
        executor: DbExecutor,
    ) -> Self {
        let pricing_sync = pricing::PricingSync::new(executor.clone());
        Self {
            db,
            roots,
            frontend,
            pricing,
            executor,
            pricing_sync,
        }
    }
}

pub fn router(state: ApiState) -> Router {
    let api = Router::new()
        .route("/status", get(status))
        .route("/overview", get(overview))
        .route("/overview/year", get(overview_year))
        .route("/projects", get(projects))
        .route("/sessions", get(sessions))
        .route("/sessions/{id}/summary", get(session_summary))
        .route("/sessions/{id}/activity", get(session_activity))
        .route(
            "/sessions/{id}/activity/{event_id}",
            get(session_activity_detail),
        )
        .route("/sessions/{id}/usage", get(session_usage))
        .route("/stats", get(stats))
        .route("/settings", get(settings))
        .route("/prices", get(prices))
        .route("/prices/refresh", post(refresh_prices))
        .route("/prices/{model_id}", put(put_price).delete(delete_price))
        .route(
            "/aliases/{observed_model_id}",
            put(put_alias).delete(delete_alias),
        )
        .fallback(api_not_found);
    let mut app = Router::new().nest("/api/v1", api);
    let index = state.frontend.join("index.html");
    if index.is_file() {
        app = app.fallback_service(
            ServeDir::new(state.frontend.clone()).fallback(ServeFile::new(index)),
        );
    } else {
        app = app.fallback(frontend_missing);
    }
    app.with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(browser_boundary))
}

async fn browser_boundary(request: Request<AxumBody>, next: Next) -> Response {
    let host = match request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
    {
        Some(host) if is_loopback_authority(host) => host.to_owned(),
        _ => return boundary_rejection("request host must be localhost or a loopback address"),
    };
    if is_mutating_method(request.method()) {
        if request
            .headers()
            .get(HeaderName::from_static("sec-fetch-site"))
            .and_then(|value| value.to_str().ok())
            .is_some_and(|site| !matches!(site, "same-origin" | "none"))
        {
            return boundary_rejection("cross-origin mutations are not allowed");
        }
        if let Some(origin) = request.headers().get(ORIGIN) {
            let allowed = origin
                .to_str()
                .ok()
                .and_then(|origin| Uri::from_str(origin).ok())
                .is_some_and(|origin| {
                    matches!(origin.scheme_str(), Some("http" | "https"))
                        && origin.authority().is_some_and(|authority| {
                            is_loopback_authority(authority.as_str())
                                && authority.as_str().eq_ignore_ascii_case(&host)
                        })
                });
            if !allowed {
                return boundary_rejection("mutation origin does not match the local application");
            }
        }
    }

    let mut response = next.run(request).await;
    let content_security_policy = response
        .headers()
        .get(HeaderName::from_static("content-security-policy"))
        .and_then(|value| value.to_str().ok())
        .map(|value| format!("{value}; frame-ancestors 'none'"))
        .unwrap_or_else(|| "frame-ancestors 'none'".to_owned());
    if let Ok(value) = HeaderValue::from_str(&content_security_policy) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("content-security-policy"), value);
    }
    response.headers_mut().insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    response
}

fn is_loopback_authority(value: &str) -> bool {
    let Ok(authority) = axum::http::uri::Authority::from_str(value) else {
        return false;
    };
    let host = authority.host().trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || IpAddr::from_str(host).is_ok_and(|address| address.is_loopback())
}

fn is_mutating_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn boundary_rejection(message: &'static str) -> Response {
    (StatusCode::FORBIDDEN, message).into_response()
}

pub async fn serve(state: ApiState, address: SocketAddr) -> Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind {address}"))?;
    tracing::info!(%address, "Codex Usage is ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn frontend_missing() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        "Frontend build not found. Run `npm run build` in frontend/.",
    )
}

async fn api_not_found() -> ApiError {
    ApiError::not_found("API route not found")
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "API request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: String,
        }
        (
            self.status,
            Json(Body {
                error: self.message,
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

async fn run_work<T, F>(state: &ApiState, class: WorkClass, work: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    state
        .executor
        .run(class, work)
        .await
        .map_err(ApiError::internal)
}

#[derive(Default)]
struct QueryCancellation {
    cancelled: AtomicBool,
    interrupt: Mutex<Option<InterruptHandle>>,
}

impl QueryCancellation {
    fn install(&self, connection: &Connection) -> Result<()> {
        let mut interrupt = self
            .interrupt
            .lock()
            .map_err(|_| anyhow!("database cancellation lock poisoned"))?;
        *interrupt = Some(connection.get_interrupt_handle());
        if self.cancelled.load(Ordering::Acquire) {
            if let Some(handle) = interrupt.as_ref() {
                handle.interrupt();
            }
            return Err(anyhow!("database query cancelled"));
        }
        Ok(())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(interrupt) = self.interrupt.lock()
            && let Some(handle) = interrupt.as_ref()
        {
            handle.interrupt();
        }
    }
}

struct CancelQueryOnDrop {
    cancellation: Arc<QueryCancellation>,
    armed: bool,
}

impl Drop for CancelQueryOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

async fn run_snapshot_work<T, F>(
    state: &ApiState,
    class: WorkClass,
    db: Db,
    read: F,
) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(&Connection) -> Result<T> + Send + 'static,
{
    let cancellation = Arc::new(QueryCancellation::default());
    let worker_cancellation = cancellation.clone();
    let mut cancel_on_drop = CancelQueryOnDrop {
        cancellation,
        armed: true,
    };
    let result = state
        .executor
        .run(class, move || {
            let connection = db.connect()?;
            worker_cancellation.install(&connection)?;
            let transaction =
                Transaction::new_unchecked(&connection, TransactionBehavior::Deferred)?;
            let value = read(&transaction)?;
            transaction.commit()?;
            Ok(value)
        })
        .await
        .map_err(ApiError::internal);
    cancel_on_drop.armed = false;
    result
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResponse {
    state: String,
    last_ingest_at: Option<String>,
    last_ingest_attempt_at: Option<String>,
    last_event_at: Option<String>,
    files_scanned: u64,
    files_failed: u64,
}

async fn status(State(state): State<ApiState>) -> ApiResult<Json<StatusResponse>> {
    let db = state.db.clone();
    Ok(Json(
        run_work(&state, WorkClass::Light, move || query_status(&db)).await?,
    ))
}

fn query_status(db: &Db) -> Result<StatusResponse> {
    let connection = db.connect()?;
    let meta = |key: &str| -> Result<Option<String>> {
        Ok(connection
            .query_row("SELECT value FROM app_meta WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?)
    };
    let (stored_files_scanned, stored_files_failed): (i64, i64) = connection.query_row(
        "SELECT COUNT(*),COALESCE(SUM(CASE WHEN last_error IS NULL THEN 0 ELSE 1 END),0)
             FROM source_files",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let last_report =
        meta("last_scan_report")?.and_then(|value| serde_json::from_str::<ScanReport>(&value).ok());
    let (files_scanned, files_failed) = last_report
        .map(|report| (report.files_seen, report.files_failed))
        .unwrap_or_else(|| {
            (
                stored_files_scanned.max(0) as u64,
                stored_files_failed.max(0) as u64,
            )
        });
    let last_event_at = connection
        .query_row("SELECT MAX(last_event_at) FROM threads", [], |row| {
            row.get(0)
        })
        .optional()?
        .flatten();
    Ok(StatusResponse {
        state: meta("ingest_state")?.unwrap_or_else(|| "idle".into()),
        last_ingest_at: meta("last_ingest_at")?,
        last_ingest_attempt_at: meta("last_ingest_attempt_at")?,
        last_event_at,
        files_scanned,
        files_failed,
    })
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRow {
    pub id: String,
    pub root_thread_id: String,
    pub started_at: String,
    pub last_event_at: String,
    pub title: String,
    pub project: String,
    pub branch: Option<String>,
    pub message_count: u64,
    pub turn_count: u64,
    pub agent_count: u64,
    pub tool_count: u64,
    pub total_tokens: u64,
    pub cost_usd: Option<f64>,
    pub unpriced_tokens: u64,
    pub lifetime_cost_usd: Option<f64>,
    pub lifetime_unpriced_tokens: u64,
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

#[derive(Debug, Serialize)]
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
    State(state): State<ApiState>,
    Query(query): Query<SessionsQuery>,
) -> ApiResult<Json<SessionsResponse>> {
    let (start, end) = query_bounds(
        query.date.as_deref(),
        query.start.as_deref(),
        query.end.as_deref(),
    )?;
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 200);
    let db = state.db.clone();
    let project = query.project;
    let search = query.q;
    let sort = query.sort.unwrap_or_else(|| "recent".into());
    let response = run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
        query_sessions_on(
            connection,
            start.as_deref(),
            end.as_deref(),
            project.as_deref(),
            search.as_deref(),
            &sort,
            page,
            page_size,
            true,
        )
    })
    .await?;
    Ok(Json(response))
}

#[derive(Debug, Serialize)]
struct ProjectsResponse {
    items: Vec<String>,
}

async fn projects(State(state): State<ApiState>) -> ApiResult<Json<ProjectsResponse>> {
    let db = state.db.clone();
    Ok(Json(ProjectsResponse {
        items: run_snapshot_work(&state, WorkClass::Heavy, db, list_projects_on).await?,
    }))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PeriodSummary {
    label: String,
    start: String,
    end: String,
    session_count: u64,
    message_count: u64,
    totals: Totals,
    delta_cost_usd: Option<f64>,
    delta_percent: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OverviewPeriods {
    today: PeriodSummary,
    week: PeriodSummary,
    month: PeriodSummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HeatmapDay {
    date: String,
    cost_usd: Option<f64>,
    session_count: u64,
    message_count: u64,
    total_tokens: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectDriver {
    project: String,
    cost_usd: Option<f64>,
    share: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PricingSummary {
    known_cost_usd: f64,
    unpriced_tokens: u64,
    complete: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OverviewResponse {
    updated_at: Option<String>,
    periods: OverviewPeriods,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OverviewYearResponse {
    year: i32,
    heatmap: Vec<HeatmapDay>,
    top_projects: Vec<ProjectDriver>,
    top_sessions: Vec<SessionRow>,
}

#[derive(Clone, Debug, Default)]
struct OverviewUsageAggregate {
    total_tokens: u64,
    known_cost_numerator: i128,
    unpriced_tokens: u64,
    last_timestamp: String,
}

impl OverviewUsageAggregate {
    fn add_aggregate(&mut self, other: &Self) {
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.known_cost_numerator = self
            .known_cost_numerator
            .saturating_add(other.known_cost_numerator);
        self.unpriced_tokens = self.unpriced_tokens.saturating_add(other.unpriced_tokens);
        if other.last_timestamp > self.last_timestamp {
            other.last_timestamp.clone_into(&mut self.last_timestamp);
        }
    }

    fn add_sums(
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

    fn cost_usd(&self) -> Option<f64> {
        (self.unpriced_tokens == 0).then_some(self.known_cost_usd())
    }

    fn known_cost_usd(&self) -> f64 {
        self.known_cost_numerator as f64 / 1_000_000_000_000.0
    }
}

#[derive(Debug)]
struct OverviewPrice {
    effective_from: String,
    effective_to: Option<String>,
    input_microusd_per_million: i64,
    cached_input_microusd_per_million: Option<i64>,
    output_microusd_per_million: i64,
}

type OverviewPriceAliases = HashMap<String, String>;
type OverviewPriceLedger = HashMap<String, Vec<OverviewPrice>>;
type OverviewPriceBook = (OverviewPriceAliases, OverviewPriceLedger);
type OverviewYearUsage = (
    Vec<OverviewUsageAggregate>,
    HashMap<String, OverviewUsageAggregate>,
    Vec<HashSet<String>>,
);

#[derive(Debug, Deserialize)]
struct OverviewYearQuery {
    year: Option<i32>,
}

async fn overview(State(state): State<ApiState>) -> ApiResult<Json<OverviewResponse>> {
    let db = state.db.clone();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, query_overview_on).await?,
    ))
}

fn query_overview_on(connection: &Connection) -> Result<OverviewResponse> {
    let today = Local::now().date_naive();
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
    let bounds = [
        (today_start, tomorrow, "Today"),
        (previous_day_start, today_start, "Previous day"),
        (week_start, tomorrow, "This week"),
        (previous_week_start, week_start, "Previous week"),
        (month_start, tomorrow, "This month"),
        (previous_month_start, month_start, "Previous month"),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (start, end, _))| SqlBucketBounds {
        ordinal,
        start_at: sql_timestamp(start),
        end_at: sql_timestamp(end),
    })
    .collect::<Vec<_>>();
    let usage = query_overview_summary_usage_on(connection, &bounds)?;
    let session_counts = query_overview_summary_sessions_on(connection, &bounds)?;
    let message_counts = query_overview_summary_messages_on(connection, &bounds)?;
    let today_summary = overview_period_summary(
        "Today",
        &bounds[0],
        usage[0].clone(),
        &usage[1],
        session_counts[0],
        message_counts[0],
    );
    let week_summary = overview_period_summary(
        "This week",
        &bounds[2],
        usage[2].clone(),
        &usage[3],
        session_counts[1],
        message_counts[1],
    );
    let month_summary = overview_period_summary(
        "This month",
        &bounds[4],
        usage[4].clone(),
        &usage[5],
        session_counts[2],
        message_counts[2],
    );
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
            today: today_summary,
            week: week_summary,
            month: month_summary,
        },
    })
}

async fn overview_year(
    State(state): State<ApiState>,
    Query(query): Query<OverviewYearQuery>,
) -> ApiResult<Json<OverviewYearResponse>> {
    let year = query.year.unwrap_or_else(|| Local::now().year());
    if !(1970..=9998).contains(&year) {
        return Err(ApiError::bad_request("year must be between 1970 and 9998"));
    }
    let start = sql_timestamp(local_midnight(
        NaiveDate::from_ymd_opt(year, 1, 1).ok_or_else(|| ApiError::bad_request("invalid year"))?,
    ));
    let end = sql_timestamp(local_midnight(
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .ok_or_else(|| ApiError::bad_request("invalid year"))?,
    ));
    let db = state.db.clone();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_overview_year_on(connection, year, &start, &end)
        })
        .await?,
    ))
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
    cost_usd: Option<f64>,
    unpriced_tokens: u64,
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
    cost_usd: Option<f64>,
    unpriced_tokens: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolSummary {
    tool: String,
    count: u64,
    failed_count: u64,
    total_duration_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionSummaryResponse {
    session: SessionDetail,
    totals: Totals,
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

async fn session_summary(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<SessionSummaryResponse>> {
    let db = state.db.clone();
    run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
        query_session_summary_on(connection, &id)
    })
    .await?
    .map(Json)
    .ok_or_else(|| ApiError::not_found("session not found"))
}

fn query_session_summary_on(
    connection: &Connection,
    id: &str,
) -> Result<Option<SessionSummaryResponse>> {
    let Some(row) = query_session_on(connection, id)? else {
        return Ok(None);
    };
    Ok(Some(SessionSummaryResponse {
        session: query_session_detail_on(connection, row)?,
        totals: query_totals_on(connection, None, None, Some(id))?,
        models: query_model_usage_on(connection, id)?,
        agents: query_agent_summary_on(connection, id)?,
        tool_summary: query_tool_summary_on(connection, id)?,
    }))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ActivityItem {
    id: String,
    turn_id: Option<String>,
    rollout_id: String,
    agent_run_id: Option<String>,
    agent_label: Option<String>,
    timestamp: String,
    kind: String,
    role: Option<String>,
    label: Option<String>,
    body: Option<String>,
    status: Option<String>,
    tool_name: Option<String>,
    duration_ms: Option<i64>,
    model: Option<String>,
    effort: Option<String>,
    has_details: bool,
    children: Vec<ActivityItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_page: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_page_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_has_more: Option<bool>,
    usage: Option<Totals>,
    counts: Option<ActivityCounts>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ActivityCounts {
    model_calls: u64,
    tool_calls: u64,
    agent_runs: u64,
    reviews: u64,
    follow_ups: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageQuery {
    page: Option<u64>,
    page_size: Option<u64>,
}

const DEFAULT_ACTIVITY_CHILD_PAGE_SIZE: u64 = 250;
const MAX_ACTIVITY_CHILD_PAGE_SIZE: u64 = 500;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityDetailQuery {
    child_page: Option<u64>,
    child_page_size: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ActivityResponse {
    items: Vec<ActivityItem>,
    days: Vec<ActivityDaySummary>,
    page: u64,
    page_size: u64,
    total: u64,
    total_pages: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ActivityDaySummary {
    date: String,
    duration_ms: u64,
    totals: Totals,
}

async fn session_activity(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> ApiResult<Json<ActivityResponse>> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(25).clamp(1, 100);
    let db = state.db.clone();
    run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
        if query_session_on(connection, &id)?.is_none() {
            return Ok(None);
        }
        query_activity_on(connection, &id, page, page_size).map(Some)
    })
    .await?
    .map(Json)
    .ok_or_else(|| ApiError::not_found("session not found"))
}

async fn session_activity_detail(
    State(state): State<ApiState>,
    AxumPath((id, event_id)): AxumPath<(String, String)>,
    Query(query): Query<ActivityDetailQuery>,
) -> ApiResult<Json<ActivityItem>> {
    let child_page = query.child_page.unwrap_or(1).max(1);
    let child_page_size = query
        .child_page_size
        .unwrap_or(DEFAULT_ACTIVITY_CHILD_PAGE_SIZE)
        .clamp(1, MAX_ACTIVITY_CHILD_PAGE_SIZE);
    let db = state.db.clone();
    run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
        if query_session_on(connection, &id)?.is_none() {
            return Ok(None);
        }
        query_activity_detail_page_on(connection, &id, &event_id, child_page, child_page_size)
            .map(Some)
    })
    .await?
    .ok_or_else(|| ApiError::not_found("session not found"))?
    .map(Json)
    .ok_or_else(|| ApiError::not_found("activity event not found"))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageBreakdown {
    id: String,
    label: String,
    model: Option<String>,
    agent_run_id: Option<String>,
    turn_id: Option<String>,
    effort: Option<String>,
    totals: Totals,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionUsageResponse {
    totals: Totals,
    by_model: Vec<ModelUsage>,
    by_agent: Vec<UsageBreakdown>,
    by_turn: Vec<UsageBreakdown>,
    pricing: PricingSummary,
}

async fn session_usage(
    State(state): State<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<SessionUsageResponse>> {
    let db = state.db.clone();
    run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
        query_session_usage_on(connection, &id)
    })
    .await?
    .map(Json)
    .ok_or_else(|| ApiError::not_found("session not found"))
}

fn query_session_usage_on(
    connection: &Connection,
    id: &str,
) -> Result<Option<SessionUsageResponse>> {
    if query_session_on(connection, id)?.is_none() {
        return Ok(None);
    }
    let totals = query_totals_on(connection, None, None, Some(id))?;
    Ok(Some(SessionUsageResponse {
        pricing: PricingSummary {
            known_cost_usd: totals.known_cost_usd,
            unpriced_tokens: totals.unpriced_tokens,
            complete: totals.pricing_complete,
        },
        totals,
        by_model: query_model_usage_on(connection, id)?,
        by_agent: query_agent_usage_on(connection, id)?,
        by_turn: query_turn_usage_on(connection, id)?,
    }))
}

#[derive(Debug, Deserialize)]
struct StatsQuery {
    range: Option<String>,
    anchor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsRow {
    period_start: String,
    period_end: String,
    label: String,
    session_count: u64,
    #[serde(flatten)]
    totals: Totals,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResponse {
    range: String,
    anchor: String,
    label: String,
    totals: Totals,
    rows: Vec<StatsRow>,
    trend: Vec<Option<f64>>,
}

async fn stats(
    State(state): State<ApiState>,
    Query(query): Query<StatsQuery>,
) -> ApiResult<Json<StatsResponse>> {
    let range = query.range.as_deref().unwrap_or("day").to_owned();
    if !matches!(range.as_str(), "day" | "week" | "month" | "year" | "all") {
        return Err(ApiError::bad_request(
            "range must be day, week, month, year, or all",
        ));
    }
    // All-time is data-derived and always ends in the current year. An anchor
    // has no semantic meaning for it; ignoring it also prevents a caller from
    // manufacturing thousands of empty future or past year buckets.
    let anchor = if range == "all" {
        Local::now().date_naive()
    } else {
        query
            .anchor
            .as_deref()
            .map(parse_date)
            .transpose()?
            .unwrap_or_else(|| Local::now().date_naive())
    };
    let display_anchor = if range == "week" {
        anchor - Duration::days(anchor.weekday().num_days_from_monday() as i64)
    } else {
        anchor
    };
    let db = state.db.clone();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_stats_on(connection, &range, display_anchor)
        })
        .await?,
    ))
}

fn query_stats_on(
    connection: &Connection,
    range: &str,
    display_anchor: NaiveDate,
) -> Result<StatsResponse> {
    let buckets = stats_buckets_on(connection, range, display_anchor)?;
    let aggregates = query_stats_bucket_aggregates_on(connection, &buckets)?;
    let totals = stats_totals_from_aggregates(&aggregates);
    let rows = buckets
        .into_iter()
        .zip(aggregates)
        .map(|((start, end, label), aggregate)| StatsRow {
            period_start: start.to_rfc3339(),
            period_end: end.to_rfc3339(),
            label,
            session_count: aggregate.session_count,
            totals: aggregate.totals,
        })
        .collect::<Vec<_>>();
    let label = match range {
        "day" => display_anchor.format("%b %-d, %Y").to_string(),
        "week" => format!("Week of {}", display_anchor.format("%b %-d, %Y")),
        "month" => display_anchor.format("%B %Y").to_string(),
        "year" => display_anchor.year().to_string(),
        _ => "All time".into(),
    };
    let trend = rows.iter().map(|row| row.totals.cost_usd).collect();
    Ok(StatsResponse {
        range: range.into(),
        anchor: display_anchor.to_string(),
        label,
        totals,
        rows,
        trend,
    })
}

fn stats_totals_from_aggregates(aggregates: &[StatsBucketAggregate]) -> Totals {
    let total_cost_numerator = aggregates.iter().fold(0i128, |total, row| {
        total.saturating_add(row.known_cost_numerator)
    });
    let mut totals = aggregates.iter().fold(Totals::default(), |mut total, row| {
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
    totals.known_cost_usd = total_cost_numerator as f64 / 1_000_000_000_000.0;
    totals.finish()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SettingsResponse {
    database_path: String,
    active_root: Option<String>,
    archive_root: Option<String>,
    timezone: String,
    last_ingest_at: Option<String>,
    session_count: u64,
    database_bytes: u64,
    pricing: PricingSummary,
}

async fn settings(State(state): State<ApiState>) -> ApiResult<Json<SettingsResponse>> {
    let db = state.db.clone();
    let database_path = db.path().display().to_string();
    let active_root = state
        .roots
        .active
        .as_ref()
        .map(|path| path.display().to_string());
    let archive_root = state
        .roots
        .archive
        .as_ref()
        .map(|path| path.display().to_string());
    let timezone = Local::now().format("%Z").to_string();
    let database_file = db.path().to_path_buf();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_settings_on(
                connection,
                database_path,
                active_root,
                archive_root,
                timezone,
                std::fs::metadata(database_file)
                    .map(|value| value.len())
                    .unwrap_or(0),
            )
        })
        .await?,
    ))
}

fn query_settings_on(
    connection: &Connection,
    database_path: String,
    active_root: Option<String>,
    archive_root: Option<String>,
    timezone: String,
    database_bytes: u64,
) -> Result<SettingsResponse> {
    let totals = query_totals_on(connection, None, None, None)?;
    let last_ingest_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='last_ingest_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let session_count: i64 = connection.query_row(
        "SELECT COUNT(DISTINCT thread_id) FROM (
                SELECT thread_id FROM events UNION SELECT thread_id FROM usage_facts
                UNION SELECT thread_id FROM messages)",
        [],
        |row| row.get(0),
    )?;
    Ok(SettingsResponse {
        database_path,
        active_root,
        archive_root,
        timezone,
        last_ingest_at,
        session_count: session_count.max(0) as u64,
        database_bytes,
        pricing: PricingSummary {
            known_cost_usd: totals.known_cost_usd,
            unpriced_tokens: totals.unpriced_tokens,
            complete: totals.pricing_complete,
        },
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PriceRow {
    model_id: String,
    effective_from: String,
    effective_to: Option<String>,
    input_per_million: String,
    cached_input_per_million: Option<String>,
    output_per_million: String,
    currency: String,
    source: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AliasRow {
    observed_model_id: String,
    canonical_model_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UnknownModelRow {
    model_id: String,
    usage_count: u64,
    total_tokens: u64,
    last_seen_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PricesQuery {
    q: Option<String>,
    page: Option<u64>,
    page_size: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PricesResponse {
    items: Vec<PriceRow>,
    aliases: Vec<AliasRow>,
    observed_unknown: Vec<UnknownModelRow>,
    page: u64,
    page_size: u64,
    total: u64,
    total_pages: u64,
    last_refresh_at: Option<String>,
    last_refresh_error_at: Option<String>,
    refresh_error_kind: Option<String>,
    refresh_error: Option<String>,
    source: Option<String>,
}

async fn prices(
    State(state): State<ApiState>,
    Query(query): Query<PricesQuery>,
) -> ApiResult<Json<PricesResponse>> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(25).clamp(1, 100);
    let db = state.db.clone();
    let search = query.q;
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_prices_on(connection, search.as_deref(), page, page_size)
        })
        .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DecimalInput {
    String(String),
    Number(Number),
}

impl DecimalInput {
    fn text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Number(value) => value.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriceInput {
    effective_from: Option<String>,
    effective_to: Option<String>,
    input_per_million: DecimalInput,
    cached_input_per_million: Option<DecimalInput>,
    output_per_million: DecimalInput,
    currency: Option<String>,
}

async fn put_price(
    State(state): State<ApiState>,
    AxumPath(model_id): AxumPath<String>,
    Json(input): Json<PriceInput>,
) -> ApiResult<StatusCode> {
    let model_id = model_id.trim();
    if model_id.is_empty() {
        return Err(ApiError::bad_request("model ID is required"));
    }
    if input.currency.as_deref().unwrap_or("USD") != "USD" {
        return Err(ApiError::bad_request("only USD prices are supported"));
    }
    let input_price = PriceMicros::from_per_million_text(&input.input_per_million.text())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let cached_price = input
        .cached_input_per_million
        .as_ref()
        .map(|value| PriceMicros::from_per_million_text(&value.text()))
        .transpose()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let output_price = PriceMicros::from_per_million_text(&input.output_per_million.text())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let effective_from = canonical_timestamp(
        input
            .effective_from
            .as_deref()
            .unwrap_or("1970-01-01T00:00:00.000000000Z"),
    )?;
    let effective_to = input
        .effective_to
        .as_deref()
        .map(canonical_timestamp)
        .transpose()?;
    let price = ManualPrice {
        model_id: model_id.to_string(),
        effective_from,
        effective_to,
        input_microusd_per_million: input_price.raw(),
        cached_input_microusd_per_million: cached_price.map(PriceMicros::raw),
        output_microusd_per_million: output_price.raw(),
    };
    let db = state.db.clone();
    run_manual_mutation(&state, move || db.manual_pricing().save_price(&db, price)).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeletePriceQuery {
    effective_from: Option<String>,
}

async fn delete_price(
    State(state): State<ApiState>,
    AxumPath(model_id): AxumPath<String>,
    Query(query): Query<DeletePriceQuery>,
) -> ApiResult<StatusCode> {
    let effective_from = query
        .effective_from
        .as_deref()
        .map(canonical_timestamp)
        .transpose()?;
    let model_id = model_id.trim().to_string();
    let db = state.db.clone();
    run_manual_mutation(&state, move || {
        db.manual_pricing()
            .delete_price(&db, &model_id, effective_from.as_deref())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AliasInput {
    canonical_model_id: String,
}

async fn put_alias(
    State(state): State<ApiState>,
    AxumPath(observed_model_id): AxumPath<String>,
    Json(input): Json<AliasInput>,
) -> ApiResult<StatusCode> {
    let alias = ManualAlias {
        observed_model_id,
        canonical_model_id: input.canonical_model_id,
    };
    let db = state.db.clone();
    run_manual_mutation(&state, move || db.manual_pricing().save_alias(&db, alias)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_alias(
    State(state): State<ApiState>,
    AxumPath(observed_model_id): AxumPath<String>,
) -> ApiResult<StatusCode> {
    let observed_model_id = observed_model_id.trim().to_string();
    let db = state.db.clone();
    run_manual_mutation(&state, move || {
        db.manual_pricing().delete_alias(&db, &observed_model_id)
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn run_manual_mutation<F>(state: &ApiState, mutation: F) -> ApiResult<()>
where
    F: FnOnce() -> std::result::Result<(), MutationError> + Send + 'static,
{
    state
        .executor
        .run(WorkClass::Light, move || Ok(mutation()))
        .await
        .map_err(ApiError::internal)?
        .map_err(manual_mutation_error)
}

fn manual_mutation_error(error: MutationError) -> ApiError {
    match error {
        MutationError::Validation(message) => ApiError::bad_request(message),
        MutationError::Storage(error) => ApiError::internal(error),
    }
}

#[derive(Debug, Serialize)]
struct RefreshResponse {
    updated: usize,
}

async fn refresh_prices(State(state): State<ApiState>) -> ApiResult<Json<RefreshResponse>> {
    let updated = state
        .pricing_sync
        .force_sync(&state.db, &state.pricing)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "price refresh failed");
            ApiError {
                status: StatusCode::BAD_GATEWAY,
                message: "Could not refresh prices; cached prices remain available.".into(),
            }
        })?;
    Ok(Json(RefreshResponse { updated }))
}

fn query_overview_top_sessions_on(
    connection: &Connection,
    start: &str,
    end: &str,
    ranked: &[(String, OverviewUsageAggregate)],
) -> Result<Vec<SessionRow>> {
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
                (SELECT COUNT(*) FROM agent_runs WHERE thread_id=?1 AND id<>thread_id),
                (SELECT COUNT(*) FROM tool_calls
                 WHERE thread_id=?1 AND started_at>=?2 AND started_at<?3),
                ?4,?5,?6,
                COALESCE(lifetime.cost_usd,0.0),
                COALESCE(lifetime.unpriced_tokens,0)
         FROM threads t
         LEFT JOIN (
             SELECT COALESCE(SUM(cost_usd),0.0) cost_usd,
                    COALESCE(SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END),0)
                        unpriced_tokens
             FROM priced_usage WHERE thread_id=?1
         ) lifetime ON 1=1
         WHERE t.id=?1",
    )?;
    for (thread_id, usage) in ranked {
        rows.push(statement.query_row(
            params![
                thread_id,
                start,
                end,
                usage.total_tokens.min(i64::MAX as u64) as i64,
                usage.known_cost_usd(),
                usage.unpriced_tokens.min(i64::MAX as u64) as i64
            ],
            session_from_row,
        )?);
    }
    Ok(rows)
}

fn populate_session_sort_costs_on(
    connection: &Connection,
    start: Option<&str>,
    end: Option<&str>,
    project: Option<&str>,
    q_pattern: Option<&str>,
) -> Result<()> {
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS session_sort_costs(
             thread_id TEXT PRIMARY KEY,
             total_tokens INTEGER NOT NULL,
             cost_usd REAL NOT NULL,
             unpriced_tokens INTEGER NOT NULL
         ) WITHOUT ROWID;
         DELETE FROM session_sort_costs;",
    )?;
    let (aliases, prices) = overview_prices_on(connection)?;
    let groups = {
        let mut statement = connection.prepare(
            "SELECT u.thread_id,u.model,
                    COALESCE(SUM(u.input_tokens-MIN(u.input_tokens,u.cached_input_tokens)),0),
                    COALESCE(SUM(MIN(u.input_tokens,u.cached_input_tokens)),0),
                    COALESCE(SUM(u.output_tokens),0),COALESCE(SUM(u.total_tokens),0),
                    MIN(u.timestamp),MAX(u.timestamp)
             FROM usage_facts u
             JOIN threads t ON t.id=u.thread_id
             WHERE (?1 IS NULL OR u.timestamp>=?1) AND (?2 IS NULL OR u.timestamp<?2)
               AND (?3 IS NULL OR t.project=?3)
               AND (?4 IS NULL OR lower(COALESCE(t.title,'')) LIKE ?4
                    OR lower(COALESCE(t.project,'')) LIKE ?4
                    OR lower(COALESCE(t.branch,'')) LIKE ?4
                    OR lower(t.id) LIKE ?4)
             GROUP BY u.thread_id,u.model",
        )?;
        statement
            .query_map(params![start, end, project, q_pattern], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?.max(0),
                    row.get::<_, i64>(3)?.max(0),
                    row.get::<_, i64>(4)?.max(0),
                    row.get::<_, i64>(5)?.max(0) as u64,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut aggregates = HashMap::<String, OverviewUsageAggregate>::new();
    for (
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
        let first_price = overview_price_for(&aliases, &prices, &model, &first_timestamp);
        let last_price = overview_price_for(&aliases, &prices, &model, &last_timestamp);
        let has_price_boundary = overview_group_has_price_boundary(
            &aliases,
            &prices,
            &model,
            &first_timestamp,
            &last_timestamp,
        );
        let (known_cost_numerator, unpriced_tokens) = match (first_price, last_price) {
            (Some((first_index, price)), Some((last_index, _)))
                if first_index == last_index && !has_price_boundary =>
            {
                (
                    overview_cost_for_price(
                        price,
                        uncached_input_tokens,
                        cached_input_tokens,
                        output_tokens,
                    ),
                    0,
                )
            }
            (None, None)
                if overview_group_has_no_price(
                    &aliases,
                    &prices,
                    &model,
                    &first_timestamp,
                    &last_timestamp,
                ) =>
            {
                (0, total_tokens)
            }
            _ => session_exceptional_group_cost_on(
                connection,
                &aliases,
                &prices,
                &thread_id,
                &model,
                &first_timestamp,
                &last_timestamp,
            )?,
        };
        aggregates.entry(thread_id).or_default().add_sums(
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
            &last_timestamp,
        );
    }
    let mut insert = connection.prepare(
        "INSERT INTO session_sort_costs(thread_id,total_tokens,cost_usd,unpriced_tokens)
         VALUES(?1,?2,?3,?4)",
    )?;
    for (thread_id, aggregate) in aggregates {
        insert.execute(params![
            thread_id,
            aggregate.total_tokens.min(i64::MAX as u64) as i64,
            aggregate.known_cost_usd(),
            aggregate.unpriced_tokens.min(i64::MAX as u64) as i64,
        ])?;
    }
    Ok(())
}

fn session_exceptional_group_cost_on(
    connection: &Connection,
    aliases: &HashMap<String, String>,
    prices: &HashMap<String, Vec<OverviewPrice>>,
    thread_id: &str,
    model: &str,
    first_timestamp: &str,
    last_timestamp: &str,
) -> Result<(i128, u64)> {
    let mut statement = connection.prepare(
        "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
         FROM usage_facts
         WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<=?4",
    )?;
    let mut rows = statement.query(params![thread_id, model, first_timestamp, last_timestamp])?;
    let mut cost_numerator = 0i128;
    let mut unpriced_tokens = 0u64;
    while let Some(row) = rows.next()? {
        let timestamp = row.get_ref(0)?.as_str()?;
        let input_tokens = row.get::<_, i64>(1)?.max(0);
        let cached_tokens = input_tokens.min(row.get::<_, i64>(2)?.max(0));
        let output_tokens = row.get::<_, i64>(3)?.max(0);
        let total_tokens = row.get::<_, i64>(4)?.max(0) as u64;
        if let Some((_, price)) = overview_price_for(aliases, prices, model, timestamp) {
            cost_numerator = cost_numerator.saturating_add(overview_cost_for_price(
                price,
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

#[allow(clippy::too_many_arguments)]
fn query_sessions_on(
    connection: &Connection,
    start: Option<&str>,
    end: Option<&str>,
    project: Option<&str>,
    q: Option<&str>,
    sort: &str,
    page: u64,
    page_size: u64,
    include_projects: bool,
) -> Result<SessionsResponse> {
    let project = project.filter(|value| !value.is_empty() && *value != "all");
    let q_pattern = q
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("%{}%", value.trim().to_lowercase()));
    let q_pattern = q_pattern.as_deref();
    let bounded = start.is_some() || end.is_some();
    let offset = page
        .saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64;

    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS session_candidates(
             thread_id TEXT PRIMARY KEY,last_activity TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE TEMP TABLE IF NOT EXISTS selected_sessions(
             id TEXT PRIMARY KEY,started_at TEXT NOT NULL,last_event_at TEXT NOT NULL,
             title TEXT NOT NULL,project TEXT NOT NULL,branch TEXT,
             total_tokens INTEGER,cost_usd REAL,unpriced_tokens INTEGER
         ) WITHOUT ROWID;
         DELETE FROM session_candidates;
         DELETE FROM selected_sessions;",
    )?;

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
        AND (?2 IS NULL OR lower(COALESCE(t.title,'')) LIKE ?2
             OR lower(COALESCE(t.project,'')) LIKE ?2
             OR lower(COALESCE(t.branch,'')) LIKE ?2
             OR lower(t.id) LIKE ?2)
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
        connection.query_row(&total_sql, params![project, q_pattern], |row| row.get(0))?;

    if sort == "cost" {
        populate_session_sort_costs_on(connection, start, end, project, q_pattern)?;
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
                COALESCE(u.total_tokens,0),COALESCE(u.cost_usd,0.0),
                COALESCE(u.unpriced_tokens,0)
            FROM {source} LEFT JOIN session_sort_costs u ON u.thread_id=t.id
            WHERE (?1 IS NULL OR t.project=?1)
              AND (?2 IS NULL OR lower(COALESCE(t.title,'')) LIKE ?2
                   OR lower(COALESCE(t.project,'')) LIKE ?2
                   OR lower(COALESCE(t.branch,'')) LIKE ?2
                   OR lower(t.id) LIKE ?2)
              {visibility}
            ORDER BY CASE WHEN COALESCE(u.unpriced_tokens,0)=0 THEN 0 ELSE 1 END,
                CASE WHEN COALESCE(u.unpriced_tokens,0)=0
                     THEN COALESCE(u.cost_usd,0.0) ELSE 0.0 END DESC,
                CASE WHEN COALESCE(u.unpriced_tokens,0)>0
                     THEN COALESCE(u.total_tokens,0) ELSE 0 END DESC,
                {last_event} DESC,t.id DESC
            LIMIT ?3 OFFSET ?4
            "#
        );
        connection.execute(&sql, params![project, q_pattern, page_size as i64, offset])?;
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
                total_tokens,cost_usd,unpriced_tokens
            )
            SELECT t.id,t.started_at,{last_event},COALESCE(t.title,'Untitled session'),
                COALESCE(t.project,'—'),t.branch,NULL,NULL,NULL
            FROM {source}
            WHERE (?1 IS NULL OR t.project=?1)
              AND (?2 IS NULL OR lower(COALESCE(t.title,'')) LIKE ?2
                   OR lower(COALESCE(t.project,'')) LIKE ?2
                   OR lower(COALESCE(t.branch,'')) LIKE ?2
                   OR lower(t.id) LIKE ?2)
              {visibility}
            ORDER BY {last_event} DESC,t.id DESC LIMIT ?3 OFFSET ?4
            "#
        );
        connection.execute(&sql, params![project, q_pattern, page_size as i64, offset])?;
    }

    // Everything below this point is bounded by the selected page. In the
    // default recent view, no corpus-wide event/message/tool aggregate is run.
    let order = if sort == "cost" {
        "CASE WHEN COALESCE(s.unpriced_tokens,u.unpriced_tokens,0)=0 THEN 0 ELSE 1 END,
         CASE WHEN COALESCE(s.unpriced_tokens,u.unpriced_tokens,0)=0
              THEN COALESCE(s.cost_usd,u.cost_usd,0.0) ELSE 0.0 END DESC,
         CASE WHEN COALESCE(s.unpriced_tokens,u.unpriced_tokens,0)>0
              THEN COALESCE(s.total_tokens,u.total_tokens,0) ELSE 0 END DESC,
         s.last_event_at DESC,s.id DESC"
    } else {
        "s.last_event_at DESC,s.id DESC"
    };
    let sql = format!(
        r#"
        WITH usage_page AS (
            SELECT p.thread_id,COALESCE(SUM(p.total_tokens),0) total_tokens,
                COALESCE(SUM(p.cost_usd),0.0) cost_usd,
                COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0) unpriced_tokens
            FROM selected_sessions s JOIN priced_usage p ON p.thread_id=s.id
            WHERE (?1 IS NULL OR p.timestamp>=?1) AND (?2 IS NULL OR p.timestamp<?2)
            GROUP BY p.thread_id
        ), usage_lifetime AS (
            SELECT p.thread_id,COALESCE(SUM(p.cost_usd),0.0) cost_usd,
                COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0) unpriced_tokens
            FROM selected_sessions s JOIN priced_usage p ON p.thread_id=s.id
            GROUP BY p.thread_id
        ), message_page AS (
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
            JOIN agent_runs a ON a.thread_id=s.id WHERE a.id<>a.thread_id
            GROUP BY a.thread_id
        )
        SELECT s.id,s.started_at,s.last_event_at,s.title,s.project,s.branch,
            COALESCE(m.value,0),COALESCE(t.value,0),COALESCE(a.value,0),COALESCE(tc.value,0),
            COALESCE(s.total_tokens,u.total_tokens,0),
            COALESCE(s.cost_usd,u.cost_usd,0.0),
            COALESCE(s.unpriced_tokens,u.unpriced_tokens,0),
            COALESCE(l.cost_usd,0.0),
            COALESCE(l.unpriced_tokens,0)
        FROM selected_sessions s
        LEFT JOIN usage_page u ON u.thread_id=s.id
        LEFT JOIN usage_lifetime l ON l.thread_id=s.id
        LEFT JOIN message_page m ON m.thread_id=s.id
        LEFT JOIN turn_page t ON t.thread_id=s.id
        LEFT JOIN tool_page tc ON tc.thread_id=s.id
        LEFT JOIN agent_page a ON a.thread_id=s.id
        ORDER BY {order}
        "#
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![start, end], session_from_row)?;
    let items = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    let projects = if include_projects {
        list_projects_on(connection)?
    } else {
        Vec::new()
    };
    let total = total.max(0) as u64;
    Ok(SessionsResponse {
        items,
        projects,
        page,
        page_size,
        total,
        total_pages: total.div_ceil(page_size),
    })
}

fn session_from_row(row: &Row<'_>) -> rusqlite::Result<SessionRow> {
    let id: String = row.get(0)?;
    let total_tokens = row.get::<_, i64>(10)?.max(0) as u64;
    let known_cost_usd = row.get::<_, f64>(11)?;
    let unpriced_tokens = row.get::<_, i64>(12)?.max(0) as u64;
    let lifetime_known_cost_usd = row.get::<_, f64>(13)?;
    let lifetime_unpriced_tokens = row.get::<_, i64>(14)?.max(0) as u64;
    Ok(SessionRow {
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
        cost_usd: (unpriced_tokens == 0).then_some(known_cost_usd),
        unpriced_tokens,
        lifetime_cost_usd: (lifetime_unpriced_tokens == 0).then_some(lifetime_known_cost_usd),
        lifetime_unpriced_tokens,
    })
}

fn query_session_on(connection: &Connection, id: &str) -> Result<Option<SessionRow>> {
    connection
        .query_row(
            "SELECT t.id,t.started_at,t.last_event_at,COALESCE(t.title,'Untitled session'),
                COALESCE(t.project,'—'),t.branch,
                (SELECT COUNT(*) FROM messages m WHERE m.thread_id=t.id),
                (SELECT COUNT(*) FROM turns tr WHERE tr.thread_id=t.id),
                (SELECT COUNT(*) FROM agent_runs a WHERE a.thread_id=t.id AND a.id<>a.thread_id),
                (SELECT COUNT(*) FROM tool_calls tc WHERE tc.thread_id=t.id),
                COALESCE((SELECT SUM(total_tokens) FROM priced_usage p WHERE p.thread_id=t.id),0),
                COALESCE((SELECT SUM(cost_usd) FROM priced_usage p WHERE p.thread_id=t.id),0.0),
                COALESCE((SELECT SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END)
                          FROM priced_usage p WHERE p.thread_id=t.id),0),
                COALESCE((SELECT SUM(cost_usd) FROM priced_usage p WHERE p.thread_id=t.id),0.0),
                COALESCE((SELECT SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END)
                          FROM priced_usage p WHERE p.thread_id=t.id),0)
         FROM threads t WHERE t.id=?1 AND (
            EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id))",
            [id],
            session_from_row,
        )
        .optional()
        .map_err(Into::into)
}

fn list_projects_on(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT DISTINCT project FROM threads t
         WHERE project IS NOT NULL AND project<>'' AND (
            EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id)
         ) ORDER BY project COLLATE NOCASE",
    )?;
    Ok(statement
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_session_detail_on(connection: &Connection, row: SessionRow) -> Result<SessionDetail> {
    let root_rollout_id = query_root_rollout_id(connection, &row.id)?;
    let (cwd, source): (Option<String>, Option<String>) = connection.query_row(
        "SELECT cwd,COALESCE(thread_source,source) FROM threads WHERE id=?1",
        [&row.id],
        |value| Ok((value.get(0)?, value.get(1)?)),
    )?;
    let first_prompt = {
        let mut statement = connection.prepare(
            "SELECT content FROM messages
             WHERE thread_id=?1 AND rollout_id=?2 AND role='user'
             ORDER BY timestamp,source_line",
        )?;
        let mut messages = statement.query(params![&row.id, &root_rollout_id])?;
        let mut prompt = None;
        while let Some(message) = messages.next()? {
            let content: String = message.get(0)?;
            if let Some(content) = first_prompt_for_display(&content) {
                prompt = Some(content);
                break;
            }
        }
        prompt
    };
    let latest_message = connection
        .query_row(
            "SELECT content FROM messages
             WHERE thread_id=?1 AND rollout_id=?2 AND role='assistant'
             ORDER BY timestamp DESC,source_line DESC LIMIT 1",
            params![&row.id, &root_rollout_id],
            |value| value.get(0),
        )
        .optional()?;
    let latest_result = match latest_message {
        Some(message) => Some(message),
        None => connection
            .query_row(
                "SELECT last_agent_message FROM turns
                 WHERE thread_id=?1 AND rollout_id=?2
                   AND last_agent_message IS NOT NULL
                   AND trim(last_agent_message)<>''
                 ORDER BY COALESCE(completed_at,started_at) DESC LIMIT 1",
                params![&row.id, &root_rollout_id],
                |value| value.get(0),
            )
            .optional()?,
    };
    let completed_at = connection
        .query_row(
            "SELECT MAX(completed_at) FROM turns WHERE thread_id=?1",
            [&row.id],
            |value| value.get(0),
        )
        .optional()?
        .flatten();
    let status = connection
        .query_row(
            "SELECT status FROM agent_runs WHERE id=?1",
            [&row.id],
            |value| value.get(0),
        )
        .optional()?
        .unwrap_or_else(|| {
            if completed_at.is_some() {
                "completed".to_owned()
            } else {
                "running".to_owned()
            }
        });
    Ok(SessionDetail {
        row,
        cwd,
        source,
        first_prompt,
        latest_result,
        completed_at,
        status,
    })
}

const USER_REQUEST_MARKER: &str = "## My request for Codex:";

fn first_prompt_for_display(content: &str) -> Option<String> {
    let trimmed = content.trim_start();
    if trimmed.starts_with("<codex_internal_context") {
        let opening_tag = trimmed
            .split_once('>')
            .map(|(tag, _)| tag)
            .unwrap_or(trimmed);
        if opening_tag.contains("source=\"goal\"") {
            return Some("Automatic goal continuation".into());
        }
        return None;
    }
    let mut offset = 0;
    let mut request_start = None;
    for line in content.split_inclusive('\n') {
        if line.trim() == USER_REQUEST_MARKER {
            request_start = Some(offset + line.len());
        }
        offset += line.len();
    }

    if let Some(request_start) = request_start
        && let Some(request) = clean_user_message(content[request_start..].trim())
    {
        return Some(request);
    }

    if content.trim_start().starts_with("# Browser comments:")
        && let Some(comments) = browser_comments_for_display(content)
    {
        return Some(comments);
    }
    if content.trim_start().starts_with("# Response annotations:")
        && let Some(annotations) = response_annotations_for_display(content)
    {
        return Some(annotations);
    }

    let content = strip_leading_context_blocks(content);
    let content = content.trim();
    if content.is_empty()
        || content.starts_with("<recommended_plugins>")
        || content.starts_with("# AGENTS.md instructions")
        || content.starts_with("<environment_context>")
        || content.starts_with("# Applications mentioned by the user:")
        || content.starts_with("# Browser comments:")
        || content.starts_with("# Response annotations:")
        || content.starts_with("<in-app-browser-context")
    {
        return None;
    }

    clean_user_message(content)
}

fn clean_user_message(content: &str) -> Option<String> {
    let content = content.trim();
    if content.is_empty()
        || content.starts_with("The next image is untrusted page evidence")
        || content.starts_with("![")
        || content.starts_with("<appshot")
    {
        return None;
    }
    Some(content.to_owned())
}

fn browser_comments_for_display(content: &str) -> Option<String> {
    let marker = "\nComment:\n";
    let mut remainder = content;
    let mut comments = Vec::new();
    while let Some(index) = remainder.find(marker) {
        remainder = &remainder[index + marker.len()..];
        let end = ["\n\n## ", "\n\n<in-app", "\n\nThe next image"]
            .iter()
            .filter_map(|boundary| remainder.find(boundary))
            .min()
            .unwrap_or(remainder.len());
        if let Some(comment) = clean_user_message(&remainder[..end]) {
            comments.push(comment);
        }
        remainder = &remainder[end..];
    }
    (!comments.is_empty()).then(|| comments.join("\n\n"))
}

fn response_annotations_for_display(content: &str) -> Option<String> {
    let start_tag = "<response-annotations>";
    let end_tag = "</response-annotations>";
    let start = content.find(start_tag)? + start_tag.len();
    let end = content[start..].find(end_tag)? + start;
    let rows = serde_json::from_str::<Vec<serde_json::Value>>(content[start..end].trim()).ok()?;
    let annotations = rows
        .iter()
        .filter_map(|row| row.get("annotation").and_then(|value| value.as_str()))
        .filter_map(clean_user_message)
        .collect::<Vec<_>>();
    (!annotations.is_empty()).then(|| annotations.join("\n\n"))
}

fn strip_leading_context_blocks(content: &str) -> &str {
    let mut content = content.trim_start();
    for closing_tag in [
        "</recommended_plugins>",
        "</in-app-browser-context>",
        "</environment_context>",
    ] {
        if content.starts_with('<')
            && let Some(end) = content.find(closing_tag)
        {
            content = content[end + closing_tag.len()..].trim_start();
        }
    }
    content
}

fn query_totals_on(
    connection: &Connection,
    start: Option<&str>,
    end: Option<&str>,
    thread_id: Option<&str>,
) -> Result<Totals> {
    let mut sql = String::from(
        "SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(cached_input_tokens),0),
                COALESCE(SUM(output_tokens),0),COALESCE(SUM(reasoning_tokens),0),
                COALESCE(SUM(total_tokens),0),COALESCE(SUM(cost_usd),0.0),
                COALESCE(SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END),0)
         FROM priced_usage",
    );
    let mut predicates = Vec::new();
    let mut values = Vec::new();
    if let Some(start) = start {
        values.push(start);
        predicates.push(format!("timestamp>=?{}", values.len()));
    }
    if let Some(end) = end {
        values.push(end);
        predicates.push(format!("timestamp<?{}", values.len()));
    }
    if let Some(thread_id) = thread_id {
        values.push(thread_id);
        predicates.push(format!("thread_id=?{}", values.len()));
    }
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    Ok(connection.query_row(&sql, params_from_iter(values), totals_from_row)?)
}

fn totals_from_row(row: &Row<'_>) -> rusqlite::Result<Totals> {
    Ok(Totals {
        input_tokens: row.get::<_, i64>(0)?.max(0) as u64,
        cached_input_tokens: row.get::<_, i64>(1)?.max(0) as u64,
        output_tokens: row.get::<_, i64>(2)?.max(0) as u64,
        reasoning_tokens: row.get::<_, i64>(3)?.max(0) as u64,
        total_tokens: row.get::<_, i64>(4)?.max(0) as u64,
        known_cost_usd: row.get(5)?,
        unpriced_tokens: row.get::<_, i64>(6)?.max(0) as u64,
        ..Totals::default()
    }
    .finish())
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

fn overview_summary_bounds_json(bounds: &[SqlBucketBounds]) -> Result<String> {
    anyhow::ensure!(
        bounds.len() == 6,
        "overview summary requires six period bounds"
    );
    Ok(serde_json::to_string(bounds)?)
}

fn query_overview_summary_usage_on(
    connection: &Connection,
    bounds: &[SqlBucketBounds],
) -> Result<Vec<Totals>> {
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
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals = vec![Totals::default(); bounds.len()];
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
        let price = overview_price_for(&aliases, &prices, model, timestamp).map(|(_, price)| price);
        let row_cost = price.map(|price| {
            overview_cost_for_price(
                price,
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
        aggregate.known_cost_usd = cost_numerator as f64 / 1_000_000_000_000.0;
        *aggregate = std::mem::take(aggregate).finish();
    }
    Ok(totals)
}

fn query_overview_summary_sessions_on(
    connection: &Connection,
    bounds: &[SqlBucketBounds],
) -> Result<[u64; 3]> {
    let bounds = overview_summary_bounds_json(bounds)?;
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

fn query_overview_summary_messages_on(
    connection: &Connection,
    bounds: &[SqlBucketBounds],
) -> Result<[u64; 3]> {
    let bounds = overview_summary_bounds_json(bounds)?;
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

fn overview_period_summary(
    label: &str,
    bounds: &SqlBucketBounds,
    totals: Totals,
    previous: &Totals,
    session_count: u64,
    message_count: u64,
) -> PeriodSummary {
    let delta_cost_usd = totals
        .cost_usd
        .zip(previous.cost_usd)
        .map(|(current, prior)| current - prior);
    let delta_percent = totals
        .cost_usd
        .zip(previous.cost_usd)
        .and_then(|(current, prior)| (prior > 0.0).then_some((current - prior) / prior * 100.0));
    PeriodSummary {
        label: label.into(),
        start: bounds.start_at.clone(),
        end: bounds.end_at.clone(),
        session_count,
        message_count,
        totals,
        delta_cost_usd,
        delta_percent,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlBucketBounds {
    ordinal: usize,
    start_at: String,
    end_at: String,
}

#[cfg(test)]
struct BucketAggregate {
    totals: Totals,
    session_count: u64,
    message_count: u64,
}

struct StatsBucketAggregate {
    totals: Totals,
    session_count: u64,
    known_cost_numerator: i128,
}

#[cfg(test)]
const BUCKET_AGGREGATES_SQL: &str = "WITH bounds AS MATERIALIZED (
            SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) ordinal,
                   json_extract(value,'$.startAt') start_at,
                   json_extract(value,'$.endAt') end_at
            FROM json_each(?1)
         ), usage_by_bucket AS (
            SELECT b.ordinal,
                   COALESCE(SUM(p.input_tokens),0) input_tokens,
                   COALESCE(SUM(p.cached_input_tokens),0) cached_input_tokens,
                   COALESCE(SUM(p.output_tokens),0) output_tokens,
                   COALESCE(SUM(p.reasoning_tokens),0) reasoning_tokens,
                   COALESCE(SUM(p.total_tokens),0) total_tokens,
                   COALESCE(SUM(p.cost_usd),0.0) known_cost_usd,
                   COALESCE(SUM(CASE WHEN p.price_known=0
                                     THEN p.total_tokens ELSE 0 END),0) unpriced_tokens
            FROM bounds b
            JOIN priced_usage p
              ON p.timestamp>=b.start_at AND p.timestamp<b.end_at
            GROUP BY b.ordinal
         ), messages_by_bucket AS (
            SELECT b.ordinal,COUNT(*) message_count
            FROM bounds b
            JOIN messages m
              ON m.timestamp>=b.start_at AND m.timestamp<b.end_at
            GROUP BY b.ordinal
         ), session_pairs AS (
            SELECT b.ordinal,e.thread_id
            FROM bounds b JOIN events e
              ON e.timestamp>=b.start_at AND e.timestamp<b.end_at
            UNION
            SELECT b.ordinal,u.thread_id
            FROM bounds b JOIN usage_facts u
              ON u.timestamp>=b.start_at AND u.timestamp<b.end_at
            UNION
            SELECT b.ordinal,m.thread_id
            FROM bounds b JOIN messages m
              ON m.timestamp>=b.start_at AND m.timestamp<b.end_at
         ), sessions_by_bucket AS (
            SELECT ordinal,COUNT(*) session_count
            FROM session_pairs GROUP BY ordinal
         )
         SELECT b.ordinal,
                COALESCE(u.input_tokens,0),
                COALESCE(u.cached_input_tokens,0),
                COALESCE(u.output_tokens,0),
                COALESCE(u.reasoning_tokens,0),
                COALESCE(u.total_tokens,0),
                COALESCE(u.known_cost_usd,0.0),
                COALESCE(u.unpriced_tokens,0),
                COALESCE(m.message_count,0),
                COALESCE(s.session_count,0)
         FROM bounds b
         LEFT JOIN usage_by_bucket u ON u.ordinal=b.ordinal
         LEFT JOIN messages_by_bucket m ON m.ordinal=b.ordinal
         LEFT JOIN sessions_by_bucket s ON s.ordinal=b.ordinal
         ORDER BY b.ordinal";

#[cfg(test)]
fn query_bucket_aggregates_on(
    connection: &Connection,
    buckets: &[StatsBucket],
) -> Result<Vec<BucketAggregate>> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    let bounds = buckets
        .iter()
        .enumerate()
        .map(|(ordinal, (start, end, _))| SqlBucketBounds {
            ordinal,
            start_at: sql_timestamp(*start),
            end_at: sql_timestamp(*end),
        })
        .collect::<Vec<_>>();
    let bounds_json = serde_json::to_string(&bounds)?;
    let mut statement = connection.prepare(BUCKET_AGGREGATES_SQL)?;
    let rows = statement
        .query_map([bounds_json], |row| {
            Ok((
                row.get::<_, i64>(0)?.max(0) as usize,
                BucketAggregate {
                    totals: Totals {
                        input_tokens: row.get::<_, i64>(1)?.max(0) as u64,
                        cached_input_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                        output_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                        reasoning_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                        total_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                        known_cost_usd: row.get(6)?,
                        unpriced_tokens: row.get::<_, i64>(7)?.max(0) as u64,
                        ..Totals::default()
                    }
                    .finish(),
                    message_count: row.get::<_, i64>(8)?.max(0) as u64,
                    session_count: row.get::<_, i64>(9)?.max(0) as u64,
                },
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if rows.len() != buckets.len()
        || rows
            .iter()
            .enumerate()
            .any(|(expected, (ordinal, _))| expected != *ordinal)
    {
        anyhow::bail!("bucket aggregate query returned incomplete bounds");
    }
    Ok(rows.into_iter().map(|(_, aggregate)| aggregate).collect())
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
            .all(|(start, end, _)| end.signed_duration_since(*start) >= Duration::days(20))
}

fn stats_exceptional_group_cost_on(
    connection: &Connection,
    aliases: &HashMap<String, String>,
    prices: &HashMap<String, Vec<OverviewPrice>>,
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
        if let Some((_, price)) = overview_price_for(aliases, prices, model, timestamp) {
            cost_numerator = cost_numerator.saturating_add(overview_cost_for_price(
                price,
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
        .map(|(ordinal, (start, end, _))| SqlBucketBounds {
            ordinal,
            start_at: sql_timestamp(*start),
            end_at: sql_timestamp(*end),
        })
        .collect::<Vec<_>>();
    let bounds_json = serde_json::to_string(&bounds)?;
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut aggregates = (0..buckets.len())
        .map(|_| StatsBucketAggregate {
            totals: Totals::default(),
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
        let first_price = overview_price_for(&aliases, &prices, &model, &first_timestamp);
        let last_price = overview_price_for(&aliases, &prices, &model, &last_timestamp);
        let has_price_boundary = overview_group_has_price_boundary(
            &aliases,
            &prices,
            &model,
            &first_timestamp,
            &last_timestamp,
        );
        let (known_cost_numerator, unpriced_tokens) = match (first_price, last_price) {
            (Some((first_index, price)), Some((last_index, _)))
                if first_index == last_index && !has_price_boundary =>
            {
                (
                    overview_cost_for_price(
                        price,
                        uncached_billed_tokens,
                        cached_billed_tokens,
                        output_tokens.min(i64::MAX as u64) as i64,
                    ),
                    0,
                )
            }
            (None, None)
                if overview_group_has_no_price(
                    &aliases,
                    &prices,
                    &model,
                    &first_timestamp,
                    &last_timestamp,
                ) =>
            {
                (0, total_tokens)
            }
            _ => stats_exceptional_group_cost_on(
                connection,
                &aliases,
                &prices,
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
        aggregate.totals.known_cost_usd =
            aggregate.known_cost_numerator as f64 / 1_000_000_000_000.0;
        aggregate.totals = std::mem::take(&mut aggregate.totals).finish();
    }
    Ok(aggregates)
}

// Overview needs all three annual panels at once. The general priced_usage view
// performs a correlated price lookup for every fact, which dominates cold-page
// latency at annual scale. Collapse the raw facts to day/session/model groups,
// then apply the tiny fixed-point ledger once per group. If a historical price
// boundary falls inside a group, the narrow fallback below prices only that
// exceptional group row by row.
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

fn overview_year_buckets(year: i32) -> Result<Vec<StatsBucket>> {
    let mut buckets = Vec::new();
    let mut date = NaiveDate::from_ymd_opt(year, 1, 1).context("invalid year")?;
    let limit = NaiveDate::from_ymd_opt(year + 1, 1, 1).context("invalid year")?;
    while date < limit {
        let next_date = date + Duration::days(1);
        buckets.push((
            local_midnight(date),
            local_midnight(next_date),
            date.to_string(),
        ));
        date = next_date;
    }
    Ok(buckets)
}

fn overview_prices_on(connection: &Connection) -> Result<OverviewPriceBook> {
    let mut alias_statement = connection
        .prepare("SELECT observed_model_id,canonical_model_id FROM resolved_model_aliases")?;
    let aliases = alias_statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()?;

    let mut price_statement = connection.prepare(
        "SELECT model_id,effective_from,effective_to,
                input_microusd_per_million,cached_input_microusd_per_million,
                output_microusd_per_million
         FROM resolved_model_prices
         ORDER BY model_id,source_priority,effective_from,source",
    )?;
    let mut prices = HashMap::<String, Vec<OverviewPrice>>::new();
    let mut rows = price_statement.query([])?;
    while let Some(row) = rows.next()? {
        prices
            .entry(row.get::<_, String>(0)?)
            .or_default()
            .push(OverviewPrice {
                effective_from: row.get(1)?,
                effective_to: row.get(2)?,
                input_microusd_per_million: row.get(3)?,
                cached_input_microusd_per_million: row.get(4)?,
                output_microusd_per_million: row.get(5)?,
            });
    }
    Ok((aliases, prices))
}

fn overview_price_for<'a>(
    aliases: &HashMap<String, String>,
    prices: &'a HashMap<String, Vec<OverviewPrice>>,
    model: &str,
    timestamp: &str,
) -> Option<(usize, &'a OverviewPrice)> {
    let priced_model = aliases.get(model).map(String::as_str).unwrap_or(model);
    prices
        .get(priced_model)?
        .iter()
        .enumerate()
        .rev()
        .find(|(_, price)| {
            price.effective_from.as_str() <= timestamp
                && price
                    .effective_to
                    .as_deref()
                    .is_none_or(|effective_to| effective_to > timestamp)
        })
}

fn overview_cost_for_price(
    price: &OverviewPrice,
    uncached_input_tokens: i64,
    cached_input_tokens: i64,
    output_tokens: i64,
) -> i128 {
    i128::from(uncached_input_tokens)
        .saturating_mul(i128::from(price.input_microusd_per_million))
        .saturating_add(
            i128::from(cached_input_tokens).saturating_mul(i128::from(
                price
                    .cached_input_microusd_per_million
                    .unwrap_or(price.input_microusd_per_million),
            )),
        )
        .saturating_add(
            i128::from(output_tokens).saturating_mul(i128::from(price.output_microusd_per_million)),
        )
}

fn overview_group_has_no_price(
    aliases: &HashMap<String, String>,
    prices: &HashMap<String, Vec<OverviewPrice>>,
    model: &str,
    first_timestamp: &str,
    last_timestamp: &str,
) -> bool {
    let priced_model = aliases.get(model).map(String::as_str).unwrap_or(model);
    prices.get(priced_model).is_none_or(|model_prices| {
        model_prices.iter().all(|price| {
            price.effective_from.as_str() > last_timestamp
                || price
                    .effective_to
                    .as_deref()
                    .is_some_and(|effective_to| effective_to <= first_timestamp)
        })
    })
}

fn overview_group_has_price_boundary(
    aliases: &HashMap<String, String>,
    prices: &HashMap<String, Vec<OverviewPrice>>,
    model: &str,
    first_timestamp: &str,
    last_timestamp: &str,
) -> bool {
    let priced_model = aliases.get(model).map(String::as_str).unwrap_or(model);
    prices.get(priced_model).is_some_and(|model_prices| {
        model_prices.iter().any(|price| {
            (price.effective_from.as_str() > first_timestamp
                && price.effective_from.as_str() <= last_timestamp)
                || price.effective_to.as_deref().is_some_and(|effective_to| {
                    effective_to > first_timestamp && effective_to <= last_timestamp
                })
        })
    })
}

fn overview_exceptional_group_cost_on(
    connection: &Connection,
    aliases: &HashMap<String, String>,
    prices: &HashMap<String, Vec<OverviewPrice>>,
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
        if let Some((_, price)) = overview_price_for(aliases, prices, model, timestamp) {
            cost_numerator = cost_numerator.saturating_add(overview_cost_for_price(
                price,
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

fn query_overview_year_usage_on(
    connection: &Connection,
    buckets: &[StatsBucket],
) -> Result<OverviewYearUsage> {
    let bounds = buckets
        .iter()
        .enumerate()
        .map(|(ordinal, (start, end, _))| SqlBucketBounds {
            ordinal,
            start_at: sql_timestamp(*start),
            end_at: sql_timestamp(*end),
        })
        .collect::<Vec<_>>();
    let bounds_json = serde_json::to_string(&bounds)?;
    let (aliases, prices) = overview_prices_on(connection)?;
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
        let first_price = overview_price_for(&aliases, &prices, &model, &first_timestamp);
        let last_price = overview_price_for(&aliases, &prices, &model, &last_timestamp);
        let has_price_boundary = overview_group_has_price_boundary(
            &aliases,
            &prices,
            &model,
            &first_timestamp,
            &last_timestamp,
        );
        let (known_cost_numerator, unpriced_tokens) = match (first_price, last_price) {
            (Some((first_index, price)), Some((last_index, _)))
                if first_index == last_index && !has_price_boundary =>
            {
                (
                    overview_cost_for_price(
                        price,
                        uncached_input_tokens,
                        cached_input_tokens,
                        output_tokens,
                    ),
                    0,
                )
            }
            (None, None)
                if overview_group_has_no_price(
                    &aliases,
                    &prices,
                    &model,
                    &first_timestamp,
                    &last_timestamp,
                ) =>
            {
                (0, total_tokens)
            }
            _ => overview_exceptional_group_cost_on(
                connection,
                &aliases,
                &prices,
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

        if let Some(aggregate) = sessions.get_mut(&thread_id) {
            aggregate.add_sums(
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
                &last_timestamp,
            );
        } else {
            let mut aggregate = OverviewUsageAggregate::default();
            aggregate.add_sums(
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
                &last_timestamp,
            );
            sessions.insert(thread_id, aggregate);
        }
    }
    Ok((daily, sessions, activity_sessions))
}

fn query_overview_year_activity_on(
    connection: &Connection,
    buckets: &[StatsBucket],
    activity_sessions: &mut [HashSet<String>],
) -> Result<Vec<u64>> {
    let bounds = buckets
        .iter()
        .enumerate()
        .map(|(ordinal, (start, end, _))| SqlBucketBounds {
            ordinal,
            start_at: sql_timestamp(*start),
            end_at: sql_timestamp(*end),
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

    // Events can outnumber sessions by orders of magnitude. For each thread,
    // seek directly to its first event in the year, record that day, then jump
    // to the next day boundary. The thread/time index makes work proportional
    // to active session-days rather than every tool and streaming event.
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
    let mut statement = connection.prepare(
        "SELECT timestamp FROM events
         WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3
         ORDER BY timestamp LIMIT 1",
    )?;
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

fn query_overview_year_projects_on(
    connection: &Connection,
    sessions: &HashMap<String, OverviewUsageAggregate>,
) -> Result<Vec<ProjectDriver>> {
    let mut statement = connection.prepare("SELECT id,COALESCE(project,'—') FROM threads")?;
    let projects = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()?;
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
            std::cmp::Ordering::Equal
        };
        price_order
            .then(value_order)
            .then_with(|| left_project.cmp(right_project))
    });
    Ok(ranked
        .into_iter()
        .take(3)
        .map(|(project, usage)| ProjectDriver {
            project,
            cost_usd: usage.cost_usd(),
            share: usage.cost_usd().map(|known_cost_usd| {
                if total_priced_cost_numerator > 0 {
                    known_cost_usd / (total_priced_cost_numerator as f64 / 1_000_000_000_000.0)
                } else {
                    0.0
                }
            }),
        })
        .collect())
}

fn rank_overview_year_sessions(
    sessions: &HashMap<String, OverviewUsageAggregate>,
) -> Vec<(String, OverviewUsageAggregate)> {
    let mut ranked = sessions.iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_id, left), (right_id, right)| {
        let price_order = (left.unpriced_tokens > 0).cmp(&(right.unpriced_tokens > 0));
        let value_order = if left.unpriced_tokens == 0 && right.unpriced_tokens == 0 {
            right.known_cost_numerator.cmp(&left.known_cost_numerator)
        } else if left.unpriced_tokens > 0 && right.unpriced_tokens > 0 {
            right.total_tokens.cmp(&left.total_tokens)
        } else {
            std::cmp::Ordering::Equal
        };
        price_order
            .then(value_order)
            .then_with(|| right.last_timestamp.cmp(&left.last_timestamp))
            .then_with(|| right_id.cmp(left_id))
    });
    ranked
        .into_iter()
        .take(3)
        .map(|(thread_id, usage)| (thread_id.clone(), usage.clone()))
        .collect()
}

fn query_overview_year_on(
    connection: &Connection,
    year: i32,
    start: &str,
    end: &str,
) -> Result<OverviewYearResponse> {
    let buckets = overview_year_buckets(year)?;
    let (daily_usage, session_usage, mut activity_sessions) =
        query_overview_year_usage_on(connection, &buckets)?;
    let message_counts =
        query_overview_year_activity_on(connection, &buckets, &mut activity_sessions)?;
    let heatmap = buckets
        .iter()
        .zip(daily_usage)
        .zip(message_counts)
        .zip(activity_sessions)
        .map(
            |((((.., date), usage), message_count), sessions)| HeatmapDay {
                date: date.clone(),
                cost_usd: usage.cost_usd(),
                session_count: sessions.len() as u64,
                message_count,
                total_tokens: usage.total_tokens,
            },
        )
        .collect();
    let top_projects = query_overview_year_projects_on(connection, &session_usage)?;
    let ranked_sessions = rank_overview_year_sessions(&session_usage);
    let top_sessions = query_overview_top_sessions_on(connection, start, end, &ranked_sessions)?;
    Ok(OverviewYearResponse {
        year,
        heatmap,
        top_projects,
        top_sessions,
    })
}

#[cfg(test)]
fn query_heatmap_on(connection: &Connection, year: i32) -> Result<Vec<HeatmapDay>> {
    let buckets = overview_year_buckets(year)?;
    Ok(buckets
        .iter()
        .zip(query_bucket_aggregates_on(connection, &buckets)?)
        .map(|((_, _, date), aggregate)| HeatmapDay {
            date: date.clone(),
            cost_usd: aggregate.totals.cost_usd,
            session_count: aggregate.session_count,
            message_count: aggregate.message_count,
            total_tokens: aggregate.totals.total_tokens,
        })
        .collect())
}

fn query_model_usage_on(connection: &Connection, thread_id: &str) -> Result<Vec<ModelUsage>> {
    let mut statement = connection.prepare(
        "SELECT model,effort,COALESCE(SUM(input_tokens),0),COALESCE(SUM(cached_input_tokens),0),
                COALESCE(SUM(output_tokens),0),COALESCE(SUM(reasoning_tokens),0),
                COALESCE(SUM(total_tokens),0),COALESCE(SUM(cost_usd),0.0),
                COALESCE(SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END),0)
         FROM priced_usage WHERE thread_id=?1 GROUP BY model,effort ORDER BY 7 DESC",
    )?;
    Ok(statement
        .query_map([thread_id], |row| {
            let total_tokens = row.get::<_, i64>(6)?.max(0) as u64;
            let known_cost_usd = row.get::<_, f64>(7)?;
            let unpriced_tokens = row.get::<_, i64>(8)?.max(0) as u64;
            Ok(ModelUsage {
                model: row.get(0)?,
                effort: row.get(1)?,
                input_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                cached_input_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                output_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                reasoning_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                total_tokens,
                cost_usd: (unpriced_tokens == 0).then_some(known_cost_usd),
                unpriced_tokens,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_agent_summary_on(connection: &Connection, thread_id: &str) -> Result<Vec<AgentSummary>> {
    let mut statement = connection.prepare(
        "SELECT a.id,a.agent_path,a.nickname,COALESCE(a.status,'running'),
                (SELECT COUNT(*) FROM turns tr WHERE tr.agent_run_id=a.id),
                (SELECT COUNT(*) FROM tool_calls tc WHERE tc.agent_run_id=a.id),
                COALESCE(SUM(p.total_tokens),0),COALESCE(SUM(p.cost_usd),0.0),
                COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0)
         FROM agent_runs a LEFT JOIN priced_usage p
              ON p.agent_run_id=a.id AND p.thread_id=?1
         WHERE a.thread_id=?1 AND a.id<>a.thread_id
         GROUP BY a.id ORDER BY a.started_at",
    )?;
    Ok(statement
        .query_map([thread_id], |row| {
            let id: String = row.get(0)?;
            let path: Option<String> = row.get(1)?;
            let nickname: Option<String> = row.get(2)?;
            let total_tokens = row.get::<_, i64>(6)?.max(0) as u64;
            let known_cost_usd = row.get::<_, f64>(7)?;
            let unpriced_tokens = row.get::<_, i64>(8)?.max(0) as u64;
            let label = nickname
                .clone()
                .or_else(|| path.clone())
                .unwrap_or_else(|| "Primary agent".into());
            Ok(AgentSummary {
                id,
                label,
                path,
                nickname,
                status: row.get(3)?,
                turn_count: row.get::<_, i64>(4)?.max(0) as u64,
                tool_count: row.get::<_, i64>(5)?.max(0) as u64,
                total_tokens,
                cost_usd: (unpriced_tokens == 0).then_some(known_cost_usd),
                unpriced_tokens,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_tool_summary_on(connection: &Connection, thread_id: &str) -> Result<Vec<ToolSummary>> {
    let mut statement = connection.prepare(
        "SELECT namespace,name,COUNT(*),SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
                COALESCE(SUM(duration_ms),0)
         FROM tool_calls WHERE thread_id=?1
         GROUP BY namespace,name",
    )?;
    let mut grouped: HashMap<String, ToolSummary> = HashMap::new();
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?.max(0) as u64,
            row.get::<_, i64>(3)?.max(0) as u64,
            row.get::<_, i64>(4)?.max(0) as u64,
        ))
    })? {
        let (namespace, name, count, failed_count, total_duration_ms) = row?;
        let tool = display_tool_name(namespace.as_deref(), &name);
        let entry = grouped.entry(tool.clone()).or_insert(ToolSummary {
            tool,
            count: 0,
            failed_count: 0,
            total_duration_ms: 0,
        });
        entry.count = entry.count.saturating_add(count);
        entry.failed_count = entry.failed_count.saturating_add(failed_count);
        entry.total_duration_ms = entry.total_duration_ms.saturating_add(total_duration_ms);
    }
    let mut tools = grouped.into_values().collect::<Vec<_>>();
    tools.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.tool.cmp(&right.tool))
    });
    tools.truncate(100);
    Ok(tools)
}

fn display_tool_name(namespace: Option<&str>, name: &str) -> String {
    let name = match name {
        "web_search_call" => "web_search",
        "tool_search_call" => "tool_search",
        "image_generation_call" => "image_generation",
        "unknown" => "tool",
        other => other,
    };
    let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
        return name.to_owned();
    };
    let namespace = namespace
        .strip_prefix("mcp__")
        .unwrap_or(namespace)
        .trim_matches('_')
        .replace("__", ".");
    if namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{namespace}.{name}")
    }
}

fn query_agent_usage_on(connection: &Connection, thread_id: &str) -> Result<Vec<UsageBreakdown>> {
    let mut statement = connection.prepare(
        "SELECT a.id,a.agent_path,a.nickname,
                COALESCE(SUM(p.input_tokens),0),COALESCE(SUM(p.cached_input_tokens),0),
                COALESCE(SUM(p.output_tokens),0),COALESCE(SUM(p.reasoning_tokens),0),
                COALESCE(SUM(p.total_tokens),0),COALESCE(SUM(p.cost_usd),0.0),
                COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0)
         FROM agent_runs a LEFT JOIN priced_usage p
              ON p.agent_run_id=a.id AND p.thread_id=?1
         WHERE a.thread_id=?1 GROUP BY a.id ORDER BY a.started_at",
    )?;
    Ok(statement
        .query_map([thread_id], |row| {
            let id: String = row.get(0)?;
            let path: Option<String> = row.get(1)?;
            let nickname: Option<String> = row.get(2)?;
            Ok(UsageBreakdown {
                id: id.clone(),
                label: nickname.or(path).unwrap_or_else(|| "Primary agent".into()),
                model: None,
                agent_run_id: Some(id),
                turn_id: None,
                effort: None,
                totals: Totals {
                    input_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                    cached_input_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                    output_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                    reasoning_tokens: row.get::<_, i64>(6)?.max(0) as u64,
                    total_tokens: row.get::<_, i64>(7)?.max(0) as u64,
                    known_cost_usd: row.get(8)?,
                    unpriced_tokens: row.get::<_, i64>(9)?.max(0) as u64,
                    ..Totals::default()
                }
                .finish(),
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_turn_usage_on(connection: &Connection, thread_id: &str) -> Result<Vec<UsageBreakdown>> {
    let mut statement = connection.prepare(
        "SELECT t.id,t.started_at,t.model,t.effort,
                COALESCE(SUM(p.input_tokens),0),COALESCE(SUM(p.cached_input_tokens),0),
                COALESCE(SUM(p.output_tokens),0),COALESCE(SUM(p.reasoning_tokens),0),
                COALESCE(SUM(p.total_tokens),0),COALESCE(SUM(p.cost_usd),0.0),
                COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0)
         FROM turns t LEFT JOIN priced_usage p
              ON p.turn_id=t.id AND p.thread_id=?1
         WHERE t.thread_id=?1 GROUP BY t.id ORDER BY t.started_at",
    )?;
    Ok(statement
        .query_map([thread_id], |row| {
            let turn_id: String = row.get(0)?;
            let started_at: String = row.get(1)?;
            let totals = Totals {
                input_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                cached_input_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                output_tokens: row.get::<_, i64>(6)?.max(0) as u64,
                reasoning_tokens: row.get::<_, i64>(7)?.max(0) as u64,
                total_tokens: row.get::<_, i64>(8)?.max(0) as u64,
                known_cost_usd: row.get(9)?,
                unpriced_tokens: row.get::<_, i64>(10)?.max(0) as u64,
                ..Totals::default()
            }
            .finish();
            Ok(UsageBreakdown {
                id: turn_id.clone(),
                label: DateTime::parse_from_rfc3339(&started_at)
                    .map(|value| {
                        value
                            .with_timezone(&Local)
                            .format("%b %-d, %H:%M")
                            .to_string()
                    })
                    .unwrap_or(started_at),
                model: row.get(2)?,
                agent_run_id: None,
                turn_id: Some(turn_id),
                effort: row.get(3)?,
                totals,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

const ACTIVITY_PREVIEW_CHARS: i64 = 240;
const LEGACY_ACTIVITY_PREFIX: &str = "legacy:";

#[derive(Clone, Debug)]
struct ActivityTurnSummary {
    id: String,
    rollout_id: String,
    agent_run_id: Option<String>,
    agent_key: String,
    agent_label: Option<String>,
    started_at: String,
    status: String,
    model: Option<String>,
    effort: Option<String>,
    body: Option<String>,
    duration_ms: Option<i64>,
    review: bool,
}

#[derive(Clone, Debug)]
struct ActivityRootScope {
    id: String,
    started_at: String,
    next_started_at: Option<String>,
    open_left: bool,
}

#[derive(Debug)]
struct ActivityAgentLinkInterval {
    agent_key: String,
    linked_at: Option<String>,
    next_linked_at: Option<String>,
}

#[derive(Default)]
struct ActivityBatch {
    user_messages: HashMap<String, Vec<String>>,
    model_calls: HashMap<String, u64>,
    explicit_agent_intervals_by_turn: HashMap<String, Vec<ActivityAgentLinkInterval>>,
    explicit_agents_anywhere: HashSet<String>,
    descendant_turns: Vec<ActivityTurnSummary>,
    tool_calls_by_turn: HashMap<String, u64>,
    orphan_tool_calls_by_root: HashMap<String, u64>,
    totals_by_turn: HashMap<String, Totals>,
}

impl ActivityBatch {
    fn load(
        connection: &Connection,
        thread_id: &str,
        root_rollout_id: &str,
        roots: &[ActivityRootScope],
    ) -> Result<Self> {
        let mut batch = Self::default();
        if roots.is_empty() {
            return Ok(batch);
        }
        connection.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS selected_activity_roots(
                 turn_id TEXT PRIMARY KEY,
                 started_at TEXT NOT NULL,
                 next_started_at TEXT,
                 open_left INTEGER NOT NULL
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS selected_activity_turns(
                 turn_id TEXT PRIMARY KEY
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS activity_explicit_agents(
                 agent_key TEXT PRIMARY KEY
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS selected_activity_agent_intervals(
                 link_id TEXT PRIMARY KEY,
                 agent_key TEXT NOT NULL,
                 root_turn_id TEXT NOT NULL,
                 linked_at TEXT,
                 next_linked_at TEXT
             );
             DELETE FROM selected_activity_roots;
             DELETE FROM selected_activity_turns;
             DELETE FROM activity_explicit_agents;
             DELETE FROM selected_activity_agent_intervals;",
        )?;
        {
            let mut insert_root = connection.prepare(
                "INSERT INTO selected_activity_roots(
                     turn_id,started_at,next_started_at,open_left
                 ) VALUES(?1,?2,?3,?4)",
            )?;
            let mut insert_turn =
                connection.prepare("INSERT INTO selected_activity_turns(turn_id) VALUES(?1)")?;
            for root in roots {
                insert_root.execute(params![
                    root.id,
                    root.started_at,
                    root.next_started_at,
                    root.open_left
                ])?;
                insert_turn.execute([&root.id])?;
            }
        }

        let mut statement = connection.prepare(
            "SELECT e.turn_id,COALESCE(NULLIF(e.body,''),NULLIF(m.content,''))
             FROM events e
             JOIN selected_activity_roots selected ON selected.turn_id=e.turn_id
             LEFT JOIN messages m
               ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
             WHERE e.thread_id=?1 AND e.turn_id IS NOT NULL
               AND (e.kind='user' OR e.role='user')
             ORDER BY e.timestamp,e.source_line,e.id",
        )?;
        let rows = statement.query_map([thread_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        let mut seen_messages = HashMap::<String, HashSet<String>>::new();
        for row in rows {
            let (turn_id, content) = row?;
            if let Some(message) = content.and_then(|value| first_prompt_for_display(&value))
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
            batch.model_calls.insert(turn_id, count.max(0) as u64);
        }

        connection.execute(
            "INSERT OR IGNORE INTO activity_explicit_agents(agent_key)
             SELECT json_extract(link.payload_json,'$.agent_thread_id')
             FROM events link
             JOIN turns root_turn
               ON root_turn.id=link.turn_id AND root_turn.thread_id=link.thread_id
             WHERE link.thread_id=?1 AND link.kind='subagent'
               AND root_turn.rollout_id=?2
               AND json_extract(link.payload_json,'$.agent_thread_id') IS NOT NULL",
            params![thread_id, root_rollout_id],
        )?;
        // Agent clocks can place the first child turn just before its spawn
        // event. Keep that first interval open on the left; every later link
        // transfers the reused identity to the newly linked root exchange.
        connection.execute(
            "INSERT OR IGNORE INTO selected_activity_agent_intervals(
                 link_id,agent_key,root_turn_id,linked_at,next_linked_at
             )
             SELECT link.link_id,link.agent_key,link.root_turn_id,
                    CASE WHEN link.link_rank=1 THEN NULL ELSE link.timestamp END,
                    link.next_linked_at
             FROM (
                 SELECT event.id link_id,
                        json_extract(event.payload_json,'$.agent_thread_id') agent_key,
                        event.turn_id root_turn_id,event.timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY json_extract(
                                event.payload_json,'$.agent_thread_id'
                            )
                            ORDER BY event.timestamp,event.source_line,event.id
                        ) link_rank,
                        LEAD(event.timestamp) OVER (
                            PARTITION BY json_extract(
                                event.payload_json,'$.agent_thread_id'
                            )
                            ORDER BY event.timestamp,event.source_line,event.id
                        ) next_linked_at
                 FROM events event
                 JOIN turns root_turn
                   ON root_turn.id=event.turn_id AND root_turn.thread_id=event.thread_id
                 WHERE event.thread_id=?1 AND event.kind='subagent'
                   AND root_turn.rollout_id=?2
                   AND json_extract(event.payload_json,'$.agent_thread_id') IS NOT NULL
             ) link
             JOIN selected_activity_roots selected
               ON selected.turn_id=link.root_turn_id",
            params![thread_id, root_rollout_id],
        )?;
        let mut statement = connection.prepare(
            "SELECT agent_key,NULL,NULL,NULL FROM activity_explicit_agents
             UNION ALL
             SELECT agent_key,root_turn_id,linked_at,next_linked_at
             FROM selected_activity_agent_intervals",
        )?;
        for row in statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })? {
            let (agent_key, turn_id, linked_at, next_linked_at) = row?;
            batch.explicit_agents_anywhere.insert(agent_key.clone());
            if let Some(turn_id) = turn_id {
                batch
                    .explicit_agent_intervals_by_turn
                    .entry(turn_id)
                    .or_default()
                    .push(ActivityAgentLinkInterval {
                        agent_key,
                        linked_at,
                        next_linked_at,
                    });
            }
        }

        let mut statement = connection.prepare(
            "SELECT t.id,t.rollout_id,t.agent_run_id,COALESCE(t.agent_run_id,t.rollout_id),
                    t.started_at,t.status,t.model,t.effort,
                    NULLIF(substr(t.last_agent_message,1,?3),''),t.duration_ms,
                    a.nickname,a.agent_path
             FROM turns t LEFT JOIN agent_runs a
               ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
             WHERE t.thread_id=?1 AND t.rollout_id<>?2
               AND (
                    EXISTS(
                        SELECT 1 FROM selected_activity_agent_intervals explicit
                        WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                          AND (explicit.linked_at IS NULL
                               OR t.started_at>=explicit.linked_at)
                          AND (explicit.next_linked_at IS NULL
                               OR t.started_at<explicit.next_linked_at)
                    )
                    OR (
                        NOT EXISTS(
                            SELECT 1 FROM activity_explicit_agents explicit
                            WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                        )
                        AND EXISTS(
                            SELECT 1 FROM selected_activity_roots selected
                            WHERE t.started_at>=selected.started_at
                              AND (selected.next_started_at IS NULL
                                   OR t.started_at<selected.next_started_at)
                        )
                    )
               )
             ORDER BY t.started_at DESC,t.id DESC",
        )?;
        batch.descendant_turns = statement
            .query_map(
                params![thread_id, root_rollout_id, ACTIVITY_PREVIEW_CHARS],
                |row| {
                    let model = row.get::<_, Option<String>>(6)?;
                    Ok(ActivityTurnSummary {
                        id: row.get(0)?,
                        rollout_id: row.get(1)?,
                        agent_run_id: row.get(2)?,
                        agent_key: row.get(3)?,
                        started_at: row.get(4)?,
                        status: row.get(5)?,
                        review: model.as_deref() == Some("codex-auto-review"),
                        model,
                        effort: row.get(7)?,
                        body: row.get(8)?,
                        duration_ms: row.get(9)?,
                        agent_label: row
                            .get::<_, Option<String>>(10)?
                            .or(row.get::<_, Option<String>>(11)?),
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        {
            let mut insert_turn = connection
                .prepare("INSERT OR IGNORE INTO selected_activity_turns(turn_id) VALUES(?1)")?;
            for turn in &batch.descendant_turns {
                insert_turn.execute([&turn.id])?;
            }
        }

        let mut statement = connection.prepare(
            "SELECT 0,tc.turn_id,COUNT(*)
             FROM tool_calls tc
             JOIN selected_activity_turns selected ON selected.turn_id=tc.turn_id
             WHERE tc.thread_id=?1
             GROUP BY tc.turn_id
             UNION ALL
             SELECT 1,selected.turn_id,COUNT(*)
             FROM selected_activity_roots selected
             JOIN tool_calls tc
               ON tc.thread_id=?1
              AND (selected.open_left=1 OR tc.started_at>=selected.started_at)
              AND (selected.next_started_at IS NULL
                   OR tc.started_at<selected.next_started_at)
             LEFT JOIN turns linked
               ON linked.id=tc.turn_id AND linked.thread_id=tc.thread_id
             WHERE linked.id IS NULL
             GROUP BY selected.turn_id",
        )?;
        for row in statement.query_map([thread_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })? {
            let (orphaned, turn_id, count) = row?;
            if orphaned == 0 {
                batch
                    .tool_calls_by_turn
                    .insert(turn_id, count.max(0) as u64);
            } else {
                // A few legacy tool calls have no valid turn link. Attribute those by the
                // stable root-exchange interval, just as the row-by-row fallback did.
                batch
                    .orphan_tool_calls_by_root
                    .insert(turn_id, count.max(0) as u64);
            }
        }

        // A usage fact can survive without a turn link (for example when a
        // partially written trace is reconciled). Root exchanges still form a
        // complete, deterministic timeline: the first exchange is open on the
        // left, every other exchange starts at its root timestamp, and the last
        // exchange is open on the right. The second UNION arm gives each such
        // fact exactly one owner without adding another query to Activity's
        // fixed statement budget.
        let mut statement = connection.prepare(
            "SELECT p.turn_id,COALESCE(SUM(p.input_tokens),0),
                    COALESCE(SUM(p.cached_input_tokens),0),COALESCE(SUM(p.output_tokens),0),
                    COALESCE(SUM(p.reasoning_tokens),0),COALESCE(SUM(p.total_tokens),0),
                    COALESCE(SUM(p.cost_usd),0.0),
                    COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0)
             FROM priced_usage p
             JOIN selected_activity_turns selected ON selected.turn_id=p.turn_id
             WHERE p.thread_id=?1 GROUP BY p.turn_id
             UNION ALL
             SELECT selected.turn_id,COALESCE(SUM(p.input_tokens),0),
                    COALESCE(SUM(p.cached_input_tokens),0),COALESCE(SUM(p.output_tokens),0),
                    COALESCE(SUM(p.reasoning_tokens),0),COALESCE(SUM(p.total_tokens),0),
                    COALESCE(SUM(p.cost_usd),0.0),
                    COALESCE(SUM(CASE WHEN p.price_known=0 THEN p.total_tokens ELSE 0 END),0)
             FROM selected_activity_roots selected
             JOIN priced_usage p
               ON p.thread_id=?1 AND p.turn_id IS NULL
              AND (selected.open_left=1 OR p.timestamp>=selected.started_at)
              AND (selected.next_started_at IS NULL
                   OR p.timestamp<selected.next_started_at)
             GROUP BY selected.turn_id",
        )?;
        for row in statement.query_map([thread_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Totals {
                    input_tokens: row.get::<_, i64>(1)?.max(0) as u64,
                    cached_input_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                    output_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                    reasoning_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                    total_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                    known_cost_usd: row.get(6)?,
                    unpriced_tokens: row.get::<_, i64>(7)?.max(0) as u64,
                    ..Totals::default()
                },
            ))
        })? {
            let (turn_id, totals) = row?;
            add_activity_usage(batch.totals_by_turn.entry(turn_id).or_default(), &totals);
        }
        for totals in batch.totals_by_turn.values_mut() {
            *totals = std::mem::take(totals).finish();
        }
        Ok(batch)
    }

    fn descendants(
        &self,
        root_turn_id: &str,
        start: &str,
        end: Option<&str>,
    ) -> Vec<AttributedDescendant> {
        let explicit_here = self.explicit_agent_intervals_by_turn.get(root_turn_id);
        self.descendant_turns
            .iter()
            .filter(|turn| {
                if self.explicit_agents_anywhere.contains(&turn.agent_key) {
                    explicit_here.is_some_and(|intervals| {
                        intervals.iter().any(|interval| {
                            interval.agent_key == turn.agent_key
                                && interval
                                    .linked_at
                                    .as_deref()
                                    .is_none_or(|start| turn.started_at.as_str() >= start)
                                && interval
                                    .next_linked_at
                                    .as_deref()
                                    .is_none_or(|end| turn.started_at.as_str() < end)
                        })
                    })
                } else {
                    timestamp_in_exchange(&turn.started_at, start, end)
                }
            })
            .map(|turn| AttributedDescendant {
                id: turn.id.clone(),
                agent_key: turn.agent_key.clone(),
                review: turn.review,
            })
            .collect()
    }

    fn counts(&self, root_turn_id: &str, descendants: &[AttributedDescendant]) -> ActivityCounts {
        let tool_calls = descendants.iter().fold(
            self.tool_calls_by_turn
                .get(root_turn_id)
                .copied()
                .unwrap_or(0)
                .saturating_add(
                    self.orphan_tool_calls_by_root
                        .get(root_turn_id)
                        .copied()
                        .unwrap_or(0),
                ),
            |total, turn| {
                total.saturating_add(self.tool_calls_by_turn.get(&turn.id).copied().unwrap_or(0))
            },
        );
        ActivityCounts {
            model_calls: self.model_calls.get(root_turn_id).copied().unwrap_or(0),
            tool_calls,
            agent_runs: descendants
                .iter()
                .filter(|turn| !turn.review)
                .map(|turn| turn.agent_key.as_str())
                .collect::<HashSet<_>>()
                .len() as u64,
            reviews: descendants.iter().filter(|turn| turn.review).count() as u64,
            follow_ups: self
                .user_messages
                .get(root_turn_id)
                .map_or(0, |messages| messages.len().saturating_sub(1) as u64),
        }
    }

    fn exchange_totals(&self, root_turn_id: &str, descendants: &[AttributedDescendant]) -> Totals {
        let mut totals = Totals::default();
        if let Some(root) = self.totals_by_turn.get(root_turn_id) {
            add_activity_usage(&mut totals, root);
        }
        for descendant in descendants {
            if let Some(usage) = self.totals_by_turn.get(&descendant.id) {
                add_activity_usage(&mut totals, usage);
            }
        }
        totals.finish()
    }

    fn turn_summaries(
        &self,
        descendants: &[AttributedDescendant],
        reviews: bool,
    ) -> Vec<ActivityItem> {
        let attributed = descendants
            .iter()
            .filter(|turn| turn.review == reviews)
            .map(|turn| turn.id.as_str())
            .collect::<HashSet<_>>();
        self.descendant_turns
            .iter()
            .filter(|turn| attributed.contains(turn.id.as_str()))
            .map(|turn| {
                let label = turn.agent_label.clone().unwrap_or_else(|| {
                    if reviews {
                        "Automated review".into()
                    } else {
                        "Agent response".into()
                    }
                });
                ActivityItem {
                    usage: Some(
                        self.totals_by_turn
                            .get(&turn.id)
                            .cloned()
                            .unwrap_or_else(|| Totals::default().finish()),
                    ),
                    id: turn.id.clone(),
                    turn_id: Some(turn.id.clone()),
                    rollout_id: turn.rollout_id.clone(),
                    agent_run_id: turn.agent_run_id.clone(),
                    agent_label: turn.agent_label.clone(),
                    timestamp: turn.started_at.clone(),
                    kind: if reviews {
                        "review".into()
                    } else {
                        "subagent".into()
                    },
                    role: None,
                    label: Some(label),
                    body: bounded_preview(turn.body.clone()),
                    status: Some(turn.status.clone()),
                    tool_name: None,
                    duration_ms: turn.duration_ms,
                    model: turn.model.clone(),
                    effort: turn.effort.clone(),
                    has_details: true,
                    children: Vec::new(),
                    child_page: None,
                    child_page_size: None,
                    child_total: None,
                    child_has_more: None,
                    counts: None,
                }
            })
            .collect()
    }
}

fn query_activity_on(
    connection: &Connection,
    thread_id: &str,
    page: u64,
    page_size: u64,
) -> Result<ActivityResponse> {
    let root_rollout_id = query_root_rollout_id(connection, thread_id)?;
    let total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM turns WHERE thread_id=?1 AND rollout_id=?2",
        params![thread_id, root_rollout_id],
        |row| row.get(0),
    )?;
    let days = query_activity_day_summaries_batched(connection, thread_id)?;
    if total == 0 {
        let item = query_legacy_activity_item(connection, thread_id, &root_rollout_id, false)?;
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
    let batch = ActivityBatch::load(connection, thread_id, &root_rollout_id, &root_scopes)?;
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
        next_started_at,
        _open_left,
    ) in turn_rows
    {
        let messages = batch
            .user_messages
            .get(&turn_id)
            .cloned()
            .unwrap_or_default();
        let descendants = batch.descendants(&turn_id, &started_at, next_started_at.as_deref());
        let counts = batch.counts(&turn_id, &descendants);
        let totals = batch.exchange_totals(&turn_id, &descendants);
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

fn query_root_rollout_id(connection: &Connection, thread_id: &str) -> Result<String> {
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

fn legacy_activity_id(thread_id: &str) -> String {
    format!("{LEGACY_ACTIVITY_PREFIX}{thread_id}")
}

fn query_legacy_activity_item(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    include_children: bool,
) -> Result<Option<ActivityItem>> {
    let exists: i64 = connection.query_row(
        "SELECT
            EXISTS(SELECT 1 FROM messages WHERE thread_id=?1)
          + EXISTS(SELECT 1 FROM events WHERE thread_id=?1)
          + EXISTS(SELECT 1 FROM tool_calls WHERE thread_id=?1)
          + EXISTS(SELECT 1 FROM usage_facts WHERE thread_id=?1)",
        [thread_id],
        |row| row.get(0),
    )?;
    if exists == 0 {
        return Ok(None);
    }

    let mut first_user = None;
    let mut latest_assistant = None;
    let mut message_children = Vec::new();
    let mut has_messages = false;
    let mut statement = connection.prepare(
        "SELECT id,rollout_id,turn_id,timestamp,role,content
         FROM messages WHERE thread_id=?1
         ORDER BY timestamp,source_line,id",
    )?;
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
        ))
    })? {
        let (id, rollout_id, turn_id, timestamp, role, content) = row?;
        has_messages = true;
        let display = if role == "user" {
            first_prompt_for_display(&content)
        } else {
            Some(redact_data_urls(&content))
        };
        if role == "user" && first_user.is_none() {
            first_user = display.clone();
        }
        if role == "assistant" {
            latest_assistant = display.clone();
        }
        if include_children {
            message_children.push(ActivityItem {
                id: format!("legacy-message:{id}"),
                turn_id,
                rollout_id,
                agent_run_id: None,
                agent_label: None,
                timestamp,
                kind: if role == "user" { "user" } else { "final" }.into(),
                role: Some(role),
                label: None,
                body: display,
                status: None,
                tool_name: None,
                duration_ms: None,
                model: None,
                effort: None,
                has_details: false,
                children: Vec::new(),
                child_page: None,
                child_page_size: None,
                child_total: None,
                child_has_more: None,
                usage: None,
                counts: None,
            });
        }
    }
    let mut children = if include_children {
        query_activity_child_previews(connection, thread_id, None)?
    } else {
        Vec::new()
    };
    if include_children {
        // Legacy projections can contain both canonical events and their mirrored message row.
        // Prefer the richer event representation, but retain messages that have no event at all.
        let event_signatures = children
            .iter()
            .filter_map(|item| {
                item.body
                    .as_ref()
                    .map(|body| (item.timestamp.clone(), body.clone()))
            })
            .collect::<HashSet<_>>();
        let event_ids = children
            .iter()
            .map(|item| item.id.clone())
            .collect::<HashSet<_>>();
        children.extend(message_children.into_iter().filter(|message| {
            let stored_id = message
                .id
                .strip_prefix("legacy-message:")
                .unwrap_or(&message.id);
            !event_ids.contains(stored_id)
                && message.body.as_ref().is_none_or(|body| {
                    !event_signatures.contains(&(message.timestamp.clone(), body.clone()))
                })
        }));
        children.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| right.id.cmp(&left.id))
        });
    }

    let timestamp = connection.query_row(
        "SELECT COALESCE(
            (SELECT MIN(value) FROM (
                SELECT MIN(timestamp) value FROM messages WHERE thread_id=?1
                UNION ALL SELECT MIN(timestamp) FROM usage_facts WHERE thread_id=?1
                UNION ALL SELECT MIN(timestamp) FROM events WHERE thread_id=?1
                UNION ALL SELECT MIN(started_at) FROM tool_calls WHERE thread_id=?1
             ) WHERE value IS NOT NULL),
            (SELECT started_at FROM threads WHERE id=?1))",
        [thread_id],
        |row| row.get::<_, String>(0),
    )?;
    let totals = query_totals_on(connection, None, None, Some(thread_id))?;
    let thread_title = connection
        .query_row(
            "SELECT NULLIF(trim(title),'') FROM threads WHERE id=?1",
            [thread_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    Ok(Some(ActivityItem {
        id: legacy_activity_id(thread_id),
        turn_id: None,
        rollout_id: root_rollout_id.to_owned(),
        agent_run_id: None,
        agent_label: None,
        timestamp,
        kind: "exchange".into(),
        role: Some("user".into()),
        label: Some(
            bounded_preview(first_user)
                .or_else(|| bounded_preview(thread_title))
                .unwrap_or_else(|| {
                    if totals.total_tokens > 0 {
                        "Usage activity".into()
                    } else {
                        "Conversation".into()
                    }
                }),
        ),
        body: bounded_preview(latest_assistant),
        status: None,
        tool_name: None,
        duration_ms: None,
        model: None,
        effort: None,
        has_details: has_messages
            || connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE thread_id=?1)
                      OR EXISTS(SELECT 1 FROM tool_calls WHERE thread_id=?1)",
                [thread_id],
                |row| row.get::<_, i64>(0),
            )? != 0,
        children,
        child_page: None,
        child_page_size: None,
        child_total: None,
        child_has_more: None,
        usage: Some(totals),
        counts: None,
    }))
}

#[derive(Debug)]
struct AttributedDescendant {
    id: String,
    agent_key: String,
    review: bool,
}

fn timestamp_in_exchange(timestamp: &str, start: &str, end: Option<&str>) -> bool {
    timestamp >= start && end.is_none_or(|end| timestamp < end)
}

fn query_next_root_turn_start(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    started_at: &str,
    turn_id: &str,
) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT started_at FROM turns
             WHERE thread_id=?1 AND rollout_id=?2
               AND (started_at>?3 OR (started_at=?3 AND id>?4))
             ORDER BY started_at,id LIMIT 1",
            params![thread_id, root_rollout_id, started_at, turn_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

fn is_first_root_turn(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    started_at: &str,
    turn_id: &str,
) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT NOT EXISTS(
             SELECT 1 FROM turns
             WHERE thread_id=?1 AND rollout_id=?2
               AND (started_at<?3 OR (started_at=?3 AND id<?4))
         )",
        params![thread_id, root_rollout_id, started_at, turn_id],
        |row| row.get::<_, i64>(0),
    )? != 0)
}

fn query_exchange_groups(
    batch: &ActivityBatch,
    root_turn_id: &str,
    root_rollout_id: &str,
    counts: &ActivityCounts,
    descendants: &[AttributedDescendant],
) -> Vec<ActivityItem> {
    let mut groups = Vec::new();
    let agent_turns = batch.turn_summaries(descendants, false);
    if !agent_turns.is_empty() {
        let agent_usage = sum_activity_usage(&agent_turns);
        let agent_duration = activity_items_union_duration(&agent_turns);
        let mut labels = Vec::new();
        let mut seen = HashSet::new();
        for item in &agent_turns {
            if let Some(label) = item
                .agent_label
                .as_ref()
                .filter(|label| seen.insert((*label).clone()))
            {
                labels.push(label.clone());
            }
        }
        groups.push(ActivityItem {
            id: format!("group:agents:{root_turn_id}"),
            turn_id: Some(root_turn_id.to_owned()),
            rollout_id: root_rollout_id.to_owned(),
            agent_run_id: None,
            agent_label: None,
            timestamp: agent_turns[0].timestamp.clone(),
            kind: "agent_group".into(),
            role: None,
            label: Some(format!("Agents · {}", counts.agent_runs)),
            body: (!labels.is_empty()).then(|| labels.join(" · ")),
            status: Some(group_status(&agent_turns)),
            tool_name: None,
            duration_ms: agent_duration,
            model: None,
            effort: None,
            has_details: true,
            children: agent_turns,
            child_page: None,
            child_page_size: None,
            child_total: None,
            child_has_more: None,
            usage: Some(agent_usage),
            counts: None,
        });
    }

    let review_turns = batch.turn_summaries(descendants, true);
    if !review_turns.is_empty() {
        let review_usage = sum_activity_usage(&review_turns);
        let review_duration = activity_items_union_duration(&review_turns);
        groups.push(ActivityItem {
            id: format!("group:reviews:{root_turn_id}"),
            turn_id: Some(root_turn_id.to_owned()),
            rollout_id: root_rollout_id.to_owned(),
            agent_run_id: None,
            agent_label: None,
            timestamp: review_turns[0].timestamp.clone(),
            kind: "review_group".into(),
            role: None,
            label: Some(format!("Automated reviews · {}", counts.reviews)),
            body: None,
            status: Some(group_status(&review_turns)),
            tool_name: None,
            duration_ms: review_duration,
            model: None,
            effort: None,
            has_details: true,
            children: review_turns,
            child_page: None,
            child_page_size: None,
            child_total: None,
            child_has_more: None,
            usage: Some(review_usage),
            counts: None,
        });
    }
    groups
}

fn group_status(items: &[ActivityItem]) -> String {
    if items
        .iter()
        .any(|item| item.status.as_deref() == Some("running"))
    {
        "running".into()
    } else if items.iter().any(|item| {
        !matches!(
            item.status.as_deref(),
            Some("completed") | Some("success") | Some("allowed")
        )
    }) {
        "attention".into()
    } else {
        "completed".into()
    }
}

fn sum_activity_usage(items: &[ActivityItem]) -> Totals {
    let mut totals = Totals::default();
    for usage in items.iter().filter_map(|item| item.usage.as_ref()) {
        add_activity_usage(&mut totals, usage);
    }
    totals.finish()
}

fn add_activity_usage(target: &mut Totals, usage: &Totals) {
    target.input_tokens = target.input_tokens.saturating_add(usage.input_tokens);
    target.cached_input_tokens = target
        .cached_input_tokens
        .saturating_add(usage.cached_input_tokens);
    target.output_tokens = target.output_tokens.saturating_add(usage.output_tokens);
    target.reasoning_tokens = target
        .reasoning_tokens
        .saturating_add(usage.reasoning_tokens);
    target.total_tokens = target.total_tokens.saturating_add(usage.total_tokens);
    target.known_cost_usd += usage.known_cost_usd;
    target.unpriced_tokens = target.unpriced_tokens.saturating_add(usage.unpriced_tokens);
}

fn activity_items_union_duration(items: &[ActivityItem]) -> Option<i64> {
    let mut intervals = items
        .iter()
        .filter_map(|item| {
            let duration_ms = item.duration_ms?.max(0);
            let start = DateTime::parse_from_rfc3339(&item.timestamp)
                .ok()?
                .with_timezone(&Utc);
            let duration = Duration::try_milliseconds(duration_ms)?;
            Some((start, start.checked_add_signed(duration)?))
        })
        .collect::<Vec<_>>();
    if intervals.is_empty() {
        return None;
    }
    intervals.sort_by_key(|(start, _)| *start);
    let mut total_ms = 0_i64;
    let mut current = intervals[0];
    for (start, end) in intervals.into_iter().skip(1) {
        if start <= current.1 {
            current.1 = current.1.max(end);
        } else {
            total_ms = total_ms.saturating_add((current.1 - current.0).num_milliseconds());
            current = (start, end);
        }
    }
    Some(total_ms.saturating_add((current.1 - current.0).num_milliseconds()))
}

fn query_activity_day_summaries_batched(
    connection: &Connection,
    thread_id: &str,
) -> Result<Vec<ActivityDaySummary>> {
    let raw_thread_bounds = connection.query_row(
        "SELECT started_at,last_event_at FROM threads WHERE id=?1",
        [thread_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let thread_bounds = parse_activity_thread_bounds(&raw_thread_bounds.0, &raw_thread_bounds.1);
    let mut statement = connection.prepare(
        "WITH activity_rows(is_turn,started_at,completed_at,duration_ms) AS (
             SELECT 1,started_at,completed_at,duration_ms FROM turns WHERE thread_id=?1
             UNION ALL
             SELECT 0,timestamp,NULL,duration_ms FROM events WHERE thread_id=?1
             UNION ALL
             SELECT 0,timestamp,NULL,NULL FROM messages WHERE thread_id=?1
             UNION ALL
             SELECT 0,timestamp,NULL,NULL FROM usage_facts WHERE thread_id=?1
         )
         SELECT * FROM activity_rows",
    )?;
    let rows = statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, i64>(0)? != 0,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })?;
    let mut dates = HashSet::new();
    let mut turn_intervals = Vec::new();
    for row in rows {
        let (is_turn, raw_start, raw_end, duration_ms) = row?;
        record_activity_row(
            &mut dates,
            &mut turn_intervals,
            is_turn,
            &raw_start,
            raw_end.as_deref(),
            duration_ms,
            thread_bounds,
        );
    }

    // Tool calls dominate large sessions. Their start dates can be collapsed inside SQLite;
    // only the rare interval that crosses a local calendar boundary needs exact Rust parsing.
    // This preserves day occupancy while avoiding one Rust allocation and RFC 3339 parse per
    // historical call on the ordinary collapsed Activity page.
    let mut statement = connection.prepare(
        "SELECT date(started_at,'localtime'),
                SUM(CASE WHEN date(
                    COALESCE(
                        CASE WHEN julianday(completed_at) IS NOT NULL THEN completed_at END,
                        datetime(
                            julianday(started_at)
                            + CAST(MAX(COALESCE(duration_ms,0),0) AS REAL) / 86400000.0
                        ),
                        started_at
                    ),
                    'localtime'
                )<>date(started_at,'localtime') THEN 1 ELSE 0 END)
         FROM tool_calls
         WHERE thread_id=?1 AND date(started_at,'localtime') IS NOT NULL
         GROUP BY date(started_at,'localtime')",
    )?;
    let mut has_crossing_tool_intervals = false;
    for row in statement.query_map([thread_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })? {
        let (raw_date, crossing_intervals) = row?;
        if let Ok(date) = NaiveDate::parse_from_str(&raw_date, "%Y-%m-%d") {
            dates.insert(date);
        }
        has_crossing_tool_intervals |= crossing_intervals > 0;
    }
    drop(statement);
    if has_crossing_tool_intervals {
        let mut statement = connection.prepare(
            "SELECT started_at,completed_at,duration_ms
             FROM tool_calls
             WHERE thread_id=?1
               AND date(
                    COALESCE(
                        CASE WHEN julianday(completed_at) IS NOT NULL THEN completed_at END,
                        datetime(
                            julianday(started_at)
                            + CAST(MAX(COALESCE(duration_ms,0),0) AS REAL) / 86400000.0
                        ),
                        started_at
                    ),
                    'localtime'
               )<>date(started_at,'localtime')",
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

    let mut totals_by_date = HashMap::<NaiveDate, Totals>::new();
    let mut statement = connection.prepare(
        "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,COALESCE(cost_usd,0.0),
                CASE WHEN price_known=0 THEN total_tokens ELSE 0 END
         FROM priced_usage WHERE thread_id=?1",
    )?;
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            Totals {
                input_tokens: row.get::<_, i64>(1)?.max(0) as u64,
                cached_input_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                output_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                reasoning_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                total_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                known_cost_usd: row.get(6)?,
                unpriced_tokens: row.get::<_, i64>(7)?.max(0) as u64,
                ..Totals::default()
            },
        ))
    })? {
        let (timestamp, usage) = row?;
        let Ok(timestamp) = DateTime::parse_from_rfc3339(&timestamp) else {
            continue;
        };
        add_activity_usage(
            totals_by_date
                .entry(timestamp.with_timezone(&Local).date_naive())
                .or_default(),
            &usage,
        );
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

fn activity_day_window(date: NaiveDate) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let next_date = date.succ_opt()?;
    Some((local_midnight(date), local_midnight(next_date)))
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
fn query_activity_detail_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
) -> Result<Option<ActivityItem>> {
    query_activity_detail_page_on(
        connection,
        thread_id,
        item_id,
        1,
        DEFAULT_ACTIVITY_CHILD_PAGE_SIZE,
    )
}

fn query_activity_detail_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    child_page: u64,
    child_page_size: u64,
) -> Result<Option<ActivityItem>> {
    let root_rollout_id = query_root_rollout_id(connection, thread_id)?;
    if item_id == legacy_activity_id(thread_id) {
        let Some(mut item) =
            query_legacy_activity_item(connection, thread_id, &root_rollout_id, true)?
        else {
            return Ok(None);
        };
        page_existing_activity_children(&mut item, child_page, child_page_size);
        return Ok(Some(item));
    }
    if let Some((reviews, root_turn_id)) = parse_activity_group_id(item_id) {
        return query_activity_group_detail_on(
            connection,
            thread_id,
            &root_rollout_id,
            root_turn_id,
            reviews,
            child_page,
            child_page_size,
        );
    }
    if let Some(mut turn) = connection
        .query_row(
            "SELECT t.id,t.rollout_id,t.agent_run_id,t.started_at,t.status,t.model,t.effort,
                    t.last_agent_message,t.duration_ms,a.nickname,a.agent_path
             FROM turns t LEFT JOIN agent_runs a
               ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
             WHERE t.thread_id=?1 AND t.id=?2",
            params![thread_id, item_id],
            |row| {
                let agent_label = row
                    .get::<_, Option<String>>(9)?
                    .or(row.get::<_, Option<String>>(10)?);
                Ok(ActivityItem {
                    id: row.get(0)?,
                    turn_id: row.get(0)?,
                    rollout_id: row.get(1)?,
                    agent_run_id: row.get(2)?,
                    agent_label: agent_label.clone(),
                    timestamp: row.get(3)?,
                    kind: "system".into(),
                    role: None,
                    label: Some(
                        agent_label
                            .map(|value| format!("{value} · Turn"))
                            .unwrap_or_else(|| "Turn".into()),
                    ),
                    body: row
                        .get::<_, Option<String>>(7)?
                        .map(|value| redact_data_urls(&value))
                        .filter(|value| !value.is_empty()),
                    status: row.get(4)?,
                    tool_name: None,
                    duration_ms: row.get(8)?,
                    model: row.get(5)?,
                    effort: row.get(6)?,
                    has_details: true,
                    children: Vec::new(),
                    child_page: None,
                    child_page_size: None,
                    child_total: None,
                    child_has_more: None,
                    usage: None,
                    counts: None,
                })
            },
        )
        .optional()?
    {
        if turn.rollout_id == root_rollout_id {
            let next_started_at = query_next_root_turn_start(
                connection,
                thread_id,
                &root_rollout_id,
                &turn.timestamp,
                item_id,
            )?;
            let root_scope = ActivityRootScope {
                id: item_id.to_owned(),
                started_at: turn.timestamp.clone(),
                next_started_at: next_started_at.clone(),
                open_left: is_first_root_turn(
                    connection,
                    thread_id,
                    &root_rollout_id,
                    &turn.timestamp,
                    item_id,
                )?,
            };
            let batch = ActivityBatch::load(
                connection,
                thread_id,
                &root_rollout_id,
                std::slice::from_ref(&root_scope),
            )?;
            let messages = batch
                .user_messages
                .get(item_id)
                .cloned()
                .unwrap_or_default();
            let descendants =
                batch.descendants(item_id, &turn.timestamp, next_started_at.as_deref());
            let counts = batch.counts(item_id, &descendants);
            turn.kind = "exchange".into();
            turn.role = Some("user".into());
            turn.label = Some(
                bounded_preview(messages.first().cloned()).unwrap_or_else(|| "Conversation".into()),
            );
            turn.counts = Some(counts.clone());
            turn.usage = Some(batch.exchange_totals(item_id, &descendants));
            let child_page = query_activity_child_previews_page(
                connection,
                thread_id,
                item_id,
                child_page,
                child_page_size,
            )?;
            turn.children = child_page.items;
            let mut groups =
                query_exchange_groups(&batch, item_id, &root_rollout_id, &counts, &descendants);
            for group in &mut groups {
                let total = group.children.len() as u64;
                group.children.clear();
                group.child_page = Some(1);
                group.child_page_size = Some(child_page_size);
                group.child_total = Some(total);
                group.child_has_more = Some(total > 0);
            }
            turn.children.extend(groups);
            turn.child_page = Some(child_page.page);
            turn.child_page_size = Some(child_page.page_size);
            turn.child_total = Some(child_page.total);
            turn.child_has_more = Some(child_page.has_more);
            turn.children.sort_by(|left, right| {
                right
                    .timestamp
                    .cmp(&left.timestamp)
                    .then_with(|| right.id.cmp(&left.id))
            });
        } else {
            turn.kind = if turn.model.as_deref() == Some("codex-auto-review") {
                "review".into()
            } else {
                "subagent".into()
            };
            turn.label = Some(turn.agent_label.clone().unwrap_or_else(|| {
                if turn.kind == "review" {
                    "Automated review".into()
                } else {
                    "Agent response".into()
                }
            }));
            let child_page = query_activity_child_previews_page(
                connection,
                thread_id,
                item_id,
                child_page,
                child_page_size,
            )?;
            turn.children = child_page.items;
            turn.child_page = Some(child_page.page);
            turn.child_page_size = Some(child_page.page_size);
            turn.child_total = Some(child_page.total);
            turn.child_has_more = Some(child_page.has_more);
            turn.usage = Some(query_activity_turn_totals_on(
                connection, thread_id, item_id,
            )?);
        }
        // Prefer the final event in its chronological place. Some modern
        // traces only carry the final body on task_complete, while legacy
        // traces also have a dedicated final response item. The child query
        // chooses exactly one; keep last_agent_message only as a sparse-data
        // fallback when neither representation exists.
        if turn.children.iter().any(|child| {
            child.kind == "final"
                && child
                    .body
                    .as_deref()
                    .is_some_and(|body| !body.trim().is_empty())
        }) {
            turn.body = None;
        }
        turn.has_details = turn.body.is_some() || !turn.children.is_empty();
        return Ok(Some(turn));
    }

    let mut event = connection
        .query_row(
            "SELECT e.id,e.turn_id,e.rollout_id,e.agent_run_id,e.timestamp,e.kind,e.role,e.label,
                    COALESCE(e.body,m.content),COALESCE(tc.status,e.status),
                    COALESCE(tc.name,e.tool_name),COALESCE(
                        tc.duration_ms,e.duration_ms,
                        CASE WHEN tc.completed_at IS NOT NULL THEN
                            CAST(ROUND((julianday(tc.completed_at)-julianday(tc.started_at))*86400000.0)
                                AS INTEGER)
                        END),
                    e.model,e.effort,a.nickname,a.agent_path,tc.namespace,e.source_line
             FROM events e
             LEFT JOIN messages m
               ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
             LEFT JOIN tool_calls tc
               ON tc.rollout_id=e.rollout_id AND tc.call_id=e.call_id
              AND tc.thread_id=e.thread_id
             LEFT JOIN agent_runs a
               ON a.id=e.agent_run_id AND a.thread_id=e.thread_id
             WHERE e.thread_id=?1 AND e.id=?2",
            params![thread_id, item_id],
            |row| {
                let stored_kind: String = row.get(5)?;
                let role: Option<String> = row.get(6)?;
                let stored_tool_name = row.get::<_, Option<String>>(10)?;
                let body = if stored_kind == "tool_call" {
                    None
                } else {
                    row.get::<_, Option<String>>(8)?
                        .map(|value| redact_data_urls(&value))
                        .filter(|value| !value.is_empty())
                };
                let agent_label = row
                    .get::<_, Option<String>>(14)?
                    .or(row.get::<_, Option<String>>(15)?);
                let tool_namespace = row.get::<_, Option<String>>(16)?;
                Ok(ActivityItem {
                    id: row.get(0)?,
                    turn_id: row.get(1)?,
                    rollout_id: row.get(2)?,
                    agent_run_id: row.get(3)?,
                    agent_label,
                    timestamp: row.get(4)?,
                    kind: normalize_activity_kind(&stored_kind, role.as_deref()),
                    role,
                    label: row.get(7)?,
                    has_details: body.is_some(),
                    body,
                    status: row.get(9)?,
                    tool_name: stored_tool_name
                        .map(|name| display_tool_name(tool_namespace.as_deref(), &name)),
                    duration_ms: row.get(11)?,
                    model: row.get(12)?,
                    effort: row.get(13)?,
                    children: Vec::new(),
                    child_page: None,
                    child_page_size: None,
                    child_total: None,
                    child_has_more: None,
                    usage: None,
                    counts: None,
                })
            },
        )
        .optional()?;
    if let Some(item) = event.as_mut()
        && let Some(turn_id) = item.turn_id.as_deref()
    {
        item.usage = query_activity_child_previews(connection, thread_id, Some(turn_id))?
            .into_iter()
            .find(|preview| preview.id == item.id)
            .and_then(|preview| preview.usage);
    }
    Ok(event)
}

fn parse_activity_group_id(item_id: &str) -> Option<(bool, &str)> {
    item_id
        .strip_prefix("group:agents:")
        .map(|root| (false, root))
        .or_else(|| {
            item_id
                .strip_prefix("group:reviews:")
                .map(|root| (true, root))
        })
}

#[allow(clippy::too_many_arguments)]
fn query_activity_group_detail_on(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    root_turn_id: &str,
    reviews: bool,
    child_page: u64,
    child_page_size: u64,
) -> Result<Option<ActivityItem>> {
    let root = connection
        .query_row(
            "SELECT started_at FROM turns
             WHERE thread_id=?1 AND rollout_id=?2 AND id=?3",
            params![thread_id, root_rollout_id, root_turn_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(started_at) = root else {
        return Ok(None);
    };
    let next_started_at = query_next_root_turn_start(
        connection,
        thread_id,
        root_rollout_id,
        &started_at,
        root_turn_id,
    )?;
    let root_scope = ActivityRootScope {
        id: root_turn_id.to_owned(),
        started_at: started_at.clone(),
        next_started_at: next_started_at.clone(),
        open_left: is_first_root_turn(
            connection,
            thread_id,
            root_rollout_id,
            &started_at,
            root_turn_id,
        )?,
    };
    let batch = ActivityBatch::load(
        connection,
        thread_id,
        root_rollout_id,
        std::slice::from_ref(&root_scope),
    )?;
    let descendants = batch.descendants(root_turn_id, &started_at, next_started_at.as_deref());
    let counts = batch.counts(root_turn_id, &descendants);
    let mut group =
        query_exchange_groups(&batch, root_turn_id, root_rollout_id, &counts, &descendants)
            .into_iter()
            .find(|group| (group.kind == "review_group") == reviews);
    if let Some(group) = group.as_mut() {
        page_existing_activity_children(group, child_page, child_page_size);
    }
    Ok(group)
}

fn page_existing_activity_children(item: &mut ActivityItem, page: u64, page_size: u64) {
    let total = item.children.len() as u64;
    let total_pages = total.div_ceil(page_size).max(1);
    let page = page.min(total_pages).max(1);
    let start = page.saturating_sub(1).saturating_mul(page_size) as usize;
    let end = start
        .saturating_add(page_size as usize)
        .min(item.children.len());
    item.children = if start < item.children.len() {
        item.children[start..end].to_vec()
    } else {
        Vec::new()
    };
    item.child_page = Some(page);
    item.child_page_size = Some(page_size);
    item.child_total = Some(total);
    item.child_has_more = Some(page < total_pages);
}

fn query_activity_turn_totals_on(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
) -> Result<Totals> {
    connection
        .query_row(
            "SELECT COALESCE(SUM(input_tokens),0),
                    COALESCE(SUM(cached_input_tokens),0),COALESCE(SUM(output_tokens),0),
                    COALESCE(SUM(reasoning_tokens),0),COALESCE(SUM(total_tokens),0),
                    COALESCE(SUM(cost_usd),0.0),
                    COALESCE(SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END),0)
             FROM priced_usage WHERE thread_id=?1 AND turn_id=?2",
            params![thread_id, turn_id],
            |row| {
                Ok(Totals {
                    input_tokens: row.get::<_, i64>(0)?.max(0) as u64,
                    cached_input_tokens: row.get::<_, i64>(1)?.max(0) as u64,
                    output_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                    reasoning_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                    total_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                    known_cost_usd: row.get(5)?,
                    unpriced_tokens: row.get::<_, i64>(6)?.max(0) as u64,
                    ..Totals::default()
                }
                .finish())
            },
        )
        .map_err(Into::into)
}

fn query_activity_child_previews(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: Option<&str>,
) -> Result<Vec<ActivityItem>> {
    populate_activity_child_events(connection, thread_id, turn_id)?;
    query_activity_child_preview_rows(connection, thread_id, turn_id, -1, 0)
}

struct ActivityChildrenPage {
    items: Vec<ActivityItem>,
    page: u64,
    page_size: u64,
    total: u64,
    has_more: bool,
}

fn query_activity_child_previews_page(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: &str,
    page: u64,
    page_size: u64,
) -> Result<ActivityChildrenPage> {
    let total = populate_activity_child_events(connection, thread_id, Some(turn_id))?;
    let total_pages = total.div_ceil(page_size).max(1);
    let page = page.min(total_pages).max(1);
    let offset = page
        .saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64;
    let items = query_activity_child_preview_rows(
        connection,
        thread_id,
        Some(turn_id),
        page_size.min(i64::MAX as u64) as i64,
        offset,
    )?;
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more: page < total_pages,
    })
}

fn populate_activity_child_events(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: Option<&str>,
) -> Result<u64> {
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS selected_activity_child_events(
             event_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM selected_activity_child_events;",
    )?;
    let inserted = connection.execute(
        "WITH ranked_tool_calls AS MATERIALIZED (
             SELECT e.id,ROW_NUMBER() OVER (
                 PARTITION BY e.rollout_id,e.call_id
                 ORDER BY e.source_line,e.id
             ) lifecycle_rank
             FROM events e
             WHERE e.thread_id=?1 AND e.kind='tool_call' AND e.call_id IS NOT NULL
         ), first_tool_calls AS (
             SELECT id FROM ranked_tool_calls WHERE lifecycle_rank=1
         ), turns_with_final AS MATERIALIZED (
             SELECT DISTINCT final_event.turn_id
             FROM events final_event
             LEFT JOIN messages final_message
               ON final_message.id=COALESCE(final_event.call_id,final_event.id)
              AND final_message.thread_id=final_event.thread_id
             WHERE final_event.thread_id=?1
               AND (?2 IS NULL OR final_event.turn_id=?2)
               AND final_event.kind='final'
               AND trim(COALESCE(final_event.body,final_message.content,''))<>''
         )
         INSERT INTO selected_activity_child_events(event_id)
         SELECT e.id
         FROM events e
         LEFT JOIN first_tool_calls first_tool ON first_tool.id=e.id
         LEFT JOIN turns_with_final final ON final.turn_id=e.turn_id
         WHERE e.thread_id=?1 AND (?2 IS NULL OR e.turn_id=?2)
           AND e.kind NOT IN ('turn_started','system','tool_output','tool_completed')
           AND (e.kind<>'tool_call' OR e.call_id IS NULL OR first_tool.id IS NOT NULL)
           AND (e.kind<>'turn_completed' OR final.turn_id IS NULL)",
        params![thread_id, turn_id],
    )?;
    Ok(inserted as u64)
}

fn query_activity_child_preview_rows(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<ActivityItem>> {
    let mut statement = connection.prepare(
        "SELECT e.id,e.turn_id,e.rollout_id,e.agent_run_id,e.timestamp,e.kind,e.role,e.label,
                CASE WHEN e.kind='user' OR e.role='user' THEN
                    COALESCE(NULLIF(e.body,''),NULLIF(m.content,''))
                WHEN e.kind='tool_call' THEN NULL
                ELSE NULLIF(substr(COALESCE(NULLIF(e.body,''),NULLIF(m.content,'')),1,?3),'') END,
                COALESCE(tc.status,e.status),COALESCE(tc.name,e.tool_name),
                COALESCE(tc.duration_ms,e.duration_ms,
                    CASE WHEN tc.completed_at IS NOT NULL THEN
                        CAST(ROUND((julianday(tc.completed_at)-julianday(tc.started_at))*86400000.0)
                            AS INTEGER)
                    END),e.model,e.effort,
                CASE WHEN e.body IS NOT NULL OR m.content IS NOT NULL THEN 1 ELSE 0 END,
                a.nickname,a.agent_path,tc.namespace,e.source_line
         FROM selected_activity_child_events selected
         JOIN events e ON e.id=selected.event_id
         LEFT JOIN messages m
           ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
         LEFT JOIN tool_calls tc
           ON tc.rollout_id=e.rollout_id AND tc.call_id=e.call_id
          AND tc.thread_id=e.thread_id
         LEFT JOIN agent_runs a
           ON a.id=e.agent_run_id AND a.thread_id=e.thread_id
         WHERE e.thread_id=?1 AND (?2 IS NULL OR e.turn_id=?2)
         ORDER BY e.timestamp DESC,e.source_line DESC,e.id DESC
         LIMIT ?4 OFFSET ?5",
    )?;
    let rows = statement.query_map(
        params![thread_id, turn_id, ACTIVITY_PREVIEW_CHARS, limit, offset],
        |row| {
            let stored_kind: String = row.get(5)?;
            let role: Option<String> = row.get(6)?;
            let stored_tool_name = row.get::<_, Option<String>>(10)?;
            let tool_namespace = row.get::<_, Option<String>>(17)?;
            let kind = normalize_activity_kind(&stored_kind, role.as_deref());
            let body = row.get::<_, Option<String>>(8)?;
            let body = if kind == "user" {
                bounded_preview(body.and_then(|value| first_prompt_for_display(&value)))
            } else {
                bounded_preview(body)
            };
            Ok((
                ActivityItem {
                    id: row.get(0)?,
                    turn_id: row.get(1)?,
                    rollout_id: row.get(2)?,
                    agent_run_id: row.get(3)?,
                    agent_label: row
                        .get::<_, Option<String>>(15)?
                        .or(row.get::<_, Option<String>>(16)?),
                    timestamp: row.get(4)?,
                    kind,
                    role,
                    label: row.get(7)?,
                    body,
                    status: row.get(9)?,
                    tool_name: stored_tool_name
                        .map(|name| display_tool_name(tool_namespace.as_deref(), &name)),
                    duration_ms: row.get(11)?,
                    model: row.get(12)?,
                    effort: row.get(13)?,
                    has_details: row.get::<_, i64>(14)? != 0,
                    children: Vec::new(),
                    child_page: None,
                    child_page_size: None,
                    child_total: None,
                    child_has_more: None,
                    usage: None,
                    counts: None,
                },
                row.get::<_, i64>(18)?,
            ))
        },
    )?;
    let mut items = Vec::new();
    for row in rows {
        let (item, source_line) = row?;
        items.push((item, source_line));
    }
    attribute_activity_usage(connection, thread_id, turn_id, &mut items)?;
    Ok(items.into_iter().map(|(item, _)| item).collect())
}

fn attribute_activity_usage(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: Option<&str>,
    items: &mut [(ActivityItem, i64)],
) -> Result<()> {
    let owners = items
        .iter()
        .enumerate()
        .filter(|(_, (item, _))| {
            matches!(
                item.kind.as_str(),
                "assistant" | "update" | "final" | "reasoning" | "tool" | "subagent"
            )
        })
        .map(|(index, (item, source_line))| {
            (
                (item.rollout_id.clone(), item.turn_id.clone(), *source_line),
                index,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut statement = connection.prepare(
        "SELECT rollout_id,turn_id,source_line,input_tokens,cached_input_tokens,
                output_tokens,reasoning_tokens,total_tokens,COALESCE(cost_usd,0.0),
                CASE WHEN price_known=0 THEN total_tokens ELSE 0 END
         FROM priced_usage
         WHERE thread_id=?1 AND (?2 IS NULL OR turn_id=?2)
         ORDER BY timestamp,source_line,id",
    )?;
    let usage_rows = statement.query_map(params![thread_id, turn_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, i64>(2)?,
            Totals {
                input_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                cached_input_tokens: row.get::<_, i64>(4)?.max(0) as u64,
                output_tokens: row.get::<_, i64>(5)?.max(0) as u64,
                reasoning_tokens: row.get::<_, i64>(6)?.max(0) as u64,
                total_tokens: row.get::<_, i64>(7)?.max(0) as u64,
                known_cost_usd: row.get(8)?,
                unpriced_tokens: row.get::<_, i64>(9)?.max(0) as u64,
                ..Totals::default()
            },
        ))
    })?;
    for usage_row in usage_rows {
        let (rollout_id, usage_turn_id, source_line, usage) = usage_row?;
        let owner_index = [1_i64, 2_i64].into_iter().find_map(|distance| {
            owners
                .get(&(
                    rollout_id.clone(),
                    usage_turn_id.clone(),
                    source_line.saturating_sub(distance),
                ))
                .copied()
        });
        if let Some(owner_index) = owner_index {
            add_activity_usage(
                items[owner_index]
                    .0
                    .usage
                    .get_or_insert_with(Totals::default),
                &usage,
            );
        }
    }
    for (item, _) in items {
        if let Some(usage) = item.usage.take() {
            item.usage = Some(usage.finish());
        }
    }
    Ok(())
}

fn bounded_preview(value: Option<String>) -> Option<String> {
    let value = redact_data_urls(value?.trim());
    if value.is_empty() {
        return None;
    }
    let mut chars = value.chars();
    let mut preview = chars
        .by_ref()
        .take(ACTIVITY_PREVIEW_CHARS as usize)
        .collect::<String>();
    if chars.next().is_some() {
        preview.push('…');
    }
    Some(preview)
}

fn normalize_activity_kind(kind: &str, role: Option<&str>) -> String {
    match kind {
        "message" if role == Some("user") => "user",
        "message" => "final",
        "turn_completed" => "final",
        "tool_call" => "tool",
        "tool_output" | "tool_completed" => "tool_result",
        "state" => "system",
        other => other,
    }
    .to_owned()
}

type StatsBucket = (DateTime<Utc>, DateTime<Utc>, String);

// This is a corruption/adversarial-input guard rather than a retention rule.
// Normal Codex history spans only a few years, while an unbounded loop here
// can manufacture an arbitrarily large JSON response from one bad timestamp.
const MAX_ALL_TIME_YEAR_BUCKETS: i32 = 200;

fn stats_buckets_on(
    connection: &Connection,
    range: &str,
    anchor: NaiveDate,
) -> Result<Vec<StatsBucket>> {
    let mut buckets = Vec::new();
    match range {
        "day" => {
            let start = local_midnight(anchor);
            let end = local_midnight(anchor + Duration::days(1));
            let mut cursor = start;
            while cursor < end {
                let next = (cursor + Duration::hours(1)).min(end);
                buckets.push((
                    cursor,
                    next,
                    cursor.with_timezone(&Local).format("%H:00").to_string(),
                ));
                cursor = next;
            }
        }
        "week" => {
            let monday = anchor - Duration::days(anchor.weekday().num_days_from_monday() as i64);
            for offset in 0..7 {
                let date = monday + Duration::days(offset);
                buckets.push((
                    local_midnight(date),
                    local_midnight(date + Duration::days(1)),
                    date.format("%a %-d").to_string(),
                ));
            }
        }
        "month" => {
            let mut date = NaiveDate::from_ymd_opt(anchor.year(), anchor.month(), 1)
                .context("invalid month")?;
            let end = date
                .checked_add_months(Months::new(1))
                .context("invalid month")?;
            while date < end {
                buckets.push((
                    local_midnight(date),
                    local_midnight(date + Duration::days(1)),
                    date.format("%Y-%m-%d").to_string(),
                ));
                date += Duration::days(1);
            }
        }
        "year" => {
            for month in 1..=12 {
                let date =
                    NaiveDate::from_ymd_opt(anchor.year(), month, 1).context("invalid year")?;
                let next = date
                    .checked_add_months(Months::new(1))
                    .context("invalid month")?;
                buckets.push((
                    local_midnight(date),
                    local_midnight(next),
                    date.format("%b").to_string(),
                ));
            }
        }
        _ => {
            let first: Option<String> = connection.query_row(
                "SELECT MIN(timestamp) FROM (
                    SELECT MIN(timestamp) timestamp FROM events
                    UNION ALL SELECT MIN(timestamp) FROM usage_facts
                    UNION ALL SELECT MIN(timestamp) FROM messages
                 )",
                [],
                |row| row.get(0),
            )?;
            let first_year = first
                .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
                .map(|value| value.with_timezone(&Local).year())
                .unwrap_or(anchor.year());
            let year_count = anchor
                .year()
                .checked_sub(first_year)
                .and_then(|difference| difference.checked_add(1))
                .context("invalid all-time year span")?;
            anyhow::ensure!(
                (1..=MAX_ALL_TIME_YEAR_BUCKETS).contains(&year_count),
                "all-time usage spans {year_count} years; maximum supported span is {MAX_ALL_TIME_YEAR_BUCKETS}"
            );
            for year in first_year..=anchor.year() {
                let date = NaiveDate::from_ymd_opt(year, 1, 1).context("invalid year")?;
                let next = NaiveDate::from_ymd_opt(year + 1, 1, 1).context("invalid year")?;
                buckets.push((local_midnight(date), local_midnight(next), year.to_string()));
            }
        }
    }
    Ok(buckets)
}

fn query_prices_on(
    connection: &Connection,
    q: Option<&str>,
    page: u64,
    page_size: u64,
) -> Result<PricesResponse> {
    let pattern = q
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("%{}%", value.trim().to_lowercase()));
    let total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM resolved_model_prices
         WHERE ?1 IS NULL OR lower(model_id) LIKE ?1",
        [pattern.as_deref()],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model_id,effective_from,effective_to,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,currency,source
         FROM resolved_model_prices
         WHERE ?1 IS NULL OR lower(model_id) LIKE ?1
         ORDER BY model_id,effective_from DESC LIMIT ?2 OFFSET ?3",
    )?;
    let raw_items = statement
        .query_map(
            params![
                pattern,
                page_size as i64,
                price_page_offset(page, page_size)
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let items = raw_items
        .into_iter()
        .map(
            |(model_id, effective_from, effective_to, input, cached, output, currency, source)| {
                Ok(PriceRow {
                    model_id,
                    effective_from,
                    effective_to,
                    input_per_million: PriceMicros::from_raw(input)?.decimal_string(),
                    cached_input_per_million: cached
                        .map(PriceMicros::from_raw)
                        .transpose()?
                        .map(PriceMicros::decimal_string),
                    output_per_million: PriceMicros::from_raw(output)?.decimal_string(),
                    currency,
                    source: public_price_source(&source),
                })
            },
        )
        .collect::<Result<Vec<_>>>()?;
    let mut statement = connection.prepare(
        "SELECT observed_model_id,canonical_model_id
         FROM resolved_model_aliases ORDER BY observed_model_id",
    )?;
    let aliases = statement
        .query_map([], |row| {
            Ok(AliasRow {
                observed_model_id: row.get(0)?,
                canonical_model_id: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut statement = connection.prepare(
        "SELECT model,COUNT(*),SUM(total_tokens),MAX(timestamp) FROM priced_usage
         WHERE price_known=0 GROUP BY model ORDER BY SUM(total_tokens) DESC",
    )?;
    let observed_unknown = statement
        .query_map([], |row| {
            Ok(UnknownModelRow {
                model_id: row.get(0)?,
                usage_count: row.get::<_, i64>(1)?.max(0) as u64,
                total_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                last_seen_at: row.get(3)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let total = total.max(0) as u64;
    let last_refresh_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_refresh_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let last_refresh_error_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_error_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let refresh_error = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_error'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|value| value.chars().take(512).collect());
    let refresh_error_kind = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_error_kind'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let source = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_source_url'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(PricesResponse {
        items,
        aliases,
        observed_unknown,
        page,
        page_size,
        total,
        total_pages: total.div_ceil(page_size),
        last_refresh_at,
        last_refresh_error_at,
        refresh_error_kind,
        refresh_error,
        source,
    })
}

fn public_price_source(source: &str) -> String {
    source.strip_prefix("remote:").unwrap_or(source).to_owned()
}

fn price_page_offset(page: u64, page_size: u64) -> i64 {
    page.saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64
}

fn query_bounds(
    date: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> ApiResult<(Option<String>, Option<String>)> {
    if let Some(date) = date {
        let date = parse_date(date)?;
        return Ok((
            Some(sql_timestamp(local_midnight(date))),
            Some(sql_timestamp(local_midnight(date + Duration::days(1)))),
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
        return Ok(sql_timestamp(timestamp.with_timezone(&Utc)));
    }
    let date = parse_date(value)?;
    let date = if inclusive_date_end {
        date + Duration::days(1)
    } else {
        date
    };
    Ok(sql_timestamp(local_midnight(date)))
}

fn sql_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_date(value: &str) -> ApiResult<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| ApiError::bad_request("expected a YYYY-MM-DD date"))
}

fn parse_timestamp(value: &str) -> ApiResult<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(local_midnight(date));
    }
    Err(ApiError::bad_request(
        "expected RFC3339 timestamp or YYYY-MM-DD",
    ))
}

fn canonical_timestamp(value: &str) -> ApiResult<String> {
    Ok(parse_timestamp(value)?.to_rfc3339_opts(SecondsFormat::Nanos, true))
}

fn local_midnight(date: NaiveDate) -> DateTime<Utc> {
    let naive = date.and_hms_opt(0, 0, 0).expect("midnight is valid");
    match Local.from_local_datetime(&naive) {
        LocalResult::Single(value) => value.with_timezone(&Utc),
        LocalResult::Ambiguous(first, _) => first.with_timezone(&Utc),
        LocalResult::None => Local
            .from_local_datetime(&(naive + Duration::hours(1)))
            .earliest()
            .unwrap_or_else(Local::now)
            .with_timezone(&Utc),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApiState, BUCKET_AGGREGATES_SQL, OVERVIEW_YEAR_USAGE_SQL, PricesQuery,
        STATS_BUCKET_SESSIONS_SQL, STATS_BUCKET_USAGE_SQL, STATS_FEW_BUCKET_SESSIONS_SQL,
        SqlBucketBounds, StatsBucketAggregate, activity_day_window, display_tool_name,
        first_prompt_for_display, price_page_offset, prices, query_activity_day_summaries_batched,
        query_activity_detail_on, query_activity_on, query_heatmap_on, query_overview_year_on,
        query_stats_on, run_snapshot_work, settings, stats_totals_from_aggregates,
    };
    use crate::{
        config::PricingConfig,
        db::Db,
        db_executor::{DbExecutor, WorkClass},
        ingest::IngestRoots,
        model::Totals,
    };
    use axum::extract::{Query, State};
    use chrono::NaiveDate;
    use rusqlite::params;
    use std::{
        collections::HashSet,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration as StdDuration, Instant},
    };

    static TRACE_LOCK: Mutex<()> = Mutex::new(());
    static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);
    static OVERVIEW_USAGE_QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_query(sql: &str) {
        let sql = sql.trim_start();
        if sql.starts_with("SELECT") || sql.starts_with("WITH") {
            QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if sql.contains("overview-year-usage") {
            OVERVIEW_USAGE_QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn price_pagination_offset_saturates_before_sql_conversion() {
        assert_eq!(price_page_offset(1, 25), 0);
        assert_eq!(price_page_offset(3, 25), 50);
        assert_eq!(price_page_offset(u64::MAX, 100), i64::MAX);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settings_and_prices_wait_for_the_heavy_executor_lane() {
        use tokio::sync::oneshot;

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let executor = DbExecutor::new(2, 1);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel();
        let blocker = {
            let executor = executor.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                executor
                    .run(WorkClass::Heavy, move || {
                        blocker_started_tx.send(()).unwrap();
                        let (lock, ready) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = ready.wait(released).unwrap();
                        }
                        Ok(())
                    })
                    .await
            })
        };
        blocker_started_rx.await.unwrap();

        let state = ApiState::with_executor(
            db,
            IngestRoots {
                active: None,
                archive: None,
            },
            temp.path().join("frontend"),
            PricingConfig {
                url: "http://127.0.0.1:9/prices.json".into(),
                refresh_interval_hours: 24,
                timeout_seconds: 1,
            },
            executor,
        );
        let (settings_entered_tx, settings_entered_rx) = oneshot::channel();
        let settings_task = {
            let state = state.clone();
            tokio::spawn(async move {
                settings_entered_tx.send(()).unwrap();
                settings(State(state)).await
            })
        };
        let (prices_entered_tx, prices_entered_rx) = oneshot::channel();
        let prices_task = {
            let state = state.clone();
            tokio::spawn(async move {
                prices_entered_tx.send(()).unwrap();
                prices(
                    State(state),
                    Query(PricesQuery {
                        q: None,
                        page: Some(1),
                        page_size: Some(25),
                    }),
                )
                .await
            })
        };
        settings_entered_rx.await.unwrap();
        prices_entered_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!settings_task.is_finished());
        assert!(!prices_task.is_finished());

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        blocker.await.unwrap().unwrap();
        let _ = settings_task.await.unwrap().unwrap();
        let _ = prices_task.await.unwrap().unwrap();
    }

    #[test]
    fn analytical_bucket_queries_have_constant_statement_budgets() {
        let _trace_guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection.trace(Some(count_query));

        QUERY_COUNT.store(0, Ordering::SeqCst);
        let heatmap = query_heatmap_on(&connection, 2026).unwrap();
        assert_eq!(heatmap.len(), 365);
        assert_eq!(QUERY_COUNT.swap(0, Ordering::SeqCst), 1);

        OVERVIEW_USAGE_QUERY_COUNT.store(0, Ordering::SeqCst);
        let overview = query_overview_year_on(
            &connection,
            2026,
            "2025-12-31T23:00:00.000000000Z",
            "2026-12-31T23:00:00.000000000Z",
        )
        .unwrap();
        assert_eq!(overview.heatmap.len(), 365);
        assert_eq!(OVERVIEW_USAGE_QUERY_COUNT.swap(0, Ordering::SeqCst), 1);
        QUERY_COUNT.store(0, Ordering::SeqCst);

        let anchor = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let stats = query_stats_on(&connection, "month", anchor).unwrap();
        assert_eq!(stats.rows.len(), 31);
        assert_eq!(QUERY_COUNT.swap(0, Ordering::SeqCst), 4);

        let stats = query_stats_on(&connection, "all", anchor).unwrap();
        assert_eq!(stats.rows.len(), 1);
        assert_eq!(QUERY_COUNT.swap(0, Ordering::SeqCst), 5);
        connection.trace(None);
    }

    #[test]
    fn broad_stats_buckets_use_thread_index_probes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let anchor = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();

        let year = super::stats_buckets_on(&connection, "year", anchor).unwrap();
        assert!(super::stats_buckets_are_broad(&year));

        let month = super::stats_buckets_on(&connection, "month", anchor).unwrap();
        assert!(!super::stats_buckets_are_broad(&month));

        let week = super::stats_buckets_on(&connection, "week", anchor).unwrap();
        assert!(!super::stats_buckets_are_broad(&week));
    }

    #[test]
    fn stats_grand_total_converts_fixed_point_once() {
        let aggregates = (0..10)
            .map(|_| StatsBucketAggregate {
                totals: Totals {
                    total_tokens: 1,
                    known_cost_usd: 0.1,
                    ..Totals::default()
                }
                .finish(),
                session_count: 1,
                known_cost_numerator: 100_000_000_000,
            })
            .collect::<Vec<_>>();

        let totals = stats_totals_from_aggregates(&aggregates);
        let exact_once = 1.0_f64;
        let floating_bucket_sum = aggregates
            .iter()
            .map(|aggregate| aggregate.totals.known_cost_usd)
            .sum::<f64>();

        assert_eq!(totals.known_cost_usd.to_bits(), exact_once.to_bits());
        assert_ne!(floating_bucket_sum.to_bits(), exact_once.to_bits());
    }

    #[test]
    fn all_time_stats_reject_implausibly_large_data_derived_year_spans() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('ancient-thread','Ancient','1826-01-01T00:00:00.000000000Z',
                        '1826-01-01T00:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('ancient-rollout','ancient-thread',
                        '1826-01-01T00:00:00.000000000Z',
                        '1826-01-01T00:00:00.000000000Z',0);
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,native
                 ) VALUES(
                    'ancient-event','ancient-thread','ancient-rollout',
                    '1826-01-01T00:00:00.000000000Z',1,'state',1
                 );",
            )
            .unwrap();

        let error = super::stats_buckets_on(
            &connection,
            "all",
            NaiveDate::from_ymd_opt(2026, 7, 19).unwrap(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("maximum supported span is 200"),
            "unexpected all-time guard error: {error:#}"
        );
    }

    #[tokio::test]
    async fn dropping_snapshot_work_interrupts_the_running_sqlite_query() {
        struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let state = ApiState::with_executor(
            db.clone(),
            IngestRoots {
                active: None,
                archive: None,
            },
            temp.path().join("frontend"),
            PricingConfig {
                url: "http://127.0.0.1:9/prices.json".into(),
                refresh_interval_hours: 24,
                timeout_seconds: 1,
            },
            DbExecutor::default(),
        );
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
                let _done = NotifyOnDrop(Some(done_tx));
                let _ = started_tx.send(());
                let _: i64 = connection.query_row(
                    "WITH RECURSIVE counter(value) AS (
                         VALUES(0) UNION ALL
                         SELECT value+1 FROM counter WHERE value<100000000
                     ) SELECT SUM(value) FROM counter",
                    [],
                    |row| row.get(0),
                )?;
                Ok(())
            })
            .await
        });

        started_rx.await.unwrap();
        task.abort();
        tokio::time::timeout(std::time::Duration::from_secs(2), done_rx)
            .await
            .expect("SQLite query kept running after its request was cancelled")
            .unwrap();
        let _ = task.await;
    }

    #[test]
    fn heatmap_preserves_empty_days_around_sparse_usage() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();

        let empty = query_heatmap_on(&connection, 2026).unwrap();
        assert_eq!(empty.len(), 365);
        assert!(empty.iter().all(|day| {
            day.cost_usd == Some(0.0)
                && day.session_count == 0
                && day.message_count == 0
                && day.total_tokens == 0
        }));

        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('sparse-thread','Sparse heatmap',
                        '2026-07-15T12:00:00.000000000Z',
                        '2026-07-15T12:02:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('sparse-rollout','sparse-thread',
                        '2026-07-15T12:00:00.000000000Z',
                        '2026-07-15T12:02:00.000000000Z',0);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES(
                    'sparse-usage','sparse-thread','sparse-rollout',
                    '2026-07-15T12:01:00.000000000Z',1,'gpt-5.5',
                    100,50,10,2,110,1
                 );
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES(
                    'sparse-message','sparse-thread','sparse-rollout',
                    '2026-07-15T12:02:00.000000000Z','user','Sparse fixture',2
                 );",
            )
            .unwrap();

        let sparse = query_heatmap_on(&connection, 2026).unwrap();
        assert_eq!(sparse.len(), 365);
        let populated = sparse.iter().find(|day| day.date == "2026-07-15").unwrap();
        assert_eq!(populated.session_count, 1);
        assert_eq!(populated.message_count, 1);
        assert_eq!(populated.total_tokens, 110);
        assert!(populated.cost_usd.unwrap() > 0.0);
        assert!(sparse.iter().filter(|day| day.total_tokens > 0).count() == 1);
        assert!(sparse.iter().filter(|day| day.message_count > 0).count() == 1);
    }

    #[test]
    fn overview_activity_counts_messages_and_event_only_session_days_exactly() {
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
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,body,native
                 ) VALUES
                    ('event-1','event-only','event-rollout',
                     '2026-07-15T10:00:00.000000000Z',1,'state','one',1),
                    ('event-2','event-only','event-rollout',
                     '2026-07-15T11:00:00.000000000Z',2,'state','two',1),
                    ('event-3','event-only','event-rollout',
                     '2026-07-16T10:00:00.000000000Z',3,'state','three',1);
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES
                    ('message-1','message-only','message-rollout',
                     '2026-07-15T12:00:00.000000000Z','user','one',1),
                    ('message-2','message-only','message-rollout',
                     '2026-07-15T12:01:00.000000000Z','assistant','two',2);",
            )
            .unwrap();

        let buckets = super::overview_year_buckets(2026).unwrap();
        let mut sessions = vec![HashSet::new(); buckets.len()];
        let message_counts =
            super::query_overview_year_activity_on(&connection, &buckets, &mut sessions).unwrap();
        let july_15 = buckets
            .iter()
            .position(|(_, _, label)| label == "2026-07-15")
            .unwrap();
        let july_16 = buckets
            .iter()
            .position(|(_, _, label)| label == "2026-07-16")
            .unwrap();

        assert_eq!(message_counts[july_15], 2);
        assert_eq!(message_counts[july_16], 0);
        assert_eq!(
            sessions[july_15],
            HashSet::from(["event-only".into(), "message-only".into()])
        );
        assert_eq!(sessions[july_16], HashSet::from(["event-only".into()]));
        assert_eq!(sessions.iter().map(HashSet::len).sum::<usize>(), 3);
    }

    #[test]
    fn optimized_price_book_honors_layer_precedence_before_effective_date() {
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
                    ('layered-model','2026-01-01T00:00:00.000000000Z',NULL,
                     9000000,9000000,9000000,'USD','remote:https://example.test/prices'),
                    ('layered-model','1970-01-01T00:00:00.000000000Z',NULL,
                     3000000,3000000,3000000,'USD','manual');",
            )
            .unwrap();

        let (aliases, prices) = super::overview_prices_on(&connection).unwrap();
        let (_, selected) = super::overview_price_for(
            &aliases,
            &prices,
            "layered-model",
            "2026-07-15T12:00:00.000000000Z",
        )
        .unwrap();
        assert_eq!(selected.input_microusd_per_million, 3_000_000);
    }

    #[test]
    fn grouped_pricing_matches_priced_usage_across_boundaries_and_gaps() {
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

        let buckets = super::overview_year_buckets(2026).unwrap();
        let (_, sessions, _) = super::query_overview_year_usage_on(&connection, &buckets).unwrap();
        for thread_id in ["boundary-thread", "gap-thread"] {
            let expected = connection
                .query_row(
                    "SELECT COALESCE(SUM(cost_usd),0.0),
                            COALESCE(SUM(CASE WHEN price_known=0
                                              THEN total_tokens ELSE 0 END),0),
                            COALESCE(SUM(total_tokens),0)
                     FROM priced_usage WHERE thread_id=?1",
                    [thread_id],
                    |row| {
                        Ok((
                            row.get::<_, f64>(0)?,
                            row.get::<_, i64>(1)?.max(0) as u64,
                            row.get::<_, i64>(2)?.max(0) as u64,
                        ))
                    },
                )
                .unwrap();
            let actual = sessions.get(thread_id).unwrap();
            assert!((actual.known_cost_usd() - expected.0).abs() < 1e-12);
            assert_eq!(actual.unpriced_tokens, expected.1);
            assert_eq!(actual.total_tokens, expected.2);
        }
        assert_eq!(sessions["boundary-thread"].unpriced_tokens, 0);
        assert_eq!(sessions["gap-thread"].unpriced_tokens, 110);

        let sorted =
            super::query_sessions_on(&connection, None, None, None, None, "cost", 1, 50, false)
                .unwrap();
        assert_eq!(sorted.items.len(), 2);
        assert_eq!(sorted.items[0].id, "boundary-thread");
        assert_eq!(sorted.items[1].id, "gap-thread");
        for item in &sorted.items {
            let expected = connection
                .query_row(
                    "SELECT COALESCE(SUM(cost_usd),0.0),
                            COALESCE(SUM(CASE WHEN price_known=0
                                              THEN total_tokens ELSE 0 END),0),
                            COALESCE(SUM(total_tokens),0)
                     FROM priced_usage WHERE thread_id=?1",
                    [&item.id],
                    |row| {
                        Ok((
                            row.get::<_, f64>(0)?,
                            row.get::<_, i64>(1)?.max(0) as u64,
                            row.get::<_, i64>(2)?.max(0) as u64,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(item.unpriced_tokens, expected.1);
            assert_eq!(item.total_tokens, expected.2);
            if expected.1 == 0 {
                assert!((item.cost_usd.unwrap() - expected.0).abs() < 1e-12);
            } else {
                assert_eq!(item.cost_usd, None);
            }
        }

        let expected = connection
            .query_row(
                "SELECT COALESCE(SUM(cost_usd),0.0),
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
                        row.get::<_, f64>(0)?,
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
        let stats = query_stats_on(
            &connection,
            "year",
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap(),
        )
        .unwrap();
        assert!((stats.totals.known_cost_usd - expected.0).abs() < 1e-12);
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
        let repriced =
            super::query_sessions_on(&connection, None, None, None, None, "cost", 1, 50, false)
                .unwrap();
        assert_eq!(repriced.items[0].id, "gap-thread");
        assert_eq!(repriced.items[0].unpriced_tokens, 0);
        let expected_gap_cost: f64 = connection
            .query_row(
                "SELECT COALESCE(SUM(cost_usd),0.0)
                 FROM priced_usage WHERE thread_id='gap-thread'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!((repriced.items[0].cost_usd.unwrap() - expected_gap_cost).abs() < 1e-12);
    }

    #[test]
    fn overview_summary_matches_legacy_pricing_and_nested_activity_at_month_boundary() {
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
                    ('summary-model','2026-07-01T00:00:00.000000000Z',NULL,
                     1000000,500000,2000000,'USD','manual'),
                    ('summary-model','2026-07-31T12:00:00.000000000Z',
                     '2026-07-31T13:00:00.000000000Z',
                     2000000,1000000,4000000,'USD','manual'),
                    ('gap-model','2026-07-01T00:00:00.000000000Z',
                     '2026-07-31T12:00:00.000000000Z',
                     1000000,500000,2000000,'USD','manual'),
                    ('gap-model','2026-07-31T13:00:00.000000000Z',NULL,
                     3000000,1500000,6000000,'USD','manual');
                 INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES(
                    'summary-alias','summary-model',
                    '2026-07-01T00:00:00.000000000Z','manual'
                 );
                 INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                    ('usage-thread','Usage','2026-07-21T10:00:00.000000000Z',
                     '2026-08-01T10:00:00.000000000Z'),
                    ('event-spill','Event spill','2026-07-29T10:00:00.000000000Z',
                     '2026-07-29T10:00:00.000000000Z'),
                    ('message-spill','Message spill','2026-07-30T10:00:00.000000000Z',
                     '2026-07-30T10:00:00.000000000Z'),
                    ('today-message','Today message','2026-08-01T11:00:00.000000000Z',
                     '2026-08-01T11:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('usage-rollout','usage-thread','2026-07-21T10:00:00.000000000Z',
                     '2026-08-01T10:00:00.000000000Z',0),
                    ('event-rollout','event-spill','2026-07-29T10:00:00.000000000Z',
                     '2026-07-29T10:00:00.000000000Z',0),
                    ('spill-rollout','message-spill','2026-07-30T10:00:00.000000000Z',
                     '2026-07-30T10:00:00.000000000Z',0),
                    ('today-rollout','today-message','2026-08-01T11:00:00.000000000Z',
                     '2026-08-01T11:00:00.000000000Z',0);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES
                    ('usage-prior','usage-thread','usage-rollout',
                     '2026-07-21T10:00:00.000000000Z',1,'summary-alias',100,20,10,3,110,1),
                    ('usage-boundary','usage-thread','usage-rollout',
                     '2026-07-31T12:30:00.000000000Z',2,'summary-alias',200,40,20,6,220,1),
                    ('usage-gap','usage-thread','usage-rollout',
                     '2026-07-31T12:30:01.000000000Z',3,'gap-model',50,10,5,2,55,1),
                    ('usage-today','usage-thread','usage-rollout',
                     '2026-08-01T10:00:00.000000000Z',4,'summary-alias',300,60,30,9,330,1);
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,body,native
                 ) VALUES('event-spill-1','event-spill','event-rollout',
                          '2026-07-29T10:00:00.000000000Z',1,'state','spill',1);
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES
                    ('message-spill-1','message-spill','spill-rollout',
                     '2026-07-30T10:00:00.000000000Z','user','spill',1),
                    ('message-today-1','today-message','today-rollout',
                     '2026-08-01T11:00:00.000000000Z','user','today',1);",
            )
            .unwrap();

        // August 1, 2026 is a Saturday: the current week begins in July,
        // before the current month. This is the edge that a month-only scan misses.
        let bounds = vec![
            SqlBucketBounds {
                ordinal: 0,
                start_at: "2026-08-01T00:00:00.000000000Z".into(),
                end_at: "2026-08-02T00:00:00.000000000Z".into(),
            },
            SqlBucketBounds {
                ordinal: 1,
                start_at: "2026-07-31T00:00:00.000000000Z".into(),
                end_at: "2026-08-01T00:00:00.000000000Z".into(),
            },
            SqlBucketBounds {
                ordinal: 2,
                start_at: "2026-07-27T00:00:00.000000000Z".into(),
                end_at: "2026-08-02T00:00:00.000000000Z".into(),
            },
            SqlBucketBounds {
                ordinal: 3,
                start_at: "2026-07-20T00:00:00.000000000Z".into(),
                end_at: "2026-07-27T00:00:00.000000000Z".into(),
            },
            SqlBucketBounds {
                ordinal: 4,
                start_at: "2026-08-01T00:00:00.000000000Z".into(),
                end_at: "2026-08-02T00:00:00.000000000Z".into(),
            },
            SqlBucketBounds {
                ordinal: 5,
                start_at: "2026-07-01T00:00:00.000000000Z".into(),
                end_at: "2026-08-01T00:00:00.000000000Z".into(),
            },
        ];
        let actual = super::query_overview_summary_usage_on(&connection, &bounds).unwrap();
        for (bound, actual) in bounds.iter().zip(&actual) {
            let expected = super::query_totals_on(
                &connection,
                Some(&bound.start_at),
                Some(&bound.end_at),
                None,
            )
            .unwrap();
            assert_eq!(actual.input_tokens, expected.input_tokens);
            assert_eq!(actual.cached_input_tokens, expected.cached_input_tokens);
            assert_eq!(actual.output_tokens, expected.output_tokens);
            assert_eq!(actual.reasoning_tokens, expected.reasoning_tokens);
            assert_eq!(actual.total_tokens, expected.total_tokens);
            assert_eq!(actual.unpriced_tokens, expected.unpriced_tokens);
            assert_eq!(actual.cost_usd.is_some(), expected.cost_usd.is_some());
            assert!((actual.known_cost_usd - expected.known_cost_usd).abs() < 1e-12);
        }
        assert!(actual[0].pricing_complete);
        assert!(!actual[1].pricing_complete);
        assert!(!actual[2].pricing_complete);
        assert!(!actual[5].pricing_complete);

        let sessions = super::query_overview_summary_sessions_on(&connection, &bounds).unwrap();
        let messages = super::query_overview_summary_messages_on(&connection, &bounds).unwrap();
        assert_eq!(sessions, [2, 4, 2]);
        assert_eq!(messages, [1, 2, 1]);

        let today = super::overview_period_summary(
            "Today",
            &bounds[0],
            actual[0].clone(),
            &actual[1],
            sessions[0],
            messages[0],
        );
        assert_eq!(today.session_count, 2);
        assert_eq!(today.message_count, 1);
        assert_eq!(today.delta_cost_usd, None);
        assert_eq!(today.delta_percent, None);
        let priced_delta = super::overview_period_summary(
            "Today",
            &bounds[0],
            actual[0].clone(),
            &actual[3],
            sessions[0],
            messages[0],
        );
        let expected_delta = actual[0].cost_usd.unwrap() - actual[3].cost_usd.unwrap();
        assert!((priced_delta.delta_cost_usd.unwrap() - expected_delta).abs() < 1e-12);
        assert!(priced_delta.delta_percent.is_some());
    }

    #[test]
    fn overview_summary_queries_use_bounded_indexes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let usage_plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                super::OVERVIEW_SUMMARY_USAGE_SQL
            ))
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

        let bounds = (0..6)
            .map(|ordinal| SqlBucketBounds {
                ordinal,
                start_at: "2026-07-01T00:00:00.000000000Z".into(),
                end_at: "2026-08-02T00:00:00.000000000Z".into(),
            })
            .collect::<Vec<_>>();
        let bounds = serde_json::to_string(&bounds).unwrap();
        let session_plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                super::OVERVIEW_SUMMARY_SESSIONS_SQL
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
                "EXPLAIN QUERY PLAN {}",
                super::OVERVIEW_SUMMARY_MESSAGES_SQL
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
    fn bucket_query_ranges_usage_by_timestamp_without_materializing_priced_usage() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let bounds_json = serde_json::to_string(&[SqlBucketBounds {
            ordinal: 0,
            start_at: "2026-07-15T00:00:00.000000000Z".into(),
            end_at: "2026-07-16T00:00:00.000000000Z".into(),
        }])
        .unwrap();
        let explain = format!("EXPLAIN QUERY PLAN {BUCKET_AGGREGATES_SQL}");
        let mut statement = connection.prepare(&explain).unwrap();
        let plan = statement
            .query_map([bounds_json], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");

        assert!(
            plan.contains("SEARCH u USING INDEX idx_usage_time"),
            "usage facts are not timestamp-indexed:\n{plan}"
        );
        assert!(
            !plan.contains("MATERIALIZE exact_usage"),
            "priced_usage is materialized before bucket filtering:\n{plan}"
        );
        assert!(
            !plan.contains("SCAN exact_usage"),
            "materialized priced usage is scanned for every bucket:\n{plan}"
        );
    }

    #[test]
    fn overview_year_usage_uses_covering_day_ranges_without_materializing_pricing() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let explain = format!("EXPLAIN QUERY PLAN {OVERVIEW_YEAR_USAGE_SQL}");
        let mut statement = connection.prepare(&explain).unwrap();
        let bounds_json = serde_json::to_string(&[SqlBucketBounds {
            ordinal: 0,
            start_at: "2025-12-31T23:00:00.000000000Z".into(),
            end_at: "2026-12-31T23:00:00.000000000Z".into(),
        }])
        .unwrap();
        let plan = statement
            .query_map([bounds_json], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");

        assert!(
            plan.contains("USING COVERING INDEX idx_usage_overview_year"),
            "overview usage facts are not read from the annual covering index:\n{plan}"
        );
    }

    #[test]
    fn overview_event_day_seek_uses_thread_time_index() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let plan = connection
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT timestamp FROM events
                 WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3
                 ORDER BY timestamp LIMIT 1",
            )
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
            plan.contains("idx_events_thread_time"),
            "overview event days are not sought by thread/time:\n{plan}"
        );
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

    fn seed_activity_roots(connection: &rusqlite::Connection, thread_id: &str, roots: usize) {
        connection
            .execute_batch(&format!(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('{thread_id}','Query budget','2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T01:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('{thread_id}','{thread_id}','2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T01:00:00.000000000Z',0);"
            ))
            .unwrap();
        for index in 0..roots {
            let minute = index + 1;
            connection
                .execute_batch(&format!(
                    "INSERT INTO turns(
                        id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
                     ) VALUES(
                        'root-{index}','{thread_id}','{thread_id}',
                        '2026-07-01T00:{minute:02}:00.000000000Z',
                        '2026-07-01T00:{minute:02}:30.000000000Z','completed',30000
                     );
                     INSERT INTO events(
                        id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                     ) VALUES(
                        'user-{index}','{thread_id}','{thread_id}','root-{index}',
                        '2026-07-01T00:{minute:02}:00.000000000Z',1,'user','user',
                        'Request {index}',1
                     );
                     INSERT INTO usage_facts(
                        id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                        input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                        total_tokens,native
                     ) VALUES(
                        'usage-{index}','{thread_id}','{thread_id}','root-{index}',
                        '2026-07-01T00:{minute:02}:10.000000000Z',2,'gpt-5.5',
                        100,50,10,2,110,1
                     );"
                ))
                .unwrap();
        }
    }

    fn seed_activity_descendants(
        connection: &rusqlite::Connection,
        thread_id: &str,
        start: usize,
        count: usize,
    ) {
        for index in start..start + count {
            connection
                .execute_batch(&format!(
                    "INSERT INTO rollouts(
                        id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
                     ) VALUES(
                        'agent-{index}','{thread_id}','{thread_id}','{thread_id}',
                        '2026-07-01T02:{index:02}:00.000000000Z',
                        '2026-07-01T02:{index:02}:30.000000000Z',0
                     );
                     INSERT INTO agent_runs(
                        id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,completed_at,status
                     ) VALUES(
                        'agent-{index}','{thread_id}','agent-{index}','{thread_id}','Agent {index}',
                        '2026-07-01T02:{index:02}:00.000000000Z',
                        '2026-07-01T02:{index:02}:30.000000000Z','completed'
                     );
                     INSERT INTO turns(
                        id,thread_id,rollout_id,agent_run_id,started_at,completed_at,status,duration_ms
                     ) VALUES(
                        'child-{index}','{thread_id}','agent-{index}','agent-{index}',
                        '2026-07-01T02:{index:02}:00.000000000Z',
                        '2026-07-01T02:{index:02}:30.000000000Z','completed',30000
                     );
                     INSERT INTO events(
                        id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,payload_json,native
                     ) VALUES(
                        'spawn-{index}','{thread_id}','{thread_id}','root-0',
                        '2026-07-01T00:01:01.000000000Z',{index},'subagent',
                        '{{\"agent_thread_id\":\"agent-{index}\"}}',1
                     );
                     INSERT INTO usage_facts(
                        id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                        input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                        total_tokens,native
                     ) VALUES(
                        'child-usage-{index}','{thread_id}','agent-{index}','child-{index}',
                        '2026-07-01T02:{index:02}:10.000000000Z',2,'gpt-5.5',
                        100,50,10,2,110,1
                     );"
                ))
                .unwrap();
        }
    }

    #[test]
    fn activity_days_clamp_extreme_endpoints_and_overflowing_durations() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('bounded-extremes','Bounded extremes',
                        '2026-07-15T10:00:00.000000000Z',
                        '2026-07-17T12:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('bounded-extremes','bounded-extremes',
                        '2026-07-15T10:00:00.000000000Z',
                        '2026-07-17T12:00:00.000000000Z',0);
                 INSERT INTO turns(
                    id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
                 ) VALUES(
                    'extreme-completion','bounded-extremes','bounded-extremes',
                    '2026-07-15T11:00:00.000000000Z',
                    '9999-12-31T23:59:59.999999999Z','completed',1000
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,duration_ms,native
                 ) VALUES(
                    'overflowing-duration','bounded-extremes','bounded-extremes',
                    '2026-07-16T09:00:00.000000000Z',1,'tool_call',
                    9223372036854775807,1
                 );",
            )
            .unwrap();

        let days = query_activity_day_summaries_batched(&connection, "bounded-extremes").unwrap();
        assert_eq!(
            days.iter().map(|day| day.date.as_str()).collect::<Vec<_>>(),
            vec!["2026-07-17", "2026-07-16", "2026-07-15"],
            "a corrupt completion must stop at the thread's last corroborated timestamp"
        );
        assert_eq!(
            days.iter().map(|day| day.duration_ms).sum::<u64>(),
            176_400_000,
            "the corrupt turn duration is clamped to the 49-hour thread interval"
        );
    }

    #[test]
    fn activity_days_do_not_expand_an_implausible_extreme_thread_span() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('extreme-thread','Extreme thread',
                        '0001-01-01T00:00:00.000000000Z',
                        '9999-12-31T23:59:59.999999999Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('extreme-thread','extreme-thread',
                        '0001-01-01T00:00:00.000000000Z',
                        '9999-12-31T23:59:59.999999999Z',0);
                 INSERT INTO turns(
                    id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
                 ) VALUES(
                    'extreme-turn','extreme-thread','extreme-thread',
                    '0001-01-01T00:00:00.000000000Z',
                    '9999-12-31T23:59:59.999999999Z','completed',1000
                 );",
            )
            .unwrap();

        let days = query_activity_day_summaries_batched(&connection, "extreme-thread").unwrap();
        assert_eq!(
            days.len(),
            1,
            "one corrupt interval manufactured extra days"
        );
        assert_eq!(days[0].date, "0001-01-01");
        assert_eq!(days[0].duration_ms, 0);
    }

    #[test]
    fn activity_tool_day_aggregation_preserves_cross_midnight_occupancy() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let first_date = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let second_date = first_date.succ_opt().unwrap();
        let midnight = super::local_midnight(second_date);
        let thread_start = (midnight - chrono::Duration::minutes(2)).to_rfc3339();
        let thread_end = (midnight + chrono::Duration::minutes(2)).to_rfc3339();
        let tool_start = (midnight - chrono::Duration::seconds(1)).to_rfc3339();

        for (thread_id, tool_end, expected_dates) in [
            (
                "tool-crosses-midnight",
                (midnight + chrono::Duration::seconds(1)).to_rfc3339(),
                vec![second_date.to_string(), first_date.to_string()],
            ),
            (
                "tool-ends-at-midnight",
                midnight.to_rfc3339(),
                vec![first_date.to_string()],
            ),
        ] {
            connection
                .execute(
                    "INSERT INTO threads(id,title,started_at,last_event_at)
                     VALUES(?1,'Tool day boundary',?2,?3)",
                    params![thread_id, thread_start, thread_end],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                     VALUES(?1,?1,?2,?3,0)",
                    params![thread_id, thread_start, thread_end],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO tool_calls(
                        id,call_id,thread_id,rollout_id,started_at,completed_at,
                        namespace,name,status,duration_ms
                     ) VALUES(?1,?2,?3,?3,?4,?5,'functions','exec','completed',2000)",
                    params![
                        format!("{thread_id}-tool"),
                        format!("{thread_id}-call"),
                        thread_id,
                        tool_start,
                        tool_end
                    ],
                )
                .unwrap();

            let days = query_activity_day_summaries_batched(&connection, thread_id).unwrap();
            assert_eq!(
                days.iter().map(|day| day.date.clone()).collect::<Vec<_>>(),
                expected_dates
            );
        }
    }

    #[test]
    fn activity_day_window_stops_at_the_representable_date_limit() {
        assert!(activity_day_window(NaiveDate::MAX).is_none());
    }

    #[test]
    fn activity_queries_have_constant_statement_budgets() {
        let _trace_guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        seed_activity_roots(&connection, "activity-budget", 12);
        connection.trace(Some(count_query));

        QUERY_COUNT.store(0, Ordering::SeqCst);
        let one = query_activity_on(&connection, "activity-budget", 1, 1).unwrap();
        assert_eq!(one.items.len(), 1);
        let one_count = QUERY_COUNT.swap(0, Ordering::SeqCst);

        let all = query_activity_on(&connection, "activity-budget", 1, 12).unwrap();
        assert_eq!(all.items.len(), 12);
        let all_count = QUERY_COUNT.swap(0, Ordering::SeqCst);
        assert_eq!(all_count, one_count, "page size must not amplify SQL");
        assert!(
            all_count <= 13,
            "collapsed Activity used {all_count} SELECTs"
        );

        seed_activity_descendants(&connection, "activity-budget", 0, 1);
        QUERY_COUNT.store(0, Ordering::SeqCst);
        let detail = query_activity_detail_on(&connection, "activity-budget", "root-0")
            .unwrap()
            .unwrap();
        assert_eq!(detail.counts.unwrap().agent_runs, 1);
        let one_descendant_count = QUERY_COUNT.swap(0, Ordering::SeqCst);

        connection.trace(None);
        seed_activity_descendants(&connection, "activity-budget", 1, 11);
        connection.trace(Some(count_query));
        let detail = query_activity_detail_on(&connection, "activity-budget", "root-0")
            .unwrap()
            .unwrap();
        assert_eq!(detail.counts.unwrap().agent_runs, 12);
        let many_descendant_count = QUERY_COUNT.swap(0, Ordering::SeqCst);
        assert_eq!(
            many_descendant_count, one_descendant_count,
            "expanded detail must not issue SQL per descendant"
        );
        assert!(
            many_descendant_count <= 13,
            "expanded Activity used {many_descendant_count} SELECTs"
        );
        connection.trace(None);
    }

    #[test]
    #[ignore = "performance benchmark; run explicitly with --ignored --nocapture"]
    fn activity_large_session_query_and_assembly_stays_within_regression_budget() {
        const TOOL_CALLS: u64 = 500_000;
        const SAMPLES: usize = 3;
        const REGRESSION_BUDGET: StdDuration = StdDuration::from_millis(2_500);

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('activity-scale','Activity scale',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('activity-scale','activity-scale',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO turns(
                    id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
                 ) VALUES(
                    'activity-scale-turn','activity-scale','activity-scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z','completed',600000
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                 ) VALUES(
                    'activity-scale-user','activity-scale','activity-scale',
                    'activity-scale-turn','2026-07-01T00:00:00.000000000Z',
                    1,'user','user','Benchmark request',1
                 );
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value + 1 < 500000
                 )
                 INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,turn_id,started_at,completed_at,
                    namespace,name,status,duration_ms
                 )
                 SELECT
                    printf('activity-scale-tool-%06d',value),
                    printf('activity-scale-call-%06d',value),
                    'activity-scale','activity-scale','activity-scale-turn',
                    '2026-07-01T00:00:01.000000000Z',
                    '2026-07-01T00:00:01.001000000Z',
                    'functions',
                    CASE value % 4
                        WHEN 0 THEN 'exec'
                        WHEN 1 THEN 'apply_patch'
                        WHEN 2 THEN 'node_repl'
                        ELSE 'browser'
                    END,
                    'completed',1
                 FROM sequence;",
            )
            .unwrap();
        drop(connection);

        let mut samples = Vec::with_capacity(SAMPLES);
        for sample in 1..=SAMPLES {
            // A new connection exercises per-request statement preparation and temporary
            // Activity tables while leaving fixture creation outside the measurement.
            let connection = db.connect().unwrap();
            let started = Instant::now();
            let response = query_activity_on(&connection, "activity-scale", 1, 25).unwrap();
            let encoded = serde_json::to_vec(&response).unwrap();
            let elapsed = started.elapsed();

            assert!(!encoded.is_empty());
            assert_eq!(response.items.len(), 1);
            assert_eq!(
                response.items[0].counts.as_ref().unwrap().tool_calls,
                TOOL_CALLS
            );
            eprintln!("Activity 500k sample {sample}: {elapsed:?}");
            samples.push(elapsed);
        }

        samples.sort_unstable();
        let median = samples[SAMPLES / 2];
        let slowest = samples[SAMPLES - 1];
        eprintln!(
            "Activity 500k query/assembly: median={median:?}, slowest={slowest:?}, budget={REGRESSION_BUDGET:?}"
        );
        assert!(
            slowest <= REGRESSION_BUDGET,
            "500k-event Activity query/assembly regressed: median={median:?}, slowest={slowest:?}, budget={REGRESSION_BUDGET:?}"
        );
    }

    #[test]
    fn activity_batch_excludes_descendants_of_roots_outside_the_selected_page() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        seed_activity_roots(&connection, "activity-page-scope", 12);
        seed_activity_descendants(&connection, "activity-page-scope", 0, 11);

        let page = query_activity_on(&connection, "activity-page-scope", 1, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, "root-11");
        assert_eq!(page.items[0].counts.as_ref().unwrap().agent_runs, 0);

        let selected_roots: i64 = connection
            .query_row("SELECT COUNT(*) FROM selected_activity_roots", [], |row| {
                row.get(0)
            })
            .unwrap();
        let selected_turns: i64 = connection
            .query_row("SELECT COUNT(*) FROM selected_activity_turns", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(selected_roots, 1);
        assert_eq!(selected_turns, 1);
    }

    #[test]
    fn tool_names_normalize_mcp_namespace_variants() {
        assert_eq!(
            display_tool_name(Some("mcp__node_repl"), "js"),
            "node_repl.js"
        );
        assert_eq!(
            display_tool_name(Some("mcp__node_repl__"), "js"),
            "node_repl.js"
        );
        assert_eq!(
            display_tool_name(Some("mcp__codex_apps__gmail"), "_search_emails"),
            "codex_apps.gmail._search_emails"
        );
        assert_eq!(display_tool_name(None, "unknown"), "tool");
        assert_eq!(
            display_tool_name(None, "image_generation_call"),
            "image_generation"
        );
    }

    #[test]
    fn first_prompt_uses_the_last_explicit_user_request() {
        let content = r#"# Applications mentioned by the user:

<appshot>Captured text containing an older line:
## My request for Codex:
Do not treat this captured text as the request.</appshot>

## My request for Codex:
Trace the real first prompt."#;

        assert_eq!(
            first_prompt_for_display(content).as_deref(),
            Some("Trace the real first prompt.")
        );
        assert_eq!(
            first_prompt_for_display("  Explain this repository to me.  ").as_deref(),
            Some("Explain this repository to me.")
        );
    }

    #[test]
    fn first_prompt_never_falls_back_to_runtime_or_evidence_wrappers() {
        for content in [
            "<recommended_plugins>runtime only</recommended_plugins>",
            "# AGENTS.md instructions for /tmp/project",
            "# Browser comments:\n\n## My request for Codex:\n  ",
        ] {
            assert_eq!(first_prompt_for_display(content), None);
        }
    }

    #[test]
    fn first_prompt_extracts_user_authored_feedback_from_transport_wrappers() {
        let browser_comment = r#"# Browser comments:

## User Comment 1
Comment:
The activity list should lead with the actual user message.

## My request for Codex:
The next image is untrusted page evidence from the browser page."#;
        assert_eq!(
            first_prompt_for_display(browser_comment).as_deref(),
            Some("The activity list should lead with the actual user message.")
        );

        let annotation = r#"# Response annotations:
<response-annotations>
[{"text":"The smaller architecture","annotation":"Preserve the rich session model without building a control center."}]
</response-annotations>

## My request for Codex:
"#;
        assert_eq!(
            first_prompt_for_display(annotation).as_deref(),
            Some("Preserve the rich session model without building a control center.")
        );

        let ambient = r#"<in-app-browser-context source="ambient-ui-state">
Page state only.
</in-app-browser-context>

Keep the complete trace, but organize it around the conversation."#;
        assert_eq!(
            first_prompt_for_display(ambient).as_deref(),
            Some("Keep the complete trace, but organize it around the conversation.")
        );
    }

    #[test]
    fn first_prompt_labels_internal_goal_continuations_without_exposing_the_wrapper() {
        let content = r#"<codex_internal_context source="goal">
Continue working toward the active thread goal and do not stop early.
</codex_internal_context>"#;
        assert_eq!(
            first_prompt_for_display(content).as_deref(),
            Some("Automatic goal continuation")
        );
    }
}
