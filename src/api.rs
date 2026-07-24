use crate::{
    MAX_JS_SAFE_INTEGER, MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR, activity_index,
    config::PricingConfig,
    db::Db,
    db_executor::{DbExecutor, WorkClass},
    fixed_price::PriceMicros,
    ingest::{IngestRoots, ScanReport},
    manual_pricing::{MAX_MODEL_ID_CHARS, ManualAlias, ManualPrice, MutationError},
    model::Totals,
    money::UsdAmount,
    pricing,
    redaction::redact_data_urls,
};
use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    body::{Body as AxumBody, to_bytes},
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderName, HeaderValue, Method, Request, StatusCode, Uri,
        header::{ALLOW, CONTENT_TYPE, HOST, ORIGIN},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, Months, NaiveDate, SecondsFormat, TimeZone,
    Timelike, Utc,
};
use rusqlite::{
    Connection, InterruptHandle, OptionalExtension, Row, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::IntoFuture,
    net::IpAddr,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Mutex as AsyncMutex;
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use unicode_normalization::UnicodeNormalization;

#[derive(Clone)]
pub struct ApiState {
    pub db: Db,
    pub roots: IngestRoots,
    pub frontend: PathBuf,
    pub pricing: PricingConfig,
    pub executor: DbExecutor,
    pub pricing_sync: pricing::PricingSync,
    manual_pricing_mutations: Arc<AsyncMutex<()>>,
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
            manual_pricing_mutations: Arc::new(AsyncMutex::new(())),
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
        .route("/stats", get(stats))
        .route("/settings", get(settings))
        .route("/prices", get(prices))
        .route("/prices/model-ids", get(price_model_ids))
        .route("/prices/metadata", get(price_metadata))
        .route("/prices/refresh", post(refresh_prices))
        .route("/prices/{model_id}", put(put_price).delete(delete_price))
        .route("/aliases", get(aliases))
        .route(
            "/aliases/{observed_model_id}",
            put(put_alias).delete(delete_alias),
        )
        .fallback(api_not_found)
        .layer(middleware::from_fn(api_error_contract));
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

async fn api_error_contract(request: Request<AxumBody>, next: Next) -> Response {
    let response = next.run(request).await;
    if !response.status().is_client_error()
        || response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return response;
    }

    let status = response.status();
    let allow = response.headers().get(ALLOW).cloned();
    let message = to_bytes(response.into_body(), 16 * 1024)
        .await
        .ok()
        .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("invalid API request")
                .to_owned()
        });
    let mut response = ApiError { status, message }.into_response();
    if let Some(allow) = allow {
        response.headers_mut().insert(ALLOW, allow);
    }
    response
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
    let fetch_site = request
        .headers()
        .get(HeaderName::from_static("sec-fetch-site"))
        .and_then(|value| value.to_str().ok());
    if is_mutating_method(request.method()) {
        if fetch_site.is_some_and(|site| !matches!(site, "same-origin" | "none")) {
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
    } else if is_api_path(request.uri().path())
        && fetch_site.is_some_and(|site| !matches!(site, "same-origin" | "none"))
    {
        return boundary_rejection("cross-origin API requests are not allowed");
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

fn is_api_path(path: &str) -> bool {
    path == "/api/v1" || path.starts_with("/api/v1/")
}

fn boundary_rejection(message: &'static str) -> Response {
    ApiError {
        status: StatusCode::FORBIDDEN,
        message: message.to_owned(),
    }
    .into_response()
}

pub async fn serve(state: ApiState, listener: tokio::net::TcpListener) -> Result<()> {
    const GRACEFUL_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

    let app = router(state);
    let address = listener
        .local_addr()
        .context("failed to inspect bound listener")?;
    tracing::info!(%address, "Codex Usage is ready");
    let (begin_shutdown, shutdown_requested) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_requested.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = shutdown_signal() => {
            // Stop accepting immediately, then give well-behaved in-flight
            // requests a short window to finish. An idle keep-alive socket or
            // an incomplete HTTP header must not hold the local process open
            // forever during shutdown.
            let _ = begin_shutdown.send(());
            match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, &mut server).await {
                Ok(result) => result?,
                Err(_) => {
                    tracing::warn!(
                        timeout_ms = GRACEFUL_SHUTDOWN_TIMEOUT.as_millis(),
                        "forcing server shutdown after graceful drain deadline"
                    );
                }
            }
        }
    }
    Ok(())
}

pub fn prewarm_current_year_analytics(db: &Db) -> Result<()> {
    let today = Local::now().date_naive();
    // Startup has no requests to protect from synchronous SQLite work yet.
    // Running this directly also avoids depending on Tokio's blocking pool
    // before the long-lived server tasks have started.
    let connection = db.connect()?;
    let transaction = Transaction::new_unchecked(&connection, TransactionBehavior::Deferred)?;
    prewarm_current_year_analytics_on(&transaction, today)
        .context("failed to prewarm current-year analytics")?;
    transaction.commit()?;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler must install");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
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
    fn install(self: &Arc<Self>, connection: &Connection) -> Result<()> {
        let cancellation = self.clone();
        connection.progress_handler(
            4_096,
            Some(move || cancellation.cancelled.load(Ordering::Acquire)),
        );
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
        run_work(&state, WorkClass::Control, move || query_status(&db)).await?,
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
    pub cost_usd: Option<UsdAmount>,
    pub unpriced_tokens: u64,
    pub lifetime_cost_usd: Option<UsdAmount>,
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

const MAX_SESSION_SEARCH_CHARS: usize = 256;

async fn sessions(
    State(state): State<ApiState>,
    Query(query): Query<SessionsQuery>,
) -> ApiResult<Json<SessionsResponse>> {
    let (start, end) = query_bounds(
        query.date.as_deref(),
        query.start.as_deref(),
        query.end.as_deref(),
    )?;
    let page = validated_page(query.page)?;
    let page_size = query.page_size.unwrap_or(50).clamp(1, 200);
    let db = state.db.clone();
    let project = query.project;
    let search = query.q;
    if search
        .as_deref()
        .is_some_and(|value| value.chars().count() > MAX_SESSION_SEARCH_CHARS)
    {
        return Err(ApiError::bad_request(format!(
            "session search must be at most {MAX_SESSION_SEARCH_CHARS} characters"
        )));
    }
    let sort = query.sort.unwrap_or_else(|| "recent".into());
    if !matches!(sort.as_str(), "recent" | "cost") {
        return Err(ApiError::bad_request("sort must be recent or cost"));
    }
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
    delta_cost_usd: Option<UsdAmount>,
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
    cost_usd: Option<UsdAmount>,
    session_count: u64,
    message_count: u64,
    total_tokens: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectDriver {
    project: String,
    cost_usd: Option<UsdAmount>,
    share: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PricingSummary {
    known_cost_usd: UsdAmount,
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

    fn cost_usd(&self) -> Option<UsdAmount> {
        (self.unpriced_tokens == 0)
            .then_some(UsdAmount::from_cost_numerator(self.known_cost_numerator))
    }
}

#[derive(Clone, Default)]
struct FixedPointUsageTotals {
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    total_tokens: u64,
    known_cost_numerator: i128,
    unpriced_tokens: u64,
}

impl FixedPointUsageTotals {
    #[allow(clippy::too_many_arguments)]
    fn add_group(
        &mut self,
        input_tokens: u64,
        cached_input_tokens: u64,
        output_tokens: u64,
        reasoning_tokens: u64,
        total_tokens: u64,
        known_cost_numerator: i128,
        unpriced_tokens: u64,
    ) {
        self.input_tokens = self.input_tokens.saturating_add(input_tokens);
        self.cached_input_tokens = self.cached_input_tokens.saturating_add(cached_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(output_tokens);
        self.reasoning_tokens = self.reasoning_tokens.saturating_add(reasoning_tokens);
        self.total_tokens = self.total_tokens.saturating_add(total_tokens);
        self.known_cost_numerator = self
            .known_cost_numerator
            .saturating_add(known_cost_numerator);
        self.unpriced_tokens = self.unpriced_tokens.saturating_add(unpriced_tokens);
    }

    fn merge(&mut self, other: Self) {
        self.add_group(
            other.input_tokens,
            other.cached_input_tokens,
            other.output_tokens,
            other.reasoning_tokens,
            other.total_tokens,
            other.known_cost_numerator,
            other.unpriced_tokens,
        );
    }

    fn finish(self) -> Totals {
        Totals {
            input_tokens: self.input_tokens,
            cached_input_tokens: self.cached_input_tokens,
            output_tokens: self.output_tokens,
            reasoning_tokens: self.reasoning_tokens,
            total_tokens: self.total_tokens,
            known_cost_numerator: self.known_cost_numerator,
            unpriced_tokens: self.unpriced_tokens,
            ..Totals::default()
        }
        .finish()
    }
}

#[allow(clippy::too_many_arguments)]
fn add_usage_fact_to_totals(
    totals: &mut FixedPointUsageTotals,
    aliases: &OverviewPriceAliases,
    prices: &OverviewPriceLedger,
    timestamp: &str,
    model: &str,
    input_tokens: i64,
    cached_input_tokens: i64,
    output_tokens: i64,
    reasoning_tokens: i64,
    total_tokens: i64,
) {
    let input_tokens = input_tokens.max(0);
    let cached_input_tokens = cached_input_tokens.max(0).min(input_tokens);
    let output_tokens = output_tokens.max(0);
    let reasoning_tokens = reasoning_tokens.max(0);
    let total_tokens = total_tokens.max(0) as u64;
    let (known_cost_numerator, unpriced_tokens) =
        if let Some((_, price)) = overview_price_for(aliases, prices, model, timestamp) {
            (
                overview_cost_for_price(
                    price,
                    input_tokens - cached_input_tokens,
                    cached_input_tokens,
                    output_tokens,
                ),
                0,
            )
        } else {
            (0, total_tokens)
        };
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

#[derive(Clone, Copy)]
enum UsageRollupScope<'a> {
    All,
    Thread,
    Turn(&'a str),
    Agent(&'a str),
    Effort(Option<&'a str>),
    ActivitySelection {
        root_turn_id: &'a str,
        usage_kind: i64,
    },
}

struct UsageRollupExceptionalQuery<'a> {
    scope: UsageRollupScope<'a>,
    thread_id: &'a str,
    model: &'a str,
    start: &'a str,
    end: &'a str,
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
    validate_public_year(year)?;
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
    cost_usd: Option<UsdAmount>,
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
    cost_usd: Option<UsdAmount>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    child_next_cursor: Option<String>,
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
    child_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityCollectionCursor {
    version: u8,
    thread_id: String,
    item_id: String,
    timestamp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_line: Option<i64>,
    sort_id: String,
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
    let page = validated_page(query.page)?;
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
    let child_page = validated_page(query.child_page)?;
    let child_page_size = query
        .child_page_size
        .unwrap_or(DEFAULT_ACTIVITY_CHILD_PAGE_SIZE)
        .clamp(1, MAX_ACTIVITY_CHILD_PAGE_SIZE);
    if let Some(cursor) = query.child_cursor.as_deref() {
        validate_activity_detail_cursor(cursor, &id, &event_id)
            .map_err(|_| ApiError::bad_request("invalid Activity cursor"))?;
    }
    let child_cursor = query.child_cursor;
    let db = state.db.clone();
    run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
        if query_session_on(connection, &id)?.is_none() {
            return Ok(None);
        }
        query_activity_detail_cursor_page_on(
            connection,
            &id,
            &event_id,
            child_page,
            child_page_size,
            child_cursor.as_deref(),
        )
        .map(Some)
    })
    .await?
    .ok_or_else(|| ApiError::not_found("session not found"))?
    .map(Json)
    .ok_or_else(|| ApiError::not_found("activity event not found"))
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
    trend: Vec<Option<UsdAmount>>,
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
    // All-time is data-derived and ends in the later of the current year or
    // the latest observed data year. An anchor has no semantic meaning for it;
    // ignoring it also prevents a caller from manufacturing thousands of
    // empty future or past year buckets.
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
    let display_anchor = match range.as_str() {
        "week" => anchor - Duration::days(anchor.weekday().num_days_from_monday() as i64),
        "month" => NaiveDate::from_ymd_opt(anchor.year(), anchor.month(), 1)
            .ok_or_else(|| ApiError::bad_request("invalid monthly anchor"))?,
        "year" => NaiveDate::from_ymd_opt(anchor.year(), 1, 1)
            .ok_or_else(|| ApiError::bad_request("invalid yearly anchor"))?,
        _ => anchor,
    };
    // Weekly anchors are canonicalized to Monday. The first few days of 1970
    // would otherwise normalize into 1969 and leak a response outside the
    // public date domain even though the request itself passed validation.
    if range == "week" && display_anchor.year() < MIN_PUBLIC_YEAR {
        return Err(ApiError::bad_request(
            "weekly anchor must be on or after 1970-01-05",
        ));
    }
    validate_public_year(display_anchor.year())?;
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
    totals.known_cost_numerator = total_cost_numerator;
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
    let database_bytes = db.storage_bytes();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_settings_on(
                connection,
                database_path,
                active_root,
                archive_root,
                timezone,
                database_bytes,
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
            known_cost_usd: UsdAmount::from_cost_numerator(totals.known_cost_numerator),
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AliasesQuery {
    q: Option<String>,
    page: Option<u64>,
    page_size: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AliasesResponse {
    items: Vec<AliasRow>,
    page: u64,
    page_size: u64,
    total: u64,
    total_pages: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PriceMetadataResponse {
    observed_unknown: Vec<UnknownModelRow>,
    observed_unknown_total: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriceMetadataQuery {
    unknown_limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PriceModelIdsQuery {
    q: Option<String>,
    limit: Option<u64>,
}

#[derive(Debug, Serialize)]
struct PriceModelIdsResponse {
    items: Vec<String>,
}

const MAX_PRICE_MODEL_ID_RESULTS: u64 = 100;
const MAX_UNKNOWN_MODEL_RESULTS: u64 = 100;

async fn prices(
    State(state): State<ApiState>,
    Query(query): Query<PricesQuery>,
) -> ApiResult<Json<PricesResponse>> {
    let page = validated_page(query.page)?;
    let page_size = query.page_size.unwrap_or(25).clamp(1, 100);
    let db = state.db.clone();
    let search = query.q;
    if search.as_deref().is_some_and(|value| {
        value.chars().count() > MAX_SESSION_SEARCH_CHARS
            || normalize_search_text(value).chars().count() > MAX_SESSION_SEARCH_CHARS
    }) {
        return Err(ApiError::bad_request(format!(
            "price search must be at most {MAX_SESSION_SEARCH_CHARS} characters"
        )));
    }
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_prices_on(connection, search.as_deref(), page, page_size)
        })
        .await?,
    ))
}

async fn aliases(
    State(state): State<ApiState>,
    Query(query): Query<AliasesQuery>,
) -> ApiResult<Json<AliasesResponse>> {
    let page = validated_page(query.page)?;
    let page_size = query.page_size.unwrap_or(25).clamp(1, 100);
    let search = query
        .q
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if search
        .as_deref()
        .is_some_and(|value| value.chars().count() > MAX_SESSION_SEARCH_CHARS)
    {
        return Err(ApiError::bad_request(format!(
            "alias search must be at most {MAX_SESSION_SEARCH_CHARS} characters"
        )));
    }
    let db = state.db.clone();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Light, db, move |connection| {
            query_aliases_on(connection, search.as_deref(), page, page_size)
        })
        .await?,
    ))
}

async fn price_metadata(
    State(state): State<ApiState>,
    Query(query): Query<PriceMetadataQuery>,
) -> ApiResult<Json<PriceMetadataResponse>> {
    let unknown_limit = query.unknown_limit.unwrap_or(MAX_UNKNOWN_MODEL_RESULTS);
    if !(1..=MAX_UNKNOWN_MODEL_RESULTS).contains(&unknown_limit) {
        return Err(ApiError::bad_request(format!(
            "unknownLimit must be between 1 and {MAX_UNKNOWN_MODEL_RESULTS}"
        )));
    }
    let db = state.db.clone();
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_price_metadata_on(connection, unknown_limit)
        })
        .await?,
    ))
}

async fn price_model_ids(
    State(state): State<ApiState>,
    Query(query): Query<PriceModelIdsQuery>,
) -> ApiResult<Json<PriceModelIdsResponse>> {
    let limit = query.limit.unwrap_or(MAX_PRICE_MODEL_ID_RESULTS);
    if !(1..=MAX_PRICE_MODEL_ID_RESULTS).contains(&limit) {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_PRICE_MODEL_ID_RESULTS}"
        )));
    }
    if query.q.as_deref().is_some_and(|value| {
        value.chars().count() > MAX_SESSION_SEARCH_CHARS
            || normalize_search_text(value.trim()).chars().count() > MAX_SESSION_SEARCH_CHARS
    }) {
        return Err(ApiError::bad_request(format!(
            "model ID search must be at most {MAX_SESSION_SEARCH_CHARS} characters"
        )));
    }
    let db = state.db.clone();
    let search = query.q;
    Ok(Json(
        run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
            query_price_model_ids_on(connection, search.as_deref(), limit)
        })
        .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriceInput {
    effective_from: Option<String>,
    effective_to: Option<String>,
    input_per_million: String,
    cached_input_per_million: Option<String>,
    output_per_million: String,
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
    if model_id.chars().count() > MAX_MODEL_ID_CHARS {
        return Err(ApiError::bad_request(format!(
            "model ID must be at most {MAX_MODEL_ID_CHARS} characters"
        )));
    }
    if input.currency.as_deref().unwrap_or("USD") != "USD" {
        return Err(ApiError::bad_request("only USD prices are supported"));
    }
    let input_price = PriceMicros::from_per_million_text(&input.input_per_million)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let cached_price = input
        .cached_input_per_million
        .as_ref()
        .map(|value| PriceMicros::from_per_million_text(value))
        .transpose()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let output_price = PriceMicros::from_per_million_text(&input.output_per_million)
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
    // Queue mutations asynchronously before they take a blocking-worker
    // permit. File locking is necessarily synchronous, but one contended
    // pricing writer must not multiply into a pool-wide outage.
    let mutation_guard = state.manual_pricing_mutations.clone().lock_owned().await;
    state
        .executor
        .run(WorkClass::Light, move || {
            // The request may disappear while `spawn_blocking` continues.
            // Keep serialization owned by the synchronous operation itself so
            // canceled writers cannot accumulate behind the file mutex.
            let _mutation_guard = mutation_guard;
            Ok(mutation())
        })
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
                (SELECT COUNT(*) FROM agent_runs
                 WHERE thread_id=?1 AND id<>thread_id
                   AND started_at>=?2 AND started_at<?3),
                (SELECT COUNT(*) FROM tool_calls
                 WHERE thread_id=?1 AND started_at>=?2 AND started_at<?3),
                ?4,?5,?6,'0',0
         FROM threads t
         WHERE t.id=?1",
    )?;
    for (thread_id, usage) in ranked {
        let mut row = statement.query_row(
            params![
                thread_id,
                start,
                end,
                usage.total_tokens.min(i64::MAX as u64) as i64,
                usage.known_cost_numerator.to_string(),
                usage.unpriced_tokens.min(i64::MAX as u64) as i64
            ],
            session_from_row,
        )?;
        let lifetime = query_all_time_rollup_totals_on(connection, Some(thread_id))?.finish();
        row.lifetime_cost_usd = lifetime.cost_usd;
        row.lifetime_unpriced_tokens = lifetime.unpriced_tokens;
        rows.push(row);
    }
    Ok(rows)
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
    let (aliases, prices) = overview_prices_on(connection)?;
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
        let mut aggregates = HashMap::<String, FixedPointUsageTotals>::new();
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
            let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
                connection,
                &aliases,
                &prices,
                UsageRollupScope::Thread,
                &thread_id,
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
                aggregate.total_tokens.min(i64::MAX as u64) as i64,
                sortable_cost_numerator(aggregate.known_cost_numerator),
                aggregate.unpriced_tokens.min(i64::MAX as u64) as i64,
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
        .map(sql_timestamp);
    let rollup_end = end_timestamp
        .map(utc_hour_floor)
        .transpose()?
        .map(sql_timestamp);
    let has_complete_hours = !matches!(
        (&rollup_start, &rollup_end),
        (Some(start), Some(end)) if start >= end
    );
    let mut aggregates = HashMap::<String, FixedPointUsageTotals>::new();
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
            let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
                connection,
                &aliases,
                &prices,
                UsageRollupScope::Thread,
                &thread_id,
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
            &aliases,
            &prices,
            project,
            q_filter,
            &sql_timestamp(start),
            &sql_timestamp(end),
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
            aggregate.total_tokens.min(i64::MAX as u64) as i64,
            sortable_cost_numerator(aggregate.known_cost_numerator),
            aggregate.unpriced_tokens.min(i64::MAX as u64) as i64,
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
    aliases: &OverviewPriceAliases,
    prices: &OverviewPriceLedger,
    project: Option<&str>,
    q_filter: Option<&str>,
    start: &str,
    end: &str,
    aggregates: &mut HashMap<String, FixedPointUsageTotals>,
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
        add_usage_fact_to_totals(
            aggregates.entry(thread_id).or_default(),
            aliases,
            prices,
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
) -> Result<HashMap<String, FixedPointUsageTotals>> {
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals = HashMap::<String, FixedPointUsageTotals>::new();
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
            let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
                connection,
                &aliases,
                &prices,
                UsageRollupScope::Thread,
                &thread_id,
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
        add_usage_fact_to_totals(
            totals.entry(thread_id).or_default(),
            &aliases,
            &prices,
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
    anyhow::ensure!(matches!(sort, "recent" | "cost"), "invalid session sort");
    let q_filter = q.filter(|value| !value.trim().is_empty());
    anyhow::ensure!(
        q_filter.is_none_or(|value| value.chars().count() <= MAX_SESSION_SEARCH_CHARS),
        "session search exceeds the {MAX_SESSION_SEARCH_CHARS}-character limit"
    );
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

    if sort == "cost" {
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
        connection.execute(&sql, params![project, q_filter, page_size as i64, offset])?;
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
        connection.execute(&sql, params![project, q_filter, page_size as i64, offset])?;
    }

    // Everything below this point is bounded by the selected page. In the
    // default recent view, no corpus-wide event/message/tool aggregate is run.
    let order = if sort == "cost" {
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

fn session_from_row(row: &Row<'_>) -> rusqlite::Result<SessionRow> {
    let id: String = row.get(0)?;
    let total_tokens = row.get::<_, i64>(10)?.max(0) as u64;
    let known_cost_numerator = cost_numerator_from_row(row, 11)?;
    let unpriced_tokens = row.get::<_, i64>(12)?.max(0) as u64;
    let lifetime_known_cost_numerator = cost_numerator_from_row(row, 13)?;
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

fn query_session_on(connection: &Connection, id: &str) -> Result<Option<SessionRow>> {
    let row = connection
        .query_row(
            "SELECT t.id,t.started_at,t.last_event_at,COALESCE(t.title,'Untitled session'),
                COALESCE(t.project,'—'),t.branch,
                (SELECT COUNT(*) FROM messages m WHERE m.thread_id=t.id),
                (SELECT COUNT(*) FROM turns tr WHERE tr.thread_id=t.id),
                (SELECT COUNT(*) FROM agent_runs a WHERE a.thread_id=t.id AND a.id<>a.thread_id),
                (SELECT COUNT(*) FROM tool_calls tc WHERE tc.thread_id=t.id),
                0,'0',0,'0',0
         FROM threads t WHERE t.id=?1 AND (
            EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id))",
            [id],
            session_from_row,
        )
        .optional()?;
    let Some(mut row) = row else {
        return Ok(None);
    };
    let totals = query_all_time_rollup_totals_on(connection, Some(id))?.finish();
    row.total_tokens = totals.total_tokens;
    row.cost_usd = totals.cost_usd;
    row.unpriced_tokens = totals.unpriced_tokens;
    row.lifetime_cost_usd = totals.cost_usd;
    row.lifetime_unpriced_tokens = totals.unpriced_tokens;
    Ok(Some(row))
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
    if start.is_none() && end.is_none() {
        query_all_time_rollup_totals_on(connection, thread_id).map(FixedPointUsageTotals::finish)
    } else {
        query_raw_usage_totals_on(connection, start, end, thread_id)
            .map(FixedPointUsageTotals::finish)
    }
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
        aggregate.known_cost_numerator = cost_numerator;
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
        start: bounds.start_at.clone(),
        end: bounds.end_at.clone(),
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
                   COALESCE(SUM(p.cost_numerator),0) known_cost_numerator,
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
                COALESCE(u.known_cost_numerator,0),
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
                        known_cost_numerator: i128::from(row.get::<_, i64>(6)?),
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
        aggregate.totals.known_cost_numerator = aggregate.known_cost_numerator;
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
        push_nonempty_stats_bucket(
            &mut buckets,
            local_midnight(date),
            local_midnight(next_date),
            date.to_string(),
        );
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

fn usage_rollup_hour_window(activity_hour: &str) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let start = DateTime::parse_from_rfc3339(activity_hour)
        .with_context(|| format!("invalid UTC usage rollup hour {activity_hour}"))?
        .with_timezone(&Utc);
    let end = start
        .checked_add_signed(Duration::hours(1))
        .context("usage rollup hour has no successor")?;
    Ok((start, end))
}

fn usage_rollup_bucket_dates<Tz: TimeZone>(
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

#[allow(clippy::too_many_arguments)]
fn usage_rollup_cost_on(
    connection: &Connection,
    aliases: &OverviewPriceAliases,
    prices: &OverviewPriceLedger,
    scope: UsageRollupScope<'_>,
    thread_id: &str,
    activity_hour: &str,
    model: &str,
    input_tokens: i64,
    cached_input_tokens: i64,
    output_tokens: i64,
    total_tokens: u64,
) -> Result<(i128, u64)> {
    let (start, end) = usage_rollup_hour_window(activity_hour)?;
    let first_timestamp = sql_timestamp(start);
    let last_timestamp = sql_timestamp(
        end.checked_sub_signed(Duration::nanoseconds(1))
            .unwrap_or(start),
    );
    let cached_input_tokens = input_tokens.min(cached_input_tokens.max(0));
    let uncached_input_tokens = input_tokens.saturating_sub(cached_input_tokens);
    let first_price = overview_price_for(aliases, prices, model, &first_timestamp);
    let last_price = overview_price_for(aliases, prices, model, &last_timestamp);
    let has_price_boundary = overview_group_has_price_boundary(
        aliases,
        prices,
        model,
        &first_timestamp,
        &last_timestamp,
    );
    match (first_price, last_price) {
        (Some((first_index, price)), Some((last_index, _)))
            if first_index == last_index && !has_price_boundary =>
        {
            Ok((
                overview_cost_for_price(
                    price,
                    uncached_input_tokens,
                    cached_input_tokens,
                    output_tokens,
                ),
                0,
            ))
        }
        (None, None)
            if overview_group_has_no_price(
                aliases,
                prices,
                model,
                &first_timestamp,
                &last_timestamp,
            ) =>
        {
            Ok((0, total_tokens))
        }
        _ => usage_rollup_exceptional_cost_on(
            connection,
            aliases,
            prices,
            UsageRollupExceptionalQuery {
                scope,
                thread_id,
                model,
                start: &first_timestamp,
                end: &sql_timestamp(end),
            },
        ),
    }
}

fn usage_rollup_exceptional_cost_on(
    connection: &Connection,
    aliases: &OverviewPriceAliases,
    prices: &OverviewPriceLedger,
    query: UsageRollupExceptionalQuery<'_>,
) -> Result<(i128, u64)> {
    let sql = match query.scope {
        UsageRollupScope::All => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_model_time
             WHERE model=?1 AND timestamp>=?2 AND timestamp<?3"
        }
        UsageRollupScope::Thread => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_thread_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4"
        }
        UsageRollupScope::Turn(_) => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_turn_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND turn_id=?5"
        }
        UsageRollupScope::Agent(_) => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_agent_run
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND agent_run_id=?5"
        }
        UsageRollupScope::Effort(_) => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_thread_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND effort IS ?5"
        }
        UsageRollupScope::ActivitySelection { .. } => {
            "SELECT u.timestamp,u.input_tokens,u.cached_input_tokens,
                    u.output_tokens,u.total_tokens
             FROM usage_facts u INDEXED BY idx_usage_thread_model_time
             JOIN selected_activity_turns selected ON selected.turn_id=u.turn_id
             WHERE u.thread_id=?1 AND u.model=?2
               AND u.timestamp>=?3 AND u.timestamp<?4
               AND selected.root_turn_id=?5 AND selected.usage_kind=?6"
        }
    };
    let mut statement = connection.prepare(sql)?;
    let mut rows = match query.scope {
        UsageRollupScope::All => statement.query(params![query.model, query.start, query.end])?,
        UsageRollupScope::Thread => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end
        ])?,
        UsageRollupScope::Turn(turn_id) => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            turn_id
        ])?,
        UsageRollupScope::Agent(agent_run_id) => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            agent_run_id
        ])?,
        UsageRollupScope::Effort(effort) => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            effort
        ])?,
        UsageRollupScope::ActivitySelection {
            root_turn_id,
            usage_kind,
        } => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            root_turn_id,
            usage_kind
        ])?,
    };
    let mut cost_numerator = 0i128;
    let mut unpriced_tokens = 0u64;
    while let Some(row) = rows.next()? {
        let timestamp = row.get_ref(0)?.as_str()?;
        let input_tokens = row.get::<_, i64>(1)?.max(0);
        let cached_tokens = input_tokens.min(row.get::<_, i64>(2)?.max(0));
        let output_tokens = row.get::<_, i64>(3)?.max(0);
        let total_tokens = row.get::<_, i64>(4)?.max(0) as u64;
        if let Some((_, price)) = overview_price_for(aliases, prices, query.model, timestamp) {
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

fn usage_rollup_local_day_splits_on(
    connection: &Connection,
    aliases: &OverviewPriceAliases,
    prices: &OverviewPriceLedger,
    query: UsageRollupExceptionalQuery<'_>,
) -> Result<HashMap<NaiveDate, FixedPointUsageTotals>> {
    let sql = match query.scope {
        UsageRollupScope::All => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_model_time
             WHERE model=?1 AND timestamp>=?2 AND timestamp<?3"
        }
        UsageRollupScope::Thread => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_thread_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4"
        }
        UsageRollupScope::Turn(_) => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_turn_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND turn_id=?5"
        }
        UsageRollupScope::Agent(_) => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_agent_run
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND agent_run_id=?5"
        }
        UsageRollupScope::Effort(_) => {
            "SELECT timestamp,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens
             FROM usage_facts INDEXED BY idx_usage_thread_model_time
             WHERE thread_id=?1 AND model=?2 AND timestamp>=?3 AND timestamp<?4
               AND effort IS ?5"
        }
        UsageRollupScope::ActivitySelection { .. } => {
            "SELECT u.timestamp,u.input_tokens,u.cached_input_tokens,u.output_tokens,
                    u.reasoning_tokens,u.total_tokens
             FROM usage_facts u INDEXED BY idx_usage_thread_model_time
             JOIN selected_activity_turns selected ON selected.turn_id=u.turn_id
             WHERE u.thread_id=?1 AND u.model=?2
               AND u.timestamp>=?3 AND u.timestamp<?4
               AND selected.root_turn_id=?5 AND selected.usage_kind=?6"
        }
    };
    let mut statement = connection.prepare(sql)?;
    let mut rows = match query.scope {
        UsageRollupScope::All => statement.query(params![query.model, query.start, query.end])?,
        UsageRollupScope::Thread => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end
        ])?,
        UsageRollupScope::Turn(turn_id) => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            turn_id
        ])?,
        UsageRollupScope::Agent(agent_run_id) => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            agent_run_id
        ])?,
        UsageRollupScope::Effort(effort) => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            effort
        ])?,
        UsageRollupScope::ActivitySelection {
            root_turn_id,
            usage_kind,
        } => statement.query(params![
            query.thread_id,
            query.model,
            query.start,
            query.end,
            root_turn_id,
            usage_kind
        ])?,
    };
    let mut totals = HashMap::<NaiveDate, FixedPointUsageTotals>::new();
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
            overview_price_for(aliases, prices, query.model, timestamp).map_or(
                (0, total_tokens),
                |(_, price)| {
                    (
                        overview_cost_for_price(
                            price,
                            input_tokens - cached_input_tokens,
                            cached_input_tokens,
                            output_tokens,
                        ),
                        0,
                    )
                },
            );
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
    Ok(totals)
}

fn query_all_time_rollup_totals_on(
    connection: &Connection,
    thread_id: Option<&str>,
) -> Result<FixedPointUsageTotals> {
    let groups = if let Some(thread_id) = thread_id {
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
            .query_map([thread_id], usage_rollup_group_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?
    } else {
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
            .query_map([], usage_rollup_group_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals = FixedPointUsageTotals::default();
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
        let scope = if thread_id.is_some() {
            UsageRollupScope::Thread
        } else {
            UsageRollupScope::All
        };
        let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
            connection,
            &aliases,
            &prices,
            scope,
            thread_id.unwrap_or(""),
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
    Ok(totals)
}

type UsageRollupGroup = (String, String, i64, i64, i64, i64, u64);

fn usage_rollup_group_from_row(row: &Row<'_>) -> rusqlite::Result<UsageRollupGroup> {
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

fn query_raw_usage_totals_on(
    connection: &Connection,
    start: Option<&str>,
    end: Option<&str>,
    thread_id: Option<&str>,
) -> Result<FixedPointUsageTotals> {
    let mut sql = String::from(
        "SELECT timestamp,model,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens FROM usage_facts",
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
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(values))?;
    let mut totals = FixedPointUsageTotals::default();
    while let Some(row) = rows.next()? {
        let timestamp = row.get::<_, String>(0)?;
        let model = row.get::<_, String>(1)?;
        add_usage_fact_to_totals(
            &mut totals,
            &aliases,
            &prices,
            &timestamp,
            &model,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        );
    }
    Ok(totals)
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

fn prewarm_current_year_analytics_on(connection: &Connection, today: NaiveDate) -> Result<()> {
    let year = today.year();
    let anchor = NaiveDate::from_ymd_opt(year, 1, 1).context("current year is invalid")?;
    let start = sql_timestamp(local_midnight(anchor));
    let next_year =
        NaiveDate::from_ymd_opt(year + 1, 1, 1).context("next year is outside the date domain")?;
    let end = sql_timestamp(local_midnight(next_year));
    let _ = query_overview_year_on(connection, year, &start, &end)?;
    let _ = query_stats_on(connection, "year", anchor)?;
    Ok(())
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
    let groups = {
        let mut statement = connection.prepare(
            "SELECT model,effort,
                    strftime('%Y-%m-%dT%H:00:00.000000000Z',timestamp) activity_hour,
                    COALESCE(SUM(input_tokens),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(output_tokens),0),
                    COALESCE(SUM(reasoning_tokens),0),
                    COALESCE(SUM(total_tokens),0)
             FROM usage_facts
             WHERE thread_id=?1
             GROUP BY model,effort,activity_hour",
        )?;
        statement
            .query_map([thread_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
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
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals = HashMap::<(String, Option<String>), FixedPointUsageTotals>::new();
    for (
        model,
        effort,
        activity_hour,
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
    ) in groups
    {
        let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
            connection,
            &aliases,
            &prices,
            UsageRollupScope::Effort(effort.as_deref()),
            thread_id,
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals.entry((model, effort)).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    let mut usage = totals
        .into_iter()
        .map(|((model, effort), totals)| {
            let totals = totals.finish();
            ModelUsage {
                model,
                effort,
                input_tokens: totals.input_tokens,
                cached_input_tokens: totals.cached_input_tokens,
                output_tokens: totals.output_tokens,
                reasoning_tokens: totals.reasoning_tokens,
                total_tokens: totals.total_tokens,
                cost_usd: totals.cost_usd,
                unpriced_tokens: totals.unpriced_tokens,
            }
        })
        .collect::<Vec<_>>();
    usage.sort_by(|left, right| {
        right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| left.model.cmp(&right.model))
            .then_with(|| left.effort.cmp(&right.effort))
    });
    Ok(usage)
}

fn query_agent_totals_on(
    connection: &Connection,
    thread_id: &str,
) -> Result<HashMap<String, FixedPointUsageTotals>> {
    let groups = {
        let mut statement = connection.prepare(
            "SELECT agent_run_id,
                    strftime('%Y-%m-%dT%H:00:00.000000000Z',timestamp) activity_hour,
                    model,COALESCE(SUM(input_tokens),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(output_tokens),0),
                    COALESCE(SUM(reasoning_tokens),0),
                    COALESCE(SUM(total_tokens),0)
             FROM usage_facts
             WHERE thread_id=?1 AND agent_run_id IS NOT NULL
             GROUP BY agent_run_id,activity_hour,model",
        )?;
        statement
            .query_map([thread_id], |row| {
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
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals = HashMap::<String, FixedPointUsageTotals>::new();
    for (
        agent_run_id,
        activity_hour,
        model,
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
    ) in groups
    {
        let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
            connection,
            &aliases,
            &prices,
            UsageRollupScope::Agent(&agent_run_id),
            thread_id,
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals.entry(agent_run_id).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    Ok(totals)
}

fn query_agent_summary_on(connection: &Connection, thread_id: &str) -> Result<Vec<AgentSummary>> {
    let mut statement = connection.prepare(
        "SELECT a.id,a.agent_path,a.nickname,COALESCE(a.status,'running'),
                (SELECT COUNT(*) FROM turns tr WHERE tr.agent_run_id=a.id),
                (SELECT COUNT(*) FROM tool_calls tc WHERE tc.agent_run_id=a.id)
         FROM agent_runs a
         WHERE a.thread_id=?1 AND a.id<>a.thread_id
         ORDER BY a.started_at",
    )?;
    let rows = statement
        .query_map([thread_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?.max(0) as u64,
                row.get::<_, i64>(5)?.max(0) as u64,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut usage = query_agent_totals_on(connection, thread_id)?;
    Ok(rows
        .into_iter()
        .map(|(id, path, nickname, status, turn_count, tool_count)| {
            let totals = usage.remove(&id).unwrap_or_default().finish();
            let label = nickname
                .clone()
                .or_else(|| path.clone())
                .unwrap_or_else(|| "Primary agent".into());
            AgentSummary {
                id,
                label,
                path,
                nickname,
                status,
                turn_count,
                tool_count,
                total_tokens: totals.total_tokens,
                cost_usd: totals.cost_usd,
                unpriced_tokens: totals.unpriced_tokens,
            }
        })
        .collect())
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

const ACTIVITY_PREVIEW_CHARS: i64 = 240;
const ACTIVITY_AGENT_LABEL_PREVIEW_LIMIT: i64 = 8;
// Legacy messages are preview-only in Activity. Keep enough bytes from both
// ends to preserve wrapper metadata and the final `My request` section without
// ever materializing an entire JSONL payload.
const ACTIVITY_MESSAGE_PARSE_BYTES: i64 = 16 * 1024;
const ACTIVITY_MESSAGE_PARSE_EDGE_BYTES: i64 = ACTIVITY_MESSAGE_PARSE_BYTES / 2;
const LEGACY_ACTIVITY_PREFIX: &str = "legacy:";

#[derive(Clone, Debug)]
struct ActivityRootScope {
    id: String,
    started_at: String,
    next_started_at: Option<String>,
    open_left: bool,
}

#[derive(Clone, Default)]
struct ActivityDescendantGroup {
    turn_count: u64,
    timestamp: String,
    status: String,
    duration_ms: Option<i64>,
    labels: Vec<String>,
    label_count: u64,
    usage: FixedPointUsageTotals,
}

fn activity_agent_labels_preview(labels: &[String], label_count: u64) -> Option<String> {
    if labels.is_empty() {
        return None;
    }
    let mut preview = labels.join(" · ");
    let omitted = label_count.saturating_sub(labels.len() as u64);
    if omitted > 0 {
        preview.push_str(&format!(" · +{omitted} more"));
    }
    Some(preview)
}

#[derive(Clone, Default)]
struct ActivityRootAggregate {
    counts: ActivityCounts,
    usage: FixedPointUsageTotals,
    agents: Option<ActivityDescendantGroup>,
    reviews: Option<ActivityDescendantGroup>,
}

impl ActivityRootAggregate {
    fn group_mut(&mut self, reviews: bool) -> &mut ActivityDescendantGroup {
        if reviews {
            self.reviews
                .get_or_insert_with(ActivityDescendantGroup::default)
        } else {
            self.agents
                .get_or_insert_with(ActivityDescendantGroup::default)
        }
    }
}

#[derive(Default)]
struct ActivityDurationAccumulator {
    current: Option<(DateTime<Utc>, DateTime<Utc>)>,
    total_ms: i64,
}

impl ActivityDurationAccumulator {
    fn add(&mut self, started_at: &str, duration_ms: i64) {
        let Ok(started_at) = DateTime::parse_from_rfc3339(started_at) else {
            return;
        };
        let started_at = started_at.with_timezone(&Utc);
        let Some(duration) = Duration::try_milliseconds(duration_ms.max(0)) else {
            return;
        };
        let Some(ended_at) = started_at.checked_add_signed(duration) else {
            return;
        };
        if let Some((current_start, current_end)) = &mut self.current {
            if started_at <= *current_end {
                *current_end = (*current_end).max(ended_at);
                return;
            }
            self.total_ms = self
                .total_ms
                .saturating_add((*current_end - *current_start).num_milliseconds());
        }
        self.current = Some((started_at, ended_at));
    }

    fn finish(self) -> Option<i64> {
        let (started_at, ended_at) = self.current?;
        Some(
            self.total_ms
                .saturating_add((ended_at - started_at).num_milliseconds()),
        )
    }
}

fn load_activity_descendant_durations(
    connection: &Connection,
    roots: &mut HashMap<String, ActivityRootAggregate>,
) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT root_turn_id,review,started_at,duration_ms
         FROM selected_activity_descendants
         WHERE duration_ms IS NOT NULL
         ORDER BY root_turn_id,review,started_at,turn_id",
    )?;
    let mut accumulators = HashMap::<(String, bool), ActivityDurationAccumulator>::new();
    for row in statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? != 0,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })? {
        let (root_turn_id, reviews, started_at, duration_ms) = row?;
        accumulators
            .entry((root_turn_id, reviews))
            .or_default()
            .add(&started_at, duration_ms);
    }
    for ((root_turn_id, reviews), accumulator) in accumulators {
        if let Some(root) = roots.get_mut(&root_turn_id) {
            root.group_mut(reviews).duration_ms = accumulator.finish();
        }
    }
    Ok(())
}

#[derive(Default)]
struct ActivityBatch {
    user_messages: HashMap<String, Vec<String>>,
    roots: HashMap<String, ActivityRootAggregate>,
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
                 turn_id TEXT PRIMARY KEY,
                 root_turn_id TEXT NOT NULL,
                 usage_kind INTEGER NOT NULL
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS selected_activity_descendants(
                 turn_id TEXT PRIMARY KEY,
                 root_turn_id TEXT NOT NULL,
                 agent_key TEXT NOT NULL,
                 review INTEGER NOT NULL,
                 started_at TEXT NOT NULL,
                 status TEXT NOT NULL,
                 duration_ms INTEGER,
                 agent_label TEXT
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS idx_selected_activity_descendants_root
                 ON selected_activity_descendants(
                     root_turn_id,review,started_at DESC,turn_id DESC
                 );
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
             DELETE FROM selected_activity_descendants;
             DELETE FROM activity_explicit_agents;
             DELETE FROM selected_activity_agent_intervals;",
        )?;
        {
            let mut insert_root = connection.prepare(
                "INSERT INTO selected_activity_roots(
                     turn_id,started_at,next_started_at,open_left
                 ) VALUES(?1,?2,?3,?4)",
            )?;
            let mut insert_turn = connection.prepare(
                "INSERT INTO selected_activity_turns(turn_id,root_turn_id,usage_kind)
                 VALUES(?1,?1,0)",
            )?;
            for root in roots {
                insert_root.execute(params![
                    root.id,
                    root.started_at,
                    root.next_started_at,
                    root.open_left
                ])?;
                insert_turn.execute([&root.id])?;
                batch.roots.entry(root.id.clone()).or_default();
            }
        }

        let mut statement = connection.prepare(
            "WITH user_events AS (
                 SELECT e.turn_id,e.timestamp,e.source_line,e.id,
                        CAST(COALESCE(NULLIF(e.body,''),NULLIF(m.content,'')) AS BLOB) content
                 FROM events e
                 JOIN selected_activity_roots selected ON selected.turn_id=e.turn_id
                 LEFT JOIN messages m
                   ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
                 WHERE e.thread_id=?1 AND e.turn_id IS NOT NULL
                   AND (e.kind='user' OR e.role='user')
             )
             SELECT turn_id,
                    CASE WHEN length(content)<=?2 THEN content
                         ELSE substr(content,1,?3) END,
                    CASE WHEN length(content)<=?2 THEN NULL
                         ELSE substr(content,-?3) END
             FROM user_events WHERE content IS NOT NULL
             ORDER BY timestamp,source_line,id",
        )?;
        let rows = statement.query_map(
            params![
                thread_id,
                ACTIVITY_MESSAGE_PARSE_BYTES,
                ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            },
        )?;
        let mut seen_messages = HashMap::<String, HashSet<String>>::new();
        for row in rows {
            let (turn_id, head, tail) = row?;
            let content = activity_content_from_edges(head, tail);
            if let Some(message) = first_prompt_for_display(&content)
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
            batch.roots.entry(turn_id).or_default().counts.model_calls = count.max(0) as u64;
        }

        let explicit_agent_count = connection.execute(
            "INSERT OR IGNORE INTO activity_explicit_agents(agent_key)
             SELECT json_extract(link.payload_json,'$.agent_thread_id')
             FROM events link
             JOIN turns root_turn
               ON root_turn.id=link.turn_id AND root_turn.thread_id=link.thread_id
             WHERE link.thread_id=?1 AND link.kind='subagent'
               AND root_turn.rollout_id=?2
               AND json_extract(link.payload_json,'$.agent_thread_id') IS NOT NULL
               AND EXISTS(
                    SELECT 1 FROM turns descendant
                    WHERE descendant.thread_id=?1 AND descendant.rollout_id<>?2
                    LIMIT 1
               )",
            params![thread_id, root_rollout_id],
        )?;
        // Agent clocks can place the first child turn just before its spawn
        // event. Keep that first interval open on the left; every later link
        // transfers the reused identity to the newly linked root exchange.
        if explicit_agent_count > 0 {
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
        }
        // Attribution can cover hundreds of thousands of child turns. Keep
        // the complete mapping in SQLite and retain only one aggregate per
        // selected root in Rust.
        let descendant_count = connection.execute(
            "INSERT OR IGNORE INTO selected_activity_descendants(
                 turn_id,root_turn_id,agent_key,review,started_at,status,
                 duration_ms,agent_label
             )
             SELECT mapped.turn_id,mapped.root_turn_id,mapped.agent_key,
                    mapped.review,mapped.started_at,mapped.status,
                    mapped.duration_ms,COALESCE(a.nickname,a.agent_path)
             FROM (
                 SELECT t.id turn_id,explicit.root_turn_id,
                        COALESCE(t.agent_run_id,t.rollout_id) agent_key,
                        COALESCE(t.model='codex-auto-review',0) review,
                        t.started_at,t.status,t.duration_ms,t.agent_run_id,t.thread_id
                 FROM turns t
                 JOIN selected_activity_agent_intervals explicit
                   ON explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                  AND (explicit.linked_at IS NULL OR t.started_at>=explicit.linked_at)
                  AND (explicit.next_linked_at IS NULL
                       OR t.started_at<explicit.next_linked_at)
                 WHERE t.thread_id=?1 AND t.rollout_id<>?2
                 UNION ALL
                 SELECT t.id,selected.turn_id,
                        COALESCE(t.agent_run_id,t.rollout_id),
                        COALESCE(t.model='codex-auto-review',0),
                        t.started_at,t.status,t.duration_ms,t.agent_run_id,t.thread_id
                 FROM turns t
                 JOIN selected_activity_roots selected
                   ON t.started_at>=selected.started_at
                  AND (selected.next_started_at IS NULL
                       OR t.started_at<selected.next_started_at)
                 WHERE t.thread_id=?1 AND t.rollout_id<>?2
                   AND NOT EXISTS(
                       SELECT 1 FROM activity_explicit_agents explicit
                       WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                   )
             ) mapped
             LEFT JOIN agent_runs a
               ON a.id=mapped.agent_run_id AND a.thread_id=mapped.thread_id",
            params![thread_id, root_rollout_id],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO selected_activity_turns(
                 turn_id,root_turn_id,usage_kind
             )
             SELECT turn_id,root_turn_id,CASE WHEN review=1 THEN 2 ELSE 1 END
             FROM selected_activity_descendants",
            [],
        )?;

        if descendant_count > 0 {
            let mut statement = connection.prepare(
                "SELECT root_turn_id,
                    COUNT(DISTINCT CASE WHEN review=0 THEN agent_key END),
                    COALESCE(SUM(review=1),0)
             FROM selected_activity_descendants
             GROUP BY root_turn_id",
            )?;
            for row in statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })? {
                let (root_turn_id, agents, reviews) = row?;
                let counts = &mut batch.roots.entry(root_turn_id).or_default().counts;
                counts.agent_runs = agents.max(0) as u64;
                counts.reviews = reviews.max(0) as u64;
            }

            let mut statement = connection.prepare(
                "SELECT root_turn_id,review,COUNT(*),MAX(started_at),
                    COALESCE(MAX(status='running'),0),
                    COALESCE(MAX(status NOT IN ('completed','success','allowed')),0)
             FROM selected_activity_descendants
             GROUP BY root_turn_id,review",
            )?;
            for row in statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, i64>(2)?.max(0) as u64,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)? != 0,
                    row.get::<_, i64>(5)? != 0,
                ))
            })? {
                let (root_turn_id, reviews, turn_count, timestamp, running, attention) = row?;
                let group = batch
                    .roots
                    .entry(root_turn_id)
                    .or_default()
                    .group_mut(reviews);
                group.turn_count = turn_count;
                group.timestamp = timestamp;
                group.status = if running {
                    "running"
                } else if attention {
                    "attention"
                } else {
                    "completed"
                }
                .into();
            }

            let mut statement = connection.prepare(
                "WITH latest_labels AS (
                     SELECT root_turn_id,agent_label,started_at,turn_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY root_turn_id,agent_label
                                ORDER BY started_at DESC,turn_id DESC
                            ) label_rank
                     FROM selected_activity_descendants
                     WHERE review=0 AND agent_label IS NOT NULL
                 ), ranked_labels AS (
                     SELECT root_turn_id,agent_label,started_at,turn_id,
                            COUNT(*) OVER (PARTITION BY root_turn_id) label_count,
                            ROW_NUMBER() OVER (
                                PARTITION BY root_turn_id
                                ORDER BY started_at DESC,turn_id DESC
                            ) preview_rank
                     FROM latest_labels WHERE label_rank=1
                 )
                 SELECT root_turn_id,agent_label,label_count
                 FROM ranked_labels WHERE preview_rank<=?1
                 ORDER BY root_turn_id,preview_rank",
            )?;
            for row in statement.query_map([ACTIVITY_AGENT_LABEL_PREVIEW_LIMIT], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?.max(0) as u64,
                ))
            })? {
                let (root_turn_id, label, label_count) = row?;
                let group = batch
                    .roots
                    .entry(root_turn_id)
                    .or_default()
                    .group_mut(false);
                group.labels.push(label);
                group.label_count = label_count;
            }
            load_activity_descendant_durations(connection, &mut batch.roots)?;
        }

        let mut statement = connection.prepare(
            "SELECT root_turn_id,SUM(tool_calls) FROM (
                 SELECT selected.root_turn_id,COUNT(*) tool_calls
                 FROM selected_activity_turns selected
                 JOIN tool_calls tc ON tc.turn_id=selected.turn_id
                 WHERE tc.thread_id=?1
                 GROUP BY selected.root_turn_id
                 UNION ALL
                 SELECT selected.turn_id,COUNT(*)
                 FROM selected_activity_roots selected
                 JOIN tool_calls tc
                   ON tc.thread_id=?1
                  AND (selected.open_left=1 OR tc.started_at>=selected.started_at)
                  AND (selected.next_started_at IS NULL
                       OR tc.started_at<selected.next_started_at)
                 LEFT JOIN turns linked
                   ON linked.id=tc.turn_id AND linked.thread_id=tc.thread_id
                 WHERE linked.id IS NULL
                 GROUP BY selected.turn_id
             ) GROUP BY root_turn_id",
        )?;
        for row in statement.query_map([thread_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })? {
            let (root_turn_id, count) = row?;
            batch
                .roots
                .entry(root_turn_id)
                .or_default()
                .counts
                .tool_calls = count.max(0) as u64;
        }

        let (aliases, prices) = overview_prices_on(connection)?;
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
            let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
                connection,
                &aliases,
                &prices,
                UsageRollupScope::ActivitySelection {
                    root_turn_id: &root_turn_id,
                    usage_kind,
                },
                thread_id,
                &activity_hour,
                &model,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                total_tokens,
            )?;
            let root = batch.roots.entry(root_turn_id).or_default();
            root.usage.add_group(
                input_tokens as u64,
                cached_input_tokens as u64,
                output_tokens as u64,
                reasoning_tokens as u64,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
            );
            if usage_kind != 0 {
                root.group_mut(usage_kind == 2).usage.add_group(
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

        // A usage fact can survive without a turn link. These rows cannot be
        // pre-attributed to an exchange because ownership is interval based,
        // but they are sparse. Seek only the NULL-turn slice and price it in
        // fixed point rather than materializing priced_usage for every linked
        // fact in the selected turns.
        let mut statement = connection.prepare(
            "SELECT selected.turn_id,u.timestamp,u.model,u.input_tokens,
                    u.cached_input_tokens,u.output_tokens,u.reasoning_tokens,u.total_tokens
             FROM selected_activity_roots selected
             JOIN usage_facts u INDEXED BY idx_usage_turn_model_time
               ON u.thread_id=?1 AND u.turn_id IS NULL
              AND (selected.open_left=1 OR u.timestamp>=selected.started_at)
              AND (selected.next_started_at IS NULL OR u.timestamp<selected.next_started_at)",
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
            let (known_cost_numerator, unpriced_tokens) = overview_price_for(
                &aliases, &prices, &model, &timestamp,
            )
            .map_or((0, total_tokens), |(_, price)| {
                (
                    overview_cost_for_price(
                        price,
                        input_tokens - cached_input_tokens,
                        cached_input_tokens,
                        output_tokens,
                    ),
                    0,
                )
            });
            batch.roots.entry(turn_id).or_default().usage.add_group(
                input_tokens as u64,
                cached_input_tokens as u64,
                output_tokens as u64,
                reasoning_tokens as u64,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
            );
        }
        Ok(batch)
    }

    fn counts(&self, root_turn_id: &str) -> ActivityCounts {
        let mut counts = self
            .roots
            .get(root_turn_id)
            .map(|root| root.counts.clone())
            .unwrap_or_default();
        counts.follow_ups = self
            .user_messages
            .get(root_turn_id)
            .map_or(0, |messages| messages.len().saturating_sub(1) as u64);
        counts
    }

    fn exchange_totals(&self, root_turn_id: &str) -> Totals {
        self.roots
            .get(root_turn_id)
            .map(|root| root.usage.clone())
            .unwrap_or_default()
            .finish()
    }

    fn group(&self, root_turn_id: &str, reviews: bool) -> Option<&ActivityDescendantGroup> {
        let root = self.roots.get(root_turn_id)?;
        if reviews {
            root.reviews.as_ref()
        } else {
            root.agents.as_ref()
        }
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
        let item = query_legacy_activity_item(connection, thread_id, &root_rollout_id)?;
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
        _next_started_at,
        _open_left,
    ) in turn_rows
    {
        let messages = batch
            .user_messages
            .get(&turn_id)
            .cloned()
            .unwrap_or_default();
        let counts = batch.counts(&turn_id);
        let totals = batch.exchange_totals(&turn_id);
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
            child_next_cursor: None,
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

fn validate_activity_detail_cursor(value: &str, thread_id: &str, item_id: &str) -> Result<()> {
    if item_id == legacy_activity_id(thread_id) || parse_activity_group_id(item_id).is_some() {
        decode_activity_collection_cursor_for(value, thread_id, item_id).map(|_| ())
    } else {
        activity_index::validate_cursor_for(value, thread_id, item_id)
    }
}

fn encode_activity_collection_cursor(
    thread_id: &str,
    item_id: &str,
    timestamp: &str,
    source_line: Option<i64>,
    sort_id: &str,
) -> Result<String> {
    serde_json::to_string(&ActivityCollectionCursor {
        version: 1,
        thread_id: thread_id.to_owned(),
        item_id: item_id.to_owned(),
        timestamp: timestamp.to_owned(),
        source_line,
        sort_id: sort_id.to_owned(),
    })
    .context("failed to encode Activity collection cursor")
}

fn decode_activity_collection_cursor_for(
    value: &str,
    thread_id: &str,
    item_id: &str,
) -> Result<ActivityCollectionCursor> {
    if value.len() > 4_096 {
        return Err(anyhow!("Activity cursor is too long"));
    }
    let cursor: ActivityCollectionCursor =
        serde_json::from_str(value).context("invalid Activity collection cursor")?;
    if cursor.version != 1
        || cursor.thread_id != thread_id
        || cursor.item_id != item_id
        || cursor.timestamp.is_empty()
        || cursor
            .source_line
            .is_some_and(|source_line| source_line < 0)
        || cursor.sort_id.is_empty()
    {
        return Err(anyhow!("Activity cursor belongs to a different collection"));
    }
    Ok(cursor)
}

fn query_legacy_activity_item(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
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

    let has_messages = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE thread_id=?1)",
        [thread_id],
        |row| row.get::<_, i64>(0),
    )? != 0;
    let mut first_user = None;
    let mut statement = connection.prepare(
        "SELECT CASE WHEN length(CAST(content AS BLOB))<=?2
                     THEN CAST(content AS BLOB)
                     ELSE substr(CAST(content AS BLOB),1,?3) END,
                CASE WHEN length(CAST(content AS BLOB))<=?2 THEN NULL
                     ELSE substr(CAST(content AS BLOB),-?3) END
         FROM messages
         WHERE thread_id=?1 AND role='user'
         ORDER BY timestamp,source_line,id",
    )?;
    let mut rows = statement.query(params![
        thread_id,
        ACTIVITY_MESSAGE_PARSE_BYTES,
        ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
    ])?;
    while let Some(row) = rows.next()? {
        let content = activity_content_from_edges(
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Option<Vec<u8>>>(1)?,
        );
        if let Some(display) = first_prompt_for_display(&content) {
            first_user = Some(display);
            break;
        }
    }
    let latest_assistant = connection
        .query_row(
            "SELECT CASE WHEN length(CAST(content AS BLOB))<=?2
                         THEN CAST(content AS BLOB)
                         ELSE substr(CAST(content AS BLOB),1,?3) END,
                    CASE WHEN length(CAST(content AS BLOB))<=?2 THEN NULL
                         ELSE substr(CAST(content AS BLOB),-?3) END
             FROM messages
             WHERE thread_id=?1 AND role='assistant'
             ORDER BY timestamp DESC,source_line DESC,id DESC LIMIT 1",
            params![
                thread_id,
                ACTIVITY_MESSAGE_PARSE_BYTES,
                ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
            ],
            |row| {
                Ok(activity_content_from_edges(
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                ))
            },
        )
        .optional()?
        .map(|content| redact_data_urls(&content));

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
        children: Vec::new(),
        child_page: None,
        child_page_size: None,
        child_total: None,
        child_has_more: None,
        child_next_cursor: None,
        usage: Some(totals),
        counts: None,
    }))
}

const LEGACY_ACTIVITY_CHILDREN_CTE: &str = "WITH canonical_event_ids(event_id) AS MATERIALIZED (
         SELECT substr(MIN(printf('%020d%s',source_line,event_id)),21)
         FROM activity_event_index
         WHERE thread_id=?1
         GROUP BY canonical_key
     ),
     canonical_events AS MATERIALIZED (
         SELECT projected.event_id,projected.timestamp,projected.source_line
         FROM canonical_event_ids canonical
         JOIN activity_event_index projected ON projected.event_id=canonical.event_id
     ),
     visible_messages AS MATERIALIZED (
         SELECT m.id,m.timestamp,m.source_line
         FROM messages m
         WHERE m.thread_id=?1
           AND NOT EXISTS(
               SELECT 1
               FROM canonical_events projected
               JOIN events e ON e.id=projected.event_id AND e.thread_id=?1
               LEFT JOIN messages event_message
                 ON event_message.id=COALESCE(e.call_id,e.id)
                AND event_message.thread_id=e.thread_id
               WHERE e.id=m.id
                  OR (
                       projected.timestamp=m.timestamp
                       AND e.kind<>'tool_call'
                       AND length(trim(COALESCE(
                           NULLIF(e.body,''),NULLIF(event_message.content,'')
                       )))<=?2
                       AND trim(COALESCE(
                           NULLIF(e.body,''),NULLIF(event_message.content,'')
                       ))=trim(m.content)
                  )
           )
     ) ";

#[derive(Debug)]
struct LegacyActivityChildRef {
    message: bool,
    id: String,
    source_line: i64,
    timestamp: String,
    sort_id: String,
}

fn query_legacy_activity_children_page(
    connection: &Connection,
    thread_id: &str,
    requested_page: u64,
    page_size: u64,
    child_cursor: Option<&str>,
) -> Result<ActivityChildrenPage> {
    let item_id = legacy_activity_id(thread_id);
    let cursor = child_cursor
        .map(|value| decode_activity_collection_cursor_for(value, thread_id, &item_id))
        .transpose()?;
    let total_sql = format!(
        "{LEGACY_ACTIVITY_CHILDREN_CTE}
         SELECT (SELECT COUNT(*) FROM canonical_events)
              + (SELECT COUNT(*) FROM visible_messages)"
    );
    let total = connection
        .query_row(
            &total_sql,
            params![thread_id, ACTIVITY_PREVIEW_CHARS],
            |row| row.get::<_, i64>(0),
        )?
        .max(0) as u64;
    let total_pages = total.div_ceil(page_size).max(1);
    let page = if cursor.is_some() {
        requested_page.max(1)
    } else {
        requested_page.max(1).min(total_pages)
    };
    let offset = page
        .saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64;
    let fetch_limit = page_size.saturating_add(1).min(i64::MAX as u64) as i64;
    let collection_sql = "SELECT child_kind,child_id,source_line,timestamp,sort_id FROM (
             SELECT 0 child_kind,event_id child_id,timestamp,source_line,event_id sort_id
             FROM canonical_events
             UNION ALL
             SELECT 1 child_kind,id child_id,timestamp,source_line,
                    'legacy-message:' || id sort_id
             FROM visible_messages
         )";
    let row_from_sql = |row: &Row<'_>| {
        Ok(LegacyActivityChildRef {
            message: row.get::<_, i64>(0)? != 0,
            id: row.get(1)?,
            source_line: row.get(2)?,
            timestamp: row.get(3)?,
            sort_id: row.get(4)?,
        })
    };
    // Continue pre-source-line cursors with their original ordering. Switching
    // ordering halfway through a collection can skip equal-timestamp records.
    let legacy_cursor_order = cursor
        .as_ref()
        .is_some_and(|cursor| cursor.source_line.is_none());
    let mut selected = if let Some(cursor) = cursor.as_ref() {
        if let Some(source_line) = cursor.source_line {
            let page_sql = format!(
                "{LEGACY_ACTIVITY_CHILDREN_CTE}
                 {collection_sql}
                 WHERE (timestamp,source_line,sort_id)<(?3,?4,?5)
                 ORDER BY timestamp DESC,source_line DESC,sort_id DESC
                 LIMIT ?6"
            );
            connection
                .prepare(&page_sql)?
                .query_map(
                    params![
                        thread_id,
                        ACTIVITY_PREVIEW_CHARS,
                        cursor.timestamp,
                        source_line,
                        cursor.sort_id,
                        fetch_limit
                    ],
                    row_from_sql,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            let page_sql = format!(
                "{LEGACY_ACTIVITY_CHILDREN_CTE}
                 {collection_sql}
                 WHERE (timestamp,sort_id)<(?3,?4)
                 ORDER BY timestamp DESC,sort_id DESC
                 LIMIT ?5"
            );
            connection
                .prepare(&page_sql)?
                .query_map(
                    params![
                        thread_id,
                        ACTIVITY_PREVIEW_CHARS,
                        cursor.timestamp,
                        cursor.sort_id,
                        fetch_limit
                    ],
                    row_from_sql,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        }
    } else {
        let page_sql = format!(
            "{LEGACY_ACTIVITY_CHILDREN_CTE}
             {collection_sql}
             ORDER BY timestamp DESC,source_line DESC,sort_id DESC
             LIMIT ?3 OFFSET ?4"
        );
        connection
            .prepare(&page_sql)?
            .query_map(
                params![thread_id, ACTIVITY_PREVIEW_CHARS, fetch_limit, offset],
                row_from_sql,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let has_more = selected.len() as u64 > page_size;
    if has_more {
        selected.truncate(page_size as usize);
    }
    let next_cursor = if has_more {
        selected
            .last()
            .map(|child| {
                encode_activity_collection_cursor(
                    thread_id,
                    &item_id,
                    &child.timestamp,
                    (!legacy_cursor_order).then_some(child.source_line),
                    &child.sort_id,
                )
            })
            .transpose()?
    } else {
        None
    };

    let indexed = selected
        .iter()
        .filter(|child| !child.message)
        .map(|child| activity_index::IndexedActivityEvent {
            event_id: child.id.clone(),
            source_line: child.source_line,
        })
        .collect::<Vec<_>>();
    let event_items = query_activity_child_preview_rows(connection, thread_id, None, &indexed)?
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<HashMap<_, _>>();
    let message_ids = selected
        .iter()
        .filter(|child| child.message)
        .map(|child| child.id.clone())
        .collect::<Vec<_>>();
    let message_items = query_legacy_message_child_rows(connection, thread_id, &message_ids)?;

    let mut items = Vec::with_capacity(selected.len());
    for child in selected {
        let item = if child.message {
            message_items.get(&child.id)
        } else {
            event_items.get(&child.id)
        };
        if let Some(item) = item {
            items.push(item.clone());
        }
    }
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more,
        next_cursor,
    })
}

fn query_legacy_message_child_rows(
    connection: &Connection,
    thread_id: &str,
    message_ids: &[String],
) -> Result<HashMap<String, ActivityItem>> {
    if message_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let requested = serde_json::to_string(message_ids)?;
    let mut statement = connection.prepare(
        "SELECT m.id,m.rollout_id,m.turn_id,m.timestamp,m.role,
                CASE WHEN length(CAST(m.content AS BLOB))<=?3
                     THEN CAST(m.content AS BLOB)
                     ELSE substr(CAST(m.content AS BLOB),1,?4) END,
                CASE WHEN length(CAST(m.content AS BLOB))<=?3 THEN NULL
                     ELSE substr(CAST(m.content AS BLOB),-?4) END
         FROM json_each(?1) requested
         JOIN messages m ON m.id=requested.value AND m.thread_id=?2",
    )?;
    let rows = statement.query_map(
        params![
            requested,
            thread_id,
            ACTIVITY_MESSAGE_PARSE_BYTES,
            ACTIVITY_MESSAGE_PARSE_EDGE_BYTES
        ],
        |row| {
            let id = row.get::<_, String>(0)?;
            let role = row.get::<_, String>(4)?;
            let head = row.get::<_, Vec<u8>>(5)?;
            let tail = row.get::<_, Option<Vec<u8>>>(6)?;
            let content = activity_content_from_edges(head, tail);
            let body = if role == "user" {
                bounded_preview(first_prompt_for_display(&content))
            } else {
                bounded_preview(Some(content))
            };
            Ok(ActivityItem {
                id: format!("legacy-message:{id}"),
                turn_id: row.get(2)?,
                rollout_id: row.get(1)?,
                agent_run_id: None,
                agent_label: None,
                timestamp: row.get(3)?,
                kind: if role == "user" { "user" } else { "final" }.into(),
                role: Some(role),
                label: None,
                body,
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
                child_next_cursor: None,
                usage: None,
                counts: None,
            })
        },
    )?;
    let mut items = HashMap::with_capacity(message_ids.len());
    for row in rows {
        let item = row?;
        let id = item
            .id
            .strip_prefix("legacy-message:")
            .unwrap_or(&item.id)
            .to_owned();
        items.insert(id, item);
    }
    Ok(items)
}

fn activity_content_from_edges(head: Vec<u8>, tail: Option<Vec<u8>>) -> String {
    let mut content = String::from_utf8_lossy(&head).into_owned();
    if let Some(tail) = tail {
        content.push_str("\n…\n");
        content.push_str(&String::from_utf8_lossy(&tail));
    }
    content
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
    child_page_size: u64,
) -> Vec<ActivityItem> {
    let mut groups = Vec::new();
    if let Some(agent_group) = batch
        .group(root_turn_id, false)
        .filter(|group| group.turn_count > 0)
    {
        groups.push(ActivityItem {
            id: format!("group:agents:{root_turn_id}"),
            turn_id: Some(root_turn_id.to_owned()),
            rollout_id: root_rollout_id.to_owned(),
            agent_run_id: None,
            agent_label: None,
            timestamp: agent_group.timestamp.clone(),
            kind: "agent_group".into(),
            role: None,
            label: Some(format!("Agents · {}", counts.agent_runs)),
            body: activity_agent_labels_preview(&agent_group.labels, agent_group.label_count),
            status: Some(agent_group.status.clone()),
            tool_name: None,
            duration_ms: agent_group.duration_ms,
            model: None,
            effort: None,
            has_details: true,
            children: Vec::new(),
            child_page: Some(1),
            child_page_size: Some(child_page_size),
            child_total: Some(agent_group.turn_count),
            child_has_more: Some(true),
            child_next_cursor: None,
            usage: Some(agent_group.usage.clone().finish()),
            counts: None,
        });
    }

    if let Some(review_group) = batch
        .group(root_turn_id, true)
        .filter(|group| group.turn_count > 0)
    {
        groups.push(ActivityItem {
            id: format!("group:reviews:{root_turn_id}"),
            turn_id: Some(root_turn_id.to_owned()),
            rollout_id: root_rollout_id.to_owned(),
            agent_run_id: None,
            agent_label: None,
            timestamp: review_group.timestamp.clone(),
            kind: "review_group".into(),
            role: None,
            label: Some(format!("Automated reviews · {}", counts.reviews)),
            body: None,
            status: Some(review_group.status.clone()),
            tool_name: None,
            duration_ms: review_group.duration_ms,
            model: None,
            effort: None,
            has_details: true,
            children: Vec::new(),
            child_page: Some(1),
            child_page_size: Some(child_page_size),
            child_total: Some(review_group.turn_count),
            child_has_more: Some(true),
            child_next_cursor: None,
            usage: Some(review_group.usage.clone().finish()),
            counts: None,
        });
    }
    groups
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
    let intervals_may_cross_local_date = match thread_bounds {
        Some((start, end)) => {
            let occupied_end = end
                .checked_sub_signed(Duration::milliseconds(1))
                .unwrap_or(end);
            start.with_timezone(&Local).date_naive()
                != occupied_end.with_timezone(&Local).date_naive()
        }
        None => true,
    };
    let mut dates = HashSet::new();
    let mut turn_intervals = Vec::new();
    let mut statement = connection.prepare(
        "SELECT started_at,completed_at,duration_ms
         FROM turns INDEXED BY idx_turns_thread_time
         WHERE thread_id=?1",
    )?;
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<i64>>(2)?,
        ))
    })? {
        let (raw_start, raw_end, duration_ms) = row?;
        record_activity_row(
            &mut dates,
            &mut turn_intervals,
            true,
            &raw_start,
            raw_end.as_deref(),
            duration_ms,
            thread_bounds,
        );
    }
    drop(statement);

    // Point activity is overwhelmingly concentrated in a handful of calendar days, but a
    // large session can contain hundreds of thousands of rows on each one. Seek the first
    // indexed timestamp at or after each local midnight, record that occupied date, then
    // jump straight to the next midnight. This makes the collapsed Activity query
    // proportional to occupied days instead of raw events/tool calls and also avoids the
    // temporary B-trees created by GROUP BY date(...,'localtime').
    let mut statement = connection.prepare(
        "SELECT MIN(activity_at) FROM (
             SELECT (
                 SELECT timestamp FROM events INDEXED BY idx_events_thread_time
                 WHERE thread_id=?1 AND timestamp>=?2
                 ORDER BY timestamp,source_line LIMIT 1
             ) activity_at
             UNION ALL
             SELECT (
                 SELECT timestamp FROM messages INDEXED BY idx_messages_thread_time
                 WHERE thread_id=?1 AND timestamp>=?2
                 ORDER BY timestamp LIMIT 1
             )
             UNION ALL
             SELECT (
                 SELECT started_at FROM tool_calls INDEXED BY idx_tools_thread_time
                 WHERE thread_id=?1 AND started_at>=?2
                 ORDER BY started_at LIMIT 1
             )
         )
         WHERE activity_at IS NOT NULL",
    )?;
    let mut cursor = "0001-01-01T00:00:00".to_owned();
    loop {
        let next_timestamp = statement
            .query_row(params![thread_id, cursor], |row| {
                row.get::<_, Option<String>>(0)
            })?
            .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok());
        let Some(next_timestamp) = next_timestamp else {
            break;
        };
        let date = next_timestamp.with_timezone(&Local).date_naive();
        dates.insert(date);
        let Some(next_date) = date.succ_opt() else {
            break;
        };
        let next_cursor = local_midnight(next_date)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        if next_cursor <= cursor {
            break;
        }
        cursor = next_cursor;
    }
    drop(statement);

    // Exact interval handling is only needed when the trustworthy thread bounds cross a
    // local date. The ordinary (and pathological dense) one-day case performs no interval
    // scan at all; multi-day sessions keep the previous exact cross-midnight behavior.
    if intervals_may_cross_local_date {
        let mut statement = connection.prepare(
            "SELECT timestamp,duration_ms
             FROM events INDEXED BY idx_events_thread_time
             WHERE thread_id=?1 AND COALESCE(duration_ms,0)>0",
        )?;
        let rows = statement.query_map([thread_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })?;
        for row in rows {
            let (raw_start, duration_ms) = row?;
            record_activity_row(
                &mut dates,
                &mut turn_intervals,
                false,
                &raw_start,
                None,
                duration_ms,
                thread_bounds,
            );
        }

        let mut statement = connection.prepare(
            "SELECT started_at,completed_at,duration_ms
             FROM tool_calls INDEXED BY idx_tools_thread_time
             WHERE thread_id=?1
               AND (completed_at IS NOT NULL OR COALESCE(duration_ms,0)>0)",
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

    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals_by_date = HashMap::<NaiveDate, FixedPointUsageTotals>::new();
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
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?.max(0),
            row.get::<_, i64>(3)?.max(0),
            row.get::<_, i64>(4)?.max(0),
            row.get::<_, i64>(5)?.max(0),
            row.get::<_, i64>(6)?.max(0) as u64,
        ))
    })? {
        let (
            activity_hour,
            model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        ) = row?;
        let (hour_start, hour_end) = usage_rollup_hour_window(&activity_hour)?;
        let (start_date, end_date) = usage_rollup_bucket_dates(hour_start, hour_end, &Local);
        if start_date == end_date {
            let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
                connection,
                &aliases,
                &prices,
                UsageRollupScope::Thread,
                thread_id,
                &activity_hour,
                &model,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                total_tokens,
            )?;
            dates.insert(start_date);
            totals_by_date.entry(start_date).or_default().add_group(
                input_tokens as u64,
                cached_input_tokens as u64,
                output_tokens as u64,
                reasoning_tokens as u64,
                total_tokens,
                known_cost_numerator,
                unpriced_tokens,
            );
        } else {
            // Sub-hour UTC offsets can place local midnight inside a UTC-hour
            // bucket. Only that boundary bucket falls back to raw indexed rows;
            // every full bucket remains compact and time-zone independent.
            let split_totals = usage_rollup_local_day_splits_on(
                connection,
                &aliases,
                &prices,
                UsageRollupExceptionalQuery {
                    scope: UsageRollupScope::Thread,
                    thread_id,
                    model: &model,
                    start: &sql_timestamp(hour_start),
                    end: &sql_timestamp(hour_end),
                },
            )?;
            for (date, totals) in split_totals {
                dates.insert(date);
                totals_by_date.entry(date).or_default().merge(totals);
            }
        }
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
    let start = local_midnight(date);
    let end = local_midnight(next_date);
    (start < end).then_some((start, end))
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

#[cfg(test)]
fn query_activity_detail_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    child_page: u64,
    child_page_size: u64,
) -> Result<Option<ActivityItem>> {
    query_activity_detail_cursor_page_on(
        connection,
        thread_id,
        item_id,
        child_page,
        child_page_size,
        None,
    )
}

fn query_activity_detail_cursor_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    child_page: u64,
    child_page_size: u64,
    child_cursor: Option<&str>,
) -> Result<Option<ActivityItem>> {
    let root_rollout_id = query_root_rollout_id(connection, thread_id)?;
    if item_id == legacy_activity_id(thread_id) {
        let Some(mut item) = query_legacy_activity_item(connection, thread_id, &root_rollout_id)?
        else {
            return Ok(None);
        };
        let child_page = query_legacy_activity_children_page(
            connection,
            thread_id,
            child_page,
            child_page_size,
            child_cursor,
        )?;
        item.children = child_page.items;
        item.child_page = Some(child_page.page);
        item.child_page_size = Some(child_page.page_size);
        item.child_total = Some(child_page.total);
        item.child_has_more = Some(child_page.has_more);
        item.child_next_cursor = child_page.next_cursor;
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
            child_cursor,
        );
    }
    if let Some(mut turn) = connection
        .query_row(
            "SELECT t.id,t.rollout_id,t.agent_run_id,t.started_at,t.status,t.model,t.effort,
                    NULLIF(substr(t.last_agent_message,1,?3),'') last_agent_message,
                    t.duration_ms,a.nickname,a.agent_path
             FROM turns t LEFT JOIN agent_runs a
               ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
             WHERE t.thread_id=?1 AND t.id=?2",
            params![thread_id, item_id, ACTIVITY_PREVIEW_CHARS + 1],
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
                    body: bounded_preview(row.get::<_, Option<String>>(7)?),
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
                    child_next_cursor: None,
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
            let counts = batch.counts(item_id);
            turn.kind = "exchange".into();
            turn.role = Some("user".into());
            turn.label = Some(
                bounded_preview(messages.first().cloned()).unwrap_or_else(|| "Conversation".into()),
            );
            turn.counts = Some(counts.clone());
            turn.usage = Some(batch.exchange_totals(item_id));
            let child_page = query_activity_child_previews_cursor_page(
                connection,
                thread_id,
                item_id,
                child_page,
                child_page_size,
                child_cursor,
            )?;
            turn.children = child_page.items;
            let groups =
                query_exchange_groups(&batch, item_id, &root_rollout_id, &counts, child_page_size);
            turn.children.extend(groups);
            turn.child_page = Some(child_page.page);
            turn.child_page_size = Some(child_page.page_size);
            turn.child_total = Some(child_page.total);
            turn.child_has_more = Some(child_page.has_more);
            turn.child_next_cursor = child_page.next_cursor;
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
            let child_page = query_activity_child_previews_cursor_page(
                connection,
                thread_id,
                item_id,
                child_page,
                child_page_size,
                child_cursor,
            )?;
            turn.children = child_page.items;
            turn.child_page = Some(child_page.page);
            turn.child_page_size = Some(child_page.page_size);
            turn.child_total = Some(child_page.total);
            turn.child_has_more = Some(child_page.has_more);
            turn.child_next_cursor = child_page.next_cursor;
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

    let event = connection
        .query_row(
            "SELECT e.id,e.turn_id,e.rollout_id,e.agent_run_id,e.timestamp,e.kind,e.role,e.label,
                    COALESCE(e.body,m.content),COALESCE(tc.status,e.status),
                    COALESCE(tc.name,e.tool_name),COALESCE(
                        tc.duration_ms,e.duration_ms,
                        CASE WHEN tc.completed_at IS NOT NULL THEN
                            CAST(ROUND((julianday(tc.completed_at)-julianday(tc.started_at))*86400000.0)
                                AS INTEGER)
                        END),
                    e.model,e.effort,a.nickname,a.agent_path,tc.namespace,e.source_line,
                    e.call_id
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
                let kind = normalize_activity_kind(&stored_kind, role.as_deref());
                Ok((
                    ActivityItem {
                        id: row.get(0)?,
                        turn_id: row.get(1)?,
                        rollout_id: row.get(2)?,
                        agent_run_id: row.get(3)?,
                        agent_label,
                        timestamp: row.get(4)?,
                        kind,
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
                        child_next_cursor: None,
                        usage: None,
                        counts: None,
                    },
                    stored_kind,
                    row.get::<_, i64>(17)?,
                    row.get::<_, Option<String>>(18)?,
                ))
            },
        )
        .optional()?;
    let Some((mut item, stored_kind, source_line, call_id)) = event else {
        return Ok(None);
    };
    if item.turn_id.is_some() {
        item.usage = query_activity_event_usage_on(
            connection,
            thread_id,
            &item,
            &stored_kind,
            source_line,
            call_id.as_deref(),
        )?;
    }
    Ok(Some(item))
}

fn query_activity_event_usage_on(
    connection: &Connection,
    thread_id: &str,
    item: &ActivityItem,
    stored_kind: &str,
    source_line: i64,
    call_id: Option<&str>,
) -> Result<Option<Totals>> {
    if !matches!(
        item.kind.as_str(),
        "assistant" | "update" | "final" | "reasoning" | "tool" | "subagent"
    ) {
        return Ok(None);
    }
    let turn_id = item.turn_id.as_deref();
    let visible = if stored_kind == "tool_call" && call_id.is_some() {
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
                item.rollout_id,
                turn_id,
                call_id,
                source_line,
                item.id
            ],
            |row| row.get::<_, i64>(0),
        )? != 0
    } else if stored_kind == "turn_completed" {
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
            params![thread_id, turn_id],
            |row| row.get::<_, i64>(0),
        )? != 0
    } else {
        true
    };
    if !visible {
        return Ok(None);
    }

    let next_source_line = source_line.saturating_add(1);
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
        params![thread_id, item.rollout_id, turn_id, next_source_line],
        |row| row.get::<_, i64>(0),
    )? != 0;
    let second_source_line = source_line.saturating_add(2);
    let (aliases, prices) = overview_prices_on(connection)?;
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
        item.rollout_id,
        turn_id,
        next_source_line,
        second_source_line,
        i64::from(following_owner)
    ])?;
    let mut totals = FixedPointUsageTotals::default();
    let mut usage_rows = 0u64;
    while let Some(row) = rows.next()? {
        let timestamp = row.get::<_, String>(0)?;
        let model = row.get::<_, String>(1)?;
        add_usage_fact_to_totals(
            &mut totals,
            &aliases,
            &prices,
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
    child_cursor: Option<&str>,
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
    prepare_activity_group_turns(connection, thread_id, root_rollout_id, &root_scope, reviews)?;
    let child_total = connection
        .query_row(
            "SELECT COUNT(*) FROM selected_activity_group_turns",
            [],
            |row| row.get::<_, i64>(0),
        )?
        .max(0) as u64;
    if child_total == 0 {
        return Ok(None);
    }
    let children = query_activity_group_child_page_on(
        connection,
        thread_id,
        &format!(
            "group:{}:{root_turn_id}",
            if reviews { "reviews" } else { "agents" }
        ),
        child_page,
        child_page_size,
        child_total,
        child_cursor,
    )?;
    let timestamp = connection.query_row(
        "SELECT t.started_at
         FROM selected_activity_group_turns selected
         JOIN turns t ON t.id=selected.turn_id
         ORDER BY t.started_at DESC,t.id DESC LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )?;
    let status = query_activity_group_status_on(connection)?;
    let duration_ms = query_activity_group_duration_on(connection)?;
    let usage = query_activity_group_totals_on(connection, thread_id)?;
    let (label, body) = if reviews {
        (format!("Automated reviews · {child_total}"), None)
    } else {
        let agent_count = connection
            .query_row(
                "SELECT COUNT(DISTINCT agent_key)
                 FROM selected_activity_group_turns",
                [],
                |row| row.get::<_, i64>(0),
            )?
            .max(0) as u64;
        (
            format!("Agents · {agent_count}"),
            query_activity_group_labels_on(connection)?,
        )
    };
    Ok(Some(ActivityItem {
        id: format!(
            "group:{}:{root_turn_id}",
            if reviews { "reviews" } else { "agents" }
        ),
        turn_id: Some(root_turn_id.to_owned()),
        rollout_id: root_rollout_id.to_owned(),
        agent_run_id: None,
        agent_label: None,
        timestamp,
        kind: if reviews {
            "review_group".into()
        } else {
            "agent_group".into()
        },
        role: None,
        label: Some(label),
        body,
        status: Some(status),
        tool_name: None,
        duration_ms,
        model: None,
        effort: None,
        has_details: true,
        children: children.items,
        child_page: Some(children.page),
        child_page_size: Some(children.page_size),
        child_total: Some(children.total),
        child_has_more: Some(children.has_more),
        child_next_cursor: children.next_cursor,
        usage: Some(usage),
        counts: None,
    }))
}

fn prepare_activity_group_turns(
    connection: &Connection,
    thread_id: &str,
    root_rollout_id: &str,
    root: &ActivityRootScope,
    reviews: bool,
) -> Result<()> {
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS selected_activity_group_turns(
             turn_id TEXT PRIMARY KEY,
             agent_key TEXT NOT NULL,
             started_at TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS idx_selected_activity_group_turns_time
             ON selected_activity_group_turns(started_at DESC,turn_id DESC);
         DELETE FROM selected_activity_group_turns;",
    )?;
    connection.execute(
        "WITH links AS MATERIALIZED (
             SELECT json_extract(event.payload_json,'$.agent_thread_id') agent_key,
                    event.turn_id root_turn_id,event.timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY json_extract(event.payload_json,'$.agent_thread_id')
                        ORDER BY event.timestamp,event.source_line,event.id
                    ) link_rank,
                    LEAD(event.timestamp) OVER (
                        PARTITION BY json_extract(event.payload_json,'$.agent_thread_id')
                        ORDER BY event.timestamp,event.source_line,event.id
                    ) next_linked_at
             FROM events event
             JOIN turns root_turn
               ON root_turn.id=event.turn_id AND root_turn.thread_id=event.thread_id
             WHERE event.thread_id=?1 AND event.kind='subagent'
               AND root_turn.rollout_id=?2
               AND json_extract(event.payload_json,'$.agent_thread_id') IS NOT NULL
               AND EXISTS(
                    SELECT 1 FROM turns descendant
                    WHERE descendant.thread_id=?1 AND descendant.rollout_id<>?2
                    LIMIT 1
               )
         ),
         explicit_agents AS MATERIALIZED (
             SELECT DISTINCT agent_key FROM links
         ),
         selected_intervals AS MATERIALIZED (
             SELECT agent_key,
                    CASE WHEN link_rank=1 THEN NULL ELSE timestamp END linked_at,
                    next_linked_at
             FROM links WHERE root_turn_id=?3
         )
         INSERT INTO selected_activity_group_turns(turn_id,agent_key,started_at)
         SELECT t.id,COALESCE(t.agent_run_id,t.rollout_id),t.started_at
         FROM turns t
         WHERE t.thread_id=?1 AND t.rollout_id<>?2
           AND (COALESCE(t.model='codex-auto-review',0)=?6)
           AND (
                (
                    EXISTS(
                        SELECT 1 FROM explicit_agents explicit
                        WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                    )
                    AND EXISTS(
                        SELECT 1 FROM selected_intervals selected
                        WHERE selected.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                          AND (selected.linked_at IS NULL OR t.started_at>=selected.linked_at)
                          AND (selected.next_linked_at IS NULL
                               OR t.started_at<selected.next_linked_at)
                    )
                )
                OR (
                    NOT EXISTS(
                        SELECT 1 FROM explicit_agents explicit
                        WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                    )
                    AND t.started_at>=?4
                    AND (?5 IS NULL OR t.started_at<?5)
                )
           )",
        params![
            thread_id,
            root_rollout_id,
            root.id,
            root.started_at,
            root.next_started_at,
            i64::from(reviews)
        ],
    )?;
    Ok(())
}

struct ActivityGroupChildRef {
    id: String,
    timestamp: String,
}

fn query_activity_group_child_page_on(
    connection: &Connection,
    thread_id: &str,
    item_id: &str,
    requested_page: u64,
    page_size: u64,
    total: u64,
    child_cursor: Option<&str>,
) -> Result<ActivityChildrenPage> {
    let cursor = child_cursor
        .map(|value| decode_activity_collection_cursor_for(value, thread_id, item_id))
        .transpose()?;
    let total_pages = total.div_ceil(page_size).max(1);
    let page = if cursor.is_some() {
        requested_page.max(1)
    } else {
        requested_page.max(1).min(total_pages)
    };
    let offset = page
        .saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64;
    let fetch_limit = page_size.saturating_add(1).min(i64::MAX as u64) as i64;
    let mut selected = if let Some(cursor) = cursor.as_ref() {
        let mut statement = connection.prepare(
            "SELECT selected.turn_id,selected.started_at
             FROM selected_activity_group_turns selected
                  INDEXED BY idx_selected_activity_group_turns_time
             WHERE (selected.started_at,selected.turn_id)<(?1,?2)
             ORDER BY selected.started_at DESC,selected.turn_id DESC
             LIMIT ?3",
        )?;
        statement
            .query_map(
                params![cursor.timestamp, cursor.sort_id, fetch_limit],
                |row| {
                    Ok(ActivityGroupChildRef {
                        id: row.get(0)?,
                        timestamp: row.get(1)?,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
    } else {
        let mut statement = connection.prepare(
            "SELECT selected.turn_id,selected.started_at
             FROM selected_activity_group_turns selected
                  INDEXED BY idx_selected_activity_group_turns_time
             ORDER BY selected.started_at DESC,selected.turn_id DESC
             LIMIT ?1 OFFSET ?2",
        )?;
        statement
            .query_map(params![fetch_limit, offset], |row| {
                Ok(ActivityGroupChildRef {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let has_more = selected.len() as u64 > page_size;
    if has_more {
        selected.truncate(page_size as usize);
    }
    let next_cursor = if has_more {
        selected
            .last()
            .map(|child| {
                encode_activity_collection_cursor(
                    thread_id,
                    item_id,
                    &child.timestamp,
                    None,
                    &child.id,
                )
            })
            .transpose()?
    } else {
        None
    };
    let mut items = query_activity_group_child_rows(connection, thread_id, &selected)?;
    query_activity_page_turn_totals_on(connection, thread_id, &mut items)?;
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more,
        next_cursor,
    })
}

fn query_activity_group_child_rows(
    connection: &Connection,
    thread_id: &str,
    selected: &[ActivityGroupChildRef],
) -> Result<Vec<ActivityItem>> {
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let requested = serde_json::to_string(
        &selected
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
    )?;
    let mut statement = connection.prepare(
        "SELECT t.id,t.rollout_id,t.agent_run_id,t.started_at,t.status,t.model,t.effort,
                NULLIF(substr(t.last_agent_message,1,?3),''),t.duration_ms,
                a.nickname,a.agent_path
         FROM json_each(?1) requested
         JOIN turns t ON t.id=requested.value AND t.thread_id=?2
         LEFT JOIN agent_runs a ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
         ORDER BY CAST(requested.key AS INTEGER)",
    )?;
    statement
        .query_map(
            params![requested, thread_id, ACTIVITY_PREVIEW_CHARS],
            activity_group_child_from_row,
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn activity_group_child_from_row(row: &Row<'_>) -> rusqlite::Result<ActivityItem> {
    let model = row.get::<_, Option<String>>(5)?;
    let review = model.as_deref() == Some("codex-auto-review");
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
        kind: if review { "review" } else { "subagent" }.into(),
        role: None,
        label: Some(agent_label.unwrap_or_else(|| {
            if review {
                "Automated review".into()
            } else {
                "Agent response".into()
            }
        })),
        body: bounded_preview(row.get(7)?),
        status: row.get(4)?,
        tool_name: None,
        duration_ms: row.get(8)?,
        model,
        effort: row.get(6)?,
        has_details: true,
        children: Vec::new(),
        child_page: None,
        child_page_size: None,
        child_total: None,
        child_has_more: None,
        child_next_cursor: None,
        usage: None,
        counts: None,
    })
}

fn query_activity_page_turn_totals_on(
    connection: &Connection,
    thread_id: &str,
    items: &mut [ActivityItem],
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let requested = serde_json::to_string(
        &items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
    )?;
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals_by_turn = HashMap::<String, FixedPointUsageTotals>::new();
    let mut statement = connection.prepare(
        "SELECT r.turn_key,r.activity_hour,r.model,
                COALESCE(SUM(r.input_tokens),0),
                COALESCE(SUM(r.cached_input_tokens),0),
                COALESCE(SUM(r.output_tokens),0),
                COALESCE(SUM(r.reasoning_tokens),0),
                COALESCE(SUM(r.total_tokens),0)
         FROM json_each(?1) requested
         JOIN usage_activity_rollups r
           ON r.thread_id=?2 AND r.turn_key=requested.value
         GROUP BY r.turn_key,r.activity_hour,r.model",
    )?;
    let mut rows = statement.query(params![requested, thread_id])?;
    while let Some(row) = rows.next()? {
        let turn_id = row.get::<_, String>(0)?;
        let activity_hour = row.get::<_, String>(1)?;
        let model = row.get::<_, String>(2)?;
        let input_tokens = row.get::<_, i64>(3)?.max(0);
        let cached_input_tokens = row.get::<_, i64>(4)?.max(0);
        let output_tokens = row.get::<_, i64>(5)?.max(0);
        let reasoning_tokens = row.get::<_, i64>(6)?.max(0);
        let total_tokens = row.get::<_, i64>(7)?.max(0) as u64;
        let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
            connection,
            &aliases,
            &prices,
            UsageRollupScope::Turn(&turn_id),
            thread_id,
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals_by_turn.entry(turn_id).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    for item in items {
        item.usage = Some(totals_by_turn.remove(&item.id).unwrap_or_default().finish());
    }
    Ok(())
}

fn query_activity_group_totals_on(connection: &Connection, thread_id: &str) -> Result<Totals> {
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals = FixedPointUsageTotals::default();
    let mut statement = connection.prepare(
        "SELECT r.turn_key,r.activity_hour,r.model,
                COALESCE(SUM(r.input_tokens),0),
                COALESCE(SUM(r.cached_input_tokens),0),
                COALESCE(SUM(r.output_tokens),0),
                COALESCE(SUM(r.reasoning_tokens),0),
                COALESCE(SUM(r.total_tokens),0)
         FROM selected_activity_group_turns selected
         JOIN usage_activity_rollups r
           ON r.thread_id=?1 AND r.turn_key=selected.turn_id
         GROUP BY r.turn_key,r.activity_hour,r.model",
    )?;
    let mut rows = statement.query([thread_id])?;
    while let Some(row) = rows.next()? {
        let turn_id = row.get::<_, String>(0)?;
        let activity_hour = row.get::<_, String>(1)?;
        let model = row.get::<_, String>(2)?;
        let input_tokens = row.get::<_, i64>(3)?.max(0);
        let cached_input_tokens = row.get::<_, i64>(4)?.max(0);
        let output_tokens = row.get::<_, i64>(5)?.max(0);
        let reasoning_tokens = row.get::<_, i64>(6)?.max(0);
        let total_tokens = row.get::<_, i64>(7)?.max(0) as u64;
        let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
            connection,
            &aliases,
            &prices,
            UsageRollupScope::Turn(&turn_id),
            thread_id,
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

fn query_activity_group_status_on(connection: &Connection) -> Result<String> {
    let (running, attention) = connection.query_row(
        "SELECT COALESCE(MAX(t.status='running'),0),
                COALESCE(MAX(t.status NOT IN ('completed','success','allowed')),0)
         FROM selected_activity_group_turns selected
         JOIN turns t ON t.id=selected.turn_id",
        [],
        |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
    )?;
    Ok(if running {
        "running"
    } else if attention {
        "attention"
    } else {
        "completed"
    }
    .into())
}

fn query_activity_group_labels_on(connection: &Connection) -> Result<Option<String>> {
    let mut statement = connection.prepare(
        "WITH latest_labels AS (
             SELECT COALESCE(a.nickname,a.agent_path) label,t.started_at,t.id,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(a.nickname,a.agent_path)
                        ORDER BY t.started_at DESC,t.id DESC
                    ) label_rank
             FROM selected_activity_group_turns selected
             JOIN turns t ON t.id=selected.turn_id
             LEFT JOIN agent_runs a ON a.id=t.agent_run_id AND a.thread_id=t.thread_id
         ), ranked_labels AS (
             SELECT label,started_at,id,COUNT(*) OVER () label_count,
                    ROW_NUMBER() OVER (ORDER BY started_at DESC,id DESC) preview_rank
             FROM latest_labels WHERE label IS NOT NULL AND label_rank=1
         )
         SELECT label,label_count FROM ranked_labels
         WHERE preview_rank<=?1 ORDER BY preview_rank",
    )?;
    let labels = statement
        .query_map([ACTIVITY_AGENT_LABEL_PREVIEW_LIMIT], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let label_count = labels.first().map_or(0, |(_, count)| *count);
    let labels = labels
        .into_iter()
        .map(|(label, _)| label)
        .collect::<Vec<_>>();
    Ok(activity_agent_labels_preview(&labels, label_count))
}

fn query_activity_group_duration_on(connection: &Connection) -> Result<Option<i64>> {
    let mut statement = connection.prepare(
        "SELECT t.started_at,t.duration_ms
         FROM selected_activity_group_turns selected
         JOIN turns t ON t.id=selected.turn_id
         WHERE t.duration_ms IS NOT NULL
         ORDER BY t.started_at,t.id",
    )?;
    let mut rows = statement.query([])?;
    let mut current = None::<(DateTime<Utc>, DateTime<Utc>)>;
    let mut total_ms = 0_i64;
    while let Some(row) = rows.next()? {
        let started_at = row.get::<_, String>(0)?;
        let duration_ms = row.get::<_, i64>(1)?.max(0);
        let Some(start) = DateTime::parse_from_rfc3339(&started_at)
            .ok()
            .map(|value| value.with_timezone(&Utc))
        else {
            continue;
        };
        let Some(duration) = Duration::try_milliseconds(duration_ms) else {
            continue;
        };
        let Some(end) = start.checked_add_signed(duration) else {
            continue;
        };
        match current {
            Some((current_start, current_end)) if start <= current_end => {
                current = Some((current_start, current_end.max(end)));
            }
            Some((current_start, current_end)) => {
                total_ms =
                    total_ms.saturating_add((current_end - current_start).num_milliseconds());
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    Ok(current.map(|(start, end)| total_ms.saturating_add((end - start).num_milliseconds())))
}

fn query_activity_turn_totals_on(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
) -> Result<Totals> {
    let (aliases, prices) = overview_prices_on(connection)?;
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
    let mut totals = FixedPointUsageTotals::default();
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
        let (known_cost_numerator, unpriced_tokens) = usage_rollup_cost_on(
            connection,
            &aliases,
            &prices,
            UsageRollupScope::Turn(turn_id),
            thread_id,
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

struct ActivityChildrenPage {
    items: Vec<ActivityItem>,
    page: u64,
    page_size: u64,
    total: u64,
    has_more: bool,
    next_cursor: Option<String>,
}

#[cfg(test)]
fn query_activity_child_previews_page(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: &str,
    page: u64,
    page_size: u64,
) -> Result<ActivityChildrenPage> {
    query_activity_child_previews_cursor_page(connection, thread_id, turn_id, page, page_size, None)
}

fn query_activity_child_previews_cursor_page(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: &str,
    page: u64,
    page_size: u64,
    cursor: Option<&str>,
) -> Result<ActivityChildrenPage> {
    let requested_page = page.max(1);
    let fallback_offset = requested_page.saturating_sub(1).saturating_mul(page_size);
    let mut indexed = activity_index::query_page(
        connection,
        thread_id,
        turn_id,
        page_size,
        cursor,
        fallback_offset,
    )?;
    let total = indexed.total;
    let total_pages = total.div_ceil(page_size).max(1);
    // Numeric pages remain a compatibility path for old bookmarks and direct
    // tests. The browser uses the opaque cursor, so ordinary Load More calls
    // never walk an OFFSET proportional to the complete turn history.
    let page = if cursor.is_some() {
        requested_page
    } else {
        requested_page.min(total_pages)
    };
    if cursor.is_none() && page != requested_page {
        let offset = page.saturating_sub(1).saturating_mul(page_size);
        indexed =
            activity_index::query_page(connection, thread_id, turn_id, page_size, None, offset)?;
    }
    let items =
        query_activity_child_preview_rows(connection, thread_id, Some(turn_id), &indexed.events)?;
    Ok(ActivityChildrenPage {
        items,
        page,
        page_size,
        total,
        has_more: indexed.next_cursor.is_some(),
        next_cursor: indexed.next_cursor,
    })
}

fn query_activity_child_preview_rows(
    connection: &rusqlite::Connection,
    thread_id: &str,
    turn_id: Option<&str>,
    indexed: &[activity_index::IndexedActivityEvent],
) -> Result<Vec<ActivityItem>> {
    if indexed.is_empty() {
        return Ok(Vec::new());
    }
    let requested = serde_json::to_string(
        &indexed
            .iter()
            .enumerate()
            .map(|(ordinal, event)| {
                serde_json::json!({
                    "ordinal": ordinal,
                    "eventId": event.event_id,
                    "sourceLine": event.source_line,
                })
            })
            .collect::<Vec<_>>(),
    )?;
    let mut statement = connection.prepare(
        "WITH selected AS MATERIALIZED (
             SELECT CAST(key AS INTEGER) ordinal,
                    json_extract(value,'$.eventId') event_id,
                    CAST(json_extract(value,'$.sourceLine') AS INTEGER) source_line
             FROM json_each(?1)
         )
         SELECT e.id,e.turn_id,e.rollout_id,e.agent_run_id,e.timestamp,e.kind,e.role,e.label,
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
                a.nickname,a.agent_path,tc.namespace,selected.source_line
         FROM selected
         JOIN events e ON e.id=selected.event_id AND e.thread_id=?2
         LEFT JOIN messages m
           ON m.id=COALESCE(e.call_id,e.id) AND m.thread_id=e.thread_id
         LEFT JOIN tool_calls tc
           ON tc.rollout_id=e.rollout_id AND tc.call_id=e.call_id
          AND tc.thread_id=e.thread_id
         LEFT JOIN agent_runs a
           ON a.id=e.agent_run_id AND a.thread_id=e.thread_id
         ORDER BY selected.ordinal",
    )?;
    let rows = statement.query_map(
        params![requested, thread_id, ACTIVITY_PREVIEW_CHARS],
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
                    child_next_cursor: None,
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
    _turn_id: Option<&str>,
    items: &mut [(ActivityItem, i64)],
) -> Result<()> {
    // Attribution is an intrinsic property of the complete event stream, not of
    // whichever neighboring owners happen to be present on this page. Resolve
    // every returned owner against its indexed source-line window in one
    // statement. Besides making every representation agree, this changes a
    // child page from a full-turn usage scan to at most two usage source lines
    // per visible owner without introducing an N+1 query pattern.
    if items.is_empty() {
        return Ok(());
    }
    let requested = serde_json::Value::Array(
        items
            .iter()
            .enumerate()
            .filter(|(_, (item, _))| {
                matches!(
                    item.kind.as_str(),
                    "assistant" | "update" | "final" | "reasoning" | "tool" | "subagent"
                )
            })
            .map(|(index, (item, source_line))| {
                serde_json::json!({
                    "ordinal": index,
                    "rolloutId": item.rollout_id,
                    "turnId": item.turn_id,
                    "sourceLine": source_line,
                })
            })
            .collect(),
    );
    let requested = serde_json::to_string(&requested)?;
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
    let (aliases, prices) = overview_prices_on(connection)?;
    let mut totals_by_ordinal = HashMap::<usize, FixedPointUsageTotals>::new();
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
        add_usage_fact_to_totals(
            totals_by_ordinal.entry(ordinal).or_default(),
            &aliases,
            &prices,
            &timestamp,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
        );
    }
    for (ordinal, totals) in totals_by_ordinal {
        if let Some((item, _)) = items.get_mut(ordinal) {
            item.usage = Some(totals.finish());
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
                    cursor.with_timezone(&Local).format("%H:%M").to_string(),
                ));
                cursor = next;
            }
            let labels = disambiguate_repeated_labels(
                buckets
                    .iter()
                    .map(|(start, _, label)| {
                        (
                            label.clone(),
                            start.with_timezone(&Local).format("%:z").to_string(),
                        )
                    })
                    .collect(),
            );
            for ((_, _, label), disambiguated) in buckets.iter_mut().zip(labels) {
                *label = disambiguated;
            }
        }
        "week" => {
            let monday = anchor - Duration::days(anchor.weekday().num_days_from_monday() as i64);
            for offset in 0..7 {
                let date = monday + Duration::days(offset);
                push_nonempty_stats_bucket(
                    &mut buckets,
                    local_midnight(date),
                    local_midnight(date + Duration::days(1)),
                    date.format("%a %-d").to_string(),
                );
            }
        }
        "month" => {
            let mut date = NaiveDate::from_ymd_opt(anchor.year(), anchor.month(), 1)
                .context("invalid month")?;
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
        "year" => {
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
        _ => {
            let mut years = occupied_local_years_on(connection)?;
            if years.is_empty() || years.iter().any(|year| *year <= anchor.year()) {
                years.insert(anchor.year());
            }
            let public_start = NaiveDate::from_ymd_opt(MIN_PUBLIC_YEAR, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .context("invalid public timestamp lower boundary")?
                .and_utc();
            let public_end = NaiveDate::from_ymd_opt(MAX_PUBLIC_YEAR + 1, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .context("invalid public timestamp upper boundary")?
                .and_utc();
            for year in years {
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
        let next_bound = sql_timestamp(local_midnight(next));
        anyhow::ensure!(
            next_bound > lower_bound,
            "all-time year scan did not advance"
        );
        lower_bound = next_bound;
    }
    Ok(years)
}

fn push_nonempty_stats_bucket(
    buckets: &mut Vec<StatsBucket>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    label: String,
) {
    // A political timezone change can delete an entire civil date (for
    // example, Samoa's 2011-12-30). Such a date has no UTC interval and must
    // not become a zero-duration analytical bucket.
    if start < end {
        buckets.push((start, end, label));
    }
}

fn query_prices_on(
    connection: &Connection,
    q: Option<&str>,
    page: u64,
    page_size: u64,
) -> Result<PricesResponse> {
    let q_filter = q.filter(|value| !value.trim().is_empty());
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS price_search_matches(
             model_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM price_search_matches;",
    )?;
    if let Some(query) = q_filter {
        let needle = normalize_search_text(query.trim());
        let mut select = connection.prepare("SELECT model_id FROM resolved_model_prices")?;
        let mut insert = connection
            .prepare("INSERT OR IGNORE INTO price_search_matches(model_id) VALUES(?1)")?;
        let mut rows = select.query([])?;
        while let Some(row) = rows.next()? {
            let model_id = row.get::<_, String>(0)?;
            if normalize_search_text(&model_id).contains(&needle) {
                insert.execute([&model_id])?;
            }
        }
    }
    let total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM resolved_model_prices
         WHERE ?1 IS NULL OR EXISTS(
             SELECT 1 FROM price_search_matches search
             WHERE search.model_id=resolved_model_prices.model_id
         )",
        [q_filter],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model_id,effective_from,effective_to,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,currency,source
         FROM resolved_model_prices
         WHERE ?1 IS NULL OR EXISTS(
             SELECT 1 FROM price_search_matches search
             WHERE search.model_id=resolved_model_prices.model_id
         )
         ORDER BY model_id,effective_from DESC LIMIT ?2 OFFSET ?3",
    )?;
    let raw_items = statement
        .query_map(
            params![
                q_filter,
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

fn query_aliases_on(
    connection: &Connection,
    q: Option<&str>,
    page: u64,
    page_size: u64,
) -> Result<AliasesResponse> {
    anyhow::ensure!((1..=100).contains(&page_size), "invalid alias page size");
    anyhow::ensure!(
        page > 0 && page <= MAX_JS_SAFE_INTEGER,
        "invalid alias page"
    );
    let q_filter = q.map(str::trim).filter(|value| !value.is_empty());
    anyhow::ensure!(
        q_filter.is_none_or(|value| value.chars().count() <= MAX_SESSION_SEARCH_CHARS),
        "alias search exceeds the {MAX_SESSION_SEARCH_CHARS}-character limit"
    );
    anyhow::ensure!(
        q_filter.is_none_or(|value| {
            normalize_search_text(value).chars().count() <= MAX_SESSION_SEARCH_CHARS
        }),
        "normalized alias search exceeds the {MAX_SESSION_SEARCH_CHARS}-character limit"
    );
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS alias_search_matches(
             observed_model_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM alias_search_matches;",
    )?;
    if let Some(query) = q_filter {
        let needle = normalize_search_text(query);
        let mut select = connection.prepare(
            "SELECT observed_model_id,canonical_model_id
             FROM resolved_model_aliases
             WHERE length(observed_model_id) BETWEEN 1 AND ?1
               AND length(canonical_model_id) BETWEEN 1 AND ?1",
        )?;
        let mut insert = connection
            .prepare("INSERT OR IGNORE INTO alias_search_matches(observed_model_id) VALUES(?1)")?;
        let mut rows = select.query([MAX_MODEL_ID_CHARS as i64])?;
        while let Some(row) = rows.next()? {
            let observed_model_id = row.get::<_, String>(0)?;
            let canonical_model_id = row.get::<_, String>(1)?;
            if normalize_search_text(&observed_model_id).contains(&needle)
                || normalize_search_text(&canonical_model_id).contains(&needle)
            {
                insert.execute([&observed_model_id])?;
            }
        }
    }
    let total = connection.query_row(
        "SELECT COUNT(*) FROM resolved_model_aliases
         WHERE length(observed_model_id) BETWEEN 1 AND ?2
           AND length(canonical_model_id) BETWEEN 1 AND ?2
           AND (
                ?1 IS NULL OR EXISTS(
                    SELECT 1 FROM alias_search_matches search
                    WHERE search.observed_model_id=resolved_model_aliases.observed_model_id
                )
           )",
        params![q_filter, MAX_MODEL_ID_CHARS as i64],
        |row| row.get::<_, i64>(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT observed_model_id,canonical_model_id
         FROM resolved_model_aliases
         WHERE length(observed_model_id) BETWEEN 1 AND ?2
           AND length(canonical_model_id) BETWEEN 1 AND ?2
           AND (
                ?1 IS NULL OR EXISTS(
                    SELECT 1 FROM alias_search_matches search
                    WHERE search.observed_model_id=resolved_model_aliases.observed_model_id
                )
           )
         ORDER BY observed_model_id
         LIMIT ?3 OFFSET ?4",
    )?;
    let items = statement
        .query_map(
            params![
                q_filter,
                MAX_MODEL_ID_CHARS as i64,
                page_size as i64,
                price_page_offset(page, page_size),
            ],
            |row| {
                Ok(AliasRow {
                    observed_model_id: row.get(0)?,
                    canonical_model_id: row.get(1)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let total = total.max(0) as u64;
    Ok(AliasesResponse {
        items,
        page,
        page_size,
        total,
        total_pages: total.div_ceil(page_size),
    })
}

fn query_price_metadata_on(
    connection: &Connection,
    unknown_limit: u64,
) -> Result<PriceMetadataResponse> {
    anyhow::ensure!(
        (1..=MAX_UNKNOWN_MODEL_RESULTS).contains(&unknown_limit),
        "invalid unknown model result limit"
    );
    let observed_unknown_total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (
            SELECT model FROM priced_usage WHERE price_known=0 GROUP BY model
         )",
        [],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model,COUNT(*),SUM(total_tokens),MAX(timestamp) FROM priced_usage
         WHERE price_known=0 AND length(model) BETWEEN 1 AND ?1 GROUP BY model
         ORDER BY SUM(total_tokens) DESC,model LIMIT ?2",
    )?;
    let observed_unknown = statement
        .query_map(
            params![MAX_MODEL_ID_CHARS as i64, unknown_limit as i64],
            |row| {
                Ok(UnknownModelRow {
                    model_id: row.get(0)?,
                    usage_count: row.get::<_, i64>(1)?.max(0) as u64,
                    total_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                    last_seen_at: row.get(3)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(PriceMetadataResponse {
        observed_unknown,
        observed_unknown_total: observed_unknown_total.max(0) as u64,
    })
}

fn query_price_model_ids_on(
    connection: &Connection,
    q: Option<&str>,
    limit: u64,
) -> Result<PriceModelIdsResponse> {
    anyhow::ensure!(
        (1..=MAX_PRICE_MODEL_ID_RESULTS).contains(&limit),
        "invalid model ID result limit"
    );
    let needle = q
        .filter(|value| !value.trim().is_empty())
        .map(|value| normalize_search_text(value.trim()));
    anyhow::ensure!(
        needle
            .as_deref()
            .is_none_or(|value| value.chars().count() <= MAX_SESSION_SEARCH_CHARS),
        "model ID search exceeds the {MAX_SESSION_SEARCH_CHARS}-character limit"
    );

    let mut statement = connection
        .prepare("SELECT DISTINCT model_id FROM resolved_model_prices ORDER BY model_id")?;
    let mut rows = statement.query([])?;
    let mut items = Vec::with_capacity(limit as usize);
    while let Some(row) = rows.next()? {
        let model_id = row.get::<_, String>(0)?;
        if model_id.chars().count() > MAX_MODEL_ID_CHARS {
            continue;
        }
        if needle
            .as_deref()
            .is_none_or(|needle| normalize_search_text(&model_id).contains(needle))
        {
            items.push(model_id);
            if items.len() == limit as usize {
                break;
            }
        }
    }
    Ok(PriceModelIdsResponse { items })
}

fn public_price_source(source: &str) -> String {
    source.strip_prefix("remote:").unwrap_or(source).to_owned()
}

fn price_page_offset(page: u64, page_size: u64) -> i64 {
    page.saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64
}

fn validated_page(page: Option<u64>) -> ApiResult<u64> {
    let page = page.unwrap_or(1).max(1);
    if page > MAX_JS_SAFE_INTEGER {
        Err(ApiError::bad_request(format!(
            "page must not exceed {MAX_JS_SAFE_INTEGER}"
        )))
    } else {
        Ok(page)
    }
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
        validate_public_year(timestamp.year())?;
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

fn parse_timestamp(value: &str) -> ApiResult<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        validate_public_year(timestamp.year())?;
        return Ok(timestamp.with_timezone(&Utc));
    }
    if let Ok(date) = parse_date(value) {
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
    let midnight = date.and_hms_opt(0, 0, 0).expect("midnight is valid");
    (0..=48 * 60)
        .find_map(|offset_minutes| {
            let candidate = midnight + Duration::minutes(offset_minutes);
            match Local.from_local_datetime(&candidate) {
                LocalResult::Single(value) => Some(value.with_timezone(&Utc)),
                LocalResult::Ambiguous(first, _) => Some(first.with_timezone(&Utc)),
                LocalResult::None => None,
            }
        })
        .expect("the local timezone must contain an instant within 48 hours of a civil midnight")
}

#[cfg(test)]
mod tests {
    use super::{
        ApiState, BUCKET_AGGREGATES_SQL, OVERVIEW_YEAR_USAGE_SQL, PricesQuery,
        STATS_BUCKET_SESSIONS_SQL, STATS_BUCKET_USAGE_SQL, STATS_FEW_BUCKET_SESSIONS_SQL,
        SqlBucketBounds, StatsBucketAggregate, activity_day_window, display_tool_name,
        first_prompt_for_display, prewarm_current_year_analytics_on, price_page_offset, prices,
        query_activity_child_previews_page, query_activity_day_summaries_batched,
        query_activity_detail_on, query_activity_detail_page_on, query_activity_on, query_bounds,
        query_heatmap_on, query_overview_year_on, query_stats_on, run_manual_mutation,
        run_snapshot_work, settings, stats_totals_from_aggregates,
    };
    use crate::{
        config::PricingConfig,
        db::Db,
        db_executor::{DbExecutor, WorkClass},
        ingest::IngestRoots,
        model::Totals,
        money::UsdAmount,
    };
    use axum::{
        extract::{Path as AxumPath, Query, State},
        http::StatusCode,
    };
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
    static STATS_USAGE_QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_query(sql: &str) {
        let sql = sql.trim_start();
        if sql.starts_with("SELECT") || sql.starts_with("WITH") {
            QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if sql.contains("overview-year-usage") {
            OVERVIEW_USAGE_QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if sql.contains("bucket_model_values AS MATERIALIZED") {
            STATS_USAGE_QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn price_pagination_offset_saturates_before_sql_conversion() {
        assert_eq!(price_page_offset(1, 25), 0);
        assert_eq!(price_page_offset(3, 25), 50);
        assert_eq!(price_page_offset(u64::MAX, 100), i64::MAX);
    }

    #[test]
    fn aliases_are_filtered_and_paginated_before_serialization() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<51
                 )
                 INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 )
                 SELECT printf('legacy-alias-%02d',value),
                        printf('canonical-target-%02d',value),
                        '2026-01-01T00:00:00.000000000Z','remote:test'
                 FROM sequence;
                 INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES(
                    replace(hex(zeroblob(300)),'00','x'),'canonical-target-00',
                    '2026-01-01T00:00:00.000000000Z','remote:test'
                 );",
            )
            .unwrap();

        let page = super::query_aliases_on(&connection, Some("LEGACY-ALIAS-"), 2, 10).unwrap();
        assert_eq!(page.page, 2);
        assert_eq!(page.page_size, 10);
        assert_eq!(page.total, 52);
        assert_eq!(page.total_pages, 6);
        assert_eq!(page.items.len(), 10);
        assert_eq!(page.items[0].observed_model_id, "legacy-alias-10");
        assert!(page.items.iter().all(|alias| {
            alias.observed_model_id.chars().count() <= super::MAX_MODEL_ID_CHARS
                && alias.canonical_model_id.chars().count() <= super::MAX_MODEL_ID_CHARS
        }));

        let canonical = super::query_aliases_on(&connection, Some("TARGET-3"), 1, 25).unwrap();
        assert_eq!(canonical.total, 10);
        assert_eq!(canonical.items.len(), 10);
        assert!(serde_json::to_vec(&canonical).unwrap().len() < 8 * 1024);
    }

    #[test]
    fn alias_search_normalizes_unicode_before_deterministic_pagination() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES
                    ('MÜNCHEN-É-02','gpt-5.5','2026-01-01T00:00:00Z','remote:test'),
                    ('MÜNCHEN-É-01','gpt-5.5','2026-01-01T00:00:00Z','remote:test');",
            )
            .unwrap();

        let first = super::query_aliases_on(&connection, Some("münchen-e\u{301}"), 1, 1).unwrap();
        assert_eq!(first.total, 2);
        assert_eq!(first.total_pages, 2);
        assert_eq!(first.items[0].observed_model_id, "MÜNCHEN-É-01");

        let second = super::query_aliases_on(&connection, Some("münchen-e\u{301}"), 2, 1).unwrap();
        assert_eq!(second.total, 2);
        assert_eq!(second.items[0].observed_model_id, "MÜNCHEN-É-02");
    }

    #[test]
    fn date_filter_rejects_an_ambiguous_explicit_range() {
        let error =
            query_bounds(Some("2026-07-22"), Some("2026-07-01"), Some("2026-07-31")).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.message, "date cannot be combined with start or end");
    }

    #[test]
    fn price_metadata_bounds_twenty_thousand_unknown_models() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('unknowns','Unknowns','2026-01-01T00:00:00.000000000Z',
                        '2026-01-01T00:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('unknowns','unknowns','2026-01-01T00:00:00.000000000Z',
                        '2026-01-01T00:00:00.000000000Z',0);
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<19999
                 )
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 )
                 SELECT printf('unknown-fact-%05d',value),'unknowns','unknowns',
                        '2026-01-01T00:00:00.000000000Z',value+1,
                        printf('unknown-model-%05d',value),1,0,0,0,1,1
                 FROM sequence;
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(
                    'unknown-fact-overlong','unknowns','unknowns',
                    '2026-01-01T00:00:00.000000000Z',20001,
                    replace(hex(zeroblob(300)),'00','y'),1000000,0,0,0,1000000,1
                 );",
            )
            .unwrap();

        let metadata = super::query_price_metadata_on(&connection, 100).unwrap();
        assert_eq!(metadata.observed_unknown_total, 20_001);
        assert_eq!(metadata.observed_unknown.len(), 100);
        assert!(
            metadata
                .observed_unknown
                .iter()
                .all(|row| row.model_id.chars().count() <= super::MAX_MODEL_ID_CHARS)
        );
        assert!(serde_json::to_vec(&metadata).unwrap().len() < 64 * 1024);
    }

    #[test]
    fn utc_rollup_hour_detects_midnight_in_fractional_offset_zones() {
        let (hour_start, hour_end) =
            super::usage_rollup_hour_window("2026-07-01T18:00:00.000000000Z").unwrap();
        let nepal = chrono::FixedOffset::east_opt(5 * 60 * 60 + 45 * 60).unwrap();
        let (start_date, end_date) = super::usage_rollup_bucket_dates(hour_start, hour_end, &nepal);
        assert_eq!(
            start_date,
            chrono::NaiveDate::from_ymd_opt(2026, 7, 1).unwrap()
        );
        assert_eq!(
            end_date,
            chrono::NaiveDate::from_ymd_opt(2026, 7, 2).unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settings_and_prices_wait_for_the_heavy_executor_lane() {
        use tokio::sync::oneshot;

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let executor = DbExecutor::new(3, 1);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_pricing_mutations_queue_before_blocking_workers() {
        use tokio::sync::oneshot;

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
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
            DbExecutor::new(3, 1),
        );
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let first = {
            let state = state.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                run_manual_mutation(&state, move || {
                    first_started_tx.send(()).unwrap();
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
        first_started_rx.await.unwrap();
        first.abort();

        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let second = {
            let state = state.clone();
            tokio::spawn(async move {
                run_manual_mutation(&state, move || {
                    second_started_tx.send(()).unwrap();
                    Ok(())
                })
                .await
            })
        };
        assert!(
            tokio::time::timeout(StdDuration::from_millis(100), &mut second_started_rx)
                .await
                .is_err(),
            "a queued mutation consumed a blocking worker before the active mutation completed"
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        assert!(first.await.unwrap_err().is_cancelled());
        tokio::time::timeout(StdDuration::from_secs(2), &mut second_started_rx)
            .await
            .expect("queued mutation did not start after the active mutation finished")
            .unwrap();
        second.await.unwrap().unwrap();
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
    fn startup_prewarm_executes_both_current_year_analytical_plans() {
        let _trace_guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection.trace(Some(count_query));
        OVERVIEW_USAGE_QUERY_COUNT.store(0, Ordering::SeqCst);
        STATS_USAGE_QUERY_COUNT.store(0, Ordering::SeqCst);

        prewarm_current_year_analytics_on(
            &connection,
            NaiveDate::from_ymd_opt(2026, 7, 23).unwrap(),
        )
        .unwrap();

        assert_eq!(OVERVIEW_USAGE_QUERY_COUNT.swap(0, Ordering::SeqCst), 1);
        assert_eq!(STATS_USAGE_QUERY_COUNT.swap(0, Ordering::SeqCst), 1);
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
    fn stats_grand_total_preserves_fixed_point_exactly() {
        let aggregates = (0..10)
            .map(|_| StatsBucketAggregate {
                totals: Totals {
                    total_tokens: 1,
                    known_cost_numerator: 100_000_000_000,
                    ..Totals::default()
                }
                .finish(),
                session_count: 1,
                known_cost_numerator: 100_000_000_000,
            })
            .collect::<Vec<_>>();

        let totals = stats_totals_from_aggregates(&aggregates);
        assert_eq!(totals.known_cost_numerator, 1_000_000_000_000);
        assert_eq!(totals.cost_usd.unwrap().decimal_string(), "1.00");
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
            "all",
            NaiveDate::from_ymd_opt(2026, 7, 19).unwrap(),
        )
        .unwrap();
        assert_eq!(
            buckets
                .iter()
                .map(|(_, _, label)| label.as_str())
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

        let future_only = super::stats_buckets_on(&connection, "all", anchor).unwrap();
        assert_eq!(
            future_only
                .iter()
                .map(|(_, _, label)| label.as_str())
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
        let mixed = super::stats_buckets_on(&connection, "all", anchor).unwrap();
        assert_eq!(
            mixed
                .iter()
                .map(|(_, _, label)| label.as_str())
                .collect::<Vec<_>>(),
            ["2025", "2026", "2027"]
        );
    }

    #[test]
    fn stats_omit_civil_dates_without_a_utc_interval() {
        let boundary = chrono::DateTime::parse_from_rfc3339("2011-12-30T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut buckets = Vec::new();
        super::push_nonempty_stats_bucket(&mut buckets, boundary, boundary, "2011-12-30".into());
        assert!(buckets.is_empty());
    }

    #[test]
    fn duplicate_hour_labels_receive_offsets_while_unique_labels_stay_plain() {
        assert_eq!(
            super::disambiguate_repeated_labels(vec![
                ("01:00".into(), "+02:00".into()),
                ("02:00".into(), "+02:00".into()),
                ("02:00".into(), "+01:00".into()),
                ("03:00".into(), "+01:00".into()),
            ]),
            ["01:00", "02:00 (+02:00)", "02:00 (+01:00)", "03:00"]
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

    #[tokio::test]
    async fn dropping_snapshot_work_stays_cancelled_between_sqlite_statements() {
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
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let (first_done_tx, first_done_rx) = tokio::sync::oneshot::channel();
        let (second_done_tx, second_done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            run_snapshot_work(&state, WorkClass::Heavy, db, move |connection| {
                let first: i64 = connection.query_row("SELECT 1", [], |row| row.get(0))?;
                assert_eq!(first, 1);
                let _ = first_done_tx.send(());

                let (lock, ready) = &*worker_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                drop(released);

                let second = connection.query_row(
                    "WITH RECURSIVE counter(value) AS (
                         VALUES(0) UNION ALL
                         SELECT value+1 FROM counter WHERE value<100000000
                     ) SELECT SUM(value) FROM counter",
                    [],
                    |row| row.get::<_, i64>(0),
                );
                let interrupted = matches!(
                    second,
                    Err(rusqlite::Error::SqliteFailure(error, _))
                        if error.code == rusqlite::ErrorCode::OperationInterrupted
                );
                let _ = second_done_tx.send(interrupted);
                Ok(())
            })
            .await
        });

        first_done_rx.await.unwrap();
        // No SQLite statement is running at this point. The immediate
        // InterruptHandle call is therefore insufficient on its own; the
        // connection-wide progress handler must stop the next statement.
        task.abort();
        let _ = task.await;
        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), second_done_rx)
                .await
                .expect("SQLite cancellation was lost between statements")
                .unwrap(),
            "the second SQLite statement was not interrupted"
        );
    }

    #[test]
    fn heatmap_preserves_empty_days_around_sparse_usage() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();

        let empty = query_heatmap_on(&connection, 2026).unwrap();
        assert_eq!(empty.len(), 365);
        assert!(empty.iter().all(|day| {
            day.cost_usd == Some(UsdAmount::ZERO)
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
        assert!(populated.cost_usd.unwrap().cost_numerator() > 0);
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
    fn session_cost_sort_and_transport_keep_fixed_point_differences() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
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
                    ('higher-usage','a-higher','higher-rollout',
                     '2026-07-15T12:00:00.000000000Z',1,'precise-sort',
                     3999999999,0,1,0,4000000000),
                    ('lower-usage','z-lower','lower-rollout',
                     '2026-07-15T12:00:00.000000000Z',1,'precise-sort',
                     3999999999,0,0,0,3999999999);",
            )
            .unwrap();

        let sorted =
            super::query_sessions_on(&connection, None, None, None, None, "cost", 1, 2, false)
                .unwrap();
        assert_eq!(
            sorted
                .items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["a-higher", "z-lower"]
        );
        assert!(sorted.items[0].cost_usd.unwrap() > sorted.items[1].cost_usd.unwrap());
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
            let actual = sessions.get(thread_id).unwrap();
            assert_eq!(actual.known_cost_numerator, expected.0);
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
                    "SELECT COALESCE(SUM(cost_numerator),0),
                            COALESCE(SUM(CASE WHEN price_known=0
                                              THEN total_tokens ELSE 0 END),0),
                            COALESCE(SUM(total_tokens),0)
                     FROM priced_usage WHERE thread_id=?1",
                    [&item.id],
                    |row| {
                        Ok((
                            i128::from(row.get::<_, i64>(0)?),
                            row.get::<_, i64>(1)?.max(0) as u64,
                            row.get::<_, i64>(2)?.max(0) as u64,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(item.unpriced_tokens, expected.1);
            assert_eq!(item.total_tokens, expected.2);
            if expected.1 == 0 {
                assert_eq!(item.cost_usd.unwrap().cost_numerator(), expected.0);
            } else {
                assert_eq!(item.cost_usd, None);
            }
        }

        // The bounded path must combine the compact 12:00 UTC rollup with raw
        // facts from the two partial boundary hours. The selected full hour also
        // contains both a price override and an unpriced gap, so this exercises
        // the exceptional per-fact repricing fallback instead of merely summing
        // one constant-price bucket.
        let bounded_start = "2026-07-15T11:30:00.000000000Z";
        let bounded_end = "2026-07-15T13:30:00.000000000Z";
        let bounded = super::query_sessions_on(
            &connection,
            Some(bounded_start),
            Some(bounded_end),
            None,
            None,
            "cost",
            1,
            50,
            false,
        )
        .unwrap();
        assert_eq!(bounded.items.len(), 2);
        for item in &bounded.items {
            let expected = connection
                .query_row(
                    "SELECT COALESCE(SUM(cost_numerator),0),
                            COALESCE(SUM(CASE WHEN price_known=0
                                              THEN total_tokens ELSE 0 END),0),
                            COALESCE(SUM(total_tokens),0)
                     FROM priced_usage
                     WHERE thread_id=?1 AND timestamp>=?2 AND timestamp<?3",
                    params![item.id, bounded_start, bounded_end],
                    |row| {
                        Ok((
                            i128::from(row.get::<_, i64>(0)?),
                            row.get::<_, i64>(1)?.max(0) as u64,
                            row.get::<_, i64>(2)?.max(0) as u64,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(item.unpriced_tokens, expected.1);
            assert_eq!(item.total_tokens, expected.2);
            if expected.1 == 0 {
                assert_eq!(item.cost_usd.unwrap().cost_numerator(), expected.0);
            } else {
                assert_eq!(item.cost_usd, None);
            }
        }

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
        let stats = query_stats_on(
            &connection,
            "year",
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
        let repriced =
            super::query_sessions_on(&connection, None, None, None, None, "cost", 1, 50, false)
                .unwrap();
        assert_eq!(repriced.items[0].id, "gap-thread");
        assert_eq!(repriced.items[0].unpriced_tokens, 0);
        let expected_gap_cost: i128 = connection
            .query_row(
                "SELECT COALESCE(SUM(cost_numerator),0)
                 FROM priced_usage WHERE thread_id='gap-thread'",
                [],
                |row| row.get::<_, i64>(0).map(i128::from),
            )
            .unwrap();
        assert_eq!(
            repriced.items[0].cost_usd.unwrap().cost_numerator(),
            expected_gap_cost
        );
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
            assert_eq!(actual.known_cost_numerator, expected.known_cost_numerator);
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
        let expected_delta = actual[0].cost_usd.unwrap().cost_numerator()
            - actual[3].cost_usd.unwrap().cost_numerator();
        assert_eq!(
            priced_delta.delta_cost_usd.unwrap().cost_numerator(),
            expected_delta
        );
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
                "PRAGMA ignore_check_constraints=ON;
                 INSERT INTO threads(id,title,started_at,last_event_at)
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
                 );
                 PRAGMA ignore_check_constraints=OFF;",
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
        // Fixed-point rollup repricing adds five bounded statements: two ledger
        // reads for day totals, two for exchange totals, and one sparse
        // NULL-turn usage probe alongside the rollup query it replaces. Indexed
        // occupied-date seeking keeps turn intervals and point activity in two
        // separate statements. The equality above is the essential guard:
        // neither page size nor raw usage-fact count may amplify the budget.
        assert!(
            all_count <= 19,
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
        // The canonical child projection uses one bounded COUNT plus one
        // indexed page seek. Descendant attribution, group metadata, labels,
        // and interval-union duration add four set-based/streamed statements;
        // none varies with descendant count. The equality above is the guard
        // against the old per-descendant query path.
        assert!(
            many_descendant_count <= 21,
            "expanded Activity used {many_descendant_count} SELECTs"
        );
        connection.trace(None);
    }

    #[test]
    fn legacy_activity_page_decodes_only_selected_message_bodies() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('bounded-legacy','Bounded legacy',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('bounded-legacy','bounded-legacy',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES(
                    'bounded-legacy-first','bounded-legacy','bounded-legacy',
                    '2026-07-01T00:00:00.000000000Z','user','First request',1
                 );",
            )
            .unwrap();
        for index in 0..200_i64 {
            connection
                .execute(
                    "INSERT INTO messages(
                        id,thread_id,rollout_id,timestamp,role,content,source_line
                     ) VALUES(?1,'bounded-legacy','bounded-legacy',?2,'user',?3,?4)",
                    params![
                        format!("bounded-legacy-unselected-{index:03}"),
                        format!("2026-07-01T00:05:{:02}.{:03}Z", index / 10, index % 10),
                        rusqlite::types::Value::Blob(vec![0x80]),
                        index + 2,
                    ],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES(
                    'bounded-legacy-latest','bounded-legacy','bounded-legacy',
                    '2026-07-01T00:10:00.000000000Z','assistant','Latest answer',?1
                 )",
                [202_i64],
            )
            .unwrap();

        let detail = query_activity_detail_page_on(
            &connection,
            "bounded-legacy",
            "legacy:bounded-legacy",
            1,
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.child_total, Some(202));
        assert_eq!(detail.children.len(), 1);
        assert_eq!(
            detail.children[0].id,
            "legacy-message:bounded-legacy-latest"
        );
        assert_eq!(detail.children[0].body.as_deref(), Some("Latest answer"));
    }

    #[test]
    fn legacy_message_previews_are_bounded_and_preserve_wrapped_requests() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('large-legacy','Large legacy',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('large-legacy','large-legacy',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z',0);",
            )
            .unwrap();
        let wrapped_user = format!(
            "# Applications mentioned by the user:\n{}\n\n## My request for Codex:\nKeep the tail request visible",
            "context ".repeat(super::ACTIVITY_MESSAGE_PARSE_BYTES as usize)
        );
        let assistant = "🙂".repeat(super::ACTIVITY_MESSAGE_PARSE_BYTES as usize * 2);
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('large-legacy-user','large-legacy','large-legacy',
                          '2026-07-01T00:00:00.000000000Z','user',?1,1)",
                [&wrapped_user],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('large-legacy-assistant','large-legacy','large-legacy',
                          '2026-07-01T00:01:00.000000000Z','assistant',?1,2)",
                [&assistant],
            )
            .unwrap();

        let items = super::query_legacy_message_child_rows(
            &connection,
            "large-legacy",
            &[
                "large-legacy-user".to_owned(),
                "large-legacy-assistant".to_owned(),
            ],
        )
        .unwrap();
        assert_eq!(
            items["large-legacy-user"].body.as_deref(),
            Some("Keep the tail request visible")
        );
        let assistant_preview = items["large-legacy-assistant"].body.as_deref().unwrap();
        assert_eq!(
            assistant_preview.chars().count(),
            super::ACTIVITY_PREVIEW_CHARS as usize + 1
        );
        assert!(assistant_preview.ends_with('…'));
        assert!(
            assistant_preview.len()
                <= super::ACTIVITY_PREVIEW_CHARS as usize * char::MAX.len_utf8() + '…'.len_utf8()
        );
    }

    #[test]
    fn legacy_activity_root_previews_read_only_bounded_message_edges() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('large-legacy-root','Large legacy root',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:02:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('large-legacy-root','large-legacy-root',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:02:00.000000000Z',0);",
            )
            .unwrap();

        let body_bytes = super::ACTIVITY_MESSAGE_PARSE_BYTES as usize * 4;
        let mut context_only = vec![b'x'; body_bytes];
        let context_prefix = b"# Applications mentioned by the user:\n";
        context_only[..context_prefix.len()].copy_from_slice(context_prefix);
        context_only[body_bytes / 2] = 0x80;

        let mut wrapped_request = vec![b'y'; body_bytes];
        wrapped_request[..context_prefix.len()].copy_from_slice(context_prefix);
        wrapped_request[body_bytes / 2] = 0x80;
        let request_suffix = b"\n\n## My request for Codex:\nKeep the bounded root request";
        wrapped_request[body_bytes - request_suffix.len()..].copy_from_slice(request_suffix);

        let mut assistant = vec![b'z'; body_bytes];
        let assistant_prefix = b"Latest data:image/png;base64,ZmFrZQ== answer ";
        assistant[..assistant_prefix.len()].copy_from_slice(assistant_prefix);
        assistant[body_bytes / 2] = 0x80;

        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('large-legacy-context','large-legacy-root','large-legacy-root',
                          '2026-07-01T00:00:00.000000000Z','user',?1,1)",
                [context_only],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('large-legacy-request','large-legacy-root','large-legacy-root',
                          '2026-07-01T00:01:00.000000000Z','user',?1,2)",
                [wrapped_request],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('large-legacy-answer','large-legacy-root','large-legacy-root',
                          '2026-07-01T00:02:00.000000000Z','assistant',?1,3)",
                [assistant],
            )
            .unwrap();

        let item = super::query_legacy_activity_item(
            &connection,
            "large-legacy-root",
            "large-legacy-root",
        )
        .unwrap()
        .unwrap();
        assert_eq!(item.label.as_deref(), Some("Keep the bounded root request"));
        let answer = item.body.as_deref().unwrap();
        assert!(answer.starts_with("Latest [embedded attachment] answer"));
        assert!(!answer.contains("data:image"));
        assert_eq!(
            answer.chars().count(),
            super::ACTIVITY_PREVIEW_CHARS as usize + 1
        );
        assert!(answer.ends_with('…'));
    }

    #[test]
    fn modern_activity_root_previews_read_bounded_edges() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        seed_activity_roots(&connection, "large-modern", 1);
        let wrapped_user = format!(
            "# Applications mentioned by the user:\n{}\n\n## My request for Codex:\nKeep the bounded modern tail",
            "context ".repeat(super::ACTIVITY_MESSAGE_PARSE_BYTES as usize)
        );
        connection
            .execute(
                "UPDATE events SET body=?1 WHERE thread_id='large-modern' AND id='user-0'",
                [&wrapped_user],
            )
            .unwrap();

        let response = query_activity_on(&connection, "large-modern", 1, 1).unwrap();
        assert_eq!(response.items.len(), 1);
        assert_eq!(
            response.items[0].label.as_deref(),
            Some("Keep the bounded modern tail")
        );
    }

    #[test]
    fn synthetic_group_page_decodes_only_selected_turn_bodies() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('bounded-group','Bounded group',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('bounded-group','bounded-group',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('bounded-group-root','bounded-group','bounded-group',
                        '2026-07-01T00:00:00.000000000Z','completed');
                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
                 ) VALUES('bounded-group-agent','bounded-group','bounded-group','bounded-group',
                          '2026-07-01T00:01:00.000000000Z',
                          '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
                 ) VALUES('bounded-group-agent','bounded-group','bounded-group-agent',
                          'bounded-group','Bounded agent',
                          '2026-07-01T00:01:00.000000000Z','completed');",
            )
            .unwrap();
        for index in 0..200_i64 {
            connection
                .execute(
                    "INSERT INTO turns(
                        id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
                     ) VALUES(?1,'bounded-group','bounded-group-agent','bounded-group-agent',
                              ?2,'completed',?3)",
                    params![
                        format!("bounded-group-unselected-{index:03}"),
                        format!("2026-07-01T00:05:{:02}.{:03}Z", index / 10, index % 10),
                        rusqlite::types::Value::Blob(vec![0x80]),
                    ],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
                 ) VALUES('bounded-group-latest','bounded-group','bounded-group-agent',
                          'bounded-group-agent','2026-07-01T00:10:00.000000000Z',
                          'completed','Latest child')",
                [],
            )
            .unwrap();

        let detail = query_activity_detail_page_on(
            &connection,
            "bounded-group",
            "group:agents:bounded-group-root",
            1,
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.child_total, Some(201));
        assert_eq!(detail.children.len(), 1);
        assert_eq!(detail.children[0].id, "bounded-group-latest");
        assert_eq!(detail.children[0].body.as_deref(), Some("Latest child"));

        let root =
            query_activity_detail_page_on(&connection, "bounded-group", "bounded-group-root", 1, 1)
                .unwrap()
                .unwrap();
        let group = root
            .children
            .iter()
            .find(|child| child.kind == "agent_group")
            .expect("root detail must retain the lazy agent-group placeholder");
        assert!(group.children.is_empty());
        assert_eq!(group.child_total, Some(201));
    }

    #[test]
    fn legacy_activity_cursor_is_stable_across_inserts_and_seeks_deep() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('legacy-cursor','Legacy cursor',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('legacy-cursor','legacy-cursor',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z',0);
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<2047
                 )
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 )
                 SELECT printf('legacy-cursor-message-%04d',value),
                        'legacy-cursor','legacy-cursor',
                        printf('2026-07-01T00:00:00.%09dZ',value),
                        'assistant',printf('Message %d',value),value+1
                 FROM sequence;",
            )
            .unwrap();

        let item_id = "legacy:legacy-cursor";
        let first = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-cursor",
            item_id,
            1,
            2,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            first
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>(),
            [
                "legacy-message:legacy-cursor-message-2047",
                "legacy-message:legacy-cursor-message-2046"
            ]
        );
        let first_cursor = first.child_next_cursor.unwrap();
        let numeric_second =
            query_activity_detail_page_on(&connection, "legacy-cursor", item_id, 2, 2)
                .unwrap()
                .unwrap();
        let cursor_second = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-cursor",
            item_id,
            2,
            2,
            Some(&first_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            cursor_second
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>(),
            numeric_second
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>()
        );

        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES('legacy-cursor-newer','legacy-cursor','legacy-cursor',
                          '2026-07-01T00:00:00.999999999Z','assistant','Newer',3000)",
                [],
            )
            .unwrap();
        let after_insert = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-cursor",
            item_id,
            2,
            2,
            Some(&first_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            after_insert
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>(),
            [
                "legacy-message:legacy-cursor-message-2045",
                "legacy-message:legacy-cursor-message-2044"
            ],
            "a newer insertion must not repeat or displace older cursor results"
        );

        let deep_cursor = super::encode_activity_collection_cursor(
            "legacy-cursor",
            item_id,
            "2026-07-01T00:00:00.000000010Z",
            Some(11),
            "legacy-message:legacy-cursor-message-0010",
        )
        .unwrap();
        let deep = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-cursor",
            item_id,
            2040,
            1,
            Some(&deep_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            deep.children[0].id,
            "legacy-message:legacy-cursor-message-0009"
        );
    }

    #[test]
    fn legacy_activity_orders_equal_timestamps_by_source_line() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('legacy-source-order','Legacy source order',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('legacy-source-order','legacy-source-order',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:00:00.000000000Z',0);
                 INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES
                    ('legacy-source-z','legacy-source-order','legacy-source-order',
                     '2026-07-01T00:00:00.000000000Z','assistant','Earlier line',1),
                    ('legacy-source-a','legacy-source-order','legacy-source-order',
                     '2026-07-01T00:00:00.000000000Z','assistant','Later line',2);",
            )
            .unwrap();

        let item_id = "legacy:legacy-source-order";
        let first = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-source-order",
            item_id,
            1,
            1,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(first.children[0].id, "legacy-message:legacy-source-a");
        let cursor = first.child_next_cursor.unwrap();
        let decoded =
            super::decode_activity_collection_cursor_for(&cursor, "legacy-source-order", item_id)
                .unwrap();
        assert_eq!(decoded.source_line, Some(2));
        let second = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-source-order",
            item_id,
            2,
            1,
            Some(&cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(second.children[0].id, "legacy-message:legacy-source-z");

        let old_cursor = serde_json::json!({
            "version": 1,
            "threadId": "legacy-source-order",
            "itemId": item_id,
            "timestamp": "2026-07-01T00:00:00.000000000Z",
            "sortId": "legacy-message:legacy-source-z"
        })
        .to_string();
        let from_old_cursor = super::query_activity_detail_cursor_page_on(
            &connection,
            "legacy-source-order",
            item_id,
            2,
            1,
            Some(&old_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            from_old_cursor.children[0].id,
            "legacy-message:legacy-source-a"
        );
    }

    #[test]
    fn synthetic_group_cursor_is_stable_across_inserts_and_seeks_deep() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('group-cursor','Group cursor',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('group-cursor','group-cursor',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('group-cursor-root','group-cursor','group-cursor',
                        '2026-07-01T00:00:00.000000000Z','completed');
                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
                 ) VALUES('group-cursor-agent','group-cursor','group-cursor','group-cursor',
                          '2026-07-01T00:00:00.000000000Z',
                          '2026-07-01T00:01:00.000000000Z',0);
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
                 ) VALUES('group-cursor-agent','group-cursor','group-cursor-agent',
                          'group-cursor','Cursor agent',
                          '2026-07-01T00:00:00.000000000Z','completed');
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<2047
                 )
                 INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
                 )
                 SELECT printf('group-cursor-turn-%04d',value),
                        'group-cursor','group-cursor-agent','group-cursor-agent',
                        printf('2026-07-01T00:00:00.%09dZ',value),
                        'completed',printf('Child %d',value)
                 FROM sequence;",
            )
            .unwrap();

        let item_id = "group:agents:group-cursor-root";
        let first = super::query_activity_detail_cursor_page_on(
            &connection,
            "group-cursor",
            item_id,
            1,
            2,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            first
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>(),
            ["group-cursor-turn-2047", "group-cursor-turn-2046"]
        );
        let first_cursor = first.child_next_cursor.unwrap();
        let numeric_second =
            query_activity_detail_page_on(&connection, "group-cursor", item_id, 2, 2)
                .unwrap()
                .unwrap();
        let cursor_second = super::query_activity_detail_cursor_page_on(
            &connection,
            "group-cursor",
            item_id,
            2,
            2,
            Some(&first_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            cursor_second
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>(),
            numeric_second
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>()
        );

        connection
            .execute(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status,last_agent_message
                 ) VALUES('group-cursor-newer','group-cursor','group-cursor-agent',
                          'group-cursor-agent','2026-07-01T00:00:00.999999999Z',
                          'completed','Newer')",
                [],
            )
            .unwrap();
        let after_insert = super::query_activity_detail_cursor_page_on(
            &connection,
            "group-cursor",
            item_id,
            2,
            2,
            Some(&first_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            after_insert
                .children
                .iter()
                .map(|child| child.id.as_str())
                .collect::<Vec<_>>(),
            ["group-cursor-turn-2045", "group-cursor-turn-2044"],
            "a newer group turn must not repeat or displace older cursor results"
        );

        let deep_cursor = super::encode_activity_collection_cursor(
            "group-cursor",
            item_id,
            "2026-07-01T00:00:00.000000010Z",
            None,
            "group-cursor-turn-0010",
        )
        .unwrap();
        let deep = super::query_activity_detail_cursor_page_on(
            &connection,
            "group-cursor",
            item_id,
            2040,
            1,
            Some(&deep_cursor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(deep.children[0].id, "group-cursor-turn-0009");
    }

    #[test]
    fn synthetic_group_totals_saturate_after_per_turn_sql_groups() {
        const MAX_ROLLUP_TOKENS: u64 = 9_007_199_254_740_991;
        const CHILDREN: u64 = 1_025;

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('group-overflow','Group overflow',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T01:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('group-overflow','group-overflow',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T01:00:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('group-overflow-root','group-overflow','group-overflow',
                        '2026-07-01T00:00:00.000000000Z','completed');
                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
                 ) VALUES('group-overflow-agent','group-overflow','group-overflow','group-overflow',
                          '2026-07-01T00:01:00.000000000Z',
                          '2026-07-01T01:00:00.000000000Z',0);
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
                 ) VALUES('group-overflow-agent','group-overflow','group-overflow-agent',
                          'group-overflow','Overflow agent',
                          '2026-07-01T00:01:00.000000000Z','completed');
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<1024
                 )
                 INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status
                 )
                 SELECT printf('group-overflow-turn-%04d',value),
                        'group-overflow','group-overflow-agent','group-overflow-agent',
                        printf('2026-07-01T00:01:00.%09dZ',value),'completed'
                 FROM sequence;
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<1024
                 )
                 INSERT INTO usage_activity_rollups(
                    thread_id,rollout_id,turn_key,activity_hour,model,fact_count,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 )
                 SELECT 'group-overflow','group-overflow-agent',
                        printf('group-overflow-turn-%04d',value),
                        '2026-07-01T00:00:00.000000000Z','overflow-unpriced',1,
                        9007199254740991,0,0,0,9007199254740991
                 FROM sequence;",
            )
            .unwrap();

        let detail = query_activity_detail_page_on(
            &connection,
            "group-overflow",
            "group:agents:group-overflow-root",
            1,
            1,
        )
        .unwrap()
        .unwrap();
        let totals = detail.usage.unwrap();
        assert_eq!(totals.input_tokens, MAX_ROLLUP_TOKENS * CHILDREN);
        assert_eq!(totals.total_tokens, MAX_ROLLUP_TOKENS * CHILDREN);
        assert_eq!(totals.unpriced_tokens, MAX_ROLLUP_TOKENS * CHILDREN);
        assert!(totals.cost_usd.is_none());
    }

    #[test]
    fn activity_child_page_is_turn_scoped_and_deduplicates_tool_lifecycles_deterministically() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('activity-child-scope','Child scope',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('activity-child-scope','activity-child-scope',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES
                    ('selected-turn','activity-child-scope','activity-child-scope',
                     '2026-07-01T00:00:00.000000000Z','completed'),
                    ('other-turn','activity-child-scope','activity-child-scope',
                     '2026-07-01T00:05:00.000000000Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,call_id,native
                 ) VALUES
                    ('tool-z','activity-child-scope','activity-child-scope','selected-turn',
                     '2026-07-01T00:00:01.000000000Z',10,'tool_call','selected-call',1),
                    ('tool-b','activity-child-scope','activity-child-scope','selected-turn',
                     '2026-07-01T00:00:01.000000000Z',5,'tool_call','selected-call',1),
                    ('tool-a','activity-child-scope','activity-child-scope','selected-turn',
                     '2026-07-01T00:00:01.000000000Z',5,'tool_call','selected-call',1);
                 INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,turn_id,started_at,name,status
                 ) VALUES(
                    'selected-tool','selected-call','activity-child-scope',
                    'activity-child-scope','selected-turn',
                    '2026-07-01T00:00:01.000000000Z','exec','completed'
                 );
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value + 1 < 64
                 )
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,call_id,native
                 )
                 SELECT printf('other-tool-%02d',value),
                        'activity-child-scope','activity-child-scope','other-turn',
                        '2026-07-01T00:05:01.000000000Z',value + 100,'tool_call',
                        printf('other-call-%02d',value),1
                 FROM sequence;",
            )
            .unwrap();

        let page = query_activity_child_previews_page(
            &connection,
            "activity-child-scope",
            "selected-turn",
            1,
            25,
        )
        .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, "tool-a");
        assert_eq!(page.items[0].tool_name.as_deref(), Some("exec"));

        connection
            .execute_batch(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                 ) VALUES(
                    'assistant-owner','activity-child-scope','activity-child-scope',
                    'selected-turn','2026-07-01T00:00:02.000000000Z',6,
                    'assistant','assistant','Scoped response',1
                 );
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES
                    ('tool-usage','activity-child-scope','activity-child-scope','selected-turn',
                     '2026-07-01T00:00:02.100000000Z',6,'gpt-5.5',8,0,3,0,11,1),
                    ('assistant-usage','activity-child-scope','activity-child-scope','selected-turn',
                     '2026-07-01T00:00:02.200000000Z',7,'gpt-5.5',17,0,5,0,22,1);",
            )
            .unwrap();

        let canonical_tool =
            query_activity_detail_page_on(&connection, "activity-child-scope", "tool-a", 1, 25)
                .unwrap()
                .unwrap();
        assert_eq!(canonical_tool.usage.unwrap().total_tokens, 11);

        let duplicate_tool =
            query_activity_detail_page_on(&connection, "activity-child-scope", "tool-b", 1, 25)
                .unwrap()
                .unwrap();
        assert!(duplicate_tool.usage.is_none());

        let assistant = query_activity_detail_page_on(
            &connection,
            "activity-child-scope",
            "assistant-owner",
            1,
            25,
        )
        .unwrap()
        .unwrap();
        assert_eq!(assistant.usage.unwrap().total_tokens, 22);
    }

    #[test]
    fn activity_usage_ownership_is_independent_of_child_pagination() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('paged-usage-owner','Paged usage owner',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('paged-usage-owner','paged-usage-owner',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('paged-owner-turn','paged-usage-owner','paged-usage-owner',
                        '2026-07-01T00:00:00.000000000Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                 ) VALUES
                    ('user-owner','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                     '2026-07-01T00:00:01.000000000Z',1,'user','user','Question',1),
                    ('owner-a','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                     '2026-07-01T00:00:10.000000000Z',10,'assistant','assistant','A',1),
                    ('owner-b','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                     '2026-07-01T00:00:11.000000000Z',11,'assistant','assistant','B',1);
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
                 ) VALUES
                    ('usage-after-user','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                     '2026-07-01T00:00:02.000000000Z',2,'gpt-5.5',0,0,7,0,7,1),
                    ('usage-a','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                     '2026-07-01T00:00:11.100000000Z',11,'gpt-5.5',0,0,11,0,11,1),
                    ('usage-b','paged-usage-owner','paged-usage-owner','paged-owner-turn',
                     '2026-07-01T00:00:12.000000000Z',12,'gpt-5.5',0,0,22,0,22,1);",
            )
            .unwrap();

        let all = query_activity_child_previews_page(
            &connection,
            "paged-usage-owner",
            "paged-owner-turn",
            1,
            100,
        )
        .unwrap()
        .items;
        let usage = |id: &str| {
            all.iter()
                .find(|item| item.id == id)
                .and_then(|item| item.usage.as_ref())
                .map(|usage| usage.total_tokens)
                .unwrap()
        };
        assert!(
            all.iter()
                .find(|item| item.id == "user-owner")
                .unwrap()
                .usage
                .is_none(),
            "user messages must not own adjacent model usage"
        );
        assert_eq!(usage("owner-a"), 11);
        assert_eq!(usage("owner-b"), 22);

        let page_one = query_activity_child_previews_page(
            &connection,
            "paged-usage-owner",
            "paged-owner-turn",
            1,
            1,
        )
        .unwrap();
        let page_two = query_activity_child_previews_page(
            &connection,
            "paged-usage-owner",
            "paged-owner-turn",
            2,
            1,
        )
        .unwrap();
        assert_eq!(page_one.items[0].id, "owner-b");
        assert_eq!(page_one.items[0].usage.as_ref().unwrap().total_tokens, 22);
        assert_eq!(page_two.items[0].id, "owner-a");
        assert_eq!(page_two.items[0].usage.as_ref().unwrap().total_tokens, 11);

        let direct = query_activity_detail_on(&connection, "paged-usage-owner", "owner-a")
            .unwrap()
            .unwrap();
        assert_eq!(direct.usage.unwrap().total_tokens, 11);
        let direct_user = query_activity_detail_on(&connection, "paged-usage-owner", "user-owner")
            .unwrap()
            .unwrap();
        assert!(direct_user.usage.is_none());
    }

    #[tokio::test]
    async fn activity_detail_rejects_malformed_and_wrong_scope_cursors() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('cursor-thread','Cursor thread',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('cursor-thread','cursor-thread',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status) VALUES
                    ('cursor-turn-a','cursor-thread','cursor-thread',
                     '2026-07-01T00:00:00.000000000Z','completed'),
                    ('cursor-turn-b','cursor-thread','cursor-thread',
                     '2026-07-01T00:01:00.000000000Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,
                    kind,role,body,native
                 ) VALUES
                    ('cursor-event-a','cursor-thread','cursor-thread','cursor-turn-a',
                     '2026-07-01T00:00:01.000000000Z',1,
                     'assistant','assistant','First',1),
                    ('cursor-event-b','cursor-thread','cursor-thread','cursor-turn-a',
                     '2026-07-01T00:00:02.000000000Z',2,
                     'assistant','assistant','Second',1);",
            )
            .unwrap();
        let first_page = super::query_activity_child_previews_cursor_page(
            &connection,
            "cursor-thread",
            "cursor-turn-a",
            1,
            1,
            None,
        )
        .unwrap();
        let cursor = first_page
            .next_cursor
            .expect("a two-row first page must expose a continuation cursor");
        drop(connection);

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
            DbExecutor::default(),
        );
        let malformed = super::session_activity_detail(
            State(state.clone()),
            AxumPath(("cursor-thread".into(), "cursor-turn-a".into())),
            Query(super::ActivityDetailQuery {
                child_page: Some(2),
                child_page_size: Some(1),
                child_cursor: Some("not a cursor".into()),
            }),
        )
        .await;
        let Err(malformed) = malformed else {
            panic!("malformed Activity cursor was accepted");
        };
        assert_eq!(malformed.status, StatusCode::BAD_REQUEST);

        let wrong_scope = super::session_activity_detail(
            State(state),
            AxumPath(("cursor-thread".into(), "cursor-turn-b".into())),
            Query(super::ActivityDetailQuery {
                child_page: Some(2),
                child_page_size: Some(1),
                child_cursor: Some(cursor),
            }),
        )
        .await;
        let Err(wrong_scope) = wrong_scope else {
            panic!("Activity cursor from another turn was accepted");
        };
        assert_eq!(wrong_scope.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn activity_usage_cost_converts_fixed_point_only_after_attribution() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                 ) VALUES(
                    'decimal-attribution','1970-01-01T00:00:00.000000000Z',
                    1000000000,1000000000,1000000000,'USD','manual'
                 );
                 INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('exact-activity','Exact activity',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('exact-activity','exact-activity',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:01:00.000000000Z',0);
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,nickname,started_at,status
                 ) VALUES(
                    'exact-agent','exact-activity','exact-activity','Exact agent',
                    '2026-07-01T00:00:00.000000000Z','completed'
                 );
                 INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status
                 ) VALUES(
                    'exact-turn','exact-activity','exact-activity','exact-agent',
                    '2026-07-01T00:00:00.000000000Z','completed'
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                 ) VALUES(
                    'exact-owner','exact-activity','exact-activity','exact-turn',
                    '2026-07-01T00:00:10.000000000Z',10,
                    'assistant','assistant','Exact response',1
                 );
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<9
                 )
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
                 )
                 SELECT printf('exact-usage-%02d',value),'exact-activity','exact-activity',
                        'exact-turn','exact-agent','2026-07-01T00:00:11.000000000Z',11,
                        'decimal-attribution',100,0,0,0,100,1
                 FROM sequence;",
            )
            .unwrap();

        let list_item =
            query_activity_child_previews_page(&connection, "exact-activity", "exact-turn", 1, 25)
                .unwrap()
                .items
                .into_iter()
                .find(|item| item.id == "exact-owner")
                .unwrap();
        let list_usage = list_item.usage.unwrap();
        assert_eq!(list_usage.total_tokens, 1_000);
        assert_eq!(list_usage.cost_usd.unwrap().decimal_string(), "1.00");

        let detail_usage = query_activity_detail_on(&connection, "exact-activity", "exact-owner")
            .unwrap()
            .unwrap()
            .usage
            .unwrap();
        assert_eq!(detail_usage.total_tokens, 1_000);
        assert_eq!(detail_usage.cost_usd.unwrap().decimal_string(), "1.00");

        let totals =
            super::query_totals_on(&connection, None, None, Some("exact-activity")).unwrap();
        assert_eq!(totals.known_cost_numerator, 1_000_000_000_000);
        let model = super::query_model_usage_on(&connection, "exact-activity").unwrap();
        assert_eq!(model[0].cost_usd.unwrap().decimal_string(), "1.00");
        let agent_summary = super::query_agent_summary_on(&connection, "exact-activity").unwrap();
        assert_eq!(agent_summary[0].cost_usd.unwrap().decimal_string(), "1.00");
    }

    #[test]
    #[ignore = "100k-descendant performance regression; run explicitly with --ignored --nocapture"]
    fn activity_hundred_thousand_descendants_stay_sql_backed_and_page_bounded() {
        const DESCENDANTS: u64 = 100_000;
        const REVIEWS: u64 = DESCENDANTS / 10;
        const AGENT_TURNS: u64 = DESCENDANTS - REVIEWS;
        const NON_REVIEW_AGENTS: u64 = 90;
        const REGRESSION_BUDGET: StdDuration = StdDuration::from_secs(3);

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('descendant-scale','Descendant scale',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('descendant-scale','descendant-scale',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('descendant-scale-root','descendant-scale','descendant-scale',
                        '2026-07-01T00:00:00.000000000Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,
                    kind,role,body,native
                 ) VALUES(
                    'descendant-scale-user','descendant-scale','descendant-scale',
                    'descendant-scale-root','2026-07-01T00:00:00.000000000Z',1,
                    'user','user','Scale request',1);
                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,parent_thread_id,
                    started_at,last_event_at,archived
                 ) VALUES(
                    'descendant-scale-agent','descendant-scale','descendant-scale',
                    'descendant-scale','2026-07-01T00:00:01.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
                 WITH RECURSIVE agents(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM agents WHERE value+1<100
                 )
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
                 )
                 SELECT printf('descendant-scale-agent-%03d',value),
                        'descendant-scale','descendant-scale-agent','descendant-scale',
                        printf('Scale agent %03d',value),
                        '2026-07-01T00:00:01.000000000Z','completed'
                 FROM agents;
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,
                    kind,payload_json,native
                 ) VALUES(
                    'descendant-scale-spawn','descendant-scale','descendant-scale',
                    'descendant-scale-root','2026-07-01T00:00:01.000000000Z',2,
                    'subagent','{\"agent_thread_id\":\"descendant-scale-agent-000\"}',1);
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL
                    SELECT value+1 FROM sequence WHERE value+1<100000
                 )
                 INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status,
                    model,duration_ms,last_agent_message
                 )
                 SELECT printf('descendant-scale-turn-%06d',value),
                        'descendant-scale','descendant-scale-agent',
                        printf('descendant-scale-agent-%03d',value%100),
                        '2026-07-01T00:01:00.000000000Z',
                        CASE WHEN value=1 THEN 'running'
                             WHEN value=10 THEN 'failed' ELSE 'completed' END,
                        CASE WHEN value%10=0 THEN 'codex-auto-review' ELSE 'gpt-5.5' END,
                        1000,
                        CASE WHEN value>=99992
                             THEN printf('Descendant %d',value) ELSE x'80' END
                 FROM sequence;
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL
                    SELECT value+1 FROM sequence WHERE value+1<100000
                 )
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    model,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 )
                 SELECT printf('descendant-scale-usage-%06d',value),
                        'descendant-scale','descendant-scale-agent',
                        printf('descendant-scale-turn-%06d',value),
                        printf('descendant-scale-agent-%03d',value%100),
                        '2026-07-01T00:01:01.000000000Z',
                        value+3,
                        CASE WHEN value%10=0 THEN 'codex-auto-review' ELSE 'gpt-5.5' END,
                        2,1,3,1,5,1
                 FROM sequence;",
            )
            .unwrap();

        let started = Instant::now();
        let list = query_activity_on(&connection, "descendant-scale", 1, 1).unwrap();
        let list_elapsed = started.elapsed();
        assert_eq!(list.items.len(), 1);
        let item = &list.items[0];
        assert_eq!(item.counts.as_ref().unwrap().agent_runs, NON_REVIEW_AGENTS);
        assert_eq!(item.counts.as_ref().unwrap().reviews, REVIEWS);
        assert_eq!(item.usage.as_ref().unwrap().total_tokens, DESCENDANTS * 5);

        let started = Instant::now();
        let detail = query_activity_detail_page_on(
            &connection,
            "descendant-scale",
            "descendant-scale-root",
            1,
            1,
        )
        .unwrap()
        .unwrap();
        let detail_elapsed = started.elapsed();
        assert!(
            detail.children.len() <= 3,
            "root detail materialized descendants"
        );
        assert!(serde_json::to_vec(&detail).unwrap().len() < 32 * 1024);
        let agent_group = detail
            .children
            .iter()
            .find(|child| child.kind == "agent_group")
            .unwrap();
        assert_eq!(agent_group.child_total, Some(AGENT_TURNS));
        assert_eq!(agent_group.status.as_deref(), Some("running"));
        assert_eq!(agent_group.duration_ms, Some(1000));
        let labels = agent_group.body.as_deref().unwrap();
        assert!(labels.contains("Scale agent 099"));
        assert!(labels.ends_with("+82 more"), "{labels}");
        assert_eq!(
            agent_group.usage.as_ref().unwrap().total_tokens,
            AGENT_TURNS * 5
        );
        let review_group = detail
            .children
            .iter()
            .find(|child| child.kind == "review_group")
            .unwrap();
        assert_eq!(review_group.child_total, Some(REVIEWS));
        assert_eq!(review_group.status.as_deref(), Some("attention"));
        assert_eq!(review_group.duration_ms, Some(1000));
        assert_eq!(
            review_group.usage.as_ref().unwrap().total_tokens,
            REVIEWS * 5
        );

        let started = Instant::now();
        let first_group_page = query_activity_detail_page_on(
            &connection,
            "descendant-scale",
            "group:agents:descendant-scale-root",
            1,
            7,
        )
        .unwrap()
        .unwrap();
        let group_elapsed = started.elapsed();
        assert_eq!(first_group_page.child_total, Some(AGENT_TURNS));
        assert_eq!(first_group_page.children.len(), 7);
        assert_eq!(first_group_page.child_has_more, Some(true));
        assert!(first_group_page.child_next_cursor.is_some());
        assert_eq!(
            first_group_page.children[0].body.as_deref(),
            Some("Descendant 99999")
        );

        eprintln!(
            "Activity 100k descendants: list={list_elapsed:?}, root detail={detail_elapsed:?}, \
             group page={group_elapsed:?}; budget={REGRESSION_BUDGET:?}"
        );
        for (name, elapsed) in [
            ("list", list_elapsed),
            ("root detail", detail_elapsed),
            ("group page", group_elapsed),
        ] {
            assert!(
                elapsed <= REGRESSION_BUDGET,
                "100k-descendant Activity {name} regressed: {elapsed:?}"
            );
        }
    }

    #[test]
    #[ignore = "performance benchmark; run explicitly with --ignored --nocapture"]
    fn activity_large_session_query_and_assembly_stays_within_regression_budget() {
        const TOOL_EVENTS: u64 = 500_000;
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
                 FROM sequence;
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value + 1 < 500000
                 )
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,
                    kind,call_id,native
                 )
                 SELECT
                    printf('activity-scale-event-%06d',value),
                    'activity-scale','activity-scale','activity-scale-turn',
                    '2026-07-01T00:00:01.000000000Z',value + 2,
                    'tool_call',printf('activity-scale-call-%06d',value),1
                 FROM sequence;",
            )
            .unwrap();
        drop(connection);

        let mut list_samples = Vec::with_capacity(SAMPLES);
        let mut detail_samples = Vec::with_capacity(SAMPLES);
        let mut numeric_deep_samples = Vec::with_capacity(SAMPLES);
        let mut cursor_deep_samples = Vec::with_capacity(SAMPLES);
        for sample in 1..=SAMPLES {
            // A new connection exercises per-request statement preparation and temporary
            // Activity tables while leaving fixture creation outside the measurement.
            let connection = db.connect().unwrap();
            let started = Instant::now();
            let response = query_activity_on(&connection, "activity-scale", 1, 1).unwrap();
            let encoded = serde_json::to_vec(&response).unwrap();
            let list_elapsed = started.elapsed();

            assert!(!encoded.is_empty());
            assert_eq!(response.items.len(), 1);
            assert_eq!(
                response.items[0].counts.as_ref().unwrap().tool_calls,
                TOOL_EVENTS
            );
            eprintln!("Activity 500k combined list sample {sample}: {list_elapsed:?}");
            list_samples.push(list_elapsed);

            let started = Instant::now();
            let detail = query_activity_detail_page_on(
                &connection,
                "activity-scale",
                "activity-scale-turn",
                1,
                1,
            )
            .unwrap()
            .unwrap();
            let encoded = serde_json::to_vec(&detail).unwrap();
            let detail_elapsed = started.elapsed();
            assert!(!encoded.is_empty());
            assert_eq!(detail.child_total, Some(TOOL_EVENTS + 1));
            assert_eq!(detail.children.len(), 1);
            assert_eq!(
                detail.children[0].id, "activity-scale-event-499999",
                "the first detail page must use canonical descending index order"
            );
            assert!(detail.child_next_cursor.is_some());
            eprintln!("Activity 500k combined detail sample {sample}: {detail_elapsed:?}");
            detail_samples.push(detail_elapsed);

            let started = Instant::now();
            let numeric_deep = query_activity_detail_page_on(
                &connection,
                "activity-scale",
                "activity-scale-turn",
                TOOL_EVENTS,
                1,
            )
            .unwrap()
            .unwrap();
            let encoded = serde_json::to_vec(&numeric_deep).unwrap();
            let numeric_deep_elapsed = started.elapsed();
            assert!(!encoded.is_empty());
            assert_eq!(numeric_deep.child_page, Some(TOOL_EVENTS));
            assert_eq!(numeric_deep.child_total, Some(TOOL_EVENTS + 1));
            assert_eq!(numeric_deep.children.len(), 1);
            assert_eq!(
                numeric_deep.children[0].id, "activity-scale-event-000000",
                "the compatibility OFFSET path must still reach the deep canonical row"
            );
            let deep_cursor = numeric_deep
                .child_next_cursor
                .clone()
                .expect("the penultimate detail row must expose a cursor");
            eprintln!("Activity 500k deep numeric sample {sample}: {numeric_deep_elapsed:?}");
            numeric_deep_samples.push(numeric_deep_elapsed);

            let started = Instant::now();
            let cursor_deep = super::query_activity_detail_cursor_page_on(
                &connection,
                "activity-scale",
                "activity-scale-turn",
                TOOL_EVENTS + 1,
                1,
                Some(&deep_cursor),
            )
            .unwrap()
            .unwrap();
            let encoded = serde_json::to_vec(&cursor_deep).unwrap();
            let cursor_deep_elapsed = started.elapsed();
            assert!(!encoded.is_empty());
            assert_eq!(cursor_deep.child_page, Some(TOOL_EVENTS + 1));
            assert_eq!(cursor_deep.child_total, Some(TOOL_EVENTS + 1));
            assert_eq!(cursor_deep.children.len(), 1);
            assert_eq!(
                cursor_deep.children[0].id, "activity-scale-user",
                "the deep keyset seek must continue exactly after its scoped cursor"
            );
            assert_eq!(cursor_deep.child_has_more, Some(false));
            assert!(cursor_deep.child_next_cursor.is_none());
            eprintln!("Activity 500k deep cursor sample {sample}: {cursor_deep_elapsed:?}");
            cursor_deep_samples.push(cursor_deep_elapsed);
        }

        list_samples.sort_unstable();
        detail_samples.sort_unstable();
        numeric_deep_samples.sort_unstable();
        cursor_deep_samples.sort_unstable();
        let list_median = list_samples[SAMPLES / 2];
        let list_slowest = list_samples[SAMPLES - 1];
        let detail_median = detail_samples[SAMPLES / 2];
        let detail_slowest = detail_samples[SAMPLES - 1];
        let numeric_deep_median = numeric_deep_samples[SAMPLES / 2];
        let numeric_deep_slowest = numeric_deep_samples[SAMPLES - 1];
        let cursor_deep_median = cursor_deep_samples[SAMPLES / 2];
        let cursor_deep_slowest = cursor_deep_samples[SAMPLES - 1];
        eprintln!(
            "Activity 500k combined: list median={list_median:?}, slowest={list_slowest:?}; \
             first detail median={detail_median:?}, slowest={detail_slowest:?}; \
             deep numeric median={numeric_deep_median:?}, slowest={numeric_deep_slowest:?}; \
             deep cursor median={cursor_deep_median:?}, slowest={cursor_deep_slowest:?}; \
             budget={REGRESSION_BUDGET:?}"
        );
        assert!(
            list_slowest <= REGRESSION_BUDGET,
            "500k combined Activity list regressed: median={list_median:?}, \
             slowest={list_slowest:?}, budget={REGRESSION_BUDGET:?}"
        );
        assert!(
            detail_slowest <= REGRESSION_BUDGET,
            "500k combined Activity detail regressed: median={detail_median:?}, \
             slowest={detail_slowest:?}, budget={REGRESSION_BUDGET:?}"
        );
        assert!(
            numeric_deep_slowest <= REGRESSION_BUDGET,
            "500k combined Activity deep numeric fallback regressed: \
             median={numeric_deep_median:?}, slowest={numeric_deep_slowest:?}, \
             budget={REGRESSION_BUDGET:?}"
        );
        assert!(
            cursor_deep_slowest <= REGRESSION_BUDGET,
            "500k combined Activity deep cursor seek regressed: \
             median={cursor_deep_median:?}, slowest={cursor_deep_slowest:?}, \
             budget={REGRESSION_BUDGET:?}"
        );
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "release-mode performance gate; run with cargo test --release -- --ignored"
    )]
    fn activity_usage_heavy_queries_stay_under_one_second() {
        const USAGE_FACTS: u64 = 500_000;
        const BUDGET: StdDuration = StdDuration::from_secs(1);
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('activity-usage-scale','Activity usage scale',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('activity-usage-scale','activity-usage-scale',
                        '2026-07-01T00:00:00.000000000Z',
                        '2026-07-01T00:10:00.000000000Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('activity-usage-turn','activity-usage-scale','activity-usage-scale',
                        '2026-07-01T00:00:00.000000000Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                 ) VALUES('activity-usage-owner','activity-usage-scale','activity-usage-scale',
                          'activity-usage-turn','2026-07-01T00:00:01.000000000Z',1,
                          'assistant','assistant','Done',1);
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL
                    SELECT value+1 FROM sequence WHERE value+1<500000
                 )
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 )
                 SELECT printf('activity-usage-%06d',value),
                        'activity-usage-scale','activity-usage-scale','activity-usage-turn',
                        '2026-07-01T00:00:02.000000000Z',value+2,'gpt-5.5',
                        1,0,1,0,2,1
                 FROM sequence;",
            )
            .unwrap();
        drop(connection);

        for sample in 1..=3 {
            let connection = db.connect().unwrap();
            let started = Instant::now();
            let list = query_activity_on(&connection, "activity-usage-scale", 1, 1).unwrap();
            let list_elapsed = started.elapsed();
            assert_eq!(
                list.items[0].usage.as_ref().unwrap().total_tokens,
                USAGE_FACTS * 2
            );

            let started = Instant::now();
            let detail = query_activity_detail_page_on(
                &connection,
                "activity-usage-scale",
                "activity-usage-turn",
                1,
                1,
            )
            .unwrap()
            .unwrap();
            let detail_elapsed = started.elapsed();
            assert_eq!(detail.usage.as_ref().unwrap().total_tokens, USAGE_FACTS * 2);
            assert!(
                list_elapsed < BUDGET && detail_elapsed < BUDGET,
                "usage-heavy Activity sample {sample} exceeded {BUDGET:?}: \
                 list={list_elapsed:?}, detail={detail_elapsed:?}"
            );
        }
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
