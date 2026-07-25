use super::{
    MAX_MODEL_ID_CHARS, ManualAlias, ManualPrice, MutationError, PricingMutations, PricingSync,
    catalog,
};
use crate::{
    MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR,
    calendar::{canonical_utc_timestamp, local_midnight},
    config::PricingConfig,
    costing::PriceMicros,
    storage::{Db, WorkClass},
    web::{
        ReadRuntime,
        error::{ApiError, ApiResult},
        pagination::{clamped_page_size, validated_page},
    },
};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct RefreshState {
    database: Db,
    config: PricingConfig,
    sync: PricingSync,
}

pub(crate) fn router(
    reads: ReadRuntime,
    database: Db,
    config: PricingConfig,
    sync: PricingSync,
    mutations: PricingMutations,
) -> Router {
    let refresh_state = RefreshState {
        database,
        config,
        sync,
    };
    Router::new()
        .route("/prices", get(prices))
        .route("/prices/model-ids", get(price_model_ids))
        .route("/prices/metadata", get(price_metadata))
        .route("/aliases", get(aliases))
        .with_state(reads)
        .merge(
            Router::new()
                .route("/prices/{model_id}", put(put_price).delete(delete_price))
                .route(
                    "/aliases/{observed_model_id}",
                    put(put_alias).delete(delete_alias),
                )
                .with_state(mutations),
        )
        .merge(
            Router::new()
                .route("/prices/refresh", post(refresh_prices))
                .with_state(refresh_state),
        )
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

impl From<catalog::PriceRecord> for PriceRow {
    fn from(value: catalog::PriceRecord) -> Self {
        Self {
            model_id: value.model_id,
            effective_from: value.effective_from,
            effective_to: value.effective_to,
            input_per_million: value.input_per_million.decimal_string(),
            cached_input_per_million: value
                .cached_input_per_million
                .map(PriceMicros::decimal_string),
            output_per_million: value.output_per_million.decimal_string(),
            currency: value.currency,
            source: public_price_source(&value.source),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AliasRow {
    observed_model_id: String,
    canonical_model_id: String,
}

impl From<catalog::AliasRecord> for AliasRow {
    fn from(value: catalog::AliasRecord) -> Self {
        Self {
            observed_model_id: value.observed_model_id,
            canonical_model_id: value.canonical_model_id,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UnknownModelRow {
    model_id: String,
    usage_count: u64,
    total_tokens: u64,
    last_seen_at: String,
}

impl From<catalog::UnknownModel> for UnknownModelRow {
    fn from(value: catalog::UnknownModel) -> Self {
        Self {
            model_id: value.model_id,
            usage_count: value.usage_count,
            total_tokens: value.total_tokens,
            last_seen_at: value.last_seen_at,
        }
    }
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

impl From<catalog::PriceListing> for PricesResponse {
    fn from(value: catalog::PriceListing) -> Self {
        Self {
            items: value.items.into_iter().map(PriceRow::from).collect(),
            page: value.page,
            page_size: value.page_size,
            total: value.total,
            total_pages: value.total_pages,
            last_refresh_at: value.last_refresh_at,
            last_refresh_error_at: value.last_refresh_error_at,
            refresh_error_kind: value.refresh_error_kind,
            refresh_error: value.refresh_error,
            source: value.source,
        }
    }
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

impl From<catalog::AliasListing> for AliasesResponse {
    fn from(value: catalog::AliasListing) -> Self {
        Self {
            items: value.items.into_iter().map(AliasRow::from).collect(),
            page: value.page,
            page_size: value.page_size,
            total: value.total,
            total_pages: value.total_pages,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PriceMetadataResponse {
    observed_unknown: Vec<UnknownModelRow>,
    observed_unknown_total: u64,
}

impl From<catalog::PriceMetadata> for PriceMetadataResponse {
    fn from(value: catalog::PriceMetadata) -> Self {
        Self {
            observed_unknown: value
                .observed_unknown
                .into_iter()
                .map(UnknownModelRow::from)
                .collect(),
            observed_unknown_total: value.observed_unknown_total,
        }
    }
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

async fn prices(
    State(reads): State<ReadRuntime>,
    Query(query): Query<PricesQuery>,
) -> ApiResult<Json<PricesResponse>> {
    let page = validated_page(query.page)?;
    let page_size = clamped_page_size(query.page_size, 25, 100);
    let search = query.q;
    if search.as_deref().is_some_and(|value| {
        value.chars().count() > catalog::MAX_SEARCH_CHARS
            || catalog::normalize_search_text(value).chars().count() > catalog::MAX_SEARCH_CHARS
    }) {
        return Err(ApiError::bad_request(format!(
            "price search must be at most {} characters",
            catalog::MAX_SEARCH_CHARS
        )));
    }
    let listing = reads
        .snapshot(WorkClass::Heavy, move |connection| {
            catalog::prices(connection, search.as_deref(), page, page_size)
        })
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(PricesResponse::from(listing)))
}

async fn aliases(
    State(reads): State<ReadRuntime>,
    Query(query): Query<AliasesQuery>,
) -> ApiResult<Json<AliasesResponse>> {
    let page = validated_page(query.page)?;
    let page_size = clamped_page_size(query.page_size, 25, 100);
    let search = query
        .q
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if search
        .as_deref()
        .is_some_and(|value| value.chars().count() > catalog::MAX_SEARCH_CHARS)
    {
        return Err(ApiError::bad_request(format!(
            "alias search must be at most {} characters",
            catalog::MAX_SEARCH_CHARS
        )));
    }
    let listing = reads
        .snapshot(WorkClass::Light, move |connection| {
            catalog::aliases(connection, search.as_deref(), page, page_size)
        })
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(AliasesResponse::from(listing)))
}

async fn price_metadata(
    State(reads): State<ReadRuntime>,
    Query(query): Query<PriceMetadataQuery>,
) -> ApiResult<Json<PriceMetadataResponse>> {
    let unknown_limit = query
        .unknown_limit
        .unwrap_or(catalog::MAX_UNKNOWN_MODEL_RESULTS);
    if !(1..=catalog::MAX_UNKNOWN_MODEL_RESULTS).contains(&unknown_limit) {
        return Err(ApiError::bad_request(format!(
            "unknownLimit must be between 1 and {}",
            catalog::MAX_UNKNOWN_MODEL_RESULTS
        )));
    }
    let metadata = reads
        .snapshot(WorkClass::Heavy, move |connection| {
            catalog::metadata(connection, unknown_limit)
        })
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(PriceMetadataResponse::from(metadata)))
}

async fn price_model_ids(
    State(reads): State<ReadRuntime>,
    Query(query): Query<PriceModelIdsQuery>,
) -> ApiResult<Json<PriceModelIdsResponse>> {
    let limit = query.limit.unwrap_or(catalog::MAX_MODEL_ID_RESULTS);
    if !(1..=catalog::MAX_MODEL_ID_RESULTS).contains(&limit) {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {}",
            catalog::MAX_MODEL_ID_RESULTS
        )));
    }
    if query.q.as_deref().is_some_and(|value| {
        value.chars().count() > catalog::MAX_SEARCH_CHARS
            || catalog::normalize_search_text(value.trim()).chars().count()
                > catalog::MAX_SEARCH_CHARS
    }) {
        return Err(ApiError::bad_request(format!(
            "model ID search must be at most {} characters",
            catalog::MAX_SEARCH_CHARS
        )));
    }
    let search = query.q;
    let items = reads
        .snapshot(WorkClass::Heavy, move |connection| {
            catalog::model_ids(connection, search.as_deref(), limit)
        })
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(PriceModelIdsResponse { items }))
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
    State(mutations): State<PricingMutations>,
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
    let effective_from = canonical_price_timestamp(
        input
            .effective_from
            .as_deref()
            .unwrap_or("1970-01-01T00:00:00.000000000Z"),
    )?;
    let effective_to = input
        .effective_to
        .as_deref()
        .map(canonical_price_timestamp)
        .transpose()?;
    mutations
        .save_price(ManualPrice {
            model_id: model_id.to_string(),
            effective_from,
            effective_to,
            input_microusd_per_million: input_price.raw(),
            cached_input_microusd_per_million: cached_price.map(PriceMicros::raw),
            output_microusd_per_million: output_price.raw(),
        })
        .await
        .map_err(manual_mutation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeletePriceQuery {
    effective_from: Option<String>,
}

async fn delete_price(
    State(mutations): State<PricingMutations>,
    AxumPath(model_id): AxumPath<String>,
    Query(query): Query<DeletePriceQuery>,
) -> ApiResult<StatusCode> {
    let effective_from = query
        .effective_from
        .as_deref()
        .map(canonical_price_timestamp)
        .transpose()?;
    mutations
        .delete_price(model_id.trim().to_string(), effective_from)
        .await
        .map_err(manual_mutation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AliasInput {
    canonical_model_id: String,
}

async fn put_alias(
    State(mutations): State<PricingMutations>,
    AxumPath(observed_model_id): AxumPath<String>,
    Json(input): Json<AliasInput>,
) -> ApiResult<StatusCode> {
    mutations
        .save_alias(ManualAlias {
            observed_model_id,
            canonical_model_id: input.canonical_model_id,
        })
        .await
        .map_err(manual_mutation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_alias(
    State(mutations): State<PricingMutations>,
    AxumPath(observed_model_id): AxumPath<String>,
) -> ApiResult<StatusCode> {
    mutations
        .delete_alias(observed_model_id.trim().to_string())
        .await
        .map_err(manual_mutation_error)?;
    Ok(StatusCode::NO_CONTENT)
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

async fn refresh_prices(State(state): State<RefreshState>) -> ApiResult<Json<RefreshResponse>> {
    let updated = state
        .sync
        .force_sync(&state.database, &state.config)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "price refresh failed");
            ApiError::bad_gateway("Could not refresh prices; cached prices remain available.")
        })?;
    Ok(Json(RefreshResponse { updated }))
}

fn public_price_source(source: &str) -> String {
    source.strip_prefix("remote:").unwrap_or(source).to_owned()
}

fn canonical_price_timestamp(value: &str) -> ApiResult<String> {
    Ok(canonical_utc_timestamp(parse_price_timestamp(value)?))
}

fn parse_price_timestamp(value: &str) -> ApiResult<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        validate_public_year(timestamp.year())?;
        return Ok(timestamp.with_timezone(&Utc));
    }
    if let Ok(date) = parse_price_date(value) {
        return Ok(local_midnight(date));
    }
    Err(ApiError::bad_request(
        "expected RFC3339 timestamp or YYYY-MM-DD",
    ))
}

fn parse_price_date(value: &str) -> ApiResult<NaiveDate> {
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
    use super::*;
    use crate::storage::{StorageExecutor, WorkClass};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration as StdDuration;
    use tokio::sync::{mpsc, oneshot};

    #[test]
    fn mutation_errors_preserve_the_http_contract() {
        let validation = manual_mutation_error(MutationError::Validation("invalid price".into()));
        assert_eq!(validation.status(), StatusCode::BAD_REQUEST);
        assert_eq!(validation.message(), "invalid price");

        let storage = manual_mutation_error(MutationError::Storage(anyhow::anyhow!(
            "pricing store failed"
        )));
        assert_eq!(storage.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(storage.message(), "pricing store failed");
    }

    #[test]
    fn transport_payloads_remain_bounded_without_serializing_catalog_models() {
        let aliases = AliasesResponse::from(catalog::AliasListing {
            items: (0..10)
                .map(|_| catalog::AliasRecord {
                    observed_model_id: "o".repeat(MAX_MODEL_ID_CHARS),
                    canonical_model_id: "c".repeat(MAX_MODEL_ID_CHARS),
                })
                .collect(),
            page: 1,
            page_size: 10,
            total: 10,
            total_pages: 1,
        });
        assert!(serde_json::to_vec(&aliases).unwrap().len() < 8 * 1024);

        let metadata = PriceMetadataResponse::from(catalog::PriceMetadata {
            observed_unknown: (0..100)
                .map(|_| catalog::UnknownModel {
                    model_id: "m".repeat(MAX_MODEL_ID_CHARS),
                    usage_count: u64::MAX,
                    total_tokens: u64::MAX,
                    last_seen_at: "2026-07-25T00:00:00.000000000Z".into(),
                })
                .collect(),
            observed_unknown_total: 20_000,
        });
        assert!(serde_json::to_vec(&metadata).unwrap().len() < 64 * 1024);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_routes_preserve_executor_work_classes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let executor = StorageExecutor::new(4, 1);
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

        let reads = ReadRuntime::new(db, executor);
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let prices_task = {
            let reads = reads.clone();
            let entered = entered_tx.clone();
            tokio::spawn(async move {
                entered.send(()).unwrap();
                prices(
                    State(reads),
                    Query(PricesQuery {
                        q: None,
                        page: Some(1),
                        page_size: Some(25),
                    }),
                )
                .await
            })
        };
        let metadata_task = {
            let reads = reads.clone();
            let entered = entered_tx.clone();
            tokio::spawn(async move {
                entered.send(()).unwrap();
                price_metadata(
                    State(reads),
                    Query(PriceMetadataQuery {
                        unknown_limit: Some(10),
                    }),
                )
                .await
            })
        };
        let model_ids_task = {
            let reads = reads.clone();
            let entered = entered_tx;
            tokio::spawn(async move {
                entered.send(()).unwrap();
                price_model_ids(
                    State(reads),
                    Query(PriceModelIdsQuery {
                        q: None,
                        limit: Some(10),
                    }),
                )
                .await
            })
        };
        for _ in 0..3 {
            entered_rx.recv().await.unwrap();
        }
        tokio::task::yield_now().await;
        assert!(!prices_task.is_finished());
        assert!(!metadata_task.is_finished());
        assert!(!model_ids_task.is_finished());

        let _ = tokio::time::timeout(
            StdDuration::from_secs(2),
            aliases(
                State(reads),
                Query(AliasesQuery {
                    q: None,
                    page: Some(1),
                    page_size: Some(25),
                }),
            ),
        )
        .await
        .expect("the light alias catalog was blocked behind heavy reads")
        .unwrap();

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        blocker.await.unwrap().unwrap();
        let _ = prices_task.await.unwrap().unwrap();
        let _ = metadata_task.await.unwrap().unwrap();
        let _ = model_ids_task.await.unwrap().unwrap();
    }
}
