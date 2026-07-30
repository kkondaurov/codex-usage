use super::{MAX_MODEL_ID_CHARS, manual_store::resolved_alias_layer_validation_message};
use crate::{
    calendar::canonical_utc_timestamp,
    config::{MAX_PRICING_REFRESH_HOURS, MIN_PRICING_REFRESH_HOURS, PricingConfig},
    costing::PriceMicros,
    storage::{DatabaseLock, Db, StorageExecutor, WorkClass, seed_fallback_prices},
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use serde_json::Number;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration as StdDuration,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

const MIN_RETRY_SECONDS: u64 = 30;
const MAX_RETRY_SECONDS: u64 = 30 * 60;
const MAX_DUE_CHECK_SECONDS: u64 = 60 * 60;
// SQLite can spend up to thirty seconds waiting on a healthy writer. Leave one
// additional second for scheduling and committing after the fetch deadline.
const PRICING_STORAGE_GRACE: StdDuration = StdDuration::from_secs(31);
const MAX_REMOTE_PRICING_BYTES: usize = 16 * 1024 * 1024;
const MAX_REMOTE_PRICING_RECORDS: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshErrorKind {
    Network,
    Http,
    PayloadLimit,
    InvalidPayload,
    Storage,
    Unknown,
}

impl RefreshErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Http => "http",
            Self::PayloadLimit => "payload_limit",
            Self::InvalidPayload => "invalid_payload",
            Self::Storage => "storage",
            Self::Unknown => "unknown",
        }
    }

    fn safe_message(self) -> &'static str {
        match self {
            Self::Network => "Could not reach the pricing source.",
            Self::Http => "The pricing source returned an unsuccessful response.",
            Self::PayloadLimit => "The pricing dataset exceeded its safety limit.",
            Self::InvalidPayload => "The pricing source returned an invalid or unusable dataset.",
            Self::Storage => "The refreshed pricing data could not be stored.",
            Self::Unknown => "The pricing refresh failed.",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RemotePrice {
    model_id: String,
    input_microusd_per_million: i64,
    cached_input_microusd_per_million: Option<i64>,
    output_microusd_per_million: i64,
}

#[derive(Clone, Debug, Deserialize)]
struct RemotePricing {
    input_cost_per_token: Option<Number>,
    output_cost_per_token: Option<Number>,
    cache_read_input_token_cost: Option<Number>,
}

#[derive(Clone, Debug)]
pub struct PricingSync {
    executor: StorageExecutor,
    refresh_gate: Arc<Mutex<()>>,
}

pub struct PricingRefresher {
    cancelled: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

struct CancelLockWaitOnDrop {
    cancelled: Option<Arc<AtomicBool>>,
}

impl CancelLockWaitOnDrop {
    fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            cancelled: Some(cancelled),
        }
    }

    fn disarm(&mut self) {
        self.cancelled = None;
    }
}

impl Drop for CancelLockWaitOnDrop {
    fn drop(&mut self) {
        if let Some(cancelled) = self.cancelled.take() {
            cancelled.store(true, Ordering::Release);
        }
    }
}

impl PricingRefresher {
    fn cancel(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(task) = &self.task {
            task.abort();
        }
    }

    pub async fn shutdown(mut self) {
        self.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for PricingRefresher {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl PricingSync {
    pub fn new(executor: StorageExecutor) -> Self {
        Self {
            executor,
            refresh_gate: Arc::new(Mutex::new(())),
        }
    }

    pub async fn sync_if_needed(&self, db: &Db, config: &PricingConfig) -> Result<bool> {
        match self.refresh_if_due(db, config).await {
            Ok(Some(updated)) => {
                tracing::info!(updated, source = %config.url, "refreshed remote pricing");
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(error) => {
                let db = db.clone();
                let cached = self
                    .executor
                    .run(WorkClass::Light, move || price_count(&db))
                    .await?;
                if cached > 0 {
                    tracing::warn!(%error, "failed to refresh pricing; using cached prices");
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn spawn_refresher(&self, db: Db, config: PricingConfig) -> PricingRefresher {
        let sync = self.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = cancelled.clone();
        let task = tokio::spawn(async move {
            let mut failures = 0_u32;
            while !task_cancelled.load(Ordering::Acquire) {
                match sync
                    .refresh_if_due_until(&db, &config, task_cancelled.clone())
                    .await
                {
                    Ok(Some(updated)) => {
                        failures = 0;
                        tracing::info!(updated, source = %config.url, "refreshed remote pricing");
                    }
                    Ok(None) => failures = 0,
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        tracing::warn!(%error, failures, "failed to refresh pricing; will retry");
                    }
                }

                let delay = if failures > 0 {
                    retry_delay(failures)
                } else {
                    due_check_delay(&config)
                };
                tokio::time::sleep(delay).await;
            }
        });
        PricingRefresher {
            cancelled,
            task: Some(task),
        }
    }

    pub async fn force_sync(&self, db: &Db, config: &PricingConfig) -> Result<usize> {
        let refresh_guard = self.refresh_gate.clone().lock_owned().await;
        let cancelled = Arc::new(AtomicBool::new(false));
        let database_guard = self
            .acquire_database_lock(db, config, cancelled)
            .await?
            .context("pricing refresh cancelled while waiting for database ownership")?;
        self.force_sync_locked(db, config, refresh_guard, database_guard)
            .await
    }

    async fn refresh_if_due(&self, db: &Db, config: &PricingConfig) -> Result<Option<usize>> {
        self.refresh_if_due_until(db, config, Arc::new(AtomicBool::new(false)))
            .await
    }

    async fn refresh_if_due_until(
        &self,
        db: &Db,
        config: &PricingConfig,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Option<usize>> {
        let refresh_guard = self.refresh_gate.clone().lock_owned().await;
        if cancelled.load(Ordering::Acquire) {
            return Ok(None);
        }
        if !self.is_refresh_due(db, config).await? {
            return Ok(None);
        }

        // Another process may have refreshed after our optimistic due check.
        // Acquire ownership, then recheck while holding it before doing any
        // network or replacement work.
        let Some(database_guard) = self
            .acquire_database_lock(db, config, cancelled.clone())
            .await?
        else {
            return Ok(None);
        };
        if cancelled.load(Ordering::Acquire) {
            return Ok(None);
        }
        if !self.is_refresh_due(db, config).await? {
            return Ok(None);
        }
        self.force_sync_locked(db, config, refresh_guard, database_guard)
            .await
            .map(Some)
    }

    async fn is_refresh_due(&self, db: &Db, config: &PricingConfig) -> Result<bool> {
        let due_db = db.clone();
        let due_config = config.clone();
        self.executor
            .run(WorkClass::Light, move || {
                refresh_is_due(&due_db, &due_config)
            })
            .await
    }

    async fn acquire_database_lock(
        &self,
        db: &Db,
        config: &PricingConfig,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Option<DatabaseLock>> {
        self.acquire_database_lock_with_timeout(db, cancelled, pricing_process_lock_timeout(config))
            .await
    }

    async fn acquire_database_lock_with_timeout(
        &self,
        db: &Db,
        cancelled: Arc<AtomicBool>,
        lock_timeout: StdDuration,
    ) -> Result<Option<DatabaseLock>> {
        let lock_db = db.clone();
        let mut cancel_wait_on_drop = CancelLockWaitOnDrop::new(cancelled.clone());
        let worker_result = tokio::task::spawn_blocking(move || {
            DatabaseLock::acquire_interruptible(
                &lock_db,
                "pricing-refresh",
                lock_timeout,
                &cancelled,
            )
        })
        .await;
        cancel_wait_on_drop.disarm();
        worker_result.context("pricing lock worker failed")?
    }

    async fn force_sync_locked(
        &self,
        db: &Db,
        config: &PricingConfig,
        refresh_guard: OwnedMutexGuard<()>,
        database_guard: DatabaseLock,
    ) -> Result<usize> {
        let fetched = fetch_prices(config)
            .await
            .map(|prices| (prices, Utc::now()));
        let db = db.clone();
        let source_url = config.url.clone();
        self.executor
            .run(WorkClass::Light, move || {
                let _refresh_guard = refresh_guard;
                let _database_guard = database_guard;
                let result = match fetched {
                    Ok((prices, observed_at)) => {
                        replace_remote_prices(&db, &prices, &source_url, observed_at)
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(updated) => Ok(updated),
                    Err(error) => {
                        let kind = classify_refresh_error(&error);
                        if let Err(meta_error) = record_refresh_error(&db, kind) {
                            tracing::warn!(%meta_error, "failed to record pricing refresh error");
                        }
                        Err(error)
                    }
                }
            })
            .await
    }
}

fn retry_delay(failures: u32) -> StdDuration {
    let exponent = failures.saturating_sub(1).min(16);
    let seconds = MIN_RETRY_SECONDS
        .saturating_mul(1_u64 << exponent)
        .min(MAX_RETRY_SECONDS);
    StdDuration::from_secs(seconds)
}

fn due_check_delay(config: &PricingConfig) -> StdDuration {
    let configured = config.refresh_interval_hours.saturating_mul(60 * 60);
    StdDuration::from_secs(configured.clamp(60, MAX_DUE_CHECK_SECONDS))
}

fn pricing_process_lock_timeout(config: &PricingConfig) -> StdDuration {
    StdDuration::from_secs(config.timeout_seconds.max(1)).saturating_add(PRICING_STORAGE_GRACE)
}

fn refresh_is_due(db: &Db, config: &PricingConfig) -> Result<bool> {
    let connection = db.connect()?;
    let source: Option<String> = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_source_url'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if source.as_deref() != Some(config.url.as_str()) {
        return Ok(true);
    }

    let fetched_at: Option<String> = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_refresh_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(fetched_at) = fetched_at else {
        return Ok(true);
    };
    let Ok(fetched_at) = DateTime::parse_from_rfc3339(&fetched_at) else {
        return Ok(true);
    };
    let refresh_interval = checked_refresh_interval(config.refresh_interval_hours)?;
    Ok(Utc::now().signed_duration_since(fetched_at.with_timezone(&Utc)) >= refresh_interval)
}

fn checked_refresh_interval(hours: u64) -> Result<Duration> {
    if !(MIN_PRICING_REFRESH_HOURS..=MAX_PRICING_REFRESH_HOURS).contains(&hours) {
        bail!(
            "pricing refresh hours must be between {MIN_PRICING_REFRESH_HOURS} and {MAX_PRICING_REFRESH_HOURS}"
        );
    }
    let hours = i64::try_from(hours).context("pricing refresh interval exceeds i64")?;
    Duration::try_hours(hours).context("pricing refresh interval exceeds chrono duration")
}

async fn fetch_prices(config: &PricingConfig) -> Result<Vec<RemotePrice>> {
    let client = Client::builder()
        .timeout(StdDuration::from_secs(config.timeout_seconds.max(1)))
        .user_agent(concat!("codex-usage/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build pricing HTTP client")?;
    let mut response = client
        .get(&config.url)
        .send()
        .await
        .with_context(|| format!("failed to fetch pricing dataset from {}", config.url))?;
    let status = response.status();
    if !status.is_success() {
        bail!("pricing dataset request failed with {status}");
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_REMOTE_PRICING_BYTES as u64)
    {
        bail!("pricing dataset exceeds the {MAX_REMOTE_PRICING_BYTES}-byte limit");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read pricing dataset")?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_REMOTE_PRICING_BYTES {
            bail!("pricing dataset exceeds the {MAX_REMOTE_PRICING_BYTES}-byte limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    parse_prices_payload(&bytes)
}

fn parse_prices_payload(bytes: &[u8]) -> Result<Vec<RemotePrice>> {
    if bytes.len() > MAX_REMOTE_PRICING_BYTES {
        bail!("pricing dataset exceeds the {MAX_REMOTE_PRICING_BYTES}-byte limit");
    }
    let raw: HashMap<String, RemotePricing> =
        serde_json::from_slice(bytes).context("failed to parse pricing dataset")?;
    if raw.len() > MAX_REMOTE_PRICING_RECORDS {
        bail!("pricing dataset exceeds the {MAX_REMOTE_PRICING_RECORDS}-record limit");
    }
    let prices = build_prices(raw);
    if prices.is_empty() {
        bail!("pricing dataset contained no usable OpenAI model prices");
    }
    Ok(prices)
}

fn build_prices(raw: HashMap<String, RemotePricing>) -> Vec<RemotePrice> {
    let mut resolved: HashMap<String, (u8, RemotePricing)> = HashMap::new();
    for (key, record) in raw {
        let Some((model_id, priority)) = normalize_model_key(&key) else {
            continue;
        };
        match resolved.get(&model_id) {
            Some((current_priority, _)) if *current_priority >= priority => {}
            _ => {
                resolved.insert(model_id, (priority, record));
            }
        }
    }

    let mut prices = resolved
        .into_iter()
        .filter_map(|(model_id, (_, record))| normalize_price(model_id, record))
        .collect::<Vec<_>>();
    prices.sort_by(|left, right| left.model_id.cmp(&right.model_id));
    prices
}

fn normalize_model_key(key: &str) -> Option<(String, u8)> {
    let trimmed = key.trim();
    if let Some(value) = trimmed.strip_prefix("openai/").map(str::trim) {
        return valid_remote_model_id(value).then(|| (value.to_string(), 3));
    }
    if let Some(value) = trimmed.strip_prefix("openai.").map(str::trim) {
        return valid_remote_model_id(value).then(|| (value.to_string(), 2));
    }
    (valid_remote_model_id(trimmed) && looks_like_openai_model(trimmed))
        .then(|| (trimmed.to_string(), 1))
}

fn valid_remote_model_id(value: &str) -> bool {
    !value.is_empty() && value.chars().count() <= MAX_MODEL_ID_CHARS
}

fn looks_like_openai_model(value: &str) -> bool {
    value
        .strip_prefix("gpt-")
        .and_then(|rest| rest.chars().next())
        .is_some_and(|character| character.is_ascii_digit())
        || value
            .strip_prefix('o')
            .and_then(|rest| rest.chars().next())
            .is_some_and(|character| character.is_ascii_digit())
}

fn normalize_price(model_id: String, record: RemotePricing) -> Option<RemotePrice> {
    let input = PriceMicros::from_per_token_number(&record.input_cost_per_token?).ok()?;
    let output = PriceMicros::from_per_token_number(&record.output_cost_per_token?).ok()?;
    let cached = record
        .cache_read_input_token_cost
        .as_ref()
        .map(PriceMicros::from_per_token_number)
        .transpose()
        .ok()?
        .map(PriceMicros::raw);
    Some(RemotePrice {
        model_id,
        input_microusd_per_million: input.raw(),
        cached_input_microusd_per_million: cached,
        output_microusd_per_million: output.raw(),
    })
}

fn replace_remote_prices(
    db: &Db,
    prices: &[RemotePrice],
    source_url: &str,
    fetched_at: DateTime<Utc>,
) -> Result<usize> {
    let remote_source = format!("remote:{source_url}");
    let observed_at = canonical_utc_timestamp(fetched_at);
    let mut connection = db.connect()?;
    let transaction = connection.transaction()?;
    seed_fallback_prices(&transaction)?;
    let mut updated = 0;

    let mut active_remote = HashMap::<String, Vec<RemotePrice>>::new();
    {
        let mut statement = transaction.prepare(
            "SELECT model_id,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million
             FROM model_prices
             WHERE source NOT IN ('manual','bundled-baseline')
               AND effective_to IS NULL",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(RemotePrice {
                model_id: row.get(0)?,
                input_microusd_per_million: row.get(1)?,
                cached_input_microusd_per_million: row.get(2)?,
                output_microusd_per_million: row.get(3)?,
            })
        })?;
        for row in rows {
            let price = row?;
            active_remote
                .entry(price.model_id.clone())
                .or_default()
                .push(price);
        }
    }

    // Absence from one snapshot is not evidence that a model or its price
    // stopped existing. Reconcile only prices that the feed actually reports.
    for price in prices {
        if active_remote
            .get(&price.model_id)
            .is_some_and(|active| active.as_slice() == std::slice::from_ref(price))
        {
            continue;
        }
        close_active_remote_prices(&transaction, &price.model_id, &observed_at)?;
        transaction.execute(
            "INSERT INTO model_prices(
                model_id,effective_from,effective_to,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,
                currency,source
             ) VALUES(?1,?2,NULL,?3,?4,?5,'USD',?6)
             ON CONFLICT(model_id,effective_from,source) DO UPDATE SET
                effective_to=NULL,
                input_microusd_per_million=excluded.input_microusd_per_million,
                cached_input_microusd_per_million=excluded.cached_input_microusd_per_million,
                output_microusd_per_million=excluded.output_microusd_per_million,
                currency=excluded.currency",
            params![
                price.model_id,
                observed_at,
                price.input_microusd_per_million,
                price.cached_input_microusd_per_million,
                price.output_microusd_per_million,
                remote_source,
            ],
        )?;
        updated += 1;
    }
    if let Some(message) = resolved_alias_layer_validation_message(&transaction)? {
        bail!("pricing refresh would leave {message}");
    }
    transaction.execute(
        "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_refresh_at',?1)",
        [&observed_at],
    )?;
    transaction.execute(
        "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_source_url',?1)",
        [source_url],
    )?;
    transaction.execute(
        "DELETE FROM app_meta WHERE key IN (
            'pricing_last_error','pricing_last_error_kind','pricing_last_error_at'
         )",
        [],
    )?;
    transaction.commit()?;
    Ok(updated)
}

fn close_active_remote_prices(
    connection: &rusqlite::Connection,
    model_id: &str,
    observed_at: &str,
) -> Result<()> {
    connection.execute(
        "UPDATE model_prices SET effective_to=?1
         WHERE model_id=?2
           AND source NOT IN ('manual','bundled-baseline')
           AND effective_to IS NULL",
        params![observed_at, model_id],
    )?;
    Ok(())
}

fn classify_refresh_error(error: &anyhow::Error) -> RefreshErrorKind {
    let message = error.to_string();
    if message.contains("request failed with") {
        return RefreshErrorKind::Http;
    }
    if message.contains("exceeds the") && message.contains("limit") {
        return RefreshErrorKind::PayloadLimit;
    }
    if message.contains("parse pricing dataset")
        || message.contains("no usable OpenAI model prices")
        || message.contains("pricing refresh would leave alias")
    {
        return RefreshErrorKind::InvalidPayload;
    }
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<reqwest::Error>().is_some())
    {
        return RefreshErrorKind::Network;
    }
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<rusqlite::Error>().is_some())
    {
        return RefreshErrorKind::Storage;
    }
    RefreshErrorKind::Unknown
}

fn record_refresh_error(db: &Db, kind: RefreshErrorKind) -> Result<()> {
    let mut connection = db.connect()?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_error',?1)",
        [kind.safe_message()],
    )?;
    transaction.execute(
        "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_error_kind',?1)",
        [kind.as_str()],
    )?;
    transaction.execute(
        "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_error_at',?1)",
        [Utc::now().to_rfc3339()],
    )?;
    transaction.commit()?;
    Ok(())
}

fn price_count(db: &Db) -> Result<i64> {
    Ok(db
        .connect()?
        .query_row("SELECT COUNT(*) FROM model_prices", [], |row| row.get(0))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pricing::{ManualAlias, ManualPricingStore, MutationError},
        storage::DatabaseLocation,
    };
    use axum::{Json, Router, routing::get};
    use serde_json::json;

    const BASELINE_EFFECTIVE_FROM: &str = "1970-01-01T00:00:00.000000000Z";
    const OBSERVED_ONE: &str = "2026-07-30T12:00:00.000000000Z";
    const OBSERVED_TWO: &str = "2026-07-30T13:00:00.000000000Z";
    const OBSERVED_THREE: &str = "2026-07-30T14:00:00.000000000Z";
    const OBSERVED_FOUR: &str = "2026-07-30T15:00:00.000000000Z";
    type StoredPriceInterval = (String, Option<String>, i64, Option<i64>, i64);

    fn observed_at(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn remote_price(model_id: &str, input: i64, cached: Option<i64>, output: i64) -> RemotePrice {
        RemotePrice {
            model_id: model_id.into(),
            input_microusd_per_million: input,
            cached_input_microusd_per_million: cached,
            output_microusd_per_million: output,
        }
    }

    fn insert_usage_facts(db: &Db, model_id: &str, rows: &[(&str, &str)]) {
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO threads(id,started_at,last_event_at) VALUES('pricing-history',?1,?1)",
                [rows[0].1],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES('pricing-history','pricing-history',?1,?1)",
                [rows[0].1],
            )
            .unwrap();
        for (source_line, (id, timestamp)) in rows.iter().enumerate() {
            connection
                .execute(
                    "INSERT INTO usage_facts(
                        id,thread_id,rollout_id,timestamp,source_line,model,
                        input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                        total_tokens,native
                     ) VALUES(?1,'pricing-history','pricing-history',?2,?3,?4,
                              1,0,0,0,1,1)",
                    params![id, timestamp, source_line as i64, model_id],
                )
                .unwrap();
        }
    }

    fn open_with_store(
        db_path: impl AsRef<std::path::Path>,
        pricing_path: impl AsRef<std::path::Path>,
    ) -> (Db, ManualPricingStore) {
        let location = DatabaseLocation::prepare(db_path).unwrap();
        let store = ManualPricingStore::new(pricing_path.as_ref().to_path_buf()).unwrap();
        let db = location.open().unwrap();
        store.hydrate(&db).unwrap();
        (db, store)
    }

    fn open_with_default_store(db_path: impl AsRef<std::path::Path>) -> (Db, ManualPricingStore) {
        let location = DatabaseLocation::prepare(db_path).unwrap();
        let store =
            ManualPricingStore::new(location.path().with_extension("pricing.json")).unwrap();
        let db = location.open().unwrap();
        store.hydrate(&db).unwrap();
        (db, store)
    }

    fn number(value: &str) -> Number {
        serde_json::from_str(value).unwrap()
    }

    fn record(input: &str, cached: Option<&str>, output: &str) -> RemotePricing {
        RemotePricing {
            input_cost_per_token: Some(number(input)),
            output_cost_per_token: Some(number(output)),
            cache_read_input_token_cost: cached.map(number),
        }
    }

    #[test]
    fn model_normalization_accepts_only_openai_models() {
        assert_eq!(
            normalize_model_key("openai/gpt-5.6-sol"),
            Some(("gpt-5.6-sol".to_string(), 3))
        );
        assert_eq!(
            normalize_model_key("openai.o4-mini"),
            Some(("o4-mini".to_string(), 2))
        );
        assert_eq!(
            normalize_model_key("gpt-5.5"),
            Some(("gpt-5.5".to_string(), 1))
        );
        assert_eq!(normalize_model_key("anthropic/claude-opus"), None);
        assert_eq!(normalize_model_key("azure/gpt-5.5"), None);
        assert_eq!(
            normalize_model_key(&format!("openai/gpt-{}", "x".repeat(MAX_MODEL_ID_CHARS))),
            None
        );
    }

    #[test]
    fn provider_specific_entry_wins_deterministically() {
        let raw = HashMap::from([
            ("gpt-5.5".to_string(), record("0.000001", None, "0.000002")),
            (
                "openai/gpt-5.5".to_string(),
                record("0.000005", Some("0.0000005"), "0.000030"),
            ),
        ]);
        let prices = build_prices(raw);
        assert_eq!(prices.len(), 1);
        assert_eq!(prices[0].input_microusd_per_million, 5_000_000);
        assert_eq!(prices[0].cached_input_microusd_per_million, Some(500_000));
        assert_eq!(prices[0].output_microusd_per_million, 30_000_000);
    }

    #[test]
    fn refresh_retry_is_exponential_and_bounded() {
        assert_eq!(retry_delay(1), StdDuration::from_secs(30));
        assert_eq!(retry_delay(2), StdDuration::from_secs(60));
        assert_eq!(retry_delay(7), StdDuration::from_secs(1_800));
        assert_eq!(retry_delay(u32::MAX), StdDuration::from_secs(1_800));
    }

    #[test]
    fn process_lock_timeout_covers_fetch_and_storage_without_overflow() {
        let config = |timeout_seconds| PricingConfig {
            url: "https://example.test/prices.json".into(),
            refresh_interval_hours: 24,
            timeout_seconds,
        };

        assert_eq!(
            pricing_process_lock_timeout(&config(9)),
            StdDuration::from_secs(40)
        );
        assert_eq!(
            pricing_process_lock_timeout(&config(0)),
            StdDuration::from_secs(32)
        );
        assert_eq!(
            pricing_process_lock_timeout(&config(u64::MAX)),
            StdDuration::MAX
        );
    }

    #[test]
    fn refresh_interval_construction_rejects_extreme_values_without_panicking() {
        assert_eq!(
            checked_refresh_interval(24).unwrap(),
            Duration::try_hours(24).unwrap()
        );
        for invalid in [0, MAX_PRICING_REFRESH_HOURS + 1, u64::MAX] {
            let error = checked_refresh_interval(invalid).unwrap_err().to_string();
            assert!(
                error.contains("pricing refresh hours must be between"),
                "{error}"
            );
        }
    }

    #[test]
    fn remote_pricing_payload_has_a_hard_byte_limit() {
        let oversized = vec![b' '; MAX_REMOTE_PRICING_BYTES + 1];
        let error = parse_prices_payload(&oversized).unwrap_err().to_string();
        assert!(error.contains("byte limit"), "{error}");
    }

    #[test]
    fn remote_pricing_payload_has_a_hard_record_limit() {
        let raw = (0..=MAX_REMOTE_PRICING_RECORDS)
            .map(|index| {
                (
                    format!("gpt-{index}"),
                    json!({
                        "input_cost_per_token": 0.000001,
                        "output_cost_per_token": 0.000002
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let payload = serde_json::to_vec(&raw).unwrap();
        assert!(payload.len() < MAX_REMOTE_PRICING_BYTES);
        let error = parse_prices_payload(&payload).unwrap_err().to_string();
        assert!(error.contains("record limit"), "{error}");
    }

    #[test]
    fn refresh_errors_are_classified_with_safe_public_messages() {
        let http = anyhow::anyhow!("pricing dataset request failed with 503 Service Unavailable");
        assert_eq!(classify_refresh_error(&http), RefreshErrorKind::Http);
        assert!(!RefreshErrorKind::Http.safe_message().contains("503"));

        let payload = anyhow::anyhow!("pricing dataset exceeds the 10-record limit");
        assert_eq!(
            classify_refresh_error(&payload),
            RefreshErrorKind::PayloadLimit
        );

        let invalid = anyhow::anyhow!("failed to parse pricing dataset");
        assert_eq!(
            classify_refresh_error(&invalid),
            RefreshErrorKind::InvalidPayload
        );
    }

    #[test]
    fn remote_snapshot_omission_preserves_existing_remote_and_bundled_layers() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let remote = remote_price("gpt-5.5", 9_000_000, Some(900_000), 40_000_000);
        replace_remote_prices(&db, &[remote], "manual", observed_at(OBSERVED_ONE)).unwrap();
        let connection = db.connect().unwrap();
        let layers: Vec<String> = connection
            .prepare(
                "SELECT source FROM model_prices
                 WHERE model_id='gpt-5.5' ORDER BY source",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            layers,
            ["bundled-baseline".to_string(), "remote:manual".to_string()]
        );
        let remote_source: String = connection
            .query_row(
                "SELECT source FROM resolved_model_prices
                 WHERE model_id='gpt-5.5'
                   AND effective_from<=?1
                   AND (effective_to IS NULL OR effective_to>?1)
                 ORDER BY source_priority DESC,effective_from DESC
                 LIMIT 1",
                [OBSERVED_ONE],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remote_source, "remote:manual");
        drop(connection);

        let later_snapshot = remote_price("gpt-5.6-sol", 8_000_000, Some(800_000), 35_000_000);
        replace_remote_prices(
            &db,
            &[later_snapshot],
            "bundled-baseline",
            observed_at(OBSERVED_TWO),
        )
        .unwrap();
        let preserved: (i64, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT input_microusd_per_million,source FROM resolved_model_prices
                 WHERE model_id='gpt-5.5'
                   AND effective_from<=?1
                   AND (effective_to IS NULL OR effective_to>?1)
                 ORDER BY source_priority DESC,effective_from DESC
                 LIMIT 1",
                [OBSERVED_TWO],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let raw_layers: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM model_prices WHERE model_id='gpt-5.5'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved, (9_000_000, "remote:manual".into()));
        assert_eq!(
            raw_layers, 2,
            "omission must leave both the remote and bundled layers untouched"
        );
        let effective_to: Option<String> = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT effective_to FROM model_prices
                 WHERE model_id='gpt-5.5' AND source='remote:manual'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(effective_to, None);
    }

    #[test]
    fn omitting_a_legacy_epoch_remote_price_leaves_it_active() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                 ) VALUES('gpt-5.5',?1,9000000,900000,40000000,'USD','remote:legacy')",
                [BASELINE_EFFECTIVE_FROM],
            )
            .unwrap();
        let unrelated = remote_price("gpt-5.6-sol", 8_000_000, Some(800_000), 35_000_000);

        replace_remote_prices(
            &db,
            &[unrelated],
            "https://example.test/prices.json",
            observed_at(OBSERVED_TWO),
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let source_at = |timestamp: &str| {
            connection
                .query_row(
                    "SELECT source FROM resolved_model_prices
                     WHERE model_id='gpt-5.5'
                       AND effective_from<=?1
                       AND (effective_to IS NULL OR effective_to>?1)
                     ORDER BY source_priority DESC,effective_from DESC,source DESC
                     LIMIT 1",
                    [timestamp],
                    |row| row.get::<_, String>(0),
                )
                .unwrap()
        };
        assert_eq!(source_at(OBSERVED_ONE), "remote:legacy");
        assert_eq!(source_at(OBSERVED_TWO), "remote:legacy");
        let effective_to: Option<String> = connection
            .query_row(
                "SELECT effective_to FROM model_prices
                 WHERE model_id='gpt-5.5' AND source='remote:legacy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(effective_to, None);
    }

    #[test]
    fn unchanged_remote_price_keeps_its_first_observation_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let remote = remote_price("gpt-9-observed", 1_000_000, Some(100_000), 2_000_000);

        replace_remote_prices(
            &db,
            std::slice::from_ref(&remote),
            "https://example.test/prices.json",
            observed_at(OBSERVED_ONE),
        )
        .unwrap();
        replace_remote_prices(
            &db,
            std::slice::from_ref(&remote),
            "https://example.test/relocated-prices.json",
            observed_at(OBSERVED_TWO),
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let rows: Vec<(String, Option<String>, String)> = connection
            .prepare(
                "SELECT effective_from,effective_to,source FROM model_prices
                 WHERE model_id='gpt-9-observed'
                   AND source NOT IN ('manual','bundled-baseline')",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        let refreshed_at: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_last_refresh_at'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            rows,
            [(
                OBSERVED_ONE.into(),
                None,
                "remote:https://example.test/prices.json".into()
            )],
            "an unchanged price must not fabricate a historical boundary"
        );
        assert_eq!(refreshed_at, OBSERVED_TWO);
    }

    #[test]
    fn first_remote_observation_does_not_reprice_earlier_usage() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let remote = remote_price("gpt-9-first-seen", 600_000, None, 2_000_000);

        assert_eq!(
            replace_remote_prices(
                &db,
                &[remote],
                "https://example.test/prices.json",
                observed_at(OBSERVED_TWO),
            )
            .unwrap(),
            1
        );
        insert_usage_facts(
            &db,
            "gpt-9-first-seen",
            &[("before-first", OBSERVED_ONE), ("at-first", OBSERVED_TWO)],
        );

        let connection = db.connect().unwrap();
        let costs: Vec<(String, i64, i64)> = connection
            .prepare(
                "SELECT id,price_known,cost_numerator FROM priced_usage
                 WHERE thread_id='pricing-history' ORDER BY timestamp",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            costs,
            [
                ("before-first".into(), 0, 0),
                ("at-first".into(), 1, 600_000),
            ]
        );

        let price_book = crate::usage::load_price_book_on(&connection).unwrap();
        assert!(
            price_book
                .price_at("gpt-9-first-seen", OBSERVED_ONE)
                .is_none()
        );
        let (_, at_first) = price_book
            .price_at("gpt-9-first-seen", OBSERVED_TWO)
            .unwrap();
        assert_eq!(at_first.cost_numerator(1, 0, 0), 600_000);
    }

    #[test]
    fn changed_remote_price_preserves_costs_across_the_exact_observation_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let old = remote_price("gpt-9-temporal", 1_000_000, Some(100_000), 2_000_000);
        let new = remote_price("gpt-9-temporal", 200_000, Some(20_000), 1_200_000);

        replace_remote_prices(
            &db,
            &[old],
            "https://example.test/prices.json",
            observed_at(OBSERVED_ONE),
        )
        .unwrap();
        replace_remote_prices(
            &db,
            &[new],
            "https://example.test/prices.json",
            observed_at(OBSERVED_TWO),
        )
        .unwrap();
        insert_usage_facts(
            &db,
            "gpt-9-temporal",
            &[
                ("before", "2026-07-30T12:59:59.999999999Z"),
                ("at", OBSERVED_TWO),
            ],
        );

        let connection = db.connect().unwrap();
        let intervals: Vec<(String, Option<String>, i64)> = connection
            .prepare(
                "SELECT effective_from,effective_to,input_microusd_per_million
                 FROM model_prices
                 WHERE model_id='gpt-9-temporal'
                 ORDER BY effective_from",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        let costs: Vec<(String, i64, i64)> = connection
            .prepare(
                "SELECT id,price_known,cost_numerator FROM priced_usage
                 WHERE thread_id='pricing-history' ORDER BY timestamp",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            intervals,
            [
                (OBSERVED_ONE.into(), Some(OBSERVED_TWO.into()), 1_000_000),
                (OBSERVED_TWO.into(), None, 200_000),
            ]
        );
        assert_eq!(
            costs,
            [("before".into(), 1, 1_000_000), ("at".into(), 1, 200_000)]
        );
    }

    #[test]
    fn omission_keeps_the_price_active_until_a_later_observed_change() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let remote = remote_price("gpt-9-returning", 700_000, None, 3_000_000);
        let unrelated = remote_price("gpt-8-unrelated", 1_000_000, None, 2_000_000);

        replace_remote_prices(
            &db,
            std::slice::from_ref(&remote),
            "https://example.test/prices.json",
            observed_at(OBSERVED_ONE),
        )
        .unwrap();
        replace_remote_prices(
            &db,
            std::slice::from_ref(&unrelated),
            "https://example.test/prices.json",
            observed_at(OBSERVED_TWO),
        )
        .unwrap();
        let changed = remote_price("gpt-9-returning", 300_000, None, 1_200_000);
        assert_eq!(
            replace_remote_prices(
                &db,
                &[remote.clone(), unrelated.clone()],
                "https://example.test/prices.json",
                observed_at(OBSERVED_THREE),
            )
            .unwrap(),
            0,
            "an unchanged price returning after omission adds no interval"
        );
        replace_remote_prices(
            &db,
            &[changed, unrelated],
            "https://example.test/prices.json",
            observed_at(OBSERVED_FOUR),
        )
        .unwrap();
        insert_usage_facts(
            &db,
            "gpt-9-returning",
            &[
                ("present", OBSERVED_ONE),
                ("omitted", "2026-07-30T13:30:00.000000000Z"),
                ("returned", OBSERVED_THREE),
                ("changed", OBSERVED_FOUR),
            ],
        );

        let connection = db.connect().unwrap();
        let intervals: Vec<(String, Option<String>)> = connection
            .prepare(
                "SELECT effective_from,effective_to FROM model_prices
                 WHERE model_id='gpt-9-returning' ORDER BY effective_from",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        let costs: Vec<(String, i64, i64)> = connection
            .prepare(
                "SELECT id,price_known,cost_numerator FROM priced_usage
                 WHERE thread_id='pricing-history' ORDER BY timestamp",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            intervals,
            [
                (OBSERVED_ONE.into(), Some(OBSERVED_FOUR.into())),
                (OBSERVED_FOUR.into(), None),
            ]
        );
        assert_eq!(
            costs,
            [
                ("present".into(), 1, 700_000),
                ("omitted".into(), 1, 700_000),
                ("returned".into(), 1, 700_000),
                ("changed".into(), 1, 300_000),
            ]
        );
    }

    #[test]
    fn fresh_projection_hydrates_remote_only_alias_before_remote_price_arrives() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("codex-usage.db");
        let pricing_path = temp.path().join("codex-usage.pricing.json");
        std::fs::write(
            &pricing_path,
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "prices": [],
                "aliases": [{
                    "observedModelId": "observed-remote-only",
                    "canonicalModelId": "gpt-9-remote-only"
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let (db, _store) = open_with_store(&db_path, &pricing_path);
        let connection = db.connect().unwrap();
        let hydrated_target: String = connection
            .query_row(
                "SELECT canonical_model_id FROM resolved_model_aliases
                 WHERE observed_model_id='observed-remote-only'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let target_is_priced: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM resolved_model_prices
                    WHERE model_id='gpt-9-remote-only'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hydrated_target, "gpt-9-remote-only");
        assert!(!target_is_priced);
        drop(connection);

        let unrelated = RemotePrice {
            model_id: "gpt-8-unrelated".into(),
            input_microusd_per_million: 3_000_000,
            cached_input_microusd_per_million: None,
            output_microusd_per_million: 4_000_000,
        };
        let error = replace_remote_prices(
            &db,
            &[unrelated],
            "https://example.invalid/prices.json",
            Utc::now(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("observed-remote-only pointing to unpriced model gpt-9-remote-only"),
            "{error:#}"
        );

        let remote_target = RemotePrice {
            model_id: "gpt-9-remote-only".into(),
            input_microusd_per_million: 1_000_000,
            cached_input_microusd_per_million: Some(100_000),
            output_microusd_per_million: 2_000_000,
        };
        replace_remote_prices(
            &db,
            &[remote_target],
            "https://example.invalid/prices.json",
            Utc::now(),
        )
        .unwrap();
        let resolved_source: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT price.source
                 FROM resolved_model_aliases alias
                 JOIN resolved_model_prices price
                   ON price.model_id=alias.canonical_model_id
                 WHERE alias.observed_model_id='observed-remote-only'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            resolved_source,
            "remote:https://example.invalid/prices.json"
        );
    }

    #[test]
    fn refresh_omission_preserves_remote_alias_target_and_imports_other_models() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("codex-usage.db"));
        let source_url = "https://example.invalid/prices.json";
        let remote_only = RemotePrice {
            model_id: "gpt-9-remote-only".into(),
            input_microusd_per_million: 1_000_000,
            cached_input_microusd_per_million: Some(100_000),
            output_microusd_per_million: 2_000_000,
        };
        replace_remote_prices(&db, &[remote_only], source_url, observed_at(OBSERVED_ONE)).unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "observed-remote-only".into(),
                    canonical_model_id: "gpt-9-remote-only".into(),
                },
            )
            .unwrap();

        let unrelated = RemotePrice {
            model_id: "gpt-8-unrelated".into(),
            input_microusd_per_million: 3_000_000,
            cached_input_microusd_per_million: None,
            output_microusd_per_million: 4_000_000,
        };
        replace_remote_prices(&db, &[unrelated], source_url, observed_at(OBSERVED_TWO)).unwrap();

        let connection = db.connect().unwrap();
        let resolved: (String, String, Option<String>) = connection
            .query_row(
                "SELECT alias.canonical_model_id,price.source,price.effective_to
                 FROM resolved_model_aliases alias
                 JOIN resolved_model_prices price
                   ON price.model_id=alias.canonical_model_id
                 WHERE alias.observed_model_id='observed-remote-only'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let unrelated_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM model_prices WHERE model_id='gpt-8-unrelated'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            resolved,
            (
                "gpt-9-remote-only".into(),
                format!("remote:{source_url}"),
                None
            ),
            "omission must leave the target interval open"
        );
        assert_eq!(unrelated_count, 1, "present models must still be imported");
    }

    #[test]
    fn alias_delete_rejects_a_revealed_chain_without_changing_either_store() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("codex-usage.db");
        let (db, store) = open_with_default_store(&db_path);
        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES('layered','middle',CURRENT_TIMESTAMP,'bundled-baseline')",
                [],
            )
            .unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "layered".into(),
                    canonical_model_id: "gpt-5.4".into(),
                },
            )
            .unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "middle".into(),
                    canonical_model_id: "gpt-5.5".into(),
                },
            )
            .unwrap();

        fn aliases(db: &Db) -> Vec<(String, String, String)> {
            db.connect()
                .unwrap()
                .prepare(
                    "SELECT observed_model_id,canonical_model_id,source
                     FROM model_aliases
                     WHERE observed_model_id IN ('layered','middle')
                     ORDER BY observed_model_id,source",
                )
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        }

        let aliases_before = aliases(&db);
        let sidecar_before = std::fs::read(store.path()).unwrap();
        let error = store.delete_alias(&db, "layered").unwrap_err();
        assert!(matches!(error, MutationError::Validation(_)), "{error:#}");
        assert!(error.to_string().contains("alias chain"), "{error:#}");
        assert_eq!(aliases(&db), aliases_before);
        assert_eq!(std::fs::read(store.path()).unwrap(), sidecar_before);

        drop(db);
        let reopened = Db::open(&db_path).unwrap();
        store.hydrate(&reopened).unwrap();
        assert_eq!(aliases(&reopened), aliases_before);
        let unrelated = RemotePrice {
            model_id: "gpt-8-unrelated".into(),
            input_microusd_per_million: 3_000_000,
            cached_input_microusd_per_million: None,
            output_microusd_per_million: 4_000_000,
        };
        replace_remote_prices(
            &reopened,
            &[unrelated],
            "https://example.invalid/prices.json",
            Utc::now(),
        )
        .unwrap();
    }

    #[test]
    fn alias_delete_ignores_an_unrelated_hydrated_remote_only_target() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("codex-usage.db");
        let pricing_path = temp.path().join("manual-pricing.json");
        std::fs::write(
            &pricing_path,
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "prices": [],
                "aliases": [{
                    "observedModelId": "remote-only-observed",
                    "canonicalModelId": "gpt-9-remote-only"
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let (db, store) = open_with_store(&db_path, &pricing_path);
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "temporary".into(),
                    canonical_model_id: "gpt-5.4".into(),
                },
            )
            .unwrap();

        store.delete_alias(&db, "temporary").unwrap();

        let resolved: Vec<(String, String)> = db
            .connect()
            .unwrap()
            .prepare(
                "SELECT observed_model_id,canonical_model_id
                 FROM resolved_model_aliases
                 WHERE observed_model_id IN ('remote-only-observed','temporary')
                 ORDER BY observed_model_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            resolved,
            [(
                "remote-only-observed".to_string(),
                "gpt-9-remote-only".to_string()
            )]
        );
        drop(db);
        let (_reopened, _store) = open_with_store(&db_path, &pricing_path);
    }

    async fn fixture_server() -> (String, tokio::task::JoinHandle<()>) {
        let body = json!({
            "openai/gpt-5.5": {
                "input_cost_per_token": 0.000005,
                "cache_read_input_token_cost": 0.0000005,
                "output_cost_per_token": 0.000030
            },
            "anthropic/claude-opus": {
                "input_cost_per_token": 1.0,
                "output_cost_per_token": 2.0
            }
        });
        let app = Router::new().route(
            "/prices.json",
            get(move || {
                let body = body.clone();
                async move { Json(body) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}/prices.json"), handle)
    }

    #[tokio::test]
    async fn remote_refresh_preserves_manual_override_and_stores_observation_time() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT OR REPLACE INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                 ) VALUES('gpt-5.5',?1,10000000,1000000,60000000,'USD','manual')",
                [BASELINE_EFFECTIVE_FROM],
            )
            .unwrap();
        drop(connection);

        let (url, server) = fixture_server().await;
        let config = PricingConfig {
            url: url.clone(),
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        };
        let sync = PricingSync::new(StorageExecutor::default());
        assert_eq!(sync.force_sync(&db, &config).await.unwrap(), 1);

        let connection = db.connect().unwrap();
        let (input, source): (i64, String) = connection
            .query_row(
                "SELECT input_microusd_per_million,source FROM resolved_model_prices
                 WHERE model_id='gpt-5.5' AND effective_from=?1",
                [BASELINE_EFFECTIVE_FROM],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let stored_url: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_source_url'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let fallback_source: String = connection
            .query_row(
                "SELECT source FROM model_prices WHERE model_id='gpt-5.3-codex-spark'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let (remote_effective_from, remote_input): (String, i64) = connection
            .query_row(
                "SELECT effective_from,input_microusd_per_million FROM model_prices
                 WHERE model_id='gpt-5.5' AND source=?1",
                [format!("remote:{url}")],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(input, 10_000_000);
        assert_eq!(source, "manual");
        assert_eq!(stored_url, url);
        assert_eq!(fallback_source, "bundled-baseline");
        assert_eq!(remote_input, 5_000_000);
        assert_ne!(remote_effective_from, BASELINE_EFFECTIVE_FROM);
        assert!(remote_effective_from.ends_with('Z'));
        server.abort();
        assert!(!sync.sync_if_needed(&db, &config).await.unwrap());
    }

    #[tokio::test]
    async fn successive_http_refreshes_append_only_when_the_observed_price_changes() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/prices.json",
            get({
                let calls = calls.clone();
                move || {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let price = if call == 0 {
                            json!({
                                "input_cost_per_token": 0.000001,
                                "cache_read_input_token_cost": 0.0000001,
                                "output_cost_per_token": 0.000006
                            })
                        } else {
                            json!({
                                "input_cost_per_token": 0.0000002,
                                "cache_read_input_token_cost": 0.00000002,
                                "output_cost_per_token": 0.0000012
                            })
                        };
                        if call == 0 {
                            Json(json!({
                                "openai/gpt-5.6-luna": price,
                                "openai/gpt-9-omitted": {
                                    "input_cost_per_token": 0.0000007,
                                    "output_cost_per_token": 0.000003
                                }
                            }))
                        } else {
                            Json(json!({"openai/gpt-5.6-luna": price}))
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let config = PricingConfig {
            url: format!("http://{address}/prices.json"),
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        };
        let sync = PricingSync::new(StorageExecutor::default());

        assert_eq!(sync.force_sync(&db, &config).await.unwrap(), 2);
        tokio::time::sleep(StdDuration::from_millis(2)).await;
        assert_eq!(sync.force_sync(&db, &config).await.unwrap(), 1);
        tokio::time::sleep(StdDuration::from_millis(2)).await;
        assert_eq!(sync.force_sync(&db, &config).await.unwrap(), 0);

        let connection = db.connect().unwrap();
        let intervals: Vec<StoredPriceInterval> = connection
            .prepare(
                "SELECT effective_from,effective_to,input_microusd_per_million,
                        cached_input_microusd_per_million,output_microusd_per_million
                 FROM model_prices
                 WHERE model_id='gpt-5.6-luna' AND source=?1
                 ORDER BY effective_from",
            )
            .unwrap()
            .query_map([format!("remote:{}", config.url)], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            intervals.len(),
            2,
            "the unchanged third refresh adds no row"
        );
        assert_eq!(
            (
                intervals[0].1.as_deref(),
                intervals[0].2,
                intervals[0].3,
                intervals[0].4,
            ),
            (
                Some(intervals[1].0.as_str()),
                1_000_000,
                Some(100_000),
                6_000_000
            )
        );
        assert_eq!(
            (
                intervals[1].1.as_deref(),
                intervals[1].2,
                intervals[1].3,
                intervals[1].4
            ),
            (None, 200_000, Some(20_000), 1_200_000)
        );
        assert!(
            intervals
                .iter()
                .all(|(effective_from, _, _, _, _)| effective_from.ends_with('Z'))
        );
        let omitted: (String, Option<String>, i64) = connection
            .query_row(
                "SELECT effective_from,effective_to,input_microusd_per_million
                 FROM model_prices
                 WHERE model_id='gpt-9-omitted' AND source=?1",
                [format!("remote:{}", config.url)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            omitted,
            (intervals[0].0.clone(), None, 700_000),
            "two later omissions must leave the first observed price active"
        );
        let old_observation = intervals[0].0.clone();
        let new_observation = intervals[1].0.clone();
        drop(connection);

        insert_usage_facts(
            &db,
            "gpt-5.6-luna",
            &[
                ("old-http", &old_observation),
                ("new-http", &new_observation),
            ],
        );
        let costs: Vec<(String, i64)> = db
            .connect()
            .unwrap()
            .prepare(
                "SELECT id,cost_numerator FROM priced_usage
                 WHERE thread_id='pricing-history' ORDER BY timestamp",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            costs,
            [("old-http".into(), 1_000_000), ("new-http".into(), 200_000)]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn due_refresh_rechecks_freshness_after_acquiring_database_lock() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/prices.json",
            get({
                let calls = calls.clone();
                move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Json(json!({
                            "openai/gpt-5.5": {
                                "input_cost_per_token": 0.000005,
                                "output_cost_per_token": 0.000030
                            }
                        }))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let config = PricingConfig {
            url: format!("http://{address}/prices.json"),
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        };
        let guard = DatabaseLock::acquire(&db, "pricing-refresh").unwrap();
        let refresh = {
            let db = db.clone();
            let config = config.clone();
            tokio::spawn(async move {
                PricingSync::new(StorageExecutor::default())
                    .sync_if_needed(&db, &config)
                    .await
            })
        };

        tokio::time::sleep(StdDuration::from_millis(100)).await;
        assert!(
            !refresh.is_finished(),
            "refresh did not wait for database ownership"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_source_url',?1)",
                [&config.url],
            )
            .unwrap();
        connection
            .execute(
                "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_refresh_at',?1)",
                [Utc::now().to_rfc3339()],
            )
            .unwrap();
        drop(connection);

        drop(guard);
        assert!(!refresh.await.unwrap().unwrap());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "fresh metadata written by the lock owner must suppress a stale fetch"
        );
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn contended_pricing_locks_are_cancellable_without_using_executor_permits() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let _guard = DatabaseLock::acquire(&db, "pricing-refresh").unwrap();
        let executor = StorageExecutor::new(3, 1);
        let sync = PricingSync::new(executor.clone());
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut waiters = Vec::new();
        for _ in 0..3 {
            let sync = sync.clone();
            let db = db.clone();
            let cancelled = cancelled.clone();
            waiters.push(tokio::spawn(async move {
                sync.acquire_database_lock_with_timeout(&db, cancelled, StdDuration::from_secs(5))
                    .await
            }));
        }

        tokio::time::sleep(StdDuration::from_millis(75)).await;
        assert!(
            waiters.iter().all(|waiter| !waiter.is_finished()),
            "contended waiter unexpectedly acquired ownership"
        );
        let light_result = tokio::time::timeout(
            StdDuration::from_secs(1),
            executor.run(WorkClass::Light, || Ok(42)),
        )
        .await
        .expect("pricing lock waiters occupied the database executor")
        .unwrap();
        assert_eq!(light_result, 42);

        cancelled.store(true, Ordering::Release);
        for waiter in waiters {
            assert!(
                tokio::time::timeout(StdDuration::from_secs(1), waiter)
                    .await
                    .expect("cancelled pricing lock waiter did not stop")
                    .unwrap()
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_lock_acquisition_honors_supplied_bound_for_healthy_owner() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let database_guard = DatabaseLock::acquire(&db, "pricing-refresh").unwrap();
        let sync = PricingSync::new(StorageExecutor::new(3, 1));

        let short_error = sync
            .acquire_database_lock_with_timeout(
                &db,
                Arc::new(AtomicBool::new(false)),
                StdDuration::from_millis(25),
            )
            .await
            .unwrap_err();
        assert!(
            format!("{short_error:#}").contains("timed out waiting for process lock"),
            "short acquisition bound did not expire: {short_error:#}"
        );

        let patient_waiter = {
            let sync = sync.clone();
            let db = db.clone();
            tokio::spawn(async move {
                sync.acquire_database_lock_with_timeout(
                    &db,
                    Arc::new(AtomicBool::new(false)),
                    StdDuration::from_millis(250),
                )
                .await
            })
        };
        tokio::time::sleep(StdDuration::from_millis(75)).await;
        assert!(
            !patient_waiter.is_finished(),
            "healthy owner outlived the supplied acquisition bound"
        );
        drop(database_guard);

        let acquired = tokio::time::timeout(StdDuration::from_secs(1), patient_waiter)
            .await
            .expect("patient lock waiter did not observe the healthy owner release")
            .unwrap()
            .unwrap();
        assert!(acquired.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn aborting_refresh_waiting_for_process_lock_cancels_blocking_waiter_promptly() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let database_guard = DatabaseLock::acquire(&db, "pricing-refresh").unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let sync = PricingSync::new(StorageExecutor::new(3, 1));
            let config = PricingConfig {
                url: "http://127.0.0.1:9/prices.json".into(),
                refresh_interval_hours: 24,
                timeout_seconds: 1,
            };
            let (barrier_started_tx, barrier_started_rx) = tokio::sync::oneshot::channel();
            let (barrier_release_tx, barrier_release_rx) = std::sync::mpsc::channel();
            let blocking_barrier = tokio::task::spawn_blocking(move || {
                let _ = barrier_started_tx.send(());
                let _ = barrier_release_rx.recv();
            });
            barrier_started_rx.await.unwrap();

            let mut refresh_future = Box::pin({
                let sync = sync.clone();
                let db = db.clone();
                async move { sync.force_sync(&db, &config).await }
            });
            std::future::poll_fn(|cx| {
                match std::future::Future::poll(refresh_future.as_mut(), cx) {
                    std::task::Poll::Pending if sync.refresh_gate.try_lock().is_err() => {
                        std::task::Poll::Ready(())
                    }
                    std::task::Poll::Pending => {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                    std::task::Poll::Ready(result) => {
                        panic!("refresh stopped before waiting for ownership: {result:?}")
                    }
                }
            })
            .await;
            let refresh = tokio::spawn(refresh_future);

            let (probe_tx, mut probe_rx) = tokio::sync::oneshot::channel();
            let blocking_probe = tokio::task::spawn_blocking(move || {
                let _ = probe_tx.send(());
            });
            barrier_release_tx.send(()).unwrap();
            blocking_barrier.await.unwrap();
            assert!(
                tokio::time::timeout(StdDuration::from_millis(100), &mut probe_rx)
                    .await
                    .is_err(),
                "process-lock waiter did not occupy the blocking worker"
            );
            refresh.abort();
            let _ = refresh.await;

            let waiter_stopped =
                tokio::time::timeout(StdDuration::from_secs(1), &mut probe_rx).await;
            drop(database_guard);
            blocking_probe.await.unwrap();
            assert!(
                waiter_stopped.is_ok(),
                "aborted refresh left its process-lock worker blocking the runtime"
            );
        });
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_dispatched_refresh_keeps_ownership_until_replace_finishes() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc as std_mpsc,
        };
        use tokio::sync::{Notify, mpsc};

        let calls = Arc::new(AtomicUsize::new(0));
        let release_first_response = Arc::new(Notify::new());
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/prices.json",
            get({
                let calls = calls.clone();
                let release_first_response = release_first_response.clone();
                move || {
                    let calls = calls.clone();
                    let release_first_response = release_first_response.clone();
                    let request_tx = request_tx.clone();
                    async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        request_tx.send(call).unwrap();
                        if call == 0 {
                            release_first_response.notified().await;
                        }
                        let input = if call == 0 { 0.000001 } else { 0.000009 };
                        Json(json!({
                            "openai/gpt-5.5": {
                                "input_cost_per_token": input,
                                "output_cost_per_token": 0.000030
                            }
                        }))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let config = PricingConfig {
            url: format!("http://{address}/prices.json"),
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        };
        let executor = StorageExecutor::new(3, 1);
        let sync = PricingSync::new(executor.clone());

        let mut write_connection = db.connect().unwrap();
        let write_lock = write_connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();

        let first = {
            let sync = sync.clone();
            let db = db.clone();
            let config = config.clone();
            tokio::spawn(async move { sync.force_sync(&db, &config).await })
        };
        assert_eq!(request_rx.recv().await, Some(0));

        let (blocker_release_tx, blocker_release_rx) = std_mpsc::channel();
        let (blocker_started_tx, mut blocker_started_rx) = mpsc::unbounded_channel();
        let executor_blocker = {
            let executor = executor.clone();
            tokio::spawn(async move {
                executor
                    .run(WorkClass::Light, move || {
                        blocker_started_tx.send(()).unwrap();
                        let _ = blocker_release_rx.recv();
                        Ok(())
                    })
                    .await
            })
        };
        blocker_started_rx.recv().await.unwrap();
        release_first_response.notify_one();

        tokio::time::timeout(StdDuration::from_secs(2), async {
            loop {
                match tokio::time::timeout(
                    StdDuration::from_millis(20),
                    executor.run(WorkClass::Light, || Ok(())),
                )
                .await
                {
                    Err(_) => break,
                    Ok(result) => result.unwrap(),
                }
            }
        })
        .await
        .expect("pricing replacement was not dispatched to the blocking executor");

        first.abort();
        let _ = first.await;
        assert!(
            sync.refresh_gate.try_lock().is_err(),
            "aborting the caller released in-process refresh ownership"
        );
        let lock_probe_cancelled = AtomicBool::new(false);
        let lock_error = DatabaseLock::acquire_interruptible(
            &db,
            "pricing-refresh",
            StdDuration::ZERO,
            &lock_probe_cancelled,
        )
        .unwrap_err();
        assert!(
            format!("{lock_error:#}").contains("timed out waiting for process lock"),
            "detached replacement released database ownership: {lock_error:#}"
        );
        let second = {
            let sync = PricingSync::new(executor.clone());
            let db = db.clone();
            let config = config.clone();
            tokio::spawn(async move { sync.force_sync(&db, &config).await })
        };

        drop(write_lock);
        blocker_release_tx.send(()).unwrap();
        executor_blocker.await.unwrap().unwrap();
        let second_request = tokio::time::timeout(StdDuration::from_secs(2), request_rx.recv())
            .await
            .expect("next refresh did not start after terminal replacement finished")
            .expect("pricing fixture server stopped before the next refresh");
        assert_eq!(second_request, 1);
        assert_eq!(second.await.unwrap().unwrap(), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_refreshers_are_serialized_and_later_success_clears_older_error() {
        use axum::{
            http::StatusCode,
            response::{IntoResponse, Response},
        };
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::sync::{Notify, mpsc, oneshot};

        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let release_first = Arc::new(Notify::new());
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let body = json!({
            "openai/gpt-5.5": {
                "input_cost_per_token": 0.000005,
                "cache_read_input_token_cost": 0.0000005,
                "output_cost_per_token": 0.000030
            }
        });
        let app = Router::new().route(
            "/prices.json",
            get({
                let calls = calls.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                let release_first = release_first.clone();
                move || {
                    let calls = calls.clone();
                    let active = active.clone();
                    let max_active = max_active.clone();
                    let release_first = release_first.clone();
                    let request_tx = request_tx.clone();
                    let body = body.clone();
                    async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(now_active, Ordering::SeqCst);
                        request_tx.send(call).unwrap();
                        let response: Response = if call == 0 {
                            release_first.notified().await;
                            (StatusCode::INTERNAL_SERVER_ERROR, "old failure").into_response()
                        } else {
                            Json(body).into_response()
                        };
                        active.fetch_sub(1, Ordering::SeqCst);
                        response
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let config = PricingConfig {
            url: format!("http://{address}/prices.json"),
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        };
        let first_sync = PricingSync::new(StorageExecutor::default());
        let first = {
            let sync = first_sync;
            let db = db.clone();
            let config = config.clone();
            tokio::spawn(async move { sync.force_sync(&db, &config).await })
        };
        assert_eq!(request_rx.recv().await, Some(0));

        let (second_started_tx, second_started_rx) = oneshot::channel();
        let second = {
            let sync = PricingSync::new(StorageExecutor::default());
            let db = db.clone();
            let config = config.clone();
            tokio::spawn(async move {
                second_started_tx.send(()).unwrap();
                sync.force_sync(&db, &config).await
            })
        };
        second_started_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(request_rx.try_recv().is_err());

        release_first.notify_one();
        assert!(first.await.unwrap().is_err());
        assert_eq!(request_rx.recv().await, Some(1));
        assert_eq!(second.await.unwrap().unwrap(), 1);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);

        let connection = db.connect().unwrap();
        let stale_error: Option<String> = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_last_error'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        let stale_error_kind: Option<String> = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_last_error_kind'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        let source: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_source_url'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale_error, None);
        assert_eq!(stale_error_kind, None);
        assert_eq!(source, config.url);
        server.abort();
    }

    #[tokio::test]
    async fn failed_refresh_keeps_cached_prices_and_records_the_error() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let config = PricingConfig {
            url: format!("http://{address}/prices.json"),
            refresh_interval_hours: 24,
            timeout_seconds: 1,
        };

        let sync = PricingSync::new(StorageExecutor::default());
        assert!(!sync.sync_if_needed(&db, &config).await.unwrap());
        let connection = db.connect().unwrap();
        let cached: i64 = connection
            .query_row("SELECT COUNT(*) FROM model_prices", [], |row| row.get(0))
            .unwrap();
        let error: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_last_error'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let error_kind: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='pricing_last_error_kind'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(cached > 10);
        assert_eq!(error, "Could not reach the pricing source.");
        assert_eq!(error_kind, "network");
    }
}
