use crate::{
    config::{MAX_PRICING_REFRESH_HOURS, MIN_PRICING_REFRESH_HOURS, PricingConfig},
    db::{Db, seed_fallback_prices},
    db_executor::{DbExecutor, WorkClass},
    fixed_price::PriceMicros,
    process_lock::DatabaseLock,
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use serde_json::Number;
use std::{collections::HashMap, sync::Arc, time::Duration as StdDuration};
use tokio::sync::Mutex;

const EFFECTIVE_FROM: &str = "1970-01-01T00:00:00.000000000Z";
const MIN_RETRY_SECONDS: u64 = 30;
const MAX_RETRY_SECONDS: u64 = 30 * 60;
const MAX_DUE_CHECK_SECONDS: u64 = 60 * 60;
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

#[derive(Clone, Debug)]
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
    executor: DbExecutor,
    refresh_gate: Arc<Mutex<()>>,
}

impl PricingSync {
    pub fn new(executor: DbExecutor) -> Self {
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

    pub fn spawn_refresher(&self, db: Db, config: PricingConfig) -> tokio::task::JoinHandle<()> {
        let sync = self.clone();
        tokio::spawn(async move {
            let mut failures = 0_u32;
            loop {
                match sync.refresh_if_due(&db, &config).await {
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
        })
    }

    pub async fn force_sync(&self, db: &Db, config: &PricingConfig) -> Result<usize> {
        let _refresh_guard = self.refresh_gate.lock().await;
        let _database_guard = self.acquire_database_lock(db).await?;
        self.force_sync_locked(db, config).await
    }

    async fn refresh_if_due(&self, db: &Db, config: &PricingConfig) -> Result<Option<usize>> {
        let _refresh_guard = self.refresh_gate.lock().await;
        if !self.is_refresh_due(db, config).await? {
            return Ok(None);
        }

        // Another process may have refreshed after our optimistic due check.
        // Acquire ownership, then recheck while holding it before doing any
        // network or replacement work.
        let _database_guard = self.acquire_database_lock(db).await?;
        if !self.is_refresh_due(db, config).await? {
            return Ok(None);
        }
        self.force_sync_locked(db, config).await.map(Some)
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

    async fn acquire_database_lock(&self, db: &Db) -> Result<DatabaseLock> {
        let lock_db = db.clone();
        self.executor
            .run(WorkClass::Light, move || {
                DatabaseLock::acquire(&lock_db, "pricing-refresh")
            })
            .await
    }

    async fn force_sync_locked(&self, db: &Db, config: &PricingConfig) -> Result<usize> {
        let result = match fetch_prices(config).await {
            Ok(prices) => {
                let db = db.clone();
                let source_url = config.url.clone();
                self.executor
                    .run(WorkClass::Light, move || {
                        replace_remote_prices(&db, &prices, &source_url, Utc::now())
                    })
                    .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(updated) => Ok(updated),
            Err(error) => {
                let db = db.clone();
                let kind = classify_refresh_error(&error);
                if let Err(meta_error) = self
                    .executor
                    .run(WorkClass::Light, move || record_refresh_error(&db, kind))
                    .await
                {
                    tracing::warn!(%meta_error, "failed to record pricing refresh error");
                }
                Err(error)
            }
        }
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
        return (!value.is_empty()).then(|| (value.to_string(), 3));
    }
    if let Some(value) = trimmed.strip_prefix("openai.").map(str::trim) {
        return (!value.is_empty()).then(|| (value.to_string(), 2));
    }
    looks_like_openai_model(trimmed).then(|| (trimmed.to_string(), 1))
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
    let mut connection = db.connect()?;
    let transaction = connection.transaction()?;
    // The remote feed owns only the remote layer. Bundled fallbacks and manual
    // overrides remain present so removing or omitting a remote row reveals the
    // next layer immediately.
    transaction.execute(
        "DELETE FROM model_prices
         WHERE source NOT IN ('manual','bundled-baseline')",
        [],
    )?;
    seed_fallback_prices(&transaction)?;
    for price in prices {
        transaction.execute(
            "INSERT INTO model_prices(
                model_id,effective_from,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,
                currency,source
             ) VALUES(?1,?2,?3,?4,?5,'USD',?6)
             ON CONFLICT(model_id,effective_from,source) DO UPDATE SET
                input_microusd_per_million=excluded.input_microusd_per_million,
                cached_input_microusd_per_million=excluded.cached_input_microusd_per_million,
                output_microusd_per_million=excluded.output_microusd_per_million,
                currency=excluded.currency",
            params![
                price.model_id,
                EFFECTIVE_FROM,
                price.input_microusd_per_million,
                price.cached_input_microusd_per_million,
                price.output_microusd_per_million,
                remote_source,
            ],
        )?;
    }
    validate_resolved_alias_layer(&transaction)?;
    transaction.execute(
        "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_refresh_at',?1)",
        [fetched_at.to_rfc3339()],
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
    Ok(prices.len())
}

fn validate_resolved_alias_layer(connection: &rusqlite::Connection) -> Result<()> {
    let chain: Option<(String, String)> = connection
        .query_row(
            "SELECT a.observed_model_id,a.canonical_model_id
             FROM resolved_model_aliases a
             WHERE a.observed_model_id=a.canonical_model_id
                OR EXISTS(
                    SELECT 1 FROM resolved_model_aliases target
                    WHERE target.observed_model_id=a.canonical_model_id
                )
             ORDER BY a.observed_model_id
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((observed, canonical)) = chain {
        bail!("pricing refresh would leave alias {observed} pointing through alias {canonical}");
    }

    let dangling: Option<(String, String)> = connection
        .query_row(
            "SELECT a.observed_model_id,a.canonical_model_id
             FROM resolved_model_aliases a
             WHERE NOT EXISTS(
                SELECT 1 FROM resolved_model_prices price
                WHERE price.model_id=a.canonical_model_id
             )
             ORDER BY a.observed_model_id
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((observed, canonical)) = dangling {
        bail!(
            "pricing refresh would leave alias {observed} pointing to unpriced model {canonical}"
        );
    }

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
    use axum::{Json, Router, routing::get};
    use serde_json::json;

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
    fn remote_snapshot_coexists_with_and_reveals_bundled_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let remote = RemotePrice {
            model_id: "gpt-5.5".into(),
            input_microusd_per_million: 9_000_000,
            cached_input_microusd_per_million: Some(900_000),
            output_microusd_per_million: 40_000_000,
        };
        replace_remote_prices(&db, &[remote], "manual", Utc::now()).unwrap();
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
                "SELECT source FROM resolved_model_prices WHERE model_id='gpt-5.5'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remote_source, "remote:manual");
        drop(connection);

        let later_snapshot = RemotePrice {
            model_id: "gpt-5.6-sol".into(),
            input_microusd_per_million: 8_000_000,
            cached_input_microusd_per_million: Some(800_000),
            output_microusd_per_million: 35_000_000,
        };
        replace_remote_prices(&db, &[later_snapshot], "bundled-baseline", Utc::now()).unwrap();
        let restored: (i64, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT input_microusd_per_million,source FROM resolved_model_prices
                 WHERE model_id='gpt-5.5'",
                [],
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
        assert_eq!(restored, (5_000_000, "bundled-baseline".into()));
        assert_eq!(raw_layers, 1);
    }

    #[test]
    fn refresh_rollback_preserves_remote_alias_target_when_next_snapshot_omits_it() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("codex-usage.db")).unwrap();
        let source_url = "https://example.invalid/prices.json";
        let remote_only = RemotePrice {
            model_id: "gpt-9-remote-only".into(),
            input_microusd_per_million: 1_000_000,
            cached_input_microusd_per_million: Some(100_000),
            output_microusd_per_million: 2_000_000,
        };
        replace_remote_prices(&db, &[remote_only], source_url, Utc::now()).unwrap();
        db.manual_pricing()
            .save_alias(
                &db,
                crate::manual_pricing::ManualAlias {
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
        let error = replace_remote_prices(&db, &[unrelated], source_url, Utc::now()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("observed-remote-only pointing to unpriced model gpt-9-remote-only"),
            "{error:#}"
        );

        let connection = db.connect().unwrap();
        let resolved: (String, String) = connection
            .query_row(
                "SELECT alias.canonical_model_id,price.source
                 FROM resolved_model_aliases alias
                 JOIN resolved_model_prices price
                   ON price.model_id=alias.canonical_model_id
                 WHERE alias.observed_model_id='observed-remote-only'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
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
            ("gpt-9-remote-only".into(), format!("remote:{source_url}"))
        );
        assert_eq!(unrelated_count, 0, "the rejected snapshot must roll back");
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
    async fn remote_refresh_replaces_cache_and_preserves_manual_override() {
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
                [EFFECTIVE_FROM],
            )
            .unwrap();
        drop(connection);

        let (url, server) = fixture_server().await;
        let config = PricingConfig {
            url: url.clone(),
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        };
        let sync = PricingSync::new(DbExecutor::default());
        assert_eq!(sync.force_sync(&db, &config).await.unwrap(), 1);

        let connection = db.connect().unwrap();
        let (input, source): (i64, String) = connection
            .query_row(
                "SELECT input_microusd_per_million,source FROM resolved_model_prices
                 WHERE model_id='gpt-5.5' AND effective_from=?1",
                [EFFECTIVE_FROM],
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
        assert_eq!(input, 10_000_000);
        assert_eq!(source, "manual");
        assert_eq!(stored_url, url);
        assert_eq!(fallback_source, "bundled-baseline");
        server.abort();
        assert!(!sync.sync_if_needed(&db, &config).await.unwrap());
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
                PricingSync::new(DbExecutor::default())
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
        let first_sync = PricingSync::new(DbExecutor::default());
        let first = {
            let sync = first_sync;
            let db = db.clone();
            let config = config.clone();
            tokio::spawn(async move { sync.force_sync(&db, &config).await })
        };
        assert_eq!(request_rx.recv().await, Some(0));

        let (second_started_tx, second_started_rx) = oneshot::channel();
        let second = {
            let sync = PricingSync::new(DbExecutor::default());
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

        let sync = PricingSync::new(DbExecutor::default());
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
