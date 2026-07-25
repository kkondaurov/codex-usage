use crate::{
    calendar::canonical_utc_timestamp,
    storage::{Db, canonicalize_storage_path, reject_multiply_linked_storage},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
#[cfg(unix)]
use std::{
    os::fd::AsRawFd,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    thread,
    time::Instant,
};
use thiserror::Error;

const CONFIG_VERSION: u32 = 1;
const PRICING_FILE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const PRICING_FILE_LOCK_RETRY: Duration = Duration::from_millis(25);
pub const MAX_MODEL_ID_CHARS: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManualPrice {
    pub model_id: String,
    pub effective_from: String,
    pub effective_to: Option<String>,
    pub input_microusd_per_million: i64,
    pub cached_input_microusd_per_million: Option<i64>,
    pub output_microusd_per_million: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManualAlias {
    pub observed_model_id: String,
    pub canonical_model_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManualPricingConfig {
    version: u32,
    #[serde(default)]
    prices: Vec<ManualPrice>,
    #[serde(default)]
    aliases: Vec<ManualAlias>,
}

impl Default for ManualPricingConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            prices: Vec::new(),
            aliases: Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum MutationError {
    #[error("{0}")]
    Validation(String),
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}

#[derive(Clone, Debug)]
pub struct ManualPricingStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl ManualPricingStore {
    pub fn new(path: PathBuf) -> Result<Self> {
        let path = canonicalize_storage_path(&path)?;
        reject_pricing_hardlinks(&path)?;
        Ok(Self {
            path,
            lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn hydrate(&self, db: &Db) -> Result<()> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("pricing config lock poisoned"))?;
        let _file_guard = self.acquire_file_lock()?;
        let config = self.load_unlocked()?;
        validate_config(&config)?;
        let mut connection = db.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("DELETE FROM model_prices WHERE source='manual'", [])?;
        transaction.execute("DELETE FROM model_aliases WHERE source='manual'", [])?;
        for price in &config.prices {
            insert_price(&transaction, price)?;
        }
        for alias in &config.aliases {
            insert_alias(&transaction, alias)?;
        }
        // Validate the fully layered result, not an insertion-order-dependent
        // intermediate state. Manual aliases may intentionally override a
        // lower row with the same observed ID, but no resolved alias may point
        // through another resolved alias in either direction.
        for alias in &config.aliases {
            if let Some(message) = alias_validation_message(
                &transaction,
                &alias.observed_model_id,
                &alias.canonical_model_id,
                false,
            )? {
                return Err(anyhow!(message));
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn save_price(&self, db: &Db, price: ManualPrice) -> Result<(), MutationError> {
        validate_price(&price).map_err(|error| validation(error.to_string()))?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| storage("pricing config lock poisoned"))?;
        let _file_guard = self.acquire_file_lock().map_err(MutationError::Storage)?;
        let previous = self.load_unlocked().map_err(MutationError::Storage)?;
        let mut config = previous.clone();
        config.prices.retain(|row| {
            row.model_id != price.model_id || row.effective_from != price.effective_from
        });
        config.prices.push(price.clone());
        sort_config(&mut config);

        let mut connection = db.connect().map_err(MutationError::Storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| MutationError::Storage(error.into()))?;
        insert_price(&transaction, &price).map_err(MutationError::Storage)?;
        self.write_then_commit(&previous, &config, || {
            transaction.commit().map_err(Into::into)
        })
    }

    pub fn delete_price(
        &self,
        db: &Db,
        model_id: &str,
        effective_from: Option<&str>,
    ) -> Result<(), MutationError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| storage("pricing config lock poisoned"))?;
        let _file_guard = self.acquire_file_lock().map_err(MutationError::Storage)?;
        let previous = self.load_unlocked().map_err(MutationError::Storage)?;
        let mut config = previous.clone();
        config.prices.retain(|row| {
            row.model_id != model_id
                || effective_from.is_some_and(|value| row.effective_from != value)
        });

        let mut connection = db.connect().map_err(MutationError::Storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| MutationError::Storage(error.into()))?;
        match effective_from {
            Some(value) => {
                transaction
                    .execute(
                        "DELETE FROM model_prices
                         WHERE model_id=?1 AND effective_from=?2 AND source='manual'",
                        params![model_id, value],
                    )
                    .map_err(|error| MutationError::Storage(error.into()))?;
            }
            None => {
                transaction
                    .execute(
                        "DELETE FROM model_prices WHERE model_id=?1 AND source='manual'",
                        [model_id],
                    )
                    .map_err(|error| MutationError::Storage(error.into()))?;
            }
        }
        let has_price: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM resolved_model_prices WHERE model_id=?1)",
                [model_id],
                |row| row.get(0),
            )
            .map_err(|error| MutationError::Storage(error.into()))?;
        let is_alias_target: bool = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM resolved_model_aliases WHERE canonical_model_id=?1
                 )",
                [model_id],
                |row| row.get(0),
            )
            .map_err(|error| MutationError::Storage(error.into()))?;
        if !has_price && is_alias_target {
            return Err(validation(format!(
                "cannot delete the last price for {model_id} while an alias uses it"
            )));
        }
        self.write_then_commit(&previous, &config, || {
            transaction.commit().map_err(Into::into)
        })
    }

    pub fn save_alias(&self, db: &Db, alias: ManualAlias) -> Result<(), MutationError> {
        let observed = alias.observed_model_id.trim();
        let canonical = alias.canonical_model_id.trim();
        validate_model_id(observed, "observed model ID")
            .map_err(|error| validation(error.to_string()))?;
        validate_model_id(canonical, "canonical model ID")
            .map_err(|error| validation(error.to_string()))?;
        if observed == canonical {
            return Err(validation("an alias cannot map a model ID to itself"));
        }

        let _guard = self
            .lock
            .lock()
            .map_err(|_| storage("pricing config lock poisoned"))?;
        let _file_guard = self.acquire_file_lock().map_err(MutationError::Storage)?;
        let mut connection = db.connect().map_err(MutationError::Storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| MutationError::Storage(error.into()))?;

        if let Some(message) = alias_validation_message(&transaction, observed, canonical, true)
            .map_err(MutationError::Storage)?
        {
            return Err(validation(message));
        }

        let previous = self.load_unlocked().map_err(MutationError::Storage)?;
        let mut config = previous.clone();
        config
            .aliases
            .retain(|row| row.observed_model_id != observed);
        let alias = ManualAlias {
            observed_model_id: observed.to_string(),
            canonical_model_id: canonical.to_string(),
        };
        config.aliases.push(alias.clone());
        sort_config(&mut config);
        insert_alias(&transaction, &alias).map_err(MutationError::Storage)?;
        self.write_then_commit(&previous, &config, || {
            transaction.commit().map_err(Into::into)
        })
    }

    pub fn delete_alias(&self, db: &Db, observed: &str) -> Result<(), MutationError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| storage("pricing config lock poisoned"))?;
        let _file_guard = self.acquire_file_lock().map_err(MutationError::Storage)?;
        let previous = self.load_unlocked().map_err(MutationError::Storage)?;
        let mut config = previous.clone();
        config
            .aliases
            .retain(|row| row.observed_model_id != observed);

        let mut connection = db.connect().map_err(MutationError::Storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| MutationError::Storage(error.into()))?;
        transaction
            .execute(
                "DELETE FROM model_aliases
                 WHERE observed_model_id=?1 AND source='manual'",
                [observed],
            )
            .map_err(|error| MutationError::Storage(error.into()))?;
        if let Some(message) =
            resolved_alias_layer_validation_message_for_observed(&transaction, observed)
                .map_err(MutationError::Storage)?
        {
            return Err(validation(format!(
                "cannot delete alias {observed}: {message}"
            )));
        }
        self.write_then_commit(&previous, &config, || {
            transaction.commit().map_err(Into::into)
        })
    }

    fn load_unlocked(&self) -> Result<ManualPricingConfig> {
        // Recheck under the store/file-lock boundary. A second hard link may
        // have been created after startup; continuing would derive a distinct
        // lock path and an atomic rename would silently split the aliases.
        reject_pricing_hardlinks(&self.path)?;
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Default::default()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", self.path.display()));
            }
        };
        repair_private_permissions(&file, &self.path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read {}", self.path.display()))?;
        let config: ManualPricingConfig = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", self.path.display()))?;
        if config.version != CONFIG_VERSION {
            return Err(anyhow!(
                "unsupported pricing config version {} in {}",
                config.version,
                self.path.display()
            ));
        }
        validate_config(&config)?;
        Ok(config)
    }

    fn acquire_file_lock(&self) -> Result<PricingFileLock> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let file_name = self
            .path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("pricing.json");
        let lock_name = if file_name.starts_with('.') {
            format!("{file_name}.lock")
        } else {
            format!(".{file_name}.lock")
        };
        let path = parent.join(lock_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .apply_private_mode()
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        repair_private_permissions(&file, &path)?;
        lock_file(&file, PRICING_FILE_LOCK_TIMEOUT)
            .with_context(|| format!("failed to lock {}", path.display()))?;
        Ok(PricingFileLock { file })
    }

    fn write_unlocked(&self, config: &ManualPricingConfig) -> Result<()> {
        validate_config(config)?;
        reject_pricing_hardlinks(&self.path)?;
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let file_name = self
            .path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("pricing.json");
        let temporary = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let payload = serde_json::to_vec_pretty(config)?;
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .apply_private_mode()
                .open(&temporary)
                .with_context(|| format!("failed to create {}", temporary.display()))?;
            file.write_all(&payload)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path).with_context(|| {
                format!(
                    "failed to replace {} with {}",
                    self.path.display(),
                    temporary.display()
                )
            })?;
            sync_directory(parent)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn write_then_commit(
        &self,
        previous: &ManualPricingConfig,
        next: &ManualPricingConfig,
        commit: impl FnOnce() -> Result<()>,
    ) -> Result<(), MutationError> {
        self.write_unlocked(next).map_err(MutationError::Storage)?;
        if let Err(commit_error) = commit() {
            return match self.write_unlocked(previous) {
                Ok(()) => Err(MutationError::Storage(
                    commit_error.context("database commit failed; restored pricing.json"),
                )),
                Err(restore_error) => Err(MutationError::Storage(anyhow!(
                    "database commit failed ({commit_error:#}) and pricing.json restoration failed ({restore_error:#})"
                ))),
            };
        }
        Ok(())
    }
}

fn reject_pricing_hardlinks(path: &Path) -> Result<()> {
    reject_multiply_linked_storage(
        path,
        "pricing locks and atomic replacement require one path identity",
    )
}

struct PricingFileLock {
    file: File,
}

impl Drop for PricingFileLock {
    fn drop(&mut self) {
        unlock_file(&self.file);
    }
}

#[cfg(unix)]
fn lock_file(file: &File, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        match error.kind() {
            ErrorKind::Interrupted => continue,
            ErrorKind::WouldBlock if started.elapsed() < timeout => {
                thread::sleep(
                    PRICING_FILE_LOCK_RETRY.min(timeout.saturating_sub(started.elapsed())),
                );
            }
            ErrorKind::WouldBlock => {
                return Err(anyhow!("timed out waiting for pricing config lock"));
            }
            _ => return Err(error.into()),
        }
    }
}

#[cfg(not(unix))]
fn lock_file(_file: &File, _timeout: Duration) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn unlock_file(file: &File) {
    // SAFETY: `file` still owns a valid descriptor while the guard is dropped.
    let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
}

#[cfg(not(unix))]
fn unlock_file(_file: &File) {}

#[cfg(unix)]
fn repair_private_permissions(file: &File, path: &Path) -> Result<()> {
    let mode = file.metadata()?.permissions().mode() & 0o777;
    if mode != 0o600 {
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn repair_private_permissions(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

trait PrivateOpenOptions {
    fn apply_private_mode(&mut self) -> &mut Self;
}

impl PrivateOpenOptions for OpenOptions {
    fn apply_private_mode(&mut self) -> &mut Self {
        #[cfg(unix)]
        self.mode(0o600);
        self
    }
}

fn validate_config(config: &ManualPricingConfig) -> Result<()> {
    let mut price_keys = HashSet::new();
    for price in &config.prices {
        validate_price(price)?;
        if !price_keys.insert((&price.model_id, &price.effective_from)) {
            return Err(anyhow!(
                "duplicate manual price for {} at {}",
                price.model_id,
                price.effective_from
            ));
        }
    }
    let mut observed = HashSet::new();
    for alias in &config.aliases {
        validate_model_id(&alias.observed_model_id, "observed model ID")?;
        validate_model_id(&alias.canonical_model_id, "canonical model ID")?;
        if alias.observed_model_id == alias.canonical_model_id {
            return Err(anyhow!("an alias cannot map a model ID to itself"));
        }
        if !observed.insert(alias.observed_model_id.as_str()) {
            return Err(anyhow!(
                "duplicate manual alias for {}",
                alias.observed_model_id
            ));
        }
    }
    let targets = config
        .aliases
        .iter()
        .map(|alias| alias.canonical_model_id.as_str())
        .collect::<HashSet<_>>();
    if let Some(chain) = config
        .aliases
        .iter()
        .find(|alias| targets.contains(alias.observed_model_id.as_str()))
    {
        return Err(anyhow!(
            "manual alias {} participates in an alias chain",
            chain.observed_model_id
        ));
    }
    Ok(())
}

fn alias_validation_message(
    connection: &rusqlite::Connection,
    observed: &str,
    canonical: &str,
    require_priced_target: bool,
) -> Result<Option<String>> {
    let target_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM resolved_model_prices WHERE model_id=?1)",
        [canonical],
        |row| row.get(0),
    )?;

    let target_is_alias: Option<String> = connection
        .query_row(
            "SELECT canonical_model_id FROM resolved_model_aliases
             WHERE observed_model_id=?1 AND observed_model_id<>?2",
            params![canonical, observed],
            |row| row.get(0),
        )
        .optional()?;
    if target_is_alias.is_some() {
        return Ok(Some(format!(
            "canonical model {canonical} is itself an alias; alias chains are not supported"
        )));
    }

    let observed_is_target: Option<String> = connection
        .query_row(
            "SELECT observed_model_id FROM resolved_model_aliases
             WHERE canonical_model_id=?1 AND observed_model_id<>?2 LIMIT 1",
            params![observed, observed],
            |row| row.get(0),
        )
        .optional()?;
    if observed_is_target.is_some() {
        return Ok(Some(format!(
            "model {observed} is already an alias target; alias chains are not supported"
        )));
    }

    // The sidecar is durable while the SQLite projection is disposable. On a
    // fresh projection, a valid alias may temporarily point at a remote-only
    // model whose price has not been restored yet. Hydration still rejects
    // malformed IDs, self-maps, and chains, while interactive mutations and
    // the completed remote snapshot require a priced target.
    if require_priced_target && !target_exists {
        return Ok(Some(format!(
            "canonical model {canonical} does not have a price"
        )));
    }

    Ok(None)
}

pub(super) fn resolved_alias_layer_validation_message(
    connection: &rusqlite::Connection,
) -> Result<Option<String>> {
    resolved_alias_layer_validation_message_scoped(connection, None)
}

fn resolved_alias_layer_validation_message_for_observed(
    connection: &rusqlite::Connection,
    observed: &str,
) -> Result<Option<String>> {
    resolved_alias_layer_validation_message_scoped(connection, Some(observed))
}

fn resolved_alias_layer_validation_message_scoped(
    connection: &rusqlite::Connection,
    affected_observed: Option<&str>,
) -> Result<Option<String>> {
    let chain: Option<(String, String)> = connection
        .query_row(
            "SELECT a.observed_model_id,a.canonical_model_id
             FROM resolved_model_aliases a
             WHERE (
                    ?1 IS NULL
                    OR a.observed_model_id=?1
                    OR a.canonical_model_id=?1
                 )
               AND (
                    a.observed_model_id=a.canonical_model_id
                    OR EXISTS(
                        SELECT 1 FROM resolved_model_aliases target
                        WHERE target.observed_model_id=a.canonical_model_id
                    )
                 )
             ORDER BY a.observed_model_id
             LIMIT 1",
            [affected_observed],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((observed, canonical)) = chain {
        return Ok(Some(format!(
            "alias {observed} pointing through alias {canonical}; alias chains are not supported"
        )));
    }

    let dangling: Option<(String, String)> = connection
        .query_row(
            "SELECT a.observed_model_id,a.canonical_model_id
             FROM resolved_model_aliases a
             WHERE (
                    ?1 IS NULL
                    OR a.observed_model_id=?1
                    OR a.canonical_model_id=?1
                 )
               AND NOT EXISTS(
                    SELECT 1 FROM resolved_model_prices price
                    WHERE price.model_id=a.canonical_model_id
                 )
             ORDER BY a.observed_model_id
             LIMIT 1",
            [affected_observed],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((observed, canonical)) = dangling {
        return Ok(Some(format!(
            "alias {observed} pointing to unpriced model {canonical}"
        )));
    }

    Ok(None)
}

fn validate_price(price: &ManualPrice) -> Result<()> {
    validate_model_id(&price.model_id, "model ID")?;
    if price.input_microusd_per_million < 0
        || price
            .cached_input_microusd_per_million
            .is_some_and(|value| value < 0)
        || price.output_microusd_per_million < 0
    {
        return Err(anyhow!("prices cannot be negative"));
    }
    require_canonical_timestamp(&price.effective_from)?;
    if let Some(value) = &price.effective_to {
        require_canonical_timestamp(value)?;
        if value <= &price.effective_from {
            return Err(anyhow!("effective-to must be after effective-from"));
        }
    }
    Ok(())
}

fn validate_model_id(value: &str, label: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("{label} is required"));
    }
    if value.chars().count() > MAX_MODEL_ID_CHARS {
        return Err(anyhow!(
            "{label} must be at most {MAX_MODEL_ID_CHARS} characters"
        ));
    }
    Ok(())
}

fn require_canonical_timestamp(value: &str) -> Result<()> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid pricing timestamp {value}"))?;
    let canonical = canonical_utc_timestamp(parsed.with_timezone(&Utc));
    if value != canonical {
        return Err(anyhow!("pricing timestamp is not canonical UTC: {value}"));
    }
    Ok(())
}

fn insert_price(connection: &rusqlite::Connection, price: &ManualPrice) -> Result<()> {
    connection.execute(
        "INSERT INTO model_prices(
            model_id,effective_from,effective_to,input_microusd_per_million,
            cached_input_microusd_per_million,output_microusd_per_million,currency,source
         ) VALUES(?1,?2,?3,?4,?5,?6,'USD','manual')
         ON CONFLICT(model_id,effective_from,source) DO UPDATE SET
            effective_to=excluded.effective_to,
            input_microusd_per_million=excluded.input_microusd_per_million,
            cached_input_microusd_per_million=excluded.cached_input_microusd_per_million,
            output_microusd_per_million=excluded.output_microusd_per_million,
            currency='USD'",
        params![
            price.model_id,
            price.effective_from,
            price.effective_to,
            price.input_microusd_per_million,
            price.cached_input_microusd_per_million,
            price.output_microusd_per_million,
        ],
    )?;
    Ok(())
}

fn insert_alias(connection: &rusqlite::Connection, alias: &ManualAlias) -> Result<()> {
    connection.execute(
        "INSERT INTO model_aliases(
            observed_model_id,canonical_model_id,created_at,source
         ) VALUES(?1,?2,?3,'manual')
         ON CONFLICT(observed_model_id,source) DO UPDATE SET
            canonical_model_id=excluded.canonical_model_id,
            created_at=excluded.created_at",
        params![
            alias.observed_model_id,
            alias.canonical_model_id,
            Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ],
    )?;
    Ok(())
}

fn sort_config(config: &mut ManualPricingConfig) {
    config.prices.sort_by(|left, right| {
        (&left.model_id, &left.effective_from).cmp(&(&right.model_id, &right.effective_from))
    });
    config
        .aliases
        .sort_by(|left, right| left.observed_model_id.cmp(&right.observed_model_id));
}

fn validation(message: impl Into<String>) -> MutationError {
    MutationError::Validation(message.into())
}

fn storage(message: impl Into<String>) -> MutationError {
    MutationError::Storage(anyhow!(message.into()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DatabaseLocation;

    fn open_with_store(
        db_path: impl AsRef<Path>,
        config_path: impl AsRef<Path>,
    ) -> (Db, ManualPricingStore) {
        let location = DatabaseLocation::prepare(db_path).unwrap();
        let store = ManualPricingStore::new(config_path.as_ref().to_path_buf()).unwrap();
        let db = location.open().unwrap();
        store.hydrate(&db).unwrap();
        (db, store)
    }

    fn open_with_default_store(db_path: impl AsRef<Path>) -> (Db, ManualPricingStore) {
        let location = DatabaseLocation::prepare(db_path).unwrap();
        let store =
            ManualPricingStore::new(location.path().with_extension("pricing.json")).unwrap();
        let db = location.open().unwrap();
        store.hydrate(&db).unwrap();
        (db, store)
    }

    fn price(model_id: &str) -> ManualPrice {
        ManualPrice {
            model_id: model_id.into(),
            effective_from: "1970-01-01T00:00:00.000000000Z".into(),
            effective_to: None,
            input_microusd_per_million: 400_000,
            cached_input_microusd_per_million: Some(100_000),
            output_microusd_per_million: 1_600_000,
        }
    }

    #[test]
    fn manual_config_survives_projection_deletion_as_fixed_point_json() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("usage.db");
        let config_path = temp.path().join("settings/pricing.json");
        let (db, store) = open_with_store(&db_path, &config_path);
        store.save_price(&db, price("manual-model")).unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "observed-model".into(),
                    canonical_model_id: "manual-model".into(),
                },
            )
            .unwrap();
        drop(db);

        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
        assert_eq!(raw["prices"][0]["inputMicrousdPerMillion"], 400_000);
        assert_eq!(
            raw["prices"][0]["effectiveFrom"],
            "1970-01-01T00:00:00.000000000Z"
        );
        assert!(raw["prices"][0]["inputMicrousdPerMillion"].is_i64());

        fs::remove_file(&db_path).unwrap();
        let _ = fs::remove_file(db_path.with_extension("db-wal"));
        let _ = fs::remove_file(db_path.with_extension("db-shm"));
        let (rebuilt, _store) = open_with_store(&db_path, &config_path);
        let connection = rebuilt.connect().unwrap();
        let stored: (i64, String) = connection
            .query_row(
                "SELECT input_microusd_per_million,source FROM model_prices
                 WHERE model_id='manual-model'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let alias: String = connection
            .query_row(
                "SELECT canonical_model_id FROM model_aliases
                 WHERE observed_model_id='observed-model'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, (400_000, "manual".into()));
        assert_eq!(alias, "manual-model");
    }

    #[cfg(unix)]
    #[test]
    fn multiply_linked_pricing_config_is_rejected_before_read_or_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp.path().join("pricing.json");
        let alias = temp.path().join("pricing-alias.json");
        fs::write(
            &primary,
            serde_json::to_vec(&ManualPricingConfig::default()).unwrap(),
        )
        .unwrap();
        let store = ManualPricingStore::new(primary).unwrap();
        fs::hard_link(store.path(), &alias).unwrap();

        let read_error = store.load_unlocked().unwrap_err().to_string();
        assert!(
            read_error.contains("multiply-linked storage file"),
            "{read_error}"
        );
        assert!(read_error.contains("atomic replacement"), "{read_error}");

        let startup_error = ManualPricingStore::new(alias).unwrap_err().to_string();
        assert!(
            startup_error.contains("multiply-linked storage file"),
            "{startup_error}"
        );
    }

    #[test]
    fn default_store_for_nonexistent_database_uses_canonical_parent_identity() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("new/projection.db");
        let location = DatabaseLocation::prepare(&db_path).unwrap();
        let store =
            ManualPricingStore::new(location.path().with_extension("pricing.json")).unwrap();

        assert_eq!(
            store.path(),
            std::fs::canonicalize(temp.path())
                .unwrap()
                .join("new/projection.pricing.json")
        );
        let db = location.open().unwrap();
        assert_eq!(db.path(), std::fs::canonicalize(db_path).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn database_symlink_aliases_produce_one_default_store_identity() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("real.db");
        let alias = temp.path().join("alias.db");
        let direct_location = DatabaseLocation::prepare(&target).unwrap();
        let direct_store =
            ManualPricingStore::new(direct_location.path().with_extension("pricing.json")).unwrap();
        let direct_db = direct_location.open().unwrap();
        symlink(&target, &alias).unwrap();

        let alias_location = DatabaseLocation::prepare(&alias).unwrap();
        let alias_store =
            ManualPricingStore::new(alias_location.path().with_extension("pricing.json")).unwrap();
        let alias_db = alias_location.open().unwrap();

        assert_eq!(alias_db.path(), direct_db.path());
        assert_eq!(alias_store.path(), direct_store.path());
        assert!(
            std::fs::symlink_metadata(alias)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn manual_pricing_rejects_unbounded_model_identifiers() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        let oversized = "x".repeat(MAX_MODEL_ID_CHARS + 1);

        let price_error = store
            .save_price(&db, price(&oversized))
            .unwrap_err()
            .to_string();
        assert!(price_error.contains("at most 256 characters"));

        let alias_error = store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: oversized,
                    canonical_model_id: "gpt-5.5".into(),
                },
            )
            .unwrap_err()
            .to_string();
        assert!(alias_error.contains("at most 256 characters"));
    }

    #[cfg(unix)]
    #[test]
    fn manual_pricing_config_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("usage.db");
        let config_path = temp.path().join("pricing.json");
        let (db, store) = open_with_store(&db_path, &config_path);
        store.save_price(&db, price("private-price")).unwrap();

        let mode = fs::metadata(&config_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn hydration_repairs_permissive_existing_config_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("usage.db");
        let config_path = temp.path().join("pricing.json");
        let store = ManualPricingStore::new(config_path.clone()).unwrap();
        let mut config = ManualPricingConfig::default();
        config.prices.push(price("existing-price"));
        store.write_unlocked(&config).unwrap();
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o644)).unwrap();

        let db = Db::open(&db_path).unwrap();
        store.hydrate(&db).unwrap();

        let mode = fs::metadata(&config_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn hidden_pricing_config_uses_a_single_dot_lock_name() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join(".codex-usage.pricing.json");
        let store = ManualPricingStore::new(config_path).unwrap();

        drop(store.acquire_file_lock().unwrap());

        assert!(temp.path().join(".codex-usage.pricing.json.lock").exists());
        assert!(!temp.path().join("..codex-usage.pricing.json.lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn atomic_pricing_write_updates_symlink_target_without_replacing_alias() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("usage.db");
        let target = temp.path().join("pricing-target.json");
        let alias = temp.path().join("pricing-alias.json");
        fs::write(
            &target,
            serde_json::to_vec_pretty(&ManualPricingConfig::default()).unwrap(),
        )
        .unwrap();
        symlink(&target, &alias).unwrap();

        let (db, store) = open_with_store(&db_path, &alias);
        store.save_price(&db, price("symlink-safe-price")).unwrap();

        assert!(
            fs::symlink_metadata(&alias)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let stored: ManualPricingConfig =
            serde_json::from_slice(&fs::read(&target).unwrap()).unwrap();
        assert!(
            stored
                .prices
                .iter()
                .any(|row| row.model_id == "symlink-safe-price")
        );
        assert_eq!(store.path(), std::fs::canonicalize(&target).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn advisory_lock_serializes_independent_processes() {
        use std::{process::Command, thread, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("usage.db");
        let config_path = temp.path().join("pricing.json");
        let ready_path = temp.path().join("child-ready");
        let acquired_path = temp.path().join("child-acquired");
        let (db, _owner_store) = open_with_store(&db_path, &config_path);
        let store = ManualPricingStore::new(config_path.clone()).unwrap();
        let guard = store.acquire_file_lock().unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("pricing::manual_store::tests::pricing_lock_child")
            .arg("--nocapture")
            .env("CODEX_USAGE_LOCK_CHILD_DB", &db_path)
            .env("CODEX_USAGE_LOCK_CHILD_CONFIG", &config_path)
            .env("CODEX_USAGE_LOCK_CHILD_READY", &ready_path)
            .env("CODEX_USAGE_LOCK_CHILD_ACQUIRED", &acquired_path)
            .spawn()
            .unwrap();

        for _ in 0..500 {
            if ready_path.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready_path.exists(), "child did not reach the file lock");
        thread::sleep(Duration::from_millis(100));
        assert!(
            !acquired_path.exists(),
            "child acquired an advisory lock held by another process"
        );

        drop(guard);
        let status = child.wait().unwrap();
        assert!(status.success());
        assert!(acquired_path.exists());
        let stored: bool = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM model_prices
                    WHERE model_id='child-process-price' AND source='manual'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(stored);
    }

    #[cfg(unix)]
    #[test]
    fn advisory_lock_wait_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pricing.lock");
        let first = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let second = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        lock_file(&first, Duration::from_secs(1)).unwrap();

        let started = Instant::now();
        let error = lock_file(&second, Duration::from_millis(75)).unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert!(started.elapsed() >= Duration::from_millis(75));
        assert!(started.elapsed() < Duration::from_secs(2));

        unlock_file(&first);
        lock_file(&second, Duration::from_millis(75)).unwrap();
        unlock_file(&second);
    }

    #[cfg(unix)]
    #[test]
    fn pricing_lock_child() {
        let Ok(db_path) = std::env::var("CODEX_USAGE_LOCK_CHILD_DB") else {
            return;
        };
        let config_path = PathBuf::from(std::env::var("CODEX_USAGE_LOCK_CHILD_CONFIG").unwrap());
        let ready_path = PathBuf::from(std::env::var("CODEX_USAGE_LOCK_CHILD_READY").unwrap());
        let acquired_path =
            PathBuf::from(std::env::var("CODEX_USAGE_LOCK_CHILD_ACQUIRED").unwrap());
        let store = ManualPricingStore::new(config_path.clone()).unwrap();
        fs::write(&ready_path, b"ready").unwrap();
        let guard = store.acquire_file_lock().unwrap();
        fs::write(&acquired_path, b"acquired").unwrap();
        drop(guard);

        let db = Db::open(db_path).unwrap();
        store.hydrate(&db).unwrap();
        store.save_price(&db, price("child-process-price")).unwrap();
    }

    #[test]
    fn failed_database_commit_restores_previous_manual_config() {
        let temp = tempfile::tempdir().unwrap();
        let store = ManualPricingStore::new(temp.path().join("pricing.json")).unwrap();
        let mut previous = ManualPricingConfig::default();
        previous.prices.push(price("previous"));
        store.write_unlocked(&previous).unwrap();
        let mut next = previous.clone();
        next.prices.push(price("next"));
        sort_config(&mut next);

        let result =
            store.write_then_commit(&previous, &next, || Err(anyhow!("forced commit failure")));

        assert!(matches!(result, Err(MutationError::Storage(_))));
        assert_eq!(store.load_unlocked().unwrap(), previous);
    }

    #[test]
    fn database_only_manual_rows_are_not_exported_or_preserved() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("usage.db");
        let (db, store) = open_with_default_store(&path);
        db.connect()
            .unwrap()
            .execute(
                "INSERT OR REPLACE INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES('legacy-manual','1970-01-01T00:00:00.000000000Z',1,2,'USD','manual')",
                [],
            )
            .unwrap();
        drop(db);

        let reopened = Db::open(&path).unwrap();
        store.hydrate(&reopened).unwrap();
        let count: i64 = reopened
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM model_prices WHERE model_id='legacy-manual'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        assert!(!path.with_extension("pricing.json").exists());
    }

    #[test]
    fn alias_validation_rejects_chains_in_both_insertion_orders() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        store.save_price(&db, price("canonical")).unwrap();
        store.save_price(&db, price("middle")).unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "first".into(),
                    canonical_model_id: "middle".into(),
                },
            )
            .unwrap();
        let reverse = store.save_alias(
            &db,
            ManualAlias {
                observed_model_id: "middle".into(),
                canonical_model_id: "canonical".into(),
            },
        );
        assert!(matches!(reverse, Err(MutationError::Validation(_))));

        store.delete_alias(&db, "first").unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "middle".into(),
                    canonical_model_id: "canonical".into(),
                },
            )
            .unwrap();
        let forward = store.save_alias(
            &db,
            ManualAlias {
                observed_model_id: "first".into(),
                canonical_model_id: "middle".into(),
            },
        );
        assert!(matches!(forward, Err(MutationError::Validation(_))));
    }

    #[test]
    fn hydration_rejects_alias_chains_through_lower_layers() {
        fn add_lower_alias(db: &Db, observed: &str, canonical: &str) {
            db.connect()
                .unwrap()
                .execute(
                    "INSERT INTO model_aliases(
                        observed_model_id,canonical_model_id,created_at,source
                     ) VALUES(?1,?2,CURRENT_TIMESTAMP,'remote:test')",
                    params![observed, canonical],
                )
                .unwrap();
        }

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let config_path = temp.path().join("manual-pricing.json");
        let store = ManualPricingStore::new(config_path).unwrap();

        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES(
                    'middle','1970-01-01T00:00:00.000000000Z',1,2,'USD','remote:test'
                 )",
                [],
            )
            .unwrap();
        add_lower_alias(&db, "middle", "gpt-5.5");
        let mut targets_lower_alias = ManualPricingConfig::default();
        targets_lower_alias.aliases.push(ManualAlias {
            observed_model_id: "top".into(),
            canonical_model_id: "middle".into(),
        });
        store.write_unlocked(&targets_lower_alias).unwrap();
        let error = store.hydrate(&db).unwrap_err().to_string();
        assert!(error.contains("itself an alias"), "{error}");

        db.connect()
            .unwrap()
            .execute("DELETE FROM model_aliases WHERE source='remote:test'", [])
            .unwrap();
        add_lower_alias(&db, "bottom", "middle");
        let mut becomes_lower_target = ManualPricingConfig::default();
        becomes_lower_target.aliases.push(ManualAlias {
            observed_model_id: "middle".into(),
            canonical_model_id: "gpt-5.5".into(),
        });
        store.write_unlocked(&becomes_lower_target).unwrap();
        let error = store.hydrate(&db).unwrap_err().to_string();
        assert!(error.contains("already an alias target"), "{error}");

        let manual_aliases: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM model_aliases WHERE source='manual'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(manual_aliases, 0);
    }

    #[test]
    fn hydration_validates_the_final_layered_alias_graph() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES('lower','middle',CURRENT_TIMESTAMP,'remote:test')",
                [],
            )
            .unwrap();
        let store = ManualPricingStore::new(temp.path().join("manual-pricing.json")).unwrap();
        let mut config = ManualPricingConfig::default();
        config.aliases.extend([
            ManualAlias {
                observed_model_id: "lower".into(),
                canonical_model_id: "gpt-5.4".into(),
            },
            ManualAlias {
                observed_model_id: "middle".into(),
                canonical_model_id: "gpt-5.5".into(),
            },
        ]);
        store.write_unlocked(&config).unwrap();

        store.hydrate(&db).unwrap();

        let resolved = db
            .connect()
            .unwrap()
            .prepare(
                "SELECT observed_model_id,canonical_model_id
                 FROM resolved_model_aliases
                 WHERE observed_model_id IN ('lower','middle')
                 ORDER BY observed_model_id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            resolved,
            [
                ("lower".to_string(), "gpt-5.4".to_string()),
                ("middle".to_string(), "gpt-5.5".to_string()),
            ]
        );
    }

    #[test]
    fn rebuilt_price_table_rejects_non_usd_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let error = db
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES('eur','1970-01-01T00:00:00.000000000Z',1,2,'EUR','manual')",
                [],
            )
            .unwrap_err();
        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[test]
    fn deleting_manual_override_restores_bundled_fallback_immediately() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        let mut override_price = price("gpt-5.5");
        override_price.input_microusd_per_million = 99_000_000;
        store.save_price(&db, override_price).unwrap();
        store
            .delete_price(&db, "gpt-5.5", Some("1970-01-01T00:00:00.000000000Z"))
            .unwrap();
        let restored: (i64, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT input_microusd_per_million,source FROM model_prices
                 WHERE model_id='gpt-5.5'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(restored, (5_000_000, "bundled-baseline".into()));
    }

    #[test]
    fn deleting_manual_override_reveals_remote_layer_immediately() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES(
                    'layered-model','1970-01-01T00:00:00.000000000Z',
                    7000000,8000000,'USD','remote:https://example.invalid/prices.json'
                 )",
                [],
            )
            .unwrap();
        let mut override_price = price("layered-model");
        override_price.input_microusd_per_million = 99_000_000;
        store.save_price(&db, override_price).unwrap();
        let connection = db.connect().unwrap();
        let selected: (i64, String) = connection
            .query_row(
                "SELECT input_microusd_per_million,source
                 FROM resolved_model_prices WHERE model_id='layered-model'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let layer_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM model_prices WHERE model_id='layered-model'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(selected, (99_000_000, "manual".into()));
        assert_eq!(layer_count, 2);
        drop(connection);

        store
            .delete_price(&db, "layered-model", Some("1970-01-01T00:00:00.000000000Z"))
            .unwrap();
        let revealed: (i64, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT input_microusd_per_million,source
                 FROM resolved_model_prices WHERE model_id='layered-model'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            revealed,
            (
                7_000_000,
                "remote:https://example.invalid/prices.json".into()
            )
        );
    }

    #[test]
    fn deleting_manual_alias_reveals_bundled_alias_immediately() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES('layered-alias','gpt-5.5',CURRENT_TIMESTAMP,'bundled-baseline')",
                [],
            )
            .unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "layered-alias".into(),
                    canonical_model_id: "gpt-5.4".into(),
                },
            )
            .unwrap();
        let selected: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT canonical_model_id FROM resolved_model_aliases
                 WHERE observed_model_id='layered-alias'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(selected, "gpt-5.4");

        store.delete_alias(&db, "layered-alias").unwrap();
        let revealed: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT canonical_model_id FROM resolved_model_aliases
                 WHERE observed_model_id='layered-alias'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revealed, "gpt-5.5");
    }

    #[test]
    fn fixed_width_effective_timestamp_prices_usage_at_the_exact_instant() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        let instant = "2026-07-18T12:34:56.000000000Z";
        let mut exact = price("exact-model");
        exact.effective_from = instant.into();
        exact.input_microusd_per_million = 1_000_000;
        store.save_price(&db, exact).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(&format!(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','{instant}','{instant}');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                    VALUES('rollout','thread','{instant}','{instant}');
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES(
                    'usage','thread','rollout','{instant}',1,'exact-model',1,0,0,0,1
                 );"
            ))
            .unwrap();
        let priced: (i64, i64) = connection
            .query_row(
                "SELECT price_known,cost_microusd FROM priced_usage WHERE id='usage'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(priced, (1, 1));
    }

    #[test]
    fn deleting_last_price_cannot_leave_a_dangling_alias() {
        let temp = tempfile::tempdir().unwrap();
        let (db, store) = open_with_default_store(temp.path().join("usage.db"));
        store.save_price(&db, price("manual-target")).unwrap();
        store
            .save_alias(
                &db,
                ManualAlias {
                    observed_model_id: "observed".into(),
                    canonical_model_id: "manual-target".into(),
                },
            )
            .unwrap();
        let result =
            store.delete_price(&db, "manual-target", Some("1970-01-01T00:00:00.000000000Z"));
        assert!(matches!(result, Err(MutationError::Validation(_))));
        let remaining: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM model_prices WHERE model_id='manual-target'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 1);
    }
}
