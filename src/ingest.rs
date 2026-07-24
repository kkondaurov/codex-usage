use crate::{
    MAX_PUBLIC_YEAR, MAX_USAGE_TOKENS_PER_FACT, MIN_PUBLIC_YEAR,
    db::Db,
    model::TokenUsage,
    process_lock::DatabaseLock,
    redaction::{redact_data_urls, serialize_redacted_json},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    fs::{File, Metadata},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use walkdir::WalkDir;

const MODEL_ATTRIBUTION_REQUIRED_FROM_YEAR: i32 = 2026;
const FINGERPRINT_CHUNK_BYTES: u64 = 1024 * 1024;
const FINGERPRINT_AUDIT_BYTES_PER_SCAN: u64 = 8 * FINGERPRINT_CHUNK_BYTES;
const FINGERPRINT_AUDIT_FILES_PER_SCAN: usize = 8;
const FINGERPRINT_AUDIT_BYTES_PER_FILE: u64 =
    FINGERPRINT_AUDIT_BYTES_PER_SCAN / FINGERPRINT_AUDIT_FILES_PER_SCAN as u64;
const FINGERPRINT_AUDIT_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
const CHUNKED_FINGERPRINT_PREFIX: &str = "chunked-sha256-v1:";
// Bound individual source records so a missing newline cannot grow memory
// without limit. Large payloads are parsed for metadata but are never retained
// in the SQLite projection.
const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;
const UNKNOWN_METADATA_STRING_CHARS: usize = 256;
const PROJECTED_EVENT_LABEL_CHARS: usize = 512;
const PROJECTED_EVENT_BODY_CHARS: usize = 16 * 1024;
const PROJECTED_IDENTIFIER_CHARS: usize = 256;
const PROJECTED_SESSION_PATH_CHARS: usize = 4 * 1024;
const PROJECTED_SESSION_TITLE_CHARS: usize = PROJECTED_EVENT_BODY_CHARS;
const PROJECTOR_GENERATION: u64 = 1;
const PROJECTOR_GENERATION_KEY: &str = "projector_generation";
// Individual turns and tool calls can legitimately run for hours, but a
// single projected activity lasting longer than 30 days is corrupt metadata.
// Bounding each stored interval also keeps aggregate SQLite integer sums far
// away from overflow for any realistic local corpus.
const MAX_STORED_DURATION_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedLine {
    Eof,
    Complete { len: u64, oversized: bool },
    Incomplete { len: u64, oversized: bool },
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    limit: usize,
) -> io::Result<BoundedLine> {
    buffer.clear();
    let mut len = 0_u64;
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(if len == 0 {
                BoundedLine::Eof
            } else {
                BoundedLine::Incomplete { len, oversized }
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if !oversized {
            if buffer.len().saturating_add(take) <= limit {
                buffer.extend_from_slice(&available[..take]);
            } else {
                oversized = true;
                buffer.clear();
            }
        }
        reader.consume(take);
        len = len.saturating_add(take as u64);
        if newline.is_some() {
            return Ok(BoundedLine::Complete { len, oversized });
        }
    }
}

#[cfg(test)]
type ProcessFileAfterSnapshotHook = Box<dyn FnOnce(&Path)>;

#[cfg(test)]
type ProcessFileAfterTransactionReadHook = Box<dyn FnOnce()>;

#[cfg(test)]
type ProcessFileBeforeOpenHook = Box<dyn FnOnce(&Path)>;

#[cfg(test)]
type ScanAfterStartHook = Box<dyn FnOnce(&Db) -> Result<()>>;

#[cfg(test)]
thread_local! {
    static PROCESS_FILE_BEFORE_OPEN_HOOK: std::cell::RefCell<
        Option<ProcessFileBeforeOpenHook>,
    > = std::cell::RefCell::new(None);
    static PROCESS_FILE_AFTER_SNAPSHOT_HOOK: std::cell::RefCell<
        Option<ProcessFileAfterSnapshotHook>,
    > = std::cell::RefCell::new(None);
    static PROCESS_FILE_AFTER_TRANSACTION_READ_HOOK: std::cell::RefCell<
        Option<ProcessFileAfterTransactionReadHook>,
    > = std::cell::RefCell::new(None);
    static SCAN_AFTER_START_HOOK: std::cell::RefCell<Option<ScanAfterStartHook>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
fn set_process_file_before_open_hook(hook: impl FnOnce(&Path) + 'static) {
    PROCESS_FILE_BEFORE_OPEN_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_process_file_before_open_hook(path: &Path) {
    PROCESS_FILE_BEFORE_OPEN_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook(path);
        }
    });
}

#[cfg(test)]
fn set_scan_after_start_hook(hook: impl FnOnce(&Db) -> Result<()> + 'static) {
    SCAN_AFTER_START_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_scan_after_start_hook(db: &Db) -> Result<()> {
    SCAN_AFTER_START_HOOK.with(|slot| {
        let hook = slot.borrow_mut().take();
        match hook {
            Some(hook) => hook(db),
            None => Ok(()),
        }
    })
}

#[cfg(test)]
fn set_process_file_after_snapshot_hook(hook: impl FnOnce(&Path) + 'static) {
    PROCESS_FILE_AFTER_SNAPSHOT_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_process_file_after_snapshot_hook(path: &Path) {
    PROCESS_FILE_AFTER_SNAPSHOT_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook(path);
        }
    });
}

#[cfg(test)]
fn set_process_file_after_transaction_read_hook(hook: impl FnOnce() + 'static) {
    PROCESS_FILE_AFTER_TRANSACTION_READ_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_process_file_after_transaction_read_hook() {
    PROCESS_FILE_AFTER_TRANSACTION_READ_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook();
        }
    });
}

#[derive(Clone, Debug)]
pub struct IngestRoots {
    pub active: Option<PathBuf>,
    pub archive: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanReport {
    pub files_seen: u64,
    pub files_ingested: u64,
    pub files_unchanged: u64,
    pub files_failed: u64,
    pub records_read: u64,
    pub inherited_records_skipped: u64,
}

impl ScanReport {
    fn merge(&mut self, other: FileReport) {
        if other.deferred {
            return;
        } else if other.failed {
            self.files_failed += 1;
        } else if other.unchanged {
            self.files_unchanged += 1;
        } else {
            self.files_ingested += 1;
        }
        self.records_read += other.records;
        self.inherited_records_skipped += other.inherited;
    }

    fn merge_scan(&mut self, other: Self) {
        self.files_seen = self.files_seen.saturating_add(other.files_seen);
        self.files_ingested = self.files_ingested.saturating_add(other.files_ingested);
        self.files_unchanged = self.files_unchanged.saturating_add(other.files_unchanged);
        self.files_failed = self.files_failed.saturating_add(other.files_failed);
        self.records_read = self.records_read.saturating_add(other.records_read);
        self.inherited_records_skipped = self
            .inherited_records_skipped
            .saturating_add(other.inherited_records_skipped);
    }
}

#[derive(Debug)]
struct ScanOutcome {
    report: ScanReport,
    root_signature_adopted: bool,
}

#[derive(Debug, Default)]
struct FileReport {
    deferred: bool,
    unchanged: bool,
    failed: bool,
    records: u64,
    inherited: u64,
    error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct CursorState {
    #[serde(default)]
    projector_generation: u64,
    owner_id: String,
    thread_id: String,
    parent_rollout_id: Option<String>,
    parent_thread_id: Option<String>,
    agent_path: Option<String>,
    agent_nickname: Option<String>,
    forked: bool,
    native_started: bool,
    current_turn: Option<String>,
    turn_context_seen: bool,
    current_model: Option<String>,
    current_effort: Option<String>,
    last_timestamp: Option<String>,
    cumulative: TokenUsage,
}

#[derive(Clone, Debug)]
struct SourceCheckpoint {
    archived: bool,
    size: u64,
    modified_ns: u64,
    identity: FileIdentity,
    fingerprint: String,
    offset: u64,
    line_number: u64,
    inherited_lines: u64,
    last_error: Option<String>,
    state: CursorState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PendingSourceShrink {
    path: String,
    size: u64,
    content_digest: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChunkedFingerprint {
    size: u64,
    chunk_bytes: u64,
    chunks: Vec<String>,
    audit_cursor: usize,
    audit_completed_at: i64,
}

impl ChunkedFingerprint {
    fn parse(value: &str) -> Option<Self> {
        let encoded = value.strip_prefix(CHUNKED_FINGERPRINT_PREFIX)?;
        let fingerprint: Self = serde_json::from_str(encoded).ok()?;
        let expected_chunks = fingerprint.size.div_ceil(FINGERPRINT_CHUNK_BYTES) as usize;
        (fingerprint.chunk_bytes == FINGERPRINT_CHUNK_BYTES
            && fingerprint.chunks.len() == expected_chunks
            && fingerprint
                .chunks
                .iter()
                .all(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
            && fingerprint.audit_cursor <= expected_chunks)
            .then_some(fingerprint)
    }

    fn encode(&self) -> Result<String> {
        Ok(format!(
            "{CHUNKED_FINGERPRINT_PREFIX}{}",
            serde_json::to_string(self)?
        ))
    }

    fn same_content(&self, other: &Self) -> bool {
        self.size == other.size
            && self.chunk_bytes == other.chunk_bytes
            && self.chunks == other.chunks
    }

    fn audit_due(&self, now: i64) -> bool {
        now.saturating_sub(self.audit_completed_at) >= FINGERPRINT_AUDIT_INTERVAL_SECONDS
    }
}

#[derive(Debug)]
struct FingerprintAuditBudget {
    bytes_remaining: u64,
    files_remaining: usize,
}

impl Default for FingerprintAuditBudget {
    fn default() -> Self {
        Self {
            bytes_remaining: FINGERPRINT_AUDIT_BYTES_PER_SCAN,
            files_remaining: FINGERPRINT_AUDIT_FILES_PER_SCAN,
        }
    }
}

struct FullFingerprint {
    current: ChunkedFingerprint,
    prefix: Option<ChunkedFingerprint>,
    legacy_current: String,
    legacy_prefix: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct FileIdentity {
    ctime_ns: Option<i64>,
    device_id: Option<i64>,
    inode: Option<i64>,
}

impl FileIdentity {
    fn is_complete(self) -> bool {
        self.ctime_ns.is_some() && self.device_id.is_some() && self.inode.is_some()
    }

    fn same_file(self, other: Self) -> bool {
        self.is_complete()
            && other.is_complete()
            && self.device_id == other.device_id
            && self.inode == other.inode
    }
}

#[derive(Clone, Debug)]
struct OwnerMeta {
    owner_id: String,
    thread_id: String,
    parent_rollout_id: Option<String>,
    parent_thread_id: Option<String>,
    agent_path: Option<String>,
    agent_nickname: Option<String>,
    is_subagent: bool,
    forked: bool,
    timestamp: String,
    cwd: Option<String>,
    project: Option<String>,
    repository_url: Option<String>,
    branch: Option<String>,
    source: Option<String>,
    thread_source: Option<String>,
    source_json: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct SessionMetadata {
    cwd: Option<String>,
    project: Option<String>,
    repository_url: Option<String>,
    branch: Option<String>,
    source: Option<String>,
    thread_source: Option<String>,
}

#[derive(Clone, Debug)]
struct SourceCandidate {
    path: PathBuf,
    archived: bool,
    size: u64,
    complete: bool,
    owner: OwnerMeta,
}

pub fn scan_once(db: &Db, roots: &IngestRoots) -> Result<ScanReport> {
    let _scan_guard = DatabaseLock::acquire(db, "ingest")?;
    Ok(scan_once_locked(db, roots)?.report)
}

/// Exclusive ownership for one projection writer configuration.
///
/// A one-shot command retains it across recovery, pricing synchronization, and
/// projection. A server retains it from recovery through prewarming and then
/// transfers it into the background scanner. Both paths therefore discover a
/// competing root owner before mutating shared state.
#[derive(Debug)]
pub struct IngestScannerLease {
    database_path: PathBuf,
    _scanner_lease: DatabaseLock,
}

impl IngestScannerLease {
    pub fn acquire(db: &Db) -> Result<Self> {
        Self::acquire_path(db.path())
    }

    /// Claim projection ownership before opening SQLite.
    ///
    /// `Db::open` performs migrations, seed writes, and manual-pricing
    /// hydration, so command entrypoints that intend to ingest must establish
    /// their exclusive writer identity from the canonical storage path first.
    pub fn acquire_path(database_path: impl AsRef<Path>) -> Result<Self> {
        let database_path = crate::db::canonicalize_storage_path(database_path.as_ref())?;
        let cancelled = AtomicBool::new(false);
        let scanner_lease = DatabaseLock::acquire_path_interruptible(
            &database_path,
            "ingest-scanner",
            Duration::ZERO,
            &cancelled,
        )
        .with_context(|| {
            format!(
                "failed to claim ingest ownership for {}; a live ingest scanner already owns this projection",
                database_path.display()
            )
        })?
        .ok_or_else(|| anyhow!("ingest ownership acquisition was cancelled"))?;
        Ok(Self {
            database_path,
            _scanner_lease: scanner_lease,
        })
    }

    fn require_database(&self, db: &Db) -> Result<()> {
        if self.database_path != db.path() {
            return Err(anyhow!(
                "ingest scanner lease for {} cannot operate on {}",
                self.database_path.display(),
                db.path().display()
            ));
        }
        Ok(())
    }
}

/// Run the bounded scan sequence used by the one-shot CLI command.
///
/// `scan_once` deliberately requires two clean observations before removing
/// projections that disappeared after a configured-root change. A long-lived
/// server naturally supplies the confirmation scan; a one-shot command must
/// do so itself or it can exit successfully with both source sets projected.
pub fn scan_one_shot(db: &Db, roots: &IngestRoots) -> Result<ScanReport> {
    let lease = IngestScannerLease::acquire(db)?;
    scan_one_shot_with_lease(db, roots, &lease)
}

/// Run the bounded one-shot projection while the caller retains command-level
/// scanner exclusion across its surrounding recovery and pricing work.
pub fn scan_one_shot_with_lease(
    db: &Db,
    roots: &IngestRoots,
    lease: &IngestScannerLease,
) -> Result<ScanReport> {
    scan_one_shot_with_lease_and_between_pass(db, roots, lease, || {})
}

#[cfg(test)]
fn scan_one_shot_with_between_pass<F>(
    db: &Db,
    roots: &IngestRoots,
    between_passes: F,
) -> Result<ScanReport>
where
    F: FnOnce(),
{
    let lease = IngestScannerLease::acquire(db)?;
    scan_one_shot_with_lease_and_between_pass(db, roots, &lease, between_passes)
}

fn scan_one_shot_with_lease_and_between_pass<F>(
    db: &Db,
    roots: &IngestRoots,
    lease: &IngestScannerLease,
    between_passes: F,
) -> Result<ScanReport>
where
    F: FnOnce(),
{
    lease.require_database(db)?;
    // Root adoption and its confirming reconciliation are one process-level
    // decision. Releasing the lock between them would let a scanner using a
    // different root set replace the signature and leave both projections
    // present after this command reports success.
    let _scan_guard = DatabaseLock::acquire(db, "ingest")?;
    let first = scan_once_locked(db, roots)?;
    let mut report = first.report;
    if first.root_signature_adopted {
        between_passes();
        match scan_once_locked(db, roots) {
            Ok(confirmation) => report.merge_scan(confirmation.report),
            Err(error) => {
                finalize_scan_sequence_error(db, &report, &error);
                return Err(error);
            }
        }
    }
    if let Err(error) = advance_projector_generation(db) {
        finalize_scan_sequence_error(db, &report, &error);
        return Err(error);
    }
    Ok(report)
}

/// Whether every durable source checkpoint was produced by this projector and
/// the last complete bounded scan published that generation globally.
///
/// A genuinely empty projection is vacuously current. This preserves the
/// useful `serve --no-ingest` contract for isolated empty databases while any
/// nonempty legacy projection still requires a synchronous one-shot replay.
pub fn projector_generation_is_current(db: &Db) -> Result<bool> {
    let connection = db.connect()?;
    let generation = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key=?1",
            [PROJECTOR_GENERATION_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|value| value.parse::<u64>().ok());
    let has_sources: bool =
        connection.query_row("SELECT EXISTS(SELECT 1 FROM source_files)", [], |row| {
            row.get(0)
        })?;
    let has_threads: bool =
        connection.query_row("SELECT EXISTS(SELECT 1 FROM threads)", [], |row| row.get(0))?;
    if !has_sources && !has_threads {
        return Ok(true);
    }
    if generation != Some(PROJECTOR_GENERATION) {
        return Ok(false);
    }
    Ok(!has_stale_projector_checkpoints(&connection)?)
}

fn has_stale_projector_checkpoints(connection: &Connection) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM source_files
                WHERE COALESCE(
                    CASE WHEN json_valid(parse_state_json)
                         THEN CAST(json_extract(
                             parse_state_json,'$.projector_generation'
                         ) AS INTEGER)
                    END,
                    0
                )<>?1
             )",
            [PROJECTOR_GENERATION as i64],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn advance_projector_generation(db: &Db) -> Result<()> {
    let mut connection = db.connect()?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if has_stale_projector_checkpoints(&transaction)? {
        return Err(anyhow!(
            "projector generation {PROJECTOR_GENERATION} remains incomplete; stale source checkpoints still require replay"
        ));
    }
    transaction.execute(
        "INSERT INTO app_meta(key,value) VALUES(?1,?2)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![PROJECTOR_GENERATION_KEY, PROJECTOR_GENERATION.to_string()],
    )?;
    transaction.commit()?;
    Ok(())
}

fn scan_once_locked(db: &Db, roots: &IngestRoots) -> Result<ScanOutcome> {
    // The caller owns the process lock across this complete application-level
    // reconciliation decision even though source files commit independently.
    set_meta(db, "ingest_state", "scanning")?;
    let attempt = scan_once_started(db, roots);
    if let Err(error) = &attempt {
        finalize_unexpected_scan_error(db, error);
    }
    attempt
}

fn scan_once_started(db: &Db, roots: &IngestRoots) -> Result<ScanOutcome> {
    #[cfg(test)]
    run_scan_after_start_hook(db)?;

    let mut report = ScanReport::default();
    let mut files = Vec::new();
    let mut observed = HashSet::new();
    let mut pending_empty = HashSet::new();
    let mut failures = Vec::new();
    let mut enumerated_roots = Vec::new();
    let mut incomplete_roots = Vec::new();
    if roots.active.is_none() && roots.archive.is_none() {
        report.files_failed += 1;
        failures.push("no ingest roots are configured".to_owned());
    }
    if let Some(root) = &roots.active {
        match collect_jsonl(root, false, &mut files, &mut observed, &mut pending_empty) {
            Ok(()) => enumerated_roots.push(root.clone()),
            Err(error) => {
                incomplete_roots.push(root.clone());
                report.files_failed += 1;
                failures.push(error.to_string());
                tracing::warn!(root=%root.display(),%error,"configured ingest root failed");
            }
        }
    }
    if let Some(root) = &roots.archive {
        match collect_jsonl(root, true, &mut files, &mut observed, &mut pending_empty) {
            Ok(()) => enumerated_roots.push(root.clone()),
            Err(error) => {
                incomplete_roots.push(root.clone());
                report.files_failed += 1;
                failures.push(error.to_string());
                tracing::warn!(root=%root.display(),%error,"configured ingest root failed");
            }
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    report.files_seen = files.len() as u64;
    let selected_source_extents = load_selected_source_extents(db)?;
    let source_handoffs = SourceHandoffIndex::new(&selected_source_extents);
    let mut protected_handoff_owners = HashSet::new();
    let mut candidates_by_owner: HashMap<String, Vec<SourceCandidate>> = HashMap::new();
    let mut owners = HashMap::new();
    for (path, archived) in files {
        let size = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        match peek_owner(&path) {
            Ok(owner) => {
                let complete = source_is_complete(&path, size);
                candidates_by_owner
                    .entry(owner.owner_id.clone())
                    .or_default()
                    .push(SourceCandidate {
                        path,
                        archived,
                        size,
                        complete,
                        owner,
                    });
            }
            Err(error) => {
                // A writer may publish a destination path before its first
                // owner record is complete. A correlated destination protects
                // the last committed projection regardless of parse status;
                // only an actually incomplete file is pending, while a
                // newline-terminated malformed file remains a real failure.
                if let Some(owner_id) = source_handoffs.matching_owner(&path)
                    && selected_source_extents
                        .get(owner_id)
                        .is_some_and(|extent| path != extent.path)
                {
                    protected_handoff_owners.insert(owner_id.to_owned());
                }
                if !source_is_complete(&path, size) {
                    tracing::debug!(
                        path = %path.display(),
                        candidate_size = size,
                        "deferring incomplete source until its owner record is complete"
                    );
                    continue;
                }
                report.files_failed += 1;
                failures.push(format!("{}: {error}", path.display()));
                tracing::warn!(path = %path.display(), %error, "failed to ingest rollout");
            }
        }
    }
    let mut pending_owners = owners_with_pending_empty_sources(
        &pending_empty,
        &selected_source_extents,
        &source_handoffs,
    );
    pending_owners
        .protect_reconciliation
        .extend(protected_handoff_owners);
    let mut selected = Vec::new();
    for (owner_id, candidates) in candidates_by_owner {
        if pending_owners.defer_selection.contains(&owner_id) {
            continue;
        }
        let selected_extent = selected_source_extents.get(&owner_id);
        let mut pending_archive_handoff = false;
        let mut ready_candidates = Vec::new();
        for candidate in candidates {
            let ready = selected_extent.is_none_or(|extent| {
                candidate.path == extent.path || source_path_switch_is_ready(&candidate, extent)
            });
            if ready {
                ready_candidates.push(candidate);
            } else {
                pending_archive_handoff = true;
                tracing::debug!(
                    owner_id,
                    path = %candidate.path.display(),
                    candidate_size = candidate.size,
                    previous_committed_size = selected_extent
                        .map_or(0, |extent| extent.committed_size),
                    "deferring source handoff until the prior byte extent is continuous"
                );
            }
        }
        let candidate = ready_candidates
            .into_iter()
            .max_by(source_candidate_preference);
        if pending_archive_handoff {
            pending_owners
                .protect_reconciliation
                .insert(owner_id.clone());
        }
        if let Some(candidate) = candidate {
            selected.push(candidate);
        }
    }
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    for candidate in &selected {
        owners.insert(candidate.owner.owner_id.clone(), candidate.owner.clone());
    }
    resolve_owner_topology(db, &mut owners)?;
    let mut audit_budget = FingerprintAuditBudget::default();
    for candidate in selected {
        let owner_id = candidate.owner.owner_id.clone();
        let Some(owner) = owners.get(&owner_id) else {
            report.files_failed += 1;
            tracing::warn!(path = %candidate.path.display(), "failed to resolve rollout owner");
            continue;
        };
        match process_file(
            db,
            &candidate.path,
            candidate.archived,
            owner,
            selected_source_extents.get(&owner_id),
            &mut audit_budget,
        ) {
            Ok(file_report) => {
                if file_report.deferred {
                    pending_owners
                        .protect_reconciliation
                        .insert(owner_id.clone());
                }
                if let Some(error) = &file_report.error {
                    failures.push(format!("{}: {error}", candidate.path.display()));
                }
                report.merge(file_report);
            }
            Err(error) => {
                if selected_source_extents.contains_key(&owner_id) {
                    pending_owners.protect_reconciliation.insert(owner_id);
                }
                report.files_failed += 1;
                failures.push(format!("{}: {error:#}", candidate.path.display()));
                tracing::warn!(path = %candidate.path.display(), %error, "failed to ingest rollout");
            }
        }
    }
    let root_signature = format!(
        "{}|{}",
        roots
            .active
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        roots
            .archive
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
    );
    let previous_signature = db
        .connect()?
        .query_row(
            "SELECT value FROM app_meta WHERE key='ingest_root_signature'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let mut root_signature_adopted = false;
    if previous_signature.as_deref() == Some(root_signature.as_str()) {
        // Reconciliation depends on enumeration completeness, not projection
        // success. One malformed file must not keep deleted rollouts alive in
        // another root that was enumerated successfully, while any root whose
        // traversal failed remains untouched.
        reconcile_missing(
            db,
            &observed,
            &pending_owners.protect_reconciliation,
            &enumerated_roots,
            &incomplete_roots,
        )?;
    } else if report.files_failed == 0 {
        // A root change may intentionally expose a different source set.
        // Adopt it after one clean scan, then reconcile only if the next
        // clean scan confirms the same configuration.
        set_meta(db, "ingest_root_signature", &root_signature)?;
        root_signature_adopted = true;
    }
    sync_session_index_titles(db, roots)?;
    let now = canonical_utc(Utc::now());
    let report_json = serde_json::to_string(&report)?;
    if report.files_failed == 0 {
        finish_scan_meta(db, &now, &report_json, None)?;
        Ok(ScanOutcome {
            report,
            root_signature_adopted,
        })
    } else {
        let detail = if failures.is_empty() {
            format!("{} ingest source(s) failed", report.files_failed)
        } else {
            failures.join("; ")
        };
        finish_scan_meta(db, &now, &report_json, Some(&detail))?;
        Err(anyhow!("ingest scan failed: {detail}"))
    }
}

fn finalize_unexpected_scan_error(db: &Db, original_error: &anyhow::Error) {
    let still_scanning = match db.connect().and_then(|connection| {
        connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='ingest_state'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
    }) {
        Ok(Some(state)) => state == "scanning",
        Ok(None) => true,
        Err(error) => {
            tracing::warn!(
                %error,
                "failed to inspect ingest state after an unexpected scan error"
            );
            true
        }
    };
    if !still_scanning {
        return;
    }

    let detail = format!("{original_error:#}");
    let now = canonical_utc(Utc::now());
    let report_json =
        serde_json::to_string(&ScanReport::default()).unwrap_or_else(|_| "{}".to_owned());
    if let Err(finalizer_error) = finish_scan_meta(db, &now, &report_json, Some(detail.as_str())) {
        // The triggering failure is the actionable cause. A secondary
        // bookkeeping failure must never replace it at the API/CLI boundary.
        tracing::warn!(
            error = %finalizer_error,
            original_error = %original_error,
            "failed to finalize ingest metadata after an unexpected scan error"
        );
    }
}

fn finalize_scan_sequence_error(
    db: &Db,
    completed_report: &ScanReport,
    original_error: &anyhow::Error,
) {
    let already_finalized = match db.connect().and_then(|connection| {
        connection
            .query_row(
                "SELECT value='error' FROM app_meta WHERE key='ingest_state'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map(|value| value == Some(1))
            .map_err(Into::into)
    }) {
        Ok(already_finalized) => already_finalized,
        Err(error) => {
            tracing::warn!(
                %error,
                "failed to inspect ingest state after a one-shot confirmation error"
            );
            false
        }
    };
    if already_finalized {
        return;
    }

    let detail = format!("{original_error:#}");
    let now = canonical_utc(Utc::now());
    let report_json = serde_json::to_string(completed_report).unwrap_or_else(|_| "{}".to_owned());
    if let Err(finalizer_error) = finish_scan_meta(db, &now, &report_json, Some(detail.as_str())) {
        tracing::warn!(
            error = %finalizer_error,
            original_error = %original_error,
            "failed to finalize a one-shot confirmation error"
        );
    }
}

#[derive(Debug)]
struct IndexedTitle {
    title: String,
    updated_at: String,
    updated_micros: i64,
    line_number: u64,
}

fn sync_session_index_titles(db: &Db, roots: &IngestRoots) -> Result<usize> {
    let Some(index_path) = discover_session_index(roots) else {
        return Ok(0);
    };
    let file = match File::open(&index_path) {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(path=%index_path.display(),%error,"failed to open Codex session title index");
            return Ok(0);
        }
    };
    let mut latest = HashMap::<String, IndexedTitle>::new();
    let mut reader = BufReader::new(file);
    let mut bytes = Vec::new();
    let mut line_number = 0_u64;
    loop {
        let line = match read_bounded_line(&mut reader, &mut bytes, MAX_JSONL_LINE_BYTES) {
            Ok(line) => line,
            Err(error) => {
                tracing::warn!(path=%index_path.display(),%error,"failed to read Codex session title index");
                break;
            }
        };
        match line {
            BoundedLine::Eof | BoundedLine::Incomplete { .. } => break,
            BoundedLine::Complete {
                oversized: true, ..
            } => {
                line_number += 1;
                tracing::warn!(path=%index_path.display(),line_number,"skipping oversized Codex session title index record");
                continue;
            }
            BoundedLine::Complete {
                oversized: false, ..
            } => line_number += 1,
        }
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            // The index is append-only and may be observed between writes.
            // A malformed trailing record must not disturb the prior titles.
            continue;
        };
        let id = match normalized_relational_identifier(
            value.get("id").and_then(Value::as_str),
            "session index thread id",
        ) {
            Ok(Some(id)) => id,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(
                    path = %index_path.display(),
                    line_number,
                    %error,
                    "skipping Codex session title with an invalid thread id"
                );
                continue;
            }
        };
        let Some(title) = value
            .get("thread_name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(updated_at) = value
            .get("updated_at")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Ok(timestamp) = DateTime::parse_from_rfc3339(updated_at) else {
            continue;
        };
        let candidate = IndexedTitle {
            title: redact_and_bound(title.trim(), PROJECTED_SESSION_TITLE_CHARS),
            updated_at: canonical_utc(timestamp.with_timezone(&Utc)),
            updated_micros: timestamp.timestamp_micros(),
            line_number,
        };
        let replace = latest.get(&id).is_none_or(|current| {
            (candidate.updated_micros, candidate.line_number)
                >= (current.updated_micros, current.line_number)
        });
        if replace {
            latest.insert(id, candidate);
        }
    }

    let mut connection = db.connect()?;
    let transaction = connection.transaction()?;
    let mut updated = 0;
    for (id, indexed) in latest {
        updated += transaction.execute(
            "UPDATE threads SET title=?1,title_updated_at=?2
             WHERE id=?3 AND (title IS NULL OR title<>?1 OR title_updated_at IS NULL OR title_updated_at<>?2)",
            params![indexed.title, indexed.updated_at, id],
        )?;
    }
    transaction.commit()?;
    Ok(updated)
}

fn discover_session_index(roots: &IngestRoots) -> Option<PathBuf> {
    session_index_candidates(roots)
        .into_iter()
        .map(|path| path.join("session_index.jsonl"))
        .find(|path| path.is_file())
}

fn session_index_candidates(roots: &IngestRoots) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    [&roots.active, &roots.archive]
        .into_iter()
        .flatten()
        .filter_map(|root| root.parent().map(Path::to_path_buf))
        .filter(|path| seen.insert(path.clone()))
        .collect()
}

fn source_candidate_preference(left: &SourceCandidate, right: &SourceCandidate) -> CmpOrdering {
    left.complete
        .cmp(&right.complete)
        .then_with(|| left.size.cmp(&right.size))
        .then_with(|| (!left.archived).cmp(&(!right.archived)))
        // A lexical minimum is the deterministic winner when every semantic
        // preference is equal, so reverse the final comparison for max_by.
        .then_with(|| right.path.cmp(&left.path))
}

fn source_is_complete(path: &Path, size: u64) -> bool {
    if size == 0 {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    if file.seek(SeekFrom::End(-1)).is_err() {
        return false;
    }
    let mut tail = [0_u8; 1];
    file.read_exact(&mut tail).is_ok() && tail[0] == b'\n'
}

pub struct ScannerHandle {
    cancelled: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ScannerHandle {
    pub fn request_stop(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn shutdown(mut self) {
        self.request_stop();
        self.reap_finished();
    }

    fn reap_finished(&mut self) {
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.is_finished())
            && let Some(worker) = self.worker.take()
        {
            let _ = worker.join();
        }
    }
}

impl Drop for ScannerHandle {
    fn drop(&mut self) {
        self.request_stop();
        // A scan may be inside a large source file or waiting on another
        // process's ingest lock. Never turn server shutdown into an unbounded
        // join; reap only a worker that has already completed and otherwise
        // let the process boundary terminate it after the graceful window.
        self.reap_finished();
    }
}

pub fn spawn_scanner(db: Db, roots: IngestRoots, interval: Duration) -> Result<ScannerHandle> {
    let lease = IngestScannerLease::acquire(&db).with_context(|| {
        format!(
            "failed to claim live ingest scanner ownership for {}",
            db.path().display()
        )
    })?;
    spawn_scanner_with_lease(db, roots, interval, lease)
}

/// Start a live scanner with ownership acquired earlier in startup.
///
/// This closes the handoff gap between synchronous projection recovery and
/// the background worker: no competing one-shot command can claim the
/// database during prewarming or scanner startup.
pub fn spawn_scanner_with_lease(
    db: Db,
    roots: IngestRoots,
    interval: Duration,
    lease: IngestScannerLease,
) -> Result<ScannerHandle> {
    lease.require_database(&db)?;
    // Hold the lifetime lease so another scanner or a one-shot ingest cannot
    // alternate a conflicting root configuration between observations.
    // Every successful live cycle uses the same bounded semantics as the CLI:
    // confirm a newly adopted root set and only then publish the completed
    // projector generation.
    let cancelled = Arc::new(AtomicBool::new(false));
    let stop = cancelled.clone();
    let worker = std::thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            if let Err(error) = scan_one_shot_with_lease(&db, &roots, &lease) {
                tracing::warn!(%error, "ingest scan failed");
                let _ = set_meta(&db, "ingest_state", "error");
            }
            let slices = (interval.as_millis() / 250).max(1) as usize;
            for _ in 0..slices {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    });
    Ok(ScannerHandle {
        cancelled,
        worker: Some(worker),
    })
}

#[cfg(test)]
mod scanner_handle_tests {
    use super::*;
    use std::{
        sync::{Condvar, Mutex, mpsc},
        time::Instant,
    };

    #[test]
    fn scanner_shutdown_requests_cancellation_without_joining_blocked_work() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = cancelled.clone();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let (lock, wake) = &*worker_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            done_tx
                .send(worker_cancelled.load(Ordering::Acquire))
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let handle = ScannerHandle {
            cancelled,
            worker: Some(worker),
        };

        let started = Instant::now();
        handle.shutdown();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "scanner shutdown joined blocked work"
        );

        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        assert!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }
}

fn collect_jsonl(
    root: &Path,
    archived: bool,
    files: &mut Vec<(PathBuf, bool)>,
    observed: &mut HashSet<String>,
    pending_empty: &mut HashSet<String>,
) -> Result<()> {
    let metadata = root
        .metadata()
        .with_context(|| format!("configured ingest root {} is unavailable", root.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "configured ingest root {} is not a directory",
            root.display()
        ));
    }
    std::fs::read_dir(root)
        .with_context(|| format!("configured ingest root {} is unreadable", root.display()))?;
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry.with_context(|| {
            format!("configured ingest root {} traversal failed", root.display())
        })?;
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|value| value == "jsonl")
        {
            let metadata = entry.metadata().with_context(|| {
                format!("source metadata unavailable for {}", entry.path().display())
            })?;
            // An existing empty JSONL is a writer-owned placeholder, not a
            // deletion. Keep it in the reconciliation set while deferring
            // parsing until the writer publishes at least one byte.
            let path_text = entry.path().to_string_lossy().into_owned();
            observed.insert(path_text.clone());
            if metadata.len() > 0 {
                files.push((entry.path().to_path_buf(), archived));
            } else {
                pending_empty.insert(path_text);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct PendingEmptyOwners {
    defer_selection: HashSet<String>,
    protect_reconciliation: HashSet<String>,
}

#[derive(Debug)]
struct SelectedSourceExtent {
    path: PathBuf,
    raw_size: u64,
    committed_size: u64,
    fingerprint: String,
}

#[derive(Debug, Default)]
struct SourceHandoffIndex {
    rollout_ids: HashMap<String, String>,
    unique_file_names: HashMap<String, Option<String>>,
}

impl SourceHandoffIndex {
    fn new(extents: &HashMap<String, SelectedSourceExtent>) -> Self {
        let mut index = Self::default();
        for (owner_id, extent) in extents {
            index
                .rollout_ids
                .insert(owner_id.to_ascii_lowercase(), owner_id.clone());
            let Some(file_name) = source_file_name_key(&extent.path) else {
                continue;
            };
            index
                .unique_file_names
                .entry(file_name)
                .and_modify(|existing| {
                    if existing.as_deref() != Some(owner_id.as_str()) {
                        *existing = None;
                    }
                })
                .or_insert_with(|| Some(owner_id.clone()));
        }
        index
    }

    fn matching_owner<'a>(&'a self, path: &Path) -> Option<&'a str> {
        rollout_id_from_source_path(&path.to_string_lossy())
            .and_then(|owner_id| self.rollout_ids.get(&owner_id.to_ascii_lowercase()))
            .or_else(|| {
                let file_name = source_file_name_key(path)?;
                self.unique_file_names.get(&file_name)?.as_ref()
            })
            .map(String::as_str)
    }
}

fn source_file_name_key(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
}

fn load_selected_source_extents(db: &Db) -> Result<HashMap<String, SelectedSourceExtent>> {
    let connection = db.connect()?;
    let mut statement = connection.prepare(
        "SELECT rollout_id,path,size_bytes,byte_offset,content_fingerprint FROM source_files",
    )?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                SelectedSourceExtent {
                    path: PathBuf::from(row.get::<_, String>(1)?),
                    raw_size: row.get::<_, i64>(2)?.max(0) as u64,
                    committed_size: row.get::<_, i64>(3)?.max(0) as u64,
                    fingerprint: row.get(4)?,
                },
            ))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()
        .map_err(Into::into)
}

fn source_path_switch_is_ready(
    candidate: &SourceCandidate,
    previous: &SelectedSourceExtent,
) -> bool {
    let Ok(mut file) = File::open(&candidate.path) else {
        return false;
    };
    source_path_switch_is_ready_from_file(&mut file, candidate.size, previous)
}

fn source_path_switch_is_ready_from_file(
    file: &mut File,
    size: u64,
    previous: &SelectedSourceExtent,
) -> bool {
    if size < previous.committed_size {
        return false;
    }
    let Ok(candidate_prefix) =
        full_content_fingerprints_from_file(file, previous.committed_size, None)
    else {
        return false;
    };
    if stored_fingerprint_matches(
        &previous.fingerprint,
        &candidate_prefix.current,
        &candidate_prefix.legacy_current,
    ) {
        return true;
    }

    // Databases created before committed-prefix fingerprints stored a hash of
    // the raw source size, including an unfinished tail. If that old source is
    // still present, compare the two committed prefixes directly and upgrade
    // naturally on the next successful ingest. If it has disappeared, remain
    // conservative rather than accepting an unverifiable handoff.
    if previous.raw_size != previous.committed_size && previous.path.is_file() {
        return full_content_fingerprints(&previous.path, previous.committed_size, None).is_ok_and(
            |selected_prefix| {
                selected_prefix
                    .current
                    .same_content(&candidate_prefix.current)
            },
        );
    }
    false
}

fn owners_with_pending_empty_sources(
    pending_empty: &HashSet<String>,
    selected_extents: &HashMap<String, SelectedSourceExtent>,
    source_handoffs: &SourceHandoffIndex,
) -> PendingEmptyOwners {
    if pending_empty.is_empty() {
        return PendingEmptyOwners::default();
    }
    let mut owners = PendingEmptyOwners::default();
    for (owner_id, extent) in selected_extents {
        let exact_path_is_empty = pending_empty
            .iter()
            .any(|path| Path::new(path) == extent.path);
        let correlated_handoff_is_empty = pending_empty.iter().any(|path| {
            Path::new(path) != extent.path
                && source_handoffs.matching_owner(Path::new(path)) == Some(owner_id.as_str())
        });
        if exact_path_is_empty {
            owners.defer_selection.insert(owner_id.clone());
        }
        if exact_path_is_empty || correlated_handoff_is_empty {
            owners.protect_reconciliation.insert(owner_id.clone());
        }
    }
    owners
}

fn rollout_id_from_source_path(path: &str) -> Option<&str> {
    let stem = Path::new(path).file_stem()?.to_str()?;
    let candidate = stem.get(stem.len().checked_sub(36)?..)?;
    looks_like_uuid(candidate).then_some(candidate)
}

fn resolve_owner_topology(db: &Db, owners: &mut HashMap<String, OwnerMeta>) -> Result<()> {
    let connection = db.connect()?;
    let existing = {
        let mut statement = connection.prepare("SELECT id,thread_id FROM rollouts")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<HashMap<_, _>, _>>()?
    };
    let discovered = owners.clone();
    let mut resolved = HashMap::new();
    for owner_id in discovered.keys() {
        let thread_id = resolve_owner_thread(
            owner_id,
            &discovered,
            &existing,
            &mut resolved,
            &mut HashSet::new(),
        );
        if let Some(owner) = owners.get_mut(owner_id) {
            owner.thread_id = thread_id;
        }
    }
    Ok(())
}

fn resolve_owner_thread(
    owner_id: &str,
    discovered: &HashMap<String, OwnerMeta>,
    existing: &HashMap<String, String>,
    resolved: &mut HashMap<String, String>,
    visiting: &mut HashSet<String>,
) -> String {
    if let Some(thread_id) = resolved.get(owner_id) {
        return thread_id.clone();
    }
    let Some(owner) = discovered.get(owner_id) else {
        return existing
            .get(owner_id)
            .cloned()
            .unwrap_or_else(|| owner_id.to_owned());
    };
    if !owner.is_subagent || !visiting.insert(owner_id.to_owned()) {
        return owner.thread_id.clone();
    }
    let anchors = [
        Some(owner.thread_id.as_str()).filter(|value| *value != owner.owner_id),
        owner.parent_rollout_id.as_deref(),
        owner.parent_thread_id.as_deref(),
    ];
    let thread_id = anchors
        .into_iter()
        .flatten()
        .find_map(|anchor| {
            if discovered.contains_key(anchor) {
                Some(resolve_owner_thread(
                    anchor, discovered, existing, resolved, visiting,
                ))
            } else {
                existing
                    .get(anchor)
                    .cloned()
                    .or_else(|| Some(anchor.to_owned()))
            }
        })
        .unwrap_or_else(|| owner.thread_id.clone());
    visiting.remove(owner_id);
    resolved.insert(owner_id.to_owned(), thread_id.clone());
    thread_id
}

fn process_file(
    db: &Db,
    path: &Path,
    archived: bool,
    resolved_owner: &OwnerMeta,
    previous_extent: Option<&SelectedSourceExtent>,
    audit_budget: &mut FingerprintAuditBudget,
) -> Result<FileReport> {
    // Open once, then derive every ownership and content decision from this
    // descriptor. A writer may rename a replacement over `path` after this
    // point, but that replacement belongs to the next scan and cannot be
    // projected under the owner discovered from the previous inode.
    #[cfg(test)]
    run_process_file_before_open_hook(path);
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to stat opened source {}", path.display()))?;
    let size = metadata.len();
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|value| value.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or_default();
    let identity = file_identity(&metadata);
    let mut owner = peek_owner_from_file(&mut file, path)?;
    if owner.owner_id != resolved_owner.owner_id
        || owner.parent_rollout_id != resolved_owner.parent_rollout_id
        || owner.parent_thread_id != resolved_owner.parent_thread_id
        || owner.is_subagent != resolved_owner.is_subagent
    {
        return Err(anyhow!(
            "{} changed ownership between discovery and its opened snapshot",
            path.display()
        ));
    }
    // Topology resolution can follow parents discovered in other files, so it
    // is the sole field intentionally carried over from the scan-wide graph.
    owner.thread_id = resolved_owner.thread_id.clone();
    if let Some(previous) = previous_extent
        && path != previous.path
        && !source_path_switch_is_ready_from_file(&mut file, size, previous)
    {
        tracing::debug!(
            owner_id = resolved_owner.owner_id,
            path = %path.display(),
            candidate_size = size,
            previous_committed_size = previous.committed_size,
            "deferring source handoff because the opened snapshot no longer contains the committed prefix"
        );
        return Ok(FileReport {
            deferred: true,
            ..FileReport::default()
        });
    }
    #[cfg(test)]
    run_process_file_after_snapshot_hook(path);
    let mut connection = db.connect()?;
    let path_text = path.to_string_lossy();
    let checkpoint_by_path = load_checkpoint_by_path(&connection, &path_text)?;
    let suspicious_same_path_shrink = checkpoint_by_path.as_ref().is_some_and(|checkpoint| {
        checkpoint.state.owner_id == owner.owner_id && size < checkpoint.offset
    });
    if !suspicious_same_path_shrink {
        clear_pending_source_shrink(&connection, &owner.owner_id)?;
    }
    let mut audit_mismatch = false;
    // On Unix, ctime cannot be restored by ordinary file-writing APIs. The
    // complete size/mtime/ctime/device/inode tuple therefore makes the common
    // unchanged scan constant-time. Chunk checkpoints are audited on a bounded
    // rolling schedule so the append-only assumption is verified rather than
    // trusted forever.
    if let Some(checkpoint) = checkpoint_by_path.as_ref()
        && checkpoint.state.projector_generation == PROJECTOR_GENERATION
        && checkpoint.state.owner_id == resolved_owner.owner_id
        && checkpoint.size == size
        && checkpoint.modified_ns == modified_ns
        && identity.is_complete()
        && checkpoint.identity == identity
        && checkpoint.state.thread_id == resolved_owner.thread_id
    {
        let fingerprint_extent =
            ChunkedFingerprint::parse(&checkpoint.fingerprint).map(|fingerprint| fingerprint.size);
        if checkpoint.size != checkpoint.offset && fingerprint_extent != Some(checkpoint.offset) {
            // Upgrade raw-extent fingerprints written by older builds while
            // the selected source is still available. This is a one-time read
            // for the rare checkpoint with an unfinished trailing record.
            let fingerprint = fingerprint_for_prefix_from_file(
                &mut file,
                &checkpoint.fingerprint,
                checkpoint.offset,
            )?;
            return mark_file_unchanged(
                &mut connection,
                checkpoint,
                archived,
                size,
                modified_ns,
                identity,
                Some(&fingerprint),
            );
        }
        if let Some(mut fingerprint) = ChunkedFingerprint::parse(&checkpoint.fingerprint)
            && fingerprint.audit_due(Utc::now().timestamp())
        {
            match audit_chunked_fingerprint_from_file(&mut file, &mut fingerprint, audit_budget)? {
                FingerprintAudit::Verified { changed } => {
                    let fingerprint = changed.then(|| fingerprint.encode()).transpose()?;
                    return mark_file_unchanged(
                        &mut connection,
                        checkpoint,
                        archived,
                        size,
                        modified_ns,
                        identity,
                        fingerprint.as_deref(),
                    );
                }
                FingerprintAudit::Mismatch => audit_mismatch = true,
            }
        } else {
            return mark_file_unchanged(
                &mut connection,
                checkpoint,
                archived,
                size,
                modified_ns,
                identity,
                None,
            );
        }
    }
    let existing = load_checkpoint(&connection, &owner.owner_id)?;
    let append_checkpoint = existing.as_ref().filter(|value| {
        value.state.projector_generation == PROJECTOR_GENERATION
            && size > value.size
            && value.offset <= value.size
            && value.state.thread_id == owner.thread_id
    });
    let incremental_append = append_checkpoint.and_then(|checkpoint| {
        let fingerprint = ChunkedFingerprint::parse(&checkpoint.fingerprint)?;
        (!audit_mismatch
            && fingerprint.size == checkpoint.size
            && checkpoint.identity.same_file(identity)
            && modified_ns > checkpoint.modified_ns)
            .then_some((checkpoint, fingerprint))
    });

    let (fingerprint, append) = if let Some((_, mut previous)) = incremental_append {
        // Growth is not evidence that the previous prefix stayed immutable.
        // Advance the same bounded rolling audit used by stable files before
        // extending the checkpoint. The updated cursor is carried into the
        // extended fingerprint, so a continuously growing file cannot evade
        // verification of its older completed chunks forever.
        match audit_growing_chunked_fingerprint_from_file(&mut file, &mut previous)? {
            FingerprintAudit::Mismatch => {
                let full = full_content_fingerprints_from_file(&mut file, size, None)?;
                (full.current.encode()?, false)
            }
            FingerprintAudit::Verified { .. } => {
                let (fingerprint, verified_tail) =
                    extend_chunked_fingerprint_from_file(&mut file, size, &previous)?;
                if verified_tail {
                    (fingerprint.encode()?, true)
                } else {
                    let full = full_content_fingerprints_from_file(&mut file, size, None)?;
                    (full.current.encode()?, false)
                }
            }
        }
    } else {
        let prefix_size = append_checkpoint.map(|checkpoint| checkpoint.size);
        let full = full_content_fingerprints_from_file(&mut file, size, prefix_size)?;

        // A metadata-only change (touch, rename over the same bytes, or the
        // first scan after adopting chunk checkpoints) refreshes metadata
        // without rebuilding the normalized projection.
        if let Some(checkpoint) = checkpoint_by_path.as_ref()
            && checkpoint.state.projector_generation == PROJECTOR_GENERATION
            && checkpoint.state.owner_id == owner.owner_id
            && checkpoint.state.thread_id == owner.thread_id
            && checkpoint.size == size
            && stored_fingerprint_matches(
                &checkpoint.fingerprint,
                &full.current,
                &full.legacy_current,
            )
        {
            let encoded = full.current.encode()?;
            return mark_file_unchanged(
                &mut connection,
                checkpoint,
                archived,
                size,
                modified_ns,
                identity,
                Some(&encoded),
            );
        }

        // Suspicious growth (including preserved-mtime rewrites) receives a
        // complete prefix verification. Ordinary same-file appends use the
        // chunk extension path above and read only the prior tail plus suffix.
        let append = append_checkpoint.is_some_and(|checkpoint| {
            full.prefix.as_ref().is_some_and(|prefix| {
                stored_fingerprint_matches(
                    &checkpoint.fingerprint,
                    prefix,
                    full.legacy_prefix.as_deref().unwrap_or_default(),
                )
            })
        });
        (full.current.encode()?, append)
    };
    if suspicious_same_path_shrink
        && !same_source_shrink_was_observed(
            &connection,
            &owner.owner_id,
            &path_text,
            size,
            &fingerprint,
        )?
    {
        tracing::debug!(
            owner_id = owner.owner_id,
            path = %path.display(),
            candidate_size = size,
            previous_committed_size = checkpoint_by_path.as_ref().map_or(0, |value| value.offset),
            "deferring same-path source shrink until an identical complete snapshot is observed again"
        );
        return Ok(FileReport {
            deferred: true,
            ..FileReport::default()
        });
    }

    let (offset, line_number, inherited_before, mut state) = if append {
        let checkpoint = existing.as_ref().expect("append requires checkpoint");
        (
            checkpoint.offset,
            checkpoint.line_number,
            checkpoint.inherited_lines,
            checkpoint.state.clone(),
        )
    } else {
        (
            0,
            0,
            0,
            CursorState {
                owner_id: owner.owner_id.clone(),
                thread_id: owner.thread_id.clone(),
                parent_rollout_id: owner.parent_rollout_id.clone(),
                parent_thread_id: owner.parent_thread_id.clone(),
                agent_path: owner.agent_path.clone(),
                agent_nickname: owner.agent_nickname.clone(),
                forked: owner.forked,
                native_started: !owner.forked,
                ..CursorState::default()
            },
        )
    };
    state.projector_generation = PROJECTOR_GENERATION;

    // Claim writer ownership before the first transactional read. A deferred
    // read transaction cannot be upgraded after pricing commits on another
    // connection; SQLite returns BUSY_SNAPSHOT immediately in that case even
    // when a busy timeout is configured.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some((replaced_owner, replaced_thread)) = transaction
        .query_row(
            "SELECT rollout_id,root_thread_id FROM source_files WHERE path=?1 AND rollout_id<>?2",
            params![path.to_string_lossy(), owner.owner_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
    {
        let cleared_thread = clear_rollout(&transaction, &replaced_owner)?;
        transaction.execute(
            "DELETE FROM source_files WHERE rollout_id=?1",
            [&replaced_owner],
        )?;
        if let Some(replaced_thread) = cleared_thread.or(replaced_thread) {
            delete_thread_if_abandoned(&transaction, &replaced_thread)?;
        }
    }
    #[cfg(test)]
    run_process_file_after_transaction_read_hook();
    if !append {
        let previous_thread = clear_rollout(&transaction, &owner.owner_id)?;
        if let Some(previous_thread) = previous_thread
            && previous_thread != owner.thread_id
        {
            delete_thread_if_abandoned(&transaction, &previous_thread)?;
        }
        if owner.owner_id == owner.thread_id {
            transaction.execute(
                "UPDATE threads SET title=NULL,title_updated_at=NULL WHERE id=?1",
                [&owner.thread_id],
            )?;
        }
    }
    upsert_owner(&transaction, &owner, archived)?;

    let remaining = size.checked_sub(offset).ok_or_else(|| {
        anyhow!(
            "{} shrank below its committed projection boundary",
            path.display()
        )
    })?;
    file.seek(SeekFrom::Start(offset))?;
    // The metadata extent is the scan's immutable read boundary. A writer may
    // append after the stat above, but those bytes belong to the next scan and
    // must not advance this scan's durable checkpoint beyond its fingerprint.
    let mut reader = BufReader::new((&mut file).take(remaining));
    let mut source_line = line_number;
    let mut committed_offset = offset;
    let mut read_offset = offset;
    let mut inherited = inherited_before;
    let mut errors = 0_u64;
    // Appending valid records does not repair an earlier malformed line. Keep
    // the unresolved failure visible until a complete rebuild verifies every
    // retained line again.
    let mut last_error = append
        .then(|| existing.as_ref().and_then(|value| value.last_error.clone()))
        .flatten();
    let mut records = 0_u64;
    let mut failed = last_error.is_some();

    let mut bytes = Vec::new();
    loop {
        let (line_len, oversized) = match read_bounded_line(
            &mut reader,
            &mut bytes,
            MAX_JSONL_LINE_BYTES,
        )? {
            BoundedLine::Eof => {
                if read_offset != size {
                    return Err(anyhow!(
                        "{} changed while reading captured extent: expected {size} bytes, read {read_offset}",
                        path.display()
                    ));
                }
                break;
            }
            BoundedLine::Incomplete { len, .. } => {
                let observed_end = read_offset
                    .checked_add(len)
                    .ok_or_else(|| anyhow!("{} read offset overflowed", path.display()))?;
                if observed_end != size {
                    return Err(anyhow!(
                        "{} changed while reading captured extent: expected {size} bytes, read {observed_end}",
                        path.display()
                    ));
                }
                // A record cut off exactly at the captured extent belongs
                // to the next scan once its terminating newline arrives.
                // Its bytes are intentionally not committed yet.
                break;
            }
            BoundedLine::Complete { len, oversized } => (len, oversized),
        };
        let line_end = read_offset
            .checked_add(line_len)
            .filter(|end| *end <= size)
            .ok_or_else(|| anyhow!("{} read beyond captured extent", path.display()))?;
        read_offset = line_end;
        if oversized {
            errors += 1;
            last_error = Some(format!(
                "line {}: record exceeds {MAX_JSONL_LINE_BYTES}-byte limit",
                source_line + 1
            ));
            failed = true;
            // The complete record has been drained without retaining its
            // contents, so later records remain independently projectable.
            committed_offset = line_end;
            source_line += 1;
            records += 1;
            continue;
        }
        let value = match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => value,
            Err(error) => {
                errors += 1;
                last_error = Some(format!("line {}: {error}", source_line + 1));
                failed = true;
                // A complete malformed record is isolated to its source line.
                // Advance the durable checkpoint and keep projecting later
                // records so one corrupt line cannot hide the rest of a
                // session forever. Incomplete trailing records are handled
                // above and remain uncommitted until their newline arrives.
                committed_offset = line_end;
                source_line += 1;
                records += 1;
                continue;
            }
        };
        committed_offset = line_end;
        source_line += 1;
        records += 1;
        project_record(&transaction, &mut state, source_line, &value)
            .with_context(|| format!("failed to project line {source_line}"))?;
        if !state.native_started {
            inherited += 1;
        }
    }
    drop(reader);

    // `byte_offset` is the durable projection boundary. Fingerprint exactly
    // that committed prefix, not a writer-owned unfinished tail beyond it, so
    // a path handoff can prove it contains every record represented in SQLite.
    let fingerprint = if committed_offset == size {
        fingerprint
    } else {
        fingerprint_for_prefix_from_file(&mut file, &fingerprint, committed_offset)?
    };
    let state_json = serde_json::to_string(&state)?;
    transaction.execute(
        "INSERT INTO source_files(
            rollout_id,path,archived,size_bytes,modified_ns,ctime_ns,device_id,inode,
            content_fingerprint,
            byte_offset,line_number,root_thread_id,parent_rollout_id,native_started,
            inherited_lines,parse_state_json,error_count,last_error,ingested_at
         ) VALUES(
            ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19
         )
         ON CONFLICT(rollout_id) DO UPDATE SET
            path=excluded.path, archived=excluded.archived, size_bytes=excluded.size_bytes,
            modified_ns=excluded.modified_ns, ctime_ns=excluded.ctime_ns,
            device_id=excluded.device_id, inode=excluded.inode,
            content_fingerprint=excluded.content_fingerprint,
            byte_offset=excluded.byte_offset, line_number=excluded.line_number,
            root_thread_id=excluded.root_thread_id, parent_rollout_id=excluded.parent_rollout_id,
            native_started=excluded.native_started, inherited_lines=excluded.inherited_lines,
            parse_state_json=excluded.parse_state_json,
            error_count=source_files.error_count+excluded.error_count,
            last_error=excluded.last_error, ingested_at=excluded.ingested_at",
        params![
            state.owner_id,
            path.to_string_lossy(),
            archived as i64,
            size as i64,
            modified_ns as i64,
            identity.ctime_ns,
            identity.device_id,
            identity.inode,
            fingerprint,
            committed_offset as i64,
            source_line as i64,
            state.thread_id,
            state.parent_rollout_id,
            state.native_started as i64,
            inherited as i64,
            state_json,
            errors as i64,
            last_error.as_deref(),
            Utc::now().to_rfc3339(),
        ],
    )?;
    if suspicious_same_path_shrink {
        transaction.execute(
            "DELETE FROM app_meta WHERE key=?1",
            [pending_source_shrink_key(&owner.owner_id)],
        )?;
    }
    // A parent may have projected a terminal child observation before the
    // child's own file was discovered. Re-apply surviving observations after
    // this rollout's native events so chronological evidence, rather than
    // source discovery order, determines the promoted lifecycle.
    rematerialize_surviving_agent_observation(&transaction, &state.owner_id)?;
    // Observations with identical timestamps use source path/line as their
    // stable tie-break. A newly discovered parent can sort before an already
    // projected parent, so replay every child touched by this rollout after
    // the source-file path is durable. Without this bounded replay,
    // incremental ingestion can disagree with a fresh rebuild.
    rematerialize_observed_children(&transaction, &state.owner_id)?;
    transaction.commit()?;

    Ok(FileReport {
        deferred: false,
        unchanged: false,
        failed,
        records,
        inherited: inherited.saturating_sub(inherited_before),
        error: last_error,
    })
}

fn mark_file_unchanged(
    connection: &mut Connection,
    checkpoint: &SourceCheckpoint,
    archived: bool,
    size: u64,
    modified_ns: u64,
    identity: FileIdentity,
    fingerprint: Option<&str>,
) -> Result<FileReport> {
    let metadata_changed = checkpoint.archived != archived
        || checkpoint.size != size
        || checkpoint.modified_ns != modified_ns
        || checkpoint.identity != identity;
    if metadata_changed || fingerprint.is_some() {
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE source_files SET
                archived=?1,size_bytes=?2,modified_ns=?3,ctime_ns=?4,device_id=?5,inode=?6,
                content_fingerprint=COALESCE(?7,content_fingerprint)
             WHERE rollout_id=?8",
            params![
                archived as i64,
                size as i64,
                modified_ns as i64,
                identity.ctime_ns,
                identity.device_id,
                identity.inode,
                fingerprint,
                checkpoint.state.owner_id,
            ],
        )?;
        if checkpoint.archived != archived {
            transaction.execute(
                "UPDATE rollouts SET archived=?1 WHERE id=?2",
                params![archived as i64, checkpoint.state.owner_id],
            )?;
        }
        transaction.commit()?;
    }
    Ok(FileReport {
        unchanged: true,
        failed: checkpoint.last_error.is_some(),
        error: checkpoint.last_error.clone(),
        ..FileReport::default()
    })
}

fn project_record(
    tx: &Transaction<'_>,
    state: &mut CursorState,
    line: u64,
    value: &Value,
) -> Result<()> {
    let outer_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let payload = value.get("payload").unwrap_or(&Value::Null);
    let payload_type = payload.get("type").and_then(Value::as_str);
    let explicit_timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .or_else(|| payload.get("timestamp").and_then(Value::as_str));
    let timestamp_owned = match explicit_timestamp {
        Some(timestamp) => {
            let timestamp = canonical_source_timestamp(timestamp)?;
            state.last_timestamp = Some(timestamp.clone());
            timestamp
        }
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp"))?,
    };
    let timestamp = timestamp_owned.as_str();

    if outer_type == "event_msg" && payload_type == Some("token_count") {
        let info = match payload.get("info") {
            Some(Value::Null) => {
                // Legacy rollouts use an explicit null info payload to delimit
                // independent cumulative token scopes. The next snapshot is a
                // fresh total, so retaining the previous scope would derive a
                // bogus cross-scope delta (and can make cached input exceed
                // that delta). The boundary itself carries no usage fact.
                state.cumulative = TokenUsage::default();
                return Ok(());
            }
            None => &Value::Null,
            Some(info @ Value::Object(_)) => info,
            Some(_) => {
                return Err(anyhow!(
                    "source line {line} has token_count.info with a non-object value"
                ));
            }
        };
        let total = parse_total_token_usage(info, line)?;
        if !state.native_started {
            if let Some(total) = total {
                state.cumulative = total;
            }
            return Ok(());
        }
        // A single accounting snapshot is emitted once per rate-limit bucket in
        // some Codex builds. Those copies can have different timestamps and
        // identical `last_token_usage`, so source-line or timestamp dedupe still
        // overcounts. Treat the cumulative counter as the source of truth and
        // materialize only its forward delta. A decrease denotes a real counter
        // reset (commonly at a turn/model boundary); on that boundary the
        // explicitly reported last usage is the precise new increment.
        let last = if total.is_some() && last_token_usage_is_total_only_hint(info) {
            // Some Codex builds emit an initial context-size hint in
            // `last_token_usage`: every attributable component is zero while
            // `total_tokens` alone is nonzero. It is not billable usage (the
            // following cumulative snapshot starts from zero), cannot be
            // priced without inventing an input/output split, and would be
            // double-counted if materialized. Ignore only this exact shape
            // when an authoritative cumulative counter is present.
            None
        } else {
            parse_token_usage(info, "last_token_usage", line)?
        };
        let mut usage = if let Some(current) = total {
            let delta = if current == state.cumulative {
                TokenUsage::default()
            } else if current.decreased_from(state.cumulative) {
                // The new sequence may already include more than this request.
                // Codex's last-usage fact is the precise increment at a reset.
                last.unwrap_or(current)
            } else {
                current.saturating_sub(state.cumulative)
            };
            state.cumulative = current;
            delta
        } else {
            last.unwrap_or_default()
        };
        if usage.total_tokens == 0 {
            usage.total_tokens = usage
                .input_tokens
                .checked_add(usage.output_tokens)
                .ok_or_else(|| anyhow!("source line {line} has overflowing total_tokens"))?;
        }
        validate_token_usage(usage, true, "derived token usage", line)?;
        let ignore_legacy_unattributed_usage = state.current_model.is_none()
            && timestamp
                .get(..4)
                .and_then(|year| year.parse::<i32>().ok())
                .is_some_and(|year| year < MODEL_ATTRIBUTION_REQUIRED_FROM_YEAR);
        if !usage.is_zero() && !ignore_legacy_unattributed_usage {
            let input_tokens = checked_token_count(usage.input_tokens, "input_tokens", line)?;
            let cached_input_tokens =
                checked_token_count(usage.cached_input_tokens, "cached_input_tokens", line)?;
            let output_tokens = checked_token_count(usage.output_tokens, "output_tokens", line)?;
            let reasoning_tokens = checked_token_count(
                usage.reasoning_output_tokens,
                "reasoning_output_tokens",
                line,
            )?;
            let total_tokens = checked_token_count(usage.total_tokens, "total_tokens", line)?;
            ensure_turn(tx, state, timestamp)?;
            tx.execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    model,effort,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,1)
                 ON CONFLICT(id) DO NOTHING",
                params![
                    event_id(state, line),
                    state.thread_id,
                    state.owner_id,
                    state.current_turn,
                    state.owner_id,
                    timestamp,
                    line as i64,
                    state.current_model.as_deref().unwrap_or("unknown"),
                    state.current_effort,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    reasoning_tokens,
                    total_tokens,
                ],
            )?;
        }
        touch_owner(tx, state, timestamp)?;
        return Ok(());
    }

    if state.forked && !state.native_started {
        if outer_type == "event_msg" && payload_type == Some("task_started") {
            let turn_id = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )?;
            if turn_id
                .as_deref()
                .is_some_and(|turn_id| is_owner_native_turn(&state.owner_id, turn_id))
            {
                state.native_started = true;
            }
        }
        if !state.native_started {
            return Ok(());
        }
    }

    let legacy_meta = value.get("type").is_none()
        && value.get("id").and_then(Value::as_str) == Some(state.owner_id.as_str());
    if outer_type == "session_meta" || legacy_meta {
        let metadata = if legacy_meta { value } else { payload };
        if metadata.get("id").and_then(Value::as_str) == Some(state.owner_id.as_str()) {
            update_owner_metadata(tx, state, metadata, timestamp)?;
        }
        return Ok(());
    }
    if !state.native_started {
        return Ok(());
    }

    match outer_type {
        "turn_context" => {
            state.current_turn = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )?
            .or_else(|| state.current_turn.clone());
            state.turn_context_seen = true;
            state.current_model = payload
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_model.clone());
            state.current_effort = payload
                .get("effort")
                .or_else(|| payload.get("reasoning_effort"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_effort.clone());
            ensure_turn(tx, state, timestamp)?;
            tx.execute(
                "UPDATE turns SET model=COALESCE(?1,model), effort=COALESCE(?2,effort)
                 WHERE id=?3",
                params![
                    state.current_model,
                    state.current_effort,
                    state.current_turn
                ],
            )?;
        }
        "response_item" => project_response_item(tx, state, line, timestamp, payload, false)?,
        "event_msg" => project_event_message(tx, state, line, timestamp, payload)?,
        // Early rollout formats placed durable conversation and tool records at
        // the top level. Normalize those shapes through the same projector.
        "message"
        | "agent_message"
        | "reasoning"
        | "function_call"
        | "function_call_output"
        | "custom_tool_call"
        | "custom_tool_call_output"
        | "tool_search_call"
        | "tool_search_output"
        | "web_search_call"
        | "image_generation_call" => {
            project_response_item(tx, state, line, timestamp, value, true)?
        }
        "compacted" => insert_event(
            tx,
            state,
            line,
            timestamp,
            "compaction",
            None,
            Some("Context compacted"),
            None,
            None,
            None,
            None,
            payload,
        )?,
        "world_state" => {}
        "inter_agent_communication_metadata" => {}
        _ => insert_event(
            tx,
            state,
            line,
            timestamp,
            "system",
            None,
            Some(outer_type),
            None,
            None,
            None,
            None,
            value.get("payload").unwrap_or(value),
        )?,
    }
    touch_owner(tx, state, timestamp)?;
    Ok(())
}

fn project_response_item(
    tx: &Transaction<'_>,
    state: &mut CursorState,
    line: u64,
    timestamp: &str,
    payload: &Value,
    allow_implicit_turn: bool,
) -> Result<()> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let explicit_turn_id = normalized_relational_identifier(
        payload
            .get("internal_chat_message_metadata_passthrough")
            .and_then(|value| value.get("turn_id"))
            .and_then(Value::as_str),
        "turn id",
    )?;
    if matches!(
        kind,
        "message"
            | "agent_message"
            | "reasoning"
            | "function_call"
            | "custom_tool_call"
            | "tool_search_call"
            | "function_call_output"
            | "custom_tool_call_output"
            | "tool_search_output"
            | "web_search_call"
            | "image_generation_call"
    ) && let Some(turn_id) = explicit_turn_id.as_deref()
    {
        state.current_turn = Some(turn_id.to_owned());
    }
    if kind == "agent_message" {
        ensure_turn(tx, state, timestamp)?;
        let body = extract_content(
            payload
                .get("content")
                .or_else(|| payload.get("message"))
                .unwrap_or(&Value::Null),
        );
        let author = payload
            .get("author")
            .and_then(Value::as_str)
            .unwrap_or("agent");
        let recipient = payload
            .get("recipient")
            .and_then(Value::as_str)
            .unwrap_or("agent");
        let label = format!("{author} → {recipient}");
        insert_event(
            tx,
            state,
            line,
            timestamp,
            "subagent",
            None,
            Some(&label),
            (!body.is_empty()).then_some(body.as_str()),
            None,
            None,
            None,
            payload,
        )?;
        return Ok(());
    }
    match kind {
        "message" => {
            let role = payload
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("assistant");
            let source_content = payload.get("content").unwrap_or(&Value::Null);
            let mut content = redact_data_urls(&extract_content(source_content));
            if content.is_empty() && has_omitted_attachment(source_content) {
                content = "[Attachment omitted]".to_owned();
            }
            if !content.is_empty() && matches!(role, "user" | "assistant") {
                if role == "user" && is_turn_abort_envelope(&content) {
                    return Ok(());
                }
                if role == "user" && is_transport_context_envelope(&content) {
                    return Ok(());
                }
                if role == "user"
                    && !state.turn_context_seen
                    && !allow_implicit_turn
                    && !state
                        .current_turn
                        .as_deref()
                        .is_some_and(|turn_id| turn_has_open_native_lifecycle(tx, turn_id))
                {
                    return Ok(());
                }
                if role == "user" && explicit_turn_id.is_none() {
                    let current_accepts_feedback = state
                        .current_turn
                        .as_deref()
                        .is_some_and(|turn_id| turn_accepts_metadata_free_feedback(tx, turn_id));
                    if current_accepts_feedback {
                        if let Some(turn_id) = state.current_turn.as_deref() {
                            reopen_provisionally_completed_turn(tx, turn_id)?;
                        }
                    } else {
                        state.current_turn = Some(format!("{}:legacy-turn:{line}", state.owner_id));
                        ensure_turn(tx, state, timestamp)?;
                    }
                }
                ensure_turn(tx, state, timestamp)?;
                // Source message IDs are not globally unique. Validate the
                // source value at the boundary, then scope its projected
                // identity to the owning rollout. The paired Activity event
                // applies the same transform to its call identity below.
                let source_id = normalized_relational_identifier(
                    payload.get("id").and_then(Value::as_str),
                    "message id",
                )?;
                let id = source_id
                    .as_deref()
                    .map(|source_id| projected_message_id(&state.owner_id, source_id))
                    .unwrap_or_else(|| event_id(state, line));
                tx.execute(
                    "INSERT OR IGNORE INTO messages(
                        id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
                     ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        id,
                        state.thread_id,
                        state.owner_id,
                        state.current_turn,
                        timestamp,
                        role,
                        content,
                        line as i64,
                    ],
                )?;
                if role == "user" && allow_implicit_turn && state.owner_id == state.thread_id {
                    let title = compact_title(&content);
                    tx.execute(
                        "UPDATE threads SET title=COALESCE(title, ?1) WHERE id=?2",
                        params![title, state.thread_id],
                    )?;
                }
                if role == "assistant" {
                    tx.execute(
                        "DELETE FROM events WHERE rollout_id=?1 AND turn_id IS ?2
                         AND kind='update' AND label='Assistant update'
                         AND ABS((julianday(timestamp)-julianday(?3))*86400.0)<1.0
                         AND (body=?4 OR body LIKE ?4 || '%' OR ?4 LIKE body || '%')",
                        params![state.owner_id, state.current_turn, timestamp, content],
                    )?;
                }
                let activity_kind = if role == "user" {
                    "message"
                } else if payload.get("phase").and_then(Value::as_str) == Some("commentary") {
                    "update"
                } else {
                    "final"
                };
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    activity_kind,
                    Some(role),
                    None,
                    Some(&content),
                    None,
                    None,
                    None,
                    payload,
                )?;
                if activity_kind == "final" {
                    complete_turn_from_final(tx, state, timestamp, &content)?;
                }
            }
        }
        "reasoning" => {
            let body = extract_content(
                payload
                    .get("summary")
                    .or_else(|| payload.get("content"))
                    .unwrap_or(&Value::Null),
            );
            if !body.is_empty() {
                ensure_turn(tx, state, timestamp)?;
                tx.execute(
                    "DELETE FROM events WHERE rollout_id=?1 AND turn_id IS ?2
                     AND kind='reasoning' AND label='Reasoning'
                     AND (body=?3 OR ABS((julianday(timestamp)-julianday(?4))*86400.0)<1.0)",
                    params![state.owner_id, state.current_turn, body, timestamp],
                )?;
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    "reasoning",
                    Some("assistant"),
                    Some("Reasoning summary"),
                    Some(&body),
                    None,
                    None,
                    None,
                    payload,
                )?;
            }
        }
        "function_call" | "custom_tool_call" | "tool_search_call" => {
            ensure_turn(tx, state, timestamp)?;
            let call_id = normalized_relational_identifier(
                payload
                    .get("call_id")
                    .or_else(|| payload.get("id"))
                    .and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_else(|| format!("line-{line}"));
            let name = payload.get("name").and_then(Value::as_str).unwrap_or(kind);
            let namespace = payload.get("namespace").and_then(Value::as_str);
            upsert_tool_call(tx, state, timestamp, &call_id, name, namespace, payload)?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "tool_call",
                None,
                Some(name),
                None,
                payload.get("status").and_then(Value::as_str),
                Some(name),
                None,
                payload,
            )?;
        }
        "function_call_output" | "custom_tool_call_output" | "tool_search_output" => {
            ensure_turn(tx, state, timestamp)?;
            let call_id = normalized_relational_identifier(
                payload
                    .get("call_id")
                    .or_else(|| payload.get("id"))
                    .and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_else(|| "unknown".to_owned());
            complete_tool_call(tx, state, timestamp, &call_id, None, None, None)?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "tool_output",
                None,
                Some("Tool result"),
                None,
                Some("completed"),
                None,
                None,
                payload,
            )?;
        }
        "web_search_call" | "image_generation_call" => {
            ensure_turn(tx, state, timestamp)?;
            let call_id = normalized_relational_identifier(
                payload
                    .get("id")
                    .or_else(|| payload.get("call_id"))
                    .and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_else(|| format!("line-{line}"));
            upsert_tool_call(tx, state, timestamp, &call_id, kind, None, payload)?;
            let status = payload.get("status").and_then(Value::as_str);
            if status == Some("completed") {
                complete_tool_call(tx, state, timestamp, &call_id, status, None, Some(kind))?;
            }
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "tool_call",
                None,
                Some(kind),
                None,
                status,
                Some(kind),
                None,
                payload,
            )?;
        }
        "ghost_snapshot" => {}
        _ => insert_event(
            tx,
            state,
            line,
            timestamp,
            "system",
            None,
            Some(kind),
            None,
            None,
            None,
            None,
            payload,
        )?,
    }
    Ok(())
}

fn project_event_message(
    tx: &Transaction<'_>,
    state: &mut CursorState,
    line: u64,
    timestamp: &str,
    payload: &Value,
) -> Result<()> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match kind {
        "task_started" => {
            if let Some(turn_id) = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )? {
                if let Some(previous_turn) = state.current_turn.as_deref()
                    && previous_turn != turn_id.as_str()
                    && turn_has_open_native_lifecycle(tx, previous_turn)
                {
                    tx.execute(
                        "UPDATE turns
                         SET completed_at=?1,status='interrupted'
                         WHERE id=?2 AND status='running'",
                        params![timestamp, previous_turn],
                    )?;
                    record_implicit_turn_interruption(tx, state, line, previous_turn, timestamp)?;
                }
                state.current_turn = Some(turn_id);
            }
            state.turn_context_seen = false;
            ensure_turn(tx, state, timestamp)?;
            tx.execute(
                "UPDATE agent_runs SET status='running',completed_at=NULL WHERE id=?1",
                [&state.owner_id],
            )?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "turn_started",
                None,
                Some("Turn started"),
                None,
                Some("running"),
                None,
                None,
                payload,
            )?;
        }
        "task_complete" => {
            if let Some(turn_id) = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )? {
                state.current_turn = Some(turn_id);
            }
            ensure_turn(tx, state, timestamp)?;
            let last_agent_message = payload
                .get("last_agent_message")
                .and_then(Value::as_str)
                .map(redact_data_urls);
            tx.execute(
                "UPDATE turns SET completed_at=?1,status='completed',last_agent_message=?2,
                    duration_ms=?3,time_to_first_token_ms=?4 WHERE id=?5",
                params![
                    timestamp,
                    last_agent_message.as_deref(),
                    raw_duration_ms(payload.get("duration_ms")),
                    payload
                        .get("time_to_first_token_ms")
                        .and_then(Value::as_i64),
                    state.current_turn,
                ],
            )?;
            tx.execute(
                "UPDATE agent_runs SET status='completed',completed_at=?1 WHERE id=?2",
                params![timestamp, state.owner_id],
            )?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "turn_completed",
                None,
                Some("Turn completed"),
                last_agent_message.as_deref(),
                Some("completed"),
                None,
                raw_duration_ms(payload.get("duration_ms")),
                payload,
            )?;
        }
        "user_message" => {
            if state.owner_id == state.thread_id
                && let Some(message) = payload
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
            {
                let title = compact_title(message);
                tx.execute(
                    "UPDATE threads SET title=?1,title_updated_at=?2
                     WHERE id=?3 AND title_updated_at IS NULL",
                    params![title, timestamp, state.thread_id],
                )?;
            }
        }
        "agent_reasoning" => {
            if let Some(body) = payload
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| redact_and_bound(value, PROJECTED_SESSION_TITLE_CHARS))
            {
                let duplicate: i64 = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM events
                     WHERE rollout_id=?1 AND turn_id IS ?2 AND kind='reasoning' AND body=?3)",
                    params![state.owner_id, state.current_turn, body],
                    |row| row.get(0),
                )?;
                if duplicate == 0 {
                    insert_event(
                        tx,
                        state,
                        line,
                        timestamp,
                        "reasoning",
                        Some("assistant"),
                        Some("Reasoning"),
                        Some(&body),
                        None,
                        None,
                        None,
                        payload,
                    )?;
                }
            }
        }
        "agent_message" => {
            if let Some(body) = payload.get("message").and_then(Value::as_str) {
                let body = redact_data_urls(body);
                let canonical: i64 = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messages
                     WHERE rollout_id=?1 AND turn_id IS ?2 AND role='assistant'
                     AND ABS((julianday(timestamp)-julianday(?3))*86400.0)<1.0
                     AND (content=?4 OR content LIKE ?4 || '%' OR ?4 LIKE content || '%'))",
                    params![state.owner_id, state.current_turn, timestamp, body],
                    |row| row.get(0),
                )?;
                if canonical == 0 {
                    insert_event(
                        tx,
                        state,
                        line,
                        timestamp,
                        "update",
                        Some("assistant"),
                        Some("Assistant update"),
                        Some(&body),
                        None,
                        None,
                        None,
                        payload,
                    )?;
                }
            }
        }
        "view_image_tool_call" | "dynamic_tool_call_request" => {}
        "item_completed" => {
            let item = payload.get("item").unwrap_or(&Value::Null);
            if item.get("type").and_then(Value::as_str) == Some("Plan") {
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    "plan",
                    None,
                    Some("Plan"),
                    item.get("text").and_then(Value::as_str),
                    Some("completed"),
                    None,
                    None,
                    payload,
                )?;
            }
        }
        "entered_review_mode" | "exited_review_mode" => {
            let (label, status) = if kind == "entered_review_mode" {
                ("Entered review mode", "active")
            } else {
                ("Exited review mode", "completed")
            };
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "state",
                None,
                Some(label),
                None,
                Some(status),
                None,
                None,
                payload,
            )?;
        }
        "sub_agent_activity" => {
            let agent_id = normalized_relational_identifier(
                payload.get("agent_thread_id").and_then(Value::as_str),
                "subagent thread id",
            )?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "subagent",
                None,
                payload.get("kind").and_then(Value::as_str),
                payload.get("agent_path").and_then(Value::as_str),
                payload.get("kind").and_then(Value::as_str),
                None,
                None,
                payload,
            )?;
            if let Some(agent_id) = agent_id {
                let activity = payload
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("running");
                upsert_observed_agent(
                    tx,
                    &agent_id,
                    &state.thread_id,
                    &state.owner_id,
                    payload.get("agent_path").and_then(Value::as_str),
                    timestamp,
                    activity,
                )?;
            }
        }
        "thread_goal_updated" => {
            let goal = payload.get("goal").unwrap_or(payload);
            let objective = goal
                .get("objective")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(redact_data_urls);
            let status = goal.get("status").and_then(Value::as_str);
            if objective.is_some() || status.is_some() {
                let previous = tx
                    .query_row(
                        "SELECT body,status FROM events WHERE thread_id=?1 AND kind='goal'
                         ORDER BY timestamp DESC,source_line DESC LIMIT 1",
                        [&state.thread_id],
                        |row| {
                            Ok((
                                row.get::<_, Option<String>>(0)?,
                                row.get::<_, Option<String>>(1)?,
                            ))
                        },
                    )
                    .optional()?;
                if previous.as_ref().is_none_or(|(body, previous_status)| {
                    body.as_deref() != objective.as_deref() || previous_status.as_deref() != status
                }) {
                    insert_event(
                        tx,
                        state,
                        line,
                        timestamp,
                        "goal",
                        None,
                        Some("Goal updated"),
                        objective.as_deref(),
                        status,
                        None,
                        None,
                        payload,
                    )?;
                }
            }
        }
        "context_compacted" => {
            let duplicate: i64 = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE thread_id=?1 AND kind='compaction'
                 AND ABS((julianday(timestamp)-julianday(?2))*86400.0)<1.0)",
                params![state.thread_id, timestamp],
                |row| row.get(0),
            )?;
            if duplicate == 0 {
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    "compaction",
                    None,
                    Some("Context compacted"),
                    None,
                    None,
                    None,
                    None,
                    payload,
                )?;
            }
        }
        "thread_settings_applied" => {
            let settings = payload.get("thread_settings").unwrap_or(payload);
            state.current_model = settings
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_model.clone());
            state.current_effort = settings
                .get("reasoning_effort")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| state.current_effort.clone());
            if state.current_turn.is_some() {
                tx.execute(
                    "UPDATE turns SET model=COALESCE(?1,model),effort=COALESCE(?2,effort)
                     WHERE id=?3",
                    params![
                        state.current_model,
                        state.current_effort,
                        state.current_turn
                    ],
                )?;
            }
        }
        "thread_name_updated" => {
            if let Some(title) = payload
                .get("thread_name")
                .or_else(|| payload.get("name"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| redact_and_bound(value, PROJECTED_SESSION_TITLE_CHARS))
            {
                tx.execute(
                    "UPDATE threads SET title=?1,title_updated_at=?2
                     WHERE id=?3 AND (title_updated_at IS NULL OR title_updated_at<=?2)",
                    params![title, timestamp, state.thread_id],
                )?;
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    "state",
                    None,
                    Some("Thread renamed"),
                    Some(&title),
                    None,
                    None,
                    None,
                    payload,
                )?;
            }
        }
        "turn_aborted" => {
            if let Some(turn_id) = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )? {
                state.current_turn = Some(turn_id);
            }
            ensure_turn(tx, state, timestamp)?;
            tx.execute(
                "UPDATE turns SET completed_at=?1,status='interrupted' WHERE id=?2",
                params![timestamp, state.current_turn],
            )?;
            tx.execute(
                "UPDATE agent_runs SET completed_at=?1,status='interrupted' WHERE id=?2",
                params![timestamp, state.owner_id],
            )?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "state",
                None,
                Some(kind),
                value_to_text(payload).as_deref(),
                Some("interrupted"),
                None,
                None,
                payload,
            )?;
        }
        "thread_rolled_back" => {
            if let Some(turn_id) = normalized_relational_identifier(
                payload.get("turn_id").and_then(Value::as_str),
                "turn id",
            )? {
                state.current_turn = Some(turn_id);
            }
            ensure_turn(tx, state, timestamp)?;
            tx.execute(
                "UPDATE turns SET completed_at=?1,status='rolled_back' WHERE id=?2",
                params![timestamp, state.current_turn],
            )?;
            tx.execute(
                "UPDATE agent_runs SET completed_at=?1,status='rolled_back' WHERE id=?2",
                params![timestamp, state.owner_id],
            )?;
            insert_event(
                tx,
                state,
                line,
                timestamp,
                "state",
                None,
                Some(kind),
                value_to_text(payload).as_deref(),
                Some("rolled_back"),
                None,
                None,
                payload,
            )?;
        }
        "exec_command_end" => {
            if let Some(call_id) = normalized_relational_identifier(
                payload.get("call_id").and_then(Value::as_str),
                "tool call id",
            )? {
                let duration = duration_ms(payload.get("duration"))
                    .or_else(|| raw_duration_ms(payload.get("duration_ms")));
                let failed = payload.get("status").and_then(Value::as_str) == Some("failed")
                    || payload
                        .get("exit_code")
                        .and_then(Value::as_i64)
                        .is_some_and(|value| value != 0)
                    || payload.get("error").is_some_and(|value| !value.is_null());
                let status = if failed { "failed" } else { "completed" };
                enrich_tool_call(
                    tx,
                    state,
                    timestamp,
                    &call_id,
                    "exec_command",
                    status,
                    duration,
                )?;
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    "tool_completed",
                    None,
                    Some("exec_command"),
                    None,
                    Some(status),
                    Some("exec_command"),
                    duration,
                    payload,
                )?;
            }
        }
        "dynamic_tool_call_response" => {
            if let Some(call_id) = normalized_relational_identifier(
                payload.get("call_id").and_then(Value::as_str),
                "tool call id",
            )? {
                let duration = duration_ms(payload.get("duration"))
                    .or_else(|| raw_duration_ms(payload.get("duration_ms")));
                let failed = payload.get("success").and_then(Value::as_bool) == Some(false)
                    || payload.get("error").is_some_and(|value| !value.is_null());
                let status = if failed { "failed" } else { "completed" };
                enrich_tool_call(
                    tx,
                    state,
                    timestamp,
                    &call_id,
                    payload
                        .get("tool")
                        .and_then(Value::as_str)
                        .unwrap_or("dynamic_tool"),
                    status,
                    duration,
                )?;
                insert_event(
                    tx,
                    state,
                    line,
                    timestamp,
                    "tool_completed",
                    None,
                    payload.get("tool").and_then(Value::as_str),
                    None,
                    Some(status),
                    payload.get("tool").and_then(Value::as_str),
                    duration,
                    payload,
                )?;
            }
        }
        "mcp_tool_call_end" | "patch_apply_end" | "web_search_end" | "image_generation_end" => {
            let mut call_id = normalized_relational_identifier(
                payload.get("call_id").and_then(Value::as_str),
                "tool call id",
            )?
            .unwrap_or_default();
            let invocation = payload.get("invocation");
            let invocation_tool = invocation
                .and_then(|value| value.get("tool"))
                .and_then(Value::as_str);
            let invocation_server = invocation
                .and_then(|value| value.get("server"))
                .and_then(Value::as_str);
            let projected_name = match kind {
                "mcp_tool_call_end" => invocation_tool,
                "patch_apply_end" => Some("apply_patch"),
                "web_search_end" => Some("web_search_call"),
                "image_generation_end" => Some("image_generation_call"),
                _ => None,
            };
            let exact_exists = if call_id.is_empty() {
                false
            } else {
                tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM tool_calls WHERE rollout_id=?1 AND call_id=?2)",
                    params![state.owner_id, call_id],
                    |row| row.get::<_, i64>(0),
                )? != 0
            };
            let mut matched_existing = exact_exists;
            if !exact_exists
                && let Some(name) = projected_name
                && let Some(existing) = tx
                    .query_row(
                        "SELECT call_id FROM tool_calls WHERE rollout_id=?1 AND name=?2
                         AND completed_at IS NULL ORDER BY started_at DESC LIMIT 1",
                        params![state.owner_id, name],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
            {
                call_id = existing;
                matched_existing = true;
            }
            let duration = duration_ms(payload.get("duration"))
                .or_else(|| raw_duration_ms(payload.get("duration_ms")));
            let failed = payload.get("success").and_then(Value::as_bool) == Some(false)
                || matches!(
                    payload.get("status").and_then(Value::as_str),
                    Some("failed" | "cancelled" | "canceled")
                )
                || payload.get("error").is_some_and(|value| !value.is_null());
            let status = if failed { "failed" } else { "completed" };
            if !call_id.is_empty() {
                if let Some(name) = projected_name {
                    upsert_tool_call(
                        tx,
                        state,
                        timestamp,
                        &call_id,
                        name,
                        invocation_server,
                        payload,
                    )?;
                }
                complete_tool_call(
                    tx,
                    state,
                    timestamp,
                    &call_id,
                    Some(status),
                    duration,
                    projected_name,
                )?;
            }
            // Some modern MCP traces only emit the completion envelope. When
            // it carries an explicit call ID and invocation metadata, that
            // record is the durable call rather than an orphaned result.
            let completion_is_call =
                !matched_existing && !call_id.is_empty() && projected_name.is_some();
            insert_event(
                tx,
                state,
                line,
                timestamp,
                if completion_is_call {
                    "tool_call"
                } else {
                    "tool_completed"
                },
                None,
                projected_name.or(Some(kind)),
                None,
                Some(status),
                projected_name,
                duration,
                payload,
            )?;
        }
        _ => insert_event(
            tx,
            state,
            line,
            timestamp,
            "system",
            None,
            Some(kind),
            None,
            None,
            None,
            None,
            payload,
        )?,
    }
    Ok(())
}

fn record_implicit_turn_interruption(
    tx: &Transaction<'_>,
    state: &CursorState,
    line: u64,
    turn_id: &str,
    timestamp: &str,
) -> Result<()> {
    // A new native task is durable evidence that the previous open task was
    // interrupted. Store that evidence alongside the direct turn update so a
    // later lifecycle rematerialization cannot resurrect the old task merely
    // because its last source event was `turn_started`.
    tx.execute(
        "INSERT OR IGNORE INTO events(
            id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
            kind,label,status,native
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,'state','Turn interrupted','interrupted',1)",
        params![
            format!("{}:{line}:implicit-interrupt", state.owner_id),
            state.thread_id,
            state.owner_id,
            turn_id,
            state.owner_id,
            timestamp,
            line as i64,
        ],
    )?;
    Ok(())
}

fn ensure_turn(tx: &Transaction<'_>, state: &mut CursorState, timestamp: &str) -> Result<()> {
    let Some(turn_id) = state.current_turn.as_deref() else {
        return Ok(());
    };
    tx.execute(
        "INSERT INTO turns(
            id,thread_id,rollout_id,agent_run_id,started_at,status,model,effort
         ) VALUES(?1,?2,?3,?4,?5,'running',?6,?7)
         ON CONFLICT(id) DO UPDATE SET
            model=COALESCE(excluded.model,turns.model),
            effort=COALESCE(excluded.effort,turns.effort)",
        params![
            turn_id,
            state.thread_id,
            state.owner_id,
            state.owner_id,
            timestamp,
            state.current_model,
            state.current_effort,
        ],
    )?;
    Ok(())
}

fn turn_accepts_metadata_free_feedback(tx: &Transaction<'_>, turn_id: &str) -> bool {
    let running = tx
        .query_row(
            "SELECT status='running' FROM turns WHERE id=?1",
            [turn_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        != 0;
    running || turn_has_open_native_lifecycle(tx, turn_id)
}

fn turn_has_open_native_lifecycle(tx: &Transaction<'_>, turn_id: &str) -> bool {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM events
             WHERE turn_id=?1 AND kind='turn_started'
         ) AND NOT EXISTS(
             SELECT 1 FROM events
             WHERE turn_id=?1
               AND (
                   kind='turn_completed'
                   OR (kind='state' AND status IN ('interrupted','rolled_back'))
               )
         )",
        [turn_id],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0)
        != 0
}

fn reopen_provisionally_completed_turn(tx: &Transaction<'_>, turn_id: &str) -> Result<()> {
    // A native task can emit final-answer text and then continue when the user
    // steers it before the durable task-complete record. The final text is a
    // useful fallback terminal signal, but it must not split that feedback into
    // a synthetic turn. Explicit completion, abort, and rollback events remain
    // authoritative and are never reopened here.
    tx.execute(
        "UPDATE turns
         SET status='running',completed_at=NULL
         WHERE id=?1 AND status='completed'
           AND EXISTS(
               SELECT 1 FROM events
               WHERE turn_id=?1 AND kind='turn_started'
           )
           AND NOT EXISTS(
               SELECT 1 FROM events
               WHERE turn_id=?1
                 AND (
                     kind='turn_completed'
                     OR (kind='state' AND status IN ('interrupted','rolled_back'))
                 )
           )",
        [turn_id],
    )?;
    Ok(())
}

fn complete_turn_from_final(
    tx: &Transaction<'_>,
    state: &CursorState,
    timestamp: &str,
    content: &str,
) -> Result<()> {
    let Some(turn_id) = state.current_turn.as_deref() else {
        return Ok(());
    };
    let content = redact_data_urls(content);
    if turn_has_open_native_lifecycle(tx, turn_id) {
        // Native tasks may accept steering after emitting final-answer text.
        // Preserve the useful fallback message, but let the explicit task
        // lifecycle decide when the turn becomes terminal.
        tx.execute(
            "UPDATE turns SET last_agent_message=?1 WHERE id=?2",
            params![content, turn_id],
        )?;
        return Ok(());
    }
    // Legacy protocols have no explicit task lifecycle. Their final assistant
    // output is therefore the durable terminal record. Only promote an open
    // turn so a prior abort/rollback remains authoritative.
    tx.execute(
        "UPDATE turns
         SET completed_at=?1,status='completed',last_agent_message=?2
         WHERE id=?3 AND status='running'",
        params![timestamp, content, turn_id],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_event(
    tx: &Transaction<'_>,
    state: &CursorState,
    line: u64,
    timestamp: &str,
    kind: &str,
    role: Option<&str>,
    label: Option<&str>,
    body: Option<&str>,
    status: Option<&str>,
    tool_name: Option<&str>,
    duration_ms: Option<i64>,
    payload: &Value,
) -> Result<()> {
    let duration_ms = bounded_duration_ms(duration_ms);
    let compact_metadata_kind = matches!(kind, "subagent" | "goal" | "plan" | "state");
    let message_payload = payload.get("type").and_then(Value::as_str) == Some("message");
    let raw_call_id = (!compact_metadata_kind)
        .then(|| {
            if message_payload {
                payload.get("id").and_then(Value::as_str)
            } else {
                payload
                    .get("call_id")
                    .or_else(|| payload.get("id"))
                    .and_then(Value::as_str)
            }
        })
        .flatten();
    let call_id = normalized_relational_identifier(raw_call_id, "event call id")?.map(|call_id| {
        if message_payload {
            projected_message_id(&state.owner_id, &call_id)
        } else {
            call_id
        }
    });
    // Compaction replacement history is already durable in the source JSONL
    // and can be hundreds of megabytes. The projection needs only the visible
    // boundary/summary and a few identifiers, so enforce that invariant here
    // for every current (and future) compaction call site.
    let compaction = (kind == "compaction").then(|| compact_compaction(payload));
    let normalized_body = if let Some((summary, _)) = compaction.as_ref() {
        Some(summary.as_str())
    } else if matches!(
        kind,
        "message" | "final" | "tool_call" | "tool_output" | "tool_completed"
    ) {
        None
    } else {
        body
    };
    let redacted_label = label.map(|value| {
        if compact_metadata_kind {
            redact_and_bound(value, PROJECTED_EVENT_LABEL_CHARS)
        } else {
            redact_data_urls(value)
        }
    });
    let redacted_body = normalized_body.map(|value| {
        if compact_metadata_kind {
            redact_and_bound(value, PROJECTED_EVENT_BODY_CHARS)
        } else {
            redact_data_urls(value)
        }
    });
    let redacted_status = status.map(|value| {
        if compact_metadata_kind {
            redact_and_bound(value, PROJECTED_IDENTIFIER_CHARS)
        } else {
            redact_data_urls(value)
        }
    });
    let redacted_tool_name = tool_name.map(redact_data_urls);
    let compact_metadata = compact_metadata_kind
        .then(|| compact_projected_metadata(kind, payload))
        .transpose()?
        .flatten();
    let payload_json = if let Some((_, metadata)) = compaction.as_ref() {
        Some(serialize_redacted_json(metadata)?)
    } else if kind == "system" {
        compact_unknown_metadata(payload)
            .as_ref()
            .map(serialize_redacted_json)
            .transpose()?
    } else if let Some(metadata) = compact_metadata.as_ref() {
        Some(serialize_redacted_json(metadata)?)
    } else {
        None
    };
    tx.execute(
        "INSERT OR IGNORE INTO events(
            id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
            kind,role,label,body,status,tool_name,call_id,duration_ms,model,effort,payload_json,native
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,1)",
        params![
            event_id(state, line),
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            line as i64,
            kind,
            role,
            redacted_label.as_deref(),
            redacted_body.as_deref(),
            redacted_status.as_deref(),
            redacted_tool_name.as_deref(),
            call_id,
            duration_ms,
            state.current_model,
            state.current_effort,
            payload_json,
        ],
    )?;
    Ok(())
}

fn compact_projected_metadata(kind: &str, payload: &Value) -> Result<Option<Value>> {
    if kind != "subagent" {
        return Ok(None);
    }
    let Some(agent_thread_id) = normalized_relational_identifier(
        payload.get("agent_thread_id").and_then(Value::as_str),
        "subagent thread id",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::json!({"agent_thread_id": agent_thread_id}),
    ))
}

fn redact_and_bound(value: &str, max_chars: usize) -> String {
    let value = redact_data_urls(value);
    let mut chars = value.chars();
    let mut bounded = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}

fn normalized_metadata_value(value: Option<&str>, max_chars: usize) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| redact_and_bound(value, max_chars))
}

fn normalized_relational_identifier(value: Option<&str>, label: &str) -> Result<Option<String>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.chars().count() > PROJECTED_IDENTIFIER_CHARS {
        return Err(anyhow!(
            "{label} exceeds the {PROJECTED_IDENTIFIER_CHARS}-character identifier limit"
        ));
    }
    if value.chars().any(char::is_control) || redact_data_urls(value) != value {
        return Err(anyhow!("{label} contains invalid identifier content"));
    }
    Ok(Some(value.to_owned()))
}

fn required_metadata_identifier(value: Option<&str>, label: &str) -> Result<String> {
    normalized_relational_identifier(value, label)?
        .ok_or_else(|| anyhow!("first session_meta has no {label}"))
}

fn normalized_session_metadata(payload: &Value) -> SessionMetadata {
    let cwd = normalized_metadata_value(
        payload.get("cwd").and_then(Value::as_str),
        PROJECTED_SESSION_PATH_CHARS,
    );
    let project = cwd
        .as_deref()
        .and_then(|value| Path::new(value).file_name()?.to_str())
        .and_then(|value| normalized_metadata_value(Some(value), PROJECTED_EVENT_LABEL_CHARS));
    let git = payload.get("git").unwrap_or(&Value::Null);
    let subagent = payload
        .get("source")
        .and_then(|value| value.get("subagent"));
    let source = normalized_metadata_value(
        payload.get("source").and_then(Value::as_str),
        PROJECTED_IDENTIFIER_CHARS,
    )
    .or_else(|| subagent.map(|_| "subagent".to_owned()));
    SessionMetadata {
        cwd,
        project,
        repository_url: normalized_metadata_value(
            git.get("repository_url").and_then(Value::as_str),
            PROJECTED_SESSION_PATH_CHARS,
        ),
        branch: normalized_metadata_value(
            git.get("branch").and_then(Value::as_str),
            PROJECTED_IDENTIFIER_CHARS,
        ),
        source,
        thread_source: normalized_metadata_value(
            payload.get("thread_source").and_then(Value::as_str),
            PROJECTED_IDENTIFIER_CHARS,
        ),
    }
}

fn compact_unknown_metadata(payload: &Value) -> Option<Value> {
    let payload = payload.as_object()?;
    let mut metadata = Map::new();
    for key in [
        "type",
        "schema_version",
        "version",
        "id",
        "call_id",
        "status",
    ] {
        let Some(value) = payload.get(key) else {
            continue;
        };
        let value = match value {
            Value::Bool(_) | Value::Number(_) => value.clone(),
            Value::String(value) => Value::String(
                redact_data_urls(value)
                    .chars()
                    .take(UNKNOWN_METADATA_STRING_CHARS)
                    .collect(),
            ),
            _ => continue,
        };
        metadata.insert(key.to_owned(), value);
    }
    (!metadata.is_empty()).then_some(Value::Object(metadata))
}

fn compact_compaction(payload: &Value) -> (String, Value) {
    let summary = payload
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Conversation context was compacted.");
    let mut summary_chars = summary.chars();
    let mut summary = summary_chars.by_ref().take(16_384).collect::<String>();
    if summary_chars.next().is_some() {
        summary.push('…');
    }
    let mut metadata = serde_json::Map::new();
    if let Some(count) = payload
        .get("replacement_history")
        .and_then(Value::as_array)
        .map(Vec::len)
    {
        metadata.insert(
            "replacement_history_count".into(),
            Value::from(count as u64),
        );
    }
    for key in [
        "window_number",
        "first_window_id",
        "previous_window_id",
        "window_id",
    ] {
        let Some(value) = payload.get(key) else {
            continue;
        };
        let compact_value = match value {
            Value::String(value) => Value::String(value.chars().take(256).collect()),
            Value::Number(_) | Value::Bool(_) => value.clone(),
            _ => continue,
        };
        metadata.insert(key.into(), compact_value);
    }
    (summary, Value::Object(metadata))
}

#[allow(clippy::too_many_arguments)]
fn upsert_tool_call(
    tx: &Transaction<'_>,
    state: &CursorState,
    timestamp: &str,
    call_id: &str,
    name: &str,
    namespace: Option<&str>,
    payload: &Value,
) -> Result<()> {
    let name = redact_data_urls(name);
    let namespace = namespace.map(redact_data_urls);
    let status = payload
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("running");
    tx.execute(
        "INSERT INTO tool_calls(
            id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
            namespace,name,status
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
         ON CONFLICT(rollout_id,call_id) DO UPDATE SET
            namespace=COALESCE(excluded.namespace,tool_calls.namespace),
            name=excluded.name,
            turn_id=COALESCE(tool_calls.turn_id,excluded.turn_id),
            status=CASE
                WHEN tool_calls.status IN ('failed','cancelled','canceled') THEN tool_calls.status
                WHEN excluded.status IN ('failed','cancelled','canceled') THEN excluded.status
                WHEN tool_calls.status='completed' THEN tool_calls.status
                WHEN excluded.status='completed' THEN excluded.status
                ELSE tool_calls.status END",
        params![
            format!("{}:{call_id}", state.owner_id),
            call_id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            namespace.as_deref(),
            name,
            status,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn complete_tool_call(
    tx: &Transaction<'_>,
    state: &CursorState,
    timestamp: &str,
    call_id: &str,
    status: Option<&str>,
    duration_ms: Option<i64>,
    tool_name_hint: Option<&str>,
) -> Result<()> {
    let duration_ms = bounded_duration_ms(duration_ms);
    let completion_status = status.unwrap_or("completed");
    let tool_name = redact_data_urls(tool_name_hint.unwrap_or("unknown"));
    tx.execute(
        "INSERT INTO tool_calls(
            id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
            completed_at,name,status,duration_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8,?9,?10)
         ON CONFLICT(rollout_id,call_id) DO UPDATE SET
            completed_at=CASE WHEN ?11 THEN excluded.completed_at
                              ELSE COALESCE(tool_calls.completed_at,excluded.completed_at) END,
            name=CASE WHEN tool_calls.name='unknown' THEN excluded.name ELSE tool_calls.name END,
            status=CASE
                WHEN tool_calls.status IN ('failed','cancelled','canceled') THEN tool_calls.status
                WHEN excluded.status IN ('failed','cancelled','canceled') THEN excluded.status
                WHEN tool_calls.status='completed' THEN tool_calls.status
                WHEN excluded.status='completed' THEN excluded.status
                ELSE tool_calls.status END,
            duration_ms=CASE WHEN ?11 AND excluded.duration_ms IS NOT NULL
                             THEN excluded.duration_ms
                             ELSE COALESCE(tool_calls.duration_ms,excluded.duration_ms) END",
        params![
            format!("{}:{call_id}", state.owner_id),
            call_id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            tool_name,
            completion_status,
            duration_ms,
            status.is_some(),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn enrich_tool_call(
    tx: &Transaction<'_>,
    state: &CursorState,
    timestamp: &str,
    call_id: &str,
    name: &str,
    status: &str,
    duration_ms: Option<i64>,
) -> Result<()> {
    let duration_ms = bounded_duration_ms(duration_ms);
    let name = redact_data_urls(name);
    tx.execute(
        "INSERT INTO tool_calls(
            id,call_id,thread_id,rollout_id,turn_id,agent_run_id,started_at,
            completed_at,name,status,duration_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8,?9,?10)
         ON CONFLICT(rollout_id,call_id) DO UPDATE SET
            completed_at=COALESCE(tool_calls.completed_at,excluded.completed_at),
            name=CASE WHEN tool_calls.name='unknown' THEN excluded.name ELSE tool_calls.name END,
            status=CASE
                WHEN tool_calls.status IN ('failed','cancelled','canceled') THEN tool_calls.status
                WHEN excluded.status IN ('failed','cancelled','canceled') THEN excluded.status
                WHEN tool_calls.status='completed' THEN tool_calls.status
                WHEN excluded.status='completed' THEN excluded.status
                ELSE tool_calls.status END,
            duration_ms=COALESCE(excluded.duration_ms,tool_calls.duration_ms)",
        params![
            format!("{}:{call_id}", state.owner_id),
            call_id,
            state.thread_id,
            state.owner_id,
            state.current_turn,
            state.owner_id,
            timestamp,
            name,
            status,
            duration_ms,
        ],
    )?;
    Ok(())
}

fn touch_owner(tx: &Transaction<'_>, state: &CursorState, timestamp: &str) -> Result<()> {
    tx.execute(
        "UPDATE threads SET last_event_at=MAX(last_event_at,?1) WHERE id=?2",
        params![timestamp, state.thread_id],
    )?;
    tx.execute(
        "UPDATE rollouts SET last_event_at=MAX(last_event_at,?1) WHERE id=?2",
        params![timestamp, state.owner_id],
    )?;
    Ok(())
}

fn update_owner_metadata(
    tx: &Transaction<'_>,
    state: &CursorState,
    payload: &Value,
    timestamp: &str,
) -> Result<()> {
    let is_root = state.owner_id == state.thread_id;
    let metadata = normalized_session_metadata(payload);
    tx.execute(
        "UPDATE threads SET
            cwd=CASE WHEN ?1=1 THEN ?2
                     WHEN root_metadata_seen=0 THEN COALESCE(cwd,?2) ELSE cwd END,
            project=CASE WHEN ?1=1 THEN ?3
                         WHEN root_metadata_seen=0 THEN COALESCE(project,?3) ELSE project END,
            repository_url=CASE WHEN ?1=1 THEN ?4
                                WHEN root_metadata_seen=0 THEN COALESCE(repository_url,?4)
                                ELSE repository_url END,
            branch=CASE WHEN ?1=1 THEN ?5
                        WHEN root_metadata_seen=0 THEN COALESCE(branch,?5) ELSE branch END,
            source=CASE WHEN ?1=1 THEN ?6
                        WHEN root_metadata_seen=0 THEN COALESCE(source,?6) ELSE source END,
            thread_source=CASE WHEN ?1=1 THEN ?7
                               WHEN root_metadata_seen=0 THEN COALESCE(thread_source,?7)
                               ELSE thread_source END,
            root_metadata_seen=MAX(root_metadata_seen,?1),
            last_event_at=MAX(last_event_at,?8)
         WHERE id=?9",
        params![
            is_root as i64,
            metadata.cwd,
            metadata.project,
            metadata.repository_url,
            metadata.branch,
            metadata.source,
            metadata.thread_source,
            timestamp,
            state.thread_id,
        ],
    )?;
    if is_root
        && let Some(title) = payload
            .get("thread_name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| redact_and_bound(value, PROJECTED_SESSION_TITLE_CHARS))
    {
        tx.execute(
            "UPDATE threads SET title=?1,title_updated_at=?2
             WHERE id=?3 AND (title_updated_at IS NULL OR title_updated_at<=?2)",
            params![title, timestamp, state.thread_id],
        )?;
    }
    Ok(())
}

fn upsert_owner(tx: &Transaction<'_>, owner: &OwnerMeta, archived: bool) -> Result<()> {
    let is_root = owner.owner_id == owner.thread_id;
    tx.execute(
        "INSERT INTO threads(
            id,cwd,project,repository_url,branch,source,thread_source,source_json,
            started_at,last_event_at,root_metadata_seen
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?9,?10)
         ON CONFLICT(id) DO UPDATE SET
            cwd=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.cwd
                     WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.cwd,excluded.cwd)
                     ELSE threads.cwd END,
            project=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.project
                         WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.project,excluded.project)
                         ELSE threads.project END,
            repository_url=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.repository_url
                                WHEN threads.root_metadata_seen=0
                                THEN COALESCE(threads.repository_url,excluded.repository_url)
                                ELSE threads.repository_url END,
            branch=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.branch
                        WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.branch,excluded.branch)
                        ELSE threads.branch END,
            source=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.source
                        WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.source,excluded.source)
                        ELSE threads.source END,
            thread_source=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.thread_source
                               WHEN threads.root_metadata_seen=0
                               THEN COALESCE(threads.thread_source,excluded.thread_source)
                               ELSE threads.thread_source END,
            source_json=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.source_json
                             WHEN threads.root_metadata_seen=0
                             THEN COALESCE(threads.source_json,excluded.source_json)
                             ELSE threads.source_json END,
            root_metadata_seen=MAX(threads.root_metadata_seen,excluded.root_metadata_seen),
            started_at=MIN(threads.started_at,excluded.started_at),
            last_event_at=MAX(threads.last_event_at,excluded.last_event_at)",
        params![
            owner.thread_id,
            owner.cwd,
            owner.project,
            owner.repository_url,
            owner.branch,
            owner.source,
            owner.thread_source,
            owner.source_json,
            owner.timestamp,
            is_root as i64,
        ],
    )?;
    tx.execute(
        "INSERT INTO rollouts(
            id,thread_id,parent_rollout_id,parent_thread_id,agent_path,agent_nickname,
            cwd,started_at,last_event_at,archived
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8,?9)
         ON CONFLICT(id) DO UPDATE SET
            thread_id=excluded.thread_id,parent_rollout_id=excluded.parent_rollout_id,
            parent_thread_id=excluded.parent_thread_id,agent_path=excluded.agent_path,
            agent_nickname=excluded.agent_nickname,cwd=COALESCE(excluded.cwd,rollouts.cwd),
            archived=excluded.archived",
        params![
            owner.owner_id,
            owner.thread_id,
            owner.parent_rollout_id,
            owner.parent_thread_id,
            owner.agent_path,
            owner.agent_nickname,
            owner.cwd,
            owner.timestamp,
            archived as i64,
        ],
    )?;
    tx.execute(
        "INSERT INTO agent_runs(
            id,thread_id,rollout_id,parent_rollout_id,agent_path,nickname,started_at,status
         ) VALUES(?1,?2,?1,?3,?4,?5,?6,'running')
         ON CONFLICT(id) DO UPDATE SET
            thread_id=excluded.thread_id,rollout_id=excluded.rollout_id,
            parent_rollout_id=excluded.parent_rollout_id,
            agent_path=excluded.agent_path,nickname=excluded.nickname,
            status=CASE
                WHEN agent_runs.rollout_id IS NULL
                 AND agent_runs.completed_at IS NOT NULL
                 AND excluded.started_at>agent_runs.completed_at
                THEN 'running'
                ELSE agent_runs.status END,
            completed_at=CASE
                WHEN agent_runs.rollout_id IS NULL
                 AND agent_runs.completed_at IS NOT NULL
                 AND excluded.started_at>agent_runs.completed_at
                THEN NULL
                ELSE agent_runs.completed_at END",
        params![
            owner.owner_id,
            owner.thread_id,
            owner.parent_rollout_id,
            owner.agent_path,
            owner.agent_nickname,
            owner.timestamp,
        ],
    )?;
    // A rebuild can legitimately shrink the surviving rollout interval. The
    // thread row is a projection of its current rollouts, not a lifetime
    // high-water mark, so restore the exact aggregate after every owner upsert.
    recompute_thread_bounds(tx, &owner.thread_id)?;
    Ok(())
}

fn upsert_observed_agent(
    tx: &Transaction<'_>,
    agent_id: &str,
    thread_id: &str,
    parent_rollout_id: &str,
    agent_path: Option<&str>,
    timestamp: &str,
    activity: &str,
) -> Result<()> {
    let Some(agent_id) = normalized_relational_identifier(Some(agent_id), "agent thread id")?
    else {
        return Ok(());
    };
    let Some(thread_id) = normalized_relational_identifier(Some(thread_id), "thread id")? else {
        return Ok(());
    };
    let Some(parent_rollout_id) =
        normalized_relational_identifier(Some(parent_rollout_id), "parent rollout id")?
    else {
        return Ok(());
    };
    let agent_path = normalized_metadata_value(agent_path, PROJECTED_SESSION_PATH_CHARS);
    let status = match activity {
        "completed" => "completed",
        "interrupted" => "interrupted",
        "rolled_back" => "rolled_back",
        _ => "running",
    };
    let completed_at =
        matches!(status, "completed" | "interrupted" | "rolled_back").then_some(timestamp);
    let existing = tx
        .query_row(
            "SELECT rollout_id,status,started_at
             FROM agent_runs WHERE id=?1",
            [&agent_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((rollout_id, current_status, started_at)) = existing else {
        tx.execute(
            "INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,agent_path,started_at,status,completed_at
             ) VALUES(?1,?2,NULL,?3,?4,?5,?6,?7)",
            params![
                agent_id,
                thread_id,
                parent_rollout_id,
                agent_path,
                timestamp,
                status,
                completed_at,
            ],
        )?;
        return Ok(());
    };

    if let Some(rollout_id) = rollout_id {
        let parent_terminal_is_authoritative = completed_at.is_some()
            && current_status == "running"
            && timestamp >= started_at.as_str()
            && tx.query_row(
                "SELECT COALESCE(
                        (SELECT MAX(timestamp) FROM events WHERE rollout_id=?1),
                        ?2
                    )<=?3",
                params![rollout_id, started_at, timestamp],
                |row| row.get::<_, i64>(0),
            )? != 0;
        tx.execute(
            "UPDATE agent_runs SET
                agent_path=COALESCE(?1,agent_path),
                status=CASE WHEN ?2=1 THEN ?3 ELSE status END,
                completed_at=CASE WHEN ?2=1 THEN ?4 ELSE completed_at END
             WHERE id=?5",
            params![
                agent_path,
                parent_terminal_is_authoritative as i64,
                status,
                completed_at,
                agent_id,
            ],
        )?;
        if parent_terminal_is_authoritative {
            tx.execute(
                "UPDATE turns
                 SET status=?1,completed_at=?2
                 WHERE rollout_id=?3
                   AND agent_run_id=?4
                   AND status='running'
                   AND started_at<=?2
                   AND EXISTS(
                       SELECT 1 FROM events e
                       WHERE e.turn_id=turns.id AND e.kind='turn_started'
                   )
                   AND NOT EXISTS(
                       SELECT 1 FROM events e
                       WHERE e.turn_id=turns.id
                         AND (
                           e.kind='turn_completed'
                           OR (
                             e.kind='state'
                             AND e.status IN ('interrupted','rolled_back')
                           )
                         )
                   )
                   AND COALESCE(
                       (SELECT MAX(e.timestamp) FROM events e WHERE e.turn_id=turns.id),
                       turns.started_at
                   )<=?2",
                params![status, timestamp, rollout_id, agent_id],
            )?;
        }
        return Ok(());
    }

    let is_latest_observation = tx.query_row(
        "SELECT NOT EXISTS(
            SELECT 1 FROM events
            WHERE kind='subagent'
              AND json_extract(payload_json,'$.agent_thread_id')=?1
              AND timestamp>?2
         )",
        params![agent_id, timestamp],
        |row| row.get::<_, i64>(0),
    )? != 0;
    tx.execute(
        "UPDATE agent_runs SET
            started_at=MIN(started_at,?1),
            agent_path=COALESCE(?2,agent_path),
            thread_id=CASE WHEN ?3=1 THEN ?4 ELSE thread_id END,
            parent_rollout_id=CASE WHEN ?3=1 THEN ?5 ELSE parent_rollout_id END,
            status=CASE WHEN ?3=1 THEN ?6 ELSE status END,
            completed_at=CASE WHEN ?3=1 THEN ?7 ELSE completed_at END
         WHERE id=?8",
        params![
            timestamp,
            agent_path,
            is_latest_observation as i64,
            thread_id,
            parent_rollout_id,
            status,
            completed_at,
            agent_id,
        ],
    )?;
    Ok(())
}

fn clear_rollout(tx: &Transaction<'_>, rollout_id: &str) -> Result<Option<String>> {
    let thread_id = tx
        .query_row(
            "SELECT thread_id FROM rollouts WHERE id=?1",
            [rollout_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let mut affected_agent_ids = {
        let mut statement = tx.prepare(
            "SELECT DISTINCT json_extract(payload_json,'$.agent_thread_id')
             FROM events
             WHERE rollout_id=?1 AND kind='subagent'
               AND json_extract(payload_json,'$.agent_thread_id') IS NOT NULL",
        )?;
        statement
            .query_map([rollout_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    if !affected_agent_ids
        .iter()
        .any(|agent_id| agent_id == rollout_id)
    {
        affected_agent_ids.push(rollout_id.to_owned());
    }
    tx.execute("DELETE FROM usage_facts WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM events WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM messages WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM tool_calls WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM turns WHERE rollout_id=?1", [rollout_id])?;
    // Parent rollout events can create a lightweight child-agent row before
    // that child has its own rollout. Those rows deliberately have no rollout
    // foreign key, so a parent rebuild must remove them explicitly rather
    // than preserve observations that no longer exist in the source.
    tx.execute(
        "DELETE FROM agent_runs
         WHERE rollout_id IS NULL AND parent_rollout_id=?1",
        [rollout_id],
    )?;
    tx.execute("DELETE FROM agent_runs WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM rollouts WHERE id=?1", [rollout_id])?;
    affected_agent_ids.sort();
    affected_agent_ids.dedup();
    for agent_id in affected_agent_ids {
        rematerialize_surviving_agent_observation(tx, &agent_id)?;
    }
    tx.execute(
        "DELETE FROM app_meta WHERE key=?1",
        [pending_source_shrink_key(rollout_id)],
    )?;
    if let Some(thread_id) = thread_id.as_deref() {
        recompute_thread_bounds(tx, thread_id)?;
        if rollout_id == thread_id {
            recompute_thread_metadata(tx, thread_id)?;
        }
    }
    Ok(thread_id)
}

fn rematerialize_surviving_agent_observation(tx: &Transaction<'_>, agent_id: &str) -> Result<()> {
    // A synthetic row is wholly derived from parent observations. Rebuild it
    // from zero before replaying the surviving evidence: merging into the old
    // row with MIN(started_at, ...) cannot move its start later when the
    // removed parent supplied the earliest observation but was not the latest
    // (and therefore not the row's current parent). Promoted rows keep their
    // native rollout identity and are reset through the native lifecycle path.
    tx.execute(
        "DELETE FROM agent_runs WHERE id=?1 AND rollout_id IS NULL",
        [agent_id],
    )?;
    restore_promoted_agent_native_state(tx, agent_id)?;
    let observations = {
        let mut statement = tx.prepare(
            "SELECT e.thread_id,e.rollout_id,e.body,e.timestamp,COALESCE(e.status,'running')
             FROM events e
             LEFT JOIN source_files sf ON sf.rollout_id=e.rollout_id
             WHERE e.kind='subagent'
               AND json_extract(e.payload_json,'$.agent_thread_id')=?1
             ORDER BY e.timestamp,COALESCE(sf.path,''),e.source_line,e.id",
        )?;
        statement
            .query_map([agent_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (thread_id, parent_rollout_id, agent_path, timestamp, activity) in observations {
        upsert_observed_agent(
            tx,
            agent_id,
            &thread_id,
            &parent_rollout_id,
            agent_path.as_deref(),
            &timestamp,
            &activity,
        )?;
    }
    Ok(())
}

fn rematerialize_observed_children(tx: &Transaction<'_>, rollout_id: &str) -> Result<()> {
    let agent_ids = {
        let mut statement = tx.prepare(
            "SELECT DISTINCT json_extract(payload_json,'$.agent_thread_id')
             FROM events
             WHERE rollout_id=?1 AND kind='subagent'
               AND json_extract(payload_json,'$.agent_thread_id') IS NOT NULL
             ORDER BY 1",
        )?;
        statement
            .query_map([rollout_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for agent_id in agent_ids {
        rematerialize_surviving_agent_observation(tx, &agent_id)?;
    }
    Ok(())
}

fn restore_promoted_agent_native_state(tx: &Transaction<'_>, agent_id: &str) -> Result<()> {
    let native = tx
        .query_row(
            "SELECT
                r.id,r.thread_id,r.parent_rollout_id,r.agent_path,r.agent_nickname,r.started_at
             FROM agent_runs a
             JOIN rollouts r ON r.id=a.rollout_id
             WHERE a.id=?1 AND a.rollout_id IS NOT NULL",
            [agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((rollout_id, thread_id, parent_rollout_id, agent_path, nickname, started_at)) = native
    else {
        return Ok(());
    };
    // Parent observations may have created the row before the native rollout
    // was discovered, or overwritten its path afterward. Reset every native
    // metadata field from the durable rollout before replaying any surviving
    // parent evidence so removed observations cannot remain as high-water
    // marks in the promoted row.
    tx.execute(
        "UPDATE agent_runs SET
            thread_id=?1,parent_rollout_id=?2,agent_path=?3,nickname=?4,started_at=?5
         WHERE id=?6 AND rollout_id=?7",
        params![
            thread_id,
            parent_rollout_id,
            agent_path,
            nickname,
            started_at,
            agent_id,
            rollout_id,
        ],
    )?;
    let turn_lifecycles = {
        let mut statement = tx.prepare(
            "SELECT
                t.id,
                CASE
                    WHEN e.kind='turn_started' THEN 'running'
                    WHEN e.kind='turn_completed' THEN 'completed'
                    ELSE e.status
                END,
                CASE WHEN e.kind='turn_started' THEN NULL ELSE e.timestamp END
             FROM turns t
             JOIN events e ON e.id=(
                 SELECT e2.id
                 FROM events e2
                 WHERE e2.turn_id=t.id
                   AND (
                     e2.kind IN ('turn_started','turn_completed')
                     OR (
                       e2.kind='state'
                       AND e2.status IN ('interrupted','rolled_back')
                     )
                   )
                 ORDER BY e2.timestamp DESC,e2.source_line DESC,e2.id DESC
                 LIMIT 1
             )
             WHERE t.rollout_id=?1 AND t.agent_run_id=?2",
        )?;
        statement
            .query_map(params![rollout_id, agent_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (turn_id, status, completed_at) in turn_lifecycles {
        tx.execute(
            "UPDATE turns SET status=?1,completed_at=?2 WHERE id=?3",
            params![status, completed_at, turn_id],
        )?;
    }
    let lifecycle = tx
        .query_row(
            "SELECT
                CASE
                    WHEN kind='turn_started' THEN 'running'
                    WHEN kind='turn_completed' THEN 'completed'
                    ELSE status
                END,
                CASE WHEN kind='turn_started' THEN NULL ELSE timestamp END
             FROM events INDEXED BY idx_events_activity_owner
             WHERE thread_id=?1 AND rollout_id=?2
               AND (
                    kind IN ('turn_started','turn_completed')
                    OR (kind='state' AND status IN ('interrupted','rolled_back'))
               )
             ORDER BY timestamp DESC,source_line DESC,id DESC
             LIMIT 1",
            params![thread_id, rollout_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
        .unwrap_or_else(|| ("running".into(), None));
    tx.execute(
        "UPDATE agent_runs SET status=?1,completed_at=?2
         WHERE id=?3 AND rollout_id=?4",
        params![lifecycle.0, lifecycle.1, agent_id, rollout_id],
    )?;
    Ok(())
}

fn delete_thread_if_abandoned(tx: &Transaction<'_>, thread_id: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM threads WHERE id=?1
         AND NOT EXISTS(SELECT 1 FROM rollouts WHERE thread_id=?1)",
        [thread_id],
    )?;
    Ok(())
}

fn recompute_thread_bounds(tx: &Transaction<'_>, thread_id: &str) -> Result<()> {
    tx.execute(
        "UPDATE threads SET
            started_at=(SELECT MIN(started_at) FROM rollouts WHERE thread_id=?1),
            last_event_at=(SELECT MAX(last_event_at) FROM rollouts WHERE thread_id=?1)
         WHERE id=?1 AND EXISTS(SELECT 1 FROM rollouts WHERE thread_id=?1)",
        [thread_id],
    )?;
    Ok(())
}

fn recompute_thread_metadata(tx: &Transaction<'_>, thread_id: &str) -> Result<()> {
    let surviving_sources = {
        let mut statement = tx.prepare(
            "SELECT sf.path
             FROM rollouts r
             JOIN source_files sf ON sf.rollout_id=r.id
             WHERE r.thread_id=?1
             ORDER BY sf.path,r.id",
        )?;
        statement
            .query_map([thread_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };

    let mut cwd = None;
    let mut project = None;
    let mut repository_url = None;
    let mut branch = None;
    let mut source = None;
    let mut thread_source = None;
    for path in surviving_sources {
        let Ok(owner) = peek_owner(Path::new(&path)) else {
            continue;
        };
        cwd = cwd.or(owner.cwd);
        project = project.or(owner.project);
        repository_url = repository_url.or(owner.repository_url);
        branch = branch.or(owner.branch);
        source = source.or(owner.source);
        thread_source = thread_source.or(owner.thread_source);
    }

    tx.execute(
        "UPDATE threads SET
            title=NULL,title_updated_at=NULL,
            cwd=?1,project=?2,repository_url=?3,branch=?4,source=?5,thread_source=?6,
            source_json=NULL,root_metadata_seen=0
         WHERE id=?7",
        params![
            cwd,
            project,
            repository_url,
            branch,
            source,
            thread_source,
            thread_id,
        ],
    )?;
    Ok(())
}

fn load_checkpoint(connection: &Connection, rollout_id: &str) -> Result<Option<SourceCheckpoint>> {
    connection
        .query_row(
            "SELECT archived,size_bytes,modified_ns,ctime_ns,device_id,inode,content_fingerprint,
                    byte_offset,line_number,inherited_lines,parse_state_json,last_error
             FROM source_files WHERE rollout_id=?1",
            [rollout_id],
            |row| {
                let state_json: String = row.get(10)?;
                Ok(SourceCheckpoint {
                    archived: row.get::<_, i64>(0)? != 0,
                    size: row.get::<_, i64>(1)?.max(0) as u64,
                    modified_ns: row.get::<_, i64>(2)?.max(0) as u64,
                    identity: FileIdentity {
                        ctime_ns: row.get(3)?,
                        device_id: row.get(4)?,
                        inode: row.get(5)?,
                    },
                    fingerprint: row.get(6)?,
                    offset: row.get::<_, i64>(7)?.max(0) as u64,
                    line_number: row.get::<_, i64>(8)?.max(0) as u64,
                    inherited_lines: row.get::<_, i64>(9)?.max(0) as u64,
                    last_error: row.get(11)?,
                    state: serde_json::from_str(&state_json).unwrap_or_default(),
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn load_checkpoint_by_path(
    connection: &Connection,
    path: &str,
) -> Result<Option<SourceCheckpoint>> {
    connection
        .query_row(
            "SELECT archived,size_bytes,modified_ns,ctime_ns,device_id,inode,content_fingerprint,
                    byte_offset,line_number,inherited_lines,parse_state_json,last_error
             FROM source_files WHERE path=?1",
            [path],
            |row| {
                let state_json: String = row.get(10)?;
                Ok(SourceCheckpoint {
                    archived: row.get::<_, i64>(0)? != 0,
                    size: row.get::<_, i64>(1)?.max(0) as u64,
                    modified_ns: row.get::<_, i64>(2)?.max(0) as u64,
                    identity: FileIdentity {
                        ctime_ns: row.get(3)?,
                        device_id: row.get(4)?,
                        inode: row.get(5)?,
                    },
                    fingerprint: row.get(6)?,
                    offset: row.get::<_, i64>(7)?.max(0) as u64,
                    line_number: row.get::<_, i64>(8)?.max(0) as u64,
                    inherited_lines: row.get::<_, i64>(9)?.max(0) as u64,
                    last_error: row.get(11)?,
                    state: serde_json::from_str(&state_json).unwrap_or_default(),
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn pending_source_shrink_key(owner_id: &str) -> String {
    format!("pending_source_shrink:{owner_id}")
}

fn clear_pending_source_shrink(connection: &Connection, owner_id: &str) -> Result<()> {
    connection.execute(
        "DELETE FROM app_meta WHERE key=?1",
        [pending_source_shrink_key(owner_id)],
    )?;
    Ok(())
}

fn same_source_shrink_was_observed(
    connection: &Connection,
    owner_id: &str,
    path: &str,
    size: u64,
    fingerprint: &str,
) -> Result<bool> {
    let key = pending_source_shrink_key(owner_id);
    let candidate = PendingSourceShrink {
        path: path.to_owned(),
        size,
        content_digest: source_content_digest(fingerprint),
    };
    let previous = connection
        .query_row("SELECT value FROM app_meta WHERE key=?1", [&key], |row| {
            row.get::<_, String>(0)
        })
        .optional()?
        .and_then(|value| serde_json::from_str::<PendingSourceShrink>(&value).ok());
    if previous.as_ref() == Some(&candidate) {
        return Ok(true);
    }
    connection.execute(
        "INSERT INTO app_meta(key,value) VALUES(?1,?2)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![key, serde_json::to_string(&candidate)?],
    )?;
    Ok(false)
}

fn source_content_digest(fingerprint: &str) -> String {
    let mut hasher = Sha256::new();
    if let Some(fingerprint) = ChunkedFingerprint::parse(fingerprint) {
        hasher.update(fingerprint.size.to_le_bytes());
        hasher.update(fingerprint.chunk_bytes.to_le_bytes());
        for chunk in fingerprint.chunks {
            hasher.update(chunk.as_bytes());
        }
    } else {
        hasher.update(fingerprint.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn peek_owner(path: &Path) -> Result<OwnerMeta> {
    let mut file = File::open(path)?;
    peek_owner_from_file(&mut file, path)
}

fn peek_owner_from_file(file: &mut File, path: &Path) -> Result<OwnerMeta> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    loop {
        match read_bounded_line(&mut reader, &mut line, MAX_JSONL_LINE_BYTES)? {
            BoundedLine::Eof | BoundedLine::Incomplete { .. } => break,
            BoundedLine::Complete {
                oversized: true, ..
            } => continue,
            BoundedLine::Complete {
                oversized: false, ..
            } => {}
        }
        let value: Value = match serde_json::from_slice(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let legacy_meta =
            value.get("type").is_none() && value.get("id").and_then(Value::as_str).is_some();
        if value.get("type").and_then(Value::as_str) != Some("session_meta") && !legacy_meta {
            continue;
        }
        let payload = value.get("payload").unwrap_or(&value);
        let owner_id =
            required_metadata_identifier(payload.get("id").and_then(Value::as_str), "rollout id")?;
        let subagent = payload
            .get("source")
            .and_then(|value| value.get("subagent"));
        let spawn = subagent.and_then(|value| value.get("thread_spawn"));
        let explicit_thread_id = normalized_relational_identifier(
            payload.get("session_id").and_then(Value::as_str),
            "session thread id",
        )?;
        let spawn_parent_thread_id = normalized_relational_identifier(
            spawn
                .and_then(|value| value.get("parent_thread_id"))
                .and_then(Value::as_str),
            "parent thread id",
        )?;
        // Older child rollouts omit session_id. Their parent thread is the
        // only top-level ownership signal and must not become a fake session.
        let thread_id = explicit_thread_id
            .clone()
            .or_else(|| spawn_parent_thread_id.clone())
            .unwrap_or_else(|| owner_id.clone());
        let parent_thread_id =
            spawn_parent_thread_id.or_else(|| (owner_id != thread_id).then(|| thread_id.clone()));
        let forked_from_id = normalized_relational_identifier(
            payload.get("forked_from_id").and_then(Value::as_str),
            "fork parent rollout id",
        )?;
        let spawn_parent_rollout_id = normalized_relational_identifier(
            spawn
                .and_then(|value| value.get("parent_rollout_id"))
                .and_then(Value::as_str),
            "spawn parent rollout id",
        )?;
        let parent_rollout_id = forked_from_id
            .or(spawn_parent_rollout_id)
            .or_else(|| parent_thread_id.clone());
        let agent_path = spawn
            .and_then(|value| value.get("agent_path"))
            .and_then(Value::as_str)
            .and_then(|value| normalized_metadata_value(Some(value), PROJECTED_SESSION_PATH_CHARS));
        let agent_nickname = spawn
            .and_then(|value| value.get("agent_nickname"))
            .and_then(Value::as_str)
            .and_then(|value| normalized_metadata_value(Some(value), PROJECTED_EVENT_LABEL_CHARS))
            .or_else(|| {
                subagent
                    .and_then(|value| value.get("other"))
                    .and_then(Value::as_str)
                    .and_then(|value| {
                        normalized_metadata_value(Some(value), PROJECTED_EVENT_LABEL_CHARS)
                    })
            });
        let metadata = normalized_session_metadata(payload);
        // Source topology and authored labels are projected into dedicated
        // columns above. The raw source object also carries transport context
        // and can contain arbitrarily large embedded payloads, so it has no
        // remaining query consumer and is intentionally not retained.
        let source_json = None;
        let is_subagent = spawn.is_some()
            || explicit_thread_id
                .as_deref()
                .is_some_and(|value| value != owner_id);
        let forked = owner_id != thread_id || spawn.is_some() || parent_rollout_id.is_some();
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .or_else(|| payload.get("timestamp").and_then(Value::as_str))
            .ok_or_else(|| anyhow!("first session_meta has no timestamp"))?;
        return Ok(OwnerMeta {
            owner_id: owner_id.clone(),
            thread_id: thread_id.clone(),
            parent_rollout_id,
            parent_thread_id,
            agent_path,
            agent_nickname,
            is_subagent,
            forked,
            timestamp: canonical_source_timestamp(timestamp)?,
            cwd: metadata.cwd,
            project: metadata.project,
            repository_url: metadata.repository_url,
            branch: metadata.branch,
            source: metadata.source,
            thread_source: metadata.thread_source,
            source_json,
        });
    }
    Err(anyhow!("{} has no session_meta record", path.display()))
}

enum FingerprintAudit {
    Verified { changed: bool },
    Mismatch,
}

fn full_content_fingerprints(
    path: &Path,
    size: u64,
    prefix_size: Option<u64>,
) -> Result<FullFingerprint> {
    let mut file = File::open(path)?;
    full_content_fingerprints_from_file(&mut file, size, prefix_size)
}

fn full_content_fingerprints_from_file(
    file: &mut File,
    size: u64,
    prefix_size: Option<u64>,
) -> Result<FullFingerprint> {
    if prefix_size.is_some_and(|prefix| prefix > size) {
        return Err(anyhow!("fingerprint prefix exceeds file size"));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut legacy_current = Sha256::new();
    legacy_current.update(size.to_le_bytes());
    let mut legacy_prefix = prefix_size.map(|prefix| {
        let mut hasher = Sha256::new();
        hasher.update(prefix.to_le_bytes());
        hasher
    });
    let mut current_chunks = Vec::with_capacity(size.div_ceil(FINGERPRINT_CHUNK_BYTES) as usize);
    let mut prefix_chunks = prefix_size
        .map(|prefix| Vec::with_capacity(prefix.div_ceil(FINGERPRINT_CHUNK_BYTES) as usize));
    let mut remaining = size;
    let mut prefix_remaining = prefix_size.unwrap_or_default();
    let mut buffer = vec![0_u8; FINGERPRINT_CHUNK_BYTES as usize];
    while remaining > 0 {
        let chunk = remaining.min(FINGERPRINT_CHUNK_BYTES) as usize;
        file.read_exact(&mut buffer[..chunk])?;
        record_fingerprint_bytes_read(chunk as u64);
        legacy_current.update(&buffer[..chunk]);
        current_chunks.push(hash_fingerprint_chunk(&buffer[..chunk]));
        if prefix_remaining > 0 {
            let prefix_chunk = prefix_remaining.min(chunk as u64) as usize;
            if let Some(hasher) = &mut legacy_prefix {
                hasher.update(&buffer[..prefix_chunk]);
            }
            if let Some(chunks) = &mut prefix_chunks {
                chunks.push(hash_fingerprint_chunk(&buffer[..prefix_chunk]));
            }
            prefix_remaining -= prefix_chunk as u64;
        }
        remaining -= chunk as u64;
    }
    let audited_at = Utc::now().timestamp();
    Ok(FullFingerprint {
        current: ChunkedFingerprint {
            size,
            chunk_bytes: FINGERPRINT_CHUNK_BYTES,
            chunks: current_chunks,
            audit_cursor: 0,
            audit_completed_at: audited_at,
        },
        prefix: prefix_size.map(|prefix| ChunkedFingerprint {
            size: prefix,
            chunk_bytes: FINGERPRINT_CHUNK_BYTES,
            chunks: prefix_chunks.unwrap_or_default(),
            audit_cursor: 0,
            audit_completed_at: audited_at,
        }),
        legacy_current: format!("{:x}", legacy_current.finalize()),
        legacy_prefix: legacy_prefix.map(|hasher| format!("{:x}", hasher.finalize())),
    })
}

#[cfg(test)]
fn extend_chunked_fingerprint(
    path: &Path,
    size: u64,
    previous: &ChunkedFingerprint,
) -> Result<(ChunkedFingerprint, bool)> {
    let mut file = File::open(path)?;
    extend_chunked_fingerprint_from_file(&mut file, size, previous)
}

fn extend_chunked_fingerprint_from_file(
    file: &mut File,
    size: u64,
    previous: &ChunkedFingerprint,
) -> Result<(ChunkedFingerprint, bool)> {
    if size <= previous.size {
        return Err(anyhow!("chunk fingerprint extension requires file growth"));
    }
    let start = previous.size / FINGERPRINT_CHUNK_BYTES * FINGERPRINT_CHUNK_BYTES;
    let retained_chunks = (start / FINGERPRINT_CHUNK_BYTES) as usize;
    let previous_tail_bytes = previous.size - start;
    let mut chunks = previous.chunks[..retained_chunks].to_vec();
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = size - start;
    let mut buffer = vec![0_u8; FINGERPRINT_CHUNK_BYTES as usize];
    let mut first_chunk = true;
    let mut verified_tail = previous_tail_bytes == 0;
    while remaining > 0 {
        let chunk = remaining.min(FINGERPRINT_CHUNK_BYTES) as usize;
        file.read_exact(&mut buffer[..chunk])?;
        record_fingerprint_bytes_read(chunk as u64);
        if first_chunk && previous_tail_bytes > 0 {
            verified_tail = hash_fingerprint_chunk(&buffer[..previous_tail_bytes as usize])
                == previous.chunks[retained_chunks];
        }
        chunks.push(hash_fingerprint_chunk(&buffer[..chunk]));
        remaining -= chunk as u64;
        first_chunk = false;
    }
    Ok((
        ChunkedFingerprint {
            size,
            chunk_bytes: FINGERPRINT_CHUNK_BYTES,
            chunks,
            audit_cursor: previous
                .audit_cursor
                .min(size.div_ceil(FINGERPRINT_CHUNK_BYTES) as usize),
            audit_completed_at: previous.audit_completed_at,
        },
        verified_tail,
    ))
}

#[cfg(test)]
fn audit_chunked_fingerprint(
    path: &Path,
    fingerprint: &mut ChunkedFingerprint,
    budget: &mut FingerprintAuditBudget,
) -> Result<FingerprintAudit> {
    let mut file = File::open(path)?;
    audit_chunked_fingerprint_from_file(&mut file, fingerprint, budget)
}

fn audit_chunked_fingerprint_from_file(
    file: &mut File,
    fingerprint: &mut ChunkedFingerprint,
    budget: &mut FingerprintAuditBudget,
) -> Result<FingerprintAudit> {
    if budget.files_remaining == 0 || budget.bytes_remaining == 0 {
        return Ok(FingerprintAudit::Verified { changed: false });
    }
    let original_cursor = fingerprint.audit_cursor;
    let original_completed_at = fingerprint.audit_completed_at;
    let mut buffer = vec![0_u8; FINGERPRINT_CHUNK_BYTES as usize];
    let mut read_any = false;
    let mut file_bytes_remaining = budget.bytes_remaining.min(FINGERPRINT_AUDIT_BYTES_PER_FILE);
    while fingerprint.audit_cursor < fingerprint.chunks.len() {
        let offset = fingerprint.audit_cursor as u64 * FINGERPRINT_CHUNK_BYTES;
        let chunk = (fingerprint.size - offset).min(FINGERPRINT_CHUNK_BYTES);
        if chunk > budget.bytes_remaining || chunk > file_bytes_remaining {
            break;
        }
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buffer[..chunk as usize])?;
        record_fingerprint_bytes_read(chunk);
        budget.bytes_remaining -= chunk;
        file_bytes_remaining -= chunk;
        read_any = true;
        if hash_fingerprint_chunk(&buffer[..chunk as usize])
            != fingerprint.chunks[fingerprint.audit_cursor]
        {
            budget.files_remaining = budget.files_remaining.saturating_sub(1);
            return Ok(FingerprintAudit::Mismatch);
        }
        fingerprint.audit_cursor += 1;
    }
    if read_any {
        budget.files_remaining = budget.files_remaining.saturating_sub(1);
    }
    if fingerprint.audit_cursor == fingerprint.chunks.len() && read_any {
        fingerprint.audit_cursor = 0;
        fingerprint.audit_completed_at = Utc::now().timestamp();
    }
    Ok(FingerprintAudit::Verified {
        changed: fingerprint.audit_cursor != original_cursor
            || fingerprint.audit_completed_at != original_completed_at,
    })
}

fn audit_growing_chunked_fingerprint_from_file(
    file: &mut File,
    fingerprint: &mut ChunkedFingerprint,
) -> Result<FingerprintAudit> {
    // A growing file is about to trust its previous projection and consume
    // only the appended suffix. Give that correctness check its own bounded
    // one-chunk budget rather than allowing earlier path-sorted files to
    // exhaust the shared background-audit budget. This guarantees progress
    // for every growing file while the global cap still governs stable-file
    // audits.
    let mut budget = FingerprintAuditBudget {
        bytes_remaining: FINGERPRINT_AUDIT_BYTES_PER_FILE,
        files_remaining: 1,
    };
    audit_chunked_fingerprint_from_file(file, fingerprint, &mut budget)
}

fn fingerprint_for_prefix_from_file(
    file: &mut File,
    current: &str,
    prefix_size: u64,
) -> Result<String> {
    let Some(mut fingerprint) = ChunkedFingerprint::parse(current) else {
        return full_content_fingerprints_from_file(file, prefix_size, None)?
            .current
            .encode();
    };
    if prefix_size > fingerprint.size {
        return Err(anyhow!("fingerprint prefix exceeds current extent"));
    }

    let chunk_count = prefix_size.div_ceil(FINGERPRINT_CHUNK_BYTES) as usize;
    fingerprint.chunks.truncate(chunk_count);
    let tail_size = prefix_size % FINGERPRINT_CHUNK_BYTES;
    if tail_size > 0 {
        let tail_offset = prefix_size - tail_size;
        file.seek(SeekFrom::Start(tail_offset))?;
        let mut tail = vec![0_u8; tail_size as usize];
        file.read_exact(&mut tail)?;
        record_fingerprint_bytes_read(tail_size);
        let tail_hash = hash_fingerprint_chunk(&tail);
        if let Some(chunk) = fingerprint.chunks.last_mut() {
            *chunk = tail_hash;
        } else {
            return Err(anyhow!("fingerprint is missing its committed tail chunk"));
        }
    }
    fingerprint.size = prefix_size;
    if fingerprint.audit_cursor >= chunk_count {
        fingerprint.audit_cursor = 0;
    }
    fingerprint.encode()
}

fn stored_fingerprint_matches(stored: &str, chunked: &ChunkedFingerprint, legacy: &str) -> bool {
    ChunkedFingerprint::parse(stored).is_some_and(|fingerprint| fingerprint.same_content(chunked))
        || stored == legacy
}

fn hash_fingerprint_chunk(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn record_fingerprint_bytes_read(bytes_read: u64) {
    #[cfg(test)]
    FINGERPRINT_BYTES_READ.with(|bytes| {
        bytes.set(bytes.get().saturating_add(bytes_read));
    });
    #[cfg(not(test))]
    let _ = bytes_read;
}

#[cfg(unix)]
fn file_identity(metadata: &Metadata) -> FileIdentity {
    let ctime_ns = metadata
        .ctime()
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(metadata.ctime_nsec()));
    FileIdentity {
        ctime_ns,
        device_id: i64::try_from(metadata.dev()).ok(),
        inode: i64::try_from(metadata.ino()).ok(),
    }
}

#[cfg(not(unix))]
fn file_identity(_metadata: &Metadata) -> FileIdentity {
    FileIdentity::default()
}

#[cfg(test)]
thread_local! {
    static FINGERPRINT_BYTES_READ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_fingerprint_bytes_read() {
    FINGERPRINT_BYTES_READ.with(|bytes| bytes.set(0));
}

#[cfg(test)]
fn fingerprint_bytes_read() -> u64 {
    FINGERPRINT_BYTES_READ.with(std::cell::Cell::get)
}

fn canonical_source_timestamp(value: &str) -> Result<String> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid RFC3339 timestamp {value:?}"))?;
    let parsed = parsed.with_timezone(&Utc);
    if !(MIN_PUBLIC_YEAR..=MAX_PUBLIC_YEAR).contains(&parsed.year()) {
        return Err(anyhow!(
            "timestamp year must be between {MIN_PUBLIC_YEAR} and {MAX_PUBLIC_YEAR}"
        ));
    }
    Ok(canonical_utc(parsed))
}

fn canonical_utc(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn extract_content(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(extract_content)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) => [
            "text",
            "input_text",
            "output_text",
            "summary_text",
            "content",
        ]
        .into_iter()
        .find_map(|key| map.get(key).map(extract_content))
        .unwrap_or_default(),
        _ => String::new(),
    }
}

fn has_omitted_attachment(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_omitted_attachment),
        Value::Object(map) => {
            let attachment_type = map.get("type").and_then(Value::as_str).is_some_and(|kind| {
                matches!(
                    kind,
                    "attachment"
                        | "file"
                        | "image"
                        | "input_audio"
                        | "input_file"
                        | "input_image"
                        | "output_audio"
                        | "output_file"
                        | "output_image"
                )
            });
            attachment_type
                || ["attachment", "attachments", "file_url", "image_url"]
                    .iter()
                    .any(|key| map.contains_key(*key))
                || map.values().any(has_omitted_attachment)
        }
        _ => false,
    }
}

fn value_to_string(value: &Value) -> String {
    if let Some(value) = value.as_str() {
        value.to_owned()
    } else {
        serde_json::to_string(value).unwrap_or_default()
    }
}

fn value_to_text(value: &Value) -> Option<String> {
    let text = extract_content(value);
    if text.is_empty() {
        Some(value_to_string(value)).filter(|value| value != "null" && value != "{}")
    } else {
        Some(text)
    }
}

fn is_turn_abort_envelope(content: &str) -> bool {
    let content = content.trim();
    content.starts_with("<turn_aborted>") && content.contains("</turn_aborted>")
}

fn is_transport_context_envelope(content: &str) -> bool {
    let content = content.trim_start();
    content.starts_with("# AGENTS.md instructions")
        || content.starts_with("<environment_context>")
        || is_recommended_plugins_transport_bundle(content)
}

fn is_recommended_plugins_transport_bundle(content: &str) -> bool {
    let Some(after_opening) = content.strip_prefix("<recommended_plugins>") else {
        return false;
    };
    let Some((_, after_plugins)) = after_opening.split_once("</recommended_plugins>") else {
        return false;
    };

    let mut remainder = after_plugins.trim();
    if remainder.is_empty() {
        return true;
    }
    if remainder.starts_with("# AGENTS.md instructions") {
        let Some((_, after_agents)) = remainder.split_once("</INSTRUCTIONS>") else {
            return false;
        };
        remainder = after_agents.trim();
    }
    if remainder.starts_with("<environment_context>") {
        let Some((_, after_environment)) = remainder.split_once("</environment_context>") else {
            return false;
        };
        remainder = after_environment.trim();
    }

    remainder.is_empty()
}

fn compact_title(value: &str) -> String {
    let value = redact_data_urls(value);
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let title: String = chars.by_ref().take(180).collect();
    if chars.next().is_some() {
        format!("{title}…")
    } else {
        title
    }
}

fn duration_ms(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    value
        .as_i64()
        .or_else(|| {
            value.as_str().and_then(|text| {
                text.strip_suffix('s')?
                    .parse::<f64>()
                    .ok()
                    .map(|seconds| (seconds * 1_000.0).round() as i64)
            })
        })
        .or_else(|| {
            value
                .as_f64()
                .map(|seconds| (seconds * 1_000.0).round() as i64)
        })
        .or_else(|| {
            let seconds = value.get("secs")?.as_i64()?;
            let nanos = value.get("nanos").and_then(Value::as_i64).unwrap_or(0);
            let whole = seconds.saturating_mul(1_000);
            let fractional = nanos.max(0).saturating_add(999_999) / 1_000_000;
            Some(whole.saturating_add(fractional))
        })
        .and_then(|value| bounded_duration_ms(Some(value)))
}

fn raw_duration_ms(value: Option<&Value>) -> Option<i64> {
    bounded_duration_ms(value.and_then(Value::as_i64))
}

fn bounded_duration_ms(value: Option<i64>) -> Option<i64> {
    value.filter(|value| (0..=MAX_STORED_DURATION_MS).contains(value))
}

fn event_id(state: &CursorState, line: u64) -> String {
    format!("{}:{line}", state.owner_id)
}

fn projected_message_id(rollout_id: &str, source_id: &str) -> String {
    format!("message:{}", serde_json::json!([rollout_id, source_id]))
}

fn is_owner_native_turn(owner_id: &str, turn_id: &str) -> bool {
    if let Some(owner_timestamp) = uuid7_timestamp(owner_id) {
        // A replayed legacy turn can use a random UUID and compare greater than
        // a time-ordered UUIDv7 by accident. Only UUIDv7 turns participate in
        // the chronological fork boundary for UUIDv7 rollouts. Compare the
        // timestamp field rather than random suffixes so same-millisecond IDs
        // remain correctly ordered as native.
        uuid7_timestamp(turn_id).is_some_and(|turn_timestamp| turn_timestamp >= owner_timestamp)
    } else {
        !turn_id.is_empty()
    }
}

fn uuid7_timestamp(value: &str) -> Option<u64> {
    looks_like_uuid7(value).then_some(())?;
    let high = u64::from_str_radix(&value[..8], 16).ok()?;
    let low = u64::from_str_radix(&value[9..13], 16).ok()?;
    Some((high << 16) | low)
}

fn looks_like_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}

fn looks_like_uuid7(value: &str) -> bool {
    let bytes = value.as_bytes();
    looks_like_uuid(value)
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
}

fn set_meta(db: &Db, key: &str, value: &str) -> Result<()> {
    let connection = db.connect()?;
    connection.execute(
        "INSERT INTO app_meta(key,value) VALUES(?1,?2)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![key, value],
    )?;
    Ok(())
}

/// Convert a transient state left by a terminated process into a durable,
/// truthful failure before this process decides whether to run ingestion.
/// Taking the same process lock as `scan_once` prevents us from recovering a
/// scan that is still active in another process.
pub fn recover_interrupted_scan(db: &Db) -> Result<bool> {
    let _scan_guard = DatabaseLock::acquire(db, "ingest")?;
    let mut connection = db.connect()?;
    let transaction = connection.transaction()?;
    let recovered = transaction.execute(
        "UPDATE app_meta SET value='error'
         WHERE key='ingest_state' AND value='scanning'",
        [],
    )? > 0;
    if recovered {
        transaction.execute(
            "INSERT INTO app_meta(key,value)
             VALUES('last_ingest_error','previous ingest process exited before completing')
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [],
        )?;
    }
    transaction.commit()?;
    Ok(recovered)
}

fn finish_scan_meta(
    db: &Db,
    attempted_at: &str,
    report_json: &str,
    error: Option<&str>,
) -> Result<()> {
    let mut connection = db.connect()?;
    let transaction = connection.transaction()?;
    for (key, value) in [
        ("last_ingest_attempt_at", attempted_at),
        ("last_scan_report", report_json),
        (
            "ingest_state",
            if error.is_some() { "error" } else { "idle" },
        ),
    ] {
        transaction.execute(
            "INSERT INTO app_meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )?;
    }
    if let Some(error) = error {
        transaction.execute(
            "INSERT INTO app_meta(key,value) VALUES('last_ingest_error',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [error],
        )?;
    } else {
        transaction.execute(
            "INSERT INTO app_meta(key,value) VALUES('last_ingest_at',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [attempted_at],
        )?;
        transaction.execute("DELETE FROM app_meta WHERE key='last_ingest_error'", [])?;
    }
    transaction.commit()?;
    Ok(())
}

fn checked_token_count(value: u64, field: &str, line: u64) -> Result<i64> {
    if value > MAX_USAGE_TOKENS_PER_FACT {
        return Err(anyhow!(
            "source line {line} has {field} above the supported {MAX_USAGE_TOKENS_PER_FACT}-token per-fact limit"
        ));
    }
    Ok(value as i64)
}

fn parse_token_usage(info: &Value, field: &str, line: u64) -> Result<Option<TokenUsage>> {
    let Some(value) = info.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let mut usage = serde_json::from_value::<TokenUsage>(value.clone())
        .with_context(|| format!("source line {line} has invalid {field}"))?;
    let total_supplied = value.get("total_tokens").is_some();
    validate_token_usage(usage, total_supplied, field, line)?;
    if !total_supplied {
        usage.total_tokens = usage
            .input_tokens
            .checked_add(usage.output_tokens)
            .ok_or_else(|| anyhow!("source line {line} has overflowing {field}.total_tokens"))?;
    }
    Ok(Some(usage))
}

fn parse_total_token_usage(info: &Value, line: u64) -> Result<Option<TokenUsage>> {
    let original_error = match parse_token_usage(info, "total_token_usage", line) {
        Ok(usage) => return Ok(usage),
        Err(error) => error,
    };
    let Some(Value::Object(value)) = info.get("total_token_usage") else {
        return Err(original_error);
    };
    let Some(context_window) = info.get("model_context_window").and_then(Value::as_u64) else {
        return Err(original_error);
    };
    if context_window == 0 {
        return Err(original_error);
    }
    let mut usage = serde_json::from_value::<TokenUsage>(Value::Object(value.clone()))
        .with_context(|| format!("source line {line} has invalid total_token_usage"))?;
    validate_token_usage(usage, false, "total_token_usage", line)?;
    let attributable_total = usage
        .input_tokens
        .checked_add(usage.output_tokens)
        .ok_or_else(|| {
            anyhow!("source line {line} has overflowing total_token_usage.total_tokens")
        })?;
    let total_with_context_window =
        attributable_total
            .checked_add(context_window)
            .ok_or_else(|| {
                anyhow!("source line {line} has overflowing total_token_usage.total_tokens")
            })?;
    if usage.total_tokens != total_with_context_window {
        return Err(original_error);
    }
    // A narrow family of Codex snapshots adds exactly one model context
    // window to the cumulative `total_tokens`, including an initial
    // zero-component sentinel. The itemized counters and subsequent deltas are
    // internally consistent, while charging the offset would double-count a
    // capacity marker as usage. Keep the attributable components and derive
    // their exact total; every other mismatch remains an error.
    usage.total_tokens = attributable_total;
    Ok(Some(usage))
}

fn last_token_usage_is_total_only_hint(info: &Value) -> bool {
    let Some(Value::Object(last)) = info.get("last_token_usage") else {
        return false;
    };
    [
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
    ]
    .into_iter()
    .all(|field| last.get(field).and_then(Value::as_u64) == Some(0))
        && last
            .get("total_tokens")
            .and_then(Value::as_u64)
            .is_some_and(|total| total > 0)
}

fn validate_token_usage(
    usage: TokenUsage,
    total_supplied: bool,
    field: &str,
    line: u64,
) -> Result<()> {
    if usage.cached_input_tokens > usage.input_tokens {
        return Err(anyhow!(
            "source line {line} has {field}.cached_input_tokens greater than input_tokens"
        ));
    }
    if usage.reasoning_output_tokens > usage.output_tokens {
        return Err(anyhow!(
            "source line {line} has {field}.reasoning_output_tokens greater than output_tokens"
        ));
    }
    let expected_total = usage
        .input_tokens
        .checked_add(usage.output_tokens)
        .ok_or_else(|| anyhow!("source line {line} has overflowing {field}.total_tokens"))?;
    if total_supplied && usage.total_tokens != expected_total {
        return Err(anyhow!(
            "source line {line} has {field}.total_tokens inconsistent with input_tokens + output_tokens"
        ));
    }
    Ok(())
}

fn reconcile_missing(
    db: &Db,
    observed: &HashSet<String>,
    pending_owners: &HashSet<String>,
    enumerated_roots: &[PathBuf],
    incomplete_roots: &[PathBuf],
) -> Result<()> {
    if enumerated_roots.is_empty() && incomplete_roots.is_empty() {
        return Ok(());
    }
    let mut connection = db.connect()?;
    let sources = {
        let mut statement =
            connection.prepare("SELECT rollout_id,path,root_thread_id FROM source_files")?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    // `clear_rollout` reads before deleting. Reserve writer ownership first so
    // a concurrent pricing commit cannot stale that snapshot mid-reconcile.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for (rollout_id, path, thread_id) in sources {
        if observed.contains(&path) || pending_owners.contains(&rollout_id) {
            continue;
        }
        let source_path = Path::new(&path);
        if incomplete_roots
            .iter()
            .any(|root| source_path.starts_with(root))
        {
            continue;
        }
        // An absent source under a fully enumerated current root is deleted.
        // An absent source outside every current root belongs to an older root
        // configuration and is also deleted, but only after the signature's
        // clean adoption scan (the caller enforces that two-scan contract).
        let cleared_thread = clear_rollout(&transaction, &rollout_id)?;
        transaction.execute(
            "DELETE FROM source_files WHERE rollout_id=?1",
            [&rollout_id],
        )?;
        if let Some(thread_id) = cleared_thread.or(thread_id) {
            delete_thread_if_abandoned(&transaction, &thread_id)?;
        }
    }
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, io::Write};

    thread_local! {
        static TRACED_PROMOTED_AGENT_LIFECYCLE_SQL: RefCell<Option<String>> =
            const { RefCell::new(None) };
    }

    fn capture_promoted_agent_lifecycle_sql(sql: &str) {
        if sql.contains("FROM events")
            && !sql.contains("FROM turns")
            && sql.contains("kind='turn_started'")
            && sql.contains("ORDER BY timestamp DESC")
        {
            TRACED_PROMOTED_AGENT_LIFECYCLE_SQL.with(|captured| {
                *captured.borrow_mut() = Some(sql.to_owned());
            });
        }
    }

    fn write_fixture(path: &Path, lines: &[Value]) {
        let mut file = File::create(path).unwrap();
        for line in lines {
            writeln!(file, "{}", serde_json::to_string(line).unwrap()).unwrap();
        }
    }

    fn meta(timestamp: &str, owner: &str, thread: &str, fork: bool) -> Value {
        let source = if fork {
            serde_json::json!({"subagent":{"thread_spawn":{
                "parent_thread_id":thread,"agent_path":"/root/child","agent_nickname":"Newton"
            }}})
        } else {
            Value::String("vscode".into())
        };
        serde_json::json!({"timestamp":timestamp,"type":"session_meta","payload":{
            "id":owner,"session_id":thread,"cwd":"/tmp/project","source":source
        }})
    }

    fn task(timestamp: &str, turn: &str) -> Value {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"task_started","turn_id":turn
        }})
    }

    fn root_fork_meta(timestamp: &str, owner: &str, parent: &str) -> Value {
        serde_json::json!({"timestamp":timestamp,"type":"session_meta","payload":{
            "id":owner,"session_id":owner,"forked_from_id":parent,
            "cwd":"/tmp/project","source":"vscode"
        }})
    }

    fn legacy_child_meta(timestamp: &str, owner: &str, parent: &str) -> Value {
        serde_json::json!({"timestamp":timestamp,"type":"session_meta","payload":{
            "id":owner,"forked_from_id":parent,"cwd":"/tmp/project",
            "source":{"subagent":{"thread_spawn":{
                "parent_thread_id":parent,"agent_path":"/root/reviewer",
                "agent_nickname":"Ramanujan"
            }}}
        }})
    }

    fn context(timestamp: &str, turn: &str, model: &str) -> Value {
        serde_json::json!({"timestamp":timestamp,"type":"turn_context","payload":{
            "turn_id":turn,"model":model,"effort":"high"
        }})
    }

    fn usage(timestamp: &str, input: u64) -> Value {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"token_count","info":{
                "total_token_usage":{"input_tokens":input,"cached_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0,"total_tokens":input+1},
                "last_token_usage":{"input_tokens":input,"cached_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0,"total_tokens":input+1}
            }
        }})
    }

    #[test]
    fn parent_observations_preserve_promoted_agent_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('parent','thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z'),
                    ('child','thread','2026-07-01T00:10:00Z','2026-07-01T00:20:00Z');
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,completed_at,status
                 ) VALUES(
                    'child','thread','child','parent','2026-07-01T00:10:00Z',
                    '2026-07-01T00:20:00Z','completed'
                 );",
            )
            .unwrap();

        let transaction = connection.transaction().unwrap();
        upsert_observed_agent(
            &transaction,
            "child",
            "thread",
            "parent",
            Some("/root/promoted"),
            "2026-07-01T00:30:00Z",
            "interrupted",
        )
        .unwrap();
        transaction.commit().unwrap();

        let lifecycle: (String, Option<String>, Option<String>, String) = connection
            .query_row(
                "SELECT status,completed_at,agent_path,rollout_id
                 FROM agent_runs WHERE id='child'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(lifecycle.0, "completed");
        assert_eq!(lifecycle.1.as_deref(), Some("2026-07-01T00:20:00Z"));
        assert_eq!(lifecycle.2.as_deref(), Some("/root/promoted"));
        assert_eq!(lifecycle.3, "child");
    }

    #[test]
    fn promoted_agent_lifecycle_lookup_uses_activity_owner_index() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z');
                 INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,started_at,last_event_at
                 ) VALUES(
                    'child','thread','parent','2026-07-01T00:10:00Z',
                    '2026-07-01T00:20:00Z'
                 );
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,status
                 ) VALUES(
                    'child','thread','child','parent','2026-07-01T00:10:00Z','running'
                 );",
            )
            .unwrap();

        TRACED_PROMOTED_AGENT_LIFECYCLE_SQL.with(|captured| {
            *captured.borrow_mut() = None;
        });
        connection.trace(Some(capture_promoted_agent_lifecycle_sql));
        let transaction = connection.transaction().unwrap();
        restore_promoted_agent_native_state(&transaction, "child").unwrap();
        transaction.commit().unwrap();
        connection.trace(None);

        let sql = TRACED_PROMOTED_AGENT_LIFECYCLE_SQL
            .with(|captured| captured.borrow_mut().take())
            .expect("promoted-agent lifecycle query was not traced");
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let plan = connection
            .prepare(&explain)
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let plan_text = plan.join("\n");
        assert!(
            plan.iter().any(|detail| {
                detail.contains("SEARCH events USING INDEX idx_events_activity_owner")
            }),
            "promoted-agent lifecycle lookup did not use the activity-owner index:\n{plan_text}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("SCAN events")),
            "promoted-agent lifecycle lookup full-scanned events:\n{plan_text}"
        );
    }

    #[test]
    fn synthetic_agent_observations_map_lifecycle_states_consistently() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-07-01T00:00:00Z','2026-07-01T01:00:00Z');",
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        upsert_observed_agent(
            &transaction,
            "interrupted-child",
            "thread",
            "parent",
            None,
            "2026-07-01T00:10:00Z",
            "started",
        )
        .unwrap();
        upsert_observed_agent(
            &transaction,
            "interrupted-child",
            "thread",
            "parent",
            None,
            "2026-07-01T00:20:00Z",
            "interrupted",
        )
        .unwrap();
        upsert_observed_agent(
            &transaction,
            "completed-child",
            "thread",
            "parent",
            None,
            "2026-07-01T00:40:00Z",
            "completed",
        )
        .unwrap();
        upsert_observed_agent(
            &transaction,
            "rolled-back-child",
            "thread",
            "parent",
            None,
            "2026-07-01T00:50:00Z",
            "rolled_back",
        )
        .unwrap();
        upsert_observed_agent(
            &transaction,
            "running-child",
            "thread",
            "parent",
            None,
            "2026-07-01T00:55:00Z",
            "interrupted",
        )
        .unwrap();
        upsert_observed_agent(
            &transaction,
            "running-child",
            "thread",
            "parent",
            None,
            "2026-07-01T01:00:00Z",
            "interacted",
        )
        .unwrap();
        transaction.commit().unwrap();

        let states = connection
            .prepare(
                "SELECT id,status,completed_at FROM agent_runs
                 ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            states,
            vec![
                (
                    "completed-child".into(),
                    "completed".into(),
                    Some("2026-07-01T00:40:00Z".into()),
                ),
                (
                    "interrupted-child".into(),
                    "interrupted".into(),
                    Some("2026-07-01T00:20:00Z".into()),
                ),
                (
                    "rolled-back-child".into(),
                    "rolled_back".into(),
                    Some("2026-07-01T00:50:00Z".into()),
                ),
                ("running-child".into(), "running".into(), None),
            ]
        );
    }

    #[test]
    fn projected_durations_are_bounded_at_parse_and_schema_boundaries() {
        assert_eq!(
            duration_ms(Some(&serde_json::json!(MAX_STORED_DURATION_MS))),
            Some(MAX_STORED_DURATION_MS)
        );
        assert_eq!(
            duration_ms(Some(&serde_json::json!(MAX_STORED_DURATION_MS + 1))),
            None
        );
        assert_eq!(raw_duration_ms(Some(&serde_json::json!(-1))), None);

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000012";
        let turn = "019f64ab-0000-7000-8000-000000000012";
        write_fixture(
            &sessions.join("duration.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:02Z",
                    "type":"event_msg",
                    "payload":{
                        "type":"task_complete","turn_id":turn,
                        "duration_ms":MAX_STORED_DURATION_MS + 1
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:03Z",
                    "type":"event_msg",
                    "payload":{
                        "type":"exec_command_end","call_id":"oversized-call",
                        "duration_ms":MAX_STORED_DURATION_MS + 1,"exit_code":0
                    }
                }),
            ],
        );
        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let stored: (Option<i64>, Option<i64>, Option<i64>) = connection
            .query_row(
                "SELECT
                    (SELECT duration_ms FROM turns WHERE id=?1),
                    (SELECT duration_ms FROM events
                     WHERE rollout_id=?2 AND kind='tool_completed'),
                    (SELECT duration_ms FROM tool_calls
                     WHERE rollout_id=?2 AND call_id='oversized-call')",
                params![turn, owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (None, None, None));

        let oversized = MAX_STORED_DURATION_MS + 1;
        assert!(
            connection
                .execute(
                    "INSERT INTO turns(
                        id,thread_id,rollout_id,started_at,status,duration_ms
                     ) VALUES('oversized-turn',?1,?1,?2,'completed',?3)",
                    params![owner, "2026-07-15T09:00:04Z", oversized],
                )
                .is_err()
        );
        assert!(
            connection
                .execute(
                    "INSERT INTO events(
                        id,thread_id,rollout_id,timestamp,source_line,kind,duration_ms
                     ) VALUES('oversized-event',?1,?1,?2,99,'tool_completed',?3)",
                    params![owner, "2026-07-15T09:00:04Z", oversized],
                )
                .is_err()
        );
        assert!(
            connection
                .execute(
                    "INSERT INTO tool_calls(
                        id,call_id,thread_id,rollout_id,started_at,name,status,duration_ms
                     ) VALUES(
                        'oversized-tool','oversized-tool',?1,?1,?2,
                        'exec_command','completed',?3
                     )",
                    params![owner, "2026-07-15T09:00:04Z", oversized],
                )
                .is_err()
        );
    }

    #[test]
    fn session_index_title_wins_and_refreshes_without_rollout_reingestion() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let thread = "019f7000-0000-7000-8000-000000000001";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[meta("2026-07-16T08:00:00Z", thread, thread, false)],
        );
        std::fs::write(
            temp.path().join("session_index.jsonl"),
            format!(
                "{{\"id\":\"{thread}\",\"thread_name\":\"Newest real title\",\"updated_at\":\"2026-07-16T08:05:00Z\"}}\n\
                 {{\"id\":\"{thread}\",\"thread_name\":\"Older title later in file\",\"updated_at\":\"2026-07-16T08:04:00Z\"}}\n\
                 {{not-json\n"
            ),
        )
        .unwrap();

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions.clone()),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let first: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [thread], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(first, "Newest real title");
        drop(connection);

        let mut index = std::fs::OpenOptions::new()
            .append(true)
            .open(temp.path().join("session_index.jsonl"))
            .unwrap();
        writeln!(
            index,
            "{{\"id\":\"{thread}\",\"thread_name\":\"Renamed while idle\",\"updated_at\":\"2026-07-16T08:06:00Z\"}}"
        )
        .unwrap();
        drop(index);

        let report = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        assert_eq!(report.files_ingested, 0);
        assert_eq!(report.files_unchanged, 1);
        let connection = db.connect().unwrap();
        let renamed: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [thread], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(renamed, "Renamed while idle");
    }

    #[test]
    fn session_index_candidates_are_scoped_to_configured_root_parents() {
        let temp = tempfile::tempdir().unwrap();
        let codex_root = temp.path().join("isolated-codex-home");
        let roots = IngestRoots {
            active: Some(codex_root.join("sessions")),
            archive: Some(codex_root.join("archived_sessions")),
        };

        assert_eq!(session_index_candidates(&roots), vec![codex_root]);
    }

    #[test]
    fn session_index_discovery_ignores_ambient_codex_home() {
        use std::process::Command;

        let temp = tempfile::tempdir().unwrap();
        let ambient = temp.path().join("ambient-codex-home");
        let configured = temp.path().join("isolated-corpus");
        std::fs::create_dir_all(configured.join("sessions")).unwrap();
        std::fs::create_dir_all(&ambient).unwrap();
        std::fs::write(ambient.join("session_index.jsonl"), b"ambient\n").unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("ingest::tests::session_index_scope_child")
            .arg("--nocapture")
            .env("CODEX_HOME", &ambient)
            .env("HOME", temp.path().join("ambient-home"))
            .env("CODEX_USAGE_CONFIGURED_CORPUS", &configured)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn session_index_scope_child() {
        let Ok(configured) = std::env::var("CODEX_USAGE_CONFIGURED_CORPUS") else {
            return;
        };
        let roots = IngestRoots {
            active: Some(PathBuf::from(configured).join("sessions")),
            archive: None,
        };
        assert_eq!(discover_session_index(&roots), None);
    }

    #[test]
    fn session_meta_thread_name_precedes_prompt_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let thread = "019f7000-0000-7000-8000-000000000002";
        let turn = "019f7001-0000-7000-8000-000000000002";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                serde_json::json!({"timestamp":"2026-07-16T08:00:00Z","type":"session_meta","payload":{
                    "id":thread,"session_id":thread,"cwd":"/tmp/project","source":"vscode",
                    "thread_name":"Metadata title"
                }}),
                task("2026-07-16T08:00:01Z", turn),
                serde_json::json!({"timestamp":"2026-07-16T08:00:02Z","type":"event_msg","payload":{
                    "type":"user_message","message":"A very long first prompt that is only a fallback"
                }}),
            ],
        );
        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [thread], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "Metadata title");
    }

    #[test]
    fn fork_replay_prefix_is_excluded_until_owner_native_turn() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("fork.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let parent = "019df47e-0000-7000-8000-000000000000";
        let inherited_turn = "019df500-0000-7000-8000-000000000000";
        let inherited_legacy_turn = "392fc773-e404-46d6-8764-595914ed82f6";
        let native_turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &file,
            &[
                root_fork_meta("2026-07-15T09:00:00Z", owner, parent),
                meta("2026-07-15T09:00:00Z", parent, parent, false),
                task("2026-07-15T09:00:01Z", inherited_turn),
                context("2026-07-15T09:00:01Z", inherited_turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 2_753_402_716),
                task("2026-07-15T09:00:02.100Z", inherited_legacy_turn),
                context("2026-07-15T09:00:02.100Z", inherited_legacy_turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02.200Z", 2_900_000_000),
                task("2026-07-15T09:00:03Z", native_turn),
                root_fork_meta("2026-07-15T09:00:03Z", owner, parent),
                context("2026-07-15T09:00:03Z", native_turn, "gpt-5.5"),
                usage("2026-07-15T09:00:04Z", 41_000),
            ],
        );
        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let (count, input): (i64, i64) = connection
            .query_row(
                "SELECT COUNT(*),COALESCE(SUM(input_tokens),0) FROM usage_facts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(input, 41_000);
    }

    #[test]
    fn source_timestamps_stay_inside_the_queryable_calendar_domain() {
        assert_eq!(
            canonical_source_timestamp("9998-12-31T23:59:59Z").unwrap(),
            "9998-12-31T23:59:59.000000000Z"
        );
        assert!(canonical_source_timestamp("1969-12-31T23:59:59Z").is_err());
        assert!(canonical_source_timestamp("9999-01-01T00:00:00Z").is_err());
    }

    #[test]
    fn uuid7_boundary_validates_shape_and_uses_timestamp_not_random_suffix() {
        let owner = "019f64aa-ffff-7fff-bfff-ffffffffffff";
        let same_millisecond_turn = "019f64aa-ffff-7000-8000-000000000000";
        assert!(is_owner_native_turn(owner, same_millisecond_turn));
        assert!(is_owner_native_turn(
            owner,
            "019F64AA-FFFF-7000-8000-000000000000"
        ));
        assert!(!is_owner_native_turn(
            owner,
            "392fc773-e404-46d6-8764-595914ed82f6"
        ));
        assert!(!is_owner_native_turn(
            owner,
            "019f64ab-0000-7zzz-8000-000000000000"
        ));
        assert!(!is_owner_native_turn(
            owner,
            "019f64ab-0000-7000-0000-000000000000"
        ));
    }

    #[test]
    fn legacy_child_without_session_id_groups_under_parent_thread() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let root = "019cc9e7-0000-7000-8000-000000000000";
        let child = "019cc9e9-0000-7000-8000-000000000000";
        let grandchild = "019cc9eb-0000-7000-8000-000000000000";
        let root_turn = "019cc9e8-0000-7000-8000-000000000000";
        let child_turn = "019cc9ea-0000-7000-8000-000000000000";
        let grandchild_turn = "019cc9ec-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("z-root.jsonl"),
            &[
                meta("2026-03-07T21:00:00Z", root, root, false),
                task("2026-03-07T21:00:01Z", root_turn),
                context("2026-03-07T21:00:01Z", root_turn, "gpt-5.5"),
                usage("2026-03-07T21:00:02Z", 100),
            ],
        );
        write_fixture(
            &sessions.join("m-child.jsonl"),
            &[
                legacy_child_meta("2026-03-07T21:07:53Z", child, root),
                task("2026-03-07T21:07:54Z", child_turn),
                context("2026-03-07T21:07:54Z", child_turn, "gpt-5.5"),
                usage("2026-03-07T21:07:55Z", 50),
            ],
        );
        write_fixture(
            &sessions.join("a-grandchild.jsonl"),
            &[
                legacy_child_meta("2026-03-07T21:08:53Z", grandchild, child),
                task("2026-03-07T21:08:54Z", grandchild_turn),
                context("2026-03-07T21:08:54Z", grandchild_turn, "gpt-5.5"),
                usage("2026-03-07T21:08:55Z", 25),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let (threads, rollouts, usage_facts, input): (i64, i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM threads),(SELECT COUNT(*) FROM rollouts),
                        (SELECT COUNT(*) FROM usage_facts),
                        (SELECT COALESCE(SUM(input_tokens),0) FROM usage_facts)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!((threads, rollouts, usage_facts, input), (1, 3, 3, 175));
        let child_projection: (String, String, String) = connection
            .query_row(
                "SELECT r.thread_id,a.thread_id,COALESCE(a.nickname,'')
                 FROM rollouts r JOIN agent_runs a ON a.id=r.id WHERE r.id=?1",
                [child],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            child_projection,
            (root.into(), root.into(), "Ramanujan".into())
        );
        let grandchild_thread: String = connection
            .query_row(
                "SELECT thread_id FROM rollouts WHERE id=?1",
                [grandchild],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(grandchild_thread, root);
    }

    #[test]
    fn imported_parent_does_not_absorb_a_top_level_root_fork() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let parent = "019df47e-0000-7000-8000-000000000000";
        let fork = "019f64aa-0000-7000-8000-000000000000";
        let parent_turn = "019df47f-0000-7000-8000-000000000000";
        let fork_turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("z-parent.jsonl"),
            &[
                meta("2026-05-04T21:00:00Z", parent, parent, false),
                task("2026-05-04T21:00:01Z", parent_turn),
                context("2026-05-04T21:00:01Z", parent_turn, "gpt-5.5"),
                usage("2026-05-04T21:00:02Z", 100),
            ],
        );
        write_fixture(
            &sessions.join("a-fork.jsonl"),
            &[
                root_fork_meta("2026-07-15T09:00:00Z", fork, parent),
                task("2026-07-15T09:00:01Z", fork_turn),
                context("2026-07-15T09:00:01Z", fork_turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 50),
            ],
        );
        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let threads: i64 = connection
            .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
            .unwrap();
        let fork_usage_thread: String = connection
            .query_row(
                "SELECT thread_id FROM usage_facts WHERE rollout_id=?1",
                [fork],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(threads, 2);
        assert_eq!(fork_usage_thread, fork);
    }

    #[test]
    fn rename_and_abort_events_update_durable_thread_and_turn_state() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"thread_name_updated","thread_name":"First title"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                    "type":"thread_name_updated","thread_name":"Summarize last 10 emails"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                    "type":"turn_aborted","turn_id":turn,"reason":"interrupted"
                }}),
            ],
        );
        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [owner], |row| {
                row.get(0)
            })
            .unwrap();
        let turn_state: (String, Option<String>) = connection
            .query_row(
                "SELECT status,completed_at FROM turns WHERE id=?1",
                [turn],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let agent_status: String = connection
            .query_row(
                "SELECT status FROM agent_runs WHERE id=?1",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(title, "Summarize last 10 emails");
        assert_eq!(turn_state.0, "interrupted");
        assert!(turn_state.1.is_some());
        assert_eq!(agent_status, "interrupted");
    }

    #[test]
    fn final_assistant_message_completes_legacy_turn_without_task_complete() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let previous_turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", previous_turn),
                context("2026-07-15T09:00:01Z", previous_turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":"Start the first request."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":previous_turn,
                    "last_agent_message":"First request complete."
                }}),
                // Some interleaved/legacy traces have no task_started or
                // turn_context for the follow-up. The projector creates a
                // stable synthetic turn from the user message's source line.
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":"Now generate the images."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"final_answer","content":[{
                        "type":"output_text","text":"The five images are ready."
                    }]
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let legacy_turn = format!("{owner}:legacy-turn:6");
        let state: (String, Option<String>, Option<String>) = connection
            .query_row(
                "SELECT status,completed_at,last_agent_message FROM turns WHERE id=?1",
                [&legacy_turn],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state.0, "completed");
        assert_eq!(state.1.as_deref(), Some("2026-07-15T09:00:05.000000000Z"));
        assert_eq!(state.2.as_deref(), Some("The five images are ready."));
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND kind='turn_completed'",
                    [&legacy_turn],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn attachment_only_messages_keep_metadata_while_tool_payloads_are_omitted() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000099";
        let turn = "019f64ab-0000-7000-8000-000000000099";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:02Z",
                    "type":"response_item",
                    "payload":{
                        "type":"message",
                        "role":"user",
                        "content":[{
                            "type":"input_image",
                            "image_url":"data:image/png;base64,IMAGE_BASE64_SENTINEL"
                        }]
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:03Z",
                    "type":"response_item",
                    "payload":{
                        "type":"custom_tool_call",
                        "call_id":"metadata-only-call",
                        "name":"exec",
                        "input":"TOOL_INPUT_SENTINEL"
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:04Z",
                    "type":"response_item",
                    "payload":{
                        "type":"custom_tool_call_output",
                        "call_id":"metadata-only-call",
                        "output":"TOOL_OUTPUT_SENTINEL data:image/png;base64,TOOL_BASE64_SENTINEL"
                    }
                }),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let message: (String, String) = connection
            .query_row(
                "SELECT content,timestamp FROM messages WHERE thread_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(message.0, "[Attachment omitted]");
        assert_eq!(message.1, "2026-07-15T09:00:02.000000000Z");

        let tool: (String, String, String, String, Option<String>, Option<i64>) = connection
            .query_row(
                "SELECT call_id,name,status,started_at,completed_at,duration_ms
                 FROM tool_calls WHERE thread_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(tool.0, "metadata-only-call");
        assert_eq!(tool.1, "exec");
        assert_eq!(tool.2, "completed");
        assert_eq!(tool.3, "2026-07-15T09:00:03.000000000Z");
        assert_eq!(tool.4.as_deref(), Some("2026-07-15T09:00:04.000000000Z"));
        assert_eq!(tool.5, None);

        let tool_payload_columns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('tool_calls')
                 WHERE name IN ('input','output')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let message_payload_columns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('messages')
                 WHERE name='content_json'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tool_payload_columns, 0);
        assert_eq!(message_payload_columns, 0);

        let retained_payload_sentinels: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM (
                    SELECT content AS value FROM messages
                    UNION ALL SELECT COALESCE(body,'') FROM events
                    UNION ALL SELECT COALESCE(payload_json,'') FROM events
                 ) WHERE value LIKE '%TOOL_INPUT_SENTINEL%'
                    OR value LIKE '%TOOL_OUTPUT_SENTINEL%'
                    OR value LIKE '%BASE64_SENTINEL%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_payload_sentinels, 0);
    }

    #[test]
    fn embedded_data_urls_are_redacted_before_visible_text_and_json_are_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000098";
        let turn = "019f64ab-0000-7000-8000-000000000098";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:02Z",
                    "type":"event_msg",
                    "payload":{
                        "type":"user_message",
                        "message":"Inspect data:image/png;base64,TITLE_BASE64_SENTINEL please"
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:03Z",
                    "type":"response_item",
                    "payload":{
                        "type":"message",
                        "role":"user",
                        "content":[{
                            "type":"input_text",
                            "text":"Please inspect data:image/png;base64,MESSAGE_BASE64_SENTINEL now"
                        }]
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:04Z",
                    "type":"event_msg",
                    "payload":{
                        "type":"agent_reasoning",
                        "text":"Reasoning around data:image/png;base64,REASONING_BASE64_SENTINEL"
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:05Z",
                    "type":"response_item",
                    "payload":{
                        "type":"agent_message",
                        "author":"data:image/png;base64,LABEL_BASE64_SENTINEL",
                        "recipient":"parent",
                        "message":"Evidence data:image/png;base64,SUBAGENT_BASE64_SENTINEL"
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:06Z",
                    "type":"event_msg",
                    "payload":{
                        "type":"thread_goal_updated",
                        "goal":{
                            "objective":"Check data:image/png;base64,GOAL_BASE64_SENTINEL",
                            "evidence":{"image":"data:image/png;base64,PAYLOAD_BASE64_SENTINEL"}
                        }
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:07Z",
                    "type":"event_msg",
                    "payload":{
                        "type":"task_complete",
                        "turn_id":turn,
                        "last_agent_message":"Done data:image/png;base64,FINAL_BASE64_SENTINEL"
                    }
                }),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [owner], |row| {
                row.get(0)
            })
            .unwrap();
        let message: String = connection
            .query_row(
                "SELECT content FROM messages WHERE thread_id=?1",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        let last_agent_message: String = connection
            .query_row(
                "SELECT last_agent_message FROM turns WHERE id=?1",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(title, "Inspect [embedded attachment] please");
        assert_eq!(message, "Please inspect [embedded attachment] now");
        assert_eq!(last_agent_message, "Done [embedded attachment]");

        let goal_payload: Option<String> = connection
            .query_row(
                "SELECT payload_json FROM events WHERE thread_id=?1 AND kind='goal'",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        assert!(goal_payload.is_none());

        let retained_data_urls: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM (
                    SELECT COALESCE(title,'') AS value FROM threads
                    UNION ALL SELECT content FROM messages
                    UNION ALL SELECT COALESCE(last_agent_message,'') FROM turns
                    UNION ALL SELECT COALESCE(label,'') FROM events
                    UNION ALL SELECT COALESCE(body,'') FROM events
                    UNION ALL SELECT COALESCE(tool_name,'') FROM events
                    UNION ALL SELECT COALESCE(payload_json,'') FROM events
                 ) WHERE lower(value) LIKE '%data:image%'
                    OR value LIKE '%BASE64_SENTINEL%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_data_urls, 0);
    }

    #[test]
    fn session_metadata_is_normalized_at_discovery_and_update_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000096";
        let embedded = "data:image/png;base64,METADATA_SENTINEL";
        let long_path = format!(
            "/tmp/{embedded} {}",
            "p".repeat(PROJECTED_SESSION_PATH_CHARS + 1_000)
        );
        let long_repository = format!(
            "{embedded} {}",
            "r".repeat(PROJECTED_SESSION_PATH_CHARS + 1_000)
        );
        let long_branch = format!(
            "{embedded} {}",
            "b".repeat(PROJECTED_IDENTIFIER_CHARS + 1_000)
        );
        let long_source = format!(
            "{embedded} {}",
            "s".repeat(PROJECTED_IDENTIFIER_CHARS + 1_000)
        );
        let long_thread_source = format!(
            "{embedded} {}",
            "t".repeat(PROJECTED_IDENTIFIER_CHARS + 1_000)
        );
        let long_title = format!(
            "{embedded} {}",
            "n".repeat(PROJECTED_SESSION_TITLE_CHARS + 1_000)
        );
        write_fixture(
            &sessions.join("metadata.jsonl"),
            &[
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:00Z",
                    "type":"session_meta",
                    "payload":{
                        "id":owner,
                        "session_id":owner,
                        "cwd":"/tmp/initial",
                        "source":"vscode",
                        "thread_source":"user",
                        "git":{
                            "repository_url":"https://example.test/initial",
                            "branch":"initial"
                        }
                    }
                }),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:01Z",
                    "type":"session_meta",
                    "payload":{
                        "id":owner,
                        "session_id":owner,
                        "thread_name":long_title,
                        "cwd":long_path,
                        "source":long_source,
                        "thread_source":long_thread_source,
                        "git":{
                            "repository_url":long_repository,
                            "branch":long_branch
                        }
                    }
                }),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let metadata: (String, String, String, String, String, String, String) = connection
            .query_row(
                "SELECT title,cwd,project,repository_url,branch,source,thread_source
                 FROM threads WHERE id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert!(metadata.0.chars().count() <= PROJECTED_SESSION_TITLE_CHARS + 1);
        assert!(metadata.1.chars().count() <= PROJECTED_SESSION_PATH_CHARS + 1);
        assert!(metadata.2.chars().count() <= PROJECTED_EVENT_LABEL_CHARS + 1);
        assert!(metadata.3.chars().count() <= PROJECTED_SESSION_PATH_CHARS + 1);
        assert!(metadata.4.chars().count() <= PROJECTED_IDENTIFIER_CHARS + 1);
        assert!(metadata.5.chars().count() <= PROJECTED_IDENTIFIER_CHARS + 1);
        assert!(metadata.6.chars().count() <= PROJECTED_IDENTIFIER_CHARS + 1);
        for value in [
            &metadata.0,
            &metadata.1,
            &metadata.2,
            &metadata.3,
            &metadata.4,
            &metadata.5,
            &metadata.6,
        ] {
            assert!(!value.to_ascii_lowercase().contains("data:image"));
            assert!(!value.contains("METADATA_SENTINEL"));
        }

        let oversized_id = format!("{}{}", "i".repeat(PROJECTED_IDENTIFIER_CHARS), embedded);
        let owner_path = temp.path().join("owner-only.jsonl");
        write_fixture(
            &owner_path,
            &[serde_json::json!({
                "timestamp":"2026-07-15T09:00:00Z",
                "type":"session_meta",
                "payload":{
                    "id":oversized_id.clone(),
                    "session_id":oversized_id,
                    "cwd":"/tmp/project",
                    "source":{
                        "subagent":{
                            "thread_spawn":{
                                "parent_thread_id":format!("parent-{embedded}"),
                                "parent_rollout_id":format!("rollout-{embedded}"),
                                "agent_path":format!("/root/{embedded}"),
                                "agent_nickname":format!("nickname-{embedded}")
                            }
                        }
                    }
                }
            })],
        );
        let error = peek_owner(&owner_path).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exceeds the 256-character identifier limit"),
            "unexpected identifier error: {error:#}"
        );
    }

    #[test]
    fn oversized_relational_identifiers_are_rejected_instead_of_colliding() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let shared_prefix = "i".repeat(PROJECTED_IDENTIFIER_CHARS);
        for (name, suffix) in [("a.jsonl", "-first"), ("b.jsonl", "-second")] {
            let owner = format!("{shared_prefix}{suffix}");
            write_fixture(
                &sessions.join(name),
                &[serde_json::json!({
                    "timestamp":"2026-07-15T09:00:00Z",
                    "type":"session_meta",
                    "payload":{
                        "id":owner,
                        "session_id":owner,
                        "cwd":"/tmp/project",
                        "source":"vscode"
                    }
                })],
            );
        }

        let error = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("exceeds the 256-character identifier limit"),
            "unexpected identifier error: {error:#}"
        );
        let connection = db.connect().unwrap();
        let projection: (i64, i64, i64) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM threads),
                    (SELECT COUNT(*) FROM rollouts),
                    (SELECT COUNT(*) FROM source_files)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(projection, (0, 0, 0));
    }

    #[test]
    fn oversized_turn_identifier_rolls_back_its_relational_projection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-0000000000f6";
        let turn = format!("{}-turn", "t".repeat(PROJECTED_IDENTIFIER_CHARS));
        write_fixture(
            &sessions.join("oversized-turn.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", &turn),
            ],
        );

        let error = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("turn id exceeds the 256-character identifier limit"),
            "unexpected identifier error: {error:#}"
        );
        let projection: (i64, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM rollouts),
                    (SELECT COUNT(*) FROM turns),
                    (SELECT COUNT(*) FROM events),
                    (SELECT COUNT(*) FROM source_files)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(projection, (0, 0, 0, 0));
    }

    #[test]
    fn lifecycle_metadata_is_allowlisted_bounded_and_kept_out_of_session_source_json() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000097";
        let turn = "019f64ab-0000-7000-8000-000000000097";
        let child = "019f64ac-0000-7000-8000-000000000097";
        let hostile = format!(
            "data:image/png;base64,HIDDEN_METADATA_SENTINEL{}",
            "x".repeat(200_000)
        );
        let long_goal = format!(
            "Keep this authored goal. {} {hostile}",
            "g".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000)
        );
        let long_plan = format!(
            "Keep this authored plan. {} {hostile}",
            "p".repeat(PROJECTED_EVENT_BODY_CHARS + 1_000)
        );
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                serde_json::json!({"timestamp":"2026-07-15T09:00:00Z","type":"session_meta","payload":{
                    "id":owner,"session_id":owner,"cwd":"/tmp/project",
                    "source":{"kind":"cli","transport_blob":hostile}
                }}),
                task("2026-07-15T09:00:01Z", turn),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"sub_agent_activity","kind":"completed","agent_thread_id":child,
                    "agent_path":"/root/reviewer","transport_blob":hostile
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                    "type":"thread_goal_updated","goal":{"objective":long_goal,"status":"active"},
                    "transport_blob":hostile
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                    "type":"item_completed","item":{"type":"Plan","text":long_plan},
                    "transport_blob":hostile
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"event_msg","payload":{
                    "type":"entered_review_mode","transport_blob":hostile
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let source_json: Option<String> = connection
            .query_row(
                "SELECT source_json FROM threads WHERE id=?1",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        assert!(source_json.is_none());
        let events: Vec<(String, Option<String>, Option<String>)> = connection
            .prepare(
                "SELECT kind,body,payload_json FROM events
                 WHERE rollout_id=?1 AND kind IN ('subagent','goal','plan','state')
                 ORDER BY timestamp,source_line",
            )
            .unwrap()
            .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 4);
        let subagent_payload: Value =
            serde_json::from_str(events[0].2.as_deref().unwrap()).unwrap();
        assert_eq!(
            subagent_payload,
            serde_json::json!({"agent_thread_id": child})
        );
        for event in &events[1..] {
            assert!(event.2.is_none(), "{} payload must be omitted", event.0);
        }
        assert!(
            events[1]
                .1
                .as_deref()
                .unwrap()
                .starts_with("Keep this authored goal.")
        );
        assert!(
            events[2]
                .1
                .as_deref()
                .unwrap()
                .starts_with("Keep this authored plan.")
        );
        assert!(events[1].1.as_deref().unwrap().chars().count() <= PROJECTED_EVENT_BODY_CHARS + 1);
        assert!(events[2].1.as_deref().unwrap().chars().count() <= PROJECTED_EVENT_BODY_CHARS + 1);
        let retained_hostile: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM (
                    SELECT COALESCE(source_json,'') value FROM threads
                    UNION ALL SELECT COALESCE(payload_json,'') FROM events
                    UNION ALL SELECT COALESCE(body,'') FROM events
                 ) WHERE value LIKE '%HIDDEN_METADATA_SENTINEL%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_hostile, 0);
    }

    #[test]
    fn explicit_turn_metadata_keeps_mid_turn_user_messages_on_native_turn() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let explicit_user_message = |timestamp: &str, text: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":text
                }],
                "internal_chat_message_metadata_passthrough":{"turn_id":turn}
            }})
        };
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                explicit_user_message("2026-07-15T09:00:02Z", "Start the research."),
                explicit_user_message(
                    "2026-07-15T09:00:03Z",
                    "Use the signed-in built-in browser.",
                ),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                    "type":"agent_reasoning","text":"Adapting the browser research."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04.100Z","type":"response_item","payload":{
                    "type":"reasoning","summary":[{
                        "type":"summary_text","text":"Adapting the browser research."
                    }],
                    "internal_chat_message_metadata_passthrough":{"turn_id":turn}
                }}),
                explicit_user_message(
                    "2026-07-15T09:00:05Z",
                    "<subagent_notification>{\"status\":\"completed\"}</subagent_notification>",
                ),
                serde_json::json!({"timestamp":"2026-07-15T09:00:06Z","type":"event_msg","payload":{
                    "type":"agent_reasoning","text":"Integrating the subagent result."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:07Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"final_answer","content":[{
                        "type":"output_text","text":"Research complete."
                    }],
                    "internal_chat_message_metadata_passthrough":{"turn_id":turn}
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:08Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":turn,"last_agent_message":"Research complete."
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let turn_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .unwrap();
        let legacy_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE id LIKE '%:legacy-turn:%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let user_messages: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE turn_id=?1 AND role='user'",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        let reasoning_events: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND kind='reasoning'",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        let status: String = connection
            .query_row("SELECT status FROM turns WHERE id=?1", [turn], |row| {
                row.get(0)
            })
            .unwrap();

        assert_eq!(turn_count, 1);
        assert_eq!(legacy_count, 0);
        assert_eq!(user_messages, 3);
        assert_eq!(reasoning_events, 2);
        assert_eq!(status, "completed");
    }

    #[test]
    fn metadata_free_feedback_stays_on_running_native_turn() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019db16f-0000-7000-8000-000000000000";
        let turn = "019db170-0000-7000-8000-000000000000";
        let user_message = |timestamp: &str, text: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":text
                }]
            }})
        };
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-04-21T19:06:18Z", owner, owner, false),
                task("2026-04-21T19:06:18.100Z", turn),
                context("2026-04-21T19:06:18.100Z", turn, "gpt-5.4"),
                user_message("2026-04-21T19:06:18.200Z", "Create a Valencia comic."),
                user_message(
                    "2026-04-21T19:27:28.444Z",
                    "Use a T-shirt and clearly blue jeans.",
                ),
                user_message("2026-04-21T19:27:28.445Z", "Keep the comic wordless."),
                user_message(
                    "2026-04-21T19:27:28.446Z",
                    "Reduce the protagonist appearances.",
                ),
                serde_json::json!({"timestamp":"2026-04-21T19:29:04Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"commentary","content":[{
                        "type":"output_text","text":"Understood: blue jeans, no captions, and fewer protagonist appearances."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-04-21T19:29:05Z","type":"event_msg","payload":{
                    "type":"agent_reasoning","text":"Applying the combined feedback."
                }}),
                usage("2026-04-21T19:29:06Z", 42_000),
                serde_json::json!({"timestamp":"2026-04-21T19:54:20Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"final_answer","content":[{
                        "type":"output_text","text":"The revised Valencia comic is complete."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-04-21T19:54:20.100Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":turn,
                    "last_agent_message":"The revised Valencia comic is complete."
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let turn_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .unwrap();
        let legacy_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE id LIKE '%:legacy-turn:%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let user_messages: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE turn_id=?1 AND role='user'",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        let reasoning_events: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND kind='reasoning'",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        let usage_facts: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM usage_facts WHERE turn_id=?1",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        let status: String = connection
            .query_row("SELECT status FROM turns WHERE id=?1", [turn], |row| {
                row.get(0)
            })
            .unwrap();

        assert_eq!(turn_count, 1);
        assert_eq!(legacy_count, 0);
        assert_eq!(user_messages, 4);
        assert_eq!(reasoning_events, 1);
        assert_eq!(usage_facts, 1);
        assert_eq!(status, "completed");
    }

    #[test]
    fn metadata_free_feedback_after_provisional_final_stays_on_native_turn() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019cc496-0000-7000-8000-000000000000";
        let turn = "019cc4e2-0000-7000-8000-000000000000";
        let user_message = |timestamp: &str, text: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":text
                }]
            }})
        };
        let final_message = |timestamp: &str, text: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":text
                }]
            }})
        };
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-03-06T20:41:20Z", owner, owner, false),
                task("2026-03-06T20:41:21Z", turn),
                context("2026-03-06T20:41:21Z", turn, "gpt-5.4"),
                user_message("2026-03-06T20:41:22Z", "Watch the batch."),
                final_message("2026-03-06T21:17:56.273Z", "The deep dive is complete."),
                user_message(
                    "2026-03-06T21:17:56.274Z",
                    "Please repair the previous takeaway and continue watching.",
                ),
                serde_json::json!({"timestamp":"2026-03-06T21:19:21Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"commentary","content":[{
                        "type":"output_text","text":"Repairing it and continuing the watch."
                    }]
                }}),
                final_message("2026-03-06T22:41:02.668Z", "The batch is stable."),
                serde_json::json!({"timestamp":"2026-03-06T22:41:02.669Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":turn,
                    "last_agent_message":"The batch is stable."
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let turn_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .unwrap();
        let user_messages: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE turn_id=?1 AND role='user'",
                [turn],
                |row| row.get(0),
            )
            .unwrap();
        let state: (String, Option<String>, Option<String>) = connection
            .query_row(
                "SELECT status,completed_at,last_agent_message FROM turns WHERE id=?1",
                [turn],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(turn_count, 1);
        assert_eq!(user_messages, 2);
        assert_eq!(state.0, "completed");
        assert_eq!(state.1.as_deref(), Some("2026-03-06T22:41:02.669000000Z"));
        assert_eq!(state.2.as_deref(), Some("The batch is stable."));
    }

    #[test]
    fn old_order_context_envelopes_do_not_hide_the_following_human_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019c443a-0000-7000-8000-000000000000";
        let first_turn = "019c443b-0000-7000-8000-000000000000";
        let second_turn = "019c5e03-0000-7000-8000-000000000000";
        let user_message = |timestamp: &str, text: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
                "type":"message","role":"user","content":[{
                    "type":"input_text","text":text
                }]
            }})
        };
        let final_message = |timestamp: &str, text: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"response_item","payload":{
                "type":"message","role":"assistant","phase":"final_answer","content":[{
                    "type":"output_text","text":text
                }]
            }})
        };
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-02-14T20:00:00Z", owner, owner, false),
                task("2026-02-14T20:00:01Z", first_turn),
                context("2026-02-14T20:00:01Z", first_turn, "gpt-5.4"),
                user_message("2026-02-14T20:00:02Z", "Finish the first task."),
                final_message("2026-02-14T20:00:03Z", "First task complete."),
                serde_json::json!({"timestamp":"2026-02-14T20:00:03.100Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":first_turn,
                    "last_agent_message":"First task complete."
                }}),
                user_message(
                    "2026-02-14T21:17:08.962Z",
                    "# AGENTS.md instructions for /Users/example/project\n\n<INSTRUCTIONS>\nUse the project rules.\n</INSTRUCTIONS>",
                ),
                user_message(
                    "2026-02-14T21:17:08.962Z",
                    "<environment_context>\n  <cwd>/Users/example/project</cwd>\n  <shell>zsh</shell>\n</environment_context>",
                ),
                task("2026-02-14T21:17:08.962Z", second_turn),
                user_message(
                    "2026-02-14T21:17:08.963Z",
                    "This is the actual second human prompt.",
                ),
                context("2026-02-14T21:17:08.964Z", second_turn, "gpt-5.4"),
                final_message("2026-02-14T21:17:09Z", "Second task complete."),
                serde_json::json!({"timestamp":"2026-02-14T21:17:09.100Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":second_turn,
                    "last_agent_message":"Second task complete."
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM turns WHERE id LIKE '%:legacy-turn:%'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        let second_prompt: String = connection
            .query_row(
                "SELECT content FROM messages WHERE turn_id=?1 AND role='user'",
                [second_turn],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(second_prompt, "This is the actual second human prompt.");
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM messages
                     WHERE content LIKE '# AGENTS.md instructions for %'
                        OR content LIKE '<environment_context>%'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn a_new_native_task_interrupts_an_unfinished_previous_task() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019cb02f-0000-7000-8000-000000000000";
        let first_turn = "019cb030-0000-7000-8000-000000000000";
        let second_turn = "019cb031-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-03-02T20:00:00Z", owner, owner, false),
                task("2026-03-02T20:00:01Z", first_turn),
                context("2026-03-02T20:00:01Z", first_turn, "gpt-5.4"),
                serde_json::json!({"timestamp":"2026-03-02T20:00:02Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":"Begin the first task."
                    }]
                }}),
                task("2026-03-02T20:05:00Z", second_turn),
                context("2026-03-02T20:05:00Z", second_turn, "gpt-5.4"),
                serde_json::json!({"timestamp":"2026-03-02T20:05:01Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"final_answer","content":[{
                        "type":"output_text","text":"Second task complete."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-03-02T20:05:01.100Z","type":"event_msg","payload":{
                    "type":"task_complete","turn_id":second_turn,
                    "last_agent_message":"Second task complete."
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let first_state: (String, Option<String>) = connection
            .query_row(
                "SELECT status,completed_at FROM turns WHERE id=?1",
                [first_turn],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(first_state.0, "interrupted");
        assert_eq!(
            first_state.1.as_deref(),
            Some("2026-03-02T20:05:00.000000000Z")
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM turns WHERE id=?1",
                    [second_turn],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );
    }

    #[test]
    fn explicit_abort_after_final_answer_remains_authoritative() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"response_item","payload":{
                    "type":"message","role":"assistant","phase":"final_answer","content":[{
                        "type":"output_text","text":"A final result that is subsequently interrupted."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                    "type":"turn_aborted","turn_id":turn,"reason":"interrupted"
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let state: (String, Option<String>) = connection
            .query_row(
                "SELECT status,completed_at FROM turns WHERE id=?1",
                [turn],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state.0, "interrupted");
        assert_eq!(state.1.as_deref(), Some("2026-07-15T09:00:03.000000000Z"));
    }

    #[test]
    fn thread_rollback_is_preserved_as_its_own_terminal_state() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"thread_rolled_back","num_turns":1
                }}),
            ],
        );
        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let turn_status: String = connection
            .query_row("SELECT status FROM turns WHERE id=?1", [turn], |row| {
                row.get(0)
            })
            .unwrap();
        let agent_status: String = connection
            .query_row(
                "SELECT status FROM agent_runs WHERE id=?1",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        let event_status: String = connection
            .query_row(
                "SELECT status FROM events WHERE label='thread_rolled_back'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(turn_status, "rolled_back");
        assert_eq!(agent_status, "rolled_back");
        assert_eq!(event_status, "rolled_back");
    }

    #[test]
    fn recommended_plugins_runtime_bundle_is_not_projected_as_a_user_message() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f6768-0000-7000-8000-000000000000";
        let turn = "019f6769-0000-7000-8000-000000000000";
        let transport_bundle = r#"<recommended_plugins>
Here is a list of plugins available to the runtime.
</recommended_plugins>
# AGENTS.md instructions for /tmp/project
<INSTRUCTIONS>
Use the project rules.
</INSTRUCTIONS>
<environment_context>
  <cwd>/tmp/project</cwd>
  <shell>zsh</shell>
</environment_context>"#;
        let actual_prompt = r#"# Applications mentioned by the user:

<appshot app="Ghostty">Terminal evidence.</appshot>

## My request for Codex:
Trace the real first prompt."#;
        let mixed_request =
            format!("{transport_bundle}\n\n## My request for Codex:\nKeep this real user request.");

        assert!(is_transport_context_envelope(transport_bundle));
        assert!(!is_transport_context_envelope(&mixed_request));

        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T20:13:11.982Z", owner, owner, false),
                task("2026-07-15T20:13:11.982Z", turn),
                serde_json::json!({"timestamp":"2026-07-15T20:13:12.003Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":transport_bundle
                    }]
                }}),
                context("2026-07-15T20:13:12.003Z", turn, "gpt-5.6-sol"),
                serde_json::json!({"timestamp":"2026-07-15T20:13:12.074Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":actual_prompt
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T20:13:12.086Z","type":"event_msg","payload":{
                    "type":"user_message","message":actual_prompt
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let messages: Vec<String> = connection
            .prepare("SELECT content FROM messages WHERE rollout_id=?1 ORDER BY source_line")
            .unwrap()
            .query_map([owner], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(messages, vec![actual_prompt.to_owned()]);
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND kind='message'",
                    [owner],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM turns WHERE rollout_id=?1",
                    [owner],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM turns WHERE rollout_id=?1 AND id LIKE '%:legacy-turn:%'",
                    [owner],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn canonical_activity_suppresses_transport_context_and_abort_envelopes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:00.001Z", turn),
                serde_json::json!({"timestamp":"2026-07-15T09:00:00.002Z","type":"response_item","payload":{
                    "type":"message","role":"developer","content":[{
                        "type":"input_text","text":"Injected developer context."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:00.003Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":"# AGENTS.md instructions\nInjected runtime context."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:00.004Z","type":"response_item","payload":{
                    "type":"ghost_snapshot","snapshot":{"checkpoint":"internal"}
                }}),
                context("2026-07-15T09:00:01.001Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02.000Z","type":"response_item","payload":{
                    "type":"message","id":"user-canonical","role":"user","content":[{
                        "type":"input_text","text":"Build the faithful projector."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02.014Z","type":"event_msg","payload":{
                    "type":"user_message","message":"Build the faithful projector."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"response_item","payload":{
                    "type":"message","id":"assistant-canonical","role":"assistant","phase":"final_answer",
                    "content":[{"type":"output_text","text":"The projector is ready. [citation metadata]"}]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03.011Z","type":"event_msg","payload":{
                    "type":"agent_message","message":"The projector is ready."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04.000Z","type":"event_msg","payload":{
                    "type":"dynamic_tool_call_request","call_id":"dynamic-1","tool":"dynamic_tool"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04.001Z","type":"response_item","payload":{
                    "type":"custom_tool_call","call_id":"dynamic-1","name":"dynamic_tool","input":"{}"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05.000Z","type":"event_msg","payload":{
                    "type":"view_image_tool_call","call_id":"image-view-1","path":"/tmp/example.png"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05.001Z","type":"response_item","payload":{
                    "type":"function_call","call_id":"image-view-1","name":"view_image",
                    "arguments":"{\"path\":\"/tmp/example.png\"}"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:06Z","type":"event_msg","payload":{
                    "type":"item_completed","item":{"type":"Plan","text":"Inspect, implement, verify."}
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:07Z","type":"event_msg","payload":{
                    "type":"entered_review_mode"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:08Z","type":"event_msg","payload":{
                    "type":"exited_review_mode"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:09Z","type":"response_item","payload":{
                    "type":"message","role":"user","content":[{
                        "type":"input_text","text":"<turn_aborted>Interrupted.</turn_aborted>"
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:09Z","type":"event_msg","payload":{
                    "type":"turn_aborted","turn_id":turn,"reason":"interrupted"
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [owner], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "Build the faithful projector.");
        let turns: Vec<(String, String)> = connection
            .prepare("SELECT id,status FROM turns WHERE rollout_id=?1 ORDER BY id")
            .unwrap()
            .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(turns, vec![(turn.into(), "interrupted".into())]);
        let messages: Vec<String> = connection
            .prepare("SELECT content FROM messages WHERE rollout_id=?1 ORDER BY timestamp")
            .unwrap()
            .query_map([owner], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            messages,
            vec![
                "Build the faithful projector.".to_owned(),
                "The projector is ready. [citation metadata]".to_owned(),
            ]
        );
        let noise: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND (
                    label IN ('ghost_snapshot','dynamic_tool_call_request','view_image_tool_call')
                    OR label='Assistant update')",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(noise, 0);
        let tool_calls: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE rollout_id=?1",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tool_calls, 2);
        let plan: (String, String, Option<String>) = connection
            .query_row(
                "SELECT body,status,payload_json FROM events
                 WHERE rollout_id=?1 AND kind='plan'",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(plan.0, "Inspect, implement, verify.");
        assert_eq!(plan.1, "completed");
        assert!(plan.2.is_none());
        let review_states: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND kind='state'
                 AND label IN ('Entered review mode','Exited review mode')",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(review_states, 2);
    }

    #[test]
    fn terminal_tool_state_survives_late_output_and_completion_before_start() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02.000Z","type":"response_item","payload":{
                    "type":"function_call","call_id":"exec-reverse","name":"exec_command",
                    "arguments":"{\"cmd\":\"git bad-command\"}"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02.100Z","type":"event_msg","payload":{
                    "type":"exec_command_end","call_id":"exec-reverse","exit_code":128,"status":"failed",
                    "duration":{"secs":0,"nanos":7000000},"aggregated_output":"secondary failure output"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02.101Z","type":"response_item","payload":{
                    "type":"function_call_output","call_id":"exec-reverse",
                    "output":"canonical failure output: Process exited with code 128"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"event_msg","payload":{
                    "type":"image_generation_end","call_id":"image-reverse","status":"generating",
                    "duration":{"secs":0,"nanos":42000000},"result":"generated image"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03.002Z","type":"response_item","payload":{
                    "type":"image_generation_call","id":"image-reverse","status":"generating"
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let tools: Vec<(String, String, String, i64)> = connection
            .prepare(
                "SELECT call_id,name,status,duration_ms FROM tool_calls
                 WHERE rollout_id=?1 ORDER BY call_id",
            )
            .unwrap()
            .query_map([owner], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            tools,
            vec![
                (
                    "exec-reverse".into(),
                    "exec_command".into(),
                    "failed".into(),
                    7
                ),
                (
                    "image-reverse".into(),
                    "image_generation_call".into(),
                    "completed".into(),
                    42
                ),
            ]
        );
    }

    #[test]
    fn nested_settings_and_terminal_tool_metadata_are_projected_without_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-old"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"thread_settings_applied","thread_settings":{
                        "model":"gpt-nested","reasoning_effort":"xhigh"
                    }
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03.000Z","type":"event_msg","payload":{
                    "type":"agent_reasoning","text":"I need to inspect the projector."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03.001Z","type":"response_item","payload":{
                    "type":"reasoning","id":"reason-1","summary":[{
                        "type":"summary_text","text":"Inspect the projector."
                    }]
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                    "type":"agent_reasoning","text":"A standalone legacy thought."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05Z","type":"response_item","payload":{
                    "type":"function_call","call_id":"call-exec","name":"exec_command","arguments":"{\"cmd\":\"false\"}"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05.100Z","type":"response_item","payload":{
                    "type":"function_call_output","call_id":"call-exec","output":"canonical exec output"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05.200Z","type":"event_msg","payload":{
                    "type":"exec_command_end","call_id":"call-exec","exit_code":1,
                    "duration":{"secs":0,"nanos":1500000},"aggregated_output":"secondary exec output"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:06Z","type":"response_item","payload":{
                    "type":"custom_tool_call","call_id":"call-dynamic","name":"dynamic_tool","input":"{}"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:06.100Z","type":"response_item","payload":{
                    "type":"custom_tool_call_output","call_id":"call-dynamic","output":"canonical dynamic output"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:06.200Z","type":"event_msg","payload":{
                    "type":"dynamic_tool_call_response","call_id":"call-dynamic","tool":"dynamic_tool",
                    "success":false,"error":"boom","duration":{"secs":0,"nanos":62030375}
                }}),
                usage("2026-07-15T09:00:07Z", 100),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let usage_projection: (String, String) = connection
            .query_row(
                "SELECT model,effort FROM usage_facts WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(usage_projection, ("gpt-nested".into(), "xhigh".into()));
        let turn_projection: (String, String) = connection
            .query_row(
                "SELECT model,effort FROM turns WHERE id=?1",
                [turn],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(turn_projection, ("gpt-nested".into(), "xhigh".into()));

        let tools = connection
            .prepare(
                "SELECT call_id,status,duration_ms FROM tool_calls
                 WHERE rollout_id=?1 ORDER BY call_id",
            )
            .unwrap()
            .query_map([owner], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            tools,
            vec![
                ("call-dynamic".into(), "failed".into(), 63,),
                ("call-exec".into(), "failed".into(), 2,),
            ]
        );
        let reasoning: Vec<(String, String)> = connection
            .prepare(
                "SELECT label,body FROM events WHERE rollout_id=?1 AND kind='reasoning'
                 ORDER BY timestamp,source_line",
            )
            .unwrap()
            .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            reasoning,
            vec![
                ("Reasoning summary".into(), "Inspect the projector.".into()),
                ("Reasoning".into(), "A standalone legacy thought.".into()),
            ]
        );
    }

    #[test]
    fn goal_heartbeats_collapse_to_meaningful_lifecycle_changes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let goal = |timestamp: &str, status: &str, tokens: u64, seconds: u64| {
            serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
                "type":"thread_goal_updated","threadId":owner,"turnId":turn,"goal":{
                    "threadId":owner,"objective":"Build faithful ingestion.","status":status,
                    "tokensUsed":tokens,"timeUsedSeconds":seconds
                }
            }})
        };
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                goal("2026-07-15T09:00:02Z", "active", 100, 10),
                goal("2026-07-15T09:00:03Z", "active", 200, 20),
                goal("2026-07-15T09:00:04Z", "active", 300, 30),
                goal("2026-07-15T09:00:05Z", "complete", 400, 40),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        let connection = db.connect().unwrap();
        let goals: Vec<(String, String, Option<String>)> = connection
            .prepare(
                "SELECT body,status,payload_json FROM events
                 WHERE thread_id=?1 AND kind='goal' ORDER BY timestamp,source_line",
            )
            .unwrap()
            .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(goals.len(), 2);
        assert_eq!(goals[0].0, "Build faithful ingestion.");
        assert_eq!(goals[0].1, "active");
        assert!(goals[0].2.is_none());
        assert_eq!(goals[1].0, "Build faithful ingestion.");
        assert_eq!(goals[1].1, "complete");
        assert!(goals[1].2.is_none());
    }

    #[test]
    fn compaction_projection_keeps_summary_and_order_without_replacement_history() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let sentinel = "raw-replacement-history".repeat(25_000);
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"agent_message","message":"Before compaction."
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"compacted","payload":{
                    "message":"  Handoff: continue with the verified plan.  ",
                    "replacement_history":[sentinel,{"role":"assistant","content":"raw only"}],
                    "window_number":2,"first_window_id":"window-1",
                    "previous_window_id":"window-1","window_id":"window-2"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z","type":"event_msg","payload":{
                    "type":"agent_message","message":"After compaction."
                }}),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let events = connection
            .prepare(
                "SELECT kind,body,COALESCE(payload_json,'') FROM events
                 WHERE thread_id=?1 AND kind IN ('update','compaction')
                 ORDER BY timestamp,source_line",
            )
            .unwrap()
            .query_map([owner], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.0.as_str())
                .collect::<Vec<_>>(),
            vec!["update", "compaction", "update"]
        );
        assert_eq!(events[1].1, "Handoff: continue with the verified plan.");
        assert!(!events[1].2.contains("raw-replacement-history"));
        assert!(events[1].2.len() < 256);
        let metadata: Value = serde_json::from_str(&events[1].2).unwrap();
        assert_eq!(metadata["replacement_history_count"], 2);
        assert_eq!(metadata["window_number"], 2);
        assert_eq!(metadata["first_window_id"], "window-1");
        assert_eq!(metadata["previous_window_id"], "window-1");
        assert_eq!(metadata["window_id"], "window-2");
        assert!(metadata.get("replacement_history").is_none());
    }

    #[test]
    fn changed_root_is_adopted_before_next_clean_scan_reconciles_old_sources() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions_a = temp.path().join("sessions-a");
        let sessions_b = temp.path().join("sessions-b");
        std::fs::create_dir(&sessions_a).unwrap();
        std::fs::create_dir(&sessions_b).unwrap();
        let owner_a = "019f64aa-0000-7000-8000-000000000000";
        let owner_b = "019f64ac-0000-7000-8000-000000000000";
        let turn_a = "019f64ab-0000-7000-8000-000000000000";
        let turn_b = "019f64ad-0000-7000-8000-000000000000";
        write_fixture(
            &sessions_a.join("root-a.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner_a, owner_a, false),
                task("2026-07-15T09:00:01Z", turn_a),
                context("2026-07-15T09:00:01Z", turn_a, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        write_fixture(
            &sessions_b.join("root-b.jsonl"),
            &[
                meta("2026-07-15T10:00:00Z", owner_b, owner_b, false),
                task("2026-07-15T10:00:01Z", turn_b),
                context("2026-07-15T10:00:01Z", turn_b, "gpt-5.5"),
                usage("2026-07-15T10:00:02Z", 200),
            ],
        );
        let roots_a = IngestRoots {
            active: Some(sessions_a),
            archive: None,
        };
        let roots_b = IngestRoots {
            active: Some(sessions_b),
            archive: None,
        };

        scan_once(&db, &roots_a).unwrap();
        scan_once(&db, &roots_a).unwrap();
        scan_once(&db, &roots_b).unwrap();
        let connection = db.connect().unwrap();
        let after_adoption: (i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),(SELECT COUNT(*) FROM threads)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(after_adoption, (2, 2));
        drop(connection);

        scan_once(&db, &roots_b).unwrap();
        let connection = db.connect().unwrap();
        let after_confirmation: (i64, i64, String) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),(SELECT COUNT(*) FROM threads),
                        (SELECT rollout_id FROM source_files LIMIT 1)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(after_confirmation, (1, 1, owner_b.into()));
    }

    #[cfg(unix)]
    #[test]
    fn long_lived_scanners_cannot_alternate_different_root_configurations() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions_a = temp.path().join("sessions-a");
        let sessions_b = temp.path().join("sessions-b");
        std::fs::create_dir(&sessions_a).unwrap();
        std::fs::create_dir(&sessions_b).unwrap();
        let owner_a = "019f64aa-0000-7000-8000-000000000020";
        let owner_b = "019f64aa-0000-7000-8000-000000000021";
        write_fixture(
            &sessions_a.join("a.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
        );
        write_fixture(
            &sessions_b.join("b.jsonl"),
            &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
        );
        let roots_a = IngestRoots {
            active: Some(sessions_a),
            archive: None,
        };
        let roots_b = IngestRoots {
            active: Some(sessions_b),
            archive: None,
        };

        let scanner_a = spawn_scanner(db.clone(), roots_a, Duration::from_millis(250)).unwrap();
        let first_deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let first_ready = db
                .connect()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                            (SELECT value FROM app_meta WHERE key='ingest_root_signature')",
                    [owner_a],
                    |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, String>(1)?)),
                )
                .ok();
            if first_ready.as_ref().is_some_and(|(ready, _)| *ready) {
                break;
            }
            assert!(
                std::time::Instant::now() < first_deadline,
                "the first long-lived scanner did not finish its initial scan"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let signature_a: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT value FROM app_meta WHERE key='ingest_root_signature'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let conflict = match spawn_scanner(db.clone(), roots_b, Duration::from_millis(250)) {
            Ok(scanner_b) => {
                scanner_b.shutdown();
                scanner_a.shutdown();
                panic!("a second long-lived scanner unexpectedly acquired the database")
            }
            Err(error) => error,
        };
        assert!(
            format!("{conflict:#}").contains("failed to claim live ingest scanner ownership"),
            "unexpected scanner conflict error: {conflict:#}"
        );
        let connection = db.connect().unwrap();
        let (source_count, signature): (i64, String) = connection
            .query_row(
                "SELECT COUNT(*),
                        (SELECT value FROM app_meta WHERE key='ingest_root_signature')
                 FROM source_files",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(source_count, 1);
        assert_eq!(signature, signature_a);
        drop(connection);
        scanner_a.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn long_lived_scanner_publishes_completed_projector_generation() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000025";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        let scanner = spawn_scanner(db.clone(), roots, Duration::from_secs(60)).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (source_exists, generation): (bool, Option<String>) = db
                .connect()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                            (SELECT value FROM app_meta WHERE key=?2)",
                    params![owner, PROJECTOR_GENERATION_KEY],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            if source_exists && generation == Some(PROJECTOR_GENERATION.to_string()) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "live scanner ingested without publishing its projector generation"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(projector_generation_is_current(&db).unwrap());
        scanner.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn preclaimed_scanner_lease_survives_background_worker_handoff() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000024";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        let lease = IngestScannerLease::acquire(&db).unwrap();
        let before_handoff = scan_one_shot(&db, &roots).unwrap_err();
        assert!(format!("{before_handoff:#}").contains("live ingest scanner"));

        let scanner =
            spawn_scanner_with_lease(db.clone(), roots.clone(), Duration::from_secs(60), lease)
                .unwrap();
        let after_handoff = scan_one_shot(&db, &roots).unwrap_err();
        assert!(format!("{after_handoff:#}").contains("live ingest scanner"));
        scanner.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn one_shot_rejects_conflicting_roots_while_live_scanner_owns_projection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions_a = temp.path().join("sessions-a");
        let sessions_b = temp.path().join("sessions-b");
        std::fs::create_dir(&sessions_a).unwrap();
        std::fs::create_dir(&sessions_b).unwrap();
        let owner_a = "019f64aa-0000-7000-8000-000000000022";
        let owner_b = "019f64aa-0000-7000-8000-000000000023";
        write_fixture(
            &sessions_a.join("a.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
        );
        write_fixture(
            &sessions_b.join("b.jsonl"),
            &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
        );
        let roots_a = IngestRoots {
            active: Some(sessions_a),
            archive: None,
        };
        let roots_b = IngestRoots {
            active: Some(sessions_b),
            archive: None,
        };

        let scanner = spawn_scanner(db.clone(), roots_a, Duration::from_secs(60)).unwrap();
        let first_deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let ready = db
                .connect()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1)",
                    [owner_a],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
                != 0;
            if ready {
                break;
            }
            assert!(
                std::time::Instant::now() < first_deadline,
                "the live scanner did not finish its initial scan"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        let attempt = scan_one_shot(&db, &roots_b);
        scanner.shutdown();
        let error = attempt.expect_err("one-shot ingestion displaced a live scanner");
        assert!(
            format!("{error:#}").contains("live ingest scanner"),
            "unexpected one-shot conflict error: {error:#}"
        );
        let projection: (i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*),
                        EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                        EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?2)
                 FROM source_files",
                params![owner_a, owner_b],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(projection, (1, 1, 0));
    }

    #[test]
    fn one_shot_scan_confirms_changed_root_and_reports_both_passes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions_a = temp.path().join("sessions-a");
        let sessions_b = temp.path().join("sessions-b");
        std::fs::create_dir(&sessions_a).unwrap();
        std::fs::create_dir(&sessions_b).unwrap();
        let owner_a = "019f64aa-0000-7000-8000-000000000010";
        let owner_b = "019f64aa-0000-7000-8000-000000000011";
        write_fixture(
            &sessions_a.join("a.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
        );
        write_fixture(
            &sessions_b.join("b.jsonl"),
            &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
        );
        let roots_a = IngestRoots {
            active: Some(sessions_a),
            archive: None,
        };
        let roots_b = IngestRoots {
            active: Some(sessions_b),
            archive: None,
        };

        let initial = scan_one_shot(&db, &roots_a).unwrap();
        assert_eq!(initial.files_seen, 2);
        assert_eq!(initial.files_ingested, 1);
        assert_eq!(initial.files_unchanged, 1);

        let changed = scan_one_shot(&db, &roots_b).unwrap();
        assert_eq!(changed.files_seen, 2);
        assert_eq!(changed.files_ingested, 1);
        assert_eq!(changed.files_unchanged, 1);
        let projection: (i64, i64, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM threads),
                        (SELECT rollout_id FROM source_files)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(projection, (1, 1, owner_b.into()));

        let unchanged = scan_one_shot(&db, &roots_b).unwrap();
        assert_eq!(unchanged.files_seen, 1);
        assert_eq!(unchanged.files_unchanged, 1);
        assert_eq!(unchanged.files_ingested, 0);
    }

    #[test]
    fn one_shot_confirmation_start_failure_is_finalized_as_an_error() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions_a = temp.path().join("sessions-a");
        let sessions_b = temp.path().join("sessions-b");
        std::fs::create_dir(&sessions_a).unwrap();
        std::fs::create_dir(&sessions_b).unwrap();
        let owner_a = "019f64aa-0000-7000-8000-000000000012";
        let owner_b = "019f64aa-0000-7000-8000-000000000013";
        write_fixture(
            &sessions_a.join("a.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
        );
        write_fixture(
            &sessions_b.join("b.jsonl"),
            &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
        );
        scan_one_shot(
            &db,
            &IngestRoots {
                active: Some(sessions_a),
                archive: None,
            },
        )
        .unwrap();

        let error = scan_one_shot_with_between_pass(
            &db,
            &IngestRoots {
                active: Some(sessions_b),
                archive: None,
            },
            || {
                db.connect()
                    .unwrap()
                    .execute_batch(
                        "CREATE TRIGGER reject_confirmation_scan_start
                         BEFORE UPDATE ON app_meta
                         WHEN OLD.key='ingest_state' AND NEW.value='scanning'
                         BEGIN
                           SELECT RAISE(ABORT,'injected confirmation start failure');
                         END;",
                    )
                    .unwrap();
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("injected confirmation start failure"),
            "unexpected error: {error:#}"
        );

        let metadata: (String, String, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                    (SELECT value FROM app_meta WHERE key='last_scan_report')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(metadata.0, "error");
        assert!(metadata.1.contains("injected confirmation start failure"));
        let report: ScanReport = serde_json::from_str(&metadata.2).unwrap();
        assert_eq!(report.files_seen, 1);
        assert_eq!(report.files_ingested, 1);
        assert_eq!(
            db.connect()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM source_files", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2,
            "the failed confirmation has not yet reconciled the adopted roots"
        );
    }

    #[cfg(unix)]
    #[test]
    fn one_shot_holds_ingest_lock_across_confirmation_pass() {
        use std::sync::mpsc;

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000099";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let mut worker = None;

        scan_one_shot_with_between_pass(&db, &roots, || {
            let contender_db = db.clone();
            let contender_roots = roots.clone();
            worker = Some(std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                completed_tx
                    .send(scan_once(&contender_db, &contender_roots))
                    .unwrap();
            }));
            started_rx.recv().unwrap();
            assert!(
                completed_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err(),
                "a competing scan interleaved between one-shot passes"
            );
        })
        .unwrap();

        completed_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        worker.unwrap().join().unwrap();
    }

    #[test]
    fn genuinely_empty_projection_is_vacuously_current() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();

        assert!(projector_generation_is_current(&db).unwrap());
        let marker_count: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM app_meta WHERE key=?1",
                [PROJECTOR_GENERATION_KEY],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            marker_count, 0,
            "a read-only freshness check must not mutate state"
        );
    }

    #[test]
    fn stale_projector_generation_replays_unchanged_and_appended_sources() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000090";
        let turn = "019f64ab-0000-7000-8000-000000000090";
        let source_message_id = "explicit-source-message";
        let scoped_message_id = projected_message_id(owner, source_message_id);
        write_fixture(
            &file,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({
                    "timestamp":"2026-07-15T09:00:01.500Z",
                    "type":"response_item",
                    "payload":{
                        "type":"message",
                        "id":source_message_id,
                        "role":"user",
                        "content":[{"type":"input_text","text":"Replay this message."}]
                    }
                }),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_one_shot(&db, &roots).unwrap();
        assert!(projector_generation_is_current(&db).unwrap());

        let connection = db.connect().unwrap();
        connection
            .execute(
                "DELETE FROM app_meta WHERE key=?1",
                [PROJECTOR_GENERATION_KEY],
            )
            .unwrap();
        drop(connection);
        assert!(
            !projector_generation_is_current(&db).unwrap(),
            "a nonempty projection without its completed-generation marker is stale"
        );
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO app_meta(key,value) VALUES(?1,?2)",
                params![PROJECTOR_GENERATION_KEY, PROJECTOR_GENERATION.to_string()],
            )
            .unwrap();
        assert!(projector_generation_is_current(&db).unwrap());
        connection
            .execute(
                "UPDATE source_files
                 SET parse_state_json=json_remove(parse_state_json,'$.projector_generation')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE messages SET id=?1 WHERE id=?2",
                params![source_message_id, scoped_message_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE events SET call_id=?1 WHERE call_id=?2",
                params![source_message_id, scoped_message_id],
            )
            .unwrap();
        drop(connection);
        assert!(!projector_generation_is_current(&db).unwrap());

        let replay = scan_one_shot(&db, &roots).unwrap();
        assert_eq!(replay.files_ingested, 1);
        assert_eq!(replay.files_unchanged, 0);
        assert_eq!(replay.records_read, 5);
        assert!(projector_generation_is_current(&db).unwrap());
        let message_identity: (i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT
                    EXISTS(SELECT 1 FROM messages WHERE id=?1),
                    EXISTS(SELECT 1 FROM messages WHERE id=?2)",
                params![source_message_id, scoped_message_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            message_identity,
            (0, 1),
            "generation replay must replace legacy unscoped message IDs"
        );

        let connection = db.connect().unwrap();
        connection
            .execute(
                "UPDATE source_files
                 SET parse_state_json=json_remove(parse_state_json,'$.projector_generation')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE app_meta SET value='0' WHERE key='projector_generation'",
                [],
            )
            .unwrap();
        drop(connection);
        let mut append = File::options().append(true).open(&file).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
        )
        .unwrap();
        drop(append);

        let replay_with_append = scan_one_shot(&db, &roots).unwrap();
        assert_eq!(replay_with_append.files_ingested, 1);
        assert_eq!(replay_with_append.files_unchanged, 0);
        assert_eq!(replay_with_append.records_read, 6);
        assert!(projector_generation_is_current(&db).unwrap());
    }

    #[test]
    fn interrupted_generation_replay_resumes_before_advancing_global_marker() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner_a = "019f64aa-0000-7000-8000-000000000091";
        let owner_b = "019f64aa-0000-7000-8000-000000000092";
        write_fixture(
            &sessions.join("a.jsonl"),
            &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
        );
        write_fixture(
            &sessions.join("b.jsonl"),
            &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_one_shot(&db, &roots).unwrap();

        let connection = db.connect().unwrap();
        connection
            .execute(
                "UPDATE source_files
                 SET parse_state_json=json_remove(parse_state_json,'$.projector_generation')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE app_meta SET value='0' WHERE key='projector_generation'",
                [],
            )
            .unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TRIGGER fail_second_generation_replay
                 BEFORE INSERT ON rollouts
                 WHEN NEW.id='{owner_b}'
                 BEGIN
                   SELECT RAISE(ABORT,'injected generation replay failure');
                 END;"
            ))
            .unwrap();
        drop(connection);

        let error = scan_one_shot(&db, &roots).unwrap_err();
        assert!(
            format!("{error:#}").contains("injected generation replay failure"),
            "unexpected replay error: {error:#}"
        );
        assert!(!projector_generation_is_current(&db).unwrap());
        let generations: (i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT
                    COALESCE(CAST(json_extract(
                        (SELECT parse_state_json FROM source_files WHERE rollout_id=?1),
                        '$.projector_generation'
                    ) AS INTEGER),0),
                    COALESCE(CAST(json_extract(
                        (SELECT parse_state_json FROM source_files WHERE rollout_id=?2),
                        '$.projector_generation'
                    ) AS INTEGER),0)",
                params![owner_a, owner_b],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(generations, (PROJECTOR_GENERATION as i64, 0));

        db.connect()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_second_generation_replay")
            .unwrap();
        let resumed = scan_one_shot(&db, &roots).unwrap();
        assert_eq!(resumed.files_unchanged, 1);
        assert_eq!(resumed.files_ingested, 1);
        assert!(projector_generation_is_current(&db).unwrap());
    }

    #[test]
    fn unchanged_scan_is_idempotent_and_partial_line_waits() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &file,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();
        let ingested_before_unchanged: String = db
            .connect()
            .unwrap()
            .query_row("SELECT ingested_at FROM source_files", [], |row| row.get(0))
            .unwrap();
        reset_fingerprint_bytes_read();
        let second = scan_once(&db, &roots).unwrap();
        #[cfg(unix)]
        assert_eq!(
            fingerprint_bytes_read(),
            0,
            "stable identity must avoid rereading the file for a digest"
        );
        #[cfg(not(unix))]
        assert_eq!(fingerprint_bytes_read(), file.metadata().unwrap().len());
        let connection = db.connect().unwrap();
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(second.files_unchanged, 1);
        let checkpoint_after_unchanged: String = connection
            .query_row("SELECT ingested_at FROM source_files", [], |row| row.get(0))
            .unwrap();
        assert_eq!(checkpoint_after_unchanged, ingested_before_unchanged);
        drop(connection);

        let previous_size = file.metadata().unwrap().len();
        let mut append = File::options().append(true).open(&file).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
        )
        .unwrap();
        drop(append);
        let new_size = file.metadata().unwrap().len();
        reset_fingerprint_bytes_read();
        let third = scan_once(&db, &roots).unwrap();
        assert_eq!(
            fingerprint_bytes_read(),
            previous_size + new_size,
            "growth audits the previous chunk and verifies the prior tail plus suffix"
        );
        assert_eq!(third.files_ingested, 1);
        assert_eq!(third.records_read, 1);
    }

    #[test]
    fn append_during_projection_waits_for_the_next_captured_extent() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("growing-during-scan.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000149";
        let turn = "019f64ab-0000-7000-8000-000000000149";
        write_fixture(
            &file,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let captured_size = file.metadata().unwrap().len();
        let appended_path = file.clone();
        set_process_file_after_snapshot_hook(move |scanned_path| {
            assert_eq!(scanned_path, appended_path);
            let mut append = File::options().append(true).open(&appended_path).unwrap();
            writeln!(
                append,
                "{}",
                serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
            )
            .unwrap();
        });
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        let first = scan_once(&db, &roots).unwrap();
        assert_eq!(first.records_read, 4);
        let first_projection: (i64, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),
                        (SELECT SUM(input_tokens) FROM usage_facts),
                        size_bytes,byte_offset FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            first_projection,
            (1, 100, captured_size as i64, captured_size as i64)
        );

        let second = scan_once(&db, &roots).unwrap();
        assert_eq!(second.records_read, 1);
        let second_projection: (i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*),SUM(input_tokens) FROM usage_facts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(second_projection, (2, 200));

        let third = scan_once(&db, &roots).unwrap();
        assert_eq!(third.files_unchanged, 1);
        let final_count: i64 = db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(final_count, 2);
    }

    #[test]
    fn file_projection_claims_writer_before_read_snapshot_can_go_stale() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("writer-race.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000150";
        write_fixture(&file, &[meta("2026-07-15T09:00:00Z", owner, owner, false)]);
        let competing_db = db.clone();
        let competing_write_committed = Arc::new(AtomicBool::new(false));
        let committed_for_hook = competing_write_committed.clone();
        set_process_file_after_transaction_read_hook(move || {
            let connection = competing_db.connect().unwrap();
            connection.busy_timeout(Duration::ZERO).unwrap();
            if connection
                .execute(
                    "INSERT INTO app_meta(key,value) VALUES('pricing-race-probe','committed')",
                    [],
                )
                .is_ok()
            {
                committed_for_hook.store(true, Ordering::Release);
            }
        });

        let report = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        assert_eq!(report.files_ingested, 1);
        assert!(
            !competing_write_committed.load(Ordering::Acquire),
            "a competing writer committed after the projection read snapshot"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rename_over_after_open_never_projects_the_replacement_under_the_old_owner() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let path = sessions.join("rollout.jsonl");
        let replacement = temp.path().join("replacement.jsonl");
        let owner_a = "019f64aa-0000-7000-8000-000000000201";
        let owner_b = "019f64aa-0000-7000-8000-000000000202";
        let turn_a = "019f64ab-0000-7000-8000-000000000201";
        let turn_b = "019f64ab-0000-7000-8000-000000000202";
        write_fixture(
            &path,
            &[
                meta("2026-07-15T09:00:00Z", owner_a, owner_a, false),
                task("2026-07-15T09:00:01Z", turn_a),
                context("2026-07-15T09:00:01Z", turn_a, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        write_fixture(
            &replacement,
            &[
                meta("2026-07-15T10:00:00Z", owner_b, owner_b, false),
                task("2026-07-15T10:00:01Z", turn_b),
                context("2026-07-15T10:00:01Z", turn_b, "gpt-5.5"),
                usage("2026-07-15T10:00:02Z", 200),
            ],
        );
        let replacement_for_hook = replacement.clone();
        let path_for_hook = path.clone();
        set_process_file_after_snapshot_hook(move |_| {
            std::fs::rename(replacement_for_hook, path_for_hook).unwrap();
        });
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        scan_once(&db, &roots).unwrap();
        let first: (String, String, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT source_files.rollout_id,usage_facts.thread_id,usage_facts.input_tokens
                 FROM source_files JOIN usage_facts USING(rollout_id)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(first, (owner_a.into(), owner_a.into(), 100));

        scan_once(&db, &roots).unwrap();
        let second: (String, String, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT source_files.rollout_id,usage_facts.thread_id,usage_facts.input_tokens,
                        (SELECT COUNT(*) FROM threads)
                 FROM source_files JOIN usage_facts USING(rollout_id)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(second, (owner_b.into(), owner_b.into(), 200, 1));
    }

    #[test]
    fn growing_chunk_checkpoint_reads_only_the_tail_and_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("large.jsonl");
        let original_size = FINGERPRINT_CHUNK_BYTES * 3 + FINGERPRINT_CHUNK_BYTES / 2;
        std::fs::write(&path, vec![b'a'; original_size as usize]).unwrap();
        let previous = full_content_fingerprints(&path, original_size, None)
            .unwrap()
            .current;
        let suffix = vec![b'b'; 4096];
        let mut file = File::options().append(true).open(&path).unwrap();
        file.write_all(&suffix).unwrap();
        drop(file);

        reset_fingerprint_bytes_read();
        let (extended, verified_tail) =
            extend_chunked_fingerprint(&path, original_size + suffix.len() as u64, &previous)
                .unwrap();
        assert!(verified_tail);
        assert!(
            fingerprint_bytes_read() <= FINGERPRINT_CHUNK_BYTES + suffix.len() as u64,
            "append verification must reread at most the prior partial chunk and suffix"
        );
        let rebuilt = full_content_fingerprints(&path, original_size + suffix.len() as u64, None)
            .unwrap()
            .current;
        assert!(extended.same_content(&rebuilt));
    }

    #[test]
    fn bounded_line_reader_drains_complete_records_and_marks_incomplete_tails() {
        let input = b"0123456789\n{}\nabcdefghij";
        let mut reader = BufReader::new(input.as_slice());
        let mut buffer = Vec::new();

        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Complete {
                len: 11,
                oversized: true
            }
        );
        assert!(buffer.is_empty());
        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Complete {
                len: 3,
                oversized: false
            }
        );
        assert_eq!(buffer, b"{}\n");
        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Incomplete {
                len: 10,
                oversized: true
            }
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn rewrite_in_earlier_chunk_plus_append_forces_projection_rebuild() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let path = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &path,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"user_message","message":"rewrite-me-A"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                    "type":"agent_message","message":"x".repeat((2 * FINGERPRINT_CHUNK_BYTES) as usize)
                }}),
            ],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();

        let mut contents = std::fs::read(&path).unwrap();
        let needle = b"rewrite-me-A";
        let offset = contents
            .windows(needle.len())
            .position(|window| window == needle)
            .unwrap();
        contents[offset + needle.len() - 1] = b'B';
        std::fs::write(&path, contents).unwrap();
        let mut append = File::options().append(true).open(&path).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:04Z", 100)).unwrap()
        )
        .unwrap();
        drop(append);

        let report = scan_once(&db, &roots).unwrap();
        assert!(
            report.records_read > 1,
            "a prefix mismatch must rebuild instead of reading only the suffix"
        );
        let connection = db.connect().unwrap();
        let title: String = connection
            .query_row("SELECT title FROM threads WHERE id=?1", [owner], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "rewrite-me-B");
    }

    #[test]
    fn continuously_growing_file_advances_audit_until_old_rewrite_is_found() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let path = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &path,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"agent_message","message":"a".repeat((3 * FINGERPRINT_CHUNK_BYTES + FINGERPRINT_CHUNK_BYTES / 2) as usize)
                }}),
            ],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();

        let rewrite_offset = 2 * FINGERPRINT_CHUNK_BYTES + 128;
        let mut file = File::options().read(true).write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(rewrite_offset)).unwrap();
        let mut original = [0_u8; 1];
        file.read_exact(&mut original).unwrap();
        assert_eq!(original[0], b'a');
        file.seek(SeekFrom::Start(rewrite_offset)).unwrap();
        file.write_all(b"b").unwrap();
        drop(file);

        let mut final_report = None;
        for index in 0..3 {
            let mut append = File::options().append(true).open(&path).unwrap();
            writeln!(
                append,
                "{}",
                serde_json::to_string(&usage(
                    &format!("2026-07-15T09:00:{:02}Z", index + 3),
                    100 + index as u64,
                ))
                .unwrap()
            )
            .unwrap();
            drop(append);
            let report = scan_once(&db, &roots).unwrap();
            if index < 2 {
                assert_eq!(
                    report.records_read, 1,
                    "the rolling audit remains bounded before reaching the changed chunk"
                );
            } else {
                final_report = Some(report);
            }
        }
        assert!(
            final_report.unwrap().records_read > 1,
            "the third rolling step must reach chunk two and rebuild"
        );
    }

    #[test]
    fn every_growing_file_advances_its_audit_when_background_budget_is_exhausted() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let mut paths = Vec::new();
        let mut owners = Vec::new();

        for index in 0..=FINGERPRINT_AUDIT_FILES_PER_SCAN {
            let path = sessions.join(format!("root-{index:02}.jsonl"));
            let owner = format!("019f64aa-0000-7000-8000-{index:012}");
            let turn = format!("019f64ab-0000-7000-8000-{index:012}");
            write_fixture(
                &path,
                &[
                    meta("2026-07-15T09:00:00Z", &owner, &owner, false),
                    task("2026-07-15T09:00:01Z", &turn),
                    context("2026-07-15T09:00:01Z", &turn, "gpt-5.5"),
                    serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                        "type":"user_message","message":"rewrite-me-A"
                    }}),
                    serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                        "type":"agent_message","message":"x".repeat((2 * FINGERPRINT_CHUNK_BYTES) as usize)
                    }}),
                ],
            );
            paths.push(path);
            owners.push(owner);
        }

        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();

        let last_path = paths.last().unwrap();
        let mut contents = std::fs::read(last_path).unwrap();
        let needle = b"rewrite-me-A";
        let offset = contents
            .windows(needle.len())
            .position(|window| window == needle)
            .unwrap();
        contents[offset + needle.len() - 1] = b'B';
        std::fs::write(last_path, contents).unwrap();

        for (index, path) in paths.iter().enumerate() {
            let mut append = File::options().append(true).open(path).unwrap();
            writeln!(
                append,
                "{}",
                serde_json::to_string(&usage(
                    &format!("2026-07-15T09:00:{:02}Z", index + 4),
                    100 + index as u64,
                ))
                .unwrap()
            )
            .unwrap();
        }

        scan_once(&db, &roots).unwrap();
        let title: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT title FROM threads WHERE id=?1",
                [owners.last().unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            title, "rewrite-me-B",
            "the ninth growing file must not be starved by the shared eight-file audit budget"
        );
    }

    #[test]
    fn oversized_incomplete_tail_waits_then_complete_record_is_drained_and_reported() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let path = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let mut file = File::create(&path).unwrap();
        for value in [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        ] {
            writeln!(file, "{}", serde_json::to_string(&value).unwrap()).unwrap();
        }
        write!(file, "{{\"oversized\":\"").unwrap();
        file.write_all(&vec![b'x'; MAX_JSONL_LINE_BYTES]).unwrap();
        drop(file);
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        scan_once(&db, &roots).unwrap();
        let connection = db.connect().unwrap();
        let (offset, size, error): (i64, i64, Option<String>) = connection
            .query_row(
                "SELECT byte_offset,size_bytes,last_error FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert!(offset < size, "an incomplete tail must remain uncommitted");
        assert!(
            error.is_none(),
            "an incomplete tail is not yet a bad record"
        );
        drop(connection);

        let mut append = File::options().append(true).open(&path).unwrap();
        writeln!(append, "\"}}").unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:02Z", 100)).unwrap()
        )
        .unwrap();
        drop(append);
        let error = scan_once(&db, &roots).unwrap_err();
        assert!(error.to_string().contains("record exceeds"));
        let connection = db.connect().unwrap();
        let (usage_count, offset, size, last_error): (i64, i64, i64, String) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset,size_bytes,last_error
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(usage_count, 1, "records after the oversized line survive");
        assert_eq!(
            offset, size,
            "the complete oversized record is checkpointed"
        );
        assert!(last_error.contains(&MAX_JSONL_LINE_BYTES.to_string()));
    }

    #[test]
    fn periodic_chunk_audit_is_bounded_and_detects_rewrites() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("audit.jsonl");
        let size = FINGERPRINT_AUDIT_BYTES_PER_SCAN + 2 * FINGERPRINT_CHUNK_BYTES;
        std::fs::write(&path, vec![b'a'; size as usize]).unwrap();
        let mut fingerprint = full_content_fingerprints(&path, size, None)
            .unwrap()
            .current;
        fingerprint.audit_completed_at = 0;

        reset_fingerprint_bytes_read();
        let mut budget = FingerprintAuditBudget::default();
        assert!(matches!(
            audit_chunked_fingerprint(&path, &mut fingerprint, &mut budget).unwrap(),
            FingerprintAudit::Verified { changed: true }
        ));
        assert_eq!(fingerprint_bytes_read(), FINGERPRINT_AUDIT_BYTES_PER_FILE);
        assert_eq!(fingerprint.audit_cursor, 1);

        let mut file = File::options().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(FINGERPRINT_CHUNK_BYTES)).unwrap();
        file.write_all(b"corrupt").unwrap();
        drop(file);
        let mut budget = FingerprintAuditBudget::default();
        assert!(matches!(
            audit_chunked_fingerprint(&path, &mut fingerprint, &mut budget).unwrap(),
            FingerprintAudit::Mismatch
        ));
    }

    #[test]
    fn legacy_unattributed_usage_is_ignored_but_current_usage_remains_visible() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let legacy_owner = "019a0000-0000-7000-8000-000000000001";
        let current_owner = "019f0000-0000-7000-8000-000000000001";

        write_fixture(
            &sessions.join("legacy.jsonl"),
            &[
                meta("2025-11-21T12:00:00Z", legacy_owner, legacy_owner, false),
                usage("2025-11-21T12:00:01Z", 100),
            ],
        );
        write_fixture(
            &sessions.join("current.jsonl"),
            &[
                meta("2026-01-02T12:00:00Z", current_owner, current_owner, false),
                usage("2026-01-02T12:00:01Z", 200),
            ],
        );

        scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();

        let connection = db.connect().unwrap();
        let rows = connection
            .prepare("SELECT thread_id,model,total_tokens FROM usage_facts ORDER BY timestamp")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows, vec![(current_owner.into(), "unknown".into(), 201)]);
    }

    #[test]
    fn valid_short_prefix_observation_preserves_committed_projection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let prefix = vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        let mut complete = prefix.clone();
        complete.push(usage("2026-07-15T09:00:03Z", 200));
        write_fixture(&file, &complete);
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();

        write_fixture(&file, &prefix);
        let deferred = scan_once(&db, &roots).unwrap();
        assert_eq!(deferred.files_ingested, 0);
        assert_eq!(deferred.records_read, 0);
        let connection = db.connect().unwrap();
        let (usage_count, committed_offset): (i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(usage_count, 2, "one short observation cannot erase facts");
        assert!(
            committed_offset > file.metadata().unwrap().len() as i64,
            "the complete committed boundary remains authoritative while the shrink is pending"
        );
        let pending: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM app_meta WHERE key=?1",
                [pending_source_shrink_key(owner)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            pending, 1,
            "the deferred candidate must survive for the next scan"
        );
    }

    #[test]
    fn stable_same_path_shrink_is_accepted_on_repeat() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let prefix = vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        let mut complete = prefix.clone();
        complete.push(usage("2026-07-15T09:00:03Z", 200));
        write_fixture(&file, &complete);
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();

        write_fixture(&file, &prefix);
        scan_once(&db, &roots).unwrap();
        let accepted = scan_once(&db, &roots).unwrap();
        assert_eq!(accepted.files_ingested, 1);
        assert_eq!(accepted.records_read, prefix.len() as u64);
        let connection = db.connect().unwrap();
        let (usage_count, committed_offset, stored_size): (i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset,size_bytes
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(usage_count, 1, "a stable shrink becomes authoritative");
        assert_eq!(committed_offset, file.metadata().unwrap().len() as i64);
        assert_eq!(stored_size, committed_offset);
        let pending: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM app_meta WHERE key=?1",
                [pending_source_shrink_key(owner)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            pending, 0,
            "the accepted candidate marker must be cleared atomically"
        );
    }

    #[test]
    fn same_size_rewrite_rebuilds_rollout() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let make = |input| {
            vec![
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", input),
            ]
        };
        write_fixture(&file, &make(100));
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();
        write_fixture(&file, &make(900));
        scan_once(&db, &roots).unwrap();
        let connection = db.connect().unwrap();
        let input: i64 = connection
            .query_row("SELECT SUM(input_tokens) FROM usage_facts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(input, 900);
    }

    #[test]
    fn large_same_size_middle_rewrite_rebuilds_rollout() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let fixture = |content: String| {
            vec![
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"response_item","payload":{
                    "type":"message","id":"large-message","role":"user",
                    "content":[{"type":"input_text","text":content}]
                }}),
            ]
        };
        let original = "a".repeat(200_000);
        write_fixture(&file, &fixture(original));
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();

        std::thread::sleep(Duration::from_millis(2));
        let mut rewritten = "a".repeat(200_000).into_bytes();
        rewritten[100_000] = b'b';
        write_fixture(&file, &fixture(String::from_utf8(rewritten).unwrap()));
        scan_once(&db, &roots).unwrap();

        let connection = db.connect().unwrap();
        let content: String = connection
            .query_row(
                "SELECT content FROM messages WHERE id=?1",
                [projected_message_id(owner, "large-message")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(content.len(), 200_000);
        assert_eq!(content.as_bytes()[100_000], b'b');
    }

    #[test]
    fn malformed_complete_line_is_reported_while_later_records_survive() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        let prefix = vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        ];
        let mut malformed = File::create(&file).unwrap();
        for value in &prefix {
            writeln!(malformed, "{}", serde_json::to_string(value).unwrap()).unwrap();
        }
        writeln!(malformed, "{{\"broken\":}}").unwrap();
        writeln!(
            malformed,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:02Z", 100)).unwrap()
        )
        .unwrap();
        drop(malformed);
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        let error = scan_once(&db, &roots).unwrap_err();
        assert!(error.to_string().contains("line 4"));
        let connection = db.connect().unwrap();
        let (usage_count, offset, size, line_number, error): (i64, i64, i64, i64, String) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset,size_bytes,line_number,last_error
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(
            usage_count, 1,
            "valid records after the malformed line survive"
        );
        assert_eq!(offset, size, "the complete malformed line is checkpointed");
        assert_eq!(line_number, 5);
        assert!(error.contains("line 4"));
        drop(connection);

        let unchanged_error = scan_once(&db, &roots).unwrap_err();
        assert!(unchanged_error.to_string().contains("line 4"));
        let connection = db.connect().unwrap();
        let usage_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(usage_count, 1);
        drop(connection);

        let mut append = File::options().append(true).open(&file).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
        )
        .unwrap();
        drop(append);
        let appended_error = scan_once(&db, &roots).unwrap_err();
        assert!(appended_error.to_string().contains("line 4"));
        let connection = db.connect().unwrap();
        let (usage_count, last_error): (i64, String) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),last_error
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(usage_count, 2, "valid appended records remain projectable");
        assert!(last_error.contains("line 4"));
        drop(connection);

        std::thread::sleep(Duration::from_millis(2));
        let mut corrected = prefix;
        corrected.push(usage("2026-07-15T09:00:02Z", 100));
        write_fixture(&file, &corrected);
        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(pending.files_ingested, 0);
        let connection = db.connect().unwrap();
        let (usage_count, last_error): (i64, Option<String>) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),last_error
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(usage_count, 2, "the first shrink observation stays pending");
        assert!(last_error.is_some());
        drop(connection);

        let corrected = scan_once(&db, &roots).unwrap();
        assert_eq!(corrected.files_failed, 0);
        let connection = db.connect().unwrap();
        let (usage_count, last_error): (i64, Option<String>) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),last_error
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(usage_count, 1);
        assert!(last_error.is_none());
    }

    #[test]
    fn malformed_file_does_not_suppress_reconciliation_in_an_enumerated_root() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let active_owner = "019f64aa-0000-7000-8000-000000000101";
        let archived_owner = "019f64aa-0000-7000-8000-000000000102";
        let malformed_owner = "019f64aa-0000-7000-8000-000000000103";
        write_fixture(
            &active.join("active.jsonl"),
            &[meta(
                "2026-07-15T09:00:00Z",
                active_owner,
                active_owner,
                false,
            )],
        );
        let archived_path = archive.join("archived.jsonl");
        write_fixture(
            &archived_path,
            &[meta(
                "2026-07-15T09:00:00Z",
                archived_owner,
                archived_owner,
                false,
            )],
        );
        let roots = IngestRoots {
            active: Some(active.clone()),
            archive: Some(archive),
        };
        scan_once(&db, &roots).unwrap();

        std::fs::remove_file(archived_path).unwrap();
        let malformed_path = active.join("malformed.jsonl");
        let mut malformed = File::create(&malformed_path).unwrap();
        writeln!(
            malformed,
            "{}",
            serde_json::to_string(&meta(
                "2026-07-15T09:00:00Z",
                malformed_owner,
                malformed_owner,
                false,
            ))
            .unwrap()
        )
        .unwrap();
        writeln!(malformed, "{{\"broken\":}}").unwrap();
        drop(malformed);

        let error = scan_once(&db, &roots).unwrap_err();
        assert!(error.to_string().contains("line 2"));
        let connection = db.connect().unwrap();
        let archived_source: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM source_files WHERE rollout_id=?1",
                [archived_owner],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            archived_source, 0,
            "a malformed active source must not keep a deleted archived rollout alive"
        );
    }

    #[test]
    fn traversal_failure_protects_sources_under_the_incomplete_root() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let active_owner = "019f64aa-0000-7000-8000-000000000111";
        let archived_owner = "019f64aa-0000-7000-8000-000000000112";
        write_fixture(
            &active.join("active.jsonl"),
            &[meta(
                "2026-07-15T09:00:00Z",
                active_owner,
                active_owner,
                false,
            )],
        );
        write_fixture(
            &archive.join("archived.jsonl"),
            &[meta(
                "2026-07-15T09:00:00Z",
                archived_owner,
                archived_owner,
                false,
            )],
        );
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive.clone()),
        };
        scan_once(&db, &roots).unwrap();
        std::fs::remove_dir_all(&archive).unwrap();

        let error = scan_once(&db, &roots).unwrap_err();
        assert!(error.to_string().contains("configured ingest root"));
        let connection = db.connect().unwrap();
        let archived_source: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM source_files WHERE rollout_id=?1",
                [archived_owner],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            archived_source, 1,
            "an incomplete traversal must never be interpreted as deletion"
        );
    }

    #[test]
    fn token_counts_outside_fixed_point_domain_fail_without_wrapping() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000121";
        let turn = "019f64ab-0000-7000-8000-000000000121";
        write_fixture(
            &sessions.join("overflow.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", MAX_USAGE_TOKENS_PER_FACT + 1),
            ],
        );

        let error = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("input_tokens"));
        let connection = db.connect().unwrap();
        let stored: (i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM usage_facts)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (0, 0));
    }

    #[test]
    fn malformed_token_accounting_is_rejected_instead_of_discarded() {
        let cases = [
            (
                "negative",
                serde_json::json!({"total_token_usage":{"input_tokens":-1}}),
                "invalid total_token_usage",
            ),
            (
                "wrong-type",
                serde_json::json!({"total_token_usage":{"input_tokens":"100"}}),
                "invalid total_token_usage",
            ),
            (
                "cached-exceeds-input",
                serde_json::json!({"total_token_usage":{
                    "input_tokens":100,"cached_input_tokens":101,"output_tokens":1
                }}),
                "cached_input_tokens greater than input_tokens",
            ),
            (
                "non-object-info",
                serde_json::json!(["not", "an", "object"]),
                "token_count.info with a non-object value",
            ),
        ];

        for (label, info, expected) in cases {
            let temp = tempfile::tempdir().unwrap();
            let db = Db::open(temp.path().join("usage.db")).unwrap();
            let sessions = temp.path().join("sessions");
            std::fs::create_dir(&sessions).unwrap();
            let owner = "019f64aa-0000-7000-8000-000000000122";
            let turn = "019f64ab-0000-7000-8000-000000000122";
            write_fixture(
                &sessions.join(format!("{label}.jsonl")),
                &[
                    meta("2026-07-15T09:00:00Z", owner, owner, false),
                    task("2026-07-15T09:00:01Z", turn),
                    context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                    serde_json::json!({"timestamp":"2026-07-15T09:00:02Z",
                        "type":"event_msg","payload":{
                            "type":"token_count","info":info
                        }
                    }),
                ],
            );

            let error = scan_once(
                &db,
                &IngestRoots {
                    active: Some(sessions),
                    archive: None,
                },
            )
            .unwrap_err();
            assert!(
                format!("{error:#}").contains(expected),
                "{label} produced unexpected error: {error:#}"
            );
            let connection = db.connect().unwrap();
            let stored: (i64, i64) = connection
                .query_row(
                    "SELECT (SELECT COUNT(*) FROM source_files),
                            (SELECT COUNT(*) FROM usage_facts)",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(stored, (0, 0), "{label} left a partial projection");
        }
    }

    #[test]
    fn absent_null_and_legacy_omitted_token_fields_remain_supported() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000123";
        let turn = "019f64ab-0000-7000-8000-000000000123";
        write_fixture(
            &sessions.join("legacy-token-usage.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z",
                    "type":"event_msg","payload":{"type":"token_count"}}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z",
                    "type":"event_msg","payload":{"type":"token_count","info":null}}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:04Z",
                "type":"event_msg","payload":{"type":"token_count","info":{
                    "total_token_usage":null,"last_token_usage":null
                }}}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:05Z",
                "type":"event_msg","payload":{"type":"token_count","info":{
                    "last_token_usage":{"input_tokens":7,"output_tokens":2}
                }}}),
            ],
        );

        let report = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        assert_eq!(report.files_failed, 0);
        let projected: (i64, i64, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*),SUM(input_tokens),SUM(cached_input_tokens),
                        SUM(output_tokens),SUM(total_tokens)
                 FROM usage_facts",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(projected, (1, 7, 0, 2, 9));
    }

    #[test]
    fn explicit_null_token_info_resets_cumulative_scope_without_usage() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000148";
        let turn = "019f64ab-0000-7000-8000-000000000148";
        let snapshot = |timestamp: &str, input: u64, cached: u64| {
            serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
                "type":"token_count","info":{
                    "total_token_usage":{
                        "input_tokens":input,"cached_input_tokens":cached,
                        "output_tokens":1,"total_tokens":input+1
                    },
                    "last_token_usage":{
                        "input_tokens":input,"cached_input_tokens":cached,
                        "output_tokens":1,"total_tokens":input+1
                    }
                }
            }})
        };
        let null_boundary = |timestamp: &str| {
            serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
                "type":"token_count","info":null
            }})
        };
        write_fixture(
            &sessions.join("null-token-scope-boundary.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                snapshot("2026-07-15T09:00:02Z", 100, 20),
                null_boundary("2026-07-15T09:00:03Z"),
                null_boundary("2026-07-15T09:00:04Z"),
                snapshot("2026-07-15T09:00:05Z", 110, 35),
                snapshot("2026-07-15T09:00:06Z", 110, 35),
            ],
        );

        let report = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        assert_eq!(report.files_failed, 0);
        let projected: (i64, i64, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*),SUM(input_tokens),SUM(cached_input_tokens),
                        SUM(output_tokens),SUM(total_tokens)
                 FROM usage_facts",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(projected, (2, 210, 55, 2, 212));
    }

    #[test]
    fn cached_input_delta_greater_than_input_delta_is_rejected_not_clamped() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000124";
        let turn = "019f64ab-0000-7000-8000-000000000124";
        let snapshot = |timestamp: &str, input: u64, cached: u64| {
            serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
                "type":"token_count","info":{"total_token_usage":{
                    "input_tokens":input,"cached_input_tokens":cached,
                    "output_tokens":1,"total_tokens":input+1
                }}
            }})
        };
        write_fixture(
            &sessions.join("invalid-cached-delta.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                snapshot("2026-07-15T09:00:02Z", 100, 0),
                snapshot("2026-07-15T09:00:03Z", 110, 15),
            ],
        );

        let error = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}")
                .contains("derived token usage.cached_input_tokens greater than input_tokens"),
            "unexpected cached delta error: {error:#}"
        );
        let stored: (i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM usage_facts)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (0, 0));
    }

    #[cfg(unix)]
    #[test]
    fn scan_waits_for_database_advisory_lock() {
        use std::sync::mpsc;

        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        let guard = DatabaseLock::acquire(&db, "ingest").unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            completed_tx.send(scan_once(&db, &roots)).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            completed_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "scan acquired an advisory lock already held by another handle"
        );

        drop(guard);
        completed_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn projector_failure_rolls_back_file_and_retries_without_advancing_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000000";
        let turn = "019f64ab-0000-7000-8000-000000000000";
        write_fixture(
            &sessions.join("root.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"force_projector_error","detail":"valid JSON that must be retried"
                }}),
                usage("2026-07-15T09:00:03Z", 100),
            ],
        );
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_projector_record BEFORE INSERT ON events
                 WHEN NEW.label='force_projector_error'
                 BEGIN SELECT RAISE(FAIL,'forced projector failure'); END;",
            )
            .unwrap();
        drop(connection);
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        let error = scan_once(&db, &roots).unwrap_err();
        assert!(error.to_string().contains("forced projector failure"));
        let connection = db.connect().unwrap();
        let rolled_back: (i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM threads),
                        (SELECT COUNT(*) FROM usage_facts)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(rolled_back, (0, 0, 0));
        connection
            .execute_batch("DROP TRIGGER fail_projector_record;")
            .unwrap();
        drop(connection);

        let retried = scan_once(&db, &roots).unwrap();
        assert_eq!(retried.files_failed, 0);
        let connection = db.connect().unwrap();
        let projected: (i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM events WHERE label='force_projector_error'),
                        (SELECT COUNT(*) FROM usage_facts)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(projected, (1, 1, 1));
    }

    #[test]
    fn zero_byte_existing_source_preserves_projection_until_path_is_absent() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("root.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000131";
        let turn = "019f64ab-0000-7000-8000-000000000131";
        write_fixture(
            &file,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();
        let checkpoint_before: (i64, i64, i64, String, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT size_bytes,byte_offset,line_number,content_fingerprint,ingested_at
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();

        File::create(&file).unwrap();
        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(pending.files_seen, 0);
        assert_eq!(pending.files_failed, 0);
        let connection = db.connect().unwrap();
        let checkpoint_after: (i64, i64, i64, String, String) = connection
            .query_row(
                "SELECT size_bytes,byte_offset,line_number,content_fingerprint,ingested_at
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        let projected_while_pending: (i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(checkpoint_after, checkpoint_before);
        assert_eq!(projected_while_pending, (1, 1, 1));
        drop(connection);

        std::fs::remove_file(&file).unwrap();
        scan_once(&db, &roots).unwrap();
        let connection = db.connect().unwrap();
        let projected_after_deletion: (i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(projected_after_deletion, (0, 0, 0));
    }

    #[test]
    fn zero_byte_archive_handoff_preserves_projection_until_destination_is_populated() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000135";
        let turn = "019f64ab-0000-7000-8000-000000000135";
        let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
        let active_path = active.join(&filename);
        let archive_path = archive.join(&filename);
        let records = [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        write_fixture(&active_path, &records);
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };

        scan_once(&db, &roots).unwrap();
        let projection = || {
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT path,archived,
                            (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                            (SELECT COUNT(*) FROM threads WHERE id=?1),
                            (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                    [owner],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    },
                )
                .optional()
                .unwrap()
        };
        let active_projection = projection().unwrap();
        assert_eq!(active_projection.0, active_path.to_string_lossy());
        assert_eq!(active_projection.1, 0);
        assert_eq!(active_projection.2, 1);
        assert_eq!(active_projection.3, 1);
        assert_eq!(active_projection.4, 1);
        assert_eq!(active_projection.5, 100);

        std::fs::remove_file(&active_path).unwrap();
        File::create(&archive_path).unwrap();
        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(pending.files_seen, 0);
        assert_eq!(pending.files_failed, 0);
        assert_eq!(
            projection().unwrap(),
            active_projection,
            "an empty archive destination must not erase the active projection"
        );

        write_fixture(&archive_path, &records);
        let populated = scan_once(&db, &roots).unwrap();
        assert_eq!(populated.files_ingested, 1);
        assert_eq!(populated.files_failed, 0);
        let archived_projection = projection().unwrap();
        assert_eq!(archived_projection.0, archive_path.to_string_lossy());
        assert_eq!(archived_projection.1, 1);
        assert_eq!(archived_projection.2, 1);
        assert_eq!(archived_projection.3, 1);
        assert_eq!(archived_projection.4, 1);
        assert_eq!(archived_projection.5, 100);
    }

    #[test]
    fn non_uuid_incomplete_archive_handoff_preserves_only_its_matching_projection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000145";
        let turn = "019f64ab-0000-7000-8000-000000000145";
        let active_path = active.join("friendly-session-name.jsonl");
        let archive_path = archive.join("friendly-session-name.jsonl");
        let records = [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        write_fixture(&active_path, &records);
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive.clone()),
        };
        scan_once(&db, &roots).unwrap();

        std::fs::remove_file(&active_path).unwrap();
        let metadata = serde_json::to_vec(&records[0]).unwrap();
        std::fs::write(&archive_path, &metadata[..metadata.len() / 2]).unwrap();
        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(pending.files_failed, 0);
        let preserved: (i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(preserved, (1, 1, 1));

        std::fs::remove_file(&archive_path).unwrap();
        std::fs::write(
            archive.join("unrelated-name.jsonl"),
            &metadata[..metadata.len() / 2],
        )
        .unwrap();
        let unrelated = scan_once(&db, &roots).unwrap();
        assert_eq!(unrelated.files_failed, 0);
        let deleted: (i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            deleted,
            (0, 0, 0),
            "an unrelated incomplete placeholder preserved a deleted projection"
        );
    }

    #[test]
    fn complete_malformed_handoff_reports_failure_without_erasing_committed_projection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000146";
        let turn = "019f64ab-0000-7000-8000-000000000146";
        let active_path = active.join("named-session.jsonl");
        write_fixture(
            &active_path,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive.clone()),
        };
        scan_once(&db, &roots).unwrap();

        std::fs::remove_file(&active_path).unwrap();
        std::fs::write(
            archive.join("named-session.jsonl"),
            b"{\"timestamp\":\"2026-07-15T09:00:00Z\",\"type\":\"session_meta\",\"payload\":{}}\n",
        )
        .unwrap();
        let error = scan_once(&db, &roots).unwrap_err();
        assert!(
            format!("{error:#}").contains("has no rollout id"),
            "unexpected handoff error: {error:#}"
        );
        let projected: (String, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT path,
                        (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(projected.0, active_path.to_string_lossy());
        assert_eq!(
            (projected.1, projected.2, projected.3),
            (1, 1, 1),
            "a failed complete handoff erased the last committed projection"
        );
        let ingest_error: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT value FROM app_meta WHERE key='last_ingest_error'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ingest_error.contains("has no rollout id"));
    }

    #[test]
    fn archive_readiness_uses_the_committed_offset_not_an_unfinished_raw_tail() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000147";
        let turn = "019f64ab-0000-7000-8000-000000000147";
        let active_path = active.join("committed-prefix.jsonl");
        let archive_path = archive.join("committed-prefix.jsonl");
        let records = [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        write_fixture(&active_path, &records);
        let mut writer = std::fs::OpenOptions::new()
            .append(true)
            .open(&active_path)
            .unwrap();
        let unfinished = serde_json::to_vec(&usage("2026-07-15T09:00:03Z", 150)).unwrap();
        writer
            .write_all(&unfinished[..unfinished.len() / 2])
            .unwrap();
        drop(writer);
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };
        scan_once(&db, &roots).unwrap();

        let raw_fingerprint =
            full_content_fingerprints(&active_path, active_path.metadata().unwrap().len(), None)
                .unwrap()
                .current
                .encode()
                .unwrap();
        db.connect()
            .unwrap()
            .execute(
                "UPDATE source_files SET content_fingerprint=?1 WHERE rollout_id=?2",
                params![raw_fingerprint, owner],
            )
            .unwrap();
        let upgraded = scan_once(&db, &roots).unwrap();
        assert_eq!(upgraded.files_unchanged, 1);

        let (raw_size, committed_size, stored_fingerprint): (i64, i64, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT size_bytes,byte_offset,content_fingerprint
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert!(raw_size > committed_size);
        assert_eq!(
            ChunkedFingerprint::parse(&stored_fingerprint).unwrap().size,
            committed_size as u64,
            "the handoff fingerprint included an uncommitted tail"
        );

        std::fs::remove_file(&active_path).unwrap();
        write_fixture(&archive_path, &records);
        assert_eq!(
            archive_path.metadata().unwrap().len(),
            committed_size as u64
        );
        let report = scan_once(&db, &roots).unwrap();
        assert_eq!(report.files_ingested, 1);
        assert_eq!(report.files_failed, 0);
        let projection: (String, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,byte_offset,
                        (SELECT COALESCE(SUM(input_tokens),0)
                         FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(projection.0, archive_path.to_string_lossy());
        assert_eq!(projection.1, 1);
        assert_eq!(projection.2, committed_size);
        assert_eq!(projection.3, 100);
    }

    #[test]
    fn partial_archive_handoff_waits_for_previously_committed_extent() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000138";
        let turn = "019f64ab-0000-7000-8000-000000000138";
        let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
        let active_path = active.join(&filename);
        let archive_path = archive.join(&filename);
        let records = [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        write_fixture(&active_path, &records);
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };

        scan_once(&db, &roots).unwrap();
        let projection = || {
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT path,archived,size_bytes,byte_offset,line_number,
                            (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                    [owner],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                        ))
                    },
                )
                .unwrap()
        };
        let active_projection = projection();
        assert_eq!(active_projection.0, active_path.to_string_lossy());
        assert_eq!(active_projection.1, 0);
        assert_eq!(active_projection.3, active_projection.2);
        assert_eq!(active_projection.4, 4);
        assert_eq!(active_projection.5, 1);
        assert_eq!(active_projection.6, 100);

        std::fs::remove_file(&active_path).unwrap();
        let metadata_record = serde_json::to_vec(&records[0]).unwrap();
        std::fs::write(&archive_path, &metadata_record[..metadata_record.len() / 2]).unwrap();
        let partial_owner = scan_once(&db, &roots).unwrap();
        assert_eq!(partial_owner.files_seen, 1);
        assert_eq!(partial_owner.files_ingested, 0);
        assert_eq!(partial_owner.files_failed, 0);
        assert_eq!(
            projection(),
            active_projection,
            "an archive with a partial owner record erased the complete active projection"
        );

        write_fixture(&archive_path, &[records[0].clone()]);
        assert!(
            archive_path.metadata().unwrap().len() < active_projection.3 as u64,
            "the metadata-only archive must still be a partial handoff"
        );
        let metadata_only = scan_once(&db, &roots).unwrap();
        assert_eq!(metadata_only.files_seen, 1);
        assert_eq!(metadata_only.files_ingested, 0);
        assert_eq!(metadata_only.files_failed, 0);
        assert_eq!(
            projection(),
            active_projection,
            "a metadata-only archive replaced the complete active projection"
        );

        let mut partial_archive = File::create(&archive_path).unwrap();
        for record in &records[..3] {
            writeln!(
                partial_archive,
                "{}",
                serde_json::to_string(record).unwrap()
            )
            .unwrap();
        }
        let trailing_record = serde_json::to_vec(&records[3]).unwrap();
        partial_archive
            .write_all(&trailing_record[..trailing_record.len() / 2])
            .unwrap();
        drop(partial_archive);
        assert!(
            archive_path.metadata().unwrap().len() < active_projection.3 as u64,
            "the longer archive with a trailing partial record must remain below the committed extent"
        );
        let trailing_partial = scan_once(&db, &roots).unwrap();
        assert_eq!(trailing_partial.files_seen, 1);
        assert_eq!(trailing_partial.files_ingested, 0);
        assert_eq!(trailing_partial.files_failed, 0);
        assert_eq!(
            projection(),
            active_projection,
            "a longer but incomplete archive replaced the complete active projection"
        );

        let mut preallocated = File::create(&archive_path).unwrap();
        preallocated.set_len(active_projection.2 as u64).unwrap();
        writeln!(
            preallocated,
            "{}",
            serde_json::to_string(&records[0]).unwrap()
        )
        .unwrap();
        preallocated
            .seek(SeekFrom::Start(active_projection.2 as u64 - 1))
            .unwrap();
        preallocated.write_all(b"\n").unwrap();
        drop(preallocated);
        assert!(source_is_complete(
            &archive_path,
            active_projection.2 as u64
        ));
        let sparse_partial = scan_once(&db, &roots).unwrap();
        assert_eq!(sparse_partial.files_seen, 1);
        assert_eq!(sparse_partial.files_ingested, 0);
        assert_eq!(sparse_partial.files_failed, 0);
        assert_eq!(
            projection(),
            active_projection,
            "a preallocated archive destination replaced the complete active projection"
        );

        write_fixture(&archive_path, &records);
        let complete = scan_once(&db, &roots).unwrap();
        assert_eq!(complete.files_ingested, 1);
        assert_eq!(complete.files_failed, 0);
        let archived_projection = projection();
        assert_eq!(archived_projection.0, archive_path.to_string_lossy());
        assert_eq!(archived_projection.1, 1);
        assert_eq!(archived_projection.2, active_projection.2);
        assert_eq!(archived_projection.3, active_projection.3);
        assert_eq!(archived_projection.4, active_projection.4);
        assert_eq!(archived_projection.5, active_projection.5);
        assert_eq!(archived_projection.6, active_projection.6);
    }

    #[test]
    fn handoff_revalidates_the_opened_snapshot_before_replacing_projection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000178";
        let turn = "019f64ab-0000-7000-8000-000000000178";
        let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
        let active_path = active.join(&filename);
        let archive_path = archive.join(&filename);
        let records = [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        write_fixture(&active_path, &records);
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };
        scan_once(&db, &roots).unwrap();

        std::fs::remove_file(&active_path).unwrap();
        write_fixture(&archive_path, &records);
        let archive_for_hook = archive_path.clone();
        let metadata_only = records[0].clone();
        set_process_file_before_open_hook(move |path| {
            assert_eq!(path, archive_for_hook);
            write_fixture(&archive_for_hook, &[metadata_only]);
        });

        let report = scan_once(&db, &roots).unwrap();
        assert_eq!(report.files_ingested, 0);
        assert_eq!(report.files_failed, 0);
        let projection: (String, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,byte_offset,
                        (SELECT COALESCE(SUM(input_tokens),0)
                         FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(projection.0, active_path.to_string_lossy());
        assert_eq!(projection.1, 0);
        assert!(projection.2 > 0);
        assert_eq!(projection.3, 100);

        let still_partial = scan_once(&db, &roots).unwrap();
        assert_eq!(still_partial.files_ingested, 0);
        let preserved_tokens: i64 = db
            .connect()
            .unwrap()
            .query_row("SELECT SUM(input_tokens) FROM usage_facts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(preserved_tokens, 100);
    }

    #[test]
    fn partial_active_restore_preserves_archived_projection_until_copy_is_complete() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000139";
        let turn = "019f64ab-0000-7000-8000-000000000139";
        let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
        let active_path = active.join(&filename);
        let archive_path = archive.join(&filename);
        let records = [
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ];
        write_fixture(&archive_path, &records);
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };

        scan_once(&db, &roots).unwrap();
        let projection = || {
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT path,archived,size_bytes,byte_offset,line_number,
                            (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                    [owner],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                        ))
                    },
                )
                .unwrap()
        };
        let archived_projection = projection();
        assert_eq!(archived_projection.0, archive_path.to_string_lossy());
        assert_eq!(archived_projection.1, 1);

        std::fs::remove_file(&archive_path).unwrap();
        let mut preallocated = File::create(&active_path).unwrap();
        preallocated.set_len(archived_projection.2 as u64).unwrap();
        writeln!(
            preallocated,
            "{}",
            serde_json::to_string(&records[0]).unwrap()
        )
        .unwrap();
        preallocated
            .seek(SeekFrom::Start(archived_projection.2 as u64 - 1))
            .unwrap();
        preallocated.write_all(b"\n").unwrap();
        drop(preallocated);

        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(pending.files_seen, 1);
        assert_eq!(pending.files_ingested, 0);
        assert_eq!(pending.files_failed, 0);
        assert_eq!(
            projection(),
            archived_projection,
            "a partial active restore replaced the complete archived projection"
        );

        write_fixture(&active_path, &records);
        let complete = scan_once(&db, &roots).unwrap();
        assert_eq!(complete.files_ingested, 1);
        assert_eq!(complete.files_failed, 0);
        let active_projection = projection();
        assert_eq!(active_projection.0, active_path.to_string_lossy());
        assert_eq!(active_projection.1, 0);
        assert_eq!(active_projection.2, archived_projection.2);
        assert_eq!(active_projection.3, archived_projection.3);
        assert_eq!(active_projection.4, archived_projection.4);
        assert_eq!(active_projection.5, archived_projection.5);
        assert_eq!(active_projection.6, archived_projection.6);
    }

    #[test]
    fn zero_byte_archive_placeholder_does_not_freeze_appending_active_source() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000137";
        let turn = "019f64ab-0000-7000-8000-000000000137";
        let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
        let active_path = active.join(&filename);
        let archive_path = archive.join(&filename);
        write_fixture(
            &active_path,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };
        scan_once(&db, &roots).unwrap();

        File::create(&archive_path).unwrap();
        let mut active_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&active_path)
            .unwrap();
        writeln!(
            active_file,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:03Z", 150)).unwrap()
        )
        .unwrap();
        drop(active_file);

        let report = scan_once(&db, &roots).unwrap();
        assert_eq!(report.files_ingested, 1);
        assert_eq!(report.files_failed, 0);
        assert_eq!(report.records_read, 1);
        let connection = db.connect().unwrap();
        let projection: (String, i64, i64, i64, i64) = connection
            .query_row(
                "SELECT path,archived,line_number,
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                        (SELECT COALESCE(SUM(input_tokens),0)
                         FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(projection.0, active_path.to_string_lossy());
        assert_eq!(projection.1, 0);
        assert_eq!(projection.2, 5);
        assert_eq!(projection.3, 2);
        assert_eq!(projection.4, 150);
    }

    #[test]
    fn unrelated_zero_byte_archive_placeholder_does_not_preserve_deleted_rollout() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000136";
        let turn = "019f64ab-0000-7000-8000-000000000136";
        let active_path = active.join(format!("rollout-2026-07-15T09-00-00-{owner}.jsonl"));
        write_fixture(
            &active_path,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive.clone()),
        };
        scan_once(&db, &roots).unwrap();

        std::fs::remove_file(active_path).unwrap();
        File::create(
            archive.join("rollout-2026-07-15T09-00-00-019f64aa-0000-7000-8000-000000000999.jsonl"),
        )
        .unwrap();
        scan_once(&db, &roots).unwrap();

        let connection = db.connect().unwrap();
        let projection: (i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(projection, (0, 0, 0));
    }

    #[test]
    fn zero_byte_selected_active_source_keeps_archived_duplicate_deferred() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let active_path = active.join("duplicate.jsonl");
        let archive_path = archive.join("duplicate.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000133";
        let turn = "019f64ab-0000-7000-8000-000000000133";
        let fixture = |input| {
            vec![
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", input),
            ]
        };
        write_fixture(&active_path, &fixture(100));
        write_fixture(&archive_path, &fixture(100));
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };

        scan_once(&db, &roots).unwrap();
        let snapshot = || {
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT path,archived,size_bytes,byte_offset,line_number,
                            content_fingerprint,ingested_at,
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                    [owner],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, i64>(7)?,
                        ))
                    },
                )
                .unwrap()
        };
        let selected_active = snapshot();
        assert_eq!(selected_active.0, active_path.to_string_lossy());
        assert_eq!(selected_active.1, 0);
        assert_eq!(selected_active.7, 100);

        File::create(&active_path).unwrap();
        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(
            pending.files_seen, 1,
            "the archived duplicate is still observed"
        );
        assert_eq!(
            snapshot(),
            selected_active,
            "an archived duplicate replaced the checkpoint for an existing empty active owner"
        );

        std::fs::remove_file(&active_path).unwrap();
        scan_once(&db, &roots).unwrap();
        let selected_archive = snapshot();
        assert_eq!(selected_archive.0, archive_path.to_string_lossy());
        assert_eq!(selected_archive.1, 1);
        assert_eq!(selected_archive.7, 100);
    }

    #[test]
    fn zero_byte_selected_archive_source_keeps_active_duplicate_deferred() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let active = temp.path().join("sessions");
        let archive = temp.path().join("archived_sessions");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let active_path = active.join("duplicate.jsonl");
        let archive_path = archive.join("duplicate.jsonl");
        let owner = "019f64aa-0000-7000-8000-000000000134";
        let turn = "019f64ab-0000-7000-8000-000000000134";
        let fixture = |input| {
            vec![
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", input),
            ]
        };
        write_fixture(&archive_path, &fixture(1_000));
        let roots = IngestRoots {
            active: Some(active),
            archive: Some(archive),
        };

        scan_once(&db, &roots).unwrap();
        let snapshot = || {
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT path,archived,size_bytes,byte_offset,line_number,
                            content_fingerprint,ingested_at,
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                    [owner],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, i64>(7)?,
                        ))
                    },
                )
                .unwrap()
        };
        let selected_archive = snapshot();
        assert_eq!(selected_archive.0, archive_path.to_string_lossy());
        assert_eq!(selected_archive.1, 1);
        assert_eq!(selected_archive.7, 1_000);

        write_fixture(&active_path, &fixture(1_000));
        File::create(&archive_path).unwrap();
        let pending = scan_once(&db, &roots).unwrap();
        assert_eq!(
            pending.files_seen, 1,
            "the active duplicate is still observed"
        );
        assert_eq!(
            snapshot(),
            selected_archive,
            "an active duplicate replaced the checkpoint for an existing empty archive owner"
        );

        std::fs::remove_file(&archive_path).unwrap();
        scan_once(&db, &roots).unwrap();
        let selected_active = snapshot();
        assert_eq!(selected_active.0, active_path.to_string_lossy());
        assert_eq!(selected_active.1, 0);
        assert_eq!(selected_active.7, 1_000);
    }

    #[test]
    fn empty_rollout_placeholder_waits_then_ingests_when_populated() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let file = sessions.join("rollout-empty.jsonl");
        File::create(&file).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000132";
        let turn = "019f64ab-0000-7000-8000-000000000132";
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };

        let report = scan_once(&db, &roots).unwrap();
        assert_eq!(report.files_seen, 0);
        assert_eq!(report.files_failed, 0);
        let connection = db.connect().unwrap();
        let sources: i64 = connection
            .query_row("SELECT COUNT(*) FROM source_files", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sources, 0);
        drop(connection);

        write_fixture(
            &file,
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100),
            ],
        );
        let populated = scan_once(&db, &roots).unwrap();
        assert_eq!(populated.files_ingested, 1);
        let connection = db.connect().unwrap();
        let projected: (i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(projected, (1, 1));
    }

    #[test]
    fn absent_root_configuration_is_an_ingest_error() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let error = scan_once(
            &db,
            &IngestRoots {
                active: None,
                archive: None,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("no ingest roots are configured"));
        let connection = db.connect().unwrap();
        let (state, detail): (String, String) = connection
            .query_row(
                "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "error");
        assert!(detail.contains("no ingest roots are configured"));
    }

    #[test]
    fn interrupted_scanning_state_is_recovered_under_the_ingest_lock() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        set_meta(&db, "ingest_state", "scanning").unwrap();

        assert!(recover_interrupted_scan(&db).unwrap());
        let recovered: (String, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(recovered.0, "error");
        assert!(recovered.1.contains("exited before completing"));
        assert!(!recover_interrupted_scan(&db).unwrap());
    }

    #[test]
    fn unexpected_scan_error_is_finalized_after_scanning_begins() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        set_scan_after_start_hook(|_| Err(anyhow!("injected post-start scan failure")));

        let error = scan_once(
            &db,
            &IngestRoots {
                active: None,
                archive: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("injected post-start scan failure"),
            "unexpected error: {error:#}"
        );

        let metadata: (String, String, String, String) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_attempt_at'),
                    (SELECT value FROM app_meta WHERE key='last_scan_report')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(metadata.0, "error");
        assert!(metadata.1.contains("injected post-start scan failure"));
        assert!(!metadata.2.is_empty());
        let report: ScanReport = serde_json::from_str(&metadata.3).unwrap();
        assert_eq!(report.files_seen, 0);
        assert_eq!(report.files_failed, 0);
    }

    #[test]
    fn scan_finalizer_failure_does_not_replace_the_original_error() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        set_scan_after_start_hook(|db| {
            db.connect()?.execute_batch(
                "CREATE TRIGGER reject_scan_finalizer
                 BEFORE UPDATE ON app_meta
                 WHEN OLD.key='ingest_state' AND NEW.value<>'scanning'
                 BEGIN
                   SELECT RAISE(ABORT,'injected finalizer failure');
                 END;",
            )?;
            Err(anyhow!("original post-start scan failure"))
        });

        let error = scan_once(
            &db,
            &IngestRoots {
                active: None,
                archive: None,
            },
        )
        .unwrap_err();
        let detail = format!("{error:#}");
        assert!(detail.contains("original post-start scan failure"));
        assert!(!detail.contains("injected finalizer failure"));

        let connection = db.connect().unwrap();
        let state: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='ingest_state'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "scanning");
        connection
            .execute_batch("DROP TRIGGER reject_scan_finalizer")
            .unwrap();
    }

    #[test]
    fn clearing_and_reinserting_rollouts_recomputes_exact_thread_bounds() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('root','thread','2026-07-10T00:00:00Z','2026-07-15T00:00:00Z'),
                    ('child','thread','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z'),
                    ('promoted-grandchild','thread','2026-07-11T00:00:00Z','2026-07-11T00:00:00Z');
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,status
                 ) VALUES
                 (
                    'synthetic-grandchild','thread',NULL,'child',
                    '2026-07-02T00:00:00Z','running'
                 ),
                 (
                    'promoted-grandchild','thread','promoted-grandchild','child',
                    '2026-07-11T00:00:00Z','completed'
                 );",
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        clear_rollout(&transaction, "child").unwrap();
        transaction.commit().unwrap();
        let bounds: (String, String) = connection
            .query_row(
                "SELECT started_at,last_event_at FROM threads WHERE id='thread'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            bounds,
            ("2026-07-10T00:00:00Z".into(), "2026-07-15T00:00:00Z".into())
        );
        let synthetic_agents: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE id='synthetic-grandchild'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(synthetic_agents, 0);
        let promoted_agents: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE id='promoted-grandchild'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(promoted_agents, 1);

        connection
            .execute_batch(
                "DELETE FROM rollouts;
                 UPDATE threads SET
                    started_at='2026-07-01T00:00:00Z',
                    last_event_at='2026-07-20T00:00:00Z';",
            )
            .unwrap();
        let owner = OwnerMeta {
            owner_id: "root".into(),
            thread_id: "thread".into(),
            parent_rollout_id: None,
            parent_thread_id: None,
            agent_path: None,
            agent_nickname: None,
            is_subagent: false,
            forked: false,
            timestamp: "2026-07-12T00:00:00Z".into(),
            cwd: None,
            project: None,
            repository_url: None,
            branch: None,
            source: None,
            thread_source: None,
            source_json: None,
        };
        let transaction = connection.transaction().unwrap();
        upsert_owner(&transaction, &owner, false).unwrap();
        transaction.commit().unwrap();
        let bounds: (String, String) = connection
            .query_row(
                "SELECT started_at,last_event_at FROM threads WHERE id='thread'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            bounds,
            ("2026-07-12T00:00:00Z".into(), "2026-07-12T00:00:00Z".into())
        );
    }

    #[test]
    fn reparented_rollout_removes_its_abandoned_former_thread() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let parent = "019f64aa-0000-7000-8000-000000000096";
        let child = "019f64ab-0000-7000-8000-000000000096";
        let parent_path = sessions.join("parent.jsonl");
        let child_path = sessions.join("child.jsonl");
        write_fixture(
            &parent_path,
            &[meta("2026-07-15T09:00:00Z", parent, parent, false)],
        );
        write_fixture(
            &child_path,
            &[meta("2026-07-15T09:00:00Z", child, child, false)],
        );
        let roots = IngestRoots {
            active: Some(sessions),
            archive: None,
        };
        scan_once(&db, &roots).unwrap();
        let initial_threads: i64 = db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(initial_threads, 2);

        write_fixture(
            &child_path,
            &[legacy_child_meta("2026-07-15T09:00:00Z", child, parent)],
        );
        scan_once(&db, &roots).unwrap();

        let connection = db.connect().unwrap();
        let child_thread: String = connection
            .query_row(
                "SELECT thread_id FROM rollouts WHERE id=?1",
                [child],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_thread, parent);
        let former_thread_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM threads WHERE id=?1)",
                [child],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!former_thread_exists);
    }

    #[test]
    fn token_snapshots_reject_contradictory_totals_and_reasoning() {
        for (field, usage, expected) in [
            (
                "total_token_usage",
                serde_json::json!({
                    "input_tokens":10,"output_tokens":5,
                    "reasoning_output_tokens":1,"total_tokens":999
                }),
                "total_tokens inconsistent",
            ),
            (
                "last_token_usage",
                serde_json::json!({
                    "input_tokens":10,"output_tokens":5,
                    "reasoning_output_tokens":99,"total_tokens":15
                }),
                "reasoning_output_tokens greater",
            ),
        ] {
            let info = serde_json::json!({field:usage});
            let error = parse_token_usage(&info, field, 7).unwrap_err().to_string();
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn total_only_last_usage_hint_is_ignored_without_double_counting() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000779";
        let turn = "019f64ab-0000-7000-8000-000000000779";
        let snapshot = |timestamp: &str, total: Value, last: Value| {
            serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
                "type":"token_count","info":{
                    "total_token_usage":total,
                    "last_token_usage":last
                }
            }})
        };
        write_fixture(
            &sessions.join("total-only-last-hint.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                snapshot(
                    "2026-07-15T09:00:02Z",
                    serde_json::json!({
                        "input_tokens":0,"cached_input_tokens":0,
                        "output_tokens":0,"reasoning_output_tokens":0,
                        "total_tokens":0
                    }),
                    serde_json::json!({
                        "input_tokens":0,"cached_input_tokens":0,
                        "output_tokens":0,"reasoning_output_tokens":0,
                        "total_tokens":18596
                    }),
                ),
                snapshot(
                    "2026-07-15T09:00:03Z",
                    serde_json::json!({
                        "input_tokens":36526,"cached_input_tokens":23936,
                        "output_tokens":404,"reasoning_output_tokens":210,
                        "total_tokens":36930
                    }),
                    serde_json::json!({
                        "input_tokens":36526,"cached_input_tokens":23936,
                        "output_tokens":404,"reasoning_output_tokens":210,
                        "total_tokens":36930
                    }),
                ),
                snapshot(
                    "2026-07-15T09:00:04Z",
                    serde_json::json!({
                        "input_tokens":10,"cached_input_tokens":4,
                        "output_tokens":2,"reasoning_output_tokens":1,
                        "total_tokens":12
                    }),
                    serde_json::json!({
                        "input_tokens":0,"cached_input_tokens":0,
                        "output_tokens":0,"reasoning_output_tokens":0,
                        "total_tokens":2048
                    }),
                ),
            ],
        );

        let report = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap();
        assert_eq!(report.files_failed, 0);
        let stored: (i64, i64, i64, i64, i64) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*),SUM(input_tokens),SUM(cached_input_tokens),
                        SUM(output_tokens),SUM(total_tokens)
                 FROM usage_facts",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(stored, (2, 36536, 23940, 406, 36942));
    }

    #[test]
    fn total_only_last_usage_without_cumulative_counter_is_rejected() {
        let info = serde_json::json!({"last_token_usage":{
            "input_tokens":0,"cached_input_tokens":0,
            "output_tokens":0,"reasoning_output_tokens":0,
            "total_tokens":18596
        }});
        assert!(last_token_usage_is_total_only_hint(&info));
        let error = parse_token_usage(&info, "last_token_usage", 7)
            .unwrap_err()
            .to_string();
        assert!(error.contains("total_tokens inconsistent"));
    }

    #[test]
    fn cumulative_context_window_offset_is_normalized_without_guessing_components() {
        let sentinel = serde_json::json!({
            "model_context_window":258400,
            "total_token_usage":{
                "input_tokens":0,"cached_input_tokens":0,
                "output_tokens":0,"reasoning_output_tokens":0,
                "total_tokens":258400
            }
        });
        assert_eq!(
            parse_total_token_usage(&sentinel, 7)
                .unwrap()
                .unwrap()
                .total_tokens,
            0
        );

        let cumulative = serde_json::json!({
            "model_context_window":258400,
            "total_token_usage":{
                "input_tokens":223027,"cached_input_tokens":215424,
                "output_tokens":673,"reasoning_output_tokens":265,
                "total_tokens":482100
            }
        });
        let usage = parse_total_token_usage(&cumulative, 8).unwrap().unwrap();
        assert_eq!(usage.input_tokens, 223027);
        assert_eq!(usage.output_tokens, 673);
        assert_eq!(usage.total_tokens, 223700);

        let unrelated_mismatch = serde_json::json!({
            "model_context_window":258400,
            "total_token_usage":{
                "input_tokens":223027,"cached_input_tokens":215424,
                "output_tokens":673,"reasoning_output_tokens":265,
                "total_tokens":482101
            }
        });
        let error = parse_total_token_usage(&unrelated_mismatch, 9)
            .unwrap_err()
            .to_string();
        assert!(error.contains("total_tokens inconsistent"));
    }

    #[test]
    fn aggregate_overflow_rolls_back_the_raw_usage_fact() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000777";
        let turn = "019f64ab-0000-7000-8000-000000000777";
        write_fixture(
            &sessions.join("overflow.jsonl"),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 1),
            ],
        );

        let maximum_safe = crate::MAX_JS_SAFE_INTEGER as i64;
        db.connect()
            .unwrap()
            .execute(
                "UPDATE usage_global_totals SET
                    fact_count=?1,input_tokens=?1-1,cached_input_tokens=0,
                    output_tokens=1,reasoning_tokens=0,total_tokens=?1
                 WHERE id=1",
                [maximum_safe],
            )
            .unwrap();

        let error = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("failed"));

        let connection = db.connect().unwrap();
        let state: (i64, i64, i64, i64) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM usage_facts),
                    (SELECT COUNT(*) FROM usage_activity_rollups),
                    fact_count,total_tokens
                 FROM usage_global_totals WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, (0, 0, maximum_safe, maximum_safe));
    }
}
