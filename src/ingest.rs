use crate::{
    db::Db,
    model::TokenUsage,
    process_lock::DatabaseLock,
    redaction::{redact_data_urls, serialize_redacted_json},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
        if other.failed {
            self.files_failed += 1;
        } else if other.unchanged {
            self.files_unchanged += 1;
        } else {
            self.files_ingested += 1;
        }
        self.records_read += other.records;
        self.inherited_records_skipped += other.inherited;
    }
}

#[derive(Debug, Default)]
struct FileReport {
    unchanged: bool,
    failed: bool,
    records: u64,
    inherited: u64,
    error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct CursorState {
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

#[derive(Clone, Debug)]
struct SourceCandidate {
    path: PathBuf,
    archived: bool,
    size: u64,
    complete: bool,
    owner: OwnerMeta,
}

pub fn scan_once(db: &Db, roots: &IngestRoots) -> Result<ScanReport> {
    // A scan is one reconciliation transaction at the application level even
    // though individual source files commit independently. Serialize that full
    // decision window across processes that share this database.
    let _scan_guard = DatabaseLock::acquire(db, "ingest")?;
    set_meta(db, "ingest_state", "scanning")?;
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
    let mut candidates_by_owner: HashMap<String, Vec<SourceCandidate>> = HashMap::new();
    let mut owners = HashMap::new();
    for (path, archived) in files {
        match peek_owner(&path) {
            Ok(owner) => {
                let size = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
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
                report.files_failed += 1;
                failures.push(format!("{}: {error}", path.display()));
                tracing::warn!(path = %path.display(), %error, "failed to ingest rollout");
            }
        }
    }
    let pending_owners = owners_with_pending_empty_sources(db, &pending_empty)?;
    let mut selected = candidates_by_owner
        .into_iter()
        .filter(|(owner_id, _)| !pending_owners.contains(owner_id))
        .filter_map(|(_, candidates)| candidates.into_iter().max_by(source_candidate_preference))
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    for candidate in &selected {
        owners.insert(candidate.owner.owner_id.clone(), candidate.owner.clone());
    }
    resolve_owner_topology(db, &mut owners)?;
    let mut audit_budget = FingerprintAuditBudget::default();
    for candidate in selected {
        let Some(owner) = owners.get(&candidate.owner.owner_id) else {
            report.files_failed += 1;
            tracing::warn!(path = %candidate.path.display(), "failed to resolve rollout owner");
            continue;
        };
        match process_file(
            db,
            &candidate.path,
            candidate.archived,
            owner,
            &mut audit_budget,
        ) {
            Ok(file_report) => {
                if let Some(error) = &file_report.error {
                    failures.push(format!("{}: {error}", candidate.path.display()));
                }
                report.merge(file_report);
            }
            Err(error) => {
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
    if previous_signature.as_deref() == Some(root_signature.as_str()) {
        // Reconciliation depends on enumeration completeness, not projection
        // success. One malformed file must not keep deleted rollouts alive in
        // another root that was enumerated successfully, while any root whose
        // traversal failed remains untouched.
        reconcile_missing(db, &observed, &enumerated_roots, &incomplete_roots)?;
    } else if report.files_failed == 0 {
        // A root change may intentionally expose a different source set.
        // Adopt it after one clean scan, then reconcile only if the next
        // clean scan confirms the same configuration.
        set_meta(db, "ingest_root_signature", &root_signature)?;
    }
    sync_session_index_titles(db, roots)?;
    let now = canonical_utc(Utc::now());
    let report_json = serde_json::to_string(&report)?;
    if report.files_failed == 0 {
        finish_scan_meta(db, &now, &report_json, None)?;
        Ok(report)
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
        let Some(id) = value
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
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
            title: redact_data_urls(title),
            updated_at: canonical_utc(timestamp.with_timezone(&Utc)),
            updated_micros: timestamp.timestamp_micros(),
            line_number,
        };
        let replace = latest.get(id).is_none_or(|current| {
            (candidate.updated_micros, candidate.line_number)
                >= (current.updated_micros, current.line_number)
        });
        if replace {
            latest.insert(id.to_owned(), candidate);
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

pub fn spawn_scanner(db: Db, roots: IngestRoots, interval: Duration) -> Arc<AtomicBool> {
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            if let Err(error) = scan_once(&db, &roots) {
                tracing::warn!(%error, "ingest scan failed");
                let _ = set_meta(&db, "ingest_state", "error");
            }
            let slices = (interval.as_millis() / 250).max(1) as usize;
            for _ in 0..slices {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    });
    stopped
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

fn owners_with_pending_empty_sources(
    db: &Db,
    pending_empty: &HashSet<String>,
) -> Result<HashSet<String>> {
    if pending_empty.is_empty() {
        return Ok(HashSet::new());
    }
    let connection = db.connect()?;
    let mut statement = connection.prepare("SELECT rollout_id,path FROM source_files")?;
    Ok(statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|row| match row {
            Ok((owner_id, path)) if pending_empty.contains(&path) => Some(Ok(owner_id)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<std::result::Result<HashSet<_>, _>>()?)
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
    audit_budget: &mut FingerprintAuditBudget,
) -> Result<FileReport> {
    let metadata = path
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;
    let size = metadata.len();
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|value| value.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or_default();
    let identity = file_identity(&metadata);
    let mut connection = db.connect()?;
    let path_text = path.to_string_lossy();
    let checkpoint_by_path = load_checkpoint_by_path(&connection, &path_text)?;
    let mut audit_mismatch = false;
    // On Unix, ctime cannot be restored by ordinary file-writing APIs. The
    // complete size/mtime/ctime/device/inode tuple therefore makes the common
    // unchanged scan constant-time. Chunk checkpoints are audited on a bounded
    // rolling schedule so the append-only assumption is verified rather than
    // trusted forever.
    if let Some(checkpoint) = checkpoint_by_path.as_ref()
        && checkpoint.state.owner_id == resolved_owner.owner_id
        && checkpoint.size == size
        && checkpoint.modified_ns == modified_ns
        && identity.is_complete()
        && checkpoint.identity == identity
        && checkpoint.state.thread_id == resolved_owner.thread_id
    {
        if let Some(mut fingerprint) = ChunkedFingerprint::parse(&checkpoint.fingerprint)
            && fingerprint.audit_due(Utc::now().timestamp())
        {
            match audit_chunked_fingerprint(path, &mut fingerprint, audit_budget)? {
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
    let owner = resolved_owner.clone();
    let existing = load_checkpoint(&connection, &owner.owner_id)?;
    let append_checkpoint = existing.as_ref().filter(|value| {
        size > value.size && value.offset <= value.size && value.state.thread_id == owner.thread_id
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
        match audit_growing_chunked_fingerprint(path, &mut previous)? {
            FingerprintAudit::Mismatch => {
                let full = full_content_fingerprints(path, size, None)?;
                (full.current.encode()?, false)
            }
            FingerprintAudit::Verified { .. } => {
                let (fingerprint, verified_tail) =
                    extend_chunked_fingerprint(path, size, &previous)?;
                if verified_tail {
                    (fingerprint.encode()?, true)
                } else {
                    let full = full_content_fingerprints(path, size, None)?;
                    (full.current.encode()?, false)
                }
            }
        }
    } else {
        let prefix_size = append_checkpoint.map(|checkpoint| checkpoint.size);
        let full = full_content_fingerprints(path, size, prefix_size)?;

        // A metadata-only change (touch, rename over the same bytes, or the
        // first scan after adopting chunk checkpoints) refreshes metadata
        // without rebuilding the normalized projection.
        if let Some(checkpoint) = checkpoint_by_path.as_ref()
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

    let transaction = connection.transaction()?;
    if let Some((replaced_owner, replaced_thread)) = transaction
        .query_row(
            "SELECT rollout_id,root_thread_id FROM source_files WHERE path=?1 AND rollout_id<>?2",
            params![path.to_string_lossy(), owner.owner_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
    {
        clear_rollout(&transaction, &replaced_owner)?;
        transaction.execute(
            "DELETE FROM source_files WHERE rollout_id=?1",
            [&replaced_owner],
        )?;
        if let Some(replaced_thread) = replaced_thread {
            transaction.execute(
                "DELETE FROM threads WHERE id=?1
                 AND NOT EXISTS(SELECT 1 FROM rollouts WHERE thread_id=?1)",
                [&replaced_thread],
            )?;
        }
    }
    if !append {
        clear_rollout(&transaction, &owner.owner_id)?;
        if owner.owner_id == owner.thread_id {
            transaction.execute(
                "UPDATE threads SET title=NULL,title_updated_at=NULL WHERE id=?1",
                [&owner.thread_id],
            )?;
        }
    }
    upsert_owner(&transaction, &owner, archived)?;

    let mut reader = BufReader::new(File::open(path)?);
    reader.seek(SeekFrom::Start(offset))?;
    let mut source_line = line_number;
    let mut committed_offset = offset;
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
        let line_start = reader.stream_position()?;
        let oversized = match read_bounded_line(&mut reader, &mut bytes, MAX_JSONL_LINE_BYTES)? {
            BoundedLine::Eof => break,
            BoundedLine::Incomplete { .. } => {
                reader.seek(SeekFrom::Start(line_start))?;
                break;
            }
            BoundedLine::Complete { oversized, .. } => oversized,
        };
        let line_end = reader.stream_position()?;
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
    transaction.commit()?;

    Ok(FileReport {
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
        let info = payload.get("info").unwrap_or(&Value::Null);
        let total = info
            .get("total_token_usage")
            .and_then(|value| serde_json::from_value::<TokenUsage>(value.clone()).ok());
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
        let last = info
            .get("last_token_usage")
            .and_then(|value| serde_json::from_value::<TokenUsage>(value.clone()).ok());
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
            usage.total_tokens = usage.input_tokens.saturating_add(usage.output_tokens);
        }
        let ignore_legacy_unattributed_usage = state.current_model.is_none()
            && timestamp
                .get(..4)
                .and_then(|year| year.parse::<i32>().ok())
                .is_some_and(|year| year < MODEL_ATTRIBUTION_REQUIRED_FROM_YEAR);
        if !usage.is_zero() && !ignore_legacy_unattributed_usage {
            let input_tokens = checked_token_count(usage.input_tokens, "input_tokens", line)?;
            let cached_input_tokens = checked_token_count(
                usage.cached_input_tokens.min(usage.input_tokens),
                "cached_input_tokens",
                line,
            )?;
            let output_tokens = checked_token_count(usage.output_tokens, "output_tokens", line)?;
            let reasoning_tokens = checked_token_count(
                usage.reasoning_output_tokens,
                "reasoning_output_tokens",
                line,
            )?;
            let total_tokens = checked_token_count(usage.total_tokens, "total_tokens", line)?;
            ensure_turn(tx, state, timestamp)?;
            tx.execute(
                "INSERT OR IGNORE INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    model,effort,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,1)",
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
            let turn_id = payload.get("turn_id").and_then(Value::as_str).unwrap_or("");
            if is_owner_native_turn(&state.owner_id, turn_id) {
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
            state.current_turn = payload
                .get("turn_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
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
    let explicit_turn_id = payload
        .get("internal_chat_message_metadata_passthrough")
        .and_then(|value| value.get("turn_id"))
        .and_then(Value::as_str);
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
    ) && let Some(turn_id) = explicit_turn_id
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
                let id = payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
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
            let call_id = payload
                .get("call_id")
                .or_else(|| payload.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
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
            let call_id = payload
                .get("call_id")
                .or_else(|| payload.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            complete_tool_call(tx, state, timestamp, call_id, None, None, None)?;
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
            let call_id = payload
                .get("id")
                .or_else(|| payload.get("call_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
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
            value_to_text(payload).as_deref(),
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
            if let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) {
                if let Some(previous_turn) = state.current_turn.as_deref()
                    && previous_turn != turn_id
                    && turn_has_open_native_lifecycle(tx, previous_turn)
                {
                    tx.execute(
                        "UPDATE turns
                         SET completed_at=?1,status='interrupted'
                         WHERE id=?2 AND status='running'",
                        params![timestamp, previous_turn],
                    )?;
                }
                state.current_turn = Some(turn_id.to_owned());
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
            if let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) {
                state.current_turn = Some(turn_id.to_owned());
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
                    payload.get("duration_ms").and_then(Value::as_i64),
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
                payload.get("duration_ms").and_then(Value::as_i64),
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
                .map(redact_data_urls)
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
            if let Some(agent_id) = payload.get("agent_thread_id").and_then(Value::as_str) {
                let activity = payload
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("running");
                let status = if activity == "completed" {
                    "completed"
                } else {
                    "running"
                };
                tx.execute(
                    "INSERT INTO agent_runs(
                        id,thread_id,rollout_id,parent_rollout_id,agent_path,started_at,status,completed_at
                     ) VALUES(?1,?2,NULL,?3,?4,?5,?6,?7)
                     ON CONFLICT(id) DO UPDATE SET
                        agent_path=COALESCE(excluded.agent_path,agent_runs.agent_path),
                        status=excluded.status,completed_at=COALESCE(excluded.completed_at,agent_runs.completed_at)",
                    params![agent_id,state.thread_id,state.owner_id,
                        payload.get("agent_path").and_then(Value::as_str),timestamp,status,
                        (status=="completed").then_some(timestamp)],
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
                .map(redact_data_urls)
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
            if let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) {
                state.current_turn = Some(turn_id.to_owned());
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
            if let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) {
                state.current_turn = Some(turn_id.to_owned());
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
            if let Some(call_id) = payload.get("call_id").and_then(Value::as_str) {
                let duration = duration_ms(payload.get("duration"))
                    .or_else(|| payload.get("duration_ms").and_then(Value::as_i64));
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
                    call_id,
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
            if let Some(call_id) = payload.get("call_id").and_then(Value::as_str) {
                let duration = duration_ms(payload.get("duration"))
                    .or_else(|| payload.get("duration_ms").and_then(Value::as_i64));
                let failed = payload.get("success").and_then(Value::as_bool) == Some(false)
                    || payload.get("error").is_some_and(|value| !value.is_null());
                let status = if failed { "failed" } else { "completed" };
                enrich_tool_call(
                    tx,
                    state,
                    timestamp,
                    call_id,
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
            let mut call_id = payload
                .get("call_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
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
                .or_else(|| payload.get("duration_ms").and_then(Value::as_i64));
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
            value_to_text(payload).as_deref(),
            None,
            None,
            None,
            payload,
        )?,
    }
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
    let call_id = payload
        .get("call_id")
        .or_else(|| payload.get("id"))
        .and_then(Value::as_str);
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
    let redacted_label = label.map(redact_data_urls);
    let redacted_body = normalized_body.map(redact_data_urls);
    let redacted_tool_name = tool_name.map(redact_data_urls);
    let payload_json = if let Some((_, metadata)) = compaction.as_ref() {
        Some(serialize_redacted_json(metadata)?)
    } else if matches!(kind, "system" | "subagent" | "goal" | "plan" | "state") {
        Some(serialize_redacted_json(payload)?)
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
            status,
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
    let cwd = payload.get("cwd").and_then(Value::as_str);
    let project = cwd.and_then(|value| Path::new(value).file_name()?.to_str());
    let git = payload.get("git").unwrap_or(&Value::Null);
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
            root_metadata_seen=MAX(root_metadata_seen,?1),
            last_event_at=MAX(last_event_at,?6)
         WHERE id=?7",
        params![
            is_root as i64,
            cwd,
            project,
            git.get("repository_url").and_then(Value::as_str),
            git.get("branch").and_then(Value::as_str),
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
            .map(redact_data_urls)
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
            agent_path=excluded.agent_path,nickname=excluded.nickname",
        params![
            owner.owner_id,
            owner.thread_id,
            owner.parent_rollout_id,
            owner.agent_path,
            owner.agent_nickname,
            owner.timestamp,
        ],
    )?;
    Ok(())
}

fn clear_rollout(tx: &Transaction<'_>, rollout_id: &str) -> Result<()> {
    tx.execute("DELETE FROM usage_facts WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM events WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM messages WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM tool_calls WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM turns WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM agent_runs WHERE rollout_id=?1", [rollout_id])?;
    tx.execute("DELETE FROM rollouts WHERE id=?1", [rollout_id])?;
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

fn peek_owner(path: &Path) -> Result<OwnerMeta> {
    let file = File::open(path)?;
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
        let owner_id = payload
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("first session_meta has no rollout id"))?
            .to_owned();
        let subagent = payload
            .get("source")
            .and_then(|value| value.get("subagent"));
        let spawn = subagent.and_then(|value| value.get("thread_spawn"));
        let explicit_thread_id = payload
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let spawn_parent_thread_id = spawn
            .and_then(|value| value.get("parent_thread_id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        // Older child rollouts omit session_id. Their parent thread is the
        // only top-level ownership signal and must not become a fake session.
        let thread_id = explicit_thread_id
            .clone()
            .or_else(|| spawn_parent_thread_id.clone())
            .unwrap_or_else(|| owner_id.clone());
        let parent_thread_id =
            spawn_parent_thread_id.or_else(|| (owner_id != thread_id).then(|| thread_id.clone()));
        let parent_rollout_id = payload
            .get("forked_from_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                spawn
                    .and_then(|value| value.get("parent_rollout_id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .or_else(|| parent_thread_id.clone());
        let agent_path = spawn
            .and_then(|value| value.get("agent_path"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let agent_nickname = spawn
            .and_then(|value| value.get("agent_nickname"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                subagent
                    .and_then(|value| value.get("other"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        let cwd = payload
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let project = cwd
            .as_deref()
            .and_then(|value| Path::new(value).file_name()?.to_str())
            .map(str::to_owned);
        let git = payload.get("git").unwrap_or(&Value::Null);
        let source_value = payload.get("source");
        let source = source_value
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| subagent.map(|_| "subagent".to_owned()));
        let thread_source = payload
            .get("thread_source")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let source_json = source_value.and_then(|value| serde_json::to_string(value).ok());
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
            cwd,
            project,
            repository_url: git
                .get("repository_url")
                .and_then(Value::as_str)
                .map(str::to_owned),
            branch: git.get("branch").and_then(Value::as_str).map(str::to_owned),
            source,
            thread_source,
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
    if prefix_size.is_some_and(|prefix| prefix > size) {
        return Err(anyhow!("fingerprint prefix exceeds file size"));
    }
    let mut file = File::open(path)?;
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

fn extend_chunked_fingerprint(
    path: &Path,
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
    let mut file = File::open(path)?;
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

fn audit_chunked_fingerprint(
    path: &Path,
    fingerprint: &mut ChunkedFingerprint,
    budget: &mut FingerprintAuditBudget,
) -> Result<FingerprintAudit> {
    if budget.files_remaining == 0 || budget.bytes_remaining == 0 {
        return Ok(FingerprintAudit::Verified { changed: false });
    }
    let original_cursor = fingerprint.audit_cursor;
    let original_completed_at = fingerprint.audit_completed_at;
    let mut file = File::open(path)?;
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

fn audit_growing_chunked_fingerprint(
    path: &Path,
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
    audit_chunked_fingerprint(path, fingerprint, &mut budget)
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
    Ok(canonical_utc(parsed.with_timezone(&Utc)))
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
}

fn event_id(state: &CursorState, line: u64) -> String {
    format!("{}:{line}", state.owner_id)
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

fn looks_like_uuid7(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
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
    i64::try_from(value)
        .map_err(|_| anyhow!("source line {line} has {field} outside the supported integer range"))
}

fn reconcile_missing(
    db: &Db,
    observed: &HashSet<String>,
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
    let transaction = connection.transaction()?;
    for (rollout_id, path, thread_id) in sources {
        if observed.contains(&path) {
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
        clear_rollout(&transaction, &rollout_id)?;
        transaction.execute(
            "DELETE FROM source_files WHERE rollout_id=?1",
            [&rollout_id],
        )?;
        if let Some(thread_id) = thread_id {
            transaction.execute(
                "DELETE FROM threads WHERE id=?1
                 AND NOT EXISTS(SELECT 1 FROM rollouts WHERE thread_id=?1)",
                [&thread_id],
            )?;
        }
    }
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

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

        let goal_payload: String = connection
            .query_row(
                "SELECT payload_json FROM events WHERE thread_id=?1 AND kind='goal'",
                [owner],
                |row| row.get(0),
            )
            .unwrap();
        let goal_payload: Value = serde_json::from_str(&goal_payload).unwrap();
        assert_eq!(
            goal_payload["goal"]["objective"],
            Value::String("Check [embedded attachment]".into())
        );
        assert_eq!(
            goal_payload["goal"]["evidence"]["image"],
            Value::String("[embedded attachment]".into())
        );

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
        let plan: (String, String, String) = connection
            .query_row(
                "SELECT body,status,payload_json FROM events
                 WHERE rollout_id=?1 AND kind='plan'",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(plan.0, "Inspect, implement, verify.");
        assert_eq!(plan.1, "completed");
        assert!(plan.2.contains("item_completed"));
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
        let goals: Vec<(String, String, String)> = connection
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
        assert!(goals[0].2.contains("\"tokensUsed\":100"));
        assert_eq!(goals[1].0, "Build faithful ingestion.");
        assert_eq!(goals[1].1, "complete");
        assert!(goals[1].2.contains("\"tokensUsed\":400"));
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
                "SELECT content FROM messages WHERE id='large-message'",
                [],
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
    fn token_counts_outside_sqlite_integer_range_fail_without_wrapping() {
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
                usage("2026-07-15T09:00:02Z", i64::MAX as u64 + 1),
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
        write_fixture(&archive_path, &fixture(200));
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
        assert_eq!(selected_archive.7, 200);
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
        write_fixture(&active_path, &fixture(100));
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
        assert_eq!(selected_active.7, 100);
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
}
