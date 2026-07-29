use super::{
    attempt::AttemptRecorder,
    catalog::{
        CatalogSelectionPlan, PreparedSourceCandidate, SourceCandidate, SourceHandoffIndex,
        collect_jsonl, owners_with_pending_empty_sources, plan_catalog_selection,
        resolve_owner_topology, source_is_complete,
    },
    checkpoint_store::load_selected_source_extents,
    file_ingestor::{FileIngestor, FileReport},
    owner_reader::read_owner,
    projection::load_existing_owner_threads,
    reconciliation::reconcile_missing,
    session_titles::sync_session_index_titles,
    source::{IngestPacing, is_compressed_rollout_path},
};
use crate::{
    calendar::canonical_utc_timestamp,
    storage::{DatabaseLock, Db, canonicalize_storage_path},
};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
    time::Duration,
};

#[cfg(test)]
type ScanAfterStartHook = Box<dyn FnOnce(&Db) -> Result<()>>;

#[cfg(test)]
thread_local! {
    static SCAN_AFTER_START_HOOK: std::cell::RefCell<Option<ScanAfterStartHook>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
pub(super) fn set_scan_after_start_hook(hook: impl FnOnce(&Db) -> Result<()> + 'static) {
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

pub fn scan_once(db: &Db, roots: &IngestRoots) -> Result<ScanReport> {
    let _scan_guard = DatabaseLock::acquire(db, "ingest")?;
    Ok(scan_once_locked(db, roots, IngestPacing::Unpaced)?.report)
}

/// Exclusive ownership for one projection writer configuration.
///
/// A one-shot command retains it across recovery, pricing synchronization, and
/// projection. Every server retains it from recovery through shutdown because
/// even `--no-ingest` servers hydrate and mutate pricing state. A scanning
/// server transfers it into the background scanner. These paths therefore
/// discover a competing projection owner before mutating shared state.
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
    /// Database initialization mutates the projection, and startup composition
    /// hydrates manual pricing immediately afterward. Every write-capable
    /// command must establish its exclusive identity from the canonical
    /// storage path before either step.
    pub fn acquire_path(database_path: impl AsRef<Path>) -> Result<Self> {
        let database_path = canonicalize_storage_path(database_path.as_ref())?;
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

    pub(super) fn require_database(&self, db: &Db) -> Result<()> {
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
    scan_one_shot_with_lease_and_between_pass(db, roots, lease, || {}, IngestPacing::Unpaced)
}

/// Run the complete scan sequence with cooperative pacing for a live server.
///
/// The CLI and synchronous startup replay intentionally use the unpaced public
/// entrypoint above. Only the background scanner selects this mode while HTTP
/// requests are concurrently using the same machine and WAL projection.
pub(super) fn scan_background_with_lease(
    db: &Db,
    roots: &IngestRoots,
    lease: &IngestScannerLease,
) -> Result<ScanReport> {
    scan_one_shot_with_lease_and_between_pass(db, roots, lease, || {}, IngestPacing::Background)
}

#[cfg(test)]
pub(super) fn scan_one_shot_with_between_pass<F>(
    db: &Db,
    roots: &IngestRoots,
    between_passes: F,
) -> Result<ScanReport>
where
    F: FnOnce(),
{
    let lease = IngestScannerLease::acquire(db)?;
    scan_one_shot_with_lease_and_between_pass(
        db,
        roots,
        &lease,
        between_passes,
        IngestPacing::Unpaced,
    )
}

fn scan_one_shot_with_lease_and_between_pass<F>(
    db: &Db,
    roots: &IngestRoots,
    lease: &IngestScannerLease,
    between_passes: F,
    pacing: IngestPacing,
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
    let first = scan_once_locked(db, roots, pacing)?;
    let mut report = first.report;
    if first.root_signature_adopted {
        between_passes();
        match scan_once_locked(db, roots, pacing) {
            Ok(confirmation) => report.merge_scan(confirmation.report),
            Err(error) => {
                finalize_scan_sequence_error(db, &report, &error);
                return Err(error);
            }
        }
    }
    if let Err(error) = AttemptRecorder::new(db).publish_projector_generation() {
        finalize_scan_sequence_error(db, &report, &error);
        return Err(error);
    }
    Ok(report)
}

fn scan_once_locked(db: &Db, roots: &IngestRoots, pacing: IngestPacing) -> Result<ScanOutcome> {
    // The caller owns the process lock across this complete application-level
    // reconciliation decision even though source files commit independently.
    AttemptRecorder::new(db).begin()?;
    let attempt = scan_once_started(db, roots, pacing);
    if let Err(error) = &attempt {
        finalize_unexpected_scan_error(db, error);
    }
    attempt
}

fn scan_once_started(db: &Db, roots: &IngestRoots, pacing: IngestPacing) -> Result<ScanOutcome> {
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
    let mut file_ingestor = FileIngestor::new(db).with_pacing(pacing);
    let mut protected_handoff_owners = HashSet::new();
    let mut candidates_by_owner: HashMap<String, Vec<SourceCandidate>> = HashMap::new();
    let mut owners = HashMap::new();
    for (path, archived) in files {
        let storage_size = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        match read_owner(&path) {
            Ok(owner) => {
                // A zstd path's physical size is not its JSONL byte extent.
                // Reuse the last logical extent only as a catalog preference
                // hint; projection derives and verifies the exact size from
                // the captured decompressed snapshot.
                let size = if is_compressed_rollout_path(&path) {
                    selected_source_extents
                        .get(&owner.owner_id)
                        .map_or(storage_size, |extent| extent.raw_size)
                } else {
                    storage_size
                };
                let complete = source_is_complete(&path, storage_size);
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
                if !source_is_complete(&path, storage_size) {
                    tracing::debug!(
                        path = %path.display(),
                        candidate_size = storage_size,
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
    let pending_owners = owners_with_pending_empty_sources(
        &pending_empty,
        &selected_source_extents,
        &source_handoffs,
    );
    let mut prepared_candidates = Vec::new();
    for (owner_id, candidates) in candidates_by_owner {
        let defer_selection = pending_owners.defer_selection.contains(&owner_id);
        let selected_extent = selected_source_extents.get(&owner_id);
        for candidate in candidates {
            let ready = defer_selection
                || selected_extent.is_none_or(|extent| {
                    candidate.path == extent.path
                        || file_ingestor.source_path_switch_is_ready(&candidate, extent)
                });
            if !ready {
                tracing::debug!(
                    owner_id,
                    path = %candidate.path.display(),
                    candidate_size = candidate.size,
                    previous_committed_size = selected_extent
                        .map_or(0, |extent| extent.committed_size),
                    "deferring source handoff until the prior byte extent is continuous"
                );
            }
            prepared_candidates.push(PreparedSourceCandidate { candidate, ready });
        }
    }
    let CatalogSelectionPlan {
        selected,
        protect_reconciliation: mut protected_reconciliation,
    } = plan_catalog_selection(
        prepared_candidates,
        pending_owners,
        protected_handoff_owners,
    );
    for candidate in &selected {
        owners.insert(candidate.owner.owner_id.clone(), candidate.owner.clone());
    }
    let existing_owner_threads = load_existing_owner_threads(db)?;
    resolve_owner_topology(&mut owners, &existing_owner_threads);
    for candidate in selected {
        let owner_id = candidate.owner.owner_id.clone();
        let Some(owner) = owners.get(&owner_id) else {
            report.files_failed += 1;
            tracing::warn!(path = %candidate.path.display(), "failed to resolve rollout owner");
            continue;
        };
        match file_ingestor.process(
            &candidate.path,
            candidate.archived,
            owner,
            selected_source_extents.get(&owner_id),
        ) {
            Ok(file_report) => {
                if file_report.deferred {
                    protected_reconciliation.insert(owner_id.clone());
                }
                if let Some(error) = &file_report.error {
                    failures.push(format!("{}: {error}", candidate.path.display()));
                }
                report.merge(file_report);
            }
            Err(error) => {
                if selected_source_extents.contains_key(&owner_id) {
                    protected_reconciliation.insert(owner_id);
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
    let attempt = AttemptRecorder::new(db);
    let previous_signature = attempt.root_signature()?;
    let mut root_signature_adopted = false;
    if previous_signature.as_deref() == Some(root_signature.as_str()) {
        // Reconciliation depends on enumeration completeness, not projection
        // success. One malformed file must not keep deleted rollouts alive in
        // another root that was enumerated successfully, while any root whose
        // traversal failed remains untouched.
        reconcile_missing(
            db,
            &observed,
            &protected_reconciliation,
            &enumerated_roots,
            &incomplete_roots,
        )?;
    } else if report.files_failed == 0 {
        // A root change may intentionally expose a different source set.
        // Adopt it after one clean scan, then reconcile only if the next
        // clean scan confirms the same configuration.
        attempt.adopt_root_signature(&root_signature)?;
        root_signature_adopted = true;
    }
    sync_session_index_titles(db, roots.active.as_deref(), roots.archive.as_deref())?;
    let now = canonical_utc_timestamp(Utc::now());
    let report_json = serde_json::to_string(&report)?;
    if report.files_failed == 0 {
        attempt.finish(&now, &report_json, None)?;
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
        attempt.finish(&now, &report_json, Some(&detail))?;
        Err(anyhow!("ingest scan failed: {detail}"))
    }
}

fn finalize_unexpected_scan_error(db: &Db, original_error: &anyhow::Error) {
    let attempt = AttemptRecorder::new(db);
    let still_scanning = match attempt.state() {
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
    let now = canonical_utc_timestamp(Utc::now());
    let report_json =
        serde_json::to_string(&ScanReport::default()).unwrap_or_else(|_| "{}".to_owned());
    if let Err(finalizer_error) = attempt.finish(&now, &report_json, Some(detail.as_str())) {
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
    let attempt = AttemptRecorder::new(db);
    let already_finalized = match attempt.state() {
        Ok(state) => state.as_deref() == Some("error"),
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
    let now = canonical_utc_timestamp(Utc::now());
    let report_json = serde_json::to_string(completed_report).unwrap_or_else(|_| "{}".to_owned());
    if let Err(finalizer_error) = attempt.finish(&now, &report_json, Some(detail.as_str())) {
        tracing::warn!(
            error = %finalizer_error,
            original_error = %original_error,
            "failed to finalize a one-shot confirmation error"
        );
    }
}

/// Convert a transient state left by a terminated process into a durable,
/// truthful failure before this process decides whether to run ingestion.
/// Taking the same process lock as `scan_once` prevents us from recovering a
/// scan that is still active in another process.
pub fn recover_interrupted_scan(db: &Db) -> Result<bool> {
    let _scan_guard = DatabaseLock::acquire(db, "ingest")?;
    AttemptRecorder::new(db).recover_interrupted_state()
}
