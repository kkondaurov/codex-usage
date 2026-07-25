use super::{
    attempt::PROJECTOR_GENERATION,
    catalog::{SelectedSourceExtent, SourceCandidate},
    checkpoint_store::{
        clear_pending_source_shrink, load_checkpoint, load_checkpoint_by_path,
        same_source_shrink_was_observed,
    },
    checkpoints::{
        ChunkedFingerprint, FingerprintAudit, FingerprintAuditBudget, SourceCheckpoint,
        audit_chunked_fingerprint_from_snapshot, audit_growing_chunked_fingerprint_from_snapshot,
        extend_chunked_fingerprint_from_snapshot, fingerprint_for_prefix_from_snapshot,
        full_content_fingerprints_from_snapshot, is_append_candidate,
        is_suspicious_same_path_shrink, matches_unchanged_snapshot, stored_fingerprint_matches,
        trusts_incremental_append,
    },
    owner_reader::{read_available_owners, read_owner_from_snapshot},
    projection::{
        PathConflict, ProjectionConnection, ProjectionTx, RemovalImpact, SourceCheckpointWrite,
        UnchangedSourceUpdate, apply_thread_metadata_reset, clear_confirmed_shrink,
        clear_projected_thread_title, delete_source_checkpoint, delete_thread_if_abandoned,
        find_path_conflict, mark_source_unchanged, rematerialize_after_checkpoint, remove_rollout,
        save_source_checkpoint, upsert_owner,
    },
    protocol::{CursorState, OwnerMeta, decode_record},
    source::{BoundedLine, FileIdentity, MAX_JSONL_LINE_BYTES, SourceSnapshot},
};
use crate::storage::Db;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use rusqlite::Connection;
use serde_json::Value;
use std::path::Path;

#[cfg(test)]
type ProcessFileAfterSnapshotHook = Box<dyn FnOnce(&Path)>;

#[cfg(test)]
type ProcessFileAfterTransactionReadHook = Box<dyn FnOnce()>;

#[cfg(test)]
type ProcessFileBeforeOpenHook = Box<dyn FnOnce(&Path)>;

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
}

#[cfg(test)]
pub(super) fn set_process_file_before_open_hook(hook: impl FnOnce(&Path) + 'static) {
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
pub(super) fn set_process_file_after_snapshot_hook(hook: impl FnOnce(&Path) + 'static) {
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
pub(super) fn set_process_file_after_transaction_read_hook(hook: impl FnOnce() + 'static) {
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

#[derive(Debug, Default)]
pub(super) struct FileReport {
    pub(super) deferred: bool,
    pub(super) unchanged: bool,
    pub(super) failed: bool,
    pub(super) records: u64,
    pub(super) inherited: u64,
    pub(super) error: Option<String>,
}

pub(super) struct FileIngestor<'db> {
    db: &'db Db,
    audit_budget: FingerprintAuditBudget,
}

impl<'db> FileIngestor<'db> {
    pub(super) fn new(db: &'db Db) -> Self {
        Self {
            db,
            audit_budget: FingerprintAuditBudget::default(),
        }
    }
}

impl FileIngestor<'_> {
    pub(super) fn source_path_switch_is_ready(
        &self,
        candidate: &SourceCandidate,
        previous: &SelectedSourceExtent,
    ) -> bool {
        let Ok(mut snapshot) = SourceSnapshot::open(&candidate.path) else {
            return false;
        };
        Self::source_path_switch_is_ready_from_snapshot(&mut snapshot, candidate.size, previous)
    }

    fn source_path_switch_is_ready_from_snapshot(
        snapshot: &mut SourceSnapshot,
        size: u64,
        previous: &SelectedSourceExtent,
    ) -> bool {
        if size < previous.committed_size {
            return false;
        }
        let Ok(candidate_prefix) =
            full_content_fingerprints_from_snapshot(snapshot, previous.committed_size, None)
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
            let Ok(mut selected_snapshot) = SourceSnapshot::open(&previous.path) else {
                return false;
            };
            return full_content_fingerprints_from_snapshot(
                &mut selected_snapshot,
                previous.committed_size,
                None,
            )
            .is_ok_and(|selected_prefix| {
                selected_prefix
                    .current
                    .same_content(&candidate_prefix.current)
            });
        }
        false
    }

    fn reset_thread_metadata_after_removal(
        tx: &ProjectionTx<'_>,
        impact: &RemovalImpact,
    ) -> Result<()> {
        let Some(reset) = impact.metadata_reset.as_ref() else {
            return Ok(());
        };
        let owners = read_available_owners(&reset.ordered_source_paths);
        apply_thread_metadata_reset(tx, reset, &owners)
    }

    pub(super) fn process(
        &mut self,
        path: &Path,
        archived: bool,
        resolved_owner: &OwnerMeta,
        previous_extent: Option<&SelectedSourceExtent>,
    ) -> Result<FileReport> {
        // Open once, then derive every ownership and content decision from this
        // descriptor. A writer may rename a replacement over `path` after this
        // point, but that replacement belongs to the next scan and cannot be
        // projected under the owner discovered from the previous inode.
        #[cfg(test)]
        run_process_file_before_open_hook(path);
        let mut snapshot = SourceSnapshot::open(path)?;
        let extent = snapshot.extent();
        let size = extent.size();
        let modified_ns = extent.modified_ns();
        let identity = extent.identity();
        let mut owner = read_owner_from_snapshot(&mut snapshot, path)?;
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
            && !Self::source_path_switch_is_ready_from_snapshot(&mut snapshot, size, previous)
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
        let mut connection = self.db.connect()?;
        let path_text = path.to_string_lossy();
        let checkpoint_by_path = load_checkpoint_by_path(&connection, &path_text)?;
        let suspicious_same_path_shrink =
            is_suspicious_same_path_shrink(checkpoint_by_path.as_ref(), &owner.owner_id, size);
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
            && matches_unchanged_snapshot(
                checkpoint,
                PROJECTOR_GENERATION,
                &resolved_owner.owner_id,
                &resolved_owner.thread_id,
                size,
                modified_ns,
                identity,
            )
        {
            let fingerprint_extent = ChunkedFingerprint::parse(&checkpoint.fingerprint)
                .map(|fingerprint| fingerprint.size);
            if checkpoint.size != checkpoint.offset && fingerprint_extent != Some(checkpoint.offset)
            {
                // Upgrade raw-extent fingerprints written by older builds while
                // the selected source is still available. This is a one-time read
                // for the rare checkpoint with an unfinished trailing record.
                let fingerprint = fingerprint_for_prefix_from_snapshot(
                    &mut snapshot,
                    &checkpoint.fingerprint,
                    checkpoint.offset,
                )?;
                return Self::mark_file_unchanged(
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
                match audit_chunked_fingerprint_from_snapshot(
                    &mut snapshot,
                    &mut fingerprint,
                    &mut self.audit_budget,
                )? {
                    FingerprintAudit::Verified { changed } => {
                        let fingerprint = changed.then(|| fingerprint.encode()).transpose()?;
                        return Self::mark_file_unchanged(
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
                return Self::mark_file_unchanged(
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
            is_append_candidate(value, PROJECTOR_GENERATION, size, &owner.thread_id)
        });
        let incremental_append = append_checkpoint.and_then(|checkpoint| {
            let fingerprint = ChunkedFingerprint::parse(&checkpoint.fingerprint)?;
            trusts_incremental_append(
                checkpoint,
                &fingerprint,
                audit_mismatch,
                identity,
                modified_ns,
            )
            .then_some((checkpoint, fingerprint))
        });

        let (fingerprint, append) = if let Some((_, mut previous)) = incremental_append {
            // Growth is not evidence that the previous prefix stayed immutable.
            // Advance the same bounded rolling audit used by stable files before
            // extending the checkpoint. The updated cursor is carried into the
            // extended fingerprint, so a continuously growing file cannot evade
            // verification of its older completed chunks forever.
            match audit_growing_chunked_fingerprint_from_snapshot(&mut snapshot, &mut previous)? {
                FingerprintAudit::Mismatch => {
                    let full = full_content_fingerprints_from_snapshot(&mut snapshot, size, None)?;
                    (full.current.encode()?, false)
                }
                FingerprintAudit::Verified { .. } => {
                    let (fingerprint, verified_tail) =
                        extend_chunked_fingerprint_from_snapshot(&mut snapshot, size, &previous)?;
                    if verified_tail {
                        (fingerprint.encode()?, true)
                    } else {
                        let full =
                            full_content_fingerprints_from_snapshot(&mut snapshot, size, None)?;
                        (full.current.encode()?, false)
                    }
                }
            }
        } else {
            let prefix_size = append_checkpoint.map(|checkpoint| checkpoint.size);
            let full = full_content_fingerprints_from_snapshot(&mut snapshot, size, prefix_size)?;

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
                return Self::mark_file_unchanged(
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
        let transaction = ProjectionConnection::new(&mut connection).begin_file_projection()?;
        if let Some(PathConflict {
            rollout_id: replaced_owner,
            root_thread_id: replaced_thread,
        }) = find_path_conflict(&transaction, &path_text, &owner.owner_id)?
        {
            let impact = remove_rollout(&transaction, &replaced_owner)?;
            Self::reset_thread_metadata_after_removal(&transaction, &impact)?;
            delete_source_checkpoint(&transaction, &replaced_owner)?;
            if let Some(replaced_thread) = impact.thread_id.or(replaced_thread) {
                delete_thread_if_abandoned(&transaction, &replaced_thread)?;
            }
        }
        #[cfg(test)]
        run_process_file_after_transaction_read_hook();
        if !append {
            let impact = remove_rollout(&transaction, &owner.owner_id)?;
            Self::reset_thread_metadata_after_removal(&transaction, &impact)?;
            if let Some(previous_thread) = impact.thread_id
                && previous_thread != owner.thread_id
            {
                delete_thread_if_abandoned(&transaction, &previous_thread)?;
            }
            if owner.owner_id == owner.thread_id {
                clear_projected_thread_title(&transaction, &owner.thread_id)?;
            }
        }
        upsert_owner(&transaction, &owner, archived)?;

        size.checked_sub(offset).ok_or_else(|| {
            anyhow!(
                "{} shrank below its committed projection boundary",
                path.display()
            )
        })?;
        // The metadata extent is the scan's immutable read boundary. A writer may
        // append after the stat above, but those bytes belong to the next scan and
        // must not advance this scan's durable checkpoint beyond its fingerprint.
        let mut reader = snapshot.jsonl_from(offset)?;
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
            let (line_len, oversized) = match reader
                .next_bounded_line(&mut bytes, MAX_JSONL_LINE_BYTES)?
            {
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
            let decoded = decode_record(&state, source_line, &value)
                .with_context(|| format!("failed to decode line {source_line}"))?;
            transaction
                .context(&mut state)
                .apply(decoded)
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
            fingerprint_for_prefix_from_snapshot(&mut snapshot, &fingerprint, committed_offset)?
        };
        let state_json = serde_json::to_string(&state)?;
        save_source_checkpoint(
            &transaction,
            &SourceCheckpointWrite {
                rollout_id: state.owner_id.clone(),
                path: path_text.to_string(),
                archived,
                size_bytes: size,
                modified_ns,
                ctime_ns: identity.ctime_ns,
                device_id: identity.device_id,
                inode: identity.inode,
                content_fingerprint: fingerprint,
                byte_offset: committed_offset,
                line_number: source_line,
                root_thread_id: state.thread_id.clone(),
                parent_rollout_id: state.parent_rollout_id.clone(),
                native_started: state.native_started,
                inherited_lines: inherited,
                parse_state_json: state_json,
                error_count: errors,
                last_error: last_error.clone(),
                ingested_at: Utc::now().to_rfc3339(),
            },
        )?;
        if suspicious_same_path_shrink {
            clear_confirmed_shrink(&transaction, &owner.owner_id)?;
        }
        // A parent may have projected a terminal child observation before the
        // child's own file was discovered. Re-apply surviving observations after
        // this rollout's native events so chronological evidence, rather than
        // source discovery order, determines the promoted lifecycle.
        rematerialize_after_checkpoint(&transaction, &state.owner_id)?;
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
            let transaction = ProjectionConnection::new(connection).begin_metadata_refresh()?;
            mark_source_unchanged(
                &transaction,
                &UnchangedSourceUpdate {
                    rollout_id: checkpoint.state.owner_id.clone(),
                    archived,
                    size_bytes: size,
                    modified_ns,
                    ctime_ns: identity.ctime_ns,
                    device_id: identity.device_id,
                    inode: identity.inode,
                    content_fingerprint: fingerprint.map(str::to_owned),
                    rollout_archive_changed: checkpoint.archived != archived,
                },
            )?;
            transaction.commit()?;
        }
        Ok(FileReport {
            unchanged: true,
            failed: checkpoint.last_error.is_some(),
            error: checkpoint.last_error.clone(),
            ..FileReport::default()
        })
    }
}
