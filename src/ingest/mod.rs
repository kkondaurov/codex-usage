mod attempt;
mod catalog;
mod checkpoint_store;
mod checkpoints;
mod coordinator;
mod file_ingestor;
mod owner_reader;
mod projection;
mod protocol;
mod reconciliation;
mod scanner;
mod session_titles;
mod source;

#[cfg(test)]
use crate::MAX_USAGE_TOKENS_PER_FACT;
#[cfg(test)]
use crate::storage::DatabaseLock;
#[cfg(test)]
use crate::storage::Db;
#[cfg(test)]
use anyhow::anyhow;
#[cfg(test)]
use attempt::AttemptRecorder;
#[cfg(test)]
use attempt::PROJECTOR_GENERATION;
pub use attempt::projector_generation_is_current;
#[cfg(test)]
use catalog::source_is_complete;
#[cfg(test)]
use checkpoint_store::pending_source_shrink_key;
#[cfg(test)]
use checkpoints::{
    ChunkedFingerprint, FINGERPRINT_AUDIT_FILES_PER_SCAN, FINGERPRINT_CHUNK_BYTES,
    fingerprint_bytes_read, full_content_fingerprints_from_snapshot, reset_fingerprint_bytes_read,
};
pub use coordinator::{
    IngestRoots, IngestScannerLease, ScanReport, recover_interrupted_scan, scan_once,
    scan_one_shot, scan_one_shot_with_lease,
};
#[cfg(test)]
use coordinator::{scan_one_shot_with_between_pass, set_scan_after_start_hook};
#[cfg(test)]
use file_ingestor::{
    set_process_file_after_snapshot_hook, set_process_file_after_transaction_read_hook,
    set_process_file_before_open_hook,
};
#[cfg(test)]
use owner_reader::read_owner;
#[cfg(test)]
use projection::{
    ProjectionConnection, apply_agent_observation, rematerialize_surviving_observation,
    remove_rollout, upsert_owner,
};
#[cfg(test)]
use protocol::OwnerMeta;
#[cfg(test)]
use protocol::{
    MAX_STORED_DURATION_MS, ObservedAgentActivity, PROJECTED_EVENT_BODY_CHARS,
    PROJECTED_EVENT_LABEL_CHARS, PROJECTED_IDENTIFIER_CHARS, canonical_source_timestamp,
    duration_ms, is_owner_native_turn, is_transport_context_envelope,
    last_token_usage_is_total_only_hint, parse_token_usage, parse_total_token_usage,
    raw_duration_ms,
};
#[cfg(test)]
use reconciliation::reconcile_missing;
pub use scanner::{ScannerHandle, spawn_scanner, spawn_scanner_with_lease};
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use session_titles::{discover_session_index, session_index_candidates};
#[cfg(test)]
use source::{MAX_JSONL_LINE_BYTES, SourceSnapshot};
#[cfg(test)]
use std::fs::File;
#[cfg(test)]
use std::io::{Read, Seek, SeekFrom};
#[cfg(test)]
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
#[cfg(test)]
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[cfg(test)]
const PROJECTED_SESSION_PATH_CHARS: usize = 4 * 1024;

mod tests;
