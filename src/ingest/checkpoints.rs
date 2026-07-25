use super::{
    protocol::CursorState,
    source::{FileIdentity, SourceSnapshot},
};
use anyhow::{Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) const FINGERPRINT_CHUNK_BYTES: u64 = 1024 * 1024;
const FINGERPRINT_AUDIT_BYTES_PER_SCAN: u64 = 8 * FINGERPRINT_CHUNK_BYTES;
pub(super) const FINGERPRINT_AUDIT_FILES_PER_SCAN: usize = 8;
const FINGERPRINT_AUDIT_BYTES_PER_FILE: u64 =
    FINGERPRINT_AUDIT_BYTES_PER_SCAN / FINGERPRINT_AUDIT_FILES_PER_SCAN as u64;
const FINGERPRINT_AUDIT_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
const CHUNKED_FINGERPRINT_PREFIX: &str = "chunked-sha256-v1:";

#[derive(Clone, Debug)]
pub(super) struct SourceCheckpoint {
    pub(super) archived: bool,
    pub(super) size: u64,
    pub(super) modified_ns: u64,
    pub(super) identity: FileIdentity,
    pub(super) fingerprint: String,
    pub(super) offset: u64,
    pub(super) line_number: u64,
    pub(super) inherited_lines: u64,
    pub(super) last_error: Option<String>,
    pub(super) state: CursorState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct PendingSourceShrink {
    path: String,
    size: u64,
    content_digest: String,
}

impl PendingSourceShrink {
    pub(super) fn new(path: &str, size: u64, fingerprint: &str) -> Self {
        Self {
            path: path.to_owned(),
            size,
            content_digest: source_content_digest(fingerprint),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct ChunkedFingerprint {
    pub(super) size: u64,
    pub(super) chunk_bytes: u64,
    pub(super) chunks: Vec<String>,
    pub(super) audit_cursor: usize,
    pub(super) audit_completed_at: i64,
}

impl ChunkedFingerprint {
    pub(super) fn parse(value: &str) -> Option<Self> {
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

    pub(super) fn encode(&self) -> Result<String> {
        Ok(format!(
            "{CHUNKED_FINGERPRINT_PREFIX}{}",
            serde_json::to_string(self)?
        ))
    }

    pub(super) fn same_content(&self, other: &Self) -> bool {
        self.size == other.size
            && self.chunk_bytes == other.chunk_bytes
            && self.chunks == other.chunks
    }

    pub(super) fn audit_due(&self, now: i64) -> bool {
        now.saturating_sub(self.audit_completed_at) >= FINGERPRINT_AUDIT_INTERVAL_SECONDS
    }
}

#[derive(Debug)]
pub(super) struct FingerprintAuditBudget {
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

pub(super) struct FullFingerprint {
    pub(super) current: ChunkedFingerprint,
    pub(super) prefix: Option<ChunkedFingerprint>,
    pub(super) legacy_current: String,
    pub(super) legacy_prefix: Option<String>,
}

pub(super) enum FingerprintAudit {
    Verified { changed: bool },
    Mismatch,
}

pub(super) fn matches_unchanged_snapshot(
    checkpoint: &SourceCheckpoint,
    projector_generation: u64,
    owner_id: &str,
    thread_id: &str,
    size: u64,
    modified_ns: u64,
    identity: FileIdentity,
) -> bool {
    checkpoint.state.projector_generation == projector_generation
        && checkpoint.state.owner_id == owner_id
        && checkpoint.size == size
        && checkpoint.modified_ns == modified_ns
        && identity.is_complete()
        && checkpoint.identity == identity
        && checkpoint.state.thread_id == thread_id
}

pub(super) fn is_append_candidate(
    checkpoint: &SourceCheckpoint,
    projector_generation: u64,
    size: u64,
    thread_id: &str,
) -> bool {
    checkpoint.state.projector_generation == projector_generation
        && size > checkpoint.size
        && checkpoint.offset <= checkpoint.size
        && checkpoint.state.thread_id == thread_id
}

pub(super) fn trusts_incremental_append(
    checkpoint: &SourceCheckpoint,
    fingerprint: &ChunkedFingerprint,
    audit_mismatch: bool,
    identity: FileIdentity,
    modified_ns: u64,
) -> bool {
    !audit_mismatch
        && fingerprint.size == checkpoint.size
        && checkpoint.identity.same_file(identity)
        && modified_ns > checkpoint.modified_ns
}

pub(super) fn is_suspicious_same_path_shrink(
    checkpoint: Option<&SourceCheckpoint>,
    owner_id: &str,
    size: u64,
) -> bool {
    checkpoint
        .is_some_and(|checkpoint| checkpoint.state.owner_id == owner_id && size < checkpoint.offset)
}

pub(super) fn source_content_digest(fingerprint: &str) -> String {
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

pub(super) fn full_content_fingerprints_from_snapshot(
    snapshot: &mut SourceSnapshot,
    size: u64,
    prefix_size: Option<u64>,
) -> Result<FullFingerprint> {
    if prefix_size.is_some_and(|prefix| prefix > size) {
        return Err(anyhow!("fingerprint prefix exceeds file size"));
    }
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
    let mut read_offset = 0_u64;
    let mut prefix_remaining = prefix_size.unwrap_or_default();
    let mut buffer = vec![0_u8; FINGERPRINT_CHUNK_BYTES as usize];
    while remaining > 0 {
        let chunk = remaining.min(FINGERPRINT_CHUNK_BYTES) as usize;
        snapshot.read_exact_at(read_offset, &mut buffer[..chunk])?;
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
        read_offset += chunk as u64;
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

pub(super) fn extend_chunked_fingerprint_from_snapshot(
    snapshot: &mut SourceSnapshot,
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
    let mut remaining = size - start;
    let mut read_offset = start;
    let mut buffer = vec![0_u8; FINGERPRINT_CHUNK_BYTES as usize];
    let mut first_chunk = true;
    let mut verified_tail = previous_tail_bytes == 0;
    while remaining > 0 {
        let chunk = remaining.min(FINGERPRINT_CHUNK_BYTES) as usize;
        snapshot.read_exact_at(read_offset, &mut buffer[..chunk])?;
        record_fingerprint_bytes_read(chunk as u64);
        if first_chunk && previous_tail_bytes > 0 {
            verified_tail = hash_fingerprint_chunk(&buffer[..previous_tail_bytes as usize])
                == previous.chunks[retained_chunks];
        }
        chunks.push(hash_fingerprint_chunk(&buffer[..chunk]));
        remaining -= chunk as u64;
        read_offset += chunk as u64;
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

pub(super) fn audit_chunked_fingerprint_from_snapshot(
    snapshot: &mut SourceSnapshot,
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
        snapshot.read_exact_at(offset, &mut buffer[..chunk as usize])?;
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

pub(super) fn audit_growing_chunked_fingerprint_from_snapshot(
    snapshot: &mut SourceSnapshot,
    fingerprint: &mut ChunkedFingerprint,
) -> Result<FingerprintAudit> {
    let mut budget = FingerprintAuditBudget {
        bytes_remaining: FINGERPRINT_AUDIT_BYTES_PER_FILE,
        files_remaining: 1,
    };
    audit_chunked_fingerprint_from_snapshot(snapshot, fingerprint, &mut budget)
}

pub(super) fn fingerprint_for_prefix_from_snapshot(
    snapshot: &mut SourceSnapshot,
    current: &str,
    prefix_size: u64,
) -> Result<String> {
    let Some(mut fingerprint) = ChunkedFingerprint::parse(current) else {
        return full_content_fingerprints_from_snapshot(snapshot, prefix_size, None)?
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
        let mut tail = vec![0_u8; tail_size as usize];
        snapshot.read_exact_at(tail_offset, &mut tail)?;
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

pub(super) fn stored_fingerprint_matches(
    stored: &str,
    chunked: &ChunkedFingerprint,
    legacy: &str,
) -> bool {
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

#[cfg(test)]
thread_local! {
    static FINGERPRINT_BYTES_READ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_fingerprint_bytes_read() {
    FINGERPRINT_BYTES_READ.with(|bytes| bytes.set(0));
}

#[cfg(test)]
pub(super) fn fingerprint_bytes_read() -> u64 {
    FINGERPRINT_BYTES_READ.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::File,
        io::{Seek, SeekFrom, Write},
    };

    fn identity(ctime_ns: i64, inode: i64) -> FileIdentity {
        FileIdentity {
            ctime_ns: Some(ctime_ns),
            device_id: Some(20),
            inode: Some(inode),
        }
    }

    fn checkpoint() -> SourceCheckpoint {
        SourceCheckpoint {
            archived: false,
            size: 100,
            modified_ns: 10,
            identity: identity(1, 30),
            fingerprint: String::new(),
            offset: 100,
            line_number: 0,
            inherited_lines: 0,
            last_error: None,
            state: CursorState {
                projector_generation: 1,
                owner_id: "owner".into(),
                thread_id: "thread".into(),
                ..CursorState::default()
            },
        }
    }

    fn one_chunk_fingerprint(audit_cursor: usize, audit_completed_at: i64) -> ChunkedFingerprint {
        ChunkedFingerprint {
            size: 1,
            chunk_bytes: FINGERPRINT_CHUNK_BYTES,
            chunks: vec!["0".repeat(64)],
            audit_cursor,
            audit_completed_at,
        }
    }

    #[test]
    fn chunked_fingerprint_parse_and_due_boundaries_are_exact() {
        let fingerprint = one_chunk_fingerprint(1, 100);
        let encoded = fingerprint.encode().unwrap();
        assert!(ChunkedFingerprint::parse(&encoded).is_some());

        let invalid_cursor = one_chunk_fingerprint(2, 100).encode().unwrap();
        assert!(ChunkedFingerprint::parse(&invalid_cursor).is_none());
        assert!(!fingerprint.audit_due(100 + FINGERPRINT_AUDIT_INTERVAL_SECONDS - 1));
        assert!(fingerprint.audit_due(100 + FINGERPRINT_AUDIT_INTERVAL_SECONDS));
        assert!(!fingerprint.audit_due(99));
    }

    #[test]
    fn full_fingerprint_reads_source_once_and_shares_audit_epoch() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("full.bin");
        let size = FINGERPRINT_CHUNK_BYTES + 17;
        let prefix_size = FINGERPRINT_CHUNK_BYTES + 7;
        std::fs::write(&path, vec![b'a'; size as usize]).unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();

        reset_fingerprint_bytes_read();
        let full = full_content_fingerprints_from_snapshot(&mut snapshot, size, Some(prefix_size))
            .unwrap();
        assert_eq!(fingerprint_bytes_read(), size);
        assert_eq!(full.current.audit_cursor, 0);
        assert_eq!(full.prefix.as_ref().unwrap().audit_cursor, 0);
        assert_eq!(
            full.current.audit_completed_at,
            full.prefix.as_ref().unwrap().audit_completed_at
        );
    }

    #[test]
    fn prefix_fingerprint_repairs_partial_tail_and_preserves_audit_time() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("prefix.bin");
        let size = FINGERPRINT_CHUNK_BYTES + 20;
        let prefix_size = FINGERPRINT_CHUNK_BYTES + 7;
        std::fs::write(&path, vec![b'a'; size as usize]).unwrap();
        let mut initial = SourceSnapshot::open(&path).unwrap();
        let mut fingerprint = full_content_fingerprints_from_snapshot(&mut initial, size, None)
            .unwrap()
            .current;
        fingerprint.audit_cursor = 2;
        fingerprint.audit_completed_at = 1234;
        let encoded = fingerprint.encode().unwrap();

        reset_fingerprint_bytes_read();
        let mut prefix_snapshot = SourceSnapshot::open(&path).unwrap();
        let prefix =
            fingerprint_for_prefix_from_snapshot(&mut prefix_snapshot, &encoded, prefix_size)
                .unwrap();
        assert_eq!(fingerprint_bytes_read(), 7);
        let parsed = ChunkedFingerprint::parse(&prefix).unwrap();
        assert_eq!(parsed.audit_cursor, 0);
        assert_eq!(parsed.audit_completed_at, 1234);

        let mut clean_snapshot = SourceSnapshot::open(&path).unwrap();
        let clean = full_content_fingerprints_from_snapshot(&mut clean_snapshot, prefix_size, None)
            .unwrap()
            .current;
        assert!(parsed.same_content(&clean));

        reset_fingerprint_bytes_read();
        let mut boundary_snapshot = SourceSnapshot::open(&path).unwrap();
        fingerprint_for_prefix_from_snapshot(
            &mut boundary_snapshot,
            &encoded,
            FINGERPRINT_CHUNK_BYTES,
        )
        .unwrap();
        assert_eq!(fingerprint_bytes_read(), 0);
    }

    #[test]
    fn source_checkpoint_decision_matrix_is_exact() {
        let base = checkpoint();
        assert!(matches_unchanged_snapshot(
            &base,
            1,
            "owner",
            "thread",
            100,
            10,
            identity(1, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            2,
            "owner",
            "thread",
            100,
            10,
            identity(1, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            1,
            "other",
            "thread",
            100,
            10,
            identity(1, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            1,
            "owner",
            "other",
            100,
            10,
            identity(1, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            1,
            "owner",
            "thread",
            101,
            10,
            identity(1, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            1,
            "owner",
            "thread",
            100,
            11,
            identity(1, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            1,
            "owner",
            "thread",
            100,
            10,
            identity(2, 30),
        ));
        assert!(!matches_unchanged_snapshot(
            &base,
            1,
            "owner",
            "thread",
            100,
            10,
            FileIdentity {
                ctime_ns: None,
                ..identity(1, 30)
            },
        ));

        assert!(is_append_candidate(&base, 1, 101, "thread"));
        assert!(!is_append_candidate(&base, 1, 100, "thread"));
        assert!(!is_append_candidate(&base, 2, 101, "thread"));
        assert!(!is_append_candidate(&base, 1, 101, "other"));
        let mut invalid_offset = checkpoint();
        invalid_offset.offset = 101;
        assert!(!is_append_candidate(&invalid_offset, 1, 102, "thread"));

        let fingerprint = ChunkedFingerprint {
            size: 100,
            chunk_bytes: FINGERPRINT_CHUNK_BYTES,
            chunks: vec!["0".repeat(64)],
            audit_cursor: 0,
            audit_completed_at: 0,
        };
        assert!(trusts_incremental_append(
            &base,
            &fingerprint,
            false,
            identity(2, 30),
            11,
        ));
        assert!(!trusts_incremental_append(
            &base,
            &fingerprint,
            true,
            identity(2, 30),
            11,
        ));
        let mut wrong_extent = fingerprint.clone();
        wrong_extent.size = 99;
        assert!(!trusts_incremental_append(
            &base,
            &wrong_extent,
            false,
            identity(2, 30),
            11,
        ));
        assert!(!trusts_incremental_append(
            &base,
            &fingerprint,
            false,
            identity(2, 31),
            11,
        ));
        assert!(!trusts_incremental_append(
            &base,
            &fingerprint,
            false,
            identity(2, 30),
            10,
        ));

        assert!(is_suspicious_same_path_shrink(Some(&base), "owner", 99));
        assert!(!is_suspicious_same_path_shrink(Some(&base), "owner", 100));
        assert!(!is_suspicious_same_path_shrink(Some(&base), "other", 99));
        let mut unfinished = checkpoint();
        unfinished.size = 200;
        unfinished.offset = 100;
        assert!(!is_suspicious_same_path_shrink(
            Some(&unfinished),
            "owner",
            150,
        ));
        assert!(!is_suspicious_same_path_shrink(None, "owner", 0));
    }

    #[test]
    fn pending_shrink_identity_ignores_audit_progress_only() {
        let mut fingerprint = one_chunk_fingerprint(0, 100);
        let first = PendingSourceShrink::new("source.jsonl", 1, &fingerprint.encode().unwrap());
        fingerprint.audit_cursor = 1;
        fingerprint.audit_completed_at = 200;
        let audit_only =
            PendingSourceShrink::new("source.jsonl", 1, &fingerprint.encode().unwrap());
        assert_eq!(first, audit_only);
        assert_ne!(
            first,
            PendingSourceShrink::new("other.jsonl", 1, &fingerprint.encode().unwrap())
        );
        assert_ne!(
            first,
            PendingSourceShrink::new("source.jsonl", 2, &fingerprint.encode().unwrap())
        );
        assert_ne!(
            first,
            PendingSourceShrink::new("source.jsonl", 1, "legacy-other")
        );
    }

    #[test]
    fn chunked_fingerprint_wire_format_and_content_digest_are_stable() {
        assert_eq!(FINGERPRINT_CHUNK_BYTES, 1_048_576);
        assert_eq!(CHUNKED_FINGERPRINT_PREFIX, "chunked-sha256-v1:");

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fingerprint.bin");
        let mut bytes = vec![b'a'; FINGERPRINT_CHUNK_BYTES as usize];
        bytes.extend_from_slice(b"xyz");
        std::fs::write(&path, &bytes).unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        let full = full_content_fingerprints_from_snapshot(
            &mut snapshot,
            bytes.len() as u64,
            Some(bytes.len() as u64 - 1),
        )
        .unwrap();

        let first = "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360";
        let current_tail = "3608bca1e44ea6c4d268eb6db02260269892c0b42b86bbf1e77a6fa16c3c9282";
        let prefix_tail = "769a4e6d0003189c7e96c5d9b7e810a0d11c3a12832527ec94b0f86d277f51ca";
        assert_eq!(full.current.size, 1_048_579);
        assert_eq!(full.current.chunk_bytes, 1_048_576);
        assert_eq!(
            full.current.chunks,
            vec![first.to_owned(), current_tail.to_owned()]
        );
        let prefix = full.prefix.as_ref().unwrap();
        assert_eq!(prefix.size, 1_048_578);
        assert_eq!(
            prefix.chunks,
            vec![first.to_owned(), prefix_tail.to_owned()]
        );
        assert_eq!(
            full.legacy_current,
            "6014272df57d0aef279cd2a2338bde3aafbd6b36f0e323ac9d35dcd6f4846097"
        );
        assert_eq!(
            full.legacy_prefix.as_deref(),
            Some("30fdd95a288d44835fc7776ae5fbed37f8aa66d86c372c7ee15e951842cd57bb")
        );

        let mut fingerprint = full.current.clone();
        fingerprint.audit_cursor = 1;
        fingerprint.audit_completed_at = 1_700_000_000;
        let encoded = fingerprint.encode().unwrap();
        assert_eq!(
            encoded,
            concat!(
                "chunked-sha256-v1:{\"size\":1048579,\"chunk_bytes\":1048576,\"chunks\":[",
                "\"9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360\",",
                "\"3608bca1e44ea6c4d268eb6db02260269892c0b42b86bbf1e77a6fa16c3c9282\"],",
                "\"audit_cursor\":1,\"audit_completed_at\":1700000000}"
            )
        );
        let parsed = ChunkedFingerprint::parse(&encoded).unwrap();
        assert!(parsed.same_content(&fingerprint));
        assert_eq!(parsed.audit_cursor, 1);
        assert_eq!(parsed.audit_completed_at, 1_700_000_000);

        const DIGEST: &str = "0a2d35149c9f52740a9fe239c9a3a32d706c24928566f6842be59dd58cf62276";
        assert_eq!(source_content_digest(&encoded), DIGEST);
        let mut audit_only = fingerprint.clone();
        audit_only.audit_cursor = 0;
        audit_only.audit_completed_at = 1_800_000_000;
        let audit_only_encoded = audit_only.encode().unwrap();
        assert_ne!(audit_only_encoded, encoded);
        assert!(audit_only.same_content(&fingerprint));
        assert_eq!(source_content_digest(&audit_only_encoded), DIGEST);
        assert_eq!(
            source_content_digest("legacy-sha256"),
            "6d1609e7bcff2f1dbbd64baa1fa132a5e781ac9209f475fbbf2e7b4be15e766b"
        );

        assert!(
            ChunkedFingerprint::parse(encoded.strip_prefix(CHUNKED_FINGERPRINT_PREFIX).unwrap())
                .is_none()
        );
        let mut invalid = fingerprint.clone();
        invalid.chunk_bytes = 1;
        assert!(ChunkedFingerprint::parse(&invalid.encode().unwrap()).is_none());
        let mut invalid = fingerprint.clone();
        invalid.chunks.pop();
        assert!(ChunkedFingerprint::parse(&invalid.encode().unwrap()).is_none());
        let mut invalid = fingerprint.clone();
        invalid.chunks[0] = "z".repeat(64);
        assert!(ChunkedFingerprint::parse(&invalid.encode().unwrap()).is_none());
        let mut invalid = fingerprint;
        invalid.audit_cursor = 3;
        assert!(ChunkedFingerprint::parse(&invalid.encode().unwrap()).is_none());
    }

    #[test]
    fn growing_chunk_checkpoint_reads_only_the_tail_and_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("large.jsonl");
        let original_size = FINGERPRINT_CHUNK_BYTES + 17;
        std::fs::write(&path, vec![b'a'; original_size as usize]).unwrap();
        let mut original = SourceSnapshot::open(&path).unwrap();
        let mut previous =
            full_content_fingerprints_from_snapshot(&mut original, original_size, None)
                .unwrap()
                .current;
        previous.audit_cursor = 1;
        previous.audit_completed_at = 1_234;
        let suffix = vec![b'b'; 4096];
        let mut file = File::options().append(true).open(&path).unwrap();
        file.write_all(&suffix).unwrap();
        drop(file);

        reset_fingerprint_bytes_read();
        let final_size = original_size + suffix.len() as u64;
        let mut grown = SourceSnapshot::open(&path).unwrap();
        let (extended, verified_tail) =
            extend_chunked_fingerprint_from_snapshot(&mut grown, final_size, &previous).unwrap();
        assert!(verified_tail);
        assert_eq!(
            fingerprint_bytes_read(),
            17 + suffix.len() as u64,
            "append verification must reread exactly the prior partial tail and suffix"
        );
        assert_eq!(extended.audit_cursor, 1);
        assert_eq!(extended.audit_completed_at, 1_234);
        let mut rebuilt_snapshot = SourceSnapshot::open(&path).unwrap();
        let rebuilt =
            full_content_fingerprints_from_snapshot(&mut rebuilt_snapshot, final_size, None)
                .unwrap()
                .current;
        assert!(extended.same_content(&rebuilt));
    }

    #[test]
    fn aligned_append_reads_only_suffix_and_mutated_tail_is_rejected() {
        let temp = tempfile::tempdir().unwrap();

        let aligned_path = temp.path().join("aligned.bin");
        std::fs::write(&aligned_path, vec![b'a'; FINGERPRINT_CHUNK_BYTES as usize]).unwrap();
        let mut aligned_initial = SourceSnapshot::open(&aligned_path).unwrap();
        let aligned_previous = full_content_fingerprints_from_snapshot(
            &mut aligned_initial,
            FINGERPRINT_CHUNK_BYTES,
            None,
        )
        .unwrap()
        .current;
        let aligned_suffix = b"aligned-suffix";
        File::options()
            .append(true)
            .open(&aligned_path)
            .unwrap()
            .write_all(aligned_suffix)
            .unwrap();

        reset_fingerprint_bytes_read();
        let mut aligned_grown = SourceSnapshot::open(&aligned_path).unwrap();
        let (_, aligned_tail_verified) = extend_chunked_fingerprint_from_snapshot(
            &mut aligned_grown,
            FINGERPRINT_CHUNK_BYTES + aligned_suffix.len() as u64,
            &aligned_previous,
        )
        .unwrap();
        assert!(aligned_tail_verified);
        assert_eq!(fingerprint_bytes_read(), aligned_suffix.len() as u64);

        let partial_path = temp.path().join("mutated-tail.bin");
        let partial_size = FINGERPRINT_CHUNK_BYTES + 17;
        std::fs::write(&partial_path, vec![b'a'; partial_size as usize]).unwrap();
        let mut partial_initial = SourceSnapshot::open(&partial_path).unwrap();
        let partial_previous =
            full_content_fingerprints_from_snapshot(&mut partial_initial, partial_size, None)
                .unwrap()
                .current;
        let mut changed = File::options()
            .read(true)
            .write(true)
            .open(&partial_path)
            .unwrap();
        changed
            .seek(SeekFrom::Start(FINGERPRINT_CHUNK_BYTES + 3))
            .unwrap();
        changed.write_all(b"z").unwrap();
        drop(changed);
        let partial_suffix = b"suffix";
        File::options()
            .append(true)
            .open(&partial_path)
            .unwrap()
            .write_all(partial_suffix)
            .unwrap();

        reset_fingerprint_bytes_read();
        let mut partial_grown = SourceSnapshot::open(&partial_path).unwrap();
        let (_, partial_tail_verified) = extend_chunked_fingerprint_from_snapshot(
            &mut partial_grown,
            partial_size + partial_suffix.len() as u64,
            &partial_previous,
        )
        .unwrap();
        assert!(!partial_tail_verified);
        assert_eq!(fingerprint_bytes_read(), 17 + partial_suffix.len() as u64);
    }

    #[test]
    fn audit_budget_and_state_transitions_are_exact() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("audit-state.bin");
        let size = FINGERPRINT_CHUNK_BYTES + 7;
        std::fs::write(&path, vec![b'a'; size as usize]).unwrap();
        let mut initial = SourceSnapshot::open(&path).unwrap();
        let mut fingerprint = full_content_fingerprints_from_snapshot(&mut initial, size, None)
            .unwrap()
            .current;
        fingerprint.audit_cursor = 0;
        fingerprint.audit_completed_at = 1_234;
        let clean = fingerprint.clone();

        let mut no_files = FingerprintAuditBudget {
            bytes_remaining: FINGERPRINT_AUDIT_BYTES_PER_SCAN,
            files_remaining: 0,
        };
        reset_fingerprint_bytes_read();
        let mut no_files_snapshot = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(
                &mut no_files_snapshot,
                &mut fingerprint,
                &mut no_files,
            )
            .unwrap(),
            FingerprintAudit::Verified { changed: false }
        ));
        assert_eq!(fingerprint_bytes_read(), 0);
        assert_eq!(fingerprint.audit_cursor, 0);
        assert_eq!(fingerprint.audit_completed_at, 1_234);
        assert_eq!(no_files.bytes_remaining, FINGERPRINT_AUDIT_BYTES_PER_SCAN);
        assert_eq!(no_files.files_remaining, 0);

        let mut no_bytes = FingerprintAuditBudget {
            bytes_remaining: 0,
            files_remaining: FINGERPRINT_AUDIT_FILES_PER_SCAN,
        };
        reset_fingerprint_bytes_read();
        let mut no_bytes_snapshot = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(
                &mut no_bytes_snapshot,
                &mut fingerprint,
                &mut no_bytes,
            )
            .unwrap(),
            FingerprintAudit::Verified { changed: false }
        ));
        assert_eq!(fingerprint_bytes_read(), 0);
        assert_eq!(fingerprint.audit_cursor, 0);
        assert_eq!(fingerprint.audit_completed_at, 1_234);
        assert_eq!(no_bytes.bytes_remaining, 0);
        assert_eq!(no_bytes.files_remaining, FINGERPRINT_AUDIT_FILES_PER_SCAN);

        let mut first_budget = FingerprintAuditBudget::default();
        reset_fingerprint_bytes_read();
        let mut first_snapshot = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(
                &mut first_snapshot,
                &mut fingerprint,
                &mut first_budget,
            )
            .unwrap(),
            FingerprintAudit::Verified { changed: true }
        ));
        assert_eq!(fingerprint_bytes_read(), FINGERPRINT_CHUNK_BYTES);
        assert_eq!(fingerprint.audit_cursor, 1);
        assert_eq!(fingerprint.audit_completed_at, 1_234);
        assert_eq!(
            first_budget.bytes_remaining,
            FINGERPRINT_AUDIT_BYTES_PER_SCAN - FINGERPRINT_CHUNK_BYTES
        );
        assert_eq!(
            first_budget.files_remaining,
            FINGERPRINT_AUDIT_FILES_PER_SCAN - 1
        );

        let before_completion = Utc::now().timestamp();
        let mut completion_budget = FingerprintAuditBudget::default();
        reset_fingerprint_bytes_read();
        let mut completion_snapshot = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(
                &mut completion_snapshot,
                &mut fingerprint,
                &mut completion_budget,
            )
            .unwrap(),
            FingerprintAudit::Verified { changed: true }
        ));
        let after_completion = Utc::now().timestamp();
        assert_eq!(fingerprint_bytes_read(), 7);
        assert_eq!(fingerprint.audit_cursor, 0);
        assert!((before_completion..=after_completion).contains(&fingerprint.audit_completed_at));
        assert_eq!(
            completion_budget.bytes_remaining,
            FINGERPRINT_AUDIT_BYTES_PER_SCAN - 7
        );
        assert_eq!(
            completion_budget.files_remaining,
            FINGERPRINT_AUDIT_FILES_PER_SCAN - 1
        );

        let mut changed_file = File::options().write(true).open(&path).unwrap();
        changed_file.seek(SeekFrom::Start(0)).unwrap();
        changed_file.write_all(b"z").unwrap();
        drop(changed_file);
        let mut mismatch = clean;
        let mut mismatch_budget = FingerprintAuditBudget::default();
        reset_fingerprint_bytes_read();
        let mut mismatch_snapshot = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(
                &mut mismatch_snapshot,
                &mut mismatch,
                &mut mismatch_budget,
            )
            .unwrap(),
            FingerprintAudit::Mismatch
        ));
        assert_eq!(fingerprint_bytes_read(), FINGERPRINT_CHUNK_BYTES);
        assert_eq!(mismatch.audit_cursor, 0);
        assert_eq!(mismatch.audit_completed_at, 1_234);
        assert_eq!(
            mismatch_budget.bytes_remaining,
            FINGERPRINT_AUDIT_BYTES_PER_SCAN - FINGERPRINT_CHUNK_BYTES
        );
        assert_eq!(
            mismatch_budget.files_remaining,
            FINGERPRINT_AUDIT_FILES_PER_SCAN - 1
        );
    }

    #[test]
    fn periodic_chunk_audit_is_bounded_and_detects_rewrites() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("audit.jsonl");
        let size = FINGERPRINT_AUDIT_BYTES_PER_SCAN + 2 * FINGERPRINT_CHUNK_BYTES;
        std::fs::write(&path, vec![b'a'; size as usize]).unwrap();
        let mut initial = SourceSnapshot::open(&path).unwrap();
        let mut fingerprint = full_content_fingerprints_from_snapshot(&mut initial, size, None)
            .unwrap()
            .current;
        fingerprint.audit_completed_at = 0;

        reset_fingerprint_bytes_read();
        let mut budget = FingerprintAuditBudget::default();
        let mut unchanged = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(&mut unchanged, &mut fingerprint, &mut budget)
                .unwrap(),
            FingerprintAudit::Verified { changed: true }
        ));
        assert_eq!(fingerprint_bytes_read(), FINGERPRINT_AUDIT_BYTES_PER_FILE);
        assert_eq!(fingerprint.audit_cursor, 1);

        let mut file = File::options().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(FINGERPRINT_CHUNK_BYTES)).unwrap();
        file.write_all(b"corrupt").unwrap();
        drop(file);
        let mut budget = FingerprintAuditBudget::default();
        let mut changed = SourceSnapshot::open(&path).unwrap();
        assert!(matches!(
            audit_chunked_fingerprint_from_snapshot(&mut changed, &mut fingerprint, &mut budget)
                .unwrap(),
            FingerprintAudit::Mismatch
        ));
    }
}
