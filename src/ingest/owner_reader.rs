use super::{
    protocol::{OwnerMeta, decode_owner_record},
    source::{BoundedLine, MAX_JSONL_LINE_BYTES, SourceSnapshot},
};
use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::path::Path;

pub(super) fn read_owner(path: &Path) -> Result<OwnerMeta> {
    let mut snapshot = SourceSnapshot::open(path)?;
    read_owner_from_snapshot(&mut snapshot, path)
}

pub(super) fn read_owner_from_snapshot(
    snapshot: &mut SourceSnapshot,
    path: &Path,
) -> Result<OwnerMeta> {
    let mut reader = snapshot.jsonl_from(0)?;
    let mut line = Vec::new();
    loop {
        match reader.next_bounded_line(&mut line, MAX_JSONL_LINE_BYTES)? {
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
        if let Some(owner) = decode_owner_record(&value)? {
            return Ok(owner);
        }
    }
    Err(anyhow!("{} has no session_meta record", path.display()))
}

pub(super) fn read_surviving_owners(paths: &[String]) -> Result<Vec<OwnerMeta>> {
    paths
        .iter()
        .map(|path| {
            read_owner(Path::new(path))
                .with_context(|| format!("failed to read surviving source owner {path}"))
        })
        .collect()
}
