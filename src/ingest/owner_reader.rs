use super::{
    protocol::{OwnerMeta, decode_owner_record},
    source::{BoundedLine, MAX_JSONL_LINE_BYTES, SourceSnapshot, is_compressed_rollout_path},
};
use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

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
            let stored_path = Path::new(path);
            let readable_path = surviving_source_path(stored_path);
            read_owner(&readable_path)
                .with_context(|| format!("failed to read surviving source owner {path}"))
        })
        .collect()
}

fn surviving_source_path(path: &Path) -> PathBuf {
    if path.is_file() {
        return path.to_path_buf();
    }
    let alternate = if is_compressed_rollout_path(path) {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".zst"))
            .map(|name| path.with_file_name(name))
    } else if path
        .extension()
        .is_some_and(|extension| extension == "jsonl")
    {
        path.file_name().map(|name| {
            let mut compressed_name = OsString::from(name);
            compressed_name.push(".zst");
            path.with_file_name(compressed_name)
        })
    } else {
        None
    };
    alternate
        .filter(|candidate| candidate.is_file())
        .unwrap_or_else(|| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, io::Write};

    fn owner_line(owner_id: &str) -> Value {
        serde_json::json!({
            "timestamp": "2026-07-29T20:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": owner_id,
                "session_id": owner_id,
                "cwd": "/tmp/project",
                "source": "vscode"
            }
        })
    }

    #[test]
    fn surviving_owner_follows_a_stale_plain_path_to_its_compressed_sibling() {
        let temp = tempfile::tempdir().unwrap();
        let plain = temp.path().join("rollout.jsonl");
        let compressed = temp.path().join("rollout.jsonl.zst");
        let output = File::create(&compressed).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(output, 3).unwrap();
        writeln!(
            encoder,
            "{}",
            owner_line("019f64aa-0000-7000-8000-000000000201")
        )
        .unwrap();
        encoder.finish().unwrap();

        let owners = read_surviving_owners(&[plain.to_string_lossy().into_owned()]).unwrap();
        assert_eq!(owners[0].owner_id, "019f64aa-0000-7000-8000-000000000201");
    }

    #[test]
    fn surviving_owner_keeps_plain_precedence_when_both_forms_exist() {
        let temp = tempfile::tempdir().unwrap();
        let plain = temp.path().join("rollout.jsonl");
        let compressed = temp.path().join("rollout.jsonl.zst");
        writeln!(
            File::create(&plain).unwrap(),
            "{}",
            owner_line("019f64aa-0000-7000-8000-000000000202")
        )
        .unwrap();
        let output = File::create(&compressed).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(output, 3).unwrap();
        writeln!(
            encoder,
            "{}",
            owner_line("019f64aa-0000-7000-8000-000000000203")
        )
        .unwrap();
        encoder.finish().unwrap();

        let owners = read_surviving_owners(&[plain.to_string_lossy().into_owned()]).unwrap();
        assert_eq!(owners[0].owner_id, "019f64aa-0000-7000-8000-000000000202");
    }
}
