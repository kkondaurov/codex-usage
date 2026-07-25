use super::{
    projection::{ProjectionConnection, apply_indexed_thread_title},
    protocol::{PROJECTED_EVENT_BODY_CHARS, normalized_relational_identifier, redact_and_bound},
    source::{BoundedLine, MAX_JSONL_LINE_BYTES, read_bounded_line},
};
use crate::{calendar::canonical_utc_timestamp, storage::Db};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

#[derive(Debug)]
struct IndexedTitle {
    title: String,
    updated_at: String,
    updated_micros: i64,
    line_number: u64,
}

pub(super) fn sync_session_index_titles(
    db: &Db,
    active_root: Option<&Path>,
    archive_root: Option<&Path>,
) -> Result<usize> {
    let Some(index_path) = discover_session_index(active_root, archive_root) else {
        return Ok(0);
    };
    let latest = load_indexed_titles(&index_path, MAX_JSONL_LINE_BYTES);

    let mut connection = db.connect()?;
    let transaction = ProjectionConnection::new(&mut connection).begin_title_import()?;
    let mut updated = 0;
    for (id, indexed) in latest {
        updated +=
            apply_indexed_thread_title(&transaction, &id, &indexed.updated_at, &indexed.title)?;
    }
    transaction.commit()?;
    Ok(updated)
}

fn load_indexed_titles(index_path: &Path, max_line_bytes: usize) -> HashMap<String, IndexedTitle> {
    let file = match File::open(index_path) {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(path=%index_path.display(),%error,"failed to open Codex session title index");
            return HashMap::new();
        }
    };
    let mut reader = BufReader::new(file);
    read_indexed_titles(&mut reader, index_path, max_line_bytes)
}

fn read_indexed_titles<R: BufRead>(
    reader: &mut R,
    index_path: &Path,
    max_line_bytes: usize,
) -> HashMap<String, IndexedTitle> {
    let mut latest = HashMap::<String, IndexedTitle>::new();
    let mut bytes = Vec::new();
    let mut line_number = 0_u64;
    loop {
        let line = match read_bounded_line(reader, &mut bytes, max_line_bytes) {
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
            title: redact_and_bound(title.trim(), PROJECTED_EVENT_BODY_CHARS),
            updated_at: canonical_utc_timestamp(timestamp.with_timezone(&Utc)),
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
    latest
}

pub(super) fn discover_session_index(
    active_root: Option<&Path>,
    archive_root: Option<&Path>,
) -> Option<PathBuf> {
    session_index_candidates(active_root, archive_root)
        .into_iter()
        .map(|path| path.join("session_index.jsonl"))
        .find(|path| path.is_file())
}

pub(super) fn session_index_candidates(
    active_root: Option<&Path>,
    archive_root: Option<&Path>,
) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    [active_root, archive_root]
        .into_iter()
        .flatten()
        .filter_map(|root| root.parent().map(Path::to_path_buf))
        .filter(|path| seen.insert(path.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Error, Read};

    struct AlwaysErrors;

    impl Read for AlwaysErrors {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(Error::other("synthetic session-index read failure"))
        }
    }

    #[test]
    fn reader_skips_oversized_and_malformed_records_and_ignores_incomplete_tail() {
        let thread = "019f7000-0000-7000-8000-000000000004";
        let input = format!(
            "{{\"id\":\"{thread}\",\"thread_name\":\"Old\",\"updated_at\":\"2026-07-16T08:00:00Z\"}}\n\
             {}\n\
             {{not-json\n\
             {{\"id\":\"{thread}\",\"thread_name\":\"Newest complete\",\"updated_at\":\"2026-07-16T08:02:00Z\"}}\n\
             {{\"id\":\"{thread}\",\"thread_name\":\"Incomplete newer\",\"updated_at\":\"2026-07-16T08:03:00Z\"}}",
            "x".repeat(257)
        );
        let mut reader = Cursor::new(input.into_bytes());

        let titles = read_indexed_titles(&mut reader, Path::new("session_index.jsonl"), 256);
        let indexed = titles.get(thread).unwrap();

        assert_eq!(indexed.title, "Newest complete");
        assert_eq!(indexed.updated_at, "2026-07-16T08:02:00.000000000Z");
        assert_eq!(indexed.line_number, 4);
    }

    #[test]
    fn reader_keeps_completed_prefix_after_read_error() {
        let thread = "019f7000-0000-7000-8000-000000000005";
        let prefix = format!(
            "{{\"id\":\"{thread}\",\"thread_name\":\"Completed prefix\",\"updated_at\":\"2026-07-16T08:00:00Z\"}}\n"
        );
        let chained = Cursor::new(prefix.into_bytes()).chain(AlwaysErrors);
        let mut reader = BufReader::new(chained);

        let titles = read_indexed_titles(
            &mut reader,
            Path::new("session_index.jsonl"),
            MAX_JSONL_LINE_BYTES,
        );

        assert_eq!(titles.get(thread).unwrap().title, "Completed prefix");
    }

    #[test]
    fn unopenable_index_is_a_nonfatal_empty_import() {
        let temp = tempfile::tempdir().unwrap();
        let titles = load_indexed_titles(
            &temp.path().join("missing-session-index.jsonl"),
            MAX_JSONL_LINE_BYTES,
        );

        assert!(titles.is_empty());
    }
}
