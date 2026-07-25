use codex_usage::{
    ingest::{IngestRoots, scan_once},
    storage::Db,
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

const OWNER: &str = "019f64aa-0000-7000-8000-000000000000";
const PARENT: &str = "019df47e-0000-7000-8000-000000000000";
const NATIVE_TURN: &str = "019f64ab-0000-7000-8000-000000000000";

fn write_jsonl(path: &Path, records: &[Value]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(path).unwrap();
    for record in records {
        writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
    }
}

fn fork_metadata(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": OWNER,
            "session_id": OWNER,
            "forked_from_id": PARENT,
            "cwd": "/tmp/ingest-protocol-order-contract",
            "source": "vscode"
        }
    })
}

fn cumulative_usage(
    timestamp: &str,
    input: u64,
    cached_input: u64,
    output: u64,
    reasoning: u64,
) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": input,
                    "cached_input_tokens": cached_input,
                    "output_tokens": output,
                    "reasoning_output_tokens": reasoning,
                    "total_tokens": input + output
                }
            }
        }
    })
}

#[test]
fn usage_is_decoded_before_the_fork_native_start_gate() {
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    fs::create_dir_all(&active).unwrap();
    let db = Db::open(temp.path().join("data/codex-usage.db")).unwrap();

    write_jsonl(
        &active.join("fork.jsonl"),
        &[
            fork_metadata("2026-07-25T09:00:00Z"),
            cumulative_usage("2026-07-25T09:00:01Z", 100, 60, 20, 8),
            json!({
                "timestamp": "2026-07-25T09:00:02Z",
                "type": "event_msg",
                "payload": {"type": "task_started", "turn_id": NATIVE_TURN}
            }),
            json!({
                "timestamp": "2026-07-25T09:00:03Z",
                "type": "turn_context",
                "payload": {
                    "turn_id": NATIVE_TURN,
                    "model": "gpt-order-contract",
                    "effort": "high"
                }
            }),
            cumulative_usage("2026-07-25T09:00:04Z", 145, 75, 31, 13),
        ],
    );

    let report = scan_once(
        &db,
        &IngestRoots {
            active: Some(active),
            archive: None,
        },
    )
    .unwrap();

    assert_eq!(report.files_failed, 0);
    assert_eq!(report.files_ingested, 1);
    assert_eq!(report.records_read, 5);
    assert_eq!(report.inherited_records_skipped, 2);

    let connection = db.connect().unwrap();
    let fact_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(fact_count, 1, "pre-native usage must not be projected");

    let fact: (i64, i64, i64, i64, i64, i64, String, String, i64) = connection
        .query_row(
            "SELECT input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,source_line,model,turn_id,native
             FROM usage_facts",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .unwrap();

    assert_eq!(
        fact,
        (
            45,
            15,
            11,
            5,
            56,
            5,
            "gpt-order-contract".into(),
            NATIVE_TURN.into(),
            1,
        ),
        "the native usage fact must be the exact delta from the inherited cumulative snapshot"
    );
}
