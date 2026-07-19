use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use chrono::Utc;
use codex_usage::{
    api::{ApiState, router},
    config::PricingConfig,
    db::Db,
    ingest::{IngestRoots, scan_once},
};
use serde_json::{Value, json};
use std::{
    fs::{self, File, FileTimes},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;
use tower::ServiceExt;

const OWNER: &str = "019f64aa-0000-7000-8000-000000000000";
const TURN: &str = "019f64ab-0000-7000-8000-000000000000";

struct Harness {
    _temp: TempDir,
    db: Db,
    active: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let active = temp.path().join("sessions");
        fs::create_dir(&active).unwrap();
        let db = Db::open(temp.path().join("data/codex-usage.db")).unwrap();
        Self {
            _temp: temp,
            db,
            active,
        }
    }

    fn roots(&self) -> IngestRoots {
        IngestRoots {
            active: Some(self.active.clone()),
            archive: None,
        }
    }
}

fn write_jsonl(path: &Path, records: &[Value]) {
    let mut file = File::create(path).unwrap();
    for record in records {
        writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
    }
}

fn meta(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": OWNER,
            "session_id": OWNER,
            "cwd": "/tmp/ingest-truthfulness",
            "source": "cli"
        }
    })
}

fn context(timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "turn_context",
        "payload": {"turn_id": TURN, "model": "gpt-5.5", "effort": "high"}
    })
}

fn message(timestamp: &str, id: &str, text: String) -> Value {
    json!({
        "timestamp": timestamp,
        "type": "response_item",
        "payload": {
            "type": "message",
            "id": id,
            "role": "user",
            "content": [{"type": "input_text", "text": text}]
        }
    })
}

fn api(db: Db, roots: IngestRoots, frontend: PathBuf) -> Router {
    router(ApiState::new(
        db,
        roots,
        frontend,
        PricingConfig {
            url: "http://127.0.0.1:9/prices.json".into(),
            refresh_interval_hours: 24,
            timeout_seconds: 1,
        },
    ))
}

async fn status_json(app: &Router) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .header("host", "127.0.0.1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

#[test]
fn source_timestamps_are_fixed_width_utc_and_sort_chronologically() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("offsets.jsonl"),
        &[
            meta("2026-01-01T00:00:00+02:00"),
            context("2025-12-31T22:30:00Z"),
            message(
                "2026-01-01T00:45:00+02:00",
                "earlier-message",
                "Earlier instant".into(),
            ),
            message(
                "2025-12-31T23:00:00Z",
                "later-message",
                "Later instant".into(),
            ),
        ],
    );

    scan_once(&harness.db, &harness.roots()).unwrap();
    let connection = harness.db.connect().unwrap();
    let thread_times: (String, String) = connection
        .query_row(
            "SELECT started_at,last_event_at FROM threads WHERE id=?1",
            [OWNER],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        thread_times,
        (
            "2025-12-31T22:00:00.000000000Z".into(),
            "2025-12-31T23:00:00.000000000Z".into(),
        )
    );
    let ordered = connection
        .prepare("SELECT id,timestamp FROM messages ORDER BY timestamp")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        ordered,
        vec![
            (
                "earlier-message".into(),
                "2025-12-31T22:45:00.000000000Z".into(),
            ),
            (
                "later-message".into(),
                "2025-12-31T23:00:00.000000000Z".into(),
            ),
        ]
    );
}

#[test]
fn malformed_source_timestamp_fails_the_attempt_and_rolls_back_the_file() {
    let harness = Harness::new();
    write_jsonl(
        &harness.active.join("malformed-time.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z"),
            context("2026-07-15T09:00:01Z"),
            message("not-rfc3339", "bad-time", "Must not persist".into()),
        ],
    );

    let error = scan_once(&harness.db, &harness.roots()).unwrap_err();
    assert!(error.to_string().contains("ingest scan failed"));
    let connection = harness.db.connect().unwrap();
    let projected: i64 = connection
        .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(projected, 0);
    let state: String = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='ingest_state'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "error");
    let successful: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM app_meta WHERE key='last_ingest_at'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(successful, 0);
}

#[tokio::test]
async fn unavailable_root_is_a_failed_attempt_without_overwriting_last_success() {
    let harness = Harness::new();
    let first = scan_once(&harness.db, &harness.roots()).unwrap();
    assert_eq!(first.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    let last_success: String = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='last_ingest_at'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);

    let missing_roots = IngestRoots {
        active: Some(harness.active.join("missing")),
        archive: None,
    };
    let error = scan_once(&harness.db, &missing_roots).unwrap_err();
    assert!(error.to_string().contains("configured ingest root"));

    let frontend = harness.active.join("frontend-missing");
    let app = api(harness.db.clone(), missing_roots, frontend);
    let status = status_json(&app).await;
    assert_eq!(status["state"], "error");
    assert_eq!(status["lastIngestAt"], last_success);
    assert!(status["lastIngestAttemptAt"].is_string());
    assert_eq!(status["filesScanned"], 0);
    assert_eq!(status["filesFailed"], 1);
}

#[test]
fn one_shot_ingest_exits_nonzero_when_a_configured_root_is_unavailable() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("usage.db");
    let archive = temp.path().join("archive");
    fs::create_dir(&archive).unwrap();
    let pricing_url = "http://127.0.0.1:9/prices.json";
    let db = Db::open(&db_path).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_source_url',?1)",
            [pricing_url],
        )
        .unwrap();
    connection
        .execute(
            "INSERT OR REPLACE INTO app_meta(key,value) VALUES('pricing_last_refresh_at',?1)",
            [Utc::now().to_rfc3339()],
        )
        .unwrap();
    drop(connection);

    let output = Command::new(env!("CARGO_BIN_EXE_codex-usage"))
        .arg("ingest")
        .arg("--db")
        .arg(&db_path)
        .arg("--sessions")
        .arg(temp.path().join("missing"))
        .arg("--archive")
        .arg(&archive)
        .arg("--pricing-url")
        .arg(pricing_url)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("configured ingest root"),
        "stderr was {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn preserved_mtime_same_size_rewrite_is_reprojected() {
    let harness = Harness::new();
    let path = harness.active.join("preserved-mtime.jsonl");
    let original = "a".repeat(200_000);
    write_jsonl(
        &path,
        &[
            meta("2026-07-15T09:00:00Z"),
            context("2026-07-15T09:00:01Z"),
            message("2026-07-15T09:00:02Z", "large-message", original),
        ],
    );
    scan_once(&harness.db, &harness.roots()).unwrap();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();

    let mut rewritten = "a".repeat(200_000).into_bytes();
    rewritten[100_000] = b'b';
    write_jsonl(
        &path,
        &[
            meta("2026-07-15T09:00:00Z"),
            context("2026-07-15T09:00:01Z"),
            message(
                "2026-07-15T09:00:02Z",
                "large-message",
                String::from_utf8(rewritten).unwrap(),
            ),
        ],
    );
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    scan_once(&harness.db, &harness.roots()).unwrap();

    let connection = harness.db.connect().unwrap();
    let content: String = connection
        .query_row(
            "SELECT content FROM messages WHERE id='large-message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(content.as_bytes()[100_000], b'b');
}

#[test]
fn preserved_mtime_middle_rewrite_plus_append_rebuilds_the_prefix() {
    let harness = Harness::new();
    let path = harness.active.join("rewrite-and-append.jsonl");
    let original = "a".repeat(200_000);
    let prefix = [
        meta("2026-07-15T09:00:00Z"),
        context("2026-07-15T09:00:01Z"),
        message("2026-07-15T09:00:02Z", "large-message", original),
    ];
    write_jsonl(&path, &prefix);
    let old_size = fs::metadata(&path).unwrap().len();
    assert!(old_size > 128 * 1024);
    scan_once(&harness.db, &harness.roots()).unwrap();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();

    let mut rewritten = "a".repeat(200_000).into_bytes();
    rewritten[100_000] = b'b';
    write_jsonl(
        &path,
        &[
            meta("2026-07-15T09:00:00Z"),
            context("2026-07-15T09:00:01Z"),
            message(
                "2026-07-15T09:00:02Z",
                "large-message",
                String::from_utf8(rewritten).unwrap(),
            ),
            message(
                "2026-07-15T09:00:03Z",
                "appended-message",
                "Appended".into(),
            ),
        ],
    );
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    scan_once(&harness.db, &harness.roots()).unwrap();

    let connection = harness.db.connect().unwrap();
    let content: String = connection
        .query_row(
            "SELECT content FROM messages WHERE id='large-message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(content.as_bytes()[100_000], b'b');
    let appended: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE id='appended-message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(appended, 1);
}
