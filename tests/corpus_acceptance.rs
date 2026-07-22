use codex_usage::{
    db::Db,
    ingest::{IngestRoots, scan_once},
};
use rusqlite::Connection;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::Value;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};
use tempfile::TempDir;

const MANIFEST: &str = include_str!("fixtures/corpus/manifest.json");
const PROJECTION_TABLES: [&str; 9] = [
    "source_files",
    "threads",
    "rollouts",
    "agent_runs",
    "turns",
    "messages",
    "events",
    "tool_calls",
    "usage_facts",
];

#[derive(Debug, PartialEq, Eq)]
struct ProjectionSnapshot {
    table_counts: Vec<(&'static str, i64)>,
    usage_sums: (i64, i64, i64, i64, i64),
}

struct CorpusHarness {
    _temp: TempDir,
    db: Db,
    roots: IngestRoots,
}

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/corpus")
}

fn manifest_cases() -> Vec<Value> {
    let manifest: Value = serde_json::from_str(MANIFEST).expect("corpus manifest is valid JSON");
    manifest["cases"]
        .as_array()
        .expect("manifest cases is an array")
        .clone()
}

fn case_spec(case_id: &str) -> Value {
    manifest_cases()
        .into_iter()
        .find(|case| case["id"] == case_id)
        .unwrap_or_else(|| panic!("manifest case {case_id} exists"))
}

fn collect_jsonl_fixtures(root: &Path, fixtures: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            collect_jsonl_fixtures(&entry.path(), fixtures);
        } else if entry.path().extension().is_some_and(|ext| ext == "jsonl") {
            fixtures.push(entry.path());
        }
    }
}

fn assert_fixture_jsonl_lines_are_valid() -> (usize, usize) {
    let root = corpus_root();
    let mut fixtures = Vec::new();
    collect_jsonl_fixtures(&root, &mut fixtures);
    fixtures.sort();

    let mut records = 0;
    for path in &fixtures {
        let content = fs::read_to_string(path).unwrap();
        for (line_index, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            serde_json::from_str::<Value>(line).unwrap_or_else(|error| {
                panic!(
                    "{}:{} is not valid JSON: {error}",
                    path.strip_prefix(&root).unwrap().display(),
                    line_index + 1
                )
            });
            records += 1;
        }
    }

    (fixtures.len(), records)
}

#[test]
fn corpus_fixtures_do_not_contain_local_home_paths() {
    let root = corpus_root();
    let mut fixtures = Vec::new();
    collect_jsonl_fixtures(&root, &mut fixtures);

    for path in fixtures {
        let content = fs::read_to_string(&path).unwrap();
        assert!(
            !content.contains("/Users/kkonstant/") && !content.contains("/home/kkonstant/"),
            "{} contains an unsanitized local home path",
            path.strip_prefix(&root).unwrap().display()
        );
    }
}

fn expected_u64(expected: &Value, key: &str) -> u64 {
    expected[key]
        .as_u64()
        .unwrap_or_else(|| panic!("expected.{key} is a u64"))
}

fn expected_cost_numerator(expected: &Value, key: &str) -> i128 {
    let number = expected[key]
        .as_number()
        .unwrap_or_else(|| panic!("expected.{key} is a number"));
    let decimal = Decimal::from_str(&number.to_string())
        .unwrap_or_else(|_| panic!("expected.{key} is an exact decimal"));
    let scaled = decimal * Decimal::from(1_000_000_000_000_i64);
    assert!(
        scaled.fract().is_zero(),
        "expected.{key} exceeds price precision"
    );
    scaled
        .to_i128()
        .unwrap_or_else(|| panic!("expected.{key} fits the exact cost range"))
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn harness_for(case_id: &str) -> (CorpusHarness, Value) {
    let case = case_spec(case_id);
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    copy_tree(
        &corpus_root().join(case["activeRoot"].as_str().unwrap()),
        &active,
    );
    let archive_root = case["archivedRoot"].as_str().map(|root| {
        copy_tree(&corpus_root().join(root), &archive);
        archive.clone()
    });
    let db = Db::open(temp.path().join("data/codex-usage.db")).unwrap();
    let roots = IngestRoots {
        active: Some(active),
        archive: archive_root,
    };
    (
        CorpusHarness {
            _temp: temp,
            db,
            roots,
        },
        case,
    )
}

fn scalar(connection: &Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn visible_sessions(connection: &Connection) -> i64 {
    scalar(
        connection,
        r#"
        SELECT COUNT(*) FROM threads
        WHERE id IN (
            SELECT thread_id FROM events
            UNION SELECT thread_id FROM usage_facts
            UNION SELECT thread_id FROM messages
        )
        "#,
    )
}

fn exact_fixture_prices(connection: &Connection) {
    connection
        .execute(
            "DELETE FROM model_prices WHERE model_id IN ('gpt-5.5','gpt-5.6-sol','codex-auto-review')",
            [],
        )
        .unwrap();
    for model in ["gpt-5.5", "gpt-5.6-sol"] {
        connection
            .execute(
                r#"
                INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                ) VALUES(?1,'1970-01-01T00:00:00Z',5000000,500000,30000000,'USD','fixture')
                "#,
                [model],
            )
            .unwrap();
    }
}

fn usage_totals(connection: &Connection, predicate: &str) -> (i64, i64, i64, i64, i64) {
    connection
        .query_row(
            &format!(
                r#"
                SELECT COALESCE(SUM(input_tokens),0),
                       COALESCE(SUM(cached_input_tokens),0),
                       COALESCE(SUM(output_tokens),0),
                       COALESCE(SUM(reasoning_tokens),0),
                       COALESCE(SUM(total_tokens),0)
                FROM usage_facts WHERE {predicate}
                "#
            ),
            [],
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
        .unwrap()
}

fn assert_usage(actual: (i64, i64, i64, i64, i64), expected: &Value) {
    assert_eq!(actual.0 as u64, expected_u64(expected, "inputTokens"));
    assert_eq!(actual.1 as u64, expected_u64(expected, "cachedInputTokens"));
    assert_eq!(actual.2 as u64, expected_u64(expected, "outputTokens"));
    assert_eq!(
        actual.3 as u64,
        expected_u64(expected, "reasoningOutputTokens")
    );
    assert_eq!(actual.4 as u64, expected_u64(expected, "totalTokens"));
}

fn projection_snapshot(connection: &Connection) -> ProjectionSnapshot {
    ProjectionSnapshot {
        table_counts: PROJECTION_TABLES
            .into_iter()
            .map(|table| {
                (
                    table,
                    scalar(connection, &format!("SELECT COUNT(*) FROM {table}")),
                )
            })
            .collect(),
        usage_sums: usage_totals(connection, "1=1"),
    }
}

#[test]
fn every_nonblank_corpus_jsonl_line_is_valid_json() {
    let (fixture_count, record_count) = assert_fixture_jsonl_lines_are_valid();
    assert!(fixture_count > 0, "corpus must contain JSONL fixtures");
    assert!(
        record_count > 0,
        "corpus fixtures must contain JSON records"
    );
}

#[test]
fn every_manifest_case_has_an_idempotent_projection_on_rescan() {
    let (fixture_count, record_count) = assert_fixture_jsonl_lines_are_valid();
    assert!(fixture_count > 0, "corpus must contain JSONL fixtures");
    assert!(
        record_count > 0,
        "corpus fixtures must contain JSON records"
    );

    for case in manifest_cases() {
        let case_id = case["id"].as_str().expect("manifest case id is a string");
        let source_files = expected_u64(&case, "sourceFiles");
        let (harness, _) = harness_for(case_id);

        let first = scan_once(&harness.db, &harness.roots).unwrap();
        assert_eq!(first.files_failed, 0, "first scan failed for {case_id}");
        assert_eq!(
            first.files_seen, source_files,
            "first scan source count drifted for {case_id}"
        );
        assert_eq!(
            first.files_ingested, source_files,
            "first scan did not ingest every source for {case_id}"
        );

        let connection = harness.db.connect().unwrap();
        let before = projection_snapshot(&connection);
        drop(connection);

        let second = scan_once(&harness.db, &harness.roots).unwrap();
        assert_eq!(second.files_failed, 0, "second scan failed for {case_id}");
        assert_eq!(
            second.files_unchanged, source_files,
            "second scan did not recognize every source for {case_id}"
        );
        let after = projection_snapshot(&harness.db.connect().unwrap());
        assert_eq!(
            after, before,
            "second scan changed table counts or usage sums for {case_id}"
        );
    }
}

#[test]
fn repeated_rate_limit_snapshots_materialize_each_cumulative_increment_once() {
    let (harness, case) = harness_for("rate_limit_duplicates");
    let expected = &case["expected"];

    let report = scan_once(&harness.db, &harness.roots).unwrap();
    assert_eq!(report.files_failed, 0);
    assert_eq!(report.records_read, expected_u64(expected, "recordsRead"));

    let connection = harness.db.connect().unwrap();
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM usage_facts") as u64,
        expected_u64(expected, "usageFacts"),
        "same cumulative snapshot emitted for multiple rate-limit buckets must not be charged twice"
    );
    assert_usage(usage_totals(&connection, "1=1"), &expected["nativeUsage"]);
}

#[test]
fn replay_corpus_counts_only_native_work_and_hides_pure_replays() {
    let (harness, case) = harness_for("replay_spike");
    let expected = &case["expected"];
    exact_fixture_prices(&harness.db.connect().unwrap());

    let first = scan_once(&harness.db, &harness.roots).unwrap();
    assert_eq!(first.files_failed, 0);
    assert_eq!(first.files_seen, expected_u64(&case, "sourceFiles"));
    assert_eq!(first.files_ingested, expected_u64(&case, "sourceFiles"));

    let connection = harness.db.connect().unwrap();
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM source_files") as u64,
        expected_u64(&case, "sourceFiles")
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM source_files WHERE native_started=1"
        ) as u64,
        expected_u64(expected, "nativeRollouts")
    );
    assert_eq!(
        visible_sessions(&connection) as u64,
        expected_u64(expected, "visibleSessionRows")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM turns") as u64,
        expected_u64(expected, "turns")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM messages") as u64,
        expected_u64(expected, "durableMessages")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM tool_calls") as u64,
        expected_u64(expected, "toolCalls")
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM pragma_table_info('tool_calls') WHERE name IN ('input','output')",
        ),
        0,
        "tool payloads are not part of the query projection"
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='content_json'",
        ),
        0,
        "embedded attachment payloads are not part of the query projection"
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM usage_facts") as u64,
        expected_u64(expected, "usageFacts")
    );
    assert_usage(
        usage_totals(&connection, "1=1"),
        &expected["allNativeUsage"],
    );

    const JULY_THREAD: &str = "019f64aa-21e8-7a41-916f-0fe9b845eede";
    assert_usage(
        usage_totals(&connection, &format!("thread_id='{JULY_THREAD}'")),
        &expected["july15NativeUsage"],
    );
    let july_cost: i128 = connection
        .query_row(
            "SELECT COALESCE(SUM(cost_numerator),0) FROM priced_usage WHERE thread_id=?1",
            [JULY_THREAD],
            |row| row.get::<_, i64>(0).map(i128::from),
        )
        .unwrap();
    assert_eq!(
        july_cost,
        expected_cost_numerator(&expected["july15NativeUsage"], "costUsd"),
    );

    const MAY_THREAD: &str = "019df47e-62a3-7ba3-a57f-d7f8565ec08f";
    assert_usage(
        usage_totals(&connection, &format!("thread_id='{MAY_THREAD}'")),
        &expected["may4NativeUsage"],
    );
    let may_cost: i128 = connection
        .query_row(
            "SELECT COALESCE(SUM(cost_numerator),0) FROM priced_usage WHERE thread_id=?1",
            [MAY_THREAD],
            |row| row.get::<_, i64>(0).map(i128::from),
        )
        .unwrap();
    assert_eq!(
        may_cost,
        expected_cost_numerator(&expected["may4NativeUsage"], "costUsd"),
    );

    for replay_only in [
        "019f64af-12fe-7170-8fcd-7d636000a8af",
        "019f64af-6612-79d0-81d0-c53d7d6caef0",
    ] {
        let projected: i64 = connection
            .query_row(
                r#"
                SELECT
                    (SELECT COUNT(*) FROM turns WHERE rollout_id=?1) +
                    (SELECT COUNT(*) FROM messages WHERE rollout_id=?1) +
                    (SELECT COUNT(*) FROM events WHERE rollout_id=?1) +
                    (SELECT COUNT(*) FROM tool_calls WHERE rollout_id=?1) +
                    (SELECT COUNT(*) FROM usage_facts WHERE rollout_id=?1)
                "#,
                [replay_only],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(projected, 0, "pure replay {replay_only} projected activity");
    }

    let before = projection_snapshot(&connection);
    drop(connection);
    let second = scan_once(&harness.db, &harness.roots).unwrap();
    assert_eq!(second.files_failed, 0);
    assert_eq!(second.files_unchanged, expected_u64(&case, "sourceFiles"));
    assert_eq!(
        projection_snapshot(&harness.db.connect().unwrap()),
        before,
        "rescanning must not duplicate any normalized projection"
    );
}

#[test]
fn rich_trace_preserves_activity_tools_subagents_compaction_and_unknowns() {
    let (harness, case) = harness_for("rich_trace");
    let expected = &case["expected"];
    exact_fixture_prices(&harness.db.connect().unwrap());
    let report = scan_once(&harness.db, &harness.roots).unwrap();
    assert_eq!(report.files_failed, 0);

    let connection = harness.db.connect().unwrap();
    assert_eq!(visible_sessions(&connection), 1);
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM turns") as u64,
        expected_u64(expected, "turns")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM messages") as u64,
        expected_u64(expected, "durableMessages")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM tool_calls") as u64,
        expected_u64(expected, "toolCalls")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM usage_facts") as u64,
        expected_u64(expected, "usageFacts")
    );
    assert_usage(usage_totals(&connection, "1=1"), &expected["nativeUsage"]);

    for (kind, count) in [
        ("message", 1),
        ("update", 1),
        ("reasoning", 1),
        ("tool_call", 8),
        ("subagent", 3),
        ("goal", 1),
        ("compaction", 1),
        ("final", 1),
    ] {
        let actual: i64 = connection
            .query_row("SELECT COUNT(*) FROM events WHERE kind=?1", [kind], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(actual, count, "unexpected semantic event count for {kind}");
    }
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM events WHERE kind IN ('message','update','reasoning','tool_call','subagent','goal','compaction','final')"
        ) as u64,
        expected_u64(&expected["activityEvents"], "total")
    );

    let exec: (String, String) = connection
        .query_row(
            "SELECT name,status FROM tool_calls WHERE call_id='call_rich_1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(exec.0, "exec");
    assert_eq!(exec.1, "completed");
    let completion_only: (String, Option<String>, i64) = connection
        .query_row(
            "SELECT name,namespace,duration_ms FROM tool_calls
             WHERE call_id='exec-evidence-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(completion_only.0, "js");
    assert_eq!(completion_only.1.as_deref(), Some("node_repl"));
    assert_eq!(completion_only.2, 313);
    let completion_event: (String, String, String) = connection
        .query_row(
            "SELECT kind,label,tool_name FROM events WHERE call_id='exec-evidence-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        completion_event,
        ("tool_call".into(), "js".into(), "js".into())
    );
    for (call_id, name, status) in [
        ("web-evidence-1", "web_search_call", "completed"),
        ("patch-evidence-1", "apply_patch", "completed"),
        ("image-evidence-1", "image_generation_call", "completed"),
    ] {
        let projected: (String, String) = connection
            .query_row(
                "SELECT name,status FROM tool_calls WHERE call_id=?1",
                [call_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(projected.0, name);
        assert_eq!(projected.1, status);
    }

    assert_eq!(
        scalar(
            &connection,
            r#"SELECT COUNT(*) FROM events
               WHERE kind='subagent'
                 AND payload_json LIKE '%019f677d-de1d-7a80-99ad-fdb6076b36d3%'"#
        ),
        2,
        "the child branch has one start and one completion lifecycle event"
    );
    assert_eq!(
        scalar(
            &connection,
            r#"SELECT COUNT(*) FROM events
               WHERE kind='subagent' AND label='/root/storage_audit → /root'"#
        ),
        1,
        "the child-agent message remains attached to the activity trace"
    );
    let unknown_payloads = connection
        .prepare(
            "SELECT payload_json FROM events
             WHERE kind='system' AND label='future_trace_marker'",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        unknown_payloads.len() as u64,
        expected_u64(expected, "unknownRecordsPreserved")
    );
    let unknown_payload: Value = serde_json::from_str(&unknown_payloads[0]).unwrap();
    assert_eq!(
        unknown_payload,
        serde_json::json!({"type":"future_trace_marker","schema_version":99}),
        "unknown records retain only bounded scalar identity metadata"
    );
}

#[test]
fn legacy_pre_envelope_history_is_retained() {
    let (harness, case) = harness_for("legacy_v0");
    let expected = &case["expected"];
    let report = scan_once(&harness.db, &harness.roots).unwrap();
    assert_eq!(
        report.files_failed, 0,
        "the pre-envelope header is a valid owner record"
    );

    let connection = harness.db.connect().unwrap();
    assert_eq!(visible_sessions(&connection), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM source_files"), 1);
    assert_eq!(
        scalar(&connection, "SELECT line_number FROM source_files"),
        10
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM turns") as u64,
        expected_u64(expected, "implicitTurns")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM messages") as u64,
        expected_u64(expected, "durableMessages")
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM events WHERE kind='reasoning'"
        ) as u64,
        expected_u64(expected, "reasoningSummaries")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM tool_calls") as u64,
        expected_u64(expected, "toolCalls")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM usage_facts") as u64,
        expected_u64(expected, "usageFacts")
    );
    let started_at: String = connection
        .query_row("SELECT started_at FROM threads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(started_at, "2025-08-31T07:17:52.824000000Z");
    let tool: (String, String) = connection
        .query_row("SELECT name,status FROM tool_calls", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(tool.0, "shell");
    assert_eq!(tool.1, "completed");
}

#[test]
fn sparse_sessions_and_pricing_alias_reprice_without_reingestion() {
    let (harness, case) = harness_for("sparse_pricing");
    let expected = &case["expected"];
    let connection = harness.db.connect().unwrap();
    exact_fixture_prices(&connection);
    connection
        .execute(
            "DELETE FROM model_aliases WHERE observed_model_id='codex-auto-review'",
            [],
        )
        .unwrap();
    drop(connection);

    let report = scan_once(&harness.db, &harness.roots).unwrap();
    assert_eq!(report.files_failed, 0);
    let connection = harness.db.connect().unwrap();
    assert_eq!(visible_sessions(&connection), 2);
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM source_files WHERE native_started=1"
        ) as u64,
        expected_u64(expected, "nativeRollouts")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM turns") as u64,
        expected_u64(expected, "turns")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM messages") as u64,
        expected_u64(expected, "durableMessages")
    );
    assert_eq!(
        scalar(&connection, "SELECT COUNT(*) FROM usage_facts") as u64,
        expected_u64(expected, "usageFacts")
    );
    assert_usage(usage_totals(&connection, "1=1"), &expected["nativeUsage"]);

    const ABORTED_ROLLOUT: &str = "019f6767-979c-7df1-a512-9830528bda62";
    assert_eq!(
        connection
            .query_row(
                "SELECT content FROM messages WHERE rollout_id=?1",
                [ABORTED_ROLLOUT],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "Why is this giant usage bucket not visible in Sessions?"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM usage_facts WHERE rollout_id=?1",
                [ABORTED_ROLLOUT],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "a null token_count.info is not billable usage"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE rollout_id=?1 AND kind='state' AND status='interrupted'",
                [ABORTED_ROLLOUT],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let before: (i64, i128, i64) = connection
        .query_row(
            r#"SELECT COUNT(*),COALESCE(SUM(cost_numerator),0),
                      COALESCE(SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END),0)
               FROM priced_usage"#,
            [],
            |row| Ok((row.get(0)?, i128::from(row.get::<_, i64>(1)?), row.get(2)?)),
        )
        .unwrap();
    assert_eq!(before.0 as u64, expected_u64(expected, "usageFacts"));
    assert_eq!(
        before.1,
        expected_cost_numerator(&expected["beforeAlias"], "knownCostUsd"),
    );
    assert_eq!(
        before.2 as u64,
        expected_u64(&expected["beforeAlias"], "unpricedTokens")
    );

    let usage_identity: String = connection
        .query_row(
            "SELECT id || ':' || model || ':' || total_tokens FROM usage_facts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            r#"INSERT INTO model_aliases(observed_model_id,canonical_model_id,created_at)
               VALUES('codex-auto-review','gpt-5.5','2026-07-15T00:00:00Z')"#,
            [],
        )
        .unwrap();
    let after: (i128, i64, String) = connection
        .query_row(
            r#"SELECT COALESCE(SUM(cost_numerator),0),
                      COALESCE(SUM(CASE WHEN price_known=0 THEN total_tokens ELSE 0 END),0),
                      MIN(model)
               FROM priced_usage"#,
            [],
            |row| Ok((i128::from(row.get::<_, i64>(0)?), row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        after.0,
        expected_cost_numerator(&expected["afterAlias"], "costUsd")
    );
    assert_eq!(after.1, 0);
    assert_eq!(
        after.2, "codex-auto-review",
        "trace data keeps the observed ID"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT id || ':' || model || ':' || total_tokens FROM usage_facts",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        usage_identity,
        "adding an alias reprices the query without rewriting usage facts"
    );
}

#[test]
fn partial_tail_is_checkpointed_then_committed_exactly_once() {
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    fs::create_dir_all(&active).unwrap();
    let source_path = corpus_root()
        .join("rich_trace/active")
        .join("rollout-2026-07-15T22-12-38-019f6768-ef84-74d3-ab05-e4b5fb717fa8.jsonl");
    let bytes = fs::read(&source_path).unwrap();
    let final_line_start = bytes[..bytes.len() - 1]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|position| position + 1)
        .unwrap();
    let complete_lines = bytes[..final_line_start]
        .iter()
        .filter(|byte| **byte == b'\n')
        .count() as i64;
    let split = final_line_start + (bytes.len() - final_line_start) / 2;
    let destination = active.join(source_path.file_name().unwrap());
    fs::write(&destination, &bytes[..split]).unwrap();

    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let roots = IngestRoots {
        active: Some(active),
        archive: None,
    };
    let first = scan_once(&db, &roots).unwrap();
    assert_eq!(first.files_failed, 0);
    let connection = db.connect().unwrap();
    let checkpoint: (i64, i64, i64) = connection
        .query_row(
            "SELECT byte_offset,size_bytes,line_number FROM source_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(checkpoint.0 as usize, final_line_start);
    assert_eq!(checkpoint.1 as usize, split);
    assert_eq!(checkpoint.2, complete_lines);
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM events WHERE kind='turn_completed'"
        ),
        0
    );
    assert_eq!(
        connection
            .query_row("SELECT status FROM turns", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "running",
        "a final answer is provisional while the explicit native lifecycle is still open"
    );
    let partial_snapshot = projection_snapshot(&connection);
    drop(connection);

    let unchanged_partial = scan_once(&db, &roots).unwrap();
    assert_eq!(unchanged_partial.files_unchanged, 1);
    assert_eq!(unchanged_partial.records_read, 0);
    let connection = db.connect().unwrap();
    assert_eq!(projection_snapshot(&connection), partial_snapshot);
    let unchanged_checkpoint: (i64, i64, i64) = connection
        .query_row(
            "SELECT byte_offset,size_bytes,line_number FROM source_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(unchanged_checkpoint, checkpoint);
    drop(connection);

    OpenOptions::new()
        .append(true)
        .open(&destination)
        .unwrap()
        .write_all(&bytes[split..])
        .unwrap();
    let second = scan_once(&db, &roots).unwrap();
    assert_eq!(second.files_failed, 0);
    let connection = db.connect().unwrap();
    let complete: (i64, i64, i64) = connection
        .query_row(
            "SELECT byte_offset,size_bytes,line_number FROM source_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(complete.0 as usize, bytes.len());
    assert_eq!(complete.1 as usize, bytes.len());
    assert_eq!(complete.2, complete_lines + 1);
    assert_eq!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM events WHERE kind='turn_completed'"
        ),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT status FROM turns", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "completed"
    );
    let before = projection_snapshot(&connection);
    drop(connection);
    let third = scan_once(&db, &roots).unwrap();
    assert_eq!(third.files_unchanged, 1);
    assert_eq!(projection_snapshot(&db.connect().unwrap()), before);
}

#[test]
fn larger_same_path_rewrite_with_changed_prefix_rebuilds_instead_of_appending() {
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    fs::create_dir_all(&active).unwrap();
    let source_path = corpus_root()
        .join("sparse_pricing/active")
        .join("rollout-2026-07-15T06-52-26-019f641e-7747-7263-9508-5466e871bd40.jsonl");
    let destination = active.join(source_path.file_name().unwrap());
    let original = fs::read_to_string(source_path).unwrap();
    fs::write(&destination, &original).unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let roots = IngestRoots {
        active: Some(active),
        archive: None,
    };

    scan_once(&db, &roots).unwrap();
    assert_eq!(
        scalar(
            &db.connect().unwrap(),
            "SELECT input_tokens FROM usage_facts"
        ),
        25_410
    );

    let rewritten = original
        .replace(
            "/Users/example/Documents/Codex/automation-review",
            "/Users/example/Documents/Codex/automation-review-with-a-deliberately-longer-prefix",
        )
        .replace("\"input_tokens\":25410", "\"input_tokens\":125410")
        .replace("\"total_tokens\":25607", "\"total_tokens\":125607");
    assert!(rewritten.len() > original.len());
    fs::write(&destination, rewritten).unwrap();

    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_failed, 0);
    assert_eq!(report.files_ingested, 1);
    let connection = db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM usage_facts"), 1);
    assert_eq!(
        scalar(&connection, "SELECT input_tokens FROM usage_facts"),
        125_410,
        "the prior fact must be cleared rather than retained as an append prefix"
    );
    assert_eq!(
        scalar(&connection, "SELECT total_tokens FROM usage_facts"),
        125_607
    );
    let cwd: String = connection
        .query_row("SELECT cwd FROM rollouts", [], |row| row.get(0))
        .unwrap();
    assert!(cwd.ends_with("automation-review-with-a-deliberately-longer-prefix"));
}

#[test]
fn same_path_replaced_by_new_owner_removes_old_projection_transactionally() {
    const OLD_ROLLOUT: &str = "019f641e-7747-7263-9508-5466e871bd40";
    const OLD_THREAD: &str = "019ee21b-697b-7090-b865-2a7acf43e3fc";
    const NEW_ROLLOUT: &str = "019f6767-979c-7df1-a512-9830528bda62";

    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    fs::create_dir_all(&active).unwrap();
    let destination = active.join("rollout-current.jsonl");
    fs::copy(
        corpus_root()
            .join("sparse_pricing/active")
            .join("rollout-2026-07-15T06-52-26-019f641e-7747-7263-9508-5466e871bd40.jsonl"),
        &destination,
    )
    .unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let roots = IngestRoots {
        active: Some(active),
        archive: None,
    };

    scan_once(&db, &roots).unwrap();
    let connection = db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM usage_facts"), 1);
    assert_eq!(
        connection
            .query_row("SELECT rollout_id FROM source_files", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        OLD_ROLLOUT
    );
    drop(connection);

    fs::copy(
        corpus_root()
            .join("sparse_pricing/archived")
            .join("rollout-2026-07-15T22-11-10-019f6767-979c-7df1-a512-9830528bda62.jsonl"),
        &destination,
    )
    .unwrap();
    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_failed, 0);
    assert_eq!(report.files_ingested, 1);

    let connection = db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM source_files"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM rollouts"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM agent_runs"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM threads"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM usage_facts"), 0);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM messages"), 1);
    assert_eq!(visible_sessions(&connection), 1);
    assert_eq!(
        connection
            .query_row("SELECT rollout_id FROM source_files", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        NEW_ROLLOUT
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM rollouts WHERE id=?1",
                [OLD_ROLLOUT],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM threads WHERE id=?1",
                [OLD_THREAD],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "the replaced rollout must not leave an orphan thread projection"
    );
}

#[test]
fn deleted_source_reconciles_its_normalized_rollout() {
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    fs::create_dir_all(&active).unwrap();
    let source_path = corpus_root()
        .join("sparse_pricing/active")
        .join("rollout-2026-07-15T06-52-26-019f641e-7747-7263-9508-5466e871bd40.jsonl");
    let destination = active.join(source_path.file_name().unwrap());
    fs::copy(source_path, &destination).unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let roots = IngestRoots {
        active: Some(active),
        archive: None,
    };

    scan_once(&db, &roots).unwrap();
    let connection = db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM source_files"), 1);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM usage_facts"), 1);
    assert_eq!(visible_sessions(&connection), 1);
    drop(connection);

    fs::remove_file(destination).unwrap();
    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_seen, 0);
    let connection = db.connect().unwrap();
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM source_files"), 0);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM rollouts"), 0);
    assert_eq!(scalar(&connection, "SELECT COUNT(*) FROM usage_facts"), 0);
    assert_eq!(visible_sessions(&connection), 0);
}

#[test]
fn moving_source_from_active_to_archive_preserves_one_projection() {
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    fs::create_dir_all(&active).unwrap();
    fs::create_dir_all(&archive).unwrap();
    let source_path = corpus_root()
        .join("sparse_pricing/archived")
        .join("rollout-2026-07-15T22-11-10-019f6767-979c-7df1-a512-9830528bda62.jsonl");
    let active_path = active.join(source_path.file_name().unwrap());
    fs::copy(source_path, &active_path).unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive.clone()),
    };

    scan_once(&db, &roots).unwrap();
    let before = projection_snapshot(&db.connect().unwrap());
    let archived_path = archive.join(active_path.file_name().unwrap());
    fs::rename(active_path, &archived_path).unwrap();
    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_failed, 0);
    let connection = db.connect().unwrap();
    assert_eq!(projection_snapshot(&connection), before);
    let metadata: (String, i64) = connection
        .query_row("SELECT path,archived FROM source_files", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(Path::new(&metadata.0), archived_path);
    assert_eq!(metadata.1, 1);
}
