#![cfg(test)]

use super::super::*;
use super::support::*;

#[test]
fn token_counts_outside_fixed_point_domain_fail_without_wrapping() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000121";
    let turn = "019f64ab-0000-7000-8000-000000000121";
    write_fixture(
        &sessions.join("overflow.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", MAX_USAGE_TOKENS_PER_FACT + 1),
        ],
    );

    let error = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("input_tokens"));
    let connection = db.connect().unwrap();
    let stored: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM usage_facts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, (0, 0));
}

#[test]
fn malformed_token_accounting_is_rejected_instead_of_discarded() {
    let cases = [
        (
            "negative",
            serde_json::json!({"total_token_usage":{"input_tokens":-1}}),
            "invalid total_token_usage",
        ),
        (
            "wrong-type",
            serde_json::json!({"total_token_usage":{"input_tokens":"100"}}),
            "invalid total_token_usage",
        ),
        (
            "cached-exceeds-input",
            serde_json::json!({"total_token_usage":{
                "input_tokens":100,"cached_input_tokens":101,"output_tokens":1
            }}),
            "cached_input_tokens greater than input_tokens",
        ),
        (
            "non-object-info",
            serde_json::json!(["not", "an", "object"]),
            "token_count.info with a non-object value",
        ),
    ];

    for (label, info, expected) in cases {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let owner = "019f64aa-0000-7000-8000-000000000122";
        let turn = "019f64ab-0000-7000-8000-000000000122";
        write_fixture(
            &sessions.join(format!("{label}.jsonl")),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", turn),
                context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z",
                    "type":"event_msg","payload":{
                        "type":"token_count","info":info
                    }
                }),
            ],
        );

        let error = scan_once(
            &db,
            &IngestRoots {
                active: Some(sessions),
                archive: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains(expected),
            "{label} produced unexpected error: {error:#}"
        );
        let connection = db.connect().unwrap();
        let stored: (i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM source_files),
                            (SELECT COUNT(*) FROM usage_facts)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (0, 0), "{label} left a partial projection");
    }
}

#[test]
fn absent_null_and_legacy_omitted_token_fields_remain_supported() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000123";
    let turn = "019f64ab-0000-7000-8000-000000000123";
    write_fixture(
        &sessions.join("legacy-token-usage.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z",
                    "type":"event_msg","payload":{"type":"token_count"}}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z",
                    "type":"event_msg","payload":{"type":"token_count","info":null}}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:04Z",
            "type":"event_msg","payload":{"type":"token_count","info":{
                "total_token_usage":null,"last_token_usage":null
            }}}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:05Z",
            "type":"event_msg","payload":{"type":"token_count","info":{
                "last_token_usage":{"input_tokens":7,"output_tokens":2}
            }}}),
        ],
    );

    let report = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    assert_eq!(report.files_failed, 0);
    let projected: (i64, i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),SUM(input_tokens),SUM(cached_input_tokens),
                        SUM(output_tokens),SUM(total_tokens)
                 FROM usage_facts",
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
        .unwrap();
    assert_eq!(projected, (1, 7, 0, 2, 9));
}

#[test]
fn explicit_null_token_info_resets_cumulative_scope_without_usage() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000148";
    let turn = "019f64ab-0000-7000-8000-000000000148";
    let snapshot = |timestamp: &str, input: u64, cached: u64| {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"token_count","info":{
                "total_token_usage":{
                    "input_tokens":input,"cached_input_tokens":cached,
                    "output_tokens":1,"total_tokens":input+1
                },
                "last_token_usage":{
                    "input_tokens":input,"cached_input_tokens":cached,
                    "output_tokens":1,"total_tokens":input+1
                }
            }
        }})
    };
    let null_boundary = |timestamp: &str| {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"token_count","info":null
        }})
    };
    write_fixture(
        &sessions.join("null-token-scope-boundary.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            snapshot("2026-07-15T09:00:02Z", 100, 20),
            null_boundary("2026-07-15T09:00:03Z"),
            null_boundary("2026-07-15T09:00:04Z"),
            snapshot("2026-07-15T09:00:05Z", 110, 35),
            snapshot("2026-07-15T09:00:06Z", 110, 35),
        ],
    );

    let report = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    assert_eq!(report.files_failed, 0);
    let projected: (i64, i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),SUM(input_tokens),SUM(cached_input_tokens),
                        SUM(output_tokens),SUM(total_tokens)
                 FROM usage_facts",
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
        .unwrap();
    assert_eq!(projected, (2, 210, 55, 2, 212));
}

#[test]
fn cached_input_delta_greater_than_input_delta_is_rejected_not_clamped() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000124";
    let turn = "019f64ab-0000-7000-8000-000000000124";
    let snapshot = |timestamp: &str, input: u64, cached: u64| {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"token_count","info":{"total_token_usage":{
                "input_tokens":input,"cached_input_tokens":cached,
                "output_tokens":1,"total_tokens":input+1
            }}
        }})
    };
    write_fixture(
        &sessions.join("invalid-cached-delta.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            snapshot("2026-07-15T09:00:02Z", 100, 0),
            snapshot("2026-07-15T09:00:03Z", 110, 15),
        ],
    );

    let error = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap_err();
    assert!(
        format!("{error:#}")
            .contains("derived token usage.cached_input_tokens greater than input_tokens"),
        "unexpected cached delta error: {error:#}"
    );
    let stored: (i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM usage_facts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, (0, 0));
}

#[test]
fn token_snapshots_reject_contradictory_totals_and_reasoning() {
    for (field, usage, expected) in [
        (
            "total_token_usage",
            serde_json::json!({
                "input_tokens":10,"output_tokens":5,
                "reasoning_output_tokens":1,"total_tokens":999
            }),
            "total_tokens inconsistent",
        ),
        (
            "last_token_usage",
            serde_json::json!({
                "input_tokens":10,"output_tokens":5,
                "reasoning_output_tokens":99,"total_tokens":15
            }),
            "reasoning_output_tokens greater",
        ),
    ] {
        let info = serde_json::json!({field:usage});
        let error = parse_token_usage(&info, field, 7).unwrap_err().to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }
}

#[test]
fn total_only_last_usage_hint_is_ignored_without_double_counting() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000779";
    let turn = "019f64ab-0000-7000-8000-000000000779";
    let snapshot = |timestamp: &str, total: Value, last: Value| {
        serde_json::json!({"timestamp":timestamp,"type":"event_msg","payload":{
            "type":"token_count","info":{
                "total_token_usage":total,
                "last_token_usage":last
            }
        }})
    };
    write_fixture(
        &sessions.join("total-only-last-hint.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            snapshot(
                "2026-07-15T09:00:02Z",
                serde_json::json!({
                    "input_tokens":0,"cached_input_tokens":0,
                    "output_tokens":0,"reasoning_output_tokens":0,
                    "total_tokens":0
                }),
                serde_json::json!({
                    "input_tokens":0,"cached_input_tokens":0,
                    "output_tokens":0,"reasoning_output_tokens":0,
                    "total_tokens":18596
                }),
            ),
            snapshot(
                "2026-07-15T09:00:03Z",
                serde_json::json!({
                    "input_tokens":36526,"cached_input_tokens":23936,
                    "output_tokens":404,"reasoning_output_tokens":210,
                    "total_tokens":36930
                }),
                serde_json::json!({
                    "input_tokens":36526,"cached_input_tokens":23936,
                    "output_tokens":404,"reasoning_output_tokens":210,
                    "total_tokens":36930
                }),
            ),
            snapshot(
                "2026-07-15T09:00:04Z",
                serde_json::json!({
                    "input_tokens":10,"cached_input_tokens":4,
                    "output_tokens":2,"reasoning_output_tokens":1,
                    "total_tokens":12
                }),
                serde_json::json!({
                    "input_tokens":0,"cached_input_tokens":0,
                    "output_tokens":0,"reasoning_output_tokens":0,
                    "total_tokens":2048
                }),
            ),
        ],
    );

    let report = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    assert_eq!(report.files_failed, 0);
    let stored: (i64, i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),SUM(input_tokens),SUM(cached_input_tokens),
                        SUM(output_tokens),SUM(total_tokens)
                 FROM usage_facts",
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
        .unwrap();
    assert_eq!(stored, (2, 36536, 23940, 406, 36942));
}

#[test]
fn total_only_last_usage_without_cumulative_counter_is_rejected() {
    let info = serde_json::json!({"last_token_usage":{
        "input_tokens":0,"cached_input_tokens":0,
        "output_tokens":0,"reasoning_output_tokens":0,
        "total_tokens":18596
    }});
    assert!(last_token_usage_is_total_only_hint(&info));
    let error = parse_token_usage(&info, "last_token_usage", 7)
        .unwrap_err()
        .to_string();
    assert!(error.contains("total_tokens inconsistent"));
}

#[test]
fn cumulative_context_window_offset_is_normalized_without_guessing_components() {
    let sentinel = serde_json::json!({
        "model_context_window":258400,
        "total_token_usage":{
            "input_tokens":0,"cached_input_tokens":0,
            "output_tokens":0,"reasoning_output_tokens":0,
            "total_tokens":258400
        }
    });
    assert_eq!(
        parse_total_token_usage(&sentinel, 7)
            .unwrap()
            .unwrap()
            .total_tokens,
        0
    );

    let cumulative = serde_json::json!({
        "model_context_window":258400,
        "total_token_usage":{
            "input_tokens":223027,"cached_input_tokens":215424,
            "output_tokens":673,"reasoning_output_tokens":265,
            "total_tokens":482100
        }
    });
    let usage = parse_total_token_usage(&cumulative, 8).unwrap().unwrap();
    assert_eq!(usage.input_tokens, 223027);
    assert_eq!(usage.output_tokens, 673);
    assert_eq!(usage.total_tokens, 223700);

    let unrelated_mismatch = serde_json::json!({
        "model_context_window":258400,
        "total_token_usage":{
            "input_tokens":223027,"cached_input_tokens":215424,
            "output_tokens":673,"reasoning_output_tokens":265,
            "total_tokens":482101
        }
    });
    let error = parse_total_token_usage(&unrelated_mismatch, 9)
        .unwrap_err()
        .to_string();
    assert!(error.contains("total_tokens inconsistent"));
}

#[test]
fn aggregate_overflow_rolls_back_the_raw_usage_fact() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000777";
    let turn = "019f64ab-0000-7000-8000-000000000777";
    write_fixture(
        &sessions.join("overflow.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 1),
        ],
    );

    const GLOBAL_TOTAL_LIMIT: i64 = 9_007_199_254_740_991;
    db.connect()
        .unwrap()
        .execute(
            "UPDATE usage_global_totals SET
                    fact_count=?1,input_tokens=?1-1,cached_input_tokens=0,
                    output_tokens=1,reasoning_tokens=0,total_tokens=?1
                 WHERE id=1",
            [GLOBAL_TOTAL_LIMIT],
        )
        .unwrap();

    let error = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("failed"));

    let connection = db.connect().unwrap();
    let state: (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                    (SELECT COUNT(*) FROM usage_facts),
                    (SELECT COUNT(*) FROM usage_activity_rollups),
                    fact_count,total_tokens
                 FROM usage_global_totals WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(state, (0, 0, GLOBAL_TOTAL_LIMIT, GLOBAL_TOTAL_LIMIT));
}
