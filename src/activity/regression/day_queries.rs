#![cfg(test)]

use super::*;

#[test]
fn activity_days_clamp_extreme_endpoints_and_overflowing_durations() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "PRAGMA ignore_check_constraints=ON;
             INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('bounded-extremes','Bounded extremes',
                    '2026-07-15T10:00:00.000000000Z',
                    '2026-07-17T12:00:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('bounded-extremes','bounded-extremes',
                    '2026-07-15T10:00:00.000000000Z',
                    '2026-07-17T12:00:00.000000000Z',0);
             INSERT INTO turns(
                id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
             ) VALUES(
                'extreme-completion','bounded-extremes','bounded-extremes',
                '2026-07-15T11:00:00.000000000Z',
                '9999-12-31T23:59:59.999999999Z','completed',1000
             );
             INSERT INTO events(
                id,thread_id,rollout_id,timestamp,source_line,kind,duration_ms,native
             ) VALUES(
                'overflowing-duration','bounded-extremes','bounded-extremes',
                '2026-07-16T09:00:00.000000000Z',1,'tool_call',
                9223372036854775807,1
             );
             PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap();

    let days = query_activity_day_summaries_batched(&connection, "bounded-extremes").unwrap();
    assert_eq!(
        days.iter().map(|day| day.date.as_str()).collect::<Vec<_>>(),
        vec!["2026-07-17", "2026-07-16", "2026-07-15"],
        "a corrupt completion must stop at the thread's last corroborated timestamp"
    );
    assert_eq!(
        days.iter().map(|day| day.duration_ms).sum::<u64>(),
        176_400_000,
        "the corrupt turn duration is clamped to the 49-hour thread interval"
    );
}

#[test]
fn activity_days_do_not_expand_an_implausible_extreme_thread_span() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('extreme-thread','Extreme thread',
                    '0001-01-01T00:00:00.000000000Z',
                    '9999-12-31T23:59:59.999999999Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('extreme-thread','extreme-thread',
                    '0001-01-01T00:00:00.000000000Z',
                    '9999-12-31T23:59:59.999999999Z',0);
             INSERT INTO turns(
                id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
             ) VALUES(
                'extreme-turn','extreme-thread','extreme-thread',
                '0001-01-01T00:00:00.000000000Z',
                '9999-12-31T23:59:59.999999999Z','completed',1000
             );",
        )
        .unwrap();

    let days = query_activity_day_summaries_batched(&connection, "extreme-thread").unwrap();
    assert_eq!(
        days.len(),
        1,
        "one corrupt interval manufactured extra days"
    );
    assert_eq!(days[0].date, "0001-01-01");
    assert_eq!(days[0].duration_ms, 0);
}

#[test]
fn activity_tool_day_aggregation_preserves_cross_midnight_occupancy() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    let first_date = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
    let second_date = first_date.succ_opt().unwrap();
    let midnight = local_midnight(second_date);
    let thread_start = (midnight - chrono::Duration::minutes(2)).to_rfc3339();
    let thread_end = (midnight + chrono::Duration::minutes(2)).to_rfc3339();
    let tool_start = (midnight - chrono::Duration::seconds(1)).to_rfc3339();

    for (thread_id, tool_end, expected_dates) in [
        (
            "tool-crosses-midnight",
            (midnight + chrono::Duration::seconds(1)).to_rfc3339(),
            vec![second_date.to_string(), first_date.to_string()],
        ),
        (
            "tool-ends-at-midnight",
            midnight.to_rfc3339(),
            vec![first_date.to_string()],
        ),
    ] {
        connection
            .execute(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES(?1,'Tool day boundary',?2,?3)",
                params![thread_id, thread_start, thread_end],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES(?1,?1,?2,?3,0)",
                params![thread_id, thread_start, thread_end],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,started_at,completed_at,
                    namespace,name,status,duration_ms
                 ) VALUES(?1,?2,?3,?3,?4,?5,'functions','exec','completed',2000)",
                params![
                    format!("{thread_id}-tool"),
                    format!("{thread_id}-call"),
                    thread_id,
                    tool_start,
                    tool_end
                ],
            )
            .unwrap();

        let days = query_activity_day_summaries_batched(&connection, thread_id).unwrap();
        assert_eq!(
            days.iter().map(|day| day.date.clone()).collect::<Vec<_>>(),
            expected_dates
        );
    }
}

#[test]
fn activity_day_window_stops_at_the_representable_date_limit() {
    assert!(activity_day_window(NaiveDate::MAX).is_none());
}

#[test]
fn activity_queries_have_constant_statement_budgets() {
    let _trace_guard = TRACE_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let mut connection = db.connect().unwrap();
    seed_activity_roots(&connection, "activity-budget", 12);
    connection.trace(Some(count_query));

    QUERY_COUNT.store(0, Ordering::SeqCst);
    let one = query_activity_on(&connection, "activity-budget", 1, 1).unwrap();
    assert_eq!(one.items.len(), 1);
    let one_count = QUERY_COUNT.swap(0, Ordering::SeqCst);

    let all = query_activity_on(&connection, "activity-budget", 1, 12).unwrap();
    assert_eq!(all.items.len(), 12);
    let all_count = QUERY_COUNT.swap(0, Ordering::SeqCst);
    assert_eq!(all_count, one_count, "page size must not amplify SQL");
    // Fixed-point rollup repricing adds five bounded statements: two ledger
    // reads for day totals, two for exchange totals, and one sparse
    // NULL-turn usage probe alongside the rollup query it replaces. Indexed
    // occupied-date seeking keeps turn intervals and point activity in two
    // separate statements. The equality above is the essential guard:
    // neither page size nor raw usage-fact count may amplify the budget.
    assert!(
        all_count <= 19,
        "collapsed Activity used {all_count} SELECTs"
    );

    seed_activity_descendants(&connection, "activity-budget", 0, 1);
    QUERY_COUNT.store(0, Ordering::SeqCst);
    let detail = query_activity_detail_on(&connection, "activity-budget", "root-0")
        .unwrap()
        .unwrap();
    assert_eq!(detail.counts.unwrap().agent_runs, 1);
    let one_descendant_count = QUERY_COUNT.swap(0, Ordering::SeqCst);

    connection.trace(None);
    seed_activity_descendants(&connection, "activity-budget", 1, 11);
    connection.trace(Some(count_query));
    let detail = query_activity_detail_on(&connection, "activity-budget", "root-0")
        .unwrap()
        .unwrap();
    assert_eq!(detail.counts.unwrap().agent_runs, 12);
    let many_descendant_count = QUERY_COUNT.swap(0, Ordering::SeqCst);
    assert_eq!(
        many_descendant_count, one_descendant_count,
        "expanded detail must not issue SQL per descendant"
    );
    // The canonical child projection uses one bounded COUNT plus one
    // indexed page seek. Descendant attribution, group metadata, labels,
    // and interval-union duration add four set-based/streamed statements;
    // none varies with descendant count. The equality above is the guard
    // against the old per-descendant query path.
    assert!(
        many_descendant_count <= 21,
        "expanded Activity used {many_descendant_count} SELECTs"
    );
    connection.trace(None);
}
