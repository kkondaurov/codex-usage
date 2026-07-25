#![cfg(test)]

use super::*;

#[test]
fn optimized_price_book_honors_layer_precedence_before_effective_date() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO model_prices(
                model_id,effective_from,effective_to,
                input_microusd_per_million,cached_input_microusd_per_million,
                output_microusd_per_million,currency,source
             ) VALUES
                ('layered-model','2026-01-01T00:00:00.000000000Z',NULL,
                 9000000,9000000,9000000,'USD','remote:https://example.test/prices'),
                ('layered-model','1970-01-01T00:00:00.000000000Z',NULL,
                 3000000,3000000,3000000,'USD','manual');",
        )
        .unwrap();

    let price_book = load_price_book_on(&connection).unwrap();
    let (_, selected) = price_book
        .price_at("layered-model", "2026-07-15T12:00:00.000000000Z")
        .unwrap();
    assert_eq!(selected.cost_numerator(1, 0, 0), 3_000_000);
}

#[test]
fn activity_usage_cost_converts_fixed_point_only_after_attribution() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO model_prices(
                model_id,effective_from,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,
                currency,source
             ) VALUES(
                'decimal-attribution','1970-01-01T00:00:00.000000000Z',
                1000000000,1000000000,1000000000,'USD','manual'
             );
             INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('exact-activity','Exact activity',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('exact-activity','exact-activity',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:01:00.000000000Z',0);
             INSERT INTO agent_runs(
                id,thread_id,rollout_id,nickname,started_at,status
             ) VALUES(
                'exact-agent','exact-activity','exact-activity','Exact agent',
                '2026-07-01T00:00:00.000000000Z','completed'
             );
             INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,status
             ) VALUES(
                'exact-turn','exact-activity','exact-activity','exact-agent',
                '2026-07-01T00:00:00.000000000Z','completed'
             );
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
             ) VALUES(
                'exact-owner','exact-activity','exact-activity','exact-turn',
                '2026-07-01T00:00:10.000000000Z',10,
                'assistant','assistant','Exact response',1
             );
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<9
             )
             INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
             )
             SELECT printf('exact-usage-%02d',value),'exact-activity','exact-activity',
                    'exact-turn','exact-agent','2026-07-01T00:00:11.000000000Z',11,
                    'decimal-attribution',100,0,0,0,100,1
             FROM sequence;",
        )
        .unwrap();

    let list_item =
        query_activity_child_previews_page(&connection, "exact-activity", "exact-turn", 1, 25)
            .unwrap()
            .items
            .into_iter()
            .find(|item| item.id == "exact-owner")
            .unwrap();
    let list_usage = list_item.usage.unwrap();
    assert_eq!(list_usage.total_tokens, 1_000);
    assert_eq!(list_usage.cost_usd.unwrap().decimal_string(), "1.00");

    let detail_usage = query_activity_detail_on(&connection, "exact-activity", "exact-owner")
        .unwrap()
        .unwrap()
        .usage
        .unwrap();
    assert_eq!(detail_usage.total_tokens, 1_000);
    assert_eq!(detail_usage.cost_usd.unwrap().decimal_string(), "1.00");

    let totals = read_totals_on(
        &connection,
        None,
        None,
        TotalsScope::Thread {
            thread_id: "exact-activity",
        },
    )
    .unwrap();
    assert_eq!(totals.known_cost_numerator, 1_000_000_000_000);
    let summary = read_summary_on(&connection, "exact-activity")
        .unwrap()
        .expect("fixture session summary exists");
    assert_eq!(summary.models[0].cost_usd.unwrap().decimal_string(), "1.00");
    assert_eq!(summary.agents[0].cost_usd.unwrap().decimal_string(), "1.00");
}

#[test]
#[ignore = "100k-descendant performance regression; run explicitly with --ignored --nocapture"]
fn activity_hundred_thousand_descendants_stay_sql_backed_and_page_bounded() {
    const DESCENDANTS: u64 = 100_000;
    const REVIEWS: u64 = DESCENDANTS / 10;
    const AGENT_TURNS: u64 = DESCENDANTS - REVIEWS;
    const NON_REVIEW_AGENTS: u64 = 90;
    const REGRESSION_BUDGET: StdDuration = StdDuration::from_secs(3);

    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('descendant-scale','Descendant scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('descendant-scale','descendant-scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('descendant-scale-root','descendant-scale','descendant-scale',
                    '2026-07-01T00:00:00.000000000Z','completed');
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,
                kind,role,body,native
             ) VALUES(
                'descendant-scale-user','descendant-scale','descendant-scale',
                'descendant-scale-root','2026-07-01T00:00:00.000000000Z',1,
                'user','user','Scale request',1);
             INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,
                started_at,last_event_at,archived
             ) VALUES(
                'descendant-scale-agent','descendant-scale','descendant-scale',
                'descendant-scale','2026-07-01T00:00:01.000000000Z',
                '2026-07-01T00:10:00.000000000Z',0);
             WITH RECURSIVE agents(value) AS (
                SELECT 0 UNION ALL SELECT value+1 FROM agents WHERE value+1<100
             )
             INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,status
             )
             SELECT printf('descendant-scale-agent-%03d',value),
                    'descendant-scale','descendant-scale-agent','descendant-scale',
                    printf('Scale agent %03d',value),
                    '2026-07-01T00:00:01.000000000Z','completed'
             FROM agents;
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,
                kind,payload_json,native
             ) VALUES(
                'descendant-scale-spawn','descendant-scale','descendant-scale',
                'descendant-scale-root','2026-07-01T00:00:01.000000000Z',2,
                'subagent','{\"agent_thread_id\":\"descendant-scale-agent-000\"}',1);
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL
                SELECT value+1 FROM sequence WHERE value+1<100000
             )
             INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,status,
                model,duration_ms,last_agent_message
             )
             SELECT printf('descendant-scale-turn-%06d',value),
                    'descendant-scale','descendant-scale-agent',
                    printf('descendant-scale-agent-%03d',value%100),
                    '2026-07-01T00:01:00.000000000Z',
                    CASE WHEN value=1 THEN 'running'
                         WHEN value=10 THEN 'failed' ELSE 'completed' END,
                    CASE WHEN value%10=0 THEN 'codex-auto-review' ELSE 'gpt-5.5' END,
                    1000,
                    CASE WHEN value>=99992
                         THEN printf('Descendant %d',value) ELSE x'80' END
             FROM sequence;
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL
                SELECT value+1 FROM sequence WHERE value+1<100000
             )
             INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                model,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
             )
             SELECT printf('descendant-scale-usage-%06d',value),
                    'descendant-scale','descendant-scale-agent',
                    printf('descendant-scale-turn-%06d',value),
                    printf('descendant-scale-agent-%03d',value%100),
                    '2026-07-01T00:01:01.000000000Z',
                    value+3,
                    CASE WHEN value%10=0 THEN 'codex-auto-review' ELSE 'gpt-5.5' END,
                    2,1,3,1,5,1
             FROM sequence;",
        )
        .unwrap();

    let started = Instant::now();
    let list = query_activity_on(&connection, "descendant-scale", 1, 1).unwrap();
    let list_elapsed = started.elapsed();
    assert_eq!(list.items.len(), 1);
    let item = &list.items[0];
    assert_eq!(item.counts.as_ref().unwrap().agent_runs, NON_REVIEW_AGENTS);
    assert_eq!(item.counts.as_ref().unwrap().reviews, REVIEWS);
    assert_eq!(item.usage.as_ref().unwrap().total_tokens, DESCENDANTS * 5);

    let started = Instant::now();
    let detail = query_activity_detail_page_on(
        &connection,
        "descendant-scale",
        "descendant-scale-root",
        1,
        1,
    )
    .unwrap()
    .unwrap();
    let detail_elapsed = started.elapsed();
    assert!(
        detail.children.len() <= 3,
        "root detail materialized descendants"
    );
    assert!(serde_json::to_vec(&detail).unwrap().len() < 32 * 1024);
    let agent_group = detail
        .children
        .iter()
        .find(|child| child.kind == "agent_group")
        .unwrap();
    assert_eq!(agent_group.child_total, Some(AGENT_TURNS));
    assert_eq!(agent_group.status.as_deref(), Some("running"));
    assert_eq!(agent_group.duration_ms, Some(1000));
    let labels = agent_group.body.as_deref().unwrap();
    assert!(labels.contains("Scale agent 099"));
    assert!(labels.ends_with("+82 more"), "{labels}");
    assert_eq!(
        agent_group.usage.as_ref().unwrap().total_tokens,
        AGENT_TURNS * 5
    );
    let review_group = detail
        .children
        .iter()
        .find(|child| child.kind == "review_group")
        .unwrap();
    assert_eq!(review_group.child_total, Some(REVIEWS));
    assert_eq!(review_group.status.as_deref(), Some("attention"));
    assert_eq!(review_group.duration_ms, Some(1000));
    assert_eq!(
        review_group.usage.as_ref().unwrap().total_tokens,
        REVIEWS * 5
    );

    let started = Instant::now();
    let first_group_page = query_activity_detail_page_on(
        &connection,
        "descendant-scale",
        "group:agents:descendant-scale-root",
        1,
        7,
    )
    .unwrap()
    .unwrap();
    let group_elapsed = started.elapsed();
    assert_eq!(first_group_page.child_total, Some(AGENT_TURNS));
    assert_eq!(first_group_page.children.len(), 7);
    assert_eq!(first_group_page.child_has_more, Some(true));
    assert!(first_group_page.child_next_cursor.is_some());
    assert_eq!(
        first_group_page.children[0].body.as_deref(),
        Some("Descendant 99999")
    );

    eprintln!(
        "Activity 100k descendants: list={list_elapsed:?}, root detail={detail_elapsed:?}, \
         group page={group_elapsed:?}; budget={REGRESSION_BUDGET:?}"
    );
    for (name, elapsed) in [
        ("list", list_elapsed),
        ("root detail", detail_elapsed),
        ("group page", group_elapsed),
    ] {
        assert!(
            elapsed <= REGRESSION_BUDGET,
            "100k-descendant Activity {name} regressed: {elapsed:?}"
        );
    }
}

#[test]
#[ignore = "performance benchmark; run explicitly with --ignored --nocapture"]
fn activity_large_session_query_and_assembly_stays_within_regression_budget() {
    const TOOL_EVENTS: u64 = 500_000;
    const SAMPLES: usize = 3;
    const REGRESSION_BUDGET: StdDuration = StdDuration::from_millis(2_500);

    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('activity-scale','Activity scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('activity-scale','activity-scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO turns(
                id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
             ) VALUES(
                'activity-scale-turn','activity-scale','activity-scale',
                '2026-07-01T00:00:00.000000000Z',
                '2026-07-01T00:10:00.000000000Z','completed',600000
             );
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
             ) VALUES(
                'activity-scale-user','activity-scale','activity-scale',
                'activity-scale-turn','2026-07-01T00:00:00.000000000Z',
                1,'user','user','Benchmark request',1
             );
             WITH RECURSIVE sequence(value) AS (
                SELECT 0
                UNION ALL
                SELECT value + 1 FROM sequence WHERE value + 1 < 500000
             )
             INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,turn_id,started_at,completed_at,
                namespace,name,status,duration_ms
             )
             SELECT
                printf('activity-scale-tool-%06d',value),
                printf('activity-scale-call-%06d',value),
                'activity-scale','activity-scale','activity-scale-turn',
                '2026-07-01T00:00:01.000000000Z',
                '2026-07-01T00:00:01.001000000Z',
                'functions',
                CASE value % 4
                    WHEN 0 THEN 'exec'
                    WHEN 1 THEN 'apply_patch'
                    WHEN 2 THEN 'node_repl'
                    ELSE 'browser'
                END,
                'completed',1
             FROM sequence;
             WITH RECURSIVE sequence(value) AS (
                SELECT 0
                UNION ALL
                SELECT value + 1 FROM sequence WHERE value + 1 < 500000
             )
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,
                kind,call_id,native
             )
             SELECT
                printf('activity-scale-event-%06d',value),
                'activity-scale','activity-scale','activity-scale-turn',
                '2026-07-01T00:00:01.000000000Z',value + 2,
                'tool_call',printf('activity-scale-call-%06d',value),1
             FROM sequence;",
        )
        .unwrap();
    drop(connection);

    let mut list_samples = Vec::with_capacity(SAMPLES);
    let mut detail_samples = Vec::with_capacity(SAMPLES);
    let mut numeric_deep_samples = Vec::with_capacity(SAMPLES);
    let mut cursor_deep_samples = Vec::with_capacity(SAMPLES);
    for sample in 1..=SAMPLES {
        // A new connection exercises per-request statement preparation and temporary
        // Activity tables while leaving fixture creation outside the measurement.
        let connection = db.connect().unwrap();
        let started = Instant::now();
        let response = query_activity_on(&connection, "activity-scale", 1, 1).unwrap();
        let encoded = serde_json::to_vec(&response).unwrap();
        let list_elapsed = started.elapsed();

        assert!(!encoded.is_empty());
        assert_eq!(response.items.len(), 1);
        assert_eq!(
            response.items[0].counts.as_ref().unwrap().tool_calls,
            TOOL_EVENTS
        );
        eprintln!("Activity 500k combined list sample {sample}: {list_elapsed:?}");
        list_samples.push(list_elapsed);

        let started = Instant::now();
        let detail = query_activity_detail_page_on(
            &connection,
            "activity-scale",
            "activity-scale-turn",
            1,
            1,
        )
        .unwrap()
        .unwrap();
        let encoded = serde_json::to_vec(&detail).unwrap();
        let detail_elapsed = started.elapsed();
        assert!(!encoded.is_empty());
        assert_eq!(detail.child_total, Some(TOOL_EVENTS + 1));
        assert_eq!(detail.children.len(), 1);
        assert_eq!(
            detail.children[0].id, "activity-scale-event-499999",
            "the first detail page must use canonical descending index order"
        );
        assert!(detail.child_next_cursor.is_some());
        eprintln!("Activity 500k combined detail sample {sample}: {detail_elapsed:?}");
        detail_samples.push(detail_elapsed);

        let started = Instant::now();
        let numeric_deep = query_activity_detail_page_on(
            &connection,
            "activity-scale",
            "activity-scale-turn",
            TOOL_EVENTS,
            1,
        )
        .unwrap()
        .unwrap();
        let encoded = serde_json::to_vec(&numeric_deep).unwrap();
        let numeric_deep_elapsed = started.elapsed();
        assert!(!encoded.is_empty());
        assert_eq!(numeric_deep.child_page, Some(TOOL_EVENTS));
        assert_eq!(numeric_deep.child_total, Some(TOOL_EVENTS + 1));
        assert_eq!(numeric_deep.children.len(), 1);
        assert_eq!(
            numeric_deep.children[0].id, "activity-scale-event-000000",
            "the compatibility OFFSET path must still reach the deep canonical row"
        );
        let deep_cursor = numeric_deep
            .child_next_cursor
            .clone()
            .expect("the penultimate detail row must expose a cursor");
        eprintln!("Activity 500k deep numeric sample {sample}: {numeric_deep_elapsed:?}");
        numeric_deep_samples.push(numeric_deep_elapsed);

        let started = Instant::now();
        let cursor_deep = query_activity_detail_cursor_page_on(
            &connection,
            "activity-scale",
            "activity-scale-turn",
            TOOL_EVENTS + 1,
            1,
            Some(&deep_cursor),
        )
        .unwrap()
        .unwrap();
        let encoded = serde_json::to_vec(&cursor_deep).unwrap();
        let cursor_deep_elapsed = started.elapsed();
        assert!(!encoded.is_empty());
        assert_eq!(cursor_deep.child_page, Some(TOOL_EVENTS + 1));
        assert_eq!(cursor_deep.child_total, Some(TOOL_EVENTS + 1));
        assert_eq!(cursor_deep.children.len(), 1);
        assert_eq!(
            cursor_deep.children[0].id, "activity-scale-user",
            "the deep keyset seek must continue exactly after its scoped cursor"
        );
        assert_eq!(cursor_deep.child_has_more, Some(false));
        assert!(cursor_deep.child_next_cursor.is_none());
        eprintln!("Activity 500k deep cursor sample {sample}: {cursor_deep_elapsed:?}");
        cursor_deep_samples.push(cursor_deep_elapsed);
    }

    list_samples.sort_unstable();
    detail_samples.sort_unstable();
    numeric_deep_samples.sort_unstable();
    cursor_deep_samples.sort_unstable();
    let list_median = list_samples[SAMPLES / 2];
    let list_slowest = list_samples[SAMPLES - 1];
    let detail_median = detail_samples[SAMPLES / 2];
    let detail_slowest = detail_samples[SAMPLES - 1];
    let numeric_deep_median = numeric_deep_samples[SAMPLES / 2];
    let numeric_deep_slowest = numeric_deep_samples[SAMPLES - 1];
    let cursor_deep_median = cursor_deep_samples[SAMPLES / 2];
    let cursor_deep_slowest = cursor_deep_samples[SAMPLES - 1];
    eprintln!(
        "Activity 500k combined: list median={list_median:?}, slowest={list_slowest:?}; \
         first detail median={detail_median:?}, slowest={detail_slowest:?}; \
         deep numeric median={numeric_deep_median:?}, slowest={numeric_deep_slowest:?}; \
         deep cursor median={cursor_deep_median:?}, slowest={cursor_deep_slowest:?}; \
         budget={REGRESSION_BUDGET:?}"
    );
    assert!(
        list_slowest <= REGRESSION_BUDGET,
        "500k combined Activity list regressed: median={list_median:?}, \
         slowest={list_slowest:?}, budget={REGRESSION_BUDGET:?}"
    );
    assert!(
        detail_slowest <= REGRESSION_BUDGET,
        "500k combined Activity detail regressed: median={detail_median:?}, \
         slowest={detail_slowest:?}, budget={REGRESSION_BUDGET:?}"
    );
    assert!(
        numeric_deep_slowest <= REGRESSION_BUDGET,
        "500k combined Activity deep numeric fallback regressed: \
         median={numeric_deep_median:?}, slowest={numeric_deep_slowest:?}, \
         budget={REGRESSION_BUDGET:?}"
    );
    assert!(
        cursor_deep_slowest <= REGRESSION_BUDGET,
        "500k combined Activity deep cursor seek regressed: \
         median={cursor_deep_median:?}, slowest={cursor_deep_slowest:?}, \
         budget={REGRESSION_BUDGET:?}"
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "release-mode performance gate; run with cargo test --release -- --ignored"
)]
fn activity_usage_heavy_queries_stay_under_one_second() {
    const USAGE_FACTS: u64 = 500_000;
    const BUDGET: StdDuration = StdDuration::from_secs(1);
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('activity-usage-scale','Activity usage scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('activity-usage-scale','activity-usage-scale',
                    '2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T00:10:00.000000000Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('activity-usage-turn','activity-usage-scale','activity-usage-scale',
                    '2026-07-01T00:00:00.000000000Z','completed');
             INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
             ) VALUES('activity-usage-owner','activity-usage-scale','activity-usage-scale',
                      'activity-usage-turn','2026-07-01T00:00:01.000000000Z',1,
                      'assistant','assistant','Done',1);
             WITH RECURSIVE sequence(value) AS (
                SELECT 0 UNION ALL
                SELECT value+1 FROM sequence WHERE value+1<500000
             )
             INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
             )
             SELECT printf('activity-usage-%06d',value),
                    'activity-usage-scale','activity-usage-scale','activity-usage-turn',
                    '2026-07-01T00:00:02.000000000Z',value+2,'gpt-5.5',
                    1,0,1,0,2,1
             FROM sequence;",
        )
        .unwrap();
    drop(connection);

    for sample in 1..=3 {
        let connection = db.connect().unwrap();
        let started = Instant::now();
        let list = query_activity_on(&connection, "activity-usage-scale", 1, 1).unwrap();
        let list_elapsed = started.elapsed();
        assert_eq!(
            list.items[0].usage.as_ref().unwrap().total_tokens,
            USAGE_FACTS * 2
        );

        let started = Instant::now();
        let detail = query_activity_detail_page_on(
            &connection,
            "activity-usage-scale",
            "activity-usage-turn",
            1,
            1,
        )
        .unwrap()
        .unwrap();
        let detail_elapsed = started.elapsed();
        assert_eq!(detail.usage.as_ref().unwrap().total_tokens, USAGE_FACTS * 2);
        assert!(
            list_elapsed < BUDGET && detail_elapsed < BUDGET,
            "usage-heavy Activity sample {sample} exceeded {BUDGET:?}: \
             list={list_elapsed:?}, detail={detail_elapsed:?}"
        );
    }
}

#[test]
fn activity_batch_excludes_descendants_of_roots_outside_the_selected_page() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let connection = db.connect().unwrap();
    seed_activity_roots(&connection, "activity-page-scope", 12);
    seed_activity_descendants(&connection, "activity-page-scope", 0, 11);

    let page = query_activity_on(&connection, "activity-page-scope", 1, 1).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, "root-11");
    assert_eq!(page.items[0].counts.as_ref().unwrap().agent_runs, 0);

    let selected_roots: i64 = connection
        .query_row("SELECT COUNT(*) FROM selected_activity_roots", [], |row| {
            row.get(0)
        })
        .unwrap();
    let selected_turns: i64 = connection
        .query_row("SELECT COUNT(*) FROM selected_activity_turns", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(selected_roots, 1);
    assert_eq!(selected_turns, 1);
}
