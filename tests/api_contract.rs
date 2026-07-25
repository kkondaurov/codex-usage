use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode},
    response::IntoResponse,
    routing::get,
};
use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, SecondsFormat, TimeZone, Utc,
};
use codex_usage::{
    app::AppDependencies,
    config::PricingConfig,
    ingest::{IngestRoots, scan_once},
    pricing::{ManualPricingStore, PricingSync},
    storage::{Db, StorageExecutor},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tempfile::TempDir;
use tower::ServiceExt;

const RICH_SESSION: &str = "019f6768-ef84-74d3-ab05-e4b5fb717fa8";
const ABORTED_SESSION: &str = "019f6767-979c-7df1-a512-9830528bda62";
const GUARDIAN_SESSION: &str = "019ee21b-697b-7090-b865-2a7acf43e3fc";
const JULY_REPLAY_SESSION: &str = "019f64aa-21e8-7a41-916f-0fe9b845eede";
const MAY_SESSION: &str = "019df47e-62a3-7ba3-a57f-d7f8565ec08f";
const LEGACY_SESSION: &str = "8a90fef4-e12d-41b3-8ebd-8281d05c653c";

struct ApiHarness {
    _temp: TempDir,
    app: Router,
    db: Db,
    roots: IngestRoots,
}

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/corpus")
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

fn exact_fixture_prices(db: &Db) {
    let connection = db.connect().unwrap();
    connection
        .execute(
            "DELETE FROM model_prices WHERE model_id IN ('gpt-5.5','gpt-5.6-sol','codex-auto-review')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM model_aliases WHERE observed_model_id='codex-auto-review'",
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

fn harness() -> ApiHarness {
    harness_with_pricing_url("http://127.0.0.1:9/prices.json".to_string())
}

fn harness_with_pricing_url(pricing_url: String) -> ApiHarness {
    let temp = tempfile::tempdir().unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    for case in ["replay_spike", "rich_trace", "legacy_v0", "sparse_pricing"] {
        let source = corpus_root().join(case).join("active");
        if source.is_dir() {
            copy_tree(&source, &active.join(case));
        }
        let source = corpus_root().join(case).join("archived");
        if source.is_dir() {
            copy_tree(&source, &archive.join(case));
        }
    }
    let frontend = temp.path().join("frontend");
    fs::create_dir_all(frontend.join("assets")).unwrap();
    fs::write(
        frontend.join("index.html"),
        "<!doctype html><title>Codex Usage fixture</title><main>SPA contract</main>",
    )
    .unwrap();
    fs::write(frontend.join("assets/app.js"), "window.fixtureApp = true").unwrap();

    let db = Db::open(temp.path().join("data/codex-usage.db")).unwrap();
    let manual_pricing = ManualPricingStore::new(db.path().with_extension("pricing.json")).unwrap();
    manual_pricing.hydrate(&db).unwrap();
    exact_fixture_prices(&db);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };
    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_seen, 9);
    assert_eq!(report.files_failed, 0);
    let executor = StorageExecutor::default();
    let pricing_sync = PricingSync::new(executor.clone());
    let app = AppDependencies::new(
        db.clone(),
        roots.clone(),
        frontend,
        PricingConfig {
            url: pricing_url,
            refresh_interval_hours: 24,
            timeout_seconds: 2,
        },
        executor,
        pricing_sync,
        manual_pricing,
    )
    .build();
    ApiHarness {
        _temp: temp,
        app,
        db,
        roots,
    }
}

async fn pricing_fixture_server() -> (String, tokio::task::JoinHandle<()>) {
    let body = json!({
        "openai/gpt-5.5": {
            "input_cost_per_token": 0.000005,
            "cache_read_input_token_cost": 0.0000005,
            "output_cost_per_token": 0.000030
        },
        "openai/gpt-5.6-sol": {
            "input_cost_per_token": 0.000005,
            "cache_read_input_token_cost": 0.0000005,
            "output_cost_per_token": 0.000030
        }
    });
    let app = Router::new().route(
        "/prices.json",
        get(move || {
            let body = body.clone();
            async move { Json(body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}/prices.json"), handle)
}

async fn flaky_pricing_fixture_server() -> (String, tokio::task::JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let body = json!({
        "openai/gpt-5.5": {
            "input_cost_per_token": 0.000005,
            "cache_read_input_token_cost": 0.0000005,
            "output_cost_per_token": 0.000030
        }
    });
    let app = Router::new().route(
        "/prices.json",
        get(move || {
            let calls = calls.clone();
            let body = body.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error":"temporary fixture failure"})),
                    )
                        .into_response()
                } else {
                    Json(body).into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}/prices.json"), handle)
}

async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, HeaderMap, Bytes) {
    let body = body
        .map(|value| Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap_or_else(Body::empty);
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "127.0.0.1")
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, headers, body)
}

async fn get_json(app: &Router, uri: &str) -> Value {
    let (status, headers, body) = raw_request(app, Method::GET, uri, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri}: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap()),
        Some("application/json")
    );
    serde_json::from_slice(&body).unwrap()
}

fn json_object_subset(value: &Value, keys: &[&str]) -> Value {
    Value::Object(
        keys.iter()
            .map(|key| ((*key).to_owned(), value[*key].clone()))
            .collect(),
    )
}

fn assert_exact_json_keys(value: &Value, expected: &[&str]) {
    let object = value.as_object().expect("expected a JSON object");
    let actual = object.keys().map(String::as_str).collect::<HashSet<_>>();
    let expected = expected.iter().copied().collect::<HashSet<_>>();
    assert_eq!(actual, expected);
    assert_eq!(object.len(), expected.len());
}

fn canonical_totals(value: &Value) -> Value {
    json_object_subset(
        value,
        &[
            "inputTokens",
            "cachedInputTokens",
            "outputTokens",
            "reasoningTokens",
            "blendedTokens",
            "totalTokens",
            "costUsd",
            "unpricedTokens",
            "pricingComplete",
        ],
    )
}

fn canonical_usage_summary(value: &Value) -> Value {
    json_object_subset(
        value,
        &[
            "totalTokens",
            "costUsd",
            "unpricedTokens",
            "pricingComplete",
        ],
    )
}

fn canonical_session_row(value: &Value) -> Value {
    json_object_subset(
        value,
        &[
            "id",
            "rootThreadId",
            "startedAt",
            "lastEventAt",
            "title",
            "project",
            "branch",
            "status",
            "messageCount",
            "turnCount",
            "agentCount",
            "toolCount",
            "totalTokens",
            "costUsd",
            "unpricedTokens",
            "lifetimeCostUsd",
            "lifetimeUnpricedTokens",
        ],
    )
}

fn canonical_session_listing_row(value: &Value) -> Value {
    json_object_subset(
        value,
        &[
            "id",
            "lastEventAt",
            "messageCount",
            "totalTokens",
            "costUsd",
            "unpricedTokens",
        ],
    )
}

fn canonical_session_page(value: &Value) -> Value {
    json!({
        "page": value["page"],
        "pageSize": value["pageSize"],
        "total": value["total"],
        "totalPages": value["totalPages"],
        "items": value["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(canonical_session_listing_row)
            .collect::<Vec<_>>(),
    })
}

fn canonical_activity_item(value: &Value) -> Value {
    let mut item = serde_json::Map::new();
    for key in [
        "id",
        "turnId",
        "agentLabel",
        "timestamp",
        "kind",
        "role",
        "label",
        "body",
        "status",
        "toolName",
        "durationMs",
        "model",
        "hasDetails",
        "childPage",
        "childPageSize",
        "childTotal",
        "childHasMore",
        "childNextCursor",
        "counts",
    ] {
        if !value[key].is_null() {
            item.insert(key.to_owned(), value[key].clone());
        }
    }
    if !value["usage"].is_null() {
        item.insert("usage".to_owned(), canonical_usage_summary(&value["usage"]));
    }
    if let Some(children) = value["children"].as_array()
        && !children.is_empty()
    {
        item.insert(
            "children".to_owned(),
            Value::Array(children.iter().map(canonical_activity_item).collect()),
        );
    }
    Value::Object(item)
}

fn canonical_activity_page(value: &Value) -> Value {
    json!({
        "page": value["page"],
        "pageSize": value["pageSize"],
        "total": value["total"],
        "totalPages": value["totalPages"],
        "days": value["days"].as_array().unwrap().iter().map(|day| json!({
            "date": day["date"],
            "durationMs": day["durationMs"],
            "totals": canonical_usage_summary(&day["totals"]),
        })).collect::<Vec<_>>(),
        "items": value["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(canonical_activity_item)
            .collect::<Vec<_>>(),
    })
}

fn assert_activity_item_wire_keys(value: &Value, pagination_keys: &[&str]) {
    let mut keys = vec![
        "id",
        "turnId",
        "rolloutId",
        "agentRunId",
        "agentLabel",
        "timestamp",
        "kind",
        "role",
        "label",
        "body",
        "status",
        "toolName",
        "durationMs",
        "model",
        "effort",
        "hasDetails",
        "children",
        "usage",
        "counts",
    ];
    keys.extend_from_slice(pagination_keys);
    assert_exact_json_keys(value, &keys);
    assert!(value["children"].is_array());
    if !value["usage"].is_null() {
        assert_exact_json_keys(
            &value["usage"],
            &[
                "inputTokens",
                "cachedInputTokens",
                "outputTokens",
                "reasoningTokens",
                "blendedTokens",
                "totalTokens",
                "costUsd",
                "unpricedTokens",
                "pricingComplete",
            ],
        );
    }
    if !value["counts"].is_null() {
        assert_exact_json_keys(
            &value["counts"],
            &[
                "modelCalls",
                "toolCalls",
                "agentRuns",
                "reviews",
                "followUps",
            ],
        );
    }
}

fn canonical_stats_row(value: &Value, range: &str) -> Value {
    let bucket = if range == "day" {
        DateTime::parse_from_rfc3339(value["periodStart"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc)
            .format("%H:%MZ")
            .to_string()
    } else {
        value["label"].as_str().unwrap().to_owned()
    };
    json!({
        "bucket": bucket,
        "sessionCount": value["sessionCount"],
        "totals": canonical_usage_summary(value),
    })
}

fn canonical_stats(value: &Value) -> Value {
    let range = value["range"].as_str().unwrap();
    let rows = value["rows"].as_array().unwrap();
    let active_rows = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row["sessionCount"].as_u64().unwrap() > 0 || row["totalTokens"].as_u64().unwrap() > 0
        })
        .map(|(index, row)| {
            json!({
                "row": canonical_stats_row(row, range),
                "trend": value["trend"][index],
            })
        })
        .collect::<Vec<_>>();
    let mut snapshot = json!({
        "range": value["range"],
        "anchor": value["anchor"],
        "label": value["label"],
        "totals": canonical_totals(&value["totals"]),
        "rowCount": rows.len(),
        "activeRows": active_rows,
        "trendCount": value["trend"].as_array().unwrap().len(),
    });
    if value["range"] == "all" {
        snapshot["anchor"] = json!("$TODAY");
        snapshot["rowCount"] = json!("$DATA_THROUGH_CURRENT_YEAR");
        snapshot["trendCount"] = json!("$MATCHES_ROW_COUNT");
    }
    snapshot
}

fn query_component(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn canonical_overview_contract(value: &Value) -> Value {
    let periods = ["today", "week", "month"]
        .into_iter()
        .map(|period| {
            let value = &value["periods"][period];
            (
                period.to_owned(),
                json!({
                    "label": value["label"],
                    "startIsTimestamp": DateTime::parse_from_rfc3339(value["start"].as_str().unwrap()).is_ok(),
                    "endIsTimestamp": DateTime::parse_from_rfc3339(value["end"].as_str().unwrap()).is_ok(),
                    "sessionCountIsNumber": value["sessionCount"].is_number(),
                    "messageCountIsNumber": value["messageCount"].is_number(),
                    "totalsKeys": value["totals"].as_object().unwrap().keys().collect::<Vec<_>>(),
                    "deltaCostIsNullableUsd": value["deltaCostUsd"].is_null() || value["deltaCostUsd"].is_string(),
                    "deltaPercentIsNullableNumber": value["deltaPercent"].is_null() || value["deltaPercent"].is_number(),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "updatedAtIsTimestamp": value["updatedAt"]
            .as_str()
            .is_some_and(|timestamp| DateTime::parse_from_rfc3339(timestamp).is_ok()),
        "periods": periods,
    })
}

fn assert_number(value: &Value, key: &str) {
    assert!(value[key].is_number(), "{key} must be a number in {value}");
}

fn usd_units(value: &Value) -> i128 {
    let text = value
        .as_str()
        .unwrap_or_else(|| panic!("USD amount must be a decimal string, got {value}"));
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |unsigned| (true, unsigned));
    let (whole, fraction) = unsigned
        .split_once('.')
        .unwrap_or_else(|| panic!("USD amount must include a decimal point: {text}"));
    assert!(!whole.is_empty(), "USD amount has no whole part: {text}");
    assert!(
        (2..=12).contains(&fraction.len()),
        "USD amount must have 2 to 12 decimal places: {text}"
    );
    assert!(
        fraction.len() == 2 || !fraction.ends_with('0'),
        "USD amount is not canonical: {text}"
    );
    let whole = whole
        .parse::<i128>()
        .unwrap_or_else(|_| panic!("invalid USD whole part: {text}"));
    let fraction_digits = fraction.len() as u32;
    let fraction = fraction
        .parse::<i128>()
        .unwrap_or_else(|_| panic!("invalid USD fraction: {text}"));
    let magnitude = whole
        .checked_mul(1_000_000_000_000)
        .and_then(|whole| whole.checked_add(fraction * 10_i128.pow(12 - fraction_digits)))
        .unwrap_or_else(|| panic!("USD amount is out of range: {text}"));
    if negative { -magnitude } else { magnitude }
}

fn assert_usd(value: &Value, expected: &str) {
    assert_eq!(
        value.as_str(),
        Some(expected),
        "expected exact USD decimal string {expected}, got {value}"
    );
    let _ = usd_units(value);
}

fn assert_nullable_usd(value: &Value, key: &str) {
    if !value[key].is_null() {
        let _ = usd_units(&value[key]);
    }
}

fn assert_totals(value: &Value) {
    for key in [
        "inputTokens",
        "cachedInputTokens",
        "outputTokens",
        "reasoningTokens",
        "blendedTokens",
        "totalTokens",
        "unpricedTokens",
    ] {
        assert_number(value, key);
    }
    assert_nullable_usd(value, "costUsd");
    assert!(value["pricingComplete"].is_boolean());
}

fn assert_session_row(value: &Value) {
    for key in [
        "id",
        "rootThreadId",
        "startedAt",
        "lastEventAt",
        "title",
        "project",
    ] {
        assert!(value[key].is_string(), "{key} must be a string in {value}");
    }
    assert!(value["branch"].is_string() || value["branch"].is_null());
    for key in [
        "messageCount",
        "turnCount",
        "agentCount",
        "toolCount",
        "totalTokens",
        "unpricedTokens",
        "lifetimeUnpricedTokens",
    ] {
        assert_number(value, key);
    }
    assert_nullable_usd(value, "costUsd");
    assert_nullable_usd(value, "lifetimeCostUsd");
}

fn test_local_midnight(date: NaiveDate) -> DateTime<Utc> {
    let naive = date.and_hms_opt(0, 0, 0).unwrap();
    match Local.from_local_datetime(&naive) {
        LocalResult::Single(value) => value.with_timezone(&Utc),
        LocalResult::Ambiguous(first, _) => first.with_timezone(&Utc),
        LocalResult::None => Local
            .from_local_datetime(&(naive + Duration::hours(1)))
            .earliest()
            .unwrap()
            .with_timezone(&Utc),
    }
}

fn local_transition_day(year: i32) -> NaiveDate {
    let mut date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let limit = NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap();
    while date < limit {
        let next = date + Duration::days(1);
        if test_local_midnight(next) - test_local_midnight(date) != Duration::hours(24) {
            return date;
        }
        date = next;
    }
    NaiveDate::from_ymd_opt(year, 1, 2).unwrap()
}

#[tokio::test]
async fn api_surface_matches_the_reviewed_characterization_oracle() {
    let harness = harness();

    let status = get_json(&harness.app, "/api/v1/status").await;
    let overview = get_json(&harness.app, "/api/v1/overview").await;
    let overview_year = get_json(&harness.app, "/api/v1/overview/year?year=2026").await;
    let active_heatmap = overview_year["heatmap"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|day| {
            day["sessionCount"].as_u64().unwrap() > 0
                || day["messageCount"].as_u64().unwrap() > 0
                || day["totalTokens"].as_u64().unwrap() > 0
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut stats = serde_json::Map::new();
    for (range, anchor) in [
        ("day", "2026-07-15"),
        ("week", "2026-07-15"),
        ("month", "2026-07-15"),
        ("year", "2026-07-15"),
    ] {
        let response = get_json(
            &harness.app,
            &format!("/api/v1/stats?range={range}&anchor={anchor}"),
        )
        .await;
        stats.insert(range.to_owned(), canonical_stats(&response));
    }
    let stats_all = get_json(&harness.app, "/api/v1/stats?range=all").await;
    stats.insert("all".to_owned(), canonical_stats(&stats_all));

    let recent_first = get_json(
        &harness.app,
        "/api/v1/sessions?sort=recent&page=1&pageSize=2",
    )
    .await;
    let recent_second = get_json(
        &harness.app,
        "/api/v1/sessions?sort=recent&page=2&pageSize=2",
    )
    .await;
    let cost_first = get_json(&harness.app, "/api/v1/sessions?sort=cost&page=1&pageSize=3").await;
    let searched = get_json(
        &harness.app,
        &format!("/api/v1/sessions?q={RICH_SESSION}&pageSize=50"),
    )
    .await;
    let project = get_json(
        &harness.app,
        "/api/v1/sessions?project=codex-dashboard&pageSize=50",
    )
    .await;
    let date = get_json(&harness.app, "/api/v1/sessions?date=2026-07-15&pageSize=50").await;
    let bounded = get_json(
        &harness.app,
        "/api/v1/sessions?start=2026-07-15T20%3A13%3A30Z&end=2026-07-15T20%3A13%3A35Z&pageSize=50",
    )
    .await;
    let projects = get_json(&harness.app, "/api/v1/projects").await;

    let summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/summary"),
    )
    .await;

    let activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/activity?page=1&pageSize=25"),
    )
    .await;
    let activity_id = activity["items"][0]["id"].as_str().unwrap();
    let detail_first = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/{activity_id}?childPage=1&childPageSize=3"
        ),
    )
    .await;
    let child_cursor = detail_first["childNextCursor"]
        .as_str()
        .expect("the rich fixture must exercise Activity cursor pagination");
    let detail_cursor = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/{activity_id}?childPageSize=3&childCursor={}",
            query_component(child_cursor)
        ),
    )
    .await;

    let replay_activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/activity?page=1&pageSize=25"),
    )
    .await;
    let replay_id = replay_activity["items"][0]["id"].as_str().unwrap();
    let replay_detail = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{replay_id}"),
    )
    .await;
    let group = replay_detail["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["kind"]
                .as_str()
                .is_some_and(|kind| kind.ends_with("_group"))
        })
        .expect("the replay fixture must expose a synthetic Activity group");
    let group_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{}?childPage=1&childPageSize=1",
            group["id"].as_str().unwrap()
        ),
    )
    .await;

    let settings = get_json(&harness.app, "/api/v1/settings").await;
    let prices = get_json(&harness.app, "/api/v1/prices?q=gpt-5.5&page=1&pageSize=10").await;
    let price_metadata = get_json(&harness.app, "/api/v1/prices/metadata?unknownLimit=10").await;
    let model_ids = get_json(&harness.app, "/api/v1/prices/model-ids?q=gpt-5.5&limit=10").await;
    let aliases = get_json(&harness.app, "/api/v1/aliases?page=1&pageSize=10").await;

    let (missing_status, missing_headers, missing_body) = raw_request(
        &harness.app,
        Method::GET,
        "/api/v1/sessions/missing/summary",
        None,
    )
    .await;
    let (unknown_status, unknown_headers, unknown_body) =
        raw_request(&harness.app, Method::GET, "/api/v1/not-a-route", None).await;
    let (method_status, method_headers, method_body) =
        raw_request(&harness.app, Method::POST, "/api/v1/status", None).await;
    let (query_status, query_headers, query_body) = raw_request(
        &harness.app,
        Method::GET,
        "/api/v1/stats?range=quarter",
        None,
    )
    .await;

    let route_contract = |status: StatusCode, headers: HeaderMap, body: Bytes| {
        json!({
            "status": status.as_u16(),
            "contentType": headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.split(';').next().unwrap()),
            "allow": headers.get("allow").and_then(|value| value.to_str().ok()),
            "body": serde_json::from_slice::<Value>(&body).unwrap(),
        })
    };

    let actual = json!({
        "system": {
            "status": {
                "state": status["state"],
                "filesScanned": status["filesScanned"],
                "filesFailed": status["filesFailed"],
                "lastIngestAtIsTimestamp": status["lastIngestAt"]
                    .as_str()
                    .is_some_and(|value| DateTime::parse_from_rfc3339(value).is_ok()),
                "lastIngestAttemptAtIsTimestamp": status["lastIngestAttemptAt"]
                    .as_str()
                    .is_some_and(|value| DateTime::parse_from_rfc3339(value).is_ok()),
                "lastEventAt": status["lastEventAt"],
            },
            "settings": {
                "databasePath": "$DATABASE",
                "activeRoot": "$ACTIVE_ROOT",
                "archiveRoot": "$ARCHIVE_ROOT",
                "timezoneIsNonempty": settings["timezone"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "lastIngestAtIsTimestamp": settings["lastIngestAt"]
                    .as_str()
                    .is_some_and(|value| DateTime::parse_from_rfc3339(value).is_ok()),
                "sessionCount": settings["sessionCount"],
                "databaseBytesIsPositive": settings["databaseBytes"].as_u64().unwrap() > 0,
                "pricing": settings["pricing"],
            },
        },
        "routes": {
            "missingSession": route_contract(missing_status, missing_headers, missing_body),
            "unknownApi": route_contract(unknown_status, unknown_headers, unknown_body),
            "wrongMethod": route_contract(method_status, method_headers, method_body),
            "invalidQuery": route_contract(query_status, query_headers, query_body),
        },
        "analytics": {
            "overviewContract": canonical_overview_contract(&overview),
            "year": {
                "year": overview_year["year"],
                "heatmapCount": overview_year["heatmap"].as_array().unwrap().len(),
                "activeHeatmap": active_heatmap,
                "topProjects": overview_year["topProjects"],
                "topSessions": overview_year["topSessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(canonical_session_listing_row)
                    .collect::<Vec<_>>(),
            },
            "stats": stats,
        },
        "sessions": {
            "projects": projects,
            "recentFirst": canonical_session_page(&recent_first),
            "recentSecond": canonical_session_page(&recent_second),
            "costFirst": canonical_session_page(&cost_first),
            "search": canonical_session_page(&searched),
            "project": canonical_session_page(&project),
            "date": canonical_session_page(&date),
            "bounded": canonical_session_page(&bounded),
            "summary": {
                "session": canonical_session_row(&summary["session"]),
                "cwd": summary["session"]["cwd"],
                "source": summary["session"]["source"],
                "firstPrompt": summary["session"]["firstPrompt"],
                "latestResult": summary["session"]["latestResult"],
                "completedAt": summary["session"]["completedAt"],
                "totals": canonical_totals(&summary["totals"]),
                "models": summary["models"],
                "agents": summary["agents"],
                "toolSummary": summary["toolSummary"],
            },
        },
        "activity": {
            "root": canonical_activity_page(&activity),
            "detailFirst": canonical_activity_item(&detail_first),
            "detailCursor": canonical_activity_item(&detail_cursor),
            "group": canonical_activity_item(&group_detail),
        },
        "pricing": {
            "prices": prices,
            "metadata": price_metadata,
            "modelIds": model_ids,
            "aliases": aliases,
        },
    });

    let domains = [
        (
            "api_surface",
            json!({"system": actual["system"], "routes": actual["routes"]}),
            include_str!("fixtures/api_oracle/api_surface.json"),
        ),
        (
            "analytics",
            actual["analytics"].clone(),
            include_str!("fixtures/api_oracle/analytics.json"),
        ),
        (
            "sessions",
            actual["sessions"].clone(),
            include_str!("fixtures/api_oracle/sessions.json"),
        ),
        (
            "activity",
            actual["activity"].clone(),
            include_str!("fixtures/api_oracle/activity.json"),
        ),
        (
            "pricing",
            actual["pricing"].clone(),
            include_str!("fixtures/api_oracle/pricing.json"),
        ),
    ];

    for (name, actual, expected) in domains {
        let expected: Value = serde_json::from_str(expected).unwrap();
        assert_eq!(
            actual, expected,
            "the {name} API changed; review the diff rather than blindly regenerating the oracle"
        );
    }
}

#[tokio::test]
async fn top_level_endpoints_match_the_frontend_contract_and_real_fixture_totals() {
    let harness = harness();

    let status = get_json(&harness.app, "/api/v1/status").await;
    assert_eq!(status["state"], "idle");
    assert_eq!(status["filesScanned"], 9);
    assert_eq!(status["filesFailed"], 0);
    assert!(status["lastIngestAt"].is_string());
    assert!(status["lastIngestAttemptAt"].is_string());
    assert!(status["lastEventAt"].is_string());

    let overview = get_json(&harness.app, "/api/v1/overview").await;
    let overview_object = overview.as_object().unwrap();
    assert_eq!(overview_object.len(), 2);
    assert!(overview_object.contains_key("updatedAt"));
    assert!(overview_object.contains_key("periods"));
    for period in ["today", "week", "month"] {
        assert!(overview["periods"][period]["sessionCount"].is_number());
        assert!(overview["periods"][period]["messageCount"].is_number());
        assert_totals(&overview["periods"][period]["totals"]);
        assert_nullable_usd(&overview["periods"][period], "deltaCostUsd");
        assert!(
            overview["periods"][period]["deltaPercent"].is_number()
                || overview["periods"][period]["deltaPercent"].is_null()
        );
        if overview["periods"][period]["totals"]["unpricedTokens"]
            .as_u64()
            .unwrap()
            > 0
        {
            assert!(overview["periods"][period]["totals"]["costUsd"].is_null());
        }
    }

    let overview_year = get_json(&harness.app, "/api/v1/overview/year?year=2026").await;
    let year_object = overview_year.as_object().unwrap();
    assert_eq!(year_object.len(), 4);
    for key in ["year", "heatmap", "topProjects", "topSessions"] {
        assert!(
            year_object.contains_key(key),
            "missing {key} in {overview_year}"
        );
    }
    assert_eq!(overview_year["year"], 2026);
    assert_eq!(overview_year["heatmap"].as_array().unwrap().len(), 365);
    for day in overview_year["heatmap"].as_array().unwrap() {
        assert_nullable_usd(day, "costUsd");
    }
    let july_15 = overview_year["heatmap"]
        .as_array()
        .unwrap()
        .iter()
        .find(|day| day["date"] == "2026-07-15")
        .unwrap();
    assert_eq!(july_15["sessionCount"], 4);
    assert_eq!(july_15["totalTokens"], 387_708);
    assert!(july_15["messageCount"].as_u64().unwrap() >= 1);
    assert!(
        july_15["costUsd"].is_null(),
        "a heatmap day with any unpriced usage carries no separate completeness field"
    );
    let top_projects = overview_year["topProjects"].as_array().unwrap();
    assert_eq!(top_projects.len(), 3);
    let mut saw_unpriced_project = false;
    let mut previous_project_cost = i128::MAX;
    for project in top_projects {
        if !project["costUsd"].is_null() {
            let cost = usd_units(&project["costUsd"]);
            assert!(!saw_unpriced_project);
            assert!(project["share"].is_number());
            assert!(cost <= previous_project_cost);
            previous_project_cost = cost;
        } else {
            saw_unpriced_project = true;
            assert!(project["share"].is_null());
        }
    }
    let top_sessions = overview_year["topSessions"].as_array().unwrap();
    assert_eq!(top_sessions.len(), 3);
    let mut saw_unpriced_session = false;
    let mut previous_session_cost = i128::MAX;
    let mut previous_unpriced_tokens = u64::MAX;
    for row in top_sessions {
        assert_session_row(row);
        assert!(row["lastEventAt"].as_str().unwrap().starts_with("2026-"));
        if row["unpricedTokens"].as_u64().unwrap() == 0 {
            assert!(!saw_unpriced_session);
            let cost = usd_units(&row["costUsd"]);
            assert!(cost <= previous_session_cost);
            previous_session_cost = cost;
        } else {
            saw_unpriced_session = true;
            assert!(row["costUsd"].is_null());
            let total_tokens = row["totalTokens"].as_u64().unwrap();
            assert!(total_tokens <= previous_unpriced_tokens);
            previous_unpriced_tokens = total_tokens;
        }
    }

    let sessions = get_json(
        &harness.app,
        "/api/v1/sessions?sort=recent&page=1&pageSize=2",
    )
    .await;
    assert_eq!(sessions["total"], 6);
    assert_eq!(sessions["page"], 1);
    assert_eq!(sessions["pageSize"], 2);
    assert_eq!(sessions["totalPages"], 3);
    assert_eq!(sessions["items"].as_array().unwrap().len(), 2);
    assert!(sessions["projects"].as_array().unwrap().len() >= 3);
    for row in sessions["items"].as_array().unwrap() {
        assert_session_row(row);
        if row["unpricedTokens"].as_u64().unwrap() > 0 {
            assert!(row["costUsd"].is_null());
        }
    }
    let projects = get_json(&harness.app, "/api/v1/projects").await;
    assert_eq!(projects["items"], sessions["projects"]);
    let all = get_json(&harness.app, "/api/v1/sessions?page=1&pageSize=50").await;
    for row in all["items"].as_array().unwrap() {
        assert_eq!(row["costUsd"], row["lifetimeCostUsd"]);
        assert_eq!(row["unpricedTokens"], row["lifetimeUnpricedTokens"]);
    }
    let ids = all["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(!ids.contains(&"019f64af-12fe-7170-8fcd-7d636000a8af"));
    assert!(!ids.contains(&"019f64af-6612-79d0-81d0-c53d7d6caef0"));

    let settings = get_json(&harness.app, "/api/v1/settings").await;
    assert_eq!(
        settings["databasePath"],
        harness.db.path().display().to_string()
    );
    assert_eq!(
        settings["activeRoot"],
        harness.roots.active.as_ref().unwrap().display().to_string()
    );
    assert_eq!(
        settings["archiveRoot"],
        harness
            .roots
            .archive
            .as_ref()
            .unwrap()
            .display()
            .to_string()
    );
    assert_eq!(settings["sessionCount"], 6);
    assert!(settings["databaseBytes"].as_u64().unwrap() > 0);
    assert!(settings["timezone"].is_string());
    assert!(settings["lastIngestAt"].is_string());
    let _ = usd_units(&settings["pricing"]["knownCostUsd"]);
    assert_eq!(settings["pricing"]["complete"], false);
    assert_eq!(settings["pricing"]["unpricedTokens"], 25_607);
}

#[tokio::test]
async fn status_prefers_a_complete_last_scan_report_over_source_file_counts() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "UPDATE source_files SET last_error='stored source error'",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT OR REPLACE INTO app_meta(key,value) VALUES('last_scan_report',?1)",
            [json!({
                "filesSeen": 123,
                "filesIngested": 80,
                "filesUnchanged": 40,
                "filesFailed": 3,
                "recordsRead": 456,
                "inheritedRecordsSkipped": 7,
            })
            .to_string()],
        )
        .unwrap();
    drop(connection);

    let status = get_json(&harness.app, "/api/v1/status").await;

    assert_eq!(status["filesScanned"], 123);
    assert_eq!(status["filesFailed"], 3);
}

#[tokio::test]
async fn status_falls_back_to_source_files_for_malformed_or_incomplete_scan_reports() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute("UPDATE source_files SET last_error=NULL", [])
        .unwrap();
    connection
        .execute(
            "UPDATE source_files SET last_error='stored source error'
             WHERE rowid=(SELECT rowid FROM source_files ORDER BY path LIMIT 1)",
            [],
        )
        .unwrap();
    let source_file_count: u64 = connection
        .query_row("SELECT COUNT(*) FROM source_files", [], |row| row.get(0))
        .unwrap();

    for report in [
        "{".to_owned(),
        json!({"filesSeen": 50, "filesFailed": 9}).to_string(),
    ] {
        connection
            .execute(
                "INSERT OR REPLACE INTO app_meta(key,value) VALUES('last_scan_report',?1)",
                [&report],
            )
            .unwrap();

        let status = get_json(&harness.app, "/api/v1/status").await;
        assert_eq!(status["filesScanned"], source_file_count);
        assert_eq!(status["filesFailed"], 1);
    }
}

#[tokio::test]
async fn heatmap_uses_gapless_local_day_bounds_across_dst_transitions() {
    let harness = harness();
    let year = 2026;
    let date = local_transition_day(year);
    let next_date = date + Duration::days(1);
    let start = test_local_midnight(date);
    let end = test_local_midnight(next_date);
    let next_end = test_local_midnight(next_date + Duration::days(1));
    let timestamps = [
        (start + Duration::milliseconds(1)).to_rfc3339_opts(SecondsFormat::Millis, true),
        (end - Duration::milliseconds(1)).to_rfc3339_opts(SecondsFormat::Millis, true),
        end.to_rfc3339_opts(SecondsFormat::Millis, true),
    ];
    assert_eq!(end, test_local_midnight(next_date));
    assert!(next_end > end);

    let connection = harness.db.connect().unwrap();
    for table in ["usage_facts", "events", "messages"] {
        connection
            .execute(
                &format!("UPDATE {table} SET timestamp='1971-01-01T00:00:00Z'"),
                [],
            )
            .unwrap();
    }
    let rollout_id: String = connection
        .query_row(
            "SELECT id FROM rollouts WHERE thread_id=?1 ORDER BY started_at LIMIT 1",
            [RICH_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    for (index, (timestamp, tokens)) in timestamps.iter().zip([11_i64, 13, 17]).enumerate() {
        connection
            .execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    model,effort,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(?1,?2,?3,NULL,NULL,?4,?5,'gpt-5.5',NULL,?6,0,0,0,?6,1)",
                rusqlite::params![
                    format!("dst-usage-{index}"),
                    RICH_SESSION,
                    rollout_id,
                    timestamp,
                    900_000 + index as i64,
                    tokens
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
                 ) VALUES(?1,?2,?3,NULL,?4,'user','DST boundary fixture',?5)",
                rusqlite::params![
                    format!("dst-message-{index}"),
                    RICH_SESSION,
                    rollout_id,
                    timestamp,
                    900_000 + index as i64
                ],
            )
            .unwrap();
    }
    drop(connection);

    let response = get_json(&harness.app, &format!("/api/v1/overview/year?year={year}")).await;
    let rows = response["heatmap"].as_array().unwrap();
    let day = rows
        .iter()
        .find(|row| row["date"] == date.to_string())
        .unwrap();
    let following = rows
        .iter()
        .find(|row| row["date"] == next_date.to_string())
        .unwrap();
    assert_eq!(day["totalTokens"], 24);
    assert_eq!(day["messageCount"], 2);
    assert_eq!(day["sessionCount"], 1);
    assert_eq!(following["totalTokens"], 17);
    assert_eq!(following["messageCount"], 1);
    assert_eq!(following["sessionCount"], 1);
}

#[tokio::test]
async fn overview_top_sessions_use_period_activity_for_cross_year_threads() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    for table in ["usage_facts", "events", "messages"] {
        connection
            .execute(
                &format!("UPDATE {table} SET timestamp='1971-01-01T00:00:00Z'"),
                [],
            )
            .unwrap();
    }
    connection
        .execute(
            "UPDATE threads SET started_at='2025-12-20T12:00:00Z',
                                last_event_at='2027-03-01T12:00:00Z'
             WHERE id=?1",
            [RICH_SESSION],
        )
        .unwrap();
    let rollout_id: String = connection
        .query_row(
            "SELECT id FROM rollouts WHERE thread_id=?1 ORDER BY started_at LIMIT 1",
            [RICH_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "UPDATE agent_runs SET started_at='2027-02-01T12:00:00Z'
             WHERE thread_id=?1 AND id<>thread_id",
            [RICH_SESSION],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO agent_runs(id,thread_id,rollout_id,started_at,status)
             VALUES('cross-year-agent-inside',?1,?2,'2026-06-01T12:00:00Z','completed'),
                   ('cross-year-agent-outside',?1,?2,'2027-06-01T12:00:00Z','completed')",
            rusqlite::params![RICH_SESSION, rollout_id],
        )
        .unwrap();
    for (id, timestamp, output_tokens) in [
        ("cross-year-inside", "2026-02-01T12:00:00Z", 10_000_000_i64),
        ("cross-year-outside", "2027-02-01T12:00:00Z", 20_000_000_i64),
    ] {
        connection
            .execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    model,effort,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(?1,?2,?3,NULL,NULL,?4,910000,'gpt-5.5',NULL,0,0,?5,0,?5,1)",
                rusqlite::params![id, RICH_SESSION, rollout_id, timestamp, output_tokens],
            )
            .unwrap();
    }
    for (id, timestamp) in [
        ("cross-year-message-inside", "2026-11-01T12:00:00Z"),
        ("cross-year-message-outside", "2027-03-01T12:00:00Z"),
    ] {
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
                 ) VALUES(?1,?2,?3,NULL,?4,'user','Cross-year fixture',910001)",
                rusqlite::params![id, RICH_SESSION, rollout_id, timestamp],
            )
            .unwrap();
    }
    drop(connection);

    let response = get_json(&harness.app, "/api/v1/overview/year?year=2026").await;
    let row = &response["topSessions"][0];
    assert_eq!(row["id"], RICH_SESSION);
    assert_eq!(row["startedAt"], "2025-12-20T12:00:00Z");
    assert_eq!(row["lastEventAt"], "2026-11-01T12:00:00Z");
    assert_eq!(row["messageCount"], 1);
    assert_eq!(row["agentCount"], 1);
    assert_eq!(row["totalTokens"], 10_000_000);
    assert!(
        usd_units(&row["lifetimeCostUsd"]) > usd_units(&row["costUsd"]),
        "usage outside the selected year belongs only to lifetime cost"
    );
}

#[tokio::test]
async fn overview_top_session_keeps_its_independent_sixteen_field_contract() {
    const THREAD: &str = "analytics-top-shape";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,project,branch,started_at,last_event_at)
            VALUES(
                '{THREAD}','Analytics top shape','stage-three','feature/top-session',
                '2042-01-01T10:00:00Z','2042-12-31T23:59:59Z'
            );
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES(
                '{THREAD}','{THREAD}','2042-01-01T10:00:00Z',
                '2042-01-01T10:05:00Z',0
            );
            INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
            VALUES(
                'analytics-top-turn','{THREAD}','{THREAD}',
                '2042-01-01T10:00:00Z','completed'
            );
            INSERT INTO messages(
                id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
            ) VALUES
                ('analytics-top-message-1','{THREAD}','{THREAD}','analytics-top-turn',
                 '2042-01-01T10:01:00Z','user','First',1),
                ('analytics-top-message-2','{THREAD}','{THREAD}','analytics-top-turn',
                 '2042-01-01T10:02:00Z','assistant','Second',2);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,native
            ) VALUES(
                'analytics-top-event','{THREAD}','{THREAD}','analytics-top-turn',
                '2042-01-01T10:03:00Z',3,'state',1
            );
            INSERT INTO agent_runs(id,thread_id,rollout_id,started_at,status)
            VALUES(
                'analytics-top-agent','{THREAD}','{THREAD}',
                '2042-01-01T10:03:30Z','completed'
            );
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,turn_id,started_at,name,status,duration_ms
            ) VALUES(
                'analytics-top-tool','analytics-top-call','{THREAD}','{THREAD}',
                'analytics-top-turn','2042-01-01T10:04:00Z','exec','completed',10
            );
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                total_tokens,native
            ) VALUES(
                'analytics-top-usage','{THREAD}','{THREAD}','analytics-top-turn',
                '2042-01-01T10:05:00Z',4,'stage-three-unpriced',100,20,23,5,123,1
            );
            "#
        ))
        .unwrap();
    drop(connection);

    let response = get_json(&harness.app, "/api/v1/overview/year?year=2042").await;
    let sessions = response["topSessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    let row = &sessions[0];
    assert_eq!(row.as_object().unwrap().len(), 16);
    assert_eq!(
        row,
        &json!({
            "id": THREAD,
            "rootThreadId": THREAD,
            "startedAt": "2042-01-01T10:00:00Z",
            "lastEventAt": "2042-01-01T10:05:00Z",
            "title": "Analytics top shape",
            "project": "stage-three",
            "branch": "feature/top-session",
            "messageCount": 2,
            "turnCount": 1,
            "agentCount": 1,
            "toolCount": 1,
            "totalTokens": 123,
            "costUsd": null,
            "unpricedTokens": 123,
            "lifetimeCostUsd": null,
            "lifetimeUnpricedTokens": 123
        })
    );
}

#[tokio::test]
async fn sessions_catalog_visibility_projects_and_summary_share_one_contract() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            r#"
            INSERT INTO threads(id,title,project,started_at,last_event_at) VALUES
                ('catalog-event','Event only','zuluCatalog','2045-01-01T00:00:00Z','2045-01-01T00:01:00Z'),
                ('catalog-message','Message only','alphaCatalog','2045-01-01T00:00:00Z','2045-01-01T00:02:00Z'),
                ('catalog-usage','Usage only','BetaCatalog','2045-01-01T00:00:00Z','2045-01-01T00:03:00Z'),
                ('catalog-empty','Empty','hiddenEmptyCatalog','2045-01-01T00:00:00Z','2045-01-01T00:04:00Z'),
                ('catalog-tool','Tool only','hiddenToolCatalog','2045-01-01T00:00:00Z','2045-01-01T00:05:00Z'),
                ('catalog-turn','Turn only','hiddenTurnCatalog','2045-01-01T00:00:00Z','2045-01-01T00:06:00Z'),
                ('catalog-blank-project','Blank project','','2045-01-01T00:00:00Z','2045-01-01T00:07:00Z'),
                ('catalog-null-project','Null project',NULL,'2045-01-01T00:00:00Z','2045-01-01T00:08:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('catalog-event','catalog-event','2045-01-01T00:00:00Z','2045-01-01T00:01:00Z',0),
                ('catalog-message','catalog-message','2045-01-01T00:00:00Z','2045-01-01T00:02:00Z',0),
                ('catalog-usage','catalog-usage','2045-01-01T00:00:00Z','2045-01-01T00:03:00Z',0),
                ('catalog-empty','catalog-empty','2045-01-01T00:00:00Z','2045-01-01T00:04:00Z',0),
                ('catalog-tool','catalog-tool','2045-01-01T00:00:00Z','2045-01-01T00:05:00Z',0),
                ('catalog-turn','catalog-turn','2045-01-01T00:00:00Z','2045-01-01T00:06:00Z',0),
                ('catalog-blank-project','catalog-blank-project','2045-01-01T00:00:00Z','2045-01-01T00:07:00Z',0),
                ('catalog-null-project','catalog-null-project','2045-01-01T00:00:00Z','2045-01-01T00:08:00Z',0);
            INSERT INTO events(
                id,thread_id,rollout_id,timestamp,source_line,kind,body,native
            ) VALUES
                ('catalog-event-row','catalog-event','catalog-event',
                 '2045-01-01T00:01:00Z',1,'state','visible',1),
                ('catalog-blank-project-row','catalog-blank-project','catalog-blank-project',
                 '2045-01-01T00:07:00Z',1,'state','visible',1);
            INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
            ) VALUES
                ('catalog-message-row','catalog-message','catalog-message',
                 '2045-01-01T00:02:00Z','user','visible',1),
                ('catalog-null-project-row','catalog-null-project','catalog-null-project',
                 '2045-01-01T00:08:00Z','user','visible',1);
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
            ) VALUES(
                'catalog-usage-row','catalog-usage','catalog-usage',
                '2045-01-01T00:03:00Z',1,'catalog-unpriced',1,0,0,0,1,1
            );
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,started_at,name,status
            ) VALUES(
                'catalog-tool-row','catalog-tool-call','catalog-tool','catalog-tool',
                '2045-01-01T00:05:00Z','exec','completed'
            );
            INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
            VALUES(
                'catalog-turn-row','catalog-turn','catalog-turn',
                '2045-01-01T00:06:00Z','completed'
            );
            "#,
        )
        .unwrap();
    drop(connection);

    let sessions = get_json(&harness.app, "/api/v1/sessions?page=1&pageSize=200").await;
    let injected_rows = sessions["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["id"].as_str().unwrap().starts_with("catalog-"))
        .collect::<Vec<_>>();
    assert_eq!(
        injected_rows
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "catalog-blank-project",
            "catalog-event",
            "catalog-message",
            "catalog-null-project",
            "catalog-usage",
        ]
        .into_iter()
        .collect()
    );
    for row in injected_rows {
        assert_session_row(row);
        assert_eq!(
            row.as_object().unwrap().len(),
            16,
            "row shape drifted: {row}"
        );
    }

    let projects = get_json(&harness.app, "/api/v1/projects").await;
    let injected_projects = projects["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .filter(|project| project.to_ascii_lowercase().contains("catalog"))
        .collect::<Vec<_>>();
    assert_eq!(
        injected_projects,
        vec!["alphaCatalog", "BetaCatalog", "zuluCatalog"]
    );
    assert!(!projects["items"].as_array().unwrap().contains(&json!("")));

    for visible_id in ["catalog-event", "catalog-message", "catalog-usage"] {
        let (status, _, body) = raw_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/sessions/{visible_id}/summary"),
            None,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "visible summary {visible_id} failed: {}",
            String::from_utf8_lossy(&body)
        );
        let summary: Value = serde_json::from_slice(&body).unwrap();
        assert_session_row(&summary["session"]);
        assert_eq!(summary["session"]["id"], visible_id);
    }

    for hidden_id in ["catalog-empty", "catalog-tool", "catalog-turn"] {
        let (status, _, body) = raw_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/sessions/{hidden_id}/summary"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{hidden_id} became visible");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"error": "session not found"})
        );
    }
}

#[tokio::test]
async fn sessions_support_search_project_sort_pagination_and_exact_date_drilldown() {
    let harness = harness();

    let search = get_json(&harness.app, "/api/v1/sessions?q=giant%20usage&pageSize=50").await;
    assert_eq!(search["total"], 1);
    assert_eq!(search["items"][0]["id"], ABORTED_SESSION);

    let project = get_json(
        &harness.app,
        "/api/v1/sessions?project=codex-dashboard&pageSize=50",
    )
    .await;
    assert_eq!(project["total"], 2);
    assert!(
        project["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| { row["project"] == "codex-dashboard" })
    );

    let july = get_json(&harness.app, "/api/v1/sessions?date=2026-07-15&pageSize=50").await;
    assert_eq!(july["total"], 4);
    let inclusive_dates = get_json(
        &harness.app,
        "/api/v1/sessions?start=2026-07-15&end=2026-07-15&pageSize=50",
    )
    .await;
    assert_eq!(inclusive_dates["total"], 4);

    let partial_session = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions?q={RICH_SESSION}&start=2026-07-15T20%3A13%3A30Z&end=2026-07-15T20%3A13%3A35Z&pageSize=50"
        ),
    )
    .await;
    assert_eq!(partial_session["total"], 1);
    let partial = &partial_session["items"][0];
    assert_session_row(partial);
    assert_eq!(partial["unpricedTokens"], 0);
    assert_eq!(partial["lifetimeUnpricedTokens"], 0);
    assert!(
        usd_units(&partial["lifetimeCostUsd"]) > usd_units(&partial["costUsd"]),
        "the filtered cost must remain distinct from the session's lifetime cost"
    );

    let connection = harness.db.connect().unwrap();
    let rollout_id: String = connection
        .query_row(
            "SELECT id FROM rollouts WHERE thread_id=?1 ORDER BY started_at LIMIT 1",
            [RICH_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "UPDATE agent_runs SET started_at='2025-01-01T00:00:00Z'
             WHERE thread_id=?1 AND id<>thread_id",
            [RICH_SESSION],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO agent_runs(id,thread_id,rollout_id,started_at,status)
             VALUES('bounded-agent-inside',?1,?2,'2026-07-15T20:30:00Z','completed'),
                   ('bounded-agent-outside',?1,?2,'2025-07-15T20:30:00Z','completed')",
            rusqlite::params![RICH_SESSION, rollout_id],
        )
        .unwrap();
    drop(connection);
    let bounded_agents = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions?q={RICH_SESSION}&start=2026-07-15T20%3A00%3A00Z&end=2026-07-15T21%3A00%3A00Z&pageSize=50"
        ),
    )
    .await;
    assert_eq!(bounded_agents["items"][0]["agentCount"], 1);
    let lifetime_agents = get_json(
        &harness.app,
        &format!("/api/v1/sessions?q={RICH_SESSION}&pageSize=50"),
    )
    .await;
    assert!(lifetime_agents["items"][0]["agentCount"].as_u64().unwrap() > 1);

    let sorted = get_json(&harness.app, "/api/v1/sessions?sort=cost&page=1&pageSize=2").await;
    assert_eq!(sorted["items"][0]["id"], JULY_REPLAY_SESSION);
    assert!(usd_units(&sorted["items"][0]["costUsd"]) >= usd_units(&sorted["items"][1]["costUsd"]));

    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE threads SET project='all' WHERE id=?1",
            [RICH_SESSION],
        )
        .unwrap();
    let literal_all = get_json(&harness.app, "/api/v1/sessions?project=all&pageSize=50").await;
    assert_eq!(literal_all["total"], 1);
    assert_eq!(literal_all["items"][0]["id"], RICH_SESSION);

    let (status, headers, body) = raw_request(
        &harness.app,
        Method::GET,
        "/api/v1/sessions?sort=surprise",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "sort must be recent or cost"
    );

    let empty_page = get_json(&harness.app, "/api/v1/sessions?page=999&pageSize=2").await;
    assert_eq!(empty_page["total"], 6);
    assert!(empty_page["items"].as_array().unwrap().is_empty());

    let stats = get_json(&harness.app, "/api/v1/stats?range=day&anchor=2026-07-15").await;
    let two_session_hour = stats["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["sessionCount"] == 2)
        .expect("rich and aborted sessions share an hour");
    let drilldown_uri = format!(
        "/api/v1/sessions?start={}&end={}&pageSize=50",
        two_session_hour["periodStart"]
            .as_str()
            .unwrap()
            .replace('+', "%2B"),
        two_session_hour["periodEnd"]
            .as_str()
            .unwrap()
            .replace('+', "%2B")
    );
    let drilldown = get_json(&harness.app, &drilldown_uri).await;
    assert_eq!(drilldown["total"], 2);
    let drilldown_ids = drilldown["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(drilldown_ids.contains(&RICH_SESSION));
    assert!(drilldown_ids.contains(&ABORTED_SESSION));
}

#[tokio::test]
async fn session_boundaries_preserve_offset_and_bare_date_semantics() {
    let harness = harness();
    let utc = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions?q={RICH_SESSION}&start=2026-07-15T20%3A13%3A30Z&end=2026-07-15T20%3A13%3A35Z&pageSize=50"
        ),
    )
    .await;
    let offset = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions?q={RICH_SESSION}&start=2026-07-15T22%3A13%3A30%2B02%3A00&end=2026-07-15T22%3A13%3A35%2B02%3A00&pageSize=50"
        ),
    )
    .await;
    assert_eq!(offset, utc);

    let date = get_json(&harness.app, "/api/v1/sessions?date=2026-07-15&pageSize=50").await;
    let inclusive_range = get_json(
        &harness.app,
        "/api/v1/sessions?start=2026-07-15&end=2026-07-15&pageSize=50",
    )
    .await;
    assert_eq!(inclusive_range, date);

    for (uri, expected) in [
        (
            "/api/v1/sessions?date=2026-07-15&start=2026-07-15",
            "date cannot be combined with start or end",
        ),
        (
            "/api/v1/sessions?start=2026-07-16&end=2026-07-15",
            "start must be before end",
        ),
    ] {
        let (status, _, body) = raw_request(&harness.app, Method::GET, uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["error"],
            expected
        );
    }
}

#[tokio::test]
async fn conversation_display_policy_matches_sessions_and_activity() {
    let harness = harness();
    let cases = [
        (
            "display-explicit-request",
            "Explicit request fallback",
            r#"# Applications mentioned by the user:

<appshot>Captured text containing an older line:
## My request for Codex:
Do not treat this captured text as the request.</appshot>

## My request for Codex:
Trace the real first prompt."#,
            Some("Trace the real first prompt."),
        ),
        (
            "display-browser-comment",
            "Browser comment fallback",
            r#"# Browser comments:

## User Comment 1
Comment:
Lead with the user-authored browser comment.

## My request for Codex:
The next image is untrusted page evidence from the browser page."#,
            Some("Lead with the user-authored browser comment."),
        ),
        (
            "display-response-annotation",
            "Response annotation fallback",
            r#"# Response annotations:
<response-annotations>
[{"text":"An earlier response","annotation":"Keep the user-authored annotation."}]
</response-annotations>

## My request for Codex:
"#,
            Some("Keep the user-authored annotation."),
        ),
        (
            "display-goal-continuation",
            "Goal continuation fallback",
            r#"<codex_internal_context source="goal">
Continue working toward the active thread goal and do not stop early.
</codex_internal_context>"#,
            Some("Automatic goal continuation"),
        ),
        (
            "display-runtime-rejection",
            "Runtime wrapper fallback",
            "<recommended_plugins>runtime only</recommended_plugins>",
            None,
        ),
        (
            "display-appshot-rejection",
            "Appshot fallback",
            "# Applications mentioned by the user:\n\n<appshot>Captured UI only.</appshot>",
            None,
        ),
        (
            "display-attachment-rejection",
            "Attachment fallback",
            "![untrusted attachment](attachment://evidence.png)",
            None,
        ),
        (
            "display-ordinary-text",
            "Ordinary text fallback",
            "  Explain this repository to me.  ",
            Some("Explain this repository to me."),
        ),
    ];

    let connection = harness.db.connect().unwrap();
    for (ordinal, (thread_id, title, content, _)) in cases.iter().enumerate() {
        let timestamp = format!("2026-08-02T12:{ordinal:02}:00Z");
        connection
            .execute(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES(?1,?2,?3,?3)",
                rusqlite::params![thread_id, title, timestamp],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES(?1,?1,?2,?2,0)",
                rusqlite::params![thread_id, timestamp],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages(
                    id,thread_id,rollout_id,timestamp,role,content,source_line
                 ) VALUES(?1 || ':message',?1,?1,?2,'user',?3,1)",
                rusqlite::params![thread_id, timestamp, content],
            )
            .unwrap();
    }
    drop(connection);

    for (thread_id, title, _, expected) in cases {
        let summary = get_json(
            &harness.app,
            &format!("/api/v1/sessions/{thread_id}/summary"),
        )
        .await;
        assert_eq!(
            summary["session"]["firstPrompt"].as_str(),
            expected,
            "Sessions summary display drifted for {thread_id}"
        );

        let activity = get_json(
            &harness.app,
            &format!("/api/v1/sessions/{thread_id}/activity?page=1&pageSize=25"),
        )
        .await;
        let expected_label = expected.unwrap_or(title);
        assert_eq!(
            activity["items"][0]["label"], expected_label,
            "Activity root display drifted for {thread_id}"
        );
    }
}

#[tokio::test]
async fn conversation_tool_names_normalize_and_merge_in_session_summary() {
    const THREAD: &str = "conversation-tool-summary";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('{THREAD}','Tool summary','2026-08-02T13:00:00Z','2026-08-02T13:05:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES('{THREAD}','{THREAD}','2026-08-02T13:00:00Z','2026-08-02T13:05:00Z',0);
            INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
            ) VALUES(
                '{THREAD}:message','{THREAD}','{THREAD}',
                '2026-08-02T13:00:00Z','user','Summarize the tools.',1
            );
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,started_at,name,namespace,status,duration_ms
            ) VALUES
                ('tool-normalized-a','call-normalized-a','{THREAD}','{THREAD}',
                 '2026-08-02T13:01:00Z','js','mcp__node_repl','completed',10),
                ('tool-normalized-b','call-normalized-b','{THREAD}','{THREAD}',
                 '2026-08-02T13:02:00Z','js','mcp__node_repl__','failed',20),
                ('tool-nested','call-nested','{THREAD}','{THREAD}',
                 '2026-08-02T13:03:00Z','_search_emails','mcp__codex_apps__gmail','completed',30),
                ('tool-unknown','call-unknown','{THREAD}','{THREAD}',
                 '2026-08-02T13:04:00Z','unknown',NULL,'completed',40),
                ('tool-web','call-web','{THREAD}','{THREAD}',
                 '2026-08-02T13:05:00Z','web_search_call',NULL,'completed',50);
            "#
        ))
        .unwrap();
    drop(connection);

    let summary = get_json(&harness.app, &format!("/api/v1/sessions/{THREAD}/summary")).await;
    let tools = summary["toolSummary"].as_array().unwrap();
    let normalized = tools
        .iter()
        .find(|row| row["tool"] == "node_repl.js")
        .expect("namespace variants must merge under one display name");
    assert_eq!(normalized["count"], 2);
    assert_eq!(normalized["failedCount"], 1);
    assert_eq!(normalized["totalDurationMs"], 30);
    assert!(
        tools
            .iter()
            .any(|row| row["tool"] == "codex_apps.gmail._search_emails")
    );
    assert!(tools.iter().any(|row| row["tool"] == "tool"));
    assert!(tools.iter().any(|row| row["tool"] == "web_search"));
}

#[tokio::test]
async fn session_search_is_unicode_normalized_and_treats_like_metacharacters_literally() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,title,project,branch,started_at,last_event_at) VALUES
                ('search-percent-new','100% literal marker','search','main',
                 '2026-08-01T12:00:00Z','2026-08-01T12:00:00Z'),
                ('search-percent-old','50% literal marker','search','main',
                 '2026-08-01T11:00:00Z','2026-08-01T11:00:00Z'),
                ('search-percent-decoy','100x literal marker','search','main',
                 '2026-08-01T10:00:00Z','2026-08-01T10:00:00Z'),
                ('search-underscore','literal_under_score','search','main',
                 '2026-08-01T09:00:00Z','2026-08-01T09:00:00Z'),
                ('search-backslash','literal\\path','search','main',
                 '2026-08-01T08:00:00Z','2026-08-01T08:00:00Z'),
                ('search-unicode','Éclair report','search','main',
                 '2026-08-01T07:00:00Z','2026-08-01T07:00:00Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('search-percent-new','search-percent-new','2026-08-01T12:00:00Z','2026-08-01T12:00:00Z',0),
                ('search-percent-old','search-percent-old','2026-08-01T11:00:00Z','2026-08-01T11:00:00Z',0),
                ('search-percent-decoy','search-percent-decoy','2026-08-01T10:00:00Z','2026-08-01T10:00:00Z',0),
                ('search-underscore','search-underscore','2026-08-01T09:00:00Z','2026-08-01T09:00:00Z',0),
                ('search-backslash','search-backslash','2026-08-01T08:00:00Z','2026-08-01T08:00:00Z',0),
                ('search-unicode','search-unicode','2026-08-01T07:00:00Z','2026-08-01T07:00:00Z',0);
             INSERT INTO messages(id,thread_id,rollout_id,timestamp,role,content,source_line) VALUES
                ('search-percent-new-message','search-percent-new','search-percent-new','2026-08-01T12:00:00Z','user','visible',1),
                ('search-percent-old-message','search-percent-old','search-percent-old','2026-08-01T11:00:00Z','user','visible',1),
                ('search-percent-decoy-message','search-percent-decoy','search-percent-decoy','2026-08-01T10:00:00Z','user','visible',1),
                ('search-underscore-message','search-underscore','search-underscore','2026-08-01T09:00:00Z','user','visible',1),
                ('search-backslash-message','search-backslash','search-backslash','2026-08-01T08:00:00Z','user','visible',1),
                ('search-unicode-message','search-unicode','search-unicode','2026-08-01T07:00:00Z','user','visible',1);",
        )
        .unwrap();
    drop(connection);

    let literal_percent =
        get_json(&harness.app, "/api/v1/sessions?q=%25&sort=cost&pageSize=50").await;
    assert_eq!(literal_percent["total"], 2);
    assert_eq!(literal_percent["items"].as_array().unwrap().len(), 2);
    assert!(
        literal_percent["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["title"].as_str().unwrap().contains('%'))
    );

    for (query, expected_id) in [
        ("%5F", "search-underscore"),
        ("%5C", "search-backslash"),
        // The query uses a decomposed accent; the stored title is precomposed.
        ("e%CC%81clair", "search-unicode"),
    ] {
        let response = get_json(
            &harness.app,
            &format!("/api/v1/sessions?q={query}&pageSize=50"),
        )
        .await;
        assert_eq!(response["total"], 1, "unexpected total for {query}");
        assert_eq!(response["items"][0]["id"], expected_id);
    }
}

#[tokio::test]
async fn price_search_is_unicode_normalized_and_treats_like_metacharacters_literally() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO model_prices(
                model_id,effective_from,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,
                currency,source
             ) VALUES
                ('price%model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                ('priceXmodel','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                ('price_model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                ('price\\model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                ('Éclair-price','1970-01-01T00:00:00Z',1,1,1,'USD','manual');",
        )
        .unwrap();
    drop(connection);

    for (query, expected_id) in [
        ("%25", "price%model"),
        ("%5F", "price_model"),
        ("%5C", "price\\model"),
        ("e%CC%81clair", "Éclair-price"),
    ] {
        let response = get_json(
            &harness.app,
            &format!("/api/v1/prices?q={query}&page=1&pageSize=25"),
        )
        .await;
        assert_eq!(response["total"], 1, "unexpected total for {query}");
        assert_eq!(response["items"][0]["modelId"], expected_id);
    }
}

#[tokio::test]
async fn pagination_never_echoes_an_integer_javascript_cannot_represent_exactly() {
    let harness = harness();
    let maximum_safe = 9_007_199_254_740_991_u64;

    let safe = get_json(
        &harness.app,
        &format!("/api/v1/sessions?page={maximum_safe}&pageSize=1"),
    )
    .await;
    assert_eq!(safe["page"].as_u64(), Some(maximum_safe));

    for uri in [
        format!("/api/v1/sessions?page={}&pageSize=1", maximum_safe + 1),
        format!("/api/v1/prices?page={}&pageSize=1", maximum_safe + 1),
        format!("/api/v1/aliases?page={}&pageSize=1", maximum_safe + 1),
        format!(
            "/api/v1/sessions/{RICH_SESSION}/activity?page={}&pageSize=1",
            maximum_safe + 1
        ),
        format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/event-rich-tool?childPage={}&childPageSize=1",
            maximum_safe + 1
        ),
    ] {
        let (status, _, body) = raw_request(&harness.app, Method::GET, &uri, None).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{uri}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(String::from_utf8_lossy(&body).contains("page must not exceed"));
    }
}

#[tokio::test]
async fn pagination_defaults_and_clamps_each_transport_surface() {
    let harness = harness();

    let sessions_default = get_json(&harness.app, "/api/v1/sessions").await;
    assert_eq!(sessions_default["page"], 1);
    assert_eq!(sessions_default["pageSize"], 50);
    let sessions_minimum = get_json(&harness.app, "/api/v1/sessions?page=0&pageSize=0").await;
    assert_eq!(sessions_minimum["page"], 1);
    assert_eq!(sessions_minimum["pageSize"], 1);
    let sessions_maximum = get_json(&harness.app, "/api/v1/sessions?pageSize=9999").await;
    assert_eq!(sessions_maximum["pageSize"], 200);

    let activity_path = format!("/api/v1/sessions/{RICH_SESSION}/activity");
    let activity_default = get_json(&harness.app, &activity_path).await;
    assert_eq!(activity_default["page"], 1);
    assert_eq!(activity_default["pageSize"], 25);
    let activity_minimum =
        get_json(&harness.app, &format!("{activity_path}?page=0&pageSize=0")).await;
    assert_eq!(activity_minimum["page"], 1);
    assert_eq!(activity_minimum["pageSize"], 1);
    let activity_maximum = get_json(&harness.app, &format!("{activity_path}?pageSize=9999")).await;
    assert_eq!(activity_maximum["pageSize"], 100);

    let detail_id = activity_default["items"][0]["id"]
        .as_str()
        .expect("the rich fixture must expose an Activity detail")
        .to_owned();
    let detail_path = format!("/api/v1/sessions/{RICH_SESSION}/activity/{detail_id}");
    let detail_default = get_json(&harness.app, &detail_path).await;
    assert_eq!(detail_default["childPage"], 1);
    assert_eq!(detail_default["childPageSize"], 250);
    let detail_minimum = get_json(
        &harness.app,
        &format!("{detail_path}?childPage=0&childPageSize=0"),
    )
    .await;
    assert_eq!(detail_minimum["childPage"], 1);
    assert_eq!(detail_minimum["childPageSize"], 1);
    let detail_maximum = get_json(&harness.app, &format!("{detail_path}?childPageSize=9999")).await;
    assert_eq!(detail_maximum["childPageSize"], 500);

    for route in ["prices", "aliases"] {
        let default = get_json(&harness.app, &format!("/api/v1/{route}")).await;
        assert_eq!(default["page"], 1);
        assert_eq!(default["pageSize"], 25);
        let minimum = get_json(&harness.app, &format!("/api/v1/{route}?page=0&pageSize=0")).await;
        assert_eq!(minimum["page"], 1);
        assert_eq!(minimum["pageSize"], 1);
        let maximum = get_json(&harness.app, &format!("/api/v1/{route}?pageSize=9999")).await;
        assert_eq!(maximum["pageSize"], 100);
    }
}

#[tokio::test]
async fn bounded_cost_sort_uses_period_cost_instead_of_lifetime_cost() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    let insert_usage = |id: &str, thread_id: &str, timestamp: &str, output_tokens: i64| {
        connection
            .execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    model,effort,input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(
                    ?1,?2,(SELECT id FROM rollouts WHERE thread_id=?2 LIMIT 1),NULL,NULL,
                    ?3,999999,'gpt-5.5',NULL,0,0,?4,0,?4,1
                 )",
                rusqlite::params![id, thread_id, timestamp, output_tokens],
            )
            .unwrap();
    };
    insert_usage(
        "period-sort-rich-inside",
        RICH_SESSION,
        "2026-07-14T00:00:10Z",
        1,
    );
    insert_usage(
        "period-sort-aborted-inside",
        ABORTED_SESSION,
        "2026-07-14T00:00:20Z",
        1_000_000,
    );
    insert_usage(
        "period-sort-rich-outside",
        RICH_SESSION,
        "2026-07-14T00:02:00Z",
        10_000_000,
    );
    drop(connection);

    let sorted = get_json(
        &harness.app,
        "/api/v1/sessions?start=2026-07-14T00%3A00%3A00Z&end=2026-07-14T00%3A01%3A00Z&sort=cost&pageSize=50",
    )
    .await;
    assert_eq!(sorted["total"], 2);
    assert_eq!(sorted["items"][0]["id"], ABORTED_SESSION);
    assert_eq!(sorted["items"][1]["id"], RICH_SESSION);
    assert!(usd_units(&sorted["items"][0]["costUsd"]) > usd_units(&sorted["items"][1]["costUsd"]));
    assert!(
        usd_units(&sorted["items"][1]["lifetimeCostUsd"])
            > usd_units(&sorted["items"][0]["lifetimeCostUsd"]),
        "lifetime order is deliberately inverted so it cannot drive bounded sorting"
    );
}

#[tokio::test]
async fn raw_activity_wire_shape_preserves_nulls_and_optional_paging_fields() {
    let harness = harness();

    let page = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/activity?page=1&pageSize=25"),
    )
    .await;
    assert_exact_json_keys(
        &page,
        &["items", "days", "page", "pageSize", "total", "totalPages"],
    );
    for day in page["days"].as_array().unwrap() {
        assert_exact_json_keys(day, &["date", "durationMs", "totals"]);
        assert_exact_json_keys(
            &day["totals"],
            &[
                "inputTokens",
                "cachedInputTokens",
                "outputTokens",
                "reasoningTokens",
                "blendedTokens",
                "totalTokens",
                "costUsd",
                "unpricedTokens",
                "pricingComplete",
            ],
        );
    }
    let root = &page["items"][0];
    assert_activity_item_wire_keys(root, &[]);
    assert!(root["children"].as_array().unwrap().is_empty());
    assert!(root.get("childPage").is_none());

    let root_id = root["id"].as_str().unwrap();
    let detail = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/activity/{root_id}?childPage=1&childPageSize=1"),
    )
    .await;
    assert_activity_item_wire_keys(
        &detail,
        &[
            "childPage",
            "childPageSize",
            "childTotal",
            "childHasMore",
            "childNextCursor",
        ],
    );
    assert_eq!(detail["childPage"], 1);
    assert_eq!(detail["childPageSize"], 1);
    assert!(detail["childNextCursor"].is_string());

    let replay_page = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/activity"),
    )
    .await;
    let replay_root_id = replay_page["items"][0]["id"].as_str().unwrap();
    let replay_detail = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{replay_root_id}"),
    )
    .await;
    let group = replay_detail["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["kind"]
                .as_str()
                .is_some_and(|kind| kind.ends_with("_group"))
        })
        .expect("the replay fixture must expose a synthetic group");
    assert_activity_item_wire_keys(
        group,
        &["childPage", "childPageSize", "childTotal", "childHasMore"],
    );
    assert!(group["body"].is_null() || group["body"].is_string());
    assert!(group["counts"].is_null());
    assert!(group.get("childNextCursor").is_none());

    let group_id = group["id"].as_str().unwrap();
    let group_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{group_id}?childPage=1&childPageSize=1"
        ),
    )
    .await;
    assert_activity_item_wire_keys(
        &group_detail,
        &["childPage", "childPageSize", "childTotal", "childHasMore"],
    );
    assert!(group_detail.get("childNextCursor").is_none());
    assert_activity_item_wire_keys(&group_detail["children"][0], &[]);
}

#[tokio::test]
async fn session_detail_preserves_rich_activity_and_tool_metadata_without_payloads() {
    let harness = harness();

    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "UPDATE messages SET content=?1 WHERE id='msg_rich_user'",
            [r#"# Applications mentioned by the user:

<appshot>Terminal evidence.</appshot>

## My request for Codex:
Revisit the usage application from ingestion through the browser UI."#],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO rollouts(id,thread_id,parent_rollout_id,parent_thread_id,
                                  agent_path,agent_nickname,cwd,started_at,last_event_at,archived)
             VALUES('child-before-root-prompt',?1,NULL,?1,'/root/audit','Audit',NULL,
                    '2026-07-15T20:13:00Z','2026-07-15T20:13:01Z',0)",
            [RICH_SESSION],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO messages(id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line)
             VALUES('child-before-root-prompt:1',?1,'child-before-root-prompt',NULL,
                    '2026-07-15T20:13:00Z','user','Audit the parent session.',1)",
            [RICH_SESSION],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO messages(id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line)
             VALUES('child-after-root-result:1',?1,'child-before-root-prompt',NULL,
                    '2026-07-15T20:50:00Z','assistant','Child-only result must not replace the parent result.',2)",
            [RICH_SESSION],
        )
        .unwrap();
    drop(connection);

    let summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/summary"),
    )
    .await;
    assert_session_row(&summary["session"]);
    assert_eq!(summary["session"]["id"], RICH_SESSION);
    assert_eq!(summary["session"]["project"], "codex-dashboard");
    assert_eq!(summary["session"]["branch"], "master");
    assert_eq!(summary["session"]["status"], "completed");
    assert!(
        summary["session"]["cwd"]
            .as_str()
            .unwrap()
            .ends_with("codex-dashboard")
    );
    assert_eq!(
        summary["session"]["firstPrompt"],
        "Revisit the usage application from ingestion through the browser UI."
    );
    assert_eq!(
        summary["session"]["latestResult"],
        "The model now keeps the rich trace while the main UI stays quiet."
    );
    assert_totals(&summary["totals"]);
    assert_eq!(summary["totals"]["totalTokens"], 85_119);
    assert_usd(&summary["totals"]["costUsd"], "0.215899");
    assert_eq!(summary["models"][0]["model"], "gpt-5.6-sol");
    assert_eq!(summary["models"][0]["effort"], "ultra");
    for model in summary["models"].as_array().unwrap() {
        assert_nullable_usd(model, "costUsd");
    }
    for agent in summary["agents"].as_array().unwrap() {
        assert_nullable_usd(agent, "costUsd");
    }
    assert!(summary["agents"].as_array().unwrap().iter().any(|agent| {
        agent["label"] == "/root/storage_audit" || agent["path"] == "/root/storage_audit"
    }));
    assert_eq!(
        summary["agents"].as_array().unwrap().len() as u64,
        summary["session"]["agentCount"].as_u64().unwrap(),
        "summary agents are the subagents counted in the session row"
    );
    let tools = summary["toolSummary"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["tool"].as_str().unwrap())
        .collect::<Vec<_>>();
    for expected in [
        "exec",
        "node_repl.js",
        "apply_patch",
        "web_search",
        "tool_search",
        "image_generation",
    ] {
        assert!(tools.contains(&expected), "missing {expected}: {tools:?}");
    }

    let connection = harness.db.connect().unwrap();
    assert_eq!(
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,role,label,body,status,tool_name,call_id,duration_ms,model,
                    effort,payload_json,native
                 ) SELECT
                    'duplicate-image-lifecycle',thread_id,rollout_id,turn_id,
                    agent_run_id,timestamp,900001,kind,role,label,body,'generating',
                    tool_name,call_id,duration_ms,model,effort,payload_json,native
                 FROM events
                 WHERE thread_id=?1 AND kind='tool_call'
                   AND call_id='image-evidence-1'
                 LIMIT 1",
                [RICH_SESSION],
            )
            .unwrap(),
        1,
        "the fixture includes a repeated lifecycle state for one tool call"
    );
    drop(connection);

    let (status, _, activity_body) = raw_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/sessions/{RICH_SESSION}/activity?page=1&pageSize=25"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        activity_body.len() < 64 * 1024,
        "collapsed activity was {} bytes",
        activity_body.len()
    );
    assert!(!String::from_utf8_lossy(&activity_body).contains("data:image"));
    assert!(!String::from_utf8_lossy(&activity_body).contains("iVBOR"));
    let activity: Value = serde_json::from_slice(&activity_body).unwrap();
    assert_eq!(activity["page"], 1);
    assert_eq!(activity["pageSize"], 25);
    assert_eq!(activity["total"], 1);
    assert_eq!(activity["totalPages"], 1);
    assert_eq!(activity["days"].as_array().unwrap().len(), 1);
    assert_eq!(activity["days"][0]["date"], "2026-07-15");
    assert_eq!(activity["days"][0]["durationMs"], 1_727_880);
    assert_totals(&activity["days"][0]["totals"]);
    let turn = &activity["items"][0];
    assert_eq!(turn["kind"], "exchange");
    assert_eq!(turn["role"], "user");
    assert_eq!(
        turn["label"],
        "Revisit the usage application from ingestion through the browser UI."
    );
    assert_eq!(
        turn["body"],
        "The model now keeps the rich trace while the main UI stays quiet."
    );
    assert_eq!(turn["model"], "gpt-5.6-sol");
    assert_eq!(turn["counts"]["modelCalls"], 2);
    assert_eq!(turn["counts"]["toolCalls"], 8);
    assert_eq!(turn["counts"]["agentRuns"], 0);
    assert_eq!(turn["counts"]["reviews"], 0);
    assert_eq!(turn["counts"]["followUps"], 0);
    assert_eq!(turn["hasDetails"], true);
    assert!(turn["children"].as_array().unwrap().is_empty());
    assert!(turn.get("attachments").is_none());
    assert_totals(&turn["usage"]);
    let turn_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/{}",
            turn["id"].as_str().unwrap()
        ),
    )
    .await;
    let children = turn_detail["children"].as_array().unwrap();
    assert!(
        children.windows(2).all(|pair| {
            pair[0]["timestamp"].as_str().unwrap() >= pair[1]["timestamp"].as_str().unwrap()
        }),
        "expanded turn activity must be newest first"
    );
    let kinds = children
        .iter()
        .map(|item| item["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    for expected in [
        "user",
        "update",
        "reasoning",
        "tool",
        "subagent",
        "goal",
        "compaction",
        "final",
    ] {
        assert!(kinds.contains(&expected), "missing {expected}: {kinds:?}");
    }
    assert!(turn_detail["body"].is_null());
    assert_eq!(turn_detail["usage"], turn["usage"]);
    assert_eq!(turn_detail["counts"], turn["counts"]);
    assert_eq!(
        kinds.iter().filter(|kind| **kind == "final").count(),
        1,
        "the chronological activity contains exactly one final answer"
    );
    assert_eq!(kinds.iter().filter(|kind| **kind == "tool").count(), 8);
    assert_eq!(kinds.iter().filter(|kind| **kind == "subagent").count(), 3);
    let attributed = children
        .iter()
        .filter(|item| !item["usage"].is_null())
        .collect::<Vec<_>>();
    assert!(
        attributed.iter().all(|item| matches!(
            item["kind"].as_str(),
            Some("tool" | "subagent" | "final" | "update" | "assistant" | "reasoning")
        )),
        "usage belongs only to the latest preceding visible model-output event: {attributed:?}"
    );
    assert_eq!(
        attributed
            .iter()
            .map(|item| item["usage"]["totalTokens"].as_u64().unwrap())
            .sum::<u64>(),
        turn["usage"]["totalTokens"].as_u64().unwrap(),
        "every usage fact is attributed exactly once"
    );
    assert_eq!(
        attributed
            .iter()
            .map(|item| usd_units(&item["usage"]["costUsd"]))
            .sum::<i128>(),
        usd_units(&turn["usage"]["costUsd"]),
        "attributed costs must reconcile exactly to turn cost",
    );
    assert!(
        children.iter().find(|item| item["kind"] == "user").unwrap()["usage"].is_null(),
        "user input does not manufacture model usage"
    );
    let attributed_event = attributed[0];
    let attributed_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/{}",
            attributed_event["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(attributed_detail["usage"], attributed_event["usage"]);
    assert!(
        children
            .iter()
            .any(|item| { item["kind"] == "tool" && item["toolName"] == "node_repl.js" })
    );
    assert!(serde_json::to_vec(&turn_detail).unwrap().len() < 64 * 1024);
    assert!(!turn_detail.to_string().contains("data:image"));
    assert!(!turn_detail.to_string().contains("iVBOR"));

    let tool = children
        .iter()
        .find(|item| item["kind"] == "tool" && item["toolName"] == "exec")
        .unwrap();
    let tool_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/{}",
            tool["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(tool_detail["toolName"], "exec");
    assert_eq!(tool_detail["status"], "completed");
    assert!(tool_detail["body"].is_null());
    assert!(tool_detail.get("attachments").is_none());

    let image_tool = children
        .iter()
        .find(|item| {
            item["kind"] == "tool"
                && item["toolName"] == "image_generation"
                && item["timestamp"]
                    .as_str()
                    .is_some_and(|value| value.starts_with("2026-07-15T20:13:06.316"))
        })
        .unwrap();
    assert_ne!(image_tool["id"], "duplicate-image-lifecycle");
    assert!(!image_tool.to_string().contains("iVBOR"));
    let image_tool_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{RICH_SESSION}/activity/{}",
            image_tool["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(image_tool_detail["toolName"], "image_generation");
    assert!(image_tool_detail["body"].is_null());
    assert!(image_tool_detail.get("attachments").is_none());

    assert_eq!(summary["models"][0]["model"], "gpt-5.6-sol");
    for model in summary["models"].as_array().unwrap() {
        assert_nullable_usd(model, "costUsd");
    }
    assert!(!summary["agents"].as_array().unwrap().is_empty());
    for row in summary["agents"].as_array().unwrap() {
        assert!(row["id"].is_string());
        assert!(row["label"].is_string());
        assert_nullable_usd(row, "costUsd");
    }

    let (status, _, aborted_body) = raw_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/sessions/{ABORTED_SESSION}/activity?page=1&pageSize=25"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(aborted_body.len() < 64 * 1024);
    assert!(!String::from_utf8_lossy(&aborted_body).contains("data:image"));
    let aborted: Value = serde_json::from_slice(&aborted_body).unwrap();
    let aborted_turn = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{ABORTED_SESSION}/activity/{}",
            aborted["items"][0]["id"].as_str().unwrap()
        ),
    )
    .await;
    let user = aborted_turn["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["kind"] == "user")
        .unwrap();
    assert!(user.get("attachments").is_none());
    let user_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{ABORTED_SESSION}/activity/{}",
            user["id"].as_str().unwrap()
        ),
    )
    .await;
    assert!(!user_detail.to_string().contains("data:image"));
    assert!(user_detail.get("attachments").is_none());

    // A real review turn occurs inside the root exchange, while its usage fact
    // lands the following day. The exchange total must remain inclusive of the
    // review it caused, while day totals continue to follow usage timestamps.
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,agent_path,
                agent_nickname,cwd,started_at,last_event_at,archived
            ) VALUES(
                'fixture-review-rollout','{JULY_REPLAY_SESSION}',
                '{JULY_REPLAY_SESSION}','{JULY_REPLAY_SESSION}',
                '/root/guardian','Guardian',NULL,
                '2026-07-15T07:33:20Z','2026-07-15T07:33:40Z',0
            );
            INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,agent_path,nickname,
                started_at,completed_at,status
            ) VALUES(
                'fixture-review-run','{JULY_REPLAY_SESSION}',
                'fixture-review-rollout','{JULY_REPLAY_SESSION}',
                '/root/guardian','Guardian','2026-07-15T07:33:20Z',
                '2026-07-15T07:33:40Z','completed'
            );
            INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,completed_at,
                status,model,effort,last_agent_message,duration_ms
            ) VALUES(
                'fixture-review-turn','{JULY_REPLAY_SESSION}',
                'fixture-review-rollout','fixture-review-run',
                '2026-07-15T07:33:20Z','2026-07-15T07:33:40Z',
                'completed','codex-auto-review','low','{{"outcome":"allow"}}',20000
            );
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                model,effort,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
            ) VALUES(
                'fixture-review-usage','{JULY_REPLAY_SESSION}',
                'fixture-review-rollout','fixture-review-turn','fixture-review-run',
                '2026-07-16T00:01:00Z',999001,'gpt-5.5','low',
                10000,0,0,0,10000,1
            );
            "#
        ))
        .unwrap();
    drop(connection);

    let replay_page_1 = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/activity?page=1&pageSize=1"),
    )
    .await;
    let replay_page_2 = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/activity?page=2&pageSize=1"),
    )
    .await;
    let replay_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{}",
            replay_page_1["items"][0]["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(replay_page_1["total"], 1);
    assert_eq!(replay_page_1["totalPages"], 1);
    assert_eq!(replay_page_1["items"].as_array().unwrap().len(), 1);
    assert_eq!(replay_page_2["page"], 1);
    assert_eq!(replay_page_2["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        replay_page_2["items"][0]["id"],
        replay_page_1["items"][0]["id"]
    );
    let replay_exchange = &replay_page_1["items"][0];
    assert_eq!(replay_exchange["kind"], "exchange");
    assert_eq!(
        replay_exchange["label"],
        "Analyze three AI compute buildout scenarios through the supply chain."
    );
    assert_eq!(replay_exchange["counts"]["modelCalls"], 1);
    assert_eq!(replay_exchange["counts"]["toolCalls"], 2);
    assert_eq!(replay_exchange["counts"]["agentRuns"], 1);
    assert_eq!(replay_exchange["counts"]["reviews"], 1);
    assert_eq!(replay_exchange["counts"]["followUps"], 0);
    let replay_days = replay_page_1["days"].as_array().unwrap();
    let july_15 = replay_days
        .iter()
        .find(|day| day["date"] == "2026-07-15")
        .unwrap();
    let july_16 = replay_days
        .iter()
        .find(|day| day["date"] == "2026-07-16")
        .expect("usage-only descendant days remain representable");
    assert_eq!(
        july_15["totals"]["totalTokens"], 276_982,
        "day totals follow usage timestamps, so next-day review usage stays out"
    );
    assert_eq!(july_16["totals"]["totalTokens"], 10_000);
    assert_eq!(
        july_15["durationMs"], 149_539,
        "the contained 20s review must not inflate the union of overlapping turns"
    );

    let replay_children = replay_detail["children"].as_array().unwrap();
    let agent_group = replay_children
        .iter()
        .find(|item| item["kind"] == "agent_group")
        .expect("subagents remain available in a synthetic agent group");
    let review_group = replay_children
        .iter()
        .find(|item| item["kind"] == "review_group")
        .expect("automated review turns remain available in a review group");
    assert!(agent_group["children"].as_array().unwrap().is_empty());
    assert_eq!(agent_group["childTotal"], 1);
    assert!(review_group["children"].as_array().unwrap().is_empty());
    assert_eq!(review_group["childTotal"], 1);
    let agent_group = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{}",
            agent_group["id"].as_str().unwrap()
        ),
    )
    .await;
    let review_group = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{JULY_REPLAY_SESSION}/activity/{}",
            review_group["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(agent_group["children"].as_array().unwrap().len(), 1);
    assert_eq!(agent_group["children"][0]["kind"], "subagent");
    assert_eq!(agent_group["children"][0]["agentLabel"], "Russell");
    assert_eq!(agent_group["usage"]["totalTokens"], 92_678);
    assert_eq!(
        agent_group["usage"], agent_group["children"][0]["usage"],
        "one-agent group usage is the real contained turn usage"
    );
    assert_eq!(review_group["children"].as_array().unwrap().len(), 1);
    assert_eq!(review_group["children"][0]["kind"], "review");
    assert_eq!(review_group["children"][0]["id"], "fixture-review-turn");
    assert_eq!(review_group["usage"]["totalTokens"], 10_000);
    assert_usd(&review_group["usage"]["costUsd"], "0.05");
    assert_eq!(
        review_group["usage"], review_group["children"][0]["usage"],
        "one-review group usage is the real contained turn usage"
    );
    assert_eq!(
        replay_exchange["usage"], replay_detail["usage"],
        "the list and expanded exchange must use the same accounting scope"
    );
    assert_eq!(
        replay_exchange["usage"]["totalTokens"], 286_982,
        "the exchange includes root work, its agent branch, and its review"
    );
    let replay_root_attributed = replay_children
        .iter()
        .filter(|item| !matches!(item["kind"].as_str(), Some("agent_group" | "review_group")))
        .filter(|item| !item["usage"].is_null())
        .collect::<Vec<_>>();
    assert_eq!(
        replay_root_attributed
            .iter()
            .map(|item| item["usage"]["totalTokens"].as_u64().unwrap())
            .sum::<u64>(),
        184_304,
        "root usage is attributed exactly once before descendant groups are added"
    );
    assert!(replay_root_attributed.iter().all(|item| matches!(
        item["kind"].as_str(),
        Some("tool" | "subagent" | "final" | "update" | "assistant" | "reasoning")
    )));
    for field in [
        "inputTokens",
        "cachedInputTokens",
        "outputTokens",
        "reasoningTokens",
        "totalTokens",
        "unpricedTokens",
    ] {
        let visible_sum = replay_children
            .iter()
            .filter_map(|item| item["usage"][field].as_u64())
            .sum::<u64>();
        assert_eq!(
            visible_sum,
            replay_exchange["usage"][field].as_u64().unwrap(),
            "visible child {field} must reconcile exactly to its exchange"
        );
    }
    let visible_cost = replay_children
        .iter()
        .filter(|item| !item["usage"]["costUsd"].is_null())
        .map(|item| usd_units(&item["usage"]["costUsd"]))
        .sum::<i128>();
    let exchange_cost = usd_units(&replay_exchange["usage"]["costUsd"]);
    assert_eq!(
        visible_cost, exchange_cost,
        "visible child costs must reconcile exactly to exchange cost"
    );

    let legacy_summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{LEGACY_SESSION}/summary"),
    )
    .await;
    assert_eq!(legacy_summary["totals"]["totalTokens"], 0);
    assert_eq!(legacy_summary["session"]["messageCount"], 3);
    assert_eq!(legacy_summary["session"]["branch"], "main");
    assert_eq!(
        legacy_summary["session"]["firstPrompt"],
        "Explain this repository to me."
    );
    assert_eq!(
        legacy_summary["session"]["latestResult"],
        "The application has a small binary entry point and a reusable library core."
    );
    let legacy_activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{LEGACY_SESSION}/activity?page=1&pageSize=25"),
    )
    .await;
    let legacy_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{LEGACY_SESSION}/activity/{}",
            legacy_activity["items"][0]["id"].as_str().unwrap()
        ),
    )
    .await;
    let legacy_kinds = legacy_detail["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(legacy_kinds.contains(&"user"));
    assert!(legacy_kinds.contains(&"reasoning"));
    assert!(legacy_kinds.contains(&"tool"));
    assert!(legacy_kinds.contains(&"final"));

    let guardian_summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert!(guardian_summary["session"]["firstPrompt"].is_null());
    assert_eq!(
        guardian_summary["session"]["latestResult"],
        "{\"outcome\":\"allow\"}"
    );
    assert_eq!(guardian_summary["session"]["messageCount"], 0);
    assert_eq!(guardian_summary["totals"]["unpricedTokens"], 25_607);
    assert!(guardian_summary["totals"]["costUsd"].is_null());
    let guardian_activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/activity?page=1&pageSize=25"),
    )
    .await;
    let guardian_detail = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{GUARDIAN_SESSION}/activity/{}",
            guardian_activity["items"][0]["id"].as_str().unwrap()
        ),
    )
    .await;
    let guardian_finals = guardian_detail["children"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["kind"] == "final")
        .collect::<Vec<_>>();
    assert_eq!(guardian_finals.len(), 1);
    assert_eq!(guardian_finals[0]["body"], "{\"outcome\":\"allow\"}");
    assert!(guardian_detail["body"].is_null());

    let aborted_summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{ABORTED_SESSION}/summary"),
    )
    .await;
    assert_eq!(aborted_summary["session"]["messageCount"], 1);
    assert_eq!(aborted_summary["session"]["status"], "interrupted");
    assert_eq!(aborted_summary["totals"]["totalTokens"], 0);
    assert_usd(&aborted_summary["totals"]["costUsd"], "0.00");
    assert!(
        aborted_turn["children"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["kind"] == "system" && item["status"] == "interrupted")
    );
}

#[tokio::test]
async fn session_summary_turn_fallback_stays_on_the_root_rollout() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    let root_result: String = connection
        .query_row(
            "SELECT last_agent_message FROM turns
             WHERE thread_id=?1 AND rollout_id=?1 AND last_agent_message IS NOT NULL
             ORDER BY started_at DESC LIMIT 1",
            [RICH_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM messages WHERE thread_id=?1 AND role='assistant'",
            [RICH_SESSION],
        )
        .unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
            ) VALUES(
                'summary-fallback-child','{RICH_SESSION}','{RICH_SESSION}','{RICH_SESSION}',
                '2026-07-16T00:00:00Z','2026-07-16T00:01:00Z',0
            );
            INSERT INTO turns(
                id,thread_id,rollout_id,started_at,completed_at,status,last_agent_message
            ) VALUES(
                'summary-fallback-child-turn','{RICH_SESSION}','summary-fallback-child',
                '2026-07-16T00:00:00Z','2026-07-16T00:01:00Z','completed',
                'A later child-only result.'
            );
            "#
        ))
        .unwrap();
    drop(connection);

    let summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/summary"),
    )
    .await;
    assert_eq!(summary["session"]["latestResult"], root_result);
}

#[tokio::test]
async fn session_summary_raw_json_shape_is_exact() {
    let harness = harness();
    let summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{RICH_SESSION}/summary"),
    )
    .await;

    assert_exact_json_keys(
        &summary,
        &["session", "totals", "models", "agents", "toolSummary"],
    );
    assert_exact_json_keys(
        &summary["session"],
        &[
            "id",
            "rootThreadId",
            "startedAt",
            "lastEventAt",
            "title",
            "project",
            "branch",
            "messageCount",
            "turnCount",
            "agentCount",
            "toolCount",
            "totalTokens",
            "costUsd",
            "unpricedTokens",
            "lifetimeCostUsd",
            "lifetimeUnpricedTokens",
            "cwd",
            "source",
            "firstPrompt",
            "latestResult",
            "completedAt",
            "status",
        ],
    );
    let model = summary["models"]
        .as_array()
        .unwrap()
        .first()
        .expect("the rich fixture has model usage");
    assert_exact_json_keys(
        model,
        &[
            "model",
            "effort",
            "inputTokens",
            "cachedInputTokens",
            "outputTokens",
            "reasoningTokens",
            "totalTokens",
            "costUsd",
            "unpricedTokens",
        ],
    );
    let agent = summary["agents"]
        .as_array()
        .unwrap()
        .first()
        .expect("the rich fixture has an agent summary");
    assert_exact_json_keys(
        agent,
        &[
            "id",
            "label",
            "path",
            "nickname",
            "status",
            "turnCount",
            "toolCount",
            "totalTokens",
            "costUsd",
            "unpricedTokens",
        ],
    );
    let tool = summary["toolSummary"]
        .as_array()
        .unwrap()
        .first()
        .expect("the rich fixture has a tool summary");
    assert_exact_json_keys(tool, &["tool", "count", "failedCount", "totalDurationMs"]);
}

#[tokio::test]
async fn session_summary_root_rollout_priority_is_stable() {
    const THREAD: &str = "summary-root-priority";
    const SELECTED: &str = "summary-root-a-early";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES(
                '{THREAD}','Root priority','2046-01-01T00:00:00Z',
                '2046-01-01T00:10:00Z'
            );
            INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
            ) VALUES
                ('summary-child-first','{THREAD}','missing-parent','missing-thread',
                 '2046-01-01T00:00:00Z','2046-01-01T00:00:30Z',0),
                ('summary-root-z-early','{THREAD}',NULL,NULL,
                 '2046-01-01T00:01:00Z','2046-01-01T00:02:00Z',0),
                ('{SELECTED}','{THREAD}',NULL,NULL,
                 '2046-01-01T00:01:00Z','2046-01-01T00:03:00Z',0),
                ('summary-root-a-later','{THREAD}',NULL,NULL,
                 '2046-01-01T00:02:00Z','2046-01-01T00:04:00Z',0);
            INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
            ) VALUES
                ('summary-child-user','{THREAD}','summary-child-first',
                 '2046-01-01T00:00:10Z','user','WRONG CHILD PROMPT',1),
                ('summary-child-assistant','{THREAD}','summary-child-first',
                 '2046-01-01T00:00:20Z','assistant','WRONG CHILD RESULT',2),
                ('summary-root-z-user','{THREAD}','summary-root-z-early',
                 '2046-01-01T00:01:10Z','user','WRONG ID-TIE PROMPT',3),
                ('summary-root-later-user','{THREAD}','summary-root-a-later',
                 '2046-01-01T00:02:10Z','user','WRONG LATER PROMPT',4),
                ('summary-selected-user','{THREAD}','{SELECTED}',
                 '2046-01-01T00:01:20Z','user','Selected parentless prompt',5),
                ('summary-selected-assistant','{THREAD}','{SELECTED}',
                 '2046-01-01T00:01:30Z','assistant','Selected parentless result',6);
            "#
        ))
        .unwrap();
    drop(connection);

    let summary = get_json(&harness.app, &format!("/api/v1/sessions/{THREAD}/summary")).await;
    assert_eq!(
        summary["session"]["firstPrompt"],
        "Selected parentless prompt"
    );
    assert_eq!(
        summary["session"]["latestResult"],
        "Selected parentless result"
    );
}

#[tokio::test]
async fn session_summary_breakdowns_preserve_ordering_labels_and_caps() {
    const THREAD: &str = "summary-breakdowns";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES(
                '{THREAD}','Summary breakdowns','2046-02-01T00:00:00Z',
                '2046-02-01T01:00:00Z'
            );
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES(
                '{THREAD}','{THREAD}','2046-02-01T00:00:00Z',
                '2046-02-01T01:00:00Z',0
            );
            INSERT INTO agent_runs(
                id,thread_id,rollout_id,agent_path,nickname,started_at,status
            ) VALUES
                ('summary-agent-default','{THREAD}','{THREAD}',NULL,NULL,
                 '2046-02-01T00:01:00Z','completed'),
                ('summary-agent-path','{THREAD}','{THREAD}','/agents/path-label',NULL,
                 '2046-02-01T00:02:00Z','running'),
                ('summary-agent-nickname','{THREAD}','{THREAD}','/agents/unused-path','Nick label',
                 '2046-02-01T00:03:00Z','interrupted');
            "#
        ))
        .unwrap();

    for (index, (id, model, effort, total_tokens)) in [
        ("summary-usage-z", "model-z", Some("low"), 500_i64),
        ("summary-usage-a-high", "model-a", Some("high"), 400),
        ("summary-usage-a-low", "model-a", Some("low"), 400),
        ("summary-usage-b", "model-b", None, 400),
    ]
    .into_iter()
    .enumerate()
    {
        connection
            .execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,effort,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES(?1,?2,?2,'2046-02-01T00:10:00Z',?3,?4,?5,?6,0,0,0,?6,1)",
                rusqlite::params![id, THREAD, 100 + index as i64, model, effort, total_tokens],
            )
            .unwrap();
    }

    for (name, count) in [("aaa-high", 3), ("aaa-tie", 2), ("bbb-tie", 2)] {
        for ordinal in 0..count {
            connection
                .execute(
                    "INSERT INTO tool_calls(
                        id,call_id,thread_id,rollout_id,started_at,name,status,duration_ms
                     ) VALUES(?1,?1,?2,?2,'2046-02-01T00:20:00Z',?3,'completed',1)",
                    rusqlite::params![format!("summary-tool-{name}-{ordinal}"), THREAD, name],
                )
                .unwrap();
        }
    }
    for ordinal in 0..99 {
        let name = format!("tool-{ordinal:03}");
        connection
            .execute(
                "INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,started_at,name,status,duration_ms
                 ) VALUES(?1,?1,?2,?2,'2046-02-01T00:30:00Z',?3,'completed',1)",
                rusqlite::params![format!("summary-tool-single-{ordinal:03}"), THREAD, name],
            )
            .unwrap();
    }
    drop(connection);

    let summary = get_json(&harness.app, &format!("/api/v1/sessions/{THREAD}/summary")).await;
    let models = summary["models"].as_array().unwrap();
    assert_eq!(models.len(), 4, "effort is part of model summary identity");
    assert_eq!(
        models
            .iter()
            .map(|row| (
                row["model"].as_str().unwrap(),
                row["effort"].as_str(),
                row["totalTokens"].as_u64().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("model-z", Some("low"), 500),
            ("model-a", Some("high"), 400),
            ("model-a", Some("low"), 400),
            ("model-b", None, 400),
        ]
    );

    let agents = summary["agents"].as_array().unwrap();
    assert_eq!(
        agents
            .iter()
            .map(|row| (row["id"].as_str().unwrap(), row["label"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        vec![
            ("summary-agent-default", "Primary agent"),
            ("summary-agent-path", "/agents/path-label"),
            ("summary-agent-nickname", "Nick label"),
        ]
    );

    let tools = summary["toolSummary"].as_array().unwrap();
    assert_eq!(tools.len(), 100, "tool summary is bounded before transport");
    assert_eq!(
        tools
            .iter()
            .take(3)
            .map(|row| (
                row["tool"].as_str().unwrap(),
                row["count"].as_u64().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![("aaa-high", 3), ("aaa-tie", 2), ("bbb-tie", 2)]
    );
    assert_eq!(tools[3]["tool"], "tool-000");
    assert_eq!(tools[99]["tool"], "tool-096");
    assert!(tools.iter().all(|row| row["tool"] != "tool-097"));
    assert!(tools.iter().all(|row| row["tool"] != "tool-098"));
}

#[tokio::test]
async fn deep_session_breakdowns_never_follow_cross_thread_relation_ids() {
    let harness = harness();
    let summary_uri = format!("/api/v1/sessions/{RICH_SESSION}/summary");
    let activity_uri = format!("/api/v1/sessions/{RICH_SESSION}/activity?page=1&pageSize=25");
    let summary_before = get_json(&harness.app, &summary_uri).await;
    let activity_before = get_json(&harness.app, &activity_uri).await;

    let connection = harness.db.connect().unwrap();
    let agent_run_id: String = connection
        .query_row(
            "SELECT id FROM agent_runs
             WHERE thread_id=?1 AND id<>thread_id ORDER BY started_at LIMIT 1",
            [RICH_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    let turn_id: String = connection
        .query_row(
            "SELECT id FROM turns WHERE thread_id=?1 ORDER BY started_at LIMIT 1",
            [RICH_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    let foreign_rollout_id: String = connection
        .query_row(
            "SELECT id FROM rollouts WHERE thread_id=?1 ORDER BY started_at LIMIT 1",
            [ABORTED_SESSION],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                model,effort,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
             ) VALUES(
                'cross-thread-relation-fixture',?1,?2,?3,?4,
                '2026-07-15T20:59:00Z',9999,'gpt-5.6-sol','ultra',
                900000,800000,90000,10000,990000,1
             )",
            rusqlite::params![ABORTED_SESSION, foreign_rollout_id, turn_id, agent_run_id],
        )
        .unwrap();
    drop(connection);

    let summary_after = get_json(&harness.app, &summary_uri).await;
    let activity_after = get_json(&harness.app, &activity_uri).await;
    assert_eq!(summary_after["agents"], summary_before["agents"]);
    assert_eq!(summary_after["totals"], summary_before["totals"]);
    assert_eq!(summary_after["models"], summary_before["models"]);
    assert_eq!(activity_after["items"], activity_before["items"]);
}

#[tokio::test]
async fn activity_visibility_requires_an_event_message_or_usage_fact() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                ('visibility-event','Event only','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z'),
                ('visibility-message','Message only','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z'),
                ('visibility-usage','Usage only','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z'),
                ('visibility-empty','Empty','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z'),
                ('visibility-tool','Tool only','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z'),
                ('visibility-turn','Turn only','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('visibility-event','visibility-event','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z',0),
                ('visibility-message','visibility-message','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z',0),
                ('visibility-usage','visibility-usage','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z',0),
                ('visibility-empty','visibility-empty','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z',0),
                ('visibility-tool','visibility-tool','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z',0),
                ('visibility-turn','visibility-turn','2043-01-01T00:00:00Z','2043-01-01T00:01:00Z',0);
            INSERT INTO events(
                id,thread_id,rollout_id,timestamp,source_line,kind,body,native
            ) VALUES(
                'visibility-event-row','visibility-event','visibility-event',
                '2043-01-01T00:01:00Z',1,'state','visible',1
            );
            INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
            ) VALUES(
                'visibility-message-row','visibility-message','visibility-message',
                '2043-01-01T00:01:00Z','user','visible',1
            );
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
            ) VALUES(
                'visibility-usage-row','visibility-usage','visibility-usage',
                '2043-01-01T00:01:00Z',1,'visibility-unpriced',1,0,0,0,1,1
            );
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,started_at,name,status
            ) VALUES(
                'visibility-tool-row','visibility-tool-call','visibility-tool',
                'visibility-tool','2043-01-01T00:01:00Z','exec','completed'
            );
            INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
            VALUES(
                'visibility-turn-row','visibility-turn','visibility-turn',
                '2043-01-01T00:01:00Z','completed'
            );
            "#,
        )
        .unwrap();
    drop(connection);

    for thread_id in ["visibility-event", "visibility-message", "visibility-usage"] {
        let (status, _, body) = raw_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/sessions/{thread_id}/activity?page=1&pageSize=25"),
            None,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{thread_id} should be visible: {}",
            String::from_utf8_lossy(&body)
        );
    }

    for thread_id in ["visibility-empty", "visibility-tool", "visibility-turn"] {
        let (status, _, body) = raw_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/sessions/{thread_id}/activity?page=1&pageSize=25"),
            None,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{thread_id} leaked into Activity"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"error": "session not found"})
        );
    }
}

#[tokio::test]
async fn activity_detail_errors_prefer_transport_then_visibility_then_event_lookup() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                ('precedence-visible','Visible','2044-01-01T00:00:00Z','2044-01-01T00:01:00Z'),
                ('precedence-hidden','Hidden','2044-01-01T00:00:00Z','2044-01-01T00:01:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('precedence-visible','precedence-visible','2044-01-01T00:00:00Z','2044-01-01T00:01:00Z',0),
                ('precedence-hidden','precedence-hidden','2044-01-01T00:00:00Z','2044-01-01T00:01:00Z',0);
            INSERT INTO events(
                id,thread_id,rollout_id,timestamp,source_line,kind,body,native
            ) VALUES(
                'precedence-visible-event','precedence-visible','precedence-visible',
                '2044-01-01T00:01:00Z',1,'state','visible',1
            );
            "#,
        )
        .unwrap();
    drop(connection);

    for (uri, expected_status, expected_error) in [
        (
            "/api/v1/sessions/precedence-missing/activity/missing-event?childCursor=not-a-cursor",
            StatusCode::BAD_REQUEST,
            "invalid Activity cursor",
        ),
        (
            "/api/v1/sessions/precedence-missing/activity/missing-event?childPage=9007199254740992",
            StatusCode::BAD_REQUEST,
            "page must not exceed 9007199254740991",
        ),
        (
            "/api/v1/sessions/precedence-missing/activity/missing-event",
            StatusCode::NOT_FOUND,
            "session not found",
        ),
        (
            "/api/v1/sessions/precedence-hidden/activity/missing-event",
            StatusCode::NOT_FOUND,
            "session not found",
        ),
        (
            "/api/v1/sessions/precedence-visible/activity/missing-event",
            StatusCode::NOT_FOUND,
            "activity event not found",
        ),
    ] {
        let (status, _, body) = raw_request(&harness.app, Method::GET, uri, None).await;
        assert_eq!(status, expected_status, "unexpected status for {uri}");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"error": expected_error}),
            "unexpected error for {uri}"
        );
    }
}

#[tokio::test]
async fn activity_detail_reserves_synthetic_ids_before_turns_and_events() {
    const THREAD: &str = "dispatch-collision";
    const ROOT: &str = "collision-root";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('dispatch-collision','Dispatch collision',
                   '2045-01-01T10:00:00Z','2045-01-01T11:00:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('dispatch-collision','dispatch-collision',
                 '2045-01-01T10:00:00Z','2045-01-01T11:00:00Z',0),
                ('dispatch-agent','dispatch-collision',
                 '2045-01-01T10:01:00Z','2045-01-01T10:06:00Z',0),
                ('dispatch-review','dispatch-collision',
                 '2045-01-01T10:02:00Z','2045-01-01T10:07:00Z',0);
            INSERT INTO turns(id,thread_id,rollout_id,started_at,status,model) VALUES
                ('collision-root','dispatch-collision','dispatch-collision',
                 '2045-01-01T10:00:00Z','completed','gpt-5.5'),
                ('dispatch-agent-turn','dispatch-collision','dispatch-agent',
                 '2045-01-01T10:05:00Z','completed','gpt-5.5'),
                ('dispatch-review-turn','dispatch-collision','dispatch-review',
                 '2045-01-01T10:06:00Z','completed','codex-auto-review'),
                ('legacy:dispatch-collision','dispatch-collision','dispatch-collision',
                 '2045-01-01T10:10:00Z','completed','gpt-5.5'),
                ('group:agents:collision-root','dispatch-collision','dispatch-collision',
                 '2045-01-01T10:20:00Z','completed','gpt-5.5'),
                ('group:reviews:collision-root','dispatch-collision','dispatch-collision',
                 '2045-01-01T10:30:00Z','completed','gpt-5.5'),
                ('ordinary-collision','dispatch-collision','dispatch-collision',
                 '2045-01-01T10:40:00Z','completed','gpt-5.5');
            INSERT INTO messages(
                id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
            ) VALUES(
                'dispatch-legacy-message','dispatch-collision','dispatch-collision',NULL,
                '2045-01-01T10:50:00Z','assistant','Legacy fallback',50
            );
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
            ) VALUES
                ('dispatch-visible','dispatch-collision','dispatch-collision','collision-root',
                 '2045-01-01T10:00:00Z',1,'user','user','Visible request',1),
                ('legacy:dispatch-collision','dispatch-collision','dispatch-collision','collision-root',
                 '2045-01-01T10:01:00Z',2,'assistant','assistant','Event collision',1),
                ('group:agents:collision-root','dispatch-collision','dispatch-collision','collision-root',
                 '2045-01-01T10:02:00Z',3,'assistant','assistant','Event collision',1),
                ('group:reviews:collision-root','dispatch-collision','dispatch-collision','collision-root',
                 '2045-01-01T10:03:00Z',4,'assistant','assistant','Event collision',1),
                ('ordinary-collision','dispatch-collision','dispatch-collision','collision-root',
                 '2045-01-01T10:04:00Z',5,'assistant','assistant','Event collision',1);
            "#,
        )
        .unwrap();
    drop(connection);

    let legacy = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/legacy:{THREAD}"),
    )
    .await;
    assert_eq!(legacy["id"], format!("legacy:{THREAD}"));
    assert!(legacy["turnId"].is_null());
    assert!(legacy.get("childPage").is_some());

    let agents = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/group:agents:{ROOT}"),
    )
    .await;
    assert_eq!(agents["kind"], "agent_group");
    assert_eq!(agents["turnId"], ROOT);
    assert_eq!(agents["childTotal"], 1);

    let reviews = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/group:reviews:{ROOT}"),
    )
    .await;
    assert_eq!(reviews["kind"], "review_group");
    assert_eq!(reviews["turnId"], ROOT);
    assert_eq!(reviews["childTotal"], 1);

    let ordinary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/ordinary-collision"),
    )
    .await;
    assert_eq!(ordinary["id"], "ordinary-collision");
    assert_eq!(ordinary["turnId"], "ordinary-collision");
    assert_eq!(ordinary["kind"], "exchange");
    assert!(ordinary["counts"].is_object());
}

#[tokio::test]
async fn activity_falls_back_to_real_messages_and_usage_without_turns() {
    const THREAD: &str = "legacy-activity-without-turns";
    const TITLE_THREAD: &str = "legacy-activity-title-fallback";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('{THREAD}','Legacy activity','2026-07-20T08:00:00Z','2026-07-21T09:00:00Z');
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('{TITLE_THREAD}','A useful stored session title','2026-07-20T09:00:00Z','2026-07-20T09:01:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES('{THREAD}','{THREAD}','2026-07-20T08:00:00Z','2026-07-21T09:00:00Z',0);
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES('{TITLE_THREAD}','{TITLE_THREAD}','2026-07-20T09:00:00Z','2026-07-20T09:01:00Z',0);
            INSERT INTO messages(
                id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
            ) VALUES
                ('legacy-no-turn-user','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:00:00Z','user','Explain the archived project.',1),
                ('legacy-no-turn-final','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:05:00Z','assistant','It is a compact local application.',2);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,
                body,call_id,tool_name,native
            ) VALUES
                ('legacy-no-turn-user-event','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:00:00Z',3,'user','user',NULL,
                 'legacy-no-turn-user',NULL,1),
                ('legacy-no-turn-reasoning','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:01:00Z',4,'reasoning',NULL,'Examining the structure.',
                 NULL,NULL,1),
                ('legacy-no-turn-update','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:02:00Z',5,'update','assistant','I found the entry point.',
                 NULL,NULL,1),
                ('legacy-no-turn-tool-event','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:03:00Z',6,'tool_call',NULL,NULL,
                 'legacy-no-turn-call','exec',1),
                ('legacy-no-turn-final-event','{THREAD}','{THREAD}',NULL,
                 '2026-07-20T08:05:00Z',7,'final','assistant',NULL,
                 'legacy-no-turn-final',NULL,1),
                ('legacy-title-only-update','{TITLE_THREAD}','{TITLE_THREAD}',NULL,
                 '2026-07-20T09:01:00Z',1,'update','assistant','Stored work update.',
                 NULL,NULL,1);
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,turn_id,started_at,name,status
            ) VALUES(
                'legacy-no-turn-tool','legacy-no-turn-call','{THREAD}','{THREAD}',NULL,
                '2026-07-20T08:03:00Z','exec','completed'
            );
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
            ) VALUES(
                'legacy-no-turn-usage','{THREAD}','{THREAD}',NULL,
                '2026-07-21T09:00:00Z',3,'gpt-5.5',100,0,20,0,120,1
            );
            "#
        ))
        .unwrap();
    drop(connection);

    let activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity?page=1&pageSize=25"),
    )
    .await;
    assert_eq!(activity["total"], 1);
    assert_eq!(activity["items"][0]["id"], format!("legacy:{THREAD}"));
    assert_eq!(activity["items"][0]["kind"], "exchange");
    assert_eq!(
        activity["items"][0]["label"],
        "Explain the archived project."
    );
    assert_eq!(
        activity["items"][0]["body"],
        "It is a compact local application."
    );
    assert_eq!(activity["items"][0]["usage"]["totalTokens"], 120);
    assert!(activity["items"][0]["counts"].is_null());
    assert!(activity["items"][0]["durationMs"].is_null());
    assert_eq!(
        activity["days"]
            .as_array()
            .unwrap()
            .iter()
            .map(|day| day["date"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["2026-07-21", "2026-07-20"]
    );
    let clamped = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity?page=999&pageSize=25"),
    )
    .await;
    assert_eq!(clamped["page"], 1);
    assert_eq!(clamped["totalPages"], 1);
    assert_eq!(clamped["items"][0]["id"], format!("legacy:{THREAD}"));

    let detail = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/legacy:{THREAD}"),
    )
    .await;
    let children = detail["children"].as_array().unwrap();
    let kinds = children
        .iter()
        .map(|item| item["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    for expected in ["user", "reasoning", "update", "tool", "final"] {
        assert!(
            kinds.contains(&expected),
            "legacy detail dropped stored {expected} activity: {kinds:?}"
        );
    }
    assert_eq!(
        kinds.iter().filter(|kind| **kind == "final").count(),
        1,
        "a mirrored message and final event must render once"
    );
    assert!(
        children
            .iter()
            .all(|item| item["usage"].is_null() && item["durationMs"].is_null())
    );

    let titled = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{TITLE_THREAD}/activity?page=1&pageSize=25"),
    )
    .await;
    assert_eq!(titled["items"][0]["label"], "A useful stored session title");
}

#[tokio::test]
async fn activity_same_timestamp_roots_prefer_explicit_lineage() {
    const THREAD: &str = "same-timestamp-root-attribution";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('{THREAD}','Duplicate roots','2026-07-22T10:00:00Z','2026-07-23T10:05:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES('{THREAD}','{THREAD}','2026-07-22T10:00:00Z','2026-07-23T10:05:00Z',0);
            INSERT INTO turns(id,thread_id,rollout_id,started_at,completed_at,status,duration_ms)
            VALUES
                ('duplicate-root-a','{THREAD}','{THREAD}','2026-07-22T10:00:00Z','2026-07-22T10:01:00Z','completed',60000),
                ('duplicate-root-b','{THREAD}','{THREAD}','2026-07-22T10:00:00Z','2026-07-22T10:02:00Z','completed',120000);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,payload_json,native
            ) VALUES
                ('duplicate-user-a','{THREAD}','{THREAD}','duplicate-root-a','2026-07-22T10:00:00Z',1,'user','user','Request A',NULL,1),
                ('duplicate-user-b','{THREAD}','{THREAD}','duplicate-root-b','2026-07-22T10:00:00Z',2,'user','user','Request B',NULL,1),
                ('duplicate-spawn-a','{THREAD}','{THREAD}','duplicate-root-a','2026-07-22T10:00:01Z',3,'subagent',NULL,NULL,'{{"agent_thread_id":"duplicate-agent-a"}}',1),
                ('duplicate-spawn-b','{THREAD}','{THREAD}','duplicate-root-b','2026-07-22T10:00:01Z',4,'subagent',NULL,NULL,'{{"agent_thread_id":"duplicate-agent-b"}}',1);
            INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,agent_nickname,started_at,last_event_at,archived
            ) VALUES
                ('duplicate-agent-a','{THREAD}','{THREAD}','{THREAD}','Agent A','2026-07-23T10:00:00Z','2026-07-23T10:01:00Z',0),
                ('duplicate-agent-b','{THREAD}','{THREAD}','{THREAD}','Agent B','2026-07-22T10:00:00Z','2026-07-22T10:01:00Z',0);
            INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,completed_at,status
            ) VALUES
                ('duplicate-agent-a','{THREAD}','duplicate-agent-a','{THREAD}','Agent A','2026-07-23T10:00:00Z','2026-07-23T10:01:00Z','completed'),
                ('duplicate-agent-b','{THREAD}','duplicate-agent-b','{THREAD}','Agent B','2026-07-22T10:00:00Z','2026-07-22T10:01:00Z','completed');
            INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,completed_at,status,duration_ms
            ) VALUES
                ('duplicate-child-a','{THREAD}','duplicate-agent-a','duplicate-agent-a','2026-07-23T10:00:00Z','2026-07-23T10:01:00Z','completed',60000),
                ('duplicate-child-b','{THREAD}','duplicate-agent-b','duplicate-agent-b','2026-07-22T10:00:00Z','2026-07-22T10:01:00Z','completed',60000);
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,turn_id,started_at,name,status
            ) VALUES
                ('duplicate-tool-a','duplicate-call-a','{THREAD}','{THREAD}','duplicate-root-a','2026-07-22T10:00:01Z','exec','completed'),
                ('duplicate-tool-b','duplicate-call-b','{THREAD}','{THREAD}','duplicate-root-b','2026-07-22T10:00:01Z','exec','completed'),
                ('duplicate-tool-unlinked','duplicate-call-unlinked','{THREAD}','{THREAD}',NULL,'2026-07-22T10:00:01Z','exec','completed');
            "#
        ))
        .unwrap();
    drop(connection);

    let activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity?page=1&pageSize=25"),
    )
    .await;
    let items = activity["items"].as_array().unwrap();
    let root_a = items
        .iter()
        .find(|item| item["id"] == "duplicate-root-a")
        .unwrap();
    let root_b = items
        .iter()
        .find(|item| item["id"] == "duplicate-root-b")
        .unwrap();
    assert_eq!(root_a["counts"]["agentRuns"], 1);
    assert_eq!(root_b["counts"]["agentRuns"], 1);
    assert_eq!(root_a["counts"]["toolCalls"], 1);
    assert_eq!(root_b["counts"]["toolCalls"], 2);
    assert_eq!(
        root_a["counts"]["toolCalls"].as_u64().unwrap()
            + root_b["counts"]["toolCalls"].as_u64().unwrap(),
        3,
        "the unlinked fallback belongs to exactly one stable exchange"
    );
    assert!(
        activity["days"]
            .as_array()
            .unwrap()
            .iter()
            .any(|day| day["date"] == "2026-07-23")
    );

    for (root_id, expected_child) in [
        ("duplicate-root-a", "duplicate-child-a"),
        ("duplicate-root-b", "duplicate-child-b"),
    ] {
        let detail = get_json(
            &harness.app,
            &format!("/api/v1/sessions/{THREAD}/activity/{root_id}"),
        )
        .await;
        let group = detail["children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["kind"] == "agent_group")
            .unwrap();
        assert!(group["children"].as_array().unwrap().is_empty());
        assert_eq!(group["childTotal"], 1);
        let group = get_json(
            &harness.app,
            &format!(
                "/api/v1/sessions/{THREAD}/activity/{}",
                group["id"].as_str().unwrap()
            ),
        )
        .await;
        assert_eq!(group["children"].as_array().unwrap().len(), 1);
        assert_eq!(group["children"][0]["id"], expected_child);
    }
}

#[tokio::test]
async fn activity_reused_agent_identity_is_attributed_by_link_interval() {
    const THREAD: &str = "reused-agent-attribution";
    const AGENT: &str = "reused-agent";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('{THREAD}','Reused agent','2026-07-26T10:00:00Z','2026-07-26T10:21:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES('{THREAD}','{THREAD}','2026-07-26T10:00:00Z','2026-07-26T10:21:00Z',0);
            INSERT INTO turns(id,thread_id,rollout_id,started_at,completed_at,status,duration_ms)
            VALUES
                ('reuse-root-a','{THREAD}','{THREAD}','2026-07-26T10:00:00Z','2026-07-26T10:01:00Z','completed',60000),
                ('reuse-root-b','{THREAD}','{THREAD}','2026-07-26T10:10:00Z','2026-07-26T10:11:00Z','completed',60000),
                ('reuse-root-c','{THREAD}','{THREAD}','2026-07-26T10:20:00Z','2026-07-26T10:21:00Z','completed',60000);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,
                kind,role,body,status,payload_json,native
            ) VALUES
                ('reuse-user-a','{THREAD}','{THREAD}','reuse-root-a','2026-07-26T10:00:00Z',1,'user','user','Request A',NULL,NULL,1),
                ('reuse-final-a','{THREAD}','{THREAD}','reuse-root-a','2026-07-26T10:00:01Z',2,'final','assistant','Answer A',NULL,NULL,1),
                ('reuse-link-a','{THREAD}','{THREAD}','reuse-root-a','2026-07-26T10:00:05Z',4,'subagent',NULL,NULL,'interacted','{{"agent_thread_id":"{AGENT}"}}',1),
                ('reuse-link-a-again','{THREAD}','{THREAD}','reuse-root-a','2026-07-26T10:00:07Z',5,'subagent',NULL,NULL,'interacted','{{"agent_thread_id":"{AGENT}"}}',1),
                ('reuse-user-b','{THREAD}','{THREAD}','reuse-root-b','2026-07-26T10:10:00Z',11,'user','user','Request B',NULL,NULL,1),
                ('reuse-final-b','{THREAD}','{THREAD}','reuse-root-b','2026-07-26T10:10:01Z',12,'final','assistant','Answer B',NULL,NULL,1),
                ('reuse-link-b','{THREAD}','{THREAD}','reuse-root-b','2026-07-26T10:10:05Z',14,'subagent',NULL,NULL,'interacted','{{"agent_thread_id":"{AGENT}"}}',1),
                ('reuse-user-c','{THREAD}','{THREAD}','reuse-root-c','2026-07-26T10:20:00Z',21,'user','user','Request C',NULL,NULL,1),
                ('reuse-final-c','{THREAD}','{THREAD}','reuse-root-c','2026-07-26T10:20:01Z',22,'final','assistant','Answer C',NULL,NULL,1),
                ('reuse-link-c','{THREAD}','{THREAD}','reuse-root-c','2026-07-26T10:20:05Z',24,'subagent',NULL,NULL,'interacted','{{"agent_thread_id":"{AGENT}"}}',1);
            INSERT INTO rollouts(
                id,thread_id,parent_rollout_id,parent_thread_id,agent_nickname,
                started_at,last_event_at,archived
            ) VALUES(
                '{AGENT}','{THREAD}','{THREAD}','{THREAD}','Reusable',
                '2026-07-26T10:00:05Z','2026-07-26T10:20:30Z',0
            );
            INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,nickname,
                started_at,completed_at,status
            ) VALUES(
                '{AGENT}','{THREAD}','{AGENT}','{THREAD}','Reusable',
                '2026-07-26T10:00:05Z','2026-07-26T10:20:30Z','completed'
            );
            INSERT INTO turns(
                id,thread_id,rollout_id,agent_run_id,started_at,completed_at,
                status,model,effort,duration_ms
            ) VALUES
                ('reuse-child-a1','{THREAD}','{AGENT}','{AGENT}','2026-07-26T10:00:06Z','2026-07-26T10:00:06.500Z','completed','gpt-5.6-sol','high',500),
                ('reuse-child-a2','{THREAD}','{AGENT}','{AGENT}','2026-07-26T10:00:08Z','2026-07-26T10:00:08.500Z','completed','gpt-5.6-sol','high',500),
                ('reuse-child-b','{THREAD}','{AGENT}','{AGENT}','2026-07-26T10:10:06Z','2026-07-26T10:10:06.500Z','completed','gpt-5.6-sol','high',500),
                ('reuse-child-c','{THREAD}','{AGENT}','{AGENT}','2026-07-26T10:20:06Z','2026-07-26T10:20:06.500Z','completed','gpt-5.6-sol','high',500);
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                model,effort,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
            ) VALUES
                ('reuse-root-usage-a','{THREAD}','{THREAD}','reuse-root-a',NULL,'2026-07-26T10:00:02Z',3,'gpt-5.6-sol','high',0,0,10,0,10,1),
                ('reuse-root-usage-b','{THREAD}','{THREAD}','reuse-root-b',NULL,'2026-07-26T10:10:02Z',13,'gpt-5.6-sol','high',0,0,10,0,10,1),
                ('reuse-root-usage-c','{THREAD}','{THREAD}','reuse-root-c',NULL,'2026-07-26T10:20:02Z',23,'gpt-5.6-sol','high',0,0,10,0,10,1),
                ('reuse-child-usage-a1','{THREAD}','{AGENT}','reuse-child-a1','{AGENT}','2026-07-26T10:00:06.400Z',101,'gpt-5.6-sol','high',0,0,100,0,100,1),
                ('reuse-child-usage-a2','{THREAD}','{AGENT}','reuse-child-a2','{AGENT}','2026-07-26T10:00:08.400Z',102,'gpt-5.6-sol','high',0,0,110,0,110,1),
                ('reuse-child-usage-b','{THREAD}','{AGENT}','reuse-child-b','{AGENT}','2026-07-26T10:10:06.400Z',103,'gpt-5.6-sol','high',0,0,200,0,200,1),
                ('reuse-child-usage-c','{THREAD}','{AGENT}','reuse-child-c','{AGENT}','2026-07-26T10:20:06.400Z',104,'gpt-5.6-sol','high',0,0,300,0,300,1);
            "#
        ))
        .unwrap();
    drop(connection);

    let summary = get_json(&harness.app, &format!("/api/v1/sessions/{THREAD}/summary")).await;
    assert_eq!(summary["totals"]["totalTokens"], 740);

    let activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity?page=1&pageSize=25"),
    )
    .await;
    let exchanges = activity["items"].as_array().unwrap();
    assert_eq!(exchanges.len(), 3);
    assert_eq!(
        exchanges
            .iter()
            .map(|exchange| exchange["usage"]["totalTokens"].as_u64().unwrap())
            .sum::<u64>(),
        summary["totals"]["totalTokens"].as_u64().unwrap(),
        "reusing one agent identity must not duplicate its usage across exchanges"
    );

    for (root_id, expected_tokens, expected_children) in [
        (
            "reuse-root-a",
            220,
            vec!["reuse-child-a1", "reuse-child-a2"],
        ),
        ("reuse-root-b", 210, vec!["reuse-child-b"]),
        ("reuse-root-c", 310, vec!["reuse-child-c"]),
    ] {
        let exchange = exchanges
            .iter()
            .find(|exchange| exchange["id"] == root_id)
            .unwrap();
        assert_eq!(exchange["usage"]["totalTokens"], expected_tokens);
        assert_eq!(exchange["counts"]["agentRuns"], 1);

        let detail = get_json(
            &harness.app,
            &format!("/api/v1/sessions/{THREAD}/activity/{root_id}"),
        )
        .await;
        let group = detail["children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["kind"] == "agent_group")
            .unwrap();
        assert_eq!(group["childTotal"], expected_children.len());

        for field in [
            "inputTokens",
            "cachedInputTokens",
            "outputTokens",
            "reasoningTokens",
            "totalTokens",
            "unpricedTokens",
        ] {
            assert_eq!(
                detail["children"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|item| item["usage"][field].as_u64())
                    .sum::<u64>(),
                detail["usage"][field].as_u64().unwrap(),
                "visible children for {root_id} must reconcile on {field}"
            );
        }
        assert_eq!(
            detail["children"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| !item["usage"]["costUsd"].is_null())
                .map(|item| usd_units(&item["usage"]["costUsd"]))
                .sum::<i128>(),
            usd_units(&detail["usage"]["costUsd"]),
            "visible child costs must reconcile exactly for {root_id}",
        );

        let group = get_json(
            &harness.app,
            &format!(
                "/api/v1/sessions/{THREAD}/activity/{}",
                group["id"].as_str().unwrap()
            ),
        )
        .await;
        let child_ids = group["children"]
            .as_array()
            .unwrap()
            .iter()
            .map(|child| child["id"].as_str().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(
            child_ids,
            expected_children.into_iter().collect::<HashSet<_>>(),
            "each reused-agent turn belongs only to the exchange whose link interval contains it"
        );
        assert_eq!(
            group["children"]
                .as_array()
                .unwrap()
                .iter()
                .map(|child| child["usage"]["totalTokens"].as_u64().unwrap())
                .sum::<u64>(),
            group["usage"]["totalTokens"].as_u64().unwrap()
        );
    }
}

#[tokio::test]
async fn activity_attributes_null_turn_usage_once_across_root_pages() {
    const THREAD: &str = "null-turn-usage-attribution";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES(
                '{THREAD}','Unlinked usage','2026-07-27T09:50:00Z','2026-07-27T10:20:00Z'
            );
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
            VALUES(
                '{THREAD}','{THREAD}','2026-07-27T09:50:00Z','2026-07-27T10:20:00Z',0
            );
            INSERT INTO turns(
                id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
            ) VALUES
                ('null-root-a','{THREAD}','{THREAD}',
                 '2026-07-27T10:00:00Z','2026-07-27T10:01:00Z','completed',60000),
                ('null-root-b','{THREAD}','{THREAD}',
                 '2026-07-27T10:10:00Z','2026-07-27T10:11:00Z','completed',60000);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
            ) VALUES
                ('null-user-a','{THREAD}','{THREAD}','null-root-a',
                 '2026-07-27T10:00:00Z',10,'user','user','Request A',1),
                ('null-user-b','{THREAD}','{THREAD}','null-root-b',
                 '2026-07-27T10:10:00Z',20,'user','user','Request B',1);
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,turn_id,started_at,name,status
            ) VALUES
                ('null-tool-linked-a','null-call-linked-a','{THREAD}','{THREAD}',
                 'null-root-a','2026-07-27T10:00:02Z','exec','completed'),
                ('null-tool-linked-b','null-call-linked-b','{THREAD}','{THREAD}',
                 'null-root-b','2026-07-27T10:10:02Z','exec','completed'),
                ('null-tool-before-first','null-call-before-first','{THREAD}','{THREAD}',
                 NULL,'2026-07-27T09:56:00Z','exec','completed'),
                ('null-tool-between-roots','null-call-between-roots','{THREAD}','{THREAD}',
                 NULL,'2026-07-27T10:06:00Z','exec','completed'),
                ('null-tool-at-boundary','null-call-at-boundary','{THREAD}','{THREAD}',
                 NULL,'2026-07-27T10:10:00Z','exec','completed'),
                ('null-tool-after-last','null-call-after-last','{THREAD}','{THREAD}',
                 NULL,'2026-07-27T10:19:00Z','exec','completed');
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,
                model,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens,native
            ) VALUES
                ('null-linked-a','{THREAD}','{THREAD}','null-root-a',
                 '2026-07-27T10:00:01Z',11,'gpt-5.5',0,0,100,0,100,1),
                ('null-linked-b','{THREAD}','{THREAD}','null-root-b',
                 '2026-07-27T10:10:01Z',21,'gpt-5.5',0,0,200,0,200,1),
                ('null-before-first','{THREAD}','{THREAD}',NULL,
                 '2026-07-27T09:55:00Z',1,'gpt-5.5',0,0,1,0,1,1),
                ('null-between-roots','{THREAD}','{THREAD}',NULL,
                 '2026-07-27T10:05:00Z',12,'gpt-5.5',0,0,2,0,2,1),
                ('null-at-boundary','{THREAD}','{THREAD}',NULL,
                 '2026-07-27T10:10:00Z',19,'gpt-5.5',0,0,4,0,4,1),
                ('null-after-last','{THREAD}','{THREAD}',NULL,
                 '2026-07-27T10:20:00Z',30,'gpt-5.5',0,0,8,0,8,1);
            "#
        ))
        .unwrap();
    drop(connection);

    let summary = get_json(&harness.app, &format!("/api/v1/sessions/{THREAD}/summary")).await;
    assert_eq!(summary["totals"]["totalTokens"], 315);

    let mut exchanges = HashMap::new();
    let mut expected_days = None;
    for page in 1..=2 {
        let activity = get_json(
            &harness.app,
            &format!("/api/v1/sessions/{THREAD}/activity?page={page}&pageSize=1"),
        )
        .await;
        assert_eq!(activity["total"], 2);
        assert_eq!(activity["totalPages"], 2);
        if let Some(expected) = expected_days.as_ref() {
            assert_eq!(
                &activity["days"], expected,
                "day summaries must not depend on the selected root page"
            );
        } else {
            expected_days = Some(activity["days"].clone());
        }
        let exchange = activity["items"][0].clone();
        exchanges.insert(exchange["id"].as_str().unwrap().to_owned(), exchange);
    }

    let unpaginated = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity?page=1&pageSize=25"),
    )
    .await;
    let expected_days = expected_days.unwrap();
    assert_eq!(unpaginated["days"], expected_days);
    assert_eq!(expected_days.as_array().unwrap().len(), 1);
    assert_eq!(expected_days[0]["date"], "2026-07-27");
    assert_eq!(expected_days[0]["durationMs"], 120_000);
    assert_eq!(expected_days[0]["totals"]["totalTokens"], 315);
    assert_eq!(expected_days[0]["totals"], summary["totals"]);

    for (root_id, expected_tokens, expected_model_calls) in
        [("null-root-a", 103, 3), ("null-root-b", 212, 3)]
    {
        let exchange = &exchanges[root_id];
        assert_eq!(exchange["usage"]["outputTokens"], expected_tokens);
        assert_eq!(exchange["usage"]["totalTokens"], expected_tokens);
        assert_eq!(exchange["counts"]["modelCalls"], expected_model_calls);
        assert_eq!(exchange["counts"]["toolCalls"], 3);

        let detail = get_json(
            &harness.app,
            &format!("/api/v1/sessions/{THREAD}/activity/{root_id}"),
        )
        .await;
        assert_eq!(detail["usage"]["outputTokens"], expected_tokens);
        assert_eq!(detail["usage"]["totalTokens"], expected_tokens);
        assert_eq!(detail["counts"]["modelCalls"], expected_model_calls);
        assert_eq!(detail["counts"]["toolCalls"], 3);
    }

    assert_eq!(
        exchanges
            .values()
            .map(|exchange| exchange["usage"]["totalTokens"].as_u64().unwrap())
            .sum::<u64>(),
        summary["totals"]["totalTokens"].as_u64().unwrap(),
        "every linked and null-turn usage fact belongs to exactly one exchange across pages"
    );
    assert_eq!(
        exchanges
            .values()
            .map(|exchange| exchange["counts"]["modelCalls"].as_u64().unwrap())
            .sum::<u64>(),
        6,
        "the model-call count also reconciles without dropping or duplicating unlinked facts"
    );
    assert_eq!(
        exchanges
            .values()
            .map(|exchange| exchange["counts"]["toolCalls"].as_u64().unwrap())
            .sum::<u64>(),
        6,
        "linked and orphan tool calls use the same complete exchange timeline"
    );
}

#[tokio::test]
async fn activity_metadata_and_detail_joins_are_thread_scoped() {
    const LOCAL: &str = "activity-local-thread";
    const FOREIGN: &str = "activity-foreign-thread";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                ('{LOCAL}','Local','2026-07-25T10:00:00Z','2026-07-25T10:05:00Z'),
                ('{FOREIGN}','Foreign','2026-07-25T10:00:00Z','2026-07-25T10:05:00Z');
            INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('{LOCAL}','{LOCAL}','2026-07-25T10:00:00Z','2026-07-25T10:05:00Z',0),
                ('{FOREIGN}','{FOREIGN}','2026-07-25T10:00:00Z','2026-07-25T10:05:00Z',0);
            INSERT INTO agent_runs(id,thread_id,rollout_id,nickname,started_at,status)
            VALUES('foreign-agent-run','{FOREIGN}','{FOREIGN}','Foreign nickname','2026-07-25T10:00:00Z','running');
            INSERT INTO turns(id,thread_id,rollout_id,agent_run_id,started_at,status)
            VALUES('local-root-turn','{LOCAL}','{LOCAL}','foreign-agent-run','2026-07-25T10:00:00Z','running');
            INSERT INTO messages(
                id,thread_id,rollout_id,timestamp,role,content,source_line
            ) VALUES('foreign-message','{FOREIGN}','{FOREIGN}','2026-07-25T10:01:00Z','assistant','FOREIGN MESSAGE',1);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,call_id,tool_name,native
            ) VALUES
                ('local-user','{LOCAL}','{LOCAL}','local-root-turn','2026-07-25T10:00:00Z',1,'user','user','Local request',NULL,NULL,1),
                ('local-message-link','{LOCAL}','{LOCAL}','local-root-turn','2026-07-25T10:01:00Z',2,'message','assistant',NULL,'foreign-message',NULL,1),
                ('local-tool-link','{LOCAL}','{LOCAL}','local-root-turn','2026-07-25T10:02:00Z',3,'tool_call',NULL,NULL,'shared-call','safe-tool',1),
                ('local-complete','{LOCAL}','{LOCAL}','local-root-turn','2026-07-25T10:03:00Z',4,'turn_completed',NULL,'Local completion',NULL,NULL,1),
                ('foreign-final','{FOREIGN}','{FOREIGN}','local-root-turn','2026-07-25T10:04:00Z',5,'final','assistant','FOREIGN FINAL',NULL,NULL,1);
            INSERT INTO tool_calls(
                id,call_id,thread_id,rollout_id,started_at,name,status
            ) VALUES('foreign-tool','shared-call','{FOREIGN}','{LOCAL}','2026-07-25T10:02:00Z','foreign-tool','completed');
            "#
        ))
        .unwrap();
    drop(connection);

    let activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{LOCAL}/activity?page=1&pageSize=25"),
    )
    .await;
    assert!(activity["items"][0]["agentLabel"].is_null());
    let detail = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{LOCAL}/activity/local-root-turn"),
    )
    .await;
    let children = detail["children"].as_array().unwrap();
    assert!(children.iter().any(|item| item["id"] == "local-complete"));
    assert!(!detail.to_string().contains("FOREIGN"));
    assert!(
        children
            .iter()
            .find(|item| item["id"] == "local-message-link")
            .unwrap()["body"]
            .is_null()
    );
    assert!(
        children
            .iter()
            .find(|item| item["id"] == "local-tool-link")
            .unwrap()["body"]
            .is_null()
    );
}

#[tokio::test]
async fn activity_detail_pages_large_event_streams_and_lazy_groups() {
    const THREAD: &str = "activity-paged-thread";
    const ROOT: &str = "activity-paged-root";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(&format!(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('{THREAD}','Paged activity','2026-07-25T10:00:00Z','2026-07-25T10:10:00Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('{THREAD}','{THREAD}','2026-07-25T10:00:00Z','2026-07-25T10:10:00Z',0);
             INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
             VALUES('{ROOT}','{THREAD}','{THREAD}','2026-07-25T10:00:00Z','completed');"
        ))
        .unwrap();
    for index in 0..620 {
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,
                    kind,role,body,native
                 ) VALUES(?1,?2,?2,?3,'2026-07-25T10:05:00Z',?4,
                          'assistant','assistant',?5,1)",
                rusqlite::params![
                    format!("paged-event-{index:04}"),
                    THREAD,
                    ROOT,
                    index,
                    format!("event {index}"),
                ],
            )
            .unwrap();
    }
    drop(connection);

    let first = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{ROOT}"),
    )
    .await;
    assert_eq!(first["childPage"], 1);
    assert_eq!(first["childPageSize"], 250);
    assert_eq!(first["childTotal"], 620);
    assert_eq!(first["childHasMore"], true);
    assert_eq!(first["children"].as_array().unwrap().len(), 250);
    assert_eq!(first["children"][0]["id"], "paged-event-0619");

    let second = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{ROOT}?childPage=2&childPageSize=200"),
    )
    .await;
    assert_eq!(second["childPage"], 2);
    assert_eq!(second["childPageSize"], 200);
    assert_eq!(second["childTotal"], 620);
    assert_eq!(second["childHasMore"], true);
    assert_eq!(second["children"].as_array().unwrap().len(), 200);
    assert_eq!(second["children"][0]["id"], "paged-event-0419");

    let last = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{ROOT}?childPage=4&childPageSize=200"),
    )
    .await;
    assert_eq!(last["childPage"], 4);
    assert_eq!(last["childHasMore"], false);
    assert_eq!(last["children"].as_array().unwrap().len(), 20);

    let clamped = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{ROOT}?childPage=1&childPageSize=5000"),
    )
    .await;
    assert_eq!(clamped["childPageSize"], 500);
    assert_eq!(clamped["children"].as_array().unwrap().len(), 500);
}

#[tokio::test]
async fn activity_review_group_cursor_is_stable_and_totals_ignore_child_pages() {
    const THREAD: &str = "review-cursor-thread";
    const ROOT: &str = "review-cursor-root";
    const GROUP: &str = "group:reviews:review-cursor-root";
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            r#"
            INSERT INTO threads(id,title,started_at,last_event_at)
            VALUES('review-cursor-thread','Review cursor',
                   '2046-01-01T10:00:00Z','2046-01-01T10:10:00Z');
            INSERT INTO rollouts(id,thread_id,parent_rollout_id,parent_thread_id,
                                 started_at,last_event_at,archived) VALUES
                ('review-cursor-thread','review-cursor-thread',NULL,NULL,
                 '2046-01-01T10:00:00Z','2046-01-01T10:10:00Z',0),
                ('review-cursor-child','review-cursor-thread','review-cursor-thread',
                 'review-cursor-thread','2046-01-01T10:01:00Z',
                 '2046-01-01T10:10:00Z',0);
            INSERT INTO turns(id,thread_id,rollout_id,started_at,status,model,duration_ms) VALUES
                ('review-cursor-root','review-cursor-thread','review-cursor-thread',
                 '2046-01-01T10:00:00Z','completed','gpt-5.5',1000),
                ('review-000','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:05:00Z','completed','codex-auto-review',1000),
                ('review-001','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:05:00Z','completed','codex-auto-review',1000),
                ('review-002','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:05:00Z','completed','codex-auto-review',1000),
                ('review-003','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:05:00Z','completed','codex-auto-review',1000),
                ('review-004','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:05:00Z','completed','codex-auto-review',1000),
                ('review-005','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:05:00Z','completed','codex-auto-review',1000),
                ('review-agent-000','review-cursor-thread','review-cursor-child',
                 '2046-01-01T10:04:00Z','completed','gpt-5.5',1000);
            INSERT INTO events(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
            ) VALUES(
                'review-cursor-visible','review-cursor-thread','review-cursor-thread',
                'review-cursor-root','2046-01-01T10:00:00Z',1,
                'user','user','Inspect the reviews',1
            );
            INSERT INTO usage_facts(
                id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens,native
            ) VALUES
                ('review-usage-000','review-cursor-thread','review-cursor-child','review-000','2046-01-01T10:05:00Z',1,'codex-auto-review',10,0,0,0,10,1),
                ('review-usage-001','review-cursor-thread','review-cursor-child','review-001','2046-01-01T10:05:00Z',2,'codex-auto-review',10,0,0,0,10,1),
                ('review-usage-002','review-cursor-thread','review-cursor-child','review-002','2046-01-01T10:05:00Z',3,'codex-auto-review',10,0,0,0,10,1),
                ('review-usage-003','review-cursor-thread','review-cursor-child','review-003','2046-01-01T10:05:00Z',4,'codex-auto-review',10,0,0,0,10,1),
                ('review-usage-004','review-cursor-thread','review-cursor-child','review-004','2046-01-01T10:05:00Z',5,'codex-auto-review',10,0,0,0,10,1),
                ('review-usage-005','review-cursor-thread','review-cursor-child','review-005','2046-01-01T10:05:00Z',6,'codex-auto-review',10,0,0,0,10,1);
            "#,
        )
        .unwrap();
    drop(connection);

    let root = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{ROOT}?childPageSize=2"),
    )
    .await;
    let placeholder = root["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["kind"] == "review_group")
        .expect("root detail must expose a lazy review group");
    assert_eq!(placeholder["childTotal"], 6);
    assert_eq!(placeholder["childHasMore"], true);
    assert_eq!(placeholder["children"], json!([]));
    assert_eq!(placeholder["usage"]["totalTokens"], 60);

    let page_one = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{GROUP}?childPage=1&childPageSize=2"),
    )
    .await;
    let cursor_one = page_one["childNextCursor"].as_str().unwrap().to_owned();
    let page_two_numeric = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{THREAD}/activity/{GROUP}?childPage=2&childPageSize=2"),
    )
    .await;
    let page_two = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{THREAD}/activity/{GROUP}?childPage=2&childPageSize=2&childCursor={}",
            query_component(&cursor_one)
        ),
    )
    .await;
    let cursor_two = page_two["childNextCursor"].as_str().unwrap().to_owned();
    let page_three = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{THREAD}/activity/{GROUP}?childPage=3&childPageSize=2&childCursor={}",
            query_component(&cursor_two)
        ),
    )
    .await;

    let child_ids = |page: &Value| {
        page["children"]
            .as_array()
            .unwrap()
            .iter()
            .map(|child| child["id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(child_ids(&page_one), ["review-005", "review-004"]);
    assert_eq!(child_ids(&page_two), ["review-003", "review-002"]);
    assert_eq!(child_ids(&page_three), ["review-001", "review-000"]);
    assert_eq!(child_ids(&page_two_numeric), child_ids(&page_two));
    for page in [&page_one, &page_two_numeric, &page_two, &page_three] {
        assert!(
            page["children"]
                .as_array()
                .unwrap()
                .iter()
                .all(|child| child["kind"] == "review")
        );
        assert_eq!(page["childPageSize"], 2);
        assert_eq!(page["usage"]["totalTokens"], 60);
        assert_eq!(page["usage"], page_one["usage"]);
        assert_eq!(page["status"], "completed");
        assert_eq!(page["durationMs"], 1000);
    }
    assert!(page_three.get("childNextCursor").is_none());

    for (page, number, has_more) in [
        (&page_one, 1, true),
        (&page_two, 2, true),
        (&page_three, 3, false),
    ] {
        assert_eq!(page["childPage"], number);
        assert_eq!(page["childTotal"], 6);
        assert_eq!(page["childHasMore"], has_more);
    }

    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO turns(id,thread_id,rollout_id,started_at,status,model,duration_ms)
             VALUES('review-999',?1,'review-cursor-child','2046-01-01T10:05:01Z',
                    'completed','codex-auto-review',1000)",
            [THREAD],
        )
        .unwrap();
    drop(connection);
    let stable_continuation = get_json(
        &harness.app,
        &format!(
            "/api/v1/sessions/{THREAD}/activity/{GROUP}?childPage=2&childPageSize=2&childCursor={}",
            query_component(&cursor_one)
        ),
    )
    .await;
    assert_eq!(
        child_ids(&stable_continuation),
        ["review-003", "review-002"],
        "a newer review must not repeat or displace older cursor results"
    );
    assert_eq!(stable_continuation["childTotal"], 7);

    let (status, _, body) = raw_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/sessions/{THREAD}/activity/group:agents:{ROOT}?childPageSize=2&childCursor={}",
            query_component(&cursor_one)
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"error": "invalid Activity cursor"})
    );
}

#[tokio::test]
async fn analytics_validation_errors_and_all_time_anchor_ignoring_are_exact() {
    let harness = harness();

    for (uri, expected_error) in [
        (
            "/api/v1/stats?range=quarter",
            "range must be day, week, month, year, or all",
        ),
        (
            "/api/v1/stats?range=day&anchor=not-a-date",
            "expected a YYYY-MM-DD date",
        ),
        (
            "/api/v1/stats?range=day&anchor=1969-12-31",
            "year must be between 1970 and 9998",
        ),
        (
            "/api/v1/stats?range=day&anchor=9999-12-31",
            "year must be between 1970 and 9998",
        ),
        (
            "/api/v1/stats?range=week&anchor=1970-01-01",
            "weekly anchor must be on or after 1970-01-05",
        ),
        (
            "/api/v1/overview/year?year=10000",
            "year must be between 1970 and 9998",
        ),
    ] {
        let (status, headers, body) = raw_request(&harness.app, Method::GET, uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.split(';').next().unwrap()),
            Some("application/json"),
            "{uri}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({ "error": expected_error }),
            "{uri}"
        );
    }

    let (status, headers, body) =
        raw_request(&harness.app, Method::GET, "/api/v1/stats?range=all", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap()),
        Some("application/json")
    );
    let baseline = serde_json::from_slice::<Value>(&body).unwrap();
    assert_eq!(baseline["range"], "all");

    for uri in [
        "/api/v1/stats?range=all&anchor=not-a-date",
        "/api/v1/stats?range=all&anchor=9999-12-31",
    ] {
        let (status, headers, body) = raw_request(&harness.app, Method::GET, uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.split(';').next().unwrap()),
            Some("application/json"),
            "{uri}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            baseline,
            "all-time anchors must be ignored before validation: {uri}"
        );
    }
}

#[tokio::test]
async fn stats_cover_every_range_and_rows_drill_down_without_bucket_guessing() {
    let harness = harness();
    for (range, anchor, expected_rows) in [
        ("day", "2026-07-15", 24),
        ("week", "2026-07-15", 7),
        ("month", "2026-07-15", 31),
        ("year", "2026-07-15", 12),
    ] {
        let response = get_json(
            &harness.app,
            &format!("/api/v1/stats?range={range}&anchor={anchor}"),
        )
        .await;
        assert_eq!(response["range"], range);
        let expected_anchor = match range {
            "week" => "2026-07-13",
            "month" => "2026-07-01",
            "year" => "2026-01-01",
            _ => anchor,
        };
        assert_eq!(response["anchor"], expected_anchor);
        assert!(response["label"].is_string());
        assert_totals(&response["totals"]);
        assert_eq!(response["rows"].as_array().unwrap().len(), expected_rows);
        assert_eq!(response["trend"].as_array().unwrap().len(), expected_rows);
        if response["totals"]["unpricedTokens"].as_u64().unwrap() > 0 {
            assert!(response["totals"]["costUsd"].is_null());
        }
        for (index, row) in response["rows"].as_array().unwrap().iter().enumerate() {
            assert!(row["periodStart"].is_string());
            assert!(row["periodEnd"].is_string());
            assert!(row["label"].is_string());
            assert!(row["sessionCount"].is_number());
            assert_totals(row);
            if row["unpricedTokens"].as_u64().unwrap() > 0 {
                assert!(row["costUsd"].is_null());
                assert!(response["trend"][index].is_null());
            } else {
                let _ = usd_units(&response["trend"][index]);
            }
        }
    }

    let midweek = get_json(&harness.app, "/api/v1/stats?range=week&anchor=2026-07-15").await;
    assert_eq!(midweek["anchor"], "2026-07-13");
    assert_eq!(midweek["label"], "Week of Jul 13, 2026");
    assert_eq!(midweek["rows"][0]["label"], "Mon 13");

    let first_public_week =
        get_json(&harness.app, "/api/v1/stats?range=week&anchor=1970-01-05").await;
    assert_eq!(first_public_week["anchor"], "1970-01-05");
    assert_eq!(first_public_week["rows"].as_array().unwrap().len(), 7);

    for (range, anchor, expected) in [
        ("month", "2026-06-30", "2026-06-01"),
        ("month", "2024-02-29", "2024-02-01"),
        ("year", "2025-12-31", "2025-01-01"),
        ("year", "2024-02-29", "2024-01-01"),
    ] {
        let response = get_json(
            &harness.app,
            &format!("/api/v1/stats?range={range}&anchor={anchor}"),
        )
        .await;
        assert_eq!(response["anchor"], expected);
    }

    let all = get_json(&harness.app, "/api/v1/stats?range=all").await;
    let today = Local::now().date_naive().to_string();
    assert_eq!(all["range"], "all");
    assert_eq!(all["anchor"], today);
    assert_eq!(
        all["rows"].as_array().unwrap().len(),
        (Local::now().year() - 2025 + 1) as usize
    );
    assert_eq!(
        all["trend"].as_array().unwrap().len(),
        all["rows"].as_array().unwrap().len()
    );
    assert_totals(&all["totals"]);
    assert!(all["totals"]["costUsd"].is_null());
    for (index, row) in all["rows"].as_array().unwrap().iter().enumerate() {
        if row["unpricedTokens"].as_u64().unwrap() > 0 {
            assert!(row["costUsd"].is_null());
            assert!(all["trend"][index].is_null());
        } else {
            let _ = usd_units(&row["costUsd"]);
            let _ = usd_units(&all["trend"][index]);
        }
    }
    for ignored_anchor in ["2020-01-01", "9999-01-01", "not-a-date"] {
        let anchored = get_json(
            &harness.app,
            &format!("/api/v1/stats?range=all&anchor={ignored_anchor}"),
        )
        .await;
        assert_eq!(anchored["anchor"], today);
        assert_eq!(anchored["rows"], all["rows"]);
        assert_eq!(anchored["totals"], all["totals"]);
    }

    let day = get_json(&harness.app, "/api/v1/stats?range=day&anchor=2026-07-15").await;
    assert_eq!(day["totals"]["totalTokens"], 387_708);
    assert_eq!(day["totals"]["unpricedTokens"], 25_607);
    assert!(day["totals"]["costUsd"].is_null());
}

#[tokio::test]
async fn stats_keep_public_year_bounds_and_fractional_dst_labels() {
    const CHILD_MARKER: &str = "CODEX_USAGE_STATS_TIMEZONE_CHILD";
    const TEST_NAME: &str = "stats_keep_public_year_bounds_and_fractional_dst_labels";

    if std::env::var_os(CHILD_MARKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD_MARKER, "1")
            .env("TZ", "Australia/Lord_Howe")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "timezone-isolated regression failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let upper_timestamp = DateTime::parse_from_rfc3339("9998-12-31T23:30:00Z").unwrap();
    assert_eq!(
        upper_timestamp.with_timezone(&Local).year(),
        9999,
        "the isolated test process must use a positive-offset timezone"
    );

    let harness = harness();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "INSERT INTO threads(id,title,started_at,last_event_at) VALUES
                ('upper-bound-thread','Upper bound','9998-12-31T23:30:00.000000000Z',
                 '9998-12-31T23:30:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                ('upper-bound-rollout','upper-bound-thread',
                 '9998-12-31T23:30:00.000000000Z',
                 '9998-12-31T23:30:00.000000000Z',0);
             INSERT INTO events(
                id,thread_id,rollout_id,timestamp,source_line,kind,native
             ) VALUES(
                'upper-bound-event','upper-bound-thread','upper-bound-rollout',
                '9998-12-31T23:30:00.000000000Z',1,'state',1
             );",
        )
        .unwrap();

    let all = get_json(&harness.app, "/api/v1/stats?range=all").await;
    let rows = all["rows"].as_array().unwrap();
    assert!(rows.iter().all(|row| {
        row["label"]
            .as_str()
            .and_then(|label| label.parse::<i32>().ok())
            .is_some_and(|year| (1970..=9998).contains(&year))
    }));
    let final_row = rows
        .iter()
        .find(|row| row["label"] == "9998")
        .expect("the upper supported year must remain represented");
    assert_eq!(final_row["periodEnd"], "9999-01-01T00:00:00+00:00");
    assert_eq!(final_row["sessionCount"], 1);

    let transition_day = get_json(&harness.app, "/api/v1/stats?range=day&anchor=2026-04-05").await;
    let labels = transition_day["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["label"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        labels.iter().any(|label| label.ends_with(":30")),
        "Lord Howe's half-hour DST transition must remain visible in labels: {labels:?}"
    );
}

#[tokio::test]
async fn stats_omit_a_civil_date_skipped_by_the_local_timezone() {
    const CHILD_MARKER: &str = "CODEX_USAGE_SKIPPED_DATE_CHILD";
    const TEST_NAME: &str = "stats_omit_a_civil_date_skipped_by_the_local_timezone";

    if std::env::var_os(CHILD_MARKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD_MARKER, "1")
            .env("TZ", "Pacific/Apia")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "timezone-isolated skipped-date regression failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let boundary = DateTime::parse_from_rfc3339("2011-12-30T10:00:00Z")
        .unwrap()
        .with_timezone(&Local);
    assert_eq!(boundary.date_naive().to_string(), "2011-12-31");

    let harness = harness();
    let skipped = get_json(&harness.app, "/api/v1/stats?range=day&anchor=2011-12-30").await;
    assert!(skipped["rows"].as_array().unwrap().is_empty());
    assert_eq!(skipped["totals"]["totalTokens"], 0);

    let month = get_json(&harness.app, "/api/v1/stats?range=month&anchor=2011-12-15").await;
    let labels = month["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["label"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(labels.len(), 30);
    assert!(!labels.contains(&"2011-12-30"));
}

#[tokio::test]
async fn price_and_alias_crud_reprice_history_immediately_without_reingestion() {
    let (pricing_url, _pricing_server) = pricing_fixture_server().await;
    let harness = harness_with_pricing_url(pricing_url.clone());

    let before = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert!(before["totals"]["costUsd"].is_null());
    assert_eq!(before["totals"]["unpricedTokens"], 25_607);
    assert_eq!(before["totals"]["pricingComplete"], false);
    let unknown = get_json(&harness.app, "/api/v1/prices?q=codex&page=1&pageSize=25").await;
    assert!(unknown["lastRefreshAt"].is_null());
    assert!(unknown["lastRefreshErrorAt"].is_null());
    assert!(unknown["refreshErrorKind"].is_null());
    assert!(unknown["refreshError"].is_null());
    assert!(unknown["source"].is_null());
    assert!(unknown.get("observedUnknown").is_none());
    let unknown_metadata = get_json(&harness.app, "/api/v1/prices/metadata").await;
    assert!(unknown_metadata.get("aliases").is_none());
    assert!(unknown_metadata.get("aliasesTotal").is_none());
    assert!(unknown_metadata["observedUnknownTotal"].as_u64().unwrap() >= 1);
    assert!(
        unknown_metadata["observedUnknown"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["modelId"] == "codex-auto-review"
                && row["totalTokens"] == 25_607
                && row["usageCount"] == 1
                && row["lastSeenAt"].is_string())
    );

    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/prices/contract-test-model",
        Some(json!({
            "effectiveFrom": "2026-01-01",
            "inputPerMillion": "2.0",
            "cachedInputPerMillion": null,
            "outputPerMillion": "3.0",
            "currency": "USD"
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert!(body.is_empty());

    let model_ids = get_json(&harness.app, "/api/v1/prices/model-ids?q=CONTRACT&limit=1").await;
    assert_eq!(model_ids["items"], json!(["contract-test-model"]));

    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/aliases/codex-auto-review",
        Some(json!({"canonicalModelId": "contract-test-model"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&body)
    );

    let repriced = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert_usd(&repriced["totals"]["costUsd"], "0.051411");
    assert_eq!(repriced["totals"]["unpricedTokens"], 0);
    assert_eq!(repriced["totals"]["pricingComplete"], true);
    let repriced_session_row = get_json(
        &harness.app,
        &format!("/api/v1/sessions?q={GUARDIAN_SESSION}&pageSize=50"),
    )
    .await;
    assert_usd(&repriced_session_row["items"][0]["costUsd"], "0.051411");
    assert_eq!(repriced_session_row["items"][0]["unpricedTokens"], 0);
    let repriced_stats = get_json(&harness.app, "/api/v1/stats?range=day&anchor=2026-07-15").await;
    assert_eq!(repriced_stats["totals"]["unpricedTokens"], 0);
    assert_usd(&repriced_stats["totals"]["costUsd"], "1.27031");
    let repriced_year = get_json(&harness.app, "/api/v1/overview/year?year=2026").await;
    for row in repriced_year["topProjects"].as_array().unwrap() {
        let _ = usd_units(&row["costUsd"]);
        assert!(row["share"].is_number());
    }
    for row in repriced_year["topSessions"].as_array().unwrap() {
        let _ = usd_units(&row["costUsd"]);
    }
    let repriced_settings = get_json(&harness.app, "/api/v1/settings").await;
    assert_eq!(repriced_settings["pricing"]["unpricedTokens"], 0);
    assert_eq!(repriced_settings["pricing"]["complete"], true);

    let listed = get_json(
        &harness.app,
        "/api/v1/prices?q=contract-test&page=1&pageSize=1",
    )
    .await;
    assert_eq!(listed["total"], 1);
    assert_eq!(listed["totalPages"], 1);
    assert_eq!(listed["items"][0]["modelId"], "contract-test-model");
    assert_eq!(listed["items"][0]["inputPerMillion"], "2.00");
    assert_eq!(listed["items"][0]["cachedInputPerMillion"], Value::Null);
    assert_eq!(listed["items"][0]["outputPerMillion"], "3.00");
    assert_eq!(listed["items"][0]["source"], "manual");
    assert!(listed.get("aliases").is_none());
    assert!(listed.get("observedUnknown").is_none());
    let metadata = get_json(&harness.app, "/api/v1/prices/metadata").await;
    assert!(metadata.get("aliases").is_none());
    assert!(metadata.get("aliasesTotal").is_none());
    assert_eq!(metadata["observedUnknownTotal"], 0);
    assert!(metadata["observedUnknown"].as_array().unwrap().is_empty());
    let alias_page = get_json(&harness.app, "/api/v1/aliases?q=CONTRACT&page=1&pageSize=1").await;
    assert_eq!(alias_page["page"], 1);
    assert_eq!(alias_page["pageSize"], 1);
    assert_eq!(alias_page["total"], 1);
    assert_eq!(alias_page["totalPages"], 1);
    assert_eq!(
        alias_page["items"][0]["observedModelId"],
        "codex-auto-review"
    );
    assert_eq!(
        alias_page["items"][0]["canonicalModelId"],
        "contract-test-model"
    );
    let effective_from = listed["items"][0]["effectiveFrom"].as_str().unwrap();

    let (status, _, _) = raw_request(
        &harness.app,
        Method::DELETE,
        "/api/v1/aliases/codex-auto-review",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let aliases_after_delete = get_json(
        &harness.app,
        "/api/v1/aliases?q=contract-test&page=1&pageSize=25",
    )
    .await;
    assert_eq!(aliases_after_delete["total"], 0);
    assert!(aliases_after_delete["items"].as_array().unwrap().is_empty());
    let unpriced_again = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert_eq!(unpriced_again["totals"]["unpricedTokens"], 25_607);
    assert!(unpriced_again["totals"]["costUsd"].is_null());
    let settings_again = get_json(&harness.app, "/api/v1/settings").await;
    assert_eq!(settings_again["pricing"]["unpricedTokens"], 25_607);
    assert_eq!(settings_again["pricing"]["complete"], false);

    let delete_uri = format!(
        "/api/v1/prices/contract-test-model?effectiveFrom={}",
        effective_from.replace('+', "%2B")
    );
    let (status, _, _) = raw_request(&harness.app, Method::DELETE, &delete_uri, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let absent = get_json(
        &harness.app,
        "/api/v1/prices?q=contract-test&page=1&pageSize=25",
    )
    .await;
    assert_eq!(absent["total"], 0);

    let (status, _, body) =
        raw_request(&harness.app, Method::POST, "/api/v1/prices/refresh", None).await;
    assert_eq!(status, StatusCode::OK);
    let refreshed: Value = serde_json::from_slice(&body).unwrap();
    assert!(refreshed["updated"].as_u64().unwrap() > 0);
    let refreshed_prices = get_json(&harness.app, "/api/v1/prices?page=1&pageSize=25").await;
    assert_eq!(refreshed_prices["source"], pricing_url);
}

#[tokio::test]
async fn alias_search_normalizes_unicode_and_paginates_deterministically() {
    let harness = harness();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO model_aliases(
                observed_model_id,canonical_model_id,created_at,source
             ) VALUES
                ('MÜNCHEN-É-02','gpt-5.5','2026-01-01T00:00:00Z','remote:test'),
                ('MÜNCHEN-É-01','gpt-5.5','2026-01-01T00:00:00Z','remote:test');",
        )
        .unwrap();
    drop(connection);

    let first = get_json(
        &harness.app,
        "/api/v1/aliases?q=m%C3%BCnchen-e%CC%81&page=1&pageSize=1",
    )
    .await;
    assert_eq!(first["total"], 2);
    assert_eq!(first["totalPages"], 2);
    assert_eq!(first["items"][0]["observedModelId"], "MÜNCHEN-É-01");

    let second = get_json(
        &harness.app,
        "/api/v1/aliases?q=m%C3%BCnchen-e%CC%81&page=2&pageSize=1",
    )
    .await;
    assert_eq!(second["total"], 2);
    assert_eq!(second["items"][0]["observedModelId"], "MÜNCHEN-É-02");
}

#[tokio::test]
async fn failed_price_refresh_is_visible_without_exposing_transport_details() {
    let (pricing_url, _server) = flaky_pricing_fixture_server().await;
    let harness = harness_with_pricing_url(pricing_url.clone());
    let (status, _, body) =
        raw_request(&harness.app, Method::POST, "/api/v1/prices/refresh", None).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "Could not refresh prices; cached prices remain available."
    );

    let prices = get_json(&harness.app, "/api/v1/prices?page=1&pageSize=25").await;
    assert!(prices["lastRefreshAt"].is_null());
    assert!(prices["lastRefreshErrorAt"].is_string());
    assert_eq!(prices["refreshErrorKind"], "http");
    assert_eq!(
        prices["refreshError"],
        "The pricing source returned an unsuccessful response."
    );
    assert!(prices["items"].as_array().unwrap().len() > 10);

    let (status, _, body) =
        raw_request(&harness.app, Method::POST, "/api/v1/prices/refresh", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        serde_json::from_slice::<Value>(&body).unwrap()["updated"]
            .as_u64()
            .unwrap()
            > 0
    );
    let recovered = get_json(&harness.app, "/api/v1/prices?page=1&pageSize=25").await;
    assert!(recovered["lastRefreshAt"].is_string());
    assert!(recovered["lastRefreshErrorAt"].is_null());
    assert!(recovered["refreshErrorKind"].is_null());
    assert!(recovered["refreshError"].is_null());
    assert_eq!(recovered["source"], pricing_url);
}

#[tokio::test]
async fn manual_price_canonicalizes_the_key_reprices_usage_and_survives_refresh() {
    let (pricing_url, _pricing_server) = pricing_fixture_server().await;
    let harness = harness_with_pricing_url(pricing_url.clone());
    let (status, _, _) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/aliases/codex-auto-review",
        Some(json!({"canonicalModelId": "gpt-5.5"})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let before = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert_usd(&before["totals"]["costUsd"], "0.098976");
    let connection = harness.db.connect().unwrap();
    let usage_count_before: i64 = connection
        .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
        .unwrap();
    drop(connection);

    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/prices/gpt-5.5",
        Some(json!({
            "effectiveFrom": "1970-01-01T00:00:00+00:00",
            "inputPerMillion": "10.0",
            "cachedInputPerMillion": "1.0",
            "outputPerMillion": "60.0",
            "currency": "USD"
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&body)
    );

    let (status, _, body) =
        raw_request(&harness.app, Method::POST, "/api/v1/prices/refresh", None).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));

    let connection = harness.db.connect().unwrap();
    let price_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM model_prices WHERE model_id='gpt-5.5'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let (effective_from, input, cached, output, source): (String, i64, i64, i64, String) =
        connection
            .query_row(
                "SELECT effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,source
             FROM resolved_model_prices WHERE model_id='gpt-5.5'",
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
    let usage_count_after: i64 = connection
        .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        price_rows, 3,
        "bundled, remote, and manual rows must coexist without overwriting"
    );
    assert_eq!(effective_from, "1970-01-01T00:00:00.000000000Z");
    assert_eq!((input, cached, output), (10_000_000, 1_000_000, 60_000_000));
    assert_eq!(source, "manual");
    assert_eq!(usage_count_after, usage_count_before);
    drop(connection);

    let after = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert_usd(&after["totals"]["costUsd"], "0.197952");

    let (status, _, _) = raw_request(
        &harness.app,
        Method::DELETE,
        "/api/v1/prices/gpt-5.5?effectiveFrom=1970-01-01T00%3A00%3A00%2B00%3A00",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let restored: (i64, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT input_microusd_per_million,source FROM resolved_model_prices
             WHERE model_id='gpt-5.5'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        restored,
        (5_000_000, format!("remote:{pricing_url}")),
        "DELETE must normalize equivalent offsets and reveal the remote layer"
    );
}

#[tokio::test]
async fn unknown_only_usage_is_null_priced_but_still_present_on_every_product_surface() {
    let harness = harness();
    let now = Local::now();
    let timestamp = now.with_timezone(&Utc).to_rfc3339();
    let connection = harness.db.connect().unwrap();
    connection
        .execute("DELETE FROM model_prices WHERE model_id='gpt-5.6-sol'", [])
        .unwrap();
    let outside_selected_year = "1971-01-01T00:00:00Z";
    for table in ["usage_facts", "events", "messages"] {
        connection
            .execute(
                &format!("UPDATE {table} SET timestamp=?1"),
                [outside_selected_year],
            )
            .unwrap();
    }
    connection
        .execute(
            "UPDATE turns SET started_at=?1,completed_at=?1",
            [outside_selected_year],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE threads SET started_at=?1,last_event_at=?1",
            [outside_selected_year],
        )
        .unwrap();
    let priced_timestamp = format!("{}-01-02T12:00:00Z", now.year());
    for table in ["usage_facts", "events", "messages"] {
        connection
            .execute(
                &format!("UPDATE {table} SET timestamp=?1 WHERE thread_id=?2"),
                [&priced_timestamp, MAY_SESSION],
            )
            .unwrap();
    }
    connection
        .execute(
            "UPDATE turns SET started_at=?1,completed_at=?1 WHERE thread_id=?2",
            [&priced_timestamp, MAY_SESSION],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE threads SET started_at=?1,last_event_at=?1 WHERE id=?2",
            [&priced_timestamp, MAY_SESSION],
        )
        .unwrap();
    for thread_id in [GUARDIAN_SESSION, RICH_SESSION] {
        for table in ["usage_facts", "events", "messages"] {
            connection
                .execute(
                    &format!("UPDATE {table} SET timestamp=?1 WHERE thread_id=?2"),
                    [&timestamp, thread_id],
                )
                .unwrap();
        }
        connection
            .execute(
                "UPDATE turns SET started_at=?1,completed_at=?1 WHERE thread_id=?2",
                [&timestamp, thread_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE threads SET started_at=?1,last_event_at=?1 WHERE id=?2",
                [&timestamp, thread_id],
            )
            .unwrap();
    }
    drop(connection);

    let sessions = get_json(
        &harness.app,
        &format!("/api/v1/sessions?q={GUARDIAN_SESSION}&pageSize=50"),
    )
    .await;
    assert_eq!(sessions["total"], 1);
    assert_eq!(sessions["items"][0]["totalTokens"], 25_607);
    assert_eq!(sessions["items"][0]["unpricedTokens"], 25_607);
    assert!(sessions["items"][0]["costUsd"].is_null());
    assert_eq!(sessions["items"][0]["lifetimeUnpricedTokens"], 25_607);
    assert!(sessions["items"][0]["lifetimeCostUsd"].is_null());

    let summary = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/summary"),
    )
    .await;
    assert_eq!(summary["totals"]["totalTokens"], 25_607);
    assert_eq!(summary["totals"]["pricingComplete"], false);
    assert!(summary["totals"]["costUsd"].is_null());
    assert!(summary["models"][0]["costUsd"].is_null());

    let activity = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{GUARDIAN_SESSION}/activity?page=1&pageSize=25"),
    )
    .await;
    assert_eq!(activity["items"][0]["usage"]["totalTokens"], 25_607);
    assert!(activity["items"][0]["usage"]["costUsd"].is_null());

    let overview = get_json(&harness.app, "/api/v1/overview").await;
    assert_eq!(
        overview["periods"]["today"]["totals"]["totalTokens"],
        110_726
    );
    assert!(overview["periods"]["today"]["totals"]["costUsd"].is_null());
    assert_eq!(
        overview["periods"]["today"]["totals"]["pricingComplete"],
        false
    );
    let today = now.date_naive().to_string();
    let overview_year = get_json(
        &harness.app,
        &format!("/api/v1/overview/year?year={}", now.year()),
    )
    .await;
    let heatmap_today = overview_year["heatmap"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["date"] == today)
        .unwrap();
    assert_eq!(heatmap_today["totalTokens"], 110_726);
    assert!(heatmap_today["costUsd"].is_null());
    let top_sessions = overview_year["topSessions"].as_array().unwrap();
    assert_eq!(top_sessions.len(), 3);
    assert_eq!(top_sessions[0]["id"], MAY_SESSION);
    assert_eq!(top_sessions[0]["unpricedTokens"], 0);
    let _ = usd_units(&top_sessions[0]["costUsd"]);
    assert_eq!(top_sessions[1]["id"], RICH_SESSION);
    assert_eq!(top_sessions[1]["totalTokens"], 85_119);
    assert!(top_sessions[1]["costUsd"].is_null());
    assert_eq!(top_sessions[2]["id"], GUARDIAN_SESSION);
    assert_eq!(top_sessions[2]["totalTokens"], 25_607);
    assert!(top_sessions[2]["costUsd"].is_null());
    let drivers = overview_year["topProjects"].as_array().unwrap();
    assert_eq!(drivers.len(), 3);
    assert_eq!(drivers[0]["project"], "peregrine");
    let _ = usd_units(&drivers[0]["costUsd"]);
    assert_eq!(drivers[0]["share"].as_f64(), Some(1.0));
    assert_eq!(drivers[1]["project"], "codex-dashboard");
    assert!(drivers[1]["costUsd"].is_null());
    assert!(drivers[1]["share"].is_null());
    assert_eq!(drivers[2]["project"], "automation-review");
    assert!(drivers[2]["costUsd"].is_null());
    assert!(drivers[2]["share"].is_null());

    let stats = get_json(
        &harness.app,
        &format!("/api/v1/stats?range=day&anchor={today}"),
    )
    .await;
    assert_eq!(stats["totals"]["totalTokens"], 110_726);
    assert!(stats["totals"]["costUsd"].is_null());
    let active_index = stats["rows"]
        .as_array()
        .unwrap()
        .iter()
        .position(|row| row["totalTokens"] == 110_726)
        .unwrap();
    let active_row = &stats["rows"][active_index];
    assert!(active_row["costUsd"].is_null());
    assert!(stats["trend"][active_index].is_null());
    let empty_row = stats["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["totalTokens"] == 0)
        .unwrap();
    assert_usd(&empty_row["costUsd"], "0.00");

    let prices = get_json(&harness.app, "/api/v1/prices/metadata").await;
    assert!(
        prices["observedUnknown"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["modelId"] == "codex-auto-review")
    );

    let replay = get_json(
        &harness.app,
        &format!("/api/v1/sessions/{JULY_REPLAY_SESSION}/summary"),
    )
    .await;
    assert!(replay["session"]["costUsd"].is_null());
    assert!(replay["session"]["lifetimeCostUsd"].is_null());
    assert!(replay["totals"]["costUsd"].is_null());
    assert!(
        replay["models"]
            .as_array()
            .unwrap()
            .iter()
            .all(|model| model["costUsd"].is_null())
    );
    assert!(
        replay["agents"]
            .as_array()
            .unwrap()
            .iter()
            .all(|agent| agent["costUsd"].is_null())
    );
}

#[tokio::test]
async fn errors_are_json_and_static_routes_use_spa_fallback_without_swallowing_api_404s() {
    let harness = harness();

    for uri in [
        "/api/v1/sessions/missing/summary",
        "/api/v1/sessions/missing/activity?page=1&pageSize=25",
        "/api/v1/sessions/missing/activity/missing-event",
    ] {
        let (status, headers, body) = raw_request(&harness.app, Method::GET, uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            headers["content-type"]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"], "session not found");
    }

    let (status, _, body) = raw_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/sessions/{RICH_SESSION}/usage"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "API route not found"
    );

    let (status, _, body) = raw_request(
        &harness.app,
        Method::GET,
        "/api/v1/sessions/missing/activity/missing-event/attachments/0",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["error"], "API route not found");

    for uri in [
        "/api/v1/sessions?date=not-a-date",
        "/api/v1/sessions?date=1969-12-31",
        "/api/v1/sessions?date=9999-12-31",
        "/api/v1/sessions?date=%2B262142-12-31",
        "/api/v1/sessions?start=9999-12-31T23%3A59%3A59Z",
        "/api/v1/sessions?start=2026-07-16&end=2026-07-15",
        "/api/v1/sessions?page=not-a-number",
        "/api/v1/stats?range=quarter",
        "/api/v1/stats?range=day&anchor=not-a-date",
        "/api/v1/stats?range=day&anchor=1969-12-31",
        "/api/v1/stats?range=week&anchor=1970-01-01",
        "/api/v1/stats?range=day&anchor=9999-12-31",
        "/api/v1/stats?range=week&anchor=9999-12-31",
        "/api/v1/stats?range=month&anchor=9999-12-31",
        "/api/v1/stats?range=year&anchor=9999-12-31",
        "/api/v1/stats?range=day&anchor=%2B262142-12-31",
        "/api/v1/overview/year?year=10000",
        "/api/v1/prices/model-ids?limit=0",
        "/api/v1/prices/metadata?unknownLimit=101",
    ] {
        let (status, headers, body) = raw_request(&harness.app, Method::GET, uri, None).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{uri}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            headers["content-type"]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        assert!(serde_json::from_slice::<Value>(&body).unwrap()["error"].is_string());
    }

    let compatibility_expanding_search = "%EF%AC%83".repeat(256);
    let uri = format!("/api/v1/prices/model-ids?q={compatibility_expanding_search}");
    let (status, headers, body) = raw_request(&harness.app, Method::GET, &uri, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "model ID search must be at most 256 characters"
    );

    let malformed_json = Request::builder()
        .method(Method::PUT)
        .uri("/api/v1/prices/malformed-json")
        .header("host", "127.0.0.1")
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .body(Body::from("{"))
        .unwrap();
    let response = harness.app.clone().oneshot(malformed_json).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(serde_json::from_slice::<Value>(&body).unwrap()["error"].is_string());

    let (status, headers, body) =
        raw_request(&harness.app, Method::POST, "/api/v1/status", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert!(headers["allow"].to_str().unwrap().contains("GET"));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "Method Not Allowed"
    );

    // Rejected boundary input must not unwind a request worker. A normal
    // request immediately afterward proves the router remains usable.
    let (status, _, body) = raw_request(&harness.app, Method::GET, "/api/v1/status", None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "status after rejected date boundaries: {}",
        String::from_utf8_lossy(&body)
    );

    let (status, headers, body) = raw_request(
        &harness.app,
        Method::GET,
        "/api/v1/definitely-not-a-route",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert!(serde_json::from_slice::<Value>(&body).unwrap()["error"].is_string());

    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/prices/bad-price",
        Some(json!({
            "effectiveFrom": "2026-01-01",
            "inputPerMillion": "-1.0",
            "cachedInputPerMillion": "0.1",
            "outputPerMillion": "2.0"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "price cannot be negative"
    );
    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/prices/bad-date",
        Some(json!({
            "effectiveFrom": "sometime-ish",
            "inputPerMillion": "1.0",
            "cachedInputPerMillion": null,
            "outputPerMillion": "2.0"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "expected RFC3339 timestamp or YYYY-MM-DD"
    );
    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/prices/euro-price",
        Some(json!({
            "effectiveFrom": "2026-01-01",
            "inputPerMillion": "1.0",
            "cachedInputPerMillion": null,
            "outputPerMillion": "2.0",
            "currency": "EUR"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "only USD prices are supported"
    );
    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/aliases/gpt-5.5",
        Some(json!({"canonicalModelId": "gpt-5.5"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "an alias cannot map a model ID to itself"
    );
    let (status, _, body) = raw_request(
        &harness.app,
        Method::PUT,
        "/api/v1/aliases/missing-target",
        Some(json!({"canonicalModelId": "not-a-priced-model"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("does not have a price")
    );

    let (status, headers, body) = raw_request(
        &harness.app,
        Method::GET,
        "/sessions/some-client-side-id?tab=activity",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    assert!(String::from_utf8_lossy(&body).contains("SPA contract"));

    let (status, headers, body) =
        raw_request(&harness.app, Method::GET, "/assets/app.js", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .contains("javascript")
    );
    assert_eq!(String::from_utf8_lossy(&body), "window.fixtureApp = true");
}

#[tokio::test]
async fn browser_boundary_rejects_rebinding_and_cross_origin_mutations() {
    let harness = harness();

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .header("host", "attacker.example:5610")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "request host must be localhost or a loopback address"
    );

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .header("host", "[::1]:5610")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-security-policy"],
        "frame-ancestors 'none'"
    );
    assert_eq!(response.headers()["x-frame-options"], "DENY");

    for site in ["same-origin", "none"] {
        let response = harness
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status")
                    .header("host", "127.0.0.1:5610")
                    .header("sec-fetch-site", site)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "site={site}");
    }

    for site in ["cross-site", "same-site"] {
        let response = harness
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/overview/year?year=2026")
                    .header("host", "127.0.0.1:5610")
                    .header("sec-fetch-site", site)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "site={site}");
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["error"],
            "cross-origin API requests are not allowed"
        );
    }

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header("host", "127.0.0.1:5610")
                .header("sec-fetch-site", "cross-site")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "ordinary cross-site navigation to the local UI must remain usable"
    );

    for (header, value, expected_error) in [
        (
            "origin",
            "https://attacker.example",
            "mutation origin does not match the local application",
        ),
        (
            "sec-fetch-site",
            "cross-site",
            "cross-origin mutations are not allowed",
        ),
    ] {
        let response = harness
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/prices/refresh")
                    .header("host", "127.0.0.1:5610")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["error"],
            expected_error
        );
    }
}
