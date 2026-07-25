use crate::{
    costing::UsdAmount,
    usage::{TotalsScope, read_totals_on},
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SettingsResponse {
    database_path: String,
    active_root: Option<String>,
    archive_root: Option<String>,
    timezone: String,
    last_ingest_at: Option<String>,
    session_count: u64,
    database_bytes: u64,
    pricing: CostCoverage,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CostCoverage {
    known_cost_usd: UsdAmount,
    unpriced_tokens: u64,
    complete: bool,
}

pub(super) fn query_on(
    connection: &Connection,
    database_path: String,
    active_root: Option<String>,
    archive_root: Option<String>,
    timezone: String,
    database_bytes: u64,
) -> Result<SettingsResponse> {
    let totals = read_totals_on(connection, None, None, TotalsScope::Global)?;
    let last_ingest_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='last_ingest_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let session_count: i64 = connection.query_row(
        "SELECT COUNT(DISTINCT thread_id) FROM (
                SELECT thread_id FROM events UNION SELECT thread_id FROM usage_facts
                UNION SELECT thread_id FROM messages)",
        [],
        |row| row.get(0),
    )?;
    Ok(SettingsResponse {
        database_path,
        active_root,
        archive_root,
        timezone,
        last_ingest_at,
        session_count: session_count.max(0) as u64,
        database_bytes,
        pricing: CostCoverage {
            known_cost_usd: UsdAmount::from_cost_numerator(totals.known_cost_numerator),
            unpriced_tokens: totals.unpriced_tokens,
            complete: totals.pricing_complete,
        },
    })
}
