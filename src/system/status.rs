use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StatusResponse {
    state: String,
    last_ingest_at: Option<String>,
    last_ingest_attempt_at: Option<String>,
    last_event_at: Option<String>,
    files_scanned: u64,
    files_failed: u64,
}

// `last_scan_report` is an ingestion-owned persisted record, not a reason for
// the System read model to import ingestion orchestration. Keep all fields
// required so malformed and incomplete records retain the established fallback
// to `source_files`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedScanReport {
    files_seen: u64,
    files_ingested: u64,
    files_unchanged: u64,
    files_failed: u64,
    records_read: u64,
    inherited_records_skipped: u64,
}

pub(super) fn query_on(connection: &Connection) -> Result<StatusResponse> {
    let meta = |key: &str| -> Result<Option<String>> {
        Ok(connection
            .query_row("SELECT value FROM app_meta WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?)
    };
    let (stored_files_scanned, stored_files_failed): (i64, i64) = connection.query_row(
        "SELECT COUNT(*),COALESCE(SUM(CASE WHEN last_error IS NULL THEN 0 ELSE 1 END),0)
             FROM source_files",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let last_report = meta("last_scan_report")?
        .and_then(|value| serde_json::from_str::<PersistedScanReport>(&value).ok());
    let (files_scanned, files_failed) = last_report
        .map(|report| {
            let PersistedScanReport {
                files_seen,
                files_ingested,
                files_unchanged,
                files_failed,
                records_read,
                inherited_records_skipped,
            } = report;
            let _ = (
                files_ingested,
                files_unchanged,
                records_read,
                inherited_records_skipped,
            );
            (files_seen, files_failed)
        })
        .unwrap_or_else(|| {
            (
                stored_files_scanned.max(0) as u64,
                stored_files_failed.max(0) as u64,
            )
        });
    let last_event_at = connection
        .query_row("SELECT MAX(last_event_at) FROM threads", [], |row| {
            row.get(0)
        })
        .optional()?
        .flatten();
    Ok(StatusResponse {
        state: meta("ingest_state")?.unwrap_or_else(|| "idle".into()),
        last_ingest_at: meta("last_ingest_at")?,
        last_ingest_attempt_at: meta("last_ingest_attempt_at")?,
        last_event_at,
        files_scanned,
        files_failed,
    })
}
