use super::{
    overview::read_year_on,
    stats::{StatsRange, read_on as read_stats_on},
};
use crate::{
    calendar::{canonical_utc_timestamp, local_midnight},
    storage::Db,
};
use anyhow::{Context, Result};
use chrono::{Datelike, Local, NaiveDate};
use rusqlite::{Connection, Transaction, TransactionBehavior};

pub fn prewarm_current_year_analytics(db: &Db) -> Result<()> {
    let today = Local::now().date_naive();
    // Startup has no requests to protect from synchronous SQLite work yet.
    // Running this directly also avoids depending on Tokio's blocking pool
    // before the long-lived server tasks have started.
    let connection = db.connect()?;
    let transaction = Transaction::new_unchecked(&connection, TransactionBehavior::Deferred)?;
    prewarm_current_year_analytics_on(&transaction, today)
        .context("failed to prewarm current-year analytics")?;
    transaction.commit()?;
    Ok(())
}

fn prewarm_current_year_analytics_on(connection: &Connection, today: NaiveDate) -> Result<()> {
    let year = today.year();
    let anchor = NaiveDate::from_ymd_opt(year, 1, 1).context("current year is invalid")?;
    let start = canonical_utc_timestamp(local_midnight(anchor));
    let next_year =
        NaiveDate::from_ymd_opt(year + 1, 1, 1).context("next year is outside the date domain")?;
    let end = canonical_utc_timestamp(local_midnight(next_year));
    let _ = read_year_on(connection, year, &start, &end)?;
    let _ = read_stats_on(connection, StatsRange::Year, anchor)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TRACE_LOCK: Mutex<()> = Mutex::new(());
    static ANALYTICS_PLAN_ORDER: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

    fn count_analytics_plan(sql: &str) {
        if sql.contains("overview-year-usage") {
            ANALYTICS_PLAN_ORDER.lock().unwrap().push("overview");
        }
        if sql.contains("bucket_model_values AS MATERIALIZED") {
            ANALYTICS_PLAN_ORDER.lock().unwrap().push("stats");
        }
    }

    #[test]
    fn startup_prewarm_executes_both_current_year_analytical_plans_in_order() {
        let _trace_guard = TRACE_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        connection.trace(Some(count_analytics_plan));
        ANALYTICS_PLAN_ORDER.lock().unwrap().clear();

        prewarm_current_year_analytics_on(
            &connection,
            NaiveDate::from_ymd_opt(2026, 7, 23).unwrap(),
        )
        .unwrap();

        assert_eq!(
            ANALYTICS_PLAN_ORDER.lock().unwrap().as_slice(),
            ["overview", "stats"],
            "startup must warm Overview before Stats inside one snapshot"
        );
        connection.trace(None);
    }
}
