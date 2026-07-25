use crate::storage::Db;
use anyhow::{Result, anyhow};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

pub(super) const PROJECTOR_GENERATION: u64 = 1;
const PROJECTOR_GENERATION_KEY: &str = "projector_generation";

pub(super) struct AttemptRecorder<'database> {
    db: &'database Db,
}

impl<'database> AttemptRecorder<'database> {
    pub(super) fn new(db: &'database Db) -> Self {
        Self { db }
    }

    pub(super) fn begin(&self) -> Result<()> {
        let connection = self.db.connect()?;
        connection.execute(
            "INSERT INTO app_meta(key,value) VALUES('ingest_state','scanning')
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [],
        )?;
        Ok(())
    }

    pub(super) fn state(&self) -> Result<Option<String>> {
        self.db
            .connect()?
            .query_row(
                "SELECT value FROM app_meta WHERE key='ingest_state'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub(super) fn root_signature(&self) -> Result<Option<String>> {
        self.db
            .connect()?
            .query_row(
                "SELECT value FROM app_meta WHERE key='ingest_root_signature'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub(super) fn adopt_root_signature(&self, signature: &str) -> Result<()> {
        let connection = self.db.connect()?;
        connection.execute(
            "INSERT INTO app_meta(key,value) VALUES('ingest_root_signature',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [signature],
        )?;
        Ok(())
    }

    pub(super) fn finish(
        &self,
        attempted_at: &str,
        report_json: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let mut connection = self.db.connect()?;
        let transaction = connection.transaction()?;
        for (key, value) in [
            ("last_ingest_attempt_at", attempted_at),
            ("last_scan_report", report_json),
            (
                "ingest_state",
                if error.is_some() { "error" } else { "idle" },
            ),
        ] {
            transaction.execute(
                "INSERT INTO app_meta(key,value) VALUES(?1,?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )?;
        }
        if let Some(error) = error {
            transaction.execute(
                "INSERT INTO app_meta(key,value) VALUES('last_ingest_error',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [error],
            )?;
        } else {
            transaction.execute(
                "INSERT INTO app_meta(key,value) VALUES('last_ingest_at',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [attempted_at],
            )?;
            transaction.execute("DELETE FROM app_meta WHERE key='last_ingest_error'", [])?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn recover_interrupted_state(&self) -> Result<bool> {
        let mut connection = self.db.connect()?;
        let transaction = connection.transaction()?;
        let recovered = transaction.execute(
            "UPDATE app_meta SET value='error'
             WHERE key='ingest_state' AND value='scanning'",
            [],
        )? > 0;
        if recovered {
            transaction.execute(
                "INSERT INTO app_meta(key,value)
                 VALUES('last_ingest_error','previous ingest process exited before completing')
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )?;
        }
        transaction.commit()?;
        Ok(recovered)
    }

    pub(super) fn mark_cycle_failed(&self) -> Result<()> {
        let connection = self.db.connect()?;
        connection.execute(
            "INSERT INTO app_meta(key,value) VALUES('ingest_state','error')
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [],
        )?;
        Ok(())
    }

    pub(super) fn publish_projector_generation(&self) -> Result<()> {
        let mut connection = self.db.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if has_stale_projector_checkpoints(&transaction)? {
            return Err(anyhow!(
                "projector generation {PROJECTOR_GENERATION} remains incomplete; stale source checkpoints still require replay"
            ));
        }
        transaction.execute(
            "INSERT INTO app_meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![PROJECTOR_GENERATION_KEY, PROJECTOR_GENERATION.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

/// Whether every durable source checkpoint was produced by this projector and
/// the last complete bounded scan published that generation globally.
///
/// A genuinely empty projection is vacuously current. This preserves the
/// useful `serve --no-ingest` contract for isolated empty databases while any
/// nonempty legacy projection still requires a synchronous one-shot replay.
pub fn projector_generation_is_current(db: &Db) -> Result<bool> {
    let connection = db.connect()?;
    let generation = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key=?1",
            [PROJECTOR_GENERATION_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|value| value.parse::<u64>().ok());
    let has_sources: bool =
        connection.query_row("SELECT EXISTS(SELECT 1 FROM source_files)", [], |row| {
            row.get(0)
        })?;
    let has_threads: bool =
        connection.query_row("SELECT EXISTS(SELECT 1 FROM threads)", [], |row| row.get(0))?;
    if !has_sources && !has_threads {
        return Ok(true);
    }
    if generation != Some(PROJECTOR_GENERATION) {
        return Ok(false);
    }
    Ok(!has_stale_projector_checkpoints(&connection)?)
}

fn has_stale_projector_checkpoints(connection: &Connection) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM source_files
                WHERE COALESCE(
                    CASE WHEN json_valid(parse_state_json)
                         THEN CAST(json_extract(
                             parse_state_json,'$.projector_generation'
                         ) AS INTEGER)
                    END,
                    0
                )<>?1
             )",
            [PROJECTOR_GENERATION as i64],
            |row| row.get(0),
        )
        .map_err(Into::into)
}
