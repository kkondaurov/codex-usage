use anyhow::{Result, bail};
use chrono::Utc;
use rusqlite::{Connection, Transaction, TransactionBehavior, params};

const MIGRATION_1: &str = include_str!("../../migrations/0001_initial.sql");
const MIGRATIONS: &[(i64, &str)] = &[(1, MIGRATION_1)];
const REQUIRED_RUNTIME_INDEXES: &str = "
    CREATE INDEX IF NOT EXISTS idx_agent_runs_rollout ON agent_runs(rollout_id);
    CREATE INDEX IF NOT EXISTS idx_agent_runs_parent_rollout
        ON agent_runs(parent_rollout_id) WHERE rollout_id IS NULL;
    CREATE INDEX IF NOT EXISTS idx_turns_rollout ON turns(rollout_id);
    CREATE INDEX IF NOT EXISTS idx_messages_rollout ON messages(rollout_id);
    CREATE INDEX IF NOT EXISTS idx_messages_turn ON messages(turn_id);
    CREATE INDEX IF NOT EXISTS idx_events_rollout_kind ON events(rollout_id, kind);
    CREATE INDEX IF NOT EXISTS idx_events_agent_run ON events(agent_run_id);
    CREATE INDEX IF NOT EXISTS idx_events_subagent_agent_time
        ON events(json_extract(payload_json, '$.agent_thread_id'), timestamp)
        WHERE kind = 'subagent';
    CREATE INDEX IF NOT EXISTS idx_tools_turn ON tool_calls(turn_id);
    CREATE INDEX IF NOT EXISTS idx_usage_rollout ON usage_facts(rollout_id);
";

pub(super) fn migrate(connection: &Connection) -> Result<()> {
    // Repeat the guard on the read-write connection; each individual
    // migration also checks while holding its immediate transaction.
    reject_future_schema(connection)?;
    for &(version, sql) in MIGRATIONS {
        apply_migration(connection, version, sql)?;
    }
    Ok(())
}

/// Repair performance-critical indexes that were added after the schema was
/// squashed to migration 1. Existing projections legitimately already report
/// migration 1, so migration bookkeeping alone cannot deliver these indexes.
///
/// Keep this list limited to idempotent, data-preserving additions. Semantic
/// projection changes belong to the projector-generation replay boundary.
pub(super) fn ensure_runtime_indexes(connection: &Connection) -> Result<()> {
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Immediate)?;
    transaction.execute_batch(REQUIRED_RUNTIME_INDEXES)?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn reject_future_schema(connection: &Connection) -> Result<()> {
    let migration_table_exists: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='schema_migrations'
         )",
        [],
        |row| row.get(0),
    )?;
    if !migration_table_exists {
        return Ok(());
    }
    let found = connection.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
        row.get::<_, Option<i64>>(0)
    })?;
    let supported = MIGRATIONS.last().map_or(0, |(version, _)| *version);
    if let Some(found) = found.filter(|found| *found > supported) {
        bail!("database schema version {found} is newer than supported version {supported}");
    }
    Ok(())
}

fn apply_migration(connection: &Connection, version: i64, sql: &str) -> Result<()> {
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Immediate)?;
    reject_future_schema(&transaction)?;
    let migration_table_exists: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='schema_migrations'
         )",
        [],
        |row| row.get(0),
    )?;
    let applied = migration_table_exists
        && transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [version],
            |row| row.get::<_, bool>(0),
        )?;
    if !applied {
        transaction.execute_batch(sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES(?1, ?2)",
            params![version, Utc::now().to_rfc3339()],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

pub(super) fn seed_pricing(connection: &Connection) -> Result<()> {
    seed_fallback_prices(connection)?;
    Ok(())
}

pub(crate) fn seed_fallback_prices(connection: &Connection) -> Result<()> {
    let prices = [
        ("gpt-5", 1_250_000_i64, 125_000_i64, 10_000_000_i64),
        ("gpt-5-codex", 1_250_000, 125_000, 10_000_000),
        ("gpt-5-codex-mini", 1_250_000, 125_000, 10_000_000),
        ("gpt-5.1", 1_250_000, 125_000, 10_000_000),
        ("gpt-5.1-codex", 1_250_000, 125_000, 10_000_000),
        ("gpt-5.1-codex-max", 1_250_000, 125_000, 10_000_000),
        ("gpt-5.2", 1_750_000, 175_000, 14_000_000),
        ("gpt-5.2-codex", 1_750_000, 175_000, 14_000_000),
        ("gpt-5.3-codex", 1_750_000, 175_000, 14_000_000),
        ("gpt-5.3-codex-spark", 1_750_000, 175_000, 14_000_000),
        ("gpt-5.4", 2_500_000, 250_000, 15_000_000),
        ("gpt-5.5", 5_000_000, 500_000, 30_000_000),
        ("gpt-5.6-luna", 1_000_000, 100_000, 6_000_000),
        ("gpt-5.6-sol", 5_000_000, 500_000, 30_000_000),
        ("gpt-5.6-terra", 2_500_000, 250_000, 15_000_000),
    ];
    for (model, input, cached, output) in prices {
        connection.execute(
            "INSERT OR IGNORE INTO model_prices(
                model_id, effective_from, input_microusd_per_million,
                cached_input_microusd_per_million, output_microusd_per_million,
                currency, source
             ) VALUES(?1, '1970-01-01T00:00:00.000000000Z', ?2, ?3, ?4, 'USD', 'bundled-baseline')",
            params![model, input, cached, output],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{Db, database::sqlite_sidecar_path};
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn fresh_database_has_the_complete_baseline_schema() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();

        let migrations: (i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), MAX(version) FROM schema_migrations",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(migrations, (1, 1));

        let price_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM model_prices", [], |row| row.get(0))
            .unwrap();
        let alias: (String, String) = connection
            .query_row(
                "SELECT canonical_model_id, source FROM model_aliases
                 WHERE observed_model_id='codex-auto-review'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(price_count > 10);
        assert_eq!(alias, ("gpt-5.5".into(), "bundled-baseline".into()));

        let views: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='view' AND name IN (
                    'priced_usage','resolved_model_prices','resolved_model_aliases'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(views, 3);

        let overview_index_columns = connection
            .prepare("SELECT name FROM pragma_index_info('idx_usage_overview_year') ORDER BY seqno")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            overview_index_columns,
            [
                "timestamp",
                "thread_id",
                "model",
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "total_tokens"
            ]
        );

        let foreign_key_errors: i64 = connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(foreign_key_errors, 0);
    }

    #[test]
    fn reopening_version_one_database_restores_rollout_replay_indexes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("usage.db");
        let db = Db::open(&path).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "DROP INDEX idx_agent_runs_rollout;
                 DROP INDEX idx_agent_runs_parent_rollout;
                 DROP INDEX idx_turns_rollout;
                 DROP INDEX idx_messages_rollout;
                 DROP INDEX idx_messages_turn;
                 DROP INDEX idx_events_rollout_kind;
                 DROP INDEX idx_events_agent_run;
                 DROP INDEX idx_events_subagent_agent_time;
                 DROP INDEX idx_tools_turn;
                 DROP INDEX idx_usage_rollout;
                 INSERT INTO app_meta(key,value)
                    VALUES('runtime-index-repair-sentinel','preserved');",
            )
            .unwrap();
        let migration: i64 = connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(migration, 1);
        drop(connection);
        drop(db);

        let reopened = Db::open(&path).unwrap();
        let connection = reopened.connect().unwrap();
        let sentinel: String = connection
            .query_row(
                "SELECT value FROM app_meta
                 WHERE key='runtime-index-repair-sentinel'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sentinel, "preserved");
        let replay_queries = [
            (
                "idx_agent_runs_rollout",
                "SELECT rowid FROM agent_runs WHERE rollout_id='rollout'",
            ),
            (
                "idx_agent_runs_parent_rollout",
                "SELECT rowid FROM agent_runs
                 WHERE parent_rollout_id='rollout' AND rollout_id IS NULL",
            ),
            (
                "idx_turns_rollout",
                "SELECT rowid FROM turns WHERE rollout_id='rollout'",
            ),
            (
                "idx_messages_rollout",
                "SELECT rowid FROM messages WHERE rollout_id='rollout'",
            ),
            (
                "idx_messages_turn",
                "SELECT rowid FROM messages WHERE turn_id='turn'",
            ),
            (
                "idx_events_rollout_kind",
                "SELECT rowid FROM events
                 WHERE rollout_id='rollout' AND kind='subagent'",
            ),
            (
                "idx_events_agent_run",
                "SELECT rowid FROM events WHERE agent_run_id='agent'",
            ),
            (
                "idx_events_subagent_agent_time",
                "SELECT rowid FROM events
                 WHERE kind='subagent'
                   AND json_extract(payload_json,'$.agent_thread_id')='agent'
                   AND timestamp>'2026-01-01T00:00:00Z'",
            ),
            (
                "idx_tools_turn",
                "SELECT rowid FROM tool_calls WHERE turn_id='turn'",
            ),
            (
                "idx_usage_rollout",
                "SELECT rowid FROM usage_facts WHERE rollout_id='rollout'",
            ),
        ];

        for (index, query) in replay_queries {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM sqlite_master
                        WHERE type='index' AND name=?1
                     )",
                    [index],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "{index} was not restored");

            let explain = format!("EXPLAIN QUERY PLAN {query}");
            let plan = connection
                .prepare(&explain)
                .unwrap()
                .query_map([], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .join("\n");
            assert!(
                plan.contains(index),
                "{query} did not use {index}; query plan:\n{plan}"
            );
        }
    }

    #[test]
    fn usage_schema_rejects_negative_or_incoherent_token_counts() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                    VALUES('rollout','thread','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');",
            )
            .unwrap();

        for (id, input, cached, output) in [
            ("negative", -1_i64, 0_i64, 0_i64),
            ("cached-over-input", 1_i64, 2_i64, 0_i64),
            ("negative-output", 0_i64, 0_i64, -1_i64),
        ] {
            let result = connection.execute(
                "INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES(?1,'thread','rollout','2026-01-01T00:00:00Z',1,'model',?2,?3,?4,0,0)",
                params![id, input, cached, output],
            );
            assert!(result.is_err(), "{id} must violate the final schema");
        }
        for statement in [
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
             ) VALUES(
                'reasoning-over-output','thread','rollout','2026-01-01T00:00:00Z',1,'model',
                10,0,5,6,15
             )",
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
             ) VALUES(
                'contradictory-total','thread','rollout','2026-01-01T00:00:00Z',1,'model',
                10,0,5,1,999
             )",
        ] {
            assert!(
                connection.execute_batch(statement).is_err(),
                "contradictory token accounting must violate the baseline schema"
            );
        }
    }

    #[test]
    fn fixed_point_domains_reject_real_or_overflowing_arithmetic() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                    VALUES('rollout','thread','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                 INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                 ) VALUES(
                    'boundary','1970-01-01T00:00:00.000000000Z',
                    1000000000,1000000000,1000000000,'USD','manual'
                 );
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES(
                    'boundary','thread','rollout','2026-01-01T00:00:00Z',1,'boundary',
                    2000000000,0,2000000000,0,4000000000
                 );",
            )
            .unwrap();

        let priced: (String, i64) = connection
            .query_row(
                "SELECT typeof(cost_microusd),cost_microusd
                 FROM priced_usage WHERE id='boundary'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(priced, ("integer".into(), 4_000_000_000_000));

        for statement in [
            "INSERT INTO model_prices(
                model_id,effective_from,input_microusd_per_million,
                output_microusd_per_million,currency,source
             ) VALUES('too-expensive','1970-01-01T00:00:00Z',1000000001,0,'USD','manual')",
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
             ) VALUES(
                'too-many','thread','rollout','2026-01-01T00:00:01Z',2,'boundary',
                4000000001,0,0,0,0
             )",
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
             ) VALUES(
                'real-token','thread','rollout','2026-01-01T00:00:02Z',3,'boundary',
                1.5,0,0,0,0
             )",
        ] {
            assert!(
                connection.execute_batch(statement).is_err(),
                "the fixed-point domain must reject: {statement}"
            );
        }

        connection
            .execute(
                "UPDATE usage_global_totals
                 SET input_tokens=9007197254740991,
                     output_tokens=2000000000,
                     total_tokens=9007199254740991
                 WHERE id=1",
                [],
            )
            .unwrap();
        let overflow = connection.execute_batch(
            "INSERT INTO usage_facts(
                id,thread_id,rollout_id,timestamp,source_line,model,
                input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
             ) VALUES(
                'global-overflow','thread','rollout','2026-01-01T00:00:03Z',4,'boundary',
                1,0,0,0,1
             )",
        );
        assert!(overflow.is_err());
        let unchanged: (i64, String, i64) = connection
            .query_row(
                "SELECT COUNT(*),typeof(input_tokens),input_tokens
                 FROM usage_activity_rollups",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(unchanged, (1, "integer".into(), 2_000_000_000));
        let fact_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fact_count, 1);
    }

    #[test]
    fn usage_rollups_follow_updates_foreign_key_nulling_and_deletes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','2026-01-01T00:00:00Z','2026-01-01T02:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                    VALUES('rollout','thread','2026-01-01T00:00:00Z','2026-01-01T02:00:00Z');
                 INSERT INTO turns(id,thread_id,rollout_id,started_at)
                    VALUES('turn','thread','rollout','2026-01-01T00:00:00Z');
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES
                    ('usage-a','thread','rollout','turn','2026-01-01T00:10:00Z',1,'model-a',
                     10,2,3,1,13),
                    ('usage-b','thread','rollout','turn','2026-01-01T00:20:00Z',2,'model-a',
                     20,4,6,2,26);",
            )
            .unwrap();

        let original: (i64, i64, i64) = connection
            .query_row(
                "SELECT fact_count,input_tokens,total_tokens
                 FROM usage_activity_rollups
                 WHERE turn_key='turn' AND activity_hour='2026-01-01T00:00:00.000000000Z'
                   AND model='model-a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(original, (2, 30, 39));

        connection
            .execute(
                "UPDATE usage_facts SET
                    timestamp='2026-01-01T01:10:00Z',model='model-b',
                    input_tokens=40,cached_input_tokens=8,output_tokens=12,
                    reasoning_tokens=4,total_tokens=52
                 WHERE id='usage-a'",
                [],
            )
            .unwrap();
        let old_bucket: (i64, i64, i64) = connection
            .query_row(
                "SELECT fact_count,input_tokens,total_tokens
                 FROM usage_activity_rollups
                 WHERE turn_key='turn' AND activity_hour='2026-01-01T00:00:00.000000000Z'
                   AND model='model-a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(old_bucket, (1, 20, 26));
        let moved_bucket: (i64, i64, i64) = connection
            .query_row(
                "SELECT fact_count,input_tokens,total_tokens
                 FROM usage_activity_rollups
                 WHERE turn_key='turn' AND activity_hour='2026-01-01T01:00:00.000000000Z'
                   AND model='model-b'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(moved_bucket, (1, 40, 52));

        connection
            .execute("DELETE FROM turns WHERE id='turn'", [])
            .unwrap();
        let detached: Vec<(String, String, i64, i64)> = connection
            .prepare(
                "SELECT activity_hour,model,fact_count,total_tokens
                 FROM usage_activity_rollups WHERE turn_key=''
                 ORDER BY activity_hour,model",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            detached,
            [
                (
                    "2026-01-01T00:00:00.000000000Z".into(),
                    "model-a".into(),
                    1,
                    26,
                ),
                (
                    "2026-01-01T01:00:00.000000000Z".into(),
                    "model-b".into(),
                    1,
                    52,
                ),
            ]
        );
        let stale_turn_rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM usage_activity_rollups WHERE turn_key='turn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale_turn_rows, 0);

        connection
            .execute("DELETE FROM usage_facts WHERE id='usage-a'", [])
            .unwrap();
        let remaining: i64 = connection
            .query_row("SELECT COUNT(*) FROM usage_activity_rollups", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining, 1);
    }

    #[test]
    fn layered_price_lookup_prioritizes_source_before_effective_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES
                    ('layered','1970-01-01T00:00:00.000000000Z',1000000,1000000,'USD','manual'),
                    ('layered','2026-01-01T00:00:00.000000000Z',9000000,9000000,'USD','remote:test');
                 INSERT INTO threads(id,started_at,last_event_at) VALUES(
                    'thread','2027-01-01T00:00:00.000000000Z','2027-01-01T00:00:00.000000000Z'
                 );
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES(
                    'rollout','thread','2027-01-01T00:00:00.000000000Z','2027-01-01T00:00:00.000000000Z'
                 );
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES(
                    'usage','thread','rollout','2027-01-01T00:00:00.000000000Z',
                    1,'layered',1000000,0,0,0,1000000
                 );",
            )
            .unwrap();

        let priced: (String, i64) = connection
            .query_row(
                "SELECT priced_model,cost_microusd FROM priced_usage WHERE id='usage'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(priced, ("layered".into(), 1_000_000));
    }

    #[test]
    fn future_schema_is_rejected_before_database_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("future.db");
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(MIGRATION_1).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE future_sentinel(value TEXT NOT NULL);
                 INSERT INTO future_sentinel(value) VALUES('untouched');
                 INSERT INTO schema_migrations(version,applied_at)
                    VALUES(999,'2099-01-01T00:00:00Z');",
            )
            .unwrap();
        drop(connection);
        let before = std::fs::read(&path).unwrap();

        let error = Db::open(&path).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("schema version 999 is newer than supported version 1")
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!sqlite_sidecar_path(&path, "-wal").exists());
        assert!(!sqlite_sidecar_path(&path, "-shm").exists());
    }

    #[test]
    fn failed_migration_rolls_back_schema_and_version_marker() {
        let connection = Connection::open_in_memory().unwrap();
        let error = apply_migration(
            &connection,
            99,
            "CREATE TABLE should_rollback(value TEXT);
             INSERT INTO table_that_does_not_exist(value) VALUES('failure');",
        )
        .unwrap_err();
        assert!(error.to_string().contains("table_that_does_not_exist"));
        let table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type='table' AND name='should_rollback'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!table_exists);
    }

    #[test]
    fn concurrent_fresh_opens_serialize_migrations() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("concurrent.db");
        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let path = path.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                Db::open(path)
            }));
        }
        barrier.wait();
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        let db = Db::open(path).unwrap();
        let connection = db.connect().unwrap();
        let migration: (i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), MAX(version) FROM schema_migrations",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(migration, (1, 1));
        assert_eq!(journal_mode, "wal");
    }

    #[test]
    fn deleting_the_bundled_alias_is_not_undone_on_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("usage.db");
        let db = Db::open(&path).unwrap();
        db.connect()
            .unwrap()
            .execute(
                "DELETE FROM model_aliases WHERE observed_model_id='codex-auto-review'",
                [],
            )
            .unwrap();
        drop(Db::open(&path).unwrap());
        let count: i64 = Db::open(&path)
            .unwrap()
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM model_aliases
                 WHERE observed_model_id='codex-auto-review'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}
