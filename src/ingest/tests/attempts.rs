#![cfg(test)]

use super::super::*;
use super::support::*;

#[test]
fn absent_root_configuration_is_an_ingest_error() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let error = scan_once(
        &db,
        &IngestRoots {
            active: None,
            archive: None,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("no ingest roots are configured"));
    let connection = db.connect().unwrap();
    let (state, detail): (String, String) = connection
        .query_row(
            "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "error");
    assert!(detail.contains("no ingest roots are configured"));
}

#[test]
fn interrupted_scanning_state_is_recovered_under_the_ingest_lock() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    AttemptRecorder::new(&db).begin().unwrap();

    assert!(recover_interrupted_scan(&db).unwrap());
    let recovered: (String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(recovered.0, "error");
    assert!(recovered.1.contains("exited before completing"));
    assert!(!recover_interrupted_scan(&db).unwrap());
}

#[test]
fn unexpected_scan_error_is_finalized_after_scanning_begins() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    set_scan_after_start_hook(|_| Err(anyhow!("injected post-start scan failure")));

    let error = scan_once(
        &db,
        &IngestRoots {
            active: None,
            archive: None,
        },
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("injected post-start scan failure"),
        "unexpected error: {error:#}"
    );

    let metadata: (String, String, String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_attempt_at'),
                    (SELECT value FROM app_meta WHERE key='last_scan_report')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(metadata.0, "error");
    assert!(metadata.1.contains("injected post-start scan failure"));
    assert!(!metadata.2.is_empty());
    let report: ScanReport = serde_json::from_str(&metadata.3).unwrap();
    assert_eq!(report.files_seen, 0);
    assert_eq!(report.files_failed, 0);
}

#[test]
fn scan_metadata_preserves_last_success_across_failure_then_recovers() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let previous_success = "2026-07-15T08:00:00Z";
    let failed_attempt = "2026-07-15T09:00:00Z";
    let successful_attempt = "2026-07-15T10:00:00Z";
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO app_meta(key,value) VALUES('last_ingest_at',?1)",
            [previous_success],
        )
        .unwrap();

    let failed_report = serde_json::to_string(&ScanReport {
        files_seen: 2,
        files_failed: 1,
        ..ScanReport::default()
    })
    .unwrap();
    let recorder = AttemptRecorder::new(&db);
    recorder
        .finish(
            failed_attempt,
            &failed_report,
            Some("injected scan failure"),
        )
        .unwrap();

    let failed_metadata: (String, String, String, String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 (SELECT value FROM app_meta WHERE key='last_ingest_at'),
                 (SELECT value FROM app_meta WHERE key='last_ingest_attempt_at'),
                 (SELECT value FROM app_meta WHERE key='last_scan_report'),
                 (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                 (SELECT value FROM app_meta WHERE key='ingest_state')",
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
    assert_eq!(failed_metadata.0, previous_success);
    assert_eq!(failed_metadata.1, failed_attempt);
    assert_eq!(failed_metadata.2, failed_report);
    assert_eq!(failed_metadata.3, "injected scan failure");
    assert_eq!(failed_metadata.4, "error");

    let successful_report = serde_json::to_string(&ScanReport {
        files_seen: 3,
        files_ingested: 1,
        ..ScanReport::default()
    })
    .unwrap();
    recorder
        .finish(successful_attempt, &successful_report, None)
        .unwrap();

    let successful_metadata: (String, String, String, Option<String>, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 (SELECT value FROM app_meta WHERE key='last_ingest_at'),
                 (SELECT value FROM app_meta WHERE key='last_ingest_attempt_at'),
                 (SELECT value FROM app_meta WHERE key='last_scan_report'),
                 (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                 (SELECT value FROM app_meta WHERE key='ingest_state')",
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
    assert_eq!(successful_metadata.0, successful_attempt);
    assert_eq!(successful_metadata.1, successful_attempt);
    assert_eq!(successful_metadata.2, successful_report);
    assert_eq!(successful_metadata.3, None);
    assert_eq!(successful_metadata.4, "idle");
}

#[test]
fn adopted_root_signature_survives_later_scan_finalization_failure() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-0000000000a0";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
    );
    let roots = IngestRoots {
        active: Some(sessions.clone()),
        archive: None,
    };
    set_scan_after_start_hook(|db| {
        db.connect()?.execute_batch(
            "CREATE TRIGGER reject_post_adoption_scan_finalizer
                 BEFORE UPDATE ON app_meta
                 WHEN OLD.key='ingest_state'
                      AND OLD.value='scanning'
                      AND NEW.value<>'scanning'
                 BEGIN
                   SELECT RAISE(ABORT,'injected post-adoption finalizer failure');
                 END;",
        )?;
        Ok(())
    });

    let error = scan_once(&db, &roots).expect_err("the injected finalizer unexpectedly succeeded");
    assert!(
        format!("{error:#}").contains("injected post-adoption finalizer failure"),
        "unexpected scan error: {error:#}"
    );

    let expected_signature = format!("{}|", sessions.display());
    let (signature, source_count): (String, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 (SELECT value FROM app_meta WHERE key='ingest_root_signature'),
                 (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(signature, expected_signature);
    assert_eq!(
        source_count, 1,
        "post-adoption bookkeeping failure rolled back committed source work"
    );
}

#[test]
fn scan_finalizer_failure_does_not_replace_the_original_error() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    set_scan_after_start_hook(|db| {
        db.connect()?.execute_batch(
            "CREATE TRIGGER reject_scan_finalizer
                 BEFORE UPDATE ON app_meta
                 WHEN OLD.key='ingest_state' AND NEW.value<>'scanning'
                 BEGIN
                   SELECT RAISE(ABORT,'injected finalizer failure');
                 END;",
        )?;
        Err(anyhow!("original post-start scan failure"))
    });

    let error = scan_once(
        &db,
        &IngestRoots {
            active: None,
            archive: None,
        },
    )
    .unwrap_err();
    let detail = format!("{error:#}");
    assert!(detail.contains("original post-start scan failure"));
    assert!(!detail.contains("injected finalizer failure"));

    let connection = db.connect().unwrap();
    let state: String = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='ingest_state'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "scanning");
    connection
        .execute_batch("DROP TRIGGER reject_scan_finalizer")
        .unwrap();
}

#[test]
fn clearing_and_reinserting_rollouts_recomputes_exact_thread_bounds() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let mut connection = db.connect().unwrap();
    connection
        .execute_batch(
            "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('root','thread','2026-07-10T00:00:00Z','2026-07-15T00:00:00Z'),
                    ('child','thread','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z'),
                    ('promoted-grandchild','thread','2026-07-11T00:00:00Z','2026-07-11T00:00:00Z');
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,status
                 ) VALUES
                 (
                    'synthetic-grandchild','thread',NULL,'child',
                    '2026-07-02T00:00:00Z','running'
                 ),
                 (
                    'promoted-grandchild','thread','promoted-grandchild','child',
                    '2026-07-11T00:00:00Z','completed'
                 );",
        )
        .unwrap();
    let transaction = ProjectionConnection::new(&mut connection)
        .begin_metadata_refresh()
        .unwrap();
    let impact = remove_rollout(&transaction, "child").unwrap();
    assert_eq!(impact.thread_id.as_deref(), Some("thread"));
    assert!(impact.metadata_reset.is_none());
    transaction.commit().unwrap();
    let bounds: (String, String) = connection
        .query_row(
            "SELECT started_at,last_event_at FROM threads WHERE id='thread'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        bounds,
        ("2026-07-10T00:00:00Z".into(), "2026-07-15T00:00:00Z".into())
    );
    let synthetic_agents: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE id='synthetic-grandchild'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(synthetic_agents, 0);
    let promoted_agents: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE id='promoted-grandchild'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(promoted_agents, 1);

    connection
        .execute_batch(
            "DELETE FROM rollouts;
                 UPDATE threads SET
                    started_at='2026-07-01T00:00:00Z',
                    last_event_at='2026-07-20T00:00:00Z';",
        )
        .unwrap();
    let owner = OwnerMeta {
        owner_id: "root".into(),
        thread_id: "thread".into(),
        parent_rollout_id: None,
        parent_thread_id: None,
        agent_path: None,
        agent_nickname: None,
        is_subagent: false,
        forked: false,
        timestamp: "2026-07-12T00:00:00Z".into(),
        cwd: None,
        project: None,
        repository_url: None,
        branch: None,
        source: None,
        thread_source: None,
        source_json: None,
    };
    let transaction = ProjectionConnection::new(&mut connection)
        .begin_metadata_refresh()
        .unwrap();
    upsert_owner(&transaction, &owner, false).unwrap();
    transaction.commit().unwrap();
    let bounds: (String, String) = connection
        .query_row(
            "SELECT started_at,last_event_at FROM threads WHERE id='thread'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        bounds,
        ("2026-07-12T00:00:00Z".into(), "2026-07-12T00:00:00Z".into())
    );
}

#[test]
fn reparented_rollout_removes_its_abandoned_former_thread() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let parent = "019f64aa-0000-7000-8000-000000000096";
    let child = "019f64ab-0000-7000-8000-000000000096";
    let parent_path = sessions.join("parent.jsonl");
    let child_path = sessions.join("child.jsonl");
    write_fixture(
        &parent_path,
        &[meta("2026-07-15T09:00:00Z", parent, parent, false)],
    );
    write_fixture(
        &child_path,
        &[meta("2026-07-15T09:00:00Z", child, child, false)],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();
    let initial_threads: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(initial_threads, 2);

    write_fixture(
        &child_path,
        &[legacy_child_meta("2026-07-15T09:00:00Z", child, parent)],
    );
    scan_once(&db, &roots).unwrap();

    let connection = db.connect().unwrap();
    let child_thread: String = connection
        .query_row(
            "SELECT thread_id FROM rollouts WHERE id=?1",
            [child],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(child_thread, parent);
    let former_thread_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM threads WHERE id=?1)",
            [child],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!former_thread_exists);
}
