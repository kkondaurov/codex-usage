#![cfg(test)]

use super::super::*;
use super::support::*;
use rusqlite::params;
use std::io::Write;

#[test]
fn changed_root_is_adopted_before_next_clean_scan_reconciles_old_sources() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000000";
    let owner_b = "019f64ac-0000-7000-8000-000000000000";
    let turn_a = "019f64ab-0000-7000-8000-000000000000";
    let turn_b = "019f64ad-0000-7000-8000-000000000000";
    write_fixture(
        &sessions_a.join("root-a.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner_a, owner_a, false),
            task("2026-07-15T09:00:01Z", turn_a),
            context("2026-07-15T09:00:01Z", turn_a, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    write_fixture(
        &sessions_b.join("root-b.jsonl"),
        &[
            meta("2026-07-15T10:00:00Z", owner_b, owner_b, false),
            task("2026-07-15T10:00:01Z", turn_b),
            context("2026-07-15T10:00:01Z", turn_b, "gpt-5.5"),
            usage("2026-07-15T10:00:02Z", 200),
        ],
    );
    let roots_a = IngestRoots {
        active: Some(sessions_a),
        archive: None,
    };
    let roots_b = IngestRoots {
        active: Some(sessions_b),
        archive: None,
    };

    scan_once(&db, &roots_a).unwrap();
    scan_once(&db, &roots_a).unwrap();
    scan_once(&db, &roots_b).unwrap();
    let connection = db.connect().unwrap();
    let after_adoption: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),(SELECT COUNT(*) FROM threads)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(after_adoption, (2, 2));
    drop(connection);

    scan_once(&db, &roots_b).unwrap();
    let connection = db.connect().unwrap();
    let after_confirmation: (i64, i64, String) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),(SELECT COUNT(*) FROM threads),
                        (SELECT rollout_id FROM source_files LIMIT 1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(after_confirmation, (1, 1, owner_b.into()));
}

#[test]
fn failed_changed_root_scan_preserves_the_established_signature_and_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000014";
    let owner_b = "019f64aa-0000-7000-8000-000000000015";
    write_fixture(
        &sessions_a.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    let malformed_path = sessions_b.join("malformed.jsonl");
    let mut malformed = File::create(&malformed_path).unwrap();
    writeln!(
        malformed,
        "{}",
        serde_json::to_string(&meta("2026-07-15T10:00:00Z", owner_b, owner_b, false,)).unwrap()
    )
    .unwrap();
    writeln!(malformed, "{{\"broken\":}}").unwrap();
    drop(malformed);
    let roots_a = IngestRoots {
        active: Some(sessions_a.clone()),
        archive: None,
    };
    let roots_b = IngestRoots {
        active: Some(sessions_b.clone()),
        archive: None,
    };

    scan_one_shot(&db, &roots_a).unwrap();
    let signature_a = format!("{}|", sessions_a.display());
    let error = scan_once(&db, &roots_b)
        .expect_err("a complete malformed replacement source unexpectedly scanned cleanly");
    assert!(
        format!("{error:#}").contains(&malformed_path.display().to_string()),
        "unexpected changed-root failure: {error:#}"
    );

    let (signature, owner_a_survives, report_json): (String, i64, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 (SELECT value FROM app_meta WHERE key='ingest_root_signature'),
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                 (SELECT value FROM app_meta WHERE key='last_scan_report')",
            [owner_a],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let report: ScanReport = serde_json::from_str(&report_json).unwrap();
    assert_eq!(signature, signature_a);
    assert_ne!(signature, format!("{}|", sessions_b.display()));
    assert_eq!(owner_a_survives, 1);
    assert_eq!(report.files_failed, 1);
}

#[cfg(unix)]
#[test]
fn long_lived_scanners_cannot_alternate_different_root_configurations() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000020";
    let owner_b = "019f64aa-0000-7000-8000-000000000021";
    write_fixture(
        &sessions_a.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    write_fixture(
        &sessions_b.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    let roots_a = IngestRoots {
        active: Some(sessions_a),
        archive: None,
    };
    let roots_b = IngestRoots {
        active: Some(sessions_b),
        archive: None,
    };

    let scanner_a = spawn_scanner(db.clone(), roots_a, Duration::from_millis(250)).unwrap();
    let first_deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let first_ready = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                            (SELECT value FROM app_meta WHERE key='ingest_root_signature')",
                [owner_a],
                |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, String>(1)?)),
            )
            .ok();
        if first_ready.as_ref().is_some_and(|(ready, _)| *ready) {
            break;
        }
        assert!(
            std::time::Instant::now() < first_deadline,
            "the first long-lived scanner did not finish its initial scan"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let signature_a: String = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT value FROM app_meta WHERE key='ingest_root_signature'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    let conflict = match spawn_scanner(db.clone(), roots_b, Duration::from_millis(250)) {
        Ok(scanner_b) => {
            scanner_b.shutdown();
            scanner_a.shutdown();
            panic!("a second long-lived scanner unexpectedly acquired the database")
        }
        Err(error) => error,
    };
    assert!(
        format!("{conflict:#}").contains("failed to claim live ingest scanner ownership"),
        "unexpected scanner conflict error: {conflict:#}"
    );
    let connection = db.connect().unwrap();
    let (source_count, signature): (i64, String) = connection
        .query_row(
            "SELECT COUNT(*),
                        (SELECT value FROM app_meta WHERE key='ingest_root_signature')
                 FROM source_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(source_count, 1);
    assert_eq!(signature, signature_a);
    drop(connection);
    scanner_a.shutdown();
}

#[cfg(unix)]
#[test]
fn long_lived_scanner_publishes_completed_projector_generation() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000025";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    let scanner = spawn_scanner(db.clone(), roots, Duration::from_secs(60)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (source_exists, generation): (bool, Option<String>) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                            (SELECT value FROM app_meta WHERE key=?2)",
                params![owner, "projector_generation"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        if source_exists && generation == Some(PROJECTOR_GENERATION.to_string()) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "live scanner ingested without publishing its projector generation"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(projector_generation_is_current(&db).unwrap());
    scanner.shutdown();
}

#[cfg(unix)]
#[test]
fn preclaimed_scanner_lease_survives_background_worker_handoff() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000024";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    let lease = IngestScannerLease::acquire(&db).unwrap();
    let before_handoff = scan_one_shot(&db, &roots).unwrap_err();
    assert!(format!("{before_handoff:#}").contains("live ingest scanner"));

    let scanner =
        spawn_scanner_with_lease(db.clone(), roots.clone(), Duration::from_secs(60), lease)
            .unwrap();
    let after_handoff = scan_one_shot(&db, &roots).unwrap_err();
    assert!(format!("{after_handoff:#}").contains("live ingest scanner"));
    scanner.shutdown();
}

#[cfg(unix)]
#[test]
fn preclaimed_scanner_lease_rejects_a_different_database_without_mutating_it() {
    let temp = tempfile::tempdir().unwrap();
    let db_a = Db::open(temp.path().join("usage-a.db")).unwrap();
    let db_b = Db::open(temp.path().join("usage-b.db")).unwrap();
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_b = "019f64aa-0000-7000-8000-000000000026";
    write_fixture(
        &sessions_b.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    let roots_b = IngestRoots {
        active: Some(sessions_b),
        archive: None,
    };
    let metadata = |db: &Db| {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT
                     (SELECT value FROM app_meta WHERE key='ingest_state'),
                     (SELECT value FROM app_meta WHERE key='ingest_root_signature'),
                     (SELECT COUNT(*) FROM source_files)",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap()
    };
    let before = metadata(&db_b);

    let lease_a = IngestScannerLease::acquire(&db_a).unwrap();
    let error = scan_one_shot_with_lease(&db_b, &roots_b, &lease_a)
        .expect_err("a scanner lease was accepted for a different database");
    let detail = format!("{error:#}");
    assert!(detail.contains("ingest scanner lease for"));
    assert!(detail.contains("cannot operate on"));
    assert!(detail.contains(&db_a.path().display().to_string()));
    assert!(detail.contains(&db_b.path().display().to_string()));
    assert_eq!(metadata(&db_b), before);
}

#[cfg(unix)]
#[test]
fn background_scanner_rejects_a_mismatched_lease_before_spawning() {
    let temp = tempfile::tempdir().unwrap();
    let db_a = Db::open(temp.path().join("usage-a.db")).unwrap();
    let db_b = Db::open(temp.path().join("usage-b.db")).unwrap();
    let roots_b = IngestRoots {
        active: None,
        archive: None,
    };
    let metadata = |db: &Db| {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT
                     (SELECT value FROM app_meta WHERE key='ingest_state'),
                     (SELECT value FROM app_meta WHERE key='ingest_root_signature'),
                     (SELECT COUNT(*) FROM source_files)",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap()
    };
    let before = metadata(&db_b);

    let lease_a = IngestScannerLease::acquire(&db_a).unwrap();
    let error =
        match spawn_scanner_with_lease(db_b.clone(), roots_b, Duration::from_secs(60), lease_a) {
            Ok(scanner) => {
                scanner.shutdown();
                panic!("a background scanner started with another database's lease")
            }
            Err(error) => error,
        };
    assert!(format!("{error:#}").contains("cannot operate on"));
    assert_eq!(metadata(&db_b), before);

    let replacement = IngestScannerLease::acquire(&db_a)
        .expect("a rejected background scanner retained or moved the lifetime lease");
    drop(replacement);
}

#[cfg(unix)]
#[test]
fn preclaimed_scanner_lease_accepts_the_canonical_database_behind_a_symlink() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("usage.db");
    let database_alias = temp.path().join("usage-alias.db");
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let db = Db::open(&database_path).unwrap();
    symlink(&database_path, &database_alias).unwrap();
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    let lease = IngestScannerLease::acquire_path(&database_alias).unwrap();
    let conflict = IngestScannerLease::acquire(&db)
        .expect_err("the canonical database escaped its alias-owned scanner lease");
    assert!(
        format!("{conflict:#}").contains("live ingest scanner"),
        "unexpected scanner conflict error: {conflict:#}"
    );

    scan_one_shot_with_lease(&db, &roots, &lease)
        .expect("an alias-owned lease rejected its canonical database");
}

#[cfg(unix)]
#[test]
fn failed_live_cycle_is_truthful_and_stop_releases_lease_within_wake_slice() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    db.connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_live_scan_begin
             BEFORE INSERT ON app_meta
             WHEN NEW.key='ingest_state' AND NEW.value='scanning'
             BEGIN
                 SELECT RAISE(ABORT,'injected live scan failure');
             END;",
        )
        .unwrap();
    let roots = IngestRoots {
        active: None,
        archive: None,
    };

    let scanner = spawn_scanner(db.clone(), roots, Duration::from_secs(60)).unwrap();
    let failed_by = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let state = AttemptRecorder::new(&db).state().unwrap();
        if state.as_deref() == Some("error") {
            break;
        }
        assert!(
            std::time::Instant::now() < failed_by,
            "the failed live cycle did not publish an error state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    scanner.request_stop();
    let stop_started = std::time::Instant::now();
    let lease_released_by = stop_started + Duration::from_secs(1);
    let replacement_lease = loop {
        match IngestScannerLease::acquire(&db) {
            Ok(lease) => break lease,
            Err(_) => {
                assert!(
                    std::time::Instant::now() < lease_released_by,
                    "the stopped scanner slept through its bounded cancellation wake-up"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    };
    assert!(
        stop_started.elapsed() < Duration::from_secs(1),
        "the stopped scanner retained its lifetime lease too long"
    );
    scanner.shutdown();
    drop(replacement_lease);

    assert_eq!(
        AttemptRecorder::new(&db).state().unwrap().as_deref(),
        Some("error")
    );
}

#[cfg(unix)]
#[test]
fn one_shot_rejects_conflicting_roots_while_live_scanner_owns_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000022";
    let owner_b = "019f64aa-0000-7000-8000-000000000023";
    write_fixture(
        &sessions_a.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    write_fixture(
        &sessions_b.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    let roots_a = IngestRoots {
        active: Some(sessions_a),
        archive: None,
    };
    let roots_b = IngestRoots {
        active: Some(sessions_b),
        archive: None,
    };

    let scanner = spawn_scanner(db.clone(), roots_a, Duration::from_secs(60)).unwrap();
    let first_deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let ready = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1)",
                [owner_a],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            != 0;
        if ready {
            break;
        }
        assert!(
            std::time::Instant::now() < first_deadline,
            "the live scanner did not finish its initial scan"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let attempt = scan_one_shot(&db, &roots_b);
    scanner.shutdown();
    let error = attempt.expect_err("one-shot ingestion displaced a live scanner");
    assert!(
        format!("{error:#}").contains("live ingest scanner"),
        "unexpected one-shot conflict error: {error:#}"
    );
    let projection: (i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),
                        EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                        EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?2)
                 FROM source_files",
            params![owner_a, owner_b],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(projection, (1, 1, 0));
}

#[test]
fn one_shot_scan_confirms_changed_root_and_reports_both_passes() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000010";
    let owner_b = "019f64aa-0000-7000-8000-000000000011";
    write_fixture(
        &sessions_a.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    write_fixture(
        &sessions_b.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    let roots_a = IngestRoots {
        active: Some(sessions_a),
        archive: None,
    };
    let roots_b = IngestRoots {
        active: Some(sessions_b),
        archive: None,
    };

    let initial = scan_one_shot(&db, &roots_a).unwrap();
    assert_eq!(initial.files_seen, 2);
    assert_eq!(initial.files_ingested, 1);
    assert_eq!(initial.files_unchanged, 1);

    let changed = scan_one_shot(&db, &roots_b).unwrap();
    assert_eq!(changed.files_seen, 2);
    assert_eq!(changed.files_ingested, 1);
    assert_eq!(changed.files_unchanged, 1);
    let projection: (i64, i64, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM threads),
                        (SELECT rollout_id FROM source_files)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(projection, (1, 1, owner_b.into()));

    let unchanged = scan_one_shot(&db, &roots_b).unwrap();
    assert_eq!(unchanged.files_seen, 1);
    assert_eq!(unchanged.files_unchanged, 1);
    assert_eq!(unchanged.files_ingested, 0);
}

#[test]
fn one_shot_confirmation_start_failure_is_finalized_as_an_error() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000012";
    let owner_b = "019f64aa-0000-7000-8000-000000000013";
    write_fixture(
        &sessions_a.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    write_fixture(
        &sessions_b.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    scan_one_shot(
        &db,
        &IngestRoots {
            active: Some(sessions_a),
            archive: None,
        },
    )
    .unwrap();

    let error = scan_one_shot_with_between_pass(
        &db,
        &IngestRoots {
            active: Some(sessions_b),
            archive: None,
        },
        || {
            db.connect()
                .unwrap()
                .execute_batch(
                    "CREATE TRIGGER reject_confirmation_scan_start
                         BEFORE UPDATE ON app_meta
                         WHEN OLD.key='ingest_state' AND NEW.value='scanning'
                         BEGIN
                           SELECT RAISE(ABORT,'injected confirmation start failure');
                         END;",
                )
                .unwrap();
        },
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("injected confirmation start failure"),
        "unexpected error: {error:#}"
    );

    let metadata: (String, String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                    (SELECT value FROM app_meta WHERE key='last_scan_report')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(metadata.0, "error");
    assert!(metadata.1.contains("injected confirmation start failure"));
    let report: ScanReport = serde_json::from_str(&metadata.2).unwrap();
    assert_eq!(report.files_seen, 1);
    assert_eq!(report.files_ingested, 1);
    assert_eq!(
        db.connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM source_files", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2,
        "the failed confirmation has not yet reconciled the adopted roots"
    );
}

#[test]
fn one_shot_confirmation_post_start_failure_keeps_the_confirmation_report() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions_a = temp.path().join("sessions-a");
    let sessions_b = temp.path().join("sessions-b");
    std::fs::create_dir(&sessions_a).unwrap();
    std::fs::create_dir(&sessions_b).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000016";
    let owner_b = "019f64aa-0000-7000-8000-000000000017";
    write_fixture(
        &sessions_a.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    write_fixture(
        &sessions_b.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    scan_one_shot(
        &db,
        &IngestRoots {
            active: Some(sessions_a),
            archive: None,
        },
    )
    .unwrap();

    let error = scan_one_shot_with_between_pass(
        &db,
        &IngestRoots {
            active: Some(sessions_b),
            archive: None,
        },
        || {
            set_scan_after_start_hook(|_| {
                Err(anyhow!("injected failure after confirmation attempt began"))
            });
        },
    )
    .expect_err("the injected post-start confirmation failure unexpectedly succeeded");
    assert!(
        format!("{error:#}").contains("injected failure after confirmation attempt began"),
        "unexpected error: {error:#}"
    );

    let metadata: (String, String, String, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    (SELECT value FROM app_meta WHERE key='ingest_state'),
                    (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                    (SELECT value FROM app_meta WHERE key='last_scan_report'),
                    (SELECT COUNT(*) FROM source_files)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(metadata.0, "error");
    assert!(
        metadata
            .1
            .contains("injected failure after confirmation attempt began")
    );
    let report: ScanReport = serde_json::from_str(&metadata.2).unwrap();
    assert_eq!(
        report.files_seen, 0,
        "once confirmation begins, its truthful failure report replaces the completed first-pass report"
    );
    assert_eq!(report.files_ingested, 0);
    assert_eq!(metadata.3, 2);
}

#[cfg(unix)]
#[test]
fn one_shot_holds_ingest_lock_across_confirmation_pass() {
    use std::sync::mpsc;

    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000099";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner, owner, false)],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    let (started_tx, started_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let mut worker = None;

    scan_one_shot_with_between_pass(&db, &roots, || {
        let contender_db = db.clone();
        let contender_roots = roots.clone();
        worker = Some(std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            completed_tx
                .send(scan_once(&contender_db, &contender_roots))
                .unwrap();
        }));
        started_rx.recv().unwrap();
        assert!(
            completed_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "a competing scan interleaved between one-shot passes"
        );
    })
    .unwrap();

    completed_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.unwrap().join().unwrap();
}

#[test]
fn genuinely_empty_projection_is_vacuously_current() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();

    assert!(projector_generation_is_current(&db).unwrap());
    let marker_count: i64 = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM app_meta WHERE key=?1",
            ["projector_generation"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        marker_count, 0,
        "a read-only freshness check must not mutate state"
    );
}

#[test]
fn stale_legacy_checkpoint_prevents_projector_generation_publication() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO source_files(
                 rollout_id,path,parse_state_json,ingested_at
             ) VALUES(?1,?2,?3,?4)",
            params![
                "legacy-rollout",
                "/tmp/legacy-rollout.jsonl",
                "malformed legacy checkpoint",
                "2026-07-15T09:00:00Z"
            ],
        )
        .unwrap();

    let error = AttemptRecorder::new(&db)
        .publish_projector_generation()
        .expect_err("a malformed legacy checkpoint published the current generation");
    assert!(
        format!("{error:#}").contains("stale source checkpoints still require replay"),
        "unexpected publication error: {error:#}"
    );
    let marker_count: i64 = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM app_meta WHERE key=?1",
            ["projector_generation"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        marker_count, 0,
        "failed generation publication left a global completion marker"
    );
}

#[test]
fn stale_projector_generation_replays_unchanged_and_appended_sources() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000090";
    let turn = "019f64ab-0000-7000-8000-000000000090";
    let source_message_id = "explicit-source-message";
    let scoped_message_id = projected_message_id(owner, source_message_id);
    write_fixture(
        &file,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({
                "timestamp":"2026-07-15T09:00:01.500Z",
                "type":"response_item",
                "payload":{
                    "type":"message",
                    "id":source_message_id,
                    "role":"user",
                    "content":[{"type":"input_text","text":"Replay this message."}]
                }
            }),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_one_shot(&db, &roots).unwrap();
    assert!(projector_generation_is_current(&db).unwrap());

    let connection = db.connect().unwrap();
    connection
        .execute(
            "DELETE FROM app_meta WHERE key=?1",
            ["projector_generation"],
        )
        .unwrap();
    drop(connection);
    assert!(
        !projector_generation_is_current(&db).unwrap(),
        "a nonempty projection without its completed-generation marker is stale"
    );
    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO app_meta(key,value) VALUES(?1,?2)",
            params!["projector_generation", PROJECTOR_GENERATION.to_string()],
        )
        .unwrap();
    assert!(projector_generation_is_current(&db).unwrap());
    connection
        .execute(
            "UPDATE source_files
                 SET parse_state_json=json_remove(parse_state_json,'$.projector_generation')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE messages SET id=?1 WHERE id=?2",
            params![source_message_id, scoped_message_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE events SET call_id=?1 WHERE call_id=?2",
            params![source_message_id, scoped_message_id],
        )
        .unwrap();
    drop(connection);
    assert!(!projector_generation_is_current(&db).unwrap());

    let replay = scan_one_shot(&db, &roots).unwrap();
    assert_eq!(replay.files_ingested, 1);
    assert_eq!(replay.files_unchanged, 0);
    assert_eq!(replay.records_read, 5);
    assert!(projector_generation_is_current(&db).unwrap());
    let message_identity: (i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    EXISTS(SELECT 1 FROM messages WHERE id=?1),
                    EXISTS(SELECT 1 FROM messages WHERE id=?2)",
            params![source_message_id, scoped_message_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        message_identity,
        (0, 1),
        "generation replay must replace legacy unscoped message IDs"
    );

    let connection = db.connect().unwrap();
    connection
        .execute(
            "UPDATE source_files
                 SET parse_state_json=json_remove(parse_state_json,'$.projector_generation')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE app_meta SET value='0' WHERE key='projector_generation'",
            [],
        )
        .unwrap();
    drop(connection);
    let mut append = File::options().append(true).open(&file).unwrap();
    writeln!(
        append,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
    )
    .unwrap();
    drop(append);

    let replay_with_append = scan_one_shot(&db, &roots).unwrap();
    assert_eq!(replay_with_append.files_ingested, 1);
    assert_eq!(replay_with_append.files_unchanged, 0);
    assert_eq!(replay_with_append.records_read, 6);
    assert!(projector_generation_is_current(&db).unwrap());
}

#[test]
fn interrupted_generation_replay_resumes_before_advancing_global_marker() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner_a = "019f64aa-0000-7000-8000-000000000091";
    let owner_b = "019f64aa-0000-7000-8000-000000000092";
    write_fixture(
        &sessions.join("a.jsonl"),
        &[meta("2026-07-15T09:00:00Z", owner_a, owner_a, false)],
    );
    write_fixture(
        &sessions.join("b.jsonl"),
        &[meta("2026-07-15T10:00:00Z", owner_b, owner_b, false)],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_one_shot(&db, &roots).unwrap();

    let connection = db.connect().unwrap();
    connection
        .execute(
            "UPDATE source_files
                 SET parse_state_json=json_remove(parse_state_json,'$.projector_generation')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE app_meta SET value='0' WHERE key='projector_generation'",
            [],
        )
        .unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_second_generation_replay
                 BEFORE INSERT ON rollouts
                 WHEN NEW.id='{owner_b}'
                 BEGIN
                   SELECT RAISE(ABORT,'injected generation replay failure');
                 END;"
        ))
        .unwrap();
    drop(connection);

    let error = scan_one_shot(&db, &roots).unwrap_err();
    assert!(
        format!("{error:#}").contains("injected generation replay failure"),
        "unexpected replay error: {error:#}"
    );
    assert!(!projector_generation_is_current(&db).unwrap());
    let generations: (i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                    COALESCE(CAST(json_extract(
                        (SELECT parse_state_json FROM source_files WHERE rollout_id=?1),
                        '$.projector_generation'
                    ) AS INTEGER),0),
                    COALESCE(CAST(json_extract(
                        (SELECT parse_state_json FROM source_files WHERE rollout_id=?2),
                        '$.projector_generation'
                    ) AS INTEGER),0)",
            params![owner_a, owner_b],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(generations, (PROJECTOR_GENERATION as i64, 0));

    db.connect()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_second_generation_replay")
        .unwrap();
    let resumed = scan_one_shot(&db, &roots).unwrap();
    assert_eq!(resumed.files_unchanged, 1);
    assert_eq!(resumed.files_ingested, 1);
    assert!(projector_generation_is_current(&db).unwrap());
}
