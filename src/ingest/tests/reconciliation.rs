#![cfg(test)]

use super::super::*;
use super::support::*;
use rusqlite::params;
use std::io::Write;

#[test]
fn malformed_file_does_not_suppress_reconciliation_in_an_enumerated_root() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let active_owner = "019f64aa-0000-7000-8000-000000000101";
    let archived_owner = "019f64aa-0000-7000-8000-000000000102";
    let malformed_owner = "019f64aa-0000-7000-8000-000000000103";
    write_fixture(
        &active.join("active.jsonl"),
        &[meta(
            "2026-07-15T09:00:00Z",
            active_owner,
            active_owner,
            false,
        )],
    );
    let archived_path = archive.join("archived.jsonl");
    write_fixture(
        &archived_path,
        &[meta(
            "2026-07-15T09:00:00Z",
            archived_owner,
            archived_owner,
            false,
        )],
    );
    let roots = IngestRoots {
        active: Some(active.clone()),
        archive: Some(archive),
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(archived_path).unwrap();
    let malformed_path = active.join("malformed.jsonl");
    let mut malformed = File::create(&malformed_path).unwrap();
    writeln!(
        malformed,
        "{}",
        serde_json::to_string(&meta(
            "2026-07-15T09:00:00Z",
            malformed_owner,
            malformed_owner,
            false,
        ))
        .unwrap()
    )
    .unwrap();
    writeln!(malformed, "{{\"broken\":}}").unwrap();
    drop(malformed);

    let error = scan_once(&db, &roots).unwrap_err();
    assert!(error.to_string().contains("line 2"));
    let connection = db.connect().unwrap();
    let archived_source: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM source_files WHERE rollout_id=?1",
            [archived_owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        archived_source, 0,
        "a malformed active source must not keep a deleted archived rollout alive"
    );
}

#[test]
fn traversal_failure_protects_sources_under_the_incomplete_root() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let active_owner = "019f64aa-0000-7000-8000-000000000111";
    let archived_owner = "019f64aa-0000-7000-8000-000000000112";
    write_fixture(
        &active.join("active.jsonl"),
        &[meta(
            "2026-07-15T09:00:00Z",
            active_owner,
            active_owner,
            false,
        )],
    );
    write_fixture(
        &archive.join("archived.jsonl"),
        &[meta(
            "2026-07-15T09:00:00Z",
            archived_owner,
            archived_owner,
            false,
        )],
    );
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive.clone()),
    };
    scan_once(&db, &roots).unwrap();
    std::fs::remove_dir_all(&archive).unwrap();

    let error = scan_once(&db, &roots).unwrap_err();
    assert!(error.to_string().contains("configured ingest root"));
    let connection = db.connect().unwrap();
    let archived_source: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM source_files WHERE rollout_id=?1",
            [archived_owner],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        archived_source, 1,
        "an incomplete traversal must never be interpreted as deletion"
    );
}

#[test]
fn reconciliation_deletes_missing_complete_root_sources_but_protects_incomplete_roots() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let active_owner = "019f64aa-0000-7000-8000-000000000113";
    let archived_owner = "019f64aa-0000-7000-8000-000000000114";
    let active_path = active.join("active.jsonl");
    write_fixture(
        &active_path,
        &[meta(
            "2026-07-15T09:00:00Z",
            active_owner,
            active_owner,
            false,
        )],
    );
    write_fixture(
        &archive.join("archived.jsonl"),
        &[meta(
            "2026-07-15T10:00:00Z",
            archived_owner,
            archived_owner,
            false,
        )],
    );
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive.clone()),
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(active_path).unwrap();
    std::fs::remove_dir_all(&archive).unwrap();
    let error = scan_once(&db, &roots).unwrap_err();
    assert!(error.to_string().contains("configured ingest root"));

    let connection = db.connect().unwrap();
    let active_rows: (i64, i64, i64) = connection
        .query_row(
            "SELECT
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                 EXISTS(SELECT 1 FROM rollouts WHERE id=?1),
                 EXISTS(SELECT 1 FROM threads WHERE id=?1)",
            [active_owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        active_rows,
        (0, 0, 0),
        "a missing source under the successfully enumerated root was not reconciled"
    );
    let archived_rows: (i64, i64, i64) = connection
        .query_row(
            "SELECT
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                 EXISTS(SELECT 1 FROM rollouts WHERE id=?1),
                 EXISTS(SELECT 1 FROM threads WHERE id=?1)",
            [archived_owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        archived_rows,
        (1, 1, 1),
        "a traversal failure in the archive root erased its durable projection"
    );
}

#[test]
fn reconciliation_rolls_back_every_removal_when_a_later_checkpoint_delete_fails() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owners = [
        "019f64aa-0000-7000-8000-000000000115",
        "019f64aa-0000-7000-8000-000000000116",
    ];
    for (index, owner) in owners.iter().enumerate() {
        let turn = format!("019f64ab-0000-7000-8000-{index:012}");
        write_fixture(
            &sessions.join(format!("{index}.jsonl")),
            &[
                meta("2026-07-15T09:00:00Z", owner, owner, false),
                task("2026-07-15T09:00:01Z", &turn),
                context("2026-07-15T09:00:01Z", &turn, "gpt-5.5"),
                usage("2026-07-15T09:00:02Z", 100 + index as u64),
            ],
        );
    }
    let roots = IngestRoots {
        active: Some(sessions.clone()),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();
    std::fs::remove_dir_all(&sessions).unwrap();

    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE reconciliation_delete_probe(rollout_id TEXT NOT NULL);
             CREATE TRIGGER reject_second_reconciliation_checkpoint
             BEFORE DELETE ON source_files
             BEGIN
                 INSERT INTO reconciliation_delete_probe(rollout_id) VALUES(OLD.rollout_id);
                 SELECT CASE
                     WHEN (SELECT COUNT(*) FROM reconciliation_delete_probe) = 2
                     THEN RAISE(ABORT, 'reject second reconciliation checkpoint')
                 END;
             END;",
        )
        .unwrap();
    drop(connection);

    let error = reconcile_missing(
        &db,
        &HashSet::new(),
        &HashSet::new(),
        std::slice::from_ref(&sessions),
        &[],
    )
    .expect_err("the second checkpoint deletion unexpectedly committed");
    assert!(
        format!("{error:#}").contains("reject second reconciliation checkpoint"),
        "unexpected reconciliation error: {error:#}"
    );

    let connection = db.connect().unwrap();
    let probe_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM reconciliation_delete_probe",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        probe_rows, 0,
        "the failed reconciliation left transaction-local trigger evidence behind"
    );
    for owner in owners {
        let rows: (i64, i64, i64, i64) = connection
            .query_row(
                "SELECT
                     EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                     EXISTS(SELECT 1 FROM rollouts WHERE id=?1),
                     EXISTS(SELECT 1 FROM threads WHERE id=?1),
                     EXISTS(SELECT 1 FROM usage_facts WHERE rollout_id=?1)",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            rows,
            (1, 1, 1, 1),
            "failed reconciliation partially removed {owner}"
        );
    }
}

#[test]
fn reconciliation_uses_checkpoint_thread_fallback_for_orphaned_rollouts() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let root = temp.path().join("sessions");
    std::fs::create_dir(&root).unwrap();
    let rollout = "019f64aa-0000-7000-8000-000000000117";
    let thread = "019f64ab-0000-7000-8000-000000000117";
    let source_path = root.join("missing.jsonl");
    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO threads(id,started_at,last_event_at) VALUES(?1,?2,?2)",
            params![thread, "2026-07-15T09:00:00Z"],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO source_files(
                 rollout_id,path,root_thread_id,parse_state_json,ingested_at
             ) VALUES(?1,?2,?3,'{}',?4)",
            params![
                rollout,
                source_path.to_string_lossy().as_ref(),
                thread,
                "2026-07-15T09:00:00Z"
            ],
        )
        .unwrap();
    drop(connection);

    reconcile_missing(
        &db,
        &HashSet::new(),
        &HashSet::new(),
        std::slice::from_ref(&root),
        &[],
    )
    .unwrap();

    let connection = db.connect().unwrap();
    let rows: (i64, i64) = connection
        .query_row(
            "SELECT
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                 EXISTS(SELECT 1 FROM threads WHERE id=?2)",
            params![rollout, thread],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        rows,
        (0, 0),
        "an orphan checkpoint did not use root_thread_id to remove its abandoned thread"
    );
}
