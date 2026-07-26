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
fn unreadable_surviving_source_rolls_back_reconciliation_and_recovers_cleanly() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let root = "019f64aa-0000-7000-8000-000000000118";
    let child = "019f64aa-0000-7000-8000-000000000119";
    let root_path = sessions.join("root.jsonl");
    let child_path = sessions.join("child.jsonl");
    let root_meta = serde_json::json!({
        "timestamp": "2026-07-15T09:00:00Z",
        "type": "session_meta",
        "payload": {
            "id": root,
            "session_id": root,
            "cwd": "/work/root-project",
            "git": {
                "repository_url": "https://example.test/root.git",
                "branch": "main"
            },
            "source": "cli",
            "thread_source": "desktop"
        }
    });
    let child_meta = serde_json::json!({
        "timestamp": "2026-07-15T09:00:01Z",
        "type": "session_meta",
        "payload": {
            "id": child,
            "session_id": root,
            "cwd": "/work/surviving-child",
            "git": {
                "repository_url": "https://example.test/child.git",
                "branch": "review"
            },
            "source": {"subagent": {"thread_spawn": {
                "parent_thread_id": root,
                "parent_rollout_id": root,
                "agent_path": "/root/reviewer"
            }}},
            "thread_source": "delegated"
        }
    });
    write_fixture(&root_path, std::slice::from_ref(&root_meta));
    write_fixture(&child_path, std::slice::from_ref(&child_meta));
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    let connection = db.connect().unwrap();
    let metadata_before: (String, String, String, String, String, String, i64) = connection
        .query_row(
            "SELECT cwd,project,repository_url,branch,source,thread_source,root_metadata_seen
             FROM threads WHERE id=?1",
            [root],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        metadata_before,
        (
            "/work/root-project".into(),
            "root-project".into(),
            "https://example.test/root.git".into(),
            "main".into(),
            "cli".into(),
            "desktop".into(),
            1,
        )
    );
    let checkpoint_before: (String, i64, String) = connection
        .query_row(
            "SELECT path,size_bytes,parse_state_json
             FROM source_files WHERE rollout_id=?1",
            [root],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let last_success_before: String = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='last_ingest_at'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);

    std::fs::remove_file(&root_path).unwrap();
    write_fixture(
        &child_path,
        &[serde_json::json!({"complete": "but no owner"})],
    );

    let error = scan_once(&db, &roots)
        .expect_err("reconciliation unexpectedly ignored an unreadable surviving owner");
    let error = format!("{error:#}");
    assert!(
        error.contains("failed to read surviving source owner")
            && error.contains(child_path.to_string_lossy().as_ref()),
        "unexpected scan error: {error}"
    );

    let connection = db.connect().unwrap();
    let metadata_after_failure: (String, String, String, String, String, String, i64) = connection
        .query_row(
            "SELECT cwd,project,repository_url,branch,source,thread_source,root_metadata_seen
             FROM threads WHERE id=?1",
            [root],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        metadata_after_failure, metadata_before,
        "a partial surviving-owner read erased authoritative thread metadata"
    );
    let checkpoint_after_failure: (String, i64, String) = connection
        .query_row(
            "SELECT path,size_bytes,parse_state_json
             FROM source_files WHERE rollout_id=?1",
            [root],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        checkpoint_after_failure, checkpoint_before,
        "failed reconciliation deleted or advanced the missing root checkpoint"
    );
    let failed_attempt: (String, String, String) = connection
        .query_row(
            "SELECT
                 (SELECT value FROM app_meta WHERE key='ingest_state'),
                 (SELECT value FROM app_meta WHERE key='last_ingest_error'),
                 (SELECT value FROM app_meta WHERE key='last_ingest_at')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(failed_attempt.0, "error");
    assert!(
        failed_attempt
            .1
            .contains("failed to read surviving source owner")
    );
    assert_eq!(failed_attempt.2, last_success_before);
    drop(connection);

    write_fixture(&child_path, std::slice::from_ref(&child_meta));
    let recovery = scan_once(&db, &roots).unwrap();
    assert_eq!(recovery.files_failed, 0);

    let connection = db.connect().unwrap();
    let recovered: (
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        i64,
        i64,
        String,
        i64,
    ) = connection
        .query_row(
            "SELECT
                     cwd,project,repository_url,branch,source,thread_source,root_metadata_seen,
                     EXISTS(SELECT 1 FROM rollouts WHERE id=?1),
                     EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                     (SELECT value FROM app_meta WHERE key='ingest_state'),
                     EXISTS(SELECT 1 FROM app_meta WHERE key='last_ingest_error')
                 FROM threads WHERE id=?1",
            [root],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        recovered,
        (
            "/work/surviving-child".into(),
            "surviving-child".into(),
            "https://example.test/child.git".into(),
            "review".into(),
            "subagent".into(),
            "delegated".into(),
            0,
            0,
            0,
            "idle".into(),
            0,
        ),
        "the clean retry did not reconcile the removed root from complete surviving evidence"
    );
}

#[test]
fn reconciliation_removes_a_missing_root_and_child_in_one_atomic_scan() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let root = "019f64aa-0000-7000-8000-000000000120";
    let child = "019f64aa-0000-7000-8000-000000000121";
    let root_path = sessions.join("00-root.jsonl");
    let child_path = sessions.join("01-child.jsonl");
    write_fixture(
        &root_path,
        &[meta("2026-07-15T09:00:00Z", root, root, false)],
    );
    write_fixture(
        &child_path,
        &[meta("2026-07-15T09:00:01Z", child, root, true)],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(root_path).unwrap();
    std::fs::remove_file(child_path).unwrap();
    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_failed, 0);

    let connection = db.connect().unwrap();
    let remaining: (i64, i64, i64, String) = connection
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM source_files WHERE rollout_id IN (?1,?2)),
                 (SELECT COUNT(*) FROM rollouts WHERE id IN (?1,?2)),
                 EXISTS(SELECT 1 FROM threads WHERE id=?1),
                 (SELECT value FROM app_meta WHERE key='ingest_state')",
            params![root, child],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        remaining,
        (0, 0, 0, "idle".into()),
        "one reconciliation pass did not remove the complete missing thread"
    );
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
