#![cfg(test)]

use super::super::*;
use super::support::*;
use rusqlite::params;
use std::io::Write;

#[test]
fn unchanged_scan_is_idempotent_and_partial_line_waits() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &file,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();
    let ingested_before_unchanged: String = db
        .connect()
        .unwrap()
        .query_row("SELECT ingested_at FROM source_files", [], |row| row.get(0))
        .unwrap();
    reset_fingerprint_bytes_read();
    let second = scan_once(&db, &roots).unwrap();
    #[cfg(unix)]
    assert_eq!(
        fingerprint_bytes_read(),
        0,
        "stable identity must avoid rereading the file for a digest"
    );
    #[cfg(not(unix))]
    assert_eq!(fingerprint_bytes_read(), file.metadata().unwrap().len());
    let connection = db.connect().unwrap();
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(second.files_unchanged, 1);
    let checkpoint_after_unchanged: String = connection
        .query_row("SELECT ingested_at FROM source_files", [], |row| row.get(0))
        .unwrap();
    assert_eq!(checkpoint_after_unchanged, ingested_before_unchanged);
    drop(connection);

    let previous_size = file.metadata().unwrap().len();
    let mut append = File::options().append(true).open(&file).unwrap();
    writeln!(
        append,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
    )
    .unwrap();
    drop(append);
    let new_size = file.metadata().unwrap().len();
    reset_fingerprint_bytes_read();
    let third = scan_once(&db, &roots).unwrap();
    assert_eq!(
        fingerprint_bytes_read(),
        previous_size + new_size,
        "growth audits the previous chunk and verifies the prior tail plus suffix"
    );
    assert_eq!(third.files_ingested, 1);
    assert_eq!(third.records_read, 1);
}

#[test]
fn append_during_projection_waits_for_the_next_captured_extent() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("growing-during-scan.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000149";
    let turn = "019f64ab-0000-7000-8000-000000000149";
    write_fixture(
        &file,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let captured_size = file.metadata().unwrap().len();
    let appended_path = file.clone();
    set_process_file_after_snapshot_hook(move |scanned_path| {
        assert_eq!(scanned_path, appended_path);
        let mut append = File::options().append(true).open(&appended_path).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
        )
        .unwrap();
    });
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    let first = scan_once(&db, &roots).unwrap();
    assert_eq!(first.records_read, 4);
    let first_projection: (i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),
                        (SELECT SUM(input_tokens) FROM usage_facts),
                        size_bytes,byte_offset FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        first_projection,
        (1, 100, captured_size as i64, captured_size as i64)
    );

    let second = scan_once(&db, &roots).unwrap();
    assert_eq!(second.records_read, 1);
    let second_projection: (i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),SUM(input_tokens) FROM usage_facts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(second_projection, (2, 200));

    let third = scan_once(&db, &roots).unwrap();
    assert_eq!(third.files_unchanged, 1);
    let final_count: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(final_count, 2);
}

#[test]
fn file_projection_claims_writer_before_read_snapshot_can_go_stale() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("writer-race.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000150";
    write_fixture(&file, &[meta("2026-07-15T09:00:00Z", owner, owner, false)]);
    let competing_db = db.clone();
    let competing_write_committed = Arc::new(AtomicBool::new(false));
    let committed_for_hook = competing_write_committed.clone();
    set_process_file_after_transaction_read_hook(move || {
        let connection = competing_db.connect().unwrap();
        connection.busy_timeout(Duration::ZERO).unwrap();
        if connection
            .execute(
                "INSERT INTO app_meta(key,value) VALUES('pricing-race-probe','committed')",
                [],
            )
            .is_ok()
        {
            committed_for_hook.store(true, Ordering::Release);
        }
    });

    let report = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();
    assert_eq!(report.files_ingested, 1);
    assert!(
        !competing_write_committed.load(Ordering::Acquire),
        "a competing writer committed after the projection read snapshot"
    );
}

#[cfg(unix)]
#[test]
fn rename_over_after_open_never_projects_the_replacement_under_the_old_owner() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let path = sessions.join("rollout.jsonl");
    let replacement = temp.path().join("replacement.jsonl");
    let owner_a = "019f64aa-0000-7000-8000-000000000201";
    let owner_b = "019f64aa-0000-7000-8000-000000000202";
    let turn_a = "019f64ab-0000-7000-8000-000000000201";
    let turn_b = "019f64ab-0000-7000-8000-000000000202";
    write_fixture(
        &path,
        &[
            meta("2026-07-15T09:00:00Z", owner_a, owner_a, false),
            task("2026-07-15T09:00:01Z", turn_a),
            context("2026-07-15T09:00:01Z", turn_a, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    write_fixture(
        &replacement,
        &[
            meta("2026-07-15T10:00:00Z", owner_b, owner_b, false),
            task("2026-07-15T10:00:01Z", turn_b),
            context("2026-07-15T10:00:01Z", turn_b, "gpt-5.5"),
            usage("2026-07-15T10:00:02Z", 200),
        ],
    );
    let replacement_for_hook = replacement.clone();
    let path_for_hook = path.clone();
    set_process_file_after_snapshot_hook(move |_| {
        std::fs::rename(replacement_for_hook, path_for_hook).unwrap();
    });
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    scan_once(&db, &roots).unwrap();
    let first: (String, String, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT source_files.rollout_id,usage_facts.thread_id,usage_facts.input_tokens
                 FROM source_files JOIN usage_facts USING(rollout_id)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(first, (owner_a.into(), owner_a.into(), 100));

    scan_once(&db, &roots).unwrap();
    let second: (String, String, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT source_files.rollout_id,usage_facts.thread_id,usage_facts.input_tokens,
                        (SELECT COUNT(*) FROM threads)
                 FROM source_files JOIN usage_facts USING(rollout_id)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(second, (owner_b.into(), owner_b.into(), 200, 1));
}

#[test]
fn owner_replacement_before_authoritative_open_is_rejected_without_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let path = sessions.join("owner-race.jsonl");
    let owner_a = "019f64aa-0000-7000-8000-000000000203";
    let owner_b = "019f64aa-0000-7000-8000-000000000204";
    let turn_a = "019f64ab-0000-7000-8000-000000000203";
    let turn_b = "019f64ab-0000-7000-8000-000000000204";
    write_fixture(
        &path,
        &[
            meta("2026-07-15T09:00:00Z", owner_a, owner_a, false),
            task("2026-07-15T09:00:01Z", turn_a),
            context("2026-07-15T09:00:01Z", turn_a, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let replacement_records = [
        meta("2026-07-15T10:00:00Z", owner_b, owner_b, false),
        task("2026-07-15T10:00:01Z", turn_b),
        context("2026-07-15T10:00:01Z", turn_b, "gpt-5.5"),
        usage("2026-07-15T10:00:02Z", 200),
    ];
    let expected_path = path.clone();
    set_process_file_before_open_hook(move |scanned_path| {
        assert_eq!(scanned_path, expected_path);
        write_fixture(scanned_path, &replacement_records);
    });

    let error = scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap_err();
    assert!(
        format!("{error:#}")
            .contains("changed ownership between discovery and its opened snapshot"),
        "unexpected ownership-race error: {error:#}"
    );
    let projection: (i64, i64, i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),
                    (SELECT COUNT(*) FROM threads),
                    (SELECT COUNT(*) FROM rollouts),
                    (SELECT COUNT(*) FROM turns),
                    (SELECT COUNT(*) FROM events),
                    (SELECT COUNT(*) FROM usage_facts)",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        projection,
        (0, 0, 0, 0, 0, 0),
        "the replacement was projected under either discovered owner"
    );
}

#[test]
fn same_path_owner_replacement_rolls_back_on_checkpoint_failure_then_retries_cleanly() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let path = sessions.join("owner-replacement.jsonl");
    let owner_a = "019f64aa-0000-7000-8000-000000000205";
    let owner_b = "019f64aa-0000-7000-8000-000000000206";
    let turn_a = "019f64ab-0000-7000-8000-000000000205";
    let turn_b = "019f64ab-0000-7000-8000-000000000206";
    write_fixture(
        &path,
        &[
            meta("2026-07-15T09:00:00Z", owner_a, owner_a, false),
            task("2026-07-15T09:00:01Z", turn_a),
            context("2026-07-15T09:00:01Z", turn_a, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();
    let checkpoint_before: (String, i64, i64, i64, String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT path,size_bytes,byte_offset,line_number,content_fingerprint,parse_state_json
                 FROM source_files WHERE rollout_id=?1",
            [owner_a],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    let projection_before: (i64, i64, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM rollouts WHERE id=?1),
                    (SELECT COUNT(*) FROM threads WHERE id=?1),
                    (SELECT COUNT(*) FROM turns WHERE rollout_id=?1),
                    (SELECT COUNT(*) FROM usage_facts WHERE rollout_id=?1),
                    (SELECT COALESCE(SUM(input_tokens),0) FROM usage_facts WHERE rollout_id=?1)",
            [owner_a],
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

    write_fixture(
        &path,
        &[
            meta("2026-07-15T10:00:00Z", owner_b, owner_b, false),
            task("2026-07-15T10:00:01Z", turn_b),
            context("2026-07-15T10:00:01Z", turn_b, "gpt-5.5"),
            usage("2026-07-15T10:00:02Z", 250),
        ],
    );
    let connection = db.connect().unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_replacement_checkpoint_insert
                 BEFORE INSERT ON source_files WHEN NEW.rollout_id='{owner_b}'
                 BEGIN SELECT RAISE(FAIL,'forced replacement checkpoint failure'); END;
             CREATE TRIGGER fail_replacement_checkpoint_update
                 BEFORE UPDATE ON source_files WHEN NEW.rollout_id='{owner_b}'
                 BEGIN SELECT RAISE(FAIL,'forced replacement checkpoint failure'); END;"
        ))
        .unwrap();
    drop(connection);

    let error = scan_once(&db, &roots).unwrap_err();
    assert!(
        format!("{error:#}").contains("forced replacement checkpoint failure"),
        "unexpected checkpoint failure: {error:#}"
    );
    let connection = db.connect().unwrap();
    let checkpoint_after: (String, i64, i64, i64, String, String) = connection
        .query_row(
            "SELECT path,size_bytes,byte_offset,line_number,content_fingerprint,parse_state_json
                 FROM source_files WHERE rollout_id=?1",
            [owner_a],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        checkpoint_after, checkpoint_before,
        "the failed replacement changed owner A's durable checkpoint"
    );
    let projection_after: (i64, i64, i64, i64, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM rollouts WHERE id=?1),
                    (SELECT COUNT(*) FROM threads WHERE id=?1),
                    (SELECT COUNT(*) FROM turns WHERE rollout_id=?1),
                    (SELECT COUNT(*) FROM usage_facts WHERE rollout_id=?1),
                    (SELECT COALESCE(SUM(input_tokens),0) FROM usage_facts WHERE rollout_id=?1),
                    (SELECT COUNT(*) FROM source_files WHERE rollout_id=?2),
                    (SELECT COUNT(*) FROM rollouts WHERE id=?2),
                    (SELECT COUNT(*) FROM threads WHERE id=?2)",
            params![owner_a, owner_b],
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
                ))
            },
        )
        .unwrap();
    assert_eq!(
        projection_after,
        (
            projection_before.0,
            projection_before.1,
            projection_before.2,
            projection_before.3,
            projection_before.4,
            0,
            0,
            0,
        ),
        "the failed replacement partially removed A or installed B"
    );
    connection
        .execute_batch(
            "DROP TRIGGER fail_replacement_checkpoint_insert;
             DROP TRIGGER fail_replacement_checkpoint_update;",
        )
        .unwrap();
    drop(connection);

    let retried = scan_once(&db, &roots).unwrap();
    assert_eq!(retried.files_ingested, 1);
    assert_eq!(retried.files_failed, 0);
    let installed: (i64, i64, i64, i64, i64, i64, i64, i64, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                    (SELECT COUNT(*) FROM rollouts WHERE id=?1),
                    (SELECT COUNT(*) FROM threads WHERE id=?1),
                    (SELECT COUNT(*) FROM usage_facts WHERE rollout_id=?1),
                    (SELECT COUNT(*) FROM source_files WHERE rollout_id=?2),
                    (SELECT COUNT(*) FROM rollouts WHERE id=?2),
                    (SELECT COUNT(*) FROM threads WHERE id=?2),
                    (SELECT COALESCE(SUM(input_tokens),0) FROM usage_facts WHERE rollout_id=?2),
                    (SELECT path FROM source_files WHERE rollout_id=?2)",
            params![owner_a, owner_b],
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
                ))
            },
        )
        .unwrap();
    assert_eq!(
        installed,
        (
            0,
            0,
            0,
            0,
            1,
            1,
            1,
            250,
            path.to_string_lossy().into_owned()
        )
    );
}

#[test]
fn rewrite_in_earlier_chunk_plus_append_forces_projection_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let path = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &path,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"user_message","message":"rewrite-me-A"
            }}),
            serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                "type":"agent_message","message":"x".repeat((2 * FINGERPRINT_CHUNK_BYTES) as usize)
            }}),
        ],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    let mut contents = std::fs::read(&path).unwrap();
    let needle = b"rewrite-me-A";
    let offset = contents
        .windows(needle.len())
        .position(|window| window == needle)
        .unwrap();
    contents[offset + needle.len() - 1] = b'B';
    std::fs::write(&path, contents).unwrap();
    let mut append = File::options().append(true).open(&path).unwrap();
    writeln!(
        append,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:04Z", 100)).unwrap()
    )
    .unwrap();
    drop(append);

    let report = scan_once(&db, &roots).unwrap();
    assert!(
        report.records_read > 1,
        "a prefix mismatch must rebuild instead of reading only the suffix"
    );
    let connection = db.connect().unwrap();
    let title: String = connection
        .query_row("SELECT title FROM threads WHERE id=?1", [owner], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(title, "rewrite-me-B");
}

#[test]
fn continuously_growing_file_advances_audit_until_old_rewrite_is_found() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let path = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &path,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"agent_message","message":"a".repeat((3 * FINGERPRINT_CHUNK_BYTES + FINGERPRINT_CHUNK_BYTES / 2) as usize)
            }}),
        ],
    );
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    let rewrite_offset = 2 * FINGERPRINT_CHUNK_BYTES + 128;
    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(rewrite_offset)).unwrap();
    let mut original = [0_u8; 1];
    file.read_exact(&mut original).unwrap();
    assert_eq!(original[0], b'a');
    file.seek(SeekFrom::Start(rewrite_offset)).unwrap();
    file.write_all(b"b").unwrap();
    drop(file);

    let mut final_report = None;
    for index in 0..3 {
        let mut append = File::options().append(true).open(&path).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage(
                &format!("2026-07-15T09:00:{:02}Z", index + 3),
                100 + index as u64,
            ))
            .unwrap()
        )
        .unwrap();
        drop(append);
        let report = scan_once(&db, &roots).unwrap();
        if index < 2 {
            assert_eq!(
                report.records_read, 1,
                "the rolling audit remains bounded before reaching the changed chunk"
            );
        } else {
            final_report = Some(report);
        }
    }
    assert!(
        final_report.unwrap().records_read > 1,
        "the third rolling step must reach chunk two and rebuild"
    );
}

#[test]
fn every_growing_file_advances_its_audit_when_background_budget_is_exhausted() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let mut paths = Vec::new();
    let mut owners = Vec::new();

    for index in 0..=FINGERPRINT_AUDIT_FILES_PER_SCAN {
        let path = sessions.join(format!("root-{index:02}.jsonl"));
        let owner = format!("019f64aa-0000-7000-8000-{index:012}");
        let turn = format!("019f64ab-0000-7000-8000-{index:012}");
        write_fixture(
            &path,
            &[
                meta("2026-07-15T09:00:00Z", &owner, &owner, false),
                task("2026-07-15T09:00:01Z", &turn),
                context("2026-07-15T09:00:01Z", &turn, "gpt-5.5"),
                serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                    "type":"user_message","message":"rewrite-me-A"
                }}),
                serde_json::json!({"timestamp":"2026-07-15T09:00:03Z","type":"event_msg","payload":{
                    "type":"agent_message","message":"x".repeat((2 * FINGERPRINT_CHUNK_BYTES) as usize)
                }}),
            ],
        );
        paths.push(path);
        owners.push(owner);
    }

    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    let last_path = paths.last().unwrap();
    let mut contents = std::fs::read(last_path).unwrap();
    let needle = b"rewrite-me-A";
    let offset = contents
        .windows(needle.len())
        .position(|window| window == needle)
        .unwrap();
    contents[offset + needle.len() - 1] = b'B';
    std::fs::write(last_path, contents).unwrap();

    for (index, path) in paths.iter().enumerate() {
        let mut append = File::options().append(true).open(path).unwrap();
        writeln!(
            append,
            "{}",
            serde_json::to_string(&usage(
                &format!("2026-07-15T09:00:{:02}Z", index + 4),
                100 + index as u64,
            ))
            .unwrap()
        )
        .unwrap();
    }

    scan_once(&db, &roots).unwrap();
    let title: String = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT title FROM threads WHERE id=?1",
            [owners.last().unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        title, "rewrite-me-B",
        "the ninth growing file must not be starved by the shared eight-file audit budget"
    );
}

#[test]
fn oversized_incomplete_tail_waits_then_complete_record_is_drained_and_reported() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let path = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let mut file = File::create(&path).unwrap();
    for value in [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
    ] {
        writeln!(file, "{}", serde_json::to_string(&value).unwrap()).unwrap();
    }
    write!(file, "{{\"oversized\":\"").unwrap();
    file.write_all(&vec![b'x'; MAX_JSONL_LINE_BYTES]).unwrap();
    drop(file);
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    scan_once(&db, &roots).unwrap();
    let connection = db.connect().unwrap();
    let (offset, size, error): (i64, i64, Option<String>) = connection
        .query_row(
            "SELECT byte_offset,size_bytes,last_error FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(offset < size, "an incomplete tail must remain uncommitted");
    assert!(
        error.is_none(),
        "an incomplete tail is not yet a bad record"
    );
    drop(connection);

    let mut append = File::options().append(true).open(&path).unwrap();
    writeln!(append, "\"}}").unwrap();
    writeln!(
        append,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:02Z", 100)).unwrap()
    )
    .unwrap();
    drop(append);
    let error = scan_once(&db, &roots).unwrap_err();
    assert!(error.to_string().contains("record exceeds"));
    let connection = db.connect().unwrap();
    let (usage_count, offset, size, last_error): (i64, i64, i64, String) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset,size_bytes,last_error
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(usage_count, 1, "records after the oversized line survive");
    assert_eq!(
        offset, size,
        "the complete oversized record is checkpointed"
    );
    assert!(last_error.contains(&MAX_JSONL_LINE_BYTES.to_string()));
}

#[test]
fn legacy_unattributed_usage_is_ignored_but_current_usage_remains_visible() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let legacy_owner = "019a0000-0000-7000-8000-000000000001";
    let current_owner = "019f0000-0000-7000-8000-000000000001";

    write_fixture(
        &sessions.join("legacy.jsonl"),
        &[
            meta("2025-11-21T12:00:00Z", legacy_owner, legacy_owner, false),
            usage("2025-11-21T12:00:01Z", 100),
        ],
    );
    write_fixture(
        &sessions.join("current.jsonl"),
        &[
            meta("2026-01-02T12:00:00Z", current_owner, current_owner, false),
            usage("2026-01-02T12:00:01Z", 200),
        ],
    );

    scan_once(
        &db,
        &IngestRoots {
            active: Some(sessions),
            archive: None,
        },
    )
    .unwrap();

    let connection = db.connect().unwrap();
    let rows = connection
        .prepare("SELECT thread_id,model,total_tokens FROM usage_facts ORDER BY timestamp")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows, vec![(current_owner.into(), "unknown".into(), 201)]);
}

#[test]
fn valid_short_prefix_observation_preserves_committed_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let prefix = vec![
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    let mut complete = prefix.clone();
    complete.push(usage("2026-07-15T09:00:03Z", 200));
    write_fixture(&file, &complete);
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    write_fixture(&file, &prefix);
    let deferred = scan_once(&db, &roots).unwrap();
    assert_eq!(deferred.files_ingested, 0);
    assert_eq!(deferred.records_read, 0);
    let connection = db.connect().unwrap();
    let (usage_count, committed_offset): (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(usage_count, 2, "one short observation cannot erase facts");
    assert!(
        committed_offset > file.metadata().unwrap().len() as i64,
        "the complete committed boundary remains authoritative while the shrink is pending"
    );
    let pending: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM app_meta WHERE key=?1",
            [pending_source_shrink_key(owner)],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        pending, 1,
        "the deferred candidate must survive for the next scan"
    );
}

#[test]
fn stable_same_path_shrink_is_accepted_on_repeat() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let prefix = vec![
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    let mut complete = prefix.clone();
    complete.push(usage("2026-07-15T09:00:03Z", 200));
    write_fixture(&file, &complete);
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    write_fixture(&file, &prefix);
    scan_once(&db, &roots).unwrap();
    let accepted = scan_once(&db, &roots).unwrap();
    assert_eq!(accepted.files_ingested, 1);
    assert_eq!(accepted.records_read, prefix.len() as u64);
    let connection = db.connect().unwrap();
    let (usage_count, committed_offset, stored_size): (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset,size_bytes
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(usage_count, 1, "a stable shrink becomes authoritative");
    assert_eq!(committed_offset, file.metadata().unwrap().len() as i64);
    assert_eq!(stored_size, committed_offset);
    let pending: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM app_meta WHERE key=?1",
            [pending_source_shrink_key(owner)],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        pending, 0,
        "the accepted candidate marker must be cleared atomically"
    );
}

#[test]
fn same_size_rewrite_rebuilds_rollout() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let make = |input| {
        vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", input),
        ]
    };
    write_fixture(&file, &make(100));
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();
    write_fixture(&file, &make(900));
    scan_once(&db, &roots).unwrap();
    let connection = db.connect().unwrap();
    let input: i64 = connection
        .query_row("SELECT SUM(input_tokens) FROM usage_facts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(input, 900);
}

#[test]
fn large_same_size_middle_rewrite_rebuilds_rollout() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let fixture = |content: String| {
        vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"response_item","payload":{
                "type":"message","id":"large-message","role":"user",
                "content":[{"type":"input_text","text":content}]
            }}),
        ]
    };
    let original = "a".repeat(200_000);
    write_fixture(&file, &fixture(original));
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    scan_once(&db, &roots).unwrap();

    std::thread::sleep(Duration::from_millis(2));
    let mut rewritten = "a".repeat(200_000).into_bytes();
    rewritten[100_000] = b'b';
    write_fixture(&file, &fixture(String::from_utf8(rewritten).unwrap()));
    scan_once(&db, &roots).unwrap();

    let connection = db.connect().unwrap();
    let content: String = connection
        .query_row(
            "SELECT content FROM messages WHERE id=?1",
            [projected_message_id(owner, "large-message")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(content.len(), 200_000);
    assert_eq!(content.as_bytes()[100_000], b'b');
}

#[test]
fn malformed_complete_line_is_reported_while_later_records_survive() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    let prefix = vec![
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
    ];
    let mut malformed = File::create(&file).unwrap();
    for value in &prefix {
        writeln!(malformed, "{}", serde_json::to_string(value).unwrap()).unwrap();
    }
    writeln!(malformed, "{{\"broken\":}}").unwrap();
    writeln!(
        malformed,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:02Z", 100)).unwrap()
    )
    .unwrap();
    drop(malformed);
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    let error = scan_once(&db, &roots).unwrap_err();
    assert!(error.to_string().contains("line 4"));
    let connection = db.connect().unwrap();
    let (usage_count, offset, size, line_number, error): (i64, i64, i64, i64, String) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM usage_facts),byte_offset,size_bytes,line_number,last_error
                 FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
    assert_eq!(
        usage_count, 1,
        "valid records after the malformed line survive"
    );
    assert_eq!(offset, size, "the complete malformed line is checkpointed");
    assert_eq!(line_number, 5);
    assert!(error.contains("line 4"));
    drop(connection);

    let unchanged_error = scan_once(&db, &roots).unwrap_err();
    assert!(unchanged_error.to_string().contains("line 4"));
    let connection = db.connect().unwrap();
    let usage_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM usage_facts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(usage_count, 1);
    drop(connection);

    let mut append = File::options().append(true).open(&file).unwrap();
    writeln!(
        append,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:03Z", 200)).unwrap()
    )
    .unwrap();
    drop(append);
    let appended_error = scan_once(&db, &roots).unwrap_err();
    assert!(appended_error.to_string().contains("line 4"));
    let connection = db.connect().unwrap();
    let (usage_count, last_error): (i64, String) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),last_error
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(usage_count, 2, "valid appended records remain projectable");
    assert!(last_error.contains("line 4"));
    drop(connection);

    std::thread::sleep(Duration::from_millis(2));
    let mut corrected = prefix;
    corrected.push(usage("2026-07-15T09:00:02Z", 100));
    write_fixture(&file, &corrected);
    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(pending.files_ingested, 0);
    let connection = db.connect().unwrap();
    let (usage_count, last_error): (i64, Option<String>) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),last_error
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(usage_count, 2, "the first shrink observation stays pending");
    assert!(last_error.is_some());
    drop(connection);

    let corrected = scan_once(&db, &roots).unwrap();
    assert_eq!(corrected.files_failed, 0);
    let connection = db.connect().unwrap();
    let (usage_count, last_error): (i64, Option<String>) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM usage_facts),last_error
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(usage_count, 1);
    assert!(last_error.is_none());
}
