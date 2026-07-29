#![cfg(test)]

use super::super::*;
use super::support::*;
use rusqlite::{OptionalExtension, params};
use std::io::Write;

#[cfg(unix)]
#[test]
fn scan_waits_for_database_advisory_lock() {
    use std::sync::mpsc;

    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };
    let guard = DatabaseLock::acquire(&db, "ingest").unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        completed_tx.send(scan_once(&db, &roots)).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        completed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "scan acquired an advisory lock already held by another handle"
    );

    drop(guard);
    completed_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
}

#[test]
fn projector_failure_rolls_back_file_and_retries_without_advancing_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000000";
    let turn = "019f64ab-0000-7000-8000-000000000000";
    write_fixture(
        &sessions.join("root.jsonl"),
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            serde_json::json!({"timestamp":"2026-07-15T09:00:02Z","type":"event_msg","payload":{
                "type":"force_projector_error","detail":"valid JSON that must be retried"
            }}),
            usage("2026-07-15T09:00:03Z", 100),
        ],
    );
    let connection = db.connect().unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_projector_record BEFORE INSERT ON events
                 WHEN NEW.label='force_projector_error'
                 BEGIN SELECT RAISE(FAIL,'forced projector failure'); END;",
        )
        .unwrap();
    drop(connection);
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    let error = scan_once(&db, &roots).unwrap_err();
    assert!(error.to_string().contains("forced projector failure"));
    let connection = db.connect().unwrap();
    let rolled_back: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM threads),
                        (SELECT COUNT(*) FROM usage_facts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(rolled_back, (0, 0, 0));
    connection
        .execute_batch("DROP TRIGGER fail_projector_record;")
        .unwrap();
    drop(connection);

    let retried = scan_once(&db, &roots).unwrap();
    assert_eq!(retried.files_failed, 0);
    let connection = db.connect().unwrap();
    let projected: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files),
                        (SELECT COUNT(*) FROM events WHERE label='force_projector_error'),
                        (SELECT COUNT(*) FROM usage_facts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(projected, (1, 1, 1));
}

#[test]
fn zero_byte_existing_source_preserves_projection_until_path_is_absent() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("root.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000131";
    let turn = "019f64ab-0000-7000-8000-000000000131";
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
    let checkpoint_before: (i64, i64, i64, String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT size_bytes,byte_offset,line_number,content_fingerprint,ingested_at
                 FROM source_files WHERE rollout_id=?1",
            [owner],
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

    File::create(&file).unwrap();
    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(pending.files_seen, 0);
    assert_eq!(pending.files_failed, 0);
    let connection = db.connect().unwrap();
    let checkpoint_after: (i64, i64, i64, String, String) = connection
        .query_row(
            "SELECT size_bytes,byte_offset,line_number,content_fingerprint,ingested_at
                 FROM source_files WHERE rollout_id=?1",
            [owner],
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
    let projected_while_pending: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(checkpoint_after, checkpoint_before);
    assert_eq!(projected_while_pending, (1, 1, 1));
    drop(connection);

    std::fs::remove_file(&file).unwrap();
    scan_once(&db, &roots).unwrap();
    let connection = db.connect().unwrap();
    let projected_after_deletion: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(projected_after_deletion, (0, 0, 0));
}

#[test]
fn zero_byte_archive_handoff_preserves_projection_until_destination_is_populated() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000135";
    let turn = "019f64ab-0000-7000-8000-000000000135";
    let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
    let active_path = active.join(&filename);
    let archive_path = archive.join(&filename);
    let records = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    write_fixture(&active_path, &records);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };

    scan_once(&db, &roots).unwrap();
    let projection = || {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,
                            (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                            (SELECT COUNT(*) FROM threads WHERE id=?1),
                            (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .unwrap()
    };
    let active_projection = projection().unwrap();
    assert_eq!(active_projection.0, active_path.to_string_lossy());
    assert_eq!(active_projection.1, 0);
    assert_eq!(active_projection.2, 1);
    assert_eq!(active_projection.3, 1);
    assert_eq!(active_projection.4, 1);
    assert_eq!(active_projection.5, 100);

    std::fs::remove_file(&active_path).unwrap();
    File::create(&archive_path).unwrap();
    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(pending.files_seen, 0);
    assert_eq!(pending.files_failed, 0);
    assert_eq!(
        projection().unwrap(),
        active_projection,
        "an empty archive destination must not erase the active projection"
    );

    write_fixture(&archive_path, &records);
    let populated = scan_once(&db, &roots).unwrap();
    assert_eq!(populated.files_ingested, 1);
    assert_eq!(populated.files_failed, 0);
    let archived_projection = projection().unwrap();
    assert_eq!(archived_projection.0, archive_path.to_string_lossy());
    assert_eq!(archived_projection.1, 1);
    assert_eq!(archived_projection.2, 1);
    assert_eq!(archived_projection.3, 1);
    assert_eq!(archived_projection.4, 1);
    assert_eq!(archived_projection.5, 100);
}

#[test]
fn non_uuid_incomplete_archive_handoff_preserves_only_its_matching_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000145";
    let turn = "019f64ab-0000-7000-8000-000000000145";
    let active_path = active.join("friendly-session-name.jsonl");
    let archive_path = archive.join("friendly-session-name.jsonl");
    let records = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    write_fixture(&active_path, &records);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive.clone()),
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(&active_path).unwrap();
    let metadata = serde_json::to_vec(&records[0]).unwrap();
    std::fs::write(&archive_path, &metadata[..metadata.len() / 2]).unwrap();
    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(pending.files_failed, 0);
    let preserved: (i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(preserved, (1, 1, 1));

    std::fs::remove_file(&archive_path).unwrap();
    std::fs::write(
        archive.join("unrelated-name.jsonl"),
        &metadata[..metadata.len() / 2],
    )
    .unwrap();
    let unrelated = scan_once(&db, &roots).unwrap();
    assert_eq!(unrelated.files_failed, 0);
    let deleted: (i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        deleted,
        (0, 0, 0),
        "an unrelated incomplete placeholder preserved a deleted projection"
    );
}

#[test]
fn complete_malformed_handoff_reports_failure_without_erasing_committed_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000146";
    let turn = "019f64ab-0000-7000-8000-000000000146";
    let active_path = active.join("named-session.jsonl");
    write_fixture(
        &active_path,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive.clone()),
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(&active_path).unwrap();
    std::fs::write(
        archive.join("named-session.jsonl"),
        b"{\"timestamp\":\"2026-07-15T09:00:00Z\",\"type\":\"session_meta\",\"payload\":{}}\n",
    )
    .unwrap();
    let error = scan_once(&db, &roots).unwrap_err();
    assert!(
        format!("{error:#}").contains("has no rollout id"),
        "unexpected handoff error: {error:#}"
    );
    let projected: (String, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT path,
                        (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(projected.0, active_path.to_string_lossy());
    assert_eq!(
        (projected.1, projected.2, projected.3),
        (1, 1, 1),
        "a failed complete handoff erased the last committed projection"
    );
    let ingest_error: String = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT value FROM app_meta WHERE key='last_ingest_error'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(ingest_error.contains("has no rollout id"));
}

#[test]
fn archive_readiness_uses_the_committed_offset_not_an_unfinished_raw_tail() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000147";
    let turn = "019f64ab-0000-7000-8000-000000000147";
    let active_path = active.join("committed-prefix.jsonl");
    let archive_path = archive.join("committed-prefix.jsonl");
    let records = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    write_fixture(&active_path, &records);
    let mut writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&active_path)
        .unwrap();
    let unfinished = serde_json::to_vec(&usage("2026-07-15T09:00:03Z", 150)).unwrap();
    writer
        .write_all(&unfinished[..unfinished.len() / 2])
        .unwrap();
    drop(writer);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };
    scan_once(&db, &roots).unwrap();

    let mut active_snapshot = SourceSnapshot::open(&active_path).unwrap();
    let raw_fingerprint = full_content_fingerprints_from_snapshot(
        &mut active_snapshot,
        active_path.metadata().unwrap().len(),
        None,
    )
    .unwrap()
    .current
    .encode()
    .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE source_files SET content_fingerprint=?1 WHERE rollout_id=?2",
            params![raw_fingerprint, owner],
        )
        .unwrap();
    let upgraded = scan_once(&db, &roots).unwrap();
    assert_eq!(upgraded.files_unchanged, 1);

    let (raw_size, committed_size, stored_fingerprint): (i64, i64, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT size_bytes,byte_offset,content_fingerprint
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(raw_size > committed_size);
    assert_eq!(
        ChunkedFingerprint::parse(&stored_fingerprint).unwrap().size,
        committed_size as u64,
        "the handoff fingerprint included an uncommitted tail"
    );

    std::fs::remove_file(&active_path).unwrap();
    write_fixture(&archive_path, &records);
    assert_eq!(
        archive_path.metadata().unwrap().len(),
        committed_size as u64
    );
    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_ingested, 1);
    assert_eq!(report.files_failed, 0);
    let projection: (String, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT path,archived,byte_offset,
                        (SELECT COALESCE(SUM(input_tokens),0)
                         FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(projection.0, archive_path.to_string_lossy());
    assert_eq!(projection.1, 1);
    assert_eq!(projection.2, committed_size);
    assert_eq!(projection.3, 100);
}

#[test]
fn partial_archive_handoff_waits_for_previously_committed_extent() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000138";
    let turn = "019f64ab-0000-7000-8000-000000000138";
    let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
    let active_path = active.join(&filename);
    let archive_path = archive.join(&filename);
    let records = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    write_fixture(&active_path, &records);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };

    scan_once(&db, &roots).unwrap();
    let projection = || {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,size_bytes,byte_offset,line_number,
                            (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .unwrap()
    };
    let active_projection = projection();
    assert_eq!(active_projection.0, active_path.to_string_lossy());
    assert_eq!(active_projection.1, 0);
    assert_eq!(active_projection.3, active_projection.2);
    assert_eq!(active_projection.4, 4);
    assert_eq!(active_projection.5, 1);
    assert_eq!(active_projection.6, 100);

    std::fs::remove_file(&active_path).unwrap();
    let metadata_record = serde_json::to_vec(&records[0]).unwrap();
    std::fs::write(&archive_path, &metadata_record[..metadata_record.len() / 2]).unwrap();
    let partial_owner = scan_once(&db, &roots).unwrap();
    assert_eq!(partial_owner.files_seen, 1);
    assert_eq!(partial_owner.files_ingested, 0);
    assert_eq!(partial_owner.files_failed, 0);
    assert_eq!(
        projection(),
        active_projection,
        "an archive with a partial owner record erased the complete active projection"
    );

    write_fixture(&archive_path, &[records[0].clone()]);
    assert!(
        archive_path.metadata().unwrap().len() < active_projection.3 as u64,
        "the metadata-only archive must still be a partial handoff"
    );
    let metadata_only = scan_once(&db, &roots).unwrap();
    assert_eq!(metadata_only.files_seen, 1);
    assert_eq!(metadata_only.files_ingested, 0);
    assert_eq!(metadata_only.files_failed, 0);
    assert_eq!(
        projection(),
        active_projection,
        "a metadata-only archive replaced the complete active projection"
    );

    let mut partial_archive = File::create(&archive_path).unwrap();
    for record in &records[..3] {
        writeln!(
            partial_archive,
            "{}",
            serde_json::to_string(record).unwrap()
        )
        .unwrap();
    }
    let trailing_record = serde_json::to_vec(&records[3]).unwrap();
    partial_archive
        .write_all(&trailing_record[..trailing_record.len() / 2])
        .unwrap();
    drop(partial_archive);
    assert!(
        archive_path.metadata().unwrap().len() < active_projection.3 as u64,
        "the longer archive with a trailing partial record must remain below the committed extent"
    );
    let trailing_partial = scan_once(&db, &roots).unwrap();
    assert_eq!(trailing_partial.files_seen, 1);
    assert_eq!(trailing_partial.files_ingested, 0);
    assert_eq!(trailing_partial.files_failed, 0);
    assert_eq!(
        projection(),
        active_projection,
        "a longer but incomplete archive replaced the complete active projection"
    );

    let mut preallocated = File::create(&archive_path).unwrap();
    preallocated.set_len(active_projection.2 as u64).unwrap();
    writeln!(
        preallocated,
        "{}",
        serde_json::to_string(&records[0]).unwrap()
    )
    .unwrap();
    preallocated
        .seek(SeekFrom::Start(active_projection.2 as u64 - 1))
        .unwrap();
    preallocated.write_all(b"\n").unwrap();
    drop(preallocated);
    assert!(source_is_complete(
        &archive_path,
        active_projection.2 as u64
    ));
    let sparse_partial = scan_once(&db, &roots).unwrap();
    assert_eq!(sparse_partial.files_seen, 1);
    assert_eq!(sparse_partial.files_ingested, 0);
    assert_eq!(sparse_partial.files_failed, 0);
    assert_eq!(
        projection(),
        active_projection,
        "a preallocated archive destination replaced the complete active projection"
    );

    write_fixture(&archive_path, &records);
    let complete = scan_once(&db, &roots).unwrap();
    assert_eq!(complete.files_ingested, 1);
    assert_eq!(complete.files_failed, 0);
    let archived_projection = projection();
    assert_eq!(archived_projection.0, archive_path.to_string_lossy());
    assert_eq!(archived_projection.1, 1);
    assert_eq!(archived_projection.2, active_projection.2);
    assert_eq!(archived_projection.3, active_projection.3);
    assert_eq!(archived_projection.4, active_projection.4);
    assert_eq!(archived_projection.5, active_projection.5);
    assert_eq!(archived_projection.6, active_projection.6);
}

#[test]
fn handoff_revalidates_the_opened_snapshot_before_replacing_projection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000178";
    let turn = "019f64ab-0000-7000-8000-000000000178";
    let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
    let active_path = active.join(&filename);
    let archive_path = archive.join(&filename);
    let records = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    write_fixture(&active_path, &records);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(&active_path).unwrap();
    write_fixture(&archive_path, &records);
    let archive_for_hook = archive_path.clone();
    let metadata_only = records[0].clone();
    set_process_file_before_open_hook(move |path| {
        assert_eq!(path, archive_for_hook);
        write_fixture(&archive_for_hook, &[metadata_only]);
    });

    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_ingested, 0);
    assert_eq!(report.files_failed, 0);
    let projection: (String, i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT path,archived,byte_offset,
                        (SELECT COALESCE(SUM(input_tokens),0)
                         FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(projection.0, active_path.to_string_lossy());
    assert_eq!(projection.1, 0);
    assert!(projection.2 > 0);
    assert_eq!(projection.3, 100);

    let still_partial = scan_once(&db, &roots).unwrap();
    assert_eq!(still_partial.files_ingested, 0);
    let preserved_tokens: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT SUM(input_tokens) FROM usage_facts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(preserved_tokens, 100);
}

#[test]
fn partial_active_restore_preserves_archived_projection_until_copy_is_complete() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000139";
    let turn = "019f64ab-0000-7000-8000-000000000139";
    let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
    let active_path = active.join(&filename);
    let archive_path = archive.join(&filename);
    let records = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    write_fixture(&archive_path, &records);
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };

    scan_once(&db, &roots).unwrap();
    let projection = || {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,size_bytes,byte_offset,line_number,
                            (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .unwrap()
    };
    let archived_projection = projection();
    assert_eq!(archived_projection.0, archive_path.to_string_lossy());
    assert_eq!(archived_projection.1, 1);

    std::fs::remove_file(&archive_path).unwrap();
    let mut preallocated = File::create(&active_path).unwrap();
    preallocated.set_len(archived_projection.2 as u64).unwrap();
    writeln!(
        preallocated,
        "{}",
        serde_json::to_string(&records[0]).unwrap()
    )
    .unwrap();
    preallocated
        .seek(SeekFrom::Start(archived_projection.2 as u64 - 1))
        .unwrap();
    preallocated.write_all(b"\n").unwrap();
    drop(preallocated);

    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(pending.files_seen, 1);
    assert_eq!(pending.files_ingested, 0);
    assert_eq!(pending.files_failed, 0);
    assert_eq!(
        projection(),
        archived_projection,
        "a partial active restore replaced the complete archived projection"
    );

    write_fixture(&active_path, &records);
    let complete = scan_once(&db, &roots).unwrap();
    assert_eq!(complete.files_ingested, 1);
    assert_eq!(complete.files_failed, 0);
    let active_projection = projection();
    assert_eq!(active_projection.0, active_path.to_string_lossy());
    assert_eq!(active_projection.1, 0);
    assert_eq!(active_projection.2, archived_projection.2);
    assert_eq!(active_projection.3, archived_projection.3);
    assert_eq!(active_projection.4, archived_projection.4);
    assert_eq!(active_projection.5, archived_projection.5);
    assert_eq!(active_projection.6, archived_projection.6);
}

#[test]
fn zero_byte_archive_placeholder_does_not_freeze_appending_active_source() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000137";
    let turn = "019f64ab-0000-7000-8000-000000000137";
    let filename = format!("rollout-2026-07-15T09-00-00-{owner}.jsonl");
    let active_path = active.join(&filename);
    let archive_path = archive.join(&filename);
    write_fixture(
        &active_path,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };
    scan_once(&db, &roots).unwrap();

    File::create(&archive_path).unwrap();
    let mut active_file = std::fs::OpenOptions::new()
        .append(true)
        .open(&active_path)
        .unwrap();
    writeln!(
        active_file,
        "{}",
        serde_json::to_string(&usage("2026-07-15T09:00:03Z", 150)).unwrap()
    )
    .unwrap();
    drop(active_file);

    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_ingested, 1);
    assert_eq!(report.files_failed, 0);
    assert_eq!(report.records_read, 1);
    let connection = db.connect().unwrap();
    let projection: (String, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT path,archived,line_number,
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1),
                        (SELECT COALESCE(SUM(input_tokens),0)
                         FROM usage_facts WHERE thread_id=?1)
                 FROM source_files WHERE rollout_id=?1",
            [owner],
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
    assert_eq!(projection.0, active_path.to_string_lossy());
    assert_eq!(projection.1, 0);
    assert_eq!(projection.2, 5);
    assert_eq!(projection.3, 2);
    assert_eq!(projection.4, 150);
}

#[test]
fn unrelated_zero_byte_archive_placeholder_does_not_preserve_deleted_rollout() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000136";
    let turn = "019f64ab-0000-7000-8000-000000000136";
    let active_path = active.join(format!("rollout-2026-07-15T09-00-00-{owner}.jsonl"));
    write_fixture(
        &active_path,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive.clone()),
    };
    scan_once(&db, &roots).unwrap();

    std::fs::remove_file(active_path).unwrap();
    File::create(
        archive.join("rollout-2026-07-15T09-00-00-019f64aa-0000-7000-8000-000000000999.jsonl"),
    )
    .unwrap();
    scan_once(&db, &roots).unwrap();

    let connection = db.connect().unwrap();
    let projection: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM threads WHERE id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(projection, (0, 0, 0));
}

#[test]
fn zero_byte_selected_active_source_keeps_archived_duplicate_deferred() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let active_path = active.join("duplicate.jsonl");
    let archive_path = archive.join("duplicate.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000133";
    let turn = "019f64ab-0000-7000-8000-000000000133";
    let fixture = |input| {
        vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", input),
        ]
    };
    write_fixture(&active_path, &fixture(100));
    write_fixture(&archive_path, &fixture(100));
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };

    scan_once(&db, &roots).unwrap();
    let snapshot = || {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,size_bytes,byte_offset,line_number,
                            content_fingerprint,ingested_at,
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .unwrap()
    };
    let selected_active = snapshot();
    assert_eq!(selected_active.0, active_path.to_string_lossy());
    assert_eq!(selected_active.1, 0);
    assert_eq!(selected_active.7, 100);

    File::create(&active_path).unwrap();
    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(
        pending.files_seen, 1,
        "the archived duplicate is still observed"
    );
    assert_eq!(
        snapshot(),
        selected_active,
        "an archived duplicate replaced the checkpoint for an existing empty active owner"
    );

    std::fs::remove_file(&active_path).unwrap();
    scan_once(&db, &roots).unwrap();
    let selected_archive = snapshot();
    assert_eq!(selected_archive.0, archive_path.to_string_lossy());
    assert_eq!(selected_archive.1, 1);
    assert_eq!(selected_archive.7, 100);
}

#[test]
fn zero_byte_selected_archive_source_keeps_active_duplicate_deferred() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let active = temp.path().join("sessions");
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&active).unwrap();
    std::fs::create_dir(&archive).unwrap();
    let active_path = active.join("duplicate.jsonl");
    let archive_path = archive.join("duplicate.jsonl");
    let owner = "019f64aa-0000-7000-8000-000000000134";
    let turn = "019f64ab-0000-7000-8000-000000000134";
    let fixture = |input| {
        vec![
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", input),
        ]
    };
    write_fixture(&archive_path, &fixture(1_000));
    let roots = IngestRoots {
        active: Some(active),
        archive: Some(archive),
    };

    scan_once(&db, &roots).unwrap();
    let snapshot = || {
        db.connect()
            .unwrap()
            .query_row(
                "SELECT path,archived,size_bytes,byte_offset,line_number,
                            content_fingerprint,ingested_at,
                            (SELECT COALESCE(SUM(input_tokens),0)
                             FROM usage_facts WHERE thread_id=?1)
                     FROM source_files WHERE rollout_id=?1",
                [owner],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .unwrap()
    };
    let selected_archive = snapshot();
    assert_eq!(selected_archive.0, archive_path.to_string_lossy());
    assert_eq!(selected_archive.1, 1);
    assert_eq!(selected_archive.7, 1_000);

    write_fixture(&active_path, &fixture(1_000));
    File::create(&archive_path).unwrap();
    let pending = scan_once(&db, &roots).unwrap();
    assert_eq!(
        pending.files_seen, 1,
        "the active duplicate is still observed"
    );
    assert_eq!(
        snapshot(),
        selected_archive,
        "an active duplicate replaced the checkpoint for an existing empty archive owner"
    );

    std::fs::remove_file(&archive_path).unwrap();
    scan_once(&db, &roots).unwrap();
    let selected_active = snapshot();
    assert_eq!(selected_active.0, active_path.to_string_lossy());
    assert_eq!(selected_active.1, 0);
    assert_eq!(selected_active.7, 1_000);
}

#[test]
fn empty_rollout_placeholder_waits_then_ingests_when_populated() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let file = sessions.join("rollout-empty.jsonl");
    File::create(&file).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000132";
    let turn = "019f64ab-0000-7000-8000-000000000132";
    let roots = IngestRoots {
        active: Some(sessions),
        archive: None,
    };

    let report = scan_once(&db, &roots).unwrap();
    assert_eq!(report.files_seen, 0);
    assert_eq!(report.files_failed, 0);
    let connection = db.connect().unwrap();
    let sources: i64 = connection
        .query_row("SELECT COUNT(*) FROM source_files", [], |row| row.get(0))
        .unwrap();
    assert_eq!(sources, 0);
    drop(connection);

    write_fixture(
        &file,
        &[
            meta("2026-07-15T09:00:00Z", owner, owner, false),
            task("2026-07-15T09:00:01Z", turn),
            context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
            usage("2026-07-15T09:00:02Z", 100),
        ],
    );
    let populated = scan_once(&db, &roots).unwrap();
    assert_eq!(populated.files_ingested, 1);
    let connection = db.connect().unwrap();
    let projected: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM source_files WHERE rollout_id=?1),
                        (SELECT COUNT(*) FROM usage_facts WHERE thread_id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(projected, (1, 1));
}

#[test]
fn compressed_handoff_preserves_projection_and_stays_visible() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().join("usage.db")).unwrap();
    let archive = temp.path().join("archived_sessions");
    std::fs::create_dir(&archive).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000301";
    let turn = "019f64ab-0000-7000-8000-000000000301";
    let lines = [
        meta("2026-07-15T09:00:00Z", owner, owner, false),
        task("2026-07-15T09:00:01Z", turn),
        context("2026-07-15T09:00:01Z", turn, "gpt-5.5"),
        usage("2026-07-15T09:00:02Z", 100),
    ];
    let plain = archive.join(format!("rollout-2026-07-15T09-00-00-{owner}.jsonl"));
    let compressed = plain.with_file_name(format!(
        "{}.zst",
        plain.file_name().unwrap().to_string_lossy()
    ));
    write_fixture(&plain, &lines);
    let logical_size = plain.metadata().unwrap().len();
    let roots = IngestRoots {
        active: None,
        archive: Some(archive),
    };

    scan_once(&db, &roots).unwrap();
    write_compressed_fixture(&compressed, &lines);
    std::fs::remove_file(&plain).unwrap();

    let handoff = scan_once(&db, &roots).unwrap();
    assert_eq!(handoff.files_ingested, 0);
    assert_eq!(handoff.files_unchanged, 1);
    assert_eq!(handoff.records_read, 0);
    let connection = db.connect().unwrap();
    let projected: (String, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                 path,size_bytes,byte_offset,
                 (SELECT COUNT(*) FROM usage_facts WHERE rollout_id=?1),
                 (SELECT SUM(input_tokens) FROM usage_facts WHERE rollout_id=?1)
             FROM source_files WHERE rollout_id=?1",
            [owner],
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
    assert_eq!(
        projected,
        (
            compressed.to_string_lossy().into_owned(),
            logical_size as i64,
            logical_size as i64,
            1,
            100,
        )
    );
    drop(connection);

    let unchanged = scan_once(&db, &roots).unwrap();
    assert_eq!(unchanged.files_unchanged, 1);
    assert_eq!(unchanged.files_ingested, 0);
    let rows: (i64, i64, i64) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                 EXISTS(SELECT 1 FROM rollouts WHERE id=?1),
                 EXISTS(SELECT 1 FROM threads WHERE id=?1)",
            [owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(rows, (1, 1, 1));
}
