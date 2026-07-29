use super::agents;
use anyhow::Result;
use rusqlite::{OptionalExtension, params};

/// Owned scalar values persisted at the final committed source boundary.
///
/// Source owns path and file-identity interpretation. Projection receives only
/// the already-captured scalar representation needed by the transactional
/// checkpoint write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) struct SourceCheckpointWrite {
    pub rollout_id: String,
    pub path: String,
    pub archived: bool,
    pub size_bytes: u64,
    pub modified_ns: u64,
    pub ctime_ns: Option<i64>,
    pub device_id: Option<i64>,
    pub inode: Option<i64>,
    pub content_fingerprint: String,
    pub byte_offset: u64,
    pub line_number: u64,
    pub root_thread_id: String,
    pub parent_rollout_id: Option<String>,
    pub native_started: bool,
    pub inherited_lines: u64,
    pub parse_state_json: String,
    pub error_count: u64,
    pub last_error: Option<String>,
    pub ingested_at: String,
}

/// Persist one file's final projection boundary in the caller's transaction.
///
/// Repeated scans accumulate newly observed complete-record errors exactly as
/// before; an unchanged scan is handled by [`mark_source_unchanged`] instead.
pub(in crate::ingest) fn save_source_checkpoint(
    tx: &super::ProjectionTx<'_>,
    checkpoint: &SourceCheckpointWrite,
) -> Result<()> {
    tx.sqlite.execute(
        "INSERT INTO source_files(
            rollout_id,path,archived,size_bytes,modified_ns,ctime_ns,device_id,inode,
            content_fingerprint,
            byte_offset,line_number,root_thread_id,parent_rollout_id,native_started,
            inherited_lines,parse_state_json,error_count,last_error,ingested_at
         ) VALUES(
            ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19
         )
         ON CONFLICT(rollout_id) DO UPDATE SET
            path=excluded.path, archived=excluded.archived, size_bytes=excluded.size_bytes,
            modified_ns=excluded.modified_ns, ctime_ns=excluded.ctime_ns,
            device_id=excluded.device_id, inode=excluded.inode,
            content_fingerprint=excluded.content_fingerprint,
            byte_offset=excluded.byte_offset, line_number=excluded.line_number,
            root_thread_id=excluded.root_thread_id, parent_rollout_id=excluded.parent_rollout_id,
            native_started=excluded.native_started, inherited_lines=excluded.inherited_lines,
            parse_state_json=excluded.parse_state_json,
            error_count=source_files.error_count+excluded.error_count,
            last_error=excluded.last_error, ingested_at=excluded.ingested_at",
        params![
            checkpoint.rollout_id,
            checkpoint.path,
            checkpoint.archived as i64,
            checkpoint.size_bytes as i64,
            checkpoint.modified_ns as i64,
            checkpoint.ctime_ns,
            checkpoint.device_id,
            checkpoint.inode,
            checkpoint.content_fingerprint,
            checkpoint.byte_offset as i64,
            checkpoint.line_number as i64,
            checkpoint.root_thread_id,
            checkpoint.parent_rollout_id,
            checkpoint.native_started as i64,
            checkpoint.inherited_lines as i64,
            checkpoint.parse_state_json,
            checkpoint.error_count as i64,
            checkpoint.last_error,
            checkpoint.ingested_at,
        ],
    )?;
    Ok(())
}

/// Scalar metadata refresh for a source whose normalized projection did not
/// change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) struct UnchangedSourceUpdate {
    pub rollout_id: String,
    pub archived: bool,
    pub size_bytes: u64,
    pub modified_ns: u64,
    pub ctime_ns: Option<i64>,
    pub device_id: Option<i64>,
    pub inode: Option<i64>,
    pub content_fingerprint: Option<String>,
    /// Preserve the old write budget: touch the normalized rollout only when
    /// its archive bit actually changed.
    pub rollout_archive_changed: bool,
}

pub(in crate::ingest) fn mark_source_unchanged(
    tx: &super::ProjectionTx<'_>,
    update: &UnchangedSourceUpdate,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE source_files SET
            archived=?1,size_bytes=?2,modified_ns=?3,ctime_ns=?4,device_id=?5,inode=?6,
            content_fingerprint=COALESCE(?7,content_fingerprint)
         WHERE rollout_id=?8",
        params![
            update.archived as i64,
            update.size_bytes as i64,
            update.modified_ns as i64,
            update.ctime_ns,
            update.device_id,
            update.inode,
            update.content_fingerprint,
            update.rollout_id,
        ],
    )?;
    if update.rollout_archive_changed {
        tx.sqlite.execute(
            "UPDATE rollouts SET archived=?1 WHERE id=?2",
            params![update.archived as i64, update.rollout_id],
        )?;
    }
    Ok(())
}

/// Move an unchanged rollout checkpoint to a verified representation sibling.
///
/// The caller has already proven that the new path contains the complete
/// committed logical extent. Preserve the normalized projection and its
/// ingestion timestamp while replacing only source-location metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) struct SourceHandoffUpdate {
    pub rollout_id: String,
    pub path: String,
    pub archived: bool,
    pub size_bytes: u64,
    pub modified_ns: u64,
    pub ctime_ns: Option<i64>,
    pub device_id: Option<i64>,
    pub inode: Option<i64>,
    pub rollout_archive_changed: bool,
}

pub(in crate::ingest) fn mark_source_handoff_unchanged(
    tx: &super::ProjectionTx<'_>,
    update: &SourceHandoffUpdate,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE source_files SET
            path=?1,archived=?2,size_bytes=?3,modified_ns=?4,ctime_ns=?5,device_id=?6,inode=?7
         WHERE rollout_id=?8",
        params![
            update.path,
            update.archived as i64,
            update.size_bytes as i64,
            update.modified_ns as i64,
            update.ctime_ns,
            update.device_id,
            update.inode,
            update.rollout_id,
        ],
    )?;
    if update.rollout_archive_changed {
        tx.sqlite.execute(
            "UPDATE rollouts SET archived=?1 WHERE id=?2",
            params![update.archived as i64, update.rollout_id],
        )?;
    }
    Ok(())
}

/// One conflicting durable source already claiming a candidate path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ingest) struct PathConflict {
    pub rollout_id: String,
    pub root_thread_id: Option<String>,
}

/// Find a different rollout whose durable checkpoint already owns `path`.
pub(in crate::ingest) fn find_path_conflict(
    tx: &super::ProjectionTx<'_>,
    path: &str,
    current_rollout_id: &str,
) -> Result<Option<PathConflict>> {
    tx.sqlite
        .query_row(
            "SELECT rollout_id,root_thread_id
         FROM source_files
         WHERE path=?1 AND rollout_id<>?2",
            params![path, current_rollout_id],
            |row| {
                Ok(PathConflict {
                    rollout_id: row.get(0)?,
                    root_thread_id: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

/// Delete one durable source checkpoint in the caller's transaction.
///
/// Normalized rollout removal remains a separate named operation so callers
/// can read surviving source evidence before this checkpoint disappears.
pub(in crate::ingest) fn delete_source_checkpoint(
    tx: &super::ProjectionTx<'_>,
    rollout_id: &str,
) -> Result<()> {
    tx.sqlite
        .execute("DELETE FROM source_files WHERE rollout_id=?1", [rollout_id])?;
    Ok(())
}

/// Clear the two-scan shrink confirmation only after the replacement
/// checkpoint has been written successfully in the same transaction.
pub(in crate::ingest) fn clear_confirmed_shrink(
    tx: &super::ProjectionTx<'_>,
    rollout_id: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "DELETE FROM app_meta WHERE key=?1",
        [format!("pending_source_shrink:{rollout_id}")],
    )?;
    Ok(())
}

/// Reconcile lifecycle evidence after the source path is durable.
///
/// The current promoted rollout must be rebuilt first. Only then may this
/// newly checkpointed parent replay the children it observed; equal-time
/// observations depend on the now-durable source path for their stable order.
pub(in crate::ingest) fn rematerialize_after_checkpoint(
    tx: &super::ProjectionTx<'_>,
    rollout_id: &str,
) -> Result<()> {
    agents::rematerialize_surviving_observation(tx, rollout_id)?;
    agents::rematerialize_observed_children(tx, rollout_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Db;
    use rusqlite::{Connection, params};

    type UnchangedMetadataRow = (
        String,
        i64,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        i64,
    );

    fn open_database() -> (tempfile::TempDir, Connection) {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        (temp, connection)
    }

    fn checkpoint(rollout_id: &str, path: &str) -> SourceCheckpointWrite {
        SourceCheckpointWrite {
            rollout_id: rollout_id.to_owned(),
            path: path.to_owned(),
            archived: false,
            size_bytes: 101,
            modified_ns: 102,
            ctime_ns: Some(103),
            device_id: Some(104),
            inode: Some(105),
            content_fingerprint: "fingerprint-one".to_owned(),
            byte_offset: 99,
            line_number: 7,
            root_thread_id: "thread".to_owned(),
            parent_rollout_id: Some("parent".to_owned()),
            native_started: true,
            inherited_lines: 3,
            parse_state_json: r#"{"owner_id":"owner"}"#.to_owned(),
            error_count: 2,
            last_error: Some("line 7: malformed".to_owned()),
            ingested_at: "2026-07-25T12:00:00Z".to_owned(),
        }
    }

    fn insert_thread_and_rollout(connection: &Connection, thread_id: &str, rollout_id: &str) {
        connection
            .execute(
                "INSERT OR IGNORE INTO threads(id,started_at,last_event_at)
                 VALUES(?1,'2026-07-25T00:00:00Z','2026-07-25T00:00:00Z')",
                [thread_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES(?1,?2,'2026-07-25T00:00:00Z','2026-07-25T00:00:00Z')",
                params![rollout_id, thread_id],
            )
            .unwrap();
    }

    #[derive(Debug, PartialEq, Eq)]
    struct StoredCheckpoint {
        path: String,
        archived: i64,
        size_bytes: i64,
        modified_ns: i64,
        ctime_ns: Option<i64>,
        device_id: Option<i64>,
        inode: Option<i64>,
        content_fingerprint: String,
        byte_offset: i64,
        line_number: i64,
        root_thread_id: Option<String>,
        parent_rollout_id: Option<String>,
        native_started: i64,
        inherited_lines: i64,
        parse_state_json: String,
        error_count: i64,
        last_error: Option<String>,
        ingested_at: String,
    }

    #[test]
    fn saves_exact_checkpoint_fields_and_accumulates_only_new_errors() {
        let (_temp, mut connection) = open_database();
        let mut first = checkpoint("owner", "/sources/one.jsonl");
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        save_source_checkpoint(&tx, &first).unwrap();
        tx.commit().unwrap();

        first.path = "/sources/two.jsonl".to_owned();
        first.archived = true;
        first.size_bytes = 201;
        first.modified_ns = 202;
        first.ctime_ns = None;
        first.device_id = Some(204);
        first.inode = None;
        first.content_fingerprint = "fingerprint-two".to_owned();
        first.byte_offset = 199;
        first.line_number = 17;
        first.root_thread_id = "other-thread".to_owned();
        first.parent_rollout_id = None;
        first.native_started = false;
        first.inherited_lines = 13;
        first.parse_state_json = r#"{"owner_id":"owner","forked":true}"#.to_owned();
        first.error_count = 5;
        first.last_error = None;
        first.ingested_at = "2026-07-25T13:00:00Z".to_owned();
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        save_source_checkpoint(&tx, &first).unwrap();
        tx.commit().unwrap();

        let row = connection
            .query_row(
                "SELECT path,archived,size_bytes,modified_ns,ctime_ns,device_id,inode,
                        content_fingerprint,byte_offset,line_number,root_thread_id,
                        parent_rollout_id,native_started,inherited_lines,parse_state_json,
                        error_count,last_error,ingested_at
                 FROM source_files WHERE rollout_id='owner'",
                [],
                |row| {
                    Ok(StoredCheckpoint {
                        path: row.get(0)?,
                        archived: row.get(1)?,
                        size_bytes: row.get(2)?,
                        modified_ns: row.get(3)?,
                        ctime_ns: row.get(4)?,
                        device_id: row.get(5)?,
                        inode: row.get(6)?,
                        content_fingerprint: row.get(7)?,
                        byte_offset: row.get(8)?,
                        line_number: row.get(9)?,
                        root_thread_id: row.get(10)?,
                        parent_rollout_id: row.get(11)?,
                        native_started: row.get(12)?,
                        inherited_lines: row.get(13)?,
                        parse_state_json: row.get(14)?,
                        error_count: row.get(15)?,
                        last_error: row.get(16)?,
                        ingested_at: row.get(17)?,
                    })
                },
            )
            .unwrap();
        assert_eq!(
            row,
            StoredCheckpoint {
                path: "/sources/two.jsonl".to_owned(),
                archived: 1,
                size_bytes: 201,
                modified_ns: 202,
                ctime_ns: None,
                device_id: Some(204),
                inode: None,
                content_fingerprint: "fingerprint-two".to_owned(),
                byte_offset: 199,
                line_number: 17,
                root_thread_id: Some("other-thread".to_owned()),
                parent_rollout_id: None,
                native_started: 0,
                inherited_lines: 13,
                parse_state_json: r#"{"owner_id":"owner","forked":true}"#.to_owned(),
                error_count: 7,
                last_error: None,
                ingested_at: "2026-07-25T13:00:00Z".to_owned(),
            }
        );
    }

    #[test]
    fn unchanged_refresh_preserves_or_replaces_fingerprint_and_updates_archive_once_requested() {
        let (_temp, mut connection) = open_database();
        insert_thread_and_rollout(&connection, "thread", "owner");
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        save_source_checkpoint(&tx, &checkpoint("owner", "/sources/one.jsonl")).unwrap();
        tx.commit().unwrap();

        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        mark_source_unchanged(
            &tx,
            &UnchangedSourceUpdate {
                rollout_id: "owner".to_owned(),
                archived: true,
                size_bytes: 301,
                modified_ns: 302,
                ctime_ns: Some(303),
                device_id: None,
                inode: Some(305),
                content_fingerprint: None,
                rollout_archive_changed: true,
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let first: UnchangedMetadataRow = connection
            .query_row(
                "SELECT content_fingerprint,archived,size_bytes,modified_ns,
                            ctime_ns,device_id,inode,
                            (SELECT archived FROM rollouts WHERE id='owner')
                     FROM source_files WHERE rollout_id='owner'",
                [],
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
            first,
            (
                "fingerprint-one".to_owned(),
                1,
                301,
                302,
                Some(303),
                None,
                Some(305),
                1,
            )
        );

        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        mark_source_unchanged(
            &tx,
            &UnchangedSourceUpdate {
                rollout_id: "owner".to_owned(),
                archived: true,
                size_bytes: 301,
                modified_ns: 302,
                ctime_ns: Some(303),
                device_id: None,
                inode: Some(305),
                content_fingerprint: Some("audited-fingerprint".to_owned()),
                rollout_archive_changed: false,
            },
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT content_fingerprint FROM source_files WHERE rollout_id='owner'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "audited-fingerprint"
        );
    }

    #[test]
    fn path_conflict_excludes_the_current_rollout_and_returns_owned_evidence() {
        let (_temp, mut connection) = open_database();
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        save_source_checkpoint(&tx, &checkpoint("owner", "/sources/one.jsonl")).unwrap();
        assert_eq!(
            find_path_conflict(&tx, "/sources/one.jsonl", "other").unwrap(),
            Some(PathConflict {
                rollout_id: "owner".to_owned(),
                root_thread_id: Some("thread".to_owned()),
            })
        );
        assert_eq!(
            find_path_conflict(&tx, "/sources/one.jsonl", "owner").unwrap(),
            None
        );
        tx.rollback().unwrap();
    }

    #[test]
    fn source_checkpoint_deletion_obeys_the_caller_transaction() {
        let (_temp, mut connection) = open_database();
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        save_source_checkpoint(&tx, &checkpoint("owner", "/sources/one.jsonl")).unwrap();
        tx.commit().unwrap();

        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        delete_source_checkpoint(&tx, "owner").unwrap();
        assert!(
            find_path_conflict(&tx, "/sources/one.jsonl", "other")
                .unwrap()
                .is_none()
        );
        tx.rollback().unwrap();
        assert!(
            connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id='owner')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );

        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        delete_source_checkpoint(&tx, "owner").unwrap();
        tx.commit().unwrap();
        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id='owner')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    }

    #[test]
    fn confirmed_shrink_clear_is_owner_scoped() {
        let (_temp, mut connection) = open_database();
        connection
            .execute_batch(
                "INSERT INTO app_meta(key,value) VALUES
                    ('pending_source_shrink:owner','candidate'),
                    ('pending_source_shrink:other','other candidate'),
                    ('unrelated','value');",
            )
            .unwrap();
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        clear_confirmed_shrink(&tx, "owner").unwrap();
        tx.commit().unwrap();
        let keys = connection
            .prepare("SELECT key FROM app_meta ORDER BY key")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            keys,
            vec![
                "pending_source_shrink:other".to_owned(),
                "unrelated".to_owned()
            ]
        );
    }

    #[test]
    fn rematerialization_runs_current_rollout_before_observed_children() {
        let (_temp, mut connection) = open_database();
        insert_thread_and_rollout(&connection, "thread", "parent");
        connection
            .execute(
                "INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,status
                 ) VALUES('parent','thread',NULL,'observer','2026-07-25T00:00:00Z','running'),
                         ('child','thread',NULL,'parent','2026-07-25T00:00:00Z','running')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,status,payload_json
                 ) VALUES(
                    'parent:1','thread','parent','2026-07-25T00:00:01Z',1,
                    'subagent','completed','{\"agent_thread_id\":\"child\"}'
                 )",
                [],
            )
            .unwrap();
        connection
            .execute_batch(
                "CREATE TABLE rematerialization_trace(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_id TEXT NOT NULL
                 );
                 CREATE TRIGGER trace_agent_rematerialization
                 AFTER DELETE ON agent_runs
                 BEGIN
                    INSERT INTO rematerialization_trace(agent_id) VALUES(OLD.id);
                 END;",
            )
            .unwrap();

        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        rematerialize_after_checkpoint(&tx, "parent").unwrap();
        tx.commit().unwrap();

        let trace = connection
            .prepare("SELECT agent_id FROM rematerialization_trace ORDER BY sequence")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(trace, vec!["parent".to_owned(), "child".to_owned()]);
        let child = connection
            .query_row(
                "SELECT parent_rollout_id,status,completed_at
                 FROM agent_runs WHERE id='child'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            child,
            (
                Some("parent".to_owned()),
                "completed".to_owned(),
                Some("2026-07-25T00:00:01Z".to_owned()),
            )
        );
    }

    #[test]
    fn checkpoint_and_confirmation_writes_roll_back_with_the_caller_transaction() {
        let (_temp, mut connection) = open_database();
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        let first = checkpoint("owner", "/sources/one.jsonl");
        save_source_checkpoint(&tx, &first).unwrap();
        tx.sqlite
            .execute(
                "INSERT INTO app_meta(key,value) VALUES('pending_source_shrink:owner','candidate')",
                [],
            )
            .unwrap();
        tx.commit().unwrap();

        let mut changed = first;
        changed.byte_offset = 100;
        changed.error_count = 4;
        let tx = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        save_source_checkpoint(&tx, &changed).unwrap();
        clear_confirmed_shrink(&tx, "owner").unwrap();
        tx.rollback().unwrap();

        let checkpoint_row = connection
            .query_row(
                "SELECT byte_offset,error_count FROM source_files WHERE rollout_id='owner'",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap();
        assert_eq!(checkpoint_row, (99, 2));
        assert_eq!(
            connection
                .query_row(
                    "SELECT value FROM app_meta WHERE key='pending_source_shrink:owner'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "candidate"
        );
    }
}
