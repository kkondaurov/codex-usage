use super::{
    catalog::SelectedSourceExtent,
    checkpoints::{PendingSourceShrink, SourceCheckpoint},
    protocol::CursorState,
    source::FileIdentity,
};
use crate::storage::Db;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::{collections::HashMap, path::PathBuf};

fn checkpoint_from_row(row: &Row<'_>) -> rusqlite::Result<SourceCheckpoint> {
    let state_json: String = row.get(10)?;
    Ok(SourceCheckpoint {
        archived: row.get::<_, i64>(0)? != 0,
        size: row.get::<_, i64>(1)?.max(0) as u64,
        modified_ns: row.get::<_, i64>(2)?.max(0) as u64,
        identity: FileIdentity {
            ctime_ns: row.get(3)?,
            device_id: row.get(4)?,
            inode: row.get(5)?,
        },
        fingerprint: row.get(6)?,
        offset: row.get::<_, i64>(7)?.max(0) as u64,
        line_number: row.get::<_, i64>(8)?.max(0) as u64,
        inherited_lines: row.get::<_, i64>(9)?.max(0) as u64,
        last_error: row.get(11)?,
        state: serde_json::from_str(&state_json).unwrap_or_else(|_| CursorState::default()),
    })
}

pub(super) fn load_selected_source_extents(
    db: &Db,
) -> Result<HashMap<String, SelectedSourceExtent>> {
    let connection = db.connect()?;
    let mut statement = connection.prepare(
        "SELECT rollout_id,path,size_bytes,byte_offset,content_fingerprint FROM source_files",
    )?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                SelectedSourceExtent {
                    path: PathBuf::from(row.get::<_, String>(1)?),
                    raw_size: row.get::<_, i64>(2)?.max(0) as u64,
                    committed_size: row.get::<_, i64>(3)?.max(0) as u64,
                    fingerprint: row.get(4)?,
                },
            ))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()
        .map_err(Into::into)
}

pub(super) fn load_checkpoint(
    connection: &Connection,
    rollout_id: &str,
) -> Result<Option<SourceCheckpoint>> {
    connection
        .query_row(
            "SELECT archived,size_bytes,modified_ns,ctime_ns,device_id,inode,content_fingerprint,
                    byte_offset,line_number,inherited_lines,parse_state_json,last_error
             FROM source_files WHERE rollout_id=?1",
            [rollout_id],
            checkpoint_from_row,
        )
        .optional()
        .map_err(Into::into)
}

pub(super) fn load_checkpoint_by_path(
    connection: &Connection,
    path: &str,
) -> Result<Option<SourceCheckpoint>> {
    connection
        .query_row(
            "SELECT archived,size_bytes,modified_ns,ctime_ns,device_id,inode,content_fingerprint,
                    byte_offset,line_number,inherited_lines,parse_state_json,last_error
             FROM source_files WHERE path=?1",
            [path],
            checkpoint_from_row,
        )
        .optional()
        .map_err(Into::into)
}

pub(super) fn pending_source_shrink_key(owner_id: &str) -> String {
    format!("pending_source_shrink:{owner_id}")
}

pub(super) fn clear_pending_source_shrink(connection: &Connection, owner_id: &str) -> Result<()> {
    connection.execute(
        "DELETE FROM app_meta WHERE key=?1",
        [pending_source_shrink_key(owner_id)],
    )?;
    Ok(())
}

pub(super) fn same_source_shrink_was_observed(
    connection: &Connection,
    owner_id: &str,
    path: &str,
    size: u64,
    fingerprint: &str,
) -> Result<bool> {
    let key = pending_source_shrink_key(owner_id);
    let candidate = PendingSourceShrink::new(path, size, fingerprint);
    let previous = connection
        .query_row("SELECT value FROM app_meta WHERE key=?1", [&key], |row| {
            row.get::<_, String>(0)
        })
        .optional()?
        .and_then(|value| serde_json::from_str::<PendingSourceShrink>(&value).ok());
    if previous.as_ref() == Some(&candidate) {
        return Ok(true);
    }
    connection.execute(
        "INSERT INTO app_meta(key,value) VALUES(?1,?2)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![key, serde_json::to_string(&candidate)?],
    )?;
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::super::checkpoints::{
        ChunkedFingerprint, FINGERPRINT_CHUNK_BYTES, source_content_digest,
    };
    use super::*;
    use crate::storage::Db;
    use rusqlite::params;
    use serde_json::Value;

    #[test]
    fn loads_exact_rows_and_defaults_malformed_state() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        connection
            .execute(
                "INSERT INTO source_files(
                    rollout_id,path,archived,size_bytes,modified_ns,ctime_ns,device_id,inode,
                    content_fingerprint,byte_offset,line_number,inherited_lines,parse_state_json,
                    last_error,ingested_at
                 ) VALUES(?1,?2,1,-9,-8,-7,20,30,'fingerprint',-6,-5,-4,'not-json',
                          'stored error','2026-07-25T00:00:00Z')",
                params!["owner", "/tmp/source.jsonl"],
            )
            .unwrap();

        let checkpoint = load_checkpoint(&connection, "owner").unwrap().unwrap();
        assert!(checkpoint.archived);
        assert_eq!(checkpoint.size, 0);
        assert_eq!(checkpoint.modified_ns, 0);
        assert_eq!(
            checkpoint.identity,
            FileIdentity {
                ctime_ns: Some(-7),
                device_id: Some(20),
                inode: Some(30),
            }
        );
        assert_eq!(checkpoint.fingerprint, "fingerprint");
        assert_eq!(checkpoint.offset, 0);
        assert_eq!(checkpoint.line_number, 0);
        assert_eq!(checkpoint.inherited_lines, 0);
        assert_eq!(checkpoint.last_error.as_deref(), Some("stored error"));
        assert_eq!(checkpoint.state.projector_generation, 0);
        assert!(checkpoint.state.owner_id.is_empty());
        assert!(checkpoint.state.thread_id.is_empty());

        let by_path = load_checkpoint_by_path(&connection, "/tmp/source.jsonl")
            .unwrap()
            .unwrap();
        assert_eq!(by_path.identity, checkpoint.identity);
        assert_eq!(by_path.last_error, checkpoint.last_error);
        assert!(load_checkpoint(&connection, "missing").unwrap().is_none());
        assert!(
            load_checkpoint_by_path(&connection, "/tmp/missing.jsonl")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn selected_source_extents_are_keyed_by_owner_and_clamp_legacy_negative_sizes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        connection
            .execute(
                "INSERT INTO source_files(
                    rollout_id,path,archived,size_bytes,modified_ns,content_fingerprint,
                    byte_offset,line_number,inherited_lines,parse_state_json,ingested_at
                 ) VALUES(?1,?2,0,-9,0,'fingerprint',-4,0,0,'{}','2026-07-25T00:00:00Z')",
                params!["owner", "/tmp/source.jsonl"],
            )
            .unwrap();
        drop(connection);

        let extents = load_selected_source_extents(&db).unwrap();
        let extent = extents.get("owner").unwrap();
        assert_eq!(extent.path, PathBuf::from("/tmp/source.jsonl"));
        assert_eq!(extent.raw_size, 0);
        assert_eq!(extent.committed_size, 0);
        assert_eq!(extent.fingerprint, "fingerprint");
    }

    #[test]
    fn pending_shrink_distinguishes_content_and_clear_resets_confirmation() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        let mut fingerprint = ChunkedFingerprint {
            size: 1,
            chunk_bytes: FINGERPRINT_CHUNK_BYTES,
            chunks: vec!["0".repeat(64)],
            audit_cursor: 0,
            audit_completed_at: 100,
        };
        let encoded = fingerprint.encode().unwrap();

        assert!(
            !same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                &encoded,
            )
            .unwrap()
        );
        let stored: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key=?1",
                ["pending_source_shrink:owner"],
                |row| row.get(0),
            )
            .unwrap();
        let stored: Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(stored["path"], "/tmp/source.jsonl");
        assert_eq!(stored["size"], 1);
        assert_eq!(stored["content_digest"], source_content_digest(&encoded));

        fingerprint.audit_cursor = 1;
        fingerprint.audit_completed_at = 200;
        assert!(
            same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                &fingerprint.encode().unwrap(),
            )
            .unwrap()
        );
        assert!(
            !same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "different-content",
            )
            .unwrap()
        );
        assert!(
            same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "different-content",
            )
            .unwrap()
        );

        clear_pending_source_shrink(&connection, "owner").unwrap();
        assert!(
            !same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "different-content",
            )
            .unwrap()
        );
    }

    #[test]
    fn pending_shrink_uses_caller_transaction_and_exact_repeat_is_read_only() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let mut connection = db.connect().unwrap();
        assert!(
            !same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "legacy-a",
            )
            .unwrap()
        );

        connection
            .execute_batch(
                "CREATE TRIGGER reject_pending_shrink_rewrite
                 BEFORE UPDATE ON app_meta
                 WHEN OLD.key='pending_source_shrink:owner'
                 BEGIN
                     SELECT RAISE(ABORT,'repeat attempted a write');
                 END;",
            )
            .unwrap();
        assert!(
            same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "legacy-a",
            )
            .unwrap()
        );
        connection
            .execute_batch("DROP TRIGGER reject_pending_shrink_rewrite")
            .unwrap();

        let transaction = connection.transaction().unwrap();
        assert!(
            !same_source_shrink_was_observed(
                &transaction,
                "owner",
                "/tmp/source.jsonl",
                1,
                "legacy-b",
            )
            .unwrap()
        );
        transaction.rollback().unwrap();
        assert!(
            same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "legacy-a",
            )
            .unwrap()
        );

        let transaction = connection.transaction().unwrap();
        clear_pending_source_shrink(&transaction, "owner").unwrap();
        transaction.rollback().unwrap();
        assert!(
            same_source_shrink_was_observed(
                &connection,
                "owner",
                "/tmp/source.jsonl",
                1,
                "legacy-a",
            )
            .unwrap()
        );

        connection
            .execute(
                "INSERT INTO app_meta(key,value) VALUES('unrelated','keep')",
                [],
            )
            .unwrap();
        assert!(
            !same_source_shrink_was_observed(
                &connection,
                "other",
                "/tmp/other.jsonl",
                2,
                "legacy-other",
            )
            .unwrap()
        );
        clear_pending_source_shrink(&connection, "owner").unwrap();
        let remaining: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM app_meta
                 WHERE key IN ('pending_source_shrink:other','unrelated')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 2);

        connection
            .execute(
                "UPDATE app_meta SET value='not-json'
                 WHERE key='pending_source_shrink:other'",
                [],
            )
            .unwrap();
        assert!(
            !same_source_shrink_was_observed(
                &connection,
                "other",
                "/tmp/other.jsonl",
                2,
                "legacy-other",
            )
            .unwrap()
        );
        assert!(
            same_source_shrink_was_observed(
                &connection,
                "other",
                "/tmp/other.jsonl",
                2,
                "legacy-other",
            )
            .unwrap()
        );
    }
}
