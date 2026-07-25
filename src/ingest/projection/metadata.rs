use super::super::protocol::{
    CursorState, DecodedMetadataRecord, MetadataIntent, MetadataUpdate, OwnerMeta,
};
use super::{agents, events, lifecycle};
use anyhow::Result;
use rusqlite::params;

/// Materialize one rollout owner in dependency order: thread, rollout,
/// native-agent promotion, then exact aggregate thread bounds.
pub(in crate::ingest) fn upsert_owner(
    tx: &super::ProjectionTx<'_>,
    owner: &OwnerMeta,
    archived: bool,
) -> Result<()> {
    let is_root = owner.owner_id == owner.thread_id;
    tx.sqlite.execute(
        "INSERT INTO threads(
            id,cwd,project,repository_url,branch,source,thread_source,source_json,
            started_at,last_event_at,root_metadata_seen
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?9,?10)
         ON CONFLICT(id) DO UPDATE SET
            cwd=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.cwd
                     WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.cwd,excluded.cwd)
                     ELSE threads.cwd END,
            project=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.project
                         WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.project,excluded.project)
                         ELSE threads.project END,
            repository_url=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.repository_url
                                WHEN threads.root_metadata_seen=0
                                THEN COALESCE(threads.repository_url,excluded.repository_url)
                                ELSE threads.repository_url END,
            branch=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.branch
                        WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.branch,excluded.branch)
                        ELSE threads.branch END,
            source=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.source
                        WHEN threads.root_metadata_seen=0 THEN COALESCE(threads.source,excluded.source)
                        ELSE threads.source END,
            thread_source=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.thread_source
                               WHEN threads.root_metadata_seen=0
                               THEN COALESCE(threads.thread_source,excluded.thread_source)
                               ELSE threads.thread_source END,
            source_json=CASE WHEN excluded.root_metadata_seen=1 THEN excluded.source_json
                             WHEN threads.root_metadata_seen=0
                             THEN COALESCE(threads.source_json,excluded.source_json)
                             ELSE threads.source_json END,
            root_metadata_seen=MAX(threads.root_metadata_seen,excluded.root_metadata_seen),
            started_at=MIN(threads.started_at,excluded.started_at),
            last_event_at=MAX(threads.last_event_at,excluded.last_event_at)",
        params![
            owner.thread_id,
            owner.cwd,
            owner.project,
            owner.repository_url,
            owner.branch,
            owner.source,
            owner.thread_source,
            owner.source_json,
            owner.timestamp,
            is_root as i64,
        ],
    )?;
    tx.sqlite.execute(
        "INSERT INTO rollouts(
            id,thread_id,parent_rollout_id,parent_thread_id,agent_path,agent_nickname,
            cwd,started_at,last_event_at,archived
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8,?9)
         ON CONFLICT(id) DO UPDATE SET
            thread_id=excluded.thread_id,parent_rollout_id=excluded.parent_rollout_id,
            parent_thread_id=excluded.parent_thread_id,agent_path=excluded.agent_path,
            agent_nickname=excluded.agent_nickname,cwd=COALESCE(excluded.cwd,rollouts.cwd),
            archived=excluded.archived",
        params![
            owner.owner_id,
            owner.thread_id,
            owner.parent_rollout_id,
            owner.parent_thread_id,
            owner.agent_path,
            owner.agent_nickname,
            owner.cwd,
            owner.timestamp,
            archived as i64,
        ],
    )?;
    agents::upsert_native_run(tx, owner)?;
    recompute_thread_bounds(tx, &owner.thread_id)?;
    Ok(())
}

/// Restore the exact thread interval from its currently projected rollouts.
pub(in crate::ingest) fn recompute_thread_bounds(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET
            started_at=(SELECT MIN(started_at) FROM rollouts WHERE thread_id=?1),
            last_event_at=(SELECT MAX(last_event_at) FROM rollouts WHERE thread_id=?1)
         WHERE id=?1 AND EXISTS(SELECT 1 FROM rollouts WHERE thread_id=?1)",
        [thread_id],
    )?;
    Ok(())
}

/// Apply one typed metadata record and publish its cursor transition only
/// after every projection write succeeds.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedMetadataRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    let touch_rollout = match &record.intent {
        MetadataIntent::Owner(update) => {
            apply_owner_update(tx, &candidate, &record.timestamp, update)?;
            false
        }
        MetadataIntent::RootUserTitle(title) => {
            if let Some(title) = title.as_deref() {
                apply_root_user_title(tx, &candidate.thread_id, &record.timestamp, title)?;
            }
            true
        }
        MetadataIntent::ThreadName { title, event } => {
            if let Some(title) = title.as_deref() {
                apply_timestamped_thread_title(tx, &candidate.thread_id, &record.timestamp, title)?;
            }
            if let Some(event) = event.as_deref() {
                events::apply(tx, &candidate, record.source_line, &record.timestamp, event)?;
            }
            true
        }
        MetadataIntent::IgnoredSession => false,
    };

    // Session metadata historically advances only the thread and source
    // cursor. Title events retain the event-message owner touch.
    if touch_rollout {
        lifecycle::touch_owner(tx, &candidate, &record.timestamp)?;
    }
    *state = candidate;
    Ok(())
}

fn apply_owner_update(
    tx: &super::ProjectionTx<'_>,
    state: &CursorState,
    timestamp: &str,
    update: &MetadataUpdate,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET
            cwd=CASE WHEN ?1=1 THEN ?2
                     WHEN root_metadata_seen=0 THEN COALESCE(cwd,?2) ELSE cwd END,
            project=CASE WHEN ?1=1 THEN ?3
                         WHEN root_metadata_seen=0 THEN COALESCE(project,?3) ELSE project END,
            repository_url=CASE WHEN ?1=1 THEN ?4
                                WHEN root_metadata_seen=0 THEN COALESCE(repository_url,?4)
                                ELSE repository_url END,
            branch=CASE WHEN ?1=1 THEN ?5
                        WHEN root_metadata_seen=0 THEN COALESCE(branch,?5) ELSE branch END,
            source=CASE WHEN ?1=1 THEN ?6
                        WHEN root_metadata_seen=0 THEN COALESCE(source,?6) ELSE source END,
            thread_source=CASE WHEN ?1=1 THEN ?7
                               WHEN root_metadata_seen=0 THEN COALESCE(thread_source,?7)
                               ELSE thread_source END,
            root_metadata_seen=MAX(root_metadata_seen,?1),
            last_event_at=MAX(last_event_at,?8)
         WHERE id=?9",
        params![
            update.is_root as i64,
            update.fields.cwd,
            update.fields.project,
            update.fields.repository_url,
            update.fields.branch,
            update.fields.source,
            update.fields.thread_source,
            timestamp,
            state.thread_id,
        ],
    )?;
    if let Some(title) = update.title.as_deref() {
        apply_timestamped_thread_title(tx, &state.thread_id, timestamp, title)?;
    }
    Ok(())
}

/// Lowest-authority title: a legacy top-level prompt fills a blank title and
/// can never replace any previously selected source.
pub(in crate::ingest) fn apply_legacy_prompt_title_fallback(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
    title: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET title=COALESCE(title, ?1) WHERE id=?2",
        params![title, thread_id],
    )?;
    Ok(())
}

/// Root user-message titles replace only an untimestamped fallback.
fn apply_root_user_title(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
    timestamp: &str,
    title: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET title=?1,title_updated_at=?2
         WHERE id=?3 AND title_updated_at IS NULL",
        params![title, timestamp, thread_id],
    )?;
    Ok(())
}

/// Timestamped metadata and rename records share one last-write-wins rule;
/// equality deliberately permits the later source record to win.
fn apply_timestamped_thread_title(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
    timestamp: &str,
    title: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET title=?1,title_updated_at=?2
         WHERE id=?3 AND (title_updated_at IS NULL OR title_updated_at<=?2)",
        params![title, timestamp, thread_id],
    )?;
    Ok(())
}

/// The session index is synchronized after rollout projection and is therefore
/// the final title authority for each scan, independent of source timestamps.
pub(in crate::ingest) fn apply_indexed_thread_title(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
    timestamp: &str,
    title: &str,
) -> Result<usize> {
    Ok(tx.sqlite.execute(
        "UPDATE threads SET title=?1,title_updated_at=?2
         WHERE id=?3 AND (title IS NULL OR title<>?1 OR title_updated_at IS NULL OR title_updated_at<>?2)",
        params![title, timestamp, thread_id],
    )?)
}

pub(in crate::ingest) fn clear_projected_thread_title(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "UPDATE threads SET title=NULL,title_updated_at=NULL WHERE id=?1",
        [thread_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::protocol::{MetadataStateTransition, ProjectedEvent, SessionMetadata};
    use rusqlite::Connection;

    fn state(owner: &str, thread: &str) -> CursorState {
        CursorState {
            owner_id: owner.into(),
            thread_id: thread.into(),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(
                    id TEXT PRIMARY KEY,title TEXT,title_updated_at TEXT,cwd TEXT,project TEXT,
                    repository_url TEXT,branch TEXT,source TEXT,thread_source TEXT,
                    root_metadata_seen INTEGER NOT NULL DEFAULT 0,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE rollouts(id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL);
                 CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 INSERT INTO threads(id,last_event_at)
                 VALUES('thread-1','2026-07-25T08:00:00.000000000Z');
                 INSERT INTO rollouts(id,last_event_at)
                 VALUES('thread-1','2026-07-25T08:00:00.000000000Z'),
                       ('child-1','2026-07-25T08:00:00.000000000Z');",
            )
            .unwrap();
        connection
    }

    fn owner_record(
        timestamp: &str,
        is_root: bool,
        fields: SessionMetadata,
        title: Option<&str>,
    ) -> DecodedMetadataRecord {
        DecodedMetadataRecord {
            source_line: 7,
            timestamp: timestamp.into(),
            transition: MetadataStateTransition {
                last_timestamp: timestamp.into(),
            },
            intent: MetadataIntent::Owner(Box::new(MetadataUpdate {
                is_root,
                fields,
                title: title.map(str::to_owned),
            })),
        }
    }

    fn title_record(line: u64, timestamp: &str, title: &str) -> DecodedMetadataRecord {
        DecodedMetadataRecord {
            source_line: line,
            timestamp: timestamp.into(),
            transition: MetadataStateTransition {
                last_timestamp: timestamp.into(),
            },
            intent: MetadataIntent::ThreadName {
                title: Some(title.into()),
                event: Some(Box::new(ProjectedEvent {
                    kind: "state".into(),
                    role: None,
                    label: Some("Thread renamed".into()),
                    body: Some(title.into()),
                    status: None,
                    tool_name: None,
                    call_id: None,
                    duration_ms: None,
                    metadata: None,
                })),
            },
        }
    }

    #[test]
    fn title_authority_ladder_is_exact_and_session_index_is_final() {
        let mut connection = connection();
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply_legacy_prompt_title_fallback(&transaction, "thread-1", "Legacy prompt").unwrap();

        let mut cursor = state("thread-1", "thread-1");
        let root_user = DecodedMetadataRecord {
            source_line: 8,
            timestamp: "2026-07-25T08:01:00.000000000Z".into(),
            transition: MetadataStateTransition {
                last_timestamp: "2026-07-25T08:01:00.000000000Z".into(),
            },
            intent: MetadataIntent::RootUserTitle(Some("Root prompt".into())),
        };
        apply(&transaction, &mut cursor, &root_user).unwrap();

        let older = owner_record(
            "2026-07-25T08:00:30.000000000Z",
            true,
            SessionMetadata::default(),
            Some("Older metadata"),
        );
        apply(&transaction, &mut cursor, &older).unwrap();
        let equal = owner_record(
            "2026-07-25T08:01:00.000000000Z",
            true,
            SessionMetadata::default(),
            Some("Equal-time metadata"),
        );
        apply(&transaction, &mut cursor, &equal).unwrap();
        let rename = title_record(9, "2026-07-25T08:02:00.000000000Z", "Later rename");
        apply(&transaction, &mut cursor, &rename).unwrap();
        apply_indexed_thread_title(
            &transaction,
            "thread-1",
            "2026-07-25T07:00:00.000000000Z",
            "Indexed title",
        )
        .unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT title,title_updated_at FROM threads WHERE id='thread-1'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            (
                "Indexed title".into(),
                "2026-07-25T07:00:00.000000000Z".into()
            )
        );
    }

    #[test]
    fn repeated_root_metadata_clears_omitted_fields_and_child_cannot_refill_them() {
        let mut connection = connection();
        let mut root = state("thread-1", "thread-1");
        let first = owner_record(
            "2026-07-25T08:01:00.000000000Z",
            true,
            SessionMetadata {
                cwd: Some("/tmp/first".into()),
                project: Some("first".into()),
                repository_url: Some("https://example.test/first".into()),
                branch: Some("main".into()),
                source: Some("cli".into()),
                thread_source: Some("desktop".into()),
            },
            None,
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut root, &first).unwrap();
        let clearing = owner_record(
            "2026-07-25T08:02:00.000000000Z",
            true,
            SessionMetadata::default(),
            None,
        );
        apply(&transaction, &mut root, &clearing).unwrap();

        let mut child = state("child-1", "thread-1");
        let child_update = owner_record(
            "2026-07-25T08:03:00.000000000Z",
            false,
            SessionMetadata {
                cwd: Some("/tmp/child".into()),
                project: Some("child".into()),
                repository_url: None,
                branch: Some("child".into()),
                source: Some("subagent".into()),
                thread_source: None,
            },
            None,
        );
        apply(&transaction, &mut child, &child_update).unwrap();
        transaction.commit().unwrap();

        let values: (Option<String>, Option<String>, Option<String>, i64) = connection
            .query_row(
                "SELECT cwd,project,branch,root_metadata_seen FROM threads WHERE id='thread-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(values, (None, None, None, 1));
    }

    #[test]
    fn metadata_only_record_advances_thread_and_cursor_but_not_rollout() {
        let mut connection = connection();
        let mut cursor = state("thread-1", "thread-1");
        let timestamp = "2026-07-25T09:00:00.000000000Z";
        let record = owner_record(timestamp, true, SessionMetadata::default(), None);
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        apply(&transaction, &mut cursor, &record).unwrap();
        transaction.commit().unwrap();

        assert_eq!(cursor.last_timestamp.as_deref(), Some(timestamp));
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_event_at FROM threads WHERE id='thread-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            timestamp
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_event_at FROM rollouts WHERE id='thread-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "2026-07-25T08:00:00.000000000Z"
        );
    }

    #[test]
    fn projection_failure_does_not_publish_cursor() {
        let mut connection = connection();
        connection.execute("DROP TABLE threads", []).unwrap();
        let mut cursor = state("thread-1", "thread-1");
        cursor.last_timestamp = Some("2026-07-25T08:00:00.000000000Z".into());
        let before = serde_json::to_string(&cursor).unwrap();
        let record = owner_record(
            "2026-07-25T09:00:00.000000000Z",
            true,
            SessionMetadata::default(),
            None,
        );
        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        assert!(apply(&transaction, &mut cursor, &record).is_err());
        assert_eq!(serde_json::to_string(&cursor).unwrap(), before);
    }
}
