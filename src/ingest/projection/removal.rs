use super::{agents, checkpoint, metadata};
use crate::ingest::protocol::OwnerMeta;
use anyhow::Result;
use rusqlite::{OptionalExtension, params};

/// Projection-side consequences of removing one normalized rollout.
///
/// Root-rollout removal cannot itself inspect the surviving JSONL files. The
/// ordered path evidence is therefore returned to ingestion orchestration, which
/// may load the corresponding owner records before feeding them back through
/// [`apply_thread_metadata_reset`] in the same transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct RemovalImpact {
    pub(in crate::ingest) thread_id: Option<String>,
    pub(in crate::ingest) metadata_reset: Option<ThreadMetadataReset>,
}

/// Ordered source evidence required to rebuild root-owned thread metadata.
///
/// Paths remain plain owned values here. Projection neither opens them nor
/// depends on source mechanics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ThreadMetadataReset {
    pub(in crate::ingest) thread_id: String,
    pub(in crate::ingest) ordered_source_paths: Vec<String>,
}

/// Remove one rollout's complete normalized projection immediately.
///
/// Affected child-agent identities are captured before event deletion, then
/// rematerialized from the surviving event history after every owned row has
/// been removed. This ordering is part of the projection contract.
pub(in crate::ingest) fn remove_rollout(
    tx: &super::ProjectionTx<'_>,
    rollout_id: &str,
) -> Result<RemovalImpact> {
    let thread_id = tx
        .sqlite
        .query_row(
            "SELECT thread_id FROM rollouts WHERE id=?1",
            [rollout_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let mut affected_agent_ids = {
        let mut statement = tx.sqlite.prepare(
            "SELECT DISTINCT json_extract(payload_json,'$.agent_thread_id')
             FROM events
             WHERE rollout_id=?1 AND kind='subagent'
               AND json_extract(payload_json,'$.agent_thread_id') IS NOT NULL",
        )?;
        statement
            .query_map([rollout_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    if !affected_agent_ids
        .iter()
        .any(|agent_id| agent_id == rollout_id)
    {
        affected_agent_ids.push(rollout_id.to_owned());
    }

    tx.sqlite
        .execute("DELETE FROM usage_facts WHERE rollout_id=?1", [rollout_id])?;
    tx.sqlite
        .execute("DELETE FROM events WHERE rollout_id=?1", [rollout_id])?;
    tx.sqlite
        .execute("DELETE FROM messages WHERE rollout_id=?1", [rollout_id])?;
    tx.sqlite
        .execute("DELETE FROM tool_calls WHERE rollout_id=?1", [rollout_id])?;
    tx.sqlite
        .execute("DELETE FROM turns WHERE rollout_id=?1", [rollout_id])?;
    // Parent rollout events can create a lightweight child-agent row before
    // that child has its own rollout. Those rows deliberately have no rollout
    // foreign key and must disappear with their removed observation source.
    tx.sqlite.execute(
        "DELETE FROM agent_runs
         WHERE rollout_id IS NULL AND parent_rollout_id=?1",
        [rollout_id],
    )?;
    tx.sqlite
        .execute("DELETE FROM agent_runs WHERE rollout_id=?1", [rollout_id])?;
    tx.sqlite
        .execute("DELETE FROM rollouts WHERE id=?1", [rollout_id])?;

    affected_agent_ids.sort();
    affected_agent_ids.dedup();
    for agent_id in affected_agent_ids {
        agents::rematerialize_surviving_observation(tx, &agent_id)?;
    }

    checkpoint::clear_confirmed_shrink(tx, rollout_id)?;

    let metadata_reset = if let Some(thread_id) = thread_id.as_deref() {
        metadata::recompute_thread_bounds(tx, thread_id)?;
        (rollout_id == thread_id)
            .then(|| ordered_metadata_reset_evidence(tx, thread_id))
            .transpose()?
    } else {
        None
    };

    Ok(RemovalImpact {
        thread_id,
        metadata_reset,
    })
}

/// Delete a thread after its last rollout has been removed.
pub(in crate::ingest) fn delete_thread_if_abandoned(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
) -> Result<()> {
    tx.sqlite.execute(
        "DELETE FROM threads WHERE id=?1
         AND NOT EXISTS(SELECT 1 FROM rollouts WHERE thread_id=?1)",
        [thread_id],
    )?;
    Ok(())
}

/// Apply root-thread metadata rebuilt from successful source-owner reads.
///
/// `owners` must retain the order of `reset.ordered_source_paths`. Ingestion
/// requires every surviving source read to succeed before calling this
/// function, so the metadata reset cannot commit from partial evidence. Each
/// field independently takes the first surviving non-null value.
pub(in crate::ingest) fn apply_thread_metadata_reset(
    tx: &super::ProjectionTx<'_>,
    reset: &ThreadMetadataReset,
    owners: &[OwnerMeta],
) -> Result<()> {
    let mut cwd = None;
    let mut project = None;
    let mut repository_url = None;
    let mut branch = None;
    let mut source = None;
    let mut thread_source = None;
    for owner in owners {
        cwd = cwd.or_else(|| owner.cwd.clone());
        project = project.or_else(|| owner.project.clone());
        repository_url = repository_url.or_else(|| owner.repository_url.clone());
        branch = branch.or_else(|| owner.branch.clone());
        source = source.or_else(|| owner.source.clone());
        thread_source = thread_source.or_else(|| owner.thread_source.clone());
    }

    tx.sqlite.execute(
        "UPDATE threads SET
            title=NULL,title_updated_at=NULL,
            cwd=?1,project=?2,repository_url=?3,branch=?4,source=?5,thread_source=?6,
            source_json=NULL,root_metadata_seen=0
         WHERE id=?7",
        params![
            cwd,
            project,
            repository_url,
            branch,
            source,
            thread_source,
            reset.thread_id,
        ],
    )?;
    Ok(())
}

fn ordered_metadata_reset_evidence(
    tx: &super::ProjectionTx<'_>,
    thread_id: &str,
) -> Result<ThreadMetadataReset> {
    let ordered_source_paths = {
        let mut statement = tx.sqlite.prepare(
            "SELECT sf.path
             FROM rollouts r
             JOIN source_files sf ON sf.rollout_id=r.id
             WHERE r.thread_id=?1
             ORDER BY sf.path,r.id",
        )?;
        statement
            .query_map([thread_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    Ok(ThreadMetadataReset {
        thread_id: thread_id.to_owned(),
        ordered_source_paths,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../migrations/0001_initial.sql"))
            .unwrap();
        connection
    }

    fn owner(
        id: &str,
        cwd: Option<&str>,
        project: Option<&str>,
        repository_url: Option<&str>,
        branch: Option<&str>,
        source: Option<&str>,
        thread_source: Option<&str>,
    ) -> OwnerMeta {
        OwnerMeta {
            owner_id: id.into(),
            thread_id: "root".into(),
            parent_rollout_id: Some("root".into()),
            parent_thread_id: Some("root".into()),
            agent_path: None,
            agent_nickname: None,
            is_subagent: true,
            forked: true,
            timestamp: "2026-07-25T10:00:00.000000000Z".into(),
            cwd: cwd.map(str::to_owned),
            project: project.map(str::to_owned),
            repository_url: repository_url.map(str::to_owned),
            branch: branch.map(str::to_owned),
            source: source.map(str::to_owned),
            thread_source: thread_source.map(str::to_owned),
            source_json: None,
        }
    }

    #[test]
    fn removal_deletes_owned_rows_before_replaying_surviving_child_evidence() {
        let mut connection = connection();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('thread','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('root','thread','2026-07-10T00:00:00Z','2026-07-15T00:00:00Z'),
                    ('doomed','thread','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z'),
                    ('survivor','thread','2026-07-12T00:00:00Z','2026-07-13T00:00:00Z');
                 INSERT INTO source_files(rollout_id,path,ingested_at)
                    VALUES('doomed','/sources/doomed.jsonl','2026-07-25T00:00:00Z');
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,body,status,
                    payload_json,native
                 ) VALUES
                    ('surviving-observation','thread','survivor','2026-07-12T01:00:00Z',1,
                     'subagent','/surviving/path','running',
                     '{\"agent_thread_id\":\"child\"}',1),
                    ('removed-observation','thread','doomed','2026-07-20T00:00:00Z',2,
                     'subagent','/removed/path','completed',
                     '{\"agent_thread_id\":\"child\"}',1);
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,agent_path,started_at,status,
                    completed_at
                 ) VALUES
                    ('child','thread',NULL,'doomed','/removed/path',
                     '2026-07-12T01:00:00Z','completed','2026-07-20T00:00:00Z'),
                    ('doomed','thread','doomed','root',NULL,
                     '2026-07-01T00:00:00Z','running',NULL);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                    VALUES('doomed-turn','thread','doomed','2026-07-01T00:00:00Z','running');
                 INSERT INTO messages(
                    id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
                 ) VALUES('doomed-message','thread','doomed','doomed-turn',
                    '2026-07-01T00:00:01Z','user','message',3);
                 INSERT INTO tool_calls(
                    id,call_id,thread_id,rollout_id,turn_id,started_at,name,status
                 ) VALUES('doomed-tool','call','thread','doomed','doomed-turn',
                    '2026-07-01T00:00:02Z','exec','running');
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
                 ) VALUES('doomed-usage','thread','doomed','doomed-turn',
                    '2026-07-01T00:00:03Z',4,'model',10,2,5,1,15);
                 INSERT INTO app_meta(key,value)
                    VALUES('pending_source_shrink:doomed','candidate');",
            )
            .unwrap();

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        let impact = remove_rollout(&transaction, "doomed").unwrap();
        transaction.commit().unwrap();

        assert_eq!(impact.thread_id.as_deref(), Some("thread"));
        assert_eq!(impact.metadata_reset, None);
        assert_eq!(
            connection
                .query_row(
                    "SELECT started_at,last_event_at FROM threads WHERE id='thread'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("2026-07-10T00:00:00Z".into(), "2026-07-15T00:00:00Z".into(),)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT parent_rollout_id,agent_path,status,completed_at
                     FROM agent_runs WHERE id='child'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .unwrap(),
            (
                "survivor".into(),
                "/surviving/path".into(),
                "running".into(),
                None,
            )
        );
        for table in [
            "rollouts",
            "events",
            "messages",
            "tool_calls",
            "turns",
            "usage_facts",
            "agent_runs",
        ] {
            let count: i64 = connection
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE id LIKE 'doomed%'"),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0, "{table} retained removed projection rows");
        }
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM source_files WHERE rollout_id='doomed'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1,
            "source checkpoint deletion remains a separate named operation"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM app_meta WHERE key='pending_source_shrink:doomed'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn root_removal_returns_ordered_paths_then_applies_first_surviving_metadata() {
        let mut connection = connection();
        connection
            .execute_batch(
                "INSERT INTO threads(
                    id,title,title_updated_at,cwd,project,repository_url,branch,source,
                    thread_source,source_json,started_at,last_event_at,root_metadata_seen
                 ) VALUES('root','old title','2026-07-25T00:00:00Z','/old','old',
                    'old-url','old-branch','old-source','old-thread-source','{}',
                    '2026-07-01T00:00:00Z','2026-07-20T00:00:00Z',1);
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('root','root','2026-07-01T00:00:00Z','2026-07-20T00:00:00Z'),
                    ('child-z','root','2026-07-10T00:00:00Z','2026-07-15T00:00:00Z'),
                    ('child-a','root','2026-07-11T00:00:00Z','2026-07-16T00:00:00Z');
                 INSERT INTO source_files(rollout_id,path,ingested_at) VALUES
                    ('root','/sources/root.jsonl','2026-07-25T00:00:00Z'),
                    ('child-z','/sources/z.jsonl','2026-07-25T00:00:00Z'),
                    ('child-a','/sources/a.jsonl','2026-07-25T00:00:00Z');",
            )
            .unwrap();

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        let impact = remove_rollout(&transaction, "root").unwrap();
        let reset = impact.metadata_reset.as_ref().unwrap();
        assert_eq!(impact.thread_id.as_deref(), Some("root"));
        assert_eq!(reset.thread_id, "root");
        assert_eq!(
            reset.ordered_source_paths,
            ["/sources/a.jsonl", "/sources/z.jsonl"]
        );

        let ordered_owners = [
            owner(
                "child-a",
                None,
                Some("project-a"),
                Some("repo-a"),
                None,
                Some("source-a"),
                None,
            ),
            owner(
                "child-z",
                Some("/work/z"),
                Some("project-z"),
                Some("repo-z"),
                Some("branch-z"),
                Some("source-z"),
                Some("desktop"),
            ),
        ];
        apply_thread_metadata_reset(&transaction, reset, &ordered_owners).unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT title,title_updated_at,cwd,project,repository_url,branch,source,
                            thread_source,source_json,root_metadata_seen,started_at,last_event_at
                     FROM threads WHERE id='root'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, Option<String>>(5)?,
                            row.get::<_, Option<String>>(6)?,
                            row.get::<_, Option<String>>(7)?,
                            row.get::<_, Option<String>>(8)?,
                            row.get::<_, i64>(9)?,
                            row.get::<_, String>(10)?,
                            row.get::<_, String>(11)?,
                        ))
                    },
                )
                .unwrap(),
            (
                None,
                None,
                Some("/work/z".into()),
                Some("project-a".into()),
                Some("repo-a".into()),
                Some("branch-z".into()),
                Some("source-a".into()),
                Some("desktop".into()),
                None,
                0,
                "2026-07-10T00:00:00Z".into(),
                "2026-07-16T00:00:00Z".into(),
            )
        );
    }

    #[test]
    fn removal_and_abandoned_thread_deletion_roll_back_as_one_unit() {
        let mut connection = connection();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                    VALUES('lonely','2026-07-01T00:00:00Z','2026-07-02T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                    VALUES('lonely','lonely','2026-07-01T00:00:00Z','2026-07-02T00:00:00Z');
                 INSERT INTO source_files(rollout_id,path,ingested_at)
                    VALUES('lonely','/sources/lonely.jsonl','2026-07-25T00:00:00Z');
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,native
                 ) VALUES('lonely-event','lonely','lonely','2026-07-02T00:00:00Z',1,
                    'unknown',1);
                 INSERT INTO app_meta(key,value)
                    VALUES('pending_source_shrink:lonely','candidate');",
            )
            .unwrap();

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        let impact = remove_rollout(&transaction, "lonely").unwrap();
        delete_thread_if_abandoned(&transaction, impact.thread_id.as_deref().unwrap()).unwrap();
        assert_eq!(
            transaction
                .sqlite
                .query_row("SELECT COUNT(*) FROM threads", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        transaction.rollback().unwrap();

        for table in ["threads", "rollouts", "events", "source_files"] {
            let count: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 1, "{table} did not roll back");
        }
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM app_meta WHERE key='pending_source_shrink:lonely'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }
}
