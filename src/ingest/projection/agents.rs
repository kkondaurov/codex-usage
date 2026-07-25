use super::super::protocol::{
    CursorState, DecodedAgentRecord, ObservedAgentActivity, OwnerMeta, normalized_metadata_value,
    normalized_relational_identifier,
};
use super::{events, lifecycle};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};

const PROJECTED_SESSION_PATH_CHARS: usize = 4 * 1024;

/// Apply one typed parent observation of a child agent.
///
/// The durable event is inserted before lifecycle reconciliation because the
/// synthetic-agent latest-observation rule intentionally queries that event
/// history. Cursor state is published only after event, lifecycle, and owner
/// writes all succeed.
pub(in crate::ingest) fn apply(
    tx: &super::ProjectionTx<'_>,
    state: &mut CursorState,
    record: &DecodedAgentRecord,
) -> Result<()> {
    let mut candidate = state.clone();
    record.transition.apply_to(&mut candidate);

    events::apply(
        tx,
        &candidate,
        record.source_line,
        &record.timestamp,
        &record.observation.event,
    )?;
    if let Some(agent_id) = record.observation.agent_id.as_deref() {
        apply_observation(
            tx,
            agent_id,
            &candidate.thread_id,
            &candidate.owner_id,
            record.observation.agent_path.as_deref(),
            &record.timestamp,
            record.observation.activity,
        )?;
    }

    lifecycle::touch_owner(tx, &candidate, &record.timestamp)?;
    *state = candidate;
    Ok(())
}

/// Reconcile one already-decoded parent observation with the child-agent row.
///
/// Reconciliation calls this with compact persisted event fields, while new
/// source records use [`apply`] and the closed typed boundary above.
pub(in crate::ingest) fn apply_observation(
    tx: &super::ProjectionTx<'_>,
    agent_id: &str,
    thread_id: &str,
    parent_rollout_id: &str,
    agent_path: Option<&str>,
    timestamp: &str,
    activity: ObservedAgentActivity,
) -> Result<()> {
    let Some(agent_id) = normalized_relational_identifier(Some(agent_id), "agent thread id")?
    else {
        return Ok(());
    };
    let Some(thread_id) = normalized_relational_identifier(Some(thread_id), "thread id")? else {
        return Ok(());
    };
    let Some(parent_rollout_id) =
        normalized_relational_identifier(Some(parent_rollout_id), "parent rollout id")?
    else {
        return Ok(());
    };
    let agent_path = normalized_metadata_value(agent_path, PROJECTED_SESSION_PATH_CHARS);
    let status = activity.status();
    let completed_at = activity.is_terminal().then_some(timestamp);
    let existing = tx
        .sqlite
        .query_row(
            "SELECT rollout_id,status,started_at
             FROM agent_runs WHERE id=?1",
            [&agent_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((rollout_id, current_status, started_at)) = existing else {
        tx.sqlite.execute(
            "INSERT INTO agent_runs(
                id,thread_id,rollout_id,parent_rollout_id,agent_path,started_at,status,completed_at
             ) VALUES(?1,?2,NULL,?3,?4,?5,?6,?7)",
            params![
                agent_id,
                thread_id,
                parent_rollout_id,
                agent_path,
                timestamp,
                status,
                completed_at,
            ],
        )?;
        return Ok(());
    };

    if let Some(rollout_id) = rollout_id {
        let parent_terminal_is_authoritative = activity.is_terminal()
            && current_status == "running"
            && timestamp >= started_at.as_str()
            && tx.sqlite.query_row(
                "SELECT COALESCE(
                        (SELECT MAX(timestamp) FROM events WHERE rollout_id=?1),
                        ?2
                    )<=?3",
                params![rollout_id, started_at, timestamp],
                |row| row.get::<_, i64>(0),
            )? != 0;
        tx.sqlite.execute(
            "UPDATE agent_runs SET
                agent_path=COALESCE(?1,agent_path),
                status=CASE WHEN ?2=1 THEN ?3 ELSE status END,
                completed_at=CASE WHEN ?2=1 THEN ?4 ELSE completed_at END
             WHERE id=?5",
            params![
                agent_path,
                parent_terminal_is_authoritative as i64,
                status,
                completed_at,
                agent_id,
            ],
        )?;
        if parent_terminal_is_authoritative {
            tx.sqlite.execute(
                "UPDATE turns
                 SET status=?1,completed_at=?2
                 WHERE rollout_id=?3
                   AND agent_run_id=?4
                   AND status='running'
                   AND started_at<=?2
                   AND EXISTS(
                       SELECT 1 FROM events e
                       WHERE e.turn_id=turns.id AND e.kind='turn_started'
                   )
                   AND NOT EXISTS(
                       SELECT 1 FROM events e
                       WHERE e.turn_id=turns.id
                         AND (
                           e.kind='turn_completed'
                           OR (
                             e.kind='state'
                             AND e.status IN ('interrupted','rolled_back')
                           )
                         )
                   )
                   AND COALESCE(
                       (SELECT MAX(e.timestamp) FROM events e WHERE e.turn_id=turns.id),
                       turns.started_at
                   )<=?2",
                params![status, timestamp, rollout_id, agent_id],
            )?;
        }
        return Ok(());
    }

    let is_latest_observation = tx.sqlite.query_row(
        "SELECT NOT EXISTS(
            SELECT 1 FROM events
            WHERE kind='subagent'
              AND json_extract(payload_json,'$.agent_thread_id')=?1
              AND timestamp>?2
         )",
        params![agent_id, timestamp],
        |row| row.get::<_, i64>(0),
    )? != 0;
    tx.sqlite.execute(
        "UPDATE agent_runs SET
            started_at=MIN(started_at,?1),
            agent_path=COALESCE(?2,agent_path),
            thread_id=CASE WHEN ?3=1 THEN ?4 ELSE thread_id END,
            parent_rollout_id=CASE WHEN ?3=1 THEN ?5 ELSE parent_rollout_id END,
            status=CASE WHEN ?3=1 THEN ?6 ELSE status END,
            completed_at=CASE WHEN ?3=1 THEN ?7 ELSE completed_at END
         WHERE id=?8",
        params![
            timestamp,
            agent_path,
            is_latest_observation as i64,
            thread_id,
            parent_rollout_id,
            status,
            completed_at,
            agent_id,
        ],
    )?;
    Ok(())
}

/// Materialize the native agent row owned by one rollout.
///
/// A parent observation may have created a synthetic terminal row before the
/// child rollout was discovered. Native work supersedes that terminal only
/// when it starts strictly later; equality deliberately preserves the parent
/// observation's terminal authority.
pub(in crate::ingest) fn upsert_native_run(
    tx: &super::ProjectionTx<'_>,
    owner: &OwnerMeta,
) -> Result<()> {
    tx.sqlite.execute(
        "INSERT INTO agent_runs(
            id,thread_id,rollout_id,parent_rollout_id,agent_path,nickname,started_at,status
         ) VALUES(?1,?2,?1,?3,?4,?5,?6,'running')
         ON CONFLICT(id) DO UPDATE SET
            thread_id=excluded.thread_id,rollout_id=excluded.rollout_id,
            parent_rollout_id=excluded.parent_rollout_id,
            agent_path=excluded.agent_path,nickname=excluded.nickname,
            status=CASE
                WHEN agent_runs.rollout_id IS NULL
                 AND agent_runs.completed_at IS NOT NULL
                 AND excluded.started_at>agent_runs.completed_at
                THEN 'running'
                ELSE agent_runs.status END,
            completed_at=CASE
                WHEN agent_runs.rollout_id IS NULL
                 AND agent_runs.completed_at IS NOT NULL
                 AND excluded.started_at>agent_runs.completed_at
                THEN NULL
                ELSE agent_runs.completed_at END",
        params![
            owner.owner_id,
            owner.thread_id,
            owner.parent_rollout_id,
            owner.agent_path,
            owner.agent_nickname,
            owner.timestamp,
        ],
    )?;
    Ok(())
}

/// Rebuild one child-agent row from surviving native and parent evidence.
pub(in crate::ingest) fn rematerialize_surviving_observation(
    tx: &super::ProjectionTx<'_>,
    agent_id: &str,
) -> Result<()> {
    // A synthetic row is wholly derived from parent observations. Rebuild it
    // from zero so removal of an earlier non-winning parent can move its start
    // forward. Promoted rows retain their native rollout identity and are
    // restored through the native lifecycle path before replay.
    tx.sqlite.execute(
        "DELETE FROM agent_runs WHERE id=?1 AND rollout_id IS NULL",
        [agent_id],
    )?;
    restore_promoted_native_state(tx, agent_id)?;
    let observations = {
        let mut statement = tx.sqlite.prepare(
            "SELECT e.thread_id,e.rollout_id,e.body,e.timestamp,COALESCE(e.status,'running')
             FROM events e
             LEFT JOIN source_files sf ON sf.rollout_id=e.rollout_id
             WHERE e.kind='subagent'
               AND json_extract(e.payload_json,'$.agent_thread_id')=?1
             ORDER BY e.timestamp,COALESCE(sf.path,''),e.source_line,e.id",
        )?;
        statement
            .query_map([agent_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (thread_id, parent_rollout_id, agent_path, timestamp, activity) in observations {
        apply_observation(
            tx,
            agent_id,
            &thread_id,
            &parent_rollout_id,
            agent_path.as_deref(),
            &timestamp,
            ObservedAgentActivity::from_source_kind(Some(&activity)),
        )?;
    }
    Ok(())
}

/// Rebuild every child observed by one newly checkpointed parent rollout.
pub(in crate::ingest) fn rematerialize_observed_children(
    tx: &super::ProjectionTx<'_>,
    rollout_id: &str,
) -> Result<()> {
    let agent_ids = {
        let mut statement = tx.sqlite.prepare(
            "SELECT DISTINCT json_extract(payload_json,'$.agent_thread_id')
             FROM events
             WHERE rollout_id=?1 AND kind='subagent'
               AND json_extract(payload_json,'$.agent_thread_id') IS NOT NULL
             ORDER BY 1",
        )?;
        statement
            .query_map([rollout_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for agent_id in agent_ids {
        rematerialize_surviving_observation(tx, &agent_id)?;
    }
    Ok(())
}

fn restore_promoted_native_state(tx: &super::ProjectionTx<'_>, agent_id: &str) -> Result<()> {
    let native = tx
        .sqlite
        .query_row(
            "SELECT
                r.id,r.thread_id,r.parent_rollout_id,r.agent_path,r.agent_nickname,r.started_at
             FROM agent_runs a
             JOIN rollouts r ON r.id=a.rollout_id
             WHERE a.id=?1 AND a.rollout_id IS NOT NULL",
            [agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((rollout_id, thread_id, parent_rollout_id, agent_path, nickname, started_at)) = native
    else {
        return Ok(());
    };

    tx.sqlite.execute(
        "UPDATE agent_runs SET
            thread_id=?1,parent_rollout_id=?2,agent_path=?3,nickname=?4,started_at=?5
         WHERE id=?6 AND rollout_id=?7",
        params![
            thread_id,
            parent_rollout_id,
            agent_path,
            nickname,
            started_at,
            agent_id,
            rollout_id,
        ],
    )?;

    let turn_lifecycles = {
        let mut statement = tx.sqlite.prepare(
            "SELECT
                t.id,
                CASE
                    WHEN e.kind='turn_started' THEN 'running'
                    WHEN e.kind='turn_completed' THEN 'completed'
                    ELSE e.status
                END,
                CASE WHEN e.kind='turn_started' THEN NULL ELSE e.timestamp END
             FROM turns t
             JOIN events e ON e.id=(
                 SELECT e2.id
                 FROM events e2
                 WHERE e2.turn_id=t.id
                   AND (
                     e2.kind IN ('turn_started','turn_completed')
                     OR (
                       e2.kind='state'
                       AND e2.status IN ('interrupted','rolled_back')
                     )
                   )
                 ORDER BY e2.timestamp DESC,e2.source_line DESC,e2.id DESC
                 LIMIT 1
             )
             WHERE t.rollout_id=?1 AND t.agent_run_id=?2",
        )?;
        statement
            .query_map(params![rollout_id, agent_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (turn_id, status, completed_at) in turn_lifecycles {
        tx.sqlite.execute(
            "UPDATE turns SET status=?1,completed_at=?2 WHERE id=?3",
            params![status, completed_at, turn_id],
        )?;
    }

    let lifecycle = tx
        .sqlite
        .query_row(
            "SELECT
                CASE
                    WHEN kind='turn_started' THEN 'running'
                    WHEN kind='turn_completed' THEN 'completed'
                    ELSE status
                END,
                CASE WHEN kind='turn_started' THEN NULL ELSE timestamp END
             FROM events INDEXED BY idx_events_activity_owner
             WHERE thread_id=?1 AND rollout_id=?2
               AND (
                    kind IN ('turn_started','turn_completed')
                    OR (kind='state' AND status IN ('interrupted','rolled_back'))
               )
             ORDER BY timestamp DESC,source_line DESC,id DESC
             LIMIT 1",
            params![thread_id, rollout_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
        .unwrap_or_else(|| ("running".into(), None));
    tx.sqlite.execute(
        "UPDATE agent_runs SET status=?1,completed_at=?2
         WHERE id=?3 AND rollout_id=?4",
        params![lifecycle.0, lifecycle.1, agent_id, rollout_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::{
        AgentObservation, AgentStateTransition, ProjectedEvent, ProjectedEventMetadata,
        SubagentMetadata,
    };
    use super::*;
    use rusqlite::Connection;

    fn state() -> CursorState {
        CursorState {
            owner_id: "parent-1".into(),
            thread_id: "thread-1".into(),
            current_turn: Some("parent-turn".into()),
            last_timestamp: Some("2026-07-25T09:00:00.000000000Z".into()),
            native_started: true,
            ..CursorState::default()
        }
    }

    fn native_owner(id: &str, timestamp: &str) -> OwnerMeta {
        OwnerMeta {
            owner_id: id.into(),
            thread_id: "thread-1".into(),
            parent_rollout_id: Some("parent-1".into()),
            parent_thread_id: Some("thread-1".into()),
            agent_path: Some("/root/native".into()),
            agent_nickname: Some("Native".into()),
            is_subagent: true,
            forked: false,
            timestamp: timestamp.into(),
            cwd: None,
            project: None,
            repository_url: None,
            branch: None,
            source: None,
            thread_source: None,
            source_json: None,
        }
    }

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads(
                    id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE rollouts(
                    id TEXT PRIMARY KEY,last_event_at TEXT NOT NULL
                 );
                 CREATE TABLE agent_runs(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT,
                    parent_rollout_id TEXT,agent_path TEXT,nickname TEXT,
                    started_at TEXT NOT NULL,status TEXT NOT NULL,completed_at TEXT
                 );
                 CREATE TABLE turns(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    agent_run_id TEXT NOT NULL,started_at TEXT NOT NULL,status TEXT NOT NULL,
                    completed_at TEXT
                 );
                 CREATE TABLE events(
                    id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,rollout_id TEXT NOT NULL,
                    turn_id TEXT,agent_run_id TEXT,timestamp TEXT NOT NULL,
                    source_line INTEGER NOT NULL,kind TEXT NOT NULL,role TEXT,label TEXT,
                    body TEXT,status TEXT,tool_name TEXT,call_id TEXT,duration_ms INTEGER,
                    model TEXT,effort TEXT,payload_json TEXT,native INTEGER NOT NULL
                 );
                 CREATE TRIGGER require_observation_before_synthetic_agent
                 BEFORE INSERT ON agent_runs WHEN NEW.rollout_id IS NULL
                 BEGIN
                    SELECT CASE WHEN NOT EXISTS(
                        SELECT 1 FROM events
                        WHERE kind='subagent'
                          AND json_extract(payload_json,'$.agent_thread_id')=NEW.id
                    ) THEN RAISE(ABORT,'agent inserted before observation') END;
                 END;
                 INSERT INTO threads(id,last_event_at)
                 VALUES('thread-1','2026-07-25T08:00:00.000000000Z');
                 INSERT INTO rollouts(id,last_event_at)
                 VALUES('parent-1','2026-07-25T08:00:00.000000000Z');",
            )
            .unwrap();
        connection
    }

    fn event(agent_id: Option<&str>, path: Option<&str>, source_status: &str) -> ProjectedEvent {
        ProjectedEvent {
            kind: "subagent".into(),
            role: None,
            label: Some(source_status.into()),
            body: path.map(str::to_owned),
            status: Some(source_status.into()),
            tool_name: None,
            call_id: None,
            duration_ms: None,
            metadata: agent_id.map(|agent_id| {
                ProjectedEventMetadata::Subagent(SubagentMetadata {
                    agent_thread_id: agent_id.into(),
                })
            }),
        }
    }

    fn record(
        line: u64,
        timestamp: &str,
        agent_id: Option<&str>,
        path: Option<&str>,
        source_status: &str,
        activity: ObservedAgentActivity,
    ) -> DecodedAgentRecord {
        DecodedAgentRecord {
            source_line: line,
            timestamp: timestamp.into(),
            transition: AgentStateTransition {
                last_timestamp: timestamp.into(),
            },
            observation: AgentObservation {
                agent_id: agent_id.map(str::to_owned),
                agent_path: path.map(str::to_owned),
                activity,
                event: event(agent_id, path, source_status),
            },
        }
    }

    fn apply_record(
        connection: &mut Connection,
        cursor: &mut CursorState,
        record: &DecodedAgentRecord,
    ) -> Result<()> {
        let transaction = crate::ingest::projection::ProjectionConnection::new(connection)
            .begin_metadata_refresh()?;
        apply(&transaction, cursor, record)?;
        transaction.commit()?;
        Ok(())
    }

    #[test]
    fn native_promotion_resets_only_terminal_evidence_strictly_before_native_start() {
        let mut connection = connection();
        connection
            .execute_batch(
                "DROP TRIGGER require_observation_before_synthetic_agent;
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,started_at,status,completed_at
                 ) VALUES
                    ('before','thread-1',NULL,'parent-1','2026-07-25T08:00:00Z',
                     'interrupted','2026-07-25T09:59:59Z'),
                    ('equal','thread-1',NULL,'parent-1','2026-07-25T08:00:00Z',
                     'interrupted','2026-07-25T10:00:00Z'),
                    ('after','thread-1',NULL,'parent-1','2026-07-25T08:00:00Z',
                     'completed','2026-07-25T10:00:01Z'),
                    ('running','thread-1',NULL,'parent-1','2026-07-25T08:00:00Z',
                     'running',NULL);",
            )
            .unwrap();

        let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
            .begin_metadata_refresh()
            .unwrap();
        for id in ["before", "equal", "after", "running"] {
            upsert_native_run(&transaction, &native_owner(id, "2026-07-25T10:00:00Z")).unwrap();
        }
        transaction.commit().unwrap();

        let rows = connection
            .prepare(
                "SELECT id,status,completed_at,rollout_id FROM agent_runs
                 WHERE id IN ('before','equal','after','running') ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "after".into(),
                    "completed".into(),
                    Some("2026-07-25T10:00:01Z".into()),
                    "after".into(),
                ),
                ("before".into(), "running".into(), None, "before".into()),
                (
                    "equal".into(),
                    "interrupted".into(),
                    Some("2026-07-25T10:00:00Z".into()),
                    "equal".into(),
                ),
                ("running".into(), "running".into(), None, "running".into()),
            ]
        );
    }

    #[test]
    fn inserts_event_before_synthetic_agent_then_touches_and_publishes() {
        let mut connection = connection();
        let mut cursor = state();
        let timestamp = "2026-07-25T10:00:00.000000000Z";
        let decoded = record(
            17,
            timestamp,
            Some("child-1"),
            Some(" /root/storage "),
            "completed",
            ObservedAgentActivity::Completed,
        );

        apply_record(&mut connection, &mut cursor, &decoded).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT thread_id,parent_rollout_id,agent_path,started_at,status,completed_at
                     FROM agent_runs WHERE id='child-1'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .unwrap(),
            (
                "thread-1".into(),
                "parent-1".into(),
                "/root/storage".into(),
                timestamp.into(),
                "completed".into(),
                timestamp.into(),
            )
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT payload_json FROM events WHERE id='parent-1:17'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            r#"{"agent_thread_id":"child-1"}"#
        );
        assert_eq!(cursor.last_timestamp.as_deref(), Some(timestamp));
        assert_eq!(
            connection
                .query_row("SELECT last_event_at FROM threads", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            timestamp
        );
        assert_eq!(
            connection
                .query_row("SELECT last_event_at FROM rollouts", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            timestamp
        );
    }

    #[test]
    fn missing_child_identity_persists_event_without_creating_an_agent() {
        let mut connection = connection();
        let mut cursor = state();
        let decoded = record(
            18,
            "2026-07-25T10:00:00.000000000Z",
            None,
            Some("/root/unidentified"),
            "completed",
            ObservedAgentActivity::Completed,
        );

        apply_record(&mut connection, &mut cursor, &decoded).unwrap();

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("SELECT payload_json FROM events", [], |row| {
                    row.get::<_, Option<String>>(0)
                })
                .unwrap(),
            None
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn synthetic_agent_uses_strictly_later_evidence_and_stable_equal_time_order() {
        let mut connection = connection();
        let mut cursor = state();
        let latest = record(
            20,
            "2026-07-25T10:00:00.000000000Z",
            Some("child-1"),
            Some("/root/latest"),
            "completed",
            ObservedAgentActivity::Completed,
        );
        apply_record(&mut connection, &mut cursor, &latest).unwrap();

        let earlier = record(
            19,
            "2026-07-25T09:00:00.000000000Z",
            Some("child-1"),
            Some("/root/earlier"),
            "started",
            ObservedAgentActivity::Running,
        );
        apply_record(&mut connection, &mut cursor, &earlier).unwrap();
        let equal_later_in_source = record(
            21,
            "2026-07-25T10:00:00.000000000Z",
            Some("child-1"),
            None,
            "interrupted",
            ObservedAgentActivity::Interrupted,
        );
        apply_record(&mut connection, &mut cursor, &equal_later_in_source).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT started_at,agent_path,status,completed_at,parent_rollout_id
                     FROM agent_runs WHERE id='child-1'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .unwrap(),
            (
                "2026-07-25T09:00:00.000000000Z".into(),
                "/root/earlier".into(),
                "interrupted".into(),
                "2026-07-25T10:00:00.000000000Z".into(),
                "parent-1".into(),
            )
        );
    }

    #[test]
    fn parent_terminal_observation_can_close_only_open_native_child_lifecycle() {
        let mut connection = connection();
        connection
            .execute_batch(
                "DROP TRIGGER require_observation_before_synthetic_agent;
                 INSERT INTO rollouts(id,last_event_at)
                 VALUES('child-1','2026-07-25T10:00:00.000000000Z');
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,agent_path,started_at,status
                 ) VALUES(
                    'child-1','thread-1','child-1','parent-1','/native',
                    '2026-07-25T10:00:00.000000000Z','running'
                 );
                 INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,status
                 ) VALUES(
                    'child-turn','thread-1','child-1','child-1',
                    '2026-07-25T10:00:00.000000000Z','running'
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,agent_run_id,timestamp,source_line,
                    kind,status,native
                 ) VALUES(
                    'child-start','thread-1','child-1','child-turn','child-1',
                    '2026-07-25T10:00:00.000000000Z',1,'turn_started','running',1
                 );",
            )
            .unwrap();
        let mut cursor = state();
        let completed = record(
            22,
            "2026-07-25T11:00:00.000000000Z",
            Some("child-1"),
            Some("/parent/observed"),
            "completed",
            ObservedAgentActivity::Completed,
        );

        apply_record(&mut connection, &mut cursor, &completed).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at,agent_path FROM agent_runs WHERE id='child-1'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .unwrap(),
            (
                "completed".into(),
                "2026-07-25T11:00:00.000000000Z".into(),
                "/parent/observed".into(),
            )
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at FROM turns WHERE id='child-turn'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("completed".into(), "2026-07-25T11:00:00.000000000Z".into(),)
        );

        let running = record(
            23,
            "2026-07-25T12:00:00.000000000Z",
            Some("child-1"),
            None,
            "interacted",
            ObservedAgentActivity::Running,
        );
        apply_record(&mut connection, &mut cursor, &running).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT status FROM agent_runs WHERE id='child-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "completed"
        );
    }

    #[test]
    fn newer_native_evidence_defeats_parent_terminal_but_path_still_enriches() {
        let mut connection = connection();
        connection
            .execute_batch(
                "DROP TRIGGER require_observation_before_synthetic_agent;
                 INSERT INTO rollouts(id,last_event_at)
                 VALUES('child-1','2026-07-25T12:00:00.000000000Z');
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,agent_path,started_at,status
                 ) VALUES(
                    'child-1','thread-1','child-1','parent-1',NULL,
                    '2026-07-25T10:00:00.000000000Z','running'
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,timestamp,source_line,kind,native
                 ) VALUES(
                    'child-newer','thread-1','child-1',
                    '2026-07-25T12:00:00.000000000Z',1,'message',1
                 );",
            )
            .unwrap();
        let mut cursor = state();
        let completed = record(
            24,
            "2026-07-25T11:00:00.000000000Z",
            Some("child-1"),
            Some("/parent/enriched"),
            "completed",
            ObservedAgentActivity::Completed,
        );

        apply_record(&mut connection, &mut cursor, &completed).unwrap();

        assert_eq!(
            connection
                .query_row(
                    "SELECT status,completed_at,agent_path FROM agent_runs WHERE id='child-1'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .unwrap(),
            ("running".into(), None, "/parent/enriched".into())
        );
    }

    #[test]
    fn projection_failure_does_not_publish_cursor_and_transaction_can_roll_back_event() {
        let mut connection = connection();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_child_projection
                 BEFORE INSERT ON agent_runs
                 BEGIN SELECT RAISE(ABORT,'forced agent failure'); END;",
            )
            .unwrap();
        let before = state();
        let mut cursor = before.clone();
        let decoded = record(
            25,
            "2026-07-25T10:00:00.000000000Z",
            Some("child-1"),
            None,
            "started",
            ObservedAgentActivity::Running,
        );

        {
            let transaction = crate::ingest::projection::ProjectionConnection::new(&mut connection)
                .begin_metadata_refresh()
                .unwrap();
            let error = apply(&transaction, &mut cursor, &decoded).unwrap_err();
            assert!(error.to_string().contains("forced agent failure"));
        }

        assert_eq!(cursor.last_timestamp, before.last_timestamp);
        assert_eq!(cursor.current_turn, before.current_turn);
        assert_eq!(cursor.thread_id, before.thread_id);
        assert_eq!(cursor.owner_id, before.owner_id);
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }
}
