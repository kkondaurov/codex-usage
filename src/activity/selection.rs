use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Clone, Debug)]
pub(crate) struct ActivityRootScope {
    pub(crate) id: String,
    pub(crate) started_at: String,
    pub(crate) next_started_at: Option<String>,
    pub(crate) open_left: bool,
}

impl ActivityRootScope {
    pub(crate) fn load_on(
        connection: &Connection,
        thread_id: &str,
        root_rollout_id: &str,
        turn_id: &str,
    ) -> Result<Option<Self>> {
        let Some(started_at) = connection
            .query_row(
                "SELECT started_at FROM turns
                 WHERE thread_id=?1 AND rollout_id=?2 AND id=?3",
                params![thread_id, root_rollout_id, turn_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            return Ok(None);
        };
        Self::from_known_on(connection, thread_id, root_rollout_id, turn_id, started_at).map(Some)
    }

    pub(crate) fn from_known_on(
        connection: &Connection,
        thread_id: &str,
        root_rollout_id: &str,
        turn_id: &str,
        started_at: String,
    ) -> Result<Self> {
        let next_started_at = connection
            .query_row(
                "SELECT started_at FROM turns
                 WHERE thread_id=?1 AND rollout_id=?2
                   AND (started_at>?3 OR (started_at=?3 AND id>?4))
                 ORDER BY started_at,id LIMIT 1",
                params![thread_id, root_rollout_id, started_at, turn_id],
                |row| row.get(0),
            )
            .optional()?;
        let open_left = connection.query_row(
            "SELECT NOT EXISTS(
                 SELECT 1 FROM turns
                 WHERE thread_id=?1 AND rollout_id=?2
                   AND (started_at<?3 OR (started_at=?3 AND id<?4))
             )",
            params![thread_id, root_rollout_id, started_at, turn_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        Ok(Self {
            id: turn_id.to_owned(),
            started_at,
            next_started_at,
            open_left,
        })
    }
}

pub(crate) struct PreparedSelection<'connection> {
    connection: &'connection Connection,
    thread_id: String,
    root_turn_ids: Vec<String>,
    descendant_count: usize,
}

impl<'connection> PreparedSelection<'connection> {
    pub(crate) fn prepare(
        connection: &'connection Connection,
        thread_id: &str,
        root_rollout_id: &str,
        roots: &[ActivityRootScope],
    ) -> Result<Self> {
        connection.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS selected_activity_roots(
                 turn_id TEXT PRIMARY KEY,
                 started_at TEXT NOT NULL,
                 next_started_at TEXT,
                 open_left INTEGER NOT NULL
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS selected_activity_turns(
                 turn_id TEXT PRIMARY KEY,
                 root_turn_id TEXT NOT NULL,
                 usage_kind INTEGER NOT NULL
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS selected_activity_descendants(
                 turn_id TEXT PRIMARY KEY,
                 root_turn_id TEXT NOT NULL,
                 agent_key TEXT NOT NULL,
                 review INTEGER NOT NULL,
                 started_at TEXT NOT NULL,
                 status TEXT NOT NULL,
                 duration_ms INTEGER,
                 agent_label TEXT
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS idx_selected_activity_descendants_root
                 ON selected_activity_descendants(
                     root_turn_id,review,started_at DESC,turn_id DESC
                 );
             CREATE TEMP TABLE IF NOT EXISTS activity_explicit_agents(
                 agent_key TEXT PRIMARY KEY
             ) WITHOUT ROWID;
             CREATE TEMP TABLE IF NOT EXISTS selected_activity_agent_intervals(
                 link_id TEXT PRIMARY KEY,
                 agent_key TEXT NOT NULL,
                 root_turn_id TEXT NOT NULL,
                 linked_at TEXT,
                 next_linked_at TEXT
             );
             DELETE FROM selected_activity_roots;
             DELETE FROM selected_activity_turns;
             DELETE FROM selected_activity_descendants;
             DELETE FROM activity_explicit_agents;
             DELETE FROM selected_activity_agent_intervals;",
        )?;

        let root_turn_ids = roots.iter().map(|root| root.id.clone()).collect::<Vec<_>>();
        if roots.is_empty() {
            return Ok(Self {
                connection,
                thread_id: thread_id.to_owned(),
                root_turn_ids,
                descendant_count: 0,
            });
        }

        {
            let mut insert_root = connection.prepare(
                "INSERT INTO selected_activity_roots(
                     turn_id,started_at,next_started_at,open_left
                 ) VALUES(?1,?2,?3,?4)",
            )?;
            let mut insert_turn = connection.prepare(
                "INSERT INTO selected_activity_turns(turn_id,root_turn_id,usage_kind)
                 VALUES(?1,?1,0)",
            )?;
            for root in roots {
                insert_root.execute(params![
                    root.id,
                    root.started_at,
                    root.next_started_at,
                    root.open_left
                ])?;
                insert_turn.execute([&root.id])?;
            }
        }

        let explicit_agent_count = connection.execute(
            "INSERT OR IGNORE INTO activity_explicit_agents(agent_key)
             SELECT json_extract(link.payload_json,'$.agent_thread_id')
             FROM events link
             JOIN turns root_turn
               ON root_turn.id=link.turn_id AND root_turn.thread_id=link.thread_id
             WHERE link.thread_id=?1 AND link.kind='subagent'
               AND root_turn.rollout_id=?2
               AND json_extract(link.payload_json,'$.agent_thread_id') IS NOT NULL
               AND EXISTS(
                    SELECT 1 FROM turns descendant
                    WHERE descendant.thread_id=?1 AND descendant.rollout_id<>?2
                    LIMIT 1
               )",
            params![thread_id, root_rollout_id],
        )?;
        // Agent clocks can place the first child turn just before its spawn
        // event. Keep that first interval open on the left; every later link
        // transfers the reused identity to the newly linked root exchange.
        if explicit_agent_count > 0 {
            connection.execute(
                "INSERT OR IGNORE INTO selected_activity_agent_intervals(
                     link_id,agent_key,root_turn_id,linked_at,next_linked_at
                 )
                 SELECT link.link_id,link.agent_key,link.root_turn_id,
                        CASE WHEN link.link_rank=1 THEN NULL ELSE link.timestamp END,
                        link.next_linked_at
                 FROM (
                     SELECT event.id link_id,
                            json_extract(event.payload_json,'$.agent_thread_id') agent_key,
                            event.turn_id root_turn_id,event.timestamp,
                            ROW_NUMBER() OVER (
                                PARTITION BY json_extract(
                                    event.payload_json,'$.agent_thread_id'
                                )
                                ORDER BY event.timestamp,event.source_line,event.id
                            ) link_rank,
                            LEAD(event.timestamp) OVER (
                                PARTITION BY json_extract(
                                    event.payload_json,'$.agent_thread_id'
                                )
                                ORDER BY event.timestamp,event.source_line,event.id
                            ) next_linked_at
                     FROM events event
                     JOIN turns root_turn
                       ON root_turn.id=event.turn_id AND root_turn.thread_id=event.thread_id
                     WHERE event.thread_id=?1 AND event.kind='subagent'
                       AND root_turn.rollout_id=?2
                       AND json_extract(event.payload_json,'$.agent_thread_id') IS NOT NULL
                 ) link
                 JOIN selected_activity_roots selected
                   ON selected.turn_id=link.root_turn_id",
                params![thread_id, root_rollout_id],
            )?;
        }

        let descendant_count = connection.execute(
            "INSERT OR IGNORE INTO selected_activity_descendants(
                 turn_id,root_turn_id,agent_key,review,started_at,status,
                 duration_ms,agent_label
             )
             SELECT mapped.turn_id,mapped.root_turn_id,mapped.agent_key,
                    mapped.review,mapped.started_at,mapped.status,
                    mapped.duration_ms,COALESCE(a.nickname,a.agent_path)
             FROM (
                 SELECT t.id turn_id,explicit.root_turn_id,
                        COALESCE(t.agent_run_id,t.rollout_id) agent_key,
                        COALESCE(t.model='codex-auto-review',0) review,
                        t.started_at,t.status,t.duration_ms,t.agent_run_id,t.thread_id
                 FROM turns t
                 JOIN selected_activity_agent_intervals explicit
                   ON explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                  AND (explicit.linked_at IS NULL OR t.started_at>=explicit.linked_at)
                  AND (explicit.next_linked_at IS NULL
                       OR t.started_at<explicit.next_linked_at)
                 WHERE t.thread_id=?1 AND t.rollout_id<>?2
                 UNION ALL
                 SELECT t.id,selected.turn_id,
                        COALESCE(t.agent_run_id,t.rollout_id),
                        COALESCE(t.model='codex-auto-review',0),
                        t.started_at,t.status,t.duration_ms,t.agent_run_id,t.thread_id
                 FROM turns t
                 JOIN selected_activity_roots selected
                   ON t.started_at>=selected.started_at
                  AND (selected.next_started_at IS NULL
                       OR t.started_at<selected.next_started_at)
                 WHERE t.thread_id=?1 AND t.rollout_id<>?2
                   AND NOT EXISTS(
                       SELECT 1 FROM activity_explicit_agents explicit
                       WHERE explicit.agent_key=COALESCE(t.agent_run_id,t.rollout_id)
                   )
             ) mapped
             LEFT JOIN agent_runs a
               ON a.id=mapped.agent_run_id AND a.thread_id=mapped.thread_id",
            params![thread_id, root_rollout_id],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO selected_activity_turns(
                 turn_id,root_turn_id,usage_kind
             )
             SELECT turn_id,root_turn_id,CASE WHEN review=1 THEN 2 ELSE 1 END
             FROM selected_activity_descendants",
            [],
        )?;

        Ok(Self {
            connection,
            thread_id: thread_id.to_owned(),
            root_turn_ids,
            descendant_count,
        })
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn root_turn_ids(&self) -> &[String] {
        &self.root_turn_ids
    }

    pub(crate) fn has_descendants(&self) -> bool {
        self.descendant_count > 0
    }

    pub(crate) fn with_connection<T>(
        &self,
        read: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        read(self.connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Db;

    fn table_count(connection: &Connection, table: &str) -> i64 {
        connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    #[test]
    fn prepared_selection_replaces_previous_state_on_same_connection() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('selection-thread','Selection',
                        '2026-07-01T00:00:00Z','2026-07-01T00:02:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('selection-thread','selection-thread',
                        '2026-07-01T00:00:00Z','2026-07-01T00:02:00Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('selection-root','selection-thread','selection-thread',
                        '2026-07-01T00:00:00Z','completed');",
            )
            .unwrap();
        let roots = [ActivityRootScope {
            id: "selection-root".into(),
            started_at: "2026-07-01T00:00:00Z".into(),
            next_started_at: None,
            open_left: true,
        }];
        let selection =
            PreparedSelection::prepare(&connection, "selection-thread", "selection-thread", &roots)
                .unwrap();
        assert_eq!(selection.root_turn_ids(), ["selection-root"]);
        assert_eq!(table_count(&connection, "selected_activity_roots"), 1);
        assert_eq!(table_count(&connection, "selected_activity_turns"), 1);
        drop(selection);

        PreparedSelection::prepare(&connection, "selection-thread", "selection-thread", &[])
            .unwrap();
        for table in [
            "selected_activity_roots",
            "selected_activity_turns",
            "selected_activity_descendants",
            "activity_explicit_agents",
            "selected_activity_agent_intervals",
        ] {
            assert_eq!(table_count(&connection, table), 0, "stale rows in {table}");
        }
    }

    #[test]
    fn prepared_selection_is_connection_local() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let first = db.connect().unwrap();
        let second = db.connect().unwrap();
        PreparedSelection::prepare(&first, "missing", "missing", &[]).unwrap();
        assert_eq!(table_count(&first, "selected_activity_roots"), 0);
        assert!(
            second
                .query_row("SELECT COUNT(*) FROM selected_activity_roots", [], |row| {
                    row.get::<_, i64>(0)
                })
                .is_err(),
            "TEMP selection tables leaked across SQLite connections"
        );
    }

    #[test]
    fn root_scope_uses_timestamp_and_id_for_stable_neighbor_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('scope-thread','Scope',
                        '2026-07-01T00:00:00Z','2026-07-01T00:01:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('scope-rollout','scope-thread',
                        '2026-07-01T00:00:00Z','2026-07-01T00:01:00Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status) VALUES
                    ('a','scope-thread','scope-rollout','2026-07-01T00:00:00Z','completed'),
                    ('b','scope-thread','scope-rollout','2026-07-01T00:00:00Z','completed'),
                    ('c','scope-thread','scope-rollout','2026-07-01T00:01:00Z','completed');",
            )
            .unwrap();

        let first = ActivityRootScope::load_on(&connection, "scope-thread", "scope-rollout", "a")
            .unwrap()
            .unwrap();
        assert!(first.open_left);
        assert_eq!(
            first.next_started_at.as_deref(),
            Some("2026-07-01T00:00:00Z")
        );

        let second = ActivityRootScope::load_on(&connection, "scope-thread", "scope-rollout", "b")
            .unwrap()
            .unwrap();
        assert!(!second.open_left);
        assert_eq!(
            second.next_started_at.as_deref(),
            Some("2026-07-01T00:01:00Z")
        );
    }

    #[test]
    fn prepared_selection_assigns_root_agent_and_review_usage_kinds() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('kind-thread','Kinds',
                        '2026-07-01T00:00:00Z','2026-07-01T00:03:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived) VALUES
                    ('kind-thread','kind-thread',
                     '2026-07-01T00:00:00Z','2026-07-01T00:03:00Z',0),
                    ('kind-child','kind-thread',
                     '2026-07-01T00:01:00Z','2026-07-01T00:03:00Z',0);
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status,model) VALUES
                    ('kind-root','kind-thread','kind-thread',
                     '2026-07-01T00:00:00Z','completed','gpt-5.5'),
                    ('kind-agent','kind-thread','kind-child',
                     '2026-07-01T00:01:00Z','completed','gpt-5.5'),
                    ('kind-review','kind-thread','kind-child',
                     '2026-07-01T00:02:00Z','completed','codex-auto-review');",
            )
            .unwrap();
        let roots = [ActivityRootScope {
            id: "kind-root".into(),
            started_at: "2026-07-01T00:00:00Z".into(),
            next_started_at: None,
            open_left: true,
        }];
        let selection =
            PreparedSelection::prepare(&connection, "kind-thread", "kind-thread", &roots).unwrap();
        assert!(selection.has_descendants());
        let mut statement = connection
            .prepare(
                "SELECT turn_id,root_turn_id,usage_kind
                 FROM selected_activity_turns ORDER BY usage_kind,turn_id",
            )
            .unwrap();
        let actual = statement
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
        assert_eq!(
            actual,
            [
                ("kind-root".into(), "kind-root".into(), 0),
                ("kind-agent".into(), "kind-root".into(), 1),
                ("kind-review".into(), "kind-root".into(), 2),
            ]
        );
    }
}
