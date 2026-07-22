//! Read helpers for the compact Activity event projection.
//!
//! Ingested event and message rows are immutable after insertion. The one
//! supported replacement path removes an entire rollout inside its ingest
//! transaction and inserts that rollout again; the event foreign key clears
//! the projection on delete, and the insert triggers rebuild it. Surgical
//! event/message updates or deletes are deliberately outside this contract.

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ActivityCursor {
    version: u8,
    thread_id: String,
    turn_id: String,
    timestamp: String,
    source_line: i64,
    event_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexedActivityEvent {
    pub(crate) event_id: String,
    pub(crate) source_line: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct ActivityIndexPage {
    pub(crate) events: Vec<IndexedActivityEvent>,
    pub(crate) total: u64,
    pub(crate) next_cursor: Option<String>,
}

pub(crate) fn validate_cursor_for(value: &str, thread_id: &str, turn_id: &str) -> Result<()> {
    let cursor = decode_cursor(value)?;
    if cursor.thread_id != thread_id || cursor.turn_id != turn_id {
        bail!("Activity cursor belongs to a different turn");
    }
    Ok(())
}

pub(crate) fn query_all(
    connection: &Connection,
    thread_id: &str,
) -> Result<Vec<IndexedActivityEvent>> {
    let mut statement = connection.prepare(
        "WITH canonical_event_ids(event_id) AS MATERIALIZED (
             SELECT substr(MIN(printf('%020d%s',source_line,event_id)),21)
             FROM activity_event_index
             WHERE thread_id=?1
             GROUP BY canonical_key
         )
         SELECT projected.event_id,projected.source_line
         FROM canonical_event_ids
         JOIN activity_event_index projected
           ON projected.event_id=canonical_event_ids.event_id
         ORDER BY projected.timestamp DESC,projected.source_line DESC,projected.event_id DESC",
    )?;
    statement
        .query_map([thread_id], |row| {
            Ok(IndexedActivityEvent {
                event_id: row.get(0)?,
                source_line: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn query_all_in_turn(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<IndexedActivityEvent>> {
    let mut statement = connection.prepare(
        "SELECT event_id,source_line
         FROM activity_event_index INDEXED BY idx_activity_event_index_turn_time
         WHERE thread_id=?1 AND turn_key=?2
         ORDER BY timestamp DESC,source_line DESC,event_id DESC",
    )?;
    statement
        .query_map(params![thread_id, turn_id], |row| {
            Ok(IndexedActivityEvent {
                event_id: row.get(0)?,
                source_line: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn query_page(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
    page_size: u64,
    cursor: Option<&str>,
    fallback_offset: u64,
) -> Result<ActivityIndexPage> {
    let total = connection
        .query_row(
            "SELECT COUNT(*)
             FROM activity_event_index INDEXED BY idx_activity_event_index_turn_time
             WHERE thread_id=?1 AND turn_key=?2",
            params![thread_id, turn_id],
            |row| row.get::<_, i64>(0),
        )?
        .max(0) as u64;
    let fetch_limit = page_size.saturating_add(1).min(i64::MAX as u64) as i64;
    let rows = if let Some(cursor) = cursor {
        let cursor = decode_cursor(cursor)?;
        if cursor.thread_id != thread_id || cursor.turn_id != turn_id {
            bail!("Activity cursor belongs to a different turn");
        }
        query_after_cursor(connection, thread_id, turn_id, &cursor, fetch_limit)?
    } else {
        query_at_offset(
            connection,
            thread_id,
            turn_id,
            fetch_limit,
            fallback_offset.min(i64::MAX as u64) as i64,
        )?
    };
    let has_more = rows.len() as u64 > page_size;
    let mut rows = rows;
    if has_more {
        rows.truncate(page_size as usize);
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|row| encode_cursor(row, thread_id, turn_id))
            .transpose()?
    } else {
        None
    };
    Ok(ActivityIndexPage {
        events: rows
            .into_iter()
            .map(|row| IndexedActivityEvent {
                event_id: row.event_id,
                source_line: row.source_line,
            })
            .collect(),
        total,
        next_cursor,
    })
}

#[derive(Clone, Debug)]
struct IndexedActivityRow {
    event_id: String,
    timestamp: String,
    source_line: i64,
}

fn query_after_cursor(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
    cursor: &ActivityCursor,
    limit: i64,
) -> Result<Vec<IndexedActivityRow>> {
    let mut statement = connection.prepare(
        "SELECT event_id,timestamp,source_line
         FROM activity_event_index INDEXED BY idx_activity_event_index_turn_time
         WHERE thread_id=?1 AND turn_key=?2
           AND (timestamp,source_line,event_id)<(?3,?4,?5)
         ORDER BY timestamp DESC,source_line DESC,event_id DESC
         LIMIT ?6",
    )?;
    statement
        .query_map(
            params![
                thread_id,
                turn_id,
                cursor.timestamp,
                cursor.source_line,
                cursor.event_id,
                limit
            ],
            |row| {
                Ok(IndexedActivityRow {
                    event_id: row.get(0)?,
                    timestamp: row.get(1)?,
                    source_line: row.get(2)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_at_offset(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<IndexedActivityRow>> {
    let mut statement = connection.prepare(
        "SELECT event_id,timestamp,source_line
         FROM activity_event_index INDEXED BY idx_activity_event_index_turn_time
         WHERE thread_id=?1 AND turn_key=?2
         ORDER BY timestamp DESC,source_line DESC,event_id DESC
         LIMIT ?3 OFFSET ?4",
    )?;
    statement
        .query_map(params![thread_id, turn_id, limit, offset], |row| {
            Ok(IndexedActivityRow {
                event_id: row.get(0)?,
                timestamp: row.get(1)?,
                source_line: row.get(2)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn encode_cursor(row: &IndexedActivityRow, thread_id: &str, turn_id: &str) -> Result<String> {
    serde_json::to_string(&ActivityCursor {
        version: 1,
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        timestamp: row.timestamp.clone(),
        source_line: row.source_line,
        event_id: row.event_id.clone(),
    })
    .context("failed to encode Activity cursor")
}

fn decode_cursor(value: &str) -> Result<ActivityCursor> {
    if value.len() > 4_096 {
        bail!("Activity cursor is too long");
    }
    let cursor: ActivityCursor = serde_json::from_str(value).context("invalid Activity cursor")?;
    if cursor.version != 1
        || cursor.thread_id.is_empty()
        || cursor.turn_id.is_empty()
        || cursor.timestamp.is_empty()
        || cursor.event_id.is_empty()
        || cursor.source_line < 0
    {
        bail!("invalid Activity cursor");
    }
    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use super::{
        IndexedActivityEvent, query_all, query_all_in_turn, query_page, validate_cursor_for,
    };
    use crate::db::Db;
    use rusqlite::{Connection, params};

    fn insert_scope(connection: &Connection, thread_id: &str, rollout_id: &str, turn_id: &str) {
        connection
            .execute(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES(?1,'2026-01-01T00:00:00Z','2026-01-01T00:00:03Z')",
                [thread_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES(?1,?2,'2026-01-01T00:00:00Z','2026-01-01T00:00:03Z')",
                params![rollout_id, thread_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES(?1,?2,?3,'2026-01-01T00:00:00Z','completed')",
                params![turn_id, thread_id, rollout_id],
            )
            .unwrap();
    }

    fn legacy_events(
        connection: &Connection,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> Vec<IndexedActivityEvent> {
        let mut statement = connection
            .prepare(
                "WITH candidate_event_ids(id) AS MATERIALIZED (
                     SELECT substr(MIN(printf('%020d%s',e.source_line,e.id)),21)
                     FROM events e
                     WHERE e.thread_id=?1
                       AND (?2 IS NULL OR e.turn_id=?2)
                       AND e.kind NOT IN (
                           'turn_started','system','tool_output','tool_completed'
                       )
                       AND (
                           e.kind<>'turn_completed'
                           OR NOT EXISTS(
                               SELECT 1
                               FROM events final_event
                               LEFT JOIN messages final_message
                                 ON final_message.id=COALESCE(
                                        final_event.call_id,final_event.id
                                    )
                                AND final_message.thread_id=final_event.thread_id
                               WHERE final_event.thread_id=e.thread_id
                                 AND final_event.turn_id=e.turn_id
                                 AND final_event.kind='final'
                                 AND trim(COALESCE(
                                        final_event.body,final_message.content,''
                                     ))<>''
                           )
                       )
                     GROUP BY
                       CASE
                           WHEN e.kind='tool_call' AND e.call_id IS NOT NULL THEN 1
                           ELSE 0
                       END,
                       CASE
                           WHEN e.kind='tool_call' AND e.call_id IS NOT NULL
                               THEN e.rollout_id
                           ELSE e.id
                       END,
                       CASE
                           WHEN e.kind='tool_call' AND e.call_id IS NOT NULL
                               THEN e.call_id
                           ELSE NULL
                       END
                 )
                 SELECT event.id,event.source_line
                 FROM candidate_event_ids
                 JOIN events event ON event.id=candidate_event_ids.id
                 ORDER BY event.timestamp DESC,event.source_line DESC,event.id DESC",
            )
            .unwrap();
        statement
            .query_map(params![thread_id, turn_id], |row| {
                Ok(IndexedActivityEvent {
                    event_id: row.get(0)?,
                    source_line: row.get(1)?,
                })
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }

    fn ids(events: &[IndexedActivityEvent]) -> Vec<&str> {
        events.iter().map(|event| event.event_id.as_str()).collect()
    }

    #[test]
    fn cursor_pages_seek_without_repeating_events() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-01-01T00:00:00Z','2026-01-01T00:00:03Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES('thread','thread','2026-01-01T00:00:00Z','2026-01-01T00:00:03Z');
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('turn','thread','thread','2026-01-01T00:00:00Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,native
                 ) VALUES
                    ('one','thread','thread','turn','2026-01-01T00:00:01Z',1,'assistant',1),
                    ('two','thread','thread','turn','2026-01-01T00:00:02Z',2,'assistant',1),
                    ('three','thread','thread','turn','2026-01-01T00:00:03Z',3,'assistant',1);",
            )
            .unwrap();

        let first = query_page(&connection, "thread", "turn", 2, None, 0).unwrap();
        assert_eq!(first.total, 3);
        assert_eq!(
            first
                .events
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["three", "two"]
        );
        let cursor = first.next_cursor.unwrap();
        validate_cursor_for(&cursor, "thread", "turn").unwrap();
        assert!(validate_cursor_for(&cursor, "thread", "another-turn").is_err());
        let second = query_page(&connection, "thread", "turn", 2, Some(&cursor), 0).unwrap();
        assert_eq!(
            second
                .events
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["one"]
        );
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn projection_deduplicates_tool_lifecycle_at_insert_time() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-01-01T00:00:00Z','2026-01-01T00:00:03Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES('thread','thread','2026-01-01T00:00:00Z','2026-01-01T00:00:03Z');
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('turn','thread','thread','2026-01-01T00:00:00Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,call_id,native
                 ) VALUES
                    ('late','thread','thread','turn','2026-01-01T00:00:03Z',3,'tool_call','call',1),
                    ('early','thread','thread','turn','2026-01-01T00:00:01Z',1,'tool_call','call',1);",
            )
            .unwrap();

        let page = query_page(&connection, "thread", "turn", 10, None, 0).unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.events[0].event_id, "early");
    }

    #[test]
    fn projection_matches_legacy_cte_for_thread_and_turn_queries() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at)
                 VALUES('thread','2026-01-01T00:00:00Z','2026-01-01T00:00:20Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES('rollout','thread','2026-01-01T00:00:00Z','2026-01-01T00:00:20Z');
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status) VALUES
                    ('turn-a','thread','rollout','2026-01-01T00:00:00Z','completed'),
                    ('turn-b','thread','rollout','2026-01-01T00:00:00Z','completed'),
                    ('turn-c','thread','rollout','2026-01-01T00:00:00Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,
                    body,call_id,native
                 ) VALUES
                    ('visible-a','thread','rollout','turn-a','2026-01-01T00:00:01Z',1,
                     'assistant',NULL,NULL,1),
                    ('tool-a-late','thread','rollout','turn-a','2026-01-01T00:00:05Z',5,
                     'tool_call',NULL,'shared-call',1),
                    ('tool-a-early','thread','rollout','turn-a','2026-01-01T00:00:03Z',3,
                     'tool_call',NULL,'shared-call',1),
                    ('tool-b-global','thread','rollout','turn-b','2026-01-01T00:00:02Z',2,
                     'tool_call',NULL,'shared-call',1),
                    ('tool-without-call','thread','rollout','turn-a','2026-01-01T00:00:04Z',4,
                     'tool_call',NULL,NULL,1),
                    ('excluded-system','thread','rollout','turn-a','2026-01-01T00:00:06Z',6,
                     'system',NULL,NULL,1),
                    ('excluded-output','thread','rollout','turn-a','2026-01-01T00:00:07Z',7,
                     'tool_output',NULL,NULL,1),
                    ('completed-a','thread','rollout','turn-a','2026-01-01T00:00:08Z',8,
                     'turn_completed',NULL,NULL,1),
                    ('final-a','thread','rollout','turn-a','2026-01-01T00:00:09Z',9,
                     'final',' direct answer ',NULL,1),
                    ('completed-b','thread','rollout','turn-b','2026-01-01T00:00:10Z',10,
                     'turn_completed',NULL,NULL,1),
                    ('final-b','thread','rollout','turn-b','2026-01-01T00:00:11Z',11,
                     'final',NULL,'late-message',1),
                    ('completed-c','thread','rollout','turn-c','2026-01-01T00:00:12Z',12,
                     'turn_completed',NULL,NULL,1),
                    ('empty-final-c','thread','rollout','turn-c','2026-01-01T00:00:13Z',13,
                     'final','   ',NULL,1);
                 INSERT INTO messages(
                    id,thread_id,rollout_id,turn_id,timestamp,role,content,source_line
                 ) VALUES(
                    'late-message','thread','rollout','turn-b','2026-01-01T00:00:14Z',
                    'assistant','message-backed answer',14
                 );",
            )
            .unwrap();

        let projected_thread = query_all(&connection, "thread").unwrap();
        assert_eq!(projected_thread, legacy_events(&connection, "thread", None));
        assert!(ids(&projected_thread).contains(&"tool-b-global"));
        assert!(!ids(&projected_thread).contains(&"tool-a-early"));
        assert!(!ids(&projected_thread).contains(&"tool-a-late"));

        for turn_id in ["turn-a", "turn-b", "turn-c"] {
            let projected_turn = query_all_in_turn(&connection, "thread", turn_id).unwrap();
            let legacy_turn = legacy_events(&connection, "thread", Some(turn_id));
            assert_eq!(
                projected_turn, legacy_turn,
                "projection drift for {turn_id}"
            );

            let page = query_page(&connection, "thread", turn_id, 100, None, 0).unwrap();
            assert_eq!(page.events, legacy_turn, "paged drift for {turn_id}");
            assert_eq!(page.total as usize, legacy_turn.len());
            assert!(page.next_cursor.is_none());
        }
    }

    #[test]
    fn cursor_orders_complete_ties_and_rejects_scope_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        insert_scope(&connection, "thread", "rollout", "turn");
        connection
            .execute_batch(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,native
                 ) VALUES
                    ('alpha','thread','rollout','turn','2026-01-01T00:00:01Z',7,'assistant',1),
                    ('beta','thread','rollout','turn','2026-01-01T00:00:01Z',7,'assistant',1),
                    ('gamma','thread','rollout','turn','2026-01-01T00:00:01Z',7,'assistant',1);",
            )
            .unwrap();

        let first = query_page(&connection, "thread", "turn", 1, None, 0).unwrap();
        assert_eq!(ids(&first.events), vec!["gamma"]);
        let first_cursor = first.next_cursor.unwrap();
        assert!(validate_cursor_for(&first_cursor, "other-thread", "turn").is_err());
        assert!(validate_cursor_for(&first_cursor, "thread", "other-turn").is_err());
        assert!(
            query_page(
                &connection,
                "other-thread",
                "turn",
                1,
                Some(&first_cursor),
                0
            )
            .is_err()
        );
        assert!(
            query_page(
                &connection,
                "thread",
                "other-turn",
                1,
                Some(&first_cursor),
                0
            )
            .is_err()
        );

        let second = query_page(&connection, "thread", "turn", 1, Some(&first_cursor), 0).unwrap();
        assert_eq!(ids(&second.events), vec!["beta"]);
        let second_cursor = second.next_cursor.unwrap();
        let third = query_page(&connection, "thread", "turn", 1, Some(&second_cursor), 0).unwrap();
        assert_eq!(ids(&third.events), vec!["alpha"]);
        assert!(third.next_cursor.is_none());
    }

    #[test]
    fn cursor_is_stable_when_newer_rows_arrive_between_pages() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        insert_scope(&connection, "thread", "rollout", "turn");
        connection
            .execute_batch(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,native
                 ) VALUES
                    ('one','thread','rollout','turn','2026-01-01T00:00:01Z',1,'assistant',1),
                    ('two','thread','rollout','turn','2026-01-01T00:00:02Z',2,'assistant',1),
                    ('three','thread','rollout','turn','2026-01-01T00:00:03Z',3,'assistant',1);",
            )
            .unwrap();

        let first = query_page(&connection, "thread", "turn", 2, None, 0).unwrap();
        assert_eq!(ids(&first.events), vec!["three", "two"]);
        let cursor = first.next_cursor.unwrap();
        connection
            .execute(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,native
                 ) VALUES(
                    'newer','thread','rollout','turn','2026-01-01T00:00:04Z',4,'assistant',1
                 )",
                [],
            )
            .unwrap();

        let second = query_page(&connection, "thread", "turn", 2, Some(&cursor), 0).unwrap();
        assert_eq!(second.total, 4);
        assert_eq!(ids(&second.events), vec!["one"]);
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn whole_rollout_replacement_rebuilds_projection_without_stale_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        insert_scope(&connection, "thread", "rollout", "turn");
        connection
            .execute_batch(
                "INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,call_id,native
                 ) VALUES
                    ('old-tool-late','thread','rollout','turn','2026-01-01T00:00:03Z',3,
                     'tool_call','call',1),
                    ('old-tool-early','thread','rollout','turn','2026-01-01T00:00:01Z',1,
                     'tool_call','call',1),
                    ('old-visible','thread','rollout','turn','2026-01-01T00:00:02Z',2,
                     'assistant',NULL,1);",
            )
            .unwrap();
        assert_eq!(
            query_all(&connection, "thread").unwrap(),
            legacy_events(&connection, "thread", None)
        );
        assert_eq!(ids(&query_all(&connection, "thread").unwrap()).len(), 2);

        // This is the supported reconciliation boundary used by ingestion.
        // It intentionally replaces the complete rollout, never individual
        // event or message rows.
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 DELETE FROM usage_facts WHERE rollout_id='rollout';
                 DELETE FROM events WHERE rollout_id='rollout';
                 DELETE FROM messages WHERE rollout_id='rollout';
                 DELETE FROM tool_calls WHERE rollout_id='rollout';
                 DELETE FROM turns WHERE rollout_id='rollout';
                 DELETE FROM agent_runs WHERE rollout_id='rollout';
                 DELETE FROM rollouts WHERE id='rollout';
                 COMMIT;",
            )
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM activity_event_index", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );

        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at)
                 VALUES('rollout','thread','2026-01-01T00:00:00Z','2026-01-01T00:00:06Z');
                 INSERT INTO turns(id,thread_id,rollout_id,started_at,status)
                 VALUES('turn','thread','rollout','2026-01-01T00:00:00Z','completed');
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,body,native
                 ) VALUES
                    ('replacement-completed','thread','rollout','turn',
                     '2026-01-01T00:00:04Z',4,'turn_completed',NULL,1),
                    ('replacement-final','thread','rollout','turn',
                     '2026-01-01T00:00:05Z',5,'final','done',1),
                    ('replacement-visible','thread','rollout','turn',
                     '2026-01-01T00:00:06Z',6,'assistant',NULL,1);
                 COMMIT;",
            )
            .unwrap();

        let projected = query_all(&connection, "thread").unwrap();
        assert_eq!(projected, legacy_events(&connection, "thread", None));
        assert_eq!(
            ids(&projected),
            vec!["replacement-visible", "replacement-final"]
        );
        assert!(!ids(&projected).iter().any(|id| id.starts_with("old-")));
    }
}
