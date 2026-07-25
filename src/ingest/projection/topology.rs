use crate::storage::Db;
use anyhow::Result;
use std::collections::HashMap;

/// Load the durable rollout-to-thread anchors used when resolving a newly
/// discovered source catalog.
pub(in crate::ingest) fn load_existing_owner_threads(db: &Db) -> Result<HashMap<String, String>> {
    let connection = db.connect()?;
    let mut statement = connection.prepare("SELECT id,thread_id FROM rollouts")?;
    statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_owner_threads_return_exact_durable_anchors() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,started_at,last_event_at) VALUES
                    ('thread-a','2026-07-25T00:00:00Z','2026-07-25T00:00:00Z'),
                    ('thread-b','2026-07-25T00:00:00Z','2026-07-25T00:00:00Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at) VALUES
                    ('owner-a','thread-a','2026-07-25T00:00:00Z','2026-07-25T00:00:00Z'),
                    ('owner-b','thread-b','2026-07-25T00:00:00Z','2026-07-25T00:00:00Z');",
            )
            .unwrap();
        drop(connection);

        assert_eq!(
            load_existing_owner_threads(&db).unwrap(),
            HashMap::from([
                ("owner-a".to_owned(), "thread-a".to_owned()),
                ("owner-b".to_owned(), "thread-b".to_owned()),
            ])
        );
    }
}
