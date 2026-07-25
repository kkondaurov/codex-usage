use anyhow::Result;
use rusqlite::{Connection, Transaction, TransactionBehavior};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct ReconciliationCandidate {
    pub(in crate::ingest) rollout_id: String,
    pub(in crate::ingest) path: String,
    pub(in crate::ingest) root_thread_id: Option<String>,
}

/// Projection's sole adapter around a caller-supplied SQLite connection.
///
/// The connection is borrowed only long enough to begin one named projection
/// transaction. No raw connection or transaction can escape back to Source.
pub(in crate::ingest) struct ProjectionConnection<'connection> {
    connection: &'connection mut Connection,
}

impl<'connection> ProjectionConnection<'connection> {
    pub(in crate::ingest) fn new(connection: &'connection mut Connection) -> Self {
        Self { connection }
    }

    /// Load the complete ordered removal candidate set before claiming the
    /// IMMEDIATE reconciliation writer transaction.
    pub(in crate::ingest) fn reconciliation_candidates(
        &self,
    ) -> Result<Vec<ReconciliationCandidate>> {
        let mut statement = self
            .connection
            .prepare("SELECT rollout_id,path,root_thread_id FROM source_files")?;
        statement
            .query_map([], |row| {
                Ok(ReconciliationCandidate {
                    rollout_id: row.get(0)?,
                    path: row.get(1)?,
                    root_thread_id: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Claim writer ownership before any projection read.
    pub(in crate::ingest) fn begin_file_projection(self) -> Result<ProjectionTx<'connection>> {
        self.begin_immediate()
    }

    /// Reconciliation removes normalized rows after reading its source list.
    pub(in crate::ingest) fn begin_reconciliation(self) -> Result<ProjectionTx<'connection>> {
        self.begin_immediate()
    }

    /// Unchanged-source metadata and title import preserve their deferred
    /// transaction behavior because they do not perform read-before-write
    /// projection decisions.
    pub(in crate::ingest) fn begin_metadata_refresh(self) -> Result<ProjectionTx<'connection>> {
        self.begin_deferred()
    }

    pub(in crate::ingest) fn begin_title_import(self) -> Result<ProjectionTx<'connection>> {
        self.begin_deferred()
    }

    fn begin_immediate(self) -> Result<ProjectionTx<'connection>> {
        Ok(ProjectionTx {
            sqlite: self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?,
        })
    }

    fn begin_deferred(self) -> Result<ProjectionTx<'connection>> {
        Ok(ProjectionTx {
            sqlite: self.connection.transaction()?,
        })
    }
}

/// Opaque projection transaction.
///
/// Only sibling Projection modules may touch the private SQLite field. The
/// ingestion orchestration receives named domain operations and `commit`, never
/// a generic execute/query escape hatch.
pub(in crate::ingest) struct ProjectionTx<'connection> {
    pub(in crate::ingest::projection) sqlite: Transaction<'connection>,
}

impl ProjectionTx<'_> {
    pub(in crate::ingest) fn commit(self) -> Result<()> {
        self.sqlite.commit().map_err(Into::into)
    }

    #[cfg(test)]
    pub(in crate::ingest::projection) fn rollback(self) -> Result<()> {
        self.sqlite.rollback().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_is_automatic_and_commit_is_explicit() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE projected(value INTEGER NOT NULL);")
            .unwrap();

        {
            let transaction = ProjectionConnection::new(&mut connection)
                .begin_file_projection()
                .unwrap();
            transaction
                .sqlite
                .execute("INSERT INTO projected(value) VALUES(1)", [])
                .unwrap();
        }
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM projected", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );

        let transaction = ProjectionConnection::new(&mut connection)
            .begin_file_projection()
            .unwrap();
        transaction
            .sqlite
            .execute("INSERT INTO projected(value) VALUES(2)", [])
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT value FROM projected", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}
