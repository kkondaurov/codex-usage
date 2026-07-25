use crate::storage::{Db, StorageExecutor, WorkClass};
use anyhow::{Result, anyhow};
use rusqlite::{Connection, InterruptHandle, Transaction, TransactionBehavior};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone)]
pub(crate) struct ReadRuntime {
    database: Db,
    executor: StorageExecutor,
}

impl ReadRuntime {
    pub(crate) fn new(database: Db, executor: StorageExecutor) -> Self {
        Self { database, executor }
    }

    pub(crate) fn database_path(&self) -> &Path {
        self.database.path()
    }

    pub(crate) fn database_storage_bytes(&self) -> u64 {
        self.database.storage_bytes()
    }

    pub(crate) async fn snapshot<T, F>(&self, class: WorkClass, read: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let cancellation = Arc::new(QueryCancellation::default());
        let worker_cancellation = cancellation.clone();
        let mut cancel_on_drop = CancelQueryOnDrop {
            cancellation,
            armed: true,
        };
        let database = self.database.clone();
        let result = self
            .executor
            .run(class, move || {
                let connection = database.connect()?;
                worker_cancellation.install(&connection)?;
                let transaction =
                    Transaction::new_unchecked(&connection, TransactionBehavior::Deferred)?;
                let value = read(&transaction)?;
                transaction.commit()?;
                Ok(value)
            })
            .await;
        cancel_on_drop.armed = false;
        result
    }
}

#[derive(Default)]
struct QueryCancellation {
    cancelled: AtomicBool,
    interrupt: Mutex<Option<InterruptHandle>>,
}

impl QueryCancellation {
    fn install(self: &Arc<Self>, connection: &Connection) -> Result<()> {
        let cancellation = self.clone();
        connection.progress_handler(
            4_096,
            Some(move || cancellation.cancelled.load(Ordering::Acquire)),
        );
        let mut interrupt = self
            .interrupt
            .lock()
            .map_err(|_| anyhow!("database cancellation lock poisoned"))?;
        *interrupt = Some(connection.get_interrupt_handle());
        if self.cancelled.load(Ordering::Acquire) {
            if let Some(handle) = interrupt.as_ref() {
                handle.interrupt();
            }
            return Err(anyhow!("database query cancelled"));
        }
        Ok(())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(interrupt) = self.interrupt.lock()
            && let Some(handle) = interrupt.as_ref()
        {
            handle.interrupt();
        }
    }
}

struct CancelQueryOnDrop {
    cancellation: Arc<QueryCancellation>,
    armed: bool,
}

impl Drop for CancelQueryOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Condvar, Mutex};

    struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn cancellation_before_connection_install_is_observed() {
        let cancellation = Arc::new(QueryCancellation::default());
        cancellation.cancel();
        let connection = Connection::open_in_memory().unwrap();

        let error = cancellation.install(&connection).unwrap_err();

        assert_eq!(error.to_string(), "database query cancelled");
    }

    #[tokio::test]
    async fn dropping_snapshot_interrupts_the_running_sqlite_query() {
        let temp = tempfile::tempdir().unwrap();
        let database = Db::open(temp.path().join("usage.db")).unwrap();
        let runtime = ReadRuntime::new(database, StorageExecutor::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            runtime
                .snapshot(WorkClass::Heavy, move |connection| {
                    let _done = NotifyOnDrop(Some(done_tx));
                    let _ = started_tx.send(());
                    let _: i64 = connection.query_row(
                        "WITH RECURSIVE counter(value) AS (
                             VALUES(0) UNION ALL
                             SELECT value+1 FROM counter WHERE value<100000000
                         ) SELECT SUM(value) FROM counter",
                        [],
                        |row| row.get(0),
                    )?;
                    Ok(())
                })
                .await
        });

        started_rx.await.unwrap();
        task.abort();
        tokio::time::timeout(std::time::Duration::from_secs(2), done_rx)
            .await
            .expect("SQLite query kept running after its request was cancelled")
            .unwrap();
        let _ = task.await;
    }

    #[tokio::test]
    async fn dropping_snapshot_stays_cancelled_between_sqlite_statements() {
        let temp = tempfile::tempdir().unwrap();
        let database = Db::open(temp.path().join("usage.db")).unwrap();
        let runtime = ReadRuntime::new(database, StorageExecutor::default());
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let (first_done_tx, first_done_rx) = tokio::sync::oneshot::channel();
        let (second_done_tx, second_done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            runtime
                .snapshot(WorkClass::Heavy, move |connection| {
                    let first: i64 = connection.query_row("SELECT 1", [], |row| row.get(0))?;
                    assert_eq!(first, 1);
                    let _ = first_done_tx.send(());

                    let (lock, ready) = &*worker_gate;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = ready.wait(released).unwrap();
                    }
                    drop(released);

                    let second = connection.query_row(
                        "WITH RECURSIVE counter(value) AS (
                             VALUES(0) UNION ALL
                             SELECT value+1 FROM counter WHERE value<100000000
                         ) SELECT SUM(value) FROM counter",
                        [],
                        |row| row.get::<_, i64>(0),
                    );
                    let interrupted = matches!(
                        second,
                        Err(rusqlite::Error::SqliteFailure(error, _))
                            if error.code == rusqlite::ErrorCode::OperationInterrupted
                    );
                    let _ = second_done_tx.send(interrupted);
                    Ok(())
                })
                .await
        });

        first_done_rx.await.unwrap();
        // No SQLite statement is running at this point. The immediate
        // InterruptHandle call is therefore insufficient on its own; the
        // connection-wide progress handler must stop the next statement.
        task.abort();
        let _ = task.await;
        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), second_done_rx)
                .await
                .expect("SQLite cancellation was lost between statements")
                .unwrap(),
            "the second SQLite statement was not interrupted"
        );
    }
}
