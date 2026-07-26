use crate::storage::{Db, StorageExecutor, WorkClass};
use anyhow::{Result, anyhow};
use rusqlite::{Connection, InterruptHandle, Transaction, TransactionBehavior};
use std::{
    collections::HashMap,
    future::Future,
    hash::Hash,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::watch;

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

/// Shares one running operation between requests with the same canonical key.
///
/// Completed values are removed before waiters are released. This is
/// deliberately not a response cache: a later request always starts a fresh
/// database snapshot, so ingestion and pricing writes need no invalidation
/// hook. If the request leading a flight is cancelled, its waiters retry and
/// elect a new leader.
pub(crate) struct SingleFlight<K, V> {
    flights: Arc<Mutex<HashMap<K, Arc<Flight<V>>>>>,
}

impl<K, V> Clone for SingleFlight<K, V> {
    fn clone(&self) -> Self {
        Self {
            flights: self.flights.clone(),
        }
    }
}

impl<K, V> Default for SingleFlight<K, V> {
    fn default() -> Self {
        Self {
            flights: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, V> SingleFlight<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub(crate) async fn run<F, Fut>(&self, key: K, operation: F) -> Result<V>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V>>,
    {
        let mut operation = Some(operation);
        loop {
            let (flight, is_leader) = {
                let mut flights = self
                    .flights
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(flight) = flights.get(&key) {
                    (flight.clone(), false)
                } else {
                    let flight = Arc::new(Flight::new());
                    flights.insert(key.clone(), flight.clone());
                    (flight, true)
                }
            };

            if is_leader {
                let leader = FlightLeader::new(self.flights.clone(), key.clone(), flight);
                let operation = operation
                    .take()
                    .expect("a single-flight operation can lead only once");
                let outcome = operation()
                    .await
                    .map_err(|error| Arc::<str>::from(error.to_string()));
                leader.complete(outcome.clone());
                return shared_outcome(outcome);
            }

            let mut updates = flight.state.subscribe();
            loop {
                let state = updates.borrow().clone();
                match state {
                    FlightState::Running => {
                        if updates.changed().await.is_err() {
                            break;
                        }
                    }
                    FlightState::Complete(outcome) => return shared_outcome(outcome),
                    FlightState::Abandoned => break,
                }
            }
        }
    }
}

fn shared_outcome<V>(outcome: std::result::Result<V, Arc<str>>) -> Result<V> {
    outcome.map_err(|message| anyhow!(message.to_string()))
}

struct Flight<V> {
    state: watch::Sender<FlightState<V>>,
}

impl<V> Flight<V> {
    fn new() -> Self {
        let (state, _) = watch::channel(FlightState::Running);
        Self { state }
    }
}

#[derive(Clone)]
enum FlightState<V> {
    Running,
    Complete(std::result::Result<V, Arc<str>>),
    Abandoned,
}

struct FlightLeader<K: Eq + Hash, V> {
    flights: Arc<Mutex<HashMap<K, Arc<Flight<V>>>>>,
    key: K,
    flight: Arc<Flight<V>>,
    completed: bool,
}

impl<K, V> FlightLeader<K, V>
where
    K: Eq + Hash,
{
    fn new(
        flights: Arc<Mutex<HashMap<K, Arc<Flight<V>>>>>,
        key: K,
        flight: Arc<Flight<V>>,
    ) -> Self {
        Self {
            flights,
            key,
            flight,
            completed: false,
        }
    }

    fn complete(mut self, outcome: std::result::Result<V, Arc<str>>) {
        self.remove();
        self.flight
            .state
            .send_replace(FlightState::Complete(outcome));
        self.completed = true;
    }

    fn remove(&self) {
        let mut flights = self
            .flights
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            flights.remove(&self.key);
        }
    }
}

impl<K, V> Drop for FlightLeader<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if !self.completed {
            self.remove();
            self.flight.state.send_replace(FlightState::Abandoned);
        }
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
    use std::sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

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

    #[tokio::test(flavor = "current_thread")]
    async fn identical_in_flight_requests_run_the_operation_once() {
        let flights = SingleFlight::<u8, u8>::default();
        let runs = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let leader = {
            let flights = flights.clone();
            let runs = runs.clone();
            let release = release.clone();
            tokio::spawn(async move {
                flights
                    .run(1, move || async move {
                        runs.fetch_add(1, AtomicOrdering::SeqCst);
                        let _ = started_tx.send(());
                        release.notified().await;
                        Ok(7)
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let (joining_tx, joining_rx) = tokio::sync::oneshot::channel();
        let follower = {
            let flights = flights.clone();
            let runs = runs.clone();
            tokio::spawn(async move {
                let _ = joining_tx.send(());
                flights
                    .run(1, move || async move {
                        runs.fetch_add(1, AtomicOrdering::SeqCst);
                        Ok(8)
                    })
                    .await
            })
        };
        joining_rx.await.unwrap();
        release.notify_one();

        assert_eq!(leader.await.unwrap().unwrap(), 7);
        assert_eq!(follower.await.unwrap().unwrap(), 7);
        assert_eq!(runs.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_values_are_not_cached() {
        let flights = SingleFlight::<u8, usize>::default();
        let runs = Arc::new(AtomicUsize::new(0));

        let first = {
            let runs = runs.clone();
            flights
                .run(1, move || async move {
                    Ok(runs.fetch_add(1, AtomicOrdering::SeqCst) + 1)
                })
                .await
                .unwrap()
        };
        let second = {
            let runs = runs.clone();
            flights
                .run(1, move || async move {
                    Ok(runs.fetch_add(1, AtomicOrdering::SeqCst) + 1)
                })
                .await
                .unwrap()
        };

        assert_eq!((first, second), (1, 2));
        assert_eq!(runs.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn follower_retries_when_the_leading_request_is_cancelled() {
        let flights = SingleFlight::<u8, u8>::default();
        let runs = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let leader = {
            let flights = flights.clone();
            let runs = runs.clone();
            tokio::spawn(async move {
                flights
                    .run(1, move || async move {
                        runs.fetch_add(1, AtomicOrdering::SeqCst);
                        let _ = started_tx.send(());
                        std::future::pending::<()>().await;
                        Ok(7)
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let (joining_tx, joining_rx) = tokio::sync::oneshot::channel();
        let follower = {
            let flights = flights.clone();
            let runs = runs.clone();
            tokio::spawn(async move {
                let _ = joining_tx.send(());
                flights
                    .run(1, move || async move {
                        runs.fetch_add(1, AtomicOrdering::SeqCst);
                        Ok(9)
                    })
                    .await
            })
        };
        joining_rx.await.unwrap();
        leader.abort();

        assert_eq!(follower.await.unwrap().unwrap(), 9);
        assert_eq!(runs.load(AtomicOrdering::SeqCst), 2);
    }
}
