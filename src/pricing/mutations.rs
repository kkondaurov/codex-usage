use super::{ManualAlias, ManualPrice, ManualPricingStore, MutationError};
use crate::storage::{Db, StorageExecutor, WorkClass};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// Serialized manual-pricing mutations executed off the async runtime.
///
/// The owned gate is acquired before a blocking-worker permit and moved into
/// the blocking operation. Dropping an HTTP request therefore cannot allow a
/// second writer to overtake the first while its synchronous work continues.
#[derive(Clone, Debug)]
pub(crate) struct PricingMutations {
    database: Db,
    store: ManualPricingStore,
    executor: StorageExecutor,
    gate: Arc<AsyncMutex<()>>,
}

impl PricingMutations {
    pub(crate) fn new(database: Db, store: ManualPricingStore, executor: StorageExecutor) -> Self {
        Self {
            database,
            store,
            executor,
            gate: Arc::new(AsyncMutex::new(())),
        }
    }

    pub(crate) async fn save_price(&self, price: ManualPrice) -> Result<(), MutationError> {
        let database = self.database.clone();
        let store = self.store.clone();
        self.run(move || store.save_price(&database, price)).await
    }

    pub(crate) async fn delete_price(
        &self,
        model_id: String,
        effective_from: Option<String>,
    ) -> Result<(), MutationError> {
        let database = self.database.clone();
        let store = self.store.clone();
        self.run(move || store.delete_price(&database, &model_id, effective_from.as_deref()))
            .await
    }

    pub(crate) async fn save_alias(&self, alias: ManualAlias) -> Result<(), MutationError> {
        let database = self.database.clone();
        let store = self.store.clone();
        self.run(move || store.save_alias(&database, alias)).await
    }

    pub(crate) async fn delete_alias(
        &self,
        observed_model_id: String,
    ) -> Result<(), MutationError> {
        let database = self.database.clone();
        let store = self.store.clone();
        self.run(move || store.delete_alias(&database, &observed_model_id))
            .await
    }

    async fn run<F>(&self, mutation: F) -> Result<(), MutationError>
    where
        F: FnOnce() -> Result<(), MutationError> + Send + 'static,
    {
        // Queue asynchronously before reserving a blocking worker. The owned
        // guard must live inside the synchronous operation because
        // `spawn_blocking` continues after its awaiting request is canceled.
        let mutation_guard = self.gate.clone().lock_owned().await;
        self.executor
            .run(WorkClass::Light, move || {
                let _mutation_guard = mutation_guard;
                Ok(mutation())
            })
            .await
            .map_err(MutationError::Storage)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Condvar, Mutex},
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};

    fn mutations(executor: StorageExecutor) -> PricingMutations {
        let temp = tempfile::tempdir().unwrap();
        let database = Db::open(temp.path().join("usage.db")).unwrap();
        let store =
            ManualPricingStore::new(database.path().with_extension("pricing.json")).unwrap();
        store.hydrate(&database).unwrap();
        PricingMutations::new(database, store, executor)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mutations_queue_before_blocking_workers() {
        let mutations = mutations(StorageExecutor::new(3, 1));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let first = {
            let mutations = mutations.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                mutations
                    .run(move || {
                        first_started_tx.send(()).unwrap();
                        let (lock, ready) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = ready.wait(released).unwrap();
                        }
                        Ok(())
                    })
                    .await
            })
        };
        first_started_rx.await.unwrap();
        first.abort();

        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let second = {
            let mutations = mutations.clone();
            tokio::spawn(async move {
                mutations
                    .run(move || {
                        second_started_tx.send(()).unwrap();
                        Ok(())
                    })
                    .await
            })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut second_started_rx)
                .await
                .is_err(),
            "a queued mutation consumed a blocking worker before the active mutation completed"
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        assert!(first.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), &mut second_started_rx)
            .await
            .expect("queued mutation did not start after the active mutation finished")
            .unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mutations_use_the_light_worker_lane() {
        let executor = StorageExecutor::new(4, 2);
        let mutations = mutations(executor.clone());
        let heavy_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (heavy_started_tx, mut heavy_started_rx) = mpsc::unbounded_channel();
        let mut heavy_tasks = Vec::new();
        for _ in 0..2 {
            let executor = executor.clone();
            let heavy_gate = heavy_gate.clone();
            let heavy_started_tx = heavy_started_tx.clone();
            heavy_tasks.push(tokio::spawn(async move {
                executor
                    .run(WorkClass::Heavy, move || {
                        heavy_started_tx.send(()).unwrap();
                        let (lock, ready) = &*heavy_gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = ready.wait(released).unwrap();
                        }
                        Ok(())
                    })
                    .await
            }));
        }
        heavy_started_rx.recv().await.unwrap();
        heavy_started_rx.recv().await.unwrap();

        let (mutation_started_tx, mut mutation_started_rx) = oneshot::channel();
        let mutation = tokio::spawn(async move {
            mutations
                .run(move || {
                    mutation_started_tx.send(()).unwrap();
                    Ok(())
                })
                .await
        });
        let began_while_heavy_was_saturated =
            tokio::time::timeout(Duration::from_millis(250), &mut mutation_started_rx)
                .await
                .is_ok();

        let (lock, ready) = &*heavy_gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for task in heavy_tasks {
            task.await.unwrap().unwrap();
        }
        if !began_while_heavy_was_saturated {
            tokio::time::timeout(Duration::from_secs(2), &mut mutation_started_rx)
                .await
                .expect("mutation did not start after heavy workers were released")
                .unwrap();
        }
        mutation.await.unwrap().unwrap();

        assert!(
            began_while_heavy_was_saturated,
            "manual pricing mutations must use the light lane, not queue behind heavy work"
        );
    }
}
