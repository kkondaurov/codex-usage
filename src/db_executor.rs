use anyhow::{Context, Result, anyhow};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_TOTAL_PERMITS: usize = 4;
const DEFAULT_HEAVY_PERMITS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkClass {
    Light,
    Heavy,
}

#[derive(Clone, Debug)]
pub struct DbExecutor {
    total: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
}

impl Default for DbExecutor {
    fn default() -> Self {
        Self::new(DEFAULT_TOTAL_PERMITS, DEFAULT_HEAVY_PERMITS)
    }
}

impl DbExecutor {
    pub fn new(total_permits: usize, heavy_permits: usize) -> Self {
        assert!(
            total_permits > 0,
            "the database executor needs a worker permit"
        );
        assert!(
            heavy_permits > 0 && heavy_permits < total_permits,
            "heavy work must leave at least one permit for control traffic"
        );
        Self {
            total: Arc::new(Semaphore::new(total_permits)),
            heavy: Arc::new(Semaphore::new(heavy_permits)),
        }
    }

    pub async fn run<T, F>(&self, class: WorkClass, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        // Heavy work takes its class permit first. A queue of analytical
        // requests therefore cannot reserve every total permit while waiting
        // for the heavy limit, leaving a lane available for status/control IO.
        let heavy_permit = match class {
            WorkClass::Light => None,
            WorkClass::Heavy => Some(acquire(self.heavy.clone()).await?),
        };
        let total_permit = acquire(self.total.clone()).await?;
        tokio::task::spawn_blocking(move || {
            let _heavy_permit = heavy_permit;
            let _total_permit = total_permit;
            work()
        })
        .await
        .context("database worker task failed")?
    }
}

async fn acquire(semaphore: Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    semaphore
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("database executor is closed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::mpsc;

    #[tokio::test(flavor = "current_thread")]
    async fn work_runs_off_the_async_runtime_thread() {
        let runtime_thread = std::thread::current().id();
        let worker_thread = DbExecutor::default()
            .run(WorkClass::Light, || Ok(std::thread::current().id()))
            .await
            .unwrap();
        assert_ne!(worker_thread, runtime_thread);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heavy_work_leaves_a_lane_for_light_control_work() {
        let executor = DbExecutor::new(4, 3);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active_heavy = Arc::new(AtomicUsize::new(0));
        let (heavy_started_tx, mut heavy_started_rx) = mpsc::unbounded_channel();
        let mut heavy_tasks = Vec::new();

        for _ in 0..3 {
            let executor = executor.clone();
            let gate = gate.clone();
            let active_heavy = active_heavy.clone();
            let started = heavy_started_tx.clone();
            heavy_tasks.push(tokio::spawn(async move {
                executor
                    .run(WorkClass::Heavy, move || {
                        active_heavy.fetch_add(1, Ordering::SeqCst);
                        started.send(()).unwrap();
                        let (lock, ready) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = ready.wait(released).unwrap();
                        }
                        active_heavy.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            }));
        }
        for _ in 0..3 {
            heavy_started_rx.recv().await.unwrap();
        }
        assert_eq!(active_heavy.load(Ordering::SeqCst), 3);

        let fourth = {
            let executor = executor.clone();
            let active_heavy = active_heavy.clone();
            let started = heavy_started_tx.clone();
            tokio::spawn(async move {
                executor
                    .run(WorkClass::Heavy, move || {
                        active_heavy.fetch_add(1, Ordering::SeqCst);
                        started.send(()).unwrap();
                        active_heavy.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };
        let (light_started_tx, mut light_started_rx) = mpsc::unbounded_channel();
        let light = {
            let executor = executor.clone();
            tokio::spawn(async move {
                executor
                    .run(WorkClass::Light, move || {
                        light_started_tx.send(()).unwrap();
                        Ok(())
                    })
                    .await
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(2), light_started_rx.recv())
            .await
            .expect("light work was starved by heavy work")
            .unwrap();
        assert_eq!(active_heavy.load(Ordering::SeqCst), 3);

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for task in heavy_tasks {
            task.await.unwrap().unwrap();
        }
        fourth.await.unwrap().unwrap();
        light.await.unwrap().unwrap();
    }
}
