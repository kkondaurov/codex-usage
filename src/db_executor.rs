use anyhow::{Context, Result, anyhow};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Three analytical workers, one ordinary light worker, and one isolated
// control worker preserve the previous query concurrency while making the
// status lane a real part of the configured capacity.
const DEFAULT_TOTAL_PERMITS: usize = 5;
const DEFAULT_HEAVY_PERMITS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkClass {
    Control,
    Light,
    Heavy,
}

#[derive(Clone, Debug)]
pub struct DbExecutor {
    regular: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
    control: Arc<Semaphore>,
}

impl Default for DbExecutor {
    fn default() -> Self {
        Self::new(DEFAULT_TOTAL_PERMITS, DEFAULT_HEAVY_PERMITS)
    }
}

impl DbExecutor {
    pub fn new(total_permits: usize, heavy_permits: usize) -> Self {
        assert!(
            total_permits > 1,
            "the database executor needs regular and control worker permits"
        );
        let regular_permits = total_permits - 1;
        assert!(
            heavy_permits > 0 && heavy_permits < regular_permits,
            "heavy work must leave one regular permit for light work"
        );
        Self {
            regular: Arc::new(Semaphore::new(regular_permits)),
            heavy: Arc::new(Semaphore::new(heavy_permits)),
            control: Arc::new(Semaphore::new(1)),
        }
    }

    pub async fn run<T, F>(&self, class: WorkClass, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        // Heavy work takes its class permit first, so queued analytics cannot
        // reserve regular workers while waiting for the heavy limit. Control
        // work uses a separate lane and remains runnable even when every
        // regular permit is occupied by a blocked light task.
        let heavy_permit = match class {
            WorkClass::Control | WorkClass::Light => None,
            WorkClass::Heavy => Some(acquire(self.heavy.clone()).await?),
        };
        let work_permit = match class {
            WorkClass::Control => acquire(self.control.clone()).await?,
            WorkClass::Light | WorkClass::Heavy => acquire(self.regular.clone()).await?,
        };
        tokio::task::spawn_blocking(move || {
            let _heavy_permit = heavy_permit;
            let _work_permit = work_permit;
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
    async fn heavy_limit_leaves_regular_capacity_for_light_work() {
        let executor = DbExecutor::new(4, 2);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active_heavy = Arc::new(AtomicUsize::new(0));
        let (heavy_started_tx, mut heavy_started_rx) = mpsc::unbounded_channel();
        let mut heavy_tasks = Vec::new();

        for _ in 0..2 {
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
        for _ in 0..2 {
            heavy_started_rx.recv().await.unwrap();
        }
        assert_eq!(active_heavy.load(Ordering::SeqCst), 2);

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
        assert_eq!(active_heavy.load(Ordering::SeqCst), 2);

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for task in heavy_tasks {
            task.await.unwrap().unwrap();
        }
        fourth.await.unwrap().unwrap();
        light.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn control_work_has_a_dedicated_lane_when_light_work_is_saturated() {
        let executor = DbExecutor::new(3, 1);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let mut blocked = Vec::new();
        for _ in 0..2 {
            let executor = executor.clone();
            let gate = gate.clone();
            let active = active.clone();
            let started = started_tx.clone();
            blocked.push(tokio::spawn(async move {
                executor
                    .run(WorkClass::Light, move || {
                        active.fetch_add(1, Ordering::SeqCst);
                        started.send(()).unwrap();
                        let (lock, ready) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = ready.wait(released).unwrap();
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            }));
        }
        started_rx.recv().await.unwrap();
        started_rx.recv().await.unwrap();

        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let control = {
            let executor = executor.clone();
            let active = active.clone();
            tokio::spawn(async move {
                executor
                    .run(WorkClass::Control, move || {
                        let concurrent = active.fetch_add(1, Ordering::SeqCst) + 1;
                        control_tx.send(concurrent).unwrap();
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };
        let concurrent = tokio::time::timeout(std::time::Duration::from_secs(2), control_rx.recv())
            .await
            .expect("control work was starved by light work")
            .expect("control task exited without reporting concurrency");
        assert_eq!(
            concurrent, 3,
            "regular workers plus the control worker must fill, not exceed, total capacity"
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for task in blocked {
            task.await.unwrap().unwrap();
        }
        control.await.unwrap().unwrap();
    }
}
