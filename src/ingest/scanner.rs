use super::{
    attempt::AttemptRecorder,
    coordinator::{IngestRoots, IngestScannerLease, scan_one_shot_with_lease},
};
use crate::storage::Db;
use anyhow::{Context, Result};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub struct ScannerHandle {
    cancelled: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ScannerHandle {
    pub fn request_stop(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn shutdown(mut self) {
        self.request_stop();
        self.reap_finished();
    }

    fn reap_finished(&mut self) {
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.is_finished())
            && let Some(worker) = self.worker.take()
        {
            let _ = worker.join();
        }
    }
}

impl Drop for ScannerHandle {
    fn drop(&mut self) {
        self.request_stop();
        // A scan may be inside a large source file or waiting on another
        // process's ingest lock. Never turn server shutdown into an unbounded
        // join; reap only a worker that has already completed and otherwise
        // let the process boundary terminate it after the graceful window.
        self.reap_finished();
    }
}

pub fn spawn_scanner(db: Db, roots: IngestRoots, interval: Duration) -> Result<ScannerHandle> {
    let lease = IngestScannerLease::acquire(&db).with_context(|| {
        format!(
            "failed to claim live ingest scanner ownership for {}",
            db.path().display()
        )
    })?;
    spawn_scanner_with_lease(db, roots, interval, lease)
}

/// Start a live scanner with ownership acquired earlier in startup.
///
/// This closes the handoff gap between synchronous projection recovery and
/// the background worker: no competing one-shot command can claim the
/// database during prewarming or scanner startup.
pub fn spawn_scanner_with_lease(
    db: Db,
    roots: IngestRoots,
    interval: Duration,
    lease: IngestScannerLease,
) -> Result<ScannerHandle> {
    lease.require_database(&db)?;
    // Hold the lifetime lease so another scanner or a one-shot ingest cannot
    // alternate a conflicting root configuration between observations.
    // Every successful live cycle uses the same bounded semantics as the CLI:
    // confirm a newly adopted root set and only then publish the completed
    // projector generation.
    let cancelled = Arc::new(AtomicBool::new(false));
    let stop = cancelled.clone();
    let worker = std::thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            if let Err(error) = scan_one_shot_with_lease(&db, &roots, &lease) {
                tracing::warn!(%error, "ingest scan failed");
                let _ = AttemptRecorder::new(&db).mark_cycle_failed();
            }
            let slices = (interval.as_millis() / 250).max(1) as usize;
            for _ in 0..slices {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    });
    Ok(ScannerHandle {
        cancelled,
        worker: Some(worker),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Condvar, Mutex, mpsc},
        time::Instant,
    };

    #[test]
    fn shutdown_requests_cancellation_without_joining_blocked_work() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = cancelled.clone();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let (lock, wake) = &*worker_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            done_tx
                .send(worker_cancelled.load(Ordering::Acquire))
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let handle = ScannerHandle {
            cancelled,
            worker: Some(worker),
        };

        let started = Instant::now();
        handle.shutdown();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "scanner shutdown joined blocked work"
        );

        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        assert!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }
}
