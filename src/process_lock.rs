use crate::db::Db;
use anyhow::{Context, Result};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

#[cfg(unix)]
use std::{
    os::fd::AsRawFd,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    thread,
    time::Instant,
};

#[cfg(unix)]
const INTERRUPTIBLE_LOCK_RETRY: Duration = Duration::from_millis(25);

/// An advisory lock shared by every process that operates on the same database.
///
/// The lock file is deliberately derived from the database path so independent
/// databases never block one another and every process that opens the same
/// projection converges on the same ownership boundary.
#[derive(Debug)]
pub(crate) struct DatabaseLock {
    file: File,
}

impl DatabaseLock {
    pub(crate) fn acquire(db: &Db, operation: &str) -> Result<Self> {
        Self::acquire_path(db.path(), operation)
    }

    pub(crate) fn acquire_path(database_path: &Path, operation: &str) -> Result<Self> {
        let (file, path) = open_lock_file(database_path, operation)?;
        lock_file(&file).with_context(|| format!("failed to lock {}", path.display()))?;
        Ok(Self { file })
    }

    /// Wait for a process lock without creating an uninterruptible blocking
    /// worker. The caller can cancel a background operation during shutdown,
    /// and the deadline prevents a live process from waiting forever on a
    /// stale or wedged peer.
    pub(crate) fn acquire_interruptible(
        db: &Db,
        operation: &str,
        timeout: Duration,
        cancelled: &AtomicBool,
    ) -> Result<Option<Self>> {
        Self::acquire_path_interruptible(db.path(), operation, timeout, cancelled)
    }

    pub(crate) fn acquire_path_interruptible(
        database_path: &Path,
        operation: &str,
        timeout: Duration,
        cancelled: &AtomicBool,
    ) -> Result<Option<Self>> {
        let (file, path) = open_lock_file(database_path, operation)?;
        if !lock_file_interruptible(&file, timeout, cancelled)
            .with_context(|| format!("failed to lock {}", path.display()))?
        {
            return Ok(None);
        }
        Ok(Some(Self { file }))
    }
}

fn open_lock_file(database_path: &Path, operation: &str) -> Result<(File, PathBuf)> {
    let path = lock_path_for_database(database_path, operation);
    if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    repair_private_permissions(&file, &path)?;
    Ok((file, path))
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        unlock_file(&self.file);
    }
}

#[cfg(test)]
fn lock_path(db: &Db, operation: &str) -> PathBuf {
    lock_path_for_database(db.path(), operation)
}

fn lock_path_for_database(database_path: &Path, operation: &str) -> PathBuf {
    let mut path = OsString::from(database_path.as_os_str());
    path.push(format!(".{operation}.lock"));
    PathBuf::from(path)
}

#[cfg(unix)]
fn lock_file(file: &File) -> Result<()> {
    loop {
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
}

#[cfg(unix)]
fn lock_file_interruptible(file: &File, timeout: Duration, cancelled: &AtomicBool) -> Result<bool> {
    let started = Instant::now();
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok(false);
        }
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        match error.kind() {
            ErrorKind::Interrupted => continue,
            ErrorKind::WouldBlock if started.elapsed() < timeout => {
                thread::sleep(
                    INTERRUPTIBLE_LOCK_RETRY.min(timeout.saturating_sub(started.elapsed())),
                );
            }
            ErrorKind::WouldBlock => {
                return Err(anyhow::anyhow!("timed out waiting for process lock"));
            }
            _ => return Err(error.into()),
        }
    }
}

#[cfg(not(unix))]
fn lock_file(_file: &File) -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn lock_file_interruptible(
    _file: &File,
    _timeout: Duration,
    cancelled: &AtomicBool,
) -> Result<bool> {
    Ok(!cancelled.load(Ordering::Acquire))
}

#[cfg(unix)]
fn unlock_file(file: &File) {
    // SAFETY: `file` still owns a valid descriptor while the guard is dropped.
    let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
}

#[cfg(not(unix))]
fn unlock_file(_file: &File) {}

#[cfg(unix)]
fn repair_private_permissions(file: &File, path: &Path) -> Result<()> {
    let mode = file.metadata()?.permissions().mode() & 0o777;
    if mode != 0o600 {
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn repair_private_permissions(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn lock_path_is_derived_from_the_database_path_and_operation() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let canonical_parent = fs::canonicalize(temp.path()).unwrap();
        assert_eq!(
            lock_path(&db, "ingest"),
            canonical_parent.join("usage.db.ingest.lock")
        );
        assert_eq!(
            lock_path(&db, "pricing-refresh"),
            canonical_parent.join("usage.db.pricing-refresh.lock")
        );
    }

    #[cfg(unix)]
    #[test]
    fn database_symlink_aliases_derive_the_same_process_lock() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("usage.db");
        let alias = temp.path().join("usage-alias.db");
        let direct = Db::open(&target).unwrap();
        symlink(&target, &alias).unwrap();
        let through_alias = Db::open(&alias).unwrap();

        assert_eq!(
            lock_path(&direct, "ingest"),
            lock_path(&through_alias, "ingest")
        );
    }

    #[cfg(unix)]
    #[test]
    fn interruptible_lock_wait_honors_cancellation() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let _guard = DatabaseLock::acquire(&db, "pricing-refresh").unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = cancelled.clone();
        let worker_db = db.clone();
        let waiter = std::thread::spawn(move || {
            DatabaseLock::acquire_interruptible(
                &worker_db,
                "pricing-refresh",
                Duration::from_secs(5),
                &worker_cancelled,
            )
        });

        std::thread::sleep(Duration::from_millis(75));
        cancelled.store(true, Ordering::Release);
        assert!(
            waiter.join().unwrap().unwrap().is_none(),
            "cancelled lock wait unexpectedly acquired ownership"
        );
    }

    #[cfg(unix)]
    #[test]
    fn interruptible_lock_wait_has_a_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let _guard = DatabaseLock::acquire(&db, "pricing-refresh").unwrap();
        let cancelled = AtomicBool::new(false);
        let started = Instant::now();
        let error = DatabaseLock::acquire_interruptible(
            &db,
            "pricing-refresh",
            Duration::from_millis(75),
            &cancelled,
        )
        .unwrap_err();

        assert!(error.to_string().contains("failed to lock"), "{error:#}");
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
