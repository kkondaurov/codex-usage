use crate::db::Db;
use anyhow::{Context, Result};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::ErrorKind,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::{
    os::fd::AsRawFd,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
};

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
        let path = lock_path(db, operation);
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
        lock_file(&file).with_context(|| format!("failed to lock {}", path.display()))?;
        Ok(Self { file })
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        unlock_file(&self.file);
    }
}

fn lock_path(db: &Db, operation: &str) -> PathBuf {
    let mut path = OsString::from(db.path().as_os_str());
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

#[cfg(not(unix))]
fn lock_file(_file: &File) -> Result<()> {
    Ok(())
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
}
