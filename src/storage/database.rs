use super::migrations::{ensure_runtime_indexes, migrate, reject_future_schema, seed_pricing};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

const DB_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
const WAL_RETRY_MAX_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct DatabaseLocation {
    path: PathBuf,
}

impl DatabaseLocation {
    pub fn prepare(path: impl AsRef<Path>) -> Result<Self> {
        let path = canonicalize_storage_path(path.as_ref())?;
        preflight_database_path(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn open(self) -> Result<Db> {
        // Repeat the non-mutating checks immediately before SQLite is opened.
        // A hard link or newer schema may have appeared after `prepare`.
        preflight_database_path(&self.path)?;
        let db = Db { path: self.path };
        let connection = db.connect()?;
        restrict_private_file(&db.path)?;
        enable_wal(&connection)?;
        migrate(&connection)?;
        ensure_runtime_indexes(&connection)?;
        seed_pricing(&connection)?;
        restrict_sqlite_sidecars(&db.path)?;
        drop(connection);
        Ok(db)
    }
}

#[derive(Clone, Debug)]
pub struct Db {
    path: PathBuf,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        DatabaseLocation::prepare(path)?.open()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn storage_bytes(&self) -> u64 {
        [
            self.path.clone(),
            sqlite_sidecar_path(&self.path, "-wal"),
            sqlite_sidecar_path(&self.path, "-shm"),
        ]
        .into_iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .fold(0_u64, |total, metadata| {
            total.saturating_add(metadata.len())
        })
    }

    pub fn connect(&self) -> Result<Connection> {
        let connection = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open {}", self.path.display()))?;
        connection.busy_timeout(DB_BUSY_TIMEOUT)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        restrict_private_file(&self.path)?;
        restrict_sqlite_sidecars(&self.path)?;
        Ok(connection)
    }

    pub fn read_snapshot<T>(&self, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let connection = self.connect()?;
        let transaction = Transaction::new_unchecked(&connection, TransactionBehavior::Deferred)?;
        let value = read(&transaction)?;
        transaction.commit()?;
        Ok(value)
    }
}

fn preflight_database_path(path: &Path) -> Result<()> {
    reject_multiply_linked_storage(
        path,
        "SQLite locks and WAL sidecars require one path identity",
    )?;
    // Probe an existing database read-only before WAL setup, permission
    // repair, migrations, or seeding can mutate it. An older binary must never
    // try to reinterpret a newer schema.
    if path.exists() {
        let probe = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to inspect {}", path.display()))?;
        reject_future_schema(&probe)?;
    }
    Ok(())
}

/// Resolve a storage file to one stable absolute identity before locks,
/// sidecars, or atomic replacement paths are derived from it.
///
/// Existing files (including symlinks) resolve to their real target. For a
/// file that does not exist yet, canonicalize its created parent and append
/// the original file name. This gives aliases of the same target one lock and
/// prevents an atomic rename from replacing a symlink itself.
pub(crate) fn canonicalize_storage_path(path: &Path) -> Result<PathBuf> {
    if path.exists() || std::fs::symlink_metadata(path).is_ok() {
        return std::fs::canonicalize(path)
            .with_context(|| format!("failed to resolve {}", path.display()));
    }

    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("storage path has no file name: {}", path.display()))?;
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    let parent = std::fs::canonicalize(parent)
        .with_context(|| format!("failed to resolve {}", parent.display()))?;
    Ok(parent.join(file_name))
}

#[cfg(unix)]
pub(crate) fn reject_multiply_linked_storage(path: &Path, reason: &str) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let links = match std::fs::metadata(path) {
        Ok(metadata) => metadata.nlink(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    if links > 1 {
        bail!(
            "refusing multiply-linked storage file {}: {reason}",
            path.display(),
        );
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn reject_multiply_linked_storage(_path: &Path, _reason: &str) -> Result<()> {
    Ok(())
}

fn enable_wal(connection: &Connection) -> Result<()> {
    let started = Instant::now();
    let mut retry_delay = Duration::from_millis(1);
    loop {
        match connection
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(mode) if started.elapsed() >= DB_BUSY_TIMEOUT => {
                bail!("failed to enable SQLite WAL mode: pragma returned {mode}")
            }
            Ok(_) => {
                thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(WAL_RETRY_MAX_DELAY);
            }
            Err(error) if is_sqlite_lock_error(&error) && started.elapsed() < DB_BUSY_TIMEOUT => {
                thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(WAL_RETRY_MAX_DELAY);
            }
            Err(error) => return Err(error).context("failed to enable SQLite WAL mode"),
        }
    }
}

fn is_sqlite_lock_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(sqlite_error, _)
            if matches!(
                sqlite_error.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

#[cfg(unix)]
fn restrict_private_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?
        .permissions();
    if permissions.mode() & 0o777 != 0o600 {
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions)
            .with_context(|| format!("failed to restrict {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_private_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn restrict_sqlite_sidecars(path: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = sqlite_sidecar_path(path, suffix);
        if sidecar.exists() {
            restrict_private_file(&sidecar)?;
        }
    }
    Ok(())
}

pub(super) fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = OsString::from(path.as_os_str());
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn sqlite_database_and_sidecars_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("private-ledger.sqlite3");
        let db = Db::open(&path).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE private_mode_probe(value INTEGER NOT NULL);
                 INSERT INTO private_mode_probe(value) VALUES(1);",
            )
            .unwrap();

        for private_path in [
            path.clone(),
            sqlite_sidecar_path(&path, "-wal"),
            sqlite_sidecar_path(&path, "-shm"),
        ] {
            assert!(private_path.exists(), "{} exists", private_path.display());
            let mode = std::fs::metadata(&private_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "{} must be owner-only", private_path.display());
        }
    }

    #[test]
    fn sqlite_sidecar_paths_append_to_the_complete_database_name() {
        let path = Path::new("data/private-ledger.sqlite3");
        assert_eq!(
            sqlite_sidecar_path(path, "-wal"),
            PathBuf::from("data/private-ledger.sqlite3-wal")
        );
        assert_eq!(
            sqlite_sidecar_path(path, "-shm"),
            PathBuf::from("data/private-ledger.sqlite3-shm")
        );
    }

    #[test]
    fn storage_bytes_include_wal_and_shared_memory_sidecars() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("usage.db");
        let db = Db::open(&path).unwrap();
        let main_bytes = std::fs::metadata(&path).unwrap().len();
        std::fs::write(sqlite_sidecar_path(&path, "-wal"), [0_u8; 7]).unwrap();
        std::fs::write(sqlite_sidecar_path(&path, "-shm"), [0_u8; 11]).unwrap();

        assert_eq!(db.storage_bytes(), main_bytes + 18);
    }

    #[test]
    fn nonexistent_storage_path_uses_canonical_parent_identity() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("new").join("projection.db");
        let expected_parent = std::fs::canonicalize(temp.path()).unwrap().join("new");

        let db = Db::open(&nested).unwrap();

        assert_eq!(db.path(), expected_parent.join("projection.db"));
    }

    #[cfg(unix)]
    #[test]
    fn database_symlink_alias_resolves_to_the_target_identity() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("real.db");
        let alias = temp.path().join("alias.db");
        let direct = Db::open(&target).unwrap();
        symlink(&target, &alias).unwrap();

        let through_alias = Db::open(&alias).unwrap();

        assert_eq!(through_alias.path(), direct.path());
        assert!(
            std::fs::symlink_metadata(alias)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn database_hardlink_alias_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("real.db");
        let alias = temp.path().join("alias.db");
        drop(Db::open(&target).unwrap());
        std::fs::hard_link(&target, &alias).unwrap();

        let error = Db::open(&alias).unwrap_err().to_string();
        assert!(error.contains("multiply-linked storage file"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn prepared_location_rechecks_hardlinks_before_open() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("prepared.db");
        let alias = temp.path().join("late-alias.db");
        let location = DatabaseLocation::prepare(&target).unwrap();
        std::fs::write(&target, []).unwrap();
        std::fs::hard_link(&target, &alias).unwrap();

        let error = location.open().unwrap_err().to_string();

        assert!(error.contains("multiply-linked storage file"), "{error}");
        assert_eq!(std::fs::metadata(&target).unwrap().len(), 0);
        assert!(!sqlite_sidecar_path(&target, "-wal").exists());
        assert!(!sqlite_sidecar_path(&target, "-shm").exists());
    }

    #[cfg(unix)]
    #[test]
    fn reopening_restricts_an_existing_database() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("usage.db");
        drop(Db::open(&path).unwrap());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        drop(Db::open(&path).unwrap());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn read_snapshot_keeps_one_version_across_a_concurrent_wal_commit() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("snapshot.db")).unwrap();
        db.connect()
            .unwrap()
            .execute_batch(
                "CREATE TABLE snapshot_probe(value INTEGER NOT NULL);
                 INSERT INTO snapshot_probe(value) VALUES(1);",
            )
            .unwrap();

        let writer_db = db.clone();
        db.read_snapshot(|connection| {
            let first: i64 =
                connection.query_row("SELECT value FROM snapshot_probe", [], |row| row.get(0))?;
            let writer = std::thread::spawn(move || {
                writer_db
                    .connect()
                    .unwrap()
                    .execute("UPDATE snapshot_probe SET value=2", [])
                    .unwrap();
            });
            writer.join().unwrap();
            let second: i64 =
                connection.query_row("SELECT value FROM snapshot_probe", [], |row| row.get(0))?;
            assert_eq!((first, second), (1, 1));
            Ok(())
        })
        .unwrap();
        let current: i64 = db
            .connect()
            .unwrap()
            .query_row("SELECT value FROM snapshot_probe", [], |row| row.get(0))
            .unwrap();
        assert_eq!(current, 2);
    }
}
