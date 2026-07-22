use std::{
    ffi::OsString,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
};

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_codex-usage"))
}

fn appended_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(suffix);
    PathBuf::from(value)
}

#[test]
fn omitted_subcommand_honors_serve_environment_validation() {
    for arguments in [Vec::<&str>::new(), vec!["serve"]] {
        let output = binary()
            .args(arguments)
            .env_clear()
            .env("CODEX_USAGE_PRICING_REFRESH_HOURS", "not-a-number")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("not-a-number"), "{stderr}");
        assert!(stderr.contains("pricing-refresh-hours"), "{stderr}");
    }
}

#[test]
fn startup_outside_the_repository_root_fails_before_creating_state() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("must-not-exist.sqlite3");
    let output = binary()
        .args(["serve", "--db"])
        .arg(&db)
        .env_clear()
        .current_dir(temp.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("must be run from its repository root"),
        "{stderr}"
    );
    assert!(!db.exists(), "startup must not create a database");
}

#[test]
fn serve_requires_the_frontend_build_before_creating_state() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("must-not-exist.sqlite3");
    let missing_frontend = temp.path().join("missing-frontend");
    let output = binary()
        .args(["serve", "--db"])
        .arg(&db)
        .arg("--frontend")
        .arg(&missing_frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("frontend build not found"), "{stderr}");
    assert!(!db.exists(), "startup must not create a database");
}

#[test]
fn occupied_serve_port_fails_before_creating_database_or_worker_state() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("must-not-exist.sqlite3");
    let pricing = temp.path().join("must-not-exist.pricing.json");
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();

    let output = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port)
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--frontend")
        .arg(&frontend)
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed to bind"), "{stderr}");
    for path in [
        db.clone(),
        appended_path(&db, "-wal"),
        appended_path(&db, "-shm"),
        appended_path(&db, ".ingest.lock"),
        appended_path(&db, ".pricing-refresh.lock"),
        pricing.clone(),
        appended_path(&pricing, ".lock"),
    ] {
        assert!(
            !path.exists(),
            "occupied-port startup created {}",
            path.display()
        );
    }
}
