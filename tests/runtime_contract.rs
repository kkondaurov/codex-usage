#[cfg(unix)]
use codex_usage::db::Db;
use std::{
    ffi::OsString,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
};
#[cfg(unix)]
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    net::TcpStream,
    os::fd::AsRawFd,
    process::{Child, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
struct ChildGuard(Option<Child>);

#[cfg(unix)]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(unix)]
fn server_responds(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let timeout = Some(Duration::from_millis(250));
    if stream.set_read_timeout(timeout).is_err() || stream.set_write_timeout(timeout).is_err() {
        return false;
    }
    let request = format!(
        "GET /api/v1/status HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = [0_u8; 128];
    let Ok(read) = stream.read(&mut response) else {
        return false;
    };
    response[..read].starts_with(b"HTTP/1.1 200")
}

#[cfg(unix)]
fn begin_manual_price_request(port: u16) -> TcpStream {
    let body = r#"{"inputPerMillion":"1","outputPerMillion":"1","currency":"USD"}"#;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(
            format!(
                "PUT /api/v1/prices/shutdown-test HTTP/1.1\r\n\
                 Host: 127.0.0.1:{port}\r\n\
                 Origin: http://127.0.0.1:{port}\r\n\
                 Sec-Fetch-Site: same-origin\r\n\
                 Content-Type: application/json\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .as_bytes(),
        )
        .unwrap();
    stream
}

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_codex-usage"))
}

fn appended_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
#[test]
fn ingesting_commands_claim_scanner_lease_before_database_hydration() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("usage.sqlite3");
    let primary_pricing = db_path.with_extension("pricing.json");
    let sessions = temp.path().join("sessions");
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&sessions).unwrap();
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    std::fs::write(
        &primary_pricing,
        r#"{
  "version": 1,
  "prices": [{
    "modelId": "owner-price",
    "effectiveFrom": "1970-01-01T00:00:00.000000000Z",
    "effectiveTo": null,
    "inputMicrousdPerMillion": 123000,
    "cachedInputMicrousdPerMillion": null,
    "outputMicrousdPerMillion": 456000
  }],
  "aliases": []
}"#,
    )
    .unwrap();
    let pricing_url = "http://127.0.0.1:9/prices.json";
    let db = Db::open(&db_path).unwrap();
    db.connect()
        .unwrap()
        .execute_batch(&format!(
            "INSERT OR REPLACE INTO app_meta(key,value)
             VALUES('ingest_state','scanning');
             INSERT OR REPLACE INTO app_meta(key,value)
             VALUES('pricing_source_url','{pricing_url}');
             INSERT OR REPLACE INTO app_meta(key,value)
             VALUES('pricing_last_refresh_at','{}');",
            chrono::Utc::now().to_rfc3339()
        ))
        .unwrap();
    let primary_pricing_before = std::fs::read(&primary_pricing).unwrap();

    let lock_path = appended_path(&db_path, ".ingest-scanner.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .unwrap();
    // SAFETY: `lock` owns a valid descriptor for this test's duration.
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);

    for command_name in ["ingest", "serve"] {
        let alternate_pricing = temp
            .path()
            .join(format!("losing-{command_name}.pricing.json"));
        let alternate_lock = temp
            .path()
            .join(format!(".losing-{command_name}.pricing.json.lock"));
        let mut command = binary();
        command.arg(command_name);
        if command_name == "serve" {
            let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = reservation.local_addr().unwrap().port().to_string();
            drop(reservation);
            command
                .args(["--host", "127.0.0.1", "--port"])
                .arg(port)
                .arg("--frontend")
                .arg(&frontend);
        }
        let output = command
            .arg("--db")
            .arg(&db_path)
            .arg("--sessions")
            .arg(&sessions)
            .arg("--pricing-config")
            .arg(&alternate_pricing)
            .arg("--pricing-url")
            .arg(pricing_url)
            .env_clear()
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("live ingest scanner"), "{stderr}");

        let connection = db.connect().unwrap();
        let ingest_state: String = connection
            .query_row(
                "SELECT value FROM app_meta WHERE key='ingest_state'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            ingest_state, "scanning",
            "the losing {command_name} mutated recovery state before checking ownership"
        );
        let manual_price: (i64, i64) = connection
            .query_row(
                "SELECT input_microusd_per_million, output_microusd_per_million
                 FROM model_prices
                 WHERE source='manual' AND model_id='owner-price'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            manual_price,
            (123_000, 456_000),
            "the losing {command_name} hydrated its alternate pricing state into SQLite"
        );
        assert_eq!(
            std::fs::read(&primary_pricing).unwrap(),
            primary_pricing_before,
            "the losing {command_name} changed the owner's pricing sidecar"
        );
        assert!(
            !alternate_pricing.exists(),
            "the losing {command_name} created alternate pricing state"
        );
        assert!(
            !alternate_lock.exists(),
            "the losing {command_name} created an alternate pricing lock"
        );
    }
    drop(lock);
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

#[cfg(unix)]
#[test]
fn sigterm_requests_a_clean_server_shutdown() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let temp = tempfile::tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    let db = temp.path().join("usage.sqlite3");
    let pricing = temp.path().join("usage.pricing.json");

    let child = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));

    let ready_by = Instant::now() + Duration::from_secs(10);
    loop {
        // A TCP connection can complete as soon as the socket is bound, before
        // Axum has polled and installed its graceful-shutdown signal future.
        // Wait for an actual response so SIGTERM tests the running server.
        if server_responds(port) {
            break;
        }
        assert!(
            child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "server exited before accepting connections"
        );
        assert!(Instant::now() < ready_by, "server did not become ready");
        thread::sleep(Duration::from_millis(20));
    }

    let pid = child.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let stopped_by = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < stopped_by, "server ignored SIGTERM");
        thread::sleep(Duration::from_millis(20));
    };
    child.0.take();
    assert!(
        status.success(),
        "server exited uncleanly after SIGTERM: {status}"
    );
}

#[cfg(unix)]
#[test]
fn sigterm_stops_server_while_pricing_refresh_lock_is_contended() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let temp = tempfile::tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    let db = temp.path().join("usage.sqlite3");
    let pricing = temp.path().join("usage.pricing.json");
    let lock_path = appended_path(&db, ".pricing-refresh.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    // SAFETY: `lock` owns a valid descriptor for this test's duration.
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);

    let child = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));

    let ready_by = Instant::now() + Duration::from_secs(10);
    loop {
        if server_responds(port) {
            break;
        }
        assert!(
            child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "server exited before accepting connections"
        );
        assert!(Instant::now() < ready_by, "server did not become ready");
        thread::sleep(Duration::from_millis(20));
    }
    // The refresher starts before Axum and must now be waiting on the lock
    // retained by this process.
    thread::sleep(Duration::from_millis(200));

    let pid = child.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let stopped_by = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < stopped_by,
            "server shutdown hung on the pricing refresh lock"
        );
        thread::sleep(Duration::from_millis(20));
    };
    child.0.take();
    assert!(
        status.success(),
        "server exited uncleanly after contended SIGTERM: {status}"
    );
    drop(lock);
}

#[cfg(unix)]
#[test]
fn sigterm_forces_shutdown_while_manual_pricing_file_lock_is_contended() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let temp = tempfile::tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    let db = temp.path().join("usage.sqlite3");
    let pricing = temp.path().join("usage.pricing.json");

    let child = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));

    let ready_by = Instant::now() + Duration::from_secs(10);
    while !server_responds(port) {
        assert!(Instant::now() < ready_by, "server did not become ready");
        thread::sleep(Duration::from_millis(20));
    }

    let lock_path = temp.path().join(".usage.pricing.json.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    // SAFETY: `lock` owns a valid descriptor for this test's duration.
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);
    let request = begin_manual_price_request(port);
    thread::sleep(Duration::from_millis(200));

    let pid = child.0.as_ref().unwrap().id() as libc::pid_t;
    let shutdown_started = Instant::now();
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let stopped_by = shutdown_started + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < stopped_by,
            "manual pricing file lock held shutdown open past its deadline"
        );
        thread::sleep(Duration::from_millis(20));
    };
    child.0.take();
    assert!(status.success(), "server exited uncleanly: {status}");
    drop(request);
    drop(lock);
}

#[cfg(unix)]
#[test]
fn sigterm_forces_shutdown_while_manual_pricing_waits_on_sqlite() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let temp = tempfile::tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    let db = temp.path().join("usage.sqlite3");
    let pricing = temp.path().join("usage.pricing.json");

    let child = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));

    let ready_by = Instant::now() + Duration::from_secs(10);
    while !server_responds(port) {
        assert!(Instant::now() < ready_by, "server did not become ready");
        thread::sleep(Duration::from_millis(20));
    }

    let database_lock = rusqlite::Connection::open(&db).unwrap();
    database_lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let request = begin_manual_price_request(port);
    thread::sleep(Duration::from_millis(200));

    let pid = child.0.as_ref().unwrap().id() as libc::pid_t;
    let shutdown_started = Instant::now();
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let stopped_by = shutdown_started + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < stopped_by,
            "SQLite writer lock held shutdown open past its deadline"
        );
        thread::sleep(Duration::from_millis(20));
    };
    child.0.take();
    assert!(status.success(), "server exited uncleanly: {status}");
    drop(request);
    database_lock.execute_batch("ROLLBACK").unwrap();
}

#[cfg(unix)]
#[test]
fn sigterm_forces_shutdown_after_incomplete_http_headers() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let temp = tempfile::tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    let db = temp.path().join("usage.sqlite3");
    let pricing = temp.path().join("usage.pricing.json");

    let child = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));

    let ready_by = Instant::now() + Duration::from_secs(10);
    loop {
        if server_responds(port) {
            break;
        }
        assert!(
            child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "server exited before accepting connections"
        );
        assert!(Instant::now() < ready_by, "server did not become ready");
        thread::sleep(Duration::from_millis(20));
    }

    // Keep a connection alive after sending a syntactically incomplete
    // request. Hyper cannot gracefully finish this connection until the peer
    // sends the terminating CRLF, so it exercises the forced-drain deadline.
    let mut incomplete = TcpStream::connect(("127.0.0.1", port)).unwrap();
    incomplete
        .write_all(format!("GET /api/v1/status HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n").as_bytes())
        .unwrap();
    thread::sleep(Duration::from_millis(100));

    let pid = child.0.as_ref().unwrap().id() as libc::pid_t;
    let shutdown_started = Instant::now();
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let stopped_by = shutdown_started + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < stopped_by,
            "incomplete HTTP headers held shutdown open past its deadline"
        );
        thread::sleep(Duration::from_millis(20));
    };
    child.0.take();
    assert!(
        shutdown_started.elapsed() >= Duration::from_millis(900),
        "test connection did not survive long enough to exercise the graceful deadline"
    );
    assert!(
        status.success(),
        "server exited uncleanly after forced graceful-shutdown deadline: {status}"
    );
    drop(incomplete);
}

#[cfg(unix)]
#[test]
fn live_ingest_publishes_projection_for_follow_up_no_ingest_start() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let temp = tempfile::tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    std::fs::create_dir(&frontend).unwrap();
    std::fs::write(frontend.join("index.html"), "<!doctype html>").unwrap();
    let sessions = temp.path().join("sessions");
    std::fs::create_dir(&sessions).unwrap();
    let owner = "019f64aa-0000-7000-8000-000000000026";
    std::fs::write(
        sessions.join("root.jsonl"),
        format!(
            "{{\"timestamp\":\"2026-07-15T09:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{owner}\",\"session_id\":\"{owner}\",\"cwd\":\"/tmp/live-generation\",\"source\":\"cli\"}}}}\n"
        ),
    )
    .unwrap();
    let db = temp.path().join("usage.sqlite3");
    let pricing = temp.path().join("usage.pricing.json");
    let pricing_url = "http://127.0.0.1:9/prices.json";

    let first = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--pricing-url")
        .arg(pricing_url)
        .arg("--pricing-timeout-seconds")
        .arg("1")
        .arg("--sessions")
        .arg(&sessions)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--poll-seconds")
        .arg("60")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut first = ChildGuard(Some(first));

    let published_by = Instant::now() + Duration::from_secs(10);
    loop {
        let published = rusqlite::Connection::open_with_flags(
            &db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .and_then(|connection| {
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                        (SELECT value FROM app_meta WHERE key='projector_generation')",
                [owner],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, Option<String>>(1)?)),
            )
        })
        .is_ok_and(|(source_exists, generation)| {
            source_exists && generation.as_deref() == Some("1")
        });
        if published && server_responds(port) {
            break;
        }
        assert!(
            first.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "live-ingest server exited before publishing its projection"
        );
        assert!(
            Instant::now() < published_by,
            "live scanner did not publish its projector generation"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let first_pid = first.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(first_pid, libc::SIGTERM) }, 0);
    let first_stopped_by = Instant::now() + Duration::from_secs(10);
    let first_status = loop {
        if let Some(status) = first.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < first_stopped_by,
            "live-ingest server ignored SIGTERM"
        );
        thread::sleep(Duration::from_millis(20));
    };
    first.0.take();
    assert!(first_status.success(), "live-ingest shutdown was unclean");

    let second = binary()
        .args(["serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--db")
        .arg(&db)
        .arg("--pricing-config")
        .arg(&pricing)
        .arg("--pricing-url")
        .arg(pricing_url)
        .arg("--pricing-timeout-seconds")
        .arg("1")
        .arg("--sessions")
        .arg(&sessions)
        .arg("--frontend")
        .arg(&frontend)
        .arg("--no-ingest")
        .env_clear()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut second = ChildGuard(Some(second));

    let ready_by = Instant::now() + Duration::from_secs(10);
    while !server_responds(port) {
        assert!(
            second.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "--no-ingest rejected the completed live projection"
        );
        assert!(
            Instant::now() < ready_by,
            "--no-ingest server did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let second_pid = second.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(second_pid, libc::SIGTERM) }, 0);
    let second_stopped_by = Instant::now() + Duration::from_secs(10);
    let second_status = loop {
        if let Some(status) = second.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < second_stopped_by,
            "--no-ingest server ignored SIGTERM"
        );
        thread::sleep(Duration::from_millis(20));
    };
    second.0.take();
    assert!(second_status.success(), "--no-ingest shutdown was unclean");
}
