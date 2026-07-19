use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_codex-usage"))
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
