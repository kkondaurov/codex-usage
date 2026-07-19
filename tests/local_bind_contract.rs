use std::process::Command;

#[test]
fn serve_rejects_non_loopback_before_creating_local_state() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("must-not-exist.db");
    let output = Command::new(env!("CARGO_BIN_EXE_codex-usage"))
        .args([
            "serve",
            "--host",
            "0.0.0.0",
            "--db",
            db_path.to_str().unwrap(),
            "--no-ingest",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr
            .contains("refusing non-loopback bind address 0.0.0.0: codex-usage is localhost-only"),
        "{stderr}"
    );
    assert!(
        !db_path.exists(),
        "bind validation must run before database initialization"
    );
}
