#![cfg(target_os = "macos")]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const SERVICE_SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/codex-usage-service");

struct Harness {
    _temp: tempfile::TempDir,
    home: PathBuf,
    launch_agents: PathBuf,
    logs: PathBuf,
    working_directory: PathBuf,
    program: PathBuf,
    launchctl: PathBuf,
    curl: PathBuf,
    launchctl_state: PathBuf,
    launchctl_log: PathBuf,
    label: &'static str,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home with spaces & symbols");
        let launch_agents = home.join("Library/LaunchAgents");
        let logs = home.join("Library/Logs");
        let working_directory = temp.path().join("checkout with spaces & symbols");
        let program = temp.path().join("fake codex usage");
        let launchctl = temp.path().join("fake-launchctl");
        let curl = temp.path().join("fake-curl");
        let launchctl_state = temp.path().join("launchctl.state");
        let launchctl_log = temp.path().join("launchctl.log");
        fs::create_dir_all(&working_directory).unwrap();
        write_executable(&program, "#!/bin/sh\nexit 0\n");
        write_executable(
            &launchctl,
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_LAUNCHCTL_LOG"
case "$1" in
  print)
    if [ -f "$FAKE_LAUNCHCTL_STATE" ]; then
      printf 'state = running\npid = 4242\n'
      exit 0
    fi
    exit 113
    ;;
  bootstrap)
    : > "$FAKE_LAUNCHCTL_STATE"
    ;;
  enable)
    ;;
  kickstart)
    [ -f "$FAKE_LAUNCHCTL_STATE" ]
    ;;
  bootout)
    if [ "${FAKE_LAUNCHCTL_FAIL_BOOTOUT:-0}" = 1 ]; then
      exit 1
    fi
    rm -f "$FAKE_LAUNCHCTL_STATE"
    ;;
  *)
    exit 64
    ;;
esac
"#,
        );
        write_executable(&curl, "#!/bin/sh\nexit 0\n");
        Self {
            _temp: temp,
            home,
            launch_agents,
            logs,
            working_directory,
            program,
            launchctl,
            curl,
            launchctl_state,
            launchctl_log,
            label: "com.kkondaurov.codex-usage.test",
        }
    }

    fn command(&self, operation: &str) -> Command {
        let mut command = Command::new(SERVICE_SCRIPT);
        command
            .arg(operation)
            .env("HOME", &self.home)
            .env("CODEX_USAGE_LAUNCHCTL", &self.launchctl)
            .env("CODEX_USAGE_CURL", &self.curl)
            .env("CODEX_USAGE_SERVICE_LABEL", self.label)
            .env("CODEX_USAGE_LAUNCH_AGENTS_DIR", &self.launch_agents)
            .env("CODEX_USAGE_SERVICE_LOG_DIR", &self.logs)
            .env("CODEX_USAGE_SERVICE_PROGRAM", &self.program)
            .env(
                "CODEX_USAGE_SERVICE_WORKING_DIRECTORY",
                &self.working_directory,
            )
            .env("CODEX_USAGE_SERVICE_SKIP_BUILD", "1")
            .env("FAKE_LAUNCHCTL_STATE", &self.launchctl_state)
            .env("FAKE_LAUNCHCTL_LOG", &self.launchctl_log);
        command
    }

    fn run(&self, operation: &str) -> Output {
        self.command(operation).output().unwrap()
    }

    fn plist(&self) -> PathBuf {
        self.launch_agents.join(format!("{}.plist", self.label))
    }
}

#[test]
fn install_stop_start_status_and_uninstall_form_one_idempotent_lifecycle() {
    let harness = Harness::new();
    let installed = harness.run("install");
    assert_success(&installed);
    assert!(harness.plist().is_file());
    assert!(harness.launchctl_state.is_file());
    assert_eq!(
        plist_value(&harness.plist(), "ProgramArguments.0"),
        harness.program.to_string_lossy()
    );
    assert_eq!(
        plist_value(&harness.plist(), "WorkingDirectory"),
        harness.working_directory.to_string_lossy()
    );
    assert_eq!(
        plist_value(&harness.plist(), "StandardOutPath"),
        harness.logs.join("codex-usage.log").to_string_lossy()
    );
    assert_eq!(plist_value(&harness.plist(), "RunAtLoad"), "true");
    assert_eq!(plist_value(&harness.plist(), "KeepAlive"), "true");

    let running = harness.run("status");
    assert_success(&running);
    assert!(String::from_utf8_lossy(&running.stdout).contains("is running"));

    let stopped = harness.run("stop");
    assert_success(&stopped);
    assert!(!harness.launchctl_state.exists());
    assert!(harness.plist().exists());
    assert_success(&harness.run("stop"));
    let stopped_status = harness.run("status");
    assert!(!stopped_status.status.success());
    assert!(String::from_utf8_lossy(&stopped_status.stdout).contains("installed but stopped"));

    assert_success(&harness.run("start"));
    assert!(harness.launchctl_state.exists());
    assert_success(&harness.run("uninstall"));
    assert!(!harness.launchctl_state.exists());
    assert!(!harness.plist().exists());
    assert_success(&harness.run("uninstall"));

    let launchctl_log = fs::read_to_string(&harness.launchctl_log).unwrap();
    assert!(launchctl_log.contains("bootstrap gui/"));
    assert!(launchctl_log.contains("bootout gui/"));
}

#[test]
fn uninstall_keeps_the_plist_when_the_loaded_job_cannot_be_stopped() {
    let harness = Harness::new();
    assert_success(&harness.run("install"));
    let failed = harness
        .command("uninstall")
        .env("FAKE_LAUNCHCTL_FAIL_BOOTOUT", "1")
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert!(harness.launchctl_state.exists());
    assert!(harness.plist().exists());
}

#[test]
fn start_requires_an_install_and_labels_are_restricted() {
    let harness = Harness::new();
    let missing = harness.run("start");
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("is not installed"));

    let invalid = harness
        .command("install")
        .env("CODEX_USAGE_SERVICE_LABEL", "not/a label")
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("invalid LaunchAgent label"));
    assert!(!harness.launch_agents.exists());
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn plist_value(path: &Path, key: &str) -> String {
    let output = Command::new("/usr/bin/plutil")
        .args(["-extract", key, "raw", "-o", "-"])
        .arg(path)
        .output()
        .unwrap();
    assert_success(&output);
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
