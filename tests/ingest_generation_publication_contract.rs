use codex_usage::{
    ingest::{IngestRoots, projector_generation_is_current, scan_once, scan_one_shot},
    storage::Db,
};
use rusqlite::OptionalExtension;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

const PROJECTOR_GENERATION: i64 = 1;
const OWNER_A: &str = "019f64aa-0000-7000-8000-0000000000a1";
const OWNER_B: &str = "019f64aa-0000-7000-8000-0000000000b2";

struct Harness {
    _temp: TempDir,
    db: Db,
    root_a: PathBuf,
    root_b: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root_a = temp.path().join("sessions-a");
        let root_b = temp.path().join("sessions-b");
        fs::create_dir_all(&root_a).unwrap();
        fs::create_dir_all(&root_b).unwrap();
        write_owner(&root_a.join("a.jsonl"), OWNER_A, "2026-07-25T09:00:00Z");
        write_owner(&root_b.join("b.jsonl"), OWNER_B, "2026-07-25T10:00:00Z");
        let db = Db::open(temp.path().join("data/codex-usage.db")).unwrap();
        Self {
            _temp: temp,
            db,
            root_a,
            root_b,
        }
    }

    fn roots_a(&self) -> IngestRoots {
        roots(&self.root_a)
    }

    fn roots_b(&self) -> IngestRoots {
        roots(&self.root_b)
    }

    fn establish_root_a_then_unpublish(&self) {
        scan_one_shot(&self.db, &self.roots_a()).unwrap();
        let deleted = self
            .db
            .connect()
            .unwrap()
            .execute("DELETE FROM app_meta WHERE key='projector_generation'", [])
            .unwrap();
        assert_eq!(
            deleted, 1,
            "the initial one-shot must publish its generation"
        );
        assert!(!projector_generation_is_current(&self.db).unwrap());
    }
}

fn roots(active: &Path) -> IngestRoots {
    IngestRoots {
        active: Some(active.to_owned()),
        archive: None,
    }
}

fn write_owner(path: &Path, owner: &str, timestamp: &str) {
    let record = json!({
        "timestamp": timestamp,
        "type": "session_meta",
        "payload": {
            "id": owner,
            "session_id": owner,
            "cwd": "/tmp/ingest-generation-publication-contract",
            "source": "cli"
        }
    });
    let mut file = File::create(path).unwrap();
    writeln!(file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
}

fn global_generation(db: &Db) -> Option<String> {
    db.connect()
        .unwrap()
        .query_row(
            "SELECT value FROM app_meta WHERE key='projector_generation'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
}

fn source_generation(db: &Db, owner: &str) -> i64 {
    db.connect()
        .unwrap()
        .query_row(
            "SELECT CAST(json_extract(parse_state_json,'$.projector_generation') AS INTEGER)
             FROM source_files WHERE rollout_id=?1",
            [owner],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn scan_once_updates_source_generation_without_publishing_global_currentness() {
    let harness = Harness::new();

    let report = scan_once(&harness.db, &harness.roots_a()).unwrap();

    assert_eq!(report.files_seen, 1);
    assert_eq!(report.files_ingested, 1);
    assert_eq!(report.files_failed, 0);
    assert_eq!(
        source_generation(&harness.db, OWNER_A),
        PROJECTOR_GENERATION
    );
    assert_eq!(
        global_generation(&harness.db),
        None,
        "one scan cycle must not claim that the complete projector generation is published"
    );
    assert!(
        !projector_generation_is_current(&harness.db).unwrap(),
        "a nonempty projection with only per-source generation state is globally stale"
    );
}

#[test]
fn one_shot_publishes_after_root_adoption_confirmation_and_reconciliation() {
    let harness = Harness::new();
    harness.establish_root_a_then_unpublish();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TABLE publication_observations(
                 old_owner_present INTEGER NOT NULL,
                 new_owner_present INTEGER NOT NULL,
                 source_count INTEGER NOT NULL,
                 ingest_state TEXT NOT NULL
             );
             CREATE TRIGGER observe_generation_publication
             AFTER INSERT ON app_meta
             WHEN NEW.key='projector_generation'
             BEGIN
                 INSERT INTO publication_observations
                 SELECT
                     EXISTS(SELECT 1 FROM source_files
                            WHERE rollout_id='019f64aa-0000-7000-8000-0000000000a1'),
                     EXISTS(SELECT 1 FROM source_files
                            WHERE rollout_id='019f64aa-0000-7000-8000-0000000000b2'),
                     (SELECT COUNT(*) FROM source_files),
                     (SELECT value FROM app_meta WHERE key='ingest_state');
             END;",
        )
        .unwrap();

    let report = scan_one_shot(&harness.db, &harness.roots_b()).unwrap();

    assert_eq!(
        report.files_seen, 2,
        "one-shot must report both bounded passes"
    );
    assert_eq!(report.files_ingested, 1);
    assert_eq!(report.files_unchanged, 1);
    let publication: (i64, i64, i64, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT old_owner_present,new_owner_present,source_count,ingest_state
             FROM publication_observations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        publication,
        (0, 1, 1, "idle".into()),
        "publication must observe the confirmed root with the old projection already reconciled"
    );
    assert_eq!(
        global_generation(&harness.db),
        Some(PROJECTOR_GENERATION.to_string())
    );
    assert!(projector_generation_is_current(&harness.db).unwrap());
}

#[test]
fn confirmation_failure_keeps_the_global_generation_unpublished() {
    let harness = Harness::new();
    harness.establish_root_a_then_unpublish();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TABLE confirmation_gate(scan_starts INTEGER NOT NULL);
             INSERT INTO confirmation_gate VALUES(0);
             CREATE TRIGGER reject_confirmation_scan_start
             BEFORE UPDATE ON app_meta
             WHEN OLD.key='ingest_state'
              AND NEW.value='scanning'
              AND (SELECT scan_starts FROM confirmation_gate)=1
             BEGIN
                 SELECT RAISE(ABORT,'injected confirmation failure');
             END;
             CREATE TRIGGER count_scan_start
             AFTER UPDATE ON app_meta
             WHEN OLD.key='ingest_state' AND NEW.value='scanning'
             BEGIN
                 UPDATE confirmation_gate SET scan_starts=scan_starts+1;
             END;",
        )
        .unwrap();

    let error = scan_one_shot(&harness.db, &harness.roots_b()).unwrap_err();

    assert!(
        format!("{error:#}").contains("injected confirmation failure"),
        "unexpected failure: {error:#}"
    );
    assert_eq!(global_generation(&harness.db), None);
    assert!(!projector_generation_is_current(&harness.db).unwrap());
    let state: (i64, String, i64, i64) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 (SELECT scan_starts FROM confirmation_gate),
                 (SELECT value FROM app_meta WHERE key='ingest_state'),
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?1),
                 EXISTS(SELECT 1 FROM source_files WHERE rollout_id=?2)",
            [OWNER_A, OWNER_B],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (1, "error".into(), 1, 1),
        "the first pass adopted the new roots, but failed confirmation neither reconciled nor published"
    );
}

#[test]
fn publication_failure_keeps_completed_source_state_globally_stale() {
    let harness = Harness::new();
    harness.establish_root_a_then_unpublish();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_generation_publication
             BEFORE INSERT ON app_meta
             WHEN NEW.key='projector_generation'
             BEGIN
                 SELECT RAISE(ABORT,'injected publication failure');
             END;",
        )
        .unwrap();

    let error = scan_one_shot(&harness.db, &harness.roots_a()).unwrap_err();

    assert!(
        format!("{error:#}").contains("injected publication failure"),
        "unexpected failure: {error:#}"
    );
    assert_eq!(
        source_generation(&harness.db, OWNER_A),
        PROJECTOR_GENERATION
    );
    assert_eq!(global_generation(&harness.db), None);
    assert!(!projector_generation_is_current(&harness.db).unwrap());
    let (ingest_state, report_json): (String, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT
                 (SELECT value FROM app_meta WHERE key='ingest_state'),
                 (SELECT value FROM app_meta WHERE key='last_scan_report')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(ingest_state, "error");
    let report: Value = serde_json::from_str(&report_json).unwrap();
    assert_eq!(report["filesSeen"], 1);
    assert_eq!(report["filesUnchanged"], 1);
    assert_eq!(report["filesFailed"], 0);
}
