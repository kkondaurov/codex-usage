use super::protocol::{OwnerMeta, looks_like_uuid};
use anyhow::{Context, Result, anyhow};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Clone, Debug)]
pub(super) struct SourceCandidate {
    pub(super) path: PathBuf,
    pub(super) archived: bool,
    pub(super) size: u64,
    pub(super) complete: bool,
    pub(super) owner: OwnerMeta,
}

#[derive(Debug)]
pub(super) struct PreparedSourceCandidate {
    pub(super) candidate: SourceCandidate,
    pub(super) ready: bool,
}

#[derive(Debug)]
pub(super) struct CatalogSelectionPlan {
    pub(super) selected: Vec<SourceCandidate>,
    pub(super) protect_reconciliation: HashSet<String>,
}

#[derive(Debug, Default)]
pub(super) struct PendingEmptyOwners {
    pub(super) defer_selection: HashSet<String>,
    pub(super) protect_reconciliation: HashSet<String>,
}

#[derive(Debug)]
pub(super) struct SelectedSourceExtent {
    pub(super) path: PathBuf,
    pub(super) raw_size: u64,
    pub(super) committed_size: u64,
    pub(super) fingerprint: String,
}

#[derive(Debug, Default)]
pub(super) struct SourceHandoffIndex {
    rollout_ids: HashMap<String, String>,
    unique_file_names: HashMap<String, Option<String>>,
}

impl SourceHandoffIndex {
    pub(super) fn new(extents: &HashMap<String, SelectedSourceExtent>) -> Self {
        let mut index = Self::default();
        for (owner_id, extent) in extents {
            index
                .rollout_ids
                .insert(owner_id.to_ascii_lowercase(), owner_id.clone());
            let Some(file_name) = source_file_name_key(&extent.path) else {
                continue;
            };
            index
                .unique_file_names
                .entry(file_name)
                .and_modify(|existing| {
                    if existing.as_deref() != Some(owner_id.as_str()) {
                        *existing = None;
                    }
                })
                .or_insert_with(|| Some(owner_id.clone()));
        }
        index
    }

    pub(super) fn matching_owner<'a>(&'a self, path: &Path) -> Option<&'a str> {
        rollout_id_from_source_path(&path.to_string_lossy())
            .and_then(|owner_id| self.rollout_ids.get(&owner_id.to_ascii_lowercase()))
            .or_else(|| {
                let file_name = source_file_name_key(path)?;
                self.unique_file_names.get(&file_name)?.as_ref()
            })
            .map(String::as_str)
    }
}

fn source_file_name_key(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
}

pub(super) fn source_is_complete(path: &Path, size: u64) -> bool {
    if size == 0 {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    if file.seek(SeekFrom::End(-1)).is_err() {
        return false;
    }
    let mut tail = [0_u8; 1];
    file.read_exact(&mut tail).is_ok() && tail[0] == b'\n'
}

pub(super) fn collect_jsonl(
    root: &Path,
    archived: bool,
    files: &mut Vec<(PathBuf, bool)>,
    observed: &mut HashSet<String>,
    pending_empty: &mut HashSet<String>,
) -> Result<()> {
    let metadata = root
        .metadata()
        .with_context(|| format!("configured ingest root {} is unavailable", root.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "configured ingest root {} is not a directory",
            root.display()
        ));
    }
    std::fs::read_dir(root)
        .with_context(|| format!("configured ingest root {} is unreadable", root.display()))?;
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry.with_context(|| {
            format!("configured ingest root {} traversal failed", root.display())
        })?;
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|value| value == "jsonl")
        {
            let metadata = entry.metadata().with_context(|| {
                format!("source metadata unavailable for {}", entry.path().display())
            })?;
            // An existing empty JSONL is a writer-owned placeholder, not a
            // deletion. Keep it in the reconciliation set while deferring
            // parsing until the writer publishes at least one byte.
            let path_text = entry.path().to_string_lossy().into_owned();
            observed.insert(path_text.clone());
            if metadata.len() > 0 {
                files.push((entry.path().to_path_buf(), archived));
            } else {
                pending_empty.insert(path_text);
            }
        }
    }
    Ok(())
}

pub(super) fn owners_with_pending_empty_sources(
    pending_empty: &HashSet<String>,
    selected_extents: &HashMap<String, SelectedSourceExtent>,
    source_handoffs: &SourceHandoffIndex,
) -> PendingEmptyOwners {
    if pending_empty.is_empty() {
        return PendingEmptyOwners::default();
    }
    let mut owners = PendingEmptyOwners::default();
    for (owner_id, extent) in selected_extents {
        let exact_path_is_empty = pending_empty
            .iter()
            .any(|path| Path::new(path) == extent.path);
        let correlated_handoff_is_empty = pending_empty.iter().any(|path| {
            Path::new(path) != extent.path
                && source_handoffs.matching_owner(Path::new(path)) == Some(owner_id.as_str())
        });
        if exact_path_is_empty {
            owners.defer_selection.insert(owner_id.clone());
        }
        if exact_path_is_empty || correlated_handoff_is_empty {
            owners.protect_reconciliation.insert(owner_id.clone());
        }
    }
    owners
}

fn rollout_id_from_source_path(path: &str) -> Option<&str> {
    let stem = Path::new(path).file_stem()?.to_str()?;
    let candidate = stem.get(stem.len().checked_sub(36)?..)?;
    looks_like_uuid(candidate).then_some(candidate)
}

pub(super) fn plan_catalog_selection(
    prepared: Vec<PreparedSourceCandidate>,
    pending_empty: PendingEmptyOwners,
    initial_protected: HashSet<String>,
) -> CatalogSelectionPlan {
    let mut prepared_by_owner: std::collections::BTreeMap<String, Vec<PreparedSourceCandidate>> =
        std::collections::BTreeMap::new();
    for candidate in prepared {
        prepared_by_owner
            .entry(candidate.candidate.owner.owner_id.clone())
            .or_default()
            .push(candidate);
    }

    let mut protect_reconciliation = initial_protected;
    protect_reconciliation.extend(pending_empty.protect_reconciliation);
    let mut selected = Vec::new();
    for (owner_id, candidates) in prepared_by_owner {
        if pending_empty.defer_selection.contains(&owner_id) {
            continue;
        }
        let mut ready_candidates = Vec::new();
        let mut has_unready_candidate = false;
        for candidate in candidates {
            if candidate.ready {
                ready_candidates.push(candidate.candidate);
            } else {
                has_unready_candidate = true;
            }
        }
        if has_unready_candidate {
            protect_reconciliation.insert(owner_id);
        }
        if let Some(candidate) = ready_candidates
            .into_iter()
            .max_by(source_candidate_preference)
        {
            selected.push(candidate);
        }
    }
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    CatalogSelectionPlan {
        selected,
        protect_reconciliation,
    }
}

pub(super) fn source_candidate_preference(
    left: &SourceCandidate,
    right: &SourceCandidate,
) -> Ordering {
    left.complete
        .cmp(&right.complete)
        .then_with(|| left.size.cmp(&right.size))
        .then_with(|| (!left.archived).cmp(&(!right.archived)))
        // A lexical minimum is the deterministic winner when every semantic
        // preference is equal, so reverse the final comparison for max_by.
        .then_with(|| right.path.cmp(&left.path))
}

pub(super) fn resolve_owner_topology(
    owners: &mut HashMap<String, OwnerMeta>,
    existing: &HashMap<String, String>,
) {
    let discovered = owners.clone();
    let mut resolved = HashMap::new();
    for owner_id in discovered.keys() {
        let thread_id = resolve_owner_thread(
            owner_id,
            &discovered,
            existing,
            &mut resolved,
            &mut HashSet::new(),
        );
        if let Some(owner) = owners.get_mut(owner_id) {
            owner.thread_id = thread_id;
        }
    }
}

fn resolve_owner_thread(
    owner_id: &str,
    discovered: &HashMap<String, OwnerMeta>,
    existing: &HashMap<String, String>,
    resolved: &mut HashMap<String, String>,
    visiting: &mut HashSet<String>,
) -> String {
    if let Some(thread_id) = resolved.get(owner_id) {
        return thread_id.clone();
    }
    let Some(owner) = discovered.get(owner_id) else {
        return existing
            .get(owner_id)
            .cloned()
            .unwrap_or_else(|| owner_id.to_owned());
    };
    if !owner.is_subagent || !visiting.insert(owner_id.to_owned()) {
        return owner.thread_id.clone();
    }
    let anchors = [
        Some(owner.thread_id.as_str()).filter(|value| *value != owner.owner_id),
        owner.parent_rollout_id.as_deref(),
        owner.parent_thread_id.as_deref(),
    ];
    let thread_id = anchors
        .into_iter()
        .flatten()
        .find_map(|anchor| {
            if discovered.contains_key(anchor) {
                Some(resolve_owner_thread(
                    anchor, discovered, existing, resolved, visiting,
                ))
            } else {
                existing
                    .get(anchor)
                    .cloned()
                    .or_else(|| Some(anchor.to_owned()))
            }
        })
        .unwrap_or_else(|| owner.thread_id.clone());
    visiting.remove(owner_id);
    resolved.insert(owner_id.to_owned(), thread_id.clone());
    thread_id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> OwnerMeta {
        OwnerMeta {
            owner_id: "owner".into(),
            thread_id: "thread".into(),
            parent_rollout_id: None,
            parent_thread_id: None,
            agent_path: None,
            agent_nickname: None,
            is_subagent: false,
            forked: false,
            timestamp: "2026-07-25T10:20:30Z".into(),
            cwd: None,
            project: None,
            repository_url: None,
            branch: None,
            source: None,
            thread_source: None,
            source_json: None,
        }
    }

    #[test]
    fn catalog_discovery_classifies_nonempty_empty_and_ignored_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sessions");
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();

        let top_level = root.join("top-level.jsonl");
        let nested_source = nested.join("nested.jsonl");
        let empty = root.join("empty.jsonl");
        let ignored = root.join("notes.txt");
        std::fs::write(&top_level, b"{}\n").unwrap();
        std::fs::write(&nested_source, b"{}\n").unwrap();
        File::create(&empty).unwrap();
        std::fs::write(&ignored, b"not a rollout").unwrap();

        #[cfg(unix)]
        {
            let outside = temp.path().join("outside.jsonl");
            std::fs::write(&outside, b"{}\n").unwrap();
            std::os::unix::fs::symlink(&outside, root.join("linked.jsonl")).unwrap();
        }

        let mut files = Vec::new();
        let mut observed = HashSet::new();
        let mut pending_empty = HashSet::new();
        collect_jsonl(&root, true, &mut files, &mut observed, &mut pending_empty).unwrap();

        assert!(files.iter().all(|(_, archived)| *archived));
        let discovered = files
            .into_iter()
            .map(|(path, _)| path)
            .collect::<HashSet<_>>();
        assert_eq!(
            discovered,
            HashSet::from([top_level.clone(), nested_source.clone()])
        );
        assert_eq!(
            observed,
            HashSet::from([
                top_level.to_string_lossy().into_owned(),
                nested_source.to_string_lossy().into_owned(),
                empty.to_string_lossy().into_owned(),
            ])
        );
        assert_eq!(
            pending_empty,
            HashSet::from([empty.to_string_lossy().into_owned()])
        );
    }

    #[test]
    fn catalog_preference_matrix_is_complete_and_permutation_invariant() {
        let owner = owner();
        let candidate = |path: &str, archived: bool, size: u64, complete: bool| SourceCandidate {
            path: PathBuf::from(path),
            archived,
            size,
            complete,
            owner: owner.clone(),
        };
        // Strictly ordered from least to most preferred. Each adjacent pair
        // isolates one precedence rule while all higher-precedence axes match.
        let candidates = vec![
            candidate("a", false, 1_000, false),
            candidate("a", true, 10, true),
            candidate("z", false, 10, true),
            candidate("a", false, 10, true),
            candidate("z", true, 20, true),
            candidate("z", false, 20, true),
            candidate("a", false, 20, true),
        ];

        for (left_index, left) in candidates.iter().enumerate() {
            for (right_index, right) in candidates.iter().enumerate() {
                assert_eq!(
                    source_candidate_preference(left, right),
                    left_index.cmp(&right_index),
                    "unexpected preference for candidate {left_index} versus {right_index}"
                );
            }
        }

        fn assert_every_permutation_selects_last(
            candidates: &[SourceCandidate],
            order: &mut [usize],
            position: usize,
        ) {
            if position == order.len() {
                let winner = order
                    .iter()
                    .map(|index| &candidates[*index])
                    .max_by(|left, right| source_candidate_preference(left, right))
                    .unwrap();
                assert!(std::ptr::eq(winner, &candidates[candidates.len() - 1]));
                return;
            }
            for index in position..order.len() {
                order.swap(position, index);
                assert_every_permutation_selects_last(candidates, order, position + 1);
                order.swap(position, index);
            }
        }

        let mut order = (0..candidates.len()).collect::<Vec<_>>();
        assert_every_permutation_selects_last(&candidates, &mut order, 0);
    }

    #[test]
    fn catalog_selection_planner_filters_before_preference_and_preserves_protection() {
        let owner = |owner_id: &str| OwnerMeta {
            owner_id: owner_id.into(),
            thread_id: owner_id.into(),
            parent_rollout_id: None,
            parent_thread_id: None,
            agent_path: None,
            agent_nickname: None,
            is_subagent: false,
            forked: false,
            timestamp: "2026-07-25T10:20:30Z".into(),
            cwd: None,
            project: None,
            repository_url: None,
            branch: None,
            source: None,
            thread_source: None,
            source_json: None,
        };
        let prepared = |owner_id: &str, path: &str, size: u64, complete: bool, ready: bool| {
            PreparedSourceCandidate {
                candidate: SourceCandidate {
                    path: PathBuf::from(path),
                    archived: false,
                    size,
                    complete,
                    owner: owner(owner_id),
                },
                ready,
            }
        };
        let candidates = vec![
            // The coordinator has already established that a new owner's candidate
            // is ready even though no prior handoff evidence exists.
            prepared("new-owner", "/z-new.jsonl", 1, false, true),
            // The selected path itself is likewise supplied as finally ready.
            prepared("same-path", "/m-same.jsonl", 10, true, true),
            // Readiness is a prerequisite: the larger otherwise-preferred
            // candidate cannot beat the smaller ready source.
            prepared("mixed", "/d-ready.jsonl", 10, true, true),
            prepared("mixed", "/b-unready.jsonl", 100, true, false),
            prepared("all-unready", "/c-unready.jsonl", 100, true, false),
            prepared("ready-choice", "/h-ready-small.jsonl", 10, true, true),
            prepared("ready-choice", "/g-ready-large.jsonl", 100, true, true),
            prepared("exact-empty", "/e-exact.jsonl", 10, true, true),
            prepared("correlated", "/a-correlated.jsonl", 10, true, true),
        ];
        let pending_empty = PendingEmptyOwners {
            defer_selection: HashSet::from(["exact-empty".into()]),
            protect_reconciliation: HashSet::from(["exact-empty".into(), "correlated".into()]),
        };
        let plan = plan_catalog_selection(
            candidates,
            pending_empty,
            HashSet::from(["initial-sentinel".into()]),
        );

        let selected = plan
            .selected
            .iter()
            .map(|candidate| {
                (
                    candidate.owner.owner_id.as_str(),
                    candidate.path.to_string_lossy().into_owned(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            [
                ("correlated", "/a-correlated.jsonl".into()),
                ("mixed", "/d-ready.jsonl".into()),
                ("ready-choice", "/g-ready-large.jsonl".into()),
                ("same-path", "/m-same.jsonl".into()),
                ("new-owner", "/z-new.jsonl".into()),
            ],
            "selected candidates must be ready, exact-empty-safe, and path sorted"
        );
        assert_eq!(
            plan.protect_reconciliation,
            HashSet::from([
                "all-unready".into(),
                "correlated".into(),
                "exact-empty".into(),
                "initial-sentinel".into(),
                "mixed".into(),
            ])
        );
    }

    #[test]
    fn handoff_index_matches_uuid_and_only_unique_filenames() {
        const UUID_OWNER: &str = "019f64aa-0000-7000-8000-000000000201";
        const OTHER_OWNER: &str = "019f64aa-0000-7000-8000-000000000202";
        const UNIQUE_OWNER: &str = "019f64aa-0000-7000-8000-000000000203";
        const AMBIGUOUS_A: &str = "019f64aa-0000-7000-8000-000000000204";
        const AMBIGUOUS_B: &str = "019f64aa-0000-7000-8000-000000000205";

        let extent = |path: &str| SelectedSourceExtent {
            path: PathBuf::from(path),
            raw_size: 1,
            committed_size: 1,
            fingerprint: "fingerprint".into(),
        };
        let uuid_name = format!("rollout-2026-07-25T00-00-00-{UUID_OWNER}.jsonl");
        let extents = HashMap::from([
            (
                UUID_OWNER.into(),
                extent(&format!("/old/primary/{uuid_name}")),
            ),
            (
                OTHER_OWNER.into(),
                extent(&format!("/old/duplicate/{uuid_name}")),
            ),
            (UNIQUE_OWNER.into(), extent("/old/Friendly-Session.JSONL")),
            (AMBIGUOUS_A.into(), extent("/old/a/Duplicate.jsonl")),
            (AMBIGUOUS_B.into(), extent("/old/b/duplicate.JSONL")),
        ]);
        let index = SourceHandoffIndex::new(&extents);

        assert_eq!(
            index.matching_owner(Path::new(
                "/new/ROLLOUT-2026-07-25T00-00-00-019F64AA-0000-7000-8000-000000000201.JSONL"
            )),
            Some(UUID_OWNER),
            "an embedded rollout id is authoritative even when its filename fallback is ambiguous"
        );
        assert_eq!(
            index.matching_owner(Path::new("/new/friendly-session.jsonl")),
            Some(UNIQUE_OWNER)
        );
        assert_eq!(
            index.matching_owner(Path::new("/new/DUPLICATE.jsonl")),
            None,
            "a shared filename must not guess which persisted owner it represents"
        );
        assert_eq!(
            index.matching_owner(Path::new("/new/unrelated.jsonl")),
            None
        );
    }

    #[test]
    fn pending_empty_policy_separates_selection_deferral_from_reconciliation_protection() {
        let extent = |path: &str| SelectedSourceExtent {
            path: PathBuf::from(path),
            raw_size: 1,
            committed_size: 1,
            fingerprint: "fingerprint".into(),
        };
        let extents = HashMap::from([
            ("exact-owner".into(), extent("/active/exact.jsonl")),
            ("handoff-owner".into(), extent("/active/friendly.jsonl")),
            ("unrelated-owner".into(), extent("/active/unrelated.jsonl")),
        ]);
        let handoffs = SourceHandoffIndex::new(&extents);
        let pending_empty = HashSet::from([
            "/active/exact.jsonl".into(),
            "/archive/friendly.jsonl".into(),
            "/archive/stranger.jsonl".into(),
        ]);

        let owners = owners_with_pending_empty_sources(&pending_empty, &extents, &handoffs);
        assert_eq!(
            owners.defer_selection,
            HashSet::from(["exact-owner".into()]),
            "only the selected path's own placeholder defers candidate selection"
        );
        assert_eq!(
            owners.protect_reconciliation,
            HashSet::from(["exact-owner".into(), "handoff-owner".into()]),
            "an exact placeholder and a correlated destination both protect their owner"
        );
        assert!(!owners.defer_selection.contains("unrelated-owner"));
        assert!(!owners.protect_reconciliation.contains("unrelated-owner"));
    }

    #[test]
    fn owner_topology_uses_complete_discovered_graph_and_explicit_existing_anchors() {
        let owner = |owner_id: &str,
                     thread_id: &str,
                     parent_rollout_id: Option<&str>,
                     is_subagent: bool| OwnerMeta {
            owner_id: owner_id.into(),
            thread_id: thread_id.into(),
            parent_rollout_id: parent_rollout_id.map(str::to_owned),
            parent_thread_id: None,
            agent_path: is_subagent.then(|| format!("/root/{owner_id}")),
            agent_nickname: None,
            is_subagent,
            forked: parent_rollout_id.is_some(),
            timestamp: "2026-07-25T10:20:30Z".into(),
            cwd: None,
            project: None,
            repository_url: None,
            branch: None,
            source: None,
            thread_source: None,
            source_json: None,
        };
        let discovered = HashMap::from([
            ("root".into(), owner("root", "root", None, false)),
            ("child".into(), owner("child", "child", Some("root"), true)),
            (
                "grandchild".into(),
                owner("grandchild", "grandchild", Some("child"), true),
            ),
            (
                "persisted-child".into(),
                owner(
                    "persisted-child",
                    "persisted-child",
                    Some("persisted-parent"),
                    true,
                ),
            ),
            (
                "explicit-anchor".into(),
                owner("explicit-anchor", "explicit-thread", Some("root"), true),
            ),
            (
                "root-fork".into(),
                owner("root-fork", "root-fork", Some("root"), false),
            ),
        ]);
        let existing = HashMap::from([("persisted-parent".into(), "persisted-thread".into())]);
        let expected = HashMap::from([
            ("root".into(), "root".into()),
            ("child".into(), "root".into()),
            ("grandchild".into(), "root".into()),
            ("persisted-child".into(), "persisted-thread".into()),
            ("explicit-anchor".into(), "explicit-thread".into()),
            ("root-fork".into(), "root-fork".into()),
        ]);

        let mut forward = discovered.clone();
        resolve_owner_topology(&mut forward, &existing);
        let actual = forward
            .into_iter()
            .map(|(owner_id, owner)| (owner_id, owner.thread_id))
            .collect::<HashMap<_, _>>();
        assert_eq!(actual, expected);

        let mut reversed_entries = discovered.into_iter().collect::<Vec<_>>();
        reversed_entries.reverse();
        let mut reverse = reversed_entries.into_iter().collect::<HashMap<_, _>>();
        resolve_owner_topology(&mut reverse, &existing);
        let actual = reverse
            .into_iter()
            .map(|(owner_id, owner)| (owner_id, owner.thread_id))
            .collect::<HashMap<_, _>>();
        assert_eq!(actual, expected);
    }
}
