use super::{
    owner_reader::read_surviving_owners,
    projection::{
        ProjectionConnection, ProjectionTx, ReconciliationCandidate, RemovalImpact,
        apply_thread_metadata_reset, delete_source_checkpoint, delete_thread_if_abandoned,
        remove_rollout,
    },
};
use crate::storage::Db;
use anyhow::Result;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

#[derive(Debug, Default, Eq, PartialEq)]
struct ReconciliationPlan {
    removals: Vec<ReconciliationCandidate>,
}

fn plan_reconciliation(
    observed_paths: &HashSet<String>,
    protected_owner_ids: &HashSet<String>,
    enumerated_roots: &[PathBuf],
    incomplete_roots: &[PathBuf],
    candidates: Vec<ReconciliationCandidate>,
) -> ReconciliationPlan {
    if enumerated_roots.is_empty() && incomplete_roots.is_empty() {
        return ReconciliationPlan::default();
    }
    let removals = candidates
        .into_iter()
        .filter(|candidate| !observed_paths.contains(&candidate.path))
        .filter(|candidate| !protected_owner_ids.contains(&candidate.rollout_id))
        .filter(|candidate| {
            let source_path = Path::new(&candidate.path);
            !incomplete_roots
                .iter()
                .any(|root| source_path.starts_with(root))
        })
        .collect();
    ReconciliationPlan { removals }
}

pub(super) fn reset_thread_metadata_from_sources(
    tx: &ProjectionTx<'_>,
    impact: &RemovalImpact,
    planned_removal_paths: &HashSet<String>,
) -> Result<()> {
    let Some(reset) = impact.metadata_reset.as_ref() else {
        return Ok(());
    };
    // `remove_rollout` computes its evidence from the current transaction
    // state. When several files from one thread disappear together, a root
    // rollout can be processed before another planned removal and that doomed
    // file will still be present in the query result. It is not surviving
    // evidence, so exclude the complete atomic plan before touching the
    // filesystem. Every path that remains must still be readable.
    let surviving_source_paths = reset
        .ordered_source_paths
        .iter()
        .filter(|path| !planned_removal_paths.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    let owners = read_surviving_owners(&surviving_source_paths)?;
    apply_thread_metadata_reset(tx, reset, &owners)
}

pub(super) fn reconcile_missing(
    db: &Db,
    observed_paths: &HashSet<String>,
    protected_owner_ids: &HashSet<String>,
    enumerated_roots: &[PathBuf],
    incomplete_roots: &[PathBuf],
) -> Result<()> {
    if enumerated_roots.is_empty() && incomplete_roots.is_empty() {
        return Ok(());
    }
    let mut connection = db.connect()?;
    let projection = ProjectionConnection::new(&mut connection);
    let candidates = projection.reconciliation_candidates()?;
    let plan = plan_reconciliation(
        observed_paths,
        protected_owner_ids,
        enumerated_roots,
        incomplete_roots,
        candidates,
    );
    let planned_removal_paths = plan
        .removals
        .iter()
        .map(|candidate| candidate.path.clone())
        .collect::<HashSet<_>>();
    // Rollout removal reads before deleting. Reserve writer ownership only
    // after the candidate list is owned, so the snapshot cannot become stale
    // while the plan is applied.
    let transaction = projection.begin_reconciliation()?;
    for candidate in plan.removals {
        let impact = remove_rollout(&transaction, &candidate.rollout_id)?;
        reset_thread_metadata_from_sources(&transaction, &impact, &planned_removal_paths)?;
        delete_source_checkpoint(&transaction, &candidate.rollout_id)?;
        if let Some(thread_id) = impact.thread_id.or(candidate.root_thread_id) {
            delete_thread_if_abandoned(&transaction, &thread_id)?;
        }
    }
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, path: &str) -> ReconciliationCandidate {
        ReconciliationCandidate {
            rollout_id: id.to_owned(),
            path: path.to_owned(),
            root_thread_id: Some(format!("thread-{id}")),
        }
    }

    #[test]
    fn plan_is_noop_without_any_enumeration_evidence() {
        let plan = plan_reconciliation(
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &[],
            vec![candidate("old", "/old/source.jsonl")],
        );

        assert!(plan.removals.is_empty());
    }

    #[test]
    fn plan_preserves_protected_sources_and_orders_every_authorized_removal() {
        let observed = HashSet::from(["/active/seen.jsonl".to_owned()]);
        let protected = HashSet::from(["handoff".to_owned()]);
        let enumerated = vec![PathBuf::from("/active")];
        let incomplete = vec![PathBuf::from("/archive/partial")];
        let plan = plan_reconciliation(
            &observed,
            &protected,
            &enumerated,
            &incomplete,
            vec![
                candidate("seen", "/active/seen.jsonl"),
                candidate("handoff", "/active/handoff.jsonl"),
                candidate("incomplete", "/archive/partial/source.jsonl"),
                candidate("active-gone", "/active/gone.jsonl"),
                candidate("old-root", "/old/gone.jsonl"),
                candidate("component-prefix", "/archive/partiality/source.jsonl"),
            ],
        );

        assert_eq!(
            plan.removals,
            vec![
                candidate("active-gone", "/active/gone.jsonl"),
                candidate("old-root", "/old/gone.jsonl"),
                candidate("component-prefix", "/archive/partiality/source.jsonl"),
            ]
        );
    }
}
