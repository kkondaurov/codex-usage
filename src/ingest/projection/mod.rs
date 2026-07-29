mod agents;
mod checkpoint;
mod connection;
mod conversation;
mod events;
mod lifecycle;
mod metadata;
mod ordinary;
pub(in crate::ingest) mod record;
mod removal;
mod thread_state;
mod tools;
mod topology;
mod usage;

use super::protocol::CursorState;
#[cfg(test)]
pub(in crate::ingest) use agents::{
    apply_observation as apply_agent_observation, rematerialize_surviving_observation,
};
pub(in crate::ingest) use checkpoint::{
    PathConflict, SourceCheckpointWrite, SourceHandoffUpdate, UnchangedSourceUpdate,
    clear_confirmed_shrink, delete_source_checkpoint, find_path_conflict,
    mark_source_handoff_unchanged, mark_source_unchanged, rematerialize_after_checkpoint,
    save_source_checkpoint,
};
pub(in crate::ingest) use connection::{
    ProjectionConnection, ProjectionTx, ReconciliationCandidate,
};
pub(in crate::ingest) use metadata::{
    apply_indexed_thread_title, clear_projected_thread_title, upsert_owner,
};
pub(in crate::ingest) use removal::{
    RemovalImpact, apply_thread_metadata_reset, delete_thread_if_abandoned, remove_rollout,
};
pub(in crate::ingest) use topology::load_existing_owner_threads;

pub(in crate::ingest) fn event_id(state: &CursorState, line: u64) -> String {
    format!("{}:{line}", state.owner_id)
}
