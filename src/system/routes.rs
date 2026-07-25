use super::{settings, status};
use crate::{
    storage::WorkClass,
    web::{
        ReadRuntime,
        error::{ApiError, ApiResult},
    },
};
use axum::{Json, Router, extract::State, routing::get};
use chrono::Local;
use std::path::PathBuf;

#[derive(Clone)]
struct SettingsState {
    reads: ReadRuntime,
    active_root: Option<PathBuf>,
    archive_root: Option<PathBuf>,
}

pub(crate) fn router(
    reads: ReadRuntime,
    active_root: Option<PathBuf>,
    archive_root: Option<PathBuf>,
) -> Router {
    Router::new()
        .route("/status", get(status))
        .with_state(reads.clone())
        .merge(
            Router::new()
                .route("/settings", get(settings))
                .with_state(SettingsState {
                    reads,
                    active_root,
                    archive_root,
                }),
        )
}

async fn status(State(reads): State<ReadRuntime>) -> ApiResult<Json<status::StatusResponse>> {
    Ok(Json(
        reads
            .snapshot(WorkClass::Control, status::query_on)
            .await
            .map_err(ApiError::internal)?,
    ))
}

async fn settings(
    State(state): State<SettingsState>,
) -> ApiResult<Json<settings::SettingsResponse>> {
    let database_path = state.reads.database_path().display().to_string();
    let active_root = state
        .active_root
        .as_ref()
        .map(|path| path.display().to_string());
    let archive_root = state
        .archive_root
        .as_ref()
        .map(|path| path.display().to_string());
    let timezone = Local::now().format("%Z").to_string();
    let database_bytes = state.reads.database_storage_bytes();
    Ok(Json(
        state
            .reads
            .snapshot(WorkClass::Heavy, move |connection| {
                settings::query_on(
                    connection,
                    database_path,
                    active_root,
                    archive_root,
                    timezone,
                    database_bytes,
                )
            })
            .await
            .map_err(ApiError::internal)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageExecutor;
    use std::sync::{Arc, Condvar, Mutex};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settings_waits_for_the_heavy_executor_lane() {
        use tokio::sync::oneshot;

        let temp = tempfile::tempdir().unwrap();
        let database = crate::storage::Db::open(temp.path().join("usage.db")).unwrap();
        let executor = StorageExecutor::new(3, 1);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel();
        let blocker = {
            let executor = executor.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                executor
                    .run(WorkClass::Heavy, move || {
                        blocker_started_tx.send(()).unwrap();
                        let (lock, ready) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = ready.wait(released).unwrap();
                        }
                        Ok(())
                    })
                    .await
            })
        };
        blocker_started_rx.await.unwrap();

        let state = SettingsState {
            reads: ReadRuntime::new(database, executor),
            active_root: None,
            archive_root: None,
        };
        let (settings_entered_tx, settings_entered_rx) = oneshot::channel();
        let settings_task = tokio::spawn(async move {
            settings_entered_tx.send(()).unwrap();
            settings(State(state)).await
        });
        settings_entered_rx.await.unwrap();
        let completed_while_heavy_lane_was_blocked =
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                loop {
                    if settings_task.is_finished() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await;
        assert!(
            completed_while_heavy_lane_was_blocked.is_err(),
            "Settings completed while the only Heavy permit was held"
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        blocker.await.unwrap().unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), settings_task)
            .await
            .expect("Settings did not resume after the Heavy permit was released")
            .unwrap()
            .unwrap();
    }
}
