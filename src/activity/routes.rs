use super::{
    detail::{
        DetailPage, read_on as read_detail_on, validate_cursor_for as validate_detail_cursor_for,
    },
    model::{ActivityItem, ActivityResponse},
    root_page::{read_page_on, visible_thread_exists_on},
};
use crate::{
    storage::WorkClass,
    web::{
        ReadRuntime,
        error::{ApiError, ApiResult},
        pagination::{clamped_page_size, validated_page},
    },
};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    routing::get,
};
use serde::Deserialize;

pub(crate) fn router(reads: ReadRuntime) -> Router {
    Router::new()
        .route("/sessions/{id}/activity", get(session_activity))
        .route(
            "/sessions/{id}/activity/{event_id}",
            get(session_activity_detail),
        )
        .with_state(reads)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageQuery {
    page: Option<u64>,
    page_size: Option<u64>,
}

const DEFAULT_ACTIVITY_CHILD_PAGE_SIZE: u64 = 250;
const MAX_ACTIVITY_CHILD_PAGE_SIZE: u64 = 500;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ActivityDetailQuery {
    pub(super) child_page: Option<u64>,
    pub(super) child_page_size: Option<u64>,
    pub(super) child_cursor: Option<String>,
}

async fn session_activity(
    State(reads): State<ReadRuntime>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> ApiResult<Json<ActivityResponse>> {
    let page = validated_page(query.page)?;
    let page_size = clamped_page_size(query.page_size, 25, 100);
    reads
        .snapshot(WorkClass::Heavy, move |connection| {
            if !visible_thread_exists_on(connection, &id)? {
                return Ok(None);
            }
            read_page_on(connection, &id, page, page_size).map(Some)
        })
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("session not found"))
}

pub(super) async fn session_activity_detail(
    State(reads): State<ReadRuntime>,
    AxumPath((id, event_id)): AxumPath<(String, String)>,
    Query(query): Query<ActivityDetailQuery>,
) -> ApiResult<Json<ActivityItem>> {
    let child_page = validated_page(query.child_page)?;
    let child_page_size = clamped_page_size(
        query.child_page_size,
        DEFAULT_ACTIVITY_CHILD_PAGE_SIZE,
        MAX_ACTIVITY_CHILD_PAGE_SIZE,
    );
    if let Some(cursor) = query.child_cursor.as_deref() {
        validate_detail_cursor_for(cursor, &id, &event_id)
            .map_err(|_| ApiError::bad_request("invalid Activity cursor"))?;
    }
    let child_cursor = query.child_cursor;
    reads
        .snapshot(WorkClass::Heavy, move |connection| {
            if !visible_thread_exists_on(connection, &id)? {
                return Ok(None);
            }
            read_detail_on(
                connection,
                &id,
                &event_id,
                DetailPage {
                    page: child_page,
                    page_size: child_page_size,
                    cursor: child_cursor.as_deref(),
                },
            )
            .map(Some)
        })
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("session not found"))?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("activity event not found"))
}
