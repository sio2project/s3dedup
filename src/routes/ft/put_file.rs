use crate::{locks, AppState};
use axum::extract::{Path, Query, State};
use std::sync::Arc;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use tracing::{debug, error};
use crate::routes::ft::{utils, LastModifiedQuery};


pub async fn ft_put_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    Query(query): Query<LastModifiedQuery>,
) -> impl IntoResponse {
    debug!("timestamp: {}", query.last_modified);
    let timestamp = utils::conv_rfc2822_to_unix_timestamp(&query.last_modified);

    if timestamp.is_err() {
        error!("Failed to parse last_modified");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Failed to parse last_modified".to_string())
            .unwrap();
    }
    let timestamp = timestamp.unwrap();

    let lock_key = locks::file_lock(&state.bucket_name, &path);
    state.locks.lock().await.acquire_exclusive(&lock_key);
    let current_modified = state.kvstorage.lock().await.get_modified(&state.bucket_name, &path).await;
    if current_modified.is_err() {
        error!("Failed to get current modified");
        state.locks.lock().await.release(&lock_key);

        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to get current modified".to_string())
            .unwrap();
    }
    let current_modified = current_modified.unwrap();

    // If the uploaded file is younger than the current one, return 200 OK
    if current_modified >= timestamp {
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::OK)
            .header("Last-Modified", query.last_modified)
            .body("".to_string())
            .unwrap();
    }

    // tmp - release lock before returning
    state.locks.lock().await.release(&lock_key);
    Response::builder()
        .status(StatusCode::OK)
        .body("".to_string())
        .unwrap()
}
