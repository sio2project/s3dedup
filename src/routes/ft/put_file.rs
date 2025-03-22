use crate::{locks, AppState};
use axum::Json;
use axum::extract::{Path, Query, State};
use std::sync::Arc;
use axum::http::{HeaderMap, Response, StatusCode};
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

    if let Err(e) = timestamp {
        error!("Failed to parse last_modified: {}", e);
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Failed to parse last_modified".to_string())
            .unwrap();
    }
    let timestamp = timestamp.unwrap();

    state.locks.acquire_exclusive(&*locks::file_lock(&state.bucket_name, &path));
    let current_modified = state.kvstorage.get_modified(&state.bucket_name, &path).await;
    if let Err(e) = current_modified {
        error!("Failed to get current modified: {}", e);
        state.locks.release(&*locks::file_lock(&state.bucket_name, &path));

        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to get current modified".to_string())
            .unwrap();
    }
    let current_modified = current_modified.unwrap();

    // If the uploaded file is younger than the current one, return 200 OK
    if current_modified >= timestamp {
        state.locks.release(&*locks::file_lock(&state.bucket_name, &path));
        return Response::builder()
            .status(StatusCode::OK)
            .header("Last-Modified", query.last_modified)
            .body("".to_string())
            .unwrap();
    }

    // tmp
    Response::builder()
        .status(StatusCode::OK)
        .body("".to_string())
        .unwrap()
}
