use crate::{locks, AppState};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use axum::body::Body;
use std::sync::Arc;
use tracing::{debug, error};
use crate::routes::ft::utils;

pub async fn ft_get_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    // Remove leading slash from wildcard path
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("Handling GET for path: {}", path);

    // 1. Acquire file lock (shared lock for read operation)
    let lock_key = locks::file_lock(&state.bucket_name, &path);
    state.locks.lock().await.acquire_shared(&lock_key);

    // 2. Check if file exists and get metadata
    let modified_time = state.kvstorage.lock().await.get_modified(&state.bucket_name, &path).await;
    if modified_time.is_err() {
        error!("Failed to get modified time");
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let modified_time = modified_time.unwrap();

    // If file doesn't exist (modified time is 0), return 404
    if modified_time == 0 {
        debug!("File {} not found", path);
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    // 3. Get the hash for this path
    let hash = state.kvstorage.lock().await.get_ref_file(&state.bucket_name, &path).await;
    if hash.is_err() {
        error!("Failed to get ref file");
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let hash = hash.unwrap();

    if hash.is_empty() {
        error!("File {} has no hash reference", path);
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    // 4. Get logical size
    let logical_size = state.kvstorage.lock().await.get_logical_size(&state.bucket_name, &hash).await;
    if logical_size.is_err() {
        error!("Failed to get logical size");
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let logical_size = logical_size.unwrap();

    // 5. Fetch the blob from S3
    let blob_data = state.s3storage.lock().await.get_object(&hash).await;
    if blob_data.is_err() {
        error!("Failed to get object from S3: {}", blob_data.err().unwrap());
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let blob_data = blob_data.unwrap();

    // 6. Release lock
    state.locks.lock().await.release(&lock_key);

    // 7. Return file with appropriate headers (matching original filetracker)
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", blob_data.len().to_string())
        .header("Content-Encoding", "gzip")
        .header("Last-Modified", utils::format_rfc2822_timestamp(modified_time))
        .header("Logical-Size", logical_size.to_string())
        .body(Body::from(blob_data))
        .unwrap()
}
