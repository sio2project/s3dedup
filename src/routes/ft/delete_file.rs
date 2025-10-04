use crate::routes::ft::{LastModifiedQuery, utils};
use crate::{AppState, locks};
use axum::extract::{Path, Query, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error, info};

pub async fn ft_delete_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    Query(query): Query<LastModifiedQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    // Remove leading slash from wildcard path
    let path = path.strip_prefix('/').unwrap_or(&path);

    debug!("DELETE request for path: {}", path);
    debug!("Query params: last_modified={:?}", query.last_modified);
    debug!("Headers: last-modified={:?}", headers.get("last-modified"));

    // 1. Parse and validate timestamp
    let timestamp = match utils::extract_timestamp(&headers, query.last_modified.as_ref()) {
        Ok(ts) => ts,
        Err(e) => {
            error!("Failed to extract timestamp: {}", e);
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(e)
                .unwrap();
        }
    };

    debug!("Handling DELETE {}@{}", path, timestamp);

    // 2. Acquire file lock (exclusive for write operation)
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let locks_storage = &state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let _guard = lock.acquire_exclusive().await;

    // 3. Check if file exists
    let current_modified = state
        .kvstorage
        .lock()
        .await
        .get_modified(&state.bucket_name, path)
        .await;
    if current_modified.is_err() {
        error!("Failed to get current modified");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to get current modified".to_string())
            .unwrap();
    }
    let current_modified = current_modified.unwrap();

    // If file doesn't exist, return 404
    if current_modified == 0 {
        debug!("File {} not found", path);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("File not found".to_string())
            .unwrap();
    }

    // 4. If trying to delete newer version, ignore (return 200 but don't delete)
    if current_modified > timestamp {
        info!(
            "Tried to delete newer version of {} ({} < {}), ignoring.",
            path, timestamp, current_modified
        );
        return Response::builder()
            .status(StatusCode::OK)
            .body("".to_string())
            .unwrap();
    }

    // 5. Get the hash for this path
    let hash = state
        .kvstorage
        .lock()
        .await
        .get_ref_file(&state.bucket_name, path)
        .await;
    if hash.is_err() {
        error!("Failed to get ref file");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to get ref file".to_string())
            .unwrap();
    }
    let hash = hash.unwrap();

    if hash.is_empty() {
        error!("File {} has no hash reference", path);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("File has no hash reference".to_string())
            .unwrap();
    }

    // 6. Decrement reference count
    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .decrement_ref_count(&state.bucket_name, &hash)
        .await
    {
        error!("Failed to decrement ref count: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to decrement ref count".to_string())
            .unwrap();
    }

    // 7. Check if we should delete the blob (ref count is now 0)
    let ref_count = state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&state.bucket_name, &hash)
        .await;
    if ref_count.is_ok() && ref_count.unwrap() <= 0 {
        debug!("Deleting blob with hash: {}", hash);
        // Delete blob from S3
        if let Err(e) = state.s3storage.lock().await.delete_object(&hash).await {
            error!("Failed to delete object from S3: {}", e);
            // Continue anyway - metadata cleanup is more important
        }

        // Delete logical size metadata
        let _ = state
            .kvstorage
            .lock()
            .await
            .set_logical_size(&state.bucket_name, &hash, 0)
            .await;
    }

    // 8. Delete file metadata (path -> hash mapping and timestamp)
    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .delete_ref_file(&state.bucket_name, path)
        .await
    {
        error!("Failed to delete ref file: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to delete ref file".to_string())
            .unwrap();
    }

    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .delete_modified(&state.bucket_name, path)
        .await
    {
        error!("Failed to delete modified time: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to delete modified time".to_string())
            .unwrap();
    }

    debug!("Deleted file {}", path);

    // 9. Dual-delete from filetracker if in live migration mode
    if let Some(filetracker_client) = &state.filetracker_client {
        debug!("Live migration mode: also deleting from filetracker");

        let result = filetracker_client.delete_file(path, timestamp).await;

        if let Err(e) = result {
            error!(
                "Failed to delete from filetracker during live migration: {}",
                e
            );
            // Continue anyway - s3dedup is primary storage
        } else {
            debug!("Successfully deleted from filetracker");
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .body("".to_string())
        .unwrap()
}
