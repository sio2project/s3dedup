use crate::routes::ft::{LastModifiedQuery, utils};
use crate::{AppState, locks, metrics};
use axum::extract::{Path, Query, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

pub async fn ft_delete_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    Query(query): Query<LastModifiedQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let start = Instant::now();

    // Helper to record metrics before returning
    let record_metrics = |status: &str| {
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["DELETE", "/ft/files", status])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["DELETE", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());
    };

    // Remove leading slash from wildcard path
    let path = path.strip_prefix('/').unwrap_or(&path);

    debug!("DELETE request for path: {}", path);
    debug!("Query params: last_modified={:?}", query.last_modified);
    debug!("Headers: last-modified={:?}", headers.get("last-modified"));

    // 1. Parse and validate timestamp (required for DELETE)
    let timestamp = match utils::extract_timestamp(&headers, query.last_modified.as_ref(), true) {
        Ok(ts) => ts,
        Err(e) => {
            error!("Failed to extract timestamp: {}", e);
            record_metrics("400");
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
    let guard = match lock.acquire_exclusive().await {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to acquire exclusive lock: {}", e);
            record_metrics("500");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to acquire lock".to_string())
                .unwrap();
        }
    };

    // 3. Check if file exists
    let current_modified = state
        .kvstorage
        .lock()
        .await
        .get_modified(&state.bucket_name, path)
        .await;
    if current_modified.is_err() {
        error!("Failed to get current modified");
        record_metrics("500");
        let _ = guard.release().await;
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to get current modified".to_string())
            .unwrap();
    }
    let current_modified = current_modified.unwrap();

    // If file doesn't exist, return 404
    if current_modified == 0 {
        debug!("File {} not found", path);
        record_metrics("404");
        let _ = guard.release().await;
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
        record_metrics("200");
        let _ = guard.release().await;
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
        record_metrics("500");
        let _ = guard.release().await;
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to get ref file".to_string())
            .unwrap();
    }
    let hash = hash.unwrap();

    if hash.is_empty() {
        error!("File {} has no hash reference", path);
        record_metrics("404");
        let _ = guard.release().await;
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("File has no hash reference".to_string())
            .unwrap();
    }

    // 6. Decrement reference count atomically and get new count
    let ref_count = match state
        .kvstorage
        .lock()
        .await
        .decrement_ref_count(&state.bucket_name, &hash)
        .await
    {
        Ok(count) => count,
        Err(e) => {
            error!("Failed to decrement ref count: {}", e);
            record_metrics("500");
            let _ = guard.release().await;
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to decrement ref count".to_string())
                .unwrap();
        }
    };

    // 7. Check if we should delete the blob (ref count is now 0)
    if ref_count <= 0 {
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
        record_metrics("500");
        let _ = guard.release().await;
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
        record_metrics("500");
        let _ = guard.release().await;
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to delete modified time".to_string())
            .unwrap();
    }

    debug!("Deleted file {}", path);

    // Release lock early since all critical metadata operations are complete
    let _ = guard.release().await;

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

    record_metrics("200");
    Response::builder()
        .status(StatusCode::OK)
        .body("".to_string())
        .unwrap()
}
