use crate::routes::ft::{LastModifiedQuery, MetricsRecorder, utils};
use crate::{AppState, locks};
use anyhow::{Context, Result};
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
    let record_metrics = MetricsRecorder::new("DELETE", "/ft/files");
    let path = path.strip_prefix('/').unwrap_or(&path);

    let timestamp = match utils::extract_timestamp(&headers, query.last_modified.as_ref(), true) {
        Ok(ts) => ts,
        Err(e) => {
            error!("Failed to extract timestamp: {}", e);
            record_metrics.record("400");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(e)
                .unwrap();
        }
    };

    match ft_delete_file_inner(&state, path, timestamp).await {
        Ok(response) => {
            let status = response.status().as_u16().to_string();
            record_metrics.record(&status);
            response
        }
        Err(e) => {
            error!("DELETE {} failed: {}", path, e);
            record_metrics.record("500");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
                .unwrap()
        }
    }
}

async fn ft_delete_file_inner(
    state: &AppState,
    path: &str,
    timestamp: i64,
) -> Result<Response<String>> {
    debug!("Handling DELETE {}@{}", path, timestamp);

    // 1. Acquire file lock (exclusive for write operation)
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let lock = state.locks.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock")?;

    // 2. Check if file exists
    let current_modified = state
        .kvstorage
        .get_modified(&state.bucket_name, path)
        .await
        .context("Failed to get current modified")?;

    if current_modified == 0 {
        debug!("File {} not found", path);
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("File not found".to_string())
            .unwrap());
    }

    // 3. If trying to delete newer version, ignore (return 200 but don't delete)
    if current_modified > timestamp {
        info!(
            "Tried to delete newer version of {} ({} < {}), ignoring.",
            path, timestamp, current_modified
        );
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body("".to_string())
            .unwrap());
    }

    // 4. Get the hash for this path
    let hash = state
        .kvstorage
        .get_ref_file(&state.bucket_name, path)
        .await
        .context("Failed to get ref file")?;

    if hash.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("File has no hash reference".to_string())
            .unwrap());
    }

    // 5. Acquire hash lock before refcount operations
    let hash_lock_key = locks::hash_lock(&state.bucket_name, &hash);
    let hash_lock = state.locks.prepare_lock(hash_lock_key).await;
    let hash_guard = hash_lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire hash lock")?;

    // 6. Decrement reference count atomically and get new count
    let ref_count = state
        .kvstorage
        .atomic_decrement_ref_count(&state.bucket_name, &hash)
        .await
        .context("Failed to decrement ref count")?;

    // 7. Delete the blob if ref count reached 0
    if ref_count <= 0 {
        debug!("Deleting blob with hash: {}", hash);
        if let Err(e) = state.s3storage.delete_object(&hash).await {
            error!(
                "Failed to delete object from S3 (bucket={}, key={}): {}",
                state.bucket_name, hash, e
            );
            // Continue anyway - metadata cleanup is more important
        }

        let _ = state
            .kvstorage
            .set_logical_size(&state.bucket_name, &hash, 0)
            .await;
    }

    // Release hash lock — done with refcount/S3 operations
    let _ = hash_guard.release().await;

    // 8. Delete file metadata (path -> hash mapping and timestamp)
    state
        .kvstorage
        .delete_ref_file(&state.bucket_name, path)
        .await
        .context("Failed to delete ref file")?;

    state
        .kvstorage
        .delete_modified(&state.bucket_name, path)
        .await
        .context("Failed to delete modified time")?;

    debug!("Deleted file {}", path);

    // Release file lock — all critical metadata operations complete
    let _ = guard.release().await;

    // 9. Dual-delete from filetracker if in live migration mode
    if let Some(filetracker_client) = &state.filetracker_client {
        debug!("Live migration mode: also deleting from filetracker");
        if let Err(e) = filetracker_client.delete_file(path, timestamp).await {
            error!(
                "Failed to delete from filetracker during live migration: {}",
                e
            );
            // Continue anyway - s3dedup is primary storage
        } else {
            debug!("Successfully deleted from filetracker");
        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("".to_string())
        .unwrap())
}
