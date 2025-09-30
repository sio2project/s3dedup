use crate::{locks, AppState};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use axum::body::Body;
use std::sync::Arc;
use tracing::{debug, error, info};
use crate::routes::ft::{utils, LastModifiedQuery, storage_helpers};


pub async fn ft_put_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    Query(query): Query<LastModifiedQuery>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    // Remove leading slash from wildcard path
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("Handling PUT for path: {}", path);
    debug!("timestamp: {}", query.last_modified);

    // 1. Parse and validate timestamp
    let timestamp = utils::conv_rfc2822_to_unix_timestamp(&query.last_modified);
    if timestamp.is_err() {
        error!("Failed to parse last_modified");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Failed to parse last_modified".to_string())
            .unwrap();
    }
    let timestamp = timestamp.unwrap();

    // 2. Extract headers (matching original filetracker behavior)
    let compressed = headers.get("content-encoding")
        .map(|v| v.to_str().unwrap_or("") == "gzip")
        .unwrap_or(false);
    let provided_digest = headers.get("sha256-checksum")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let provided_logical_size = headers.get("logical-size")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());

    // Log unusual headers like original
    if compressed && provided_digest.is_some() && provided_logical_size.is_some() {
        debug!("Handling PUT {}.", path);
    } else {
        info!(
            "Handling PUT {} with unusual headers: compressed={}, digest={:?}, logical_size={:?}",
            path, compressed, provided_digest, provided_logical_size
        );
    }

    // 3. Read body
    let body_bytes = match storage_helpers::read_body_bytes(body).await {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Failed to read request body: {}", e);
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Failed to read request body".to_string())
                .unwrap();
        }
    };

    // 4. Acquire file lock
    let lock_key = locks::file_lock(&state.bucket_name, &path);
    state.locks.lock().await.acquire_exclusive(&lock_key);

    // 5. Check existing version (matching original logic)
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

    // If trying to store older version, ignore (return current version)
    if current_modified > timestamp {
        info!(
            "Tried to store older version of {} ({} < {}), ignoring.",
            path, timestamp, current_modified
        );
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .header("Last-Modified", utils::format_rfc2822_timestamp(current_modified))
            .body("".to_string())
            .unwrap();
    }

    // 6. Compute hash and logical size if not provided (matching original)
    let (digest, logical_size, final_data) = if compressed && provided_digest.is_some() && provided_logical_size.is_some() {
        // Use provided values, data is already compressed
        (provided_digest.unwrap(), provided_logical_size.unwrap(), body_bytes.to_vec())
    } else {
        // Handle data processing like original filetracker
        let uncompressed_data = if compressed {
            match storage_helpers::decompress_gzip(&body_bytes) {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to decompress gzip data: {}", e);
                    state.locks.lock().await.release(&lock_key);
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to decompress gzip data".to_string())
                        .unwrap();
                }
            }
        } else {
            body_bytes.to_vec()
        };

        let computed_digest = storage_helpers::compute_sha256(&uncompressed_data);
        let logical_size = uncompressed_data.len();

        // Always store compressed (matching original behavior)
        let final_data = if compressed {
            body_bytes.to_vec() // Already compressed
        } else {
            match storage_helpers::compress_gzip(&uncompressed_data) {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to compress data: {}", e);
                    state.locks.lock().await.release(&lock_key);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to compress data".to_string())
                        .unwrap();
                }
            }
        };

        (computed_digest, logical_size, final_data)
    };

    // 7. Handle deduplication (use hash directly as S3 key for better performance)
    let s3_key = &digest;

    // Check if blob already exists
    let blob_exists = match state.s3storage.lock().await.object_exists(&s3_key).await {
        Ok(exists) => exists,
        Err(e) => {
            error!("Failed to check object existence: {}", e);
            state.locks.lock().await.release(&lock_key);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to check object existence".to_string())
                .unwrap();
        }
    };

    // Update reference count and store blob if needed (matching original transaction order)
    if !blob_exists {
        debug!("Creating new blob.");
        // Store blob in S3
        if let Err(e) = state.s3storage.lock().await.put_object(&s3_key, final_data).await {
            error!("Failed to store object in S3: {}", e);
            state.locks.lock().await.release(&lock_key);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to store object".to_string())
                .unwrap();
        }

        // Store logical size metadata
        if let Err(e) = state.kvstorage.lock().await.set_logical_size(&state.bucket_name, &digest, logical_size).await {
            error!("Failed to store logical size: {}", e);
            state.locks.lock().await.release(&lock_key);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to store logical size".to_string())
                .unwrap();
        }
    }

    // Increment reference count
    if let Err(e) = state.kvstorage.lock().await.increment_ref_count(&state.bucket_name, &digest).await {
        error!("Failed to increment ref count: {}", e);
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to increment ref count".to_string())
            .unwrap();
    }

    // If overwriting existing file, handle deletion of old blob reference
    if current_modified > 0 {
        // Get old hash to decrement its reference count
        if let Ok(old_hash) = state.kvstorage.lock().await.get_ref_file(&state.bucket_name, &path).await {
            if !old_hash.is_empty() && old_hash != digest {
                info!("Overwriting existing link {}.", path);
                // Decrement old reference count
                let _ = state.kvstorage.lock().await.decrement_ref_count(&state.bucket_name, &old_hash).await;

                // Check if we should delete the old blob
                if let Ok(old_ref_count) = state.kvstorage.lock().await.get_ref_count(&state.bucket_name, &old_hash).await {
                    if old_ref_count <= 0 {
                        debug!("Deleting unused blob: {}", old_hash);
                        let _ = state.s3storage.lock().await.delete_object(&old_hash).await;
                    }
                }
            }
        }
    }

    // 8. Update file metadata (path -> hash mapping and timestamp)
    if let Err(e) = state.kvstorage.lock().await.set_ref_file(&state.bucket_name, &path, &digest).await {
        error!("Failed to set ref file: {}", e);
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to set ref file".to_string())
            .unwrap();
    }

    if let Err(e) = state.kvstorage.lock().await.set_modified(&state.bucket_name, &path, timestamp).await {
        error!("Failed to set modified time: {}", e);
        state.locks.lock().await.release(&lock_key);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to set modified time".to_string())
            .unwrap();
    }

    debug!("Created link {}.", path);

    // 9. Release lock and return (matching original response format)
    state.locks.lock().await.release(&lock_key);

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain")
        .header("Last-Modified", utils::format_rfc2822_timestamp(timestamp))
        .body("".to_string())
        .unwrap()
}
