use crate::routes::ft::{LastModifiedQuery, storage_helpers, utils};
use crate::{AppState, locks};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error, info};

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
    let compressed = headers
        .get("content-encoding")
        .map(|v| v.to_str().unwrap_or("") == "gzip")
        .unwrap_or(false);
    let provided_digest = headers
        .get("sha256-checksum")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let provided_logical_size = headers
        .get("logical-size")
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
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let locks_storage = &state.locks;
    let lock = locks_storage.prepare_lock(lock_key);
    let _guard = lock.acquire_exclusive().await;

    // 5. Check existing version (matching original logic)
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

    // If trying to store older version, ignore (return current version)
    if current_modified > timestamp {
        info!(
            "Tried to store older version of {} ({} < {}), ignoring.",
            path, timestamp, current_modified
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .header(
                "Last-Modified",
                utils::format_rfc2822_timestamp(current_modified),
            )
            .body("".to_string())
            .unwrap();
    }

    // 6. Compute hash and logical size if not provided (matching original)
    let (digest, logical_size, final_data) = if let (true, Some(digest), Some(size)) =
        (compressed, provided_digest, provided_logical_size)
    {
        // Use provided values, data is already compressed
        (digest, size, body_bytes.to_vec())
    } else {
        // Handle data processing like original filetracker
        let uncompressed_data = if compressed {
            match storage_helpers::decompress_gzip(&body_bytes) {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to decompress gzip data: {}", e);
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

    debug!("Checking if blob {} already exists", s3_key);
    // Check if blob already exists
    let blob_exists = match state.s3storage.lock().await.object_exists(s3_key).await {
        Ok(exists) => exists,
        Err(e) => {
            error!("Failed to check object existence: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to check object existence".to_string())
                .unwrap();
        }
    };

    // Update reference count and store blob if needed (matching original transaction order)
    if !blob_exists {
        debug!("Creating new blob.");
        // Store blob in S3 (clone data before moving it in case we need it for dual-write)
        if let Err(e) = state
            .s3storage
            .lock()
            .await
            .put_object(s3_key, final_data.clone())
            .await
        {
            error!("Failed to store object in S3: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to store object".to_string())
                .unwrap();
        }
    }

    // Store logical size metadata (always, in case KV metadata was lost but S3 blob still exists)
    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .set_logical_size(&state.bucket_name, &digest, logical_size)
        .await
    {
        error!("Failed to store logical size: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to store logical size".to_string())
            .unwrap();
    }

    // Increment reference count
    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .increment_ref_count(&state.bucket_name, &digest)
        .await
    {
        error!("Failed to increment ref count: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to increment ref count".to_string())
            .unwrap();
    }

    // If overwriting existing file, handle deletion of old blob reference
    if current_modified > 0 {
        // Get old hash to decrement its reference count (acquire lock, get value, release)
        let old_hash_result = state
            .kvstorage
            .lock()
            .await
            .get_ref_file(&state.bucket_name, path)
            .await;

        if let Ok(old_hash) = old_hash_result
            && !old_hash.is_empty()
            && old_hash != digest
        {
            info!(
                "Overwriting existing link {}. Old hash: {}, new hash: {}",
                path, old_hash, digest
            );
            // Decrement old reference count
            let _ = state
                .kvstorage
                .lock()
                .await
                .decrement_ref_count(&state.bucket_name, &old_hash)
                .await;

            // Check if we should delete the old blob
            let old_ref_count_result = state
                .kvstorage
                .lock()
                .await
                .get_ref_count(&state.bucket_name, &old_hash)
                .await;

            if let Ok(old_ref_count) = old_ref_count_result
                && old_ref_count <= 0
            {
                debug!("Deleting unused blob: {}", old_hash);
                let _ = state.s3storage.lock().await.delete_object(&old_hash).await;
            }
        }
    }

    // 8. Update file metadata (path -> hash mapping and timestamp)
    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .set_ref_file(&state.bucket_name, path, &digest)
        .await
    {
        error!("Failed to set ref file: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to set ref file".to_string())
            .unwrap();
    }

    if let Err(e) = state
        .kvstorage
        .lock()
        .await
        .set_modified(&state.bucket_name, path, timestamp)
        .await
    {
        error!("Failed to set modified time: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to set modified time".to_string())
            .unwrap();
    }

    debug!("Created link {}.", path);

    // 9. Dual-write to filetracker if in live migration mode
    if let Some(filetracker_client) = &state.filetracker_client {
        debug!("Live migration mode: also writing to filetracker");

        // Reconstruct the data that needs to be sent to filetracker
        // We need to use the final_data (compressed) that was stored
        let result = filetracker_client
            .put_file(
                path,
                final_data.clone(),
                timestamp,
                logical_size,
                &digest,
                true, // Always compressed in storage
            )
            .await;

        if let Err(e) = result {
            error!(
                "Failed to write to filetracker during live migration: {}",
                e
            );
            // Continue anyway - s3dedup is primary storage
        } else {
            debug!("Successfully wrote to filetracker");
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain")
        .header("Last-Modified", utils::format_rfc2822_timestamp(timestamp))
        .body("".to_string())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::routes::ft::utils;

    #[tokio::test]
    async fn test_timestamp_conversion() {
        let timestamp = "Mon, 01 Jan 2024 12:00:00 GMT";
        let result = utils::conv_rfc2822_to_unix_timestamp(timestamp);
        assert!(result.is_ok());

        let invalid_timestamp = "invalid-timestamp";
        let result = utils::conv_rfc2822_to_unix_timestamp(invalid_timestamp);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_path_processing() {
        // Test path processing logic
        let path = "/test.txt";
        let stripped = path.strip_prefix('/').unwrap_or(path);
        assert_eq!(stripped, "test.txt");

        let path = "test.txt";
        let stripped = path.strip_prefix('/').unwrap_or(path);
        assert_eq!(stripped, "test.txt");

        // Test subdirectory paths
        let path = "/subdir/deep/test.txt";
        let stripped = path.strip_prefix('/').unwrap_or(path);
        assert_eq!(stripped, "subdir/deep/test.txt");
    }

    #[tokio::test]
    async fn test_header_parsing() {
        // Test header parsing logic similar to what's in the PUT handler
        use axum::http::HeaderMap;

        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        headers.insert("sha256-checksum", "abc123".parse().unwrap());
        headers.insert("logical-size", "1024".parse().unwrap());

        let compressed = headers
            .get("content-encoding")
            .map(|v| v.to_str().unwrap_or("") == "gzip")
            .unwrap_or(false);
        assert!(compressed);

        let provided_digest = headers
            .get("sha256-checksum")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        assert_eq!(provided_digest, Some("abc123".to_string()));

        let provided_logical_size = headers
            .get("logical-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok());
        assert_eq!(provided_logical_size, Some(1024));
    }

    #[tokio::test]
    async fn test_storage_helpers() {
        use crate::routes::ft::storage_helpers;

        let test_data = b"Hello, World! This is test data for compression.";

        // Test SHA256 computation
        let hash = storage_helpers::compute_sha256(test_data);
        assert_eq!(hash.len(), 64); // SHA256 produces 64 hex characters
        assert!(!hash.is_empty());

        // Test compression and decompression
        let compressed = storage_helpers::compress_gzip(test_data).unwrap();
        assert!(compressed.len() < test_data.len() + 100); // Should be reasonable size

        let decompressed = storage_helpers::decompress_gzip(&compressed).unwrap();
        assert_eq!(decompressed, test_data);

        // Test idempotency - same data should produce same hash
        let hash2 = storage_helpers::compute_sha256(test_data);
        assert_eq!(hash, hash2);
    }
}
