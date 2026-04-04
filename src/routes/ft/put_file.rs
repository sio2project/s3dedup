use crate::routes::ft::{LastModifiedQuery, MetricsRecorder, storage_helpers, utils};
use crate::{AppState, locks, metrics};
use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub async fn ft_put_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    Query(query): Query<LastModifiedQuery>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    let record_metrics = MetricsRecorder::new("PUT", "/ft/files");
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("PUT request for path: {}", path);
    debug!("Query params: last_modified={:?}", query.last_modified);
    debug!(
        "Headers: content-encoding={:?}, sha256-checksum={:?}, logical-size={:?}, content-length={:?}, last-modified={:?}",
        headers.get("content-encoding"),
        headers.get("sha256-checksum"),
        headers.get("logical-size"),
        headers.get("content-length"),
        headers.get("last-modified")
    );

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

    match ft_put_file_inner(&state, path, timestamp, &headers, body).await {
        Ok(response) => {
            let status = response.status().as_u16().to_string();
            record_metrics.record(&status);
            response
        }
        Err(e) => {
            error!("PUT {} failed: {}", path, e);
            record_metrics.record("500");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
                .unwrap()
        }
    }
}

async fn ft_put_file_inner(
    state: &AppState,
    path: &str,
    timestamp: i64,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response<String>> {
    // Extract headers (matching original filetracker behavior)
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

    // ====================================================================
    // FAST PATH: compressed with all headers provided and no dual-write.
    // Acquire locks BEFORE reading body to skip body read on dedup hit.
    //
    // TRUST ASSUMPTION: The client-provided SHA256-Checksum is used as the
    // S3 key without verification. This is by design for the Filetracker v2
    // protocol, which assumes trusted clients. On dedup hit the body is never
    // read, so a lying client could point a path at arbitrary existing content.
    // If untrusted clients become a concern, add hash verification on dedup miss.
    // ====================================================================
    if let (true, Some(digest), Some(logical_size)) =
        (compressed, provided_digest.clone(), provided_logical_size)
        && state.filetracker_client.is_none()
    {
        // Acquire file lock before reading body
        let lock_key = locks::file_lock(&state.bucket_name, path);
        let locks_storage = &state.locks;
        let lock = locks_storage.prepare_lock(lock_key).await;
        let guard = lock
            .acquire_exclusive()
            .await
            .context("Failed to acquire exclusive lock")?;

        // Check existing version
        let current_modified = state
            .kvstorage
            .get_modified(&state.bucket_name, path)
            .await
            .context("Failed to get current modified")?;

        // If trying to store older version, return early — body never read
        if current_modified > timestamp {
            info!(
                "Tried to store older version of {} ({} < {}), ignoring.",
                path, timestamp, current_modified
            );
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .header(
                    "Last-Modified",
                    utils::format_rfc2822_timestamp(current_modified),
                )
                .body("".to_string())
                .unwrap());
        }

        // Acquire hash lock
        let hash_lock_key = locks::hash_lock(&state.bucket_name, &digest);
        let hash_lock = locks_storage.prepare_lock(hash_lock_key).await;
        let hash_guard = hash_lock
            .acquire_exclusive()
            .await
            .context("Failed to acquire hash lock")?;

        // Check dedup
        let blob_exists = state
            .s3storage
            .object_exists(&digest)
            .await
            .context("Failed to check object existence")?;

        if blob_exists {
            metrics::DEDUP_HITS_TOTAL
                .with_label_values(&[&state.bucket_name])
                .inc();
        } else {
            metrics::DEDUP_MISSES_TOTAL
                .with_label_values(&[&state.bucket_name])
                .inc();
        }

        // Upload to S3 if needed. On dedup hit, body is dropped unread.
        let compressed_size: Option<usize> = if !blob_exists {
            // Check Content-Length to decide: buffer in memory or stream via temp file
            let fast_content_length: Option<usize> = headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok());
            let fast_use_tempfile =
                fast_content_length.is_none_or(|len| len > state.max_inmemory_size);

            let (compressed_size, _fast_temp_file) = if fast_use_tempfile {
                // Large file: stream body to temp file, upload from disk
                let raw =
                    match storage_helpers::stream_body_to_temp_file(body, &state.temp_dir).await {
                        Ok(r) => r,
                        Err(e) => {
                            error!("Failed to stream request body: {}", e);
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body("Failed to stream request body".to_string())
                                .unwrap());
                        }
                    };
                let compressed_size = raw.data_size;
                let byte_stream = aws_sdk_s3::primitives::ByteStream::from_path(&raw.temp_path)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to open temp file for S3 upload: {}", e)
                    })?;
                state
                    .s3storage
                    .put_object_stream(&digest, byte_stream, Some(compressed_size as i64))
                    .await
                    .context("Failed to store object in S3")?;
                (compressed_size, Some(raw))
            } else {
                // Small file: buffer in memory and upload directly
                let body_bytes =
                    match storage_helpers::read_body_bytes(body, state.max_inmemory_size).await {
                        Ok(b) => b,
                        Err(e) => {
                            error!("Failed to read request body: {}", e);
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body("Failed to read request body".to_string())
                                .unwrap());
                        }
                    };
                let compressed_size = body_bytes.len();
                let byte_stream = aws_sdk_s3::primitives::ByteStream::from(body_bytes);
                state
                    .s3storage
                    .put_object_stream(&digest, byte_stream, Some(compressed_size as i64))
                    .await
                    .context("Failed to store object in S3")?;
                (compressed_size, None)
            };

            Some(compressed_size)
        } else {
            // Dedup hit — body dropped unread (key memory optimization).
            // Don't update compressed_size — it's already set from the first upload.
            drop(body);
            None
        };

        // Handle refcount — get old_hash before updating metadata
        let old_hash = if current_modified > 0 {
            state
                .kvstorage
                .get_ref_file(&state.bucket_name, path)
                .await
                .ok()
        } else {
            None
        };

        // Record blob metadata: logical size, compressed size, and conditionally increment refcount
        let new_refcount = state
            .record_blob_metadata(&digest, logical_size, compressed_size, old_hash.as_deref())
            .await
            .context("Failed to record blob metadata")?;

        // Update file metadata while still holding hash lock (same reason as slow path)
        state
            .update_file_ref(path, &digest, timestamp)
            .await
            .context("Failed to update file reference")?;

        // Release hash lock — done with S3/refcount/ref_file operations for new hash
        let _ = hash_guard.release().await;

        // Build stats delta
        let mut delta = crate::kvstorage::StatsDelta::default();
        if let Some(rc) = new_refcount {
            let cs = match compressed_size {
                Some(cs) => cs as i64,
                None => state.get_blob_sizes(&digest).await.1,
            };
            delta = crate::kvstorage::StatsDelta::for_ref_increment(rc, logical_size as i64, cs);
        }
        if current_modified == 0 {
            delta.total_files = 1;
        }

        // Handle old hash decrement
        if let Some(old_hash) = old_hash {
            let (old_ls, old_cs) = state.get_blob_sizes(&old_hash).await;
            match state.decrement_old_ref(&old_hash, &digest).await {
                Ok(Some(old_rc)) => {
                    delta.merge(&crate::kvstorage::StatsDelta::for_ref_decrement(
                        old_rc, old_ls, old_cs,
                    ));
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        "Failed to decrement old ref for {}, refcount may be leaked (cleaner will reclaim): {}",
                        old_hash, e
                    );
                }
            }
        }

        if let Err(e) = state
            .kvstorage
            .adjust_stats(&state.bucket_name, &delta)
            .await
        {
            warn!("Failed to adjust stats: {}", e);
        }

        debug!("Created link {} (fast path).", path);

        // Release file lock — all critical metadata operations complete
        let _ = guard.release().await;

        let last_modified_header = utils::format_rfc2822_timestamp(timestamp);
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .header("Last-Modified", last_modified_header)
            .body("".to_string())
            .unwrap());
    }

    // ====================================================================
    // SLOW PATH: used when headers are missing or dual-write is enabled.
    // Small files (below max_inmemory_size) are processed in memory.
    // Large files are spilled to temp files to bound memory usage.
    // The threshold applies to ALL sub-paths (with headers, without headers).
    // ====================================================================

    // Determine if headers provide digest/size (dual-write slow path with headers)
    let has_headers = compressed && provided_digest.is_some() && provided_logical_size.is_some();

    // Use Content-Length to decide in-memory vs temp file.
    // Unknown size (missing Content-Length) → use temp file (safe default for large uploads).
    let content_length_hint: Option<usize> = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let use_tempfile = content_length_hint.is_none_or(|len| len > state.max_inmemory_size);

    // 3. Process body
    let (digest, logical_size, compressed_size, processed_file, final_data_for_dual_write) =
        if has_headers && !use_tempfile {
            // Headers provided, small file — buffer in memory
            let body_bytes =
                match storage_helpers::read_body_bytes(body, state.max_inmemory_size).await {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Failed to read request body: {}", e);
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Failed to read request body".to_string())
                            .unwrap());
                    }
                };
            let final_data = body_bytes.to_vec();
            let compressed_size = final_data.len();
            (
                provided_digest.unwrap(),
                provided_logical_size.unwrap(),
                compressed_size,
                None,
                Some(final_data),
            )
        } else if has_headers {
            // Headers provided, large file — stream to temp file to bound memory
            match storage_helpers::stream_body_to_temp_file(body, &state.temp_dir).await {
                Ok(raw) => {
                    let compressed_size = raw.data_size;
                    let pf = storage_helpers::ProcessedFile {
                        digest: provided_digest.clone().unwrap(),
                        logical_size: provided_logical_size.unwrap(),
                        compressed_size,
                        _temp_file: Some(raw._temp_file),
                        compressed_path: raw.temp_path,
                    };
                    (
                        provided_digest.unwrap(),
                        provided_logical_size.unwrap(),
                        compressed_size,
                        Some(pf),
                        None,
                    )
                }
                Err(e) => {
                    error!("Failed to stream request body: {}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to stream request body".to_string())
                        .unwrap());
                }
            }
        } else if use_tempfile {
            // Large file: stream body to temp file to bound memory usage
            match storage_helpers::process_body_to_temp_file(body, compressed, &state.temp_dir)
                .await
            {
                Ok(pf) => {
                    let digest = pf.digest.clone();
                    let logical_size = pf.logical_size;
                    let compressed_size = pf.compressed_size;
                    (digest, logical_size, compressed_size, Some(pf), None)
                }
                Err(e) => {
                    error!("Failed to process request body: {}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to process request body".to_string())
                        .unwrap());
                }
            }
        } else {
            // Small file (Content-Length ≤ threshold): process in memory (faster, no disk I/O)
            let body_bytes =
                match storage_helpers::read_body_bytes(body, state.max_inmemory_size).await {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Failed to read request body: {}", e);
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Failed to read request body".to_string())
                            .unwrap());
                    }
                };

            let uncompressed_data = if compressed {
                match storage_helpers::decompress_gzip(&body_bytes) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to decompress gzip data: {}", e);
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Failed to decompress gzip data".to_string())
                            .unwrap());
                    }
                }
            } else {
                body_bytes.to_vec()
            };

            let computed_digest = storage_helpers::compute_sha256(&uncompressed_data);
            let logical_size = uncompressed_data.len();

            let final_data = if compressed {
                body_bytes.to_vec()
            } else {
                storage_helpers::compress_gzip(&uncompressed_data)
                    .context("Failed to compress data")?
            };

            let compressed_size = final_data.len();
            (
                computed_digest,
                logical_size,
                compressed_size,
                None,
                Some(final_data),
            )
        };

    // 4. Acquire file lock
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let locks_storage = &state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock")?;

    // 5. Check existing version (matching original logic)
    let current_modified = state
        .kvstorage
        .get_modified(&state.bucket_name, path)
        .await
        .context("Failed to get current modified")?;

    // If trying to store older version, ignore (return current version)
    if current_modified > timestamp {
        info!(
            "Tried to store older version of {} ({} < {}), ignoring.",
            path, timestamp, current_modified
        );
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .header(
                "Last-Modified",
                utils::format_rfc2822_timestamp(current_modified),
            )
            .body("".to_string())
            .unwrap());
    }

    // 7. Acquire hash lock for refcount/S3 operations
    let hash_lock_key = locks::hash_lock(&state.bucket_name, &digest);
    let hash_lock = locks_storage.prepare_lock(hash_lock_key).await;
    let hash_guard = hash_lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire hash lock")?;

    // 8. Handle deduplication
    let s3_key = &digest;

    debug!("Checking if blob {} already exists", s3_key);
    let blob_exists = state
        .s3storage
        .object_exists(s3_key)
        .await
        .context("Failed to check object existence")?;

    // Record deduplication metrics
    if blob_exists {
        metrics::DEDUP_HITS_TOTAL
            .with_label_values(&[&state.bucket_name])
            .inc();
    } else {
        metrics::DEDUP_MISSES_TOTAL
            .with_label_values(&[&state.bucket_name])
            .inc();
    }

    // Upload to S3 if blob doesn't exist
    if !blob_exists {
        debug!("Creating new blob.");
        let upload_result = if let Some(ref data) = final_data_for_dual_write {
            // Buffered data available (dual-write path)
            state.s3storage.put_object(s3_key, data.clone()).await
        } else if let Some(ref pf) = processed_file {
            // Stream from temp file (no memory buffering)
            let byte_stream =
                aws_sdk_s3::primitives::ByteStream::from_path(&pf.compressed_path).await;
            match byte_stream {
                Ok(bs) => {
                    state
                        .s3storage
                        .put_object_stream(s3_key, bs, Some(compressed_size as i64))
                        .await
                }
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to open temp file for S3 upload: {}",
                    e
                )),
            }
        } else {
            Err(anyhow::anyhow!("No data available for S3 upload"))
        };

        upload_result.context("Failed to store object in S3")?;
    }

    // If overwriting existing file, get old hash for reference count management
    let old_hash = if current_modified > 0 {
        state
            .kvstorage
            .get_ref_file(&state.bucket_name, path)
            .await
            .ok()
    } else {
        None
    };

    // Record blob metadata: logical size, compressed size, and conditionally increment refcount
    let new_refcount = state
        .record_blob_metadata(
            &digest,
            logical_size,
            Some(compressed_size),
            old_hash.as_deref(),
        )
        .await
        .context("Failed to record blob metadata")?;

    // 9. Update file metadata while still holding hash lock, so that
    // ref_file is set atomically with the refcount increment. This prevents
    // a race where the cleaner sees a positive refcount with no ref_file
    // (which looks like a crash orphan) during the window between releasing
    // the hash lock and setting ref_file.
    state
        .update_file_ref(path, &digest, timestamp)
        .await
        .context("Failed to update file reference")?;

    // Release hash lock — done with S3/refcount/ref_file operations for new hash
    let _ = hash_guard.release().await;

    // Build stats delta
    let mut delta = if let Some(rc) = new_refcount {
        crate::kvstorage::StatsDelta::for_ref_increment(
            rc,
            logical_size as i64,
            compressed_size as i64,
        )
    } else {
        crate::kvstorage::StatsDelta::default()
    };
    if current_modified == 0 {
        delta.total_files = 1;
    }

    // If overwriting with different content, decrement old blob reference
    if let Some(old_hash) = old_hash {
        let (old_ls, old_cs) = state.get_blob_sizes(&old_hash).await;
        match state.decrement_old_ref(&old_hash, &digest).await {
            Ok(Some(old_rc)) => {
                delta.merge(&crate::kvstorage::StatsDelta::for_ref_decrement(
                    old_rc, old_ls, old_cs,
                ));
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "Failed to decrement old ref for {}, refcount may be leaked (cleaner will reclaim): {}",
                    old_hash, e
                );
            }
        }
    }

    if let Err(e) = state
        .kvstorage
        .adjust_stats(&state.bucket_name, &delta)
        .await
    {
        warn!("Failed to adjust stats: {}", e);
    }

    debug!("Created link {}.", path);

    // Release file lock — all critical metadata operations complete
    let _ = guard.release().await;

    // 10. Dual-write to filetracker if in live migration mode
    if let Some(filetracker_client) = &state.filetracker_client {
        debug!("Live migration mode: also writing to filetracker");

        // Get compressed data from either buffered path or temp file
        let compressed_data = if let Some(ref data) = final_data_for_dual_write {
            Some(data.clone())
        } else if let Some(ref pf) = processed_file {
            tokio::fs::read(&pf.compressed_path).await.ok()
        } else {
            None
        };

        if let Some(compressed_data) = compressed_data {
            // V1 filetracker doesn't understand compression - it stores files uncompressed.
            // If decompression fails, skip dual-write entirely to avoid storing corrupt data.
            let uncompressed_for_v1 = match storage_helpers::decompress_gzip(&compressed_data) {
                Ok(data) => data,
                Err(e) => {
                    error!(
                        "Failed to decompress for V1 filetracker dual-write, skipping: {}",
                        e
                    );
                    // Don't send compressed bytes to V1 — it would store garbage.
                    // s3dedup write already succeeded, just skip the dual-write.
                    let last_modified_header = utils::format_rfc2822_timestamp(timestamp);
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/plain")
                        .header("Last-Modified", last_modified_header)
                        .body("".to_string())
                        .unwrap());
                }
            };

            let result = filetracker_client
                .put_file(
                    path,
                    uncompressed_for_v1,
                    timestamp,
                    logical_size,
                    &digest,
                    false, // V1 filetracker stores uncompressed
                )
                .await;

            if let Err(e) = result {
                error!(
                    "Failed to write to filetracker during live migration: {}",
                    e
                );
            } else {
                debug!("Successfully wrote to filetracker");
            }
        } else {
            warn!("No data available for filetracker dual-write");
        }
    }

    let last_modified_header = utils::format_rfc2822_timestamp(timestamp);
    debug!(
        "PUT {} complete, returning Last-Modified: {} (unix: {})",
        path, last_modified_header, timestamp
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain")
        .header("Last-Modified", last_modified_header)
        .body("".to_string())
        .unwrap())
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
