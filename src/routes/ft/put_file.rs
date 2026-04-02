use crate::routes::ft::{LastModifiedQuery, storage_helpers, utils};
use crate::{AppState, locks, metrics};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

pub async fn ft_put_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    Query(query): Query<LastModifiedQuery>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    let start = Instant::now();

    // Remove leading slash from wildcard path
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

    // Helper to record metrics before returning
    let record_metrics = |status: &str| {
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["PUT", "/ft/files", status])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["PUT", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());
    };

    // 1. Parse and validate timestamp (required for PUT)
    let raw_timestamp = headers
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| query.last_modified.clone());
    let timestamp = match utils::extract_timestamp(&headers, query.last_modified.as_ref(), true) {
        Ok(ts) => {
            debug!(
                "PUT {} timestamp: raw='{}' -> parsed={}",
                path,
                raw_timestamp.as_deref().unwrap_or("none"),
                ts
            );
            ts
        }
        Err(e) => {
            error!("Failed to extract timestamp: {}", e);
            record_metrics("400");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(e)
                .unwrap();
        }
    };

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

        // Check existing version
        let current_modified = match state.kvstorage.get_modified(&state.bucket_name, path).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to get current modified: {}", e);
                record_metrics("500");
                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to get current modified".to_string())
                    .unwrap();
            }
        };

        // If trying to store older version, return early — body never read
        if current_modified > timestamp {
            info!(
                "Tried to store older version of {} ({} < {}), ignoring.",
                path, timestamp, current_modified
            );
            record_metrics("200");
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
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

        // Acquire hash lock
        let hash_lock_key = locks::hash_lock(&state.bucket_name, &digest);
        let hash_lock = locks_storage.prepare_lock(hash_lock_key).await;
        let hash_guard = match hash_lock.acquire_exclusive().await {
            Ok(g) => g,
            Err(e) => {
                error!("Failed to acquire hash lock: {}", e);
                record_metrics("500");
                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to acquire hash lock".to_string())
                    .unwrap();
            }
        };

        // Check dedup
        let blob_exists = match state.s3storage.object_exists(&digest).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("Failed to check object existence: {}", e);
                record_metrics("500");
                if let Err(e) = hash_guard.release().await {
                    warn!("Failed to release hash lock: {}", e);
                }
                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to check object existence".to_string())
                    .unwrap();
            }
        };

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
        if !blob_exists {
            let body_bytes = match storage_helpers::read_body_bytes(body).await {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to read request body: {}", e);
                    record_metrics("400");
                    if let Err(e) = hash_guard.release().await {
                        warn!("Failed to release hash lock: {}", e);
                    }
                    if let Err(e) = guard.release().await {
                        warn!("Failed to release file lock: {}", e);
                    }
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to read request body".to_string())
                        .unwrap();
                }
            };
            let compressed_size = body_bytes.len();
            let byte_stream = aws_sdk_s3::primitives::ByteStream::from(body_bytes);
            if let Err(e) = state
                .s3storage
                .put_object_stream(&digest, byte_stream, Some(compressed_size as i64))
                .await
            {
                error!("Failed to store object in S3: {}", e);
                record_metrics("500");
                if let Err(e) = hash_guard.release().await {
                    warn!("Failed to release hash lock: {}", e);
                }
                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to store object".to_string())
                    .unwrap();
            }

            // Set compressed size (only on dedup miss — on hit, it's already set)
            if let Err(e) = state
                .kvstorage
                .set_compressed_size(&state.bucket_name, &digest, compressed_size)
                .await
            {
                error!("Failed to store compressed size: {}", e);
                record_metrics("500");
                if let Err(e) = hash_guard.release().await {
                    warn!("Failed to release hash lock: {}", e);
                }
                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to store compressed size".to_string())
                    .unwrap();
            }
        } else {
            // Dedup hit — body dropped unread (key memory optimization)
            drop(body);
        }

        // Always set logical_size (idempotent, cheap — repairs metadata if KV was lost).
        // compressed_size is only set on dedup miss above, since it already exists from
        // the first upload and we don't have the body to measure on dedup hit.
        if let Err(e) = state
            .kvstorage
            .set_logical_size(&state.bucket_name, &digest, logical_size)
            .await
        {
            error!("Failed to store logical size: {}", e);
            record_metrics("500");
            if let Err(e) = hash_guard.release().await {
                warn!("Failed to release hash lock: {}", e);
            }
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to store logical size".to_string())
                .unwrap();
        }

        // Handle refcount
        let old_hash = if current_modified > 0 {
            state
                .kvstorage
                .get_ref_file(&state.bucket_name, path)
                .await
                .ok()
        } else {
            None
        };

        let should_increment = match &old_hash {
            Some(old) if !old.is_empty() && old == &digest => {
                debug!("Overwriting {} with same content, keeping refcount", path);
                false
            }
            _ => true,
        };

        if should_increment
            && let Err(e) = state
                .kvstorage
                .atomic_increment_ref_count(&state.bucket_name, &digest)
                .await
        {
            error!("Failed to increment ref count: {}", e);
            record_metrics("500");
            if let Err(e) = hash_guard.release().await {
                warn!("Failed to release hash lock: {}", e);
            }
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to increment ref count".to_string())
                .unwrap();
        }

        if let Err(e) = hash_guard.release().await {
            warn!("Failed to release hash lock: {}", e);
        }

        // Handle old hash decrement
        if let Some(old_hash) = old_hash
            && !old_hash.is_empty()
            && old_hash != digest
        {
            info!(
                "Overwriting existing link {}. Old hash: {}, new hash: {}",
                path, old_hash, digest
            );
            let old_hash_lock_key = locks::hash_lock(&state.bucket_name, &old_hash);
            let old_hash_lock = locks_storage.prepare_lock(old_hash_lock_key).await;
            match old_hash_lock.acquire_exclusive().await {
                Ok(old_hash_guard) => {
                    let old_ref_count_result = state
                        .kvstorage
                        .atomic_decrement_ref_count(&state.bucket_name, &old_hash)
                        .await;
                    if let Ok(old_ref_count) = old_ref_count_result
                        && old_ref_count <= 0
                    {
                        debug!("Deleting unused blob: {}", old_hash);
                        let _ = state.s3storage.delete_object(&old_hash).await;
                    }
                    if let Err(e) = old_hash_guard.release().await {
                        warn!("Failed to release old hash lock: {}", e);
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to acquire old hash lock for {}, refcount may be leaked (cleaner will reclaim): {}",
                        old_hash, e
                    );
                }
            }
        }

        // Update file metadata
        if let Err(e) = state
            .kvstorage
            .set_ref_file(&state.bucket_name, path, &digest)
            .await
        {
            error!("Failed to set ref file: {}", e);
            record_metrics("500");
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to set ref file".to_string())
                .unwrap();
        }

        if let Err(e) = state
            .kvstorage
            .set_modified(&state.bucket_name, path, timestamp)
            .await
        {
            error!("Failed to set modified time: {}", e);
            record_metrics("500");
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to set modified time".to_string())
                .unwrap();
        }

        debug!("Created link {} (fast path).", path);
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }

        record_metrics("200");
        let last_modified_header = utils::format_rfc2822_timestamp(timestamp);
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .header("Last-Modified", last_modified_header)
            .body("".to_string())
            .unwrap();
    }

    // ====================================================================
    // SLOW PATH: used when headers are missing or dual-write is enabled.
    // Small files (below max_inmemory_size) are processed in memory.
    // Large files are spilled to temp files to bound memory usage.
    //
    // NOTE: When dual-write is enabled with headers (has_headers=true),
    // the body is always buffered in memory regardless of max_inmemory_size.
    // This is because dual-write needs the data for decompression before
    // sending to V1 filetracker. Dual-write is a temporary migration feature.
    // ====================================================================

    // Determine if headers provide digest/size (dual-write slow path with headers)
    let has_headers = compressed && provided_digest.is_some() && provided_logical_size.is_some();

    // Use Content-Length to decide in-memory vs temp file for the no-headers path.
    // If Content-Length is absent or below threshold, process in memory (faster).
    let content_length_hint = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let use_tempfile = !has_headers && content_length_hint > state.max_inmemory_size;

    // 3. Process body
    let (digest, logical_size, compressed_size, processed_file, final_data_for_dual_write) =
        if has_headers {
            // Headers provided but dual-write needed — buffer for dual-write
            let body_bytes = match storage_helpers::read_body_bytes(body).await {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to read request body: {}", e);
                    record_metrics("400");
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to read request body".to_string())
                        .unwrap();
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
        } else if use_tempfile {
            // Large file: stream body to temp file to bound memory usage
            match storage_helpers::process_body_to_temp_file(body, compressed).await {
                Ok(pf) => {
                    let digest = pf.digest.clone();
                    let logical_size = pf.logical_size;
                    let compressed_size = pf.compressed_size;
                    (digest, logical_size, compressed_size, Some(pf), None)
                }
                Err(e) => {
                    error!("Failed to process request body: {}", e);
                    record_metrics("400");
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to process request body".to_string())
                        .unwrap();
                }
            }
        } else {
            // Small file (or unknown size): process in memory (faster, no disk I/O)
            let body_bytes = match storage_helpers::read_body_bytes(body).await {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to read request body: {}", e);
                    record_metrics("400");
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Failed to read request body".to_string())
                        .unwrap();
                }
            };

            let uncompressed_data = if compressed {
                match storage_helpers::decompress_gzip(&body_bytes) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to decompress gzip data: {}", e);
                        record_metrics("400");
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

            let final_data = if compressed {
                body_bytes.to_vec()
            } else {
                match storage_helpers::compress_gzip(&uncompressed_data) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to compress data: {}", e);
                        record_metrics("500");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to compress data".to_string())
                            .unwrap();
                    }
                }
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

    // 5. Check existing version (matching original logic)
    let current_modified = state.kvstorage.get_modified(&state.bucket_name, path).await;
    if current_modified.is_err() {
        error!("Failed to get current modified");
        record_metrics("500");
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
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
        record_metrics("200");
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
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

    // 7. Acquire hash lock for refcount/S3 operations
    let hash_lock_key = locks::hash_lock(&state.bucket_name, &digest);
    let hash_lock = locks_storage.prepare_lock(hash_lock_key).await;
    let hash_guard = match hash_lock.acquire_exclusive().await {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to acquire hash lock: {}", e);
            record_metrics("500");
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to acquire hash lock".to_string())
                .unwrap();
        }
    };

    // 8. Handle deduplication
    let s3_key = &digest;

    debug!("Checking if blob {} already exists", s3_key);
    let blob_exists = match state.s3storage.object_exists(s3_key).await {
        Ok(exists) => exists,
        Err(e) => {
            error!(
                "Failed to check object existence for path '{}' (bucket={}, key={}): {}",
                path, state.bucket_name, s3_key, e
            );
            record_metrics("500");
            if let Err(e) = hash_guard.release().await {
                warn!("Failed to release hash lock: {}", e);
            }
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to check object existence".to_string())
                .unwrap();
        }
    };

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

        if let Err(e) = upload_result {
            error!(
                "Failed to store object in S3 for path '{}' (bucket={}, key={}): {}",
                path, state.bucket_name, s3_key, e
            );
            record_metrics("500");
            if let Err(e) = hash_guard.release().await {
                warn!("Failed to release hash lock: {}", e);
            }
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to store object".to_string())
                .unwrap();
        }
    }

    // Store compressed size metadata (always, in case KV metadata was lost but S3 blob still exists)
    if let Err(e) = state
        .kvstorage
        .set_compressed_size(&state.bucket_name, &digest, compressed_size)
        .await
    {
        error!("Failed to store compressed size: {}", e);
        record_metrics("500");
        if let Err(e) = hash_guard.release().await {
            warn!("Failed to release hash lock: {}", e);
        }
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to store compressed size".to_string())
            .unwrap();
    }

    // Store logical size metadata (always, in case KV metadata was lost but S3 blob still exists)
    if let Err(e) = state
        .kvstorage
        .set_logical_size(&state.bucket_name, &digest, logical_size)
        .await
    {
        error!("Failed to store logical size: {}", e);
        record_metrics("500");
        if let Err(e) = hash_guard.release().await {
            warn!("Failed to release hash lock: {}", e);
        }
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to store logical size".to_string())
            .unwrap();
    }

    // If overwriting existing file, handle reference count updates
    let old_hash = if current_modified > 0 {
        state
            .kvstorage
            .get_ref_file(&state.bucket_name, path)
            .await
            .ok()
    } else {
        None
    };

    // Only increment reference count if we're creating a new link or changing to different content
    let should_increment = match &old_hash {
        Some(old) if !old.is_empty() && old == &digest => {
            debug!("Overwriting {} with same content, keeping refcount", path);
            false
        }
        _ => true,
    };

    if should_increment
        && let Err(e) = state
            .kvstorage
            .atomic_increment_ref_count(&state.bucket_name, &digest)
            .await
    {
        error!("Failed to increment ref count: {}", e);
        record_metrics("500");
        if let Err(e) = hash_guard.release().await {
            warn!("Failed to release hash lock: {}", e);
        }
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to increment ref count".to_string())
            .unwrap();
    }

    // Release new hash lock - we're done with S3/refcount operations for new hash
    if let Err(e) = hash_guard.release().await {
        warn!("Failed to release hash lock: {}", e);
    }

    // If overwriting with different content, decrement old blob reference
    if let Some(old_hash) = old_hash
        && !old_hash.is_empty()
        && old_hash != digest
    {
        info!(
            "Overwriting existing link {}. Old hash: {}, new hash: {}",
            path, old_hash, digest
        );

        // Acquire lock on old hash before decrement
        let old_hash_lock_key = locks::hash_lock(&state.bucket_name, &old_hash);
        let old_hash_lock = locks_storage.prepare_lock(old_hash_lock_key).await;
        let old_hash_guard = match old_hash_lock.acquire_exclusive().await {
            Ok(g) => g,
            Err(e) => {
                error!("Failed to acquire old hash lock: {}", e);
                record_metrics("500");
                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to acquire old hash lock".to_string())
                    .unwrap();
            }
        };

        // Decrement old reference count atomically and get new count
        let old_ref_count_result = state
            .kvstorage
            .atomic_decrement_ref_count(&state.bucket_name, &old_hash)
            .await;

        // Delete old blob if no longer referenced
        if let Ok(old_ref_count) = old_ref_count_result
            && old_ref_count <= 0
        {
            debug!("Deleting unused blob: {}", old_hash);
            let _ = state.s3storage.delete_object(&old_hash).await;
        }

        // Release old hash lock
        if let Err(e) = old_hash_guard.release().await {
            warn!("Failed to release old hash lock: {}", e);
        }
    }

    // 9. Update file metadata (path -> hash mapping and timestamp)
    if let Err(e) = state
        .kvstorage
        .set_ref_file(&state.bucket_name, path, &digest)
        .await
    {
        error!("Failed to set ref file: {}", e);
        record_metrics("500");
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to set ref file".to_string())
            .unwrap();
    }

    if let Err(e) = state
        .kvstorage
        .set_modified(&state.bucket_name, path, timestamp)
        .await
    {
        error!("Failed to set modified time: {}", e);
        record_metrics("500");
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to set modified time".to_string())
            .unwrap();
    }

    debug!("Created link {}.", path);

    // Release lock early since all critical metadata operations are complete
    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }

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
                    record_metrics("200");
                    let last_modified_header = utils::format_rfc2822_timestamp(timestamp);
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/plain")
                        .header("Last-Modified", last_modified_header)
                        .body("".to_string())
                        .unwrap();
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

    record_metrics("200");
    let last_modified_header = utils::format_rfc2822_timestamp(timestamp);
    debug!(
        "PUT {} complete, returning Last-Modified: {} (unix: {})",
        path, last_modified_header, timestamp
    );
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain")
        .header("Last-Modified", last_modified_header)
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
