use crate::routes::ft::utils;
use crate::{AppState, locks, metrics};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error, warn};

pub async fn ft_get_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();

    // Remove leading slash from wildcard path
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("Handling GET for path: {}", path);

    // 1. Acquire file lock (shared lock for read operation)
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let locks_storage = &state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let guard = match lock.acquire_shared().await {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to acquire shared lock: {}", e);
            metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&["GET", "/ft/files", "500"])
                .inc();
            metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&["GET", "/ft/files"])
                .observe(start.elapsed().as_secs_f64());

            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap();
        }
    };

    // 2. Check if file exists and get metadata
    let modified_time = state.kvstorage.get_modified(&state.bucket_name, path).await;
    if modified_time.is_err() {
        error!("Failed to get modified time");
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["GET", "/ft/files", "500"])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["GET", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());

        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let modified_time = modified_time.unwrap();

    // If file doesn't exist (modified time is 0), check filetracker if in live migration mode
    if modified_time == 0 {
        if let Some(filetracker_client) = &state.filetracker_client {
            debug!("File {} not found in s3dedup, checking filetracker", path);

            // Download file from filetracker — small files in memory, large on disk
            match filetracker_client
                .download_file(path, state.max_inmemory_size)
                .await
            {
                Ok(downloaded) => {
                    debug!("File {} found in filetracker, migrating on-the-fly", path);

                    // Track filetracker fallback
                    metrics::FILETRACKER_FALLBACKS_TOTAL
                        .with_label_values(&[&state.bucket_name])
                        .inc();

                    // Release the shared lock before migration to avoid deadlock
                    // (migration needs exclusive lock on the same key)
                    if let Err(e) = guard.release().await {
                        warn!("Failed to release file lock: {}", e);
                    }

                    // Migrate: in-memory for small files, temp file for large
                    let result = match downloaded {
                        crate::filetracker_client::DownloadedFile::InMemory(file_metadata) => {
                            crate::migration::migrate_single_file_from_metadata(
                                &state,
                                path,
                                file_metadata,
                            )
                            .await
                        }
                        crate::filetracker_client::DownloadedFile::OnDisk(ref streaming_meta) => {
                            crate::migration::migrate_single_file_from_streaming(
                                &state,
                                path,
                                streaming_meta,
                            )
                            .await
                        }
                    };

                    if let Err(e) = result {
                        error!("Failed to migrate file on-the-fly: {}", e);
                        metrics::HTTP_REQUESTS_TOTAL
                            .with_label_values(&["GET", "/ft/files", "500"])
                            .inc();
                        metrics::HTTP_REQUEST_DURATION_SECONDS
                            .with_label_values(&["GET", "/ft/files"])
                            .observe(start.elapsed().as_secs_f64());

                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::empty())
                            .unwrap();
                    }

                    // File is now in s3dedup — serve from S3 via streaming GET.
                    // Re-acquire shared lock for consistent metadata reads.
                    let lock_key2 = locks::file_lock(&state.bucket_name, path);
                    let lock2 = state.locks.prepare_lock(lock_key2).await;
                    let guard2 = match lock2.acquire_shared().await {
                        Ok(g) => g,
                        Err(e) => {
                            error!("Failed to re-acquire shared lock after migration: {}", e);
                            metrics::HTTP_REQUESTS_TOTAL
                                .with_label_values(&["GET", "/ft/files", "500"])
                                .inc();
                            metrics::HTTP_REQUEST_DURATION_SECONDS
                                .with_label_values(&["GET", "/ft/files"])
                                .observe(start.elapsed().as_secs_f64());
                            return Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::empty())
                                .unwrap();
                        }
                    };

                    let hash = match state.kvstorage.get_ref_file(&state.bucket_name, path).await {
                        Ok(h) => h,
                        Err(e) => {
                            error!("Failed to get ref file after migration: {}", e);
                            metrics::HTTP_REQUESTS_TOTAL
                                .with_label_values(&["GET", "/ft/files", "500"])
                                .inc();
                            metrics::HTTP_REQUEST_DURATION_SECONDS
                                .with_label_values(&["GET", "/ft/files"])
                                .observe(start.elapsed().as_secs_f64());
                            if let Err(e) = guard2.release().await {
                                warn!("Failed to release file lock: {}", e);
                            }
                            return Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::empty())
                                .unwrap();
                        }
                    };

                    let (byte_stream, s3_content_length) =
                        match state.s3storage.get_object_stream(&hash).await {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to get object from S3 after migration: {}", e);
                                metrics::HTTP_REQUESTS_TOTAL
                                    .with_label_values(&["GET", "/ft/files", "500"])
                                    .inc();
                                metrics::HTTP_REQUEST_DURATION_SECONDS
                                    .with_label_values(&["GET", "/ft/files"])
                                    .observe(start.elapsed().as_secs_f64());
                                if let Err(e) = guard2.release().await {
                                    warn!("Failed to release file lock: {}", e);
                                }
                                return Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::empty())
                                    .unwrap();
                            }
                        };

                    let compressed_size = state
                        .kvstorage
                        .get_compressed_size(&state.bucket_name, &hash)
                        .await
                        .unwrap_or(0);
                    let content_length = if compressed_size > 0 {
                        compressed_size as i64
                    } else {
                        s3_content_length.unwrap_or(0)
                    };
                    let logical_size = state
                        .kvstorage
                        .get_logical_size(&state.bucket_name, &hash)
                        .await
                        .unwrap_or(0);
                    let modified_time_after = state
                        .kvstorage
                        .get_modified(&state.bucket_name, path)
                        .await
                        .unwrap_or(0);

                    // Release lock — S3 stream already initiated
                    if let Err(e) = guard2.release().await {
                        warn!("Failed to release file lock: {}", e);
                    }

                    metrics::HTTP_REQUESTS_TOTAL
                        .with_label_values(&["GET", "/ft/files", "200"])
                        .inc();
                    metrics::HTTP_REQUEST_DURATION_SECONDS
                        .with_label_values(&["GET", "/ft/files"])
                        .observe(start.elapsed().as_secs_f64());

                    let body = Body::new(byte_stream.into_inner());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", content_length.to_string())
                        .header("Content-Encoding", "gzip")
                        .header(
                            "Last-Modified",
                            utils::format_rfc2822_timestamp(modified_time_after),
                        )
                        .header("Logical-Size", logical_size.to_string())
                        .body(body)
                        .unwrap();
                }
                Err(e) => {
                    debug!("File {} not found in filetracker either: {}", path, e);
                }
            }
        }

        debug!("File {} not found", path);
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["GET", "/ft/files", "404"])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["GET", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());

        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }

        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    // 3. Get the hash for this path
    let hash = state.kvstorage.get_ref_file(&state.bucket_name, path).await;
    if hash.is_err() {
        error!("Failed to get ref file");
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["GET", "/ft/files", "500"])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["GET", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());

        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let hash = hash.unwrap();

    if hash.is_empty() {
        error!("File {} has no hash reference", path);
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["GET", "/ft/files", "404"])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["GET", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());

        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    // 4. Get logical size and compressed size concurrently
    let logical_size_fut = state.kvstorage.get_logical_size(&state.bucket_name, &hash);
    let compressed_size_fut = state
        .kvstorage
        .get_compressed_size(&state.bucket_name, &hash);
    let (logical_size, compressed_size) =
        match tokio::try_join!(logical_size_fut, compressed_size_fut) {
            Ok((ls, cs)) => (ls, cs),
            Err(e) => {
                error!("Failed to get size metadata: {}", e);
                metrics::HTTP_REQUESTS_TOTAL
                    .with_label_values(&["GET", "/ft/files", "500"])
                    .inc();
                metrics::HTTP_REQUEST_DURATION_SECONDS
                    .with_label_values(&["GET", "/ft/files"])
                    .observe(start.elapsed().as_secs_f64());

                if let Err(e) = guard.release().await {
                    warn!("Failed to release file lock: {}", e);
                }
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap();
            }
        };

    // 5. Fetch the blob from S3 as a stream (no memory buffering)
    let (byte_stream, s3_content_length) = match state.s3storage.get_object_stream(&hash).await {
        Ok(v) => v,
        Err(e) => {
            error!(
                "Failed to get object from S3 (bucket={}, key={}): {}",
                state.bucket_name, hash, e
            );
            metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&["GET", "/ft/files", "500"])
                .inc();
            metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&["GET", "/ft/files"])
                .observe(start.elapsed().as_secs_f64());

            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap();
        }
    };

    // Release lock before streaming the response body. This is safe because:
    // - The S3 GetObject request has already been initiated; S3-compatible backends
    //   (including Garage) will complete an in-progress GET even if the object is
    //   deleted concurrently.
    // - A concurrent DELETE would need to decrement refcount to 0 first, which requires
    //   the hash lock. Even if it does, the S3 backend serves the in-flight stream.
    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }

    // Use compressed_size from KV, fall back to S3 content_length for legacy blobs
    let content_length = if compressed_size > 0 {
        compressed_size as i64
    } else {
        s3_content_length.unwrap_or(0)
    };

    // 6. Record metrics
    metrics::HTTP_REQUESTS_TOTAL
        .with_label_values(&["GET", "/ft/files", "200"])
        .inc();
    metrics::HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&["GET", "/ft/files"])
        .observe(start.elapsed().as_secs_f64());

    // 7. Return file with streaming body (matching original filetracker headers)
    let last_modified_header = utils::format_rfc2822_timestamp(modified_time);
    debug!(
        "GET {} returning Last-Modified: {} (unix: {})",
        path, last_modified_header, modified_time
    );
    // Convert S3 ByteStream to axum Body without buffering
    let body = Body::new(byte_stream.into_inner());
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", content_length.to_string())
        .header("Content-Encoding", "gzip")
        .header("Last-Modified", last_modified_header)
        .header("Logical-Size", logical_size.to_string())
        .body(body)
        .unwrap()
}
