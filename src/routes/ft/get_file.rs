use crate::routes::ft::utils;
use crate::{AppState, locks, metrics};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error};

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
    let _guard = lock.acquire_shared().await;

    // 2. Check if file exists and get metadata
    let modified_time = state
        .kvstorage
        .lock()
        .await
        .get_modified(&state.bucket_name, path)
        .await;
    if modified_time.is_err() {
        error!("Failed to get modified time");
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
    let modified_time = modified_time.unwrap();

    // If file doesn't exist (modified time is 0), check filetracker if in live migration mode
    if modified_time == 0 {
        if let Some(filetracker_client) = &state.filetracker_client {
            debug!("File {} not found in s3dedup, checking filetracker", path);

            // Try to get file from filetracker
            match filetracker_client.get_file(path).await {
                Ok(file_metadata) => {
                    debug!("File {} found in filetracker, migrating on-the-fly", path);

                    // Track filetracker fallback
                    metrics::FILETRACKER_FALLBACKS_TOTAL
                        .with_label_values(&[&state.bucket_name])
                        .inc();

                    // Drop the shared lock before migration to avoid deadlock
                    // (migration needs exclusive lock on the same key)
                    drop(_guard);

                    // Migrate the file on-the-fly using migration logic
                    let result = crate::migration::migrate_single_file_from_metadata(
                        &state,
                        path,
                        file_metadata.clone(),
                    )
                    .await;

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

                    metrics::HTTP_REQUESTS_TOTAL
                        .with_label_values(&["GET", "/ft/files", "200"])
                        .inc();
                    metrics::HTTP_REQUEST_DURATION_SECONDS
                        .with_label_values(&["GET", "/ft/files"])
                        .observe(start.elapsed().as_secs_f64());

                    // Serve the file directly from filetracker response
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", file_metadata.data.len().to_string())
                        .header(
                            "Content-Encoding",
                            if file_metadata.is_compressed {
                                "gzip"
                            } else {
                                "identity"
                            },
                        )
                        .header(
                            "Last-Modified",
                            utils::format_rfc2822_timestamp(file_metadata.last_modified),
                        )
                        .header("Logical-Size", file_metadata.logical_size.to_string())
                        .body(Body::from(file_metadata.data))
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

        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    // 3. Get the hash for this path
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
            .body(Body::empty())
            .unwrap();
    }
    let hash = hash.unwrap();

    if hash.is_empty() {
        error!("File {} has no hash reference", path);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    // 4. Get logical size
    let logical_size = state
        .kvstorage
        .lock()
        .await
        .get_logical_size(&state.bucket_name, &hash)
        .await;
    if logical_size.is_err() {
        error!("Failed to get logical size");
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
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap();
    }
    let blob_data = blob_data.unwrap();

    drop(_guard);

    // 6. Record metrics
    metrics::HTTP_REQUESTS_TOTAL
        .with_label_values(&["GET", "/ft/files", "200"])
        .inc();
    metrics::HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&["GET", "/ft/files"])
        .observe(start.elapsed().as_secs_f64());

    // 7. Return file with appropriate headers (matching original filetracker)
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", blob_data.len().to_string())
        .header("Content-Encoding", "gzip")
        .header(
            "Last-Modified",
            utils::format_rfc2822_timestamp(modified_time),
        )
        .header("Logical-Size", logical_size.to_string())
        .body(Body::from(blob_data))
        .unwrap()
}
