use crate::routes::ft::utils;
use crate::{AppState, locks, metrics};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// HEAD handler — returns file metadata headers without fetching file content from S3.
/// Uses a shared lock to ensure consistent metadata reads.
pub async fn ft_head_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("Handling HEAD for path: {}", path);

    // Acquire shared lock for consistent metadata reads
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let lock = state.locks.prepare_lock(lock_key).await;
    let guard = match lock.acquire_shared().await {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to acquire shared lock for HEAD: {}", e);
            metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&["HEAD", "/ft/files", "500"])
                .inc();
            metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&["HEAD", "/ft/files"])
                .observe(start.elapsed().as_secs_f64());
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap();
        }
    };

    let modified_time = match state.kvstorage.get_modified(&state.bucket_name, path).await {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to get modified time: {}", e);
            metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&["HEAD", "/ft/files", "500"])
                .inc();
            metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&["HEAD", "/ft/files"])
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

    if modified_time == 0 {
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }

        // Check filetracker in live migration mode using HEAD (no body download)
        if let Some(filetracker_client) = &state.filetracker_client
            && let Ok(file_headers) = filetracker_client.head_file(path).await
        {
            metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&["HEAD", "/ft/files", "200"])
                .inc();
            metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&["HEAD", "/ft/files"])
                .observe(start.elapsed().as_secs_f64());
            return Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", file_headers.content_length.to_string())
                .header(
                    "Content-Encoding",
                    if file_headers.is_compressed {
                        "gzip"
                    } else {
                        "identity"
                    },
                )
                .header(
                    "Last-Modified",
                    utils::format_rfc2822_timestamp(file_headers.last_modified),
                )
                .header("Logical-Size", file_headers.logical_size.to_string())
                .body(Body::empty())
                .unwrap();
        }

        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&["HEAD", "/ft/files", "404"])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&["HEAD", "/ft/files"])
            .observe(start.elapsed().as_secs_f64());
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }

    let hash = match state.kvstorage.get_ref_file(&state.bucket_name, path).await {
        Ok(h) if !h.is_empty() => h,
        _ => {
            metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&["HEAD", "/ft/files", "404"])
                .inc();
            metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&["HEAD", "/ft/files"])
                .observe(start.elapsed().as_secs_f64());
            if let Err(e) = guard.release().await {
                warn!("Failed to release file lock: {}", e);
            }
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap();
        }
    };

    let logical_size_fut = state.kvstorage.get_logical_size(&state.bucket_name, &hash);
    let compressed_size_fut = state
        .kvstorage
        .get_compressed_size(&state.bucket_name, &hash);
    let (logical_size, compressed_size) =
        match tokio::try_join!(logical_size_fut, compressed_size_fut) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to get size metadata: {}", e);
                metrics::HTTP_REQUESTS_TOTAL
                    .with_label_values(&["HEAD", "/ft/files", "500"])
                    .inc();
                metrics::HTTP_REQUEST_DURATION_SECONDS
                    .with_label_values(&["HEAD", "/ft/files"])
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

    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }

    // For legacy blobs without stored compressed_size, fall back to S3 head_object
    let content_length = if compressed_size > 0 {
        compressed_size as i64
    } else {
        match state.s3storage.object_exists_with_size(&hash).await {
            Ok(Some(size)) => size,
            _ => 0,
        }
    };

    metrics::HTTP_REQUESTS_TOTAL
        .with_label_values(&["HEAD", "/ft/files", "200"])
        .inc();
    metrics::HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&["HEAD", "/ft/files"])
        .observe(start.elapsed().as_secs_f64());

    let last_modified_header = utils::format_rfc2822_timestamp(modified_time);
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", content_length.to_string())
        .header("Content-Encoding", "gzip")
        .header("Last-Modified", last_modified_header)
        .header("Logical-Size", logical_size.to_string())
        .body(Body::empty())
        .unwrap()
}
