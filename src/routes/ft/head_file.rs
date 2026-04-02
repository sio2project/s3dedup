use crate::routes::ft::{MetricsRecorder, build_ft_file_response, utils};
use crate::{AppState, locks};
use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{debug, error};

/// HEAD handler — returns file metadata headers without fetching file content from S3.
/// Uses a shared lock to ensure consistent metadata reads.
pub async fn ft_head_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    let record_metrics = MetricsRecorder::new("HEAD", "/ft/files");
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("Handling HEAD for path: {}", path);

    match ft_head_file_inner(&state, path).await {
        Ok(response) => {
            let status = response.status().as_u16().to_string();
            record_metrics.record(&status);
            response
        }
        Err(e) => {
            error!("HEAD {} failed: {}", path, e);
            record_metrics.record("500");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        }
    }
}

async fn ft_head_file_inner(state: &AppState, path: &str) -> Result<Response<Body>> {
    // Acquire shared lock for consistent metadata reads
    let lock_key = locks::file_lock(&state.bucket_name, path);
    let lock = state.locks.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_shared()
        .await
        .context("Failed to acquire shared lock")?;

    let modified_time = state
        .kvstorage
        .get_modified(&state.bucket_name, path)
        .await
        .context("Failed to get modified time")?;

    if modified_time == 0 {
        // Drop guard before filetracker fallback (no longer need local lock)
        let _ = guard.release().await;

        // Check filetracker in live migration mode using HEAD (no body download)
        if let Some(filetracker_client) = &state.filetracker_client {
            match filetracker_client.head_file(path).await {
                Err(e) => {
                    debug!("File {} not found in filetracker: {}", path, e);
                }
                Ok(file_headers) => {
                    return Ok(Response::builder()
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
                        .unwrap());
                }
            }
        }

        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap());
    }

    let hash = state
        .kvstorage
        .get_ref_file(&state.bucket_name, path)
        .await
        .context("Failed to get ref file")?;

    if hash.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap());
    }

    let logical_size_fut = state.kvstorage.get_logical_size(&state.bucket_name, &hash);
    let compressed_size_fut = state
        .kvstorage
        .get_compressed_size(&state.bucket_name, &hash);
    let (logical_size, compressed_size) = tokio::try_join!(logical_size_fut, compressed_size_fut)
        .context("Failed to get size metadata")?;

    // Release lock — all metadata reads complete
    let _ = guard.release().await;

    // For legacy blobs without stored compressed_size, fall back to S3 head_object.
    // This runs after lock release — a concurrent DELETE could remove the object,
    // causing us to return Content-Length: 0. This is acceptable: the file is being
    // deleted anyway, and HEAD has no body to truncate.
    let content_length = if compressed_size > 0 {
        compressed_size as i64
    } else {
        match state.s3storage.object_exists_with_size(&hash).await {
            Ok(Some(size)) => size,
            _ => 0,
        }
    };

    Ok(build_ft_file_response(
        Body::empty(),
        content_length,
        logical_size,
        modified_time,
    ))
}
