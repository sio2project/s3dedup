use crate::routes::ft::{MetricsRecorder, build_ft_file_response, utils};
use crate::{AppState, locks, metrics};
use anyhow::{Context, Result};
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
    let record_metrics = MetricsRecorder::new("GET", "/ft/files");
    let path = path.strip_prefix('/').unwrap_or(&path);
    debug!("Handling GET for path: {}", path);

    match ft_get_file_inner(&state, path).await {
        Ok(response) => {
            let status = response.status().as_u16().to_string();
            record_metrics.record(&status);
            response
        }
        Err(e) => {
            error!("GET {} failed: {}", path, e);
            record_metrics.record("500");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        }
    }
}

async fn ft_get_file_inner(state: &AppState, path: &str) -> Result<Response<Body>> {
    // 1. Acquire file lock and check if file exists
    let lock = state
        .locks
        .prepare_lock(locks::file_lock(&state.bucket_name, path))
        .await;
    let guard = lock
        .acquire_shared()
        .await
        .context("Failed to acquire shared lock")?;

    let modified_time = state
        .kvstorage
        .get_modified(&state.bucket_name, path)
        .await
        .context("Failed to get modified time")?;

    // 2. If file doesn't exist, try filetracker fallback (live migration mode)
    if modified_time == 0 {
        // Drop shared lock — migration needs exclusive, and ft download may be slow
        let _ = guard.release().await;

        if let Some(filetracker_client) = &state.filetracker_client {
            debug!("File {} not found in s3dedup, checking filetracker", path);

            match filetracker_client
                .download_file(path, state.max_inmemory_size, &state.temp_dir)
                .await
            {
                Ok(downloaded) => {
                    debug!("File {} found in filetracker, migrating on-the-fly", path);

                    metrics::FILETRACKER_FALLBACKS_TOTAL
                        .with_label_values(&[&state.bucket_name])
                        .inc();

                    let result = match downloaded {
                        crate::filetracker_client::DownloadedFile::InMemory(file_metadata) => {
                            crate::migration::migrate_single_file_from_metadata(
                                state,
                                path,
                                file_metadata,
                            )
                            .await
                        }
                        crate::filetracker_client::DownloadedFile::OnDisk(ref streaming_meta) => {
                            crate::migration::migrate_single_file_from_streaming(
                                state,
                                path,
                                streaming_meta,
                            )
                            .await
                        }
                    };
                    result.context("Failed to migrate file on-the-fly")?;

                    // Migration succeeded — re-acquire lock and serve
                    return serve_file(state, path).await;
                }
                Err(e) => {
                    debug!("File {} not found in filetracker either: {}", path, e);
                }
            }
        }

        debug!("File {} not found", path);
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap());
    }

    // 3. Normal path — guard still held, read metadata under lock
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

    let (byte_stream, s3_content_length) = state
        .s3storage
        .get_object_stream(&hash)
        .await
        .with_context(|| {
            format!(
                "Failed to get object from S3 (bucket={}, key={})",
                state.bucket_name, hash
            )
        })?;

    // Release lock — S3 stream already initiated. Safe because S3-compatible backends
    // (including Garage) complete in-progress GETs even if object is deleted.
    let _ = guard.release().await;

    // Use S3 content-length as source of truth for Content-Length header.
    // DB compressed_size can diverge from actual S3 object size (e.g. different gzip
    // encodings across uploads with same content hash).
    let content_length = s3_content_length.unwrap_or(compressed_size as i64);
    if compressed_size > 0
        && let Some(s3_len) = s3_content_length
        && s3_len != compressed_size as i64
    {
        error!(
            "compressed_size mismatch for hash {}: db={} s3={}",
            hash, compressed_size, s3_len
        );
    }

    debug!(
        "GET {} returning Last-Modified: {} (unix: {})",
        path,
        utils::format_rfc2822_timestamp(modified_time),
        modified_time
    );
    let body = Body::new(byte_stream.into_inner());
    Ok(build_ft_file_response(
        body,
        content_length,
        logical_size,
        modified_time,
    ))
}

/// Re-acquire shared lock and serve a file from S3.
/// Used only after on-the-fly migration from filetracker.
async fn serve_file(state: &AppState, path: &str) -> Result<Response<Body>> {
    let lock = state
        .locks
        .prepare_lock(locks::file_lock(&state.bucket_name, path))
        .await;
    let guard = lock
        .acquire_shared()
        .await
        .context("Failed to re-acquire shared lock after migration")?;

    let modified_time = state
        .kvstorage
        .get_modified(&state.bucket_name, path)
        .await
        .context("Failed to get modified time")?;

    let hash = state
        .kvstorage
        .get_ref_file(&state.bucket_name, path)
        .await
        .context("Failed to get ref file after migration")?;

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

    let (byte_stream, s3_content_length) = state
        .s3storage
        .get_object_stream(&hash)
        .await
        .context("Failed to get object from S3 after migration")?;

    let _ = guard.release().await;

    let content_length = s3_content_length.unwrap_or(compressed_size as i64);
    if compressed_size > 0
        && let Some(s3_len) = s3_content_length
        && s3_len != compressed_size as i64
    {
        error!(
            "compressed_size mismatch for hash {}: db={} s3={}",
            hash, compressed_size, s3_len
        );
    }

    let body = Body::new(byte_stream.into_inner());
    Ok(build_ft_file_response(
        body,
        content_length,
        logical_size,
        modified_time,
    ))
}
