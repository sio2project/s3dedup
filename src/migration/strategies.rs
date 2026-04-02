use crate::filetracker_client::{DownloadedFile, FileMetadata, FiletrackerClient};
use crate::metrics::MIGRATION_FILES_MIGRATED;
use crate::routes::ft::storage_helpers;
use crate::AppState;
use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::warn;

/// Migrate a single file from filetracker to s3dedup
/// Returns Ok(true) if migrated, Ok(false) if skipped, Err if failed
pub(super) async fn migrate_single_file(
    filetracker_client: &FiletrackerClient,
    app_state: Arc<AppState>,
    path: &str,
) -> Result<bool> {
    // Download file — small files buffered in memory, large files streamed to temp file
    let downloaded = filetracker_client
        .download_file(path, app_state.max_inmemory_size)
        .await?;

    let (last_modified, is_compressed) = match &downloaded {
        DownloadedFile::InMemory(m) => (m.last_modified, m.is_compressed),
        DownloadedFile::OnDisk(s) => (s.last_modified, s.is_compressed),
    };

    // Check if file already exists in s3dedup with same or newer version
    let current_modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= last_modified {
        return Ok(false);
    }

    // Process file data: in-memory for small files, temp file for large
    let (digest, logical_size, compressed_size, compressed_data, _compressed_path, _keep_alive) =
        match downloaded {
            DownloadedFile::InMemory(file_metadata) => {
                // Small file: process entirely in memory (fast, no disk I/O)
                let uncompressed_data = if is_compressed {
                    storage_helpers::decompress_gzip(&file_metadata.data)?
                } else {
                    file_metadata.data
                };
                let digest = storage_helpers::compute_sha256(&uncompressed_data);
                let logical_size = uncompressed_data.len();
                let compressed_data = storage_helpers::compress_gzip(&uncompressed_data)?;
                let compressed_size = compressed_data.len();
                (
                    digest,
                    logical_size,
                    compressed_size,
                    Some(compressed_data),
                    None::<std::path::PathBuf>,
                    None::<Box<dyn std::any::Any + Send>>,
                )
            }
            DownloadedFile::OnDisk(streaming_meta) => {
                // Large file: process from temp file in 64KB chunks
                let temp_path = streaming_meta.temp_path.clone();
                let processed = tokio::task::spawn_blocking(move || {
                    if is_compressed {
                        storage_helpers::process_compressed_temp_file(&temp_path)
                    } else {
                        storage_helpers::process_uncompressed_temp_file(&temp_path)
                    }
                })
                .await
                .context("Task panicked during file processing")?
                .context("Failed to process downloaded file")?;

                let compressed_path = processed.compressed_path.clone();
                // Keep temp files alive: download temp + output temp (if uncompressed)
                let keep_alive: Box<dyn std::any::Any + Send> =
                    Box::new((streaming_meta, processed.output_temp_file));
                (
                    processed.digest,
                    processed.logical_size,
                    processed.compressed_size,
                    None,
                    Some(compressed_path),
                    Some(keep_alive),
                )
            }
        };

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks_storage = &app_state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = match app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e);
        }
    };

    if current_modified_after_lock >= last_modified {
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Ok(false);
    }

    // Acquire hash lock for S3/refcount operations
    let hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &digest);
    let hash_lock = locks_storage.prepare_lock(hash_lock_key).await;
    let hash_guard = match hash_lock.acquire_exclusive().await {
        Ok(g) => g,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e.context("Failed to acquire hash lock for migration"));
        }
    };

    let blob_exists = match app_state.s3storage.object_exists(&digest).await {
        Ok(v) => v,
        Err(e) => {
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    };

    // Upload: in-memory data via put_object, or stream from temp file via ByteStream::from_path
    if !blob_exists {
        let upload_result = if let Some(data) = compressed_data {
            app_state.s3storage.put_object(&digest, data).await
        } else if let Some(ref path) = _compressed_path {
            match aws_sdk_s3::primitives::ByteStream::from_path(path).await {
                Ok(bs) => {
                    app_state
                        .s3storage
                        .put_object_stream(&digest, bs, Some(compressed_size as i64))
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
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Record blob metadata (sizes + refcount increment)
    if let Err(e) = app_state
        .record_blob_metadata(&digest, logical_size, Some(compressed_size), None)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Release new hash lock
    if let Err(e) = hash_guard.release().await {
        warn!("Failed to release hash lock: {}", e);
    }

    // Handle overwriting existing file — decrement old hash refcount
    if current_modified > 0 {
        let old_hash = match app_state
            .kvstorage
            .get_ref_file(&app_state.bucket_name, path)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let _ = guard.release().await;
                return Err(e);
            }
        };
        if let Err(e) = app_state.decrement_old_ref(&old_hash, &digest).await {
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .update_file_ref(path, &digest, last_modified)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }

    // Increment migration metric
    MIGRATION_FILES_MIGRATED
        .with_label_values(&[&app_state.bucket_name])
        .inc();

    Ok(true)
}

/// Migrate a single file from filetracker metadata (for on-the-fly migration during GET)
pub async fn migrate_single_file_from_metadata(
    app_state: &AppState,
    path: &str,
    file_metadata: FileMetadata,
) -> Result<()> {
    // Check if file already exists in s3dedup with same or newer version
    let current_modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= file_metadata.last_modified {
        // File already exists with same or newer version, skip
        return Ok(());
    }

    // Process file data: in-memory for small files, temp file for large files
    let is_compressed = file_metadata.is_compressed;
    let data = file_metadata.data; // move, not clone — file_metadata is owned
    let use_tempfile = data.len() > app_state.max_inmemory_size;

    let (digest, logical_size, compressed_size, compressed_data, _compressed_path, _keep_alive) =
        if use_tempfile {
            // Large file: write to temp file, process via chunked pipeline
            let processed = tokio::task::spawn_blocking(move || {
                use std::io::Write;
                let mut temp = tempfile::NamedTempFile::new()?;
                temp.write_all(&data)?;
                temp.flush()?;
                let input_path = temp.path().to_path_buf();

                let result = if is_compressed {
                    storage_helpers::process_compressed_temp_file(&input_path)?
                } else {
                    storage_helpers::process_uncompressed_temp_file(&input_path)?
                };

                let keep_alive: Box<dyn std::any::Any + Send> = if is_compressed {
                    Box::new(temp)
                } else {
                    Box::new(result.output_temp_file)
                };

                let compressed_path = if is_compressed {
                    input_path
                } else {
                    result.compressed_path
                };

                Ok::<_, anyhow::Error>((
                    result.digest,
                    result.logical_size,
                    result.compressed_size,
                    compressed_path,
                    keep_alive,
                ))
            })
            .await
            .context("Task panicked during file processing")?
            .context("Failed to process file data")?;

            (
                processed.0,
                processed.1,
                processed.2,
                None,
                Some(processed.3),
                Some(processed.4),
            )
        } else {
            // Small file: process entirely in memory (fast, no disk I/O)
            let uncompressed_data = if is_compressed {
                storage_helpers::decompress_gzip(&data)?
            } else {
                data
            };
            let digest = storage_helpers::compute_sha256(&uncompressed_data);
            let logical_size = uncompressed_data.len();
            let compressed_data = storage_helpers::compress_gzip(&uncompressed_data)?;
            let compressed_size = compressed_data.len();
            (
                digest,
                logical_size,
                compressed_size,
                Some(compressed_data),
                None::<std::path::PathBuf>,
                None::<Box<dyn std::any::Any + Send>>,
            )
        };

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks = &app_state.locks;
    let lock = locks.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = match app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e);
        }
    };

    if current_modified_after_lock >= file_metadata.last_modified {
        // File was migrated by another concurrent task, skip
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Ok(());
    }

    // Acquire hash lock for S3/refcount operations
    let hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &digest);
    let hash_lock = locks.prepare_lock(hash_lock_key).await;
    let hash_guard = match hash_lock.acquire_exclusive().await {
        Ok(g) => g,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e.context("Failed to acquire hash lock for migration"));
        }
    };

    // Check if blob already exists in S3
    let blob_exists = match app_state.s3storage.object_exists(&digest).await {
        Ok(v) => v,
        Err(e) => {
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    };

    // Upload: in-memory or from temp file
    if !blob_exists {
        let upload_result = if let Some(data) = compressed_data {
            app_state.s3storage.put_object(&digest, data).await
        } else if let Some(ref p) = _compressed_path {
            match aws_sdk_s3::primitives::ByteStream::from_path(p).await {
                Ok(bs) => {
                    app_state
                        .s3storage
                        .put_object_stream(&digest, bs, Some(compressed_size as i64))
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
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Record blob metadata (sizes + refcount increment)
    if let Err(e) = app_state
        .record_blob_metadata(&digest, logical_size, Some(compressed_size), None)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Release new hash lock
    if let Err(e) = hash_guard.release().await {
        warn!("Failed to release hash lock: {}", e);
    }

    // Handle overwriting existing file — decrement old hash refcount
    if current_modified > 0 {
        let old_hash = match app_state
            .kvstorage
            .get_ref_file(&app_state.bucket_name, path)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let _ = guard.release().await;
                return Err(e);
            }
        };
        if let Err(e) = app_state.decrement_old_ref(&old_hash, &digest).await {
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .update_file_ref(path, &digest, file_metadata.last_modified)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }
    Ok(())
}

/// Migrate a single file from streaming download (temp file on disk).
/// No in-memory buffering — processes directly from the temp file.
pub async fn migrate_single_file_from_streaming(
    app_state: &AppState,
    path: &str,
    streaming_meta: &crate::filetracker_client::StreamingFileMetadata,
) -> Result<()> {
    let current_modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= streaming_meta.last_modified {
        return Ok(());
    }

    // Process from temp file (hash + compress in 64KB chunks, no full buffer)
    let is_compressed = streaming_meta.is_compressed;
    let temp_path = streaming_meta.temp_path.clone();
    let processed = tokio::task::spawn_blocking(move || {
        if is_compressed {
            storage_helpers::process_compressed_temp_file(&temp_path)
        } else {
            storage_helpers::process_uncompressed_temp_file(&temp_path)
        }
    })
    .await
    .context("Task panicked during file processing")?
    .context("Failed to process downloaded file")?;

    let digest = processed.digest.clone();
    let logical_size = processed.logical_size;
    let compressed_size = processed.compressed_size;
    let compressed_path = processed.compressed_path.clone();

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks = &app_state.locks;
    let lock = locks.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck after lock
    let current_modified_after_lock = match app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e);
        }
    };

    if current_modified_after_lock >= streaming_meta.last_modified {
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Ok(());
    }

    // Acquire hash lock
    let hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &digest);
    let hash_lock = locks.prepare_lock(hash_lock_key).await;
    let hash_guard = match hash_lock.acquire_exclusive().await {
        Ok(g) => g,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e.context("Failed to acquire hash lock for migration"));
        }
    };

    let blob_exists = match app_state.s3storage.object_exists(&digest).await {
        Ok(v) => v,
        Err(e) => {
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    };

    // Upload from temp file
    if !blob_exists {
        let upload_result =
            match aws_sdk_s3::primitives::ByteStream::from_path(&compressed_path).await {
                Ok(bs) => {
                    app_state
                        .s3storage
                        .put_object_stream(&digest, bs, Some(compressed_size as i64))
                        .await
                }
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to open compressed temp file for S3 upload: {}",
                    e
                )),
            };
        if let Err(e) = upload_result {
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Record blob metadata (sizes + refcount increment)
    if let Err(e) = app_state
        .record_blob_metadata(&digest, logical_size, Some(compressed_size), None)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = hash_guard.release().await {
        warn!("Failed to release hash lock: {}", e);
    }

    // Handle overwriting existing file — decrement old hash refcount
    if current_modified_after_lock > 0 {
        let old_hash = match app_state
            .kvstorage
            .get_ref_file(&app_state.bucket_name, path)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let _ = guard.release().await;
                return Err(e);
            }
        };
        if let Err(e) = app_state.decrement_old_ref(&old_hash, &digest).await {
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .update_file_ref(path, &digest, streaming_meta.last_modified)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }
    Ok(())
}

/// Migrate a single file from V1 filetracker filesystem to s3dedup
/// Returns Ok(true) if migrated, Ok(false) if skipped, Err if failed
pub(super) async fn migrate_single_file_from_v1_fs(
    app_state: Arc<AppState>,
    file_info: &super::v1_filesystem::V1FileInfo,
) -> Result<bool> {
    let path = &file_info.relative_path;

    // Check if file already exists in s3dedup with same or newer version
    let current_modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= file_info.last_modified {
        // File already exists with same or newer version, skip
        return Ok(false);
    }

    // Move blocking operations (file I/O, SHA256, compression) to blocking thread pool
    // Process via temp file to avoid holding large files in memory
    let file_info_clone = file_info.clone();
    let (digest, logical_size, compressed_size, _keep_output, compressed_path) =
        tokio::task::spawn_blocking(move || {
            // V1 files are uncompressed on disk — process directly without reading into memory
            let result =
                storage_helpers::process_uncompressed_temp_file(&file_info_clone.absolute_path)?;
            let output_path = result.compressed_path.clone();
            Ok::<_, anyhow::Error>((
                result.digest,
                result.logical_size,
                result.compressed_size,
                result.output_temp_file,
                output_path,
            ))
        })
        .await
        .context("Task panicked during file processing")?
        .context("Failed to read and process V1 file")?;

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks_storage = &app_state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = match app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, path)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e);
        }
    };

    if current_modified_after_lock >= file_info.last_modified {
        // File was migrated by another concurrent task, skip
        if let Err(e) = guard.release().await {
            warn!("Failed to release file lock: {}", e);
        }
        return Ok(false);
    }

    // Acquire hash lock for S3/refcount operations
    let hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &digest);
    let hash_lock = locks_storage.prepare_lock(hash_lock_key).await;
    let hash_guard = match hash_lock.acquire_exclusive().await {
        Ok(g) => g,
        Err(e) => {
            let _ = guard.release().await;
            return Err(e.context("Failed to acquire hash lock for migration"));
        }
    };

    // Check if blob already exists in S3
    let blob_exists = match app_state.s3storage.object_exists(&digest).await {
        Ok(v) => v,
        Err(e) => {
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    };

    // Upload from temp file (V1 files always processed on disk)
    if !blob_exists {
        let upload_result =
            match aws_sdk_s3::primitives::ByteStream::from_path(&compressed_path).await {
                Ok(bs) => {
                    app_state
                        .s3storage
                        .put_object_stream(&digest, bs, Some(compressed_size as i64))
                        .await
                }
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to open compressed temp file for S3 upload: {}",
                    e
                )),
            };
        if let Err(e) = upload_result {
            let _ = hash_guard.release().await;
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Record blob metadata (sizes + refcount increment)
    if let Err(e) = app_state
        .record_blob_metadata(&digest, logical_size, Some(compressed_size), None)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Release new hash lock
    if let Err(e) = hash_guard.release().await {
        warn!("Failed to release hash lock: {}", e);
    }

    // Handle overwriting existing file — decrement old hash refcount
    if current_modified > 0 {
        let old_hash = match app_state
            .kvstorage
            .get_ref_file(&app_state.bucket_name, path)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let _ = guard.release().await;
                return Err(e);
            }
        };
        if let Err(e) = app_state.decrement_old_ref(&old_hash, &digest).await {
            let _ = guard.release().await;
            return Err(e);
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .update_file_ref(path, &digest, file_info.last_modified)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = guard.release().await {
        warn!("Failed to release file lock: {}", e);
    }

    // Increment migration metric
    MIGRATION_FILES_MIGRATED
        .with_label_values(&[&app_state.bucket_name])
        .inc();

    Ok(true)
}
