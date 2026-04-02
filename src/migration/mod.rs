use crate::AppState;
use crate::filetracker_client::{FileMetadata, FiletrackerClient};
use crate::metrics::MIGRATION_FILES_MIGRATED;
use crate::routes::ft::storage_helpers;
use anyhow::{Context, Result};
use futures_util::future::join_all;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

/// Default number of retries for failed file migrations
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff (in milliseconds)
const RETRY_BASE_DELAY_MS: u64 = 1000;

/// Maximum delay for infinite retry backoff (in milliseconds)
const MAX_RETRY_DELAY_MS: u64 = 60_000;

/// Maximum retries for non-transient errors before giving up (e.g. corrupt data)
const MAX_PERMANENT_ERROR_RETRIES: u32 = 5;

pub mod v1_filesystem;

pub struct MigrationStats {
    pub total_files: usize,
    pub migrated: usize,
    pub failed: usize,
    pub skipped: usize,
}

/// Result of a single file migration attempt with retries
enum MigrationResult {
    /// File was successfully migrated
    Migrated,
    /// File was skipped (already exists with same or newer version)
    Skipped,
    /// File failed permanently after all retries
    PermanentFailure(String, anyhow::Error),
}

/// Migrate all files from filetracker to s3dedup
///
/// If any file fails to migrate after retries, the entire migration is aborted.
pub async fn migrate_all_files(
    filetracker_client: Arc<FiletrackerClient>,
    app_state: Arc<AppState>,
    max_concurrency: usize,
) -> Result<MigrationStats> {
    info!("Starting offline migration from filetracker to s3dedup");
    info!("Max concurrency: {}", max_concurrency);
    info!(
        "Retry policy: {} retries with exponential backoff",
        DEFAULT_MAX_RETRIES
    );

    // List all files from filetracker
    // Don't pass timestamp parameter to avoid triggering a bug in the original Filetracker server
    // (it defaults to current time, which returns all files anyway)
    let files = filetracker_client.list_files("", None).await?;

    let total_files = files.len();
    info!("Found {} files to migrate", total_files);

    if total_files == 0 {
        return Ok(MigrationStats {
            total_files: 0,
            migrated: 0,
            failed: 0,
            skipped: 0,
        });
    }

    // Track stats
    let migrated = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));

    // Abort flag - set to true if any file permanently fails
    let abort_flag = Arc::new(AtomicBool::new(false));
    // Store the first permanent failure for error reporting
    let first_failure: Arc<tokio::sync::Mutex<Option<(String, String)>>> =
        Arc::new(tokio::sync::Mutex::new(None));

    // Process files in batches to avoid spawning millions of tasks
    // Use batch size = max_concurrency * 10 to keep task overhead reasonable
    let batch_size = max_concurrency * 10;
    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    for (batch_idx, batch) in files.chunks(batch_size).enumerate() {
        // Check if we should abort before starting a new batch
        if abort_flag.load(Ordering::SeqCst) {
            info!("Migration aborted due to permanent failure, stopping batch processing");
            break;
        }

        let batch_start = batch_idx * batch_size;
        info!(
            "Processing batch {}/{} (files {}-{})",
            batch_idx + 1,
            total_files.div_ceil(batch_size),
            batch_start,
            batch_start + batch.len()
        );

        // Spawn tasks for this batch only
        let mut handles = vec![];
        for (idx_in_batch, path) in batch.iter().enumerate() {
            let filetracker_client = filetracker_client.clone();
            let app_state = app_state.clone();
            let migrated = migrated.clone();
            let skipped = skipped.clone();
            let semaphore = semaphore.clone();
            let abort_flag = abort_flag.clone();
            let first_failure = first_failure.clone();
            let path = path.clone();
            let file_idx = batch_start + idx_in_batch;

            let handle = tokio::spawn(async move {
                // Check abort flag before starting
                if abort_flag.load(Ordering::SeqCst) {
                    return;
                }

                // Acquire semaphore permit
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        error!("Semaphore closed unexpectedly for file: {}", path);
                        return;
                    }
                };

                // Check abort flag again after acquiring permit
                if abort_flag.load(Ordering::SeqCst) {
                    return;
                }

                // Log progress every 100 files
                if file_idx.is_multiple_of(100) && file_idx > 0 {
                    let current_migrated = migrated.load(Ordering::Relaxed);
                    let current_skipped = skipped.load(Ordering::Relaxed);
                    info!(
                        "Progress: {}/{} (migrated: {}, skipped: {})",
                        file_idx, total_files, current_migrated, current_skipped
                    );
                }

                // Migrate the file with retries
                match migrate_single_file_with_retry(
                    &filetracker_client,
                    app_state,
                    &path,
                    DEFAULT_MAX_RETRIES,
                )
                .await
                {
                    MigrationResult::Migrated => {
                        migrated.fetch_add(1, Ordering::Relaxed);
                    }
                    MigrationResult::Skipped => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    MigrationResult::PermanentFailure(failed_path, err) => {
                        // Signal abort to all other tasks
                        abort_flag.store(true, Ordering::SeqCst);

                        // Store the first failure
                        let mut failure_guard = first_failure.lock().await;
                        if failure_guard.is_none() {
                            *failure_guard = Some((failed_path, err.to_string()));
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for this batch to complete before moving to next batch
        let _ = join_all(handles).await;
    }

    let migrated_count = migrated.load(Ordering::Relaxed);
    let skipped_count = skipped.load(Ordering::Relaxed);

    // Check if migration was aborted
    if abort_flag.load(Ordering::SeqCst) {
        let failure_info = first_failure.lock().await;
        if let Some((path, error_msg)) = failure_info.as_ref() {
            error!(
                "Migration aborted due to permanent failure of file '{}': {}",
                path, error_msg
            );
            anyhow::bail!(
                "Migration failed: file '{}' failed after {} retries: {}",
                path,
                DEFAULT_MAX_RETRIES,
                error_msg
            );
        } else {
            anyhow::bail!("Migration failed: unknown permanent failure");
        }
    }

    info!("Migration complete:");
    info!("  Total files: {}", total_files);
    info!("  Migrated: {}", migrated_count);
    info!("  Skipped: {}", skipped_count);

    Ok(MigrationStats {
        total_files,
        migrated: migrated_count,
        failed: 0,
        skipped: skipped_count,
    })
}

/// Migrate a single file with retry logic and exponential backoff
async fn migrate_single_file_with_retry(
    filetracker_client: &FiletrackerClient,
    app_state: Arc<AppState>,
    path: &str,
    max_retries: u32,
) -> MigrationResult {
    let mut last_error = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s, ...
            let delay_ms = RETRY_BASE_DELAY_MS
                .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(16)));
            warn!(
                "Retrying migration of '{}' (attempt {}/{}) after {}ms delay",
                path,
                attempt + 1,
                max_retries + 1,
                delay_ms
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        }

        match migrate_single_file(filetracker_client, app_state.clone(), path).await {
            Ok(true) => return MigrationResult::Migrated,
            Ok(false) => return MigrationResult::Skipped,
            Err(e) => {
                if attempt < max_retries {
                    warn!(
                        "Migration attempt {} for '{}' failed: {}",
                        attempt + 1,
                        path,
                        e
                    );
                }
                last_error = Some(e);
            }
        }
    }

    // All retries exhausted
    let err = last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error"));
    error!(
        "Permanent failure migrating '{}' after {} attempts: {}",
        path,
        max_retries + 1,
        err
    );
    MigrationResult::PermanentFailure(path.to_string(), err)
}

/// Migrate a single V1 file with retry logic and exponential backoff
async fn migrate_single_file_from_v1_fs_with_retry(
    app_state: Arc<AppState>,
    file_info: &v1_filesystem::V1FileInfo,
    max_retries: u32,
) -> MigrationResult {
    let mut last_error = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s, ...
            let delay_ms = RETRY_BASE_DELAY_MS
                .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(16)));
            warn!(
                "Retrying V1 migration of '{}' (attempt {}/{}) after {}ms delay",
                file_info.relative_path,
                attempt + 1,
                max_retries + 1,
                delay_ms
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        }

        match migrate_single_file_from_v1_fs(app_state.clone(), file_info).await {
            Ok(true) => return MigrationResult::Migrated,
            Ok(false) => return MigrationResult::Skipped,
            Err(e) => {
                if attempt < max_retries {
                    warn!(
                        "V1 migration attempt {} for '{}' failed: {}",
                        attempt + 1,
                        file_info.relative_path,
                        e
                    );
                }
                last_error = Some(e);
            }
        }
    }

    // All retries exhausted
    let err = last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error"));
    error!(
        "Permanent failure migrating V1 file '{}' after {} attempts: {}",
        file_info.relative_path,
        max_retries + 1,
        err
    );
    MigrationResult::PermanentFailure(file_info.relative_path.clone(), err)
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

    // Store logical size metadata
    if let Err(e) = app_state
        .kvstorage
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Store compressed size metadata
    if let Err(e) = app_state
        .kvstorage
        .set_compressed_size(&app_state.bucket_name, &digest, compressed_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Increment reference count atomically
    if let Err(e) = app_state
        .kvstorage
        .atomic_increment_ref_count(&app_state.bucket_name, &digest)
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

    // Handle overwriting existing file
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

        if !old_hash.is_empty() && old_hash != digest {
            // Acquire lock on old hash before decrement
            let old_hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &old_hash);
            let old_hash_lock = locks.prepare_lock(old_hash_lock_key).await;
            let old_hash_guard = match old_hash_lock.acquire_exclusive().await {
                Ok(g) => g,
                Err(e) => {
                    let _ = guard.release().await;
                    return Err(e.context("Failed to acquire old hash lock for migration"));
                }
            };

            // Decrement old reference count atomically and get new count
            let old_ref_count = match app_state
                .kvstorage
                .atomic_decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let _ = old_hash_guard.release().await;
                    let _ = guard.release().await;
                    return Err(e);
                }
            };

            // Delete blob if no longer referenced
            if old_ref_count <= 0
                && let Err(e) = app_state.s3storage.delete_object(&old_hash).await
            {
                warn!(
                    "Failed to delete orphaned S3 object (bucket={}, key={}) during migration: {}",
                    app_state.bucket_name, old_hash, e
                );
            }

            // Release old hash lock
            if let Err(e) = old_hash_guard.release().await {
                warn!("Failed to release old hash lock: {}", e);
            }
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .kvstorage
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = app_state
        .kvstorage
        .set_modified(&app_state.bucket_name, path, file_metadata.last_modified)
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

    if let Err(e) = app_state
        .kvstorage
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = app_state
        .kvstorage
        .set_compressed_size(&app_state.bucket_name, &digest, compressed_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = app_state
        .kvstorage
        .atomic_increment_ref_count(&app_state.bucket_name, &digest)
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

        if !old_hash.is_empty() && old_hash != digest {
            let old_hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &old_hash);
            let old_hash_lock = locks.prepare_lock(old_hash_lock_key).await;
            let old_hash_guard = match old_hash_lock.acquire_exclusive().await {
                Ok(g) => g,
                Err(e) => {
                    let _ = guard.release().await;
                    return Err(e.context("Failed to acquire old hash lock for migration"));
                }
            };

            let old_ref_count = match app_state
                .kvstorage
                .atomic_decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let _ = old_hash_guard.release().await;
                    let _ = guard.release().await;
                    return Err(e);
                }
            };

            if old_ref_count <= 0
                && let Err(e) = app_state.s3storage.delete_object(&old_hash).await
            {
                warn!(
                    "Failed to delete orphaned S3 object during migration: {}",
                    e
                );
            }

            if let Err(e) = old_hash_guard.release().await {
                warn!("Failed to release old hash lock: {}", e);
            }
        }
    }

    if let Err(e) = app_state
        .kvstorage
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = app_state
        .kvstorage
        .set_modified(&app_state.bucket_name, path, streaming_meta.last_modified)
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

/// Migrate a single file from filetracker to s3dedup
/// Returns Ok(true) if migrated, Ok(false) if skipped, Err if failed
async fn migrate_single_file(
    filetracker_client: &FiletrackerClient,
    app_state: Arc<AppState>,
    path: &str,
) -> Result<bool> {
    use crate::filetracker_client::DownloadedFile;

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

    // Store logical size metadata
    if let Err(e) = app_state
        .kvstorage
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Store compressed size metadata
    if let Err(e) = app_state
        .kvstorage
        .set_compressed_size(&app_state.bucket_name, &digest, compressed_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Increment reference count atomically
    if let Err(e) = app_state
        .kvstorage
        .atomic_increment_ref_count(&app_state.bucket_name, &digest)
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

    // Handle overwriting existing file
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

        if !old_hash.is_empty() && old_hash != digest {
            // Acquire lock on old hash before decrement
            let old_hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &old_hash);
            let old_hash_lock = locks_storage.prepare_lock(old_hash_lock_key).await;
            let old_hash_guard = match old_hash_lock.acquire_exclusive().await {
                Ok(g) => g,
                Err(e) => {
                    let _ = guard.release().await;
                    return Err(e.context("Failed to acquire old hash lock for migration"));
                }
            };

            // Decrement old reference count atomically and get new count
            let old_ref_count = match app_state
                .kvstorage
                .atomic_decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let _ = old_hash_guard.release().await;
                    let _ = guard.release().await;
                    return Err(e);
                }
            };

            // Delete blob if no longer referenced
            if old_ref_count <= 0
                && let Err(e) = app_state.s3storage.delete_object(&old_hash).await
            {
                warn!(
                    "Failed to delete orphaned S3 object (bucket={}, key={}) during migration: {}",
                    app_state.bucket_name, old_hash, e
                );
            }

            // Release old hash lock
            if let Err(e) = old_hash_guard.release().await {
                warn!("Failed to release old hash lock: {}", e);
            }
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .kvstorage
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = app_state
        .kvstorage
        .set_modified(&app_state.bucket_name, path, last_modified)
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

/// Background worker for live migration
/// This runs the same migration as offline migration, but in the background while the server is running
pub async fn live_migration_worker(
    filetracker_client: Arc<FiletrackerClient>,
    app_state: Arc<AppState>,
    max_concurrency: usize,
) {
    info!("Starting background migration worker");
    info!("Max concurrency: {}", max_concurrency);

    // Run the same migration logic as offline migration
    match migrate_all_files(filetracker_client, app_state, max_concurrency).await {
        Ok(stats) => {
            info!("Background migration completed successfully");
            info!("Total files: {}", stats.total_files);
            info!("Migrated: {}", stats.migrated);
            info!("Skipped: {}", stats.skipped);
            info!("Failed: {}", stats.failed);

            if stats.failed > 0 {
                warn!("{} files failed to migrate", stats.failed);
            }
        }
        Err(e) => {
            error!("Background migration failed: {}", e);
        }
    }

    // Reset migration_active gauge to indicate migration is complete
    crate::metrics::MIGRATION_ACTIVE.set(0);
    info!("Background migration worker finished, migration_active set to 0");
}

/// Migrate a single file with retry logic:
/// - If the file is not found on filetracker (404), skip it immediately (file was deleted).
/// - Transient errors (connection failures, timeouts, 5xx): retry forever with capped
///   exponential backoff up to MAX_RETRY_DELAY_MS (60s).
/// - Non-transient errors (corrupt data, bad headers): retry up to MAX_PERMANENT_ERROR_RETRIES
///   times, then skip with an error log.
async fn migrate_single_file_with_infinite_retry(
    filetracker_client: &FiletrackerClient,
    app_state: Arc<AppState>,
    path: &str,
) -> MigrationResult {
    let mut attempt: u32 = 0;
    let mut permanent_error_count: u32 = 0;
    loop {
        if attempt > 0 {
            let delay_ms = std::cmp::min(
                RETRY_BASE_DELAY_MS
                    .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(16))),
                MAX_RETRY_DELAY_MS,
            );
            warn!(
                "Retrying migration of '{}' (attempt {}) after {}ms delay",
                path,
                attempt + 1,
                delay_ms
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        }
        match migrate_single_file(filetracker_client, app_state.clone(), path).await {
            Ok(true) => return MigrationResult::Migrated,
            Ok(false) => return MigrationResult::Skipped,
            Err(e) => {
                // If file was not found (404), skip it — the file was likely deleted
                if e.downcast_ref::<crate::filetracker_client::FileNotFoundError>()
                    .is_some()
                {
                    warn!("File '{}' not found on filetracker, skipping", path);
                    return MigrationResult::Skipped;
                }

                let is_transient = e
                    .downcast_ref::<crate::filetracker_client::TransientError>()
                    .is_some();

                if is_transient {
                    // Transient error (connection failure, timeout, 5xx) — retry forever
                    warn!(
                        "Transient error migrating '{}' (attempt {}): {}",
                        path,
                        attempt + 1,
                        e
                    );
                } else {
                    // Non-transient error (corrupt data, bad headers) — limited retries
                    permanent_error_count += 1;
                    if permanent_error_count >= MAX_PERMANENT_ERROR_RETRIES {
                        error!(
                            "Permanent failure migrating '{}' after {} attempts, skipping: {}",
                            path, permanent_error_count, e
                        );
                        return MigrationResult::PermanentFailure(path.to_string(), e);
                    }
                    warn!(
                        "Non-transient error migrating '{}' (attempt {}/{}): {}",
                        path, permanent_error_count, MAX_PERMANENT_ERROR_RETRIES, e
                    );
                }

                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Migrate all files listed in a file, one path per line.
/// Uses chunked streaming to avoid loading the entire file into memory.
/// Uses infinite retry so transient filetracker failures don't abort migration.
pub async fn migrate_all_files_from_file_list(
    file_list_path: &str,
    filetracker_client: Arc<FiletrackerClient>,
    app_state: Arc<AppState>,
    max_concurrency: usize,
) -> Result<MigrationStats> {
    info!("Starting file-list migration from: {}", file_list_path);
    info!("Max concurrency: {}", max_concurrency);
    info!(
        "Retry policy: infinite retry for transient errors, {} retries for permanent errors, backoff max {}ms",
        MAX_PERMANENT_ERROR_RETRIES, MAX_RETRY_DELAY_MS
    );
    info!("Reading file list in chunks to handle large file counts efficiently");

    let total_files = Arc::new(AtomicUsize::new(0));
    let migrated = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    let file_chunk_size = 10_000;
    let task_batch_size = max_concurrency * 10;

    // Create a channel to send chunks from blocking reader to async processor
    let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<String>>();

    let path_owned = file_list_path.to_string();

    // Spawn the file reader in a blocking task
    let reader_handle = tokio::task::spawn_blocking(move || {
        use std::io::BufRead;
        let file = std::fs::File::open(&path_owned)
            .with_context(|| format!("Failed to open file list: {}", path_owned))?;
        let reader = std::io::BufReader::new(file);
        let mut chunk = Vec::with_capacity(file_chunk_size);
        for line in reader.lines() {
            let line = line.context("Failed to read line from file list")?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Strip leading '/' to normalize paths
            let path = trimmed.strip_prefix('/').unwrap_or(trimmed).to_string();
            chunk.push(path);
            if chunk.len() >= file_chunk_size
                && chunk_tx
                    .send(std::mem::replace(
                        &mut chunk,
                        Vec::with_capacity(file_chunk_size),
                    ))
                    .is_err()
            {
                anyhow::bail!("Chunk receiver dropped");
            }
        }
        if !chunk.is_empty() && chunk_tx.send(chunk).is_err() {
            anyhow::bail!("Chunk receiver dropped");
        }
        Ok::<_, anyhow::Error>(())
    });

    // Process chunks as they arrive
    let mut chunk_count = 0;
    let mut files_offset = 0usize; // running offset for accurate progress logging
    while let Some(file_chunk) = chunk_rx.recv().await {
        chunk_count += 1;
        let chunk_size = file_chunk.len();
        total_files.fetch_add(chunk_size, Ordering::Relaxed);
        let total_so_far = total_files.load(Ordering::Relaxed);

        info!(
            "Processing file-list chunk {} with {} files (total discovered: {})",
            chunk_count, chunk_size, total_so_far
        );

        // Process this chunk in task batches
        let total_batches = file_chunk.chunks(task_batch_size).len();
        for (batch_idx, batch) in file_chunk.chunks(task_batch_size).enumerate() {
            let batch_start = files_offset + batch_idx * task_batch_size;

            if batch_idx.is_multiple_of(10) {
                let current_migrated = migrated.load(Ordering::Relaxed);
                let current_skipped = skipped.load(Ordering::Relaxed);
                let current_failed = failed.load(Ordering::Relaxed);
                info!(
                    "Progress: file ~{}/{} (migrated: {}, skipped: {}, failed: {}) [chunk {} batch {}/{}]",
                    batch_start,
                    total_so_far,
                    current_migrated,
                    current_skipped,
                    current_failed,
                    chunk_count,
                    batch_idx + 1,
                    total_batches
                );
            }

            let mut handles = vec![];

            for path in batch.iter() {
                let filetracker_client = filetracker_client.clone();
                let app_state = app_state.clone();
                let migrated = migrated.clone();
                let skipped = skipped.clone();
                let failed = failed.clone();
                let semaphore = semaphore.clone();
                let path = path.clone();

                let handle = tokio::spawn(async move {
                    // Acquire semaphore permit
                    let _permit = match semaphore.acquire().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            error!("Semaphore closed unexpectedly for file: {}", path);
                            return;
                        }
                    };

                    match migrate_single_file_with_infinite_retry(
                        &filetracker_client,
                        app_state,
                        &path,
                    )
                    .await
                    {
                        MigrationResult::Migrated => {
                            migrated.fetch_add(1, Ordering::Relaxed);
                        }
                        MigrationResult::Skipped => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                        }
                        MigrationResult::PermanentFailure(path, err) => {
                            error!("Permanent failure for '{}': {}", path, err);
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });

                handles.push(handle);
            }

            // Wait for this batch to complete before moving to next batch
            let _ = join_all(handles).await;
        }

        files_offset += chunk_size;
    }

    // Wait for the reader to finish and check for errors
    match reader_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            error!("File list reader failed: {}", e);
            return Err(e);
        }
        Err(e) => {
            error!("File list reader task panicked: {}", e);
            anyhow::bail!("File list reader task panicked: {}", e);
        }
    }

    let total = total_files.load(Ordering::Relaxed);
    let migrated_count = migrated.load(Ordering::Relaxed);
    let skipped_count = skipped.load(Ordering::Relaxed);
    let failed_count = failed.load(Ordering::Relaxed);

    info!("File-list migration complete:");
    info!("  Total files: {}", total);
    info!("  Migrated: {}", migrated_count);
    info!("  Skipped: {}", skipped_count);
    info!("  Failed: {}", failed_count);

    if failed_count > 0 {
        warn!(
            "{} files failed permanently (corrupt data or other non-transient errors)",
            failed_count
        );
    }

    Ok(MigrationStats {
        total_files: total,
        migrated: migrated_count,
        failed: failed_count,
        skipped: skipped_count,
    })
}

/// Background worker for live migration from a file list
/// This runs file-list based migration in the background while the server is running
pub async fn live_migration_worker_from_file_list(
    file_list_path: String,
    filetracker_client: Arc<FiletrackerClient>,
    app_state: Arc<AppState>,
    max_concurrency: usize,
) {
    info!(
        "Starting background file-list migration worker from: {}",
        file_list_path
    );
    info!("Max concurrency: {}", max_concurrency);

    match migrate_all_files_from_file_list(
        &file_list_path,
        filetracker_client,
        app_state,
        max_concurrency,
    )
    .await
    {
        Ok(stats) => {
            info!("Background file-list migration completed");
            info!("Total files: {}", stats.total_files);
            info!("Migrated: {}", stats.migrated);
            info!("Skipped: {}", stats.skipped);
            info!("Failed: {}", stats.failed);

            if stats.failed > 0 {
                warn!(
                    "{} files failed permanently — check error logs above for details",
                    stats.failed
                );
            }
        }
        Err(e) => {
            error!("Background file-list migration failed: {}", e);
        }
    }

    crate::metrics::MIGRATION_ACTIVE.set(0);
    info!("Background file-list migration worker finished, migration_active set to 0");
}

/// Migrate all files from V1 filetracker filesystem to s3dedup
///
/// This function uses chunked processing to avoid loading all file metadata into memory,
/// making it suitable for directories with millions of files.
///
/// If any file fails to migrate after retries, the entire migration is aborted.
pub async fn migrate_all_files_from_v1_fs(
    v1_dir: &str,
    app_state: Arc<AppState>,
    max_concurrency: usize,
) -> Result<MigrationStats> {
    info!(
        "Starting V1 filesystem migration from directory: {}",
        v1_dir
    );
    info!("Max concurrency: {}", max_concurrency);
    info!(
        "Retry policy: {} retries with exponential backoff",
        DEFAULT_MAX_RETRIES
    );
    info!("Processing directory in chunks to handle large file counts efficiently");

    // Track stats across all chunks using atomics to avoid async locks in sync context
    let total_files = Arc::new(AtomicUsize::new(0));
    let migrated = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    // Abort flag - set to true if any file permanently fails
    let abort_flag = Arc::new(AtomicBool::new(false));
    // Store the first permanent failure for error reporting
    let first_failure: Arc<tokio::sync::Mutex<Option<(String, String)>>> =
        Arc::new(tokio::sync::Mutex::new(None));

    // Chunk size for filesystem walking: 10,000 files per chunk
    // This keeps memory usage reasonable while still being efficient
    let filesystem_chunk_size = 10_000;

    // Task batch size: spawn tasks in smaller batches to avoid too many concurrent tasks
    let task_batch_size = max_concurrency * 10;

    // Create a channel to send chunks from blocking walker to async processor
    let (chunk_tx, mut chunk_rx) =
        tokio::sync::mpsc::unbounded_channel::<Vec<v1_filesystem::V1FileInfo>>();

    let v1_dir_owned = v1_dir.to_string();

    // Spawn the filesystem walker in a blocking task to avoid nested block_on
    let walker_handle = tokio::task::spawn_blocking(move || {
        v1_filesystem::walk_v1_directory_chunked(
            &v1_dir_owned,
            filesystem_chunk_size,
            |file_chunk| {
                // Send chunk to async processor
                // If receiver is dropped, stop walking
                if chunk_tx.send(file_chunk.to_vec()).is_err() {
                    anyhow::bail!("Chunk receiver dropped");
                }
                Ok(())
            },
        )
    });

    // Process chunks as they arrive
    let mut chunk_count = 0;
    while let Some(file_chunk) = chunk_rx.recv().await {
        // Check if we should abort before processing a new chunk
        if abort_flag.load(Ordering::SeqCst) {
            info!("V1 migration aborted due to permanent failure, stopping chunk processing");
            break;
        }

        chunk_count += 1;
        let chunk_size = file_chunk.len();
        total_files.fetch_add(chunk_size, Ordering::Relaxed);
        let total_so_far = total_files.load(Ordering::Relaxed);

        info!(
            "Processing filesystem chunk {} with {} files (total discovered: {})",
            chunk_count, chunk_size, total_so_far
        );

        // Process this chunk in task batches
        let total_batches = file_chunk.chunks(task_batch_size).len();
        for (batch_idx, batch) in file_chunk.chunks(task_batch_size).enumerate() {
            // Check if we should abort before starting a new batch
            if abort_flag.load(Ordering::SeqCst) {
                info!("V1 migration aborted due to permanent failure, stopping batch processing");
                break;
            }

            let mut handles = vec![];

            for file_info in batch.iter() {
                let app_state = app_state.clone();
                let migrated = migrated.clone();
                let skipped = skipped.clone();
                let semaphore = semaphore.clone();
                let abort_flag = abort_flag.clone();
                let first_failure = first_failure.clone();
                let file_info = file_info.clone();

                let handle = tokio::spawn(async move {
                    // Check abort flag before starting
                    if abort_flag.load(Ordering::SeqCst) {
                        return;
                    }

                    // Acquire semaphore permit
                    let _permit = match semaphore.acquire().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            error!(
                                "Semaphore closed unexpectedly for file: {}",
                                file_info.relative_path
                            );
                            return;
                        }
                    };

                    // Check abort flag again after acquiring permit
                    if abort_flag.load(Ordering::SeqCst) {
                        return;
                    }

                    // Migrate the file with retries
                    match migrate_single_file_from_v1_fs_with_retry(
                        app_state,
                        &file_info,
                        DEFAULT_MAX_RETRIES,
                    )
                    .await
                    {
                        MigrationResult::Migrated => {
                            migrated.fetch_add(1, Ordering::Relaxed);
                        }
                        MigrationResult::Skipped => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                        }
                        MigrationResult::PermanentFailure(failed_path, err) => {
                            // Signal abort to all other tasks
                            abort_flag.store(true, Ordering::SeqCst);

                            // Store the first failure
                            let mut failure_guard = first_failure.lock().await;
                            if failure_guard.is_none() {
                                *failure_guard = Some((failed_path, err.to_string()));
                            }
                        }
                    }
                });

                handles.push(handle);
            }

            // Wait for this task batch to complete
            let _ = join_all(handles).await;

            // Log progress periodically
            if batch_idx % 10 == 0 || batch_idx == total_batches - 1 {
                let current_migrated = migrated.load(Ordering::Relaxed);
                let current_skipped = skipped.load(Ordering::Relaxed);
                info!(
                    "Progress: {} files discovered (migrated: {}, skipped: {})",
                    total_so_far, current_migrated, current_skipped
                );
            }
        }
    }

    // Wait for walker to complete (ignore errors if we're aborting)
    match walker_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            // Only report walker errors if we're not already aborting
            if !abort_flag.load(Ordering::SeqCst) {
                error!("Filesystem walker failed: {}", e);
                anyhow::bail!("Filesystem walker failed: {}", e);
            }
        }
        Err(e) => {
            // Only report walker panics if we're not already aborting
            if !abort_flag.load(Ordering::SeqCst) {
                error!("Walker task panicked: {}", e);
                anyhow::bail!("Walker task panicked: {}", e);
            }
        }
    }

    let total_count = total_files.load(Ordering::Relaxed);
    let migrated_count = migrated.load(Ordering::Relaxed);
    let skipped_count = skipped.load(Ordering::Relaxed);

    // Check if migration was aborted
    if abort_flag.load(Ordering::SeqCst) {
        let failure_info = first_failure.lock().await;
        if let Some((path, error_msg)) = failure_info.as_ref() {
            error!(
                "V1 migration aborted due to permanent failure of file '{}': {}",
                path, error_msg
            );
            anyhow::bail!(
                "V1 migration failed: file '{}' failed after {} retries: {}",
                path,
                DEFAULT_MAX_RETRIES,
                error_msg
            );
        } else {
            anyhow::bail!("V1 migration failed: unknown permanent failure");
        }
    }

    info!("V1 filesystem migration complete:");
    info!("  Total files: {}", total_count);
    info!("  Migrated: {}", migrated_count);
    info!("  Skipped: {}", skipped_count);

    Ok(MigrationStats {
        total_files: total_count,
        migrated: migrated_count,
        failed: 0,
        skipped: skipped_count,
    })
}

/// Migrate a single file from V1 filetracker filesystem to s3dedup
/// Returns Ok(true) if migrated, Ok(false) if skipped, Err if failed
async fn migrate_single_file_from_v1_fs(
    app_state: Arc<AppState>,
    file_info: &v1_filesystem::V1FileInfo,
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

    // Store logical size metadata
    if let Err(e) = app_state
        .kvstorage
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Store compressed size metadata
    if let Err(e) = app_state
        .kvstorage
        .set_compressed_size(&app_state.bucket_name, &digest, compressed_size)
        .await
    {
        let _ = hash_guard.release().await;
        let _ = guard.release().await;
        return Err(e);
    }

    // Increment reference count atomically
    if let Err(e) = app_state
        .kvstorage
        .atomic_increment_ref_count(&app_state.bucket_name, &digest)
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

    // Handle overwriting existing file
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

        if !old_hash.is_empty() && old_hash != digest {
            // Acquire lock on old hash before decrement
            let old_hash_lock_key = crate::locks::hash_lock(&app_state.bucket_name, &old_hash);
            let old_hash_lock = locks_storage.prepare_lock(old_hash_lock_key).await;
            let old_hash_guard = match old_hash_lock.acquire_exclusive().await {
                Ok(g) => g,
                Err(e) => {
                    let _ = guard.release().await;
                    return Err(e.context("Failed to acquire old hash lock for migration"));
                }
            };

            // Decrement old reference count atomically and get new count
            let old_ref_count = match app_state
                .kvstorage
                .atomic_decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let _ = old_hash_guard.release().await;
                    let _ = guard.release().await;
                    return Err(e);
                }
            };

            // Delete blob if no longer referenced
            if old_ref_count <= 0
                && let Err(e) = app_state.s3storage.delete_object(&old_hash).await
            {
                warn!(
                    "Failed to delete orphaned S3 object (bucket={}, key={}) during migration: {}",
                    app_state.bucket_name, old_hash, e
                );
            }

            // Release old hash lock
            if let Err(e) = old_hash_guard.release().await {
                warn!("Failed to release old hash lock: {}", e);
            }
        }
    }

    // Update file metadata
    if let Err(e) = app_state
        .kvstorage
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await
    {
        let _ = guard.release().await;
        return Err(e);
    }

    if let Err(e) = app_state
        .kvstorage
        .set_modified(&app_state.bucket_name, path, file_info.last_modified)
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
