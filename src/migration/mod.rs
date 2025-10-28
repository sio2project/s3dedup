use crate::AppState;
use crate::filetracker_client::{FileMetadata, FiletrackerClient};
use crate::routes::ft::storage_helpers;
use anyhow::{Context, Result};
use futures_util::future::join_all;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

pub mod v1_filesystem;

pub struct MigrationStats {
    pub total_files: usize,
    pub migrated: usize,
    pub failed: usize,
    pub skipped: usize,
}

/// Migrate all files from filetracker to s3dedup
pub async fn migrate_all_files(
    filetracker_client: Arc<FiletrackerClient>,
    app_state: Arc<AppState>,
    max_concurrency: usize,
) -> Result<MigrationStats> {
    info!("Starting offline migration from filetracker to s3dedup");
    info!("Max concurrency: {}", max_concurrency);

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
    let migrated = Arc::new(tokio::sync::Mutex::new(0usize));
    let failed = Arc::new(tokio::sync::Mutex::new(0usize));
    let skipped = Arc::new(tokio::sync::Mutex::new(0usize));

    // Process files in batches to avoid spawning millions of tasks
    // Use batch size = max_concurrency * 10 to keep task overhead reasonable
    let batch_size = max_concurrency * 10;
    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    for (batch_idx, batch) in files.chunks(batch_size).enumerate() {
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
            let failed = failed.clone();
            let skipped = skipped.clone();
            let semaphore = semaphore.clone();
            let path = path.clone();
            let file_idx = batch_start + idx_in_batch;

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        error!("Semaphore closed unexpectedly for file: {}", path);
                        *failed.lock().await += 1;
                        return;
                    }
                };

                // Log progress every 100 files
                if file_idx.is_multiple_of(100) && file_idx > 0 {
                    let current_migrated = *migrated.lock().await;
                    let current_failed = *failed.lock().await;
                    let current_skipped = *skipped.lock().await;
                    info!(
                        "Progress: {}/{} (migrated: {}, skipped: {}, failed: {})",
                        file_idx, total_files, current_migrated, current_skipped, current_failed
                    );
                }

                // Migrate the file
                match migrate_single_file(&filetracker_client, app_state, &path).await {
                    Ok(true) => {
                        *migrated.lock().await += 1;
                    }
                    Ok(false) => {
                        *skipped.lock().await += 1;
                    }
                    Err(e) => {
                        error!("Failed to migrate file {}: {}", path, e);
                        *failed.lock().await += 1;
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for this batch to complete before moving to next batch
        // TODO: Propagate errors? (Tokio returns `Err`, when thread from `JoinHandle` panicked).
        let _ = join_all(handles).await;
    }

    let migrated_count = *migrated.lock().await;
    let failed_count = *failed.lock().await;
    let skipped_count = *skipped.lock().await;

    info!("Migration complete:");
    info!("  Total files: {}", total_files);
    info!("  Migrated: {}", migrated_count);
    info!("  Skipped: {}", skipped_count);
    info!("  Failed: {}", failed_count);

    Ok(MigrationStats {
        total_files,
        migrated: migrated_count,
        failed: failed_count,
        skipped: skipped_count,
    })
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
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= file_metadata.last_modified {
        // File already exists with same or newer version, skip
        return Ok(());
    }

    // Process file data (decompress if needed, compute hash, compress)
    let uncompressed_data = if file_metadata.is_compressed {
        storage_helpers::decompress_gzip(&file_metadata.data)?
    } else {
        file_metadata.data.clone()
    };

    let digest = storage_helpers::compute_sha256(&uncompressed_data);
    let logical_size = uncompressed_data.len();

    // Always compress for storage
    let compressed_data = storage_helpers::compress_gzip(&uncompressed_data)?;

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks = &app_state.locks;
    let lock = locks.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = app_state
        .kvstorage
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified_after_lock >= file_metadata.last_modified {
        // File was migrated by another concurrent task, skip
        let _ = guard.release().await;
        return Ok(());
    }

    // Check if blob already exists in S3
    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&digest)
        .await?;

    // Store blob if it doesn't exist
    if !blob_exists {
        app_state
            .s3storage
            .lock()
            .await
            .put_object(&digest, compressed_data)
            .await?;
    }

    // Store logical size metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await?;

    // Increment reference count
    app_state
        .kvstorage
        .lock()
        .await
        .increment_ref_count(&app_state.bucket_name, &digest)
        .await?;

    // Handle overwriting existing file
    if current_modified > 0 {
        let old_hash = app_state
            .kvstorage
            .lock()
            .await
            .get_ref_file(&app_state.bucket_name, path)
            .await?;

        if !old_hash.is_empty() && old_hash != digest {
            // Decrement old reference count atomically and get new count
            let old_ref_count = app_state
                .kvstorage
                .lock()
                .await
                .decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            // Delete blob if no longer referenced
            if old_ref_count <= 0
                && let Err(e) = app_state
                    .s3storage
                    .lock()
                    .await
                    .delete_object(&old_hash)
                    .await
            {
                warn!(
                    "Failed to delete orphaned S3 object {} during migration: {}",
                    old_hash, e
                );
            }
        }
    }

    // Update file metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await?;

    app_state
        .kvstorage
        .lock()
        .await
        .set_modified(&app_state.bucket_name, path, file_metadata.last_modified)
        .await?;

    let _ = guard.release().await;
    Ok(())
}

/// Migrate a single file from filetracker to s3dedup
/// Returns Ok(true) if migrated, Ok(false) if skipped, Err if failed
async fn migrate_single_file(
    filetracker_client: &FiletrackerClient,
    app_state: Arc<AppState>,
    path: &str,
) -> Result<bool> {
    // Get file from filetracker
    let file_metadata = filetracker_client.get_file(path).await?;

    // Check if file already exists in s3dedup with same or newer version
    let current_modified = app_state
        .kvstorage
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= file_metadata.last_modified {
        // File already exists with same or newer version, skip
        return Ok(false);
    }

    // Process file data (decompress if needed, compute hash, compress)
    let uncompressed_data = if file_metadata.is_compressed {
        storage_helpers::decompress_gzip(&file_metadata.data)?
    } else {
        file_metadata.data.clone()
    };

    let digest = storage_helpers::compute_sha256(&uncompressed_data);
    let logical_size = uncompressed_data.len();

    // Always compress for storage
    let compressed_data = storage_helpers::compress_gzip(&uncompressed_data)?;

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks_storage = &app_state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = app_state
        .kvstorage
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified_after_lock >= file_metadata.last_modified {
        // File was migrated by another concurrent task, skip
        let _ = guard.release().await;
        return Ok(false);
    }

    // Check if blob already exists in S3
    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&digest)
        .await?;

    // Store blob if it doesn't exist
    if !blob_exists {
        app_state
            .s3storage
            .lock()
            .await
            .put_object(&digest, compressed_data)
            .await?;
    }

    // Store logical size metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await?;

    // Increment reference count
    app_state
        .kvstorage
        .lock()
        .await
        .increment_ref_count(&app_state.bucket_name, &digest)
        .await?;

    // Handle overwriting existing file
    if current_modified > 0 {
        let old_hash = app_state
            .kvstorage
            .lock()
            .await
            .get_ref_file(&app_state.bucket_name, path)
            .await?;

        if !old_hash.is_empty() && old_hash != digest {
            // Decrement old reference count atomically and get new count
            let old_ref_count = app_state
                .kvstorage
                .lock()
                .await
                .decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            // Delete blob if no longer referenced
            if old_ref_count <= 0
                && let Err(e) = app_state
                    .s3storage
                    .lock()
                    .await
                    .delete_object(&old_hash)
                    .await
            {
                warn!(
                    "Failed to delete orphaned S3 object {} during migration: {}",
                    old_hash, e
                );
            }
        }
    }

    // Update file metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await?;

    app_state
        .kvstorage
        .lock()
        .await
        .set_modified(&app_state.bucket_name, path, file_metadata.last_modified)
        .await?;

    let _ = guard.release().await;
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

/// Migrate all files from V1 filetracker filesystem to s3dedup
///
/// This function uses chunked processing to avoid loading all file metadata into memory,
/// making it suitable for directories with millions of files.
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
    info!("Processing directory in chunks to handle large file counts efficiently");

    // Track stats across all chunks using atomics to avoid async locks in sync context
    let total_files = Arc::new(AtomicUsize::new(0));
    let migrated = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(max_concurrency));

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
            let mut handles = vec![];

            for file_info in batch.iter() {
                let app_state = app_state.clone();
                let migrated = migrated.clone();
                let failed = failed.clone();
                let skipped = skipped.clone();
                let semaphore = semaphore.clone();
                let file_info = file_info.clone();

                let handle = tokio::spawn(async move {
                    // Acquire semaphore permit
                    let _permit = match semaphore.acquire().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            error!(
                                "Semaphore closed unexpectedly for file: {}",
                                file_info.relative_path
                            );
                            failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }
                    };

                    // Migrate the file
                    match migrate_single_file_from_v1_fs(app_state, &file_info).await {
                        Ok(true) => {
                            migrated.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(false) => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            error!("Failed to migrate file {}: {}", file_info.relative_path, e);
                            failed.fetch_add(1, Ordering::Relaxed);
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
                let current_failed = failed.load(Ordering::Relaxed);
                let current_skipped = skipped.load(Ordering::Relaxed);
                info!(
                    "Progress: {} files discovered (migrated: {}, skipped: {}, failed: {})",
                    total_so_far, current_migrated, current_skipped, current_failed
                );
            }
        }
    }

    // Wait for walker to complete
    match walker_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            error!("Filesystem walker failed: {}", e);
            anyhow::bail!("Filesystem walker failed: {}", e);
        }
        Err(e) => {
            error!("Walker task panicked: {}", e);
            anyhow::bail!("Walker task panicked: {}", e);
        }
    }

    let total_count = total_files.load(Ordering::Relaxed);
    let migrated_count = migrated.load(Ordering::Relaxed);
    let failed_count = failed.load(Ordering::Relaxed);
    let skipped_count = skipped.load(Ordering::Relaxed);

    info!("V1 filesystem migration complete:");
    info!("  Total files: {}", total_count);
    info!("  Migrated: {}", migrated_count);
    info!("  Skipped: {}", skipped_count);
    info!("  Failed: {}", failed_count);

    Ok(MigrationStats {
        total_files: total_count,
        migrated: migrated_count,
        failed: failed_count,
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
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified >= file_info.last_modified {
        // File already exists with same or newer version, skip
        return Ok(false);
    }

    // Move blocking operations (file I/O, SHA256, compression) to blocking thread pool
    // to avoid blocking the async runtime
    let file_info_clone = file_info.clone();
    let (uncompressed_data, digest, compressed_data) = tokio::task::spawn_blocking(move || {
        // Read file data from filesystem (blocking I/O)
        let uncompressed_data = v1_filesystem::read_v1_file(&file_info_clone)?;

        // Compute SHA256 hash (CPU-intensive)
        let digest = storage_helpers::compute_sha256(&uncompressed_data);

        // Compress data (CPU-intensive)
        let compressed_data = storage_helpers::compress_gzip(&uncompressed_data)?;

        Ok::<_, anyhow::Error>((uncompressed_data, digest, compressed_data))
    })
    .await
    .context("Task panicked during file processing")?
    .context("Failed to read and process V1 file")?;

    let logical_size = uncompressed_data.len();

    // Acquire file lock
    let lock_key = crate::locks::file_lock(&app_state.bucket_name, path);
    let locks_storage = &app_state.locks;
    let lock = locks_storage.prepare_lock(lock_key).await;
    let guard = lock
        .acquire_exclusive()
        .await
        .context("Failed to acquire exclusive lock for migration")?;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = app_state
        .kvstorage
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified_after_lock >= file_info.last_modified {
        // File was migrated by another concurrent task, skip
        let _ = guard.release().await;
        return Ok(false);
    }

    // Check if blob already exists in S3
    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&digest)
        .await?;

    // Store blob if it doesn't exist
    if !blob_exists {
        app_state
            .s3storage
            .lock()
            .await
            .put_object(&digest, compressed_data.clone())
            .await?;
    }

    // Store logical size metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_logical_size(&app_state.bucket_name, &digest, logical_size)
        .await?;

    // Store compressed size metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_compressed_size(&app_state.bucket_name, &digest, compressed_data.len())
        .await?;

    // Increment reference count
    app_state
        .kvstorage
        .lock()
        .await
        .increment_ref_count(&app_state.bucket_name, &digest)
        .await?;

    // Handle overwriting existing file
    if current_modified > 0 {
        let old_hash = app_state
            .kvstorage
            .lock()
            .await
            .get_ref_file(&app_state.bucket_name, path)
            .await?;

        if !old_hash.is_empty() && old_hash != digest {
            // Decrement old reference count atomically and get new count
            let old_ref_count = app_state
                .kvstorage
                .lock()
                .await
                .decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            // Delete blob if no longer referenced
            if old_ref_count <= 0
                && let Err(e) = app_state
                    .s3storage
                    .lock()
                    .await
                    .delete_object(&old_hash)
                    .await
            {
                warn!(
                    "Failed to delete orphaned S3 object {} during migration: {}",
                    old_hash, e
                );
            }
        }
    }

    // Update file metadata
    app_state
        .kvstorage
        .lock()
        .await
        .set_ref_file(&app_state.bucket_name, path, &digest)
        .await?;

    app_state
        .kvstorage
        .lock()
        .await
        .set_modified(&app_state.bucket_name, path, file_info.last_modified)
        .await?;

    let _ = guard.release().await;
    Ok(true)
}
