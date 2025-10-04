use crate::AppState;
use crate::filetracker_client::{FileMetadata, FiletrackerClient};
use crate::routes::ft::storage_helpers;
use anyhow::Result;
use futures_util::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

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

    // List all files from filetracker (using far-future timestamp to get all files)
    let timestamp = chrono::Utc::now().timestamp() + 100_000_000; // ~3 years in future
    let files = filetracker_client.list_files("", timestamp).await?;

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
                let _permit = semaphore.acquire().await.unwrap();

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
    let _guard = lock.acquire_exclusive().await;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = app_state
        .kvstorage
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified_after_lock >= file_metadata.last_modified {
        // File was migrated by another concurrent task, skip
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
            // Decrement old reference count
            app_state
                .kvstorage
                .lock()
                .await
                .decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            // Check if we should delete the old blob
            let old_ref_count = app_state
                .kvstorage
                .lock()
                .await
                .get_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            if old_ref_count <= 0 {
                let _ = app_state
                    .s3storage
                    .lock()
                    .await
                    .delete_object(&old_hash)
                    .await;
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
    let _guard = lock.acquire_exclusive().await;

    // Recheck if file was already migrated after acquiring lock (race condition protection)
    let current_modified_after_lock = app_state
        .kvstorage
        .lock()
        .await
        .get_modified(&app_state.bucket_name, path)
        .await?;

    if current_modified_after_lock >= file_metadata.last_modified {
        // File was migrated by another concurrent task, skip
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
            // Decrement old reference count
            app_state
                .kvstorage
                .lock()
                .await
                .decrement_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            // Check if we should delete the old blob
            let old_ref_count = app_state
                .kvstorage
                .lock()
                .await
                .get_ref_count(&app_state.bucket_name, &old_hash)
                .await?;

            if old_ref_count <= 0 {
                let _ = app_state
                    .s3storage
                    .lock()
                    .await
                    .delete_object(&old_hash)
                    .await;
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
