use crate::kvstorage::KVStorage;
use crate::locks::{self, LocksStorage};
use crate::metrics::{
    CLEANER_DELETED_BLOBS_TOTAL, CLEANER_ERRORS_TOTAL, CLEANER_FREED_BYTES_TOTAL,
    CLEANER_LAST_RUN_TIMESTAMP, CLEANER_TOTAL_RUNS,
};
use crate::s3storage::S3Storage;
use anyhow::Result;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

#[derive(Debug, Deserialize, Clone)]
pub struct CleanerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_interval_seconds")]
    pub interval_seconds: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_max_deletes_per_run")]
    pub max_deletes_per_run: usize,
}

fn default_interval_seconds() -> u64 {
    3600 // 1 hour
}

fn default_batch_size() -> usize {
    1000
}

fn default_max_deletes_per_run() -> usize {
    10000
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: default_interval_seconds(),
            batch_size: default_batch_size(),
            max_deletes_per_run: default_max_deletes_per_run(),
        }
    }
}

pub struct Cleaner {
    bucket_name: String,
    kvstorage: Arc<Mutex<Box<KVStorage>>>,
    s3storage: Arc<Mutex<Box<S3Storage>>>,
    locks: Arc<LocksStorage>,
    config: CleanerConfig,
}

impl Cleaner {
    pub fn new(
        bucket_name: String,
        kvstorage: Arc<Mutex<Box<KVStorage>>>,
        s3storage: Arc<Mutex<Box<S3Storage>>>,
        locks: Arc<LocksStorage>,
        config: CleanerConfig,
    ) -> Self {
        Self {
            bucket_name,
            kvstorage,
            s3storage,
            locks,
            config,
        }
    }

    /// Start the cleaner task
    pub fn start(self: Arc<Self>) {
        if !self.config.enabled {
            info!("Cleaner disabled for bucket: {}", self.bucket_name);
            return;
        }

        info!(
            "Starting cleaner for bucket: {} with interval: {}s",
            self.bucket_name, self.config.interval_seconds
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                self.config.interval_seconds,
            ));

            loop {
                interval.tick().await;
                info!("Running cleanup cycle for bucket: {}", self.bucket_name);

                if let Err(e) = self.run_cleanup().await {
                    CLEANER_ERRORS_TOTAL
                        .with_label_values(&[&self.bucket_name])
                        .inc();
                    error!(
                        "Cleanup cycle failed for bucket {}: {}",
                        self.bucket_name, e
                    );
                }
            }
        });
    }

    /// Run a full cleanup cycle
    pub async fn run_cleanup(&self) -> Result<()> {
        // Increment run counter and update timestamp at start
        // This ensures both metrics are consistent even if the run fails
        CLEANER_TOTAL_RUNS
            .with_label_values(&[&self.bucket_name])
            .inc();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        CLEANER_LAST_RUN_TIMESTAMP
            .with_label_values(&[&self.bucket_name])
            .set(timestamp);

        let mut total_deletes = 0;
        let mut total_bytes_freed: u64 = 0;

        // Case 1: Clean ref_files pointing to non-existent hashes
        info!("Phase 1: Cleaning ref_files with missing hashes");
        total_deletes += self.clean_orphaned_ref_files().await?;

        if total_deletes >= self.config.max_deletes_per_run {
            warn!(
                "Reached max deletes limit ({}) in phase 1, stopping cleanup cycle",
                self.config.max_deletes_per_run
            );
            self.update_cleanup_metrics(total_deletes, total_bytes_freed);
            return Ok(());
        }

        // Case 2: Clean refcounts with no corresponding ref_files
        info!("Phase 2: Cleaning refcounts with no ref_files");
        total_deletes += self.clean_unreferenced_refcounts().await?;

        if total_deletes >= self.config.max_deletes_per_run {
            warn!(
                "Reached max deletes limit ({}) in phase 2, stopping cleanup cycle",
                self.config.max_deletes_per_run
            );
            self.update_cleanup_metrics(total_deletes, total_bytes_freed);
            return Ok(());
        }

        // Case 3: Clean S3 objects with no refcount or refcount = 0
        info!("Phase 3: Cleaning S3 objects with no refcount or refcount = 0");
        let (s3_deletes, s3_bytes_freed) = self.clean_unused_s3_objects().await?;
        total_deletes += s3_deletes;
        total_bytes_freed += s3_bytes_freed;

        if total_deletes >= self.config.max_deletes_per_run {
            warn!(
                "Reached max deletes limit ({}) in phase 3, stopping cleanup cycle",
                self.config.max_deletes_per_run
            );
            self.update_cleanup_metrics(total_deletes, total_bytes_freed);
            return Ok(());
        }

        // Case 4: Clean logical_size entries with no refcount
        info!("Phase 4: Cleaning logical_size entries with no refcount");
        total_deletes += self.clean_orphaned_logical_sizes().await?;

        info!(
            "Cleanup cycle complete for bucket: {} (total items deleted: {}, bytes freed: {})",
            self.bucket_name, total_deletes, total_bytes_freed
        );

        self.update_cleanup_metrics(total_deletes, total_bytes_freed);
        Ok(())
    }

    /// Update cleanup metrics after a run
    fn update_cleanup_metrics(&self, total_deletes: usize, total_bytes_freed: u64) {
        // Update timestamp (use unwrap_or to handle edge case of system time before UNIX_EPOCH)
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        CLEANER_LAST_RUN_TIMESTAMP
            .with_label_values(&[&self.bucket_name])
            .set(timestamp);

        // Update deleted blobs counter
        CLEANER_DELETED_BLOBS_TOTAL
            .with_label_values(&[&self.bucket_name])
            .inc_by(total_deletes as u64);

        // Update freed bytes counter
        CLEANER_FREED_BYTES_TOTAL
            .with_label_values(&[&self.bucket_name])
            .inc_by(total_bytes_freed);
    }

    /// Clean ref_files that point to non-existent hashes in refcount table
    async fn clean_orphaned_ref_files(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut offset = 0;

        loop {
            let ref_files = self
                .kvstorage
                .lock()
                .await
                .list_ref_files_batch(&self.bucket_name, self.config.batch_size, offset)
                .await?;

            if ref_files.is_empty() {
                break;
            }

            let batch_len = ref_files.len();
            let deleted_before = deleted_count;

            for (path, hash) in ref_files.iter() {
                let refcount = self
                    .kvstorage
                    .lock()
                    .await
                    .get_ref_count(&self.bucket_name, hash)
                    .await?;

                if refcount == 0 {
                    debug!(
                        "Found orphaned ref_file: path={}, hash={} (refcount=0)",
                        path, hash
                    );

                    // Acquire file lock before deleting ref_file to prevent race with PUT
                    let lock_key = locks::file_lock(&self.bucket_name, path);
                    let lock = self.locks.prepare_lock(lock_key).await;
                    let guard = match lock.acquire_exclusive().await {
                        Ok(g) => g,
                        Err(e) => {
                            warn!("Failed to acquire file lock for cleaner {}: {}", path, e);
                            continue; // Skip this file, try next
                        }
                    };

                    // Re-check refcount after acquiring lock (double-check pattern)
                    let refcount_after_lock = self
                        .kvstorage
                        .lock()
                        .await
                        .get_ref_count(&self.bucket_name, hash)
                        .await;

                    let refcount_after_lock = match refcount_after_lock {
                        Ok(r) => r,
                        Err(e) => {
                            error!("Failed to re-check refcount for {}: {}", hash, e);
                            if let Err(e) = guard.release().await {
                                warn!("Failed to release file lock: {}", e);
                            }
                            continue;
                        }
                    };

                    if refcount_after_lock != 0 {
                        // Refcount changed while we were acquiring lock, skip
                        debug!(
                            "Refcount changed for {} (now {}), skipping",
                            hash, refcount_after_lock
                        );
                        if let Err(e) = guard.release().await {
                            warn!("Failed to release file lock: {}", e);
                        }
                        continue;
                    }

                    // Delete ref_file and modified entries
                    if let Err(e) = self
                        .kvstorage
                        .lock()
                        .await
                        .delete_ref_file(&self.bucket_name, path)
                        .await
                    {
                        error!("Failed to delete ref_file {}: {}", path, e);
                        if let Err(e) = guard.release().await {
                            warn!("Failed to release file lock: {}", e);
                        }
                        continue;
                    }

                    if let Err(e) = self
                        .kvstorage
                        .lock()
                        .await
                        .delete_modified(&self.bucket_name, path)
                        .await
                    {
                        error!("Failed to delete modified entry for {}: {}", path, e);
                    }

                    if let Err(e) = guard.release().await {
                        warn!("Failed to release file lock: {}", e);
                    }

                    deleted_count += 1;

                    if deleted_count >= self.config.max_deletes_per_run {
                        return Ok(deleted_count);
                    }
                }
            }

            // Only advance offset by entries that were NOT deleted,
            // since deleted entries shift remaining rows down
            let batch_deleted = deleted_count - deleted_before;
            offset += batch_len - batch_deleted;
        }

        Ok(deleted_count)
    }

    /// Clean refcounts that have no corresponding ref_files
    /// Uses reverse lookup (database query per hash) instead of loading all hashes into memory
    async fn clean_unreferenced_refcounts(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut offset = 0;

        // Process refcounts in batches, checking each hash against ref_files table
        loop {
            let refcounts = self
                .kvstorage
                .lock()
                .await
                .list_refcounts_batch(&self.bucket_name, self.config.batch_size, offset)
                .await?;

            if refcounts.is_empty() {
                break;
            }

            let batch_len = refcounts.len();
            let deleted_before = deleted_count;

            for (hash, count) in refcounts {
                // Check if hash is referenced by any ref_file (database lookup)
                let is_referenced = self
                    .kvstorage
                    .lock()
                    .await
                    .hash_is_referenced(&self.bucket_name, &hash)
                    .await?;

                if !is_referenced {
                    debug!(
                        "Found unreferenced refcount: hash={}, count={} (no ref_files point to it)",
                        hash, count
                    );

                    // Delete the refcount entry
                    if let Err(e) = self
                        .kvstorage
                        .lock()
                        .await
                        .delete_refcount(&self.bucket_name, &hash)
                        .await
                    {
                        error!("Failed to delete refcount {}: {}", hash, e);
                        continue;
                    }

                    deleted_count += 1;

                    if deleted_count >= self.config.max_deletes_per_run {
                        return Ok(deleted_count);
                    }
                }
            }

            let batch_deleted = deleted_count - deleted_before;
            offset += batch_len - batch_deleted;
        }

        Ok(deleted_count)
    }

    /// Clean S3 objects that have no refcount or refcount = 0
    /// Returns (deleted_count, bytes_freed)
    async fn clean_unused_s3_objects(&self) -> Result<(usize, u64)> {
        let mut deleted_count = 0;
        let mut bytes_freed: u64 = 0;
        let mut continuation_token: Option<String> = None;

        loop {
            let (keys, next_token) = self
                .s3storage
                .lock()
                .await
                .list_objects(continuation_token.clone())
                .await?;

            if keys.is_empty() {
                break;
            }

            for key in keys {
                // Acquire hash lock before checking refcount and deleting
                // This prevents race with PUT operations that might be incrementing refcount
                let hash_lock_key = locks::hash_lock(&self.bucket_name, &key);
                let hash_lock = self.locks.prepare_lock(hash_lock_key).await;
                let hash_guard = match hash_lock.acquire_exclusive().await {
                    Ok(g) => g,
                    Err(e) => {
                        error!("Failed to acquire hash lock for {}: {}", key, e);
                        continue;
                    }
                };

                let refcount = match self
                    .kvstorage
                    .lock()
                    .await
                    .get_ref_count(&self.bucket_name, &key)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = hash_guard.release().await;
                        return Err(e);
                    }
                };

                if refcount == 0 {
                    debug!("Found unused S3 object: key={} (refcount=0)", key);

                    // Get compressed size before deleting (for metrics)
                    let compressed_size = self
                        .kvstorage
                        .lock()
                        .await
                        .get_compressed_size(&self.bucket_name, &key)
                        .await
                        .unwrap_or(0);

                    // Delete the S3 object
                    if let Err(e) = self.s3storage.lock().await.delete_object(&key).await {
                        error!(
                            "Failed to delete S3 object (bucket={}, key={}): {}",
                            self.bucket_name, key, e
                        );
                        if let Err(e) = hash_guard.release().await {
                            warn!("Failed to release hash lock: {}", e);
                        }
                        continue;
                    }

                    deleted_count += 1;
                    bytes_freed += compressed_size as u64;

                    if deleted_count >= self.config.max_deletes_per_run {
                        if let Err(e) = hash_guard.release().await {
                            warn!("Failed to release hash lock: {}", e);
                        }
                        return Ok((deleted_count, bytes_freed));
                    }
                }

                if let Err(e) = hash_guard.release().await {
                    warn!("Failed to release hash lock: {}", e);
                }
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok((deleted_count, bytes_freed))
    }

    /// Clean logical_size entries that have no corresponding refcount
    async fn clean_orphaned_logical_sizes(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut offset = 0;

        loop {
            let hashes = self
                .kvstorage
                .lock()
                .await
                .list_logical_sizes_batch(&self.bucket_name, self.config.batch_size, offset)
                .await?;

            if hashes.is_empty() {
                break;
            }

            let batch_len = hashes.len();
            let deleted_before = deleted_count;

            for hash in hashes {
                let refcount = self
                    .kvstorage
                    .lock()
                    .await
                    .get_ref_count(&self.bucket_name, &hash)
                    .await?;

                if refcount == 0 {
                    debug!("Found orphaned logical_size: hash={} (refcount=0)", hash);

                    // Delete the logical_size entry
                    if let Err(e) = self
                        .kvstorage
                        .lock()
                        .await
                        .delete_logical_size(&self.bucket_name, &hash)
                        .await
                    {
                        error!("Failed to delete logical_size {}: {}", hash, e);
                        continue;
                    }

                    deleted_count += 1;

                    if deleted_count >= self.config.max_deletes_per_run {
                        return Ok(deleted_count);
                    }
                }
            }

            let batch_deleted = deleted_count - deleted_before;
            offset += batch_len - batch_deleted;
        }

        Ok(deleted_count)
    }
}
