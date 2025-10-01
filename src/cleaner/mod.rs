use crate::kvstorage::KVStorage;
use crate::s3storage::S3Storage;
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;
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
    config: CleanerConfig,
}

impl Cleaner {
    pub fn new(
        bucket_name: String,
        kvstorage: Arc<Mutex<Box<KVStorage>>>,
        s3storage: Arc<Mutex<Box<S3Storage>>>,
        config: CleanerConfig,
    ) -> Self {
        Self {
            bucket_name,
            kvstorage,
            s3storage,
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
                    error!(
                        "Cleanup cycle failed for bucket {}: {}",
                        self.bucket_name, e
                    );
                }
            }
        });
    }

    /// Run a full cleanup cycle
    pub async fn run_cleanup(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut total_deletes = 0;

        // Case 1: Clean ref_files pointing to non-existent hashes
        info!("Phase 1: Cleaning ref_files with missing hashes");
        total_deletes += self.clean_orphaned_ref_files().await?;

        if total_deletes >= self.config.max_deletes_per_run {
            warn!(
                "Reached max deletes limit ({}) in phase 1, stopping cleanup cycle",
                self.config.max_deletes_per_run
            );
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
            return Ok(());
        }

        // Case 3: Clean S3 objects with no refcount or refcount = 0
        info!("Phase 3: Cleaning S3 objects with no refcount or refcount = 0");
        total_deletes += self.clean_unused_s3_objects().await?;

        if total_deletes >= self.config.max_deletes_per_run {
            warn!(
                "Reached max deletes limit ({}) in phase 3, stopping cleanup cycle",
                self.config.max_deletes_per_run
            );
            return Ok(());
        }

        // Case 4: Clean logical_size entries with no refcount
        info!("Phase 4: Cleaning logical_size entries with no refcount");
        total_deletes += self.clean_orphaned_logical_sizes().await?;

        info!(
            "Cleanup cycle complete for bucket: {} (total items deleted: {})",
            self.bucket_name, total_deletes
        );

        Ok(())
    }

    /// Clean ref_files that point to non-existent hashes in refcount table
    async fn clean_orphaned_ref_files(
        &self,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
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

                    // Delete ref_file and modified entries
                    if let Err(e) = self
                        .kvstorage
                        .lock()
                        .await
                        .delete_ref_file(&self.bucket_name, path)
                        .await
                    {
                        error!("Failed to delete ref_file {}: {}", path, e);
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

                    deleted_count += 1;

                    if deleted_count >= self.config.max_deletes_per_run {
                        return Ok(deleted_count);
                    }
                }
            }

            offset += self.config.batch_size;
        }

        Ok(deleted_count)
    }

    /// Clean refcounts that have no corresponding ref_files
    async fn clean_unreferenced_refcounts(
        &self,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut deleted_count = 0;
        let mut offset = 0;

        // Build a set of all hashes referenced by ref_files
        let mut referenced_hashes = HashSet::new();
        let mut ref_offset = 0;

        loop {
            let ref_files = self
                .kvstorage
                .lock()
                .await
                .list_ref_files_batch(&self.bucket_name, self.config.batch_size, ref_offset)
                .await?;

            if ref_files.is_empty() {
                break;
            }

            for (_path, hash) in ref_files {
                referenced_hashes.insert(hash);
            }

            ref_offset += self.config.batch_size;
        }

        debug!("Found {} referenced hashes", referenced_hashes.len());

        // Now check refcounts against this set
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

            for (hash, count) in refcounts {
                if !referenced_hashes.contains(&hash) {
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

            offset += self.config.batch_size;
        }

        Ok(deleted_count)
    }

    /// Clean S3 objects that have no refcount or refcount = 0
    async fn clean_unused_s3_objects(
        &self,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut deleted_count = 0;
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
                let refcount = self
                    .kvstorage
                    .lock()
                    .await
                    .get_ref_count(&self.bucket_name, &key)
                    .await?;

                if refcount == 0 {
                    debug!("Found unused S3 object: key={} (refcount=0)", key);

                    // Delete the S3 object
                    if let Err(e) = self.s3storage.lock().await.delete_object(&key).await {
                        error!("Failed to delete S3 object {}: {}", key, e);
                        continue;
                    }

                    deleted_count += 1;

                    if deleted_count >= self.config.max_deletes_per_run {
                        return Ok(deleted_count);
                    }
                }
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(deleted_count)
    }

    /// Clean logical_size entries that have no corresponding refcount
    async fn clean_orphaned_logical_sizes(
        &self,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
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

            offset += self.config.batch_size;
        }

        Ok(deleted_count)
    }
}
