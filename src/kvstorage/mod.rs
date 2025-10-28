use crate::config::Config;
use anyhow::Result;
use serde::Deserialize;
use tracing::{debug, info};

mod pooled;
pub mod postgres;
pub mod sqlite;

#[derive(Debug, Deserialize, Clone)]
pub enum KVStorageType {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "sqlite")]
    SQLite,
}

pub(crate) trait KVStorageTrait {
    async fn new(config: &Config) -> Result<Box<Self>>
    where
        Self: Sized;

    async fn setup(&mut self) -> Result<()>;
    async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32>;
    async fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<()>;
    /// Atomically increment the reference count (database-level atomic operation)
    /// Prefer this over get_ref_count + set_ref_count to avoid race conditions
    async fn atomic_increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        // Default implementation: non-atomic. Database implementations should override.
        let cnt = self.get_ref_count(bucket, hash).await?;
        self.set_ref_count(bucket, hash, cnt + 1).await?;
        Ok(cnt + 1)
    }

    /// Atomically decrement the reference count (database-level atomic operation)
    /// Prefer this over get_ref_count + set_ref_count to avoid race conditions
    /// If the reference count is already 0, do nothing and return 0.
    /// Returns the new reference count after decrementing.
    async fn atomic_decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        // Default implementation: non-atomic. Database implementations should override.
        let cnt = self.get_ref_count(bucket, hash).await?;
        if cnt == 0 {
            return Ok(0);
        }
        let new_count = cnt - 1;
        self.set_ref_count(bucket, hash, new_count).await?;
        Ok(new_count)
    }

    async fn increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<()> {
        let cnt = self.get_ref_count(bucket, hash).await?;
        self.set_ref_count(bucket, hash, cnt + 1).await
    }

    async fn decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i64> {
        let cnt = self.get_ref_count(bucket, hash).await?;
        if cnt == 0 {
            return Ok(0);
        }
        let new_count = cnt - 1;
        self.set_ref_count(bucket, hash, new_count).await?;
        Ok(new_count as i64)
    }

    async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64>;
    async fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<()>;
    async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<()>;

    async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String>;
    async fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<()>;
    async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<()>;

    async fn get_logical_size(&mut self, bucket: &str, hash: &str) -> Result<usize>;
    async fn set_logical_size(&mut self, bucket: &str, hash: &str, size: usize) -> Result<()>;

    async fn get_compressed_size(&mut self, bucket: &str, hash: &str) -> Result<usize>;
    async fn set_compressed_size(&mut self, bucket: &str, hash: &str, size: usize) -> Result<()>;

    /// List all files under a given path prefix that were modified at or before the given timestamp
    async fn list_files(
        &mut self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>>;

    // Batched methods for cleaner
    /// List ref_file entries in batches (path, hash)
    async fn list_ref_files_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>>;

    /// List refcount entries in batches (hash, count)
    async fn list_refcounts_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, i32)>>;

    /// List logical_size entries in batches (hash)
    async fn list_logical_sizes_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>>;

    /// Delete a refcount entry
    async fn delete_refcount(&mut self, bucket: &str, hash: &str) -> Result<()>;

    /// Delete a logical_size entry
    async fn delete_logical_size(&mut self, bucket: &str, hash: &str) -> Result<()>;

    // Aggregate statistics methods for metrics
    /// Get total number of files (count of file_modified entries)
    async fn get_total_files(&mut self, bucket: &str) -> Result<i64>;

    /// Get total number of blobs (count of refcount entries where refcount > 0)
    async fn get_total_blobs(&mut self, bucket: &str) -> Result<i64>;

    /// Get total storage bytes (sum of compressed_size for all blobs - actual S3 storage)
    async fn get_total_storage_bytes(&mut self, bucket: &str) -> Result<i64>;

    /// Get total logical bytes (sum of logical_size * refcount for all blobs - what storage would be without dedup)
    async fn get_total_logical_bytes(&mut self, bucket: &str) -> Result<i64>;

    // Version tracking
    /// Get the stored instance version for a bucket
    async fn get_version(&mut self, bucket: &str) -> Result<Option<String>>;

    /// Store the instance version for a bucket
    async fn set_version(&mut self, bucket: &str, version: &str) -> Result<()>;

    /// Get deduplicated bytes saved (sum of (refcount - 1) * logical_size for all blobs)
    async fn get_deduplicated_bytes_saved(&mut self, bucket: &str) -> Result<i64>;

    /// Get connection pool statistics (active connections, idle connections)
    /// Returns (active, idle)
    fn get_pool_stats(&self) -> (u32, u32) {
        // Default implementation for non-database backends
        (0, 0)
    }
}

#[derive(Clone)]
pub enum KVStorage {
    Postgres(postgres::Postgres),
    SQLite(sqlite::SQLite),
}

impl KVStorage {
    pub async fn new(config: &Config) -> Result<Box<Self>> {
        match config.kvstorage_type {
            KVStorageType::Postgres => {
                info!("Using Postgres as KV storage");
                let storage = postgres::Postgres::new(config).await?;
                Ok(Box::new(KVStorage::Postgres(*storage)))
            }
            KVStorageType::SQLite => {
                info!("Using SQLite as KV storage");
                let storage = sqlite::SQLite::new(config).await?;
                Ok(Box::new(KVStorage::SQLite(*storage)))
            }
        }
    }

    /**
     * Setup the KV storage.
     */
    pub async fn setup(&mut self) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.setup().await,
            KVStorage::SQLite(storage) => storage.setup().await,
        }
    }

    /**
     * Get the reference count for a hash.
     * If the hash does not exist, return 0.
     */
    pub async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        debug!("Getting ref count for bucket: {}, hash: {}", bucket, hash);
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_ref_count(bucket, hash).await,
        }
    }

    /**
     * Set the reference count for a hash.
     */
    pub async fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<()> {
        debug!(
            "Setting ref count for bucket: {}, hash: {} to {}",
            bucket, hash, ref_cnt
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_ref_count(bucket, hash, ref_cnt).await,
            KVStorage::SQLite(storage) => storage.set_ref_count(bucket, hash, ref_cnt).await,
        }
    }

    /**
     * Atomically increment the reference count (database-level atomic operation).
     * Returns the new reference count after incrementing.
     * Prefer this over increment_ref_count to avoid race conditions.
     */
    pub async fn atomic_increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        debug!(
            "Atomically incrementing ref count for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.atomic_increment_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.atomic_increment_ref_count(bucket, hash).await,
        }
    }

    /**
     * Atomically decrement the reference count (database-level atomic operation).
     * If the reference count is already 0, do nothing and return 0.
     * Returns the new reference count after decrementing.
     * Prefer this over decrement_ref_count to avoid race conditions.
     */
    pub async fn atomic_decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        debug!(
            "Atomically decrementing ref count for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.atomic_decrement_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.atomic_decrement_ref_count(bucket, hash).await,
        }
    }

    /**
     * Increment the reference count for a hash.
     */
    pub async fn increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<()> {
        debug!(
            "Incrementing ref count for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.increment_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.increment_ref_count(bucket, hash).await,
        }
    }

    /**
     * Decrement the reference count for a hash.
     * If the reference count is already 0, do nothing.
     * Returns the new reference count after decrementing.
     */
    pub async fn decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i64> {
        debug!(
            "Decrementing ref count for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.decrement_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.decrement_ref_count(bucket, hash).await,
        }
    }

    /**
     * Get the modified time for a path.
     * If the path does not exist, return 0.
     */
    pub async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64> {
        debug!(
            "Getting modified time for bucket: {}, path: {}",
            bucket, path
        );
        match self {
            KVStorage::Postgres(storage) => storage.get_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_modified(bucket, path).await,
        }
    }

    /**
     * Set the modified time for a path.
     */
    pub async fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<()> {
        debug!(
            "Setting modified time for bucket: {}, path: {} to {}",
            bucket, path, modified
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_modified(bucket, path, modified).await,
            KVStorage::SQLite(storage) => storage.set_modified(bucket, path, modified).await,
        }
    }

    /**
     * Delete the modified time for a path.
     */
    pub async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<()> {
        debug!(
            "Deleting modified time for bucket: {}, path: {}",
            bucket, path
        );
        match self {
            KVStorage::Postgres(storage) => storage.delete_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_modified(bucket, path).await,
        }
    }

    /**
     * Get the reference file for a path.
     * If the path does not exist, return an empty string.
     */
    pub async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String> {
        debug!("Getting ref file for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_ref_file(bucket, path).await,
        }
    }

    /**
     * Set the reference file for a path.
     */
    pub async fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<()> {
        debug!(
            "Setting ref file for bucket: {}, path: {} to {}",
            bucket, path, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_ref_file(bucket, path, hash).await,
            KVStorage::SQLite(storage) => storage.set_ref_file(bucket, path, hash).await,
        }
    }

    /**
     * Delete the reference file for a path.
     */
    pub async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<()> {
        debug!("Deleting ref file for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.delete_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_ref_file(bucket, path).await,
        }
    }

    /**
     * Get the logical size for a hash.
     * If the hash does not exist, return 0.
     */
    pub async fn get_logical_size(&mut self, bucket: &str, hash: &str) -> Result<usize> {
        debug!(
            "Getting logical size for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.get_logical_size(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_logical_size(bucket, hash).await,
        }
    }

    /**
     * Set the logical size for a hash.
     */
    pub async fn set_logical_size(&mut self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        debug!(
            "Setting logical size for bucket: {}, hash: {} to {}",
            bucket, hash, size
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_logical_size(bucket, hash, size).await,
            KVStorage::SQLite(storage) => storage.set_logical_size(bucket, hash, size).await,
        }
    }

    pub async fn get_compressed_size(&mut self, bucket: &str, hash: &str) -> Result<usize> {
        debug!(
            "Getting compressed size for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.get_compressed_size(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_compressed_size(bucket, hash).await,
        }
    }

    pub async fn set_compressed_size(
        &mut self,
        bucket: &str,
        hash: &str,
        size: usize,
    ) -> Result<()> {
        debug!(
            "Setting compressed size for bucket: {}, hash: {} to {}",
            bucket, hash, size
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_compressed_size(bucket, hash, size).await,
            KVStorage::SQLite(storage) => storage.set_compressed_size(bucket, hash, size).await,
        }
    }

    /**
     * List all files under a given path prefix that were modified at or before the given timestamp.
     */
    pub async fn list_files(
        &mut self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>> {
        debug!(
            "Listing files for bucket: {}, prefix: {}, timestamp: {}",
            bucket, path_prefix, timestamp
        );
        match self {
            KVStorage::Postgres(storage) => {
                storage.list_files(bucket, path_prefix, timestamp).await
            }
            KVStorage::SQLite(storage) => storage.list_files(bucket, path_prefix, timestamp).await,
        }
    }

    pub async fn list_ref_files_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>> {
        match self {
            KVStorage::Postgres(storage) => {
                storage.list_ref_files_batch(bucket, limit, offset).await
            }
            KVStorage::SQLite(storage) => storage.list_ref_files_batch(bucket, limit, offset).await,
        }
    }

    pub async fn list_refcounts_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, i32)>> {
        match self {
            KVStorage::Postgres(storage) => {
                storage.list_refcounts_batch(bucket, limit, offset).await
            }
            KVStorage::SQLite(storage) => storage.list_refcounts_batch(bucket, limit, offset).await,
        }
    }

    pub async fn list_logical_sizes_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>> {
        match self {
            KVStorage::Postgres(storage) => {
                storage
                    .list_logical_sizes_batch(bucket, limit, offset)
                    .await
            }
            KVStorage::SQLite(storage) => {
                storage
                    .list_logical_sizes_batch(bucket, limit, offset)
                    .await
            }
        }
    }

    pub async fn delete_refcount(&mut self, bucket: &str, hash: &str) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.delete_refcount(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.delete_refcount(bucket, hash).await,
        }
    }

    pub async fn delete_logical_size(&mut self, bucket: &str, hash: &str) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.delete_logical_size(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.delete_logical_size(bucket, hash).await,
        }
    }

    pub async fn get_total_files(&mut self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_files(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_files(bucket).await,
        }
    }

    pub async fn get_total_blobs(&mut self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_blobs(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_blobs(bucket).await,
        }
    }

    pub async fn get_total_storage_bytes(&mut self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_storage_bytes(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_storage_bytes(bucket).await,
        }
    }

    pub async fn get_total_logical_bytes(&mut self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_logical_bytes(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_logical_bytes(bucket).await,
        }
    }

    pub async fn get_deduplicated_bytes_saved(&mut self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_deduplicated_bytes_saved(bucket).await,
            KVStorage::SQLite(storage) => storage.get_deduplicated_bytes_saved(bucket).await,
        }
    }

    pub fn get_pool_stats(&self) -> (u32, u32) {
        match self {
            KVStorage::Postgres(storage) => storage.get_pool_stats(),
            KVStorage::SQLite(storage) => storage.get_pool_stats(),
        }
    }

    pub async fn get_version(&mut self, bucket: &str) -> Result<Option<String>> {
        match self {
            KVStorage::Postgres(storage) => storage.get_version(bucket).await,
            KVStorage::SQLite(storage) => storage.get_version(bucket).await,
        }
    }

    pub async fn set_version(&mut self, bucket: &str, version: &str) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.set_version(bucket, version).await,
            KVStorage::SQLite(storage) => storage.set_version(bucket, version).await,
        }
    }
}
