use crate::config::Config;
use anyhow::Result;
use serde::Deserialize;
use tracing::{debug, info};

mod pooled;
pub mod postgres;
pub mod sqlite;

#[cfg(feature = "test-mocks")]
pub mod mock;

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

    async fn setup(&self) -> Result<()>;
    async fn get_ref_count(&self, bucket: &str, hash: &str) -> Result<i32>;

    /// Atomically increment the reference count (database-level atomic operation)
    /// Returns the new reference count after incrementing.
    async fn atomic_increment_ref_count(&self, bucket: &str, hash: &str) -> Result<i32>;

    /// Atomically decrement the reference count (database-level atomic operation)
    /// If the reference count is already 0, do nothing and return 0.
    /// Returns the new reference count after decrementing.
    async fn atomic_decrement_ref_count(&self, bucket: &str, hash: &str) -> Result<i32>;

    async fn get_modified(&self, bucket: &str, path: &str) -> Result<i64>;
    async fn set_modified(&self, bucket: &str, path: &str, modified: i64) -> Result<()>;
    async fn delete_modified(&self, bucket: &str, path: &str) -> Result<()>;

    async fn get_ref_file(&self, bucket: &str, path: &str) -> Result<String>;
    async fn set_ref_file(&self, bucket: &str, path: &str, hash: &str) -> Result<()>;
    async fn delete_ref_file(&self, bucket: &str, path: &str) -> Result<()>;

    async fn get_logical_size(&self, bucket: &str, hash: &str) -> Result<usize>;
    async fn set_logical_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()>;

    async fn get_compressed_size(&self, bucket: &str, hash: &str) -> Result<usize>;
    async fn set_compressed_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()>;

    /// List all files under a given path prefix that were modified at or before the given timestamp
    async fn list_files(
        &self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>>;

    // Cursor-based orphan-detection methods for cleaner (server-side JOIN filtering)

    /// List orphaned ref_files: entries whose hash has refcount=0 or no refcount row.
    /// Cursor-based: returns entries with path > after_cursor, ordered by path.
    /// Pass empty string for initial call.
    async fn list_orphaned_ref_files(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>>;

    /// List orphaned refcounts: entries with no ref_file pointing to them.
    /// Cursor-based: returns entries with hash > after_cursor, ordered by hash.
    async fn list_orphaned_refcounts(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, i32)>>;

    /// List orphaned logical_size entries: those with refcount=0 or no refcount row.
    /// Cursor-based: returns entries with hash > after_cursor, ordered by hash.
    async fn list_orphaned_logical_sizes(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<String>>;

    /// Delete a refcount entry
    async fn delete_refcount(&self, bucket: &str, hash: &str) -> Result<()>;

    /// Delete a logical_size entry
    async fn delete_logical_size(&self, bucket: &str, hash: &str) -> Result<()>;

    /// Check if a hash is referenced by any ref_file entry
    /// Used by cleaner to check if a refcount entry is orphaned
    async fn hash_is_referenced(&self, bucket: &str, hash: &str) -> Result<bool>;

    // Aggregate statistics methods for metrics
    /// Get total number of files (count of file_modified entries)
    async fn get_total_files(&self, bucket: &str) -> Result<i64>;

    /// Get total number of blobs (count of refcount entries where refcount > 0)
    async fn get_total_blobs(&self, bucket: &str) -> Result<i64>;

    /// Get total storage bytes (sum of compressed_size for all blobs - actual S3 storage)
    async fn get_total_storage_bytes(&self, bucket: &str) -> Result<i64>;

    /// Get total logical bytes (sum of logical_size * refcount for all blobs - what storage would be without dedup)
    async fn get_total_logical_bytes(&self, bucket: &str) -> Result<i64>;

    // Version tracking
    /// Get the stored instance version
    async fn get_version(&self) -> Result<Option<String>>;

    /// Store the instance version
    async fn set_version(&self, version: &str) -> Result<()>;

    /// Get deduplicated bytes saved (sum of (refcount - 1) * logical_size for all blobs)
    async fn get_deduplicated_bytes_saved(&self, bucket: &str) -> Result<i64>;

    /// Get total compressed bytes without deduplication (sum of refcount * compressed_size)
    async fn get_total_compressed_bytes_no_dedup(&self, bucket: &str) -> Result<i64>;

    /// Get all storage statistics in a single query (avoids repeated expensive JOINs).
    async fn get_storage_stats(&self, bucket: &str) -> Result<StorageStats>;

    /// Get connection pool statistics (active connections, idle connections)
    /// Returns (active, idle)
    fn get_pool_stats(&self) -> (u32, u32) {
        // Default implementation for non-database backends
        (0, 0)
    }
}

/// All aggregate storage metrics returned by a single combined query.
#[derive(Debug, Default)]
pub struct StorageStats {
    pub total_files: i64,
    pub total_blobs: i64,
    pub total_storage_bytes: i64,
    pub total_logical_bytes: i64,
    pub deduplicated_bytes_saved: i64,
    pub total_compressed_bytes_no_dedup: i64,
}

#[derive(Clone)]
pub enum KVStorage {
    Postgres(postgres::Postgres),
    SQLite(sqlite::SQLite),
    #[cfg(feature = "test-mocks")]
    Mock(mock::MockKVStorage),
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
    pub async fn setup(&self) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.setup().await,
            KVStorage::SQLite(storage) => storage.setup().await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.setup().await,
        }
    }

    /**
     * Get the reference count for a hash.
     * If the hash does not exist, return 0.
     */
    pub async fn get_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        debug!("Getting ref count for bucket: {}, hash: {}", bucket, hash);
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_ref_count(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_ref_count(bucket, hash).await,
        }
    }

    /**
     * Atomically increment the reference count (database-level atomic operation).
     * Returns the new reference count after incrementing.
     */
    pub async fn atomic_increment_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        debug!(
            "Atomically incrementing ref count for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.atomic_increment_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.atomic_increment_ref_count(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.atomic_increment_ref_count(bucket, hash).await,
        }
    }

    /**
     * Atomically decrement the reference count (database-level atomic operation).
     * If the reference count is already 0, do nothing and return 0.
     * Returns the new reference count after decrementing.
     */
    pub async fn atomic_decrement_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        debug!(
            "Atomically decrementing ref count for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.atomic_decrement_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.atomic_decrement_ref_count(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.atomic_decrement_ref_count(bucket, hash).await,
        }
    }

    /**
     * Get the modified time for a path.
     * If the path does not exist, return 0.
     */
    pub async fn get_modified(&self, bucket: &str, path: &str) -> Result<i64> {
        debug!(
            "Getting modified time for bucket: {}, path: {}",
            bucket, path
        );
        match self {
            KVStorage::Postgres(storage) => storage.get_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_modified(bucket, path).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_modified(bucket, path).await,
        }
    }

    /**
     * Set the modified time for a path.
     */
    pub async fn set_modified(&self, bucket: &str, path: &str, modified: i64) -> Result<()> {
        debug!(
            "Setting modified time for bucket: {}, path: {} to {}",
            bucket, path, modified
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_modified(bucket, path, modified).await,
            KVStorage::SQLite(storage) => storage.set_modified(bucket, path, modified).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.set_modified(bucket, path, modified).await,
        }
    }

    /**
     * Delete the modified time for a path.
     */
    pub async fn delete_modified(&self, bucket: &str, path: &str) -> Result<()> {
        debug!(
            "Deleting modified time for bucket: {}, path: {}",
            bucket, path
        );
        match self {
            KVStorage::Postgres(storage) => storage.delete_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_modified(bucket, path).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.delete_modified(bucket, path).await,
        }
    }

    /**
     * Get the reference file for a path.
     * If the path does not exist, return an empty string.
     */
    pub async fn get_ref_file(&self, bucket: &str, path: &str) -> Result<String> {
        debug!("Getting ref file for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_ref_file(bucket, path).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_ref_file(bucket, path).await,
        }
    }

    /**
     * Set the reference file for a path.
     */
    pub async fn set_ref_file(&self, bucket: &str, path: &str, hash: &str) -> Result<()> {
        debug!(
            "Setting ref file for bucket: {}, path: {} to {}",
            bucket, path, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_ref_file(bucket, path, hash).await,
            KVStorage::SQLite(storage) => storage.set_ref_file(bucket, path, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.set_ref_file(bucket, path, hash).await,
        }
    }

    /**
     * Delete the reference file for a path.
     */
    pub async fn delete_ref_file(&self, bucket: &str, path: &str) -> Result<()> {
        debug!("Deleting ref file for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.delete_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_ref_file(bucket, path).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.delete_ref_file(bucket, path).await,
        }
    }

    /**
     * Get the logical size for a hash.
     * If the hash does not exist, return 0.
     */
    pub async fn get_logical_size(&self, bucket: &str, hash: &str) -> Result<usize> {
        debug!(
            "Getting logical size for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.get_logical_size(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_logical_size(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_logical_size(bucket, hash).await,
        }
    }

    /**
     * Set the logical size for a hash.
     */
    pub async fn set_logical_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        debug!(
            "Setting logical size for bucket: {}, hash: {} to {}",
            bucket, hash, size
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_logical_size(bucket, hash, size).await,
            KVStorage::SQLite(storage) => storage.set_logical_size(bucket, hash, size).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.set_logical_size(bucket, hash, size).await,
        }
    }

    pub async fn get_compressed_size(&self, bucket: &str, hash: &str) -> Result<usize> {
        debug!(
            "Getting compressed size for bucket: {}, hash: {}",
            bucket, hash
        );
        match self {
            KVStorage::Postgres(storage) => storage.get_compressed_size(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_compressed_size(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_compressed_size(bucket, hash).await,
        }
    }

    pub async fn set_compressed_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        debug!(
            "Setting compressed size for bucket: {}, hash: {} to {}",
            bucket, hash, size
        );
        match self {
            KVStorage::Postgres(storage) => storage.set_compressed_size(bucket, hash, size).await,
            KVStorage::SQLite(storage) => storage.set_compressed_size(bucket, hash, size).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.set_compressed_size(bucket, hash, size).await,
        }
    }

    /**
     * List all files under a given path prefix that were modified at or before the given timestamp.
     */
    pub async fn list_files(
        &self,
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
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.list_files(bucket, path_prefix, timestamp).await,
        }
    }

    pub async fn list_orphaned_ref_files(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        match self {
            KVStorage::Postgres(storage) => {
                storage
                    .list_orphaned_ref_files(bucket, after_cursor, limit)
                    .await
            }
            KVStorage::SQLite(storage) => {
                storage
                    .list_orphaned_ref_files(bucket, after_cursor, limit)
                    .await
            }
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => {
                storage
                    .list_orphaned_ref_files(bucket, after_cursor, limit)
                    .await
            }
        }
    }

    pub async fn list_orphaned_refcounts(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, i32)>> {
        match self {
            KVStorage::Postgres(storage) => {
                storage
                    .list_orphaned_refcounts(bucket, after_cursor, limit)
                    .await
            }
            KVStorage::SQLite(storage) => {
                storage
                    .list_orphaned_refcounts(bucket, after_cursor, limit)
                    .await
            }
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => {
                storage
                    .list_orphaned_refcounts(bucket, after_cursor, limit)
                    .await
            }
        }
    }

    pub async fn list_orphaned_logical_sizes(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        match self {
            KVStorage::Postgres(storage) => {
                storage
                    .list_orphaned_logical_sizes(bucket, after_cursor, limit)
                    .await
            }
            KVStorage::SQLite(storage) => {
                storage
                    .list_orphaned_logical_sizes(bucket, after_cursor, limit)
                    .await
            }
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => {
                storage
                    .list_orphaned_logical_sizes(bucket, after_cursor, limit)
                    .await
            }
        }
    }

    pub async fn delete_refcount(&self, bucket: &str, hash: &str) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.delete_refcount(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.delete_refcount(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.delete_refcount(bucket, hash).await,
        }
    }

    pub async fn delete_logical_size(&self, bucket: &str, hash: &str) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.delete_logical_size(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.delete_logical_size(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.delete_logical_size(bucket, hash).await,
        }
    }

    /// Check if a hash is referenced by any ref_file entry
    pub async fn hash_is_referenced(&self, bucket: &str, hash: &str) -> Result<bool> {
        match self {
            KVStorage::Postgres(storage) => storage.hash_is_referenced(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.hash_is_referenced(bucket, hash).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.hash_is_referenced(bucket, hash).await,
        }
    }

    pub async fn get_total_files(&self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_files(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_files(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_total_files(bucket).await,
        }
    }

    pub async fn get_total_blobs(&self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_blobs(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_blobs(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_total_blobs(bucket).await,
        }
    }

    pub async fn get_total_storage_bytes(&self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_storage_bytes(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_storage_bytes(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_total_storage_bytes(bucket).await,
        }
    }

    pub async fn get_total_logical_bytes(&self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_total_logical_bytes(bucket).await,
            KVStorage::SQLite(storage) => storage.get_total_logical_bytes(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_total_logical_bytes(bucket).await,
        }
    }

    pub async fn get_deduplicated_bytes_saved(&self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => storage.get_deduplicated_bytes_saved(bucket).await,
            KVStorage::SQLite(storage) => storage.get_deduplicated_bytes_saved(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_deduplicated_bytes_saved(bucket).await,
        }
    }

    pub async fn get_total_compressed_bytes_no_dedup(&self, bucket: &str) -> Result<i64> {
        match self {
            KVStorage::Postgres(storage) => {
                storage.get_total_compressed_bytes_no_dedup(bucket).await
            }
            KVStorage::SQLite(storage) => storage.get_total_compressed_bytes_no_dedup(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_total_compressed_bytes_no_dedup(bucket).await,
        }
    }

    pub fn get_pool_stats(&self) -> (u32, u32) {
        match self {
            KVStorage::Postgres(storage) => storage.get_pool_stats(),
            KVStorage::SQLite(storage) => storage.get_pool_stats(),
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_pool_stats(),
        }
    }

    pub async fn get_version(&self) -> Result<Option<String>> {
        match self {
            KVStorage::Postgres(storage) => storage.get_version().await,
            KVStorage::SQLite(storage) => storage.get_version().await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_version().await,
        }
    }

    pub async fn set_version(&self, version: &str) -> Result<()> {
        match self {
            KVStorage::Postgres(storage) => storage.set_version(version).await,
            KVStorage::SQLite(storage) => storage.set_version(version).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.set_version(version).await,
        }
    }

    pub async fn get_storage_stats(&self, bucket: &str) -> Result<StorageStats> {
        match self {
            KVStorage::Postgres(storage) => storage.get_storage_stats(bucket).await,
            KVStorage::SQLite(storage) => storage.get_storage_stats(bucket).await,
            #[cfg(feature = "test-mocks")]
            KVStorage::Mock(storage) => storage.get_storage_stats(bucket).await,
        }
    }
}
