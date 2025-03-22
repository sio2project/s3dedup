use crate::config::BucketConfig;
use serde::Deserialize;
use std::error::Error;
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
    async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn Error>>
    where
        Self: Sized;

    async fn setup(&mut self) -> Result<(), Box<dyn Error>>;
    async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>>;
    async fn set_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
        ref_cnt: i32,
    ) -> Result<(), Box<dyn Error>>;
    async fn increment_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>> {
        let cnt = self.get_ref_count(bucket, hash).await?;
        self.set_ref_count(bucket, hash, cnt + 1).await
    }

    async fn decrement_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>> {
        let cnt = self.get_ref_count(bucket, hash).await?;
        if cnt == 0 {
            return Ok(());
        }
        self.set_ref_count(bucket, hash, cnt - 1).await
    }

    async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>>;
    async fn set_modified(
        &mut self,
        bucket: &str,
        path: &str,
        modified: i64,
    ) -> Result<(), Box<dyn Error>>;
    async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>>;

    async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>>;
    async fn set_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>>;
    async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>>;
}

#[derive(Clone)]
pub enum KVStorage {
    Postgres(postgres::Postgres),
    SQLite(sqlite::SQLite),
}

impl KVStorage {
    pub async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn Error>> {
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
    pub async fn setup(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.setup().await,
            KVStorage::SQLite(storage) => storage.setup().await,
        }
    }

    /**
     * Get the reference count for a hash.
     * If the hash does not exist, return 0.
     */
    pub async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>> {
        debug!("Getting ref count for bucket: {}, hash: {}", bucket, hash);
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_ref_count(bucket, hash).await,
        }
    }

    /**
     * Set the reference count for a hash.
     */
    pub async fn set_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
        ref_cnt: i32,
    ) -> Result<(), Box<dyn Error>> {
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
     * Increment the reference count for a hash.
     */
    pub async fn increment_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>> {
        debug!("Incrementing ref count for bucket: {}, hash: {}", bucket, hash);
        match self {
            KVStorage::Postgres(storage) => storage.increment_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.increment_ref_count(bucket, hash).await,
        }
    }

    /**
     * Decrement the reference count for a hash.
     * If the reference count is already 0, do nothing.
     */
    pub async fn decrement_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>> {
        debug!("Decrementing ref count for bucket: {}, hash: {}", bucket, hash);
        match self {
            KVStorage::Postgres(storage) => storage.decrement_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.decrement_ref_count(bucket, hash).await,
        }
    }

    /**
     * Get the modified time for a path.
     * If the path does not exist, return 0.
     */
    pub async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>> {
        debug!("Getting modified time for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.get_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_modified(bucket, path).await,
        }
    }

    /**
     * Set the modified time for a path.
     */
    pub async fn set_modified(
        &mut self,
        bucket: &str,
        path: &str,
        modified: i64,
    ) -> Result<(), Box<dyn Error>> {
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
    pub async fn delete_modified(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<(), Box<dyn Error>> {
        debug!("Deleting modified time for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.delete_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_modified(bucket, path).await,
        }
    }

    /**
     * Get the reference file for a path.
     * If the path does not exist, return an empty string.
     */
    pub async fn get_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<String, Box<dyn Error>> {
        debug!("Getting ref file for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_ref_file(bucket, path).await,
        }
    }

    /**
     * Set the reference file for a path.
     */
    pub async fn set_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>> {
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
    pub async fn delete_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<(), Box<dyn Error>> {
        debug!("Deleting ref file for bucket: {}, path: {}", bucket, path);
        match self {
            KVStorage::Postgres(storage) => storage.delete_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_ref_file(bucket, path).await,
        }
    }
}
