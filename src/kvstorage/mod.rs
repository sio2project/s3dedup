use std::error::Error;
use serde::Deserialize;
use crate::config::Config;

pub mod postgres;
pub mod sqlite;
mod pooled;

#[derive(Debug, Deserialize, Clone)]
pub enum KVStorageType {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "sqlite")]
    SQLite,
}

pub(crate) trait KVStorageTrait {
    async fn new(config: &Config) -> Result<Box<Self>, Box<dyn Error>>
    where
        Self: Sized;

    async fn setup(&mut self) -> Result<(), Box<dyn Error>>;
    async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>>;
    async fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<(), Box<dyn Error>>;
    async fn increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        let cnt = self.get_ref_count(bucket, hash).await?;
        self.set_ref_count(bucket, hash, cnt + 1).await
    }

    async fn decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        let cnt = self.get_ref_count(bucket, hash).await?;
        self.set_ref_count(bucket, hash, cnt - 1).await
    }

    async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>>;
    async fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<(), Box<dyn Error>>;
    async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>>;

    async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>>;
    async fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<(), Box<dyn Error>>;
    async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>>;
}

#[derive(Clone)]
pub enum KVStorage {
    Postgres(postgres::Postgres),
    SQLite(sqlite::SQLite),
}

impl KVStorage {
    pub async fn new(config: &Config) -> Result<Box<Self>, Box<dyn Error>> {
        match config.kvstorage_type {
            KVStorageType::Postgres => {
                let storage = postgres::Postgres::new(config).await?;
                Ok(Box::new(KVStorage::Postgres(*storage)))
            }
            KVStorageType::SQLite => {
                let storage = sqlite::SQLite::new(config).await?;
                Ok(Box::new(KVStorage::SQLite(*storage)))
            }
        }
    }

    pub async fn setup(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.setup().await,
            KVStorage::SQLite(storage) => storage.setup().await,
        }
    }

    pub async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.get_ref_count(bucket, hash).await,
        }
    }

    pub async fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.set_ref_count(bucket, hash, ref_cnt).await,
            KVStorage::SQLite(storage) => storage.set_ref_count(bucket, hash, ref_cnt).await,
        }
    }

    pub async fn increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.increment_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.increment_ref_count(bucket, hash).await,
        }
    }

    pub async fn decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.decrement_ref_count(bucket, hash).await,
            KVStorage::SQLite(storage) => storage.decrement_ref_count(bucket, hash).await,
        }
    }

    pub async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.get_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_modified(bucket, path).await,
        }
    }

    pub async fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.set_modified(bucket, path, modified).await,
            KVStorage::SQLite(storage) => storage.set_modified(bucket, path, modified).await,
        }
    }

    pub async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.delete_modified(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_modified(bucket, path).await,
        }
    }

    pub async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.get_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.get_ref_file(bucket, path).await,
        }
    }

    pub async fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.set_ref_file(bucket, path, hash).await,
            KVStorage::SQLite(storage) => storage.set_ref_file(bucket, path, hash).await,
        }
    }

    pub async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
        match self {
            KVStorage::Postgres(storage) => storage.delete_ref_file(bucket, path).await,
            KVStorage::SQLite(storage) => storage.delete_ref_file(bucket, path).await,
        }
    }
}
