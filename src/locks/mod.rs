use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

pub mod memory;
pub mod postgres;

/**
 * Get key for lock on file
 */
pub(crate) fn file_lock(bucket: &str, path: &str) -> String {
    format!("file:{}:{}", bucket, path)
}

/**
 * Get key for lock on hash
 */
#[allow(dead_code)]
fn hash_lock(bucket: &str, hash: &str) -> String {
    format!("hash:{}:{}", bucket, hash)
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum LocksType {
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "postgres")]
    Postgres,
}

use std::pin::Pin;
use std::future::Future;

#[must_use = "droping temporary lock makes no sense"]
pub trait SharedLockGuard<'a> {
    /// Release the lock explicitly before the guard is dropped.
    /// For PostgreSQL locks, this unlocks the advisory lock in the database.
    /// For memory locks, this drops the Tokio RwLock guard.
    fn release(self: Box<Self>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[must_use = "droping temporary lock makes no sense"]
pub trait ExclusiveLockGuard<'a> {
    /// Release the lock explicitly before the guard is dropped.
    /// For PostgreSQL locks, this unlocks the advisory lock in the database.
    /// For memory locks, this drops the Tokio RwLock guard.
    fn release(self: Box<Self>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[async_trait]
#[must_use = "preparing temporary lock makes no sense"]
pub trait Lock {
    async fn acquire_shared<'a>(
        &'a self,
    ) -> anyhow::Result<Box<dyn SharedLockGuard<'a> + Send + 'a>>;
    async fn acquire_exclusive<'a>(
        &'a self,
    ) -> anyhow::Result<Box<dyn ExclusiveLockGuard<'a> + Send + 'a>>;
}

#[async_trait]
pub(crate) trait LockStorage {
    fn new() -> Box<Self>;

    async fn prepare_lock<'a>(&'a self, key: String) -> Box<dyn Lock + 'a + Send>;
}

#[allow(private_interfaces)]
#[derive(Clone)]
pub enum LocksStorage {
    Memory(memory::MemoryLocks),
    Postgres(Box<postgres::PostgresLocks>),
}

impl LocksStorage {
    pub fn new(lock_type: LocksType) -> Box<Self> {
        match lock_type {
            LocksType::Memory => {
                info!("Using memory as locks storage");
                Box::new(LocksStorage::Memory(*memory::MemoryLocks::new()))
            }
            LocksType::Postgres => {
                panic!("PostgreSQL locks must be initialized with config via new_with_config")
            }
        }
    }

    pub async fn new_with_config(
        lock_type: LocksType,
        config: &crate::config::Config,
    ) -> anyhow::Result<Box<Self>> {
        match lock_type {
            LocksType::Memory => {
                info!("Using memory as locks storage");
                Ok(Box::new(LocksStorage::Memory(*memory::MemoryLocks::new())))
            }
            LocksType::Postgres => {
                info!("Using PostgreSQL as locks storage");
                let pg_locks = postgres::PostgresLocks::new_with_config(config).await?;
                Ok(Box::new(LocksStorage::Postgres(pg_locks)))
            }
        }
    }

    pub async fn prepare_lock<'a>(&'a self, key: String) -> Box<dyn Lock + 'a + Send> {
        match self {
            LocksStorage::Memory(memory_locks) => memory_locks.prepare_lock(key).await,
            LocksStorage::Postgres(postgres_locks) => postgres_locks.prepare_lock(key).await,
        }
    }
}
