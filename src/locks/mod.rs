use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

pub mod memory;

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
}

pub(crate) trait SharedLockGuard<'a> {}
pub(crate) trait ExclusiveLockGuard<'a> {}

#[async_trait]
pub(crate) trait Lock {
    async fn acquire_shared<'a>(&'a self) -> Box<dyn SharedLockGuard<'a> + 'a + Send>;
    async fn acquire_exclusive<'a>(&'a self) -> Box<dyn ExclusiveLockGuard<'a> + 'a + Send>;
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
}

impl LocksStorage {
    pub fn new(lock_type: LocksType) -> Box<Self> {
        match lock_type {
            LocksType::Memory => {
                info!("Using memory as locks storage");
                Box::new(LocksStorage::Memory(*memory::MemoryLocks::new()))
            }
        }
    }

    pub(crate) async fn prepare_lock<'a>(&'a self, key: String) -> Box<dyn Lock + 'a + Send> {
        match self {
            LocksStorage::Memory(memory_locks) => memory_locks.prepare_lock(key).await,
        }
    }
}
