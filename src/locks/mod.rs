use serde::Deserialize;
use tracing::{debug, info};

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
fn hash_lock(bucket: &str, hash: &str) -> String {
    format!("hash:{}:{}", bucket, hash)
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) enum LocksType {
    #[serde(rename = "memory")]
    Memory,
}

pub(crate) trait Locks {
    fn new() -> Box<Self>
    where
        Self: Sized;

    fn acquire_shared(&mut self, key: &str);
    fn acquire_exclusive(&mut self, key: &str);
    fn release(&mut self, key: &str) -> bool;
}

#[derive(Clone)]
pub enum LocksStorage {
    Memory(memory::MemoryLocks),
}

impl LocksStorage {
    pub fn new(lock_type: &LocksType) -> Box<Self> {
        match lock_type {
            LocksType::Memory => {
                info!("Using memory as locks storage");
                Box::new(LocksStorage::Memory(*memory::MemoryLocks::new()))
            },
        }
    }

    /**
     * Acquire shared lock for key
     */
    pub fn acquire_shared(&mut self, key: &str) {
        debug!("Acquiring shared lock for key: {}", key);
        match self {
            LocksStorage::Memory(lock) => {
                lock.acquire_shared(key);
            }
        }
    }

    /**
     * Acquire exclusive lock for key
     */
    pub fn acquire_exclusive(&mut self, key: &str) {
        debug!("Acquiring exclusive lock for key: {}", key);
        match self {
            LocksStorage::Memory(lock) => {
                lock.acquire_exclusive(key);
            }
        }
    }

    /**
     * Release lock for key
     */
    pub fn release(&mut self, key: &str) -> bool {
        debug!("Releasing lock for key: {}", key);
        match self {
            LocksStorage::Memory(lock) => lock.release(key),
        }
    }
}
