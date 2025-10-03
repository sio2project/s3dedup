use std::fmt::Display;

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
#[allow(dead_code)]
fn hash_lock(bucket: &str, hash: &str) -> String {
    format!("hash:{}:{}", bucket, hash)
}

#[derive(Debug, Deserialize, Clone)]
pub enum LocksType {
    #[serde(rename = "memory")]
    Memory,
}

pub(crate) trait Locks {
    fn new() -> Box<Self>
    where
        Self: Sized;

    fn acquire_shared(&mut self, key: String);
    fn acquire_exclusive(&mut self, key: String);
    fn release(&mut self, key: impl AsRef<str>) -> bool;
}

#[allow(private_interfaces)]
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
            }
        }
    }

    /**
     * Acquire shared lock for key
     */
    pub fn acquire_shared(&mut self, key: String) {
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
    pub fn acquire_exclusive(&mut self, key: String) {
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
    pub fn release(&mut self, key: impl AsRef<str> + Display) -> bool {
        debug!("Releasing lock for key: {}", key);
        match self {
            LocksStorage::Memory(lock) => lock.release(key),
        }
    }
}
