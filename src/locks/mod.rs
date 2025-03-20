use serde::Deserialize;

pub mod memory;

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
                Box::new(LocksStorage::Memory(*memory::MemoryLocks::new()))
            }
        }
    }

    pub fn acquire_shared(&mut self, key: &str) {
        match self {
            LocksStorage::Memory(lock) => {
                lock.acquire_shared(key);
            }
        }
    }

    pub fn acquire_exclusive(&mut self, key: &str) {
        match self {
            LocksStorage::Memory(lock) => {
                lock.acquire_exclusive(key);
            }
        }
    }

    pub fn release(&mut self, key: &str) -> bool {
        match self {
            LocksStorage::Memory(lock) => {
                lock.release(key)
            }
        }
    }
}