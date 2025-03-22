use crate::locks::Locks;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type LockMap = Arc<RwLock<HashMap<String, Arc<RwLock<()>>>>>;
#[derive(Clone)]
pub(crate) struct MemoryLocks {
    locks: LockMap,
}

impl MemoryLocks {
    fn get_or_create_lock(&self, key: &str) -> Arc<RwLock<()>> {
        let mut locks = self.locks.write().unwrap();
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }
}

impl Locks for MemoryLocks {
    fn new() -> Box<Self> {
        Box::new(Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn acquire_shared(&mut self, key: &str) {
        let lock = self.get_or_create_lock(key);
        let _guard = lock.read().unwrap();
    }

    fn acquire_exclusive(&mut self, key: &str) {
        let lock = self.get_or_create_lock(key);
        let _guard = lock.write().unwrap();
    }

    fn release(&mut self, key: &str) -> bool {
        let mut locks = self.locks.write().unwrap();
        locks.remove(key).is_some()
    }
}
