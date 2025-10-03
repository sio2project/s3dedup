use crate::locks::Locks;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type LockMap = Arc<RwLock<HashMap<String, Arc<RwLock<()>>>>>;
#[derive(Clone)]
pub(crate) struct MemoryLocks {
    locks: LockMap,
}

impl MemoryLocks {
    fn get_or_create_lock(&self, key: String) -> Arc<RwLock<()>> {
        let mut locks = self.locks.write().unwrap();
        locks
            .entry(key)
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

    fn acquire_shared(&mut self, key: String) {
        let lock = self.get_or_create_lock(key);
        let _guard = lock.read().unwrap();
    }

    fn acquire_exclusive(&mut self, key: String) {
        let lock = self.get_or_create_lock(key);
        let _guard = lock.write().unwrap();
    }

    fn release(&mut self, key: impl AsRef<str>) -> bool {
        let mut locks = self.locks.write().unwrap();
        locks.remove(key.as_ref()).is_some()
    }
}
