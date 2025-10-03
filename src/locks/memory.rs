use crate::locks::{ExclusiveLockGuard, Lock, LockStorage, SharedLockGuard};
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio_async_drop::tokio_async_drop;

type LockType<T> = Arc<RwLock<T>>;
type LockMap = LockType<HashMap<String, LockType<()>>>;

#[derive(Clone)]
pub(crate) struct MemoryLocks {
    locks: LockMap,
}

struct LockedKey<'a> {
    lock: LockType<()>,
    parent: &'a MemoryLocks,
    key: String,
}

impl<'a> SharedLockGuard<'a> for RwLockReadGuard<'a, ()> {}
impl<'a> ExclusiveLockGuard<'a> for RwLockWriteGuard<'a, ()> {}

#[async_trait]
impl<'b> Lock for LockedKey<'b> {
    async fn acquire_shared<'a>(&'a self) -> Box<dyn SharedLockGuard<'a> + 'a + Send> {
        Box::new(self.lock.read().await)
    }

    async fn acquire_exclusive<'a>(&'a self) -> Box<dyn ExclusiveLockGuard<'a> + 'a + Send> {
        Box::new(self.lock.write().await)
    }
}

impl<'a> Drop for LockedKey<'a> {
    fn drop(&mut self) {
        tokio_async_drop!({ self.parent.locks.write().await.remove(&self.key) });
    }
}

impl MemoryLocks {
    async fn get_or_create_lock(&self, key: String) -> Arc<RwLock<()>> {
        let mut locks = self.locks.write().await;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }
}

#[async_trait]
impl LockStorage for MemoryLocks {
    fn new() -> Box<Self> {
        Box::new(Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn prepare_lock<'a>(&'a self, key: String) -> Box<dyn Lock + 'a + Send> {
        let lock = self.get_or_create_lock(key.clone()).await;
        Box::new(LockedKey {
            lock,
            parent: self,
            key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn assert_locks_compile() {
        let memory = MemoryLocks::new();
        let lock = memory.prepare_lock("1".into()).await;
        let _guard = lock.acquire_exclusive().await;
    }
}
