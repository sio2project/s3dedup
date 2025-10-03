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
        tokio_async_drop!({
            // Lock the map to prevent concurrent modifications while we check the refcount
            let mut locks = self.parent.locks.write().await;
            // Only remove the entry if this is the last LockedKey holding it.
            // Arc::strong_count == 2 means: 1 in self.lock + 1 in the HashMap
            if Arc::strong_count(&self.lock) == 2 {
                locks.remove(&self.key);
            }
        });
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
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

    #[tokio::test(flavor = "multi_thread")]
    async fn assert_locks_compile() {
        let memory = MemoryLocks::new();
        let lock = memory.prepare_lock("1".into()).await;
        let _guard = lock.acquire_exclusive().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_shared_locks() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        for i in 0..3 {
            let memory = memory.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let lock = memory.prepare_lock("key1".into()).await;
                let _guard = lock.acquire_shared().await;
                tx.send(format!("acquired_{}", i)).await.unwrap();
                sleep(Duration::from_millis(50)).await;
                tx.send(format!("released_{}", i)).await.unwrap();
            });
        }
        drop(tx);

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }

        assert_eq!(messages.len(), 6);
        assert!(messages[0].starts_with("acquired_"));
        assert!(messages[1].starts_with("acquired_"));
        assert!(messages[2].starts_with("acquired_"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exclusive_lock_mutual_exclusion() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("key1".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx1.send("task1_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            tx1.send("task1_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("key1".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx2.send("task2_acquired").await.unwrap();
        });

        drop(tx);

        let messages: Vec<_> = rx
            .recv()
            .await
            .into_iter()
            .chain(rx.recv().await)
            .chain(rx.recv().await)
            .collect();
        assert_eq!(
            messages,
            vec!["task1_acquired", "task1_released", "task2_acquired"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_shared_exclusive_mutual_exclusion() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("key1".into()).await;
            let _guard = lock.acquire_shared().await;
            tx1.send("shared_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            tx1.send("shared_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("key1".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx2.send("exclusive_acquired").await.unwrap();
        });

        drop(tx);

        let messages: Vec<_> = rx
            .recv()
            .await
            .into_iter()
            .chain(rx.recv().await)
            .chain(rx.recv().await)
            .collect();
        assert_eq!(
            messages,
            vec!["shared_acquired", "shared_released", "exclusive_acquired"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_cleanup() {
        let memory = Arc::new(*MemoryLocks::new());

        {
            let lock = memory.prepare_lock("cleanup_key".into()).await;
            let _guard = lock.acquire_exclusive().await;
        }

        sleep(Duration::from_millis(50)).await;

        let locks_map = memory.locks.read().await;
        assert!(
            !locks_map.contains_key("cleanup_key"),
            "Lock should be cleaned up from HashMap"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_key_independence() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("key1".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx1.send("key1_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            tx1.send("key1_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("key2".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx2.send("key2_acquired").await.unwrap();
        });

        drop(tx);

        let msg1 = rx.recv().await.unwrap();
        let msg2 = rx.recv().await.unwrap();
        assert_eq!(msg1, "key1_acquired");
        assert_eq!(msg2, "key2_acquired");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_locked_keys_prevent_bypass() {
        // This test ensures that dropping one LockedKey doesn't remove the lock
        // from the map while other LockedKeys still exist, which would allow
        // a third task to bypass the lock
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("shared_key".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx1.send("task1_acquired").await.unwrap();
            sleep(Duration::from_millis(50)).await;
            tx1.send("task1_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("shared_key".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx2.send("task2_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            tx2.send("task2_released").await.unwrap();
        });

        sleep(Duration::from_millis(70)).await;

        let memory3 = memory.clone();
        let tx3 = tx.clone();
        tokio::spawn(async move {
            let lock = memory3.prepare_lock("shared_key".into()).await;
            let _guard = lock.acquire_exclusive().await;
            tx3.send("task3_acquired").await.unwrap();
        });

        drop(tx);

        let messages: Vec<_> = rx
            .recv()
            .await
            .into_iter()
            .chain(rx.recv().await)
            .chain(rx.recv().await)
            .chain(rx.recv().await)
            .chain(rx.recv().await)
            .collect();

        assert_eq!(
            messages,
            [
                "task1_acquired",
                "task1_released",
                "task2_acquired",
                "task2_released",
                "task3_acquired"
            ]
        );
    }
}
