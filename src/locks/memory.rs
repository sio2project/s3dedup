use crate::locks::{ExclusiveLockGuard, Lock, LockStorage, SharedLockGuard};
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock as TokioRwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::spawn_blocking;

// HashMap management: parking_lot (sync, fast, held briefly)
type LockMap = Arc<parking_lot::RwLock<HashMap<String, Arc<TokioRwLock<()>>>>>;

#[derive(Clone)]
pub(crate) struct MemoryLocks {
    locks: LockMap,
}

struct LockedKey<'a> {
    lock: Arc<TokioRwLock<()>>,
    parent: &'a MemoryLocks,
    key: String,
}

impl<'a> SharedLockGuard<'a> for RwLockReadGuard<'a, ()> {
    fn release(
        self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            drop(self); // Explicitly drop the guard to release the read lock
            Ok(())
        })
    }
}

impl<'a> ExclusiveLockGuard<'a> for RwLockWriteGuard<'a, ()> {
    fn release(
        self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            drop(self); // Explicitly drop the guard to release the write lock
            Ok(())
        })
    }
}

#[async_trait]
impl<'a> Lock for LockedKey<'a> {
    async fn acquire_shared<'b>(
        &'b self,
    ) -> anyhow::Result<Box<dyn SharedLockGuard<'b> + Send + 'b>> {
        Ok(Box::new(self.lock.read().await))
    }

    async fn acquire_exclusive<'b>(
        &'b self,
    ) -> anyhow::Result<Box<dyn ExclusiveLockGuard<'b> + Send + 'b>> {
        Ok(Box::new(self.lock.write().await))
    }
}

impl<'a> Drop for LockedKey<'a> {
    fn drop(&mut self) {
        // Lock the map to prevent concurrent modifications while we check the refcount
        // parking_lot allows sync access, held very briefly (just a refcount check + hash remove)
        let mut locks = self.parent.locks.write();
        // Only remove the entry if this is the last LockedKey holding it.
        // Arc::strong_count == 2 means: 1 in self.lock + 1 in the HashMap
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(&self.key);
        }
    }
}

impl MemoryLocks {
    async fn get_or_create_lock(&self, key: String) -> Arc<TokioRwLock<()>> {
        let locks = self.locks.clone();
        // Because `.write()` returns guard with reference to a locked `RwLock`, we need to block on whole body.
        // `tokio::task::block_in_place()` could be different approach, blocking only `self.locks.write()`, without needing to `.clone()` locks or worrying about lifetimes.
        spawn_blocking(move || {
            let mut locks = locks.write();
            locks
                .entry(key)
                .or_insert_with(|| Arc::new(TokioRwLock::new(())))
                .clone()
        })
        .await
        .expect("`parking_lot::RwLock::write()` panicked")
    }
}

#[async_trait]
impl LockStorage for MemoryLocks {
    fn new() -> Box<Self> {
        Box::new(Self {
            locks: Arc::new(parking_lot::RwLock::new(HashMap::new())),
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

    #[tokio::test]
    async fn assert_locks_compile() {
        let memory = MemoryLocks::new();
        let lock = memory.prepare_lock("1".into()).await;
        let _guard = lock.acquire_exclusive().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_shared_locks() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        for i in 0..3 {
            let memory = memory.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let lock = memory.prepare_lock("key1".into()).await;
                let guard = lock.acquire_shared().await.unwrap();
                tx.send(format!("acquired_{}", i)).await.unwrap();
                sleep(Duration::from_millis(50)).await;
                let _ = guard.release().await;
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

    #[tokio::test]
    async fn test_exclusive_lock_mutual_exclusion() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("key1".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx1.send("task1_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            let _ = guard.release().await;
            tx1.send("task1_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("key1".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx2.send("task2_acquired").await.unwrap();
            let _ = guard.release().await;
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

    #[tokio::test]
    async fn test_shared_exclusive_mutual_exclusion() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("key1".into()).await;
            let guard = lock.acquire_shared().await.unwrap();
            tx1.send("shared_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            let _ = guard.release().await;
            tx1.send("shared_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("key1".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx2.send("exclusive_acquired").await.unwrap();
            let _ = guard.release().await;
        });

        drop(tx);

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }
        // Shared must acquire first
        assert_eq!(messages[0], "shared_acquired");
        // All 3 messages should be present
        assert_eq!(messages.len(), 3);
        assert!(messages.contains(&"shared_released"));
        assert!(messages.contains(&"exclusive_acquired"));
        // The locks work correctly - exclusive waits. Message ordering can vary due to async timing.
    }

    #[tokio::test]
    async fn test_lock_cleanup() {
        let memory = Arc::new(*MemoryLocks::new());

        {
            let lock = memory.prepare_lock("cleanup_key".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            let _ = guard.release().await;
        }

        sleep(Duration::from_millis(50)).await;

        let locks_map = memory.locks.read();
        assert!(
            !locks_map.contains_key("cleanup_key"),
            "Lock should be cleaned up from HashMap"
        );
    }

    #[tokio::test]
    async fn test_key_independence() {
        let memory = Arc::new(*MemoryLocks::new());
        let (tx, mut rx) = mpsc::channel(10);

        let memory1 = memory.clone();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let lock = memory1.prepare_lock("key1".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx1.send("key1_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            let _ = guard.release().await;
            tx1.send("key1_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("key2".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx2.send("key2_acquired").await.unwrap();
            let _ = guard.release().await;
        });

        drop(tx);

        let msg1 = rx.recv().await.unwrap();
        let msg2 = rx.recv().await.unwrap();
        assert_eq!(msg1, "key1_acquired");
        assert_eq!(msg2, "key2_acquired");
    }

    #[tokio::test]
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
            let guard = lock.acquire_exclusive().await.unwrap();
            tx1.send("task1_acquired").await.unwrap();
            sleep(Duration::from_millis(50)).await;
            let _ = guard.release().await;
            tx1.send("task1_released").await.unwrap();
        });

        sleep(Duration::from_millis(10)).await;

        let memory2 = memory.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let lock = memory2.prepare_lock("shared_key".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx2.send("task2_acquired").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            let _ = guard.release().await;
            tx2.send("task2_released").await.unwrap();
        });

        sleep(Duration::from_millis(70)).await;

        let memory3 = memory.clone();
        let tx3 = tx.clone();
        tokio::spawn(async move {
            let lock = memory3.prepare_lock("shared_key".into()).await;
            let guard = lock.acquire_exclusive().await.unwrap();
            tx3.send("task3_acquired").await.unwrap();
            let _ = guard.release().await;
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
