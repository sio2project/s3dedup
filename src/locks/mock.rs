use crate::locks::{ExclusiveLockGuard, Lock, SharedLockGuard};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Mock lock manager with configurable failure injection.
///
/// By default, all operations succeed immediately (no actual locking).
/// Call `set_failing("acquire_exclusive")` / `set_failing("acquire_shared")`
/// / `set_failing("release")` to make those operations return errors.
#[derive(Clone)]
pub struct MockLocks {
    fail_ops: Arc<Mutex<HashSet<String>>>,
}

impl Default for MockLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl MockLocks {
    pub fn new() -> Self {
        Self {
            fail_ops: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Configure an operation to fail. Valid names: "acquire_exclusive", "acquire_shared", "release".
    pub fn set_failing(&self, op: &str) {
        self.fail_ops.lock().unwrap().insert(op.to_string());
    }

    /// Stop an operation from failing.
    pub fn clear_failing(&self, op: &str) {
        self.fail_ops.lock().unwrap().remove(op);
    }

    pub async fn prepare_lock(&self, _key: String) -> Box<dyn Lock + Send + '_> {
        Box::new(MockLock {
            fail_ops: self.fail_ops.clone(),
        })
    }
}

struct MockLock {
    fail_ops: Arc<Mutex<HashSet<String>>>,
}

#[async_trait]
impl Lock for MockLock {
    async fn acquire_shared<'a>(&'a self) -> Result<Box<dyn SharedLockGuard<'a> + Send + 'a>> {
        if self.fail_ops.lock().unwrap().contains("acquire_shared") {
            return Err(anyhow!("MockLocks: injected failure for acquire_shared"));
        }
        Ok(Box::new(MockSharedGuard {
            fail_ops: self.fail_ops.clone(),
        }))
    }

    async fn acquire_exclusive<'a>(
        &'a self,
    ) -> Result<Box<dyn ExclusiveLockGuard<'a> + Send + 'a>> {
        if self.fail_ops.lock().unwrap().contains("acquire_exclusive") {
            return Err(anyhow!("MockLocks: injected failure for acquire_exclusive"));
        }
        Ok(Box::new(MockExclusiveGuard {
            fail_ops: self.fail_ops.clone(),
        }))
    }
}

struct MockSharedGuard {
    fail_ops: Arc<Mutex<HashSet<String>>>,
}

impl<'a> SharedLockGuard<'a> for MockSharedGuard {
    fn release(
        self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.fail_ops.lock().unwrap().contains("release") {
                return Err(anyhow!("MockLocks: injected failure for release"));
            }
            Ok(())
        })
    }
}

struct MockExclusiveGuard {
    fail_ops: Arc<Mutex<HashSet<String>>>,
}

impl<'a> ExclusiveLockGuard<'a> for MockExclusiveGuard {
    fn release(
        self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.fail_ops.lock().unwrap().contains("release") {
                return Err(anyhow!("MockLocks: injected failure for release"));
            }
            Ok(())
        })
    }
}
