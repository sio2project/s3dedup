use crate::config::Config;
use crate::db;
use crate::locks::{ExclusiveLockGuard, Lock, LockStorage, SharedLockGuard};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

/// PostgreSQL-based distributed locks using advisory locks.
/// Uses separate connection pools for file locks and hash locks to prevent
/// deadlock when a request needs both (file lock held, waiting for hash lock).
#[derive(Clone)]
pub struct PostgresLocks {
    /// Pool for file locks (keys starting with "file:")
    file_pool: Arc<PgPool>,
    /// Pool for hash locks (keys starting with "hash:")
    hash_pool: Arc<PgPool>,
}

impl PostgresLocks {
    /// Hash a lock key to a 64-bit integer for PostgreSQL advisory locks
    fn hash_key(key: &str) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as i64
    }
}

impl PostgresLocks {
    /// Create a new PostgreSQL locks instance with configuration.
    /// Creates two separate connection pools to prevent deadlock:
    /// - file_pool: for file locks (path-based)
    /// - hash_pool: for hash locks (content-based)
    pub async fn new_with_config(config: &Config) -> Result<Box<Self>> {
        let pg_config = config.postgres.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "PostgreSQL locks require PostgreSQL configuration, but none was provided"
            )
        })?;

        let file_pool = db::create_pg_pool(pg_config, "file locks").await?;
        let hash_pool = db::create_pg_pool(pg_config, "hash locks").await?;

        Ok(Box::new(PostgresLocks {
            file_pool: Arc::new(file_pool),
            hash_pool: Arc::new(hash_pool),
        }))
    }

    /// Select the appropriate pool based on lock key prefix
    fn select_pool(&self, key: &str) -> Arc<PgPool> {
        if key.starts_with("hash:") {
            self.hash_pool.clone()
        } else {
            // Default to file pool for "file:" and any other keys
            self.file_pool.clone()
        }
    }
}

#[async_trait]
impl LockStorage for PostgresLocks {
    fn new() -> Box<Self> {
        panic!("PostgresLocks must be initialized with config via new_with_config");
    }

    async fn prepare_lock<'a>(&'a self, key: String) -> Box<dyn Lock + 'a + Send> {
        let key_hash = Self::hash_key(&key);
        let pool = self.select_pool(&key);
        Box::new(PostgresLock {
            pool,
            key,
            key_hash,
        })
    }
}

struct PostgresLock {
    pool: Arc<PgPool>,
    key: String,
    key_hash: i64,
}

/// Maximum number of retry attempts for lock acquisition
const MAX_LOCK_RETRIES: u32 = 100;
/// Initial backoff delay in milliseconds
const INITIAL_BACKOFF_MS: u64 = 10;
/// Maximum backoff delay in milliseconds
const MAX_BACKOFF_MS: u64 = 1000;

#[async_trait]
impl Lock for PostgresLock {
    async fn acquire_shared<'a>(&'a self) -> Result<Box<dyn SharedLockGuard<'a> + Send + 'a>> {
        // Use non-blocking try_lock with retry to prevent connection pool exhaustion deadlock.
        // Each lock holds a connection, so blocking waits would deadlock when requests need
        // multiple locks (file + hash) but pool is exhausted.
        let mut backoff_ms = INITIAL_BACKOFF_MS;

        for attempt in 0..MAX_LOCK_RETRIES {
            // Get connection from pool
            let mut conn = self
                .pool
                .acquire()
                .await
                .context("Failed to acquire connection for shared lock")?;

            // Try to acquire shared advisory lock (non-blocking)
            let result: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock_shared($1)")
                .bind(self.key_hash)
                .fetch_one(&mut *conn)
                .await
                .context("Failed to try shared lock")?;

            if result.0 {
                // Lock acquired successfully
                debug!(
                    "Acquired shared lock for key: {} (attempt {})",
                    self.key,
                    attempt + 1
                );
                return Ok(Box::new(PostgresSharedLockGuard {
                    key: self.key.clone(),
                    key_hash: self.key_hash,
                    conn: Some(conn),
                }));
            }

            // Lock not available - drop connection and retry with backoff
            drop(conn);

            if attempt < MAX_LOCK_RETRIES - 1 {
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = db::next_backoff(backoff_ms, MAX_BACKOFF_MS);
            }
        }

        anyhow::bail!(
            "Failed to acquire shared lock for key '{}' after {} attempts",
            self.key,
            MAX_LOCK_RETRIES
        )
    }

    async fn acquire_exclusive<'a>(
        &'a self,
    ) -> Result<Box<dyn ExclusiveLockGuard<'a> + Send + 'a>> {
        // Use non-blocking try_lock with retry to prevent connection pool exhaustion deadlock.
        // Each lock holds a connection, so blocking waits would deadlock when requests need
        // multiple locks (file + hash) but pool is exhausted.
        let mut backoff_ms = INITIAL_BACKOFF_MS;

        for attempt in 0..MAX_LOCK_RETRIES {
            // Get connection from pool
            let mut conn = self
                .pool
                .acquire()
                .await
                .context("Failed to acquire connection for exclusive lock")?;

            // Try to acquire exclusive advisory lock (non-blocking)
            let result: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
                .bind(self.key_hash)
                .fetch_one(&mut *conn)
                .await
                .context("Failed to try exclusive lock")?;

            if result.0 {
                // Lock acquired successfully
                debug!(
                    "Acquired exclusive lock for key: {} (attempt {})",
                    self.key,
                    attempt + 1
                );
                return Ok(Box::new(PostgresExclusiveLockGuard {
                    key: self.key.clone(),
                    key_hash: self.key_hash,
                    conn: Some(conn),
                }));
            }

            // Lock not available - drop connection and retry with backoff
            drop(conn);

            if attempt < MAX_LOCK_RETRIES - 1 {
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = db::next_backoff(backoff_ms, MAX_BACKOFF_MS);
            }
        }

        anyhow::bail!(
            "Failed to acquire exclusive lock for key '{}' after {} attempts",
            self.key,
            MAX_LOCK_RETRIES
        )
    }
}

/// Wrapper around a shared advisory lock that requires explicit async release
pub struct PostgresSharedLockGuard {
    key: String,
    key_hash: i64,
    conn: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
}

impl<'a> SharedLockGuard<'a> for PostgresSharedLockGuard {
    fn release(
        mut self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(mut conn) = self.conn.take() {
                sqlx::query("SELECT pg_advisory_unlock_shared($1)")
                    .bind(self.key_hash)
                    .execute(&mut *conn)
                    .await
                    .context("Failed to release shared advisory lock")?;
                debug!("Released shared lock for key: {}", self.key);
            }
            Ok(())
        })
    }
}

impl Drop for PostgresSharedLockGuard {
    fn drop(&mut self) {
        // Best-effort cleanup: if connection wasn't released explicitly, spawn a task to unlock
        if let Some(mut conn) = self.conn.take() {
            let key_hash = self.key_hash;
            let key = self.key.clone();
            tokio::spawn(async move {
                let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
                    .bind(key_hash)
                    .execute(&mut *conn)
                    .await;
                tracing::warn!(
                    "PostgreSQL shared lock guard dropped without explicit release for key: {}",
                    key
                );
            });
        }
    }
}

/// Wrapper around an exclusive advisory lock that requires explicit async release
pub struct PostgresExclusiveLockGuard {
    key: String,
    key_hash: i64,
    conn: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
}

impl<'a> ExclusiveLockGuard<'a> for PostgresExclusiveLockGuard {
    fn release(
        mut self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(mut conn) = self.conn.take() {
                sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(self.key_hash)
                    .execute(&mut *conn)
                    .await
                    .context("Failed to release exclusive advisory lock")?;
                debug!("Released exclusive lock for key: {}", self.key);
            }
            Ok(())
        })
    }
}

impl Drop for PostgresExclusiveLockGuard {
    fn drop(&mut self) {
        // Best-effort cleanup: if connection wasn't released explicitly, spawn a task to unlock
        if let Some(mut conn) = self.conn.take() {
            let key_hash = self.key_hash;
            let key = self.key.clone();
            tokio::spawn(async move {
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(key_hash)
                    .execute(&mut *conn)
                    .await;
                tracing::warn!(
                    "PostgreSQL exclusive lock guard dropped without explicit release for key: {}",
                    key
                );
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_key_deterministic() {
        let key = "file:bucket:path";
        let hash1 = PostgresLocks::hash_key(key);
        let hash2 = PostgresLocks::hash_key(key);
        assert_eq!(hash1, hash2, "Hash should be deterministic");
    }

    #[test]
    fn test_hash_key_different_for_different_keys() {
        let hash1 = PostgresLocks::hash_key("file:bucket:path1");
        let hash2 = PostgresLocks::hash_key("file:bucket:path2");
        assert_ne!(
            hash1, hash2,
            "Different keys should produce different hashes"
        );
    }

    #[test]
    fn test_hash_key_returns_i64() {
        let hash = PostgresLocks::hash_key("test");
        // Just verify that hash is computed (all i64 values are valid for advisory locks)
        let _hash = hash;
    }
}
