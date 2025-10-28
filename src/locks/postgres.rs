use crate::config::Config;
use crate::locks::{ExclusiveLockGuard, Lock, LockStorage, SharedLockGuard};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub pool_size: u32,
}

/// PostgreSQL-based distributed locks using advisory locks
#[derive(Clone)]
pub struct PostgresLocks {
    pool: Arc<PgPool>,
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
    /// Create a new PostgreSQL locks instance with configuration
    pub async fn new_with_config(config: &Config) -> Result<Box<Self>> {
        let pg_config = config.postgres.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "PostgreSQL locks require PostgreSQL configuration, but none was provided"
            )
        })?;

        let db_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.dbname
        );

        debug!("Connecting to Postgres for locks: {}", db_url);

        let pool = PgPoolOptions::new()
            .max_connections(pg_config.pool_size)
            .connect(&db_url)
            .await?;

        Ok(Box::new(PostgresLocks {
            pool: Arc::new(pool),
        }))
    }
}

#[async_trait]
impl LockStorage for PostgresLocks {
    fn new() -> Box<Self> {
        panic!("PostgresLocks must be initialized with config via new_with_config");
    }

    async fn prepare_lock<'a>(&'a self, key: String) -> Box<dyn Lock + 'a + Send> {
        let key_hash = Self::hash_key(&key);
        Box::new(PostgresLock {
            pool: self.pool.clone(),
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

#[async_trait]
impl Lock for PostgresLock {
    async fn acquire_shared<'a>(&'a self) -> Box<dyn SharedLockGuard<'a> + Send + 'a> {
        // Get connection from pool
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("Failed to acquire connection for shared lock");

        // Acquire shared advisory lock (returns void, so we use query instead of query_scalar)
        sqlx::query("SELECT pg_advisory_lock_shared($1)")
            .bind(self.key_hash)
            .execute(&mut *conn)
            .await
            .expect("Failed to acquire shared lock");

        debug!("Acquired shared lock for key: {}", self.key);

        Box::new(PostgresSharedLockGuard {
            key: self.key.clone(),
            key_hash: self.key_hash,
            pool: self.pool.clone(),
        })
    }

    async fn acquire_exclusive<'a>(&'a self) -> Box<dyn ExclusiveLockGuard<'a> + Send + 'a> {
        // Get connection from pool
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("Failed to acquire connection for exclusive lock");

        // Acquire exclusive advisory lock (returns void, so we use query instead of query_scalar)
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(self.key_hash)
            .execute(&mut *conn)
            .await
            .expect("Failed to acquire exclusive lock");

        debug!("Acquired exclusive lock for key: {}", self.key);

        Box::new(PostgresExclusiveLockGuard {
            key: self.key.clone(),
            key_hash: self.key_hash,
            pool: self.pool.clone(),
        })
    }
}

struct PostgresSharedLockGuard {
    #[allow(dead_code)]
    key: String,
    key_hash: i64,
    pool: Arc<PgPool>,
}

impl Drop for PostgresSharedLockGuard {
    fn drop(&mut self) {
        // Release lock when guard is dropped
        let key_hash = self.key_hash;
        let pool = self.pool.clone();
        // Spawn background task to release lock
        // Note: We can't await in Drop, so we spawn a background task
        tokio::spawn(async move {
            if let Err(e) = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
                .bind(key_hash)
                .execute(&*pool)
                .await
            {
                tracing::warn!("Failed to release shared lock: {}", e);
            }
        });
    }
}

impl<'a> SharedLockGuard<'a> for PostgresSharedLockGuard {}

struct PostgresExclusiveLockGuard {
    #[allow(dead_code)]
    key: String,
    key_hash: i64,
    pool: Arc<PgPool>,
}

impl Drop for PostgresExclusiveLockGuard {
    fn drop(&mut self) {
        // Release lock when guard is dropped
        let key_hash = self.key_hash;
        let pool = self.pool.clone();
        // Spawn background task to release lock
        // Note: We can't await in Drop, so we spawn a background task
        tokio::spawn(async move {
            if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(key_hash)
                .execute(&*pool)
                .await
            {
                tracing::warn!("Failed to release exclusive lock: {}", e);
            }
        });
    }
}

impl<'a> ExclusiveLockGuard<'a> for PostgresExclusiveLockGuard {}

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
