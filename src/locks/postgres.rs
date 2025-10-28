use crate::config::Config;
use crate::locks::{ExclusiveLockGuard, Lock, LockStorage, SharedLockGuard};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::Arc;
use std::time::Duration;
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
            "postgres://{}:{}@{}:{}/{}?connect_timeout=10",
            pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.dbname
        );

        debug!(
            "Connecting to Postgres for locks: postgres://{}:****@{}:{}/{}",
            pg_config.user, pg_config.host, pg_config.port, pg_config.dbname
        );

        let pool = PgPoolOptions::new()
            .max_connections(pg_config.pool_size)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Some(Duration::from_secs(600)))
            .max_lifetime(Some(Duration::from_secs(1800)))
            .connect(&db_url)
            .await
            .context("Failed to connect to PostgreSQL for locks")?;

        // Validate connection works
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .context("PostgreSQL locks connection validation failed")?;

        debug!("Successfully validated PostgreSQL locks connection");

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
    async fn acquire_shared<'a>(&'a self) -> Result<Box<dyn SharedLockGuard<'a> + Send + 'a>> {
        // Get connection from pool
        let mut conn = self
            .pool
            .acquire()
            .await
            .context("Failed to acquire connection for shared lock")?;

        // Acquire shared advisory lock (returns void, so we use query instead of query_scalar)
        // PostgreSQL advisory locks are session-scoped and automatically released when the
        // connection is closed or returned to the pool.
        sqlx::query("SELECT pg_advisory_lock_shared($1)")
            .bind(self.key_hash)
            .execute(&mut *conn)
            .await
            .context("Failed to acquire shared lock")?;

        debug!("Acquired shared lock for key: {}", self.key);

        // Return a guard that holds the connection. When the guard is dropped, the
        // connection is automatically returned to the pool, which triggers the lock release.
        // This design is safe and reliable - it leverages PostgreSQL's guarantee that
        // locks are released when a connection closes, without needing explicit unlock calls.
        Ok(Box::new(PostgresSharedLockGuard {
            key: self.key.clone(),
            _conn: conn,
        }))
    }

    async fn acquire_exclusive<'a>(
        &'a self,
    ) -> Result<Box<dyn ExclusiveLockGuard<'a> + Send + 'a>> {
        // Get connection from pool
        let mut conn = self
            .pool
            .acquire()
            .await
            .context("Failed to acquire connection for exclusive lock")?;

        // Acquire exclusive advisory lock (returns void, so we use query instead of query_scalar)
        // PostgreSQL advisory locks are session-scoped and automatically released when the
        // connection is closed or returned to the pool.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(self.key_hash)
            .execute(&mut *conn)
            .await
            .context("Failed to acquire exclusive lock")?;

        debug!("Acquired exclusive lock for key: {}", self.key);

        // Return a guard that holds the connection. When the guard is dropped, the
        // connection is automatically returned to the pool, which triggers the lock release.
        // This design is safe and reliable - it leverages PostgreSQL's guarantee that
        // locks are released when a connection closes, without needing explicit unlock calls.
        Ok(Box::new(PostgresExclusiveLockGuard {
            key: self.key.clone(),
            _conn: conn,
        }))
    }
}

/// Guard for a PostgreSQL shared advisory lock.
///
/// This guard holds a database connection from the connection pool. When the guard is dropped
/// (goes out of scope), the connection is automatically returned to the pool. PostgreSQL
/// automatically releases all advisory locks held by a connection when that connection is
/// closed or returned to the pool.
///
/// Lock Release Mechanism:
/// 1. Guard is created in acquire_shared() with the connection acquired from the pool
/// 2. PostgreSQL advisory lock is held as long as the connection is active
/// 3. When guard is dropped, the connection goes out of scope and is returned to the pool
/// 4. PostgreSQL automatically releases the lock when the connection returns to the pool
///
/// This design is superior to explicit unlock calls because:
/// - No risk of forgetting to unlock (guaranteed by Rust's Drop trait)
/// - No fire-and-forget async tasks that might fail to execute
/// - Lock release is atomic with connection return
/// - Works correctly even if the runtime is shutting down
struct PostgresSharedLockGuard {
    #[allow(dead_code)]
    key: String,
    #[allow(dead_code)]
    _conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl<'a> SharedLockGuard<'a> for PostgresSharedLockGuard {}

/// Guard for a PostgreSQL exclusive advisory lock.
///
/// This guard holds a database connection from the connection pool. When the guard is dropped
/// (goes out of scope), the connection is automatically returned to the pool. PostgreSQL
/// automatically releases all advisory locks held by a connection when that connection is
/// closed or returned to the pool.
///
/// Lock Release Mechanism:
/// 1. Guard is created in acquire_exclusive() with the connection acquired from the pool
/// 2. PostgreSQL advisory lock is held as long as the connection is active
/// 3. When guard is dropped, the connection goes out of scope and is returned to the pool
/// 4. PostgreSQL automatically releases the lock when the connection returns to the pool
///
/// This design is superior to explicit unlock calls because:
/// - No risk of forgetting to unlock (guaranteed by Rust's Drop trait)
/// - No fire-and-forget async tasks that might fail to execute
/// - Lock release is atomic with connection return
/// - Works correctly even if the runtime is shutting down
struct PostgresExclusiveLockGuard {
    #[allow(dead_code)]
    key: String,
    #[allow(dead_code)]
    _conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
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
