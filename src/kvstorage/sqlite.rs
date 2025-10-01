use std::path::Path;
use crate::config::BucketConfig;
use crate::kvstorage::KVStorageTrait;
use serde::Deserialize;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use tracing::debug;

#[derive(Debug, Clone, Deserialize)]
pub struct SQLiteConfig {
    pub path: String,
    pub pool_size: u32,
}

#[derive(Clone)]
pub struct SQLite {
    pool: SqlitePool,
}

impl KVStorageTrait for SQLite {
    async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let sqlite_config = config.sqlite.as_ref().unwrap();

        if !Path::new(&sqlite_config.path).exists() {
            std::fs::File::create(&sqlite_config.path)?;
        }

        let db_url = format!("sqlite://{}?mode=rwc", sqlite_config.path);
        debug!("Connecting to SQLite database: {}", db_url);

        let pool = SqlitePoolOptions::new()
            .max_connections(sqlite_config.pool_size)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&db_url)
            .await?;

        // Enable WAL mode for better concurrency
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout=30000")
            .execute(&pool)
            .await?;

        Ok(Box::new(SQLite { pool }))
    }

    async fn setup(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS refcount (
                bucket TEXT NOT NULL,
                hash TEXT NOT NULL,
                refcount INTEGER NOT NULL,
                PRIMARY KEY (bucket, hash)
            );
            CREATE TABLE IF NOT EXISTS modified (
                bucket TEXT NOT NULL,
                path TEXT NOT NULL,
                modified INTEGER NOT NULL,
                PRIMARY KEY (bucket, path)
            );
            CREATE TABLE IF NOT EXISTS ref_file (
                bucket TEXT NOT NULL,
                path TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (bucket, path)
            );
            CREATE TABLE IF NOT EXISTS logical_size (
                bucket TEXT NOT NULL,
                hash TEXT NOT NULL,
                logical_size INTEGER NOT NULL,
                PRIMARY KEY (bucket, hash)
            );",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        let result: Result<(i32,), sqlx::Error> = sqlx::query_as(
            "SELECT refcount FROM refcount WHERE bucket = ?1 AND hash = ?2"
        )
        .bind(bucket)
        .bind(hash)
        .fetch_one(&self.pool)
        .await;

        match result {
            Ok((count,)) => Ok(count),
            Err(_) => Ok(0),
        }
    }

    async fn set_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
        ref_cnt: i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query("INSERT OR REPLACE INTO refcount (bucket, hash, refcount) VALUES (?1, ?2, ?3)")
            .bind(bucket)
            .bind(hash)
            .bind(ref_cnt)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_modified(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let result: Result<(i64,), sqlx::Error> = sqlx::query_as(
            "SELECT modified FROM modified WHERE bucket = ?1 AND path = ?2"
        )
        .bind(bucket)
        .bind(path)
        .fetch_one(&self.pool)
        .await;

        match result {
            Ok((modified,)) => Ok(modified),
            Err(_) => Ok(0),
        }
    }

    async fn set_modified(
        &mut self,
        bucket: &str,
        path: &str,
        modified: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query("INSERT OR REPLACE INTO modified (bucket, path, modified) VALUES (?1, ?2, ?3)")
            .bind(bucket)
            .bind(path)
            .bind(modified)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_modified(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query("DELETE FROM modified WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let result: Result<(String,), sqlx::Error> = sqlx::query_as(
            "SELECT hash FROM ref_file WHERE bucket = ?1 AND path = ?2"
        )
        .bind(bucket)
        .bind(path)
        .fetch_one(&self.pool)
        .await;

        match result {
            Ok((hash,)) => Ok(hash),
            Err(_) => Ok("".to_string()),
        }
    }

    async fn set_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
        hash: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query("INSERT OR REPLACE INTO ref_file (bucket, path, hash) VALUES (?1, ?2, ?3)")
            .bind(bucket)
            .bind(path)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query("DELETE FROM ref_file WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_logical_size(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let result: Result<(i64,), sqlx::Error> = sqlx::query_as(
            "SELECT logical_size FROM logical_size WHERE bucket = ?1 AND hash = ?2"
        )
        .bind(bucket)
        .bind(hash)
        .fetch_one(&self.pool)
        .await;

        match result {
            Ok((size,)) => Ok(size as usize),
            Err(_) => Ok(0), // Default to 0 if not found
        }
    }

    async fn set_logical_size(
        &mut self,
        bucket: &str,
        hash: &str,
        size: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sqlx::query("INSERT OR REPLACE INTO logical_size (bucket, hash, logical_size) VALUES (?1, ?2, ?3)")
            .bind(bucket)
            .bind(hash)
            .bind(size as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_files(
        &mut self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let pattern = format!("{}%", path_prefix);
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT path FROM modified WHERE bucket = ?1 AND path LIKE ?2 AND modified <= ?3 ORDER BY path"
        )
        .bind(bucket)
        .bind(pattern)
        .bind(timestamp)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(path,)| path).collect())
    }
}
