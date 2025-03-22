use std::path::Path;
use crate::config::Config;
use crate::kvstorage::KVStorageTrait;
use crate::kvstorage::pooled::{RowModified, RowRefFile, RowRefcount};
use serde::Deserialize;
use sqlx::SqlitePool;
use tracing::debug;
use tracing::field::debug;

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
    async fn new(config: &Config) -> Result<Box<Self>, Box<dyn std::error::Error>> {
        let sqlite_config = config.sqlite.as_ref().unwrap();

        if !Path::new(&sqlite_config.path).exists() {
            std::fs::File::create(&sqlite_config.path)?;
        }

        let db_url = format!("sqlite://{}", sqlite_config.path);
        debug!("Connecting to SQLite database: {}", db_url);
        let pool = SqlitePool::connect(&db_url).await?;
        Ok(Box::new(SQLite { pool }))
    }

    async fn setup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<i32, Box<dyn std::error::Error>> {
        sqlx::query_as("SELECT refcount FROM refcount WHERE bucket = ?1 AND hash = ?2")
            .bind(bucket)
            .bind(hash)
            .fetch_one(&self.pool)
            .await
            .map(|row: RowRefcount| row.refcount)
            .or(Ok(0))
    }

    async fn set_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
        ref_cnt: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<i64, Box<dyn std::error::Error>> {
        sqlx::query_as("SELECT modified FROM modified WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .fetch_one(&self.pool)
            .await
            .map(|row: RowModified| row.modified)
            .or(Ok(0))
    }

    async fn set_modified(
        &mut self,
        bucket: &str,
        path: &str,
        modified: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<String, Box<dyn std::error::Error>> {
        sqlx::query_as("SELECT hash FROM ref_file WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .fetch_one(&self.pool)
            .await
            .map(|row: RowRefFile| row.hash)
            .or(Ok("".to_string()))
    }

    async fn set_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
        hash: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query("DELETE FROM ref_file WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
