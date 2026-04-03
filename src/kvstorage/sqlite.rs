use crate::config::Config;
use crate::kvstorage::KVStorageTrait;
use anyhow::{Context, Result};
use serde::Deserialize;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::time::Duration;
use tracing::debug;

#[derive(Debug, Clone, Deserialize)]
pub struct SQLiteConfig {
    pub path: String,
    pub pool_size: u32,
}

#[derive(Clone)]
pub struct SQLite {
    pool: SqlitePool,
    bucket: String,
}

impl KVStorageTrait for SQLite {
    async fn new(config: &Config) -> Result<Box<Self>> {
        let sqlite_config = config.sqlite.as_ref().unwrap();

        // Ensure parent directory exists
        if let Some(parent) = Path::new(&sqlite_config.path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // SQLite with mode=rwc creates file atomically if needed
        let db_url = format!("sqlite://{}?mode=rwc", sqlite_config.path);
        debug!("Connecting to SQLite database: {}", db_url);

        let pool = SqlitePoolOptions::new()
            .max_connections(sqlite_config.pool_size)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&db_url)
            .await
            .context("Failed to connect to SQLite")?;

        // Validate connection works
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .context("SQLite connection validation failed")?;

        debug!("Successfully validated SQLite connection");

        // Enable WAL mode for better concurrency
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout=30000")
            .execute(&pool)
            .await?;

        Ok(Box::new(SQLite {
            pool,
            bucket: config.bucket.name.clone(),
        }))
    }

    async fn setup(&self) -> Result<()> {
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
                compressed_size INTEGER,
                PRIMARY KEY (bucket, hash)
            );
            CREATE TABLE IF NOT EXISTS version (
                bucket TEXT NOT NULL PRIMARY KEY,
                version TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ref_file_hash ON ref_file(bucket, hash);",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        let result: Result<(i32,), sqlx::Error> =
            sqlx::query_as("SELECT refcount FROM refcount WHERE bucket = ?1 AND hash = ?2")
                .bind(bucket)
                .bind(hash)
                .fetch_one(&self.pool)
                .await;

        match result {
            Ok((count,)) => Ok(count),
            Err(_) => Ok(0),
        }
    }

    async fn atomic_increment_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        // SQLite atomic increment using INSERT...ON CONFLICT...RETURNING (SQLite 3.35+)
        // - New hash: INSERT with refcount=1, return 1
        // - Existing hash: UPDATE refcount = refcount + 1, return new value
        let (count,): (i32,) = sqlx::query_as(
            "INSERT INTO refcount (bucket, hash, refcount) VALUES (?1, ?2, 1)
             ON CONFLICT (bucket, hash) DO UPDATE SET refcount = refcount + 1
             RETURNING refcount",
        )
        .bind(bucket)
        .bind(hash)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    async fn atomic_decrement_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        // SQLite atomic decrement using UPDATE...RETURNING (SQLite 3.35+)
        // Use fetch_optional to return 0 if row doesn't exist (matches PostgreSQL behavior)
        let result = sqlx::query_as::<_, (i32,)>(
            "UPDATE refcount SET refcount = MAX(0, refcount - 1)
             WHERE bucket = ?1 AND hash = ?2
             RETURNING refcount",
        )
        .bind(bucket)
        .bind(hash)
        .fetch_optional(&self.pool)
        .await;

        match result {
            Ok(Some((count,))) => Ok(count),
            Ok(None) => Ok(0), // Row not found, return 0
            Err(e) => Err(e.into()),
        }
    }

    async fn get_modified(&self, bucket: &str, path: &str) -> Result<i64> {
        let result: Result<(i64,), sqlx::Error> =
            sqlx::query_as("SELECT modified FROM modified WHERE bucket = ?1 AND path = ?2")
                .bind(bucket)
                .bind(path)
                .fetch_one(&self.pool)
                .await;

        match result {
            Ok((modified,)) => Ok(modified),
            Err(_) => Ok(0),
        }
    }

    async fn set_modified(&self, bucket: &str, path: &str, modified: i64) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO modified (bucket, path, modified) VALUES (?1, ?2, ?3)")
            .bind(bucket)
            .bind(path)
            .bind(modified)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_modified(&self, bucket: &str, path: &str) -> Result<()> {
        sqlx::query("DELETE FROM modified WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_ref_file(&self, bucket: &str, path: &str) -> Result<String> {
        let result: Result<(String,), sqlx::Error> =
            sqlx::query_as("SELECT hash FROM ref_file WHERE bucket = ?1 AND path = ?2")
                .bind(bucket)
                .bind(path)
                .fetch_one(&self.pool)
                .await;

        match result {
            Ok((hash,)) => Ok(hash),
            Err(_) => Ok("".to_string()),
        }
    }

    async fn set_ref_file(&self, bucket: &str, path: &str, hash: &str) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO ref_file (bucket, path, hash) VALUES (?1, ?2, ?3)")
            .bind(bucket)
            .bind(path)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_ref_file(&self, bucket: &str, path: &str) -> Result<()> {
        sqlx::query("DELETE FROM ref_file WHERE bucket = ?1 AND path = ?2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_logical_size(&self, bucket: &str, hash: &str) -> Result<usize> {
        let result: Result<(i64,), sqlx::Error> =
            sqlx::query_as("SELECT logical_size FROM logical_size WHERE bucket = ?1 AND hash = ?2")
                .bind(bucket)
                .bind(hash)
                .fetch_one(&self.pool)
                .await;

        match result {
            Ok((size,)) => Ok(size as usize),
            Err(_) => Ok(0), // Default to 0 if not found
        }
    }

    async fn set_logical_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        sqlx::query(
            "INSERT INTO logical_size (bucket, hash, logical_size, compressed_size) VALUES (?1, ?2, ?3, 0)
             ON CONFLICT (bucket, hash) DO UPDATE SET logical_size = ?3",
        )
        .bind(bucket)
        .bind(hash)
        .bind(size as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_files(
        &self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>> {
        let escaped = path_prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{}%", escaped);
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT path FROM modified WHERE bucket = ?1 AND path LIKE ?2 ESCAPE '\\' AND modified <= ?3 ORDER BY path"
        )
        .bind(bucket)
        .bind(pattern)
        .bind(timestamp)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(path,)| path).collect())
    }

    async fn list_orphaned_ref_files(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT rf.path, rf.hash FROM ref_file rf
             LEFT JOIN refcount rc ON rf.bucket = rc.bucket AND rf.hash = rc.hash
             WHERE rf.bucket = ?1 AND (rc.refcount IS NULL OR rc.refcount = 0)
               AND rf.path > ?2
             ORDER BY rf.path LIMIT ?3",
        )
        .bind(bucket)
        .bind(after_cursor)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_orphaned_refcounts(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, i32)>> {
        let rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT rc.hash, rc.refcount FROM refcount rc
             LEFT JOIN ref_file rf ON rc.bucket = rf.bucket AND rc.hash = rf.hash
             WHERE rc.bucket = ?1 AND rf.hash IS NULL
               AND rc.hash > ?2
             ORDER BY rc.hash LIMIT ?3",
        )
        .bind(bucket)
        .bind(after_cursor)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_orphaned_logical_sizes(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ls.hash FROM logical_size ls
             LEFT JOIN refcount rc ON ls.bucket = rc.bucket AND ls.hash = rc.hash
             WHERE ls.bucket = ?1 AND (rc.refcount IS NULL OR rc.refcount = 0)
               AND ls.hash > ?2
             ORDER BY ls.hash LIMIT ?3",
        )
        .bind(bucket)
        .bind(after_cursor)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(hash,)| hash).collect())
    }

    async fn delete_refcount(&self, bucket: &str, hash: &str) -> Result<()> {
        sqlx::query("DELETE FROM refcount WHERE bucket = ?1 AND hash = ?2")
            .bind(bucket)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_logical_size(&self, bucket: &str, hash: &str) -> Result<()> {
        sqlx::query("DELETE FROM logical_size WHERE bucket = ?1 AND hash = ?2")
            .bind(bucket)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn hash_is_referenced(&self, bucket: &str, hash: &str) -> Result<bool> {
        let result: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM ref_file WHERE bucket = ?1 AND hash = ?2 LIMIT 1")
                .bind(bucket)
                .bind(hash)
                .fetch_optional(&self.pool)
                .await?;
        Ok(result.is_some())
    }

    async fn get_compressed_size(&self, bucket: &str, hash: &str) -> Result<usize> {
        let result: Result<(Option<i64>,), sqlx::Error> = sqlx::query_as(
            "SELECT compressed_size FROM logical_size WHERE bucket = ?1 AND hash = ?2",
        )
        .bind(bucket)
        .bind(hash)
        .fetch_one(&self.pool)
        .await;

        match result {
            Ok((Some(size),)) => Ok(size as usize),
            Ok((None,)) => Ok(0),
            Err(sqlx::Error::RowNotFound) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    async fn set_compressed_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        sqlx::query(
            "INSERT INTO logical_size (bucket, hash, logical_size, compressed_size) VALUES (?1, ?2, 0, ?3)
             ON CONFLICT (bucket, hash) DO UPDATE SET compressed_size = ?3",
        )
        .bind(bucket)
        .bind(hash)
        .bind(size as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_total_files(&self, bucket: &str) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM modified WHERE bucket = ?1")
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn get_total_blobs(&self, bucket: &str) -> Result<i64> {
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM refcount WHERE bucket = ?1 AND refcount > 0")
                .bind(bucket)
                .fetch_one(&self.pool)
                .await?;
        Ok(count)
    }

    async fn get_total_storage_bytes(&self, bucket: &str) -> Result<i64> {
        // Only count storage for blobs that are actually referenced (refcount > 0)
        let (total,): (Option<i64>,) = sqlx::query_as(
            "SELECT SUM(l.compressed_size)
             FROM logical_size l
             INNER JOIN refcount r ON l.bucket = r.bucket AND l.hash = r.hash
             WHERE l.bucket = ?1 AND r.refcount > 0",
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await?;
        Ok(total.unwrap_or(0))
    }

    async fn get_total_logical_bytes(&self, bucket: &str) -> Result<i64> {
        let (total,): (Option<i64>,) = sqlx::query_as(
            "SELECT SUM(r.refcount * l.logical_size)
             FROM refcount r
             INNER JOIN logical_size l ON r.bucket = l.bucket AND r.hash = l.hash
             WHERE r.bucket = ?1",
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await?;
        Ok(total.unwrap_or(0))
    }

    async fn get_total_compressed_bytes_no_dedup(&self, bucket: &str) -> Result<i64> {
        let (total,): (Option<i64>,) = sqlx::query_as(
            "SELECT SUM(r.refcount * l.compressed_size)
             FROM refcount r
             INNER JOIN logical_size l ON r.bucket = l.bucket AND r.hash = l.hash
             WHERE r.bucket = ?1 AND r.refcount > 0",
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await?;
        Ok(total.unwrap_or(0))
    }

    async fn get_deduplicated_bytes_saved(&self, bucket: &str) -> Result<i64> {
        let (total,): (Option<i64>,) = sqlx::query_as(
            "SELECT SUM((r.refcount - 1) * l.logical_size)
             FROM refcount r
             INNER JOIN logical_size l ON r.bucket = l.bucket AND r.hash = l.hash
             WHERE r.bucket = ?1 AND r.refcount > 1",
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await?;
        Ok(total.unwrap_or(0))
    }

    async fn get_storage_stats(&self, bucket: &str) -> Result<crate::kvstorage::StorageStats> {
        type Row = (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        );
        let row: Row = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM modified WHERE bucket = ?1),
                agg.total_blobs,
                agg.total_storage_bytes,
                agg.total_logical_bytes,
                agg.deduplicated_bytes_saved,
                agg.total_compressed_bytes_no_dedup
             FROM (
                SELECT
                    COUNT(*) AS total_blobs,
                    SUM(l.compressed_size) AS total_storage_bytes,
                    SUM(r.refcount * l.logical_size) AS total_logical_bytes,
                    SUM(CASE WHEN r.refcount > 1 THEN (r.refcount - 1) * l.logical_size ELSE 0 END) AS deduplicated_bytes_saved,
                    SUM(r.refcount * l.compressed_size) AS total_compressed_bytes_no_dedup
                FROM refcount r
                INNER JOIN logical_size l ON r.bucket = l.bucket AND r.hash = l.hash
                WHERE r.bucket = ?1 AND r.refcount > 0
             ) agg",
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await?;
        Ok(crate::kvstorage::StorageStats {
            total_files: row.0.unwrap_or(0),
            total_blobs: row.1.unwrap_or(0),
            total_storage_bytes: row.2.unwrap_or(0),
            total_logical_bytes: row.3.unwrap_or(0),
            deduplicated_bytes_saved: row.4.unwrap_or(0),
            total_compressed_bytes_no_dedup: row.5.unwrap_or(0),
        })
    }

    fn get_pool_stats(&self) -> (u32, u32) {
        let total_connections = self.pool.size();
        let idle_connections = self.pool.num_idle() as u32;
        let active_connections = total_connections.saturating_sub(idle_connections);
        (active_connections, idle_connections)
    }

    async fn get_version(&self) -> Result<Option<String>> {
        let result: Option<(String,)> =
            sqlx::query_as("SELECT version FROM version WHERE bucket = ?1")
                .bind(&self.bucket)
                .fetch_optional(&self.pool)
                .await?;
        Ok(result.map(|r| r.0))
    }

    async fn set_version(&self, version: &str) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO version (bucket, version) VALUES (?1, ?2)")
            .bind(&self.bucket)
            .bind(version)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
