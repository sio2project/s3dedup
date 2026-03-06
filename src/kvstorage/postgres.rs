use crate::config::Config;
use crate::kvstorage::KVStorageTrait;
use anyhow::{Context, Result};
use serde::Deserialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
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

#[derive(Clone)]
pub struct Postgres {
    pool: PgPool,
    bucket: String,
}

impl Postgres {
    fn table_name(&self, base: &str) -> String {
        // Sanitize bucket name to be SQL-safe (replace hyphens with underscores)
        let safe_bucket = self.bucket.replace("-", "_");
        format!("{}_{}", safe_bucket, base)
    }
}

impl KVStorageTrait for Postgres {
    async fn new(config: &Config) -> Result<Box<Self>> {
        let pg_config = config.postgres.as_ref().unwrap();
        let db_url = format!(
            "postgres://{}:{}@{}:{}/{}?connect_timeout=10",
            pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.dbname
        );
        debug!(
            "Connecting to Postgres: postgres://{}:****@{}:{}/{}",
            pg_config.user, pg_config.host, pg_config.port, pg_config.dbname
        );

        let pool = PgPoolOptions::new()
            .max_connections(pg_config.pool_size)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Some(Duration::from_secs(600)))
            .max_lifetime(Some(Duration::from_secs(1800)))
            .connect(&db_url)
            .await
            .context("Failed to connect to PostgreSQL")?;

        // Validate connection works
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .context("PostgreSQL connection validation failed")?;

        debug!("Successfully validated PostgreSQL connection");

        Ok(Box::new(Postgres {
            pool,
            bucket: config.bucket.name.clone(),
        }))
    }
    async fn setup(&mut self) -> Result<()> {
        // PostgreSQL doesn't support multiple statements in a single query
        // Create tables with bucket-specific names to allow parallel tests
        let refcount_table = self.table_name("refcount");
        let modified_table = self.table_name("modified");
        let ref_file_table = self.table_name("ref_file");
        let logical_size_table = self.table_name("logical_size");

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                refcount INT NOT NULL,
                PRIMARY KEY (bucket, hash)
            )",
            refcount_table
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                modified BIGINT NOT NULL,
                PRIMARY KEY (bucket, path)
            )",
            modified_table
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                PRIMARY KEY (bucket, path)
            )",
            ref_file_table
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                logical_size BIGINT NOT NULL,
                compressed_size BIGINT,
                PRIMARY KEY (bucket, hash)
            )",
            logical_size_table
        ))
        .execute(&self.pool)
        .await?;

        let version_table = self.table_name("version");
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                version VARCHAR(255) NOT NULL
            )",
            version_table
        ))
        .execute(&self.pool)
        .await?;

        // Create index on ref_file(bucket, hash) for efficient hash lookups by cleaner
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_hash ON {}(bucket, hash)",
            ref_file_table.replace('.', "_"),
            ref_file_table
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        let table = self.table_name("refcount");
        let query = format!(
            "SELECT refcount FROM {} WHERE bucket = $1 AND hash = $2",
            table
        );
        let result: Result<(i32,), sqlx::Error> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(hash)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok((refcount,)) => Ok(refcount),
            Err(_) => Ok(0),
        }
    }

    async fn atomic_increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        let table = self.table_name("refcount");
        // PostgreSQL: atomic increment using INSERT...ON CONFLICT...DO UPDATE...RETURNING
        // Must qualify refcount column with table name to avoid ambiguity
        let query = format!(
            "INSERT INTO {table} (bucket, hash, refcount) VALUES ($1, $2, 1)
             ON CONFLICT (bucket, hash) DO UPDATE SET refcount = {table}.refcount + 1
             RETURNING refcount",
            table = table
        );
        let (count,): (i32,) = sqlx::query_as(&query)
            .bind(bucket)
            .bind(hash)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn atomic_decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32> {
        let table = self.table_name("refcount");
        // PostgreSQL: atomic decrement using UPDATE...RETURNING with GREATEST to prevent negative
        let query = format!(
            "UPDATE {} SET refcount = GREATEST(0, refcount - 1)
             WHERE bucket = $1 AND hash = $2
             RETURNING refcount",
            table
        );
        let result = sqlx::query_as::<_, (i32,)>(&query)
            .bind(bucket)
            .bind(hash)
            .fetch_optional(&self.pool)
            .await;

        match result {
            Ok(Some((count,))) => Ok(count),
            Ok(None) => {
                // Row not found, return 0
                Ok(0)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64> {
        let table = self.table_name("modified");
        let query = format!(
            "SELECT modified FROM {} WHERE bucket = $1 AND path = $2",
            table
        );
        let result: Result<(i64,), sqlx::Error> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(path)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok((modified,)) => Ok(modified),
            Err(_) => Ok(0),
        }
    }

    async fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<()> {
        let table = self.table_name("modified");
        let query = format!(
            "INSERT INTO {} (bucket, path, modified) VALUES ($1, $2, $3)
            ON CONFLICT (bucket, path) DO UPDATE SET modified = $3",
            table
        );
        sqlx::query(&query)
            .bind(bucket)
            .bind(path)
            .bind(modified)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<()> {
        let table = self.table_name("modified");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND path = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String> {
        let table = self.table_name("ref_file");
        let query = format!("SELECT hash FROM {} WHERE bucket = $1 AND path = $2", table);
        let result: Result<(String,), sqlx::Error> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(path)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok((hash,)) => Ok(hash),
            Err(_) => Ok("".to_string()),
        }
    }

    async fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<()> {
        let table = self.table_name("ref_file");
        let query = format!(
            "INSERT INTO {} (bucket, path, hash) VALUES ($1, $2, $3)
            ON CONFLICT (bucket, path) DO UPDATE SET hash = $3",
            table
        );
        sqlx::query(&query)
            .bind(bucket)
            .bind(path)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<()> {
        let table = self.table_name("ref_file");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND path = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_logical_size(&mut self, bucket: &str, hash: &str) -> Result<usize> {
        let table = self.table_name("logical_size");
        let query = format!(
            "SELECT logical_size FROM {} WHERE bucket = $1 AND hash = $2",
            table
        );
        let result: Result<(i64,), sqlx::Error> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(hash)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok((size,)) => Ok(size as usize),
            Err(_) => Ok(0), // Default to 0 if not found
        }
    }

    async fn set_logical_size(&mut self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        let table = self.table_name("logical_size");
        let query = format!(
            "INSERT INTO {} (bucket, hash, logical_size, compressed_size) VALUES ($1, $2, $3, 0) ON CONFLICT (bucket, hash) DO UPDATE SET logical_size = $3",
            table
        );
        sqlx::query(&query)
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
    ) -> Result<Vec<String>> {
        let table = self.table_name("modified");
        let pattern = format!("{}%", path_prefix);
        let query = format!(
            "SELECT path FROM {} WHERE bucket = $1 AND path LIKE $2 AND modified <= $3 ORDER BY path",
            table
        );
        let rows: Vec<(String,)> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(pattern)
            .bind(timestamp)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|(path,)| path).collect())
    }

    async fn list_ref_files_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>> {
        let table = self.table_name("ref_file");
        let query = format!(
            "SELECT path, hash FROM {} WHERE bucket = $1 ORDER BY path LIMIT $2 OFFSET $3",
            table
        );
        let rows: Vec<(String, String)> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn list_refcounts_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, i32)>> {
        let table = self.table_name("refcount");
        let query = format!(
            "SELECT hash, refcount FROM {} WHERE bucket = $1 ORDER BY hash LIMIT $2 OFFSET $3",
            table
        );
        let rows: Vec<(String, i32)> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn list_logical_sizes_batch(
        &mut self,
        bucket: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>> {
        let table = self.table_name("logical_size");
        let query = format!(
            "SELECT hash FROM {} WHERE bucket = $1 ORDER BY hash LIMIT $2 OFFSET $3",
            table
        );
        let rows: Vec<(String,)> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|(hash,)| hash).collect())
    }

    async fn delete_refcount(&mut self, bucket: &str, hash: &str) -> Result<()> {
        let table = self.table_name("refcount");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND hash = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_logical_size(&mut self, bucket: &str, hash: &str) -> Result<()> {
        let table = self.table_name("logical_size");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND hash = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn hash_is_referenced(&mut self, bucket: &str, hash: &str) -> Result<bool> {
        let table = self.table_name("ref_file");
        let query = format!(
            "SELECT 1 FROM {} WHERE bucket = $1 AND hash = $2 LIMIT 1",
            table
        );
        let result: Option<(i32,)> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(hash)
            .fetch_optional(&self.pool)
            .await?;
        Ok(result.is_some())
    }

    async fn get_compressed_size(&mut self, bucket: &str, hash: &str) -> Result<usize> {
        let table = self.table_name("logical_size");
        let query = format!(
            "SELECT compressed_size FROM {} WHERE bucket = $1 AND hash = $2",
            table
        );
        let result: Result<(Option<i64>,), sqlx::Error> = sqlx::query_as(&query)
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

    async fn set_compressed_size(&mut self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        let table = self.table_name("logical_size");
        let query = format!(
            "INSERT INTO {} (bucket, hash, logical_size, compressed_size) VALUES ($1, $2, 0, $3)
             ON CONFLICT (bucket, hash) DO UPDATE SET compressed_size = $3",
            table
        );
        sqlx::query(&query)
            .bind(bucket)
            .bind(hash)
            .bind(size as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_total_files(&mut self, bucket: &str) -> Result<i64> {
        let table = self.table_name("modified");
        let query = format!("SELECT COUNT(*) FROM {} WHERE bucket = $1", table);
        let (count,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn get_total_blobs(&mut self, bucket: &str) -> Result<i64> {
        let table = self.table_name("refcount");
        let query = format!(
            "SELECT COUNT(*) FROM {} WHERE bucket = $1 AND refcount > 0",
            table
        );
        let (count,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn get_total_storage_bytes(&mut self, bucket: &str) -> Result<i64> {
        // Only count storage for blobs that are actually referenced (refcount > 0)
        let refcount_table = self.table_name("refcount");
        let logical_size_table = self.table_name("logical_size");
        let query = format!(
            "SELECT COALESCE(SUM(l.compressed_size), 0)::BIGINT
             FROM {} l
             INNER JOIN {} r ON l.bucket = r.bucket AND l.hash = r.hash
             WHERE l.bucket = $1 AND r.refcount > 0",
            logical_size_table, refcount_table
        );
        let (total,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(total)
    }

    async fn get_total_logical_bytes(&mut self, bucket: &str) -> Result<i64> {
        let refcount_table = self.table_name("refcount");
        let logical_size_table = self.table_name("logical_size");
        let query = format!(
            "SELECT COALESCE(SUM(r.refcount * l.logical_size), 0)::BIGINT
             FROM {} r
             INNER JOIN {} l ON r.bucket = l.bucket AND r.hash = l.hash
             WHERE r.bucket = $1",
            refcount_table, logical_size_table
        );
        let (total,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(total)
    }

    async fn get_total_compressed_bytes_no_dedup(&mut self, bucket: &str) -> Result<i64> {
        let refcount_table = self.table_name("refcount");
        let logical_size_table = self.table_name("logical_size");
        let query = format!(
            "SELECT COALESCE(SUM(r.refcount * l.compressed_size), 0)::BIGINT
             FROM {} r
             INNER JOIN {} l ON r.bucket = l.bucket AND r.hash = l.hash
             WHERE r.bucket = $1 AND r.refcount > 0",
            refcount_table, logical_size_table
        );
        let (total,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(total)
    }

    async fn get_deduplicated_bytes_saved(&mut self, bucket: &str) -> Result<i64> {
        let refcount_table = self.table_name("refcount");
        let logical_size_table = self.table_name("logical_size");
        let query = format!(
            "SELECT COALESCE(SUM((r.refcount - 1) * l.logical_size), 0)::BIGINT
             FROM {} r
             INNER JOIN {} l ON r.bucket = l.bucket AND r.hash = l.hash
             WHERE r.bucket = $1 AND r.refcount > 1",
            refcount_table, logical_size_table
        );
        let (total,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(total)
    }

    fn get_pool_stats(&self) -> (u32, u32) {
        let total_connections = self.pool.size();
        let idle_connections = self.pool.num_idle() as u32;
        let active_connections = total_connections.saturating_sub(idle_connections);
        (active_connections, idle_connections)
    }

    async fn get_version(&mut self) -> Result<Option<String>> {
        let table = self.table_name("version");
        let query = format!("SELECT version FROM {} WHERE id = 1", table);
        let result: Option<(String,)> = sqlx::query_as(&query).fetch_optional(&self.pool).await?;
        Ok(result.map(|r| r.0))
    }

    async fn set_version(&mut self, version: &str) -> Result<()> {
        let table = self.table_name("version");
        // Use upsert to ensure only one row exists
        let query = format!(
            "INSERT INTO {} (id, version) VALUES (1, $1)
             ON CONFLICT (id) DO UPDATE SET version = $1",
            table
        );
        sqlx::query(&query)
            .bind(version)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
