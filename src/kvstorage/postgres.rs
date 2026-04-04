use crate::config::Config;
use crate::db;
use crate::kvstorage::KVStorageTrait;
use anyhow::Result;
use sqlx::PgPool;

pub use crate::db::PostgresConfig;

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
        let pool = db::create_pg_pool(pg_config, "kvstorage").await?;

        Ok(Box::new(Postgres {
            pool,
            bucket: config.bucket.name.clone(),
        }))
    }
    async fn setup(&self) -> Result<()> {
        // PostgreSQL doesn't support multiple statements in a single query
        // Create tables with bucket-specific names to allow parallel tests
        let refcount_table = self.table_name("refcount");
        let modified_table = self.table_name("modified");
        let ref_file_table = self.table_name("ref_file");
        let logical_size_table = self.table_name("logical_size");

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket TEXT NOT NULL,
                hash TEXT NOT NULL,
                refcount INT NOT NULL,
                PRIMARY KEY (bucket, hash)
            )",
            refcount_table
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket TEXT NOT NULL,
                path TEXT NOT NULL,
                modified BIGINT NOT NULL,
                PRIMARY KEY (bucket, path)
            )",
            modified_table
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket TEXT NOT NULL,
                path TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (bucket, path)
            )",
            ref_file_table
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket TEXT NOT NULL,
                hash TEXT NOT NULL,
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
                version TEXT NOT NULL
            )",
            version_table
        ))
        .execute(&self.pool)
        .await?;

        let stats_table = self.table_name("stats");
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                bucket TEXT PRIMARY KEY,
                total_files BIGINT NOT NULL DEFAULT 0,
                total_blobs BIGINT NOT NULL DEFAULT 0,
                total_storage_bytes BIGINT NOT NULL DEFAULT 0,
                total_logical_bytes BIGINT NOT NULL DEFAULT 0,
                deduplicated_bytes_saved BIGINT NOT NULL DEFAULT 0,
                total_compressed_bytes_no_dedup BIGINT NOT NULL DEFAULT 0
            )",
            stats_table
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

        // Migrate existing VARCHAR(255) columns to TEXT (one-time, skipped if already TEXT)
        let needs_migration: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_name = $1 AND data_type = 'character varying'
            )",
        )
        .bind(&refcount_table)
        .fetch_one(&self.pool)
        .await?;

        if needs_migration {
            for (table, columns) in [
                (&refcount_table, vec!["bucket", "hash"]),
                (&modified_table, vec!["bucket", "path"]),
                (&ref_file_table, vec!["bucket", "path", "hash"]),
                (&logical_size_table, vec!["bucket", "hash"]),
                (&version_table, vec!["version"]),
            ] {
                for col in columns {
                    sqlx::query(&format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE TEXT",
                        table, col
                    ))
                    .execute(&self.pool)
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn get_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
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

    async fn atomic_increment_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
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

    async fn atomic_decrement_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
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

    async fn get_modified(&self, bucket: &str, path: &str) -> Result<i64> {
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

    async fn set_modified(&self, bucket: &str, path: &str, modified: i64) -> Result<()> {
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

    async fn delete_modified(&self, bucket: &str, path: &str) -> Result<()> {
        let table = self.table_name("modified");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND path = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_ref_file(&self, bucket: &str, path: &str) -> Result<String> {
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

    async fn set_ref_file(&self, bucket: &str, path: &str, hash: &str) -> Result<()> {
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

    async fn delete_ref_file(&self, bucket: &str, path: &str) -> Result<()> {
        let table = self.table_name("ref_file");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND path = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_logical_size(&self, bucket: &str, hash: &str) -> Result<usize> {
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

    async fn set_logical_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
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
        &self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>> {
        let table = self.table_name("modified");
        let escaped = path_prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{}%", escaped);
        let query = format!(
            "SELECT path FROM {} WHERE bucket = $1 AND path LIKE $2 ESCAPE '\\' AND modified <= $3 ORDER BY path",
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

    async fn list_orphaned_ref_files(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let ref_file_table = self.table_name("ref_file");
        let refcount_table = self.table_name("refcount");
        let query = format!(
            "SELECT rf.path, rf.hash FROM {rf} rf
             LEFT JOIN {rc} rc ON rf.bucket = rc.bucket AND rf.hash = rc.hash
             WHERE rf.bucket = $1 AND (rc.refcount IS NULL OR rc.refcount = 0)
               AND rf.path > $2
             ORDER BY rf.path LIMIT $3",
            rf = ref_file_table,
            rc = refcount_table
        );
        let rows: Vec<(String, String)> = sqlx::query_as(&query)
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
        let refcount_table = self.table_name("refcount");
        let ref_file_table = self.table_name("ref_file");
        let query = format!(
            "SELECT rc.hash, rc.refcount FROM {rc} rc
             LEFT JOIN {rf} rf ON rc.bucket = rf.bucket AND rc.hash = rf.hash
             WHERE rc.bucket = $1 AND rf.hash IS NULL
               AND rc.hash > $2
             ORDER BY rc.hash LIMIT $3",
            rc = refcount_table,
            rf = ref_file_table
        );
        let rows: Vec<(String, i32)> = sqlx::query_as(&query)
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
        let logical_size_table = self.table_name("logical_size");
        let refcount_table = self.table_name("refcount");
        let query = format!(
            "SELECT ls.hash FROM {ls} ls
             LEFT JOIN {rc} rc ON ls.bucket = rc.bucket AND ls.hash = rc.hash
             WHERE ls.bucket = $1 AND (rc.refcount IS NULL OR rc.refcount = 0)
               AND ls.hash > $2
             ORDER BY ls.hash LIMIT $3",
            ls = logical_size_table,
            rc = refcount_table
        );
        let rows: Vec<(String,)> = sqlx::query_as(&query)
            .bind(bucket)
            .bind(after_cursor)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(hash,)| hash).collect())
    }

    async fn delete_refcount(&self, bucket: &str, hash: &str) -> Result<()> {
        let table = self.table_name("refcount");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND hash = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_logical_size(&self, bucket: &str, hash: &str) -> Result<()> {
        let table = self.table_name("logical_size");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND hash = $2", table);
        sqlx::query(&query)
            .bind(bucket)
            .bind(hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn hash_is_referenced(&self, bucket: &str, hash: &str) -> Result<bool> {
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

    async fn get_compressed_size(&self, bucket: &str, hash: &str) -> Result<usize> {
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

    async fn set_compressed_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
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

    async fn get_total_files(&self, bucket: &str) -> Result<i64> {
        let table = self.table_name("modified");
        let query = format!("SELECT COUNT(*) FROM {} WHERE bucket = $1", table);
        let (count,): (i64,) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    async fn get_total_blobs(&self, bucket: &str) -> Result<i64> {
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

    async fn get_total_storage_bytes(&self, bucket: &str) -> Result<i64> {
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

    async fn get_total_logical_bytes(&self, bucket: &str) -> Result<i64> {
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

    async fn get_total_compressed_bytes_no_dedup(&self, bucket: &str) -> Result<i64> {
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

    async fn get_deduplicated_bytes_saved(&self, bucket: &str) -> Result<i64> {
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

    async fn get_storage_stats(&self, bucket: &str) -> Result<crate::kvstorage::StorageStats> {
        let stats_table = self.table_name("stats");
        let query = format!(
            "SELECT total_files, total_blobs, total_storage_bytes, total_logical_bytes,
                    deduplicated_bytes_saved, total_compressed_bytes_no_dedup
             FROM {} WHERE bucket = $1",
            stats_table
        );
        let result: Option<(i64, i64, i64, i64, i64, i64)> = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        match result {
            Some(row) => Ok(crate::kvstorage::StorageStats {
                total_files: row.0,
                total_blobs: row.1,
                total_storage_bytes: row.2,
                total_logical_bytes: row.3,
                deduplicated_bytes_saved: row.4,
                total_compressed_bytes_no_dedup: row.5,
            }),
            None => Ok(crate::kvstorage::StorageStats::default()),
        }
    }

    async fn adjust_stats(&self, bucket: &str, delta: &crate::kvstorage::StatsDelta) -> Result<()> {
        let stats_table = self.table_name("stats");
        let query = format!(
            "INSERT INTO {t} (bucket, total_files, total_blobs, total_storage_bytes,
                total_logical_bytes, deduplicated_bytes_saved, total_compressed_bytes_no_dedup)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (bucket) DO UPDATE SET
                total_files = {t}.total_files + EXCLUDED.total_files,
                total_blobs = {t}.total_blobs + EXCLUDED.total_blobs,
                total_storage_bytes = {t}.total_storage_bytes + EXCLUDED.total_storage_bytes,
                total_logical_bytes = {t}.total_logical_bytes + EXCLUDED.total_logical_bytes,
                deduplicated_bytes_saved = {t}.deduplicated_bytes_saved + EXCLUDED.deduplicated_bytes_saved,
                total_compressed_bytes_no_dedup = {t}.total_compressed_bytes_no_dedup + EXCLUDED.total_compressed_bytes_no_dedup",
            t = stats_table
        );
        sqlx::query(&query)
            .bind(bucket)
            .bind(delta.total_files)
            .bind(delta.total_blobs)
            .bind(delta.total_storage_bytes)
            .bind(delta.total_logical_bytes)
            .bind(delta.deduplicated_bytes_saved)
            .bind(delta.total_compressed_bytes_no_dedup)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn recompute_stats(&self, bucket: &str) -> Result<crate::kvstorage::StorageStats> {
        let modified_table = self.table_name("modified");
        let refcount_table = self.table_name("refcount");
        let logical_size_table = self.table_name("logical_size");
        let stats_table = self.table_name("stats");

        // Expensive full-scan query
        let query = format!(
            "SELECT
                (SELECT COUNT(*) FROM {modified} WHERE bucket = $1),
                COALESCE(agg.total_blobs, 0),
                COALESCE(agg.total_storage_bytes, 0),
                COALESCE(agg.total_logical_bytes, 0),
                COALESCE(agg.deduplicated_bytes_saved, 0),
                COALESCE(agg.total_compressed_bytes_no_dedup, 0)
             FROM (
                SELECT
                    COUNT(*)::BIGINT AS total_blobs,
                    SUM(l.compressed_size)::BIGINT AS total_storage_bytes,
                    SUM(r.refcount * l.logical_size)::BIGINT AS total_logical_bytes,
                    SUM(CASE WHEN r.refcount > 1 THEN (r.refcount - 1) * l.logical_size ELSE 0 END)::BIGINT AS deduplicated_bytes_saved,
                    SUM(r.refcount * l.compressed_size)::BIGINT AS total_compressed_bytes_no_dedup
                FROM {refcount} r
                INNER JOIN {logical_size} l ON r.bucket = l.bucket AND r.hash = l.hash
                WHERE r.bucket = $1 AND r.refcount > 0
             ) agg",
            modified = modified_table,
            refcount = refcount_table,
            logical_size = logical_size_table,
        );
        let row: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(&query)
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;

        let stats = crate::kvstorage::StorageStats {
            total_files: row.0,
            total_blobs: row.1,
            total_storage_bytes: row.2,
            total_logical_bytes: row.3,
            deduplicated_bytes_saved: row.4,
            total_compressed_bytes_no_dedup: row.5,
        };

        // Store in stats cache table
        let upsert = format!(
            "INSERT INTO {t} (bucket, total_files, total_blobs, total_storage_bytes,
                total_logical_bytes, deduplicated_bytes_saved, total_compressed_bytes_no_dedup)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (bucket) DO UPDATE SET
                total_files = EXCLUDED.total_files,
                total_blobs = EXCLUDED.total_blobs,
                total_storage_bytes = EXCLUDED.total_storage_bytes,
                total_logical_bytes = EXCLUDED.total_logical_bytes,
                deduplicated_bytes_saved = EXCLUDED.deduplicated_bytes_saved,
                total_compressed_bytes_no_dedup = EXCLUDED.total_compressed_bytes_no_dedup",
            t = stats_table
        );
        sqlx::query(&upsert)
            .bind(bucket)
            .bind(stats.total_files)
            .bind(stats.total_blobs)
            .bind(stats.total_storage_bytes)
            .bind(stats.total_logical_bytes)
            .bind(stats.deduplicated_bytes_saved)
            .bind(stats.total_compressed_bytes_no_dedup)
            .execute(&self.pool)
            .await?;

        Ok(stats)
    }

    fn get_pool_stats(&self) -> (u32, u32) {
        let total_connections = self.pool.size();
        let idle_connections = self.pool.num_idle() as u32;
        let active_connections = total_connections.saturating_sub(idle_connections);
        (active_connections, idle_connections)
    }

    async fn get_version(&self) -> Result<Option<String>> {
        let table = self.table_name("version");
        let query = format!("SELECT version FROM {} WHERE id = 1", table);
        let result: Option<(String,)> = sqlx::query_as(&query).fetch_optional(&self.pool).await?;
        Ok(result.map(|r| r.0))
    }

    async fn set_version(&self, version: &str) -> Result<()> {
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
