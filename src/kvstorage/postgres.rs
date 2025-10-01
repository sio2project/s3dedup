use crate::config::BucketConfig;
use crate::kvstorage::KVStorageTrait;
use serde::Deserialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::error::Error;
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
    async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn Error + Send + Sync>> {
        let pg_config = config.postgres.as_ref().unwrap();
        let db_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.dbname
        );
        debug!("Connecting to Postgres database: {}", db_url);
        let pool = PgPoolOptions::new()
            .max_connections(pg_config.pool_size)
            .connect(&db_url)
            .await?;
        Ok(Box::new(Postgres {
            pool,
            bucket: config.name.clone(),
        }))
    }
    async fn setup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
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
                PRIMARY KEY (bucket, hash)
            )",
            logical_size_table
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
    ) -> Result<i32, Box<dyn Error + Send + Sync>> {
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

    async fn set_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
        ref_cnt: i32,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let table = self.table_name("refcount");
        let query = format!(
            "INSERT INTO {} (bucket, hash, refcount) VALUES ($1, $2, $3)
            ON CONFLICT (bucket, hash) DO UPDATE SET refcount = $3",
            table
        );
        sqlx::query(&query)
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
    ) -> Result<i64, Box<dyn Error + Send + Sync>> {
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

    async fn set_modified(
        &mut self,
        bucket: &str,
        path: &str,
        modified: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

    async fn delete_modified(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let table = self.table_name("modified");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND path = $2", table);
        sqlx::query(&query)
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
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
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

    async fn set_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

    async fn delete_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let table = self.table_name("ref_file");
        let query = format!("DELETE FROM {} WHERE bucket = $1 AND path = $2", table);
        sqlx::query(&query)
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
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
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

    async fn set_logical_size(
        &mut self,
        bucket: &str,
        hash: &str,
        size: usize,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let table = self.table_name("logical_size");
        let query = format!(
            "INSERT INTO {} (bucket, hash, logical_size) VALUES ($1, $2, $3) ON CONFLICT (bucket, hash) DO UPDATE SET logical_size = $3",
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
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
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
}
