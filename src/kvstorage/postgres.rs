use crate::config::Config;
use crate::kvstorage::KVStorageTrait;
use crate::kvstorage::pooled::{RowModified, RowRefFile, RowRefcount};
use serde::Deserialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::error::Error;

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
}

impl KVStorageTrait for Postgres {
    async fn new(config: &Config) -> Result<Box<Self>, Box<dyn Error>> {
        let pg_config = config.postgres.as_ref().unwrap();
        let db_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.dbname
        );
        let pool = PgPoolOptions::new()
            .max_connections(pg_config.pool_size)
            .connect(&db_url)
            .await?;
        Ok(Box::new(Postgres { pool }))
    }
    async fn setup(&mut self) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS refcount (
                bucket VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                refcount INT NOT NULL,
                PRIMARY KEY (bucket, hash)
            );
            CREATE TABLE IF NOT EXISTS modified (
                bucket VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                modified BIGINT NOT NULL,
                PRIMARY KEY (bucket, path)
            );
            CREATE TABLE IF NOT EXISTS ref_file (
                bucket VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                PRIMARY KEY (bucket, path)
            );",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>> {
        sqlx::query_as("SELECT refcount FROM refcount WHERE bucket = $1 AND hash = $2")
            .bind(bucket)
            .bind(hash)
            .fetch_one(&self.pool)
            .await
            .map(|row: RowRefcount| row.refcount)
            .or_else(|_| Ok(0))
    }

    async fn set_ref_count(
        &mut self,
        bucket: &str,
        hash: &str,
        ref_cnt: i32,
    ) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            "INSERT INTO refcount (bucket, hash, refcount) VALUES ($1, $2, $3)
            ON CONFLICT (bucket, hash) DO UPDATE SET refcount = $3",
        )
        .bind(bucket)
        .bind(hash)
        .bind(ref_cnt)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>> {
        sqlx::query_as("SELECT modified FROM modified WHERE bucket = $1 AND path = $2")
            .bind(bucket)
            .bind(path)
            .fetch_one(&self.pool)
            .await
            .map(|row: RowModified| row.modified)
            .or_else(|_| Ok(0))
    }

    async fn set_modified(
        &mut self,
        bucket: &str,
        path: &str,
        modified: i64,
    ) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            "INSERT INTO modified (bucket, path, modified) VALUES ($1, $2, $3)
            ON CONFLICT (bucket, path) DO UPDATE SET modified = $3",
        )
        .bind(bucket)
        .bind(path)
        .bind(modified)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
        sqlx::query("DELETE FROM modified WHERE bucket = $1 AND path = $2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>> {
        sqlx::query_as("SELECT hash FROM ref_file WHERE bucket = $1 AND path = $2")
            .bind(bucket)
            .bind(path)
            .fetch_one(&self.pool)
            .await
            .map(|row: RowRefFile| row.hash)
            .or_else(|_| Ok("".to_string()))
    }

    async fn set_ref_file(
        &mut self,
        bucket: &str,
        path: &str,
        hash: &str,
    ) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            "INSERT INTO ref_file (bucket, path, hash) VALUES ($1, $2, $3)
            ON CONFLICT (bucket, path) DO UPDATE SET hash = $3",
        )
        .bind(bucket)
        .bind(path)
        .bind(hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
        sqlx::query("DELETE FROM ref_file WHERE bucket = $1 AND path = $2")
            .bind(bucket)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
