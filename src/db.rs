use anyhow::{Context, Result};
use rand::Rng;
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

impl PostgresConfig {
    /// Build a connection URL from this config.
    pub fn db_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?connect_timeout=10",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }
}

/// Create a PostgreSQL connection pool with standard settings and validate it.
pub async fn create_pg_pool(config: &PostgresConfig, label: &str) -> Result<PgPool> {
    let db_url = config.db_url();

    debug!(
        "Connecting to Postgres for {}: postgres://{}:****@{}:{}/{}",
        label, config.user, config.host, config.port, config.dbname
    );

    let pool = PgPoolOptions::new()
        .max_connections(config.pool_size)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Some(Duration::from_secs(600)))
        .max_lifetime(Some(Duration::from_secs(1800)))
        .connect(&db_url)
        .await
        .context(format!("Failed to connect to PostgreSQL for {}", label))?;

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .context(format!("PostgreSQL {} pool validation failed", label))?;

    debug!("Successfully validated PostgreSQL {} pool", label);

    Ok(pool)
}

/// Apply exponential backoff with ±25% jitter to the current delay, capped at `max_ms`.
pub fn next_backoff(current_ms: u64, max_ms: u64) -> u64 {
    let doubled = std::cmp::min(current_ms * 2, max_ms);
    let jitter = (doubled as f64 * 0.25 * (rand::rng().random::<f64>() - 0.5)) as i64;
    (doubled as i64 + jitter).max(1) as u64
}
