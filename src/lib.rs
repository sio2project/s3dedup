use anyhow::Result;
use serde::Serialize;
use std::sync::Arc;

use tokio::sync::Mutex;

pub mod cleaner;
pub mod config;
pub mod filetracker_client;
pub mod kvstorage;
pub mod locks;
pub mod logging;
pub mod metrics;
pub mod migration;
pub mod routes;
pub mod s3storage;

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub uptime_seconds: i64,
    pub checks: HealthChecks,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthChecks {
    pub database: String,
    pub s3: String,
}

pub struct AppState {
    pub bucket_name: String,
    pub kvstorage: Arc<Mutex<Box<kvstorage::KVStorage>>>,
    pub locks: Arc<locks::LocksStorage>,
    pub s3storage: Arc<Mutex<Box<s3storage::S3Storage>>>,
    pub filetracker_client: Option<Arc<filetracker_client::FiletrackerClient>>,
    pub metrics: Arc<metrics::Metrics>,
}

impl AppState {
    pub async fn new(config: &config::Config) -> Result<Arc<Self>> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new_with_config(config.locks_type, config).await?;
        let s3storage = s3storage::S3Storage::new(&config.bucket).await?;
        let metrics = Arc::new(metrics::Metrics::new());
        Ok(Arc::new(Self {
            bucket_name: config.bucket.name.clone(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks: Arc::new(*locks),
            s3storage: Arc::new(Mutex::new(s3storage)),
            filetracker_client: None,
            metrics,
        }))
    }

    pub async fn new_with_filetracker(
        config: &config::Config,
        filetracker_url: String,
    ) -> Result<Arc<Self>> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new_with_config(config.locks_type, config).await?;
        let s3storage = s3storage::S3Storage::new(&config.bucket).await?;
        let filetracker_client = filetracker_client::FiletrackerClient::new(filetracker_url);
        let metrics = Arc::new(metrics::Metrics::new());

        // Mark migration as active
        metrics::MIGRATION_ACTIVE.set(1);

        Ok(Arc::new(Self {
            bucket_name: config.bucket.name.clone(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks: Arc::new(*locks),
            s3storage: Arc::new(Mutex::new(s3storage)),
            filetracker_client: Some(Arc::new(filetracker_client)),
            metrics,
        }))
    }

    /// Update storage gauge metrics from database
    pub async fn update_storage_metrics(&self) -> Result<()> {
        let mut kv = self.kvstorage.lock().await;

        // Update database connection pool metrics
        let (active_conns, idle_conns) = kv.get_pool_stats();
        metrics::DB_CONNECTIONS_ACTIVE.set(active_conns as i64);
        metrics::DB_CONNECTIONS_IDLE.set(idle_conns as i64);

        // Get all stats
        let total_files = kv.get_total_files(&self.bucket_name).await?;
        let total_blobs = kv.get_total_blobs(&self.bucket_name).await?;
        let total_storage_bytes = kv.get_total_storage_bytes(&self.bucket_name).await?;
        let total_logical_bytes = kv.get_total_logical_bytes(&self.bucket_name).await?;
        let deduplicated_bytes_saved = kv.get_deduplicated_bytes_saved(&self.bucket_name).await?;

        // Update gauges
        metrics::TOTAL_FILES.set(total_files);
        metrics::TOTAL_BLOBS.set(total_blobs);
        metrics::TOTAL_STORAGE_BYTES.set(total_storage_bytes);
        metrics::TOTAL_LOGICAL_SIZE_BYTES.set(total_logical_bytes);
        metrics::DEDUPLICATED_BYTES_SAVED.set(deduplicated_bytes_saved);

        // Calculate derived metrics
        if total_files > 0 && total_blobs > 0 {
            // Deduplication ratio: percentage of files that are deduplicated
            // 0.0 (0%) = no dedup (all files unique), 0.5 (50%) = half the files share blobs
            let dedup_ratio = (total_files - total_blobs) as f64 / total_files as f64;
            metrics::DEDUPLICATION_RATIO.set(dedup_ratio);
            // Average reference count: how many files point to each blob on average
            metrics::AVERAGE_REFERENCE_COUNT.set(total_files as f64 / total_blobs as f64);
        }

        if total_logical_bytes > 0 {
            // Clamp to 0 minimum - negative "savings" can happen when compression
            // overhead exceeds gains (tiny files), but we report 0% not negative
            let savings_ratio = ((total_logical_bytes - total_storage_bytes) as f64
                / total_logical_bytes as f64)
                .max(0.0);
            metrics::STORAGE_SAVINGS_RATIO.set(savings_ratio);
        }

        Ok(())
    }

    /// Check health of database and S3 connectivity
    pub async fn check_health(&self) -> HealthStatus {
        let uptime_seconds = self.metrics.start_time.elapsed().as_secs() as i64;

        // Check database connectivity
        let db_status = match self
            .kvstorage
            .lock()
            .await
            .get_total_files(&self.bucket_name)
            .await
        {
            Ok(_) => "ok".to_string(),
            Err(e) => {
                tracing::error!("Database health check failed: {}", e);
                "error".to_string()
            }
        };

        // Check S3 connectivity
        let s3_status = match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.s3storage.lock().await.check_health(),
        )
        .await
        {
            Ok(Ok(_)) => "ok".to_string(),
            Ok(Err(e)) => {
                tracing::error!("S3 health check failed: {}", e);
                "error".to_string()
            }
            Err(_) => {
                tracing::error!("S3 health check timed out");
                "timeout".to_string()
            }
        };

        // Determine overall status
        let status = if db_status == "ok" && s3_status == "ok" {
            "ok".to_string()
        } else if db_status == "error" || s3_status == "error" {
            "error".to_string()
        } else {
            "degraded".to_string()
        };

        HealthStatus {
            status,
            uptime_seconds,
            checks: HealthChecks {
                database: db_status,
                s3: s3_status,
            },
        }
    }
}
