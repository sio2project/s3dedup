use anyhow::{Context, Result};
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, warn};

pub mod cleaner;
pub mod config;
pub mod db;
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
    pub kvstorage: Arc<kvstorage::KVStorage>,
    pub locks: Arc<locks::LocksStorage>,
    pub s3storage: Arc<s3storage::S3Storage>,
    pub filetracker_client: Option<Arc<filetracker_client::FiletrackerClient>>,
    pub metrics: Arc<metrics::Metrics>,
    /// Max file size to process in memory during PUT slow path. Larger files use temp files.
    pub max_inmemory_size: usize,
}

impl AppState {
    pub async fn new(config: &config::Config) -> Result<Arc<Self>> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new_with_config(config.locks_type, config).await?;
        let s3storage = s3storage::S3Storage::new(&config.bucket).await?;
        let metrics = Arc::new(metrics::Metrics::new());
        Ok(Arc::new(Self {
            bucket_name: config.bucket.name.clone(),
            kvstorage: Arc::new(*kvstorage),
            locks: Arc::new(*locks),
            s3storage: Arc::new(*s3storage),
            filetracker_client: None,
            metrics,
            max_inmemory_size: config.bucket.max_inmemory_size,
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
            kvstorage: Arc::new(*kvstorage),
            locks: Arc::new(*locks),
            s3storage: Arc::new(*s3storage),
            filetracker_client: Some(Arc::new(filetracker_client)),
            metrics,
            max_inmemory_size: config.bucket.max_inmemory_size,
        }))
    }

    /// Update storage gauge metrics from database
    pub async fn update_storage_metrics(&self) -> Result<()> {
        // Update database connection pool metrics
        let (active_conns, idle_conns) = self.kvstorage.get_pool_stats();
        metrics::DB_CONNECTIONS_ACTIVE.set(active_conns as i64);
        metrics::DB_CONNECTIONS_IDLE.set(idle_conns as i64);

        // Get all stats in a single combined query (avoids 4 separate expensive JOINs)
        let stats = self.kvstorage.get_storage_stats(&self.bucket_name).await?;

        // Update gauges
        metrics::TOTAL_FILES.set(stats.total_files);
        metrics::TOTAL_BLOBS.set(stats.total_blobs);
        metrics::TOTAL_STORAGE_BYTES.set(stats.total_storage_bytes);
        metrics::TOTAL_LOGICAL_SIZE_BYTES.set(stats.total_logical_bytes);
        metrics::DEDUPLICATED_BYTES_SAVED.set(stats.deduplicated_bytes_saved);
        metrics::TOTAL_COMPRESSED_BYTES_NO_DEDUP.set(stats.total_compressed_bytes_no_dedup);

        // Calculate derived metrics
        if stats.total_files > 0 && stats.total_blobs > 0 {
            let dedup_ratio =
                (stats.total_files - stats.total_blobs) as f64 / stats.total_files as f64;
            metrics::DEDUPLICATION_RATIO.set(dedup_ratio);
            metrics::AVERAGE_REFERENCE_COUNT
                .set(stats.total_files as f64 / stats.total_blobs as f64);
        }

        if stats.total_logical_bytes > 0 {
            // Clamp to 0 minimum - negative "savings" can happen when compression
            // overhead exceeds gains (tiny files), but we report 0% not negative
            let savings_ratio = ((stats.total_logical_bytes - stats.total_storage_bytes) as f64
                / stats.total_logical_bytes as f64)
                .max(0.0);
            metrics::STORAGE_SAVINGS_RATIO.set(savings_ratio);
        }

        Ok(())
    }

    // --- Shared helpers for PUT and migration ---

    /// Set ref_file and modified timestamp for a path.
    pub async fn update_file_ref(&self, path: &str, digest: &str, timestamp: i64) -> Result<()> {
        self.kvstorage
            .set_ref_file(&self.bucket_name, path, digest)
            .await?;
        self.kvstorage
            .set_modified(&self.bucket_name, path, timestamp)
            .await?;
        Ok(())
    }

    /// Record blob metadata: logical size, optionally compressed size, and conditionally increment refcount.
    /// If `old_hash` is Some and equals `digest`, refcount is NOT incremented (same content).
    /// Pass `None` for `old_hash` to always increment (migration path).
    /// Pass `None` for `compressed_size` to skip updating it (e.g. dedup hit where body wasn't read).
    pub async fn record_blob_metadata(
        &self,
        digest: &str,
        logical_size: usize,
        compressed_size: Option<usize>,
        old_hash: Option<&str>,
    ) -> Result<()> {
        self.kvstorage
            .set_logical_size(&self.bucket_name, digest, logical_size)
            .await?;
        if let Some(cs) = compressed_size {
            self.kvstorage
                .set_compressed_size(&self.bucket_name, digest, cs)
                .await?;
        }

        let same_content = matches!(old_hash, Some(old) if !old.is_empty() && old == digest);
        if !same_content {
            self.kvstorage
                .atomic_increment_ref_count(&self.bucket_name, digest)
                .await?;
        }
        Ok(())
    }

    /// Decrement the refcount for `old_hash` and delete the S3 blob if it reaches 0.
    /// Acquires hash lock internally. No-op if `old_hash` is empty or equals `new_digest`.
    pub async fn decrement_old_ref(&self, old_hash: &str, new_digest: &str) -> Result<()> {
        if old_hash.is_empty() || old_hash == new_digest {
            return Ok(());
        }

        let hash_lock_key = locks::hash_lock(&self.bucket_name, old_hash);
        let lock = self.locks.prepare_lock(hash_lock_key).await;
        let guard = lock
            .acquire_exclusive()
            .await
            .context("Failed to acquire old hash lock for decrement")?;

        let ref_count = self
            .kvstorage
            .atomic_decrement_ref_count(&self.bucket_name, old_hash)
            .await?;

        if ref_count <= 0 {
            debug!("Deleting unused blob: {}", old_hash);
            let _ = self.s3storage.delete_object(old_hash).await;
        }

        if let Err(e) = guard.release().await {
            warn!("Failed to release old hash lock: {}", e);
        }
        Ok(())
    }

    /// Check health of database and S3 connectivity
    pub async fn check_health(&self) -> HealthStatus {
        let uptime_seconds = self.metrics.start_time.elapsed().as_secs() as i64;

        // Check database connectivity
        let db_status = match self.kvstorage.get_total_files(&self.bucket_name).await {
            Ok(_) => "ok".to_string(),
            Err(e) => {
                tracing::error!("Database health check failed: {}", e);
                "error".to_string()
            }
        };

        // Check S3 connectivity
        let s3_status = match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.s3storage.check_health(),
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
