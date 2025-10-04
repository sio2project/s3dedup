use anyhow::Result;
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

pub struct AppState {
    pub bucket_name: String,
    pub kvstorage: Arc<Mutex<Box<kvstorage::KVStorage>>>,
    pub locks: Box<locks::LocksStorage>,
    pub s3storage: Arc<Mutex<Box<s3storage::S3Storage>>>,
    pub filetracker_client: Option<Arc<filetracker_client::FiletrackerClient>>,
    pub metrics: Arc<metrics::Metrics>,
}

impl AppState {
    pub async fn new(config: &config::BucketConfig) -> Result<Arc<Self>> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new(config.locks_type);
        let s3storage = s3storage::S3Storage::new(config).await?;
        let metrics = Arc::new(metrics::Metrics::new());
        Ok(Arc::new(Self {
            bucket_name: config.name.clone(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks,
            s3storage: Arc::new(Mutex::new(s3storage)),
            filetracker_client: None,
            metrics,
        }))
    }

    pub async fn new_with_filetracker(
        config: &config::BucketConfig,
        filetracker_url: String,
    ) -> Result<Arc<Self>> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new(config.locks_type);
        let s3storage = s3storage::S3Storage::new(config).await?;
        let filetracker_client = filetracker_client::FiletrackerClient::new(filetracker_url);
        let metrics = Arc::new(metrics::Metrics::new());

        // Mark migration as active
        metrics::MIGRATION_ACTIVE.set(1);

        Ok(Arc::new(Self {
            bucket_name: config.name.clone(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks,
            s3storage: Arc::new(Mutex::new(s3storage)),
            filetracker_client: Some(Arc::new(filetracker_client)),
            metrics,
        }))
    }

    /// Update storage gauge metrics from database
    pub async fn update_storage_metrics(&self) -> Result<()> {
        let mut kv = self.kvstorage.lock().await;

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
            let savings_ratio =
                (total_logical_bytes - total_storage_bytes) as f64 / total_logical_bytes as f64;
            metrics::STORAGE_SAVINGS_RATIO.set(savings_ratio);
        }

        Ok(())
    }
}
