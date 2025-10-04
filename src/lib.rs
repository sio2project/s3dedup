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
}
