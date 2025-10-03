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

#[derive(Clone)]
pub struct AppState {
    pub bucket_name: Arc<str>,
    pub kvstorage: Arc<Mutex<Box<kvstorage::KVStorage>>>,
    pub locks: Arc<Mutex<Box<locks::LocksStorage>>>,
    pub s3storage: Arc<Mutex<Box<s3storage::S3Storage>>>,
    pub filetracker_client: Option<Arc<filetracker_client::FiletrackerClient>>,
    pub metrics: Arc<metrics::Metrics>,
}

impl AppState {
    pub async fn new(config: &config::BucketConfig) -> Result<Self> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new(config.locks_type);
        let s3storage = s3storage::S3Storage::new(config).await?;
        let metrics = Arc::new(metrics::Metrics::new());
        Ok(Self {
            bucket_name: config.name.clone().into(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks: Arc::new(Mutex::new(locks)),
            s3storage: Arc::new(Mutex::new(s3storage)),
            filetracker_client: None,
            metrics,
        })
    }

    pub async fn new_with_filetracker(
        config: &config::BucketConfig,
        filetracker_url: String,
    ) -> Result<Self> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new(config.locks_type);
        let s3storage = s3storage::S3Storage::new(config).await?;
        let filetracker_client = filetracker_client::FiletrackerClient::new(filetracker_url);
        let metrics = Arc::new(metrics::Metrics::new());

        // Mark migration as active
        metrics::MIGRATION_ACTIVE.set(1);

        Ok(Self {
            bucket_name: config.name.clone().into(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks: Arc::new(Mutex::new(locks)),
            s3storage: Arc::new(Mutex::new(s3storage)),
            filetracker_client: Some(Arc::new(filetracker_client)),
            metrics,
        })
    }
}
