use std::sync::Arc;
use tokio::sync::Mutex;

pub mod cleaner;
pub mod config;
pub mod kvstorage;
pub mod locks;
pub mod logging;
pub mod routes;
pub mod s3storage;

#[derive(Clone)]
pub struct AppState {
    pub bucket_name: String,
    pub kvstorage: Arc<Mutex<Box<kvstorage::KVStorage>>>,
    pub locks: Arc<Mutex<Box<locks::LocksStorage>>>,
    pub s3storage: Arc<Mutex<Box<s3storage::S3Storage>>>,
}

impl AppState {
    pub async fn new(
        config: &config::BucketConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let kvstorage = kvstorage::KVStorage::new(config).await?;
        let locks = locks::LocksStorage::new(&config.locks_type);
        let s3storage = s3storage::S3Storage::new(config).await?;
        Ok(Self {
            bucket_name: config.name.clone(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks: Arc::new(Mutex::new(locks)),
            s3storage: Arc::new(Mutex::new(s3storage)),
        })
    }
}
