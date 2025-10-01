use crate::config::BucketConfig;
use async_trait::async_trait;
use serde::Deserialize;
use std::error::Error;
use tracing::{debug, info};

pub mod minio;

#[derive(Debug, Deserialize, Clone)]
pub enum S3StorageType {
    #[serde(rename = "minio")]
    MinIO,
    #[serde(rename = "aws")]
    AWS,
}

#[async_trait]
pub(crate) trait S3StorageTrait {
    async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn Error + Send + Sync>>
    where
        Self: Sized;

    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;
    async fn delete_object(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn object_exists(&self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>>;

    /// List objects in batches with continuation support
    /// Returns (keys, continuation_token)
    async fn list_objects(
        &self,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Box<dyn Error + Send + Sync>>;
}

#[derive(Clone)]
pub enum S3Storage {
    MinIO(minio::MinIOClient),
}

impl S3Storage {
    pub async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn Error + Send + Sync>> {
        match config.s3storage_type {
            S3StorageType::MinIO => {
                info!("Using MinIO as S3 storage");
                let client = minio::MinIOClient::new(config).await?;
                Ok(Box::new(S3Storage::MinIO(*client)))
            }
            S3StorageType::AWS => {
                // TODO: Implement AWS S3 client
                todo!("AWS S3 client not implemented yet")
            }
        }
    }

    pub async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Putting object with key: {}", key);
        match self {
            S3Storage::MinIO(client) => client.put_object(key, data).await,
        }
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        debug!("Getting object with key: {}", key);
        match self {
            S3Storage::MinIO(client) => client.get_object(key).await,
        }
    }

    pub async fn delete_object(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Deleting object with key: {}", key);
        match self {
            S3Storage::MinIO(client) => client.delete_object(key).await,
        }
    }

    pub async fn object_exists(&self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        debug!("Checking if object exists with key: {}", key);
        match self {
            S3Storage::MinIO(client) => client.object_exists(key).await,
        }
    }

    pub async fn list_objects(
        &self,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Box<dyn Error + Send + Sync>> {
        debug!("Listing objects with continuation_token: {:?}", continuation_token);
        match self {
            S3Storage::MinIO(client) => client.list_objects(continuation_token).await,
        }
    }
}
