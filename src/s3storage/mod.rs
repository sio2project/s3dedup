use crate::config::BucketConfig;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use serde::Deserialize;
use tracing::{debug, info};

pub mod s3compat;

#[cfg(feature = "test-mocks")]
pub mod mock;

/// Configuration for S3 key sharding
/// When enabled, transforms keys like "abcdef..." to "ab/cd/abcdef..."
/// to distribute objects across directories and avoid ext4 performance issues
#[derive(Debug, Deserialize, Clone)]
pub struct KeyShardingConfig {
    /// Whether key sharding is enabled
    #[serde(default = "default_sharding_enabled")]
    pub enabled: bool,
    /// Number of prefix levels (2 = ab/cd/, 3 = ab/cd/ef/)
    #[serde(default = "default_sharding_depth")]
    pub depth: usize,
}

fn default_sharding_enabled() -> bool {
    true
}

fn default_sharding_depth() -> usize {
    2
}

impl Default for KeyShardingConfig {
    fn default() -> Self {
        Self {
            enabled: default_sharding_enabled(),
            depth: default_sharding_depth(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub enum S3StorageType {
    #[serde(rename = "s3")]
    S3Compat,
}

#[async_trait]
pub(crate) trait S3StorageTrait {
    async fn new(config: &BucketConfig) -> Result<Box<Self>>
    where
        Self: Sized;

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()>;
    async fn get_object(&self, key: &str) -> Result<Vec<u8>>;
    async fn delete_object(&self, key: &str) -> Result<()>;
    async fn object_exists(&self, key: &str) -> Result<bool>;

    /// Put an object from a ByteStream without buffering into memory.
    async fn put_object_stream(
        &self,
        key: &str,
        body: ByteStream,
        content_length: Option<i64>,
    ) -> Result<()>;

    /// Get an object as a stream without buffering into memory.
    /// Returns (ByteStream, content_length).
    async fn get_object_stream(&self, key: &str) -> Result<(ByteStream, Option<i64>)>;

    /// HEAD an object to check existence and get content length.
    /// Returns None if the object does not exist.
    async fn object_exists_with_size(&self, key: &str) -> Result<Option<i64>>;

    /// List objects in batches with continuation support
    /// Returns (keys, continuation_token)
    async fn list_objects(
        &self,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)>;

    /// Health check - lightweight operation to verify S3 connectivity
    async fn check_health(&self) -> Result<()>;
}

#[derive(Clone)]
pub enum S3Storage {
    S3Compat(s3compat::S3CompatClient),
    #[cfg(feature = "test-mocks")]
    Mock(mock::MockS3Storage),
}

impl S3Storage {
    pub async fn new(config: &BucketConfig) -> Result<Box<Self>> {
        match config.s3storage_type {
            S3StorageType::S3Compat => {
                info!("Using S3-compatible storage");
                let client = s3compat::S3CompatClient::new(config).await?;
                Ok(Box::new(S3Storage::S3Compat(*client)))
            }
        }
    }

    pub async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()> {
        debug!("Putting object with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.put_object(key, data).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.put_object(key, data).await,
        }
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        debug!("Getting object with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.get_object(key).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.get_object(key).await,
        }
    }

    pub async fn get_object_stream(&self, key: &str) -> Result<(ByteStream, Option<i64>)> {
        debug!("Getting object stream with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.get_object_stream(key).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.get_object_stream(key).await,
        }
    }

    pub async fn put_object_stream(
        &self,
        key: &str,
        body: ByteStream,
        content_length: Option<i64>,
    ) -> Result<()> {
        debug!("Putting object stream with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.put_object_stream(key, body, content_length).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.put_object_stream(key, body, content_length).await,
        }
    }

    pub async fn delete_object(&self, key: &str) -> Result<()> {
        debug!("Deleting object with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.delete_object(key).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.delete_object(key).await,
        }
    }

    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        debug!("Checking if object exists with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.object_exists(key).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.object_exists(key).await,
        }
    }

    pub async fn object_exists_with_size(&self, key: &str) -> Result<Option<i64>> {
        debug!("HEAD object for size with key: {}", key);
        match self {
            S3Storage::S3Compat(s) => s.object_exists_with_size(key).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.object_exists_with_size(key).await,
        }
    }

    pub async fn list_objects(
        &self,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        debug!(
            "Listing objects with continuation_token: {:?}",
            continuation_token
        );
        match self {
            S3Storage::S3Compat(s) => s.list_objects(continuation_token).await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.list_objects(continuation_token).await,
        }
    }

    pub async fn check_health(&self) -> Result<()> {
        debug!("Checking S3 health");
        match self {
            S3Storage::S3Compat(s) => s.check_health().await,
            #[cfg(feature = "test-mocks")]
            S3Storage::Mock(s) => s.check_health().await,
        }
    }
}
