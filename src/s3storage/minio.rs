use crate::config::BucketConfig;
use crate::s3storage::{KeyShardingConfig, S3StorageTrait};
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use serde::Deserialize;
use tracing::{debug, error};

#[derive(Debug, Clone, Deserialize)]
pub struct MinIOConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
    #[serde(default)]
    pub key_sharding: KeyShardingConfig,
}

fn default_force_path_style() -> bool {
    true
}

#[derive(Clone)]
pub struct MinIOClient {
    client: Client,
    bucket: String,
    sharding: KeyShardingConfig,
}

#[async_trait]
impl S3StorageTrait for MinIOClient {
    async fn new(config: &BucketConfig) -> Result<Box<Self>> {
        let minio_config = config
            .minio
            .as_ref()
            .ok_or_else(|| anyhow!("MinIO config not found"))?;

        debug!("Connecting to MinIO at: {}", minio_config.endpoint);

        // Create credentials
        let credentials = Credentials::new(
            &minio_config.access_key,
            &minio_config.secret_key,
            None,
            None,
            "minio",
        );

        // Set up region (MinIO doesn't care about regions, but AWS SDK requires one)
        let region = Region::new("us-east-1");

        let s3_config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region)
            .credentials_provider(credentials)
            .endpoint_url(&minio_config.endpoint)
            .force_path_style(minio_config.force_path_style)
            .build();

        let client = Client::from_conf(s3_config);

        debug!("MinIO client initialized for bucket: {}", config.name);
        debug!(
            "Key sharding: enabled={}, depth={}",
            minio_config.key_sharding.enabled, minio_config.key_sharding.depth
        );

        let minio_client = MinIOClient {
            client,
            bucket: config.name.clone(),
            sharding: minio_config.key_sharding.clone(),
        };

        // Create bucket if it doesn't exist
        minio_client.ensure_bucket_exists().await?;

        Ok(Box::new(minio_client))
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let s3_key = self.hash_to_s3_key(key);
        debug!(
            "Putting object: {} -> {} (size: {} bytes)",
            key,
            s3_key,
            data.len()
        );

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .body(ByteStream::from(data))
            .content_type("application/octet-stream")
            .send()
            .await?;

        debug!("Successfully put object: {}", s3_key);
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let s3_key = self.hash_to_s3_key(key);
        debug!("Getting object: {} -> {}", key, s3_key);

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await?;

        let data = resp.body.collect().await?.into_bytes().to_vec();
        debug!(
            "Successfully got object: {} (size: {} bytes)",
            s3_key,
            data.len()
        );
        Ok(data)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let s3_key = self.hash_to_s3_key(key);
        debug!("Deleting object: {} -> {}", key, s3_key);

        let delete_future = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send();

        // Add 10 second timeout to prevent indefinite hanging
        match tokio::time::timeout(std::time::Duration::from_secs(10), delete_future).await {
            Ok(Ok(_)) => {
                debug!("Successfully deleted object: {}", s3_key);
                Ok(())
            }
            Ok(Err(e)) => {
                error!("Failed to delete object {}: {}", s3_key, e);
                Err(anyhow::Error::from(e))
            }
            Err(_) => {
                error!("Timeout deleting object {}", s3_key);
                bail!("Timeout deleting object")
            }
        }
    }

    async fn object_exists(&self, key: &str) -> Result<bool> {
        let s3_key = self.hash_to_s3_key(key);
        debug!("Checking if object exists: {} -> {}", key, s3_key);

        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await
        {
            Ok(_) => {
                debug!("Object exists: {}", s3_key);
                Ok(true)
            }
            Err(err) => {
                if Self::is_not_found_error(&err) {
                    debug!("Object does not exist: {}", s3_key);
                    Ok(false)
                } else {
                    debug!("Error checking object existence: {}", err);
                    bail!(err)
                }
            }
        }
    }

    async fn list_objects(
        &self,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        debug!(
            "Listing objects with continuation_token: {:?}",
            continuation_token
        );

        let mut request = self.client.list_objects_v2().bucket(&self.bucket);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let resp = request.send().await?;

        // Strip sharding prefixes to return raw hashes
        let keys: Vec<String> = resp
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| self.s3_key_to_hash(k)))
            .collect();

        let next_token = resp.next_continuation_token().map(|t| t.to_string());

        debug!(
            "Listed {} objects (hashes), has more: {}",
            keys.len(),
            next_token.is_some()
        );

        Ok((keys, next_token))
    }

    async fn check_health(&self) -> Result<()> {
        debug!("Health check: HEAD bucket {}", self.bucket);
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await?;
        debug!("Health check passed for bucket {}", self.bucket);
        Ok(())
    }
}

impl MinIOClient {
    /// Transform a raw hash to S3 key with sharding prefix
    /// "abcdef..." -> "ab/cd/abcdef..."
    fn hash_to_s3_key(&self, hash: &str) -> String {
        if !self.sharding.enabled {
            return hash.to_string();
        }

        // Need at least depth*2 characters for sharding
        if hash.len() < self.sharding.depth * 2 {
            return hash.to_string();
        }

        let mut parts = Vec::with_capacity(self.sharding.depth + 1);
        for i in 0..self.sharding.depth {
            parts.push(&hash[i * 2..(i + 1) * 2]);
        }
        parts.push(hash);
        parts.join("/")
    }

    /// Extract raw hash from S3 key (strip sharding prefix)
    /// "ab/cd/abcdef..." -> "abcdef..."
    fn s3_key_to_hash(&self, key: &str) -> String {
        if !self.sharding.enabled {
            return key.to_string();
        }
        // The hash is always the last component
        key.rsplit('/').next().unwrap_or(key).to_string()
    }

    /// Check if an error indicates object not found
    fn is_not_found_error(err: &aws_sdk_s3::error::SdkError<impl std::fmt::Debug>) -> bool {
        let err_string = err.to_string();
        err_string.contains("NotFound")
            || err_string.contains("404")
            || err_string.contains("NoSuchKey")
            || format!("{:?}", err).contains("NotFound")
            || format!("{:?}", err).contains("NoSuchKey")
    }

    /// Ensures the bucket exists, creating it if necessary
    async fn ensure_bucket_exists(&self) -> Result<()> {
        debug!("Checking if bucket exists: {}", self.bucket);

        // Try to check if bucket exists using head_bucket
        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => {
                debug!("Bucket already exists: {}", self.bucket);
                Ok(())
            }
            Err(err) => {
                // Check if the error is "NotFound" or similar, indicating bucket doesn't exist
                let err_string = err.to_string();
                debug!("Head bucket error: {}", err_string);
                debug!("Head bucket error debug: {:?}", err);

                if err_string.contains("NotFound")
                    || err_string.contains("404")
                    || err_string.contains("NoSuchBucket")
                    || format!("{:?}", err).contains("NotFound")
                    || format!("{:?}", err).contains("NoSuchBucket")
                {
                    debug!("Bucket does not exist, creating: {}", self.bucket);

                    // Create the bucket
                    match self
                        .client
                        .create_bucket()
                        .bucket(&self.bucket)
                        .send()
                        .await
                    {
                        Ok(_) => {
                            debug!("Successfully created bucket: {}", self.bucket);
                            Ok(())
                        }
                        Err(create_err) => {
                            // Check if bucket was created by another process concurrently
                            let create_err_string = create_err.to_string();
                            debug!("Create bucket error: {}", create_err_string);
                            if create_err_string.contains("BucketAlreadyExists")
                                || create_err_string.contains("BucketAlreadyOwnedByYou")
                            {
                                debug!("Bucket was created concurrently: {}", self.bucket);
                                Ok(())
                            } else {
                                error!("Failed to create bucket {}: {}", self.bucket, create_err);
                                bail!(create_err)
                            }
                        }
                    }
                } else {
                    error!("Error checking bucket existence: {}", err);
                    bail!(err)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a test sharding config
    fn make_sharding(enabled: bool, depth: usize) -> KeyShardingConfig {
        KeyShardingConfig { enabled, depth }
    }

    // Helper struct to test key transformation without needing full MinIOClient
    struct TestClient {
        sharding: KeyShardingConfig,
    }

    impl TestClient {
        fn hash_to_s3_key(&self, hash: &str) -> String {
            if !self.sharding.enabled {
                return hash.to_string();
            }

            if hash.len() < self.sharding.depth * 2 {
                return hash.to_string();
            }

            let mut parts = Vec::with_capacity(self.sharding.depth + 1);
            for i in 0..self.sharding.depth {
                parts.push(&hash[i * 2..(i + 1) * 2]);
            }
            parts.push(hash);
            parts.join("/")
        }

        fn s3_key_to_hash(&self, key: &str) -> String {
            if !self.sharding.enabled {
                return key.to_string();
            }
            key.rsplit('/').next().unwrap_or(key).to_string()
        }
    }

    #[test]
    fn test_hash_to_s3_key_disabled() {
        let client = TestClient {
            sharding: make_sharding(false, 2),
        };
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        assert_eq!(client.hash_to_s3_key(hash), hash);
    }

    #[test]
    fn test_hash_to_s3_key_depth_2() {
        let client = TestClient {
            sharding: make_sharding(true, 2),
        };
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        assert_eq!(
            client.hash_to_s3_key(hash),
            "ab/cd/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn test_hash_to_s3_key_depth_3() {
        let client = TestClient {
            sharding: make_sharding(true, 3),
        };
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        assert_eq!(
            client.hash_to_s3_key(hash),
            "ab/cd/ef/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn test_hash_to_s3_key_short_hash() {
        let client = TestClient {
            sharding: make_sharding(true, 2),
        };
        // Hash too short for depth 2 (needs at least 4 chars)
        let hash = "abc";
        assert_eq!(client.hash_to_s3_key(hash), hash);
    }

    #[test]
    fn test_s3_key_to_hash_disabled() {
        let client = TestClient {
            sharding: make_sharding(false, 2),
        };
        let key = "ab/cd/abcdef123";
        assert_eq!(client.s3_key_to_hash(key), key);
    }

    #[test]
    fn test_s3_key_to_hash_enabled() {
        let client = TestClient {
            sharding: make_sharding(true, 2),
        };
        let key = "ab/cd/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        assert_eq!(
            client.s3_key_to_hash(key),
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn test_s3_key_to_hash_no_slashes() {
        let client = TestClient {
            sharding: make_sharding(true, 2),
        };
        let key = "abcdef1234567890";
        assert_eq!(client.s3_key_to_hash(key), key);
    }

    #[test]
    fn test_roundtrip() {
        let client = TestClient {
            sharding: make_sharding(true, 2),
        };
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let s3_key = client.hash_to_s3_key(hash);
        let recovered = client.s3_key_to_hash(&s3_key);
        assert_eq!(recovered, hash);
    }
}
