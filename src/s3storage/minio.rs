use crate::config::BucketConfig;
use crate::s3storage::S3StorageTrait;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use serde::Deserialize;
use std::error::Error;
use tracing::{debug, error};

#[derive(Debug, Clone, Deserialize)]
pub struct MinIOConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
}

fn default_force_path_style() -> bool {
    true
}

#[derive(Clone)]
pub struct MinIOClient {
    client: Client,
    bucket: String,
}

#[async_trait]
impl S3StorageTrait for MinIOClient {
    async fn new(config: &BucketConfig) -> Result<Box<Self>, Box<dyn Error + Send + Sync>> {
        let minio_config = config.minio.as_ref().ok_or("MinIO config not found")?;

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

        let minio_client = MinIOClient {
            client,
            bucket: config.name.clone(),
        };

        // Create bucket if it doesn't exist
        minio_client.ensure_bucket_exists().await?;

        Ok(Box::new(minio_client))
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Putting object: {} (size: {} bytes)", key, data.len());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .content_type("application/octet-stream")
            .send()
            .await?;

        debug!("Successfully put object: {}", key);
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        debug!("Getting object: {}", key);

        let resp = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let data = resp.body.collect().await?.into_bytes().to_vec();
        debug!("Successfully got object: {} (size: {} bytes)", key, data.len());
        Ok(data)
    }

    async fn delete_object(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Deleting object: {}", key);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        debug!("Successfully deleted object: {}", key);
        Ok(())
    }

    async fn object_exists(&self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        debug!("Checking if object exists: {}", key);

        match self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => {
                debug!("Object exists: {}", key);
                Ok(true)
            }
            Err(err) => {
                let err_string = err.to_string();
                debug!("Head object error: {}", err_string);

                if err_string.contains("NotFound") || err_string.contains("404") || err_string.contains("NoSuchKey") ||
                   format!("{:?}", err).contains("NotFound") || format!("{:?}", err).contains("NoSuchKey") {
                    debug!("Object does not exist: {}", key);
                    Ok(false)
                } else {
                    debug!("Error checking object existence: {}", err);
                    Err(Box::new(err))
                }
            }
        }
    }
}

impl MinIOClient {
    /// Ensures the bucket exists, creating it if necessary
    async fn ensure_bucket_exists(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
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

                if err_string.contains("NotFound") || err_string.contains("404") || err_string.contains("NoSuchBucket") ||
                   format!("{:?}", err).contains("NotFound") || format!("{:?}", err).contains("NoSuchBucket") {
                    debug!("Bucket does not exist, creating: {}", self.bucket);

                    // Create the bucket
                    match self.client.create_bucket().bucket(&self.bucket).send().await {
                        Ok(_) => {
                            debug!("Successfully created bucket: {}", self.bucket);
                            Ok(())
                        }
                        Err(create_err) => {
                            // Check if bucket was created by another process concurrently
                            let create_err_string = create_err.to_string();
                            debug!("Create bucket error: {}", create_err_string);
                            if create_err_string.contains("BucketAlreadyExists") || create_err_string.contains("BucketAlreadyOwnedByYou") {
                                debug!("Bucket was created concurrently: {}", self.bucket);
                                Ok(())
                            } else {
                                error!("Failed to create bucket {}: {}", self.bucket, create_err);
                                Err(Box::new(create_err))
                            }
                        }
                    }
                } else {
                    error!("Error checking bucket existence: {}", err);
                    Err(Box::new(err))
                }
            }
        }
    }
}