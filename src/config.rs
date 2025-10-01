use crate::logging::LoggingConfig;
use std::error::Error;

pub use crate::kvstorage::KVStorageType;
pub use crate::kvstorage::postgres::PostgresConfig;
pub use crate::kvstorage::sqlite::SQLiteConfig;
pub use crate::locks::LocksType;
pub use crate::s3storage::S3StorageType;
pub use crate::s3storage::minio::MinIOConfig;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Config {
    pub logging: LoggingConfig,
    pub buckets: Vec<BucketConfig>,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct BucketConfig {
    pub name: String,
    pub address: String,
    pub port: u16,

    pub kvstorage_type: KVStorageType,

    #[serde(default)]
    pub postgres: Option<PostgresConfig>,

    #[serde(default)]
    pub sqlite: Option<SQLiteConfig>,

    pub locks_type: LocksType,

    pub s3storage_type: S3StorageType,

    #[serde(default)]
    pub minio: Option<MinIOConfig>,
}

impl Config {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(config_str.as_str())?;
        Ok(config)
    }
}
