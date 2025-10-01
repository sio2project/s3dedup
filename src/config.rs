use crate::cleaner::CleanerConfig;
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

    #[serde(default)]
    pub cleaner: CleanerConfig,

    /// Optional filetracker URL for live migration mode
    #[serde(default)]
    pub filetracker_url: Option<String>,
}

impl Config {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let mut config: Config = serde_json::from_str(config_str.as_str())?;

        // Apply environment variable overrides for the first bucket
        if !config.buckets.is_empty() {
            config.buckets[0].apply_env_overrides();
        }

        Ok(config)
    }

    /// Create a default config from environment variables only
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let logging = LoggingConfig {
            level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            json: std::env::var("LOG_JSON")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
        };

        let bucket = BucketConfig::from_env()?;

        Ok(Config {
            logging,
            buckets: vec![bucket],
        })
    }
}

impl BucketConfig {
    /// Apply environment variable overrides to this bucket config
    fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var("BUCKET_NAME") {
            self.name = val;
        }
        if let Ok(val) = std::env::var("LISTEN_ADDRESS") {
            self.address = val;
        }
        if let Ok(val) = std::env::var("LISTEN_PORT")
            && let Ok(port) = val.parse()
        {
            self.port = port;
        }

        // SQLite overrides
        if let Some(ref mut sqlite) = self.sqlite {
            if let Ok(val) = std::env::var("SQLITE_PATH") {
                sqlite.path = val;
            }
            if let Ok(val) = std::env::var("SQLITE_MAX_CONNECTIONS")
                && let Ok(pool_size) = val.parse()
            {
                sqlite.pool_size = pool_size;
            }
        }

        // PostgreSQL overrides
        if let Some(ref mut postgres) = self.postgres {
            if let Ok(val) = std::env::var("POSTGRES_HOST") {
                postgres.host = val;
            }
            if let Ok(val) = std::env::var("POSTGRES_PORT")
                && let Ok(port) = val.parse()
            {
                postgres.port = port;
            }
            if let Ok(val) = std::env::var("POSTGRES_USER") {
                postgres.user = val;
            }
            if let Ok(val) = std::env::var("POSTGRES_PASSWORD") {
                postgres.password = val;
            }
            if let Ok(val) = std::env::var("POSTGRES_DB") {
                postgres.dbname = val;
            }
            if let Ok(val) = std::env::var("POSTGRES_MAX_CONNECTIONS")
                && let Ok(pool_size) = val.parse()
            {
                postgres.pool_size = pool_size;
            }
        }

        // MinIO/S3 overrides
        if let Some(ref mut minio) = self.minio {
            if let Ok(val) = std::env::var("S3_ENDPOINT") {
                minio.endpoint = val;
            }
            if let Ok(val) = std::env::var("S3_ACCESS_KEY") {
                minio.access_key = val;
            }
            if let Ok(val) = std::env::var("S3_SECRET_KEY") {
                minio.secret_key = val;
            }
        }

        // Filetracker URL for live migration
        if let Ok(val) = std::env::var("FILETRACKER_URL") {
            self.filetracker_url = Some(val);
        }
    }

    /// Create a bucket config from environment variables only
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let kvstorage_type_str =
            std::env::var("KVSTORAGE_TYPE").unwrap_or_else(|_| "sqlite".to_string());
        let kvstorage_type = match kvstorage_type_str.as_str() {
            "postgres" => KVStorageType::Postgres,
            "sqlite" => KVStorageType::SQLite,
            _ => return Err(format!("Invalid KVSTORAGE_TYPE: {}", kvstorage_type_str).into()),
        };

        let sqlite = if matches!(kvstorage_type, KVStorageType::SQLite) {
            Some(SQLiteConfig {
                path: std::env::var("SQLITE_PATH")
                    .unwrap_or_else(|_| "/app/data/kv.db".to_string()),
                pool_size: std::env::var("SQLITE_MAX_CONNECTIONS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10),
            })
        } else {
            None
        };

        let postgres = if matches!(kvstorage_type, KVStorageType::Postgres) {
            Some(PostgresConfig {
                host: std::env::var("POSTGRES_HOST")?,
                port: std::env::var("POSTGRES_PORT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5432),
                user: std::env::var("POSTGRES_USER")?,
                password: std::env::var("POSTGRES_PASSWORD")?,
                dbname: std::env::var("POSTGRES_DB")?,
                pool_size: std::env::var("POSTGRES_MAX_CONNECTIONS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10),
            })
        } else {
            None
        };

        let s3storage_type_str =
            std::env::var("S3STORAGE_TYPE").unwrap_or_else(|_| "minio".to_string());
        let s3storage_type = match s3storage_type_str.as_str() {
            "minio" => S3StorageType::MinIO,
            _ => return Err(format!("Invalid S3STORAGE_TYPE: {}", s3storage_type_str).into()),
        };

        let minio = if matches!(s3storage_type, S3StorageType::MinIO) {
            Some(MinIOConfig {
                endpoint: std::env::var("S3_ENDPOINT")?,
                access_key: std::env::var("S3_ACCESS_KEY")?,
                secret_key: std::env::var("S3_SECRET_KEY")?,
                force_path_style: std::env::var("S3_FORCE_PATH_STYLE")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(true),
            })
        } else {
            None
        };

        Ok(BucketConfig {
            name: std::env::var("BUCKET_NAME").unwrap_or_else(|_| "default".to_string()),
            address: std::env::var("LISTEN_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: std::env::var("LISTEN_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8080),
            kvstorage_type,
            sqlite,
            postgres,
            locks_type: {
                let locks_type_str =
                    std::env::var("LOCKS_TYPE").unwrap_or_else(|_| "memory".to_string());
                match locks_type_str.as_str() {
                    "memory" => LocksType::Memory,
                    _ => LocksType::Memory, // Default to memory
                }
            },
            s3storage_type,
            minio,
            cleaner: CleanerConfig {
                enabled: std::env::var("CLEANER_ENABLED")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(true),
                interval_seconds: std::env::var("CLEANER_INTERVAL")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3600),
                batch_size: std::env::var("CLEANER_BATCH_SIZE")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1000),
                max_deletes_per_run: std::env::var("CLEANER_MAX_DELETES")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10000),
            },
            filetracker_url: std::env::var("FILETRACKER_URL").ok(),
        })
    }
}
