use crate::cleaner::CleanerConfig;
use crate::logging::LoggingConfig;
use anyhow::Result;
use anyhow::bail;

pub use crate::db::PostgresConfig;
pub use crate::kvstorage::KVStorageType;
pub use crate::kvstorage::sqlite::SQLiteConfig;
pub use crate::locks::LocksType;
pub use crate::s3storage::KeyShardingConfig;
pub use crate::s3storage::S3StorageType;
pub use crate::s3storage::s3compat::S3CompatConfig;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Config {
    pub logging: LoggingConfig,

    pub kvstorage_type: KVStorageType,

    #[serde(default)]
    pub postgres: Option<PostgresConfig>,

    #[serde(default)]
    pub sqlite: Option<SQLiteConfig>,

    pub locks_type: LocksType,

    pub bucket: BucketConfig,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct BucketConfig {
    pub name: String,
    pub address: String,
    pub port: u16,

    pub s3storage_type: S3StorageType,

    #[serde(default)]
    pub s3: Option<S3CompatConfig>,

    #[serde(default)]
    pub cleaner: CleanerConfig,

    /// Maximum file size (in bytes) to process in memory during PUT slow path.
    /// Files larger than this are spilled to temp files.
    /// Default: 64MB. Set to 0 to always use temp files.
    #[serde(default = "default_max_inmemory_size")]
    pub max_inmemory_size: usize,

    /// Optional filetracker URL for live migration mode
    #[serde(default)]
    pub filetracker_url: Option<String>,

    /// Optional V1 filetracker directory for filesystem-based migration
    #[serde(default)]
    pub filetracker_v1_dir: Option<String>,
}

fn default_max_inmemory_size() -> usize {
    64 * 1024 * 1024 // 64MB
}

impl Config {
    pub fn new(path: &str) -> Result<Self> {
        let config_str = std::fs::read_to_string(path)?;
        let mut config: Config = serde_json::from_str(config_str.as_str())?;

        // Apply environment variable overrides
        config.apply_env_overrides();

        Ok(config)
    }

    /// Create a default config from environment variables only
    pub fn from_env() -> Result<Self> {
        let logging = LoggingConfig {
            level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            json: std::env::var("LOG_JSON")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            json_log_path: std::env::var("JSON_LOG_PATH").ok(),
        };

        let kvstorage_type_str =
            std::env::var("KVSTORAGE_TYPE").unwrap_or_else(|_| "sqlite".to_string());
        let kvstorage_type = match kvstorage_type_str.as_str() {
            "postgres" => KVStorageType::Postgres,
            "sqlite" => KVStorageType::SQLite,
            _ => bail!("Invalid KVSTORAGE_TYPE: {}", kvstorage_type_str),
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

        let locks_type = {
            let locks_type_str =
                std::env::var("LOCKS_TYPE").unwrap_or_else(|_| "memory".to_string());
            match locks_type_str.as_str() {
                "memory" => LocksType::Memory,
                _ => LocksType::Memory, // Default to memory
            }
        };

        let bucket = BucketConfig::from_env()?;

        Ok(Config {
            logging,
            kvstorage_type,
            postgres,
            sqlite,
            locks_type,
            bucket,
        })
    }

    /// Apply environment variable overrides to this config
    fn apply_env_overrides(&mut self) {
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

        // Apply bucket-specific overrides
        self.bucket.apply_env_overrides();
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

        // S3 storage overrides
        if let Some(ref mut s3_config) = self.s3 {
            if let Ok(val) = std::env::var("S3_ENDPOINT") {
                s3_config.endpoint = val;
            }
            if let Ok(val) = std::env::var("S3_ACCESS_KEY") {
                s3_config.access_key = val;
            }
            if let Ok(val) = std::env::var("S3_SECRET_KEY") {
                s3_config.secret_key = val;
            }
            if let Ok(val) = std::env::var("S3_KEY_SHARDING_ENABLED")
                && let Ok(enabled) = val.parse()
            {
                s3_config.key_sharding.enabled = enabled;
            }
            if let Ok(val) = std::env::var("S3_KEY_SHARDING_DEPTH")
                && let Ok(depth) = val.parse()
            {
                s3_config.key_sharding.depth = depth;
            }
        }

        // Filetracker URL for live migration
        if let Ok(val) = std::env::var("FILETRACKER_URL") {
            self.filetracker_url = Some(val);
        }

        if let Ok(val) = std::env::var("MAX_INMEMORY_SIZE")
            && let Ok(size) = val.parse()
        {
            self.max_inmemory_size = size;
        }
    }

    /// Create a bucket config from environment variables only
    fn from_env() -> Result<Self> {
        let s3 = Some(S3CompatConfig {
            endpoint: std::env::var("S3_ENDPOINT")?,
            access_key: std::env::var("S3_ACCESS_KEY")?,
            secret_key: std::env::var("S3_SECRET_KEY")?,
            force_path_style: std::env::var("S3_FORCE_PATH_STYLE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            region: std::env::var("S3_REGION").unwrap_or_else(|_| "garage".to_string()),
            key_sharding: KeyShardingConfig {
                enabled: std::env::var("S3_KEY_SHARDING_ENABLED")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(true),
                depth: std::env::var("S3_KEY_SHARDING_DEPTH")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(2),
            },
        });

        Ok(BucketConfig {
            name: std::env::var("BUCKET_NAME").unwrap_or_else(|_| "default".to_string()),
            address: std::env::var("LISTEN_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: std::env::var("LISTEN_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8080),
            s3storage_type: S3StorageType::S3Compat,
            s3,
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
            max_inmemory_size: std::env::var("MAX_INMEMORY_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_max_inmemory_size),
            filetracker_url: std::env::var("FILETRACKER_URL").ok(),
            filetracker_v1_dir: std::env::var("FILETRACKER_V1_DIR").ok(),
        })
    }
}
