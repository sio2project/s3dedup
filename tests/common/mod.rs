#![allow(dead_code)]

use s3dedup::config::{BucketConfig, Config, S3CompatConfig};

/// Generate a unique ID for test isolation using process ID, thread ID, and timestamp.
pub fn generate_unique_id() -> String {
    let thread_id = std::thread::current().id();
    let thread_id_str = format!("{:?}", thread_id)
        .replace("ThreadId(", "")
        .replace(")", "");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}{}{}", std::process::id(), thread_id_str, nanos)
}

/// Create an S3CompatConfig from environment variables.
/// Panics if S3_ACCESS_KEY or S3_SECRET_KEY are not set.
pub fn s3_config_from_env() -> S3CompatConfig {
    S3CompatConfig {
        endpoint: std::env::var("S3_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:3900".to_string()),
        access_key: std::env::var("S3_ACCESS_KEY")
            .expect("S3_ACCESS_KEY environment variable required for tests"),
        secret_key: std::env::var("S3_SECRET_KEY")
            .expect("S3_SECRET_KEY environment variable required for tests"),
        force_path_style: true,
        region: std::env::var("S3_REGION").unwrap_or_else(|_| "garage".to_string()),
        key_sharding: Default::default(),
    }
}

/// Create a BucketConfig with a unique bucket name.
/// Returns (config, unique_id) where unique_id can be used for SQLite DB naming.
pub fn create_test_bucket_config(prefix: &str) -> (BucketConfig, String) {
    let unique_id = generate_unique_id();
    let test_bucket = format!("{}-{}", prefix, unique_id.to_lowercase());

    let config = BucketConfig {
        name: test_bucket,
        address: "127.0.0.1".to_string(),
        port: 3000,
        s3storage_type: s3dedup::s3storage::S3StorageType::S3Compat,
        s3: Some(s3_config_from_env()),
        cleaner: Default::default(),
        max_inmemory_size: 64 * 1024 * 1024,
        temp_dir: None,
        filetracker_url: None,
        filetracker_v1_dir: None,
    };

    (config, unique_id)
}

/// Create a full test Config with SQLite backend and unique bucket.
/// Detects DATABASE_URL for postgres, otherwise uses SQLite.
pub fn create_test_config(prefix: &str) -> (Config, String) {
    let (bucket_config, unique_id) = create_test_bucket_config(prefix);
    let use_postgres = std::env::var("DATABASE_URL").is_ok();

    std::fs::create_dir_all("db").ok();

    let (kvstorage_type, sqlite, postgres) = if use_postgres {
        (
            s3dedup::config::KVStorageType::Postgres,
            None,
            Some(s3dedup::config::PostgresConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                dbname: "s3dedup_test".to_string(),
                pool_size: 10,
            }),
        )
    } else {
        (
            s3dedup::config::KVStorageType::SQLite,
            Some(s3dedup::config::SQLiteConfig {
                path: format!("db/test-{}-{}.db", prefix, unique_id),
                pool_size: 50,
            }),
            None,
        )
    };

    let config = Config {
        logging: s3dedup::logging::LoggingConfig {
            level: "info".to_string(),
            json: false,
            json_log_path: None,
        },
        kvstorage_type,
        sqlite,
        postgres,
        locks_type: s3dedup::config::LocksType::Memory,
        bucket: bucket_config,
    };

    (config, unique_id)
}

/// Check if S3 storage is available (credentials set in env).
pub fn is_s3_available() -> bool {
    std::env::var("S3_ACCESS_KEY").is_ok() && std::env::var("S3_SECRET_KEY").is_ok()
}
