mod common;

use s3dedup::routes::ft::storage_helpers::{compress_gzip, decompress_gzip};
use s3dedup::s3storage::KeyShardingConfig;

// ---- Storage helper tests (no backends needed) ----

/// Truncated gzip data should return an error from decompress_gzip
#[test]
fn test_truncated_gzip_data_returns_error() {
    let original = b"Hello world, this is some test data for gzip truncation test";
    let compressed = compress_gzip(original).unwrap();

    // Truncate the compressed data at roughly half its length
    let truncated = &compressed[..compressed.len() / 2];

    let result = decompress_gzip(truncated);
    assert!(
        result.is_err(),
        "decompress_gzip should return Err for truncated gzip data"
    );
}

/// Non-gzip data (random bytes) with intent to decompress should return an error
#[test]
fn test_non_gzip_data_returns_error() {
    // Random bytes that are definitely not valid gzip
    let random_bytes: Vec<u8> = (0..100).map(|i| (i * 37 + 13) as u8).collect();

    let result = decompress_gzip(&random_bytes);
    assert!(
        result.is_err(),
        "decompress_gzip should return Err for non-gzip data"
    );
}

// ---- Key sharding tests (no backends needed) ----

/// Helper struct to test key transformation without needing a full S3CompatClient
struct TestShardingClient {
    sharding: KeyShardingConfig,
}

impl TestShardingClient {
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
}

/// Sharding with depth=0 and enabled=true should return the raw hash unchanged
#[test]
fn test_sharding_depth_zero_enabled() {
    let client = TestShardingClient {
        sharding: KeyShardingConfig {
            enabled: true,
            depth: 0,
        },
    };
    let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

    let result = client.hash_to_s3_key(hash);
    assert_eq!(
        result, hash,
        "depth=0 with enabled=true should return the raw hash"
    );
}

/// Very large sharding depth (100) with a 64-char hash should fall back to raw hash
/// because the hash is too short (needs depth*2 = 200 chars but only has 64)
#[test]
fn test_sharding_very_large_depth() {
    let client = TestShardingClient {
        sharding: KeyShardingConfig {
            enabled: true,
            depth: 100,
        },
    };
    let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    assert_eq!(hash.len(), 64, "SHA256 hash should be 64 hex chars");

    let result = client.hash_to_s3_key(hash);
    assert_eq!(
        result, hash,
        "depth=100 with 64-char hash should fall back to raw hash (needs 200 chars)"
    );
}

// ---- list_files with backslash in path prefix (needs real backends) ----

/// Verify that list_files handles path prefixes containing backslashes correctly.
/// Backslash is a LIKE escape character in SQL, so it needs special handling.
#[tokio::test(flavor = "multi_thread")]
async fn test_list_files_with_backslash_in_prefix() {
    if !common::is_s3_available() {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (config, _unique_id) = common::create_test_config("backslash-test");
    let kvstorage = s3dedup::kvstorage::KVStorage::new(&config).await.unwrap();
    kvstorage.setup().await.unwrap();

    let bucket = &config.bucket.name;

    // Insert files: one with backslash in path, one without
    kvstorage
        .set_modified(bucket, r"path\with\backslash\file.txt", 100)
        .await
        .unwrap();
    kvstorage
        .set_modified(bucket, "path/normal/file.txt", 100)
        .await
        .unwrap();

    // List with backslash prefix — should only match the backslash file
    let results = kvstorage
        .list_files(bucket, r"path\with\backslash", i64::MAX)
        .await
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "list_files with backslash prefix should match exactly 1 file, got: {:?}",
        results
    );
    assert_eq!(results[0], r"path\with\backslash\file.txt");

    // The normal prefix should not match the backslash file
    let normal_results = kvstorage
        .list_files(bucket, "path/normal", i64::MAX)
        .await
        .unwrap();
    assert_eq!(normal_results.len(), 1);
    assert_eq!(normal_results[0], "path/normal/file.txt");
}

// Note: "HEAD: FT returns invalid headers (missing Last-Modified)" is already covered by
// tests/filetracker_client_test.rs (test_head_missing_last_modified + test_download_missing_last_modified)

/// Verify that setup() migrates existing VARCHAR(255) columns to TEXT.
/// Creates tables with old VARCHAR(255) schema, runs setup(), checks columns are now TEXT.
#[tokio::test(flavor = "multi_thread")]
async fn test_varchar_to_text_migration() {
    if std::env::var("DATABASE_URL").is_err() {
        eprintln!("Skipping test: DATABASE_URL not set (requires PostgreSQL)");
        return;
    }

    let (config, _unique_id) = common::create_test_config("varcharmig");

    // We need direct pool access to create old-schema tables
    let pg_config = config.postgres.as_ref().unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://{}:{}@{}:{}/{}",
            pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.dbname
        ))
        .await
        .unwrap();

    // Derive table names the same way Postgres::table_name does
    let safe_bucket = config.bucket.name.replace("-", "_");
    let refcount_table = format!("{}_refcount", safe_bucket);
    let modified_table = format!("{}_modified", safe_bucket);
    let ref_file_table = format!("{}_ref_file", safe_bucket);
    let logical_size_table = format!("{}_logical_size", safe_bucket);

    // Drop tables if they exist (clean slate)
    for table in [
        &refcount_table,
        &modified_table,
        &ref_file_table,
        &logical_size_table,
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {}", table))
            .execute(&pool)
            .await
            .unwrap();
    }
    let version_table = format!("{}_version", safe_bucket);
    sqlx::query(&format!("DROP TABLE IF EXISTS {}", version_table))
        .execute(&pool)
        .await
        .unwrap();

    // Create tables with OLD VARCHAR(255) schema
    sqlx::query(&format!(
        "CREATE TABLE {} (
            bucket VARCHAR(255) NOT NULL,
            hash VARCHAR(255) NOT NULL,
            refcount INT NOT NULL,
            PRIMARY KEY (bucket, hash)
        )",
        refcount_table
    ))
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(&format!(
        "CREATE TABLE {} (
            bucket VARCHAR(255) NOT NULL,
            path VARCHAR(255) NOT NULL,
            modified BIGINT NOT NULL,
            PRIMARY KEY (bucket, path)
        )",
        modified_table
    ))
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(&format!(
        "CREATE TABLE {} (
            bucket VARCHAR(255) NOT NULL,
            path VARCHAR(255) NOT NULL,
            hash VARCHAR(255) NOT NULL,
            PRIMARY KEY (bucket, path)
        )",
        ref_file_table
    ))
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(&format!(
        "CREATE TABLE {} (
            bucket VARCHAR(255) NOT NULL,
            hash VARCHAR(255) NOT NULL,
            logical_size BIGINT NOT NULL,
            compressed_size BIGINT,
            PRIMARY KEY (bucket, hash)
        )",
        logical_size_table
    ))
    .execute(&pool)
    .await
    .unwrap();

    // Verify columns are VARCHAR before migration
    let varchar_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns
         WHERE table_name = $1 AND data_type = 'character varying'",
    )
    .bind(&refcount_table)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        varchar_count.0, 2,
        "refcount table should have 2 VARCHAR columns before migration"
    );

    // Run setup() — should detect VARCHAR and migrate to TEXT
    let kvstorage = s3dedup::kvstorage::KVStorage::new(&config).await.unwrap();
    kvstorage.setup().await.unwrap();

    // Verify columns are now TEXT
    let varchar_count_after: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns
         WHERE table_name = $1 AND data_type = 'character varying'",
    )
    .bind(&refcount_table)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        varchar_count_after.0, 0,
        "refcount table should have 0 VARCHAR columns after migration"
    );

    let text_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns
         WHERE table_name = $1 AND data_type = 'text'",
    )
    .bind(&refcount_table)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        text_count.0, 2,
        "refcount table should have 2 TEXT columns after migration"
    );

    // Verify data operations work after migration
    let bucket = &config.bucket.name;
    kvstorage
        .set_modified(bucket, "migrated/file.txt", 999)
        .await
        .unwrap();
    let modified = kvstorage
        .get_modified(bucket, "migrated/file.txt")
        .await
        .unwrap();
    assert_eq!(modified, 999);

    // Verify long paths work (would fail with VARCHAR(255))
    let long_path = format!("migrated/{}/file.txt", "x".repeat(300));
    kvstorage
        .set_modified(bucket, &long_path, 1000)
        .await
        .unwrap();
    let modified = kvstorage.get_modified(bucket, &long_path).await.unwrap();
    assert_eq!(modified, 1000);

    // Cleanup
    for table in [
        &refcount_table,
        &modified_table,
        &ref_file_table,
        &logical_size_table,
        &version_table,
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {}", table))
            .execute(&pool)
            .await
            .ok();
    }
}

// ---- PostgreSQL-specific tests (needs DB) ----

/// Verify that bucket names with hyphens are sanitized in table names
/// The table_name() method should replace hyphens with underscores
#[tokio::test(flavor = "multi_thread")]
async fn test_bucket_name_with_hyphens_table_sanitization() {
    if !common::is_s3_available() {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    // Create a config with hyphens in the bucket name
    let (config, _unique_id) = common::create_test_config("test-hyphen-bucket");

    // Confirm the bucket name contains hyphens
    assert!(
        config.bucket.name.contains('-'),
        "Test bucket name should contain hyphens: {}",
        config.bucket.name
    );

    // Create KV storage and run setup - this should succeed even with hyphens
    // because table_name() sanitizes them to underscores
    let kvstorage = s3dedup::kvstorage::KVStorage::new(&config).await.unwrap();
    kvstorage.setup().await.unwrap();

    // Perform basic operations to verify the sanitized table names work
    let bucket = &config.bucket.name;

    // set_ref_file + get_ref_file
    kvstorage
        .set_ref_file(bucket, "test-file.txt", "testhash123")
        .await
        .unwrap();
    let hash = kvstorage
        .get_ref_file(bucket, "test-file.txt")
        .await
        .unwrap();
    assert_eq!(hash, "testhash123");

    // atomic_increment_ref_count + get_ref_count
    let rc = kvstorage
        .atomic_increment_ref_count(bucket, "testhash123")
        .await
        .unwrap();
    assert_eq!(rc, 1);

    let rc = kvstorage
        .get_ref_count(bucket, "testhash123")
        .await
        .unwrap();
    assert_eq!(rc, 1);

    // set_modified + get_modified
    kvstorage
        .set_modified(bucket, "test-file.txt", 12345)
        .await
        .unwrap();
    let modified = kvstorage
        .get_modified(bucket, "test-file.txt")
        .await
        .unwrap();
    assert_eq!(modified, 12345);

    // set_logical_size + get_logical_size
    kvstorage
        .set_logical_size(bucket, "testhash123", 42)
        .await
        .unwrap();
    let size = kvstorage
        .get_logical_size(bucket, "testhash123")
        .await
        .unwrap();
    assert_eq!(size, 42);
}
