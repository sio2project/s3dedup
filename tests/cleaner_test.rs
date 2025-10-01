use s3dedup::cleaner::{Cleaner, CleanerConfig};
use s3dedup::config::BucketConfig;
use s3dedup::kvstorage::KVStorage;
use s3dedup::s3storage::S3Storage;
use std::sync::Arc;
use tokio::sync::Mutex;

// Helper to check if MinIO is available
async fn is_minio_available() -> bool {
    let bucket_config = create_test_bucket_config("health_check");
    S3Storage::new(&bucket_config).await.is_ok()
}

// Helper to create test bucket config
fn create_test_bucket_config(bucket_name: &str) -> BucketConfig {
    let config_str = format!(
        r#"{{
            "name": "{}",
            "address": "0.0.0.0",
            "port": 3000,
            "kvstorage_type": "sqlite",
            "sqlite": {{
                "path": ":memory:",
                "pool_size": 5
            }},
            "locks_type": "memory",
            "s3storage_type": "minio",
            "minio": {{
                "endpoint": "http://localhost:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
                "force_path_style": true
            }}
        }}"#,
        bucket_name
    );

    serde_json::from_str(&config_str).unwrap()
}

// Helper to setup test environment
async fn setup_test_env(
    bucket_name: &str,
) -> (
    Arc<Mutex<Box<KVStorage>>>,
    Arc<Mutex<Box<S3Storage>>>,
    String,
) {
    let bucket_config = create_test_bucket_config(bucket_name);

    let kvstorage = KVStorage::new(&bucket_config).await.unwrap();
    let s3storage = S3Storage::new(&bucket_config).await.unwrap();

    let kvstorage = Arc::new(Mutex::new(kvstorage));
    let s3storage = Arc::new(Mutex::new(s3storage));

    // Setup KV storage
    kvstorage.lock().await.setup().await.unwrap();

    (kvstorage, s3storage, bucket_name.to_string())
}

#[tokio::test]
async fn test_cleaner_config_defaults() {
    let config = CleanerConfig::default();
    assert!(!config.enabled);
    assert_eq!(config.interval_seconds, 3600);
    assert_eq!(config.batch_size, 1000);
    assert_eq!(config.max_deletes_per_run, 10000);
}

#[tokio::test]
async fn test_cleaner_config_deserialization() {
    let json = r#"{
        "enabled": true,
        "interval_seconds": 7200,
        "batch_size": 500,
        "max_deletes_per_run": 5000
    }"#;

    let config: CleanerConfig = serde_json::from_str(json).unwrap();
    assert!(config.enabled);
    assert_eq!(config.interval_seconds, 7200);
    assert_eq!(config.batch_size, 500);
    assert_eq!(config.max_deletes_per_run, 5000);
}

#[tokio::test]
async fn test_cleaner_config_partial_deserialization() {
    // Test that defaults work when fields are missing
    let json = r#"{"enabled": true}"#;
    let config: CleanerConfig = serde_json::from_str(json).unwrap();
    assert!(config.enabled);
    assert_eq!(config.interval_seconds, 3600); // default
    assert_eq!(config.batch_size, 1000); // default
}

#[tokio::test]
async fn test_clean_orphaned_ref_files() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) = setup_test_env("test_clean_orphaned_ref_files").await;

    // Create ref_file entries without corresponding refcounts
    kvstorage
        .lock()
        .await
        .set_ref_file(&bucket_name, "file1.txt", "hash1")
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_modified(&bucket_name, "file1.txt", 1000)
        .await
        .unwrap();

    kvstorage
        .lock()
        .await
        .set_ref_file(&bucket_name, "file2.txt", "hash2")
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_modified(&bucket_name, "file2.txt", 2000)
        .await
        .unwrap();

    // Verify ref_files exist
    let hash1 = kvstorage
        .lock()
        .await
        .get_ref_file(&bucket_name, "file1.txt")
        .await
        .unwrap();
    assert_eq!(hash1, "hash1");

    // Run cleaner
    let config = CleanerConfig {
        enabled: false, // We'll call run_cleanup directly
        interval_seconds: 1,
        batch_size: 10,
        max_deletes_per_run: 100,
    };

    let cleaner = Cleaner::new(bucket_name.clone(), kvstorage.clone(), s3storage, config);
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // Verify ref_files were cleaned up
    let hash1 = kvstorage
        .lock()
        .await
        .get_ref_file(&bucket_name, "file1.txt")
        .await
        .unwrap();
    assert_eq!(hash1, ""); // Should be deleted

    let modified1 = kvstorage
        .lock()
        .await
        .get_modified(&bucket_name, "file1.txt")
        .await
        .unwrap();
    assert_eq!(modified1, 0); // Should be deleted
}

#[tokio::test]
async fn test_clean_unreferenced_refcounts() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) =
        setup_test_env("test_clean_unreferenced_refcounts").await;

    // Create refcount entries without corresponding ref_files
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "hash1", 1)
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "hash2", 2)
        .await
        .unwrap();

    // Create one ref_file that points to hash3 (which will have a refcount)
    kvstorage
        .lock()
        .await
        .set_ref_file(&bucket_name, "file3.txt", "hash3")
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "hash3", 1)
        .await
        .unwrap();

    // Verify refcounts exist
    let count1 = kvstorage
        .lock()
        .await
        .get_ref_count(&bucket_name, "hash1")
        .await
        .unwrap();
    assert_eq!(count1, 1);

    // Run cleaner
    let config = CleanerConfig {
        enabled: false,
        interval_seconds: 1,
        batch_size: 10,
        max_deletes_per_run: 100,
    };

    let cleaner = Cleaner::new(bucket_name.clone(), kvstorage.clone(), s3storage, config);
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // Verify unreferenced refcounts were cleaned up
    let count1 = kvstorage
        .lock()
        .await
        .get_ref_count(&bucket_name, "hash1")
        .await
        .unwrap();
    assert_eq!(count1, 0); // Should be deleted

    let count2 = kvstorage
        .lock()
        .await
        .get_ref_count(&bucket_name, "hash2")
        .await
        .unwrap();
    assert_eq!(count2, 0); // Should be deleted

    // hash3 should still exist because it has a ref_file pointing to it
    let count3 = kvstorage
        .lock()
        .await
        .get_ref_count(&bucket_name, "hash3")
        .await
        .unwrap();
    assert_eq!(count3, 1); // Should still exist
}

#[tokio::test]
async fn test_clean_unused_s3_objects() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) = setup_test_env("test_clean_unused_s3_objects").await;

    // Upload objects to S3
    s3storage
        .lock()
        .await
        .put_object("hash1", vec![1, 2, 3])
        .await
        .unwrap();
    s3storage
        .lock()
        .await
        .put_object("hash2", vec![4, 5, 6])
        .await
        .unwrap();
    s3storage
        .lock()
        .await
        .put_object("hash3", vec![7, 8, 9])
        .await
        .unwrap();

    // Create refcount for hash3 only
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "hash3", 1)
        .await
        .unwrap();

    // Verify objects exist
    let exists1 = s3storage.lock().await.object_exists("hash1").await.unwrap();
    assert!(exists1);

    // Run cleaner
    let config = CleanerConfig {
        enabled: false,
        interval_seconds: 1,
        batch_size: 10,
        max_deletes_per_run: 100,
    };

    let cleaner = Cleaner::new(
        bucket_name.clone(),
        kvstorage.clone(),
        s3storage.clone(),
        config,
    );
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // Verify unused S3 objects were cleaned up
    let exists1 = s3storage.lock().await.object_exists("hash1").await.unwrap();
    assert!(!exists1); // Should be deleted

    let exists2 = s3storage.lock().await.object_exists("hash2").await.unwrap();
    assert!(!exists2); // Should be deleted

    // hash3 should still exist because it has a refcount
    let exists3 = s3storage.lock().await.object_exists("hash3").await.unwrap();
    assert!(exists3); // Should still exist
}

#[tokio::test]
async fn test_clean_orphaned_logical_sizes() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) =
        setup_test_env("test_clean_orphaned_logical_sizes").await;

    // Create logical_size entries without corresponding refcounts
    kvstorage
        .lock()
        .await
        .set_logical_size(&bucket_name, "hash1", 100)
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_logical_size(&bucket_name, "hash2", 200)
        .await
        .unwrap();

    // Create one with a refcount
    kvstorage
        .lock()
        .await
        .set_logical_size(&bucket_name, "hash3", 300)
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "hash3", 1)
        .await
        .unwrap();

    // Verify logical_sizes exist
    let size1 = kvstorage
        .lock()
        .await
        .get_logical_size(&bucket_name, "hash1")
        .await
        .unwrap();
    assert_eq!(size1, 100);

    // Run cleaner
    let config = CleanerConfig {
        enabled: false,
        interval_seconds: 1,
        batch_size: 10,
        max_deletes_per_run: 100,
    };

    let cleaner = Cleaner::new(bucket_name.clone(), kvstorage.clone(), s3storage, config);
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // Verify orphaned logical_sizes were cleaned up
    let size1 = kvstorage
        .lock()
        .await
        .get_logical_size(&bucket_name, "hash1")
        .await
        .unwrap();
    assert_eq!(size1, 0); // Should be deleted

    let size2 = kvstorage
        .lock()
        .await
        .get_logical_size(&bucket_name, "hash2")
        .await
        .unwrap();
    assert_eq!(size2, 0); // Should be deleted

    // hash3 should still exist because it has a refcount
    let size3 = kvstorage
        .lock()
        .await
        .get_logical_size(&bucket_name, "hash3")
        .await
        .unwrap();
    assert_eq!(size3, 300); // Should still exist
}

#[tokio::test]
async fn test_max_deletes_per_run_limit() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) =
        setup_test_env("test_max_deletes_per_run_limit").await;

    // Create many orphaned ref_files (more than max_deletes_per_run)
    for i in 0..20 {
        kvstorage
            .lock()
            .await
            .set_ref_file(
                &bucket_name,
                &format!("file{}.txt", i),
                &format!("hash{}", i),
            )
            .await
            .unwrap();
        kvstorage
            .lock()
            .await
            .set_modified(&bucket_name, &format!("file{}.txt", i), 1000 + i as i64)
            .await
            .unwrap();
    }

    // Run cleaner with low max_deletes_per_run
    let config = CleanerConfig {
        enabled: false,
        interval_seconds: 1,
        batch_size: 10,
        max_deletes_per_run: 5, // Only allow 5 deletes
    };

    let cleaner = Cleaner::new(bucket_name.clone(), kvstorage.clone(), s3storage, config);
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // Count how many ref_files still exist
    let mut remaining = 0;
    for i in 0..20 {
        let hash = kvstorage
            .lock()
            .await
            .get_ref_file(&bucket_name, &format!("file{}.txt", i))
            .await
            .unwrap();
        if !hash.is_empty() {
            remaining += 1;
        }
    }

    // Should have deleted exactly 5, leaving 15
    assert_eq!(remaining, 15);
}

#[tokio::test]
async fn test_batched_processing() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) = setup_test_env("test_batched_processing").await;

    // Create more entries than batch_size
    for i in 0..25 {
        kvstorage
            .lock()
            .await
            .set_ref_file(
                &bucket_name,
                &format!("file{}.txt", i),
                &format!("hash{}", i),
            )
            .await
            .unwrap();
        kvstorage
            .lock()
            .await
            .set_modified(&bucket_name, &format!("file{}.txt", i), 1000 + i as i64)
            .await
            .unwrap();
    }

    // Run cleaner with small batch size
    let config = CleanerConfig {
        enabled: false,
        interval_seconds: 1,
        batch_size: 5, // Small batch size to test pagination
        max_deletes_per_run: 100,
    };

    let cleaner = Cleaner::new(bucket_name.clone(), kvstorage.clone(), s3storage, config);
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // All ref_files should be cleaned (none have refcounts)
    for i in 0..25 {
        let hash = kvstorage
            .lock()
            .await
            .get_ref_file(&bucket_name, &format!("file{}.txt", i))
            .await
            .unwrap();
        assert_eq!(hash, "", "file{}.txt should be cleaned", i);
    }
}

#[tokio::test]
async fn test_full_cleanup_cycle() {
    if !is_minio_available().await {
        eprintln!("Skipping test: MinIO not available");
        return;
    }

    let (kvstorage, s3storage, bucket_name) = setup_test_env("test_full_cleanup_cycle").await;

    // Scenario: Simulate a crash during PUT operation
    // 1. S3 object uploaded
    s3storage
        .lock()
        .await
        .put_object("crash_hash", vec![1, 2, 3, 4, 5])
        .await
        .unwrap();

    // 2. Refcount incremented
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "crash_hash", 1)
        .await
        .unwrap();

    // 3. Logical size set
    kvstorage
        .lock()
        .await
        .set_logical_size(&bucket_name, "crash_hash", 5)
        .await
        .unwrap();

    // 4. Crash happens before ref_file and modified are set!
    // Now we have: S3 object ✓, refcount ✓, logical_size ✓, but NO ref_file

    // Also add a properly completed file
    s3storage
        .lock()
        .await
        .put_object("good_hash", vec![6, 7, 8])
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_ref_count(&bucket_name, "good_hash", 1)
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_logical_size(&bucket_name, "good_hash", 3)
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_ref_file(&bucket_name, "good_file.txt", "good_hash")
        .await
        .unwrap();
    kvstorage
        .lock()
        .await
        .set_modified(&bucket_name, "good_file.txt", 5000)
        .await
        .unwrap();

    // Run cleaner
    let config = CleanerConfig {
        enabled: false,
        interval_seconds: 1,
        batch_size: 10,
        max_deletes_per_run: 100,
    };

    let cleaner = Cleaner::new(
        bucket_name.clone(),
        kvstorage.clone(),
        s3storage.clone(),
        config,
    );
    let result = cleaner.run_cleanup().await;
    assert!(result.is_ok());

    // Verify crash_hash was cleaned up (no ref_file points to it)
    let crash_refcount = kvstorage
        .lock()
        .await
        .get_ref_count(&bucket_name, "crash_hash")
        .await
        .unwrap();
    assert_eq!(crash_refcount, 0, "crash_hash refcount should be deleted");

    let crash_size = kvstorage
        .lock()
        .await
        .get_logical_size(&bucket_name, "crash_hash")
        .await
        .unwrap();
    assert_eq!(crash_size, 0, "crash_hash logical_size should be deleted");

    // Note: S3 object cleanup happens in phase 3 after refcount is cleaned in phase 2
    // So it won't be cleaned in the same run. In a real scenario, next run would clean it.

    // Verify good_hash is still intact
    let good_refcount = kvstorage
        .lock()
        .await
        .get_ref_count(&bucket_name, "good_hash")
        .await
        .unwrap();
    assert_eq!(good_refcount, 1, "good_hash refcount should remain");

    let good_size = kvstorage
        .lock()
        .await
        .get_logical_size(&bucket_name, "good_hash")
        .await
        .unwrap();
    assert_eq!(good_size, 3, "good_hash logical_size should remain");

    let good_exists = s3storage
        .lock()
        .await
        .object_exists("good_hash")
        .await
        .unwrap();
    assert!(good_exists, "good_hash S3 object should remain");
}
