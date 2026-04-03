//! Migration error path tests using mock KV/S3 infrastructure.
//! These tests call migration functions directly on an AppState with mock backends
//! to verify correct error propagation when individual operations fail.
//!
//! Run with: cargo test --features test-mocks --test migration_error_test

#![cfg(feature = "test-mocks")]

use s3dedup::AppState;
use s3dedup::filetracker_client::FileMetadata;
use s3dedup::kvstorage::KVStorage;
use s3dedup::kvstorage::mock::MockKVStorage;
use s3dedup::locks::LocksStorage;
use s3dedup::migration::migrate_single_file_from_metadata;
use s3dedup::s3storage::S3Storage;
use s3dedup::s3storage::mock::MockS3Storage;
use std::sync::Arc;

/// Build a test AppState with mock backends. Returns (AppState, MockKVStorage, MockS3Storage).
fn create_mock_app_state() -> (Arc<AppState>, MockKVStorage, MockS3Storage) {
    let mock_kv = MockKVStorage::new();
    let mock_s3 = MockS3Storage::new();

    let kvstorage = Arc::new(KVStorage::Mock(mock_kv.clone()));
    let locks = Arc::new(*LocksStorage::new(s3dedup::config::LocksType::Memory));
    let s3storage = Arc::new(S3Storage::Mock(mock_s3.clone()));
    let cleaner = Arc::new(s3dedup::cleaner::Cleaner::new(
        "test-bucket".to_string(),
        kvstorage.clone(),
        s3storage.clone(),
        locks.clone(),
        Default::default(),
    ));
    let app_state = Arc::new(AppState {
        bucket_name: "test-bucket".to_string(),
        kvstorage,
        locks,
        s3storage,
        filetracker_client: None,
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner,
    });

    (app_state, mock_kv, mock_s3)
}

/// Create a simple FileMetadata for testing. The data is uncompressed.
fn make_test_metadata(data: &[u8], timestamp: i64) -> FileMetadata {
    FileMetadata {
        data: data.to_vec(),
        last_modified: timestamp,
        logical_size: data.len(),
        is_compressed: false,
    }
}

// ============================================================
// 1. record_blob_metadata fails (set_logical_size) -> Err
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_migration_record_blob_metadata_fails() {
    let (app_state, mock_kv, _mock_s3) = create_mock_app_state();

    // Make set_logical_size fail — this is the first op in record_blob_metadata
    mock_kv.set_failing("set_logical_size");

    let metadata = make_test_metadata(b"hello world", 1700010000);
    let result =
        migrate_single_file_from_metadata(&app_state, "test/blob_meta_fail.txt", metadata).await;

    assert!(
        result.is_err(),
        "Migration should fail when set_logical_size fails"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("set_logical_size"),
        "Error should mention set_logical_size, got: {}",
        err_msg
    );
}

// ============================================================
// 2. update_file_ref fails (set_ref_file) -> Err, orphaned blob
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_migration_update_file_ref_fails_orphaned_blob() {
    let (app_state, mock_kv, mock_s3) = create_mock_app_state();

    // Make set_ref_file fail — this happens inside update_file_ref, after blob upload
    mock_kv.set_failing("set_ref_file");

    let content = b"orphaned blob content";
    let metadata = make_test_metadata(content, 1700020000);

    let result =
        migrate_single_file_from_metadata(&app_state, "test/orphan_blob.txt", metadata).await;

    assert!(
        result.is_err(),
        "Migration should fail when set_ref_file fails"
    );

    // The blob should still exist in S3 (orphaned) because the upload succeeded
    // before set_ref_file was called
    let digest = s3dedup::routes::ft::storage_helpers::compute_sha256(content);
    let blob_exists = mock_s3.object_exists(&digest).await.unwrap();
    assert!(
        blob_exists,
        "Blob should still exist in S3 (orphaned) after set_ref_file failure"
    );
}

// ============================================================
// 3. decrement_old_ref fails -> Err
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_migration_decrement_old_ref_fails() {
    let (app_state, mock_kv, _mock_s3) = create_mock_app_state();

    // First, put an existing file so there's an old_hash to decrement
    let old_content = b"old content for decrement test";
    let old_digest = s3dedup::routes::ft::storage_helpers::compute_sha256(old_content);
    let old_timestamp = 1700030000i64;

    // Simulate existing file state in KV: set modified, ref_file, and refcount
    app_state
        .kvstorage
        .set_modified("test-bucket", "test/decrement_fail.txt", old_timestamp)
        .await
        .unwrap();
    app_state
        .kvstorage
        .set_ref_file("test-bucket", "test/decrement_fail.txt", &old_digest)
        .await
        .unwrap();
    app_state
        .kvstorage
        .atomic_increment_ref_count("test-bucket", &old_digest)
        .await
        .unwrap();

    // Upload old blob to S3
    let old_compressed = s3dedup::routes::ft::storage_helpers::compress_gzip(old_content).unwrap();
    mock_kv.clear_failing("atomic_decrement_ref_count"); // make sure not failing yet
    app_state
        .s3storage
        .put_object(&old_digest, old_compressed)
        .await
        .unwrap();

    // Now make atomic_decrement_ref_count fail
    mock_kv.set_failing("atomic_decrement_ref_count");

    // Migrate with new content (different hash triggers decrement_old_ref)
    let new_content = b"new content for decrement test";
    let new_metadata = make_test_metadata(new_content, 1700040000);

    let result =
        migrate_single_file_from_metadata(&app_state, "test/decrement_fail.txt", new_metadata)
            .await;

    assert!(
        result.is_err(),
        "Migration should fail when atomic_decrement_ref_count fails"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("atomic_decrement_ref_count"),
        "Error should mention atomic_decrement_ref_count, got: {}",
        err_msg
    );
}
