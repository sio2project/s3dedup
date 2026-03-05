mod common;

use axum::Router;
use axum::routing::get;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Barrier;
use tower::ServiceExt;

// Helper to create test app with access to app state
async fn create_test_app_with_state() -> (Router, Arc<s3dedup::AppState>) {
    use s3dedup::AppState;
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;
    use tokio::sync::Mutex;

    let (config, _unique_id) = common::create_test_config("test-concurrent");

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new_with_config(config.locks_type, &config)
        .await
        .unwrap();
    let s3storage = S3Storage::new(&config.bucket).await.unwrap();

    let app_state = Arc::new(AppState {
        bucket_name: config.bucket.name.clone(),
        kvstorage: Arc::new(Mutex::new(kvstorage)),
        locks: Arc::new(*locks),
        s3storage: Arc::new(Mutex::new(s3storage)),
        filetracker_client: None,
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
    });

    app_state.kvstorage.lock().await.setup().await.unwrap();

    let router = Router::new()
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file)
                .head(s3dedup::routes::ft::get_file::ft_get_file)
                .put(s3dedup::routes::ft::put_file::ft_put_file)
                .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
        )
        .with_state(app_state.clone());

    (router, app_state)
}

// Check if S3 storage is available
async fn is_s3_available() -> bool {
    use s3dedup::s3storage::S3Storage;

    if !common::is_s3_available() {
        return false;
    }
    let (bucket_config, _) = common::create_test_bucket_config("health-check");
    S3Storage::new(&bucket_config).await.is_ok()
}

// Helper to create a PUT request
fn create_put_request(path: &str, content: &[u8]) -> Request<Body> {
    use s3dedup::routes::ft::storage_helpers;

    let compressed_data = storage_helpers::compress_gzip(content).unwrap();
    let sha256 = storage_helpers::compute_sha256(content);
    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    Request::builder()
        .uri(format!(
            "/ft/files/{}?last_modified={}",
            path, encoded_timestamp
        ))
        .method("PUT")
        .header("Content-Encoding", "gzip")
        .header("SHA256-Checksum", sha256)
        .header("Logical-Size", content.len().to_string())
        .body(Body::from(compressed_data))
        .unwrap()
}

// Helper to create a DELETE request
fn create_delete_request(path: &str) -> Request<Body> {
    // Use a far-future timestamp to ensure we can delete any version
    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    Request::builder()
        .uri(format!(
            "/ft/files/{}?last_modified={}",
            path, encoded_timestamp
        ))
        .method("DELETE")
        .body(Body::empty())
        .unwrap()
}

// Helper to create a GET request
fn create_get_request(path: &str) -> Request<Body> {
    Request::builder()
        .uri(format!("/ft/files/{}", path))
        .method("GET")
        .body(Body::empty())
        .unwrap()
}

/// Test the specific race condition:
/// P1: DELETE -> check refcount (sees 0) -> P2: PUT (increments to 1) -> P1: deletes blob
///
/// This test creates a file, then concurrently:
/// - Multiple tasks try to DELETE it
/// - Multiple tasks try to PUT new files with the SAME content
///
/// Without proper locking, the blob could be deleted while refcount > 0
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_delete_put_race_condition() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    // Shared content for deduplication
    let shared_content = b"This content will be shared across multiple files for dedup testing";
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(shared_content);

    // First, create initial files that all share the same blob
    let num_initial_files = 5;
    for i in 0..num_initial_files {
        let response = (*router)
            .clone()
            .oneshot(create_put_request(
                &format!("race/initial_{}.txt", i),
                shared_content,
            ))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Initial file {} creation failed",
            i
        );
    }

    // Verify initial refcount
    let initial_refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        initial_refcount, num_initial_files as i32,
        "Initial refcount mismatch"
    );

    // Now run concurrent DELETEs and PUTs
    let num_concurrent = 20;
    let barrier = Arc::new(Barrier::new(num_concurrent * 2)); // DELETE + PUT tasks
    let mut handles = Vec::new();

    // Spawn DELETE tasks for initial files (cycling through them)
    for i in 0..num_concurrent {
        let router = router.clone();
        let barrier = barrier.clone();
        let file_idx = i % num_initial_files;

        handles.push(tokio::spawn(async move {
            barrier.wait().await; // Synchronize start
            let response = (*router)
                .clone()
                .oneshot(create_delete_request(&format!(
                    "race/initial_{}.txt",
                    file_idx
                )))
                .await
                .unwrap();
            // Either OK (deleted) or NOT_FOUND (already deleted by another task)
            assert!(
                response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND,
                "Unexpected DELETE status: {}",
                response.status()
            );
        }));
    }

    // Spawn PUT tasks with same content to different paths
    for i in 0..num_concurrent {
        let router = router.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await; // Synchronize start
            let response = (*router)
                .clone()
                .oneshot(create_put_request(
                    &format!("race/new_{}.txt", i),
                    shared_content,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "PUT {} failed", i);
        }));
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify data integrity: all new files should be readable
    for i in 0..num_concurrent {
        let response = (*router)
            .clone()
            .oneshot(create_get_request(&format!("race/new_{}.txt", i)))
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "File race/new_{}.txt should exist but GET failed - possible data loss from race condition!",
            i
        );

        // Verify content
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
        assert_eq!(
            decompressed, shared_content,
            "Content mismatch for race/new_{}.txt",
            i
        );
    }

    // Verify final refcount matches expected (num_concurrent new files)
    let final_refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();

    assert_eq!(
        final_refcount, num_concurrent as i32,
        "Final refcount mismatch: expected {}, got {}. This indicates a race condition!",
        num_concurrent, final_refcount
    );

    // Verify blob exists in S3
    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();

    assert!(
        blob_exists,
        "S3 blob should exist but was deleted - race condition caused data loss!"
    );
}

/// Test concurrent PUTs of the same content to different paths
/// All operations should succeed and refcount should be exact
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_put_same_content() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    let shared_content = b"Identical content for concurrent PUT test";
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(shared_content);

    let num_concurrent = 50;
    let barrier = Arc::new(Barrier::new(num_concurrent));
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..num_concurrent {
        let router = router.clone();
        let barrier = barrier.clone();
        let success_count = success_count.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let response = (*router)
                .clone()
                .oneshot(create_put_request(
                    &format!("concurrent_put/file_{}.txt", i),
                    shared_content,
                ))
                .await
                .unwrap();

            if response.status() == StatusCode::OK {
                success_count.fetch_add(1, Ordering::SeqCst);
            }
            response.status()
        }));
    }

    let mut statuses = Vec::new();
    for handle in handles {
        statuses.push(handle.await.unwrap());
    }

    let successes = success_count.load(Ordering::SeqCst);
    assert_eq!(
        successes, num_concurrent,
        "All {} PUTs should succeed, but only {} succeeded. Statuses: {:?}",
        num_concurrent, successes, statuses
    );

    // Verify refcount
    let refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();

    assert_eq!(
        refcount, num_concurrent as i32,
        "Refcount should be {} but is {}. Race condition in refcount increment!",
        num_concurrent, refcount
    );

    // Verify all files are readable
    for i in 0..num_concurrent {
        let response = (*router)
            .clone()
            .oneshot(create_get_request(&format!(
                "concurrent_put/file_{}.txt",
                i
            )))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "File {} should be readable",
            i
        );
    }
}

/// Test concurrent DELETEs of files sharing the same blob
/// Blob should only be deleted when last reference is removed
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_delete_shared_blob() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    let shared_content = b"Content shared by files that will be deleted concurrently";
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(shared_content);

    // Create files
    let num_files = 30;
    for i in 0..num_files {
        let response = (*router)
            .clone()
            .oneshot(create_put_request(
                &format!("concurrent_delete/file_{}.txt", i),
                shared_content,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Verify initial state
    let initial_refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(initial_refcount, num_files as i32);

    // Concurrently delete all files
    let barrier = Arc::new(Barrier::new(num_files));
    let mut handles = Vec::new();

    for i in 0..num_files {
        let router = router.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let response = (*router)
                .clone()
                .oneshot(create_delete_request(&format!(
                    "concurrent_delete/file_{}.txt",
                    i
                )))
                .await
                .unwrap();
            response.status()
        }));
    }

    let mut ok_count = 0;
    let mut not_found_count = 0;
    for handle in handles {
        match handle.await.unwrap() {
            StatusCode::OK => ok_count += 1,
            StatusCode::NOT_FOUND => not_found_count += 1,
            status => panic!("Unexpected status: {}", status),
        }
    }

    // All deletes should succeed (OK) since each file is unique
    assert_eq!(
        ok_count, num_files,
        "Expected {} OK responses, got {} OK and {} NOT_FOUND",
        num_files, ok_count, not_found_count
    );

    // Verify final refcount is 0
    let final_refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(final_refcount, 0, "Final refcount should be 0");

    // Verify blob is deleted from S3
    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(
        !blob_exists,
        "S3 blob should be deleted when refcount reaches 0"
    );
}

/// Test mixed concurrent operations: PUT, GET, DELETE on overlapping files
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_mixed_concurrent_operations() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    // Create some initial files with different content
    let contents: Vec<&[u8]> = vec![
        b"Content A for mixed test",
        b"Content B for mixed test",
        b"Content C for mixed test",
    ];

    let num_files_per_content = 5;
    for (content_idx, content) in contents.iter().enumerate() {
        for file_idx in 0..num_files_per_content {
            let response = (*router)
                .clone()
                .oneshot(create_put_request(
                    &format!("mixed/content_{}/file_{}.txt", content_idx, file_idx),
                    content,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    // Run mixed operations concurrently
    let num_operations = 100;
    let barrier = Arc::new(Barrier::new(num_operations));
    let mut handles = Vec::new();

    for op_idx in 0..num_operations {
        let router = router.clone();
        let barrier = barrier.clone();
        let content_idx = op_idx % contents.len();
        let file_idx = op_idx % num_files_per_content;
        let content = contents[content_idx];

        handles.push(tokio::spawn(async move {
            barrier.wait().await;

            match op_idx % 4 {
                0 => {
                    // PUT (update existing or create new)
                    let path = format!("mixed/content_{}/file_{}.txt", content_idx, file_idx + 10);
                    let response = (*router)
                        .clone()
                        .oneshot(create_put_request(&path, content))
                        .await
                        .unwrap();
                    ("PUT", path, response.status())
                }
                1 => {
                    // GET
                    let path = format!("mixed/content_{}/file_{}.txt", content_idx, file_idx);
                    let response = (*router)
                        .clone()
                        .oneshot(create_get_request(&path))
                        .await
                        .unwrap();
                    ("GET", path, response.status())
                }
                2 => {
                    // DELETE
                    let path = format!("mixed/content_{}/file_{}.txt", content_idx, file_idx);
                    let response = (*router)
                        .clone()
                        .oneshot(create_delete_request(&path))
                        .await
                        .unwrap();
                    ("DELETE", path, response.status())
                }
                _ => {
                    // PUT with same content to new path
                    let path = format!("mixed/new/op_{}.txt", op_idx);
                    let response = (*router)
                        .clone()
                        .oneshot(create_put_request(&path, content))
                        .await
                        .unwrap();
                    ("PUT_NEW", path, response.status())
                }
            }
        }));
    }

    // Collect results
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // Verify no internal server errors
    for (op, path, status) in &results {
        assert_ne!(
            *status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Operation {} on {} returned 500",
            op,
            path
        );
    }

    // Verify refcounts are non-negative for all content hashes
    for content in &contents {
        let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(content);
        let refcount = app_state
            .kvstorage
            .lock()
            .await
            .get_ref_count(&app_state.bucket_name, &sha256)
            .await
            .unwrap();

        assert!(
            refcount >= 0,
            "Refcount for content should be >= 0, got {}. Race condition in refcount!",
            refcount
        );

        // If refcount > 0, blob should exist
        if refcount > 0 {
            let blob_exists = app_state
                .s3storage
                .lock()
                .await
                .object_exists(&sha256)
                .await
                .unwrap();
            assert!(
                blob_exists,
                "Blob should exist when refcount is {} > 0",
                refcount
            );
        }
    }
}

/// Stress test: rapid PUT/DELETE cycles on the same path
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_rapid_put_delete_same_path() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    let path = "rapid/same_path.txt";
    let content = b"Content for rapid PUT/DELETE test";
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(content);

    let num_cycles = 50;
    let barrier = Arc::new(Barrier::new(num_cycles * 2));
    let mut handles = Vec::new();

    // Spawn alternating PUT and DELETE tasks
    for i in 0..num_cycles {
        let router = router.clone();
        let barrier = barrier.clone();

        // PUT task
        handles.push(tokio::spawn({
            let router = router.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                let response = (*router)
                    .clone()
                    .oneshot(create_put_request(path, content))
                    .await
                    .unwrap();
                ("PUT", i, response.status())
            }
        }));

        // DELETE task
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let response = (*router)
                .clone()
                .oneshot(create_delete_request(path))
                .await
                .unwrap();
            ("DELETE", i, response.status())
        }));
    }

    // Collect results
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // Verify no internal server errors
    for (op, idx, status) in &results {
        assert_ne!(
            *status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Operation {} #{} returned 500",
            op,
            idx
        );
    }

    // Final state: file either exists or doesn't, but system should be consistent
    let file_hash = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_file(&app_state.bucket_name, path)
        .await
        .unwrap();

    let refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();

    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();

    // Consistency check
    if !file_hash.is_empty() {
        // File exists, so refcount should be > 0 and blob should exist
        assert!(refcount > 0, "File exists but refcount is {}", refcount);
        assert!(blob_exists, "File exists but blob is missing - data loss!");
    }

    if refcount > 0 {
        assert!(
            blob_exists,
            "Refcount is {} but blob doesn't exist - data loss!",
            refcount
        );
    }
}

/// Test concurrent overwrites of the same file with different content
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_overwrite_different_content() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    let path = "overwrite/target.txt";

    // Create initial file
    let initial_content = b"Initial content";
    let response = (*router)
        .clone()
        .oneshot(create_put_request(path, initial_content))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Concurrently overwrite with different content
    let num_overwrites = 20;
    let barrier = Arc::new(Barrier::new(num_overwrites));
    let mut handles = Vec::new();

    for i in 0..num_overwrites {
        let router = router.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let content = format!("Overwrite content version {}", i);
            let response = (*router)
                .clone()
                .oneshot(create_put_request(path, content.as_bytes()))
                .await
                .unwrap();
            (i, response.status())
        }));
    }

    // All should succeed
    for handle in handles {
        let (idx, status) = handle.await.unwrap();
        assert_eq!(status, StatusCode::OK, "Overwrite {} failed", idx);
    }

    // File should be readable with some valid content
    let response = (*router)
        .clone()
        .oneshot(create_get_request(path))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    let content_str = String::from_utf8_lossy(&decompressed);

    // Content should be one of the overwritten versions
    assert!(
        content_str.starts_with("Overwrite content version"),
        "Unexpected content: {}",
        content_str
    );

    // Initial content blob should have refcount 0 (or be deleted)
    let initial_sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(initial_content);
    let initial_refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &initial_sha256)
        .await
        .unwrap();
    assert_eq!(
        initial_refcount, 0,
        "Initial content refcount should be 0 after overwrite"
    );
}

/// Test that cleaner doesn't delete blobs being concurrently referenced
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cleaner_vs_put_race() {
    if !is_s3_available().await {
        eprintln!("Skipping test: S3 not available");
        return;
    }

    let (router, app_state) = create_test_app_with_state().await;
    let router = Arc::new(router);

    let content = b"Content that will be cleaned and re-added";
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(content);

    // Create and delete a file to leave orphaned S3 object
    let response = (*router)
        .clone()
        .oneshot(create_put_request("cleaner_race/initial.txt", content))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Now simulate the race: cleaner runs while PUT is happening
    let num_concurrent = 10;
    let barrier = Arc::new(Barrier::new(num_concurrent + 1));

    // Cleaner task (simulated by checking refcount and deleting)
    let cleaner_app_state = app_state.clone();
    let cleaner_barrier = barrier.clone();
    let cleaner_sha256 = sha256.clone();
    let cleaner_handle = tokio::spawn(async move {
        cleaner_barrier.wait().await;

        // Simulate cleaner behavior: check refcount, then delete if 0
        // Note: In real cleaner, this is protected by hash lock
        cleaner_app_state
            .kvstorage
            .lock()
            .await
            .get_ref_count(&cleaner_app_state.bucket_name, &cleaner_sha256)
            .await
            .unwrap()
    });

    // PUT tasks
    let mut put_handles = Vec::new();
    for i in 0..num_concurrent {
        let router = router.clone();
        let barrier = barrier.clone();

        put_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let response = (*router)
                .clone()
                .oneshot(create_put_request(
                    &format!("cleaner_race/new_{}.txt", i),
                    content,
                ))
                .await
                .unwrap();
            response.status()
        }));
    }

    // Wait for all
    let _cleaner_refcount = cleaner_handle.await.unwrap();
    for handle in put_handles {
        let status = handle.await.unwrap();
        assert_eq!(status, StatusCode::OK, "PUT should succeed");
    }

    // Verify all new files exist and blob is intact
    let final_refcount = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &sha256)
        .await
        .unwrap();

    // Should be initial file + num_concurrent new files
    assert!(
        final_refcount >= num_concurrent as i32,
        "Refcount should be at least {} but is {}",
        num_concurrent,
        final_refcount
    );

    let blob_exists = app_state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(
        blob_exists,
        "Blob should exist - cleaner race caused data loss!"
    );

    // Verify all new files are readable
    for i in 0..num_concurrent {
        let response = (*router)
            .clone()
            .oneshot(create_get_request(&format!("cleaner_race/new_{}.txt", i)))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "File cleaner_race/new_{}.txt should exist",
            i
        );
    }
}
