mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Response, StatusCode};
use axum::routing::get;
use s3dedup::AppState;
use s3dedup::filetracker_client::FiletrackerClient;
use s3dedup::migration::{migrate_all_files, migrate_all_files_from_file_list};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tower::util::ServiceExt;

// Type alias for file storage: (data, timestamp)
type FileStorage = Arc<Mutex<HashMap<String, (Vec<u8>, i64)>>>;

// Mock filetracker server state
#[derive(Clone)]
struct MockFiletrackerState {
    files: FileStorage,
}

impl MockFiletrackerState {
    fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn add_file(&self, path: &str, data: Vec<u8>) {
        let timestamp = chrono::Utc::now().timestamp();
        self.files
            .lock()
            .await
            .insert(path.to_string(), (data, timestamp));
    }
}

// Mock filetracker server handlers
async fn mock_ft_list(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
    path: Option<axum::extract::Path<String>>,
) -> Response<Body> {
    // Handle both empty path and non-empty path
    let _path = path.map(|p| p.0).unwrap_or_default();

    let files = state.files.lock().await;
    let file_list: Vec<String> = files.keys().cloned().collect();
    let response_body = file_list.join("\n");

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(if response_body.is_empty() {
            "".to_string()
        } else {
            response_body + "\n"
        }))
        .unwrap()
}

async fn mock_ft_get(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Response<Body> {
    let path = path.strip_prefix('/').unwrap_or(&path);
    let files = state.files.lock().await;

    match files.get(path) {
        Some((data, timestamp)) => {
            // Compress the data with gzip
            let compressed = s3dedup::routes::ft::storage_helpers::compress_gzip(data).unwrap();
            let last_modified = chrono::DateTime::from_timestamp(*timestamp, 0)
                .unwrap()
                .to_rfc2822();

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", compressed.len().to_string())
                .header("Content-Encoding", "gzip")
                .header("Last-Modified", last_modified)
                .header("Logical-Size", data.len().to_string())
                .body(Body::from(compressed))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

async fn mock_ft_put(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    body: Body,
) -> Response<Body> {
    let path = path.strip_prefix('/').unwrap_or(&path);
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();

    let timestamp = chrono::Utc::now().timestamp();
    state
        .files
        .lock()
        .await
        .insert(path.to_string(), (bytes.to_vec(), timestamp));

    let last_modified = chrono::DateTime::from_timestamp(timestamp, 0)
        .unwrap()
        .to_rfc2822();

    Response::builder()
        .status(StatusCode::OK)
        .header("Last-Modified", last_modified)
        .body(Body::empty())
        .unwrap()
}

async fn mock_ft_delete(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Response<Body> {
    let path = path.strip_prefix('/').unwrap_or(&path);
    let mut files = state.files.lock().await;

    match files.remove(path) {
        Some(_) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

// Helper to create a mock filetracker server
async fn create_mock_filetracker() -> (MockFiletrackerState, String) {
    let state = MockFiletrackerState::new();

    let app = Router::new()
        .route("/list/", get(mock_ft_list))
        .route("/list/{*path}", get(mock_ft_list))
        .route(
            "/files/{*path}",
            get(mock_ft_get).put(mock_ft_put).delete(mock_ft_delete),
        )
        .with_state(state.clone());

    // Find an available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    // Spawn the server
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (state, url)
}

// Helper to create test AppState
async fn create_test_app_state() -> Arc<AppState> {
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;

    let (config, _unique_id) = common::create_test_config("test-migration");

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new_with_config(config.locks_type, &config)
        .await
        .unwrap();
    let s3storage = S3Storage::new(&config.bucket).await.unwrap();

    let kvstorage = Arc::new(*kvstorage);
    let locks = Arc::new(*locks);
    let s3storage = Arc::new(*s3storage);
    let cleaner = Arc::new(s3dedup::cleaner::Cleaner::new(
        config.bucket.name.clone(),
        kvstorage.clone(),
        s3storage.clone(),
        locks.clone(),
        Default::default(),
    ));
    let app_state = Arc::new(AppState {
        bucket_name: config.bucket.name.clone(),
        kvstorage,
        locks,
        s3storage,
        filetracker_client: None,
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner,
    });

    app_state.kvstorage.setup().await.unwrap();

    app_state
}

#[tokio::test(flavor = "multi_thread")]
async fn test_offline_migration_empty() {
    let (_mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    let result = migrate_all_files(client, app_state, 5).await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 0);
    assert_eq!(stats.migrated, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.skipped, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_offline_migration_single_file() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add a test file to mock filetracker
    let test_data = b"Hello, migration test!";
    mock_state.add_file("test.txt", test_data.to_vec()).await;

    // Run migration
    let result = migrate_all_files(client.clone(), app_state.clone(), 5).await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 1);
    assert_eq!(stats.migrated, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.skipped, 0);

    // Verify file was migrated
    let modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "test.txt")
        .await
        .unwrap();
    assert!(modified > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_offline_migration_multiple_files() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add multiple test files
    for i in 0..10 {
        let data = format!("Test file {}", i);
        mock_state
            .add_file(&format!("file{}.txt", i), data.as_bytes().to_vec())
            .await;
    }

    // Run migration
    let result = migrate_all_files(client, app_state.clone(), 3).await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 10);
    assert_eq!(stats.migrated, 10);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.skipped, 0);

    // Verify all files were migrated
    for i in 0..10 {
        let modified = app_state
            .kvstorage
            .get_modified(&app_state.bucket_name, &format!("file{}.txt", i))
            .await
            .unwrap();
        assert!(modified > 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_offline_migration_skips_existing() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add test files
    mock_state.add_file("file1.txt", b"File 1".to_vec()).await;
    mock_state.add_file("file2.txt", b"File 2".to_vec()).await;

    // First migration
    let result = migrate_all_files(client.clone(), app_state.clone(), 5).await;
    assert!(result.is_ok());
    let stats = result.unwrap();
    assert_eq!(stats.migrated, 2);
    assert_eq!(stats.skipped, 0);

    // Second migration (should skip already migrated files)
    let result = migrate_all_files(client, app_state, 5).await;
    assert!(result.is_ok());
    let stats = result.unwrap();
    assert_eq!(stats.total_files, 2);
    assert_eq!(stats.migrated, 0);
    assert_eq!(stats.skipped, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_migration_deduplication() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add files with identical content (should deduplicate)
    let same_data = b"Same content";
    mock_state.add_file("file1.txt", same_data.to_vec()).await;
    mock_state.add_file("file2.txt", same_data.to_vec()).await;
    mock_state.add_file("file3.txt", same_data.to_vec()).await;

    // Run migration
    let result = migrate_all_files(client, app_state.clone(), 5).await;
    assert!(result.is_ok());

    // Verify all files exist
    for file in &["file1.txt", "file2.txt", "file3.txt"] {
        let modified = app_state
            .kvstorage
            .get_modified(&app_state.bucket_name, file)
            .await
            .unwrap();
        assert!(modified > 0);
    }

    // Verify they all point to the same hash
    let hash1 = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "file1.txt")
        .await
        .unwrap();
    let hash2 = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "file2.txt")
        .await
        .unwrap();
    let hash3 = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "file3.txt")
        .await
        .unwrap();

    assert_eq!(hash1, hash2);
    assert_eq!(hash2, hash3);

    // Verify reference count is 3
    let ref_count = app_state
        .kvstorage
        .get_ref_count(&app_state.bucket_name, &hash1)
        .await
        .unwrap();
    assert_eq!(ref_count, 3);
}

// Live migration tests

#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_get_fallback() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // Add a file to mock filetracker only (not in s3dedup yet)
    let test_data = b"File from filetracker";
    mock_state
        .add_file("fallback.txt", test_data.to_vec())
        .await;

    // Create app state with filetracker client (live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // Create router with live migration support
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    // Make GET request - should fallback to filetracker and migrate on-the-fly
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/fallback.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify file was migrated to s3dedup
    let modified = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "fallback.txt")
        .await
        .unwrap();
    assert!(modified > 0, "File should be migrated to s3dedup");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_put_dual_write() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // Create app state with filetracker client (live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // Create router with live migration support
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::put(s3dedup::routes::ft::put_file::ft_put_file),
        )
        .with_state(app_state_with_ft.clone());

    // Prepare test data
    let test_data = b"Dual write test data";
    let compressed = s3dedup::routes::ft::storage_helpers::compress_gzip(test_data).unwrap();
    let last_modified = chrono::Utc::now().to_rfc2822();

    // Make PUT request - should write to both s3dedup and filetracker
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(format!(
                    "/ft/files/dualwrite.txt?last_modified={}",
                    urlencoding::encode(&last_modified)
                ))
                .header("Content-Encoding", "gzip")
                .header("Logical-Size", test_data.len().to_string())
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify file is in s3dedup
    let modified_s3dedup = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "dualwrite.txt")
        .await
        .unwrap();
    assert!(modified_s3dedup > 0, "File should be in s3dedup");

    // Verify file is also in filetracker
    let files = mock_state.files.lock().await;
    assert!(
        files.contains_key("dualwrite.txt"),
        "File should also be in filetracker"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_delete_dual_delete() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // First, add file to both s3dedup and filetracker
    let test_data = b"File to delete";
    mock_state
        .add_file("todelete.txt", test_data.to_vec())
        .await;

    // Migrate the file to s3dedup
    let client = Arc::new(FiletrackerClient::new(url.clone()));
    migrate_all_files(client, app_state.clone(), 5)
        .await
        .unwrap();

    // Create app state with filetracker client (live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // Verify file exists in both before deletion
    let modified_before = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "todelete.txt")
        .await
        .unwrap();
    assert!(modified_before > 0);
    assert!(mock_state.files.lock().await.contains_key("todelete.txt"));

    // Create router with live migration support
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::delete(s3dedup::routes::ft::delete_file::ft_delete_file),
        )
        .with_state(app_state_with_ft.clone());

    let last_modified = chrono::Utc::now().to_rfc2822();

    // Make DELETE request - should delete from both s3dedup and filetracker
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/ft/files/todelete.txt?last_modified={}",
                    urlencoding::encode(&last_modified)
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify file is deleted from s3dedup
    let modified_after = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "todelete.txt")
        .await
        .unwrap();
    assert_eq!(modified_after, 0, "File should be deleted from s3dedup");

    // Verify file is also deleted from filetracker
    let files = mock_state.files.lock().await;
    assert!(
        !files.contains_key("todelete.txt"),
        "File should also be deleted from filetracker"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_get_not_found_in_both() {
    let (_mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // Create app state with filetracker client (live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // Create router with live migration support
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    // Make GET request for non-existent file - should return 404
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/nonexistent.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_get_fallback_response_data() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // Add a file with specific content to mock filetracker
    let test_data = b"Specific test content for proxying";
    mock_state
        .add_file("proxy_test.txt", test_data.to_vec())
        .await;

    // Create app state with filetracker client (live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // Create router with live migration support
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    // Make GET request - should fallback to filetracker
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/proxy_test.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify headers are correctly proxied
    let content_encoding = response.headers().get("content-encoding");
    assert!(content_encoding.is_some());
    assert_eq!(content_encoding.unwrap(), "gzip");

    let logical_size = response.headers().get("logical-size");
    assert!(logical_size.is_some());
    assert_eq!(
        logical_size.unwrap().to_str().unwrap(),
        test_data.len().to_string()
    );

    // Verify response body is the compressed data
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    // Decompress and verify content
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(&decompressed[..], test_data);
}

/// This test validates the critical deadlock fix in src/routes/ft/get_file.rs:66
///
/// Deadlock scenario WITHOUT the fix:
/// 1. ft_get_file acquires a SHARED lock on the file (get_file.rs:24)
/// 2. File not found in s3dedup (modified_time == 0), found in filetracker
/// 3. Calls migrate_single_file_from_metadata while STILL HOLDING the shared lock
/// 4. Migration tries to acquire EXCLUSIVE lock on the SAME file (migration/mod.rs:167)
/// 5. Exclusive lock waits for shared lock to release → DEADLOCK (RwLock semantics)
///
/// The fix (get_file.rs:66): drop(_guard) before calling migration
///
/// This test would HANG/TIMEOUT without the drop, proving the fix is necessary.
#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_get_no_deadlock_on_fallback() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // Add a file ONLY to filetracker (not in s3dedup) to trigger fallback migration
    let test_data = b"File that triggers on-the-fly migration";
    mock_state
        .add_file("deadlock_test.txt", test_data.to_vec())
        .await;

    // Create app state with filetracker client (enables live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // Create router
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    // Make GET request with timeout to detect deadlocks quickly
    // Without the drop(_guard) fix in get_file.rs:66, this would deadlock and timeout
    let response = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        app.oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/deadlock_test.txt")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("Request timed out - likely deadlock! Check that get_file.rs drops shared lock before migration")
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify file was successfully migrated to s3dedup
    let modified = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "deadlock_test.txt")
        .await
        .unwrap();
    assert!(modified > 0, "File should be migrated to s3dedup");

    // Verify response data is correct
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(&decompressed[..], test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_subsequent_get_from_s3dedup() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;

    // Add a file to mock filetracker
    let test_data = b"File for second GET test";
    mock_state
        .add_file("second_get.txt", test_data.to_vec())
        .await;

    // Create app state with filetracker client (live migration mode)
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    // First GET - should fallback to filetracker and migrate
    let app1 = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    let response1 = app1
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/second_get.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response1.status(), StatusCode::OK);

    // Remove file from mock filetracker to prove second GET comes from s3dedup
    mock_state.files.lock().await.remove("second_get.txt");

    // Second GET - should come from s3dedup (not filetracker)
    let app2 = Router::new()
        .route(
            "/ft/files/{*path}",
            axum::routing::get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    let response2 = app2
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/second_get.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response2.status(), StatusCode::OK);

    // Verify response body is still correct
    let body_bytes2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();

    let decompressed2 =
        s3dedup::routes::ft::storage_helpers::decompress_gzip(&body_bytes2).unwrap();
    assert_eq!(&decompressed2[..], test_data);
}

// ============================================================================
// File-list migration tests
// ============================================================================

/// Mock filetracker state that can simulate transient failures
#[derive(Clone)]
struct FlakyMockFiletrackerState {
    files: FileStorage,
    /// Number of GET requests to fail before succeeding
    fail_next_n: Arc<AtomicUsize>,
}

impl FlakyMockFiletrackerState {
    fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
            fail_next_n: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn add_file(&self, path: &str, data: Vec<u8>) {
        let timestamp = chrono::Utc::now().timestamp();
        self.files
            .lock()
            .await
            .insert(path.to_string(), (data, timestamp));
    }
}

async fn flaky_mock_ft_list(
    axum::extract::State(state): axum::extract::State<FlakyMockFiletrackerState>,
    path: Option<axum::extract::Path<String>>,
) -> Response<Body> {
    let _path = path.map(|p| p.0).unwrap_or_default();
    let files = state.files.lock().await;
    let file_list: Vec<String> = files.keys().cloned().collect();
    let response_body = file_list.join("\n");

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(if response_body.is_empty() {
            "".to_string()
        } else {
            response_body + "\n"
        }))
        .unwrap()
}

async fn flaky_mock_ft_get(
    axum::extract::State(state): axum::extract::State<FlakyMockFiletrackerState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Response<Body> {
    // Check if we should simulate a failure
    let should_fail = state
        .fail_next_n
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
            if n > 0 { Some(n - 1) } else { None }
        })
        .is_ok();
    if should_fail {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::empty())
            .unwrap();
    }

    let path = path.strip_prefix('/').unwrap_or(&path);
    let files = state.files.lock().await;

    match files.get(path) {
        Some((data, timestamp)) => {
            let compressed = s3dedup::routes::ft::storage_helpers::compress_gzip(data).unwrap();
            let last_modified = chrono::DateTime::from_timestamp(*timestamp, 0)
                .unwrap()
                .to_rfc2822();

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", compressed.len().to_string())
                .header("Content-Encoding", "gzip")
                .header("Last-Modified", last_modified)
                .header("Logical-Size", data.len().to_string())
                .body(Body::from(compressed))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

async fn create_flaky_mock_filetracker() -> (FlakyMockFiletrackerState, String) {
    let state = FlakyMockFiletrackerState::new();

    let app = Router::new()
        .route("/list/", get(flaky_mock_ft_list))
        .route("/list/{*path}", get(flaky_mock_ft_list))
        .route(
            "/files/{*path}",
            get(flaky_mock_ft_get)
                .put({
                    |axum::extract::State(state): axum::extract::State<
                        FlakyMockFiletrackerState,
                    >,
                     axum::extract::Path(path): axum::extract::Path<String>,
                     body: Body| async move {
                        let path = path.strip_prefix('/').unwrap_or(&path).to_string();
                        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                        let timestamp = chrono::Utc::now().timestamp();
                        state
                            .files
                            .lock()
                            .await
                            .insert(path, (bytes.to_vec(), timestamp));
                        let last_modified = chrono::DateTime::from_timestamp(timestamp, 0)
                            .unwrap()
                            .to_rfc2822();
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("Last-Modified", last_modified)
                            .body(Body::empty())
                            .unwrap()
                    }
                })
                .delete(
                    |axum::extract::State(state): axum::extract::State<
                        FlakyMockFiletrackerState,
                    >,
                     axum::extract::Path(path): axum::extract::Path<String>| async move {
                        let path = path.strip_prefix('/').unwrap_or(&path).to_string();
                        let mut files = state.files.lock().await;
                        match files.remove(&path) {
                            Some(_) => Response::builder()
                                .status(StatusCode::OK)
                                .body(Body::empty())
                                .unwrap(),
                            None => Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Body::empty())
                                .unwrap(),
                        }
                    },
                ),
        )
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (state, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file_list_migration() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add 3 files to mock filetracker
    mock_state
        .add_file("file_a.txt", b"content A".to_vec())
        .await;
    mock_state
        .add_file("file_b.txt", b"content B".to_vec())
        .await;
    mock_state
        .add_file("file_c.txt", b"content C".to_vec())
        .await;

    // Write file list with only 2 of the 3 files
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_list_path = tmp_dir.path().join("file_list.txt");
    std::fs::write(&file_list_path, "file_a.txt\nfile_b.txt\n").unwrap();

    // Run file-list migration
    let result = migrate_all_files_from_file_list(
        file_list_path.to_str().unwrap(),
        client,
        app_state.clone(),
        5,
    )
    .await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 2);
    assert_eq!(stats.migrated, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.skipped, 0);

    // Verify file_a and file_b were migrated
    let modified_a = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "file_a.txt")
        .await
        .unwrap();
    assert!(modified_a > 0);

    let modified_b = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "file_b.txt")
        .await
        .unwrap();
    assert!(modified_b > 0);

    // Verify file_c was NOT migrated
    let modified_c = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "file_c.txt")
        .await
        .unwrap();
    assert_eq!(modified_c, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file_list_migration_retries_on_ft_failure() {
    let (mock_state, url) = create_flaky_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add a file
    mock_state
        .add_file("retry_test.txt", b"retry content".to_vec())
        .await;

    // Make first 3 GET requests fail
    mock_state.fail_next_n.store(3, Ordering::SeqCst);

    // Write file list
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_list_path = tmp_dir.path().join("file_list.txt");
    std::fs::write(&file_list_path, "retry_test.txt\n").unwrap();

    // Run file-list migration — should succeed after retries
    let result = migrate_all_files_from_file_list(
        file_list_path.to_str().unwrap(),
        client,
        app_state.clone(),
        1,
    )
    .await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 1);
    assert_eq!(stats.migrated, 1);
    assert_eq!(stats.failed, 0);

    // Verify file was migrated
    let modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "retry_test.txt")
        .await
        .unwrap();
    assert!(modified > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file_list_migration_empty_file() {
    let (_mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Write empty file list (with whitespace/empty lines)
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_list_path = tmp_dir.path().join("file_list.txt");
    std::fs::write(&file_list_path, "\n\n  \n").unwrap();

    let result =
        migrate_all_files_from_file_list(file_list_path.to_str().unwrap(), client, app_state, 5)
            .await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 0);
    assert_eq!(stats.migrated, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file_list_migration_skips_already_migrated() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    mock_state
        .add_file("existing.txt", b"existing content".to_vec())
        .await;

    // Migrate once
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_list_path = tmp_dir.path().join("file_list.txt");
    std::fs::write(&file_list_path, "existing.txt\n").unwrap();

    let result = migrate_all_files_from_file_list(
        file_list_path.to_str().unwrap(),
        client.clone(),
        app_state.clone(),
        5,
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().migrated, 1);

    // Migrate again — should skip
    let result =
        migrate_all_files_from_file_list(file_list_path.to_str().unwrap(), client, app_state, 5)
            .await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 1);
    assert_eq!(stats.migrated, 0);
    assert_eq!(stats.skipped, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file_list_migration_skips_deleted_files() {
    let (mock_state, url) = create_mock_filetracker().await;
    let app_state = create_test_app_state().await;
    let client = Arc::new(FiletrackerClient::new(url));

    // Add only one file — the other is "deleted" (not on ft)
    mock_state.add_file("exists.txt", b"I exist".to_vec()).await;

    // File list references both an existing and a deleted file
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_list_path = tmp_dir.path().join("file_list.txt");
    std::fs::write(&file_list_path, "exists.txt\ndeleted_eval_file.txt\n").unwrap();

    let result = migrate_all_files_from_file_list(
        file_list_path.to_str().unwrap(),
        client,
        app_state.clone(),
        5,
    )
    .await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.total_files, 2);
    assert_eq!(stats.migrated, 1);
    assert_eq!(stats.skipped, 1); // deleted file was skipped, not retried forever
    assert_eq!(stats.failed, 0);

    // Verify the existing file was migrated
    let modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "exists.txt")
        .await
        .unwrap();
    assert!(modified > 0);

    // Verify the deleted file was not migrated
    let modified_deleted = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "deleted_eval_file.txt")
        .await
        .unwrap();
    assert_eq!(modified_deleted, 0);
}

// ====================================================================
// Migration streaming / threshold tests
// ====================================================================

// Helper to create AppState with a custom max_inmemory_size
async fn create_test_app_state_with_threshold(max_inmemory_size: usize) -> Arc<AppState> {
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;

    let (config, _unique_id) = common::create_test_config("test-migration");

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new_with_config(config.locks_type, &config)
        .await
        .unwrap();
    let s3storage = S3Storage::new(&config.bucket).await.unwrap();

    let kvstorage = Arc::new(*kvstorage);
    let locks = Arc::new(*locks);
    let s3storage = Arc::new(*s3storage);
    let cleaner = Arc::new(s3dedup::cleaner::Cleaner::new(
        config.bucket.name.clone(),
        kvstorage.clone(),
        s3storage.clone(),
        locks.clone(),
        Default::default(),
    ));
    let app_state = Arc::new(AppState {
        bucket_name: config.bucket.name.clone(),
        kvstorage,
        locks,
        s3storage,
        filetracker_client: None,
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size,
        cleaner,
    });

    app_state.kvstorage.setup().await.unwrap();
    app_state
}

/// Test offline migration with small files processed in memory (below threshold).
#[tokio::test(flavor = "multi_thread")]
async fn test_migration_inmemory_small_files() {
    let (mock_state, url) = create_mock_filetracker().await;
    // 1MB threshold — test data is small, will use in-memory path
    let app_state = create_test_app_state_with_threshold(1024 * 1024).await;

    let test_data = b"Small migration test file content";
    mock_state
        .add_file("small_inmem.txt", test_data.to_vec())
        .await;

    let client = Arc::new(FiletrackerClient::new(url));
    let result = migrate_all_files(client, app_state.clone(), 5)
        .await
        .unwrap();

    assert_eq!(result.migrated, 1);

    // Verify file was migrated correctly
    let modified = app_state
        .kvstorage
        .get_modified(&app_state.bucket_name, "small_inmem.txt")
        .await
        .unwrap();
    assert!(modified > 0);

    // Verify content via S3
    let hash = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "small_inmem.txt")
        .await
        .unwrap();
    let expected_hash = s3dedup::routes::ft::storage_helpers::compute_sha256(test_data);
    assert_eq!(hash, expected_hash);

    // Verify sizes
    let logical_size = app_state
        .kvstorage
        .get_logical_size(&app_state.bucket_name, &hash)
        .await
        .unwrap();
    assert_eq!(logical_size, test_data.len());
}

/// Test offline migration with files above threshold, forcing temp file processing.
#[tokio::test(flavor = "multi_thread")]
async fn test_migration_tempfile_large_files() {
    let (mock_state, url) = create_mock_filetracker().await;
    // Set threshold to 10 bytes — forces temp file path for any real data
    let app_state = create_test_app_state_with_threshold(10).await;

    // 1KB of data — well above the 10-byte threshold
    let test_data: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
    mock_state
        .add_file("large_tempfile.bin", test_data.clone())
        .await;

    let client = Arc::new(FiletrackerClient::new(url));
    let result = migrate_all_files(client, app_state.clone(), 5)
        .await
        .unwrap();

    assert_eq!(result.migrated, 1);

    // Verify hash
    let hash = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "large_tempfile.bin")
        .await
        .unwrap();
    let expected_hash = s3dedup::routes::ft::storage_helpers::compute_sha256(&test_data);
    assert_eq!(hash, expected_hash);

    // Verify sizes
    let logical_size = app_state
        .kvstorage
        .get_logical_size(&app_state.bucket_name, &hash)
        .await
        .unwrap();
    assert_eq!(logical_size, test_data.len());

    let compressed_size = app_state
        .kvstorage
        .get_compressed_size(&app_state.bucket_name, &hash)
        .await
        .unwrap();
    assert!(compressed_size > 0);
}

/// Test migration with multiple files, mixed sizes relative to threshold.
/// Some go through in-memory, some through temp file.
#[tokio::test(flavor = "multi_thread")]
async fn test_migration_mixed_threshold() {
    let (mock_state, url) = create_mock_filetracker().await;
    // 100-byte threshold
    let app_state = create_test_app_state_with_threshold(100).await;

    // Small file (50 bytes) → in-memory
    let small_data = b"Small file under threshold for in-memory processing.";
    mock_state
        .add_file("mixed/small.txt", small_data.to_vec())
        .await;

    // Large file (500 bytes) → temp file
    let large_data: Vec<u8> = (0..500).map(|i| (i % 251) as u8).collect();
    mock_state
        .add_file("mixed/large.bin", large_data.clone())
        .await;

    let client = Arc::new(FiletrackerClient::new(url));
    let result = migrate_all_files(client, app_state.clone(), 5)
        .await
        .unwrap();

    assert_eq!(result.migrated, 2);

    // Verify both files have correct hashes
    let small_hash = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "mixed/small.txt")
        .await
        .unwrap();
    assert_eq!(
        small_hash,
        s3dedup::routes::ft::storage_helpers::compute_sha256(small_data)
    );

    let large_hash = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "mixed/large.bin")
        .await
        .unwrap();
    assert_eq!(
        large_hash,
        s3dedup::routes::ft::storage_helpers::compute_sha256(&large_data)
    );
}

/// Test deduplication works correctly across threshold boundaries.
/// Same content uploaded via both in-memory and temp file paths should dedup.
#[tokio::test(flavor = "multi_thread")]
async fn test_migration_dedup_across_threshold() {
    let (mock_state, url) = create_mock_filetracker().await;
    // 100-byte threshold
    let app_state = create_test_app_state_with_threshold(100).await;

    // Same content in two files, but the content is >100 bytes so both go through temp file
    let shared_data: Vec<u8> = (0..200).map(|i| (i % 251) as u8).collect();
    mock_state
        .add_file("dedup/file_a.bin", shared_data.clone())
        .await;
    mock_state
        .add_file("dedup/file_b.bin", shared_data.clone())
        .await;

    let client = Arc::new(FiletrackerClient::new(url));
    let result = migrate_all_files(client, app_state.clone(), 5)
        .await
        .unwrap();

    assert_eq!(result.migrated, 2);

    // Both should point to the same hash
    let hash_a = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "dedup/file_a.bin")
        .await
        .unwrap();
    let hash_b = app_state
        .kvstorage
        .get_ref_file(&app_state.bucket_name, "dedup/file_b.bin")
        .await
        .unwrap();
    assert_eq!(hash_a, hash_b, "Dedup should work across temp file path");

    // Refcount should be 2
    let refcount = app_state
        .kvstorage
        .get_ref_count(&app_state.bucket_name, &hash_a)
        .await
        .unwrap();
    assert_eq!(refcount, 2);
}

/// Test on-the-fly migration via GET with streaming (temp file path).
/// Uses low threshold to force temp file processing in migrate_single_file_from_streaming.
#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_get_streaming_tempfile() {
    let (mock_state, url) = create_mock_filetracker().await;
    // 10-byte threshold — forces streaming/temp file for any real content
    let app_state = create_test_app_state_with_threshold(10).await;

    let test_data = b"Live migration GET streaming test - this data goes through temp file path";
    mock_state
        .add_file("stream_get.txt", test_data.to_vec())
        .await;

    // Create app state with filetracker client
    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 10,
        cleaner: app_state.cleaner.clone(),
    });

    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    // GET triggers on-the-fly migration via streaming path
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/stream_get.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify response body is correct (served from S3 after migration)
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, test_data);

    // Verify file is now in s3dedup
    let modified = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "stream_get.txt")
        .await
        .unwrap();
    assert!(modified > 0, "File should be migrated to s3dedup");

    // Verify hash
    let hash = app_state_with_ft
        .kvstorage
        .get_ref_file(&app_state_with_ft.bucket_name, "stream_get.txt")
        .await
        .unwrap();
    assert_eq!(
        hash,
        s3dedup::routes::ft::storage_helpers::compute_sha256(test_data)
    );
}

/// Test on-the-fly migration via GET with in-memory path (high threshold, small file).
/// Verifies that download_file dispatches to migrate_single_file_from_metadata.
#[tokio::test(flavor = "multi_thread")]
async fn test_live_migration_get_inmemory_small() {
    let (mock_state, url) = create_mock_filetracker().await;
    // 1MB threshold — small test data goes through in-memory path
    let app_state = create_test_app_state_with_threshold(1024 * 1024).await;

    let test_data = b"Small file for in-memory on-the-fly migration via GET";
    mock_state
        .add_file("inmem_get.txt", test_data.to_vec())
        .await;

    let app_state_with_ft = Arc::new(AppState {
        bucket_name: app_state.bucket_name.clone(),
        kvstorage: app_state.kvstorage.clone(),
        locks: app_state.locks.clone(),
        s3storage: app_state.s3storage.clone(),
        filetracker_client: Some(Arc::new(FiletrackerClient::new(url))),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 1024 * 1024,
        cleaner: app_state.cleaner.clone(),
    });

    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .with_state(app_state_with_ft.clone());

    // GET triggers on-the-fly migration via in-memory path
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/ft/files/inmem_get.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify response body
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, test_data);

    // Verify file is migrated
    let modified = app_state_with_ft
        .kvstorage
        .get_modified(&app_state_with_ft.bucket_name, "inmem_get.txt")
        .await
        .unwrap();
    assert!(modified > 0);

    let hash = app_state_with_ft
        .kvstorage
        .get_ref_file(&app_state_with_ft.bucket_name, "inmem_get.txt")
        .await
        .unwrap();
    assert_eq!(
        hash,
        s3dedup::routes::ft::storage_helpers::compute_sha256(test_data)
    );
}
