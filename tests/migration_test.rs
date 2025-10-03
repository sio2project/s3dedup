use axum::Router;
use axum::body::Body;
use axum::http::{Response, StatusCode};
use axum::routing::get;
use s3dedup::AppState;
use s3dedup::config::BucketConfig;
use s3dedup::filetracker_client::FiletrackerClient;
use s3dedup::migration::migrate_all_files;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

// Mock filetracker server state
#[derive(Clone)]
struct MockFiletrackerState {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockFiletrackerState {
    fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn add_file(&self, path: &str, data: Vec<u8>) {
        self.files.lock().await.insert(path.to_string(), data);
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
        Some(data) => {
            // Compress the data with gzip
            let compressed = s3dedup::routes::ft::storage_helpers::compress_gzip(data).unwrap();
            let last_modified = chrono::Utc::now().to_rfc2822();

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

    state
        .files
        .lock()
        .await
        .insert(path.to_string(), bytes.to_vec());

    let last_modified = chrono::Utc::now().to_rfc2822();

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
    use s3dedup::config::{KVStorageType, MinIOConfig, SQLiteConfig};
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;

    // Use thread ID and nanoseconds to ensure uniqueness across parallel tests
    let thread_id = std::thread::current().id();
    let thread_id_str = format!("{:?}", thread_id)
        .replace("ThreadId(", "")
        .replace(")", "");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_id = format!("{}{}{}", std::process::id(), thread_id_str, nanos);
    let test_db = format!("db/test_migration_{}.db", unique_id);
    let test_bucket = format!("test-migration-{}", unique_id.to_lowercase());

    let config = BucketConfig {
        name: test_bucket.clone(),
        address: "127.0.0.1".to_string(),
        port: 3001,
        kvstorage_type: KVStorageType::SQLite,
        sqlite: Some(SQLiteConfig {
            path: test_db.clone(),
            pool_size: 50,
        }),
        postgres: None,
        locks_type: s3dedup::locks::LocksType::Memory,
        s3storage_type: s3dedup::s3storage::S3StorageType::MinIO,
        minio: Some(MinIOConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            force_path_style: true,
        }),
        cleaner: s3dedup::cleaner::CleanerConfig::default(),
        filetracker_url: None,
    };

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new(config.locks_type);
    let s3storage = S3Storage::new(&config).await.unwrap();

    let app_state = Arc::new(AppState {
        bucket_name: config.name,
        kvstorage: Arc::new(tokio::sync::Mutex::new(kvstorage)),
        locks,
        s3storage: Arc::new(tokio::sync::Mutex::new(s3storage)),
        filetracker_client: None,
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
    });

    app_state.kvstorage.lock().await.setup().await.unwrap();

    app_state
}

#[tokio::test]
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

#[tokio::test]
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
        .lock()
        .await
        .get_modified(&app_state.bucket_name, "test.txt")
        .await
        .unwrap();
    assert!(modified > 0);
}

#[tokio::test]
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
            .lock()
            .await
            .get_modified(&app_state.bucket_name, &format!("file{}.txt", i))
            .await
            .unwrap();
        assert!(modified > 0);
    }
}

#[tokio::test]
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

#[tokio::test]
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
            .lock()
            .await
            .get_modified(&app_state.bucket_name, file)
            .await
            .unwrap();
        assert!(modified > 0);
    }

    // Verify they all point to the same hash
    let hash1 = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_file(&app_state.bucket_name, "file1.txt")
        .await
        .unwrap();
    let hash2 = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_file(&app_state.bucket_name, "file2.txt")
        .await
        .unwrap();
    let hash3 = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_file(&app_state.bucket_name, "file3.txt")
        .await
        .unwrap();

    assert_eq!(hash1, hash2);
    assert_eq!(hash2, hash3);

    // Verify reference count is 3
    let ref_count = app_state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&app_state.bucket_name, &hash1)
        .await
        .unwrap();
    assert_eq!(ref_count, 3);
}

// Live migration tests

#[tokio::test]
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
        .lock()
        .await
        .get_modified(&app_state_with_ft.bucket_name, "fallback.txt")
        .await
        .unwrap();
    assert!(modified > 0, "File should be migrated to s3dedup");
}

#[tokio::test]
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
        .lock()
        .await
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

#[tokio::test]
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
    });

    // Verify file exists in both before deletion
    let modified_before = app_state_with_ft
        .kvstorage
        .lock()
        .await
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
        .lock()
        .await
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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
    let body_bytes = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();

    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(&decompressed[..], test_data);
}
