use axum::body::Body;
use axum::http::{Response, StatusCode};
use axum::routing::get;
use axum::Router;
use s3dedup::config::BucketConfig;
use s3dedup::filetracker_client::FiletrackerClient;
use s3dedup::migration::migrate_all_files;
use s3dedup::AppState;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
        .route("/files/{*path}", get(mock_ft_get).put(mock_ft_put).delete(mock_ft_delete))
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
    let locks = LocksStorage::new(&config.locks_type);
    let s3storage = S3Storage::new(&config).await.unwrap();

    let app_state = Arc::new(AppState {
        bucket_name: config.name.clone(),
        kvstorage: Arc::new(tokio::sync::Mutex::new(kvstorage)),
        locks: Arc::new(tokio::sync::Mutex::new(locks)),
        s3storage: Arc::new(tokio::sync::Mutex::new(s3storage)),
        filetracker_client: None,
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
