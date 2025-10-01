use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;
use axum::Router;
use axum::routing::{get, put};
use std::sync::Arc;

// Helper to create test app state
async fn create_test_app() -> Router {
    use s3dedup::{AppState};
    use s3dedup::config::{BucketConfig, KVStorageType, SQLiteConfig, MinIOConfig};
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;
    use tokio::sync::Mutex;

    // Create temporary test database
    let test_db = format!("test_{}.db", std::process::id());

    let config = BucketConfig {
        name: "test-bucket".to_string(),
        address: "127.0.0.1".to_string(),
        port: 3001,
        kvstorage_type: KVStorageType::SQLite,
        sqlite: Some(SQLiteConfig {
            path: test_db.clone(),
            pool_size: 5,
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
    };

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new(&config.locks_type);
    let s3storage = S3Storage::new(&config).await.unwrap();

    let app_state = AppState {
        bucket_name: config.name.clone(),
        kvstorage: Arc::new(Mutex::new(kvstorage)),
        locks: Arc::new(Mutex::new(locks)),
        s3storage: Arc::new(Mutex::new(s3storage)),
    };

    app_state.kvstorage.lock().await.setup().await.unwrap();

    Router::new()
        .route("/ft/files/{*path}", get(s3dedup::routes::ft::get_file::ft_get_file).put(s3dedup::routes::ft::put_file::ft_put_file))
        .with_state(Arc::new(app_state))
}

#[tokio::test]
async fn test_get_nonexistent_file() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/nonexistent.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_put_and_get_file() {
    let app = create_test_app().await;

    // Test data
    let test_content = b"Hello, World! This is a test file.";

    // Compress the data
    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    // Generate timestamp
    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    // PUT the file
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/ft/files/test/file.txt?last_modified={}", encoded_timestamp))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(put_response.status(), StatusCode::OK);
    assert!(put_response.headers().get("Last-Modified").is_some());

    // GET the file back
    let get_response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/test/file.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        get_response.headers().get("Content-Encoding").unwrap(),
        "gzip"
    );
    assert_eq!(
        get_response.headers().get("Content-Type").unwrap(),
        "application/octet-stream"
    );
    assert_eq!(
        get_response.headers().get("Logical-Size").unwrap().to_str().unwrap(),
        test_content.len().to_string()
    );
    assert!(get_response.headers().get("Last-Modified").is_some());

    // Verify the content
    use axum::body::to_bytes;
    let body_bytes = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, test_content);
}

#[tokio::test]
async fn test_put_twice_same_content() {
    let app = create_test_app().await;

    let test_content = b"Same content uploaded twice";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    // PUT the file first time
    let put_response1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/ft/files/dedup/file1.txt?last_modified={}", encoded_timestamp1))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(put_response1.status(), StatusCode::OK);

    // PUT the same content to a different path
    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    let put_response2 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/ft/files/dedup/file2.txt?last_modified={}", encoded_timestamp2))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(put_response2.status(), StatusCode::OK);

    // GET both files - they should return the same content (deduplication works)
    let get_response1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/dedup/file1.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let get_response2 = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/dedup/file2.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response1.status(), StatusCode::OK);
    assert_eq!(get_response2.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body1 = to_bytes(get_response1.into_body(), usize::MAX).await.unwrap();
    let body2 = to_bytes(get_response2.into_body(), usize::MAX).await.unwrap();

    let decompressed1 = storage_helpers::decompress_gzip(&body1).unwrap();
    let decompressed2 = storage_helpers::decompress_gzip(&body2).unwrap();

    assert_eq!(decompressed1, test_content);
    assert_eq!(decompressed2, test_content);
}

#[tokio::test]
async fn test_get_headers() {
    let app = create_test_app().await;

    let test_content = b"Test content for header verification";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    // PUT the file
    app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/ft/files/headers/test.txt?last_modified={}", encoded_timestamp))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    // GET the file and verify headers
    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/headers/test.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let headers = response.headers();
    assert_eq!(headers.get("Content-Type").unwrap(), "application/octet-stream");
    assert_eq!(headers.get("Content-Encoding").unwrap(), "gzip");
    assert_eq!(headers.get("Logical-Size").unwrap().to_str().unwrap(), test_content.len().to_string());
    assert!(headers.get("Last-Modified").is_some());
    assert!(headers.get("Content-Length").is_some());
}
