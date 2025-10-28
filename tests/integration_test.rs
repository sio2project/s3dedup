use axum::Router;
use axum::routing::get;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tower::ServiceExt;

// Helper to create test app state
async fn create_test_app() -> Router {
    let (router, _) = create_test_app_with_state().await;
    router
}

// Helper to create test app with access to app state for S3 verification
async fn create_test_app_with_state() -> (Router, Arc<s3dedup::AppState>) {
    use s3dedup::AppState;
    use s3dedup::config::{BucketConfig, KVStorageType, MinIOConfig, PostgresConfig, SQLiteConfig};
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;
    use tokio::sync::Mutex;

    // Determine which KV storage to use from environment
    let use_postgres = std::env::var("DATABASE_URL").is_ok();

    // Create unique identifiers for this test (database and bucket)
    std::fs::create_dir_all("db").ok();

    // Use thread ID and nanosecond timestamp to ensure uniqueness across parallel tests
    let thread_id = std::thread::current().id();
    let thread_id_str = format!("{:?}", thread_id)
        .replace("ThreadId(", "")
        .replace(")", "");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_id = format!("{}{}{}", std::process::id(), thread_id_str, nanos);

    let test_db = format!("db/test_{}.db", unique_id);
    // S3 bucket names must be lowercase and can only contain letters, numbers, and hyphens
    let test_bucket = format!("test-{}", unique_id.to_lowercase());

    let (kvstorage_type, sqlite_config, postgres_config) = if use_postgres {
        (
            KVStorageType::Postgres,
            None,
            Some(PostgresConfig {
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
            KVStorageType::SQLite,
            Some(SQLiteConfig {
                path: test_db.clone(),
                pool_size: 50,
            }),
            None,
        )
    };

    let config = BucketConfig {
        name: test_bucket.clone(),
        address: "127.0.0.1".to_string(),
        port: 3001,
        kvstorage_type,
        sqlite: sqlite_config,
        postgres: postgres_config,
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
        filetracker_v1_dir: None,
    };

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new(config.locks_type);
    let s3storage = S3Storage::new(&config).await.unwrap();

    let app_state = Arc::new(AppState {
        bucket_name: config.name,
        kvstorage: Arc::new(Mutex::new(kvstorage)),
        locks,
        s3storage: Arc::new(Mutex::new(s3storage)),
        filetracker_client: None,
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
    });

    app_state.kvstorage.lock().await.setup().await.unwrap();

    let router = Router::new()
        .route(
            "/ft/list/",
            get(s3dedup::routes::ft::list_files::ft_list_files),
        )
        .route(
            "/ft/list/{*path}",
            get(s3dedup::routes::ft::list_files::ft_list_files),
        )
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file)
                .head(s3dedup::routes::ft::get_file::ft_get_file)
                .put(s3dedup::routes::ft::put_file::ft_put_file)
                .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
        )
        .route("/health", get(s3dedup::routes::metrics::health_handler))
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .route(
            "/metrics/json",
            get(s3dedup::routes::metrics::metrics_json_handler),
        )
        .with_state(app_state.clone());

    (router, app_state)
}

#[tokio::test(flavor = "multi_thread")]
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

#[tokio::test(flavor = "multi_thread")]
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
                .uri(format!(
                    "/ft/files/test/file.txt?last_modified={}",
                    encoded_timestamp
                ))
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
        get_response
            .headers()
            .get("Logical-Size")
            .unwrap()
            .to_str()
            .unwrap(),
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

#[tokio::test(flavor = "multi_thread")]
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
                .uri(format!(
                    "/ft/files/dedup/file1.txt?last_modified={}",
                    encoded_timestamp1
                ))
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
                .uri(format!(
                    "/ft/files/dedup/file2.txt?last_modified={}",
                    encoded_timestamp2
                ))
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
    let body1 = to_bytes(get_response1.into_body(), usize::MAX)
        .await
        .unwrap();
    let body2 = to_bytes(get_response2.into_body(), usize::MAX)
        .await
        .unwrap();

    let decompressed1 = storage_helpers::decompress_gzip(&body1).unwrap();
    let decompressed2 = storage_helpers::decompress_gzip(&body2).unwrap();

    assert_eq!(decompressed1, test_content);
    assert_eq!(decompressed2, test_content);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_headers() {
    let app = create_test_app().await;

    let test_content = b"Test content for header verification";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    // PUT the file
    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/headers/test.txt?last_modified={}",
                    encoded_timestamp
                ))
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
    assert_eq!(
        headers.get("Content-Type").unwrap(),
        "application/octet-stream"
    );
    assert_eq!(headers.get("Content-Encoding").unwrap(), "gzip");
    assert_eq!(
        headers.get("Logical-Size").unwrap().to_str().unwrap(),
        test_content.len().to_string()
    );
    assert!(headers.get("Last-Modified").is_some());
    assert!(headers.get("Content-Length").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_head_nonexistent_file() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/nonexistent.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Verify no body
    use axum::body::to_bytes;
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body_bytes.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_head_existing_file() {
    let app = create_test_app().await;

    let test_content = b"Test content for HEAD request";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    // PUT the file first
    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/head/test.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    // HEAD the file
    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/head/test.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify headers are present (same as GET)
    let headers = response.headers();
    assert_eq!(
        headers.get("Content-Type").unwrap(),
        "application/octet-stream"
    );
    assert_eq!(headers.get("Content-Encoding").unwrap(), "gzip");
    assert_eq!(
        headers.get("Logical-Size").unwrap().to_str().unwrap(),
        test_content.len().to_string()
    );
    assert!(headers.get("Last-Modified").is_some());
    assert!(headers.get("Content-Length").is_some());

    // Verify no body
    use axum::body::to_bytes;
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body_bytes.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_nonexistent_file() {
    let app = create_test_app().await;

    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/nonexistent.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_file() {
    let (mut app, state) = create_test_app_with_state().await;
    use tower::Service;

    let test_content = b"Test content to be deleted";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    // PUT the file first
    let put_response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/delete/test.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(put_response.status(), StatusCode::OK);

    // Verify blob exists in S3
    let blob_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(blob_exists, "Blob should exist in S3 after upload");

    // DELETE the file
    let delete_response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/delete/test.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(delete_response.status(), StatusCode::OK);

    // Verify blob is deleted from S3
    let blob_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(
        !blob_exists,
        "Blob should be deleted from S3 after file deletion"
    );

    // Verify file is gone
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/delete/test.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dedup_refcount_increment() {
    let app = create_test_app().await;

    let test_content = b"Shared content for refcount test";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    // PUT first file
    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/refcount/file1.txt?last_modified={}",
                    encoded_timestamp1
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();

    // PUT second file with same content
    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/refcount/file2.txt?last_modified={}",
                    encoded_timestamp2
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();

    // PUT third file with same content
    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp3 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp3 = urlencoding::encode(&timestamp3);

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/refcount/file3.txt?last_modified={}",
                    encoded_timestamp3
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    // Verify all three files can be retrieved
    let get1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/refcount/file1.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let get2 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/refcount/file2.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let get3 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/refcount/file3.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get1.status(), StatusCode::OK);
    assert_eq!(get2.status(), StatusCode::OK);
    assert_eq!(get3.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dedup_blob_deleted_when_refcount_zero() {
    let (mut app, state) = create_test_app_with_state().await;
    use tower::Service;

    let test_content = b"Content that will be fully deleted";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    // PUT two files with same content
    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    app.call(
        Request::builder()
            .uri(format!(
                "/ft/files/deletion/file1.txt?last_modified={}",
                encoded_timestamp1
            ))
            .method("PUT")
            .header("Content-Encoding", "gzip")
            .header("SHA256-Checksum", &sha256)
            .header("Logical-Size", test_content.len().to_string())
            .body(Body::from(compressed_data.clone()))
            .unwrap(),
    )
    .await
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    app.call(
        Request::builder()
            .uri(format!(
                "/ft/files/deletion/file2.txt?last_modified={}",
                encoded_timestamp2
            ))
            .method("PUT")
            .header("Content-Encoding", "gzip")
            .header("SHA256-Checksum", &sha256)
            .header("Logical-Size", test_content.len().to_string())
            .body(Body::from(compressed_data))
            .unwrap(),
    )
    .await
    .unwrap();

    // Verify blob exists in S3
    let blob_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(blob_exists, "Blob should exist in S3 after upload");

    // DELETE first file - blob should still exist (refcount = 1)
    app.call(
        Request::builder()
            .uri(format!(
                "/ft/files/deletion/file1.txt?last_modified={}",
                encoded_timestamp1
            ))
            .method("DELETE")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Verify blob still exists in S3 (refcount = 1)
    let blob_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(
        blob_exists,
        "Blob should still exist in S3 after first deletion (refcount=1)"
    );

    // Second file should still be retrievable
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/deletion/file2.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, test_content);

    // DELETE second file - blob should be deleted from S3 (refcount = 0)
    app.call(
        Request::builder()
            .uri(format!(
                "/ft/files/deletion/file2.txt?last_modified={}",
                encoded_timestamp2
            ))
            .method("DELETE")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Verify blob is deleted from S3
    let blob_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(
        !blob_exists,
        "Blob should be deleted from S3 after refcount reaches 0"
    );

    // Both files should now be gone
    let get1 = app
        .call(
            Request::builder()
                .uri("/ft/files/deletion/file1.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let get2 = app
        .call(
            Request::builder()
                .uri("/ft/files/deletion/file2.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get1.status(), StatusCode::NOT_FOUND);
    assert_eq!(get2.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dedup_partial_deletion() {
    let app = create_test_app().await;

    let test_content = b"Partially deleted content";

    use s3dedup::routes::ft::storage_helpers;
    let compressed_data = storage_helpers::compress_gzip(test_content).unwrap();
    let sha256 = storage_helpers::compute_sha256(test_content);

    // PUT three files with same content
    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/partial/file1.txt?last_modified={}",
                    encoded_timestamp1
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/partial/file2.txt?last_modified={}",
                    encoded_timestamp2
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp3 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp3 = urlencoding::encode(&timestamp3);

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/partial/file3.txt?last_modified={}",
                    encoded_timestamp3
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", test_content.len().to_string())
                .body(Body::from(compressed_data))
                .unwrap(),
        )
        .await
        .unwrap();

    // DELETE two files - one should still remain
    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/partial/file1.txt?last_modified={}",
                    encoded_timestamp1
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    app.clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/ft/files/partial/file2.txt?last_modified={}",
                    encoded_timestamp2
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Third file should still be retrievable (refcount = 1)
    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/partial/file3.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, test_content);

    // First two files should be gone
    let get1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/partial/file1.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let get2 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ft/files/partial/file2.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get1.status(), StatusCode::NOT_FOUND);
    assert_eq!(get2.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dedup_update_same_path() {
    use tower::Service;

    let (mut app, state) = create_test_app_with_state().await;

    let content_v1 = b"Version 1 content";
    let content_v2 = b"Version 2 content - different!";

    use s3dedup::routes::ft::storage_helpers;

    // PUT version 1
    let compressed_v1 = storage_helpers::compress_gzip(content_v1).unwrap();
    let sha256_v1 = storage_helpers::compute_sha256(content_v1);

    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    let put1 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/update/same.txt?last_modified={}",
                    encoded_timestamp1
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256_v1)
                .header("Logical-Size", content_v1.len().to_string())
                .body(Body::from(compressed_v1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put1.status(), StatusCode::OK);

    // Verify v1 blob exists in S3
    let v1_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256_v1)
        .await
        .unwrap();
    assert!(v1_exists, "V1 blob should exist in S3");

    // PUT version 2 to the same path (should decrement refcount of v1, increment v2)
    std::thread::sleep(std::time::Duration::from_millis(100));
    let compressed_v2 = storage_helpers::compress_gzip(content_v2).unwrap();
    let sha256_v2 = storage_helpers::compute_sha256(content_v2);

    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    let put2 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/update/same.txt?last_modified={}",
                    encoded_timestamp2
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256_v2)
                .header("Logical-Size", content_v2.len().to_string())
                .body(Body::from(compressed_v2))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::OK);

    // Verify v1 blob is deleted from S3 (refcount dropped to 0)
    let v1_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256_v1)
        .await
        .unwrap();
    assert!(!v1_exists, "V1 blob should be deleted from S3 after update");

    // Verify v2 blob exists in S3
    let v2_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256_v2)
        .await
        .unwrap();
    assert!(v2_exists, "V2 blob should exist in S3");

    // GET should return version 2
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/update/same.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, content_v2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dedup_same_content_same_path_refcount() {
    use tower::Service;

    let (mut app, state) = create_test_app_with_state().await;

    let content = b"Same content uploaded to same path";

    use s3dedup::routes::ft::storage_helpers;

    // PUT content first time
    let compressed = storage_helpers::compress_gzip(content).unwrap();
    let sha256 = storage_helpers::compute_sha256(content);

    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    let put1 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/refcount/same.txt?last_modified={}",
                    encoded_timestamp1
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", content.len().to_string())
                .body(Body::from(compressed.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put1.status(), StatusCode::OK);

    // Verify refcount is 1
    let refcount1 = state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(refcount1, 1, "Refcount should be 1 after first PUT");

    // PUT same content to same path again (different timestamp to allow update)
    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    let put2 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/refcount/same.txt?last_modified={}",
                    encoded_timestamp2
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", content.len().to_string())
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::OK);

    // Verify refcount is STILL 1 (not incremented)
    let refcount2 = state
        .kvstorage
        .lock()
        .await
        .get_ref_count(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        refcount2, 1,
        "Refcount should still be 1 after PUTing same content to same path (bug fix)"
    );

    // Verify blob still exists in S3
    let blob_exists = state
        .s3storage
        .lock()
        .await
        .object_exists(&sha256)
        .await
        .unwrap();
    assert!(blob_exists, "Blob should still exist in S3");

    // GET should return the content
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/refcount/same.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = storage_helpers::decompress_gzip(&body_bytes).unwrap();
    assert_eq!(decompressed, content);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compressed_size_preserved() {
    use tower::Service;

    let (mut app, state) = create_test_app_with_state().await;

    let content = b"Test content for compressed size tracking";

    use s3dedup::routes::ft::storage_helpers;

    // PUT a file
    let compressed = storage_helpers::compress_gzip(content).unwrap();
    let sha256 = storage_helpers::compute_sha256(content);

    let timestamp1 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp1 = urlencoding::encode(&timestamp1);

    let put1 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/compress/test.txt?last_modified={}",
                    encoded_timestamp1
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", content.len().to_string())
                .body(Body::from(compressed.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put1.status(), StatusCode::OK);

    // Verify compressed_size is set correctly (should be size of gzipped data)
    let compressed_size = state
        .kvstorage
        .lock()
        .await
        .get_compressed_size(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        compressed_size,
        compressed.len(),
        "Compressed size should match the gzipped data size"
    );

    // Verify logical_size is set correctly
    let logical_size = state
        .kvstorage
        .lock()
        .await
        .get_logical_size(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        logical_size,
        content.len(),
        "Logical size should match the uncompressed data size"
    );

    // PUT same content to same path again with newer timestamp
    // This calls set_logical_size again, which should NOT overwrite compressed_size
    std::thread::sleep(std::time::Duration::from_millis(100));
    let timestamp2 = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp2 = urlencoding::encode(&timestamp2);

    let put2 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/compress/test.txt?last_modified={}",
                    encoded_timestamp2
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha256)
                .header("Logical-Size", content.len().to_string())
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::OK);

    // Verify compressed_size is STILL preserved (bug fix test)
    let compressed_size_after = state
        .kvstorage
        .lock()
        .await
        .get_compressed_size(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        compressed_size_after, compressed_size,
        "Compressed size should be preserved after second PUT with same content (bug fix)"
    );

    // Verify logical_size is still correct
    let logical_size_after = state
        .kvstorage
        .lock()
        .await
        .get_logical_size(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        logical_size_after, logical_size,
        "Logical size should remain correct"
    );

    // Verify metrics query works correctly
    let total_storage = state
        .kvstorage
        .lock()
        .await
        .get_total_storage_bytes(&state.bucket_name)
        .await
        .unwrap();
    assert_eq!(
        total_storage, compressed_size as i64,
        "Total storage bytes should equal compressed size"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_files_empty() {
    let app = create_test_app().await;

    // List files in empty prefix
    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/list/empty/")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str, "");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_files_basic() {
    use tower::Service;
    let mut app = create_test_app().await;

    // Upload three files
    use s3dedup::routes::ft::storage_helpers;
    let content1 = b"File 1 content";
    let content2 = b"File 2 content";
    let content3 = b"File 3 content";

    let compressed1 = storage_helpers::compress_gzip(content1).unwrap();
    let sha1 = storage_helpers::compute_sha256(content1);
    let compressed2 = storage_helpers::compress_gzip(content2).unwrap();
    let sha2 = storage_helpers::compute_sha256(content2);
    let compressed3 = storage_helpers::compress_gzip(content3).unwrap();
    let sha3 = storage_helpers::compute_sha256(content3);

    let timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_timestamp = urlencoding::encode(&timestamp);

    // Upload to different paths
    let put1 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/dir/file1.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha1)
                .header("Logical-Size", content1.len().to_string())
                .body(Body::from(compressed1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put1.status(), StatusCode::OK);

    let put2 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/dir/file2.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha2)
                .header("Logical-Size", content2.len().to_string())
                .body(Body::from(compressed2))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::OK);

    let put3 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/other/file3.txt?last_modified={}",
                    encoded_timestamp
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha3)
                .header("Logical-Size", content3.len().to_string())
                .body(Body::from(compressed3))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put3.status(), StatusCode::OK);

    // List files under "dir/" - should get 2 files
    let list_response = app
        .call(
            Request::builder()
                .uri("/ft/list/dir/")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    let files: Vec<&str> = body_str.trim().split('\n').collect();
    assert_eq!(files.len(), 2);
    assert!(files.contains(&"dir/file1.txt"));
    assert!(files.contains(&"dir/file2.txt"));

    // List files under "other/" - should get 1 file
    let list_response2 = app
        .call(
            Request::builder()
                .uri("/ft/list/other/")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response2.status(), StatusCode::OK);

    let body_bytes2 = to_bytes(list_response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str2 = String::from_utf8(body_bytes2.to_vec()).unwrap();
    assert_eq!(body_str2.trim(), "other/file3.txt");

    // List all files - should get 3 files
    let list_response3 = app
        .call(
            Request::builder()
                .uri("/ft/list/")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response3.status(), StatusCode::OK);

    let body_bytes3 = to_bytes(list_response3.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str3 = String::from_utf8(body_bytes3.to_vec()).unwrap();
    let all_files: Vec<&str> = body_str3.trim().split('\n').collect();
    assert_eq!(all_files.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_files_with_timestamp() {
    use tower::Service;
    let mut app = create_test_app().await;

    use s3dedup::routes::ft::storage_helpers;
    let content1 = b"Old file";
    let content2 = b"New file";

    let compressed1 = storage_helpers::compress_gzip(content1).unwrap();
    let sha1 = storage_helpers::compute_sha256(content1);
    let compressed2 = storage_helpers::compress_gzip(content2).unwrap();
    let sha2 = storage_helpers::compute_sha256(content2);

    // Upload first file with old timestamp
    let old_time = chrono::Utc::now() - chrono::Duration::seconds(3600);
    let old_timestamp = old_time.to_rfc2822();
    let encoded_old = urlencoding::encode(&old_timestamp);

    let put1 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/test/old.txt?last_modified={}",
                    encoded_old
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha1)
                .header("Logical-Size", content1.len().to_string())
                .body(Body::from(compressed1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put1.status(), StatusCode::OK);

    // Upload second file with recent timestamp
    let new_time = chrono::Utc::now();
    let new_timestamp = new_time.to_rfc2822();
    let encoded_new = urlencoding::encode(&new_timestamp);

    let put2 = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/test/new.txt?last_modified={}",
                    encoded_new
                ))
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", &sha2)
                .header("Logical-Size", content2.len().to_string())
                .body(Body::from(compressed2))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::OK);

    // List files with timestamp in the middle - should only get old file
    let middle_time = chrono::Utc::now() - chrono::Duration::seconds(1800);
    let middle_timestamp = middle_time.to_rfc2822();
    let encoded_middle = urlencoding::encode(&middle_timestamp);

    let list_response = app
        .call(
            Request::builder()
                .uri(format!("/ft/list/test/?last_modified={}", encoded_middle))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str.trim(), "test/old.txt");

    // List files with current timestamp - should get both files
    let current_timestamp = chrono::Utc::now().to_rfc2822();
    let encoded_current = urlencoding::encode(&current_timestamp);

    let list_response2 = app
        .call(
            Request::builder()
                .uri(format!("/ft/list/test/?last_modified={}", encoded_current))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response2.status(), StatusCode::OK);

    let body_bytes2 = to_bytes(list_response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str2 = String::from_utf8(body_bytes2.to_vec()).unwrap();
    let files: Vec<&str> = body_str2.trim().split('\n').collect();
    assert_eq!(files.len(), 2);
    assert!(files.contains(&"test/old.txt"));
    assert!(files.contains(&"test/new.txt"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_health_endpoint_healthy() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 200 OK when healthy
    assert_eq!(response.status(), StatusCode::OK);

    // Verify JSON response structure
    use axum::body::to_bytes;
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let health_status: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(health_status["status"], "ok");
    assert!(health_status["uptime_seconds"].is_number());
    assert_eq!(health_status["checks"]["database"], "ok");
    assert_eq!(health_status["checks"]["s3"], "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_health_endpoint_uptime() {
    let app = create_test_app().await;

    // Wait a bit to ensure uptime is > 0
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let health_status: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let uptime = health_status["uptime_seconds"].as_i64().unwrap();
    assert!(uptime >= 0, "Uptime should be non-negative");
}
