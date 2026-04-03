//! Phase 2: Error path tests using mock infrastructure.
//! These tests verify correct behavior when KV storage or S3 operations fail.
//!
//! Run with: cargo test --features test-mocks --test error_path_test

#![cfg(feature = "test-mocks")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use s3dedup::AppState;
use s3dedup::kvstorage::KVStorage;
use s3dedup::kvstorage::mock::MockKVStorage;
use s3dedup::locks::LocksStorage;
use s3dedup::locks::mock::MockLocks;
use s3dedup::s3storage::S3Storage;
use s3dedup::s3storage::mock::MockS3Storage;
use std::sync::Arc;
use tower::ServiceExt;

fn make_rfc2822(unix_ts: i64) -> String {
    use chrono::{DateTime, Utc};
    DateTime::<Utc>::from_timestamp(unix_ts, 0)
        .unwrap()
        .to_rfc2822()
}

/// Create a test app with mock backends. Returns (Router, MockKVStorage, MockS3Storage)
/// so tests can configure failures.
fn create_mock_app() -> (Router, MockKVStorage, MockS3Storage) {
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
                .head(s3dedup::routes::ft::head_file::ft_head_file)
                .put(s3dedup::routes::ft::put_file::ft_put_file)
                .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
        )
        .route("/health", get(s3dedup::routes::metrics::health_handler))
        .with_state(app_state);

    (router, mock_kv, mock_s3)
}

fn build_put_request(path: &str, content: &[u8], timestamp_rfc2822: &str) -> Request<Body> {
    use s3dedup::routes::ft::storage_helpers;
    let compressed = storage_helpers::compress_gzip(content).unwrap();
    let sha256 = storage_helpers::compute_sha256(content);
    let encoded_ts = urlencoding::encode(timestamp_rfc2822);

    Request::builder()
        .uri(format!("/ft/files/{}?last_modified={}", path, encoded_ts))
        .method("PUT")
        .header("Content-Encoding", "gzip")
        .header("SHA256-Checksum", sha256)
        .header("Logical-Size", content.len().to_string())
        .body(Body::from(compressed))
        .unwrap()
}

// ============================================================
// 1. PUT: set_modified fails after set_ref_file succeeds (1.4)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_set_modified_fails_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    // Make set_modified fail
    mock_kv.set_failing("set_modified");

    let ts = make_rfc2822(1700010000);
    let req = build_put_request("error/set_modified.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "Should return 500 when set_modified fails"
    );
}

// ============================================================
// 2. PUT: set_logical_size fails → 500 (1.4)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_set_logical_size_fails_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("set_logical_size");

    let ts = make_rfc2822(1700010100);
    let req = build_put_request("error/logical_size.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 3. PUT: atomic_increment_ref_count fails → 500 (1.4)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_increment_refcount_fails_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("atomic_increment_ref_count");

    let ts = make_rfc2822(1700010200);
    let req = build_put_request("error/refcount.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 4. PUT: S3 put_object_stream fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_s3_upload_fails_returns_500() {
    let (app, _mock_kv, mock_s3) = create_mock_app();

    mock_s3.set_failing("put_object_stream");

    let ts = make_rfc2822(1700010300);
    let req = build_put_request("error/s3_upload.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 5. GET: get_modified fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_db_error_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("get_modified");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/error/db_fail.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "DB error during GET should return 500, not 404"
    );
}

// ============================================================
// 6. HEAD: get_modified fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_head_db_error_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("get_modified");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/error/db_fail.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "DB error during HEAD should return 500, not 404"
    );
}

// ============================================================
// 7. DELETE: get_ref_file fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_get_ref_file_fails_returns_500() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700010400);

    // First PUT a file successfully
    let req = build_put_request("error/del_ref.txt", b"delete me", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Now make get_ref_file fail for the DELETE
    mock_kv.set_failing("get_ref_file");

    let encoded_ts = urlencoding::encode(&ts);
    let response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/error/del_ref.txt?last_modified={}",
                    encoded_ts
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "get_ref_file failure during DELETE should return 500"
    );
}

// ============================================================
// 8. LIST: list_files DB fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_list_db_error_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("list_files");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/list/")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 9. GET: S3 get_object_stream fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_s3_stream_fails_returns_500() {
    use tower::Service;
    let (mut app, _mock_kv, mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700010500);

    // PUT a file successfully
    let req = build_put_request("error/s3_stream.txt", b"s3 stream fail", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Now make S3 get_object_stream fail
    mock_s3.set_failing("get_object_stream");

    let response = app
        .call(
            Request::builder()
                .uri("/ft/files/error/s3_stream.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 10. PUT: set_ref_file fails → 500 (1.4)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_set_ref_file_fails_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("set_ref_file");

    let ts = make_rfc2822(1700010600);
    let req = build_put_request("error/set_ref.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 11. PUT: set_compressed_size fails → 500 (1.4)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_set_compressed_size_fails_returns_500() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    mock_kv.set_failing("set_compressed_size");

    let ts = make_rfc2822(1700010700);
    let req = build_put_request("error/compressed_size.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 12. Normal PUT + GET roundtrip with mocks (sanity check)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_mock_put_get_roundtrip() {
    use axum::body::to_bytes;
    use tower::Service;

    let (mut app, _mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700010800);
    let content = b"mock roundtrip content";

    let req = build_put_request("mock/roundtrip.txt", content, &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/mock/roundtrip.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);
}

// ============================================================
// 13. DELETE: delete_ref_file fails → 500 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_ref_file_removal_fails_returns_500() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700010900);

    // PUT a file successfully
    let req = build_put_request("error/del_fail.txt", b"delete me", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make delete_ref_file fail
    mock_kv.set_failing("delete_ref_file");

    let encoded_ts = urlencoding::encode(&ts);
    let response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/error/del_fail.txt?last_modified={}",
                    encoded_ts
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 14. Health endpoint when S3 is unreachable → degraded (2.5)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_health_s3_failure_returns_degraded() {
    let (app, _mock_kv, mock_s3) = create_mock_app();

    mock_s3.set_failing("check_health");

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

    // Should be 503 when S3 health check fails
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    use axum::body::to_bytes;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Status should not be "ok"
    assert_ne!(json["status"], "ok");
}

// ============================================================
// 15. PUT: bad gzip in slow path → 400 (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_bad_gzip_slow_path() {
    let (app, _mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011000);
    let encoded_ts = urlencoding::encode(&ts);

    // Send with content-encoding: gzip but invalid gzip data (no checksum header → slow path)
    let req = Request::builder()
        .uri(format!(
            "/ft/files/error/bad_gzip.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .header("Content-Encoding", "gzip")
        .body(Body::from(vec![0x1f, 0x8b, 0x00, 0xff, 0xff])) // truncated gzip
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    // Should fail due to bad gzip — either 400 or 500
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::INTERNAL_SERVER_ERROR,
        "Bad gzip should fail, got: {}",
        response.status()
    );
}

// ============================================================
// 16. DELETE: delete_modified fails after delete_ref_file (1.3)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_modified_fails_returns_500() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011100);

    // PUT a file
    let req = build_put_request("error/del_mod.txt", b"content", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make delete_modified fail
    mock_kv.set_failing("delete_modified");

    let encoded_ts = urlencoding::encode(&ts);
    let response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/error/del_mod.txt?last_modified={}",
                    encoded_ts
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================
// 17. DELETE: S3 delete_object fails → file still deleted from
//     metadata, returns 200
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_s3_delete_fails_still_returns_200() {
    use tower::Service;
    let (mut app, _mock_kv, mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011200);

    // PUT a file successfully
    let req = build_put_request("error/s3_del_fail.txt", b"content to delete", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make S3 delete_object fail (non-fatal in delete handler)
    mock_s3.set_failing("delete_object");

    let encoded_ts = urlencoding::encode(&ts);
    let response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/error/s3_del_fail.txt?last_modified={}",
                    encoded_ts
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // S3 delete failure is logged but non-fatal — metadata cleanup still happens
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "DELETE should return 200 even when S3 delete_object fails"
    );

    // After DELETE, GET should return 404 (metadata was cleaned up)
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/error/s3_del_fail.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        get_response.status(),
        StatusCode::NOT_FOUND,
        "GET after DELETE should return 404 (metadata cleaned up despite S3 failure)"
    );
}

// ============================================================
// 18. GET: get_ref_file returns empty string when modified > 0
//     (corruption case) → 404
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_ref_file_empty_with_modified_set_returns_404() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011300);

    // PUT a file successfully (sets both modified and ref_file)
    let req = build_put_request("error/corrupt_ref.txt", b"some content", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Simulate corruption: make delete_modified fail so DELETE removes
    // ref_file but leaves modified > 0
    mock_kv.set_failing("delete_modified");

    let encoded_ts = urlencoding::encode(&ts);
    let del_response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/files/error/corrupt_ref.txt?last_modified={}",
                    encoded_ts
                ))
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // DELETE itself fails (500) because delete_modified fails,
    // but delete_ref_file already ran, creating the corruption state
    assert_eq!(del_response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Now clear the failure so GET can proceed normally
    mock_kv.clear_failing("delete_modified");

    // GET: modified > 0 but ref_file returns "" → should return 404
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/error/corrupt_ref.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        get_response.status(),
        StatusCode::NOT_FOUND,
        "GET should return 404 when ref_file is empty but modified > 0 (corruption case)"
    );
}

// ============================================================
// 19. GET: get_logical_size + get_compressed_size fail → 500
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_size_metadata_fails_returns_500() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011400);

    // PUT a file successfully
    let req = build_put_request("error/size_fail.txt", b"size metadata test", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make get_logical_size fail (causes try_join! to fail in GET handler)
    mock_kv.set_failing("get_logical_size");

    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/error/size_fail.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        get_response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "GET should return 500 when size metadata query fails"
    );
}

// ============================================================
// 20. HEAD: S3 object_exists_with_size fails → handled gracefully
//     (compressed_size from PUT takes priority, S3 fallback not hit)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_head_s3_object_exists_with_size_fails_handled_gracefully() {
    use tower::Service;
    let (mut app, _mock_kv, mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011500);

    // PUT a file successfully (compressed_size is stored in KV)
    let req = build_put_request("error/head_s3.txt", b"head s3 test content", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make S3 object_exists_with_size fail
    mock_s3.set_failing("object_exists_with_size");

    let head_response = app
        .call(
            Request::builder()
                .uri("/ft/files/error/head_s3.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // HEAD should succeed because compressed_size > 0 from PUT, so
    // the S3 object_exists_with_size fallback is never called
    assert_eq!(
        head_response.status(),
        StatusCode::OK,
        "HEAD should handle S3 object_exists_with_size failure gracefully"
    );
}

// ============================================================
// 21. PUT: atomic_decrement_ref_count fails during overwrite
//     → PUT still returns 200 (decrement failure is non-fatal)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_overwrite_decrement_fails_still_returns_200() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts1 = make_rfc2822(1700011600);
    let ts2 = make_rfc2822(1700011700);

    // PUT file with content A
    let req = build_put_request("error/decrement.txt", b"content A", &ts1);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make atomic_decrement_ref_count fail (triggers on overwrite with different content)
    mock_kv.set_failing("atomic_decrement_ref_count");

    // PUT same path with content B (different content → triggers decrement_old_ref)
    let req = build_put_request("error/decrement.txt", b"content B", &ts2);
    let response = app.call(req).await.unwrap();

    // decrement_old_ref failure is non-fatal (warn-logged, refcount leak reclaimed by cleaner)
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "PUT should return 200 even when atomic_decrement_ref_count fails (non-fatal)"
    );
}

// ============================================================
// 22. GET: get_compressed_size fails → 500
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_compressed_size_fails_returns_500() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3) = create_mock_app();

    let ts = make_rfc2822(1700011800);

    // PUT a file successfully
    let req = build_put_request("error/comp_size.txt", b"compressed size test", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make get_compressed_size fail (causes try_join! to fail in GET handler)
    mock_kv.set_failing("get_compressed_size");

    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/error/comp_size.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        get_response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "GET should return 500 when get_compressed_size fails"
    );
}

// ============================================================
// Helper: create app with mock locks (instead of memory locks)
// ============================================================

fn create_mock_app_with_mock_locks() -> (Router, MockKVStorage, MockS3Storage, MockLocks) {
    let mock_kv = MockKVStorage::new();
    let mock_s3 = MockS3Storage::new();
    let mock_locks = MockLocks::new();

    let kvstorage = Arc::new(KVStorage::Mock(mock_kv.clone()));
    let locks = Arc::new(LocksStorage::Mock(mock_locks.clone()));
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
                .head(s3dedup::routes::ft::head_file::ft_head_file)
                .put(s3dedup::routes::ft::put_file::ft_put_file)
                .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
        )
        .route("/health", get(s3dedup::routes::metrics::health_handler))
        .with_state(app_state);

    (router, mock_kv, mock_s3, mock_locks)
}

// ============================================================
// 23. PUT: file lock acquisition fails -> 500
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_file_lock_acquire_fails_returns_500() {
    let (app, _mock_kv, _mock_s3, mock_locks) = create_mock_app_with_mock_locks();

    // Make acquire_exclusive fail — this affects both file lock and hash lock
    mock_locks.set_failing("acquire_exclusive");

    let ts = make_rfc2822(1700012000);
    let req = build_put_request("error/lock_fail.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "PUT should return 500 when file lock acquisition fails"
    );
}

// ============================================================
// 24. PUT: lock release fails -> still returns 200
//     (release failure is non-fatal, just logged with let _ = ...)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_lock_release_fails_still_returns_200() {
    let (app, _mock_kv, _mock_s3, mock_locks) = create_mock_app_with_mock_locks();

    // Release failures are ignored by the PUT handler (let _ = guard.release().await)
    mock_locks.set_failing("release");

    let ts = make_rfc2822(1700012100);
    let req = build_put_request("error/release_fail.txt", b"test content", &ts);
    let response = app.oneshot(req).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "PUT should return 200 even when lock release fails (non-fatal)"
    );
}

// ============================================================
// 25. PUT overwrite: decrement_old_ref hash lock fails -> still 200
//     (decrement_old_ref failure is non-fatal, warn-logged)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_overwrite_decrement_hash_lock_fails_still_returns_200() {
    use tower::Service;
    let (mut app, mock_kv, _mock_s3, _mock_locks) = create_mock_app_with_mock_locks();

    let ts1 = make_rfc2822(1700012200);
    let ts2 = make_rfc2822(1700012300);

    // PUT content A
    let req = build_put_request("error/decrement_lock.txt", b"content A", &ts1);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Make atomic_decrement_ref_count fail (triggers on overwrite with different content).
    // We can't selectively fail only the decrement_old_ref hash lock with MockLocks
    // (it fails ALL acquire_exclusive calls), so we inject the failure at the KV level.
    // This verifies decrement_old_ref failure is non-fatal.
    mock_kv.set_failing("atomic_decrement_ref_count");

    // PUT content B to same path (different content -> triggers decrement_old_ref)
    let req = build_put_request("error/decrement_lock.txt", b"content B", &ts2);
    let response = app.call(req).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "PUT should return 200 even when decrement_old_ref fails (non-fatal, cleaner reclaims)"
    );
}

// ============================================================
// 26. Health: DB query fails -> 503
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_health_db_query_fails_returns_503() {
    let (app, mock_kv, _mock_s3) = create_mock_app();

    // Make get_total_files fail (used in check_health)
    mock_kv.set_failing("get_total_files");

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

    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "Health should return 503 when DB query fails"
    );

    use axum::body::to_bytes;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_ne!(
        json["status"], "ok",
        "Health status should not be 'ok' when DB fails"
    );
    assert_eq!(
        json["checks"]["database"], "error",
        "Database check should report 'error'"
    );
}
