mod common;

use axum::Router;
use axum::routing::get;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tower::ServiceExt;

async fn create_test_app() -> Router {
    let (router, _) = create_test_app_with_state().await;
    router
}

async fn create_test_app_with_state() -> (Router, Arc<s3dedup::AppState>) {
    use s3dedup::AppState;
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;

    let (config, _unique_id) = common::create_test_config("edge");

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
        temp_dir: std::env::temp_dir(),
        cleaner,
    });

    app_state.kvstorage.setup().await.unwrap();

    let router = Router::new()
        .route("/ft/version", get(s3dedup::routes::ft::version::ft_version))
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
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .route(
            "/metrics/json",
            get(s3dedup::routes::metrics::metrics_json_handler),
        )
        .with_state(app_state.clone());

    (router, app_state)
}

/// Helper: PUT a file with fast path (all 3 headers).
/// Returns the timestamp string used.
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

fn build_delete_request(path: &str, timestamp_rfc2822: &str) -> Request<Body> {
    let encoded_ts = urlencoding::encode(timestamp_rfc2822);
    Request::builder()
        .uri(format!("/ft/files/{}?last_modified={}", path, encoded_ts))
        .method("DELETE")
        .body(Body::empty())
        .unwrap()
}

fn make_rfc2822(unix_ts: i64) -> String {
    use chrono::{DateTime, Utc};
    DateTime::<Utc>::from_timestamp(unix_ts, 0)
        .unwrap()
        .to_rfc2822()
}

// ============================================================
// 1. Version Endpoint
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_version_endpoint() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/version")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["protocol_versions"], serde_json::json!([2]));
}

// ============================================================
// 2. Timestamp Edge Cases
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_with_timestamp_zero_rejected() {
    let app = create_test_app().await;

    let ts = "Thu, 01 Jan 1970 00:00:00 +0000"; // Unix timestamp 0
    let req = build_put_request("edge/ts_zero.txt", b"content", ts);

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put_with_negative_timestamp_rejected() {
    let app = create_test_app().await;

    let ts = "Wed, 31 Dec 1969 23:00:00 +0000"; // Unix timestamp -3600
    let req = build_put_request("edge/ts_neg.txt", b"content", ts);

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_timestamp_zero_rejected() {
    let app = create_test_app().await;

    let ts = "Thu, 01 Jan 1970 00:00:00 +0000";
    let req = build_delete_request("edge/ts_zero.txt", ts);

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put_with_identical_timestamp_overwrites() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700000000);
    let content1 = b"original content";
    let content2 = b"updated content";

    // First PUT
    let req = build_put_request("edge/same_ts.txt", content1, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Second PUT with same timestamp, different content
    let req = build_put_request("edge/same_ts.txt", content2, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET should return updated content
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/same_ts.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_exact_timestamp_succeeds() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700000100);

    // PUT a file
    let req = build_put_request("edge/del_exact_ts.txt", b"to delete", &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // DELETE with exact same timestamp
    let req = build_delete_request("edge/del_exact_ts.txt", &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET should return 404
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/del_exact_ts.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

// ============================================================
// 3. PUT without required timestamp
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_without_timestamp_returns_400() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/edge/no_ts.txt")
                .method("PUT")
                .header("Content-Encoding", "gzip")
                .header("SHA256-Checksum", "abc123")
                .header("Logical-Size", "10")
                .body(Body::from(vec![1, 2, 3]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_without_timestamp_returns_400() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/edge/no_ts.txt")
                .method("DELETE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================
// 4. LIKE Wildcard Injection
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_list_files_like_wildcard_not_injected() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700000200);

    // Upload files with normal paths
    let req = build_put_request("like_test/file_one.txt", b"content1", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let req = build_put_request("like_test/file_two.txt", b"content2", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let req = build_put_request("like_test/other.txt", b"content3", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // LIST with wildcard-containing prefix: "like_test/file_" should NOT match "like_test/other.txt"
    // If _ is not escaped, LIKE "like_test/file_%"  would match "like_test/file_one.txt" and "like_test/file_two.txt"
    // (same as escaped), BUT "like_test/file%" without underscore would match all three via the % wildcard
    // The key test: prefix with % should only match literal %
    let encoded_ts = urlencoding::encode(&ts);
    let list_response = app
        .call(
            Request::builder()
                .uri(format!(
                    "/ft/list/like_test/file%25?last_modified={}",
                    encoded_ts
                ))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Prefix "like_test/file%" contains a literal %. No files have "%" in their path,
    // so we expect empty result if LIKE wildcards are properly escaped.
    assert_eq!(
        body_str, "",
        "LIKE wildcard % in prefix should be escaped and match no files since no path contains literal %"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_files_underscore_wildcard_escaped() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700000300);

    // Upload files
    let req = build_put_request("uscore/a_b.txt", b"ab", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let req = build_put_request("uscore/axb.txt", b"axb", &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // LIST with prefix "uscore/a_b" — underscore should be literal, not wildcard
    let encoded_ts = urlencoding::encode(&ts);
    let list_response = app
        .call(
            Request::builder()
                .uri(format!("/ft/list/uscore/a_b?last_modified={}", encoded_ts))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Should only match "uscore/a_b.txt", NOT "uscore/axb.txt"
    let files: Vec<&str> = body_str
        .trim()
        .split('\n')
        .filter(|s| !s.is_empty())
        .collect();
    assert_eq!(
        files.len(),
        1,
        "Underscore should be literal, got: {:?}",
        files
    );
    assert!(files[0].contains("a_b.txt"));
}

// ============================================================
// 5. Zero-byte File Deduplication
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zero_byte_file_put_get() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700000400);
    let empty_content: &[u8] = b"";

    let req = build_put_request("edge/empty1.txt", empty_content, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET should return the empty file
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/empty1.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(get_response.headers().get("Logical-Size").unwrap(), "0");

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_zero_byte_files_dedup() {
    use tower::Service;
    let (mut app, state) = create_test_app_with_state().await;

    let ts = make_rfc2822(1700000500);
    let empty_content: &[u8] = b"";
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(empty_content);

    // PUT two empty files at different paths
    let req = build_put_request("edge/empty_a.txt", empty_content, &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    let req = build_put_request("edge/empty_b.txt", empty_content, &ts);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Both should share the same blob — check refcount is 2
    let refcount = state
        .kvstorage
        .get_ref_count(&state.bucket_name, &sha256)
        .await
        .unwrap();
    assert_eq!(
        refcount, 2,
        "Two empty files should share same blob with refcount=2"
    );
}

// ============================================================
// 6. PUT with Content-Encoding: deflate (not gzip)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_with_deflate_encoding_treated_as_uncompressed() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700000600);
    let content = b"deflate test content";

    // Send with content-encoding: deflate — the server should treat this as uncompressed
    // (since it only recognizes gzip), enter slow path, compute hash, and compress with gzip
    let encoded_ts = urlencoding::encode(&ts);
    let req = Request::builder()
        .uri(format!(
            "/ft/files/edge/deflate.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .header("Content-Encoding", "deflate")
        .body(Body::from(content.to_vec()))
        .unwrap();

    let response = app.call(req).await.unwrap();
    // Should succeed (treated as uncompressed body)
    assert_eq!(response.status(), StatusCode::OK);

    // GET should return gzip-compressed data that decompresses to original content
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/deflate.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);
}

// ============================================================
// 7. DELETE older timestamp is ignored
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_older_timestamp_ignored() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts_new = make_rfc2822(1700001000);
    let ts_old = make_rfc2822(1700000000);

    // PUT with newer timestamp
    let req = build_put_request("edge/del_old.txt", b"keep me", &ts_new);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // DELETE with older timestamp — should be ignored
    let req = build_delete_request("edge/del_old.txt", &ts_old);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // File should still exist
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/del_old.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
}

// ============================================================
// 8. PUT older timestamp is skipped
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_older_timestamp_skipped() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts_new = make_rfc2822(1700002000);
    let ts_old = make_rfc2822(1700001000);

    // PUT with newer timestamp
    let req = build_put_request("edge/put_old.txt", b"new content", &ts_new);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // PUT with older timestamp — should be skipped
    let req = build_put_request("edge/put_old.txt", b"old content", &ts_old);
    let r = app.call(req).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // GET should return new content
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/put_old.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, b"new content");
}

// ============================================================
// 9. Metrics with empty DB
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_json_empty_db() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics/json")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Verify metrics JSON is a non-empty object (Prometheus metrics as keys)
    assert!(json.is_object());
    assert!(!json.as_object().unwrap().is_empty());
}

// ============================================================
// 10. Health endpoint
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_health_endpoint_returns_healthy() {
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

    assert_eq!(response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

// ============================================================
// 11. HEAD of nonexistent file returns 404
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_head_nonexistent_file_returns_404() {
    let app = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/edge/absolutely_does_not_exist.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ============================================================
// 12. DELETE of nonexistent file returns 404
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_nonexistent_file_returns_404() {
    let app = create_test_app().await;

    let ts = make_rfc2822(1700003000);
    let req = build_delete_request("edge/nonexistent_del.txt", &ts);

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ============================================================
// 13. PUT fast path with empty SHA256-Checksum header
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_fast_path_empty_checksum() {
    let app = create_test_app().await;

    let ts = make_rfc2822(1700003100);
    let encoded_ts = urlencoding::encode(&ts);

    // Send with empty SHA256-Checksum — should fall to slow path or handle gracefully
    let req = Request::builder()
        .uri(format!(
            "/ft/files/edge/empty_checksum.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .header("Content-Encoding", "gzip")
        .header("SHA256-Checksum", "")
        .header("Logical-Size", "10")
        .body(Body::from(
            s3dedup::routes::ft::storage_helpers::compress_gzip(b"test data!").unwrap(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    // The server should handle this without panic (either 200 or 400/500)
    // We just verify it doesn't crash
    let status = response.status();
    assert!(
        status == StatusCode::OK
            || status == StatusCode::BAD_REQUEST
            || status == StatusCode::INTERNAL_SERVER_ERROR,
        "Unexpected status: {}",
        status
    );
}

// ============================================================
// 14. PUT with inconsistent Logical-Size header
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_fast_path_wrong_logical_size() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700003200);
    let content = b"actual content here";
    let compressed = s3dedup::routes::ft::storage_helpers::compress_gzip(content).unwrap();
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(content);
    let encoded_ts = urlencoding::encode(&ts);

    // Send with wrong logical-size (0 instead of actual)
    let req = Request::builder()
        .uri(format!(
            "/ft/files/edge/wrong_size.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .header("Content-Encoding", "gzip")
        .header("SHA256-Checksum", &sha256)
        .header("Logical-Size", "0")
        .body(Body::from(compressed))
        .unwrap();

    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // HEAD should show the stored (wrong) logical size
    let head_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/wrong_size.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head_response.status(), StatusCode::OK);
    assert_eq!(
        head_response.headers().get("Logical-Size").unwrap(),
        "0",
        "Server stores the provided Logical-Size header value as-is"
    );
}

// ============================================================
// 15. PUT slow path (uncompressed, no checksum)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_slow_path_uncompressed_no_headers() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700003300);
    let content = b"slow path uncompressed content";
    let encoded_ts = urlencoding::encode(&ts);

    // No Content-Encoding, no SHA256-Checksum, no Logical-Size → slow path
    let req = Request::builder()
        .uri(format!(
            "/ft/files/edge/slow_path.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .body(Body::from(content.to_vec()))
        .unwrap();

    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET and verify content roundtrips
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/slow_path.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);
}

// ============================================================
// 16. Long file paths (>255 chars) — validates TEXT columns work
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_get_long_path() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700003400);
    // 300-char path — would fail with VARCHAR(255)
    let long_path = format!("edge/{}/file.txt", "a".repeat(290));
    let content = b"long path content";

    let req = build_put_request(&long_path, content, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let get_response = app
        .call(
            Request::builder()
                .uri(format!("/ft/files/{}", long_path))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);
}

// ============================================================
// 17. Unicode in file paths
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_get_unicode_path() {
    use tower::Service;
    let mut app = create_test_app().await;

    let ts = make_rfc2822(1700003500);
    let content = b"unicode path content";

    let req = build_put_request("edge/zażółć/gęślą/jaźń.txt", content, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/za%C5%BC%C3%B3%C5%82%C4%87/g%C4%99%C5%9Bl%C4%85/ja%C5%BA%C5%84.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);
}

// ============================================================
// 18. max_inmemory_size = 0 forces temp file path
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_max_inmemory_size_zero_forces_tempfile() {
    use s3dedup::AppState;
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;

    let (config, _unique_id) = common::create_test_config("inmem0");

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
        max_inmemory_size: 0, // Force temp file for ALL sizes
        temp_dir: std::env::temp_dir(),
        cleaner,
    });

    app_state.kvstorage.setup().await.unwrap();

    let router = Router::new()
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file)
                .put(s3dedup::routes::ft::put_file::ft_put_file),
        )
        .with_state(app_state);

    use tower::Service;
    let mut app = router;

    let ts = make_rfc2822(1700003600);
    let content = b"small content via tempfile";

    let req = build_put_request("edge/inmem0.txt", content, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/inmem0.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    use axum::body::to_bytes;
    let body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);
}

// ============================================================
// 19. Content-Length mismatch: header says 100, body has 10 bytes
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_put_content_length_mismatch_handles_gracefully() {
    let app = create_test_app().await;

    let ts = make_rfc2822(1700003700);
    let encoded_ts = urlencoding::encode(&ts);
    let body_data = b"short body";

    // Send with Content-Length: 100 but only 10 bytes in body
    let req = Request::builder()
        .uri(format!(
            "/ft/files/edge/content_len_mismatch.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .header("Content-Length", "100")
        .body(Body::from(body_data.to_vec()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    // The server should handle this without crashing — either succeed
    // (processing only the bytes it received) or return an error.
    let status = response.status();
    assert!(
        status == StatusCode::OK
            || status == StatusCode::BAD_REQUEST
            || status == StatusCode::INTERNAL_SERVER_ERROR,
        "Unexpected status on Content-Length mismatch: {}",
        status
    );
}

// ============================================================
// 20. HEAD with compressed_size=0 and S3 object deleted -> Content-Length: 0
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_head_after_s3_object_deleted_returns_content_length_zero() {
    use tower::Service;
    let (mut app, state) = create_test_app_with_state().await;

    let ts = make_rfc2822(1700003800);
    let content = b"content that will lose its S3 object";

    // PUT the file normally
    let req = build_put_request("edge/s3_deleted_head.txt", content, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get the hash so we can delete the S3 object directly
    let hash = state
        .kvstorage
        .get_ref_file(&state.bucket_name, "edge/s3_deleted_head.txt")
        .await
        .unwrap();
    assert!(!hash.is_empty(), "File should have a hash in KV store");

    // Force compressed_size to 0 so HEAD falls through to the S3 head_object path
    state
        .kvstorage
        .set_compressed_size(&state.bucket_name, &hash, 0)
        .await
        .unwrap();

    // Delete the S3 object directly (simulating data loss or cleanup race)
    state.s3storage.delete_object(&hash).await.unwrap();

    // HEAD should still return 200 (metadata exists) but with Content-Length: 0
    let head_response = app
        .call(
            Request::builder()
                .uri("/ft/files/edge/s3_deleted_head.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(head_response.status(), StatusCode::OK);
    assert_eq!(
        head_response
            .headers()
            .get("Content-Length")
            .unwrap()
            .to_str()
            .unwrap(),
        "0",
        "Content-Length should be 0 when compressed_size=0 and S3 object is deleted"
    );
}

// ============================================================
// Empty Bucket Name Behavior
// ============================================================

/// Empty bucket name causes S3 client creation to fail — the AWS SDK
/// rejects empty bucket names. This validates the system fails at startup,
/// not at first request.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_bucket_name_fails_at_s3_creation() {
    use s3dedup::s3storage::S3Storage;

    let (mut config, _unique_id) = common::create_test_config("empty_bucket");
    config.bucket.name = "".to_string();

    let result = S3Storage::new(&config.bucket).await;
    assert!(
        result.is_err(),
        "S3Storage::new should fail with empty bucket name"
    );
}

// ============================================================
// 21. Invalid S3 endpoint → fails at startup
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_s3_endpoint_fails_at_creation() {
    use s3dedup::config::{BucketConfig, S3CompatConfig};
    use s3dedup::s3storage::S3Storage;

    let config = BucketConfig {
        name: "test-bucket".to_string(),
        address: "127.0.0.1".to_string(),
        port: 3000,
        s3storage_type: s3dedup::s3storage::S3StorageType::S3Compat,
        s3: Some(S3CompatConfig {
            endpoint: "not-a-valid-url".to_string(),
            access_key: "key".to_string(),
            secret_key: "secret".to_string(),
            force_path_style: true,
            region: "us-east-1".to_string(),
            key_sharding: Default::default(),
        }),
        cleaner: Default::default(),
        max_inmemory_size: 64 * 1024 * 1024,
        temp_dir: None,
        filetracker_url: None,
        filetracker_v1_dir: None,
    };

    // S3Storage::new calls ensure_bucket_exists, which should fail with an invalid endpoint.
    let result = S3Storage::new(&config).await;
    assert!(
        result.is_err(),
        "S3Storage::new should fail with invalid endpoint"
    );
}
