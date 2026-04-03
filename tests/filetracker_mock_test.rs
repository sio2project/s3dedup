mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use axum::routing::get;
use s3dedup::AppState;
use s3dedup::filetracker_client::FiletrackerClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;

// ============================================================
// Mock Filetracker Server
// ============================================================

/// Type alias for file storage: (data, timestamp)
type FileStorage = Arc<Mutex<HashMap<String, (Vec<u8>, i64)>>>;

/// Mock filetracker server state — normal (working) variant.
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

    async fn has_file(&self, path: &str) -> bool {
        self.files.lock().await.contains_key(path)
    }
}

// --- Normal mock filetracker handlers ---

async fn mock_ft_list(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
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

async fn mock_ft_get(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Response<Body> {
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

async fn mock_ft_head(
    axum::extract::State(state): axum::extract::State<MockFiletrackerState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Response<Body> {
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
                .body(Body::empty())
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

/// Create a normal (working) mock filetracker server.
async fn create_mock_filetracker() -> (MockFiletrackerState, String) {
    let state = MockFiletrackerState::new();

    let app = Router::new()
        .route("/list/", get(mock_ft_list))
        .route("/list/{*path}", get(mock_ft_list))
        .route(
            "/files/{*path}",
            get(mock_ft_get)
                .head(mock_ft_head)
                .put(mock_ft_put)
                .delete(mock_ft_delete),
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

// ============================================================
// Failing Mock Filetracker Server — returns 500 on specific ops
// ============================================================

/// Mock filetracker that returns 500 on PUT (simulating a broken filetracker).
async fn failing_ft_put(
    axum::extract::Path(_path): axum::extract::Path<String>,
    _body: Body,
) -> Response<Body> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Mock internal server error"))
        .unwrap()
}

/// Create a mock filetracker that fails on PUT but works for GET/HEAD/DELETE/LIST.
async fn create_failing_put_filetracker() -> (MockFiletrackerState, String) {
    let state = MockFiletrackerState::new();

    let app = Router::new()
        .route("/list/", get(mock_ft_list))
        .route("/list/{*path}", get(mock_ft_list))
        .route(
            "/files/{*path}",
            get(mock_ft_get)
                .head(mock_ft_head)
                .put(failing_ft_put)
                .delete(mock_ft_delete),
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

// ============================================================
// Test Helpers
// ============================================================

/// Create an s3dedup AppState with a filetracker client pointing to the given URL.
async fn create_test_app_with_ft(ft_url: &str) -> (Router, Arc<AppState>) {
    use s3dedup::kvstorage::KVStorage;
    use s3dedup::locks::LocksStorage;
    use s3dedup::s3storage::S3Storage;

    let (config, _unique_id) = common::create_test_config("ftmock");

    let kvstorage = KVStorage::new(&config).await.unwrap();
    let locks = LocksStorage::new_with_config(config.locks_type, &config)
        .await
        .unwrap();
    let s3storage = S3Storage::new(&config.bucket).await.unwrap();

    let ft_client = FiletrackerClient::new(ft_url.to_string());

    let app_state = Arc::new(AppState {
        bucket_name: config.bucket.name.clone(),
        kvstorage: Arc::new(*kvstorage),
        locks: Arc::new(*locks),
        s3storage: Arc::new(*s3storage),
        filetracker_client: Some(Arc::new(ft_client)),
        metrics: Arc::new(s3dedup::metrics::Metrics::new()),
        max_inmemory_size: 64 * 1024 * 1024,
    });

    app_state.kvstorage.setup().await.unwrap();

    let router = Router::new()
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file)
                .head(s3dedup::routes::ft::head_file::ft_head_file)
                .put(s3dedup::routes::ft::put_file::ft_put_file)
                .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
        )
        .with_state(app_state.clone());

    (router, app_state)
}

/// Build a PUT request with fast-path headers (gzip + SHA256 + Logical-Size).
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
// 1. Dual-write PUT: filetracker PUT fails -> still returns 200
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_dual_write_put_ft_fails_returns_200() {
    let (_mock_state, ft_url) = create_failing_put_filetracker().await;
    let (app, _state) = create_test_app_with_ft(&ft_url).await;

    let ts = make_rfc2822(1700010000);
    let content = b"dual-write failure test content";
    let req = build_put_request("ftmock/dual_write_fail.txt", content, &ts);

    let response = app.oneshot(req).await.unwrap();

    // s3dedup write succeeded; filetracker failure is non-fatal
    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================
// 2. Dual-write PUT: normal operation (both succeed)
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_dual_write_put_both_succeed() {
    let (mock_state, ft_url) = create_mock_filetracker().await;
    let (app, _state) = create_test_app_with_ft(&ft_url).await;

    let ts = make_rfc2822(1700010100);
    let content = b"dual-write success content";
    let req = build_put_request("ftmock/dual_write_ok.txt", content, &ts);

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify the mock filetracker received the file
    assert!(
        mock_state.has_file("ftmock/dual_write_ok.txt").await,
        "File should have been written to mock filetracker"
    );
}

// ============================================================
// 3. GET fallback: file in filetracker but not s3dedup -> 200
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_fallback_migrates_from_ft() {
    use tower::Service;

    let (mock_state, ft_url) = create_mock_filetracker().await;
    let (mut app, _state) = create_test_app_with_ft(&ft_url).await;

    // Add file only to mock filetracker, not to s3dedup
    let content = b"filetracker-only content for GET fallback";
    mock_state
        .add_file("ftmock/ft_only_get.txt", content.to_vec())
        .await;

    // GET from s3dedup — should fall back to filetracker, migrate on the fly
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/ftmock/ft_only_get.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = s3dedup::routes::ft::storage_helpers::decompress_gzip(&body).unwrap();
    assert_eq!(decompressed, content);

    // Second GET should now be served from s3dedup (migration happened)
    let get_response2 = app
        .call(
            Request::builder()
                .uri("/ft/files/ftmock/ft_only_get.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response2.status(), StatusCode::OK);
}

// ============================================================
// 4. HEAD fallback: file in filetracker but not s3dedup -> 200
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_head_fallback_returns_ft_headers() {
    let (mock_state, ft_url) = create_mock_filetracker().await;
    let (app, _state) = create_test_app_with_ft(&ft_url).await;

    // Add file only to mock filetracker
    let content = b"filetracker-only content for HEAD fallback";
    mock_state
        .add_file("ftmock/ft_only_head.txt", content.to_vec())
        .await;

    // HEAD from s3dedup — should fall back to filetracker
    let head_response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/ftmock/ft_only_head.txt")
                .method("HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(head_response.status(), StatusCode::OK);

    // Verify headers from filetracker are forwarded
    assert!(
        head_response.headers().contains_key("Last-Modified"),
        "HEAD fallback should include Last-Modified"
    );
    assert_eq!(
        head_response
            .headers()
            .get("Logical-Size")
            .unwrap()
            .to_str()
            .unwrap(),
        content.len().to_string(),
        "HEAD fallback should include correct Logical-Size"
    );
}

// ============================================================
// 5. GET fallback: file not in either system -> 404
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_fallback_not_found_returns_404() {
    let (_mock_state, ft_url) = create_mock_filetracker().await;
    let (app, _state) = create_test_app_with_ft(&ft_url).await;

    let get_response = app
        .oneshot(
            Request::builder()
                .uri("/ft/files/ftmock/totally_missing.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

// ============================================================
// 6. DELETE dual-delete: also deletes from filetracker
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_dual_delete_removes_from_ft() {
    use tower::Service;

    let (mock_state, ft_url) = create_mock_filetracker().await;
    let (mut app, _state) = create_test_app_with_ft(&ft_url).await;

    let ts = make_rfc2822(1700010500);
    let content = b"file to be dual-deleted";

    // PUT to s3dedup (dual-write also puts to FT)
    let req = build_put_request("ftmock/dual_del.txt", content, &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify file exists in mock FT after dual-write
    assert!(
        mock_state.has_file("ftmock/dual_del.txt").await,
        "File should exist in mock FT after PUT"
    );

    // DELETE from s3dedup (dual-delete should also delete from FT)
    let req = build_delete_request("ftmock/dual_del.txt", &ts);
    let response = app.call(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify file was removed from mock filetracker
    assert!(
        !mock_state.has_file("ftmock/dual_del.txt").await,
        "File should have been removed from mock FT after DELETE"
    );

    // Verify file is also gone from s3dedup
    let get_response = app
        .call(
            Request::builder()
                .uri("/ft/files/ftmock/dual_del.txt")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

// ============================================================
// 7. FT list_files fails → migrate_all_files returns error
// ============================================================

/// Mock FT that returns 500 on list endpoint.
async fn create_failing_list_filetracker() -> String {
    async fn failing_list() -> Response<Body> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("list failed"))
            .unwrap()
    }

    let app = Router::new()
        .route("/list/", get(failing_list))
        .route("/list/{*path}", get(failing_list));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    url
}

#[tokio::test(flavor = "multi_thread")]
async fn test_migrate_all_files_list_fails_returns_error() {
    let ft_url = create_failing_list_filetracker().await;
    let (_app, app_state) = create_test_app_with_ft(&ft_url).await;

    let ft_client = Arc::new(FiletrackerClient::new(ft_url));
    let result = s3dedup::migration::migrate_all_files(ft_client, app_state, 4).await;

    assert!(
        result.is_err(),
        "migrate_all_files should fail when FT list_files returns 500"
    );
}

// ============================================================
// 8. Dual-write: decompression fails → still returns 200
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_dual_write_decompress_fails_returns_200() {
    let (mock_state, ft_url) = create_mock_filetracker().await;
    let (app, _state) = create_test_app_with_ft(&ft_url).await;

    let ts = make_rfc2822(1700010600);
    let encoded_ts = urlencoding::encode(&ts);

    // Send with Content-Encoding: gzip + SHA256 + Logical-Size headers
    // (triggers slow path with has_headers=true since filetracker_client is set)
    // Body is NOT valid gzip — just random bytes with gzip magic bytes
    let fake_gzip = vec![0x1f, 0x8b, 0x08, 0x00, 0xff, 0xff, 0xff, 0xff, 0xde, 0xad];
    let sha256 = s3dedup::routes::ft::storage_helpers::compute_sha256(&fake_gzip);

    let req = Request::builder()
        .uri(format!(
            "/ft/files/ftmock/decompress_fail.txt?last_modified={}",
            encoded_ts
        ))
        .method("PUT")
        .header("Content-Encoding", "gzip")
        .header("SHA256-Checksum", &sha256)
        .header("Logical-Size", "100")
        .body(Body::from(fake_gzip))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();

    // s3dedup write succeeds (stores raw bytes), but dual-write decompression fails.
    // Handler should still return 200 (skips dual-write, logs error).
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Dual-write decompression failure should not prevent 200 response"
    );

    // Verify file was NOT written to mock filetracker (decompress failed)
    assert!(
        !mock_state.has_file("ftmock/decompress_fail.txt").await,
        "File should NOT be in mock FT since decompression failed"
    );
}
