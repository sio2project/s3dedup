use axum::Router;
use axum::body::Body;
use axum::http::{Response, StatusCode};
use axum::routing::{delete, get};
use bytes::Bytes;
use futures_util::stream;
use s3dedup::filetracker_client::{DownloadedFile, FiletrackerClient};
use std::time::Duration;

/// Spin up an Axum server on a random port and return its base URL.
async fn create_mock_server(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    format!("http://{}", addr)
}

// ---------------------------------------------------------------------------
// 1. Missing Last-Modified header -> error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_head_file_missing_last_modified() {
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "0")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let result = client.head_file("test/missing_lm.txt").await;
    match result {
        Err(err) => {
            let err_msg = format!("{}", err);
            assert!(
                err_msg.contains("Last-Modified"),
                "Error should mention Last-Modified, got: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Expected error when Last-Modified is missing"),
    }
}

#[tokio::test]
async fn test_download_file_missing_last_modified() {
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "5")
                .body(Body::from("hello"))
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let result = client.download_file("test/missing_lm.txt", 1024).await;
    assert!(
        result.is_err(),
        "Expected error when Last-Modified is missing"
    );
}

// ---------------------------------------------------------------------------
// 2. Invalid RFC2822 timestamp -> error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_head_file_invalid_timestamp() {
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("Last-Modified", "not-a-date")
                .header("Content-Length", "0")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let result = client.head_file("test/bad_date.txt").await;
    assert!(
        result.is_err(),
        "Expected error for invalid RFC2822 timestamp"
    );
}

#[tokio::test]
async fn test_download_file_invalid_timestamp() {
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("Last-Modified", "not-a-date")
                .header("Content-Length", "5")
                .body(Body::from("hello"))
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let result = client.download_file("test/bad_date.txt", 1024).await;
    assert!(
        result.is_err(),
        "Expected error for invalid RFC2822 timestamp"
    );
}

// ---------------------------------------------------------------------------
// 3. Missing Logical-Size defaults to 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_head_file_missing_logical_size_defaults_to_zero() {
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("Last-Modified", "Thu, 01 Jan 2026 00:00:00 +0000")
                .header("Content-Length", "42")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let headers = client
        .head_file("test/no_logical_size.txt")
        .await
        .expect("head_file should succeed without Logical-Size");
    assert_eq!(headers.logical_size, 0, "logical_size should default to 0");
    assert_eq!(headers.content_length, 42);
    assert!(!headers.is_compressed);
}

// ---------------------------------------------------------------------------
// 4. DELETE 404 treated as success
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delete_file_404_is_ok() {
    let app = Router::new().route(
        "/files/{*path}",
        delete(|| async {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let result = client.delete_file("test/nonexistent.txt", 1000000).await;
    assert!(
        result.is_ok(),
        "DELETE returning 404 should be treated as success"
    );
}

// ---------------------------------------------------------------------------
// 5. list_files returns paths with leading slashes as-is
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_files_preserves_leading_slashes() {
    let app = Router::new()
        .route(
            "/list/",
            get(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("/dir/file1.txt\n/dir/file2.txt\n"))
                    .unwrap()
            }),
        )
        .route(
            "/list/{*path}",
            get(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("/dir/file1.txt\n/dir/file2.txt\n"))
                    .unwrap()
            }),
        );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let files = client
        .list_files("dir", None)
        .await
        .expect("list_files should succeed");
    assert_eq!(files.len(), 2);
    assert_eq!(files[0], "/dir/file1.txt");
    assert_eq!(files[1], "/dir/file2.txt");
}

// ---------------------------------------------------------------------------
// 6. download_file without Content-Length streams to temp file (OnDisk)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_file_missing_content_length_streams_to_disk() {
    // When Content-Length is absent, reqwest reports content_length() as None (mapped to 0).
    // The condition `content_length > 0 && content_length <= max_inmemory_size` is false,
    // so download_file takes the OnDisk streaming path.
    //
    // We use a streaming body so hyper doesn't infer Content-Length from a known-size body.
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            let stream = stream::once(async { Ok::<_, std::io::Error>(Bytes::from("hello")) });
            let body = Body::from_stream(stream);
            Response::builder()
                .status(StatusCode::OK)
                .header("Last-Modified", "Thu, 01 Jan 2026 00:00:00 +0000")
                .body(body)
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let result = client
        .download_file("test/no_cl.txt", 1024)
        .await
        .expect("download should succeed");

    match result {
        DownloadedFile::OnDisk(meta) => {
            assert_eq!(meta.data_size, 5, "Should have streamed 5 bytes to disk");
            assert!(meta.temp_path.exists(), "Temp file should exist");
        }
        DownloadedFile::InMemory(_) => {
            panic!("Expected OnDisk variant when Content-Length is absent");
        }
    }
}

// ---------------------------------------------------------------------------
// 7. head_file with all headers present - normal case
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_head_file_all_headers_present() {
    let app = Router::new().route(
        "/files/{*path}",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("Last-Modified", "Wed, 15 Jan 2025 12:30:00 +0000")
                .header("Logical-Size", "9999")
                .header("Content-Length", "4567")
                .header("Content-Encoding", "gzip")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let url = create_mock_server(app).await;
    let client = FiletrackerClient::new(url);

    let headers = client
        .head_file("test/all_headers.txt")
        .await
        .expect("head_file should succeed with all headers");

    // Wed, 15 Jan 2025 12:30:00 +0000 => Unix timestamp
    let expected_ts = chrono::DateTime::parse_from_rfc2822("Wed, 15 Jan 2025 12:30:00 +0000")
        .unwrap()
        .timestamp();
    assert_eq!(headers.last_modified, expected_ts);
    assert_eq!(headers.logical_size, 9999);
    assert_eq!(headers.content_length, 4567);
    assert!(
        headers.is_compressed,
        "Content-Encoding: gzip should set is_compressed=true"
    );
}
