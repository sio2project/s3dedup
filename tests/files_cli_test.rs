mod common;

use axum::Router;
use axum::routing::get;
use s3dedup::files_cli::{DeleteOutcome, FilesClient};
use std::io::Write;
use std::sync::Arc;

/// Start a real s3dedup HTTP server on a random port and return its base URL.
async fn start_server() -> String {
    let (config, _unique_id) = common::create_test_config("cli");
    let app_state = s3dedup::AppState::new(&config).await.unwrap();
    app_state.kvstorage.setup().await.unwrap();

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
        .with_state(Arc::clone(&app_state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_put_get_stat_roundtrip() {
    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();
    let data = b"some test content\n".repeat(10_000);

    let result = client
        .upload(std::io::Cursor::new(data.clone()), "/dir/a.txt", 1700000000)
        .await
        .unwrap();
    assert_eq!(result.logical_size, data.len() as u64);
    assert_eq!(result.server_version, 1700000000);
    assert!(result.compressed_size < data.len() as u64);

    let info = client.stat("dir/a.txt").await.unwrap().unwrap();
    assert_eq!(info.path, "dir/a.txt");
    assert_eq!(info.logical_size, data.len() as u64);
    assert_eq!(info.last_modified, 1700000000);

    let mut out = Vec::new();
    let got = client.download("dir/a.txt", &mut out).await.unwrap();
    assert!(got.is_some());
    assert_eq!(out, data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_missing_file() {
    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();

    assert!(client.stat("nope").await.unwrap().is_none());
    let mut out = Vec::new();
    assert!(client.download("nope", &mut out).await.unwrap().is_none());
    assert_eq!(
        client.delete("nope", 1700000000).await.unwrap(),
        DeleteOutcome::NotFound
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_list_and_delete() {
    let url = start_server().await;
    // Trailing /ft is accepted as well
    let client = FilesClient::new(&format!("{}/ft/", url)).unwrap();

    for path in ["x/1", "x/y/2", "z/3"] {
        client
            .upload(
                std::io::Cursor::new(path.as_bytes().to_vec()),
                path,
                1700000000,
            )
            .await
            .unwrap();
    }

    assert_eq!(client.list("").await.unwrap(), vec!["x/1", "x/y/2", "z/3"]);
    assert_eq!(client.list("/x/").await.unwrap(), vec!["x/1", "x/y/2"]);

    let infos = client
        .stat_many(&client.list("x").await.unwrap())
        .await
        .unwrap();
    assert_eq!(infos.len(), 2);
    assert_eq!(infos[1].path, "x/y/2");
    assert_eq!(infos[1].logical_size, 5);

    assert_eq!(
        client.delete("x/1", 1700000001).await.unwrap(),
        DeleteOutcome::Deleted
    );
    assert_eq!(client.list("").await.unwrap(), vec!["x/y/2", "z/3"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_older_version_is_ignored() {
    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();

    client
        .upload(std::io::Cursor::new(b"new".to_vec()), "v", 1700000100)
        .await
        .unwrap();
    let result = client
        .upload(std::io::Cursor::new(b"old".to_vec()), "v", 1700000000)
        .await
        .unwrap();
    assert_eq!(result.server_version, 1700000100);

    let mut out = Vec::new();
    client.download("v", &mut out).await.unwrap();
    assert_eq!(out, b"new");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_future_version_listed_and_not_deleted() {
    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();
    let future = chrono::Utc::now().timestamp() + 3600;

    client
        .upload(std::io::Cursor::new(b"f".to_vec()), "future", future)
        .await
        .unwrap();
    assert_eq!(client.list("").await.unwrap(), vec!["future"]);

    let now = chrono::Utc::now().timestamp();
    assert_eq!(
        client.delete("future", now).await.unwrap(),
        DeleteOutcome::NewerKept(future)
    );
    assert!(client.stat("future").await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_special_paths_and_empty_file() {
    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();

    for path in ["a b/c%20d@1+x", "q/#hash?x"] {
        client
            .upload(
                std::io::Cursor::new(path.as_bytes().to_vec()),
                path,
                1700000000,
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        client.download(path, &mut out).await.unwrap().unwrap();
        assert_eq!(out, path.as_bytes());
    }

    let result = client
        .upload(std::io::Cursor::new(Vec::new()), "empty", 1700000000)
        .await
        .unwrap();
    assert_eq!(result.logical_size, 0);
    let mut out = Vec::new();
    let info = client.download("empty", &mut out).await.unwrap().unwrap();
    assert_eq!(info.logical_size, 0);
    assert!(out.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_rejects_bad_paths_and_versions() {
    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();

    for path in ["", "/", "a/../b", "a/./b", "a//b"] {
        assert!(
            client
                .upload(std::io::Cursor::new(b"x".to_vec()), path, 1700000000)
                .await
                .is_err(),
            "{:?}",
            path
        );
        assert!(client.delete(path, 1700000000).await.is_err());
    }
    assert!(
        client
            .upload(std::io::Cursor::new(b"x".to_vec()), "ok", 1700000000000)
            .await
            .is_err()
    );
    assert!(client.list("").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_download_multi_member_gzip() {
    use sha2::{Digest, Sha256};

    let url = start_server().await;
    let client = FilesClient::new(&url).unwrap();

    // Two concatenated gzip members, as another client might upload them.
    let mut body = Vec::new();
    for part in [&b"aaa"[..], &b"bbb"[..]] {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(part).unwrap();
        body.extend(encoder.finish().unwrap());
    }
    let response = reqwest::Client::new()
        .put(format!(
            "{}/ft/files/multi?last_modified=Tue,%2014%20Nov%202023%2022:13:20%20%2B0000",
            url
        ))
        .header("Content-Encoding", "gzip")
        .header("Logical-Size", "6")
        .header(
            "SHA256-Checksum",
            format!("{:x}", Sha256::digest(b"aaabbb")),
        )
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    let mut out = Vec::new();
    client.download("multi", &mut out).await.unwrap().unwrap();
    assert_eq!(out, b"aaabbb");
}
