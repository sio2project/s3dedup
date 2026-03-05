mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use s3dedup::AppState;
use s3dedup::config::Config;
use std::sync::Arc;
use tower::util::ServiceExt;

async fn create_test_app_state() -> Arc<AppState> {
    let (config, _unique_id) = common::create_test_config("test-metrics");
    let app_state = AppState::new(&config).await.unwrap();
    app_state.kvstorage.lock().await.setup().await.unwrap();
    app_state
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_endpoint_exists() {
    let app_state = create_test_app_state().await;

    let app = Router::new()
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .with_state(app_state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_format() {
    let app_state = create_test_app_state().await;

    let app = Router::new()
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .with_state(app_state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Check for Prometheus format - should have HELP and TYPE comments
    assert!(
        body_str.contains("# HELP"),
        "Metrics should contain HELP comments"
    );
    assert!(
        body_str.contains("# TYPE"),
        "Metrics should contain TYPE comments"
    );

    // Check for some expected metrics
    assert!(
        body_str.contains("s3dedup_uptime_seconds"),
        "Should contain uptime metric"
    );
    // Note: HTTP requests metric only appears after at least one request is made
    // We just check that the metrics endpoint returns valid Prometheus format
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_after_get_request() {
    let app_state = create_test_app_state().await;

    // Create a router with both GET and metrics endpoints
    let app = Router::new()
        .route(
            "/ft/files/{*path}",
            get(s3dedup::routes::ft::get_file::ft_get_file),
        )
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .with_state(app_state);

    // Make a GET request that will return 404
    let app1 = app.clone();
    let _response = app1
        .oneshot(
            Request::builder()
                .uri("/ft/files/nonexistent.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Check metrics endpoint
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Should have recorded the GET request with 404 status
    assert!(
        body_str.contains("s3dedup_http_requests_total"),
        "Should contain HTTP requests metric"
    );
    assert!(
        body_str.contains("GET"),
        "Should contain GET method in metrics"
    );
    assert!(
        body_str.contains("404"),
        "Should contain 404 status in metrics"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_uptime_increases() {
    let app_state = create_test_app_state().await;

    let app = Router::new()
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .with_state(app_state.clone());

    // First request
    let response1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX)
        .await
        .unwrap();
    let body1_str = String::from_utf8(body1.to_vec()).unwrap();

    // Extract uptime value from first response
    let uptime1 = body1_str
        .lines()
        .find(|line| line.starts_with("s3dedup_uptime_seconds "))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|val| val.parse::<i64>().ok())
        .expect("Should have uptime metric");

    // Wait a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Second request
    let response2 = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let body2_str = String::from_utf8(body2.to_vec()).unwrap();

    let uptime2 = body2_str
        .lines()
        .find(|line| line.starts_with("s3dedup_uptime_seconds "))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|val| val.parse::<i64>().ok())
        .expect("Should have uptime metric");

    assert!(
        uptime2 >= uptime1,
        "Uptime should increase or stay the same"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_migration_active_metric() {
    let (mut bucket_config, unique_id) = common::create_test_bucket_config("test-migration");
    bucket_config.filetracker_url = Some("http://localhost:8000".to_string());

    std::fs::create_dir_all("db").ok();

    let config = Config {
        logging: s3dedup::logging::LoggingConfig {
            level: "info".to_string(),
            json: false,
        },
        kvstorage_type: s3dedup::config::KVStorageType::SQLite,
        sqlite: Some(s3dedup::config::SQLiteConfig {
            path: format!("db/test-migration-{}.db", unique_id),
            pool_size: 10,
        }),
        postgres: None,
        locks_type: s3dedup::config::LocksType::Memory,
        bucket: bucket_config,
    };

    let app_state = AppState::new_with_filetracker(&config, "http://localhost:8000".to_string())
        .await
        .unwrap();
    app_state.kvstorage.lock().await.setup().await.unwrap();

    let app = Router::new()
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .with_state(app_state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Should have migration_active set to 1
    assert!(
        body_str.contains("s3dedup_migration_active"),
        "Should contain migration_active metric"
    );

    // Find the value
    let migration_active = body_str
        .lines()
        .find(|line| line.starts_with("s3dedup_migration_active "))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|val| val.parse::<i64>().ok());

    assert_eq!(
        migration_active,
        Some(1),
        "Migration should be marked as active"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_json_endpoint() {
    let app_state = create_test_app_state().await;

    let app = Router::new()
        .route(
            "/metrics/json",
            get(s3dedup::routes::metrics::metrics_json_handler),
        )
        .with_state(app_state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Parse as JSON to verify it's valid JSON
    let json: serde_json::Value = serde_json::from_str(&body_str).unwrap();

    // Verify it's an object
    assert!(json.is_object(), "Response should be a JSON object");

    // Verify it contains uptime metric
    assert!(
        json.get("s3dedup_uptime_seconds").is_some(),
        "Should contain uptime metric"
    );
}
