use axum::Router;
use axum::routing::{get, head};
use s3dedup::routes::ft::get_file::ft_get_file;
use s3dedup::routes::ft::put_file::ft_put_file;
use s3dedup::routes::ft::version::ft_version;
use s3dedup::config;
use s3dedup::AppState;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};

async fn run_server(addr: SocketAddr, app: Router) {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tokio::main]
async fn main() {
    let config = config::Config::new("config.json").unwrap();
    s3dedup::logging::setup(&config.logging).unwrap();
    let mut handles = vec![];

    for bucket in config.buckets.iter() {
        info!("Starting server for bucket: {}", bucket.name);

        let app_state = AppState::new(bucket).await.unwrap();
        app_state.kvstorage.lock().await.setup().await.unwrap();

        let app = Router::new()
            .route("/ft/version", get(ft_version))
            .route("/ft/files/{*path}", get(ft_get_file).head(ft_get_file).put(ft_put_file))
            .layer( // Logging middleware
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .with_state(Arc::new(app_state));
        let address: SocketAddr = format!("{}:{}", bucket.address, bucket.port).parse().unwrap();
        let handle = tokio::spawn(run_server(address, app));
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
