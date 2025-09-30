use crate::kvstorage::KVStorage;
use crate::locks::LocksStorage;
use crate::s3storage::S3Storage;
use axum::Router;
use axum::routing::{get, put};
use routes::ft::put_file::ft_put_file;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};
use crate::routes::ft::version::ft_version;

mod config;
mod kvstorage;
mod locks;
mod routes;
mod logging;
mod s3storage;

#[derive(Clone)]
pub struct AppState {
    pub bucket_name: String,
    pub kvstorage: Arc<Mutex<Box<KVStorage>>>,
    pub locks: Arc<Mutex<Box<LocksStorage>>>,
    pub s3storage: Arc<Mutex<Box<S3Storage>>>,
}

impl AppState {
    async fn new(config: &config::BucketConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let kvstorage = KVStorage::new(&config).await?;
        let locks = LocksStorage::new(&config.locks_type);
        let s3storage = S3Storage::new(&config).await?;
        Ok(Self {
            bucket_name: config.name.clone(),
            kvstorage: Arc::new(Mutex::new(kvstorage)),
            locks: Arc::new(Mutex::new(locks)),
            s3storage: Arc::new(Mutex::new(s3storage)),
        })
    }
}

async fn run_server(addr: SocketAddr, app: Router) {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tokio::main]
async fn main() {
    let config = config::Config::new("config.json").unwrap();
    logging::setup(&config.logging).unwrap();
    let mut handles = vec![];

    for bucket in config.buckets.iter() {
        info!("Starting server for bucket: {}", bucket.name);

        let app_state = AppState::new(bucket).await.unwrap();
        app_state.kvstorage.lock().await.setup().await.unwrap();

        let app = Router::new()
            .route("/ft/version", get(ft_version))
            .route("/ft/files/{*path}", put(ft_put_file))
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
