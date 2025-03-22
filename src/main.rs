use crate::kvstorage::{KVStorage, KVStorageTrait};
use crate::locks::{Locks, LocksStorage};
use axum::Router;
use axum::routing::put;
use routes::ft::put_file::ft_put_file;
use std::error::Error;
use std::sync::Arc;

mod config;
mod kvstorage;
mod locks;
mod routes;

#[derive(Clone)]
struct AppState {
    kvstorage: Box<KVStorage>,
    locks: Box<LocksStorage>,
    config: config::Config,
}

impl AppState {
    async fn new(config: config::Config) -> Result<Self, Box<dyn Error>> {
        let kvstorage = KVStorage::new(&config).await?;
        let locks = LocksStorage::new(&config.locks_type);
        Ok(Self {
            kvstorage,
            locks,
            config,
        })
    }
}

#[tokio::main]
async fn main() {
    let config = config::Config::new("config.json").unwrap();
    let mut app_state = AppState::new(config).await.unwrap();
    app_state.kvstorage.setup().await.unwrap();

    let app = Router::new()
        .route("/ft/files/{path}", put(ft_put_file))
        .with_state(Arc::new(app_state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
