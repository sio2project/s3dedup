use std::error::Error;
use axum::Router;
use axum::routing::put;
use crate::kvstorage::KVStorage;
use crate::locks::Locks;
use crate::routes::put_file::put_file;

mod kvstorage;
mod config;
mod locks;
mod routes;

#[derive(Clone)]
struct AppState {
    // TODO: Im not sure if kvstoragae should be in the state. Probably each request should create a new connection?
    kvstorage: Box<dyn kvstorage::KVStorage>,
    locks: Box<dyn locks::Locks>,
    config: config::Config,
}

impl AppState {
    fn new(config: config::Config) -> Result<Self, Box<dyn Error>> {
        let kvstorage: Box<dyn kvstorage::KVStorage> = match config.kvstorage_type {
            kvstorage::KVStorageType::Postgres => {
                kvstorage::postgres::Postgres::new(&config)?
            }
            kvstorage::KVStorageType::SQLite => {
                kvstorage::sqlite::SQLite::new(&config)?
            }
        };
        let locks: Box<dyn locks::Locks> = match config.locks_type {
            locks::LocksType::Memory => {
                locks::memory::MemoryLocks::new()
            }
        };
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
    let mut app_state = AppState::new(config).unwrap();
    app_state.kvstorage.setup().unwrap();

    let app = Router::new()
        .route("/files/:path", put(put_file))
        .with_state(app_state);

}
