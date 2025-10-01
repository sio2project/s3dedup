use axum::Router;
use axum::routing::get;
use clap::{Parser, Subcommand};
use s3dedup::cleaner::Cleaner;
use s3dedup::AppState;
use s3dedup::config;
use s3dedup::routes::ft::delete_file::ft_delete_file;
use s3dedup::routes::ft::get_file::ft_get_file;
use s3dedup::routes::ft::list_files::ft_list_files;
use s3dedup::routes::ft::put_file::ft_put_file;
use s3dedup::routes::ft::version::ft_version;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, info};

#[derive(Parser)]
#[command(name = "s3dedup")]
#[command(about = "S3 deduplication proxy server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the S3 deduplication server
    Server {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.json")]
        config: String,
    },
    /// Migrate data from old filetracker to s3dedup
    Migrate {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.json")]
        config: String,
    },
    /// Perform live migration while server is running
    LiveMigrate {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.json")]
        config: String,
    },
}

async fn run_server(addr: SocketAddr, app: Router) {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn run_s3dedup_server(config_path: &str) {
    let config = config::Config::new(config_path).unwrap();
    s3dedup::logging::setup(&config.logging).unwrap();
    let mut handles = vec![];

    for bucket in config.buckets.iter() {
        info!("Starting server for bucket: {}", bucket.name);

        let app_state = AppState::new(bucket).await.unwrap();
        app_state.kvstorage.lock().await.setup().await.unwrap();

        // Start cleaner for this bucket
        let cleaner = Arc::new(Cleaner::new(
            bucket.name.clone(),
            app_state.kvstorage.clone(),
            app_state.s3storage.clone(),
            bucket.cleaner.clone(),
        ));
        cleaner.start();

        let app = Router::new()
            .route("/ft/version", get(ft_version))
            .route("/ft/list/", get(ft_list_files))
            .route("/ft/list/{*path}", get(ft_list_files))
            .route(
                "/ft/files/{*path}",
                get(ft_get_file)
                    .head(ft_get_file)
                    .put(ft_put_file)
                    .delete(ft_delete_file),
            )
            .layer(
                // Logging middleware
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .with_state(Arc::new(app_state));
        let address: SocketAddr = format!("{}:{}", bucket.address, bucket.port)
            .parse()
            .unwrap();
        let handle = tokio::spawn(run_server(address, app));
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_migrate(config_path: &str) {
    let config = config::Config::new(config_path).unwrap();
    s3dedup::logging::setup(&config.logging).unwrap();

    info!("Starting migration from old filetracker to s3dedup");
    info!("Using config file: {}", config_path);

    // TODO: Implement migration logic
    println!("Migration not yet implemented");
}

async fn run_live_migrate(config_path: &str) {
    let config = config::Config::new(config_path).unwrap();
    s3dedup::logging::setup(&config.logging).unwrap();

    info!("Starting live migration from old filetracker to s3dedup");
    info!("Using config file: {}", config_path);

    // TODO: Implement live migration logic
    println!("Live migration not yet implemented");
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config } => {
            run_s3dedup_server(&config).await;
        }
        Commands::Migrate { config } => {
            run_migrate(&config).await;
        }
        Commands::LiveMigrate { config } => {
            run_live_migrate(&config).await;
        }
    }
}
