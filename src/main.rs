use axum::Router;
use axum::routing::get;
use clap::{Parser, Subcommand};
use s3dedup::AppState;
use s3dedup::cleaner::Cleaner;
use s3dedup::config;
use s3dedup::routes::ft::delete_file::ft_delete_file;
use s3dedup::routes::ft::get_file::ft_get_file;
use s3dedup::routes::ft::list_files::ft_list_files;
use s3dedup::routes::ft::put_file::ft_put_file;
use s3dedup::routes::ft::version::ft_version;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, error, info, warn};

#[derive(Parser)]
#[command(name = "s3dedup")]
#[command(about = "S3 deduplication proxy server", long_about = None)]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the S3 deduplication server
    Server {
        /// Path to configuration file (optional if using environment variables)
        #[arg(short, long)]
        config: Option<String>,
        /// Use environment variables for configuration instead of config file
        #[arg(short, long)]
        env: bool,
    },
    /// Migrate data from old filetracker to s3dedup
    Migrate {
        /// Path to configuration file (optional if using environment variables)
        #[arg(short, long)]
        config: Option<String>,
        /// Use environment variables for configuration instead of config file
        #[arg(short, long)]
        env: bool,
        /// URL of the old filetracker server
        #[arg(short, long)]
        filetracker_url: String,
        /// Maximum number of concurrent migration workers
        #[arg(short, long, default_value = "10")]
        max_concurrency: usize,
    },
    /// Perform live migration while server is running
    /// Each bucket can specify its own filetracker_url in the config file
    LiveMigrate {
        /// Path to configuration file (optional if using environment variables)
        #[arg(short, long)]
        config: Option<String>,
        /// Use environment variables for configuration instead of config file
        #[arg(short, long)]
        env: bool,
        /// Maximum number of concurrent migration workers per bucket
        #[arg(short, long, default_value = "10")]
        max_concurrency: usize,
    },
}

async fn run_server(addr: SocketAddr, app: Router) {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn run_s3dedup_server(config_path: Option<&str>, use_env: bool) {
    let config = if use_env {
        config::Config::from_env().unwrap()
    } else {
        config::Config::new(config_path.unwrap_or("config.json")).unwrap()
    };
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
            .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
            .route(
                "/metrics/json",
                get(s3dedup::routes::metrics::metrics_json_handler),
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

async fn run_migrate(
    config_path: Option<&str>,
    use_env: bool,
    filetracker_url: &str,
    max_concurrency: usize,
) {
    let config = if use_env {
        config::Config::from_env().unwrap()
    } else {
        config::Config::new(config_path.unwrap_or("config.json")).unwrap()
    };
    s3dedup::logging::setup(&config.logging).unwrap();

    info!("Starting offline migration from old filetracker to s3dedup");
    if use_env {
        info!("Using environment variables for configuration");
    } else {
        info!("Config file: {}", config_path.unwrap_or("config.json"));
    }
    info!("Filetracker URL: {}", filetracker_url);
    info!("Max concurrency: {}", max_concurrency);

    // For offline migration, we only migrate the first bucket
    if config.buckets.is_empty() {
        error!("No buckets configured");
        return;
    }

    let bucket_config = &config.buckets[0];
    info!("Migrating to bucket: {}", bucket_config.name);

    // Initialize AppState
    let app_state = match AppState::new(bucket_config).await {
        Ok(state) => Arc::new(state),
        Err(e) => {
            error!("Failed to initialize app state: {}", e);
            return;
        }
    };

    // Setup KV storage
    if let Err(e) = app_state.kvstorage.lock().await.setup().await {
        error!("Failed to setup KV storage: {}", e);
        return;
    }

    // Initialize filetracker client
    let filetracker_client = Arc::new(s3dedup::filetracker_client::FiletrackerClient::new(
        filetracker_url.to_string(),
    ));

    // Run migration with specified concurrency
    match s3dedup::migration::migrate_all_files(filetracker_client, app_state, max_concurrency)
        .await
    {
        Ok(stats) => {
            info!("Migration completed successfully");
            info!("Total files: {}", stats.total_files);
            info!("Migrated: {}", stats.migrated);
            info!("Skipped: {}", stats.skipped);
            info!("Failed: {}", stats.failed);

            if stats.failed > 0 {
                warn!("{} files failed to migrate", stats.failed);
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("Migration failed: {}", e);
            std::process::exit(1);
        }
    }
}

async fn run_live_migrate(config_path: Option<&str>, use_env: bool, max_concurrency: usize) {
    let config = if use_env {
        config::Config::from_env().unwrap()
    } else {
        config::Config::new(config_path.unwrap_or("config.json")).unwrap()
    };
    s3dedup::logging::setup(&config.logging).unwrap();
    let mut handles = vec![];

    info!("Starting live migration from old filetracker to s3dedup");
    if use_env {
        info!("Using environment variables for configuration");
    } else {
        info!("Config file: {}", config_path.unwrap_or("config.json"));
    }
    info!("Max concurrency per bucket: {}", max_concurrency);

    for bucket in config.buckets.iter() {
        // Check if this bucket has a filetracker URL configured
        let filetracker_url = match &bucket.filetracker_url {
            Some(url) => url.clone(),
            None => {
                info!(
                    "Bucket '{}' has no filetracker_url configured, starting in normal mode",
                    bucket.name
                );
                // Start server without migration for this bucket
                let app_state = AppState::new(bucket).await.unwrap();
                app_state.kvstorage.lock().await.setup().await.unwrap();

                let cleaner = Arc::new(s3dedup::cleaner::Cleaner::new(
                    bucket.name.clone(),
                    app_state.kvstorage.clone(),
                    app_state.s3storage.clone(),
                    bucket.cleaner.clone(),
                ));
                cleaner.start();

                let app = Router::new()
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
                            .head(s3dedup::routes::ft::get_file::ft_get_file)
                            .put(s3dedup::routes::ft::put_file::ft_put_file)
                            .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
                    )
                    .layer(
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
                continue;
            }
        };

        info!(
            "Starting server with live migration for bucket: {} (filetracker: {})",
            bucket.name, filetracker_url
        );

        let app_state = AppState::new_with_filetracker(bucket, filetracker_url)
            .await
            .unwrap();
        app_state.kvstorage.lock().await.setup().await.unwrap();

        // Start cleaner for this bucket
        let cleaner = Arc::new(s3dedup::cleaner::Cleaner::new(
            bucket.name.clone(),
            app_state.kvstorage.clone(),
            app_state.s3storage.clone(),
            bucket.cleaner.clone(),
        ));
        cleaner.start();

        // Start background migration worker
        let migration_app_state = app_state.clone();
        let migration_client = app_state.filetracker_client.clone().unwrap();
        tokio::spawn(async move {
            s3dedup::migration::live_migration_worker(
                migration_client,
                Arc::new(migration_app_state),
                max_concurrency,
            )
            .await;
        });

        let app = Router::new()
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
                    .head(s3dedup::routes::ft::get_file::ft_get_file)
                    .put(s3dedup::routes::ft::put_file::ft_put_file)
                    .delete(s3dedup::routes::ft::delete_file::ft_delete_file),
            )
            .layer(
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config, env } => {
            run_s3dedup_server(config.as_deref(), env).await;
        }
        Commands::Migrate {
            config,
            env,
            filetracker_url,
            max_concurrency,
        } => {
            run_migrate(config.as_deref(), env, &filetracker_url, max_concurrency).await;
        }
        Commands::LiveMigrate {
            config,
            env,
            max_concurrency,
        } => {
            run_live_migrate(config.as_deref(), env, max_concurrency).await;
        }
    }
}
