use anyhow::Context;
use axum::Router;
use axum::routing::get;
use clap::{Parser, Subcommand};
use s3dedup::AppState;
use s3dedup::cleaner::Cleaner;
use s3dedup::config;
use s3dedup::metrics;
use s3dedup::routes::ft::delete_file::ft_delete_file;
use s3dedup::routes::ft::get_file::ft_get_file;
use s3dedup::routes::ft::head_file::ft_head_file;
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
        /// Run in proxy-only mode: forward GETs to filetracker, dual-write PUTs,
        /// but do NOT start background migration. Use this as the first phase of
        /// migrating huge filetracker instances.
        #[arg(long, conflicts_with = "file_list")]
        proxy_only: bool,
        /// Path to a file containing one file path per line to migrate, instead of
        /// calling /list/. Uses infinite retry with backoff for ft downtime resilience.
        /// Generate with: find /var/lib/filetracker/links -type l | sed 's|^/var/lib/filetracker/links||'
        #[arg(long, conflicts_with = "proxy_only", value_name = "PATH")]
        file_list: Option<String>,
    },
    /// Migrate data from V1 filetracker filesystem to s3dedup
    MigrateV1 {
        /// Path to configuration file (optional if using environment variables)
        #[arg(short, long)]
        config: Option<String>,
        /// Use environment variables for configuration instead of config file
        #[arg(short, long)]
        env: bool,
        /// Path to V1 filetracker directory ($FILETRACKER_DIR)
        #[arg(short = 'd', long)]
        v1_directory: String,
        /// Maximum number of concurrent migration workers
        #[arg(short, long, default_value = "10")]
        max_concurrency: usize,
    },
    /// Perform live migration from V1 filetracker while server is running
    LiveMigrateV1 {
        /// Path to configuration file (optional if using environment variables)
        #[arg(short, long)]
        config: Option<String>,
        /// Use environment variables for configuration instead of config file
        #[arg(short, long)]
        env: bool,
        /// Path to V1 filetracker directory for background migration
        #[arg(short = 'd', long)]
        v1_directory: Option<String>,
        /// URL of the V1 filetracker HTTP server for fallback (optional, can be set via config/env)
        #[arg(short = 'u', long)]
        filetracker_url: Option<String>,
        /// Maximum number of concurrent migration workers per bucket
        #[arg(short, long, default_value = "10")]
        max_concurrency: usize,
    },
}

async fn run_server(addr: SocketAddr, app: Router) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("Failed to bind TCP listener")?;
    axum::serve(listener, app).await.context("Server error")?;
    Ok(())
}

/// Create the router with all Filetracker routes
fn create_router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ft/version", get(ft_version))
        .route("/ft/version/", get(ft_version))
        .route("/ft/list/", get(ft_list_files))
        .route("/ft/list/{*path}", get(ft_list_files))
        .route(
            "/ft/files/{*path}",
            get(ft_get_file)
                .head(ft_head_file)
                .put(ft_put_file)
                .delete(ft_delete_file),
        )
        .route("/metrics", get(s3dedup::routes::metrics::metrics_handler))
        .route(
            "/metrics/json",
            get(s3dedup::routes::metrics::metrics_json_handler),
        )
        .route("/health", get(s3dedup::routes::metrics::health_handler))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(app_state)
}

/// Start background tasks (cleaner and metrics updater)
fn start_background_tasks(app_state: Arc<AppState>, bucket_config: &config::BucketConfig) {
    // Start cleaner
    let cleaner = Arc::new(Cleaner::new(
        bucket_config.name.clone(),
        app_state.kvstorage.clone(),
        app_state.s3storage.clone(),
        app_state.locks.clone(),
        bucket_config.cleaner.clone(),
    ));
    cleaner.start();

    // Start metrics updater task
    let metrics_state = app_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = metrics_state.update_storage_metrics().await {
                warn!("Failed to update storage metrics: {}", e);
            }
        }
    });
}

async fn run_s3dedup_server(config_path: Option<&str>, use_env: bool) -> anyhow::Result<()> {
    let config = if use_env {
        config::Config::from_env().context("Failed to load configuration from environment")?
    } else {
        config::Config::new(config_path.unwrap_or("config.json"))
            .context("Failed to load configuration from file")?
    };
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting server for bucket: {}", config.bucket.name);

    let app_state = AppState::new(&config)
        .await
        .context("Failed to initialize app state")?;
    app_state
        .kvstorage
        .setup()
        .await
        .context("Failed to setup KV storage")?;

    // Store instance version
    app_state
        .kvstorage
        .set_version(env!("CARGO_PKG_VERSION"))
        .await
        .context("Failed to store instance version")?;
    metrics::INSTANCE_VERSION
        .with_label_values(&[env!("CARGO_PKG_VERSION")])
        .set(1);

    start_background_tasks(app_state.clone(), &config.bucket);

    let app = create_router(app_state);
    let address: SocketAddr = format!("{}:{}", config.bucket.address, config.bucket.port)
        .parse()
        .context("Failed to parse socket address")?;

    run_server(address, app).await
}

async fn run_migrate(
    config_path: Option<&str>,
    use_env: bool,
    filetracker_url: &str,
    max_concurrency: usize,
) -> anyhow::Result<()> {
    let config = if use_env {
        config::Config::from_env().context("Failed to load configuration from environment")?
    } else {
        config::Config::new(config_path.unwrap_or("config.json"))
            .context("Failed to load configuration from file")?
    };
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting offline migration from old filetracker to s3dedup");
    if use_env {
        info!("Using environment variables for configuration");
    } else {
        info!("Config file: {}", config_path.unwrap_or("config.json"));
    }
    info!("Filetracker URL: {}", filetracker_url);
    info!("Bucket: {}", config.bucket.name);
    info!("Max concurrency: {}", max_concurrency);

    // Initialize AppState
    let app_state = AppState::new(&config)
        .await
        .context("Failed to initialize app state")?;

    // Setup KV storage
    app_state
        .kvstorage
        .setup()
        .await
        .context("Failed to setup KV storage")?;

    // Initialize filetracker client
    let filetracker_client = Arc::new(s3dedup::filetracker_client::FiletrackerClient::new(
        filetracker_url.to_string(),
    ));

    // Run migration with specified concurrency
    let stats =
        s3dedup::migration::migrate_all_files(filetracker_client, app_state, max_concurrency)
            .await
            .context("Migration failed")?;

    info!("Migration completed successfully");
    info!("Total files: {}", stats.total_files);
    info!("Migrated: {}", stats.migrated);
    info!("Skipped: {}", stats.skipped);
    info!("Failed: {}", stats.failed);

    if stats.failed > 0 {
        warn!("{} files failed to migrate", stats.failed);
        std::process::exit(1);
    }

    Ok(())
}

async fn run_migrate_v1(
    config_path: Option<&str>,
    use_env: bool,
    v1_directory: &str,
    max_concurrency: usize,
) -> anyhow::Result<()> {
    let config = if use_env {
        config::Config::from_env().context("Failed to load configuration from environment")?
    } else {
        config::Config::new(config_path.unwrap_or("config.json"))
            .context("Failed to load configuration from file")?
    };
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting offline V1 filesystem migration to s3dedup");
    if use_env {
        info!("Using environment variables for configuration");
    } else {
        info!("Config file: {}", config_path.unwrap_or("config.json"));
    }
    info!("V1 directory: {}", v1_directory);
    info!("Bucket: {}", config.bucket.name);
    info!("Max concurrency: {}", max_concurrency);

    // Initialize AppState
    let app_state = AppState::new(&config)
        .await
        .context("Failed to initialize app state")?;

    // Setup KV storage
    app_state
        .kvstorage
        .setup()
        .await
        .context("Failed to setup KV storage")?;

    // Run V1 filesystem migration
    let stats =
        s3dedup::migration::migrate_all_files_from_v1_fs(v1_directory, app_state, max_concurrency)
            .await
            .context("V1 migration failed")?;

    info!("V1 migration completed successfully");
    info!("Total files: {}", stats.total_files);
    info!("Migrated: {}", stats.migrated);
    info!("Skipped: {}", stats.skipped);
    info!("Failed: {}", stats.failed);

    if stats.failed > 0 {
        warn!("{} files failed to migrate", stats.failed);
        std::process::exit(1);
    }

    Ok(())
}

async fn run_live_migrate(
    config_path: Option<&str>,
    use_env: bool,
    max_concurrency: usize,
    proxy_only: bool,
    file_list: Option<&str>,
) -> anyhow::Result<()> {
    let config = if use_env {
        config::Config::from_env().context("Failed to load configuration from environment")?
    } else {
        config::Config::new(config_path.unwrap_or("config.json"))
            .context("Failed to load configuration from file")?
    };
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting live migration from old filetracker to s3dedup");
    if use_env {
        info!("Using environment variables for configuration");
    } else {
        info!("Config file: {}", config_path.unwrap_or("config.json"));
    }
    info!("Bucket: {}", config.bucket.name);
    info!("Max concurrency: {}", max_concurrency);

    // Validate file-list if provided (open to check both existence and read permissions)
    if let Some(path) = file_list {
        std::fs::File::open(path)
            .with_context(|| format!("File list not found or not readable: {}", path))?;
        info!("Using file list for migration: {}", path);
    }

    // Check if this bucket has a filetracker URL configured
    let filetracker_url = &config.bucket.filetracker_url;

    if (proxy_only || file_list.is_some()) && filetracker_url.is_none() {
        anyhow::bail!(
            "Cannot use --proxy-only or --file-list without filetracker_url configured for bucket '{}'",
            config.bucket.name
        );
    }

    if filetracker_url.is_none() {
        info!(
            "Bucket '{}' has no filetracker_url configured, starting in normal server mode (no migration)",
            config.bucket.name
        );
    } else {
        info!(
            "Starting server with live migration for bucket: {} (filetracker: {})",
            config.bucket.name,
            filetracker_url.as_ref().unwrap()
        );
    }

    // Initialize AppState
    let app_state = if let Some(url) = filetracker_url {
        AppState::new_with_filetracker(&config, url.clone())
            .await
            .context("Failed to initialize app state with filetracker")?
    } else {
        AppState::new(&config)
            .await
            .context("Failed to initialize app state")?
    };

    app_state
        .kvstorage
        .setup()
        .await
        .context("Failed to setup KV storage")?;

    start_background_tasks(app_state.clone(), &config.bucket);

    // Start background migration worker based on mode
    if filetracker_url.is_some() {
        if proxy_only {
            info!(
                "Proxy-only mode: server running with filetracker fallback, no background migration worker. \
                 migration_active metric will remain 1 until server is restarted with --file-list or in normal mode."
            );
        } else if let Some(path) = file_list {
            let migration_app_state = app_state.clone();
            let migration_client = app_state
                .filetracker_client
                .clone()
                .context("Filetracker client not available")?;
            let file_list_path = path.to_string();
            tokio::spawn(async move {
                s3dedup::migration::live_migration_worker_from_file_list(
                    file_list_path,
                    migration_client,
                    migration_app_state,
                    max_concurrency,
                )
                .await;
            });
        } else {
            let migration_app_state = app_state.clone();
            let migration_client = app_state
                .filetracker_client
                .clone()
                .context("Filetracker client not available")?;
            tokio::spawn(async move {
                s3dedup::migration::live_migration_worker(
                    migration_client,
                    migration_app_state,
                    max_concurrency,
                )
                .await;
            });
        }
    }

    let app = create_router(app_state);
    let address: SocketAddr = format!("{}:{}", config.bucket.address, config.bucket.port)
        .parse()
        .context("Failed to parse socket address")?;

    run_server(address, app).await
}

async fn run_live_migrate_v1(
    config_path: Option<&str>,
    use_env: bool,
    v1_directory: Option<&str>,
    filetracker_url: Option<&str>,
    max_concurrency: usize,
) -> anyhow::Result<()> {
    let config = if use_env {
        config::Config::from_env().context("Failed to load configuration from environment")?
    } else {
        config::Config::new(config_path.unwrap_or("config.json"))
            .context("Failed to load configuration from file")?
    };
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting live migration from V1 filetracker to s3dedup");
    if use_env {
        info!("Using environment variables for configuration");
    } else {
        info!("Config file: {}", config_path.unwrap_or("config.json"));
    }
    info!("Bucket: {}", config.bucket.name);
    info!("Max concurrency: {}", max_concurrency);

    // Determine V1 directory: CLI > config > env
    let effective_v1_dir = v1_directory
        .or(config.bucket.filetracker_v1_dir.as_deref())
        .map(|s| s.to_string());

    // Determine filetracker URL: CLI > config > env
    let effective_ft_url = filetracker_url
        .or(config.bucket.filetracker_url.as_deref())
        .map(|s| s.to_string());

    info!(
        "V1 migration configuration: v1_dir: {:?}, filetracker_url: {:?}",
        effective_v1_dir, effective_ft_url
    );

    // Initialize AppState with filetracker client if URL is provided
    let app_state = if let Some(ref ft_url) = effective_ft_url {
        info!("Creating app state with V1 filetracker client for HTTP fallback");
        AppState::new_with_filetracker(&config, ft_url.clone())
            .await
            .context("Failed to initialize app state with filetracker")?
    } else {
        AppState::new(&config)
            .await
            .context("Failed to initialize app state")?
    };
    app_state
        .kvstorage
        .setup()
        .await
        .context("Failed to setup KV storage")?;

    start_background_tasks(app_state.clone(), &config.bucket);

    // Start background V1 filesystem migration worker if v1_directory is provided
    if let Some(v1_dir) = effective_v1_dir {
        // Set migration_active gauge to indicate migration is in progress
        s3dedup::metrics::MIGRATION_ACTIVE.set(1);

        let migration_app_state = app_state.clone();
        tokio::spawn(async move {
            match s3dedup::migration::migrate_all_files_from_v1_fs(
                &v1_dir,
                migration_app_state,
                max_concurrency,
            )
            .await
            {
                Ok(stats) => {
                    info!("Background V1 filesystem migration completed successfully");
                    info!("Total files: {}", stats.total_files);
                    info!("Migrated: {}", stats.migrated);
                    info!("Skipped: {}", stats.skipped);
                    info!("Failed: {}", stats.failed);

                    if stats.failed > 0 {
                        warn!("{} files failed to migrate", stats.failed);
                    }
                }
                Err(e) => {
                    error!("Background V1 filesystem migration failed: {}", e);
                }
            }

            // Reset migration_active gauge
            s3dedup::metrics::MIGRATION_ACTIVE.set(0);
            info!("Background V1 filesystem migration worker finished");
        });
    }

    let app = create_router(app_state);
    let address: SocketAddr = format!("{}:{}", config.bucket.address, config.bucket.port)
        .parse()
        .context("Failed to parse socket address")?;

    run_server(address, app).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config, env } => {
            run_s3dedup_server(config.as_deref(), env).await?;
        }
        Commands::Migrate {
            config,
            env,
            filetracker_url,
            max_concurrency,
        } => {
            run_migrate(config.as_deref(), env, &filetracker_url, max_concurrency).await?;
        }
        Commands::LiveMigrate {
            config,
            env,
            max_concurrency,
            proxy_only,
            file_list,
        } => {
            run_live_migrate(
                config.as_deref(),
                env,
                max_concurrency,
                proxy_only,
                file_list.as_deref(),
            )
            .await?;
        }
        Commands::MigrateV1 {
            config,
            env,
            v1_directory,
            max_concurrency,
        } => {
            run_migrate_v1(config.as_deref(), env, &v1_directory, max_concurrency).await?;
        }
        Commands::LiveMigrateV1 {
            config,
            env,
            v1_directory,
            filetracker_url,
            max_concurrency,
        } => {
            run_live_migrate_v1(
                config.as_deref(),
                env,
                v1_directory.as_deref(),
                filetracker_url.as_deref(),
                max_concurrency,
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_proxy_only_and_file_list_mutually_exclusive() {
        let result = Cli::try_parse_from([
            "s3dedup",
            "live-migrate",
            "--proxy-only",
            "--file-list",
            "/tmp/files.txt",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_proxy_only_flag_accepted() {
        let cli = Cli::try_parse_from(["s3dedup", "live-migrate", "--proxy-only"]);
        assert!(cli.is_ok());
        match cli.unwrap().command {
            Commands::LiveMigrate {
                proxy_only,
                file_list,
                ..
            } => {
                assert!(proxy_only);
                assert!(file_list.is_none());
            }
            _ => panic!("Expected LiveMigrate command"),
        }
    }

    #[test]
    fn test_file_list_flag_accepted() {
        let cli = Cli::try_parse_from(["s3dedup", "live-migrate", "--file-list", "/tmp/files.txt"]);
        assert!(cli.is_ok());
        match cli.unwrap().command {
            Commands::LiveMigrate {
                proxy_only,
                file_list,
                ..
            } => {
                assert!(!proxy_only);
                assert_eq!(file_list.as_deref(), Some("/tmp/files.txt"));
            }
            _ => panic!("Expected LiveMigrate command"),
        }
    }
}
