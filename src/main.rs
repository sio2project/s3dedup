use anyhow::Context;
use axum::Router;
use axum::routing::get;
use clap::{Parser, Subcommand};
use s3dedup::AppState;
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
    /// Browse, download, upload and delete files on a running s3dedup server
    Files {
        /// Server URL, e.g. http://s3dedup-sio2:8080 (the /ft suffix is optional)
        #[arg(short, long, env = "S3DEDUP_URL")]
        url: String,
        #[command(subcommand)]
        command: FilesCommand,
    },
}

#[derive(Subcommand)]
enum FilesCommand {
    /// List files under a path (recursively), one full path per line
    Ls {
        /// Path prefix to list (default: everything)
        #[arg(default_value = "")]
        path: String,
        /// Also show logical size and version of each file
        #[arg(short, long)]
        long: bool,
    },
    /// Show metadata of a file
    Stat {
        /// Remote path
        path: String,
    },
    /// Download a file (decompressed)
    Get {
        /// Remote path
        path: String,
        /// Local destination; "-" or omitted writes to stdout
        output: Option<String>,
    },
    /// Upload a file
    Put {
        /// Local file; "-" reads from stdin
        input: String,
        /// Remote path
        path: String,
        /// File version as Unix timestamp or RFC 2822 date (default: now).
        /// The server keeps the newer version if it already has one.
        #[arg(long, value_name = "VERSION")]
        last_modified: Option<String>,
    },
    /// Delete files
    Rm {
        /// Remote paths
        #[arg(required = true)]
        paths: Vec<String>,
        /// Delete only if the server version is not newer than this
        /// (Unix timestamp or RFC 2822 date, default: now)
        #[arg(long, value_name = "VERSION")]
        last_modified: Option<String>,
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
fn start_background_tasks(app_state: Arc<AppState>, _bucket_config: &config::BucketConfig) {
    // Lightweight cleaner (phases 1, 2, 4) runs on interval_seconds.
    // Full S3 scan (phase 3) runs on full_scan_cron schedule if configured.
    app_state.cleaner.clone().start();

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

fn load_config(config_path: Option<&str>, use_env: bool) -> anyhow::Result<config::Config> {
    if use_env {
        config::Config::from_env().context("Failed to load configuration from environment")
    } else {
        config::Config::new(config_path.unwrap_or("config.json"))
            .context("Failed to load configuration from file")
    }
}

async fn init_app_state(config: &config::Config) -> anyhow::Result<Arc<AppState>> {
    let app_state = AppState::new(config)
        .await
        .context("Failed to initialize app state")?;
    app_state
        .kvstorage
        .setup()
        .await
        .context("Failed to setup KV storage")?;
    Ok(app_state)
}

async fn run_s3dedup_server(config_path: Option<&str>, use_env: bool) -> anyhow::Result<()> {
    let config = load_config(config_path, use_env)?;
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting server for bucket: {}", config.bucket.name);

    let app_state = init_app_state(&config).await?;

    // Store instance version
    app_state
        .kvstorage
        .set_version(env!("CARGO_PKG_VERSION"))
        .await
        .context("Failed to store instance version")?;
    metrics::INSTANCE_VERSION
        .with_label_values(&[env!("CARGO_PKG_VERSION")])
        .set(1);

    info!("Computing storage stats...");
    if let Err(e) = app_state
        .kvstorage
        .recompute_stats(&app_state.bucket_name)
        .await
    {
        warn!("Failed to compute storage stats on startup: {}", e);
    }

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
    let config = load_config(config_path, use_env)?;
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting offline migration from old filetracker to s3dedup");
    info!("Filetracker URL: {}", filetracker_url);
    info!("Bucket: {}", config.bucket.name);
    info!("Max concurrency: {}", max_concurrency);

    let app_state = init_app_state(&config).await?;

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
    let config = load_config(config_path, use_env)?;
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting offline V1 filesystem migration to s3dedup");
    info!("V1 directory: {}", v1_directory);
    info!("Bucket: {}", config.bucket.name);
    info!("Max concurrency: {}", max_concurrency);

    let app_state = init_app_state(&config).await?;

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
    let config = load_config(config_path, use_env)?;
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting live migration from old filetracker to s3dedup");
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

    // Initialize AppState (with filetracker client if configured)
    let app_state = if let Some(url) = filetracker_url {
        let app_state = AppState::new_with_filetracker(&config, url.clone())
            .await
            .context("Failed to initialize app state with filetracker")?;
        app_state
            .kvstorage
            .setup()
            .await
            .context("Failed to setup KV storage")?;
        app_state
    } else {
        init_app_state(&config).await?
    };

    info!("Computing storage stats...");
    if let Err(e) = app_state
        .kvstorage
        .recompute_stats(&app_state.bucket_name)
        .await
    {
        warn!("Failed to compute storage stats on startup: {}", e);
    }

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
    let config = load_config(config_path, use_env)?;
    let _log_guard = s3dedup::logging::setup(&config.logging).context("Failed to setup logging")?;

    info!("Starting live migration from V1 filetracker to s3dedup");
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
        let app_state = AppState::new_with_filetracker(&config, ft_url.clone())
            .await
            .context("Failed to initialize app state with filetracker")?;
        app_state
            .kvstorage
            .setup()
            .await
            .context("Failed to setup KV storage")?;
        app_state
    } else {
        init_app_state(&config).await?
    };

    info!("Computing storage stats...");
    if let Err(e) = app_state
        .kvstorage
        .recompute_stats(&app_state.bucket_name)
        .await
    {
        warn!("Failed to compute storage stats on startup: {}", e);
    }

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

fn format_version(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn version_or_now(version: Option<&str>) -> anyhow::Result<i64> {
    match version {
        Some(v) => s3dedup::files_cli::parse_version(v),
        None => Ok(chrono::Utc::now().timestamp()),
    }
}

/// Resolve the local destination of `files get`: like `cp`, a directory
/// (existing, or given with a trailing slash) gets the remote file name.
fn get_destination(remote: &str, output: &str) -> std::path::PathBuf {
    let output_path = std::path::Path::new(output);
    if output.ends_with('/') || output_path.is_dir() {
        let name = remote
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(remote);
        output_path.join(name)
    } else {
        output_path.to_path_buf()
    }
}

/// Download `remote` to `output` through a temp file in the same directory,
/// so a failed download never leaves a truncated file behind.
async fn download_to_file(
    client: &s3dedup::files_cli::FilesClient,
    remote: &str,
    output: &std::path::Path,
) -> anyhow::Result<()> {
    let dir = match output.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => std::path::Path::new("."),
    };
    let mut temp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("Cannot write to {}", dir.display()))?;
    client
        .download(remote, std::io::BufWriter::new(temp.as_file_mut()))
        .await?
        .with_context(|| format!("{}: not found", remote))?;
    // Temp files are created with mode 0600; keep the mode of an existing
    // destination, otherwise use the usual 0644.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::metadata(output)
            .map(|m| m.permissions())
            .unwrap_or_else(|_| std::fs::Permissions::from_mode(0o644));
        temp.as_file().set_permissions(permissions)?;
    }
    temp.persist(output)
        .map_err(|e| e.error)
        .with_context(|| format!("Failed to write {}", output.display()))?;
    Ok(())
}

async fn run_files(url: &str, command: FilesCommand) -> anyhow::Result<()> {
    use s3dedup::files_cli::DeleteOutcome;
    use std::io::Write;

    let client = s3dedup::files_cli::FilesClient::new(url)?;
    match command {
        FilesCommand::Ls { path, long } => {
            let files = client.list(&path).await?;
            let mut out = std::io::BufWriter::new(std::io::stdout().lock());
            if long {
                for info in client.stat_many(&files).await? {
                    writeln!(
                        out,
                        "{:>12}  {}  {}",
                        info.logical_size,
                        format_version(info.last_modified),
                        info.path
                    )?;
                }
            } else {
                for file in files {
                    writeln!(out, "{}", file)?;
                }
            }
            out.flush()?;
        }
        FilesCommand::Stat { path } => {
            let info = client
                .stat(&path)
                .await?
                .with_context(|| format!("{}: not found", path))?;
            let mut out = std::io::stdout().lock();
            writeln!(out, "path:          {}", info.path)?;
            writeln!(out, "size:          {}", info.logical_size)?;
            writeln!(out, "stored size:   {}", info.stored_size)?;
            writeln!(
                out,
                "last modified: {} ({})",
                format_version(info.last_modified),
                info.last_modified
            )?;
        }
        FilesCommand::Get { path, output } => match output.as_deref() {
            None | Some("-") => {
                // The stdout lock is held across awaits; fine, since this runs
                // directly in the main task and is never spawned.
                let stdout = std::io::stdout().lock();
                client
                    .download(&path, std::io::BufWriter::new(stdout))
                    .await?
                    .with_context(|| format!("{}: not found", path))?;
            }
            Some(output) => {
                download_to_file(&client, &path, &get_destination(&path, output)).await?;
            }
        },
        FilesCommand::Put {
            input,
            path,
            last_modified,
        } => {
            let version = version_or_now(last_modified.as_deref())?;
            let reader: Box<dyn std::io::Read + Send> = if input == "-" {
                Box::new(std::io::stdin())
            } else {
                Box::new(
                    std::fs::File::open(&input)
                        .with_context(|| format!("Cannot open {}", input))?,
                )
            };
            let result = client.upload(reader, &path, version).await?;
            if result.server_version > version {
                eprintln!(
                    "warning: server already has a newer version of {} ({}), upload ignored",
                    path,
                    format_version(result.server_version)
                );
            } else {
                println!(
                    "{}  {} bytes ({} compressed)  sha256 {}",
                    path, result.logical_size, result.compressed_size, result.sha256
                );
            }
        }
        FilesCommand::Rm {
            paths,
            last_modified,
        } => {
            let version = version_or_now(last_modified.as_deref())?;
            let mut failed = 0;
            for path in &paths {
                match client.delete(path, version).await {
                    Ok(DeleteOutcome::Deleted) => {}
                    Ok(DeleteOutcome::NotFound) => {
                        eprintln!("{}: not found", path);
                        failed += 1;
                    }
                    Ok(DeleteOutcome::NewerKept(server_version)) => {
                        eprintln!(
                            "{}: not deleted, server has a newer version ({})",
                            path,
                            format_version(server_version)
                        );
                        failed += 1;
                    }
                    Err(e) => {
                        eprintln!("{}: {:#}", path, e);
                        failed += 1;
                    }
                }
            }
            if failed > 0 {
                anyhow::bail!("{} of {} files not deleted", failed, paths.len());
            }
        }
    }
    Ok(())
}

/// Whether the error was caused by writing to a closed pipe (e.g. `| head`).
fn is_broken_pipe(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
    })
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
        Commands::Files { url, command } => {
            if let Err(e) = run_files(&url, command).await {
                if is_broken_pipe(&e) {
                    std::process::exit(0);
                }
                eprintln!("error: {:#}", e);
                std::process::exit(1);
            }
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

    #[test]
    fn test_files_put_parses() {
        let cli = Cli::try_parse_from([
            "s3dedup",
            "files",
            "--url",
            "http://localhost:8080",
            "put",
            "local.txt",
            "remote/path.txt",
            "--last-modified",
            "1700000000",
        ])
        .unwrap();
        match cli.command {
            Commands::Files {
                url,
                command:
                    FilesCommand::Put {
                        input,
                        path,
                        last_modified,
                    },
            } => {
                assert_eq!(url, "http://localhost:8080");
                assert_eq!(input, "local.txt");
                assert_eq!(path, "remote/path.txt");
                assert_eq!(last_modified.as_deref(), Some("1700000000"));
            }
            _ => panic!("Expected Files Put command"),
        }
    }

    #[test]
    fn test_files_rm_requires_path() {
        let result = Cli::try_parse_from(["s3dedup", "files", "--url", "http://x", "rm"]);
        assert!(result.is_err());
    }
}
