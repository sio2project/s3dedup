use anyhow::Result;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone, Debug, serde::Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    /// If true (and json_log_path is not set), stdout uses JSON format.
    /// Ignored when json_log_path is set (stdout is always pretty in dual mode).
    pub json: bool,
    /// Optional path for JSON log file. When set, stdout gets colored pretty
    /// logs and the file gets JSON logs (for rsyslog / scraping).
    #[serde(default)]
    pub json_log_path: Option<String>,
}

/// Sets up the global tracing subscriber.
///
/// Returns an optional `WorkerGuard` that **must be held alive** for the
/// lifetime of the process — dropping it flushes and stops the background
/// writer thread.
pub fn setup(logging_config: &LoggingConfig) -> Result<Option<WorkerGuard>> {
    let filter = EnvFilter::new(&logging_config.level);

    if let Some(ref path) = logging_config.json_log_path {
        // Dual output: pretty stdout + JSON file
        let json_filter = EnvFilter::new(&logging_config.level);

        let stdout_layer = fmt::layer().with_ansi(true);

        let log_path = std::path::Path::new(path);
        let dir = log_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let filename = log_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("s3dedup.json"));

        let file_appender = tracing_appender::rolling::never(dir, filename);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let json_layer = fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(non_blocking);

        tracing_subscriber::registry()
            .with(filter)
            .with(stdout_layer)
            .with(json_layer.with_filter(json_filter))
            .init();

        Ok(Some(guard))
    } else if logging_config.json {
        // JSON-only stdout (legacy behavior)
        let subscriber = fmt::Subscriber::builder()
            .with_env_filter(filter)
            .json()
            .finish();
        tracing::subscriber::set_global_default(subscriber)?;
        Ok(None)
    } else {
        // Pretty stdout only (legacy behavior)
        let subscriber = fmt::Subscriber::builder().with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber)?;
        Ok(None)
    }
}
