use tracing::dispatcher::SetGlobalDefaultError;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Clone, Debug, serde::Deserialize)]
pub struct LoggingConfig {
    level: String,
    json: bool,
}

pub fn setup(logging_config: &LoggingConfig) -> Result<(), SetGlobalDefaultError> {
    let filter = EnvFilter::new(&logging_config.level);
    if logging_config.json {
        let subscriber = fmt::Subscriber::builder()
            .with_env_filter(filter)
            .json()
            .finish();
        tracing::subscriber::set_global_default(subscriber)
    } else {
        let subscriber = fmt::Subscriber::builder()
            .with_env_filter(filter)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
    }
}