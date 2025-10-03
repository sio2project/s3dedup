use anyhow::Result;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Clone, Debug, serde::Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub json: bool,
}

pub fn setup(logging_config: &LoggingConfig) -> Result<()> {
    let filter = EnvFilter::new(&logging_config.level);
    if logging_config.json {
        let subscriber = fmt::Subscriber::builder()
            .with_env_filter(filter)
            .json()
            .finish();
        Ok(tracing::subscriber::set_global_default(subscriber)?)
    } else {
        let subscriber = fmt::Subscriber::builder().with_env_filter(filter).finish();
        Ok(tracing::subscriber::set_global_default(subscriber)?)
    }
}
