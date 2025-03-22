use crate::kvstorage::KVStorageType;
use crate::kvstorage::postgres::PostgresConfig;
use crate::kvstorage::sqlite::SQLiteConfig;
use crate::locks::LocksType;
use std::error::Error;
use crate::logging::LoggingConfig;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Config {
    pub logging: LoggingConfig,

    pub kvstorage_type: KVStorageType,

    #[serde(default)]
    pub postgres: Option<PostgresConfig>,

    #[serde(default)]
    pub sqlite: Option<SQLiteConfig>,

    pub locks_type: LocksType,
}

impl Config {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(config_str.as_str())?;
        Ok(config)
    }
}
