use crate::kvstorage::postgres::PostgresConfig;
use crate::kvstorage::redis::RedisConfig;
use crate::kvstorage::sqlite::SQLiteConfig;

pub struct Config {
    pub postgres: PostgresConfig,
    pub sqlite: SQLiteConfig,
    pub redis: RedisConfig,
}