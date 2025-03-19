use std::error::Error;
use serde::Deserialize;
use crate::config::Config;

pub mod postgres;
pub mod sqlite;

#[derive(Debug, Deserialize, Clone)]
pub enum KVStorageType {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "sqlite")]
    SQLite,
}

pub(crate) trait KVStorage {
    fn new(config: &Config) -> Result<Box<Self>, Box<dyn Error>>
    where
        Self: Sized;

    fn setup(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>>;
    fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<(), Box<dyn Error>>;
    fn increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        let cnt = self.get_ref_count(bucket, hash)?;
        self.set_ref_count(bucket, hash, cnt + 1)
    }

    fn decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        let cnt = self.get_ref_count(bucket, hash)?;
        self.set_ref_count(bucket, hash, cnt - 1)
    }

    fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>>;
    fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<(), Box<dyn Error>>;
    fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>>;

    fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>>;
    fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<(), Box<dyn Error>>;
    fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>>;
}