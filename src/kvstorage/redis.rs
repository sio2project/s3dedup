use std::error::Error;
use redis::{Client, Commands};
use crate::config::Config;
use crate::kvstorage::KVStorage;

pub struct RedisConfig {
    pub host: String,
    pub port: u16,
}

pub struct Redis {
    client: Client,
}

impl KVStorage for Redis {
    fn new(config: &Config) -> Result<Self, Box<dyn Error>> {
        let client = Client::open(
            format!(
                "redis://{}:{}/",
                config.redis.host,
                config.redis.port
            ).as_str()
        )?;
        Ok(Redis {
            client,
        })
    }

    fn setup(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>> {
        let key = format!("refcount:{}:{}", bucket, hash);
        let mut conn = self.client.get_connection()?;
        let cnt: i32 = conn.get(key).unwrap_or(0);
        Ok(cnt)
    }

    fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<(), Box<dyn Error>> {
        let key = format!("refcount:{}:{}", bucket, hash);
        let mut conn = self.client.get_connection()?;
        conn
            .set(key, ref_cnt)?;
        Ok(())
    }

    // fn increment_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
    //     let key = format!("refcount:{}:{}", bucket, hash);
    //     let mut conn = self.client.get_connection()?;
    //     conn.incr(key, 1)?
    // }
    //
    // fn decrement_ref_count(&mut self, bucket: &str, hash: &str) -> Result<(), Box<dyn Error>> {
    //     let key = format!("refcount:{}:{}", bucket, hash);
    //     let mut conn = self.client.get_connection()?;
    //     conn.decr(key, 1)?
    // }
    //
    // fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>> {
    //     let key = format!("modified:{}:{}", bucket, path);
    //     let mut conn = self.client.get_connection()?;
    //     let modified: i64 = conn.get(key).unwrap_or(0);
    //     Ok(modified)
    // }
    //
    // fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<(), Box<dyn Error>> {
    //     let key = format!("modified:{}:{}", bucket, path);
    //     let mut conn = self.client.get_connection()?;
    //     conn.set(key, modified)?;
    //     Ok(())
    // }
    //
    // fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
    //     let key = format!("modified:{}:{}", bucket, path);
    //     let mut conn = self.client.get_connection()?;
    //     conn.del(key)?;
    //     Ok(())
    // }
    //
    // fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>> {
    //     let key = format!("ref_file:{}:{}", bucket, path);
    //     let mut conn = self.client.get_connection()?;
    //     let hash: String = conn.get(key).unwrap_or("".to_string());
    //     Ok(hash)
    // }
    //
    // fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<(), Box<dyn Error>> {
    //     let key = format!("ref_file:{}:{}", bucket, path);
    //     let mut conn = self.client.get_connection()?;
    //     conn.set(key, hash)?;
    //     Ok(())
    // }
    //
    // fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
    //     let key = format!("ref_file:{}:{}", bucket, path);
    //     let mut conn = self.client.get_connection()?;
    //     conn.del(key)?;
    //     Ok(())
    // }
}