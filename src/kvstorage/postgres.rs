use postgres::{Client};
use std::error::Error;
use crate::config::Config;
use crate::kvstorage::KVStorage;

pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

pub struct Postgres {
    conn: Client,
}

impl KVStorage for Postgres {
    fn new(config: &Config) -> Result<Self, Box<dyn Error>> {
        let conn = Client::connect(
            format!(
                "host={} port={} user={} password={} dbname={}",
                config.postgres.host,
                config.postgres.port,
                config.postgres.user,
                config.postgres.password,
                config.postgres.dbname
            ).as_str(),
            postgres::NoTls
        )?;
        Ok(Postgres {
            conn,
        })
    }
    fn setup(&mut self) -> Result<(), Box<dyn Error>> {
        self.conn.batch_execute(
            "CREATE TABLE IF NOT EXISTS refcount (
                bucket VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                refcount INT NOT NULL,
                PRIMARY KEY (bucket, hash)
            );
            CREATE TABLE IF NOT EXISTS modified (
                bucket VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                modified BIGINT NOT NULL,
                PRIMARY KEY (bucket, path)
            );
            CREATE TABLE IF NOT EXISTS ref_file (
                bucket VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                hash VARCHAR(255) NOT NULL,
                PRIMARY KEY (bucket, path)
            );"
        )?;
        Ok(())
    }

    fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn Error>> {
        let stmt = self.conn.prepare("SELECT refcount FROM refcount WHERE bucket = $1 AND hash = $2")?;
        let rows = self.conn.query(&stmt, &[&bucket, &hash])?;
        if rows.len() == 0 {
            Ok(0)
        } else {
            Ok(rows.get(0).unwrap().get(0))
        }
    }

    fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<(), Box<dyn Error>> {
        let stmt = self.conn.prepare("INSERT INTO refcount (bucket, hash, refcount) VALUES ($1, $2, $3) ON CONFLICT (bucket, hash) DO UPDATE SET refcount = $3")?;
        self.conn.execute(&stmt, &[&bucket, &hash, &ref_cnt])?;
        Ok(())
    }

    fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn Error>> {
        let stmt = self.conn.prepare("SELECT modified FROM modified WHERE bucket = $1 AND path = $2")?;
        let rows = self.conn.query(&stmt, &[&bucket, &path])?;
        if rows.len() == 0 {
            Ok(0)
        } else {
            Ok(rows.get(0).unwrap().get(0))
        }
    }

    fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<(), Box<dyn Error>> {
        let stmt = self.conn.prepare("INSERT INTO modified (bucket, path, modified) VALUES ($1, $2, $3) ON CONFLICT (bucket, path) DO UPDATE SET modified = $3")?;
        self.conn.execute(&stmt, &[&bucket, &path, &modified])?;
        Ok(())
    }

    fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
        let stmt = self.conn.prepare("DELETE FROM modified WHERE bucket = $1 AND path = $2")?;
        self.conn.execute(&stmt, &[&bucket, &path])?;
        Ok(())
    }

    fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn Error>> {
        let stmt = self.conn.prepare("SELECT hash FROM ref_file WHERE bucket = $1 AND path = $2")?;
        let rows = self.conn.query(&stmt, &[&bucket, &path])?;
        if rows.len() == 0 {
            Ok("".to_string())
        } else {
            Ok(rows.get(0).unwrap().get(0))
        }
    }

    fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<(), Box<dyn Error>> {
        let stmt = self.conn.prepare("INSERT INTO ref_file (bucket, path, hash) VALUES ($1, $2, $3) ON CONFLICT (bucket, path) DO UPDATE SET hash = $3")?;
        self.conn.execute(&stmt, &[&bucket, &path, &hash])?;
        Ok(())
    }

    fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn Error>> {
        let stmt = self.conn.prepare("DELETE FROM ref_file WHERE bucket = $1 AND path = $2")?;
        self.conn.execute(&stmt, &[&bucket, &path])?;
        Ok(())
    }
}