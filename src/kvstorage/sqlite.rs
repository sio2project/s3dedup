use rusqlite::{params, Connection};
use crate::config::Config;
use crate::kvstorage::KVStorage;

pub struct SQLiteConfig {
    pub path: String,
}

pub struct SQLite {
    conn: Connection,
}

impl KVStorage for SQLite {
    fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open(config.sqlite.path.as_str())?;
        Ok(SQLite {
            conn,
        })
    }

    fn setup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS refcount (
                bucket TEXT NOT NULL,
                hash TEXT NOT NULL,
                refcount INTEGER NOT NULL,
                PRIMARY KEY (bucket, hash)
            );
            CREATE TABLE IF NOT EXISTS modified (
                bucket TEXT NOT NULL,
                path TEXT NOT NULL,
                modified INTEGER NOT NULL,
                PRIMARY KEY (bucket, path)
            );
            CREATE TABLE IF NOT EXISTS ref_file (
                bucket TEXT NOT NULL,
                path TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (bucket, path)
            );"
        )?;
        Ok(())
    }

    fn get_ref_count(&mut self, bucket: &str, hash: &str) -> Result<i32, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare("SELECT refcount FROM refcount WHERE bucket = ?1 AND hash = ?2")?;
        let mut rows = stmt.query(params![&bucket, &hash])?;
        if let Some(row) = rows.next()? {
            Ok(row.get(0)?)
        } else {
            Ok(0)
        }
    }

    fn set_ref_count(&mut self, bucket: &str, hash: &str, ref_cnt: i32) -> Result<(), Box<dyn std::error::Error>> {
        self.conn.execute("INSERT OR REPLACE INTO refcount (bucket, hash, refcount) VALUES (?1, ?2, ?3)", params![&bucket, &hash, &ref_cnt])?;
        Ok(())
    }

    fn get_modified(&mut self, bucket: &str, path: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare("SELECT modified FROM modified WHERE bucket = ?1 AND path = ?2")?;
        let mut rows = stmt.query(params![&bucket, &path])?;
        if let Some(row) = rows.next()? {
            Ok(row.get(0)?)
        } else {
            Ok(0)
        }
    }

    fn set_modified(&mut self, bucket: &str, path: &str, modified: i64) -> Result<(), Box<dyn std::error::Error>> {
        self.conn.execute("INSERT OR REPLACE INTO modified (bucket, path, modified) VALUES (?1, ?2, ?3)", params![&bucket, &path, &modified])?;
        Ok(())
    }

    fn delete_modified(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.conn.execute("DELETE FROM modified WHERE bucket = ?1 AND path = ?2", params![&bucket, &path])?;
        Ok(())
    }

    fn get_ref_file(&mut self, bucket: &str, path: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare("SELECT hash FROM ref_file WHERE bucket = ?1 AND path = ?2")?;
        let mut rows = stmt.query(params![&bucket, &path])?;
        if let Some(row) = rows.next()? {
            Ok(row.get(0)?)
        } else {
            Ok("".to_string())
        }
    }

    fn set_ref_file(&mut self, bucket: &str, path: &str, hash: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.conn.execute("INSERT OR REPLACE INTO ref_file (bucket, path, hash) VALUES (?1, ?2, ?3)", params![&bucket, &path, &hash])?;
        Ok(())
    }

    fn delete_ref_file(&mut self, bucket: &str, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.conn.execute("DELETE FROM ref_file WHERE bucket = ?1 AND path = ?2", params![&bucket, &path])?;
        Ok(())
    }
}