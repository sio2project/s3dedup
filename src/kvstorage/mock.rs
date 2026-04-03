use crate::kvstorage::{KVStorageTrait, StorageStats};
use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// In-memory KVStorage mock with configurable failure injection.
///
/// By default, all operations succeed using in-memory HashMaps.
/// Call `set_failing("operation_name")` to make specific operations return errors.
#[derive(Clone)]
pub struct MockKVStorage {
    inner: std::sync::Arc<MockInner>,
}

struct MockInner {
    refcount: Mutex<HashMap<(String, String), i32>>,
    modified: Mutex<HashMap<(String, String), i64>>,
    ref_file: Mutex<HashMap<(String, String), String>>,
    logical_size: Mutex<HashMap<(String, String), usize>>,
    compressed_size: Mutex<HashMap<(String, String), usize>>,
    version: Mutex<Option<String>>,
    /// Set of operation names that should fail (e.g., "set_modified", "set_ref_file")
    fail_ops: Mutex<HashSet<String>>,
}

impl Default for MockKVStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MockKVStorage {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(MockInner {
                refcount: Mutex::new(HashMap::new()),
                modified: Mutex::new(HashMap::new()),
                ref_file: Mutex::new(HashMap::new()),
                logical_size: Mutex::new(HashMap::new()),
                compressed_size: Mutex::new(HashMap::new()),
                version: Mutex::new(None),
                fail_ops: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Configure an operation to fail. Operation names match trait method names.
    pub fn set_failing(&self, op: &str) {
        self.inner.fail_ops.lock().unwrap().insert(op.to_string());
    }

    /// Stop an operation from failing.
    pub fn clear_failing(&self, op: &str) {
        self.inner.fail_ops.lock().unwrap().remove(op);
    }

    fn check_fail(&self, op: &str) -> Result<()> {
        if self.inner.fail_ops.lock().unwrap().contains(op) {
            Err(anyhow!("MockKVStorage: injected failure for {}", op))
        } else {
            Ok(())
        }
    }
}

impl KVStorageTrait for MockKVStorage {
    async fn new(_config: &crate::config::Config) -> Result<Box<Self>> {
        Ok(Box::new(MockKVStorage::new()))
    }

    async fn setup(&self) -> Result<()> {
        Ok(())
    }

    async fn get_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        self.check_fail("get_ref_count")?;
        let map = self.inner.refcount.lock().unwrap();
        Ok(*map
            .get(&(bucket.to_string(), hash.to_string()))
            .unwrap_or(&0))
    }

    async fn atomic_increment_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        self.check_fail("atomic_increment_ref_count")?;
        let mut map = self.inner.refcount.lock().unwrap();
        let entry = map
            .entry((bucket.to_string(), hash.to_string()))
            .or_insert(0);
        *entry += 1;
        Ok(*entry)
    }

    async fn atomic_decrement_ref_count(&self, bucket: &str, hash: &str) -> Result<i32> {
        self.check_fail("atomic_decrement_ref_count")?;
        let mut map = self.inner.refcount.lock().unwrap();
        let entry = map
            .entry((bucket.to_string(), hash.to_string()))
            .or_insert(0);
        if *entry > 0 {
            *entry -= 1;
        }
        Ok(*entry)
    }

    async fn get_modified(&self, bucket: &str, path: &str) -> Result<i64> {
        self.check_fail("get_modified")?;
        let map = self.inner.modified.lock().unwrap();
        Ok(*map
            .get(&(bucket.to_string(), path.to_string()))
            .unwrap_or(&0))
    }

    async fn set_modified(&self, bucket: &str, path: &str, modified: i64) -> Result<()> {
        self.check_fail("set_modified")?;
        let mut map = self.inner.modified.lock().unwrap();
        map.insert((bucket.to_string(), path.to_string()), modified);
        Ok(())
    }

    async fn delete_modified(&self, bucket: &str, path: &str) -> Result<()> {
        self.check_fail("delete_modified")?;
        let mut map = self.inner.modified.lock().unwrap();
        map.remove(&(bucket.to_string(), path.to_string()));
        Ok(())
    }

    async fn get_ref_file(&self, bucket: &str, path: &str) -> Result<String> {
        self.check_fail("get_ref_file")?;
        let map = self.inner.ref_file.lock().unwrap();
        Ok(map
            .get(&(bucket.to_string(), path.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    async fn set_ref_file(&self, bucket: &str, path: &str, hash: &str) -> Result<()> {
        self.check_fail("set_ref_file")?;
        let mut map = self.inner.ref_file.lock().unwrap();
        map.insert((bucket.to_string(), path.to_string()), hash.to_string());
        Ok(())
    }

    async fn delete_ref_file(&self, bucket: &str, path: &str) -> Result<()> {
        self.check_fail("delete_ref_file")?;
        let mut map = self.inner.ref_file.lock().unwrap();
        map.remove(&(bucket.to_string(), path.to_string()));
        Ok(())
    }

    async fn get_logical_size(&self, bucket: &str, hash: &str) -> Result<usize> {
        self.check_fail("get_logical_size")?;
        let map = self.inner.logical_size.lock().unwrap();
        Ok(*map
            .get(&(bucket.to_string(), hash.to_string()))
            .unwrap_or(&0))
    }

    async fn set_logical_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        self.check_fail("set_logical_size")?;
        let mut map = self.inner.logical_size.lock().unwrap();
        map.insert((bucket.to_string(), hash.to_string()), size);
        Ok(())
    }

    async fn get_compressed_size(&self, bucket: &str, hash: &str) -> Result<usize> {
        self.check_fail("get_compressed_size")?;
        let map = self.inner.compressed_size.lock().unwrap();
        Ok(*map
            .get(&(bucket.to_string(), hash.to_string()))
            .unwrap_or(&0))
    }

    async fn set_compressed_size(&self, bucket: &str, hash: &str, size: usize) -> Result<()> {
        self.check_fail("set_compressed_size")?;
        let mut map = self.inner.compressed_size.lock().unwrap();
        map.insert((bucket.to_string(), hash.to_string()), size);
        Ok(())
    }

    async fn list_files(
        &self,
        bucket: &str,
        path_prefix: &str,
        timestamp: i64,
    ) -> Result<Vec<String>> {
        self.check_fail("list_files")?;
        let modified = self.inner.modified.lock().unwrap();
        let mut results: Vec<String> = modified
            .iter()
            .filter(|((b, p), ts)| b == bucket && p.starts_with(path_prefix) && **ts <= timestamp)
            .map(|((_, p), _)| p.clone())
            .collect();
        results.sort();
        Ok(results)
    }

    async fn list_orphaned_ref_files(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        self.check_fail("list_orphaned_ref_files")?;
        let ref_file_map = self.inner.ref_file.lock().unwrap();
        let refcount_map = self.inner.refcount.lock().unwrap();
        let mut entries: Vec<(String, String)> = ref_file_map
            .iter()
            .filter(|((b, _), _)| b == bucket)
            .filter(|((_, _), hash)| {
                let rc = refcount_map
                    .get(&(bucket.to_string(), hash.to_string()))
                    .copied()
                    .unwrap_or(0);
                rc == 0
            })
            .map(|((_, p), h)| (p.clone(), h.clone()))
            .filter(|(p, _)| p.as_str() > after_cursor)
            .collect();
        entries.sort();
        Ok(entries.into_iter().take(limit).collect())
    }

    async fn list_orphaned_refcounts(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<(String, i32)>> {
        self.check_fail("list_orphaned_refcounts")?;
        let refcount_map = self.inner.refcount.lock().unwrap();
        let ref_file_map = self.inner.ref_file.lock().unwrap();
        let mut entries: Vec<(String, i32)> = refcount_map
            .iter()
            .filter(|((b, _), _)| b == bucket)
            .filter(|((_, hash), _)| {
                !ref_file_map
                    .iter()
                    .any(|((b, _), h)| b == bucket && h == hash)
            })
            .map(|((_, h), &c)| (h.clone(), c))
            .filter(|(h, _)| h.as_str() > after_cursor)
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries.into_iter().take(limit).collect())
    }

    async fn list_orphaned_logical_sizes(
        &self,
        bucket: &str,
        after_cursor: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.check_fail("list_orphaned_logical_sizes")?;
        let ls_map = self.inner.logical_size.lock().unwrap();
        let refcount_map = self.inner.refcount.lock().unwrap();
        let mut entries: Vec<String> = ls_map
            .iter()
            .filter(|((b, _), _)| b == bucket)
            .filter(|((_, hash), _)| {
                let rc = refcount_map
                    .get(&(bucket.to_string(), hash.to_string()))
                    .copied()
                    .unwrap_or(0);
                rc == 0
            })
            .map(|((_, h), _)| h.clone())
            .filter(|h| h.as_str() > after_cursor)
            .collect();
        entries.sort();
        Ok(entries.into_iter().take(limit).collect())
    }

    async fn delete_refcount(&self, bucket: &str, hash: &str) -> Result<()> {
        self.check_fail("delete_refcount")?;
        let mut map = self.inner.refcount.lock().unwrap();
        map.remove(&(bucket.to_string(), hash.to_string()));
        Ok(())
    }

    async fn delete_logical_size(&self, bucket: &str, hash: &str) -> Result<()> {
        self.check_fail("delete_logical_size")?;
        let mut map = self.inner.logical_size.lock().unwrap();
        map.remove(&(bucket.to_string(), hash.to_string()));
        Ok(())
    }

    async fn hash_is_referenced(&self, bucket: &str, hash: &str) -> Result<bool> {
        self.check_fail("hash_is_referenced")?;
        let map = self.inner.ref_file.lock().unwrap();
        Ok(map.iter().any(|((b, _), h)| b == bucket && h == hash))
    }

    async fn get_total_files(&self, bucket: &str) -> Result<i64> {
        self.check_fail("get_total_files")?;
        let map = self.inner.modified.lock().unwrap();
        Ok(map
            .iter()
            .filter(|((b, _), v)| b == bucket && **v > 0)
            .count() as i64)
    }

    async fn get_total_blobs(&self, bucket: &str) -> Result<i64> {
        let map = self.inner.refcount.lock().unwrap();
        Ok(map
            .iter()
            .filter(|((b, _), v)| b == bucket && **v > 0)
            .count() as i64)
    }

    async fn get_total_storage_bytes(&self, _bucket: &str) -> Result<i64> {
        Ok(0)
    }

    async fn get_total_logical_bytes(&self, _bucket: &str) -> Result<i64> {
        Ok(0)
    }

    async fn get_version(&self) -> Result<Option<String>> {
        Ok(self.inner.version.lock().unwrap().clone())
    }

    async fn set_version(&self, version: &str) -> Result<()> {
        *self.inner.version.lock().unwrap() = Some(version.to_string());
        Ok(())
    }

    async fn get_deduplicated_bytes_saved(&self, _bucket: &str) -> Result<i64> {
        Ok(0)
    }

    async fn get_total_compressed_bytes_no_dedup(&self, _bucket: &str) -> Result<i64> {
        Ok(0)
    }

    async fn get_storage_stats(&self, bucket: &str) -> Result<StorageStats> {
        self.check_fail("get_storage_stats")?;
        Ok(StorageStats {
            total_files: self.get_total_files(bucket).await?,
            total_blobs: self.get_total_blobs(bucket).await?,
            ..Default::default()
        })
    }
}
