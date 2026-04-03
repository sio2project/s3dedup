use anyhow::{Result, anyhow};
use aws_sdk_s3::primitives::ByteStream;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// In-memory S3 mock with configurable failure injection.
#[derive(Clone)]
pub struct MockS3Storage {
    inner: std::sync::Arc<MockS3Inner>,
}

struct MockS3Inner {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    fail_ops: Mutex<HashSet<String>>,
}

impl Default for MockS3Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl MockS3Storage {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(MockS3Inner {
                objects: Mutex::new(HashMap::new()),
                fail_ops: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Configure an operation to fail.
    pub fn set_failing(&self, op: &str) {
        self.inner.fail_ops.lock().unwrap().insert(op.to_string());
    }

    pub fn clear_failing(&self, op: &str) {
        self.inner.fail_ops.lock().unwrap().remove(op);
    }

    fn check_fail(&self, op: &str) -> Result<()> {
        if self.inner.fail_ops.lock().unwrap().contains(op) {
            Err(anyhow!("MockS3Storage: injected failure for {}", op))
        } else {
            Ok(())
        }
    }

    pub async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.check_fail("put_object")?;
        let mut map = self.inner.objects.lock().unwrap();
        map.insert(key.to_string(), data);
        Ok(())
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        self.check_fail("get_object")?;
        let map = self.inner.objects.lock().unwrap();
        map.get(key)
            .cloned()
            .ok_or_else(|| anyhow!("Object not found: {}", key))
    }

    pub async fn get_object_stream(&self, key: &str) -> Result<(ByteStream, Option<i64>)> {
        self.check_fail("get_object_stream")?;
        let data = self.get_object(key).await?;
        let len = data.len() as i64;
        Ok((ByteStream::from(data), Some(len)))
    }

    pub async fn put_object_stream(
        &self,
        key: &str,
        body: ByteStream,
        _content_length: Option<i64>,
    ) -> Result<()> {
        self.check_fail("put_object_stream")?;
        let data = body.collect().await?.to_vec();
        self.put_object(key, data).await
    }

    pub async fn delete_object(&self, key: &str) -> Result<()> {
        self.check_fail("delete_object")?;
        let mut map = self.inner.objects.lock().unwrap();
        map.remove(key);
        Ok(())
    }

    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        self.check_fail("object_exists")?;
        let map = self.inner.objects.lock().unwrap();
        Ok(map.contains_key(key))
    }

    pub async fn object_exists_with_size(&self, key: &str) -> Result<Option<i64>> {
        self.check_fail("object_exists_with_size")?;
        let map = self.inner.objects.lock().unwrap();
        Ok(map.get(key).map(|d| d.len() as i64))
    }

    pub async fn list_objects(
        &self,
        _continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        self.check_fail("list_objects")?;
        let map = self.inner.objects.lock().unwrap();
        let keys: Vec<String> = map.keys().cloned().collect();
        Ok((keys, None))
    }

    pub async fn check_health(&self) -> Result<()> {
        self.check_fail("check_health")?;
        Ok(())
    }
}
