use chrono::DateTime;
use reqwest::{Client, StatusCode};
use std::error::Error;
use tracing::{debug, error, warn};

#[derive(Clone)]
pub struct FiletrackerClient {
    base_url: String,
    client: Client,
}

#[derive(Clone)]
pub struct FileMetadata {
    pub data: Vec<u8>,
    pub last_modified: i64,
    pub logical_size: usize,
    pub is_compressed: bool,
}

impl FiletrackerClient {
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // 5 minutes for large files
            .build()
            .unwrap();

        Self { base_url, client }
    }

    /// List all files under a path with modification time before the given timestamp
    pub async fn list_files(
        &self,
        path: &str,
        timestamp: i64,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        // The original filetracker expects a Unix timestamp as a string, not RFC2822
        let url = format!(
            "{}/list/{}?last_modified={}",
            self.base_url,
            path,
            timestamp
        );

        debug!("Listing files from filetracker: {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            error!("Failed to list files: HTTP {}", response.status());
            return Err(format!("HTTP {}", response.status()).into());
        }

        let body = response.text().await?;
        let files: Vec<String> = body
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| line.to_string())
            .collect();

        debug!("Listed {} files from filetracker", files.len());
        Ok(files)
    }

    /// Get a file from filetracker
    pub async fn get_file(
        &self,
        path: &str,
    ) -> Result<FileMetadata, Box<dyn Error + Send + Sync>> {
        let url = format!("{}/files/{}", self.base_url, path);
        debug!("Getting file from filetracker: {}", url);

        let response = self.client.get(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err("File not found".into());
        }

        if !response.status().is_success() {
            error!("Failed to get file: HTTP {}", response.status());
            return Err(format!("HTTP {}", response.status()).into());
        }

        // Extract headers
        let last_modified_str = response
            .headers()
            .get("Last-Modified")
            .and_then(|v| v.to_str().ok())
            .ok_or("Missing Last-Modified header")?;

        let last_modified = DateTime::parse_from_rfc2822(last_modified_str)?
            .timestamp();

        let logical_size = response
            .headers()
            .get("Logical-Size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let is_compressed = response
            .headers()
            .get("Content-Encoding")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == "gzip")
            .unwrap_or(false);

        // Get body
        let data = response.bytes().await?.to_vec();

        debug!(
            "Got file from filetracker: {} bytes, logical_size: {}, compressed: {}",
            data.len(),
            logical_size,
            is_compressed
        );

        Ok(FileMetadata {
            data,
            last_modified,
            logical_size,
            is_compressed,
        })
    }

    /// Put a file to filetracker
    pub async fn put_file(
        &self,
        path: &str,
        data: Vec<u8>,
        last_modified: i64,
        logical_size: usize,
        sha256_checksum: &str,
        is_compressed: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let timestamp_rfc2822 = DateTime::from_timestamp(last_modified, 0)
            .ok_or("Invalid timestamp")?
            .to_rfc2822();

        let url = format!(
            "{}/files/{}?last_modified={}",
            self.base_url,
            path,
            urlencoding::encode(&timestamp_rfc2822)
        );

        debug!("Putting file to filetracker: {}", url);

        let mut request = self.client.put(&url).body(data);

        if is_compressed {
            request = request.header("Content-Encoding", "gzip");
        }

        request = request
            .header("Logical-Size", logical_size.to_string())
            .header("SHA256-Checksum", sha256_checksum);

        let response = request.send().await?;

        if !response.status().is_success() {
            error!("Failed to put file: HTTP {}", response.status());
            return Err(format!("HTTP {}", response.status()).into());
        }

        debug!("Put file to filetracker successfully");
        Ok(())
    }

    /// Delete a file from filetracker
    pub async fn delete_file(
        &self,
        path: &str,
        last_modified: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let timestamp_rfc2822 = DateTime::from_timestamp(last_modified, 0)
            .ok_or("Invalid timestamp")?
            .to_rfc2822();

        let url = format!(
            "{}/files/{}?last_modified={}",
            self.base_url,
            path,
            urlencoding::encode(&timestamp_rfc2822)
        );

        debug!("Deleting file from filetracker: {}", url);

        let response = self.client.delete(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            warn!("File not found on filetracker, already deleted");
            return Ok(());
        }

        if !response.status().is_success() {
            error!("Failed to delete file: HTTP {}", response.status());
            return Err(format!("HTTP {}", response.status()).into());
        }

        debug!("Deleted file from filetracker successfully");
        Ok(())
    }
}
