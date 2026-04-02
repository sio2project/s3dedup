use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use chrono::DateTime;
use reqwest::{Client, Response, StatusCode};
use tracing::{debug, error, warn};

/// Parse common filetracker response headers (Last-Modified, Logical-Size, Content-Encoding).
fn parse_ft_headers(response: &Response) -> Result<(i64, usize, bool)> {
    let last_modified_str = response
        .headers()
        .get("Last-Modified")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("Missing Last-Modified header"))?;
    let last_modified = DateTime::parse_from_rfc2822(last_modified_str)?.timestamp();

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

    Ok((last_modified, logical_size, is_compressed))
}

/// Error indicating that the requested file was not found on the filetracker (HTTP 404).
/// This is distinct from connection errors or server errors, which should be retried.
#[derive(Debug)]
pub struct FileNotFoundError(pub String);

impl std::fmt::Display for FileNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "File not found: {}", self.0)
    }
}

impl std::error::Error for FileNotFoundError {}

/// Transient error that should be retried (connection failures, timeouts, server errors).
/// Non-transient errors (corrupt data, bad headers) should NOT be retried.
#[derive(Debug)]
pub struct TransientError(pub String);

impl std::fmt::Display for TransientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transient error: {}", self.0)
    }
}

impl std::error::Error for TransientError {}

#[derive(Clone)]
pub struct FiletrackerClient {
    base_url: String,
    client: Client,
}

/// Result of downloading a file — either buffered in memory or streamed to temp file.
pub enum DownloadedFile {
    InMemory(FileMetadata),
    OnDisk(StreamingFileMetadata),
}

#[derive(Clone)]
pub struct FileHeaders {
    pub last_modified: i64,
    pub logical_size: usize,
    pub content_length: usize,
    pub is_compressed: bool,
}

pub struct StreamingFileMetadata {
    pub last_modified: i64,
    pub logical_size: usize,
    pub is_compressed: bool,
    /// Temp file holding the downloaded data (kept alive to prevent cleanup)
    pub temp_file: tempfile::NamedTempFile,
    pub temp_path: std::path::PathBuf,
    pub data_size: usize,
}

#[derive(Clone)]
pub struct FileMetadata {
    pub data: Vec<u8>,
    pub last_modified: i64,
    pub logical_size: usize,
    pub is_compressed: bool,
}

/// Check HTTP response status and return an appropriate error for non-success responses.
/// Returns Ok(()) if the response is successful.
/// Returns FileNotFoundError for 404, TransientError for 5xx, and a generic error otherwise.
async fn check_error_response(response: Response, path: &str) -> Result<Response> {
    if response.status() == StatusCode::NOT_FOUND {
        return Err(FileNotFoundError(path.to_string()).into());
    }

    if !response.status().is_success() {
        let status = response.status();
        let x_exception = response
            .headers()
            .get("X-Exception")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        let body = response.text().await.unwrap_or_default();
        error!(
            "Filetracker error for '{}': HTTP {} - X-Exception: {} - Body: {}",
            path, status, x_exception, body
        );
        let msg = format!("HTTP {} - {}", status, x_exception);
        if status.is_server_error() {
            return Err(TransientError(msg).into());
        }
        bail!("{}", msg)
    }

    Ok(response)
}

impl FiletrackerClient {
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // 5 minutes for large files
            .no_gzip() // Disable automatic decompression to preserve Content-Encoding header
            .build()
            .unwrap();

        Self { base_url, client }
    }

    /// List all files under a path.
    ///
    /// The `timestamp` parameter is optional - if provided, only returns files with
    /// modification time <= timestamp. If None, defaults to current time (returns all files).
    ///
    /// Note: The original Filetracker has a bug where providing a timestamp as a string
    /// causes a TypeError. To avoid this bug, we don't send the parameter at all unless
    /// explicitly needed, letting Filetracker default to current time.
    pub async fn list_files(&self, path: &str, timestamp: Option<i64>) -> Result<Vec<String>> {
        // Build URL - only include last_modified parameter if timestamp is provided
        // This avoids triggering the bug in the original Filetracker server where
        // string-to-int conversion is missing for the timestamp parameter
        let url = if let Some(ts) = timestamp {
            format!("{}/list/{}?last_modified={}", self.base_url, path, ts)
        } else {
            format!("{}/list/{}", self.base_url, path)
        };

        debug!("Listing files from filetracker: {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let x_exception = response
                .headers()
                .get("X-Exception")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown")
                .to_string();
            let body = response.text().await.unwrap_or_default();
            error!(
                "Failed to list files: HTTP {} - X-Exception: {} - Body: {}",
                status, x_exception, body
            );
            bail!("HTTP {} - {}", status, x_exception);
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

    /// Check if a file exists in filetracker and get its metadata (no body download).
    /// Uses HTTP HEAD to avoid downloading the file content.
    pub async fn head_file(&self, path: &str) -> Result<FileHeaders> {
        let url = format!("{}/files/{}", self.base_url, path);
        debug!("HEAD file from filetracker: {}", url);

        let response = self
            .client
            .head(&url)
            .send()
            .await
            .map_err(|e| TransientError(e.to_string()))?;
        let response = check_error_response(response, path).await?;

        let (last_modified, logical_size, is_compressed) = parse_ft_headers(&response)?;
        let content_length = response
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        Ok(FileHeaders {
            last_modified,
            logical_size,
            content_length,
            is_compressed,
        })
    }

    /// Download a file from filetracker. Uses Content-Length to decide:
    /// - Small files (≤ max_inmemory_size): buffer in memory (fast, no disk I/O)
    /// - Large files (> max_inmemory_size) or unknown size: stream to temp file
    pub async fn download_file(
        &self,
        path: &str,
        max_inmemory_size: usize,
    ) -> Result<DownloadedFile> {
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;

        let url = format!("{}/files/{}", self.base_url, path);
        debug!("Downloading file from filetracker: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TransientError(e.to_string()))?;
        let response = check_error_response(response, path).await?;

        let (last_modified, logical_size, is_compressed) = parse_ft_headers(&response)?;
        let content_length = response.content_length().unwrap_or(0);
        let use_memory = content_length > 0 && content_length <= max_inmemory_size as u64;

        if use_memory {
            // Small file: buffer in memory
            let data = response
                .bytes()
                .await
                .map_err(|e| TransientError(e.to_string()))?
                .to_vec();

            debug!(
                "Downloaded file to memory: {} bytes, compressed: {}",
                data.len(),
                is_compressed
            );

            Ok(DownloadedFile::InMemory(FileMetadata {
                data,
                last_modified,
                logical_size,
                is_compressed,
            }))
        } else {
            // Large or unknown size: stream to temp file
            let temp_file = tempfile::NamedTempFile::new()?;
            let temp_path = temp_file.path().to_path_buf();
            let std_file = temp_file.as_file().try_clone()?;
            let mut async_file = tokio::fs::File::from_std(std_file);

            let mut stream = response.bytes_stream();
            let mut total_bytes: usize = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| TransientError(e.to_string()))?;
                async_file
                    .write_all(&chunk)
                    .await
                    .map_err(|e| anyhow!("Failed to write to temp file: {}", e))?;
                total_bytes += chunk.len();
            }
            async_file
                .flush()
                .await
                .map_err(|e| anyhow!("Failed to flush temp file: {}", e))?;

            debug!(
                "Downloaded file to disk: {} bytes, compressed: {}",
                total_bytes, is_compressed
            );

            Ok(DownloadedFile::OnDisk(StreamingFileMetadata {
                last_modified,
                logical_size,
                is_compressed,
                temp_file,
                temp_path,
                data_size: total_bytes,
            }))
        }
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
    ) -> Result<()> {
        let timestamp_rfc2822 = DateTime::from_timestamp(last_modified, 0)
            .ok_or_else(|| anyhow!("Invalid timestamp"))?
            .to_rfc2822();

        let url = format!(
            "{}/files/{}?last_modified={}",
            self.base_url,
            path,
            urlencoding::encode(&timestamp_rfc2822)
        );

        debug!("Putting file to filetracker: {}", url);

        let mut request = self.client.put(&url).body(data);

        // V1 filetracker reads timestamp from Last-Modified header, not query param
        // We send both for compatibility with V1 and V2
        request = request.header("Last-Modified", &timestamp_rfc2822);

        if is_compressed {
            request = request.header("Content-Encoding", "gzip");
        }

        request = request
            .header("Logical-Size", logical_size.to_string())
            .header("SHA256-Checksum", sha256_checksum);

        let response = request.send().await?;
        check_error_response(response, path).await?;

        debug!("Put file to filetracker successfully");
        Ok(())
    }

    /// Delete a file from filetracker
    pub async fn delete_file(&self, path: &str, last_modified: i64) -> Result<()> {
        let timestamp_rfc2822 = DateTime::from_timestamp(last_modified, 0)
            .ok_or_else(|| anyhow!("Invalid timestamp"))?
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

        check_error_response(response, path).await?;

        debug!("Deleted file from filetracker successfully");
        Ok(())
    }
}
