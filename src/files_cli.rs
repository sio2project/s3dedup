//! Client-side file operations against a running s3dedup server.
//!
//! Talks to the Filetracker-compatible HTTP API (`/ft/files`, `/ft/list`),
//! the same way the Filetracker client used by OIOIOI does: uploads are
//! gzip-compressed with `SHA256-Checksum` and `Logical-Size` headers set,
//! downloads are transparently decompressed.

use anyhow::{Context, Result, anyhow, bail};
use chrono::DateTime;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

/// Concurrency used for HEAD requests in long listings.
const LIST_STAT_CONCURRENCY: usize = 16;

/// Largest version that can be sent over the protocol (9999-12-31 23:59:59 UTC).
pub const MAX_VERSION: i64 = 253_402_300_799;

/// Metadata of a file stored on the server, as returned by HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub path: String,
    /// File version (Unix timestamp).
    pub last_modified: i64,
    /// Size after decompression.
    pub logical_size: u64,
    /// Size of the stored (compressed) object.
    pub stored_size: u64,
}

/// Outcome of a delete request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
    /// The server has a newer version than requested and kept the file.
    NewerKept(i64),
}

/// Outcome of an upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadResult {
    pub sha256: String,
    pub logical_size: u64,
    pub compressed_size: u64,
    /// Version of the file on the server after the request. Differs from the
    /// requested version when the server already had a newer one.
    pub server_version: i64,
}

pub struct FilesClient {
    base_url: String,
    client: Client,
}

/// Normalize a user-provided server URL to the Filetracker API base (`.../ft`).
pub fn normalize_base_url(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if url.ends_with("/ft") {
        url.to_string()
    } else {
        format!("{}/ft", url)
    }
}

/// Normalize a remote path prefix: strip leading and trailing slashes.
pub fn normalize_remote_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

/// Normalize and validate a remote file path. Rejects empty paths and paths
/// with empty, `.` or `..` segments, which URL normalization would rewrite.
pub fn validate_remote_path(path: &str) -> Result<String> {
    let normalized = normalize_remote_path(path);
    if normalized.is_empty() {
        bail!("Invalid path '{}': empty", path);
    }
    if normalized
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        bail!(
            "Invalid path '{}': empty, '.' or '..' segments are not allowed",
            path
        );
    }
    Ok(normalized)
}

/// Parse a version given as a Unix timestamp or an RFC 2822 date.
pub fn parse_version(s: &str) -> Result<i64> {
    let ts = match s.parse::<i64>() {
        Ok(ts) => ts,
        Err(_) => DateTime::parse_from_rfc2822(s)
            .with_context(|| {
                format!(
                    "Invalid version '{}': expected Unix timestamp or RFC 2822",
                    s
                )
            })?
            .timestamp(),
    };
    if !(1..=MAX_VERSION).contains(&ts) {
        bail!(
            "Invalid version '{}': must be between 1 and {} (Unix seconds)",
            s,
            MAX_VERSION
        );
    }
    Ok(ts)
}

fn rfc2822(ts: i64) -> Result<String> {
    if !(1..=MAX_VERSION).contains(&ts) {
        bail!("Invalid timestamp {}", ts);
    }
    Ok(DateTime::from_timestamp(ts, 0)
        .ok_or_else(|| anyhow!("Invalid timestamp {}", ts))?
        .to_rfc2822())
}

fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn header_str<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn parse_last_modified(headers: &reqwest::header::HeaderMap) -> Result<i64> {
    let value = header_str(headers, "Last-Modified")
        .ok_or_else(|| anyhow!("Missing Last-Modified header"))?;
    Ok(DateTime::parse_from_rfc2822(value)
        .with_context(|| format!("Invalid Last-Modified header '{}'", value))?
        .timestamp())
}

async fn error_from_response(response: reqwest::Response, what: &str) -> anyhow::Error {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let body = body.trim();
    if body.is_empty() {
        anyhow!("{}: HTTP {}", what, status)
    } else {
        anyhow!("{}: HTTP {} - {}", what, status, body)
    }
}

/// Hash and gzip-compress `reader` into a temporary file.
/// Returns (temp file, hex sha256 of uncompressed data, logical size).
fn compress_to_temp(
    mut reader: impl Read,
    temp_dir: &std::path::Path,
) -> Result<(tempfile::NamedTempFile, String, u64)> {
    let temp = tempfile::NamedTempFile::new_in(temp_dir).context("Failed to create temp file")?;
    let mut encoder = flate2::write::GzEncoder::new(
        std::io::BufWriter::new(temp.as_file().try_clone()?),
        flate2::Compression::default(),
    );
    let mut hasher = Sha256::new();
    let mut logical_size: u64 = 0;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = reader.read(&mut buf).context("Failed to read input")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        encoder.write_all(&buf[..n])?;
        logical_size += n as u64;
    }
    encoder.finish()?.flush()?;
    Ok((temp, format!("{:x}", hasher.finalize()), logical_size))
}

impl FilesClient {
    pub fn new(url: &str) -> Result<Self> {
        let client = Client::builder()
            .no_gzip() // We handle Content-Encoding ourselves
            .connect_timeout(std::time::Duration::from_secs(10))
            .read_timeout(std::time::Duration::from_secs(300))
            .build()
            .context("Failed to build HTTP client")?;
        Ok(Self {
            base_url: normalize_base_url(url),
            client,
        })
    }

    fn file_url(&self, path: &str) -> String {
        format!("{}/files/{}", self.base_url, encode_path(path))
    }

    /// List files under `prefix` (recursively). Returns full paths, sorted.
    /// Includes files with versions in the future (the server would otherwise
    /// list only files up to the current time).
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = normalize_remote_path(prefix);
        let cutoff = urlencoding::encode(&rfc2822(MAX_VERSION)?).into_owned();
        let url = if prefix.is_empty() {
            format!("{}/list/?last_modified={}", self.base_url, cutoff)
        } else {
            format!(
                "{}/list/{}/?last_modified={}",
                self.base_url,
                encode_path(&prefix),
                cutoff
            )
        };
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to connect to {}", url))?;
        if !response.status().is_success() {
            return Err(error_from_response(response, &format!("LIST {}", prefix)).await);
        }
        let body = response.text().await?;
        let mut files: Vec<String> = body
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                if prefix.is_empty() {
                    line.to_string()
                } else {
                    format!("{}/{}", prefix, line)
                }
            })
            .collect();
        files.sort();
        Ok(files)
    }

    /// Get file metadata. Returns `None` if the file does not exist.
    pub async fn stat(&self, path: &str) -> Result<Option<FileInfo>> {
        let path = validate_remote_path(path)?;
        let response = self
            .client
            .head(self.file_url(&path))
            .send()
            .await
            .context("HEAD request failed")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(error_from_response(response, &format!("HEAD {}", path)).await);
        }
        let headers = response.headers();
        let parse_u64 = |name: &str| header_str(headers, name).and_then(|v| v.parse::<u64>().ok());
        Ok(Some(FileInfo {
            last_modified: parse_last_modified(headers)?,
            logical_size: parse_u64("Logical-Size").unwrap_or(0),
            stored_size: parse_u64("Content-Length").unwrap_or(0),
            path,
        }))
    }

    /// Stat many files with bounded concurrency, preserving order.
    /// Files that disappeared in the meantime are skipped.
    pub async fn stat_many(&self, paths: &[String]) -> Result<Vec<FileInfo>> {
        let results: Vec<Result<Option<FileInfo>>> = futures_util::stream::iter(paths)
            .map(|p| self.stat(p))
            .buffered(LIST_STAT_CONCURRENCY)
            .collect()
            .await;
        let mut infos = Vec::with_capacity(results.len());
        for result in results {
            if let Some(info) = result? {
                infos.push(info);
            }
        }
        Ok(infos)
    }

    /// Download a file, decompress it and write it to `out`.
    /// Returns `None` if the file does not exist.
    pub async fn download(&self, path: &str, out: impl Write) -> Result<Option<FileInfo>> {
        let path = validate_remote_path(path)?;
        let response = self
            .client
            .get(self.file_url(&path))
            .send()
            .await
            .context("GET request failed")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(error_from_response(response, &format!("GET {}", path)).await);
        }
        let headers = response.headers();
        let last_modified = parse_last_modified(headers)?;
        let expected_size = header_str(headers, "Logical-Size").and_then(|v| v.parse::<u64>().ok());
        let compressed = header_str(headers, "Content-Encoding") == Some("gzip");

        let mut out = CountingWriter::new(out);
        let mut stored_size: u64 = 0;
        let mut stream = response.bytes_stream();
        if compressed {
            // The stored object may consist of several gzip members (the server
            // keeps client-compressed data as is), so decode all of them.
            let mut decoder = flate2::write::MultiGzDecoder::new(&mut out);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.context("Failed to read response body")?;
                stored_size += chunk.len() as u64;
                decoder
                    .write_all(&chunk)
                    .context("Failed to write output")?;
            }
            decoder.try_finish().context("Failed to write output")?;
        } else {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.context("Failed to read response body")?;
                stored_size += chunk.len() as u64;
                out.write_all(&chunk).context("Failed to write output")?;
            }
        }
        out.flush().context("Failed to write output")?;

        if let Some(expected) = expected_size
            && expected != out.written
        {
            bail!(
                "Size mismatch for {}: server reported {} bytes, got {}",
                path,
                expected,
                out.written
            );
        }

        Ok(Some(FileInfo {
            path,
            last_modified,
            logical_size: out.written,
            stored_size,
        }))
    }

    /// Upload data read from `reader` as `path` with the given version.
    pub async fn upload(
        &self,
        reader: impl Read + Send + 'static,
        path: &str,
        version: i64,
    ) -> Result<UploadResult> {
        // Validate before reading and compressing the whole input.
        let path = validate_remote_path(path)?;
        let version_str = rfc2822(version)?;
        let temp_dir = std::env::temp_dir();
        let (temp, sha256, logical_size) =
            tokio::task::spawn_blocking(move || compress_to_temp(reader, &temp_dir))
                .await
                .context("Compression task failed")??;
        let compressed_size = temp.as_file().metadata()?.len();
        let file = tokio::fs::File::open(temp.path()).await?;

        let url = format!(
            "{}?last_modified={}",
            self.file_url(&path),
            urlencoding::encode(&version_str)
        );
        let response = self
            .client
            .put(&url)
            .header("Content-Encoding", "gzip")
            .header("Content-Length", compressed_size)
            .header("Content-Type", "application/octet-stream")
            .header("SHA256-Checksum", &sha256)
            .header("Logical-Size", logical_size)
            .header("Last-Modified", &version_str)
            .body(file)
            .send()
            .await
            .context("PUT request failed")?;
        if !response.status().is_success() {
            return Err(error_from_response(response, &format!("PUT {}", path)).await);
        }
        let server_version = parse_last_modified(response.headers()).unwrap_or(version);
        drop(temp);

        Ok(UploadResult {
            sha256,
            logical_size,
            compressed_size,
            server_version,
        })
    }

    /// Delete `path` if its version on the server is not newer than `version`.
    pub async fn delete(&self, path: &str, version: i64) -> Result<DeleteOutcome> {
        let path = validate_remote_path(path)?;
        let version_str = rfc2822(version)?;
        let url = format!(
            "{}?last_modified={}",
            self.file_url(&path),
            urlencoding::encode(&version_str)
        );
        let response = self
            .client
            .delete(&url)
            .header("Last-Modified", &version_str)
            .send()
            .await
            .context("DELETE request failed")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(DeleteOutcome::NotFound);
        }
        if !response.status().is_success() {
            return Err(error_from_response(response, &format!("DELETE {}", path)).await);
        }
        // The server answers 200 also when it keeps a newer version, so check.
        match self.stat(&path).await? {
            Some(info) if info.last_modified > version => {
                Ok(DeleteOutcome::NewerKept(info.last_modified))
            }
            _ => Ok(DeleteOutcome::Deleted),
        }
    }
}

struct CountingWriter<W> {
    inner: W,
    written: u64,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_base_url() {
        assert_eq!(normalize_base_url("http://h:8080"), "http://h:8080/ft");
        assert_eq!(normalize_base_url("http://h:8080/"), "http://h:8080/ft");
        assert_eq!(normalize_base_url("http://h:8080/ft"), "http://h:8080/ft");
        assert_eq!(normalize_base_url("http://h:8080/ft/"), "http://h:8080/ft");
    }

    #[test]
    fn test_normalize_remote_path() {
        assert_eq!(normalize_remote_path("/a/b/"), "a/b");
        assert_eq!(normalize_remote_path("a/b"), "a/b");
        assert_eq!(normalize_remote_path("/"), "");
    }

    #[test]
    fn test_validate_remote_path() {
        assert_eq!(validate_remote_path("/a/b c@1/").unwrap(), "a/b c@1");
        for bad in ["", "/", "a//b", "a/./b", "a/../b", ".."] {
            assert!(validate_remote_path(bad).is_err(), "{:?}", bad);
        }
    }

    #[test]
    fn test_parse_version() {
        assert_eq!(parse_version("1700000000").unwrap(), 1700000000);
        assert_eq!(
            parse_version("Tue, 14 Nov 2023 22:13:20 +0000").unwrap(),
            1700000000
        );
        assert!(parse_version("yesterday").is_err());
        assert!(parse_version("0").is_err());
        assert!(parse_version("-5").is_err());
        // Milliseconds instead of seconds
        assert!(parse_version("1700000000000").is_err());
        assert_eq!(
            parse_version(&MAX_VERSION.to_string()).unwrap(),
            MAX_VERSION
        );
        assert!(rfc2822(MAX_VERSION).is_ok());
        assert!(rfc2822(MAX_VERSION + 1).is_err());
    }

    #[test]
    fn test_encode_path_keeps_slashes() {
        assert_eq!(encode_path("a b/c@1"), "a%20b/c%401");
    }

    #[test]
    fn test_compress_to_temp_roundtrip() {
        let data = b"hello world".repeat(1000);
        let (temp, sha, size) = compress_to_temp(&data[..], &std::env::temp_dir()).unwrap();
        assert_eq!(size, data.len() as u64);
        assert_eq!(sha, format!("{:x}", Sha256::digest(&data)));
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(std::fs::File::open(temp.path()).unwrap())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, data);
    }
}
