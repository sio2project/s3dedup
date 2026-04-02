use anyhow::Result;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::io::Read;

pub fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

pub fn compress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    let compressed = encoder.finish()?;
    Ok(compressed)
}

pub fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::GzDecoder;

    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

pub async fn read_body_bytes(body: axum::body::Body, max_size: usize) -> Result<Bytes> {
    use axum::body::to_bytes;

    let bytes = to_bytes(body, max_size).await?;
    Ok(bytes)
}

/// Stream HTTP body to a temp file without processing.
/// Used when headers already provide digest/sizes but the body is too large for memory.
pub async fn stream_body_to_temp_file(body: axum::body::Body) -> Result<RawTempFile> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    let temp_file = tempfile::NamedTempFile::new()?;
    let temp_path = temp_file.path().to_path_buf();

    let std_file = temp_file.as_file().try_clone()?;
    let mut async_file = tokio::fs::File::from_std(std_file);
    let mut body_stream = body.into_data_stream();
    let mut total_bytes: usize = 0;
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk.map_err(|e| anyhow::anyhow!("Failed to read body chunk: {}", e))?;
        async_file.write_all(&chunk).await?;
        total_bytes += chunk.len();
    }
    async_file.flush().await?;

    Ok(RawTempFile {
        _temp_file: temp_file,
        temp_path,
        data_size: total_bytes,
    })
}

pub struct RawTempFile {
    pub _temp_file: tempfile::NamedTempFile,
    pub temp_path: std::path::PathBuf,
    pub data_size: usize,
}

/// Stream HTTP body to a temp file, then process it to compute hash and compress.
/// Returns (digest, logical_size, compressed_size, temp_path_of_compressed_data).
///
/// For uncompressed input: streams body to temp file A, then in a blocking task
/// reads A through SHA256 hasher + GzEncoder → temp file B.
///
/// For compressed input (without headers): streams body to temp file A (compressed data),
/// then in a blocking task reads A through GzDecoder + SHA256 hasher to get the hash.
/// Returns temp file A as the compressed data.
pub async fn process_body_to_temp_file(
    body: axum::body::Body,
    is_compressed: bool,
) -> Result<ProcessedFile> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    // Stream body to temp file using async I/O to avoid blocking the runtime
    let temp_input = tempfile::NamedTempFile::new()?;
    let temp_input_path = temp_input.path().to_path_buf();

    {
        let std_file = temp_input.as_file().try_clone()?;
        let mut async_file = tokio::fs::File::from_std(std_file);
        let mut body_stream = body.into_data_stream();
        while let Some(chunk) = body_stream.next().await {
            let chunk = chunk.map_err(|e| anyhow::anyhow!("Failed to read body chunk: {}", e))?;
            async_file.write_all(&chunk).await?;
        }
        async_file.flush().await?;
    }

    let input_path = temp_input_path.clone();
    let result = tokio::task::spawn_blocking(move || {
        if is_compressed {
            // Input is compressed: compute hash by decompressing, keep original as-is
            process_compressed_temp_file(&input_path)
        } else {
            // Input is uncompressed: compute hash and compress to a new temp file
            process_uncompressed_temp_file(&input_path)
        }
    })
    .await??;

    // For uncompressed input, temp_input (raw data) can be dropped.
    // For compressed input, we need temp_input to stay alive since it IS the compressed data.
    if is_compressed {
        Ok(ProcessedFile {
            digest: result.digest,
            logical_size: result.logical_size,
            compressed_size: result.compressed_size,
            _temp_file: Some(temp_input),
            compressed_path: temp_input_path,
        })
    } else {
        Ok(ProcessedFile {
            digest: result.digest,
            logical_size: result.logical_size,
            compressed_size: result.compressed_size,
            _temp_file: result.output_temp_file,
            compressed_path: result.compressed_path,
        })
    }
}

pub struct ProcessedFile {
    pub digest: String,
    pub logical_size: usize,
    pub compressed_size: usize,
    /// Keeps the temp file alive; dropped when ProcessedFile is dropped.
    pub _temp_file: Option<tempfile::NamedTempFile>,
    pub compressed_path: std::path::PathBuf,
}

pub struct ProcessingResult {
    pub digest: String,
    pub logical_size: usize,
    pub compressed_size: usize,
    pub output_temp_file: Option<tempfile::NamedTempFile>,
    pub compressed_path: std::path::PathBuf,
}

/// Process a compressed temp file: decompress to compute hash, keep original for S3.
pub fn process_compressed_temp_file(input_path: &std::path::Path) -> Result<ProcessingResult> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let file = std::fs::File::open(input_path)?;
    let compressed_size = file.metadata()?.len() as usize;
    let mut decoder = GzDecoder::new(file);

    let mut hasher = Sha256::new();
    let mut logical_size: usize = 0;
    let mut buf = vec![0u8; 64 * 1024]; // 64KB chunks

    loop {
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        logical_size += n;
    }

    let digest = format!("{:x}", hasher.finalize());

    Ok(ProcessingResult {
        digest,
        logical_size,
        compressed_size,
        output_temp_file: None,
        compressed_path: input_path.to_path_buf(),
    })
}

/// Process an uncompressed temp file: compute hash and compress to new temp file.
pub fn process_uncompressed_temp_file(input_path: &std::path::Path) -> Result<ProcessingResult> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::{Read, Write};

    let mut input_file = std::fs::File::open(input_path)?;
    let output_temp = tempfile::NamedTempFile::new()?;
    let output_path = output_temp.path().to_path_buf();

    let mut hasher = Sha256::new();
    let mut logical_size: usize = 0;
    // Use the NamedTempFile's existing file handle via reopen to avoid double-open
    let output_file = output_temp.as_file().try_clone()?;
    let mut encoder = GzEncoder::new(output_file, Compression::default());

    let mut buf = vec![0u8; 64 * 1024]; // 64KB chunks
    loop {
        let n = input_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        encoder.write_all(&buf[..n])?;
        logical_size += n;
    }

    encoder.finish()?;
    let digest = format!("{:x}", hasher.finalize());
    let compressed_size = std::fs::metadata(&output_path)?.len() as usize;

    Ok(ProcessingResult {
        digest,
        logical_size,
        compressed_size,
        output_temp_file: Some(output_temp),
        compressed_path: output_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sha256() {
        let data = b"hello world";
        let hash = compute_sha256(data);
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_gzip_compress_decompress() {
        let original = b"hello world, this is a test string for compression";
        let compressed = compress_gzip(original).unwrap();
        let decompressed = decompress_gzip(&compressed).unwrap();
        assert_eq!(original, decompressed.as_slice());
    }

    #[test]
    fn test_process_uncompressed_temp_file() {
        use std::io::Write;

        let data = b"hello world, test data for uncompressed processing";
        let expected_hash = compute_sha256(data);
        let expected_compressed = compress_gzip(data).unwrap();

        // Write uncompressed data to temp file
        let mut input = tempfile::NamedTempFile::new().unwrap();
        input.write_all(data).unwrap();
        input.flush().unwrap();

        let result = process_uncompressed_temp_file(input.path()).unwrap();

        assert_eq!(result.digest, expected_hash, "Hash should match");
        assert_eq!(
            result.logical_size,
            data.len(),
            "Logical size should match input"
        );
        assert_eq!(
            result.compressed_size,
            expected_compressed.len(),
            "Compressed size should match gzip output"
        );

        // Verify the compressed file can be decompressed back to original
        let compressed_bytes = std::fs::read(&result.compressed_path).unwrap();
        let decompressed = decompress_gzip(&compressed_bytes).unwrap();
        assert_eq!(
            decompressed, data,
            "Decompressed output should match original"
        );
    }

    #[test]
    fn test_process_compressed_temp_file() {
        use std::io::Write;

        let data = b"hello world, test data for compressed processing";
        let expected_hash = compute_sha256(data);
        let compressed = compress_gzip(data).unwrap();

        // Write compressed data to temp file
        let mut input = tempfile::NamedTempFile::new().unwrap();
        input.write_all(&compressed).unwrap();
        input.flush().unwrap();

        let result = process_compressed_temp_file(input.path()).unwrap();

        assert_eq!(
            result.digest, expected_hash,
            "Hash should match uncompressed content"
        );
        assert_eq!(
            result.logical_size,
            data.len(),
            "Logical size should match uncompressed"
        );
        assert_eq!(
            result.compressed_size,
            compressed.len(),
            "Compressed size should match input file"
        );
        // compressed_path should point to the input (it IS the compressed data)
        assert_eq!(result.compressed_path, input.path());
    }

    #[test]
    fn test_process_uncompressed_large_chunked() {
        use std::io::Write;

        // Create data larger than the 64KB chunk buffer to test multi-chunk processing
        let data: Vec<u8> = (0..200_000).map(|i| (i % 256) as u8).collect();
        let expected_hash = compute_sha256(&data);

        let mut input = tempfile::NamedTempFile::new().unwrap();
        input.write_all(&data).unwrap();
        input.flush().unwrap();

        let result = process_uncompressed_temp_file(input.path()).unwrap();

        assert_eq!(result.digest, expected_hash);
        assert_eq!(result.logical_size, data.len());
        assert!(result.compressed_size > 0);

        // Verify roundtrip
        let compressed_bytes = std::fs::read(&result.compressed_path).unwrap();
        let decompressed = decompress_gzip(&compressed_bytes).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_process_compressed_large_chunked() {
        use std::io::Write;

        // Create data larger than the 64KB chunk buffer
        let data: Vec<u8> = (0..200_000).map(|i| (i % 256) as u8).collect();
        let expected_hash = compute_sha256(&data);
        let compressed = compress_gzip(&data).unwrap();

        let mut input = tempfile::NamedTempFile::new().unwrap();
        input.write_all(&compressed).unwrap();
        input.flush().unwrap();

        let result = process_compressed_temp_file(input.path()).unwrap();

        assert_eq!(result.digest, expected_hash);
        assert_eq!(result.logical_size, data.len());
        assert_eq!(result.compressed_size, compressed.len());
    }

    #[test]
    fn test_process_empty_file() {
        use std::io::Write;

        let data = b"";
        let expected_hash = compute_sha256(data);

        let mut input = tempfile::NamedTempFile::new().unwrap();
        input.write_all(data).unwrap();
        input.flush().unwrap();

        let result = process_uncompressed_temp_file(input.path()).unwrap();
        assert_eq!(result.digest, expected_hash);
        assert_eq!(result.logical_size, 0);
    }

    #[tokio::test]
    async fn test_process_body_to_temp_file_uncompressed() {
        let data = b"body data for temp file processing test";
        let expected_hash = compute_sha256(data);

        let body = axum::body::Body::from(data.to_vec());
        let result = process_body_to_temp_file(body, false).await.unwrap();

        assert_eq!(result.digest, expected_hash);
        assert_eq!(result.logical_size, data.len());
        assert!(result.compressed_size > 0);

        // Verify the compressed output decompresses correctly
        let compressed_bytes = std::fs::read(&result.compressed_path).unwrap();
        let decompressed = decompress_gzip(&compressed_bytes).unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_process_body_to_temp_file_compressed() {
        let data = b"body data for compressed temp file test";
        let expected_hash = compute_sha256(data);
        let compressed = compress_gzip(data).unwrap();

        let body = axum::body::Body::from(compressed.clone());
        let result = process_body_to_temp_file(body, true).await.unwrap();

        assert_eq!(result.digest, expected_hash);
        assert_eq!(result.logical_size, data.len());
        assert_eq!(result.compressed_size, compressed.len());
    }

    #[tokio::test]
    async fn test_process_body_to_temp_file_large() {
        // Test with data larger than chunk size to exercise multi-chunk streaming
        let data: Vec<u8> = (0..300_000).map(|i| (i % 251) as u8).collect();
        let expected_hash = compute_sha256(&data);

        let body = axum::body::Body::from(data.clone());
        let result = process_body_to_temp_file(body, false).await.unwrap();

        assert_eq!(result.digest, expected_hash);
        assert_eq!(result.logical_size, data.len());

        let compressed_bytes = std::fs::read(&result.compressed_path).unwrap();
        let decompressed = decompress_gzip(&compressed_bytes).unwrap();
        assert_eq!(decompressed, data);
    }
}
