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

pub async fn read_body_bytes(body: axum::body::Body) -> Result<Bytes> {
    use axum::body::to_bytes;

    let bytes = to_bytes(body, usize::MAX).await?;
    Ok(bytes)
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
}
