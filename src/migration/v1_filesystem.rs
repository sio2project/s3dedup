use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};
use walkdir::WalkDir;

/// Information about a file in V1 filetracker filesystem
#[derive(Debug, Clone)]
pub struct V1FileInfo {
    /// Relative path from filetracker root (e.g., "submissions/123/file.txt")
    pub relative_path: String,
    /// Absolute path to the file on filesystem
    pub absolute_path: PathBuf,
    /// Last modified timestamp (Unix timestamp)
    pub last_modified: i64,
    /// File size in bytes
    pub size: u64,
}

/// Walk V1 filetracker directory and return list of all files
///
/// V1 filetracker stores files in $FILETRACKER_DIR/files/
/// This function recursively scans that directory and returns metadata for all files.
///
/// **Note**: For large directories with millions of files, use `walk_v1_directory_chunked` instead
/// to avoid loading all file paths into memory at once.
pub fn walk_v1_directory(v1_dir: &str) -> Result<Vec<V1FileInfo>> {
    let mut files = Vec::new();
    walk_v1_directory_chunked(v1_dir, usize::MAX, |chunk| {
        files.extend_from_slice(chunk);
        Ok(())
    })?;
    Ok(files)
}

/// Walk V1 filetracker directory and process files in chunks
///
/// This function processes the directory in chunks to avoid loading all file metadata into memory
/// at once. This is essential for directories with millions of files.
///
/// The callback function is called with each chunk of files. If the callback returns an error,
/// the walk stops and the error is propagated.
///
/// # Arguments
/// * `v1_dir` - Path to V1 filetracker directory
/// * `chunk_size` - Number of files to process per chunk
/// * `callback` - Function called with each chunk of file metadata
pub fn walk_v1_directory_chunked<F>(v1_dir: &str, chunk_size: usize, mut callback: F) -> Result<()>
where
    F: FnMut(&[V1FileInfo]) -> Result<()>,
{
    let files_dir = Path::new(v1_dir).join("files");

    if !files_dir.exists() {
        anyhow::bail!(
            "V1 filetracker files directory does not exist: {}",
            files_dir.display()
        );
    }

    if !files_dir.is_dir() {
        anyhow::bail!(
            "V1 filetracker files path is not a directory: {}",
            files_dir.display()
        );
    }

    debug!(
        "Walking V1 filetracker directory in chunks of {}: {}",
        chunk_size,
        files_dir.display()
    );

    // Cap capacity at a reasonable value to avoid overflow with usize::MAX
    let capacity = chunk_size.min(10_000);
    let mut chunk = Vec::with_capacity(capacity);
    let mut total_files = 0;

    for entry_result in WalkDir::new(&files_dir).follow_links(false).into_iter() {
        // Propagate walkdir errors instead of silently skipping them
        let entry = entry_result.context("Failed to read directory entry during V1 migration")?;

        // Skip directories
        if !entry.file_type().is_file() {
            continue;
        }

        let absolute_path = entry.path().to_path_buf();

        // Get relative path from files/ directory
        let relative_path = absolute_path
            .strip_prefix(&files_dir)
            .context("Failed to strip prefix from file path")?
            .to_str()
            .context("File path contains invalid UTF-8")?
            .to_string();

        // Skip if relative path is empty (shouldn't happen, but be safe)
        if relative_path.is_empty() {
            warn!(
                "Skipping file with empty relative path: {:?}",
                absolute_path
            );
            continue;
        }

        // Get file metadata
        let metadata = entry.metadata().context("Failed to get file metadata")?;

        // Get last modified time
        let modified_time = metadata
            .modified()
            .context("Failed to get file modification time")?;

        let last_modified = modified_time
            .duration_since(std::time::UNIX_EPOCH)
            .context("File modification time is before Unix epoch")?
            .as_secs() as i64;

        let size = metadata.len();

        chunk.push(V1FileInfo {
            relative_path,
            absolute_path,
            last_modified,
            size,
        });

        total_files += 1;

        // Process chunk when it reaches the desired size
        if chunk.len() >= chunk_size {
            debug!(
                "Processing chunk of {} files (total processed: {})",
                chunk.len(),
                total_files
            );
            callback(&chunk)?;
            chunk.clear();
        }
    }

    // Process remaining files
    if !chunk.is_empty() {
        debug!(
            "Processing final chunk of {} files (total: {})",
            chunk.len(),
            total_files
        );
        callback(&chunk)?;
    }

    debug!("Finished walking {} files", total_files);

    Ok(())
}

/// Read a file from V1 filetracker filesystem
///
/// V1 files are stored uncompressed on disk.
pub fn read_v1_file(file_info: &V1FileInfo) -> Result<Vec<u8>> {
    debug!("Reading V1 file: {}", file_info.relative_path);

    let data = fs::read(&file_info.absolute_path).context(format!(
        "Failed to read file: {}",
        file_info.absolute_path.display()
    ))?;

    debug!(
        "Read {} bytes from V1 file: {}",
        data.len(),
        file_info.relative_path
    );

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_walk_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let v1_dir = temp_dir.path();

        // Create files/ directory
        fs::create_dir(v1_dir.join("files")).unwrap();

        let files = walk_v1_directory(v1_dir.to_str().unwrap()).unwrap();
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn test_walk_with_files() {
        let temp_dir = TempDir::new().unwrap();
        let v1_dir = temp_dir.path();

        // Create files/ directory structure
        let files_dir = v1_dir.join("files");
        fs::create_dir(&files_dir).unwrap();
        fs::create_dir(files_dir.join("dir1")).unwrap();
        fs::create_dir(files_dir.join("dir1/dir2")).unwrap();

        // Create test files
        fs::write(files_dir.join("file1.txt"), b"content1").unwrap();
        fs::write(files_dir.join("dir1/file2.txt"), b"content2").unwrap();
        fs::write(files_dir.join("dir1/dir2/file3.txt"), b"content3").unwrap();

        let files = walk_v1_directory(v1_dir.to_str().unwrap()).unwrap();
        assert_eq!(files.len(), 3);

        // Check relative paths are correct
        let paths: Vec<String> = files.iter().map(|f| f.relative_path.clone()).collect();
        assert!(paths.contains(&"file1.txt".to_string()));
        assert!(paths.contains(&"dir1/file2.txt".to_string()));
        assert!(paths.contains(&"dir1/dir2/file3.txt".to_string()));
    }

    #[test]
    fn test_read_v1_file() {
        let temp_dir = TempDir::new().unwrap();
        let v1_dir = temp_dir.path();

        // Create files/ directory
        let files_dir = v1_dir.join("files");
        fs::create_dir(&files_dir).unwrap();

        // Create test file
        let test_content = b"Hello, V1 Filetracker!";
        fs::write(files_dir.join("test.txt"), test_content).unwrap();

        // Walk directory to get file info
        let files = walk_v1_directory(v1_dir.to_str().unwrap()).unwrap();
        assert_eq!(files.len(), 1);

        // Read file
        let data = read_v1_file(&files[0]).unwrap();
        assert_eq!(data, test_content);
    }

    #[test]
    fn test_missing_files_directory() {
        let temp_dir = TempDir::new().unwrap();
        let v1_dir = temp_dir.path();

        let result = walk_v1_directory(v1_dir.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_symlinks_are_skipped() {
        let temp_dir = TempDir::new().unwrap();
        let v1_dir = temp_dir.path();

        let files_dir = v1_dir.join("files");
        fs::create_dir(&files_dir).unwrap();

        // Create a regular file
        fs::write(files_dir.join("real_file.txt"), b"real content").unwrap();

        // Create a symlink pointing to the regular file
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            files_dir.join("real_file.txt"),
            files_dir.join("link_file.txt"),
        )
        .unwrap();

        let files = walk_v1_directory(v1_dir.to_str().unwrap()).unwrap();

        // Only the regular file should be included; symlink is not a regular file
        assert_eq!(files.len(), 1, "Symlink should be skipped");
        assert_eq!(files[0].relative_path, "real_file.txt");
    }

    #[test]
    fn test_zero_byte_file() {
        let temp_dir = TempDir::new().unwrap();
        let v1_dir = temp_dir.path();

        let files_dir = v1_dir.join("files");
        fs::create_dir(&files_dir).unwrap();

        // Create an empty file
        fs::write(files_dir.join("empty.txt"), b"").unwrap();

        let files = walk_v1_directory(v1_dir.to_str().unwrap()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].size, 0);

        // Verify read_v1_file returns empty content
        let data = read_v1_file(&files[0]).unwrap();
        assert!(
            data.is_empty(),
            "read_v1_file should return empty Vec for 0-byte file"
        );
    }
}
