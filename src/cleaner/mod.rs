use crate::kvstorage::KVStorage;
use crate::locks::{self, LocksStorage};
use crate::metrics::{
    CLEANER_DELETED_BLOBS_TOTAL, CLEANER_ERRORS_TOTAL, CLEANER_FREED_BYTES_TOTAL,
    CLEANER_LAST_RUN_TIMESTAMP, CLEANER_TOTAL_RUNS,
};
use crate::s3storage::S3Storage;
use anyhow::Result;
use chrono::Utc;
use cron::Schedule;
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

#[derive(Debug, Deserialize, Clone)]
pub struct CleanerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_interval_seconds")]
    pub interval_seconds: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Cron schedule for full S3 scan (phase 3). E.g. "0 3 * * *" for 3 AM daily.
    /// Accepts standard 5-field cron (min hour day month weekday).
    /// Empty or omitted = no automatic full scans.
    #[serde(default)]
    pub full_scan_cron: Option<String>,
}

fn default_interval_seconds() -> u64 {
    3600 // 1 hour
}

fn default_batch_size() -> usize {
    1000
}

fn default_concurrency() -> usize {
    8
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: default_interval_seconds(),
            batch_size: default_batch_size(),
            concurrency: default_concurrency(),
            full_scan_cron: None,
        }
    }
}

pub struct Cleaner {
    bucket_name: String,
    kvstorage: Arc<KVStorage>,
    s3storage: Arc<S3Storage>,
    locks: Arc<LocksStorage>,
    config: CleanerConfig,
}

impl Cleaner {
    pub fn new(
        bucket_name: String,
        kvstorage: Arc<KVStorage>,
        s3storage: Arc<S3Storage>,
        locks: Arc<LocksStorage>,
        mut config: CleanerConfig,
    ) -> Self {
        config.concurrency = config.concurrency.max(1);
        Self {
            bucket_name,
            kvstorage,
            s3storage,
            locks,
            config,
        }
    }

    /// Start the cleaner background tasks:
    /// - Lightweight cleanup (phases 1, 2, 4) runs on `interval_seconds`
    /// - Full S3 scan (phase 3) runs on `full_scan_cron` schedule (if configured)
    pub fn start(self: Arc<Self>) {
        if !self.config.enabled {
            info!("Cleaner disabled for bucket: {}", self.bucket_name);
            return;
        }

        info!(
            "Starting cleaner for bucket: {} with interval: {}s",
            self.bucket_name, self.config.interval_seconds
        );

        // Lightweight cleanup loop
        let lightweight = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                lightweight.config.interval_seconds,
            ));

            loop {
                interval.tick().await;
                info!(
                    "Running lightweight cleanup cycle for bucket: {}",
                    lightweight.bucket_name
                );

                if let Err(e) = lightweight.run_cleanup().await {
                    CLEANER_ERRORS_TOTAL
                        .with_label_values(&[&lightweight.bucket_name])
                        .inc();
                    error!(
                        "Cleanup cycle failed for bucket {}: {}",
                        lightweight.bucket_name, e
                    );
                }
            }
        });

        // Full S3 scan on cron schedule
        if let Some(ref cron_expr) = self.config.full_scan_cron {
            // Support standard 5-field cron (min hour day month weekday) by prepending "0 " for seconds
            let cron_with_seconds = if cron_expr.split_whitespace().count() == 5 {
                format!("0 {}", cron_expr)
            } else {
                cron_expr.clone()
            };

            let schedule = match Schedule::from_str(&cron_with_seconds) {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        "Invalid full_scan_cron expression '{}': {}. Full S3 scan disabled.",
                        cron_expr, e
                    );
                    return;
                }
            };

            if let Some(next) = schedule.upcoming(Utc).next() {
                info!(
                    "Full S3 scan scheduled for bucket: {} with cron: {} (next run: {})",
                    self.bucket_name, cron_expr, next
                );
            }

            tokio::spawn(async move {
                loop {
                    let now = Utc::now();
                    let next = match schedule.upcoming(Utc).next() {
                        Some(t) => t,
                        None => {
                            error!("Cron schedule exhausted, stopping full S3 scan task");
                            return;
                        }
                    };

                    let duration = (next - now).to_std().unwrap_or_default();
                    info!(
                        "Next full S3 scan for bucket {} at {} (in {}s)",
                        self.bucket_name,
                        next,
                        duration.as_secs()
                    );

                    tokio::time::sleep(duration).await;

                    info!(
                        "Running full cleanup cycle (with S3 scan) for bucket: {}",
                        self.bucket_name
                    );

                    if let Err(e) = self.run_full_cleanup().await {
                        CLEANER_ERRORS_TOTAL
                            .with_label_values(&[&self.bucket_name])
                            .inc();
                        error!(
                            "Full cleanup cycle failed for bucket {}: {}",
                            self.bucket_name, e
                        );
                    }
                }
            });
        }
    }

    /// Run a lightweight cleanup cycle (phases 1, 2, 4 — DB only, no S3 scan).
    /// Fast in steady state: completes in seconds when there are few orphans.
    pub async fn run_cleanup(&self) -> Result<()> {
        self.run_cleanup_inner(false).await
    }

    /// Run a full cleanup cycle including S3 object scan (all 4 phases).
    /// Phase 3 lists all S3 objects — expensive with millions of objects (15-30+ min).
    /// Scheduled automatically via `full_scan_cron` config.
    pub async fn run_full_cleanup(&self) -> Result<()> {
        self.run_cleanup_inner(true).await
    }

    async fn run_cleanup_inner(&self, include_s3_scan: bool) -> Result<()> {
        CLEANER_TOTAL_RUNS
            .with_label_values(&[&self.bucket_name])
            .inc();

        let mut total_deletes = 0;
        let mut total_bytes_freed: u64 = 0;

        info!("Phase 1: Cleaning ref_files with missing hashes");
        total_deletes += self.clean_orphaned_ref_files().await?;

        info!("Phase 2: Cleaning refcounts with no ref_files");
        total_deletes += self.clean_unreferenced_refcounts().await?;

        if include_s3_scan {
            info!("Phase 3: Cleaning S3 objects with no refcount or refcount = 0");
            let (s3_deletes, s3_bytes_freed) = self.clean_unused_s3_objects().await?;
            total_deletes += s3_deletes;
            total_bytes_freed += s3_bytes_freed;
        }

        info!("Phase 4: Cleaning logical_size entries with no refcount");
        total_deletes += self.clean_orphaned_logical_sizes().await?;

        info!(
            "Cleanup cycle complete for bucket: {} (total items deleted: {}, bytes freed: {})",
            self.bucket_name, total_deletes, total_bytes_freed
        );

        self.update_cleanup_metrics(total_deletes, total_bytes_freed);
        Ok(())
    }

    /// Update cleanup metrics after a run
    fn update_cleanup_metrics(&self, total_deletes: usize, total_bytes_freed: u64) {
        // Update timestamp (use unwrap_or to handle edge case of system time before UNIX_EPOCH)
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        CLEANER_LAST_RUN_TIMESTAMP
            .with_label_values(&[&self.bucket_name])
            .set(timestamp);

        // Update deleted blobs counter
        CLEANER_DELETED_BLOBS_TOTAL
            .with_label_values(&[&self.bucket_name])
            .inc_by(total_deletes as u64);

        // Update freed bytes counter
        CLEANER_FREED_BYTES_TOTAL
            .with_label_values(&[&self.bucket_name])
            .inc_by(total_bytes_freed);
    }

    /// Clean ref_files that point to non-existent or zero-refcount hashes.
    /// Uses cursor-based pagination with server-side JOIN filtering and concurrent processing.
    async fn clean_orphaned_ref_files(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut cursor = String::new();

        loop {
            let orphans = self
                .kvstorage
                .list_orphaned_ref_files(&self.bucket_name, &cursor, self.config.batch_size)
                .await?;

            if orphans.is_empty() {
                break;
            }

            cursor = orphans.last().unwrap().0.clone();

            let results: Vec<bool> = stream::iter(orphans)
                .map(|(path, hash)| {
                    let kvstorage = &self.kvstorage;
                    let locks = &self.locks;
                    let bucket = &self.bucket_name;
                    async move {
                        let lock_key = locks::file_lock(bucket, &path);
                        let lock = locks.prepare_lock(lock_key).await;
                        let guard = match lock.acquire_exclusive().await {
                            Ok(g) => g,
                            Err(e) => {
                                warn!(
                                    "Failed to acquire file lock for cleaner {}: {}",
                                    path, e
                                );
                                return false;
                            }
                        };

                        // Re-check under lock: verify ref_file still points to the
                        // same hash (a concurrent PUT could have overwritten this path
                        // with a different hash).
                        let current_hash =
                            match kvstorage.get_ref_file(bucket, &path).await {
                                Ok(h) => h,
                                Err(e) => {
                                    error!(
                                        "Failed to re-read ref_file for {}: {}",
                                        path, e
                                    );
                                    let _ = guard.release().await;
                                    return false;
                                }
                            };

                        if current_hash != hash {
                            debug!(
                                "ref_file for {} changed (now {}), skipping",
                                path, current_hash
                            );
                            let _ = guard.release().await;
                            return false;
                        }

                        // Re-check refcount (double-check pattern)
                        let refcount = match kvstorage.get_ref_count(bucket, &hash).await {
                            Ok(r) => r,
                            Err(e) => {
                                error!("Failed to re-check refcount for {}: {}", hash, e);
                                let _ = guard.release().await;
                                return false;
                            }
                        };

                        if refcount != 0 {
                            debug!(
                                "Refcount changed for {} (now {}), skipping",
                                hash, refcount
                            );
                            let _ = guard.release().await;
                            return false;
                        }

                        if let Err(e) = kvstorage.delete_ref_file(bucket, &path).await {
                            error!("Failed to delete ref_file {}: {}", path, e);
                            let _ = guard.release().await;
                            return false;
                        }

                        if let Err(e) = kvstorage.delete_modified(bucket, &path).await {
                            error!("Failed to delete modified entry for {}: {}", path, e);
                        }

                        let _ = guard.release().await;
                        true
                    }
                })
                .buffer_unordered(self.config.concurrency)
                .collect()
                .await;

            deleted_count += results.iter().filter(|&&r| r).count();
        }

        Ok(deleted_count)
    }

    /// Clean refcounts that have no corresponding ref_files.
    /// Uses cursor-based pagination with server-side JOIN filtering, hash locks, and concurrent processing.
    async fn clean_unreferenced_refcounts(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut cursor = String::new();

        loop {
            let orphans = self
                .kvstorage
                .list_orphaned_refcounts(&self.bucket_name, &cursor, self.config.batch_size)
                .await?;

            if orphans.is_empty() {
                break;
            }

            cursor = orphans.last().unwrap().0.clone();

            let results: Vec<bool> = stream::iter(orphans)
                .map(|(hash, count)| {
                    let kvstorage = &self.kvstorage;
                    let locks = &self.locks;
                    let bucket = &self.bucket_name;
                    async move {
                        // Acquire hash lock to prevent race with concurrent PUT
                        let lock_key = locks::hash_lock(bucket, &hash);
                        let lock = locks.prepare_lock(lock_key).await;
                        let guard = match lock.acquire_exclusive().await {
                            Ok(g) => g,
                            Err(e) => {
                                warn!("Failed to acquire hash lock for cleaner {}: {}", hash, e);
                                return false;
                            }
                        };

                        // Double-check: re-verify no ref_file points to this hash
                        let is_referenced =
                            match kvstorage.hash_is_referenced(bucket, &hash).await {
                                Ok(r) => r,
                                Err(e) => {
                                    error!(
                                        "Failed to re-check hash_is_referenced for {}: {}",
                                        hash, e
                                    );
                                    let _ = guard.release().await;
                                    return false;
                                }
                            };

                        if is_referenced {
                            debug!("Hash {} now referenced, skipping", hash);
                            let _ = guard.release().await;
                            return false;
                        }

                        debug!(
                            "Found unreferenced refcount: hash={}, count={} (no ref_files point to it)",
                            hash, count
                        );

                        if let Err(e) = kvstorage.delete_refcount(bucket, &hash).await {
                            error!("Failed to delete refcount {}: {}", hash, e);
                            let _ = guard.release().await;
                            return false;
                        }

                        let _ = guard.release().await;
                        true
                    }
                })
                .buffer_unordered(self.config.concurrency)
                .collect()
                .await;

            deleted_count += results.iter().filter(|&&r| r).count();
        }

        Ok(deleted_count)
    }

    /// Clean S3 objects that have no refcount or refcount = 0.
    /// Uses continuation-token pagination and concurrent processing with hash locks.
    /// Returns (deleted_count, bytes_freed)
    async fn clean_unused_s3_objects(&self) -> Result<(usize, u64)> {
        let mut deleted_count = 0;
        let mut bytes_freed: u64 = 0;
        let mut continuation_token: Option<String> = None;

        loop {
            let (keys, next_token) = self
                .s3storage
                .list_objects(continuation_token.clone())
                .await?;

            if keys.is_empty() {
                break;
            }

            // Process items concurrently with bounded concurrency
            let results: Vec<(bool, u64)> = stream::iter(keys)
                .map(|key| {
                    let kvstorage = &self.kvstorage;
                    let s3storage = &self.s3storage;
                    let locks = &self.locks;
                    let bucket = &self.bucket_name;
                    async move {
                        // Acquire hash lock before checking refcount and deleting
                        let hash_lock_key = locks::hash_lock(bucket, &key);
                        let hash_lock = locks.prepare_lock(hash_lock_key).await;
                        let hash_guard = match hash_lock.acquire_exclusive().await {
                            Ok(g) => g,
                            Err(e) => {
                                error!("Failed to acquire hash lock for {}: {}", key, e);
                                return (false, 0u64);
                            }
                        };

                        let refcount = match kvstorage.get_ref_count(bucket, &key).await {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to get refcount for {}: {}", key, e);
                                let _ = hash_guard.release().await;
                                return (false, 0u64);
                            }
                        };

                        if refcount != 0 {
                            let _ = hash_guard.release().await;
                            return (false, 0u64);
                        }

                        debug!("Found unused S3 object: key={} (refcount=0)", key);

                        let compressed_size = kvstorage
                            .get_compressed_size(bucket, &key)
                            .await
                            .unwrap_or(0);

                        if let Err(e) = s3storage.delete_object(&key).await {
                            error!(
                                "Failed to delete S3 object (bucket={}, key={}): {}",
                                bucket, key, e
                            );
                            let _ = hash_guard.release().await;
                            return (false, 0u64);
                        }

                        let _ = hash_guard.release().await;
                        (true, compressed_size as u64)
                    }
                })
                .buffer_unordered(self.config.concurrency)
                .collect()
                .await;

            for (deleted, freed) in results {
                if deleted {
                    deleted_count += 1;
                    bytes_freed += freed;
                }
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok((deleted_count, bytes_freed))
    }

    /// Clean logical_size entries that have no corresponding refcount.
    /// Uses cursor-based pagination with server-side JOIN filtering, hash locks, and concurrent processing.
    async fn clean_orphaned_logical_sizes(&self) -> Result<usize> {
        let mut deleted_count = 0;
        let mut cursor = String::new();

        loop {
            let orphans = self
                .kvstorage
                .list_orphaned_logical_sizes(&self.bucket_name, &cursor, self.config.batch_size)
                .await?;

            if orphans.is_empty() {
                break;
            }

            cursor = orphans.last().unwrap().clone();

            let results: Vec<bool> = stream::iter(orphans)
                .map(|hash| {
                    let kvstorage = &self.kvstorage;
                    let locks = &self.locks;
                    let bucket = &self.bucket_name;
                    async move {
                        // Acquire hash lock to prevent race with concurrent PUT
                        let lock_key = locks::hash_lock(bucket, &hash);
                        let lock = locks.prepare_lock(lock_key).await;
                        let guard = match lock.acquire_exclusive().await {
                            Ok(g) => g,
                            Err(e) => {
                                warn!(
                                    "Failed to acquire hash lock for cleaner {}: {}",
                                    hash, e
                                );
                                return false;
                            }
                        };

                        // Double-check refcount under lock
                        let refcount = match kvstorage.get_ref_count(bucket, &hash).await {
                            Ok(r) => r,
                            Err(e) => {
                                error!("Failed to re-check refcount for {}: {}", hash, e);
                                let _ = guard.release().await;
                                return false;
                            }
                        };

                        if refcount != 0 {
                            debug!(
                                "Refcount changed for {} (now {}), skipping logical_size delete",
                                hash, refcount
                            );
                            let _ = guard.release().await;
                            return false;
                        }

                        debug!("Found orphaned logical_size: hash={} (refcount=0)", hash);

                        if let Err(e) = kvstorage.delete_logical_size(bucket, &hash).await {
                            error!("Failed to delete logical_size {}: {}", hash, e);
                            let _ = guard.release().await;
                            return false;
                        }

                        let _ = guard.release().await;
                        true
                    }
                })
                .buffer_unordered(self.config.concurrency)
                .collect()
                .await;

            deleted_count += results.iter().filter(|&&r| r).count();
        }

        Ok(deleted_count)
    }
}
