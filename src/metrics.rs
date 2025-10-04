use lazy_static::lazy_static;
use prometheus::{
    Encoder, Gauge, HistogramVec, IntCounterVec, IntGauge, TextEncoder, register_gauge,
    register_histogram_vec, register_int_counter_vec, register_int_gauge,
};
use serde_json::{Value, json};

lazy_static! {
    // HTTP Request metrics
    pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_http_requests_total",
        "Total number of HTTP requests",
        &["method", "endpoint", "status"]
    )
    .unwrap();

    pub static ref HTTP_REQUEST_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "s3dedup_http_request_duration_seconds",
        "HTTP request latencies in seconds",
        &["method", "endpoint"]
    )
    .unwrap();

    // Storage metrics
    pub static ref TOTAL_FILES: IntGauge = register_int_gauge!(
        "s3dedup_total_files",
        "Total number of logical files stored"
    )
    .unwrap();

    pub static ref TOTAL_BLOBS: IntGauge = register_int_gauge!(
        "s3dedup_total_blobs",
        "Total number of unique content blobs in S3"
    )
    .unwrap();

    pub static ref DEDUPLICATION_RATIO: Gauge = register_gauge!(
        "s3dedup_deduplication_ratio",
        "Deduplication effectiveness: (files - blobs) / files. 0.0 = no dedup, 1.0 = perfect dedup"
    )
    .unwrap();

    pub static ref TOTAL_LOGICAL_SIZE_BYTES: IntGauge = register_int_gauge!(
        "s3dedup_total_logical_size_bytes",
        "Sum of (refcount * logical_size) for all blobs - what storage would be without deduplication"
    )
    .unwrap();

    pub static ref TOTAL_STORAGE_BYTES: IntGauge = register_int_gauge!(
        "s3dedup_total_storage_bytes",
        "Actual storage used in S3 (sum of compressed blob sizes)"
    )
    .unwrap();

    pub static ref DEDUPLICATED_BYTES_SAVED: IntGauge = register_int_gauge!(
        "s3dedup_deduplicated_bytes_saved",
        "Bytes saved by deduplication: sum of (refcount - 1) * logical_size"
    )
    .unwrap();

    pub static ref STORAGE_SAVINGS_RATIO: Gauge = register_gauge!(
        "s3dedup_storage_savings_ratio",
        "Storage savings ratio (logical - physical) / logical"
    )
    .unwrap();

    pub static ref AVERAGE_REFERENCE_COUNT: Gauge = register_gauge!(
        "s3dedup_average_reference_count",
        "Average number of files pointing to each blob"
    )
    .unwrap();

    // Deduplication metrics
    pub static ref DEDUP_HITS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_dedup_hits_total",
        "Number of PUT requests that matched existing blobs",
        &["bucket"]
    )
    .unwrap();

    pub static ref DEDUP_MISSES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_dedup_misses_total",
        "Number of PUT requests requiring new blob storage",
        &["bucket"]
    )
    .unwrap();

    pub static ref DEDUP_HIT_RATE: Gauge = register_gauge!(
        "s3dedup_dedup_hit_rate",
        "Deduplication hit rate (hits / (hits + misses))"
    )
    .unwrap();

    // Cleaner metrics
    pub static ref CLEANER_LAST_RUN_TIMESTAMP: IntGauge = register_int_gauge!(
        "s3dedup_cleaner_last_run_timestamp_seconds",
        "Timestamp of last successful cleaner run"
    )
    .unwrap();

    pub static ref CLEANER_TOTAL_RUNS: IntCounterVec = register_int_counter_vec!(
        "s3dedup_cleaner_total_runs",
        "Total number of cleaner runs",
        &["bucket"]
    )
    .unwrap();

    pub static ref CLEANER_DELETED_BLOBS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_cleaner_deleted_blobs_total",
        "Total blobs deleted by cleaner",
        &["bucket"]
    )
    .unwrap();

    pub static ref CLEANER_FREED_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_cleaner_freed_bytes_total",
        "Total storage freed by cleaner",
        &["bucket"]
    )
    .unwrap();

    pub static ref CLEANER_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_cleaner_errors_total",
        "Total cleaner errors",
        &["bucket"]
    )
    .unwrap();

    // Migration metrics
    pub static ref MIGRATION_ACTIVE: IntGauge = register_int_gauge!(
        "s3dedup_migration_active",
        "Whether migration is currently active (1 = active, 0 = inactive)"
    )
    .unwrap();

    pub static ref MIGRATION_FILES_MIGRATED: IntCounterVec = register_int_counter_vec!(
        "s3dedup_migration_files_migrated_total",
        "Total files migrated from old filetracker",
        &["bucket"]
    )
    .unwrap();

    pub static ref FILETRACKER_FALLBACKS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "s3dedup_filetracker_fallbacks_total",
        "Number of GET requests served from old filetracker",
        &["bucket"]
    )
    .unwrap();

    // System health metrics
    pub static ref LOCK_QUEUE_SIZE: IntGauge = register_int_gauge!(
        "s3dedup_lock_queue_size",
        "Number of requests waiting for locks"
    )
    .unwrap();

    pub static ref UPTIME_SECONDS: IntGauge = register_int_gauge!(
        "s3dedup_uptime_seconds",
        "Server uptime in seconds"
    )
    .unwrap();
}

#[derive(Clone)]
pub struct Metrics {
    start_time: std::time::Instant,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
        }
    }

    /// Update uptime metric
    pub fn update_uptime(&self) {
        UPTIME_SECONDS.set(self.start_time.elapsed().as_secs() as i64);
    }

    /// Gather all metrics and return as Prometheus text format
    pub fn gather(&self) -> Result<String, Box<dyn std::error::Error>> {
        self.update_uptime();

        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    /// Gather all metrics and return as JSON
    pub fn gather_json(&self) -> Result<Value, Box<dyn std::error::Error>> {
        self.update_uptime();

        let metric_families = prometheus::gather();
        let mut metrics = json!({});

        for mf in metric_families {
            let name = mf.get_name();
            let help = mf.get_help();
            let metric_type = format!("{:?}", mf.get_field_type());

            let mut metric_values = vec![];

            for m in mf.get_metric() {
                let mut labels = json!({});
                for label in m.get_label() {
                    labels[label.get_name()] = json!(label.get_value());
                }

                let value = if m.has_counter() {
                    json!({
                        "labels": labels,
                        "value": m.get_counter().get_value(),
                    })
                } else if m.has_gauge() {
                    json!({
                        "labels": labels,
                        "value": m.get_gauge().get_value(),
                    })
                } else if m.has_histogram() {
                    json!({
                        "labels": labels,
                        "sample_count": m.get_histogram().get_sample_count(),
                        "sample_sum": m.get_histogram().get_sample_sum(),
                    })
                } else {
                    continue;
                };

                metric_values.push(value);
            }

            metrics[name] = json!({
                "help": help,
                "type": metric_type,
                "metrics": metric_values,
            });
        }

        Ok(metrics)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
