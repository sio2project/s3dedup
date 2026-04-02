pub mod delete_file;
pub mod get_file;
pub mod head_file;
pub mod list_files;
pub mod put_file;
pub mod storage_helpers;
mod utils;
pub mod version;

use axum::body::Body;
use axum::http::{Response, StatusCode};

use crate::metrics;

#[derive(Debug, serde::Deserialize)]
pub struct LastModifiedQuery {
    #[serde(default)]
    pub last_modified: Option<String>,
}

/// Helper to record HTTP request metrics (counter + duration histogram).
pub(crate) struct MetricsRecorder {
    method: &'static str,
    path: &'static str,
    start: std::time::Instant,
}

impl MetricsRecorder {
    pub fn new(method: &'static str, path: &'static str) -> Self {
        Self {
            method,
            path,
            start: std::time::Instant::now(),
        }
    }

    pub fn record(&self, status: &str) {
        metrics::HTTP_REQUESTS_TOTAL
            .with_label_values(&[self.method, self.path, status])
            .inc();
        metrics::HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&[self.method, self.path])
            .observe(self.start.elapsed().as_secs_f64());
    }
}

/// Build a standard Filetracker file response with the common headers.
pub(crate) fn build_ft_file_response(
    body: Body,
    content_length: i64,
    logical_size: usize,
    last_modified: i64,
) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", content_length.to_string())
        .header("Content-Encoding", "gzip")
        .header(
            "Last-Modified",
            utils::format_rfc2822_timestamp(last_modified),
        )
        .header("Logical-Size", logical_size.to_string())
        .body(body)
        .unwrap()
}
