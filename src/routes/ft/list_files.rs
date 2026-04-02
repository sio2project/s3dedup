use crate::AppState;
use crate::routes::ft::MetricsRecorder;
use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, error};

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default = "default_last_modified")]
    last_modified: String,
}

fn default_last_modified() -> String {
    chrono::Utc::now().to_rfc2822()
}

pub async fn ft_list_files(
    State(state): State<Arc<AppState>>,
    path: Option<Path<String>>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let record_metrics = MetricsRecorder::new("GET", "/ft/list");

    // Extract path or use empty string for root
    let path_str = path.map(|Path(p)| p).unwrap_or_default();
    let path = path_str.strip_prefix('/').unwrap_or(&path_str);

    // Parse the timestamp (optional for LIST - defaults to current time if not provided)
    let timestamp = match crate::routes::ft::utils::extract_timestamp(
        &headers,
        Some(&query.last_modified),
        false,
    ) {
        Ok(ts) => ts,
        Err(e) => {
            error!("Failed to extract timestamp: {}", e);
            // For LIST, be lenient - use current time if parsing fails
            chrono::Utc::now().timestamp()
        }
    };

    match ft_list_files_inner(&state, path, timestamp).await {
        Ok(response) => {
            let status = response.status().as_u16().to_string();
            record_metrics.record(&status);
            response
        }
        Err(e) => {
            error!("LIST {} failed: {}", path, e);
            record_metrics.record("500");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(e.to_string())
                .unwrap()
        }
    }
}

async fn ft_list_files_inner(
    state: &AppState,
    path: &str,
    timestamp: i64,
) -> Result<Response<String>> {
    debug!("Handling GET /list/{} (@{})", path, timestamp);

    // Get all files under this path prefix
    let files = state
        .kvstorage
        .list_files(&state.bucket_name, path, timestamp)
        .await
        .context("Failed to list files")?;

    // Return files as newline-separated list
    let response_body = files.join("\n");
    if !response_body.is_empty() {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(response_body + "\n")
            .unwrap())
    } else {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body("".to_string())
            .unwrap())
    }
}
