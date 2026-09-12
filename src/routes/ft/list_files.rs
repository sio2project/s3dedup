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

    // Extract path or use empty string for root. Ensure the path ends with a /,
    // except for empty paths.
    let path_str = path.map(|Path(p)| p).unwrap_or_default();
    let path_trimmed = path_str.trim_matches('/');
    let path_proper = if path_trimmed.is_empty() {
        String::new()
    } else {
        format!("{}/", path_trimmed)
    };

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

    match ft_list_files_inner(&state, &path_proper, timestamp).await {
        Ok(response) => {
            let status = response.status().as_u16().to_string();
            record_metrics.record(&status);
            response
        }
        Err(e) => {
            error!("LIST {} failed: {}", path_str, e);
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

    // Return files as newline-separated list,
    // with the path prefix stripped each file entry.
    // This works under the assumption that path ends with a /
    // unless it would just be "/".
    let response_body = files
        .iter()
        .map(|f| f.strip_prefix(path).unwrap_or(f))
        .collect::<Vec<_>>()
        .join("\n");

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
