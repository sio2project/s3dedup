use crate::AppState;
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
    // Extract path or use empty string for root
    let path_str = path.map(|Path(p)| p).unwrap_or_default();
    let path = path_str.strip_prefix('/').unwrap_or(&path_str);

    debug!("LIST request for path: {}", path);
    debug!("Query params: last_modified={}", query.last_modified);
    debug!("Headers: last-modified={:?}", headers.get("last-modified"));

    // Parse the timestamp (LIST typically only uses query param, but support header too)
    let timestamp =
        match crate::routes::ft::utils::extract_timestamp(&headers, Some(&query.last_modified)) {
            Ok(ts) => ts,
            Err(e) => {
                error!("Failed to extract timestamp: {}", e);
                // For LIST, be lenient - use current time if parsing fails
                chrono::Utc::now().timestamp()
            }
        };

    debug!("Handling GET /list/{} (@{})", path, timestamp);

    // Get all files under this path prefix
    let files_result = state
        .kvstorage
        .lock()
        .await
        .list_files(&state.bucket_name, path, timestamp)
        .await;

    match files_result {
        Ok(files) => {
            // Return files as newline-separated list
            let response_body = files.join("\n");
            if !response_body.is_empty() {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(response_body + "\n")
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::OK)
                    .body("".to_string())
                    .unwrap()
            }
        }
        Err(e) => {
            error!("Failed to list files: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to list files".to_string())
                .unwrap()
        }
    }
}
