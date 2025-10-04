use crate::{AppState, metrics};
use axum::Json;
use axum::extract::State;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, serde::Serialize)]
pub struct VersionResponse {
    protocol_versions: Vec<i32>,
}

pub async fn ft_version(State(_state): State<Arc<AppState>>) -> Json<VersionResponse> {
    let start = Instant::now();

    metrics::HTTP_REQUESTS_TOTAL
        .with_label_values(&["GET", "/ft/version", "200"])
        .inc();
    metrics::HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&["GET", "/ft/version"])
        .observe(start.elapsed().as_secs_f64());

    Json(VersionResponse {
        protocol_versions: vec![2],
    })
}
