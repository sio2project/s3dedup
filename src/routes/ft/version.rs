use crate::AppState;
use axum::Json;
use axum::extract::State;
use std::sync::Arc;

#[derive(Debug, serde::Serialize)]
pub struct VersionResponse {
    protocol_versions: Vec<i32>,
}

pub async fn ft_version(State(_state): State<Arc<AppState>>) -> Json<VersionResponse> {
    Json(VersionResponse {
        protocol_versions: vec![2],
    })
}
