use std::sync::Arc;
use axum::extract::State;
use axum::Json;
use crate::AppState;

#[derive(Debug, serde::Serialize)]
pub struct VersionResponse {
    protocol_versions: Vec<i32>,
}

pub async fn ft_version(
    State(_state): State<Arc<AppState>>
) -> Json<VersionResponse> {
    Json(VersionResponse {
        protocol_versions: vec![2],
    })
}