use std::sync::Arc;
use axum::extract::{Path, State};
use axum::Json;
use crate::AppState;

#[derive(Debug, serde::Serialize)]
pub struct PutFileResponse {
    success: bool,
    path: String,
}

pub async fn ft_put_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>
) -> Json<PutFileResponse> {
    Json(PutFileResponse {
        success: true,
        path,
    })
}