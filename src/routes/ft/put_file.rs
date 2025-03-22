use crate::AppState;
use axum::Json;
use axum::extract::{Path, State};
use std::sync::Arc;

#[derive(Debug, serde::Serialize)]
pub struct PutFileResponse {
    success: bool,
    path: String,
}

pub async fn ft_put_file(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> Json<PutFileResponse> {
    Json(PutFileResponse {
        success: true,
        path,
    })
}
