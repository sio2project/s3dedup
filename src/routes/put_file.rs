use std::sync::Arc;
use axum::extract::State;
use axum::Json;
use crate::AppState;

struct PutFileRequest {
    path: String,
}

struct PutFileResponse {
    success: bool,
}

pub fn put_file(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PutFileRequest>,
) -> Json<PutFileResponse> {
    Json(PutFileResponse {
        success: true,
    })
}