use crate::AppState;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;

pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.metrics.gather() {
        Ok(metrics_output) => (StatusCode::OK, metrics_output),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error gathering metrics: {}", e),
        ),
    }
}

pub async fn metrics_json_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.metrics.gather_json() {
        Ok(metrics_json) => (StatusCode::OK, Json(metrics_json)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Error gathering metrics: {}", e)
            })),
        ),
    }
}

pub async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let health_status = state.check_health().await;

    // Return 503 if any checks failed
    let status_code = if health_status.status == "ok" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(health_status))
}
