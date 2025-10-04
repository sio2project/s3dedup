use anyhow::Result;
use axum::http::HeaderMap;
use chrono::{DateTime, TimeZone, Utc};
use tracing::debug;

pub fn conv_rfc2822_to_unix_timestamp(rfc2822: &str) -> Result<i64> {
    let dt = DateTime::parse_from_rfc2822(rfc2822)?;
    Ok(dt.timestamp())
}

pub fn format_rfc2822_timestamp(timestamp: i64) -> String {
    let dt = Utc.timestamp_opt(timestamp, 0).unwrap();
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Extract timestamp from request (header or query param)
///
/// Protocol v1/v2 clients send Last-Modified header, v0 clients use query parameter.
/// If `required` is true, returns an error if neither header nor query param is provided.
/// If `required` is false, returns current time when timestamp is missing.
pub fn extract_timestamp(
    headers: &HeaderMap,
    query_param: Option<&String>,
    required: bool,
) -> Result<i64, String> {
    // Check Last-Modified header first (protocol v1/v2)
    let timestamp_str = headers
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            debug!("Using Last-Modified header: {}", s);
            s.to_string()
        })
        .or_else(|| {
            query_param.map(|s| {
                debug!("Using last_modified query param: {}", s);
                s.clone()
            })
        });

    match timestamp_str {
        Some(ts_str) => match conv_rfc2822_to_unix_timestamp(&ts_str) {
            Ok(ts) => {
                debug!("Parsed timestamp: {}", ts);
                Ok(ts)
            }
            Err(e) => Err(format!("Failed to parse timestamp '{}': {}", ts_str, e)),
        },
        None => {
            if required {
                Err("\"?last_modified=\" is required".to_string())
            } else {
                let ts = Utc::now().timestamp();
                debug!("No timestamp provided, using current time: {}", ts);
                Ok(ts)
            }
        }
    }
}
