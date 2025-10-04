use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};

pub fn conv_rfc2822_to_unix_timestamp(rfc2822: &str) -> Result<i64> {
    let dt = DateTime::parse_from_rfc2822(rfc2822)?;
    Ok(dt.timestamp())
}

pub fn format_rfc2822_timestamp(timestamp: i64) -> String {
    let dt = Utc.timestamp_opt(timestamp, 0).unwrap();
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}
