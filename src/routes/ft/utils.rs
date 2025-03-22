use std::error::Error;
use chrono::DateTime;

pub fn conv_rfc2822_to_unix_timestamp(rfc2822: &str) -> Result<i64, Box<dyn Error>> {
    let dt = DateTime::parse_from_rfc2822(rfc2822)?;
    Ok(dt.timestamp())
}