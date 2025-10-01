pub mod get_file;
pub mod put_file;
pub mod version;
mod utils;
pub mod storage_helpers;

#[derive(Debug, serde::Deserialize)]
pub struct LastModifiedQuery {
    last_modified: String,
}