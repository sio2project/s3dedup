pub mod put_file;
pub mod version;
mod utils;

#[derive(Debug, serde::Deserialize)]
pub struct LastModifiedQuery {
    last_modified: String,
}