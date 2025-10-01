pub mod delete_file;
pub mod get_file;
pub mod list_files;
pub mod put_file;
pub mod storage_helpers;
mod utils;
pub mod version;

#[derive(Debug, serde::Deserialize)]
pub struct LastModifiedQuery {
    last_modified: String,
}
