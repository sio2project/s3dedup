pub mod delete_file;
pub mod get_file;
pub mod head_file;
pub mod list_files;
pub mod put_file;
pub mod storage_helpers;
mod utils;
pub mod version;

#[derive(Debug, serde::Deserialize)]
pub struct LastModifiedQuery {
    #[serde(default)]
    pub last_modified: Option<String>,
}
