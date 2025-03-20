use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct RowRefcount {
    pub bucket: String,
    pub hash: String,
    pub refcount: i32,
}

#[derive(Debug, FromRow)]
pub struct RowModified {
    pub bucket: String,
    pub path: String,
    pub modified: i64,
}

#[derive(Debug, FromRow)]
pub struct RowRefFile {
    pub bucket: String,
    pub path: String,
    pub hash: String,
}