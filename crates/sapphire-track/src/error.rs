use thiserror::Error;

/// Errors produced by the [`sapphire-track`](crate) crate.
///
/// The redb sub-errors are kept as distinct variants so that `?` works
/// directly on every storage operation without an intermediate conversion.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("directory walk error: {0}")]
    Walk(#[from] walkdir::Error),
    #[error("redb database error: {0}")]
    RedbDatabase(#[from] redb::DatabaseError),
    #[error("redb transaction error: {0}")]
    RedbTransaction(#[from] redb::TransactionError),
    #[error("redb table error: {0}")]
    RedbTable(#[from] redb::TableError),
    #[error("redb storage error: {0}")]
    RedbStorage(#[from] redb::StorageError),
    #[error("redb commit error: {0}")]
    RedbCommit(#[from] redb::CommitError),
}

pub type Result<T> = std::result::Result<T, Error>;
