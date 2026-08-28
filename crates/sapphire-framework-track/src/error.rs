use thiserror::Error;

/// Errors produced by the [`sapphire-track`](crate) crate.
///
/// The redb sub-errors are kept as distinct variants so that `?` works
/// directly on every storage operation without an intermediate conversion.
/// Their payloads are boxed: `redb::TransactionError` alone is ~160 bytes, and
/// inlining it would bloat every `Result` this crate returns.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("directory walk error: {0}")]
    Walk(#[from] walkdir::Error),
    #[error("redb database error: {0}")]
    RedbDatabase(Box<redb::DatabaseError>),
    #[error("redb transaction error: {0}")]
    RedbTransaction(Box<redb::TransactionError>),
    #[error("redb table error: {0}")]
    RedbTable(Box<redb::TableError>),
    #[error("redb storage error: {0}")]
    RedbStorage(Box<redb::StorageError>),
    #[error("redb commit error: {0}")]
    RedbCommit(Box<redb::CommitError>),
}

macro_rules! from_boxed {
    ($source:ty => $variant:ident) => {
        impl From<$source> for Error {
            fn from(err: $source) -> Self {
                Self::$variant(Box::new(err))
            }
        }
    };
}

from_boxed!(redb::DatabaseError => RedbDatabase);
from_boxed!(redb::TransactionError => RedbTransaction);
from_boxed!(redb::TableError => RedbTable);
from_boxed!(redb::StorageError => RedbStorage);
from_boxed!(redb::CommitError => RedbCommit);

pub type Result<T> = std::result::Result<T, Error>;
