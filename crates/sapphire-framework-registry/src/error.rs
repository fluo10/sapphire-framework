use thiserror::Error;

/// What can go wrong reading or writing a ledger.
#[derive(Debug, Error)]
pub enum Error {
    /// A file operation failed.
    #[error("registry io error: {0}")]
    Io(#[from] std::io::Error),

    /// Parsing or storing failed, an id / name was duplicated, or a selector
    /// resolved to no entry.
    #[error("registry file error: {0}")]
    File(String),
}

pub type Result<T> = std::result::Result<T, Error>;
