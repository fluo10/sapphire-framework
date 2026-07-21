use sapphire_rpc::{JsonRpcError, error_codes};
use thiserror::Error;

/// Errors raised while servicing a remote sync request.
#[derive(Debug, Error)]
pub enum Error {
    /// A filesystem operation on the origin store failed.
    #[error("origin I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The change-log database failed.
    #[error("change log error: {0}")]
    Redb(String),

    /// The retrieve cache failed.
    #[error("retrieve error: {0}")]
    Retrieve(#[from] sapphire_retrieve::Error),

    /// The blob store failed.
    #[error("blob error: {0}")]
    Blob(#[from] sapphire_blob::Error),

    /// (De)serialisation failed.
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// A base64 body could not be decoded.
    #[error("invalid base64 payload: {0}")]
    Base64(String),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Map an internal error to a JSON-RPC error object (always the internal
    /// error code — parameter/auth problems are reported separately by the
    /// dispatcher).
    pub fn to_jsonrpc(&self) -> JsonRpcError {
        JsonRpcError::new(error_codes::INTERNAL_ERROR, self.to_string())
    }
}

// redb has several distinct error types; collapse them into a string so the
// server error stays simple.
impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Self {
        Error::Redb(e.to_string())
    }
}
impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Error::Redb(e.to_string())
    }
}
impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::Redb(e.to_string())
    }
}
impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::Redb(e.to_string())
    }
}
impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::Redb(e.to_string())
    }
}
impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::Redb(e.to_string())
    }
}
