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

    /// The track (mtime change-detection) store failed.
    #[error("track error: {0}")]
    Track(#[from] sapphire_track::Error),

    /// (De)serialisation failed.
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// A base64 body could not be decoded.
    #[error("invalid base64 payload: {0}")]
    Base64(String),

    /// 同期対象として受け付けられないパス（隠しファイル・`..`・絶対パス）。
    #[error("path is not syncable: {0}")]
    NotSyncable(String),

    /// The API key file failed to parse, serialize, or generate a key.
    #[error("key file error: {0}")]
    KeyFile(String),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Map an internal error to a JSON-RPC error object. 受け付けられないパスは
    /// クライアント側の誤りなので `INVALID_PARAMS`、それ以外は内部エラー。
    pub fn to_jsonrpc(&self) -> JsonRpcError {
        let code = match self {
            Error::NotSyncable(_) => error_codes::INVALID_PARAMS,
            _ => error_codes::INTERNAL_ERROR,
        };
        JsonRpcError::new(code, self.to_string())
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
