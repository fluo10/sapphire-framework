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

    /// クライアントが名乗った change log の世代がサーバの現在値と食い違う。
    /// サーバ側で log が作り直されて `seq` が巻き戻っている状態なので、
    /// クライアントは `workspace.snapshot` から取り直す必要がある。
    #[error("change log generation is {actual}, client claimed {claimed}; re-snapshot")]
    GenerationMismatch {
        /// サーバの現在の世代。
        actual: uuid::Uuid,
        /// クライアントが名乗った世代。
        claimed: uuid::Uuid,
    },

    /// The key file failed to parse or save, a key could not be generated, or
    /// a revoke selector did not resolve to exactly one key.
    #[error("key file error: {0}")]
    KeyFile(String),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Map an internal error to a JSON-RPC error object. 受け付けられないパスと
    /// blob アドレスとして成立しないハッシュはクライアント側の誤りなので
    /// `INVALID_PARAMS`、それ以外は内部エラー。
    pub fn to_jsonrpc(&self) -> JsonRpcError {
        let code = match self {
            Error::NotSyncable(_) => error_codes::INVALID_PARAMS,
            Error::Blob(sapphire_blob::Error::InvalidHash { .. }) => error_codes::INVALID_PARAMS,
            Error::GenerationMismatch { .. } => error_codes::GENERATION_MISMATCH,
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
