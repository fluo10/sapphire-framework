use thiserror::Error;

/// Errors returned by the remote sync client.
#[derive(Debug, Error)]
pub enum Error {
    /// The HTTP transport failed (connection, status, timeout).
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    /// The server returned a JSON-RPC error object.
    #[error("remote error {code}: {message}")]
    Rpc {
        /// JSON-RPC error code (see `sapphire_rpc::error_codes`).
        code: i64,
        /// Human-readable message from the server.
        message: String,
    },

    /// A request/response body could not be (de)serialized.
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// A base64 blob payload could not be decoded.
    #[error("decode error: {0}")]
    Decode(String),
}

/// Convenience alias for client results.
pub type Result<T> = std::result::Result<T, Error>;
