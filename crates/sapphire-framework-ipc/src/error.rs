use thiserror::Error;

use crate::message::RpcError;

/// Errors surfaced by the IPC layer.
#[derive(Debug, Error)]
pub enum Error {
    /// Socket, pipe or file-system failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A frame was not valid JSON, or did not deserialise into the expected shape.
    #[error("malformed frame: {0}")]
    Codec(#[from] serde_json::Error),

    /// A frame was syntactically valid but not a legal message here (for example a
    /// response to an id that was never sent, or a string request id).
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A frame exceeded [`MAX_FRAME_LEN`](crate::MAX_FRAME_LEN).
    #[error("frame of {len} bytes exceeds the {max} byte limit")]
    FrameTooLarge {
        /// Length that was announced or read.
        len: usize,
        /// The configured limit.
        max: usize,
    },

    /// The two ends do not speak the same protocol version.
    #[error("protocol version mismatch: this process speaks {ours}, the peer speaks {theirs}")]
    VersionMismatch {
        /// This process's version.
        ours: u32,
        /// The peer's version.
        theirs: u32,
    },

    /// The peer is not the same OS user, and was disconnected.
    #[error("rejected a connection from another user")]
    PeerRejected,

    /// The connection closed before the operation finished.
    #[error("connection closed")]
    Closed,

    /// The peer answered the call with a JSON-RPC error.
    #[error("remote error {}: {}", .0.code, .0.message)]
    Rpc(RpcError),

    /// A server did not appear, or did not answer, within the time allowed.
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),

    /// The server could not be started.
    #[error("could not start the server: {0}")]
    Spawn(String),

    /// A running server speaks a different version and cannot be replaced because it is
    /// managed by the OS service manager.
    #[error(
        "the installed service is version {running}, this process is version {ours}; \
         restart the service"
    )]
    ServiceVersionMismatch {
        /// Version reported by the running server.
        running: String,
        /// This process's version.
        ours: String,
    },
}

/// Convenience alias for IPC results.
pub type Result<T> = std::result::Result<T, Error>;
