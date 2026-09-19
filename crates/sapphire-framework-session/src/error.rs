//! Error type for the replication session.

/// Errors raised by a session.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Reading or writing the stream failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A control frame's JSON did not decode, or a message did not encode.
    #[error("serialization error: {0}")]
    Codec(#[from] serde_json::Error),
    /// The peer sent something the framing or the session does not allow.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A frame announced more payload than [`crate::MAX_FRAME_LEN`].
    #[error("frame of {len} bytes exceeds the {max}-byte limit")]
    FrameTooLarge {
        /// Length the frame announced, in bytes.
        len: usize,
        /// Length this build accepts, in bytes.
        max: usize,
    },
    /// The peer speaks a session format this build does not.
    #[error("session format {theirs} is not the supported {ours}")]
    VersionMismatch {
        /// The format this build speaks.
        ours: u32,
        /// The format the peer announced.
        theirs: u32,
    },
    /// The replication core failed; the message is its error's `Display`.
    #[error("replication error: {0}")]
    Sync(String),
    /// The peer said it would not continue, and why.
    #[error("the peer refused the session: {0}")]
    Refused(String),
}

/// Result alias for a session.
pub type Result<T> = std::result::Result<T, Error>;
