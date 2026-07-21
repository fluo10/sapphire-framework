use thiserror::Error;

/// Errors surfaced by a [`WorkspaceBackend`](crate::WorkspaceBackend).
#[derive(Debug, Error)]
pub enum Error {
    /// The underlying local workspace failed.
    #[error(transparent)]
    Workspace(#[from] sapphire_workspace::Error),

    /// The remote sync client failed.
    #[error(transparent)]
    Remote(#[from] sapphire_remote_client::Error),

    /// A blocking task panicked or was cancelled.
    #[error("backend task failed: {0}")]
    Join(String),

    /// A push was rejected because the remote holds a newer, conflicting
    /// version of one or more paths. The local cache keeps the caller's edit;
    /// the next [`sync`](crate::WorkspaceBackend::sync) pulls the remote
    /// version and reconciles last-writer-wins.
    #[error("remote rejected push for {} path(s): {}", .paths.len(), .paths.join(", "))]
    Conflict {
        /// The workspace-relative paths the remote rejected.
        paths: Vec<String>,
    },

    /// The operation is not available on this backend.
    #[error("operation not supported by this backend: {0}")]
    Unsupported(&'static str),
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::Join(e.to_string())
    }
}

/// Convenience alias for backend results.
pub type Result<T> = std::result::Result<T, Error>;
