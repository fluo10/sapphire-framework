use thiserror::Error;

/// Errors raised while serving an application's workspaces.
#[derive(Debug, Error)]
pub enum Error {
    /// Opening or using a workspace failed.
    #[error(transparent)]
    Workspace(#[from] sapphire_workspace::Error),

    /// A backend operation failed.
    #[error(transparent)]
    Backend(#[from] sapphire_backend::Error),

    /// The IPC layer failed.
    #[error(transparent)]
    Ipc(#[from] sapphire_ipc::Error),

    /// Listening, or preparing the runtime directory, failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The request named a directory that is not a workspace of this application.
    #[error("{0} is not a {1} workspace")]
    UnknownWorkspace(std::path::PathBuf, &'static str),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;
