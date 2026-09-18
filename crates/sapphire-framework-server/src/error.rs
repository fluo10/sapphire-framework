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

    /// Privilege separation could not be set up.
    ///
    /// Always fatal: a server that meant to drop privileges and did not must never go on to
    /// serve requests.
    #[error("privilege separation failed: {0}")]
    Privilege(String),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;
