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

    /// A workspace's sync identity could not be read or written.
    ///
    /// Never repaired by minting a new identity: the workspace may already exist on
    /// another device, and a second identity would make the two sync as unrelated
    /// workspaces for ever.
    #[error("{0}")]
    SyncId(String),

    /// The replication core failed.
    ///
    /// Carries its error's `Display`, not the error: the core's own type is an internal
    /// detail, while the message is what status and the logs need.
    #[error("replication failed: {0}")]
    Sync(String),

    /// The bridge refused a request, or could not be reached.
    ///
    /// Not fatal to the app server: the bridge carries replication, and everything the
    /// application itself does works without it (spec §10).
    #[error("the bridge could not be reached: {0}")]
    Bridge(String),

    /// A replication session failed.
    #[error(transparent)]
    Session(#[from] sapphire_framework_session::Error),

    /// Privilege separation could not be set up.
    ///
    /// Always fatal: a server that meant to drop privileges and did not must never go on to
    /// serve requests.
    #[error("privilege separation failed: {0}")]
    Privilege(String),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;
