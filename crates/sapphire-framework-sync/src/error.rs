//! Error type for the replication core.

use crate::report::PauseReason;

/// Errors raised by the replication core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("replica store error: {0}")]
    Store(#[from] redb::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ignore file error: {0}")]
    Ignore(#[from] ignore::Error),
    #[error("replica store format {found} is newer than this build supports ({supported})")]
    FormatTooNew { found: u32, supported: u32 },
    #[error("replica store belongs to root {stored:?}, not {requested:?}")]
    RootMismatch { stored: String, requested: String },
    #[error("replica store is corrupt: {0}")]
    Corrupt(String),
    #[error("replica is paused: {0:?}")]
    Paused(PauseReason),
    #[cfg(any(test, feature = "test-util"))]
    #[error("injected fault")]
    InjectedFault,
}

/// Result alias for the replication core.
pub type Result<T> = std::result::Result<T, Error>;

/// Converts redb's per-operation error types into [`Error::Store`].
pub(crate) trait RedbExt<T> {
    fn db(self) -> Result<T>;
}

impl<T, E: Into<redb::Error>> RedbExt<T> for std::result::Result<T, E> {
    fn db(self) -> Result<T> {
        self.map_err(|e| Error::Store(e.into()))
    }
}
