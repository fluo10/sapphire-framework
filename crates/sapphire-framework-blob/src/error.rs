use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by [`BlobStore`](crate::BlobStore) implementations.
#[derive(Debug, Error)]
pub enum Error {
    /// A filesystem operation failed.
    #[error("blob I/O error at '{path}': {source}")]
    Io {
        /// Path being operated on when the error occurred.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Stored bytes did not hash to the address they were fetched under
    /// (corruption or tampering).
    #[error("blob '{expected}' failed integrity check (got '{actual}')")]
    IntegrityMismatch {
        /// The hash the blob was requested/stored under.
        expected: String,
        /// The hash actually computed from the bytes.
        actual: String,
    },
}

/// Convenience alias for blob-store results.
pub type Result<T> = std::result::Result<T, Error>;
