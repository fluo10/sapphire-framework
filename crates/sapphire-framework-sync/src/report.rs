//! What a scan, an apply or a fetch did.

use crate::entry::Entry;

/// Why a path was not synced on this replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// Excluded by the built-in rule or `.sapphireignore`.
    Ignored,
    TooLarge {
        len: u64,
        max: u64,
    },
    /// The local OS cannot represent the path.
    Unrepresentable,
    /// Another path differing only in case is already on disk.
    CaseCollision {
        other: String,
    },
    Symlink,
    /// Not available locally and the content source could not provide the bytes.
    ContentUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skipped {
    pub path: String,
    pub reason: SkipReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub path: String,
    pub copy_path: String,
}

/// Why a replica refuses to scan or apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseReason {
    RootMissing,
    MarkerMissing,
}

/// Effects of one `scan`, `apply` or `fetch_missing` call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Local writes recorded, including conflict copies.
    pub recorded: Vec<Entry>,
    /// Number of path-state changes committed.
    pub changed: usize,
    pub conflicts: Vec<Conflict>,
    pub skipped: Vec<Skipped>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScanOutcome {
    Scanned(Report),
    Paused(PauseReason),
}
