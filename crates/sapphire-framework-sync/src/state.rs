//! Per-path state kept by a replica.

use serde::{Deserialize, Serialize};

use crate::entry::Entry;
use crate::hash::ContentHash;
use crate::merge;
use crate::vv::VersionVector;

/// What is on disk for a path.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskState {
    /// Hash of the file; `None` when there is no file.
    pub hash: Option<ContentHash>,
    /// Versions the file on disk reflects. Context of the next local edit.
    pub seen: VersionVector,
    /// Change-detection pre-filter: mtime (ns since the epoch) and size when recorded.
    pub mtime_ns: i64,
    pub len: u64,
    /// Wall-clock time (ns since the epoch) the stamp was taken. A file whose mtime is
    /// within two seconds of this is "racy": filesystems with coarse mtime granularity
    /// could hide a same-size edit, so it is re-hashed instead of trusted.
    pub checked_ns: i64,
}

/// A path's sibling versions, everything merged so far, and what is on disk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathState {
    pub versions: Vec<Entry>,
    pub seen: VersionVector,
    pub disk: DiskState,
}

impl PathState {
    pub fn winner(&self) -> &Entry {
        merge::winner(&self.versions)
    }
}
