//! Versions of a path and the unit of replication.

use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::hlc::Hlc;
use crate::vv::{Dot, VersionVector};

/// What a version holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Content {
    File { hash: ContentHash, len: u64 },
    Tombstone,
}

impl Content {
    pub fn hash(&self) -> Option<ContentHash> {
        match self {
            Content::File { hash, .. } => Some(*hash),
            Content::Tombstone => None,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, Content::Tombstone)
    }
}

/// One version of one path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Workspace-relative path with POSIX separators.
    pub path: String,
    pub content: Content,
    pub hlc: Hlc,
    pub dot: Dot,
    /// Versions of this path the writer's file was based on.
    pub context: VersionVector,
    /// Device id of the writer. Display and audit only; never used for merging.
    pub author: GrainId,
}

/// A path's replicated state: its sibling versions and everything merged so far.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathUpdate {
    pub path: String,
    pub versions: Vec<Entry>,
    pub seen: VersionVector,
}
