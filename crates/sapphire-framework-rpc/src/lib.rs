//! Serde-only shared types for the sapphire-framework remote sync protocol.
//!
//! This crate is deliberately dependency-light (no tokio / reqwest / I/O) and
//! wasm-safe so that both the remote **client**
//! (`sapphire-framework-remote-client`) and **server**
//! (`sapphire-framework-remote-server`) can share a single definition of the
//! wire types. A future WASM journal frontend links it directly.
//!
//! ## Model
//!
//! The server keeps a per-workspace, monotonically increasing **change log**.
//! Each committed document change gets a [`Cursor`] (`seq`) one higher than the
//! last. A client tracks the highest `seq` it has applied and pulls everything
//! newer. Document identity across peers is the **workspace-relative path**;
//! conflicts are resolved last-writer-wins on [`Change::updated_at`].
//!
//! Vector indexes are **not** part of the protocol — the sync layer only moves
//! document bodies, metadata and binary blob references (see
//! `docs/ARCHITECTURE.md`).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod jsonrpc;
pub use jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, error_codes, methods};

/// Server-assigned change-log position. `0` means "before the first change";
/// a pull `since: 0` therefore returns the entire history.
pub type Cursor = u64;

/// Reference to a content-addressed binary blob stored out-of-band (see
/// [`methods::BLOB_GET`] / [`methods::BLOB_PUT`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    /// Lower-case hex SHA-256 of the blob contents.
    pub hash: String,
    /// Length of the blob in bytes.
    pub len: u64,
}

/// What happened to a document in a single [`Change`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChangeKind {
    /// The document was created or its contents replaced.
    Upsert {
        /// Full text body of the document.
        body: String,
        /// Binary attachments referenced by this document, if any.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        blobs: Vec<BlobRef>,
    },
    /// The document was removed. Retained as a tombstone so peers that were
    /// offline still learn about the deletion.
    Delete,
}

/// One entry in a workspace's change log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Change {
    /// Change-log position assigned by the server. Clients ignore this on push
    /// (the server assigns it) and treat it as authoritative on pull.
    #[serde(default)]
    pub seq: Cursor,
    /// Workspace-relative path identifying the document (POSIX separators).
    pub path: String,
    /// The mutation.
    #[serde(flatten)]
    pub kind: ChangeKind,
    /// Client-supplied wall-clock time of the edit. Used for last-writer-wins
    /// conflict resolution.
    pub updated_at: DateTime<Utc>,
}

impl Change {
    /// Convenience constructor for an upsert with no blob attachments.
    pub fn upsert(path: impl Into<String>, body: impl Into<String>, updated_at: DateTime<Utc>) -> Self {
        Self {
            seq: 0,
            path: path.into(),
            kind: ChangeKind::Upsert {
                body: body.into(),
                blobs: Vec::new(),
            },
            updated_at,
        }
    }

    /// Convenience constructor for a deletion tombstone.
    pub fn delete(path: impl Into<String>, updated_at: DateTime<Utc>) -> Self {
        Self {
            seq: 0,
            path: path.into(),
            kind: ChangeKind::Delete,
            updated_at,
        }
    }
}

/// A single search hit — the serde projection of the workspace layer's
/// `FileSearchResult` (kept independent so this crate stays dependency-light).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    /// Workspace-relative (or absolute, server-defined) path of the matching file.
    pub path: String,
    /// Representative relevance score (higher is better).
    pub score: f64,
    /// Optional excerpt around the match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

// ── method params / results ─────────────────────────────────────────────────

/// Parameters for [`methods::WORKSPACE_SNAPSHOT`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotParams {
    /// Workspace identifier.
    pub ws: String,
}

/// Result of [`methods::WORKSPACE_SNAPSHOT`]: the current live document set plus
/// the cursor it corresponds to.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResult {
    /// Highest applied change-log position.
    pub cursor: Cursor,
    /// Live documents (tombstones folded out), each as an `Upsert` change.
    pub docs: Vec<Change>,
}

/// Parameters for [`methods::CHANGES_PULL`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangesPullParams {
    /// Workspace identifier.
    pub ws: String,
    /// Return changes with `seq` strictly greater than this.
    pub since: Cursor,
    /// Maximum number of changes to return in this batch.
    pub limit: usize,
}

/// Result of [`methods::CHANGES_PULL`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangesPullResult {
    /// Highest `seq` included in `changes` (or `since` when empty).
    pub cursor: Cursor,
    /// The changes newer than the requested `since`, ascending by `seq`.
    pub changes: Vec<Change>,
    /// `true` when the server has more changes beyond this batch.
    pub more: bool,
}

/// Parameters for [`methods::CHANGES_PUSH`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangesPushParams {
    /// Workspace identifier.
    pub ws: String,
    /// The cursor the client believed it was up to date with. Used to detect
    /// that the server moved ahead for the same document.
    pub base_cursor: Cursor,
    /// Changes to apply (server assigns each a fresh `seq`).
    pub changes: Vec<Change>,
}

/// Result of [`methods::CHANGES_PUSH`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangesPushResult {
    /// Highest `seq` after applying the accepted changes.
    pub cursor: Cursor,
    /// Paths rejected because the server had a newer (or concurrent) version.
    /// The client should pull and retry these.
    #[serde(default)]
    pub conflicts: Vec<String>,
}

/// Parameters for [`methods::BLOB_GET`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobGetParams {
    /// Workspace identifier.
    pub ws: String,
    /// Lower-case hex SHA-256 of the desired blob.
    pub hash: String,
}

/// Result of [`methods::BLOB_GET`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobGetResult {
    /// Base64 (standard alphabet) of the blob, or `None` if not present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_base64: Option<String>,
}

/// Parameters for [`methods::BLOB_PUT`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobPutParams {
    /// Workspace identifier.
    pub ws: String,
    /// Base64 (standard alphabet) of the blob to store.
    pub bytes_base64: String,
}

/// Result of [`methods::BLOB_PUT`]: the content-addressed reference the server
/// assigned.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobPutResult {
    /// Lower-case hex SHA-256 of the stored blob.
    pub hash: String,
    /// Length of the stored blob in bytes.
    pub len: u64,
}

/// Parameters for [`methods::SEARCH_FTS`] and [`methods::SEARCH_SEMANTIC`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchParams {
    /// Workspace identifier.
    pub ws: String,
    /// Query string.
    pub q: String,
    /// Maximum number of hits to return.
    pub limit: usize,
}

/// Result of a search method.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    /// Matching files, best first.
    pub hits: Vec<Hit>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-20T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn roundtrip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let json = serde_json::to_string(value).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn change_upsert_roundtrips() {
        let c = Change::upsert("notes/a.md", "hello", ts());
        assert_eq!(roundtrip(&c), c);
    }

    #[test]
    fn change_delete_roundtrips() {
        let c = Change::delete("notes/gone.md", ts());
        assert_eq!(roundtrip(&c), c);
    }

    #[test]
    fn change_with_blobs_roundtrips() {
        let mut c = Change::upsert("doc.md", "body", ts());
        if let ChangeKind::Upsert { blobs, .. } = &mut c.kind {
            blobs.push(BlobRef {
                hash: "abc123".into(),
                len: 42,
            });
        }
        assert_eq!(roundtrip(&c), c);
    }

    #[test]
    fn change_kind_tag_is_flattened() {
        // The `kind` tag lives at the top level thanks to `#[serde(flatten)]`.
        let c = Change::upsert("a.md", "x", ts());
        let v: serde_json::Value = serde_json::to_value(&c).unwrap();
        assert_eq!(v["kind"], "upsert");
        assert_eq!(v["path"], "a.md");
        assert_eq!(v["body"], "x");
    }

    #[test]
    fn method_results_roundtrip() {
        let r = ChangesPullResult {
            cursor: 7,
            changes: vec![Change::upsert("a.md", "x", ts())],
            more: true,
        };
        let back: ChangesPullResult = roundtrip(&r);
        assert_eq!(back.cursor, 7);
        assert!(back.more);
        assert_eq!(back.changes.len(), 1);
    }

    #[test]
    fn hit_snippet_optional() {
        let h = Hit {
            path: "a.md".into(),
            score: 1.5,
            snippet: None,
        };
        let v = serde_json::to_value(&h).unwrap();
        assert!(v.get("snippet").is_none(), "None snippet must be omitted");
        assert_eq!(roundtrip(&h), h);
    }
}
