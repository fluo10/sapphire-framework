//! Per-workspace server state: file **origin** + redb **retrieve cache** +
//! **change log** + **blob store** (Model B — the server mirrors a client).
//!
//! All methods here are synchronous (redb / tantivy / filesystem). The axum
//! layer wraps calls in `spawn_blocking`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sapphire_blob::{BlobStore, FsBlobStore};
use sapphire_retrieve::{Document, FtsQuery, RetrieveStore, open_redb};
use sapphire_rpc::{
    BlobRef, Change, ChangeKind, ChangesPullResult, ChangesPushResult, Cursor, Hit, SnapshotResult,
};

use crate::change_log::ChangeLog;
use crate::error::{Error, Result};

/// Storage for a single workspace on the server.
pub struct WsStore {
    origin_dir: PathBuf,
    retrieve: Arc<dyn RetrieveStore + Send + Sync>,
    change_log: ChangeLog,
    blobs: FsBlobStore,
}

impl WsStore {
    /// Open (creating as needed) the four stores for one workspace under
    /// `base_dir`, namespaced by `ws`.
    pub fn open(base_dir: &Path, ws: &str) -> Result<Self> {
        let safe = sanitize(ws);
        let origin_dir = base_dir.join("origin").join(&safe);
        std::fs::create_dir_all(&origin_dir)?;
        let retrieve = open_redb(&base_dir.join("cache").join(format!("{safe}.redb")))?;
        let change_log = ChangeLog::open(&base_dir.join("changelog").join(format!("{safe}.redb")))?;
        let blobs = FsBlobStore::open(base_dir.join("blobs").join(&safe))?;
        Ok(Self {
            origin_dir,
            retrieve,
            change_log,
            blobs,
        })
    }

    // ── sync methods ────────────────────────────────────────────────────────

    /// Current live document set (tombstones folded out) plus the cursor.
    pub fn snapshot(&self) -> Result<SnapshotResult> {
        let cursor = self.change_log.max_seq()?;
        let mut docs: Vec<Change> = self
            .change_log
            .latest_per_path()?
            .into_values()
            .filter(|c| matches!(c.kind, ChangeKind::Upsert { .. }))
            .collect();
        docs.sort_by(|a, b| a.seq.cmp(&b.seq));
        Ok(SnapshotResult { cursor, docs })
    }

    /// Changes newer than `since`, capped at `limit`.
    pub fn pull(&self, since: Cursor, limit: usize) -> Result<ChangesPullResult> {
        let (changes, more) = self.change_log.since(since, limit)?;
        let cursor = changes.last().map(|c| c.seq).unwrap_or(since);
        Ok(ChangesPullResult {
            cursor,
            changes,
            more,
        })
    }

    /// Apply `changes` on top of `base_cursor`, last-writer-wins by
    /// `updated_at`. Paths for which the server holds a newer concurrent edit
    /// are rejected and reported in [`ChangesPushResult::conflicts`].
    pub fn push(&self, base_cursor: Cursor, changes: Vec<Change>) -> Result<ChangesPushResult> {
        // Snapshot of the server's latest change per path, used for conflict
        // detection. Updated in-place as we accept changes so two incoming
        // edits to the same path within one batch behave sensibly.
        let mut latest = self.change_log.latest_per_path()?;
        let mut conflicts = Vec::new();
        let mut applied = false;

        for change in changes {
            if let Some(existing) = latest.get(&change.path) {
                // The server moved ahead of what the client had seen…
                let concurrent = existing.seq > base_cursor;
                // …and the client's edit is not strictly newer → reject.
                if concurrent && change.updated_at <= existing.updated_at {
                    conflicts.push(change.path.clone());
                    continue;
                }
            }

            self.apply_one(&change)?;
            let stored = self.change_log.append(change)?;
            latest.insert(stored.path.clone(), stored);
            applied = true;
        }

        // A single FTS rebuild after the batch (upsert_document leaves the
        // inverted index stale until rebuilt).
        if applied {
            self.retrieve.rebuild_fts()?;
        }

        Ok(ChangesPushResult {
            cursor: self.change_log.max_seq()?,
            conflicts,
        })
    }

    /// Write one change through to the origin file and the retrieve cache.
    /// Does **not** rebuild FTS (the caller batches that) nor append to the log.
    fn apply_one(&self, change: &Change) -> Result<()> {
        let abs = self.origin_dir.join(posix_to_native(&change.path));
        match &change.kind {
            ChangeKind::Upsert { body, .. } => {
                if let Some(parent) = abs.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&abs, body)?;
                self.retrieve.upsert_document(&Document {
                    id: path_to_doc_id(&change.path),
                    body: body.clone(),
                    path: change.path.clone(),
                    chunks: None,
                })?;
            }
            ChangeKind::Delete => {
                match std::fs::remove_file(&abs) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(Error::Io(e)),
                }
                self.retrieve.remove_document(path_to_doc_id(&change.path))?;
            }
        }
        Ok(())
    }

    /// Store a blob, returning its content-addressed reference.
    pub fn blob_put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let r = self.blobs.put(bytes)?;
        Ok(BlobRef {
            hash: r.hash,
            len: r.len,
        })
    }

    /// Fetch a blob by hash.
    pub fn blob_get(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.blobs.get(hash)?)
    }

    /// Full-text search over the retrieve cache.
    pub fn search_fts(&self, q: &str, limit: usize) -> Result<Vec<Hit>> {
        let query = FtsQuery::new(q).limit(limit);
        let results = self.retrieve.search_fts(&query)?;
        Ok(results
            .into_iter()
            .map(|r| Hit {
                path: r.path,
                score: r.score,
                snippet: r.chunks.into_iter().next().map(|c| c.text),
            })
            .collect())
    }
}

/// FNV-1a hash of the (workspace-relative) path — the stable document id used
/// by the retrieve cache. Mirrors `sapphire_workspace::path_to_doc_id` so the
/// server and the workspace layer agree on identity.
fn path_to_doc_id(path: &str) -> i64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for b in path.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h as i64
}

/// Convert a POSIX wire path to a native relative path.
fn posix_to_native(path: &str) -> PathBuf {
    path.split('/').collect()
}

/// Make a workspace id safe to use as a single path component (no separators,
/// no traversal). Non-alphanumeric characters become `_`.
fn sanitize(ws: &str) -> String {
    let cleaned: String = ws
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect();
    if cleaned.is_empty() {
        "default".to_owned()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn store() -> (tempfile::TempDir, WsStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = WsStore::open(tmp.path(), "ws1").unwrap();
        (tmp, store)
    }

    #[test]
    fn push_then_pull_roundtrips() {
        let (_t, store) = store();
        let out = store
            .push(0, vec![Change::upsert("a.md", "hello", Utc::now())])
            .unwrap();
        assert_eq!(out.cursor, 1);
        assert!(out.conflicts.is_empty());

        let pulled = store.pull(0, 10).unwrap();
        assert_eq!(pulled.changes.len(), 1);
        assert_eq!(pulled.changes[0].path, "a.md");
        assert_eq!(pulled.cursor, 1);
    }

    #[test]
    fn push_writes_origin_file() {
        let (_t, store) = store();
        store
            .push(0, vec![Change::upsert("sub/b.md", "body", Utc::now())])
            .unwrap();
        let path = store.origin_dir.join("sub").join("b.md");
        assert_eq!(std::fs::read_to_string(path).unwrap(), "body");
    }

    #[test]
    fn concurrent_older_edit_conflicts() {
        let (_t, store) = store();
        let t0 = Utc::now();
        // Server accepts a newer edit at seq 1.
        store
            .push(0, vec![Change::upsert("a.md", "server", t0 + chrono::Duration::seconds(10))])
            .unwrap();
        // Client pushes an OLDER edit with base_cursor 0 (unaware of seq 1).
        let out = store
            .push(0, vec![Change::upsert("a.md", "client-stale", t0)])
            .unwrap();
        assert_eq!(out.conflicts, vec!["a.md".to_owned()]);
        // Server content unchanged.
        let snap = store.snapshot().unwrap();
        match &snap.docs[0].kind {
            ChangeKind::Upsert { body, .. } => assert_eq!(body, "server"),
            _ => panic!(),
        }
    }

    #[test]
    fn newer_concurrent_edit_wins() {
        let (_t, store) = store();
        let t0 = Utc::now();
        store.push(0, vec![Change::upsert("a.md", "server", t0)]).unwrap();
        let out = store
            .push(0, vec![Change::upsert("a.md", "client-newer", t0 + chrono::Duration::seconds(5))])
            .unwrap();
        assert!(out.conflicts.is_empty());
        let snap = store.snapshot().unwrap();
        match &snap.docs[0].kind {
            ChangeKind::Upsert { body, .. } => assert_eq!(body, "client-newer"),
            _ => panic!(),
        }
    }

    #[test]
    fn search_finds_pushed_document() {
        let (_t, store) = store();
        store
            .push(0, vec![Change::upsert("note.md", "the quick brown fox", Utc::now())])
            .unwrap();
        let hits = store.search_fts("quick", 10).unwrap();
        assert!(hits.iter().any(|h| h.path == "note.md"), "got {hits:?}");
    }

    #[test]
    fn snapshot_folds_out_tombstones() {
        let (_t, store) = store();
        store.push(0, vec![Change::upsert("a.md", "x", Utc::now())]).unwrap();
        store.push(1, vec![Change::delete("a.md", Utc::now())]).unwrap();
        let snap = store.snapshot().unwrap();
        assert!(snap.docs.is_empty(), "deleted doc must not appear in snapshot");
    }

    #[test]
    fn blob_roundtrip() {
        let (_t, store) = store();
        let r = store.blob_put(b"binary").unwrap();
        assert_eq!(store.blob_get(&r.hash).unwrap().as_deref(), Some(&b"binary"[..]));
    }
}
