//! The `ChangeSource` abstraction: a uniform pull/push/snapshot interface over
//! *document-level* synchronisation, independent of transport.
//!
//! Two implementations exist (see `docs/ARCHITECTURE.md`):
//!
//! - [`GitChangeSource`] (this crate, `git` feature) — derives changes from the
//!   working tree of a git repository and pushes by writing files + running a
//!   git sync cycle.
//! - `RemoteChangeSource` (`sapphire-framework-remote-client`) — talks JSON-RPC
//!   to a remote server.
//!
//! Both move only document bodies + metadata (+ blob references); vector
//! indexes are rebuilt locally and never travel over a `ChangeSource`.
//!
//! The trait is `async` so the same abstraction fits the network client and a
//! future WASM frontend. The git implementation does synchronous filesystem
//! work inside its async methods; callers that must not block a runtime worker
//! can wrap calls in `spawn_blocking`.

use async_trait::async_trait;

pub use sapphire_rpc::{
    Change, ChangeKind, ChangesPullResult as PullBatch, ChangesPushResult as PushOutcome, Cursor,
    SnapshotResult as Snapshot,
};

use crate::Result;

/// A document-level, transport-agnostic sync source.
#[async_trait]
pub trait ChangeSource: Send + Sync {
    /// The full live document set plus the cursor it corresponds to.
    async fn snapshot(&self) -> Result<Snapshot>;

    /// Changes with `seq` strictly greater than `since`, up to `limit`.
    async fn pull(&self, since: Cursor, limit: usize) -> Result<PullBatch>;

    /// Apply `changes` on top of `base`. The returned [`PushOutcome::conflicts`]
    /// lists paths the source rejected because it had a newer version.
    async fn push(&self, base: Cursor, changes: Vec<Change>) -> Result<PushOutcome>;
}

#[cfg(feature = "git")]
pub use git_source::GitChangeSource;

#[cfg(feature = "git")]
mod git_source {
    use std::path::{Path, PathBuf};

    use chrono::{DateTime, TimeZone, Utc};

    use super::*;
    use crate::{Error, GitSync, SyncBackend};

    /// A [`ChangeSource`] backed by a git working tree.
    ///
    /// Change granularity is per-file at the resolution of filesystem mtime:
    /// the cursor is the highest mtime (in whole seconds) observed across the
    /// tree, and [`pull`](ChangeSource::pull) returns every file whose mtime is
    /// newer than `since`. This is coarse compared to the remote server's
    /// per-edit change log, but it is consistent with the last-writer-wins
    /// merge git already performs. A future refinement could walk commit
    /// history instead.
    ///
    /// Only files that decode as UTF-8 text are surfaced as [`Change`]s; binary
    /// files are left to the blob layer and skipped here.
    pub struct GitChangeSource {
        root: PathBuf,
        git: GitSync,
    }

    impl GitChangeSource {
        /// Create a change source over the git working tree rooted at `root`.
        ///
        /// `root` should be the workspace root (the directory whose files are
        /// synced), which is discovered as / inside a git repository.
        pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
            let root = root.into();
            let git = GitSync::open(&root)?;
            Ok(Self { root, git })
        }

        /// Create a change source using an already-configured [`GitSync`]
        /// (e.g. one carrying a device id or a non-default remote).
        pub fn with_git(root: impl Into<PathBuf>, git: GitSync) -> Self {
            Self {
                root: root.into(),
                git,
            }
        }

        /// The workspace root this source walks.
        pub fn root(&self) -> &Path {
            &self.root
        }

        /// Walk the tree, returning `(relative_path, mtime_secs, body)` for
        /// every UTF-8 text file. Hidden directories (name starting with `.`)
        /// are skipped at any depth, matching the workspace indexer.
        fn scan(&self) -> Result<Vec<(String, i64, String)>> {
            let mut out = Vec::new();
            for entry in walkdir::WalkDir::new(&self.root)
                .follow_links(false)
                .into_iter()
                .filter_entry(|e| {
                    // Skip hidden directories at any depth *below* the root, but
                    // never prune the root itself (its own name may legitimately
                    // start with '.', e.g. a temp dir).
                    if e.file_type().is_dir() && e.depth() > 0 {
                        !e.file_name().to_string_lossy().starts_with('.')
                    } else {
                        true
                    }
                })
            {
                let entry = entry.map_err(|e| Error::Io {
                    path: self.root.clone(),
                    source: e.into(),
                })?;
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                // Skip binary / non-UTF-8 files — those belong to the blob layer.
                let Ok(body) = std::fs::read_to_string(path) else {
                    continue;
                };
                let rel = match path.strip_prefix(&self.root) {
                    Ok(r) => rel_to_posix(r),
                    Err(_) => continue,
                };
                out.push((rel, mtime_secs(path), body));
            }
            // Deterministic order: ascending mtime, then path.
            out.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            Ok(out)
        }

        fn change_for(rel: String, mtime: i64, body: String) -> Change {
            Change::upsert(rel, body, secs_to_utc(mtime))
        }
    }

    #[async_trait]
    impl ChangeSource for GitChangeSource {
        async fn snapshot(&self) -> Result<Snapshot> {
            let files = self.scan()?;
            let cursor = files.iter().map(|(_, m, _)| *m as u64).max().unwrap_or(0);
            let docs = files
                .into_iter()
                .map(|(rel, m, body)| Self::change_for(rel, m, body))
                .collect();
            Ok(Snapshot { cursor, docs })
        }

        async fn pull(&self, since: Cursor, limit: usize) -> Result<PullBatch> {
            let files = self.scan()?;
            // Files strictly newer than `since` (by whole-second mtime).
            let mut fresh: Vec<Change> = files
                .into_iter()
                .filter(|(_, m, _)| (*m as u64) > since)
                .map(|(rel, m, body)| Self::change_for(rel, m, body))
                .collect();
            let more = fresh.len() > limit;
            fresh.truncate(limit);
            let cursor = fresh
                .iter()
                .map(|c| c.updated_at.timestamp() as u64)
                .max()
                .unwrap_or(since);
            Ok(PullBatch {
                cursor,
                changes: fresh,
                more,
            })
        }

        async fn push(&self, _base: Cursor, changes: Vec<Change>) -> Result<PushOutcome> {
            for change in &changes {
                let abs = self.root.join(posix_to_native(&change.path));
                match &change.kind {
                    ChangeKind::Upsert { body, .. } => {
                        if let Some(parent) = abs.parent() {
                            std::fs::create_dir_all(parent).map_err(|source| Error::Io {
                                path: parent.to_owned(),
                                source,
                            })?;
                        }
                        std::fs::write(&abs, body).map_err(|source| Error::Io {
                            path: abs.clone(),
                            source,
                        })?;
                        self.git.add_file(&abs)?;
                    }
                    ChangeKind::Delete => {
                        match std::fs::remove_file(&abs) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(source) => {
                                return Err(Error::Io {
                                    path: abs.clone(),
                                    source,
                                });
                            }
                        }
                        // Ignore "not staged" style errors on removal.
                        let _ = self.git.remove_file(&abs);
                    }
                }
            }
            // Commit + fetch/merge/push. Git's own LWW merge resolves conflicts,
            // so we report none at this layer.
            self.git.sync()?;
            let cursor = changes
                .iter()
                .map(|c| c.updated_at.timestamp() as u64)
                .max()
                .unwrap_or(0);
            Ok(PushOutcome {
                cursor,
                conflicts: Vec::new(),
            })
        }
    }

    fn mtime_secs(path: &Path) -> i64 {
        path.metadata()
            .and_then(|m| m.modified())
            .map(|t| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64
            })
            .unwrap_or(0)
    }

    fn secs_to_utc(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).single().unwrap_or_else(Utc::now)
    }

    /// Convert a workspace-relative path to POSIX (`/`) separators for the wire.
    fn rel_to_posix(rel: &Path) -> String {
        rel.components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Convert a POSIX wire path back to a native relative path.
    fn posix_to_native(path: &str) -> PathBuf {
        path.split('/').collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn init_repo() -> tempfile::TempDir {
            let tmp = tempfile::tempdir().unwrap();
            let repo = git2::Repository::init(tmp.path()).unwrap();
            // A signature is needed for the first commit in some environments.
            let mut cfg = repo.config().unwrap();
            let _ = cfg.set_str("user.name", "test");
            let _ = cfg.set_str("user.email", "test@example.com");
            tmp
        }

        #[tokio::test]
        async fn snapshot_lists_text_files_only() {
            let tmp = init_repo();
            std::fs::write(tmp.path().join("a.md"), "alpha").unwrap();
            std::fs::write(tmp.path().join("b.txt"), "beta").unwrap();
            // Binary (invalid UTF-8) file must be skipped.
            std::fs::write(tmp.path().join("c.bin"), [0xff, 0xfe, 0x00]).unwrap();

            let src = GitChangeSource::open(tmp.path()).unwrap();
            let snap = src.snapshot().await.unwrap();

            let paths: Vec<&str> = snap.docs.iter().map(|c| c.path.as_str()).collect();
            assert!(paths.contains(&"a.md"));
            assert!(paths.contains(&"b.txt"));
            assert!(!paths.contains(&"c.bin"), "binary file must be skipped");
        }

        #[tokio::test]
        async fn push_writes_files_into_tree() {
            let tmp = init_repo();
            let src = GitChangeSource::open(tmp.path()).unwrap();

            let change = Change::upsert("sub/new.md", "written", Utc::now());
            // No remote configured, so `git.sync()` will attempt a fetch and
            // fail; the file write itself is what we assert. Push may return an
            // error from the remote step — accept either but require the file.
            let _ = src.push(0, vec![change]).await;
            assert_eq!(
                std::fs::read_to_string(tmp.path().join("sub").join("new.md")).unwrap(),
                "written"
            );
        }

        #[test]
        fn posix_roundtrip() {
            let native = posix_to_native("a/b/c.md");
            assert_eq!(rel_to_posix(&native), "a/b/c.md");
        }
    }
}
