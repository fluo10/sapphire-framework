//! File-type-agnostic mtime-based file-change detection.
//!
//! `sapphire-track` owns the "what files changed since last time" concern,
//! independent of *why* a caller cares about those files. It persists a
//! `path -> mtime` snapshot and diffs the current filesystem state against it.
//!
//! The crate deliberately knows nothing about file types: the caller decides
//! which paths to track by supplying an `accept` predicate to [`scan`] (or by
//! feeding [`Observed`] entries directly to [`diff`]). This keeps retrieval
//! concerns (handled by `sapphire-retrieve`) separate from change detection —
//! an application can track updates to files that are *not* retrievable (e.g.
//! audio assets) without involving the retrieve index at all.
//!
//! ## Backends
//!
//! - [`open_redb`] — persistent, pure-Rust [redb] store (the default).
//! - [`open_in_memory`] — ephemeral [`InMemoryTrackStore`] for tests and
//!   no-persistence builds.
//!
//! The store is treated as a rebuildable cache: if the on-disk format becomes
//! incompatible the file is simply recreated (callers version the filename).
//!
//! [redb]: https://docs.rs/redb

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

mod error;
mod redb_store;

pub use error::{Error, Result};
pub use redb_store::RedbTrackStore;

/// One observed path plus its current mtime (seconds since the UNIX epoch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub path: PathBuf,
    pub mtime: i64,
}

/// The result of diffing the current filesystem state against a stored
/// snapshot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Changes {
    /// Observed now, absent from the stored snapshot.
    pub added: Vec<PathBuf>,
    /// Present in the snapshot but with a different mtime.
    pub modified: Vec<PathBuf>,
    /// Present in the snapshot but not observed now.
    pub removed: Vec<PathBuf>,
}

impl Changes {
    /// `true` when nothing was added, modified, or removed.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.removed.is_empty()
    }

    /// The set of paths a caller must re-process: `added` followed by
    /// `modified`.
    pub fn upserted(&self) -> impl Iterator<Item = &Path> {
        self.added
            .iter()
            .chain(self.modified.iter())
            .map(PathBuf::as_path)
    }
}

/// Persistent `path -> mtime` store.
///
/// All methods are synchronous, mirroring the style of
/// `sapphire_retrieve::RetrieveStore`. Paths are stored as their string
/// representation; the caller is responsible for passing consistent
/// (e.g. canonicalized) paths.
pub trait TrackStore: Send + Sync {
    /// Return the full `path -> mtime` snapshot.
    fn mtimes(&self) -> Result<HashMap<String, i64>>;

    /// Insert or update the mtime for `path`.
    fn upsert(&self, path: &str, mtime: i64) -> Result<()>;

    /// Remove the entry for `path` (no-op if absent).
    fn remove(&self, path: &str) -> Result<()>;

    /// Number of tracked paths.
    fn count(&self) -> Result<u64>;

    /// Insert or update many entries. Backends override this to commit the
    /// whole batch in a single transaction.
    fn upsert_many(&self, entries: &[(String, i64)]) -> Result<()> {
        for (path, mtime) in entries {
            self.upsert(path, *mtime)?;
        }
        Ok(())
    }
}

/// Diff `observed` (the current filesystem state) against `stored` (the
/// previous snapshot). Pure: performs no I/O.
///
/// Caller-supplied filtering is assumed to already be applied to `observed`.
pub fn diff(stored: &HashMap<String, i64>, observed: &[Observed]) -> Changes {
    let mut changes = Changes::default();
    let mut seen: HashSet<String> = HashSet::with_capacity(observed.len());

    for obs in observed {
        let key = obs.path.to_string_lossy().into_owned();
        match stored.get(&key) {
            None => changes.added.push(obs.path.clone()),
            Some(&prev) if prev != obs.mtime => changes.modified.push(obs.path.clone()),
            Some(_) => {}
        }
        seen.insert(key);
    }

    for path in stored.keys() {
        if !seen.contains(path) {
            changes.removed.push(PathBuf::from(path));
        }
    }

    changes
}

/// Read the stored snapshot from `store` and diff `observed` against it.
///
/// Does **not** mutate the store — the caller commits new mtimes (via
/// [`TrackStore::upsert_many`]) only after successfully processing the
/// changes, so an interrupted run re-detects the work rather than dropping it.
pub fn detect_changes(store: &dyn TrackStore, observed: &[Observed]) -> Result<Changes> {
    let stored = store.mtimes()?;
    Ok(diff(&stored, observed))
}

/// Return the mtime of `path` as seconds since the UNIX epoch, or 0 on error.
pub fn mtime_secs(path: &Path) -> i64 {
    path.metadata()
        .and_then(|m| m.modified())
        .map(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
        })
        .unwrap_or(0)
}

/// Walk `root` recursively and return an [`Observed`] entry for every file for
/// which `accept(path)` is `true`.
///
/// `accept` also governs directories, so it must return `true` for every
/// directory the caller wants descended into — a predicate written to accept
/// only files (say, `path.extension() == Some("md")`) prunes every
/// subdirectory and silently observes nothing below `root`. A directory for
/// which `accept` returns `false` is not descended into, so its contents are
/// never observed (and never even visited — this is a pruning decision, not
/// just a filter on the final file list). `root` itself is always entered
/// regardless of what `accept(root)` would return, so callers don't need a
/// special case for a root that happens to fail their own predicate (e.g. a
/// hidden directory used as `root` directly). Symlinks are not followed.
pub fn scan<F: Fn(&Path) -> bool>(root: &Path, accept: F) -> Result<Vec<Observed>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        // `accept` is the single decision point: it decides both which
        // directories to descend into and which files end up in `out` (the
        // loop body below just picks files back out, it does not re-check
        // `accept`). Root (depth 0) is exempt so a root that itself fails
        // `accept` — an allowed hidden directory, say — is still entered.
        .filter_entry(|e| e.depth() == 0 || accept(e.path()))
    {
        let entry = entry?;
        if entry.file_type().is_file() {
            let path = entry.path();
            out.push(Observed {
                path: path.to_path_buf(),
                mtime: mtime_secs(path),
            });
        }
    }
    Ok(out)
}

/// Open (or create) a persistent redb-backed [`TrackStore`] at `path`.
pub fn open_redb(path: &Path) -> Result<RedbTrackStore> {
    RedbTrackStore::open(path)
}

/// Create an ephemeral in-memory [`TrackStore`].
pub fn open_in_memory() -> InMemoryTrackStore {
    InMemoryTrackStore::default()
}

/// In-memory [`TrackStore`] backed by a `Mutex<HashMap>`. Used for tests and
/// no-persistence builds.
#[derive(Default)]
pub struct InMemoryTrackStore {
    inner: std::sync::Mutex<HashMap<String, i64>>,
}

impl TrackStore for InMemoryTrackStore {
    fn mtimes(&self) -> Result<HashMap<String, i64>> {
        Ok(self.inner.lock().unwrap().clone())
    }

    fn upsert(&self, path: &str, mtime: i64) -> Result<()> {
        self.inner.lock().unwrap().insert(path.to_owned(), mtime);
        Ok(())
    }

    fn remove(&self, path: &str) -> Result<()> {
        self.inner.lock().unwrap().remove(path);
        Ok(())
    }

    fn count(&self) -> Result<u64> {
        Ok(self.inner.lock().unwrap().len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(path: &str, mtime: i64) -> Observed {
        Observed {
            path: PathBuf::from(path),
            mtime,
        }
    }

    fn stored(pairs: &[(&str, i64)]) -> HashMap<String, i64> {
        pairs.iter().map(|(p, m)| (p.to_string(), *m)).collect()
    }

    #[test]
    fn diff_classifies_added_modified_removed_and_skips_unchanged() {
        let prev = stored(&[("a", 1), ("b", 2), ("gone", 9)]);
        let now = [obs("a", 1), obs("b", 5), obs("c", 3)];

        let changes = diff(&prev, &now);

        assert_eq!(changes.added, vec![PathBuf::from("c")]);
        assert_eq!(changes.modified, vec![PathBuf::from("b")]);
        assert_eq!(changes.removed, vec![PathBuf::from("gone")]);
        // "a" is unchanged → in none of the buckets.
        assert!(changes.upserted().eq([Path::new("c"), Path::new("b")]));
    }

    #[test]
    fn diff_empty_when_nothing_changed() {
        let prev = stored(&[("a", 1)]);
        let now = [obs("a", 1)];
        assert!(diff(&prev, &now).is_empty());
    }

    #[test]
    fn in_memory_store_round_trips() {
        let store = open_in_memory();
        store.upsert("x", 10).unwrap();
        store
            .upsert_many(&[("y".into(), 20), ("z".into(), 30)])
            .unwrap();
        assert_eq!(store.count().unwrap(), 3);

        let m = store.mtimes().unwrap();
        assert_eq!(m.get("x"), Some(&10));
        assert_eq!(m.get("y"), Some(&20));

        store.remove("x").unwrap();
        assert_eq!(store.count().unwrap(), 2);
        assert!(!store.mtimes().unwrap().contains_key("x"));
    }

    #[test]
    fn detect_changes_reads_store_then_diffs() {
        let store = open_in_memory();
        store.upsert("a", 1).unwrap();
        let now = [obs("a", 1), obs("b", 2)];
        let changes = detect_changes(&store, &now).unwrap();
        assert_eq!(changes.added, vec![PathBuf::from("b")]);
        assert!(changes.modified.is_empty());
        assert!(changes.removed.is_empty());
    }

    #[test]
    fn scan_prunes_a_directory_the_predicate_rejects() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("keep")).unwrap();
        std::fs::write(tmp.path().join("keep").join("a.txt"), "a").unwrap();
        std::fs::create_dir_all(tmp.path().join("skip")).unwrap();
        std::fs::write(tmp.path().join("skip").join("b.txt"), "b").unwrap();

        // Rejects the "skip" directory itself, so its contents must never be
        // visited — not merely filtered out of the result.
        let observed = scan(tmp.path(), |p| {
            !p.components().any(|c| c.as_os_str() == "skip")
        })
        .unwrap();

        let names: Vec<_> = observed
            .iter()
            .map(|o| o.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["a.txt"]);
    }

    #[test]
    fn scan_allows_a_differently_named_dot_directory_when_the_predicate_says_so() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".allowed")).unwrap();
        std::fs::write(tmp.path().join(".allowed").join("a.txt"), "a").unwrap();
        std::fs::create_dir_all(tmp.path().join(".blocked")).unwrap();
        std::fs::write(tmp.path().join(".blocked").join("b.txt"), "b").unwrap();

        // No built-in "skip anything starting with `.`" rule any more: the
        // predicate alone decides, so one dot-directory can be let through
        // while a differently-named one is still pruned.
        let observed = scan(tmp.path(), |p| {
            !p.components().any(|c| c.as_os_str() == ".blocked")
        })
        .unwrap();

        let names: Vec<_> = observed
            .iter()
            .map(|o| o.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["a.txt"]);
    }

    #[test]
    fn scan_always_enters_the_root_even_if_the_predicate_would_reject_it() {
        let tmp = tempfile::tempdir().unwrap();
        let hidden_root = tmp.path().join(".hidden_root");
        std::fs::create_dir_all(&hidden_root).unwrap();
        std::fs::write(hidden_root.join("a.txt"), "a").unwrap();

        // A predicate that rejects anything hidden would reject the root
        // itself if applied there — the walk must not let that empty out the
        // whole scan.
        let observed = scan(&hidden_root, |p| {
            !p.file_name().unwrap().to_string_lossy().starts_with('.')
        })
        .unwrap();

        let names: Vec<_> = observed
            .iter()
            .map(|o| o.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["a.txt"]);
    }
}
