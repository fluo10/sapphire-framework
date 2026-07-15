use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use sapphire_retrieve::{Chunker, Document, JsonlChunker, RetrieveStore, TomlChunker};
use sapphire_track::TrackStore;
use thiserror::Error;

use crate::{error::Result, workspace::Workspace};

/// Return the mtime of `path` as seconds since UNIX epoch, or 0 on error.
///
/// Thin re-export of [`sapphire_track::mtime_secs`] kept under the indexer's
/// name for the single-file update paths in `workspace_state`.
pub(crate) fn file_mtime_secs(path: &Path) -> i64 {
    sapphire_track::mtime_secs(path)
}

const MARKDOWN_EXTENSIONS: &[&str] = &["md", "markdown", "txt", "rst", "org"];
const JSONL_EXTENSIONS: &[&str] = &["jsonl"];
const TOML_EXTENSIONS: &[&str] = &["toml"];

/// `true` if `path` has an extension the indexer recognises (markdown family,
/// JSONL, or TOML).
pub(crate) fn is_indexable_path(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    MARKDOWN_EXTENSIONS.contains(&ext.as_str())
        || JSONL_EXTENSIONS.contains(&ext.as_str())
        || TOML_EXTENSIONS.contains(&ext.as_str())
}

/// Generate a stable `i64` document ID from a file path (FNV-1a).
pub fn path_to_doc_id(path: &Path) -> i64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for b in path.as_os_str().as_encoded_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h as i64
}

/// Build a [`Document`] for `path` by reading the file from disk and applying
/// the extension-based chunking (paragraph / per-line / whole-file).
///
/// Returns the `io::Error` from `read_to_string` if the file cannot be read.
/// Bulk walkers call this and silently drop failures via `.ok()`; the
/// single-file `on_file_updated_*` methods propagate the error.
pub(crate) fn build_document_from_disk(path: &Path, doc_id: i64) -> std::io::Result<Document> {
    let raw = std::fs::read_to_string(path)?;

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let is_jsonl = JSONL_EXTENSIONS.contains(&ext.as_str());
    let is_toml = TOML_EXTENSIONS.contains(&ext.as_str());

    let path_str = path.to_string_lossy().into_owned();

    let doc = if is_jsonl || is_toml {
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text_chunks = if is_jsonl {
            JsonlChunker.chunk(&file_name, &raw)
        } else {
            TomlChunker.chunk(&file_name, &raw)
        };
        let body = text_chunks
            .iter()
            .map(|c| c.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        let chunks: Vec<(usize, usize, String)> = text_chunks
            .into_iter()
            .map(|c| (c.line_start, c.line_end, c.text))
            .collect();
        Document {
            id: doc_id,
            body,
            path: path_str,
            chunks: Some(chunks),
        }
    } else {
        Document {
            id: doc_id,
            body: raw,
            path: path_str,
            chunks: None,
        }
    };

    Ok(doc)
}

/// Recursively walk `workspace` and upsert all text files into `retrieve_db`.
///
/// Returns `(upserted, removed)`.
///
/// # Supported file types
///
/// | Extension | Chunking | line range in results |
/// |-----------|----------|-----------------------|
/// | `md`, `markdown`, `txt`, `rst`, `org` | paragraph split | start/end line of paragraph |
/// | `jsonl` | one message per line | `line_start == line_end` |
/// | `toml` | single whole-file chunk | first/last non-blank line |
pub fn sync_workspace(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
    track: &dyn TrackStore,
) -> Result<(usize, usize)> {
    let mut hook = NoopHook;
    let report = sync_workspace_full_with_hook(workspace, retrieve_db, track, &mut hook).map_err(
        |e| match e {
            SyncWithHookError::Workspace(e) => e,
            SyncWithHookError::Hook(never) => match never {},
        },
    )?;
    Ok((report.upserted, report.removed))
}

// ── hook-driven sync ─────────────────────────────────────────────────────────

/// Callback hook invoked during a sync run, used by external callers (e.g.
/// sapphire-journal) to update their own per-file caches alongside the
/// workspace's retrieve index.
///
/// The hook is a **side channel**: the workspace always reads the file from
/// disk and constructs the [`Document`] itself. The hook does not see or
/// modify the indexed body. Its only job is to update the calling
/// application's own DB at the same point in time the workspace is touching
/// a file.
///
/// See [`sync_workspace_with_hook`] / [`sync_workspace_full_with_hook`].
pub trait IndexHook {
    /// Error type the hook can produce. Surfaces through [`SyncWithHookError::Hook`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// Called when a file is new or its mtime changed, after the workspace
    /// has decided the file is in scope (matching extension etc.). The
    /// workspace will read the file from disk and upsert it into the retrieve
    /// DB regardless of what this method does — return `Ok(())` for the
    /// normal case.
    fn on_changed(&mut self, path: &Path, disk_mtime: i64) -> std::result::Result<(), Self::Error>;

    /// Called for each path that was in retrieve DB but is gone from disk,
    /// **before** the workspace removes its document/file row. The hook can
    /// fetch IDs from its own DB and clean up here.
    fn on_removed(&mut self, path: &str) -> std::result::Result<(), Self::Error>;

    /// Called once after all changed/removed files have been processed but
    /// before the final `rebuild_fts`. Suitable for batch validation or for
    /// the caller to commit its own DB transaction.
    fn after_sweep(&mut self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

/// Summary of a single hook-driven sync call.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncReport {
    pub upserted: usize,
    pub removed: usize,
}

/// Error returned by hook-aware sync entry points.
///
/// Wraps either an error originating in the workspace itself or one returned
/// by the [`IndexHook`], without forcing hook errors through the crate's
/// [`Error`](crate::Error) enum.
#[derive(Debug, Error)]
pub enum SyncWithHookError<E: std::error::Error + Send + Sync + 'static> {
    /// Error originating in workspace walking, file IO, or the retrieve DB.
    #[error(transparent)]
    Workspace(#[from] crate::Error),
    /// Error returned by the [`IndexHook`].
    #[error("index hook error: {0}")]
    Hook(#[source] E),
}

/// Collect all in-scope candidate file paths under `root`.
///
/// Phase 1 of the two-phase walk used by [`sync_workspace_with_hook`] /
/// [`sync_workspace_full_with_hook`]. Doing all enumeration up front means
/// that file renames performed inside [`IndexHook::on_changed`] cannot
/// perturb an in-flight walk.
fn collect_candidates(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| {
            if e.file_type().is_dir() {
                !e.file_name().to_string_lossy().starts_with('.')
            } else {
                true
            }
        })
    {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        if is_indexable_path(entry.path()) {
            out.push(entry.path().to_path_buf());
        }
    }
    Ok(out)
}

/// Hook-driven **incremental** sync.
///
/// Walks `workspace.root` with the same extension set and `.`-hidden filter
/// as [`sync_workspace_incremental`]. Files whose mtime matches the value in
/// the retrieve DB are skipped (hook is not invoked for them).
///
/// ## Phases
///
/// 1. Collect every in-scope candidate path into a vec.
/// 2. Per file: compare disk mtime against the cached value; if unchanged,
///    skip. Otherwise call [`IndexHook::on_changed`], read the file and
///    upsert it.
/// 3. For each path the DB knows about that is gone from disk: call
///    [`IndexHook::on_removed`], then `remove_file` + `remove_document`.
/// 4. [`IndexHook::after_sweep`] is called once before `rebuild_fts`.
/// 5. `rebuild_fts` runs once if anything was upserted or removed.
pub fn sync_workspace_with_hook<H: IndexHook>(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
    track: &dyn TrackStore,
    hook: &mut H,
) -> std::result::Result<SyncReport, SyncWithHookError<H::Error>> {
    sync_inner(workspace, retrieve_db, track, hook, /* full = */ false)
}

/// Hook-driven **full** sync.
///
/// Like [`sync_workspace_with_hook`] but re-indexes every file regardless of
/// mtime. Use when callers need a full rebuild (e.g. after a schema migration
/// or a manual cache wipe) and still want their app DB updated in lockstep.
pub fn sync_workspace_full_with_hook<H: IndexHook>(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
    track: &dyn TrackStore,
    hook: &mut H,
) -> std::result::Result<SyncReport, SyncWithHookError<H::Error>> {
    sync_inner(workspace, retrieve_db, track, hook, /* full = */ true)
}

fn sync_inner<H: IndexHook>(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
    track: &dyn TrackStore,
    hook: &mut H,
    full: bool,
) -> std::result::Result<SyncReport, SyncWithHookError<H::Error>> {
    let known_mtimes = track.mtimes().map_err(crate::Error::from)?;
    let existing_ids: HashSet<i64> = retrieve_db
        .document_ids()
        .map_err(crate::Error::from)?
        .into_iter()
        .collect();

    let candidates = collect_candidates(&workspace.root)?;

    let mut current_paths: HashSet<String> = HashSet::with_capacity(candidates.len());
    let mut current_ids: HashSet<i64> = HashSet::with_capacity(candidates.len());
    let mut upserted = 0usize;

    for path in &candidates {
        let path_str = path.to_string_lossy().into_owned();
        let doc_id = path_to_doc_id(path);
        current_paths.insert(path_str.clone());
        current_ids.insert(doc_id);

        let disk_mtime = file_mtime_secs(path);
        if !full
            && let Some(&cached_mtime) = known_mtimes.get(&path_str)
            && cached_mtime == disk_mtime
        {
            continue;
        }

        hook.on_changed(path, disk_mtime)
            .map_err(SyncWithHookError::Hook)?;

        let Ok(doc) = build_document_from_disk(path, doc_id) else {
            // Unreadable file — leave the existing DB row untouched and move on.
            continue;
        };

        // Index into the retrieve DB first, then record the mtime. If we crash
        // between the two, the file is re-detected as changed next run and
        // re-indexed (idempotent via the stable `doc_id`) — never silently
        // skipped, which the reverse order would risk. See the crate-split
        // atomicity note in the design.
        retrieve_db
            .upsert_document(&doc)
            .map_err(crate::Error::from)?;
        track
            .upsert(&path_str, disk_mtime)
            .map_err(crate::Error::from)?;
        upserted += 1;
    }

    // Removals: paths the DB knows about that are gone from disk.
    let mut removed_doc_ids: HashSet<i64> = HashSet::new();
    let mut removed = 0usize;

    for path_str in known_mtimes.keys() {
        if current_paths.contains(path_str) {
            continue;
        }
        hook.on_removed(path_str).map_err(SyncWithHookError::Hook)?;
        let doc_id = path_to_doc_id(Path::new(path_str));
        if existing_ids.contains(&doc_id) && removed_doc_ids.insert(doc_id) {
            retrieve_db
                .remove_document(doc_id)
                .map_err(crate::Error::from)?;
            removed += 1;
        }
        track.remove(path_str).map_err(crate::Error::from)?;
    }

    // Orphan documents: IDs in the DB with no corresponding file row.
    for &id in &existing_ids {
        if current_ids.contains(&id) || removed_doc_ids.contains(&id) {
            continue;
        }
        retrieve_db
            .remove_document(id)
            .map_err(crate::Error::from)?;
        removed += 1;
    }

    hook.after_sweep().map_err(SyncWithHookError::Hook)?;

    if upserted > 0 || removed > 0 {
        retrieve_db.rebuild_fts().map_err(crate::Error::from)?;
    }

    Ok(SyncReport { upserted, removed })
}

/// Default no-op hook used internally by the non-hook entry points.
pub(crate) struct NoopHook;

impl IndexHook for NoopHook {
    type Error = std::convert::Infallible;

    fn on_changed(
        &mut self,
        _path: &Path,
        _disk_mtime: i64,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn on_removed(&mut self, _path: &str) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

/// Convert a `SyncWithHookError<Infallible>` to the crate's `Error` type.
///
/// Used by the legacy non-hook wrappers since `Infallible` cannot be
/// constructed.
pub(crate) fn unwrap_infallible(err: SyncWithHookError<std::convert::Infallible>) -> crate::Error {
    match err {
        SyncWithHookError::Workspace(e) => e,
        SyncWithHookError::Hook(never) => match never {},
    }
}

/// Walk the workspace and update only files whose mtime (tracked in `track`)
/// has changed since the last sync. Also removes documents for files that no
/// longer exist.
///
/// Returns `(upserted, removed)`.
///
/// Thin wrapper around [`sync_workspace_with_hook`] using a no-op hook.
pub fn sync_workspace_incremental(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
    track: &dyn TrackStore,
) -> Result<(usize, usize)> {
    let mut hook = NoopHook;
    let report = sync_workspace_with_hook(workspace, retrieve_db, track, &mut hook)
        .map_err(unwrap_infallible)?;
    Ok((report.upserted, report.removed))
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "redb-store"))]
mod tests {
    use std::{cell::Cell, fs};

    use sapphire_retrieve::open_redb;
    use tempfile::TempDir;

    use super::*;
    use crate::{AppContext, Workspace};

    fn ctx() -> &'static AppContext {
        static CTX: std::sync::OnceLock<AppContext> = std::sync::OnceLock::new();
        CTX.get_or_init(|| AppContext::new("indexer-hook-test"))
    }

    fn make_workspace() -> (
        TempDir,
        Workspace,
        Arc<dyn RetrieveStore + Send + Sync>,
        sapphire_track::InMemoryTrackStore,
    ) {
        // Use a non-dotted prefix: hook-aware sync skips dotted directories at
        // any depth (matching `sync_workspace_incremental`), which includes the
        // workspace root.
        let tmp = tempfile::Builder::new().prefix("ws-").tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join(".indexer-hook-test")).unwrap();
        let workspace = Workspace::from_root(ctx(), &root).unwrap();
        let db_path = root.join(".indexer-hook-test").join("retrieve.db");
        let db = open_redb(&db_path).unwrap();
        let track = sapphire_track::open_in_memory();
        (tmp, workspace, db, track)
    }

    struct RecordingHook {
        changed: Vec<PathBuf>,
        removed: Vec<String>,
        after_sweep_count: Cell<usize>,
    }

    impl RecordingHook {
        fn new() -> Self {
            Self {
                changed: Vec::new(),
                removed: Vec::new(),
                after_sweep_count: Cell::new(0),
            }
        }
    }

    impl IndexHook for RecordingHook {
        type Error = std::convert::Infallible;

        fn on_changed(
            &mut self,
            path: &Path,
            _disk_mtime: i64,
        ) -> std::result::Result<(), Self::Error> {
            self.changed.push(path.to_path_buf());
            Ok(())
        }

        fn on_removed(&mut self, path: &str) -> std::result::Result<(), Self::Error> {
            self.removed.push(path.to_owned());
            Ok(())
        }

        fn after_sweep(&mut self) -> std::result::Result<(), Self::Error> {
            self.after_sweep_count.set(self.after_sweep_count.get() + 1);
            Ok(())
        }
    }

    #[test]
    fn hook_sees_changed_and_workspace_indexes_default() {
        let (_tmp, ws, db, track) = make_workspace();
        // Build paths from the canonicalized workspace root, not the raw
        // tempdir: the indexer walks `ws.root`, and on Windows canonicalization
        // adds a `\\?\` prefix that would not match a raw tempdir path.
        let file = ws.root.join("note.md");
        fs::write(&file, "hello world").unwrap();

        let mut hook = RecordingHook::new();
        let report = sync_workspace_with_hook(&ws, db.clone(), &track, &mut hook).unwrap();

        assert_eq!(report.upserted, 1);
        assert_eq!(report.removed, 0);
        assert_eq!(hook.changed, vec![file.clone()]);
        assert!(hook.removed.is_empty());
        assert_eq!(hook.after_sweep_count.get(), 1);

        assert_eq!(db.document_count().unwrap(), 1);
        let mtimes = track.mtimes().unwrap();
        assert!(mtimes.contains_key(file.to_string_lossy().as_ref()));
    }

    #[test]
    fn unchanged_file_skips_hook_on_second_run() {
        let (tmp, ws, db, track) = make_workspace();
        let file = tmp.path().join("stable.md");
        fs::write(&file, "stable").unwrap();

        let mut hook = RecordingHook::new();
        let first = sync_workspace_with_hook(&ws, db.clone(), &track, &mut hook).unwrap();
        assert_eq!(first.upserted, 1);

        let mut hook2 = RecordingHook::new();
        let second = sync_workspace_with_hook(&ws, db.clone(), &track, &mut hook2).unwrap();
        assert_eq!(second.upserted, 0);
        assert!(hook2.changed.is_empty());
        assert_eq!(hook2.after_sweep_count.get(), 1);
    }

    #[test]
    fn full_sync_with_hook_visits_every_file_regardless_of_mtime() {
        let (tmp, ws, db, track) = make_workspace();
        let file = tmp.path().join("stable.md");
        fs::write(&file, "stable").unwrap();

        let mut h1 = RecordingHook::new();
        sync_workspace_with_hook(&ws, db.clone(), &track, &mut h1).unwrap();
        assert_eq!(h1.changed.len(), 1);

        // Second incremental call: mtime unchanged → hook skipped.
        let mut h2 = RecordingHook::new();
        sync_workspace_with_hook(&ws, db.clone(), &track, &mut h2).unwrap();
        assert_eq!(h2.changed.len(), 0);

        // Second full call: mtime unchanged but hook is still invoked.
        let mut h3 = RecordingHook::new();
        let r = sync_workspace_full_with_hook(&ws, db, &track, &mut h3).unwrap();
        assert_eq!(h3.changed.len(), 1);
        assert_eq!(r.upserted, 1);
    }

    #[test]
    fn removed_file_triggers_on_removed_before_db_delete() {
        let (_tmp, ws, db, track) = make_workspace();
        let file = ws.root.join("doomed.md");
        fs::write(&file, "bye").unwrap();

        let mut hook = RecordingHook::new();
        sync_workspace_with_hook(&ws, db.clone(), &track, &mut hook).unwrap();
        let file_str = file.to_string_lossy().into_owned();
        assert!(track.mtimes().unwrap().contains_key(&file_str));

        fs::remove_file(&file).unwrap();
        let mut hook2 = RecordingHook::new();
        let report = sync_workspace_with_hook(&ws, db.clone(), &track, &mut hook2).unwrap();

        assert_eq!(report.removed, 1);
        assert_eq!(hook2.removed, vec![file_str.clone()]);
        assert!(!track.mtimes().unwrap().contains_key(&file_str));
        assert_eq!(db.document_count().unwrap(), 0);
    }

    #[test]
    fn after_sweep_runs_exactly_once_per_call() {
        let (tmp, ws, db, track) = make_workspace();
        fs::write(tmp.path().join("a.md"), "a").unwrap();
        fs::write(tmp.path().join("b.md"), "b").unwrap();

        let mut hook = RecordingHook::new();
        sync_workspace_with_hook(&ws, db, &track, &mut hook).unwrap();
        assert_eq!(hook.after_sweep_count.get(), 1);
        assert_eq!(hook.changed.len(), 2);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("boom")]
    struct Boom;

    struct ErrorOnFirstChange;
    impl IndexHook for ErrorOnFirstChange {
        type Error = Boom;
        fn on_changed(
            &mut self,
            _path: &Path,
            _disk_mtime: i64,
        ) -> std::result::Result<(), Self::Error> {
            Err(Boom)
        }
        fn on_removed(&mut self, _path: &str) -> std::result::Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn hook_error_is_propagated_as_hook_variant() {
        let (tmp, ws, db, track) = make_workspace();
        fs::write(tmp.path().join("a.md"), "a").unwrap();
        let mut hook = ErrorOnFirstChange;
        let err = sync_workspace_with_hook(&ws, db, &track, &mut hook).unwrap_err();
        assert!(matches!(err, SyncWithHookError::Hook(Boom)));
    }

    #[test]
    fn wrapper_preserves_legacy_signature_and_counts() {
        let (tmp, ws, db, track) = make_workspace();
        fs::write(tmp.path().join("a.md"), "a").unwrap();
        fs::write(tmp.path().join("b.md"), "b").unwrap();

        let (up, rm) = sync_workspace_incremental(&ws, db.clone(), &track).unwrap();
        assert_eq!((up, rm), (2, 0));

        fs::remove_file(tmp.path().join("a.md")).unwrap();
        let (up2, rm2) = sync_workspace_incremental(&ws, db, &track).unwrap();
        assert_eq!((up2, rm2), (0, 1));
    }
}
