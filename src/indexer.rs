use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use sapphire_retrieve::{Chunker, Document, JsonlChunker, RetrieveStore, TomlChunker};
use thiserror::Error;

use crate::{error::Result, workspace::Workspace};

/// Return the mtime of `path` as seconds since UNIX epoch, or 0 on error.
fn file_mtime_secs(path: &Path) -> i64 {
    path.metadata()
        .and_then(|m| m.modified())
        .map(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
        })
        .unwrap_or(0)
}

const MARKDOWN_EXTENSIONS: &[&str] = &["md", "markdown", "txt", "rst", "org"];
const JSONL_EXTENSIONS: &[&str] = &["jsonl"];
const TOML_EXTENSIONS: &[&str] = &["toml"];

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

/// Build a [`Document`] for `path` using the extension-default chunking.
///
/// Returns `None` if the file cannot be read (mirrors the silent-skip behavior
/// of the legacy [`sync_workspace_incremental`]).
fn build_default_document(path: &Path, doc_id: i64) -> Option<Document> {
    let raw = std::fs::read_to_string(path).ok()?;

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

    Some(doc)
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
) -> Result<(usize, usize)> {
    let existing_ids: HashSet<i64> = retrieve_db
        .document_ids()
        .unwrap_or_default()
        .into_iter()
        .collect();

    let mut current_ids: HashSet<i64> = HashSet::new();
    let mut upserted = 0;

    for entry in walkdir::WalkDir::new(&workspace.root)
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
        let path = entry.path();

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let is_markdown = MARKDOWN_EXTENSIONS.contains(&ext.as_str());
        let is_jsonl = JSONL_EXTENSIONS.contains(&ext.as_str());
        let is_toml = TOML_EXTENSIONS.contains(&ext.as_str());

        if !is_markdown && !is_jsonl && !is_toml {
            continue;
        }

        let doc_id = path_to_doc_id(path);
        let Some(doc) = build_default_document(path, doc_id) else {
            continue;
        };

        retrieve_db.upsert_document(&doc)?;
        current_ids.insert(doc_id);
        upserted += 1;
    }

    retrieve_db.rebuild_fts()?;

    let mut removed = 0;
    for id in &existing_ids {
        if !current_ids.contains(id) {
            retrieve_db.remove_document(*id)?;
            removed += 1;
        }
    }
    if removed > 0 {
        retrieve_db.rebuild_fts()?;
    }

    Ok((upserted, removed))
}

// ── hook-driven incremental sync ──────────────────────────────────────────────

/// Callback hook invoked during incremental sync.
///
/// The workspace owns the walk, mtime read, default `Document` construction,
/// and all writes to the retrieve DB. The hook gets to (a) override the
/// `Document` construction for files it recognizes (e.g. parse frontmatter)
/// and update its own caches, and (b) react to file removals.
///
/// See [`sync_workspace_with_hook`].
pub trait IndexHook {
    /// Error type the hook can produce. Surfaces through [`SyncWithHookError::Hook`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// Called when a file is new or its mtime changed, after the workspace
    /// has decided the file is in scope (matching extension etc.).
    ///
    /// - `Ok(Some(doc))` → the workspace uses *this* `Document` for
    ///   `upsert_document`, instead of building one from the raw file body.
    ///   The hook is expected to have updated its own caches/tables already.
    /// - `Ok(None)` → the workspace falls back to its default `Document`
    ///   construction (read file body, chunk by extension, upsert). This is
    ///   the path for files the hook doesn't recognize but still wants
    ///   indexed for plain-text search.
    fn on_changed(
        &mut self,
        path: &Path,
        disk_mtime: i64,
    ) -> std::result::Result<Option<Document>, Self::Error>;

    /// Called for each path that was in retrieve DB but is gone from disk,
    /// **before** the workspace removes its document/file row. The hook can
    /// fetch IDs from its own DB and clean up here.
    fn on_removed(&mut self, path: &str) -> std::result::Result<(), Self::Error>;

    /// Called once after all changed/removed files have been processed but
    /// before the final `rebuild_fts`. Suitable for batch validation
    /// or for the caller to commit its own DB transaction.
    fn after_sweep(&mut self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

/// Summary of a single [`sync_workspace_with_hook`] run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncReport {
    pub upserted: usize,
    pub removed: usize,
}

/// Error returned by [`sync_workspace_with_hook`].
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

/// Hook-driven incremental sync.
///
/// Walks `workspace.root` with the same extension set and `.`-hidden filter
/// as [`sync_workspace_incremental`]. The hook is invoked per file but the
/// workspace remains in charge of file enumeration, mtime tracking, and
/// retrieve DB writes.
///
/// ## Phases
///
/// 1. **Collect candidates.** Walk the tree once and gather every in-scope
///    file path into a vec before invoking the hook. This guarantees that
///    file renames performed inside [`IndexHook::on_changed`] cannot perturb
///    the in-flight walk.
/// 2. **Per-file dispatch.** For each candidate, compare disk mtime against
///    the cached value. If unchanged, skip. Otherwise call
///    [`IndexHook::on_changed`]:
///    - `Some(doc)` → workspace runs
///      `upsert_file(path, disk_mtime) + upsert_document(&doc)`.
///    - `None` → workspace builds a default `Document` from the file body
///      and runs the same two upserts.
/// 3. **Removals.** For each path in the retrieve DB that no longer appears
///    on disk, call [`IndexHook::on_removed`] **before** `remove_file` /
///    `remove_document`.
/// 4. **`after_sweep`.** Called once, after all upserts and removals, before
///    the final `rebuild_fts`.
/// 5. **`rebuild_fts`** is run once if anything was upserted or removed.
pub fn sync_workspace_with_hook<H: IndexHook>(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
    hook: &mut H,
) -> std::result::Result<SyncReport, SyncWithHookError<H::Error>> {
    let known_mtimes = retrieve_db.file_mtimes().map_err(crate::Error::from)?;
    let existing_ids: HashSet<i64> = retrieve_db
        .document_ids()
        .map_err(crate::Error::from)?
        .into_iter()
        .collect();

    // Phase 1: collect all in-scope candidate paths.
    let mut candidates: Vec<PathBuf> = Vec::new();
    for entry in walkdir::WalkDir::new(&workspace.root)
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
        let entry = entry.map_err(crate::Error::from)?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let is_markdown = MARKDOWN_EXTENSIONS.contains(&ext.as_str());
        let is_jsonl = JSONL_EXTENSIONS.contains(&ext.as_str());
        let is_toml = TOML_EXTENSIONS.contains(&ext.as_str());

        if is_markdown || is_jsonl || is_toml {
            candidates.push(path.to_path_buf());
        }
    }

    let mut current_paths: HashSet<String> = HashSet::with_capacity(candidates.len());
    let mut current_ids: HashSet<i64> = HashSet::with_capacity(candidates.len());
    let mut upserted = 0usize;

    // Phase 2: invoke hook per file.
    for path in &candidates {
        let path_str = path.to_string_lossy().into_owned();
        let doc_id = path_to_doc_id(path);
        current_paths.insert(path_str.clone());
        current_ids.insert(doc_id);

        let disk_mtime = file_mtime_secs(path);
        if let Some(&cached_mtime) = known_mtimes.get(&path_str)
            && cached_mtime == disk_mtime
        {
            continue;
        }

        let hooked = hook
            .on_changed(path, disk_mtime)
            .map_err(SyncWithHookError::Hook)?;

        let doc = match hooked {
            Some(doc) => doc,
            None => match build_default_document(path, doc_id) {
                Some(doc) => doc,
                None => continue,
            },
        };

        retrieve_db
            .upsert_file(&path_str, disk_mtime)
            .map_err(crate::Error::from)?;
        retrieve_db
            .upsert_document(&doc)
            .map_err(crate::Error::from)?;
        upserted += 1;
    }

    // Phase 3: removals — paths the DB knows about that are gone from disk.
    let mut removed_doc_ids: HashSet<i64> = HashSet::new();
    let mut removed = 0usize;

    for path_str in known_mtimes.keys() {
        if current_paths.contains(path_str) {
            continue;
        }
        hook.on_removed(path_str).map_err(SyncWithHookError::Hook)?;
        retrieve_db
            .remove_file(path_str)
            .map_err(crate::Error::from)?;
        let doc_id = path_to_doc_id(Path::new(path_str));
        if existing_ids.contains(&doc_id) && removed_doc_ids.insert(doc_id) {
            retrieve_db
                .remove_document(doc_id)
                .map_err(crate::Error::from)?;
            removed += 1;
        }
    }

    // Phase 3b: orphan documents — IDs in the DB with no corresponding file
    // row. The hook has no path to receive, so we just drop the document.
    for &id in &existing_ids {
        if current_ids.contains(&id) || removed_doc_ids.contains(&id) {
            continue;
        }
        retrieve_db
            .remove_document(id)
            .map_err(crate::Error::from)?;
        removed += 1;
    }

    // Phase 4: hook gets a final chance before fts is rebuilt.
    hook.after_sweep().map_err(SyncWithHookError::Hook)?;

    // Phase 5: rebuild FTS once if anything changed.
    if upserted > 0 || removed > 0 {
        retrieve_db.rebuild_fts().map_err(crate::Error::from)?;
    }

    Ok(SyncReport { upserted, removed })
}

/// Default no-op hook used internally by [`sync_workspace_incremental`].
struct NoopHook;

impl IndexHook for NoopHook {
    type Error = std::convert::Infallible;

    fn on_changed(
        &mut self,
        _path: &Path,
        _disk_mtime: i64,
    ) -> std::result::Result<Option<Document>, Self::Error> {
        Ok(None)
    }

    fn on_removed(&mut self, _path: &str) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

/// Walk the workspace and update only files whose mtime has changed since the
/// last sync. Also removes documents (and their file rows) for files that no
/// longer exist.
///
/// Returns `(upserted, removed)`.
///
/// This is now a thin wrapper around [`sync_workspace_with_hook`] using a
/// no-op hook. Behavior matches the prior bespoke implementation, with one
/// minor cleanup: stale `files` rows for deleted paths are now removed
/// (previously they were left behind, leaking entries over time).
pub fn sync_workspace_incremental(
    workspace: &Workspace,
    retrieve_db: Arc<dyn RetrieveStore + Send + Sync>,
) -> Result<(usize, usize)> {
    let mut hook = NoopHook;
    let report =
        sync_workspace_with_hook(workspace, retrieve_db, &mut hook).map_err(|e| match e {
            SyncWithHookError::Workspace(e) => e,
            SyncWithHookError::Hook(never) => match never {},
        })?;
    Ok((report.upserted, report.removed))
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "sqlite-store"))]
mod tests {
    use std::{cell::Cell, fs};

    use sapphire_retrieve::open_sqlite_fts;
    use tempfile::TempDir;

    use super::*;
    use crate::{AppContext, Workspace};

    fn ctx() -> &'static AppContext {
        static CTX: std::sync::OnceLock<AppContext> = std::sync::OnceLock::new();
        CTX.get_or_init(|| AppContext::new("indexer-hook-test"))
    }

    fn make_workspace() -> (TempDir, Workspace, Arc<dyn RetrieveStore + Send + Sync>) {
        // Use a non-dotted prefix: `sync_workspace_with_hook` skips dotted
        // directories at any depth (matching `sync_workspace_incremental`),
        // which includes the workspace root.
        let tmp = tempfile::Builder::new().prefix("ws-").tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join(".indexer-hook-test")).unwrap();
        let workspace = Workspace::from_root(ctx(), &root).unwrap();
        let db_path = root.join(".indexer-hook-test").join("retrieve.sqlite");
        let db = open_sqlite_fts(&db_path);
        (tmp, workspace, db)
    }

    struct RecordingHook {
        changed: Vec<PathBuf>,
        removed: Vec<String>,
        after_sweep_count: Cell<usize>,
        override_for: Option<PathBuf>,
        override_body: String,
    }

    impl RecordingHook {
        fn new() -> Self {
            Self {
                changed: Vec::new(),
                removed: Vec::new(),
                after_sweep_count: Cell::new(0),
                override_for: None,
                override_body: String::new(),
            }
        }
    }

    impl IndexHook for RecordingHook {
        type Error = std::convert::Infallible;

        fn on_changed(
            &mut self,
            path: &Path,
            _disk_mtime: i64,
        ) -> std::result::Result<Option<Document>, Self::Error> {
            self.changed.push(path.to_path_buf());
            if self.override_for.as_deref() == Some(path) {
                Ok(Some(Document {
                    id: path_to_doc_id(path),
                    body: self.override_body.clone(),
                    path: path.to_string_lossy().into_owned(),
                    chunks: None,
                }))
            } else {
                Ok(None)
            }
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
    fn hook_sees_changed_and_falls_back_to_default_indexing() {
        let (tmp, ws, db) = make_workspace();
        let file = tmp.path().join("note.md");
        fs::write(&file, "hello world").unwrap();

        let mut hook = RecordingHook::new();
        let report = sync_workspace_with_hook(&ws, db.clone(), &mut hook).unwrap();

        assert_eq!(report.upserted, 1);
        assert_eq!(report.removed, 0);
        assert_eq!(hook.changed, vec![file.clone()]);
        assert!(hook.removed.is_empty());
        assert_eq!(hook.after_sweep_count.get(), 1);

        // Default indexing should have stored body via fts.
        assert_eq!(db.document_count().unwrap(), 1);
        let mtimes = db.file_mtimes().unwrap();
        assert!(mtimes.contains_key(file.to_string_lossy().as_ref()));
    }

    #[test]
    fn hook_override_replaces_default_document() {
        let (tmp, ws, db) = make_workspace();
        let file = tmp.path().join("entry.md");
        fs::write(&file, "raw on-disk body").unwrap();

        let mut hook = RecordingHook::new();
        hook.override_for = Some(file.clone());
        hook.override_body = "synthetic body from hook".to_string();

        let report = sync_workspace_with_hook(&ws, db.clone(), &mut hook).unwrap();
        assert_eq!(report.upserted, 1);

        // The override body should be searchable, not the raw file body.
        let hits = db
            .search_fts(&sapphire_retrieve::FtsQuery {
                query: "synthetic",
                path_prefix: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        let hits_raw = db
            .search_fts(&sapphire_retrieve::FtsQuery {
                query: "raw",
                path_prefix: None,
                limit: 10,
            })
            .unwrap();
        assert!(hits_raw.is_empty());
    }

    #[test]
    fn unchanged_file_skips_hook_on_second_run() {
        let (tmp, ws, db) = make_workspace();
        let file = tmp.path().join("stable.md");
        fs::write(&file, "stable").unwrap();

        let mut hook = RecordingHook::new();
        let first = sync_workspace_with_hook(&ws, db.clone(), &mut hook).unwrap();
        assert_eq!(first.upserted, 1);

        let mut hook2 = RecordingHook::new();
        let second = sync_workspace_with_hook(&ws, db.clone(), &mut hook2).unwrap();
        assert_eq!(second.upserted, 0);
        assert!(hook2.changed.is_empty());
        assert_eq!(hook2.after_sweep_count.get(), 1);
    }

    #[test]
    fn removed_file_triggers_on_removed_before_db_delete() {
        let (tmp, ws, db) = make_workspace();
        let file = tmp.path().join("doomed.md");
        fs::write(&file, "bye").unwrap();

        let mut hook = RecordingHook::new();
        sync_workspace_with_hook(&ws, db.clone(), &mut hook).unwrap();
        let file_str = file.to_string_lossy().into_owned();
        assert!(db.file_mtimes().unwrap().contains_key(&file_str));

        fs::remove_file(&file).unwrap();
        let mut hook2 = RecordingHook::new();
        let report = sync_workspace_with_hook(&ws, db.clone(), &mut hook2).unwrap();

        assert_eq!(report.removed, 1);
        assert_eq!(hook2.removed, vec![file_str.clone()]);
        assert!(!db.file_mtimes().unwrap().contains_key(&file_str));
        assert_eq!(db.document_count().unwrap(), 0);
    }

    #[test]
    fn after_sweep_runs_exactly_once_per_call() {
        let (tmp, ws, db) = make_workspace();
        fs::write(tmp.path().join("a.md"), "a").unwrap();
        fs::write(tmp.path().join("b.md"), "b").unwrap();

        let mut hook = RecordingHook::new();
        sync_workspace_with_hook(&ws, db, &mut hook).unwrap();
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
        ) -> std::result::Result<Option<Document>, Self::Error> {
            Err(Boom)
        }
        fn on_removed(&mut self, _path: &str) -> std::result::Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn hook_error_is_propagated_as_hook_variant() {
        let (tmp, ws, db) = make_workspace();
        fs::write(tmp.path().join("a.md"), "a").unwrap();
        let mut hook = ErrorOnFirstChange;
        let err = sync_workspace_with_hook(&ws, db, &mut hook).unwrap_err();
        assert!(matches!(err, SyncWithHookError::Hook(Boom)));
    }

    #[test]
    fn wrapper_preserves_legacy_signature_and_counts() {
        let (tmp, ws, db) = make_workspace();
        fs::write(tmp.path().join("a.md"), "a").unwrap();
        fs::write(tmp.path().join("b.md"), "b").unwrap();

        let (up, rm) = sync_workspace_incremental(&ws, db.clone()).unwrap();
        assert_eq!((up, rm), (2, 0));

        fs::remove_file(tmp.path().join("a.md")).unwrap();
        let (up2, rm2) = sync_workspace_incremental(&ws, db).unwrap();
        assert_eq!((up2, rm2), (0, 1));
    }
}
