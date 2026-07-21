//! Remote backend: a workspace mirrored into a **local cache** and kept in
//! sync with a `sapphire-framework-remote-server` over JSON-RPC.
//!
//! The design goal (issue #86, Step A) is that a remote workspace behaves like
//! a local one. That is achieved by giving `RemoteBackend` its own local
//! [`WorkspaceState`] cache:
//!
//! - **reads** (`read_file` / `list_dir` / `search`) come from the local cache,
//!   so they work offline and search uses the local FTS index;
//! - **writes** (`write_file` / `append_file` / `delete_file`) are applied to
//!   the cache first, then pushed to the server;
//! - [`sync`](WorkspaceBackend::sync) pulls changes newer than the local cursor
//!   and applies them to the cache.
//!
//! Only text documents are handled; binary files are ignored for now
//! (framework issue #87). Vector indexes never travel over the wire.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sapphire_remote_client::RemoteClient;
use sapphire_rpc::{Change, ChangeKind, Cursor};
use sapphire_workspace::WorkspaceState;
use tokio::sync::broadcast;

use crate::local::search_state;
use crate::{BackendEvent, Error, FileSearchResult, Result, SearchMode, SyncSummary, WorkspaceBackend};

const EVENT_CAPACITY: usize = 128;

/// Number of changes to request per `changes.pull` batch during a sync.
const PULL_BATCH: usize = 256;

/// A [`WorkspaceBackend`] backed by a remote server plus a local cache.
pub struct RemoteBackend {
    client: RemoteClient,
    ws: String,
    /// Local mirror of the remote workspace: reads hit this, writes are applied
    /// here before being pushed.
    cache: Arc<WorkspaceState>,
    /// Highest change-log cursor applied to the cache.
    cursor: Mutex<Cursor>,
    events: broadcast::Sender<BackendEvent>,
}

impl RemoteBackend {
    /// Create a remote backend for workspace `ws`, mirroring it into `cache`.
    ///
    /// `cache` is an ordinary local [`WorkspaceState`] opened on a scratch
    /// directory; it should **not** have a git sync backend attached (a plain
    /// [`WorkspaceState::open`](sapphire_workspace::WorkspaceState::open) on a
    /// non-repository directory is correct). The caller is responsible for
    /// creating that directory's workspace marker.
    pub fn new(client: RemoteClient, ws: impl Into<String>, cache: Arc<WorkspaceState>) -> Self {
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        Self {
            client,
            ws: ws.into(),
            cache,
            cursor: Mutex::new(0),
            events,
        }
    }

    /// The underlying JSON-RPC client (e.g. for blob transfer, out of the
    /// `WorkspaceBackend` surface).
    pub fn client(&self) -> &RemoteClient {
        &self.client
    }

    /// The local cache state.
    pub fn cache(&self) -> &Arc<WorkspaceState> {
        &self.cache
    }

    /// The cursor the cache has been synced up to.
    pub fn cursor(&self) -> Cursor {
        *self.cursor.lock().unwrap()
    }

    fn emit(&self, event: BackendEvent) {
        let _ = self.events.send(event);
    }

    /// Convert a workspace-relative path to the POSIX form the wire uses.
    /// Absolute paths are rejected — remote documents are keyed on relative
    /// paths.
    fn wire_path(path: &Path) -> Result<String> {
        if path.is_absolute() {
            return Err(Error::Unsupported(
                "remote backend requires workspace-relative paths",
            ));
        }
        Ok(path
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/"))
    }

    /// Push one change to the server. Returns `Err(Conflict)` when the server
    /// rejected the path because it held a newer version.
    async fn push(&self, change: Change) -> Result<()> {
        let base = self.cursor();
        let path = change.path.clone();
        let outcome = self.client.push(&self.ws, base, vec![change]).await?;
        if outcome.conflicts.contains(&path) {
            self.emit(BackendEvent::Error {
                message: format!("push conflict on '{path}'"),
            });
            return Err(Error::Conflict {
                paths: outcome.conflicts,
            });
        }
        Ok(())
    }

    /// Apply one pulled change to the local cache. Runs on the blocking pool.
    fn apply_to_cache(cache: &WorkspaceState, change: &Change) -> Result<()> {
        let rel: PathBuf = change.path.split('/').collect();
        match &change.kind {
            ChangeKind::Upsert { body, .. } => {
                cache.write_file(&rel, body)?;
            }
            ChangeKind::Delete => {
                // Ignore deletes for files the cache never had.
                let abs = cache.workspace.root.join(&rel);
                if abs.exists() {
                    cache.delete_file(&rel)?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkspaceBackend for RemoteBackend {
    async fn search(
        &self,
        query: &str,
        limit: usize,
        mode: SearchMode,
    ) -> Result<Vec<FileSearchResult>> {
        // Search the local cache (offline-capable, local FTS index).
        let cache = Arc::clone(&self.cache);
        let query = query.to_owned();
        Ok(tokio::task::spawn_blocking(move || search_state(&cache, &query, limit, mode)).await??)
    }

    async fn read_file(&self, path: &Path) -> Result<String> {
        let cache = Arc::clone(&self.cache);
        let path = path.to_owned();
        Ok(tokio::task::spawn_blocking(move || cache.read_file(&path)).await??)
    }

    async fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        let wire = Self::wire_path(path)?;
        // Apply locally first so a failed push still leaves the edit in the cache.
        let cache = Arc::clone(&self.cache);
        let owned = path.to_owned();
        let content_owned = content.to_owned();
        let for_task = owned.clone();
        tokio::task::spawn_blocking(move || cache.write_file(&for_task, &content_owned)).await??;

        self.push(Change::upsert(wire, content.to_owned(), chrono::Utc::now()))
            .await?;
        self.emit(BackendEvent::FileChanged { path: owned });
        Ok(())
    }

    async fn append_file(&self, path: &Path, content: &str) -> Result<()> {
        let wire = Self::wire_path(path)?;
        // Append locally, then push the *full* new contents — the wire protocol
        // only carries whole-document upserts.
        let cache = Arc::clone(&self.cache);
        let owned = path.to_owned();
        let content_owned = content.to_owned();
        let for_task = owned.clone();
        let full = tokio::task::spawn_blocking(move || {
            cache.append_file(&for_task, &content_owned)?;
            cache.read_file(&for_task)
        })
        .await??;

        self.push(Change::upsert(wire, full, chrono::Utc::now())).await?;
        self.emit(BackendEvent::FileChanged { path: owned });
        Ok(())
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        let wire = Self::wire_path(path)?;
        let cache = Arc::clone(&self.cache);
        let owned = path.to_owned();
        let for_task = owned.clone();
        tokio::task::spawn_blocking(move || cache.delete_file(&for_task)).await??;

        self.push(Change::delete(wire, chrono::Utc::now())).await?;
        self.emit(BackendEvent::FileRemoved { path: owned });
        Ok(())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<(PathBuf, bool)>> {
        let cache = Arc::clone(&self.cache);
        let path = path.to_owned();
        Ok(tokio::task::spawn_blocking(move || cache.list_dir(&path)).await??)
    }

    async fn sync(&self) -> Result<SyncSummary> {
        let mut upserted = 0;
        let mut removed = 0;
        loop {
            let since = self.cursor();
            let batch = self.client.pull(&self.ws, since, PULL_BATCH).await?;
            if batch.changes.is_empty() {
                break;
            }

            // Apply this batch to the cache on the blocking pool.
            let cache = Arc::clone(&self.cache);
            let changes = batch.changes.clone();
            let (u, r) = tokio::task::spawn_blocking(move || -> Result<(usize, usize)> {
                let (mut u, mut r) = (0, 0);
                for change in &changes {
                    Self::apply_to_cache(&cache, change)?;
                    match change.kind {
                        ChangeKind::Upsert { .. } => u += 1,
                        ChangeKind::Delete => r += 1,
                    }
                }
                Ok((u, r))
            })
            .await??;
            upserted += u;
            removed += r;

            *self.cursor.lock().unwrap() = batch.cursor.max(since);
            if !batch.more {
                break;
            }
        }
        self.emit(BackendEvent::Synced { upserted, removed });
        Ok(SyncSummary { upserted, removed })
    }

    fn subscribe(&self) -> broadcast::Receiver<BackendEvent> {
        self.events.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_remote_server::{ServerState, router};
    use sapphire_rpc::Change;
    use sapphire_workspace::{AppContext, Workspace};

    fn ctx() -> &'static AppContext {
        static CTX: std::sync::OnceLock<AppContext> = std::sync::OnceLock::new();
        CTX.get_or_init(|| AppContext::new("be-remote-e2e"))
    }

    /// Start an in-process server on an ephemeral port; return its base URL.
    async fn start_server() -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::new(tmp.path()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(state)).await.unwrap();
        });
        (tmp, format!("http://{addr}"))
    }

    /// Open a scratch cache `WorkspaceState`, creating its marker dir.
    fn open_cache() -> (tempfile::TempDir, Arc<WorkspaceState>) {
        let tmp = tempfile::Builder::new().prefix("cache-").tempdir().unwrap();
        ctx().set_cache_dir(std::env::temp_dir().join("be-remote-e2e-cache"));
        std::fs::create_dir_all(tmp.path().join(".be-remote-e2e")).unwrap();
        let ws = Workspace::from_root(ctx(), tmp.path()).unwrap();
        let state = WorkspaceState::open(ws).unwrap();
        (tmp, Arc::new(state))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_hits_cache_and_server() {
        let (_srv, url) = start_server().await;
        let (_cache_dir, cache) = open_cache();
        let backend = RemoteBackend::new(RemoteClient::new(url.clone()), "wsE2E", cache);

        backend
            .write_file(Path::new("a.md"), "hello world")
            .await
            .unwrap();

        // Readable from the local cache.
        assert_eq!(backend.read_file(Path::new("a.md")).await.unwrap(), "hello world");
        // Searchable via the local FTS index.
        let hits = backend.search("hello", 10, SearchMode::Fts).await.unwrap();
        assert!(hits.iter().any(|h| h.path.ends_with("a.md")), "got {hits:?}");

        // The server received the push (a second client can pull it).
        let other = RemoteClient::new(url);
        let pulled = other.pull("wsE2E", 0, 10).await.unwrap();
        assert!(pulled.changes.iter().any(|c| c.path == "a.md"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_pulls_remote_edits_into_cache() {
        let (_srv, url) = start_server().await;
        let (_cache_dir, cache) = open_cache();
        let backend = RemoteBackend::new(RemoteClient::new(url.clone()), "wsSync", cache);

        // A different client writes a document directly to the server.
        let remote = RemoteClient::new(url);
        remote
            .push("wsSync", 0, vec![Change::upsert("remote.md", "from elsewhere", chrono::Utc::now())])
            .await
            .unwrap();

        // Before sync the cache doesn't have it.
        assert!(backend.read_file(Path::new("remote.md")).await.is_err());

        let summary = backend.sync().await.unwrap();
        assert!(summary.upserted >= 1);

        // After sync it's in the local cache.
        assert_eq!(
            backend.read_file(Path::new("remote.md")).await.unwrap(),
            "from elsewhere"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_propagates_and_absolute_path_rejected() {
        let (_srv, url) = start_server().await;
        let (_cache_dir, cache) = open_cache();
        let backend = RemoteBackend::new(RemoteClient::new(url), "wsDel", cache);

        backend.write_file(Path::new("doomed.md"), "bye").await.unwrap();
        assert!(backend.read_file(Path::new("doomed.md")).await.is_ok());
        backend.delete_file(Path::new("doomed.md")).await.unwrap();
        assert!(backend.read_file(Path::new("doomed.md")).await.is_err());

        // Absolute paths are rejected (documents are keyed on relative paths).
        let abs = if cfg!(windows) { Path::new("C:/abs.md") } else { Path::new("/abs.md") };
        assert!(matches!(
            backend.write_file(abs, "x").await,
            Err(Error::Unsupported(_))
        ));
    }
}
