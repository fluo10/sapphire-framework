//! Remote backend: an async facade that talks to a
//! `sapphire-framework-remote-server` over JSON-RPC.
//!
//! MVP scope (framework-only, see `docs/ARCHITECTURE.md`): search and
//! differential sync work against the server, and `write_file` / `delete_file`
//! push a document change. Operations that need a local cache (`read_file`,
//! `append_file`, `list_dir`) return [`Error::Unsupported`]; a future WASM /
//! desktop `RemoteJournalBackend` layers a local cache on top to fill them in.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use sapphire_remote_client::RemoteClient;
use sapphire_rpc::{Change, ChangeKind, Cursor};
use tokio::sync::broadcast;

use crate::{BackendEvent, Error, FileSearchResult, Result, SearchMode, SyncSummary, WorkspaceBackend};

const EVENT_CAPACITY: usize = 128;

/// A [`WorkspaceBackend`] backed by a remote server.
pub struct RemoteBackend {
    client: RemoteClient,
    ws: String,
    /// Highest change-log cursor this backend has observed.
    cursor: Mutex<Cursor>,
    events: broadcast::Sender<BackendEvent>,
}

impl RemoteBackend {
    /// Create a remote backend for workspace `ws` reachable via `client`.
    pub fn new(client: RemoteClient, ws: impl Into<String>) -> Self {
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        Self {
            client,
            ws: ws.into(),
            cursor: Mutex::new(0),
            events,
        }
    }

    /// The underlying JSON-RPC client (e.g. for blob transfer).
    pub fn client(&self) -> &RemoteClient {
        &self.client
    }

    /// The cursor this backend has synced up to.
    pub fn cursor(&self) -> Cursor {
        *self.cursor.lock().unwrap()
    }

    fn emit(&self, event: BackendEvent) {
        let _ = self.events.send(event);
    }

    /// Convert a workspace path to the POSIX, workspace-relative form the wire
    /// protocol uses. Absolute paths are rejected (the remote backend is keyed
    /// on relative document paths).
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

    /// Push a single change and advance the cursor.
    async fn push_change(&self, change: Change) -> Result<()> {
        let base = self.cursor();
        let outcome = self.client.push(&self.ws, base, vec![change]).await?;
        *self.cursor.lock().unwrap() = outcome.cursor;
        Ok(())
    }
}

#[async_trait]
impl WorkspaceBackend for RemoteBackend {
    async fn search(
        &self,
        query: &str,
        limit: usize,
        _mode: SearchMode,
    ) -> Result<Vec<FileSearchResult>> {
        // The server currently only exposes FTS (semantic falls back to it).
        let hits = self.client.search_fts(&self.ws, query, limit).await?;
        Ok(hits
            .into_iter()
            .map(|h| FileSearchResult {
                id: 0,
                path: h.path,
                score: h.score,
                chunks: Vec::new(),
            })
            .collect())
    }

    async fn read_file(&self, _path: &Path) -> Result<String> {
        Err(Error::Unsupported("read_file (no local cache in MVP remote backend)"))
    }

    async fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        let wire = Self::wire_path(path)?;
        let change = Change::upsert(wire, content.to_owned(), chrono::Utc::now());
        self.push_change(change).await?;
        self.emit(BackendEvent::FileChanged {
            path: path.to_owned(),
        });
        Ok(())
    }

    async fn append_file(&self, _path: &Path, _content: &str) -> Result<()> {
        Err(Error::Unsupported("append_file (needs current contents; no local cache)"))
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        let wire = Self::wire_path(path)?;
        let change = Change::delete(wire, chrono::Utc::now());
        self.push_change(change).await?;
        self.emit(BackendEvent::FileRemoved {
            path: path.to_owned(),
        });
        Ok(())
    }

    async fn list_dir(&self, _path: &Path) -> Result<Vec<(PathBuf, bool)>> {
        Err(Error::Unsupported("list_dir (no local cache in MVP remote backend)"))
    }

    async fn sync(&self) -> Result<SyncSummary> {
        // Pull everything newer than our cursor in reasonably-sized batches.
        let mut upserted = 0;
        let mut removed = 0;
        loop {
            let since = self.cursor();
            let batch = self.client.pull(&self.ws, since, 256).await?;
            for change in &batch.changes {
                match change.kind {
                    ChangeKind::Upsert { .. } => upserted += 1,
                    ChangeKind::Delete => removed += 1,
                }
            }
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
