//! Replication, from the app server's side.
//!
//! One `Replica` per synced workspace, registered with the bridge, driven by announcements
//! from it and by local edits.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.2.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_bridge_api::{BridgeClient, ManagedBy, RegisterParams, WorkspaceRegistration};
use sapphire_sync::{PauseReason, Replica, ReplicaConfig, ScanOutcome, SystemClock};
use sapphire_workspace::{AppContext, Workspace};
use tokio::sync::{Mutex, OnceCell, mpsc};

pub mod id;
mod methods;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
mod watch;

pub use id::{SYNC_ID_FILE, sync_id, sync_id_path};
pub use methods::sync_router;
pub use watch::{DEBOUNCE, Watcher};

use crate::error::{Error, Result};

/// What `sync.status` reports.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SyncStatus {
    /// Whether this workspace is synced at all.
    pub enabled: bool,
    /// Its identity across devices, when it is.
    pub workspace_id: Option<GrainId>,
    /// How many devices the workgroup has, this host excluded.
    pub peers: usize,
    /// Why replication is paused, if it is — a missing root or marker directory.
    pub paused: Option<String>,
    /// The last failure, if any.
    pub last_error: Option<String>,
    /// Whether the bridge is reachable. `false` is not an outage: the app server works.
    pub bridge_available: bool,
}

/// One workspace this runtime syncs.
struct Synced {
    /// Its identity across devices, shared with every peer.
    workspace_id: GrainId,
    /// Its replica store. Held open because the store is a redb database.
    replica: Arc<Mutex<Replica>>,
    /// Why replication is paused, from the last scan.
    paused: Option<PauseReason>,
    /// The last failure, if any.
    last_error: Option<String>,
}

/// Replication for one application's workspaces.
pub struct SyncRuntime {
    ctx: &'static AppContext,
    bridge: Arc<BridgeClient>,
    exe_path: PathBuf,
    managed_by: ManagedBy,
    synced: Mutex<HashMap<PathBuf, Synced>>,
    /// This host's device id. Asked of the bridge once, on the first [`enable`], and never
    /// again: a second registration with an empty workspace list would briefly clear this
    /// application's routes.
    ///
    /// [`enable`]: SyncRuntime::enable
    device_id: OnceCell<GrainId>,
    /// The file watcher, once [`watch_changes`] has started it.
    ///
    /// The runtime owns it because the spec puts it here: the sync runtime is a watcher and
    /// a replica per registered root, and the two must agree on which roots are synced.
    /// [`enable`] and [`disable`] keep them in step.
    ///
    /// [`watch_changes`]: SyncRuntime::watch_changes
    /// [`enable`]: SyncRuntime::enable
    /// [`disable`]: SyncRuntime::disable
    watcher: OnceCell<Arc<Watcher>>,
}

impl SyncRuntime {
    /// A runtime for `ctx`'s application.
    pub fn new(
        ctx: &'static AppContext,
        bridge: Arc<BridgeClient>,
        exe_path: PathBuf,
        managed_by: ManagedBy,
    ) -> SyncRuntime {
        SyncRuntime {
            ctx,
            bridge,
            exe_path,
            managed_by,
            synced: Mutex::new(HashMap::new()),
            device_id: OnceCell::new(),
            watcher: OnceCell::new(),
        }
    }

    /// Start syncing `root`, returning its identity across devices.
    ///
    /// Idempotent: enabling an already-synced workspace returns the same id and changes
    /// nothing.
    pub async fn enable(&self, root: &Path) -> Result<GrainId> {
        let key = root.canonicalize().map_err(Error::Io)?;
        let watch_key = key.clone();
        let workspace_id = sync_id(self.ctx.app_name, &key)?;

        // The map stays locked across opening the replica, so two simultaneous `enable`s for
        // one root cannot both open it — the store is a redb database, and it is opened once.
        {
            let mut synced = self.synced.lock().await;
            if let Some(existing) = synced.get(&key) {
                return Ok(existing.workspace_id);
            }
            let device_id = self.device_id().await?;
            let state_dir = self.state_dir(&key)?;
            std::fs::create_dir_all(&state_dir).map_err(Error::Io)?;
            let config = ReplicaConfig::new(self.ctx.app_name, key.clone(), device_id, &state_dir);
            let replica = Replica::open(config, Arc::new(SystemClock))
                .map_err(|e| Error::Sync(e.to_string()))?;
            synced.insert(
                key,
                Synced {
                    workspace_id,
                    replica: Arc::new(Mutex::new(replica)),
                    paused: None,
                    last_error: None,
                },
            );
        }

        // Watch the new root too, so an edit made outside the server from now on is seen.
        // Not fatal if it fails: the app server's own writes take the exact path in
        // `handlers.rs`, and the next `watch_changes` re-reads the root set anyway.
        if let Some(watcher) = self.watcher.get()
            && let Err(err) = watcher.watch(&watch_key)
        {
            tracing::warn!(root = %watch_key.display(), "could not watch: {err}");
        }

        // Register only once the replica is open, so a registration never names a workspace
        // that cannot serve a session.
        self.reregister().await?;
        Ok(workspace_id)
    }

    /// Stop syncing `root`. Files and the sync id stay, so re-enabling rejoins the same
    /// workspace rather than creating a second one.
    pub async fn disable(&self, root: &Path) -> Result<()> {
        let Ok(key) = root.canonicalize() else {
            return Ok(());
        };
        let removed = self.synced.lock().await.remove(&key);
        if removed.is_some()
            && let Some(watcher) = self.watcher.get()
        {
            watcher.unwatch(&key);
        }
        if let Some(removed) = removed {
            self.bridge
                .unregister(removed.workspace_id)
                .await
                .map_err(|e| Error::Bridge(e.to_string()))?;
            self.reregister().await?;
        }
        Ok(())
    }

    /// What `sync.status` answers with.
    pub async fn status(&self, root: &Path) -> SyncStatus {
        let bridge_available = self.bridge.status().await.is_ok();
        let peers = match self.bridge.peers().await {
            Ok(p) => p.peers.len().saturating_sub(1),
            Err(_) => 0,
        };
        let Ok(key) = root.canonicalize() else {
            return SyncStatus {
                enabled: false,
                workspace_id: None,
                peers,
                paused: None,
                last_error: None,
                bridge_available,
            };
        };
        let synced = self.synced.lock().await;
        match synced.get(&key) {
            Some(entry) => SyncStatus {
                enabled: true,
                workspace_id: Some(entry.workspace_id),
                peers,
                paused: entry.paused.map(|r| format!("{r:?}")),
                last_error: entry.last_error.clone(),
                bridge_available,
            },
            None => SyncStatus {
                enabled: false,
                workspace_id: None,
                peers,
                paused: None,
                last_error: None,
                bridge_available,
            },
        }
    }

    /// Bring the replica's view of the files up to date.
    ///
    /// Called straight after the app server's own writes, and by the watcher for everything
    /// else. A scan that finds nothing is cheap; a scan that is skipped loses an edit until
    /// the next one.
    pub async fn scan(&self, root: &Path) -> Result<()> {
        let Ok(key) = root.canonicalize() else {
            return Ok(());
        };
        let replica = {
            let synced = self.synced.lock().await;
            match synced.get(&key) {
                Some(entry) => Arc::clone(&entry.replica),
                None => return Ok(()),
            }
        };
        let outcome = {
            let mut replica = replica.lock().await;
            replica.scan().map_err(|e| Error::Sync(e.to_string()))?
        };
        let mut synced = self.synced.lock().await;
        if let Some(entry) = synced.get_mut(&key) {
            entry.paused = match outcome {
                ScanOutcome::Paused(reason) => Some(reason),
                ScanOutcome::Scanned(_) => None,
            };
        }
        Ok(())
    }

    /// Open a session with every peer that will take one.
    pub async fn sync_now(&self, root: &Path) -> Result<()> {
        let Ok(key) = root.canonicalize() else {
            return Ok(());
        };
        let (workspace_id, replica) = {
            let synced = self.synced.lock().await;
            match synced.get(&key) {
                Some(entry) => (entry.workspace_id, Arc::clone(&entry.replica)),
                None => return Ok(()),
            }
        };
        let peers = self
            .bridge
            .peers()
            .await
            .map_err(|e| Error::Bridge(e.to_string()))?;
        let me = self.device_id().await?;

        for peer in peers.peers.into_iter().filter(|p| p.device_id != me) {
            // A peer that does not host this workspace refuses, which is normal and cheap.
            let stream = match self.bridge.open_stream(workspace_id, peer.device_id).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::debug!(peer = %peer.name, "no session: {err}");
                    continue;
                }
            };
            let mut replica = replica.lock().await;
            if let Err(err) =
                sapphire_framework_session::run_session(stream, &mut replica, workspace_id).await
            {
                tracing::warn!(peer = %peer.name, "session failed: {err}");
            }
        }
        Ok(())
    }

    /// Answer the bridge's announcements until the connection closes.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut incoming = self.bridge.incoming();
        loop {
            let announcement = match incoming.recv().await {
                Ok(a) => a,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "fell behind on bridge announcements");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            };

            let replica = {
                let synced = self.synced.lock().await;
                synced
                    .values()
                    .find(|s| s.workspace_id == announcement.workspace_id)
                    .map(|s| Arc::clone(&s.replica))
            };
            let Some(replica) = replica else {
                // The bridge routed to us for a workspace we no longer hold. Not fatal:
                // ignore it and let the ticket expire.
                tracing::debug!(
                    workspace = %announcement.workspace_id,
                    "an announcement for a workspace this server does not hold"
                );
                continue;
            };

            let bridge = Arc::clone(&self.bridge);
            let workspace_id = announcement.workspace_id;
            tokio::spawn(async move {
                let stream = match bridge.accept_stream(announcement.ticket).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        tracing::warn!("could not claim an announced stream: {err}");
                        return;
                    }
                };
                let mut replica = replica.lock().await;
                if let Err(err) =
                    sapphire_framework_session::run_session(stream, &mut replica, workspace_id)
                        .await
                {
                    tracing::warn!("an inbound session failed: {err}");
                }
            });
        }
    }

    /// Every synced root, for the watcher.
    pub async fn roots(&self) -> Vec<PathBuf> {
        self.synced.lock().await.keys().cloned().collect()
    }

    /// Watch the synced roots and, on a debounced report, scan and dial.
    ///
    /// This is the safety net for everything the app server did not do itself — a file
    /// edited in an editor, a `git checkout`. The exact path is [`scan`] called from
    /// `handlers.rs` right after a write the server made. A scan that finds nothing is
    /// cheap; a scan that is skipped loses an edit until the next one.
    ///
    /// Returns when the reporting channel closes — that is, when the [`Watcher`] this
    /// runtime holds is dropped, which happens when the runtime itself is.
    ///
    /// [`scan`]: SyncRuntime::scan
    pub async fn watch_changes(self: Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(64);
        let watcher = Arc::new(Watcher::start(self.roots().await, tx)?);
        let _ = self.watcher.set(Arc::clone(&watcher));

        // A root enabled between `roots()` above and the `set` just now was not in the
        // starting set, and `enable` could not reach a watcher that did not exist yet.
        // Watching the current set again is idempotent on `notify`'s side.
        for root in self.roots().await {
            if let Err(err) = watcher.watch(&root) {
                tracing::warn!(root = %root.display(), "could not watch: {err}");
            }
        }

        while let Some(root) = rx.recv().await {
            // Logged, not fatal: a failed scan or dial leaves the replica behind, and the
            // next report tries again. Neither may take the app server down (spec §10).
            if let Err(err) = self.scan(&root).await {
                tracing::warn!(root = %root.display(), "scan after a local edit failed: {err}");
            }
            if let Err(err) = self.sync_now(&root).await {
                tracing::warn!(root = %root.display(), "dial after a local edit failed: {err}");
            }
        }
        Ok(())
    }

    /// Tell the bridge the complete current set. A registration is not a delta.
    async fn reregister(&self) -> Result<()> {
        let workspaces: Vec<WorkspaceRegistration> = {
            let synced = self.synced.lock().await;
            synced
                .iter()
                .map(|(root, entry)| WorkspaceRegistration {
                    workspace_id: entry.workspace_id,
                    root: root.clone(),
                })
                .collect()
        };
        self.bridge
            .register(RegisterParams {
                app_name: self.ctx.app_name.to_owned(),
                exe_path: self.exe_path.clone(),
                managed_by: self.managed_by,
                workspaces,
            })
            .await
            .map_err(|e| Error::Bridge(e.to_string()))?;
        Ok(())
    }

    /// This host's device id, asked of the bridge once.
    async fn device_id(&self) -> Result<GrainId> {
        if let Some(id) = self.device_id.get() {
            return Ok(*id);
        }
        let result = self
            .bridge
            .register(RegisterParams {
                app_name: self.ctx.app_name.to_owned(),
                exe_path: self.exe_path.clone(),
                managed_by: self.managed_by,
                workspaces: Vec::new(),
            })
            .await
            .map_err(|e| Error::Bridge(e.to_string()))?;
        let _ = self.device_id.set(result.device_id);
        Ok(result.device_id)
    }

    /// `<workspace cache dir>/sync/`.
    fn state_dir(&self, root: &Path) -> Result<PathBuf> {
        let workspace = Workspace::from_root(self.ctx, root)?;
        Ok(workspace.cache_dir().join("sync"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::testing::StubBridge;
    use crate::test_support;
    use sapphire_workspace::AppKind;
    use std::ffi::OsString;

    static CTX: AppContext = AppContext::new("sapphire-synctest");

    /// The env vars `CTX.init` reads.
    const DIR_VARS: [&str; 3] = [
        "SAPPHIRE_SYNCTEST_CACHE_DIR",
        "SAPPHIRE_SYNCTEST_DATA_DIR",
        "SAPPHIRE_SYNCTEST_CONFIG_DIR",
    ];

    /// Points the context's directories at the test's scratch tree, and restores the
    /// previous values when dropped — including while unwinding from a panic.
    struct EnvGuard {
        previous: [Option<OsString>; 3],
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: `self._lock` still serialises the environment; it is dropped only
            // after this method returns.
            for (name, previous) in DIR_VARS.iter().zip(self.previous.iter_mut()) {
                match previous.take() {
                    Some(value) => unsafe { std::env::set_var(name, value) },
                    None => test_support::remove(name),
                }
            }
        }
    }

    /// Everything one test needs, held for the test's whole body.
    ///
    /// The context is a `static` shared by this binary, so its directories are resolved from
    /// the environment by whichever test initialises it first. The environment lock is
    /// therefore held for the whole test, as in the crate's other test modules, and `_tmp`
    /// is declared before `_env` so the scratch tree is gone only once the environment no
    /// longer points at it.
    struct Fixture {
        _tmp: tempfile::TempDir,
        _env: EnvGuard,
        root: PathBuf,
        stub: StubBridge,
        runtime: Arc<SyncRuntime>,
    }

    impl Fixture {
        /// A second workspace root beside the first, marker directory and all.
        fn second_root(&self) -> PathBuf {
            let root = self._tmp.path().join("ws2");
            std::fs::create_dir_all(root.join(".sapphire-synctest")).unwrap();
            root.canonicalize().unwrap()
        }
    }

    async fn fixture() -> Fixture {
        let lock = test_support::lock();
        let tmp = tempfile::tempdir().unwrap();
        let previous = DIR_VARS.map(std::env::var_os);
        // SAFETY (via `test_support::set`): `lock` serialises every read and write of the
        // process environment in this test binary, and it is held until `drop` has restored
        // the old values.
        for (name, dir) in DIR_VARS
            .iter()
            .zip(["cache", "data", "config"].map(|cat| tmp.path().join(cat)))
        {
            test_support::set(name, &dir);
        }
        let env = EnvGuard {
            previous,
            _lock: lock,
        };
        CTX.init(AppKind::Server);

        let root = tmp.path().join("ws");
        std::fs::create_dir_all(root.join(".sapphire-synctest")).unwrap();
        let root = root.canonicalize().unwrap();

        let (stub, client) = StubBridge::start().await;
        let runtime = Arc::new(SyncRuntime::new(
            &CTX,
            client,
            "/bin/true".into(),
            ManagedBy::Service,
        ));
        Fixture {
            _tmp: tmp,
            _env: env,
            root,
            stub,
            runtime,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enabling_registers_the_workspace_with_the_bridge() {
        let f = fixture().await;

        let id = f.runtime.enable(&f.root).await.unwrap();
        assert_eq!(f.stub.last_workspaces(), vec![id]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enabling_twice_is_idempotent_and_keeps_the_same_id() {
        let f = fixture().await;

        let first = f.runtime.enable(&f.root).await.unwrap();
        let second = f.runtime.enable(&f.root).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(
            f.stub.last_workspaces(),
            vec![first],
            "still exactly one workspace"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_registration_carries_the_complete_current_list() {
        let f = fixture().await;
        let second_root = f.second_root();

        let a = f.runtime.enable(&f.root).await.unwrap();
        let b = f.runtime.enable(&second_root).await.unwrap();

        let mut seen = f.stub.last_workspaces();
        seen.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(seen, want, "registration is the whole set, not a delta");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_later_registration_clears_this_apps_routes() {
        // The device id is asked of the bridge once, on the first `enable`, with an empty
        // workspace list — the one moment that is safe, because no route exists yet. Every
        // later registration must name the complete set: one with an empty list would
        // briefly clear this application's routes, and a peer asking for a workspace this
        // server owns would be told nobody has it.
        let f = fixture().await;
        let second_root = f.second_root();

        f.runtime.enable(&f.root).await.unwrap();
        f.runtime.enable(&second_root).await.unwrap();
        f.runtime.sync_now(&f.root).await.unwrap();
        f.runtime.disable(&f.root).await.unwrap();

        let seen = f.stub.seen.lock().expect("stub");
        let first_full = seen
            .registrations
            .iter()
            .position(|r| !r.workspaces.is_empty())
            .expect("enabling registers the workspace");
        let emptied_afterwards = seen.registrations[first_full + 1..]
            .iter()
            .filter(|r| r.workspaces.is_empty())
            .count();
        assert_eq!(
            emptied_afterwards, 0,
            "a registration after the first workspace must still name the whole set"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabling_unregisters_and_leaves_the_files_alone() {
        let f = fixture().await;
        std::fs::write(f.root.join("keep.md"), "content").unwrap();

        let id = f.runtime.enable(&f.root).await.unwrap();
        f.runtime.disable(&f.root).await.unwrap();

        assert!(f.stub.last_workspaces().is_empty());
        assert!(
            f.root.join("keep.md").exists(),
            "disabling sync must not touch files"
        );
        assert!(
            crate::sync::id::sync_id_path("sapphire-synctest", &f.root).exists(),
            "the sync id stays, so re-enabling rejoins the same workspace"
        );
        let _ = id;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn re_enabling_after_disabling_reuses_the_identity() {
        let f = fixture().await;

        let first = f.runtime.enable(&f.root).await.unwrap();
        f.runtime.disable(&f.root).await.unwrap();
        let again = f.runtime.enable(&f.root).await.unwrap();

        assert_eq!(first, again);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_reports_disabled_for_a_workspace_that_was_never_enabled() {
        let f = fixture().await;

        let status = f.runtime.status(&f.root).await;
        assert!(!status.enabled);
        assert!(status.workspace_id.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabling_something_that_was_never_enabled_is_not_an_error() {
        let f = fixture().await;
        f.runtime
            .disable(&f.root)
            .await
            .expect("disabling twice is fine");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_workspace_whose_root_vanished_is_reported_as_paused_not_deleted() {
        let f = fixture().await;
        std::fs::write(f.root.join("a.md"), "content").unwrap();
        f.runtime.enable(&f.root).await.unwrap();
        f.runtime.scan(&f.root).await.unwrap();

        // The drive is unmounted: the marker directory goes with it.
        std::fs::remove_dir_all(f.root.join(".sapphire-synctest")).unwrap();
        let _ = f.runtime.scan(&f.root).await;

        let status = f.runtime.status(&f.root).await;
        assert!(
            status.paused.is_some(),
            "a missing root must pause, not replicate as a mass deletion"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_directory_that_is_not_a_workspace_cannot_be_enabled() {
        let f = fixture().await;
        let plain = f._tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();

        assert!(f.runtime.enable(&plain).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_through_the_workspace_router_is_committed_to_the_replica() {
        // The brief's "main path": a write the app server itself made is scanned straight
        // after, not left to the watcher's debounce. Asserted against the replica's own
        // state, which is the only thing that can tell the two paths apart — reading the
        // file back would pass even if the scan never ran.
        let f = fixture().await;
        f.runtime.enable(&f.root).await.unwrap();

        let host = Arc::new(crate::WorkspaceHost::new(&CTX));
        let router = Arc::new(crate::workspace_router_with_sync(
            host,
            Some(Arc::clone(&f.runtime)),
        ));
        let (client_conn, server_conn) = sapphire_ipc::Connection::pair();
        tokio::spawn(async move {
            let info = sapphire_ipc::ServerInfo {
                version: "0.0.0".into(),
                pid: std::process::id(),
                managed_by: ManagedBy::Spawned,
            };
            let _ = sapphire_ipc::serve(server_conn, router, "sapphire-synctest", info).await;
        });
        let info = sapphire_ipc::ClientInfo {
            kind: "test".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        };
        let (client, _) = sapphire_ipc::Client::handshake(client_conn, "sapphire-synctest", info)
            .await
            .unwrap();

        let _: sapphire_backend::protocol::Ack = client
            .call(
                sapphire_backend::protocol::WRITE_FILE,
                sapphire_backend::protocol::ContentParams {
                    ws: f.root.clone(),
                    path: PathBuf::from("through-the-server.md"),
                    content: "written by the server".into(),
                },
            )
            .await
            .unwrap();

        // `scan` runs before the reply is written, so by the time the call returns the
        // replica must already know the path.
        let synced = f.runtime.synced.lock().await;
        let entry = synced.get(&f.root).expect("enabled above");
        let replica = entry.replica.lock().await;
        let state = replica
            .state("through-the-server.md")
            .expect("a readable state");
        assert!(
            state.is_some(),
            "a write through the server must be scanned into the replica, not left to the watcher"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_announcement_for_an_unknown_workspace_is_ignored_not_fatal() {
        let f = fixture().await;
        f.runtime.enable(&f.root).await.unwrap();

        let driver = Arc::clone(&f.runtime);
        let handle = tokio::spawn(async move { driver.run().await });

        f.stub
            .announce(grain_id::GrainId::random(), "no-such-ticket")
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            !handle.is_finished(),
            "one bad announcement must not stop the runtime"
        );
        handle.abort();
    }
}
