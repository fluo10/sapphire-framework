//! The app server skeleton.
//!
//! An application builds one of these, adds its own methods, and runs it. Everything a
//! sapphire app needs on the server side — owning the workspaces, answering `workspace.*`,
//! pushing events, exiting when nobody is using it — is here.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.
//!
//! ```rust,ignore
//! static CTX: AppContext = AppContext::new("sapphire-journal");
//!
//! AppServer::new(&CTX, env!("CARGO_PKG_VERSION"))
//!     .extend(|router| router.method("journal.create_entry", create_entry))
//!     .run()
//!     .await?;
//! ```
//!
//! Application methods are named `<app>.<method>`. The framework owns `workspace.*` and
//! `server.*`; anything else is the application's.

#![warn(missing_docs)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sapphire_backend::protocol as proto;
use sapphire_ipc::{Endpoint, ManagedBy, Router, ServerInfo, serve};
use sapphire_workspace::AppContext;

mod error;
mod events;
mod handlers;
mod host;
#[cfg(test)]
mod test_support;

pub use error::{Error, Result};
pub use events::subscribe_method;
pub use handlers::workspace_router;
pub use host::{DEFAULT_IDLE, DEFAULT_MAX_OPEN, WorkspaceHost};

/// How long a spawned server stays up with nothing to do.
pub const DEFAULT_IDLE_EXIT: Duration = Duration::from_secs(15 * 60);

/// Longest gap between idle checks.
///
/// The actual interval is this or the idle limit, whichever is shorter, so a server asked to
/// exit after 200 ms does not sit for ten seconds first. Tests depend on that.
const IDLE_TICK: Duration = Duration::from_secs(10);

/// An application's server.
pub struct AppServer {
    ctx: &'static AppContext,
    version: &'static str,
    endpoint: Option<Endpoint>,
    managed_by: ManagedBy,
    max_open: usize,
    workspace_idle: Duration,
    idle_exit: Option<Duration>,
    host: Arc<WorkspaceHost>,
    extend: Option<Box<dyn FnOnce(Router) -> Router + Send>>,
}

impl AppServer {
    /// A server for `ctx`'s application, reporting `version` in its handshake.
    ///
    /// Pass `env!("CARGO_PKG_VERSION")`: a client compares it with its own and replaces a
    /// spawned server that does not match (spec §2.6).
    pub fn new(ctx: &'static AppContext, version: &'static str) -> AppServer {
        AppServer {
            ctx,
            version,
            endpoint: None,
            managed_by: ManagedBy::Spawned,
            max_open: DEFAULT_MAX_OPEN,
            workspace_idle: DEFAULT_IDLE,
            idle_exit: Some(DEFAULT_IDLE_EXIT),
            host: Arc::new(WorkspaceHost::new(ctx)),
            extend: None,
        }
    }

    /// Listen somewhere other than the application's default endpoint. Used by tests.
    pub fn endpoint(mut self, endpoint: Endpoint) -> AppServer {
        self.endpoint = Some(endpoint);
        self
    }

    /// How this process was started. `Service` disables idle exit.
    pub fn managed_by(mut self, managed_by: ManagedBy) -> AppServer {
        self.managed_by = managed_by;
        self
    }

    /// How many workspaces to keep open, and how long a cold one may linger.
    pub fn limits(mut self, max_open: usize, idle: Duration) -> AppServer {
        self.max_open = max_open;
        self.workspace_idle = idle;
        self.host = Arc::new(WorkspaceHost::with_limits(self.ctx, max_open, idle));
        self
    }

    /// How long to stay up with no connections. `None` never exits on its own.
    pub fn idle_exit(mut self, after: Option<Duration>) -> AppServer {
        self.idle_exit = after;
        self
    }

    /// Add the application's own methods.
    pub fn extend(mut self, f: impl FnOnce(Router) -> Router + Send + 'static) -> AppServer {
        self.extend = Some(Box::new(f));
        self
    }

    /// The workspaces this server has open. An application's own handlers use it to reach a
    /// workspace the same way the framework's do.
    pub fn host(&self) -> &Arc<WorkspaceHost> {
        &self.host
    }

    /// Listen until told to stop, or until idle.
    pub async fn run(self) -> Result<()> {
        let AppServer {
            ctx,
            version,
            endpoint,
            managed_by,
            host,
            extend,
            idle_exit,
            ..
        } = self;

        let endpoint = match endpoint {
            Some(e) => e,
            None => Endpoint::for_app(ctx.app_name)?,
        };
        let info = ServerInfo {
            version: version.to_owned(),
            pid: std::process::id(),
            managed_by,
        };

        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let live = Arc::new(AtomicU64::new(0));
        let last_activity = Arc::new(Mutex::new(std::time::Instant::now()));

        let mut router = subscribe_method(Arc::clone(&host), workspace_router(Arc::clone(&host)));
        router = router
            .method(proto::SERVER_INFO, {
                let info = info.clone();
                move |_| {
                    let info = info.clone();
                    async move {
                        serde_json::to_value(info)
                            .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                    }
                }
            })
            .method(sapphire_ipc::SHUTDOWN_METHOD, {
                let stop_tx = stop_tx.clone();
                move |_| {
                    let stop_tx = stop_tx.clone();
                    async move {
                        let _ = stop_tx.send(true);
                        serde_json::to_value(proto::Ack {})
                            .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                    }
                }
            });
        if let Some(extend) = extend {
            router = extend(router);
        }
        let router = Arc::new(router);

        #[cfg(unix)]
        let listener = sapphire_ipc::bind(&endpoint).await?;
        #[cfg(windows)]
        let mut listener = sapphire_ipc::bind(&endpoint)?;

        // Close cold workspaces, and stop when nothing has used us for a while.
        let ticker = {
            let host = Arc::clone(&host);
            let live = Arc::clone(&live);
            let last_activity = Arc::clone(&last_activity);
            let stop_tx = stop_tx.clone();
            tokio::spawn(async move {
                let tick = idle_exit
                    .map_or(IDLE_TICK, |limit| limit.min(IDLE_TICK))
                    .max(Duration::from_millis(50));
                let mut interval = tokio::time::interval(tick);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    host.close_idle();
                    let Some(limit) = idle_exit else { continue };
                    if managed_by == ManagedBy::Service {
                        continue;
                    }
                    if live.load(Ordering::Relaxed) > 0 {
                        continue;
                    }
                    let idle_for = last_activity.lock().expect("activity clock").elapsed();
                    if idle_for >= limit {
                        tracing::info!(?idle_for, "exiting after being idle");
                        let _ = stop_tx.send(true);
                        return;
                    }
                }
            })
        };

        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let conn = accepted?;
                    *last_activity.lock().expect("activity clock") =
                        std::time::Instant::now();
                    live.fetch_add(1, Ordering::Relaxed);
                    let router = Arc::clone(&router);
                    let app = ctx.app_name;
                    let info = info.clone();
                    let live = Arc::clone(&live);
                    let last_activity = Arc::clone(&last_activity);
                    tokio::spawn(async move {
                        let _ = serve(conn, router, app, info).await;
                        live.fetch_sub(1, Ordering::Relaxed);
                        *last_activity.lock().expect("activity clock") =
                            std::time::Instant::now();
                    });
                }
            }
        }

        ticker.abort();
        host.close_all();
        drop(listener); // removes the socket file on Unix
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_ipc::{ClientInfo, Endpoint, ManagedBy, SpawnConfig, ensure_server};
    use sapphire_workspace::{AppContext, AppKind};
    use std::ffi::OsString;

    use crate::test_support;

    static CTX: AppContext = AppContext::new("sapphire-appservertest");

    /// The env vars `init_ctx` writes, in the order the guard restores them.
    const DIR_VARS: [&str; 3] = [
        "SAPPHIRE_APPSERVERTEST_CACHE_DIR",
        "SAPPHIRE_APPSERVERTEST_DATA_DIR",
        "SAPPHIRE_APPSERVERTEST_CONFIG_DIR",
    ];

    fn client_info() -> ClientInfo {
        ClientInfo {
            kind: "test".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        }
    }

    /// Point the context's directories at `tmp` and restore the previous values (present
    /// or absent) when dropped — including while unwinding from a panic. Without the
    /// unconditional restore, a panicking assertion would leave the variables pointing at
    /// a temp directory the test then deletes, and the context of a later test would
    /// resolve into the void.
    struct EnvGuard {
        previous: [Option<OsString>; 3],
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        /// Write the three directory vars while holding the environment lock.
        fn set(lock: std::sync::MutexGuard<'static, ()>, tmp: &std::path::Path) -> EnvGuard {
            let previous = DIR_VARS.map(std::env::var_os);
            let dirs = ["cache", "data", "config"].map(|cat| tmp.join(cat));
            // SAFETY (via `test_support::set`): `lock` serialises every read and write of
            // the process environment in this test binary — the `host`, `handlers` and
            // `events` modules' tests share the same lock — and it is held until `drop`
            // has restored the old values.
            for (name, dir) in DIR_VARS.iter().zip(dirs) {
                test_support::set(name, &dir);
            }
            EnvGuard {
                previous,
                _lock: lock,
            }
        }
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
    /// `_tmp` is declared before `_env` so the scratch tree is deleted while the
    /// environment lock is still held: a sibling test that wakes on the lock must never
    /// observe the tree mid-deletion, nor env vars pointing at a deleted directory.
    struct Fixture {
        _tmp: tempfile::TempDir,
        _env: EnvGuard,
        endpoint: Endpoint,
    }

    /// A listening endpoint in a scratch directory, with the context's directories
    /// pointed at the same scratch tree.
    ///
    /// The caller must keep the fixture (and with it the environment lock) alive for the
    /// whole test: the static [`CTX`] is first-writer-wins, so while one test runs, every
    /// other test that would re-point the context's directories must wait.
    fn prepared() -> Fixture {
        let lock = test_support::lock();
        let tmp = tempfile::tempdir().unwrap();
        let env = EnvGuard::set(lock, tmp.path());
        CTX.init(AppKind::Server);
        let endpoint = Endpoint::in_dir("sapphire-appservertest", tmp.path().to_path_buf());
        Fixture {
            _tmp: tmp,
            _env: env,
            endpoint,
        }
    }

    /// Wait until something is listening on `endpoint`.
    ///
    /// `tokio::spawn(server.run())` only schedules the server; without this wait a fast
    /// `ensure_server` probe can run before `run` has bound the socket, and with
    /// `SpawnConfig::disabled` that single unlucky probe is the whole test failing.
    async fn wait_until_listening(endpoint: &Endpoint) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !sapphire_ipc::probe(endpoint).await.unwrap() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the server never started listening"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_running_server_answers_server_info() {
        let f = prepared();
        let endpoint = f.endpoint.clone();

        let server = AppServer::new(&CTX, "1.2.3").endpoint(endpoint.clone());
        let handle = tokio::spawn(async move { server.run().await });
        wait_until_listening(&endpoint).await;

        let (client, info) = ensure_server(
            &endpoint,
            "sapphire-appservertest",
            ClientInfo {
                version: "1.2.3".into(),
                ..client_info()
            },
            &SpawnConfig::disabled(),
        )
        .await
        .unwrap();
        assert_eq!(info.version, "1.2.3");

        let reported: sapphire_ipc::ServerInfo = client
            .call(
                sapphire_backend::protocol::SERVER_INFO,
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(reported.version, "1.2.3");

        let _: serde_json::Value = client
            .call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({}))
            .await
            .unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_stops_the_server() {
        let f = prepared();
        let endpoint = f.endpoint.clone();

        let server = AppServer::new(&CTX, "0.0.0").endpoint(endpoint.clone());
        let handle = tokio::spawn(async move { server.run().await });
        wait_until_listening(&endpoint).await;

        let (client, _) = ensure_server(
            &endpoint,
            "sapphire-appservertest",
            client_info(),
            &SpawnConfig::disabled(),
        )
        .await
        .unwrap();
        let _: serde_json::Value = client
            .call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({}))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the server must stop")
            .unwrap()
            .unwrap();
        assert!(!sapphire_ipc::probe(&endpoint).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_spawned_server_exits_when_it_goes_idle() {
        let f = prepared();
        let endpoint = f.endpoint.clone();

        let server = AppServer::new(&CTX, "0.0.0")
            .endpoint(endpoint.clone())
            .managed_by(ManagedBy::Spawned)
            .idle_exit(Some(Duration::from_millis(200)));
        let handle = tokio::spawn(async move { server.run().await });

        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("an idle spawned server must exit")
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_service_server_does_not_exit_when_idle() {
        let f = prepared();
        let endpoint = f.endpoint.clone();

        let server = AppServer::new(&CTX, "0.0.0")
            .endpoint(endpoint.clone())
            .managed_by(ManagedBy::Service)
            .idle_exit(Some(Duration::from_millis(100)));
        let handle = tokio::spawn(async move { server.run().await });

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(!handle.is_finished(), "a service must stay up");
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_application_can_add_its_own_methods() {
        let f = prepared();
        let endpoint = f.endpoint.clone();

        let server = AppServer::new(&CTX, "0.0.0")
            .endpoint(endpoint.clone())
            .extend(|r| {
                r.method("sapphire-appservertest.greet", |_| async move {
                    Ok(serde_json::json!("hello"))
                })
            });
        let handle = tokio::spawn(async move { server.run().await });
        wait_until_listening(&endpoint).await;

        let (client, _) = ensure_server(
            &endpoint,
            "sapphire-appservertest",
            client_info(),
            &SpawnConfig::disabled(),
        )
        .await
        .unwrap();
        let greeting: String = client
            .call("sapphire-appservertest.greet", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(greeting, "hello");

        let _: serde_json::Value = client
            .call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({}))
            .await
            .unwrap();
        handle.await.unwrap().unwrap();
    }
}
