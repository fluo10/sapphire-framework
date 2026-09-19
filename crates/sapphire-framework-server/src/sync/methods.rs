//! The `sync.*` methods.

use std::sync::Arc;

use sapphire_backend::protocol as proto;
use sapphire_ipc::{Router, RpcError};

use crate::handlers::rpc_error;
use crate::sync::SyncRuntime;

/// Add `sync.enable`, `sync.disable` and `sync.status` to `router`.
pub fn sync_router(runtime: Arc<SyncRuntime>, router: Router) -> Router {
    let enable = Arc::clone(&runtime);
    let disable = Arc::clone(&runtime);
    let status = runtime;

    router
        .method(proto::SYNC_ENABLE, move |ctx| {
            let runtime = Arc::clone(&enable);
            async move {
                let p: proto::WsParams = serde_json::from_value(ctx.params)
                    .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
                let workspace_id = runtime.enable(&p.ws).await.map_err(|e| rpc_error(&e))?;
                serde_json::to_value(proto::SyncEnableResult { workspace_id })
                    .map_err(|e| RpcError::internal(e.to_string()))
            }
        })
        .method(proto::SYNC_DISABLE, move |ctx| {
            let runtime = Arc::clone(&disable);
            async move {
                let p: proto::WsParams = serde_json::from_value(ctx.params)
                    .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
                runtime.disable(&p.ws).await.map_err(|e| rpc_error(&e))?;
                serde_json::to_value(proto::Ack {}).map_err(|e| RpcError::internal(e.to_string()))
            }
        })
        .method(proto::SYNC_STATUS, move |ctx| {
            let runtime = Arc::clone(&status);
            async move {
                let p: proto::WsParams = serde_json::from_value(ctx.params)
                    .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
                // Never fails on account of the bridge: a status call that errored when the
                // bridge was down would read as an outage of the app server itself.
                let status = runtime.status(&p.ws).await;
                serde_json::to_value(proto::SyncStatusResult {
                    enabled: status.enabled,
                    workspace_id: status.workspace_id,
                    peers: status.peers,
                    paused: status.paused,
                    last_error: status.last_error,
                    bridge_available: status.bridge_available,
                })
                .map_err(|e| RpcError::internal(e.to_string()))
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::testing::StubBridge;
    use crate::test_support;
    use sapphire_ipc::{Client, ClientInfo, Connection, ManagedBy, ServerInfo, serve};
    use sapphire_workspace::{AppContext, AppKind};
    use std::ffi::OsString;
    use std::path::PathBuf;

    static CTX: AppContext = AppContext::new("sapphire-syncmethods");

    /// The env vars `CTX.init` reads.
    const DIR_VARS: [&str; 3] = [
        "SAPPHIRE_SYNCMETHODS_CACHE_DIR",
        "SAPPHIRE_SYNCMETHODS_DATA_DIR",
        "SAPPHIRE_SYNCMETHODS_CONFIG_DIR",
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

    /// A server exposing only the sync namespace, plus a workspace root.
    ///
    /// The environment lock is held for the whole test, as in the crate's other test
    /// modules: the context is a `static` shared by this binary, and every env mutation in
    /// these tests goes through `test_support` for that reason.
    struct Fixture {
        _tmp: tempfile::TempDir,
        _env: EnvGuard,
        root: PathBuf,
        client: Client,
        _stub: StubBridge,
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
        std::fs::create_dir_all(root.join(".sapphire-syncmethods")).unwrap();
        let root = root.canonicalize().unwrap();

        let (stub, bridge) = StubBridge::start().await;
        let runtime = Arc::new(SyncRuntime::new(
            &CTX,
            bridge,
            "/bin/true".into(),
            ManagedBy::Service,
        ));
        let router = Arc::new(sync_router(runtime, sapphire_ipc::Router::new()));

        let (client_conn, server_conn) = Connection::pair();
        tokio::spawn(async move {
            let info = ServerInfo {
                version: "0.0.0".into(),
                pid: std::process::id(),
                managed_by: ManagedBy::Service,
            };
            let _ = serve(server_conn, router, "sapphire-syncmethods", info).await;
        });
        let info = ClientInfo {
            kind: "cli".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        };
        let (client, _) = Client::handshake(client_conn, "sapphire-syncmethods", info)
            .await
            .unwrap();
        Fixture {
            _tmp: tmp,
            _env: env,
            root,
            client,
            _stub: stub,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enabling_returns_the_workspace_id() {
        let f = fixture().await;
        let result: proto::SyncEnableResult = f
            .client
            .call(proto::SYNC_ENABLE, proto::WsParams { ws: f.root.clone() })
            .await
            .unwrap();
        assert!(!result.workspace_id.to_string().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_before_enabling_reports_disabled() {
        let f = fixture().await;
        let status: proto::SyncStatusResult = f
            .client
            .call(proto::SYNC_STATUS, proto::WsParams { ws: f.root.clone() })
            .await
            .unwrap();
        assert!(!status.enabled);
        assert!(status.workspace_id.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_after_enabling_reports_the_same_id() {
        let f = fixture().await;
        let enabled: proto::SyncEnableResult = f
            .client
            .call(proto::SYNC_ENABLE, proto::WsParams { ws: f.root.clone() })
            .await
            .unwrap();
        let status: proto::SyncStatusResult = f
            .client
            .call(proto::SYNC_STATUS, proto::WsParams { ws: f.root.clone() })
            .await
            .unwrap();

        assert!(status.enabled);
        assert_eq!(status.workspace_id, Some(enabled.workspace_id));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabling_an_unsynced_workspace_is_not_an_error() {
        let f = fixture().await;
        let _: proto::Ack = f
            .client
            .call(proto::SYNC_DISABLE, proto::WsParams { ws: f.root.clone() })
            .await
            .expect("disabling what was never enabled is fine");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_directory_that_is_not_a_workspace_is_an_invalid_parameter() {
        let f = fixture().await;
        let plain = f._tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();

        let err = f
            .client
            .call::<_, proto::SyncEnableResult>(proto::SYNC_ENABLE, proto::WsParams { ws: plain })
            .await
            .unwrap_err();
        match err {
            sapphire_ipc::Error::Rpc(e) => {
                assert_eq!(e.code, sapphire_ipc::codes::INVALID_PARAMS, "{}", e.message);
            }
            other => panic!("got {other:?}"),
        }
    }
}
