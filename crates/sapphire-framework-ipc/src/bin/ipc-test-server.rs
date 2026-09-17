//! A minimal server used by the integration tests.
//!
//! Usage: `ipc-test-server <runtime-dir> <app-name> [--version <v>] [--service]
//! [--shutdownable]`
//!
//! Serves `ping` (returns `"pong"`), `pid` (returns this process's id) and `sleep`
//! (waits for `params.ms` milliseconds), then exits when the listener is dropped.
//! With `--shutdownable` it also serves [`SHUTDOWN_METHOD`] and exits when it is
//! called, the way a spawned server retires (spec §2.6).

use std::sync::Arc;

use sapphire_framework_ipc::{Endpoint, ManagedBy, Router, SHUTDOWN_METHOD, ServerInfo, serve};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("a runtime directory");
    let app = args.next().expect("an app name");
    let mut version = "0.0.0".to_owned();
    let mut managed_by = ManagedBy::Spawned;
    let mut shutdownable = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" => version = args.next().expect("a version"),
            "--service" => managed_by = ManagedBy::Service,
            "--shutdownable" => shutdownable = true,
            other => panic!("unexpected argument {other}"),
        }
    }

    let endpoint = Endpoint::in_dir(app.clone(), dir.into());
    let info = ServerInfo {
        version,
        pid: std::process::id(),
        managed_by,
    };
    let mut router = Router::new()
        .method("ping", |_| async move { Ok(serde_json::json!("pong")) })
        .method("pid", |_| async move {
            Ok(serde_json::json!(std::process::id()))
        })
        .method("sleep", |ctx| async move {
            let ms = ctx.params.get("ms").and_then(|v| v.as_u64()).unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            Ok(serde_json::json!(ms))
        });
    if shutdownable {
        // A spawned server retires by exiting the whole process. Reply first; the
        // short delay only makes sure the response is out before the process dies.
        router = router.method(SHUTDOWN_METHOD, |_| async move {
            tokio::spawn(async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                std::process::exit(0);
            });
            Ok(serde_json::json!(true))
        });
    }
    let router = Arc::new(router);

    serve_forever(&endpoint, router, &app, info).await
}

#[cfg(unix)]
async fn serve_forever(
    endpoint: &Endpoint,
    router: Arc<Router>,
    app: &str,
    info: ServerInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = sapphire_framework_ipc::bind(endpoint).await?;
    loop {
        let conn = listener.accept().await?;
        let (router, app, info) = (Arc::clone(&router), app.to_owned(), info.clone());
        tokio::spawn(async move {
            let _ = serve(conn, router, &app, info).await;
        });
    }
}

#[cfg(windows)]
async fn serve_forever(
    endpoint: &Endpoint,
    router: Arc<Router>,
    app: &str,
    info: ServerInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = sapphire_framework_ipc::bind(endpoint)?;
    loop {
        let conn = listener.accept().await?;
        let (router, app, info) = (Arc::clone(&router), app.to_owned(), info.clone());
        tokio::spawn(async move {
            let _ = serve(conn, router, &app, info).await;
        });
    }
}
