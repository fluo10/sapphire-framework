//! Integration tests for the IPC layer.

/// The crate must stay free of the workspace, search and sync stacks so that the bridge
/// can use it (spec §2, "the crate carries transport, framing, the router and process
/// startup"). Cargo enforces this; this test states it so a future dependency addition is
/// a deliberate act.
#[test]
fn dependency_surface_is_documented() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        "sapphire-framework-workspace",
        "sapphire-framework-retrieve",
        "sapphire-framework-backend",
        "sapphire-framework-sync",
        "iroh",
        "reqwest",
        "axum",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "sapphire-framework-ipc must not depend on {forbidden}"
        );
    }
}

use std::sync::Arc;

use sapphire_framework_ipc::{
    Client, ClientInfo, Connection, Endpoint, ManagedBy, Router, ServerInfo, serve,
};

fn client_info() -> ClientInfo {
    ClientInfo {
        kind: "cli".into(),
        version: "0.0.0".into(),
        pid: std::process::id(),
    }
}

fn server_info() -> ServerInfo {
    ServerInfo {
        version: "0.0.0".into(),
        pid: std::process::id(),
        managed_by: ManagedBy::Spawned,
    }
}

fn router() -> Arc<Router> {
    Arc::new(
        Router::new()
            .method("echo", |ctx| async move { Ok(ctx.params) })
            .method("announce", |ctx| async move {
                ctx.peer
                    .notify("tick", serde_json::json!({ "n": 1 }))
                    .await
                    .ok();
                Ok(serde_json::json!(null))
            }),
    )
}

/// The in-process carrier: what mobile uses (spec §2.1).
#[tokio::test]
async fn in_process_round_trip() {
    let (client_conn, server_conn) = Connection::pair();
    tokio::spawn(async move {
        let _ = serve(server_conn, router(), "test-app", server_info()).await;
    });
    let (client, _) = Client::handshake(client_conn, "test-app", client_info())
        .await
        .unwrap();

    let echoed: serde_json::Value = client
        .call("echo", serde_json::json!({ "hello": "world" }))
        .await
        .unwrap();
    assert_eq!(echoed, serde_json::json!({ "hello": "world" }));
}

/// A large payload travels as a JSON string (spec §2.2).
#[tokio::test]
async fn a_multi_megabyte_payload_round_trips() {
    let (client_conn, server_conn) = Connection::pair();
    tokio::spawn(async move {
        let _ = serve(server_conn, router(), "test-app", server_info()).await;
    });
    let (client, _) = Client::handshake(client_conn, "test-app", client_info())
        .await
        .unwrap();

    let big = "y".repeat(4 * 1024 * 1024);
    let echoed: serde_json::Value = client
        .call("echo", serde_json::json!({ "content": big }))
        .await
        .unwrap();
    assert_eq!(echoed["content"].as_str().unwrap().len(), 4 * 1024 * 1024);
}

/// Notifications reach every subscriber of a connection.
#[tokio::test]
async fn two_subscribers_both_see_a_notification() {
    let (client_conn, server_conn) = Connection::pair();
    tokio::spawn(async move {
        let _ = serve(server_conn, router(), "test-app", server_info()).await;
    });
    let (client, _) = Client::handshake(client_conn, "test-app", client_info())
        .await
        .unwrap();

    let mut a = client.notifications();
    let mut b = client.notifications();
    let _: serde_json::Value = client
        .call("announce", serde_json::Value::Null)
        .await
        .unwrap();

    for events in [&mut a, &mut b] {
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n.method, "tick");
    }
}

/// The real carrier for this platform, end to end through the listener.
#[tokio::test]
async fn socket_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let endpoint = Endpoint::in_dir("roundtrip-app", tmp.path().to_path_buf());

    #[cfg(unix)]
    let listener = sapphire_framework_ipc::bind(&endpoint).await.unwrap();
    #[cfg(windows)]
    let mut listener = sapphire_framework_ipc::bind(&endpoint).unwrap();

    tokio::spawn(async move {
        loop {
            let Ok(conn) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _ = serve(conn, router(), "roundtrip-app", server_info()).await;
            });
        }
    });

    let conn = sapphire_framework_ipc::connect(&endpoint).await.unwrap();
    let (client, info) = Client::handshake(conn, "roundtrip-app", client_info())
        .await
        .unwrap();
    assert_eq!(info.managed_by, ManagedBy::Spawned);

    let echoed: serde_json::Value = client
        .call("echo", serde_json::json!([1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(echoed, serde_json::json!([1, 2, 3]));
}

/// Two clients on separate connections are served at the same time — the property the
/// whole design exists for: one server, several callers, no lock contention.
#[tokio::test(flavor = "multi_thread")]
async fn two_clients_are_served_concurrently() {
    let tmp = tempfile::tempdir().unwrap();
    let endpoint = Endpoint::in_dir("concurrent-app", tmp.path().to_path_buf());

    #[cfg(unix)]
    let listener = sapphire_framework_ipc::bind(&endpoint).await.unwrap();
    #[cfg(windows)]
    let mut listener = sapphire_framework_ipc::bind(&endpoint).unwrap();

    tokio::spawn(async move {
        loop {
            let Ok(conn) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _ = serve(conn, router(), "concurrent-app", server_info()).await;
            });
        }
    });

    let mut tasks = Vec::new();
    for n in 0..2u32 {
        let endpoint = endpoint.clone();
        tasks.push(tokio::spawn(async move {
            let conn = sapphire_framework_ipc::connect(&endpoint).await.unwrap();
            let (client, _) = Client::handshake(conn, "concurrent-app", client_info())
                .await
                .unwrap();
            let echoed: u32 = client.call("echo", n).await.unwrap();
            echoed
        }));
    }

    let mut seen = Vec::new();
    for task in tasks {
        seen.push(task.await.unwrap());
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1]);
}
