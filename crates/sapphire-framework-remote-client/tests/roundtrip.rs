//! Round-trip tests: a real `RemoteClient` against a live in-process server.

use std::sync::Arc;

use sapphire_framework_remote_client::RemoteClient;
use sapphire_remote_server::{ServerState, router};
use sapphire_rpc::Change;

/// Start a server on an ephemeral port and return its base URL + tempdir guard.
async fn start_server(token: Option<&str>) -> (tempfile::TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = ServerState::new(tmp.path());
    if let Some(t) = token {
        state = state.with_token(t);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(Arc::new(state))).await.unwrap();
    });
    (tmp, format!("http://{addr}"))
}

#[tokio::test]
async fn client_push_pull_blob_search_roundtrip() {
    let (_tmp, url) = start_server(None).await;
    let client = RemoteClient::new(url);
    let ws = "roundtrip";

    // push
    let change = Change::upsert("note.md", "the quick brown fox", chrono::Utc::now());
    let push = client.push(ws, 0, vec![change]).await.unwrap();
    assert_eq!(push.cursor, 1);
    assert!(push.conflicts.is_empty());

    // pull
    let pull = client.pull(ws, 0, 10).await.unwrap();
    assert_eq!(pull.changes.len(), 1);
    assert_eq!(pull.changes[0].path, "note.md");

    // blob
    let put = client.blob_put(ws, b"binary-payload").await.unwrap();
    let got = client.blob_get(ws, &put.hash).await.unwrap();
    assert_eq!(got.as_deref(), Some(&b"binary-payload"[..]));
    // Missing blob → None.
    assert!(client.blob_get(ws, "0000").await.unwrap().is_none());

    // search
    let hits = client.search_fts(ws, "quick", 5).await.unwrap();
    assert!(hits.iter().any(|h| h.path == "note.md"), "got {hits:?}");
}

#[tokio::test]
async fn bad_token_surfaces_rpc_error() {
    let (_tmp, url) = start_server(Some("secret")).await;
    let client = RemoteClient::new(url).with_token("wrong");
    let err = client.snapshot("x").await.unwrap_err();
    match err {
        sapphire_framework_remote_client::Error::Rpc { code, .. } => {
            assert_eq!(code, sapphire_rpc::error_codes::UNAUTHORIZED);
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }
}
