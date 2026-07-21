//! End-to-end JSON-RPC tests driving the axum router directly via
//! `tower::ServiceExt::oneshot` (no network socket needed).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use sapphire_framework_remote_server::{ServerState, router};
use sapphire_rpc::{
    BlobPutResult, Change, ChangesPullResult, ChangesPushResult, JsonRpcRequest, JsonRpcResponse,
    SearchResult, methods,
};
use serde_json::{Value, json};
use tower::ServiceExt as _;

fn state(token: Option<&str>) -> (tempfile::TempDir, Arc<ServerState>) {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = ServerState::new(tmp.path());
    if let Some(t) = token {
        s = s.with_token(t);
    }
    (tmp, Arc::new(s))
}

/// Issue one JSON-RPC call against a fresh clone of the router and return the
/// decoded response.
async fn call(
    state: &Arc<ServerState>,
    token: Option<&str>,
    method: &str,
    params: Value,
) -> JsonRpcResponse {
    let req = JsonRpcRequest::new(Value::from(1), method, params);
    let mut builder = Request::builder()
        .method("POST")
        .uri("/rpc")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let http_req = builder
        .body(Body::from(serde_json::to_vec(&req).unwrap()))
        .unwrap();

    let response = router(Arc::clone(state)).oneshot(http_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn result<T: for<'de> serde::Deserialize<'de>>(resp: JsonRpcResponse) -> T {
    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    serde_json::from_value(resp.result.expect("result present")).unwrap()
}

#[tokio::test]
async fn push_then_pull_roundtrip() {
    let (_tmp, state) = state(None);
    let ws = "wsA";

    let change = Change::upsert("dir/a.md", "hello world", chrono::Utc::now());
    let push: ChangesPushResult = result(
        call(
            &state,
            None,
            methods::CHANGES_PUSH,
            json!({"ws": ws, "base_cursor": 0, "changes": [change]}),
        )
        .await,
    );
    assert_eq!(push.cursor, 1);
    assert!(push.conflicts.is_empty());

    let pull: ChangesPullResult = result(
        call(
            &state,
            None,
            methods::CHANGES_PULL,
            json!({"ws": ws, "since": 0, "limit": 10}),
        )
        .await,
    );
    assert_eq!(pull.changes.len(), 1);
    assert_eq!(pull.changes[0].path, "dir/a.md");
    assert_eq!(pull.cursor, 1);
}

#[tokio::test]
async fn search_finds_pushed_document() {
    let (_tmp, state) = state(None);
    let ws = "wsSearch";
    let change = Change::upsert("note.md", "the quick brown fox jumps", chrono::Utc::now());
    let _: ChangesPushResult = result(
        call(
            &state,
            None,
            methods::CHANGES_PUSH,
            json!({"ws": ws, "base_cursor": 0, "changes": [change]}),
        )
        .await,
    );

    let search: SearchResult = result(
        call(
            &state,
            None,
            methods::SEARCH_FTS,
            json!({"ws": ws, "q": "quick", "limit": 5}),
        )
        .await,
    );
    assert!(
        search.hits.iter().any(|h| h.path == "note.md"),
        "got {:?}",
        search.hits
    );
}

#[tokio::test]
async fn blob_put_then_get() {
    let (_tmp, state) = state(None);
    let ws = "wsBlob";
    let payload = base64::engine::general_purpose::STANDARD.encode(b"binary-bytes");

    let put: BlobPutResult = result(
        call(
            &state,
            None,
            methods::BLOB_PUT,
            json!({"ws": ws, "bytes_base64": payload}),
        )
        .await,
    );
    assert_eq!(put.len, 12);

    let get = call(
        &state,
        None,
        methods::BLOB_GET,
        json!({"ws": ws, "hash": put.hash}),
    )
    .await;
    let value = get.result.unwrap();
    let b64 = value["bytes_base64"].as_str().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .unwrap();
    assert_eq!(decoded, b"binary-bytes");
}

#[tokio::test]
async fn missing_token_is_unauthorized() {
    let (_tmp, state) = state(Some("secret"));
    // No Authorization header → UNAUTHORIZED error, not a panic.
    let resp = call(
        &state,
        None,
        methods::WORKSPACE_SNAPSHOT,
        json!({"ws": "x"}),
    )
    .await;
    let err = resp.error.expect("expected auth error");
    assert_eq!(err.code, sapphire_rpc::error_codes::UNAUTHORIZED);
}

#[tokio::test]
async fn correct_token_is_accepted() {
    let (_tmp, state) = state(Some("secret"));
    let resp = call(
        &state,
        Some("secret"),
        methods::WORKSPACE_SNAPSHOT,
        json!({"ws": "x"}),
    )
    .await;
    assert!(resp.error.is_none(), "got {:?}", resp.error);
}

#[tokio::test]
async fn unknown_method_reports_method_not_found() {
    let (_tmp, state) = state(None);
    let resp = call(&state, None, "does.not.exist", json!({})).await;
    let err = resp.error.expect("expected error");
    assert_eq!(err.code, sapphire_rpc::error_codes::METHOD_NOT_FOUND);
}
