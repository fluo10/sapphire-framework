//! End-to-end JSON-RPC tests driving the axum router directly via
//! `tower::ServiceExt::oneshot` (no network socket needed).

// `ServerState::with_resolver` hands back the crate's `Error`, which is a large
// enum — every `Result`-returning item in this crate already trips
// `result_large_err`, so every resolver closure written against the hook does
// too. Shrinking `Error` is pre-existing housekeeping, not this file's job.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use sapphire_framework_remote_server::{
    Authenticated, KeyStore, ServerState, WsStoreConfig, protect, router, serve,
};
use sapphire_rpc::{
    BlobPutResult, Change, ChangesPullResult, ChangesPushResult, JsonRpcRequest, JsonRpcResponse,
    SearchResult, SnapshotResult, methods,
};
use serde_json::{Value, json};
use tower::ServiceExt as _;

/// 鍵を設定した（`Some`）／認証を明示的に外した（`None`）サーバ状態。
///
/// `None` の側が `insecure_for_tests()` を呼ぶのは飾りではない。鍵ストアの無い
/// ルータは既定で全リクエストを 503 で拒否するので、この明示的な逃げ道を通ら
/// なければ以下のテストは 1 つも通らない。
fn state(token: Option<&str>) -> (tempfile::TempDir, Arc<ServerState>) {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = ServerState::new(tmp.path());
    match token {
        // テストは固定トークンを使いたいので、生成ではなく直接書いた鍵を読ませる。
        Some(t) => {
            let key_path = tmp.path().join("keys.toml");
            std::fs::write(&key_path, format!("[[key]]\ntoken = \"{t}\"\n")).unwrap();
            s = s.with_keys(Arc::new(KeyStore::load(&key_path).unwrap()));
        }
        None => s = s.insecure_for_tests(),
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
    let (_tmp, st) = state(Some("secret"));
    // No Authorization header → HTTP 401, not a panic.
    let req = JsonRpcRequest::new(
        Value::from(1),
        methods::WORKSPACE_SNAPSHOT,
        json!({"ws": "x"}),
    );
    let http_req = Request::builder()
        .method("POST")
        .uri("/rpc")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&req).unwrap()))
        .unwrap();

    let response = router(Arc::clone(&st)).oneshot(http_req).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_bad_token_is_rejected_with_http_401() {
    let (_t, st) = state(Some("sjt_secret"));
    let req = JsonRpcRequest::new(
        Value::from(1),
        methods::WORKSPACE_SNAPSHOT,
        json!({"ws": "w"}),
    );
    let http_req = Request::builder()
        .method("POST")
        .uri("/rpc")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer wrong")
        .body(Body::from(serde_json::to_vec(&req).unwrap()))
        .unwrap();

    let response = router(Arc::clone(&st)).oneshot(http_req).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn protect_guards_a_foreign_route_with_the_same_key() {
    let (_t, st) = state(Some("sjt_secret"));
    let app = protect(
        Arc::clone(&st),
        Router::new().route("/mcp", axum::routing::get(|| async { "ok" })),
    );

    let unauthorized = app
        .clone()
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = app
        .oneshot(
            Request::builder()
                .uri("/mcp")
                .header(header::AUTHORIZATION, "Bearer sjt_secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
}

#[tokio::test]
async fn an_authenticated_request_carries_the_key_id() {
    let (_t, st) = state(Some("sjt_secret"));
    let key_id = st.keys().unwrap().entries()[0].id;

    let app = protect(
        Arc::clone(&st),
        Router::new().route(
            "/whoami",
            axum::routing::get(
                |axum::Extension(who): axum::Extension<Authenticated>| async move {
                    who.key_id.to_string()
                },
            ),
        ),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/whoami")
                .header(header::AUTHORIZATION, "Bearer sjt_secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        String::from_utf8(body.to_vec()).unwrap(),
        key_id.to_string()
    );
}

#[tokio::test]
async fn serve_refuses_to_start_without_a_usable_key() {
    // `state(None)` は `insecure_for_tests()` 付き。それでも `serve` は起動を
    // 拒否する — 逃げ道はルータを組むテストのためのもので、本番の待ち受けを
    // 無認証にする手段ではない。
    let (_t, st) = state(None);

    // ポートを掴む前に弾かれるので、bind せずに戻ってくる。
    let err = serve("127.0.0.1:0".parse().unwrap(), st).await.unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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

#[tokio::test]
async fn snapshot_reports_a_stable_generation() {
    let (_t, st) = state(None);
    let first = call(&st, None, methods::WORKSPACE_SNAPSHOT, json!({"ws": "w"})).await;
    let second = call(&st, None, methods::WORKSPACE_SNAPSHOT, json!({"ws": "w"})).await;

    let g1 = serde_json::from_value::<SnapshotResult>(first.result.unwrap())
        .unwrap()
        .generation;
    let g2 = serde_json::from_value::<SnapshotResult>(second.result.unwrap())
        .unwrap()
        .generation;

    assert_eq!(g1, g2);
    assert_eq!(g1.get_version_num(), 7, "generation は UUIDv7");
}

#[tokio::test]
async fn pull_with_a_foreign_generation_is_rejected() {
    let (_t, st) = state(None);
    let response = call(
        &st,
        None,
        methods::CHANGES_PULL,
        json!({"ws": "w", "since": 0, "limit": 10, "generation": uuid::Uuid::nil()}),
    )
    .await;

    let err = response.error.expect("expected an error");
    assert_eq!(err.code, sapphire_rpc::error_codes::GENERATION_MISMATCH);
}

#[tokio::test]
async fn pull_without_a_generation_is_accepted() {
    let (_t, st) = state(None);
    let response = call(
        &st,
        None,
        methods::CHANGES_PULL,
        json!({"ws": "w", "since": 0, "limit": 10}),
    )
    .await;
    assert!(response.error.is_none());
}

#[tokio::test]
async fn blob_get_cannot_be_steered_at_an_arbitrary_file() {
    // `blob.get` は認証済みクライアントなら誰でも叩けるので、hash を検証しない
    // 限りサーバプロセスが読めるファイルは何でも読めてしまう — 平文の keys.toml
    // を含めて。
    let (tmp, st) = state(None);
    let secret = tmp.path().join("keys.toml");
    std::fs::write(&secret, "[[key]]\ntoken = \"sjt_topsecret\"\n").unwrap();

    let ws = "wsTraversal";
    // 先にワークスペースを開かせて、blob ルートを実在させる。
    let _: SnapshotResult =
        result(call(&st, None, methods::WORKSPACE_SNAPSHOT, json!({"ws": ws})).await);

    for hash in [
        "../../../keys.toml",
        "./../../../keys.toml",
        secret.to_str().unwrap(),
        "/etc/passwd",
        // 多バイト文字はシャードの `hash[0..2]` スライスを panic させていた。
        "€uro",
        "deadbeef",
    ] {
        let response = call(
            &st,
            None,
            methods::BLOB_GET,
            json!({"ws": ws, "hash": hash}),
        )
        .await;
        let err = response
            .error
            .unwrap_or_else(|| panic!("blob.get({hash:?}) must be rejected, got a result"));
        assert_eq!(
            err.code,
            sapphire_rpc::error_codes::INVALID_PARAMS,
            "blob.get({hash:?}) は不正なアドレスとして弾く"
        );
    }

    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "[[key]]\ntoken = \"sjt_topsecret\"\n"
    );
}

#[tokio::test]
async fn blob_get_still_reports_a_well_formed_but_absent_address_as_none() {
    let (_t, st) = state(None);
    let response = call(
        &st,
        None,
        methods::BLOB_GET,
        json!({"ws": "wsAbsent", "hash": "0".repeat(64)}),
    )
    .await;

    assert!(response.error.is_none(), "got {:?}", response.error);
    assert!(response.result.unwrap()["bytes_base64"].is_null());
}

#[tokio::test]
async fn a_router_without_a_key_store_refuses_every_request() {
    // 鍵の設定漏れは「素通し」ではなく「何も通さない」に倒す。認証を外すのは
    // insecure_for_tests() を明示的に呼んだときだけ。
    let tmp = tempfile::tempdir().unwrap();
    let st = Arc::new(ServerState::new(tmp.path()));

    let req = JsonRpcRequest::new(
        Value::from(1),
        methods::WORKSPACE_SNAPSHOT,
        json!({"ws": "x"}),
    );
    let response = router(Arc::clone(&st))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rpc")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn protect_without_a_key_store_refuses_the_app_s_own_routes() {
    // アプリが /rpc と /mcp を同じポートに載せるとき、アプリは自前の listener を
    // 持つので `serve` の「鍵が無ければ起動しない」検査を通らない。保証は
    // レイヤ側に無ければ意味がない。
    let tmp = tempfile::tempdir().unwrap();
    let st = Arc::new(ServerState::new(tmp.path()));

    let app = protect(
        Arc::clone(&st),
        Router::new().route("/mcp", axum::routing::get(|| async { "ok" })),
    );

    let response = app
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "鍵ストアが無いのに素通ししてはならない"
    );
}

#[tokio::test]
async fn insecure_for_tests_is_the_only_way_through_without_keys() {
    let (_t, st) = state(None);
    assert!(st.is_insecure());

    let app = protect(
        Arc::clone(&st),
        Router::new().route("/mcp", axum::routing::get(|| async { "ok" })),
    );
    let response = app
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_resolver_provided_store_round_trips_an_app_dir_push_over_http() {
    // `with_config` / `app_dir` の許可リストがサーブされる API から効くことを、
    // WsStore を直接叩かずに /rpc 越しに確かめる。resolver フックが無ければ
    // ServerState は WsStore::open しか作れず、app_dir は常に None になる。
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let resolver_root = root.clone();

    let st = Arc::new(
        ServerState::new(&root)
            .insecure_for_tests()
            .with_resolver(move |ws| {
                let origin = resolver_root.join("journals").join(ws);
                Ok(WsStoreConfig {
                    origin_dir: origin.clone(),
                    state_dir: origin.join(".sapphire-journal").join("server"),
                    retrieve: None,
                    app_dir: Some(".sapphire-journal".to_owned()),
                })
            }),
    );

    let ws = "diary";
    let change = Change::upsert(
        ".sapphire-journal/config.toml",
        "grain = \"day\"\n",
        chrono::Utc::now(),
    );
    let push: ChangesPushResult = result(
        call(
            &st,
            None,
            methods::CHANGES_PUSH,
            json!({"ws": ws, "base_cursor": 0, "changes": [change]}),
        )
        .await,
    );
    assert_eq!(push.cursor, 1);
    assert!(push.conflicts.is_empty());

    // resolver の指したディレクトリに落ちていること（既定レイアウトの
    // `<data_dir>/origin/<ws>/` ではない）。
    let written = root
        .join("journals")
        .join(ws)
        .join(".sapphire-journal")
        .join("config.toml");
    assert_eq!(
        std::fs::read_to_string(&written).unwrap(),
        "grain = \"day\"\n"
    );
    assert!(
        !root.join("origin").join(ws).exists(),
        "resolver があるなら既定レイアウトは掘らない"
    );

    // 同じ /rpc から読み戻せること。
    let snapshot: SnapshotResult =
        result(call(&st, None, methods::WORKSPACE_SNAPSHOT, json!({"ws": ws})).await);
    assert_eq!(snapshot.docs.len(), 1);
    assert_eq!(snapshot.docs[0].path, ".sapphire-journal/config.toml");

    // 許可したのは 1 つだけ。他の隠しディレクトリは resolver 経由でも通らない。
    let denied = call(
        &st,
        None,
        methods::CHANGES_PUSH,
        json!({"ws": ws, "base_cursor": 1, "changes": [
            Change::upsert(".git/config", "gotcha", chrono::Utc::now())
        ]}),
    )
    .await;
    assert_eq!(
        denied.error.expect("expected an error").code,
        sapphire_rpc::error_codes::INVALID_PARAMS
    );
}

#[tokio::test]
async fn a_resolver_can_refuse_an_unknown_workspace() {
    let tmp = tempfile::tempdir().unwrap();
    let st = Arc::new(
        ServerState::new(tmp.path())
            .insecure_for_tests()
            .with_resolver(|ws| {
                Err(sapphire_framework_remote_server::Error::NotSyncable(
                    ws.to_owned(),
                ))
            }),
    );

    let response = call(
        &st,
        None,
        methods::WORKSPACE_SNAPSHOT,
        json!({"ws": "nope"}),
    )
    .await;

    assert!(
        response.error.is_some(),
        "未知の ws は拒否できなければならない"
    );
}
