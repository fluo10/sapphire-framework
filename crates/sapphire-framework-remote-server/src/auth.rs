//! 鍵による認証を tower layer として提供する。
//!
//! framework のルートと、アプリが同じプロセスで生やす自前のルート（MCP など）に
//! **同じ鍵**をかけられるようにするためのもの。`/rpc` は守られているのに `/mcp` は
//! 素通し、という事故を避ける。

use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    middleware::{Next, from_fn_with_state},
    response::Response,
};
use uuid::Uuid;

use crate::ServerState;

/// 認証に成功したリクエストの拡張として入る値。
///
/// 将来 `Change` に書き込み元を持たせるときは、rpc 型に 1 フィールド足して
/// ここの `key_id` を読むだけでよい。
#[derive(Clone, Debug)]
pub struct Authenticated {
    pub key_id: Uuid,
    pub label: Option<String>,
}

/// `router` を `state` の鍵で保護して返す。鍵が設定されていない場合は素通しし、
/// 警告を出す（テスト用途のみ。`serve` は鍵なしの起動を拒否する）。
pub fn protect(state: Arc<ServerState>, router: Router) -> Router {
    if state.keys().is_none() {
        tracing::warn!("no key store configured; this router is unauthenticated");
        return router;
    }
    router.layer(from_fn_with_state(state, authenticate))
}

async fn authenticate(
    State(state): State<Arc<ServerState>>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, StatusCode> {
    let Some(keys) = state.keys() else {
        return Ok(next.run(request).await);
    };

    let presented = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let entry = keys
        .authenticate(presented)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let who = Authenticated {
        key_id: entry.id,
        label: entry.label.clone(),
    };

    request.extensions_mut().insert(who);
    Ok(next.run(request).await)
}
