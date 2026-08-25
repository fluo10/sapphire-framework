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

/// `router` を `state` の鍵で保護して返す。
///
/// 鍵ストアが設定されていない場合は**閉じる**：全リクエストを 503 で拒否する
/// レイヤを被せる。素通しにはしない。
///
/// 「鍵が無ければ待ち受けない」保証を [`serve`](crate::serve) だけに置くのでは
/// 足りない — このブランチが可能にした構成、つまりアプリが `/rpc` と `/mcp` を
/// 同じポートに載せる場合、アプリは自前の listener を持つので `serve` を通らず
/// `axum::serve(listener, router(state).merge(protect(state, mcp)))` を呼ぶ。
/// 保証はレイヤ側に無ければ意味がない。
///
/// 401 ではなく 503 なのは、これが資格情報の問題ではなくサーバの設定漏れだから。
/// クライアントに再試行や鍵の入れ直しを促すべき状況ではない。
///
/// 意図して認証を外したい場合は [`ServerState::insecure_for_tests`] を呼ぶ。
/// 逃げ道に名前を与えてあるので、設定漏れと区別がつく。
pub fn protect(state: Arc<ServerState>, router: Router) -> Router {
    if state.keys().is_none() {
        if state.is_insecure() {
            tracing::warn!(
                "ServerState::insecure_for_tests() is set; this router is unauthenticated"
            );
            return router;
        }
        tracing::error!("no key store configured; this router will refuse every request");
        return router.layer(from_fn_with_state(state, refuse));
    }
    router.layer(from_fn_with_state(state, authenticate))
}

/// 鍵ストアが無いときに被せるレイヤ。何も通さない。
async fn refuse(
    State(_state): State<Arc<ServerState>>,
    _request: Request,
    _next: Next,
) -> std::result::Result<Response, StatusCode> {
    Err(StatusCode::SERVICE_UNAVAILABLE)
}

async fn authenticate(
    State(state): State<Arc<ServerState>>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, StatusCode> {
    // `protect` はこのレイヤを鍵ストアがあるときにしか被せないので、ここは
    // 到達しない。到達したなら鍵ストアが実行中に消えたということ（将来の
    // ホットリロード等）なので、素通しではなく拒否する。
    let Some(keys) = state.keys() else {
        tracing::error!("key store vanished while the auth layer was installed; refusing");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
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
