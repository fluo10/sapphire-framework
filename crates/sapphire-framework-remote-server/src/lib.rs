//! axum JSON-RPC server for sapphire-framework remote sync + search.
//!
//! The server is **symmetric** with a client (Model B, see
//! `docs/ARCHITECTURE.md`): every workspace has a file **origin**, a redb
//! **retrieve cache**, an append-only **change log**, and a content-addressed
//! **blob store**. Clients converge by pulling changes newer than their cursor
//! and pushing their own, resolved last-writer-wins.
//!
//! A single endpoint — `POST /rpc` — speaks JSON-RPC 2.0 using the shared types
//! in [`sapphire_rpc`]. Build a router with [`router`] (handy for tests via
//! `tower::ServiceExt::oneshot`) or run one with [`serve`].
//!
//! ```no_run
//! # async fn run() -> std::io::Result<()> {
//! use std::sync::Arc;
//! use sapphire_framework_remote_server::{KeyStore, serve, ServerState};
//!
//! let keys = Arc::new(KeyStore::load(std::path::Path::new("/etc/sapphire/keys.toml")).unwrap());
//! let state = Arc::new(ServerState::new("/var/lib/sapphire").with_keys(keys));
//! serve("127.0.0.1:8080".parse().unwrap(), state).await
//! # }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::State, routing::post};
use base64::Engine as _;
use sapphire_rpc::{
    BlobGetParams, BlobGetResult, BlobPutParams, BlobPutResult, ChangesPullParams,
    ChangesPushParams, JsonRpcError, JsonRpcRequest, JsonRpcResponse, SearchParams, SearchResult,
    SnapshotParams, error_codes, methods,
};
use serde::Serialize;
use serde_json::Value;

mod auth;
mod change_log;
mod error;
mod keys;
mod ws_store;

pub use auth::{Authenticated, protect};
pub use change_log::ChangeLog;
pub use error::{Error, Result};
pub use keys::{KeyEntry, KeyStore};
pub use ws_store::{Detection, ReconcileReport, WsStore, WsStoreConfig, is_syncable};

/// ワークスペース名から [`WsStoreConfig`] を解決するフック。
///
/// アプリが既に持っているワークスペースの上でサーバを動かすための差し込み口。
type Resolver = Box<dyn Fn(&str) -> Result<WsStoreConfig> + Send + Sync>;

/// Shared server state: a base data directory, an optional key store, and a
/// lazily-populated map of open workspaces.
pub struct ServerState {
    data_dir: PathBuf,
    keys: Option<Arc<KeyStore>>,
    resolver: Option<Resolver>,
    insecure: bool,
    workspaces: Mutex<HashMap<String, Arc<WsStore>>>,
}

impl ServerState {
    /// Create server state rooted at `data_dir`. Workspaces are opened on first
    /// use under `data_dir/{origin,cache,changelog,blobs}/<ws>`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            keys: None,
            resolver: None,
            insecure: false,
            workspaces: Mutex::new(HashMap::new()),
        }
    }

    /// `Authorization: Bearer <token>` を、この鍵ストアに対して検証させる。
    pub fn with_keys(mut self, keys: Arc<KeyStore>) -> Self {
        self.keys = Some(keys);
        self
    }

    /// ワークスペース名から [`WsStoreConfig`] を解決する関数を差し替える。
    ///
    /// 既定（未設定）では [`WsStore::open`] のレイアウト
    /// (`data_dir/{origin,cache,changelog,blobs,track}/<ws>`) をそのまま使う。
    /// 差し替えると、アプリが既に持っているワークスペースのディレクトリ・
    /// retrieve ストア・`app_dir` 許可リストの上でサーバを動かせる — つまり
    /// `/rpc` から届く push が、アプリ自身が MCP で書くのと同じファイル群に
    /// 落ちる。これが無いと `WsStore::with_config` はサーブされる API からは
    /// 到達不能で、`app_dir` の許可リストも効かせようがない。
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use sapphire_framework_remote_server::{ServerState, WsStoreConfig};
    /// let root = std::path::PathBuf::from("/srv/journals");
    /// let state = ServerState::new(&root).with_resolver(move |ws| {
    ///     Ok(WsStoreConfig {
    ///         origin_dir: root.join(ws),
    ///         state_dir: root.join(ws).join(".sapphire-journal/server"),
    ///         retrieve: None,
    ///         app_dir: Some(".sapphire-journal".to_owned()),
    ///     })
    /// });
    /// ```
    ///
    /// 解決関数はワークスペースごとに一度だけ、初回アクセス時に呼ばれる。
    /// 未知のワークスペースは `Err` を返して拒否できる。
    pub fn with_resolver(
        mut self,
        f: impl Fn(&str) -> Result<WsStoreConfig> + Send + Sync + 'static,
    ) -> Self {
        self.resolver = Some(Box::new(f));
        self
    }

    /// 鍵ストア無しで**素通しの**ルータを作ることを明示的に許可する。テスト専用。
    ///
    /// 既定では鍵ストアが無いルータは全リクエストを 503 で拒否する
    /// ([`protect`] 参照)。この逃げ道に名前を与えてあるのは、「鍵を設定し忘れた」
    /// と「認証を意図的に外した」がコード上で見分けられるようにするため。
    /// [`serve`] はこのフラグを見ない — 鍵の無い待ち受けは依然として拒否する。
    pub fn insecure_for_tests(mut self) -> Self {
        self.insecure = true;
        self
    }

    /// 設定されている鍵ストア。
    pub fn keys(&self) -> Option<&Arc<KeyStore>> {
        self.keys.as_ref()
    }

    /// [`insecure_for_tests`](Self::insecure_for_tests) が呼ばれているか。
    pub fn is_insecure(&self) -> bool {
        self.insecure
    }

    /// Get (opening if necessary) the store for workspace `ws`.
    ///
    /// 同じプロセスでアプリ自身のルート（MCP など）を生やす場合、そちらの
    /// ハンドラもこれで同じ [`WsStore`] を掴み、ファイルを書いたあと
    /// [`WsStore::record_local_write`] を呼ぶ。`/rpc` と同じインスタンスなので
    /// change log は 1 本に保たれる。
    pub fn workspace(&self, ws: &str) -> Result<Arc<WsStore>> {
        let mut map = self.workspaces.lock().unwrap();
        if let Some(store) = map.get(ws) {
            return Ok(Arc::clone(store));
        }
        let store = Arc::new(match &self.resolver {
            Some(resolve) => WsStore::with_config(resolve(ws)?)?,
            None => WsStore::open(&self.data_dir, ws)?,
        });
        map.insert(ws.to_owned(), Arc::clone(&store));
        Ok(store)
    }
}

/// Build the axum router for `state` (single `POST /rpc` endpoint). 認証は適用済み。
pub fn router(state: Arc<ServerState>) -> Router {
    let routes = Router::new()
        .route("/rpc", post(rpc_handler))
        .with_state(Arc::clone(&state));
    crate::auth::protect(state, routes)
}

/// Bind `addr` and serve until the process is stopped.
///
/// 有効な鍵が 1 件も無い場合は起動を拒否する。認証なしで待ち受ける状態を作らない。
pub async fn serve(addr: SocketAddr, state: Arc<ServerState>) -> std::io::Result<()> {
    match state.keys() {
        Some(keys) if keys.has_usable_key() => {}
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no usable API key configured; write a key file containing \
                 `[[key]]` / `token = \"...\"` and pass it to \
                 `ServerState::with_keys` (the remaining fields are filled in \
                 on load)",
            ));
        }
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "sapphire remote server listening");
    axum::serve(listener, router(state)).await
}

async fn rpc_handler(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let id = req.id.clone();

    match dispatch(state, req).await {
        Ok(result) => Json(JsonRpcResponse::ok(id, result)),
        Err(err) => Json(JsonRpcResponse::err(id, err)),
    }
}

/// Route one request to its handler, returning the JSON result value or a
/// JSON-RPC error.
async fn dispatch(state: Arc<ServerState>, req: JsonRpcRequest) -> std::result::Result<Value, JsonRpcError> {
    match req.method.as_str() {
        methods::WORKSPACE_SNAPSHOT => {
            let p: SnapshotParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            run(move || store.snapshot()).await.and_then(to_value)
        }
        methods::CHANGES_PULL => {
            let p: ChangesPullParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            let claimed = p.generation;
            run(move || match generation_error(&store, claimed) {
                Some(e) => Err(e),
                None => store.pull(p.since, p.limit),
            })
            .await
            .and_then(to_value)
        }
        methods::CHANGES_PUSH => {
            let p: ChangesPushParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            let claimed = p.generation;
            run(move || match generation_error(&store, claimed) {
                Some(e) => Err(e),
                None => store.push(p.base_cursor, p.changes),
            })
            .await
            .and_then(to_value)
        }
        methods::BLOB_PUT => {
            let p: BlobPutParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(p.bytes_base64.as_bytes())
                .map_err(|e| JsonRpcError::new(error_codes::INVALID_PARAMS, format!("bad base64: {e}")))?;
            let r = run(move || store.blob_put(&bytes)).await?;
            to_value(BlobPutResult {
                hash: r.hash,
                len: r.len,
            })
        }
        methods::BLOB_GET => {
            let p: BlobGetParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            let hash = p.hash.clone();
            let bytes = run(move || store.blob_get(&hash)).await?;
            to_value(BlobGetResult {
                bytes_base64: bytes
                    .map(|b| base64::engine::general_purpose::STANDARD.encode(b)),
            })
        }
        methods::SEARCH_FTS | methods::SEARCH_SEMANTIC => {
            // Semantic search falls back to FTS for now: the server has no
            // embedder configured in the MVP (see docs/ARCHITECTURE.md).
            let p: SearchParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            let hits = run(move || store.search_fts(&p.q, p.limit)).await?;
            to_value(SearchResult { hits })
        }
        other => Err(JsonRpcError::new(
            error_codes::METHOD_NOT_FOUND,
            format!("unknown method '{other}'"),
        )),
    }
}

/// Deserialize method params, mapping failures to an `INVALID_PARAMS` error.
fn parse_params<T: for<'de> serde::Deserialize<'de>>(
    params: Value,
) -> std::result::Result<T, JsonRpcError> {
    serde_json::from_value(params)
        .map_err(|e| JsonRpcError::new(error_codes::INVALID_PARAMS, e.to_string()))
}

/// Open (or reuse) a workspace store, mapping failures to an internal error.
fn open_ws(state: &Arc<ServerState>, ws: &str) -> std::result::Result<Arc<WsStore>, JsonRpcError> {
    state.workspace(ws).map_err(|e| e.to_jsonrpc())
}

/// クライアントが世代を名乗ってきたときだけ照合し、食い違い（または読み出しの
/// 失敗）があればそのエラーを返す。名乗らないクライアントは当面そのまま通す。
///
/// `generation()` は redb への同期読みなので、これは `dispatch` の他のストア
/// 呼び出しと同じ `spawn_blocking` クロージャの中から呼ぶ — `pull` のホット
/// パス上で async executor を塞がないため。
fn generation_error(store: &WsStore, claimed: Option<uuid::Uuid>) -> Option<Error> {
    let claimed = claimed?;
    match store.generation() {
        Ok(actual) if actual == claimed => None,
        Ok(actual) => Some(Error::GenerationMismatch { actual, claimed }),
        Err(e) => Some(e),
    }
}

/// Run a blocking store operation on the blocking pool and map its error.
async fn run<T, F>(f: F) -> std::result::Result<T, JsonRpcError>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(e.to_jsonrpc()),
        Err(join) => Err(JsonRpcError::new(
            error_codes::INTERNAL_ERROR,
            format!("task panicked: {join}"),
        )),
    }
}

/// Serialize a handler result to a JSON value.
fn to_value<T: Serialize>(value: T) -> std::result::Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|e| JsonRpcError::new(error_codes::INTERNAL_ERROR, e.to_string()))
}
