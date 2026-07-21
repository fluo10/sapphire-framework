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
//! use sapphire_framework_remote_server::{serve, ServerState};
//!
//! let state = Arc::new(ServerState::new("/var/lib/sapphire").with_token("secret"));
//! serve("127.0.0.1:8080".parse().unwrap(), state).await
//! # }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::State,
    http::HeaderMap,
    routing::post,
};
use base64::Engine as _;
use sapphire_rpc::{
    BlobGetParams, BlobGetResult, BlobPutParams, BlobPutResult, ChangesPullParams,
    ChangesPushParams, JsonRpcError, JsonRpcRequest, JsonRpcResponse, SearchParams, SearchResult,
    SnapshotParams, error_codes, methods,
};
use serde::Serialize;
use serde_json::Value;

mod change_log;
mod error;
mod ws_store;

pub use change_log::ChangeLog;
pub use error::{Error, Result};
pub use ws_store::WsStore;

/// Shared server state: a base data directory, an optional bearer token, and a
/// lazily-populated map of open workspaces.
pub struct ServerState {
    data_dir: PathBuf,
    token: Option<String>,
    workspaces: Mutex<HashMap<String, Arc<WsStore>>>,
}

impl ServerState {
    /// Create server state rooted at `data_dir`. Workspaces are opened on first
    /// use under `data_dir/{origin,cache,changelog,blobs}/<ws>`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            token: None,
            workspaces: Mutex::new(HashMap::new()),
        }
    }

    /// Require callers to present `Authorization: Bearer <token>`.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Get (opening if necessary) the store for workspace `ws`.
    fn workspace(&self, ws: &str) -> Result<Arc<WsStore>> {
        let mut map = self.workspaces.lock().unwrap();
        if let Some(store) = map.get(ws) {
            return Ok(Arc::clone(store));
        }
        let store = Arc::new(WsStore::open(&self.data_dir, ws)?);
        map.insert(ws.to_owned(), Arc::clone(&store));
        Ok(store)
    }

    /// Check the `Authorization` header against the configured token. When no
    /// token is configured, all requests are allowed.
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = &self.token else {
            return true;
        };
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|got| got == expected)
            .unwrap_or(false)
    }
}

/// Build the axum router for `state` (single `POST /rpc` endpoint).
pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/rpc", post(rpc_handler))
        .with_state(state)
}

/// Bind `addr` and serve until the process is stopped.
pub async fn serve(addr: SocketAddr, state: Arc<ServerState>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "sapphire remote server listening");
    axum::serve(listener, router(state)).await
}

async fn rpc_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let id = req.id.clone();

    if !state.authorized(&headers) {
        return Json(JsonRpcResponse::err(
            id,
            JsonRpcError::new(error_codes::UNAUTHORIZED, "missing or invalid bearer token"),
        ));
    }

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
            run(move || store.pull(p.since, p.limit)).await.and_then(to_value)
        }
        methods::CHANGES_PUSH => {
            let p: ChangesPushParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            run(move || store.push(p.base_cursor, p.changes)).await.and_then(to_value)
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
