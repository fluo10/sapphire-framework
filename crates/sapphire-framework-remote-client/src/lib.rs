//! JSON-RPC client for a `sapphire-framework-remote-server`, plus a
//! [`RemoteChangeSource`] adapter that plugs remote sync into the
//! [`ChangeSource`](sapphire_sync::ChangeSource) abstraction.
//!
//! ```no_run
//! # async fn run() -> Result<(), sapphire_framework_remote_client::Error> {
//! use sapphire_framework_remote_client::RemoteClient;
//!
//! let client = RemoteClient::new("http://127.0.0.1:8080").with_token("secret");
//! let snap = client.snapshot("my-workspace").await?;
//! println!("cursor = {}", snap.cursor);
//! # Ok(()) }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use base64::Engine as _;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use sapphire_rpc::{
    BlobGetParams, BlobGetResult, BlobPutParams, BlobPutResult, Change, ChangesPullParams,
    ChangesPullResult, ChangesPushParams, ChangesPushResult, Hit, JsonRpcRequest, JsonRpcResponse,
    SearchParams, SearchResult, SnapshotParams, SnapshotResult, methods,
};

mod error;
pub use error::{Error, Result};

/// A typed JSON-RPC client for the remote sync server.
///
/// Cloning is cheap (the inner `reqwest::Client` is reference-counted); the
/// request-id counter is shared so ids stay unique across clones.
#[derive(Clone)]
pub struct RemoteClient {
    endpoint: String,
    token: Option<String>,
    http: reqwest::Client,
    next_id: std::sync::Arc<AtomicU64>,
}

impl RemoteClient {
    /// Create a client targeting `base_url` (the `/rpc` path is appended).
    pub fn new(base_url: impl Into<String>) -> Self {
        let base = base_url.into();
        let endpoint = format!("{}/rpc", base.trim_end_matches('/'));
        Self {
            endpoint,
            token: None,
            http: reqwest::Client::new(),
            next_id: std::sync::Arc::new(AtomicU64::new(1)),
        }
    }

    /// Use `token` as the `Authorization: Bearer` credential.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Use a caller-provided `reqwest::Client` (e.g. one with custom timeouts).
    pub fn with_http(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// Perform one JSON-RPC call and deserialize the result.
    async fn call<P: Serialize, R: DeserializeOwned>(&self, method: &str, params: P) -> Result<R> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = JsonRpcRequest::new(
            Value::from(id),
            method,
            serde_json::to_value(params)?,
        );

        let mut builder = self.http.post(&self.endpoint).json(&request);
        if let Some(token) = &self.token {
            builder = builder.bearer_auth(token);
        }
        let response: JsonRpcResponse = builder.send().await?.error_for_status()?.json().await?;

        if let Some(err) = response.error {
            return Err(Error::Rpc {
                code: err.code,
                message: err.message,
            });
        }
        let result = response.result.unwrap_or(Value::Null);
        Ok(serde_json::from_value(result)?)
    }

    /// `workspace.snapshot`.
    pub async fn snapshot(&self, ws: &str) -> Result<SnapshotResult> {
        self.call(methods::WORKSPACE_SNAPSHOT, SnapshotParams { ws: ws.to_owned() })
            .await
    }

    /// `changes.pull`.
    pub async fn pull(&self, ws: &str, since: u64, limit: usize) -> Result<ChangesPullResult> {
        self.call(
            methods::CHANGES_PULL,
            ChangesPullParams {
                ws: ws.to_owned(),
                since,
                limit,
            },
        )
        .await
    }

    /// `changes.push`.
    pub async fn push(
        &self,
        ws: &str,
        base_cursor: u64,
        changes: Vec<Change>,
    ) -> Result<ChangesPushResult> {
        self.call(
            methods::CHANGES_PUSH,
            ChangesPushParams {
                ws: ws.to_owned(),
                base_cursor,
                changes,
            },
        )
        .await
    }

    /// `blob.put` — store `bytes`, returning the content-addressed reference.
    pub async fn blob_put(&self, ws: &str, bytes: &[u8]) -> Result<BlobPutResult> {
        let bytes_base64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        self.call(
            methods::BLOB_PUT,
            BlobPutParams {
                ws: ws.to_owned(),
                bytes_base64,
            },
        )
        .await
    }

    /// `blob.get` — fetch a blob by hash, decoding it. `None` when absent.
    pub async fn blob_get(&self, ws: &str, hash: &str) -> Result<Option<Vec<u8>>> {
        let result: BlobGetResult = self
            .call(
                methods::BLOB_GET,
                BlobGetParams {
                    ws: ws.to_owned(),
                    hash: hash.to_owned(),
                },
            )
            .await?;
        match result.bytes_base64 {
            None => Ok(None),
            Some(b64) => Ok(Some(
                base64::engine::general_purpose::STANDARD
                    .decode(b64.as_bytes())
                    .map_err(|e| Error::Decode(e.to_string()))?,
            )),
        }
    }

    /// `search.fts`.
    pub async fn search_fts(&self, ws: &str, q: &str, limit: usize) -> Result<Vec<Hit>> {
        let result: SearchResult = self
            .call(
                methods::SEARCH_FTS,
                SearchParams {
                    ws: ws.to_owned(),
                    q: q.to_owned(),
                    limit,
                },
            )
            .await?;
        Ok(result.hits)
    }
}

/// Adapts a [`RemoteClient`] + workspace id to the
/// [`ChangeSource`](sapphire_sync::ChangeSource) abstraction, so remote sync is
/// interchangeable with git sync at the call site.
pub struct RemoteChangeSource {
    client: RemoteClient,
    ws: String,
}

impl RemoteChangeSource {
    /// Bind `client` to workspace `ws`.
    pub fn new(client: RemoteClient, ws: impl Into<String>) -> Self {
        Self {
            client,
            ws: ws.into(),
        }
    }

    /// The underlying client (e.g. for blob transfer, which is outside the
    /// `ChangeSource` surface).
    pub fn client(&self) -> &RemoteClient {
        &self.client
    }
}

#[async_trait]
impl sapphire_sync::ChangeSource for RemoteChangeSource {
    async fn snapshot(&self) -> sapphire_sync::Result<sapphire_sync::Snapshot> {
        self.client.snapshot(&self.ws).await.map_err(to_sync_err)
    }

    async fn pull(
        &self,
        since: sapphire_sync::Cursor,
        limit: usize,
    ) -> sapphire_sync::Result<sapphire_sync::PullBatch> {
        self.client
            .pull(&self.ws, since, limit)
            .await
            .map_err(to_sync_err)
    }

    async fn push(
        &self,
        base: sapphire_sync::Cursor,
        changes: Vec<sapphire_sync::Change>,
    ) -> sapphire_sync::Result<sapphire_sync::PushOutcome> {
        self.client
            .push(&self.ws, base, changes)
            .await
            .map_err(to_sync_err)
    }
}

/// Collapse a client error into the low-level sync crate's error type (which
/// does not depend on this crate).
fn to_sync_err(e: Error) -> sapphire_sync::Error {
    sapphire_sync::Error::Remote(e.to_string())
}
