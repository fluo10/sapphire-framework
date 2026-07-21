//! Minimal JSON-RPC 2.0 envelope types and method/error-code constants.
//!
//! Kept generic (`serde_json::Value` for `id` / `params` / `result`) so the
//! same envelope carries every method in this crate. The remote client builds
//! [`JsonRpcRequest`]s and the server replies with [`JsonRpcResponse`]s.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Method-name constants for the remote sync protocol (see `docs/ARCHITECTURE.md`).
pub mod methods {
    /// `workspace.snapshot {ws} -> {cursor, docs[]}`.
    pub const WORKSPACE_SNAPSHOT: &str = "workspace.snapshot";
    /// `changes.pull {ws, since, limit} -> {cursor, changes[], more}`.
    pub const CHANGES_PULL: &str = "changes.pull";
    /// `changes.push {ws, base_cursor, changes[]} -> {cursor, conflicts[]}`.
    pub const CHANGES_PUSH: &str = "changes.push";
    /// `blob.get {ws, hash} -> {bytes_base64?}`.
    pub const BLOB_GET: &str = "blob.get";
    /// `blob.put {ws, bytes_base64} -> {hash, len}`.
    pub const BLOB_PUT: &str = "blob.put";
    /// `search.fts {ws, q, limit} -> {hits[]}`.
    pub const SEARCH_FTS: &str = "search.fts";
    /// `search.semantic {ws, q, limit} -> {hits[]}`.
    pub const SEARCH_SEMANTIC: &str = "search.semantic";
}

/// JSON-RPC 2.0 error codes plus application-specific extensions.
pub mod error_codes {
    /// Malformed JSON was received.
    pub const PARSE_ERROR: i64 = -32700;
    /// The JSON is not a valid request object.
    pub const INVALID_REQUEST: i64 = -32600;
    /// The requested method does not exist.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Invalid method parameters.
    pub const INVALID_PARAMS: i64 = -32602;
    /// Internal server error.
    pub const INTERNAL_ERROR: i64 = -32603;

    // ── application-defined (reserved -32000..=-32099 server range) ──

    /// The caller is not authorized (missing / wrong bearer token).
    pub const UNAUTHORIZED: i64 = -32001;
    /// A `changes.push` was rejected because the server moved ahead; the
    /// conflicting paths are reported in the result's `conflicts`.
    pub const CONFLICT: i64 = -32002;
}

/// A JSON-RPC 2.0 request object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    /// Always `"2.0"`.
    pub jsonrpc: String,
    /// Correlation id echoed back in the response.
    pub id: Value,
    /// The method name (see [`methods`]).
    pub method: String,
    /// Method parameters (a JSON object for all methods in this crate).
    #[serde(default)]
    pub params: Value,
}

impl JsonRpcRequest {
    /// Build a `"2.0"` request with the given id, method and params.
    pub fn new(id: Value, method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC 2.0 response object. Exactly one of `result` / `error` is set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    /// Always `"2.0"`.
    pub jsonrpc: String,
    /// The id of the request this answers (`null` when it could not be parsed).
    pub id: Value,
    /// The successful result payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// The error, when the call failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    /// Build a success response.
    pub fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build an error response.
    pub fn err(id: Value, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

/// A JSON-RPC 2.0 error object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcError {
    /// One of [`error_codes`].
    pub code: i64,
    /// Human-readable description.
    pub message: String,
    /// Optional structured detail (e.g. the `conflicts` list on a
    /// [`error_codes::CONFLICT`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    /// Build an error with no `data`.
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Attach structured `data`.
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips_and_defaults_jsonrpc() {
        let req = JsonRpcRequest::new(
            Value::from(1),
            methods::CHANGES_PULL,
            serde_json::json!({"ws": "w", "since": 0, "limit": 10}),
        );
        let s = serde_json::to_string(&req).unwrap();
        let back: JsonRpcRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.jsonrpc, "2.0");
        assert_eq!(back.method, methods::CHANGES_PULL);
        assert_eq!(back.params["limit"], 10);
    }

    #[test]
    fn ok_response_omits_error() {
        let r = JsonRpcResponse::ok(Value::from(1), serde_json::json!({"cursor": 3}));
        let v = serde_json::to_value(&r).unwrap();
        assert!(v.get("error").is_none());
        assert_eq!(v["result"]["cursor"], 3);
    }

    #[test]
    fn err_response_omits_result() {
        let e = JsonRpcError::new(error_codes::CONFLICT, "moved ahead")
            .with_data(serde_json::json!(["a.md"]));
        let r = JsonRpcResponse::err(Value::from(2), e);
        let v = serde_json::to_value(&r).unwrap();
        assert!(v.get("result").is_none());
        assert_eq!(v["error"]["code"], error_codes::CONFLICT);
        assert_eq!(v["error"]["data"][0], "a.md");
    }
}
