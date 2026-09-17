# IPC Layer (`sapphire-framework-ipc`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Build the local inter-process layer every sapphire app will stand on — framed
JSON-RPC over a Unix socket, a Windows named pipe, or an in-process channel, with a router,
a client, same-user authentication, and start-the-server-on-demand — as the new crate
`sapphire-framework-ipc`, proven by unit and integration tests with no application code.

**Architecture:** A `Connection` is a framed, bidirectional message stream: newline-delimited
JSON frames pumped between a pair of `tokio::sync::mpsc` channels and whatever carries them
(a socket, a pipe, or nothing at all for the in-process case). Above it sit a `Router` that
dispatches JSON-RPC requests to async handlers and a `Client` that issues them and receives
notifications. Around both, an `Endpoint` names the per-user socket and `ensure_server`
connects to it — starting the server from `current_exe()` if nothing is listening, serialised
by an atomically created lock file so that several simultaneous clients produce one server.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), tokio 1 (`rt`, `net`, `io-util`, `macros`,
`sync`, `process`, `time`), serde + serde_json, thiserror 2, tracing 0.1, dirs 6;
`libc` 0.2 on Unix, `windows-sys` 0.61 on Windows; dev: tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-16-process-architecture-design.md` — §2 in full
(§2.1–§2.6). Implementation order step 3 of that spec's §9.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch; the spec commits are its base).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task. Commit
  `Cargo.lock` whenever dependencies change.
- - `windows-sys` is a large generated crate whose module paths move between releases. The
  imports in Task 5 are written for **0.59**. If `cargo check` on Windows reports a missing
  path, find the item with `cargo doc -p windows-sys --open` and fix the `use`; do not
  silence it by enabling more features blindly.
- `sapphire-framework-ipc` must **not** depend on `sapphire-framework-workspace`,
  `-retrieve`, `-backend`, `-sync` or any networking crate. It is a transport, and the bridge
  (which has no search stack) must be able to use it. A test in Task 1 pins this.
- Crate version follows the workspace (`version.workspace = true`); path dependencies
  elsewhere use `version = "0.14.0"`.
- Every public item carries a doc comment. The crate has `#![warn(missing_docs)]`.
- `PROTOCOL_VERSION` is `1`.
- Frames are newline-delimited compact JSON. A frame never contains a raw newline, because
  `serde_json::to_vec` escapes them.
- Maximum frame length is 64 MiB (`MAX_FRAME_LEN`); a longer frame is a protocol error.
- Request ids are `u64`, allocated by the client. Both ends of this protocol are ours, so the
  string ids JSON-RPC also permits are rejected.
- The runtime directory is `<dirs::data_dir()>/sapphire/run/`, mode `0700` on Unix,
  overridden whole by `SAPPHIRE_RUNTIME_DIR`. `$XDG_RUNTIME_DIR` and `/run/user/<uid>` are
  deliberately **not** consulted (spec §2.5).
- Idle exit, `workspace.*` handlers and privilege separation are **not** in this plan. They
  belong to `sapphire-framework-server` (spec §4) and its own plan.

## Deviations from the spec, agreed up front

Two refinements. Both keep the spec's behaviour and simplify the code; record them in the
commit that introduces them.

1. **Spec §2.1 describes a `trait Transport` with three implementations.** This plan uses one
   concrete `Connection` type with three constructors (`from_io`, `pair`, and the
   platform listeners) instead. The three carriers still exist and the in-process one still
   serialises, so the property the spec cared about — mobile runs the same code path — holds,
   without a trait whose only purpose is to be erased immediately.
2. **The `workspace.*` wire types are not in this crate.** The spec says `-ipc` must not
   implement `workspace.*`; this plan puts those types in `-backend` (which already owns
   `WorkspaceBackend`, `FileSearchResult` and `SearchMode`) rather than duplicating them
   here. `-ipc` stays a generic JSON-RPC transport. That work is in the `-server` plan.

## File Structure

```
crates/sapphire-framework-ipc/
    Cargo.toml
    src/
        lib.rs          # module wiring, re-exports, PROTOCOL_VERSION, MAX_FRAME_LEN
        error.rs        # Error, Result
        message.rs      # JSON-RPC envelope: Id, Request, Response, Notification, RpcError, Message
        handshake.rs    # Hello, ClientInfo, Welcome, ServerInfo, ManagedBy
        conn.rs         # Connection: NDJSON framing over any AsyncRead + AsyncWrite; pair()
        endpoint.rs     # runtime_dir(), Endpoint: socket path, pipe name, spawn-lock path
        unix.rs         # #[cfg(unix)] listener/connector + peer uid check
        windows.rs      # #[cfg(windows)] named-pipe listener/connector + restrictive DACL
        router.rs       # Router, RequestCtx, PeerHandle, serve(), listen()
        client.rs       # Client: connect, call, notifications, shutdown_server
        spawn.rs        # SpawnConfig, ensure_server(), the spawn lock
    src/bin/
        ipc-test-server.rs   # test-only echo server (required-features = ["test-util"])
    tests/
        roundtrip.rs    # client <-> server over every carrier
        race.rs         # several clients starting one server at once
```

---

### Task 1: Crate scaffold, errors and the JSON-RPC envelope

**Files:**
- Create: `crates/sapphire-framework-ipc/Cargo.toml`
- Create: `crates/sapphire-framework-ipc/src/{lib.rs,error.rs,message.rs,handshake.rs}`
- Modify: `Cargo.toml` (workspace `members`)
- Modify: `crates/sapphire-framework/Cargo.toml`, `crates/sapphire-framework/src/lib.rs` (feature `ipc`)
- Test: inline `#[cfg(test)] mod tests` in `message.rs`

**Interfaces:**
- Produces:
  - `PROTOCOL_VERSION: u32 = 1`, `MAX_FRAME_LEN: usize = 64 * 1024 * 1024`
  - `Error` (variants listed in Step 2), `type Result<T> = std::result::Result<T, Error>`
  - `RpcError { code: i32, message: String, data: Option<Value> }` with constructors
    `invalid_params`, `method_not_found`, `internal`, and the constants
    `codes::{PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS, INTERNAL_ERROR}`
  - `Request { id: u64, method: String, params: Value }`,
    `Response { id: u64, payload: ResponsePayload }`,
    `ResponsePayload::{Ok(Value), Err(RpcError)}`,
    `Notification { method: String, params: Value }`
  - `Message::{Request, Response, Notification}` with
    `Message::decode(&[u8]) -> Result<Message>` and `Message::encode(&self) -> Result<Vec<u8>>`
    (the encoded form has **no** trailing newline; framing adds it)
  - `Hello { protocol: u32, app: String, client: ClientInfo }`,
    `ClientInfo { kind: String, version: String, pid: u32 }`,
    `Welcome { protocol: u32, server: ServerInfo }`,
    `ServerInfo { version: String, pid: u32, managed_by: ManagedBy }`,
    `ManagedBy::{Service, Spawned}` (serde `lowercase`)

- [ ] **Step 1: Create the manifest and register the crate**

`crates/sapphire-framework-ipc/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-ipc"
version.workspace = true
edition.workspace = true
description = "Local inter-process JSON-RPC transport for sapphire-framework apps"
license.workspace = true
repository.workspace = true
keywords = ["ipc", "json-rpc", "unix-socket", "named-pipe"]
categories = ["network-programming"]

[features]
# Builds the test-only echo server binary used by the integration tests.
test-util = []

[[bin]]
name = "ipc-test-server"
path = "src/bin/ipc-test-server.rs"
required-features = ["test-util"]

[dependencies]
dirs.workspace = true
serde = { workspace = true }
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["rt", "net", "io-util", "macros", "sync", "process", "time"] }
tracing.workspace = true

[target.'cfg(unix)'.dependencies]
libc = "0.2"

[target.'cfg(windows)'.dependencies]
windows-sys = { version = "0.59", features = [
    "Win32_Foundation",
    "Win32_Security",
    "Win32_Security_Authorization",
    "Win32_System_Memory",
    "Win32_System_Threading",
] }

[dev-dependencies]
sapphire-framework-ipc = { path = ".", features = ["test-util"] }
tempfile = "3"
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time"] }
```

Root `Cargo.toml`: add `"crates/sapphire-framework-ipc",` to `members`, right after
`"crates/sapphire-framework-sync",`.

`crates/sapphire-framework/Cargo.toml`, in the `# ── modules` block:

```toml
ipc = ["dep:sapphire-framework-ipc"]
```

and in `[dependencies]`:

```toml
sapphire-framework-ipc = { version = "0.14.0", path = "../sapphire-framework-ipc", optional = true }
```

`crates/sapphire-framework/src/lib.rs`, next to the other module re-exports:

```rust
#[cfg(feature = "ipc")]
pub use sapphire_framework_ipc as ipc;
```

- [ ] **Step 2: Write the error type**

`crates/sapphire-framework-ipc/src/error.rs`:

```rust
use thiserror::Error;

use crate::message::RpcError;

/// Errors surfaced by the IPC layer.
#[derive(Debug, Error)]
pub enum Error {
    /// Socket, pipe or file-system failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A frame was not valid JSON, or did not deserialise into the expected shape.
    #[error("malformed frame: {0}")]
    Codec(#[from] serde_json::Error),

    /// A frame was syntactically valid but not a legal message here (for example a
    /// response to an id that was never sent, or a string request id).
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A frame exceeded [`MAX_FRAME_LEN`](crate::MAX_FRAME_LEN).
    #[error("frame of {len} bytes exceeds the {max} byte limit")]
    FrameTooLarge {
        /// Length that was announced or read.
        len: usize,
        /// The configured limit.
        max: usize,
    },

    /// The two ends do not speak the same protocol version.
    #[error("protocol version mismatch: this process speaks {ours}, the peer speaks {theirs}")]
    VersionMismatch {
        /// This process's version.
        ours: u32,
        /// The peer's version.
        theirs: u32,
    },

    /// The peer is not the same OS user, and was disconnected.
    #[error("rejected a connection from another user")]
    PeerRejected,

    /// The connection closed before the operation finished.
    #[error("connection closed")]
    Closed,

    /// The peer answered the call with a JSON-RPC error.
    #[error("remote error {}: {}", .0.code, .0.message)]
    Rpc(RpcError),

    /// A server did not appear, or did not answer, within the time allowed.
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),

    /// The server could not be started.
    #[error("could not start the server: {0}")]
    Spawn(String),

    /// A running server speaks a different version and cannot be replaced because it is
    /// managed by the OS service manager.
    #[error(
        "the installed service is version {running}, this process is version {ours}; \
         restart the service"
    )]
    ServiceVersionMismatch {
        /// Version reported by the running server.
        running: String,
        /// This process's version.
        ours: String,
    },
}

/// Convenience alias for IPC results.
pub type Result<T> = std::result::Result<T, Error>;
```

- [ ] **Step 3: Write the failing test for the envelope**

`crates/sapphire-framework-ipc/src/message.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trips() {
        let msg = Message::Request(Request {
            id: 7,
            method: "workspace.read_file".into(),
            params: json!({ "path": "notes/a.md" }),
        });
        let bytes = msg.encode().unwrap();
        assert!(!bytes.contains(&b'\n'));
        assert_eq!(Message::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn ok_and_error_responses_round_trip() {
        let ok = Message::Response(Response {
            id: 1,
            payload: ResponsePayload::Ok(json!({ "content": "hi" })),
        });
        assert_eq!(Message::decode(&ok.encode().unwrap()).unwrap(), ok);

        let err = Message::Response(Response {
            id: 2,
            payload: ResponsePayload::Err(RpcError::invalid_params("path is required")),
        });
        assert_eq!(Message::decode(&err.encode().unwrap()).unwrap(), err);
    }

    #[test]
    fn notification_round_trips() {
        let msg = Message::Notification(Notification {
            method: "workspace.event".into(),
            params: json!({ "kind": "FileChanged" }),
        });
        assert_eq!(Message::decode(&msg.encode().unwrap()).unwrap(), msg);
    }

    #[test]
    fn a_response_carrying_both_result_and_error_is_rejected() {
        let raw = br#"{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-1,"message":"x"}}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }

    #[test]
    fn a_string_request_id_is_rejected() {
        let raw = br#"{"jsonrpc":"2.0","id":"abc","method":"ping"}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }

    #[test]
    fn a_missing_jsonrpc_version_is_rejected() {
        let raw = br#"{"id":1,"method":"ping"}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cargo test -p sapphire-framework-ipc --all-features`
Expected: FAIL — the crate does not compile yet, because `message.rs` has no types.

- [ ] **Step 5: Implement the envelope**

`crates/sapphire-framework-ipc/src/message.rs`, above the test module:

```rust
//! The JSON-RPC 2.0 envelope, restricted to what this transport uses.
//!
//! Both ends of a connection are sapphire processes, so the protocol is narrower than
//! JSON-RPC allows: request ids are always integers, and batches are not accepted.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

/// Standard JSON-RPC error codes.
pub mod codes {
    /// The frame was not valid JSON.
    pub const PARSE_ERROR: i32 = -32700;
    /// The frame was valid JSON but not a valid request.
    pub const INVALID_REQUEST: i32 = -32600;
    /// No handler is registered for the method.
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// The parameters did not match the method.
    pub const INVALID_PARAMS: i32 = -32602;
    /// The handler failed.
    pub const INTERNAL_ERROR: i32 = -32603;
}

/// An error returned by a method handler.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    /// Machine-readable code; see [`codes`].
    pub code: i32,
    /// Human-readable, one line, no trailing period.
    pub message: String,
    /// Optional structured detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    /// The parameters did not match the method.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self { code: codes::INVALID_PARAMS, message: message.into(), data: None }
    }

    /// No handler is registered for the method.
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: codes::METHOD_NOT_FOUND,
            message: format!("no such method: {method}"),
            data: None,
        }
    }

    /// The handler failed.
    pub fn internal(message: impl Into<String>) -> Self {
        Self { code: codes::INTERNAL_ERROR, message: message.into(), data: None }
    }
}

/// A method call awaiting a response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// Client-allocated id, unique while the call is in flight.
    pub id: u64,
    /// Method name, `namespace.method`.
    pub method: String,
    /// Method parameters; `Value::Null` when there are none.
    pub params: Value,
}

/// What a response carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponsePayload {
    /// The call succeeded.
    Ok(Value),
    /// The call failed.
    Err(RpcError),
}

/// The answer to a [`Request`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// The id of the request being answered.
    pub id: u64,
    /// Success or failure.
    pub payload: ResponsePayload,
}

/// A one-way message that is never answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    /// Method name, `namespace.method`.
    pub method: String,
    /// Parameters; `Value::Null` when there are none.
    pub params: Value,
}

/// Anything that can travel in a frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A call.
    Request(Request),
    /// An answer.
    Response(Response),
    /// A one-way message.
    Notification(Notification),
}

/// The on-the-wire shape. Classification happens after deserialisation, because JSON-RPC
/// distinguishes the three kinds by which fields are present.
#[derive(Serialize, Deserialize)]
struct Raw {
    jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

const VERSION: &str = "2.0";

fn id_of(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| Error::Protocol(format!("request id must be an integer, got {value}")))
}

impl Message {
    /// Serialise to a single frame's payload. The result never contains a newline, so the
    /// framing in [`Connection`](crate::Connection) can simply append one.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let raw = match self {
            Message::Request(r) => Raw {
                jsonrpc: VERSION.into(),
                id: Some(Value::from(r.id)),
                method: Some(r.method.clone()),
                params: Some(r.params.clone()),
                result: None,
                error: None,
            },
            Message::Response(r) => {
                let (result, error) = match &r.payload {
                    ResponsePayload::Ok(v) => (Some(v.clone()), None),
                    ResponsePayload::Err(e) => (None, Some(e.clone())),
                };
                Raw {
                    jsonrpc: VERSION.into(),
                    id: Some(Value::from(r.id)),
                    method: None,
                    params: None,
                    result,
                    error,
                }
            }
            Message::Notification(n) => Raw {
                jsonrpc: VERSION.into(),
                id: None,
                method: Some(n.method.clone()),
                params: Some(n.params.clone()),
                result: None,
                error: None,
            },
        };
        Ok(serde_json::to_vec(&raw)?)
    }

    /// Parse one frame's payload.
    pub fn decode(bytes: &[u8]) -> Result<Message> {
        let raw: Raw = serde_json::from_slice(bytes)?;
        if raw.jsonrpc != VERSION {
            return Err(Error::Protocol(format!(
                "expected jsonrpc \"{VERSION}\", got \"{}\"",
                raw.jsonrpc
            )));
        }
        match (raw.method, raw.id) {
            (Some(method), Some(id)) => Ok(Message::Request(Request {
                id: id_of(&id)?,
                method,
                params: raw.params.unwrap_or(Value::Null),
            })),
            (Some(method), None) => Ok(Message::Notification(Notification {
                method,
                params: raw.params.unwrap_or(Value::Null),
            })),
            (None, Some(id)) => {
                let id = id_of(&id)?;
                match (raw.result, raw.error) {
                    (Some(_), Some(_)) => Err(Error::Protocol(
                        "a response carries both result and error".into(),
                    )),
                    (Some(v), None) => {
                        Ok(Message::Response(Response { id, payload: ResponsePayload::Ok(v) }))
                    }
                    (None, Some(e)) => {
                        Ok(Message::Response(Response { id, payload: ResponsePayload::Err(e) }))
                    }
                    (None, None) => Err(Error::Protocol(
                        "a response carries neither result nor error".into(),
                    )),
                }
            }
            (None, None) => Err(Error::Protocol("a frame with neither method nor id".into())),
        }
    }
}
```

- [ ] **Step 6: Write the handshake types**

`crates/sapphire-framework-ipc/src/handshake.rs`:

```rust
//! The first exchange on every connection (spec §2.4).

use serde::{Deserialize, Serialize};

/// Sent by the client as the first frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The client's IPC protocol version.
    pub protocol: u32,
    /// The application whose server the client expects to be talking to.
    pub app: String,
    /// Who is connecting.
    pub client: ClientInfo,
}

/// Identifies the connecting process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientInfo {
    /// `cli`, `desktop`, `mcp`, …
    pub kind: String,
    /// The client's crate version.
    pub version: String,
    /// The client's process id.
    pub pid: u32,
}

/// Sent by the server in answer to [`Hello`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Welcome {
    /// The server's IPC protocol version.
    pub protocol: u32,
    /// Who answered.
    pub server: ServerInfo,
}

/// Identifies the serving process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// The server's crate version.
    pub version: String,
    /// The server's process id.
    pub pid: u32,
    /// How the server was started, which decides what a client may do about a version
    /// mismatch (spec §2.6).
    pub managed_by: ManagedBy,
}

/// How a server process came to exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManagedBy {
    /// Started by the OS service manager. A client must not shut it down.
    Service,
    /// Started on demand by a client. A client may ask it to exit and start a new one.
    Spawned,
}
```

- [ ] **Step 7: Write `lib.rs`**

`crates/sapphire-framework-ipc/src/lib.rs`:

```rust
//! Local inter-process transport for sapphire-framework applications.
//!
//! A sapphire app's cache is a redb database, which only one process may open. The app's
//! **server** is therefore the only process that touches it, and its CLI, stdio MCP server
//! and desktop UI reach it through this crate: framed JSON-RPC 2.0 over a Unix domain
//! socket, a Windows named pipe, or an in-process channel.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §2.
//!
//! This crate is a transport. It knows nothing about workspaces, search or sync, so the
//! bridge — which has no search stack — can use it too.

#![warn(missing_docs)]

mod error;
mod handshake;
mod message;

pub use error::{Error, Result};
pub use handshake::{ClientInfo, Hello, ManagedBy, ServerInfo, Welcome};
pub use message::{
    Message, Notification, Request, Response, ResponsePayload, RpcError, codes,
};

/// The IPC protocol version this build speaks. Bumped on a breaking change to the
/// envelope or the handshake.
pub const PROTOCOL_VERSION: u32 = 1;

/// Longest frame accepted, in bytes. Frames carry file content as JSON strings, and this
/// bounds the memory one hostile or buggy peer can make the other allocate.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features`
Expected: PASS, 6 tests in `message::tests`.

- [ ] **Step 9: Pin the dependency constraint**

`crates/sapphire-framework-ipc/tests/roundtrip.rs` (the file grows in later tasks; start it):

```rust
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
```

- [ ] **Step 10: Verify the whole workspace still builds, then commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-ipc Cargo.toml Cargo.lock crates/sapphire-framework
git commit -m "feat(ipc): add the crate scaffold, errors and the JSON-RPC envelope"
```

---

### Task 2: `Connection` — newline-delimited framing

**Files:**
- Create: `crates/sapphire-framework-ipc/src/conn.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs` (add `mod conn;` and re-export)
- Test: inline `#[cfg(test)] mod tests` in `conn.rs`

**Interfaces:**
- Consumes: `Message`, `Error`, `Result`, `MAX_FRAME_LEN` (Task 1)
- Produces:
  - `Connection`: `from_io<S>(io: S) -> Connection where S: AsyncRead + AsyncWrite + Send + 'static`,
    `pair() -> (Connection, Connection)`,
    `sender(&self) -> Sender`,
    `async recv(&mut self) -> Option<Result<Message>>`,
    `async send(&self, msg: Message) -> Result<()>`,
    `close(self)`
  - `Sender` (cheap `Clone`): `async send(&self, msg: Message) -> Result<()>`

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-ipc/src/conn.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(id: u64) -> Message {
        Message::Request(Request { id, method: "ping".into(), params: json!({ "n": id }) })
    }

    #[tokio::test]
    async fn a_pair_carries_messages_both_ways() {
        let (mut a, mut b) = Connection::pair();
        a.send(req(1)).await.unwrap();
        assert_eq!(b.recv().await.unwrap().unwrap(), req(1));

        b.send(req(2)).await.unwrap();
        assert_eq!(a.recv().await.unwrap().unwrap(), req(2));
    }

    #[tokio::test]
    async fn dropping_one_end_of_a_pair_closes_the_other() {
        let (a, mut b) = Connection::pair();
        drop(a);
        assert!(b.recv().await.is_none());
    }

    #[tokio::test]
    async fn io_framing_round_trips_many_messages() {
        let (client_io, server_io) = tokio::io::duplex(8 * 1024);
        let mut client = Connection::from_io(client_io);
        let mut server = Connection::from_io(server_io);

        for id in 1..=100 {
            client.send(req(id)).await.unwrap();
        }
        for id in 1..=100 {
            assert_eq!(server.recv().await.unwrap().unwrap(), req(id));
        }
    }

    #[tokio::test]
    async fn a_message_larger_than_the_buffer_still_round_trips() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        let mut client = Connection::from_io(client_io);
        let mut server = Connection::from_io(server_io);

        let big = "x".repeat(200_000);
        let msg = Message::Notification(Notification {
            method: "big".into(),
            params: json!({ "content": big }),
        });
        let sent = msg.clone();
        tokio::spawn(async move { client.send(sent).await.unwrap() });
        assert_eq!(server.recv().await.unwrap().unwrap(), msg);
    }

    #[tokio::test]
    async fn an_oversized_frame_is_reported_and_closes_the_connection() {
        let (mut raw, server_io) = tokio::io::duplex(1024);
        let mut server = Connection::from_io(server_io);

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            // A single line longer than the limit, written without ever ending it.
            let chunk = vec![b'x'; 1024 * 1024];
            for _ in 0..(MAX_FRAME_LEN / chunk.len() + 1) {
                if raw.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });

        let err = server.recv().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::FrameTooLarge { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn a_malformed_frame_is_reported_without_killing_the_connection() {
        let (mut raw, server_io) = tokio::io::duplex(1024);
        let mut server = Connection::from_io(server_io);

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            raw.write_all(b"{ not json }\n").await.unwrap();
            let good = Message::Notification(Notification {
                method: "after".into(),
                params: serde_json::Value::Null,
            });
            raw.write_all(&good.encode().unwrap()).await.unwrap();
            raw.write_all(b"\n").await.unwrap();
            // Keep the write half alive so the reader does not see EOF first.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        assert!(server.recv().await.unwrap().is_err());
        let next = server.recv().await.unwrap().unwrap();
        assert_eq!(
            next,
            Message::Notification(Notification {
                method: "after".into(),
                params: serde_json::Value::Null
            })
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-ipc --all-features conn`
Expected: FAIL — `Connection` does not exist.

- [ ] **Step 3: Implement `Connection`**

`crates/sapphire-framework-ipc/src/conn.rs`, above the test module:

```rust
//! A framed, bidirectional message stream.
//!
//! Frames are newline-delimited compact JSON (spec §2.2). Whatever carries them — a Unix
//! socket, a Windows named pipe, or nothing at all in the in-process case — is pumped by
//! two background tasks into a pair of channels, so every caller sees the same type.

use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::MAX_FRAME_LEN;

/// How many outgoing messages may queue before `send` waits.
const OUTGOING_CAPACITY: usize = 256;
/// How many incoming messages may queue before the reader waits.
const INCOMING_CAPACITY: usize = 256;

/// A cloneable handle for sending on a [`Connection`].
///
/// Used to hand a connection's write side to many tasks — for example so a request handler
/// can emit notifications while the read loop keeps running.
#[derive(Clone, Debug)]
pub struct Sender(mpsc::Sender<Message>);

impl Sender {
    /// Queue a message. Waits while the outgoing queue is full; fails once the connection
    /// has closed.
    pub async fn send(&self, msg: Message) -> Result<()> {
        self.0.send(msg).await.map_err(|_| Error::Closed)
    }
}

/// A framed, bidirectional message stream.
#[derive(Debug)]
pub struct Connection {
    outgoing: mpsc::Sender<Message>,
    incoming: mpsc::Receiver<Result<Message>>,
    pumps: Vec<JoinHandle<()>>,
}

impl Connection {
    /// Wrap anything that reads and writes bytes.
    ///
    /// Reading and writing run in **separate tasks**, not in one `select!` loop. Reading a
    /// frame is not cancel-safe — it consumes bytes from the buffer before it has a whole
    /// line — so a `select!` that dropped the read future whenever an outgoing message
    /// arrived would silently lose the partial frame.
    pub fn from_io<S>(io: S) -> Connection
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (out_tx, mut out_rx) = mpsc::channel::<Message>(OUTGOING_CAPACITY);
        let (in_tx, in_rx) = mpsc::channel::<Result<Message>>(INCOMING_CAPACITY);
        let (read_half, mut write_half) = tokio::io::split(io);

        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                let Ok(mut bytes) = msg.encode() else {
                    // Our own message failed to serialise. Nothing useful can be sent, and
                    // the caller already owns the value, so drop it and keep serving.
                    tracing::error!("dropping an outgoing message that could not be encoded");
                    continue;
                };
                bytes.push(b'\n');
                if write_half.write_all(&bytes).await.is_err() {
                    break;
                }
                if write_half.flush().await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        });

        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(read_half);
            loop {
                let mut line = Vec::new();
                match read_limited(&mut reader, &mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        if line.is_empty() {
                            continue;
                        }
                        if in_tx.send(Message::decode(&line)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = in_tx.send(Err(err)).await;
                        break;
                    }
                }
            }
        });

        Connection { outgoing: out_tx, incoming: in_rx, pumps: vec![reader, writer] }
    }

    /// Two connections wired directly to each other, with no carrier.
    ///
    /// This is the in-process transport of spec §2.1, used on mobile and in tests. It still
    /// moves [`Message`] values rather than bytes; the serialisation the spec asks for
    /// happens wherever the message is built, so both sides run the same code path.
    pub fn pair() -> (Connection, Connection) {
        let (a_out, b_in) = mpsc::channel::<Message>(OUTGOING_CAPACITY);
        let (b_out, a_in) = mpsc::channel::<Message>(OUTGOING_CAPACITY);
        (Connection::from_channels(a_out, a_in), Connection::from_channels(b_out, b_in))
    }

    fn from_channels(
        outgoing: mpsc::Sender<Message>,
        mut raw_in: mpsc::Receiver<Message>,
    ) -> Connection {
        let (in_tx, in_rx) = mpsc::channel::<Result<Message>>(INCOMING_CAPACITY);
        let pump = tokio::spawn(async move {
            while let Some(msg) = raw_in.recv().await {
                // Serialise and parse even though both ends are in this process: mobile
                // runs this carrier, and it must exercise the same code path as the
                // others (spec §2.1).
                let round_tripped = msg.encode().and_then(|bytes| Message::decode(&bytes));
                if in_tx.send(round_tripped).await.is_err() {
                    break;
                }
            }
        });
        Connection { outgoing, incoming: in_rx, pumps: vec![pump] }
    }

    /// A cloneable handle for the write side.
    pub fn sender(&self) -> Sender {
        Sender(self.outgoing.clone())
    }

    /// Queue a message.
    pub async fn send(&self, msg: Message) -> Result<()> {
        self.outgoing.send(msg).await.map_err(|_| Error::Closed)
    }

    /// Await the next message. `None` once the connection has closed.
    pub async fn recv(&mut self) -> Option<Result<Message>> {
        self.incoming.recv().await
    }

    /// Close the connection and stop its pumps.
    pub fn close(self) {
        drop(self);
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        for pump in self.pumps.drain(..) {
            pump.abort();
        }
    }
}

/// `read_until(b'\n')`, refusing to grow past [`MAX_FRAME_LEN`].
///
/// `AsyncBufReadExt::read_until` has no limit, so a peer that never sends a newline would
/// otherwise make us allocate without bound.
async fn read_limited<R>(reader: &mut R, out: &mut Vec<u8>) -> Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let available = match reader.fill_buf().await {
            Ok(buf) => buf,
            Err(err) => return Err(Error::Io(err)),
        };
        if available.is_empty() {
            return Ok(out.len());
        }
        match available.iter().position(|b| *b == b'\n') {
            Some(idx) => {
                out.extend_from_slice(&available[..=idx]);
                reader.consume(idx + 1);
                return Ok(out.len());
            }
            None => {
                let len = available.len();
                if out.len() + len > MAX_FRAME_LEN {
                    return Err(Error::FrameTooLarge { len: out.len() + len, max: MAX_FRAME_LEN });
                }
                out.extend_from_slice(available);
                reader.consume(len);
            }
        }
    }
}
```

`crates/sapphire-framework-ipc/src/lib.rs`: add `mod conn;` next to the other modules and
`pub use conn::{Connection, Sender};` next to the other re-exports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features conn`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-ipc
git commit -m "feat(ipc): frame messages as newline-delimited JSON over any carrier"
```

---

### Task 3: `Endpoint` — where the socket lives

**Files:**
- Create: `crates/sapphire-framework-ipc/src/endpoint.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `endpoint.rs`

**Interfaces:**
- Consumes: `Error`, `Result` (Task 1)
- Produces:
  - `runtime_dir() -> Result<PathBuf>` — `SAPPHIRE_RUNTIME_DIR`, else
    `dirs::data_dir()/sapphire/run`, else `std::env::temp_dir()/sapphire/run`; created with
    mode `0700` on Unix
  - `Endpoint { name: String, dir: PathBuf }`:
    `for_app(app_name: &str) -> Result<Endpoint>`,
    `for_bridge() -> Result<Endpoint>`,
    `in_dir(name: impl Into<String>, dir: PathBuf) -> Endpoint` (tests and
    `SAPPHIRE_RUNTIME_DIR` overrides),
    `socket_path(&self) -> PathBuf`,
    `pipe_name(&self) -> String`,
    `lock_path(&self) -> PathBuf`

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-ipc/src/endpoint.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_names_its_socket_and_lock_under_its_directory() {
        let dir = std::env::temp_dir().join("sapphire-endpoint-test");
        let ep = Endpoint::in_dir("sapphire-journal", dir.clone());
        assert_eq!(ep.socket_path(), dir.join("sapphire-journal.sock"));
        assert_eq!(ep.lock_path(), dir.join("sapphire-journal.spawn.lock"));
    }

    #[test]
    fn the_bridge_endpoint_is_named_bridge() {
        let ep = Endpoint::in_dir(BRIDGE_NAME, std::env::temp_dir());
        assert_eq!(ep.socket_path().file_name().unwrap(), "bridge.sock");
    }

    #[test]
    fn a_pipe_name_is_scoped_to_the_user() {
        let ep = Endpoint::in_dir("sapphire-journal", std::env::temp_dir());
        let name = ep.pipe_name();
        assert!(name.starts_with(r"\\.\pipe\sapphire."), "{name}");
        assert!(name.ends_with(".sapphire-journal"), "{name}");
    }

    #[test]
    fn the_runtime_directory_env_var_replaces_the_whole_path() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test process section; no other thread reads the
        // environment concurrently in this test binary.
        unsafe { std::env::set_var("SAPPHIRE_RUNTIME_DIR", tmp.path()) };
        let dir = runtime_dir().unwrap();
        unsafe { std::env::remove_var("SAPPHIRE_RUNTIME_DIR") };
        assert_eq!(dir, tmp.path());
        assert!(dir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn the_runtime_directory_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("run");
        ensure_private_dir(&dir).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "mode was {:o}", mode & 0o777);
    }
}
```

Add `tempfile = "3"` to `[dev-dependencies]` — it is already there from Task 1.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-ipc --all-features endpoint`
Expected: FAIL — `Endpoint` does not exist.

- [ ] **Step 3: Implement the endpoint**

`crates/sapphire-framework-ipc/src/endpoint.rs`, above the test module:

```rust
//! Where a server listens (spec §2.5).
//!
//! The runtime directory is persistent, not `/run/user/<uid>`. A server started as a system
//! service and dropped to a user has neither `$XDG_RUNTIME_DIR` nor a login session's
//! `/run/user/<uid>`, while a CLI in a login session has both — the two would resolve
//! different paths and never meet.

use std::path::{Path, PathBuf};

use crate::error::Result;

/// The endpoint name the host-wide bridge listens under.
pub const BRIDGE_NAME: &str = "bridge";

/// Environment variable that replaces the runtime directory outright.
pub const RUNTIME_DIR_ENV: &str = "SAPPHIRE_RUNTIME_DIR";

/// Create `dir` if needed and make it private to the current user.
pub(crate) fn ensure_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(dir)?.permissions();
        if perms.mode() & 0o777 != 0o700 {
            perms.set_mode(0o700);
            std::fs::set_permissions(dir, perms)?;
        }
    }
    Ok(())
}

/// The directory holding this user's sockets and spawn locks, created if absent.
pub fn runtime_dir() -> Result<PathBuf> {
    let dir = match std::env::var_os(RUNTIME_DIR_ENV).filter(|v| !v.is_empty()) {
        Some(v) => PathBuf::from(v),
        None => dirs::data_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("sapphire")
            .join("run"),
    };
    ensure_private_dir(&dir)?;
    Ok(dir)
}

/// Identifies one server's listening address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// The app name, or [`BRIDGE_NAME`].
    pub name: String,
    /// The directory the socket and lock live in.
    pub dir: PathBuf,
}

impl Endpoint {
    /// The endpoint of `app_name`'s server, creating the runtime directory if needed.
    pub fn for_app(app_name: &str) -> Result<Endpoint> {
        Ok(Endpoint { name: app_name.to_owned(), dir: runtime_dir()? })
    }

    /// The endpoint of the host-wide bridge.
    pub fn for_bridge() -> Result<Endpoint> {
        Endpoint::for_app(BRIDGE_NAME)
    }

    /// An endpoint in an explicit directory. Used by tests and by callers that resolved
    /// the directory themselves.
    pub fn in_dir(name: impl Into<String>, dir: PathBuf) -> Endpoint {
        Endpoint { name: name.into(), dir }
    }

    /// The Unix domain socket path.
    pub fn socket_path(&self) -> PathBuf {
        self.dir.join(format!("{}.sock", self.name))
    }

    /// The Windows named pipe name, scoped to the current user so that two users on one
    /// machine get separate pipes.
    pub fn pipe_name(&self) -> String {
        format!(r"\\.\pipe\sapphire.{}.{}", user_scope(), self.name)
    }

    /// The lock file that serialises start-on-demand (spec §2.6 step 3a).
    pub fn lock_path(&self) -> PathBuf {
        self.dir.join(format!("{}.spawn.lock", self.name))
    }
}

/// A stable, per-user string used to scope the Windows pipe name.
///
/// The security descriptor is what actually restricts access (see `windows.rs`); this only
/// keeps two users' pipes from colliding by name.
fn user_scope() -> String {
    #[cfg(windows)]
    {
        crate::windows::current_user_sid().unwrap_or_else(|_| "unknown".to_owned())
    }
    #[cfg(unix)]
    {
        // SAFETY: getuid is always successful and has no preconditions.
        unsafe { libc::getuid() }.to_string()
    }
}
```

`crates/sapphire-framework-ipc/src/lib.rs`: add `mod endpoint;` and
`pub use endpoint::{BRIDGE_NAME, Endpoint, RUNTIME_DIR_ENV, runtime_dir};`.

> `user_scope` refers to `crate::windows::current_user_sid`, which Task 5 adds. Until then,
> build and test on Unix; on Windows, do Task 5 before running this task's tests. If you are
> on Windows and want Task 3 green first, stub `current_user_sid` in Task 5's file with
> `Ok("unknown".to_owned())` and replace it in Task 5.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features endpoint`
Expected: PASS, 5 tests on Unix (4 on Windows — the permission test is `cfg(unix)`).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-ipc
git commit -m "feat(ipc): resolve the per-user runtime directory and endpoint paths"
```

---

### Task 4: Unix transport and the peer-uid check

**Files:**
- Create: `crates/sapphire-framework-ipc/src/unix.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `unix.rs`

**Interfaces:**
- Consumes: `Connection` (Task 2), `Endpoint` (Task 3), `Error`, `Result`
- Produces (all `#[cfg(unix)]`):
  - `struct UnixListenerHandle` with `async accept(&self) -> Result<Connection>` and
    `path(&self) -> &Path`; removes the socket file on drop
  - `async bind(endpoint: &Endpoint) -> Result<UnixListenerHandle>` — removes a stale socket
    first (spec §2.6 step 4)
  - `async connect(endpoint: &Endpoint) -> Result<Connection>`
  - `async probe(endpoint: &Endpoint) -> Result<bool>` — `true` if something is listening;
    unlinks the socket file and returns `false` on `ECONNREFUSED`
  - `fn peer_uid(stream: &tokio::net::UnixStream) -> Result<u32>`

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-ipc/src/unix.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Notification, Message};

    fn endpoint(dir: &std::path::Path) -> Endpoint {
        Endpoint::in_dir("test-app", dir.to_path_buf())
    }

    #[tokio::test]
    async fn a_client_reaches_the_listener() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        let listener = bind(&ep).await.unwrap();

        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            conn.recv().await.unwrap().unwrap()
        });

        let client = connect(&ep).await.unwrap();
        let msg = Message::Notification(Notification {
            method: "hello".into(),
            params: serde_json::Value::Null,
        });
        client.send(msg.clone()).await.unwrap();
        assert_eq!(server.await.unwrap(), msg);
    }

    #[tokio::test]
    async fn the_peer_uid_of_a_local_connection_is_our_own() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        let listener = tokio::net::UnixListener::bind(ep.socket_path()).unwrap();

        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            peer_uid(&stream).unwrap()
        });
        let _client = tokio::net::UnixStream::connect(ep.socket_path()).await.unwrap();

        // SAFETY: getuid has no preconditions.
        assert_eq!(accept.await.unwrap(), unsafe { libc::getuid() });
    }

    #[tokio::test]
    async fn probing_an_empty_directory_reports_nothing_listening() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!probe(&endpoint(tmp.path())).await.unwrap());
    }

    #[tokio::test]
    async fn a_stale_socket_file_is_removed_and_reported_as_not_listening() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        // A socket file with nobody behind it: bind, then drop the listener without
        // letting our own Drop clean up.
        let listener = tokio::net::UnixListener::bind(ep.socket_path()).unwrap();
        drop(listener);
        assert!(ep.socket_path().exists());

        assert!(!probe(&ep).await.unwrap());
        assert!(!ep.socket_path().exists(), "the stale socket should have been unlinked");
    }

    #[tokio::test]
    async fn binding_over_a_stale_socket_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        drop(tokio::net::UnixListener::bind(ep.socket_path()).unwrap());

        let listener = bind(&ep).await.unwrap();
        assert!(connect(&ep).await.is_ok());
        drop(listener);
    }

    #[tokio::test]
    async fn a_listener_removes_its_socket_file_on_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        let listener = bind(&ep).await.unwrap();
        assert!(ep.socket_path().exists());
        drop(listener);
        assert!(!ep.socket_path().exists());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-ipc --all-features unix`
Expected: FAIL — `bind` / `connect` / `probe` / `peer_uid` do not exist.

- [ ] **Step 3: Implement the Unix transport**

`crates/sapphire-framework-ipc/src/unix.rs`, above the test module:

```rust
//! Unix domain socket carrier, with same-user authentication (spec §2.3).

use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};

use crate::conn::Connection;
use crate::endpoint::{Endpoint, ensure_private_dir};
use crate::error::{Error, Result};

/// A bound listener that removes its socket file when dropped.
#[derive(Debug)]
pub struct UnixListenerHandle {
    listener: UnixListener,
    path: PathBuf,
}

impl UnixListenerHandle {
    /// The socket path this listener is bound to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accept one connection, rejecting peers that are not the same OS user.
    ///
    /// A rejected peer is disconnected and the wait continues, so one hostile connection
    /// cannot stop the server from serving legitimate ones.
    pub async fn accept(&self) -> Result<Connection> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            // SAFETY: getuid has no preconditions.
            let ours = unsafe { libc::getuid() };
            match peer_uid(&stream) {
                Ok(uid) if uid == ours => return Ok(Connection::from_io(stream)),
                Ok(uid) => {
                    tracing::warn!(peer_uid = uid, our_uid = ours, "rejected a connection from another user");
                    drop(stream);
                }
                Err(err) => {
                    tracing::warn!("could not read peer credentials, rejecting: {err}");
                    drop(stream);
                }
            }
        }
    }
}

impl Drop for UnixListenerHandle {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Listen on `endpoint`, first removing a socket file left behind by a dead server.
pub async fn bind(endpoint: &Endpoint) -> Result<UnixListenerHandle> {
    ensure_private_dir(&endpoint.dir)?;
    let path = endpoint.socket_path();
    if path.exists() {
        // Either nothing is listening (stale) or another server is. `probe` unlinks the
        // former; the latter makes the bind below fail with EADDRINUSE, which is correct.
        probe(endpoint).await?;
    }
    let listener = UnixListener::bind(&path)?;
    Ok(UnixListenerHandle { listener, path })
}

/// Connect to `endpoint`.
pub async fn connect(endpoint: &Endpoint) -> Result<Connection> {
    let stream = UnixStream::connect(endpoint.socket_path()).await?;
    Ok(Connection::from_io(stream))
}

/// Is a server listening on `endpoint`?
///
/// Unlinks the socket file and returns `false` when the file exists but nothing answers —
/// the state left by a crash or a reboot (spec §2.6 step 4).
pub async fn probe(endpoint: &Endpoint) -> Result<bool> {
    let path = endpoint.socket_path();
    if !path.exists() {
        return Ok(false);
    }
    match UnixStream::connect(&path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
            tracing::debug!(path = %path.display(), "removing a stale socket");
            let _ = std::fs::remove_file(&path);
            Ok(false)
        }
        Err(err) => Err(Error::Io(err)),
    }
}

/// The uid of the process at the other end.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn peer_uid(stream: &UnixStream) -> Result<u32> {
    use std::os::fd::AsRawFd;

    let mut cred = libc::ucred { pid: 0, uid: 0, gid: 0 };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `cred` and `len` are valid for writes of the sizes passed, and the fd is
    // owned by `stream` for the duration of the call.
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut cred).cast::<libc::c_void>(),
            &raw mut len,
        )
    };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(cred.uid)
}

/// The uid of the process at the other end.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn peer_uid(stream: &UnixStream) -> Result<u32> {
    use std::os::fd::AsRawFd;

    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    // SAFETY: both out-pointers are valid for writes, and the fd is owned by `stream`.
    let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &raw mut uid, &raw mut gid) };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(uid)
}
```

`crates/sapphire-framework-ipc/src/lib.rs`:

```rust
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::{UnixListenerHandle, bind, peer_uid};
```

`connect` and `probe` are **not** re-exported here: Task 8 re-exports platform-independent
wrappers of the same names, so callers need no `cfg`.

- [ ] **Step 4: Run the tests to verify they pass**

Run (on Linux or macOS): `cargo test -p sapphire-framework-ipc --all-features unix`
Expected: PASS, 6 tests.

If you are developing on Windows, this task cannot be verified locally. Push the branch and
confirm the Linux CI job is green before treating the task as done — do not mark it complete
on an unverified build.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-ipc
git commit -m "feat(ipc): add the Unix socket carrier with a peer-uid check"
```

---

### Task 5: Windows named pipe transport with a restrictive DACL

**Files:**
- Create: `crates/sapphire-framework-ipc/src/windows.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `windows.rs`

**Interfaces:**
- Consumes: `Connection` (Task 2), `Endpoint` (Task 3), `Error`, `Result`
- Produces (all `#[cfg(windows)]`):
  - `fn current_user_sid() -> Result<String>` — the current user's SID in string form
    (also used by `Endpoint::pipe_name`, Task 3)
  - `struct PipeListener` with `async accept(&mut self) -> Result<Connection>`
  - `fn bind(endpoint: &Endpoint) -> Result<PipeListener>`
  - `async fn connect(endpoint: &Endpoint) -> Result<Connection>`
  - `async fn probe(endpoint: &Endpoint) -> Result<bool>`

**Why the DACL matters:** a named pipe created with default security is reachable by more
than its creator. The security descriptor below grants full control to the creating user and
to `SYSTEM`, and to nobody else. `reject_remote_clients` additionally refuses connections
arriving over SMB.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-ipc/src/windows.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, Notification};

    fn endpoint(name: &str) -> Endpoint {
        Endpoint::in_dir(name, std::env::temp_dir())
    }

    #[test]
    fn the_current_user_sid_looks_like_a_sid() {
        let sid = current_user_sid().unwrap();
        assert!(sid.starts_with("S-1-"), "{sid}");
    }

    #[tokio::test]
    async fn a_client_reaches_the_listener() {
        let ep = endpoint(&format!("ipc-test-{}", std::process::id()));
        let mut listener = bind(&ep).unwrap();

        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            conn.recv().await.unwrap().unwrap()
        });

        let client = connect(&ep).await.unwrap();
        let msg = Message::Notification(Notification {
            method: "hello".into(),
            params: serde_json::Value::Null,
        });
        client.send(msg.clone()).await.unwrap();
        assert_eq!(server.await.unwrap(), msg);
    }

    #[tokio::test]
    async fn probing_an_unused_name_reports_nothing_listening() {
        let ep = endpoint(&format!("ipc-absent-{}", std::process::id()));
        assert!(!probe(&ep).await.unwrap());
    }

    #[tokio::test]
    async fn probing_a_bound_name_reports_a_server() {
        let ep = endpoint(&format!("ipc-present-{}", std::process::id()));
        let _listener = bind(&ep).unwrap();
        assert!(probe(&ep).await.unwrap());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run (on Windows): `cargo test -p sapphire-framework-ipc --all-features windows`
Expected: FAIL — the functions do not exist.

- [ ] **Step 3: Implement the SID lookup and the security descriptor**

`crates/sapphire-framework-ipc/src/windows.rs`, above the test module:

```rust
//! Windows named pipe carrier, restricted to the current user (spec §2.3).

use std::ffi::c_void;

use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};
use windows_sys::Win32::Foundation::{CloseHandle, ERROR_PIPE_BUSY, HANDLE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
    SDDL_REVISION_1,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    TokenUser,
};
use windows_sys::Win32::System::Memory::LocalFree;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use crate::conn::Connection;
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};

/// How many bytes a pipe instance buffers in each direction.
const PIPE_BUFFER: u32 = 64 * 1024;

fn last_error() -> Error {
    Error::Io(std::io::Error::last_os_error())
}

/// Decode a NUL-terminated wide string.
///
/// # Safety
/// `ptr` must point to a valid, NUL-terminated UTF-16 string.
unsafe fn wide_to_string(ptr: *const u16) -> String {
    let mut len = 0usize;
    // SAFETY: the caller guarantees a NUL terminator.
    while unsafe { *ptr.add(len) } != 0 {
        len += 1;
    }
    // SAFETY: `ptr` is valid for `len` elements by the loop above.
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// The current user's SID, in string form (`S-1-5-21-…`).
pub fn current_user_sid() -> Result<String> {
    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: `token` is a valid out-pointer; the handle is closed below.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_error());
    }
    struct TokenGuard(HANDLE);
    impl Drop for TokenGuard {
        fn drop(&mut self) {
            // SAFETY: the handle came from OpenProcessToken and is closed exactly once.
            unsafe { CloseHandle(self.0) };
        }
    }
    let _guard = TokenGuard(token);

    let mut needed: u32 = 0;
    // SAFETY: querying the required size with a null buffer is the documented pattern.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &raw mut needed) };
    if needed == 0 {
        return Err(last_error());
    }
    let mut buf = vec![0u8; needed as usize];
    // SAFETY: `buf` is valid for `needed` bytes.
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buf.as_mut_ptr().cast::<c_void>(),
            needed,
            &raw mut needed,
        )
    } == 0
    {
        return Err(last_error());
    }
    // SAFETY: on success the buffer holds a TOKEN_USER.
    let user = unsafe { &*buf.as_ptr().cast::<TOKEN_USER>() };

    let mut raw: *mut u16 = std::ptr::null_mut();
    // SAFETY: `user.User.Sid` is a valid SID owned by `buf`.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &raw mut raw) } == 0 {
        return Err(last_error());
    }
    // SAFETY: `raw` is a NUL-terminated wide string allocated by the call above.
    let sid = unsafe { wide_to_string(raw) };
    // SAFETY: `raw` was allocated with LocalAlloc by ConvertSidToStringSidW.
    unsafe { LocalFree(raw.cast::<c_void>()) };
    Ok(sid)
}

/// A self-freeing security descriptor granting full control to `sid` and to `SYSTEM`.
struct Descriptor(PSECURITY_DESCRIPTOR);

impl Descriptor {
    fn for_current_user() -> Result<Descriptor> {
        let sid = current_user_sid()?;
        // D:P            — a DACL, protected from inheritance
        // (A;;GA;;;<sid>) — allow generic-all to this user
        // (A;;GA;;;SY)    — allow generic-all to LocalSystem
        let sddl: Vec<u16> = format!("D:P(A;;GA;;;{sid})(A;;GA;;;SY)")
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: `sddl` is NUL-terminated; `psd` is a valid out-pointer.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &raw mut psd,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(last_error());
        }
        Ok(Descriptor(psd))
    }

    fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.0,
            bInheritHandle: 0,
        }
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        // SAFETY: allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW.
        unsafe { LocalFree(self.0.cast::<c_void>()) };
    }
}
```

- [ ] **Step 4: Implement the listener and connector**

Append to `crates/sapphire-framework-ipc/src/windows.rs`:

```rust
/// A named pipe listener.
///
/// Windows named pipes serve one client per instance, so accepting means handing out the
/// waiting instance and immediately creating the next one.
#[derive(Debug)]
pub struct PipeListener {
    name: String,
    next: Option<NamedPipeServer>,
}

impl PipeListener {
    fn create_instance(name: &str, first: bool) -> Result<NamedPipeServer> {
        let descriptor = Descriptor::for_current_user()?;
        let mut attrs = descriptor.attributes();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first)
            .reject_remote_clients(true)
            .in_buffer_size(PIPE_BUFFER)
            .out_buffer_size(PIPE_BUFFER);
        // SAFETY: `attrs` points at a live descriptor for the duration of the call, and
        // the pipe copies the security information it needs.
        let server = unsafe {
            options.create_with_security_attributes_raw(
                name,
                (&raw mut attrs).cast::<c_void>(),
            )
        }?;
        Ok(server)
    }

    /// Accept one connection.
    pub async fn accept(&mut self) -> Result<Connection> {
        let server = match self.next.take() {
            Some(s) => s,
            None => Self::create_instance(&self.name, false)?,
        };
        server.connect().await?;
        self.next = Some(Self::create_instance(&self.name, false)?);
        Ok(Connection::from_io(server))
    }
}

/// Create the pipe and wait for clients.
///
/// `first_pipe_instance` makes this fail if another process already owns the name, which is
/// what stops two servers from serving the same endpoint.
pub fn bind(endpoint: &Endpoint) -> Result<PipeListener> {
    let name = endpoint.pipe_name();
    let first = PipeListener::create_instance(&name, true)?;
    Ok(PipeListener { name, next: Some(first) })
}

/// Connect to `endpoint`, waiting briefly while every instance is busy.
pub async fn connect(endpoint: &Endpoint) -> Result<Connection> {
    let name = endpoint.pipe_name();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match ClientOptions::new().open(&name) {
            Ok(client) => return Ok(Connection::from_io(client)),
            Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                if std::time::Instant::now() >= deadline {
                    return Err(Error::Timeout("a free pipe instance"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(err) => return Err(Error::Io(err)),
        }
    }
}

/// Is a server listening on `endpoint`?
///
/// Unlike the Unix carrier there is no file to go stale: the name exists exactly while a
/// process holds an instance.
pub async fn probe(endpoint: &Endpoint) -> Result<bool> {
    match ClientOptions::new().open(endpoint.pipe_name()) {
        Ok(_) => Ok(true),
        Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(Error::Io(err)),
    }
}
```

`crates/sapphire-framework-ipc/src/lib.rs`:

```rust
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{PipeListener, bind, current_user_sid};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run (on Windows): `cargo test -p sapphire-framework-ipc --all-features windows`
Expected: PASS, 4 tests.

If you are developing on Linux or macOS, this task cannot be verified locally. CI must cover
it: add a Windows job if `.github/workflows/ci.yml` has none, mirroring the existing job with
`runs-on: windows-latest`. Do not mark the task done on an unverified build.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-ipc .github/workflows/ci.yml
git commit -m "feat(ipc): add the Windows named pipe carrier restricted to the current user"
```

---

### Task 6: Router and server loop

**Files:**
- Create: `crates/sapphire-framework-ipc/src/router.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `router.rs`

**Interfaces:**
- Consumes: `Connection`, `Sender` (Task 2); `Message`, `Request`, `Response`,
  `ResponsePayload`, `RpcError`, `Notification` (Task 1); `Hello`, `Welcome`, `ServerInfo`
  (Task 1)
- Produces:
  - `PeerHandle`: `async notify(&self, method: &str, params: Value) -> Result<()>`,
    `client(&self) -> &ClientInfo`
  - `RequestCtx { params: Value, peer: PeerHandle }`
  - `type HandlerFuture = Pin<Box<dyn Future<Output = std::result::Result<Value, RpcError>> + Send>>`
  - `Router`: `new()`, `method(self, name: &str, f) -> Self`, `has(&self, name: &str) -> bool`
  - `async serve(conn: Connection, router: Arc<Router>, app: &str, info: ServerInfo) -> Result<()>`

**Behaviour this task must get right:** requests are handled **concurrently** — a slow
handler must not block the ones behind it — and a handler's notifications reach the same
client while its request is still running.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-ipc/src/router.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::handshake::{ClientInfo, Hello, ManagedBy};
    use crate::message::{Message, Request, ResponsePayload};
    use serde_json::json;
    use std::sync::Arc;

    fn info() -> ServerInfo {
        ServerInfo { version: "0.0.0".into(), pid: 1, managed_by: ManagedBy::Spawned }
    }

    fn hello() -> Message {
        Message::Request(Request {
            id: 0,
            method: HANDSHAKE_METHOD.into(),
            params: serde_json::to_value(Hello {
                protocol: crate::PROTOCOL_VERSION,
                app: "test-app".into(),
                client: ClientInfo { kind: "cli".into(), version: "0.0.0".into(), pid: 2 },
            })
            .unwrap(),
        })
    }

    fn router() -> Arc<Router> {
        Arc::new(
            Router::new()
                .method("echo", |ctx| async move { Ok(ctx.params) })
                .method("slow", |ctx| async move {
                    let ms = ctx.params.get("ms").and_then(|v| v.as_u64()).unwrap_or(0);
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                    Ok(json!("done"))
                })
                .method("boom", |_| async move { Err(RpcError::internal("nope")) })
                .method("announce", |ctx| async move {
                    ctx.peer.notify("event", json!({ "hi": true })).await.ok();
                    Ok(json!(null))
                }),
        )
    }

    async fn connected() -> Connection {
        let (client, server) = Connection::pair();
        let router = router();
        tokio::spawn(async move {
            let _ = serve(server, router, "test-app", info()).await;
        });
        let mut client = client;
        client.send(hello()).await.unwrap();
        let welcome = client.recv().await.unwrap().unwrap();
        match welcome {
            Message::Response(r) => assert!(matches!(r.payload, ResponsePayload::Ok(_))),
            other => panic!("expected a welcome, got {other:?}"),
        }
        client
    }

    async fn call(client: &mut Connection, id: u64, method: &str, params: serde_json::Value) {
        client
            .send(Message::Request(Request { id, method: method.into(), params }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_request_gets_its_result() {
        let mut client = connected().await;
        call(&mut client, 1, "echo", json!({ "a": 1 })).await;
        match client.recv().await.unwrap().unwrap() {
            Message::Response(Response { id: 1, payload: ResponsePayload::Ok(v) }) => {
                assert_eq!(v, json!({ "a": 1 }));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unknown_method_is_answered_with_method_not_found() {
        let mut client = connected().await;
        call(&mut client, 1, "nope", serde_json::Value::Null).await;
        match client.recv().await.unwrap().unwrap() {
            Message::Response(Response { payload: ResponsePayload::Err(e), .. }) => {
                assert_eq!(e.code, crate::codes::METHOD_NOT_FOUND);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_failing_handler_is_answered_with_its_error() {
        let mut client = connected().await;
        call(&mut client, 9, "boom", serde_json::Value::Null).await;
        match client.recv().await.unwrap().unwrap() {
            Message::Response(Response { id: 9, payload: ResponsePayload::Err(e) }) => {
                assert_eq!(e.message, "nope");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_slow_request_does_not_block_the_one_behind_it() {
        let mut client = connected().await;
        call(&mut client, 1, "slow", json!({ "ms": 300 })).await;
        call(&mut client, 2, "echo", json!("quick")).await;

        let first = client.recv().await.unwrap().unwrap();
        match first {
            Message::Response(Response { id: 2, .. }) => {}
            other => panic!("the quick call should answer first, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_handler_can_notify_its_client() {
        let mut client = connected().await;
        call(&mut client, 1, "announce", serde_json::Value::Null).await;

        let mut saw_notification = false;
        let mut saw_response = false;
        for _ in 0..2 {
            match client.recv().await.unwrap().unwrap() {
                Message::Notification(n) => {
                    assert_eq!(n.method, "event");
                    saw_notification = true;
                }
                Message::Response(_) => saw_response = true,
                other => panic!("got {other:?}"),
            }
        }
        assert!(saw_notification && saw_response);
    }

    #[tokio::test]
    async fn a_wrong_protocol_version_is_refused() {
        let (client, server) = Connection::pair();
        tokio::spawn(async move {
            let _ = serve(server, router(), "test-app", info()).await;
        });
        let mut client = client;
        client
            .send(Message::Request(Request {
                id: 0,
                method: HANDSHAKE_METHOD.into(),
                params: json!({
                    "protocol": crate::PROTOCOL_VERSION + 1,
                    "app": "test-app",
                    "client": { "kind": "cli", "version": "0.0.0", "pid": 2 }
                }),
            }))
            .await
            .unwrap();
        match client.recv().await.unwrap().unwrap() {
            Message::Response(Response { payload: ResponsePayload::Err(e), .. }) => {
                assert!(e.message.contains("protocol"), "{}", e.message);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_request_before_the_handshake_is_refused() {
        let (client, server) = Connection::pair();
        tokio::spawn(async move {
            let _ = serve(server, router(), "test-app", info()).await;
        });
        let mut client = client;
        call(&mut client, 1, "echo", json!(1)).await;
        match client.recv().await.unwrap().unwrap() {
            Message::Response(Response { payload: ResponsePayload::Err(e), .. }) => {
                assert_eq!(e.code, crate::codes::INVALID_REQUEST);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_handshake_for_another_app_is_refused() {
        let (client, server) = Connection::pair();
        tokio::spawn(async move {
            let _ = serve(server, router(), "other-app", info()).await;
        });
        let mut client = client;
        client.send(hello()).await.unwrap();
        match client.recv().await.unwrap().unwrap() {
            Message::Response(Response { payload: ResponsePayload::Err(e), .. }) => {
                assert!(e.message.contains("other-app"), "{}", e.message);
            }
            other => panic!("got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-ipc --all-features router`
Expected: FAIL — `Router` and `serve` do not exist.

- [ ] **Step 3: Implement the router**

`crates/sapphire-framework-ipc/src/router.rs`, above the test module:

```rust
//! Dispatching requests to handlers, and the per-connection server loop.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use crate::conn::{Connection, Sender};
use crate::error::Result;
use crate::handshake::{ClientInfo, Hello, ServerInfo, Welcome};
use crate::message::{
    Message, Notification, Request, Response, ResponsePayload, RpcError, codes,
};

/// The method name carrying the handshake (spec §2.4). It is a request like any other so
/// that one code path serves everything.
pub const HANDSHAKE_METHOD: &str = "ipc.hello";

/// What a handler returns.
pub type HandlerFuture =
    Pin<Box<dyn Future<Output = std::result::Result<Value, RpcError>> + Send>>;

type Handler = Arc<dyn Fn(RequestCtx) -> HandlerFuture + Send + Sync>;

/// A handle back to the client that made a request.
#[derive(Clone, Debug)]
pub struct PeerHandle {
    sender: Sender,
    client: Arc<ClientInfo>,
}

impl PeerHandle {
    /// Send a notification to this client.
    pub async fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.sender
            .send(Message::Notification(Notification { method: method.to_owned(), params }))
            .await
    }

    /// Who is on the other end.
    pub fn client(&self) -> &ClientInfo {
        &self.client
    }
}

/// Everything a handler is given.
#[derive(Debug)]
pub struct RequestCtx {
    /// The request's parameters.
    pub params: Value,
    /// The client that sent it.
    pub peer: PeerHandle,
}

/// Maps method names to handlers.
#[derive(Clone, Default)]
pub struct Router {
    methods: HashMap<String, Handler>,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<&str> = self.methods.keys().map(String::as_str).collect();
        names.sort_unstable();
        f.debug_struct("Router").field("methods", &names).finish()
    }
}

impl Router {
    /// An empty router.
    pub fn new() -> Router {
        Router::default()
    }

    /// Register `name`. Registering the same name twice replaces the first handler.
    pub fn method<F, Fut>(mut self, name: &str, f: F) -> Router
    where
        F: Fn(RequestCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Value, RpcError>> + Send + 'static,
    {
        self.methods
            .insert(name.to_owned(), Arc::new(move |ctx| Box::pin(f(ctx)) as HandlerFuture));
        self
    }

    /// Is `name` registered?
    pub fn has(&self, name: &str) -> bool {
        self.methods.contains_key(name)
    }

    fn get(&self, name: &str) -> Option<Handler> {
        self.methods.get(name).cloned()
    }
}

/// Serve one connection until it closes.
///
/// The first request must be [`HANDSHAKE_METHOD`]; anything else is refused. After that,
/// each request is dispatched on its own task, so a slow handler does not delay the
/// requests behind it.
pub async fn serve(
    mut conn: Connection,
    router: Arc<Router>,
    app: &str,
    info: ServerInfo,
) -> Result<()> {
    let sender = conn.sender();
    let mut peer: Option<PeerHandle> = None;

    while let Some(incoming) = conn.recv().await {
        let msg = match incoming {
            Ok(msg) => msg,
            Err(err) => {
                tracing::debug!("dropping a bad frame: {err}");
                continue;
            }
        };

        let Message::Request(req) = msg else {
            // Clients do not send notifications or responses to a server.
            tracing::debug!("ignoring a non-request frame from a client");
            continue;
        };

        if req.method == HANDSHAKE_METHOD {
            match handshake(&req, app, &info) {
                Ok((welcome, client)) => {
                    peer = Some(PeerHandle { sender: sender.clone(), client: Arc::new(client) });
                    respond(&sender, req.id, ResponsePayload::Ok(welcome)).await?;
                }
                Err(err) => {
                    respond(&sender, req.id, ResponsePayload::Err(err)).await?;
                    break;
                }
            }
            continue;
        }

        let Some(peer) = peer.clone() else {
            respond(
                &sender,
                req.id,
                ResponsePayload::Err(RpcError {
                    code: codes::INVALID_REQUEST,
                    message: format!("the first request must be {HANDSHAKE_METHOD}"),
                    data: None,
                }),
            )
            .await?;
            continue;
        };

        let Some(handler) = router.get(&req.method) else {
            respond(
                &sender,
                req.id,
                ResponsePayload::Err(RpcError::method_not_found(&req.method)),
            )
            .await?;
            continue;
        };

        let sender = sender.clone();
        let Request { id, params, .. } = req;
        tokio::spawn(async move {
            let payload = match handler(RequestCtx { params, peer }).await {
                Ok(value) => ResponsePayload::Ok(value),
                Err(err) => ResponsePayload::Err(err),
            };
            let _ = respond(&sender, id, payload).await;
        });
    }

    Ok(())
}

/// Validate a handshake request, returning the welcome value to send back and the client
/// info to remember for the rest of the connection.
fn handshake(
    req: &Request,
    app: &str,
    info: &ServerInfo,
) -> std::result::Result<(Value, ClientInfo), RpcError> {
    let hello: Hello = serde_json::from_value(req.params.clone())
        .map_err(|e| RpcError::invalid_params(format!("malformed handshake: {e}")))?;
    if hello.protocol != crate::PROTOCOL_VERSION {
        return Err(RpcError {
            code: codes::INVALID_REQUEST,
            message: format!(
                "protocol version mismatch: this server speaks {}, the client speaks {}",
                crate::PROTOCOL_VERSION,
                hello.protocol
            ),
            data: Some(serde_json::json!({ "server_protocol": crate::PROTOCOL_VERSION })),
        });
    }
    if hello.app != app {
        return Err(RpcError {
            code: codes::INVALID_REQUEST,
            message: format!("this is the {app} server, not {}", hello.app),
            data: None,
        });
    }
    let welcome = Welcome { protocol: crate::PROTOCOL_VERSION, server: info.clone() };
    let value =
        serde_json::to_value(welcome).map_err(|e| RpcError::internal(e.to_string()))?;
    Ok((value, hello.client))
}

async fn respond(sender: &Sender, id: u64, payload: ResponsePayload) -> Result<()> {
    sender.send(Message::Response(Response { id, payload })).await
}
```

`crates/sapphire-framework-ipc/src/lib.rs`: add `mod router;` and
`pub use router::{HANDSHAKE_METHOD, HandlerFuture, PeerHandle, RequestCtx, Router, serve};`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features router`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-ipc
git commit -m "feat(ipc): dispatch requests concurrently through a router"
```

---

### Task 7: Client

**Files:**
- Create: `crates/sapphire-framework-ipc/src/client.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `client.rs`

**Interfaces:**
- Consumes: `Connection` (Task 2), `Router`, `serve` (Task 6), `Hello`, `Welcome`,
  `ServerInfo`, `ClientInfo` (Task 1)
- Produces:
  - `Client`: `async handshake(conn: Connection, app: &str, client: ClientInfo) -> Result<(Client, ServerInfo)>`,
    `async call<P: Serialize, R: DeserializeOwned>(&self, method: &str, params: P) -> Result<R>`,
    `fn notifications(&self) -> broadcast::Receiver<Notification>`,
    `fn server(&self) -> &ServerInfo`
  - `NOTIFICATION_CAPACITY: usize = 256`

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-ipc/src/client.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::handshake::ManagedBy;
    use crate::router::{Router, serve};
    use serde_json::json;
    use std::sync::Arc;

    fn client_info() -> ClientInfo {
        ClientInfo { kind: "cli".into(), version: "0.0.0".into(), pid: std::process::id() }
    }

    fn server_info() -> ServerInfo {
        ServerInfo { version: "0.0.0".into(), pid: 1, managed_by: ManagedBy::Spawned }
    }

    async fn connect() -> (Client, ServerInfo) {
        let (client_conn, server_conn) = Connection::pair();
        let router = Arc::new(
            Router::new()
                .method("add", |ctx| async move {
                    let a = ctx.params.get("a").and_then(|v| v.as_i64()).unwrap_or(0);
                    let b = ctx.params.get("b").and_then(|v| v.as_i64()).unwrap_or(0);
                    Ok(json!(a + b))
                })
                .method("slow", |ctx| async move {
                    let ms = ctx.params.get("ms").and_then(|v| v.as_u64()).unwrap_or(0);
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                    Ok(json!(ms))
                })
                .method("boom", |_| async move { Err(RpcError::internal("nope")) })
                .method("announce", |ctx| async move {
                    ctx.peer.notify("tick", json!({ "n": 1 })).await.ok();
                    Ok(json!(null))
                }),
        );
        tokio::spawn(async move {
            let _ = serve(server_conn, router, "test-app", server_info()).await;
        });
        Client::handshake(client_conn, "test-app", client_info()).await.unwrap()
    }

    #[tokio::test]
    async fn a_call_returns_a_typed_result() {
        let (client, info) = connect().await;
        assert_eq!(info.managed_by, ManagedBy::Spawned);
        let sum: i64 = client.call("add", json!({ "a": 2, "b": 3 })).await.unwrap();
        assert_eq!(sum, 5);
    }

    #[tokio::test]
    async fn calls_in_flight_together_are_matched_by_id() {
        let (client, _) = connect().await;
        let slow = client.call::<_, u64>("slow", json!({ "ms": 200 }));
        let quick = client.call::<_, i64>("add", json!({ "a": 1, "b": 1 }));
        let (slow, quick) = tokio::join!(slow, quick);
        assert_eq!(slow.unwrap(), 200);
        assert_eq!(quick.unwrap(), 2);
    }

    #[tokio::test]
    async fn a_remote_error_surfaces_as_an_rpc_error() {
        let (client, _) = connect().await;
        let err = client.call::<_, serde_json::Value>("boom", json!(null)).await.unwrap_err();
        match err {
            Error::Rpc(e) => assert_eq!(e.message, "nope"),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn notifications_reach_a_subscriber() {
        let (client, _) = connect().await;
        let mut events = client.notifications();
        let _: serde_json::Value = client.call("announce", json!(null)).await.unwrap();
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n.method, "tick");
    }

    #[tokio::test]
    async fn a_pending_call_fails_when_the_server_goes_away() {
        let (client_conn, server_conn) = Connection::pair();
        let router = Arc::new(Router::new().method("slow", |_| async move {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            Ok(json!(null))
        }));
        let server = tokio::spawn(async move {
            let _ = serve(server_conn, router, "test-app", server_info()).await;
        });
        let (client, _) =
            Client::handshake(client_conn, "test-app", client_info()).await.unwrap();

        let pending = tokio::spawn(async move {
            client.call::<_, serde_json::Value>("slow", json!(null)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        server.abort();

        let err = pending.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Closed), "got {err:?}");
    }

    #[tokio::test]
    async fn a_refused_handshake_is_reported() {
        let (client_conn, server_conn) = Connection::pair();
        let router = Arc::new(Router::new());
        tokio::spawn(async move {
            let _ = serve(server_conn, router, "other-app", server_info()).await;
        });
        let err = Client::handshake(client_conn, "test-app", client_info()).await.unwrap_err();
        assert!(matches!(err, Error::Rpc(_)), "got {err:?}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-ipc --all-features client`
Expected: FAIL — `Client` does not exist.

- [ ] **Step 3: Implement the client**

`crates/sapphire-framework-ipc/src/client.rs`, above the test module:

```rust
//! The calling side of a connection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{broadcast, oneshot};

use crate::conn::{Connection, Sender};
use crate::error::{Error, Result};
use crate::handshake::{ClientInfo, Hello, ServerInfo, Welcome};
use crate::message::{
    Message, Notification, Request, ResponsePayload, RpcError,
};
use crate::router::HANDSHAKE_METHOD;

/// How many notifications may queue for a subscriber before it starts losing the oldest.
pub const NOTIFICATION_CAPACITY: usize = 256;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<std::result::Result<serde_json::Value, RpcError>>>>>;

/// A connected client.
///
/// Cloning is not provided; share it behind an `Arc`. All methods take `&self`, so one
/// `Arc<Client>` serves any number of concurrent callers.
#[derive(Debug)]
pub struct Client {
    sender: Sender,
    pending: Pending,
    events: broadcast::Sender<Notification>,
    next_id: AtomicU64,
    server: ServerInfo,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Client {
    /// Perform the handshake on an established connection.
    pub async fn handshake(
        mut conn: Connection,
        app: &str,
        client: ClientInfo,
    ) -> Result<(Client, ServerInfo)> {
        let sender = conn.sender();
        let hello = Hello {
            protocol: crate::PROTOCOL_VERSION,
            app: app.to_owned(),
            client,
        };
        sender
            .send(Message::Request(Request {
                id: 0,
                method: HANDSHAKE_METHOD.to_owned(),
                params: serde_json::to_value(hello)?,
            }))
            .await?;

        let welcome: Welcome = loop {
            match conn.recv().await {
                None => return Err(Error::Closed),
                Some(Err(err)) => return Err(err),
                Some(Ok(Message::Response(resp))) if resp.id == 0 => match resp.payload {
                    ResponsePayload::Ok(value) => break serde_json::from_value(value)?,
                    ResponsePayload::Err(err) => return Err(Error::Rpc(err)),
                },
                Some(Ok(_)) => continue,
            }
        };

        if welcome.protocol != crate::PROTOCOL_VERSION {
            return Err(Error::VersionMismatch {
                ours: crate::PROTOCOL_VERSION,
                theirs: welcome.protocol,
            });
        }

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (events, _) = broadcast::channel(NOTIFICATION_CAPACITY);

        let reader = tokio::spawn({
            let pending = Arc::clone(&pending);
            let events = events.clone();
            async move {
                let mut conn = conn;
                while let Some(incoming) = conn.recv().await {
                    match incoming {
                        Ok(Message::Response(resp)) => {
                            let waiter = pending.lock().expect("pending mutex").remove(&resp.id);
                            if let Some(tx) = waiter {
                                let payload = match resp.payload {
                                    ResponsePayload::Ok(v) => Ok(v),
                                    ResponsePayload::Err(e) => Err(e),
                                };
                                let _ = tx.send(payload);
                            } else {
                                tracing::debug!(id = resp.id, "response for an unknown request");
                            }
                        }
                        Ok(Message::Notification(n)) => {
                            let _ = events.send(n);
                        }
                        Ok(Message::Request(req)) => {
                            tracing::debug!(method = %req.method, "ignoring a request from a server");
                        }
                        Err(err) => tracing::debug!("dropping a bad frame: {err}"),
                    }
                }
                // The connection closed: fail every pending call rather than leaving
                // callers waiting forever.
                let waiters: Vec<_> =
                    pending.lock().expect("pending mutex").drain().map(|(_, tx)| tx).collect();
                drop(waiters);
            }
        });

        let server = welcome.server.clone();
        let client = Client {
            sender,
            pending,
            events,
            next_id: AtomicU64::new(1),
            server: welcome.server,
            reader,
        };
        Ok((client, server))
    }

    /// Call a method and deserialise its result.
    pub async fn call<P, R>(&self, method: &str, params: P) -> Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().expect("pending mutex").insert(id, tx);

        let send = self
            .sender
            .send(Message::Request(Request {
                id,
                method: method.to_owned(),
                params: serde_json::to_value(params)?,
            }))
            .await;
        if let Err(err) = send {
            self.pending.lock().expect("pending mutex").remove(&id);
            return Err(err);
        }

        match rx.await {
            Ok(Ok(value)) => Ok(serde_json::from_value(value)?),
            Ok(Err(err)) => Err(Error::Rpc(err)),
            Err(_) => Err(Error::Closed),
        }
    }

    /// Subscribe to server notifications. Each subscriber gets its own receiver, and one
    /// that falls [`NOTIFICATION_CAPACITY`] behind observes a lag rather than blocking the
    /// reader.
    pub fn notifications(&self) -> broadcast::Receiver<Notification> {
        self.events.subscribe()
    }

    /// What the server said about itself during the handshake.
    pub fn server(&self) -> &ServerInfo {
        &self.server
    }
}
```

`crates/sapphire-framework-ipc/src/lib.rs`: add `mod client;` and
`pub use client::{Client, NOTIFICATION_CAPACITY};`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features client`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-ipc
git commit -m "feat(ipc): add the client with multiplexed calls and notifications"
```

---

### Task 8: Start-on-demand

**Files:**
- Create: `crates/sapphire-framework-ipc/src/spawn.rs`
- Create: `crates/sapphire-framework-ipc/src/bin/ipc-test-server.rs`
- Modify: `crates/sapphire-framework-ipc/src/lib.rs`
- Test: `crates/sapphire-framework-ipc/tests/spawn.rs` (the spawn tests live in an
  integration-test target, not inline in `spawn.rs` — Cargo defines `CARGO_BIN_EXE_*`
  only for integration-test/bench targets, so inline lib unit tests cannot read it);
  `crates/sapphire-framework-ipc/tests/race.rs`

**Interfaces:**
- Consumes: `Client` (Task 7), `Endpoint` (Task 3), the platform `connect` / `probe`
  (Tasks 4 and 5), `ManagedBy`, `ClientInfo` (Task 1)
- Produces:
  - `SpawnConfig { exe: PathBuf, args: Vec<String>, allow_spawn: bool, connect_timeout: Duration, lock_timeout: Duration, stale_lock_age: Duration }`
    with `Default` (`exe` = `current_exe()`, `args` = `["server", "run"]`, `allow_spawn` =
    `true`, both timeouts 10 s, `stale_lock_age` = `STALE_LOCK_AGE`) and `disabled()`
    (`allow_spawn: false`)
  - `SHUTDOWN_METHOD: &str = "server.shutdown"` — the method a client calls to retire a
    spawned server of the wrong version. `-ipc` only names it; `-server` implements it.
  - `async connect(endpoint: &Endpoint) -> Result<Connection>` — the platform carrier,
    re-exported so callers need no `cfg`
  - `async probe(endpoint: &Endpoint) -> Result<bool>` — likewise
  - `async ensure_server(endpoint: &Endpoint, app: &str, client: ClientInfo, spawn: &SpawnConfig) -> Result<(Client, ServerInfo)>`
  - `STALE_LOCK_AGE: Duration = 30s`

**The three hazards this task addresses** (spec §2.6): a start race, a version mismatch, and
a stale socket. The lock file is created with `create_new`, which is atomic on every
platform, so exactly one racing process wins; the losers loop back and connect to the
server the winner started.

- [ ] **Step 1: Write the test-only server binary**

`crates/sapphire-framework-ipc/src/bin/ipc-test-server.rs`:

```rust
//! A minimal server used by the integration tests.
//!
//! Usage: `ipc-test-server <runtime-dir> <app-name> [--version <v>] [--service]`
//!
//! Serves `ping` (returns `"pong"`), `pid` (returns this process's id) and `sleep`
//! (waits for `params.ms` milliseconds), then exits when the listener is dropped.

use std::sync::Arc;

use sapphire_framework_ipc::{Endpoint, ManagedBy, Router, ServerInfo, serve};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("a runtime directory");
    let app = args.next().expect("an app name");
    let mut version = "0.0.0".to_owned();
    let mut managed_by = ManagedBy::Spawned;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" => version = args.next().expect("a version"),
            "--service" => managed_by = ManagedBy::Service,
            other => panic!("unexpected argument {other}"),
        }
    }

    let endpoint = Endpoint::in_dir(app.clone(), dir.into());
    let info = ServerInfo { version, pid: std::process::id(), managed_by };
    let router = Arc::new(
        Router::new()
            .method("ping", |_| async move { Ok(serde_json::json!("pong")) })
            .method("pid", |_| async move { Ok(serde_json::json!(std::process::id())) })
            .method("sleep", |ctx| async move {
                let ms = ctx.params.get("ms").and_then(|v| v.as_u64()).unwrap_or(0);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                Ok(serde_json::json!(ms))
            }),
    );

    serve_forever(&endpoint, router, &app, info).await
}

#[cfg(unix)]
async fn serve_forever(
    endpoint: &Endpoint,
    router: Arc<Router>,
    app: &str,
    info: ServerInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = sapphire_framework_ipc::bind(endpoint).await?;
    loop {
        let conn = listener.accept().await?;
        let (router, app, info) = (Arc::clone(&router), app.to_owned(), info.clone());
        tokio::spawn(async move {
            let _ = serve(conn, router, &app, info).await;
        });
    }
}

#[cfg(windows)]
async fn serve_forever(
    endpoint: &Endpoint,
    router: Arc<Router>,
    app: &str,
    info: ServerInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = sapphire_framework_ipc::bind(endpoint)?;
    loop {
        let conn = listener.accept().await?;
        let (router, app, info) = (Arc::clone(&router), app.to_owned(), info.clone());
        tokio::spawn(async move {
            let _ = serve(conn, router, &app, info).await;
        });
    }
}
```

- [ ] **Step 2: Write the failing tests**

`crates/sapphire-framework-ipc/tests/spawn.rs` (integration-test target — see the
Files note: `env!("CARGO_BIN_EXE_ipc-test-server")` only resolves in test targets):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> ClientInfo {
        ClientInfo { kind: "cli".into(), version: "0.0.0".into(), pid: std::process::id() }
    }

    fn test_server(dir: &std::path::Path, app: &str, extra: &[&str]) -> SpawnConfig {
        let mut args = vec![dir.display().to_string(), app.to_owned()];
        args.extend(extra.iter().map(|s| (*s).to_owned()));
        SpawnConfig {
            exe: env!("CARGO_BIN_EXE_ipc-test-server").into(),
            args,
            ..SpawnConfig::default()
        }
    }

    #[tokio::test]
    async fn a_server_is_started_when_none_is_listening() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = Endpoint::in_dir("race-app-1", tmp.path().to_path_buf());
        let cfg = test_server(tmp.path(), "race-app-1", &[]);

        let (client, _) = ensure_server(&ep, "race-app-1", info(), &cfg).await.unwrap();
        let pong: String = client.call("ping", serde_json::Value::Null).await.unwrap();
        assert_eq!(pong, "pong");
    }

    #[tokio::test]
    async fn a_second_client_reuses_the_running_server() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = Endpoint::in_dir("race-app-2", tmp.path().to_path_buf());
        let cfg = test_server(tmp.path(), "race-app-2", &[]);

        let (first, _) = ensure_server(&ep, "race-app-2", info(), &cfg).await.unwrap();
        let (second, _) = ensure_server(&ep, "race-app-2", info(), &cfg).await.unwrap();

        let a: u32 = first.call("pid", serde_json::Value::Null).await.unwrap();
        let b: u32 = second.call("pid", serde_json::Value::Null).await.unwrap();
        assert_eq!(a, b, "both clients should have reached the same server");
    }

    #[tokio::test]
    async fn spawning_can_be_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = Endpoint::in_dir("race-app-3", tmp.path().to_path_buf());
        let cfg = SpawnConfig { allow_spawn: false, ..test_server(tmp.path(), "race-app-3", &[]) };

        let err = ensure_server(&ep, "race-app-3", info(), &cfg).await.unwrap_err();
        assert!(matches!(err, Error::Spawn(_)), "got {err:?}");
    }

    /// A lock left behind by a process that died must not block starts forever.
    ///
    /// `std::fs` cannot backdate a file, so rather than faking the clock the staleness
    /// threshold is a field on `SpawnConfig` and the test sets it to zero. Making it
    /// configurable is useful anyway: an application that knows its server starts quickly
    /// can shorten it.
    #[tokio::test]
    async fn a_stale_lock_file_does_not_block_a_start() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = Endpoint::in_dir("race-app-4", tmp.path().to_path_buf());
        std::fs::write(ep.lock_path(), "999999").unwrap();

        let cfg = SpawnConfig {
            stale_lock_age: Duration::ZERO,
            ..test_server(tmp.path(), "race-app-4", &[])
        };
        let (client, _) = ensure_server(&ep, "race-app-4", info(), &cfg).await.unwrap();
        let pong: String = client.call("ping", serde_json::Value::Null).await.unwrap();
        assert_eq!(pong, "pong");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-ipc --all-features spawn`
Expected: FAIL — `ensure_server` does not exist.

- [ ] **Step 4: Implement start-on-demand**

`crates/sapphire-framework-ipc/src/spawn.rs`, above the test module:

```rust
//! Connecting to a server, starting one if necessary (spec §2.6).

use std::path::PathBuf;
use std::time::Duration;

use crate::client::Client;
use crate::conn::Connection;
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::handshake::{ClientInfo, ManagedBy, ServerInfo};

/// A lock file older than this is assumed to belong to a process that died before
/// releasing it.
pub const STALE_LOCK_AGE: Duration = Duration::from_secs(30);

/// The method a client calls to retire a spawned server of the wrong version.
///
/// This crate only names it, so that both sides agree; `sapphire-framework-server`
/// implements it. A server that does not is simply waited for and then reported as a
/// timeout, which is the right outcome — an unresponsive server of the wrong version is not
/// something a client may work around.
pub const SHUTDOWN_METHOD: &str = "server.shutdown";

/// Connect to `endpoint` using this platform's carrier.
pub async fn connect(endpoint: &Endpoint) -> Result<Connection> {
    #[cfg(unix)]
    {
        crate::unix::connect(endpoint).await
    }
    #[cfg(windows)]
    {
        crate::windows::connect(endpoint).await
    }
}

/// Is a server listening on `endpoint`? Clears a stale Unix socket file as a side effect.
pub async fn probe(endpoint: &Endpoint) -> Result<bool> {
    #[cfg(unix)]
    {
        crate::unix::probe(endpoint).await
    }
    #[cfg(windows)]
    {
        crate::windows::probe(endpoint).await
    }
}

/// How to start a server that is not running.
#[derive(Clone, Debug)]
pub struct SpawnConfig {
    /// The executable to run. Defaults to this process's own, so one binary serves as both
    /// the client and the server.
    pub exe: PathBuf,
    /// Arguments that make `exe` run as a server.
    pub args: Vec<String>,
    /// When `false`, a missing server is an error instead of something to fix. Set this for
    /// an app configured with privilege separation, whose server runs as root and cannot be
    /// started by a user's CLI (spec §2.6, §3).
    pub allow_spawn: bool,
    /// How long to wait for a spawned server's socket to appear.
    pub connect_timeout: Duration,
    /// How long to wait for the spawn lock.
    pub lock_timeout: Duration,
    /// How old a lock file must be before it is treated as abandoned.
    pub stale_lock_age: Duration,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        SpawnConfig {
            exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("sapphire")),
            args: vec!["server".to_owned(), "run".to_owned()],
            allow_spawn: true,
            connect_timeout: Duration::from_secs(10),
            lock_timeout: Duration::from_secs(10),
            stale_lock_age: STALE_LOCK_AGE,
        }
    }
}

impl SpawnConfig {
    /// A configuration that never starts a server.
    pub fn disabled() -> SpawnConfig {
        SpawnConfig { allow_spawn: false, ..SpawnConfig::default() }
    }
}

/// Connect to `endpoint`'s server, starting it if nothing is listening.
///
/// On a protocol or version mismatch with a **spawned** server, the running server is asked
/// to exit and a new one is started. A server started by the OS service manager is never
/// replaced; the caller is told to restart the service instead.
pub async fn ensure_server(
    endpoint: &Endpoint,
    app: &str,
    client: ClientInfo,
    spawn: &SpawnConfig,
) -> Result<(Client, ServerInfo)> {
    // 1-2. Something listening? Use it, unless it is the wrong version.
    if probe(endpoint).await? {
        match handshake_with(endpoint, app, client.clone()).await {
            Ok((c, info)) if info.version == client.version => return Ok((c, info)),
            Ok((c, info)) => match info.managed_by {
                ManagedBy::Service => {
                    return Err(Error::ServiceVersionMismatch {
                        running: info.version,
                        ours: client.version,
                    });
                }
                ManagedBy::Spawned => {
                    tracing::info!(
                        running = %info.version,
                        ours = %client.version,
                        "replacing a spawned server of a different version"
                    );
                    let _: std::result::Result<serde_json::Value, _> =
                        c.call(SHUTDOWN_METHOD, serde_json::Value::Null).await;
                    drop(c);
                    wait_until_gone(endpoint, spawn.connect_timeout).await?;
                }
            },
            // A half-open socket, or a server shutting down: fall through and start one.
            Err(Error::Io(_)) | Err(Error::Closed) => {}
            Err(err) => return Err(err),
        }
    }

    // 3. Nothing listening. Serialise the start.
    if !spawn.allow_spawn {
        return Err(Error::Spawn(format!(
            "no {app} server is running, and this process is not allowed to start one"
        )));
    }

    let _guard = SpawnLock::acquire(endpoint, spawn).await?;

    // 3b. Another process may have won the race and started it while we waited.
    if probe(endpoint).await? {
        return handshake_with(endpoint, app, client).await;
    }

    // 3c. Start it.
    let mut command = tokio::process::Command::new(&spawn.exe);
    command.args(&spawn.args);
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::null());
    command.stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        // Detach from this process's session so the server outlives the CLI.
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid is async-signal-safe and is the documented way to detach.
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            })
        };
    }
    let child = command.spawn().map_err(|e| {
        Error::Spawn(format!("could not run {}: {e}", spawn.exe.display()))
    })?;
    // The server outlives us; do not reap it.
    drop(child);

    // 3d. Wait for it.
    let deadline = tokio::time::Instant::now() + spawn.connect_timeout;
    let mut delay = Duration::from_millis(10);
    loop {
        if probe(endpoint).await.unwrap_or(false)
            && let Ok(pair) = handshake_with(endpoint, app, client.clone()).await
        {
            return Ok(pair);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Timeout("the server to start listening"));
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_millis(250));
    }
}

async fn handshake_with(
    endpoint: &Endpoint,
    app: &str,
    client: ClientInfo,
) -> Result<(Client, ServerInfo)> {
    let conn = connect(endpoint).await?;
    Client::handshake(conn, app, client).await
}

async fn wait_until_gone(endpoint: &Endpoint, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while probe(endpoint).await.unwrap_or(false) {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Timeout("the old server to exit"));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

/// The lock that serialises start-on-demand.
///
/// `create_new` is atomic on every platform we target, so of several processes trying at
/// once exactly one creates the file and the rest wait, then find the server already up.
struct SpawnLock {
    path: PathBuf,
}

impl SpawnLock {
    async fn acquire(endpoint: &Endpoint, spawn: &SpawnConfig) -> Result<SpawnLock> {
        let path = endpoint.lock_path();
        let deadline = tokio::time::Instant::now() + spawn.lock_timeout;
        loop {
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    use std::io::Write;
                    let _ = write!(file, "{}", std::process::id());
                    return Ok(SpawnLock { path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if lock_is_stale(&path, spawn.stale_lock_age) {
                        tracing::debug!(path = %path.display(), "clearing an abandoned spawn lock");
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Timeout("the spawn lock"));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => return Err(Error::Io(err)),
            }
        }
    }
}

impl Drop for SpawnLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn lock_is_stale(path: &std::path::Path, max_age: Duration) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return true;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    modified.elapsed().map(|age| age >= max_age).unwrap_or(false)
}
```

`crates/sapphire-framework-ipc/src/lib.rs`: add `mod spawn;` and
`pub use spawn::{SHUTDOWN_METHOD, STALE_LOCK_AGE, SpawnConfig, connect, ensure_server, probe};`.

- [ ] **Step 5: Run the unit tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features spawn`
Expected: PASS, 4 tests.

- [ ] **Step 6: Write the start-race integration test**

`crates/sapphire-framework-ipc/tests/race.rs`:

```rust
//! Several clients starting one server at the same time (spec §2.6, steps 3a-3b).

use sapphire_framework_ipc::{ClientInfo, Endpoint, SpawnConfig, ensure_server};

fn client_info() -> ClientInfo {
    ClientInfo { kind: "cli".into(), version: "0.0.0".into(), pid: std::process::id() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_simultaneous_clients_produce_one_server() {
    let tmp = tempfile::tempdir().unwrap();
    let app = "race-integration";
    let endpoint = Endpoint::in_dir(app, tmp.path().to_path_buf());
    let config = SpawnConfig {
        exe: env!("CARGO_BIN_EXE_ipc-test-server").into(),
        args: vec![tmp.path().display().to_string(), app.to_owned()],
        ..SpawnConfig::default()
    };

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (endpoint, config) = (endpoint.clone(), config.clone());
        tasks.push(tokio::spawn(async move {
            let (client, _) = ensure_server(&endpoint, app, client_info(), &config)
                .await
                .expect("a server");
            client.call::<_, u32>("pid", serde_json::Value::Null).await.expect("a pid")
        }));
    }

    let mut pids = Vec::new();
    for task in tasks {
        pids.push(task.await.expect("the task"));
    }

    let first = pids[0];
    assert!(
        pids.iter().all(|pid| *pid == first),
        "every client should have reached one server, got {pids:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_managed_server_of_another_version_is_not_replaced() {
    let tmp = tempfile::tempdir().unwrap();
    let app = "service-version";
    let endpoint = Endpoint::in_dir(app, tmp.path().to_path_buf());
    let exe: std::path::PathBuf = env!("CARGO_BIN_EXE_ipc-test-server").into();

    // Start a "service" server claiming version 9.9.9.
    let mut child = tokio::process::Command::new(&exe)
        .args([
            tmp.path().display().to_string(),
            app.to_owned(),
            "--version".to_owned(),
            "9.9.9".to_owned(),
            "--service".to_owned(),
        ])
        .spawn()
        .expect("the test server");

    // Wait for it to listen.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !sapphire_framework_ipc::probe(&endpoint).await.unwrap_or(false) {
        assert!(std::time::Instant::now() < deadline, "the server never started");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let config = SpawnConfig {
        exe,
        args: vec![tmp.path().display().to_string(), app.to_owned()],
        ..SpawnConfig::default()
    };
    let err = ensure_server(&endpoint, app, client_info(), &config)
        .await
        .expect_err("a version mismatch against a service");
    let message = err.to_string();
    assert!(message.contains("restart the service"), "{message}");

    child.kill().await.ok();
}
```

- [ ] **Step 7: Run the integration tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features --test race`
Expected: PASS, 2 tests.

If `eight_simultaneous_clients_produce_one_server` is flaky, the bug is real and is in
`SpawnLock::acquire` or in step 3b — do not add sleeps to the test to make it pass. Run it
twenty times (`for i in $(seq 20); do cargo test --all-features --test race || break; done`)
before treating it as green.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-ipc
git commit -m "feat(ipc): start a server on demand, serialised by a spawn lock"
```

---

### Task 9: End-to-end round trip over every carrier

**Files:**
- Modify: `crates/sapphire-framework-ipc/tests/roundtrip.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–8.

This task adds no production code. It proves the pieces work together over each carrier, so
that the `-server` plan can build on the crate without re-testing the transport.

- [ ] **Step 1: Write the tests**

Append to `crates/sapphire-framework-ipc/tests/roundtrip.rs`:

```rust
use std::sync::Arc;

use sapphire_framework_ipc::{
    Client, ClientInfo, Connection, Endpoint, ManagedBy, Router, ServerInfo, serve,
};

fn client_info() -> ClientInfo {
    ClientInfo { kind: "cli".into(), version: "0.0.0".into(), pid: std::process::id() }
}

fn server_info() -> ServerInfo {
    ServerInfo { version: "0.0.0".into(), pid: std::process::id(), managed_by: ManagedBy::Spawned }
}

fn router() -> Arc<Router> {
    Arc::new(
        Router::new()
            .method("echo", |ctx| async move { Ok(ctx.params) })
            .method("announce", |ctx| async move {
                ctx.peer.notify("tick", serde_json::json!({ "n": 1 })).await.ok();
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
    let (client, _) = Client::handshake(client_conn, "test-app", client_info()).await.unwrap();

    let echoed: serde_json::Value =
        client.call("echo", serde_json::json!({ "hello": "world" })).await.unwrap();
    assert_eq!(echoed, serde_json::json!({ "hello": "world" }));
}

/// A large payload travels as a JSON string (spec §2.2).
#[tokio::test]
async fn a_multi_megabyte_payload_round_trips() {
    let (client_conn, server_conn) = Connection::pair();
    tokio::spawn(async move {
        let _ = serve(server_conn, router(), "test-app", server_info()).await;
    });
    let (client, _) = Client::handshake(client_conn, "test-app", client_info()).await.unwrap();

    let big = "y".repeat(4 * 1024 * 1024);
    let echoed: serde_json::Value =
        client.call("echo", serde_json::json!({ "content": big })).await.unwrap();
    assert_eq!(echoed["content"].as_str().unwrap().len(), 4 * 1024 * 1024);
}

/// Notifications reach every subscriber of a connection.
#[tokio::test]
async fn two_subscribers_both_see_a_notification() {
    let (client_conn, server_conn) = Connection::pair();
    tokio::spawn(async move {
        let _ = serve(server_conn, router(), "test-app", server_info()).await;
    });
    let (client, _) = Client::handshake(client_conn, "test-app", client_info()).await.unwrap();

    let mut a = client.notifications();
    let mut b = client.notifications();
    let _: serde_json::Value = client.call("announce", serde_json::Value::Null).await.unwrap();

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
            let Ok(conn) = listener.accept().await else { break };
            tokio::spawn(async move {
                let _ = serve(conn, router(), "roundtrip-app", server_info()).await;
            });
        }
    });

    let conn = sapphire_framework_ipc::connect(&endpoint).await.unwrap();
    let (client, info) =
        Client::handshake(conn, "roundtrip-app", client_info()).await.unwrap();
    assert_eq!(info.managed_by, ManagedBy::Spawned);

    let echoed: serde_json::Value =
        client.call("echo", serde_json::json!([1, 2, 3])).await.unwrap();
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
            let Ok(conn) = listener.accept().await else { break };
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
            let (client, _) =
                Client::handshake(conn, "concurrent-app", client_info()).await.unwrap();
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
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-ipc --all-features --test roundtrip`
Expected: PASS, 6 tests (including `dependency_surface_is_documented` from Task 1).

- [ ] **Step 3: Run the whole suite and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-ipc
git commit -m "test(ipc): cover every carrier end to end"
```

---

### Task 10: Document the crate in `ARCHITECTURE.md`

**Files:**
- Modify: `docs/ARCHITECTURE.md`

`ARCHITECTURE.md` is in Japanese and grandfathered by `CONTRIBUTING.md`; the surrounding
sections stay as they are. Add the new crate to the crate table only, in Japanese to match
its neighbours.

- [ ] **Step 1: Add the row**

In the `## crate 構成（目標）` table, after the `sapphire-framework-rpc` row:

```markdown
| `sapphire-framework-ipc` | ローカル IPC（UDS / 名前付きパイプ / プロセス内チャネル上の JSON-RPC、ルータ、自動起動） | ✅ |
```

- [ ] **Step 2: Add a pointer to the spec**

At the end of the `## remote 同期 API（JSON-RPC・実装済み）` section, add:

```markdown
> **2026-09-16 以降の方針**: アプリのキャッシュ（redb）を開くプロセスを 1 つに絞るため、
> サーバを CLI / desktop の依存に格上げする。CLI・stdio MCP・desktop は
> `sapphire-framework-ipc` 経由でアプリサーバに接続し、ホストごとの常駐 `sapphire-bridge`
> が同期を仲介する。設計は
> `docs/superpowers/specs/2026-09-16-process-architecture-design.md`。
```

- [ ] **Step 3: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs(architecture): record the IPC crate and the process-architecture spec"
```

---

## What this plan does not cover

These are the remaining steps of the spec's §9, each needing its own plan:

| Spec step | Plan | Depends on |
|---|---|---|
| 2 — Registry (users removed, one file per record, `node_id`) | independent of this one | — |
| 4 — App server skeleton (`-server`): `workspace.*`, many workspaces, `IpcBackend`, `ServerCommand` | next; **this is the milestone that solves the original problem** | this plan |
| 5 — Privilege separation (spec §3, Unix only) | after step 4 | step 4 |
| 6 — Bridge basics (`-bridge`): directory, single instance, control and data planes, iroh | after step 4 | this plan, step 2 |
| 7 — Sync runtime in the app server | after step 6 | steps 4, 6 |
| 8 — Pairing and workgroups | after step 7 | step 6 |
| 9 — Server features: embedded relay, `wake_on_sync` | after step 8 | step 6 |
| 10 — Service installation with `run_as` / `helper_as` | after step 5 | step 5 |
| 11 — Cleanup: extract `-keys`, remove `-rpc` / `-remote-*` / `-blob`, the §7 directory migration, facade features, rewrite `ARCHITECTURE.md` | last | all |
