# App Server Skeleton (`sapphire-framework-server`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Make the server the only process that opens an app's cache, and give the CLI, the
stdio MCP server and the desktop a way to reach it — so that `sapphire-journal add` and the
journal's MCP server can run at the same time, which today they cannot.

**Architecture:** `AppServer` listens on the app's endpoint and serves a `workspace.*`
namespace that mirrors `WorkspaceBackend` method for method. Behind it, a `WorkspaceHost`
keeps one `LocalBackend` per workspace root, opening them lazily and closing the
least-recently-used ones, so one process serves every workspace of the app. On the other side
`IpcBackend` implements the same `WorkspaceBackend` trait over the wire, so a desktop app
changes one line to become an IPC client. Applications add their own methods to the same
router.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), `sapphire-framework-ipc`, tokio 1, serde +
serde_json, async-trait, clap 4, thiserror 2, tracing; dev: tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-16-process-architecture-design.md` — §4 in full, and
§2.6's `SHUTDOWN_METHOD` contract. Implementation order step 4 of that spec's §9.

**Depends on:** `docs/superpowers/plans/2026-09-16-ipc-layer-plan.md` (step 3) must be
complete. This plan uses `Client`, `Connection`, `Endpoint`, `Router`, `serve`, `bind`,
`ensure_server`, `SpawnConfig`, `ServerInfo`, `ClientInfo`, `ManagedBy`, `RpcError`,
`SHUTDOWN_METHOD` from it.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task. Commit
  `Cargo.lock` whenever dependencies change.
- Crate version follows the workspace (`version.workspace = true`); path dependencies use
  `version = "0.14.0"`.
- Every public item carries a doc comment. Both new crates' modules have `#![warn(missing_docs)]`.
- `AppContext` is always `&'static` in this codebase (`Workspace::from_root` requires it).
  Every signature here takes `&'static AppContext`.
- Paths cross the wire as `PathBuf`, which serde encodes as a string. A path that is not valid
  UTF-8 therefore fails to serialise. That is acceptable — the workspace already treats paths
  as `String` in the retrieve index — but the failure must be an `INVALID_PARAMS` error, not a
  panic.
- **Sync, privilege separation, the bridge and `service install` are not in this plan.** They
  are steps 5, 6, 7 and 10. `sync.enable` / `sync.disable` / `sync.status` are not implemented
  here.

## Deviations from the spec, agreed up front

**Spec §4 shows `AppServer::new(ctx).namespace("journal", journal_handlers)`.** This plan uses
`AppServer::extend(|router| …)`, where the application registers its own fully-qualified
method names (`journal.create_entry`). A prefixing `namespace` wrapper would need the router to
support merging with a prefix, which `-ipc` deliberately does not do, and buys nothing: the
app writes the prefix once per method instead of once per server. The naming convention —
app methods are `<app>.<method>` — is documented and tested rather than enforced by a type.

## File Structure

```
crates/sapphire-framework-backend/src/
    protocol.rs     # NEW: the workspace.* method names and their params/results
    ipc.rs          # NEW: IpcBackend — WorkspaceBackend over sapphire-framework-ipc
    lib.rs          # MODIFIED: BackendEvent gains serde; new modules re-exported

crates/sapphire-framework-workspace/src/
    workspace_state.rs   # MODIFIED: SearchMode gains serde

crates/sapphire-framework-server/
    Cargo.toml
    src/
        lib.rs      # AppServer, module wiring
        error.rs    # Error, Result
        host.rs     # WorkspaceHost: one LocalBackend per root, LRU + idle eviction
        handlers.rs # the workspace.* handlers
        events.rs   # workspace.subscribe and the notification pump
        command.rs  # ServerCommand (clap)
    src/bin/
        server-test-app.rs   # test-only app server (required-features = ["test-util"])
    tests/
        concurrent.rs        # the regression test this whole design exists for
```

---

### Task 1: Wire types

**Files:**
- Create: `crates/sapphire-framework-backend/src/protocol.rs`
- Modify: `crates/sapphire-framework-backend/src/lib.rs` (serde on `BackendEvent` and
  `SyncSummary`, new module)
- Modify: `crates/sapphire-framework-backend/Cargo.toml` (depend on `-ipc`)
- Modify: `crates/sapphire-framework-workspace/src/workspace_state.rs` (serde on `SearchMode`)
- Test: inline `#[cfg(test)] mod tests` in `protocol.rs`

**Interfaces:**
- Produces, in `sapphire_backend::protocol`:
  - method-name constants `SEARCH`, `READ_FILE`, `WRITE_FILE`, `APPEND_FILE`, `DELETE_FILE`,
    `LIST_DIR`, `REINDEX`, `SUBSCRIBE`, `SERVER_INFO`, and the event notification name `EVENT`
  - `SearchParams { ws: PathBuf, query: String, limit: usize, mode: SearchMode }`
  - `SearchResult { hits: Vec<FileSearchResult> }`
  - `PathParams { ws: PathBuf, path: PathBuf }`
  - `ContentParams { ws: PathBuf, path: PathBuf, content: String }`
  - `WsParams { ws: PathBuf }`
  - `DirEntry { path: PathBuf, is_dir: bool }`, `ListDirResult { entries: Vec<DirEntry> }`
  - `ReindexResult { upserted: usize, removed: usize }`
  - `Ack {}` — the empty success payload, so that a handler returning "nothing" still returns
    a JSON object rather than `null`
  - `EventParams { ws: PathBuf, event: BackendEvent }`
- Also produces: `BackendEvent` and `SyncSummary` gain `Serialize`/`Deserialize`;
  `SearchMode` gains `Serialize`/`Deserialize` with `#[serde(rename_all = "lowercase")]`

**Why these live in `-backend` and not `-ipc`:** `-ipc` must stay free of the workspace and
search stacks so the bridge can use it (spec §2). `-backend` already owns `WorkspaceBackend`,
`FileSearchResult` and `SearchMode`, so it is the one place where both sides of this namespace
can name the same types without duplication.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-backend/src/protocol.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T) -> T
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap()
    }

    #[test]
    fn search_parameters_round_trip() {
        let params = SearchParams {
            ws: PathBuf::from("/tmp/ws"),
            query: "hello".into(),
            limit: 10,
            mode: SearchMode::Fts,
        };
        let back = round_trip(&params);
        assert_eq!(back.query, "hello");
        assert_eq!(back.limit, 10);
        assert_eq!(back.mode, SearchMode::Fts);
    }

    #[test]
    fn every_search_mode_has_a_stable_lowercase_name() {
        for (mode, name) in [
            (SearchMode::Fts, "fts"),
            (SearchMode::Semantic, "semantic"),
            (SearchMode::Hybrid, "hybrid"),
        ] {
            assert_eq!(serde_json::to_value(mode).unwrap(), serde_json::json!(name));
            assert_eq!(
                serde_json::from_value::<SearchMode>(serde_json::json!(name)).unwrap(),
                mode
            );
        }
    }

    #[test]
    fn a_missing_mode_defaults_to_hybrid() {
        let value = serde_json::json!({ "ws": "/tmp/ws", "query": "q", "limit": 5 });
        let params: SearchParams = serde_json::from_value(value).unwrap();
        assert_eq!(params.mode, SearchMode::Hybrid);
    }

    #[test]
    fn every_backend_event_round_trips() {
        for event in [
            BackendEvent::Synced { upserted: 3, removed: 1 },
            BackendEvent::FileChanged { path: PathBuf::from("a.md") },
            BackendEvent::FileRemoved { path: PathBuf::from("b.md") },
            BackendEvent::Error { message: "boom".into() },
        ] {
            assert_eq!(round_trip(&event), event);
        }
    }

    #[test]
    fn a_directory_listing_round_trips() {
        let listing = ListDirResult {
            entries: vec![
                DirEntry { path: PathBuf::from("notes"), is_dir: true },
                DirEntry { path: PathBuf::from("a.md"), is_dir: false },
            ],
        };
        let back = round_trip(&listing);
        assert_eq!(back.entries.len(), 2);
        assert!(back.entries[0].is_dir);
        assert!(!back.entries[1].is_dir);
    }

    #[test]
    fn an_ack_is_an_object_not_null() {
        assert_eq!(serde_json::to_value(Ack {}).unwrap(), serde_json::json!({}));
    }

    #[test]
    fn method_names_are_namespaced() {
        for name in [SEARCH, READ_FILE, WRITE_FILE, APPEND_FILE, DELETE_FILE, LIST_DIR, REINDEX, SUBSCRIBE] {
            assert!(name.starts_with("workspace."), "{name}");
        }
        assert_eq!(EVENT, "workspace.event");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-backend --all-features protocol`
Expected: FAIL — the module does not exist.

- [ ] **Step 3: Add serde to the shared types**

`crates/sapphire-framework-workspace/src/workspace_state.rs`:

```rust
/// Controls which retrieval strategy [`WorkspaceState::retrieve_files`] uses.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
```

`crates/sapphire-framework-backend/src/lib.rs`: add
`serde::Serialize, serde::Deserialize` to the derives on `SyncSummary` and `BackendEvent`.
`BackendEvent`'s variants are externally tagged by default, which gives
`{"FileChanged":{"path":"a.md"}}` — fine, and the test above pins it.

- [ ] **Step 4: Write the protocol module**

`crates/sapphire-framework-backend/src/protocol.rs`:

```rust
//! The `workspace.*` namespace: method names and their parameter and result types.
//!
//! Both sides of the connection name these types — the server in
//! `sapphire-framework-server`, the client in [`IpcBackend`](crate::IpcBackend) — so they
//! live here, beside the [`WorkspaceBackend`](crate::WorkspaceBackend) trait they mirror,
//! rather than in `sapphire-framework-ipc`, which must stay free of the search stack.
//!
//! Every request carries `ws`, the workspace root, so a request never depends on anything
//! the connection remembers.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{BackendEvent, FileSearchResult, SearchMode};

/// Search the workspace.
pub const SEARCH: &str = "workspace.search";
/// Read a text file in full.
pub const READ_FILE: &str = "workspace.read_file";
/// Create or overwrite a text file.
pub const WRITE_FILE: &str = "workspace.write_file";
/// Append to a text file.
pub const APPEND_FILE: &str = "workspace.append_file";
/// Delete a file.
pub const DELETE_FILE: &str = "workspace.delete_file";
/// List a directory's direct children.
pub const LIST_DIR: &str = "workspace.list_dir";
/// Rebuild the index from disk.
///
/// Named `reindex`, not `sync`: [`WorkspaceBackend::sync`](crate::WorkspaceBackend::sync)
/// means "walk the files and update the index", which reads as peer-to-peer sync once the
/// bridge exists.
pub const REINDEX: &str = "workspace.reindex";
/// Start receiving [`EVENT`] notifications for a workspace.
pub const SUBSCRIBE: &str = "workspace.subscribe";
/// Notification carrying one [`BackendEvent`].
pub const EVENT: &str = "workspace.event";
/// What the server knows about itself.
pub const SERVER_INFO: &str = "server.info";

/// Parameters naming only a workspace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WsParams {
    /// The workspace root.
    pub ws: PathBuf,
}

/// Parameters naming a path inside a workspace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PathParams {
    /// The workspace root.
    pub ws: PathBuf,
    /// Workspace-relative path.
    pub path: PathBuf,
}

/// Parameters naming a path and the text to put there.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentParams {
    /// The workspace root.
    pub ws: PathBuf,
    /// Workspace-relative path.
    pub path: PathBuf,
    /// The text.
    pub content: String,
}

/// Parameters of [`SEARCH`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchParams {
    /// The workspace root.
    pub ws: PathBuf,
    /// The query.
    pub query: String,
    /// Maximum number of files to return.
    pub limit: usize,
    /// Which retrieval strategy to use.
    #[serde(default)]
    pub mode: SearchMode,
}

/// Result of [`SEARCH`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    /// File-level hits, best first.
    pub hits: Vec<FileSearchResult>,
}

/// Result of [`READ_FILE`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadResult {
    /// The file's contents.
    pub content: String,
}

/// One entry of a directory listing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirEntry {
    /// The child's path.
    pub path: PathBuf,
    /// Whether it is a directory.
    pub is_dir: bool,
}

/// Result of [`LIST_DIR`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListDirResult {
    /// The direct children.
    pub entries: Vec<DirEntry>,
}

/// Result of [`REINDEX`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ReindexResult {
    /// Documents added or updated.
    pub upserted: usize,
    /// Documents removed.
    pub removed: usize,
}

/// The success payload of a method that returns nothing.
///
/// An empty object rather than `null`, so that a later release can add a field without
/// changing the shape of the response.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Ack {}

/// Parameters of an [`EVENT`] notification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventParams {
    /// The workspace the event came from.
    pub ws: PathBuf,
    /// What happened.
    pub event: BackendEvent,
}
```

`crates/sapphire-framework-backend/src/lib.rs`: add `pub mod protocol;`.

`crates/sapphire-framework-backend/Cargo.toml`, in `[dependencies]`:

```toml
sapphire-ipc = { package = "sapphire-framework-ipc", version = "0.14.0", path = "../sapphire-framework-ipc" }
```

(The dependency is unused until Task 6; adding it here keeps the manifest changes in one
place and `cargo clippy` does not complain about unused dependencies.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-backend --all-features`
Expected: PASS, 7 tests in `protocol::tests`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-backend crates/sapphire-framework-workspace Cargo.lock
git commit -m "feat(backend): define the workspace.* wire types"
```

---

### Task 2: `WorkspaceHost` — one backend per root

**Files:**
- Create: `crates/sapphire-framework-server/Cargo.toml`
- Create: `crates/sapphire-framework-server/src/{lib.rs,error.rs,host.rs}`
- Modify: `Cargo.toml` (workspace `members`)
- Modify: `crates/sapphire-framework/Cargo.toml`, `crates/sapphire-framework/src/lib.rs`
  (feature `server`)
- Test: inline `#[cfg(test)] mod tests` in `host.rs`

**Interfaces:**
- Produces:
  - `Error::{Workspace, Backend, Ipc, Io, UnknownWorkspace, Rejected}` and `type Result<T>`
  - `WorkspaceHost::new(ctx: &'static AppContext) -> WorkspaceHost`
  - `WorkspaceHost::with_limits(ctx, max_open: usize, idle: Duration) -> WorkspaceHost`
  - `async WorkspaceHost::backend(&self, root: &Path) -> Result<Arc<LocalBackend>>`
  - `WorkspaceHost::open_count(&self) -> usize`
  - `WorkspaceHost::close_idle(&self) -> usize` — returns how many were closed
  - `WorkspaceHost::close_all(&self)`
  - `DEFAULT_MAX_OPEN: usize = 8`, `DEFAULT_IDLE: Duration = 5 min`

**The property that matters:** the same root always yields the *same* `Arc<LocalBackend>`, so
one redb database is opened once, however many clients ask for it. Eviction drops the `Arc`;
a handler still holding one keeps the database alive until it finishes.

- [ ] **Step 1: Create the manifest and register the crate**

`crates/sapphire-framework-server/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-server"
version.workspace = true
edition.workspace = true
description = "App server skeleton for sapphire-framework: owns a workspace's cache and serves it over IPC"
license.workspace = true
repository.workspace = true
keywords = ["server", "ipc", "local-first", "workspace"]
categories = ["asynchronous"]

[features]
default = ["redb-store"]
redb-store = ["sapphire-backend/redb-store"]
# Builds the test-only app server binary used by the integration tests.
test-util = []

[[bin]]
name = "server-test-app"
path = "src/bin/server-test-app.rs"
required-features = ["test-util"]

[dependencies]
sapphire-backend = { package = "sapphire-framework-backend", version = "0.14.0", path = "../sapphire-framework-backend", default-features = false }
sapphire-ipc = { package = "sapphire-framework-ipc", version = "0.14.0", path = "../sapphire-framework-ipc" }
sapphire-workspace = { package = "sapphire-framework-workspace", version = "0.14.0", path = "../sapphire-framework-workspace", default-features = false }
clap.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "sync", "time", "signal"] }
tracing.workspace = true

[dev-dependencies]
sapphire-framework-server = { path = ".", features = ["test-util"] }
tempfile = "3"
```

Root `Cargo.toml`: add `"crates/sapphire-framework-server",` to `members` after
`"crates/sapphire-framework-ipc",`.

`crates/sapphire-framework/Cargo.toml`:

```toml
server = ["dep:sapphire-framework-server", "backend"]
```

and in `[dependencies]`:

```toml
sapphire-framework-server = { version = "0.14.0", path = "../sapphire-framework-server", optional = true }
```

`crates/sapphire-framework/src/lib.rs`:

```rust
#[cfg(feature = "server")]
pub use sapphire_framework_server as server;
```

- [ ] **Step 2: Write the error type**

`crates/sapphire-framework-server/src/error.rs`:

```rust
use thiserror::Error;

/// Errors raised while serving an application's workspaces.
#[derive(Debug, Error)]
pub enum Error {
    /// Opening or using a workspace failed.
    #[error(transparent)]
    Workspace(#[from] sapphire_workspace::Error),

    /// A backend operation failed.
    #[error(transparent)]
    Backend(#[from] sapphire_backend::Error),

    /// The IPC layer failed.
    #[error(transparent)]
    Ipc(#[from] sapphire_ipc::Error),

    /// Listening, or preparing the runtime directory, failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The request named a directory that is not a workspace of this application.
    #[error("{0} is not a {1} workspace")]
    UnknownWorkspace(std::path::PathBuf, &'static str),
}

/// Convenience alias for server results.
pub type Result<T> = std::result::Result<T, Error>;
```

- [ ] **Step 3: Write the failing tests**

`crates/sapphire-framework-server/src/host.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_workspace::{AppContext, AppKind};

    static CTX: AppContext = AppContext::new("sapphire-hosttest");

    /// Point the context's directories at a scratch location and make a workspace root.
    fn workspace(parent: &std::path::Path, name: &str) -> std::path::PathBuf {
        let root = parent.join(name);
        std::fs::create_dir_all(root.join(".sapphire-hosttest")).unwrap();
        root
    }

    fn init_ctx(tmp: &std::path::Path) {
        // SAFETY: the test binary sets these before any other thread reads them.
        unsafe {
            std::env::set_var("SAPPHIRE_HOSTTEST_CACHE_DIR", tmp.join("cache"));
            std::env::set_var("SAPPHIRE_HOSTTEST_DATA_DIR", tmp.join("data"));
            std::env::set_var("SAPPHIRE_HOSTTEST_CONFIG_DIR", tmp.join("config"));
        }
        CTX.init(AppKind::Server);
    }

    #[tokio::test]
    async fn the_same_root_yields_the_same_backend() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let root = workspace(tmp.path(), "ws");
        let host = WorkspaceHost::new(&CTX);

        let a = host.backend(&root).await.unwrap();
        let b = host.backend(&root).await.unwrap();
        assert!(Arc::ptr_eq(&a, &b), "one workspace must be opened once");
        assert_eq!(host.open_count(), 1);
    }

    #[tokio::test]
    async fn two_roots_are_served_by_one_host() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let a = workspace(tmp.path(), "a");
        let b = workspace(tmp.path(), "b");
        let host = WorkspaceHost::new(&CTX);

        host.backend(&a).await.unwrap();
        host.backend(&b).await.unwrap();
        assert_eq!(host.open_count(), 2);
    }

    #[tokio::test]
    async fn a_directory_without_a_marker_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let plain = tmp.path().join("not-a-workspace");
        std::fs::create_dir_all(&plain).unwrap();
        let host = WorkspaceHost::new(&CTX);

        assert!(host.backend(&plain).await.is_err());
        assert_eq!(host.open_count(), 0, "a failed open must leave nothing behind");
    }

    #[tokio::test]
    async fn the_least_recently_used_workspace_is_closed_when_the_limit_is_reached() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let a = workspace(tmp.path(), "a");
        let b = workspace(tmp.path(), "b");
        let c = workspace(tmp.path(), "c");
        let host = WorkspaceHost::with_limits(&CTX, 2, Duration::from_secs(3600));

        host.backend(&a).await.unwrap();
        host.backend(&b).await.unwrap();
        // Touch `a` so `b` becomes the least recently used.
        host.backend(&a).await.unwrap();
        host.backend(&c).await.unwrap();

        assert_eq!(host.open_count(), 2);
        assert!(host.is_open(&a), "the recently used workspace must stay");
        assert!(!host.is_open(&b), "the least recently used must go");
    }

    #[tokio::test]
    async fn an_idle_workspace_is_closed() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let root = workspace(tmp.path(), "ws");
        let host = WorkspaceHost::with_limits(&CTX, 8, Duration::ZERO);

        host.backend(&root).await.unwrap();
        assert_eq!(host.close_idle(), 1);
        assert_eq!(host.open_count(), 0);
    }

    #[tokio::test]
    async fn reopening_after_eviction_works() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let root = workspace(tmp.path(), "ws");
        let host = WorkspaceHost::with_limits(&CTX, 8, Duration::ZERO);

        host.backend(&root).await.unwrap();
        host.close_idle();
        // If eviction left the redb database open, this second open fails.
        host.backend(&root).await.expect("the database must have been released");
    }

    #[tokio::test]
    async fn a_relative_and_an_absolute_spelling_of_one_root_are_the_same_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let root = workspace(tmp.path(), "ws");
        let host = WorkspaceHost::new(&CTX);

        let a = host.backend(&root).await.unwrap();
        let b = host.backend(&root.join(".").join("..").join("ws")).await.unwrap();
        assert!(Arc::ptr_eq(&a, &b), "the root must be canonicalised before it is a key");
    }
}
```

`reopening_after_eviction_works` is the test that catches the worst bug this type can have:
an eviction that removes the map entry while something still holds the `Arc` leaves the redb
file locked, and the next open fails with a lock error.

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features host`
Expected: FAIL — `WorkspaceHost` does not exist.

- [ ] **Step 5: Implement `WorkspaceHost`**

`crates/sapphire-framework-server/src/host.rs`:

```rust
//! One [`LocalBackend`] per workspace root, opened lazily and closed when cold.
//!
//! The server is the only process that may open an app's cache, so it must be able to hold
//! several workspaces at once — a user runs `journal add` in one workspace and searches
//! another — while not holding every workspace it has ever seen.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sapphire_backend::LocalBackend;
use sapphire_workspace::{AppContext, Workspace, WorkspaceState};

use crate::error::{Error, Result};

/// How many workspaces one server keeps open by default.
pub const DEFAULT_MAX_OPEN: usize = 8;
/// How long a workspace may go unused before it is closed, by default.
pub const DEFAULT_IDLE: Duration = Duration::from_secs(5 * 60);

struct Open {
    backend: Arc<LocalBackend>,
    last_used: Instant,
}

/// The set of workspaces this server currently has open.
pub struct WorkspaceHost {
    ctx: &'static AppContext,
    open: Mutex<HashMap<PathBuf, Open>>,
    max_open: usize,
    idle: Duration,
}

impl std::fmt::Debug for WorkspaceHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceHost")
            .field("app", &self.ctx.app_name)
            .field("open", &self.open_count())
            .field("max_open", &self.max_open)
            .finish()
    }
}

impl WorkspaceHost {
    /// A host with the default limits.
    pub fn new(ctx: &'static AppContext) -> WorkspaceHost {
        WorkspaceHost::with_limits(ctx, DEFAULT_MAX_OPEN, DEFAULT_IDLE)
    }

    /// A host with explicit limits.
    pub fn with_limits(
        ctx: &'static AppContext,
        max_open: usize,
        idle: Duration,
    ) -> WorkspaceHost {
        WorkspaceHost {
            ctx,
            open: Mutex::new(HashMap::new()),
            max_open: max_open.max(1),
            idle,
        }
    }

    /// The backend for `root`, opening the workspace if it is not already open.
    ///
    /// Two callers naming the same root get the same `Arc`, so the cache is opened once.
    pub async fn backend(&self, root: &Path) -> Result<Arc<LocalBackend>> {
        let key = canonical(root)?;

        if let Some(backend) = self.touch(&key) {
            return Ok(backend);
        }

        let ctx = self.ctx;
        let for_task = key.clone();
        let state = tokio::task::spawn_blocking(move || -> Result<WorkspaceState> {
            let workspace = Workspace::from_root(ctx, &for_task)?;
            Ok(WorkspaceState::open(workspace)?)
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))??;

        let backend = Arc::new(LocalBackend::new(Arc::new(state)));

        let mut open = self.open.lock().expect("host mutex");
        // Another task may have opened the same workspace while this one was blocking.
        // Keep whichever is already in the map, so the "one workspace, one database"
        // invariant holds even under a race.
        let entry = open.entry(key).or_insert_with(|| Open {
            backend: Arc::clone(&backend),
            last_used: Instant::now(),
        });
        entry.last_used = Instant::now();
        let chosen = Arc::clone(&entry.backend);
        drop(open);

        self.enforce_limit();
        Ok(chosen)
    }

    /// Is `root` open right now? Mostly useful in tests.
    pub fn is_open(&self, root: &Path) -> bool {
        let Ok(key) = canonical(root) else { return false };
        self.open.lock().expect("host mutex").contains_key(&key)
    }

    /// How many workspaces are open.
    pub fn open_count(&self) -> usize {
        self.open.lock().expect("host mutex").len()
    }

    /// Close every workspace unused for longer than the idle limit. Returns how many closed.
    pub fn close_idle(&self) -> usize {
        let mut open = self.open.lock().expect("host mutex");
        let before = open.len();
        open.retain(|_, entry| entry.last_used.elapsed() < self.idle);
        before - open.len()
    }

    /// Close every workspace.
    pub fn close_all(&self) {
        self.open.lock().expect("host mutex").clear();
    }

    fn touch(&self, key: &Path) -> Option<Arc<LocalBackend>> {
        let mut open = self.open.lock().expect("host mutex");
        let entry = open.get_mut(key)?;
        entry.last_used = Instant::now();
        Some(Arc::clone(&entry.backend))
    }

    fn enforce_limit(&self) {
        let mut open = self.open.lock().expect("host mutex");
        while open.len() > self.max_open {
            let Some(victim) = open
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            tracing::debug!(workspace = %victim.display(), "closing a cold workspace");
            open.remove(&victim);
        }
    }
}

/// Canonicalise a root so that two spellings of one directory are one key.
fn canonical(root: &Path) -> Result<PathBuf> {
    root.canonicalize().map_err(Error::Io)
}
```

`crates/sapphire-framework-server/src/lib.rs`:

```rust
//! The app server skeleton.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.

#![warn(missing_docs)]

mod error;
mod host;

pub use error::{Error, Result};
pub use host::{DEFAULT_IDLE, DEFAULT_MAX_OPEN, WorkspaceHost};
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features host`
Expected: PASS, 7 tests.

Note the eviction caveat the tests encode: dropping the `Arc` from the map only closes the
database once every other holder has finished. That is correct and intended — a request in
flight keeps its workspace alive — but it means `close_idle` returning 1 does not guarantee
the file is unlocked at that instant.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server Cargo.toml Cargo.lock crates/sapphire-framework
git commit -m "feat(server): keep one backend per workspace root, closing cold ones"
```

---

### Task 3: The `workspace.*` handlers

**Files:**
- Create: `crates/sapphire-framework-server/src/handlers.rs`
- Modify: `crates/sapphire-framework-server/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `handlers.rs`

**Interfaces:**
- Consumes: `WorkspaceHost` (Task 2); `protocol::*` (Task 1); `Router`, `RequestCtx`,
  `RpcError`, `codes` from `-ipc`
- Produces:
  - `workspace_router(host: Arc<WorkspaceHost>) -> Router` — a router carrying every
    `workspace.*` method except `SUBSCRIBE` (Task 4)
  - `pub(crate) fn rpc_error(err: &crate::Error) -> RpcError`

**Error mapping, decided once here:** a caller's mistake is `INVALID_PARAMS`; anything else is
`INTERNAL_ERROR`. A path that escapes the workspace root, a missing workspace marker and a
malformed parameter object are the caller's mistakes. A failing database is not.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/src/handlers.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_backend::protocol as proto;
    use sapphire_ipc::{Client, ClientInfo, Connection, ManagedBy, ServerInfo, serve};
    use sapphire_workspace::{AppContext, AppKind};
    use std::path::PathBuf;

    static CTX: AppContext = AppContext::new("sapphire-handlertest");

    fn init_ctx(tmp: &std::path::Path) {
        // SAFETY: the test binary sets these before any other thread reads them.
        unsafe {
            std::env::set_var("SAPPHIRE_HANDLERTEST_CACHE_DIR", tmp.join("cache"));
            std::env::set_var("SAPPHIRE_HANDLERTEST_DATA_DIR", tmp.join("data"));
            std::env::set_var("SAPPHIRE_HANDLERTEST_CONFIG_DIR", tmp.join("config"));
        }
        CTX.init(AppKind::Server);
    }

    /// A server over an in-process connection, plus a workspace root to use.
    async fn fixture() -> (tempfile::TempDir, PathBuf, Client) {
        let tmp = tempfile::tempdir().unwrap();
        init_ctx(tmp.path());
        let root = tmp.path().join("ws");
        std::fs::create_dir_all(root.join(".sapphire-handlertest")).unwrap();
        let root = root.canonicalize().unwrap();

        let host = Arc::new(WorkspaceHost::new(&CTX));
        let router = Arc::new(workspace_router(host));
        let (client_conn, server_conn) = Connection::pair();
        tokio::spawn(async move {
            let info = ServerInfo {
                version: "0.0.0".into(),
                pid: std::process::id(),
                managed_by: ManagedBy::Spawned,
            };
            let _ = serve(server_conn, router, "sapphire-handlertest", info).await;
        });
        let client_info = ClientInfo {
            kind: "test".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        };
        let (client, _) =
            Client::handshake(client_conn, "sapphire-handlertest", client_info).await.unwrap();
        (tmp, root, client)
    }

    #[tokio::test]
    async fn a_file_written_through_the_server_can_be_read_back() {
        let (_tmp, ws, client) = fixture().await;
        let _: proto::Ack = client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("a.md"),
                    content: "# hello".into(),
                },
            )
            .await
            .unwrap();

        let read: proto::ReadResult = client
            .call(proto::READ_FILE, proto::PathParams { ws, path: PathBuf::from("a.md") })
            .await
            .unwrap();
        assert_eq!(read.content, "# hello");
    }

    #[tokio::test]
    async fn appending_adds_to_the_file() {
        let (_tmp, ws, client) = fixture().await;
        let write = proto::ContentParams {
            ws: ws.clone(),
            path: PathBuf::from("a.md"),
            content: "one\n".into(),
        };
        let _: proto::Ack = client.call(proto::WRITE_FILE, write).await.unwrap();
        let append = proto::ContentParams {
            ws: ws.clone(),
            path: PathBuf::from("a.md"),
            content: "two\n".into(),
        };
        let _: proto::Ack = client.call(proto::APPEND_FILE, append).await.unwrap();

        let read: proto::ReadResult = client
            .call(proto::READ_FILE, proto::PathParams { ws, path: PathBuf::from("a.md") })
            .await
            .unwrap();
        assert_eq!(read.content, "one\ntwo\n");
    }

    #[tokio::test]
    async fn a_deleted_file_is_gone() {
        let (_tmp, ws, client) = fixture().await;
        let _: proto::Ack = client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("a.md"),
                    content: "x".into(),
                },
            )
            .await
            .unwrap();
        let _: proto::Ack = client
            .call(
                proto::DELETE_FILE,
                proto::PathParams { ws: ws.clone(), path: PathBuf::from("a.md") },
            )
            .await
            .unwrap();

        let err = client
            .call::<_, proto::ReadResult>(
                proto::READ_FILE,
                proto::PathParams { ws, path: PathBuf::from("a.md") },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, sapphire_ipc::Error::Rpc(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn a_directory_listing_separates_files_from_directories() {
        let (_tmp, ws, client) = fixture().await;
        std::fs::create_dir_all(ws.join("notes")).unwrap();
        let _: proto::Ack = client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("a.md"),
                    content: "x".into(),
                },
            )
            .await
            .unwrap();

        let listing: proto::ListDirResult = client
            .call(proto::LIST_DIR, proto::PathParams { ws, path: PathBuf::from(".") })
            .await
            .unwrap();
        assert!(listing.entries.iter().any(|e| e.is_dir && e.path.ends_with("notes")));
        assert!(listing.entries.iter().any(|e| !e.is_dir && e.path.ends_with("a.md")));
    }

    #[tokio::test]
    async fn a_written_file_is_searchable() {
        let (_tmp, ws, client) = fixture().await;
        let _: proto::Ack = client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("a.md"),
                    content: "the quick brown fox".into(),
                },
            )
            .await
            .unwrap();

        let hits: proto::SearchResult = client
            .call(
                proto::SEARCH,
                proto::SearchParams {
                    ws,
                    query: "brown".into(),
                    limit: 10,
                    mode: sapphire_backend::SearchMode::Fts,
                },
            )
            .await
            .unwrap();
        assert!(hits.hits.iter().any(|h| h.path.ends_with("a.md")), "{:?}", hits.hits);
    }

    #[tokio::test]
    async fn reindexing_reports_what_it_found() {
        let (_tmp, ws, client) = fixture().await;
        std::fs::write(ws.join("outside.md"), "written behind the server's back").unwrap();

        let report: proto::ReindexResult =
            client.call(proto::REINDEX, proto::WsParams { ws }).await.unwrap();
        assert!(report.upserted >= 1, "{report:?}");
    }

    #[tokio::test]
    async fn a_path_escaping_the_workspace_is_an_invalid_parameter() {
        let (_tmp, ws, client) = fixture().await;
        let err = client
            .call::<_, proto::ReadResult>(
                proto::READ_FILE,
                proto::PathParams { ws, path: PathBuf::from("../../etc/passwd") },
            )
            .await
            .unwrap_err();
        match err {
            sapphire_ipc::Error::Rpc(e) => {
                assert_eq!(e.code, sapphire_ipc::codes::INVALID_PARAMS, "{}", e.message);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_directory_that_is_not_a_workspace_is_an_invalid_parameter() {
        let (tmp, _ws, client) = fixture().await;
        let plain = tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();

        let err = client
            .call::<_, proto::ReadResult>(
                proto::READ_FILE,
                proto::PathParams { ws: plain, path: PathBuf::from("a.md") },
            )
            .await
            .unwrap_err();
        match err {
            sapphire_ipc::Error::Rpc(e) => {
                assert_eq!(e.code, sapphire_ipc::codes::INVALID_PARAMS, "{}", e.message);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_malformed_parameter_object_is_an_invalid_parameter() {
        let (_tmp, _ws, client) = fixture().await;
        let err = client
            .call::<_, proto::ReadResult>(proto::READ_FILE, serde_json::json!({ "ws": 42 }))
            .await
            .unwrap_err();
        match err {
            sapphire_ipc::Error::Rpc(e) => {
                assert_eq!(e.code, sapphire_ipc::codes::INVALID_PARAMS);
            }
            other => panic!("got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features handlers`
Expected: FAIL — `workspace_router` does not exist.

- [ ] **Step 3: Implement the handlers**

`crates/sapphire-framework-server/src/handlers.rs`:

```rust
//! The `workspace.*` methods, one per [`WorkspaceBackend`] method.

use std::sync::Arc;

use sapphire_backend::protocol as proto;
use sapphire_backend::{WorkspaceBackend, protocol::Ack};
use sapphire_ipc::{RequestCtx, Router, RpcError, codes};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::Error;
use crate::host::WorkspaceHost;

/// Turn a server error into a JSON-RPC error.
///
/// Only a caller's mistake is `INVALID_PARAMS`: a path outside the workspace, a directory
/// that is not a workspace, a parameter object that does not fit. A failing database is the
/// server's problem, and the caller cannot fix it by asking differently.
pub(crate) fn rpc_error(err: &Error) -> RpcError {
    use sapphire_workspace::Error as WsError;

    let caller_error = matches!(
        err,
        Error::UnknownWorkspace(..)
            | Error::Workspace(
                WsError::PathEscapesWorkspace { .. }
                    | WsError::MarkerDirMissing { .. }
                    | WsError::MarkerNotFound { .. }
            )
    ) || matches!(
        err,
        Error::Backend(sapphire_backend::Error::Workspace(
            WsError::PathEscapesWorkspace { .. }
                | WsError::MarkerDirMissing { .. }
                | WsError::MarkerNotFound { .. }
        ))
    );

    if caller_error {
        RpcError::invalid_params(err.to_string())
    } else {
        RpcError { code: codes::INTERNAL_ERROR, message: err.to_string(), data: None }
    }
}

fn params<T: DeserializeOwned>(ctx: &RequestCtx) -> std::result::Result<T, RpcError> {
    serde_json::from_value(ctx.params.clone())
        .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))
}

fn ok<T: serde::Serialize>(value: T) -> std::result::Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|e| RpcError::internal(e.to_string()))
}

/// Every `workspace.*` method except `workspace.subscribe`, which needs the event pump.
pub fn workspace_router(host: Arc<WorkspaceHost>) -> Router {
    let read = Arc::clone(&host);
    let write = Arc::clone(&host);
    let append = Arc::clone(&host);
    let delete = Arc::clone(&host);
    let list = Arc::clone(&host);
    let search = Arc::clone(&host);
    let reindex = Arc::clone(&host);

    Router::new()
        .method(proto::READ_FILE, move |ctx| {
            let host = Arc::clone(&read);
            async move {
                let p: proto::PathParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                let content = backend
                    .read_file(&p.path)
                    .await
                    .map_err(|e| rpc_error(&Error::Backend(e)))?;
                ok(proto::ReadResult { content })
            }
        })
        .method(proto::WRITE_FILE, move |ctx| {
            let host = Arc::clone(&write);
            async move {
                let p: proto::ContentParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                backend
                    .write_file(&p.path, &p.content)
                    .await
                    .map_err(|e| rpc_error(&Error::Backend(e)))?;
                ok(Ack {})
            }
        })
        .method(proto::APPEND_FILE, move |ctx| {
            let host = Arc::clone(&append);
            async move {
                let p: proto::ContentParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                backend
                    .append_file(&p.path, &p.content)
                    .await
                    .map_err(|e| rpc_error(&Error::Backend(e)))?;
                ok(Ack {})
            }
        })
        .method(proto::DELETE_FILE, move |ctx| {
            let host = Arc::clone(&delete);
            async move {
                let p: proto::PathParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                backend
                    .delete_file(&p.path)
                    .await
                    .map_err(|e| rpc_error(&Error::Backend(e)))?;
                ok(Ack {})
            }
        })
        .method(proto::LIST_DIR, move |ctx| {
            let host = Arc::clone(&list);
            async move {
                let p: proto::PathParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                let entries = backend
                    .list_dir(&p.path)
                    .await
                    .map_err(|e| rpc_error(&Error::Backend(e)))?
                    .into_iter()
                    .map(|(path, is_dir)| proto::DirEntry { path, is_dir })
                    .collect();
                ok(proto::ListDirResult { entries })
            }
        })
        .method(proto::SEARCH, move |ctx| {
            let host = Arc::clone(&search);
            async move {
                let p: proto::SearchParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                let hits = backend
                    .search(&p.query, p.limit, p.mode)
                    .await
                    .map_err(|e| rpc_error(&Error::Backend(e)))?;
                ok(proto::SearchResult { hits })
            }
        })
        .method(proto::REINDEX, move |ctx| {
            let host = Arc::clone(&reindex);
            async move {
                let p: proto::WsParams = params(&ctx)?;
                let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;
                let summary =
                    backend.sync().await.map_err(|e| rpc_error(&Error::Backend(e)))?;
                ok(proto::ReindexResult {
                    upserted: summary.upserted,
                    removed: summary.removed,
                })
            }
        })
}
```

`crates/sapphire-framework-server/src/lib.rs`: add `mod handlers;` and
`pub use handlers::workspace_router;`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features handlers`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): serve the workspace.* namespace over IPC"
```

---

### Task 4: Events

**Files:**
- Create: `crates/sapphire-framework-server/src/events.rs`
- Modify: `crates/sapphire-framework-server/src/{lib.rs,handlers.rs}`
- Test: inline `#[cfg(test)] mod tests` in `events.rs`

**Interfaces:**
- Consumes: `WorkspaceHost` (Task 2), `PeerHandle` from `-ipc`, `proto::{SUBSCRIBE, EVENT, EventParams, WsParams, Ack}`
- Produces: `subscribe_method(host: Arc<WorkspaceHost>, router: Router) -> Router` — adds
  `workspace.subscribe` to an existing router

**Behaviour:** `workspace.subscribe` opens the workspace, subscribes to its `BackendEvent`
broadcast, and spawns a task that forwards each event to that client as a `workspace.event`
notification. The task stops when the client disconnects — `PeerHandle::notify` fails once the
connection is gone — so a client that comes and goes does not leak tasks.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/src/events.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_backend::protocol as proto;
    use sapphire_backend::BackendEvent;
    use sapphire_ipc::{Client, ClientInfo, Connection, ManagedBy, ServerInfo, serve};
    use sapphire_workspace::{AppContext, AppKind};
    use std::path::PathBuf;
    use std::time::Duration;

    static CTX: AppContext = AppContext::new("sapphire-eventtest");

    async fn fixture() -> (tempfile::TempDir, PathBuf, Client) {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: the test binary sets these before any other thread reads them.
        unsafe {
            std::env::set_var("SAPPHIRE_EVENTTEST_CACHE_DIR", tmp.path().join("cache"));
            std::env::set_var("SAPPHIRE_EVENTTEST_DATA_DIR", tmp.path().join("data"));
            std::env::set_var("SAPPHIRE_EVENTTEST_CONFIG_DIR", tmp.path().join("config"));
        }
        CTX.init(AppKind::Server);

        let root = tmp.path().join("ws");
        std::fs::create_dir_all(root.join(".sapphire-eventtest")).unwrap();
        let root = root.canonicalize().unwrap();

        let host = Arc::new(WorkspaceHost::new(&CTX));
        let router = Arc::new(subscribe_method(
            Arc::clone(&host),
            crate::workspace_router(host),
        ));
        let (client_conn, server_conn) = Connection::pair();
        tokio::spawn(async move {
            let info = ServerInfo {
                version: "0.0.0".into(),
                pid: std::process::id(),
                managed_by: ManagedBy::Spawned,
            };
            let _ = serve(server_conn, router, "sapphire-eventtest", info).await;
        });
        let client_info = ClientInfo {
            kind: "test".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        };
        let (client, _) =
            Client::handshake(client_conn, "sapphire-eventtest", client_info).await.unwrap();
        (tmp, root, client)
    }

    #[tokio::test]
    async fn a_subscriber_sees_a_write_by_another_caller() {
        let (_tmp, ws, client) = fixture().await;
        let mut events = client.notifications();

        let _: proto::Ack =
            client.call(proto::SUBSCRIBE, proto::WsParams { ws: ws.clone() }).await.unwrap();
        let _: proto::Ack = client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("a.md"),
                    content: "x".into(),
                },
            )
            .await
            .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("an event within five seconds")
            .unwrap();
        assert_eq!(notification.method, proto::EVENT);
        let params: proto::EventParams =
            serde_json::from_value(notification.params).unwrap();
        assert_eq!(params.ws, ws);
        assert!(
            matches!(params.event, BackendEvent::FileChanged { .. }),
            "{:?}",
            params.event
        );
    }

    #[tokio::test]
    async fn subscribing_to_a_directory_that_is_not_a_workspace_is_refused() {
        let (tmp, _ws, client) = fixture().await;
        let plain = tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();

        let err = client
            .call::<_, proto::Ack>(proto::SUBSCRIBE, proto::WsParams { ws: plain })
            .await
            .unwrap_err();
        assert!(matches!(err, sapphire_ipc::Error::Rpc(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn subscribing_twice_does_not_duplicate_events() {
        let (_tmp, ws, client) = fixture().await;
        let mut events = client.notifications();

        let _: proto::Ack =
            client.call(proto::SUBSCRIBE, proto::WsParams { ws: ws.clone() }).await.unwrap();
        let _: proto::Ack =
            client.call(proto::SUBSCRIBE, proto::WsParams { ws: ws.clone() }).await.unwrap();

        let _: proto::Ack = client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("a.md"),
                    content: "x".into(),
                },
            )
            .await
            .unwrap();

        // One event arrives …
        tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("the first event")
            .unwrap();
        // … and no second copy of it.
        let second = tokio::time::timeout(Duration::from_millis(500), events.recv()).await;
        assert!(second.is_err(), "a duplicate event arrived: {second:?}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features events`
Expected: FAIL — `subscribe_method` does not exist.

- [ ] **Step 3: Implement the subscription**

`crates/sapphire-framework-server/src/events.rs`:

```rust
//! `workspace.subscribe` and the pump that turns backend events into notifications.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use sapphire_backend::protocol as proto;
use sapphire_backend::{WorkspaceBackend, protocol::Ack};
use sapphire_ipc::{PeerHandle, Router, RpcError};
use tokio::sync::broadcast::error::RecvError;

use crate::error::Error;
use crate::handlers::rpc_error;
use crate::host::WorkspaceHost;

/// Add `workspace.subscribe` to `router`.
pub fn subscribe_method(host: Arc<WorkspaceHost>, router: Router) -> Router {
    // Which (client, workspace) pairs already have a pump. A client that subscribes twice
    // must not receive two copies of every event.
    let active: Arc<Mutex<HashSet<(u32, PathBuf)>>> = Arc::new(Mutex::new(HashSet::new()));

    router.method(proto::SUBSCRIBE, move |ctx| {
        let host = Arc::clone(&host);
        let active = Arc::clone(&active);
        async move {
            let p: proto::WsParams = serde_json::from_value(ctx.params.clone())
                .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
            let backend = host.backend(&p.ws).await.map_err(|e| rpc_error(&e))?;

            let key = (ctx.peer.client().pid, p.ws.clone());
            if !active.lock().expect("subscription set").insert(key.clone()) {
                // Already pumping for this client and workspace.
                return serde_json::to_value(Ack {})
                    .map_err(|e| RpcError::internal(e.to_string()));
            }

            let mut events = backend.subscribe();
            let peer: PeerHandle = ctx.peer.clone();
            let ws = p.ws.clone();
            tokio::spawn(async move {
                loop {
                    match events.recv().await {
                        Ok(event) => {
                            let params = proto::EventParams { ws: ws.clone(), event };
                            let Ok(value) = serde_json::to_value(params) else { continue };
                            // A failure here means the client is gone.
                            if peer.notify(proto::EVENT, value).await.is_err() {
                                break;
                            }
                        }
                        Err(RecvError::Lagged(missed)) => {
                            tracing::warn!(
                                missed,
                                workspace = %ws.display(),
                                "a subscriber fell behind and lost events"
                            );
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                active.lock().expect("subscription set").remove(&key);
            });

            serde_json::to_value(Ack {}).map_err(|e| RpcError::internal(e.to_string()))
        }
    })
}
```

`handlers.rs`: make `rpc_error` visible to this module — it is already `pub(crate)`.

`lib.rs`: add `mod events;` and `pub use events::subscribe_method;`.

> **A limitation to write down rather than hide:** a lagging subscriber loses events. The
> broadcast channel holds 128 (`LocalBackend`'s `EVENT_CAPACITY`), and a client that falls
> further behind gets a warning in the server log and a gap in its stream. A UI that must not
> miss a change should treat an event as "something happened, re-read", not as the change
> itself. That is how the desktop already uses `BackendEvent`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features events`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): push workspace events to subscribed clients"
```

---

### Task 5: `AppServer` — listening, idle exit, shutdown

**Files:**
- Modify: `crates/sapphire-framework-server/src/lib.rs`
- Create: `crates/sapphire-framework-server/src/bin/server-test-app.rs`
- Test: inline `#[cfg(test)] mod tests` in `lib.rs`

**Interfaces:**
- Consumes: everything above; `Endpoint`, `bind`, `serve`, `ServerInfo`, `ManagedBy`,
  `SHUTDOWN_METHOD` from `-ipc`
- Produces:
  - `AppServer::new(ctx: &'static AppContext, version: &'static str) -> AppServer`
  - `AppServer::managed_by(self, ManagedBy) -> AppServer`
  - `AppServer::limits(self, max_open: usize, idle: Duration) -> AppServer`
  - `AppServer::idle_exit(self, Option<Duration>) -> AppServer`
  - `AppServer::extend(self, f: impl FnOnce(Router) -> Router) -> AppServer`
  - `AppServer::host(&self) -> &Arc<WorkspaceHost>`
  - `async AppServer::run(self) -> Result<()>`
  - `DEFAULT_IDLE_EXIT: Duration = 15 min`

**Two rules:**
- `server.shutdown` always works; it is how a client replaces a spawned server of the wrong
  version (spec §2.6).
- **Idle exit is disabled when `managed_by` is `Service`.** A service manager would restart the
  process immediately, and the operator asked for it to stay.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/src/lib.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_ipc::{ClientInfo, Endpoint, ManagedBy, SpawnConfig, ensure_server};
    use sapphire_workspace::{AppContext, AppKind};

    static CTX: AppContext = AppContext::new("sapphire-appservertest");

    fn client_info() -> ClientInfo {
        ClientInfo {
            kind: "test".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        }
    }

    fn prepared(tmp: &std::path::Path) -> Endpoint {
        // SAFETY: the test binary sets these before any other thread reads them.
        unsafe {
            std::env::set_var("SAPPHIRE_APPSERVERTEST_CACHE_DIR", tmp.join("cache"));
            std::env::set_var("SAPPHIRE_APPSERVERTEST_DATA_DIR", tmp.join("data"));
            std::env::set_var("SAPPHIRE_APPSERVERTEST_CONFIG_DIR", tmp.join("config"));
        }
        CTX.init(AppKind::Server);
        Endpoint::in_dir("sapphire-appservertest", tmp.to_path_buf())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_running_server_answers_server_info() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = prepared(tmp.path());

        let server = AppServer::new(&CTX, "1.2.3").endpoint(endpoint.clone());
        let handle = tokio::spawn(async move { server.run().await });

        let (client, info) = ensure_server(
            &endpoint,
            "sapphire-appservertest",
            ClientInfo { version: "1.2.3".into(), ..client_info() },
            &SpawnConfig::disabled(),
        )
        .await
        .unwrap();
        assert_eq!(info.version, "1.2.3");

        let reported: sapphire_ipc::ServerInfo =
            client.call(sapphire_backend::protocol::SERVER_INFO, serde_json::json!({}))
                .await
                .unwrap();
        assert_eq!(reported.version, "1.2.3");

        let _: serde_json::Value =
            client.call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_stops_the_server() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = prepared(tmp.path());

        let server = AppServer::new(&CTX, "0.0.0").endpoint(endpoint.clone());
        let handle = tokio::spawn(async move { server.run().await });

        let (client, _) = ensure_server(
            &endpoint,
            "sapphire-appservertest",
            client_info(),
            &SpawnConfig::disabled(),
        )
        .await
        .unwrap();
        let _: serde_json::Value =
            client.call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({})).await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("the server must stop")
            .unwrap()
            .unwrap();
        assert!(!sapphire_ipc::probe(&endpoint).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_spawned_server_exits_when_it_goes_idle() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = prepared(tmp.path());

        let server = AppServer::new(&CTX, "0.0.0")
            .endpoint(endpoint.clone())
            .managed_by(ManagedBy::Spawned)
            .idle_exit(Some(std::time::Duration::from_millis(200)));
        let handle = tokio::spawn(async move { server.run().await });

        tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("an idle spawned server must exit")
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_service_server_does_not_exit_when_idle() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = prepared(tmp.path());

        let server = AppServer::new(&CTX, "0.0.0")
            .endpoint(endpoint.clone())
            .managed_by(ManagedBy::Service)
            .idle_exit(Some(std::time::Duration::from_millis(100)));
        let handle = tokio::spawn(async move { server.run().await });

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert!(!handle.is_finished(), "a service must stay up");
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_application_can_add_its_own_methods() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = prepared(tmp.path());

        let server = AppServer::new(&CTX, "0.0.0").endpoint(endpoint.clone()).extend(|r| {
            r.method("sapphire-appservertest.greet", |_| async move {
                Ok(serde_json::json!("hello"))
            })
        });
        let handle = tokio::spawn(async move { server.run().await });

        let (client, _) = ensure_server(
            &endpoint,
            "sapphire-appservertest",
            client_info(),
            &SpawnConfig::disabled(),
        )
        .await
        .unwrap();
        let greeting: String =
            client.call("sapphire-appservertest.greet", serde_json::json!({})).await.unwrap();
        assert_eq!(greeting, "hello");

        let _: serde_json::Value =
            client.call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features --lib`
Expected: FAIL — `AppServer` does not exist.

- [ ] **Step 3: Implement `AppServer`**

`crates/sapphire-framework-server/src/lib.rs`, above the test module:

```rust
//! The app server skeleton.
//!
//! An application builds one of these, adds its own methods, and runs it. Everything a
//! sapphire app needs on the server side — owning the workspaces, answering `workspace.*`,
//! pushing events, exiting when nobody is using it — is here.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.
//!
//! ```rust,ignore
//! static CTX: AppContext = AppContext::new("sapphire-journal");
//!
//! AppServer::new(&CTX, env!("CARGO_PKG_VERSION"))
//!     .extend(|router| router.method("journal.create_entry", create_entry))
//!     .run()
//!     .await?;
//! ```
//!
//! Application methods are named `<app>.<method>`. The framework owns `workspace.*` and
//! `server.*`; anything else is the application's.

#![warn(missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sapphire_backend::protocol as proto;
use sapphire_ipc::{Endpoint, ManagedBy, Router, ServerInfo, serve};
use sapphire_workspace::AppContext;

mod error;
mod events;
mod handlers;
mod host;

pub use error::{Error, Result};
pub use events::subscribe_method;
pub use handlers::workspace_router;
pub use host::{DEFAULT_IDLE, DEFAULT_MAX_OPEN, WorkspaceHost};

/// How long a spawned server stays up with nothing to do.
pub const DEFAULT_IDLE_EXIT: Duration = Duration::from_secs(15 * 60);

/// Longest gap between idle checks.
///
/// The actual interval is this or the idle limit, whichever is shorter, so a server asked to
/// exit after 200 ms does not sit for ten seconds first. Tests depend on that.
const IDLE_TICK: Duration = Duration::from_secs(10);

/// An application's server.
pub struct AppServer {
    ctx: &'static AppContext,
    version: &'static str,
    endpoint: Option<Endpoint>,
    managed_by: ManagedBy,
    max_open: usize,
    workspace_idle: Duration,
    idle_exit: Option<Duration>,
    host: Arc<WorkspaceHost>,
    extend: Option<Box<dyn FnOnce(Router) -> Router + Send>>,
}

impl AppServer {
    /// A server for `ctx`'s application, reporting `version` in its handshake.
    ///
    /// Pass `env!("CARGO_PKG_VERSION")`: a client compares it with its own and replaces a
    /// spawned server that does not match (spec §2.6).
    pub fn new(ctx: &'static AppContext, version: &'static str) -> AppServer {
        AppServer {
            ctx,
            version,
            endpoint: None,
            managed_by: ManagedBy::Spawned,
            max_open: DEFAULT_MAX_OPEN,
            workspace_idle: DEFAULT_IDLE,
            idle_exit: Some(DEFAULT_IDLE_EXIT),
            host: Arc::new(WorkspaceHost::new(ctx)),
            extend: None,
        }
    }

    /// Listen somewhere other than the application's default endpoint. Used by tests.
    pub fn endpoint(mut self, endpoint: Endpoint) -> AppServer {
        self.endpoint = Some(endpoint);
        self
    }

    /// How this process was started. `Service` disables idle exit.
    pub fn managed_by(mut self, managed_by: ManagedBy) -> AppServer {
        self.managed_by = managed_by;
        self
    }

    /// How many workspaces to keep open, and how long a cold one may linger.
    pub fn limits(mut self, max_open: usize, idle: Duration) -> AppServer {
        self.max_open = max_open;
        self.workspace_idle = idle;
        self.host = Arc::new(WorkspaceHost::with_limits(self.ctx, max_open, idle));
        self
    }

    /// How long to stay up with no connections. `None` never exits on its own.
    pub fn idle_exit(mut self, after: Option<Duration>) -> AppServer {
        self.idle_exit = after;
        self
    }

    /// Add the application's own methods.
    pub fn extend(mut self, f: impl FnOnce(Router) -> Router + Send + 'static) -> AppServer {
        self.extend = Some(Box::new(f));
        self
    }

    /// The workspaces this server has open. An application's own handlers use it to reach a
    /// workspace the same way the framework's do.
    pub fn host(&self) -> &Arc<WorkspaceHost> {
        &self.host
    }

    /// Listen until told to stop, or until idle.
    pub async fn run(self) -> Result<()> {
        let AppServer {
            ctx,
            version,
            endpoint,
            managed_by,
            host,
            extend,
            idle_exit,
            ..
        } = self;

        let endpoint = match endpoint {
            Some(e) => e,
            None => Endpoint::for_app(ctx.app_name)?,
        };
        let info = ServerInfo {
            version: version.to_owned(),
            pid: std::process::id(),
            managed_by,
        };

        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let live = Arc::new(AtomicU64::new(0));
        let last_activity = Arc::new(Mutex::new(std::time::Instant::now()));

        let mut router = subscribe_method(Arc::clone(&host), workspace_router(Arc::clone(&host)));
        router = router
            .method(proto::SERVER_INFO, {
                let info = info.clone();
                move |_| {
                    let info = info.clone();
                    async move {
                        serde_json::to_value(info)
                            .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                    }
                }
            })
            .method(sapphire_ipc::SHUTDOWN_METHOD, {
                let stop_tx = stop_tx.clone();
                move |_| {
                    let stop_tx = stop_tx.clone();
                    async move {
                        let _ = stop_tx.send(true);
                        serde_json::to_value(proto::Ack {})
                            .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                    }
                }
            });
        if let Some(extend) = extend {
            router = extend(router);
        }
        let router = Arc::new(router);

        #[cfg(unix)]
        let listener = sapphire_ipc::bind(&endpoint).await?;
        #[cfg(windows)]
        let mut listener = sapphire_ipc::bind(&endpoint)?;

        // Close cold workspaces, and stop when nothing has used us for a while.
        let ticker = {
            let host = Arc::clone(&host);
            let live = Arc::clone(&live);
            let last_activity = Arc::clone(&last_activity);
            let stop_tx = stop_tx.clone();
            tokio::spawn(async move {
                let tick = idle_exit.map_or(IDLE_TICK, |limit| limit.min(IDLE_TICK)).max(Duration::from_millis(50));
                let mut interval = tokio::time::interval(tick);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    host.close_idle();
                    let Some(limit) = idle_exit else { continue };
                    if managed_by == ManagedBy::Service {
                        continue;
                    }
                    if live.load(Ordering::Relaxed) > 0 {
                        continue;
                    }
                    let idle_for = last_activity.lock().expect("activity clock").elapsed();
                    if idle_for >= limit {
                        tracing::info!(?idle_for, "exiting after being idle");
                        let _ = stop_tx.send(true);
                        return;
                    }
                }
            })
        };

        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let conn = accepted?;
                    *last_activity.lock().expect("activity clock") =
                        std::time::Instant::now();
                    live.fetch_add(1, Ordering::Relaxed);
                    let router = Arc::clone(&router);
                    let app = ctx.app_name;
                    let info = info.clone();
                    let live = Arc::clone(&live);
                    let last_activity = Arc::clone(&last_activity);
                    tokio::spawn(async move {
                        let _ = serve(conn, router, app, info).await;
                        live.fetch_sub(1, Ordering::Relaxed);
                        *last_activity.lock().expect("activity clock") =
                            std::time::Instant::now();
                    });
                }
            }
        }

        ticker.abort();
        host.close_all();
        drop(listener); // removes the socket file on Unix
        Ok(())
    }
}

use std::sync::Mutex;
```

Move `use std::sync::Mutex;` up into the import block at the top of the file rather than
leaving it at the bottom.

The tick interval is the idle limit or `IDLE_TICK`, whichever is shorter, with a 50 ms floor.
Without that, a test asking for a 200 ms idle limit would wait a full ten seconds for the
first check, and `a_spawned_server_exits_when_it_goes_idle` would sit exactly on its timeout.

- [ ] **Step 4: Write the test-only application binary**

`crates/sapphire-framework-server/src/bin/server-test-app.rs`:

```rust
//! A minimal application server used by the integration tests.
//!
//! Usage: `server-test-app <runtime-dir> <state-dir>`
//!
//! Serves the framework's `workspace.*` namespace and nothing else. `<state-dir>` becomes
//! the application's cache, data and config root.

use sapphire_framework_server::AppServer;
use sapphire_ipc::{Endpoint, ManagedBy};
use sapphire_workspace::{AppContext, AppKind};

static CTX: AppContext = AppContext::new("sapphire-servertest");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let runtime_dir = args.next().expect("a runtime directory");
    let state_dir = args.next().expect("a state directory");

    // SAFETY: set before the runtime does anything else with the environment.
    unsafe {
        std::env::set_var("SAPPHIRE_SERVERTEST_CACHE_DIR", format!("{state_dir}/cache"));
        std::env::set_var("SAPPHIRE_SERVERTEST_DATA_DIR", format!("{state_dir}/data"));
        std::env::set_var("SAPPHIRE_SERVERTEST_CONFIG_DIR", format!("{state_dir}/config"));
    }
    CTX.init(AppKind::Server);

    AppServer::new(&CTX, env!("CARGO_PKG_VERSION"))
        .endpoint(Endpoint::in_dir("sapphire-servertest", runtime_dir.into()))
        .managed_by(ManagedBy::Spawned)
        .idle_exit(None)
        .run()
        .await?;
    Ok(())
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features --lib`
Expected: PASS, 5 tests in the `lib` test module plus the module tests from Tasks 2–4.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): listen, shut down on request, and exit when idle"
```

---

### Task 6: `IpcBackend`

**Files:**
- Create: `crates/sapphire-framework-backend/src/ipc.rs`
- Modify: `crates/sapphire-framework-backend/src/{lib.rs,error.rs}`
- Test: `crates/sapphire-framework-backend/tests/ipc_backend.rs`

**Interfaces:**
- Consumes: `protocol::*` (Task 1); `Client`, `Endpoint`, `SpawnConfig`, `ensure_server`,
  `ClientInfo` from `-ipc`
- Produces:
  - `Error::Ipc(sapphire_ipc::Error)` added to the backend error enum
  - `IpcBackend::connect(endpoint: &Endpoint, app: &str, kind: &str, version: &str, spawn: &SpawnConfig, ws: PathBuf) -> Result<IpcBackend>`
  - `IpcBackend::from_client(client: Arc<Client>, ws: PathBuf) -> IpcBackend`
  - `impl WorkspaceBackend for IpcBackend`
  - `IpcBackend::client(&self) -> &Arc<Client>` — so an application's own client code shares
    one connection with the framework's

**Why this is the payoff:** the desktop holds a `Box<dyn WorkspaceBackend>`. Swapping
`LocalBackend` for `IpcBackend` is the entire change needed to move it off the cache.

- [ ] **Step 1: Write the failing test**

`crates/sapphire-framework-backend/tests/ipc_backend.rs`:

```rust
//! `IpcBackend` must behave like `LocalBackend`, because the UI cannot tell them apart.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sapphire_backend::{IpcBackend, SearchMode, WorkspaceBackend};
use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig};

fn client_info(version: &str) -> ClientInfo {
    ClientInfo { kind: "test".into(), version: version.into(), pid: std::process::id() }
}

/// Start the test application server from `sapphire-framework-server`, and return the
/// endpoint plus a workspace root inside it.
async fn fixture(tmp: &Path) -> (Endpoint, PathBuf, SpawnConfig) {
    let runtime = tmp.join("run");
    let state = tmp.join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let root = tmp.join("ws");
    std::fs::create_dir_all(root.join(".sapphire-servertest")).unwrap();
    let root = root.canonicalize().unwrap();

    let endpoint = Endpoint::in_dir("sapphire-servertest", runtime.clone());
    let spawn = SpawnConfig {
        exe: env!("CARGO_BIN_EXE_server-test-app").into(),
        args: vec![runtime.display().to_string(), state.display().to_string()],
        ..SpawnConfig::default()
    };
    (endpoint, root, spawn)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ipc_backend_reads_back_what_it_wrote() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path()).await;

    let backend = IpcBackend::connect(
        &endpoint,
        "sapphire-servertest",
        "test",
        env!("CARGO_PKG_VERSION"),
        &spawn,
        ws,
    )
    .await
    .unwrap();

    backend.write_file(Path::new("a.md"), "# hello").await.unwrap();
    assert_eq!(backend.read_file(Path::new("a.md")).await.unwrap(), "# hello");

    let hits = backend.search("hello", 10, SearchMode::Fts).await.unwrap();
    assert!(hits.iter().any(|h| h.path.ends_with("a.md")), "{hits:?}");

    let listing = backend.list_dir(Path::new(".")).await.unwrap();
    assert!(listing.iter().any(|(p, is_dir)| !is_dir && p.ends_with("a.md")));

    backend.delete_file(Path::new("a.md")).await.unwrap();
    assert!(backend.read_file(Path::new("a.md")).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn events_reach_a_subscriber_of_an_ipc_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path()).await;

    let backend = Arc::new(
        IpcBackend::connect(
            &endpoint,
            "sapphire-servertest",
            "test",
            env!("CARGO_PKG_VERSION"),
            &spawn,
            ws,
        )
        .await
        .unwrap(),
    );

    // `subscribe` is synchronous, so asking the server to start sending is a separate call.
    backend.start_events().await.unwrap();
    let mut events = backend.subscribe();
    backend.write_file(Path::new("a.md"), "x").await.unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), events.recv())
        .await
        .expect("an event within ten seconds")
        .unwrap();
    assert!(
        matches!(event, sapphire_backend::BackendEvent::FileChanged { .. }),
        "{event:?}"
    );
}
```

`sapphire-framework-backend/Cargo.toml` needs the server crate as a dev-dependency so
`CARGO_BIN_EXE_server-test-app` is set:

```toml
[dev-dependencies]
sapphire-framework-server = { version = "0.14.0", path = "../sapphire-framework-server", features = ["test-util"] }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sapphire-framework-backend --all-features --test ipc_backend`
Expected: FAIL — `IpcBackend` does not exist.

- [ ] **Step 3: Implement `IpcBackend`**

`crates/sapphire-framework-backend/src/error.rs`: add

```rust
    /// The IPC layer failed.
    #[error(transparent)]
    Ipc(#[from] sapphire_ipc::Error),
```

`crates/sapphire-framework-backend/src/ipc.rs`:

```rust
//! A [`WorkspaceBackend`] that forwards every call to the application's server.
//!
//! The server is the only process that may open the cache, so a CLI, a stdio MCP server or
//! a desktop UI holds one of these instead of a [`LocalBackend`](crate::LocalBackend). The
//! two are interchangeable: that is the whole point of the trait.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use sapphire_ipc::{Client, ClientInfo, Endpoint, SpawnConfig, ensure_server};
use tokio::sync::broadcast;

use crate::protocol as proto;
use crate::{
    BackendEvent, Error, FileSearchResult, Result, SearchMode, SyncSummary, WorkspaceBackend,
};

/// Capacity of the local event fan-out. Matches `LocalBackend`'s, so a subscriber behaves
/// the same whichever backend it holds.
const EVENT_CAPACITY: usize = 128;

/// A [`WorkspaceBackend`] over an IPC connection to the application's server.
#[derive(Debug)]
pub struct IpcBackend {
    client: Arc<Client>,
    ws: PathBuf,
    events: broadcast::Sender<BackendEvent>,
}

impl IpcBackend {
    /// Connect to `app`'s server, starting it if necessary, and bind to one workspace.
    pub async fn connect(
        endpoint: &Endpoint,
        app: &str,
        kind: &str,
        version: &str,
        spawn: &SpawnConfig,
        ws: PathBuf,
    ) -> Result<IpcBackend> {
        let info = ClientInfo {
            kind: kind.to_owned(),
            version: version.to_owned(),
            pid: std::process::id(),
        };
        let (client, _) = ensure_server(endpoint, app, info, spawn).await?;
        Ok(IpcBackend::from_client(Arc::new(client), ws))
    }

    /// Bind an existing client to one workspace.
    ///
    /// An application that already has a connection — because it also calls its own
    /// methods — passes it here rather than opening a second one.
    pub fn from_client(client: Arc<Client>, ws: PathBuf) -> IpcBackend {
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let backend = IpcBackend { client: Arc::clone(&client), ws: ws.clone(), events: events.clone() };

        // Translate this workspace's notifications into BackendEvents.
        let mut notifications = client.notifications();
        tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(n) if n.method == proto::EVENT => {
                        let Ok(params) =
                            serde_json::from_value::<proto::EventParams>(n.params)
                        else {
                            continue;
                        };
                        if params.ws == ws {
                            let _ = events.send(params.event);
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        backend
    }

    /// The underlying client, for an application's own methods.
    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    /// Ask the server to start sending this workspace's events.
    ///
    /// Called by [`subscribe`](WorkspaceBackend::subscribe) is not possible — that method is
    /// synchronous — so a caller that wants events calls this once after connecting.
    pub async fn start_events(&self) -> Result<()> {
        let _: proto::Ack = self
            .client
            .call(proto::SUBSCRIBE, proto::WsParams { ws: self.ws.clone() })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl WorkspaceBackend for IpcBackend {
    async fn search(
        &self,
        query: &str,
        limit: usize,
        mode: SearchMode,
    ) -> Result<Vec<FileSearchResult>> {
        let result: proto::SearchResult = self
            .client
            .call(
                proto::SEARCH,
                proto::SearchParams {
                    ws: self.ws.clone(),
                    query: query.to_owned(),
                    limit,
                    mode,
                },
            )
            .await?;
        Ok(result.hits)
    }

    async fn read_file(&self, path: &Path) -> Result<String> {
        let result: proto::ReadResult = self
            .client
            .call(
                proto::READ_FILE,
                proto::PathParams { ws: self.ws.clone(), path: path.to_owned() },
            )
            .await?;
        Ok(result.content)
    }

    async fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        let _: proto::Ack = self
            .client
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: self.ws.clone(),
                    path: path.to_owned(),
                    content: content.to_owned(),
                },
            )
            .await?;
        Ok(())
    }

    async fn append_file(&self, path: &Path, content: &str) -> Result<()> {
        let _: proto::Ack = self
            .client
            .call(
                proto::APPEND_FILE,
                proto::ContentParams {
                    ws: self.ws.clone(),
                    path: path.to_owned(),
                    content: content.to_owned(),
                },
            )
            .await?;
        Ok(())
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        let _: proto::Ack = self
            .client
            .call(
                proto::DELETE_FILE,
                proto::PathParams { ws: self.ws.clone(), path: path.to_owned() },
            )
            .await?;
        Ok(())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<(PathBuf, bool)>> {
        let result: proto::ListDirResult = self
            .client
            .call(
                proto::LIST_DIR,
                proto::PathParams { ws: self.ws.clone(), path: path.to_owned() },
            )
            .await?;
        Ok(result.entries.into_iter().map(|e| (e.path, e.is_dir)).collect())
    }

    async fn sync(&self) -> Result<SyncSummary> {
        let result: proto::ReindexResult = self
            .client
            .call(proto::REINDEX, proto::WsParams { ws: self.ws.clone() })
            .await?;
        Ok(SyncSummary { upserted: result.upserted, removed: result.removed })
    }

    fn subscribe(&self) -> broadcast::Receiver<BackendEvent> {
        self.events.subscribe()
    }
}
```

`crates/sapphire-framework-backend/src/lib.rs`: add `mod ipc;` and `pub use ipc::IpcBackend;`.

> **Why `start_events` is a separate call:** `WorkspaceBackend::subscribe` is synchronous, and
> telling the server to start sending is not. Making the trait method async would ripple into
> `LocalBackend` and the desktop for no benefit — a local backend has nobody to ask. So an IPC
> client calls `start_events()` once after connecting, and `subscribe()` as often as it likes.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p sapphire-framework-backend --all-features --test ipc_backend`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-backend Cargo.lock
git commit -m "feat(backend): add IpcBackend so a UI can leave the cache to the server"
```

---

### Task 7: `ServerCommand`

**Files:**
- Create: `crates/sapphire-framework-server/src/command.rs`
- Modify: `crates/sapphire-framework-server/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `command.rs`

**Interfaces:**
- Produces:
  - `ServerCommand::{Run, Status, Stop}` (`clap::Subcommand`)
  - `RunArgs { foreground: bool }`
  - `async ServerCommand::dispatch(self, server: AppServer, app: &str, version: &str) -> Result<i32>`
    — returns the process exit code

`service install` is **not** here. It arrives with `sapphire-framework-service` in step 10.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(subcommand)]
        server: ServerCommand,
    }

    #[test]
    fn the_subcommands_parse() {
        assert!(matches!(
            Probe::try_parse_from(["app", "run"]).unwrap().server,
            ServerCommand::Run(_)
        ));
        assert!(matches!(
            Probe::try_parse_from(["app", "status"]).unwrap().server,
            ServerCommand::Status
        ));
        assert!(matches!(
            Probe::try_parse_from(["app", "stop"]).unwrap().server,
            ServerCommand::Stop
        ));
    }

    #[test]
    fn run_takes_a_foreground_flag() {
        let parsed = Probe::try_parse_from(["app", "run", "--foreground"]).unwrap();
        match parsed.server {
            ServerCommand::Run(args) => assert!(args.foreground),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn status_reports_no_server_when_none_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = sapphire_ipc::Endpoint::in_dir("status-test", tmp.path().to_path_buf());
        let code = status(&endpoint, "status-test", "0.0.0").await.unwrap();
        assert_eq!(code, 1, "no server is a non-zero exit");
    }

    #[tokio::test]
    async fn stop_reports_no_server_when_none_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = sapphire_ipc::Endpoint::in_dir("stop-test", tmp.path().to_path_buf());
        let code = stop(&endpoint, "stop-test", "0.0.0").await.unwrap();
        assert_eq!(code, 1);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features command`
Expected: FAIL — `ServerCommand` does not exist.

- [ ] **Step 3: Implement the command**

`crates/sapphire-framework-server/src/command.rs`:

```rust
//! The `server` subcommands an application flattens into its own CLI.

use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig, ensure_server};

use crate::error::Result;
use crate::AppServer;

/// Subcommands for managing this application's server.
#[derive(Debug, clap::Subcommand)]
pub enum ServerCommand {
    /// Run the server in this process.
    Run(RunArgs),
    /// Report whether a server is running, and which version.
    Status,
    /// Ask a running server to exit.
    Stop,
}

/// Arguments of `server run`.
#[derive(Debug, clap::Args)]
pub struct RunArgs {
    /// Stay in the foreground and never exit on idle.
    ///
    /// Without it the server exits once nothing has used it for a while, which is what a
    /// server started on demand by a CLI should do.
    #[arg(long)]
    pub foreground: bool,
}

impl ServerCommand {
    /// Carry out the command, returning the process exit code.
    pub async fn dispatch(
        self,
        server: AppServer,
        app: &str,
        version: &str,
    ) -> Result<i32> {
        match self {
            ServerCommand::Run(args) => {
                let server = if args.foreground { server.idle_exit(None) } else { server };
                server.run().await?;
                Ok(0)
            }
            ServerCommand::Status => status(&Endpoint::for_app(app)?, app, version).await,
            ServerCommand::Stop => stop(&Endpoint::for_app(app)?, app, version).await,
        }
    }
}

fn client_info(version: &str) -> ClientInfo {
    ClientInfo {
        kind: "cli".to_owned(),
        version: version.to_owned(),
        pid: std::process::id(),
    }
}

async fn status(endpoint: &Endpoint, app: &str, version: &str) -> Result<i32> {
    if !sapphire_ipc::probe(endpoint).await? {
        println!("no {app} server is running");
        return Ok(1);
    }
    let (_, info) =
        ensure_server(endpoint, app, client_info(version), &SpawnConfig::disabled()).await?;
    println!(
        "{app} server running: version {}, pid {}, started as {:?}",
        info.version, info.pid, info.managed_by
    );
    Ok(0)
}

async fn stop(endpoint: &Endpoint, app: &str, version: &str) -> Result<i32> {
    if !sapphire_ipc::probe(endpoint).await? {
        println!("no {app} server is running");
        return Ok(1);
    }
    let (client, info) =
        ensure_server(endpoint, app, client_info(version), &SpawnConfig::disabled()).await?;
    let _: serde_json::Value =
        client.call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({})).await?;
    println!("asked the {app} server (pid {}) to exit", info.pid);
    Ok(0)
}
```

`lib.rs`: add `mod command;` and `pub use command::{RunArgs, ServerCommand};`.

`status` and `stop` use `SpawnConfig::disabled()` deliberately: neither should start a server
in order to ask about it or to stop it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features command`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): add the server run/status/stop subcommands"
```

---

### Task 8: The regression test, the facade and the architecture note

**Files:**
- Create: `crates/sapphire-framework-server/tests/concurrent.rs`
- Modify: `crates/sapphire-framework/src/lib.rs`
- Modify: `docs/ARCHITECTURE.md`

This is what the whole design exists for. It must fail on `main` and pass here.

- [ ] **Step 1: Write the test**

`crates/sapphire-framework-server/tests/concurrent.rs`:

```rust
//! Several client processes using one workspace at the same time.
//!
//! This is the problem the process architecture exists to solve: a redb database takes an
//! exclusive file lock, so two processes that open an app's cache directly cannot both run.
//! `first_pins_the_problem` states that; the rest show it gone.

use std::path::{Path, PathBuf};

use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig, ensure_server};
use sapphire_backend::protocol as proto;

fn client_info() -> ClientInfo {
    ClientInfo {
        kind: "cli".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        pid: std::process::id(),
    }
}

fn fixture(tmp: &Path) -> (Endpoint, PathBuf, SpawnConfig) {
    let runtime = tmp.join("run");
    let state = tmp.join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let root = tmp.join("ws");
    std::fs::create_dir_all(root.join(".sapphire-servertest")).unwrap();
    let root = root.canonicalize().unwrap();

    let endpoint = Endpoint::in_dir("sapphire-servertest", runtime.clone());
    let spawn = SpawnConfig {
        exe: env!("CARGO_BIN_EXE_server-test-app").into(),
        args: vec![runtime.display().to_string(), state.display().to_string()],
        ..SpawnConfig::default()
    };
    (endpoint, root, spawn)
}

/// The behaviour that forced this design: a second direct open of the cache fails.
#[test]
fn first_pins_the_problem() {
    use sapphire_workspace::{AppContext, AppKind, Workspace, WorkspaceState};

    static CTX: AppContext = AppContext::new("sapphire-lockproof");

    let tmp = tempfile::tempdir().unwrap();
    // SAFETY: set before any other thread reads the environment in this test binary.
    unsafe {
        std::env::set_var("SAPPHIRE_LOCKPROOF_CACHE_DIR", tmp.path().join("cache"));
        std::env::set_var("SAPPHIRE_LOCKPROOF_DATA_DIR", tmp.path().join("data"));
        std::env::set_var("SAPPHIRE_LOCKPROOF_CONFIG_DIR", tmp.path().join("config"));
    }
    CTX.init(AppKind::Server);

    let root = tmp.path().join("ws");
    std::fs::create_dir_all(root.join(".sapphire-lockproof")).unwrap();

    let first = WorkspaceState::open(Workspace::from_root(&CTX, &root).unwrap()).unwrap();
    let second = WorkspaceState::open(Workspace::from_root(&CTX, &root).unwrap());
    assert!(
        second.is_err(),
        "if this ever passes, the premise of the process architecture has changed"
    );
    drop(first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_clients_all_write_successfully() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path());

    let mut tasks = Vec::new();
    for n in 0..8u32 {
        let (endpoint, ws, spawn) = (endpoint.clone(), ws.clone(), spawn.clone());
        tasks.push(tokio::spawn(async move {
            let (client, _) =
                ensure_server(&endpoint, "sapphire-servertest", client_info(), &spawn)
                    .await
                    .expect("a server");
            let _: proto::Ack = client
                .call(
                    proto::WRITE_FILE,
                    proto::ContentParams {
                        ws,
                        path: PathBuf::from(format!("note-{n}.md")),
                        content: format!("written by client {n}"),
                    },
                )
                .await
                .expect("the write");
        }));
    }
    for task in tasks {
        task.await.expect("the task");
    }

    for n in 0..8u32 {
        assert!(
            ws.join(format!("note-{n}.md")).exists(),
            "client {n}'s file is missing"
        );
    }
}

/// The original report: `journal add` while the stdio MCP server is running.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_lived_client_and_a_one_shot_client_coexist() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path());

    // The MCP server: connects and stays.
    let (long_lived, _) =
        ensure_server(&endpoint, "sapphire-servertest", client_info(), &spawn).await.unwrap();
    let _: proto::Ack = long_lived
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: ws.clone(),
                path: PathBuf::from("from-mcp.md"),
                content: "agent".into(),
            },
        )
        .await
        .unwrap();

    // The CLI: connects, writes, goes away.
    {
        let (one_shot, _) =
            ensure_server(&endpoint, "sapphire-servertest", client_info(), &spawn).await.unwrap();
        let _: proto::Ack = one_shot
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("from-cli.md"),
                    content: "human".into(),
                },
            )
            .await
            .unwrap();
    }

    // The long-lived client still works after the other one left.
    let read: proto::ReadResult = long_lived
        .call(
            proto::READ_FILE,
            proto::PathParams { ws, path: PathBuf::from("from-cli.md") },
        )
        .await
        .unwrap();
    assert_eq!(read.content, "human");
}
```

`crates/sapphire-framework-server/Cargo.toml` needs `sapphire-workspace` in
`[dev-dependencies]` with its default features, so `first_pins_the_problem` can open a real
store:

```toml
[dev-dependencies]
sapphire-backend = { package = "sapphire-framework-backend", version = "0.14.0", path = "../sapphire-framework-backend" }
sapphire-framework-server = { path = ".", features = ["test-util"] }
tempfile = "3"
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features --test concurrent`
Expected: PASS, 3 tests.

If `first_pins_the_problem` fails — that is, a second direct open succeeds — stop and report
it rather than deleting the test. It would mean the premise of this whole design changed, and
that is a conversation, not a test to adjust.

- [ ] **Step 3: Export from the facade**

`crates/sapphire-framework/src/lib.rs`, in the prelude or re-export block alongside the other
backend items:

```rust
#[cfg(feature = "backend")]
pub use crate::backend::{IpcBackend, protocol};
```

and, guarded by the `server` feature:

```rust
#[cfg(feature = "server")]
pub use sapphire_framework_server::{AppServer, ServerCommand, WorkspaceHost};
```

Run: `cargo build -p sapphire-framework --all-features`
Expected: success.

- [ ] **Step 4: Note the crate in `ARCHITECTURE.md`**

`docs/ARCHITECTURE.md`, in the crate table, after the `sapphire-framework-ipc` row added by
the IPC plan:

```markdown
| `sapphire-framework-server` | アプリサーバ骨格（`workspace.*` 名前空間・ワークスペース多重管理・アイドル終了・`ServerCommand`） | ✅ |
```

and in the `sapphire-framework-backend` row's description, add `IpcBackend`.

- [ ] **Step 5: Run the whole suite and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-server crates/sapphire-framework docs/ARCHITECTURE.md Cargo.lock
git commit -m "test(server): prove concurrent clients no longer contend for the cache"
```

---

## What this plan does not cover

| Spec step | Left for |
|---|---|
| `sync.enable` / `sync.disable` / `sync.status` | step 7, once the bridge exists |
| Privilege separation (`PrivilegeConfig`, `run_as`, `helper_as`) | step 5 |
| The bridge control and data planes | step 6 |
| `service install` | step 10 |
| HTTP endpoints (`/mcp`, `/acp`, `/a2a`) on `AppServer` | folded into each app's migration; `AppServer::http` is added when the first app needs it |
| Removing the per-kind directory layout (spec §7) | step 11 |
| Migrating journal, ledger, timer and agent onto `AppServer` | each app's own repository and spec |
