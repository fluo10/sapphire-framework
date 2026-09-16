# Bridge Basics (`sapphire-framework-bridge`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Build the host-wide daemon every app server talks to — device identity, workgroup
authorization, and a switchboard that splices a peer's connection to whichever app server owns
the workspace it asked for — as the new crates `sapphire-framework-bridge-api` and
`sapphire-framework-bridge`, plus the `sapphire-bridge` binary.

**Architecture:** Two listeners in one process. The **control plane** is JSON-RPC over the
bridge's own endpoint: app servers register the workspaces they own, and the bridge tells them
when a peer wants one. The **data plane** is a second endpoint where a connection sends one
header line and then becomes raw bytes, spliced to an iroh stream. Between the two sits a
routing table from workspace to owner. The bridge holds no application replication state:
merging and file writing happen at the ends, and this process only moves bytes between them.
Peer transport is a trait, so every task but one is tested without a network.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), `sapphire-framework-ipc`,
`sapphire-framework-registry`, `sapphire-framework-sync`, iroh 1.2, tokio 1, serde + toml +
serde_json, grain-id 0.16, clap 4, thiserror 2, tracing; dev: tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-16-process-architecture-design.md` — §5 in full,
§4.3 (the control and data planes as the app server sees them), §1 (what the bridge owns).
Implementation order step 6 of that spec's §9. The surviving material of
`docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` §3.1, §3.4, §3.5 and §3.8 applies,
read through the substitution table at the head of its §3.

**Depends on:** steps 2 (`docs/superpowers/plans/2026-09-16-registry-devices-plan.md`),
3 (`2026-09-16-ipc-layer-plan.md`) and 4 (`2026-09-16-app-server-plan.md`) must be complete.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task. Commit
  `Cargo.lock` whenever dependencies change.
- Every public item carries a doc comment; both crates have `#![warn(missing_docs)]`.
- `sapphire-framework-bridge` must **not** depend on `-workspace`, `-retrieve` or `-backend`.
  The bridge never looks inside a workspace. A test pins this.
- The bridge directory is `<dirs::data_dir()>/sapphire/bridge/`, overridden whole by
  `SAPPHIRE_BRIDGE_DIR`. Mode `0700` on Unix, like everything else the framework creates.
- `BRIDGE_FORMAT_VERSION` is `1`. A directory whose `format` is newer stops the bridge; an
  older one is migrated before anything else runs.
- Identifiers a user can see or type are grain-ids (`workgroup_id`, `workspace_id`,
  `device_id`). A node id is iroh's, 64 lowercase hex characters, carried as a string
  everywhere outside the iroh transport.
- The ALPN is `sapphire/ws/1`.
- **Pairing, invites and `workgroup join` are step 8**, not here. This plan includes only
  `workgroup create`, because without a workgroup nothing can be authorized and nothing can be
  tested.

## Two deviations from the spec, agreed up front

1. **The control-plane types live in their own crate, `sapphire-framework-bridge-api`.** The
   spec §6 says they sit behind a dependency-light `client` feature of `-bridge`. That does
   not hold: Cargo unifies features across a workspace build, so the moment the `sapphire-bridge`
   binary turns iroh on, every other crate in the same build links a `-bridge` that contains
   it. A separate serde-only crate makes "the app server does not pull in iroh" a fact rather
   than an intention.
2. **Inbound streams are announced, not pushed.** The spec §4.3 shows the bridge splicing an
   iroh stream to "that owner's data connection". An app server has no listener for the bridge
   to connect to, so the bridge sends a `bridge.incoming` notification carrying a ticket on the
   control connection, and the app server opens a data connection quoting it. One listener,
   one direction of connection, and a ticket that is useless to anyone who did not receive it.

## The shape of a connection

```
 app server                         bridge                        peer
     |                                |                             |
     |-- control: ipc.hello --------->|                             |
     |-- bridge.register ------------>|  routes.toml                |
     |                                |                             |
     |  --- outbound ---              |                             |
     |-- data conn ------------------>|                             |
     |   {"kind":"open", ws, device}  |-- iroh stream (ALPN) ------>|
     |<------ {"ok":true} ------------|                             |
     |========== raw bytes ===========|=========== raw bytes =======|
     |                                |                             |
     |  --- inbound ---               |<----- iroh stream ----------|
     |<-- bridge.incoming {ticket} ---|  (authorize the node id)    |
     |-- data conn ------------------>|                             |
     |   {"kind":"accept", ticket}    |                             |
     |<------ {"ok":true} ------------|                             |
     |========== raw bytes ===========|=========== raw bytes =======|
```

The replication protocol runs end to end between two app servers. The bridge decides *whether*
the bytes may flow and *where* they go, and never what they mean.

## File Structure

```
crates/sapphire-framework-bridge-api/
    Cargo.toml
    src/
        lib.rs        # method names, params, results, DataHeader, BridgeClient

crates/sapphire-framework-bridge/
    Cargo.toml
    src/
        lib.rs        # Bridge, module wiring
        error.rs      # Error, Result
        dir.rs        # BridgeDir: layout, format version, single-instance lock
        routes.rs     # RouteTable, routes.toml
        control.rs    # the bridge.* handlers and the registry of connected app servers
        data.rs       # the data-plane listener, tickets, splicing
        peer.rs       # PeerTransport trait, PeerStream, LoopbackTransport (test-util)
        iroh.rs       # IrohTransport (feature `node`)
        workgroup.rs  # workgroup.toml, the device ledger, authorization, `workgroup create`
        net.rs        # net.toml
        command.rs    # BridgeCommand (clap)

apps/sapphire-bridge/
    Cargo.toml
    src/main.rs
```

---

### Task 1: `sapphire-framework-bridge-api`

**Files:**
- Create: `crates/sapphire-framework-bridge-api/Cargo.toml`
- Create: `crates/sapphire-framework-bridge-api/src/lib.rs`
- Modify: `Cargo.toml` (workspace `members`)
- Test: inline `#[cfg(test)] mod tests` in `lib.rs`

**Interfaces:**
- Produces:
  - `BRIDGE_NAME: &str = "bridge"`, `BRIDGE_DATA_NAME: &str = "bridge-data"`,
    `ALPN: &[u8] = b"sapphire/ws/1"`
  - method names `REGISTER`, `UNREGISTER`, `PEERS`, `STATUS`, and the notification `INCOMING`
  - `WorkspaceRegistration { workspace_id: GrainId, root: PathBuf }`
  - `RegisterParams { app_name: String, exe_path: PathBuf, managed_by: ManagedBy, workspaces: Vec<WorkspaceRegistration> }`
  - `RegisterResult { device_id: GrainId, node_id: String, workgroup_id: GrainId }`
  - `UnregisterParams { workspace_id: GrainId }`
  - `PeerInfo { device_id: GrainId, name: String, node_id: String, connected: bool }`,
    `PeersResult { peers: Vec<PeerInfo> }`
  - `RouteStatus { workspace_id: GrainId, app_name: String, root: PathBuf, owner_online: bool }`
  - `StatusResult { version: String, node_id: String, workgroup: Option<WorkgroupStatus>, routes: Vec<RouteStatus> }`,
    `WorkgroupStatus { workgroup_id: GrainId, name: String, devices: usize }`
  - `IncomingParams { workspace_id: GrainId, peer_device_id: GrainId, ticket: String }`
  - `DataHeader::{Open { workspace_id, device_id }, Accept { ticket }}` (serde tag `kind`)
  - `DataAck { ok: bool, error: Option<String> }`
  - `Ack {}`
  - `BridgeClient` — `connect`, `register`, `unregister`, `peers`, `status`,
    `incoming(&self) -> broadcast::Receiver<IncomingParams>`, `open_stream` and `accept_stream` (both returning `sapphire_ipc::RawStream`)

**Why a separate crate:** so that `sapphire-framework-server` can talk to the bridge without
linking iroh. Keep it serde plus `-ipc` and nothing else; a test asserts the manifest.

- [ ] **Step 1: Create the manifest**

`crates/sapphire-framework-bridge-api/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-bridge-api"
version.workspace = true
edition.workspace = true
description = "Control-plane protocol and client for the sapphire-framework bridge"
license.workspace = true
repository.workspace = true
keywords = ["bridge", "ipc", "protocol", "local-first"]
categories = ["network-programming"]

[dependencies]
sapphire-ipc = { package = "sapphire-framework-ipc", version = "0.14.0", path = "../sapphire-framework-ipc" }
grain-id.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["rt", "sync"] }
tracing.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time"] }
```

Root `Cargo.toml`: add `"crates/sapphire-framework-bridge-api",` to `members`.

- [ ] **Step 2: Write the failing tests**

`crates/sapphire-framework-bridge-api/src/lib.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned>(v: &T) -> T {
        serde_json::from_value(serde_json::to_value(v).unwrap()).unwrap()
    }

    fn id() -> GrainId {
        GrainId::random()
    }

    #[test]
    fn registration_round_trips() {
        let params = RegisterParams {
            app_name: "sapphire-journal".into(),
            exe_path: "/usr/bin/sapphire-journal".into(),
            managed_by: sapphire_ipc::ManagedBy::Spawned,
            workspaces: vec![WorkspaceRegistration {
                workspace_id: id(),
                root: "/home/me/journal".into(),
            }],
        };
        let back = round_trip(&params);
        assert_eq!(back.app_name, "sapphire-journal");
        assert_eq!(back.workspaces.len(), 1);
    }

    #[test]
    fn an_open_header_round_trips_and_is_tagged() {
        let header = DataHeader::Open { workspace_id: id(), device_id: id() };
        let value = serde_json::to_value(&header).unwrap();
        assert_eq!(value["kind"], "open");
        assert_eq!(round_trip(&header), header);
    }

    #[test]
    fn an_accept_header_round_trips_and_is_tagged() {
        let header = DataHeader::Accept { ticket: "t-123".into() };
        let value = serde_json::to_value(&header).unwrap();
        assert_eq!(value["kind"], "accept");
        assert_eq!(round_trip(&header), header);
    }

    #[test]
    fn an_unknown_header_kind_is_refused() {
        let value = serde_json::json!({ "kind": "sideways" });
        assert!(serde_json::from_value::<DataHeader>(value).is_err());
    }

    #[test]
    fn a_data_ack_carries_its_reason_when_it_fails() {
        let ack = DataAck { ok: false, error: Some("no such workspace".into()) };
        assert_eq!(round_trip(&ack).error.as_deref(), Some("no such workspace"));
    }

    #[test]
    fn method_names_are_namespaced() {
        for name in [REGISTER, UNREGISTER, PEERS, STATUS, INCOMING] {
            assert!(name.starts_with("bridge."), "{name}");
        }
    }

    #[test]
    fn the_crate_stays_free_of_the_workspace_and_network_stacks() {
        let manifest = include_str!("../Cargo.toml");
        for forbidden in ["iroh", "sapphire-framework-workspace", "sapphire-framework-retrieve", "sapphire-framework-backend"] {
            assert!(
                !manifest.contains(forbidden),
                "sapphire-framework-bridge-api must not depend on {forbidden}"
            );
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge-api`
Expected: FAIL — the crate has no types.

- [ ] **Step 4: Implement the protocol types**

`crates/sapphire-framework-bridge-api/src/lib.rs`:

```rust
//! What an app server says to the bridge, and how it says it.
//!
//! Kept separate from `sapphire-framework-bridge` so that an app server can talk to the
//! bridge without linking iroh: Cargo unifies features across a workspace build, so a
//! feature flag on one crate would not have been enough.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.3 and §5.

#![warn(missing_docs)]

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use grain_id::GrainId;
pub use sapphire_ipc::ManagedBy;

mod client;
pub use client::BridgeClient;

/// The endpoint name the bridge's control plane listens under.
pub const BRIDGE_NAME: &str = "bridge";
/// The endpoint name the bridge's data plane listens under.
pub const BRIDGE_DATA_NAME: &str = "bridge-data";
/// The application-layer protocol name used on every peer connection.
pub const ALPN: &[u8] = b"sapphire/ws/1";

/// Announce which workspaces this app server owns.
pub const REGISTER: &str = "bridge.register";
/// Stop owning one workspace.
pub const UNREGISTER: &str = "bridge.unregister";
/// List the workgroup's devices and whether they are connected.
pub const PEERS: &str = "bridge.peers";
/// Describe the bridge.
pub const STATUS: &str = "bridge.status";
/// Notification: a peer wants a workspace this app server owns.
pub const INCOMING: &str = "bridge.incoming";

/// The success payload of a call that returns nothing.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Ack {}

/// One workspace an app server owns.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WorkspaceRegistration {
    /// The workspace's sync identity, shared across devices.
    pub workspace_id: GrainId,
    /// Where it lives on this host.
    pub root: PathBuf,
}

/// Parameters of [`REGISTER`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegisterParams {
    /// Which application this server belongs to.
    pub app_name: String,
    /// The executable to run when a peer wants a workspace and this server is not running.
    pub exe_path: PathBuf,
    /// How this server was started. A `Service` server is never started by the bridge.
    pub managed_by: ManagedBy,
    /// The workspaces it owns. Registering again replaces the previous list for this app.
    pub workspaces: Vec<WorkspaceRegistration>,
}

/// Result of [`REGISTER`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegisterResult {
    /// This host's device id inside the workgroup, used as `Entry.author`.
    pub device_id: GrainId,
    /// This host's iroh node id.
    pub node_id: String,
    /// The workgroup these workspaces belong to.
    pub workgroup_id: GrainId,
}

/// Parameters of [`UNREGISTER`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UnregisterParams {
    /// The workspace to stop owning.
    pub workspace_id: GrainId,
}

/// One device of the workgroup.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeerInfo {
    /// Its device id.
    pub device_id: GrainId,
    /// Its name.
    pub name: String,
    /// Its iroh node id.
    pub node_id: String,
    /// Whether the bridge currently holds a connection to it.
    pub connected: bool,
}

/// Result of [`PEERS`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeersResult {
    /// Every non-retired device of the workgroup, this host included.
    pub peers: Vec<PeerInfo>,
}

/// One row of the routing table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteStatus {
    /// The workspace.
    pub workspace_id: GrainId,
    /// The application that owns it.
    pub app_name: String,
    /// Where it lives on this host.
    pub root: PathBuf,
    /// Whether that application's server is connected right now.
    pub owner_online: bool,
}

/// The workgroup this host belongs to.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkgroupStatus {
    /// Its id.
    pub workgroup_id: GrainId,
    /// Its name.
    pub name: String,
    /// How many non-retired devices it has.
    pub devices: usize,
}

/// Result of [`STATUS`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StatusResult {
    /// The bridge's version.
    pub version: String,
    /// This host's iroh node id.
    pub node_id: String,
    /// The workgroup, if this host has joined one.
    pub workgroup: Option<WorkgroupStatus>,
    /// Every registered workspace.
    pub routes: Vec<RouteStatus>,
}

/// Parameters of the [`INCOMING`] notification.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IncomingParams {
    /// The workspace the peer asked for.
    pub workspace_id: GrainId,
    /// Which device asked.
    pub peer_device_id: GrainId,
    /// A single-use token naming the waiting stream.
    ///
    /// Useless to anyone who did not receive this notification, and consumed the first time
    /// it is presented.
    pub ticket: String,
}

/// The first line of a data-plane connection.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum DataHeader {
    /// The app server wants a stream to `device_id` for `workspace_id`.
    Open {
        /// The workspace being synced.
        workspace_id: GrainId,
        /// The peer to reach.
        device_id: GrainId,
    },
    /// The app server is answering a [`INCOMING`] notification.
    Accept {
        /// The ticket from that notification.
        ticket: String,
    },
}

/// The bridge's answer to a [`DataHeader`], sent as one line before the raw bytes begin.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataAck {
    /// Whether the stream is open.
    pub ok: bool,
    /// Why not, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
```

- [ ] **Step 5: Write the client**

`crates/sapphire-framework-bridge-api/src/client.rs`:

```rust
//! The app server's side of the control plane.

use std::sync::Arc;

use sapphire_ipc::{Client, ClientInfo, Connection, Endpoint, SpawnConfig, ensure_server};
use tokio::sync::broadcast;

use crate::{
    Ack, BRIDGE_DATA_NAME, BRIDGE_NAME, DataAck, DataHeader, GrainId, IncomingParams,
    PeersResult, REGISTER, RegisterParams, RegisterResult, STATUS, StatusResult, UNREGISTER,
    UnregisterParams, PEERS,
};

/// How many pending incoming announcements a subscriber may fall behind by.
const INCOMING_CAPACITY: usize = 64;

/// An app server's connection to the bridge.
#[derive(Debug)]
pub struct BridgeClient {
    client: Arc<Client>,
    incoming: broadcast::Sender<IncomingParams>,
    runtime_dir: std::path::PathBuf,
}

impl BridgeClient {
    /// Connect to the bridge, starting it if nothing is listening.
    pub async fn connect(
        kind: &str,
        version: &str,
        spawn: &SpawnConfig,
    ) -> sapphire_ipc::Result<BridgeClient> {
        let runtime_dir = sapphire_ipc::runtime_dir()?;
        let endpoint = Endpoint::in_dir(BRIDGE_NAME, runtime_dir.clone());
        let info = ClientInfo {
            kind: kind.to_owned(),
            version: version.to_owned(),
            pid: std::process::id(),
        };
        let (client, _) = ensure_server(&endpoint, BRIDGE_NAME, info, spawn).await?;
        Ok(BridgeClient::from_client(Arc::new(client), runtime_dir))
    }

    /// Wrap an existing connection. Used by tests and by a caller that already has one.
    pub fn from_client(client: Arc<Client>, runtime_dir: std::path::PathBuf) -> BridgeClient {
        let (incoming, _) = broadcast::channel(INCOMING_CAPACITY);
        let mut notifications = client.notifications();
        let sender = incoming.clone();
        tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(n) if n.method == crate::INCOMING => {
                        match serde_json::from_value::<IncomingParams>(n.params) {
                            Ok(params) => {
                                let _ = sender.send(params);
                            }
                            Err(err) => tracing::warn!("malformed bridge.incoming: {err}"),
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "fell behind on bridge announcements");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        BridgeClient { client, incoming, runtime_dir }
    }

    /// Announce the workspaces this app server owns.
    pub async fn register(&self, params: RegisterParams) -> sapphire_ipc::Result<RegisterResult> {
        self.client.call(REGISTER, params).await
    }

    /// Stop owning one workspace.
    pub async fn unregister(&self, workspace_id: GrainId) -> sapphire_ipc::Result<()> {
        let _: Ack = self.client.call(UNREGISTER, UnregisterParams { workspace_id }).await?;
        Ok(())
    }

    /// The workgroup's devices.
    pub async fn peers(&self) -> sapphire_ipc::Result<PeersResult> {
        self.client.call(PEERS, serde_json::json!({})).await
    }

    /// What the bridge knows about itself.
    pub async fn status(&self) -> sapphire_ipc::Result<StatusResult> {
        self.client.call(STATUS, serde_json::json!({})).await
    }

    /// Announcements that a peer wants a workspace this server owns.
    ///
    /// Answer each one by calling [`accept_stream`](Self::accept_stream) with its ticket.
    pub fn incoming(&self) -> broadcast::Receiver<IncomingParams> {
        self.incoming.subscribe()
    }

    /// Open a stream to `device` for `workspace`.
    pub async fn open_stream(
        &self,
        workspace_id: GrainId,
        device_id: GrainId,
    ) -> sapphire_ipc::Result<sapphire_ipc::RawStream> {
        self.data(DataHeader::Open { workspace_id, device_id }).await
    }

    /// Claim the stream a [`IncomingParams`] announced.
    pub async fn accept_stream(
        &self,
        ticket: String,
    ) -> sapphire_ipc::Result<sapphire_ipc::RawStream> {
        self.data(DataHeader::Accept { ticket }).await
    }

    async fn data(&self, header: DataHeader) -> sapphire_ipc::Result<sapphire_ipc::RawStream> {
        let endpoint = Endpoint::in_dir(BRIDGE_DATA_NAME, self.runtime_dir.clone());
        let raw = sapphire_ipc::connect_raw(&endpoint).await?;
        crate::handshake_data(raw, header).await
    }
}
```

> `Connection` frames messages; the data plane must not. Add to `sapphire-framework-ipc`, in
> `spawn.rs` beside `connect`:
>
> ```rust
> /// Connect to `endpoint` and hand back the byte stream, unframed.
> ///
> /// The bridge's data plane speaks one JSON line and then raw bytes, so it cannot use
> /// [`Connection`](crate::Connection).
> pub async fn connect_raw(endpoint: &Endpoint) -> Result<RawStream> { … }
> ```
>
> where `RawStream` is `tokio::net::UnixStream` on Unix and `NamedPipeClient` on Windows,
> exposed as `Box<dyn AsyncRead + AsyncWrite + Send + Unpin>`. Add `pub type RawStream` and
> the matching `accept_raw` on the listener types. Do this in Task 1 and note it in the
> commit: it is a small, clearly motivated addition to the previous plan's crate.

And the shared line handshake, in `bridge-api/src/lib.rs`:

```rust
/// Send `header`, read the acknowledgement, and hand back the stream ready for raw bytes.
pub async fn handshake_data<S>(mut stream: S, header: DataHeader) -> sapphire_ipc::Result<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut line = serde_json::to_vec(&header)?;
    line.push(b'\n');
    stream.write_all(&line).await?;
    stream.flush().await?;

    // Read exactly one line without buffering past it, so the raw bytes that follow stay on
    // the stream.
    let mut reader = BufReader::with_capacity(1, &mut stream);
    let mut answer = String::new();
    reader.read_line(&mut answer).await?;
    let ack: DataAck = serde_json::from_str(answer.trim())?;
    if !ack.ok {
        return Err(sapphire_ipc::Error::Protocol(
            ack.error.unwrap_or_else(|| "the bridge refused the stream".to_owned()),
        ));
    }
    Ok(stream)
}
```

`BufReader::with_capacity(1, …)` is deliberate: a larger buffer would swallow bytes belonging
to the payload. Write a test for exactly that in Task 5.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge-api -p sapphire-framework-ipc --all-features`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge-api crates/sapphire-framework-ipc Cargo.toml Cargo.lock
git commit -m "feat(bridge): define the control-plane protocol and its client"
```

---

### Task 2: The bridge directory and the single-instance lock

**Files:**
- Create: `crates/sapphire-framework-bridge/Cargo.toml`
- Create: `crates/sapphire-framework-bridge/src/{lib.rs,error.rs,dir.rs}`
- Modify: `Cargo.toml` (workspace `members`), facade
- Test: inline `#[cfg(test)] mod tests` in `dir.rs`

**Interfaces:**
- Produces:
  - `Error::{Io, Ipc, Registry, Format, NoWorkgroup, UnknownWorkspace, AlreadyRunning, Config}`, `Result<T>`
  - `BRIDGE_DIR_ENV: &str = "SAPPHIRE_BRIDGE_DIR"`, `BRIDGE_FORMAT_VERSION: u32 = 1`
  - `BridgeDir { root: PathBuf }`: `open() -> Result<BridgeDir>`, `at(root: PathBuf) -> Result<BridgeDir>`,
    `node_key(&self)`, `lock_path(&self)`, `net_toml(&self)`, `routes_toml(&self)`,
    `status_json(&self)`, `log_dir(&self)`, `workgroups_dir(&self)`,
    `workgroup_dir(&self, id: GrainId)`, `devices_dir(&self, id: GrainId)`
  - `InstanceLock`: `acquire(dir: &BridgeDir) -> Result<InstanceLock>`, released on drop

`bridge.lock` means only "there must not be a second bridge". It is not an election: a process
that cannot take it connects to the running bridge and is done — no handoff, no follower state.

- [ ] **Step 1: Create the manifest**

`crates/sapphire-framework-bridge/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-bridge"
version.workspace = true
edition.workspace = true
description = "Host-wide sapphire daemon: device identity, workgroup authorization and a switchboard to app servers"
license.workspace = true
repository.workspace = true
keywords = ["bridge", "p2p", "iroh", "local-first"]
categories = ["network-programming"]

[features]
default = []
# The real peer transport. Off by default so a build that only needs the switchboard, or a
# test, does not compile iroh.
node = ["dep:iroh"]
# Loopback transport and fixtures for tests.
test-util = []

[dependencies]
sapphire-bridge-api = { package = "sapphire-framework-bridge-api", version = "0.14.0", path = "../sapphire-framework-bridge-api" }
sapphire-ipc = { package = "sapphire-framework-ipc", version = "0.14.0", path = "../sapphire-framework-ipc" }
sapphire-registry = { package = "sapphire-framework-registry", version = "0.14.0", path = "../sapphire-framework-registry" }
clap.workspace = true
dirs.workspace = true
getrandom.workspace = true
grain-id.workspace = true
iroh = { version = "1.2", optional = true }
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "io-util", "sync", "time", "process"] }
toml.workspace = true
tracing.workspace = true

[dev-dependencies]
sapphire-framework-bridge = { path = ".", features = ["test-util"] }
tempfile = "3"
```

Root `Cargo.toml`: add `"crates/sapphire-framework-bridge",`.
Facade: feature `bridge = ["dep:sapphire-framework-bridge"]` and the matching optional
dependency and `pub use`.

- [ ] **Step 2: Write the failing tests**

`crates/sapphire-framework-bridge/src/dir.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opening_creates_the_layout_and_stamps_the_format() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();

        assert!(dir.root.is_dir());
        assert!(dir.log_dir().is_dir());
        assert!(dir.workgroups_dir().is_dir());
        assert_eq!(
            std::fs::read_to_string(dir.root.join("format")).unwrap().trim(),
            BRIDGE_FORMAT_VERSION.to_string()
        );
    }

    #[test]
    fn opening_twice_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bridge");
        BridgeDir::at(path.clone()).unwrap();
        BridgeDir::at(path).unwrap();
    }

    #[test]
    fn a_newer_format_stops_the_bridge() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bridge");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("format"), (BRIDGE_FORMAT_VERSION + 1).to_string()).unwrap();

        let err = BridgeDir::at(path).unwrap_err();
        assert!(err.to_string().contains("newer"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn the_directory_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let mode = std::fs::metadata(&dir.root).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "mode was {:o}", mode & 0o777);
    }

    #[test]
    fn the_environment_variable_replaces_the_whole_path() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: the test binary sets this before any other thread reads it.
        unsafe { std::env::set_var(BRIDGE_DIR_ENV, tmp.path()) };
        let dir = BridgeDir::open().unwrap();
        unsafe { std::env::remove_var(BRIDGE_DIR_ENV) };
        assert_eq!(dir.root, tmp.path());
    }

    #[test]
    fn a_second_instance_cannot_take_the_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();

        let first = InstanceLock::acquire(&dir).unwrap();
        let err = InstanceLock::acquire(&dir).unwrap_err();
        assert!(err.to_string().contains("already running"), "{err}");

        drop(first);
        InstanceLock::acquire(&dir).expect("the lock must be free once the holder drops it");
    }

    #[test]
    fn a_lock_left_by_a_dead_process_is_reclaimed() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        // A pid that is certainly not running: pid 0 is never a user process.
        std::fs::write(dir.lock_path(), "0").unwrap();

        InstanceLock::acquire(&dir).expect("a lock naming a dead process must be reclaimed");
    }

    #[test]
    fn the_lock_records_the_holder_pid() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let _lock = InstanceLock::acquire(&dir).unwrap();
        let text = std::fs::read_to_string(dir.lock_path()).unwrap();
        assert_eq!(text.trim(), std::process::id().to_string());
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features dir`
Expected: FAIL — the module does not exist.

- [ ] **Step 4: Implement the directory and the lock**

`crates/sapphire-framework-bridge/src/dir.rs`:

```rust
//! The bridge's directory: what it holds, and the guard that keeps it to one process.

use std::path::{Path, PathBuf};

use grain_id::GrainId;

use crate::error::{Error, Result};

/// Overrides the bridge directory outright.
pub const BRIDGE_DIR_ENV: &str = "SAPPHIRE_BRIDGE_DIR";

/// On-disk format version of the bridge directory.
pub const BRIDGE_FORMAT_VERSION: u32 = 1;

/// The bridge's directory.
///
/// Framework-wide: no app name and no kind, because one host is one device. It sits outside
/// the per-app layout deliberately.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeDir {
    /// The directory itself.
    pub root: PathBuf,
}

impl BridgeDir {
    /// The default directory, created if absent.
    pub fn open() -> Result<BridgeDir> {
        let root = match std::env::var_os(BRIDGE_DIR_ENV).filter(|v| !v.is_empty()) {
            Some(v) => PathBuf::from(v),
            None => dirs::data_dir()
                .unwrap_or_else(std::env::temp_dir)
                .join("sapphire")
                .join("bridge"),
        };
        BridgeDir::at(root)
    }

    /// A specific directory, created if absent.
    pub fn at(root: PathBuf) -> Result<BridgeDir> {
        let dir = BridgeDir { root };
        dir.prepare()?;
        Ok(dir)
    }

    fn prepare(&self) -> Result<()> {
        private_dir(&self.root)?;
        private_dir(&self.log_dir())?;
        private_dir(&self.workgroups_dir())?;

        let format = self.root.join("format");
        match std::fs::read_to_string(&format) {
            Ok(text) => {
                let found: u32 = text.trim().parse().map_err(|_| {
                    Error::Format(format!("{}: {:?} is not a version", format.display(), text.trim()))
                })?;
                if found > BRIDGE_FORMAT_VERSION {
                    // A newer build has been here. Running against a format we do not
                    // understand could corrupt it; leave it to the newer bridge.
                    return Err(Error::Format(format!(
                        "the bridge directory is format {found}, which is newer than this \
                         build understands ({BRIDGE_FORMAT_VERSION}); upgrade sapphire-bridge"
                    )));
                }
                if found < BRIDGE_FORMAT_VERSION {
                    // No older format exists yet. When one does, migrate here — idempotently
                    // — before writing the new stamp.
                    std::fs::write(&format, BRIDGE_FORMAT_VERSION.to_string())?;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::write(&format, BRIDGE_FORMAT_VERSION.to_string())?;
            }
            Err(e) => return Err(Error::Io(e)),
        }
        Ok(())
    }

    /// The iroh secret key, from which this device's node id follows.
    pub fn node_key(&self) -> PathBuf {
        self.root.join("node.key")
    }

    /// The single-instance guard.
    pub fn lock_path(&self) -> PathBuf {
        self.root.join("bridge.lock")
    }

    /// Host-local network configuration.
    pub fn net_toml(&self) -> PathBuf {
        self.root.join("net.toml")
    }

    /// The routing table.
    pub fn routes_toml(&self) -> PathBuf {
        self.root.join("routes.toml")
    }

    /// Runtime state, rewritten as it changes.
    pub fn status_json(&self) -> PathBuf {
        self.root.join("status.json")
    }

    /// Where the bridge's log goes.
    pub fn log_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    /// One directory per workgroup this host belongs to.
    pub fn workgroups_dir(&self) -> PathBuf {
        self.root.join("workgroups")
    }

    /// One workgroup's directory.
    pub fn workgroup_dir(&self, id: GrainId) -> PathBuf {
        self.workgroups_dir().join(id.to_string())
    }

    /// A workgroup's device ledger directory, as `sapphire-framework-registry` wants it.
    pub fn devices_dir(&self, id: GrainId) -> PathBuf {
        self.workgroup_dir(id).join("root").join("devices")
    }
}

fn private_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)?.permissions();
        if perms.mode() & 0o777 != 0o700 {
            perms.set_mode(0o700);
            std::fs::set_permissions(path, perms)?;
        }
    }
    Ok(())
}

/// Guarantees there is only one bridge for this directory.
///
/// Not an election. A process that cannot take this connects to the running bridge instead;
/// there is no handoff and no follower role.
#[derive(Debug)]
pub struct InstanceLock {
    path: PathBuf,
}

impl InstanceLock {
    /// Take the lock, or report who holds it.
    pub fn acquire(dir: &BridgeDir) -> Result<InstanceLock> {
        let path = dir.lock_path();
        loop {
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    use std::io::Write;
                    write!(file, "{}", std::process::id())?;
                    return Ok(InstanceLock { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let holder = std::fs::read_to_string(&path).unwrap_or_default();
                    let pid: u32 = holder.trim().parse().unwrap_or(0);
                    if pid != 0 && process_is_alive(pid) {
                        return Err(Error::AlreadyRunning(pid));
                    }
                    // Nobody is behind it: a crash or a reboot left it. Clear and retry.
                    tracing::debug!(path = %path.display(), "clearing an abandoned bridge lock");
                    std::fs::remove_file(&path)?;
                }
                Err(e) => return Err(Error::Io(e)),
            }
        }
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 only tests for the process; it has no other effect.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    // SAFETY: the handle is closed on every path.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut code = 0u32;
        let ok = GetExitCodeProcess(handle, &raw mut code) != 0;
        CloseHandle(handle);
        ok && code == STILL_ACTIVE as u32
    }
}
```

The platform `process_is_alive` needs `libc` on Unix and `windows-sys` on Windows; add them to
the manifest the same way `sapphire-framework-ipc` does.

Write `error.rs` with the variants listed in the Interfaces block, `AlreadyRunning(u32)`
rendering as `another bridge is already running (pid {0})`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge --all-features dir`
Expected: PASS, 8 tests (7 on Windows).

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge Cargo.toml Cargo.lock crates/sapphire-framework
git commit -m "feat(bridge): lay out the bridge directory and keep it to one process"
```

---

### Task 3: The routing table

**Files:**
- Create: `crates/sapphire-framework-bridge/src/routes.rs`
- Modify: `crates/sapphire-framework-bridge/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `routes.rs`

**Interfaces:**
- Produces:
  - `Route { workspace_id: GrainId, app_name: String, root: PathBuf, exe_path: PathBuf, managed_by: ManagedBy }`
  - `RouteTable::load(path: &Path) -> Result<RouteTable>`,
    `replace_app(&mut self, app_name: &str, exe_path, managed_by, workspaces: &[WorkspaceRegistration]) -> Result<()>`,
    `remove(&mut self, workspace_id: GrainId) -> Result<bool>`,
    `get(&self, workspace_id: GrainId) -> Option<&Route>`,
    `entries(&self) -> &[Route]`

`replace_app` replaces every route of one application at once, because that is what a
registration is: the app server's complete current list. Nothing else touches another app's
rows.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_bridge_api::WorkspaceRegistration;

    fn reg(root: &str) -> WorkspaceRegistration {
        WorkspaceRegistration { workspace_id: GrainId::random(), root: root.into() }
    }

    fn table(dir: &std::path::Path) -> RouteTable {
        RouteTable::load(&dir.join("routes.toml")).unwrap()
    }

    #[test]
    fn a_missing_file_is_an_empty_table() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(table(tmp.path()).entries().is_empty());
    }

    #[test]
    fn registered_routes_survive_a_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let a = reg("/home/me/journal");
        t.replace_app("sapphire-journal", "/usr/bin/j".into(), ManagedBy::Spawned, &[a.clone()])
            .unwrap();

        let reloaded = table(tmp.path());
        let route = reloaded.get(a.workspace_id).expect("the route");
        assert_eq!(route.app_name, "sapphire-journal");
        assert_eq!(route.root, std::path::PathBuf::from("/home/me/journal"));
        assert_eq!(route.exe_path, std::path::PathBuf::from("/usr/bin/j"));
    }

    #[test]
    fn registering_again_replaces_that_apps_routes_only() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let journal = reg("/j");
        let ledger = reg("/l");
        t.replace_app("sapphire-journal", "/usr/bin/j".into(), ManagedBy::Spawned, &[journal.clone()])
            .unwrap();
        t.replace_app("sapphire-ledger", "/usr/bin/l".into(), ManagedBy::Spawned, &[ledger.clone()])
            .unwrap();

        // The journal server restarts with a different set.
        let journal2 = reg("/j2");
        t.replace_app("sapphire-journal", "/usr/bin/j".into(), ManagedBy::Spawned, &[journal2.clone()])
            .unwrap();

        assert!(t.get(journal.workspace_id).is_none(), "the old journal route must go");
        assert!(t.get(journal2.workspace_id).is_some());
        assert!(t.get(ledger.workspace_id).is_some(), "the ledger route must not be touched");
    }

    #[test]
    fn removing_a_route_reports_whether_it_was_there() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let a = reg("/a");
        t.replace_app("app", "/usr/bin/app".into(), ManagedBy::Spawned, &[a.clone()]).unwrap();

        assert!(t.remove(a.workspace_id).unwrap());
        assert!(!t.remove(a.workspace_id).unwrap());
        assert!(table(tmp.path()).entries().is_empty());
    }

    #[test]
    fn two_apps_cannot_own_the_same_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let shared = reg("/s");
        t.replace_app("sapphire-journal", "/usr/bin/j".into(), ManagedBy::Spawned, &[shared.clone()])
            .unwrap();

        let err = t
            .replace_app("sapphire-ledger", "/usr/bin/l".into(), ManagedBy::Spawned, &[shared])
            .unwrap_err();
        assert!(err.to_string().contains("sapphire-journal"), "{err}");
    }

    #[test]
    fn entries_are_ordered_so_listings_are_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        t.replace_app("b-app", "/b".into(), ManagedBy::Spawned, &[reg("/b1"), reg("/b2")]).unwrap();
        t.replace_app("a-app", "/a".into(), ManagedBy::Spawned, &[reg("/a1")]).unwrap();

        let names: Vec<&str> = t.entries().iter().map(|r| r.app_name.as_str()).collect();
        assert_eq!(names, vec!["a-app", "b-app", "b-app"]);
    }
}
```

`two_apps_cannot_own_the_same_workspace` states spec §1's rule that every piece of state has
one writing process: two app servers owning one workspace would be two writers of its files.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features routes`
Expected: FAIL — `RouteTable` does not exist.

- [ ] **Step 3: Implement the routing table**

`crates/sapphire-framework-bridge/src/routes.rs`:

```rust
//! Which app server owns which workspace on this host.
//!
//! Persisted so the bridge can answer "that workspace lives here, but its server is not
//! running" and can start the owner when a peer asks for it.

use std::path::{Path, PathBuf};

use grain_id::GrainId;
use sapphire_bridge_api::{ManagedBy, WorkspaceRegistration};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// One workspace and the application that owns it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Route {
    /// The workspace's sync identity.
    pub workspace_id: GrainId,
    /// The owning application.
    pub app_name: String,
    /// Where the workspace lives on this host.
    pub root: PathBuf,
    /// The executable to run when the owner is not connected.
    pub exe_path: PathBuf,
    /// How the owner is started. A `Service` owner is never started by the bridge.
    pub managed_by: ManagedBy,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RawTable {
    #[serde(default)]
    route: Vec<Route>,
}

const HEADER: &str = "\
# Which app server owns which workspace on this host.
#
# Written by sapphire-bridge as app servers register. Editing it by hand does nothing
# useful: the owning server rewrites its own rows the next time it connects.
";

/// The routing table.
#[derive(Debug)]
pub struct RouteTable {
    path: PathBuf,
    routes: Vec<Route>,
}

impl RouteTable {
    /// Read the table. A missing file is an empty table and is not created.
    pub fn load(path: &Path) -> Result<RouteTable> {
        let routes = match std::fs::read_to_string(path) {
            Ok(text) => {
                let raw: RawTable = toml::from_str(&text)
                    .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
                raw.route
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(Error::Io(e)),
        };
        let mut table = RouteTable { path: path.to_owned(), routes };
        table.sort();
        Ok(table)
    }

    /// Replace every route belonging to `app_name`.
    ///
    /// A registration is the app server's complete current list, so anything it no longer
    /// names is no longer owned by it. Other applications' rows are untouched.
    pub fn replace_app(
        &mut self,
        app_name: &str,
        exe_path: PathBuf,
        managed_by: ManagedBy,
        workspaces: &[WorkspaceRegistration],
    ) -> Result<()> {
        for ws in workspaces {
            if let Some(existing) = self.get(ws.workspace_id)
                && existing.app_name != app_name
            {
                return Err(Error::Config(format!(
                    "workspace {} is already owned by {} on this host",
                    ws.workspace_id, existing.app_name
                )));
            }
        }

        let mut next: Vec<Route> =
            self.routes.iter().filter(|r| r.app_name != app_name).cloned().collect();
        next.extend(workspaces.iter().map(|ws| Route {
            workspace_id: ws.workspace_id,
            app_name: app_name.to_owned(),
            root: ws.root.clone(),
            exe_path: exe_path.clone(),
            managed_by,
        }));
        self.save(next)
    }

    /// Forget one workspace. `false` if it was not there.
    pub fn remove(&mut self, workspace_id: GrainId) -> Result<bool> {
        if self.get(workspace_id).is_none() {
            return Ok(false);
        }
        let next: Vec<Route> =
            self.routes.iter().filter(|r| r.workspace_id != workspace_id).cloned().collect();
        self.save(next)?;
        Ok(true)
    }

    /// The route for one workspace.
    pub fn get(&self, workspace_id: GrainId) -> Option<&Route> {
        self.routes.iter().find(|r| r.workspace_id == workspace_id)
    }

    /// Every route, ordered by application then workspace.
    pub fn entries(&self) -> &[Route] {
        &self.routes
    }

    fn save(&mut self, mut routes: Vec<Route>) -> Result<()> {
        routes.sort_by(|a, b| {
            a.app_name.cmp(&b.app_name).then_with(|| a.workspace_id.cmp(&b.workspace_id))
        });
        let body = toml::to_string_pretty(&RawTable { route: routes.clone() })
            .map_err(|e| Error::Config(e.to_string()))?;
        write_atomic(&self.path, HEADER, &body)?;
        self.routes = routes;
        Ok(())
    }

    fn sort(&mut self) {
        self.routes.sort_by(|a, b| {
            a.app_name.cmp(&b.app_name).then_with(|| a.workspace_id.cmp(&b.workspace_id))
        });
    }
}

/// Write through a temporary file so a crash never leaves a half-written table.
fn write_atomic(path: &Path, header: &str, body: &str) -> Result<()> {
    use std::io::Write;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        "{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("routes.toml"),
        std::process::id()
    ));
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(header.as_bytes())?;
        file.write_all(b"\n")?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge --all-features routes`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge
git commit -m "feat(bridge): track which app server owns which workspace"
```

---

### Task 4: Workgroup and authorization

**Files:**
- Create: `crates/sapphire-framework-bridge/src/workgroup.rs`
- Create: `crates/sapphire-framework-bridge/src/net.rs`
- Modify: `crates/sapphire-framework-bridge/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `workgroup.rs`

**Interfaces:**
- Produces:
  - `Workgroup { id: GrainId, name: String, dir: PathBuf }`:
    `create(dir: &BridgeDir, name: &str, this_device: &str, node_id: &str) -> Result<Workgroup>`,
    `open(dir: &BridgeDir) -> Result<Option<Workgroup>>`,
    `devices(&self) -> Result<Devices>`,
    `this_device(&self) -> Result<Device>`,
    `authorize(&self, node_id: &str) -> Result<Device>`
  - `NetConfig { wake_on_sync: bool, relays: Vec<String>, discovery: bool }` with
    `load(path) -> Result<NetConfig>` and a `Default` of `wake_on_sync: true`,
    `discovery: true`, no relays

`workgroup join`, invites and pairing are **step 8**. `create` exists here because
authorization needs a workgroup and a device ledger, and without one nothing in this plan can
be tested.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::BridgeDir;

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn bridge_dir() -> (tempfile::TempDir, BridgeDir) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        (tmp, dir)
    }

    #[test]
    fn creating_a_workgroup_writes_this_devices_record() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();

        let me = wg.this_device().unwrap();
        assert_eq!(me.name, "laptop");
        assert_eq!(me.node_id.as_deref(), Some(NODE_A));
    }

    #[test]
    fn a_created_workgroup_reopens() {
        let (_tmp, dir) = bridge_dir();
        let created = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let reopened = Workgroup::open(&dir).unwrap().expect("a workgroup");
        assert_eq!(reopened.id, created.id);
        assert_eq!(reopened.name, "home");
    }

    #[test]
    fn a_host_without_a_workgroup_reports_none() {
        let (_tmp, dir) = bridge_dir();
        assert!(Workgroup::open(&dir).unwrap().is_none());
    }

    #[test]
    fn a_second_workgroup_is_refused_for_now() {
        let (_tmp, dir) = bridge_dir();
        Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let err = Workgroup::create(&dir, "work", "laptop", NODE_A).unwrap_err();
        assert!(err.to_string().contains("already"), "{err}");
    }

    #[test]
    fn a_known_node_is_authorized() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let mut devices = wg.devices().unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();

        let device = wg.authorize(NODE_B).unwrap();
        assert_eq!(device.name, "phone");
    }

    #[test]
    fn an_unknown_node_is_refused() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let err = wg.authorize(NODE_B).unwrap_err();
        assert!(err.to_string().contains("not a device of this workgroup"), "{err}");
    }

    #[test]
    fn a_retired_node_is_refused_and_says_so() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let mut devices = wg.devices().unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();
        devices.retire("phone").unwrap();

        let err = wg.authorize(NODE_B).unwrap_err();
        assert!(err.to_string().contains("retired"), "{err}");
    }

    #[test]
    fn authorization_rereads_the_ledger_each_time() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let mut devices = wg.devices().unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();
        assert!(wg.authorize(NODE_B).is_ok());

        // Retirement arrives from another device while the bridge is running.
        let mut fresh = wg.devices().unwrap();
        fresh.retire("phone").unwrap();

        assert!(
            wg.authorize(NODE_B).is_err(),
            "a revocation must take effect without restarting the bridge"
        );
    }

    #[test]
    fn net_configuration_defaults_to_waking_owners() {
        let (_tmp, dir) = bridge_dir();
        let net = NetConfig::load(&dir.net_toml()).unwrap();
        assert!(net.wake_on_sync);
        assert!(net.discovery);
        assert!(net.relays.is_empty());
    }
}
```

`authorization_rereads_the_ledger_each_time` is the important one: revocation is a change to
the workgroup that replicates, and a bridge that cached the ledger at startup would keep
talking to a retired device until it restarted.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features workgroup`
Expected: FAIL — `Workgroup` does not exist.

- [ ] **Step 3: Implement the workgroup**

`crates/sapphire-framework-bridge/src/workgroup.rs`:

```rust
//! The workgroup this host belongs to, and who is allowed to connect.

use std::path::PathBuf;

use grain_id::GrainId;
use sapphire_registry::{Device, Devices};
use serde::{Deserialize, Serialize};

use crate::dir::BridgeDir;
use crate::error::{Error, Result};

#[derive(Debug, Deserialize, Serialize)]
struct WorkgroupFile {
    name: String,
}

/// A set of devices that share workspaces.
#[derive(Clone, Debug)]
pub struct Workgroup {
    /// Its id.
    pub id: GrainId,
    /// Its name.
    pub name: String,
    /// `<bridge dir>/workgroups/<id>/`.
    pub dir: PathBuf,
    devices_dir: PathBuf,
}

impl Workgroup {
    /// Create a workgroup and write this device's own record into it.
    ///
    /// The founding device needs a record before its first sync, because `Entry.author` is
    /// its device id.
    ///
    /// The first release allows one workgroup per host; the layout and the wire format
    /// support several, so lifting the limit is a CLI change.
    pub fn create(
        dir: &BridgeDir,
        name: &str,
        this_device: &str,
        node_id: &str,
    ) -> Result<Workgroup> {
        if Workgroup::open(dir)?.is_some() {
            return Err(Error::Config(
                "this host already belongs to a workgroup".to_owned(),
            ));
        }
        let id = GrainId::random();
        let wg_dir = dir.workgroup_dir(id);
        std::fs::create_dir_all(wg_dir.join("root"))?;
        std::fs::create_dir_all(dir.devices_dir(id))?;
        std::fs::write(
            wg_dir.join("root").join("workgroup.toml"),
            toml::to_string_pretty(&WorkgroupFile { name: name.to_owned() })
                .map_err(|e| Error::Config(e.to_string()))?,
        )?;

        let workgroup = Workgroup {
            id,
            name: name.to_owned(),
            dir: wg_dir,
            devices_dir: dir.devices_dir(id),
        };
        let mut devices = workgroup.devices()?;
        devices.add(this_device, Some(node_id.to_owned()), None)?;
        Ok(workgroup)
    }

    /// The workgroup this host belongs to, if any.
    pub fn open(dir: &BridgeDir) -> Result<Option<Workgroup>> {
        let entries = match std::fs::read_dir(dir.workgroups_dir()) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(id) = name.to_str().and_then(|s| s.parse::<GrainId>().ok()) else {
                continue;
            };
            let file = entry.path().join("root").join("workgroup.toml");
            let text = std::fs::read_to_string(&file)?;
            let parsed: WorkgroupFile = toml::from_str(&text)
                .map_err(|e| Error::Config(format!("{}: {e}", file.display())))?;
            return Ok(Some(Workgroup {
                id,
                name: parsed.name,
                dir: entry.path(),
                devices_dir: dir.devices_dir(id),
            }));
        }
        Ok(None)
    }

    /// The device ledger, read fresh.
    ///
    /// Always reads from disk: the ledger is synced, so a pairing or a retirement that
    /// arrived from another device must take effect without restarting the bridge.
    pub fn devices(&self) -> Result<Devices> {
        Ok(Devices::open(&self.devices_dir)?)
    }

    /// This host's own record.
    pub fn this_device(&self) -> Result<Device> {
        let devices = self.devices()?;
        // The founding record is the only one written before any node id but ours exists,
        // so identify ourselves by node id through the caller instead where possible; here
        // the first record is ours by construction of `create`.
        devices
            .entries()
            .first()
            .cloned()
            .ok_or_else(|| Error::Config("this workgroup has no devices".to_owned()))
    }

    /// The device behind `node_id`, if it may connect.
    pub fn authorize(&self, node_id: &str) -> Result<Device> {
        let devices = self.devices()?;
        let Some(device) = devices.by_node_id(node_id) else {
            return Err(Error::Unauthorized(format!(
                "{node_id} is not a device of this workgroup"
            )));
        };
        if device.is_retired() {
            return Err(Error::Unauthorized(format!(
                "the device {} ({node_id}) is retired",
                device.name
            )));
        }
        Ok(device.clone())
    }
}
```

> `this_device` taking the first record is a placeholder that works only while `create` is the
> only way a host gets a workgroup. Step 8 adds `join`, at which point this must find the
> record whose `node_id` matches this host's. Leave a `// TODO(step 8)` saying exactly that,
> and do not let it grow other callers in the meantime.

`crates/sapphire-framework-bridge/src/net.rs`:

```rust
//! Host-local network configuration (`net.toml`).

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// What this host does on the network.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct NetConfig {
    /// Start a stopped app server when a peer asks for one of its workspaces.
    pub wake_on_sync: bool,
    /// Use discovery services to find peers.
    pub discovery: bool,
    /// Relay URLs to use in addition to the defaults.
    pub relays: Vec<String>,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig { wake_on_sync: true, discovery: true, relays: Vec::new() }
    }
}

impl NetConfig {
    /// Read the file, or the defaults if it is not there.
    pub fn load(path: &Path) -> Result<NetConfig> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                toml::from_str(&text).map_err(|e| Error::Config(format!("{}: {e}", path.display())))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(NetConfig::default()),
            Err(e) => Err(Error::Io(e)),
        }
    }
}
```

Add `Error::Unauthorized(String)` to the error enum.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge --all-features workgroup`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge
git commit -m "feat(bridge): create a workgroup and authorize peers against its ledger"
```

---

### Task 5: The peer transport trait, and a loopback that needs no network

**Files:**
- Create: `crates/sapphire-framework-bridge/src/peer.rs`
- Modify: `crates/sapphire-framework-bridge/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `peer.rs`

**Interfaces:**
- Produces:
  - `trait PeerStream: AsyncRead + AsyncWrite + Send + Unpin` with a blanket impl;
    `type BoxedStream = Box<dyn PeerStream>`
  - `StreamRequest { workspace_id: GrainId, node_id: String }` — the first thing sent on a
    peer stream, so the far side knows what was asked for
  - `#[async_trait] trait PeerTransport: Send + Sync + 'static`:
    `async fn open(&self, node_id: &str, workspace_id: GrainId) -> Result<BoxedStream>`,
    `async fn accept(&self) -> Result<(String, GrainId, BoxedStream)>`,
    `fn node_id(&self) -> String`
  - `LoopbackTransport` (feature `test-util`): a switchboard that connects transports created
    from the same `LoopbackNetwork`, so two bridges can talk inside one test process

Making the transport a trait is what lets Tasks 6 and 7 be tested without a network — and it
is the same shape the sync spec asked of the replication core, for the same reason.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_loopback_stream_carries_bytes_both_ways() {
        let net = LoopbackNetwork::new();
        let a = net.transport("node-a");
        let b = net.transport("node-b");
        let ws = GrainId::random();

        let accept = tokio::spawn(async move { b.accept().await });
        let mut opened = a.open("node-b", ws).await.unwrap();

        let (from, asked, mut accepted) = accept.await.unwrap().unwrap();
        assert_eq!(from, "node-a");
        assert_eq!(asked, ws);

        opened.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        accepted.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        accepted.write_all(b"world").await.unwrap();
        let mut back = [0u8; 5];
        opened.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"world");
    }

    #[tokio::test]
    async fn opening_to_an_unknown_node_fails() {
        let net = LoopbackNetwork::new();
        let a = net.transport("node-a");
        let err = a.open("node-nowhere", GrainId::random()).await.unwrap_err();
        assert!(err.to_string().contains("node-nowhere"), "{err}");
    }

    #[tokio::test]
    async fn a_transport_reports_its_own_node_id() {
        let net = LoopbackNetwork::new();
        assert_eq!(net.transport("node-a").node_id(), "node-a");
    }

    #[tokio::test]
    async fn closing_one_end_shows_as_end_of_file_on_the_other() {
        let net = LoopbackNetwork::new();
        let a = net.transport("node-a");
        let b = net.transport("node-b");

        let accept = tokio::spawn(async move { b.accept().await });
        let opened = a.open("node-b", GrainId::random()).await.unwrap();
        let (_, _, mut accepted) = accept.await.unwrap().unwrap();

        drop(opened);
        let mut buf = [0u8; 1];
        assert_eq!(accepted.read(&mut buf).await.unwrap(), 0);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features peer`
Expected: FAIL — the module does not exist.

- [ ] **Step 3: Implement the trait and the loopback**

`crates/sapphire-framework-bridge/src/peer.rs`:

```rust
//! How the bridge reaches other devices.
//!
//! An interface, not an implementation: the switchboard, the routing and the authorization
//! are all testable against [`LoopbackTransport`], and iroh is one implementation behind the
//! `node` feature.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use grain_id::GrainId;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::error::{Error, Result};

/// A bidirectional byte stream to another device.
pub trait PeerStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> PeerStream for T {}

/// A boxed [`PeerStream`].
pub type BoxedStream = Box<dyn PeerStream>;

/// Reaching other devices.
#[async_trait::async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    /// Open a stream to `node_id` asking for `workspace_id`.
    async fn open(&self, node_id: &str, workspace_id: GrainId) -> Result<BoxedStream>;

    /// Wait for an inbound stream. Returns the caller's node id, what it asked for, and the
    /// stream.
    ///
    /// Authorization is the bridge's, not the transport's: a transport reports who called,
    /// and the bridge decides.
    async fn accept(&self) -> Result<(String, GrainId, BoxedStream)>;

    /// This host's node id.
    fn node_id(&self) -> String;
}

// ── loopback ────────────────────────────────────────────────────────────────

/// Buffer size of each loopback stream, in bytes.
#[cfg(any(test, feature = "test-util"))]
const LOOPBACK_BUFFER: usize = 64 * 1024;

type Inbox = mpsc::UnboundedSender<(String, GrainId, tokio::io::DuplexStream)>;

/// A set of transports that can reach each other, with no network.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Debug, Default)]
pub struct LoopbackNetwork {
    nodes: Arc<Mutex<HashMap<String, Inbox>>>,
}

#[cfg(any(test, feature = "test-util"))]
impl LoopbackNetwork {
    /// An empty network.
    pub fn new() -> LoopbackNetwork {
        LoopbackNetwork::default()
    }

    /// A transport for `node_id`, registered on this network.
    pub fn transport(&self, node_id: &str) -> LoopbackTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        self.nodes.lock().expect("loopback network").insert(node_id.to_owned(), tx);
        LoopbackTransport {
            node_id: node_id.to_owned(),
            nodes: Arc::clone(&self.nodes),
            inbox: tokio::sync::Mutex::new(rx),
        }
    }
}

/// One device's end of a [`LoopbackNetwork`].
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
pub struct LoopbackTransport {
    node_id: String,
    nodes: Arc<Mutex<HashMap<String, Inbox>>>,
    inbox: tokio::sync::Mutex<mpsc::UnboundedReceiver<(String, GrainId, tokio::io::DuplexStream)>>,
}

#[cfg(any(test, feature = "test-util"))]
#[async_trait::async_trait]
impl PeerTransport for LoopbackTransport {
    async fn open(&self, node_id: &str, workspace_id: GrainId) -> Result<BoxedStream> {
        let inbox = {
            let nodes = self.nodes.lock().expect("loopback network");
            nodes.get(node_id).cloned()
        };
        let Some(inbox) = inbox else {
            return Err(Error::Peer(format!("no such node on the loopback network: {node_id}")));
        };
        let (mine, theirs) = tokio::io::duplex(LOOPBACK_BUFFER);
        inbox
            .send((self.node_id.clone(), workspace_id, theirs))
            .map_err(|_| Error::Peer(format!("{node_id} is no longer listening")))?;
        Ok(Box::new(mine))
    }

    async fn accept(&self) -> Result<(String, GrainId, BoxedStream)> {
        let mut inbox = self.inbox.lock().await;
        match inbox.recv().await {
            Some((from, ws, stream)) => Ok((from, ws, Box::new(stream))),
            None => Err(Error::Peer("the loopback network is gone".to_owned())),
        }
    }

    fn node_id(&self) -> String {
        self.node_id.clone()
    }
}
```

Add `async-trait.workspace = true` to the manifest and `Error::Peer(String)` to the error enum.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge --all-features peer`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge Cargo.lock
git commit -m "feat(bridge): abstract the peer transport and add a loopback for tests"
```

---

### Task 6: The switchboard — control plane, data plane, tickets

**Files:**
- Create: `crates/sapphire-framework-bridge/src/{control.rs,data.rs}`
- Modify: `crates/sapphire-framework-bridge/src/lib.rs`
- Test: `crates/sapphire-framework-bridge/tests/switchboard.rs`

**Interfaces:**
- Produces:
  - `Bridge`: `new(dir: BridgeDir, transport: Arc<dyn PeerTransport>, version: &'static str) -> Result<Bridge>`,
    `async run(self) -> Result<()>`, `endpoints(&self) -> (Endpoint, Endpoint)`
  - `BridgeBuilder`-style setters: `control_endpoint`, `data_endpoint`, `net(NetConfig)`
  - internal: `Owners` — which app is connected, and its `PeerHandle` for notifications;
    `Tickets` — pending inbound streams, single use, expiring

**Two rules the tests pin:**
- A ticket is consumed the first time it is presented, and a ticket nobody claims expires
  rather than holding a stream open for ever.
- Bytes are never inspected. The relay copies in both directions until one side closes.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-bridge/tests/switchboard.rs`:

```rust
//! Two bridges on a loopback network, with a stub app server on each.

use std::sync::Arc;

use sapphire_bridge_api::{
    BridgeClient, ManagedBy, RegisterParams, WorkspaceRegistration,
};
use sapphire_framework_bridge::{Bridge, BridgeDir, LoopbackNetwork, Workgroup};
use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

struct Host {
    tmp: tempfile::TempDir,
    control: Endpoint,
    runtime: std::path::PathBuf,
    workgroup_id: grain_id::GrainId,
}

/// Start a bridge on `net` as `node_id`, in its own directories.
async fn start(net: &LoopbackNetwork, node_id: &str, device_name: &str) -> Host {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = tmp.path().join("run");
    std::fs::create_dir_all(&runtime).unwrap();
    let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
    let wg = Workgroup::create(&dir, "test", device_name, node_id).unwrap();

    let control = Endpoint::in_dir("bridge", runtime.clone());
    let data = Endpoint::in_dir("bridge-data", runtime.clone());
    let bridge = Bridge::new(dir, Arc::new(net.transport(node_id)), "0.0.0")
        .unwrap()
        .control_endpoint(control.clone())
        .data_endpoint(data);
    tokio::spawn(async move {
        let _ = bridge.run().await;
    });

    // Wait for the control endpoint to come up.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !sapphire_ipc::probe(&control).await.unwrap_or(false) {
        assert!(std::time::Instant::now() < deadline, "the bridge never started");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    Host { tmp, control, runtime, workgroup_id: wg.id }
}

fn client_info() -> ClientInfo {
    ClientInfo { kind: "test".into(), version: "0.0.0".into(), pid: std::process::id() }
}

async fn connect(host: &Host) -> BridgeClient {
    let (client, _) = sapphire_ipc::ensure_server(
        &host.control,
        "bridge",
        client_info(),
        &SpawnConfig::disabled(),
    )
    .await
    .unwrap();
    BridgeClient::from_client(Arc::new(client), host.runtime.clone())
}

/// Teach each host about the other's device, so authorization passes both ways.
fn introduce(a_dir: &BridgeDir, a_wg: grain_id::GrainId, name: &str, node: &str) {
    let devices_dir = a_dir.devices_dir(a_wg);
    let mut devices = sapphire_registry::Devices::open(&devices_dir).unwrap();
    devices.add(name, Some(node.to_owned()), None).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_reaches_the_owning_app_server_on_the_other_host() {
    let net = LoopbackNetwork::new();
    let a = start(&net, NODE_A, "host-a").await;
    let b = start(&net, NODE_B, "host-b").await;

    let ws = grain_id::GrainId::random();

    // Both app servers register the same workspace on their own host.
    let client_a = connect(&a).await;
    let reg_a = client_a
        .register(RegisterParams {
            app_name: "test-app".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration { workspace_id: ws, root: "/a".into() }],
        })
        .await
        .unwrap();

    let client_b = connect(&b).await;
    let reg_b = client_b
        .register(RegisterParams {
            app_name: "test-app".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration { workspace_id: ws, root: "/b".into() }],
        })
        .await
        .unwrap();

    // Each host learns the other's device.
    introduce(
        &BridgeDir::at(a.tmp.path().join("bridge")).unwrap(),
        a.workgroup_id,
        "host-b",
        NODE_B,
    );
    introduce(
        &BridgeDir::at(b.tmp.path().join("bridge")).unwrap(),
        b.workgroup_id,
        "host-a",
        NODE_A,
    );

    // B waits for an announcement; A opens a stream to B.
    let mut incoming = client_b.incoming();
    let mut from_a = client_a.open_stream(ws, reg_b.device_id).await.unwrap();

    let announced = tokio::time::timeout(std::time::Duration::from_secs(10), incoming.recv())
        .await
        .expect("an announcement")
        .unwrap();
    assert_eq!(announced.workspace_id, ws);
    assert_eq!(announced.peer_device_id, reg_a.device_id);

    let mut on_b = client_b.accept_stream(announced.ticket).await.unwrap();

    from_a.write_all(b"sync me").await.unwrap();
    let mut buf = [0u8; 7];
    on_b.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"sync me");

    on_b.write_all(b"ok").await.unwrap();
    let mut back = [0u8; 2];
    from_a.read_exact(&mut back).await.unwrap();
    assert_eq!(&back, b"ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_works_once() {
    let net = LoopbackNetwork::new();
    let a = start(&net, NODE_A, "host-a").await;
    let b = start(&net, NODE_B, "host-b").await;
    // … register and introduce as above; abbreviated here by calling the helper …
    let (client_a, client_b, ws, device_b) = pair(&a, &b).await;

    let mut incoming = client_b.incoming();
    let _from_a = client_a.open_stream(ws, device_b).await.unwrap();
    let announced = incoming.recv().await.unwrap();

    let _first = client_b.accept_stream(announced.ticket.clone()).await.unwrap();
    let err = client_b.accept_stream(announced.ticket).await.unwrap_err();
    assert!(err.to_string().contains("ticket"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_for_an_unowned_workspace_is_refused() {
    let net = LoopbackNetwork::new();
    let a = start(&net, NODE_A, "host-a").await;
    let b = start(&net, NODE_B, "host-b").await;
    let (client_a, _client_b, _ws, device_b) = pair(&a, &b).await;

    let err = client_a
        .open_stream(grain_id::GrainId::random(), device_b)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("workspace"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_outside_the_workgroup_is_refused_before_any_app_server_hears_of_it() {
    let net = LoopbackNetwork::new();
    let a = start(&net, NODE_A, "host-a").await;
    let b = start(&net, NODE_B, "host-b").await;
    let (client_a, client_b, ws, device_b) = pair(&a, &b).await;

    // B retires A.
    let dir_b = BridgeDir::at(b.tmp.path().join("bridge")).unwrap();
    let mut devices = sapphire_registry::Devices::open(&dir_b.devices_dir(b.workgroup_id)).unwrap();
    devices.retire("host-a").unwrap();

    let mut incoming = client_b.incoming();
    let _ = client_a.open_stream(ws, device_b).await;

    let announced = tokio::time::timeout(std::time::Duration::from_secs(2), incoming.recv()).await;
    assert!(
        announced.is_err(),
        "a retired device's stream must not reach an app server"
    );
}

/// Register the same workspace on both hosts and introduce their devices.
///
/// Returns both clients, the workspace, and B's device id.
async fn pair(
    a: &Host,
    b: &Host,
) -> (BridgeClient, BridgeClient, grain_id::GrainId, grain_id::GrainId) {
    let ws = grain_id::GrainId::random();
    let client_a = connect(a).await;
    client_a
        .register(RegisterParams {
            app_name: "test-app".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration { workspace_id: ws, root: "/a".into() }],
        })
        .await
        .unwrap();
    let client_b = connect(b).await;
    let reg_b = client_b
        .register(RegisterParams {
            app_name: "test-app".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration { workspace_id: ws, root: "/b".into() }],
        })
        .await
        .unwrap();

    introduce(
        &BridgeDir::at(a.tmp.path().join("bridge")).unwrap(),
        a.workgroup_id,
        "host-b",
        NODE_B,
    );
    introduce(
        &BridgeDir::at(b.tmp.path().join("bridge")).unwrap(),
        b.workgroup_id,
        "host-a",
        NODE_A,
    );
    (client_a, client_b, ws, reg_b.device_id)
}
```

Put `Host`, `start`, `connect`, `introduce` and `pair` in `tests/common/mod.rs` from the start,
and `mod common;` from both `switchboard.rs` and `wake.rs` — Task 7 needs the same fixtures,
and copying them is how the two drift apart.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features --test switchboard`
Expected: FAIL — `Bridge` does not exist.

- [ ] **Step 3: Implement the control plane**

`crates/sapphire-framework-bridge/src/control.rs` holds `Owners` (app name → `PeerHandle` and
its routes) and builds the `Router` with the four methods. Key points, in order:

```rust
/// Which app servers are connected right now.
#[derive(Debug, Default)]
pub(crate) struct Owners {
    by_app: Mutex<HashMap<String, PeerHandle>>,
}

impl Owners {
    pub(crate) fn connect(&self, app_name: &str, peer: PeerHandle) { … }
    pub(crate) fn peer(&self, app_name: &str) -> Option<PeerHandle> { … }
    pub(crate) fn is_online(&self, app_name: &str) -> bool { … }
    /// Drop an app's registration when its control connection closes.
    pub(crate) fn disconnect(&self, app_name: &str) { … }
}
```

`bridge.register` does, in this order:

1. parse the parameters;
2. `RouteTable::replace_app(...)` — which rejects a workspace another app already owns;
3. record the caller in `Owners`, keyed by `app_name`, with `ctx.peer.clone()`;
4. answer with this host's `device_id`, `node_id` and `workgroup_id`, failing with
   `Error::NoWorkgroup` if the host has not joined one.

`bridge.unregister` removes one route. `bridge.peers` reads the ledger fresh and marks each
device `connected` from the transport's live set. `bridge.status` assembles `StatusResult`,
with `owner_online` from `Owners`.

**A registration lasts as long as the control connection.** When it closes, the app is no
longer online — but its **routes stay in `routes.toml`**, because that is how the bridge knows
where to find a workspace whose server is merely stopped.

- [ ] **Step 4: Implement the data plane**

`crates/sapphire-framework-bridge/src/data.rs`:

```rust
//! The data plane: one header line, then bytes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use grain_id::GrainId;
use sapphire_bridge_api::{DataAck, DataHeader};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::error::{Error, Result};
use crate::peer::BoxedStream;

/// How long an unclaimed inbound stream is held.
///
/// Long enough for a stopped app server to start (`wake_on_sync`), short enough that a peer
/// that vanishes does not pin a stream for ever.
pub(crate) const TICKET_TTL: Duration = Duration::from_secs(60);

struct Pending {
    stream: BoxedStream,
    created: Instant,
}

/// Inbound streams waiting for their owner to claim them.
#[derive(Default)]
pub(crate) struct Tickets {
    pending: Mutex<HashMap<String, Pending>>,
}

impl Tickets {
    /// Park a stream and return its single-use ticket.
    pub(crate) fn park(&self, stream: BoxedStream) -> String {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).expect("the system random source");
        let ticket = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let mut pending = self.pending.lock().expect("tickets");
        pending.retain(|_, p| p.created.elapsed() < TICKET_TTL);
        pending.insert(ticket.clone(), Pending { stream, created: Instant::now() });
        ticket
    }

    /// Take a stream. A ticket works once; presenting it again finds nothing.
    pub(crate) fn claim(&self, ticket: &str) -> Option<BoxedStream> {
        let mut pending = self.pending.lock().expect("tickets");
        pending.retain(|_, p| p.created.elapsed() < TICKET_TTL);
        pending.remove(ticket).map(|p| p.stream)
    }
}

/// Read the header line, without consuming any of the bytes that follow it.
pub(crate) async fn read_header<S>(stream: &mut S) -> Result<DataHeader>
where
    S: tokio::io::AsyncRead + Unpin,
{
    // Capacity 1: a larger buffer would read past the newline and swallow payload bytes.
    let mut reader = BufReader::with_capacity(1, stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    serde_json::from_str(line.trim())
        .map_err(|e| Error::Protocol(format!("bad data-plane header: {e}")))
}

/// Answer a header.
pub(crate) async fn write_ack<S>(stream: &mut S, ack: DataAck) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let mut line = serde_json::to_vec(&ack).map_err(|e| Error::Protocol(e.to_string()))?;
    line.push(b'\n');
    stream.write_all(&line).await?;
    stream.flush().await?;
    Ok(())
}

/// Copy bytes in both directions until either side closes.
///
/// The bridge never looks at what it is copying: the replication protocol runs end to end
/// between two app servers.
pub(crate) async fn splice<A, B>(mut a: A, mut b: B)
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Err(err) = tokio::io::copy_bidirectional(&mut a, &mut b).await {
        tracing::debug!("a relayed stream ended: {err}");
    }
}
```

The bridge's data listener then:

- reads the header;
- **`Open`** — look the workspace up in `RouteTable`; refuse if it is not registered here;
  resolve `device_id` to a node id through the ledger; `transport.open(...)`; send
  `DataAck { ok: true }`; `splice`;
- **`Accept`** — `tickets.claim(...)`; refuse an unknown or expired ticket with
  `"no such ticket, or it has already been used"`; send the acknowledgement; `splice`.

And the inbound loop:

```
loop {
    let (peer_node_id, workspace_id, stream) = transport.accept().await?;
    // 1. Authorize before anything else knows a stranger called.
    let Ok(device) = workgroup.authorize(&peer_node_id) else { drop(stream); continue };
    // 2. Whose workspace is it?
    let Some(route) = routes.get(workspace_id) else { drop(stream); continue };
    // 3. Park it, announce it.
    let ticket = tickets.park(stream);
    match owners.peer(&route.app_name) {
        Some(peer) => peer.notify(INCOMING, …).await,
        // 4. Nobody home: wake_on_sync (Task 7 of this plan wires it; here, log and drop).
        None => tracing::info!(app = %route.app_name, "the owner is not connected"),
    }
}
```

Step 1 comes before step 2 deliberately: an unauthorized peer must not be able to learn which
workspaces this host holds by watching which requests are answered differently.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge --all-features --test switchboard`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge
git commit -m "feat(bridge): splice peer streams to the app server that owns the workspace"
```

---

### Task 7: `wake_on_sync`

**Files:**
- Modify: `crates/sapphire-framework-bridge/src/{control.rs,data.rs,lib.rs}`
- Test: `crates/sapphire-framework-bridge/tests/wake.rs`

**Interfaces:**
- Produces: `Bridge` starts a stopped owner from `routes.toml`'s `exe_path` when a peer asks
  for one of its workspaces and `net.wake_on_sync` is on

**The exception that must not be forgotten:** a route whose `managed_by` is `Service` is
**never** started. That covers a privilege-separated server (spec §3), which runs as root and
which the bridge — running as the human user — cannot start any more than a CLI can. The
workspace is reported offline instead.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-bridge/tests/wake.rs`:

```rust
//! Starting a stopped owner when a peer asks for its workspace.

use std::sync::Arc;

use sapphire_bridge_api::{ManagedBy, RegisterParams, WorkspaceRegistration};
use sapphire_framework_bridge::{Bridge, BridgeDir, LoopbackNetwork, NetConfig, Workgroup};

// … `start`, `connect`, `introduce` as in switchboard.rs; factor them into
// `tests/common/mod.rs` and use them from both files.

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_spawned_owner_is_started() {
    let marker = tempfile::tempdir().unwrap();
    let evidence = marker.path().join("started");

    // The "app server" is a shell that touches a file and exits.
    let exe = "/bin/sh";
    let args_marker = evidence.display().to_string();

    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start_with_exe(
        &net,
        common::NODE_B,
        "host-b",
        exe,
        &["-c", &format!("touch {args_marker}")],
        ManagedBy::Spawned,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;

    // B's app server disconnects; its routes stay.
    common::disconnect_owner(&b).await;

    let _ = client_a.open_stream(ws, device_b).await;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !evidence.exists() {
        assert!(std::time::Instant::now() < deadline, "the owner was never started");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_managed_owner_is_never_started() {
    let marker = tempfile::tempdir().unwrap();
    let evidence = marker.path().join("started");

    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start_with_exe(
        &net,
        common::NODE_B,
        "host-b",
        "/bin/sh",
        &["-c", &format!("touch {}", evidence.display())],
        ManagedBy::Service,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;
    common::disconnect_owner(&b).await;

    let _ = client_a.open_stream(ws, device_b).await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    assert!(
        !evidence.exists(),
        "a service-managed owner must be left to its service manager"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn waking_can_be_turned_off() {
    let marker = tempfile::tempdir().unwrap();
    let evidence = marker.path().join("started");

    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start_with_net(
        &net,
        common::NODE_B,
        "host-b",
        NetConfig { wake_on_sync: false, ..NetConfig::default() },
        "/bin/sh",
        &["-c", &format!("touch {}", evidence.display())],
        ManagedBy::Spawned,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;
    common::disconnect_owner(&b).await;

    let _ = client_a.open_stream(ws, device_b).await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(!evidence.exists());
}
```

These are Unix-shaped (`/bin/sh`). Gate the file with `#![cfg(unix)]` and note that the
Windows path is covered by the switchboard tests, which do not spawn anything.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features --test wake`
Expected: FAIL — nothing starts the owner.

- [ ] **Step 3: Implement waking**

In the inbound loop, where Task 6 logged "the owner is not connected":

```rust
None if net.wake_on_sync && route.managed_by == ManagedBy::Spawned => {
    // The ticket is already parked, and its TTL is what gives the owner time to start,
    // connect and claim it.
    if let Err(err) = wake(route) {
        tracing::warn!(app = %route.app_name, "could not start the owner: {err}");
    }
}
None => {
    tracing::info!(
        app = %route.app_name,
        managed_by = ?route.managed_by,
        "the owner is not connected; reporting the workspace as offline"
    );
}
```

```rust
/// Start a stopped app server.
///
/// Detached, with no inherited stdio: the bridge is not its parent in any useful sense, and a
/// server that outlives this call is exactly what is wanted.
fn wake(route: &Route) -> std::io::Result<()> {
    let mut command = std::process::Command::new(&route.exe_path);
    command
        .args(["server", "run"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid is async-signal-safe and is the documented way to detach.
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            })
        };
    }
    let child = command.spawn()?;
    // The server outlives us; do not reap it.
    std::mem::forget(child);
    Ok(())
}
```

Note the rate limit this needs and does not yet have: record the last wake per application and
refuse to start the same one more than once every few seconds, so a peer that reconnects in a
loop cannot fork-bomb the host. Add it now, with a test:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_is_not_started_twice_in_quick_succession() { … }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-bridge --all-features --test wake`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge
git commit -m "feat(bridge): start a stopped owner when a peer asks for its workspace"
```

---

### Task 8: The iroh transport

**Files:**
- Create: `crates/sapphire-framework-bridge/src/iroh.rs`
- Modify: `crates/sapphire-framework-bridge/src/lib.rs`
- Test: `crates/sapphire-framework-bridge/tests/iroh_transport.rs`

**Interfaces:**
- Produces (feature `node`):
  - `IrohTransport::new(key_path: &Path, net: &NetConfig) -> Result<IrohTransport>` — loads or
    creates `node.key` at `0600`
  - `impl PeerTransport for IrohTransport`

**Read the API before writing this.** iroh is the one dependency in these plans whose surface
is not pinned by anything in this repository. Check `https://docs.rs/iroh/1.2` for the exact
spelling of `Endpoint::builder`, `SecretKey`, `Endpoint::connect`, `Endpoint::accept` and the
incoming-connection type before implementing, and adjust the sketch below to match rather than
forcing the code into this shape.

The shape the rest of the bridge depends on is only this: open a stream to a node id, accept a
stream and learn the caller's node id. Nothing else about iroh leaks past this file.

- [ ] **Step 1: Write the tests**

`crates/sapphire-framework-bridge/tests/iroh_transport.rs`:

```rust
//! Two iroh endpoints in one process, on localhost, with no relay and no discovery.

#![cfg(feature = "node")]

use grain_id::GrainId;
use sapphire_framework_bridge::{IrohTransport, NetConfig, PeerTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn offline() -> NetConfig {
    NetConfig { wake_on_sync: false, discovery: false, relays: vec![] }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_file_is_created_once_and_reused() {
    let tmp = tempfile::tempdir().unwrap();
    let key = tmp.path().join("node.key");

    let first = IrohTransport::new(&key, &offline()).await.unwrap();
    let id = first.node_id();
    drop(first);

    let second = IrohTransport::new(&key, &offline()).await.unwrap();
    assert_eq!(second.node_id(), id, "the node id must survive a restart");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn the_key_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let key = tmp.path().join("node.key");
    let _ = IrohTransport::new(&key, &offline()).await.unwrap();
    let mode = std::fs::metadata(&key).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "mode was {:o}", mode & 0o777);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_endpoints_exchange_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let a = IrohTransport::new(&tmp.path().join("a.key"), &offline()).await.unwrap();
    let b = IrohTransport::new(&tmp.path().join("b.key"), &offline()).await.unwrap();

    // Teach A where B is, since discovery is off.
    a.add_known_address(&b.node_addr().await.unwrap()).unwrap();

    let ws = GrainId::random();
    let accept = tokio::spawn(async move { b.accept().await });
    let mut opened = a.open(&b_node_id, ws).await.unwrap();

    let (from, asked, mut accepted) = accept.await.unwrap().unwrap();
    assert_eq!(from, a.node_id());
    assert_eq!(asked, ws);

    opened.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    accepted.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}
```

> The last test needs B's node id and address before B is moved into the task. Restructure it
> to capture `let b_node_id = b.node_id(); let b_addr = b.node_addr().await.unwrap();` before
> the `tokio::spawn`, and give `IrohTransport` an `add_known_address` that feeds iroh's address
> book — with discovery off, A has no other way to find B. If iroh 1.2 spells this differently,
> follow its API and keep the test's intent.

- [ ] **Step 2: Implement the transport**

`crates/sapphire-framework-bridge/src/iroh.rs`, in outline — fill in against the real API:

```rust
//! The iroh implementation of [`PeerTransport`](crate::PeerTransport).
//!
//! Everything iroh-shaped lives here. The rest of the bridge knows only "open a stream to a
//! node id" and "accept a stream and learn who called", so replacing this file would not
//! touch anything else.

use std::path::Path;

use grain_id::GrainId;

use crate::error::{Error, Result};
use crate::net::NetConfig;
use crate::peer::{BoxedStream, PeerTransport};

/// This host's endpoint on the network.
pub struct IrohTransport {
    endpoint: iroh::Endpoint,
    node_id: String,
}

impl IrohTransport {
    /// Bind an endpoint, loading or creating the secret key at `key_path`.
    pub async fn new(key_path: &Path, net: &NetConfig) -> Result<IrohTransport> {
        let secret = load_or_create_key(key_path)?;
        let mut builder = iroh::Endpoint::builder()
            .secret_key(secret)
            .alpns(vec![sapphire_bridge_api::ALPN.to_vec()]);
        if net.discovery {
            builder = builder.discovery_n0();
        }
        // Relay URLs from net.toml go here; the spec's §3.8 material applies.
        let endpoint = builder.bind().await.map_err(|e| Error::Peer(e.to_string()))?;
        let node_id = endpoint.node_id().to_string();
        Ok(IrohTransport { endpoint, node_id })
    }
}

/// Read the secret key, or create one at `0600`.
fn load_or_create_key(path: &Path) -> Result<iroh::SecretKey> { … }
```

`open` parses the node id, connects with the ALPN, opens a bidirectional stream, writes the
`StreamRequest` line naming the workspace, and boxes the stream. `accept` waits for a
connection, reads its remote node id, reads the request line, and boxes the stream. The
`(read, write)` halves iroh returns are joined into one duplex with `tokio::io::join`.

**Do not put authorization here.** The transport reports who called; the bridge decides.
Keeping the decision in one place is what makes the retirement test in Task 6 meaningful.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p sapphire-framework-bridge --features node --test iroh_transport`
Expected: PASS, 3 tests.

A real-relay test is out of scope; if you add one, mark it `#[ignore]` and say in its doc
comment that it needs the network.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-bridge Cargo.lock
git commit -m "feat(bridge): reach other devices over iroh"
```

---

### Task 9: The `sapphire-bridge` binary

**Files:**
- Create: `apps/sapphire-bridge/{Cargo.toml,README.md,README.ja.md,src/main.rs}`
- Create: `crates/sapphire-framework-bridge/src/command.rs`
- Modify: `Cargo.toml` (workspace `members`), `CONTRIBUTING.md`, `docs/ARCHITECTURE.md`
- Test: inline `#[cfg(test)] mod tests` in `command.rs`

**Interfaces:**
- Produces:
  - `BridgeCommand::{Run, Status, Device, Workgroup, Workspace}` (`clap::Subcommand`)
  - `DeviceCommand::{List, Forget { selector }}`
  - `WorkgroupCommand::{Create { name, device_name }, List}`
  - `WorkspaceCommand::List` — read-only, per spec §1: the bridge knows what the workgroup
    holds; the owning app's CLI decides what this host keeps
  - `async BridgeCommand::dispatch(self, version: &'static str) -> Result<i32>`

`pair`, `workgroup join` and invites are **step 8**.

- [ ] **Step 1: Write the manifest and the binary**

`apps/sapphire-bridge/Cargo.toml`:

```toml
[package]
name = "sapphire-bridge"
version.workspace = true
edition.workspace = true
description = "Host-wide sapphire daemon: device identity, workgroup authorization and sync routing"
license.workspace = true
repository.workspace = true
keywords = ["sync", "p2p", "daemon", "local-first"]
categories = ["command-line-utilities"]

[dependencies]
sapphire-bridge = { package = "sapphire-framework-bridge", version = "0.14.0", path = "../../crates/sapphire-framework-bridge", features = ["node"] }
clap.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
tracing.workspace = true
tracing-subscriber.workspace = true
```

Root `Cargo.toml`: add `"apps/sapphire-bridge",` to `members`.

`apps/sapphire-bridge/src/main.rs`:

```rust
//! The host-wide sapphire daemon.
//!
//! Running it with no subcommand starts the bridge; every other subcommand is a one-shot
//! command against a running one.

use clap::Parser;
use sapphire_bridge::BridgeCommand;

#[derive(Parser)]
#[command(name = "sapphire-bridge", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<BridgeCommand>,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "sapphire_framework_bridge=info".into()),
    )
    .init();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or(BridgeCommand::Run);
    match command.dispatch(env!("CARGO_PKG_VERSION")).await {
        Ok(code) => std::process::ExitCode::from(code as u8),
        Err(err) => {
            eprintln!("sapphire-bridge: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}
```

- [ ] **Step 2: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(subcommand)]
        command: BridgeCommand,
    }

    #[test]
    fn the_subcommands_parse() {
        for args in [
            vec!["b", "run"],
            vec!["b", "status"],
            vec!["b", "device", "list"],
            vec!["b", "device", "forget", "phone"],
            vec!["b", "workgroup", "create", "home", "--device-name", "laptop"],
            vec!["b", "workgroup", "list"],
            vec!["b", "workspace", "list"],
        ] {
            assert!(Probe::try_parse_from(&args).is_ok(), "{args:?}");
        }
    }

    #[test]
    fn there_is_no_way_to_map_a_workspace_from_here() {
        // Placing a workspace on this host is the owning application's business (spec §1).
        assert!(Probe::try_parse_from(["b", "workspace", "map", "notes", "/tmp/x"]).is_err());
    }

    #[test]
    fn pairing_is_not_here_yet() {
        assert!(Probe::try_parse_from(["b", "pair", "create"]).is_err());
    }

    #[tokio::test]
    async fn status_against_no_bridge_exits_non_zero() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: set before any other thread reads the environment in this test binary.
        unsafe { std::env::set_var("SAPPHIRE_RUNTIME_DIR", tmp.path()) };
        let code = BridgeCommand::Status.dispatch("0.0.0").await.unwrap();
        unsafe { std::env::remove_var("SAPPHIRE_RUNTIME_DIR") };
        assert_eq!(code, 1);
    }
}
```

`there_is_no_way_to_map_a_workspace_from_here` and `pairing_is_not_here_yet` are scope tests:
they fail the moment someone adds a subcommand that belongs somewhere else, which is the
easiest boundary in this design to blur.

- [ ] **Step 3: Implement the commands**

`run` builds a `BridgeDir`, takes the `InstanceLock`, builds an `IrohTransport` when the
`node` feature is on, and runs `Bridge`. Taking the lock fails with
`Error::AlreadyRunning(pid)`, which `run` turns into a message naming the pid and exit code 1
— not an error, because "the bridge is already running" is the normal outcome of starting it
twice.

`status`, `device` and `workspace` connect to the running bridge over the control plane
(`SpawnConfig::disabled()` — asking about a bridge must not start one) and print. `workgroup
create` works directly on the directory, because there is nothing to ask yet.

- [ ] **Step 4: Note the repository convention**

`CONTRIBUTING.md`, in the layout section:

```markdown
- Library crates live in `crates/`; binaries that ship as part of the framework live in
  `apps/` (`apps/sapphire-bridge/`). An application with its own repository stays there.
```

- [ ] **Step 5: Note it in `ARCHITECTURE.md`**

In the crate table:

```markdown
| `sapphire-framework-bridge-api` | bridge 制御プレーンのプロトコルとクライアント（serde のみ・iroh 非依存） | ✅ |
| `sapphire-framework-bridge` | ホスト常駐デーモン本体（デバイス識別・workgroup 認可・交換台・iroh） | ✅ |
| `apps/sapphire-bridge` | 上記のバイナリ | ✅ |
```

- [ ] **Step 6: Run everything and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add apps/sapphire-bridge crates/sapphire-framework-bridge Cargo.toml Cargo.lock CONTRIBUTING.md docs/ARCHITECTURE.md
git commit -m "feat(bridge): ship the sapphire-bridge binary and its CLI"
```

---

## What this plan does not cover

| Spec step | Left for |
|---|---|
| **Replicating the workgroup workspace itself** — spec §1 makes the bridge the app server of one app, the workgroup, using `-sync` like any other owner. Here the device ledger is only read and written locally. | step 8, when a second device first exists to replicate with. `sapphire-framework-sync` is deliberately **not** a dependency of `-bridge` until then: an unused one is noise. |
| `bridge log` and `status.json` | step 9, with the rest of the operational surface. There is no log file to read until then, and a subcommand that prints nothing is worse than one that does not exist. |
| Pairing, invites, `workgroup join`, `pair create/accept` | step 8 |
| `Workgroup::this_device` finding the record by node id rather than taking the first | step 8, with `join` |
| The app server's sync runtime: watcher, `Replica`, `sync.enable` / `disable` / `status` | step 7 |
| Publishing the workgroup's workspace list (`workspaces/<id>.toml`) | step 8 |
| The embedded relay and relay URLs from the workgroup | step 9 |
| `service install` for the bridge | step 10 |
| `status.json` and the shared log file | folded into step 9 with the other operational surface |
| Several workgroups per host | supported by the layout; the CLI limit is lifted when someone needs it |
