# Peer-to-peer workspace sync over iroh

- Date: 2026-09-15 (revised the same day: shared host node, device mesh, single-user)
- Scope: `sapphire-framework` — new crates `sapphire-framework-sync`, `-net`, `-keys`;
  changes to `-registry`, `-backend`, the facade; removal of `-rpc`, `-remote-client`,
  `-remote-server`, `-blob`
- Follow-ups (separate specs, in their own repositories): `sapphire-sync` (new reference
  app, first consumer), then `sapphire-timer`, `sapphire-ledger`, `sapphire-journal`,
  `sapphire-agent`
- Related issues: #83, #86, #87, #90, #92, #103, #104, #117

## Background

The framework's stated goal is local-first applications. Today, though, the only way two
copies of a workspace converge is through a central server: `sapphire-framework-remote-server`
keeps a per-workspace change log with a server-assigned `seq`, clients track a cursor into
it over JSON-RPC-over-HTTP, and conflicts resolve last-writer-wins on the client's wall
clock. Without a server, nothing syncs.

That should not be a requirement. Clients (cli, desktop, later mobile) must be able to sync
with each other directly, while a server stays useful as an always-on, well-connected peer
(and relay). The existing RPC sync is not in production, so migration cost is ignored.

[iroh](https://www.iroh.computer/) (1.0 shipped 2026-06-15, 1.2.0 current) gives us
QUIC connections dialed by public key, with hole punching, relay fallback and discovery,
and a wire-stability commitment. Its higher-level protocols (`iroh-docs`, `iroh-blobs`,
`iroh-gossip`) are still 0.10x.

What already fits a peer-to-peer model:

- **Server and client are already symmetric** (Model B): both hold files as the origin
  and a DB as a cache. A server becomes "a peer that happens to be always on".
- **Only document content travels**; vector indexes stay per-node.
- **Content is addressable by hash.**
- **A device registry exists** (`sapphire-framework-registry`). A device's iroh `NodeId`
  slots into it.

What does not fit, and must be redesigned:

| Today | Peer-to-peer replacement |
|---|---|
| Server-assigned `seq` + client cursor | Per-replica counters + version vectors |
| LWW on client wall clock (`updated_at`) | Hybrid logical clock + replica id tie-break |
| `generation` to detect a recreated log | Unnecessary (state-based delta sync) |
| Server-only fix-ups (journal `dedupe.rs`, ledger `reconcile`) | External-edit detection on every node; app fix-ups as content-deterministic rules inside each app |
| API keys (`keys.toml`) for sync, per app | One host `NodeId` checked against a cross-app device mesh; pairing tickets |
| Users + devices | Devices only (single user) |

## Decisions

Agreed during brainstorming on 2026-09-15:

1. **Serverless scope — relays.** Public n0 relays and discovery are acceptable. When a
   self-hosted server exists, it doubles as a relay so a deployment can avoid depending on
   n0 entirely.
2. **Concurrent edits.** Resolve last-writer-wins, but when two edits were *truly
   concurrent* (detected via version vectors), keep the loser as a conflict copy. Nothing is
   dropped silently. Per-file-type merge (e.g. JSONL line union, #83) is out of scope.
3. **Single user; all devices are equals.** Every device belongs to the same person; any
   device may pair or retire devices. User management is dropped from the framework
   (humans and agents acting through the same device cannot be told apart by a user id
   anyway; an app that wants to mark "written via MCP" does so itself). Changes stay
   attributed to a device through the sync entry.
4. **HTTP/JSON-RPC for sync is removed.** Everything sync-related runs over iroh. The sync
   core is transport-agnostic so another transport can be added later if ever needed.
   API keys survive for non-sync HTTP endpoints (agent's `/mcp`, `/acp`, `/a2a`).
5. **Fix-ups converge by content.** External-edit detection is framework behaviour on every
   node. App-specific fix-ups (journal duplicate ids) run inside the app, decide from file
   content and paths only, and therefore converge wherever they run. Making file moves
   first-class is out of scope.
6. **Approach: iroh as transport + our own replication protocol.** `iroh-docs` was
   rejected: no concurrency detection, a shared namespace secret prevents per-device
   revocation, it keeps content in its own redb (double storage beside the file origin),
   and it is pre-1.0. `iroh-gossip` alone gives no catch-up after being offline.
7. **WASM is dropped** as a project target. Phase 4 of `ARCHITECTURE.md` and #86 steps
   D–F are out of scope.
8. **One node per host, shared by all apps.** Protocols (ALPNs) are common to every app.
   Sync configuration and state live in one framework-wide directory; whichever process
   holds its lock runs the node and syncs every registered workspace of every app; other
   processes only touch files and take over when the lock frees. No handoff protocol, no
   daemon requirement. Where a shared directory is impossible (mobile sandboxes), each app
   keeps its own directory and becomes its own device — same mechanism, different location.
9. **Devices form one cross-app mesh.** A device pairs once and can then host any of the
   mesh's workspaces; each device chooses which workspaces it keeps a copy of.
10. **Any file type is synced, with a size cap.** Content is addressed by hash for all
    files; whole-file transfer only (default cap 64 MiB); chunked/resumable transfer is
    deferred.
11. **A reference app, `sapphire-sync` (Syncthing-like), is built before migrating the
    existing apps.** It is also the way to run a headless node (e.g. on a server) without
    any other app.

## 1. Crate layout

### New

| crate | role | depends on |
|---|---|---|
| `sapphire-framework-sync` | Replication core, **transport-agnostic**: wire types, `ReplicaStore` (redb), merge rules, HLC, conflict copies, declarative filtering, external-edit detection via `TrackStore` | serde, redb, sha2, ignore, `-track` |
| `sapphire-framework-net` | Shared node directory and lock, iroh `Endpoint`, protocol handlers (`sync`, `blob`, `pair`), authorization against the mesh, discovery and relay configuration, `SyncNode` runtime, file watcher, workspace registration API, `NodeCommand` CLI subcommands (feature `cli`), embedded relay (feature `embedded-relay`) | iroh 1.x, `-sync`, `-registry` |
| `sapphire-framework-keys` | `KeyStore` / `KeyEntry` / `protect`, moved out of `-remote-server` (#103). For non-sync HTTP endpoints | serde, toml, axum (feature) |

`-sync` and `-net` must **not** depend on `-workspace` or `-retrieve`. `sapphire-sync`
(which has no search index) verifies this.

### Changed

- **`-registry`**: users are removed (`users.rs`, `Device::user_id`). Devices are stored one
  file per record (`<dir>/<grain-id>.toml`) instead of one `devices.toml` (see §3.5);
  `Device` gains `node_id`. A migration from the single-file format is provided for any
  directory.
- **`-backend`**: `RemoteBackend` is replaced by `SyncedBackend` = `LocalBackend` plus a
  handle to the host node (§4.4). "Local" and "remote" workspaces disappear as distinct
  things: every workspace is local, with zero or more peers. `WorkspaceLocator` becomes
  `Path` only; joining happens through `NodeCommand`.
- **Facade**: features `rpc` / `remote-client` / `remote-server` become `sync` / `net` /
  `keys`.

### Removed

- `sapphire-framework-rpc`, `-remote-client`, `-remote-server`. `WsStore`'s external-edit
  detection (`record_local_write` / `reconcile`) and `is_syncable` move into `-sync` as
  behaviour common to every node.
- `sapphire-framework-blob`: its only user is the server being removed; file content is
  served from the origin (§2.3).
- The search RPC (`search.fts` / `search.semantic`). Every node has its local index; remote
  search can return later if needed.

### Unchanged

`-track`, `-retrieve`, `-workspace`.

## 2. Replication core (`sapphire-framework-sync`)

### 2.1 Types

```rust
/// Assigned once when a replica store is created (UUIDv7). Distinct from the device id:
/// a reinstall gets a fresh replica id, so dots are never reused.
struct ReplicaId(Uuid);

struct Dot { replica: ReplicaId, counter: u64 }

/// replica -> highest counter covered.
struct VersionVector(BTreeMap<ReplicaId, u64>);

struct Hlc { wall_ms: u64, logical: u32 }

enum Content {
    File { hash: [u8; 32] /* SHA-256 */, len: u64 },
    Tombstone,
}

struct Entry {
    path: String,            // workspace-relative, POSIX separators
    content: Content,
    hlc: Hlc,
    dot: Dot,
    context: VersionVector,  // versions of *this path* the writer had seen
    author: GrainId,         // device id; display/audit only, never used for merging
}
```

File metadata (mtime, permissions) is not synced. Adding it later as a
`#[serde(default)]` field does not break the wire format.

A replica is opened from its registration (§3.2):
`ReplicaConfig { app_name, workspace_id: Uuid, root: PathBuf, max_file_size: u64 /* default 64 MiB */ }`.

### 2.2 Store (redb)

Per replica:

- per path: `PathState { cur: Entry, seen: VersionVector, materialized: Option<[u8; 32]> }`
  - `cur` — the adopted version (tombstones are kept in the store; they have no file)
  - `seen` — every version of this path merged so far
  - `materialized` — hash of what was last written to (or read from) the file on disk
- node-wide version vector `vv`
- own `ReplicaId`, `counter`, last `Hlc`
- `format_version` (§3.1)

Losing the store is recoverable: a new replica id is created, every file is recorded as a
local write, and syncing with peers converges without conflict copies because identical
content never produces one (rule 3).

### 2.3 File content lives in the origin

There is no second copy in a blob store.

- **Serving**: a `blob` request reads the origin file, hashes it, and sends it only if the
  hash matches; otherwise it answers "not available" and the requester tries another peer.
- **Receiving**: write to `<node dir>/staging/`, verify the hash, then move into place
  (`rename`, which replaces atomically on the same volume on Linux and Windows).
- **Change detection** uses `TrackStore` mtime + size as a pre-filter; only changed files
  are re-hashed.

### 2.4 Merge rules

Merging an incoming entry `e` into a path's `(cur, seen)`:

1. **`seen` covers `e.dot`** → already merged; ignore.
2. **`e.context` covers `seen ∪ {cur.dot}`** → `e` was written on top of what we have;
   `cur = e`, `seen = e.context ∪ {e.dot}`.
3. **Otherwise the edits are concurrent.**
   - Winner = maximum of `(is_not_tombstone, hlc, replica_id)`. It is a total order, so every
     node picks the same winner regardless of arrival order, and an edit always beats a
     concurrent delete.
   - `seen = seen ∪ e.context ∪ {e.dot}`; `cur = winner`.
   - If the loser is not a tombstone and its content differs from the winner's, create a
     **conflict copy** as an ordinary local write at
     `<stem>.conflict-<first 8 hex of loser replica id>-<loser counter>.<ext>`
     (`<name>.conflict-…` when there is no extension).
     - The path is deterministic and identical content never produces a copy, so several
       nodes creating the same copy concurrently converge on one file.
     - If the loser is local, the local file is moved to the copy path before the winner is
       written. If the loser is remote, its bytes are fetched from a peer; if no peer can
       serve them anymore, the copy is skipped here — the replica that wrote the loser will
       create the same copy from its own file once it receives the winner.

Before merging into a path, the node checks that path's file for an unrecorded external
edit (§2.5) and records it first, so a pending local edit is never overwritten unseen.

### 2.5 Local writes and external edits

Apps write files; the node records them. Every write — by the node's own process or by any
other process, including editors — is detected on disk and produces an entry with
`dot = (self, counter + 1)`, `context = seen ∪ {cur.dot}`, `hlc = tick()`.

Detection compares the file's hash with `materialized`:

- file hash ≠ `materialized` → an external edit; record it.
- file hash = `materialized` but ≠ `cur` → a materialization was interrupted
  (the store commits before the file is written); write `cur` out again.

Recording the same content as `cur` is a no-op and assigns no dot.

### 2.6 Filtering and limits

Filtering is **declarative**, because the process running the node may belong to a
different app than the workspace (§3.3):

- **Built-in rule per `app_name`**: hidden entries excluded, the app directory `.<app_name>/`
  allowed (today's `is_syncable`). The rule is keyed on the name only, so it needs no code
  from the app.
- **`.sapphireignore`** at the workspace root, gitignore syntax, synced like any file.
  `sapphire-sync` workspaces rely on it.
- **Files over `max_file_size`** are not synced and are reported.
- **Not synced**: empty directories; symlinks (skipped with a warning).
- **Paths the local OS cannot represent** (invalid characters on Windows, case-only
  collisions) keep their entry in the store but are not materialized and are reported.
  Automatic renaming is out of scope.

Skipped paths are reported in `status.json` (§3.1).

### 2.7 Clock

- `tick()` advances past both the wall clock and the last HLC.
- On receipt, the HLC observes the remote value. A remote wall time more than 24 h ahead of
  the local clock is accepted but logged, and the local HLC never advances beyond
  `now + 24h`, so one badly skewed device cannot drag everyone's clock forward.

### 2.8 Out of scope

Tombstone GC (possible once every active device's reported version vector covers the
tombstone), first-class moves/renames, per-file-type merge, chunked/resumable transfer.

## 3. Host node, mesh and pairing (`sapphire-framework-net`)

### 3.1 The shared node directory

```
<platform data root>/sapphire/            # override: SAPPHIRE_NODE_DIR
    format                                 # directory format version
    node.key                               # iroh secret key -> this device's NodeId
    node.lock                              # OS file lock, held by the process running the node
    net.toml                               # host-local network config (discovery, relays, embedded relay)
    workspaces.toml                        # workspaces this host keeps: {id, app_name, root, max_file_size}
    invites.toml                           # pending pairing invites
    status.json                            # written by the running node, including who runs it
    logs/node.log                          # sync log, written by whichever process runs the node
    staging/
    replicas/<workspace-id>/sync.redb
    mesh/                                  # the device mesh, itself a synced workspace (§3.5)
```

It is framework-wide: it carries no app name and no kind. This sits outside the per-app,
per-kind layout of #129, deliberately — one host is one device.

The replica store records the workspace root it was registered with and refuses to open
for a different path (two copies of a workspace with the same id on one host).

**Environments without a shared directory** (mobile app sandboxes, or an app configured
with its own `SAPPHIRE_NODE_DIR`) simply use a private directory. The app then is its own
device in the mesh (e.g. `phone-journal` and `phone-ledger`). If the platform later offers a
shared container (e.g. an iOS App Group), pointing both apps at it merges them into one
device with no other change.

**Format versioning.** `format` and each store's `format_version` are checked before the
node starts. A process that finds a newer format than it understands does not run the node:
it releases the lock and stays in files-only mode (logging once), leaving the node to a
newer app. A process that finds an older format migrates it (idempotently) before starting.

### 3.2 Registering workspaces

- `net::register_workspace(app_name, workspace_id, root)` adds an entry to
  `workspaces.toml` and publishes `{app_name, display name}` to
  `mesh/workspaces/<workspace-id>.toml`. Apps call it when the user enables sync for a
  workspace.
- The running node watches `workspaces.toml` and opens or closes replicas as entries
  appear or disappear.
- Another device picks a workspace from the mesh list and places it locally with
  `workspace add <workspace-id> --dir <dir>`.

### 3.3 Lock and roles

- On start, every framework app that has sync enabled tries to take `node.lock`.
- **Holder**: runs `SyncNode` for **every** workspace in `workspaces.toml`, whichever app
  it belongs to.
- **Follower**: reads and writes files only, and retries the lock every 10 s. When the
  holder exits or crashes, the OS releases the lock and the next process to retry becomes
  the holder. No handoff is needed; sync pauses for a few seconds.
- A follower's writes reach peers because the holder watches every registered root
  (§2.5).
- **Inter-process communication is files only**: invites via `invites.toml`, state via
  `status.json` (connected peers, per-workspace progress, skipped paths, conflicts; written
  every 5 s and on change), registration via `workspaces.toml`. A local socket can be added
  later if a GUI needs live events from another process.

**Which process is syncing, and where its log is.** With several long-running services on
one host (e.g. journal and agent servers), it must be obvious who runs the node:

- `status.json` carries `holder: { app_name, kind, pid, version, started_at }`.
- A follower logs one line at startup and whenever it notices the holder changed (on its
  lock retry):
  `INFO sync node is run by sapphire-journal server (pid 4321); sync log: <node dir>/logs/node.log`
- The holder adds a tracing layer that writes events from the `sapphire_framework_sync` and
  `sapphire_framework_net` targets to `<node dir>/logs/node.log`, in addition to the
  service's own output. Only the holder writes (the lock guarantees a single writer), so
  the file continues across holder changes; a new holder starts by logging
  `holder changed: <previous> → <new>`. Size-based rotation: 10 MiB × 3 files.
- `node status` shows the holder; `node log [--follow]` reads the shared log from any app.

Broadcasting sync logs to followers over a socket was rejected: nothing is received when no
follower runs, other apps' logs get mixed into each service's output, and Windows would need
a separate named-pipe path. A `preferred_holder` setting (non-preferred holders yield) was
deferred because it requires a handoff.

### 3.4 Protocols

ALPNs are common to every app:

| ALPN | purpose |
|---|---|
| `sapphire/sync/1` | replication (§3.7) |
| `sapphire/blob/1` | `{workspace_id, hash}` → bytes; requester verifies SHA-256 |
| `sapphire/pair/1` | joining the mesh (§3.6) |

Messages are length-delimited postcard frames.

### 3.5 The mesh and authorization

- The mesh is identified by a `mesh_id` (UUIDv7), created by the first device.
- `mesh/` is synced like any workspace, among all devices, always. It holds:
  - `devices/<grain-id>.toml` — `{name, node_id, retired_at?}`, one file per device, so
    concurrent pairings on different devices never collide in one file;
  - `workspaces/<workspace-id>.toml` — `{app_name, name}`.
- **Founding device**: creating the mesh writes the device's own record, so it has a
  `device_id` to use as `Entry.author` before its first sync.
- **Authorization**: a connection is accepted if the remote `NodeId` belongs to a
  non-retired device of the mesh. There is no per-workspace permission (single user); a
  device syncs whatever workspaces both sides host.
- **Revocation**: retiring a device is a mesh change; as it replicates, every node drops and
  refuses that device's connections.
- Attribution of mesh changes comes from the sync `Entry` (`author`, `hlc`).

### 3.6 Pairing

1. **Invite** (on any device): `device invite --name phone` creates a ticket encoding
   `{mesh_id, inviter NodeAddr, secret: 32 random bytes, expires_at}` (postcard, base32,
   prefixed `sapphire:`). The invite goes to `invites.toml`: single use, default TTL
   10 minutes. The holder re-reads the file on each pairing attempt, so any process can
   issue invites. If no process holds the lock, the inviting CLI takes it and runs the node
   in the foreground until the invite is used or expires.
2. **Join** (on the new device): `mesh join <ticket>` connects over `pair/1` and sends the
   secret and the proposed device name.
3. **Admit** (holder on the inviter's host): verify the secret in constant time, check
   expiry and unused status, write the device record (with `node_id`) as a local write,
   mark the invite used, reply `{mesh_id, device_id}`.
4. **Sync**: the joiner syncs `mesh/` and can then `workspace add` any listed workspace.
   At first only the inviter knows the new device; other devices accept it once the mesh
   change reaches them.

### 3.7 Connections and sessions

Per connection (bidirectional control stream):

1. Both sides send `Hello { mesh_id, device_id, hosted: [(workspace_id, app_name)] }`. A
   mismatched mesh closes the connection with a reason (logged, not retried).
2. For each workspace both sides host, a dedicated stream runs a session:
   1. Both sides send `SessionHello { workspace_id, replica_id, vv }`.
   2. Each side sends, in pages, every `cur` entry whose dot is not covered by the peer's
      `vv`. Content of files ≤ 64 KiB is sent inline in the same stream, avoiding a round
      trip per small file. Then `Done`.
   3. Received entries are merged as they arrive (rule 1 makes redelivery harmless). The
      replica's `vv` is updated only after the peer's `Done`; an interrupted session is
      simply resent next time.
   4. Missing larger content is fetched over `blob/1`.
   5. The stream stays open for live propagation (§4.2).
3. Workspaces registered later on either side are announced on the control stream and get
   a session.

### 3.8 Endpoint configuration (`net.toml`, host-local)

- **Discovery**: n0 DNS (pkarr) and local-network mDNS, both on by default; static
  addresses may be added.
- **Relays**: n0 default, custom URLs, or disabled. Custom relay URLs may also be published
  in `mesh/net.toml` (synced), so installing one server makes every device use its relay.
- **Embedded relay** (`[relay.embedded]`): runs iroh-relay in-process, TLS via ACME or
  supplied certificates. Honoured only by binaries built with feature `embedded-relay`
  (in practice `sapphire-sync` on a server); a holder built without it logs a warning.

## 4. Node runtime

### 4.1 `SyncNode`

Run by the lock holder. It:

- owns the iroh `Endpoint` and one replica per registered workspace, plus `mesh/`;
- tracks, per session, the peer's last known version vector;
- watches every registered root (`notify`, debounced) plus a periodic full scan
  (default 5 min). Its own materializations match `materialized` and are not mistaken for
  external edits;
- writes `status.json`.

### 4.2 Dialing and live propagation

- **Dial**: every non-retired device of the mesh, plus mDNS-discovered mesh devices. Full
  mesh — device counts are small. Exponential backoff, max 5 min.
- **Live**: after the initial exchange, sessions stay open. Local commits are pushed to all
  connected peers hosting that workspace. Entries received from one peer that changed state
  are forwarded to other connected peers whose known version vector does not cover them, so
  A → S → B propagates live even when A and B cannot reach each other.

### 4.3 Per kind

Every kind competes for the lock the same way; they differ in how long they live.

| kind | lifetime | typical role |
|---|---|---|
| server (`sapphire-sync` headless, or an app server) | daemon | holder most of the time; optional embedded relay |
| desktop | while the app is open | holder or follower |
| cli | per invocation | follower for ordinary commands; `sync` runs a one-shot sync |
| mobile | while the app is in the foreground | holder of its private directory |

One-shot `sync`:

1. Take the lock; if held, print that the running node handles sync and exit.
2. Scan for external edits.
3. Dial reachable peers in parallel (10 s timeout each) and run sessions until both sides
   have sent `Done` for every shared workspace.
4. Fetch missing content, release the lock, exit.

Whether a cli syncs automatically after write commands is the app's choice.

### 4.4 Backend

- `SyncedBackend` exposes `role() -> Holder | Follower` and `status()` (read from
  `status.json` for followers, from memory for the holder).
- `sync()` is a no-op while a holder is live, otherwise a one-shot sync.
- `BackendEvent` gains `Conflict { path, copy_path }` and `Skipped { path, reason }`,
  emitted by the holder; followers see the same information through `status()` and, for
  conflicts, as files.

### 4.5 Errors

- Session errors are logged; the connection is dropped and redialed with backoff.
- Mesh mismatches and unauthorized `NodeId`s are rejected with a reason and logged; they
  are not retried.
- A replica that fails to open (format too new, root path mismatch, I/O) is reported in
  `status.json` and skipped; other workspaces keep syncing.
- Clock skew: §2.7.

## 5. App fix-ups, CLI, app migration, rollout

### 5.1 App fix-ups

There is no sync hook. An app that must repair states produced by merging (journal: two
live files with the same entry id) does it in its own process, on its own file-change
events, under this contract:

- **Content-deterministic**: decisions depend only on file contents and paths — never on
  local time, the local device, or sync metadata the app cannot see.
- **Idempotent**: running again on corrected state writes nothing.
- **Convergent**: two devices running the fix-up concurrently write the same content to the
  same paths, so rule 3's identical-content case creates no conflict copies.

A fix-up runs wherever the app runs; hosts without the app receive the corrected files
through sync.

### 5.2 CLI subcommands

`NodeCommand` (clap `Subcommand`, feature `cli` of `-net`): `sync`, `node status`,
`node log [--follow]`,
`mesh create`, `mesh join <ticket>`, `device invite`, `device list`, `device retire`,
`workspace list`, `workspace share <path>`, `workspace add <id> --dir <dir>`,
`workspace remove <id>`. Apps embed it with `#[command(flatten)]`, like `WorkspaceArgs`
(#128). Because the node is shared, running these from any app acts on the same mesh.

This reverses the earlier "the framework ships no subcommands" stance: pairing written four
times would drift between apps.

### 5.3 App migration requirements

Each gets its own spec in its own repository.

- **`sapphire-sync`** (new, first): Syncthing-like folder sync over arbitrary directories
  (`app_name = "sapphire-sync"`, filtering via `.sapphireignore`); the reference
  implementation, the E2E test bed, and the headless server node.
- **timer**: drop the `remote` subcommand; embed `NodeCommand`; call `register_workspace`.
- **ledger**: remove `sapphire-ledger-sync` and the server's `/rpc`; replace the
  token → device lookup in `identity.rs` with the host device from the mesh. `updated_by`
  is the local device id for local writes (remote changes arrive as file content that
  already carries it).
- **journal**: rewrite `dedupe.rs` as a content-deterministic fix-up (§5.1) inside the
  journal process — e.g. keep the lexicographically smallest path among files sharing an
  id, quarantine the others locally and delete them from the workspace.
  `increment_until_free` must also become content-deterministic; it currently depends on
  the node's local state and can diverge. This is the largest item in journal's spec.
  `updated_by` displays the device name; the user lookup goes away.
- **agent**: import `KeyStore` from `keys`. Its HTTP device table (key → device →
  room_profile) no longer needs users; whether it uses mesh devices or keeps its own
  per-workspace table is decided in agent's spec.

### 5.4 Compatibility

- Breaking wire and crate changes: release as **0.15.0**.
- No sync data migration (not in production): existing server change logs are discarded;
  devices pair into a new mesh.
- Registry: a migration from single-file `devices.toml` to one file per record is provided
  (idempotent, retryable), because agent already authenticates against the registry.
  `users.toml` and `user_id` fields are ignored on load and dropped on the next write.
  Moving an app's existing device table into the mesh is decided per app.

### 5.5 Implementation order (framework)

1. **Sync core**: types, redb store, merge rules, external-edit detection, conflict copies,
   declarative filtering. Tested with several in-memory replicas, no network.
2. **Registry**: users removed, one file per record + migration, `node_id`.
3. **Node directory**: layout, format versioning, lock and roles, `workspaces.toml`
   registration, `status.json` with holder info, shared sync log.
4. **Net basics**: `Endpoint`, control stream + `sync/1`, `blob/1`, mesh authorization,
   `SyncNode`, live propagation, watcher.
5. **Pairing**: mesh creation, `pair/1`, invites, `NodeCommand`.
6. **Server features**: `embedded-relay`, relay URLs from `mesh/net.toml`.
7. **Cleanup**: extract `keys`; remove `rpc`, `remote-*`, `blob`; `SyncedBackend`; facade;
   rewrite the sync sections of `ARCHITECTURE.md`, mark WASM out of scope, remove users.

`sapphire-sync` can start as an E2E harness from step 4. App order afterwards:
sapphire-sync → timer → ledger → journal → agent.

## 6. Testing

### 6.1 Sync core (no network)

- **Unit**: HLC monotonicity, observation, 24 h clamp; version-vector cover/merge; merge
  rules as a table — fast-forward (rule 2), duplicate (rule 1), concurrent (rule 3),
  delete vs edit, identical content, three-way concurrency.
- **Property-based (proptest) — the primary guarantee.** 3–4 replicas; random writes,
  deletes and external edits; random pairwise syncs with interruptions, redelivery and
  reordering. Properties:
  - **Convergence**: after everyone has synced with everyone, all stores and file trees
    are identical.
  - **No silent loss**: any content that was ever `cur` on some replica is, unless
    causally overwritten, present at the end either as the final version or as a conflict
    copy.
  - **Idempotence**: merging the same entries again changes nothing.
- **Crash recovery**: a fault-injection point between store commit and file write; after
  restart the scan re-materializes instead of recording an external edit.
- **Filtering**: built-in rule per app name, `.sapphireignore`, size cap, unrepresentable
  paths (entry kept, not materialized, reported).

### 6.2 Node directory (no network)

- lock: holder/follower roles; takeover after the holder process exits (a child process
  that is killed); format too new → follower; older format → migrated.
- registration: adding/removing `workspaces.toml` entries opens/closes replicas.
- root path mismatch refuses to open a replica.
- holder visibility: `status.json` names the holder; a follower logs the holder line at
  startup and after a takeover; the shared log records `holder changed` and rotates at the
  size limit.

### 6.3 Net integration (real iroh, in-process)

2–3 nodes, each with its own node directory, `RelayMode::Disabled`, localhost bind, static
addresses — no external network.

- basic sync of two workspaces of different apps over one connection; blob fetch; inline
  content ≤ 64 KiB
- a workspace hosted on only one side gets no session
- **A–S–B topology** (A and B not connected): live propagation through S
- rejection: unknown `NodeId`, mesh mismatch, retired device (connection dropped once the
  retirement arrives)
- pairing: success, expired, reused, wrong secret; the joiner sees `mesh/workspaces/`
- blob fallback: content changed on one peer, fetched from another
- follower writes reach peers through the holder

A real-relay test exists as `#[ignore]`, run manually.

### 6.4 Registry and CI

- Registry migration: idempotent, retryable after partial failure; users dropped.
- CI on Linux and Windows (path rules, rename semantics, file-lock semantics). proptest
  case counts are reduced in CI.
- E2E tests belong to the `sapphire-sync` spec.

## Risks

1. **journal's id reassignment** must become content-deterministic; until it does, journal
   cannot migrate safely.
2. **Framework version skew across apps on one host.** Format versioning keeps an old app
   from corrupting newer state, but a host whose newest app is not running does not sync.
3. **File locks** behave poorly on network filesystems; the node directory must be local.
4. **Mobile devices appear once per app** unless the platform allows a shared container.
5. **Embedded relay** needs a publicly reachable address and TLS; home servers behind NAT
   may still need n0 relays.
6. **iroh API evolution** within 1.x (wire-stable, but API-level changes are possible), and
   the exact names of discovery features must be confirmed against 1.2.
7. **Large workspaces**: full-mesh dialing and whole-file transfer are sized for a handful of
   devices and files under the cap; chunking and smarter topology are deferred.
8. **Tombstones grow without GC** until tombstone GC is designed.
