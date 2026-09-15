# Peer-to-peer workspace sync over iroh

- Date: 2026-09-15 (revised the same day: shared host node, sync groups, single user, grain-id identifiers, service installation)
- Scope: `sapphire-framework` — new crates `sapphire-framework-sync`, `-net`, `-keys`, `-service`;
  changes to `-registry`, `-backend`, the facade; removal of `-rpc`, `-remote-client`,
  `-remote-server`, `-blob`
- Follow-ups (separate specs): `sapphire-sync` (new sync app, first consumer; lives in
  this repository), then `sapphire-timer`, `sapphire-ledger`, `sapphire-journal`,
  `sapphire-agent` (each in its own repository)
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
| API keys (`keys.toml`) for sync, per app | One host `NodeId` checked against a cross-app sync group; pairing tickets |
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
9. **Devices form cross-app sync groups.** A device pairs into a group once and can then host any
   of that group's workspaces; each device chooses which workspaces it keeps a copy of
   (`workspace map`). Layout, wire format and authorization support a host in several groups
   (e.g. work and personal); the first release's CLI limits a host to one.
10. **User-visible identifiers are grain-ids** (`group_id`, `workspace_id`, `device_id`, ids
    in file names); only the internal `ReplicaId` stays a UUID (§2.1). Workspaces and groups
    can also be selected by name.
11. **`sapphire-framework-service`** provides `service install` for any app: user-level
    units by default, a system-wide systemd unit when run as root on Linux, with a per-app
    default run-as user (§5.5).
12. **Any file type is synced, with a size cap.** Content is addressed by hash for all
    files; whole-file transfer only (default cap 64 MiB); chunked/resumable transfer is
    deferred.
13. **A sync-only app, `sapphire-sync` (Syncthing-like), is built before migrating the
    existing apps.** It is the reference implementation, the headless node for servers
    (it can serve as the production always-on peer), and the dedicated background sync
    service for hosts that prefer one — the role iCloud's background daemon plays, which
    keeps a later move to an OS-service-style deployment an operational choice rather than
    a redesign. Because it is a core component, it **lives in the `sapphire-framework`
    repository** and is versioned in lockstep with the framework crates. Its directory
    layout and packaging are decided in its own spec.
14. **Guardrails for embedding the node in apps**: a compatibility policy tied to the format
    version, fault isolation so a node failure never takes the app down, and a per-host
    switch that leaves the node to a dedicated service (§3.3, §4.1, §5.4).

## 1. Crate layout

### New

| crate | role | depends on |
|---|---|---|
| `sapphire-framework-sync` | Replication core, **transport-agnostic**: wire types, `ReplicaStore` (redb), merge rules, HLC, conflict copies, declarative filtering, external-edit detection (mtime + size pre-filter in its own store) | serde, redb, sha2, ignore, walkdir |
| `sapphire-framework-net` | Shared node directory and lock, iroh `Endpoint`, protocol handlers (`sync`, `blob`, `pair`), authorization against the group, discovery and relay configuration, `SyncNode` runtime, file watcher, workspace registration API, `NodeCommand` CLI subcommands (feature `cli`), embedded relay (feature `embedded-relay`) | iroh 1.x, `-sync`, `-registry` |
| `sapphire-framework-keys` | `KeyStore` / `KeyEntry` / `protect`, moved out of `-remote-server` (#103). For non-sync HTTP endpoints | serde, toml, axum (feature) |
| `sapphire-framework-service` | `ServiceCommand` (`service install` / `uninstall` / `status`) and `ServiceSpec`: registers an app's long-running command with the OS service manager (§5.5). Not sync-specific; reused by `sapphire-sync`, `sapphire-agent`, … | clap, `-workspace` (`AppContext`) |

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
`ReplicaConfig { group_id: GrainId, app_name, workspace_id: GrainId, root: PathBuf, max_file_size: u64 /* default 64 MiB */ }`.

**Identifier policy.** Identifiers a user can see or type (`group_id`, `workspace_id`,
`device_id`, and ids embedded in file names) are grain-ids. `ReplicaId` stays a UUID: it is
never typed, and a collision would reuse dots, which is unrecoverable, so it gets the wider
id space. Where a replica must appear in a file name, it is shown as a grain-id derived from
the UUID's bytes with grain-id's byte-slice constructor — from the **suffix** for UUIDv7 (the
prefix is a timestamp, so prefixes of ids created close together collide) and from the
**prefix** for UUIDv4. `ReplicaId` is UUIDv7, so its suffix is used.

### 2.2 Store (redb)

Per replica:

- per path: `PathState { versions: Vec<Entry>, seen: VersionVector, disk: DiskState }`
  - `versions` — the **sibling set**: every version of the path not superseded by another
    known version (usually one; several after concurrent edits). Tombstones are versions
    too; they have no file.
  - `seen` — every version of this path merged so far (always covers `versions`)
  - `disk: DiskState { hash: Option<[u8; 32]>, seen: VersionVector, mtime_ns, len }` —
    what is on disk (`hash = None` means no file), which versions that file reflects, and
    the file's mtime and size at that moment (the change-detection pre-filter; kept here,
    in the same transaction as the state, rather than in a separate `TrackStore`)
- node-wide version vector `vv`
- own `ReplicaId`, `counter`, last `Hlc`
- `format_version` (§3.1)

The **winner** of a path is the sibling with the maximum
`(is_not_tombstone, hlc, replica_id, counter)`. It is what the file on disk should hold.

Losing the store is recoverable: a new replica id is created, every file is recorded as a
local write, and syncing with peers converges without conflict copies because identical
content never produces one (§2.4).

### 2.3 File content lives in the origin

There is no second copy in a blob store.

- **Serving**: a `blob` request looks for a file whose recorded disk hash matches (or a staged
  file), re-hashes it, and sends it only if the hash still matches; otherwise it answers
  "not available" and the requester tries another peer.
- **Receiving**: write to the replica's staging directory under the content hash, verify
  the hash, then move into place with `rename` (atomic replace on one volume on Linux and
  Windows). When the staging directory is on another volume than the workspace, the file is
  copied to a hidden temporary name beside the target and renamed from there.
- **Change detection** compares a file's mtime and size with `disk`; only changed files are
  re-hashed.

### 2.4 Merge rules

The unit of replication is a path's state, `PathUpdate { path, versions, seen }` — not a
single entry. Joining an incoming update into a local state (the DVV-set join):

1. Keep a local sibling if the incoming `seen` does not cover its dot, or the incoming
   `versions` contain it.
2. Keep an incoming sibling if the local `seen` does not cover its dot, or the local
   `versions` contain it.
3. `seen = local.seen ∪ incoming.seen`.

The join is commutative, associative and idempotent, so every replica reaches the same
sibling set whatever order updates arrive in. (Keeping only a single winner per path is not
associative: a delete that supersedes an edit can lose to that edit in one merge order and
win in another, because "edits beat deletes" makes the winner key non-monotonic along
causality.)

A **local write** is joined the same way, as an update with
`versions = [e]`, `seen = e.context ∪ {e.dot}`.

**Conflict copies.** Whenever a state has more than one sibling, every *loser* (sibling
other than the winner) that is not a tombstone and whose content differs from the
winner's gets a **conflict copy**, written as an ordinary local write at
`<stem>.conflict-<loser replica as grain-id>-<loser counter>.<ext>`
(`<name>.conflict-…` when there is no extension), e.g. `note.conflict-123abcd-42.md`.
The grain-id is derived from the replica UUID's suffix (§2.1).

- The path is deterministic and identical content never produces a copy, so several
  nodes creating the same copy concurrently converge on one file.
- Copies are created **before** the winner is written over the loser's file, reading the
  loser's bytes from the local file when it is on disk, otherwise from local content or a
  peer.
- A copy is created only if the copy path has never had a state on this replica, so a copy
  the user deleted is not resurrected.
- If the loser's bytes are unavailable, no copy is made here, and the file's `disk.seen`
  excludes that loser (§2.5) so a later local edit does not supersede it; a replica that has
  the bytes will create the copy.

Before joining an update into a path, the node checks that path's file for an unrecorded
external edit (§2.5) and records it first, so a pending local edit is never overwritten
unseen.

### 2.5 Local writes and external edits

Apps write files; the node records them. Every write — by the node's own process or by any
other process, including editors — is detected on disk and produces an entry with
`dot = (self, counter + 1)`, `context = disk.seen`, `hlc = tick()`. Using `disk.seen`
(what the edited file was based on) rather than `seen` matters when a remote version has
been merged but not yet written to disk: the edit is then correctly concurrent with it.

Detection compares the file's hash (`None` for a missing file) with `disk.hash`:

- file hash ≠ `disk.hash` → an external edit; record it.
- file hash = `disk.hash` but ≠ the winner's → a materialization is pending or was
  interrupted (the store commits before the file is written); write the winner out again.

After the winner is on disk, `disk.seen = seen`, except that for each loser still needing
a conflict copy that could not be made, the loser's replica entry is lowered to
`counter - 1`.

Recording the same content as `disk.hash` is a no-op and assigns no dot.

**Missing root guard.** A root that is temporarily absent (unplugged drive, unmounted
network share) would otherwise look like every file was deleted, and the tombstones would
delete the files on every other device. Before any scan, the node checks that the root and
its marker directory (`.<app_name>/`) exist. If the store holds materialized files but the
root or marker is missing, the replica is **paused** — nothing is recorded, the pause is
reported in `status.json`, and scanning resumes when both reappear. A newly mapped
workspace (§3.2) has its root and marker created at mapping time, so it is never paused
for being empty.

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

## 3. Host node, group and pairing (`sapphire-framework-net`)

### 3.1 The shared node directory

```
<platform data root>/sapphire/            # override: SAPPHIRE_NODE_DIR
    format                                 # directory format version
    node.key                               # iroh secret key -> this device's NodeId
    node.lock                              # OS file lock, held by the process running the node
    net.toml                               # host-local network config (discovery, relays, embedded relay)
    workspaces.toml                        # workspaces this host keeps: {group_id, id, app_name, root, max_file_size}
    invites.toml                           # pending pairing invites
    status.json                            # written by the running node, including who runs it
    logs/node.log                          # sync log, written by whichever process runs the node
    staging/
    replicas/<workspace-id>/sync.redb
    groups/<group-id>/                      # one directory per group this host belongs to (§3.5)
        replica/sync.redb                  #   replica store of the group workspace itself
        root/                              #   the group workspace's files, synced among its devices
```

It is framework-wide: it carries no app name and no kind. This sits outside the per-app,
per-kind layout of #129, deliberately — one host is one device.

The replica store records the workspace root it was registered with and refuses to open
for a different path (two copies of a workspace with the same id on one host).

**Environments without a shared directory** (mobile app sandboxes, or an app configured
with its own `SAPPHIRE_NODE_DIR`) simply use a private directory. The app then is its own
device in the group (e.g. `phone-journal` and `phone-ledger`). If the platform later offers a
shared container (e.g. an iOS App Group), pointing both apps at it merges them into one
device with no other change.

**Format versioning.** `format` and each store's `format_version` are checked before the
node starts. A process that finds a newer format than it understands does not run the node:
it releases the lock and stays in files-only mode (logging once), leaving the node to a
newer app. A process that finds an older format migrates it (idempotently) before starting.

### 3.2 Registering workspaces

- **Sync id.** `Workspace::uuid` is derived from the workspace path, so it differs between
  hosts and cannot identify a workspace across devices. The sync identity is a separate
  grain-id stored in the marker directory as `.<app_name>/sync-id`. The file is synced, and
  its content is the same on every device, so it never conflicts. Re-registering a
  workspace (e.g. after losing the node directory) reads the existing id. Cache keys keep
  using the path-derived UUID.
- **Share** (a new workspace): `net::share_workspace(group, app_name, root, name)` reads or
  creates `sync-id` (a fresh grain-id that is not already listed in the group), adds an entry
  to `workspaces.toml`, and publishes `{app_name, name}` to the group's
  `workspaces/<workspace-id>.toml`. `name` defaults to the root directory's name. Apps call
  it when the user enables sync for a workspace.
- **Map** (an existing workspace onto a local directory): `workspace map <name|id> <dir>`
  looks the workspace up in the group, creates `<dir>` and its marker `.<app_name>/` with
  `sync-id`, and adds the `workspaces.toml` entry. **Unmap** (`workspace unmap <name|id>`)
  removes the entry; local files stay, and the workspace stays listed in the group.
- **Selectors** follow the registry's rule: a selector matches a name first, then parses as a
  grain-id; a name shared by several workspaces cannot be used as a selector.
- The running node watches `workspaces.toml` and opens or closes replicas as entries
  appear or disappear.

### 3.3 Lock and roles

- On start, every framework app that has sync enabled tries to take `node.lock`.
- **Holder**: runs `SyncNode` for **every** workspace in `workspaces.toml`, whichever app
  it belongs to.
- **Follower**: reads and writes files only, and retries the lock every 10 s. When the
  holder exits or crashes, the OS releases the lock and the next process to retry becomes
  the holder. No handoff is needed; sync pauses for a few seconds.
- A follower's writes reach peers because the holder watches every registered root
  (§2.5).
- **Dedicated service mode**: with `embedded_node = false` in `net.toml`, processes that
  embed the node as a library never try the lock and stay followers. Only a process started
  as a dedicated node (`sapphire-sync` running as a service) takes it. Recommended on
  servers and everyday machines, where fault isolation and a single framework version
  matter more than zero setup; apps need no change, they simply become followers.
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
| `sapphire/pair/1` | joining a group (§3.6) |

Messages are length-delimited postcard frames.

### 3.5 Groups and authorization

A **sync group** ("group" in the CLI; `SyncGroup` / `group_id` in code) is a set of devices that share workspaces. **The data model, on-disk layout and
wire format support a host belonging to several groups** (e.g. a laptop in both a "work"
and a "personal" group, while a work-only machine sees only the work group). In the first
release the CLI allows **one** group per host: `group create` / `group join` refuse when the
host already belongs to a group. Lifting that limit is a CLI change, not a format change.

- A group is identified by a `group_id` (grain-id) and has a `name`, both chosen by the device
  that creates it.
- Each group's workspace (`groups/<group-id>/root/`) is synced among that group's devices,
  always. It holds:
  - `group.toml` — `{name}`;
  - `devices/<grain-id>.toml` — `{name, node_id, retired_at?}`, one file per device, so
    concurrent pairings on different devices never collide in one file;
  - `workspaces/<workspace-id>.toml` — `{app_name, name}`;
  - `net.toml` — optional relay URLs published to the group (§3.8).
- **Every workspace belongs to exactly one group** (`workspaces.toml` records `group_id`).
- **Device ids are per group.** One host has one `NodeId` but a separate device record, and
  so a separate `device_id`, in each group it belongs to. `Entry.author` is the device id in
  the workspace's group.
- **Founding device**: creating a group writes the device's own record, so it has a
  `device_id` to use as `Entry.author` before its first sync.
- **Authorization**: a connection is accepted if the remote `NodeId` belongs to a
  non-retired device of **at least one group this host also belongs to**. Only workspaces of
  groups both sides share are exchanged. There is no per-workspace permission inside a group
  (single user).
- **Revocation**: retiring a device is a change to that group; as it replicates, every node
  stops exchanging that group's workspaces with the device, and drops the connection once
  no shared group remains.
- Attribution of group changes comes from the sync `Entry` (`author`, `hlc`).
- **Group selectors** (`--group <name|id>`) use the same name-first rule as workspaces. The
  flag may be omitted while the host belongs to a single group.

### 3.6 Pairing

1. **Invite** (on any device): `device invite --name phone [--group <name|id>]` creates a ticket encoding
   `{group_id, inviter NodeAddr, secret: 32 random bytes, expires_at}` (postcard, base32,
   prefixed `sapphire:`). The invite goes to `invites.toml`: single use, default TTL
   10 minutes. The holder re-reads the file on each pairing attempt, so any process can
   issue invites. If no process holds the lock, the inviting CLI takes it and runs the node
   in the foreground until the invite is used or expires.
2. **Join** (on the new device): `group join <ticket>` connects over `pair/1` and sends the
   secret and the proposed device name.
3. **Admit** (holder on the inviter's host): verify the secret in constant time, check
   expiry and unused status, write the device record (with `node_id`) as a local write,
   mark the invite used, reply `{group_id, device_id}`.
4. **Sync**: the joiner syncs the group workspace and can then `workspace map` any listed
   workspace.
   At first only the inviter knows the new device; other devices accept it once the group
   change reaches them.

### 3.7 Connections and sessions

Per connection (bidirectional control stream):

1. Both sides send
   `Hello { groups: [{ group_id, device_id, hosted: [(workspace_id, app_name)] }] }`, listing
   only groups the peer's `NodeId` is authorized for (a node does not reveal groups the
   peer is not in). No shared group closes the connection with a reason (logged, not
   retried).
2. For each workspace both sides host in a shared group — including each shared group's own
   workspace — a dedicated stream runs a session:
   1. Both sides send `SessionHello { workspace_id, replica_id, vv }`.
   2. Each side sends, in pages, every path state (`PathUpdate`) whose `seen` is not covered by the peer's
      `vv`. Content of files ≤ 64 KiB is sent inline in the same stream, avoiding a round
      trip per small file. Then `Done`.
   3. Received updates are joined as they arrive (the join is idempotent, so redelivery is harmless). The
      replica's `vv` is updated only after the peer's `Done`; an interrupted session is
      simply resent next time.
   4. Missing larger content is fetched over `blob/1`.
   5. The stream stays open for live propagation (§4.2).
3. Workspaces registered later on either side, and changes in shared groups, are announced
   on the control stream; sessions start or stop accordingly.

### 3.8 Endpoint configuration (`net.toml`, host-local)

- **Discovery**: n0 DNS (pkarr) and local-network mDNS, both on by default; static
  addresses may be added.
- **Relays**: n0 default, custom URLs, or disabled. Custom relay URLs may also be published
  in a group's `net.toml` (synced), so installing one server makes every device of that group
  use its relay. A host in several groups uses the union.
- **Embedded relay** (`[relay.embedded]`): runs iroh-relay in-process, TLS via ACME or
  supplied certificates. Honoured only by binaries built with feature `embedded-relay`
  (in practice `sapphire-sync` on a server); a holder built without it logs a warning.

## 4. Node runtime

### 4.1 `SyncNode`

Run by the lock holder. It:

- owns the iroh `Endpoint` and one replica per registered workspace, plus one per group;
- tracks, per session, the peer's last known version vector;
- watches every registered root (`notify`, debounced) plus a periodic full scan
  (default 5 min). Its own materializations match `disk` and are not mistaken for
  external edits;
- writes `status.json`.

**Fault isolation.** The node runs on its own threads and its own async runtime, never on
the app's. Panics inside node tasks are caught; the node shuts down, logs the failure,
releases the lock (so another process can take over) and reports the failure to the app as
an error state. The app keeps running in follower mode.

### 4.2 Dialing and live propagation

- **Dial**: every non-retired device of every group this host belongs to (one connection per
  remote `NodeId`, however many groups are shared), plus mDNS-discovered devices of those
  groups. Every device dials every other — device counts are small. Exponential backoff, max 5 min.
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
- Group mismatches and unauthorized `NodeId`s are rejected with a reason and logged; they
  are not retried.
- A replica that fails to open (format too new, root path mismatch, I/O) is reported in
  `status.json` and skipped; other workspaces keep syncing.
- A replica whose root or marker is missing is paused, not scanned (§2.5).
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

`NodeCommand` (clap `Subcommand`, feature `cli` of `-net`):

| command | effect |
|---|---|
| `sync` | one-shot sync (§4.3) |
| `node status`, `node log [--follow]` | holder, peers, per-workspace state; shared sync log |
| `group create --name <name>`, `group join <ticket>`, `group list` | create or join a group (one per host in the first release) |
| `device invite --name <name>`, `device list`, `device retire <name\|id>` | pairing and revocation, in the selected group |
| `workspace list` | name, grain-id, app, group, local directory (`-` if unmapped) |
| `workspace share <path> [--name <name>]` | publish a local workspace to the group |
| `workspace map <name\|id> <dir>` | place an existing group workspace in a local directory |
| `workspace unmap <name\|id>` | stop keeping it locally (files and group listing stay) |

Commands acting on a group take `--group <name|id>`, optional while the host is in one group.
Apps embed `NodeCommand` with `#[command(flatten)]`, like `WorkspaceArgs` (#128). Because the
node is shared, running these from any app acts on the same groups.

This reverses the earlier "the framework ships no subcommands" stance: pairing written four
times would drift between apps.

### 5.3 App migration requirements

Each gets its own spec.

- **`sapphire-sync`** (new, first; in this repository): Syncthing-like folder sync over
  arbitrary directories (`app_name = "sapphire-sync"`, filtering via `.sapphireignore`);
  the reference implementation, the E2E test bed, the headless server node, and the
  dedicated sync service of §3.3.
- **timer**: drop the `remote` subcommand; embed `NodeCommand`; call `register_workspace`.
- **ledger**: remove `sapphire-ledger-sync` and the server's `/rpc`; replace the
  token → device lookup in `identity.rs` with the host device from the group. `updated_by`
  is the local device id for local writes (remote changes arrive as file content that
  already carries it).
- **journal**: rewrite `dedupe.rs` as a content-deterministic fix-up (§5.1) inside the
  journal process — e.g. keep the lexicographically smallest path among files sharing an
  id, quarantine the others locally and delete them from the workspace.
  `increment_until_free` must also become content-deterministic; it currently depends on
  the node's local state and can diverge. This is the largest item in journal's spec.
  `updated_by` displays the device name; the user lookup goes away.
- **agent**: import `KeyStore` from `keys`. Its HTTP device table (key → device →
  room_profile) no longer needs users; whether it uses group devices or keeps its own
  per-workspace table is decided in agent's spec.

### 5.4 Compatibility

- Breaking wire and crate changes: release as **0.15.0**.
- **Behavioural compatibility policy.** Because a node built from one app's framework
  version may sync another app's workspaces, anything that changes what two nodes would do
  with the same input — merge rules, conflict-copy naming, filtering rules, the meaning of
  a wire field — requires bumping the format version (§3.1), even when the bytes on disk or
  on the wire are unchanged. Golden tests pin the behaviour of each format version (fixed
  inputs → expected store state and file tree) so an accidental change fails CI.
- No sync data migration (not in production): existing server change logs are discarded;
  devices pair into a new group.
- Registry: a migration from single-file `devices.toml` to one file per record is provided
  (idempotent, retryable), because agent already authenticates against the registry.
  `users.toml` and `user_id` fields are ignored on load and dropped on the next write.
  Moving an app's existing device table into the group is decided per app.

### 5.5 Service installation (`sapphire-framework-service`)

`ServiceCommand` (`service install` / `uninstall` / `status`) registers an app's
long-running command with the OS service manager. Apps describe themselves with:

```rust
pub struct ServiceSpec {
    pub args: Vec<String>,          // e.g. ["run"]
    pub system_run_as: RunAs,       // Root | InvokingUser — default `User=` for system units
    pub post_install: Option<Box<dyn Fn(&InstallContext) -> Result<()>>>,
}
```

**Scope selection on Linux** (overridable with `--user` / `--system`):

| invoked as | unit | activation |
|---|---|---|
| a regular user | `~/.config/systemd/user/<app>.service` | `systemctl --user enable --now`; prints a hint to `loginctl enable-linger` for servers |
| root (including `sudo`) | `/etc/systemd/system/<app>.service`, `After=network-online.target` | `systemctl enable --now` |

**Run-as user for system units** (`--run-as <user>` overrides):

- `RunAs::Root` — no `User=`. For apps that start as root and drop privileges themselves
  (`sapphire-agent`, agent issue #257).
- `RunAs::InvokingUser` — `User=$SUDO_USER`. Without `SUDO_USER` and without `--run-as`,
  install fails with an explanation. For `sapphire-sync`: running it as root would put
  the node directory under `/root` and create synced files owned by root.

`ExecStart` is the absolute path of the running executable plus `args`;
`Restart=on-failure`.

**Other platforms**: user-level only — macOS LaunchAgent, Windows Task Scheduler task at
logon. Invoked with administrator/root rights there, install fails with "system-wide
installation is supported on Linux only". LaunchDaemons and Windows services can be added
when needed.

`post_install` runs with the resolved target user; files it writes into another user's
directories are chowned to that user. `sapphire-sync` uses it to write
`embedded_node = false` into the target user's `net.toml` (skipped with `--keep-embedded`).

An agent running as root with an embedded node uses root's node directory and therefore
joins groups as its own device; agent's spec records this.

### 5.6 Implementation order (framework)

1. **Sync core**: types, redb store, merge rules, external-edit detection, conflict copies,
   declarative filtering. Tested with several in-memory replicas, no network.
2. **Registry**: users removed, one file per record + migration, `node_id`.
3. **Node directory**: layout (per-group directories), format versioning, lock and roles,
   `sync-id`, share/map/unmap registration, missing root guard, `status.json` with holder
   info, shared sync log.
4. **Net basics**: `Endpoint`, control stream + `sync/1`, `blob/1`, per-group authorization,
   `SyncNode`, live propagation, watcher.
5. **Pairing**: group creation, `pair/1`, invites, `NodeCommand`.
6. **Server features**: `embedded-relay`, relay URLs from group `net.toml`.
7. **Service**: `sapphire-framework-service`.
8. **Cleanup**: extract `keys`; remove `rpc`, `remote-*`, `blob`; `SyncedBackend`; facade;
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
  - **No silent loss**: any content a replica ever recorded is, unless
    causally overwritten, present at the end either as the final version or as a conflict
    copy.
  - **Idempotence**: joining the same updates again changes nothing.
- **Golden behaviour tests** per format version (§5.4): fixed scenarios whose resulting store
  state and file tree are checked in; a change requires a format bump.
- **Crash recovery**: a fault-injection point between store commit and file write; after
  restart the scan re-materializes instead of recording an external edit.
- **Filtering**: built-in rule per app name, `.sapphireignore`, size cap, unrepresentable
  paths (entry kept, not materialized, reported).

### 6.2 Node directory (no network)

- lock: holder/follower roles; takeover after the holder process exits (a child process
  that is killed); format too new → follower; older format → migrated.
- registration: adding/removing `workspaces.toml` entries opens/closes replicas.
- root path mismatch refuses to open a replica.
- missing root guard: removing (then restoring) a root or its marker pauses (then resumes)
  the replica and records no tombstones; a freshly mapped empty workspace is not paused.
- `sync-id`: share creates it; re-registering after deleting the node directory reuses it;
  map writes the group's id; selectors resolve names first, reject ambiguous names.
- the first release refuses a second `group create` / `group join`.
- dedicated service mode: with `embedded_node = false`, an embedding process never takes
  the lock; a dedicated node does.
- fault isolation: a panic injected into a node task stops the node, releases the lock and
  leaves the host process running as a follower.
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
- rejection: unknown `NodeId`, group mismatch, retired device (connection dropped once the
  retirement arrives)
- pairing: success, expired, reused, wrong secret; the joiner sees the group's workspaces
- multiple groups (constructed directly, bypassing the first-release CLI limit): with A–B in
  group C and B–D in group E, A and D never receive each other's workspaces, and `Hello` does
  not list a group the peer is not in
- blob fallback: content changed on one peer, fetched from another
- follower writes reach peers through the holder

A real-relay test exists as `#[ignore]`, run manually.

### 6.4 Registry and CI

- Registry migration: idempotent, retryable after partial failure; users dropped.
- Service: unit/LaunchAgent/task file generation per scope and run-as (golden files);
  scope detection from effective uid and `SUDO_USER`; refusal without `SUDO_USER` for
  `InvokingUser`; refusal of system-wide install off Linux. Calls to `systemctl` /
  `launchctl` / `schtasks` go through a trait so tests do not touch the host.
- CI on Linux and Windows (path rules, rename semantics, file-lock semantics). proptest
  case counts are reduced in CI.
- E2E tests belong to the `sapphire-sync` spec.

## Risks

1. **journal's id reassignment** must become content-deterministic; until it does, journal
   cannot migrate safely.
2. **Framework version skew across apps on one host.** Format versioning and the
   behavioural compatibility policy keep an old app from corrupting newer state or merging
   differently, but a host whose newest app is not running does not sync. Dedicated service
   mode (§3.3) removes the skew where it matters.
3. **A bug in one app's process can pause sync for all apps** on the host until another
   process takes over (≤ 10 s); fault isolation (§4.1) keeps the app itself running, and
   dedicated service mode removes the exposure.
4. **File locks** behave poorly on network filesystems; the node directory must be local.
5. **Mobile devices appear once per app** unless the platform allows a shared container.
6. **Embedded relay** needs a publicly reachable address and TLS; home servers behind NAT
   may still need n0 relays.
7. **iroh API evolution** within 1.x (wire-stable, but API-level changes are possible), and
   the exact names of discovery features must be confirmed against 1.2.
8. **Large workspaces**: all-pairs dialing and whole-file transfer are sized for a handful of
   devices and files under the cap; chunking and smarter topology are deferred.
9. **Tombstones grow without GC** until tombstone GC is designed.
