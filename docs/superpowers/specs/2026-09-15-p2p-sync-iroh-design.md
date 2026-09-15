# Peer-to-peer workspace sync over iroh

- Date: 2026-09-15
- Scope: `sapphire-framework` — new crates `sapphire-framework-sync`, `-net`, `-keys`;
  changes to `-registry`, `-backend`, the facade; removal of `-rpc`, `-remote-client`,
  `-remote-server` (and `-blob`)
- Follow-ups (separate specs, in their own repositories): `sapphire-sync` (new reference
  app, first consumer), then `sapphire-timer`, `sapphire-ledger`, `sapphire-journal`,
  `sapphire-agent`
- Related issues: #83, #86, #87, #90, #92, #103, #104

## Background

The framework's stated goal is local-first applications. Today, though, the only way two
copies of a workspace converge is through a central server: `sapphire-framework-remote-server`
keeps a per-workspace change log with a server-assigned `seq`, clients track a cursor into
it over JSON-RPC-over-HTTP, and conflicts resolve last-writer-wins on the client's wall
clock. Without a server, nothing syncs.

That should not be a requirement. Clients (cli, desktop) must be able to sync with each
other directly, while a server stays useful as an always-on, well-connected peer (and relay).
The existing RPC sync is not in production, so migration cost is ignored.

[iroh](https://www.iroh.computer/) (1.0 shipped 2026-06-15, 1.2.0 current) gives us
QUIC connections dialed by public key, with hole punching, relay fallback and discovery,
and a wire-stability commitment. Its higher-level protocols (`iroh-docs`, `iroh-blobs`,
`iroh-gossip`) are still 0.10x.

What already fits a peer-to-peer model:

- **Server and client are already symmetric** (Model B): both hold files as the origin
  and a DB as a cache. A server becomes "a peer that happens to be always on".
- **Only document content travels**; vector indexes stay per-node.
- **Blobs are content-addressed.**
- **A device registry exists** (`sapphire-framework-registry`), and ledger already syncs
  `devices.toml`. A device's iroh `NodeId` slots into it.

What does not fit, and must be redesigned:

| Today | Peer-to-peer replacement |
|---|---|
| Server-assigned `seq` + client cursor | Per-replica counters + version vectors |
| LWW on client wall clock (`updated_at`) | Hybrid logical clock + replica id tie-break |
| `generation` to detect a recreated log | Unnecessary (state-based delta sync) |
| Server-only fix-ups (journal `dedupe.rs`, ledger `reconcile`) | Deterministic rules that run on every node |
| API keys (`keys.toml`) for sync | `NodeId` checked against the registry; pairing tickets |

## Decisions

Agreed during brainstorming on 2026-09-15:

1. **Serverless scope — relays.** Public n0 relays and discovery are acceptable. When a
   self-hosted server exists, it doubles as a relay so a deployment can avoid depending on
   n0 entirely.
2. **Concurrent edits.** Resolve last-writer-wins, but when two edits were *truly
   concurrent* (detected via version vectors), keep the loser as a conflict copy. Nothing is
   dropped silently. Per-file-type merge (e.g. JSONL line union, #83) is out of scope.
3. **Trust model.** All member devices are equals: any member may pair or retire devices.
   Every registry change is attributed (author device + HLC) so signature-based
   authorization can be layered on later without breaking the wire format.
4. **HTTP/JSON-RPC for sync is removed.** Everything sync-related runs over iroh. The sync
   core is transport-agnostic so another transport can be added later if ever needed.
   API keys survive for non-sync HTTP endpoints (agent's `/mcp`, `/acp`, `/a2a`).
5. **App fix-ups run on every node** as deterministic, idempotent, convergent hooks.
   Making file moves first-class (to remove the root cause of journal duplicates) is out
   of scope.
6. **Approach: iroh as transport + our own replication protocol.** `iroh-docs` was
   rejected: no concurrency detection, a shared namespace secret prevents per-device
   revocation, it keeps content in its own redb (double storage beside the file origin),
   and it is pre-1.0. `iroh-gossip` alone gives no catch-up after being offline.
7. **WASM is dropped** as a project target. Phase 4 of `ARCHITECTURE.md` and #86 steps
   D–F are out of scope.
8. **Protocols and keys are namespaced per app** so that, e.g., a journal node and an
   agent node can never sync with each other.
9. **Node identity is per app per host**, not per kind (an explicit exception to the
   per-kind layout of #129).
10. **Any file type is synced, with a size cap.** Content is addressed by hash for all
    files; whole-file transfer only (default cap 64 MiB); chunked/resumable transfer is
    deferred.
11. **A reference app, `sapphire-sync` (Syncthing-like), is built before migrating the
    existing apps.**

## 1. Crate layout

### New

| crate | role | depends on |
|---|---|---|
| `sapphire-framework-sync` | Replication core, **transport-agnostic**: wire types, `ReplicaStore` (redb), merge rules, HLC, conflict copies, `SyncFilter`, `SyncHook`, external-edit detection via `TrackStore` | serde, redb, sha2, `-track` |
| `sapphire-framework-net` | iroh `Endpoint`, protocol handlers (`sync`, `blob`, `pair`, `search`), connection authorization, discovery and relay configuration, `SyncNode` runtime, file watcher, `NodeCommand` CLI subcommands (feature `cli`), embedded relay (feature `embedded-relay`) | iroh 1.x, `-sync`, `-registry` |
| `sapphire-framework-keys` | `KeyStore` / `KeyEntry` / `protect`, moved out of `-remote-server` (#103). For non-sync HTTP endpoints | serde, toml, axum (feature) |

`-sync` and `-net` must **not** depend on `-workspace` or `-retrieve`. `sapphire-sync`
(which has no search index) verifies this.

### Changed

- **`-registry`**: one file per record (`.<app>/devices/<grain-id>.toml`,
  `.<app>/users/<grain-id>.toml`) instead of one `devices.toml` / `users.toml` (see §3.4);
  `Device` gains `node_id`. One-time migration on load.
- **`-backend`**: `RemoteBackend` is replaced by `SyncedBackend` = `LocalBackend` plus an
  optional `SyncNode` handle. "Local" and "remote" workspaces disappear as distinct things:
  every workspace is local, with zero or more peers. `WorkspaceLocator` becomes
  `Path | Ticket`. `SyncedBackend` installs an internal `SyncHook` that updates the
  `WorkspaceState` index for changed paths.
- **Facade**: features `rpc` / `remote-client` / `remote-server` become `sync` / `net` /
  `keys`.

### Removed

- `sapphire-framework-rpc`, `-remote-client`, `-remote-server`. `WsStore`'s external-edit
  detection (`record_local_write` / `reconcile`) and `is_syncable` move into `-sync` as
  behaviour common to every node.
- `sapphire-framework-blob`: its only user is the server being removed; file content is
  served from the origin (§2.3).

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

A replica is configured with
`ReplicaConfig { app_name, workspace_id: Uuid, root: PathBuf, filter: Box<dyn SyncFilter>, max_file_size: u64 /* default 64 MiB */ }`.

### 2.2 Store (redb)

Per replica:

- per path: `PathState { cur: Entry, seen: VersionVector, materialized: Option<[u8; 32]> }`
  - `cur` — the adopted version (tombstones are kept in the store; they have no file)
  - `seen` — every version of this path merged so far
  - `materialized` — hash of what was last written to (or read from) the file on disk
- node-wide version vector `vv`
- own `ReplicaId`, `counter`, last `Hlc`

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

A local write — through the API or detected on disk — produces an entry with
`dot = (self, counter + 1)`, `context = seen ∪ {cur.dot}`, `hlc = tick()`.

Detection compares the file's hash with `materialized`:

- file hash ≠ `materialized` → an external edit; record it.
- file hash = `materialized` but ≠ `cur` → a materialization was interrupted
  (the store commits before the file is written); write `cur` out again.

Writing the same content as `cur` is a no-op and assigns no dot.

### 2.6 Filtering and limits

- **`SyncFilter`** decides which paths participate. The default filter is today's
  `is_syncable` rule (hidden entries excluded, the app directory `.<app>/` allowed). Apps
  supply their own (`sapphire-sync` adds an ignore file).
- **Files over `max_file_size`** are not synced and are reported.
- **Not synced**: empty directories; symlinks (skipped with a warning).
- **Paths the local OS cannot represent** (invalid characters on Windows, case-only
  collisions) keep their entry in the store but are not materialized and are reported.
  Automatic renaming is out of scope.

Skipped paths surface as `BackendEvent::Skipped { path, reason }` and in `node status`.

### 2.7 Clock

- `tick()` advances past both the wall clock and the last HLC.
- On receipt, the HLC observes the remote value. A remote wall time more than 24 h ahead of
  the local clock is accepted but logged, and the local HLC never advances beyond
  `now + 24h`, so one badly skewed device cannot drag everyone's clock forward.

### 2.8 Out of scope

Tombstone GC (possible once every active device's reported version vector covers the
tombstone), first-class moves/renames, per-file-type merge, chunked/resumable transfer.

## 3. Transport, identity and pairing (`sapphire-framework-net`)

### 3.1 Node identity and on-disk location

Identity is **per app per host**, independent of kind: if `journal-cli` and
`journal-desktop` on one machine open the same workspace directory, they must be one
replica and one device, not two replicas fighting over the same files.

This adds one kind-independent subtree to the #129 layout:

```
<platform data root>/<app>/node/
    node.key                   # iroh secret key
    node.lock                  # held by the one process running the network node
    invites.toml               # pending pairing invites
    staging/                   # incoming file downloads
    replicas/<workspace-id>/sync.redb
```

`<platform data root>` resolves as in #129 (`SAPPHIRE_<APP>_DATA_DIR` replaces the platform
root). The replica store records the workspace root path it was opened with and refuses to
open for a different path (two copies of a workspace with the same id on one host).

**Only one process per app per host runs the network node.** Other processes read and
write files only; the running node picks their writes up as external edits.

### 3.2 Protocols

ALPNs carry the app name, taken from `AppContext` (apps never spell it):

| ALPN | purpose |
|---|---|
| `sapphire/<app>/sync/1` | replication session (§3.6) |
| `sapphire/<app>/blob/1` | `{workspace_id, hash}` → bytes; requester verifies SHA-256 |
| `sapphire/<app>/pair/1` | pairing (§3.5) |
| `sapphire/<app>/search/1` | `{workspace_id, q, limit, mode}` → hits; served only by nodes that enable it (normally a server) |

A connection with a different app's ALPN fails in the TLS handshake, before any
application code runs. Relays never see ALPNs (traffic is end-to-end encrypted), so one
relay can serve every app.

Messages are length-delimited postcard frames.

### 3.3 Endpoint configuration

- **Discovery**: n0 DNS (pkarr) and local-network mDNS, both on by default; static
  addresses may be added in config.
- **Relays**: n0 default, custom URLs, or disabled. Custom relay URLs can be set in the
  synced workspace file `.<app>/net.toml`, so installing one server makes every device use
  its relay.
- **Embedded relay** (feature `embedded-relay`, for servers): runs iroh-relay in-process;
  TLS certificates via ACME or supplied manually.

### 3.4 Authorization

- **On accept**: the remote `NodeId` must belong to a non-retired device in the registry of
  at least one workspace this node serves.
- **On `Hello`**: checked again against the target workspace's registry.
- **Revocation**: retiring a device is a registry change; as it replicates, every node
  drops and refuses that device's connections.

The registry lives in the workspace (`.<app>/devices/`, `.<app>/users/`) and is itself
synced. It moves to **one file per record**: with a single `devices.toml`, two members
pairing different devices concurrently would create a conflict copy and one device would
disappear from the effective registry. Attribution of registry changes (decision 3) comes
from the sync `Entry` (`author`, `hlc`); no extra fields in the TOML.

### 3.5 Pairing

0. **Founding device**: when a workspace is first enabled for sync, the node writes its own
   device record (with its `node_id`) as a local write, so every node has a `device_id` to
   use as `Entry.author` before its first sync.
1. **Invite** (on any member): `<app> device invite --name phone [--user <id>]` creates a
   ticket encoding `{app_name, workspace_id, inviter NodeAddr, secret: 32 random bytes,
   expires_at}` (postcard, base32, prefixed `sapphire:<app>:`). The invite is stored in
   `invites.toml`: single use, default TTL 10 minutes. A running node re-reads the file on
   each pairing attempt, so the CLI can issue invites while a daemon holds the node lock.
2. **Join** (on the new device): `<app> workspace join <ticket> [--workspace-dir <dir>]`
   connects over `pair/1` and sends the secret and the proposed device name.
3. **Admit** (inviter): verify the secret in constant time, check expiry and unused
   status, write the device record (with `node_id`; user defaults to the inviter's) as a
   local write, mark the invite used, and reply `{workspace_id, device_id}`.
4. **Sync**: the joiner lets the app create its workspace marker for `workspace_id`, then
   starts syncing. At first only the inviter knows the new device; other members accept it
   once the registry change reaches them.

### 3.6 Sync session

Over one bidirectional QUIC stream:

1. Both sides send `Hello { app_name, workspace_id, replica_id, vv }`. A mismatched app or
   workspace closes the session with a reason (logged, not retried).
2. Each side sends, in pages, every `cur` entry whose dot is not covered by the peer's `vv`.
   Content of files ≤ 64 KiB is sent inline in the same stream, avoiding a round trip per
   small file. Then `Done`.
3. Received entries are merged as they arrive (rule 1 makes redelivery harmless). The
   node-wide `vv` is updated only after the peer's `Done`; an interrupted session is simply
   resent next time.
4. Missing larger content is fetched over `blob/1`.
5. The stream stays open for live propagation (§4.2).

## 4. Node runtime

### 4.1 `SyncNode`

One per process. It:

- takes the node lock; if that fails it runs in files-only mode;
- owns one iroh `Endpoint` and a map `workspace_id → replica`;
- tracks, per session, the peer's last known version vector;
- watches the workspace roots (`notify`, debounced) plus a periodic full scan
  (default 5 min). Its own materializations match `materialized` and are not mistaken for
  external edits.

### 4.2 Connections and live propagation

- **Dial**: every non-retired device in the registry, plus mDNS-discovered nodes of the
  same workspace. Full mesh — device counts are small. Exponential backoff, max 5 min.
- **Live**: after the initial exchange, sessions stay open. Local commits are pushed to all
  connected peers. Entries received from one peer that changed state are forwarded to other
  connected peers whose known version vector does not cover them, so A → S → B propagates
  live even when A and B cannot reach each other.

### 4.3 Per kind

| kind | lifetime | behaviour |
|---|---|---|
| server | daemon | accept, dial, watch; `search/1` enabled; optional `embedded-relay` |
| desktop | while the app is open | like server without `search/1`; if the node lock is taken, show "another process is syncing" |
| cli | per invocation | ordinary commands only touch files; `<app> sync` runs a one-shot sync |

One-shot `sync`:

1. Take the node lock; if held, print that the running node handles sync and exit.
2. Scan for external edits.
3. Dial reachable peers in parallel (10 s timeout each) and run sessions until both sides
   have sent `Done`.
4. Fetch missing content, exit.

Whether a cli syncs automatically after write commands is the app's choice; the framework
only provides the call.

### 4.4 Backend

- `SyncedBackend::sync()` is a no-op while the node is live, otherwise a one-shot sync.
- New `BackendEvent`s: `PeerConnected { device_id }`, `PeerDisconnected { device_id }`,
  `Conflict { path, copy_path }`, `Skipped { path, reason }`, `NodeLocked`.

### 4.5 Errors

- Session errors are logged; the connection is dropped and redialed with backoff.
- App or workspace mismatches and unauthorized `NodeId`s are rejected with a reason and
  logged; they are not retried.
- Clock skew: §2.7.

## 5. Hooks, CLI, app migration, rollout

### 5.1 `SyncHook`

```rust
pub trait SyncHook: Send + Sync {
    /// Called after state changed (remote merge or external-edit detection).
    fn after_change(&self, ctx: &mut HookContext<'_>, changed: &[ChangedPath]) -> Result<()>;
}
// ChangedPath { path, entry: Entry, origin: Origin /* Remote | External */ }
// HookContext: read files, read other paths' Entry metadata, write/delete (= local writes)
```

Contract (documented on the trait):

- **Deterministic**: decisions depend only on replicated state — file content and entry
  `hlc` / `dot` / `author` — never on local time or the local replica id.
- **Idempotent**: running again on corrected state writes nothing.
- **Convergent**: two nodes running the hook concurrently write the same content to the
  same paths, so rule 3's identical-content case creates no conflict copies.

Framework guards: writes identical to `cur` are no-ops; if a hook's writes change state,
hooks run again, at most 3 rounds, then a warning.

### 5.2 CLI subcommands

`NodeCommand` (clap `Subcommand`, feature `cli` of `-net`): `sync`, `device invite`,
`device list`, `device retire`, `workspace join`, `node status`. Apps embed it with
`#[command(flatten)]`, like `WorkspaceArgs` (#128).

This reverses the earlier "the framework ships no subcommands" stance: pairing written four
times would drift between apps.

### 5.3 App migration requirements

Each gets its own spec in its own repository.

- **`sapphire-sync`** (new, first): Syncthing-like folder sync; the reference
  implementation and E2E test bed.
- **timer**: drop the `remote` subcommand; embed `NodeCommand`; the server runs a
  `SyncNode`.
- **ledger**: remove `sapphire-ledger-sync` and the server's `/rpc`; replace the
  token → device lookup in `identity.rs` with `NodeId` → device. `updated_by` is the local
  device for local writes and `Entry.author` for remote changes.
- **journal**: rewrite `dedupe.rs` as a `SyncHook`. The surviving file is chosen by each
  file's entry `(hlc, replica_id)`; the other is deleted from the workspace (tombstone) and
  moved to each node's local `quarantine/`. `increment_until_free` must also become
  deterministic (e.g. derive the new id from the dot) — it currently depends on the
  node's local state and can diverge. This is the largest item in journal's spec.
- **agent**: import `KeyStore` from `keys`.

### 5.4 Compatibility

- Breaking wire and crate changes: release as **0.15.0**.
- No sync data migration (not in production): existing server change logs are discarded;
  clients re-join.
- Registry file-format migration (single TOML → one file per record) **is** provided,
  because agent already authenticates against the registry. It runs once on load, is
  idempotent, and can be retried after a partial failure.

### 5.5 Implementation order (framework)

1. **Sync core**: types, redb store, merge rules, local writes, external-edit detection,
   conflict copies, `SyncFilter`, `SyncHook`. Tested with several in-memory replicas, no
   network.
2. **Registry**: one file per record + migration; `node_id`.
3. **Net basics**: `Endpoint`, `sync/1`, `blob/1`, authorization, `SyncNode`, live
   propagation, watcher.
4. **Pairing**: `pair/1`, invites, `NodeCommand`.
5. **Server features**: `search/1`, `embedded-relay`, relay URLs from `net.toml`.
6. **Cleanup**: extract `keys`; remove `rpc`, `remote-*`, `blob`; `SyncedBackend`; facade;
   rewrite the sync sections of `ARCHITECTURE.md` and mark WASM out of scope.

`sapphire-sync` can start as an E2E harness from step 3. App order afterwards:
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
- **Hooks**: a dedupe-like hook on several replicas converges; mutually rewriting hooks
  stop after 3 rounds.
- **Unrepresentable paths and size cap** produce `Skipped` and keep the entry.

### 6.2 Net integration (real iroh, in-process)

2–3 `SyncNode`s with `RelayMode::Disabled`, localhost bind, static addresses — no external
network.

- basic sync; blob fetch; inline content ≤ 64 KiB
- **A–S–B topology** (A and B not connected): live propagation through S
- rejection: unknown `NodeId`, other app's ALPN, `workspace_id` mismatch, retired device
  (connection dropped once the retirement arrives)
- pairing: success, expired, reused, wrong secret
- blob fallback: content changed on one peer, fetched from another
- lock contention: a second `SyncNode` reports `NodeLocked`

A real-relay test exists as `#[ignore]`, run manually.

### 6.3 Registry and CI

- Registry migration: idempotent, retryable after partial failure.
- CI on Linux and Windows (path rules, rename semantics). proptest case counts are reduced
  in CI.
- E2E tests belong to the `sapphire-sync` spec.

## Risks

1. **journal's id reassignment** must become deterministic; until it does, journal cannot
   migrate safely.
2. **Embedded relay** needs a publicly reachable address and TLS; home servers behind NAT
   may still need n0 relays.
3. **iroh API evolution** within 1.x (wire-stable, but API-level changes are possible),
   and the exact names of discovery features must be confirmed against 1.2.
4. **Large workspaces**: full-mesh dialing and whole-file transfer are sized for a handful of
   devices and files under the cap; chunking and smarter topology are deferred.
5. **Tombstones grow without GC** until tombstone GC is designed.
