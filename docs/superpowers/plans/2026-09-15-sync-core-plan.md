# Sync Core (`sapphire-framework-sync`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the transport-agnostic replication core — types, redb replica store, DVV-set join, external-edit detection, materialization, conflict copies, declarative filtering, missing-root guard — as the new crate `sapphire-framework-sync`, proven by unit, property-based and golden tests with in-memory replicas and no network.

**Architecture:** Pure value types (`ReplicaId`, `Dot`, `VersionVector`, `Hlc`, `ContentHash`, `Entry`) and a pure join function sit underneath a `Replica` that owns one workspace root, one redb `ReplicaStore` and a staging directory. Every change — a local file edit found by `scan()` or a `PathUpdate` received through `apply()` — becomes a join of path states; after each join the replica writes conflict copies and then materializes the winner. Replicas talk to each other only through `delta_for` / `apply` / `commit_session` and a `ContentSource` trait, so later steps can put iroh underneath without touching this crate.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), redb 4, serde + serde_json, sha2 0.11, uuid 1 (v7), grain-id 0.16, ignore 0.4, walkdir 2, thiserror 2, tracing; dev: proptest 1, tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` — §2 (replication core) and §6.1 (tests). Implementation order step 1 of §5.6.

**Branch:** `feat/p2p-sync-iroh` (exists; the spec commits are its base).

## Global Constraints

- Code, comments, commit messages and tests in **English** (CONTRIBUTING.md).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features --locked`. All three must pass after every task; commit `Cargo.lock` whenever dependencies change.
- `sapphire-framework-sync` must **not** depend on `sapphire-framework-workspace`, `-retrieve`, `-track` or any networking crate.
- Crate version follows the workspace (`version.workspace = true`); all path dependencies elsewhere use `version = "0.14.0"`.
- User-visible identifiers are grain-ids; `ReplicaId` is a UUIDv7 shown in file names via `GrainId::from_byte_suffix` over the UUID's last 5 bytes.
- Default `max_file_size` = 64 MiB (`64 * 1024 * 1024`).
- HLC drift cap = 24 h (`24 * 60 * 60 * 1000` ms).
- Store format version constant `FORMAT_VERSION = 1`.
- Winner key: `(is_not_tombstone, hlc, dot.replica, dot.counter)`, maximum wins.
- Conflict copy name: `<stem>.conflict-<grain-id>-<counter>.<ext>`, or `<name>.conflict-<grain-id>-<counter>` without an extension (a leading dot does not start an extension).
- Ignore file name: `.sapphireignore` (gitignore syntax, always synced even though it is hidden).
- Test-only APIs live behind `#[cfg(any(test, feature = "test-util"))]`; integration tests enable `test-util` through a self dev-dependency.

## File Structure

```
crates/sapphire-framework-sync/
    Cargo.toml
    src/
        lib.rs        # module wiring + re-exports
        error.rs      # Error, Result, RedbExt helper
        id.rs         # ReplicaId
        hash.rs       # ContentHash (SHA-256, hex serde)
        hlc.rs        # Hlc, Clock, SystemClock
        vv.rs         # Dot, VersionVector
        entry.rs      # Content, Entry, PathUpdate
        merge.rs      # winner, join, conflict_path, needs_copy
        paths.rs      # rel <-> native, validity, representability
        filter.rs     # SyncFilter (built-in rule + .sapphireignore)
        state.rs      # DiskState, PathState
        store.rs      # ReplicaStore (redb), Meta
        report.rs     # Report, Skipped, SkipReason, Conflict, PauseReason, ScanOutcome
        replica.rs    # Replica, ReplicaConfig, ContentSource
        testing.rs    # ManualClock, FaultPoint, MapSource (test-util)
    tests/
        common/mod.rs       # fixtures: replica(), push(), write(), tree()
        replica_basic.rs    # propagation, deletes, idempotent scan
        replica_conflict.rs # conflict copies, delete vs edit, 3-way
        replica_guards.rs   # missing root, skips, crash recovery
        convergence.rs      # proptest
        golden.rs           # format-1 behaviour pin
        golden/format-1/concurrent-edit.json
```

Modified: root `Cargo.toml` (workspace member), `crates/sapphire-framework/Cargo.toml` + `src/lib.rs` (facade feature `sync`), `Cargo.lock`.

---

## Scope of this plan

In: spec §2 entirely (types, store, join, conflict copies, local and external edits, missing-root guard, filtering and limits, clock) and the sync-core tests of §6.1.

Deliberately **not** in this plan (later implementation steps of spec §5.6):

- `group_id` / `workspace_id` on `ReplicaConfig`, `.<app>/sync-id`, share/map/unmap registration, the shared node directory, lock and roles (step 3). The replica here takes a root, a device id and a state directory; step 3 wraps it.
- iroh, sessions over the network, inline small-file content, `status.json`, the file watcher (step 4). `delta_for` / `apply` / `commit_session` / `ContentSource` are the seams step 4 plugs into.
- Tombstone GC, moves/renames, per-file-type merge, chunked transfer (spec §2.8).

Spec deviations already folded into the spec (commit `cfc2f96`): path state holds a sibling set (DVV-set join) instead of a single current entry; the mtime/size pre-filter lives in the replica store instead of `sapphire-framework-track`. This plan adds one implementation detail not spelled out in the spec — the racy-mtime check (`DiskState::checked_ns`, 2 s window), without which same-size edits on coarse-mtime filesystems would go unnoticed.

---

### Task 1: Crate scaffold and value types

**Files:**
- Create: `crates/sapphire-framework-sync/Cargo.toml`
- Create: `crates/sapphire-framework-sync/src/{lib.rs,error.rs,report.rs,id.rs,hash.rs,hlc.rs,vv.rs,entry.rs}`
- Modify: `Cargo.toml` (workspace `members`)
- Modify: `crates/sapphire-framework/Cargo.toml`, `crates/sapphire-framework/src/lib.rs` (feature `sync`)
- Test: inline `#[cfg(test)] mod tests` in `id.rs`, `hash.rs`, `hlc.rs`, `vv.rs`

**Interfaces:**
- Produces:
  - `Error` (variants below), `type Result<T>`, crate-private trait `RedbExt<T> { fn db(self) -> Result<T>; }`
  - `ReplicaId(pub Uuid)`: `new() -> Self`, `display_id(&self) -> GrainId`
  - `ContentHash(pub [u8; 32])`: `of_bytes(&[u8]) -> Self`, `of_file(&Path) -> io::Result<Self>`, `to_hex() -> String`, `FromStr`, `Display`; serde = 64-char lowercase hex string
  - `Hlc { wall_ms: u64, logical: u32 }`: `tick(self, now_ms: u64) -> Hlc`, `observe(self, remote: Hlc, now_ms: u64) -> Hlc`; const `MAX_DRIFT_MS`; trait `Clock: Send + Sync { fn now_ms(&self) -> u64 }`; `SystemClock`
  - `Dot { replica: ReplicaId, counter: u64 }` (`Ord`); `VersionVector(pub BTreeMap<ReplicaId, u64>)`: `new`, `get`, `covers_dot`, `covers`, `add_dot`, `merge`
  - `Content::{File { hash: ContentHash, len: u64 }, Tombstone}`: `hash() -> Option<ContentHash>`, `is_tombstone() -> bool`
  - `Entry { path: String, content: Content, hlc: Hlc, dot: Dot, context: VersionVector, author: GrainId }`
  - `PathUpdate { path: String, versions: Vec<Entry>, seen: VersionVector }`
  - `report`: `SkipReason`, `Skipped { path, reason }`, `Conflict { path, copy_path }`, `PauseReason::{RootMissing, MarkerMissing}`, `Report { recorded: Vec<Entry>, changed: usize, conflicts: Vec<Conflict>, skipped: Vec<Skipped> }`, `ScanOutcome::{Scanned(Report), Paused(PauseReason)}`

- [ ] **Step 1: Create the manifest and register the crate**

`crates/sapphire-framework-sync/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-sync"
version.workspace = true
edition.workspace = true
description = "Transport-agnostic peer-to-peer replication core for sapphire-framework workspaces"
license.workspace = true
repository.workspace = true
keywords = ["sync", "replication", "local-first", "p2p"]
categories = ["filesystem"]

[features]
# Deterministic clocks, fault injection and logical dumps for tests.
test-util = []

[dependencies]
grain-id.workspace = true
ignore = "0.4"
redb.workspace = true
serde.workspace = true
serde_json.workspace = true
sha2.workspace = true
thiserror.workspace = true
tracing.workspace = true
uuid.workspace = true
walkdir.workspace = true

[dev-dependencies]
proptest = "1"
sapphire-framework-sync = { path = ".", features = ["test-util"] }
tempfile = "3"
```

Root `Cargo.toml`: add `"crates/sapphire-framework-sync",` to `members` right after `"crates/sapphire-framework-track",`.

`crates/sapphire-framework/Cargo.toml`, in the `# ── modules` block:

```toml
sync = ["dep:sapphire-framework-sync"]
```

and in `[dependencies]`:

```toml
sapphire-framework-sync = { version = "0.14.0", path = "../sapphire-framework-sync", optional = true }
```

`crates/sapphire-framework/src/lib.rs`: add the doc-table row ``//! | `sync` | [`sync`] | `sapphire-framework-sync` |`` after the `track` row, and after the `track` re-export:

```rust
#[cfg(feature = "sync")]
pub use sapphire_framework_sync as sync;
```

- [ ] **Step 2: Write `error.rs` and `report.rs`**

`src/error.rs`:

```rust
//! Error type for the replication core.

use crate::report::PauseReason;

/// Errors raised by the replication core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("replica store error: {0}")]
    Store(#[from] redb::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ignore file error: {0}")]
    Ignore(#[from] ignore::Error),
    #[error("replica store format {found} is newer than this build supports ({supported})")]
    FormatTooNew { found: u32, supported: u32 },
    #[error("replica store belongs to root {stored:?}, not {requested:?}")]
    RootMismatch { stored: String, requested: String },
    #[error("replica store is corrupt: {0}")]
    Corrupt(String),
    #[error("replica is paused: {0:?}")]
    Paused(PauseReason),
    #[cfg(any(test, feature = "test-util"))]
    #[error("injected fault")]
    InjectedFault,
}

/// Result alias for the replication core.
pub type Result<T> = std::result::Result<T, Error>;

/// Converts redb's per-operation error types into [`Error::Store`].
pub(crate) trait RedbExt<T> {
    fn db(self) -> Result<T>;
}

impl<T, E: Into<redb::Error>> RedbExt<T> for std::result::Result<T, E> {
    fn db(self) -> Result<T> {
        self.map_err(|e| Error::Store(e.into()))
    }
}
```

`src/report.rs`:

```rust
//! What a scan, an apply or a fetch did.

use crate::entry::Entry;

/// Why a path was not synced on this replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// Excluded by the built-in rule or `.sapphireignore`.
    Ignored,
    TooLarge { len: u64, max: u64 },
    /// The local OS cannot represent the path.
    Unrepresentable,
    /// Another path differing only in case is already on disk.
    CaseCollision { other: String },
    Symlink,
    /// Not available locally and the content source could not provide the bytes.
    ContentUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skipped {
    pub path: String,
    pub reason: SkipReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub path: String,
    pub copy_path: String,
}

/// Why a replica refuses to scan or apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseReason {
    RootMissing,
    MarkerMissing,
}

/// Effects of one `scan`, `apply` or `fetch_missing` call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Local writes recorded, including conflict copies.
    pub recorded: Vec<Entry>,
    /// Number of path-state changes committed.
    pub changed: usize,
    pub conflicts: Vec<Conflict>,
    pub skipped: Vec<Skipped>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScanOutcome {
    Scanned(Report),
    Paused(PauseReason),
}
```

- [ ] **Step 3: Write the value types with their failing tests**

`src/id.rs`:

```rust
//! Replica identity.

use grain_id::GrainId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identity of one replica store, created once (UUIDv7). A reinstall gets a new one,
/// so dots are never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReplicaId(pub Uuid);

impl ReplicaId {
    /// A fresh time-ordered id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// The grain-id shown in file names. A UUIDv7 starts with a timestamp, so the
    /// trailing random bytes are used.
    pub fn display_id(&self) -> GrainId {
        let bytes = self.0.as_bytes();
        let tail: &[u8; 5] = bytes[11..16].try_into().expect("a UUID has 16 bytes");
        GrainId::from_byte_suffix(tail)
    }
}

impl Default for ReplicaId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_id_uses_the_uuid_suffix() {
        let max = ReplicaId(Uuid::from_u128(0x0190_0000_0000_7000_8000_00ff_ffff_ffff));
        let nil = ReplicaId(Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000));
        assert_eq!(max.display_id(), GrainId::MAX);
        assert_eq!(nil.display_id(), GrainId::NIL);
    }

    #[test]
    fn ids_created_together_rarely_share_a_display_id() {
        let ids: std::collections::HashSet<_> =
            (0..1000).map(|_| ReplicaId::new().display_id()).collect();
        assert!(ids.len() > 990);
    }
}
```

`src/hash.rs`:

```rust
//! Content addressing.

use std::fmt;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// SHA-256 of a file's bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash(pub [u8; 32]);

/// A string that is not 64 hex digits.
#[derive(Debug, thiserror::Error)]
#[error("invalid content hash")]
pub struct ParseHashError;

impl ContentHash {
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn of_file(path: &Path) -> std::io::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(Self(hasher.finalize().into()))
    }

    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentHash({})", self.to_hex())
    }
}

impl FromStr for ContentHash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(ParseHashError);
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            let pair = s.get(2 * i..2 * i + 2).ok_or(ParseHashError)?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| ParseHashError)?;
        }
        Ok(Self(out))
    }
}

impl Serialize for ContentHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_sha256() {
        assert_eq!(
            ContentHash::of_bytes(b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn file_hash_matches_bytes_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let data = vec![7u8; 200_000];
        std::fs::write(&path, &data).unwrap();
        assert_eq!(ContentHash::of_file(&path).unwrap(), ContentHash::of_bytes(&data));
    }

    #[test]
    fn hex_round_trips_through_serde() {
        let h = ContentHash::of_bytes(b"x");
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, format!("\"{}\"", h.to_hex()));
        assert_eq!(serde_json::from_str::<ContentHash>(&json).unwrap(), h);
        assert!("zz".parse::<ContentHash>().is_err());
    }
}
```

`src/hlc.rs`:

```rust
//! Hybrid logical clock.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// How far ahead of the local wall clock a remote timestamp may pull the local clock.
pub const MAX_DRIFT_MS: u64 = 24 * 60 * 60 * 1000;

/// Hybrid logical clock value, ordered by wall time and then the logical counter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hlc {
    pub wall_ms: u64,
    pub logical: u32,
}

/// Source of wall-clock time, injectable for tests.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

impl Hlc {
    /// The timestamp of a new local event; strictly greater than `self`.
    pub fn tick(self, now_ms: u64) -> Hlc {
        if now_ms > self.wall_ms {
            Hlc { wall_ms: now_ms, logical: 0 }
        } else if self.logical == u32::MAX {
            Hlc { wall_ms: self.wall_ms + 1, logical: 0 }
        } else {
            Hlc { wall_ms: self.wall_ms, logical: self.logical + 1 }
        }
    }

    /// Fold a received timestamp into the local clock. A remote wall time beyond
    /// `now + MAX_DRIFT_MS` is logged and clamped, so one skewed device cannot drag
    /// every clock forward.
    pub fn observe(self, remote: Hlc, now_ms: u64) -> Hlc {
        let cap = now_ms.saturating_add(MAX_DRIFT_MS);
        let remote = if remote.wall_ms > cap {
            tracing::warn!(
                remote_wall_ms = remote.wall_ms,
                now_ms,
                "remote clock is more than 24h ahead; clamping"
            );
            Hlc { wall_ms: cap, logical: 0 }
        } else {
            remote
        };
        self.max(remote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_strictly_monotonic_even_when_the_wall_clock_stalls() {
        let a = Hlc::default().tick(100);
        let b = a.tick(100);
        let c = b.tick(50);
        assert!(a < b && b < c);
        assert_eq!(a, Hlc { wall_ms: 100, logical: 0 });
        assert_eq!(c, Hlc { wall_ms: 100, logical: 2 });
    }

    #[test]
    fn observe_takes_the_later_clock() {
        let local = Hlc { wall_ms: 10, logical: 3 };
        let remote = Hlc { wall_ms: 20, logical: 1 };
        assert_eq!(local.observe(remote, 15), remote);
        assert!(local.observe(remote, 15).tick(15) > remote);
    }

    #[test]
    fn observe_clamps_far_future_remotes() {
        let remote = Hlc { wall_ms: 1_000 + MAX_DRIFT_MS + 5, logical: 0 };
        let got = Hlc::default().observe(remote, 1_000);
        assert_eq!(got, Hlc { wall_ms: 1_000 + MAX_DRIFT_MS, logical: 0 });
    }
}
```

`src/vv.rs`:

```rust
//! Dots and version vectors.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::id::ReplicaId;

/// One event: the `counter`-th write recorded by `replica`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub replica: ReplicaId,
    pub counter: u64,
}

/// For each replica, the highest counter covered.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionVector(pub BTreeMap<ReplicaId, u64>);

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, replica: &ReplicaId) -> u64 {
        self.0.get(replica).copied().unwrap_or(0)
    }

    pub fn covers_dot(&self, dot: &Dot) -> bool {
        self.get(&dot.replica) >= dot.counter
    }

    pub fn covers(&self, other: &VersionVector) -> bool {
        other.0.iter().all(|(r, c)| self.get(r) >= *c)
    }

    pub fn add_dot(&mut self, dot: &Dot) {
        let slot = self.0.entry(dot.replica).or_insert(0);
        *slot = (*slot).max(dot.counter);
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (r, c) in &other.0 {
            let slot = self.0.entry(*r).or_insert(0);
            *slot = (*slot).max(*c);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn r(n: u128) -> ReplicaId {
        ReplicaId(Uuid::from_u128(n))
    }

    #[test]
    fn cover_and_merge() {
        let mut a = VersionVector::new();
        a.add_dot(&Dot { replica: r(1), counter: 3 });
        let mut b = VersionVector::new();
        b.add_dot(&Dot { replica: r(2), counter: 1 });
        assert!(a.covers_dot(&Dot { replica: r(1), counter: 2 }));
        assert!(!a.covers_dot(&Dot { replica: r(2), counter: 1 }));
        assert!(!a.covers(&b));
        a.merge(&b);
        assert!(a.covers(&b));
        a.add_dot(&Dot { replica: r(1), counter: 1 });
        assert_eq!(a.get(&r(1)), 3, "add_dot never lowers a counter");
    }

    #[test]
    fn serializes_as_a_json_object_keyed_by_uuid() {
        let mut v = VersionVector::new();
        v.add_dot(&Dot { replica: r(1), counter: 2 });
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#"{"00000000-0000-0000-0000-000000000001":2}"#);
        assert_eq!(serde_json::from_str::<VersionVector>(&json).unwrap(), v);
    }
}
```

`src/entry.rs`:

```rust
//! Versions of a path and the unit of replication.

use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::hlc::Hlc;
use crate::vv::{Dot, VersionVector};

/// What a version holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Content {
    File { hash: ContentHash, len: u64 },
    Tombstone,
}

impl Content {
    pub fn hash(&self) -> Option<ContentHash> {
        match self {
            Content::File { hash, .. } => Some(*hash),
            Content::Tombstone => None,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, Content::Tombstone)
    }
}

/// One version of one path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Workspace-relative path with POSIX separators.
    pub path: String,
    pub content: Content,
    pub hlc: Hlc,
    pub dot: Dot,
    /// Versions of this path the writer's file was based on.
    pub context: VersionVector,
    /// Device id of the writer. Display and audit only; never used for merging.
    pub author: GrainId,
}

/// A path's replicated state: its sibling versions and everything merged so far.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathUpdate {
    pub path: String,
    pub versions: Vec<Entry>,
    pub seen: VersionVector,
}
```

`src/lib.rs` (later tasks add modules and re-exports):

```rust
//! Transport-agnostic replication core for sapphire-framework workspaces.
//!
//! See `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md`, section 2.

mod entry;
mod error;
mod hash;
mod hlc;
mod id;
mod report;
mod vv;

pub use entry::{Content, Entry, PathUpdate};
pub use error::{Error, Result};
pub use hash::{ContentHash, ParseHashError};
pub use hlc::{Clock, Hlc, MAX_DRIFT_MS, SystemClock};
pub use id::ReplicaId;
pub use report::{Conflict, PauseReason, Report, ScanOutcome, SkipReason, Skipped};
pub use vv::{Dot, VersionVector};
```

`RedbExt` is unused until Task 4; add `#[allow(dead_code)]` on the trait now and remove it in Task 4.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p sapphire-framework-sync`
Expected: 10 tests pass (`id` 2, `hash` 3, `hlc` 3, `vv` 2).
If `Sha256::digest(..).into()` does not convert to `[u8; 32]` under sha2 0.11, write `Sha256::digest(bytes).as_slice().try_into().expect("32 bytes")` (and the same for `finalize()`).

- [ ] **Step 5: Lint, format, commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings && cargo check -p sapphire-framework --features sync`
Expected: no warnings, no errors.

```bash
git add Cargo.toml Cargo.lock crates/sapphire-framework-sync crates/sapphire-framework
git commit -m "feat(sync): scaffold sapphire-framework-sync with value types"
```

---

### Task 2: DVV-set join, winner and conflict-copy naming

**Files:**
- Create: `crates/sapphire-framework-sync/src/merge.rs`
- Modify: `crates/sapphire-framework-sync/src/lib.rs` (`mod merge;` and re-exports)
- Test: inline `#[cfg(test)] mod tests` in `merge.rs`

**Interfaces:**
- Consumes: `Entry`, `Content`, `Dot`, `VersionVector`, `Hlc`, `ReplicaId` (Task 1)
- Produces:
  - `pub fn winner(versions: &[Entry]) -> &Entry` (panics on an empty slice)
  - `pub fn join(local: Option<(&[Entry], &VersionVector)>, incoming: &[Entry], incoming_seen: &VersionVector) -> Option<(Vec<Entry>, VersionVector)>` — `None` means "no change"; returned versions are sorted by `dot`
  - `pub fn needs_copy(loser: &Entry, winner: &Entry) -> bool`
  - `pub fn conflict_path(path: &str, loser: &Dot) -> String`

Background for the implementer: a path's state is a *set* of sibling versions plus `seen`. A version survives a join unless the other side's `seen` covers its dot and the other side no longer has it. This is the DVV-set join (spec §2.4); it is commutative, associative and idempotent, which the tests below check directly.

- [ ] **Step 1: Write the failing tests**

Create `src/merge.rs` with only the tests module and the `use` lines, so the tests fail to compile:

```rust
//! Joining path states and choosing what is written to disk.

use crate::entry::Entry;
use crate::vv::{Dot, VersionVector};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Content;
    use crate::hash::ContentHash;
    use crate::hlc::Hlc;
    use crate::id::ReplicaId;
    use grain_id::GrainId;
    use uuid::Uuid;

    fn rid(n: u128) -> ReplicaId {
        ReplicaId(Uuid::from_u128(n))
    }

    fn dot(r: u128, c: u64) -> Dot {
        Dot { replica: rid(r), counter: c }
    }

    fn vv(dots: &[Dot]) -> VersionVector {
        let mut v = VersionVector::new();
        for d in dots {
            v.add_dot(d);
        }
        v
    }

    fn file(d: Dot, wall: u64, body: &str, ctx: &[Dot]) -> Entry {
        Entry {
            path: "a.txt".into(),
            content: Content::File { hash: ContentHash::of_bytes(body.as_bytes()), len: body.len() as u64 },
            hlc: Hlc { wall_ms: wall, logical: 0 },
            dot: d,
            context: vv(ctx),
            author: GrainId::NIL,
        }
    }

    fn tomb(d: Dot, wall: u64, ctx: &[Dot]) -> Entry {
        Entry { content: Content::Tombstone, ..file(d, wall, "", ctx) }
    }

    /// A state as (versions, seen), built the way a local write would build it.
    fn written(e: Entry) -> (Vec<Entry>, VersionVector) {
        let mut seen = e.context.clone();
        seen.add_dot(&e.dot);
        (vec![e], seen)
    }

    fn j(a: &(Vec<Entry>, VersionVector), b: &(Vec<Entry>, VersionVector)) -> (Vec<Entry>, VersionVector) {
        join(Some((&a.0, &a.1)), &b.0, &b.1).unwrap_or_else(|| a.clone())
    }

    #[test]
    fn into_nothing_takes_the_incoming_state() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        assert_eq!(join(None, &x.0, &x.1), Some(x.clone()));
    }

    #[test]
    fn a_descendant_replaces_its_ancestor() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let y = written(file(dot(2, 1), 20, "y", &[dot(1, 1)]));
        let got = j(&x, &y);
        assert_eq!(got, y);
        // and the ancestor never comes back
        assert_eq!(join(Some((&got.0, &got.1)), &x.0, &x.1), None);
    }

    #[test]
    fn concurrent_versions_become_siblings() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let y = written(file(dot(2, 1), 20, "y", &[]));
        let got = j(&x, &y);
        assert_eq!(got.0.len(), 2);
        assert_eq!(got.1, vv(&[dot(1, 1), dot(2, 1)]));
        assert_eq!(winner(&got.0).dot, dot(2, 1), "later hlc wins");
    }

    #[test]
    fn an_edit_beats_a_concurrent_delete_even_if_older() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let d = written(tomb(dot(2, 1), 99, &[]));
        let got = j(&x, &d);
        assert_eq!(winner(&got.0).dot, dot(1, 1));
    }

    #[test]
    fn join_is_idempotent() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let y = written(file(dot(2, 1), 20, "y", &[]));
        let s = j(&x, &y);
        assert_eq!(join(Some((&s.0, &s.1)), &s.0, &s.1), None);
    }

    /// The case a single-winner merge gets wrong: B deletes after seeing X, C edits
    /// concurrently, and the winner key is not monotonic along causality.
    #[test]
    fn join_is_associative_and_commutative_with_deletes() {
        let a = written(file(dot(1, 1), 50, "x", &[]));
        let b = written(tomb(dot(2, 1), 60, &[dot(1, 1)]));
        let c = written(file(dot(3, 1), 40, "z", &[]));
        let orders = [
            j(&j(&a, &b), &c),
            j(&a, &j(&b, &c)),
            j(&j(&a, &c), &b),
            j(&j(&c, &a), &b),
            j(&j(&b, &c), &a),
        ];
        for got in &orders[1..] {
            assert_eq!(got, &orders[0]);
        }
        let dots: Vec<Dot> = orders[0].0.iter().map(|e| e.dot).collect();
        assert_eq!(dots, vec![dot(2, 1), dot(3, 1)], "X is superseded; the delete and Z are siblings");
        assert_eq!(winner(&orders[0].0).dot, dot(3, 1));
    }

    #[test]
    fn needs_copy_only_for_differing_live_losers() {
        let w = file(dot(1, 1), 10, "same", &[]);
        assert!(!needs_copy(&file(dot(2, 1), 5, "same", &[]), &w));
        assert!(!needs_copy(&tomb(dot(2, 1), 5, &[]), &w));
        assert!(needs_copy(&file(dot(2, 1), 5, "other", &[]), &w));
    }

    #[test]
    fn conflict_path_naming() {
        let d = Dot { replica: ReplicaId(Uuid::from_u128(0x0190_0000_0000_7000_8000_00ff_ffff_ffff)), counter: 42 };
        let g = GrainId::MAX.to_string();
        assert_eq!(conflict_path("note.md", &d), format!("note.conflict-{g}-42.md"));
        assert_eq!(conflict_path("dir/a.tar.gz", &d), format!("dir/a.tar.conflict-{g}-42.gz"));
        assert_eq!(conflict_path("dir/Makefile", &d), format!("dir/Makefile.conflict-{g}-42"));
        assert_eq!(conflict_path(".sapphireignore", &d), format!(".sapphireignore.conflict-{g}-42"));
    }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Add `mod merge;` to `lib.rs`. Run: `cargo test -p sapphire-framework-sync merge`
Expected: compile errors — `join`, `winner`, `needs_copy`, `conflict_path` not found.

- [ ] **Step 3: Implement**

Insert above the tests module in `src/merge.rs`:

```rust
use crate::hlc::Hlc;
use crate::id::ReplicaId;

/// Ordering key of a version: edits beat deletes, then later clock, then replica and
/// counter as tie-breakers. The maximum is the winner.
fn winner_key(e: &Entry) -> (bool, Hlc, ReplicaId, u64) {
    (!e.content.is_tombstone(), e.hlc, e.dot.replica, e.dot.counter)
}

/// The sibling whose content belongs on disk.
pub fn winner(versions: &[Entry]) -> &Entry {
    versions
        .iter()
        .max_by_key(|e| winner_key(e))
        .expect("a path state always has at least one version")
}

/// DVV-set join of a local state with an incoming one. Returns the joined
/// `(versions, seen)`, or `None` when the local state already contains everything.
pub fn join(
    local: Option<(&[Entry], &VersionVector)>,
    incoming: &[Entry],
    incoming_seen: &VersionVector,
) -> Option<(Vec<Entry>, VersionVector)> {
    let Some((local, local_seen)) = local else {
        let mut versions = incoming.to_vec();
        versions.sort_by_key(|e| e.dot);
        return Some((versions, incoming_seen.clone()));
    };

    let mut versions: Vec<Entry> = local
        .iter()
        .filter(|e| !incoming_seen.covers_dot(&e.dot) || incoming.iter().any(|i| i.dot == e.dot))
        .cloned()
        .collect();
    for e in incoming {
        let keep = !local_seen.covers_dot(&e.dot) || local.iter().any(|l| l.dot == e.dot);
        if keep && !versions.iter().any(|v| v.dot == e.dot) {
            versions.push(e.clone());
        }
    }
    versions.sort_by_key(|e| e.dot);

    let mut seen = local_seen.clone();
    seen.merge(incoming_seen);

    if versions.is_empty() {
        // Impossible for well-formed states: each side would have to know of a version
        // superseding everything the other holds. Keep the local state rather than
        // leave a path with no version.
        tracing::error!("join produced an empty sibling set; keeping the local state");
        return None;
    }
    if versions.as_slice() == local && seen == *local_seen {
        return None;
    }
    Some((versions, seen))
}

/// Whether `loser` needs a conflict copy next to `winner`.
pub fn needs_copy(loser: &Entry, winner: &Entry) -> bool {
    !loser.content.is_tombstone() && loser.content.hash() != winner.content.hash()
}

/// `<stem>.conflict-<grain-id>-<counter>.<ext>`, or `<name>.conflict-<grain-id>-<counter>`
/// when the name has no extension. A leading dot does not start an extension.
pub fn conflict_path(path: &str, loser: &Dot) -> String {
    let (dir, name) = match path.rfind('/') {
        Some(i) => (&path[..=i], &path[i + 1..]),
        None => ("", path),
    };
    let tag = format!("conflict-{}-{}", loser.replica.display_id(), loser.counter);
    match name.rfind('.') {
        Some(i) if i > 0 => format!("{dir}{}.{tag}.{}", &name[..i], &name[i + 1..]),
        _ => format!("{dir}{name}.{tag}"),
    }
}
```

Add to `lib.rs`: `pub use merge::{conflict_path, join, needs_copy, winner};`

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test -p sapphire-framework-sync merge`
Expected: 8 tests pass.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync
git commit -m "feat(sync): DVV-set join, winner selection and conflict-copy naming"
```

---

### Task 3: Paths and declarative filtering

**Files:**
- Create: `crates/sapphire-framework-sync/src/paths.rs`
- Create: `crates/sapphire-framework-sync/src/filter.rs`
- Modify: `crates/sapphire-framework-sync/src/lib.rs`
- Test: inline tests in both files

**Interfaces:**
- Produces:
  - `paths::to_native(root: &Path, rel: &str) -> PathBuf`
  - `paths::rel_from_native(root: &Path, abs: &Path) -> Option<String>` (`None` for the root itself, non-UTF-8 names, or paths outside `root`)
  - `paths::is_valid_rel(rel: &str) -> bool` (rejects empty, absolute, backslashes, a drive prefix in the first segment, empty / `.` / `..` segments)
  - `paths::representable(rel: &str) -> bool`, `paths::representable_on(rel: &str, windows: bool) -> bool`
  - `paths::CASE_INSENSITIVE_FS: bool`
  - `filter::IGNORE_FILE: &str = ".sapphireignore"`
  - `SyncFilter::load(root: &Path, app_name: &str) -> Result<SyncFilter>`, `SyncFilter::allows(&self, rel: &str, is_dir: bool) -> bool`

Rule (spec §2.6): hidden segments are excluded except the app directory `.<app_name>` at any depth; `.sapphireignore` at the root is always allowed; everything else is then matched against `.sapphireignore` with gitignore semantics.

- [ ] **Step 1: Write the failing tests**

`src/paths.rs` (tests first, implementation in Step 3):

```rust
//! Workspace-relative paths.

use std::path::{Component, Path, PathBuf};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_round_trip() {
        let root = Path::new("root");
        let abs = to_native(root, "a/b/c.txt");
        assert_eq!(abs, Path::new("root").join("a").join("b").join("c.txt"));
        assert_eq!(rel_from_native(root, &abs).as_deref(), Some("a/b/c.txt"));
        assert_eq!(rel_from_native(root, root), None);
        assert_eq!(rel_from_native(root, Path::new("elsewhere/x")), None);
    }

    #[test]
    fn validity() {
        for ok in ["a", "a/b.md", "2026-08-25T10:00.md", ".app/x"] {
            assert!(is_valid_rel(ok), "{ok}");
        }
        for bad in ["", "/a", "a\\b", "C:/x", "c:x", "a//b", "./a", "a/../b", "a/."] {
            assert!(!is_valid_rel(bad), "{bad}");
        }
    }

    #[test]
    fn windows_representability() {
        assert!(representable_on("a/b.txt", true));
        for bad in ["a:b.txt", "x/what?.md", "trailing.", "space ", "CON", "con.txt", "dir/LPT1.log", "a\u{1}b"] {
            assert!(!representable_on(bad, true), "{bad}");
            assert!(representable_on(bad, false) || bad.contains('\u{0}'), "{bad} is fine elsewhere");
        }
        assert!(representable_on("COM0", true), "COM0 is not reserved");
        assert!(representable_on("CONSOLE.txt", true));
        assert!(!representable_on("nul\u{0}", false));
    }
}
```

`src/filter.rs`:

```rust
//! Which paths take part in sync.

use std::path::Path;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::error::Result;
use crate::paths;

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_with(ignore: Option<&str>) -> (tempfile::TempDir, SyncFilter) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(body) = ignore {
            std::fs::write(dir.path().join(IGNORE_FILE), body).unwrap();
        }
        let f = SyncFilter::load(dir.path(), "test-app").unwrap();
        (dir, f)
    }

    #[test]
    fn built_in_rule() {
        let (_d, f) = filter_with(None);
        assert!(f.allows("notes/a.md", false));
        assert!(f.allows(".test-app", true));
        assert!(f.allows(".test-app/sync-id", false));
        assert!(f.allows("sub/.test-app/x", false));
        assert!(f.allows(IGNORE_FILE, false));
        assert!(!f.allows(".git/config", false));
        assert!(!f.allows("a/.hidden", false));
        assert!(!f.allows(".other-app/x", false));
        assert!(!f.allows("../escape", false));
    }

    #[test]
    fn ignore_file_patterns() {
        let (_d, f) = filter_with(Some("*.tmp\nbuild/\n!keep.tmp\n"));
        assert!(!f.allows("a.tmp", false));
        assert!(f.allows("keep.tmp", false));
        assert!(!f.allows("build", true));
        assert!(!f.allows("build/out.bin", false));
        assert!(f.allows("src/main.rs", false));
    }

    #[test]
    fn the_ignore_file_itself_cannot_be_ignored() {
        let (_d, f) = filter_with(Some("*\n"));
        assert!(f.allows(IGNORE_FILE, false));
        assert!(!f.allows("anything", false));
    }
}
```

Add `pub mod paths;` and `mod filter;` to `lib.rs`.

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test -p sapphire-framework-sync paths filter`
Expected: compile errors for the missing functions and `SyncFilter`.

- [ ] **Step 3: Implement**

Insert above the tests module in `src/paths.rs`:

```rust
/// Whether the local filesystem is assumed case-insensitive.
pub const CASE_INSENSITIVE_FS: bool = cfg!(any(windows, target_os = "macos"));

/// Join a POSIX workspace-relative path onto `root`.
pub fn to_native(root: &Path, rel: &str) -> PathBuf {
    let mut out = root.to_path_buf();
    for segment in rel.split('/') {
        out.push(segment);
    }
    out
}

/// The POSIX workspace-relative form of `abs`.
pub fn rel_from_native(root: &Path, abs: &Path) -> Option<String> {
    let rel = abs.strip_prefix(root).ok()?;
    let mut parts = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(s) => parts.push(s.to_str()?.to_owned()),
            _ => return None,
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// A well-formed workspace-relative POSIX path that cannot escape the root.
pub fn is_valid_rel(rel: &str) -> bool {
    if rel.is_empty() || rel.starts_with('/') || rel.contains('\\') {
        return false;
    }
    let first = rel.split('/').next().unwrap_or_default().as_bytes();
    if first.len() >= 2 && first[0].is_ascii_alphabetic() && first[1] == b':' {
        return false;
    }
    rel.split('/').all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

/// Whether this OS can hold a file at `rel`.
pub fn representable(rel: &str) -> bool {
    representable_on(rel, cfg!(windows))
}

/// Whether an OS (Windows when `windows`, otherwise POSIX) can hold a file at `rel`.
pub fn representable_on(rel: &str, windows: bool) -> bool {
    if rel.contains('\0') {
        return false;
    }
    if !windows {
        return true;
    }
    rel.split('/').all(|seg| {
        !seg.chars().any(|c| matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*') || (c as u32) < 32)
            && !seg.ends_with('.')
            && !seg.ends_with(' ')
            && !is_reserved_windows_name(seg)
    })
}

fn is_reserved_windows_name(segment: &str) -> bool {
    let stem = segment.split('.').next().unwrap_or(segment).to_ascii_uppercase();
    match stem.as_str() {
        "CON" | "PRN" | "AUX" | "NUL" => true,
        s if s.len() == 4 && (s.starts_with("COM") || s.starts_with("LPT")) => {
            matches!(s.as_bytes()[3], b'1'..=b'9')
        }
        _ => false,
    }
}
```

Insert above the tests module in `src/filter.rs`:

```rust
/// Name of the per-workspace ignore file.
pub const IGNORE_FILE: &str = ".sapphireignore";

/// Declarative sync filter: the built-in rule for an app name plus `.sapphireignore`.
pub struct SyncFilter {
    app_dir: String,
    ignore: Option<Gitignore>,
}

impl SyncFilter {
    /// Build the filter for `root`, reading `.sapphireignore` if present.
    pub fn load(root: &Path, app_name: &str) -> Result<Self> {
        let file = root.join(IGNORE_FILE);
        let ignore = if file.is_file() {
            let mut builder = GitignoreBuilder::new(root);
            if let Some(err) = builder.add(&file) {
                return Err(err.into());
            }
            Some(builder.build()?)
        } else {
            None
        };
        Ok(Self { app_dir: format!(".{app_name}"), ignore })
    }

    /// Whether `rel` takes part in sync.
    pub fn allows(&self, rel: &str, is_dir: bool) -> bool {
        if !paths::is_valid_rel(rel) {
            return false;
        }
        if rel == IGNORE_FILE {
            return true;
        }
        if !rel.split('/').all(|seg| !seg.starts_with('.') || seg == self.app_dir) {
            return false;
        }
        match &self.ignore {
            Some(gi) => !gi
                .matched_path_or_any_parents(paths::to_native(Path::new(""), rel), is_dir)
                .is_ignore(),
            None => true,
        }
    }
}
```

`lib.rs`: add `pub use filter::{IGNORE_FILE, SyncFilter};`

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test -p sapphire-framework-sync paths filter`
Expected: 6 tests pass.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync Cargo.lock
git commit -m "feat(sync): path validity, representability and declarative filter"
```

---

### Task 4: Path state and the redb replica store

**Files:**
- Create: `crates/sapphire-framework-sync/src/state.rs`
- Create: `crates/sapphire-framework-sync/src/store.rs`
- Modify: `crates/sapphire-framework-sync/src/lib.rs`, `src/error.rs` (drop the `#[allow(dead_code)]` on `RedbExt`)
- Test: inline tests in `store.rs`

**Interfaces:**
- Consumes: `Entry`, `VersionVector`, `ContentHash`, `Hlc`, `ReplicaId`, `merge::winner`, `Error`, `RedbExt`
- Produces:
  - `DiskState { hash: Option<ContentHash>, seen: VersionVector, mtime_ns: i64, len: u64, checked_ns: i64 }` (`Default`)
  - `PathState { versions: Vec<Entry>, seen: VersionVector, disk: DiskState }` with `winner(&self) -> &Entry`
  - `const FORMAT_VERSION: u32 = 1`
  - `Meta { format_version: u32, replica_id: ReplicaId, counter: u64, hlc: Hlc, vv: VersionVector, root: String }`
  - `ReplicaStore::open(path: &Path, root: &str) -> Result<Self>`
  - `ReplicaStore::open_with_id(path: &Path, root: &str, id: Option<ReplicaId>) -> Result<Self>` (id used only when creating)
  - `ReplicaStore::meta(&self) -> Result<Meta>`
  - `ReplicaStore::get(&self, path: &str) -> Result<Option<PathState>>`
  - `ReplicaStore::all(&self) -> Result<Vec<(String, PathState)>>` (sorted by path)
  - `ReplicaStore::any_materialized(&self) -> Result<bool>`
  - `ReplicaStore::commit(&self, meta: &Meta, states: &[(String, PathState)]) -> Result<()>` (one transaction)

- [ ] **Step 1: Write `state.rs`**

```rust
//! Per-path state kept by a replica.

use serde::{Deserialize, Serialize};

use crate::entry::Entry;
use crate::hash::ContentHash;
use crate::merge;
use crate::vv::VersionVector;

/// What is on disk for a path.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskState {
    /// Hash of the file; `None` when there is no file.
    pub hash: Option<ContentHash>,
    /// Versions the file on disk reflects. Context of the next local edit.
    pub seen: VersionVector,
    /// Change-detection pre-filter: mtime (ns since the epoch) and size when recorded.
    pub mtime_ns: i64,
    pub len: u64,
    /// Wall-clock time (ns since the epoch) the stamp was taken. A file whose mtime is
    /// within two seconds of this is "racy": filesystems with coarse mtime granularity
    /// could hide a same-size edit, so it is re-hashed instead of trusted.
    pub checked_ns: i64,
}

/// A path's sibling versions, everything merged so far, and what is on disk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathState {
    pub versions: Vec<Entry>,
    pub seen: VersionVector,
    pub disk: DiskState,
}

impl PathState {
    pub fn winner(&self) -> &Entry {
        merge::winner(&self.versions)
    }
}
```

- [ ] **Step 2: Write the failing store tests**

`src/store.rs`:

```rust
//! Replica store (redb): one table of path states and one metadata record.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::{Error, RedbExt, Result};
use crate::hlc::Hlc;
use crate::id::ReplicaId;
use crate::state::PathState;
use crate::vv::VersionVector;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{Content, Entry};
    use crate::hash::ContentHash;
    use crate::state::DiskState;
    use crate::vv::Dot;
    use grain_id::GrainId;

    fn state(id: ReplicaId, body: &str, on_disk: bool) -> PathState {
        let e = Entry {
            path: "a.txt".into(),
            content: Content::File { hash: ContentHash::of_bytes(body.as_bytes()), len: 1 },
            hlc: Hlc::default(),
            dot: Dot { replica: id, counter: 1 },
            context: VersionVector::new(),
            author: GrainId::NIL,
        };
        let mut seen = VersionVector::new();
        seen.add_dot(&e.dot);
        let disk = DiskState { hash: on_disk.then(|| ContentHash::of_bytes(body.as_bytes())), ..DiskState::default() };
        PathState { versions: vec![e], seen, disk }
    }

    #[test]
    fn creates_meta_once_and_persists_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sync.redb");
        let (id, s) = {
            let store = ReplicaStore::open(&path, "/root").unwrap();
            let mut meta = store.meta().unwrap();
            assert_eq!(meta.format_version, FORMAT_VERSION);
            assert_eq!(meta.counter, 0);
            meta.counter = 7;
            let s = state(meta.replica_id, "x", false);
            store.commit(&meta, &[("a.txt".into(), s.clone())]).unwrap();
            (meta.replica_id, s)
        };
        let store = ReplicaStore::open(&path, "/root").unwrap();
        let meta = store.meta().unwrap();
        assert_eq!((meta.replica_id, meta.counter), (id, 7));
        assert_eq!(store.get("a.txt").unwrap(), Some(s.clone()));
        assert_eq!(store.get("b.txt").unwrap(), None);
        assert_eq!(store.all().unwrap(), vec![("a.txt".to_string(), s)]);
        assert!(!store.any_materialized().unwrap());
    }

    #[test]
    fn any_materialized_sees_files_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = ReplicaStore::open(&dir.path().join("s.redb"), "/r").unwrap();
        let meta = store.meta().unwrap();
        store.commit(&meta, &[("a.txt".into(), state(meta.replica_id, "x", true))]).unwrap();
        assert!(store.any_materialized().unwrap());
    }

    #[test]
    fn refuses_another_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        drop(ReplicaStore::open(&path, "/one").unwrap());
        assert!(matches!(ReplicaStore::open(&path, "/two"), Err(Error::RootMismatch { .. })));
    }

    #[test]
    fn refuses_a_newer_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        {
            let store = ReplicaStore::open(&path, "/r").unwrap();
            let mut meta = store.meta().unwrap();
            meta.format_version = FORMAT_VERSION + 1;
            store.commit(&meta, &[]).unwrap();
        }
        assert!(matches!(
            ReplicaStore::open(&path, "/r"),
            Err(Error::FormatTooNew { found, .. }) if found == FORMAT_VERSION + 1
        ));
    }

    #[test]
    fn open_with_id_uses_the_id_only_on_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        let fixed = ReplicaId(uuid::Uuid::from_u128(5));
        let store = ReplicaStore::open_with_id(&path, "/r", Some(fixed)).unwrap();
        assert_eq!(store.meta().unwrap().replica_id, fixed);
        drop(store);
        let other = ReplicaId(uuid::Uuid::from_u128(6));
        let store = ReplicaStore::open_with_id(&path, "/r", Some(other)).unwrap();
        assert_eq!(store.meta().unwrap().replica_id, fixed);
    }
}
```

Add `mod state;` and `mod store;` to `lib.rs`.

- [ ] **Step 3: Run the tests to see them fail**

Run: `cargo test -p sapphire-framework-sync store`
Expected: compile errors (`ReplicaStore`, `Meta`, `FORMAT_VERSION` missing).

- [ ] **Step 4: Implement**

Insert above the tests module in `src/store.rs`:

```rust
/// On-disk format of the replica store. Bump for any change in stored data *or* in
/// merge behaviour (spec §5.4).
pub const FORMAT_VERSION: u32 = 1;

const PATHS: TableDefinition<&str, &[u8]> = TableDefinition::new("paths");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const META_KEY: &str = "meta";

/// Replica-wide metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    pub format_version: u32,
    pub replica_id: ReplicaId,
    /// Counter of the last dot this replica assigned.
    pub counter: u64,
    pub hlc: Hlc,
    /// Every dot whose effects this replica's states include.
    pub vv: VersionVector,
    /// Workspace root this store belongs to.
    pub root: String,
}

/// Persistent replica state.
pub struct ReplicaStore {
    db: Database,
}

#[derive(Deserialize)]
struct FormatProbe {
    format_version: u32,
}

impl ReplicaStore {
    pub fn open(path: &Path, root: &str) -> Result<Self> {
        Self::open_with_id(path, root, None)
    }

    /// Open or create the store. `id` is used only when the store is created.
    pub fn open_with_id(path: &Path, root: &str, id: Option<ReplicaId>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path).db()?;
        let wtx = db.begin_write().db()?;
        {
            wtx.open_table(PATHS).db()?;
            let mut meta_table = wtx.open_table(META).db()?;
            let existing = meta_table.get(META_KEY).db()?.map(|g| g.value().to_vec());
            match existing {
                None => {
                    let meta = Meta {
                        format_version: FORMAT_VERSION,
                        replica_id: id.unwrap_or_default(),
                        counter: 0,
                        hlc: Hlc::default(),
                        vv: VersionVector::new(),
                        root: root.to_owned(),
                    };
                    meta_table.insert(META_KEY, serde_json::to_vec(&meta)?.as_slice()).db()?;
                }
                Some(bytes) => {
                    let probe: FormatProbe = serde_json::from_slice(&bytes)?;
                    if probe.format_version > FORMAT_VERSION {
                        return Err(Error::FormatTooNew { found: probe.format_version, supported: FORMAT_VERSION });
                    }
                    let meta: Meta = serde_json::from_slice(&bytes)?;
                    if meta.root != root {
                        return Err(Error::RootMismatch { stored: meta.root, requested: root.to_owned() });
                    }
                }
            }
        }
        wtx.commit().db()?;
        Ok(Self { db })
    }

    pub fn meta(&self) -> Result<Meta> {
        let rtx = self.db.begin_read().db()?;
        let table = rtx.open_table(META).db()?;
        let guard = table.get(META_KEY).db()?.ok_or_else(|| Error::Corrupt("missing meta record".into()))?;
        Ok(serde_json::from_slice(guard.value())?)
    }

    pub fn get(&self, path: &str) -> Result<Option<PathState>> {
        let rtx = self.db.begin_read().db()?;
        let table = rtx.open_table(PATHS).db()?;
        match table.get(path).db()? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    pub fn all(&self) -> Result<Vec<(String, PathState)>> {
        let rtx = self.db.begin_read().db()?;
        let table = rtx.open_table(PATHS).db()?;
        let mut out = Vec::new();
        for item in table.iter().db()? {
            let (k, v) = item.db()?;
            out.push((k.value().to_owned(), serde_json::from_slice(v.value())?));
        }
        Ok(out)
    }

    pub fn any_materialized(&self) -> Result<bool> {
        Ok(self.all()?.iter().any(|(_, s)| s.disk.hash.is_some()))
    }

    /// Write `meta` and `states` in one transaction.
    pub fn commit(&self, meta: &Meta, states: &[(String, PathState)]) -> Result<()> {
        let wtx = self.db.begin_write().db()?;
        {
            let mut meta_table = wtx.open_table(META).db()?;
            meta_table.insert(META_KEY, serde_json::to_vec(meta)?.as_slice()).db()?;
            let mut paths = wtx.open_table(PATHS).db()?;
            for (path, state) in states {
                paths.insert(path.as_str(), serde_json::to_vec(state)?.as_slice()).db()?;
            }
        }
        wtx.commit().db()?;
        Ok(())
    }
}
```

`lib.rs`: `pub use state::{DiskState, PathState};` and `pub use store::{FORMAT_VERSION, Meta, ReplicaStore};`

redb keys iterate in byte order, so `all()` is sorted by path.

- [ ] **Step 5: Run the tests to see them pass**

Run: `cargo test -p sapphire-framework-sync store`
Expected: 5 tests pass. If a redb 4 import is unused (e.g. `ReadableTable`), remove it.

- [ ] **Step 6: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync
git commit -m "feat(sync): path state and redb replica store with format and root checks"
```

---

### Task 5: Replica — scanning, joining, materializing, sessions

**Files:**
- Create: `crates/sapphire-framework-sync/src/replica.rs`
- Create: `crates/sapphire-framework-sync/src/testing.rs`
- Create: `crates/sapphire-framework-sync/tests/common/mod.rs`
- Create: `crates/sapphire-framework-sync/tests/replica_basic.rs`
- Modify: `crates/sapphire-framework-sync/src/lib.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces:
  - `const DEFAULT_MAX_FILE_SIZE: u64`
  - `trait ContentSource { fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>>; }` — implemented by `Replica` and `testing::MapSource`
  - `ReplicaConfig { app_name: String, root: PathBuf, device_id: GrainId, max_file_size: u64, store_path: PathBuf, staging_dir: PathBuf }` (`Clone`), `ReplicaConfig::new(app_name, root, device_id, state_dir: &Path)` → `store_path = state_dir/sync.redb`, `staging_dir = state_dir/staging`
  - `Replica::open(config, clock: Arc<dyn Clock>) -> Result<Replica>`
  - `Replica::{replica_id() -> ReplicaId, vv() -> &VersionVector, config() -> &ReplicaConfig, state(&str) -> Result<Option<PathState>>, states() -> Result<Vec<(String, PathState)>>}`
  - `Replica::scan(&mut self) -> Result<ScanOutcome>`
  - `Replica::delta_for(&self, peer: &VersionVector) -> Result<Vec<PathUpdate>>`
  - `Replica::apply(&mut self, updates: &[PathUpdate], source: &dyn ContentSource) -> Result<Report>`
  - `Replica::commit_session(&mut self, peer: &VersionVector) -> Result<()>` — call only after every update of the session was applied without error
  - `Replica::fetch_missing(&mut self, source: &dyn ContentSource) -> Result<Report>`
  - `Replica::read_content(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>>`
  - test-util: `Replica::open_with_replica_id(config, clock, id: ReplicaId)`, `Replica::logical_dump(&self) -> Result<serde_json::Value>`, `testing::ManualClock` (`new(ms) -> Arc<Self>`, `set`, `advance`), `testing::MapSource` (`Default`, `with(bytes) -> Self`)
  - Private methods later tasks edit: `reconcile_path`, `integrate`, `settle`, `disk_seen`

How it fits together (read before coding):

1. `scan` walks the root and every stored path, calling `reconcile_path` on each.
2. `reconcile_path` hashes the file (mtime/size pre-filter unless racy). If the hash differs from `disk.hash`, the file was edited: it records a local entry with `context = disk.seen` and joins it via `integrate`. Otherwise it calls `settle`.
3. `apply` does, per update: observe HLCs → `reconcile_path` (record any pending local edit first) → `integrate`.
4. `integrate` joins (Task 2), commits the state, then calls `settle`.
5. `settle` writes the winner to disk if `disk.hash` differs (stage → verify → rename), then sets `disk.seen` from `disk_seen`.

The store commits before the file is written; `reconcile_path` finishes an interrupted write because the file still matches `disk.hash` but not the winner.

- [ ] **Step 1: Write the test helpers**

`src/testing.rs`:

```rust
//! Helpers for tests (feature `test-util`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::hash::ContentHash;
use crate::hlc::Clock;
use crate::replica::ContentSource;

/// A clock the test moves by hand.
#[derive(Debug)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(ms: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(ms)))
    }

    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A content source backed by a map.
#[derive(Debug, Default)]
pub struct MapSource(pub HashMap<ContentHash, Vec<u8>>);

impl MapSource {
    pub fn with(mut self, bytes: &[u8]) -> Self {
        self.0.insert(ContentHash::of_bytes(bytes), bytes.to_vec());
        self
    }
}

impl ContentSource for MapSource {
    fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>> {
        self.0.get(hash).cloned()
    }
}
```

`tests/common/mod.rs`:

```rust
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_framework_sync::testing::ManualClock;
use sapphire_framework_sync::{Replica, ReplicaConfig, Report, ScanOutcome};

pub const APP: &str = "test-app";

pub struct Node {
    pub replica: Replica,
    pub root: PathBuf,
    pub clock: Arc<ManualClock>,
    _dir: tempfile::TempDir,
}

/// A replica with its marker directory in place. `n` seeds the device id and clock.
pub fn node(n: u64) -> Node {
    node_with(n, |_| {})
}

pub fn node_with(n: u64, tweak: impl FnOnce(&mut ReplicaConfig)) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join(format!(".{APP}"))).unwrap();
    let mut config = ReplicaConfig::new(APP, &root, GrainId::from_u64(n).unwrap(), &dir.path().join("state"));
    tweak(&mut config);
    let clock = ManualClock::new(1_000_000 * n);
    let replica = Replica::open(config, clock.clone()).unwrap();
    Node { replica, root, clock, _dir: dir }
}

/// Close and reopen the replica on the same state directory.
pub fn reopen(node: Node) -> Node {
    let Node { replica, root, clock, _dir } = node;
    let config = replica.config().clone();
    drop(replica);
    let replica = Replica::open(config, clock.clone()).unwrap();
    Node { replica, root, clock, _dir }
}

pub fn write(node: &Node, rel: &str, body: &str) {
    let path = node.root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

pub fn remove(node: &Node, rel: &str) {
    let _ = std::fs::remove_file(node.root.join(rel));
}

pub fn read(node: &Node, rel: &str) -> Option<String> {
    std::fs::read_to_string(node.root.join(rel)).ok()
}

/// Scan after moving the clock, so successive writes get distinct timestamps.
pub fn scan(node: &mut Node) -> Report {
    node.clock.advance(10);
    match node.replica.scan().unwrap() {
        ScanOutcome::Scanned(report) => report,
        ScanOutcome::Paused(reason) => panic!("unexpectedly paused: {reason:?}"),
    }
}

/// One direction of a session: `to` receives everything `from` has that it lacks.
pub fn push(from: &Node, to: &mut Node) -> Report {
    let from_vv = from.replica.vv().clone();
    let updates = from.replica.delta_for(to.replica.vv()).unwrap();
    let report = to.replica.apply(&updates, &from.replica).unwrap();
    to.replica.commit_session(&from_vv).unwrap();
    report
}

pub fn sync(a: &mut Node, b: &mut Node) {
    push(a, b);
    push(b, a);
}

/// Files under the root (excluding the marker directory) and their contents.
pub fn tree(node: &Node) -> BTreeMap<String, String> {
    let marker = node.root.join(format!(".{APP}"));
    walkdir::WalkDir::new(&node.root)
        .into_iter()
        .map(Result::unwrap)
        .filter(|e| e.file_type().is_file() && !e.path().starts_with(&marker))
        .map(|e| {
            let rel = e.path().strip_prefix(&node.root).unwrap().to_string_lossy().replace('\\', "/");
            (rel, std::fs::read_to_string(e.path()).unwrap())
        })
        .collect()
}
```

Add `walkdir.workspace = true` and `grain-id.workspace = true` to `[dev-dependencies]` in the crate manifest.

- [ ] **Step 2: Write the failing integration tests**

`tests/replica_basic.rs`:

```rust
mod common;

use common::*;
use sapphire_framework_sync::testing::MapSource;
use sapphire_framework_sync::{Content, PathUpdate, SkipReason, VersionVector};

#[test]
fn a_new_file_propagates() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "notes/a.txt", "hello");
    let report = scan(&mut a);
    assert_eq!(report.recorded.len(), 1);
    push(&a, &mut b);
    assert_eq!(read(&b, "notes/a.txt").as_deref(), Some("hello"));
    let sa = a.replica.state("notes/a.txt").unwrap().unwrap();
    let sb = b.replica.state("notes/a.txt").unwrap().unwrap();
    assert_eq!(sa.versions, sb.versions);
    assert_eq!(sa.seen, sb.seen);
}

#[test]
fn an_edit_on_top_of_a_synced_file_supersedes_it() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);
    write(&b, "a.txt", "two");
    scan(&mut b);
    push(&b, &mut a);
    assert_eq!(read(&a, "a.txt").as_deref(), Some("two"));
    assert_eq!(a.replica.state("a.txt").unwrap().unwrap().versions.len(), 1);
    assert_eq!(tree(&a).len(), 1, "no conflict copy");
}

#[test]
fn a_delete_propagates() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);
    remove(&a, "a.txt");
    let report = scan(&mut a);
    assert!(report.recorded[0].content.is_tombstone());
    push(&a, &mut b);
    assert_eq!(read(&b, "a.txt"), None);
    assert!(b.replica.state("a.txt").unwrap().unwrap().winner().content.is_tombstone());
}

#[test]
fn scanning_twice_records_nothing_new() {
    let mut a = node(1);
    write(&a, "a.txt", "one");
    scan(&mut a);
    let again = scan(&mut a);
    assert!(again.recorded.is_empty());
    assert_eq!(again.changed, 0);
    write(&a, "a.txt", "one");
    assert!(scan(&mut a).recorded.is_empty(), "rewriting identical bytes is not an edit");
}

#[test]
fn nothing_to_send_to_an_up_to_date_peer() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);
    assert!(a.replica.delta_for(b.replica.vv()).unwrap().is_empty());
    assert!(b.replica.delta_for(a.replica.vv()).unwrap().is_empty());
}

#[test]
fn re_applying_a_full_delta_changes_nothing() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    push(&a, &mut b);
    let everything = a.replica.delta_for(&VersionVector::new()).unwrap();
    let report = b.replica.apply(&everything, &a.replica).unwrap();
    assert_eq!(report.changed, 0);
}

#[test]
fn hidden_and_ignored_files_are_not_recorded() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, ".sapphireignore", "*.tmp\n");
    write(&a, "x.tmp", "scratch");
    write(&a, ".git/config", "secret");
    write(&a, "keep.txt", "kept");
    let recorded: Vec<String> = scan(&mut a).recorded.into_iter().map(|e| e.path).collect();
    assert_eq!(recorded, vec![".sapphireignore".to_string(), "keep.txt".to_string()]);
    push(&a, &mut b);
    assert_eq!(read(&b, "x.tmp"), None);
    assert_eq!(read(&b, "keep.txt").as_deref(), Some("kept"));
}

#[test]
fn a_local_file_over_the_size_cap_is_skipped() {
    let mut a = node_with(1, |c| c.max_file_size = 4);
    write(&a, "big.bin", "0123456789");
    let report = scan(&mut a);
    assert!(report.recorded.is_empty());
    assert_eq!(report.skipped[0].reason, SkipReason::TooLarge { len: 10, max: 4 });
}

#[test]
fn a_remote_file_over_the_size_cap_is_kept_but_not_written() {
    let mut a = node(1);
    let mut b = node_with(2, |c| c.max_file_size = 4);
    write(&a, "big.bin", "0123456789");
    scan(&mut a);
    let report = push(&a, &mut b);
    assert_eq!(report.skipped[0].reason, SkipReason::TooLarge { len: 10, max: 4 });
    assert!(b.replica.state("big.bin").unwrap().is_some());
    assert_eq!(read(&b, "big.bin"), None);
}

#[test]
fn unavailable_content_is_fetched_later() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);
    let updates = a.replica.delta_for(b.replica.vv()).unwrap();
    let report = b.replica.apply(&updates, &MapSource::default()).unwrap();
    assert_eq!(report.skipped[0].reason, SkipReason::ContentUnavailable);
    assert_eq!(read(&b, "a.txt"), None);
    b.replica.fetch_missing(&a.replica).unwrap();
    assert_eq!(read(&b, "a.txt").as_deref(), Some("hello"));
}

#[test]
fn malformed_updates_are_ignored() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);
    let mut update: PathUpdate = a.replica.delta_for(b.replica.vv()).unwrap().remove(0);
    update.path = "../escape.txt".into();
    for v in &mut update.versions {
        v.path = update.path.clone();
    }
    let report = b.replica.apply(&[update], &a.replica).unwrap();
    assert_eq!(report.changed, 0);
    assert!(b.replica.states().unwrap().is_empty());
}

#[test]
fn state_survives_reopening() {
    let mut a = node(1);
    write(&a, "a.txt", "hello");
    scan(&mut a);
    let id = a.replica.replica_id();
    let mut a = reopen(a);
    assert_eq!(a.replica.replica_id(), id);
    assert!(scan(&mut a).recorded.is_empty());
    assert!(matches!(
        a.replica.state("a.txt").unwrap().unwrap().winner().content,
        Content::File { len: 5, .. }
    ));
}

#[cfg(unix)]
#[test]
fn symlinks_are_skipped() {
    let mut a = node(1);
    write(&a, "target.txt", "t");
    std::os::unix::fs::symlink(a.root.join("target.txt"), a.root.join("link.txt")).unwrap();
    let report = scan(&mut a);
    assert!(report.skipped.iter().any(|s| s.path == "link.txt" && s.reason == SkipReason::Symlink));
    assert!(report.recorded.iter().all(|e| e.path != "link.txt"));
}
```

Add `pub mod testing;` (gated) and `mod replica;` to `lib.rs` so the tests reach the compile stage:

```rust
mod replica;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;

pub use replica::{ContentSource, DEFAULT_MAX_FILE_SIZE, Replica, ReplicaConfig};
```

- [ ] **Step 3: Run the tests to see them fail**

Run: `cargo test -p sapphire-framework-sync --test replica_basic`
Expected: compile errors — `Replica`, `ReplicaConfig`, `ContentSource` do not exist yet.

- [ ] **Step 4: Implement `src/replica.rs`**

```rust
//! A replica: one workspace root kept in sync with peers by joining path states.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use grain_id::GrainId;

use crate::entry::{Content, Entry, PathUpdate};
use crate::error::{Error, Result};
use crate::filter::SyncFilter;
use crate::hash::ContentHash;
use crate::hlc::Clock;
use crate::id::ReplicaId;
use crate::merge;
use crate::paths;
use crate::report::{Report, ScanOutcome, SkipReason, Skipped};
use crate::state::{DiskState, PathState};
use crate::store::{Meta, ReplicaStore};
use crate::vv::{Dot, VersionVector};

/// Files larger than this are not synced unless configured otherwise.
pub const DEFAULT_MAX_FILE_SIZE: u64 = 64 * 1024 * 1024;

/// A file whose mtime is this close to when it was checked is re-hashed.
const RACY_NS: i64 = 2_000_000_000;

/// Where a replica gets bytes it does not have: a peer, in practice.
pub trait ContentSource {
    fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>>;
}

/// How a replica is set up.
#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub app_name: String,
    pub root: PathBuf,
    /// Recorded as `Entry::author` on local writes.
    pub device_id: GrainId,
    pub max_file_size: u64,
    pub store_path: PathBuf,
    pub staging_dir: PathBuf,
}

impl ReplicaConfig {
    /// A config keeping the store and staging directory under `state_dir`.
    pub fn new(
        app_name: impl Into<String>,
        root: impl Into<PathBuf>,
        device_id: GrainId,
        state_dir: &Path,
    ) -> Self {
        Self {
            app_name: app_name.into(),
            root: root.into(),
            device_id,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            store_path: state_dir.join("sync.redb"),
            staging_dir: state_dir.join("staging"),
        }
    }
}

/// One workspace root and its replica store.
pub struct Replica {
    config: ReplicaConfig,
    store: ReplicaStore,
    meta: Meta,
    clock: Arc<dyn Clock>,
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn stamp_of(meta: &fs::Metadata) -> (i64, u64) {
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    (mtime, meta.len())
}

fn is_racy(disk: &DiskState) -> bool {
    disk.mtime_ns.saturating_add(RACY_NS) >= disk.checked_ns
}

/// Move a verified staged file into place, falling back to copy + rename when the
/// staging directory is on another volume.
fn place(staged: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    if fs::rename(staged, dest).is_ok() {
        return Ok(());
    }
    let name = dest.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let tmp = dest.with_file_name(format!(".{name}.sapphire-tmp"));
    fs::copy(staged, &tmp)?;
    fs::rename(&tmp, dest)?;
    fs::remove_file(staged)?;
    Ok(())
}

impl Replica {
    pub fn open(config: ReplicaConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        Self::open_inner(config, clock, None)
    }

    fn open_inner(config: ReplicaConfig, clock: Arc<dyn Clock>, id: Option<ReplicaId>) -> Result<Self> {
        fs::create_dir_all(&config.staging_dir)?;
        let root = config.root.to_string_lossy().into_owned();
        let store = ReplicaStore::open_with_id(&config.store_path, &root, id)?;
        let meta = store.meta()?;
        Ok(Self { config, store, meta, clock })
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.meta.replica_id
    }

    pub fn vv(&self) -> &VersionVector {
        &self.meta.vv
    }

    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    pub fn state(&self, path: &str) -> Result<Option<PathState>> {
        self.store.get(path)
    }

    pub fn states(&self) -> Result<Vec<(String, PathState)>> {
        self.store.all()
    }

    fn filter(&self) -> Result<SyncFilter> {
        SyncFilter::load(&self.config.root, &self.config.app_name)
    }

    /// Record every unrecorded edit under the root and finish pending writes.
    pub fn scan(&mut self) -> Result<ScanOutcome> {
        let filter = self.filter()?;
        let mut report = Report::default();
        let mut rels = BTreeSet::new();
        if self.config.root.is_dir() {
            let root = self.config.root.clone();
            let walker = walkdir::WalkDir::new(&root).follow_links(false).into_iter().filter_entry(|e| {
                e.depth() == 0
                    || paths::rel_from_native(&root, e.path())
                        .is_some_and(|rel| filter.allows(&rel, e.file_type().is_dir()))
            });
            for item in walker {
                let item = item.map_err(|e| Error::Io(std::io::Error::other(e)))?;
                if item.depth() == 0 || item.file_type().is_dir() {
                    continue;
                }
                let Some(rel) = paths::rel_from_native(&root, item.path()) else {
                    continue;
                };
                if item.file_type().is_symlink() {
                    report.skipped.push(Skipped { path: rel, reason: SkipReason::Symlink });
                    continue;
                }
                rels.insert(rel);
            }
        }
        for (rel, _) in self.store.all()? {
            rels.insert(rel);
        }
        for rel in rels {
            self.reconcile_path(&rel, &filter, None, &mut report)?;
        }
        Ok(ScanOutcome::Scanned(report))
    }

    /// Path states the peer with version vector `peer` has not merged.
    pub fn delta_for(&self, peer: &VersionVector) -> Result<Vec<PathUpdate>> {
        Ok(self
            .store
            .all()?
            .into_iter()
            .filter(|(_, s)| !peer.covers(&s.seen))
            .map(|(path, s)| PathUpdate { path, versions: s.versions, seen: s.seen })
            .collect())
    }

    /// Join updates received from a peer, fetching content from `source`.
    pub fn apply(&mut self, updates: &[PathUpdate], source: &dyn ContentSource) -> Result<Report> {
        let filter = self.filter()?;
        let mut report = Report::default();
        let now = self.clock.now_ms();
        for update in updates {
            let well_formed = paths::is_valid_rel(&update.path)
                && !update.versions.is_empty()
                && update.versions.iter().all(|v| v.path == update.path);
            if !well_formed {
                tracing::warn!(path = %update.path, "ignoring a malformed path update");
                continue;
            }
            for version in &update.versions {
                self.meta.hlc = self.meta.hlc.observe(version.hlc, now);
            }
            self.reconcile_path(&update.path, &filter, Some(source), &mut report)?;
            self.integrate(update.clone(), None, &filter, Some(source), &mut report)?;
        }
        Ok(report)
    }

    /// Declare that every update of a session with `peer` has been applied.
    pub fn commit_session(&mut self, peer: &VersionVector) -> Result<()> {
        self.meta.vv.merge(peer);
        self.store.commit(&self.meta, &[])
    }

    /// Retry writes that were waiting for content.
    pub fn fetch_missing(&mut self, source: &dyn ContentSource) -> Result<Report> {
        let filter = self.filter()?;
        let mut report = Report::default();
        for (rel, _) in self.store.all()? {
            self.reconcile_path(&rel, &filter, Some(source), &mut report)?;
        }
        Ok(report)
    }

    /// Bytes with `hash` from a file on disk or the staging directory.
    pub fn read_content(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>> {
        for (rel, state) in self.store.all()? {
            if state.disk.hash.as_ref() != Some(hash) {
                continue;
            }
            if let Ok(bytes) = fs::read(paths::to_native(&self.config.root, &rel))
                && ContentHash::of_bytes(&bytes) == *hash
            {
                return Ok(Some(bytes));
            }
        }
        if let Ok(bytes) = fs::read(self.config.staging_dir.join(hash.to_hex()))
            && ContentHash::of_bytes(&bytes) == *hash
        {
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    fn next_entry(&mut self, rel: &str, content: Content, context: VersionVector) -> Entry {
        self.meta.counter += 1;
        self.meta.hlc = self.meta.hlc.tick(self.clock.now_ms());
        let dot = Dot { replica: self.meta.replica_id, counter: self.meta.counter };
        self.meta.vv.add_dot(&dot);
        Entry {
            path: rel.to_owned(),
            content,
            hlc: self.meta.hlc,
            dot,
            context,
            author: self.config.device_id,
        }
    }

    /// Compare one path's file with its state: record an edit, or settle the state.
    fn reconcile_path(
        &mut self,
        rel: &str,
        filter: &SyncFilter,
        source: Option<&dyn ContentSource>,
        report: &mut Report,
    ) -> Result<()> {
        if !filter.allows(rel, false) || !paths::representable(rel) {
            return Ok(());
        }
        let abs = paths::to_native(&self.config.root, rel);
        let state = self.store.get(rel)?;
        let disk = state.as_ref().map(|s| s.disk.clone()).unwrap_or_default();
        // On a case-insensitive filesystem, `a.txt` would find the file of `A.txt`.
        if disk.hash.is_none() && self.case_twin(rel)?.is_some() {
            if let Some(state) = state {
                self.settle(rel, state, filter, source, report)?;
            }
            return Ok(());
        }
        let fs_meta = match fs::symlink_metadata(&abs) {
            Ok(m) => Some(m),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let mut stamp = (0, 0);
        let file_hash = match &fs_meta {
            Some(m) if m.file_type().is_symlink() => {
                report.skipped.push(Skipped { path: rel.to_owned(), reason: SkipReason::Symlink });
                return Ok(());
            }
            Some(m) if m.is_file() => {
                stamp = stamp_of(m);
                if disk.hash.is_some() && stamp == (disk.mtime_ns, disk.len) && !is_racy(&disk) {
                    disk.hash
                } else if m.len() > self.config.max_file_size {
                    report.skipped.push(Skipped {
                        path: rel.to_owned(),
                        reason: SkipReason::TooLarge { len: m.len(), max: self.config.max_file_size },
                    });
                    return Ok(());
                } else {
                    Some(ContentHash::of_file(&abs)?)
                }
            }
            _ => None,
        };

        if file_hash == disk.hash {
            if let Some(mut state) = state {
                if file_hash.is_some() && (stamp != (disk.mtime_ns, disk.len) || is_racy(&disk)) {
                    state.disk.mtime_ns = stamp.0;
                    state.disk.len = stamp.1;
                    state.disk.checked_ns = now_ns();
                    self.store.commit(&self.meta, &[(rel.to_owned(), state.clone())])?;
                }
                self.settle(rel, state, filter, source, report)?;
            }
            return Ok(());
        }

        let content = match file_hash {
            Some(hash) => Content::File { hash, len: stamp.1 },
            None => Content::Tombstone,
        };
        if state.is_none() && content.is_tombstone() {
            return Ok(());
        }
        let entry = self.next_entry(rel, content, disk.seen.clone());
        report.recorded.push(entry.clone());
        let mut seen = entry.context.clone();
        seen.add_dot(&entry.dot);
        let new_disk = DiskState {
            hash: file_hash,
            seen: seen.clone(),
            mtime_ns: stamp.0,
            len: stamp.1,
            checked_ns: now_ns(),
        };
        let update = PathUpdate { path: rel.to_owned(), versions: vec![entry], seen };
        self.integrate(update, Some(new_disk), filter, source, report)
    }

    /// Join `update` into the stored state, commit, then settle the file.
    /// `local_disk` is set when the update is a local write already on disk.
    fn integrate(
        &mut self,
        update: PathUpdate,
        local_disk: Option<DiskState>,
        filter: &SyncFilter,
        source: Option<&dyn ContentSource>,
        report: &mut Report,
    ) -> Result<()> {
        let rel = update.path.clone();
        let old = self.store.get(&rel)?;
        let joined = merge::join(
            old.as_ref().map(|s| (s.versions.as_slice(), &s.seen)),
            &update.versions,
            &update.seen,
        );
        let Some((versions, seen)) = joined else {
            return Ok(());
        };
        let disk = match local_disk {
            Some(d) => d,
            None => old.map(|s| s.disk).unwrap_or_default(),
        };
        let state = PathState { versions, seen, disk };
        self.store.commit(&self.meta, &[(rel.clone(), state.clone())])?;
        report.changed += 1;
        self.settle(&rel, state, filter, source, report)
    }

    /// Make the file on disk hold the winner, then record what it reflects.
    fn settle(
        &mut self,
        rel: &str,
        mut state: PathState,
        filter: &SyncFilter,
        source: Option<&dyn ContentSource>,
        report: &mut Report,
    ) -> Result<()> {
        let winner = state.winner().clone();
        if winner.content.hash() == state.disk.hash {
            let seen = self.disk_seen(&state)?;
            if seen != state.disk.seen {
                state.disk.seen = seen;
                self.store.commit(&self.meta, &[(rel.to_owned(), state)])?;
            }
            return Ok(());
        }
        if let Some(reason) = self.skip_reason(rel, &winner, filter)? {
            report.skipped.push(Skipped { path: rel.to_owned(), reason });
            return Ok(());
        }
        let abs = paths::to_native(&self.config.root, rel);
        match winner.content {
            Content::Tombstone => {
                match fs::remove_file(&abs) {
                    Ok(()) => {}
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                state.disk = DiskState::default();
            }
            Content::File { hash, .. } => {
                let Some(staged) = self.stage(&hash, source)? else {
                    report.skipped.push(Skipped { path: rel.to_owned(), reason: SkipReason::ContentUnavailable });
                    return Ok(());
                };
                place(&staged, &abs)?;
                let (mtime_ns, len) = stamp_of(&fs::metadata(&abs)?);
                state.disk = DiskState { hash: Some(hash), seen: VersionVector::new(), mtime_ns, len, checked_ns: now_ns() };
            }
        }
        state.disk.seen = self.disk_seen(&state)?;
        self.store.commit(&self.meta, &[(rel.to_owned(), state)])
    }

    /// Versions the file on disk reflects once the winner is written.
    /// (Task 6 excludes losers whose conflict copy could not be made.)
    fn disk_seen(&self, state: &PathState) -> Result<VersionVector> {
        Ok(state.seen.clone())
    }

    fn skip_reason(&self, rel: &str, winner: &Entry, filter: &SyncFilter) -> Result<Option<SkipReason>> {
        if !filter.allows(rel, false) {
            return Ok(Some(SkipReason::Ignored));
        }
        if !paths::representable(rel) {
            return Ok(Some(SkipReason::Unrepresentable));
        }
        if let Content::File { len, .. } = winner.content
            && len > self.config.max_file_size
        {
            return Ok(Some(SkipReason::TooLarge { len, max: self.config.max_file_size }));
        }
        if !winner.content.is_tombstone()
            && let Some(other) = self.case_twin(rel)?
        {
            return Ok(Some(SkipReason::CaseCollision { other }));
        }
        Ok(None)
    }

    /// Another stored path, differing from `rel` only in case, whose file is on disk.
    /// Always `None` on case-sensitive filesystems.
    fn case_twin(&self, rel: &str) -> Result<Option<String>> {
        if !paths::CASE_INSENSITIVE_FS {
            return Ok(None);
        }
        let lower = rel.to_lowercase();
        Ok(self
            .store
            .all()?
            .into_iter()
            .find(|(other, s)| other != rel && other.to_lowercase() == lower && s.disk.hash.is_some())
            .map(|(other, _)| other))
    }

    /// A verified copy of `hash` in the staging directory, if one can be had.
    fn stage(&self, hash: &ContentHash, source: Option<&dyn ContentSource>) -> Result<Option<PathBuf>> {
        let staged = self.config.staging_dir.join(hash.to_hex());
        if staged.is_file() && ContentHash::of_file(&staged)? == *hash {
            return Ok(Some(staged));
        }
        let bytes = match self.read_content(hash)? {
            Some(bytes) => Some(bytes),
            None => source.and_then(|s| s.fetch(hash)),
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if ContentHash::of_bytes(&bytes) != *hash {
            tracing::warn!(%hash, "content source returned bytes with the wrong hash");
            return Ok(None);
        }
        fs::write(&staged, &bytes)?;
        Ok(Some(staged))
    }
}

impl ContentSource for Replica {
    fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>> {
        self.read_content(hash).ok().flatten()
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Replica {
    /// Open with a fixed replica id (used only when the store is created).
    pub fn open_with_replica_id(config: ReplicaConfig, clock: Arc<dyn Clock>, id: ReplicaId) -> Result<Self> {
        Self::open_inner(config, clock, Some(id))
    }

    /// Everything that must agree across converged replicas, without timestamps of
    /// the local filesystem.
    pub fn logical_dump(&self) -> Result<serde_json::Value> {
        let mut path_states = serde_json::Map::new();
        for (rel, s) in self.store.all()? {
            path_states.insert(
                rel,
                serde_json::json!({
                    "versions": s.versions,
                    "seen": s.seen,
                    "disk_hash": s.disk.hash,
                    "disk_seen": s.disk.seen,
                }),
            );
        }
        let mut files = serde_json::Map::new();
        if self.config.root.is_dir() {
            for item in walkdir::WalkDir::new(&self.config.root).sort_by_file_name() {
                let item = item.map_err(|e| Error::Io(std::io::Error::other(e)))?;
                if !item.file_type().is_file() {
                    continue;
                }
                if let Some(rel) = paths::rel_from_native(&self.config.root, item.path()) {
                    files.insert(rel, serde_json::json!(ContentHash::of_file(item.path())?));
                }
            }
        }
        Ok(serde_json::json!({
            "replica_id": self.meta.replica_id,
            "vv": self.meta.vv,
            "paths": path_states,
            "files": files,
        }))
    }
}
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `cargo test -p sapphire-framework-sync`
Expected: all unit tests and the 13 `replica_basic` tests pass (12 on Windows, where the symlink test is compiled out).

- [ ] **Step 6: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync Cargo.lock
git commit -m "feat(sync): replica scanning, joining, materialization and sessions"
```

---

### Task 6: Conflict copies

**Files:**
- Modify: `crates/sapphire-framework-sync/src/replica.rs` (new method, two call sites, `disk_seen` body)
- Create: `crates/sapphire-framework-sync/tests/replica_conflict.rs`

**Interfaces:**
- Consumes: `merge::{needs_copy, conflict_path}`, `Replica` internals from Task 5
- Produces: `Report::conflicts` populated; private `Replica::ensure_conflict_copies(&mut self, rel: &str, state: &PathState, filter: &SyncFilter, source: Option<&dyn ContentSource>, report: &mut Report) -> Result<()>`

Rules (spec §2.4, §2.5): every loser that is not a tombstone and differs from the winner gets a copy at `conflict_path`, written **before** the winner overwrites the loser's file; a copy is only created if the copy path has never had a state; if the bytes are unavailable no copy is made and `disk.seen` excludes that loser.

- [ ] **Step 1: Write the failing tests**

`tests/replica_conflict.rs`:

```rust
mod common;

use std::collections::BTreeSet;

use common::*;
use sapphire_framework_sync::testing::MapSource;

fn based(a: &mut Node, b: &mut Node) {
    write(a, "a.txt", "base");
    scan(a);
    sync(a, b);
}

fn contents(node: &Node) -> BTreeSet<String> {
    tree(node).into_values().collect()
}

#[test]
fn concurrent_edits_keep_both_versions() {
    let (mut a, mut b) = (node(1), node(2));
    based(&mut a, &mut b);
    write(&a, "a.txt", "from a");
    write(&b, "a.txt", "from b");
    scan(&mut a);
    scan(&mut b);
    sync(&mut a, &mut b);
    sync(&mut a, &mut b);

    assert_eq!(tree(&a), tree(&b));
    assert_eq!(tree(&a).len(), 2);
    assert_eq!(contents(&a), BTreeSet::from(["from a".to_string(), "from b".to_string()]));
    let copy = tree(&a).into_keys().find(|p| p != "a.txt").unwrap();
    assert!(copy.starts_with("a.conflict-") && copy.ends_with(".txt"), "{copy}");

    // An edit on top of the merged state supersedes both siblings; no new copies.
    write(&a, "a.txt", "resolved");
    scan(&mut a);
    sync(&mut a, &mut b);
    assert_eq!(read(&b, "a.txt").as_deref(), Some("resolved"));
    assert_eq!(b.replica.state("a.txt").unwrap().unwrap().versions.len(), 1);
    assert_eq!(tree(&b).len(), 2);
}

#[test]
fn an_edit_beats_a_concurrent_delete_without_a_copy() {
    let (mut a, mut b) = (node(1), node(2));
    based(&mut a, &mut b);
    remove(&a, "a.txt");
    write(&b, "a.txt", "edited");
    scan(&mut a);
    scan(&mut b);
    sync(&mut a, &mut b);
    assert_eq!(tree(&a), tree(&b));
    assert_eq!(read(&a, "a.txt").as_deref(), Some("edited"));
    assert_eq!(tree(&a).len(), 1);
}

#[test]
fn identical_concurrent_edits_make_no_copy() {
    let (mut a, mut b) = (node(1), node(2));
    based(&mut a, &mut b);
    write(&a, "a.txt", "same");
    write(&b, "a.txt", "same");
    scan(&mut a);
    scan(&mut b);
    sync(&mut a, &mut b);
    assert_eq!(tree(&a), tree(&b));
    assert_eq!(tree(&a).len(), 1);
}

#[test]
fn three_way_concurrency_converges_with_two_copies() {
    let (mut a, mut b, mut c) = (node(1), node(2), node(3));
    based(&mut a, &mut b);
    sync(&mut b, &mut c);
    write(&a, "a.txt", "A");
    write(&b, "a.txt", "B");
    write(&c, "a.txt", "C");
    scan(&mut a);
    scan(&mut b);
    scan(&mut c);
    for _ in 0..3 {
        sync(&mut a, &mut b);
        sync(&mut b, &mut c);
        sync(&mut a, &mut c);
    }
    assert_eq!(tree(&a), tree(&b));
    assert_eq!(tree(&b), tree(&c));
    assert_eq!(contents(&a), BTreeSet::from(["A".to_string(), "B".to_string(), "C".to_string()]));
    assert_eq!(tree(&a).len(), 3);
}

#[test]
fn a_deleted_copy_is_not_resurrected() {
    let (mut a, mut b) = (node(1), node(2));
    based(&mut a, &mut b);
    write(&a, "a.txt", "from a");
    write(&b, "a.txt", "from b");
    scan(&mut a);
    scan(&mut b);
    sync(&mut a, &mut b);
    sync(&mut a, &mut b);
    let copy = tree(&a).into_keys().find(|p| p != "a.txt").unwrap();
    remove(&a, &copy);
    scan(&mut a);
    sync(&mut a, &mut b);
    scan(&mut b);
    sync(&mut a, &mut b);
    assert_eq!(read(&a, &copy), None);
    assert_eq!(read(&b, &copy), None);
}

#[test]
fn a_loser_without_content_is_not_superseded_by_a_later_local_edit() {
    let (mut a, mut b) = (node(1), node(2));
    based(&mut a, &mut b);
    write(&a, "a.txt", "from a");
    scan(&mut a);
    b.clock.set(9_000_000);
    write(&b, "a.txt", "from b");
    scan(&mut b);

    // b merges a's version but cannot get its bytes, so it cannot make the copy.
    let a_vv = a.replica.vv().clone();
    let updates = a.replica.delta_for(b.replica.vv()).unwrap();
    b.replica.apply(&updates, &MapSource::default()).unwrap();
    b.replica.commit_session(&a_vv).unwrap();
    assert_eq!(tree(&b).len(), 1);

    // A later edit on b must not silently supersede a's version...
    write(&b, "a.txt", "b again");
    scan(&mut b);
    // ...so a, which has the bytes, makes the copy when it learns b won.
    sync(&mut b, &mut a);
    sync(&mut b, &mut a);
    assert_eq!(read(&a, "a.txt").as_deref(), Some("b again"));
    assert!(contents(&a).contains("from a"));
    assert!(contents(&b).contains("from a"));
    assert_eq!(tree(&a), tree(&b));
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test -p sapphire-framework-sync --test replica_conflict`
Expected: every test except `an_edit_beats_a_concurrent_delete_without_a_copy` and `identical_concurrent_edits_make_no_copy` fails — no copy files are created.

- [ ] **Step 3: Implement**

In `src/replica.rs`, change the report import to `use crate::report::{Conflict, Report, ScanOutcome, SkipReason, Skipped};`.

Add this method inside `impl Replica` (after `integrate`):

```rust
    /// Write a conflict copy for every loser that needs one and has never had one.
    fn ensure_conflict_copies(
        &mut self,
        rel: &str,
        state: &PathState,
        filter: &SyncFilter,
        source: Option<&dyn ContentSource>,
        report: &mut Report,
    ) -> Result<()> {
        if state.versions.len() < 2 {
            return Ok(());
        }
        let winner = state.winner().clone();
        let losers: Vec<Entry> = state
            .versions
            .iter()
            .filter(|v| v.dot != winner.dot && merge::needs_copy(v, &winner))
            .cloned()
            .collect();
        for loser in losers {
            let Content::File { hash, .. } = loser.content else {
                continue;
            };
            let copy_rel = merge::conflict_path(rel, &loser.dot);
            if self.store.get(&copy_rel)?.is_some() {
                continue;
            }
            let copy_abs = paths::to_native(&self.config.root, &copy_rel);
            if !copy_abs.exists() {
                // The loser may be exactly what is on disk at `rel` right now.
                let on_disk = if state.disk.hash == Some(hash) {
                    fs::read(paths::to_native(&self.config.root, rel)).ok()
                } else {
                    None
                };
                let bytes = match on_disk {
                    Some(bytes) => Some(bytes),
                    None => match self.read_content(&hash)? {
                        Some(bytes) => Some(bytes),
                        None => source.and_then(|s| s.fetch(&hash)),
                    },
                };
                let Some(bytes) = bytes.filter(|b| ContentHash::of_bytes(b) == hash) else {
                    report.skipped.push(Skipped { path: copy_rel, reason: SkipReason::ContentUnavailable });
                    continue;
                };
                let staged = self.config.staging_dir.join(format!("{}.copy", hash.to_hex()));
                fs::write(&staged, &bytes)?;
                place(&staged, &copy_abs)?;
                report.conflicts.push(Conflict { path: rel.to_owned(), copy_path: copy_rel.clone() });
            }
            // Record the copy (or a file someone already put there) as a local write.
            self.reconcile_path(&copy_rel, filter, source, report)?;
        }
        Ok(())
    }
```

In `integrate`, replace

```rust
        let state = PathState { versions, seen, disk };
        self.store.commit(&self.meta, &[(rel.clone(), state.clone())])?;
```

with

```rust
        let state = PathState { versions, seen, disk };
        // Copies first: settling may overwrite the loser's bytes on disk.
        self.ensure_conflict_copies(&rel, &state, filter, source, report)?;
        self.store.commit(&self.meta, &[(rel.clone(), state.clone())])?;
```

In `reconcile_path`, in the `if file_hash == disk.hash` branch, replace

```rust
                self.settle(rel, state, filter, source, report)?;
```

with

```rust
                self.ensure_conflict_copies(rel, &state, filter, source, report)?;
                self.settle(rel, state, filter, source, report)?;
```

Replace the whole `disk_seen` method with:

```rust
    /// Versions the file on disk reflects once the winner is written: everything
    /// merged, except losers still waiting for a conflict copy — a local edit must
    /// not supersede a version whose bytes were never preserved.
    fn disk_seen(&self, state: &PathState) -> Result<VersionVector> {
        let winner = state.winner();
        let mut seen = state.seen.clone();
        for loser in state.versions.iter().filter(|v| v.dot != winner.dot && merge::needs_copy(v, winner)) {
            if self.store.get(&merge::conflict_path(&loser.path, &loser.dot))?.is_none() {
                let slot = seen.0.entry(loser.dot.replica).or_insert(0);
                *slot = (*slot).min(loser.dot.counter - 1);
            }
        }
        Ok(seen)
    }
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test -p sapphire-framework-sync`
Expected: the 6 `replica_conflict` tests pass; `replica_basic` still passes.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync
git commit -m "feat(sync): conflict copies for concurrent versions"
```

---

### Task 7: Missing-root guard, crash recovery and platform path skips

**Files:**
- Modify: `crates/sapphire-framework-sync/src/replica.rs`
- Modify: `crates/sapphire-framework-sync/src/testing.rs`
- Create: `crates/sapphire-framework-sync/tests/replica_guards.rs`

**Interfaces:**
- Consumes: Task 5/6 `Replica`
- Produces:
  - `Replica::pause_reason(&self) -> Result<Option<PauseReason>>`
  - `scan` returns `ScanOutcome::Paused(reason)`; `apply` and `fetch_missing` return `Err(Error::Paused(reason))`
  - test-util: `testing::FaultPoint::AfterCommitBeforeWrite`, `Replica::inject_fault(&mut self, FaultPoint)`

- [ ] **Step 1: Write the failing tests**

`tests/replica_guards.rs`:

```rust
mod common;

use common::*;
use sapphire_framework_sync::testing::FaultPoint;
use sapphire_framework_sync::{Error, PauseReason, ScanOutcome};

#[test]
fn a_missing_root_pauses_instead_of_deleting_everything() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);

    let parked = a.root.with_file_name("parked");
    std::fs::rename(&a.root, &parked).unwrap();
    assert_eq!(a.replica.scan().unwrap(), ScanOutcome::Paused(PauseReason::RootMissing));
    let updates = b.replica.delta_for(&Default::default()).unwrap();
    assert!(matches!(a.replica.apply(&updates, &b.replica), Err(Error::Paused(PauseReason::RootMissing))));
    push(&a, &mut b);
    assert_eq!(read(&b, "a.txt").as_deref(), Some("one"), "the peer keeps its files");

    std::fs::rename(&parked, &a.root).unwrap();
    assert!(scan(&mut a).recorded.is_empty());
}

#[test]
fn a_missing_marker_pauses() {
    let mut a = node(1);
    write(&a, "a.txt", "one");
    scan(&mut a);
    std::fs::remove_dir_all(a.root.join(format!(".{APP}"))).unwrap();
    assert_eq!(a.replica.scan().unwrap(), ScanOutcome::Paused(PauseReason::MarkerMissing));
}

#[test]
fn an_empty_new_replica_without_a_root_is_not_paused() {
    let a = node(1);
    std::fs::remove_dir_all(&a.root).unwrap();
    let mut a = a;
    assert!(matches!(a.replica.scan().unwrap(), ScanOutcome::Scanned(_)));
}

#[test]
fn an_interrupted_write_is_finished_on_the_next_scan() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);

    b.replica.inject_fault(FaultPoint::AfterCommitBeforeWrite);
    let updates = a.replica.delta_for(b.replica.vv()).unwrap();
    assert!(matches!(b.replica.apply(&updates, &a.replica), Err(Error::InjectedFault)));
    assert_eq!(read(&b, "a.txt"), None);
    assert!(b.replica.state("a.txt").unwrap().is_some(), "the state was committed");

    let mut b = reopen(b);
    let report = scan(&mut b);
    assert!(report.recorded.is_empty(), "the missing file is not mistaken for a local delete");
    assert_eq!(read(&b, "a.txt").as_deref(), Some("hello"), "written from the staged copy");
}

#[test]
fn a_lost_store_reconverges_without_conflict_copies() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    write(&a, "dir/b.txt", "two");
    scan(&mut a);
    sync(&mut a, &mut b);
    let old_id = a.replica.replica_id();

    let mut a = reset_store(a);
    assert_ne!(a.replica.replica_id(), old_id);
    assert_eq!(scan(&mut a).recorded.len(), 2, "every file is recorded again");
    sync(&mut a, &mut b);
    sync(&mut a, &mut b);
    assert_eq!(tree(&a), tree(&b));
    assert_eq!(tree(&a).len(), 2, "identical content never makes a copy");
}

#[cfg(windows)]
#[test]
fn a_path_windows_cannot_hold_is_kept_but_not_written() {
    use grain_id::GrainId;
    use sapphire_framework_sync::testing::MapSource;
    use sapphire_framework_sync::{Content, ContentHash, Dot, Entry, Hlc, PathUpdate, SkipReason, VersionVector};

    let mut b = node(2);
    let e = Entry {
        path: "what?.txt".into(),
        content: Content::File { hash: ContentHash::of_bytes(b"x"), len: 1 },
        hlc: Hlc { wall_ms: 1, logical: 0 },
        dot: Dot { replica: sapphire_framework_sync::ReplicaId::new(), counter: 1 },
        context: VersionVector::new(),
        author: GrainId::NIL,
    };
    let mut seen = VersionVector::new();
    seen.add_dot(&e.dot);
    let update = PathUpdate { path: e.path.clone(), versions: vec![e], seen };
    let report = b.replica.apply(&[update], &MapSource::default().with(b"x")).unwrap();
    assert_eq!(report.skipped[0].reason, SkipReason::Unrepresentable);
    assert!(b.replica.state("what?.txt").unwrap().is_some());
}

#[cfg(any(windows, target_os = "macos"))]
#[test]
fn a_case_only_collision_is_skipped() {
    use grain_id::GrainId;
    use sapphire_framework_sync::testing::MapSource;
    use sapphire_framework_sync::{Content, ContentHash, Dot, Entry, Hlc, PathUpdate, SkipReason, VersionVector};

    let mut b = node(2);
    let update = |path: &str, body: &[u8], counter: u64| {
        let e = Entry {
            path: path.into(),
            content: Content::File { hash: ContentHash::of_bytes(body), len: body.len() as u64 },
            hlc: Hlc { wall_ms: counter, logical: 0 },
            dot: Dot { replica: sapphire_framework_sync::ReplicaId(uuid::Uuid::from_u128(9)), counter },
            context: VersionVector::new(),
            author: GrainId::NIL,
        };
        let mut seen = VersionVector::new();
        seen.add_dot(&e.dot);
        PathUpdate { path: path.into(), versions: vec![e], seen }
    };
    let source = MapSource::default().with(b"upper").with(b"lower");
    let report = b.replica.apply(&[update("A.txt", b"upper", 1), update("a.txt", b"lower", 2)], &source).unwrap();
    assert_eq!(report.skipped[0].reason, SkipReason::CaseCollision { other: "A.txt".into() });
    assert_eq!(read(&b, "A.txt").as_deref(), Some("upper"));
}
```

Add `uuid.workspace = true` to `[dev-dependencies]`, and this helper to `tests/common/mod.rs`:

```rust
/// Close the replica, delete its store and staging directory, and open a fresh one.
pub fn reset_store(node: Node) -> Node {
    let Node { replica, root, clock, _dir } = node;
    let config = replica.config().clone();
    drop(replica);
    std::fs::remove_file(&config.store_path).unwrap();
    let _ = std::fs::remove_dir_all(&config.staging_dir);
    let replica = Replica::open(config, clock.clone()).unwrap();
    Node { replica, root, clock, _dir }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test -p sapphire-framework-sync --test replica_guards`
Expected: compile errors (`FaultPoint`, `inject_fault`); once those exist, the pause tests fail because nothing pauses.

- [ ] **Step 3: Implement**

`src/testing.rs`, append:

```rust
/// Where an injected fault fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    /// After a path state is committed, before its file is written or removed.
    AfterCommitBeforeWrite,
}
```

`src/replica.rs`:

1. Imports: `use crate::report::{Conflict, PauseReason, Report, ScanOutcome, SkipReason, Skipped};`

2. Add the field to `struct Replica`:

```rust
    #[cfg(any(test, feature = "test-util"))]
    fault: Option<crate::testing::FaultPoint>,
```

and in `open_inner` replace `Ok(Self { config, store, meta, clock })` with:

```rust
        Ok(Self {
            config,
            store,
            meta,
            clock,
            #[cfg(any(test, feature = "test-util"))]
            fault: None,
        })
```

3. Add inside `impl Replica`:

```rust
    /// Why this replica must not scan or apply right now. A root or marker that is
    /// missing while files were written would otherwise read as "everything deleted".
    pub fn pause_reason(&self) -> Result<Option<PauseReason>> {
        let root_ok = self.config.root.is_dir();
        let marker_ok = self.config.root.join(format!(".{}", self.config.app_name)).is_dir();
        if root_ok && marker_ok {
            return Ok(None);
        }
        if !self.store.any_materialized()? {
            return Ok(None);
        }
        Ok(Some(if root_ok { PauseReason::MarkerMissing } else { PauseReason::RootMissing }))
    }

    #[cfg(any(test, feature = "test-util"))]
    fn fault_check(&mut self) -> Result<()> {
        match self.fault.take() {
            Some(crate::testing::FaultPoint::AfterCommitBeforeWrite) => Err(Error::InjectedFault),
            None => Ok(()),
        }
    }

    #[cfg(not(any(test, feature = "test-util")))]
    fn fault_check(&mut self) -> Result<()> {
        Ok(())
    }
```

and in the test-util `impl Replica` block:

```rust
    pub fn inject_fault(&mut self, point: crate::testing::FaultPoint) {
        self.fault = Some(point);
    }
```

4. First lines of `scan`, before `let filter`:

```rust
        if let Some(reason) = self.pause_reason()? {
            return Ok(ScanOutcome::Paused(reason));
        }
```

First lines of `apply` and of `fetch_missing`, before `let filter`:

```rust
        if let Some(reason) = self.pause_reason()? {
            return Err(Error::Paused(reason));
        }
```

5. In `settle`, put the fault check immediately before each file operation:
   - in the `Content::Tombstone` arm, as the first statement: `self.fault_check()?;`
   - in the `Content::File` arm, between the `let Some(staged) = …` block and `place(&staged, &abs)?;`: `self.fault_check()?;`

   The staged file is written before the fault fires, which is what lets the reopened replica finish the write without a peer.

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test -p sapphire-framework-sync`
Expected: the `replica_guards` tests pass (5 on Linux; 7 on Windows; 6 on macOS) along with everything else. `a_lost_store_reconverges_without_conflict_copies` already passes before Step 3; it guards the recovery claim of spec §2.2.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync
git commit -m "feat(sync): pause on a missing root, finish interrupted writes, platform path skips"
```

---

### Task 8: Property test — convergence, no silent loss, idempotence

**Files:**
- Create: `crates/sapphire-framework-sync/tests/convergence.rs`

**Interfaces:**
- Consumes: `tests/common` (`node`, `write`, `remove`, `scan`, `push`, `tree`), `Replica::{delta_for, apply, fetch_missing, states}`

This is the primary guarantee of spec §6.1. Random writes, deletes and (possibly interrupted) pushes run on 2–4 replicas. The replicas are then fully synced, and three properties are checked:

- **Convergence:** stores and file trees are identical everywhere.
- **No silent loss:** every recorded file version is still present somewhere in the final tree, unless a later recorded write on the same path had it in its context.
- **Idempotence:** re-applying a full delta changes nothing.

If this test finds a counterexample, proptest prints a minimal op sequence and saves it under `tests/convergence.proptest-regressions`. Fix the code, keep the regression file, and commit it.

- [ ] **Step 1: Write the test**

`tests/convergence.rs`:

```rust
mod common;

use std::collections::BTreeSet;

use common::*;
use proptest::prelude::*;
use sapphire_framework_sync::{Content, ContentHash, Entry, VersionVector};

const PATHS: &[&str] = &["a.txt", "b.txt", "dir/c.txt"];
const BODIES: &[&str] = &["one", "two", "three", "four"];
const MAX_NODES: usize = 4;

#[derive(Clone, Debug)]
enum Op {
    Write { node: usize, path: usize, body: usize },
    Delete { node: usize, path: usize },
    Push { from: usize, to: usize },
    /// A session cut short: only the first `take` updates arrive, and no commit.
    PartialPush { from: usize, to: usize, take: usize },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..MAX_NODES, 0..PATHS.len(), 0..BODIES.len())
            .prop_map(|(node, path, body)| Op::Write { node, path, body }),
        1 => (0..MAX_NODES, 0..PATHS.len()).prop_map(|(node, path)| Op::Delete { node, path }),
        3 => (0..MAX_NODES, 0..MAX_NODES).prop_map(|(from, to)| Op::Push { from, to }),
        1 => (0..MAX_NODES, 0..MAX_NODES, 0..4usize)
            .prop_map(|(from, to, take)| Op::PartialPush { from, to, take }),
    ]
}

/// A shared and an exclusive borrow of two different nodes.
fn two(nodes: &mut [Node], from: usize, to: usize) -> (&Node, &mut Node) {
    assert_ne!(from, to);
    if from < to {
        let (left, right) = nodes.split_at_mut(to);
        (&left[from], &mut right[0])
    } else {
        let (left, right) = nodes.split_at_mut(from);
        (&right[0], &mut left[to])
    }
}

type Logical = Vec<(String, Vec<Entry>, VersionVector, Option<ContentHash>)>;

fn logical(node: &Node) -> Logical {
    node.replica
        .states()
        .unwrap()
        .into_iter()
        .map(|(path, s)| (path, s.versions, s.seen, s.disk.hash))
        .collect()
}

fn run(n: usize, ops: &[Op]) -> Result<(), TestCaseError> {
    let mut nodes: Vec<Node> = (1..=n as u64).map(node).collect();
    let mut recorded: Vec<Entry> = Vec::new();

    for op in ops {
        match *op {
            Op::Write { node, path, body } => {
                let i = node % n;
                write(&nodes[i], PATHS[path], BODIES[body]);
                recorded.extend(scan(&mut nodes[i]).recorded);
            }
            Op::Delete { node, path } => {
                let i = node % n;
                remove(&nodes[i], PATHS[path]);
                recorded.extend(scan(&mut nodes[i]).recorded);
            }
            Op::Push { from, to } if from % n != to % n => {
                let (f, t) = two(&mut nodes, from % n, to % n);
                recorded.extend(push(f, t).recorded);
            }
            Op::PartialPush { from, to, take } if from % n != to % n => {
                let (f, t) = two(&mut nodes, from % n, to % n);
                let updates = f.replica.delta_for(t.replica.vv()).unwrap();
                let k = take.min(updates.len());
                recorded.extend(t.replica.apply(&updates[..k], &f.replica).unwrap().recorded);
            }
            _ => {}
        }
    }

    // Everyone syncs with everyone until nothing moves.
    for _ in 0..6 {
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    let (f, t) = two(&mut nodes, i, j);
                    recorded.extend(push(f, t).recorded);
                }
            }
        }
        for node in nodes.iter_mut() {
            recorded.extend(scan(node).recorded);
        }
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    let (source, target) = two(&mut nodes, j, i);
                    recorded.extend(target.replica.fetch_missing(&source.replica).unwrap().recorded);
                }
            }
        }
    }

    // Convergence.
    let reference = logical(&nodes[0]);
    let reference_tree = tree(&nodes[0]);
    for other in &nodes[1..] {
        prop_assert_eq!(logical(other), reference.clone());
        prop_assert_eq!(tree(other), reference_tree.clone());
    }

    // No silent loss.
    let present: BTreeSet<ContentHash> =
        reference_tree.values().map(|body| ContentHash::of_bytes(body.as_bytes())).collect();
    for e in &recorded {
        let Content::File { hash, .. } = e.content else {
            continue;
        };
        let overwritten = recorded.iter().any(|later| later.path == e.path && later.context.covers_dot(&e.dot));
        if !overwritten {
            prop_assert!(present.contains(&hash), "version lost: {:?}", e);
        }
    }

    // Idempotence.
    let before = logical(&nodes[1]);
    let (f, t) = two(&mut nodes, 0, 1);
    let everything = f.replica.delta_for(&VersionVector::new()).unwrap();
    let report = t.replica.apply(&everything, &f.replica).unwrap();
    prop_assert_eq!(report.changed, 0);
    prop_assert_eq!(logical(&nodes[1]), before);
    Ok(())
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(48)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn replicas_converge_without_losing_versions(
        n in 2usize..=MAX_NODES,
        ops in prop::collection::vec(op(), 1..30),
    ) {
        run(n, &ops)?;
    }
}

/// The ordering that breaks a single-winner merge (see `merge::tests`), end to end.
#[test]
fn delete_after_edit_concurrent_with_another_edit() {
    let ops = [
        Op::Write { node: 0, path: 0, body: 0 },
        Op::Write { node: 2, path: 0, body: 1 },
        Op::Push { from: 0, to: 1 },
        Op::Delete { node: 1, path: 0 },
        Op::Push { from: 0, to: 2 },
        Op::Push { from: 1, to: 2 },
    ];
    run(3, &ops).unwrap();
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p sapphire-framework-sync --test convergence`
Expected: both tests pass. On failure, read the shrunk op sequence proptest prints, reproduce it as a named `#[test]` like `delete_after_edit_concurrent_with_another_edit`, fix `replica.rs` or `merge.rs`, and re-run. Do not weaken the properties.

- [ ] **Step 3: Check the run time**

Run: `PROPTEST_CASES=200 cargo test -p sapphire-framework-sync --release --test convergence` (PowerShell: `$env:PROPTEST_CASES=200; cargo test ...`)
Expected: passes. Note the debug-mode duration of the default 48 cases in the commit message; if it exceeds ~60 s on CI, lower the default in `cases()`.

- [ ] **Step 4: Lint and commit**

Run: `cargo fmt --all && cargo clippy -p sapphire-framework-sync --all-targets --all-features -- -D warnings`

```bash
git add crates/sapphire-framework-sync
git commit -m "test(sync): property test for convergence, no silent loss and idempotence"
```

---

### Task 9: Golden behaviour test for format 1 and final verification

**Files:**
- Create: `crates/sapphire-framework-sync/tests/golden.rs`
- Create: `crates/sapphire-framework-sync/tests/golden/format-1/concurrent-edit.json` (generated, then reviewed)

**Interfaces:**
- Consumes: `Replica::{open_with_replica_id, logical_dump}`, `testing::ManualClock`, `FORMAT_VERSION`

Spec §5.4: a node built from one app's framework version may sync another app's workspaces, so any change in merge behaviour must bump `FORMAT_VERSION`. This test pins format 1's behaviour on a fixed scenario with fixed replica ids and a manual clock. If it fails after a code change, either the change is a bug or it needs a format bump plus a new `format-2` golden. Never just regenerate `format-1`.

- [ ] **Step 1: Write the test**

`tests/golden.rs`:

```rust
use std::path::{Path, PathBuf};
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_framework_sync::testing::ManualClock;
use sapphire_framework_sync::{FORMAT_VERSION, Replica, ReplicaConfig, ReplicaId, ScanOutcome};

struct Fixed {
    replica: Replica,
    root: PathBuf,
    clock: Arc<ManualClock>,
    _dir: tempfile::TempDir,
}

fn fixed(n: u64) -> Fixed {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join(".test-app")).unwrap();
    let config = ReplicaConfig::new("test-app", &root, GrainId::from_u64(n).unwrap(), &dir.path().join("state"));
    let clock = ManualClock::new(0);
    let id = ReplicaId(uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 + u128::from(n)));
    let replica = Replica::open_with_replica_id(config, clock.clone(), id).unwrap();
    Fixed { replica, root, clock, _dir: dir }
}

fn write_at(node: &mut Fixed, now: u64, rel: &str, body: Option<&str>) {
    node.clock.set(now);
    let path = node.root.join(rel);
    match body {
        Some(body) => {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        }
        None => std::fs::remove_file(path).unwrap(),
    }
    assert!(matches!(node.replica.scan().unwrap(), ScanOutcome::Scanned(_)));
}

fn push(from: &Fixed, to: &mut Fixed, now: u64) {
    to.clock.set(now);
    let from_vv = from.replica.vv().clone();
    let updates = from.replica.delta_for(to.replica.vv()).unwrap();
    to.replica.apply(&updates, &from.replica).unwrap();
    to.replica.commit_session(&from_vv).unwrap();
}

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(format!("format-{FORMAT_VERSION}"))
        .join(name)
}

#[test]
fn concurrent_edit_scenario_matches_the_format_golden() {
    let mut a = fixed(1);
    let mut b = fixed(2);

    write_at(&mut a, 1_000, "a.txt", Some("base"));
    push(&a, &mut b, 1_100);
    write_at(&mut a, 2_000, "a.txt", Some("from a"));
    write_at(&mut b, 3_000, "a.txt", Some("from b"));
    write_at(&mut b, 3_100, "dir/b.txt", Some("only b"));
    push(&a, &mut b, 4_000);
    push(&b, &mut a, 4_100);
    push(&a, &mut b, 4_200);
    write_at(&mut a, 5_000, "dir/b.txt", None);
    push(&a, &mut b, 5_100);

    let actual = serde_json::json!({
        "format_version": FORMAT_VERSION,
        "a": a.replica.logical_dump().unwrap(),
        "b": b.replica.logical_dump().unwrap(),
    });

    let path = golden_path("concurrent-edit.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        return;
    }
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("missing {}; run with UPDATE_GOLDEN=1 once and review it", path.display())),
    )
    .unwrap();
    assert_eq!(
        actual, expected,
        "merge behaviour changed for format {FORMAT_VERSION}: fix the regression, or bump FORMAT_VERSION and add a new golden"
    );
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p sapphire-framework-sync --test golden`
Expected: FAIL with "missing …concurrent-edit.json; run with UPDATE_GOLDEN=1 once and review it".

- [ ] **Step 3: Generate and review the golden**

Run: `UPDATE_GOLDEN=1 cargo test -p sapphire-framework-sync --test golden` (PowerShell: `$env:UPDATE_GOLDEN=1; cargo test -p sapphire-framework-sync --test golden; Remove-Item Env:UPDATE_GOLDEN`)

Review `tests/golden/format-1/concurrent-edit.json` by hand. It must show, for both `a` and `b`:
- `paths["a.txt"].versions` with **two** entries (hashes of "from a" and "from b"), and the "from b" one has the larger `hlc`;
- a path `a.conflict-<grain-id>-2.txt` whose single version holds the hash of "from a" (replica 1's second write);
- `paths["dir/b.txt"].versions` with one tombstone;
- identical `files` maps in `a` and `b`: `a.txt` → hash of "from b", the conflict copy → hash of "from a", no `dir/b.txt`.

If any of these do not hold, the implementation is wrong: fix it (with a failing unit or integration test first), then regenerate.

- [ ] **Step 4: Run it to see it pass**

Run: `cargo test -p sapphire-framework-sync --test golden`
Expected: PASS.

- [ ] **Step 5: Full verification (CI parity)**

Run each and confirm it is clean:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
cargo tree -p sapphire-framework-sync -e normal | grep -E "sapphire-framework-(workspace|retrieve|track)" && echo "FORBIDDEN DEPENDENCY" || echo "dependency check ok"
```

Expected: fmt/clippy silent, all workspace tests pass, `dependency check ok`.

- [ ] **Step 6: Commit**

```bash
git add crates/sapphire-framework-sync
git commit -m "test(sync): pin format-1 merge behaviour with a golden scenario"
```
