# Sync Runtime (`sapphire-framework-session`, `-server`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Make two app servers on different hosts converge — put a `Replica` behind every
synced workspace, run a replication session over the stream the bridge splices, notice local
edits, and expose `sync.enable` / `disable` / `status`.

**Architecture:** A new crate, `sapphire-framework-session`, carries the one thing two
replicas say to each other: a framed exchange of version vectors, path updates and content.
It is the only place that knows the wire format, and both the app server and (in step 8) the
bridge run it. In the app server, a `SyncRuntime` holds one `Replica` per synced workspace,
registers them with the bridge, answers `bridge.incoming` announcements, and dials peers when
something changes locally.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), `sapphire-framework-sync`,
`sapphire-framework-bridge-api`, tokio 1, serde + serde_json, notify 8, grain-id 0.16,
thiserror 2, tracing; dev: tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.2 and §4.3;
`docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` §2 (authoritative, implemented),
§3.2 (`sync-id` and registration) and §3.7 (sessions), read through the substitution table at
the head of its §3. Implementation order step 7 of the process-architecture spec's §9.

**Depends on:** steps 3, 4 and 6 (`2026-09-16-ipc-layer-plan.md`,
`2026-09-16-app-server-plan.md`, `2026-09-16-bridge-basics-plan.md`).

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task. Commit
  `Cargo.lock` whenever dependencies change.
- Every public item carries a doc comment; the new crate has `#![warn(missing_docs)]`.
- `sapphire-framework-session` must **not** depend on `-workspace`, `-retrieve`, `-backend`,
  `-bridge` or iroh. It takes a byte stream and a `Replica`.
- `SESSION_FORMAT_VERSION` is `1`, sent in the first message and checked.
- Inline content limit is **64 KiB**; anything larger is fetched by hash in the same session.
- Maximum frame payload is 64 MiB, matching `-sync`'s `DEFAULT_MAX_FILE_SIZE`.
- A workspace's sync identity is a grain-id stored at `.<app_name>/sync-id`. It is the same
  on every device, is itself synced, and is never derived from the path —
  `Workspace::uuid` is path-derived and therefore differs between hosts.

## Two substitutions from the sync spec's §3.7

That section described one process holding every replica, so a connection carried a control
stream plus one stream per shared workspace.

1. **There is no `Hello` listing workgroups.** The bridge authorizes the peer before an app
   server hears of it (bridge plan, Task 6), and each spliced stream names exactly one
   workspace. The session therefore starts at `SessionHello`.
2. **Which workspaces a peer holds is not negotiated.** A dialler asks for one workspace; if
   the peer does not host it, the bridge there refuses and the stream never reaches an app
   server. That is cheap and needs no extra protocol. Discovering which workspaces *exist* in
   the workgroup is step 8's `workspaces/<id>.toml`.

## File Structure

```
crates/sapphire-framework-session/
    Cargo.toml
    src/
        lib.rs        # module wiring, SESSION_FORMAT_VERSION, INLINE_LIMIT
        error.rs      # Error, Result
        frame.rs      # tag + length framing, read_frame / write_frame
        message.rs    # Message, PathUpdatePage
        session.rs    # run_session

crates/sapphire-framework-server/src/
    sync/
        mod.rs        # SyncRuntime
        id.rs         # sync-id: read or create .<app>/sync-id
        watch.rs      # the file watcher
        methods.rs    # sync.enable / sync.disable / sync.status
```

---

### Task 1: Framing and messages

**Files:**
- Create: `crates/sapphire-framework-session/{Cargo.toml,src/lib.rs,src/error.rs,src/frame.rs,src/message.rs}`
- Modify: `Cargo.toml` (workspace `members`), facade
- Test: inline `#[cfg(test)] mod tests` in `frame.rs` and `message.rs`

**Interfaces:**
- Produces:
  - `SESSION_FORMAT_VERSION: u32 = 1`, `INLINE_LIMIT: usize = 64 * 1024`,
    `MAX_FRAME_LEN: usize = 64 * 1024 * 1024`
  - `Error::{Io, Codec, Protocol, FrameTooLarge, VersionMismatch, Sync, Refused}`, `Result<T>`
  - `Message::{Hello { format, workspace_id, replica_id, vv }, Updates(Vec<PathUpdate>), Done, Want(ContentHash), Missing(ContentHash), Refused(String)}`
  - `async write_control<W>(w: &mut W, msg: &Message) -> Result<()>`
  - `async write_blob<W>(w: &mut W, hash: &ContentHash, bytes: &[u8]) -> Result<()>`
  - `Frame::{Control(Message), Blob { hash: ContentHash, bytes: Vec<u8> }}`
  - `async read_frame<R>(r: &mut R) -> Result<Option<Frame>>` — `None` at end of stream

**The framing, and why it is not NDJSON:** content is bytes. Base64 inside JSON would inflate
every file by a third for no benefit, and the `-ipc` crate's line framing exists for a
different job. A frame is one tag byte, a four-byte big-endian length, then the payload:
control frames carry JSON, blob frames carry a 32-byte hash followed by the content.

- [ ] **Step 1: Create the manifest**

`crates/sapphire-framework-session/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-session"
version.workspace = true
edition.workspace = true
description = "The replication session two sapphire-framework replicas run over a byte stream"
license.workspace = true
repository.workspace = true
keywords = ["sync", "replication", "protocol", "local-first"]
categories = ["network-programming"]

[dependencies]
sapphire-sync = { package = "sapphire-framework-sync", version = "0.14.0", path = "../sapphire-framework-sync" }
grain-id.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["io-util"] }
tracing.workspace = true

[dev-dependencies]
sapphire-sync = { package = "sapphire-framework-sync", version = "0.14.0", path = "../sapphire-framework-sync", features = ["test-util"] }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "io-util", "time"] }
tempfile = "3"
```

Root `Cargo.toml`: add `"crates/sapphire-framework-session",`. Facade: feature `session`.

- [ ] **Step 2: Write the failing tests**

`crates/sapphire-framework-session/src/frame.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_sync::{ContentHash, VersionVector};

    fn hello() -> Message {
        Message::Hello {
            format: SESSION_FORMAT_VERSION,
            workspace_id: grain_id::GrainId::random(),
            replica_id: sapphire_sync::ReplicaId::new(),
            vv: VersionVector::new(),
        }
    }

    #[tokio::test]
    async fn a_control_frame_round_trips() {
        let mut buf: Vec<u8> = Vec::new();
        let msg = hello();
        write_control(&mut buf, &msg).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        match read_frame(&mut cursor).await.unwrap().unwrap() {
            Frame::Control(back) => assert_eq!(format!("{back:?}"), format!("{msg:?}")),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_blob_frame_round_trips_without_growing() {
        let content = vec![0xABu8; 100_000];
        let hash = ContentHash::of_bytes(&content);

        let mut buf: Vec<u8> = Vec::new();
        write_blob(&mut buf, &hash, &content).await.unwrap();
        // Tag, length, hash, content — and not a byte more.
        assert_eq!(buf.len(), 1 + 4 + 32 + content.len());

        let mut cursor = std::io::Cursor::new(buf);
        match read_frame(&mut cursor).await.unwrap().unwrap() {
            Frame::Blob { hash: back, bytes } => {
                assert_eq!(back, hash);
                assert_eq!(bytes, content);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn several_frames_read_back_in_order() {
        let mut buf: Vec<u8> = Vec::new();
        write_control(&mut buf, &hello()).await.unwrap();
        write_control(&mut buf, &Message::Done).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        assert!(matches!(read_frame(&mut cursor).await.unwrap(), Some(Frame::Control(Message::Hello { .. }))));
        assert!(matches!(read_frame(&mut cursor).await.unwrap(), Some(Frame::Control(Message::Done))));
        assert!(read_frame(&mut cursor).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_oversized_length_is_refused_before_allocating() {
        // Tag 0, length just over the limit, and nothing else: a reader that trusted the
        // length would try to allocate 64 MiB + 1 before discovering the stream is empty.
        let mut buf = vec![0u8];
        buf.extend_from_slice(&((MAX_FRAME_LEN + 1) as u32).to_be_bytes());

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, Error::FrameTooLarge { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn an_unknown_tag_is_a_protocol_error() {
        let mut buf = vec![99u8];
        buf.extend_from_slice(&0u32.to_be_bytes());
        let mut cursor = std::io::Cursor::new(buf);
        assert!(matches!(
            read_frame(&mut cursor).await.unwrap_err(),
            Error::Protocol(_)
        ));
    }

    #[tokio::test]
    async fn a_truncated_frame_is_an_error_not_a_silent_short_read() {
        let mut buf = vec![0u8];
        buf.extend_from_slice(&64u32.to_be_bytes());
        buf.extend_from_slice(b"only ten b");

        let mut cursor = std::io::Cursor::new(buf);
        assert!(read_frame(&mut cursor).await.is_err());
    }

    #[tokio::test]
    async fn a_blob_frame_shorter_than_its_hash_is_refused() {
        let mut buf = vec![1u8];
        buf.extend_from_slice(&8u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        let mut cursor = std::io::Cursor::new(buf);
        assert!(matches!(
            read_frame(&mut cursor).await.unwrap_err(),
            Error::Protocol(_)
        ));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-session`
Expected: FAIL — the crate has no code.

- [ ] **Step 4: Implement framing and messages**

`crates/sapphire-framework-session/src/message.rs`:

```rust
//! What two replicas say to each other.

use grain_id::GrainId;
use sapphire_sync::{ContentHash, PathUpdate, ReplicaId, VersionVector};
use serde::{Deserialize, Serialize};

/// One control message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Message {
    /// The first message from each side.
    Hello {
        /// Session format this side speaks.
        format: u32,
        /// Which workspace this session is about.
        workspace_id: GrainId,
        /// The sender's replica id.
        replica_id: ReplicaId,
        /// Everything the sender already has.
        vv: VersionVector,
    },
    /// A page of path states the peer's version vector does not cover.
    Updates(Vec<PathUpdate>),
    /// The sender has sent everything it had at `Hello` time.
    Done,
    /// The sender needs the content behind this hash.
    Want(ContentHash),
    /// The sender does not have the content behind this hash either.
    Missing(ContentHash),
    /// The sender will not continue, and why.
    Refused(String),
}
```

`crates/sapphire-framework-session/src/frame.rs`:

```rust
//! Framing: one tag byte, a four-byte big-endian length, then the payload.
//!
//! Not the line framing of `sapphire-framework-ipc`: content is bytes, and base64 inside
//! JSON would inflate every file by a third to no purpose.

use sapphire_sync::ContentHash;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::message::Message;
use crate::MAX_FRAME_LEN;

const TAG_CONTROL: u8 = 0;
const TAG_BLOB: u8 = 1;
const HASH_LEN: usize = 32;

/// One frame off the wire.
#[derive(Debug)]
pub enum Frame {
    /// A control message.
    Control(Message),
    /// Content, addressed by its hash.
    Blob {
        /// The content's hash, which the receiver must verify.
        hash: ContentHash,
        /// The content.
        bytes: Vec<u8>,
    },
}

/// Send a control message.
pub async fn write_control<W>(w: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(msg)?;
    write_framed(w, TAG_CONTROL, &payload).await
}

/// Send content.
pub async fn write_blob<W>(w: &mut W, hash: &ContentHash, bytes: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(HASH_LEN + bytes.len());
    payload.extend_from_slice(&hash.0);
    payload.extend_from_slice(bytes);
    write_framed(w, TAG_BLOB, &payload).await
}

async fn write_framed<W>(w: &mut W, tag: u8, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if payload.len() > MAX_FRAME_LEN {
        return Err(Error::FrameTooLarge { len: payload.len(), max: MAX_FRAME_LEN });
    }
    w.write_u8(tag).await?;
    w.write_u32(payload.len() as u32).await?;
    w.write_all(payload).await?;
    w.flush().await?;
    Ok(())
}

/// Read one frame. `None` at a clean end of stream.
pub async fn read_frame<R>(r: &mut R) -> Result<Option<Frame>>
where
    R: AsyncRead + Unpin,
{
    let tag = match r.read_u8().await {
        Ok(tag) => tag,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    };
    let len = r.read_u32().await? as usize;
    // Check before allocating: a peer that announces 4 GiB must not make us try.
    if len > MAX_FRAME_LEN {
        return Err(Error::FrameTooLarge { len, max: MAX_FRAME_LEN });
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;

    match tag {
        TAG_CONTROL => Ok(Some(Frame::Control(serde_json::from_slice(&payload)?))),
        TAG_BLOB => {
            if payload.len() < HASH_LEN {
                return Err(Error::Protocol(format!(
                    "a blob frame of {} bytes cannot hold a hash",
                    payload.len()
                )));
            }
            let mut hash = [0u8; HASH_LEN];
            hash.copy_from_slice(&payload[..HASH_LEN]);
            Ok(Some(Frame::Blob {
                hash: ContentHash(hash),
                bytes: payload[HASH_LEN..].to_vec(),
            }))
        }
        other => Err(Error::Protocol(format!("unknown frame tag {other}"))),
    }
}
```

Write `error.rs` and `lib.rs` with the items from the Interfaces block.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-session`
Expected: PASS, 7 tests.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-session Cargo.toml Cargo.lock crates/sapphire-framework
git commit -m "feat(session): frame replication messages and content"
```

---

### Task 2: The session

**Files:**
- Create: `crates/sapphire-framework-session/src/session.rs`
- Modify: `crates/sapphire-framework-session/src/lib.rs`
- Test: `crates/sapphire-framework-session/tests/converge.rs`

**Interfaces:**
- Consumes: Task 1; `Replica`, `PathUpdate`, `VersionVector`, `ContentSource` from `-sync`
- Produces:
  - `SessionOutcome { sent: usize, received: usize, report: Report }`
  - `async run_session<S>(stream: S, replica: &mut Replica, workspace_id: GrainId) -> Result<SessionOutcome>`
    where `S: AsyncRead + AsyncWrite + Send + Unpin`

**The order, which the sync spec's §3.7 fixes and the tests pin:**

1. Both sides send `Hello`. A format mismatch, or a different `workspace_id`, ends the session
   with `Refused` — before any state is exchanged.
2. Each side sends the path states the peer's `vv` does not cover, in pages, inlining content
   of 64 KiB or less, then `Done`.
3. Updates are joined **as they arrive**. The join is idempotent, so a redelivered page is
   harmless.
4. Content still missing is asked for with `Want` and answered with a blob or `Missing`.
5. **`commit_session` runs only after the peer's `Done`.** An interrupted session leaves the
   version vector where it was, and everything is simply resent next time. Committing early
   would make the replica claim to have versions whose content never arrived.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-session/tests/converge.rs`:

```rust
//! Two replicas in one process, connected by a duplex, must converge.

use std::path::Path;

use sapphire_framework_session::run_session;
use sapphire_sync::{Replica, ReplicaConfig, SystemClock};

struct Fixture {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    replica: Replica,
}

fn replica(name: &str, device: grain_id::GrainId) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join(name);
    std::fs::create_dir_all(root.join(".test-app")).unwrap();
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let config = ReplicaConfig::new("test-app", root.clone(), device, &state);
    let replica = Replica::open(config, std::sync::Arc::new(SystemClock)).unwrap();
    Fixture { _tmp: tmp, root, replica }
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// Run one session between `a` and `b` over an in-process duplex.
async fn sync(a: &mut Replica, b: &mut Replica, ws: grain_id::GrainId) {
    let (left, right) = tokio::io::duplex(64 * 1024);
    let (ra, rb) = (a, b);
    let (x, y) = tokio::join!(run_session(left, ra, ws), run_session(right, rb, ws));
    x.unwrap();
    y.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_written_on_one_side_appears_on_the_other() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "notes/hello.md", "# hello");
    a.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    assert_eq!(
        std::fs::read_to_string(b.root.join("notes/hello.md")).unwrap(),
        "# hello"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_larger_than_the_inline_limit_is_fetched_by_hash() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    let big = "x".repeat(300_000);
    write(&a.root, "big.md", &big);
    a.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    assert_eq!(std::fs::read_to_string(b.root.join("big.md")).unwrap().len(), 300_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_session_sends_nothing_new() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "a.md", "one");
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, _) = tokio::join!(
        run_session(left, &mut a.replica, ws),
        run_session(right, &mut b.replica, ws)
    );
    assert_eq!(x.unwrap().sent, 0, "nothing new should be sent the second time");
}

#[tokio::test(flavor = "multi_thread")]
async fn edits_on_both_sides_both_arrive() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "from-a.md", "a");
    write(&b.root, "from-b.md", "b");
    a.replica.scan().unwrap();
    b.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    assert!(b.root.join("from-a.md").exists());
    assert!(a.root.join("from-b.md").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deletion_propagates() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "doomed.md", "x");
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;
    assert!(b.root.join("doomed.md").exists());

    std::fs::remove_file(a.root.join("doomed.md")).unwrap();
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;

    assert!(!b.root.join("doomed.md").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_edits_leave_a_conflict_copy_rather_than_losing_one() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "shared.md", "seed");
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;

    // Both edit without seeing the other.
    write(&a.root, "shared.md", "from a");
    write(&b.root, "shared.md", "from b");
    a.replica.scan().unwrap();
    b.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    let names: Vec<String> = std::fs::read_dir(&b.root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n.contains(".conflict-")),
        "the losing edit must survive as a conflict copy; saw {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_for_a_different_workspace_is_refused() {
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        run_session(left, &mut a.replica, grain_id::GrainId::random()),
        run_session(right, &mut b.replica, grain_id::GrainId::random())
    );
    assert!(x.is_err() || y.is_err(), "mismatched workspaces must not exchange state");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_session_does_not_advance_the_version_vector() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "a.md", "one");
    a.replica.scan().unwrap();

    // B's side is dropped as soon as it has said Hello, so A never sees Done.
    let (left, right) = tokio::io::duplex(64 * 1024);
    let before = b.replica.vv().clone();
    let cut = tokio::spawn(async move {
        let mut right = right;
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 16];
        let _ = right.read(&mut buf).await;
        drop(right);
    });
    let _ = run_session(left, &mut a.replica, ws).await;
    cut.await.unwrap();

    assert_eq!(
        b.replica.vv(),
        &before,
        "an interrupted session must leave the version vector untouched"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-session --test converge`
Expected: FAIL — `run_session` does not exist.

- [ ] **Step 3: Implement the session**

`crates/sapphire-framework-session/src/session.rs`:

```rust
//! One replication session over one stream.
//!
//! Both sides run this same function. It is symmetric: each sends what the other lacks and
//! applies what it is sent, and neither is a client.

use std::collections::{HashMap, HashSet};

use grain_id::GrainId;
use sapphire_sync::{
    Content, ContentHash, ContentSource, PathUpdate, Replica, Report, VersionVector,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::frame::{Frame, read_frame, write_blob, write_control};
use crate::message::Message;
use crate::{INLINE_LIMIT, SESSION_FORMAT_VERSION};

/// How many path states travel in one `Updates` message.
const PAGE: usize = 256;

/// What a session did.
#[derive(Clone, Debug, Default)]
pub struct SessionOutcome {
    /// Path updates sent.
    pub sent: usize,
    /// Path updates received.
    pub received: usize,
    /// What applying them did.
    pub report: Report,
}

/// One thing to put on the wire.
enum Out {
    Control(Message),
    Blob(ContentHash, Vec<u8>),
}

/// Content received inline or fetched during this session.
#[derive(Default)]
struct Received(HashMap<ContentHash, Vec<u8>>);

impl ContentSource for Received {
    fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>> {
        self.0.get(hash).cloned()
    }
}

/// Run a session to completion.
///
/// Returns once both sides have said `Done` and every piece of content this side asked for
/// has been answered — with the bytes, or with `Missing`.
pub async fn run_session<S>(
    stream: S,
    replica: &mut Replica,
    workspace_id: GrainId,
) -> Result<SessionOutcome>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Everything we send goes through one task, so the read loop never blocks on a write.
    // Two `run_session` calls facing each other would otherwise deadlock the moment both
    // wrote more than the socket buffer while each waited to read.
    let (out_tx, mut out_rx) = mpsc::channel::<Out>(64);
    let writer_task = tokio::spawn(async move {
        while let Some(item) = out_rx.recv().await {
            let result = match item {
                Out::Control(msg) => write_control(&mut writer, &msg).await,
                Out::Blob(hash, bytes) => write_blob(&mut writer, &hash, &bytes).await,
            };
            if result.is_err() {
                break;
            }
        }
    });

    // 1. Hello, before anything else.
    send(
        &out_tx,
        Out::Control(Message::Hello {
            format: SESSION_FORMAT_VERSION,
            workspace_id,
            replica_id: replica.replica_id(),
            vv: replica.vv().clone(),
        }),
    )
    .await?;

    let peer_vv = match read_frame(&mut reader).await? {
        Some(Frame::Control(Message::Hello { format, workspace_id: theirs, vv, .. })) => {
            if format != SESSION_FORMAT_VERSION {
                let _ = send(
                    &out_tx,
                    Out::Control(Message::Refused(format!(
                        "session format {format}, expected {SESSION_FORMAT_VERSION}"
                    ))),
                )
                .await;
                return finish(writer_task, Err(Error::VersionMismatch {
                    ours: SESSION_FORMAT_VERSION,
                    theirs: format,
                }))
                .await;
            }
            if theirs != workspace_id {
                let _ = send(
                    &out_tx,
                    Out::Control(Message::Refused(format!(
                        "this stream is for workspace {workspace_id}, not {theirs}"
                    ))),
                )
                .await;
                return finish(
                    writer_task,
                    Err(Error::Protocol(format!("workspace mismatch: {theirs}"))),
                )
                .await;
            }
            vv
        }
        Some(Frame::Control(Message::Refused(why))) => {
            return finish(writer_task, Err(Error::Refused(why))).await;
        }
        other => {
            return finish(
                writer_task,
                Err(Error::Protocol(format!("expected Hello, got {other:?}"))),
            )
            .await;
        }
    };

    // 2. Build the whole outgoing side as owned data, then hand it to a task. Computing it
    //    up front keeps the read loop free while it is written.
    let delta = replica.delta_for(&peer_vv).map_err(|e| Error::Sync(e.to_string()))?;
    let sent = delta.len();
    let mut outgoing: Vec<Out> = Vec::new();
    for page in delta.chunks(PAGE) {
        outgoing.push(Out::Control(Message::Updates(page.to_vec())));
        for update in page {
            for entry in &update.versions {
                let Content::File { hash, len } = entry.content else { continue };
                if len as usize > INLINE_LIMIT {
                    continue;
                }
                if let Ok(Some(bytes)) = replica.read_content(&hash) {
                    outgoing.push(Out::Blob(hash, bytes));
                }
            }
        }
    }
    outgoing.push(Out::Control(Message::Done));

    let sender = out_tx.clone();
    let send_task = tokio::spawn(async move {
        for item in outgoing {
            if sender.send(item).await.is_err() {
                break;
            }
        }
    });

    // 3. Read until the peer is done and every hash we asked for has been answered.
    let mut received = Received::default();
    let mut wanted: HashSet<ContentHash> = HashSet::new();
    let mut report = Report::default();
    let mut received_count = 0usize;
    let mut peer_done = false;

    while !peer_done || !wanted.is_empty() {
        let Some(frame) = read_frame(&mut reader).await? else {
            // The peer went away. Everything is resent next session, so this is not an
            // error to the caller — but nothing is committed either.
            send_task.abort();
            return finish(writer_task, Ok(SessionOutcome { sent, received: received_count, report }))
                .await;
        };
        match frame {
            Frame::Control(Message::Updates(updates)) => {
                received_count += updates.len();
                let page = replica
                    .apply(&updates, &received)
                    .map_err(|e| Error::Sync(e.to_string()))?;
                merge_report(&mut report, page);
                // Ask for content the page needs and we do not have.
                for hash in needed(replica, &updates, &received)? {
                    if wanted.insert(hash) {
                        send(&out_tx, Out::Control(Message::Want(hash))).await?;
                    }
                }
            }
            Frame::Control(Message::Done) => peer_done = true,
            Frame::Control(Message::Want(hash)) => {
                let answer = match replica.read_content(&hash) {
                    Ok(Some(bytes)) => Out::Blob(hash, bytes),
                    _ => Out::Control(Message::Missing(hash)),
                };
                send(&out_tx, answer).await?;
            }
            Frame::Control(Message::Missing(hash)) => {
                // The peer does not have it either. Another peer may; the path simply stays
                // unmaterialised until one does.
                wanted.remove(&hash);
            }
            Frame::Control(Message::Refused(why)) => {
                send_task.abort();
                return finish(writer_task, Err(Error::Refused(why))).await;
            }
            Frame::Control(Message::Hello { .. }) => {
                return finish(
                    writer_task,
                    Err(Error::Protocol("a second Hello".to_owned())),
                )
                .await;
            }
            Frame::Blob { hash, bytes } => {
                // Verify before storing: content is addressed by hash, so a peer that sends
                // the wrong bytes for one must not be able to plant them.
                if ContentHash::of_bytes(&bytes) != hash {
                    return finish(
                        writer_task,
                        Err(Error::Protocol(format!("content does not match {hash}"))),
                    )
                    .await;
                }
                wanted.remove(&hash);
                received.0.insert(hash, bytes);
            }
        }
    }

    // 4. Materialise anything that was waiting on content, then commit.
    let fetched = replica
        .fetch_missing(&received)
        .map_err(|e| Error::Sync(e.to_string()))?;
    merge_report(&mut report, fetched);

    // Only now: an interrupted session must leave the version vector where it was, so that
    // everything is simply resent next time. Committing earlier would make this replica
    // claim versions whose content never arrived.
    replica
        .commit_session(&peer_vv)
        .map_err(|e| Error::Sync(e.to_string()))?;

    let _ = send_task.await;
    finish(writer_task, Ok(SessionOutcome { sent, received: received_count, report })).await
}

async fn send(tx: &mpsc::Sender<Out>, item: Out) -> Result<()> {
    tx.send(item).await.map_err(|_| Error::Protocol("the stream closed".to_owned()))
}

/// Drop the sender, let the writer drain, and return `result`.
async fn finish(
    writer: tokio::task::JoinHandle<()>,
    result: Result<SessionOutcome>,
) -> Result<SessionOutcome> {
    writer.abort();
    result
}

/// Hashes these updates need that neither the store nor this session has.
fn needed(
    replica: &Replica,
    updates: &[PathUpdate],
    received: &Received,
) -> Result<Vec<ContentHash>> {
    let mut out = Vec::new();
    for update in updates {
        for entry in &update.versions {
            let Some(hash) = entry.content.hash() else { continue };
            if received.0.contains_key(&hash) {
                continue;
            }
            let have = replica
                .read_content(&hash)
                .map_err(|e| Error::Sync(e.to_string()))?
                .is_some();
            if !have {
                out.push(hash);
            }
        }
    }
    Ok(out)
}

fn merge_report(into: &mut Report, from: Report) {
    into.changed += from.changed;
    into.recorded.extend(from.recorded);
    into.conflicts.extend(from.conflicts);
    into.skipped.extend(from.skipped);
}
```

Three points the tests depend on, stated so they are not optimised away:

- **The outgoing side is computed before the read loop and written by a task.** Two
  `run_session` calls facing each other on a duplex deadlock otherwise: both would be writing
  more than the buffer holds while each waited for the other to read.
- **Every blob's hash is verified before it is stored.** Content is addressed by hash; a peer
  that could store arbitrary bytes under a hash could put them into every other device.
- **`commit_session` runs last**, after the peer's `Done` and after `fetch_missing`. This is
  what `an_interrupted_session_does_not_advance_the_version_vector` checks.

`Report` needs `Default` and public fields for `merge_report`; if `-sync` does not expose them,
add a `Report::merge(&mut self, other: Report)` there instead and call that — do not duplicate
the type.


- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-session --test converge`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-session
git commit -m "feat(session): exchange path states and content until two replicas agree"
```

---

### Task 3: The workspace's sync identity

**Files:**
- Create: `crates/sapphire-framework-server/src/sync/id.rs`
- Modify: `crates/sapphire-framework-server/src/{lib.rs,Cargo.toml}`
- Test: inline `#[cfg(test)] mod tests` in `id.rs`

**Interfaces:**
- Produces:
  - `SYNC_ID_FILE: &str = "sync-id"`
  - `fn sync_id(app_name: &str, root: &Path) -> Result<GrainId>` — read, or create and write
  - `fn sync_id_path(app_name: &str, root: &Path) -> PathBuf` — `<root>/.<app>/sync-id`

**Why not `Workspace::uuid`:** it is derived from the path, so the same workspace on two hosts
has two different uuids. The sync identity has to be the same everywhere, so it is a grain-id
written into the marker directory — and because the file is synced and its content is
identical on every device, it never conflicts. Cache keys go on using the path-derived uuid.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn workspace(tmp: &std::path::Path) -> std::path::PathBuf {
        let root = tmp.join("ws");
        std::fs::create_dir_all(root.join(".test-app")).unwrap();
        root
    }

    #[test]
    fn an_id_is_created_once_and_read_back() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());

        let first = sync_id("test-app", &root).unwrap();
        let second = sync_id("test-app", &root).unwrap();
        assert_eq!(first, second, "the id must not change between calls");
    }

    #[test]
    fn the_id_lives_in_the_marker_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        let id = sync_id("test-app", &root).unwrap();

        let path = root.join(".test-app").join("sync-id");
        assert_eq!(std::fs::read_to_string(path).unwrap().trim(), id.to_string());
    }

    #[test]
    fn an_id_that_arrived_by_sync_is_used_as_is() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        let theirs = grain_id::GrainId::random();
        std::fs::write(root.join(".test-app").join("sync-id"), theirs.to_string()).unwrap();

        assert_eq!(sync_id("test-app", &root).unwrap(), theirs);
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        let theirs = grain_id::GrainId::random();
        std::fs::write(
            root.join(".test-app").join("sync-id"),
            format!("  {theirs}\n"),
        )
        .unwrap();

        assert_eq!(sync_id("test-app", &root).unwrap(), theirs);
    }

    #[test]
    fn a_corrupt_id_file_is_an_error_not_a_silent_new_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        std::fs::write(root.join(".test-app").join("sync-id"), "not an id!").unwrap();

        let err = sync_id("test-app", &root).unwrap_err();
        assert!(err.to_string().contains("sync-id"), "{err}");
    }

    #[test]
    fn a_missing_marker_directory_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let err = sync_id("test-app", &tmp.path().join("plain")).unwrap_err();
        assert!(err.to_string().contains("test-app"), "{err}");
    }
}
```

`a_corrupt_id_file_is_an_error_not_a_silent_new_identity` matters more than it looks:
overwriting an unreadable id would give this host a second identity for a workspace that
already exists elsewhere, and both copies would then sync as unrelated workspaces for ever.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features sync::id`
Expected: FAIL.

- [ ] **Step 3: Implement it**

```rust
//! A workspace's identity across devices.

use std::path::{Path, PathBuf};

use grain_id::GrainId;

use crate::error::{Error, Result};

/// The file inside the marker directory that names the workspace.
pub const SYNC_ID_FILE: &str = "sync-id";

/// `<root>/.<app_name>/sync-id`.
pub fn sync_id_path(app_name: &str, root: &Path) -> PathBuf {
    root.join(format!(".{app_name}")).join(SYNC_ID_FILE)
}

/// The workspace's sync identity, creating it on first use.
///
/// The file is synced and holds the same bytes on every device, so it never conflicts.
pub fn sync_id(app_name: &str, root: &Path) -> Result<GrainId> {
    let marker = root.join(format!(".{app_name}"));
    if !marker.is_dir() {
        return Err(Error::UnknownWorkspace(root.to_owned(), "this application's"));
    }
    let path = sync_id_path(app_name, root);
    match std::fs::read_to_string(&path) {
        Ok(text) => text.trim().parse().map_err(|_| {
            Error::Privilege(format!(
                "{}: the sync-id is unreadable; refusing to mint a new identity for a \
                 workspace that may already exist elsewhere",
                path.display()
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = GrainId::random();
            std::fs::write(&path, format!("{id}\n"))?;
            Ok(id)
        }
        Err(e) => Err(Error::Io(e)),
    }
}
```

> `Error::Privilege` is the wrong variant for this. Add `Error::Workspace`-adjacent
> `Error::SyncId(String)` to `crates/sapphire-framework-server/src/error.rs` rendering as
> `{0}`, and use it here.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features sync::id`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): give each workspace an identity that survives across devices"
```

---

### Task 4: `SyncRuntime`

**Files:**
- Create: `crates/sapphire-framework-server/src/sync/mod.rs`
- Create: `crates/sapphire-framework-server/src/sync/testing.rs`
- Modify: `crates/sapphire-framework-server/src/{lib.rs,error.rs}`, `Cargo.toml`
- Test: inline `#[cfg(test)] mod tests` in `sync/mod.rs`

**Interfaces:**
- Consumes: `sync_id` (Task 3), `run_session` (Task 2), `BridgeClient` (bridge plan)
- Produces:
  - `SyncStatus { enabled: bool, workspace_id: Option<GrainId>, peers: usize, paused: Option<String>, last_error: Option<String>, bridge_available: bool }`
  - `SyncRuntime::new(ctx: &'static AppContext, bridge: Arc<BridgeClient>, exe_path: PathBuf, managed_by: ManagedBy) -> SyncRuntime`
  - `async enable(&self, root: &Path) -> Result<GrainId>`
  - `async disable(&self, root: &Path) -> Result<()>`
  - `async status(&self, root: &Path) -> SyncStatus`
  - `async sync_now(&self, root: &Path) -> Result<()>`
  - `async scan(&self, root: &Path) -> Result<()>` — called after the server's own writes
  - `async run(self: Arc<Self>) -> Result<()>` — answers `bridge.incoming` for ever
  - `Error::{Sync, Session, Bridge}` added to the server error enum
  - `testing::StubBridge` (feature `test-util`) — a bridge control plane that records
    registrations and can be told to announce an incoming stream

**Where the replica store lives:** `<workspace cache dir>/sync/`, beside the retrieve and
track stores. The path-derived `Workspace::uuid` is the right key here, because this is
per-host state and two workspaces on one host must not share a store. The replica itself
refuses to open against a different root, which is the guard against two copies of one
workspace on the same machine.

**One `Replica` per workspace, opened once and held**, because it owns a redb database — the
same rule as `WorkspaceHost`, for the same reason. Sessions take that replica's mutex, so two
sessions for one workspace serialise rather than racing.

- [ ] **Step 1: Write the bridge stub**

`crates/sapphire-framework-server/src/sync/testing.rs`:

```rust
//! A bridge that records what it was told, for testing `SyncRuntime` without a real one.

use std::sync::{Arc, Mutex};

use sapphire_bridge_api::{
    Ack, BridgeClient, GrainId, IncomingParams, PeersResult, RegisterParams, RegisterResult,
    StatusResult, UnregisterParams,
};
use sapphire_ipc::{ClientInfo, Connection, ManagedBy, Router, ServerInfo, serve};

/// What a `StubBridge` saw.
#[derive(Clone, Debug, Default)]
pub struct Seen {
    /// Every `bridge.register` it received, in order.
    pub registrations: Vec<RegisterParams>,
    /// Every `bridge.unregister` it received.
    pub unregistrations: Vec<GrainId>,
}

/// A stand-in for the bridge's control plane.
pub struct StubBridge {
    /// What it has been told.
    pub seen: Arc<Mutex<Seen>>,
    /// The device id it answers registrations with.
    pub device_id: GrainId,
    /// The workgroup id it answers registrations with.
    pub workgroup_id: GrainId,
    announcer: sapphire_ipc::Sender,
}

impl StubBridge {
    /// Start a stub on one end of an in-process connection and return a client for the other.
    pub async fn start() -> (StubBridge, Arc<BridgeClient>) {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let device_id = GrainId::random();
        let workgroup_id = GrainId::random();

        let (client_conn, server_conn) = Connection::pair();
        let announcer = server_conn.sender();

        let router = {
            let seen = Arc::clone(&seen);
            Arc::new(
                Router::new()
                    .method(sapphire_bridge_api::REGISTER, move |ctx| {
                        let seen = Arc::clone(&seen);
                        async move {
                            let params: RegisterParams =
                                serde_json::from_value(ctx.params).map_err(|e| {
                                    sapphire_ipc::RpcError::invalid_params(e.to_string())
                                })?;
                            seen.lock().expect("stub").registrations.push(params);
                            serde_json::to_value(RegisterResult {
                                device_id,
                                node_id: "stub".into(),
                                workgroup_id,
                            })
                            .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                        }
                    })
                    .method(sapphire_bridge_api::UNREGISTER, {
                        let seen = Arc::clone(&seen);
                        move |ctx| {
                            let seen = Arc::clone(&seen);
                            async move {
                                let params: UnregisterParams =
                                    serde_json::from_value(ctx.params).map_err(|e| {
                                        sapphire_ipc::RpcError::invalid_params(e.to_string())
                                    })?;
                                seen.lock()
                                    .expect("stub")
                                    .unregistrations
                                    .push(params.workspace_id);
                                serde_json::to_value(Ack {})
                                    .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                            }
                        }
                    })
                    .method(sapphire_bridge_api::PEERS, |_| async move {
                        serde_json::to_value(PeersResult { peers: vec![] })
                            .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                    })
                    .method(sapphire_bridge_api::STATUS, move |_| async move {
                        serde_json::to_value(StatusResult {
                            version: "stub".into(),
                            node_id: "stub".into(),
                            workgroup: None,
                            routes: vec![],
                        })
                        .map_err(|e| sapphire_ipc::RpcError::internal(e.to_string()))
                    }),
            )
        };

        tokio::spawn(async move {
            let info = ServerInfo {
                version: "stub".into(),
                pid: std::process::id(),
                managed_by: ManagedBy::Service,
            };
            let _ = serve(server_conn, router, "bridge", info).await;
        });

        let info = ClientInfo {
            kind: "test".into(),
            version: "stub".into(),
            pid: std::process::id(),
        };
        let (client, _) = sapphire_ipc::Client::handshake(client_conn, "bridge", info)
            .await
            .expect("the stub handshake");
        let client =
            Arc::new(BridgeClient::from_client(Arc::new(client), std::env::temp_dir()));

        (StubBridge { seen, device_id, workgroup_id, announcer }, client)
    }

    /// Pretend a peer wants `workspace_id`.
    pub async fn announce(&self, workspace_id: GrainId, ticket: &str) {
        let params = IncomingParams {
            workspace_id,
            peer_device_id: GrainId::random(),
            ticket: ticket.to_owned(),
        };
        let _ = self
            .announcer
            .send(sapphire_ipc::Message::Notification(sapphire_ipc::Notification {
                method: sapphire_bridge_api::INCOMING.to_owned(),
                params: serde_json::to_value(params).expect("announcement"),
            }))
            .await;
    }

    /// The last registration's workspace list.
    pub fn last_workspaces(&self) -> Vec<GrainId> {
        self.seen
            .lock()
            .expect("stub")
            .registrations
            .last()
            .map(|r| r.workspaces.iter().map(|w| w.workspace_id).collect())
            .unwrap_or_default()
    }
}
```

- [ ] **Step 2: Write the failing tests**

`crates/sapphire-framework-server/src/sync/mod.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::testing::StubBridge;
    use sapphire_workspace::{AppContext, AppKind};

    static CTX: AppContext = AppContext::new("sapphire-synctest");

    fn prepare(tmp: &std::path::Path) -> std::path::PathBuf {
        // SAFETY: the test binary sets these before any other thread reads them.
        unsafe {
            std::env::set_var("SAPPHIRE_SYNCTEST_CACHE_DIR", tmp.join("cache"));
            std::env::set_var("SAPPHIRE_SYNCTEST_DATA_DIR", tmp.join("data"));
            std::env::set_var("SAPPHIRE_SYNCTEST_CONFIG_DIR", tmp.join("config"));
        }
        CTX.init(AppKind::Server);

        let root = tmp.join("ws");
        std::fs::create_dir_all(root.join(".sapphire-synctest")).unwrap();
        root.canonicalize().unwrap()
    }

    async fn runtime(tmp: &std::path::Path) -> (StubBridge, Arc<SyncRuntime>, std::path::PathBuf) {
        let root = prepare(tmp);
        let (stub, client) = StubBridge::start().await;
        let runtime = Arc::new(SyncRuntime::new(
            &CTX,
            client,
            "/bin/true".into(),
            sapphire_ipc::ManagedBy::Service,
        ));
        (stub, runtime, root)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enabling_registers_the_workspace_with_the_bridge() {
        let tmp = tempfile::tempdir().unwrap();
        let (stub, runtime, root) = runtime(tmp.path()).await;

        let id = runtime.enable(&root).await.unwrap();
        assert_eq!(stub.last_workspaces(), vec![id]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enabling_twice_is_idempotent_and_keeps_the_same_id() {
        let tmp = tempfile::tempdir().unwrap();
        let (stub, runtime, root) = runtime(tmp.path()).await;

        let first = runtime.enable(&root).await.unwrap();
        let second = runtime.enable(&root).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(stub.last_workspaces(), vec![first], "still exactly one workspace");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_registration_carries_the_complete_current_list() {
        let tmp = tempfile::tempdir().unwrap();
        let (stub, runtime, first_root) = runtime(tmp.path()).await;
        let second_root = tmp.path().join("ws2");
        std::fs::create_dir_all(second_root.join(".sapphire-synctest")).unwrap();
        let second_root = second_root.canonicalize().unwrap();

        let a = runtime.enable(&first_root).await.unwrap();
        let b = runtime.enable(&second_root).await.unwrap();

        let mut seen = stub.last_workspaces();
        seen.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(seen, want, "registration is the whole set, not a delta");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabling_unregisters_and_leaves_the_files_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let (stub, runtime, root) = runtime(tmp.path()).await;
        std::fs::write(root.join("keep.md"), "content").unwrap();

        let id = runtime.enable(&root).await.unwrap();
        runtime.disable(&root).await.unwrap();

        assert!(stub.last_workspaces().is_empty());
        assert!(root.join("keep.md").exists(), "disabling sync must not touch files");
        assert!(
            crate::sync::id::sync_id_path("sapphire-synctest", &root).exists(),
            "the sync id stays, so re-enabling rejoins the same workspace"
        );
        let _ = id;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn re_enabling_after_disabling_reuses_the_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let (_stub, runtime, root) = runtime(tmp.path()).await;

        let first = runtime.enable(&root).await.unwrap();
        runtime.disable(&root).await.unwrap();
        let again = runtime.enable(&root).await.unwrap();

        assert_eq!(first, again);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_reports_disabled_for_a_workspace_that_was_never_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let (_stub, runtime, root) = runtime(tmp.path()).await;

        let status = runtime.status(&root).await;
        assert!(!status.enabled);
        assert!(status.workspace_id.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabling_something_that_was_never_enabled_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let (_stub, runtime, root) = runtime(tmp.path()).await;
        runtime.disable(&root).await.expect("disabling twice is fine");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_workspace_whose_root_vanished_is_reported_as_paused_not_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let (_stub, runtime, root) = runtime(tmp.path()).await;
        std::fs::write(root.join("a.md"), "content").unwrap();
        runtime.enable(&root).await.unwrap();
        runtime.scan(&root).await.unwrap();

        // The drive is unmounted: the marker directory goes with it.
        std::fs::remove_dir_all(root.join(".sapphire-synctest")).unwrap();
        let _ = runtime.scan(&root).await;

        let status = runtime.status(&root).await;
        assert!(
            status.paused.is_some(),
            "a missing root must pause, not replicate as a mass deletion"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_directory_that_is_not_a_workspace_cannot_be_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let (_stub, runtime, _root) = runtime(tmp.path()).await;
        let plain = tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();

        assert!(runtime.enable(&plain).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_announcement_for_an_unknown_workspace_is_ignored_not_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let (stub, runtime, root) = runtime(tmp.path()).await;
        runtime.enable(&root).await.unwrap();

        let driver = Arc::clone(&runtime);
        let handle = tokio::spawn(async move { driver.run().await });

        stub.announce(grain_id::GrainId::random(), "no-such-ticket").await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(!handle.is_finished(), "one bad announcement must not stop the runtime");
        handle.abort();
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features sync::`
Expected: FAIL — `SyncRuntime` does not exist.

- [ ] **Step 4: Implement `SyncRuntime`**

`crates/sapphire-framework-server/src/sync/mod.rs`:

```rust
//! Replication, from the app server's side.
//!
//! One `Replica` per synced workspace, registered with the bridge, driven by announcements
//! from it and by local edits.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_bridge_api::{
    BridgeClient, ManagedBy, RegisterParams, WorkspaceRegistration,
};
use sapphire_sync::{PauseReason, Replica, ReplicaConfig, ScanOutcome, SystemClock};
use sapphire_workspace::{AppContext, Workspace};
use tokio::sync::Mutex;

pub mod id;
mod methods;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
mod watch;

pub use id::{SYNC_ID_FILE, sync_id, sync_id_path};
pub use methods::sync_router;
pub use watch::{DEBOUNCE, Watcher};

use crate::error::{Error, Result};

/// What `sync.status` reports.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SyncStatus {
    /// Whether this workspace is synced at all.
    pub enabled: bool,
    /// Its identity across devices, when it is.
    pub workspace_id: Option<GrainId>,
    /// How many devices the workgroup has, this host excluded.
    pub peers: usize,
    /// Why replication is paused, if it is — a missing root or marker directory.
    pub paused: Option<String>,
    /// The last failure, if any.
    pub last_error: Option<String>,
    /// Whether the bridge is reachable. `false` is not an outage: the app server works.
    pub bridge_available: bool,
}

struct Synced {
    workspace_id: GrainId,
    replica: Arc<Mutex<Replica>>,
    paused: Option<PauseReason>,
    last_error: Option<String>,
}

/// Replication for one application's workspaces.
pub struct SyncRuntime {
    ctx: &'static AppContext,
    bridge: Arc<BridgeClient>,
    exe_path: PathBuf,
    managed_by: ManagedBy,
    synced: Mutex<HashMap<PathBuf, Synced>>,
}

impl SyncRuntime {
    /// A runtime for `ctx`'s application.
    pub fn new(
        ctx: &'static AppContext,
        bridge: Arc<BridgeClient>,
        exe_path: PathBuf,
        managed_by: ManagedBy,
    ) -> SyncRuntime {
        SyncRuntime {
            ctx,
            bridge,
            exe_path,
            managed_by,
            synced: Mutex::new(HashMap::new()),
        }
    }

    /// Start syncing `root`, returning its identity across devices.
    ///
    /// Idempotent: enabling an already-synced workspace returns the same id and changes
    /// nothing.
    pub async fn enable(&self, root: &Path) -> Result<GrainId> {
        let key = root.canonicalize().map_err(Error::Io)?;
        let workspace_id = sync_id(self.ctx.app_name, &key)?;

        {
            let mut synced = self.synced.lock().await;
            if let Some(existing) = synced.get(&key) {
                return Ok(existing.workspace_id);
            }
            let device_id = self.device_id().await?;
            let state_dir = self.state_dir(&key)?;
            std::fs::create_dir_all(&state_dir).map_err(Error::Io)?;
            let config =
                ReplicaConfig::new(self.ctx.app_name, key.clone(), device_id, &state_dir);
            let replica = Replica::open(config, Arc::new(SystemClock))
                .map_err(|e| Error::Sync(e.to_string()))?;
            synced.insert(
                key.clone(),
                Synced {
                    workspace_id,
                    replica: Arc::new(Mutex::new(replica)),
                    paused: None,
                    last_error: None,
                },
            );
        }

        self.reregister().await?;
        Ok(workspace_id)
    }

    /// Stop syncing `root`. Files and the sync id stay, so re-enabling rejoins the same
    /// workspace rather than creating a second one.
    pub async fn disable(&self, root: &Path) -> Result<()> {
        let Ok(key) = root.canonicalize() else { return Ok(()) };
        let removed = self.synced.lock().await.remove(&key);
        if let Some(removed) = removed {
            self.bridge
                .unregister(removed.workspace_id)
                .await
                .map_err(|e| Error::Bridge(e.to_string()))?;
            self.reregister().await?;
        }
        Ok(())
    }

    /// What `sync.status` answers with.
    pub async fn status(&self, root: &Path) -> SyncStatus {
        let bridge_available = self.bridge.status().await.is_ok();
        let peers = match self.bridge.peers().await {
            Ok(p) => p.peers.len().saturating_sub(1),
            Err(_) => 0,
        };
        let Ok(key) = root.canonicalize() else {
            return SyncStatus {
                enabled: false,
                workspace_id: None,
                peers,
                paused: None,
                last_error: None,
                bridge_available,
            };
        };
        let synced = self.synced.lock().await;
        match synced.get(&key) {
            Some(entry) => SyncStatus {
                enabled: true,
                workspace_id: Some(entry.workspace_id),
                peers,
                paused: entry.paused.map(|r| format!("{r:?}")),
                last_error: entry.last_error.clone(),
                bridge_available,
            },
            None => SyncStatus {
                enabled: false,
                workspace_id: None,
                peers,
                paused: None,
                last_error: None,
                bridge_available,
            },
        }
    }

    /// Bring the replica's view of the files up to date.
    ///
    /// Called straight after the app server's own writes, and by the watcher for everything
    /// else. A scan that finds nothing is cheap; a scan that is skipped loses an edit until
    /// the next one.
    pub async fn scan(&self, root: &Path) -> Result<()> {
        let Ok(key) = root.canonicalize() else { return Ok(()) };
        let replica = {
            let synced = self.synced.lock().await;
            match synced.get(&key) {
                Some(entry) => Arc::clone(&entry.replica),
                None => return Ok(()),
            }
        };
        let outcome = {
            let mut replica = replica.lock().await;
            replica.scan().map_err(|e| Error::Sync(e.to_string()))?
        };
        let mut synced = self.synced.lock().await;
        if let Some(entry) = synced.get_mut(&key) {
            entry.paused = match outcome {
                ScanOutcome::Paused(reason) => Some(reason),
                ScanOutcome::Scanned(_) => None,
            };
        }
        Ok(())
    }

    /// Open a session with every peer that will take one.
    pub async fn sync_now(&self, root: &Path) -> Result<()> {
        let Ok(key) = root.canonicalize() else { return Ok(()) };
        let (workspace_id, replica) = {
            let synced = self.synced.lock().await;
            match synced.get(&key) {
                Some(entry) => (entry.workspace_id, Arc::clone(&entry.replica)),
                None => return Ok(()),
            }
        };
        let peers = self
            .bridge
            .peers()
            .await
            .map_err(|e| Error::Bridge(e.to_string()))?;
        let me = self.device_id().await?;

        for peer in peers.peers.into_iter().filter(|p| p.device_id != me) {
            // A peer that does not host this workspace refuses, which is normal and cheap.
            let stream = match self.bridge.open_stream(workspace_id, peer.device_id).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::debug!(peer = %peer.name, "no session: {err}");
                    continue;
                }
            };
            let mut replica = replica.lock().await;
            if let Err(err) =
                sapphire_framework_session::run_session(stream, &mut replica, workspace_id).await
            {
                tracing::warn!(peer = %peer.name, "session failed: {err}");
            }
        }
        Ok(())
    }

    /// Answer the bridge's announcements until the connection closes.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut incoming = self.bridge.incoming();
        loop {
            let announcement = match incoming.recv().await {
                Ok(a) => a,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "fell behind on bridge announcements");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            };

            let replica = {
                let synced = self.synced.lock().await;
                synced
                    .values()
                    .find(|s| s.workspace_id == announcement.workspace_id)
                    .map(|s| Arc::clone(&s.replica))
            };
            let Some(replica) = replica else {
                // The bridge routed to us for a workspace we no longer hold. Not fatal:
                // ignore it and let the ticket expire.
                tracing::debug!(
                    workspace = %announcement.workspace_id,
                    "an announcement for a workspace this server does not hold"
                );
                continue;
            };

            let bridge = Arc::clone(&self.bridge);
            let workspace_id = announcement.workspace_id;
            tokio::spawn(async move {
                let stream = match bridge.accept_stream(announcement.ticket).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        tracing::warn!("could not claim an announced stream: {err}");
                        return;
                    }
                };
                let mut replica = replica.lock().await;
                if let Err(err) = sapphire_framework_session::run_session(
                    stream,
                    &mut replica,
                    workspace_id,
                )
                .await
                {
                    tracing::warn!("an inbound session failed: {err}");
                }
            });
        }
    }

    /// Every synced root, for the watcher.
    pub async fn roots(&self) -> Vec<PathBuf> {
        self.synced.lock().await.keys().cloned().collect()
    }

    /// Tell the bridge the complete current set. A registration is not a delta.
    async fn reregister(&self) -> Result<()> {
        let workspaces: Vec<WorkspaceRegistration> = {
            let synced = self.synced.lock().await;
            synced
                .iter()
                .map(|(root, entry)| WorkspaceRegistration {
                    workspace_id: entry.workspace_id,
                    root: root.clone(),
                })
                .collect()
        };
        self.bridge
            .register(RegisterParams {
                app_name: self.ctx.app_name.to_owned(),
                exe_path: self.exe_path.clone(),
                managed_by: self.managed_by,
                workspaces,
            })
            .await
            .map_err(|e| Error::Bridge(e.to_string()))?;
        Ok(())
    }

    async fn device_id(&self) -> Result<GrainId> {
        let result = self
            .bridge
            .register(RegisterParams {
                app_name: self.ctx.app_name.to_owned(),
                exe_path: self.exe_path.clone(),
                managed_by: self.managed_by,
                workspaces: Vec::new(),
            })
            .await
            .map_err(|e| Error::Bridge(e.to_string()))?;
        Ok(result.device_id)
    }

    /// `<workspace cache dir>/sync/`.
    fn state_dir(&self, root: &Path) -> Result<PathBuf> {
        let workspace = Workspace::from_root(self.ctx, root)?;
        Ok(workspace.cache_dir().join("sync"))
    }
}
```

> `device_id` registering an empty list before `enable` inserts the workspace would briefly
> clear this app's routes in the bridge. Fix it by caching: call the bridge once, on the first
> `enable`, and keep the `device_id` in a `OnceCell<GrainId>` on `SyncRuntime`. Write the
> `OnceCell` version, not the one above — the sketch is there so the ordering hazard is
> visible, and `a_registration_carries_the_complete_current_list` catches it if you forget.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features sync::`
Expected: PASS, 10 tests.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server Cargo.lock
git commit -m "feat(server): hold a replica per synced workspace and answer the bridge"
```

---

### Task 5: `sync.enable` / `sync.disable` / `sync.status`

**Files:**
- Create: `crates/sapphire-framework-server/src/sync/methods.rs`
- Modify: `crates/sapphire-framework-backend/src/protocol.rs`
- Modify: `crates/sapphire-framework-server/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `methods.rs`

**Interfaces:**
- Produces, in `sapphire_backend::protocol`:
  - `SYNC_ENABLE: &str = "sync.enable"`, `SYNC_DISABLE: &str = "sync.disable"`,
    `SYNC_STATUS: &str = "sync.status"`
  - `SyncEnableResult { workspace_id: GrainId }`
  - `SyncStatusResult` — the same shape as `SyncRuntime::SyncStatus`
- Produces, in `-server`: `sync_router(runtime: Arc<SyncRuntime>, router: Router) -> Router`

`-backend` gains `grain-id` as a dependency for `GrainId` in these types.

**The failure this must report rather than hide:** with the bridge down, `sync.status` answers
with `bridge_available: false` and the app server keeps working. Spec §10 makes that the point
of the layering; a status call that failed instead would look like an outage of the app.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::testing::StubBridge;
    use sapphire_backend::protocol as proto;
    use sapphire_ipc::{Client, ClientInfo, Connection, ManagedBy, ServerInfo, serve};
    use sapphire_workspace::{AppContext, AppKind};

    static CTX: AppContext = AppContext::new("sapphire-syncmethods");

    /// A server exposing only the sync namespace, plus a workspace root.
    async fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Client, StubBridge) {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: set before any other thread reads the environment in this test binary.
        unsafe {
            std::env::set_var("SAPPHIRE_SYNCMETHODS_CACHE_DIR", tmp.path().join("cache"));
            std::env::set_var("SAPPHIRE_SYNCMETHODS_DATA_DIR", tmp.path().join("data"));
            std::env::set_var("SAPPHIRE_SYNCMETHODS_CONFIG_DIR", tmp.path().join("config"));
        }
        CTX.init(AppKind::Server);

        let root = tmp.path().join("ws");
        std::fs::create_dir_all(root.join(".sapphire-syncmethods")).unwrap();
        let root = root.canonicalize().unwrap();

        let (stub, bridge) = StubBridge::start().await;
        let runtime = Arc::new(crate::sync::SyncRuntime::new(
            &CTX,
            bridge,
            "/bin/true".into(),
            ManagedBy::Service,
        ));
        let router = Arc::new(sync_router(runtime, sapphire_ipc::Router::new()));

        let (client_conn, server_conn) = Connection::pair();
        tokio::spawn(async move {
            let info = ServerInfo {
                version: "0.0.0".into(),
                pid: std::process::id(),
                managed_by: ManagedBy::Service,
            };
            let _ = serve(server_conn, router, "sapphire-syncmethods", info).await;
        });
        let info = ClientInfo {
            kind: "cli".into(),
            version: "0.0.0".into(),
            pid: std::process::id(),
        };
        let (client, _) = Client::handshake(client_conn, "sapphire-syncmethods", info)
            .await
            .unwrap();
        (tmp, root, client, stub)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enabling_returns_the_workspace_id() {
        let (_tmp, ws, client, _stub) = fixture().await;
        let result: proto::SyncEnableResult = client
            .call(proto::SYNC_ENABLE, proto::WsParams { ws })
            .await
            .unwrap();
        assert!(!result.workspace_id.to_string().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_before_enabling_reports_disabled() {
        let (_tmp, ws, client, _stub) = fixture().await;
        let status: proto::SyncStatusResult =
            client.call(proto::SYNC_STATUS, proto::WsParams { ws }).await.unwrap();
        assert!(!status.enabled);
        assert!(status.workspace_id.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_after_enabling_reports_the_same_id() {
        let (_tmp, ws, client, _stub) = fixture().await;
        let enabled: proto::SyncEnableResult = client
            .call(proto::SYNC_ENABLE, proto::WsParams { ws: ws.clone() })
            .await
            .unwrap();
        let status: proto::SyncStatusResult =
            client.call(proto::SYNC_STATUS, proto::WsParams { ws }).await.unwrap();

        assert!(status.enabled);
        assert_eq!(status.workspace_id, Some(enabled.workspace_id));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabling_an_unsynced_workspace_is_not_an_error() {
        let (_tmp, ws, client, _stub) = fixture().await;
        let _: proto::Ack = client
            .call(proto::SYNC_DISABLE, proto::WsParams { ws })
            .await
            .expect("disabling what was never enabled is fine");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_directory_that_is_not_a_workspace_is_an_invalid_parameter() {
        let (tmp, _ws, client, _stub) = fixture().await;
        let plain = tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();

        let err = client
            .call::<_, proto::SyncEnableResult>(
                proto::SYNC_ENABLE,
                proto::WsParams { ws: plain },
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
}
```

Add a test for the bridge-down case as an integration test in Task 7, where a real bridge can
be stopped; the stub is always up by construction, and faking its absence here would test the
stub rather than the runtime.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features sync::methods`
Expected: FAIL — `sync_router` does not exist.

- [ ] **Step 3: Implement the methods**

`crates/sapphire-framework-backend/src/protocol.rs`, appended:

```rust
/// Start syncing a workspace.
pub const SYNC_ENABLE: &str = "sync.enable";
/// Stop syncing a workspace. Files stay.
pub const SYNC_DISABLE: &str = "sync.disable";
/// Report a workspace's replication state.
pub const SYNC_STATUS: &str = "sync.status";

/// Result of [`SYNC_ENABLE`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncEnableResult {
    /// The workspace's identity across devices.
    pub workspace_id: grain_id::GrainId,
}

/// Result of [`SYNC_STATUS`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncStatusResult {
    /// Whether this workspace is synced.
    pub enabled: bool,
    /// Its identity across devices, when it is.
    pub workspace_id: Option<grain_id::GrainId>,
    /// How many other devices the workgroup has.
    pub peers: usize,
    /// Why replication is paused, if it is.
    pub paused: Option<String>,
    /// The last failure, if any.
    pub last_error: Option<String>,
    /// Whether the bridge is reachable. `false` does not mean the app server is down.
    pub bridge_available: bool,
}
```

`crates/sapphire-framework-server/src/sync/methods.rs`:

```rust
//! The `sync.*` methods.

use std::sync::Arc;

use sapphire_backend::protocol as proto;
use sapphire_ipc::{Router, RpcError};

use crate::handlers::rpc_error;
use crate::sync::SyncRuntime;

/// Add `sync.enable`, `sync.disable` and `sync.status` to `router`.
pub fn sync_router(runtime: Arc<SyncRuntime>, router: Router) -> Router {
    let enable = Arc::clone(&runtime);
    let disable = Arc::clone(&runtime);
    let status = runtime;

    router
        .method(proto::SYNC_ENABLE, move |ctx| {
            let runtime = Arc::clone(&enable);
            async move {
                let p: proto::WsParams = serde_json::from_value(ctx.params)
                    .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
                let workspace_id =
                    runtime.enable(&p.ws).await.map_err(|e| rpc_error(&e))?;
                serde_json::to_value(proto::SyncEnableResult { workspace_id })
                    .map_err(|e| RpcError::internal(e.to_string()))
            }
        })
        .method(proto::SYNC_DISABLE, move |ctx| {
            let runtime = Arc::clone(&disable);
            async move {
                let p: proto::WsParams = serde_json::from_value(ctx.params)
                    .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
                runtime.disable(&p.ws).await.map_err(|e| rpc_error(&e))?;
                serde_json::to_value(proto::Ack {})
                    .map_err(|e| RpcError::internal(e.to_string()))
            }
        })
        .method(proto::SYNC_STATUS, move |ctx| {
            let runtime = Arc::clone(&status);
            async move {
                let p: proto::WsParams = serde_json::from_value(ctx.params)
                    .map_err(|e| RpcError::invalid_params(format!("bad parameters: {e}")))?;
                // Never fails on account of the bridge: a status call that errored when the
                // bridge was down would read as an outage of the app server itself.
                let status = runtime.status(&p.ws).await;
                serde_json::to_value(proto::SyncStatusResult {
                    enabled: status.enabled,
                    workspace_id: status.workspace_id,
                    peers: status.peers,
                    paused: status.paused,
                    last_error: status.last_error,
                    bridge_available: status.bridge_available,
                })
                .map_err(|e| RpcError::internal(e.to_string()))
            }
        })
}
```

`rpc_error` must map `Error::SyncId` and `Error::UnknownWorkspace` to `INVALID_PARAMS`, and
`Error::Bridge` and `Error::Sync` to `INTERNAL_ERROR`. Extend the match from the app-server
plan's Task 3 accordingly.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features sync::methods`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server crates/sapphire-framework-backend Cargo.lock
git commit -m "feat(server): expose sync.enable, sync.disable and sync.status"
```

---

### Task 6: Noticing local edits

**Files:**
- Create: `crates/sapphire-framework-server/src/sync/watch.rs`
- Modify: `crates/sapphire-framework-server/src/{sync/mod.rs,handlers.rs}`, `Cargo.toml`
- Test: inline `#[cfg(test)] mod tests` in `watch.rs`

**Interfaces:**
- Produces:
  - `DEBOUNCE: Duration = Duration::from_millis(300)`
  - `Watcher::start(roots: Vec<PathBuf>, tx: mpsc::Sender<PathBuf>) -> Result<Watcher>`
  - `Watcher::watch(&self, root: &Path) -> Result<()>`, `Watcher::unwatch(&self, root: &Path)`
- Adds `notify = "8"` to the manifest.

**Two paths, and neither is optional:**

- **The app server's own writes** already go through `WorkspaceBackend::write_file`. Call
  `SyncRuntime::scan` directly after the write succeeds. This is the main path and it is exact.
- **Everything else** — a file edited in Vim, a `git checkout` — is the watcher's job. It
  coalesces events for `DEBOUNCE` and then reports the root.

The second is the safety net the process-architecture spec demoted it to, not the main route.
A scan that finds nothing is cheap; a scan that is skipped loses an edit until the next one.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn recv(rx: &mut tokio::sync::mpsc::Receiver<PathBuf>) -> Option<PathBuf> {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_created_outside_the_server_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("ws");
        std::fs::create_dir_all(&root).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let _watcher = Watcher::start(vec![root.clone()], tx).unwrap();

        std::fs::write(root.join("new.md"), "content").unwrap();
        assert_eq!(recv(&mut rx).await.as_deref(), Some(root.as_path()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_burst_of_writes_produces_one_report() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("ws");
        std::fs::create_dir_all(&root).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let _watcher = Watcher::start(vec![root.clone()], tx).unwrap();

        for n in 0..20 {
            std::fs::write(root.join(format!("f{n}.md")), "x").unwrap();
        }
        assert!(recv(&mut rx).await.is_some());

        // Nothing more within another debounce window plus slack.
        let extra = tokio::time::timeout(DEBOUNCE * 3, rx.recv()).await;
        assert!(extra.is_err(), "the burst should have coalesced, got {extra:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn events_inside_the_marker_directory_are_reported_too() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("ws");
        std::fs::create_dir_all(root.join(".test-app")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let _watcher = Watcher::start(vec![root.clone()], tx).unwrap();

        std::fs::write(root.join(".test-app").join("config.toml"), "x = 1").unwrap();
        assert!(
            recv(&mut rx).await.is_some(),
            "the marker directory holds sync-id and config, which are synced"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_watched_root_that_disappears_does_not_kill_the_watcher() {
        let tmp = tempfile::tempdir().unwrap();
        let going = tmp.path().join("going");
        let staying = tmp.path().join("staying");
        std::fs::create_dir_all(&going).unwrap();
        std::fs::create_dir_all(&staying).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let _watcher = Watcher::start(vec![going.clone(), staying.clone()], tx).unwrap();

        std::fs::remove_dir_all(&going).unwrap();
        tokio::time::sleep(DEBOUNCE * 2).await;
        while rx.try_recv().is_ok() {}

        std::fs::write(staying.join("still-here.md"), "x").unwrap();
        assert_eq!(
            recv(&mut rx).await.as_deref(),
            Some(staying.as_path()),
            "one lost root must not stop the others being watched"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_root_added_later_is_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first");
        let second = tmp.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let watcher = Watcher::start(vec![first], tx).unwrap();
        watcher.watch(&second).unwrap();

        std::fs::write(second.join("a.md"), "x").unwrap();
        assert_eq!(recv(&mut rx).await.as_deref(), Some(second.as_path()));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features sync::watch`
Expected: FAIL — `Watcher` does not exist.

- [ ] **Step 3: Implement the watcher**

```rust
//! Noticing edits the app server did not make.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use notify::{RecursiveMode, Watcher as _};
use tokio::sync::mpsc;

use crate::error::{Error, Result};

/// How long events are coalesced before a root is reported.
pub const DEBOUNCE: Duration = Duration::from_millis(300);

/// Watches workspace roots and reports which one changed.
pub struct Watcher {
    inner: Mutex<notify::RecommendedWatcher>,
    roots: Arc<Mutex<Vec<PathBuf>>>,
}

impl Watcher {
    /// Start watching `roots`, reporting on `tx`.
    pub fn start(roots: Vec<PathBuf>, tx: mpsc::Sender<PathBuf>) -> Result<Watcher> {
        let known = Arc::new(Mutex::new(roots.clone()));
        let pending: Arc<Mutex<HashMap<PathBuf, Instant>>> = Arc::new(Mutex::new(HashMap::new()));

        // The debounce timer: report a root once its last event is DEBOUNCE old.
        {
            let pending = Arc::clone(&pending);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(DEBOUNCE / 3);
                loop {
                    ticker.tick().await;
                    let ready: Vec<PathBuf> = {
                        let mut pending = pending.lock().expect("pending");
                        let ready: Vec<PathBuf> = pending
                            .iter()
                            .filter(|(_, last)| last.elapsed() >= DEBOUNCE)
                            .map(|(root, _)| root.clone())
                            .collect();
                        for root in &ready {
                            pending.remove(root);
                        }
                        ready
                    };
                    for root in ready {
                        if tx.send(root).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }

        let handler_roots = Arc::clone(&known);
        let handler_pending = Arc::clone(&pending);
        let mut watcher = notify::recommended_watcher(
            move |event: notify::Result<notify::Event>| {
                let Ok(event) = event else { return };
                let roots = handler_roots.lock().expect("roots").clone();
                for path in event.paths {
                    // Attribute the event to the root it happened under. Nothing is filtered
                    // by path here: what is synced is `SyncFilter`'s decision, made later on
                    // content — the marker directory holds sync-id and config, which sync.
                    if let Some(root) = roots.iter().find(|r| path.starts_with(r)) {
                        handler_pending
                            .lock()
                            .expect("pending")
                            .insert(root.clone(), Instant::now());
                    }
                }
            },
        )
        .map_err(|e| Error::Sync(e.to_string()))?;

        for root in &roots {
            // A root that cannot be watched is logged, not fatal: the others must keep
            // working, and the next `scan` will pick this one up anyway.
            if let Err(err) = watcher.watch(root, RecursiveMode::Recursive) {
                tracing::warn!(root = %root.display(), "could not watch: {err}");
            }
        }

        Ok(Watcher { inner: Mutex::new(watcher), roots: known })
    }

    /// Start watching one more root.
    pub fn watch(&self, root: &Path) -> Result<()> {
        self.roots.lock().expect("roots").push(root.to_owned());
        self.inner
            .lock()
            .expect("watcher")
            .watch(root, RecursiveMode::Recursive)
            .map_err(|e| Error::Sync(e.to_string()))
    }

    /// Stop watching one root.
    pub fn unwatch(&self, root: &Path) {
        self.roots.lock().expect("roots").retain(|r| r != root);
        let _ = self.inner.lock().expect("watcher").unwatch(root);
    }
}
```

Wire it into `AppServer::run`: create a `Watcher` over `SyncRuntime::roots()`, and on each
report call `runtime.scan(&root)` then `runtime.sync_now(&root)`. In `handlers.rs`, call
`runtime.scan` after `write_file`, `append_file` and `delete_file` succeed — the exact path,
which the watcher only backs up.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features sync::watch`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server Cargo.lock
git commit -m "feat(server): scan a workspace when something changes it from outside"
```

---

### Task 7: End to end — two hosts converge

**Files:**
- Create: `crates/sapphire-framework-server/tests/converge.rs`
- Modify: `crates/sapphire-framework-server/Cargo.toml` (dev-dependency on `-bridge` with
  `test-util`)
- Modify: `docs/ARCHITECTURE.md`

Two bridges on a loopback network, an app server against each, one workspace on both. The
first test in the series that exercises the finished shape: client → app server → bridge →
peer bridge → peer app server → files.

- [ ] **Step 1: Write the tests**

```rust
//! Two complete hosts, converging.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sapphire_backend::protocol as proto;
use sapphire_framework_bridge::LoopbackNetwork;

mod common;
use common::{Host, start_host};

/// Enable sync on both hosts for one workspace and let them meet.
async fn synced_pair(net: &LoopbackNetwork) -> (Host, Host) {
    let a = start_host(net, common::NODE_A, "host-a").await;
    let b = start_host(net, common::NODE_B, "host-b").await;
    common::introduce(&a, &b);
    let _: proto::SyncEnableResult = a
        .client
        .call(proto::SYNC_ENABLE, proto::WsParams { ws: a.ws.clone() })
        .await
        .unwrap();
    let _: proto::SyncEnableResult = b
        .client
        .call(proto::SYNC_ENABLE, proto::WsParams { ws: b.ws.clone() })
        .await
        .unwrap();
    (a, b)
}

/// Wait for `path` to exist on `host`, or fail. Never sleeps blindly: polls a condition.
async fn await_file(host: &Host, rel: &str) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(text) = std::fs::read_to_string(host.ws.join(rel)) {
            return text;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{rel} never arrived on {}",
            host.ws.display()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_made_through_the_ipc_client_is_synced() {
    let net = LoopbackNetwork::new();
    let (a, b) = synced_pair(&net).await;

    let _: proto::Ack = a
        .client
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: a.ws.clone(),
                path: PathBuf::from("note.md"),
                content: "from host a".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(await_file(&b, "note.md").await, "from host a");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edit_made_outside_the_server_is_synced() {
    let net = LoopbackNetwork::new();
    let (a, b) = synced_pair(&net).await;

    // As if the user had opened an editor.
    std::fs::write(a.ws.join("by-hand.md"), "typed directly").unwrap();

    assert_eq!(await_file(&b, "by-hand.md").await, "typed directly");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_search_index_on_the_receiving_host_finds_the_new_file() {
    let net = LoopbackNetwork::new();
    let (a, b) = synced_pair(&net).await;

    let _: proto::Ack = a
        .client
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: a.ws.clone(),
                path: PathBuf::from("searchable.md"),
                content: "the quick brown fox".into(),
            },
        )
        .await
        .unwrap();
    await_file(&b, "searchable.md").await;

    // The receiving app server applied the change, so the file and its index moved together.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let hits: proto::SearchResult = b
            .client
            .call(
                proto::SEARCH,
                proto::SearchParams {
                    ws: b.ws.clone(),
                    query: "brown".into(),
                    limit: 10,
                    mode: sapphire_backend::SearchMode::Fts,
                },
            )
            .await
            .unwrap();
        if hits.hits.iter().any(|h| h.path.ends_with("searchable.md")) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a synced file must become searchable without a manual reindex"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_that_was_offline_catches_up_when_it_returns() {
    let net = LoopbackNetwork::new();
    let (a, b) = synced_pair(&net).await;

    // B goes away.
    b.stop().await;

    let _: proto::Ack = a
        .client
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: a.ws.clone(),
                path: PathBuf::from("while-away.md"),
                content: "written while b was down".into(),
            },
        )
        .await
        .unwrap();

    // B comes back with the same directories.
    let b = b.restart(&net).await;
    let _: proto::SyncEnableResult = b
        .client
        .call(proto::SYNC_ENABLE, proto::WsParams { ws: b.ws.clone() })
        .await
        .unwrap();

    assert_eq!(await_file(&b, "while-away.md").await, "written while b was down");
}

#[tokio::test(flavor = "multi_thread")]
async fn with_the_bridge_down_the_app_server_still_serves_files() {
    let net = LoopbackNetwork::new();
    let (a, _b) = synced_pair(&net).await;

    a.stop_bridge().await;

    let _: proto::Ack = a
        .client
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: a.ws.clone(),
                path: PathBuf::from("offline.md"),
                content: "still works".into(),
            },
        )
        .await
        .expect("the app server must not depend on the bridge to serve files");

    let status: proto::SyncStatusResult = a
        .client
        .call(proto::SYNC_STATUS, proto::WsParams { ws: a.ws.clone() })
        .await
        .expect("status must answer even with the bridge down");
    assert!(!status.bridge_available);
}
```

Put `Host`, `start_host`, `introduce`, `NODE_A` and `NODE_B` in
`crates/sapphire-framework-server/tests/common/mod.rs`, with `Host` carrying the workspace
root, the IPC client, handles for the bridge and the server, and `stop`, `restart` and
`stop_bridge`.

`the_search_index_on_the_receiving_host_finds_the_new_file` is the claim the whole architecture
rests on and the one nothing has tested until now: because the **app server** applied the
change, the file, the cache and the replica state moved together. Under the model this design
replaced, the file would have landed behind the index's back and stayed invisible to search
until the next reindex.

- [ ] **Step 2: Run the tests**

Run: `cargo test -p sapphire-framework-server --all-features --test converge`
Expected: PASS, 5 tests.

If one is flaky, find the race — do not add a sleep. The usual cause is asserting on a file
before the session carrying it has committed; wait on a condition, as `await_file` does.

- [ ] **Step 3: Note it in `ARCHITECTURE.md` and commit**

```markdown
| `sapphire-framework-session` | 2 つのレプリカ間のセッション（フレーミング・vv 交換・差分と内容の転送） | ✅ |
```

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-server docs/ARCHITECTURE.md Cargo.lock
git commit -m "test(server): two hosts converge end to end"
```

## What this plan does not cover

| | Left for |
|---|---|
| Live propagation — keeping a session's stream open and pushing changes as they happen (sync spec §4.2) | step 9. Until then a change reaches a peer at the next dial, which the watcher triggers. |
| Discovering which workspaces exist in the workgroup (`workspaces/<id>.toml`) and `sync map` | step 8 |
| The bridge replicating its own workgroup workspace with this session code | step 8 |
| App-specific fix-ups after a sync (journal's duplicate ids) | each application's own repository |
