# Server Features: live propagation, relays, visibility — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Make sync feel immediate rather than periodic, let a self-hosted server carry
devices that cannot reach each other directly, and make it possible to see what the bridge is
doing without attaching a debugger.

**Architecture:** A session no longer ends at `Done`: the stream stays open, and each side
pushes what it commits to every peer already on the line. A host that both sides can reach
forwards what it received to the peers whose version vectors do not cover it, so A → S → B
propagates live even when A and B cannot see each other. Around that, relay configuration
comes from two places — the host's own `net.toml` and the workgroup's published one — and the
bridge writes `status.json` and a log file that any process can read.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), `sapphire-framework-session`,
`sapphire-framework-sync`, iroh 1.2 (with `iroh-relay` for the embedded relay), tokio 1,
serde + serde_json + toml, tracing + tracing-subscriber, tracing-appender 0.2.

**Spec:** `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` §4.2 (dialing and live
propagation) and §3.8 (endpoint configuration), read through the substitution table at the
head of its §3; `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §5.
Implementation order step 9 of the process-architecture spec's §9.

**Depends on:** steps 6, 7 and 8 (`2026-09-16-bridge-basics-plan.md`,
`2026-09-16-sync-runtime-plan.md`, `2026-09-16-pairing-plan.md`).

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task.
- Every public item carries a doc comment.
- Dial backoff is exponential, capped at **5 minutes**.
- `status.json` is rewritten **every 5 seconds and on change**, atomically.
- The log rotates at **10 MiB**, keeping **3** files.
- The embedded relay is behind the `embedded-relay` feature, off by default. A relay needs a
  publicly reachable address and a certificate; most hosts have neither.
- **Live propagation must not turn into a broadcast storm.** An entry is forwarded to a peer
  only when that peer's known version vector does not cover it, and never back to the peer it
  came from. Task 2's tests pin both.

## File Structure

```
crates/sapphire-framework-session/src/
    live.rs        # LiveSession: the phase after Done

crates/sapphire-framework-server/src/sync/
    live.rs        # keeping sessions open, pushing commits, forwarding

crates/sapphire-framework-bridge/src/
    relay.rs       # relay configuration from two net.toml files; the embedded relay
    status.rs      # status.json
    logging.rs     # the bridge's log file and `bridge log`
    command.rs     # MODIFIED: bridge log
```

---

### Task 1: A session that stays open

**Files:**
- Create: `crates/sapphire-framework-session/src/live.rs`
- Modify: `crates/sapphire-framework-session/src/{lib.rs,session.rs,message.rs}`
- Test: `crates/sapphire-framework-session/tests/live.rs`

**Interfaces:**
- Consumes: `run_session`'s pieces (step 7, Task 2)
- Produces:
  - `Message::Live(Vec<PathUpdate>)` — a push after the initial exchange
  - `LiveSession`: `push(&self, updates: Vec<PathUpdate>) -> Result<()>`,
    `updates(&self) -> broadcast::Receiver<Vec<PathUpdate>>`,
    `peer_vv(&self) -> VersionVector`, `is_open(&self) -> bool`, `close(self)`
  - `async open_live_session<S>(stream: S, replica: Arc<Mutex<Replica>>, workspace_id: GrainId) -> Result<(SessionOutcome, LiveSession)>`

**What changes from step 7:** `run_session` still exists, unchanged, for a one-shot catch-up
and for the tests that use it. `open_live_session` does the same initial exchange and then,
instead of returning, keeps the reader running: `Live` messages are applied as they arrive, and
`push` sends. It reports the initial outcome as soon as both sides have said `Done`, so a
caller can tell "we are caught up" from "we are still connected".

`peer_vv` is the key to not shouting: it advances as the peer acknowledges, and the caller
consults it before forwarding anything.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-session/tests/live.rs`:

```rust
//! A session that keeps going after the initial exchange.

use std::sync::Arc;

use sapphire_framework_session::open_live_session;
use sapphire_sync::{Replica, ReplicaConfig, SystemClock};
use tokio::sync::Mutex;

struct Side {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    replica: Arc<Mutex<Replica>>,
}

fn side(name: &str) -> Side {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join(name);
    std::fs::create_dir_all(root.join(".test-app")).unwrap();
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let config =
        ReplicaConfig::new("test-app", root.clone(), grain_id::GrainId::random(), &state);
    let replica = Replica::open(config, Arc::new(SystemClock)).unwrap();
    Side { _tmp: tmp, root, replica: Arc::new(Mutex::new(replica)) }
}

async fn scan(side: &Side) -> Vec<sapphire_sync::PathUpdate> {
    let mut replica = side.replica.lock().await;
    let outcome = replica.scan().unwrap();
    match outcome {
        sapphire_sync::ScanOutcome::Scanned(report) => report
            .recorded
            .into_iter()
            .map(|entry| sapphire_sync::PathUpdate {
                path: entry.path.clone(),
                versions: vec![entry],
                seen: replica.vv().clone(),
            })
            .collect(),
        sapphire_sync::ScanOutcome::Paused(_) => vec![],
    }
}

/// Wait until `rel` exists on `side`, or fail.
async fn await_file(side: &Side, rel: &str) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Ok(text) = std::fs::read_to_string(side.root.join(rel)) {
            return text;
        }
        assert!(std::time::Instant::now() < deadline, "{rel} never arrived");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_initial_exchange_still_happens() {
    let ws = grain_id::GrainId::random();
    let a = side("a");
    let b = side("b");
    std::fs::write(a.root.join("first.md"), "before the session").unwrap();
    scan(&a).await;

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        open_live_session(left, Arc::clone(&a.replica), ws),
        open_live_session(right, Arc::clone(&b.replica), ws)
    );
    let (outcome_a, _live_a) = x.unwrap();
    let (_outcome_b, _live_b) = y.unwrap();

    assert_eq!(outcome_a.sent, 1);
    assert_eq!(await_file(&b, "first.md").await, "before the session");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_push_after_the_exchange_arrives_without_a_new_session() {
    let ws = grain_id::GrainId::random();
    let a = side("a");
    let b = side("b");

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        open_live_session(left, Arc::clone(&a.replica), ws),
        open_live_session(right, Arc::clone(&b.replica), ws)
    );
    let (_, live_a) = x.unwrap();
    let (_, _live_b) = y.unwrap();

    std::fs::write(a.root.join("later.md"), "after the session started").unwrap();
    let updates = scan(&a).await;
    live_a.push(updates).await.unwrap();

    assert_eq!(await_file(&b, "later.md").await, "after the session started");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_sees_what_arrived() {
    let ws = grain_id::GrainId::random();
    let a = side("a");
    let b = side("b");

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        open_live_session(left, Arc::clone(&a.replica), ws),
        open_live_session(right, Arc::clone(&b.replica), ws)
    );
    let (_, live_a) = x.unwrap();
    let (_, live_b) = y.unwrap();

    let mut received = live_b.updates();
    std::fs::write(a.root.join("watched.md"), "x").unwrap();
    live_a.push(scan(&a).await).await.unwrap();

    let updates = tokio::time::timeout(std::time::Duration::from_secs(10), received.recv())
        .await
        .expect("an update within ten seconds")
        .unwrap();
    assert!(updates.iter().any(|u| u.path.ends_with("watched.md")), "{updates:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_peers_version_vector_advances_as_pushes_are_taken() {
    let ws = grain_id::GrainId::random();
    let a = side("a");
    let b = side("b");

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        open_live_session(left, Arc::clone(&a.replica), ws),
        open_live_session(right, Arc::clone(&b.replica), ws)
    );
    let (_, live_a) = x.unwrap();
    let (_, _live_b) = y.unwrap();

    let before = live_a.peer_vv();
    std::fs::write(b.root.join("from-b.md").clone(), "x").unwrap();
    let updates = scan(&b).await;
    let (_, live_b) = (0, {
        // Re-take B's live handle from the join above.
        y_handle_placeholder()
    });
    live_b.push(updates).await.unwrap();
    await_file(&a, "from-b.md").await;

    assert_ne!(
        live_a.peer_vv(),
        before,
        "the peer's version vector must advance, or every entry is forwarded for ever"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_one_side_is_visible_on_the_other() {
    let ws = grain_id::GrainId::random();
    let a = side("a");
    let b = side("b");

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        open_live_session(left, Arc::clone(&a.replica), ws),
        open_live_session(right, Arc::clone(&b.replica), ws)
    );
    let (_, live_a) = x.unwrap();
    let (_, live_b) = y.unwrap();

    live_a.close();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while live_b.is_open() {
        assert!(std::time::Instant::now() < deadline, "the close was never noticed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pushing_on_a_closed_session_fails_rather_than_hanging() {
    let ws = grain_id::GrainId::random();
    let a = side("a");
    let b = side("b");

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        open_live_session(left, Arc::clone(&a.replica), ws),
        open_live_session(right, Arc::clone(&b.replica), ws)
    );
    let (_, live_a) = x.unwrap();
    let (_, live_b) = y.unwrap();
    live_b.close();

    // Give the close a moment to reach A, then push.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    std::fs::write(a.root.join("into-the-void.md"), "x").unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        live_a.push(scan(&a).await),
    )
    .await;
    assert!(result.is_ok(), "push must not hang on a closed session");
}
```

> `the_peers_version_vector_advances_as_pushes_are_taken` as written cannot get at B's handle,
> because `y` was destructured above. Restructure it: bind `let (_, live_b) = y.unwrap();`
> alongside `live_a` at the top, and delete the `y_handle_placeholder()` line. Write it that
> way — the placeholder is there only so the mistake is not copied silently.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-session --all-features --test live`
Expected: FAIL — `open_live_session` does not exist.

- [ ] **Step 3: Implement the live phase**

Add `Live(Vec<PathUpdate>)` to `Message`. Refactor `session.rs` so the initial exchange is a
private function returning the reader, the writer channel, the peer's version vector and the
outcome; `run_session` closes over it as it does today, and `open_live_session` keeps it.

```rust
/// A session that stays open after the initial exchange.
///
/// Dropping it closes the session; `close` is the same thing said out loud.
pub struct LiveSession {
    out: mpsc::Sender<Out>,
    updates: broadcast::Sender<Vec<PathUpdate>>,
    peer_vv: Arc<Mutex<VersionVector>>,
    open: Arc<AtomicBool>,
    reader: JoinHandle<()>,
}

impl LiveSession {
    /// Send updates to the peer.
    ///
    /// Fails rather than waiting once the session has closed, so a caller looping over
    /// peers does not stall on one that went away.
    pub async fn push(&self, updates: Vec<PathUpdate>) -> Result<()> {
        if !self.is_open() {
            return Err(Error::Protocol("the session is closed".to_owned()));
        }
        self.out
            .send(Out::Control(Message::Live(updates)))
            .await
            .map_err(|_| Error::Protocol("the session is closed".to_owned()))
    }

    /// Updates that arrived from the peer, after they were applied.
    pub fn updates(&self) -> broadcast::Receiver<Vec<PathUpdate>> {
        self.updates.subscribe()
    }

    /// Everything the peer is known to have.
    ///
    /// Consulted before forwarding: an entry the peer already covers must not be sent again,
    /// or three connected hosts will pass one edit round in a circle.
    pub fn peer_vv(&self) -> VersionVector {
        self.peer_vv.lock().expect("peer vv").clone()
    }

    /// Is the session still usable?
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }

    /// Close it.
    pub fn close(self) {
        drop(self);
    }
}

impl Drop for LiveSession {
    fn drop(&mut self) {
        self.open.store(false, Ordering::Relaxed);
        self.reader.abort();
    }
}
```

The reader task applies each `Live` batch to the replica, merges the batch's `seen` into
`peer_vv`, publishes the applied updates on the broadcast, and clears `open` when the stream
ends.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-session --all-features`
Expected: PASS — the six live tests plus step 7's eight.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-session
git commit -m "feat(session): keep a session open and push what is committed"
```

---

### Task 2: Live propagation in the app server

**Files:**
- Create: `crates/sapphire-framework-server/src/sync/live.rs`
- Modify: `crates/sapphire-framework-server/src/sync/mod.rs`
- Test: `crates/sapphire-framework-server/tests/live.rs`

**Interfaces:**
- Produces:
  - `LivePeers` — the open sessions for one workspace, keyed by device id
  - `SyncRuntime::after_commit(&self, root: &Path, updates: Vec<PathUpdate>)` — push to every
    open session
  - `SyncRuntime::dial_loop(self: Arc<Self>)` — keeps a session open to every peer, with
    exponential backoff capped at 5 minutes
  - `DIAL_BACKOFF_MAX: Duration = 5 min`

**The two rules that keep this from becoming a storm, both tested:**

1. **Never send back to where it came from.** The session an update arrived on is excluded
   when forwarding it.
2. **Never send what the peer already has.** `LiveSession::peer_vv` is consulted first.

Together they make A → S → B work — S forwards A's entries to B — while three connected hosts
do not pass one edit round in a circle for ever.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/tests/live.rs`:

```rust
//! Live propagation between app servers, including through a middle host.

mod common;

use std::path::PathBuf;

use sapphire_backend::protocol as proto;
use sapphire_framework_bridge::LoopbackNetwork;

/// Wait for `rel` to appear on `host`, with a deadline rather than a sleep.
async fn await_file(host: &common::Host, rel: &str) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(text) = std::fs::read_to_string(host.ws.join(rel)) {
            return text;
        }
        assert!(std::time::Instant::now() < deadline, "{rel} never arrived");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn write(host: &common::Host, rel: &str, content: &str) {
    let _: proto::Ack = host
        .client
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: host.ws.clone(),
                path: PathBuf::from(rel),
                content: content.into(),
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_change_arrives_without_waiting_for_the_next_dial() {
    let net = LoopbackNetwork::new();
    let (a, b) = common::synced_pair(&net).await;
    common::settle(&[&a, &b]).await;

    let started = std::time::Instant::now();
    write(&a, "quick.md", "now").await;
    assert_eq!(await_file(&b, "quick.md").await, "now");

    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "a live session should deliver in well under the dial interval, took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_change_propagates_through_a_host_in_the_middle() {
    // A and S are connected; S and B are connected; A and B are not. This is the topology a
    // self-hosted server exists for.
    let net = LoopbackNetwork::partitioned(&[
        (common::NODE_A, common::NODE_S),
        (common::NODE_S, common::NODE_B),
    ]);
    let (a, s, b) = common::synced_triple(&net).await;
    common::settle(&[&a, &s, &b]).await;

    write(&a, "relayed.md", "from a").await;
    assert_eq!(await_file(&b, "relayed.md").await, "from a");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_entry_is_not_forwarded_back_to_the_peer_it_came_from() {
    let net = LoopbackNetwork::new();
    let (a, b) = common::synced_pair(&net).await;
    common::settle(&[&a, &b]).await;

    let before = common::frames_sent(&b);
    write(&a, "one-way.md", "x").await;
    await_file(&b, "one-way.md").await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let after = common::frames_sent(&b);
    assert!(
        after - before < 5,
        "B sent {} frames for one incoming file; it is echoing",
        after - before
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn three_connected_hosts_do_not_loop_an_edit_between_them() {
    let net = LoopbackNetwork::new(); // fully connected
    let (a, s, b) = common::synced_triple(&net).await;
    common::settle(&[&a, &s, &b]).await;

    write(&a, "triangle.md", "x").await;
    await_file(&b, "triangle.md").await;
    await_file(&s, "triangle.md").await;

    // Let any loop run for a while, then check nobody is still talking.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let quiet_a = common::frames_sent(&a);
    let quiet_b = common::frames_sent(&b);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    assert_eq!(common::frames_sent(&a), quiet_a, "A is still sending");
    assert_eq!(common::frames_sent(&b), quiet_b, "B is still sending");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_goes_away_is_redialled() {
    let net = LoopbackNetwork::new();
    let (a, b) = common::synced_pair(&net).await;
    common::settle(&[&a, &b]).await;

    b.drop_connections().await;
    write(&a, "after-drop.md", "x").await;

    assert_eq!(
        await_file(&b, "after-drop.md").await,
        "x",
        "a dropped connection must be redialled, not mourned"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_peer_does_not_stall_the_others() {
    let net = LoopbackNetwork::partitioned(&[(common::NODE_A, common::NODE_B)]);
    let (a, s, b) = common::synced_triple(&net).await;
    // S is listed in the workgroup but reachable by nobody.
    common::settle(&[&a, &b]).await;

    write(&a, "despite-s.md", "x").await;
    assert_eq!(await_file(&b, "despite-s.md").await, "x");
    let _ = s;
}
```

`LoopbackNetwork::partitioned`, `Host::drop_connections`, `common::frames_sent`,
`common::settle` and `synced_triple` go in the shared fixtures. `frames_sent` needs the
loopback transport to count frames — add a counter to `LoopbackTransport` behind `test-util`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features --test live`
Expected: FAIL — live propagation does not exist.

- [ ] **Step 3: Implement it**

```rust
/// The open sessions for one workspace.
#[derive(Default)]
pub(crate) struct LivePeers {
    sessions: Mutex<HashMap<GrainId, LiveSession>>,
}

impl LivePeers {
    /// Push `updates` to every peer that does not already have them, except `from`.
    ///
    /// The two exclusions are what keep this from becoming a broadcast storm: an entry goes
    /// nowhere it came from, and nowhere it already is.
    pub(crate) async fn fan_out(&self, updates: &[PathUpdate], from: Option<GrainId>) {
        let targets: Vec<(GrainId, Vec<PathUpdate>)> = {
            let sessions = self.sessions.lock().expect("live peers");
            sessions
                .iter()
                .filter(|(device, _)| Some(**device) != from)
                .filter_map(|(device, session)| {
                    if !session.is_open() {
                        return None;
                    }
                    let peer_vv = session.peer_vv();
                    let wanted: Vec<PathUpdate> = updates
                        .iter()
                        .filter(|u| !peer_vv.covers(&u.seen))
                        .cloned()
                        .collect();
                    (!wanted.is_empty()).then_some((*device, wanted))
                })
                .collect()
        };
        for (device, wanted) in targets {
            let session = {
                let sessions = self.sessions.lock().expect("live peers");
                sessions.get(&device).map(|_| device)
            };
            let Some(device) = session else { continue };
            let sessions = self.sessions.lock().expect("live peers");
            if let Some(session) = sessions.get(&device)
                && let Err(err) = futures_lite_block(session.push(wanted))
            {
                tracing::debug!(%device, "dropping a closed session: {err}");
            }
        }
        self.reap();
    }

    /// Forget sessions that have closed.
    fn reap(&self) {
        self.sessions.lock().expect("live peers").retain(|_, s| s.is_open());
    }
}
```

> The sketch above holds a `std::sync::Mutex` across an `await` (`session.push`), which does
> not compile and would be a deadlock if it did. Use `tokio::sync::Mutex` for `sessions`, or —
> better — collect `(device, updates)` under the lock, drop it, and then await the pushes.
> Write the second form; the `futures_lite_block` name is deliberately not a real function so
> this cannot be copied by accident.

`dial_loop` walks `bridge.peers()` every interval and opens a live session to each peer that
has none, with per-peer exponential backoff capped at `DIAL_BACKOFF_MAX`. `after_commit` calls
`fan_out` with `from: None`; the reader side of each session calls it with `from: Some(peer)`
after applying, which is what makes A → S → B work.

- [ ] **Step 4: Run the tests to verify they pass, then commit**

```bash
cargo test -p sapphire-framework-server --all-features --test live
git commit -m "feat(server): propagate commits live, and through a host in the middle"
```

---

### Task 3: Relay configuration

**Files:**
- Create: `crates/sapphire-framework-bridge/src/relay.rs`
- Modify: `crates/sapphire-framework-bridge/src/{net.rs,iroh.rs,workgroup.rs}`
- Test: inline `#[cfg(test)] mod tests` in `relay.rs`

**Interfaces:**
- Produces:
  - `RelayConfig { urls: Vec<String>, use_default: bool }`
  - `fn relays(host: &NetConfig, workgroup: Option<&Workgroup>) -> Result<RelayConfig>` —
    merges the host's `net.toml` with the workgroup's published one
  - `Workgroup::published_net(&self) -> Result<NetConfig>` — reads `root/net.toml`
  - `Workgroup::publish_net(&self, net: &NetConfig) -> Result<()>`

**Two files, and which wins:** the host's `net.toml` is local preference; the workgroup's is
what a self-hosted server announces to its devices. They are **merged, not overridden** — a
device keeps its own relays and gains the workgroup's — because a device that lost its own
relay when it joined a workgroup would be worse off than before. `use_default` is `false` only
if **both** turn the public relays off, so one side cannot silently strand the other.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::NetConfig;

    fn host_with(urls: &[&str], use_default: bool) -> NetConfig {
        NetConfig {
            relays: urls.iter().map(|u| (*u).to_owned()).collect(),
            use_default_relays: use_default,
            ..NetConfig::default()
        }
    }

    #[test]
    fn with_no_configuration_the_public_relays_are_used() {
        let config = relays(&NetConfig::default(), None).unwrap();
        assert!(config.use_default);
        assert!(config.urls.is_empty());
    }

    #[test]
    fn a_hosts_own_relay_is_used() {
        let config = relays(&host_with(&["https://relay.example"], true), None).unwrap();
        assert_eq!(config.urls, vec!["https://relay.example".to_owned()]);
        assert!(config.use_default);
    }

    #[test]
    fn the_two_sources_are_merged_not_overridden() {
        let (_tmp, _dir, wg) = workgroup_with_net(&host_with(&["https://group.example"], true));
        let config = relays(&host_with(&["https://mine.example"], true), Some(&wg)).unwrap();

        let mut urls = config.urls.clone();
        urls.sort();
        assert_eq!(
            urls,
            vec!["https://group.example".to_owned(), "https://mine.example".to_owned()],
            "a device must not lose its own relay by joining a workgroup"
        );
    }

    #[test]
    fn a_duplicate_relay_appears_once() {
        let (_tmp, _dir, wg) = workgroup_with_net(&host_with(&["https://same.example"], true));
        let config = relays(&host_with(&["https://same.example"], true), Some(&wg)).unwrap();
        assert_eq!(config.urls.len(), 1);
    }

    #[test]
    fn the_public_relays_go_off_only_when_both_sides_say_so() {
        let (_tmp, _dir, wg) = workgroup_with_net(&host_with(&[], false));

        // Host still wants them.
        assert!(relays(&host_with(&[], true), Some(&wg)).unwrap().use_default);
        // Both say no.
        assert!(!relays(&host_with(&[], false), Some(&wg)).unwrap().use_default);
    }

    #[test]
    fn a_workgroup_without_a_published_net_file_contributes_nothing() {
        let (_tmp, dir) = crate::dir::tests_bridge_dir();
        let wg = crate::workgroup::Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let config = relays(&host_with(&["https://mine.example"], true), Some(&wg)).unwrap();
        assert_eq!(config.urls, vec!["https://mine.example".to_owned()]);
    }

    #[test]
    fn a_relay_url_that_is_not_a_url_is_refused() {
        let err = relays(&host_with(&["not a url"], true), None).unwrap_err();
        assert!(err.to_string().contains("not a url"), "{err}");
    }
}
```

Write `workgroup_with_net` as a helper that creates a workgroup and calls `publish_net`.

- [ ] **Step 2–5: Implement, verify, commit**

`NetConfig` gains `use_default_relays: bool` (default `true`). `iroh.rs` feeds `RelayConfig`
into the endpoint builder. `relays` validates each URL with `url::Url::parse` — add `url = "2"`
— so a typo is caught at startup rather than as a silent lack of connectivity.

```bash
cargo test -p sapphire-framework-bridge --all-features relay
git commit -m "feat(bridge): merge relay configuration from the host and the workgroup"
```

---

### Task 4: The embedded relay

**Files:**
- Modify: `crates/sapphire-framework-bridge/src/relay.rs`, `Cargo.toml`
- Modify: `apps/sapphire-bridge/Cargo.toml`
- Test: `crates/sapphire-framework-bridge/tests/embedded_relay.rs`

**Interfaces (feature `embedded-relay`):**
- `EmbeddedRelay::start(config: &EmbeddedRelayConfig) -> Result<EmbeddedRelay>`
- `EmbeddedRelayConfig { bind: SocketAddr, hostname: String, tls: TlsConfig }`
- `EmbeddedRelay::url(&self) -> String`, `EmbeddedRelay::stop(self)`
- `NetConfig` gains `embedded_relay: Option<EmbeddedRelayConfig>`

**Off by default, and the error says why.** A relay needs a publicly reachable address and a
certificate. Turning it on without either produces a relay nobody can use, so the start-up
check is explicit: the address must not be loopback unless `allow_loopback` is set (which the
tests do), and a hostname is required.

**Read the API before writing this**: the embedded relay is iroh's, and its surface is not
pinned by anything in this repository. Check `https://docs.rs/iroh-relay` for the current
server type and its configuration, and shape the code to that rather than to the sketch here.

- [ ] **Step 1: Write the tests**

```rust
#![cfg(feature = "embedded-relay")]

use sapphire_framework_bridge::{EmbeddedRelay, EmbeddedRelayConfig};

fn loopback(port: u16) -> EmbeddedRelayConfig {
    EmbeddedRelayConfig {
        bind: format!("127.0.0.1:{port}").parse().unwrap(),
        hostname: "localhost".into(),
        allow_loopback: true,
        ..EmbeddedRelayConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_starts_and_reports_its_url() {
    let relay = EmbeddedRelay::start(&loopback(0)).await.unwrap();
    assert!(relay.url().starts_with("http"), "{}", relay.url());
    relay.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_loopback_address_is_refused_unless_it_is_asked_for() {
    let mut config = loopback(0);
    config.allow_loopback = false;
    let err = EmbeddedRelay::start(&config).await.unwrap_err();
    assert!(
        err.to_string().contains("reachable"),
        "the message must say what is wrong: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_hostname_is_refused() {
    let mut config = loopback(0);
    config.hostname = String::new();
    assert!(EmbeddedRelay::start(&config).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn two_endpoints_meet_through_the_embedded_relay() {
    // Direct connections off, so the relay is the only path. This is the test that says the
    // feature works rather than merely starts.
    let relay = EmbeddedRelay::start(&loopback(0)).await.unwrap();
    // … build two IrohTransports configured with only this relay and with direct
    // addresses suppressed, then open a stream between them …
    relay.stop().await;
}
```

Write the last one against whatever iroh 1.2 offers for suppressing direct paths; if it offers
nothing, mark it `#[ignore]` with a doc comment saying so rather than deleting it — an
untested relay is worth knowing about.

- [ ] **Step 2–4: Implement, verify, commit**

```bash
cargo test -p sapphire-framework-bridge --features embedded-relay --test embedded_relay
git commit -m "feat(bridge): optionally run a relay for a workgroup"
```

---

### Task 5: `status.json`

**Files:**
- Create: `crates/sapphire-framework-bridge/src/status.rs`
- Modify: `crates/sapphire-framework-bridge/src/{lib.rs,command.rs}`
- Test: inline `#[cfg(test)] mod tests` in `status.rs`

**Interfaces:**
- Produces:
  - `StatusFile { version, pid, started_at, node_id, workgroup: Option<WorkgroupStatus>, peers: Vec<PeerStatus>, routes: Vec<RouteStatus>, relays: Vec<String> }`
  - `PeerStatus { device_id, name, node_id, connected, last_seen: Option<DateTime<Utc>>, last_error: Option<String> }`
  - `StatusWriter::start(path: PathBuf, source: Arc<dyn StatusSource>) -> StatusWriter`
  - `STATUS_INTERVAL: Duration = 5s`

**Written atomically**, through a temporary file and a rename. A reader that caught a
half-written `status.json` would report nonsense at exactly the moment someone is trying to
find out what is wrong.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(StatusFile);

    impl StatusSource for Fixed {
        fn snapshot(&self) -> StatusFile {
            self.0.clone()
        }
    }

    fn sample() -> StatusFile {
        StatusFile {
            version: "0.0.0".into(),
            pid: std::process::id(),
            started_at: chrono::Utc::now(),
            node_id: "abc".into(),
            workgroup: None,
            peers: vec![],
            routes: vec![],
            relays: vec![],
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_file_appears_promptly_and_parses() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("status.json");
        let writer = StatusWriter::start(path.clone(), Arc::new(Fixed(sample())));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !path.exists() {
            assert!(std::time::Instant::now() < deadline, "no status.json was written");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let parsed: StatusFile = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed.node_id, "abc");
        writer.stop();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reader_never_sees_a_half_written_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("status.json");
        let mut big = sample();
        big.peers = (0..2000)
            .map(|n| PeerStatus {
                device_id: grain_id::GrainId::random(),
                name: format!("device-{n}"),
                node_id: "x".repeat(64),
                connected: true,
                last_seen: Some(chrono::Utc::now()),
                last_error: None,
            })
            .collect();
        let writer = StatusWriter::start(path.clone(), Arc::new(Fixed(big)));

        // Read repeatedly while it is being rewritten; every read must parse.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut reads = 0;
        while std::time::Instant::now() < deadline {
            if let Ok(text) = std::fs::read_to_string(&path) {
                serde_json::from_str::<StatusFile>(&text)
                    .expect("a partially written status.json reached a reader");
                reads += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(reads > 10, "the test did not actually read anything");
        writer.stop();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stopping_the_writer_leaves_the_last_snapshot_in_place() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("status.json");
        let writer = StatusWriter::start(path.clone(), Arc::new(Fixed(sample())));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        writer.stop();
        assert!(path.exists(), "the last status is useful after a clean stop");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_temporary_files_are_left_behind() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("status.json");
        let writer = StatusWriter::start(path.clone(), Arc::new(Fixed(sample())));
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        writer.stop();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let leftovers: Vec<String> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n != "status.json")
            .collect();
        assert!(leftovers.is_empty(), "left behind {leftovers:?}");
    }
}
```

- [ ] **Step 2–5: Implement, verify, commit**

`bridge status` prefers a live control-plane call and falls back to reading `status.json` when
no bridge answers — which is how you find out what the bridge was doing before it stopped.

```bash
cargo test -p sapphire-framework-bridge --all-features status
git commit -m "feat(bridge): publish status.json atomically"
```

---

### Task 6: The log, and `bridge log`

**Files:**
- Create: `crates/sapphire-framework-bridge/src/logging.rs`
- Modify: `crates/sapphire-framework-bridge/src/command.rs`, `apps/sapphire-bridge/src/main.rs`
- Test: inline `#[cfg(test)] mod tests` in `logging.rs`

**Interfaces:**
- Produces:
  - `LOG_FILE: &str = "node.log"`, `LOG_MAX_BYTES: u64 = 10 * 1024 * 1024`, `LOG_KEEP: usize = 3`
  - `fn install(dir: &BridgeDir) -> Result<LogGuard>` — a `tracing` layer writing the bridge's
    own targets to `<bridge dir>/logs/node.log`, in addition to whatever the process already
    prints
  - `BridgeCommand::Log { follow: bool, lines: usize }`

This is the `bridge log` the bridge plan deferred. One writer — the single-instance lock
guarantees it — so the file is a continuous record across restarts rather than an interleaving.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::BridgeDir;

    #[test]
    fn installing_creates_the_log_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let guard = install(&dir).unwrap();
        tracing::info!(target: "sapphire_framework_bridge", "hello from the test");
        drop(guard);

        let text = std::fs::read_to_string(dir.log_dir().join(LOG_FILE)).unwrap();
        assert!(text.contains("hello from the test"), "{text}");
    }

    #[test]
    fn the_log_rotates_at_its_size_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let guard = install_with_limit(&dir, 4096).unwrap();
        for n in 0..2000 {
            tracing::info!(target: "sapphire_framework_bridge", "line {n} padded {}", "x".repeat(64));
        }
        drop(guard);

        let files: Vec<String> = std::fs::read_dir(dir.log_dir())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(files.len() > 1, "the log never rotated: {files:?}");
        assert!(files.len() <= LOG_KEEP + 1, "too many kept: {files:?}");
    }

    #[test]
    fn a_restart_continues_the_same_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();

        let guard = install(&dir).unwrap();
        tracing::info!(target: "sapphire_framework_bridge", "first run");
        drop(guard);

        let guard = install(&dir).unwrap();
        tracing::info!(target: "sapphire_framework_bridge", "second run");
        drop(guard);

        let text = std::fs::read_to_string(dir.log_dir().join(LOG_FILE)).unwrap();
        assert!(text.contains("first run") && text.contains("second run"), "{text}");
    }

    #[test]
    fn reading_the_tail_of_a_missing_log_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        assert!(tail(&dir, 20).unwrap().is_empty());
    }

    #[test]
    fn the_tail_returns_the_last_lines_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        std::fs::create_dir_all(dir.log_dir()).unwrap();
        std::fs::write(
            dir.log_dir().join(LOG_FILE),
            (0..100).map(|n| format!("line {n}\n")).collect::<String>(),
        )
        .unwrap();

        let lines = tail(&dir, 3).unwrap();
        assert_eq!(lines, vec!["line 97", "line 98", "line 99"]);
    }
}
```

`install_with_limit` is `install` with the rotation threshold as a parameter; make `install`
call it with `LOG_MAX_BYTES` so the test exercises the real path.

- [ ] **Step 2–5: Implement, verify, commit**

Add `tracing-appender = "0.2"`. `bridge log` prints the tail, and with `--follow` keeps
printing as the file grows.

```bash
cargo test -p sapphire-framework-bridge --all-features logging
git commit -m "feat(bridge): write a log any process can read, and add bridge log"
```

---

### Task 7: Update the CLI table and the architecture note

**Files:**
- Modify: `crates/sapphire-framework-bridge/src/command.rs` (tests for the new subcommand)
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Extend the CLI parse test**

```rust
#[test]
fn the_log_subcommand_parses() {
    assert!(Probe::try_parse_from(["b", "log"]).is_ok());
    assert!(Probe::try_parse_from(["b", "log", "--follow"]).is_ok());
    assert!(Probe::try_parse_from(["b", "log", "--lines", "50"]).is_ok());
}
```

- [ ] **Step 2: Note the operational surface**

`docs/ARCHITECTURE.md`, after the bridge rows:

```markdown
> **bridge の可視化**: `<bridge dir>/status.json`（5 秒ごと + 変化時、アトミック書き込み）と
> `<bridge dir>/logs/node.log`（10 MiB × 3 でローテーション）。書き手は単一インスタンスロックが
> 保証する 1 プロセスのみなので、ログは再起動をまたいで連続する。`sapphire-bridge status` は
> 稼働中の bridge に問い合わせ、応答が無ければ `status.json` を読む。
```

- [ ] **Step 3: Run everything and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates docs/ARCHITECTURE.md
git commit -m "docs(bridge): record the operational surface"
```

---

## What this plan does not cover

| | Left for |
|---|---|
| `service install` for the bridge and for app servers | step 10 |
| Removing `-rpc`, `-remote-*` and `-blob`, and the per-kind directory migration | step 11 |
| mDNS discovery of devices on the same network | iroh's discovery is configured in Task 3; a sapphire-specific layer on top is not planned |
| Chunked or resumable transfer of large files | deferred with the sync spec's §2.8, and it moves with `-sync`, not with this layer |
| Bandwidth limits and scheduling | nobody has asked; the hooks would go in `LivePeers::fan_out` |
