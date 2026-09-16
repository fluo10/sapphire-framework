# Pairing and Workgroups (`sapphire-framework-bridge`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Let a second device join a workgroup and stay in it — an invite ticket, a pairing
exchange that happens before authorization can apply, a device record that replicates to every
other device, and the list of workspaces a joiner can then map.

**Architecture:** The workgroup's own workspace becomes a replica like any other, with the
bridge as its owner (spec §1: the bridge is the app server of one app, and that app is the
workgroup). Device records and the workspace list live in it, so a pairing on one device
reaches the rest by the same replication every other workspace uses. Pairing itself cannot go
through that path — a joiner is not yet a member — so it gets its own protocol on a second
ALPN, gated by a single-use secret instead of by workgroup membership.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), `sapphire-framework-sync`,
`sapphire-framework-session`, `sapphire-framework-registry`, iroh 1.2, postcard 1,
data-encoding 2, getrandom 0.4, subtle 2, tokio 1, serde + toml, grain-id 0.16, clap 4.

**Spec:** `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` §3.5 (workgroups and
authorization) and §3.6 (pairing), read through the substitution table at the head of its §3;
`docs/superpowers/specs/2026-09-16-process-architecture-design.md` §1 and §5. Implementation
order step 8 of the process-architecture spec's §9.

**Depends on:** steps 2, 6 and 7 (`2026-09-16-registry-devices-plan.md`,
`2026-09-16-bridge-basics-plan.md`, `2026-09-16-sync-runtime-plan.md`).

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task.
- Every public item carries a doc comment.
- The pairing ALPN is `sapphire/pair/1`, separate from `sapphire/ws/1`.
- An invite's secret is **32 bytes from the system random source**, compared in **constant
  time** (`subtle::ConstantTimeEq`). A byte-by-byte comparison leaks the secret to a patient
  attacker who can retry.
- An invite is **single use** and expires after **10 minutes** by default.
- A ticket is `sapphire:` followed by Crockford-free base32 (`data-encoding::BASE32_NOPAD`) of
  a postcard encoding, so it survives copy-paste through a chat window.
- The first release allows **one workgroup per host**: `workgroup create` and `workgroup join`
  refuse when the host already belongs to one. The layout and the wire format already support
  several, so lifting it is a CLI change.

## The one thing pairing cannot do

**A joiner is not a member yet**, so the authorization the bridge applies to every other
connection (bridge plan, Task 6) cannot apply to this one. That is why pairing has its own
ALPN and its own gate: a secret that was handed over out of band, is good once, and expires.

Everything that follows a successful pairing goes back through the normal path. Nothing else
in the system gets an exception.

## File Structure

```
crates/sapphire-framework-bridge/src/
    invite.rs      # Invite, Ticket, invites.toml
    pairing.rs     # the pair/1 protocol: join and admit
    wgsync.rs      # the workgroup workspace as a replica
    workgroup.rs   # MODIFIED: join, this_device by node id, workspace list
    command.rs     # MODIFIED: device invite, workgroup join, workspace list
```

---

### Task 1: The workgroup workspace as a replica

**Files:**
- Create: `crates/sapphire-framework-bridge/src/wgsync.rs`
- Modify: `crates/sapphire-framework-bridge/src/{lib.rs,workgroup.rs}`, `Cargo.toml`
- Test: inline `#[cfg(test)] mod tests` in `wgsync.rs`

**Interfaces:**
- Produces:
  - `WorkgroupReplica::open(dir: &BridgeDir, workgroup: &Workgroup, device_id: GrainId) -> Result<WorkgroupReplica>`
  - `async WorkgroupReplica::session(&self, stream: S) -> Result<()>`
  - `WorkgroupReplica::scan(&self) -> Result<()>`
  - `WorkgroupReplica::workspace_id(&self) -> GrainId` — the workgroup id doubles as the
    workspace id of its own workspace, so no second identifier is needed
- Adds `sapphire-sync` and `sapphire-session` to the manifest — the dependency the bridge plan
  deliberately left out until there was a use for it.

The replica's root is `<bridge dir>/workgroups/<id>/root/`, its store
`<bridge dir>/workgroups/<id>/replica/`. Everything in the bridge plan's §5 layout is already
in the right place.

**The bridge registers this workspace with itself.** In the routing table it looks like any
other row, with `app_name` `"bridge"` — so an incoming stream for it is routed and spliced by
the same code, and a second implementation is not needed.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::BridgeDir;

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn host(name: &str, node: &str) -> (tempfile::TempDir, BridgeDir, Workgroup) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let wg = Workgroup::create(&dir, "test", name, node).unwrap();
        (tmp, dir, wg)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_device_record_replicates_to_the_other_side() {
        let (_ta, dir_a, wg_a) = host("host-a", NODE_A);
        let (_tb, dir_b, wg_b) = host("host-b", NODE_B);

        // Force both sides onto the same workgroup id, as a real join would.
        let wg_b = crate::testing::adopt_workgroup(&dir_b, &wg_a).unwrap();

        let a = WorkgroupReplica::open(&dir_a, &wg_a, wg_a.this_device().unwrap().id).unwrap();
        let b = WorkgroupReplica::open(&dir_b, &wg_b, wg_b.this_device().unwrap().id).unwrap();

        // A learns about B.
        wg_a.devices().unwrap().add("host-b", Some(NODE_B.to_owned()), None).unwrap();
        a.scan().unwrap();

        let (left, right) = tokio::io::duplex(64 * 1024);
        let (x, y) = tokio::join!(a.session(left), b.session(right));
        x.unwrap();
        y.unwrap();

        assert!(
            wg_b.devices().unwrap().by_node_id(NODE_B).is_some(),
            "B must learn its own record from A"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_retirement_replicates() {
        let (_ta, dir_a, wg_a) = host("host-a", NODE_A);
        let (_tb, dir_b, _) = host("host-b", NODE_B);
        let wg_b = crate::testing::adopt_workgroup(&dir_b, &wg_a).unwrap();

        wg_a.devices().unwrap().add("phone", Some("c1".repeat(32)), None).unwrap();
        let a = WorkgroupReplica::open(&dir_a, &wg_a, wg_a.this_device().unwrap().id).unwrap();
        let b = WorkgroupReplica::open(&dir_b, &wg_b, wg_b.this_device().unwrap().id).unwrap();
        a.scan().unwrap();
        let (l, r) = tokio::io::duplex(64 * 1024);
        let _ = tokio::join!(a.session(l), b.session(r));
        assert!(wg_b.devices().unwrap().by_node_id(&"c1".repeat(32)).is_some());

        // A retires the phone.
        wg_a.devices().unwrap().retire("phone").unwrap();
        a.scan().unwrap();
        let (l, r) = tokio::io::duplex(64 * 1024);
        let _ = tokio::join!(a.session(l), b.session(r));

        let devices = wg_b.devices().unwrap();
        let phone = devices.by_node_id(&"c1".repeat(32)).expect("the record stays");
        assert!(phone.is_retired(), "revocation must reach every device");
    }

    #[test]
    fn the_workgroup_id_is_its_own_workspace_id() {
        let (_t, dir, wg) = host("host-a", NODE_A);
        let replica = WorkgroupReplica::open(&dir, &wg, wg.this_device().unwrap().id).unwrap();
        assert_eq!(replica.workspace_id(), wg.id);
    }

    #[test]
    fn opening_twice_against_one_directory_fails() {
        let (_t, dir, wg) = host("host-a", NODE_A);
        let device = wg.this_device().unwrap().id;
        let _first = WorkgroupReplica::open(&dir, &wg, device).unwrap();
        assert!(
            WorkgroupReplica::open(&dir, &wg, device).is_err(),
            "the replica store is a redb database; a second open must fail loudly"
        );
    }
}
```

`crate::testing::adopt_workgroup` writes another host's `workgroup.toml` and id into this
bridge directory, standing in for the join of Task 4. Put it behind
`#[cfg(any(test, feature = "test-util"))]`, and delete it when Task 4 makes it redundant if
nothing else uses it.

- [ ] **Step 2: Run the tests to verify they fail, implement, verify, commit**

`WorkgroupReplica` wraps a `Replica` opened on the workgroup root, with `app_name` `"bridge"`
and the bridge's device id, and `session` is `sapphire_framework_session::run_session` with
`workspace_id = workgroup.id`. Registering the workgroup workspace in the routing table
happens in `Bridge::run`, right after the workgroup is opened.

```bash
cargo test -p sapphire-framework-bridge --all-features wgsync
git commit -m "feat(bridge): replicate the workgroup's own workspace"
```

---

### Task 2: Invites

**Files:**
- Create: `crates/sapphire-framework-bridge/src/invite.rs`
- Modify: `crates/sapphire-framework-bridge/src/lib.rs`, `Cargo.toml`
- Test: inline `#[cfg(test)] mod tests` in `invite.rs`

**Interfaces:**
- Produces:
  - `TICKET_PREFIX: &str = "sapphire:"`, `DEFAULT_TTL: Duration = 10 min`
  - `Ticket { workgroup_id: GrainId, node_addr: Vec<u8>, secret: [u8; 32], expires_at: DateTime<Utc> }`
    with `encode(&self) -> String` and `decode(text: &str) -> Result<Ticket>`
  - `Invite { id: GrainId, secret_hex: String, device_name: String, expires_at, used_at: Option<DateTime<Utc>> }`
  - `Invites::load(path: &Path) -> Result<Invites>`,
    `create(&mut self, device_name: &str, ttl: Duration) -> Result<(Invite, [u8; 32])>`,
    `redeem(&mut self, secret: &[u8; 32]) -> Result<Invite>`,
    `entries(&self) -> &[Invite]`, `prune(&mut self) -> Result<usize>`
- Adds `postcard = "1"`, `data-encoding = "2"`, `subtle = "2"`, `chrono` to the manifest.

**`redeem` is where the security of the whole scheme sits.** It must: re-read the file (any
process may have issued the invite), compare in constant time, reject an expired or already
used invite, and mark it used **before** returning. The tests check each of those separately.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn invites(dir: &std::path::Path) -> Invites {
        Invites::load(&dir.join("invites.toml")).unwrap()
    }

    #[test]
    fn a_ticket_round_trips_through_its_text_form() {
        let ticket = Ticket {
            workgroup_id: grain_id::GrainId::random(),
            node_addr: vec![1, 2, 3, 4],
            secret: [7u8; 32],
            expires_at: chrono::Utc::now() + chrono::Duration::minutes(10),
        };
        let text = ticket.encode();
        assert!(text.starts_with(TICKET_PREFIX), "{text}");
        let back = Ticket::decode(&text).unwrap();
        assert_eq!(back.workgroup_id, ticket.workgroup_id);
        assert_eq!(back.secret, ticket.secret);
    }

    #[test]
    fn a_ticket_survives_being_pasted_with_whitespace() {
        let ticket = Ticket {
            workgroup_id: grain_id::GrainId::random(),
            node_addr: vec![],
            secret: [1u8; 32],
            expires_at: chrono::Utc::now(),
        };
        let text = format!("  {}\n", ticket.encode());
        assert!(Ticket::decode(&text).is_ok());
    }

    #[test]
    fn a_ticket_without_the_prefix_is_refused() {
        assert!(Ticket::decode("ABCDEF").is_err());
    }

    #[test]
    fn a_corrupt_ticket_is_an_error_not_a_panic() {
        assert!(Ticket::decode("sapphire:!!!!").is_err());
    }

    #[test]
    fn an_invite_can_be_redeemed_once() {
        let tmp = tempfile::tempdir().unwrap();
        let mut invites = invites(tmp.path());
        let (_invite, secret) = invites.create("phone", DEFAULT_TTL).unwrap();

        assert!(invites.redeem(&secret).is_ok());
        let err = invites.redeem(&secret).unwrap_err();
        assert!(err.to_string().contains("already used"), "{err}");
    }

    #[test]
    fn a_wrong_secret_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let mut invites = invites(tmp.path());
        invites.create("phone", DEFAULT_TTL).unwrap();

        let err = invites.redeem(&[0u8; 32]).unwrap_err();
        assert!(err.to_string().contains("no matching invite"), "{err}");
    }

    #[test]
    fn an_expired_invite_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let mut invites = invites(tmp.path());
        let (_invite, secret) = invites.create("phone", Duration::from_secs(0)).unwrap();

        let err = invites.redeem(&secret).unwrap_err();
        assert!(err.to_string().contains("expired"), "{err}");
    }

    #[test]
    fn redeeming_rereads_the_file_so_any_process_may_issue_invites() {
        let tmp = tempfile::tempdir().unwrap();
        let mut reader = invites(tmp.path());

        // A different process writes an invite after `reader` was loaded.
        let mut writer = invites(tmp.path());
        let (_invite, secret) = writer.create("phone", DEFAULT_TTL).unwrap();

        assert!(
            reader.redeem(&secret).is_ok(),
            "an invite issued elsewhere must be redeemable"
        );
    }

    #[test]
    fn two_invites_are_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let mut invites = invites(tmp.path());
        let (_a, secret_a) = invites.create("phone", DEFAULT_TTL).unwrap();
        let (_b, secret_b) = invites.create("tablet", DEFAULT_TTL).unwrap();

        assert_eq!(invites.redeem(&secret_a).unwrap().device_name, "phone");
        assert_eq!(invites.redeem(&secret_b).unwrap().device_name, "tablet");
    }

    #[test]
    fn pruning_removes_expired_and_used_invites() {
        let tmp = tempfile::tempdir().unwrap();
        let mut invites = invites(tmp.path());
        let (_a, secret_a) = invites.create("used", DEFAULT_TTL).unwrap();
        invites.create("stale", Duration::from_secs(0)).unwrap();
        invites.create("live", DEFAULT_TTL).unwrap();
        invites.redeem(&secret_a).unwrap();

        assert_eq!(invites.prune().unwrap(), 2);
        assert_eq!(invites.entries().len(), 1);
        assert_eq!(invites.entries()[0].device_name, "live");
    }

    #[test]
    fn a_secret_is_thirty_two_bytes_from_the_system_source() {
        let tmp = tempfile::tempdir().unwrap();
        let mut invites = invites(tmp.path());
        let (_a, first) = invites.create("a", DEFAULT_TTL).unwrap();
        let (_b, second) = invites.create("b", DEFAULT_TTL).unwrap();
        assert_ne!(first, second, "two invites must not share a secret");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features invite`
Expected: FAIL.

- [ ] **Step 3: Implement invites**

```rust
//! Invite tickets and the file that tracks them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use data_encoding::BASE32_NOPAD;
use grain_id::GrainId;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::{Error, Result};

/// What a ticket's text form starts with.
pub const TICKET_PREFIX: &str = "sapphire:";

/// How long an invite is good for unless told otherwise.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);

/// What a joiner needs to reach the inviter and prove it was invited.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Ticket {
    /// The workgroup being joined.
    pub workgroup_id: GrainId,
    /// The inviter's address, as iroh encodes a `NodeAddr`.
    pub node_addr: Vec<u8>,
    /// The single-use secret.
    pub secret: [u8; 32],
    /// When the invite stops working.
    pub expires_at: DateTime<Utc>,
}

impl Ticket {
    /// The text a user copies between devices.
    ///
    /// Base32 without padding, so it survives a chat window, a QR code and a double click.
    pub fn encode(&self) -> String {
        let bytes = postcard::to_stdvec(self).expect("a ticket always encodes");
        format!("{TICKET_PREFIX}{}", BASE32_NOPAD.encode(&bytes))
    }

    /// Parse a ticket, tolerating surrounding whitespace.
    pub fn decode(text: &str) -> Result<Ticket> {
        let trimmed = text.trim();
        let body = trimmed.strip_prefix(TICKET_PREFIX).ok_or_else(|| {
            Error::Config(format!("a ticket starts with {TICKET_PREFIX:?}"))
        })?;
        let bytes = BASE32_NOPAD
            .decode(body.as_bytes())
            .map_err(|e| Error::Config(format!("this is not a ticket: {e}")))?;
        postcard::from_bytes(&bytes)
            .map_err(|e| Error::Config(format!("this is not a ticket: {e}")))
    }
}

/// One outstanding invitation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Invite {
    /// Its id, for listing and revoking.
    pub id: GrainId,
    /// The secret, hex-encoded so the file stays human-readable.
    pub secret_hex: String,
    /// What the joining device will be called.
    pub device_name: String,
    /// When it stops working.
    pub expires_at: DateTime<Utc>,
    /// When it was used, if it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_at: Option<DateTime<Utc>>,
}

impl Invite {
    /// Is this invite still usable?
    pub fn is_live(&self) -> bool {
        self.used_at.is_none() && self.expires_at > Utc::now()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RawInvites {
    #[serde(default)]
    invite: Vec<Invite>,
}

const HEADER: &str = "\
# Pending pairing invitations.
#
# Each is single use and expires. Deleting one cancels it; nothing else needs doing.
";

/// The invite file.
#[derive(Debug)]
pub struct Invites {
    path: PathBuf,
    entries: Vec<Invite>,
}

impl Invites {
    /// Read the file. A missing file is an empty list.
    pub fn load(path: &Path) -> Result<Invites> {
        let entries = match std::fs::read_to_string(path) {
            Ok(text) => {
                toml::from_str::<RawInvites>(&text)
                    .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?
                    .invite
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(Error::Io(e)),
        };
        Ok(Invites { path: path.to_owned(), entries })
    }

    /// Issue an invite, returning it and the secret to put in the ticket.
    pub fn create(&mut self, device_name: &str, ttl: Duration) -> Result<(Invite, [u8; 32])> {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret)
            .map_err(|e| Error::Config(format!("no system random source: {e}")))?;
        let invite = Invite {
            id: GrainId::random(),
            secret_hex: secret.iter().map(|b| format!("{b:02x}")).collect(),
            device_name: device_name.to_owned(),
            expires_at: Utc::now()
                + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::zero()),
            used_at: None,
        };
        self.reload()?;
        let mut next = self.entries.clone();
        next.push(invite.clone());
        self.save(next)?;
        Ok((invite, secret))
    }

    /// Use an invite up.
    ///
    /// Re-reads the file first: any process may have issued the invite, and the one
    /// answering the pairing is not necessarily the one that created it.
    pub fn redeem(&mut self, secret: &[u8; 32]) -> Result<Invite> {
        self.reload()?;
        let offered = secret.iter().map(|b| format!("{b:02x}")).collect::<String>();

        // Constant time, and over every entry: returning early on the first match would
        // leak which invite matched through timing.
        let mut found: Option<usize> = None;
        for (i, invite) in self.entries.iter().enumerate() {
            let hit: bool = invite.secret_hex.as_bytes().ct_eq(offered.as_bytes()).into();
            if hit && found.is_none() {
                found = Some(i);
            }
        }
        let Some(i) = found else {
            return Err(Error::Unauthorized("no matching invite".to_owned()));
        };
        if self.entries[i].used_at.is_some() {
            return Err(Error::Unauthorized("this invite was already used".to_owned()));
        }
        if self.entries[i].expires_at <= Utc::now() {
            return Err(Error::Unauthorized("this invite has expired".to_owned()));
        }

        let mut next = self.entries.clone();
        next[i].used_at = Some(Utc::now());
        let used = next[i].clone();
        // Marked used before returning, so a concurrent second attempt loses.
        self.save(next)?;
        Ok(used)
    }

    /// Every invite, live or not.
    pub fn entries(&self) -> &[Invite] {
        &self.entries
    }

    /// Drop used and expired invites. Returns how many went.
    pub fn prune(&mut self) -> Result<usize> {
        self.reload()?;
        let before = self.entries.len();
        let next: Vec<Invite> = self.entries.iter().filter(|i| i.is_live()).cloned().collect();
        let removed = before - next.len();
        if removed > 0 {
            self.save(next)?;
        }
        Ok(removed)
    }

    fn reload(&mut self) -> Result<()> {
        self.entries = Invites::load(&self.path)?.entries;
        Ok(())
    }

    fn save(&mut self, entries: Vec<Invite>) -> Result<()> {
        let body = toml::to_string_pretty(&RawInvites { invite: entries.clone() })
            .map_err(|e| Error::Config(e.to_string()))?;
        crate::routes::write_atomic(&self.path, HEADER, &body)?;
        self.entries = entries;
        Ok(())
    }
}
```

Make `routes::write_atomic` `pub(crate)` so this reuses it rather than growing a second copy.

- [ ] **Step 4: Run the tests to verify they pass, then commit**

```bash
cargo test -p sapphire-framework-bridge --all-features invite
git commit -m "feat(bridge): issue single-use invite tickets"
```

---

### Task 3: The `pair/1` protocol

**Files:**
- Create: `crates/sapphire-framework-bridge/src/pairing.rs`
- Modify: `crates/sapphire-framework-bridge/src/{lib.rs,peer.rs,iroh.rs}`
- Test: inline `#[cfg(test)] mod tests` in `pairing.rs`

**Interfaces:**
- Produces:
  - `PAIR_ALPN: &[u8] = b"sapphire/pair/1"`
  - `JoinRequest { secret: [u8; 32], device_name: String, node_id: String }`
  - `JoinResponse::{Admitted { workgroup_id: GrainId, workgroup_name: String, device_id: GrainId }, Rejected(String)}`
  - `async join<S>(stream: S, request: JoinRequest) -> Result<JoinResponse>`
  - `async admit<S>(stream: S, invites: &mut Invites, workgroup: &Workgroup) -> Result<Option<Device>>`
  - `PeerTransport` gains `async open_pairing(&self, node_addr: &[u8]) -> Result<BoxedStream>`
    and `accept` reports which ALPN the stream arrived on

**What `admit` does, in this order:** redeem the secret (constant time, single use, expiry
checked), then write the device record with the joiner's node id as a **local write** to the
workgroup workspace, then reply. Writing the record before replying is what makes the
admission durable if the reply is lost — the joiner can retry the *sync*, which is idempotent,
rather than the *pairing*, which is not.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::BridgeDir;
    use crate::invite::{DEFAULT_TTL, Invites};

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn inviter() -> (tempfile::TempDir, BridgeDir, Workgroup) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        (tmp, dir, wg)
    }

    /// Run `join` and `admit` against each other over a duplex.
    async fn pair(
        dir: &BridgeDir,
        wg: &Workgroup,
        secret: [u8; 32],
        name: &str,
        node: &str,
    ) -> (Result<JoinResponse>, Result<Option<sapphire_registry::Device>>) {
        let (left, right) = tokio::io::duplex(8 * 1024);
        let mut invites = Invites::load(&dir.root.join("invites.toml")).unwrap();
        tokio::join!(
            join(
                left,
                JoinRequest {
                    secret,
                    device_name: name.to_owned(),
                    node_id: node.to_owned(),
                },
            ),
            admit(right, &mut invites, wg),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_valid_secret_admits_the_device() {
        let (_tmp, dir, wg) = inviter();
        let (_invite, secret) = Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("phone", DEFAULT_TTL)
            .unwrap();

        let (joined, admitted) = pair(&dir, &wg, secret, "phone", NODE_B).await;
        match joined.unwrap() {
            JoinResponse::Admitted { workgroup_id, workgroup_name, .. } => {
                assert_eq!(workgroup_id, wg.id);
                assert_eq!(workgroup_name, "home");
            }
            other => panic!("got {other:?}"),
        }
        assert!(admitted.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_device_record_is_written_before_the_reply() {
        let (_tmp, dir, wg) = inviter();
        let (_invite, secret) = Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("phone", DEFAULT_TTL)
            .unwrap();

        let (_joined, _admitted) = pair(&dir, &wg, secret, "phone", NODE_B).await;

        let devices = wg.devices().unwrap();
        let phone = devices.by_node_id(NODE_B).expect("the record must exist");
        assert_eq!(phone.name, "phone");
        assert!(!phone.is_retired());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_wrong_secret_is_rejected_and_writes_nothing() {
        let (_tmp, dir, wg) = inviter();
        Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("phone", DEFAULT_TTL)
            .unwrap();

        let (joined, admitted) = pair(&dir, &wg, [0u8; 32], "phone", NODE_B).await;
        assert!(matches!(joined.unwrap(), JoinResponse::Rejected(_)));
        assert!(admitted.unwrap().is_none());
        assert!(wg.devices().unwrap().by_node_id(NODE_B).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_secret_cannot_be_used_twice() {
        let (_tmp, dir, wg) = inviter();
        let (_invite, secret) = Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("phone", DEFAULT_TTL)
            .unwrap();

        let (first, _) = pair(&dir, &wg, secret, "phone", NODE_B).await;
        assert!(matches!(first.unwrap(), JoinResponse::Admitted { .. }));

        let third_node = "c1".repeat(32);
        let (second, _) = pair(&dir, &wg, secret, "intruder", &third_node).await;
        assert!(matches!(second.unwrap(), JoinResponse::Rejected(_)));
        assert!(wg.devices().unwrap().by_node_id(&third_node).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_expired_invite_is_rejected() {
        let (_tmp, dir, wg) = inviter();
        let (_invite, secret) = Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("phone", std::time::Duration::from_secs(0))
            .unwrap();

        let (joined, _) = pair(&dir, &wg, secret, "phone", NODE_B).await;
        assert!(matches!(joined.unwrap(), JoinResponse::Rejected(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_device_name_that_is_taken_is_rejected() {
        let (_tmp, dir, wg) = inviter();
        let (_invite, secret) = Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("laptop", DEFAULT_TTL)
            .unwrap();

        // "laptop" is already this host's own name.
        let (joined, _) = pair(&dir, &wg, secret, "laptop", NODE_B).await;
        assert!(
            matches!(joined.unwrap(), JoinResponse::Rejected(ref why) if why.contains("laptop")),
            "a duplicate name must be refused with a message a user can act on"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_node_id_that_is_already_a_member_is_rejected() {
        let (_tmp, dir, wg) = inviter();
        let (_invite, secret) = Invites::load(&dir.root.join("invites.toml"))
            .unwrap()
            .create("laptop-again", DEFAULT_TTL)
            .unwrap();

        let (joined, _) = pair(&dir, &wg, secret, "laptop-again", NODE_A).await;
        assert!(matches!(joined.unwrap(), JoinResponse::Rejected(_)));
    }
}
```

- [ ] **Step 2–5: Implement, verify, commit**

The wire format is the same tag-and-length framing as
`sapphire-framework-session` — reuse `sapphire_framework_session::frame` rather than inventing
a third; add `sapphire-session` to the bridge's dependencies and make `frame` public there.

In `iroh.rs`, add `PAIR_ALPN` to the endpoint's ALPN list and report the ALPN from `accept` so
`Bridge::run` can route a pairing connection to `admit` and everything else to the
switchboard. **A pairing connection is the only one that skips `Workgroup::authorize`**, and
the reason is in the doc comment: the joiner is not a member yet.

```bash
cargo test -p sapphire-framework-bridge --all-features pairing
git commit -m "feat(bridge): admit a new device over the pairing protocol"
```

---

### Task 4: `workgroup join`, and finding our own record

**Files:**
- Modify: `crates/sapphire-framework-bridge/src/workgroup.rs`
- Test: inline `#[cfg(test)] mod tests` in `workgroup.rs`

**Interfaces:**
- Produces:
  - `async Workgroup::join(dir: &BridgeDir, ticket: &Ticket, device_name: &str, transport: &dyn PeerTransport) -> Result<Workgroup>`
  - `Workgroup::this_device(&self, node_id: &str) -> Result<Device>` — **signature change**:
    finds the record whose `node_id` matches, instead of taking the first

**The placeholder this removes:** the bridge plan's `this_device` returned the first record,
which is only correct while `create` is the only way in. Every caller must now pass this
host's node id. Find them with `cargo check` and fix each; the `// TODO(step 8)` comment left
there says exactly this.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn this_device_is_found_by_node_id_not_by_position() {
    let (_tmp, dir) = bridge_dir();
    let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
    // Another device is added and happens to sort first.
    wg.devices().unwrap().add("aaa-phone", Some(NODE_B.to_owned()), None).unwrap();

    assert_eq!(wg.this_device(NODE_A).unwrap().name, "laptop");
    assert_eq!(wg.this_device(NODE_B).unwrap().name, "aaa-phone");
}

#[test]
fn a_node_id_that_is_not_a_member_has_no_record() {
    let (_tmp, dir) = bridge_dir();
    let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
    assert!(wg.this_device(&"c1".repeat(32)).is_err());
}

/// An inviter on a loopback network, plus a ticket for it.
///
/// `LoopbackTransport` carries `node_addr` as the node id's bytes, so a ticket made here
/// reaches it the same way a real one reaches an iroh address.
async fn invited(
    net: &crate::peer::LoopbackNetwork,
    device_name: &str,
) -> (tempfile::TempDir, BridgeDir, Workgroup, crate::invite::Ticket) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
    let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();

    let (_invite, secret) = crate::invite::Invites::load(&dir.root.join("invites.toml"))
        .unwrap()
        .create(device_name, crate::invite::DEFAULT_TTL)
        .unwrap();
    let ticket = crate::invite::Ticket {
        workgroup_id: wg.id,
        node_addr: NODE_A.as_bytes().to_vec(),
        secret,
        expires_at: chrono::Utc::now() + chrono::Duration::minutes(10),
    };

    // Answer one pairing connection in the background, as `Bridge::run` would.
    let transport = net.transport(NODE_A);
    let answering_dir = dir.clone();
    let answering_wg = wg.clone();
    tokio::spawn(async move {
        if let Ok((_from, _alpn, stream)) = transport.accept_pairing().await {
            let mut invites =
                crate::invite::Invites::load(&answering_dir.root.join("invites.toml")).unwrap();
            let _ = crate::pairing::admit(stream, &mut invites, &answering_wg).await;
        }
    });

    (tmp, dir, wg, ticket)
}

#[tokio::test(flavor = "multi_thread")]
async fn joining_writes_the_workgroup_locally() {
    let net = crate::peer::LoopbackNetwork::new();
    let (_inviter_tmp, _inviter_dir, inviter_wg, ticket) = invited(&net, "phone").await;

    let joiner_tmp = tempfile::tempdir().unwrap();
    let joiner_dir = BridgeDir::at(joiner_tmp.path().join("bridge")).unwrap();
    let joiner_transport = net.transport(NODE_B);

    let joined = Workgroup::join(&joiner_dir, &ticket, "phone", &joiner_transport)
        .await
        .unwrap();

    assert_eq!(joined.id, inviter_wg.id, "both sides must agree on the workgroup");
    assert_eq!(joined.name, "home");
    assert_eq!(
        joined.this_device(NODE_B).unwrap().name,
        "phone",
        "the joiner must have its own record locally"
    );
    assert!(
        Workgroup::open(&joiner_dir).unwrap().is_some(),
        "the workgroup must survive a reopen"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn joining_a_second_workgroup_is_refused_for_now() {
    let net = crate::peer::LoopbackNetwork::new();
    let (_inviter_tmp, _inviter_dir, _wg, ticket) = invited(&net, "phone").await;

    let joiner_tmp = tempfile::tempdir().unwrap();
    let joiner_dir = BridgeDir::at(joiner_tmp.path().join("bridge")).unwrap();
    // The joiner already belongs to one.
    Workgroup::create(&joiner_dir, "work", "phone", NODE_B).unwrap();

    let err = Workgroup::join(&joiner_dir, &ticket, "phone", &net.transport(NODE_B))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_join_leaves_no_workgroup_directory_behind() {
    // A half-created workgroup would make every later attempt fail with "already belongs
    // to a workgroup", which is the worst possible message for someone who has joined
    // nothing.
    let net = crate::peer::LoopbackNetwork::new();
    let (_inviter_tmp, _inviter_dir, wg, mut ticket) = invited(&net, "phone").await;
    ticket.secret = [0u8; 32]; // not the invited secret

    let joiner_tmp = tempfile::tempdir().unwrap();
    let joiner_dir = BridgeDir::at(joiner_tmp.path().join("bridge")).unwrap();

    assert!(
        Workgroup::join(&joiner_dir, &ticket, "phone", &net.transport(NODE_B))
            .await
            .is_err()
    );
    assert!(
        Workgroup::open(&joiner_dir).unwrap().is_none(),
        "a failed join must leave the host able to try again"
    );
    assert!(
        !joiner_dir.workgroup_dir(wg.id).exists(),
        "no partial directory may remain"
    );
}
```

`LoopbackTransport` needs a pairing channel of its own, so add `open_pairing` and
`accept_pairing` alongside `open` and `accept` in Task 3, with the same loopback backing. On
iroh they are the same endpoint with a different ALPN; on loopback they are a second inbox.

- [ ] **Step 2–5: Implement, verify, commit**

`join` connects over `PAIR_ALPN` to the ticket's address, runs `pairing::join`, and on
`Admitted` creates the workgroup directory with the returned id and name and writes its own
device record. On `Rejected` it removes anything it created — the last test is what enforces
that.

```bash
cargo test -p sapphire-framework-bridge --all-features workgroup
git commit -m "feat(bridge)!: join a workgroup, and find this host's record by node id"
```

---

### Task 5: The workgroup's workspace list

**Files:**
- Modify: `crates/sapphire-framework-bridge/src/{workgroup.rs,control.rs}`
- Test: inline tests in `workgroup.rs`, plus `tests/switchboard.rs`

**Interfaces:**
- Produces:
  - `WorkgroupWorkspace { workspace_id: GrainId, app_name: String, name: String }`
  - `Workgroup::workspaces(&self) -> Result<Vec<WorkgroupWorkspace>>` — reads
    `root/workspaces/<id>.toml`
  - `Workgroup::publish_workspace(&self, ws: &WorkgroupWorkspace) -> Result<()>`
  - `bridge.register` publishes each newly registered workspace, so enabling sync on one host
    makes the workspace visible to every other

One file per workspace, for the same reason as one file per device: two hosts publishing at
the same moment write different files.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod workspace_list_tests {
    use super::*;
    use crate::dir::BridgeDir;

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn workgroup() -> (tempfile::TempDir, BridgeDir, Workgroup) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        (tmp, dir, wg)
    }

    fn entry(app: &str, name: &str) -> WorkgroupWorkspace {
        WorkgroupWorkspace {
            workspace_id: GrainId::random(),
            app_name: app.to_owned(),
            name: name.to_owned(),
        }
    }

    #[test]
    fn publishing_writes_one_file_per_workspace() {
        let (_tmp, _dir, wg) = workgroup();
        let notes = entry("sapphire-journal", "notes");
        let books = entry("sapphire-ledger", "books");
        wg.publish_workspace(&notes).unwrap();
        wg.publish_workspace(&books).unwrap();

        let listing = wg.dir.join("root").join("workspaces");
        let mut names: Vec<String> = std::fs::read_dir(&listing)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        let mut want = vec![
            format!("{}.toml", notes.workspace_id),
            format!("{}.toml", books.workspace_id),
        ];
        want.sort();
        assert_eq!(names, want);
    }

    #[test]
    fn the_list_survives_a_reload() {
        let (_tmp, dir, wg) = workgroup();
        let notes = entry("sapphire-journal", "notes");
        wg.publish_workspace(&notes).unwrap();

        let reopened = Workgroup::open(&dir).unwrap().expect("the workgroup");
        let listed = reopened.workspaces().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].workspace_id, notes.workspace_id);
        assert_eq!(listed[0].app_name, "sapphire-journal");
        assert_eq!(listed[0].name, "notes");
    }

    #[test]
    fn publishing_the_same_workspace_twice_does_not_duplicate_it() {
        let (_tmp, _dir, wg) = workgroup();
        let notes = entry("sapphire-journal", "notes");
        wg.publish_workspace(&notes).unwrap();
        wg.publish_workspace(&notes).unwrap();

        assert_eq!(wg.workspaces().unwrap().len(), 1);
    }

    #[test]
    fn republishing_with_a_new_name_updates_that_file_only() {
        let (_tmp, _dir, wg) = workgroup();
        let notes = entry("sapphire-journal", "notes");
        let books = entry("sapphire-ledger", "books");
        wg.publish_workspace(&notes).unwrap();
        wg.publish_workspace(&books).unwrap();

        let renamed = WorkgroupWorkspace { name: "journal".into(), ..notes.clone() };
        wg.publish_workspace(&renamed).unwrap();

        let listed = wg.workspaces().unwrap();
        assert_eq!(listed.len(), 2);
        let found = listed.iter().find(|w| w.workspace_id == notes.workspace_id).unwrap();
        assert_eq!(found.name, "journal");
        let other = listed.iter().find(|w| w.workspace_id == books.workspace_id).unwrap();
        assert_eq!(other.name, "books", "the other file must not have been touched");
    }

    #[test]
    fn a_file_whose_name_is_not_a_grain_id_is_refused() {
        let (_tmp, _dir, wg) = workgroup();
        let listing = wg.dir.join("root").join("workspaces");
        std::fs::create_dir_all(&listing).unwrap();
        std::fs::write(listing.join("not-an-id!.toml"), "app_name = \"x\"\nname = \"y\"\n")
            .unwrap();

        let err = wg.workspaces().unwrap_err();
        assert!(err.to_string().contains("not-an-id!"), "{err}");
    }

    #[test]
    fn an_empty_workgroup_lists_nothing() {
        let (_tmp, _dir, wg) = workgroup();
        assert!(wg.workspaces().unwrap().is_empty());
    }
}
```

And in `tests/switchboard.rs`, using the fixtures from `tests/common/mod.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn registering_a_workspace_publishes_it_to_the_workgroup() {
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let client = common::connect(&a).await;

    let ws = grain_id::GrainId::random();
    client
        .register(RegisterParams {
            app_name: "sapphire-journal".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration { workspace_id: ws, root: "/a/notes".into() }],
        })
        .await
        .unwrap();

    let wg = Workgroup::open(&BridgeDir::at(a.tmp.path().join("bridge")).unwrap())
        .unwrap()
        .expect("a workgroup");
    let listed = wg.workspaces().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].workspace_id, ws);
    assert_eq!(listed[0].app_name, "sapphire-journal");
    assert_eq!(
        listed[0].name, "notes",
        "the published name defaults to the root directory's name"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workspace_the_other_host_published_appears_in_workspace_list() {
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start(&net, common::NODE_B, "host-b").await;
    common::introduce_both(&a, &b);

    // A publishes; the workgroup workspace replicates to B.
    let client_a = common::connect(&a).await;
    let ws = grain_id::GrainId::random();
    client_a
        .register(RegisterParams {
            app_name: "sapphire-journal".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration { workspace_id: ws, root: "/a/notes".into() }],
        })
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let wg_b = Workgroup::open(&BridgeDir::at(b.tmp.path().join("bridge")).unwrap())
            .unwrap()
            .expect("a workgroup");
        if wg_b.workspaces().unwrap().iter().any(|w| w.workspace_id == ws) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "B never learned about A's workspace"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

`common::introduce_both` is `introduce` applied in both directions plus the `adopt_workgroup`
of Task 1, so the two hosts share a workgroup id.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features workspace_list`
Expected: FAIL — `WorkgroupWorkspace` does not exist.

- [ ] **Step 3: Implement the list**

```rust
/// One workspace the workgroup knows about.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WorkgroupWorkspace {
    /// Its identity across devices. Carried by the file name, not repeated inside.
    #[serde(skip)]
    pub workspace_id: GrainId,
    /// Which application owns it.
    pub app_name: String,
    /// A human-chosen name, used as a selector.
    pub name: String,
}

impl Workgroup {
    fn workspaces_dir(&self) -> PathBuf {
        self.dir.join("root").join("workspaces")
    }

    /// Every workspace the workgroup knows about.
    pub fn workspaces(&self) -> Result<Vec<WorkgroupWorkspace>> {
        let dir = self.workspaces_dir();
        let mut out = Vec::new();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(Error::Io(e)),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(stem) = name.strip_suffix(".toml") else { continue };
            let workspace_id: GrainId = stem.parse().map_err(|_| {
                Error::Config(format!(
                    "{}: the file name is not a grain-id",
                    entry.path().display()
                ))
            })?;
            let text = std::fs::read_to_string(entry.path())?;
            let mut parsed: WorkgroupWorkspace = toml::from_str(&text)
                .map_err(|e| Error::Config(format!("{}: {e}", entry.path().display())))?;
            parsed.workspace_id = workspace_id;
            out.push(parsed);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Announce a workspace to the workgroup, or update its entry.
    ///
    /// One file per workspace, for the same reason as one file per device: two hosts
    /// publishing at the same moment write different files instead of contending for one.
    pub fn publish_workspace(&self, ws: &WorkgroupWorkspace) -> Result<()> {
        let dir = self.workspaces_dir();
        std::fs::create_dir_all(&dir)?;
        let body = toml::to_string_pretty(ws).map_err(|e| Error::Config(e.to_string()))?;
        crate::routes::write_atomic(
            &dir.join(format!("{}.toml", ws.workspace_id)),
            "# A workspace of this workgroup. The file name is its id.\n",
            &body,
        )
    }
}
```

In `control.rs`, `bridge.register` publishes each workspace it records, with `name` defaulting
to the root directory's file name.

- [ ] **Step 4: Run the tests to verify they pass, then commit**

```bash
cargo test -p sapphire-framework-bridge --all-features
git commit -m "feat(bridge): publish each synced workspace to its workgroup"
```

---

### Task 6: The CLI

**Files:**
- Modify: `crates/sapphire-framework-bridge/src/command.rs`
- Modify: `crates/sapphire-framework-server/src/sync/methods.rs` (the app-side `sync.map`)
- Modify: `docs/superpowers/plans/2026-09-16-bridge-basics-plan.md` (its CLI table)
- Test: inline `#[cfg(test)] mod tests` in `command.rs`

**Interfaces:**
- `DeviceCommand::{List, Invite { name, ttl, workgroup }, Forget { selector }}`
- `WorkgroupCommand::{Create { name, device_name }, Join { ticket, device_name }, List}`
- `WorkspaceCommand::List` — unchanged, still read-only
- In `-server`: `SYNC_MAP: &str = "sync.map"`, `SyncMapParams { workspace: String, dir: PathBuf }`

**Naming follows the sync spec §3.6**, not the placeholder list in the bridge plan's Task 9:
`device invite` creates a ticket, `workgroup join <ticket>` uses it. Update that plan's CLI
table in the same commit, so the two do not disagree.

**`workspace map` stays out of the bridge.** Placing a workspace on this host is the owning
application's business (process-architecture spec §1): `journal sync map <name|id> <dir>`
creates the directory and its marker, writes the `sync-id` the workgroup lists, and enables
sync. That is `sync.map` on the app server, not a bridge subcommand.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod pairing_cli_tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(subcommand)]
        command: BridgeCommand,
    }

    #[test]
    fn the_pairing_subcommands_parse() {
        for args in [
            vec!["b", "device", "invite", "--name", "phone"],
            vec!["b", "device", "invite", "--name", "phone", "--ttl", "300"],
            vec!["b", "workgroup", "join", "sapphire:ABCDEF"],
            vec!["b", "workgroup", "join", "sapphire:ABCDEF", "--device-name", "phone"],
        ] {
            assert!(Probe::try_parse_from(&args).is_ok(), "{args:?}");
        }
    }

    #[test]
    fn an_invite_needs_a_name() {
        assert!(Probe::try_parse_from(["b", "device", "invite"]).is_err());
    }

    #[test]
    fn a_join_needs_a_ticket() {
        assert!(Probe::try_parse_from(["b", "workgroup", "join"]).is_err());
    }

    #[test]
    fn mapping_a_workspace_is_still_not_a_bridge_command() {
        // Placing a workspace on this host is the owning application's business.
        assert!(Probe::try_parse_from(["b", "workspace", "map", "notes", "/tmp/x"]).is_err());
    }

    #[tokio::test]
    async fn joining_without_a_running_bridge_says_so_rather_than_starting_one() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: the test binary sets these before any other thread reads them.
        unsafe {
            std::env::set_var("SAPPHIRE_RUNTIME_DIR", tmp.path());
            std::env::set_var(crate::dir::BRIDGE_DIR_ENV, tmp.path().join("bridge"));
        }
        let result = BridgeCommand::Workgroup(WorkgroupCommand::Join {
            ticket: "sapphire:ABCDEF".into(),
            device_name: Some("phone".into()),
        })
        .dispatch("0.0.0")
        .await;
        unsafe {
            std::env::remove_var("SAPPHIRE_RUNTIME_DIR");
            std::env::remove_var(crate::dir::BRIDGE_DIR_ENV);
        }

        match result {
            Ok(code) => assert_eq!(code, 1, "a missing bridge is a non-zero exit, not a panic"),
            Err(err) => assert!(
                !err.to_string().contains("panic"),
                "it must fail with a message, not a panic: {err}"
            ),
        }
    }

    #[test]
    fn a_ttl_is_read_as_seconds() {
        let parsed = Probe::try_parse_from(["b", "device", "invite", "--name", "p", "--ttl", "90"])
            .unwrap();
        match parsed.command {
            BridgeCommand::Device(DeviceCommand::Invite { ttl, .. }) => {
                assert_eq!(ttl, Some(90));
            }
            other => panic!("{other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-bridge --all-features pairing_cli`
Expected: FAIL — the subcommands do not exist.

- [ ] **Step 3: Implement the commands**

`device invite` asks the running bridge to create one and prints the ticket on its own line,
with nothing else on that line, so it can be piped. `workgroup join` parses the ticket, checks
the host is not already in a workgroup, and runs `Workgroup::join`. `workgroup list` and
`workspace list` print what the workgroup holds.

Both use `SpawnConfig::disabled()`: asking a bridge to do something must not start one.

- [ ] **Step 4: Run the tests to verify they pass, then commit**

```bash
cargo test -p sapphire-framework-bridge -p sapphire-framework-server --all-features
git add crates docs/superpowers/plans/2026-09-16-bridge-basics-plan.md
git commit -m "feat(bridge): add device invite and workgroup join"
```

---

### Task 7: End to end — two hosts pair and converge

**Files:**
- Create: `crates/sapphire-framework-bridge/tests/pairing_e2e.rs`

- [ ] **Step 1: Write the tests**

```rust
//! A second host joins a workgroup and starts syncing.

mod common;

use sapphire_framework_bridge::{BridgeDir, LoopbackNetwork, Workgroup};

/// Invite `joiner_name` from `a`, and join from a fresh host on `net`.
///
/// Returns the joiner's bridge directory and the workgroup it ended up in.
async fn pair_in(
    net: &LoopbackNetwork,
    a: &common::Host,
    node: &str,
    joiner_name: &str,
) -> (tempfile::TempDir, BridgeDir, Workgroup) {
    let ticket = common::invite(a, joiner_name).await;
    let tmp = tempfile::tempdir().unwrap();
    let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
    let wg = Workgroup::join(&dir, &ticket, joiner_name, &net.transport(node))
        .await
        .expect("the join");
    (tmp, dir, wg)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_joined_device_appears_on_the_inviter() {
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let (_tmp, _dir, _wg) = pair_in(&net, &a, common::NODE_B, "phone").await;

    let wg_a = Workgroup::open(&BridgeDir::at(a.tmp.path().join("bridge")).unwrap())
        .unwrap()
        .expect("a workgroup");
    let devices = wg_a.devices().unwrap();
    let phone = devices.by_node_id(common::NODE_B).expect("the joiner's record");
    assert_eq!(phone.name, "phone");
    assert!(!phone.is_retired());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_joined_device_can_open_a_sync_stream() {
    // The whole point: after pairing, the ordinary authorization path admits it.
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let (_tmp, _dir, wg_b) = pair_in(&net, &a, common::NODE_B, "phone").await;

    let wg_a = Workgroup::open(&BridgeDir::at(a.tmp.path().join("bridge")).unwrap())
        .unwrap()
        .unwrap();
    assert!(
        wg_a.authorize(common::NODE_B).is_ok(),
        "a paired device must pass the normal check"
    );
    assert_eq!(wg_b.id, wg_a.id);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_third_device_learns_about_the_second_through_the_workgroup_workspace() {
    // A invites B, then A invites C. C never spoke to B, but the ledger replicates, so C
    // admits B's connection. Without this, a workgroup of four devices would need six
    // pairings instead of three.
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let (_tb, dir_b, _wg_b) = pair_in(&net, &a, common::NODE_B, "phone").await;
    let (_tc, dir_c, _wg_c) = pair_in(&net, &a, common::NODE_C, "tablet").await;

    // Let the workgroup workspace reach C.
    common::sync_workgroup(&a, &dir_c, &net).await;

    let wg_c = Workgroup::open(&dir_c).unwrap().unwrap();
    assert!(
        wg_c.authorize(common::NODE_B).is_ok(),
        "C must admit B without ever having paired with it"
    );
    let _ = dir_b;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retired_device_stops_being_admitted_everywhere() {
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let (_tb, _dir_b, _) = pair_in(&net, &a, common::NODE_B, "phone").await;
    let (_tc, dir_c, _) = pair_in(&net, &a, common::NODE_C, "tablet").await;
    common::sync_workgroup(&a, &dir_c, &net).await;
    assert!(Workgroup::open(&dir_c).unwrap().unwrap().authorize(common::NODE_B).is_ok());

    // A retires the phone, and the change reaches C.
    let wg_a = Workgroup::open(&BridgeDir::at(a.tmp.path().join("bridge")).unwrap())
        .unwrap()
        .unwrap();
    wg_a.devices().unwrap().retire("phone").unwrap();
    common::sync_workgroup(&a, &dir_c, &net).await;

    let err = Workgroup::open(&dir_c).unwrap().unwrap().authorize(common::NODE_B).unwrap_err();
    assert!(err.to_string().contains("retired"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_device_that_was_never_invited_is_refused() {
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;

    let wg_a = Workgroup::open(&BridgeDir::at(a.tmp.path().join("bridge")).unwrap())
        .unwrap()
        .unwrap();
    let err = wg_a.authorize(common::NODE_B).unwrap_err();
    assert!(err.to_string().contains("not a device of this workgroup"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_that_was_already_used_does_not_admit_a_second_device() {
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let ticket = common::invite(&a, "phone").await;

    let tmp_b = tempfile::tempdir().unwrap();
    let dir_b = BridgeDir::at(tmp_b.path().join("bridge")).unwrap();
    Workgroup::join(&dir_b, &ticket, "phone", &net.transport(common::NODE_B))
        .await
        .expect("the first join");

    let tmp_c = tempfile::tempdir().unwrap();
    let dir_c = BridgeDir::at(tmp_c.path().join("bridge")).unwrap();
    assert!(
        Workgroup::join(&dir_c, &ticket, "intruder", &net.transport(common::NODE_C))
            .await
            .is_err(),
        "a ticket is good once"
    );
}
```

Add `NODE_C`, `invite` and `sync_workgroup` to `tests/common/mod.rs`. `sync_workgroup` runs one
`WorkgroupReplica` session between two hosts' workgroup replicas over a duplex, which is what
the running bridges would do on their own.

- [ ] **Step 2: Run the tests**

Run: `cargo test -p sapphire-framework-bridge --all-features --test pairing_e2e`
Expected: PASS, 6 tests.

- [ ] **Step 3: Run everything and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-bridge
git commit -m "test(bridge): pair a second device and let the workgroup carry the rest"
```


## What this plan does not cover

| | Left for |
|---|---|
| Live propagation — sessions staying open and pushing commits (sync spec §4.2) | step 9 |
| The embedded relay and relay URLs published to the workgroup | step 9 |
| `status.json`, the shared log and `bridge log` | step 9 |
| More than one workgroup per host | supported by the layout; the CLI limit lifts when someone needs it |
| Rotating a device's `node_id` after a reinstall | it rejoins as a new device; merging the two records is a question nobody has asked yet |
