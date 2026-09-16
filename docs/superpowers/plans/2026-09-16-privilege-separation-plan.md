# Privilege Separation (`sapphire-framework-server`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Let an app server start as root purely in order to become two different users — the
human user for the workspace, the cache and the sockets, and a second, lower-privileged user
for the shell and generic file tools it runs on behalf of an agent — and then never be root
again.

**Architecture:** One function, `privilege::apply`, runs the whole sequence: resolve both
users, hand the directories to the human user, fork the helper as the agent user over a
socketpair, drop permanently, and verify that the drop cannot be undone. Order is forced:
forking as a second user needs root, and binding a socket must happen after the drop or the
socket belongs to root. Nothing survives as root, because only one extra identity is ever
needed and one fork before the drop supplies it.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), libc 0.2, tokio 1 (`net`), serde, thiserror 2,
tracing. Unix only.

**Spec:** `docs/superpowers/specs/2026-09-16-process-architecture-design.md` — §3 in full,
plus §2.6's rule that start-on-demand is disabled for a privilege-separated app.
Implementation order step 5 of that spec's §9.

**Depends on:** `docs/superpowers/plans/2026-09-16-app-server-plan.md` (step 4) must be
complete. This plan adds a module to `sapphire-framework-server` and uses
`sapphire_ipc::runtime_dir`.

**Motivating issue:** `sapphire-agent` #257 — shell and generic fs tools must keep their
freedom while losing access to workspace files, because otherwise an agent can read another
memory namespace or edit the heartbeat configuration that constrains it.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task. Commit
  `Cargo.lock` whenever dependencies change.
- Every public item carries a doc comment.
- **Unix only.** The whole module is `#[cfg(unix)]`; on Windows, a configuration that asks for
  privilege separation fails with a clear error rather than being ignored.
- **Every directory and file the framework creates is `0700` / `0600`** (spec §3.2). These
  modes are what actually keeps the helper user out of the workspace, the cache and the keys,
  so the rule admits no exceptions.
- Neither `run_as` nor `helper_as` may resolve to uid 0. A helper running as root would defeat
  the entire arrangement, and a `run_as` of root means nothing was dropped.
- **No root process survives `apply`.** Do not add a lingering privileged supervisor: one
  extra identity is all that is ever needed, so one fork before the drop is enough.

## Why the sequence is in this order

```
1. resolve run_as and helper_as (uid, gid, supplementary groups)
2. hand the cache, data, config and runtime directories to run_as, mode 0700
3. create a socketpair
4. fork/exec the helper as helper_as        <- impossible after step 5
5. drop permanently to run_as               <- verified, not assumed
6. bind the IPC socket                      <- must be after 5, or root owns it
7. connect to the bridge                    <- after 5, so it is the user's own bridge
```

Steps 4 and 6 sit on opposite sides of step 5 and cannot move. Sharing the human user's
bridge needs no bridge-side support at all *because* step 7 comes after step 5: by then the
process is an ordinary process of that user.

`setgroups` runs before `setgid`, which runs before `setuid`. Reversed, the supplementary
groups of root survive the drop — the classic form of this bug.

## File Structure

```
crates/sapphire-framework-server/
    Cargo.toml                 # MODIFIED: libc on unix; tokio "net"
    src/
        lib.rs                 # MODIFIED: mod privilege; re-exports
        privilege/
            mod.rs             # PrivilegeConfig, UserSpec, apply(), the Windows stub
            users.rs           # #[cfg(unix)] resolving a user to uid/gid
            drop.rs            # #[cfg(unix)] the drop and its verification
            helper.rs          # #[cfg(unix)] socketpair + fork/exec as another user
    tests/
        privilege_root.rs      # #[ignore] tests that need root
.github/workflows/ci.yml       # MODIFIED: a job that runs the ignored tests as root
```

---

### Task 1: `UserSpec` and resolution

**Files:**
- Create: `crates/sapphire-framework-server/src/privilege/mod.rs`
- Create: `crates/sapphire-framework-server/src/privilege/users.rs`
- Modify: `crates/sapphire-framework-server/Cargo.toml`
- Modify: `crates/sapphire-framework-server/src/{lib.rs,error.rs}`
- Test: inline `#[cfg(test)] mod tests` in `users.rs`

**Interfaces:**
- Produces:
  - `UserSpec::{Name(String), Uid(u32)}` with `FromStr`, `Display`, `Serialize`,
    `Deserialize` (both forms parse from a string: `"agent"` or `"1001"`)
  - `HelperSpec { user: UserSpec, program: PathBuf, args: Vec<String> }` (`Deserialize`)
  - `PrivilegeConfig { run_as: UserSpec, helper: Option<HelperSpec> }` (`Deserialize`)
  - `ResolvedUser { uid: u32, gid: u32, name: String }`
  - `#[cfg(unix)] fn resolve(spec: &UserSpec) -> Result<ResolvedUser>`
  - `#[cfg(unix)] fn current_uid() -> u32`
  - `Error::Privilege(String)` added to the server error enum

- [ ] **Step 1: Add the dependency**

`crates/sapphire-framework-server/Cargo.toml`:

```toml
[target.'cfg(unix)'.dependencies]
libc = "0.2"
```

and add `"net"` to the existing `tokio` feature list, for the helper socketpair.

- [ ] **Step 2: Extend the error type**

`crates/sapphire-framework-server/src/error.rs`, a new variant:

```rust
    /// Privilege separation could not be set up.
    ///
    /// Always fatal: a server that meant to drop privileges and did not must never go on to
    /// serve requests.
    #[error("privilege separation failed: {0}")]
    Privilege(String),
```

- [ ] **Step 3: Write the failing tests**

`crates/sapphire-framework-server/src/privilege/users.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_spec_parses_a_name_or_a_number() {
        assert_eq!("agent".parse::<UserSpec>().unwrap(), UserSpec::Name("agent".into()));
        assert_eq!("1001".parse::<UserSpec>().unwrap(), UserSpec::Uid(1001));
    }

    #[test]
    fn a_user_spec_round_trips_through_its_text_form() {
        for text in ["agent", "1001"] {
            let spec: UserSpec = text.parse().unwrap();
            assert_eq!(spec.to_string(), text);
        }
    }

    #[test]
    fn a_user_spec_deserialises_from_a_plain_string() {
        let spec: UserSpec = serde_json::from_value(serde_json::json!("agent")).unwrap();
        assert_eq!(spec, UserSpec::Name("agent".into()));
        let spec: UserSpec = serde_json::from_value(serde_json::json!("0")).unwrap();
        assert_eq!(spec, UserSpec::Uid(0));
    }

    #[test]
    fn an_empty_user_spec_is_refused() {
        assert!("".parse::<UserSpec>().is_err());
    }

    #[test]
    fn the_current_user_resolves_by_uid() {
        let resolved = resolve(&UserSpec::Uid(current_uid())).unwrap();
        assert_eq!(resolved.uid, current_uid());
        assert!(!resolved.name.is_empty());
    }

    #[test]
    fn the_current_user_resolves_by_name_to_the_same_uid() {
        let by_uid = resolve(&UserSpec::Uid(current_uid())).unwrap();
        let by_name = resolve(&UserSpec::Name(by_uid.name.clone())).unwrap();
        assert_eq!(by_name.uid, by_uid.uid);
        assert_eq!(by_name.gid, by_uid.gid);
    }

    #[test]
    fn an_unknown_user_is_an_error_not_a_panic() {
        let err = resolve(&UserSpec::Name("no-such-user-9f3a".into())).unwrap_err();
        assert!(err.to_string().contains("no-such-user-9f3a"), "{err}");
    }

    #[test]
    fn root_is_refused() {
        let err = resolve(&UserSpec::Uid(0)).unwrap_err();
        assert!(err.to_string().contains("root"), "{err}");
    }
}
```

`root_is_refused` states the rule at the only place it can be enforced once: neither identity
may be root, so resolution itself rejects uid 0 and no caller can forget to check.

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features privilege`
Expected: FAIL — the module does not exist.

- [ ] **Step 5: Implement the module skeleton**

`crates/sapphire-framework-server/src/privilege/mod.rs`:

```rust
//! Starting as root in order to become two different users, and then neither being root nor
//! able to become root again.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §3, and
//! `sapphire-agent` issue #257 for what this is for: an agent's shell and generic file tools
//! keep their freedom while losing access to the workspace.
//!
//! Unix only. On any other platform a configuration that asks for this fails.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[cfg(unix)]
mod drop;
#[cfg(unix)]
mod helper;
#[cfg(unix)]
mod users;

#[cfg(unix)]
pub use helper::HelperHandle;
#[cfg(unix)]
pub use users::{ResolvedUser, current_uid, resolve};

/// Which OS user to become, by name or by numeric id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserSpec {
    /// A login name, looked up in the password database.
    Name(String),
    /// A numeric user id.
    Uid(u32),
}

impl std::str::FromStr for UserSpec {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("a user must be named".to_owned());
        }
        // All digits is a uid; anything else is a name. A login name made entirely of digits
        // is legal on some systems and unreachable here — say so rather than guessing.
        match s.parse::<u32>() {
            Ok(uid) => Ok(UserSpec::Uid(uid)),
            Err(_) => Ok(UserSpec::Name(s.to_owned())),
        }
    }
}

impl std::fmt::Display for UserSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserSpec::Name(name) => f.write_str(name),
            UserSpec::Uid(uid) => write!(f, "{uid}"),
        }
    }
}

impl Serialize for UserSpec {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for UserSpec {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// The lower-privileged helper an application wants forked before the drop.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HelperSpec {
    /// The user the helper runs as. Must not be root, and should not be `run_as` either —
    /// a helper with the same identity separates nothing.
    pub user: UserSpec,
    /// The program to run.
    pub program: PathBuf,
    /// Its arguments.
    #[serde(default)]
    pub args: Vec<String>,
}

/// What an application wants privilege separation to do.
///
/// Deserialised from the application's configuration file. Its presence also tells the
/// application's CLI not to try starting the server itself (spec §2.6): a process running as
/// the human user cannot spawn a root one.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrivilegeConfig {
    /// The user that owns the workspace, the cache and the sockets.
    pub run_as: UserSpec,
    /// An optional helper forked before the drop, under a different user.
    #[serde(default)]
    pub helper: Option<HelperSpec>,
}
```

`crates/sapphire-framework-server/src/privilege/users.rs`:

```rust
//! Resolving a [`UserSpec`](super::UserSpec) against the password database.

use crate::error::{Error, Result};

use super::UserSpec;

/// A user, resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedUser {
    /// Numeric user id.
    pub uid: u32,
    /// The user's primary group.
    pub gid: u32,
    /// Login name, needed to look up supplementary groups.
    pub name: String,
}

/// This process's real user id.
pub fn current_uid() -> u32 {
    // SAFETY: getuid has no preconditions and always succeeds.
    unsafe { libc::getuid() }
}

/// Look `spec` up in the password database.
///
/// Refuses root: neither identity in privilege separation may be root, and enforcing it here
/// means no caller can forget.
pub fn resolve(spec: &UserSpec) -> Result<ResolvedUser> {
    let resolved = match spec {
        UserSpec::Uid(uid) => by_uid(*uid)?,
        UserSpec::Name(name) => by_name(name)?,
    };
    if resolved.uid == 0 {
        return Err(Error::Privilege(format!(
            "{spec} is root; privilege separation needs two non-root users"
        )));
    }
    Ok(resolved)
}

/// Run `f` with a `getpw*_r` buffer, growing it while the call asks for more room.
fn with_passwd<F>(describe: &str, mut f: F) -> Result<ResolvedUser>
where
    F: FnMut(*mut libc::passwd, *mut libc::c_char, usize, *mut *mut libc::passwd) -> libc::c_int,
{
    let mut size = match unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) } {
        n if n > 0 => n as usize,
        _ => 1024,
    };
    loop {
        let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut buf = vec![0 as libc::c_char; size];
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let rc = f(&raw mut passwd, buf.as_mut_ptr(), buf.len(), &raw mut result);
        if rc == libc::ERANGE && size < 1 << 20 {
            size *= 2;
            continue;
        }
        if rc != 0 {
            return Err(Error::Privilege(format!(
                "could not look up {describe}: {}",
                std::io::Error::from_raw_os_error(rc)
            )));
        }
        if result.is_null() {
            return Err(Error::Privilege(format!("no such user: {describe}")));
        }
        // SAFETY: `result` points at `passwd`, which the call filled in, and `pw_name`
        // points into `buf`, which is alive for this block.
        let name = unsafe { std::ffi::CStr::from_ptr(passwd.pw_name) }
            .to_string_lossy()
            .into_owned();
        return Ok(ResolvedUser {
            uid: passwd.pw_uid,
            gid: passwd.pw_gid,
            name,
        });
    }
}

fn by_uid(uid: u32) -> Result<ResolvedUser> {
    with_passwd(&uid.to_string(), |pw, buf, len, out| {
        // SAFETY: all pointers are valid for the sizes passed.
        unsafe { libc::getpwuid_r(uid, pw, buf, len, out) }
    })
}

fn by_name(name: &str) -> Result<ResolvedUser> {
    let c_name = std::ffi::CString::new(name)
        .map_err(|_| Error::Privilege(format!("a user name may not contain a NUL: {name:?}")))?;
    with_passwd(name, |pw, buf, len, out| {
        // SAFETY: `c_name` is NUL-terminated and outlives the call; the rest are valid.
        unsafe { libc::getpwnam_r(c_name.as_ptr(), pw, buf, len, out) }
    })
}
```

`crates/sapphire-framework-server/src/lib.rs`: add `pub mod privilege;` and
`pub use privilege::{HelperSpec, PrivilegeConfig, UserSpec};`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features privilege`
Expected: PASS, 8 tests.

Add `serde_json` to `[dev-dependencies]` if the deserialisation test does not compile — it is
already a normal dependency of this crate, so it should.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server Cargo.lock
git commit -m "feat(server): resolve the two users privilege separation needs"
```

---

### Task 2: The drop, and proving it stuck

**Files:**
- Create: `crates/sapphire-framework-server/src/privilege/drop.rs`
- Modify: `crates/sapphire-framework-server/src/privilege/mod.rs`
- Test: inline `#[cfg(test)] mod tests` in `drop.rs`

**Interfaces:**
- Consumes: `ResolvedUser`, `current_uid` (Task 1)
- Produces:
  - `#[cfg(unix)] fn drop_to(user: &ResolvedUser) -> Result<()>` — `setgroups` via
    `initgroups`, then `setgid`, then `setuid`, then verification
  - `#[cfg(unix)] fn is_root() -> bool`
  - `#[cfg(unix)] fn hand_over(dirs: &[&Path], user: &ResolvedUser) -> Result<()>` —
    recursive `chown` to `user` plus `0700` / `0600` modes

**Verification is not optional.** After the drop, `getuid` and `geteuid` must both be the
target, and `setuid(0)` must fail. A drop that silently did not happen is worse than no drop
at all, because everything downstream assumes it worked.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/src/privilege/drop.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_non_root_process_is_not_root() {
        if is_root() {
            // The root CI job covers the other side; here we only assert the predicate
            // agrees with the process it is describing.
            assert_eq!(super::super::current_uid(), 0);
        } else {
            assert_ne!(super::super::current_uid(), 0);
        }
    }

    #[test]
    fn handing_over_sets_private_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cache");
        std::fs::create_dir_all(dir.join("inner")).unwrap();
        std::fs::write(dir.join("inner").join("a.redb"), b"x").unwrap();
        // Start from something permissive so the change is visible.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(
            dir.join("inner").join("a.redb"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();

        let me = super::super::resolve(&super::super::UserSpec::Uid(
            super::super::current_uid(),
        ))
        .unwrap();
        hand_over(&[dir.as_path()], &me).unwrap();

        let mode = |p: &std::path::Path| {
            std::fs::metadata(p).unwrap().permissions().mode() & 0o777
        };
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&dir.join("inner")), 0o700);
        assert_eq!(mode(&dir.join("inner").join("a.redb")), 0o600);
    }

    #[test]
    fn handing_over_a_missing_directory_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let me = super::super::resolve(&super::super::UserSpec::Uid(
            super::super::current_uid(),
        ))
        .unwrap();
        hand_over(&[tmp.path().join("absent").as_path()], &me).unwrap();
    }

    #[test]
    fn dropping_to_the_current_user_from_a_non_root_process_succeeds() {
        if is_root() {
            return; // covered by the root job
        }
        let me = super::super::resolve(&super::super::UserSpec::Uid(
            super::super::current_uid(),
        ))
        .unwrap();
        // Dropping to who we already are must be a no-op, not a failure: that is the
        // path a server takes when it is started without privilege separation.
        drop_to(&me).unwrap();
        assert_eq!(super::super::current_uid(), me.uid);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features privilege::drop`
Expected: FAIL — `drop_to` does not exist.

- [ ] **Step 3: Implement the drop**

`crates/sapphire-framework-server/src/privilege/drop.rs`:

```rust
//! Handing the directories over, and dropping privileges for good.

use std::path::Path;

use crate::error::{Error, Result};

use super::users::ResolvedUser;

/// Is this process running as root?
pub fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() } == 0
}

/// Give `dirs` to `user`, with `0700` on directories and `0600` on files, recursively.
///
/// Called while still root, because a chown to another user needs it. A directory that does
/// not exist is skipped — the caller lists every directory the application might use, and not
/// all of them exist on a first run.
pub fn hand_over(dirs: &[&Path], user: &ResolvedUser) -> Result<()> {
    for dir in dirs {
        if !dir.exists() {
            continue;
        }
        hand_over_one(dir, user)?;
    }
    Ok(())
}

fn hand_over_one(path: &Path, user: &ResolvedUser) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| Error::Privilege(format!("{}: {e}", path.display())))?;

    // Never follow a symlink while chowning as root: a link planted in a cache directory
    // would otherwise let its owner point this at a file they do not own.
    if meta.file_type().is_symlink() {
        tracing::warn!(path = %path.display(), "skipping a symlink while handing over directories");
        return Ok(());
    }

    chown(path, user)?;
    let mode = if meta.is_dir() { 0o700 } else { 0o600 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|e| Error::Privilege(format!("{}: {e}", path.display())))?;

    if meta.is_dir() {
        let entries = std::fs::read_dir(path)
            .map_err(|e| Error::Privilege(format!("{}: {e}", path.display())))?;
        for entry in entries {
            let entry = entry.map_err(|e| Error::Privilege(format!("{}: {e}", path.display())))?;
            hand_over_one(&entry.path(), user)?;
        }
    }
    Ok(())
}

fn chown(path: &Path, user: &ResolvedUser) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| Error::Privilege(format!("{} contains a NUL", path.display())))?;
    // SAFETY: `c_path` is NUL-terminated and alive for the call. `lchown` does not follow
    // symlinks, which matters while we are still root.
    let rc = unsafe { libc::lchown(c_path.as_ptr(), user.uid, user.gid) };
    if rc != 0 {
        return Err(Error::Privilege(format!(
            "could not give {} to {}: {}",
            path.display(),
            user.name,
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// Become `user`, permanently.
///
/// `setgroups` (through `initgroups`) must come first: after `setuid` it is no longer
/// permitted, and root's supplementary groups would survive the drop.
///
/// Dropping to the user this process already is succeeds and changes nothing, which is the
/// path a server started without privilege separation takes.
pub fn drop_to(user: &ResolvedUser) -> Result<()> {
    if super::current_uid() == user.uid && !is_root() {
        return Ok(());
    }

    let c_name = std::ffi::CString::new(user.name.as_str())
        .map_err(|_| Error::Privilege(format!("{} contains a NUL", user.name)))?;

    // SAFETY: `c_name` is NUL-terminated and alive for the call.
    if unsafe { libc::initgroups(c_name.as_ptr(), user.gid as _) } != 0 {
        return Err(Error::Privilege(format!(
            "setgroups for {} failed: {}",
            user.name,
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: setgid has no preconditions beyond privilege, which is checked by the result.
    if unsafe { libc::setgid(user.gid) } != 0 {
        return Err(Error::Privilege(format!(
            "setgid({}) failed: {}",
            user.gid,
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: as above. Called from root, setuid sets the real, effective and saved ids, so
    // there is no saved id left to escalate back to.
    if unsafe { libc::setuid(user.uid) } != 0 {
        return Err(Error::Privilege(format!(
            "setuid({}) failed: {}",
            user.uid,
            std::io::Error::last_os_error()
        )));
    }

    verify(user)
}

/// Prove the drop happened and cannot be undone.
fn verify(user: &ResolvedUser) -> Result<()> {
    // SAFETY: neither call has preconditions.
    let (uid, euid) = unsafe { (libc::getuid(), libc::geteuid()) };
    if uid != user.uid || euid != user.uid {
        return Err(Error::Privilege(format!(
            "the drop did not take: uid {uid}, euid {euid}, expected {}",
            user.uid
        )));
    }
    if user.uid != 0 {
        // SAFETY: setuid has no preconditions; we require it to fail.
        let escalated = unsafe { libc::setuid(0) } == 0;
        if escalated {
            return Err(Error::Privilege(
                "privileges can still be regained after the drop".to_owned(),
            ));
        }
    }
    Ok(())
}
```

`privilege/mod.rs`: add `pub use drop::{drop_to, hand_over, is_root};`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features privilege`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): drop privileges permanently and verify the drop"
```

---

### Task 3: The helper

**Files:**
- Create: `crates/sapphire-framework-server/src/privilege/helper.rs`
- Modify: `crates/sapphire-framework-server/src/privilege/mod.rs`
- Test: inline `#[cfg(test)] mod tests` in `helper.rs`

**Interfaces:**
- Consumes: `ResolvedUser` (Task 1)
- Produces:
  - `HelperHandle { pid: u32, user: ResolvedUser, socket: tokio::net::UnixStream }`
  - `#[cfg(unix)] fn spawn_helper(spec: &HelperSpec, user: &ResolvedUser) -> Result<HelperHandle>`

**The contract, which the framework does not extend:** the helper is handed one connected
socket on **file descriptor 0**, and it reads and writes on it. What is said over that socket
— "run this command", and the answer — is the application's design. `sapphire-agent` defines
its own protocol there; this crate never looks at the bytes.

Using fd 0 rather than fd 3 keeps the fork simple: `Stdio::from(UnixStream)` already places a
socket there, so there is no `dup2` and no `FD_CLOEXEC` juggling in the `pre_exec` hook, where
only async-signal-safe calls are allowed. The helper's `stderr` is inherited so its logging
still reaches the service's journal.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/src/privilege/helper.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn me() -> ResolvedUser {
        super::super::resolve(&super::super::UserSpec::Uid(super::super::current_uid())).unwrap()
    }

    /// Spawning a helper as *our own* user needs no privileges, so this runs everywhere and
    /// covers the socket plumbing. The root job covers becoming a different user.
    #[tokio::test]
    async fn the_helper_gets_a_connected_socket_on_stdin() {
        let spec = HelperSpec {
            user: super::super::UserSpec::Uid(super::super::current_uid()),
            // `cat` copies stdin to stdout; with a socket on fd 0 and stdout redirected to
            // the same socket by the shell, it echoes.
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "cat <&0 >&0".into()],
        };
        let mut handle = spawn_helper(&spec, &me()).unwrap();
        assert!(handle.pid > 0);

        handle.socket.write_all(b"ping\n").await.unwrap();
        let mut buf = [0u8; 5];
        handle.socket.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping\n");
    }

    #[tokio::test]
    async fn the_parent_sees_end_of_file_when_the_helper_exits() {
        let spec = HelperSpec {
            user: super::super::UserSpec::Uid(super::super::current_uid()),
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "exit 0".into()],
        };
        let mut handle = spawn_helper(&spec, &me()).unwrap();

        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.socket.read(&mut buf),
        )
        .await
        .expect("the helper must exit promptly")
        .unwrap();
        assert_eq!(read, 0, "an exited helper must show up as EOF");
    }

    #[tokio::test]
    async fn a_missing_program_is_an_error_not_a_hang() {
        let spec = HelperSpec {
            user: super::super::UserSpec::Uid(super::super::current_uid()),
            program: "/nonexistent/helper-9f3a".into(),
            args: vec![],
        };
        let err = spawn_helper(&spec, &me()).unwrap_err();
        assert!(err.to_string().contains("helper-9f3a"), "{err}");
    }
}
```

`the_parent_sees_end_of_file_when_the_helper_exits` pins the spec's failure model: a helper
that dies shows up as EOF on the socketpair, reported to the application, and the framework
does not restart it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features privilege::helper`
Expected: FAIL — `spawn_helper` does not exist.

- [ ] **Step 3: Implement the helper fork**

`crates/sapphire-framework-server/src/privilege/helper.rs`:

```rust
//! Forking the lower-privileged helper, before the drop.

use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};

use crate::error::{Error, Result};

use super::HelperSpec;
use super::users::ResolvedUser;

/// A running helper.
pub struct HelperHandle {
    /// The helper's process id.
    pub pid: u32,
    /// The user it runs as.
    pub user: ResolvedUser,
    /// The parent end of the socketpair. The helper has the other end on its fd 0.
    ///
    /// What travels over it is the application's protocol; this crate never reads it. A
    /// helper that exits closes it, which the application sees as end of file.
    pub socket: tokio::net::UnixStream,
}

impl std::fmt::Debug for HelperHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelperHandle")
            .field("pid", &self.pid)
            .field("user", &self.user.name)
            .finish()
    }
}

/// Fork `spec.program` as `user`, connected by a socketpair.
///
/// Must be called while still root when `user` differs from the current one. The child does
/// `setgroups` / `setgid` / `setuid` in its `pre_exec` hook, where only async-signal-safe
/// calls are allowed — which is why the identity change is three libc calls and nothing else.
pub fn spawn_helper(spec: &HelperSpec, user: &ResolvedUser) -> Result<HelperHandle> {
    let (parent, child) = socketpair()?;

    let name = std::ffi::CString::new(user.name.as_str())
        .map_err(|_| Error::Privilege(format!("{} contains a NUL", user.name)))?;
    let (uid, gid) = (user.uid, user.gid);

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .stdin(Stdio::from(child))
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        // A minimal environment: the helper is the less trusted half, and inheriting the
        // service's variables would hand it configuration it has no business reading.
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", format!("/home/{}", user.name))
        .env("USER", &user.name);

    // SAFETY: the closure runs between fork and exec, so it may call only async-signal-safe
    // functions. `initgroups`, `setgid` and `setuid` qualify; nothing here allocates.
    unsafe {
        command.pre_exec(move || {
            if libc::initgroups(name.as_ptr(), gid as _) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setgid(gid) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setuid(uid) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let child_process = command.spawn().map_err(|e| {
        Error::Privilege(format!("could not start the helper {}: {e}", spec.program.display()))
    })?;
    let pid = child_process.id();
    // The helper outlives this call; the application owns its lifetime through the socket.
    std::mem::forget(child_process);

    let parent = std::os::unix::net::UnixStream::from(parent);
    parent
        .set_nonblocking(true)
        .map_err(|e| Error::Privilege(format!("could not prepare the helper socket: {e}")))?;
    let socket = tokio::net::UnixStream::from_std(parent)
        .map_err(|e| Error::Privilege(format!("could not prepare the helper socket: {e}")))?;

    Ok(HelperHandle { pid, user: user.clone(), socket })
}

/// A connected pair of Unix stream sockets.
fn socketpair() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: `fds` is a valid array of two ints, which is what socketpair writes.
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(Error::Privilege(format!(
            "socketpair failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: both fds are freshly created and owned by nobody else.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}
```

`privilege/mod.rs`: add `pub use helper::{HelperHandle, spawn_helper};`.

> `std::mem::forget(child_process)` deliberately leaks the `Child` so Rust does not try to
> reap the helper. The application decides when the helper's life ends; a `Child` dropped
> here would leave a zombie nobody waits for. If the application wants to wait, it holds the
> pid.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features privilege::helper`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server
git commit -m "feat(server): fork the lower-privileged helper over a socketpair"
```

---

### Task 4: `apply` — the whole sequence

**Files:**
- Modify: `crates/sapphire-framework-server/src/privilege/mod.rs`
- Test: inline `#[cfg(test)] mod tests` in `privilege/mod.rs`

**Interfaces:**
- Consumes: Tasks 1–3
- Produces:
  - `Privileges { run_as: ResolvedUser, helper: Option<HelperHandle> }`
  - `fn apply(config: &PrivilegeConfig, dirs: &[&Path]) -> Result<Privileges>` — available on
    every platform; on non-Unix it always fails with a clear message

**Rules encoded here:**
- Started as **root**: run the full sequence.
- Started as a **non-root** user: succeed only if `run_as` is that user, and refuse a `helper`
  — the identity cannot be changed, so a helper would run with the server's own privileges,
  which separates nothing. The same binary therefore works with and without privilege
  separation, and without it an agent's shell tools run as the server's own user, as today.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-server/src/privilege/mod.rs`, at the bottom:

```rust
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn my_spec() -> UserSpec {
        UserSpec::Uid(current_uid())
    }

    #[test]
    fn a_non_root_process_may_run_as_itself() {
        if drop::is_root() {
            return; // the root job covers the privileged path
        }
        let tmp = tempfile::tempdir().unwrap();
        let config = PrivilegeConfig { run_as: my_spec(), helper: None };
        let privileges = apply(&config, &[tmp.path()]).unwrap();
        assert_eq!(privileges.run_as.uid, current_uid());
        assert!(privileges.helper.is_none());
    }

    #[test]
    fn a_non_root_process_may_not_run_as_someone_else() {
        if drop::is_root() {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        // uid 1 is `daemon` or `bin` on every Unix, and is never the test runner.
        let config = PrivilegeConfig { run_as: UserSpec::Uid(1), helper: None };
        let err = apply(&config, &[tmp.path()]).unwrap_err();
        assert!(err.to_string().contains("without root"), "{err}");
    }

    #[test]
    fn a_non_root_process_may_not_ask_for_a_helper() {
        if drop::is_root() {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let config = PrivilegeConfig {
            run_as: my_spec(),
            helper: Some(HelperSpec {
                user: my_spec(),
                program: "/bin/true".into(),
                args: vec![],
            }),
        };
        let err = apply(&config, &[tmp.path()]).unwrap_err();
        assert!(err.to_string().contains("helper"), "{err}");
    }

    #[test]
    fn running_as_root_is_refused_outright() {
        let tmp = tempfile::tempdir().unwrap();
        let config = PrivilegeConfig { run_as: UserSpec::Uid(0), helper: None };
        let err = apply(&config, &[tmp.path()]).unwrap_err();
        assert!(err.to_string().contains("root"), "{err}");
    }

    #[test]
    fn a_configuration_round_trips_through_toml() {
        let text = r#"
run_as = "alice"

[helper]
user = "sapphire-agent-tools"
program = "/usr/lib/sapphire-agent/tool-broker"
args = ["--quiet"]
"#;
        let config: PrivilegeConfig = toml::from_str(text).unwrap();
        assert_eq!(config.run_as, UserSpec::Name("alice".into()));
        let helper = config.helper.unwrap();
        assert_eq!(helper.user, UserSpec::Name("sapphire-agent-tools".into()));
        assert_eq!(helper.args, vec!["--quiet".to_owned()]);
    }
}
```

The TOML test needs `toml` in `[dev-dependencies]` of `sapphire-framework-server`:

```toml
toml.workspace = true
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features privilege`
Expected: FAIL — `apply` does not exist.

- [ ] **Step 3: Implement `apply`**

Append to `crates/sapphire-framework-server/src/privilege/mod.rs`:

```rust
use std::path::Path;

use crate::error::{Error, Result};

/// The result of applying a [`PrivilegeConfig`].
///
/// Defined on every platform so that calling code needs no `cfg`, even though on non-Unix
/// [`apply`] never returns one.
#[derive(Debug)]
pub struct Privileges {
    /// The user this process now is.
    #[cfg(unix)]
    pub run_as: ResolvedUser,
    /// The helper, if one was configured. Its socket is the application's to use.
    #[cfg(unix)]
    pub helper: Option<HelperHandle>,
}

/// Run the privilege-separation sequence of spec §3.1.
///
/// `dirs` are handed to `run_as` before the drop: pass every directory the application will
/// write to — the cache, data and config trees from `AppContext`, and
/// [`sapphire_ipc::runtime_dir`]. Directories that do not exist are skipped.
///
/// Call this **before** binding any socket and **before** connecting to the bridge. After it
/// returns, this process is an ordinary process of `run_as` and can do neither of those
/// things as root.
#[cfg(unix)]
pub fn apply(config: &PrivilegeConfig, dirs: &[&Path]) -> Result<Privileges> {
    let run_as = resolve(&config.run_as)?;

    if !drop::is_root() {
        // Without root there is no second identity to be had. Succeed only if the
        // configuration describes what is already true.
        if run_as.uid != current_uid() {
            return Err(Error::Privilege(format!(
                "cannot run as {} without root: this process is uid {}",
                config.run_as,
                current_uid()
            )));
        }
        if config.helper.is_some() {
            return Err(Error::Privilege(
                "a helper needs root: without it the helper would run with this server's own \
                 privileges, which separates nothing"
                    .to_owned(),
            ));
        }
        drop::hand_over(dirs, &run_as)?;
        return Ok(Privileges { run_as, helper: None });
    }

    // 2. The directories become the user's, while we can still chown.
    drop::hand_over(dirs, &run_as)?;

    // 3-4. The helper, while we can still become someone else.
    let helper = match &config.helper {
        Some(spec) => {
            let user = resolve(&spec.user)?;
            if user.uid == run_as.uid {
                return Err(Error::Privilege(format!(
                    "the helper user and run_as are both {}; a helper with the same identity \
                     separates nothing",
                    user.name
                )));
            }
            Some(helper::spawn_helper(spec, &user)?)
        }
        None => None,
    };

    // 5. The drop, verified.
    drop::drop_to(&run_as)?;
    tracing::info!(
        user = %run_as.name,
        helper = ?helper.as_ref().map(|h| (&h.user.name, h.pid)),
        "dropped privileges"
    );

    Ok(Privileges { run_as, helper })
}

/// Privilege separation is a Unix facility.
///
/// A configuration that asks for it on another platform fails here rather than being
/// silently ignored: an application that believes it is separated and is not is worse off
/// than one that knows it cannot be.
#[cfg(not(unix))]
pub fn apply(_config: &PrivilegeConfig, _dirs: &[&Path]) -> Result<Privileges> {
    Err(Error::Privilege(
        "privilege separation is not available on this platform".to_owned(),
    ))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features privilege`
Expected: PASS.

Run on Windows too — `cargo check -p sapphire-framework-server --all-features` must succeed,
with `apply` present and always failing.

- [ ] **Step 5: Document the calling order**

Add to the module documentation in `privilege/mod.rs`, after the first paragraph:

```rust
//! ```rust,ignore
//! static CTX: AppContext = AppContext::new("sapphire-agent");
//!
//! fn main() -> anyhow::Result<()> {
//!     CTX.init(AppKind::Server);
//!     let runtime = sapphire_ipc::runtime_dir()?;
//!
//!     // Everything below this line runs as the human user.
//!     let privileges = privilege::apply(
//!         &config.privileges,
//!         &[&runtime, CTX.cache_dir(), CTX.data_dir(), CTX.config_dir()],
//!     )?;
//!
//!     let tools = privileges.helper.map(|h| ToolBroker::new(h.socket));
//!     // … build and run the AppServer …
//!     Ok(())
//! }
//! ```
```

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server Cargo.lock
git commit -m "feat(server): run the privilege separation sequence"
```

---

### Task 5: Root tests and a CI job for them

**Files:**
- Create: `crates/sapphire-framework-server/tests/privilege_root.rs`
- Modify: `.github/workflows/ci.yml`

Everything so far is tested from an unprivileged process, which can prove the plumbing but
not the thing that matters: that a root process really becomes two different users and cannot
go back. These tests need root and therefore run in their own job.

- [ ] **Step 1: Write the tests**

`crates/sapphire-framework-server/tests/privilege_root.rs`:

```rust
//! Privilege separation, exercised as root.
//!
//! Marked `#[ignore]` so a normal `cargo test` skips them. CI runs them in a job that is
//! root and that creates the two users they need:
//!
//! ```sh
//! sudo useradd -m sapphire-human
//! sudo useradd -m sapphire-tools
//! sudo -E env "PATH=$PATH" cargo test -p sapphire-framework-server --all-features \
//!     --test privilege_root -- --ignored --test-threads=1
//! ```
//!
//! `--test-threads=1` is not a nicety: the drop is process-wide, so two of these running at
//! once would fight over the identity of the same process. Each test therefore forks a child
//! and does the drop there.

#![cfg(unix)]

use std::process::Command;

use sapphire_framework_server::privilege::{self, HelperSpec, PrivilegeConfig, UserSpec};

const HUMAN: &str = "sapphire-human";
const TOOLS: &str = "sapphire-tools";

fn require_root() {
    // SAFETY: geteuid has no preconditions.
    assert_eq!(unsafe { libc::geteuid() }, 0, "these tests must run as root");
}

/// Run `body` in a forked child and return its exit status, so a process-wide drop does not
/// affect the test runner.
fn in_child(body: impl FnOnce() -> i32) -> i32 {
    // SAFETY: the child calls only what `body` calls and then `_exit`; it never returns into
    // the test harness, so the usual post-fork restrictions are satisfied by construction as
    // long as `body` avoids allocating across threads. These bodies only call this crate.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        let code = body();
        // SAFETY: _exit is async-signal-safe and never returns.
        unsafe { libc::_exit(code) };
    }
    let mut status = 0;
    // SAFETY: `status` is a valid out-pointer.
    unsafe { libc::waitpid(pid, &raw mut status, 0) };
    libc::WEXITSTATUS(status)
}

#[test]
#[ignore = "needs root and the sapphire-human / sapphire-tools accounts"]
fn a_root_process_becomes_the_human_user_and_cannot_go_back() {
    require_root();
    let code = in_child(|| {
        let config = PrivilegeConfig {
            run_as: UserSpec::Name(HUMAN.into()),
            helper: None,
        };
        let tmp = std::path::Path::new("/tmp/sapphire-privsep-a");
        let _ = std::fs::create_dir_all(tmp);
        let Ok(privileges) = privilege::apply(&config, &[tmp]) else { return 10 };

        // SAFETY: none of these have preconditions.
        let (uid, euid) = unsafe { (libc::getuid(), libc::geteuid()) };
        if uid != privileges.run_as.uid || euid != privileges.run_as.uid {
            return 11;
        }
        // SAFETY: we require this to fail.
        if unsafe { libc::setuid(0) } == 0 {
            return 12;
        }
        0
    });
    assert_eq!(code, 0, "the drop or its verification failed (code {code})");
}

#[test]
#[ignore = "needs root and the sapphire-human / sapphire-tools accounts"]
fn the_workspace_is_unreadable_from_the_helper_user() {
    require_root();

    let dir = std::path::Path::new("/tmp/sapphire-privsep-b");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("secret.md"), "namespace contents").unwrap();

    let code = in_child(|| {
        let config = PrivilegeConfig {
            run_as: UserSpec::Name(HUMAN.into()),
            helper: None,
        };
        if privilege::apply(&config, &[dir]).is_err() {
            return 10;
        }
        0
    });
    assert_eq!(code, 0);

    // Now try to read it as the helper user.
    let status = Command::new("sudo")
        .args(["-u", TOOLS, "cat"])
        .arg(dir.join("secret.md"))
        .status()
        .expect("sudo");
    assert!(
        !status.success(),
        "the helper user could read the workspace; the 0700 rule is not holding"
    );
}

#[test]
#[ignore = "needs root and the sapphire-human / sapphire-tools accounts"]
fn the_helper_runs_as_the_other_user() {
    require_root();

    let out = std::path::Path::new("/tmp/sapphire-privsep-helper-uid");
    let _ = std::fs::remove_file(out);

    let code = in_child(|| {
        let config = PrivilegeConfig {
            run_as: UserSpec::Name(HUMAN.into()),
            helper: Some(HelperSpec {
                user: UserSpec::Name(TOOLS.into()),
                program: "/bin/sh".into(),
                args: vec![
                    "-c".into(),
                    format!("id -u > {} 2>/dev/null", out.display()),
                ],
            }),
        };
        let tmp = std::path::Path::new("/tmp/sapphire-privsep-c");
        let _ = std::fs::create_dir_all(tmp);
        let Ok(privileges) = privilege::apply(&config, &[tmp]) else { return 10 };
        if privileges.helper.is_none() {
            return 11;
        }
        // Give the helper a moment to write the file before the child exits.
        std::thread::sleep(std::time::Duration::from_millis(500));
        0
    });
    assert_eq!(code, 0, "apply failed (code {code})");

    let written = std::fs::read_to_string(out).expect("the helper must have run");
    let helper_uid: u32 = written.trim().parse().expect("a uid");
    let tools_uid = privilege::resolve(&UserSpec::Name(TOOLS.into())).unwrap().uid;
    assert_eq!(helper_uid, tools_uid, "the helper ran as the wrong user");
}

#[test]
#[ignore = "needs root and the sapphire-human / sapphire-tools accounts"]
fn a_socket_bound_after_the_drop_belongs_to_the_human_user() {
    require_root();
    use std::os::unix::fs::MetadataExt;

    let dir = std::path::Path::new("/tmp/sapphire-privsep-d");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let socket = dir.join("app.sock");

    let code = in_child(|| {
        let config = PrivilegeConfig {
            run_as: UserSpec::Name(HUMAN.into()),
            helper: None,
        };
        if privilege::apply(&config, &[dir]).is_err() {
            return 10;
        }
        // Binding creates the socket file. Dropping the listener does not remove it, so
        // the parent can inspect its ownership after the child exits.
        if std::os::unix::net::UnixListener::bind(&socket).is_err() {
            return 11;
        }
        0
    });
    assert_eq!(code, 0, "bind after the drop failed (code {code})");

    let human_uid = privilege::resolve(&UserSpec::Name(HUMAN.into())).unwrap().uid;
    assert_eq!(
        std::fs::metadata(&socket).unwrap().uid(),
        human_uid,
        "the socket must belong to the human user, not root"
    );
}
```

`libc` must be available to the integration test, so add it to `[dev-dependencies]` as well
as the unix target section:

```toml
[target.'cfg(unix)'.dev-dependencies]
libc = "0.2"
```

- [ ] **Step 2: Add the CI job**

`.github/workflows/ci.yml`, a new job alongside the existing one:

```yaml
  privileged:
    name: privilege separation (root)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Create the two test accounts
        run: |
          sudo useradd -m sapphire-human
          sudo useradd -m sapphire-tools
      - name: Print the toolchain
        run: cargo --version
      - name: Run the privileged tests
        run: |
          sudo -E env "PATH=$PATH" "CARGO_HOME=$CARGO_HOME" \
            cargo test -p sapphire-framework-server --all-features \
            --test privilege_root -- --ignored --test-threads=1
```

`sudo -E env "PATH=$PATH"` is needed because plain `sudo` resets the environment and would
not find `cargo`; `CARGO_HOME` is passed so the root build reuses the runner's registry cache
rather than downloading the index again.

- [ ] **Step 3: Verify**

Locally, if you have root and can create the accounts, run the command from the job. If you
cannot, push the branch and confirm the `privilege separation (root)` job is green before
treating this task as done. **Do not mark it complete on an unverified build** — these tests
are the only evidence the feature does what it claims.

Also confirm the normal job still passes, with the ignored tests skipped:

```bash
cargo test --all-features --locked
```

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-server .github/workflows/ci.yml Cargo.lock
git commit -m "test(server): prove privilege separation works, as root, in CI"
```

---

### Task 6: Disable start-on-demand for a privilege-separated app

**Files:**
- Modify: `crates/sapphire-framework-server/src/command.rs`
- Modify: `crates/sapphire-framework-server/src/lib.rs`
- Modify: `docs/ARCHITECTURE.md`
- Test: inline `#[cfg(test)] mod tests` in `command.rs`

**Interfaces:**
- Produces:
  - `fn spawn_config_for(privileges: Option<&PrivilegeConfig>) -> SpawnConfig` — the
    `SpawnConfig` an application's CLI should use, given its configuration

Spec §2.6: a CLI running as the human user cannot spawn a root process, so finding `run_as`
in the application's configuration it must report that the app runs as a privileged service
instead of trying and failing with a confusing error.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod spawn_policy_tests {
    use super::*;
    use crate::privilege::{PrivilegeConfig, UserSpec};

    #[test]
    fn an_ordinary_app_may_start_its_own_server() {
        assert!(spawn_config_for(None).allow_spawn);
    }

    #[test]
    fn a_privilege_separated_app_may_not() {
        let config = PrivilegeConfig { run_as: UserSpec::Name("alice".into()), helper: None };
        assert!(!spawn_config_for(Some(&config)).allow_spawn);
    }

    #[tokio::test]
    async fn the_error_says_to_start_the_service() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint =
            sapphire_ipc::Endpoint::in_dir("privsep-cli-test", tmp.path().to_path_buf());
        let config = PrivilegeConfig { run_as: UserSpec::Name("alice".into()), helper: None };

        let err = sapphire_ipc::ensure_server(
            &endpoint,
            "privsep-cli-test",
            sapphire_ipc::ClientInfo {
                kind: "cli".into(),
                version: "0.0.0".into(),
                pid: std::process::id(),
            },
            &spawn_config_for(Some(&config)),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not allowed to start one"), "{err}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-server --all-features spawn_policy`
Expected: FAIL — `spawn_config_for` does not exist.

- [ ] **Step 3: Implement it**

`crates/sapphire-framework-server/src/command.rs`:

```rust
use crate::privilege::PrivilegeConfig;

/// The [`SpawnConfig`] an application's CLI should use.
///
/// An application configured for privilege separation runs its server as root, and a CLI
/// running as the human user cannot start one (spec §2.6, §3). Saying so up front is much
/// clearer than letting the spawn fail somewhere inside the service manager's territory.
pub fn spawn_config_for(privileges: Option<&PrivilegeConfig>) -> SpawnConfig {
    match privileges {
        Some(_) => SpawnConfig::disabled(),
        None => SpawnConfig::default(),
    }
}
```

`lib.rs`: add `spawn_config_for` to the `command` re-exports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-server --all-features`
Expected: PASS.

- [ ] **Step 5: Note it in `ARCHITECTURE.md`**

In the crate table, extend the `sapphire-framework-server` description:

```markdown
| `sapphire-framework-server` | アプリサーバ骨格（`workspace.*` 名前空間・ワークスペース多重管理・アイドル終了・`ServerCommand`・**特権分離**） | ✅ |
```

and add a short paragraph after the process-architecture pointer added by the app-server plan:

```markdown
> **特権分離（Unix のみ）**: root で起動したサーバは、ワークスペース・キャッシュ・ソケットを
> 人間ユーザーに渡し、shell / 汎用 fs ツール用のヘルパーだけを別ユーザーで fork してから、
> 恒久的に降格する。降格は検証付きで、root は残らない。設計は上記 spec の §3、
> 動機は `sapphire-agent` #257。
```

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-server docs/ARCHITECTURE.md
git commit -m "feat(server): refuse start-on-demand for a privilege-separated app"
```

---

## What this plan does not cover

- **What the helper socket carries.** The framework hands over one connected socket and never
  looks at the bytes. `sapphire-agent` defines the "run this command" protocol in its own
  repository, along with which tools go through it.
- **`service install` with `run_as` / `helper_as`.** Step 10. Until then, a privilege-separated
  server is started by a unit the operator writes.
- **Windows.** There is no equivalent, and there is no plan to invent one. An agent's tool
  isolation is a Unix-only feature; say so in the application's documentation rather than
  letting a Windows user assume otherwise.
- **Restricting the workspace further than `0700`.** Which parts of a workspace an agent may
  reach through its file tools is the application's policy, enforced by the tools. This layer
  only guarantees that the helper user cannot go around them.
