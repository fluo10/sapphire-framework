//! Forking the lower-privileged helper, before the drop.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
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
///
/// The child keeps nothing of ours across the exec beyond stdio: descriptors are handed over
/// close-on-exec and swept on the parent side, so a file the server has open — the workspace
/// database, a log — does not follow the fork into the less trusted half. See
/// [`cloak_fds_above_stderr`].
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
            // Becoming the identity we already are needs no privileges, and `initgroups`
            // would fail with EPERM without them. Mirroring `drop_to`'s early return is what
            // lets a server started without privilege separation still fork its helper.
            // Both ids must match: a process whose effective id differs must still go
            // through the three calls below, or it would exec the helper as the wrong user.
            if libc::getuid() == uid && libc::geteuid() == uid && uid != 0 {
                return Ok(());
            }
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

    // SAFETY: the closure runs between fork and exec, so it may call only async-signal-safe
    // functions. `close_range` is a direct syscall — see `close_inherited_fds` — and nothing
    // here allocates.
    unsafe {
        command.pre_exec(|| close_inherited_fds());
    }

    // The sweep, before the fork, so even the parent window between two helpers stays
    // clean and the table is already marked when `fork` copies it.
    cloak_fds_above_stderr();

    let child_process = command.spawn().map_err(|e| {
        Error::Privilege(format!(
            "could not start the helper {}: {e}",
            spec.program.display()
        ))
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

    Ok(HelperHandle {
        pid,
        user: user.clone(),
        socket,
    })
}

/// Mark every descriptor above stdio close-on-exec, in the child, before the exec.
///
/// Spec §3.1 step 4: *fork/exec the helper … close every fd but the child end of the
/// socketpair*. By hook time std's plumbing is in place — fd 0 is the socketpair's child
/// end, 1 is `/dev/null`, 2 is our stderr — so everything from 3 up is inherited and must
/// not survive. The exec closing each marked descriptor is what delivers the requirement.
///
/// Marking rather than closing is deliberate. `std::process::Command` reports an exec
/// failure by writing to a socketpair whose child end is still open, and counts on that
/// end being closed *by the exec*: closing descriptors here, before the exec, turns the
/// report into `EBADF` and leaves the child aborting with a report the parent reads as
/// ordinary end of file — a helper that never started would come back as success. The
/// close-on-exec flag does the same closing, one instruction later, with the report path
/// intact; observed from the execed program the effect is identical.
///
/// On Linux this is `close_range(3, .., CLOSE_RANGE_CLOEXEC)`, a direct syscall and
/// async-signal-safe (kernel ≥ 5.9). Elsewhere the table cannot be enumerated from an
/// async-signal-safe hook (`/proc` is Linux-only), so the hook reports the gap back
/// through `spawn` instead of pretending; the parent-side sweep
/// ([`cloak_fds_above_stderr`]) still covers what this process opened through libc.
#[cfg(target_os = "linux")]
unsafe fn close_inherited_fds() -> std::io::Result<()> {
    // The socketpair child end, after `dup2` onto fd 0, sits below 3; keeping std's stdio
    // plumbing intact means touching only 3 and up.
    // SAFETY: the arguments are plain integers and the call has no further preconditions.
    let flags = libc::CLOSE_RANGE_CLOEXEC as libc::c_int;
    if unsafe { libc::close_range(3, u32::MAX, flags) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// The non-Linux hook: the sweep below is the only guarantee, so say so through `spawn`.
#[cfg(not(target_os = "linux"))]
unsafe fn close_inherited_fds() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "closing inherited fds before exec is only implemented for Linux",
    ))
}

/// The parent half of the same guarantee: hand *this process's* open descriptors to the
/// next helper close-on-exec.
///
/// Rust's std cloexecs what it opens, but a server reaches files through libc too — a redb
/// database, an openat handle — and `open(2)` without `O_CLOEXEC` is exactly the leak the
/// spec closes: such a descriptor crosses the fork unmarked and, unmarked, survives the
/// exec into the helper. Sweeping here closes that class at the moment a helper is forked.
/// Only fds of this process are touched — descriptors are not shared across processes.
fn cloak_fds_above_stderr() {
    #[cfg(target_os = "linux")]
    {
        // Best effort by design: an old kernel rejects the call, and a failure leaves the
        // exec-time hook as the remaining guard rather than failing a start that would
        // otherwise work.
        // SAFETY: the arguments are plain integers and the call has no preconditions.
        let flags = libc::CLOSE_RANGE_CLOEXEC as libc::c_int;
        let _ = unsafe { libc::close_range(3, u32::MAX, flags) };
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Without close_range the table cannot be swept safely from a signal-safe context
        // and the process may not even have /proc; the child-side hook reports the gap.
    }
}

/// A connected pair of Unix stream sockets.
///
/// Both ends are created close-on-exec, and then the child end is made inheritable again.
/// That ordering matters: the helper inherits every fd that is not close-on-exec, so a
/// parent end left inheritable would let the helper hold its own socket open and the parent
/// would never see end of file. The child end must survive the exec because it is the
/// helper's fd 0.
fn socketpair() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: `fds` is a valid array of two ints, which is what socketpair writes.
    let rc = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if rc != 0 {
        return Err(Error::Privilege(format!(
            "socketpair failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: both fds are freshly created and owned by nobody else.
    let (parent, child) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    clear_cloexec(&child)?;
    Ok((parent, child))
}

/// Drop close-on-exec from `fd`, so the child that inherits it keeps it after the exec.
fn clear_cloexec(fd: &OwnedFd) -> Result<()> {
    // SAFETY: `fd` is a valid, open file descriptor owned by the caller when the call is
    // made; F_SETFD with 0 only clears FD_CLOEXEC.
    let rc = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, 0) };
    if rc != 0 {
        return Err(Error::Privilege(format!(
            "could not prepare the helper socket: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// These tests spawn a helper as *this* process's own user, which needs no privileges.
    /// That makes them a no-op under a root job, where this user is root and `resolve`
    /// refuses it: the root job has to cover spawning as a genuinely different user.
    fn skip_as_root() -> bool {
        super::super::is_root()
    }

    fn me() -> ResolvedUser {
        super::super::resolve(&super::super::UserSpec::Uid(super::super::current_uid())).unwrap()
    }

    /// Spawning a helper as *our own* user needs no privileges, so this runs everywhere and
    /// covers the socket plumbing. The root job covers becoming a different user.
    #[tokio::test]
    async fn the_helper_gets_a_connected_socket_on_stdin() {
        if skip_as_root() {
            return;
        }
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
        if skip_as_root() {
            return;
        }
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
        if skip_as_root() {
            return;
        }
        let spec = HelperSpec {
            user: super::super::UserSpec::Uid(super::super::current_uid()),
            program: "/nonexistent/helper-9f3a".into(),
            args: vec![],
        };
        let err = spawn_helper(&spec, &me()).unwrap_err();
        assert!(err.to_string().contains("helper-9f3a"), "{err}");
    }

    /// The helper must start with nothing above stdio: the leak this closes is a plain fd
    /// to the workspace database, which the helper user could never open by path.
    ///
    /// The `sh` below walks `/proc/self/fd` and writes the table to a file the test reads
    /// after the helper exits. The assertion is strict — nothing at all beyond fds 0–2 —
    /// so it fails the moment the parent leaks anything, whatever the descriptor points at
    /// and whatever std opens next.
    #[tokio::test]
    async fn the_helper_inherits_no_descriptors_beyond_stdio() {
        if skip_as_root() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cache.redb");
        std::fs::write(&db, b"sapphire-fixture").unwrap();
        // A descriptor std never cloexecs: a raw dup of stderr. Whether it survives the
        // exec is the kernel's business, not the helper's to inherit as a *third* copy —
        // keep it here so the test would catch it if it did.
        let dup = {
            // SAFETY: `dup` of a valid fd yields a fresh descriptor on success.
            let fd = unsafe { libc::dup(2) };
            assert!(fd >= 0);
            // SAFETY: a fresh descriptor from `dup`, owned by nobody else.
            unsafe { OwnedFd::from_raw_fd(fd) }
        };

        let report = dir.path().join("fds.txt");
        let spec = HelperSpec {
            user: super::super::UserSpec::Uid(super::super::current_uid()),
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                format!("ls -l /proc/self/fd > {}", report.display()),
            ],
        };
        let mut handle = spawn_helper(&spec, &me()).unwrap();
        drop(dup);
        let mut buf = [0u8; 1];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.socket.read(&mut buf),
        )
        .await
        .expect("the helper must exit promptly")
        .unwrap();

        let listed = std::fs::read_to_string(&report).unwrap();
        // One line per open descriptor, `N -> target`. Three stdio lines, `ls`'s own handle
        // on the directory it is reading, and nothing else — anything above that is a leak.
        let mut stdio = 0;
        for line in listed.lines() {
            let Some((number, target)) = line.split_once(" -> ") else {
                continue; // the directory handle, described above
            };
            let number = number
                .split_whitespace()
                .last()
                .expect("an ls -l line has a name");
            if target.starts_with("/proc/") {
                continue; // ls's own handle again, this time with an arrow
            }
            assert!(
                matches!(number, "0" | "1" | "2"),
                "the helper holds fd {number}: {line}\n{listed}"
            );
            stdio += 1;
        }
        assert_eq!(stdio, 3, "fds 0, 1 and 2 must all be there: {listed}");
        assert!(
            !listed.contains("cache.redb"),
            "the workspace database leaked to the helper: {listed}"
        );
    }
}
