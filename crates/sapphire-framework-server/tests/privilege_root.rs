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

use std::os::fd::AsRawFd;
use std::process::Command;

use sapphire_framework_server::privilege::{self, HelperSpec, PrivilegeConfig, UserSpec};

const HUMAN: &str = "sapphire-human";
const TOOLS: &str = "sapphire-tools";

fn require_root() {
    // SAFETY: geteuid has no preconditions.
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "these tests must run as root"
    );
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
        // A panic in a forked body must not read as success. Left to itself a panic in the
        // child unwinds out of `body` and then unwinds again out of the test runner, whose
        // exit path can report 0 — so a body that panicked before doing its work would look
        // exactly like one that finished. Catch it and use the conventional panic exit code
        // instead. (A body killed by a signal cannot be caught here; the parent checks
        // `WIFEXITED` for that.)
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or(101);
        // SAFETY: _exit is async-signal-safe and never returns.
        unsafe { libc::_exit(code) };
    }
    let mut status = 0;
    // SAFETY: `status` is a valid out-pointer.
    unsafe { libc::waitpid(pid, &raw mut status, 0) };
    assert!(
        libc::WIFEXITED(status),
        "the child died of a signal; a body that aborts or is killed is not a pass"
    );
    // WIFEXITED held, so WEXITSTATUS is meaningful.
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
        let Ok(privileges) = privilege::apply(&config, &[tmp]) else {
            return 10;
        };

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
        // `spawn_helper` hands back a `tokio::net::UnixStream`, which adopts the socketpair
        // through `UnixStream::from_std` and therefore needs a reactor. The child is forked
        // from the test harness and has none, so it builds its own — a real application calls
        // `apply` from inside the runtime it is about to serve on, and this mirrors that.
        // (Without this the child panics with "there is no reactor running".)
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => return 20,
        };
        runtime.block_on(async {
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
            let Ok(privileges) = privilege::apply(&config, &[tmp]) else {
                return 10;
            };
            if privileges.helper.is_none() {
                return 11;
            }
            // Give the helper a moment to write the file before the child exits.
            std::thread::sleep(std::time::Duration::from_millis(500));
            0
        })
    });
    assert_eq!(code, 0, "apply failed (code {code})");

    let written = std::fs::read_to_string(out).expect("the helper must have run");
    let helper_uid: u32 = written.trim().parse().expect("a uid");
    let tools_uid = privilege::resolve(&UserSpec::Name(TOOLS.into()))
        .unwrap()
        .uid;
    assert_eq!(helper_uid, tools_uid, "the helper ran as the wrong user");
}

/// The helper must start with nothing beyond its three stdio descriptors — spec §3.1
/// step 4: *close every fd but the child end of the socketpair*. The fd the helper could
/// not otherwise have is the point: the parent holds the workspace database open as root
/// before the fork, so the helper user would be reading a file it cannot open by path.
///
/// The leak this closes is not hypothetical std behaviour: the fixture is a raw descriptor
/// from `dup(2)`, which carries no close-on-exec flag, exactly like a redb handle opened
/// through libc.
#[test]
#[ignore = "needs root and the sapphire-human / sapphire-tools accounts"]
fn the_helper_starts_with_no_descriptors_beyond_stdio() {
    require_root();

    let report = std::path::Path::new("/tmp/sapphire-privsep-fd-report");
    let _ = std::fs::remove_file(report);
    let dir = std::path::Path::new("/tmp/sapphire-privsep-e");
    let _ = std::fs::remove_dir_all(dir);

    let code = in_child(|| {
        let report = std::path::Path::new("/tmp/sapphire-privsep-fd-report");
        // The workspace database, open while still root: everything a leak would expose.
        // Mode 0600 root-owned, so `sapphire-tools` could never open it by path.
        let db = match std::fs::File::create("/tmp/sapphire-privsep-e-database") {
            Ok(db) => db,
            Err(_) => return 20,
        };
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(
            "/tmp/sapphire-privsep-e-database",
            std::fs::Permissions::from_mode(0o600),
        );
        // SAFETY: `dup` of a valid fd yields a fresh descriptor on success. The raw
        // descriptor is what the test holds: it has no close-on-exec flag, which is the
        // leak class under test, and is closed by exec if the fix holds.
        let leaked = unsafe { libc::dup(db.as_raw_fd()) };
        if leaked < 0 {
            return 21;
        }

        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => return 22,
        };
        let code = runtime.block_on(async {
            let config = PrivilegeConfig {
                run_as: UserSpec::Name(HUMAN.into()),
                helper: Some(HelperSpec {
                    user: UserSpec::Name(TOOLS.into()),
                    program: "/bin/sh".into(),
                    args: vec![
                        "-c".into(),
                        format!("ls -l /proc/self/fd > {}", report.display()),
                    ],
                }),
            };
            let tmp = std::path::Path::new("/tmp/sapphire-privsep-e");
            let _ = std::fs::create_dir_all(tmp);
            let Ok(privileges) = privilege::apply(&config, &[tmp]) else {
                return 10;
            };
            if privileges.helper.is_none() {
                return 11;
            }
            // Give the helper a moment to write the listing before the child exits.
            std::thread::sleep(std::time::Duration::from_millis(500));
            0
        });
        // The descriptor is ours to close, not the leak check's: dropping the OwningFd
        // types here is enough for the parent, which exits right after.
        let _ = leaked;
        let _ = db;
        code
    });
    assert_eq!(code, 0, "apply failed (code {code})");

    // The helper ran as `sapphire-tools`, which cannot open a 0600 file owned by root
    // (/tmp is sticky, so the fix's uninstall path would be: leak the fd, read through
    // it). The assertion below is on the fd *table*: either way, what the table shows is
    // what the exec let the helper keep.
    let listed = std::fs::read_to_string(report).expect("the helper must have run");
    let mut stdio = 0;
    for line in listed.lines() {
        let Some((number, target)) = line.split_once(" -> ") else {
            continue; // the directory handle `ls` itself holds while listing
        };
        let number = number
            .split_whitespace()
            .last()
            .expect("an ls -l line has a name");
        if target.starts_with("/proc/") {
            continue; // ls's own handle, described above
        }
        assert!(
            matches!(number, "0" | "1" | "2"),
            "the helper holds fd {number}: {line}\n{listed}"
        );
        stdio += 1;
    }
    assert_eq!(stdio, 3, "fds 0, 1 and 2 must all be there: {listed}");
    assert!(
        !listed.contains("privsep-e-database"),
        "the workspace database leaked to the helper: {listed}"
    );
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

    let human_uid = privilege::resolve(&UserSpec::Name(HUMAN.into()))
        .unwrap()
        .uid;
    assert_eq!(
        std::fs::metadata(&socket).unwrap().uid(),
        human_uid,
        "the socket must belong to the human user, not root"
    );
}
