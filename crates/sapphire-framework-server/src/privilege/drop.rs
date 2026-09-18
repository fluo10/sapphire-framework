//! Handing the directories over, and dropping privileges for good.

use std::path::Path;

use crate::error::{Error, Result};

use super::users::ResolvedUser;

/// Is this process running as root?
pub fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
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

        let me = super::super::resolve(&super::super::UserSpec::Uid(super::super::current_uid()))
            .unwrap();
        hand_over(&[dir.as_path()], &me).unwrap();

        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&dir.join("inner")), 0o700);
        assert_eq!(mode(&dir.join("inner").join("a.redb")), 0o600);
    }

    #[test]
    fn handing_over_a_missing_directory_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let me = super::super::resolve(&super::super::UserSpec::Uid(super::super::current_uid()))
            .unwrap();
        hand_over(&[tmp.path().join("absent").as_path()], &me).unwrap();
    }

    #[test]
    fn dropping_to_the_current_user_from_a_non_root_process_succeeds() {
        if is_root() {
            return; // covered by the root job
        }
        let me = super::super::resolve(&super::super::UserSpec::Uid(super::super::current_uid()))
            .unwrap();
        // Dropping to who we already are must be a no-op, not a failure: that is the
        // path a server takes when it is started without privilege separation.
        drop_to(&me).unwrap();
        assert_eq!(super::super::current_uid(), me.uid);
    }
}
