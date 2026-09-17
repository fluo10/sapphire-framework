//! Where a server listens (spec §2.5).
//!
//! The runtime directory is persistent, not `/run/user/<uid>`. A server started as a system
//! service and dropped to a user has neither `$XDG_RUNTIME_DIR` nor a login session's
//! `/run/user/<uid>`, while a CLI in a login session has both — the two would resolve
//! different paths and never meet.

use std::path::{Path, PathBuf};

use crate::error::Result;

/// The endpoint name the host-wide bridge listens under.
pub const BRIDGE_NAME: &str = "bridge";

/// Environment variable that replaces the runtime directory outright.
pub const RUNTIME_DIR_ENV: &str = "SAPPHIRE_RUNTIME_DIR";

/// Create `dir` if needed and make it private to the current user.
pub(crate) fn ensure_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(dir)?.permissions();
        if perms.mode() & 0o777 != 0o700 {
            perms.set_mode(0o700);
            std::fs::set_permissions(dir, perms)?;
        }
    }
    Ok(())
}

/// The directory holding this user's sockets and spawn locks, created if absent.
pub fn runtime_dir() -> Result<PathBuf> {
    let dir = match std::env::var_os(RUNTIME_DIR_ENV).filter(|v| !v.is_empty()) {
        Some(v) => PathBuf::from(v),
        None => dirs::data_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("sapphire")
            .join("run"),
    };
    ensure_private_dir(&dir)?;
    Ok(dir)
}

/// Identifies one server's listening address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// The app name, or [`BRIDGE_NAME`].
    pub name: String,
    /// The directory the socket and lock live in.
    pub dir: PathBuf,
}

impl Endpoint {
    /// The endpoint of `app_name`'s server, creating the runtime directory if needed.
    pub fn for_app(app_name: &str) -> Result<Endpoint> {
        Ok(Endpoint {
            name: app_name.to_owned(),
            dir: runtime_dir()?,
        })
    }

    /// The endpoint of the host-wide bridge.
    pub fn for_bridge() -> Result<Endpoint> {
        Endpoint::for_app(BRIDGE_NAME)
    }

    /// An endpoint in an explicit directory. Used by tests and by callers that resolved
    /// the directory themselves.
    pub fn in_dir(name: impl Into<String>, dir: PathBuf) -> Endpoint {
        Endpoint {
            name: name.into(),
            dir,
        }
    }

    /// The Unix domain socket path.
    pub fn socket_path(&self) -> PathBuf {
        self.dir.join(format!("{}.sock", self.name))
    }

    /// The Windows named pipe name, scoped to the current user so that two users on one
    /// machine get separate pipes.
    pub fn pipe_name(&self) -> String {
        format!(r"\\.\pipe\sapphire.{}.{}", user_scope(), self.name)
    }

    /// The lock file that serialises start-on-demand (spec §2.6 step 3a).
    pub fn lock_path(&self) -> PathBuf {
        self.dir.join(format!("{}.spawn.lock", self.name))
    }
}

/// A stable, per-user string used to scope the Windows pipe name.
///
/// The security descriptor is what actually restricts access (see `windows.rs`); this only
/// keeps two users' pipes from colliding by name.
fn user_scope() -> String {
    #[cfg(windows)]
    {
        crate::windows::current_user_sid().unwrap_or_else(|_| "unknown".to_owned())
    }
    #[cfg(unix)]
    {
        // SAFETY: getuid is always successful and has no preconditions.
        unsafe { libc::getuid() }.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_names_its_socket_and_lock_under_its_directory() {
        let dir = std::env::temp_dir().join("sapphire-endpoint-test");
        let ep = Endpoint::in_dir("sapphire-journal", dir.clone());
        assert_eq!(ep.socket_path(), dir.join("sapphire-journal.sock"));
        assert_eq!(ep.lock_path(), dir.join("sapphire-journal.spawn.lock"));
    }

    #[test]
    fn the_bridge_endpoint_is_named_bridge() {
        let ep = Endpoint::in_dir(BRIDGE_NAME, std::env::temp_dir());
        assert_eq!(ep.socket_path().file_name().unwrap(), "bridge.sock");
    }

    #[test]
    fn a_pipe_name_is_scoped_to_the_user() {
        let ep = Endpoint::in_dir("sapphire-journal", std::env::temp_dir());
        let name = ep.pipe_name();
        assert!(name.starts_with(r"\\.\pipe\sapphire."), "{name}");
        assert!(name.ends_with(".sapphire-journal"), "{name}");
    }

    #[test]
    fn the_runtime_directory_env_var_replaces_the_whole_path() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test process section; no other thread reads the
        // environment concurrently in this test binary.
        unsafe { std::env::set_var("SAPPHIRE_RUNTIME_DIR", tmp.path()) };
        let dir = runtime_dir().unwrap();
        unsafe { std::env::remove_var("SAPPHIRE_RUNTIME_DIR") };
        assert_eq!(dir, tmp.path());
        assert!(dir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn the_runtime_directory_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("run");
        ensure_private_dir(&dir).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "mode was {:o}", mode & 0o777);
    }
}
