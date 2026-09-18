//! Starting as root in order to become two different users, and then neither being root nor
//! able to become root again.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §3, and
//! `sapphire-agent` issue #257 for what this is for: an agent's shell and generic file tools
//! keep their freedom while losing access to the workspace.
//!
//! Unix only. On any other platform a configuration that asks for this fails.
//!
//! The calling order is forced (spec §3.1): the helper is forked before the drop, because
//! becoming another user needs root, and the IPC socket is bound after `apply` returns,
//! because otherwise it would be created owned by root.
//!
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

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[cfg(unix)]
mod drop;
#[cfg(unix)]
mod helper;
#[cfg(unix)]
mod users;

#[cfg(unix)]
pub use drop::{drop_to, hand_over, is_root};
#[cfg(unix)]
pub use helper::{HelperHandle, spawn_helper};
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
        return Ok(Privileges {
            run_as,
            helper: None,
        });
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
        let config = PrivilegeConfig {
            run_as: my_spec(),
            helper: None,
        };
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
        let config = PrivilegeConfig {
            run_as: UserSpec::Uid(1),
            helper: None,
        };
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
        let config = PrivilegeConfig {
            run_as: UserSpec::Uid(0),
            helper: None,
        };
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
