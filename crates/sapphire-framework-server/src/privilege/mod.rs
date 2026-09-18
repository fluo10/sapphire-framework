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
