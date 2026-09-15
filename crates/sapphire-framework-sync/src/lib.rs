//! Transport-agnostic replication core for sapphire-framework workspaces.
//!
//! See `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md`, section 2.

mod entry;
mod error;
mod hash;
mod hlc;
mod id;
mod report;
mod vv;

pub use entry::{Content, Entry, PathUpdate};
pub use error::{Error, Result};
pub use hash::{ContentHash, ParseHashError};
pub use hlc::{Clock, Hlc, MAX_DRIFT_MS, SystemClock};
pub use id::ReplicaId;
pub use report::{Conflict, PauseReason, Report, ScanOutcome, SkipReason, Skipped};
pub use vv::{Dot, VersionVector};
