//! The app server skeleton.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.

#![warn(missing_docs)]

mod error;
mod host;

pub use error::{Error, Result};
pub use host::{DEFAULT_IDLE, DEFAULT_MAX_OPEN, WorkspaceHost};
