//! The app server skeleton.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §4.

#![warn(missing_docs)]

mod error;
mod events;
mod handlers;
mod host;
#[cfg(test)]
mod test_support;

pub use error::{Error, Result};
pub use events::subscribe_method;
pub use handlers::workspace_router;
pub use host::{DEFAULT_IDLE, DEFAULT_MAX_OPEN, WorkspaceHost};
