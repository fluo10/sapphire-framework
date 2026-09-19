//! The replication session two sapphire-framework replicas run over a byte stream.
//!
//! One `Replica` per side, one bidirectional stream, and a framed exchange of version
//! vectors, path updates and content. This crate is the only place that knows the wire
//! format; it neither knows nor cares what carries the bytes.
//!
//! See `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` §3.7.

#![warn(missing_docs)]

mod error;
mod frame;
mod message;

pub use error::{Error, Result};
pub use frame::{Frame, read_frame, write_blob, write_control};
pub use message::Message;

/// The session format this build speaks. Sent in the first message and checked.
pub const SESSION_FORMAT_VERSION: u32 = 1;

/// Largest payload carried inline in an `Updates` page, in bytes. Anything larger is
/// fetched by hash within the same session.
pub const INLINE_LIMIT: usize = 64 * 1024;

/// Longest frame payload accepted, in bytes. Matches the replication core's
/// `DEFAULT_MAX_FILE_SIZE`, and bounds the memory one peer can make the other allocate.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;
