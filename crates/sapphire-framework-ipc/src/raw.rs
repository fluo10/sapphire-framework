//! A byte stream with no framing on top (spec §4.3).
//!
//! [`Connection`](crate::Connection) frames everything it carries as newline-delimited
//! JSON. A data-plane connection speaks one JSON line and then raw bytes — re-wrapping a
//! QUIC stream as base64 inside JSON is what the bridge's data plane exists to avoid — so
//! it needs a carrier that does not frame at all.

use tokio::io::{AsyncRead, AsyncWrite};

/// Anything that reads and writes bytes.
pub trait RawIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> RawIo for T {}

/// A byte stream with no framing on top.
///
/// The bridge's data plane speaks one line and then raw bytes, so it cannot use
/// [`Connection`](crate::Connection), which frames everything.
pub type RawStream = Box<dyn RawIo>;
