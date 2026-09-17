//! Placeholder for the test-only echo server.
//!
//! The real server — a `Router` over an `Endpoint` — arrives with the spawn step of the
//! plan. It exists from the start because `Cargo.toml` declares the `[[bin]]` target, and
//! cargo refuses to resolve a package whose declared target has no source file.

fn main() {}
