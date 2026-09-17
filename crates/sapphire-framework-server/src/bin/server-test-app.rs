//! Placeholder for the test-only application server.
//!
//! The real app — an [`AppServer`](../lib.rs) serving the `workspace.*` namespace — arrives
//! with a later task in the plan. It exists from the start because `Cargo.toml` declares the
//! `[[bin]]` target, and cargo refuses to resolve a package whose declared target has no
//! source file.

fn main() {}
