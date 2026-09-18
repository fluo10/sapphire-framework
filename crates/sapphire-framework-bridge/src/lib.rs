//! The sapphire bridge: one host-wide daemon per user.
//!
//! Apps run one server per app, each owning a cache that only one process may open. The
//! bridge is the piece that sits above them: it holds this host's device identity, knows
//! which workgroups the host belongs to, and tells an app server where its peers are. It
//! never looks inside a workspace — it routes to the app server that owns one.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §2 and §4.

#![warn(missing_docs)]

mod dir;
mod error;
mod net;
mod peer;
mod routes;
mod workgroup;

pub use dir::{BRIDGE_DIR_ENV, BRIDGE_FORMAT_VERSION, BridgeDir, InstanceLock};
pub use error::{Error, Result};
pub use net::NetConfig;
pub use peer::{BoxedStream, PeerStream, PeerTransport, StreamRequest};
#[cfg(any(test, feature = "test-util"))]
pub use peer::{LoopbackNetwork, LoopbackTransport};
pub use routes::{Route, RouteTable};
pub use workgroup::Workgroup;

#[cfg(test)]
mod tests {
    #[test]
    fn the_bridge_stays_free_of_the_workspace_and_retrieval_stacks() {
        // Global constraint of the plan: the bridge routes to the app server that owns a
        // workspace; it never looks inside one. Those crates are deliberately absent from
        // the manifest above, and this keeps them out.
        let manifest = include_str!("../Cargo.toml");
        for forbidden in [
            "sapphire-framework-workspace",
            "sapphire-framework-retrieve",
            "sapphire-framework-backend",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "sapphire-framework-bridge must not depend on {forbidden}"
            );
        }
    }
}
