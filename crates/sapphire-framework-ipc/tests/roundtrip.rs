//! Integration tests for the IPC layer.

/// The crate must stay free of the workspace, search and sync stacks so that the bridge
/// can use it (spec §2, "the crate carries transport, framing, the router and process
/// startup"). Cargo enforces this; this test states it so a future dependency addition is
/// a deliberate act.
#[test]
fn dependency_surface_is_documented() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        "sapphire-framework-workspace",
        "sapphire-framework-retrieve",
        "sapphire-framework-backend",
        "sapphire-framework-sync",
        "iroh",
        "reqwest",
        "axum",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "sapphire-framework-ipc must not depend on {forbidden}"
        );
    }
}
