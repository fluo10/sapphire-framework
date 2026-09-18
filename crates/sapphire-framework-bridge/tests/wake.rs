//! Starting a stopped owner when a peer asks for its workspace.
//!
//! Unix-shaped: the "app server" these tests start is a shell one-liner. The Windows path is
//! covered by `switchboard.rs`, which spawns nothing.
#![cfg(unix)]

mod common;

use sapphire_bridge_api::ManagedBy;
use sapphire_framework_bridge::{LoopbackNetwork, NetConfig};
use std::time::{Duration, Instant};

/// Poll `predicate` until it holds, or give up.
async fn eventually(mut predicate: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(Instant::now() < deadline, "{message}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_spawned_owner_is_started() {
    let marker = tempfile::tempdir().unwrap();
    let evidence = marker.path().join("started");

    // The "app server" is a shell that touches a file and exits.
    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start_with_exe(
        &net,
        common::NODE_B,
        "host-b",
        "/bin/sh",
        &["-c", &format!("touch {}", evidence.display())],
        ManagedBy::Spawned,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;

    // B's app server disconnects; its routes stay.
    common::disconnect_owner(&b).await;

    let _ = client_a.open_stream(ws, device_b).await;

    eventually(|| evidence.exists(), "the owner was never started").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_managed_owner_is_never_started() {
    let marker = tempfile::tempdir().unwrap();
    let evidence = marker.path().join("started");

    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start_with_exe(
        &net,
        common::NODE_B,
        "host-b",
        "/bin/sh",
        &["-c", &format!("touch {}", evidence.display())],
        ManagedBy::Service,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;
    common::disconnect_owner(&b).await;

    let _ = client_a.open_stream(ws, device_b).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        !evidence.exists(),
        "a service-managed owner must be left to its service manager"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn waking_can_be_turned_off() {
    let marker = tempfile::tempdir().unwrap();
    let evidence = marker.path().join("started");

    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    let b = common::start_with_net(
        &net,
        common::NODE_B,
        "host-b",
        NetConfig {
            wake_on_sync: false,
            ..NetConfig::default()
        },
        "/bin/sh",
        &["-c", &format!("touch {}", evidence.display())],
        ManagedBy::Spawned,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;
    common::disconnect_owner(&b).await;

    let _ = client_a.open_stream(ws, device_b).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!evidence.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_owner_is_not_started_twice_in_quick_succession() {
    let marker = tempfile::tempdir().unwrap();
    let counter = marker.path().join("starts");

    let net = LoopbackNetwork::new();
    let a = common::start(&net, common::NODE_A, "host-a").await;
    // Each start appends a line, so the file's length counts them.
    let b = common::start_with_exe(
        &net,
        common::NODE_B,
        "host-b",
        "/bin/sh",
        &["-c", &format!("echo started >> {}", counter.display())],
        ManagedBy::Spawned,
    )
    .await;

    let (client_a, ws, device_b) = common::register_both(&a, &b).await;
    common::disconnect_owner(&b).await;

    // Ten attempts in a row, as a peer reconnecting in a loop would produce.
    for _ in 0..10 {
        let _ = client_a.open_stream(ws, device_b).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let starts = std::fs::read_to_string(&counter)
        .unwrap_or_default()
        .lines()
        .count();
    assert!(
        starts <= 2,
        "the owner was started {starts} times; a peer reconnecting in a loop must not be \
         able to fork-bomb the host"
    );
}
