//! Two iroh endpoints in one process, on localhost, with no relay and no discovery.
//!
//! These are the tests that say the real transport works: the loopback tests in `peer.rs`
//! prove the switchboard, not the network. Everything here stays offline — the endpoints are
//! built from an offline [`NetConfig`], so a machine with no route to a relay still runs them.

#![cfg(feature = "node")]

use std::sync::Arc;
use std::time::Duration;

use grain_id::GrainId;
use sapphire_framework_bridge::{IrohTransport, NetConfig, PeerTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// `Result::unwrap_err` wants the success type to be `Debug`, and a peer stream is not: it is
/// an arbitrary reader and writer. Unwrapping by hand keeps `PeerStream` free of a `Debug`
/// bound no transport should have to satisfy.
#[track_caller]
fn expect_err<T>(result: sapphire_framework_bridge::Result<T>) -> sapphire_framework_bridge::Error {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(err) => err,
    }
}

/// A host that neither discovers peers nor uses a relay: local addresses only.
fn offline() -> NetConfig {
    NetConfig {
        wake_on_sync: false,
        discovery: false,
        relays: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_file_is_created_once_and_reused() {
    let tmp = tempfile::tempdir().unwrap();
    let key = tmp.path().join("node.key");

    let first = IrohTransport::new(&key, &offline()).await.unwrap();
    let id = first.node_id();
    drop(first);

    let second = IrohTransport::new(&key, &offline()).await.unwrap();
    assert_eq!(second.node_id(), id, "the node id must survive a restart");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn the_key_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let key = tmp.path().join("node.key");
    let _ = IrohTransport::new(&key, &offline()).await.unwrap();
    let mode = std::fs::metadata(&key).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "mode was {:o}", mode & 0o777);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_endpoints_exchange_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let a = IrohTransport::new(&tmp.path().join("a.key"), &offline())
        .await
        .unwrap();
    // `accept` borrows the transport, and the accepting side must outlive the call: the
    // endpoint is what keeps the connection up. This is the shape the bridge uses, too — one
    // `Arc<dyn PeerTransport>` shared by the loops.
    let b = Arc::new(
        IrohTransport::new(&tmp.path().join("b.key"), &offline())
            .await
            .unwrap(),
    );

    // B's identity and address are needed before B is moved into the accepting task.
    let b_node_id = b.node_id();
    let b_addr = b.node_addr().await.unwrap();

    // Teach A where B is, since discovery is off: nothing else tells A how to reach B.
    a.add_known_address(&b_addr).unwrap();

    let ws = GrainId::random();
    let a_node_id = a.node_id();
    let accepting = Arc::clone(&b);
    let accept = tokio::spawn(async move { accepting.accept().await });
    let mut opened = a.open(&b_node_id, ws).await.unwrap();

    let (from, asked, mut accepted) = tokio::time::timeout(Duration::from_secs(10), accept)
        .await
        .expect("the far side to accept")
        .unwrap()
        .unwrap();
    assert_eq!(from, a_node_id, "the far side must learn who called");
    assert_eq!(asked, ws, "the far side must learn what was asked for");

    opened.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    accepted.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    // And the far side can answer, on the same stream.
    accepted.write_all(b"pong").await.unwrap();
    let mut back = [0u8; 4];
    opened.read_exact(&mut back).await.unwrap();
    assert_eq!(&back, b"pong");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_id_that_is_not_a_node_id_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let a = IrohTransport::new(&tmp.path().join("a.key"), &offline())
        .await
        .unwrap();

    let err = expect_err(a.open("not-a-node-id", GrainId::random()).await);
    assert!(err.to_string().contains("not-a-node-id"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_address_that_is_not_an_address_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let a = IrohTransport::new(&tmp.path().join("a.key"), &offline())
        .await
        .unwrap();

    let mut addr = a.node_addr().await.unwrap();
    addr.addrs = vec!["not-an-address".to_owned()];
    let err = expect_err(a.add_known_address(&addr));
    assert!(err.to_string().contains("not-an-address"), "{err}");
}
