//! How the bridge reaches other devices.
//!
//! An interface, not an implementation: the switchboard, the routing and the authorization
//! are all testable against [`LoopbackTransport`], and iroh is one implementation behind the
//! `node` feature.

use grain_id::GrainId;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

// The loopback below is the only user of these, and it is behind the same gate.
#[cfg(any(test, feature = "test-util"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-util"))]
use std::sync::{Arc, Mutex};
#[cfg(any(test, feature = "test-util"))]
use tokio::sync::mpsc;

#[cfg(any(test, feature = "test-util"))]
use crate::error::Error;
use crate::error::Result;

/// A bidirectional byte stream to another device.
pub trait PeerStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> PeerStream for T {}

/// A boxed [`PeerStream`].
pub type BoxedStream = Box<dyn PeerStream>;

/// What a caller asks a peer for: the first thing sent on a peer stream.
///
/// It travels ahead of the payload so the far side knows which workspace was asked for
/// without looking inside the stream. `node_id` names the caller, which lets a receiver that
/// is told who called by some other means — [`PeerTransport::accept`] reports it — confirm
/// the two agree. Deciding what to do about a disagreement is the bridge's job.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StreamRequest {
    /// The workspace the caller wants.
    pub workspace_id: GrainId,
    /// The caller's own node id.
    pub node_id: String,
}

/// Reaching other devices.
#[async_trait::async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    /// Open a stream to `node_id` asking for `workspace_id`.
    async fn open(&self, node_id: &str, workspace_id: GrainId) -> Result<BoxedStream>;

    /// Wait for an inbound stream. Returns the caller's node id, what it asked for, and the
    /// stream.
    ///
    /// Authorization is the bridge's, not the transport's: a transport reports who called,
    /// and the bridge decides.
    async fn accept(&self) -> Result<(String, GrainId, BoxedStream)>;

    /// This host's node id.
    fn node_id(&self) -> String;
}

// ── loopback ────────────────────────────────────────────────────────────────

/// Buffer size of each loopback stream, in bytes.
#[cfg(any(test, feature = "test-util"))]
const LOOPBACK_BUFFER: usize = 64 * 1024;

#[cfg(any(test, feature = "test-util"))]
type Inbox = mpsc::UnboundedSender<(String, GrainId, tokio::io::DuplexStream)>;

/// A set of transports that can reach each other, with no network.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Debug, Default)]
pub struct LoopbackNetwork {
    nodes: Arc<Mutex<HashMap<String, Inbox>>>,
}

#[cfg(any(test, feature = "test-util"))]
impl LoopbackNetwork {
    /// An empty network.
    pub fn new() -> LoopbackNetwork {
        LoopbackNetwork::default()
    }

    /// A transport for `node_id`, registered on this network.
    pub fn transport(&self, node_id: &str) -> LoopbackTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        self.nodes
            .lock()
            .expect("loopback network")
            .insert(node_id.to_owned(), tx);
        LoopbackTransport {
            node_id: node_id.to_owned(),
            nodes: Arc::clone(&self.nodes),
            inbox: tokio::sync::Mutex::new(rx),
        }
    }
}

/// One device's end of a [`LoopbackNetwork`].
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
pub struct LoopbackTransport {
    node_id: String,
    nodes: Arc<Mutex<HashMap<String, Inbox>>>,
    inbox: tokio::sync::Mutex<mpsc::UnboundedReceiver<(String, GrainId, tokio::io::DuplexStream)>>,
}

#[cfg(any(test, feature = "test-util"))]
#[async_trait::async_trait]
impl PeerTransport for LoopbackTransport {
    async fn open(&self, node_id: &str, workspace_id: GrainId) -> Result<BoxedStream> {
        let inbox = {
            let nodes = self.nodes.lock().expect("loopback network");
            nodes.get(node_id).cloned()
        };
        let Some(inbox) = inbox else {
            return Err(Error::Peer(format!(
                "no such node on the loopback network: {node_id}"
            )));
        };
        let (mine, theirs) = tokio::io::duplex(LOOPBACK_BUFFER);
        inbox
            .send((self.node_id.clone(), workspace_id, theirs))
            .map_err(|_| Error::Peer(format!("{node_id} is no longer listening")))?;
        Ok(Box::new(mine))
    }

    async fn accept(&self) -> Result<(String, GrainId, BoxedStream)> {
        let mut inbox = self.inbox.lock().await;
        match inbox.recv().await {
            Some((from, ws, stream)) => Ok((from, ws, Box::new(stream))),
            None => Err(Error::Peer("the loopback network is gone".to_owned())),
        }
    }

    fn node_id(&self) -> String {
        self.node_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `Result::unwrap_err` wants the success type to be `Debug`, and a peer stream is not:
    /// it is an arbitrary reader and writer. Unwrapping by hand keeps `PeerStream` free of a
    /// `Debug` bound no transport should have to satisfy.
    #[track_caller]
    fn expect_err<T>(result: Result<T>) -> Error {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(err) => err,
        }
    }

    #[tokio::test]
    async fn a_loopback_stream_carries_bytes_both_ways() {
        let net = LoopbackNetwork::new();
        let a = net.transport("node-a");
        let b = net.transport("node-b");
        let ws = GrainId::random();

        let accept = tokio::spawn(async move { b.accept().await });
        let mut opened = a.open("node-b", ws).await.unwrap();

        let (from, asked, mut accepted) = accept.await.unwrap().unwrap();
        assert_eq!(from, "node-a");
        assert_eq!(asked, ws);

        opened.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        accepted.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        accepted.write_all(b"world").await.unwrap();
        let mut back = [0u8; 5];
        opened.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"world");
    }

    #[tokio::test]
    async fn opening_to_an_unknown_node_fails() {
        let net = LoopbackNetwork::new();
        let a = net.transport("node-a");
        let err = expect_err(a.open("node-nowhere", GrainId::random()).await);
        assert!(err.to_string().contains("node-nowhere"), "{err}");
    }

    #[tokio::test]
    async fn a_transport_reports_its_own_node_id() {
        let net = LoopbackNetwork::new();
        assert_eq!(net.transport("node-a").node_id(), "node-a");
    }

    #[tokio::test]
    async fn closing_one_end_shows_as_end_of_file_on_the_other() {
        let net = LoopbackNetwork::new();
        let a = net.transport("node-a");
        let b = net.transport("node-b");

        let accept = tokio::spawn(async move { b.accept().await });
        let opened = a.open("node-b", GrainId::random()).await.unwrap();
        let (_, _, mut accepted) = accept.await.unwrap().unwrap();

        drop(opened);
        let mut buf = [0u8; 1];
        assert_eq!(accepted.read(&mut buf).await.unwrap(), 0);
    }
}
