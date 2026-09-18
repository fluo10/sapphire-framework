//! The iroh implementation of [`PeerTransport`](crate::PeerTransport).
//!
//! Everything iroh-shaped lives here. The rest of the bridge knows only "open a stream to a
//! node id" and "accept a stream and learn who called", so replacing this file would not
//! touch anything else.

use std::net::SocketAddr;
use std::path::Path;

use grain_id::GrainId;
use sapphire_bridge_api::ALPN;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

// `iroh` is also the name of this module, so the crate is spelled with a leading `::` or the
// path would be ambiguous.
use ::iroh::address_lookup::{DnsAddressLookup, MemoryLookup, PkarrPublisher, PkarrResolver};
use ::iroh::endpoint::presets;
use ::iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr};

use crate::error::{Error, Result};
use crate::net::NetConfig;
use crate::peer::{BoxedStream, PeerTransport, StreamRequest};

/// How many bytes of the request line to accept before giving up.
///
/// The request is a small JSON object; anything past this is a peer that is not speaking this
/// protocol, and reading forever on it would be a way to make the bridge allocate.
const MAX_REQUEST_LINE: u64 = 4 * 1024;

/// A device's node id and the addresses that reach it.
///
/// This is what one device hands another out of band — the pairing flow this plan leaves for
/// later — so that the other can dial it without a discovery service. It deliberately names
/// nothing from iroh: a caller shares it, stores it, or prints it without linking iroh.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeAddr {
    /// The device's node id.
    pub node_id: String,
    /// The socket addresses this device can be reached at, as `host:port`.
    pub addrs: Vec<String>,
}

/// This host's endpoint on the network.
pub struct IrohTransport {
    endpoint: Endpoint,
    node_id: String,
    /// Addresses learned out of band, so a peer can be dialed without a discovery service.
    known: MemoryLookup,
}

impl IrohTransport {
    /// Bind an endpoint, loading or creating the secret key at `key_path`.
    ///
    /// `net` decides what the endpoint offers and uses: `discovery` turns on iroh's address
    /// lookup services, and `relays` replaces the default relay set (an empty list disables
    /// relays entirely, which is what a test or a fully local host wants).
    pub async fn new(key_path: &Path, net: &NetConfig) -> Result<IrohTransport> {
        let secret = load_or_create_key(key_path)?;
        let known = MemoryLookup::new();

        // `Minimal` rather than `N0`: it picks the crypto provider, and nothing else. Every
        // other thing `N0` turns on is what `net` is here to decide.
        let mut builder = Endpoint::builder(presets::Minimal)
            .secret_key(secret)
            .alpns(vec![ALPN.to_vec()])
            .address_lookup(known.clone());
        if net.discovery {
            builder = builder
                .address_lookup(PkarrPublisher::n0_dns())
                .address_lookup(PkarrResolver::n0_dns())
                .address_lookup(DnsAddressLookup::n0_dns());
        }

        let endpoint = builder
            .relay_mode(relay_mode(net)?)
            .bind()
            .await
            .map_err(|e| Error::Peer(format!("could not bind the endpoint: {e}")))?;
        let node_id = endpoint.id().to_string();

        Ok(IrohTransport {
            endpoint,
            node_id,
            known,
        })
    }

    /// This device's node id and addresses, for handing to a peer out of band.
    ///
    /// Fails while the endpoint has nothing to share: an address with no transports on it
    /// would tell a peer to dial a device it cannot reach, and a pairing flow that shared one
    /// would be silently useless.
    pub async fn node_addr(&self) -> Result<NodeAddr> {
        let addr = self.endpoint.addr();
        if addr.is_empty() {
            return Err(Error::Peer(
                "this endpoint has no address to share yet".to_owned(),
            ));
        }
        Ok(NodeAddr {
            node_id: addr.id.to_string(),
            addrs: addr.ip_addrs().map(|addr| addr.to_string()).collect(),
        })
    }

    /// Teach this endpoint how to reach `addr`, which is how a peer with discovery off is
    /// dialed.
    pub fn add_known_address(&self, addr: &NodeAddr) -> Result<()> {
        let id: EndpointId = addr
            .node_id
            .parse()
            .map_err(|e| Error::Config(format!("{}: not a node id: {e}", addr.node_id)))?;
        let mut transports = Vec::with_capacity(addr.addrs.len());
        for address in &addr.addrs {
            let socket: SocketAddr = address
                .parse()
                .map_err(|e| Error::Config(format!("{address}: not a socket address: {e}")))?;
            transports.push(TransportAddr::Ip(socket));
        }
        self.known
            .add_endpoint_info(EndpointAddr::from_parts(id, transports));
        Ok(())
    }
}

#[async_trait::async_trait]
impl PeerTransport for IrohTransport {
    async fn open(&self, node_id: &str, workspace_id: GrainId) -> Result<BoxedStream> {
        let id: EndpointId = node_id
            .parse()
            .map_err(|e| Error::Peer(format!("{node_id}: not a node id: {e}")))?;
        let conn = self
            .endpoint
            .connect(id, ALPN)
            .await
            .map_err(|e| Error::Peer(format!("could not reach {node_id}: {e}")))?;
        let (mut send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| Error::Peer(format!("could not open a stream to {node_id}: {e}")))?;

        // The request travels ahead of the payload, so the far side knows what was asked for
        // without looking inside the stream. Same framing as the data plane's header: one
        // JSON object and a newline.
        let request = StreamRequest {
            workspace_id,
            node_id: self.node_id.clone(),
        };
        let mut line = serde_json::to_vec(&request).map_err(|e| Error::Protocol(e.to_string()))?;
        line.push(b'\n');
        send.write_all(&line)
            .await
            .map_err(|e| Error::Peer(format!("could not send the request to {node_id}: {e}")))?;
        send.flush().await.map_err(|e| Error::Peer(e.to_string()))?;

        // The two halves keep the connection alive between them, so the caller may keep this
        // for as long as it likes.
        Ok(Box::new(tokio::io::join(recv, send)))
    }

    async fn accept(&self) -> Result<(String, GrainId, BoxedStream)> {
        loop {
            let Some(incoming) = self.endpoint.accept().await else {
                return Err(Error::Peer("the endpoint is closed".to_owned()));
            };
            // Any host on the network may send a packet here, so a failed handshake or a
            // stream that never arrives must not end the loop: it is one caller going away,
            // not this bridge stopping.
            let accepting = match incoming.accept() {
                Ok(accepting) => accepting,
                Err(err) => {
                    tracing::debug!("a peer could not be accepted: {err}");
                    continue;
                }
            };
            let conn = match accepting.await {
                Ok(conn) => conn,
                Err(err) => {
                    tracing::debug!("a peer's handshake failed: {err}");
                    continue;
                }
            };
            let from = conn.remote_id();
            let (send, recv) = match conn.accept_bi().await {
                Ok(halves) => halves,
                Err(err) => {
                    tracing::debug!(peer = %from, "a peer opened no stream: {err}");
                    continue;
                }
            };
            let (request, recv) = match read_request(recv).await {
                Ok(read) => read,
                Err(err) => {
                    tracing::debug!(peer = %from, "a peer sent an unreadable request: {err}");
                    continue;
                }
            };
            if request.node_id != from.to_string() {
                // Reported, not enforced: this transport reports who called, and the bridge
                // decides what a disagreement means. The name below is the authenticated one,
                // because that is the identity the far side cannot forge.
                tracing::debug!(
                    authenticated = %from,
                    claimed = %request.node_id,
                    "a peer's request named a different node id than the one that called"
                );
            }
            return Ok((
                from.to_string(),
                request.workspace_id,
                Box::new(tokio::io::join(recv, send)),
            ));
        }
    }

    fn node_id(&self) -> String {
        self.node_id.clone()
    }
}

/// Read the request line, leaving the stream positioned at the first byte of the payload.
async fn read_request(
    mut recv: ::iroh::endpoint::RecvStream,
) -> Result<(StreamRequest, ::iroh::endpoint::RecvStream)> {
    // Capacity 1: a larger buffer would read past the newline and swallow payload bytes the
    // caller is about to be handed.
    let mut reader = BufReader::with_capacity(1, &mut recv);
    let mut line = String::new();
    // `read_line` grows its buffer until it sees a newline, so it is bounded here rather than
    // checked afterwards: a peer that never sends one must not be able to make the bridge
    // allocate without limit. One byte over, so a line that fits exactly still reads.
    let read = (&mut reader)
        .take(MAX_REQUEST_LINE + 1)
        .read_line(&mut line)
        .await
        .map_err(|e| Error::Protocol(format!("could not read the request line: {e}")))?;
    if read == 0 {
        return Err(Error::Protocol("the peer sent no request line".to_owned()));
    }
    if read as u64 > MAX_REQUEST_LINE {
        return Err(Error::Protocol(format!(
            "the request line is longer than {MAX_REQUEST_LINE} bytes"
        )));
    }
    let request = serde_json::from_str(line.trim())
        .map_err(|e| Error::Protocol(format!("bad peer request: {e}")))?;
    Ok((request, recv))
}

/// The relay mode `net` asks for: none at all, or exactly the relays it names.
fn relay_mode(net: &NetConfig) -> Result<RelayMode> {
    if net.relays.is_empty() {
        return Ok(RelayMode::Disabled);
    }
    let mut urls = Vec::with_capacity(net.relays.len());
    for relay in &net.relays {
        let url = relay
            .parse()
            .map_err(|e| Error::Config(format!("{relay}: not a relay URL: {e}")))?;
        urls.push(url);
    }
    Ok(RelayMode::custom(urls))
}

/// Read the secret key, or create one at `0600`.
///
/// This file **is** the device's identity: losing it means rejoining every workgroup as a new
/// device, so it is created once and never regenerated on a parse failure.
fn load_or_create_key(path: &Path) -> Result<SecretKey> {
    match std::fs::read(path) {
        Ok(bytes) => parse_key(path, &bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes)
                .map_err(|e| Error::Config(format!("no system random source: {e}")))?;
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            match create_private(path, &bytes)? {
                Created::Yes => Ok(SecretKey::from_bytes(&bytes)),
                // Another process won the race and wrote *its* key. Use that one, not the
                // bytes generated here: the file is the identity, and two processes that
                // disagreed about it would each think the other was a stranger.
                Created::AlreadyExisted => load_or_create_key(path),
            }
        }
        Err(e) => Err(Error::Io(e)),
    }
}

/// This host's node id, from the key at `key_path`, creating the key if it is absent.
///
/// Derived without binding an endpoint, which is what a one-shot command needs: founding a
/// workgroup records this host's node id, and that happens before — sometimes long before —
/// a bridge is running to ask.
pub fn load_or_create_node_id(key_path: &Path) -> Result<String> {
    Ok(load_or_create_key(key_path)?.public().to_string())
}

/// Interpret the bytes of a key file.
fn parse_key(path: &Path, bytes: &[u8]) -> Result<SecretKey> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
        Error::Config(format!(
            "{}: a node key is 32 bytes, found {}; move it aside rather than \
             letting a new identity be minted",
            path.display(),
            bytes.len()
        ))
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}

/// Whether [`create_private`] wrote the file or found one already there.
#[derive(Debug, PartialEq, Eq)]
enum Created {
    Yes,
    AlreadyExisted,
}

/// Write a new key file that nobody else can read.
///
/// `create_new`, so a key that appeared between the read and here is never overwritten: the
/// device that already holds an identity keeps it.
fn create_private(path: &Path, bytes: &[u8; 32]) -> Result<Created> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // The mode is masked by the umask, so it is set again below: the key must be private
        // whatever the user's umask is.
        options.mode(0o600);
    }

    match options.open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.flush()?;
        }
        // Another process created the identity first.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Ok(Created::AlreadyExisted);
        }
        Err(e) => return Err(Error::Io(e)),
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(Created::Yes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_relays_means_no_relay_transport() {
        let net = NetConfig {
            wake_on_sync: false,
            discovery: false,
            relays: vec![],
        };
        assert!(matches!(relay_mode(&net).unwrap(), RelayMode::Disabled));
    }

    #[test]
    fn a_named_relay_is_used_and_a_bad_one_is_refused() {
        let net = NetConfig {
            wake_on_sync: false,
            discovery: false,
            relays: vec!["https://relay.example/".to_owned()],
        };
        assert!(matches!(relay_mode(&net).unwrap(), RelayMode::Custom(_)));

        let net = NetConfig {
            wake_on_sync: false,
            discovery: false,
            relays: vec!["not a url".to_owned()],
        };
        let err = relay_mode(&net).unwrap_err();
        assert!(err.to_string().contains("not a url"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_node_id_follows_from_the_key_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("node.key");

        // Derived without an endpoint, and stable across calls.
        let id = load_or_create_node_id(&path).unwrap();
        assert_eq!(
            id.len(),
            64,
            "a node id is 64 lowercase hex characters: {id}"
        );
        assert_eq!(load_or_create_node_id(&path).unwrap(), id);

        // The same identity the transport binds.
        let net = NetConfig {
            wake_on_sync: false,
            discovery: false,
            relays: vec![],
        };
        let transport = IrohTransport::new(&path, &net).await.unwrap();
        assert_eq!(transport.node_id(), id);
    }

    #[test]
    fn a_key_of_the_wrong_length_is_not_replaced() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("node.key");
        std::fs::write(&path, b"short").unwrap();

        let err = load_or_create_key(&path).unwrap_err();
        assert!(err.to_string().contains("32 bytes"), "{err}");
        // The bytes are still there: a damaged key is a human decision, not a fresh identity.
        assert_eq!(std::fs::read(&path).unwrap(), b"short");
    }

    #[test]
    fn an_existing_key_is_never_overwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("node.key");

        let first = load_or_create_key(&path).unwrap();
        assert_eq!(
            create_private(&path, &[7u8; 32]).unwrap(),
            Created::AlreadyExisted
        );

        // Loading again gives the identity that is on disk, not the one just offered.
        let second = load_or_create_key(&path).unwrap();
        assert_eq!(second.to_bytes(), first.to_bytes());
        assert_eq!(std::fs::read(&path).unwrap(), first.to_bytes());
    }
}
