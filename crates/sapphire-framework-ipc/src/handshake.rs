//! The first exchange on every connection (spec §2.4).

use serde::{Deserialize, Serialize};

/// Sent by the client as the first frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The client's IPC protocol version.
    pub protocol: u32,
    /// The application whose server the client expects to be talking to.
    pub app: String,
    /// Who is connecting.
    pub client: ClientInfo,
}

/// Identifies the connecting process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientInfo {
    /// `cli`, `desktop`, `mcp`, …
    pub kind: String,
    /// The client's crate version.
    pub version: String,
    /// The client's process id.
    pub pid: u32,
}

/// Sent by the server in answer to [`Hello`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Welcome {
    /// The server's IPC protocol version.
    pub protocol: u32,
    /// Who answered.
    pub server: ServerInfo,
}

/// Identifies the serving process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// The server's crate version.
    pub version: String,
    /// The server's process id.
    pub pid: u32,
    /// How the server was started, which decides what a client may do about a version
    /// mismatch (spec §2.6).
    pub managed_by: ManagedBy,
}

/// How a server process came to exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManagedBy {
    /// Started by the OS service manager. A client must not shut it down.
    Service,
    /// Started on demand by a client. A client may ask it to exit and start a new one.
    Spawned,
}
