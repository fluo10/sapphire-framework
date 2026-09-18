//! The app server's side of the control plane.

use std::sync::Arc;

use sapphire_ipc::{Client, ClientInfo, Endpoint, SpawnConfig, ensure_server};
use tokio::sync::broadcast;

use crate::{
    Ack, BRIDGE_DATA_NAME, BRIDGE_NAME, DataHeader, GrainId, IncomingParams, PEERS, PeersResult,
    REGISTER, RegisterParams, RegisterResult, STATUS, StatusResult, UNREGISTER, UnregisterParams,
};

/// How many pending incoming announcements a subscriber may fall behind by.
const INCOMING_CAPACITY: usize = 64;

/// An app server's connection to the bridge.
#[derive(Debug)]
pub struct BridgeClient {
    client: Arc<Client>,
    incoming: broadcast::Sender<IncomingParams>,
    runtime_dir: std::path::PathBuf,
}

impl BridgeClient {
    /// Connect to the bridge, starting it if nothing is listening.
    pub async fn connect(
        kind: &str,
        version: &str,
        spawn: &SpawnConfig,
    ) -> sapphire_ipc::Result<BridgeClient> {
        let runtime_dir = sapphire_ipc::runtime_dir()?;
        let endpoint = Endpoint::in_dir(BRIDGE_NAME, runtime_dir.clone());
        let info = ClientInfo {
            kind: kind.to_owned(),
            version: version.to_owned(),
            pid: std::process::id(),
        };
        let (client, _) = ensure_server(&endpoint, BRIDGE_NAME, info, spawn).await?;
        Ok(BridgeClient::from_client(Arc::new(client), runtime_dir))
    }

    /// Wrap an existing connection. Used by tests and by a caller that already has one.
    pub fn from_client(client: Arc<Client>, runtime_dir: std::path::PathBuf) -> BridgeClient {
        let (incoming, _) = broadcast::channel(INCOMING_CAPACITY);
        let mut notifications = client.notifications();
        let sender = incoming.clone();
        tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(n) if n.method == crate::INCOMING => {
                        match serde_json::from_value::<IncomingParams>(n.params) {
                            Ok(params) => {
                                let _ = sender.send(params);
                            }
                            Err(err) => tracing::warn!("malformed bridge.incoming: {err}"),
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "fell behind on bridge announcements");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        BridgeClient {
            client,
            incoming,
            runtime_dir,
        }
    }

    /// Announce the workspaces this app server owns.
    pub async fn register(&self, params: RegisterParams) -> sapphire_ipc::Result<RegisterResult> {
        self.client.call(REGISTER, params).await
    }

    /// Stop owning one workspace.
    pub async fn unregister(&self, workspace_id: GrainId) -> sapphire_ipc::Result<()> {
        let _: Ack = self
            .client
            .call(UNREGISTER, UnregisterParams { workspace_id })
            .await?;
        Ok(())
    }

    /// The workgroup's devices.
    pub async fn peers(&self) -> sapphire_ipc::Result<PeersResult> {
        self.client.call(PEERS, serde_json::json!({})).await
    }

    /// What the bridge knows about itself.
    pub async fn status(&self) -> sapphire_ipc::Result<StatusResult> {
        self.client.call(STATUS, serde_json::json!({})).await
    }

    /// Announcements that a peer wants a workspace this server owns.
    ///
    /// Answer each one by calling [`accept_stream`](Self::accept_stream) with its ticket.
    pub fn incoming(&self) -> broadcast::Receiver<IncomingParams> {
        self.incoming.subscribe()
    }

    /// Open a stream to `device` for `workspace`.
    pub async fn open_stream(
        &self,
        workspace_id: GrainId,
        device_id: GrainId,
    ) -> sapphire_ipc::Result<sapphire_ipc::RawStream> {
        self.data(DataHeader::Open {
            workspace_id,
            device_id,
        })
        .await
    }

    /// Claim the stream a [`IncomingParams`] announced.
    pub async fn accept_stream(
        &self,
        ticket: String,
    ) -> sapphire_ipc::Result<sapphire_ipc::RawStream> {
        self.data(DataHeader::Accept { ticket }).await
    }

    async fn data(&self, header: DataHeader) -> sapphire_ipc::Result<sapphire_ipc::RawStream> {
        let endpoint = Endpoint::in_dir(BRIDGE_DATA_NAME, self.runtime_dir.clone());
        let raw = sapphire_ipc::connect_raw(&endpoint).await?;
        crate::handshake_data(raw, header).await
    }
}
