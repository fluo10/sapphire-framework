//! The sapphire bridge: one host-wide daemon per user.
//!
//! Apps run one server per app, each owning a cache that only one process may open. The
//! bridge is the piece that sits above them: it holds this host's device identity, knows
//! which workgroups the host belongs to, and tells an app server where its peers are. It
//! never looks inside a workspace — it routes to the app server that owns one.
//!
//! See `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §2 and §4.

#![warn(missing_docs)]

mod command;
mod control;
mod data;
mod dir;
mod error;
#[cfg(feature = "node")]
mod iroh;
mod net;
mod peer;
mod routes;
mod workgroup;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use grain_id::GrainId;
use sapphire_bridge_api::{BRIDGE_DATA_NAME, BRIDGE_NAME, ManagedBy, WorkspaceRegistration};
use sapphire_ipc::{Endpoint, ServerInfo};

pub use command::BridgeCommand;
pub use dir::{BRIDGE_DIR_ENV, BRIDGE_FORMAT_VERSION, BridgeDir, InstanceLock};
pub use error::{Error, Result};
#[cfg(feature = "node")]
pub use iroh::{IrohTransport, NodeAddr};
pub use net::NetConfig;
pub use peer::{BoxedStream, PeerStream, PeerTransport, StreamRequest};
#[cfg(any(test, feature = "test-util"))]
pub use peer::{LoopbackNetwork, LoopbackTransport};
pub use routes::{Route, RouteTable};
pub use workgroup::Workgroup;

use crate::control::{Owners, Wakes};
use crate::data::Tickets;

/// The switchboard: one control plane, one data plane, and the tables they share.
///
/// Build one with [`Bridge::new`], adjust it with the setters, then hand it to
/// [`Bridge::run`], which serves three loops until one of them fails:
/// - a **control** listener, answering `bridge.register` and its siblings over JSON-RPC;
/// - a **data** listener, where an app server asks for a stream by ticket or by device;
/// - an **inbound** loop, which accepts peer streams, authorizes them, and parks them for
///   the app server that owns the workspace they asked for.
///
/// The two listeners are dispatchers in the sense that matters here: the control plane never
/// looks inside a workspace, and the data plane never looks at the bytes it relays.
pub struct Bridge {
    dir: BridgeDir,
    transport: Arc<dyn PeerTransport>,
    version: &'static str,
    control: Endpoint,
    data: Endpoint,
    /// `None` means "read `net.toml` when `run` starts", which is what a real caller wants:
    /// the file may have been edited since this bridge was built. Tests set it outright.
    net: Option<NetConfig>,
    /// The routing table, held in memory rather than re-read per call: two app servers
    /// registering at the same moment would otherwise load-modify-write over each other. Only
    /// this process writes the file, so it cannot go stale behind us.
    routes: Mutex<RouteTable>,
    /// Which app servers are connected right now.
    owners: Owners,
    /// Inbound streams waiting for their owner.
    tickets: Tickets,
    /// When each application was last started by `wake_on_sync`.
    wakes: Wakes,
}

impl Bridge {
    /// A bridge over `dir`, reaching other devices through `transport`, reporting `version`.
    ///
    /// The endpoints default to the built-in ones in this user's runtime directory, and the
    /// network configuration to whatever `net.toml` says. Tests override all three.
    pub fn new(
        dir: BridgeDir,
        transport: Arc<dyn PeerTransport>,
        version: &'static str,
    ) -> Result<Bridge> {
        let runtime = sapphire_ipc::runtime_dir()?;
        let routes = RouteTable::load(&dir.routes_toml())?;
        Ok(Bridge {
            dir,
            transport,
            version,
            control: Endpoint::in_dir(BRIDGE_NAME, runtime.clone()),
            data: Endpoint::in_dir(BRIDGE_DATA_NAME, runtime),
            net: None,
            routes: Mutex::new(routes),
            owners: Owners::default(),
            tickets: Tickets::default(),
            wakes: Wakes::default(),
        })
    }

    /// Listen for app servers here instead of at the default control endpoint.
    pub fn control_endpoint(mut self, endpoint: Endpoint) -> Bridge {
        self.control = endpoint;
        self
    }

    /// Serve app-server streams here instead of at the default data endpoint.
    pub fn data_endpoint(mut self, endpoint: Endpoint) -> Bridge {
        self.data = endpoint;
        self
    }

    /// Use this network configuration instead of reading `net.toml`.
    pub fn net(mut self, net: NetConfig) -> Bridge {
        self.net = Some(net);
        self
    }

    /// The endpoints this bridge serves on: the control plane first, the data plane second.
    pub fn endpoints(&self) -> (Endpoint, Endpoint) {
        (self.control.clone(), self.data.clone())
    }

    /// Serve the control plane, the data plane and the inbound peer loop until one fails.
    ///
    /// Nothing here takes the single-instance lock: that is the caller's, because "a bridge
    /// is already running" is a normal outcome for a command and an error for a caller to
    /// interpret.
    pub async fn run(mut self) -> Result<()> {
        let net = match self.net.take() {
            Some(net) => net,
            // Read up front rather than at the first peer request, so an unreadable file is
            // reported when the bridge starts.
            None => NetConfig::load(&self.dir.net_toml())?,
        };
        let (control_endpoint, data_endpoint) = self.endpoints();
        let bridge = Arc::new(self);
        let info = ServerInfo {
            version: bridge.version.to_owned(),
            pid: std::process::id(),
            // The bridge is not installed as a service yet; it is started on demand. A
            // client that finds a mismatched version may therefore replace it, which is the
            // right answer for something this process started.
            managed_by: ManagedBy::Spawned,
        };

        tokio::select! {
            result = control::listen(Arc::clone(&bridge), control_endpoint, info) => result,
            result = data::listen(Arc::clone(&bridge), data_endpoint) => result,
            result = data::inbound(bridge, net) => result,
        }
    }

    // ── what the loops share ────────────────────────────────────────────────

    /// How this host reaches other devices.
    pub(crate) fn transport(&self) -> &dyn PeerTransport {
        self.transport.as_ref()
    }

    /// The version this bridge reports.
    pub(crate) fn version(&self) -> &'static str {
        self.version
    }

    /// The route for one workspace, if an app server on this host owns it.
    pub(crate) fn route(&self, workspace_id: GrainId) -> Option<Route> {
        self.routes
            .lock()
            .expect("routes")
            .get(workspace_id)
            .cloned()
    }

    /// Every route, ordered by application then workspace.
    pub(crate) fn route_entries(&self) -> Vec<Route> {
        self.routes.lock().expect("routes").entries().to_vec()
    }

    /// Replace every route belonging to `app_name`.
    ///
    /// Refuses a workspace another application already owns, without changing anything.
    pub(crate) fn replace_app(
        &self,
        app_name: &str,
        exe_path: PathBuf,
        managed_by: ManagedBy,
        workspaces: &[WorkspaceRegistration],
    ) -> Result<()> {
        self.routes
            .lock()
            .expect("routes")
            .replace_app(app_name, exe_path, managed_by, workspaces)
    }

    /// Forget one workspace. `false` if it was not there.
    pub(crate) fn remove_route(&self, workspace_id: GrainId) -> Result<bool> {
        self.routes.lock().expect("routes").remove(workspace_id)
    }

    /// The workgroup this host belongs to, if any.
    pub(crate) fn workgroup(&self) -> Result<Option<Workgroup>> {
        Workgroup::open(&self.dir)
    }

    /// Which app servers are connected right now.
    pub(crate) fn owners(&self) -> &Owners {
        &self.owners
    }

    /// Inbound streams waiting for their owner.
    pub(crate) fn tickets(&self) -> &Tickets {
        &self.tickets
    }

    /// When each application was last started by `wake_on_sync`.
    pub(crate) fn wakes(&self) -> &Wakes {
        &self.wakes
    }
}

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
