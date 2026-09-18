//! The `sapphire-bridge` command line.
//!
//! Running it with no subcommand starts the bridge; every other subcommand is a one-shot
//! command against a running one. Two of them work directly on the bridge directory instead —
//! [`WorkgroupCommand::Create`], because there is nothing to ask about a workgroup that does
//! not exist yet, and [`DeviceCommand::Forget`], because the control plane has no method for
//! it. Both then see their effect immediately: the ledger is re-read on every authorization.

// The transport is only assembled behind the `node` feature; the imports that build it are
// gated with it so a build without the feature stays warning-free.
#[cfg(feature = "node")]
use std::sync::Arc;

use sapphire_bridge_api::{BRIDGE_NAME, BridgeClient};
use sapphire_ipc::{Endpoint, SpawnConfig};

#[cfg(feature = "node")]
use crate::NetConfig;
use crate::error::{Error, Result};
use crate::workgroup::Workgroup;
use crate::{Bridge, BridgeDir, InstanceLock};

/// The bridge daemon's subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum BridgeCommand {
    /// Run the bridge in this process.
    Run,
    /// Report whether a bridge is running, and what it knows.
    Status,
    /// The workgroup's devices.
    Device {
        /// Which `device` command to run.
        #[command(subcommand)]
        command: DeviceCommand,
    },
    /// This host's workgroup.
    Workgroup {
        /// Which `workgroup` command to run.
        #[command(subcommand)]
        command: WorkgroupCommand,
    },
    /// What the workgroup contains — read-only.
    Workspace {
        /// Which `workspace` command to run.
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
}

/// `device` subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum DeviceCommand {
    /// List the workgroup's devices, and which are reachable.
    List,
    /// Retire a device, so it may no longer connect.
    ///
    /// The record stays as a tombstone: a device id is written into synced content and must
    /// keep resolving.
    Forget {
        /// The device's name or id.
        selector: String,
    },
}

/// `workgroup` subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum WorkgroupCommand {
    /// Found a workgroup on this host.
    Create {
        /// The workgroup's name.
        name: String,
        /// This host's device name inside it.
        #[arg(long)]
        device_name: String,
    },
    /// Show the workgroup this host belongs to.
    List,
}

/// `workspace` subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum WorkspaceCommand {
    /// List the workspaces this host serves.
    List,
}

impl BridgeCommand {
    /// Carry out the command, returning the process exit code.
    pub async fn dispatch(self, version: &'static str) -> Result<i32> {
        match self {
            BridgeCommand::Run => run(version).await,
            BridgeCommand::Status => status(version).await,
            BridgeCommand::Device { command } => match command {
                DeviceCommand::List => device_list(version).await,
                DeviceCommand::Forget { selector } => device_forget(&selector),
            },
            BridgeCommand::Workgroup { command } => match command {
                WorkgroupCommand::Create { name, device_name } => {
                    workgroup_create(&name, &device_name)
                }
                WorkgroupCommand::List => workgroup_list(),
            },
            BridgeCommand::Workspace { command } => match command {
                WorkspaceCommand::List => workspace_list(version).await,
            },
        }
    }
}

// ── run ─────────────────────────────────────────────────────────────────────

/// Start the bridge, or report the one already running.
async fn run(version: &'static str) -> Result<i32> {
    let dir = BridgeDir::open()?;
    // Held for as long as the bridge runs; dropping it frees the next start. The lock is
    // taken before anything reads `net.toml` or `node.key`, so a directory of a newer format
    // stops us here rather than after a new identity has been minted (see `BridgeDir::at`).
    let _lock = match InstanceLock::acquire(&dir) {
        Ok(lock) => lock,
        // Not an error: starting the bridge twice is a normal thing to do by accident, and
        // the answer is the pid of the one that is already there.
        Err(Error::AlreadyRunning(pid)) => {
            println!("the bridge is already running (pid {pid})");
            return Ok(1);
        }
        Err(err) => return Err(err),
    };

    let bridge = build_bridge(dir, version).await?;
    bridge.run().await?;
    Ok(0)
}

/// The bridge this host should run: the iroh transport, over the directory's configuration.
#[cfg(feature = "node")]
async fn build_bridge(dir: BridgeDir, version: &'static str) -> Result<Bridge> {
    // Read once, and hand the same configuration to the bridge: it would otherwise read
    // `net.toml` again when it starts its loops, and the two reads could disagree.
    let net = NetConfig::load(&dir.net_toml())?;
    let transport = Arc::new(crate::iroh::IrohTransport::new(&dir.node_key(), &net).await?);
    Ok(Bridge::new(dir, transport, version)?.net(net))
}

/// Without the `node` feature there is no transport to reach other devices with.
#[cfg(not(feature = "node"))]
async fn build_bridge(_dir: BridgeDir, _version: &'static str) -> Result<Bridge> {
    Err(Error::Config(
        "this build has no peer transport; rebuild sapphire-bridge with the `node` feature"
            .to_owned(),
    ))
}

// ── one-shot commands ───────────────────────────────────────────────────────

/// Connect to the running bridge, or `None` when none is listening.
///
/// Never starts one: asking a question about the bridge must not bring a daemon up.
async fn connect(version: &str) -> Result<Option<BridgeClient>> {
    let endpoint = Endpoint::in_dir(BRIDGE_NAME, sapphire_ipc::runtime_dir()?);
    if !sapphire_ipc::probe(&endpoint).await? {
        return Ok(None);
    }
    Ok(Some(
        BridgeClient::connect("cli", version, &SpawnConfig::disabled()).await?,
    ))
}

/// Report what the running bridge knows about itself.
async fn status(version: &str) -> Result<i32> {
    let Some(client) = connect(version).await? else {
        println!("no bridge is running");
        return Ok(1);
    };
    let status = client.status().await?;

    println!(
        "sapphire-bridge {} (node {}, {} workspace(s))",
        status.version,
        status.node_id,
        status.routes.len()
    );
    match status.workgroup {
        Some(workgroup) => println!(
            "workgroup {} ({}), {} device(s)",
            workgroup.name, workgroup.workgroup_id, workgroup.devices
        ),
        None => println!("no workgroup"),
    }
    for route in status.routes {
        println!(
            "  {} {} {}{}",
            route.workspace_id,
            route.app_name,
            route.root.display(),
            if route.owner_online { " (online)" } else { "" }
        );
    }
    Ok(0)
}

/// List the workgroup's devices, as the running bridge sees them.
async fn device_list(version: &str) -> Result<i32> {
    let Some(client) = connect(version).await? else {
        println!("no bridge is running");
        return Ok(1);
    };
    let peers = client.peers().await?;
    if peers.peers.is_empty() {
        println!("no devices");
        return Ok(0);
    }
    for peer in peers.peers {
        println!(
            "{} {} {}{}",
            peer.name,
            peer.device_id,
            if peer.node_id.is_empty() {
                "-".to_owned()
            } else {
                peer.node_id
            },
            if peer.connected { " (online)" } else { "" }
        );
    }
    Ok(0)
}

/// Retire a device in the ledger.
///
/// Works directly on the bridge directory: the control plane has no `forget` method, and the
/// ledger is a directory this user already owns. Retirement takes effect at once because the
/// bridge re-reads the ledger on every authorization.
fn device_forget(selector: &str) -> Result<i32> {
    let dir = BridgeDir::open()?;
    let Some(workgroup) = Workgroup::open(&dir)? else {
        println!("this host has not joined a workgroup");
        return Ok(1);
    };
    let mut devices = workgroup.devices()?;
    let device = devices.retire(selector)?;
    println!("retired device {} ({})", device.name, device.id);
    Ok(0)
}

/// Found a workgroup, recording this host as its first device.
///
/// Works directly on the directory: nothing can be asked about a workgroup that does not
/// exist yet.
fn workgroup_create(name: &str, device_name: &str) -> Result<i32> {
    let dir = BridgeDir::open()?;
    // The directory is opened first, so a format we do not understand stops us before a new
    // identity is minted.
    let node_id = this_node_id(&dir)?;
    let workgroup = Workgroup::create(&dir, name, device_name, &node_id)?;
    println!(
        "created workgroup {} ({}); this device is {} ({})",
        workgroup.name,
        workgroup.id,
        device_name,
        workgroup.this_device()?.id
    );
    Ok(0)
}

/// Show the workgroup this host belongs to.
fn workgroup_list() -> Result<i32> {
    let dir = BridgeDir::open()?;
    match Workgroup::open(&dir)? {
        Some(workgroup) => {
            println!("{} ({})", workgroup.name, workgroup.id);
            Ok(0)
        }
        None => {
            println!("this host has not joined a workgroup");
            Ok(1)
        }
    }
}

/// List the workspaces this host serves.
///
/// Read-only by design (spec §1): the bridge knows what exists, and the app that owns a
/// workspace decides what this host keeps. The workgroup-wide list arrives with the workgroup
/// workspace itself; until then this is what the bridge's routing table holds.
async fn workspace_list(version: &str) -> Result<i32> {
    let Some(client) = connect(version).await? else {
        println!("no bridge is running");
        return Ok(1);
    };
    let status = client.status().await?;
    if status.routes.is_empty() {
        println!("no workspaces are registered on this host");
        return Ok(0);
    }
    for route in status.routes {
        println!(
            "{} {} {}{}",
            route.workspace_id,
            route.app_name,
            route.root.display(),
            if route.owner_online { " (online)" } else { "" }
        );
    }
    Ok(0)
}

/// This host's node id, from `node.key`, creating the key if this is the first time.
#[cfg(feature = "node")]
fn this_node_id(dir: &BridgeDir) -> Result<String> {
    crate::iroh::load_or_create_node_id(&dir.node_key())
}

/// Without the `node` feature there is no key to derive a node id from.
#[cfg(not(feature = "node"))]
fn this_node_id(_dir: &BridgeDir) -> Result<String> {
    Err(Error::Config(
        "this build has no peer transport; rebuild sapphire-bridge with the `node` feature"
            .to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(subcommand)]
        command: BridgeCommand,
    }

    #[test]
    fn the_subcommands_parse() {
        for args in [
            vec!["b", "run"],
            vec!["b", "status"],
            vec!["b", "device", "list"],
            vec!["b", "device", "forget", "phone"],
            vec![
                "b",
                "workgroup",
                "create",
                "home",
                "--device-name",
                "laptop",
            ],
            vec!["b", "workgroup", "list"],
            vec!["b", "workspace", "list"],
        ] {
            assert!(Probe::try_parse_from(&args).is_ok(), "{args:?}");
        }
    }

    #[test]
    fn there_is_no_way_to_map_a_workspace_from_here() {
        // Placing a workspace on this host is the owning application's business (spec §1).
        assert!(Probe::try_parse_from(["b", "workspace", "map", "notes", "/tmp/x"]).is_err());
    }

    #[test]
    fn pairing_is_not_here_yet() {
        assert!(Probe::try_parse_from(["b", "pair", "create"]).is_err());
    }

    #[tokio::test]
    async fn status_against_no_bridge_exits_non_zero() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: set before any other thread reads the environment in this test binary.
        unsafe { std::env::set_var("SAPPHIRE_RUNTIME_DIR", tmp.path()) };
        let code = BridgeCommand::Status.dispatch("0.0.0").await.unwrap();
        unsafe { std::env::remove_var("SAPPHIRE_RUNTIME_DIR") };
        assert_eq!(code, 1);
    }
}
