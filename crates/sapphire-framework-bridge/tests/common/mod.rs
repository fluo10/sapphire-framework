//! Fixtures shared by the bridge's integration tests.
//!
//! Each test file is its own crate, so a fixture one of them does not use is reported as
//! dead code. Reusing one module across `switchboard.rs` and `wake.rs` is what keeps the two
//! from drifting apart, and letting this module hold fixtures only one of them needs yet is
//! what makes that reuse cheap.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_bridge_api::{BridgeClient, ManagedBy, RegisterParams, WorkspaceRegistration};
use sapphire_framework_bridge::{Bridge, BridgeDir, LoopbackNetwork, Workgroup};
use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig};

/// The node id of the first host: 64 lowercase hex digits, as the ledger wants them.
pub const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
/// The node id of the second host.
pub const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

/// One host: its bridge's directories, its control endpoint, and its workgroup.
pub struct Host {
    /// Held so the directories outlive the test.
    pub tmp: tempfile::TempDir,
    /// This host's bridge directory.
    pub dir: BridgeDir,
    /// The bridge's control endpoint on this host.
    pub control: Endpoint,
    /// The runtime directory both of this host's endpoints live in.
    pub runtime: PathBuf,
    /// The workgroup this host joined.
    pub workgroup_id: GrainId,
}

/// Start a bridge on `net` as `node_id`, in its own directories.
pub async fn start(net: &LoopbackNetwork, node_id: &str, device_name: &str) -> Host {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = tmp.path().join("run");
    std::fs::create_dir_all(&runtime).unwrap();
    let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
    let wg = Workgroup::create(&dir, "test", device_name, node_id).unwrap();

    let control = Endpoint::in_dir("bridge", runtime.clone());
    let data = Endpoint::in_dir("bridge-data", runtime.clone());
    let bridge = Bridge::new(dir.clone(), Arc::new(net.transport(node_id)), "0.0.0")
        .unwrap()
        .control_endpoint(control.clone())
        .data_endpoint(data);
    tokio::spawn(async move {
        let _ = bridge.run().await;
    });

    // Wait for the control endpoint to come up.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !sapphire_ipc::probe(&control).await.unwrap_or(false) {
        assert!(
            std::time::Instant::now() < deadline,
            "the bridge never started"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    Host {
        tmp,
        dir,
        control,
        runtime,
        workgroup_id: wg.id,
    }
}

/// Who a test's connection says it is.
pub fn client_info() -> ClientInfo {
    ClientInfo {
        kind: "test".into(),
        version: "0.0.0".into(),
        pid: std::process::id(),
    }
}

/// Connect to a host's bridge.
pub async fn connect(host: &Host) -> BridgeClient {
    let (client, _) = sapphire_ipc::ensure_server(
        &host.control,
        "bridge",
        client_info(),
        &SpawnConfig::disabled(),
    )
    .await
    .unwrap();
    BridgeClient::from_client(Arc::new(client), host.runtime.clone())
}

/// Give `to` the device record `name` holds in `from`'s ledger — **the same record**.
///
/// The ledger is synced, so a device travels to another host with its id intact; the id is
/// both the record's filename and the `Entry.author` written into synced content, so it must
/// survive. Copying the record says exactly that, and it is what makes the two hosts agree
/// about a device.
///
/// [`sapphire_registry::Devices::add`] cannot stand in for this: it invents a fresh id per
/// ledger, so two hosts that each `add`ed a device named "host-a" would hold two different
/// devices — and an app server's `device_id`, which comes from its own host's ledger, would
/// name a device the other host has never heard of.
///
/// Returns the id both hosts now share.
pub fn introduce(
    from: &BridgeDir,
    from_workgroup: GrainId,
    name: &str,
    to: &BridgeDir,
    to_workgroup: GrainId,
) -> GrainId {
    let devices = sapphire_registry::Devices::open(&from.devices_dir(from_workgroup)).unwrap();
    let device = devices.resolve(name).unwrap().clone();

    let source = from.devices_dir(from_workgroup).join(device.file_name());
    let destination = to.devices_dir(to_workgroup).join(device.file_name());
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    std::fs::copy(&source, &destination).unwrap();
    device.id
}

/// Register the same workspace on both hosts and give each the other's device record.
///
/// Returns both clients, the workspace, A's device id, and B's.
pub async fn pair(a: &Host, b: &Host) -> (BridgeClient, BridgeClient, GrainId, GrainId, GrainId) {
    let ws = GrainId::random();

    // Registration comes first, and `Workgroup::this_device` reports the first record by
    // name — so an app server registering before the two hosts learn about each other gets
    // its own device. That ordering is load-bearing until `this_device` matches on node id
    // (see the TODO on it in `workgroup.rs`); `switchboard.rs` and `wake.rs` both rely on it.
    let client_a = connect(a).await;
    let reg_a = client_a
        .register(RegisterParams {
            app_name: "test-app".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration {
                workspace_id: ws,
                root: "/a".into(),
            }],
        })
        .await
        .unwrap();
    let client_b = connect(b).await;
    let reg_b = client_b
        .register(RegisterParams {
            app_name: "test-app".into(),
            exe_path: "/bin/true".into(),
            managed_by: ManagedBy::Service,
            workspaces: vec![WorkspaceRegistration {
                workspace_id: ws,
                root: "/b".into(),
            }],
        })
        .await
        .unwrap();

    // Now each host learns the other's device, the way a sync would teach it.
    introduce(&a.dir, a.workgroup_id, "host-a", &b.dir, b.workgroup_id);
    introduce(&b.dir, b.workgroup_id, "host-b", &a.dir, a.workgroup_id);

    (client_a, client_b, ws, reg_a.device_id, reg_b.device_id)
}
