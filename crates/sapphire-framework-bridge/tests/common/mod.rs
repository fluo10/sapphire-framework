//! Fixtures shared by the bridge's integration tests.
//!
//! Each test file is its own crate, so a fixture one of them does not use is reported as
//! dead code. Reusing one module across `switchboard.rs` and `wake.rs` is what keeps the two
//! from drifting apart, and letting this module hold fixtures only one of them needs yet is
//! what makes that reuse cheap.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use grain_id::GrainId;
use sapphire_bridge_api::{BridgeClient, ManagedBy, RegisterParams, WorkspaceRegistration};
use sapphire_framework_bridge::{Bridge, BridgeDir, LoopbackNetwork, NetConfig, Workgroup};
use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig};

/// The node id of the first host: 64 lowercase hex digits, as the ledger wants them.
pub const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
/// The node id of the second host.
pub const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

/// The application every test's stub app server registers under.
pub const APP: &str = "test-app";

/// How this host's stub app server is started when a peer asks for its workspace.
struct AppServer {
    /// What the registration calls `exe_path`: the executable the bridge runs.
    exe_path: PathBuf,
    /// How that server was started. A `Service` owner is never started by the bridge.
    managed_by: ManagedBy,
}

/// One host: its bridge's directories, its control endpoint, its workgroup, and its app
/// server.
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
    /// How to start this host's stub app server.
    app_server: AppServer,
    /// The app server's control connection, while it is connected.
    ///
    /// [`register_both`] leaves it here rather than handing it back, so that
    /// [`disconnect_owner`] has something to close.
    owner: Mutex<Option<BridgeClient>>,
}

impl Host {
    /// The registration this host's stub app server sends for `workspace_id`.
    fn registration(&self, workspace_id: GrainId) -> RegisterParams {
        RegisterParams {
            app_name: APP.into(),
            exe_path: self.app_server.exe_path.clone(),
            managed_by: self.app_server.managed_by,
            workspaces: vec![WorkspaceRegistration {
                workspace_id,
                root: "/workspace".into(),
            }],
        }
    }

    /// Hold on to an app server's control connection.
    fn stash_owner(&self, client: BridgeClient) {
        *self.owner.lock().expect("owner") = Some(client);
    }

    /// Close the app server's control connection, as if the server had exited.
    fn drop_owner(&self) {
        let owner = self.owner.lock().expect("owner").take();
        drop(owner);
    }
}

/// Start a bridge on `net` as `node_id`, in its own directories, whose app server would be
/// started as `exe`.
///
/// An app server with arguments of its own — everything in `wake.rs` is a shell one-liner —
/// cannot be named by a route: `Route` records an executable and nothing else, and the bridge
/// starts it as `<exe_path> server run`. So `exe args` is written as a small script, and that
/// script is what the route names; the arguments the bridge appends are harmless to it.
pub async fn start_with_exe(
    net: &LoopbackNetwork,
    node_id: &str,
    device_name: &str,
    exe: &str,
    args: &[&str],
    managed_by: ManagedBy,
) -> Host {
    start_with_net(
        net,
        node_id,
        device_name,
        NetConfig::default(),
        exe,
        args,
        managed_by,
    )
    .await
}

/// As [`start_with_exe`], with this host's `net.toml` spelled out.
pub async fn start_with_net(
    net: &LoopbackNetwork,
    node_id: &str,
    device_name: &str,
    config: NetConfig,
    exe: &str,
    args: &[&str],
    managed_by: ManagedBy,
) -> Host {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = tmp.path().join("run");
    std::fs::create_dir_all(&runtime).unwrap();
    let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
    let wg = Workgroup::create(&dir, "test", device_name, node_id).unwrap();
    let exe_path = app_server(exe, args, tmp.path());

    let control = Endpoint::in_dir("bridge", runtime.clone());
    let data = Endpoint::in_dir("bridge-data", runtime.clone());
    let bridge = Bridge::new(dir.clone(), Arc::new(net.transport(node_id)), "0.0.0")
        .unwrap()
        .net(config)
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
        app_server: AppServer {
            exe_path,
            managed_by,
        },
        owner: Mutex::new(None),
    }
}

/// Start a bridge with the default network configuration and no app server to speak of.
pub async fn start(net: &LoopbackNetwork, node_id: &str, device_name: &str) -> Host {
    start_with_exe(
        net,
        node_id,
        device_name,
        "/bin/true",
        &[],
        ManagedBy::Service,
    )
    .await
}

/// Where the bridge runs when a peer asks for this host's workspace.
///
/// Written into the host's own temporary directory, so it lives exactly as long as the test.
#[cfg(unix)]
fn app_server(exe: &str, args: &[&str], dir: &Path) -> PathBuf {
    if args.is_empty() {
        return PathBuf::from(exe);
    }
    let path = dir.join("app-server");
    let quoted: Vec<String> = args.iter().map(|a| shell_quote(a)).collect();
    std::fs::write(
        &path,
        format!("#!/bin/sh\nexec {exe} {}\n", quoted.join(" ")),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// See the unix version: only the unix tests pass app-server arguments, because they are the
/// ones that spawn a shell.
#[cfg(not(unix))]
fn app_server(exe: &str, args: &[&str], _dir: &Path) -> PathBuf {
    debug_assert!(args.is_empty(), "only the unix tests pass arguments");
    PathBuf::from(exe)
}

/// Quote one argument for `/bin/sh`.
#[cfg(unix)]
fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', r"'\''"))
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
    let reg_a = client_a.register(a.registration(ws)).await.unwrap();
    let client_b = connect(b).await;
    let reg_b = client_b.register(b.registration(ws)).await.unwrap();

    // Now each host learns the other's device, the way a sync would teach it.
    introduce(&a.dir, a.workgroup_id, "host-a", &b.dir, b.workgroup_id);
    introduce(&b.dir, b.workgroup_id, "host-b", &a.dir, a.workgroup_id);

    (client_a, client_b, ws, reg_a.device_id, reg_b.device_id)
}

/// As [`pair`], but B's app-server connection is kept inside its `Host` so that a test can
/// call [`disconnect_owner`] afterwards.
///
/// Returns A's client, the workspace, and B's device id.
pub async fn register_both(a: &Host, b: &Host) -> (BridgeClient, GrainId, GrainId) {
    let (client_a, client_b, ws, _device_a, device_b) = pair(a, b).await;
    b.stash_owner(client_b);
    (client_a, ws, device_b)
}

/// Close this host's app-server connection, as if the server had stopped.
///
/// Waits until the bridge agrees the owner is gone. Its **route stays**, which is exactly the
/// state a peer asking for the workspace, and `wake_on_sync`, act on.
pub async fn disconnect_owner(host: &Host) {
    host.drop_owner();
    let client = connect(host).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let offline = client
            .status()
            .await
            .unwrap()
            .routes
            .iter()
            .all(|route| !route.owner_online);
        if offline {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a registration must end with its control connection"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
