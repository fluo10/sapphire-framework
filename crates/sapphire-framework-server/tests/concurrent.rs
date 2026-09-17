//! Several client processes using one workspace at the same time.
//!
//! This is the problem the process architecture exists to solve: a redb database takes an
//! exclusive file lock, so two processes that open an app's cache directly cannot both run.
//! `first_pins_the_problem` states that; the rest show it gone.

use std::path::{Path, PathBuf};

use sapphire_backend::protocol as proto;
use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig, ensure_server};

fn client_info() -> ClientInfo {
    ClientInfo {
        kind: "cli".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        pid: std::process::id(),
    }
}

fn fixture(tmp: &Path) -> (Endpoint, PathBuf, SpawnConfig) {
    let runtime = tmp.join("run");
    let state = tmp.join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let root = tmp.join("ws");
    std::fs::create_dir_all(root.join(".sapphire-servertest")).unwrap();
    let root = root.canonicalize().unwrap();

    let endpoint = Endpoint::in_dir("sapphire-servertest", runtime.clone());
    let spawn = SpawnConfig {
        exe: env!("CARGO_BIN_EXE_server-test-app").into(),
        args: vec![runtime.display().to_string(), state.display().to_string()],
        ..SpawnConfig::default()
    };
    (endpoint, root, spawn)
}

/// The behaviour that forced this design: a second direct open of the cache fails.
#[test]
fn first_pins_the_problem() {
    use sapphire_workspace::{AppContext, AppKind, Workspace, WorkspaceState};

    static CTX: AppContext = AppContext::new("sapphire-lockproof");

    let tmp = tempfile::tempdir().unwrap();
    // SAFETY: set before any other thread reads the environment in this test binary.
    unsafe {
        std::env::set_var("SAPPHIRE_LOCKPROOF_CACHE_DIR", tmp.path().join("cache"));
        std::env::set_var("SAPPHIRE_LOCKPROOF_DATA_DIR", tmp.path().join("data"));
        std::env::set_var("SAPPHIRE_LOCKPROOF_CONFIG_DIR", tmp.path().join("config"));
    }
    CTX.init(AppKind::Server);

    let root = tmp.path().join("ws");
    std::fs::create_dir_all(root.join(".sapphire-lockproof")).unwrap();

    let first = WorkspaceState::open(Workspace::from_root(&CTX, &root).unwrap()).unwrap();
    let second = WorkspaceState::open(Workspace::from_root(&CTX, &root).unwrap());
    assert!(
        second.is_err(),
        "if this ever passes, the premise of the process architecture has changed"
    );
    drop(first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_clients_all_write_successfully() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path());

    let mut tasks = Vec::new();
    for n in 0..8u32 {
        let (endpoint, ws, spawn) = (endpoint.clone(), ws.clone(), spawn.clone());
        tasks.push(tokio::spawn(async move {
            let (client, _) =
                ensure_server(&endpoint, "sapphire-servertest", client_info(), &spawn)
                    .await
                    .expect("a server");
            let _: proto::Ack = client
                .call(
                    proto::WRITE_FILE,
                    proto::ContentParams {
                        ws,
                        path: PathBuf::from(format!("note-{n}.md")),
                        content: format!("written by client {n}"),
                    },
                )
                .await
                .expect("the write");
        }));
    }
    for task in tasks {
        task.await.expect("the task");
    }

    for n in 0..8u32 {
        assert!(
            ws.join(format!("note-{n}.md")).exists(),
            "client {n}'s file is missing"
        );
    }
}

/// The original report: `journal add` while the stdio MCP server is running.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_lived_client_and_a_one_shot_client_coexist() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path());

    // The MCP server: connects and stays.
    let (long_lived, _) = ensure_server(&endpoint, "sapphire-servertest", client_info(), &spawn)
        .await
        .unwrap();
    let _: proto::Ack = long_lived
        .call(
            proto::WRITE_FILE,
            proto::ContentParams {
                ws: ws.clone(),
                path: PathBuf::from("from-mcp.md"),
                content: "agent".into(),
            },
        )
        .await
        .unwrap();

    // The CLI: connects, writes, goes away.
    {
        let (one_shot, _) = ensure_server(&endpoint, "sapphire-servertest", client_info(), &spawn)
            .await
            .unwrap();
        let _: proto::Ack = one_shot
            .call(
                proto::WRITE_FILE,
                proto::ContentParams {
                    ws: ws.clone(),
                    path: PathBuf::from("from-cli.md"),
                    content: "human".into(),
                },
            )
            .await
            .unwrap();
    }

    // The long-lived client still works after the other one left.
    let read: proto::ReadResult = long_lived
        .call(
            proto::READ_FILE,
            proto::PathParams {
                ws,
                path: PathBuf::from("from-cli.md"),
            },
        )
        .await
        .unwrap();
    assert_eq!(read.content, "human");
}
