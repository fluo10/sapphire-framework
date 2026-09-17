//! `IpcBackend` must behave like `LocalBackend`, because the UI cannot tell them apart.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sapphire_backend::{IpcBackend, SearchMode, WorkspaceBackend};
use sapphire_ipc::{Endpoint, SpawnConfig};

/// Where `cargo test` put the test server binary.
///
/// Cargo only defines `CARGO_BIN_EXE_*` for binaries of the package being tested, and
/// `server-test-app` belongs to the dev-dependency `sapphire-framework-server`, so the
/// variable is not set here. The binary is still built — the dev-dependency triggers it —
/// next to this test harness: the harness runs from `{target}/{profile}/deps/`, so the
/// profile directory one level up holds `server-test-app`.
fn server_test_app_exe() -> PathBuf {
    if let Some(exe) = std::env::var_os("CARGO_BIN_EXE_server-test-app") {
        return PathBuf::from(exe);
    }
    let deps_dir = std::env::current_exe()
        .expect("the test binary's path")
        .parent()
        .expect("deps directory")
        .to_path_buf();
    deps_dir
        .parent()
        .expect("profile directory")
        .join("server-test-app")
}

/// Start the test application server from `sapphire-framework-server`, and return the
/// endpoint plus a workspace root inside it.
async fn fixture(tmp: &Path) -> (Endpoint, PathBuf, SpawnConfig) {
    let runtime = tmp.join("run");
    let state = tmp.join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let root = tmp.join("ws");
    std::fs::create_dir_all(root.join(".sapphire-servertest")).unwrap();
    let root = root.canonicalize().unwrap();

    let endpoint = Endpoint::in_dir("sapphire-servertest", runtime.clone());
    let spawn = SpawnConfig {
        exe: server_test_app_exe(),
        args: vec![runtime.display().to_string(), state.display().to_string()],
        ..SpawnConfig::default()
    };
    (endpoint, root, spawn)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ipc_backend_reads_back_what_it_wrote() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path()).await;

    let backend = IpcBackend::connect(
        &endpoint,
        "sapphire-servertest",
        "test",
        env!("CARGO_PKG_VERSION"),
        &spawn,
        ws,
    )
    .await
    .unwrap();

    backend
        .write_file(Path::new("a.md"), "# hello")
        .await
        .unwrap();
    assert_eq!(
        backend.read_file(Path::new("a.md")).await.unwrap(),
        "# hello"
    );

    let hits = backend.search("hello", 10, SearchMode::Fts).await.unwrap();
    assert!(hits.iter().any(|h| h.path.ends_with("a.md")), "{hits:?}");

    let listing = backend.list_dir(Path::new(".")).await.unwrap();
    assert!(
        listing
            .iter()
            .any(|(p, is_dir)| !is_dir && p.ends_with("a.md"))
    );

    backend.delete_file(Path::new("a.md")).await.unwrap();
    assert!(backend.read_file(Path::new("a.md")).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn events_reach_a_subscriber_of_an_ipc_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let (endpoint, ws, spawn) = fixture(tmp.path()).await;

    let backend = Arc::new(
        IpcBackend::connect(
            &endpoint,
            "sapphire-servertest",
            "test",
            env!("CARGO_PKG_VERSION"),
            &spawn,
            ws,
        )
        .await
        .unwrap(),
    );

    // `subscribe` is synchronous, so asking the server to start sending is a separate call.
    backend.start_events().await.unwrap();
    let mut events = backend.subscribe();
    backend.write_file(Path::new("a.md"), "x").await.unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), events.recv())
        .await
        .expect("an event within ten seconds")
        .unwrap();
    assert!(
        matches!(event, sapphire_backend::BackendEvent::FileChanged { .. }),
        "{event:?}"
    );
}
