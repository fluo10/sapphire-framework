//! Unit tests for start-on-demand, run as an integration test so that
//! `CARGO_BIN_EXE_ipc-test-server` is defined (Cargo does not set it for lib unit
//! tests) and the test-only server binary is built via the `test-util` feature.

use std::time::Duration;

use sapphire_framework_ipc::{ClientInfo, Endpoint, Error, SpawnConfig, ensure_server};

fn info() -> ClientInfo {
    ClientInfo {
        kind: "cli".into(),
        version: "0.0.0".into(),
        pid: std::process::id(),
    }
}

fn test_server(dir: &std::path::Path, app: &str, extra: &[&str]) -> SpawnConfig {
    let mut args = vec![dir.display().to_string(), app.to_owned()];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    SpawnConfig {
        exe: env!("CARGO_BIN_EXE_ipc-test-server").into(),
        args,
        ..SpawnConfig::default()
    }
}

#[tokio::test]
async fn a_server_is_started_when_none_is_listening() {
    let tmp = tempfile::tempdir().unwrap();
    let ep = Endpoint::in_dir("race-app-1", tmp.path().to_path_buf());
    let cfg = test_server(tmp.path(), "race-app-1", &[]);

    let (client, _) = ensure_server(&ep, "race-app-1", info(), &cfg)
        .await
        .unwrap();
    let pong: String = client.call("ping", serde_json::Value::Null).await.unwrap();
    assert_eq!(pong, "pong");
}

#[tokio::test]
async fn a_second_client_reuses_the_running_server() {
    let tmp = tempfile::tempdir().unwrap();
    let ep = Endpoint::in_dir("race-app-2", tmp.path().to_path_buf());
    let cfg = test_server(tmp.path(), "race-app-2", &[]);

    let (first, _) = ensure_server(&ep, "race-app-2", info(), &cfg)
        .await
        .unwrap();
    let (second, _) = ensure_server(&ep, "race-app-2", info(), &cfg)
        .await
        .unwrap();

    let a: u32 = first.call("pid", serde_json::Value::Null).await.unwrap();
    let b: u32 = second.call("pid", serde_json::Value::Null).await.unwrap();
    assert_eq!(a, b, "both clients should have reached the same server");
}

#[tokio::test]
async fn spawning_can_be_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let ep = Endpoint::in_dir("race-app-3", tmp.path().to_path_buf());
    let cfg = SpawnConfig {
        allow_spawn: false,
        ..test_server(tmp.path(), "race-app-3", &[])
    };

    let err = ensure_server(&ep, "race-app-3", info(), &cfg)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Spawn(_)), "got {err:?}");
}

/// A lock left behind by a process that died must not block starts forever.
///
/// `std::fs` cannot backdate a file, so rather than faking the clock the staleness
/// threshold is a field on `SpawnConfig` and the test sets it to zero. Making it
/// configurable is useful anyway: an application that knows its server starts quickly
/// can shorten it.
#[tokio::test]
async fn a_stale_lock_file_does_not_block_a_start() {
    let tmp = tempfile::tempdir().unwrap();
    let ep = Endpoint::in_dir("race-app-4", tmp.path().to_path_buf());
    std::fs::write(ep.lock_path(), "999999").unwrap();

    let cfg = SpawnConfig {
        stale_lock_age: Duration::ZERO,
        ..test_server(tmp.path(), "race-app-4", &[])
    };
    let (client, _) = ensure_server(&ep, "race-app-4", info(), &cfg)
        .await
        .unwrap();
    let pong: String = client.call("ping", serde_json::Value::Null).await.unwrap();
    assert_eq!(pong, "pong");
}
