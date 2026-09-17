//! Several clients starting one server at the same time (spec §2.6, steps 3a-3b).

use sapphire_framework_ipc::{ClientInfo, Endpoint, SHUTDOWN_METHOD, SpawnConfig, ensure_server};

fn client_info() -> ClientInfo {
    ClientInfo {
        kind: "cli".into(),
        version: "0.0.0".into(),
        pid: std::process::id(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_simultaneous_clients_produce_one_server() {
    let tmp = tempfile::tempdir().unwrap();
    let app = "race-integration";
    let endpoint = Endpoint::in_dir(app, tmp.path().to_path_buf());
    let config = SpawnConfig {
        exe: env!("CARGO_BIN_EXE_ipc-test-server").into(),
        args: vec![tmp.path().display().to_string(), app.to_owned()],
        ..SpawnConfig::default()
    };

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (endpoint, config) = (endpoint.clone(), config.clone());
        tasks.push(tokio::spawn(async move {
            let (client, _) = ensure_server(&endpoint, app, client_info(), &config)
                .await
                .expect("a server");
            client
                .call::<_, u32>("pid", serde_json::Value::Null)
                .await
                .expect("a pid")
        }));
    }

    let mut pids = Vec::new();
    for task in tasks {
        pids.push(task.await.expect("the task"));
    }

    let first = pids[0];
    assert!(
        pids.iter().all(|pid| *pid == first),
        "every client should have reached one server, got {pids:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_managed_server_of_another_version_is_not_replaced() {
    let tmp = tempfile::tempdir().unwrap();
    let app = "service-version";
    let endpoint = Endpoint::in_dir(app, tmp.path().to_path_buf());
    let exe: std::path::PathBuf = env!("CARGO_BIN_EXE_ipc-test-server").into();

    // Start a "service" server claiming version 9.9.9.
    let mut child = tokio::process::Command::new(&exe)
        .args([
            tmp.path().display().to_string(),
            app.to_owned(),
            "--version".to_owned(),
            "9.9.9".to_owned(),
            "--service".to_owned(),
        ])
        .spawn()
        .expect("the test server");

    // Wait for it to listen.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !sapphire_framework_ipc::probe(&endpoint)
        .await
        .unwrap_or(false)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the server never started"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let config = SpawnConfig {
        exe,
        args: vec![tmp.path().display().to_string(), app.to_owned()],
        ..SpawnConfig::default()
    };
    let err = ensure_server(&endpoint, app, client_info(), &config)
        .await
        .expect_err("a version mismatch against a service");
    let message = err.to_string();
    assert!(message.contains("restart the service"), "{message}");

    child.kill().await.ok();
}

/// A *spawned* server of the wrong version is retired and a new one started in its
/// place (spec §2.6): the running server is asked to exit, a fresh one answers, and
/// the old process is gone.
#[tokio::test(flavor = "multi_thread")]
async fn a_spawned_server_of_another_version_is_replaced() {
    let tmp = tempfile::tempdir().unwrap();
    let app = "spawned-version";
    let endpoint = Endpoint::in_dir(app, tmp.path().to_path_buf());
    let exe: std::path::PathBuf = env!("CARGO_BIN_EXE_ipc-test-server").into();

    // Start a spawned server claiming version 9.9.9. `--shutdownable` gives it the
    // `server.shutdown` method a real spawned server implements.
    let mut child = tokio::process::Command::new(&exe)
        .args([
            tmp.path().display().to_string(),
            app.to_owned(),
            "--version".to_owned(),
            "9.9.9".to_owned(),
            "--shutdownable".to_owned(),
        ])
        .spawn()
        .expect("the test server");

    // Wait for it to listen.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !sapphire_framework_ipc::probe(&endpoint)
        .await
        .unwrap_or(false)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the server never started"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // A client of version 0.0.0 must replace it. The replacement is started with the
    // same args, so it too serves `server.shutdown` and can be retired below.
    let config = SpawnConfig {
        exe: exe.clone(),
        args: vec![
            tmp.path().display().to_string(),
            app.to_owned(),
            "--shutdownable".to_owned(),
        ],
        ..SpawnConfig::default()
    };
    let (client, info) = ensure_server(&endpoint, app, client_info(), &config)
        .await
        .expect("a replacement server");
    assert_eq!(info.version, "0.0.0", "a server of our version answers");
    let new_pid: u32 = client
        .call("pid", serde_json::Value::Null)
        .await
        .expect("a pid from the replacement");
    assert_eq!(
        new_pid, info.pid,
        "the pid endpoint agrees with the welcome"
    );
    assert_ne!(
        new_pid,
        child.id().expect("the old server still runs"),
        "the old server must have been replaced by a new process"
    );

    // The old process exits after being asked to.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while child.try_wait().expect("the old server's status").is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the replaced server never exited"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Tidy up: the replacement is detached, so retire it the way a client would.
    let _: std::result::Result<serde_json::Value, _> =
        client.call(SHUTDOWN_METHOD, serde_json::Value::Null).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while sapphire_framework_ipc::probe(&endpoint)
        .await
        .unwrap_or(false)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the replacement server never exited"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
