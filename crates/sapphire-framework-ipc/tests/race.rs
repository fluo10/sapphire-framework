//! Several clients starting one server at the same time (spec §2.6, steps 3a-3b).

use sapphire_framework_ipc::{ClientInfo, Endpoint, SpawnConfig, ensure_server};

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
