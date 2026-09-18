//! The `server` subcommands an application flattens into its own CLI.

use sapphire_ipc::{ClientInfo, Endpoint, SpawnConfig, ensure_server};

use crate::AppServer;
use crate::error::Result;
use crate::privilege::PrivilegeConfig;

/// Subcommands for managing this application's server.
#[derive(Debug, clap::Subcommand)]
pub enum ServerCommand {
    /// Run the server in this process.
    Run(RunArgs),
    /// Report whether a server is running, and which version.
    Status,
    /// Ask a running server to exit.
    Stop,
}

/// Arguments of `server run`.
#[derive(Debug, clap::Args)]
pub struct RunArgs {
    /// Stay in the foreground and never exit on idle.
    ///
    /// Without it the server exits once nothing has used it for a while, which is what a
    /// server started on demand by a CLI should do.
    #[arg(long)]
    pub foreground: bool,
}

impl ServerCommand {
    /// Carry out the command, returning the process exit code.
    pub async fn dispatch(self, server: AppServer, app: &str, version: &str) -> Result<i32> {
        match self {
            ServerCommand::Run(args) => {
                let server = if args.foreground {
                    server.idle_exit(None)
                } else {
                    server
                };
                server.run().await?;
                Ok(0)
            }
            ServerCommand::Status => status(&Endpoint::for_app(app)?, app, version).await,
            ServerCommand::Stop => stop(&Endpoint::for_app(app)?, app, version).await,
        }
    }
}

/// The [`SpawnConfig`] an application's CLI should use.
///
/// An application configured for privilege separation runs its server as root, and a CLI
/// running as the human user cannot start one (spec §2.6, §3). Saying so up front is much
/// clearer than letting the spawn fail somewhere inside the service manager's territory.
pub fn spawn_config_for(privileges: Option<&PrivilegeConfig>) -> SpawnConfig {
    match privileges {
        Some(_) => SpawnConfig::disabled(),
        None => SpawnConfig::default(),
    }
}

fn client_info(version: &str) -> ClientInfo {
    ClientInfo {
        kind: "cli".to_owned(),
        version: version.to_owned(),
        pid: std::process::id(),
    }
}

async fn status(endpoint: &Endpoint, app: &str, version: &str) -> Result<i32> {
    if !sapphire_ipc::probe(endpoint).await? {
        println!("no {app} server is running");
        return Ok(1);
    }
    let (_, info) = ensure_server(
        endpoint,
        app,
        client_info(version),
        &SpawnConfig::disabled(),
    )
    .await?;
    println!(
        "{app} server running: version {}, pid {}, started as {:?}",
        info.version, info.pid, info.managed_by
    );
    Ok(0)
}

async fn stop(endpoint: &Endpoint, app: &str, version: &str) -> Result<i32> {
    if !sapphire_ipc::probe(endpoint).await? {
        println!("no {app} server is running");
        return Ok(1);
    }
    let (client, info) = ensure_server(
        endpoint,
        app,
        client_info(version),
        &SpawnConfig::disabled(),
    )
    .await?;
    let _: serde_json::Value = client
        .call(sapphire_ipc::SHUTDOWN_METHOD, serde_json::json!({}))
        .await?;
    println!("asked the {app} server (pid {}) to exit", info.pid);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(subcommand)]
        server: ServerCommand,
    }

    #[test]
    fn the_subcommands_parse() {
        assert!(matches!(
            Probe::try_parse_from(["app", "run"]).unwrap().server,
            ServerCommand::Run(_)
        ));
        assert!(matches!(
            Probe::try_parse_from(["app", "status"]).unwrap().server,
            ServerCommand::Status
        ));
        assert!(matches!(
            Probe::try_parse_from(["app", "stop"]).unwrap().server,
            ServerCommand::Stop
        ));
    }

    #[test]
    fn run_takes_a_foreground_flag() {
        let parsed = Probe::try_parse_from(["app", "run", "--foreground"]).unwrap();
        match parsed.server {
            ServerCommand::Run(args) => assert!(args.foreground),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn status_reports_no_server_when_none_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = sapphire_ipc::Endpoint::in_dir("status-test", tmp.path().to_path_buf());
        let code = status(&endpoint, "status-test", "0.0.0").await.unwrap();
        assert_eq!(code, 1, "no server is a non-zero exit");
    }

    #[tokio::test]
    async fn stop_reports_no_server_when_none_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = sapphire_ipc::Endpoint::in_dir("stop-test", tmp.path().to_path_buf());
        let code = stop(&endpoint, "stop-test", "0.0.0").await.unwrap();
        assert_eq!(code, 1);
    }
}

#[cfg(test)]
mod spawn_policy_tests {
    use super::*;
    use crate::privilege::{PrivilegeConfig, UserSpec};

    #[test]
    fn an_ordinary_app_may_start_its_own_server() {
        assert!(spawn_config_for(None).allow_spawn);
    }

    #[test]
    fn a_privilege_separated_app_may_not() {
        let config = PrivilegeConfig {
            run_as: UserSpec::Name("alice".into()),
            helper: None,
        };
        assert!(!spawn_config_for(Some(&config)).allow_spawn);
    }

    #[tokio::test]
    async fn the_error_says_to_start_the_service() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = sapphire_ipc::Endpoint::in_dir("privsep-cli-test", tmp.path().to_path_buf());
        let config = PrivilegeConfig {
            run_as: UserSpec::Name("alice".into()),
            helper: None,
        };

        let err = sapphire_ipc::ensure_server(
            &endpoint,
            "privsep-cli-test",
            sapphire_ipc::ClientInfo {
                kind: "cli".into(),
                version: "0.0.0".into(),
                pid: std::process::id(),
            },
            &spawn_config_for(Some(&config)),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("not allowed to start one"),
            "{err}"
        );
    }
}
