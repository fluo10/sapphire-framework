//! The host-wide sapphire daemon.
//!
//! Running it with no subcommand starts the bridge; every other subcommand is a one-shot
//! command against a running one.

use clap::Parser;
use sapphire_bridge::BridgeCommand;

#[derive(Parser)]
#[command(name = "sapphire-bridge", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<BridgeCommand>,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sapphire_framework_bridge=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or(BridgeCommand::Run);
    match command.dispatch(env!("CARGO_PKG_VERSION")).await {
        Ok(code) => std::process::ExitCode::from(code as u8),
        Err(err) => {
            eprintln!("sapphire-bridge: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}
