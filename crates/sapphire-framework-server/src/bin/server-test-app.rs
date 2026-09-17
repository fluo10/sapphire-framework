//! A minimal application server used by the integration tests.
//!
//! Usage: `server-test-app <runtime-dir> <state-dir>`
//!
//! Serves the framework's `workspace.*` namespace and nothing else. `<state-dir>` becomes
//! the application's cache, data and config root.

use sapphire_framework_server::AppServer;
use sapphire_ipc::{Endpoint, ManagedBy};
use sapphire_workspace::{AppContext, AppKind};

static CTX: AppContext = AppContext::new("sapphire-servertest");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let runtime_dir = args.next().expect("a runtime directory");
    let state_dir = args.next().expect("a state directory");

    // SAFETY: set before the runtime does anything else with the environment.
    unsafe {
        std::env::set_var(
            "SAPPHIRE_SERVERTEST_CACHE_DIR",
            format!("{state_dir}/cache"),
        );
        std::env::set_var("SAPPHIRE_SERVERTEST_DATA_DIR", format!("{state_dir}/data"));
        std::env::set_var(
            "SAPPHIRE_SERVERTEST_CONFIG_DIR",
            format!("{state_dir}/config"),
        );
    }
    CTX.init(AppKind::Server);

    AppServer::new(&CTX, env!("CARGO_PKG_VERSION"))
        .endpoint(Endpoint::in_dir("sapphire-servertest", runtime_dir.into()))
        .managed_by(ManagedBy::Spawned)
        .idle_exit(None)
        .run()
        .await?;
    Ok(())
}
