//! Connecting to a server, starting one if necessary (spec §2.6).

use std::path::PathBuf;
use std::time::Duration;

use crate::client::Client;
use crate::conn::Connection;
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::handshake::{ClientInfo, ManagedBy, ServerInfo};

/// A lock file older than this is assumed to belong to a process that died before
/// releasing it.
pub const STALE_LOCK_AGE: Duration = Duration::from_secs(30);

/// The method a client calls to retire a spawned server of the wrong version.
///
/// This crate only names it, so that both sides agree; `sapphire-framework-server`
/// implements it. A server that does not is simply waited for and then reported as a
/// timeout, which is the right outcome — an unresponsive server of the wrong version is
/// not something a client may work around.
pub const SHUTDOWN_METHOD: &str = "server.shutdown";

/// Connect to `endpoint` using this platform's carrier.
pub async fn connect(endpoint: &Endpoint) -> Result<Connection> {
    #[cfg(unix)]
    {
        crate::unix::connect(endpoint).await
    }
    #[cfg(windows)]
    {
        crate::windows::connect(endpoint).await
    }
}

/// Is a server listening on `endpoint`? Clears a stale Unix socket file as a side effect.
pub async fn probe(endpoint: &Endpoint) -> Result<bool> {
    #[cfg(unix)]
    {
        crate::unix::probe(endpoint).await
    }
    #[cfg(windows)]
    {
        crate::windows::probe(endpoint).await
    }
}

/// How to start a server that is not running.
#[derive(Clone, Debug)]
pub struct SpawnConfig {
    /// The executable to run. Defaults to this process's own, so one binary serves as
    /// both the client and the server.
    pub exe: PathBuf,
    /// Arguments that make `exe` run as a server.
    pub args: Vec<String>,
    /// When `false`, a missing server is an error instead of something to fix. Set this
    /// for an app configured with privilege separation, whose server runs as root and
    /// cannot be started by a user's CLI (spec §2.6, §3).
    pub allow_spawn: bool,
    /// How long to wait for a spawned server's socket to appear.
    pub connect_timeout: Duration,
    /// How long to wait for the spawn lock.
    pub lock_timeout: Duration,
    /// How old a lock file must be before it is treated as abandoned.
    pub stale_lock_age: Duration,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        SpawnConfig {
            exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("sapphire")),
            args: vec!["server".to_owned(), "run".to_owned()],
            allow_spawn: true,
            connect_timeout: Duration::from_secs(10),
            lock_timeout: Duration::from_secs(10),
            stale_lock_age: STALE_LOCK_AGE,
        }
    }
}

impl SpawnConfig {
    /// A configuration that never starts a server.
    pub fn disabled() -> SpawnConfig {
        SpawnConfig {
            allow_spawn: false,
            ..SpawnConfig::default()
        }
    }
}

/// Connect to `endpoint`'s server, starting it if nothing is listening.
///
/// On a protocol or version mismatch with a **spawned** server, the running server is
/// asked to exit and a new one is started. A server started by the OS service manager is
/// never replaced; the caller is told to restart the service instead.
pub async fn ensure_server(
    endpoint: &Endpoint,
    app: &str,
    client: ClientInfo,
    spawn: &SpawnConfig,
) -> Result<(Client, ServerInfo)> {
    // 1-2. Something listening? Use it, unless it is the wrong version.
    if probe(endpoint).await? {
        match handshake_with(endpoint, app, client.clone()).await {
            Ok((c, info)) if info.version == client.version => return Ok((c, info)),
            Ok((c, info)) => match info.managed_by {
                ManagedBy::Service => {
                    return Err(Error::ServiceVersionMismatch {
                        running: info.version,
                        ours: client.version,
                    });
                }
                ManagedBy::Spawned => {
                    tracing::info!(
                        running = %info.version,
                        ours = %client.version,
                        "replacing a spawned server of a different version"
                    );
                    let _: std::result::Result<serde_json::Value, _> =
                        c.call(SHUTDOWN_METHOD, serde_json::Value::Null).await;
                    drop(c);
                    wait_until_gone(endpoint, spawn.connect_timeout).await?;
                }
            },
            // A half-open socket, or a server shutting down: fall through and start one.
            Err(Error::Io(_)) | Err(Error::Closed) => {}
            Err(err) => return Err(err),
        }
    }

    // 3. Nothing listening. Serialise the start.
    if !spawn.allow_spawn {
        return Err(Error::Spawn(format!(
            "no {app} server is running, and this process is not allowed to start one"
        )));
    }

    let _guard = SpawnLock::acquire(endpoint, spawn).await?;

    // 3b. Another process may have won the race and started it while we waited.
    if probe(endpoint).await? {
        return handshake_with(endpoint, app, client).await;
    }

    // 3c. Start it.
    let mut command = tokio::process::Command::new(&spawn.exe);
    command.args(&spawn.args);
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::null());
    command.stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        // Detach from this process's session so the server outlives the CLI.
        // SAFETY: setsid is async-signal-safe and is the documented way to detach.
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            })
        };
    }
    let child = command
        .spawn()
        .map_err(|e| Error::Spawn(format!("could not run {}: {e}", spawn.exe.display())))?;
    // The server outlives us; do not reap it.
    drop(child);

    // 3d. Wait for it.
    let deadline = tokio::time::Instant::now() + spawn.connect_timeout;
    let mut delay = Duration::from_millis(10);
    loop {
        if probe(endpoint).await.unwrap_or(false)
            && let Ok(pair) = handshake_with(endpoint, app, client.clone()).await
        {
            return Ok(pair);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Timeout("the server to start listening"));
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_millis(250));
    }
}

async fn handshake_with(
    endpoint: &Endpoint,
    app: &str,
    client: ClientInfo,
) -> Result<(Client, ServerInfo)> {
    let conn = connect(endpoint).await?;
    Client::handshake(conn, app, client).await
}

async fn wait_until_gone(endpoint: &Endpoint, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while probe(endpoint).await.unwrap_or(false) {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Timeout("the old server to exit"));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

/// The lock that serialises start-on-demand.
///
/// `create_new` is atomic on every platform we target, so of several processes trying at
/// once exactly one creates the file and the rest wait, then find the server already up.
struct SpawnLock {
    path: PathBuf,
}

impl SpawnLock {
    async fn acquire(endpoint: &Endpoint, spawn: &SpawnConfig) -> Result<SpawnLock> {
        let path = endpoint.lock_path();
        let deadline = tokio::time::Instant::now() + spawn.lock_timeout;
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    use std::io::Write;
                    let _ = write!(file, "{}", std::process::id());
                    return Ok(SpawnLock { path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if lock_is_stale(&path, spawn.stale_lock_age) {
                        tracing::debug!(path = %path.display(), "clearing an abandoned spawn lock");
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Timeout("the spawn lock"));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => return Err(Error::Io(err)),
            }
        }
    }
}

impl Drop for SpawnLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn lock_is_stale(path: &std::path::Path, max_age: Duration) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return true;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    modified
        .elapsed()
        .map(|age| age >= max_age)
        .unwrap_or(false)
}
