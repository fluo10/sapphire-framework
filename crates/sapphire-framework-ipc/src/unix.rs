//! Unix domain socket carrier, with same-user authentication (spec §2.3).

use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};

use crate::conn::Connection;
use crate::endpoint::{Endpoint, ensure_private_dir};
use crate::error::{Error, Result};

/// A bound listener that removes its socket file when dropped.
#[derive(Debug)]
pub struct UnixListenerHandle {
    listener: UnixListener,
    path: PathBuf,
}

impl UnixListenerHandle {
    /// The socket path this listener is bound to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accept one connection, rejecting peers that are not the same OS user.
    ///
    /// A rejected peer is disconnected and the wait continues, so one hostile connection
    /// cannot stop the server from serving legitimate ones.
    pub async fn accept(&self) -> Result<Connection> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            // SAFETY: getuid has no preconditions.
            let ours = unsafe { libc::getuid() };
            match peer_uid(&stream) {
                Ok(uid) if uid == ours => return Ok(Connection::from_io(stream)),
                Ok(uid) => {
                    tracing::warn!(
                        peer_uid = uid,
                        our_uid = ours,
                        "rejected a connection from another user"
                    );
                    drop(stream);
                }
                Err(err) => {
                    tracing::warn!("could not read peer credentials, rejecting: {err}");
                    drop(stream);
                }
            }
        }
    }
}

impl Drop for UnixListenerHandle {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Listen on `endpoint`, first removing a socket file left behind by a dead server.
pub async fn bind(endpoint: &Endpoint) -> Result<UnixListenerHandle> {
    ensure_private_dir(&endpoint.dir)?;
    let path = endpoint.socket_path();
    if path.exists() {
        // Either nothing is listening (stale) or another server is. `probe` unlinks the
        // former; the latter makes the bind below fail with EADDRINUSE, which is correct.
        probe(endpoint).await?;
    }
    let listener = UnixListener::bind(&path)?;
    Ok(UnixListenerHandle { listener, path })
}

/// Connect to `endpoint`.
// Only the tests call this until the spawn step re-exports a platform-independent
// `connect`; it is deliberately not re-exported from the crate root yet (see lib.rs).
#[allow(dead_code)]
pub async fn connect(endpoint: &Endpoint) -> Result<Connection> {
    let stream = UnixStream::connect(endpoint.socket_path()).await?;
    Ok(Connection::from_io(stream))
}

/// Is a server listening on `endpoint`?
///
/// Unlinks the socket file and returns `false` when the file exists but nothing answers —
/// the state left by a crash or a reboot (spec §2.6 step 4).
pub async fn probe(endpoint: &Endpoint) -> Result<bool> {
    let path = endpoint.socket_path();
    if !path.exists() {
        return Ok(false);
    }
    match UnixStream::connect(&path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
            tracing::debug!(path = %path.display(), "removing a stale socket");
            let _ = std::fs::remove_file(&path);
            Ok(false)
        }
        Err(err) => Err(Error::Io(err)),
    }
}

/// The uid of the process at the other end.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn peer_uid(stream: &UnixStream) -> Result<u32> {
    use std::os::fd::AsRawFd;

    let mut cred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `cred` and `len` are valid for writes of the sizes passed, and the fd is
    // owned by `stream` for the duration of the call.
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut cred).cast::<libc::c_void>(),
            &raw mut len,
        )
    };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(cred.uid)
}

/// The uid of the process at the other end.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn peer_uid(stream: &UnixStream) -> Result<u32> {
    use std::os::fd::AsRawFd;

    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    // SAFETY: both out-pointers are valid for writes, and the fd is owned by `stream`.
    let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &raw mut uid, &raw mut gid) };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(uid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, Notification};

    fn endpoint(dir: &std::path::Path) -> Endpoint {
        Endpoint::in_dir("test-app", dir.to_path_buf())
    }

    #[tokio::test]
    async fn a_client_reaches_the_listener() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        let listener = bind(&ep).await.unwrap();

        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            conn.recv().await.unwrap().unwrap()
        });

        let client = connect(&ep).await.unwrap();
        let msg = Message::Notification(Notification {
            method: "hello".into(),
            params: serde_json::Value::Null,
        });
        client.send(msg.clone()).await.unwrap();
        assert_eq!(server.await.unwrap(), msg);
    }

    #[tokio::test]
    async fn the_peer_uid_of_a_local_connection_is_our_own() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        let listener = tokio::net::UnixListener::bind(ep.socket_path()).unwrap();

        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            peer_uid(&stream).unwrap()
        });
        let _client = tokio::net::UnixStream::connect(ep.socket_path())
            .await
            .unwrap();

        // SAFETY: getuid has no preconditions.
        assert_eq!(accept.await.unwrap(), unsafe { libc::getuid() });
    }

    #[tokio::test]
    async fn probing_an_empty_directory_reports_nothing_listening() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!probe(&endpoint(tmp.path())).await.unwrap());
    }

    #[tokio::test]
    async fn a_stale_socket_file_is_removed_and_reported_as_not_listening() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        // A socket file with nobody behind it: bind, then drop the listener without
        // letting our own Drop clean up.
        let listener = tokio::net::UnixListener::bind(ep.socket_path()).unwrap();
        drop(listener);
        assert!(ep.socket_path().exists());

        assert!(!probe(&ep).await.unwrap());
        assert!(
            !ep.socket_path().exists(),
            "the stale socket should have been unlinked"
        );
    }

    #[tokio::test]
    async fn binding_over_a_stale_socket_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        drop(tokio::net::UnixListener::bind(ep.socket_path()).unwrap());

        let listener = bind(&ep).await.unwrap();
        assert!(connect(&ep).await.is_ok());
        drop(listener);
    }

    #[tokio::test]
    async fn a_listener_removes_its_socket_file_on_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let ep = endpoint(tmp.path());
        let listener = bind(&ep).await.unwrap();
        assert!(ep.socket_path().exists());
        drop(listener);
        assert!(!ep.socket_path().exists());
    }
}
