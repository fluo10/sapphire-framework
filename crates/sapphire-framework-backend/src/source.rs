//! Choosing a backend: a local path or a remote endpoint, behind one type.
//!
//! [`WorkspaceLocator`] parses a user-supplied reference (a filesystem path or
//! an `http(s)://` URL) into a tagged value. [`WorkspaceSource`] holds the
//! opened resources and produces a `Box<dyn WorkspaceBackend>`, so a CLI or GUI
//! can open "a local or a remote workspace" through a single call site.
//!
//! Opening the underlying [`WorkspaceState`] stays the caller's job — it needs
//! the app's `AppContext` and workspace marker, which are application concerns.
//! For a remote workspace the caller opens a *cache* `WorkspaceState` on a
//! scratch directory (see [`RemoteBackend::new`]).

use std::path::PathBuf;
use std::sync::Arc;

use sapphire_remote_client::RemoteClient;
use sapphire_workspace::WorkspaceState;

use crate::{LocalBackend, RemoteBackend, WorkspaceBackend};

/// The default workspace id used when a remote locator omits one.
///
/// A self-hosted server serves a single workspace (framework issue #86), so a
/// bare URL maps to this id. Multi-workspace servers can still address others
/// via the `#<ws>` fragment.
pub const DEFAULT_WS: &str = "default";

/// A parsed workspace reference: a local path or a remote endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceLocator {
    /// A local filesystem workspace root.
    Local(PathBuf),
    /// A remote workspace: server base URL plus workspace id.
    Remote {
        /// Server base URL (the `/rpc` path is appended by the client).
        url: String,
        /// Workspace id on that server.
        ws: String,
    },
}

impl WorkspaceLocator {
    /// Parse a reference. `http://` / `https://` prefixes select a remote
    /// workspace; the workspace id is the URL's `#fragment` (defaulting to
    /// [`DEFAULT_WS`]). Anything else is a local path.
    ///
    /// ```
    /// # use sapphire_framework_backend::{WorkspaceLocator, DEFAULT_WS};
    /// assert_eq!(
    ///     WorkspaceLocator::parse("https://host:8080#notes"),
    ///     WorkspaceLocator::Remote { url: "https://host:8080".into(), ws: "notes".into() },
    /// );
    /// assert!(matches!(WorkspaceLocator::parse("/data/ws"), WorkspaceLocator::Local(_)));
    /// let _ = DEFAULT_WS;
    /// ```
    pub fn parse(s: &str) -> Self {
        if s.starts_with("http://") || s.starts_with("https://") {
            match s.split_once('#') {
                Some((url, ws)) if !ws.is_empty() => Self::Remote {
                    url: url.to_owned(),
                    ws: ws.to_owned(),
                },
                _ => Self::Remote {
                    url: s.to_owned(),
                    ws: DEFAULT_WS.to_owned(),
                },
            }
        } else {
            Self::Local(PathBuf::from(s))
        }
    }

    /// Whether this locator points at a remote workspace.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}

/// Opened resources for a workspace, ready to become a backend.
pub enum WorkspaceSource {
    /// A local workspace, driven directly.
    Local {
        /// The opened local workspace state.
        state: Arc<WorkspaceState>,
    },
    /// A remote workspace mirrored into a local cache.
    Remote {
        /// JSON-RPC client for the server.
        client: RemoteClient,
        /// Workspace id on the server.
        ws: String,
        /// Local cache state (a scratch `WorkspaceState`).
        cache: Arc<WorkspaceState>,
    },
}

impl WorkspaceSource {
    /// Build the concrete backend behind a trait object, so callers hold one
    /// `Box<dyn WorkspaceBackend>` regardless of locality.
    pub fn into_backend(self) -> Box<dyn WorkspaceBackend> {
        match self {
            WorkspaceSource::Local { state } => Box::new(LocalBackend::new(state)),
            WorkspaceSource::Remote { client, ws, cache } => {
                Box::new(RemoteBackend::new(client, ws, cache))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_local_path() {
        assert_eq!(
            WorkspaceLocator::parse("/home/me/notes"),
            WorkspaceLocator::Local(PathBuf::from("/home/me/notes"))
        );
        assert_eq!(
            WorkspaceLocator::parse("relative/dir"),
            WorkspaceLocator::Local(PathBuf::from("relative/dir"))
        );
    }

    #[test]
    fn parse_remote_default_ws() {
        assert_eq!(
            WorkspaceLocator::parse("http://localhost:8080"),
            WorkspaceLocator::Remote {
                url: "http://localhost:8080".into(),
                ws: DEFAULT_WS.into(),
            }
        );
    }

    #[test]
    fn parse_remote_with_ws_fragment() {
        assert_eq!(
            WorkspaceLocator::parse("https://example.com#work"),
            WorkspaceLocator::Remote {
                url: "https://example.com".into(),
                ws: "work".into(),
            }
        );
    }

    #[test]
    fn empty_fragment_falls_back_to_default() {
        match WorkspaceLocator::parse("https://example.com#") {
            WorkspaceLocator::Remote { ws, .. } => assert_eq!(ws, DEFAULT_WS),
            other => panic!("expected remote, got {other:?}"),
        }
    }
}
