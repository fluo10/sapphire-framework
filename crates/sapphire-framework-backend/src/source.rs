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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use indexmap::IndexMap;
use sapphire_remote_client::RemoteClient;
use sapphire_workspace::WorkspaceState;
use serde::{Deserialize, Serialize};

use crate::{Error, LocalBackend, RemoteBackend, Result, WorkspaceBackend};

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
        /// Optional bearer token for an authenticated server.
        token: Option<String>,
    },
}

impl WorkspaceLocator {
    /// Build a remote locator from a URL (`base` or `base#ws`) plus an optional
    /// bearer token. Missing `#ws` defaults to [`DEFAULT_WS`].
    pub fn remote(url: &str, token: Option<&str>) -> Self {
        let (base, ws) = match url.split_once('#') {
            Some((u, ws)) if !ws.is_empty() => (u.to_owned(), ws.to_owned()),
            _ => (url.to_owned(), DEFAULT_WS.to_owned()),
        };
        Self::Remote {
            url: base,
            ws,
            token: token.map(str::to_owned),
        }
    }

    /// Parse a reference. `http://` / `https://` prefixes select a remote
    /// workspace; the workspace id is the URL's `#fragment` (defaulting to
    /// [`DEFAULT_WS`]). Anything else is a local path.
    ///
    /// ```
    /// # use sapphire_framework_backend::{WorkspaceLocator, DEFAULT_WS};
    /// assert!(matches!(
    ///     WorkspaceLocator::parse("https://host:8080#notes"),
    ///     WorkspaceLocator::Remote { ws, .. } if ws == "notes"
    /// ));
    /// assert!(matches!(WorkspaceLocator::parse("/data/ws"), WorkspaceLocator::Local(_)));
    /// let _ = DEFAULT_WS;
    /// ```
    pub fn parse(s: &str) -> Self {
        if s.starts_with("http://") || s.starts_with("https://") {
            Self::remote(s, None)
        } else {
            Self::Local(PathBuf::from(s))
        }
    }

    /// Whether this locator points at a remote workspace.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}

/// The registry id of the default workspace.
pub const DEFAULT_ID: &str = "default";

/// One registered workspace: either a local path or a remote URL (mutually
/// exclusive). Serialised as a `[workspace.<id>]` TOML table so the CLI config
/// and the GUI share one representation across apps (timer / journal / ledger).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEntry {
    /// Display name (defaults to the registry id when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Local workspace root. Mutually exclusive with [`url`](Self::url).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// Remote server URL (optionally `base#ws`). Mutually exclusive with `path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Optional bearer token for an authenticated remote server.
    ///
    /// Stored in plaintext for now; real auth is server-side labeled keys
    /// (framework issue #92).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

impl WorkspaceEntry {
    /// A local entry at `path`.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            ..Default::default()
        }
    }

    /// A remote entry at `url` (optionally `base#ws`).
    pub fn remote(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            ..Default::default()
        }
    }

    /// Resolve this entry to a [`WorkspaceLocator`]. Errors when both or
    /// neither of `path`/`url` are set.
    pub fn locator(&self) -> Result<WorkspaceLocator> {
        match (&self.path, &self.url) {
            (Some(p), None) => Ok(WorkspaceLocator::Local(p.clone())),
            (None, Some(u)) => Ok(WorkspaceLocator::remote(u, self.token.as_deref())),
            (Some(_), Some(_)) => Err(Error::InvalidWorkspace(
                "entry has both `path` and `url` (they are mutually exclusive)".into(),
            )),
            (None, None) => Err(Error::InvalidWorkspace(
                "entry has neither `path` nor `url`".into(),
            )),
        }
    }
}

/// A set of named workspaces, keyed by id. Embed in an app's config with
/// `#[serde(default)] pub workspace: WorkspaceRegistry` so it reads as
/// `[workspace.<id>]` tables.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkspaceRegistry(pub IndexMap<String, WorkspaceEntry>);

/// A caller's workspace choice: ad-hoc CLI arguments plus an optional registry
/// id. Passed to [`WorkspaceRegistry::resolve`].
#[derive(Clone, Debug, Default)]
pub struct WorkspaceSelection<'a> {
    /// `--workspace <id>`: look this id up in the registry.
    pub id: Option<&'a str>,
    /// An ad-hoc local path (e.g. `--timer-dir`). Highest-precedence after url.
    pub ad_hoc_path: Option<&'a Path>,
    /// An ad-hoc remote URL (e.g. `--remote`). Highest precedence.
    pub ad_hoc_url: Option<&'a str>,
    /// Bearer token for an ad-hoc/looked-up remote.
    pub token: Option<&'a str>,
}

impl WorkspaceRegistry {
    /// Look up an entry by id.
    pub fn get(&self, id: &str) -> Option<&WorkspaceEntry> {
        self.0.get(id)
    }

    /// Insert or replace an entry.
    pub fn insert(&mut self, id: impl Into<String>, entry: WorkspaceEntry) {
        self.0.insert(id.into(), entry);
    }

    /// Remove an entry, returning it if present.
    pub fn remove(&mut self, id: &str) -> Option<WorkspaceEntry> {
        self.0.shift_remove(id)
    }

    /// Registered workspace ids, in insertion order.
    pub fn ids(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }

    /// Display name for `id` (its `name`, falling back to the id itself).
    pub fn display_name(&self, id: &str) -> String {
        self.get(id)
            .and_then(|e| e.name.clone())
            .unwrap_or_else(|| id.to_owned())
    }

    /// Resolve a [`WorkspaceSelection`] to a [`WorkspaceLocator`], shared by all
    /// apps' CLIs.
    ///
    /// Precedence: ad-hoc URL → ad-hoc path → registry id → the `default` entry
    /// → the built-in default local path `<data_dir>/workspaces/default`.
    pub fn resolve(
        &self,
        sel: &WorkspaceSelection<'_>,
        data_dir: &Path,
    ) -> Result<WorkspaceLocator> {
        if let Some(url) = sel.ad_hoc_url {
            return Ok(WorkspaceLocator::remote(url, sel.token));
        }
        if let Some(path) = sel.ad_hoc_path {
            return Ok(WorkspaceLocator::Local(path.to_path_buf()));
        }
        if let Some(id) = sel.id {
            let entry = self
                .get(id)
                .ok_or_else(|| Error::InvalidWorkspace(format!("unknown workspace id '{id}'")))?;
            return entry.locator();
        }
        if let Some(entry) = self.get(DEFAULT_ID) {
            return entry.locator();
        }
        Ok(WorkspaceLocator::Local(
            data_dir.join("workspaces").join(DEFAULT_ID),
        ))
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
                token: None,
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
                token: None,
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

    #[test]
    fn entry_locator_local_and_remote() {
        assert_eq!(
            WorkspaceEntry::local("/data/ws").locator().unwrap(),
            WorkspaceLocator::Local(PathBuf::from("/data/ws"))
        );
        let mut e = WorkspaceEntry::remote("https://host#notes");
        e.token = Some("tok".into());
        assert_eq!(
            e.locator().unwrap(),
            WorkspaceLocator::Remote {
                url: "https://host".into(),
                ws: "notes".into(),
                token: Some("tok".into()),
            }
        );
    }

    #[test]
    fn entry_locator_rejects_both_and_neither() {
        let both = WorkspaceEntry {
            path: Some("/p".into()),
            url: Some("https://h".into()),
            ..Default::default()
        };
        assert!(both.locator().is_err());
        assert!(WorkspaceEntry::default().locator().is_err());
    }

    #[test]
    fn registry_toml_roundtrip() {
        let toml = r#"
[default]
path = "/home/me/ws"

[work]
name = "Work"
url = "https://example.com#work"
token = "secret"
"#;
        let reg: WorkspaceRegistry = toml::from_str(toml).unwrap();
        assert_eq!(reg.ids().count(), 2);
        assert_eq!(reg.display_name("work"), "Work");
        assert_eq!(reg.display_name("default"), "default");
        let back = toml::to_string(&reg).unwrap();
        let reg2: WorkspaceRegistry = toml::from_str(&back).unwrap();
        assert_eq!(reg, reg2);
    }

    #[test]
    fn resolve_precedence() {
        let mut reg = WorkspaceRegistry::default();
        reg.insert("work", WorkspaceEntry::remote("https://example.com#work"));
        let data = Path::new("/data");

        // ad-hoc url wins over everything.
        let sel = WorkspaceSelection {
            id: Some("work"),
            ad_hoc_url: Some("https://other#x"),
            token: Some("t"),
            ..Default::default()
        };
        assert_eq!(
            reg.resolve(&sel, data).unwrap(),
            WorkspaceLocator::Remote {
                url: "https://other".into(),
                ws: "x".into(),
                token: Some("t".into()),
            }
        );

        // ad-hoc path next.
        let sel = WorkspaceSelection {
            ad_hoc_path: Some(Path::new("/tmp/ws")),
            ..Default::default()
        };
        assert_eq!(
            reg.resolve(&sel, data).unwrap(),
            WorkspaceLocator::Local("/tmp/ws".into())
        );

        // registry id.
        let sel = WorkspaceSelection {
            id: Some("work"),
            ..Default::default()
        };
        assert!(reg.resolve(&sel, data).unwrap().is_remote());

        // unknown id errors.
        let sel = WorkspaceSelection {
            id: Some("nope"),
            ..Default::default()
        };
        assert!(reg.resolve(&sel, data).is_err());

        // fall back to the built-in default path.
        assert_eq!(
            reg.resolve(&WorkspaceSelection::default(), data).unwrap(),
            WorkspaceLocator::Local(PathBuf::from("/data/workspaces/default"))
        );

        // an explicit `default` entry overrides the built-in path.
        reg.insert(DEFAULT_ID, WorkspaceEntry::local("/custom/default"));
        assert_eq!(
            reg.resolve(&WorkspaceSelection::default(), data).unwrap(),
            WorkspaceLocator::Local(PathBuf::from("/custom/default"))
        );
    }
}
