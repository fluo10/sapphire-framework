//! `sapphire-framework` — a single-dependency facade over the local-first
//! framework crates.
//!
//! Depend on this one crate and enable the features you need; each feature
//! re-exports an internal `sapphire-framework-*` crate as a module. This mirrors
//! the way large Rust libraries (e.g. bevy) ship a facade over many internal
//! crates: the split crates keep compile-time isolation, while consumers depend
//! on one name.
//!
//! ```toml
//! # A native app that indexes a local workspace and talks to a remote server:
//! sapphire-framework = { version = "0.1", features = ["native", "redb-store"] }
//! ```
//!
//! ```ignore
//! // (requires the `backend` feature)
//! use sapphire_framework::prelude::*;
//! let backend = LocalBackend::new(state);
//! ```
//!
//! ## Modules (feature-gated)
//!
//! | feature | module | crate |
//! |---|---|---|
//! | `workspace` | [`workspace`] | `sapphire-framework-workspace` |
//! | `retrieve` | [`retrieve`] | `sapphire-framework-retrieve` |
//! | `track` | [`track`] | `sapphire-framework-track` |
//! | `rpc` | [`rpc`] | `sapphire-framework-rpc` |
//! | `blob` | [`blob`] | `sapphire-framework-blob` |
//! | `backend` | [`backend`] | `sapphire-framework-backend` |
//! | `remote-client` | [`remote_client`] | `sapphire-framework-remote-client` |
//! | `remote-server` | [`remote_server`] | `sapphire-framework-remote-server` |

#[cfg(feature = "workspace")]
pub use sapphire_framework_workspace as workspace;

#[cfg(feature = "retrieve")]
pub use sapphire_framework_retrieve as retrieve;

#[cfg(feature = "track")]
pub use sapphire_framework_track as track;

#[cfg(feature = "rpc")]
pub use sapphire_framework_rpc as rpc;

#[cfg(feature = "blob")]
pub use sapphire_framework_blob as blob;

#[cfg(feature = "backend")]
pub use sapphire_framework_backend as backend;

#[cfg(feature = "remote-client")]
pub use sapphire_framework_remote_client as remote_client;

#[cfg(feature = "remote-server")]
pub use sapphire_framework_remote_server as remote_server;

/// Commonly-used types, re-exported for `use sapphire_framework::prelude::*;`.
///
/// What is available depends on the enabled features.
pub mod prelude {
    #[cfg(feature = "workspace")]
    pub use crate::workspace::{
        AppContext, FileSearchResult, RetrieveConfig, RetrieveParams, SearchMode, Workspace,
        WorkspaceState,
    };

    #[cfg(feature = "backend")]
    pub use crate::backend::{
        BackendEvent, LocalBackend, RemoteBackend, RemoteClient, SyncSummary, WorkspaceBackend,
        WorkspaceLocator, WorkspaceSource,
    };

    // `backend` already re-exports `RemoteClient`; only pull it from the client
    // crate when the backend module isn't present, to avoid a duplicate name.
    #[cfg(all(feature = "remote-client", not(feature = "backend")))]
    pub use crate::remote_client::RemoteClient;

    #[cfg(feature = "remote-server")]
    pub use crate::remote_server::{ServerState, router, serve};
}
