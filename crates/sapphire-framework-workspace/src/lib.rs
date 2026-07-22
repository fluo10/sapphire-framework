pub mod config;
pub mod context;
pub mod indexer;
pub mod workspace;
pub mod workspace_state;

mod error;
pub use error::{Error, Result};

pub use config::{EmbeddingConfig, HybridConfig, RetrieveConfig, VectorDb};
pub use context::AppContext;
pub use indexer::{
    IndexHook, SyncReport, SyncWithHookError, path_to_doc_id, sync_workspace,
    sync_workspace_full_with_hook, sync_workspace_incremental, sync_workspace_with_hook,
};
pub use workspace::Workspace;
pub use workspace::{DEFAULT_WORKSPACE_MARKER, path_uuid};
pub use workspace_state::{DbInfo, RetrieveParams, SearchMode, WorkspaceState};

// Re-export sapphire-retrieve public API so callers can use a single dependency.
/// Retrieve cache schema version. Retained for API compatibility with the
/// retired SQLite backend; the pure-Rust redb backend manages its own on-disk
/// format, so this is always `0`.
pub const RETRIEVE_SCHEMA_VERSION: i32 = 0;
#[cfg(feature = "lancedb-store")]
pub use sapphire_retrieve::lancedb_store;
pub use sapphire_retrieve::{
    Chunk, ChunkHit, Document, Embedder, EmbedderConfig, Error as RetrieveError, FileSearchResult,
    FtsQuery, HybridQuery, RetrieveDb, RetrieveStore, VecInfo, VectorQuery, build_embedder,
    default_hybrid, merge_rrf_files,
};

