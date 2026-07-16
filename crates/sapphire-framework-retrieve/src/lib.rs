pub mod chunker;
pub mod config;
pub mod db;
pub mod embed;
pub mod error;
#[cfg(feature = "lancedb-store")]
pub mod lancedb_store;
#[cfg(feature = "redb-store")]
pub mod redb_store;
pub mod retrieve_store;
pub mod vector_store;

pub use chunker::{Chunker, JsonlChunker, MarkdownChunker, TextChunk, TomlChunker};
pub use config::{EmbeddingConfig, HybridConfig, RetrieveConfig, VectorDb};
pub use db::open_in_memory;
#[cfg(feature = "redb-store")]
pub use db::{open_redb, open_redb_vec};
#[cfg(feature = "lancedb-store")]
pub use db::open_lancedb;
pub use db::{RetrieveDb, default_hybrid, merge_rrf_files};
pub use embed::{Embedder, EmbedderConfig, build_embedder};
pub use error::{Error, Result};
pub use retrieve_store::{
    ChunkHit, Document, FileSearchResult, FtsQuery, HybridQuery, RetrieveStore, VectorQuery,
};
pub use vector_store::{Chunk, VecInfo};
