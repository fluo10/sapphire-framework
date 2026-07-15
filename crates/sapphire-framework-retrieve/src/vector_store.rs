//! Internal vector-store types and grouping helpers shared by the storage
//! backends (SQLite, LanceDB, redb+tantivy).

#[allow(unused_imports)]
use std::collections::HashMap;

use crate::retrieve_store::{ChunkHit, FileSearchResult};

// ── public types ──────────────────────────────────────────────────────────────

/// A single text chunk derived from a document, ready to be embedded.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Stable document ID assigned by the caller.
    pub doc_id: i64,
    /// First source line of this chunk (inclusive, 0-based).
    pub line_start: usize,
    /// Last source line of this chunk (inclusive, 0-based).
    pub line_end: usize,
    /// Embeddable text content of the chunk.
    pub text: String,
    /// Denormalised absolute file path (for display in search results).
    pub doc_path: String,
}

/// Statistics about the vector index.
pub struct VecInfo {
    /// Embedding dimension (number of f32 values per vector).
    pub embedding_dim: u32,
    /// Number of chunks that have an embedding stored.
    pub vector_count: u64,
    /// Number of chunks that do not yet have an embedding.
    pub pending_count: u64,
}

// ── internal helpers shared by db.rs and lancedb_store.rs ─────────────────────

/// Serialize a float slice to the little-endian bytes expected by sqlite-vec.
#[allow(dead_code)]
pub(crate) fn vec_serialize(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Deserialize a little-endian `f32` blob produced by [`vec_serialize`].
#[allow(dead_code)]
pub(crate) fn vec_deserialize(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .collect()
}

/// Euclidean (L2) distance between two equal-length vectors. Lower = closer.
#[allow(dead_code)]
pub(crate) fn l2_distance(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = (*x - *y) as f64;
            d * d
        })
        .sum::<f64>()
        .sqrt()
}

// ── chunk-row grouping (shared by SQLite / redb+tantivy backends) ─────────────

/// A flat chunk-level search hit, before grouping into files.
#[allow(dead_code)]
pub(crate) struct ChunkRow {
    pub doc_id: i64,
    pub path: String,
    pub line_start: usize,
    pub line_end: usize,
    pub text: String,
    pub score: f64,
}

/// Group chunk-level rows by `doc_id` into [`FileSearchResult`]s.
///
/// `is_better(a, b)` returns true when score `a` is better than `b` (used both
/// for picking the representative score and for sorting). Chunks within each
/// file are sorted by the same comparator; the output is truncated to `limit`.
#[allow(dead_code)]
pub(crate) fn group_by_file<F>(
    rows: Vec<ChunkRow>,
    limit: usize,
    is_better: F,
) -> Vec<FileSearchResult>
where
    F: Fn(f64, f64) -> bool + Copy,
{
    let mut by_doc: HashMap<i64, FileSearchResult> = HashMap::new();

    for r in rows {
        let entry = by_doc.entry(r.doc_id).or_insert_with(|| FileSearchResult {
            id: r.doc_id,
            path: r.path.clone(),
            score: r.score,
            chunks: Vec::new(),
        });
        if is_better(r.score, entry.score) {
            entry.score = r.score;
        }
        entry.chunks.push(ChunkHit {
            line_start: r.line_start,
            line_end: r.line_end,
            text: r.text,
            score: r.score,
        });
    }

    let cmp = |a: f64, b: f64| {
        if is_better(a, b) {
            std::cmp::Ordering::Less
        } else if is_better(b, a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    };

    let mut files: Vec<FileSearchResult> = by_doc.into_values().collect();
    for f in &mut files {
        f.chunks.sort_by(|a, b| cmp(a.score, b.score));
    }
    files.sort_by(|a, b| cmp(a.score, b.score));
    files.truncate(limit);
    files
}
