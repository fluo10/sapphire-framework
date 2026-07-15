//! Pure-Rust backend for [`RetrieveStore`]: **redb + tantivy**.
//!
//! [`RedbStore`] keeps all state in a directory, with **no C dependency** (no
//! SQLite / libsqlite3-sys), so downstream binaries are never tied to another
//! crate's rusqlite version.
//!
//! Layout (`<dir>/`):
//!
//! | path | role |
//! |------|------|
//! | `docs.redb` | canonical record store (documents + chunk vectors + meta) |
//! | `tantivy/`  | full-text inverted index (derived, rebuildable from redb) |
//!
//! - **redb** holds the source-of-truth cache records. `documents` maps
//!   `doc_id -> {path, chunks}`; `vectors` maps `(doc_id, line_start) -> f32[]`.
//! - **tantivy** holds a trigram full-text index over chunk text (BM25). It is
//!   derived from redb and can be rebuilt at any time.
//! - **Vector search is brute-force** over the `vectors` table (exact, no ANN).
//!   Fine up to tens of thousands of chunks; swap in an HNSW index later if the
//!   collection grows.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use tantivy::{
    Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term,
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Field, INDEXED, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value},
    tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer},
};

use crate::{
    chunker::chunk_document,
    embed::Embedder,
    error::{Error, Result},
    retrieve_store::{Document, FileSearchResult, FtsQuery, RetrieveStore, VectorQuery},
    vector_store::{ChunkRow, VecInfo, group_by_file, l2_distance, vec_deserialize, vec_serialize},
};

// ── redb tables ────────────────────────────────────────────────────────────────

/// `doc_id -> serde_json(DocRecord)`.
const DOCUMENTS: TableDefinition<i64, &[u8]> = TableDefinition::new("documents");
/// `vkey(doc_id, line_start) -> little-endian f32 blob`.
const VECTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vectors");
/// misc key/value metadata (e.g. `embedding_dim`).
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredChunk {
    line_start: usize,
    line_end: usize,
    text: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DocRecord {
    path: String,
    chunks: Vec<StoredChunk>,
}

/// 16-byte vector key: `doc_id` (i64 LE) ++ `line_start` (u64 LE).
fn vkey(doc_id: i64, line_start: usize) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&doc_id.to_le_bytes());
    k[8..].copy_from_slice(&(line_start as u64).to_le_bytes());
    k
}

fn vkey_parse(b: &[u8]) -> (i64, usize) {
    let doc_id = i64::from_le_bytes(b[..8].try_into().unwrap());
    let line_start = u64::from_le_bytes(b[8..16].try_into().unwrap()) as usize;
    (doc_id, line_start)
}

// ── error mapping ────────────────────────────────────────────────────────────

fn redb_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Redb(e.to_string())
}
fn tantivy_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Tantivy(e.to_string())
}

// ── tantivy schema ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Fields {
    doc_id: Field,
    line_start: Field,
    text: Field,
}

const TRIGRAM_TOKENIZER: &str = "trigram";

/// Build the tantivy schema. `text` is indexed with a character-trigram
/// tokenizer (mirrors the previous SQLite FTS5 `trigram` design, so substring
/// and CJK matching keep working); `doc_id`/`line_start` are stored so hits can
/// be resolved back to redb records, and `doc_id` is indexed for `delete_term`.
fn build_schema() -> (Schema, Fields) {
    let mut sb = Schema::builder();
    let doc_id = sb.add_i64_field("doc_id", INDEXED | STORED);
    let line_start = sb.add_u64_field("line_start", STORED);
    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer(TRIGRAM_TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_opts = TextOptions::default().set_indexing_options(text_indexing);
    let text = sb.add_text_field("text", text_opts);
    let schema = sb.build();
    (
        schema,
        Fields {
            doc_id,
            line_start,
            text,
        },
    )
}

fn register_trigram(index: &Index) -> Result<()> {
    let ngram = NgramTokenizer::new(3, 3, false).map_err(tantivy_err)?;
    let analyzer = TextAnalyzer::builder(ngram).filter(LowerCaser).build();
    index.tokenizers().register(TRIGRAM_TOKENIZER, analyzer);
    Ok(())
}

// ── RedbStore ────────────────────────────────────────────────────────────────

pub struct RedbStore {
    db: Arc<Database>,
    index: Index,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
    fields: Fields,
    dim: Option<u32>,
}

impl RedbStore {
    /// Open (or create) a store at `dir`. `dim` enables vector search when set.
    pub fn open(dir: &Path, dim: Option<u32>) -> Result<Self> {
        std::fs::create_dir_all(dir)?;

        // redb
        let db = Database::create(dir.join("docs.redb")).map_err(redb_err)?;
        {
            let wtx = db.begin_write().map_err(redb_err)?;
            wtx.open_table(DOCUMENTS).map_err(redb_err)?;
            wtx.open_table(VECTORS).map_err(redb_err)?;
            wtx.open_table(META).map_err(redb_err)?;
            wtx.commit().map_err(redb_err)?;
        }

        // tantivy
        let tantivy_dir = dir.join("tantivy");
        std::fs::create_dir_all(&tantivy_dir)?;
        let (schema, fields) = build_schema();
        let mmap = tantivy::directory::MmapDirectory::open(&tantivy_dir).map_err(tantivy_err)?;
        let index = Index::open_or_create(mmap, schema).map_err(tantivy_err)?;
        register_trigram(&index)?;
        let writer: IndexWriter = index.writer(50_000_000).map_err(tantivy_err)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(tantivy_err)?;

        let store = Self {
            db: Arc::new(db),
            index,
            writer: Mutex::new(writer),
            reader,
            fields,
            dim,
        };

        if let Some(d) = dim {
            store.set_meta_u32("embedding_dim", d)?;
        } else if let Some(d) = store.get_meta_u32("embedding_dim")? {
            // Re-open with a previously configured dim.
            return Ok(Self {
                dim: Some(d),
                ..store
            });
        }
        Ok(store)
    }

    fn set_meta_u32(&self, key: &str, value: u32) -> Result<()> {
        let wtx = self.db.begin_write().map_err(redb_err)?;
        {
            let mut t = wtx.open_table(META).map_err(redb_err)?;
            t.insert(key, value.to_le_bytes().as_slice())
                .map_err(redb_err)?;
        }
        wtx.commit().map_err(redb_err)?;
        Ok(())
    }

    fn get_meta_u32(&self, key: &str) -> Result<Option<u32>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let t = rtx.open_table(META).map_err(redb_err)?;
        let v = t.get(key).map_err(redb_err)?;
        Ok(v.and_then(|g| {
            let b = g.value();
            (b.len() == 4).then(|| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        }))
    }

    pub fn dim(&self) -> Option<u32> {
        self.dim
    }

    fn get_doc(&self, doc_id: i64) -> Result<Option<DocRecord>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let t = rtx.open_table(DOCUMENTS).map_err(redb_err)?;
        let v = t.get(doc_id).map_err(redb_err)?;
        Ok(v.and_then(|g| serde_json::from_slice(g.value()).ok()))
    }

    /// Resolve `doc.chunks` (or auto-chunk `doc.body`) into stored chunks.
    fn resolve_chunks(doc: &Document) -> Vec<StoredChunk> {
        if let Some(chunks) = &doc.chunks {
            chunks
                .iter()
                .map(|(ls, le, text)| StoredChunk {
                    line_start: *ls,
                    line_end: *le,
                    text: text.clone(),
                })
                .collect()
        } else {
            chunk_document(&doc.body)
                .into_iter()
                .enumerate()
                .map(|(i, text)| StoredChunk {
                    line_start: i,
                    line_end: i,
                    text,
                })
                .collect()
        }
    }

    /// Re-index a document's chunks in tantivy (delete-then-add). Writes are
    /// buffered in the [`IndexWriter`]; call [`RetrieveStore::rebuild_fts`] to
    /// commit and make them searchable.
    fn reindex_fts(&self, doc_id: i64, path: &str, chunks: &[StoredChunk]) -> Result<()> {
        let _ = path; // path is resolved from redb at search time
        let w = self.writer.lock().unwrap();
        w.delete_term(Term::from_field_i64(self.fields.doc_id, doc_id));
        for c in chunks {
            w.add_document(doc!(
                self.fields.doc_id => doc_id,
                self.fields.line_start => c.line_start as u64,
                self.fields.text => c.text.clone(),
            ))
            .map_err(tantivy_err)?;
        }
        Ok(())
    }
}

impl RetrieveStore for RedbStore {
    fn upsert_document(&self, doc: &Document) -> Result<()> {
        let chunks = Self::resolve_chunks(doc);
        let new_starts: HashSet<usize> = chunks.iter().map(|c| c.line_start).collect();

        // Detect stale/changed vectors from the previous record.
        let previous = self.get_doc(doc.id)?;
        let mut drop_vectors: Vec<usize> = Vec::new();
        if let Some(prev) = &previous {
            for pc in &prev.chunks {
                let changed = chunks
                    .iter()
                    .find(|c| c.line_start == pc.line_start)
                    .map(|c| c.text != pc.text)
                    .unwrap_or(true); // removed chunk
                if changed {
                    drop_vectors.push(pc.line_start);
                }
            }
        }
        let _ = new_starts;

        let record = DocRecord {
            path: doc.path.clone(),
            chunks,
        };
        let bytes = serde_json::to_vec(&record).map_err(|e| Error::Redb(e.to_string()))?;

        let wtx = self.db.begin_write().map_err(redb_err)?;
        {
            let mut docs = wtx.open_table(DOCUMENTS).map_err(redb_err)?;
            docs.insert(doc.id, bytes.as_slice()).map_err(redb_err)?;
            let mut vecs = wtx.open_table(VECTORS).map_err(redb_err)?;
            for ls in drop_vectors {
                vecs.remove(vkey(doc.id, ls).as_slice()).map_err(redb_err)?;
            }
        }
        wtx.commit().map_err(redb_err)?;

        self.reindex_fts(doc.id, &record.path, &record.chunks)?;
        Ok(())
    }

    fn remove_document(&self, id: i64) -> Result<()> {
        // Remove the record and all its vectors.
        let wtx = self.db.begin_write().map_err(redb_err)?;
        let starts: Vec<usize> = {
            let docs = wtx.open_table(DOCUMENTS).map_err(redb_err)?;
            docs.get(id)
                .map_err(redb_err)?
                .and_then(|g| serde_json::from_slice::<DocRecord>(g.value()).ok())
                .map(|r| r.chunks.iter().map(|c| c.line_start).collect())
                .unwrap_or_default()
        };
        {
            let mut docs = wtx.open_table(DOCUMENTS).map_err(redb_err)?;
            docs.remove(id).map_err(redb_err)?;
            let mut vecs = wtx.open_table(VECTORS).map_err(redb_err)?;
            for ls in starts {
                vecs.remove(vkey(id, ls).as_slice()).map_err(redb_err)?;
            }
        }
        wtx.commit().map_err(redb_err)?;

        let w = self.writer.lock().unwrap();
        w.delete_term(Term::from_field_i64(self.fields.doc_id, id));
        Ok(())
    }

    fn rebuild_fts(&self) -> Result<()> {
        {
            let mut w = self.writer.lock().unwrap();
            w.commit().map_err(tantivy_err)?;
        }
        self.reader.reload().map_err(tantivy_err)?;
        Ok(())
    }

    fn document_ids(&self) -> Result<Vec<i64>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let t = rtx.open_table(DOCUMENTS).map_err(redb_err)?;
        let mut ids = Vec::new();
        for entry in t.iter().map_err(redb_err)? {
            let (k, _) = entry.map_err(redb_err)?;
            ids.push(k.value());
        }
        Ok(ids)
    }

    fn document_count(&self) -> Result<u64> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let t = rtx.open_table(DOCUMENTS).map_err(redb_err)?;
        Ok(t.len().map_err(redb_err)?)
    }

    fn embed_pending(
        &self,
        embedder: &dyn Embedder,
        on_progress: &dyn Fn(usize, usize),
    ) -> Result<usize> {
        if self.dim.is_none() {
            return Ok(0);
        }

        // Collect (doc_id, line_start, text) for chunks that lack a vector.
        let embedded: HashSet<[u8; 16]> = {
            let rtx = self.db.begin_read().map_err(redb_err)?;
            let vecs = rtx.open_table(VECTORS).map_err(redb_err)?;
            let mut set = HashSet::new();
            for entry in vecs.iter().map_err(redb_err)? {
                let (k, _) = entry.map_err(redb_err)?;
                let mut key = [0u8; 16];
                key.copy_from_slice(k.value());
                set.insert(key);
            }
            set
        };

        let mut pending: Vec<(i64, usize, String)> = Vec::new();
        {
            let rtx = self.db.begin_read().map_err(redb_err)?;
            let docs = rtx.open_table(DOCUMENTS).map_err(redb_err)?;
            for entry in docs.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let doc_id = k.value();
                let Ok(rec) = serde_json::from_slice::<DocRecord>(v.value()) else {
                    continue;
                };
                for c in rec.chunks {
                    if !embedded.contains(&vkey(doc_id, c.line_start)) {
                        pending.push((doc_id, c.line_start, c.text));
                    }
                }
            }
        }

        let total = pending.len();
        let mut done = 0;
        for batch in pending.chunks(100) {
            let texts: Vec<&str> = batch.iter().map(|(_, _, t)| t.as_str()).collect();
            let embeddings = embedder.embed_texts(&texts)?;
            let wtx = self.db.begin_write().map_err(redb_err)?;
            {
                let mut vecs = wtx.open_table(VECTORS).map_err(redb_err)?;
                for ((doc_id, line_start, _), emb) in batch.iter().zip(embeddings.iter()) {
                    vecs.insert(vkey(*doc_id, *line_start).as_slice(), vec_serialize(emb).as_slice())
                        .map_err(redb_err)?;
                }
            }
            wtx.commit().map_err(redb_err)?;
            done += batch.len();
            on_progress(done, total);
        }
        Ok(total)
    }

    fn vec_info(&self) -> Result<VecInfo> {
        let Some(dim) = self.dim else {
            return Ok(VecInfo {
                embedding_dim: 0,
                vector_count: 0,
                pending_count: 0,
            });
        };
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let vector_count = rtx
            .open_table(VECTORS)
            .map_err(redb_err)?
            .len()
            .map_err(redb_err)?;
        let mut chunk_count: u64 = 0;
        let docs = rtx.open_table(DOCUMENTS).map_err(redb_err)?;
        for entry in docs.iter().map_err(redb_err)? {
            let (_, v) = entry.map_err(redb_err)?;
            if let Ok(rec) = serde_json::from_slice::<DocRecord>(v.value()) {
                chunk_count += rec.chunks.len() as u64;
            }
        }
        Ok(VecInfo {
            embedding_dim: dim,
            vector_count,
            pending_count: chunk_count.saturating_sub(vector_count),
        })
    }

    fn search_fts(&self, q: &FtsQuery<'_>) -> Result<Vec<FileSearchResult>> {
        self.reader.reload().map_err(tantivy_err)?;
        let searcher = self.reader.searcher();
        let mut qp = QueryParser::for_index(&self.index, vec![self.fields.text]);
        qp.set_conjunction_by_default();
        let query = match qp.parse_query(q.query) {
            Ok(query) => query,
            Err(_) => return Ok(Vec::new()), // unparseable / too-short query
        };
        let over_fetch = q.limit.saturating_mul(5).max(q.limit);
        let hits = searcher
            .search(&query, &TopDocs::with_limit(over_fetch))
            .map_err(tantivy_err)?;

        let prefix = q.path_prefix.map(|p| p.to_string_lossy().to_string());
        let mut rows: Vec<ChunkRow> = Vec::new();
        for (score, addr) in hits {
            let d: TantivyDocument = searcher.doc(addr).map_err(tantivy_err)?;
            let Some(doc_id) = d.get_first(self.fields.doc_id).and_then(|v| v.as_i64()) else {
                continue;
            };
            let line_start = d
                .get_first(self.fields.line_start)
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            let Some(rec) = self.get_doc(doc_id)? else {
                continue;
            };
            if let Some(pfx) = &prefix
                && !rec.path.starts_with(pfx.as_str())
            {
                continue;
            }
            let (line_end, text) = rec
                .chunks
                .iter()
                .find(|c| c.line_start == line_start)
                .map(|c| (c.line_end, c.text.clone()))
                .unwrap_or((line_start, String::new()));
            rows.push(ChunkRow {
                doc_id,
                path: rec.path,
                line_start,
                line_end,
                text,
                score: score as f64,
            });
        }
        // BM25: higher = better.
        Ok(group_by_file(rows, q.limit, |a, b| a > b))
    }

    fn search_similar(&self, q: &VectorQuery<'_>) -> Result<Vec<FileSearchResult>> {
        if self.dim.is_none() {
            return Ok(Vec::new());
        }
        let query_vecs = q.embedder.embed_texts(&[q.query])?;
        let query_vec = query_vecs
            .into_iter()
            .next()
            .ok_or_else(|| Error::Embed("embedder returned empty result".into()))?;

        let over_fetch = q.limit.saturating_mul(5).max(q.limit);

        // Brute-force scan: keep the best `over_fetch` by L2 distance.
        let mut scored: Vec<(f64, i64, usize)> = Vec::new();
        {
            let rtx = self.db.begin_read().map_err(redb_err)?;
            let vecs = rtx.open_table(VECTORS).map_err(redb_err)?;
            for entry in vecs.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let (doc_id, line_start) = vkey_parse(k.value());
                let emb = vec_deserialize(v.value());
                if emb.len() != query_vec.len() {
                    continue;
                }
                let dist = l2_distance(&query_vec, &emb);
                scored.push((dist, doc_id, line_start));
            }
        }
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(over_fetch);

        let prefix = q.path_prefix.map(|p| p.to_string_lossy().to_string());
        let mut rows: Vec<ChunkRow> = Vec::new();
        for (dist, doc_id, line_start) in scored {
            let Some(rec) = self.get_doc(doc_id)? else {
                continue;
            };
            if let Some(pfx) = &prefix
                && !rec.path.starts_with(pfx.as_str())
            {
                continue;
            }
            let (line_end, text) = rec
                .chunks
                .iter()
                .find(|c| c.line_start == line_start)
                .map(|c| (c.line_end, c.text.clone()))
                .unwrap_or((line_start, String::new()));
            rows.push(ChunkRow {
                doc_id,
                path: rec.path,
                line_start,
                line_end,
                text,
                score: dist,
            });
        }
        // L2 distance: lower = better.
        Ok(group_by_file(rows, q.limit, |a, b| a < b))
    }
}

// ── maintenance ────────────────────────────────────────────────────────────────

/// Delete the on-disk store at `dir` (used by a full rebuild).
pub fn wipe_store(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Directory name (under the workspace cache dir) for a redb retrieve store.
pub fn store_dir(base: &Path) -> PathBuf {
    base.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retrieve_store::Document;

    /// Deterministic embedder: banana→[1,0,0], cherry→[0,1,0], else→[0,0,1].
    struct FakeEmbedder;
    impl Embedder for FakeEmbedder {
        fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
            Ok(texts
                .iter()
                .map(|t| {
                    if t.contains("banana") {
                        vec![1.0, 0.0, 0.0]
                    } else if t.contains("cherry") {
                        vec![0.0, 1.0, 0.0]
                    } else {
                        vec![0.0, 0.0, 1.0]
                    }
                })
                .collect())
        }
    }

    fn doc(id: i64, path: &str, text: &str) -> Document {
        Document {
            id,
            body: String::new(),
            path: path.to_owned(),
            chunks: Some(vec![(0, 0, text.to_owned())]),
        }
    }

    #[test]
    fn fts_and_vector_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RedbStore::open(tmp.path(), Some(3)).unwrap();

        store.upsert_document(&doc(1, "/a.md", "the banana is yellow")).unwrap();
        store.upsert_document(&doc(2, "/b.md", "a cherry is red")).unwrap();
        store.rebuild_fts().unwrap();

        assert_eq!(store.document_count().unwrap(), 2);

        // Full-text search (trigram) finds the right document.
        let hits = store.search_fts(&FtsQuery::new("banana").limit(10)).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, 1);
        assert_eq!(hits[0].path, "/a.md");

        // Embed pending chunks, then semantic search.
        let embedder = FakeEmbedder;
        let embedded = store.embed_pending(&embedder, &|_, _| {}).unwrap();
        assert_eq!(embedded, 2);
        let info = store.vec_info().unwrap();
        assert_eq!(info.vector_count, 2);
        assert_eq!(info.pending_count, 0);

        let sem = store
            .search_similar(&VectorQuery::new("banana", &embedder).limit(10))
            .unwrap();
        assert_eq!(sem[0].id, 1, "closest vector should be the banana doc");

        // Removal drops the document from both stores.
        store.remove_document(1).unwrap();
        store.rebuild_fts().unwrap();
        assert_eq!(store.document_count().unwrap(), 1);
        let hits = store.search_fts(&FtsQuery::new("banana").limit(10)).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn persists_and_reopens_with_dim() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = RedbStore::open(tmp.path(), Some(3)).unwrap();
            store.upsert_document(&doc(1, "/a.md", "hello world text")).unwrap();
            store.rebuild_fts().unwrap();
        }
        // Reopen without passing dim: it is recovered from meta.
        let store = RedbStore::open(tmp.path(), None).unwrap();
        assert_eq!(store.dim(), Some(3));
        assert_eq!(store.document_count().unwrap(), 1);
        let hits = store.search_fts(&FtsQuery::new("world").limit(10)).unwrap();
        assert_eq!(hits.len(), 1);
    }
}
