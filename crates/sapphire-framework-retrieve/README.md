# sapphire-retrieve

Full-text and semantic search library extracted from [sapphire-journal](https://github.com/fluo10/sapphire-journal).

## What this crate provides

- **Full-text search** — trigram search over a tantivy index (`RetrieveDb::search_fts`)
- **Vector search** — brute-force nearest-neighbour search over stored embeddings (`RetrieveDb::search_similar`)
- **Chunker** — splits documents into overlapping text chunks for embedding (`chunker::chunk_document`)
- **Embedder trait** — pluggable embedding backends (`build_embedder`)
  - `openai` — OpenAI-compatible REST API
  - `ollama` — local Ollama server
  - `fastembed` *(feature: `fastembed-embed`)* — local ONNX inference, no server required
- **Config types** — `RetrieveConfig`, `VectorDb`, `EmbeddingConfig` in `sapphire_retrieve::config`

The store is pure Rust — redb for the records, tantivy for the full-text
index — so nothing here pulls a C library into a downstream binary.

## Features

| Feature | Default | Description |
|---|---|---|
| `redb-store` | yes | redb records + tantivy full-text index + brute-force vectors |
| `fastembed-embed` | yes | Local ONNX embedding via fastembed |

## License

MIT OR Apache-2.0
