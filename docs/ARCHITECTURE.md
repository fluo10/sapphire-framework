# sapphire-framework アーキテクチャ

> project-sapphire（`sapphire-journal` / `sapphire-agent` / `sapphire-ledger` と基盤 `sapphire-framework`）
> のローカルファースト基盤の設計ドキュメント。このリポジトリは旧 `sapphire-workspace` の履歴を
> 引き継いでおり、将来 `sapphire-workspace` リモートを `sapphire-framework` にリネームする前提。

## 背景と目的

各アプリは「**ファイルを原本・ローカルDBをキャッシュ**」というローカルファースト設計＋ git 同期＋MCP 連携を共有する。
現状の課題と、それに対する本設計の方針:

1. **git 同期はタイムラグがあり共同編集に不便** → Patroni による Postgres 分散を活かした中央集権型の
   *リモートワークスペース* を選択肢に追加する。ただし **サーバもクライアントと対称**（ファイル原本＋DBキャッシュ）にし、
   低レイテンシは git ではなく「サーバ仲介の change_log + 差分同期」で得る。
2. **職場でネイティブアプリのインストールに懸念** → 環境を汚さない **WASM 版 journal**。
   ローカルキャッシュ(IndexedDB/OPFS)を持ち、リモートとのやり取りを「差分同期だけ」に絞る。
3. 共通機能は「workspace」の枠を超えるため、新リポジトリ **`sapphire-framework`**（全crate `sapphire-framework-*` プレフィクス）に集約する。

## 確定した方針（ユーザー合意済み）

- **framework 移行を土台**にし、remote/WASM はその上の実装として後続フェーズ。
- `sapphire-workspace` は **framework に吸収し廃止**。全crateは **`sapphire-framework-*`** プレフィクス
  （`sapphire-*` 一般名前空間を占有しないため意図的に長くする）。crates.io では新規crate名で publish。
  移行時のコード改変を最小化するため、依存宣言で Cargo の `package = "..."` エイリアスを使い、
  **コード内の extern 名（`sapphire_retrieve` 等）はそのまま維持**している。
- **キャッシュは純Rust製にして SQLite 依存を排除**（後述）。matrix-sdk の rusqlite ピンに縛られないため。
- remote 通信は **JSON-RPC 2.0 over HTTP**（MCP と同系＝統一感）。
- サーバも「ファイル原本＋DBキャッシュ」で対称化（**Model B**）。storage backend を抽象化し、
  v1=ファイル原本+SQLite/redb+FSブロブ、将来=Postgres原本+S3ブロブ に差し替え可能に。
- **ベクター索引は同期対象外**。リモート/ローカルが各自保持し、オフライン=軽量モデル、オンライン=サーバ大モデル。
  差分同期が運ぶのは**ドキュメント本体（テキスト+メタ+バイナリブロブ参照）のみ**。
- native も WASM も「**ローカルキャッシュ＋リモート差分同期**」という同一構造。git-file 同期と remote-JSON-RPC 同期は
  同じ `ChangeSource` 抽象の2実装。

## キャッシュバックエンド: SQLite 脱却（redb + tantivy）

**なぜ**: `sqlite-store` を必須にすると、sapphire-agent の matrix-sdk（rusqlite 0.37 / libsqlite3-sys 0.35 にピン）と
`libsqlite3-sys`（`links="sqlite3"`）が衝突しうる。調査の結果 **agent は元々 sqlite-store を使わず lancedb を使用**しており
衝突は未顕在だった。matrix に rusqlite バージョンを縛られないよう、framework のキャッシュから SQLite を無くす。

**sqlite-store は optional 化では不十分だったので削除済み**（2026-07-15）。
Cargo は **feature が無効な optional 依存もバージョン解決の対象にし**、`links` の一意性をそこで検査する。
そのため `sqlite-store` を切っていても framework の rusqlite はグラフに残り、ピン先が全消費者の制約になっていた:

- 0.37 にピン（matrix 合わせ）→ **journal が解決不能**（grain-id が rusqlite 0.39 / libsqlite3-sys 0.37 を要求）
- 0.39 に変更 → **agent が解決不能**（matrix-sdk-sqlite 経由）

両立する単一バージョンが存在しないため、optional のまま残すのではなく `sqlite_store.rs` ごと削除した。
これに伴い `VectorDb::SqliteVec` / `Error::SqliteStoreNotEnabled` / `open_sqlite_fts` / `open_sqlite_vec` /
`RetrieveDb::init_sqlite_vec` も廃止。`RETRIEVE_SCHEMA_VERSION` は常に 0（redb が自前で on-disk 形式を管理するため）。
**レガシー DB からの移行パスは無く、`db = "sqlite_vec"` の設定は `db = "redb"` に変更が必要。**

**構成**（`sapphire-framework-retrieve`）:

- **`RetrieveStore` trait**（同期）が統一インターフェース（`upsert_document`/`remove_document`/`rebuild_fts`/
  `document_ids`/`document_count`/`embed_pending`/`vec_info`/`search_fts`/`search_similar`/`search_hybrid`）。
  mtime 追跡は責務外（`sapphire-framework-track` の `TrackStore` が持つ）。
- **唯一の永続実装 = `RedbStore`（redb + tantivy + brute-force vectors）**。C依存ゼロ・純Rust。
  `redb-store` を切ると揮発する in-memory ストアにフォールバックするだけなので、
  **各アプリは `redb-store` を既定に入れること**（`lancedb-store` はベクトル索引しか担わない）。
  - **redb** = 正本レコード保管。`documents: doc_id -> {path, chunks}`、`vectors: (doc_id,line_start) -> f32[]`、`meta`。
  - **tantivy** = redb から作る転置インデックス。**trigram トークナイザ**（`NgramTokenizer(3,3)`）で
    旧 FTS5 `trigram` 相当（substring・CJK 対応）。BM25 ランキング。索引は redb から再構築可能。
  - **ベクトル検索は brute-force**（redb 上の全ベクトルを L2 距離でスキャン）。数万件までミリ秒未満で厳密。
    規模が要求したら HNSW（`instant-distance` 等の純Rust）に差し替え可能。lancedb は重い場所のみ任意。
  - **VectorStore を別 trait に切らず redb に統合**（vectors は同期対象外。非同期性は sync 層＝Change がドキュメントのみ運ぶことで担保）。
- **feature**: `redb-store`（既定）/ `lancedb-store` / `fastembed-embed`。`sqlite-store` は削除済み（上記参照）。
  `VectorDb` config enum は `None` / `Redb`（既定のブルートフォース）/ `LanceDb`。

ストア分離の共有ヘルパー（`ChunkRow` / `group_by_file` / `vec_serialize` / `vec_deserialize` / `l2_distance`）は
`vector_store.rs` に集約し、sqlite / redb 両バックエンドで共用。

## crate 構成（目標）

Cargo workspace（モノレポ）。既存済み ✅ / 予定 ⬜。

| crate | 役割 | 状態 |
|---|---|---|
| `sapphire-framework-track` | mtime 変更検知 `TrackStore`（redb） | ✅ 移設済 |
| `sapphire-framework-retrieve` | 検索。`RetrieveStore` + `RedbStore`(redb+tantivy) 既定。sqlite/lancedb は optional | ✅ 移設+redb実装済 |
| `sapphire-framework-sync` | 同期抽象 `SyncBackend` + 新 `ChangeSource`（git/remote 2実装）。git実装 | ✅ 移設済 / ⬜ ChangeSource |
| `sapphire-framework-workspace` | `AppContext`/`Workspace`/`WorkspaceState`/`IndexHook`（旧ルートlib） | ✅ 移設済 |
| `sapphire-framework-workspace-cli` | rmcp ベース MCP サーバ含む参照 CLI | ✅ 移設済 |
| `sapphire-framework-rpc` | client/server 共有 JSON-RPC 型/メソッド定義 | ⬜ |
| `sapphire-framework-remote-client` | JSON-RPC 差分同期クライアント（reqwest, `ChangeSource` remote 実装） | ⬜ |
| `sapphire-framework-remote-server` | axum JSON-RPC 同期/検索サーバ（v1=ファイル原本+SQLite/redb+change_log） | ⬜ |
| `sapphire-framework-blob` | バイナリブロブ抽象 `BlobStore`（FS/OPFS/将来S3） | ⬜ |
| `sapphire-framework-backend` | GUI 向け**非同期** Backend trait + Local/Remote 実装、`BackendEvent` | ⬜ |
| `sapphire-framework-mcp` | rmcp ベース MCP 骨格（`RecallServer` 汎用化 + stdio/http transport） | ⬜ |
| `sapphire-framework-cache-wasm` | wasm 専用: IndexedDB/OPFS の track/entries + substring 検索 | ⬜ |

## GUI 向け 非同期 Backend trait（後続フェーズ）

現在 GUI が直接呼ぶ `ops::*`（`&rusqlite::Connection` を引数に取る）と `JournalState::*` を、
GUI が依存する `#[async_trait(?Send)]` の `JournalBackend` に集約する（`list_entries`/`get_entry`/`create_entry`/
`update_entry`/`remove_entry`/`search`/`sync`/`subscribe`）。`&Connection` は Local 実装内部に隠蔽。

- **`LocalJournalBackend`**（native）= 既存同期 `JournalState`/`ops` を `spawn_blocking` で包む。純粋ロジックは残置。
- **`RemoteJournalBackend`**（remote/WASM 共通）= JSON-RPC 差分同期でローカルキャッシュ（native=redb / wasm=IndexedDB）を更新。
- egui は `dyn` を跨スレッド送信せず具象型を保持して `runtime.spawn`（既存 app.rs パターン）。WASM は `spawn_local`。

## remote 同期 API（JSON-RPC・後続フェーズ）

サーバ v1 = ファイル原本 + DBキャッシュ + `change_log`（`seq` 単調増加・tombstone）。cursor = 最後に取り込んだ `seq`。
メソッド（Bearer トークン=デバイス単位）:

```
workspace.snapshot  {ws}                         -> {cursor, docs[]}
changes.pull        {ws, since, limit}           -> {cursor, changes[], more}   textメタ+blob参照
changes.push        {ws, base_cursor, changes[]} -> {cursor, conflicts[]} / err:conflict
blob.get/put        {ws, hash[, bytes:base64]}   -> content-addressed バイナリ
search.fts          {ws, q, limit}               -> サーバFTS（wasm FTS代替）
search.semantic     {ws, q, limit}               -> サーバ大モデルでベクター検索（online委譲）
```

`ChangeSource` trait（`sapphire-framework-sync`、git/remote 2実装）: `pull`/`push`/`snapshot`。
競合は MVP で LWW(`updated_at`)+tombstone+`conflict`再pull。CRDT は後続。

## 実装フェーズ

- **Phase 0**（scaffold）✅: 履歴保持で crate 移設・`sapphire-framework-*` リネーム。
- **Phase 0c**（キャッシュ SQLite 脱却）✅: `RedbStore`(redb+tantivy+brute-force) を既定に。**sqlite-store は削除済み**。
- **Phase 1** 🟡（進行中）: リモートのリネーム ✅・`.gitmodules` 更新 ✅・journal ✅ / agent ✅ の依存差し替え。
  残: **ledger の framework 初依存**、**journal `cache.rs`（entries/tags）の redb 化**（grain-id の `rusqlite` feature も要除去）、
  crates.io への publish（現状アプリは git 依存なので publish 不可）。
- **Phase 2** ⬜: 非同期 `JournalBackend` 導入 + journal desktop GUI リファクタ。
- **Phase 3** ⬜: remote server + JSON-RPC + RemoteBackend + BlobStore。
- **Phase 4** ⬜: WASM cache（IndexedDB/OPFS）+ WASM journal frontend。

## 既知のリスク / 難所

1. 同期→非同期の波及は Backend trait のみ async 化で封じる（`ops::update_entry(&Connection,...)` の `&Connection` を trait から外す破壊的変更）。
2. egui native の async: `?Send` により `dyn` は跨スレッド不可 → 具象型保持 + `runtime.spawn`。
3. WASM 非互換（lancedb/arrow・rusqlite・git2・fastembed・sqlx-postgres・tantivy/redb）は `cfg(not(wasm32))` / 独立バイナリで隔離。
   共有型は serde-only の `sapphire-framework-rpc` に。
4. `GrainId`/uuid v7 の wasm 時刻: `SystemTime::now()` trap → `getrandom/js` + `js_sys::Date::now()`。要検証。
5. tantivy trigram FTS の挙動同等性（BM25・prefix フィルタ・短いクエリ<3文字は無マッチ＝FTS5同等）。
6. storage backend の将来差替（Postgres+S3）。`OriginStore`/`BlobStore` trait を切る。content-addressed hash の GC は後続。
