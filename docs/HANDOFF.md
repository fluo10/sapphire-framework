# 作業引継ぎ（別ホストで再開するためのメモ）

最終更新: 2026-07-15 / 対象リポジトリ: `sapphire-framework`（旧 `sapphire-workspace` の履歴を継承）

全体設計は [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md) を参照。ここでは**現在地**と**次にやること**だけを書く。

## いまどこまで終わっているか

### ✅ Phase 0 — framework scaffold（完了・検証済）
- 旧 `sapphire-workspace` の全履歴（183コミット）を `git merge --allow-unrelated-histories` で取り込み。
- crate を `git mv` で `sapphire-framework-*` にリネーム（rename として履歴追跡）:
  - `crates/sapphire-framework-track` / `-retrieve` / `-sync` / `-workspace`（旧ルートlib） / `-workspace-cli`
- 依存宣言は Cargo の `package = "sapphire-framework-*"` エイリアスを使い、**コード内 extern 名（`sapphire_retrieve` 等）は不変**。
- root `Cargo.toml` を純 `[workspace]` 化（version `0.1.0`）。`release-plz.toml` / `.github/workflows/release.yml` / `README.md` も新名へ。

### ✅ Phase 0c — キャッシュ SQLite 脱却（完了・検証済）
- **`sapphire-framework-retrieve` に `RedbStore`（redb + tantivy + brute-force vectors）を実装し、既定バックエンドに。**
  - `crates/sapphire-framework-retrieve/src/redb_store.rs`（新規、単体テスト2本付き）。
  - redb=レコード保管、tantivy=trigram 転置インデックス（FTS5 trigram 相当・CJK対応・BM25）、ベクトルは redb 上を L2 で総当たり。
  - 共有ヘルパー（`ChunkRow`/`group_by_file`/`vec_serialize`/`vec_deserialize`/`l2_distance`）を `src/vector_store.rs` に集約。
- **feature 再編**: `redb-store`（既定）/ `sqlite-store`（optional-legacy）/ `lancedb-store` / `fastembed-embed`。
  retrieve・workspace・workspace-cli の各 `Cargo.toml` の `default` から sqlite/lancedb を外し `redb-store` に。
- `db.rs`: `BackendState::Redb` + `open`/`rebuild`/`init_redb_vec` + factory `open_redb`/`open_redb_vec`。
- `config.rs`: `VectorDb::Redb` 追加。`workspace_state.rs`: `open_initial_backend`/`make_vector_backend`/`open_initial_track` を redb 優先に。
- `workspace/src/error.rs`: `RedbStoreNotEnabled` 追加。`workspace/src/lib.rs`: `RETRIEVE_SCHEMA_VERSION` に redb 時 `0` のフォールバック。

### 検証結果（この時点で緑）
- `cargo check --workspace`（default=redb-store）→ 成功（既存 dead_code 警告2件のみ: workspace-cli の `RecallServer.tool_router`）。
- `cargo test -p sapphire-framework-retrieve --no-default-features --features redb-store` → **21 passed, 0 failed**（redb FTS/ベクトル/永続化の実挙動を含む）。
- `cargo tree --workspace`（default）→ **libsqlite3-sys = 0 / rusqlite = 0**（SQLite 完全排除）、redb・tantivy 在り。

> ⚠️ 未実行: `lancedb-store` / `fastembed-embed` のフル default ビルド（fastembed=ONNX で重い）。
> `fastembed-embed` は default に残っているので `cargo build --workspace`（check ではなく build）は ONNX ビルドで時間がかかる点に注意。
> リネーム以外コードは変えていないので通る見込みだが未確認。

## 現在のリポジトリ状態

- ブランチ: `main`。マージコミット（旧 workspace 履歴取り込み）まではコミット済み。
- **Phase 0a/0c の変更は作業ツリー（未コミット or 引継ぎ用の単一コミット、下記参照）にある。**
  `git status` / `git log` で確認。
- `.gitmodules`（親 project-sapphire 側）の framework 登録・workspace 削除は**未実施**（Phase 1 で行う）。

## 次にやること — Phase 1

1. **旧 `sapphire-workspace` の後始末**: リモートを `sapphire-framework` にリネーム（GitHub 設定）。
   親 `project-sapphire` の `.gitmodules` を framework 追加・workspace 削除に更新。
2. **`sapphire-framework-retrieve` の DocStore/VectorStore 論理仕上げ**: 現状 `RetrieveStore` 一枚に統合済み。
   同期層で「Change はドキュメントのみ運ぶ（vectors 非同期）」を守る前提。trait を物理分割する必要は当面なし（ユーザー合意）。
3. **アプリ側の依存差し替え**（別リポジトリ）:
   - `sapphire-journal`（`sapphire-journal-core` 他）・`sapphire-agent` の `sapphire-workspace = "0.12.1"` を
     `{ package = "sapphire-framework-workspace", version = "0.1", ... }` などへ（extern 名 `sapphire_workspace` は維持できる）。
   - **journal-core `cache.rs`（rusqlite 直使用）を redb 化**（journal も SQLite を落とす方向。journal は matrix 非依存なので急がないが方針として）。
   - `sapphire-ledger` を framework 初依存に。
   - 各アプリの feature 既定から sqlite/lancedb を外し redb に寄せる。agent は既に lancedb 使用なので据え置き可。
4. 検証: 3アプリ native `cargo build/test`。agent の matrix 共存で `cargo tree -i libsqlite3-sys` が matrix 由来の単一系統のみになること。

## 落とし穴メモ

- **tantivy 0.24** / **redb 2.6** 使用。tantivy の trigram は `NgramTokenizer(3,3,false)`＋`LowerCaser`。3文字未満クエリは無マッチ（FTS5 trigram と同じ）。
- rust-analyzer の cfg 判定が feature 再編で一時的にズレることがある。**権威は `cargo check --manifest-path <framework>/Cargo.toml`**。
- このホストは低スペック VM。`cargo build`（特に fastembed/lancedb/tantivy 初回）は重い。`cargo check` と feature 限定（`--no-default-features --features redb-store`）で回すのが速い。
- シェルの cwd がたまに親 `project-sapphire`（Cargo.toml 無し）に戻る。`--manifest-path` 指定が安全。
