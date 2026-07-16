# 作業引継ぎ（別ホストで再開するためのメモ）

最終更新: 2026-07-15 / 対象リポジトリ: `sapphire-framework`（旧 `sapphire-workspace` の履歴を継承）

全体設計は [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md) を参照。ここでは**現在地**と**次にやること**だけを書く。

## いまどこまで終わっているか

### ✅ Phase 0 — framework scaffold（完了・検証済）
- 旧 `sapphire-workspace` の全履歴（183コミット）を `git merge --allow-unrelated-histories` で取り込み。
- crate を `git mv` で `sapphire-framework-*` にリネーム（rename として履歴追跡）:
  - `crates/sapphire-framework-track` / `-retrieve` / `-sync` / `-workspace`（旧ルートlib） / `-workspace-cli`
- 依存宣言は Cargo の `package = "sapphire-framework-*"` エイリアスを使い、**コード内 extern 名（`sapphire_retrieve` 等）は不変**。
- root `Cargo.toml` を純 `[workspace]` 化（version `0.1.0`）。

### ✅ Phase 0c — キャッシュ SQLite 脱却（完了・検証済）
- `RedbStore`（redb + tantivy + brute-force vectors）を実装し既定バックエンドに。
- **`sqlite-store` は optional 化ではなく削除した**（後述の落とし穴を参照）。

### 🟡 Phase 1 — アプリ側差し替え（進行中）

| 項目 | 状態 |
|---|---|
| リモートを `sapphire-framework` にリネーム | ✅ 完了済（旧 `sapphire-workspace` リポジトリは消滅） |
| 親 `project-sapphire` の `.gitmodules` 更新 | ✅ workspace エントリ削除・framework 登録済 |
| framework の `sqlite-store` 削除 | ✅ ブランチ `feat/framework-migration` の `5b5d826` |
| `sapphire-journal` の依存差し替え | ✅ ブランチ `feat/framework-migration`（journal リポジトリ側） |
| `sapphire-agent` の依存差し替え | ✅ ブランチ `feat/framework-migration`（agent リポジトリ側） |
| `sapphire-ledger` を framework 初依存に | ⬜ **未着手**（何を使わせるかの設計から） |
| journal `cache.rs`（entries/tags）の redb 化 | ⬜ **未着手** |
| crates.io へ publish | ⬜ 未着手（アプリは暫定で git 依存） |

### 検証結果（この時点で緑・Windows ホストで実施）
- framework: `cargo check --workspace --all-targets` → 既存 dead_code 警告2件のみ。
  `cargo test -p sapphire-framework-retrieve --no-default-features --features redb-store` → **21 passed**。
  `cargo test -p sapphire-framework-workspace` → **15 passed**。`cargo tree --workspace` → **libsqlite3-sys / rusqlite = 0**。
- journal: `cargo check --workspace` 緑・全テストパス。`cargo tree -i libsqlite3-sys` → **grain-id / 自前 cache.rs 由来の単一系統のみ**。
- agent: `cargo check --workspace` 緑。**`cargo tree -i libsqlite3-sys` → matrix-sdk-sqlite 由来の単一系統のみ**（Phase 1 の受け入れ条件を達成）。

## いまの依存の繋ぎ方（重要）

アプリは framework を **git 依存**で参照している（crates.io 未公開のため）:

```toml
sapphire-workspace = { package = "sapphire-framework-workspace",
                       git = "https://github.com/fluo10/sapphire-framework",
                       branch = "feat/framework-migration", default-features = false }
```

- **アプリは crates.io に publish できない**（git 依存が含まれるため）。publish 前に framework を publish → version 依存へ差し替えが必要。
- **framework 側を直した場合、push しないとアプリ側に反映されない**。ローカルで回すときは各アプリの root `Cargo.toml` に
  一時的な `[patch."https://github.com/fluo10/sapphire-framework"]`（sibling submodule への path）を入れると速い。**コミットしないこと**。

## 次にやること

1. **`sapphire-ledger` を framework 初依存に**。ledger は現状 framework 依存ゼロ。何を使わせるか（workspace / retrieve / sync）の設計から。
2. **journal `cache.rs`（entries/tags の SQLite キャッシュ）の redb 化**。journal から SQLite を落とすには
   **grain-id の `rusqlite` feature も外す**必要がある（現在 `grain-id = { version = "0.15", features = ["serde", "rusqlite", "schemars"] }`）。
3. `feat/framework-migration` の **PR 作成 → main へマージ**（PR は未作成）。
4. crates.io へ `sapphire-framework-*` 0.1.0 を publish → アプリを version 依存へ。

## 落とし穴メモ

- **Cargo は feature が無効な optional 依存もバージョン解決し、`links` 衝突を検査する。**
  これが `sqlite-store` を optional で残せなかった理由（詳細は ARCHITECTURE.md）。同種の C ライブラリを足すときは同じ罠に注意。
- **`redb-store` を切ると永続ストアが消えて in-memory にフォールバックする**（エラーにならない）。
  `lancedb-store` はベクトル索引しか担わないので、**単独で有効にしても FTS/レコードは揮発する**。agent が実際にこの状態だったので既定に `redb-store` を追加した。
- **agent は `#![recursion_limit = "256"]` が必要**（`src/main.rs`）。framework 経由で redb/tantivy が型グラフに入ると、
  matrix-sdk の E2EE future の `Send` 証明が既定の再帰上限を超える。
- **tantivy 0.24** / **redb 2.6**。trigram は `NgramTokenizer(3,3,false)`＋`LowerCaser`。3文字未満クエリは無マッチ（FTS5 trigram と同じ）。
- **`RedbStore` は開いている間 tantivy の `IndexWriter`（50MB budget）を保持し続ける**（`redb_store.rs` の `index.writer(50_000_000)`）。
  読み取り専用でも常駐し、writer ロックを握るので同一ストアを複数プロセスから開けない。将来の改善候補。
- **ベクトル検索は全チャンクのスコアを一旦 Vec に貯めて全体ソートしている**（`search_similar`）。O(N) メモリ・O(N log N)。
  `over_fetch` 件の `BinaryHeap` にすれば O(k) にできる。10万チャンクで約2.4MB なので実害は小さい。
- **Windows: `Workspace::from_root` は root を canonicalize する**（`\\?\` UNC 接頭辞が付く）。
  テストでパスを比較するときは `tmp.path()` ではなく `ws.root` 起点で組むこと。
- rust-analyzer の cfg 判定が feature 再編で一時的にズレることがある。**権威は `cargo check --manifest-path <framework>/Cargo.toml`**。
- シェルの cwd がたまに親 `project-sapphire`（Cargo.toml 無し）に戻る。`--manifest-path` 指定が安全。
