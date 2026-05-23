# Releasing

このリポジトリのリリースは [release-plz](https://release-plz.dev/) で自動化しています。

## 通常運用

1. `main` に変更が push されると release-plz が自動で release PR を作成・更新します。
2. release PR の内容（バージョン、`CHANGELOG.md`、`Cargo.toml`）を確認します。
3. PR をマージすると、`v{version}` タグ・GitHub release・crates.io への publish が自動で行われ、CLI バイナリも各プラットフォーム向けにビルド・アップロードされます。

## バージョン決定ルール

release-plz は conventional commits からバージョンを推論します（pre-1.0 の `0.x.y` を前提）。

- `feat:` / `fix:` / `chore(deps):` などの非破壊的変更 → patch bump
- `feat!:` / `chore!:` あるいは commit footer の `BREAKING CHANGE:` → minor bump
- 自分のコードの公開 API を変更した場合（関数シグネチャ、公開構造体のフィールド等） → minor bump（明示的に `!` を付ける）

エラー型に `#[from] some_crate::Error` で外部 crate を露出している場合、依存メジャー bump は理屈上 breaking ですが、実害がほぼないため通常は patch のままで構いません（コミュニティ的にも pragmatic な扱いが多数派）。

## `-sys` crate 依存更新の特例

`-sys` crate（`libgit2-sys`, `libsqlite3-sys`, `openssl-sys` など、Cargo.toml に `links = "..."` を持つ crate）のメジャーバージョン更新は **必ず minor bump** にします。

理由: Cargo は `links` キーが同じ crate の異なるメジャーバージョンが依存グラフ内に同居することを許しません。patch リリースで `-sys` のメジャーを上げると、別の `-sys` メジャーを使うダウンストリームクレートが build 失敗します（API 互換性ではなく hard error）。

[Cargo SemVer guide の `-sys` セクション](https://doc.rust-lang.org/cargo/reference/semver.html) でも明示的に major change として扱われています。

### 対象になる依存（例）

このリポジトリで `-sys` を引き込んでいる主な依存:

- `git2` → `libgit2-sys`
- `rusqlite` → `libsqlite3-sys`
- `lancedb` の連鎖依存に `arrow-*` 経由で複数の `-sys` クレートが含まれることあり

### 手順

依存更新の release PR が出てきたら CHANGELOG を確認し、上記カテゴリの crate が含まれていれば：

1. PR のブランチに直接 push して `Cargo.toml` の `workspace.package.version` と関連する内部依存指定を patch から minor に書き換える
2. `CHANGELOG.md` の該当バージョン見出し（および compare URL）も合わせて書き換える
3. 理由を CHANGELOG エントリに一行添える（例: 「pulls in a new major of `libgit2-sys`」）

## 必要な secrets

- `RELEASE_PLZ_TOKEN`: fine-grained PAT、Contents + Pull requests の read/write
- `CARGO_REGISTRY_TOKEN`: crates.io publish 用
