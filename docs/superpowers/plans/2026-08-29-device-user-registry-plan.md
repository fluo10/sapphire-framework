# デバイス／ユーザー台帳（registry）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** アプリごとの `devices.toml` / `users.toml` を読み書きする `sapphire-framework-registry` クレートを追加し、`KeyEntry` が `device_id` でデバイスを指せるようにする。

**Architecture:** 新規クレートは `&Path` を受け取るだけの純粋なファイル形式ライブラリで、`sapphire-framework-workspace` には依存しない（retrieve / track / tokio full を引かないため）。ファイル形式の作法は既存の `KeyStore`（`crates/sapphire-framework-remote-server/src/keys.rs`）を踏襲する — 先頭に書式説明ヘッダを毎回再生成、保存は全上書き、書き込みは一時ファイル → rename、欠けた `id` / `created_at` は load 時に補完して書き戻す。パス規約は `Workspace` 側のメソッドで 1 箇所に決める。

**Tech Stack:** Rust 2024 edition, `grain-id` 0.16（serde feature）, `serde`, `toml` 1.1, `chrono`, `thiserror` 2

**Spec:** `docs/superpowers/specs/2026-08-29-device-user-registry-design.md`

## Global Constraints

- ID は **grain-id**（`grain_id::GrainId`）。`KeyEntry::id` だけは **UUID のまま**据え置く。
- registry クレートの依存は `grain-id` / `serde` / `toml` / `chrono` / `thiserror` に限る。`sapphire-framework-workspace` にも `sapphire-framework-retrieve` にも依存させない。
- `grain-id` のバージョンは **0.16**、features は `["serde"]`。ワークスペースの `[workspace.dependencies]` に追加し、各クレートは `grain-id.workspace = true` で参照する。
- registry のファイルは**秘密を含まない**ので、`keys.rs` の `create_private`（0600）は使わない。通常の `File::create` でよい。
- ドキュメントコメントは既存クレートに合わせて**日本語**で書く（`keys.rs` がそうなっている）。
- 既存の鍵ファイル（`device_id` の無い `[[key]]`）は**そのまま読めなければならない**。

---

### Task 1: registry クレートと `User` / `Users`

**Files:**
- Create: `crates/sapphire-framework-registry/Cargo.toml`
- Create: `crates/sapphire-framework-registry/src/lib.rs`
- Create: `crates/sapphire-framework-registry/src/error.rs`
- Create: `crates/sapphire-framework-registry/src/store.rs`
- Create: `crates/sapphire-framework-registry/src/users.rs`
- Modify: `Cargo.toml`（`[workspace] members` と `[workspace.dependencies]`）
- Modify: `crates/sapphire-framework/Cargo.toml`（`registry` feature と dep）
- Modify: `crates/sapphire-framework/src/lib.rs`（モジュール表・re-export・prelude）
- Test: `crates/sapphire-framework-registry/src/users.rs`（`#[cfg(test)] mod tests`、`keys.rs` と同じくインライン）

**Interfaces:**
- Consumes: なし（最初のタスク）
- Produces:
  - `sapphire_framework_registry::{Error, Result}`
  - `crate::store::write_atomic(path: &Path, header: &str, body: &str) -> Result<()>`（`pub(crate)`）
  - `sapphire_framework_registry::{User, Users}`
  - `User { id: GrainId, name: String, description: Option<String>, created_at: DateTime<Utc>, retired_at: Option<DateTime<Utc>> }`
  - `Users::load(path: &Path) -> Result<Users>`
  - `Users::entries(&self) -> &[User]`
  - `Users::add(&mut self, name: &str, description: Option<String>) -> Result<User>`
  - `Users::get(&self, id: GrainId) -> Option<&User>`
  - `Users::resolve(&self, selector: &str) -> Result<&User>`
  - `Users::retire(&mut self, selector: &str) -> Result<User>`
  - `Users::purge(&mut self, selector: &str) -> Result<User>`
  - facade: `sapphire_framework::registry`（`registry` feature）

- [ ] **Step 1: クレートをワークスペースに登録する**

`Cargo.toml` の `members` に 1 行足す（`crates/sapphire-framework-remote-client` の次）:

```toml
    "crates/sapphire-framework-registry",
```

`[workspace.dependencies]` に足す（この表はアルファベット順ではないので末尾でよい）:

```toml
grain-id = { version = "0.16", features = ["serde"] }
```

- [ ] **Step 2: クレートの `Cargo.toml` を作る**

`crates/sapphire-framework-registry/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-registry"
edition.workspace = true
version.workspace = true
description = "Per-app device and user tables for sapphire-framework workspaces"
license.workspace = true
repository.workspace = true
keywords = ["registry", "device", "user", "local-first"]
categories = ["filesystem"]

[dependencies]
grain-id.workspace = true
chrono.workspace = true
serde.workspace = true
thiserror.workspace = true
toml.workspace = true

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: `error.rs` を書く**

```rust
use thiserror::Error;

/// 台帳ファイルの読み書きで起きる失敗。
#[derive(Debug, Error)]
pub enum Error {
    /// ファイル操作が失敗した。
    #[error("registry io error: {0}")]
    Io(#[from] std::io::Error),

    /// パース・保存に失敗した、id / name が重複していた、または
    /// セレクタがどのエントリにも解決しなかった。
    #[error("registry file error: {0}")]
    File(String),
}

pub type Result<T> = std::result::Result<T, Error>;
```

- [ ] **Step 4: `store.rs` を書く**

```rust
//! 台帳ファイル共通の書き出し。
//!
//! 内容は `users.rs` / `devices.rs` がそれぞれ組み立てる。ここが受け持つのは
//! 「ヘッダ + 本文を、途中で壊れない形で置く」ことだけ。

use std::path::Path;

use crate::error::{Error, Result};

/// `header` + 空行 + `body` を **一時ファイル → rename** で書き出す。
///
/// その場で truncate すると、書き込み中にクラッシュした瞬間に台帳が消える。
/// `device_id` はジャーナルのフロントマターに焼かれていて、台帳を失うと過去の
/// 参照が解決できなくなるので、`KeyStore::save_entries` と同じ手口を取る。
///
/// `keys.rs` と違って 0600 では作らない。この台帳に秘密は無く（トークンは
/// 鍵ファイル側にある）、ワークスペースごと同期される前提のファイルなので、
/// 所有者限定のパーミッションは意味を持たない。
pub(crate) fn write_atomic(path: &Path, header: &str, body: &str) -> Result<()> {
    use std::io::Write as _;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let file_name = path
        .file_name()
        .ok_or_else(|| Error::File(format!("{} is not a file path", path.display())))?;
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(".tmp");
    let tmp = parent.join(tmp_name);

    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(header.as_bytes())?;
        file.write_all(b"\n")?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(())
}
```

- [ ] **Step 5: `lib.rs` を書く**

```rust
//! アプリごとのデバイス／ユーザー台帳。
//!
//! `.{app_name}/devices.toml` と `.{app_name}/users.toml` を読み書きする。
//! ID はアプリの中だけで意味を持ち、アプリ間で共有しない — 各アプリは互いに
//! MCP などの API 越しに 1 つのクライアントデバイスとして映る。
//!
//! パスの規約は `sapphire-framework-workspace` の `Workspace::devices_path` /
//! `users_path` が持つ。このクレートは `&Path` を受け取るだけで、ワークスペースの
//! 解決には関わらない。

mod error;
mod store;
mod users;

pub use error::{Error, Result};
pub use users::{User, Users};
```

- [ ] **Step 6: 失敗するテストを書く**

`crates/sapphire-framework-registry/src/users.rs` の末尾に:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.toml");
        (dir, path)
    }

    #[test]
    fn add_then_reload_round_trips() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("fluo10", Some("人間".into())).unwrap();

        let reloaded = Users::load(&path).unwrap();

        assert_eq!(reloaded.entries(), &[added]);
    }

    #[test]
    fn a_missing_file_loads_as_empty_and_is_not_created() {
        let (_d, path) = tmp();
        let users = Users::load(&path).unwrap();
        assert!(users.entries().is_empty());
        assert!(!path.exists(), "load はファイルを作らない");
    }

    #[test]
    fn load_fills_in_a_hand_written_entry_and_writes_it_back() {
        let (_d, path) = tmp();
        // ヘッダが案内する通り、name だけの手書きエントリ。
        std::fs::write(&path, "[[user]]\nname = \"fluo10\"\n").unwrap();

        let users = Users::load(&path).unwrap();

        assert_eq!(users.entries().len(), 1);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("id = "), "補完した id が書き戻されていない: {text}");
        assert!(text.contains("created_at = "), "{text}");
    }

    #[test]
    fn load_rejects_two_users_sharing_an_id() {
        let (_d, path) = tmp();
        std::fs::write(
            &path,
            "[[user]]\nid = \"a3f9k2p\"\nname = \"a\"\n\n\
             [[user]]\nid = \"a3f9k2p\"\nname = \"b\"\n",
        )
        .unwrap();

        let err = Users::load(&path).unwrap_err();

        assert!(err.to_string().contains("a3f9k2p"), "{err}");
    }

    #[test]
    fn load_rejects_two_users_sharing_a_name() {
        let (_d, path) = tmp();
        std::fs::write(&path, "[[user]]\nname = \"dup\"\n\n[[user]]\nname = \"dup\"\n").unwrap();

        let err = Users::load(&path).unwrap_err();

        assert!(err.to_string().contains("dup"), "{err}");
    }

    #[test]
    fn add_rejects_a_duplicate_name() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        users.add("fluo10", None).unwrap();

        let err = users.add("fluo10", None).unwrap_err();

        assert!(err.to_string().contains("fluo10"), "{err}");
    }

    #[test]
    fn resolve_finds_by_id_and_by_name() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("fluo10", None).unwrap();

        assert_eq!(users.resolve("fluo10").unwrap(), &added);
        assert_eq!(users.resolve(&added.id.to_string()).unwrap(), &added);
    }

    #[test]
    fn resolve_errors_on_no_match() {
        let (_d, path) = tmp();
        let users = Users::load(&path).unwrap();
        assert!(users.resolve("nobody").is_err());
    }

    #[test]
    fn retire_keeps_the_entry_resolvable() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("gone", None).unwrap();

        let retired = users.retire("gone").unwrap();

        assert!(retired.retired_at.is_some());
        // 履歴の解決のために残る、というのが retire の要点。
        assert_eq!(users.get(added.id).map(|u| u.id), Some(added.id));
        let reloaded = Users::load(&path).unwrap();
        assert!(reloaded.entries()[0].retired_at.is_some());
    }

    #[test]
    fn purge_removes_the_entry() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        users.add("gone", None).unwrap();

        users.purge("gone").unwrap();

        assert!(Users::load(&path).unwrap().entries().is_empty());
    }

    #[test]
    fn the_header_documents_every_field() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        users.add("fluo10", None).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();

        for field in ["id", "name", "description", "created_at", "retired_at"] {
            assert!(
                text.contains(&format!("# {field}")),
                "ヘッダが {field} を説明していない: {text}"
            );
        }
    }
}
```

- [ ] **Step 7: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-registry`
Expected: コンパイルエラー（`Users` が存在しない）

- [ ] **Step 8: `users.rs` の本体を書く**

テストの上（`#[cfg(test)] mod tests` の前）に:

```rust
//! ユーザー台帳（`.{app_name}/users.toml`）。
//!
//! 「最終更新者が人間か AI か」を表示するための逆引き先。ID はコンテンツには
//! 焼かれない（フロントマターに入るのは `device_id`）が、`devices.toml` の
//! `user_id` から参照される。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::store::write_atomic;

/// 保存時に毎回書き出す書式説明。
const HEADER: &str = "\
# sapphire users.
#
# One `[[user]]` table per person or agent. Hand-editing is fine: a table
# with just a `name` is a valid entry — the remaining fields are filled in
# and written back the next time this file is loaded.
#
# id          optional. A grain-id. Filled in on load when blank. Referred
#             to by `user_id` in devices.toml. Ids must be unique here.
# name        required. Unique within this file. Accepted in place of the
#             id anywhere a command asks for a user. A name that happens to
#             parse as a grain-id is never matched as a name, only as an id.
# description optional. A note for you.
# created_at  optional. RFC 3339. Filled in on load when blank.
# retired_at  optional. RFC 3339. Set when the user is retired. The entry
#             stays so historical references still resolve; only an explicit
#             purge removes it.
#
# This file is rewritten in full on every change; comments you add are lost.
";

/// 一人のユーザー。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: GrainId,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub retired_at: Option<DateTime<Utc>>,
}

impl User {
    /// 引退済みか。認証の可否には使わない（それは鍵ファイルの仕事）。
    pub fn is_retired(&self) -> bool {
        self.retired_at.is_some()
    }
}

/// ファイル上の表現。手書きを許すため `id` / `created_at` は省略可。
#[derive(Debug, Serialize, Deserialize)]
struct RawUser {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<GrainId>,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RawFile {
    #[serde(default)]
    user: Vec<RawUser>,
}

/// ユーザー台帳ファイルとその中身。
pub struct Users {
    path: PathBuf,
    entries: Vec<User>,
}

impl Users {
    /// 読み込み、欠けた `id` / `created_at` を補完する。補完があれば書き戻す。
    /// ファイルが無い場合は空の台帳を返す（作成はしない）。
    pub fn load(path: &Path) -> Result<Self> {
        let raw: RawFile = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::File(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RawFile::default(),
            Err(e) => return Err(Error::Io(e)),
        };

        // 重複したまま読み込むと resolve がどちらか決められない。エントリごと
        // コピーして複製する事故は実際に起きる。
        let mut seen_ids: HashSet<GrainId> = HashSet::new();
        let mut seen_names: HashSet<&str> = HashSet::new();
        for u in &raw.user {
            if let Some(id) = u.id
                && !seen_ids.insert(id)
            {
                return Err(Error::File(format!(
                    "{}: two users share the id {id}",
                    path.display()
                )));
            }
            if !seen_names.insert(u.name.as_str()) {
                return Err(Error::File(format!(
                    "{}: two users share the name {:?}",
                    path.display(),
                    u.name
                )));
            }
        }

        let mut filled = false;
        let now = Utc::now();
        let mut entries: Vec<User> = Vec::with_capacity(raw.user.len());
        for u in raw.user {
            if u.id.is_none() || u.created_at.is_none() {
                filled = true;
            }
            entries.push(User {
                id: u.id.unwrap_or_else(GrainId::random),
                name: u.name,
                description: u.description,
                created_at: u.created_at.unwrap_or(now),
                retired_at: u.retired_at,
            });
        }

        let store = Self {
            path: path.to_path_buf(),
            entries,
        };
        if filled {
            store.save()?;
        }
        Ok(store)
    }

    pub fn entries(&self) -> &[User] {
        &self.entries
    }

    /// 新しいユーザーを追加して保存する。`name` の重複は拒否する。
    pub fn add(&mut self, name: &str, description: Option<String>) -> Result<User> {
        if self.entries.iter().any(|u| u.name == name) {
            return Err(Error::File(format!("a user named {name:?} already exists")));
        }
        let entry = User {
            id: GrainId::random(),
            name: name.to_owned(),
            description,
            created_at: Utc::now(),
            retired_at: None,
        };
        let mut candidate = self.entries.clone();
        candidate.push(entry.clone());
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(entry)
    }

    pub fn get(&self, id: GrainId) -> Option<&User> {
        self.entries.iter().find(|u| u.id == id)
    }

    /// `selector` を 1 件のエントリの位置に解決する。
    ///
    /// grain-id として読めるなら id、読めないなら name。id も name もこの
    /// ファイル内で一意なので、複数一致は起こらない。名前が偶然 grain-id として
    /// 読めてしまう場合は id 側に回り「一致無し」で失敗する — 誤ったユーザーに
    /// 当たることはないが、name 側を強制する逃げ道は無い。`KeyStore::resolve`
    /// が UUID で同じ制約を持っている。
    fn index_of(&self, selector: &str) -> Result<usize> {
        let found = match selector.parse::<GrainId>() {
            Ok(id) => self.entries.iter().position(|u| u.id == id),
            Err(_) => self.entries.iter().position(|u| u.name == selector),
        };
        found.ok_or_else(|| Error::File(format!("no user matches {selector:?}")))
    }

    pub fn resolve(&self, selector: &str) -> Result<&User> {
        Ok(&self.entries[self.index_of(selector)?])
    }

    /// 引退させる。エントリは残るので、過去の参照は解決し続ける。
    /// 既に引退済みなら `retired_at` は上書きしない。
    pub fn retire(&mut self, selector: &str) -> Result<User> {
        let i = self.index_of(selector)?;
        let mut candidate = self.entries.clone();
        if candidate[i].retired_at.is_none() {
            candidate[i].retired_at = Some(Utc::now());
        }
        let retired = candidate[i].clone();
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(retired)
    }

    /// 本当に削除する。過去の参照は解決できなくなる。
    pub fn purge(&mut self, selector: &str) -> Result<User> {
        let i = self.index_of(selector)?;
        let mut candidate = self.entries.clone();
        let removed = candidate.remove(i);
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(removed)
    }

    fn save(&self) -> Result<()> {
        self.save_entries(&self.entries)
    }

    /// `entries` をヘッダ付きで全上書きする。`self.entries` には触れない —
    /// 呼び出し側は保存が成功してから代入すること。
    fn save_entries(&self, entries: &[User]) -> Result<()> {
        let raw = RawFile {
            user: entries
                .iter()
                .map(|u| RawUser {
                    id: Some(u.id),
                    name: u.name.clone(),
                    description: u.description.clone(),
                    created_at: Some(u.created_at),
                    retired_at: u.retired_at,
                })
                .collect(),
        };
        let body = toml::to_string_pretty(&raw)
            .map_err(|e| Error::File(format!("serializing users: {e}")))?;
        write_atomic(&self.path, HEADER, &body)
    }
}
```

- [ ] **Step 9: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-registry`
Expected: PASS（11 テスト）

- [ ] **Step 10: facade に生やす**

`crates/sapphire-framework/Cargo.toml` の `[features]` に足す（`rpc` の次の行）:

```toml
registry = ["dep:sapphire-framework-registry"]
```

`native` フィーチャを差し替える:

```toml
native = ["workspace", "backend", "remote-client", "remote-server", "registry"]
```

`[dependencies]` に足す:

```toml
sapphire-framework-registry = { version = "0.1.0", path = "../sapphire-framework-registry", optional = true }
```

`crates/sapphire-framework/src/lib.rs` のモジュール表に 1 行足す（`| rpc | ... |` の次）:

```
//! | `registry` | [`registry`] | `sapphire-framework-registry` |
```

re-export を足す（`pub use sapphire_framework_rpc as rpc;` の次）:

```rust
#[cfg(feature = "registry")]
pub use sapphire_framework_registry as registry;
```

prelude に足す:

```rust
    #[cfg(feature = "registry")]
    pub use crate::registry::{User, Users};
```

- [ ] **Step 11: facade がビルドできることを確認する**

Run: `cargo build -p sapphire-framework --features registry`
Expected: 成功

- [ ] **Step 12: Commit**

```bash
git add Cargo.toml crates/sapphire-framework-registry crates/sapphire-framework/Cargo.toml crates/sapphire-framework/src/lib.rs
git commit -m "feat(registry): user table with the key file's own discipline"
```

---

### Task 2: `Device` / `Devices`

**Files:**
- Create: `crates/sapphire-framework-registry/src/devices.rs`
- Modify: `crates/sapphire-framework-registry/src/lib.rs`
- Modify: `crates/sapphire-framework/src/lib.rs`（prelude）
- Test: `crates/sapphire-framework-registry/src/devices.rs`（インライン）

**Interfaces:**
- Consumes: `crate::store::write_atomic`, `crate::error::{Error, Result}`（Task 1）
- Produces:
  - `Device { id: GrainId, name: String, description: Option<String>, user_id: Option<GrainId>, created_at: DateTime<Utc>, retired_at: Option<DateTime<Utc>> }`
  - `Devices::load(path: &Path) -> Result<Devices>`
  - `Devices::entries(&self) -> &[Device]`
  - `Devices::add(&mut self, name: &str, description: Option<String>, user_id: Option<GrainId>) -> Result<Device>`
  - `Devices::get(&self, id: GrainId) -> Option<&Device>`
  - `Devices::resolve(&self, selector: &str) -> Result<&Device>`
  - `Devices::retire(&mut self, selector: &str) -> Result<Device>`
  - `Devices::purge(&mut self, selector: &str) -> Result<Device>`

- [ ] **Step 1: モジュールを宣言する**

`crates/sapphire-framework-registry/src/lib.rs`:

```rust
mod devices;
mod error;
mod store;
mod users;

pub use devices::{Device, Devices};
pub use error::{Error, Result};
pub use users::{User, Users};
```

- [ ] **Step 2: 失敗するテストを書く**

`crates/sapphire-framework-registry/src/devices.rs` に:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("devices.toml");
        (dir, path)
    }

    #[test]
    fn add_then_reload_round_trips() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let user = GrainId::random();
        let added = devices
            .add("pendant", Some("首から下げるやつ".into()), Some(user))
            .unwrap();

        let reloaded = Devices::load(&path).unwrap();

        assert_eq!(reloaded.entries(), &[added]);
        assert_eq!(reloaded.entries()[0].user_id, Some(user));
    }

    #[test]
    fn a_missing_file_loads_as_empty_and_is_not_created() {
        let (_d, path) = tmp();
        let devices = Devices::load(&path).unwrap();
        assert!(devices.entries().is_empty());
        assert!(!path.exists(), "load はファイルを作らない");
    }

    #[test]
    fn load_fills_in_a_hand_written_entry_and_writes_it_back() {
        let (_d, path) = tmp();
        std::fs::write(&path, "[[device]]\nname = \"pendant\"\n").unwrap();

        let devices = Devices::load(&path).unwrap();

        assert_eq!(devices.entries().len(), 1);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("id = "), "{text}");
        assert!(text.contains("created_at = "), "{text}");
    }

    #[test]
    fn load_rejects_two_devices_sharing_an_id() {
        let (_d, path) = tmp();
        std::fs::write(
            &path,
            "[[device]]\nid = \"a3f9k2p\"\nname = \"a\"\n\n\
             [[device]]\nid = \"a3f9k2p\"\nname = \"b\"\n",
        )
        .unwrap();

        let err = Devices::load(&path).unwrap_err();

        assert!(err.to_string().contains("a3f9k2p"), "{err}");
    }

    #[test]
    fn load_rejects_two_devices_sharing_a_name() {
        let (_d, path) = tmp();
        std::fs::write(
            &path,
            "[[device]]\nname = \"dup\"\n\n[[device]]\nname = \"dup\"\n",
        )
        .unwrap();

        let err = Devices::load(&path).unwrap_err();

        assert!(err.to_string().contains("dup"), "{err}");
    }

    #[test]
    fn add_rejects_a_duplicate_name() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("pendant", None, None).unwrap();

        let err = devices.add("pendant", None, None).unwrap_err();

        assert!(err.to_string().contains("pendant"), "{err}");
    }

    #[test]
    fn resolve_finds_by_id_and_by_name() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let added = devices.add("pendant", None, None).unwrap();

        assert_eq!(devices.resolve("pendant").unwrap(), &added);
        assert_eq!(devices.resolve(&added.id.to_string()).unwrap(), &added);
    }

    #[test]
    fn resolve_errors_on_no_match() {
        let (_d, path) = tmp();
        let devices = Devices::load(&path).unwrap();
        assert!(devices.resolve("nothing").is_err());
    }

    #[test]
    fn retire_keeps_the_entry_resolvable() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let added = devices.add("gone", None, None).unwrap();

        let retired = devices.retire("gone").unwrap();

        assert!(retired.retired_at.is_some());
        // device_id はジャーナルのフロントマターに焼かれるので、引退しても
        // 逆引きできなければならない。
        assert!(devices.get(added.id).is_some());
        let reloaded = Devices::load(&path).unwrap();
        assert!(reloaded.entries()[0].retired_at.is_some());
    }

    #[test]
    fn purge_removes_the_entry() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("gone", None, None).unwrap();

        devices.purge("gone").unwrap();

        assert!(Devices::load(&path).unwrap().entries().is_empty());
    }

    #[test]
    fn the_header_documents_every_field() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("pendant", None, None).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();

        for field in [
            "id",
            "name",
            "description",
            "user_id",
            "created_at",
            "retired_at",
        ] {
            assert!(
                text.contains(&format!("# {field}")),
                "ヘッダが {field} を説明していない: {text}"
            );
        }
    }
}
```

- [ ] **Step 3: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-registry devices`
Expected: コンパイルエラー（`Devices` が存在しない）

- [ ] **Step 4: `devices.rs` の本体を書く**

テストの上に:

```rust
//! デバイス台帳（`.{app_name}/devices.toml`）。
//!
//! `id` は**コンテンツに永続化される** — ジャーナルのフロントマターの
//! `updated_by` がこれを指し、表示時に `user_id` 経由で人間の名前へ逆引き
//! される。だから台帳からの削除は既定でトゥームストーン（`retired_at`）で、
//! 物理削除は `purge` を明示したときだけ。
//!
//! ID はこのアプリの中だけで意味を持つ。同じ物理デバイスが別のアプリの台帳に
//! 別の ID で載っていてよい。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::store::write_atomic;

/// 保存時に毎回書き出す書式説明。
const HEADER: &str = "\
# sapphire devices.
#
# One `[[device]]` table per client device. Hand-editing is fine: a table
# with just a `name` is a valid entry — the remaining fields are filled in
# and written back the next time this file is loaded.
#
# id          optional. A grain-id. Filled in on load when blank. This is
#             the id that gets written into content (a journal entry's
#             `updated_by`, say), so it must stay stable. Ids must be
#             unique within this file.
# name        required. Unique within this file. Accepted in place of the
#             id anywhere a command asks for a device. A name that happens
#             to parse as a grain-id is never matched as a name, only as
#             an id.
# description optional. A note for you.
# user_id     optional. A grain-id from users.toml — whose device this is.
# created_at  optional. RFC 3339. Filled in on load when blank.
# retired_at  optional. RFC 3339. Set when the device is retired. The entry
#             stays so historical references still resolve; only an explicit
#             purge removes it. Revoking access is a separate job, done in
#             the server's own key file.
#
# This file is rewritten in full on every change; comments you add are lost.
";

/// 一台のデバイス。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    pub id: GrainId,
    pub name: String,
    pub description: Option<String>,
    pub user_id: Option<GrainId>,
    pub created_at: DateTime<Utc>,
    pub retired_at: Option<DateTime<Utc>>,
}

impl Device {
    /// 引退済みか。認証の可否には使わない（それは鍵ファイルの仕事）。
    pub fn is_retired(&self) -> bool {
        self.retired_at.is_some()
    }
}

/// ファイル上の表現。手書きを許すため `id` / `created_at` は省略可。
#[derive(Debug, Serialize, Deserialize)]
struct RawDevice {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<GrainId>,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_id: Option<GrainId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RawFile {
    #[serde(default)]
    device: Vec<RawDevice>,
}

/// デバイス台帳ファイルとその中身。
pub struct Devices {
    path: PathBuf,
    entries: Vec<Device>,
}

impl Devices {
    /// 読み込み、欠けた `id` / `created_at` を補完する。補完があれば書き戻す。
    /// ファイルが無い場合は空の台帳を返す（作成はしない）。
    pub fn load(path: &Path) -> Result<Self> {
        let raw: RawFile = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::File(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RawFile::default(),
            Err(e) => return Err(Error::Io(e)),
        };

        let mut seen_ids: HashSet<GrainId> = HashSet::new();
        let mut seen_names: HashSet<&str> = HashSet::new();
        for d in &raw.device {
            if let Some(id) = d.id
                && !seen_ids.insert(id)
            {
                return Err(Error::File(format!(
                    "{}: two devices share the id {id}",
                    path.display()
                )));
            }
            if !seen_names.insert(d.name.as_str()) {
                return Err(Error::File(format!(
                    "{}: two devices share the name {:?}",
                    path.display(),
                    d.name
                )));
            }
        }

        let mut filled = false;
        let now = Utc::now();
        let mut entries: Vec<Device> = Vec::with_capacity(raw.device.len());
        for d in raw.device {
            if d.id.is_none() || d.created_at.is_none() {
                filled = true;
            }
            entries.push(Device {
                id: d.id.unwrap_or_else(GrainId::random),
                name: d.name,
                description: d.description,
                user_id: d.user_id,
                created_at: d.created_at.unwrap_or(now),
                retired_at: d.retired_at,
            });
        }

        let store = Self {
            path: path.to_path_buf(),
            entries,
        };
        if filled {
            store.save()?;
        }
        Ok(store)
    }

    pub fn entries(&self) -> &[Device] {
        &self.entries
    }

    /// 新しいデバイスを追加して保存する。`name` の重複は拒否する。
    pub fn add(
        &mut self,
        name: &str,
        description: Option<String>,
        user_id: Option<GrainId>,
    ) -> Result<Device> {
        if self.entries.iter().any(|d| d.name == name) {
            return Err(Error::File(format!(
                "a device named {name:?} already exists"
            )));
        }
        let entry = Device {
            id: GrainId::random(),
            name: name.to_owned(),
            description,
            user_id,
            created_at: Utc::now(),
            retired_at: None,
        };
        let mut candidate = self.entries.clone();
        candidate.push(entry.clone());
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(entry)
    }

    pub fn get(&self, id: GrainId) -> Option<&Device> {
        self.entries.iter().find(|d| d.id == id)
    }

    /// `users.rs` の `index_of` と同じ規則 — grain-id として読めるなら id、
    /// 読めないなら name。
    fn index_of(&self, selector: &str) -> Result<usize> {
        let found = match selector.parse::<GrainId>() {
            Ok(id) => self.entries.iter().position(|d| d.id == id),
            Err(_) => self.entries.iter().position(|d| d.name == selector),
        };
        found.ok_or_else(|| Error::File(format!("no device matches {selector:?}")))
    }

    pub fn resolve(&self, selector: &str) -> Result<&Device> {
        Ok(&self.entries[self.index_of(selector)?])
    }

    /// 引退させる。エントリは残るので、コンテンツに焼かれた `device_id` は
    /// 解決し続ける。既に引退済みなら `retired_at` は上書きしない。
    pub fn retire(&mut self, selector: &str) -> Result<Device> {
        let i = self.index_of(selector)?;
        let mut candidate = self.entries.clone();
        if candidate[i].retired_at.is_none() {
            candidate[i].retired_at = Some(Utc::now());
        }
        let retired = candidate[i].clone();
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(retired)
    }

    /// 本当に削除する。過去の `updated_by` は解決できなくなる。
    pub fn purge(&mut self, selector: &str) -> Result<Device> {
        let i = self.index_of(selector)?;
        let mut candidate = self.entries.clone();
        let removed = candidate.remove(i);
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(removed)
    }

    fn save(&self) -> Result<()> {
        self.save_entries(&self.entries)
    }

    fn save_entries(&self, entries: &[Device]) -> Result<()> {
        let raw = RawFile {
            device: entries
                .iter()
                .map(|d| RawDevice {
                    id: Some(d.id),
                    name: d.name.clone(),
                    description: d.description.clone(),
                    user_id: d.user_id,
                    created_at: Some(d.created_at),
                    retired_at: d.retired_at,
                })
                .collect(),
        };
        let body = toml::to_string_pretty(&raw)
            .map_err(|e| Error::File(format!("serializing devices: {e}")))?;
        write_atomic(&self.path, HEADER, &body)
    }
}
```

- [ ] **Step 5: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-registry`
Expected: PASS（22 テスト）

- [ ] **Step 6: prelude を差し替える**

`crates/sapphire-framework/src/lib.rs`:

```rust
    #[cfg(feature = "registry")]
    pub use crate::registry::{Device, Devices, User, Users};
```

- [ ] **Step 7: ビルドを確認する**

Run: `cargo build -p sapphire-framework --features registry`
Expected: 成功

- [ ] **Step 8: Commit**

```bash
git add crates/sapphire-framework-registry/src crates/sapphire-framework/src/lib.rs
git commit -m "feat(registry): device table, whose ids get written into content"
```

---

### Task 3: `KeyEntry.device_id`

**Files:**
- Modify: `crates/sapphire-framework-remote-server/Cargo.toml`
- Modify: `crates/sapphire-framework-remote-server/src/keys.rs`（`HEADER`, `KeyEntry`, `RawKey`, `load`, `generate`, `save_entries`）
- Modify: `crates/sapphire-framework-remote-server/tests/rpc.rs`（`generate` 呼び出し）
- Test: `crates/sapphire-framework-remote-server/src/keys.rs`（既存の `#[cfg(test)] mod tests`）

**Interfaces:**
- Consumes: `grain_id::GrainId`
- Produces:
  - `KeyEntry.device_id: Option<grain_id::GrainId>`（公開フィールド）
  - `KeyStore::generate(&mut self, prefix: &str, id: Option<Uuid>, device_id: Option<GrainId>, label: Option<String>, expires_at: Option<DateTime<Utc>>) -> Result<KeyEntry>` — **`device_id` が 3 番目に挿入され 5 引数になる（破壊的変更）**
  - `KeyStore::rotate` はシグネチャ不変。`device_id` を保つ。

- [ ] **Step 1: 失敗するテストを書く**

`crates/sapphire-framework-remote-server/src/keys.rs` の `mod tests` の末尾に:

```rust
    #[test]
    fn generate_records_the_device_id_and_it_survives_a_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        let mut store = KeyStore::load(&path).unwrap();
        let device = grain_id::GrainId::random();

        let entry = store
            .generate("sat", None, Some(device), Some("pendant".into()), None)
            .unwrap();

        assert_eq!(entry.device_id, Some(device));
        let reloaded = KeyStore::load(&path).unwrap();
        assert_eq!(reloaded.entries()[0].device_id, Some(device));
    }

    #[test]
    fn rotate_keeps_the_device_id() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        let mut store = KeyStore::load(&path).unwrap();
        let device = grain_id::GrainId::random();
        store
            .generate("sat", None, Some(device), Some("pendant".into()), None)
            .unwrap();

        let rotated = store.rotate("sat", "pendant", None).unwrap();

        // rotate の約束は「id・label・created_at を保ってトークンだけ差し替える」。
        // device_id は「誰の鍵か」を担う側なので、当然保たれなければならない。
        assert_eq!(rotated.device_id, Some(device));
        let reloaded = KeyStore::load(&path).unwrap();
        assert_eq!(reloaded.entries()[0].device_id, Some(device));
    }

    #[test]
    fn a_key_file_without_device_id_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        // device_id を知らない時代に書かれた鍵ファイル。
        std::fs::write(&path, "[[key]]\ntoken = \"sjt_old\"\nlabel = \"laptop\"\n").unwrap();

        let store = KeyStore::load(&path).unwrap();

        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].device_id, None);
        assert!(store.authenticate("sjt_old").is_some());
    }

    #[test]
    fn the_header_documents_device_id() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        let mut store = KeyStore::load(&path).unwrap();
        store.generate("sat", None, None, None, None).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();

        assert!(text.contains("# device_id"), "{text}");
    }
```

- [ ] **Step 2: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-remote-server`
Expected: コンパイルエラー（`generate` が 4 引数、`device_id` フィールドが無い）

- [ ] **Step 3: `grain-id` を依存に足す**

`crates/sapphire-framework-remote-server/Cargo.toml` の `[dependencies]` に:

```toml
grain-id.workspace = true
```

- [ ] **Step 4: `HEADER` を更新する**

`keys.rs` の `HEADER` の、`id` の説明ブロックの直後（`label` の説明の前）に挿入する:

```
# device_id   optional. A grain-id naming an entry in the application's
#             devices.toml. This is how a key says whose device it is. The
#             device table is per-workspace while this file is per-host, so
#             one physical device talking to two servers has two keys in two
#             files: the link runs key -> device and never the other way.
```

- [ ] **Step 5: 型とフィールドを足す**

`KeyEntry` の `id` の次に:

```rust
    /// アプリの `devices.toml` のエントリを指す。鍵がデバイスを指す向きで、
    /// 逆ではない — 鍵ファイルはホストごと、デバイス台帳はワークスペースごとに
    /// あるので、1 台のデバイスが複数ホストに別々の鍵を持ちうる。
    pub device_id: Option<grain_id::GrainId>,
```

`RawKey` の `id` の次に:

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device_id: Option<grain_id::GrainId>,
```

`load` の `entries.push(KeyEntry { ... })` に足す:

```rust
                device_id: k.device_id,
```

`save_entries` の `RawKey { ... }` に足す:

```rust
                    device_id: e.device_id,
```

- [ ] **Step 6: `generate` のシグネチャを変える**

```rust
    pub fn generate(
        &mut self,
        prefix: &str,
        id: Option<Uuid>,
        device_id: Option<grain_id::GrainId>,
        label: Option<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<KeyEntry> {
```

本体の `let entry = KeyEntry { ... }` に `device_id,` を足す。

`rotate` は `token` / `expires_at` / `rotated_at` しか触らないので、`device_id` は自動的に保たれる。**変更不要**。

- [ ] **Step 7: 既存の `generate` 呼び出しをすべて直す**

Run: `cargo test -p sapphire-framework-remote-server 2>&1 | grep -c "arguments were supplied"`

出てきた箇所（`keys.rs` の既存テストと `tests/rpc.rs`）すべてで 3 番目の引数として `None` を挿入する。例:

```rust
// 変更前
store.generate("sjt", None, Some("laptop".into()), None).unwrap();
// 変更後
store.generate("sjt", None, None, Some("laptop".into()), None).unwrap();
```

- [ ] **Step 8: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-remote-server`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add crates/sapphire-framework-remote-server
git commit -m "feat(remote-server)!: let a key name the device it belongs to"
```

---

### Task 4: `Workspace::devices_path` / `users_path`

**Files:**
- Modify: `crates/sapphire-framework-workspace/src/workspace.rs`（`config_path` の直後）
- Test: `crates/sapphire-framework-workspace/src/workspace.rs`（末尾）

**Interfaces:**
- Consumes: `Workspace::marker_dir`, `Workspace::config_path`, `AppContext::new`（すべて既存）
- Produces:
  - `Workspace::devices_path(&self) -> PathBuf` — `{root}/.{app_name}/devices.toml`
  - `Workspace::users_path(&self) -> PathBuf` — `{root}/.{app_name}/users.toml`

- [ ] **Step 1: 失敗するテストを書く**

`crates/sapphire-framework-workspace/src/workspace.rs` の末尾に:

```rust
#[cfg(test)]
mod registry_path_tests {
    use super::*;

    #[test]
    fn devices_and_users_sit_next_to_the_workspace_config() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".sapphire-agent")).unwrap();
        let ctx: &'static AppContext = Box::leak(Box::new(AppContext::new("sapphire-agent")));
        let ws = Workspace::from_root(ctx, tmp.path()).unwrap();

        // 台帳はワークスペース設定と同じマーカーディレクトリに置く。
        // ファイル名の規約を 1 箇所に閉じ込めるのがこのメソッドの存在理由。
        assert_eq!(ws.devices_path().parent(), ws.config_path().parent());
        assert_eq!(ws.users_path().parent(), ws.config_path().parent());
        assert_eq!(ws.devices_path().file_name().unwrap(), "devices.toml");
        assert_eq!(ws.users_path().file_name().unwrap(), "users.toml");
    }
}
```

- [ ] **Step 2: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-workspace registry_path`
Expected: コンパイルエラー（`devices_path` が無い）

- [ ] **Step 3: メソッドを足す**

`pub fn config_path(&self) -> PathBuf { ... }` の直後に:

```rust
    /// Path to `{root}/.{app_name}/devices.toml`.
    ///
    /// 台帳そのものの読み書きは `sapphire-framework-registry` が持つ。ここが
    /// 決めるのはファイル名の規約だけで、逆方向の依存は張らない — registry は
    /// `&Path` を受け取るだけなので、このクレートを引かずに使える。
    pub fn devices_path(&self) -> PathBuf {
        self.marker_dir().join("devices.toml")
    }

    /// Path to `{root}/.{app_name}/users.toml`.
    pub fn users_path(&self) -> PathBuf {
        self.marker_dir().join("users.toml")
    }
```

- [ ] **Step 4: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-workspace registry_path`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/sapphire-framework-workspace/src/workspace.rs
git commit -m "feat(workspace): name the registry files next to the workspace config"
```

---

### Task 5: README と全体検証

**Files:**
- Create: `crates/sapphire-framework-registry/README.md`

**Interfaces:**
- Consumes: Task 1–4 のすべて
- Produces: なし

- [ ] **Step 1: README を書く**

`crates/sapphire-framework-registry/README.md`:

~~~markdown
# sapphire-framework-registry

アプリごとのデバイス／ユーザー台帳。`.{app_name}/devices.toml` と
`.{app_name}/users.toml` を読み書きする。

```rust
use sapphire_framework::registry::Devices;

let mut devices = Devices::load(&workspace.devices_path())?;
let pendant = devices.add("pendant", Some("首から下げるやつ".into()), None)?;
println!("{}", pendant.id); // 例: "a3f9k2p"
```

## ID はアプリの中で閉じる

`Device` / `User` の ID はそのアプリの台帳の中だけで意味を持ち、アプリ間で
共有しない。sapphire-journal / sapphire-ledger / sapphire-agent は互いに MCP
などの API 越しに **1 つのクライアントデバイス**として映るので、揃える必要が
無い。

`device.id` は**コンテンツに永続化される**（ジャーナルのエントリの
`updated_by` など）。だから削除は既定でトゥームストーン（`retired_at`）で、
物理削除は `purge` を明示したときだけ。アクセスの停止は台帳ではなく、
サーバの鍵ファイル（`KeyStore::revoke`）の仕事。

## 鍵との関係

`KeyEntry.device_id` が台帳のエントリを指す。向きが逆でないのは、鍵ファイルが
ホストごと・台帳がワークスペースごとに存在するため — 1 台の物理デバイスが
2 台のサーバに喋るなら、鍵は 2 本・別々のファイルに入る。
~~~

- [ ] **Step 2: ワークスペース全体のテストを走らせる**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 3: clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 警告なし

- [ ] **Step 4: fmt**

Run: `cargo fmt --all -- --check`
Expected: 差分なし（差分が出たら `cargo fmt --all` で直してから再実行）

- [ ] **Step 5: `--features native` でビルドできることを確認する**

`native` に `registry` を足したので、既存の利用者がこれで壊れないことを見る。

Run: `cargo build -p sapphire-framework --features native`
Expected: 成功

- [ ] **Step 6: Commit**

```bash
git add crates/sapphire-framework-registry/README.md
git commit -m "docs(registry): README"
```

---

## 完了条件

- `cargo test --workspace` が通る
- `cargo clippy --workspace --all-targets -- -D warnings` が通る
- `sapphire_framework::registry::{Device, Devices, User, Users}` が `registry` フィーチャで使える
- `KeyEntry.device_id` が生成・保存・再読み込み・`rotate` を通して保たれる
- `device_id` の無い既存の鍵ファイルがそのまま読める
- `Workspace::devices_path()` / `users_path()` がマーカーディレクトリの下を指す

## 下流への申し送り

`KeyStore::generate` が **5 引数になる**（`device_id` が 3 番目）。これは
`sapphire-journal-server` と `sapphire-agent` の両方をコンパイルエラーにする。
それぞれのリポジトリの計画でこの追従を扱う。
