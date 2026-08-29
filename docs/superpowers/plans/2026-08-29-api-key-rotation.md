# API キーの再発行と UUID 鍵 id — 実装プラン

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `KeyStore` に「id・label を保ったまま token だけ差し替える」再発行を足し、鍵 id を UUID に戻して呼び出し側から指定できるようにする。

**Architecture:** 変更は `sapphire-framework-remote-server` の `keys.rs` にほぼ閉じる。`KeyEntry::id` の型が `Uuid` に戻るので `auth.rs` の `Authenticated::key_id`、クレートルートの再エクスポート、`sapphire-framework` の prelude が追従する。id が UUID になると label と名前空間が重ならなくなるため、`revoke` の「id 一致が label 一致より優先」という規則を「UUID として parse できれば id、できなければ label」に置き換え、`revoke` と新設の `rotate` で共有する。

**Tech Stack:** Rust (edition 2024), `uuid` (v4), `chrono`, `toml`, `serde`, `base64`, `getrandom`, `tempfile`（dev）

**Spec:** [`docs/superpowers/specs/2026-08-29-api-key-rotation-design.md`](../specs/2026-08-29-api-key-rotation-design.md)

## Global Constraints

- 作業は `sapphire-framework` リポジトリ内の feature ブランチで行う（superproject のポインタは触らない）。
- `keys.rs` の既存の様式に合わせる: **テスト名は英語の文**（`load_rejects_two_keys_that_share_an_id` のように）、**「なぜそうしたか」のコメントは日本語**。「何をしているか」だけのコメントは書かない。
- テストは `tempfile::tempdir()` + `KeyStore::load` で実ファイルを使う。`KeyStore` をモックしない。
- 保存を伴う変更は必ず「複製を作る → `save_entries(&candidate)` → 成功したら `self.entries = candidate`」の順。保存に失敗したらメモリ上の状態を変えてはならない。
- 各タスクの最後に `cargo fmt --all` と `cargo clippy --workspace --all-targets -- -D warnings` を通してからコミットする。
- コミットメッセージ末尾に `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` を付ける。
- 型を変える 2 つの公開項目 `KeyEntry::id` と `Authenticated::key_id` は BREAKING CHANGE として Task 1 のコミットに書く。

---

## File Structure

| ファイル | 役割 | 変更 |
| --- | --- | --- |
| `crates/sapphire-framework-remote-server/src/keys.rs` | 鍵ファイルの読み書き・生成・失効・再発行。本プランの主戦場 | Task 1〜3 で修正 |
| `crates/sapphire-framework-remote-server/src/auth.rs` | `Authenticated::key_id` の型 | Task 1 |
| `crates/sapphire-framework-remote-server/src/lib.rs` | クレートルートの再エクスポート | Task 1 |
| `crates/sapphire-framework/src/lib.rs` | prelude の再エクスポート | Task 1 |
| `crates/sapphire-framework-remote-server/Cargo.toml` | `grain-id` 依存の削除 | Task 1 |
| `Cargo.toml`（ワークスペース） | `grain-id` のワークスペース依存の削除 | Task 1 |

`keys.rs` は 632 行で、単位としてはまだ手に負える大きさ。分割はしない。

---

### Task 1: 鍵 id を `Uuid` に戻し、セレクタ解決を一本化する

id の型変更とセレクタ規則は分けられない。「id 一致が label 一致より優先」という規則は grain-id が `desktop` や `keyfile` を正当な 7 文字 id として読めることだけを理由に存在しており、UUID にした時点で規則の前提が消えるため。

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/keys.rs`
- Modify: `crates/sapphire-framework-remote-server/src/auth.rs:16,26`
- Modify: `crates/sapphire-framework-remote-server/src/lib.rs:45-51`
- Modify: `crates/sapphire-framework/src/lib.rs:88`
- Modify: `crates/sapphire-framework-remote-server/Cargo.toml`
- Modify: `Cargo.toml`（ワークスペースルート）
- Test: `crates/sapphire-framework-remote-server/src/keys.rs` の `mod tests`

**Interfaces:**
- Produces:
  - `pub struct KeyEntry { pub token: String, pub id: uuid::Uuid, pub label: Option<String>, pub created_at: DateTime<Utc>, pub expires_at: Option<DateTime<Utc>> }`
  - `pub struct Authenticated { pub key_id: uuid::Uuid, pub label: Option<String> }`
  - `fn KeyStore::resolve(&self, selector: &str) -> Result<usize>`（private、Task 3 が使う）
  - `pub use uuid::Uuid;`（`sapphire_framework_remote_server` のルートと `sapphire_framework::prelude`）

- [ ] **Step 1: 失敗するテストを書く**

`keys.rs` の `mod tests` から次の 3 つを**削除**する（grain-id 固有で、コンパイルが通らなくなるもの）:
- `fn taken(ids: &[&str]) -> HashSet<GrainId>` ヘルパー
- `fresh_id_skips_an_id_that_is_already_taken`
- `fresh_id_gives_up_instead_of_looping_forever`

`revoke_matches_an_id_before_a_label_that_looks_like_one` を次で**置き換える**:

```rust
    #[test]
    fn a_uuid_selector_matches_an_id_and_never_a_label() {
        // 旧 grain-id では `desktop` のような語も 7 文字の正当な id として読めた
        // ため、「id 一致が label 一致より優先」という規則が要った。UUID では
        // 名前空間が重ならないので、parse できるかどうかだけで行き先が決まる。
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let target = store.generate("sjt", Some("by id".into()), None).unwrap();
        // 別の鍵の id をそのまま label に持つ鍵。
        let decoy = store
            .generate("sjt", Some(target.id.to_string()), None)
            .unwrap();

        let removed = store.revoke(&target.id.to_string()).unwrap();

        assert_eq!(removed.id, target.id, "UUID 文字列は id にしか当たらない");
        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].id, decoy.id);
    }
```

`load_fills_in_a_hand_written_entry_and_writes_it_back` の id に関する 2 つの assert を差し替える:

```rust
        let id = store.entries()[0].id;
        assert_ne!(id, Uuid::nil(), "補完された id は nil であってはならない");
        assert_eq!(
            id.to_string().parse::<Uuid>().unwrap(),
            id,
            "表示形から読み戻せる"
        );
```

`load_rejects_two_keys_that_share_an_id` の id 文字列を UUID に差し替える:

```rust
    #[test]
    fn load_rejects_two_keys_that_share_an_id() {
        // 手で書くことはまず無いが、エントリごとコピーして複製する事故はある。
        // 重複したまま読むと revoke がどちらの鍵か決められない。
        let tmp = tempfile::tempdir().unwrap();
        let p = path(&tmp);
        std::fs::write(
            &p,
            "[[key]]\ntoken = \"sjt_a\"\nid = \"6f1c4a9e-5d2b-4c8f-9a30-1e7b5c8d2f41\"\n\n\
             [[key]]\ntoken = \"sjt_b\"\nid = \"6f1c4a9e-5d2b-4c8f-9a30-1e7b5c8d2f41\"\n",
        )
        .unwrap();

        let err = match KeyStore::load(&p) {
            Ok(_) => panic!("重複した id を受け入れてはならない"),
            Err(e) => e.to_string(),
        };

        assert!(
            err.contains("6f1c4a9e-5d2b-4c8f-9a30-1e7b5c8d2f41"),
            "どの id が重複したか示す: {err}"
        );
    }
```

`saving_regenerates_the_header_comment` に 1 行足す:

```rust
        assert!(
            !text.contains("grain-id"),
            "id はもう grain-id ではない: {text}"
        );
```

- [ ] **Step 2: テストが失敗する（コンパイルが通らない）ことを確認**

Run: `cargo test -p sapphire-framework-remote-server --lib keys`
Expected: FAIL。`Uuid` が `keys.rs` のテストスコープに無い、`GrainId` の型不一致、`fresh_id` が未定義、などのコンパイルエラー。

- [ ] **Step 3: `keys.rs` を実装する**

`use grain_id::GrainId;` を `use uuid::Uuid;` に差し替える。

`HEADER` の `id` の行を書き換える:

```rust
# id          optional. A UUID. Filled in on load when blank. Ties a key to a
#             user or device, so it survives a label change. Ids must be
#             unique within this file.
```

`KeyEntry::id` を `pub id: Uuid`、`RawKey::id` を `Option<Uuid>` に。

`load` の重複検出と補完を差し替える（`fresh_id` を呼ばない形にする）:

```rust
        // 重複したまま読み込むと revoke がどちらの鍵か決められない。UUID を手で
        // 書くことはまず無いが、エントリごとコピーして複製する事故はある。
        let mut seen: HashSet<Uuid> = HashSet::new();
        for k in &raw.key {
            if let Some(id) = k.id
                && !seen.insert(id)
            {
                return Err(Error::KeyFile(format!(
                    "{}: two keys share the id {id}",
                    path.display()
                )));
            }
        }

        let mut filled = false;
        let now = Utc::now();
        let mut entries: Vec<KeyEntry> = Vec::with_capacity(raw.key.len());
        for k in raw.key {
            if k.id.is_none() || k.created_at.is_none() {
                filled = true;
            }
            entries.push(KeyEntry {
                token: k.token,
                id: k.id.unwrap_or_else(Uuid::new_v4),
                label: k.label,
                created_at: k.created_at.unwrap_or(now),
                expires_at: k.expires_at,
            });
        }
```

`fn fresh_id` とその doc コメントを**丸ごと削除**する。122 ビットは確率に任せてよく、引き当て直す理由が無くなったため。

`generate` の id 決定を差し替える（`taken` の収集も削除する）:

```rust
        let entry = KeyEntry {
            token: format!("{prefix}_{random}"),
            id: Uuid::new_v4(),
            label,
            created_at: Utc::now(),
            expires_at,
        };
```

`revoke` の直前に `resolve` を新設する:

```rust
    /// `selector` を 1 件のエントリの位置に解決する。
    ///
    /// UUID として読めるなら id、読めないなら label を見る。両者の名前空間は
    /// 重ならないので、grain-id のころに要った「id 一致が label 一致より優先」と
    /// いう規則は要らない。
    fn resolve(&self, selector: &str) -> Result<usize> {
        let matches: Vec<usize> = match selector.parse::<Uuid>() {
            Ok(id) => self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.id == id)
                .map(|(i, _)| i)
                .collect(),
            Err(_) => self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.label.as_deref() == Some(selector))
                .map(|(i, _)| i)
                .collect(),
        };

        match matches.as_slice() {
            [] => Err(Error::KeyFile(format!("no key matches {selector:?}"))),
            [i] => Ok(*i),
            // id は `load` と `generate` が一意性を保証するので、複数一致は
            // label 側でしか起こらない。
            many => Err(Error::KeyFile(format!(
                "{} keys share the label {selector:?}; pass the id instead",
                many.len()
            ))),
        }
    }
```

`revoke` を `resolve` を使う形に縮める（doc コメントの「**`id` 一致が label 一致より先**」の段落は削除する）:

```rust
    /// `selector`（`id` またはラベル）に一致する鍵を削除する。ラベルが複数一致
    /// する場合はエラーにして `id` を要求する。
    pub fn revoke(&mut self, selector: &str) -> Result<KeyEntry> {
        let i = self.resolve(selector)?;
        let mut candidate = self.entries.clone();
        let removed = candidate.remove(i);
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(removed)
    }
```

テストモジュール冒頭の `use std::collections::HashSet;` は `taken` ヘルパーと一緒に不要になるので削除する。

- [ ] **Step 4: 追従先を直す**

`auth.rs`: `use grain_id::GrainId;` → `use uuid::Uuid;`、`pub key_id: GrainId,` → `pub key_id: Uuid,`。

`crates/sapphire-framework-remote-server/src/lib.rs`: `pub use grain_id::GrainId;` を削除し、コメントごと次に差し替える（並びはアルファベット順を保つため `pub use error::...` の後）:

```rust
// `Authenticated::key_id` と `KeyEntry::id` の型。アプリが uuid を自前で
// 依存に足さなくても名指しできるように出しておく。
pub use uuid::Uuid;
```

`crates/sapphire-framework/src/lib.rs:88`: prelude の `GrainId` を `Uuid` に差し替える（並びはアルファベット順なので末尾へ移動する）:

```rust
    pub use crate::remote_server::{
        Authenticated, KeyStore, ServerState, Uuid, WsStore, WsStoreConfig, protect, router,
        serve,
    };
```

`crates/sapphire-framework-remote-server/Cargo.toml`: `[dependencies]` と `[dev-dependencies]` の両方から `grain-id.workspace = true` を削除する（`uuid.workspace = true` は両方に既にある）。

ワークスペースルートの `Cargo.toml`: `grain-id = { version = "0.15", features = ["serde"] }` の行を削除する。このワークスペースで grain-id を使っていたのは remote-server だけ。

- [ ] **Step 5: テストが通ることを確認**

```
cargo test -p sapphire-framework-remote-server
cargo check -p sapphire-framework --all-features
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

Expected: すべて PASS。`grain_id` への参照がワークスペースに 1 つも残っていないことを `grep -rn "grain.id" --include=*.rs --include=*.toml crates/ Cargo.toml` で確認する（出力が空）。

- [ ] **Step 6: コミット**

```bash
git add crates/sapphire-framework-remote-server/src/keys.rs \
        crates/sapphire-framework-remote-server/src/auth.rs \
        crates/sapphire-framework-remote-server/src/lib.rs \
        crates/sapphire-framework-remote-server/Cargo.toml \
        crates/sapphire-framework/src/lib.rs Cargo.toml
git commit -F- <<'EOF'
feat(remote-server)!: identify API keys by UUID again

ba6b12c chose grain-id because the id's only readers were humans: `revoke
<id>` and a planned `keys = [...]` reference from the main config. The
second reader is gone — an application mints the id, writes its own device
row, and hands the id to `generate`, so nobody types one into a config
file. What is left is a rare fallback that copy-pastes from the key file,
which puts the id back on the UUID side of the house rule.

The namespaces separate for free. `desktop` and `keyfile` are both valid
7-character grain-ids, so `revoke` needed a documented precedence rule and
a decoy test to pin it down. A UUID cannot be mistaken for a label, so
`resolve` dispatches on whether the selector parses and both `revoke` and
the rotation to come share it.

`fresh_id` goes with it: redrawing bought certainty over 35 bits, and 122
bits do not need it.

BREAKING CHANGE: `KeyEntry::id` and `Authenticated::key_id` are
`uuid::Uuid` again instead of `grain_id::GrainId`, and `GrainId` is no
longer re-exported from the crate root or the `sapphire-framework`
prelude. An existing `keys.toml` whose entries carry grain-id ids fails to
load; delete the `id` lines and let them be filled in again, or drop the
file.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 2: `generate` が呼び出し側の id を受け取る

アプリが先に UUID をミントして device 行を書き、その id で鍵を作れるようにする。順序をこう決めたのは、途中で失敗したときに残るのが「鍵の無い device 行」（繋がらないだけで無害）であって「動くオーファン鍵」ではないようにするため。

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/keys.rs`
- Test: `crates/sapphire-framework-remote-server/src/keys.rs` の `mod tests`

**Interfaces:**
- Consumes: Task 1 の `KeyEntry::id: Uuid`
- Produces: `pub fn KeyStore::generate(&mut self, prefix: &str, id: Option<Uuid>, label: Option<String>, expires_at: Option<DateTime<Utc>>) -> Result<KeyEntry>`

- [ ] **Step 1: 失敗するテストを書く**

`mod tests` に追加する:

```rust
    #[test]
    fn generate_uses_a_supplied_id() {
        // アプリは device 行を先に書いてから鍵を作る。id を渡せなければ
        // 「鍵を作る → 返った id を device 行に書く」順しか取れず、途中で失敗
        // したときに動くオーファン鍵が残る。
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let want: Uuid = "6f1c4a9e-5d2b-4c8f-9a30-1e7b5c8d2f41".parse().unwrap();

        let entry = store
            .generate("sjt", Some(want), Some("iPhone".into()), None)
            .unwrap();

        assert_eq!(entry.id, want);
        let reloaded = KeyStore::load(&path(&tmp)).unwrap();
        assert_eq!(reloaded.entries()[0].id, want, "ファイルにも入っている");
    }

    #[test]
    fn generate_rejects_an_id_that_is_already_taken() {
        // 呼び出し側は特定の id を要求している。空きを探して別の id を黙って
        // 返すのは要求に応えていない。
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let first = store.generate("sjt", None, Some("keeper".into()), None).unwrap();

        let err = match store.generate("sjt", Some(first.id), None, None) {
            Ok(e) => panic!("使われている id を受け入れてはならない: {}", e.id),
            Err(e) => e.to_string(),
        };

        assert!(err.contains(&first.id.to_string()), "どの id か示す: {err}");
        assert_eq!(store.entries().len(), 1, "失敗した生成が鍵を増やしてはならない");
    }

    #[test]
    fn generate_without_an_id_mints_a_fresh_one() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();

        let a = store.generate("sjt", None, None, None).unwrap();
        let b = store.generate("sjt", None, None, None).unwrap();

        assert_ne!(a.id, b.id);
        assert_ne!(a.id, Uuid::nil());
    }
```

既存テストの `generate` 呼び出しは**すべて第 2 引数に `None` を挿入する**（`store.generate("sjt", Some("laptop".into()), None)` → `store.generate("sjt", None, Some("laptop".into()), None)`）。対象はコンパイラが全部指すが、`keys.rs` の `mod tests` 内のみで、他のクレートやインテグレーションテストに `generate` の呼び出しは無い（それらは `keys.toml` を直接書いている）。

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test -p sapphire-framework-remote-server --lib keys`
Expected: FAIL。引数の個数が合わないコンパイルエラー。

- [ ] **Step 3: 実装する**

`generate` のシグネチャと id 決定部分を差し替える:

```rust
    /// 新しい鍵を生成して追記・保存する。
    ///
    /// `id` を渡すとその値を使う。アプリが device 行を先に書いてから鍵を作れる
    /// ようにするため。既に使われている id を渡した場合は**空きを探さずに**
    /// 失敗する — 呼び出し側は特定の id を要求しているので、別の id を黙って
    /// 返すのは要求に応えていない。
    pub fn generate(
        &mut self,
        prefix: &str,
        id: Option<Uuid>,
        label: Option<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<KeyEntry> {
        let id = match id {
            Some(id) => {
                if self.entries.iter().any(|e| e.id == id) {
                    return Err(Error::KeyFile(format!("a key with the id {id} already exists")));
                }
                id
            }
            None => Uuid::new_v4(),
        };

        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes)
            .map_err(|e| Error::KeyFile(format!("no randomness available: {e}")))?;
        let random = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);

        let entry = KeyEntry {
            token: format!("{prefix}_{random}"),
            id,
            label,
            created_at: Utc::now(),
            expires_at,
        };
        let mut candidate = self.entries.clone();
        candidate.push(entry.clone());
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(entry)
    }
```

- [ ] **Step 4: テストが通ることを確認**

```
cargo test -p sapphire-framework-remote-server
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

Expected: すべて PASS。

- [ ] **Step 5: コミット**

```bash
git add crates/sapphire-framework-remote-server/src/keys.rs
git commit -F- <<'EOF'
feat(remote-server)!: let the caller choose a new key's id

An application that manages devices wants the key id it already minted, so
it can write its own device row first and create the key second. That order
matters: a partial failure then leaves a device row with no key — it simply
cannot connect — rather than a working key no device row claims.

A taken id fails instead of redrawing. The caller asked for one specific
id; quietly returning a different one does not answer the request.

BREAKING CHANGE: `KeyStore::generate` takes `id: Option<Uuid>` as its
second argument. Pass `None` for the previous behaviour.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 3: `KeyStore::rotate`

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/keys.rs`
- Test: `crates/sapphire-framework-remote-server/src/keys.rs` の `mod tests`

**Interfaces:**
- Consumes: Task 1 の `KeyStore::resolve`、Task 2 の `generate` シグネチャ
- Produces:
  - `pub fn KeyStore::rotate(&mut self, prefix: &str, selector: &str, expires_at: Option<DateTime<Utc>>) -> Result<KeyEntry>`
  - `pub rotated_at: Option<DateTime<Utc>>`（`KeyEntry` の新フィールド）

- [ ] **Step 1: 失敗するテストを書く**

`mod tests` に追加する:

```rust
    #[test]
    fn rotate_replaces_the_token_and_keeps_the_identity() {
        // id が別に在る理由がこれ。token を更新しても、その鍵が誰のものかは
        // 変わらない。
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let before = store
            .generate("sjt", None, Some("iPhone".into()), None)
            .unwrap();

        let after = store.rotate("sjt", &before.id.to_string(), None).unwrap();

        assert_eq!(after.id, before.id);
        assert_eq!(after.label.as_deref(), Some("iPhone"));
        assert_eq!(after.created_at, before.created_at, "同一性の誕生日は動かない");
        assert_ne!(after.token, before.token);
        assert!(after.token.starts_with("sjt_"));
        assert!(after.rotated_at.is_some());
        assert_eq!(store.entries().len(), 1, "再発行は鍵を増やさない");
    }

    #[test]
    fn the_old_token_stops_working_the_moment_it_is_rotated() {
        // 猶予期間は持たない。旧トークンが少しでも生き続けるなら、それは
        // 「2 本目の生きた秘密」であり、この脅威モデルでそれを持つ理由が無い。
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let before = store.generate("sjt", None, Some("iPhone".into()), None).unwrap();

        let after = store.rotate("sjt", "iPhone", None).unwrap();

        assert!(store.authenticate(&before.token).is_none(), "旧トークンは即死");
        assert_eq!(store.authenticate(&after.token).unwrap().id, before.id);
    }

    #[test]
    fn rotate_can_revive_an_expired_key() {
        // 期限切れは「消えた鍵」ではなく「止まっている鍵」で、revoke するまで
        // ファイルに残る。再開の手段が rotate なのは自然。
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let past = Utc::now() - Duration::hours(1);
        let dead = store.generate("sjt", None, Some("iPhone".into()), Some(past)).unwrap();
        assert!(store.authenticate(&dead.token).is_none());

        let revived = store
            .rotate("sjt", "iPhone", Some(Utc::now() + Duration::hours(1)))
            .unwrap();

        assert_eq!(store.authenticate(&revived.token).unwrap().id, dead.id);
    }

    #[test]
    fn rotate_normalises_a_hand_written_token() {
        // 手で書いたトークンは `<prefix>_<random>` 形式とは限らないので、旧
        // トークンから接頭辞を取り出すことはできない。だから prefix は引数。
        let tmp = tempfile::tempdir().unwrap();
        let p = path(&tmp);
        std::fs::write(&p, "[[key]]\ntoken = \"plainsecret\"\nlabel = \"iPhone\"\n").unwrap();
        let mut store = KeyStore::load(&p).unwrap();

        let after = store.rotate("sjt", "iPhone", None).unwrap();

        assert!(after.token.starts_with("sjt_"));
        assert_eq!(after.token.len(), "sjt_".len() + 43);
    }

    #[test]
    fn rotate_reports_a_selector_that_matches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        store.generate("sjt", None, Some("iPhone".into()), None).unwrap();

        let err = match store.rotate("sjt", "iPad", None) {
            Ok(e) => panic!("存在しない鍵を再発行してはならない: {}", e.id),
            Err(e) => e.to_string(),
        };

        assert!(err.contains("iPad"), "何に一致しなかったか示す: {err}");
    }

    #[test]
    fn rotate_refuses_an_ambiguous_label() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        store.generate("sjt", None, Some("dup".into()), None).unwrap();
        store.generate("sjt", None, Some("dup".into()), None).unwrap();

        assert!(store.rotate("sjt", "dup", None).is_err(), "id を要求する");
    }

    #[test]
    fn rotate_does_not_mutate_state_when_save_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        store.generate("sjt", None, Some("keeper".into()), None).unwrap();

        let blocker = tmp.path().join("afile");
        std::fs::write(&blocker, "not a directory").unwrap();
        store.path = blocker.join("keys.toml");

        let before = store.entries().to_vec();
        assert!(store.rotate("sjt", "keeper", None).is_err());
        assert_eq!(
            store.entries(),
            before.as_slice(),
            "a failed save must not mutate in-memory state"
        );
    }

    #[test]
    fn rotated_at_survives_a_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let p = path(&tmp);
        let mut store = KeyStore::load(&p).unwrap();
        store.generate("sjt", None, Some("iPhone".into()), None).unwrap();
        let rotated = store.rotate("sjt", "iPhone", None).unwrap();

        let reloaded = KeyStore::load(&p).unwrap();

        assert_eq!(reloaded.entries()[0].rotated_at, rotated.rotated_at);
    }

    #[test]
    fn a_key_that_was_never_rotated_has_no_rotated_at() {
        // `load` は補完しない。一度も再発行していない鍵に再発行時刻は無い。
        let tmp = tempfile::tempdir().unwrap();
        let p = path(&tmp);
        let mut store = KeyStore::load(&p).unwrap();
        store.generate("sjt", None, None, None).unwrap();

        assert!(store.entries()[0].rotated_at.is_none());
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(
            text.matches("rotated_at = ").count(),
            0,
            "空のフィールドを書き出さない: {text}"
        );
    }
```

`saving_regenerates_the_header_comment` に 2 つ足す:

```rust
        assert!(text.contains("rotated_at"), "新しいフィールドを説明する");
        assert!(
            !text.contains("Nothing in the system reads it"),
            "label は revoke / rotate のセレクタとして読まれる: {text}"
        );
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test -p sapphire-framework-remote-server --lib keys`
Expected: FAIL。`rotate` が未定義、`rotated_at` フィールドが無い、というコンパイルエラー。

- [ ] **Step 3: 実装する**

`HEADER` を書き換える。1 段落目の管理操作の列挙と、`label` / `rotated_at` の行:

```rust
# Generating, rotating and revoking keys from a command line is a job for
# the application server that embeds sapphire-framework — the framework
# itself ships no subcommands.
```

```rust
# label       optional. A note for you, like an authorized_keys comment.
#             Also accepted in place of the id anywhere a command asks for
#             a key.
# created_at  optional. RFC 3339. Filled in on load when blank.
# rotated_at  optional. RFC 3339. When the token was last replaced. Absent
#             until the first rotation; never filled in on load.
# expires_at  optional. RFC 3339. Absent means the key never expires.
```

`KeyEntry` と `RawKey` に `rotated_at` を足す（`created_at` の直後）:

```rust
pub struct KeyEntry {
    pub token: String,
    pub id: Uuid,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    /// token を最後に差し替えた時刻。一度も再発行していなければ `None`。
    pub rotated_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}
```

```rust
struct RawKey {
    token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rotated_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<DateTime<Utc>>,
}
```

`load` の `entries.push(KeyEntry { ... })` に `rotated_at: k.rotated_at,` を、`save_entries` の `RawKey { ... }` に `rotated_at: e.rotated_at,` を足す。`load` の `filled` 判定には**足さない** — `rotated_at` は補完対象ではない。

トークン生成を `generate` から関数に切り出し、`rotate` と共有する（`fn create_private` の近く、`fn constant_time_eq` の前に置く）:

```rust
/// `<prefix>_<43 文字の乱数>` を作る。
fn mint_token(prefix: &str) -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| Error::KeyFile(format!("no randomness available: {e}")))?;
    let random = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    Ok(format!("{prefix}_{random}"))
}
```

`generate` の中の 4 行（`bytes` / `getrandom` / `random` / `format!`）を `token: mint_token(prefix)?,` に置き換え、`generate` の `KeyEntry` 構築に `rotated_at: None,` を足す。

`revoke` の直後に `rotate` を足す:

```rust
    /// `selector`（`id` またはラベル）の鍵の token だけを差し替える。
    /// id・label・`created_at` は保つ。
    ///
    /// `prefix` を受け取るのは、旧トークンから接頭辞を取り出せないため。この
    /// ファイルは手書きの `token` を許しており、`<prefix>_<random>` 形式とは
    /// 限らないので `split_once('_')` は当てにならない。
    ///
    /// `expires_at` は保持ではなく置き換え。期限切れの鍵を期限そのままで再発行
    /// しても使えないので、呼び出し側に指定させる。`None` は「無期限」。
    ///
    /// 旧トークンは即座に無効になる。猶予期間は持たない — 私設網・単一運用者
    /// という脅威モデルで 2 本目の生きた秘密を抱える理由が薄い。必要になったら
    /// 同じエントリに `previous_token` を足す形で後付けできる。
    pub fn rotate(
        &mut self,
        prefix: &str,
        selector: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<KeyEntry> {
        let i = self.resolve(selector)?;
        let token = mint_token(prefix)?;

        let mut candidate = self.entries.clone();
        let entry = &mut candidate[i];
        entry.token = token;
        entry.expires_at = expires_at;
        entry.rotated_at = Some(Utc::now());
        let rotated = entry.clone();

        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(rotated)
    }
```

- [ ] **Step 4: テストが通ることを確認**

```
cargo test -p sapphire-framework-remote-server
cargo check -p sapphire-framework --all-features
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

Expected: すべて PASS。

- [ ] **Step 5: コミット**

```bash
git add crates/sapphire-framework-remote-server/src/keys.rs
git commit -F- <<'EOF'
feat(remote-server): re-issue a key without changing who it is

The store could mint and revoke but not re-issue, so changing a token meant
revoke + generate — and a new id. The id exists precisely so a key survives
a label change; a single token refresh was breaking the one thing it was
there to hold onto. `rotate` keeps the id, the label and `created_at`, and
records `rotated_at`.

`prefix` is an argument because the old token cannot supply it: the file
accepts a hand-written `token`, which need not be `<prefix>_<random>`, so
`split_once('_')` is not something to lean on. Such a token is normalised
by a rotation.

`expires_at` replaces rather than carries over. Re-issuing an expired key
without extending it produces a key that still does not work, so the caller
says what the new expiry is — the same shape `generate` already has. An
expired key can therefore be revived: expiry parks a key, it does not
remove it.

No grace period. A second live secret is not worth having on a private
network with one operator, and it can be added later as another field on
the same entry without disturbing the one-id-per-entry rule.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

## Self-Review

**Spec coverage:**

| Spec の項目 | 実装するタスク |
| --- | --- |
| 決めたこと 1（`rotate`） | Task 3 |
| 決めたこと 2（`generate` が id を受け取る） | Task 2 |
| 決めたこと 3（`Uuid` へ戻す） | Task 1 |
| 決めたこと 4（label は自由記述のまま） | 変更なしなので実装タスク無し。Task 3 でヘッダの `label` 行から「Nothing in the system reads it」を落とす分だけが該当 |
| 決めたこと 5（アプリ側の `add-device` / `allow_unknown_device`） | **本プランの対象外**（spec の「スコープ外」通り、各アプリの spec で扱う） |
| 設計 1（`rotate`） | Task 3 |
| 設計 2（`generate` の id） | Task 2 |
| 設計 3（`Uuid` / 再エクスポート / `fresh_id` 削除 / 重複検出は残す） | Task 1 |
| 設計 4（`resolve` の一本化、decoy テストの削除） | Task 1 |
| 設計 5（`rotated_at` とヘッダ） | Task 3 |
| 移行（BREAKING CHANGE の記述） | Task 1・Task 2 のコミットメッセージ |

`generate` に `NewKey` 構造体を使わない判断は spec の設計 2 に記録済み。プランは spec 通り引数追加で実装する。

**型の整合:** `KeyEntry` は Task 1 で `id: Uuid`、Task 3 で `rotated_at: Option<DateTime<Utc>>` が増える。Task 3 のテストが `store.generate("sjt", None, Some(...), None)` の 4 引数形を使っているのは Task 2 の変更後だから。`resolve` は Task 1 で private として作り、Task 3 の `rotate` が使う。`mint_token` は Task 3 で導入し `generate` と `rotate` が共有する。

**Placeholder scan:** 「適切なエラー処理を足す」「Task N と同様」の類は無し。全ステップに実際のコードが入っている。
