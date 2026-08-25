# アプリ固有サービスと remote-workspace の共存 — 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** アプリ固有サービス（MCP など）が同一プロセスで remote-workspace 同期 API と共存でき、そのサービスがファイルへ直接書いた内容も同期に載るようにする。

**Architecture:** `WsStore` を「既存ワークスペース＋既存 retrieve ストア」に向けられる注入コンストラクタで開けるようにし、アプリの書き込みを `record_local_write` で change log に載せる。取りこぼしは `sapphire-framework-track` ベースの `reconcile` で回収する。認証は単一トークンからラベル付き鍵ファイルへ移し、tower layer として切り出してアプリのルートにも同じ鍵をかけられるようにする。

**Tech Stack:** Rust 2024 edition / axum 0.8 / redb 2 / tokio 1 / uuid 1 / base64 0.22 / chrono 0.4 / thiserror 2

**Spec:** `docs/superpowers/specs/2026-08-25-app-service-coexistence-design.md`

## Global Constraints

- 対象 crate: `sapphire-framework-remote-server`, `sapphire-framework-rpc`, `sapphire-framework-remote-client`, `sapphire-framework`（ファサード）。
- 追加する外部依存は **`getrandom` のみ**。`uuid` には `v4` feature を追加する（`v7` は既に有効）。
- **鍵の `id` は UUID v4。change log の generation は UUID v7。** 取り違えないこと。
- トークン形式は `<prefix>_<random>`。区切りは**アンダースコア**。乱数部は `getrandom` で 32 バイト取り、`base64` の `URL_SAFE_NO_PAD` で符号化（43 文字）。
- 鍵ファイルは**平文**保存。書き込みは**全上書き**で、先頭に固定ヘッダコメントを毎回再生成する。`toml_edit` は使わない。
- **同期対象を絞る設定フックは作らない。** `reconcile` のスキャンだけが `.git/` を固定ルールで無視する。
- テスト実行は `cargo test -p <crate>`。全体確認は `cargo test --workspace`。
- 既存の `WsStore::open(base_dir, ws)` の**ディスク上のレイアウトは変えない**（既存の timer-server のデータを孤立させないため）。

---

### Task 1: `WsStoreConfig` — 既存ワークスペースに向けられる `WsStore`

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/ws_store.rs`
- Modify: `crates/sapphire-framework-remote-server/src/lib.rs`（re-export）
- Modify: `crates/sapphire-framework-remote-server/Cargo.toml`（`sapphire-track` 依存追加）
- Test: `crates/sapphire-framework-remote-server/src/ws_store.rs`（末尾の `mod tests`）

**Interfaces:**
- Consumes: 既存の `WsStore::open(base_dir, ws)`、`sapphire_retrieve::{RetrieveStore, open_redb}`、`sapphire_blob::FsBlobStore`、`crate::change_log::ChangeLog`
- Produces:
  - `pub struct WsStoreConfig { pub origin_dir: PathBuf, pub state_dir: PathBuf, pub retrieve: Option<Arc<dyn RetrieveStore + Send + Sync>> }`
  - `pub fn WsStore::with_config(config: WsStoreConfig) -> Result<WsStore>`
  - `WsStore` は内部に `track: Box<dyn TrackStore>` を持つ（Task 3 で使う）

- [ ] **Step 1: `sapphire-track` を依存に追加する**

`crates/sapphire-framework-remote-server/Cargo.toml` の `[dependencies]` に追記:

```toml
sapphire-track = { package = "sapphire-framework-track", version = "0.1.0", path = "../sapphire-framework-track" }
```

- [ ] **Step 2: 失敗するテストを書く**

`crates/sapphire-framework-remote-server/src/ws_store.rs` の `mod tests` に追記:

```rust
#[test]
fn with_config_uses_the_given_origin_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let origin = tmp.path().join("my-journal");
    std::fs::create_dir_all(&origin).unwrap();

    let store = WsStore::with_config(WsStoreConfig {
        origin_dir: origin.clone(),
        state_dir: tmp.path().join("server-state"),
        retrieve: None,
    })
    .unwrap();

    store
        .push(0, vec![Change::upsert("a.md", "hello", Utc::now())])
        .unwrap();

    // origin_dir 直下に書かれること（origin/<ws>/ を掘らない）
    assert_eq!(std::fs::read_to_string(origin.join("a.md")).unwrap(), "hello");
}

#[test]
fn with_config_reuses_an_injected_retrieve_store() {
    let tmp = tempfile::tempdir().unwrap();
    let retrieve = sapphire_retrieve::open_redb(&tmp.path().join("shared.redb")).unwrap();

    let store = WsStore::with_config(WsStoreConfig {
        origin_dir: tmp.path().join("origin"),
        state_dir: tmp.path().join("state"),
        retrieve: Some(std::sync::Arc::clone(&retrieve)),
    })
    .unwrap();

    store
        .push(0, vec![Change::upsert("a.md", "hello", Utc::now())])
        .unwrap();

    // 注入したストア側から見えること = 二重インデックスになっていない
    assert_eq!(retrieve.document_count().unwrap(), 1);
}
```

- [ ] **Step 3: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-remote-server with_config`
Expected: FAIL — `cannot find struct WsStoreConfig` / `no function or associated item named with_config`

- [ ] **Step 4: 最小実装を書く**

`ws_store.rs` の `WsStore` 定義と `open` を次で置き換える:

```rust
/// [`WsStore::with_config`] の入力。アプリが既に持つワークスペースとキャッシュを
/// そのまま使わせるための注入点。
pub struct WsStoreConfig {
    /// 同期対象のファイルが実際に置かれているディレクトリ。
    pub origin_dir: PathBuf,
    /// change log / blob / track db を置くサーバ側の作業ディレクトリ。
    pub state_dir: PathBuf,
    /// アプリが既に持っている retrieve ストア。`None` なら `state_dir` 配下に自前で開く。
    pub retrieve: Option<Arc<dyn RetrieveStore + Send + Sync>>,
}

/// Storage for a single workspace on the server.
pub struct WsStore {
    origin_dir: PathBuf,
    retrieve: Arc<dyn RetrieveStore + Send + Sync>,
    change_log: ChangeLog,
    blobs: FsBlobStore,
    track: Box<dyn sapphire_track::TrackStore>,
}

impl WsStore {
    /// Open (creating as needed) the stores for one workspace under `base_dir`,
    /// namespaced by `ws`. ディスク上のレイアウトは従来通り。
    pub fn open(base_dir: &Path, ws: &str) -> Result<Self> {
        let safe = sanitize(ws);
        let origin_dir = base_dir.join("origin").join(&safe);
        std::fs::create_dir_all(&origin_dir)?;
        let retrieve = open_redb(&base_dir.join("cache").join(format!("{safe}.redb")))?;
        let change_log = ChangeLog::open(&base_dir.join("changelog").join(format!("{safe}.redb")))?;
        let blobs = FsBlobStore::open(base_dir.join("blobs").join(&safe))?;
        let track_path = base_dir.join("track").join(format!("{safe}.redb"));
        std::fs::create_dir_all(track_path.parent().unwrap())?;
        let track = Box::new(sapphire_track::open_redb(&track_path)?);
        Ok(Self { origin_dir, retrieve, change_log, blobs, track })
    }

    /// Open the stores for a workspace that already exists on disk.
    pub fn with_config(config: WsStoreConfig) -> Result<Self> {
        let WsStoreConfig { origin_dir, state_dir, retrieve } = config;
        std::fs::create_dir_all(&origin_dir)?;
        std::fs::create_dir_all(&state_dir)?;
        let retrieve = match retrieve {
            Some(r) => r,
            None => open_redb(&state_dir.join("cache.redb"))?,
        };
        let change_log = ChangeLog::open(&state_dir.join("changelog.redb"))?;
        let blobs = FsBlobStore::open(state_dir.join("blobs"))?;
        let track = Box::new(sapphire_track::open_redb(&state_dir.join("track_v1.redb"))?);
        Ok(Self { origin_dir, retrieve, change_log, blobs, track })
    }
```

`lib.rs` の re-export を更新:

```rust
pub use ws_store::{WsStore, WsStoreConfig};
```

- [ ] **Step 5: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-remote-server`
Expected: PASS（既存テストも含めて全緑）

- [ ] **Step 6: コミット**

```bash
git add crates/sapphire-framework-remote-server/Cargo.toml \
        crates/sapphire-framework-remote-server/src/ws_store.rs \
        crates/sapphire-framework-remote-server/src/lib.rs
git commit -m "feat(remote-server): open a WsStore against an existing workspace

WsStoreConfig injects the origin directory and, optionally, a retrieve store
the app already owns, so a server hosting an app's own service does not build
a second index over the same files. WsStore::open keeps its on-disk layout."
```

---

### Task 2: `record_local_write` — アプリの書き込みを change log に載せる

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/ws_store.rs`
- Test: `crates/sapphire-framework-remote-server/src/ws_store.rs`（`mod tests`）

**Interfaces:**
- Consumes: Task 1 の `WsStore`、`ChangeLog::{append, latest_per_path, max_seq}`
- Produces: `pub fn WsStore::record_local_write(&self, paths: &[String], updated_at: DateTime<Utc>) -> Result<Cursor>`

- [ ] **Step 1: 失敗するテストを書く**

`mod tests` に追記:

```rust
#[test]
fn record_local_write_publishes_a_file_written_behind_the_log() {
    let (_t, store) = store();

    // アプリが ops 経由で直接書いた、という想定
    std::fs::write(store.origin_dir.join("a.md"), "written by the app").unwrap();
    let cursor = store
        .record_local_write(&["a.md".to_owned()], Utc::now())
        .unwrap();

    assert_eq!(cursor, 1);
    let (changes, _) = store.change_log.since(0, 10).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path, "a.md");
    assert!(matches!(&changes[0].kind, ChangeKind::Upsert { body, .. } if body == "written by the app"));
}

#[test]
fn record_local_write_records_a_missing_file_as_a_delete() {
    let (_t, store) = store();
    store
        .push(0, vec![Change::upsert("a.md", "hello", Utc::now())])
        .unwrap();
    std::fs::remove_file(store.origin_dir.join("a.md")).unwrap();

    store
        .record_local_write(&["a.md".to_owned()], Utc::now())
        .unwrap();

    let snapshot = store.snapshot().unwrap();
    assert!(snapshot.docs.is_empty());
}

#[test]
fn record_local_write_is_idempotent_for_unchanged_content() {
    let (_t, store) = store();
    std::fs::write(store.origin_dir.join("a.md"), "same").unwrap();

    let first = store.record_local_write(&["a.md".to_owned()], Utc::now()).unwrap();
    let second = store.record_local_write(&["a.md".to_owned()], Utc::now()).unwrap();

    assert_eq!(first, second, "内容が同じなら seq を進めない");
}

#[test]
fn record_local_write_batches_a_rename_into_one_call() {
    let (_t, store) = store();
    store
        .push(0, vec![Change::upsert("1_old.md", "body", Utc::now())])
        .unwrap();

    std::fs::rename(
        store.origin_dir.join("1_old.md"),
        store.origin_dir.join("1_new.md"),
    )
    .unwrap();
    store
        .record_local_write(&["1_old.md".to_owned(), "1_new.md".to_owned()], Utc::now())
        .unwrap();

    let snapshot = store.snapshot().unwrap();
    let paths: Vec<_> = snapshot.docs.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(paths, vec!["1_new.md"]);
}
```

- [ ] **Step 2: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-remote-server record_local_write`
Expected: FAIL — `no method named record_local_write`

- [ ] **Step 3: 最小実装を書く**

`ws_store.rs` の `impl WsStore` に追加（`push` の直後）:

```rust
    /// `paths`（ワークスペース相対・POSIX 区切り）を origin から読み直し、実在する
    /// ものを `Upsert`、消えているものを `Delete` として change log に追記する。
    ///
    /// アプリのサービスがファイルを直接書いたあとに呼ぶ。`push` と違い競合判定は
    /// 行わない — サーバ上のファイルが既に真であり、log をそれに追随させるのが
    /// この API の役目のため。内容が前回と同一なら追記しない。
    ///
    /// **1 回の呼び出しが 1 バッチ。** リネーム（旧パス削除＋新パス作成）は必ず
    /// 同じ呼び出しに含めること。分けて記録すると、pull した側が一瞬エントリを
    /// 失ったり二重に見えたりする。
    pub fn record_local_write(
        &self,
        paths: &[String],
        updated_at: DateTime<Utc>,
    ) -> Result<Cursor> {
        let latest = self.change_log.latest_per_path()?;
        let mut applied = false;

        for path in paths {
            let abs = self.origin_dir.join(posix_to_native(path));
            let change = match std::fs::read_to_string(&abs) {
                Ok(body) => {
                    // 同一内容なら何もしない。
                    if let Some(existing) = latest.get(path) {
                        if let ChangeKind::Upsert { body: old, .. } = &existing.kind {
                            if old == &body {
                                continue;
                            }
                        }
                    }
                    Change::upsert(path.clone(), body, updated_at)
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // 既に tombstone / 未知のパスなら何もしない。
                    match latest.get(path) {
                        Some(existing) if matches!(existing.kind, ChangeKind::Delete) => continue,
                        None => continue,
                        _ => Change {
                            seq: 0,
                            path: path.clone(),
                            kind: ChangeKind::Delete,
                            updated_at,
                        },
                    }
                }
                Err(e) => return Err(Error::Io(e)),
            };

            self.apply_one(&change)?;
            self.change_log.append(change)?;
            applied = true;
        }

        if applied {
            self.retrieve.rebuild_fts()?;
        }
        self.change_log.max_seq()
    }
```

`ws_store.rs` の `use` に `chrono::{DateTime, Utc}` を追加する。

- [ ] **Step 4: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-remote-server`
Expected: PASS

- [ ] **Step 5: コミット**

```bash
git add crates/sapphire-framework-remote-server/src/ws_store.rs
git commit -m "feat(remote-server): record writes made behind the change log

An app service that writes through its own ops layer was invisible to
changes.pull, and the next client push silently overwrote it. record_local_write
reads those paths back and appends them, treating one call as one batch so a
rename lands as a single delete+upsert."
```

---

### Task 3: `reconcile` — 取りこぼしを回収する整合スキャン

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/ws_store.rs`
- Modify: `crates/sapphire-framework-remote-server/src/lib.rs`（re-export）
- Test: `crates/sapphire-framework-remote-server/src/ws_store.rs`（`mod tests`）

**Interfaces:**
- Consumes: Task 2 の `record_local_write`、`sapphire_track::{scan, diff, TrackStore, Observed}`
- Produces:
  - `pub enum Detection { Mtime }`
  - `pub struct ReconcileReport { pub upserted: usize, pub removed: usize, pub detection: Detection }`
  - `pub fn WsStore::reconcile(&self) -> Result<ReconcileReport>`

- [ ] **Step 1: 失敗するテストを書く**

```rust
#[test]
fn reconcile_picks_up_a_hand_written_file() {
    let (_t, store) = store();
    std::fs::write(store.origin_dir.join("manual.md"), "typed in by hand").unwrap();

    let report = store.reconcile().unwrap();

    assert_eq!(report.upserted, 1);
    assert_eq!(report.removed, 0);
    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.docs.len(), 1);
    assert_eq!(snapshot.docs[0].path, "manual.md");
}

#[test]
fn reconcile_picks_up_a_hand_deleted_file() {
    let (_t, store) = store();
    std::fs::write(store.origin_dir.join("a.md"), "hello").unwrap();
    store.reconcile().unwrap();

    std::fs::remove_file(store.origin_dir.join("a.md")).unwrap();
    let report = store.reconcile().unwrap();

    assert_eq!(report.removed, 1);
    assert!(store.snapshot().unwrap().docs.is_empty());
}

#[test]
fn reconcile_ignores_the_git_directory() {
    let (_t, store) = store();
    let git = store.origin_dir.join(".git").join("objects");
    std::fs::create_dir_all(&git).unwrap();
    std::fs::write(git.join("deadbeef"), "packfile guts").unwrap();

    let report = store.reconcile().unwrap();

    assert_eq!(report.upserted, 0, ".git/ は同期対象に載せない");
    assert!(store.snapshot().unwrap().docs.is_empty());
}

#[test]
fn reconcile_is_quiet_when_nothing_changed() {
    let (_t, store) = store();
    std::fs::write(store.origin_dir.join("a.md"), "hello").unwrap();
    store.reconcile().unwrap();

    let report = store.reconcile().unwrap();

    assert_eq!(report.upserted, 0);
    assert_eq!(report.removed, 0);
}
```

- [ ] **Step 2: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-remote-server reconcile`
Expected: FAIL — `no method named reconcile`

- [ ] **Step 3: 最小実装を書く**

`ws_store.rs` に追加:

```rust
/// `reconcile` が変更を検出した方法。将来 mtime から内容ハッシュへ上げられるよう
/// 報告に残す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detection {
    /// mtime（秒分解能）の比較。同一秒内の連続書き込みは検出できない。
    Mtime,
}

/// [`WsStore::reconcile`] の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub upserted: usize,
    pub removed: usize,
    pub detection: Detection,
}
```

`impl WsStore` に追加:

```rust
    /// origin を走査し、track db との差分を change log に反映する。
    ///
    /// [`record_local_write`](Self::record_local_write) の呼び忘れ、サーバ上での
    /// 手作業、外部ツールの編集を回収するための安全網。起動直後に 1 回と、
    /// 呼び出し側が回す定期ティックから呼ぶ想定（このメソッド自身はタイマーを
    /// 持たない）。
    pub fn reconcile(&self) -> Result<ReconcileReport> {
        let observed = sapphire_track::scan(&self.origin_dir, |p| !is_inside_git_dir(p))
            .map_err(|e| Error::Redb(e.to_string()))?;
        let stored = self.track.mtimes().map_err(|e| Error::Redb(e.to_string()))?;
        let changes = sapphire_track::diff(&stored, &observed);

        let mut paths: Vec<String> = changes
            .upserted()
            .filter_map(|p| self.to_ws_path(p))
            .collect();
        let upserted = paths.len();
        let removed_paths: Vec<String> = changes
            .removed
            .iter()
            .filter_map(|p| self.to_ws_path(p))
            .collect();
        let removed = removed_paths.len();
        paths.extend(removed_paths);

        if !paths.is_empty() {
            self.record_local_write(&paths, Utc::now())?;
        }

        // track db を今回の観測に合わせる。
        let entries: Vec<(String, i64)> = observed
            .iter()
            .map(|o| (o.path.to_string_lossy().into_owned(), o.mtime))
            .collect();
        self.track
            .upsert_many(&entries)
            .map_err(|e| Error::Redb(e.to_string()))?;
        for p in &changes.removed {
            self.track
                .remove(&p.to_string_lossy())
                .map_err(|e| Error::Redb(e.to_string()))?;
        }

        Ok(ReconcileReport { upserted, removed, detection: Detection::Mtime })
    }

    /// 絶対パスを origin 相対の POSIX パスへ。origin の外なら `None`。
    fn to_ws_path(&self, abs: &Path) -> Option<String> {
        let rel = abs.strip_prefix(&self.origin_dir).ok()?;
        let mut out = String::new();
        for comp in rel.components() {
            if !out.is_empty() {
                out.push('/');
            }
            out.push_str(&comp.as_os_str().to_string_lossy());
        }
        Some(out)
    }
}

/// `.git/` 配下かどうか。ユーザーが journal ルートを外部ツールとして git 管理する
/// 運用があるため、スキャンは固定でここを無視する（設定項目にはしない）。
fn is_inside_git_dir(path: &Path) -> bool {
    path.components()
        .any(|c| c.as_os_str() == std::ffi::OsStr::new(".git"))
}
```

`lib.rs` の re-export を更新:

```rust
pub use ws_store::{Detection, ReconcileReport, WsStore, WsStoreConfig};
```

- [ ] **Step 4: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-remote-server`
Expected: PASS

- [ ] **Step 5: コミット**

```bash
git add crates/sapphire-framework-remote-server/src/ws_store.rs \
        crates/sapphire-framework-remote-server/src/lib.rs
git commit -m "feat(remote-server): reconcile the origin against the change log

Catches what record_local_write missed: a forgotten call, an edit made on the
server by hand, an external tool. Detection is mtime at second resolution, so
ReconcileReport names the method and can be raised to content hashing later.
The scan skips .git/ by a fixed rule."
```

---

### Task 4: change log の世代 ID

**Files:**
- Modify: `crates/sapphire-framework-remote-server/src/change_log.rs`
- Modify: `crates/sapphire-framework-remote-server/src/ws_store.rs`
- Modify: `crates/sapphire-framework-remote-server/src/lib.rs`（dispatch で照合）
- Modify: `crates/sapphire-framework-rpc/src/lib.rs`（型に `generation`）
- Modify: `crates/sapphire-framework-rpc/src/jsonrpc.rs`（エラーコード）
- Modify: ルート `Cargo.toml`, `crates/sapphire-framework-rpc/Cargo.toml`, `crates/sapphire-framework-remote-server/Cargo.toml`, `crates/sapphire-framework-remote-client/Cargo.toml`（`uuid`）
- Modify: `crates/sapphire-framework-remote-client/src/lib.rs`
- Test: `crates/sapphire-framework-remote-server/tests/rpc.rs`

**Interfaces:**
- Consumes: Task 1 の `WsStore`
- Produces:
  - `pub fn ChangeLog::generation(&self) -> Result<Uuid>`
  - `pub fn WsStore::generation(&self) -> Result<Uuid>`
  - `SnapshotResult.generation: Uuid`
  - `ChangesPullParams.generation: Option<Uuid>` / `ChangesPushParams.generation: Option<Uuid>`
  - `error_codes::GENERATION_MISMATCH: i64 = -32003`

- [ ] **Step 1: `uuid` を依存に追加する**

ルート `Cargo.toml` の `[workspace.dependencies]` の `uuid` に `v4` を足す:

```toml
uuid = { version = "1", features = ["v4", "v7", "serde"] }
```

`crates/sapphire-framework-rpc/Cargo.toml`、`crates/sapphire-framework-remote-server/Cargo.toml`、
`crates/sapphire-framework-remote-client/Cargo.toml` の `[dependencies]` にそれぞれ:

```toml
uuid.workspace = true
```

- [ ] **Step 2: 失敗するテストを書く**

`crates/sapphire-framework-remote-server/tests/rpc.rs` に追記:

```rust
#[tokio::test]
async fn snapshot_reports_a_stable_generation() {
    let (_t, st) = state(None);
    let first = call(&st, None, methods::WORKSPACE_SNAPSHOT, json!({"ws": "w"})).await;
    let second = call(&st, None, methods::WORKSPACE_SNAPSHOT, json!({"ws": "w"})).await;

    let g1 = serde_json::from_value::<SnapshotResult>(first.result.unwrap())
        .unwrap()
        .generation;
    let g2 = serde_json::from_value::<SnapshotResult>(second.result.unwrap())
        .unwrap()
        .generation;

    assert_eq!(g1, g2);
    assert_eq!(g1.get_version_num(), 7, "generation は UUIDv7");
}

#[tokio::test]
async fn pull_with_a_foreign_generation_is_rejected() {
    let (_t, st) = state(None);
    let response = call(
        &st,
        None,
        methods::CHANGES_PULL,
        json!({"ws": "w", "since": 0, "limit": 10, "generation": uuid::Uuid::nil()}),
    )
    .await;

    let err = response.error.expect("expected an error");
    assert_eq!(err.code, sapphire_rpc::error_codes::GENERATION_MISMATCH);
}

#[tokio::test]
async fn pull_without_a_generation_is_accepted() {
    let (_t, st) = state(None);
    let response = call(&st, None, methods::CHANGES_PULL, json!({"ws": "w", "since": 0, "limit": 10})).await;
    assert!(response.error.is_none());
}
```

`tests/rpc.rs` の `use sapphire_rpc::{...}` に `SnapshotResult` を追加し、
`[dev-dependencies]` に `uuid.workspace = true` を足す。

- [ ] **Step 3: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-remote-server generation`
Expected: FAIL — `no field 'generation' on type SnapshotResult` / `cannot find value GENERATION_MISMATCH`

- [ ] **Step 4: rpc 側の型を変更する**

`crates/sapphire-framework-rpc/src/jsonrpc.rs` の `error_codes` に追加:

```rust
    /// クライアントのカーソルがサーバの change log の世代と一致しない。
    /// `workspace.snapshot` からやり直すこと。
    pub const GENERATION_MISMATCH: i64 = -32003;
```

`crates/sapphire-framework-rpc/src/lib.rs` の `SnapshotResult`:

```rust
pub struct SnapshotResult {
    /// Highest applied change-log position.
    pub cursor: Cursor,
    /// この change log の世代。作り直されると変わる。
    pub generation: uuid::Uuid,
    /// Live documents (tombstones folded out), each as an `Upsert` change.
    pub docs: Vec<Change>,
}
```

`ChangesPullParams` と `ChangesPushParams` の両方に追記:

```rust
    /// クライアントが把握している change log の世代。`None` は照合を省く。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<uuid::Uuid>,
```

- [ ] **Step 5: change log に世代を持たせる**

`change_log.rs` の `TABLE` 定義の下に:

```rust
/// `key -> value` のメタデータ表（現状 `generation` のみ）。
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
const GENERATION_KEY: &str = "generation";
```

`open` の中、`wtx.open_table(TABLE)?;` の直後に:

```rust
        {
            let mut meta = wtx.open_table(META)?;
            if meta.get(GENERATION_KEY)?.is_none() {
                // v7: 先頭が時刻由来なので、この log がいつ初期化されたかが ID から読める。
                meta.insert(GENERATION_KEY, uuid::Uuid::now_v7().to_string().as_str())?;
            }
        }
```

`impl ChangeLog` に追加:

```rust
    /// この change log の世代。作成時に一度だけ採番される。
    pub fn generation(&self) -> Result<uuid::Uuid> {
        let rtx = self.db.begin_read()?;
        let meta = rtx.open_table(META)?;
        let raw = meta
            .get(GENERATION_KEY)?
            .map(|v| v.value().to_owned())
            .unwrap_or_default();
        uuid::Uuid::parse_str(&raw)
            .map_err(|e| crate::error::Error::Redb(format!("bad generation {raw:?}: {e}")))
    }
```

- [ ] **Step 6: サーバ側で世代を返し、照合する**

`ws_store.rs` の `snapshot` を差し替え、`generation` を足す:

```rust
    /// この workspace の change log の世代。
    pub fn generation(&self) -> Result<uuid::Uuid> {
        self.change_log.generation()
    }

    /// Current live document set (tombstones folded out) plus the cursor.
    pub fn snapshot(&self) -> Result<SnapshotResult> {
        let cursor = self.change_log.max_seq()?;
        let generation = self.change_log.generation()?;
        let mut docs: Vec<Change> = self
            .change_log
            .latest_per_path()?
            .into_values()
            .filter(|c| matches!(c.kind, ChangeKind::Upsert { .. }))
            .collect();
        docs.sort_by(|a, b| a.seq.cmp(&b.seq));
        Ok(SnapshotResult { cursor, generation, docs })
    }
```

`lib.rs` の `dispatch` の 2 か所に照合を挟む:

```rust
        methods::CHANGES_PULL => {
            let p: ChangesPullParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            check_generation(&store, p.generation)?;
            run(move || store.pull(p.since, p.limit)).await.and_then(to_value)
        }
        methods::CHANGES_PUSH => {
            let p: ChangesPushParams = parse_params(req.params)?;
            let store = open_ws(&state, &p.ws)?;
            check_generation(&store, p.generation)?;
            run(move || store.push(p.base_cursor, p.changes)).await.and_then(to_value)
        }
```

`lib.rs` の `parse_params` の近くに:

```rust
/// クライアントが世代を名乗ってきたときだけ照合する。名乗らないクライアントは
/// 当面そのまま通す。
fn check_generation(
    store: &Arc<WsStore>,
    claimed: Option<uuid::Uuid>,
) -> std::result::Result<(), JsonRpcError> {
    let Some(claimed) = claimed else {
        return Ok(());
    };
    let actual = store.generation().map_err(|e| e.to_jsonrpc())?;
    if claimed == actual {
        Ok(())
    } else {
        Err(JsonRpcError::new(
            error_codes::GENERATION_MISMATCH,
            format!("change log generation is {actual}, client claimed {claimed}; re-snapshot"),
        ))
    }
}
```

- [ ] **Step 7: クライアントに世代を覚えさせる**

`crates/sapphire-framework-remote-client/src/lib.rs`。`pull` / `push` のシグネチャは変えない
（`sapphire-framework-backend` へ波及させないため）。`RemoteClient` の構造体に追加:

```rust
    /// 直近の `snapshot` が返した世代。`pull` / `push` に自動で添える。
    generation: std::sync::Mutex<Option<uuid::Uuid>>,
```

`RemoteClient::new` の初期化に `generation: std::sync::Mutex::new(None),` を足し、
`snapshot` を次の形にする:

```rust
        let result: SnapshotResult = self
            .call(methods::WORKSPACE_SNAPSHOT, SnapshotParams { ws: ws.to_owned() })
            .await?;
        *self.generation.lock().unwrap() = Some(result.generation);
        Ok(result)
```

`pull` / `push` のパラメータ構築に次の 1 行を足す:

```rust
                generation: *self.generation.lock().unwrap(),
```

- [ ] **Step 8: テストが通ることを確認する**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 9: コミット**

```bash
git add Cargo.toml crates/sapphire-framework-rpc \
        crates/sapphire-framework-remote-server \
        crates/sapphire-framework-remote-client
git commit -m "feat(rpc): stamp the change log with a generation id

Cursors are change-log sequence numbers, so rebuilding the log silently made
every client's cursor point at something else. The log now carries a UUIDv7 —
v7 so the id also says when the log was initialised — and pull/push reject a
cursor from a foreign generation with GENERATION_MISMATCH so the client
re-snapshots. The client tracks it internally, leaving pull/push signatures
untouched."
```

---

### Task 5: `KeyStore` — ラベル付きトークンの鍵ファイル

**Files:**
- Create: `crates/sapphire-framework-remote-server/src/keys.rs`
- Modify: `crates/sapphire-framework-remote-server/src/lib.rs`（`mod keys;` と re-export）
- Modify: `crates/sapphire-framework-remote-server/Cargo.toml`（`getrandom`, `toml`）
- Test: `crates/sapphire-framework-remote-server/src/keys.rs`（`mod tests`）

**Interfaces:**
- Consumes: なし（独立）
- Produces:
  - `pub struct KeyEntry { pub token: String, pub id: Uuid, pub label: Option<String>, pub created_at: DateTime<Utc>, pub expires_at: Option<DateTime<Utc>> }`
  - `pub fn KeyEntry::is_expired(&self, now: DateTime<Utc>) -> bool`
  - `pub struct KeyStore`
  - `pub fn KeyStore::load(path: &Path) -> Result<KeyStore>`
  - `pub fn KeyStore::generate(&mut self, prefix: &str, label: Option<String>, expires_at: Option<DateTime<Utc>>) -> Result<KeyEntry>`
  - `pub fn KeyStore::revoke(&mut self, selector: &str) -> Result<KeyEntry>`
  - `pub fn KeyStore::entries(&self) -> &[KeyEntry]`
  - `pub fn KeyStore::authenticate(&self, token: &str) -> Option<&KeyEntry>`
  - `pub fn KeyStore::has_usable_key(&self) -> bool`

- [ ] **Step 1: 依存を追加する**

`crates/sapphire-framework-remote-server/Cargo.toml` の `[dependencies]` に:

```toml
toml.workspace = true
getrandom = "0.2"
```

- [ ] **Step 2: 失敗するテストを書く**

`crates/sapphire-framework-remote-server/src/keys.rs` を新規作成し、まずテストだけ書く:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn path(tmp: &tempfile::TempDir) -> std::path::PathBuf {
        tmp.path().join("keys.toml")
    }

    #[test]
    fn load_fills_in_a_hand_written_entry_and_writes_it_back() {
        let tmp = tempfile::tempdir().unwrap();
        let p = path(&tmp);
        std::fs::write(&p, "[[key]]\ntoken = \"sjt_hand\"\nlabel = \"typed by hand\"\n").unwrap();

        let store = KeyStore::load(&p).unwrap();

        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].id.get_version_num(), 4, "鍵の id は UUIDv4");
        assert_eq!(store.entries()[0].label.as_deref(), Some("typed by hand"));

        // 補完がファイルへ書き戻されていること
        let reloaded = KeyStore::load(&p).unwrap();
        assert_eq!(reloaded.entries()[0].id, store.entries()[0].id);
        assert_eq!(reloaded.entries()[0].created_at, store.entries()[0].created_at);
    }

    #[test]
    fn generate_uses_the_given_prefix_and_authenticates() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();

        let entry = store.generate("sjt", Some("laptop".into()), None).unwrap();

        assert!(entry.token.starts_with("sjt_"));
        assert_eq!(entry.token.len(), "sjt_".len() + 43);
        assert_eq!(store.authenticate(&entry.token).unwrap().id, entry.id);
        assert!(store.authenticate("sjt_nope").is_none());
    }

    #[test]
    fn an_expired_key_does_not_authenticate() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let past = Utc::now() - Duration::hours(1);

        let entry = store.generate("sjt", None, Some(past)).unwrap();

        assert!(store.authenticate(&entry.token).is_none());
        assert_eq!(store.entries().len(), 1, "期限切れでも自動削除はしない");
    }

    #[test]
    fn revoke_accepts_an_id_or_a_label_and_rejects_an_ambiguous_one() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let a = store.generate("sjt", Some("dup".into()), None).unwrap();
        store.generate("sjt", Some("dup".into()), None).unwrap();
        let solo = store.generate("sjt", Some("solo".into()), None).unwrap();

        assert!(store.revoke("dup").is_err(), "ラベル重複は id を要求する");
        assert_eq!(store.revoke("solo").unwrap().id, solo.id);
        assert_eq!(store.revoke(&a.id.to_string()).unwrap().id, a.id);
        assert_eq!(store.entries().len(), 1);
    }

    #[test]
    fn saving_regenerates_the_header_comment() {
        let tmp = tempfile::tempdir().unwrap();
        let p = path(&tmp);
        let mut store = KeyStore::load(&p).unwrap();
        store.generate("sjt", None, None).unwrap();

        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.starts_with("# "), "先頭に書式説明のヘッダが要る");
        assert!(text.contains("label"));
    }
}
```

- [ ] **Step 3: テストが失敗することを確認する**

`lib.rs` に `mod keys;` を足したうえで:

Run: `cargo test -p sapphire-framework-remote-server keys`
Expected: FAIL — `cannot find type KeyStore in this scope`

- [ ] **Step 4: 最小実装を書く**

`keys.rs` の先頭（`mod tests` の上）に:

```rust
//! ラベル付き API キーの平文ファイル。
//!
//! 脅威モデルはプライベート網であり、鍵ファイルはサーバ上にある。ハッシュ化が
//! 守るものは乏しい一方、新しいクライアントを設定するときに既存の鍵を読み直せる
//! 利便性が効くため、平文で保存する。
//!
//! 書き込みは常に全上書きで、先頭の書式説明コメントを毎回再生成する。注釈用途は
//! `label` が担うので、ユーザーの独自コメントは保持しない。

use std::path::{Path, PathBuf};

use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

/// 保存時に毎回書き出す書式説明。
const HEADER: &str = "\
# sapphire API keys — managed by the server's gen-key / revoke-key subcommands.
#
# token       required. `<prefix>_<random>`. Generated; do not edit.
# id          optional. UUIDv4. Filled in on load when blank. Used to tie a key
#             to a user, so it survives a label change.
# label       optional. A note for you, like an authorized_keys comment.
#             Nothing in the system reads it.
# created_at  optional. RFC 3339. Filled in on load when blank.
# expires_at  optional. RFC 3339. Absent means the key never expires.
#
# This file is rewritten in full on every change; comments you add are lost.
";

/// 一件の API キー。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEntry {
    pub token: String,
    pub id: Uuid,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl KeyEntry {
    /// `expires_at` を過ぎているか。
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|e| e <= now)
    }
}

/// ファイル上の表現。手書きを許すため `id` / `created_at` は省略可。
#[derive(Debug, Serialize, Deserialize)]
struct RawKey {
    token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RawFile {
    #[serde(default)]
    key: Vec<RawKey>,
}

/// 鍵ファイルとその中身。
pub struct KeyStore {
    path: PathBuf,
    entries: Vec<KeyEntry>,
}

impl KeyStore {
    /// 読み込み、欠けた `id` / `created_at` を補完する。補完があれば書き戻す。
    /// ファイルが無い場合は空のストアを返す（作成はしない）。
    pub fn load(path: &Path) -> Result<Self> {
        let raw: RawFile = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::Redb(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RawFile::default(),
            Err(e) => return Err(Error::Io(e)),
        };

        let mut filled = false;
        let now = Utc::now();
        let entries = raw
            .key
            .into_iter()
            .map(|k| {
                if k.id.is_none() || k.created_at.is_none() {
                    filled = true;
                }
                KeyEntry {
                    token: k.token,
                    // 識別子として短縮表示するため v4。近い時刻の値で上位桁が
                    // 揃ってしまう v7 は使わない（change log の世代とは逆）。
                    id: k.id.unwrap_or_else(Uuid::new_v4),
                    label: k.label,
                    created_at: k.created_at.unwrap_or(now),
                    expires_at: k.expires_at,
                }
            })
            .collect();

        let store = Self { path: path.to_path_buf(), entries };
        if filled {
            store.save()?;
        }
        Ok(store)
    }

    pub fn entries(&self) -> &[KeyEntry] {
        &self.entries
    }

    /// 新しい鍵を生成して追記・保存する。
    pub fn generate(
        &mut self,
        prefix: &str,
        label: Option<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<KeyEntry> {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes)
            .map_err(|e| Error::Redb(format!("no randomness available: {e}")))?;
        let random = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);

        let entry = KeyEntry {
            token: format!("{prefix}_{random}"),
            id: Uuid::new_v4(),
            label,
            created_at: Utc::now(),
            expires_at,
        };
        self.entries.push(entry.clone());
        self.save()?;
        Ok(entry)
    }

    /// `selector`（`id` またはラベル）に一致する鍵を削除する。ラベルが複数一致
    /// する場合はエラーにして `id` を要求する。
    pub fn revoke(&mut self, selector: &str) -> Result<KeyEntry> {
        let by_id: Vec<usize> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| e.id.to_string() == selector)
            .map(|(i, _)| i)
            .collect();
        let matches: Vec<usize> = if by_id.is_empty() {
            self.entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.label.as_deref() == Some(selector))
                .map(|(i, _)| i)
                .collect()
        } else {
            by_id
        };

        match matches.as_slice() {
            [] => Err(Error::Redb(format!("no key matches {selector:?}"))),
            [i] => {
                let removed = self.entries.remove(*i);
                self.save()?;
                Ok(removed)
            }
            many => Err(Error::Redb(format!(
                "{} keys share the label {selector:?}; pass the id instead",
                many.len()
            ))),
        }
    }

    /// 提示されたトークンを検証する。期限切れは `None`。
    pub fn authenticate(&self, token: &str) -> Option<&KeyEntry> {
        let now = Utc::now();
        self.entries
            .iter()
            .find(|e| constant_time_eq(e.token.as_bytes(), token.as_bytes()) && !e.is_expired(now))
    }

    /// 有効な（期限切れでない）鍵が 1 件以上あるか。
    pub fn has_usable_key(&self) -> bool {
        let now = Utc::now();
        self.entries.iter().any(|e| !e.is_expired(now))
    }

    /// ヘッダコメントを再生成して全上書きする。
    fn save(&self) -> Result<()> {
        let raw = RawFile {
            key: self
                .entries
                .iter()
                .map(|e| RawKey {
                    token: e.token.clone(),
                    id: Some(e.id),
                    label: e.label.clone(),
                    created_at: Some(e.created_at),
                    expires_at: e.expires_at,
                })
                .collect(),
        };
        let body = toml::to_string_pretty(&raw)
            .map_err(|e| Error::Redb(format!("serializing keys: {e}")))?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, format!("{HEADER}\n{body}"))?;
        restrict_permissions(&self.path)?;
        Ok(())
    }
}

/// 秘密の比較は長さ以外の情報を漏らさない形で行う。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_permissions(path: &Path) -> Result<()> {
    // Windows には 0600 と等価な一発の API が無い。ACL の絞り込みは行わないので、
    // 置き場所で守る前提であることを呼び出し側に伝える。
    tracing::warn!(
        path = %path.display(),
        "key file permissions are not restricted on this platform; keep it out of shared directories"
    );
    Ok(())
}
```

`lib.rs` に追加:

```rust
mod keys;
pub use keys::{KeyEntry, KeyStore};
```

- [ ] **Step 5: テストが通ることを確認する**

Run: `cargo test -p sapphire-framework-remote-server keys`
Expected: PASS

- [ ] **Step 6: コミット**

```bash
git add crates/sapphire-framework-remote-server/Cargo.toml \
        crates/sapphire-framework-remote-server/src/keys.rs \
        crates/sapphire-framework-remote-server/src/lib.rs
git commit -m "feat(remote-server): labelled API keys in a plaintext key file

Implements framework #92. Keys carry a v4 id that survives a label change, so
a future writer-identity feature can tie a user to the id rather than the
label. Hand-written entries only need a token; id and created_at are filled in
on load and written back. Saving rewrites the file whole with a regenerated
header, so no toml_edit."
```

---

### Task 6: 認証 layer — `/rpc` と外部ルートに同じ鍵をかける

**Files:**
- Create: `crates/sapphire-framework-remote-server/src/auth.rs`
- Modify: `crates/sapphire-framework-remote-server/src/lib.rs`
- Modify: `crates/sapphire-framework-remote-server/tests/rpc.rs`
- Modify: `crates/sapphire-framework-remote-client/src/lib.rs`（HTTP 401 の扱い）
- Modify: `crates/sapphire-framework-remote-client/tests/roundtrip.rs`

**Interfaces:**
- Consumes: Task 5 の `KeyStore` / `KeyEntry`
- Produces:
  - `pub struct Authenticated { pub key_id: Uuid, pub label: Option<String> }`
  - `pub fn protect(state: Arc<ServerState>, router: Router) -> Router`
  - `pub fn ServerState::with_keys(self, keys: Arc<KeyStore>) -> Self`（`with_token` は削除）
  - `pub fn ServerState::keys(&self) -> Option<&Arc<KeyStore>>`
  - `router(state)` は認証適用済みを返す

**注意:** これまで未認証は HTTP 200 ＋ JSON-RPC `UNAUTHORIZED` を返していたが、layer 化により
**HTTP 401** になる。クライアントとテストの両方を追従させること。

- [ ] **Step 1: 失敗するテストを書く**

`crates/sapphire-framework-remote-server/tests/rpc.rs` の `state` ヘルパを差し替え、
テストを追加する:

```rust
fn state(token: Option<&str>) -> (tempfile::TempDir, Arc<ServerState>) {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = ServerState::new(tmp.path());
    if let Some(t) = token {
        // テストは固定トークンを使いたいので、生成ではなく直接書いた鍵を読ませる。
        let key_path = tmp.path().join("keys.toml");
        std::fs::write(&key_path, format!("[[key]]\ntoken = \"{t}\"\n")).unwrap();
        s = s.with_keys(Arc::new(KeyStore::load(&key_path).unwrap()));
    }
    (tmp, Arc::new(s))
}

#[tokio::test]
async fn a_bad_token_is_rejected_with_http_401() {
    let (_t, st) = state(Some("sjt_secret"));
    let req = JsonRpcRequest::new(Value::from(1), methods::WORKSPACE_SNAPSHOT, json!({"ws": "w"}));
    let http_req = Request::builder()
        .method("POST")
        .uri("/rpc")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer wrong")
        .body(Body::from(serde_json::to_vec(&req).unwrap()))
        .unwrap();

    let response = router(Arc::clone(&st)).oneshot(http_req).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn protect_guards_a_foreign_route_with_the_same_key() {
    let (_t, st) = state(Some("sjt_secret"));
    let app = protect(
        Arc::clone(&st),
        Router::new().route("/mcp", axum::routing::get(|| async { "ok" })),
    );

    let unauthorized = app
        .clone()
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = app
        .oneshot(
            Request::builder()
                .uri("/mcp")
                .header(header::AUTHORIZATION, "Bearer sjt_secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
}

#[tokio::test]
async fn an_authenticated_request_carries_the_key_id() {
    let (_t, st) = state(Some("sjt_secret"));
    let key_id = st.keys().unwrap().entries()[0].id;

    let app = protect(
        Arc::clone(&st),
        Router::new().route(
            "/whoami",
            axum::routing::get(|axum::Extension(who): axum::Extension<Authenticated>| async move {
                who.key_id.to_string()
            }),
        ),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/whoami")
                .header(header::AUTHORIZATION, "Bearer sjt_secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), key_id.to_string());
}
```

```rust
#[tokio::test]
async fn serve_refuses_to_start_without_a_usable_key() {
    let (_t, st) = state(None);

    // ポートを掴む前に弾かれるので、bind せずに戻ってくる。
    let err = serve("127.0.0.1:0".parse().unwrap(), st).await.unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
```

`tests/rpc.rs` の `use` に次を足す:

```rust
use axum::Router;
use sapphire_framework_remote_server::{Authenticated, KeyStore, ServerState, protect, router, serve};
```

既存のテストのうち、未認証時に JSON-RPC エラーを期待しているものは HTTP 401 を期待する形へ
直す。

- [ ] **Step 2: テストが失敗することを確認する**

Run: `cargo test -p sapphire-framework-remote-server`
Expected: FAIL — `no method named with_keys` / `cannot find function protect`

- [ ] **Step 3: 認証 layer を実装する**

`crates/sapphire-framework-remote-server/src/auth.rs` を新規作成:

```rust
//! 鍵による認証を tower layer として提供する。
//!
//! framework のルートと、アプリが同じプロセスで生やす自前のルート（MCP など）に
//! **同じ鍵**をかけられるようにするためのもの。`/rpc` は守られているのに `/mcp` は
//! 素通し、という事故を避ける。

use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    middleware::{Next, from_fn_with_state},
    response::Response,
};
use uuid::Uuid;

use crate::ServerState;

/// 認証に成功したリクエストの拡張として入る値。
///
/// 将来 `Change` に書き込み元を持たせるときは、rpc 型に 1 フィールド足して
/// ここの `key_id` を読むだけでよい。
#[derive(Clone, Debug)]
pub struct Authenticated {
    pub key_id: Uuid,
    pub label: Option<String>,
}

/// `router` を `state` の鍵で保護して返す。鍵が設定されていない場合は素通しし、
/// 警告を出す（テスト用途のみ。`serve` は鍵なしの起動を拒否する）。
pub fn protect(state: Arc<ServerState>, router: Router) -> Router {
    if state.keys().is_none() {
        tracing::warn!("no key store configured; this router is unauthenticated");
        return router;
    }
    router.layer(from_fn_with_state(state, authenticate))
}

async fn authenticate(
    State(state): State<Arc<ServerState>>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, StatusCode> {
    let Some(keys) = state.keys() else {
        return Ok(next.run(request).await);
    };

    let presented = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let entry = keys.authenticate(presented).ok_or(StatusCode::UNAUTHORIZED)?;
    let who = Authenticated { key_id: entry.id, label: entry.label.clone() };

    request.extensions_mut().insert(who);
    Ok(next.run(request).await)
}
```

- [ ] **Step 4: `ServerState` を鍵ストア方式へ移す**

`lib.rs`。`token: Option<String>` を `keys: Option<Arc<KeyStore>>` に置き換え、
`with_token` と `authorized` を削除する:

```rust
pub struct ServerState {
    data_dir: PathBuf,
    keys: Option<Arc<KeyStore>>,
    workspaces: Mutex<HashMap<String, Arc<WsStore>>>,
}

impl ServerState {
    /// Create server state rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self { data_dir: data_dir.into(), keys: None, workspaces: Mutex::new(HashMap::new()) }
    }

    /// `Authorization: Bearer <token>` を、この鍵ストアに対して検証させる。
    pub fn with_keys(mut self, keys: Arc<KeyStore>) -> Self {
        self.keys = Some(keys);
        self
    }

    /// 設定されている鍵ストア。
    pub fn keys(&self) -> Option<&Arc<KeyStore>> {
        self.keys.as_ref()
    }
```

`rpc_handler` から認証判定のブロックと `headers: HeaderMap` 引数を削除する（layer が担う）。
`router` と `serve` を更新:

```rust
/// Build the axum router for `state` (single `POST /rpc` endpoint). 認証は適用済み。
pub fn router(state: Arc<ServerState>) -> Router {
    let routes = Router::new()
        .route("/rpc", post(rpc_handler))
        .with_state(Arc::clone(&state));
    crate::auth::protect(state, routes)
}

/// Bind `addr` and serve until the process is stopped.
///
/// 有効な鍵が 1 件も無い場合は起動を拒否する。認証なしで待ち受ける状態を作らない。
pub async fn serve(addr: SocketAddr, state: Arc<ServerState>) -> std::io::Result<()> {
    match state.keys() {
        Some(keys) if keys.has_usable_key() => {}
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no usable API key configured; run `gen-key` first",
            ));
        }
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "sapphire remote server listening");
    axum::serve(listener, router(state)).await
}
```

`lib.rs` に `mod auth;` と `pub use auth::{Authenticated, protect};` を追加し、未使用になる
`HeaderMap` の `use` を削除する。

- [ ] **Step 5: クライアントを HTTP 401 に追従させる**

`crates/sapphire-framework-remote-client/src/lib.rs` の `call` で、レスポンスを JSON として
読む前にステータスを見る:

```rust
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::Rpc {
                code: sapphire_rpc::error_codes::UNAUTHORIZED,
                message: "missing or invalid bearer token".to_owned(),
            });
        }
```

`crates/sapphire-framework-remote-client/tests/roundtrip.rs` の `with_token` 呼び出しを
Step 1 と同じ `KeyStore` 経由に差し替える。「間違ったトークンで拒否される」テストは
`Error::Rpc { code: UNAUTHORIZED, .. }` を期待する形のままで通る。

- [ ] **Step 6: テストが通ることを確認する**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 7: doc example を直す**

`crates/sapphire-framework-remote-server/src/lib.rs` 冒頭の doc example が `with_token` を
使っているので更新する:

```rust
//! let keys = Arc::new(KeyStore::load(std::path::Path::new("/etc/sapphire/keys.toml")).unwrap());
//! let state = Arc::new(ServerState::new("/var/lib/sapphire").with_keys(keys));
//! serve("127.0.0.1:8080".parse().unwrap(), state).await
```

Run: `cargo test -p sapphire-framework-remote-server --doc`
Expected: PASS

- [ ] **Step 8: コミット**

```bash
git add crates/sapphire-framework-remote-server crates/sapphire-framework-remote-client
git commit -m "feat(remote-server)!: authenticate with a key store, via a layer

Auth moves out of the /rpc handler into a tower layer, so an app hosting its
own routes in the same process can wrap them with protect() and cannot end up
with /rpc guarded and /mcp open. The layer resolves the key id into an
Authenticated extension, which is what a future writer-identity feature reads.

BREAKING: ServerState::with_token is replaced by with_keys, serve() refuses to
start without a usable key, and an unauthorized request is now HTTP 401 rather
than a 200 carrying a JSON-RPC error."
```

---

### Task 7: `sapphire-timer-server` のコンパイル追従

**別リポジトリ**（`sapphire-timer` サブモジュール）で行う。framework の `with_token` 削除で
壊れるため、framework 側が main に入るのと前後して追従させる。

**Files:**
- Modify: `sapphire-timer/sapphire-timer-server/src/main.rs`

**Interfaces:**
- Consumes: Task 5 の `KeyStore`、Task 6 の `ServerState::with_keys`

- [ ] **Step 1: sapphire-timer に作業ブランチを作る**

```bash
cd ../sapphire-timer
git checkout -b fix/framework-key-store main
```

- [ ] **Step 2: `--token` を鍵ファイルへ移す**

`sapphire-timer-server/src/main.rs` の `--token` 引数を `--keys` に置き換える:

```rust
    /// Path to the API key file. Defaults to `<data_dir>/keys.toml`.
    #[arg(long, env = "SAPPHIRE_TIMER_SERVER_KEYS", value_name = "FILE")]
    keys: Option<PathBuf>,
```

`main` の state 構築を差し替える:

```rust
    let keys_path = args.keys.unwrap_or_else(|| data_dir.join("keys.toml"));
    let keys = KeyStore::load(&keys_path)
        .with_context(|| format!("loading API keys from {}", keys_path.display()))?;
    let state = Arc::new(ServerState::new(&data_dir).with_keys(Arc::new(keys)));
```

`use` を更新する:

```rust
use sapphire_framework::remote_server::{KeyStore, ServerState, serve};
```

- [ ] **Step 3: ビルドとテストを確認する**

Run: `cargo build -p sapphire-timer-server`
Expected: SUCCESS

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 4: 鍵が無いと起動しないことを手で確かめる**

```bash
cargo run -p sapphire-timer-server -- --data-dir /tmp/sapphire-timer-keytest
```
Expected: `no usable API key configured; run gen-key first` を表示して終了する

- [ ] **Step 5: コミット**

```bash
git add sapphire-timer-server
git commit -m "fix(server): follow the framework key store

ServerState::with_token is gone; load keys.toml instead. gen-key style
subcommands are not added here — this only keeps the binary compiling and
refuses to start unauthenticated."
```

---

## 完了後

framework 側はここまでで単体で緑になる。次は journal 側の計画
（`sapphire-journal/docs/superpowers/plans/`）で `sapphire-journal-server` を作る。
