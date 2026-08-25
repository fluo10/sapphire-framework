//! Per-workspace server state: file **origin** + redb **retrieve cache** +
//! **change log** + **blob store** (Model B — the server mirrors a client).
//!
//! All methods here are synchronous (redb / tantivy / filesystem). The axum
//! layer wraps calls in `spawn_blocking`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sapphire_blob::{BlobStore, FsBlobStore};
use sapphire_retrieve::{Document, FtsQuery, RetrieveStore, open_redb};
use sapphire_rpc::{
    BlobRef, Change, ChangeKind, ChangesPullResult, ChangesPushResult, Cursor, Hit, SnapshotResult,
};

use crate::change_log::ChangeLog;
use crate::error::{Error, Result};

/// [`WsStore::with_config`] の入力。アプリが既に持つワークスペースとキャッシュを
/// そのまま使わせるための注入点。
pub struct WsStoreConfig {
    /// 同期対象のファイルが実際に置かれているディレクトリ。
    pub origin_dir: PathBuf,
    /// change log / blob / track db を置くサーバ側の作業ディレクトリ。
    pub state_dir: PathBuf,
    /// アプリが既に持っている retrieve ストア。`None` なら `state_dir` 配下に自前で開く。
    pub retrieve: Option<Arc<dyn RetrieveStore + Send + Sync>>,
    /// 同期対象に含めてよい唯一の隠しディレクトリ（例 `".sapphire-journal"`）。
    /// `None` なら隠しファイルは一切同期しない。判定は Task 3 の `is_syncable`。
    pub app_dir: Option<String>,
}

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

/// Storage for a single workspace on the server.
pub struct WsStore {
    origin_dir: PathBuf,
    retrieve: Arc<dyn RetrieveStore + Send + Sync>,
    change_log: ChangeLog,
    blobs: FsBlobStore,
    track: Box<dyn sapphire_track::TrackStore>,
    app_dir: Option<String>,
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
        // 従来レイアウトに隠しディレクトリは無いので、許可するものも無い。
        Ok(Self {
            origin_dir,
            retrieve,
            change_log,
            blobs,
            track,
            app_dir: None,
        })
    }

    /// Open the stores for a workspace that already exists on disk.
    pub fn with_config(config: WsStoreConfig) -> Result<Self> {
        let WsStoreConfig {
            origin_dir,
            state_dir,
            retrieve,
            app_dir,
        } = config;
        std::fs::create_dir_all(&origin_dir)?;
        std::fs::create_dir_all(&state_dir)?;
        let retrieve = match retrieve {
            Some(r) => r,
            None => open_redb(&state_dir.join("cache.redb"))?,
        };
        let change_log = ChangeLog::open(&state_dir.join("changelog.redb"))?;
        let blobs = FsBlobStore::open(state_dir.join("blobs"))?;
        let track = Box::new(sapphire_track::open_redb(&state_dir.join("track_v1.redb"))?);
        Ok(Self {
            origin_dir,
            retrieve,
            change_log,
            blobs,
            track,
            app_dir,
        })
    }

    // ── sync methods ────────────────────────────────────────────────────────

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

    /// Changes newer than `since`, capped at `limit`.
    pub fn pull(&self, since: Cursor, limit: usize) -> Result<ChangesPullResult> {
        let (changes, more) = self.change_log.since(since, limit)?;
        let cursor = changes.last().map(|c| c.seq).unwrap_or(since);
        Ok(ChangesPullResult {
            cursor,
            changes,
            more,
        })
    }

    /// Apply `changes` on top of `base_cursor`, last-writer-wins by
    /// `updated_at`. Paths for which the server holds a newer concurrent edit
    /// are rejected and reported in [`ChangesPushResult::conflicts`].
    pub fn push(&self, base_cursor: Cursor, changes: Vec<Change>) -> Result<ChangesPushResult> {
        // Snapshot of the server's latest change per path, used for conflict
        // detection. Updated in-place as we accept changes so two incoming
        // edits to the same path within one batch behave sensibly.
        let mut latest = self.change_log.latest_per_path()?;
        let mut conflicts = Vec::new();
        let mut applied = false;

        for change in changes {
            if let Some(existing) = latest.get(&change.path) {
                // The server moved ahead of what the client had seen…
                let concurrent = existing.seq > base_cursor;
                // …and the client's edit is not strictly newer → reject.
                if concurrent && change.updated_at <= existing.updated_at {
                    conflicts.push(change.path.clone());
                    continue;
                }
            }

            self.apply_one(&change)?;
            let stored = self.change_log.append(change)?;
            latest.insert(stored.path.clone(), stored);
            applied = true;
        }

        // A single FTS rebuild after the batch (upsert_document leaves the
        // inverted index stale until rebuilt).
        if applied {
            self.retrieve.rebuild_fts()?;
        }

        Ok(ChangesPushResult {
            cursor: self.change_log.max_seq()?,
            conflicts,
        })
    }

    /// `paths`（ワークスペース相対・POSIX 区切り）を origin から読み直し、実在する
    /// ものを `Upsert`、消えているものを `Delete` として change log に追記する。
    ///
    /// アプリのサービスがファイルを直接書いたあとに呼ぶ。`push` と違い競合判定は
    /// 行わない — サーバ上のファイルが既に真であり、log をそれに追随させるのが
    /// この API の役目のため。内容が前回と同一なら追記しない。
    ///
    /// **ファイルには一切書き込まない。** origin は既に真なので、読んだ内容を
    /// そのまま書き戻すのは無駄な上に mtime を荒らし、`reconcile` に「今まさに
    /// 自分が書いた変更」を外部編集と誤認させて二重に upsert 報告させる —
    /// だから `apply_one` は呼ばず、`is_syncable` の門番を自前でかけたあと
    /// `index_change` だけを呼ぶ。
    ///
    /// **1 回の呼び出しが 1 バッチ。** 複数パスは渡した順に記録される — リネー
    /// ムなら旧パス→新パスの順で渡せば delete→upsert の順になり、バッチ全体を
    /// 適用すれば正しい終了状態に収束する。呼び出し側が旧パスの削除を記録し
    /// 忘れることはない。
    ///
    /// ただし **これはトランザクションではない。** 各パスは別々の redb コミッ
    /// トとして追記されるため、同時に走る `pull`（特に `limit` で範囲が分割さ
    /// れた場合）はリネームを途中状態のまま観測しうる — クライアントは次の
    /// pull で収束する。呼び出し途中でクラッシュすると log が origin より遅れ
    /// た状態のまま残るが、そこからの回復は `reconcile`（Task 4）の役目。
    pub fn record_local_write(
        &self,
        paths: &[String],
        updated_at: DateTime<Utc>,
    ) -> Result<Cursor> {
        let mut latest = self.change_log.latest_per_path()?;
        let mut applied = false;

        for path in paths {
            // ガードはループの先頭で — origin の外を指すパスは、読みにすら行かせない。
            if !is_syncable(path, self.app_dir.as_deref()) {
                return Err(Error::NotSyncable(path.clone()));
            }
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
                        _ => Change::delete(path.clone(), updated_at),
                    }
                }
                // UTF-8 でないファイルは「文書ではない」として飛ばす。change は
                // 本文を String で持つので、そもそも載せようがない。
                //
                // ここで `Err` を返すとバッチ全体が落ちる。それは `reconcile`
                // にとって致命的で、`reconcile` は走査した全パスをここへ渡した
                // あとで track db を更新するため、1 バイトでも UTF-8 でない
                // ファイル（journal に紛れ込んだ PNG 1 枚）があるとその更新に
                // 到達せず、次のティックが同じ差分を再検出して同じように落ちる
                // ——安全網が恒久的に死ぬ。添付ファイルを書いた MCP ツールに
                // エラーが返るのも同じ原因。飛ばして続ける。
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    tracing::debug!(path, "not valid UTF-8; not a document, skipping");
                    continue;
                }
                Err(e) => return Err(Error::Io(e)),
            };

            self.index_change(&change)?;
            let stored = self.change_log.append(change)?;
            latest.insert(stored.path.clone(), stored);
            applied = true;
        }

        if applied {
            self.retrieve.rebuild_fts()?;
        }
        self.change_log.max_seq()
    }

    /// Write one change through to the origin file, then index it. Does
    /// **not** rebuild FTS (the caller batches that) nor append to the log.
    ///
    /// [`record_local_write`](Self::record_local_write) does *not* go through
    /// here — its whole premise is that the file on disk already holds the
    /// change, so writing it back would be a pointless, mtime-churning
    /// round-trip (and, worse, it would feed `reconcile` a false positive on
    /// its next pass, since the write it just made looks exactly like an
    /// external edit). It calls [`Self::index_change`] directly instead,
    /// after applying the same `is_syncable` guard itself.
    fn apply_one(&self, change: &Change) -> Result<()> {
        if !is_syncable(&change.path, self.app_dir.as_deref()) {
            return Err(Error::NotSyncable(change.path.clone()));
        }
        let abs = self.origin_dir.join(posix_to_native(&change.path));
        match &change.kind {
            ChangeKind::Upsert { body, .. } => {
                if let Some(parent) = abs.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&abs, body)?;
            }
            ChangeKind::Delete => match std::fs::remove_file(&abs) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::Io(e)),
            },
        }
        self.index_change(change)
    }

    /// Update the retrieve cache to match `change`. Touches neither the
    /// filesystem nor the change log — callers apply their own `is_syncable`
    /// guard and (if relevant) filesystem write before calling this.
    fn index_change(&self, change: &Change) -> Result<()> {
        match &change.kind {
            ChangeKind::Upsert { body, .. } => {
                self.retrieve.upsert_document(&Document {
                    id: path_to_doc_id(&change.path),
                    body: body.clone(),
                    path: change.path.clone(),
                    chunks: None,
                })?;
            }
            ChangeKind::Delete => {
                self.retrieve.remove_document(path_to_doc_id(&change.path))?;
            }
        }
        Ok(())
    }

    /// Store a blob, returning its content-addressed reference.
    pub fn blob_put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let r = self.blobs.put(bytes)?;
        Ok(BlobRef {
            hash: r.hash,
            len: r.len,
        })
    }

    /// Fetch a blob by hash.
    pub fn blob_get(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.blobs.get(hash)?)
    }

    /// Full-text search over the retrieve cache.
    pub fn search_fts(&self, q: &str, limit: usize) -> Result<Vec<Hit>> {
        let query = FtsQuery::new(q).limit(limit);
        let results = self.retrieve.search_fts(&query)?;
        Ok(results
            .into_iter()
            .map(|r| Hit {
                path: r.path,
                score: r.score,
                snippet: r.chunks.into_iter().next().map(|c| c.text),
            })
            .collect())
    }

    /// origin を走査し、track db との差分を change log に反映する。
    ///
    /// [`record_local_write`](Self::record_local_write) の呼び忘れ、サーバ上での
    /// 手作業、外部ツールの編集を回収するための安全網。起動直後に 1 回と、
    /// 呼び出し側が回す定期ティックから呼ぶ想定(このメソッド自身はタイマーを
    /// 持たない)。
    pub fn reconcile(&self) -> Result<ReconcileReport> {
        // 走査そのものが同期可能なパスだけを剪定する — `is_syncable` が
        // ディレクトリの descend 可否も決めるので、`.git` のようなディレクトリ
        // は中身を舐めることすらない。判定は書き込み側(apply_one)と同じ関数
        // なので、規則が二か所に散らない。
        let observed = sapphire_track::scan(&self.origin_dir, |p| {
            self.to_ws_path(p)
                .is_some_and(|rel| is_syncable(&rel, self.app_dir.as_deref()))
        })?;
        let stored = self.track.mtimes()?;
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
        self.track.upsert_many(&entries)?;
        for p in &changes.removed {
            self.track.remove(&p.to_string_lossy())?;
        }

        Ok(ReconcileReport {
            upserted,
            removed,
            detection: Detection::Mtime,
        })
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

/// FNV-1a hash of the (workspace-relative) path — the stable document id used
/// by the retrieve cache. Mirrors `sapphire_workspace::path_to_doc_id` so the
/// server and the workspace layer agree on identity.
fn path_to_doc_id(path: &str) -> i64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for b in path.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h as i64
}

/// Convert a POSIX wire path to a native relative path.
fn posix_to_native(path: &str) -> PathBuf {
    path.split('/').collect()
}

/// 同期対象に含めてよいワークスペース相対パス（POSIX 区切り）か。
///
/// 許可制にしてある。隠しファイル・隠しディレクトリは原則すべて除外し、
/// `app_dir` に名指ししたディレクトリ（例 `.sapphire-journal`）だけを通す。
/// 除外一覧を育てる方式より、除外し忘れが起きない。
///
/// 併せて `..`・絶対パス・空要素も拒否する。これが origin の外へ書かせない唯一の
/// 防壁なので、`WsStore` の書き込み経路は必ずこれを通す。
pub fn is_syncable(rel: &str, app_dir: Option<&str>) -> bool {
    if rel.is_empty() || rel.starts_with('/') {
        return false;
    }
    // ワイヤ上のパスは POSIX 区切りのみ。逆スラッシュとドライブ指定は受け付けない。
    if rel.contains('\\') || rel.contains(':') {
        return false;
    }
    rel.split('/').all(|seg| {
        if seg.is_empty() || seg == "." || seg == ".." {
            return false;
        }
        if seg.starts_with('.') {
            return app_dir == Some(seg);
        }
        true
    })
}

/// Make a workspace id safe to use as a single path component (no separators,
/// no traversal). Non-alphanumeric characters become `_`.
fn sanitize(ws: &str) -> String {
    let cleaned: String = ws
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect();
    if cleaned.is_empty() {
        "default".to_owned()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn store() -> (tempfile::TempDir, WsStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = WsStore::open(tmp.path(), "ws1").unwrap();
        (tmp, store)
    }

    #[test]
    fn push_then_pull_roundtrips() {
        let (_t, store) = store();
        let out = store
            .push(0, vec![Change::upsert("a.md", "hello", Utc::now())])
            .unwrap();
        assert_eq!(out.cursor, 1);
        assert!(out.conflicts.is_empty());

        let pulled = store.pull(0, 10).unwrap();
        assert_eq!(pulled.changes.len(), 1);
        assert_eq!(pulled.changes[0].path, "a.md");
        assert_eq!(pulled.cursor, 1);
    }

    #[test]
    fn push_writes_origin_file() {
        let (_t, store) = store();
        store
            .push(0, vec![Change::upsert("sub/b.md", "body", Utc::now())])
            .unwrap();
        let path = store.origin_dir.join("sub").join("b.md");
        assert_eq!(std::fs::read_to_string(path).unwrap(), "body");
    }

    #[test]
    fn concurrent_older_edit_conflicts() {
        let (_t, store) = store();
        let t0 = Utc::now();
        // Server accepts a newer edit at seq 1.
        store
            .push(0, vec![Change::upsert("a.md", "server", t0 + chrono::Duration::seconds(10))])
            .unwrap();
        // Client pushes an OLDER edit with base_cursor 0 (unaware of seq 1).
        let out = store
            .push(0, vec![Change::upsert("a.md", "client-stale", t0)])
            .unwrap();
        assert_eq!(out.conflicts, vec!["a.md".to_owned()]);
        // Server content unchanged.
        let snap = store.snapshot().unwrap();
        match &snap.docs[0].kind {
            ChangeKind::Upsert { body, .. } => assert_eq!(body, "server"),
            _ => panic!(),
        }
    }

    #[test]
    fn newer_concurrent_edit_wins() {
        let (_t, store) = store();
        let t0 = Utc::now();
        store.push(0, vec![Change::upsert("a.md", "server", t0)]).unwrap();
        let out = store
            .push(0, vec![Change::upsert("a.md", "client-newer", t0 + chrono::Duration::seconds(5))])
            .unwrap();
        assert!(out.conflicts.is_empty());
        let snap = store.snapshot().unwrap();
        match &snap.docs[0].kind {
            ChangeKind::Upsert { body, .. } => assert_eq!(body, "client-newer"),
            _ => panic!(),
        }
    }

    #[test]
    fn search_finds_pushed_document() {
        let (_t, store) = store();
        store
            .push(0, vec![Change::upsert("note.md", "the quick brown fox", Utc::now())])
            .unwrap();
        let hits = store.search_fts("quick", 10).unwrap();
        assert!(hits.iter().any(|h| h.path == "note.md"), "got {hits:?}");
    }

    #[test]
    fn snapshot_folds_out_tombstones() {
        let (_t, store) = store();
        store.push(0, vec![Change::upsert("a.md", "x", Utc::now())]).unwrap();
        store.push(1, vec![Change::delete("a.md", Utc::now())]).unwrap();
        let snap = store.snapshot().unwrap();
        assert!(snap.docs.is_empty(), "deleted doc must not appear in snapshot");
    }

    #[test]
    fn blob_roundtrip() {
        let (_t, store) = store();
        let r = store.blob_put(b"binary").unwrap();
        assert_eq!(store.blob_get(&r.hash).unwrap().as_deref(), Some(&b"binary"[..]));
    }

    #[test]
    fn with_config_uses_the_given_origin_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let origin = tmp.path().join("my-journal");
        std::fs::create_dir_all(&origin).unwrap();

        let store = WsStore::with_config(WsStoreConfig {
            origin_dir: origin.clone(),
            state_dir: tmp.path().join("server-state"),
            retrieve: None,
            app_dir: None,
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
            app_dir: None,
        })
        .unwrap();

        store
            .push(0, vec![Change::upsert("a.md", "hello", Utc::now())])
            .unwrap();

        // 注入したストア側から見えること = 二重インデックスになっていない
        assert_eq!(retrieve.document_count().unwrap(), 1);
    }

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

        let first = store
            .record_local_write(&["a.md".to_owned()], Utc::now())
            .unwrap();
        let second = store
            .record_local_write(&["a.md".to_owned()], Utc::now())
            .unwrap();

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

    #[test]
    fn record_local_write_rejects_a_non_syncable_path() {
        let (tmp, store) = store();

        // 万一ガードをすり抜けたら読めてしまう位置に、それとわかる内容を置いておく。
        let escaped = store.origin_dir.parent().unwrap().join("escaped.md");
        std::fs::write(&escaped, "should never be read").unwrap();

        let result = store.record_local_write(&["../escaped.md".to_owned()], Utc::now());

        assert!(matches!(result, Err(Error::NotSyncable(_))));
        let (changes, _) = store.change_log.since(0, 10).unwrap();
        assert!(
            changes.is_empty(),
            "拒否されたパスが log に載ってはならない"
        );
        assert_eq!(
            std::fs::read_to_string(&escaped).unwrap(),
            "should never be read",
            "origin の外のファイルが書き換えられてはならない"
        );
        let _ = &tmp;
    }

    #[test]
    fn record_local_write_does_not_touch_the_filesystem() {
        // mtime は秒分解能なので同一秒内の書き戻しは reconcile からは見えない
        // (reconcile_does_not_re_upsert_a_file_it_just_recorded 参照)。ここでは
        // record_local_write が保証すべき本体 — ファイルへの書き込みが一切
        // 起きないこと — を、OS のフル精度タイムスタンプで直接確認する。
        let (_t, store) = store();
        let path = store.origin_dir.join("a.md");
        std::fs::write(&path, "hello").unwrap();
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();

        // 書き込みが起きていれば秒分解能を待たずとも確実にタイムスタンプが動く
        // よう、わずかに間を置く。
        std::thread::sleep(std::time::Duration::from_millis(20));

        store
            .record_local_write(&["a.md".to_owned()], Utc::now())
            .unwrap();

        let after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(
            before, after,
            "record_local_write must not rewrite the file it just read"
        );
    }

    #[test]
    fn is_syncable_allows_ordinary_paths() {
        assert!(is_syncable("a.md", None));
        assert!(is_syncable("2026/1_note.md", None));
    }

    #[test]
    fn is_syncable_rejects_hidden_components() {
        assert!(!is_syncable(".gitignore", None));
        assert!(!is_syncable(".git/config", None));
        assert!(!is_syncable("2026/.hidden.md", None));
        assert!(!is_syncable(".sapphire-journal/config.toml", None));
    }

    #[test]
    fn is_syncable_allows_only_the_named_app_dir() {
        let app = Some(".sapphire-journal");
        assert!(is_syncable(".sapphire-journal/config.toml", app));
        assert!(
            !is_syncable(".git/config", app),
            "許可するのは名指しした 1 つだけ"
        );
        assert!(
            !is_syncable(".sapphire-journal/.git/config", app),
            "許可ディレクトリの中の隠しディレクトリも除外する"
        );
    }

    #[test]
    fn is_syncable_rejects_traversal_and_absolute_paths() {
        assert!(!is_syncable("../outside.md", None));
        assert!(!is_syncable("2026/../../outside.md", None));
        assert!(!is_syncable("/etc/passwd", None));
        assert!(!is_syncable("", None));
        assert!(!is_syncable("a//b.md", None));
        // ワイヤ上のパスは POSIX 区切りのみ。Windows 風のパスは受け付けない。
        assert!(!is_syncable("C:/windows/system32", None));
        assert!(!is_syncable("a\\b.md", None));
    }

    #[test]
    fn push_cannot_write_outside_the_origin() {
        let (tmp, store) = store();

        let result = store.push(
            0,
            vec![Change::upsert("../escaped.md", "gotcha", Utc::now())],
        );

        assert!(matches!(result, Err(Error::NotSyncable(_))));
        // `store()` の origin は `<tmp>/origin/ws1` なので、`../` の着地点はその親。
        let escaped = store.origin_dir.parent().unwrap().join("escaped.md");
        assert!(
            !escaped.exists(),
            "origin の外にファイルが作られてはならない"
        );
        let _ = &tmp;
    }

    #[test]
    fn push_cannot_write_a_hidden_path() {
        let (_t, store) = store();

        let result = store.push(0, vec![Change::upsert(".git/config", "gotcha", Utc::now())]);

        assert!(matches!(result, Err(Error::NotSyncable(_))));
    }

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
    fn reconcile_ignores_hidden_files() {
        let (_t, store) = store();
        let git = store.origin_dir.join(".git").join("objects");
        std::fs::create_dir_all(&git).unwrap();
        std::fs::write(git.join("deadbeef"), "packfile guts").unwrap();
        std::fs::write(store.origin_dir.join(".gitignore"), "target/").unwrap();

        let report = store.reconcile().unwrap();

        assert_eq!(report.upserted, 0, "隠しファイルは同期対象に載せない");
        assert!(store.snapshot().unwrap().docs.is_empty());
    }

    #[test]
    fn reconcile_picks_up_the_configured_app_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = WsStore::with_config(WsStoreConfig {
            origin_dir: tmp.path().join("origin"),
            state_dir: tmp.path().join("state"),
            retrieve: None,
            app_dir: Some(".sapphire-journal".to_owned()),
        })
        .unwrap();
        let app = store.origin_dir.join(".sapphire-journal");
        std::fs::create_dir_all(&app).unwrap();
        std::fs::write(app.join("config.toml"), "k = 1").unwrap();
        std::fs::create_dir_all(store.origin_dir.join(".git")).unwrap();
        std::fs::write(store.origin_dir.join(".git").join("HEAD"), "ref: x").unwrap();

        let report = store.reconcile().unwrap();

        assert_eq!(report.upserted, 1);
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.docs[0].path, ".sapphire-journal/config.toml");
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

    #[test]
    fn reconcile_does_not_re_upsert_a_file_it_just_recorded() {
        // record_local_write は origin にファイルを書き戻してはならない — 戻すと
        // mtime が動き、reconcile が事前に取っておいた mtime と食い違って、次の
        // reconcile でまた「変更あり」と報告してしまう。
        let (_t, store) = store();
        std::fs::write(store.origin_dir.join("manual.md"), "typed in by hand").unwrap();

        let first = store.reconcile().unwrap();
        assert_eq!(first.upserted, 1);

        let second = store.reconcile().unwrap();
        assert_eq!(
            second.upserted, 0,
            "直前に取り込んだファイルを再び upserted に数えてはならない"
        );
        assert_eq!(second.removed, 0);
    }

    #[test]
    fn push_accepts_the_configured_app_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = WsStore::with_config(WsStoreConfig {
            origin_dir: tmp.path().join("origin"),
            state_dir: tmp.path().join("state"),
            retrieve: None,
            app_dir: Some(".sapphire-journal".to_owned()),
        })
        .unwrap();

        store
            .push(
                0,
                vec![Change::upsert(
                    ".sapphire-journal/config.toml",
                    "k = 1",
                    Utc::now(),
                )],
            )
            .unwrap();

        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.docs.len(), 1);
        assert_eq!(snapshot.docs[0].path, ".sapphire-journal/config.toml");
    }

    #[test]
    fn record_local_write_skips_a_non_utf8_file() {
        let (_t, store) = store();
        // 0xFF は UTF-8 のどのシーケンスにも現れない。
        std::fs::write(store.origin_dir.join("photo.png"), [0xFFu8, 0xD8, 0xFF]).unwrap();
        std::fs::write(store.origin_dir.join("note.md"), "text").unwrap();

        let cursor = store
            .record_local_write(&["photo.png".to_owned(), "note.md".to_owned()], Utc::now())
            .unwrap();

        assert_eq!(cursor, 1, "テキストの 1 件だけが log に載る");
        let paths: Vec<String> = store
            .snapshot()
            .unwrap()
            .docs
            .into_iter()
            .map(|c| c.path)
            .collect();
        assert_eq!(paths, vec!["note.md".to_owned()]);
    }

    #[test]
    fn reconcile_survives_a_binary_file_in_the_workspace() {
        // 非 UTF-8 のファイルが 1 つあるだけで安全網が恒久的に死んではならない。
        // record_local_write が Err を返すと reconcile はその先の track db 更新に
        // 到達せず、次のティックが同じ差分を再検出して同じように落ちる。
        let (_t, store) = store();
        // 走査順（名前順）で binary がテキストの間に来るように名前を付ける。
        std::fs::write(store.origin_dir.join("a-before.md"), "before").unwrap();
        std::fs::write(
            store.origin_dir.join("m-photo.png"),
            [0x89u8, 0x50, 0x4E, 0xFF],
        )
        .unwrap();
        std::fs::write(store.origin_dir.join("z-after.md"), "after").unwrap();

        let first = store.reconcile().unwrap();
        assert_eq!(first.removed, 0);

        // バイナリの前後どちらのテキストも取り込まれていること。
        let mut paths: Vec<String> = store
            .snapshot()
            .unwrap()
            .docs
            .into_iter()
            .map(|c| c.path)
            .collect();
        paths.sort();
        assert_eq!(
            paths,
            vec!["a-before.md".to_owned(), "z-after.md".to_owned()],
            "バイナリより後ろのファイルも log に載る"
        );

        // 2 周目は何も新しく見つからない = track db が更新されている。
        let second = store.reconcile().unwrap();
        assert_eq!(second.upserted, 0, "同じ差分を再検出してはならない");
        assert_eq!(second.removed, 0);
    }
}
