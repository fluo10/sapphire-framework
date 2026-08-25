# アプリ固有サービスと remote-workspace の共存（framework 側）

- 日付: 2026-08-25
- 対象: `sapphire-framework-remote-server`, `sapphire-framework-rpc`
- 関連: `sapphire-journal` の `2026-08-25-remote-workspace-server-design.md`（本 spec を前提に書かれている）

## 背景

`sapphire-journal` / `-ledger` / `-timer` は、いずれも **1 つのサーバプロセス**で

- framework のリモートワークスペース同期 API（`POST /rpc`）
- アプリ固有の MCP エンドポイント（`/mcp`）

の両方を提供したい。journal での当面の目的は、**人間（同期クライアント経由）と AI（MCP 経由）が
同じワークスペースを共同編集できるようにする**こと。同一ファイルの同時編集（Google Docs 的な
文字単位マージ）は要求しない。レコード単位で別々のものを並行編集できれば十分。

各アプリのサーバは固有の MCP エンドポイントを持つため、**複数アプリを 1 プロセスに相乗りさせる
ことはしない**。framework は「サーバ組み立てキット」を提供する側に徹し、束ねる役はやらない。

## 現状で噛み合っていない点

1. **origin とキャッシュが固定レイアウト。**
   `WsStore::open` は `base_dir/origin/<ws>` を掘り、retrieve キャッシュも
   `base_dir/cache/<ws>.redb` を自前で開く（`ws_store.rs`）。アプリの実ワークスペース
   （journal なら `.sapphire-journal/` を含むルート）を origin にできず、アプリが既に持つ
   retrieve ストアと**同じファイル群に対してインデックスが 2 つ**できてしまう。

2. **アプリの書き込みが change log に載らない。**
   change log へ追記されるのは `changes.push` 経由の書き込みだけ。`snapshot` / `pull` はその
   log から組み立てられる。アプリ固有サービスがファイルを直接書くと、
   (a) その編集が `changes.pull` に出てこない、
   (b) 次にクライアントが push したとき LWW 比較の相手が古いままなので、**アプリ側の編集を
   黙って上書きする**。

3. **認証が framework のルート内に閉じている。**
   単一 `Option<String>` トークンをハンドラ内の `authorized(&headers)` で検証する形
   （`lib.rs`）。`/mcp` など framework 外のルートに同じ認証をかける手段がない。

4. **カーソルの寿命が change log の寿命に縛られている。**
   `Cursor = u64` は change log の連番。log を作り直すとクライアントのカーソルが黙って
   別物を指す。

## スコープ

**含む**: 上記 1〜4 の解消と、ラベル付き複数トークンの鍵ファイル管理。

**含まない**（将来）: 書き込みごとの作成者記録（`Change` への writer フィールド追加）、
鍵ごとの権限差（読み取り専用鍵など）、TLS / OAuth、CRDT、blob の GC。

## 設計

### 1. `WsStore` を既存ワークスペースに向けられるようにする

`WsStore::open(base_dir, ws)` は互換のため残しつつ、注入版を追加する。

```rust
pub struct WsStoreConfig {
    /// 同期対象のファイルが実際に置かれているディレクトリ。
    pub origin_dir: PathBuf,
    /// change log / blob / track db を置くサーバ側の作業ディレクトリ。
    pub state_dir: PathBuf,
    /// アプリが既に持っている retrieve ストア。None なら state_dir 配下に自前で開く。
    pub retrieve: Option<Arc<dyn RetrieveStore + Send + Sync>>,
    /// 同期対象に含めるかを判定する。None なら全ファイル。
    pub accept: Option<Arc<dyn Fn(&Path) -> bool + Send + Sync>>,
}

impl WsStore {
    pub fn with_config(config: WsStoreConfig) -> Result<Self>;
}
```

`accept` は journal の `.sapphire-journal/` 配下（キャッシュ・設定）を同期対象から外すために要る。
**鍵ファイルは origin の外に置く方針**だが、`accept` は多層防御として機能する。

`ServerState` にも、ワークスペース名から `WsStoreConfig` を解決するフックを持たせる
（既定は現行のレイアウト、アプリは自前の解決関数を差せる）。

### 2. ローカル書き込みの記録入口

```rust
impl WsStore {
    /// `paths`（ワークスペース相対・POSIX 区切り）を origin から読み直し、
    /// 実在するものは Upsert、消えているものは Delete として change log に追記する。
    /// FTS の再構築はバッチ末尾で 1 回。戻り値は追記後のカーソル。
    pub fn record_local_write(
        &self,
        paths: &[String],
        updated_at: DateTime<Utc>,
    ) -> Result<Cursor>;
}
```

- **1 回の呼び出しが 1 バッチ**。リネーム（旧パス削除＋新パス作成）は必ず同じ呼び出しに含める。
- `changes.push` の競合判定は通さない。サーバ上のファイルが既に真であり、log をそれに追随させる
  のがこの API の役目のため。
- 冪等: 内容が前回と同一なら追記しない（無意味な `seq` の増加とクライアントの再取得を避ける）。

### 3. 整合スキャン（安全網）

`record_local_write` の呼び忘れ、サーバ上での手作業、外部ツールの編集を回収する。
`sapphire-framework-track` の `scan` / `diff` / `RedbTrackStore` をそのまま使う。

```rust
impl WsStore {
    /// origin を走査し、track db との差分を change log に反映する。戻り値は反映した件数。
    pub fn reconcile(&self) -> Result<ReconcileReport>;
}
```

- track db は `state_dir` 配下。`Changes { added, modified, removed }` をそのまま
  `record_local_write` 相当の処理に流す。
- 呼び出し時点は**起動直後に 1 回**と、アプリが回す定期ティック。framework はタイマーを持たない。
- **既知の限界**: `track` の mtime 分解能は秒。同一秒内の連続書き込みは検出できない。主経路は
  2 の明示記録であり、これはあくまで安全網なので許容する。取りこぼしが問題になったら
  内容ハッシュ比較に上げる（`ReconcileReport` に検出方法を残しておき、後で差し替えられるようにする）。

### 4. change log の世代 ID

change log 作成時に `generation: Uuid`（v4）を採番し、log 自身に保存する。

- `SnapshotResult` に `generation` を追加。
- `ChangesPullParams` / `ChangesPushParams` に `generation: Option<Uuid>` を追加。
- サーバは不一致なら新しいエラーコード `GENERATION_MISMATCH` を返す。クライアントは
  `snapshot` からやり直す。`None` は「世代を知らない古いクライアント」として当面は許容する。

これが無いと「サーバのキャッシュを作り直したら差分が壊れた」を後で踏む。
`sapphire-framework-rpc` の型変更になるが、アプリは全て git 依存で未 publish のため今なら安い。

### 5. 鍵ファイル（ラベル付き複数トークン）

**置き場所**: origin の外。パスは呼び出し側が指定する（既定はサーバの設定ファイルの隣）。
origin 配下に置くと同期でクライアントに配られてしまうため、ここは譲れない。

**形式**（`keys.toml`）:

```toml
[[key]]
token = "sjj_9f3a…"          # 必須。プレフィクスはアプリが指定する
id = "3f2a…"                 # 任意。空欄なら読み込み時に UUID v4 を採番して書き戻す
label = "laptop"             # 任意。authorized_keys のコメント相当。システムは参照しない
created_at = "2026-08-25T…"  # 任意。空欄なら読み込み時に現在時刻を入れて書き戻す
expires_at = "2026-11-23T…"  # 任意。無ければ無期限
```

- **UUID は v4**。v7 は先頭が時刻由来のため、近い時刻に発行した鍵ほど上位桁が揃い、
  短縮表示での識別に向かない。`uuid` 依存に `v4` feature を追加する。
- **トークンは平文保存**。脅威モデル（プライベート網・鍵ファイルはサーバ上にある）では
  ハッシュ化が守るものが乏しい一方、新しいクライアントを設定するときに既存の鍵を読み直せる
  利便性が効く。ファイルは `0600`（Windows では ACL 相当の最小権限）で作成する。
- **書き戻しは全上書き**。先頭に書式を説明する固定ヘッダコメントを毎回再生成する。
  ユーザーの独自コメントは失われるが、注釈用途は `label` が担うため `toml_edit` は導入しない。
- 手動追記は許容する（`token` だけ書けば起動時に `id` / `created_at` が補完される）。

```rust
pub struct KeyEntry {
    pub token: String,
    pub id: Uuid,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

pub struct KeyStore { /* path + entries */ }

impl KeyStore {
    /// 読み込み、欠けた id / created_at を補完し、補完があればファイルへ書き戻す。
    pub fn load(path: &Path) -> Result<Self>;
    /// 新しい鍵を生成して追記・保存し、生成したエントリを返す。
    pub fn generate(&mut self, prefix: &str, label: Option<String>, expires_at: Option<DateTime<Utc>>) -> Result<KeyEntry>;
    pub fn revoke(&mut self, selector: &str) -> Result<KeyEntry>; // id または label
    pub fn entries(&self) -> &[KeyEntry];
    /// 提示されたトークンを検証する。期限切れは None。
    pub fn authenticate(&self, token: &str) -> Option<&KeyEntry>;
}
```

- 期限切れの鍵は**自動削除しない**。残したまま「期限切れ」として扱う。勝手に消えるより、
  なぜ繋がらないかが分かるほうがよい。
- `revoke` の `selector` は id 優先。label は重複しうるため、複数一致したらエラーにして
  id を要求する。
- **鍵ファイルが存在しないか有効な鍵が 0 件なら、サーバは起動を拒否する**。認証なしで
  待ち受ける状態を作らない。

### 6. 認証 layer

```rust
/// framework のルート。認証は適用済み。
pub fn router(state: Arc<ServerState>) -> Router;

/// 任意のルータを、同じ鍵で保護して返す。アプリの /mcp 用。
pub fn protect(state: Arc<ServerState>, router: Router) -> Router;

/// 認証成功時にリクエスト拡張へ入る値。
#[derive(Clone, Debug)]
pub struct Authenticated {
    pub key_id: Uuid,
    pub label: Option<String>,
}
```

- `ServerState::with_token`（単一トークン）は `KeyStore` を持つ形に置き換える。
- ハンドラ内の `authorized()` は廃し、layer に一本化する。
- **`Authenticated` に `key_id` を載せるところまでを今回やる。** 将来 `Change` に
  書き込み元を持たせるときは、rpc 型に 1 フィールド足して layer が載せた値を読むだけになる。
  ここを単一トークンのまま作ると、その時点で認証層を書き直すことになる。
- 「認証をかけ忘れる」事故を型で防ぐため、`router()` は認証適用済みを返し、
  アプリ側は自分のサービスを `protect()` に通す形にする。

## テスト

- `record_local_write` 後に `changes.pull` で見えること。同内容の再呼び出しで `seq` が
  増えないこと（冪等）。
- リネーム相当（旧パス削除＋新パス作成）を 1 バッチで記録し、pull 側で一貫して見えること。
- `reconcile` が手書きファイルの追加・変更・削除を拾うこと。
- 世代不一致で `GENERATION_MISMATCH` が返り、`snapshot` からやり直せること。
- `KeyStore`: `token` だけ書いた `keys.toml` を読むと `id` / `created_at` が補完されて
  書き戻ること。期限切れ鍵が `authenticate` で弾かれること。`revoke` の label 重複でエラー。
- 認証: 鍵なしで `/rpc` が 401。`protect()` を通した任意ルートも 401。
- 鍵 0 件で起動が失敗すること。
- `WsStoreConfig` の `accept` で除外したパスが change log に載らないこと。

## リスク / 留意点

1. **mtime 秒分解能**（3 で既述）。安全網の取りこぼしを内容ハッシュに上げる余地を残す。
2. **rpc 型の変更**は破壊的。未 publish の今のうちに入れる。
3. `retrieve` を注入すると、FTS 再構築のタイミングがアプリ側の再インデックスと競合しうる。
   サーバ構成では走査を 1 つのティックに統合する（アプリ側 spec で扱う）。
4. Windows のファイル権限は `0600` と等価にならない。最小権限の ACL を設定し、
   できない場合は起動時に警告を出す。
