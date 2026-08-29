# デバイス／ユーザー台帳（registry）

- 日付: 2026-08-29
- 対象: `sapphire-framework-registry`（新規クレート）, `sapphire-framework-remote-server`（`keys.rs`）,
  `sapphire-framework-workspace`（`workspace.rs`）, `sapphire-framework`（facade, prelude）
- 関連: `2026-08-29-api-key-rotation-design.md`（`KeyEntry::id` を UUID へ戻し、`generate` に
  呼び出し側の id を受け取らせた回。本 spec はその spec の項目 5「アプリ側の device 管理」を
  引き取り、**framework の側に置き直す**）
- 後続: `sapphire-agent` の `2026-08-29-device-based-auth-design.md`（最初の消費者）

## 背景

`sapphire-journal` / `sapphire-ledger` / `sapphire-agent` はいずれも「どの端末から来たか」「それは
誰の端末か」を持ちたい。用途はそれぞれ違う：

- journal: エントリのフロントマターに `updated_by` を焼き、表示時に人間の名前へ逆引きする
- agent: 認証されたデバイスから room_profile（＝ LLM プロファイルとメモリ名前空間）を決める
- ledger: journal と同じ最終更新者の記録

3 つとも「小さな TOML を 2 枚、壊さずに読み書きする」という同じ仕事を必要とする。`KeyStore` が
既に持っている作法（先頭に書式説明ヘッダを毎回再生成／全上書き／一時ファイル → rename／欠けた
フィールドは load 時に補完して書き戻す）を 3 回書き直すことになるので、framework に置く。

### アプリ間で ID を共有しない

当初「`updated_by` が意味を持つにはユーザー ID をアプリ横断で揃える必要がある」と考えたが、
**誤り**だった。これらのアプリは MCP などの API を通じて互いに 1 つのクライアントデバイスとして
連携するだけで、ID を共有する必要はどこにも無い。

フロントマターに焼かれるのは `device_id` で、表示時の解決は
`device_id → device.user_id → user.name` と**そのアプリの中で完結する**：

```
agent がジャーナルを編集
  → journal-server がトークンを解決 → device "sapphire-agent"（journal の台帳）
  → device.user_id → 「AI」ユーザー（journal の台帳）
  → updated_by: <その device_id>

僕がスマホから編集
  → journal-server がトークンを解決 → device "phone"（journal の台帳）
  → updated_by: <その device_id>
```

したがって台帳は**アプリごと**、`.{app_name}/` の下でよい。共有ルートも新しいマーカーも要らない。

## 決めたこと

1. **`sapphire-framework-registry` クレートを新設**し、facade に `registry` フィーチャで生やす。
2. **ID は grain-id。** device / user 双方。
3. **`KeyEntry` に `device_id: Option<GrainId>` を足す。** 鍵がデバイスを指す向き。
4. **`Workspace` に `devices_path()` / `users_path()` を足す。**
5. **`retired_at` によるトゥームストーン**を持つ。物理削除もできるが既定ではない。

### 1. 新クレートにする理由と、workspace クレートに依存させない理由

`sapphire-framework-workspace` は `sapphire-framework-retrieve`（tantivy / fastembed / arrow）と
`sapphire-framework-track`（redb）と `tokio = { features = ["full"] }` を引く。registry の仕事は
小さな TOML を 2 枚読み書きすることだけで、必要なのは grain-id / serde / toml / chrono /
thiserror に限られる。デバイス一覧を読みたいだけの小さなバイナリ（将来の ledger CLI、あるいは
`remote-server` 自身がデバイスを解決したくなった場合）に検索インデックスを背負わせない。

`.{app_name}/devices.toml` というパスの計算には `Workspace` が要るが、それは**呼ぶ側が既に持って
いる**。registry 自身は `&Path` を受け取る。ファイル名の規約は workspace クレート側の
`devices_path()` / `users_path()`（既存の `config_path()` の隣）で 1 箇所に決める。依存の向きは
どちらにも張らない。

### 2. ID を grain-id にする理由

`device_id` は**ジャーナルの全エントリのフロントマターに永続化される**。`sapphire-journal` の
`Frontmatter.id` は既に `GrainId` なので、`updated_by: a3f9k2p` は `id:` と同じ幅で並ぶ。UUID だと
本文より長い行が毎エントリに載る。

加えて、agent では device と room_profile の紐づけを**人間がホスト設定に手で書く**
（`devices = ["a3f9k2p"]`）。7 文字と 36 文字の差はここで効く。

`user_id` はコンテンツに出ないが、揃えない理由も無いので同じ grain-id。

`KeyEntry::id` は **UUID のまま据え置く**。あれは人間が触らない鍵の内部同一性で、`revoke` /
`rotate` のセレクタには label が使える。framework 内に UUID と grain-id が同居することになるが、
役割で分かれる — **UUID = 鍵の内部同一性、grain-id = 人間とドキュメントが参照する主体**。

### 3. 紐づけを `KeyEntry` 側に置く理由

`2026-08-29-api-key-rotation-design.md` は `generate` に `id: Option<Uuid>` を足し、「アプリが
device 行を先に書いてから鍵を作れるように」＝ **`device.id == KeyEntry.id`** を意図していた。
これは成立しない。

**鍵ファイルはサーバ（ホスト）ごとに存在し、デバイス台帳はワークスペースごとに存在する。**
1 台の物理デバイスが sapphire-agent と sapphire-journal-server の両方に喋るなら、トークンは 2 本・
別々の鍵ファイルに入る。デバイスが「グローバルに 1 つの鍵 id」を持つことはあり得ない。
デバイス → 鍵は 1 対 1 に潰れないので、**鍵の側がデバイスを指す**。多対一が自然に表現できる。

`generate` の `id: Option<Uuid>` 引数はそのまま残す（鍵の id を呼び出し側が決めたい場面は
なお存在しうる）。`device_id` はそれとは別の、新しい任意フィールド。

これにより `sapphire-framework-remote-server` が `grain-id` に依存する。registry クレートには
依存させない — 鍵ファイルは台帳を読まず、`device_id` を不透明な識別子として持つだけ。

### 4. トゥームストーン（`retired_at`）

`device_id` は消せないコンテンツに焼き付く。台帳から物理削除すると過去のフロントマターが
解決不能になるので、既定はエントリを残して `retired_at` を立てる。役割分担は：

- **アクセスの停止** → 鍵ファイル側の `KeyStore::revoke`
- **履歴の解決** → 台帳に残った retired なエントリ

物理削除は呼び出し側が明示的に要求したときだけ。

## ファイル形式

```toml
# .{app_name}/devices.toml
[[device]]
id          = "a3f9k2p"     # grain-id。空なら load 時に採番して書き戻す
name        = "pendant"     # ファイル内で一意。CLI のセレクタにも使う
description = "XIAO ESP32S3 Sense、首から下げるやつ"   # 任意
user_id     = "k3m9x2p"     # 任意。users.toml への参照
created_at  = "2026-08-29T11:00:00Z"                  # 空なら load 時に補完
retired_at  = "2026-09-01T00:00:00Z"                  # 任意
```

```toml
# .{app_name}/users.toml
[[user]]
id          = "k3m9x2p"
name        = "fluo10"      # ファイル内で一意
description = "人間"        # 任意
created_at  = "2026-08-29T11:00:00Z"
retired_at  = "..."         # 任意
```

`KeyStore` と同じく、先頭に書式説明ヘッダを毎回再生成し、保存は常に全上書き、書き込みは
一時ファイル → rename。ユーザーの独自コメントは保持しない（注釈用途は `description` が担う）。

### `User.kind` を持たない

「僕か AI か」はユーザーが誰かで判別できる。`"human" | "agent"` の列挙を足すと、後で `"tool"`
`"service"` と際限が無くなる。必要になってから足す。

## API

```rust
// sapphire-framework-registry
pub struct Device {
    pub id: GrainId,
    pub name: String,
    pub description: Option<String>,
    pub user_id: Option<GrainId>,
    pub created_at: DateTime<Utc>,
    pub retired_at: Option<DateTime<Utc>>,
}
pub struct User { /* 同型（user_id を除く） */ }

pub struct Devices { /* path + Vec<Device> */ }
impl Devices {
    pub fn load(path: &Path) -> Result<Self>;   // 欠けた id / created_at を補完し、あれば書き戻す
    pub fn entries(&self) -> &[Device];
    pub fn add(&mut self, name: &str, description: Option<String>, user_id: Option<GrainId>) -> Result<Device>;
    pub fn get(&self, id: GrainId) -> Option<&Device>;
    pub fn resolve(&self, selector: &str) -> Result<&Device>;   // id または name
    pub fn retire(&mut self, selector: &str) -> Result<Device>;
    pub fn purge(&mut self, selector: &str) -> Result<Device>;
}
// Users も同型
```

`resolve` のセレクタ解決は `KeyStore::resolve` と同じ規則にする — grain-id として読めるなら id、
読めないなら name。名前が grain-id として読めてしまう場合の逃げ道は無い（`KeyStore` が既に同じ
制約を文書化している）。ただし `name` はファイル内で一意なので、`KeyStore` の label と違って
「複数一致」は起こらない。

`load` は重複した id と重複した name をどちらもエラーにする。エントリごとコピーして複製する
事故は実際に起きる。

## テスト

`keys.rs` のテスト群と同じ形。ラウンドトリップ（保存 → 読み直しで一致）、id / `created_at` の
補完と書き戻し、重複 id / 重複 name の拒否、`retire` が解決可能性を保つこと、`purge` が消すこと、
ヘッダが新しいフィールドを説明していること、セレクタが id と name の両方で当たること。

`KeyEntry.device_id` 側は、既存の鍵ファイル（`device_id` の無い `[[key]]`）がそのまま読めること
（後方互換）と、ラウンドトリップで失われないことを見る。

## 移行

`device_id` は任意フィールドの追加なので、既存の鍵ファイルはそのまま読める。registry のファイル
2 枚は新規で、存在しなければ空として扱う（`KeyStore::load` と同じ — 作成はしない）。framework の
既存利用者に破壊的変更は無い。
