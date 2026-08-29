# API キーの再発行と、鍵 id の役割の整理

- 日付: 2026-08-29
- 対象: `sapphire-framework-remote-server`（`keys.rs`, `auth.rs`）, `sapphire-framework`（prelude）
- 関連: `2026-08-25-app-service-coexistence-design.md`（鍵ファイルを導入した spec）、
  コミット `ba6b12c`（`KeyEntry::id` を UUID から grain-id へ変えた回。本 spec はこれを差し戻す）

## 背景

`KeyStore` にあるのは `generate` と `revoke` だけで、**トークンだけを更新する手段が無い**。
鍵を変えたければ revoke してから generate することになり、そのとき id が変わる。
id は「label を変えても生き残る、鍵を人やデバイスに結び付けるための手掛かり」として
置かれているのに、トークンを一度更新しただけでその手掛かりが切れてしまう。

同時に、アプリ側（`sapphire-journal` など）はクライアント端末を device として自前で管理する。
現状は「鍵を作る」操作と「作った鍵をアプリの設定でどの device に紐づけるか」が別々の手作業なので、
忘れる・食い違うという事故が構造的に起きる。

## 決めたこと

1. **`KeyStore::rotate` を足す。** id・label・`created_at` を保ったまま token だけ差し替える。
2. **`generate` が呼び出し側の id を受け取れるようにする。** アプリが先に id をミントできる。
3. **`KeyEntry::id` / `Authenticated::key_id` を `Uuid` に戻す。**
4. **`label` は自由記述のまま**（文字種の制限も一意性の強制もしない）。
5. アプリ側は `add-device` で device 行と鍵を一度に作り、`allow_unknown_device` で
   「device 表に無い鍵を通すか」を切り替える。**これはフレームワークの外の話**で、
   framework には何も要らない。framework 側の変更は 1〜3 と、そこから派生するもの
   （セレクタ解決の一本化・ファイル形式とヘッダの追従）に限られる。

### なぜ id を UUID に戻すのか

`ba6b12c` は「id の読者は人間しかいない」ことを根拠に grain-id を選んだ。挙げられていた読者は
2 つ、`revoke <id>` と「将来 main config から `keys = [...]` で参照する」。

**後者がこの設計で消える。** アプリが device 表に key_id を持ち、`add-device` が両方を書くので、
人が設定ファイルに id を書く運用が生まれない。残るのは `revoke <id>` だけで、それも label という
通常の入り口があるうえでの、ラベルが重複・不在のときのフォールバックであり、そのときは id は
ファイルの中のトークンのすぐ隣に書いてあってコピペで足りる。

「人が直接扱う id は grain-id、そうでなければ UUID」という各アプリ共通の規約に照らすと、
役割が変わった以上 UUID 側に属する。規約を曲げるのではなく、対象の性質が変わったという判断。

戻すことで得られるものがもう一つある。**id と label の名前空間が自然に分離する。**
grain-id は Crockford base32 なので `desktop` も `laptop1` も `keyfile` も 7 文字の正当な id として
パースでき、そのため `revoke` には「id 一致が label 一致より優先」という規則と、その規則を守らせる
テストが要った。UUID なら衝突しようがないので、規則ごと消せる（後述の 4 節）。

コストは、同じフィールドの破壊的変更を短期間に 2 度行うこと。移行手順は `ba6b12c` と同じものが
そのまま使える（後述の「移行」）。

### 採らなかった案

- **`label` を厳密化（文字種指定・一意）して id を置き換える。**
  文字種を絞ると「Alice の旧ノート、9 月に廃止」のような注釈が書けなくなり、`comment` を足したく
  なる。すると結局 2 フィールドに戻り、違いは同一性が人の手で書き換えられる可変値になったことだけ
  になる。加えて label は人が再利用する（revoke した `iPhone` を別の鍵に付け直す）ので、将来
  `Change` に書き込み元を記録したとき、過去のレコードが新しい持ち主を指してしまう。
- **`device_id` と鍵の id を同じ値にする。**
  一見すると紐づけを保存する場所が消えて良いが、`keys.toml` を直接編集する運用を残す以上、
  「device 表に無い id を名乗る鍵」の意味が不透明になる。id を分けておけば
  「device 行に紐づかない鍵」という well-defined な状態になり、`allow_unknown_device` で
  素直に扱える。またアプリのドメイン上の同一性が framework の生成物に従属してしまい、
  鍵を持たない device（同一ホスト上のものなど）を表現できない。
- **猶予期間つきの rotate**（旧トークンをしばらく生かす）。
  私設網・単一運用者という脅威モデルで「2 本目の生きた秘密」を持つ理由が薄い。必要になったら
  `previous_token` / `previous_expires_at` を同じエントリに足す形で後付けできる（id は 1 つのまま
  なので、ファイル形式の一意性制約と衝突しない）。

## 設計

### 1. `KeyStore::rotate`

```rust
/// `selector` の鍵の token だけを差し替える。id・label・created_at は保つ。
pub fn rotate(
    &mut self,
    prefix: &str,
    selector: &str,
    expires_at: Option<DateTime<Utc>>,
) -> Result<KeyEntry>;
```

- **`prefix` を引数に取る**のは、旧トークンから接頭辞を取り出す方法が無いため。ファイルは手書きの
  `token` を許しており（`<prefix>_<random>` 形式とは限らない）、`split_once('_')` が当てにならない。
  呼び出し側は自分の接頭辞を知っているので、渡してもらう方が確実。手書きのトークンは rotate を
  通ると `<prefix>_<random>` に正規化される。
- **`expires_at` は引数で置き換える**（`generate` と同じ形）。期限切れの鍵を期限そのままで再発行
  しても使えないので、保持ではなく指定にする。`None` は「無期限」。
- **旧トークンは即座に無効**。猶予期間は持たない。
- `KeyEntry` に `rotated_at: Option<DateTime<Utc>>` を足し、rotate 時に現在時刻を入れる。
  `created_at` は同一性の誕生日として据え置く。
- 保存は `generate` / `revoke` と同じ「複製を作る → `save_entries` → 成功したら代入」の順で行い、
  保存に失敗したらメモリ上の状態を変えない。
- **期限切れの鍵も rotate できる**。セレクタの解決は期限を見ない。新しい `expires_at` を渡せば
  そのまま復活する。期限切れは「消えた鍵」ではなく「止まっている鍵」であり、`revoke` するまで
  ファイルに残り続ける（既存のテスト `an_expired_key_does_not_authenticate` が示す挙動）以上、
  再開の手段が rotate なのは自然。

### 2. `generate` が id を受け取る

```rust
pub fn generate(
    &mut self,
    prefix: &str,
    id: Option<Uuid>,
    label: Option<String>,
    expires_at: Option<DateTime<Utc>>,
) -> Result<KeyEntry>;
```

- `None` なら `Uuid::new_v4()`。
- `Some(id)` が既に使われていたら `Error::KeyFile` で落とす。**空きを探して引き直してはならない**
  — 呼び出し側は特定の id を要求しているので、別の id を黙って返すのは要求に応えていない。
- 引数が 4 つになるので `NewKey { id, label, expires_at }` のような構造体も検討したが、
  型がすべて異なり取り違えはコンパイルエラーになるため、公開型を増やさない側を採る。

### 3. `KeyEntry::id` を `Uuid` に戻す

- `KeyEntry::id`, `Authenticated::key_id` の型を `uuid::Uuid` に。
- `remote-server` のクレートルートと `sapphire-framework::prelude` の再エクスポートを
  `GrainId` から `Uuid` に差し替える（アプリがバージョンを合わせた `uuid` 依存を自分で足さずに
  済むようにするため。`GrainId` を出していた理由がそのまま当てはまる）。`uuid` は change log 生成で
  既に依存にある。
- `fresh_id` の引き直しループは削除する。122 ビットなら確率に任せてよい。
- `load` の**重複 id 検出は残す**。手でエントリをコピペして複製する事故は起こりうるし、
  重複したまま読むと revoke がどちらの鍵か決められない。ただしエラーの性格は
  「よくある事故」から「まず起きない異常」に変わる。

### 4. セレクタの解決を一本化する

`revoke` と `rotate` が同じ解決規則を使うので、共通の内部関数に切り出す。

```rust
fn resolve(&self, selector: &str) -> Result<usize>;
```

規則は、UUID として parse できるなら id 照合、できないなら label 照合。**優先順位は不要**
（名前空間が重ならないため）。label が複数一致した場合は今まで通り「id を渡せ」と言って落とす。
一致 0 件はエラー。

これに伴い、`revoke_matches_an_id_before_a_label_that_looks_like_one` のテストは不要になる
（grain-id 特有の問題だったため）。

### 5. ファイル形式とヘッダコメント

`RawKey` に `rotated_at` を足す（省略可、`skip_serializing_if`）。`load` は補完しない
— 一度も rotate していない鍵に rotate 時刻は無い。

`HEADER` の書き換え:

- `id` の説明を「A UUID. Filled in on load when blank.」に。用途は
  「token を差し替えても label を変えても生き残る、鍵を人・デバイスに結び付けるための手掛かり」。
- `label` から **「Nothing in the system reads it.」を削る**。`revoke` / `rotate` のセレクタとして
  読まれるようになる。「A note for you, like an authorized_keys comment. Also accepted anywhere a
  command asks for a key.」といった書き方にする。
- `rotated_at` の行を足す。

### 6. アプリ側（framework 外・参考）

本 spec の実装対象ではないが、上の設計はこの運用を前提にしているので記録しておく。

- **`add-device`** は次の順で書く: UUID をミント → device 行（`key_id` を含む）を書く →
  その id を渡して `KeyStore::generate`。この順序なのは、途中で失敗したときに残るのが
  「鍵の無い device 行」（繋がらないだけで無害）になるから。逆順だと「動くオーファン鍵」が残る。
- **`remove-device`** は device 行と鍵の両方を落とす。
- **`allow_unknown_device: bool`（既定 `true`）**。認証は通ったが device 表に無い `key_id` を
  通すかどうか。既定を `true` にするのは、それが現在の framework の契約（鍵ファイルにあれば通る）
  であり、厳しい側をオプトインにすれば変更が加算的になって誰も静かに締め出されないため。
  `false` で拒否したときは `key_id` と `label` をログに出す — トークンは正しいのに拒否される
  状況は、手掛かりが無いと極めて分かりにくい。

## テスト

`keys.rs` の既存テストの様式（`tempfile` + `KeyStore::load`）に合わせる。

**rotate**
- id・label・`created_at` が保たれ、token が変わり、`rotated_at` が入る
- rotate 後、**旧トークンでは authenticate できない**／新トークンではできる
- label をセレクタにして rotate できる
- 一致 0 件はエラー
- 保存に失敗したらメモリ上の状態を変えない（`generate_does_not_mutate_state_when_save_fails` と同型）
- 手書きの（`_` を含まない）トークンが `<prefix>_<random>` に正規化される

**generate**
- 渡した id がそのまま使われる
- 既に使われている id を渡すとエラーになり、エントリが増えない
- id を渡さなければ新しい UUID が入る

**load / 形式**
- `id` 未記入のエントリに UUID が補完され、書き戻される
- 重複 id を持つファイルは落ちる
- `rotated_at` が往復する（読み込み → 保存 → 再読み込みで保たれる）
- ヘッダが `rotated_at` に触れており、label を「何も読まない」と書いていない

**セレクタ**
- UUID 文字列は id に、それ以外は label に当たる
- label が複数一致したらエラー（`revoke` / `rotate` 両方）

## 移行

`ba6b12c` と同じ性質の破壊的変更になる。既存の `keys.toml` は `id` が grain-id なので読めなくなる。
手順も同じ: **`id = ...` の行を削って再読み込みし、補完させ直す**（あるいはファイルごと捨てる）。

id が変わるので、その id を参照している device 行があれば壊れる。ただし device 表はこの設計で
これから作るものなので、実際に困る利用者は今のところ居ない。

`GrainId` の再エクスポートが消えるため、`Authenticated::key_id` に触れているアプリはコンパイルが
通らなくなる。BREAKING CHANGE として記述する。

## スコープ外

- 猶予期間つき rotate（2 本の生きたトークン）
- 鍵ごとの権限差（読み取り専用鍵など）
- 各アプリの `add-device` / `allow_unknown_device` の実装そのもの（アプリ側の spec で扱う）
- `Change` への書き込み元の記録。ただし本 spec の id は、それが来たときにそのまま使える性質
  （安定・再利用されない）を持つように選んである
- TLS / OAuth
