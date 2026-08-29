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
