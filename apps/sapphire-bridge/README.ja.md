# sapphire-bridge（日本語）

ホスト単位で常駐する sapphire のデーモンです。OS ユーザーごとに 1 プロセスで、そのマシン上の
すべての sapphire アプリに共有されます。

> English: [README.md](README.md)

## これは何か

各アプリはそれぞれ自分のアプリサーバを動かし、そのサーバがワークスペースのキャッシュを所有します
（1 プロセスしか開けないファイルです）。bridge はその上に位置します。このホストのデバイス識別を持ち、
ホストがどの workgroup に属しているかを把握し、アプリサーバにピアの居場所を伝えます。

bridge はワークスペースの中身を一切見ません。所有しているアプリサーバへ *ルーティング* し、
ピアのバイト列が流れてよい *かどうか* を決めるだけで、その意味には関与しません。

## 動かす

```console
$ sapphire-bridge            # `sapphire-bridge run` と同じ
$ sapphire-bridge status
$ sapphire-bridge workgroup create home --device-name laptop
$ sapphire-bridge device list
```

サブコマンドを付けずに実行すると、bridge はフォアグラウンドで起動し続けます。それ以外の
サブコマンドは起動中の bridge に対する単発コマンドです。例外は `workgroup create` と
`device forget` で、これらは bridge ディレクトリを直接操作します（前者はまだ問い合わせる先が
無いため、後者は制御プレーンにメソッドが無いため）。

2 つ目の bridge を起動してもエラーにはなりません。すでに動いているものの pid を報告し、
非ゼロで終了します。

## コマンド

| コマンド | 内容 |
|---|---|
| `run`（デフォルト） | bridge をフォアグラウンドで実行 |
| `status` | 起動中の bridge のバージョン・node id・workgroup・登録済みワークスペースを表示 |
| `device list` | workgroup のデバイス一覧と、到達可能かどうかを表示 |
| `device forget <selector>` | 名前または id でデバイスを退役させる |
| `workgroup create <name> --device-name <name>` | workgroup を作成し、このホストを最初のデバイスとして記録 |
| `workgroup list` | このホストが属する workgroup を表示 |
| `workspace list` | このホストが提供するワークスペース一覧（読み取り専用） |

`pair`・`workgroup join`・招待は未実装です。

## 保存場所

`<プラットフォームのデータルート>/sapphire/bridge/`（`SAPPHIRE_BRIDGE_DIR` で全体を上書き可）:

```
bridge/
    format          # ディレクトリのフォーマットバージョン
    node.key        # iroh の秘密鍵 -> このデバイスの node id
    bridge.lock     # 単一インスタンス用のガード。役割選出ではない
    net.toml        # discovery・relay・wake_on_sync
    routes.toml     # workspace_id -> 所有するアプリサーバ
    workgroups/<workgroup-id>/
        root/       # デバイス台帳
        root/devices/<device-id>.toml
```

Unix ではディレクトリは `0700`、`node.key` は `0600` です。このファイルはこのデバイスの
識別そのものです。

## 設定

`net.toml`。どのフィールドも省略可能です:

```toml
wake_on_sync = true    # ピアがワークスペースを求めたとき、停止中のアプリサーバを起動する
discovery = true       # discovery サービスでピアを見つける
relays = []            # relay URL。空にすると relay を使わない
```

ログのフィルタはフレームワークの他と同じく `RUST_LOG` で設定します。

## ライセンス

MIT OR Apache-2.0。
