//! アプリごとのデバイス／ユーザー台帳。
//!
//! `.{app_name}/devices.toml` と `.{app_name}/users.toml` を読み書きする。
//! ID はアプリの中だけで意味を持ち、アプリ間で共有しない — 各アプリは互いに
//! MCP などの API 越しに 1 つのクライアントデバイスとして映る。
//!
//! パスの規約は `sapphire-framework-workspace` の `Workspace::devices_path` /
//! `users_path` が持つ。このクレートは `&Path` を受け取るだけで、ワークスペースの
//! 解決には関わらない。

mod devices;
mod error;
mod store;
mod users;

pub use devices::{Device, Devices};
pub use error::{Error, Result};
pub use users::{User, Users};
// `Device::id` / `Device::user_id` / `Devices::add` などの型。アプリが
// grain-id を自前で依存に足さなくても名指しできるように出しておく。
pub use grain_id::GrainId;
