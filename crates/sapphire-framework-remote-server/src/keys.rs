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
                .map_err(|e| Error::KeyFile(format!("{}: {e}", path.display())))?,
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

        let store = Self {
            path: path.to_path_buf(),
            entries,
        };
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
            .map_err(|e| Error::KeyFile(format!("no randomness available: {e}")))?;
        let random = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);

        let entry = KeyEntry {
            token: format!("{prefix}_{random}"),
            id: Uuid::new_v4(),
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
            [] => Err(Error::KeyFile(format!("no key matches {selector:?}"))),
            [i] => {
                let mut candidate = self.entries.clone();
                let removed = candidate.remove(*i);
                self.save_entries(&candidate)?;
                self.entries = candidate;
                Ok(removed)
            }
            many => Err(Error::KeyFile(format!(
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

    /// 現在の `self.entries` をヘッダ付きで全上書きする。
    fn save(&self) -> Result<()> {
        self.save_entries(&self.entries)
    }

    /// `entries` をヘッダ付きで全上書きする。`self.entries` には触れない — 呼び出し側
    /// は保存が成功してから代入すること。こうしておけば、ディスクフル等で保存が
    /// 失敗しても、メモリ上の状態とファイルの中身がずれない。
    fn save_entries(&self, entries: &[KeyEntry]) -> Result<()> {
        let raw = RawFile {
            key: entries
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
            .map_err(|e| Error::KeyFile(format!("serializing keys: {e}")))?;
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
        std::fs::write(
            &p,
            "[[key]]\ntoken = \"sjt_hand\"\nlabel = \"typed by hand\"\n",
        )
        .unwrap();

        let store = KeyStore::load(&p).unwrap();

        assert_eq!(store.entries().len(), 1);
        assert_eq!(
            store.entries()[0].id.get_version_num(),
            4,
            "鍵の id は UUIDv4"
        );
        assert_eq!(store.entries()[0].label.as_deref(), Some("typed by hand"));

        // 補完がファイルへ書き戻されていること
        let reloaded = KeyStore::load(&p).unwrap();
        assert_eq!(reloaded.entries()[0].id, store.entries()[0].id);
        assert_eq!(
            reloaded.entries()[0].created_at,
            store.entries()[0].created_at
        );
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
    fn generate_does_not_mutate_state_when_save_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let first = store.generate("sjt", Some("keeper".into()), None).unwrap();

        // Point the store at a path whose parent cannot be created: `afile`
        // already exists as a regular file, so `create_dir_all` on it fails.
        let blocker = tmp.path().join("afile");
        std::fs::write(&blocker, "not a directory").unwrap();
        store.path = blocker.join("keys.toml");

        let before = store.entries().to_vec();
        assert!(store.generate("sjt", Some("doomed".into()), None).is_err());
        assert_eq!(
            store.entries(),
            before.as_slice(),
            "a failed save must not mutate in-memory state"
        );
        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].id, first.id);
    }

    #[test]
    fn revoke_does_not_mutate_state_when_save_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let entry = store.generate("sjt", Some("keeper".into()), None).unwrap();

        let blocker = tmp.path().join("afile");
        std::fs::write(&blocker, "not a directory").unwrap();
        store.path = blocker.join("keys.toml");

        let before = store.entries().to_vec();
        assert!(store.revoke("keeper").is_err());
        assert_eq!(
            store.entries(),
            before.as_slice(),
            "a failed save must not mutate in-memory state"
        );
        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].id, entry.id);
    }

    #[test]
    fn has_usable_key_ignores_expired_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = KeyStore::load(&path(&tmp)).unwrap();
        let past = Utc::now() - Duration::hours(1);

        store.generate("sjt", None, Some(past)).unwrap();
        assert!(!store.has_usable_key(), "only an expired key exists");

        store.generate("sjt", None, None).unwrap();
        assert!(store.has_usable_key(), "a live key now exists");
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
