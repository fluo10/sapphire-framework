//! ユーザー台帳（`.{app_name}/users.toml`）。
//!
//! 「最終更新者が人間か AI か」を表示するための逆引き先。ID はコンテンツには
//! 焼かれない（フロントマターに入るのは `device_id`）が、`devices.toml` の
//! `user_id` から参照される。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::store::write_atomic;

/// 保存時に毎回書き出す書式説明。
const HEADER: &str = "\
# sapphire users.
#
# One `[[user]]` table per person or agent. Hand-editing is fine: a table
# with just a `name` is a valid entry — the remaining fields are filled in
# and written back the next time this file is loaded.
#
# id          optional. A grain-id. Filled in on load when blank. Referred
#             to by `user_id` in devices.toml. Ids must be unique here.
# name        required. Unique within this file. Accepted in place of the
#             id anywhere a command asks for a user. A selector is matched
#             against this name first; if no name matches, the selector is
#             parsed as a grain-id and matched against ids. Consequently, if
#             a user's name is literally another user's id string, the name
#             takes precedence.
# description optional. A note for you.
# created_at  optional. RFC 3339. Filled in on load when blank.
# retired_at  optional. RFC 3339. Set when the user is retired. The entry
#             stays so historical references still resolve; only an explicit
#             purge removes it.
#
# This file is rewritten in full on every change; comments you add are lost.
";

/// 一人のユーザー。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: GrainId,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub retired_at: Option<DateTime<Utc>>,
}

impl User {
    /// 引退済みか。認証の可否には使わない（それは鍵ファイルの仕事）。
    pub fn is_retired(&self) -> bool {
        self.retired_at.is_some()
    }
}

/// ファイル上の表現。手書きを許すため `id` / `created_at` は省略可。
#[derive(Debug, Serialize, Deserialize)]
struct RawUser {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<GrainId>,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RawFile {
    #[serde(default)]
    user: Vec<RawUser>,
}

/// ユーザー台帳ファイルとその中身。
#[derive(Debug)]
pub struct Users {
    path: PathBuf,
    entries: Vec<User>,
}

impl Users {
    /// 読み込み、欠けた `id` / `created_at` を補完する。補完があれば書き戻す。
    /// ファイルが無い場合は空の台帳を返す（作成はしない）。
    pub fn load(path: &Path) -> Result<Self> {
        let raw: RawFile = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::File(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RawFile::default(),
            Err(e) => return Err(Error::Io(e)),
        };

        // 重複したまま読み込むと resolve がどちらか決められない。エントリごと
        // コピーして複製する事故は実際に起きる。
        let mut seen_ids: HashSet<GrainId> = HashSet::new();
        let mut seen_names: HashSet<&str> = HashSet::new();
        for u in &raw.user {
            if let Some(id) = u.id
                && !seen_ids.insert(id)
            {
                return Err(Error::File(format!(
                    "{}: two users share the id {id}",
                    path.display()
                )));
            }
            if !seen_names.insert(u.name.as_str()) {
                return Err(Error::File(format!(
                    "{}: two users share the name {:?}",
                    path.display(),
                    u.name
                )));
            }
        }

        let mut filled = false;
        let now = Utc::now();
        let mut entries: Vec<User> = Vec::with_capacity(raw.user.len());
        for u in raw.user {
            if u.id.is_none() || u.created_at.is_none() {
                filled = true;
            }
            entries.push(User {
                id: u.id.unwrap_or_else(GrainId::random),
                name: u.name,
                description: u.description,
                created_at: u.created_at.unwrap_or(now),
                retired_at: u.retired_at,
            });
        }

        let store = Self {
            path: path.to_path_buf(),
            entries,
        };
        if filled {
            store.save()?;
        }
        Ok(store)
    }

    pub fn entries(&self) -> &[User] {
        &self.entries
    }

    /// 新しいユーザーを追加して保存する。`name` の重複は拒否する。
    pub fn add(&mut self, name: &str, description: Option<String>) -> Result<User> {
        if self.entries.iter().any(|u| u.name == name) {
            return Err(Error::File(format!("a user named {name:?} already exists")));
        }
        let entry = User {
            id: GrainId::random(),
            name: name.to_owned(),
            description,
            created_at: Utc::now(),
            retired_at: None,
        };
        let mut candidate = self.entries.clone();
        candidate.push(entry.clone());
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(entry)
    }

    pub fn get(&self, id: GrainId) -> Option<&User> {
        self.entries.iter().find(|u| u.id == id)
    }

    /// `selector` を 1 件のエントリの位置に解決する。
    ///
    /// 名前を先に試す。ユーザー名が grain-id として読める可能性を考慮すると
    /// 名前を優先する方が安全。名前に一致するエントリがあれば、それを返す。
    /// なければ grain-id として読めるか試す — 読めたら id で探す。
    ///
    /// 名前も id もファイル内で一意なので、複数一致は起こらない。
    /// 名前が偶然 grain-id として読めてしまう場合は名前側が優先される —
    /// 誤ったユーザーに当たることはないが、id で強制する逃げ道は無い。
    /// `KeyStore::resolve` は UUID で似た制約を持つが、UUID は 32 文字なので
    /// 衝突の確率がはるかに低い。
    fn index_of(&self, selector: &str) -> Result<usize> {
        // 名前を先に試す
        if let Some(pos) = self.entries.iter().position(|u| u.name == selector) {
            return Ok(pos);
        }
        // 名前に一致しなければ、grain-id として読めるか試す
        if let Ok(id) = selector.parse::<GrainId>()
            && let Some(pos) = self.entries.iter().position(|u| u.id == id)
        {
            return Ok(pos);
        }
        Err(Error::File(format!("no user matches {selector:?}")))
    }

    pub fn resolve(&self, selector: &str) -> Result<&User> {
        Ok(&self.entries[self.index_of(selector)?])
    }

    /// 引退させる。エントリは残るので、過去の参照は解決し続ける。
    /// 既に引退済みなら `retired_at` は上書きしない。
    pub fn retire(&mut self, selector: &str) -> Result<User> {
        let i = self.index_of(selector)?;
        let mut candidate = self.entries.clone();
        if candidate[i].retired_at.is_none() {
            candidate[i].retired_at = Some(Utc::now());
        }
        let retired = candidate[i].clone();
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(retired)
    }

    /// 本当に削除する。過去の参照は解決できなくなる。
    pub fn purge(&mut self, selector: &str) -> Result<User> {
        let i = self.index_of(selector)?;
        let mut candidate = self.entries.clone();
        let removed = candidate.remove(i);
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(removed)
    }

    fn save(&self) -> Result<()> {
        self.save_entries(&self.entries)
    }

    /// `entries` をヘッダ付きで全上書きする。`self.entries` には触れない —
    /// 呼び出し側は保存が成功してから代入すること。
    fn save_entries(&self, entries: &[User]) -> Result<()> {
        let raw = RawFile {
            user: entries
                .iter()
                .map(|u| RawUser {
                    id: Some(u.id),
                    name: u.name.clone(),
                    description: u.description.clone(),
                    created_at: Some(u.created_at),
                    retired_at: u.retired_at,
                })
                .collect(),
        };
        let body = toml::to_string_pretty(&raw)
            .map_err(|e| Error::File(format!("serializing users: {e}")))?;
        write_atomic(&self.path, HEADER, &body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.toml");
        (dir, path)
    }

    #[test]
    fn add_then_reload_round_trips() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("fluo10", Some("人間".into())).unwrap();

        let reloaded = Users::load(&path).unwrap();

        assert_eq!(reloaded.entries(), &[added]);
    }

    #[test]
    fn a_missing_file_loads_as_empty_and_is_not_created() {
        let (_d, path) = tmp();
        let users = Users::load(&path).unwrap();
        assert!(users.entries().is_empty());
        assert!(!path.exists(), "load はファイルを作らない");
    }

    #[test]
    fn load_fills_in_a_hand_written_entry_and_writes_it_back() {
        let (_d, path) = tmp();
        // ヘッダが案内する通り、name だけの手書きエントリ。
        std::fs::write(&path, "[[user]]\nname = \"fluo10\"\n").unwrap();

        let users = Users::load(&path).unwrap();

        assert_eq!(users.entries().len(), 1);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("id = "), "補完した id が書き戻されていない: {text}");
        assert!(text.contains("created_at = "), "{text}");
    }

    #[test]
    fn load_rejects_two_users_sharing_an_id() {
        let (_d, path) = tmp();
        std::fs::write(
            &path,
            "[[user]]\nid = \"a3f9k2p\"\nname = \"a\"\n\n\
             [[user]]\nid = \"a3f9k2p\"\nname = \"b\"\n",
        )
        .unwrap();

        let err = Users::load(&path).unwrap_err();

        assert!(err.to_string().contains("a3f9k2p"), "{err}");
    }

    #[test]
    fn load_rejects_two_users_sharing_a_name() {
        let (_d, path) = tmp();
        std::fs::write(&path, "[[user]]\nname = \"dup\"\n\n[[user]]\nname = \"dup\"\n").unwrap();

        let err = Users::load(&path).unwrap_err();

        assert!(err.to_string().contains("dup"), "{err}");
    }

    #[test]
    fn add_rejects_a_duplicate_name() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        users.add("fluo10", None).unwrap();

        let err = users.add("fluo10", None).unwrap_err();

        assert!(err.to_string().contains("fluo10"), "{err}");
    }

    #[test]
    fn resolve_finds_by_id_and_by_name() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("fluo10", None).unwrap();

        assert_eq!(users.resolve("fluo10").unwrap(), &added);
        assert_eq!(users.resolve(&added.id.to_string()).unwrap(), &added);
    }

    #[test]
    fn resolve_errors_on_no_match() {
        let (_d, path) = tmp();
        let users = Users::load(&path).unwrap();
        assert!(users.resolve("nobody").is_err());
    }

    #[test]
    fn retire_keeps_the_entry_resolvable() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("gone", None).unwrap();

        let retired = users.retire("gone").unwrap();

        assert!(retired.retired_at.is_some());
        // 履歴の解決のために残る、というのが retire の要点。
        assert_eq!(users.get(added.id).map(|u| u.id), Some(added.id));
        let reloaded = Users::load(&path).unwrap();
        assert!(reloaded.entries()[0].retired_at.is_some());
    }

    #[test]
    fn purge_removes_the_entry() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        users.add("gone", None).unwrap();

        users.purge("gone").unwrap();

        assert!(Users::load(&path).unwrap().entries().is_empty());
    }

    #[test]
    fn the_header_documents_every_field() {
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        users.add("fluo10", None).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();

        for field in ["id", "name", "description", "created_at", "retired_at"] {
            assert!(
                text.contains(&format!("# {field}")),
                "ヘッダが {field} を説明していない: {text}"
            );
        }
    }

    #[test]
    fn a_name_that_parses_as_a_grain_id_still_resolves_as_a_name() {
        // ユーザー名が grain-id として読める場合、名前が優先される。
        // 例えば "abcd" のような短い名前は Crockford base32 として読める。
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let added = users.add("abcd", None).unwrap();

        // "abcd" は grain-id として読める（"abcd".parse::<GrainId>() は Ok）
        // だが、resolve("abcd") は名前で一致すべき。
        assert_eq!(users.resolve("abcd").unwrap(), &added);
        // id でも解決できる
        assert_eq!(users.resolve(&added.id.to_string()).unwrap(), &added);
    }

    #[test]
    fn a_user_name_matching_another_user_id_resolves_by_name() {
        // ユーザーの名前が別のユーザーの id 文字列と同じ場合、名前が優先される。
        let (_d, path) = tmp();
        let mut users = Users::load(&path).unwrap();
        let first = users.add("user1", None).unwrap();
        // second の名前を first の id にする
        let second = users.add(&first.id.to_string(), None).unwrap();

        // resolve(first.id) は second ユーザー（名前が id に等しい）を返す
        assert_eq!(users.resolve(&first.id.to_string()).unwrap(), &second);
        // first を見つけるには "user1" で探す
        assert_eq!(users.resolve("user1").unwrap(), &first);
    }
}
