//! デバイス台帳（`.{app_name}/devices.toml`）。
//!
//! `id` は**コンテンツに永続化される** — ジャーナルのフロントマターの
//! `updated_by` がこれを指し、表示時に `user_id` 経由で人間の名前へ逆引き
//! される。だから台帳からの削除は既定でトゥームストーン（`retired_at`）で、
//! 物理削除は `purge` を明示したときだけ。
//!
//! ID はこのアプリの中だけで意味を持つ。同じ物理デバイスが別のアプリの台帳に
//! 別の ID で載っていてよい。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::store::write_atomic;

/// 保存時に毎回書き出す書式説明。
const HEADER: &str = "\
# sapphire devices.
#
# One `[[device]]` table per client device. Hand-editing is fine: a table
# with just a `name` is a valid entry — the remaining fields are filled in
# and written back the next time this file is loaded.
#
# id          optional. A grain-id. Filled in on load when blank. This is
#             the id that gets written into content (a journal entry's
#             `updated_by`, say), so it must stay stable. Ids must be
#             unique within this file.
# name        required. Unique within this file. Accepted in place of the
#             id anywhere a command asks for a device. A name that happens
#             to parse as a grain-id is never matched as a name, only as
#             an id.
# description optional. A note for you.
# user_id     optional. A grain-id from users.toml — whose device this is.
# created_at  optional. RFC 3339. Filled in on load when blank.
# retired_at  optional. RFC 3339. Set when the device is retired. The entry
#             stays so historical references still resolve; only an explicit
#             purge removes it. Revoking access is a separate job, done in
#             the server's own key file.
#
# This file is rewritten in full on every change; comments you add are lost.
";

/// 一台のデバイス。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    pub id: GrainId,
    pub name: String,
    pub description: Option<String>,
    pub user_id: Option<GrainId>,
    pub created_at: DateTime<Utc>,
    pub retired_at: Option<DateTime<Utc>>,
}

impl Device {
    /// 引退済みか。認証の可否には使わない（それは鍵ファイルの仕事）。
    pub fn is_retired(&self) -> bool {
        self.retired_at.is_some()
    }
}

/// ファイル上の表現。手書きを許すため `id` / `created_at` は省略可。
#[derive(Debug, Serialize, Deserialize)]
struct RawDevice {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<GrainId>,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_id: Option<GrainId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RawFile {
    #[serde(default)]
    device: Vec<RawDevice>,
}

/// デバイス台帳ファイルとその中身。
#[derive(Debug)]
pub struct Devices {
    path: PathBuf,
    entries: Vec<Device>,
}

impl Devices {
    /// 読み込み、欠けた `id` / `created_at` を補完する。補完があれば書き戻す。
    /// ファイルが無い場合は空の台帳を返す（作成はしない）。
    pub fn load(path: &Path) -> Result<Self> {
        let raw: RawFile = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::File(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RawFile::default(),
            Err(e) => return Err(Error::Io(e)),
        };

        let mut seen_ids: HashSet<GrainId> = HashSet::new();
        let mut seen_names: HashSet<&str> = HashSet::new();
        for d in &raw.device {
            if let Some(id) = d.id
                && !seen_ids.insert(id)
            {
                return Err(Error::File(format!(
                    "{}: two devices share the id {id}",
                    path.display()
                )));
            }
            if !seen_names.insert(d.name.as_str()) {
                return Err(Error::File(format!(
                    "{}: two devices share the name {:?}",
                    path.display(),
                    d.name
                )));
            }
        }

        let mut filled = false;
        let now = Utc::now();
        let mut entries: Vec<Device> = Vec::with_capacity(raw.device.len());
        for d in raw.device {
            if d.id.is_none() || d.created_at.is_none() {
                filled = true;
            }
            entries.push(Device {
                id: d.id.unwrap_or_else(GrainId::random),
                name: d.name,
                description: d.description,
                user_id: d.user_id,
                created_at: d.created_at.unwrap_or(now),
                retired_at: d.retired_at,
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

    pub fn entries(&self) -> &[Device] {
        &self.entries
    }

    /// 新しいデバイスを追加して保存する。`name` の重複は拒否する。
    pub fn add(
        &mut self,
        name: &str,
        description: Option<String>,
        user_id: Option<GrainId>,
    ) -> Result<Device> {
        if self.entries.iter().any(|d| d.name == name) {
            return Err(Error::File(format!(
                "a device named {name:?} already exists"
            )));
        }
        let entry = Device {
            id: GrainId::random(),
            name: name.to_owned(),
            description,
            user_id,
            created_at: Utc::now(),
            retired_at: None,
        };
        let mut candidate = self.entries.clone();
        candidate.push(entry.clone());
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(entry)
    }

    pub fn get(&self, id: GrainId) -> Option<&Device> {
        self.entries.iter().find(|d| d.id == id)
    }

    /// `users.rs` の `index_of` と同じ規則 — grain-id として読めるなら id、
    /// 読めないなら name。
    fn index_of(&self, selector: &str) -> Result<usize> {
        let found = match selector.parse::<GrainId>() {
            Ok(id) => self.entries.iter().position(|d| d.id == id),
            Err(_) => self.entries.iter().position(|d| d.name == selector),
        };
        found.ok_or_else(|| Error::File(format!("no device matches {selector:?}")))
    }

    pub fn resolve(&self, selector: &str) -> Result<&Device> {
        Ok(&self.entries[self.index_of(selector)?])
    }

    /// 引退させる。エントリは残るので、コンテンツに焼かれた `device_id` は
    /// 解決し続ける。既に引退済みなら `retired_at` は上書きしない。
    pub fn retire(&mut self, selector: &str) -> Result<Device> {
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

    /// 本当に削除する。過去の `updated_by` は解決できなくなる。
    pub fn purge(&mut self, selector: &str) -> Result<Device> {
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

    fn save_entries(&self, entries: &[Device]) -> Result<()> {
        let raw = RawFile {
            device: entries
                .iter()
                .map(|d| RawDevice {
                    id: Some(d.id),
                    name: d.name.clone(),
                    description: d.description.clone(),
                    user_id: d.user_id,
                    created_at: Some(d.created_at),
                    retired_at: d.retired_at,
                })
                .collect(),
        };
        let body = toml::to_string_pretty(&raw)
            .map_err(|e| Error::File(format!("serializing devices: {e}")))?;
        write_atomic(&self.path, HEADER, &body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("devices.toml");
        (dir, path)
    }

    #[test]
    fn add_then_reload_round_trips() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let user = GrainId::random();
        let added = devices
            .add("pendant", Some("首から下げるやつ".into()), Some(user))
            .unwrap();

        let reloaded = Devices::load(&path).unwrap();

        assert_eq!(reloaded.entries(), &[added]);
        assert_eq!(reloaded.entries()[0].user_id, Some(user));
    }

    #[test]
    fn a_missing_file_loads_as_empty_and_is_not_created() {
        let (_d, path) = tmp();
        let devices = Devices::load(&path).unwrap();
        assert!(devices.entries().is_empty());
        assert!(!path.exists(), "load はファイルを作らない");
    }

    #[test]
    fn load_fills_in_a_hand_written_entry_and_writes_it_back() {
        let (_d, path) = tmp();
        std::fs::write(&path, "[[device]]\nname = \"pendant\"\n").unwrap();

        let devices = Devices::load(&path).unwrap();

        assert_eq!(devices.entries().len(), 1);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("id = "), "{text}");
        assert!(text.contains("created_at = "), "{text}");
    }

    #[test]
    fn load_rejects_two_devices_sharing_an_id() {
        let (_d, path) = tmp();
        std::fs::write(
            &path,
            "[[device]]\nid = \"a3f9k2p\"\nname = \"a\"\n\n\
             [[device]]\nid = \"a3f9k2p\"\nname = \"b\"\n",
        )
        .unwrap();

        let err = Devices::load(&path).unwrap_err();

        assert!(err.to_string().contains("a3f9k2p"), "{err}");
    }

    #[test]
    fn load_rejects_two_devices_sharing_a_name() {
        let (_d, path) = tmp();
        std::fs::write(
            &path,
            "[[device]]\nname = \"dup\"\n\n[[device]]\nname = \"dup\"\n",
        )
        .unwrap();

        let err = Devices::load(&path).unwrap_err();

        assert!(err.to_string().contains("dup"), "{err}");
    }

    #[test]
    fn add_rejects_a_duplicate_name() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("pendant", None, None).unwrap();

        let err = devices.add("pendant", None, None).unwrap_err();

        assert!(err.to_string().contains("pendant"), "{err}");
    }

    #[test]
    fn resolve_finds_by_id_and_by_name() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let added = devices.add("pendant", None, None).unwrap();

        assert_eq!(devices.resolve("pendant").unwrap(), &added);
        assert_eq!(devices.resolve(&added.id.to_string()).unwrap(), &added);
    }

    #[test]
    fn resolve_errors_on_no_match() {
        let (_d, path) = tmp();
        let devices = Devices::load(&path).unwrap();
        assert!(devices.resolve("nothing").is_err());
    }

    #[test]
    fn retire_keeps_the_entry_resolvable() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let added = devices.add("gone", None, None).unwrap();

        let retired = devices.retire("gone").unwrap();

        assert!(retired.retired_at.is_some());
        // device_id はジャーナルのフロントマターに焼かれるので、引退しても
        // 逆引きできなければならない。
        assert!(devices.get(added.id).is_some());
        let reloaded = Devices::load(&path).unwrap();
        assert!(reloaded.entries()[0].retired_at.is_some());
    }

    #[test]
    fn purge_removes_the_entry() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("gone", None, None).unwrap();

        devices.purge("gone").unwrap();

        assert!(Devices::load(&path).unwrap().entries().is_empty());
    }

    #[test]
    fn the_header_documents_every_field() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("pendant", None, None).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();

        for field in [
            "id",
            "name",
            "description",
            "user_id",
            "created_at",
            "retired_at",
        ] {
            assert!(
                text.contains(&format!("# {field}")),
                "ヘッダが {field} を説明していない: {text}"
            );
        }
    }
}
