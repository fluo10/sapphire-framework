//! The device ledger (`.<app_name>/devices.toml`).
//!
//! A device's `id` is **persisted into content** — a journal entry's frontmatter
//! `updated_by` points at it. So removing a device from the ledger is a tombstone
//! (`retired_at`) by default; only an explicit `purge` deletes it physically.
//!
//! Ids mean nothing outside this app. The same physical device may sit in another
//! app's ledger under a different id.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use grain_id::GrainId;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::store::write_atomic;

/// The format description written out on every save.
const HEADER: &str = "\
# sapphire devices.
#
# One `[[device]]` table per client device. Hand-editing is fine: a table
# with just a `name` is a valid entry — the remaining fields are filled in
# and written back the next time this file is loaded.
#
# id          optional. A grain-id. Filled in on load when blank. This is
#             the id that gets written into content (a journal entry's
#             `updated_by`, say). Normalized to canonical form on load —
#             grain-id's decode table aliases i/l to 1, o to 0, u to v, and
#             accepts uppercase, so a hand-written id of DESKTOP loads fine
#             but is written back in its canonical spelling. The id it
#             decodes to stays stable; the exact string you typed might not.
#             Ids must be unique within this file.
# name        required. Unique within this file. Accepted in place of the
#             id anywhere a command asks for a device. A selector is matched
#             against this name first; if no name matches, the selector is
#             parsed as a grain-id and matched against ids. Consequently, if
#             a device's name is literally another device's id string, the
#             name takes precedence.
# node_id     optional. The device's iroh node id: 64 lowercase hex digits.
#             Filled in when the device pairs. Unique within this ledger.
# description optional. A note for you.
# created_at  optional. RFC 3339. Filled in on load when blank.
# retired_at  optional. RFC 3339. Set when the device is retired. The entry
#             stays so historical references still resolve; only an explicit
#             purge removes it. Revoking access is a separate job, done in
#             the server's own key file.
#
# This file is rewritten in full on every change; comments you add are lost.
# File permissions are reset on every save too (the file is recreated and
# renamed into place), so a hand-set chmod does not survive a save. Harmless
# here — this file holds no secrets — but worth knowing.
";

/// One device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// Stable id, written into synced content as `Entry.author`.
    pub id: GrainId,
    /// Human-chosen name, unique within the ledger.
    pub name: String,
    /// The device's iroh node id, 64 lowercase hex characters, once it has one.
    ///
    /// `None` for a record written before the device announced itself: the founding device
    /// of a workgroup, or a record a user added by hand.
    pub node_id: Option<String>,
    /// A note for the user; the system never reads it.
    pub description: Option<String>,
    /// When the record was created.
    pub created_at: DateTime<Utc>,
    /// When the device was retired, if it was.
    pub retired_at: Option<DateTime<Utc>>,
}

impl Device {
    /// Whether this device is retired. Not an authorization check — that is the
    /// key file's job.
    pub fn is_retired(&self) -> bool {
        self.retired_at.is_some()
    }
}

/// The on-file representation. `id` / `created_at` are optional so that a record
/// can be hand-written.
#[derive(Debug, Serialize, Deserialize)]
struct RawDevice {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<GrainId>,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
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
    device: Vec<RawDevice>,
}

/// The device ledger file and its contents.
///
/// Holds the snapshot taken when it was loaded. Every later `add` / `retire` /
/// `purge` appends to that snapshot and rewrites the whole file, so a change that
/// reached the file after the load (a hand edit, as the HEADER invites, or a sync
/// from another host) is invisible to this instance and is silently overwritten by
/// the next mutation. Where the file may have changed — anything but a
/// just-started process, a long-lived one especially — load it again before
/// mutating.
#[derive(Debug)]
pub struct Devices {
    path: PathBuf,
    entries: Vec<Device>,
}

impl Devices {
    /// Read the file, filling in a missing `id` / `created_at`. Writes back if
    /// anything was filled in. A missing file is an empty ledger; it is not
    /// created here.
    pub fn load(path: &Path) -> Result<Self> {
        let raw: RawFile = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::File(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RawFile::default(),
            Err(e) => return Err(Error::Io(e)),
        };

        // Loading a duplicate would leave `resolve` unable to pick one. Copying
        // entries one by one and duplicating an entry does happen in practice.
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
                node_id: d.node_id,
                description: d.description,
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

    /// Add a device. Rejects a duplicate `name` or `node_id`.
    pub fn add(
        &mut self,
        name: &str,
        node_id: Option<String>,
        description: Option<String>,
    ) -> Result<Device> {
        if self.entries.iter().any(|d| d.name == name) {
            return Err(Error::File(format!(
                "a device named {name:?} already exists"
            )));
        }
        if let Some(node) = node_id.as_deref()
            && let Some(existing) = self.by_node_id(node)
        {
            return Err(Error::File(format!(
                "node id {node} already belongs to the device {:?}",
                existing.name
            )));
        }
        let id = GrainId::random();
        if self.entries.iter().any(|d| d.id == id) {
            // Astronomically unlikely, but writing it anyway would make `load`
            // detect a duplicate id and refuse to read the whole file — a bricked
            // ledger. Ask the caller to `add` again rather than hunting for a free
            // id.
            return Err(Error::File(format!(
                "generated id {id} collides with an existing device; try again"
            )));
        }
        let entry = Device {
            id,
            name: name.to_owned(),
            node_id,
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

    pub fn get(&self, id: GrainId) -> Option<&Device> {
        self.entries.iter().find(|d| d.id == id)
    }

    /// The device with this node id, retired or not.
    ///
    /// Retired devices are included deliberately: a retired device and an unknown one are
    /// different things to the bridge, and only the caller can decide what to do with each.
    pub fn by_node_id(&self, node_id: &str) -> Option<&Device> {
        self.entries
            .iter()
            .find(|d| d.node_id.as_deref() == Some(node_id))
    }

    /// Give an existing device its node id.
    pub fn set_node_id(&mut self, selector: &str, node_id: String) -> Result<Device> {
        let i = self.index_of(selector)?;
        if let Some(existing) = self.by_node_id(&node_id)
            && existing.id != self.entries[i].id
        {
            return Err(Error::File(format!(
                "node id {node_id} already belongs to the device {:?}",
                existing.name
            )));
        }
        if self.entries[i].node_id.as_deref() == Some(node_id.as_str()) {
            return Ok(self.entries[i].clone());
        }
        let mut candidate = self.entries.clone();
        candidate[i].node_id = Some(node_id);
        let updated = candidate[i].clone();
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(updated)
    }

    /// Resolve `selector` to the position of one entry.
    ///
    /// Device names are usually 7-8 characters and often drawn from a subset of the
    /// Crockford base32 alphabet ("pendant", "speaker", "desktop"), so a name has a
    /// good chance of parsing as a grain-id. That is why a name wins: if an entry's
    /// name matches, return it; otherwise try to read the selector as a grain-id and
    /// look up by id.
    ///
    /// Names and ids are each unique in the file, so more than one match cannot
    /// happen. When a name happens to parse as a grain-id, the name wins — this
    /// never hits the wrong device, but there is no escape hatch to force an id.
    /// `KeyStore::resolve` has a similar limitation with UUIDs, though a UUID's 32
    /// characters make a collision far less likely.
    fn index_of(&self, selector: &str) -> Result<usize> {
        // Try the name first (a 7-8 character name very often reads as a grain-id).
        if let Some(pos) = self.entries.iter().position(|d| d.name == selector) {
            return Ok(pos);
        }
        // If no name matched, try reading the selector as a grain-id.
        if let Ok(id) = selector.parse::<GrainId>()
            && let Some(pos) = self.entries.iter().position(|d| d.id == id)
        {
            return Ok(pos);
        }
        Err(Error::File(format!("no device matches {selector:?}")))
    }

    pub fn resolve(&self, selector: &str) -> Result<&Device> {
        Ok(&self.entries[self.index_of(selector)?])
    }

    /// Retire a device. The entry stays, so a `device_id` baked into content keeps
    /// resolving. An already-retired entry keeps its `retired_at` and is not saved:
    /// this instance is the snapshot from `load`, so saving unconditionally here
    /// would trample changes that arrived from another host or a hand edit, all for
    /// an entry that did not change.
    pub fn retire(&mut self, selector: &str) -> Result<Device> {
        let i = self.index_of(selector)?;
        if self.entries[i].retired_at.is_some() {
            return Ok(self.entries[i].clone());
        }
        let mut candidate = self.entries.clone();
        candidate[i].retired_at = Some(Utc::now());
        let retired = candidate[i].clone();
        self.save_entries(&candidate)?;
        self.entries = candidate;
        Ok(retired)
    }

    /// Really delete a device. Past `updated_by` references stop resolving.
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

    /// Overwrite the whole file with `entries`, header first. Does not touch
    /// `self.entries` — the caller assigns only after the save succeeds.
    fn save_entries(&self, entries: &[Device]) -> Result<()> {
        let raw = RawFile {
            device: entries
                .iter()
                .map(|d| RawDevice {
                    id: Some(d.id),
                    name: d.name.clone(),
                    node_id: d.node_id.clone(),
                    description: d.description.clone(),
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
        let added = devices
            .add("pendant", None, Some("worn around the neck".into()))
            .unwrap();

        let reloaded = Devices::load(&path).unwrap();

        assert_eq!(reloaded.entries(), &[added]);
    }

    #[test]
    fn a_missing_file_loads_as_empty_and_is_not_created() {
        let (_d, path) = tmp();
        let devices = Devices::load(&path).unwrap();
        assert!(devices.entries().is_empty());
        assert!(!path.exists(), "load must not create the file");
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
        // device_id is baked into a journal entry's frontmatter, so retiring must
        // leave it resolvable.
        assert!(devices.get(added.id).is_some());
        let reloaded = Devices::load(&path).unwrap();
        assert!(reloaded.entries()[0].retired_at.is_some());
    }

    #[test]
    fn retire_does_not_resave_when_already_retired() {
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        devices.add("gone", None, None).unwrap();
        let first = devices.retire("gone").unwrap();

        // Mimic a change that reached the file after the load (a sync or a hand
        // edit; this `devices` instance knows nothing about it).
        let mut synced = std::fs::read_to_string(&path).unwrap();
        synced.push_str("\n# synced by another host\n");
        std::fs::write(&path, &synced).unwrap();

        let second = devices.retire("gone").unwrap();

        assert_eq!(second.retired_at, first.retired_at, "must not overwrite");
        // The early return must not re-save, or the synced line would be gone.
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.contains("synced by another host"),
            "retiring an already-retired device rewrote the file: {text}"
        );
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
            "node_id",
            "description",
            "created_at",
            "retired_at",
        ] {
            assert!(
                text.contains(&format!("# {field}")),
                "the header does not document {field}: {text}"
            );
        }
    }

    #[test]
    fn a_name_that_parses_as_a_grain_id_still_resolves_as_a_name() {
        // A 7-character device name is often made of characters Crockford base32
        // accepts, so it can read as a grain-id: "pendant", "speaker", "desktop".
        // The name-first rule makes the name match before the id.
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let name = "pendant";
        // The premise of this test: if `name` does not actually parse as a
        // grain-id, both rules (id first / name first) take the same branch and
        // this test guarantees nothing.
        assert!(
            name.parse::<GrainId>().is_ok(),
            "the premise that {name:?} parses as a grain-id no longer holds"
        );
        let added = devices.add(name, None, None).unwrap();

        // Thanks to the name-first rule, resolve(name) must match by name.
        assert_eq!(devices.resolve(name).unwrap(), &added);
        // The id resolves too.
        assert_eq!(devices.resolve(&added.id.to_string()).unwrap(), &added);
    }

    #[test]
    fn a_device_name_matching_another_device_id_resolves_by_name() {
        // When a device's name equals another device's id string, the name wins.
        let (_d, path) = tmp();
        let mut devices = Devices::load(&path).unwrap();
        let first = devices.add("device1", None, None).unwrap();
        // Give the second device the first one's id as its name.
        let second = devices.add(&first.id.to_string(), None, None).unwrap();

        // resolve(first.id) returns the second device (whose name equals that id).
        assert_eq!(devices.resolve(&first.id.to_string()).unwrap(), &second);
        // There is no way to reach `first` by id, only by another route — that is
        // this trade-off. "device1" does find it.
        assert_eq!(devices.resolve("device1").unwrap(), &first);
    }
}

#[cfg(test)]
mod user_removal_tests {
    use super::*;

    #[test]
    fn a_device_record_has_no_user_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("devices.toml");
        std::fs::write(&path, "[[device]]\nname = \"laptop\"\n").unwrap();

        let mut devices = Devices::load(&path).unwrap();
        devices.add("desktop", None, None).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("user_id"), "a user_id survived:\n{text}");
    }

    #[test]
    fn a_hand_written_user_id_is_ignored_rather_than_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("devices.toml");
        std::fs::write(
            &path,
            "[[device]]\nname = \"laptop\"\nuser_id = \"abcdef\"\n",
        )
        .unwrap();

        let devices = Devices::load(&path).unwrap();
        assert_eq!(devices.entries().len(), 1);
        assert_eq!(devices.entries()[0].name, "laptop");
    }
}

#[cfg(test)]
mod node_id_tests {
    use super::*;

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn ledger(dir: &std::path::Path) -> Devices {
        Devices::load(&dir.join("devices.toml")).unwrap()
    }

    #[test]
    fn a_node_id_round_trips_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        let added = devices
            .add("laptop", Some(NODE_A.to_owned()), None)
            .unwrap();
        assert_eq!(added.node_id.as_deref(), Some(NODE_A));

        let reloaded = ledger(dir.path());
        assert_eq!(reloaded.entries()[0].node_id.as_deref(), Some(NODE_A));
    }

    #[test]
    fn a_device_can_be_found_by_its_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices
            .add("laptop", Some(NODE_A.to_owned()), None)
            .unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();

        assert_eq!(devices.by_node_id(NODE_A).unwrap().name, "laptop");
        assert_eq!(devices.by_node_id(NODE_B).unwrap().name, "phone");
        assert!(devices.by_node_id("deadbeef").is_none());
    }

    #[test]
    fn a_record_without_a_node_id_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("devices.toml");
        std::fs::write(&path, "[[device]]\nname = \"laptop\"\n").unwrap();

        let devices = Devices::load(&path).unwrap();
        assert!(devices.entries()[0].node_id.is_none());
    }

    #[test]
    fn a_node_id_can_be_set_later() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices.add("laptop", None, None).unwrap();

        let updated = devices.set_node_id("laptop", NODE_A.to_owned()).unwrap();
        assert_eq!(updated.node_id.as_deref(), Some(NODE_A));
        assert_eq!(
            ledger(dir.path()).by_node_id(NODE_A).unwrap().name,
            "laptop"
        );
    }

    #[test]
    fn two_devices_cannot_share_a_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices
            .add("laptop", Some(NODE_A.to_owned()), None)
            .unwrap();

        let err = devices
            .add("phone", Some(NODE_A.to_owned()), None)
            .unwrap_err();
        assert!(err.to_string().contains("node id"), "{err}");
    }

    #[test]
    fn a_retired_device_keeps_its_node_id_and_is_still_found() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices
            .add("laptop", Some(NODE_A.to_owned()), None)
            .unwrap();
        devices.retire("laptop").unwrap();

        let found = devices
            .by_node_id(NODE_A)
            .expect("a retired device is still a record");
        assert!(
            found.is_retired(),
            "retirement is what authorization checks"
        );
    }
}
