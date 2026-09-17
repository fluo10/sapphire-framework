# Registry: one file per device, no users (`sapphire-framework-registry`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Turn the device ledger into what a synced workgroup needs — one TOML file per
device instead of one file holding all of them, an iroh `node_id` on each record, and no
users at all — with a migration from the current single-file format.

**Architecture:** `Devices` stops being "a parsed `devices.toml` plus a rewrite of the whole
file" and becomes "a directory of `<grain-id>.toml` files, each read and written on its own".
That is the point of the change: two devices that pair at the same moment on different hosts
write different files, so the sync layer sees two independent additions instead of one file
with a conflict. `users.rs` is deleted; a single user owns every device, so a `user_id` can
only ever be the same value (spec decision 3 of the sync spec).

**Tech Stack:** Rust 2024 (toolchain 1.98.0), serde + toml, grain-id 0.16, chrono, thiserror 2;
dev: tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` — §1 ("Changed",
`-registry`) and §3.5 (what a workgroup's device records hold). Implementation order step 2
of `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §9.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`). The registry
  crate's existing comments are in Japanese and grandfathered — but this plan rewrites those
  files, so **translate every comment you touch**, per the boy-scout rule in
  `CONTRIBUTING.md`.
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task.
- Crate version follows the workspace (`version.workspace = true`).
- Every public item carries a doc comment.
- A device record file is `<dir>/<grain-id>.toml`. The file name **is** the id; the id is not
  repeated inside the file.
- `node_id` is an iroh node id in its canonical string form (64 lowercase hex characters, the
  `NodeId`'s `Display`). This crate does **not** depend on iroh: it stores and compares the
  string. Validation is the bridge's job, which has the type.
- Device selectors follow the existing rule: match a `name` first; if none matches, parse the
  selector as a grain-id and match ids. A name that is literally another device's id string
  therefore wins — that behaviour is preserved.
- Retirement stays a tombstone (`retired_at`), never a deletion, because `Entry.author` in
  synced content refers to a device id forever.

## The compatibility hazard this format is chosen for

These files are **synced between devices that may run different framework versions**. Serde
drops fields it does not know about, so a device running an older build that rewrites a record
would silently strip a newer build's fields.

One file per record bounds that damage to the single record being changed, which is most of
why the format changed. Do not widen it: never rewrite a record the operation did not touch,
and never "tidy" the directory by rewriting every file. Task 4 has a test for this.

## File Structure

```
crates/sapphire-framework-registry/
    Cargo.toml
    src/
        lib.rs        # module wiring and re-exports (users removed)
        error.rs      # Error, Result (unchanged shape, comments translated)
        store.rs      # write_atomic (unchanged, comments translated)
        devices.rs    # Device, Devices — one file per record
        migrate.rs    # devices.toml -> <dir>/<grain-id>.toml
        users.rs      # DELETED
```

---

### Task 1: Delete users

**Files:**
- Delete: `crates/sapphire-framework-registry/src/users.rs`
- Modify: `crates/sapphire-framework-registry/src/lib.rs`
- Modify: `crates/sapphire-framework-registry/src/devices.rs` (drop `user_id`)
- Modify: `crates/sapphire-framework/src/lib.rs:107` (facade re-export)
- Modify: `crates/sapphire-framework-workspace/src/workspace.rs:104-106,293-304`
  (`users_path` and its test)

**Interfaces:**
- Produces: `Device { id, name, description, created_at, retired_at }` — `user_id` gone;
  `Devices::add(&mut self, name, description) -> Result<Device>` — the `user_id` argument gone
- Removed: `User`, `Users`, `Workspace::users_path`

**Why:** the sync spec's decision 3 — every device belongs to the same person, and a human and
an agent acting through one device cannot be told apart by a user id anyway. An app that wants
to record "written via MCP" does so itself.

- [ ] **Step 1: Write the failing test**

In `crates/sapphire-framework-registry/src/devices.rs`, replace any test that passes a
`user_id` and add:

```rust
#[cfg(test)]
mod user_removal_tests {
    use super::*;

    #[test]
    fn a_device_record_has_no_user_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("devices.toml");
        std::fs::write(&path, "[[device]]\nname = \"laptop\"\n").unwrap();

        let mut devices = Devices::load(&path).unwrap();
        devices.add("desktop", None).unwrap();

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
```

The second test matters: a ledger written by the previous version will contain `user_id`, and
refusing to load it would strand every existing workspace.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-registry --all-features`
Expected: FAIL — `Devices::add` still takes three arguments, so the file does not compile.

- [ ] **Step 3: Remove users**

```bash
git rm crates/sapphire-framework-registry/src/users.rs
```

`crates/sapphire-framework-registry/src/lib.rs` — replace the whole file (its comments were
Japanese; this is the translated version):

```rust
//! The device ledger for a sapphire workgroup.
//!
//! One TOML file per device, named by the device's grain-id. One file per record is what
//! lets two hosts pair at the same moment without colliding: each writes its own file, and
//! the sync layer sees two independent additions rather than one contested file.
//!
//! Path conventions belong to the caller. This crate takes a directory and works inside it.

mod devices;
mod error;
mod migrate;
mod store;

pub use devices::{Device, Devices};
pub use error::{Error, Result};
pub use migrate::migrate_single_file;
// Re-exported so an application can name `Device::id` without depending on grain-id itself.
pub use grain_id::GrainId;
```

In `devices.rs`:
- delete the `user_id` field from `Device` and from `RawDevice`;
- delete the `user_id` parameter from `Devices::add`;
- delete the `user_id` paragraph from the `HEADER` constant;
- translate every Japanese comment in the file to English as you go.

`crates/sapphire-framework/src/lib.rs:107`:

```rust
    pub use crate::registry::{Device, Devices, GrainId};
```

`crates/sapphire-framework-workspace/src/workspace.rs`: delete `users_path` and, in the test
`devices_and_users_sit_next_to_the_workspace_config`, delete the two `users_path` assertions
and rename it to `the_device_ledger_sits_next_to_the_workspace_config`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --all-features --locked`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add -A crates/sapphire-framework-registry crates/sapphire-framework crates/sapphire-framework-workspace
git commit -m "feat(registry)!: drop users; every device belongs to one person"
```

---

### Task 2: Add `node_id`

**Files:**
- Modify: `crates/sapphire-framework-registry/src/devices.rs`
- Test: inline `#[cfg(test)] mod tests` in `devices.rs`

**Interfaces:**
- Consumes: `Device`, `Devices` (Task 1)
- Produces:
  - `Device { id, name, node_id: Option<String>, description, created_at, retired_at }`
  - `Devices::add(&mut self, name: &str, node_id: Option<String>, description: Option<String>) -> Result<Device>`
  - `Devices::by_node_id(&self, node_id: &str) -> Option<&Device>`
  - `Devices::set_node_id(&mut self, selector: &str, node_id: String) -> Result<Device>`

`node_id` is `Option` because a record may be written before the device announces itself —
the founding device of a workgroup writes its own record, and a record hand-added by a user
has no node id until that device pairs.

- [ ] **Step 1: Write the failing tests**

```rust
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
        let added = devices.add("laptop", Some(NODE_A.to_owned()), None).unwrap();
        assert_eq!(added.node_id.as_deref(), Some(NODE_A));

        let reloaded = ledger(dir.path());
        assert_eq!(reloaded.entries()[0].node_id.as_deref(), Some(NODE_A));
    }

    #[test]
    fn a_device_can_be_found_by_its_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices.add("laptop", Some(NODE_A.to_owned()), None).unwrap();
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
        assert_eq!(ledger(dir.path()).by_node_id(NODE_A).unwrap().name, "laptop");
    }

    #[test]
    fn two_devices_cannot_share_a_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices.add("laptop", Some(NODE_A.to_owned()), None).unwrap();

        let err = devices.add("phone", Some(NODE_A.to_owned()), None).unwrap_err();
        assert!(err.to_string().contains("node id"), "{err}");
    }

    #[test]
    fn a_retired_device_keeps_its_node_id_and_is_still_found() {
        let dir = tempfile::tempdir().unwrap();
        let mut devices = ledger(dir.path());
        devices.add("laptop", Some(NODE_A.to_owned()), None).unwrap();
        devices.retire("laptop").unwrap();

        let found = devices.by_node_id(NODE_A).expect("a retired device is still a record");
        assert!(found.is_retired(), "retirement is what authorization checks");
    }
}
```

The last test states the rule the bridge depends on: `by_node_id` finds retired devices too,
and the caller checks `is_retired`. A lookup that hid them would make a retired device look
like an unknown one, and the two get different treatment (spec §3.5, revocation).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-registry --all-features node_id`
Expected: FAIL — `node_id` does not exist.

- [ ] **Step 3: Implement `node_id`**

In `devices.rs`:

```rust
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
```

Add the matching `node_id: Option<String>` field to `RawDevice`, with
`#[serde(default, skip_serializing_if = "Option::is_none")]`.

```rust
impl Devices {
    /// Add a device. Rejects a duplicate `name` or `node_id`.
    pub fn add(
        &mut self,
        name: &str,
        node_id: Option<String>,
        description: Option<String>,
    ) -> Result<Device> {
        if self.entries.iter().any(|d| d.name == name) {
            return Err(Error::File(format!("a device named {name:?} already exists")));
        }
        if let Some(node) = node_id.as_deref()
            && let Some(existing) = self.by_node_id(node)
        {
            return Err(Error::File(format!(
                "node id {node} already belongs to the device {:?}",
                existing.name
            )));
        }
        // … id generation and collision check as before …
    }

    /// The device with this node id, retired or not.
    ///
    /// Retired devices are included deliberately: a retired device and an unknown one are
    /// different things to the bridge, and only the caller can decide what to do with each.
    pub fn by_node_id(&self, node_id: &str) -> Option<&Device> {
        self.entries.iter().find(|d| d.node_id.as_deref() == Some(node_id))
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
}
```

Document `node_id` in the `HEADER` constant, next to the other fields:

```
# node_id     optional. The device's iroh node id: 64 lowercase hex digits.
#             Filled in when the device pairs. Unique within this ledger.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-registry --all-features`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-registry
git commit -m "feat(registry): record each device's iroh node id"
```

---

### Task 3: One file per record

**Files:**
- Modify: `crates/sapphire-framework-registry/src/devices.rs`
- Modify: `crates/sapphire-framework-registry/src/store.rs` (comments to English)
- Test: inline `#[cfg(test)] mod tests` in `devices.rs`

**Interfaces:**
- Consumes: everything from Tasks 1 and 2
- Produces:
  - `Devices::open(dir: &Path) -> Result<Devices>` — replaces `load(path)`. Reads every
    `*.toml` in `dir`; a missing directory is an empty ledger, and the directory is not created
    until something is written.
  - `Devices::dir(&self) -> &Path`
  - `Device::file_name(&self) -> String` — `<grain-id>.toml`
  - unchanged in shape: `entries`, `get`, `resolve`, `add`, `retire`, `purge`, `by_node_id`,
    `set_node_id`
- Removed: `Devices::load`

**The rule this task exists to enforce:** every mutation writes **exactly one file**, the one
whose record changed (`purge` removes exactly one). Nothing rewrites the directory.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod directory_tests {
    use super::*;

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn file_names(dir: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    #[test]
    fn a_missing_directory_is_an_empty_ledger_and_is_not_created() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        let devices = Devices::open(&dir).unwrap();
        assert!(devices.entries().is_empty());
        assert!(!dir.exists(), "opening must not create the directory");
    }

    #[test]
    fn each_device_gets_its_own_file_named_by_its_id() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        let mut devices = Devices::open(&dir).unwrap();

        let laptop = devices.add("laptop", Some(NODE_A.to_owned()), None).unwrap();
        let phone = devices.add("phone", None, None).unwrap();

        assert_eq!(
            file_names(&dir),
            {
                let mut want = vec![format!("{}.toml", laptop.id), format!("{}.toml", phone.id)];
                want.sort();
                want
            }
        );
    }

    #[test]
    fn the_id_is_the_file_name_and_is_not_repeated_inside() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        let mut devices = Devices::open(&dir).unwrap();
        let laptop = devices.add("laptop", None, None).unwrap();

        let text = std::fs::read_to_string(dir.join(format!("{}.toml", laptop.id))).unwrap();
        assert!(text.contains("name = \"laptop\""), "{text}");
        assert!(!text.contains(&laptop.id.to_string()), "the id is the file name:\n{text}");
    }

    #[test]
    fn records_reload_from_the_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        let mut devices = Devices::open(&dir).unwrap();
        devices.add("laptop", Some(NODE_A.to_owned()), None).unwrap();
        devices.add("phone", None, None).unwrap();

        let reloaded = Devices::open(&dir).unwrap();
        let mut names: Vec<&str> =
            reloaded.entries().iter().map(|d| d.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["laptop", "phone"]);
    }

    #[test]
    fn retiring_one_device_rewrites_only_that_devices_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        let mut devices = Devices::open(&dir).unwrap();
        let laptop = devices.add("laptop", None, None).unwrap();
        let phone = devices.add("phone", None, None).unwrap();

        // Put something unknown in the phone's file, as a newer build would.
        let phone_file = dir.join(format!("{}.toml", phone.id));
        let mut text = std::fs::read_to_string(&phone_file).unwrap();
        text.push_str("from_the_future = true\n");
        std::fs::write(&phone_file, &text).unwrap();

        devices.retire("laptop").unwrap();

        assert!(
            std::fs::read_to_string(&phone_file).unwrap().contains("from_the_future"),
            "an untouched record must not be rewritten"
        );
        let laptop_text =
            std::fs::read_to_string(dir.join(format!("{}.toml", laptop.id))).unwrap();
        assert!(laptop_text.contains("retired_at"), "{laptop_text}");
    }

    #[test]
    fn purging_removes_exactly_one_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        let mut devices = Devices::open(&dir).unwrap();
        let laptop = devices.add("laptop", None, None).unwrap();
        let phone = devices.add("phone", None, None).unwrap();

        devices.purge("laptop").unwrap();

        assert!(!dir.join(format!("{}.toml", laptop.id)).exists());
        assert!(dir.join(format!("{}.toml", phone.id)).exists());
        assert_eq!(devices.entries().len(), 1);
    }

    #[test]
    fn a_file_whose_name_is_not_a_grain_id_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("not-an-id!.toml"), "name = \"x\"\n").unwrap();

        let err = Devices::open(&dir).unwrap_err();
        assert!(err.to_string().contains("not-an-id!"), "{err}");
    }

    #[test]
    fn a_non_toml_file_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("devices");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("README.md"), "notes\n").unwrap();

        assert!(Devices::open(&dir).unwrap().entries().is_empty());
    }
}
```

The "not a grain-id" case is an error rather than something to skip: a file in this directory
that cannot be addressed is a device nobody can retire, and losing it silently is worse than
refusing to start.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-registry --all-features directory`
Expected: FAIL — `Devices::open` does not exist.

- [ ] **Step 3: Implement the directory layout**

Replace the storage half of `devices.rs`. The header comment shrinks, because it is now
repeated in every record file:

```rust
/// Written at the top of every record file.
const HEADER: &str = "\
# A sapphire device record. The file name is the device's id.
#
# name        required. Unique within the ledger. Accepted in place of the id
#             anywhere a command asks for a device.
# node_id     optional. The device's iroh node id: 64 lowercase hex digits.
#             Filled in when the device pairs. Unique within the ledger.
# description optional. A note for you; the system never reads it.
# created_at  optional. Filled in when the record is written.
# retired_at  optional. Set by `device forget`. The record stays, because
#             synced content refers to this device's id forever.
";

impl Device {
    /// The record's file name inside the ledger directory.
    pub fn file_name(&self) -> String {
        format!("{}.toml", self.id)
    }
}

impl Devices {
    /// Read every record in `dir`.
    ///
    /// A missing directory is an empty ledger; it is created by the first write, not here.
    /// Files that do not end in `.toml` are ignored, so a README or a sync layer's own
    /// bookkeeping can sit alongside the records.
    pub fn open(dir: &Path) -> Result<Devices> {
        let mut entries = Vec::new();
        match std::fs::read_dir(dir) {
            Ok(rd) => {
                for entry in rd {
                    let entry = entry?;
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    let Some(stem) = name.strip_suffix(".toml") else { continue };
                    let id: GrainId = stem.parse().map_err(|_| {
                        Error::File(format!(
                            "{}: the file name is not a grain-id",
                            entry.path().display()
                        ))
                    })?;
                    let text = std::fs::read_to_string(entry.path())?;
                    let raw: RawDevice = toml::from_str(&text).map_err(|e| {
                        Error::File(format!("{}: {e}", entry.path().display()))
                    })?;
                    entries.push(Device {
                        id,
                        name: raw.name,
                        node_id: raw.node_id,
                        description: raw.description,
                        created_at: raw.created_at.unwrap_or_else(Utc::now),
                        retired_at: raw.retired_at,
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::Io(e)),
        }
        // A stable order, so listings and tests do not depend on directory iteration.
        entries.sort_by(|a, b| a.name.cmp(&b.name));

        if let Some(dup) = first_duplicate_name(&entries) {
            return Err(Error::File(format!(
                "{}: two devices are named {dup:?}",
                dir.display()
            )));
        }
        Ok(Devices { dir: dir.to_owned(), entries })
    }

    /// The directory holding the records.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write one record. Never touches any other file.
    fn save_one(&self, device: &Device) -> Result<()> {
        let raw = RawDevice {
            name: device.name.clone(),
            node_id: device.node_id.clone(),
            description: device.description.clone(),
            created_at: Some(device.created_at),
            retired_at: device.retired_at,
        };
        let body = toml::to_string_pretty(&raw)
            .map_err(|e| Error::File(format!("{}: {e}", device.file_name())))?;
        write_atomic(&self.dir.join(device.file_name()), HEADER, &body)
    }

    /// Remove one record's file.
    fn remove_one(&self, device: &Device) -> Result<()> {
        match std::fs::remove_file(self.dir.join(device.file_name())) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Io(e)),
        }
    }
}

fn first_duplicate_name(entries: &[Device]) -> Option<String> {
    let mut seen = std::collections::HashSet::new();
    entries.iter().find(|d| !seen.insert(d.name.as_str())).map(|d| d.name.clone())
}
```

`RawDevice` loses its `id` field — the file name carries it — and `RawFile` is deleted
outright. Delete `save`, `save_entries`, and the `Devices { path, .. }` field in favour of
`dir`. Rewrite the four mutations so each writes exactly one file:

```rust
impl Devices {
    /// Add a device. Rejects a duplicate `name` or `node_id`.
    pub fn add(
        &mut self,
        name: &str,
        node_id: Option<String>,
        description: Option<String>,
    ) -> Result<Device> {
        if self.entries.iter().any(|d| d.name == name) {
            return Err(Error::File(format!("a device named {name:?} already exists")));
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
            // Astronomically unlikely, but writing it anyway would put two records under
            // one id, and `open` refuses a ledger it cannot address. Ask the caller to
            // try again rather than hunting for a free id.
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
        self.save_one(&entry)?;
        self.entries.push(entry.clone());
        self.entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entry)
    }

    /// Retire a device. The record stays, because synced content refers to its id forever.
    ///
    /// Retiring an already-retired device writes nothing: this instance is a snapshot, and
    /// rewriting an unchanged record could trample an edit that arrived after `open`.
    pub fn retire(&mut self, selector: &str) -> Result<Device> {
        let i = self.index_of(selector)?;
        if self.entries[i].retired_at.is_some() {
            return Ok(self.entries[i].clone());
        }
        let mut updated = self.entries[i].clone();
        updated.retired_at = Some(Utc::now());
        self.save_one(&updated)?;
        self.entries[i] = updated.clone();
        Ok(updated)
    }

    /// Really delete a device. Past `Entry.author` references stop resolving.
    pub fn purge(&mut self, selector: &str) -> Result<Device> {
        let i = self.index_of(selector)?;
        let removed = self.entries[i].clone();
        self.remove_one(&removed)?;
        self.entries.remove(i);
        Ok(removed)
    }
}
```

`set_node_id` from Task 2 changes the same way: build the updated `Device`, call
`self.save_one(&updated)`, then assign it into `self.entries[i]`. Nothing iterates the
directory during a mutation.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-registry --all-features`
Expected: PASS. Tests from Tasks 1 and 2 that called `Devices::load(&path)` must be updated to
`Devices::open(&dir)`; fix them now rather than leaving two spellings.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sapphire-framework-registry
git commit -m "feat(registry)!: store one file per device so concurrent pairings never collide"
```

---

### Task 4: Migrate from the single-file format

**Files:**
- Create: `crates/sapphire-framework-registry/src/migrate.rs`
- Modify: `crates/sapphire-framework-registry/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `migrate.rs`

**Interfaces:**
- Consumes: `Device`, `Devices` (Task 3)
- Produces:
  - `MigrationReport { migrated: usize, skipped: usize }`
  - `migrate_single_file(file: &Path, dir: &Path) -> Result<MigrationReport>` — idempotent,
    deletes nothing, never overwrites an existing record

The old ledger is `devices.toml` with `[[device]]` tables; the new one is a directory. Any
directory can be migrated, which is what lets an application point this at whichever
`.{app}/devices.toml` it used to keep.

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-registry/src/migrate.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Devices;

    const OLD: &str = "\
[[device]]
id = \"DESKTOP\"
name = \"laptop\"
created_at = \"2026-01-01T00:00:00Z\"

[[device]]
name = \"phone\"
user_id = \"someone\"
";

    fn setup() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("devices.toml");
        let dir = tmp.path().join("devices");
        std::fs::write(&file, OLD).unwrap();
        (tmp, file, dir)
    }

    #[test]
    fn every_record_becomes_its_own_file() {
        let (_tmp, file, dir) = setup();
        let report = migrate_single_file(&file, &dir).unwrap();
        assert_eq!(report.migrated, 2);
        assert_eq!(report.skipped, 0);

        let devices = Devices::open(&dir).unwrap();
        let mut names: Vec<&str> = devices.entries().iter().map(|d| d.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["laptop", "phone"]);
    }

    #[test]
    fn an_explicit_id_is_preserved_and_a_missing_one_is_generated() {
        let (_tmp, file, dir) = setup();
        migrate_single_file(&file, &dir).unwrap();

        let devices = Devices::open(&dir).unwrap();
        let laptop = devices.resolve("laptop").unwrap();
        assert_eq!(
            laptop.id,
            "DESKTOP".parse::<crate::GrainId>().unwrap(),
            "an id written into content must survive"
        );
        assert!(devices.resolve("phone").is_ok());
    }

    #[test]
    fn the_old_file_is_left_alone() {
        let (_tmp, file, dir) = setup();
        migrate_single_file(&file, &dir).unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), OLD);
    }

    #[test]
    fn migrating_twice_changes_nothing_the_second_time() {
        let (_tmp, file, dir) = setup();
        migrate_single_file(&file, &dir).unwrap();
        let second = migrate_single_file(&file, &dir).unwrap();
        assert_eq!(second.migrated, 0);
        assert_eq!(second.skipped, 2);
        assert_eq!(Devices::open(&dir).unwrap().entries().len(), 2);
    }

    #[test]
    fn an_existing_record_is_never_overwritten() {
        let (_tmp, file, dir) = setup();
        migrate_single_file(&file, &dir).unwrap();

        // Someone renamed the migrated device afterwards.
        let mut devices = Devices::open(&dir).unwrap();
        let laptop = devices.resolve("laptop").unwrap().clone();
        let record = dir.join(laptop.file_name());
        std::fs::write(&record, "name = \"renamed\"\n").unwrap();
        devices = Devices::open(&dir).unwrap();
        assert!(devices.resolve("renamed").is_ok());

        migrate_single_file(&file, &dir).unwrap();
        assert_eq!(std::fs::read_to_string(&record).unwrap(), "name = \"renamed\"\n");
    }

    #[test]
    fn a_missing_old_file_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let report =
            migrate_single_file(&tmp.path().join("absent.toml"), &tmp.path().join("d")).unwrap();
        assert_eq!(report.migrated, 0);
    }

    #[test]
    fn a_user_id_in_the_old_file_is_dropped() {
        let (_tmp, file, dir) = setup();
        migrate_single_file(&file, &dir).unwrap();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
            assert!(!text.contains("user_id"), "{text}");
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-registry --all-features migrate`
Expected: FAIL — `migrate_single_file` does not exist.

- [ ] **Step 3: Implement the migration**

`crates/sapphire-framework-registry/src/migrate.rs`:

```rust
//! Moving a `devices.toml` into a directory of one-file-per-device records.
//!
//! The migration is idempotent, deletes nothing and never overwrites an existing record, so
//! running it at every startup is safe and running it after someone has edited the new
//! records is harmless.

use std::path::Path;

use chrono::{DateTime, Utc};
use grain_id::GrainId;
use serde::Deserialize;

use crate::devices::Devices;
use crate::error::{Error, Result};

/// What a migration did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Records written into the directory.
    pub migrated: usize,
    /// Records already present, and therefore left alone.
    pub skipped: usize,
}

/// One `[[device]]` table of the old format. `user_id` is accepted and discarded.
#[derive(Debug, Deserialize)]
struct OldDevice {
    #[serde(default)]
    id: Option<GrainId>,
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    retired_at: Option<DateTime<Utc>>,
    #[serde(default, rename = "user_id")]
    _user_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct OldFile {
    #[serde(default)]
    device: Vec<OldDevice>,
}

/// Write every device in `file` into `dir` as its own record.
pub fn migrate_single_file(file: &Path, dir: &Path) -> Result<MigrationReport> {
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MigrationReport::default());
        }
        Err(e) => return Err(Error::Io(e)),
    };
    let old: OldFile =
        toml::from_str(&text).map_err(|e| Error::File(format!("{}: {e}", file.display())))?;

    let mut report = MigrationReport::default();
    let existing = Devices::open(dir)?;

    for entry in old.device {
        // An id written into content must survive; one that was never written can be new.
        let id = entry.id.unwrap_or_else(GrainId::random);
        if existing.get(id).is_some() || existing.entries().iter().any(|d| d.name == entry.name)
        {
            report.skipped += 1;
            continue;
        }
        let device = crate::Device {
            id,
            name: entry.name,
            node_id: entry.node_id,
            description: entry.description,
            created_at: entry.created_at.unwrap_or_else(Utc::now),
            retired_at: entry.retired_at,
        };
        Devices::write_record(dir, &device)?;
        report.migrated += 1;
    }
    Ok(report)
}
```

`migrate_single_file` needs to write a record without going through `add` (which would mint a
new id). Add to `devices.rs`:

```rust
impl Devices {
    /// Write one record into `dir` without opening the whole ledger.
    ///
    /// Used by the migration, which already knows the id each record must keep.
    pub(crate) fn write_record(dir: &Path, device: &Device) -> Result<()> {
        Devices { dir: dir.to_owned(), entries: Vec::new() }.save_one(device)
    }
}
```

`lib.rs`: `pub use migrate::{MigrationReport, migrate_single_file};`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sapphire-framework-registry --all-features`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework-registry crates/sapphire-framework
git commit -m "feat(registry): migrate a devices.toml into per-device record files"
```

---

### Task 5: Update the facade and the architecture note

**Files:**
- Modify: `crates/sapphire-framework/src/lib.rs`
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Check the facade re-export compiles and covers the new items**

`crates/sapphire-framework/src/lib.rs`, in the registry re-export block:

```rust
    pub use crate::registry::{Device, Devices, GrainId, MigrationReport, migrate_single_file};
```

Run: `cargo build -p sapphire-framework --all-features`
Expected: success.

- [ ] **Step 2: Note the format change**

`docs/ARCHITECTURE.md`, in the crate table, replace the `sapphire-framework-registry`
description with:

```markdown
| `sapphire-framework-registry` | デバイス台帳（`<dir>/<grain-id>.toml` を 1 デバイス 1 ファイル。`node_id` を保持。users は撤去） | ✅ |
```

If the table has no registry row, add one in the same position as the crate's place in
`Cargo.toml`'s `members`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates/sapphire-framework docs/ARCHITECTURE.md
git commit -m "docs(registry): record the per-device file format"
```

---

## What this plan does not cover

- **Where the ledger directory lives.** This crate takes a `&Path`. The bridge points it at
  `workgroups/<workgroup-id>/root/devices/` (sync spec §3.5); that wiring is in the bridge
  plan, not here.
- **Validating a `node_id`.** This crate stores the string. The bridge parses it into iroh's
  `NodeId`, and it is the only place that has the type.
- **Calling the migration.** Nothing calls `migrate_single_file` yet. The bridge calls it when
  it opens a workgroup, and each app calls it for its own old `.{app}/devices.toml` during its
  migration, in its own repository's plan.
- **`Workspace::devices_path`.** It still points at `.{app}/devices.toml`. It is removed in
  step 11's cleanup, once no application reads it.
