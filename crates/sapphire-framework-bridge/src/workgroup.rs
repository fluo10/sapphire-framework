//! The workgroup this host belongs to, and who is allowed to connect.

use std::path::PathBuf;

use grain_id::GrainId;
use sapphire_registry::{Device, Devices};
use serde::{Deserialize, Serialize};

use crate::dir::BridgeDir;
use crate::error::{Error, Result};

/// The `workgroup.toml` inside a workgroup's root: what the workgroup says about itself.
///
/// It lives in the workgroup's synced root, so every device of the workgroup sees the same
/// name once the workgroup is shared.
#[derive(Debug, Deserialize, Serialize)]
struct WorkgroupFile {
    name: String,
}

/// A set of devices that share workspaces.
#[derive(Clone, Debug)]
pub struct Workgroup {
    /// Its id.
    pub id: GrainId,
    /// Its name.
    pub name: String,
    /// `<bridge dir>/workgroups/<id>/`.
    pub dir: PathBuf,
    /// `<bridge dir>/workgroups/<id>/root/devices/`, the ledger directory.
    devices_dir: PathBuf,
}

impl Workgroup {
    /// Create a workgroup and write this device's own record into it.
    ///
    /// The founding device needs a record before its first sync, because `Entry.author` is
    /// its device id.
    ///
    /// The first release allows one workgroup per host; the layout and the wire format
    /// support several, so lifting the limit is a CLI change.
    pub fn create(
        dir: &BridgeDir,
        name: &str,
        this_device: &str,
        node_id: &str,
    ) -> Result<Workgroup> {
        if Workgroup::open(dir)?.is_some() {
            return Err(Error::Config(
                "this host already belongs to a workgroup".to_owned(),
            ));
        }
        let id = GrainId::random();
        let wg_dir = dir.workgroup_dir(id);
        std::fs::create_dir_all(wg_dir.join("root"))?;
        std::fs::create_dir_all(dir.devices_dir(id))?;
        let file = wg_dir.join("root").join("workgroup.toml");
        std::fs::write(
            &file,
            toml::to_string_pretty(&WorkgroupFile {
                name: name.to_owned(),
            })
            .map_err(|e| Error::Config(format!("{}: {e}", file.display())))?,
        )?;

        let workgroup = Workgroup {
            id,
            name: name.to_owned(),
            dir: wg_dir,
            devices_dir: dir.devices_dir(id),
        };
        let mut devices = workgroup.devices()?;
        devices.add(this_device, Some(node_id.to_owned()), None)?;
        Ok(workgroup)
    }

    /// The workgroup this host belongs to, if any.
    pub fn open(dir: &BridgeDir) -> Result<Option<Workgroup>> {
        let entries = match std::fs::read_dir(dir.workgroups_dir()) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            // A directory that is not named after a workgroup id is not one: a leftover
            // temporary file, or something a user put here.
            let Some(id) = name.to_str().and_then(|s| s.parse::<GrainId>().ok()) else {
                continue;
            };
            let file = entry.path().join("root").join("workgroup.toml");
            let text = std::fs::read_to_string(&file).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::Config(format!(
                        "{}: there is no workgroup here ({} is missing)",
                        entry.path().display(),
                        file.display()
                    ))
                } else {
                    Error::Io(e)
                }
            })?;
            let parsed: WorkgroupFile = toml::from_str(&text)
                .map_err(|e| Error::Config(format!("{}: {e}", file.display())))?;
            return Ok(Some(Workgroup {
                id,
                name: parsed.name,
                dir: entry.path(),
                devices_dir: dir.devices_dir(id),
            }));
        }
        Ok(None)
    }

    /// The device ledger, read fresh.
    ///
    /// Always reads from disk: the ledger is synced, so a pairing or a retirement that
    /// arrived from another device must take effect without restarting the bridge.
    pub fn devices(&self) -> Result<Devices> {
        Ok(Devices::open(&self.devices_dir)?)
    }

    /// This host's own record.
    // TODO(step 8): `this_device` takes the first record, which only works while `create` is
    // the only way a host gets a workgroup. `workgroup join` gives a host a workgroup it did
    // not found, so this must then find the record whose `node_id` matches this host's —
    // which means `bridge.rs` has to hand the node id in. No other caller until then.
    pub fn this_device(&self) -> Result<Device> {
        let devices = self.devices()?;
        devices
            .entries()
            .first()
            .cloned()
            .ok_or_else(|| Error::Config("this workgroup has no devices".to_owned()))
    }

    /// The device behind `node_id`, if it may connect.
    ///
    /// Reads the ledger on every call: authorization is a live question, and a retirement
    /// that arrived from another device must close the door without a restart.
    pub fn authorize(&self, node_id: &str) -> Result<Device> {
        let devices = self.devices()?;
        let Some(device) = devices.by_node_id(node_id) else {
            return Err(Error::Unauthorized(format!(
                "{node_id} is not a device of this workgroup"
            )));
        };
        if device.is_retired() {
            return Err(Error::Unauthorized(format!(
                "the device {} ({node_id}) is retired",
                device.name
            )));
        }
        Ok(device.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::BridgeDir;
    use crate::net::NetConfig;

    const NODE_A: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const NODE_B: &str = "b1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn bridge_dir() -> (tempfile::TempDir, BridgeDir) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = BridgeDir::at(tmp.path().join("bridge")).unwrap();
        (tmp, dir)
    }

    #[test]
    fn creating_a_workgroup_writes_this_devices_record() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();

        let me = wg.this_device().unwrap();
        assert_eq!(me.name, "laptop");
        assert_eq!(me.node_id.as_deref(), Some(NODE_A));
    }

    #[test]
    fn a_created_workgroup_reopens() {
        let (_tmp, dir) = bridge_dir();
        let created = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let reopened = Workgroup::open(&dir).unwrap().expect("a workgroup");
        assert_eq!(reopened.id, created.id);
        assert_eq!(reopened.name, "home");
    }

    #[test]
    fn a_host_without_a_workgroup_reports_none() {
        let (_tmp, dir) = bridge_dir();
        assert!(Workgroup::open(&dir).unwrap().is_none());
    }

    #[test]
    fn a_second_workgroup_is_refused_for_now() {
        let (_tmp, dir) = bridge_dir();
        Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let err = Workgroup::create(&dir, "work", "laptop", NODE_A).unwrap_err();
        assert!(err.to_string().contains("already"), "{err}");
    }

    #[test]
    fn a_known_node_is_authorized() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let mut devices = wg.devices().unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();

        let device = wg.authorize(NODE_B).unwrap();
        assert_eq!(device.name, "phone");
    }

    #[test]
    fn an_unknown_node_is_refused() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let err = wg.authorize(NODE_B).unwrap_err();
        assert!(
            err.to_string().contains("not a device of this workgroup"),
            "{err}"
        );
    }

    #[test]
    fn a_retired_node_is_refused_and_says_so() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let mut devices = wg.devices().unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();
        devices.retire("phone").unwrap();

        let err = wg.authorize(NODE_B).unwrap_err();
        assert!(err.to_string().contains("retired"), "{err}");
    }

    #[test]
    fn authorization_rereads_the_ledger_each_time() {
        let (_tmp, dir) = bridge_dir();
        let wg = Workgroup::create(&dir, "home", "laptop", NODE_A).unwrap();
        let mut devices = wg.devices().unwrap();
        devices.add("phone", Some(NODE_B.to_owned()), None).unwrap();
        assert!(wg.authorize(NODE_B).is_ok());

        // Retirement arrives from another device while the bridge is running.
        let mut fresh = wg.devices().unwrap();
        fresh.retire("phone").unwrap();

        assert!(
            wg.authorize(NODE_B).is_err(),
            "a revocation must take effect without restarting the bridge"
        );
    }

    #[test]
    fn net_configuration_defaults_to_waking_owners() {
        let (_tmp, dir) = bridge_dir();
        let net = NetConfig::load(&dir.net_toml()).unwrap();
        assert!(net.wake_on_sync);
        assert!(net.discovery);
        assert!(net.relays.is_empty());
    }
}
