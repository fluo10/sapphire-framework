//! Replica store (redb): one table of path states and one metadata record.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::{Error, RedbExt, Result};
use crate::hlc::Hlc;
use crate::id::ReplicaId;
use crate::state::PathState;
use crate::vv::VersionVector;

/// On-disk format of the replica store. Bump for any change in stored data *or* in
/// merge behaviour (spec §5.4).
pub const FORMAT_VERSION: u32 = 1;

const PATHS: TableDefinition<&str, &[u8]> = TableDefinition::new("paths");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const META_KEY: &str = "meta";

/// Replica-wide metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    pub format_version: u32,
    pub replica_id: ReplicaId,
    /// Counter of the last dot this replica assigned.
    pub counter: u64,
    pub hlc: Hlc,
    /// Every dot whose effects this replica's states include.
    pub vv: VersionVector,
    /// Workspace root this store belongs to.
    pub root: String,
}

/// Persistent replica state.
pub struct ReplicaStore {
    db: Database,
}

#[derive(Deserialize)]
struct FormatProbe {
    format_version: u32,
}

impl ReplicaStore {
    pub fn open(path: &Path, root: &str) -> Result<Self> {
        Self::open_with_id(path, root, None)
    }

    /// Open or create the store. `id` is used only when the store is created.
    pub fn open_with_id(path: &Path, root: &str, id: Option<ReplicaId>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path).db()?;
        let wtx = db.begin_write().db()?;
        {
            wtx.open_table(PATHS).db()?;
            let mut meta_table = wtx.open_table(META).db()?;
            let existing = meta_table.get(META_KEY).db()?.map(|g| g.value().to_vec());
            match existing {
                None => {
                    let meta = Meta {
                        format_version: FORMAT_VERSION,
                        replica_id: id.unwrap_or_default(),
                        counter: 0,
                        hlc: Hlc::default(),
                        vv: VersionVector::new(),
                        root: root.to_owned(),
                    };
                    meta_table
                        .insert(META_KEY, serde_json::to_vec(&meta)?.as_slice())
                        .db()?;
                }
                Some(bytes) => {
                    let probe: FormatProbe = serde_json::from_slice(&bytes)?;
                    if probe.format_version > FORMAT_VERSION {
                        return Err(Error::FormatTooNew {
                            found: probe.format_version,
                            supported: FORMAT_VERSION,
                        });
                    }
                    let meta: Meta = serde_json::from_slice(&bytes)?;
                    if meta.root != root {
                        return Err(Error::RootMismatch {
                            stored: meta.root,
                            requested: root.to_owned(),
                        });
                    }
                }
            }
        }
        wtx.commit().db()?;
        Ok(Self { db })
    }

    pub fn meta(&self) -> Result<Meta> {
        let rtx = self.db.begin_read().db()?;
        let table = rtx.open_table(META).db()?;
        let guard = table
            .get(META_KEY)
            .db()?
            .ok_or_else(|| Error::Corrupt("missing meta record".into()))?;
        Ok(serde_json::from_slice(guard.value())?)
    }

    pub fn get(&self, path: &str) -> Result<Option<PathState>> {
        let rtx = self.db.begin_read().db()?;
        let table = rtx.open_table(PATHS).db()?;
        match table.get(path).db()? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    pub fn all(&self) -> Result<Vec<(String, PathState)>> {
        let rtx = self.db.begin_read().db()?;
        let table = rtx.open_table(PATHS).db()?;
        let mut out = Vec::new();
        for item in table.iter().db()? {
            let (k, v) = item.db()?;
            out.push((k.value().to_owned(), serde_json::from_slice(v.value())?));
        }
        Ok(out)
    }

    pub fn any_materialized(&self) -> Result<bool> {
        Ok(self.all()?.iter().any(|(_, s)| s.disk.hash.is_some()))
    }

    /// Write `meta` and `states` in one transaction.
    pub fn commit(&self, meta: &Meta, states: &[(String, PathState)]) -> Result<()> {
        let wtx = self.db.begin_write().db()?;
        {
            let mut meta_table = wtx.open_table(META).db()?;
            meta_table
                .insert(META_KEY, serde_json::to_vec(meta)?.as_slice())
                .db()?;
            let mut paths = wtx.open_table(PATHS).db()?;
            for (path, state) in states {
                paths
                    .insert(path.as_str(), serde_json::to_vec(state)?.as_slice())
                    .db()?;
            }
        }
        wtx.commit().db()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{Content, Entry};
    use crate::hash::ContentHash;
    use crate::state::DiskState;
    use crate::vv::Dot;
    use grain_id::GrainId;

    fn state(id: ReplicaId, body: &str, on_disk: bool) -> PathState {
        let e = Entry {
            path: "a.txt".into(),
            content: Content::File {
                hash: ContentHash::of_bytes(body.as_bytes()),
                len: 1,
            },
            hlc: Hlc::default(),
            dot: Dot {
                replica: id,
                counter: 1,
            },
            context: VersionVector::new(),
            author: GrainId::NIL,
        };
        let mut seen = VersionVector::new();
        seen.add_dot(&e.dot);
        let disk = DiskState {
            hash: on_disk.then(|| ContentHash::of_bytes(body.as_bytes())),
            ..DiskState::default()
        };
        PathState {
            versions: vec![e],
            seen,
            disk,
        }
    }

    #[test]
    fn creates_meta_once_and_persists_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sync.redb");
        let (id, s) = {
            let store = ReplicaStore::open(&path, "/root").unwrap();
            let mut meta = store.meta().unwrap();
            assert_eq!(meta.format_version, FORMAT_VERSION);
            assert_eq!(meta.counter, 0);
            meta.counter = 7;
            let s = state(meta.replica_id, "x", false);
            store.commit(&meta, &[("a.txt".into(), s.clone())]).unwrap();
            (meta.replica_id, s)
        };
        let store = ReplicaStore::open(&path, "/root").unwrap();
        let meta = store.meta().unwrap();
        assert_eq!((meta.replica_id, meta.counter), (id, 7));
        assert_eq!(store.get("a.txt").unwrap(), Some(s.clone()));
        assert_eq!(store.get("b.txt").unwrap(), None);
        assert_eq!(store.all().unwrap(), vec![("a.txt".to_string(), s)]);
        assert!(!store.any_materialized().unwrap());
    }

    #[test]
    fn any_materialized_sees_files_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = ReplicaStore::open(&dir.path().join("s.redb"), "/r").unwrap();
        let meta = store.meta().unwrap();
        store
            .commit(
                &meta,
                &[("a.txt".into(), state(meta.replica_id, "x", true))],
            )
            .unwrap();
        assert!(store.any_materialized().unwrap());
    }

    #[test]
    fn refuses_another_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        drop(ReplicaStore::open(&path, "/one").unwrap());
        assert!(matches!(
            ReplicaStore::open(&path, "/two"),
            Err(Error::RootMismatch { .. })
        ));
    }

    #[test]
    fn refuses_a_newer_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        {
            let store = ReplicaStore::open(&path, "/r").unwrap();
            let mut meta = store.meta().unwrap();
            meta.format_version = FORMAT_VERSION + 1;
            store.commit(&meta, &[]).unwrap();
        }
        assert!(matches!(
            ReplicaStore::open(&path, "/r"),
            Err(Error::FormatTooNew { found, .. }) if found == FORMAT_VERSION + 1
        ));
    }

    #[test]
    fn open_with_id_uses_the_id_only_on_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        let fixed = ReplicaId(uuid::Uuid::from_u128(5));
        let store = ReplicaStore::open_with_id(&path, "/r", Some(fixed)).unwrap();
        assert_eq!(store.meta().unwrap().replica_id, fixed);
        drop(store);
        let other = ReplicaId(uuid::Uuid::from_u128(6));
        let store = ReplicaStore::open_with_id(&path, "/r", Some(other)).unwrap();
        assert_eq!(store.meta().unwrap().replica_id, fixed);
    }
}
