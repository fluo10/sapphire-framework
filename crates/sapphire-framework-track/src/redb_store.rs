use std::{collections::HashMap, path::Path};

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::{Result, TrackStore};

/// Single table mapping a file path to its last-seen mtime (epoch seconds).
const TABLE: TableDefinition<&str, i64> = TableDefinition::new("tracked");

/// Persistent [`TrackStore`] backed by a pure-Rust [redb] database.
///
/// The database is a rebuildable cache: callers version the filename (e.g.
/// `track_v1.redb`) so that an incompatible redb format bump simply orphans
/// the old file and a fresh one is created here.
///
/// [redb]: https://docs.rs/redb
pub struct RedbTrackStore {
    db: Database,
}

impl RedbTrackStore {
    /// Open (or create) the database at `path`, ensuring the table exists so
    /// that read transactions never fail on a brand-new file.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        // Materialise the table once up front; `open_table` in a write txn
        // creates it if missing.
        let wtx = db.begin_write()?;
        wtx.open_table(TABLE)?;
        wtx.commit()?;
        Ok(Self { db })
    }
}

impl TrackStore for RedbTrackStore {
    fn mtimes(&self) -> Result<HashMap<String, i64>> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(TABLE)?;
        let mut out = HashMap::new();
        for entry in table.iter()? {
            let (k, v) = entry?;
            out.insert(k.value().to_owned(), v.value());
        }
        Ok(out)
    }

    fn upsert(&self, path: &str, mtime: i64) -> Result<()> {
        let wtx = self.db.begin_write()?;
        {
            let mut table = wtx.open_table(TABLE)?;
            table.insert(path, mtime)?;
        }
        wtx.commit()?;
        Ok(())
    }

    fn remove(&self, path: &str) -> Result<()> {
        let wtx = self.db.begin_write()?;
        {
            let mut table = wtx.open_table(TABLE)?;
            table.remove(path)?;
        }
        wtx.commit()?;
        Ok(())
    }

    fn count(&self) -> Result<u64> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(TABLE)?;
        Ok(table.len()?)
    }

    fn upsert_many(&self, entries: &[(String, i64)]) -> Result<()> {
        let wtx = self.db.begin_write()?;
        {
            let mut table = wtx.open_table(TABLE)?;
            for (path, mtime) in entries {
                table.insert(path.as_str(), *mtime)?;
            }
        }
        wtx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redb_round_trips_and_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("track_v1.redb");

        {
            let store = RedbTrackStore::open(&path).unwrap();
            store.upsert("a", 1).unwrap();
            store
                .upsert_many(&[("b".into(), 2), ("c".into(), 3)])
                .unwrap();
            assert_eq!(store.count().unwrap(), 3);
            store.remove("a").unwrap();
            assert_eq!(store.count().unwrap(), 2);
        }

        // Reopen: data survives across instances.
        let store = RedbTrackStore::open(&path).unwrap();
        let m = store.mtimes().unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("b"), Some(&2));
        assert!(!m.contains_key("a"));
    }

    #[test]
    fn fresh_db_reads_empty_without_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RedbTrackStore::open(&tmp.path().join("track_v1.redb")).unwrap();
        assert_eq!(store.count().unwrap(), 0);
        assert!(store.mtimes().unwrap().is_empty());
    }
}
