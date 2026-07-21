//! Per-workspace change log persisted in redb.
//!
//! The log is an append-only `seq(u64) -> Change(json)` table. `seq` is the
//! [`Cursor`](sapphire_rpc::Cursor): strictly increasing, assigned by
//! [`append`](ChangeLog::append). Clients pull everything with a `seq` greater
//! than the cursor they last applied.

use std::collections::HashMap;
use std::path::Path;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use sapphire_rpc::{Change, Cursor};

use crate::error::Result;

/// `seq -> serialized Change`.
const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("changes");

/// Append-only change log backed by redb.
pub struct ChangeLog {
    db: Database,
}

impl ChangeLog {
    /// Open (or create) the change log at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        let wtx = db.begin_write()?;
        wtx.open_table(TABLE)?;
        wtx.commit()?;
        Ok(Self { db })
    }

    /// The highest assigned `seq` (0 when the log is empty).
    pub fn max_seq(&self) -> Result<Cursor> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(TABLE)?;
        Ok(match table.last()? {
            Some((k, _)) => k.value(),
            None => 0,
        })
    }

    /// Append `change`, assigning it the next `seq`. The stored copy carries the
    /// assigned `seq`; the returned [`Change`] does too.
    pub fn append(&self, mut change: Change) -> Result<Change> {
        let wtx = self.db.begin_write()?;
        let seq = {
            let mut table = wtx.open_table(TABLE)?;
            let next = match table.last()? {
                Some((k, _)) => k.value() + 1,
                None => 1,
            };
            change.seq = next;
            let bytes = serde_json::to_vec(&change)?;
            table.insert(next, bytes.as_slice())?;
            next
        };
        wtx.commit()?;
        change.seq = seq;
        Ok(change)
    }

    /// All changes with `seq > since`, ascending, capped at `limit`. The second
    /// element of the tuple is `true` when more changes exist beyond the cap.
    pub fn since(&self, since: Cursor, limit: usize) -> Result<(Vec<Change>, bool)> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(TABLE)?;
        let mut out = Vec::new();
        let mut more = false;
        // `since + 1` is the first seq we want; guard against overflow.
        let start = since.saturating_add(1);
        for entry in table.range(start..)? {
            let (_, v) = entry?;
            if out.len() == limit {
                more = true;
                break;
            }
            out.push(serde_json::from_slice::<Change>(v.value())?);
        }
        Ok((out, more))
    }

    /// Fold the entire log into the latest [`Change`] per path (ascending seq,
    /// so later entries win). Tombstones are retained in the map.
    pub fn latest_per_path(&self) -> Result<HashMap<String, Change>> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(TABLE)?;
        let mut map: HashMap<String, Change> = HashMap::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let change: Change = serde_json::from_slice(v.value())?;
            map.insert(change.path.clone(), change);
        }
        Ok(map)
    }

    /// Number of entries in the log.
    pub fn len(&self) -> Result<u64> {
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(TABLE)?;
        Ok(table.len()?)
    }

    /// Whether the log has no entries.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn log() -> (tempfile::TempDir, ChangeLog) {
        let tmp = tempfile::tempdir().unwrap();
        let log = ChangeLog::open(&tmp.path().join("changes.redb")).unwrap();
        (tmp, log)
    }

    #[test]
    fn append_assigns_increasing_seq() {
        let (_t, log) = log();
        let a = log.append(Change::upsert("a.md", "1", Utc::now())).unwrap();
        let b = log.append(Change::upsert("b.md", "2", Utc::now())).unwrap();
        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert_eq!(log.max_seq().unwrap(), 2);
    }

    #[test]
    fn since_returns_newer_and_more_flag() {
        let (_t, log) = log();
        for i in 0..5 {
            log.append(Change::upsert(format!("{i}.md"), "x", Utc::now()))
                .unwrap();
        }
        let (batch, more) = log.since(0, 3).unwrap();
        assert_eq!(batch.len(), 3);
        assert!(more);
        assert_eq!(batch[0].seq, 1);

        let (rest, more2) = log.since(3, 10).unwrap();
        assert_eq!(rest.len(), 2);
        assert!(!more2);
        assert_eq!(rest[0].seq, 4);
    }

    #[test]
    fn latest_per_path_keeps_last_write() {
        let (_t, log) = log();
        log.append(Change::upsert("a.md", "old", Utc::now())).unwrap();
        log.append(Change::upsert("a.md", "new", Utc::now())).unwrap();
        let map = log.latest_per_path().unwrap();
        match &map["a.md"].kind {
            sapphire_rpc::ChangeKind::Upsert { body, .. } => assert_eq!(body, "new"),
            _ => panic!("expected upsert"),
        }
    }
}
