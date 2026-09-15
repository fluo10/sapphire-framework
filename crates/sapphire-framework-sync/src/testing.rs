//! Helpers for tests (feature `test-util`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::hash::ContentHash;
use crate::hlc::Clock;
use crate::replica::ContentSource;

/// A clock the test moves by hand.
#[derive(Debug)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(ms: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(ms)))
    }

    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A content source backed by a map.
#[derive(Debug, Default)]
pub struct MapSource(pub HashMap<ContentHash, Vec<u8>>);

impl MapSource {
    pub fn with(mut self, bytes: &[u8]) -> Self {
        self.0.insert(ContentHash::of_bytes(bytes), bytes.to_vec());
        self
    }
}

impl ContentSource for MapSource {
    fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>> {
        self.0.get(hash).cloned()
    }
}

/// Where an injected fault fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    /// After a path state is committed, before its file is written or removed.
    AfterCommitBeforeWrite,
}
