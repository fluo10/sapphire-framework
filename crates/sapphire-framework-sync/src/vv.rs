//! Dots and version vectors.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::id::ReplicaId;

/// One event: the `counter`-th write recorded by `replica`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub replica: ReplicaId,
    pub counter: u64,
}

/// For each replica, the highest counter covered.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionVector(pub BTreeMap<ReplicaId, u64>);

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, replica: &ReplicaId) -> u64 {
        self.0.get(replica).copied().unwrap_or(0)
    }

    pub fn covers_dot(&self, dot: &Dot) -> bool {
        self.get(&dot.replica) >= dot.counter
    }

    pub fn covers(&self, other: &VersionVector) -> bool {
        other.0.iter().all(|(r, c)| self.get(r) >= *c)
    }

    pub fn add_dot(&mut self, dot: &Dot) {
        let slot = self.0.entry(dot.replica).or_insert(0);
        *slot = (*slot).max(dot.counter);
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (r, c) in &other.0 {
            let slot = self.0.entry(*r).or_insert(0);
            *slot = (*slot).max(*c);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn r(n: u128) -> ReplicaId {
        ReplicaId(Uuid::from_u128(n))
    }

    #[test]
    fn cover_and_merge() {
        let mut a = VersionVector::new();
        a.add_dot(&Dot {
            replica: r(1),
            counter: 3,
        });
        let mut b = VersionVector::new();
        b.add_dot(&Dot {
            replica: r(2),
            counter: 1,
        });
        assert!(a.covers_dot(&Dot {
            replica: r(1),
            counter: 2
        }));
        assert!(!a.covers_dot(&Dot {
            replica: r(2),
            counter: 1
        }));
        assert!(!a.covers(&b));
        a.merge(&b);
        assert!(a.covers(&b));
        a.add_dot(&Dot {
            replica: r(1),
            counter: 1,
        });
        assert_eq!(a.get(&r(1)), 3, "add_dot never lowers a counter");
    }

    #[test]
    fn serializes_as_a_json_object_keyed_by_uuid() {
        let mut v = VersionVector::new();
        v.add_dot(&Dot {
            replica: r(1),
            counter: 2,
        });
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#"{"00000000-0000-0000-0000-000000000001":2}"#);
        assert_eq!(serde_json::from_str::<VersionVector>(&json).unwrap(), v);
    }
}
