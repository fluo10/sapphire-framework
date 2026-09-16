//! Replica identity.

use grain_id::GrainId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identity of one replica store, created once (UUIDv7). A reinstall gets a new one,
/// so dots are never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReplicaId(pub Uuid);

impl ReplicaId {
    /// A fresh time-ordered id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// The grain-id shown in file names. A UUIDv7 starts with a timestamp, so the
    /// trailing random bytes are used.
    pub fn display_id(&self) -> GrainId {
        let bytes = self.0.as_bytes();
        let tail: &[u8; 5] = bytes[11..16].try_into().expect("a UUID has 16 bytes");
        GrainId::from_byte_suffix(tail)
    }
}

impl Default for ReplicaId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_id_uses_the_uuid_suffix() {
        let max = ReplicaId(Uuid::from_u128(0x0190_0000_0000_7000_8000_00ff_ffff_ffff));
        let nil = ReplicaId(Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000));
        assert_eq!(max.display_id(), GrainId::MAX);
        assert_eq!(nil.display_id(), GrainId::NIL);
    }

    #[test]
    fn ids_created_together_rarely_share_a_display_id() {
        let ids: std::collections::HashSet<_> =
            (0..1000).map(|_| ReplicaId::new().display_id()).collect();
        assert!(ids.len() > 990);
    }
}
