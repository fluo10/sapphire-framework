//! What two replicas say to each other.

use grain_id::GrainId;
use sapphire_sync::{ContentHash, PathUpdate, ReplicaId, VersionVector};
use serde::{Deserialize, Serialize};

/// One control message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Message {
    /// The first message from each side.
    Hello {
        /// Session format this side speaks.
        format: u32,
        /// Which workspace this session is about.
        workspace_id: GrainId,
        /// The sender's replica id.
        replica_id: ReplicaId,
        /// Everything the sender already has.
        vv: VersionVector,
    },
    /// A page of path states the peer's version vector does not cover.
    Updates(Vec<PathUpdate>),
    /// The sender has sent everything it had at `Hello` time.
    Done,
    /// The sender needs the content behind this hash.
    Want(ContentHash),
    /// The sender does not have the content behind this hash either.
    Missing(ContentHash),
    /// The sender will not continue, and why.
    Refused(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(msg: &Message) -> Message {
        let json = serde_json::to_vec(msg).unwrap();
        serde_json::from_slice(&json).unwrap()
    }

    #[test]
    fn a_hello_round_trips() {
        let mut vv = VersionVector::new();
        vv.add_dot(&sapphire_sync::Dot {
            replica: ReplicaId::new(),
            counter: 3,
        });
        let msg = Message::Hello {
            format: crate::SESSION_FORMAT_VERSION,
            workspace_id: GrainId::random(),
            replica_id: ReplicaId::new(),
            vv,
        };
        assert_eq!(format!("{:?}", round_trip(&msg)), format!("{msg:?}"));
    }

    #[test]
    fn the_payload_free_messages_round_trip() {
        assert!(matches!(round_trip(&Message::Done), Message::Done));
    }

    #[test]
    fn a_hash_carrying_message_round_trips_as_hex() {
        let hash = ContentHash::of_bytes(b"content");
        let json = serde_json::to_string(&Message::Want(hash)).unwrap();
        // The hash travels as hex, not as an array of 32 numbers.
        assert!(json.contains(&hash.to_hex()), "got {json}");
        assert_eq!(
            format!("{:?}", round_trip(&Message::Want(hash))),
            format!("{:?}", Message::Want(hash))
        );
        assert_eq!(
            format!("{:?}", round_trip(&Message::Missing(hash))),
            format!("{:?}", Message::Missing(hash))
        );
    }

    #[test]
    fn a_refusal_round_trips() {
        let msg = Message::Refused("format 2".to_owned());
        match round_trip(&msg) {
            Message::Refused(why) => assert_eq!(why, "format 2"),
            other => panic!("got {other:?}"),
        }
    }
}
