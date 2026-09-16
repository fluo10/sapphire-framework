//! Joining path states and choosing what is written to disk.

use crate::entry::Entry;
use crate::hlc::Hlc;
use crate::id::ReplicaId;
use crate::vv::{Dot, VersionVector};

/// Ordering key of a version: edits beat deletes, then later clock, then replica and
/// counter as tie-breakers. The maximum is the winner.
fn winner_key(e: &Entry) -> (bool, Hlc, ReplicaId, u64) {
    (
        !e.content.is_tombstone(),
        e.hlc,
        e.dot.replica,
        e.dot.counter,
    )
}

/// The sibling whose content belongs on disk.
pub fn winner(versions: &[Entry]) -> &Entry {
    versions
        .iter()
        .max_by_key(|e| winner_key(e))
        .expect("a path state always has at least one version")
}

/// DVV-set join of a local state with an incoming one. Returns the joined
/// `(versions, seen)`, or `None` when the local state already contains everything.
pub fn join(
    local: Option<(&[Entry], &VersionVector)>,
    incoming: &[Entry],
    incoming_seen: &VersionVector,
) -> Option<(Vec<Entry>, VersionVector)> {
    // With no version on either side there is nothing to return but an empty sibling
    // set, and `winner` — also `pub` — panics on one. The `local = None` branch below
    // would otherwise hand the empty `incoming` straight back.
    if incoming.is_empty() && local.is_none_or(|(versions, _)| versions.is_empty()) {
        tracing::error!("join of two empty sibling sets; keeping the local state");
        return None;
    }
    let Some((local, local_seen)) = local else {
        let mut versions = incoming.to_vec();
        versions.sort_by_key(|e| e.dot);
        return Some((versions, incoming_seen.clone()));
    };

    let mut versions: Vec<Entry> = local
        .iter()
        .filter(|e| !incoming_seen.covers_dot(&e.dot) || incoming.iter().any(|i| i.dot == e.dot))
        .cloned()
        .collect();
    for e in incoming {
        let keep = !local_seen.covers_dot(&e.dot) || local.iter().any(|l| l.dot == e.dot);
        if keep && !versions.iter().any(|v| v.dot == e.dot) {
            versions.push(e.clone());
        }
    }
    versions.sort_by_key(|e| e.dot);

    let mut seen = local_seen.clone();
    seen.merge(incoming_seen);

    if versions.is_empty() {
        // Impossible for well-formed states: each side would have to know of a version
        // superseding everything the other holds. Keep the local state rather than
        // leave a path with no version.
        tracing::error!("join produced an empty sibling set; keeping the local state");
        return None;
    }
    if versions.as_slice() == local && seen == *local_seen {
        return None;
    }
    Some((versions, seen))
}

/// Whether `loser` needs a conflict copy next to `winner`.
pub fn needs_copy(loser: &Entry, winner: &Entry) -> bool {
    !loser.content.is_tombstone() && loser.content.hash() != winner.content.hash()
}

/// `<stem>.conflict-<grain-id>-<counter>.<ext>`, or `<name>.conflict-<grain-id>-<counter>`
/// when the name has no extension. A leading dot does not start an extension.
pub fn conflict_path(path: &str, loser: &Dot) -> String {
    let (dir, name) = match path.rfind('/') {
        Some(i) => (&path[..=i], &path[i + 1..]),
        None => ("", path),
    };
    let tag = format!("conflict-{}-{}", loser.replica.display_id(), loser.counter);
    match name.rfind('.') {
        Some(i) if i > 0 => format!("{dir}{}.{tag}.{}", &name[..i], &name[i + 1..]),
        _ => format!("{dir}{name}.{tag}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Content;
    use crate::hash::ContentHash;
    use crate::hlc::Hlc;
    use crate::id::ReplicaId;
    use grain_id::GrainId;
    use uuid::Uuid;

    fn rid(n: u128) -> ReplicaId {
        ReplicaId(Uuid::from_u128(n))
    }

    fn dot(r: u128, c: u64) -> Dot {
        Dot {
            replica: rid(r),
            counter: c,
        }
    }

    fn vv(dots: &[Dot]) -> VersionVector {
        let mut v = VersionVector::new();
        for d in dots {
            v.add_dot(d);
        }
        v
    }

    fn file(d: Dot, wall: u64, body: &str, ctx: &[Dot]) -> Entry {
        Entry {
            path: "a.txt".into(),
            content: Content::File {
                hash: ContentHash::of_bytes(body.as_bytes()),
                len: body.len() as u64,
            },
            hlc: Hlc {
                wall_ms: wall,
                logical: 0,
            },
            dot: d,
            context: vv(ctx),
            author: GrainId::NIL,
        }
    }

    fn tomb(d: Dot, wall: u64, ctx: &[Dot]) -> Entry {
        Entry {
            content: Content::Tombstone,
            ..file(d, wall, "", ctx)
        }
    }

    /// A state as (versions, seen), built the way a local write would build it.
    fn written(e: Entry) -> (Vec<Entry>, VersionVector) {
        let mut seen = e.context.clone();
        seen.add_dot(&e.dot);
        (vec![e], seen)
    }

    fn j(
        a: &(Vec<Entry>, VersionVector),
        b: &(Vec<Entry>, VersionVector),
    ) -> (Vec<Entry>, VersionVector) {
        join(Some((&a.0, &a.1)), &b.0, &b.1).unwrap_or_else(|| a.clone())
    }

    #[test]
    fn into_nothing_takes_the_incoming_state() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        assert_eq!(join(None, &x.0, &x.1), Some(x.clone()));
    }

    #[test]
    fn a_descendant_replaces_its_ancestor() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let y = written(file(dot(2, 1), 20, "y", &[dot(1, 1)]));
        let got = j(&x, &y);
        assert_eq!(got, y);
        // and the ancestor never comes back
        assert_eq!(join(Some((&got.0, &got.1)), &x.0, &x.1), None);
    }

    #[test]
    fn concurrent_versions_become_siblings() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let y = written(file(dot(2, 1), 20, "y", &[]));
        let got = j(&x, &y);
        assert_eq!(got.0.len(), 2);
        assert_eq!(got.1, vv(&[dot(1, 1), dot(2, 1)]));
        assert_eq!(winner(&got.0).dot, dot(2, 1), "later hlc wins");
    }

    #[test]
    fn an_edit_beats_a_concurrent_delete_even_if_older() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let d = written(tomb(dot(2, 1), 99, &[]));
        let got = j(&x, &d);
        assert_eq!(winner(&got.0).dot, dot(1, 1));
    }

    #[test]
    fn join_is_idempotent() {
        let x = written(file(dot(1, 1), 10, "x", &[]));
        let y = written(file(dot(2, 1), 20, "y", &[]));
        let s = j(&x, &y);
        assert_eq!(join(Some((&s.0, &s.1)), &s.0, &s.1), None);
    }

    /// The case a single-winner merge gets wrong: B deletes after seeing X, C edits
    /// concurrently, and the winner key is not monotonic along causality.
    #[test]
    fn join_is_associative_and_commutative_with_deletes() {
        let a = written(file(dot(1, 1), 50, "x", &[]));
        let b = written(tomb(dot(2, 1), 60, &[dot(1, 1)]));
        let c = written(file(dot(3, 1), 40, "z", &[]));
        let orders = [
            j(&j(&a, &b), &c),
            j(&a, &j(&b, &c)),
            j(&j(&a, &c), &b),
            j(&j(&c, &a), &b),
            j(&j(&b, &c), &a),
        ];
        for got in &orders[1..] {
            assert_eq!(got, &orders[0]);
        }
        let dots: Vec<Dot> = orders[0].0.iter().map(|e| e.dot).collect();
        assert_eq!(
            dots,
            vec![dot(2, 1), dot(3, 1)],
            "X is superseded; the delete and Z are siblings"
        );
        assert_eq!(winner(&orders[0].0).dot, dot(3, 1));
    }

    /// `join` and `winner` are both `pub`: a join must never return a sibling set
    /// `winner` would panic on.
    #[test]
    fn an_empty_join_never_returns_an_empty_sibling_set() {
        let seen = vv(&[dot(1, 1)]);
        assert_eq!(join(None, &[], &seen), None);
        assert_eq!(join(Some((&[], &seen)), &[], &seen), None);
        let x = written(file(dot(1, 1), 10, "x", &[]));
        for got in [
            join(Some((&x.0, &x.1)), &[], &seen),
            join(Some((&[], &seen)), &x.0, &x.1),
        ]
        .into_iter()
        .flatten()
        {
            assert!(!got.0.is_empty());
        }
    }

    #[test]
    fn needs_copy_only_for_differing_live_losers() {
        let w = file(dot(1, 1), 10, "same", &[]);
        assert!(!needs_copy(&file(dot(2, 1), 5, "same", &[]), &w));
        assert!(!needs_copy(&tomb(dot(2, 1), 5, &[]), &w));
        assert!(needs_copy(&file(dot(2, 1), 5, "other", &[]), &w));
    }

    #[test]
    fn conflict_path_naming() {
        let d = Dot {
            replica: ReplicaId(Uuid::from_u128(0x0190_0000_0000_7000_8000_00ff_ffff_ffff)),
            counter: 42,
        };
        let g = GrainId::MAX.to_string();
        assert_eq!(
            conflict_path("note.md", &d),
            format!("note.conflict-{g}-42.md")
        );
        assert_eq!(
            conflict_path("dir/a.tar.gz", &d),
            format!("dir/a.tar.conflict-{g}-42.gz")
        );
        assert_eq!(
            conflict_path("dir/Makefile", &d),
            format!("dir/Makefile.conflict-{g}-42")
        );
        assert_eq!(
            conflict_path(".sapphireignore", &d),
            format!(".sapphireignore.conflict-{g}-42")
        );
    }
}
