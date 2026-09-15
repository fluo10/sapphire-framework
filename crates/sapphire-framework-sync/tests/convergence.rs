mod common;

use std::collections::BTreeSet;

use common::*;
use proptest::prelude::*;
use sapphire_framework_sync::{Content, ContentHash, Entry, VersionVector};

const PATHS: &[&str] = &["a.txt", "b.txt", "dir/c.txt"];
const BODIES: &[&str] = &["one", "two", "three", "four"];
const MAX_NODES: usize = 4;

#[derive(Clone, Debug)]
enum Op {
    Write {
        node: usize,
        path: usize,
        body: usize,
    },
    Delete {
        node: usize,
        path: usize,
    },
    Push {
        from: usize,
        to: usize,
    },
    /// A session cut short: only the first `take` updates arrive, and no commit.
    PartialPush {
        from: usize,
        to: usize,
        take: usize,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..MAX_NODES, 0..PATHS.len(), 0..BODIES.len())
            .prop_map(|(node, path, body)| Op::Write { node, path, body }),
        1 => (0..MAX_NODES, 0..PATHS.len()).prop_map(|(node, path)| Op::Delete { node, path }),
        3 => (0..MAX_NODES, 0..MAX_NODES).prop_map(|(from, to)| Op::Push { from, to }),
        1 => (0..MAX_NODES, 0..MAX_NODES, 0..4usize)
            .prop_map(|(from, to, take)| Op::PartialPush { from, to, take }),
    ]
}

/// A shared and an exclusive borrow of two different nodes.
fn two(nodes: &mut [Node], from: usize, to: usize) -> (&Node, &mut Node) {
    assert_ne!(from, to);
    if from < to {
        let (left, right) = nodes.split_at_mut(to);
        (&left[from], &mut right[0])
    } else {
        let (left, right) = nodes.split_at_mut(from);
        (&right[0], &mut left[to])
    }
}

type Logical = Vec<(String, Vec<Entry>, VersionVector, Option<ContentHash>)>;

fn logical(node: &Node) -> Logical {
    node.replica
        .states()
        .unwrap()
        .into_iter()
        .map(|(path, s)| (path, s.versions, s.seen, s.disk.hash))
        .collect()
}

fn run(n: usize, ops: &[Op]) -> Result<(), TestCaseError> {
    let mut nodes: Vec<Node> = (1..=n as u64).map(node).collect();
    let mut recorded: Vec<Entry> = Vec::new();

    for op in ops {
        match *op {
            Op::Write { node, path, body } => {
                let i = node % n;
                write(&nodes[i], PATHS[path], BODIES[body]);
                recorded.extend(scan(&mut nodes[i]).recorded);
            }
            Op::Delete { node, path } => {
                let i = node % n;
                remove(&nodes[i], PATHS[path]);
                recorded.extend(scan(&mut nodes[i]).recorded);
            }
            Op::Push { from, to } if from % n != to % n => {
                let (f, t) = two(&mut nodes, from % n, to % n);
                recorded.extend(push(f, t).recorded);
            }
            Op::PartialPush { from, to, take } if from % n != to % n => {
                let (f, t) = two(&mut nodes, from % n, to % n);
                let updates = f.replica.delta_for(t.replica.vv()).unwrap();
                let k = take.min(updates.len());
                recorded.extend(t.replica.apply(&updates[..k], &f.replica).unwrap().recorded);
            }
            _ => {}
        }
    }

    // Everyone syncs with everyone until nothing moves.
    for _ in 0..6 {
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    let (f, t) = two(&mut nodes, i, j);
                    recorded.extend(push(f, t).recorded);
                }
            }
        }
        for node in nodes.iter_mut() {
            recorded.extend(scan(node).recorded);
        }
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    let (source, target) = two(&mut nodes, j, i);
                    recorded.extend(
                        target
                            .replica
                            .fetch_missing(&source.replica)
                            .unwrap()
                            .recorded,
                    );
                }
            }
        }
    }

    // Convergence.
    let reference = logical(&nodes[0]);
    let reference_tree = tree(&nodes[0]);
    for other in &nodes[1..] {
        prop_assert_eq!(logical(other), reference.clone());
        prop_assert_eq!(tree(other), reference_tree.clone());
    }

    // No silent loss.
    let present: BTreeSet<ContentHash> = reference_tree
        .values()
        .map(|body| ContentHash::of_bytes(body.as_bytes()))
        .collect();
    for e in &recorded {
        let Content::File { hash, .. } = e.content else {
            continue;
        };
        let overwritten = recorded
            .iter()
            .any(|later| later.path == e.path && later.context.covers_dot(&e.dot));
        if !overwritten {
            prop_assert!(present.contains(&hash), "version lost: {:?}", e);
        }
    }

    // Idempotence.
    let before = logical(&nodes[1]);
    let (f, t) = two(&mut nodes, 0, 1);
    let everything = f.replica.delta_for(&VersionVector::new()).unwrap();
    let report = t.replica.apply(&everything, &f.replica).unwrap();
    prop_assert_eq!(report.changed, 0);
    prop_assert_eq!(logical(&nodes[1]), before);
    Ok(())
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(48)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn replicas_converge_without_losing_versions(
        n in 2usize..=MAX_NODES,
        ops in prop::collection::vec(op(), 1..30),
    ) {
        run(n, &ops)?;
    }
}

/// The ordering that breaks a single-winner merge (see `merge::tests`), end to end.
#[test]
fn delete_after_edit_concurrent_with_another_edit() {
    let ops = [
        Op::Write {
            node: 0,
            path: 0,
            body: 0,
        },
        Op::Write {
            node: 2,
            path: 0,
            body: 1,
        },
        Op::Push { from: 0, to: 1 },
        Op::Delete { node: 1, path: 0 },
        Op::Push { from: 0, to: 2 },
        Op::Push { from: 1, to: 2 },
    ];
    run(3, &ops).unwrap();
}
