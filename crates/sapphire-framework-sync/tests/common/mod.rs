#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_framework_sync::testing::ManualClock;
use sapphire_framework_sync::{Replica, ReplicaConfig, Report, ScanOutcome};

pub const APP: &str = "test-app";

pub struct Node {
    pub replica: Replica,
    pub root: PathBuf,
    pub clock: Arc<ManualClock>,
    _dir: tempfile::TempDir,
}

/// A replica with its marker directory in place. `n` seeds the device id and clock.
pub fn node(n: u64) -> Node {
    node_with(n, |_| {})
}

pub fn node_with(n: u64, tweak: impl FnOnce(&mut ReplicaConfig)) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join(format!(".{APP}"))).unwrap();
    let mut config = ReplicaConfig::new(
        APP,
        &root,
        GrainId::from_u64(n).unwrap(),
        &dir.path().join("state"),
    );
    tweak(&mut config);
    let clock = ManualClock::new(1_000_000 * n);
    let replica = Replica::open(config, clock.clone()).unwrap();
    Node {
        replica,
        root,
        clock,
        _dir: dir,
    }
}

/// Close and reopen the replica on the same state directory.
pub fn reopen(node: Node) -> Node {
    let Node {
        replica,
        root,
        clock,
        _dir,
    } = node;
    let config = replica.config().clone();
    drop(replica);
    let replica = Replica::open(config, clock.clone()).unwrap();
    Node {
        replica,
        root,
        clock,
        _dir,
    }
}

pub fn write(node: &Node, rel: &str, body: &str) {
    let path = node.root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

pub fn remove(node: &Node, rel: &str) {
    let _ = std::fs::remove_file(node.root.join(rel));
}

pub fn read(node: &Node, rel: &str) -> Option<String> {
    std::fs::read_to_string(node.root.join(rel)).ok()
}

/// Scan after moving the clock, so successive writes get distinct timestamps.
pub fn scan(node: &mut Node) -> Report {
    node.clock.advance(10);
    match node.replica.scan().unwrap() {
        ScanOutcome::Scanned(report) => report,
        ScanOutcome::Paused(reason) => panic!("unexpectedly paused: {reason:?}"),
    }
}

/// One direction of a session: `to` receives everything `from` has that it lacks.
pub fn push(from: &Node, to: &mut Node) -> Report {
    let from_vv = from.replica.vv().clone();
    let updates = from.replica.delta_for(to.replica.vv()).unwrap();
    let report = to.replica.apply(&updates, &from.replica).unwrap();
    to.replica.commit_session(&from_vv).unwrap();
    report
}

pub fn sync(a: &mut Node, b: &mut Node) {
    push(a, b);
    push(b, a);
}

/// Files under the root (excluding the marker directory) and their contents.
pub fn tree(node: &Node) -> BTreeMap<String, String> {
    let marker = node.root.join(format!(".{APP}"));
    walkdir::WalkDir::new(&node.root)
        .into_iter()
        .map(Result::unwrap)
        .filter(|e| e.file_type().is_file() && !e.path().starts_with(&marker))
        .map(|e| {
            let rel = e
                .path()
                .strip_prefix(&node.root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            (rel, std::fs::read_to_string(e.path()).unwrap())
        })
        .collect()
}
