//! Two replicas in one process, connected by a duplex, must converge.

use std::path::Path;

use sapphire_framework_session::run_session;
use sapphire_sync::{Replica, ReplicaConfig, SystemClock};

struct Fixture {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    replica: Replica,
}

fn replica(name: &str, device: grain_id::GrainId) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join(name);
    std::fs::create_dir_all(root.join(".test-app")).unwrap();
    let state = tmp.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let config = ReplicaConfig::new("test-app", root.clone(), device, &state);
    let replica = Replica::open(config, std::sync::Arc::new(SystemClock)).unwrap();
    Fixture {
        _tmp: tmp,
        root,
        replica,
    }
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// Run one session between `a` and `b` over an in-process duplex.
async fn sync(a: &mut Replica, b: &mut Replica, ws: grain_id::GrainId) {
    let (left, right) = tokio::io::duplex(64 * 1024);
    let (ra, rb) = (a, b);
    let (x, y) = tokio::join!(run_session(left, ra, ws), run_session(right, rb, ws));
    x.unwrap();
    y.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_written_on_one_side_appears_on_the_other() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "notes/hello.md", "# hello");
    a.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    assert_eq!(
        std::fs::read_to_string(b.root.join("notes/hello.md")).unwrap(),
        "# hello"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_larger_than_the_inline_limit_is_fetched_by_hash() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    let big = "x".repeat(300_000);
    write(&a.root, "big.md", &big);
    a.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    assert_eq!(
        std::fs::read_to_string(b.root.join("big.md"))
            .unwrap()
            .len(),
        300_000
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_session_sends_nothing_new() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "a.md", "one");
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, _) = tokio::join!(
        run_session(left, &mut a.replica, ws),
        run_session(right, &mut b.replica, ws)
    );
    assert_eq!(
        x.unwrap().sent,
        0,
        "nothing new should be sent the second time"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn edits_on_both_sides_both_arrive() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "from-a.md", "a");
    write(&b.root, "from-b.md", "b");
    a.replica.scan().unwrap();
    b.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    assert!(b.root.join("from-a.md").exists());
    assert!(a.root.join("from-b.md").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deletion_propagates() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "doomed.md", "x");
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;
    assert!(b.root.join("doomed.md").exists());

    std::fs::remove_file(a.root.join("doomed.md")).unwrap();
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;

    assert!(!b.root.join("doomed.md").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_edits_leave_a_conflict_copy_rather_than_losing_one() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    write(&a.root, "shared.md", "seed");
    a.replica.scan().unwrap();
    sync(&mut a.replica, &mut b.replica, ws).await;

    // Both edit without seeing the other.
    write(&a.root, "shared.md", "from a");
    write(&b.root, "shared.md", "from b");
    a.replica.scan().unwrap();
    b.replica.scan().unwrap();

    sync(&mut a.replica, &mut b.replica, ws).await;

    let names: Vec<String> = std::fs::read_dir(&b.root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n.contains(".conflict-")),
        "the losing edit must survive as a conflict copy; saw {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_for_a_different_workspace_is_refused() {
    let mut a = replica("a", grain_id::GrainId::random());
    let mut b = replica("b", grain_id::GrainId::random());

    let (left, right) = tokio::io::duplex(64 * 1024);
    let (x, y) = tokio::join!(
        run_session(left, &mut a.replica, grain_id::GrainId::random()),
        run_session(right, &mut b.replica, grain_id::GrainId::random())
    );
    assert!(
        x.is_err() || y.is_err(),
        "mismatched workspaces must not exchange state"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_session_does_not_advance_the_version_vector() {
    let ws = grain_id::GrainId::random();
    let mut a = replica("a", grain_id::GrainId::random());
    let b = replica("b", grain_id::GrainId::random());

    write(&a.root, "a.md", "one");
    a.replica.scan().unwrap();

    // B's side is dropped as soon as it has said Hello, so A never sees Done.
    let (left, right) = tokio::io::duplex(64 * 1024);
    let before = b.replica.vv().clone();
    let cut = tokio::spawn(async move {
        let mut right = right;
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 16];
        let _ = right.read(&mut buf).await;
        drop(right);
    });
    let _ = run_session(left, &mut a.replica, ws).await;
    cut.await.unwrap();

    assert_eq!(
        b.replica.vv(),
        &before,
        "an interrupted session must leave the version vector untouched"
    );
}
