mod common;

use common::*;
use sapphire_framework_sync::testing::FaultPoint;
use sapphire_framework_sync::{Error, PauseReason, ScanOutcome};

#[test]
fn a_missing_root_pauses_instead_of_deleting_everything() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);

    let parked = a.root.with_file_name("parked");
    std::fs::rename(&a.root, &parked).unwrap();
    assert_eq!(
        a.replica.scan().unwrap(),
        ScanOutcome::Paused(PauseReason::RootMissing)
    );
    let updates = b.replica.delta_for(&Default::default()).unwrap();
    assert!(matches!(
        a.replica.apply(&updates, &b.replica),
        Err(Error::Paused(PauseReason::RootMissing))
    ));
    push(&a, &mut b);
    assert_eq!(
        read(&b, "a.txt").as_deref(),
        Some("one"),
        "the peer keeps its files"
    );

    std::fs::rename(&parked, &a.root).unwrap();
    assert!(scan(&mut a).recorded.is_empty());
}

#[test]
fn a_missing_marker_pauses() {
    let mut a = node(1);
    write(&a, "a.txt", "one");
    scan(&mut a);
    std::fs::remove_dir_all(a.root.join(format!(".{APP}"))).unwrap();
    assert_eq!(
        a.replica.scan().unwrap(),
        ScanOutcome::Paused(PauseReason::MarkerMissing)
    );
}

#[test]
fn fetch_missing_refuses_while_paused() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);

    let parked = a.root.with_file_name("parked");
    std::fs::rename(&a.root, &parked).unwrap();
    assert!(matches!(
        a.replica.fetch_missing(&b.replica),
        Err(Error::Paused(PauseReason::RootMissing))
    ));

    std::fs::rename(&parked, &a.root).unwrap();
}

#[test]
fn an_empty_new_replica_without_a_root_is_not_paused() {
    let a = node(1);
    std::fs::remove_dir_all(&a.root).unwrap();
    let mut a = a;
    assert!(matches!(a.replica.scan().unwrap(), ScanOutcome::Scanned(_)));
}

#[test]
fn an_interrupted_write_is_finished_on_the_next_scan() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);

    b.replica.inject_fault(FaultPoint::AfterCommitBeforeWrite);
    let updates = a.replica.delta_for(b.replica.vv()).unwrap();
    assert!(matches!(
        b.replica.apply(&updates, &a.replica),
        Err(Error::InjectedFault)
    ));
    assert_eq!(read(&b, "a.txt"), None);
    assert!(
        b.replica.state("a.txt").unwrap().is_some(),
        "the state was committed"
    );

    let mut b = reopen(b);
    let report = scan(&mut b);
    assert!(
        report.recorded.is_empty(),
        "the missing file is not mistaken for a local delete"
    );
    assert_eq!(
        read(&b, "a.txt").as_deref(),
        Some("hello"),
        "written from the staged copy"
    );
}

#[test]
fn an_interrupted_delete_is_finished_on_the_next_scan() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);
    sync(&mut a, &mut b);

    remove(&a, "a.txt");
    scan(&mut a);

    b.replica.inject_fault(FaultPoint::AfterCommitBeforeWrite);
    let updates = a.replica.delta_for(b.replica.vv()).unwrap();
    assert!(matches!(
        b.replica.apply(&updates, &a.replica),
        Err(Error::InjectedFault)
    ));
    assert!(
        read(&b, "a.txt").is_some(),
        "the file is not removed before the fault fires"
    );
    let state = b.replica.state("a.txt").unwrap().unwrap();
    assert!(
        state.winner().content.is_tombstone(),
        "the tombstone was committed"
    );

    let mut b = reopen(b);
    let report = scan(&mut b);
    assert!(
        report.recorded.is_empty(),
        "the file still on disk is not mistaken for a local re-creation"
    );
    assert_eq!(read(&b, "a.txt"), None);
}

#[test]
fn a_lost_store_reconverges_without_conflict_copies() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    write(&a, "dir/b.txt", "two");
    scan(&mut a);
    sync(&mut a, &mut b);
    let old_id = a.replica.replica_id();

    let mut a = reset_store(a);
    assert_ne!(a.replica.replica_id(), old_id);
    assert_eq!(
        scan(&mut a).recorded.len(),
        2,
        "every file is recorded again"
    );
    sync(&mut a, &mut b);
    sync(&mut a, &mut b);
    assert_eq!(tree(&a), tree(&b));
    assert_eq!(tree(&a).len(), 2, "identical content never makes a copy");
}

#[cfg(windows)]
#[test]
fn a_path_windows_cannot_hold_is_kept_but_not_written() {
    use grain_id::GrainId;
    use sapphire_framework_sync::testing::MapSource;
    use sapphire_framework_sync::{
        Content, ContentHash, Dot, Entry, Hlc, PathUpdate, SkipReason, VersionVector,
    };

    let mut b = node(2);
    let e = Entry {
        path: "what?.txt".into(),
        content: Content::File {
            hash: ContentHash::of_bytes(b"x"),
            len: 1,
        },
        hlc: Hlc {
            wall_ms: 1,
            logical: 0,
        },
        dot: Dot {
            replica: sapphire_framework_sync::ReplicaId::new(),
            counter: 1,
        },
        context: VersionVector::new(),
        author: GrainId::NIL,
    };
    let mut seen = VersionVector::new();
    seen.add_dot(&e.dot);
    let update = PathUpdate {
        path: e.path.clone(),
        versions: vec![e],
        seen,
    };
    let report = b
        .replica
        .apply(&[update], &MapSource::default().with(b"x"))
        .unwrap();
    assert_eq!(report.skipped[0].reason, SkipReason::Unrepresentable);
    assert!(b.replica.state("what?.txt").unwrap().is_some());
}

#[cfg(any(windows, target_os = "macos"))]
#[test]
fn a_case_only_collision_is_skipped() {
    use grain_id::GrainId;
    use sapphire_framework_sync::testing::MapSource;
    use sapphire_framework_sync::{
        Content, ContentHash, Dot, Entry, Hlc, PathUpdate, SkipReason, VersionVector,
    };

    let mut b = node(2);
    let update = |path: &str, body: &[u8], counter: u64| {
        let e = Entry {
            path: path.into(),
            content: Content::File {
                hash: ContentHash::of_bytes(body),
                len: body.len() as u64,
            },
            hlc: Hlc {
                wall_ms: counter,
                logical: 0,
            },
            dot: Dot {
                replica: sapphire_framework_sync::ReplicaId(uuid::Uuid::from_u128(9)),
                counter,
            },
            context: VersionVector::new(),
            author: GrainId::NIL,
        };
        let mut seen = VersionVector::new();
        seen.add_dot(&e.dot);
        PathUpdate {
            path: path.into(),
            versions: vec![e],
            seen,
        }
    };
    let source = MapSource::default().with(b"upper").with(b"lower");
    let report = b
        .replica
        .apply(
            &[update("A.txt", b"upper", 1), update("a.txt", b"lower", 2)],
            &source,
        )
        .unwrap();
    assert_eq!(
        report.skipped[0].reason,
        SkipReason::CaseCollision {
            other: "A.txt".into()
        }
    );
    assert_eq!(read(&b, "A.txt").as_deref(), Some("upper"));
}
