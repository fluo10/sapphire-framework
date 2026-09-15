mod common;

use common::*;
use sapphire_framework_sync::testing::MapSource;
use sapphire_framework_sync::{Content, PathUpdate, SkipReason, VersionVector};

#[test]
fn a_new_file_propagates() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "notes/a.txt", "hello");
    let report = scan(&mut a);
    assert_eq!(report.recorded.len(), 1);
    push(&a, &mut b);
    assert_eq!(read(&b, "notes/a.txt").as_deref(), Some("hello"));
    let sa = a.replica.state("notes/a.txt").unwrap().unwrap();
    let sb = b.replica.state("notes/a.txt").unwrap().unwrap();
    assert_eq!(sa.versions, sb.versions);
    assert_eq!(sa.seen, sb.seen);
}

#[test]
fn an_edit_on_top_of_a_synced_file_supersedes_it() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);
    write(&b, "a.txt", "two");
    scan(&mut b);
    push(&b, &mut a);
    assert_eq!(read(&a, "a.txt").as_deref(), Some("two"));
    assert_eq!(a.replica.state("a.txt").unwrap().unwrap().versions.len(), 1);
    assert_eq!(tree(&a).len(), 1, "no conflict copy");
}

#[test]
fn a_delete_propagates() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);
    remove(&a, "a.txt");
    let report = scan(&mut a);
    assert!(report.recorded[0].content.is_tombstone());
    push(&a, &mut b);
    assert_eq!(read(&b, "a.txt"), None);
    assert!(
        b.replica
            .state("a.txt")
            .unwrap()
            .unwrap()
            .winner()
            .content
            .is_tombstone()
    );
}

#[test]
fn scanning_twice_records_nothing_new() {
    let mut a = node(1);
    write(&a, "a.txt", "one");
    scan(&mut a);
    let again = scan(&mut a);
    assert!(again.recorded.is_empty());
    assert_eq!(again.changed, 0);
    write(&a, "a.txt", "one");
    assert!(
        scan(&mut a).recorded.is_empty(),
        "rewriting identical bytes is not an edit"
    );
}

#[test]
fn nothing_to_send_to_an_up_to_date_peer() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    sync(&mut a, &mut b);
    assert!(a.replica.delta_for(b.replica.vv()).unwrap().is_empty());
    assert!(b.replica.delta_for(a.replica.vv()).unwrap().is_empty());
}

#[test]
fn re_applying_a_full_delta_changes_nothing() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "one");
    scan(&mut a);
    push(&a, &mut b);
    let everything = a.replica.delta_for(&VersionVector::new()).unwrap();
    let report = b.replica.apply(&everything, &a.replica).unwrap();
    assert_eq!(report.changed, 0);
}

#[test]
fn hidden_and_ignored_files_are_not_recorded() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, ".sapphireignore", "*.tmp\n");
    write(&a, "x.tmp", "scratch");
    write(&a, ".git/config", "secret");
    write(&a, "keep.txt", "kept");
    let recorded: Vec<String> = scan(&mut a).recorded.into_iter().map(|e| e.path).collect();
    assert_eq!(
        recorded,
        vec![".sapphireignore".to_string(), "keep.txt".to_string()]
    );
    push(&a, &mut b);
    assert_eq!(read(&b, "x.tmp"), None);
    assert_eq!(read(&b, "keep.txt").as_deref(), Some("kept"));
}

#[test]
fn a_local_file_over_the_size_cap_is_skipped() {
    let mut a = node_with(1, |c| c.max_file_size = 4);
    write(&a, "big.bin", "0123456789");
    let report = scan(&mut a);
    assert!(report.recorded.is_empty());
    assert_eq!(
        report.skipped[0].reason,
        SkipReason::TooLarge { len: 10, max: 4 }
    );
}

#[test]
fn a_remote_file_over_the_size_cap_is_kept_but_not_written() {
    let mut a = node(1);
    let mut b = node_with(2, |c| c.max_file_size = 4);
    write(&a, "big.bin", "0123456789");
    scan(&mut a);
    let report = push(&a, &mut b);
    assert_eq!(
        report.skipped[0].reason,
        SkipReason::TooLarge { len: 10, max: 4 }
    );
    assert!(b.replica.state("big.bin").unwrap().is_some());
    assert_eq!(read(&b, "big.bin"), None);
}

#[test]
fn unavailable_content_is_fetched_later() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);
    let updates = a.replica.delta_for(b.replica.vv()).unwrap();
    let report = b.replica.apply(&updates, &MapSource::default()).unwrap();
    assert_eq!(report.skipped[0].reason, SkipReason::ContentUnavailable);
    assert_eq!(read(&b, "a.txt"), None);
    b.replica.fetch_missing(&a.replica).unwrap();
    assert_eq!(read(&b, "a.txt").as_deref(), Some("hello"));
}

#[test]
fn malformed_updates_are_ignored() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "a.txt", "hello");
    scan(&mut a);
    let mut update: PathUpdate = a.replica.delta_for(b.replica.vv()).unwrap().remove(0);
    update.path = "../escape.txt".into();
    for v in &mut update.versions {
        v.path = update.path.clone();
    }
    let report = b.replica.apply(&[update], &a.replica).unwrap();
    assert_eq!(report.changed, 0);
    assert!(b.replica.states().unwrap().is_empty());

    // A dot with counter 0 has no predecessor for `disk.seen` to pin a loser at.
    let mut zeroed: PathUpdate = a.replica.delta_for(b.replica.vv()).unwrap().remove(0);
    for v in &mut zeroed.versions {
        v.dot.counter = 0;
    }
    let report = b.replica.apply(&[zeroed], &a.replica).unwrap();
    assert_eq!(report.changed, 0);
    assert!(b.replica.states().unwrap().is_empty());
}

#[test]
fn state_survives_reopening() {
    let mut a = node(1);
    write(&a, "a.txt", "hello");
    scan(&mut a);
    let id = a.replica.replica_id();
    let mut a = reopen(a);
    assert_eq!(a.replica.replica_id(), id);
    assert!(scan(&mut a).recorded.is_empty());
    assert!(matches!(
        a.replica.state("a.txt").unwrap().unwrap().winner().content,
        Content::File { len: 5, .. }
    ));
}

#[test]
fn an_incoming_update_leaves_an_oversized_local_file_alone() {
    let mut a = node(1);
    let mut b = node_with(2, |c| c.max_file_size = 8);
    write(&a, "a.txt", "small");
    scan(&mut a);
    push(&a, &mut b);
    write(&b, "a.txt", "this is far too large");
    scan(&mut b);
    write(&a, "a.txt", "tiny");
    scan(&mut a);
    let report = push(&a, &mut b);
    assert_eq!(read(&b, "a.txt").as_deref(), Some("this is far too large"));
    assert!(
        report
            .skipped
            .iter()
            .any(|s| s.path == "a.txt" && s.reason == SkipReason::Occupied),
        "{report:?}"
    );
}

#[test]
fn a_directory_in_the_way_does_not_wedge_the_replica() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "notes", "a file");
    scan(&mut a);
    write(&b, "notes/inner.txt", "a directory here");
    scan(&mut b);
    let report = push(&a, &mut b);
    assert!(
        report.skipped.iter().any(|s| s.path == "notes"),
        "{report:?}"
    );
    assert_eq!(
        read(&b, "notes/inner.txt").as_deref(),
        Some("a directory here")
    );
    write(&b, "later.txt", "still recorded");
    assert!(scan(&mut b).recorded.iter().any(|e| e.path == "later.txt"));
}

#[cfg(unix)]
#[test]
fn symlinks_are_skipped() {
    let mut a = node(1);
    write(&a, "target.txt", "t");
    std::os::unix::fs::symlink(a.root.join("target.txt"), a.root.join("link.txt")).unwrap();
    let report = scan(&mut a);
    assert!(
        report
            .skipped
            .iter()
            .any(|s| s.path == "link.txt" && s.reason == SkipReason::Symlink)
    );
    assert!(report.recorded.iter().all(|e| e.path != "link.txt"));
}

#[cfg(unix)]
#[test]
fn an_unreadable_directory_does_not_abort_the_scan() {
    use std::os::unix::fs::PermissionsExt;
    let mut a = node(1);
    write(&a, "locked/inner.txt", "hidden");
    write(&a, "sibling.txt", "ok");
    let locked = a.root.join("locked");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let report = scan(&mut a);
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        report
            .skipped
            .iter()
            .any(|s| matches!(s.reason, SkipReason::Io(_))),
        "{report:?}"
    );
    assert!(
        report.recorded.iter().any(|e| e.path == "sibling.txt"),
        "the walk continued past the unreadable directory: {report:?}"
    );
}

/// `tolerate_io` is what keeps one bad path from wedging a replica: the path is
/// reported and the rest of the scan still runs.
#[cfg(unix)]
#[test]
fn an_unreadable_file_is_reported_and_the_scan_continues() {
    use std::os::unix::fs::PermissionsExt;
    let mut a = node(1);
    write(&a, "locked.txt", "secret");
    write(&a, "sibling.txt", "ok");
    let locked = a.root.join("locked.txt");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let report = scan(&mut a);
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        report
            .skipped
            .iter()
            .any(|s| s.path == "locked.txt" && matches!(s.reason, SkipReason::Io(_))),
        "{report:?}"
    );
    assert!(
        report.recorded.iter().any(|e| e.path == "sibling.txt"),
        "{report:?}"
    );
    assert!(a.replica.state("locked.txt").unwrap().is_none());
}

#[test]
fn an_ignored_path_is_reported_when_a_peer_sends_it() {
    let (mut a, mut b) = (node(1), node(2));
    write(&a, "build.log", "noise");
    scan(&mut a);
    write(&b, ".sapphireignore", "*.log\n");
    scan(&mut b);
    let report = push(&a, &mut b);
    assert!(
        report
            .skipped
            .iter()
            .any(|s| s.path == "build.log" && s.reason == SkipReason::Ignored),
        "{report:?}"
    );
    assert_eq!(read(&b, "build.log"), None);
}
