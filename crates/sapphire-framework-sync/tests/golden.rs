use std::path::{Path, PathBuf};
use std::sync::Arc;

use grain_id::GrainId;
use sapphire_framework_sync::testing::ManualClock;
use sapphire_framework_sync::{FORMAT_VERSION, Replica, ReplicaConfig, ReplicaId, ScanOutcome};

struct Fixed {
    replica: Replica,
    root: PathBuf,
    clock: Arc<ManualClock>,
    _dir: tempfile::TempDir,
}

fn fixed(n: u64) -> Fixed {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join(".test-app")).unwrap();
    let config = ReplicaConfig::new(
        "test-app",
        &root,
        GrainId::from_u64(n).unwrap(),
        &dir.path().join("state"),
    );
    let clock = ManualClock::new(0);
    let id = ReplicaId(uuid::Uuid::from_u128(
        0x0190_0000_0000_7000_8000_0000_0000_0000 + u128::from(n),
    ));
    let replica = Replica::open_with_replica_id(config, clock.clone(), id).unwrap();
    Fixed {
        replica,
        root,
        clock,
        _dir: dir,
    }
}

fn write_at(node: &mut Fixed, now: u64, rel: &str, body: Option<&str>) {
    node.clock.set(now);
    let path = node.root.join(rel);
    match body {
        Some(body) => {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        }
        None => std::fs::remove_file(path).unwrap(),
    }
    assert!(matches!(
        node.replica.scan().unwrap(),
        ScanOutcome::Scanned(_)
    ));
}

fn push(from: &Fixed, to: &mut Fixed, now: u64) {
    to.clock.set(now);
    let from_vv = from.replica.vv().clone();
    let updates = from.replica.delta_for(to.replica.vv()).unwrap();
    to.replica.apply(&updates, &from.replica).unwrap();
    to.replica.commit_session(&from_vv).unwrap();
}

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(format!("format-{FORMAT_VERSION}"))
        .join(name)
}

#[test]
fn concurrent_edit_scenario_matches_the_format_golden() {
    let mut a = fixed(1);
    let mut b = fixed(2);

    write_at(&mut a, 1_000, "a.txt", Some("base"));
    push(&a, &mut b, 1_100);
    write_at(&mut a, 2_000, "a.txt", Some("from a"));
    write_at(&mut b, 3_000, "a.txt", Some("from b"));
    write_at(&mut b, 3_100, "dir/b.txt", Some("only b"));
    push(&a, &mut b, 4_000);
    push(&b, &mut a, 4_100);
    push(&a, &mut b, 4_200);
    write_at(&mut a, 5_000, "dir/b.txt", None);
    push(&a, &mut b, 5_100);

    let actual = serde_json::json!({
        "format_version": FORMAT_VERSION,
        "a": a.replica.logical_dump().unwrap(),
        "b": b.replica.logical_dump().unwrap(),
    });

    let path = golden_path("concurrent-edit.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        return;
    }
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "missing {}; run with UPDATE_GOLDEN=1 once and review it",
                path.display()
            )
        }))
        .unwrap();
    assert_eq!(
        actual, expected,
        "merge behaviour changed for format {FORMAT_VERSION}: fix the regression, or bump FORMAT_VERSION and add a new golden"
    );
}
