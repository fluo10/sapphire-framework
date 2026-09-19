//! A workspace's identity across devices.

use std::path::{Path, PathBuf};

use grain_id::GrainId;

use crate::error::{Error, Result};

/// The file inside the marker directory that names the workspace.
pub const SYNC_ID_FILE: &str = "sync-id";

/// `<root>/.<app_name>/sync-id`.
pub fn sync_id_path(app_name: &str, root: &Path) -> PathBuf {
    root.join(format!(".{app_name}")).join(SYNC_ID_FILE)
}

/// The workspace's sync identity, creating it on first use.
///
/// The identity is a grain-id, not a path-derived value: the same directory on two hosts
/// holds two different [`Workspace::uuid`](sapphire_workspace::Workspace::uuid)s, but must
/// agree on one identity. The file is synced and holds the same bytes on every device, so it
/// never conflicts.
///
/// # Errors
///
/// [`Error::Workspace`] (as
/// [`MarkerDirMissing`](sapphire_workspace::Error::MarkerDirMissing)) when `root` has no
/// `.<app_name>` marker directory, and [`Error::SyncId`] when the file exists but does not
/// hold an id. A missing file is not an error: a fresh id is minted and written.
pub fn sync_id(app_name: &str, root: &Path) -> Result<GrainId> {
    let marker = root.join(format!(".{app_name}"));
    if !marker.is_dir() {
        return Err(Error::Workspace(
            sapphire_workspace::Error::MarkerDirMissing {
                marker: format!(".{app_name}"),
                root: root.to_owned(),
            },
        ));
    }
    let path = sync_id_path(app_name, root);
    match std::fs::read_to_string(&path) {
        Ok(text) => text.trim().parse().map_err(|_| {
            Error::SyncId(format!(
                "{}: the sync-id is unreadable; refusing to mint a new identity for a \
                 workspace that may already exist elsewhere",
                path.display()
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = GrainId::random();
            std::fs::write(&path, format!("{id}\n"))?;
            Ok(id)
        }
        Err(e) => Err(Error::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace(tmp: &std::path::Path) -> std::path::PathBuf {
        let root = tmp.join("ws");
        std::fs::create_dir_all(root.join(".test-app")).unwrap();
        root
    }

    #[test]
    fn an_id_is_created_once_and_read_back() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());

        let first = sync_id("test-app", &root).unwrap();
        let second = sync_id("test-app", &root).unwrap();
        assert_eq!(first, second, "the id must not change between calls");
    }

    #[test]
    fn the_id_lives_in_the_marker_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        let id = sync_id("test-app", &root).unwrap();

        let path = root.join(".test-app").join("sync-id");
        assert_eq!(
            std::fs::read_to_string(path).unwrap().trim(),
            id.to_string()
        );
    }

    #[test]
    fn an_id_that_arrived_by_sync_is_used_as_is() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        let theirs = grain_id::GrainId::random();
        std::fs::write(root.join(".test-app").join("sync-id"), theirs.to_string()).unwrap();

        assert_eq!(sync_id("test-app", &root).unwrap(), theirs);
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        let theirs = grain_id::GrainId::random();
        std::fs::write(
            root.join(".test-app").join("sync-id"),
            format!("  {theirs}\n"),
        )
        .unwrap();

        assert_eq!(sync_id("test-app", &root).unwrap(), theirs);
    }

    #[test]
    fn a_corrupt_id_file_is_an_error_not_a_silent_new_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workspace(tmp.path());
        std::fs::write(root.join(".test-app").join("sync-id"), "not an id!").unwrap();

        let err = sync_id("test-app", &root).unwrap_err();
        assert!(err.to_string().contains("sync-id"), "{err}");
    }

    #[test]
    fn a_missing_marker_directory_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let err = sync_id("test-app", &tmp.path().join("plain")).unwrap_err();
        assert!(err.to_string().contains("test-app"), "{err}");
    }
}
