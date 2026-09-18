//! Which app server owns which workspace on this host.
//!
//! Persisted so the bridge can answer "that workspace lives here, but its server is not
//! running" and can start the owner when a peer asks for it.

use std::path::{Path, PathBuf};

use grain_id::GrainId;
use sapphire_bridge_api::{ManagedBy, WorkspaceRegistration};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// One workspace and the application that owns it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Route {
    /// The workspace's sync identity.
    pub workspace_id: GrainId,
    /// The owning application.
    pub app_name: String,
    /// Where the workspace lives on this host.
    pub root: PathBuf,
    /// The executable to run when the owner is not connected.
    pub exe_path: PathBuf,
    /// How the owner is started. A `Service` owner is never started by the bridge.
    pub managed_by: ManagedBy,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RawTable {
    #[serde(default)]
    route: Vec<Route>,
}

const HEADER: &str = "\
# Which app server owns which workspace on this host.
#
# Written by sapphire-bridge as app servers register. Editing it by hand does nothing
# useful: the owning server rewrites its own rows the next time it connects.
";

/// The routing table.
#[derive(Debug)]
pub struct RouteTable {
    path: PathBuf,
    routes: Vec<Route>,
}

impl RouteTable {
    /// Read the table. A missing file is an empty table and is not created.
    pub fn load(path: &Path) -> Result<RouteTable> {
        let routes = match std::fs::read_to_string(path) {
            Ok(text) => {
                let raw: RawTable = toml::from_str(&text)
                    .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
                raw.route
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(Error::Io(e)),
        };
        let mut table = RouteTable {
            path: path.to_owned(),
            routes,
        };
        table.sort();
        Ok(table)
    }

    /// Replace every route belonging to `app_name`.
    ///
    /// A registration is the app server's complete current list, so anything it no longer
    /// names is no longer owned by it. Other applications' rows are untouched.
    pub fn replace_app(
        &mut self,
        app_name: &str,
        exe_path: PathBuf,
        managed_by: ManagedBy,
        workspaces: &[WorkspaceRegistration],
    ) -> Result<()> {
        for ws in workspaces {
            if let Some(existing) = self.get(ws.workspace_id)
                && existing.app_name != app_name
            {
                return Err(Error::Config(format!(
                    "workspace {} is already owned by {} on this host",
                    ws.workspace_id, existing.app_name
                )));
            }
        }

        let mut next: Vec<Route> = self
            .routes
            .iter()
            .filter(|r| r.app_name != app_name)
            .cloned()
            .collect();
        next.extend(workspaces.iter().map(|ws| Route {
            workspace_id: ws.workspace_id,
            app_name: app_name.to_owned(),
            root: ws.root.clone(),
            exe_path: exe_path.clone(),
            managed_by,
        }));
        self.save(next)
    }

    /// Forget one workspace. `false` if it was not there.
    pub fn remove(&mut self, workspace_id: GrainId) -> Result<bool> {
        if self.get(workspace_id).is_none() {
            return Ok(false);
        }
        let next: Vec<Route> = self
            .routes
            .iter()
            .filter(|r| r.workspace_id != workspace_id)
            .cloned()
            .collect();
        self.save(next)?;
        Ok(true)
    }

    /// The route for one workspace.
    pub fn get(&self, workspace_id: GrainId) -> Option<&Route> {
        self.routes.iter().find(|r| r.workspace_id == workspace_id)
    }

    /// Every route, ordered by application then workspace.
    pub fn entries(&self) -> &[Route] {
        &self.routes
    }

    fn save(&mut self, mut routes: Vec<Route>) -> Result<()> {
        routes.sort_by(|a, b| {
            a.app_name
                .cmp(&b.app_name)
                .then_with(|| a.workspace_id.cmp(&b.workspace_id))
        });
        let body = toml::to_string_pretty(&RawTable {
            route: routes.clone(),
        })
        .map_err(|e| Error::Config(e.to_string()))?;
        write_atomic(&self.path, HEADER, &body)?;
        self.routes = routes;
        Ok(())
    }

    fn sort(&mut self) {
        self.routes.sort_by(|a, b| {
            a.app_name
                .cmp(&b.app_name)
                .then_with(|| a.workspace_id.cmp(&b.workspace_id))
        });
    }
}

/// Write through a temporary file so a crash never leaves a half-written table.
fn write_atomic(path: &Path, header: &str, body: &str) -> Result<()> {
    use std::io::Write;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        "{}.tmp.{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("routes.toml"),
        std::process::id()
    ));
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(header.as_bytes())?;
        file.write_all(b"\n")?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sapphire_bridge_api::WorkspaceRegistration;

    fn reg(root: &str) -> WorkspaceRegistration {
        WorkspaceRegistration {
            workspace_id: GrainId::random(),
            root: root.into(),
        }
    }

    fn table(dir: &std::path::Path) -> RouteTable {
        RouteTable::load(&dir.join("routes.toml")).unwrap()
    }

    #[test]
    fn a_missing_file_is_an_empty_table() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(table(tmp.path()).entries().is_empty());
    }

    #[test]
    fn registered_routes_survive_a_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let a = reg("/home/me/journal");
        t.replace_app(
            "sapphire-journal",
            "/usr/bin/j".into(),
            ManagedBy::Spawned,
            std::slice::from_ref(&a),
        )
        .unwrap();

        let reloaded = table(tmp.path());
        let route = reloaded.get(a.workspace_id).expect("the route");
        assert_eq!(route.app_name, "sapphire-journal");
        assert_eq!(route.root, std::path::PathBuf::from("/home/me/journal"));
        assert_eq!(route.exe_path, std::path::PathBuf::from("/usr/bin/j"));
    }

    #[test]
    fn registering_again_replaces_that_apps_routes_only() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let journal = reg("/j");
        let ledger = reg("/l");
        t.replace_app(
            "sapphire-journal",
            "/usr/bin/j".into(),
            ManagedBy::Spawned,
            std::slice::from_ref(&journal),
        )
        .unwrap();
        t.replace_app(
            "sapphire-ledger",
            "/usr/bin/l".into(),
            ManagedBy::Spawned,
            std::slice::from_ref(&ledger),
        )
        .unwrap();

        // The journal server restarts with a different set.
        let journal2 = reg("/j2");
        t.replace_app(
            "sapphire-journal",
            "/usr/bin/j".into(),
            ManagedBy::Spawned,
            std::slice::from_ref(&journal2),
        )
        .unwrap();

        assert!(
            t.get(journal.workspace_id).is_none(),
            "the old journal route must go"
        );
        assert!(t.get(journal2.workspace_id).is_some());
        assert!(
            t.get(ledger.workspace_id).is_some(),
            "the ledger route must not be touched"
        );
    }

    #[test]
    fn removing_a_route_reports_whether_it_was_there() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let a = reg("/a");
        t.replace_app(
            "app",
            "/usr/bin/app".into(),
            ManagedBy::Spawned,
            std::slice::from_ref(&a),
        )
        .unwrap();

        assert!(t.remove(a.workspace_id).unwrap());
        assert!(!t.remove(a.workspace_id).unwrap());
        assert!(table(tmp.path()).entries().is_empty());
    }

    #[test]
    fn two_apps_cannot_own_the_same_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        let shared = reg("/s");
        t.replace_app(
            "sapphire-journal",
            "/usr/bin/j".into(),
            ManagedBy::Spawned,
            std::slice::from_ref(&shared),
        )
        .unwrap();

        let err = t
            .replace_app(
                "sapphire-ledger",
                "/usr/bin/l".into(),
                ManagedBy::Spawned,
                &[shared],
            )
            .unwrap_err();
        assert!(err.to_string().contains("sapphire-journal"), "{err}");
    }

    #[test]
    fn entries_are_ordered_so_listings_are_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = table(tmp.path());
        t.replace_app(
            "b-app",
            "/b".into(),
            ManagedBy::Spawned,
            &[reg("/b1"), reg("/b2")],
        )
        .unwrap();
        t.replace_app("a-app", "/a".into(), ManagedBy::Spawned, &[reg("/a1")])
            .unwrap();

        let names: Vec<&str> = t.entries().iter().map(|r| r.app_name.as_str()).collect();
        assert_eq!(names, vec!["a-app", "b-app", "b-app"]);
    }
}
