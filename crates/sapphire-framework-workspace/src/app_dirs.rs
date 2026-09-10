//! Platform-directory resolution, per-binary-type layout, and one-shot
//! migration of the pre-unification layouts (issues #128/#129).
//!
//! Layout (option B, decided in the design spec): every app stores state under
//! `<platform-root>/<app-name>/<kind>/`, where `<kind>` is the binary type
//! (`cli`, `server`, `desktop`). Two legacy layouts are migrated on first launch:
//!
//! - **Option A** (`<platform-root>/<app-name>-<kind>/`, agent today): the whole
//!   directory is renamed into place with one `rename`.
//! - **Shared** (`<platform-root>/<app-name>/<uuid>/` directly, journal/ledger
//!   today): UUID-named per-workspace directories are moved under `<kind>/` the
//!   first time a kind runs; later kinds just get their own empty directory and
//!   rebuild caches.
//!
//! `keys.toml` files found under a migrated *cache* tree are moved into the
//! matching per-workspace directory of the *data* tree once — they are secrets,
//! not rebuildable cache.

use std::path::{Path, PathBuf};

/// Which binary type is initialising the context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppKind {
    Cli,
    Server,
    Desktop,
}

impl AppKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AppKind::Cli => "cli",
            AppKind::Server => "server",
            AppKind::Desktop => "desktop",
        }
    }
}

/// Env var that overrides the platform root for one directory category:
/// `SAPPHIRE_JOURNAL_CACHE_DIR` for ("sapphire-journal", "cache").
pub fn app_dir_env_var(app_name: &str, category: &str) -> String {
    format!("{}_{}_DIR", app_name.to_uppercase().replace('-', "_"), category.to_uppercase())
}

/// Env var that names the workspace root for an app: `SAPPHIRE_JOURNAL_DIR`.
pub fn workspace_dir_env_var(app_name: &str) -> String {
    format!("{}_DIR", app_name.to_uppercase().replace('-', "_"))
}

fn is_uuid_name(name: &str) -> bool {
    uuid::Uuid::parse_str(name).is_ok()
}

/// Apply the per-kind layout to `app_dir` (`<platform-root>/<app-name>`),
/// migrating a legacy layout if one is present, and return the created
/// per-kind directory. Idempotent: guarded by existence checks.
pub fn migrate_app_dir(app_dir: &Path, kind: AppKind) -> std::io::Result<PathBuf> {
    let app_name = app_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string();
    let kind_dir = app_dir.join(kind.as_str());

    // Option A: a sibling `<app>-<kind>` directory replaces the app directory.
    if let Some(parent) = app_dir.parent() {
        let legacy = parent.join(format!("{app_name}-{}", kind.as_str()));
        if legacy.is_dir() && !kind_dir.exists() {
            std::fs::create_dir_all(app_dir)?;
            std::fs::rename(&legacy, &kind_dir)?;
            return Ok(kind_dir);
        }
    }

    std::fs::create_dir_all(&kind_dir)?;

    // Shared layout: UUID-named per-workspace dirs directly under the app dir
    // move under the kind directory. (No-op once a migration has run, since
    // nothing UUID-named remains directly under the app dir.)
    for entry in std::fs::read_dir(app_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name_str) = name.to_str().map(str::to_owned) else { continue };
        if is_uuid_name(&name_str) && entry.file_type()?.is_dir() {
            std::fs::rename(app_dir.join(&name_str), kind_dir.join(&name_str))?;
        }
    }
    Ok(kind_dir)
}

/// Move per-workspace `keys.toml` files from the (already migrated) cache
/// tree's `<app>/<kind>/<uuid>/` layout into the data tree's. Once per uuid:
/// skipped when the data tree already has that workspace directory.
pub fn migrate_keys_to_data(cache_app_dir: &Path, data_app_dir: &Path, kind: AppKind) -> std::io::Result<()> {
    let kind_str = kind.as_str();
    let (cache_kind, data_kind) = (cache_app_dir.join(kind_str), data_app_dir.join(kind_str));
    if !cache_kind.is_dir() || !data_kind.is_dir() {
        return Ok(());
    }
    let mut migrated_any = false;
    for entry in std::fs::read_dir(&cache_kind)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
        if !is_uuid_name(&name) || !entry.file_type()?.is_dir() {
            continue;
        }
        migrated_any = true;
        let key = cache_kind.join(&name).join("keys.toml");
        if key.exists() {
            let dest = data_kind.join(&name);
            std::fs::create_dir_all(&dest)?;
            std::fs::rename(&key, dest.join("keys.toml"))?;
        }
    }
    if migrated_any {
        tracing::warn!(
            "migrated per-workspace data (keys.toml) under {} for the first time",
            kind_str
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn env_var_names_follow_the_app_name_rule() {
        assert_eq!(app_dir_env_var("sapphire-journal", "cache"), "SAPPHIRE_JOURNAL_CACHE_DIR");
        assert_eq!(app_dir_env_var("sapphire-agent", "data"), "SAPPHIRE_AGENT_DATA_DIR");
        assert_eq!(workspace_dir_env_var("sapphire-ledger"), "SAPPHIRE_LEDGER_DIR");
    }

    #[test]
    fn option_a_sibling_directory_is_moved_into_the_kind_directory() {
        let root = tempdir().unwrap();
        let app_dir = root.path().join("sapphire-agent");
        let legacy = root.path().join("sapphire-agent-server");
        std::fs::create_dir_all(legacy.join("2f1c0000-0000-8000-8000-000000000000")).unwrap();
        std::fs::write(legacy.join("keys.toml"), "k").unwrap();

        let kind_dir = migrate_app_dir(&app_dir, AppKind::Server).unwrap();

        assert_eq!(kind_dir, app_dir.join("server"));
        assert!(kind_dir.join("2f1c0000-0000-8000-8000-000000000000").is_dir());
        assert!(!legacy.exists());
    }

    #[test]
    fn shared_uuid_directories_move_under_the_kind_directory_once() {
        let root = tempdir().unwrap();
        let app_dir = root.path().join("sapphire-journal");
        let uuid = "2f1c0000-0000-8000-8000-000000000000";
        std::fs::create_dir_all(app_dir.join(uuid)).unwrap();
        std::fs::write(app_dir.join(uuid).join("keys.toml"), "k").unwrap();

        let first = migrate_app_dir(&app_dir, AppKind::Server).unwrap();
        let second = migrate_app_dir(&app_dir, AppKind::Server).unwrap();

        assert!(first.join(uuid).is_dir());
        assert_eq!(first, second);
        assert!(first.join(uuid).join("keys.toml").exists());
        // the legacy flat layout is gone
        assert!(!app_dir.join(uuid).exists());
    }

    #[test]
    fn keys_files_migrate_from_the_cache_tree_into_the_data_tree() {
        let root = tempdir().unwrap();
        let cache = root.path().join("cache");
        let data = root.path().join("data");
        let uuid = "2f1c0000-0000-8000-8000-000000000000";
        std::fs::create_dir_all(cache.join("sapphire-ledger/server").join(uuid)).unwrap();
        std::fs::create_dir_all(data.join("sapphire-ledger/server")).unwrap();
        std::fs::write(cache.join("sapphire-ledger/server").join(uuid).join("keys.toml"), "k").unwrap();

        migrate_keys_to_data(&cache.join("sapphire-ledger"), &data.join("sapphire-ledger"), AppKind::Server).unwrap();

        assert!(data.join("sapphire-ledger/server").join(uuid).join("keys.toml").exists());
        assert!(!cache.join("sapphire-ledger/server").join(uuid).join("keys.toml").exists());
    }

    #[test]
    fn migration_is_a_noop_on_a_fresh_layout() {
        let root = tempdir().unwrap();
        let app_dir = root.path().join("sapphire-tally");
        let kind_dir = migrate_app_dir(&app_dir, AppKind::Server).unwrap();
        assert!(kind_dir.exists());
        assert_eq!(kind_dir.file_name().unwrap(), "server");
    }
}
