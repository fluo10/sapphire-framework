use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::app_dirs::{AppKind, app_dir_env_var, migrate_app_dir, migrate_keys_to_data};
use crate::workspace::path_uuid;

/// Application-wide context shared across all [`Workspace`](crate::Workspace) instances.
///
/// Holds the `app_name` (used for the marker directory) and the cache / data /
/// config directories.
///
/// [`init`](Self::init) is the single startup entry point that fills them: it
/// resolves the three platform roots with the `dirs` crate (re-exported by the
/// framework facade for apps), applies each category's
/// `SAPPHIRE_<APP-UPPER>_<CATEGORY>_DIR` env override (an env var replaces the
/// *platform root*, never the `<app_name>/<kind>` layer), and applies the
/// per-binary-type layout and one-shot migration of [`crate::app_dirs`] —
/// first writer wins, as with [`set_cache_dir`](Self::set_cache_dir).
///
/// # Usage
///
/// Declare a `static` instance in your application crate, then initialise the
/// directories once at startup before opening any workspace:
///
/// ```rust,ignore
/// use sapphire_workspace::{AppContext, AppKind};
///
/// pub static MY_CTX: AppContext = AppContext::new("my-app");
///
/// fn main() {
///     MY_CTX.init(AppKind::Server);
///     // … run app …
/// }
/// ```
pub struct AppContext {
    /// Application name without a leading dot.  Controls the marker
    /// directory: `{root}/.{app_name}/`.  Shared across all binaries
    /// (CLI, GUI, etc.) that read/write the same workspace format.
    pub app_name: &'static str,
    /// When `true`, file-operation methods on [`WorkspaceState`](crate::WorkspaceState)
    /// accept paths outside the workspace root (absolute paths or relative
    /// paths that traverse above the root).  External files are accessed via
    /// plain `std::fs` operations without updating the retrieve index.
    ///
    /// Default: `false` — any path that resolves outside the workspace root
    /// returns [`Error::PathEscapesWorkspace`](crate::Error::PathEscapesWorkspace).
    allow_external_paths: bool,
    /// App-specific cache directory (per-kind layout applied by
    /// [`init`](Self::init) or set via [`set_cache_dir`](Self::set_cache_dir)).
    cache_dir: OnceLock<PathBuf>,
    /// App-specific persistent data directory (per-kind layout applied by
    /// [`init`](Self::init) or set via [`set_data_dir`](Self::set_data_dir)).
    data_dir: OnceLock<PathBuf>,
    /// App-specific config directory (per-kind layout applied by
    /// [`init`](Self::init) or set via [`set_config_dir`](Self::set_config_dir)).
    config_dir: OnceLock<PathBuf>,
}

impl AppContext {
    /// Create a new context.  This is `const` so it can be used in `static`
    /// initialisers.
    pub const fn new(app_name: &'static str) -> Self {
        Self {
            app_name,
            allow_external_paths: false,
            cache_dir: OnceLock::new(),
            data_dir: OnceLock::new(),
            config_dir: OnceLock::new(),
        }
    }

    /// Initialise the cache, data and config directories from the platform
    /// defaults (`dirs::cache_dir` / `dirs::data_dir` / `dirs::config_dir`,
    /// each falling back to [`std::env::temp_dir`] when unavailable), applying
    /// each category's env override and the per-binary-type layout and
    /// one-shot migration described in [`crate::app_dirs`], and store all
    /// three (first writer wins, as with [`set_cache_dir`](Self::set_cache_dir)).
    ///
    /// Each category's env var (`SAPPHIRE_<APP-UPPER>_CACHE_DIR`, `..._DATA_DIR`,
    /// `..._CONFIG_DIR` — see [`app_dir_env_var`](crate::app_dirs::app_dir_env_var))
    /// replaces the *platform root* only; the `<app_name>/<kind>` layering
    /// always applies on top.
    ///
    /// `keys.toml` migration from the cache tree into the data tree runs once
    /// per workspace (see [`migrate_keys_to_data`](crate::app_dirs::migrate_keys_to_data)),
    /// best-effort — a failure is logged, never fatal at startup.
    pub fn init(&self, kind: AppKind) {
        let cache = self.init_category("cache", kind, dirs::cache_dir());
        let data = self.init_category("data", kind, dirs::data_dir());
        let config = self.init_category("config", kind, dirs::config_dir());
        if let Some(dir) = cache.as_deref() {
            self.set_cache_dir(dir.to_owned());
        }
        if let Some(dir) = data.as_deref() {
            self.set_data_dir(dir.to_owned());
        }
        if let Some(dir) = config.as_deref() {
            self.set_config_dir(dir.to_owned());
        }
        // Once-per-workspace secrets migration, once both trees exist:
        // keys.toml files move from the cache tree into the data tree (the
        // app dir is the parent of each per-kind directory).
        let app_dir =
            |dir: &Option<PathBuf>| dir.as_deref().and_then(|p| p.parent().map(Path::to_owned));
        if let (Some(cache_app), Some(data_app)) = (app_dir(&cache), app_dir(&data))
            && let Err(err) = migrate_keys_to_data(&cache_app, &data_app, kind)
        {
            tracing::warn!("keys.toml cache-to-data migration failed: {err}");
        }
    }

    /// Resolve one category's per-kind directory: env-var override or the
    /// platform root (fallback [`std::env::temp_dir`]), then
    /// [`migrate_app_dir`](crate::app_dirs::migrate_app_dir) on
    /// `<root>/<app_name>`.  `None` (with a warning) when the tree cannot be
    /// prepared.
    fn init_category(
        &self,
        category: &str,
        kind: AppKind,
        platform_root: Option<PathBuf>,
    ) -> Option<PathBuf> {
        let root = std::env::var(app_dir_env_var(self.app_name, category))
            .ok()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .or(platform_root)
            .unwrap_or_else(std::env::temp_dir);
        let app_dir = root.join(self.app_name);
        match migrate_app_dir(&app_dir, kind) {
            Ok(kind_dir) => Some(kind_dir),
            Err(err) => {
                tracing::warn!(
                    "could not prepare the {} directory {}: {err}",
                    category,
                    app_dir.display()
                );
                None
            }
        }
    }

    /// Allow file operations on paths outside the workspace root.
    ///
    /// When enabled, [`WorkspaceState`](crate::WorkspaceState) file methods
    /// accept absolute or traversing-relative paths that resolve outside the
    /// workspace.  External files are handled with plain `std::fs` — no
    /// index updates.
    pub const fn allow_external_paths(mut self) -> Self {
        self.allow_external_paths = true;
        self
    }

    /// Returns `true` if external (out-of-workspace) file access is permitted.
    pub fn allows_external_paths(&self) -> bool {
        self.allow_external_paths
    }

    /// Set the app cache directory directly.  Normally [`init`](Self::init)
    /// sets it; direct injection remains for hosts that resolve storage
    /// themselves.  Subsequent calls are silently ignored (first writer wins).
    pub fn set_cache_dir(&self, path: PathBuf) {
        let _ = self.cache_dir.set(path);
    }

    /// Return the app cache directory.
    ///
    /// # Panics
    /// Panics if neither [`init`](Self::init) nor [`set_cache_dir`](Self::set_cache_dir)
    /// has been called.
    pub fn cache_dir(&self) -> &Path {
        self.cache_dir
            .get()
            .map(|p| p.as_path())
            .expect("AppContext::set_cache_dir must be called at startup")
    }

    /// Compute the cache directory for a workspace rooted at `root`.
    ///
    /// Returns `{cache_dir}/{uuid}/` where `uuid` is the stable UUIDv8
    /// derived from the canonicalized `root` path.
    pub fn cache_dir_for(&self, root: &Path) -> PathBuf {
        self.cache_dir().join(path_uuid(root).to_string())
    }

    /// Return the directory where embedding models should be cached
    /// (`{cache_dir}/models`).
    pub fn model_cache_dir(&self) -> PathBuf {
        self.cache_dir().join("models")
    }

    /// Set the app persistent-data directory directly (see
    /// [`set_cache_dir`](Self::set_cache_dir)).  Subsequent calls are
    /// silently ignored (first writer wins).
    pub fn set_data_dir(&self, path: PathBuf) {
        let _ = self.data_dir.set(path);
    }

    /// Return the app persistent-data directory.
    ///
    /// # Panics
    /// Panics if neither [`init`](Self::init) nor [`set_data_dir`](Self::set_data_dir)
    /// has been called.
    pub fn data_dir(&self) -> &Path {
        self.data_dir
            .get()
            .map(|p| p.as_path())
            .expect("AppContext::set_data_dir must be called at startup")
    }

    /// Set the app config directory directly (see
    /// [`set_cache_dir`](Self::set_cache_dir)).  Subsequent calls are
    /// silently ignored (first writer wins).
    pub fn set_config_dir(&self, path: PathBuf) {
        let _ = self.config_dir.set(path);
    }

    /// Return the app config directory.
    ///
    /// # Panics
    /// Panics if neither [`init`](Self::init) nor [`set_config_dir`](Self::set_config_dir)
    /// has been called.
    pub fn config_dir(&self) -> &Path {
        self.config_dir
            .get()
            .map(|p| p.as_path())
            .expect("AppContext::set_config_dir must be called at startup")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_dirs::AppKind;
    use crate::test_env::TestEnv;
    use tempfile::tempdir;

    #[test]
    fn init_sets_all_three_dirs_under_the_kind_directory() {
        let _env = TestEnv::lock();
        let (cache, data, config) = (tempdir().unwrap(), tempdir().unwrap(), tempdir().unwrap());
        TestEnv::set("SAPPHIRE_TESTJOURNAL_CACHE_DIR", cache.path());
        TestEnv::set("SAPPHIRE_TESTJOURNAL_DATA_DIR", data.path());
        TestEnv::set("SAPPHIRE_TESTJOURNAL_CONFIG_DIR", config.path());
        let ctx: &'static AppContext = Box::leak(Box::new(AppContext::new("sapphire-testjournal")));
        ctx.init(AppKind::Server);
        assert_eq!(
            ctx.cache_dir(),
            cache.path().join("sapphire-testjournal").join("server")
        );
        assert_eq!(
            ctx.data_dir(),
            data.path().join("sapphire-testjournal").join("server")
        );
        assert_eq!(
            ctx.config_dir(),
            config.path().join("sapphire-testjournal").join("server")
        );
    }

    #[test]
    fn init_is_idempotent_first_writer_wins() {
        let _env = TestEnv::lock();
        let (cache, data, config) = (tempdir().unwrap(), tempdir().unwrap(), tempdir().unwrap());
        TestEnv::set("SAPPHIRE_TESTJOURNAL2_CACHE_DIR", cache.path());
        TestEnv::set("SAPPHIRE_TESTJOURNAL2_DATA_DIR", data.path());
        TestEnv::set("SAPPHIRE_TESTJOURNAL2_CONFIG_DIR", config.path());
        let ctx: &'static AppContext =
            Box::leak(Box::new(AppContext::new("sapphire-testjournal2")));
        ctx.init(AppKind::Server);
        ctx.init(AppKind::Cli); // first writer wins — no change
        assert_eq!(
            ctx.cache_dir(),
            cache.path().join("sapphire-testjournal2").join("server")
        );
        assert_eq!(
            ctx.data_dir(),
            data.path().join("sapphire-testjournal2").join("server")
        );
        assert_eq!(
            ctx.config_dir(),
            config.path().join("sapphire-testjournal2").join("server")
        );
    }

    #[test]
    fn init_moves_legacy_shared_uuid_dirs_under_the_kind_directory() {
        let _env = TestEnv::lock();
        let cache = tempdir().unwrap();
        let uuid = "2f1c0000-0000-8000-8000-000000000000";
        std::fs::create_dir_all(cache.path().join("sapphire-testjournal4").join(uuid)).unwrap();
        TestEnv::set("SAPPHIRE_TESTJOURNAL4_CACHE_DIR", cache.path());
        let ctx: &'static AppContext =
            Box::leak(Box::new(AppContext::new("sapphire-testjournal4")));
        ctx.init(AppKind::Cli);
        let kind_dir = cache.path().join("sapphire-testjournal4").join("cli");
        assert_eq!(ctx.cache_dir(), kind_dir);
        assert!(
            kind_dir.join(uuid).is_dir(),
            "shared-layout UUID dir must move under the kind dir"
        );
        assert!(
            !cache
                .path()
                .join("sapphire-testjournal4")
                .join(uuid)
                .exists()
        );
    }

    #[test]
    fn init_moves_keys_toml_from_the_cache_tree_into_the_data_tree() {
        let _env = TestEnv::lock();
        let (cache, data) = (tempdir().unwrap(), tempdir().unwrap());
        let uuid = "2f1c0000-0000-8000-8000-000000000000";
        let cache_uuid = cache
            .path()
            .join("sapphire-testjournal5")
            .join("server")
            .join(uuid);
        std::fs::create_dir_all(&cache_uuid).unwrap();
        std::fs::write(cache_uuid.join("keys.toml"), "secret").unwrap();
        TestEnv::set("SAPPHIRE_TESTJOURNAL5_CACHE_DIR", cache.path());
        TestEnv::set("SAPPHIRE_TESTJOURNAL5_DATA_DIR", data.path());
        let ctx: &'static AppContext =
            Box::leak(Box::new(AppContext::new("sapphire-testjournal5")));
        ctx.init(AppKind::Server);
        let data_uuid = data
            .path()
            .join("sapphire-testjournal5")
            .join("server")
            .join(uuid);
        assert_eq!(
            std::fs::read_to_string(data_uuid.join("keys.toml")).unwrap(),
            "secret"
        );
        assert!(!cache_uuid.join("keys.toml").exists());
    }

    #[test]
    fn cache_dir_for_appends_the_workspace_uuid() {
        let _env = TestEnv::lock();
        let cache = tempdir().unwrap();
        TestEnv::set("SAPPHIRE_TESTJOURNAL3_CACHE_DIR", cache.path());
        let ctx: &'static AppContext =
            Box::leak(Box::new(AppContext::new("sapphire-testjournal3")));
        ctx.init(AppKind::Cli);
        let root = tempdir().unwrap();
        let uuid = crate::path_uuid(root.path()).to_string();
        assert_eq!(
            ctx.cache_dir_for(root.path()),
            cache
                .path()
                .join("sapphire-testjournal3")
                .join("cli")
                .join(uuid)
        );
    }

    #[test]
    fn model_cache_dir_is_under_the_kind_cache_directory() {
        let _env = TestEnv::lock();
        let cache = tempdir().unwrap();
        TestEnv::set("SAPPHIRE_TESTJOURNAL6_CACHE_DIR", cache.path());
        let ctx: &'static AppContext =
            Box::leak(Box::new(AppContext::new("sapphire-testjournal6")));
        ctx.init(AppKind::Desktop);
        assert_eq!(
            ctx.model_cache_dir(),
            cache
                .path()
                .join("sapphire-testjournal6")
                .join("desktop")
                .join("models")
        );
    }
}
