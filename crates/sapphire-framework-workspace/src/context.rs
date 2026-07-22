use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::workspace::path_uuid;

/// Application-wide context shared across all [`Workspace`](crate::Workspace) instances.
///
/// Holds the `app_name` (used for the marker directory) and the cache / data
/// directories.
///
/// This crate intentionally does **not** depend on platform path crates (e.g.
/// `dirs`); the host application resolves the correct directories for its
/// target and injects them via [`set_cache_dir`](Self::set_cache_dir) /
/// [`set_data_dir`](Self::set_data_dir) at startup.
///
/// # Usage
///
/// Declare a `static` instance in your application crate, then initialise the
/// directories before opening any workspace:
///
/// ```rust,ignore
/// use sapphire_workspace::AppContext;
///
/// pub static MY_CTX: AppContext = AppContext::new("my-app");
///
/// fn main() {
///     MY_CTX.set_cache_dir(host_cache_dir);
///     MY_CTX.set_data_dir(host_data_dir);
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
    /// App-specific cache directory.  Set once at startup by the host app via
    /// [`set_cache_dir`](Self::set_cache_dir).
    cache_dir: OnceLock<PathBuf>,
    /// App-specific persistent data directory.  Set once at startup by the
    /// host app via [`set_data_dir`](Self::set_data_dir).
    data_dir: OnceLock<PathBuf>,
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

    /// Set the app cache directory.  Must be called once at startup before
    /// any workspace operation that reads [`cache_dir`](Self::cache_dir).
    /// Subsequent calls are silently ignored (first writer wins).
    pub fn set_cache_dir(&self, path: PathBuf) {
        let _ = self.cache_dir.set(path);
    }

    /// Return the app cache directory.
    ///
    /// # Panics
    /// Panics if [`set_cache_dir`](Self::set_cache_dir) has not been called.
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

    /// Set the app persistent-data directory.  Must be called once at
    /// startup before any workspace operation that reads
    /// [`data_dir`](Self::data_dir).  Subsequent calls are silently ignored
    /// (first writer wins).
    pub fn set_data_dir(&self, path: PathBuf) {
        let _ = self.data_dir.set(path);
    }

    /// Return the app persistent-data directory.
    ///
    /// # Panics
    /// Panics if [`set_data_dir`](Self::set_data_dir) has not been called.
    pub fn data_dir(&self) -> &Path {
        self.data_dir
            .get()
            .map(|p| p.as_path())
            .expect("AppContext::set_data_dir must be called at startup")
    }
}
