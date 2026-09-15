//! Which paths take part in sync.

use std::path::Path;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::error::Result;
use crate::paths;

/// Name of the per-workspace ignore file.
pub const IGNORE_FILE: &str = ".sapphireignore";

/// Declarative sync filter: the built-in rule for an app name plus `.sapphireignore`.
pub struct SyncFilter {
    app_dir: String,
    ignore: Option<Gitignore>,
}

impl SyncFilter {
    /// Build the filter for `root`, reading `.sapphireignore` if present.
    pub fn load(root: &Path, app_name: &str) -> Result<Self> {
        let file = root.join(IGNORE_FILE);
        let ignore = if file.is_file() {
            let mut builder = GitignoreBuilder::new(root);
            if let Some(err) = builder.add(&file) {
                return Err(err.into());
            }
            Some(builder.build()?)
        } else {
            None
        };
        Ok(Self {
            app_dir: format!(".{app_name}"),
            ignore,
        })
    }

    /// Whether `rel` takes part in sync.
    pub fn allows(&self, rel: &str, is_dir: bool) -> bool {
        if !paths::is_valid_rel(rel) {
            return false;
        }
        if rel == IGNORE_FILE {
            return true;
        }
        if !rel
            .split('/')
            .all(|seg| !seg.starts_with('.') || seg == self.app_dir)
        {
            return false;
        }
        match &self.ignore {
            Some(gi) => !gi
                .matched_path_or_any_parents(paths::to_native(Path::new(""), rel), is_dir)
                .is_ignore(),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_with(ignore: Option<&str>) -> (tempfile::TempDir, SyncFilter) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(body) = ignore {
            std::fs::write(dir.path().join(IGNORE_FILE), body).unwrap();
        }
        let f = SyncFilter::load(dir.path(), "test-app").unwrap();
        (dir, f)
    }

    #[test]
    fn built_in_rule() {
        let (_d, f) = filter_with(None);
        assert!(f.allows("notes/a.md", false));
        assert!(f.allows(".test-app", true));
        assert!(f.allows(".test-app/sync-id", false));
        assert!(f.allows("sub/.test-app/x", false));
        assert!(f.allows(IGNORE_FILE, false));
        assert!(!f.allows(".git/config", false));
        assert!(!f.allows("a/.hidden", false));
        assert!(!f.allows(".other-app/x", false));
        assert!(!f.allows("../escape", false));
    }

    #[test]
    fn ignore_file_patterns() {
        let (_d, f) = filter_with(Some("*.tmp\nbuild/\n!keep.tmp\n"));
        assert!(!f.allows("a.tmp", false));
        assert!(f.allows("keep.tmp", false));
        assert!(!f.allows("build", true));
        assert!(!f.allows("build/out.bin", false));
        assert!(f.allows("src/main.rs", false));
    }

    #[test]
    fn the_ignore_file_itself_cannot_be_ignored() {
        let (_d, f) = filter_with(Some("*\n"));
        assert!(f.allows(IGNORE_FILE, false));
        assert!(!f.allows("anything", false));
    }
}
