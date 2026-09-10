# Framework CLI-args & Directory Unification (#128/#129) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move platform-directory resolution (with per-binary-type layout + legacy migration) and shared CLI/env conventions into `sapphire-framework-workspace`, and re-export `clap`/`serde`/`dirs` from the facade crate, so the four sapphire apps can drop their own `dirs`/`directories` plumbing.

**Architecture:** A new `app_dirs` module in `sapphire-framework-workspace` owns platform-directory resolution (`dirs` re-export), the per-binary-type layout (`<platform-root>/<app-name>/<kind>/`), one-shot migration of the two legacy layouts, and the per-app env-var naming rule. `AppContext::init(kind)` becomes the single startup entry point apps call. `Workspace::resolve`'s env fallback switches to the per-app env name. The facade crate re-exports everything.

**Tech Stack:** Rust 2024, `dirs` 6 (already in `[workspace.dependencies]`), `clap` 4 derive+env (already in `[workspace.dependencies]`), `tempfile` (dev-dep, already present).

**Spec:** `docs/superpowers/specs/2026-09-10-cli-args-dir-unification-design.md`

**Branch:** `feat/issues-128-129-cli-dir-unification` (already created from `origin/main`; the spec commit is its base).

## Global Constraints

- Code, comments, commit messages, and tests are written in **English** (CONTRIBUTING.md).
- New comments in English; Japanese comments in touched files may be converted opportunistically (boy-scout rule), never required.
- All tests: `cargo test --workspace --all-targets` must stay green; `cargo clippy --workspace --all-targets -- -D warnings` clean.
- Breaking changes (per the spec) are accepted for this change set; the deprecation window applies only to CLI flag/env **names**, not to Rust APIs.
- App names are fixed: `sapphire-journal`, `sapphire-ledger`, `sapphire-tally`, `sapphire-agent`.

## Design decisions locked (from spec review, already approved)

- Layout **option B**: `<platform-root>/<app-name>/<kind>/` where kind ∈ {`cli`, `server`, `desktop`}.
- Migration is a one-shot `std::fs::rename` (fallback: copy contents + delete old), guarded so it is idempotent.
- `keys.toml` migrates from the cache tree into the per-workspace **data** directory.
- Env names: `{APP_UPPER}_CACHE_DIR` / `_DATA_DIR` / `_CONFIG_DIR` (app-name derived, e.g. `SAPPHIRE_JOURNAL_CACHE_DIR`); workspace-root env name `{APP_UPPER}_DIR` (e.g. `SAPPHIRE_JOURNAL_DIR`); legacy `SAPPHIRE_WORKSPACE_DIR` still honored with a deprecation `warn!`.
- Env override replaces the **platform root** only; the `<app>/<kind>` layering always applies.
- `AppContext::init` sets cache **and** data (and config only where the app needs it via `set_config_dir`); agent's four cache sub-directories hang off the per-kind cache dir (app-side concern, not framework).

---

### Task 1: `app_dirs` module — AppKind, env names, migration

**Files:**
- Create: `crates/sapphire-framework-workspace/src/app_dirs.rs`
- Modify: `crates/sapphire-framework-workspace/src/lib.rs` (add `pub mod app_dirs; pub use app_dirs::AppKind;`)
- Modify: `crates/sapphire-framework-workspace/Cargo.toml` (add `dirs.workspace = true`)
- Test: `crates/sapphire-framework-workspace/src/app_dirs.rs` (inline `#[cfg(test)] mod tests`, tempfile-based)

**Interfaces:**
- Produces: `AppKind` (`Cli`/`Server`/`Desktop`, `as_str()` → `"cli"|"server"|"desktop"`, derives `Debug, Clone, Copy, PartialEq, Eq`), `app_dir_env_var(app_name: &str, category: &str) -> String`, `workspace_dir_env_var(app_name: &str) -> String`, `migrate_app_dir(app_dir: &Path, kind: AppKind) -> std::io::Result<PathBuf>` (returns the created per-kind dir). Task 2's `AppContext::init` consumes all of these.

- [ ] **Step 1: Write the failing tests** — inline tests in `app_dirs.rs`:

```rust
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
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sapphire-framework-workspace app_dirs`
Expected: compile error (`app_dirs` missing).

- [ ] **Step 3: Implement `app_dirs.rs`**

```rust
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
```

- [ ] **Step 4: Implement the module wiring** — `lib.rs` add:

```rust
pub mod app_dirs;
pub use app_dirs::AppKind;
```

`Cargo.toml`: add `dirs.workspace = true` to `[dependencies]`.

- [ ] **Step 5: Run tests, commit**

Run: `cargo test -p sapphire-framework-workspace app_dirs` → PASS, then `cargo clippy -p sapphire-framework-workspace --all-targets -- -D warnings` → clean.

```bash
git add crates/sapphire-framework-workspace
git commit -m "feat(workspace): app_dirs module — per-kind layout, migration, env-name rule (#129, #128)"
```

---

### Task 2: `AppContext::init` + `Workspace::resolve` env unification

**Files:**
- Modify: `crates/sapphire-framework-workspace/src/context.rs` (replace the "does not depend on dirs" doc note, add `config_dir`/`set_config_dir`/`init`, rework doc example)
- Modify: `crates/sapphire-framework-workspace/src/workspace.rs` (`resolve` env fallback: per-app env name first, `SAPPHIRE_WORKSPACE_DIR` kept with a deprecation warning)
- Test: inline `#[cfg(test)] mod tests` in `context.rs` and `workspace.rs`

**Interfaces:**
- Consumes: Task 1's `AppKind`, `app_dir_env_var`, `migrate_app_dir`, `migrate_keys_to_data`, `workspace_dir_env_var`.
- Produces: `AppContext::init(&'static self, kind: AppKind)` (sets cache+data via the new layout, idempotent), `AppContext::config_dir() -> &Path`, `AppContext::set_config_dir(PathBuf)`, `Workspace::resolve(ctx, explicit)` unchanged signature, new env resolution order `explicit → {APP}_DIR → SAPPHIRE_WORKSPACE_DIR (warn) → cwd`. Task 4/5 (app PRs) call `CTX.init(AppKind::Server)`.

- [ ] **Step 1: Write failing tests** — `context.rs` tests (one `AppContext` per test via `Box::leak`, following the existing `workspace.rs` test pattern):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_dirs::AppKind;
    use tempfile::tempdir;

    #[test]
    fn init_sets_cache_and_data_under_the_kind_directory() {
        let cache = tempdir().unwrap();
        let data = tempdir().unwrap();
        let ctx: &'static AppContext = Box::leak(Box::new(AppContext::new("sapphire-journal")));
        ctx.init(cache.path(), data.path(), AppKind::Server);
        assert_eq!(ctx.cache_dir(), cache.path().join("sapphire-journal").join("server"));
        assert_eq!(ctx.data_dir(), data.path().join("sapphire-journal").join("server"));
        // Idempotent: a second init never overwrites (first writer wins).
        ctx.init(tempdir().unwrap().path(), tempdir().unwrap().path(), AppKind::Cli);
        assert_eq!(ctx.cache_dir(), cache.path().join("sapphire-journal").join("server"));
    }

    #[test]
    fn cache_dir_for_appends_the_workspace_uuid() {
        let cache = tempdir().unwrap();
        let data = tempdir().unwrap();
        let root = tempdir().unwrap();
        let ctx: &'static AppContext = Box::leak(Box::new(AppContext::new("sapphire-ledger")));
        ctx.init(cache.path(), data.path(), AppKind::Cli);
        let uuid = crate::path_uuid(root.path()).to_string();
        assert_eq!(ctx.cache_dir_for(root.path()), cache.path().join("sapphire-ledger").join("cli").join(uuid));
    }
}
```

And `workspace.rs` tests appended to the existing `registry_path_tests` module (or a new module): resolve precedence with an explicit path beats the env var; when the env var `{APP}_DIR` names a marker-bearing dir, resolve returns it.

- [ ] **Step 2: Run — expect compile failure (`init`/`config_dir` missing)**

Run: `cargo test -p sapphire-framework-workspace context` → FAIL.

- [ ] **Step 3: Implement in `context.rs`** — replace the "does not depend on platform path crates" doc paragraph (the dirs-independence policy is retired by this issue); add:

```rust
    /// App-local config directory (`<platform-config>/<app>/<kind>`), set by
    /// [`init`](Self::init) or [`set_config_dir`](Self::set_config_dir).
    config_dir: OnceLock<PathBuf>,
```

```rust
/// Initialise cache and data directories from the platform defaults, applying
/// the per-binary-type layout and one-shot migration (see [`crate::app_dirs`]).
///
/// `cache_root` / `data_root` are the *platform* roots — the apps pass the
/// `dirs`-resolved root or an env override; `init` appends `<app_name>/<kind>`
/// itself and creates the leaf directory. First writer wins, as with
/// [`set_cache_dir`](Self::set_cache_dir).
pub fn init(&self, cache_root: &Path, data_root: &Path, kind: AppKind) {
    let app = |root: &Path| root.join(self.app_name);
    let cache = crate::app_dirs::migrate_app_dir(&app(cache_root), kind);
    let data = crate::app_dirs::migrate_app_dir(&app(data_root), kind);
    if let (Ok(cache), Ok(data)) = (&cache, &data) {
        let _ = crate::app_dirs::migrate_keys_to_data(
            &app(cache_root), &app(data_root), kind,
        );
        let _ = self.cache_dir.set(cache.clone());
        let _ = self.data_dir.set(data.clone());
    }
}
```

(plus `set_config_dir`/`config_dir` mirroring the existing cache/data pair; the config tree is **not** per-kind — the agent stores `config.toml` directly in `<platform-config>/<app>/`, so `config_dir()` returns `<platform-config>/<app>/` as-is; document that). Update the struct's module doc example to use `init`.

`workspace.rs` `resolve`: replace the hard-coded env read with:

```rust
        } else if let Ok(val) = std::env::var(crate::app_dirs::workspace_dir_env_var(ctx.app_name)) {
            // … existing non-empty handling …
        } else if let Ok(val) = std::env::var("SAPPHIRE_WORKSPACE_DIR") {
            tracing::warn!("SAPPHIRE_WORKSPACE_DIR is deprecated; set {} instead", crate::app_dirs::workspace_dir_env_var(ctx.app_name));
            // … same handling …
        } else {
```

- [ ] **Step 4: Run tests + clippy — PASS** (run with `--test-threads=1` only if env tests race).

- [ ] **Step 5: Commit** `feat(workspace): AppContext::init with per-kind layout + migration; per-app workspace-dir env (#129)`

---

### Task 3: `WorkspaceArgs` + `clap`/`serde` re-exports + facade passthrough

**Files:**
- Create: `crates/sapphire-framework-workspace/src/args.rs`
- Modify: `crates/sapphire-framework-workspace/src/lib.rs` (`pub mod args; pub use args::WorkspaceArgs; pub use clap; pub use serde; pub use dirs;`)
- Modify: `crates/sapphire-framework-workspace/Cargo.toml` (add `clap.workspace = true` — the workspace dep already carries `derive` + `env` features)
- Modify: `crates/sapphire-framework/src/lib.rs` (facade re-exports)
- Test: inline tests in `args.rs`

**Interfaces:**
- Consumes: Task 2's per-app env rule (env resolution itself stays app-side: the framework provides the canonical flag definition and the env-name helper).
- Produces: `sapphire_framework::clap`, `sapphire_framework::serde`, `sapphire_framework::dirs`, `sapphire_framework::workspace::WorkspaceArgs` (clap `Args` struct: single field `workspace_dir: Option<PathBuf>`, `#[arg(long = "workspace-dir", alias = "journal-dir", alias = "ledger-dir", alias = "data-dir", global = true, value_name = "DIR")]`).

- [ ] **Step 1: Write failing tests** — `args.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(flatten)]
        args: WorkspaceArgs,
    }

    #[test]
    fn canonical_flag_and_legacy_aliases_all_parse() {
        for flag in ["--workspace-dir", "--journal-dir", "--ledger-dir", "--data-dir"] {
            let parsed = Probe::try_parse_from(["app", flag, "/tmp/x"]).unwrap();
            assert_eq!(parsed.args.workspace_dir.unwrap(), std::path::PathBuf::from("/tmp/x"), "{flag}");
        }
    }

    #[test]
    fn absent_flag_is_none() {
        assert!(Probe::try_parse_from(["app"]).unwrap().args.workspace_dir.is_none());
    }
}
```

- [ ] **Step 2: Verify FAIL** — `cargo test -p sapphire-framework-workspace args`.
- [ ] **Step 3: Implement** `args.rs`:

```rust
//! The shared workspace-directory CLI argument (issue #128).
//!
//! Apps embed this with `#[command(flatten)]`. The flag is uniform
//! (`--workspace-dir`); the per-app historical names are accepted as clap
//! aliases for one deprecation cycle. Env resolution stays in the caller —
//! pass this value to `Workspace::resolve`, which applies the per-app
//! `SAPPHIRE_<APP>_DIR` fallback (and the deprecated `SAPPHIRE_WORKSPACE_DIR`).

use std::path::PathBuf;

/// Shared `--workspace-dir` argument: the explicit workspace root, overriding
/// the upward marker search.
#[derive(clap::Args)]
pub struct WorkspaceArgs {
    /// Workspace root directory. Overrides the automatic upward search.
    #[arg(
        long = "workspace-dir",
        alias = "journal-dir",
        alias = "ledger-dir",
        alias = "data-dir",
        global = true,
        value_name = "DIR"
    )]
    pub workspace_dir: Option<PathBuf>,
}
```

`lib.rs` (workspace crate) — add re-exports of the three dependencies and the new items; `Cargo.toml` add `clap.workspace = true`. Facade `crates/sapphire-framework/src/lib.rs`: inside `pub mod workspace` re-export surface, ensure `AppKind`/`WorkspaceArgs` reach consumers (`pub use crate::workspace::{AppKind, WorkspaceArgs}` inside `prelude` under the `workspace` feature).

- [ ] **Step 4: `cargo test -p sapphire-framework-workspace` + `cargo check -p sapphire-framework --all-features` PASS; clippy clean.**
- [ ] **Step 5: Commit** `feat(workspace): WorkspaceArgs + clap/serde/dirs re-exports; facade passthrough (#128)`

---

### Task 4: documentation & release prep

**Files:**
- Modify: `docs/ARCHITECTURE.md` (the "dirs 非依存" policy note is retired; document the per-kind layout and migration once)
- Modify: `README.md` / `README.ja.md` if they document `AppContext` injection usage (check; only touch if applicable)

- [ ] **Step 1:** Update `docs/ARCHITECTURE.md`: replace the dirs-independence bullet with the new policy (dirs re-exported; per-binary-type layout `~/.cache/<app>/<kind>/`; one-shot migration of the two legacy layouts; `keys.toml` → data tree). Add the env-name rule (`SAPPHIRE_<APP>_DIR` unified; old `*_SERVER_*` names deprecated for one cycle).
- [ ] **Step 2:** `cargo test --workspace --all-targets` and `cargo clippy --workspace --all-targets -- -D warnings` — all green.
- [ ] **Step 3:** Commit `docs: document per-kind directory layout and CLI conventions (#128, #129)` and push the branch.

---

## Out of scope (follow-up plans, after this releases as v0.15.0)

- The four app migration PRs (journal/ledger/tally/agent switching to `init(AppKind::…)`, `WorkspaceArgs`, unified env names, agent's option-A migration riding along for free) — one plan per repo after the crates.io release.
- mobile/wasm platform backends.
- Removing the deprecated aliases/env names after one release cycle (follow-up commit referencing #128/#129).
