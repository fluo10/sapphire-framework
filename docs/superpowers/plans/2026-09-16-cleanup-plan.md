# Cleanup: remove the old sync stack and the per-kind directories — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Take out what the new architecture replaced — the HTTP sync stack and the per-kind
directory split — and leave the repository describing what it now is.

**Architecture:** Four removals and one reversal, in an order that keeps the workspace
compiling at every commit: lift `KeyStore` out of the crate that is going away, delete the four
crates, undo #129's per-kind directories with a migration that never destroys anything, then
tidy the facade and rewrite the parts of `ARCHITECTURE.md` that still describe the old world.

**Tech Stack:** Rust 2024 (toolchain 1.98.0). No new dependencies; this plan only removes them.

**Spec:** `docs/superpowers/specs/2026-09-16-process-architecture-design.md` §6 (crate layout)
and §7 (reverting the per-kind split); `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md`
§1 ("Removed"). Implementation order step 11 of the process-architecture spec's §9.

**Depends on:** every other step. Nothing here may run while something still uses what it
removes.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass **after every task** — this
  plan is a sequence of removals, and a task that leaves the workspace red has been done in the
  wrong order.
- Every public item carries a doc comment.
- **The migration in Task 3 deletes nothing.** It moves, and where it cannot move safely it
  leaves both copies and says so.
- Commit `Cargo.lock` whenever dependencies change; several of these tasks remove large
  subtrees from it.

## The order, and why

```
1. Extract KeyStore          <- -remote-server holds something still wanted
2. Delete the four crates    <- nothing depends on them once 1 is done
3. Undo the per-kind split   <- independent of 1 and 2, but noisier; do it alone
4. Facade features           <- follows 2 and 3
5. Rewrite ARCHITECTURE.md   <- last, when there is nothing left to be wrong about
```

Task 1 before Task 2 because `KeyStore` lives in a crate about to be deleted and is the only
thing in it anyone still wants. Task 3 alone because it touches every application's data on
disk and a failure there should not be tangled up with a crate removal.

---

### Task 1: Extract `sapphire-framework-keys`

**Files:**
- Create: `crates/sapphire-framework-keys/{Cargo.toml,src/lib.rs,src/error.rs}`
- Move: `crates/sapphire-framework-remote-server/src/{keys.rs,auth.rs}` → the new crate
- Modify: `Cargo.toml` (workspace `members`), facade
- Test: the tests that move with the code, plus the new ones below

**Interfaces:**
- Produces:
  - `KeyEntry { token, id, label, created_at, expires_at }`
  - `KeyStore`: `load(path) -> Result<KeyStore>`, `generate(prefix, label, expires_at)`,
    `revoke(selector)`, `entries()`, `authenticate(token) -> Option<&KeyEntry>`
  - `protect(store: Arc<KeyStore>, router: axum::Router) -> axum::Router` (feature `axum`)
  - `Authenticated { key_id: Uuid, label: Option<String> }`

**Why this survives at all:** sync no longer uses HTTP, but the apps' other HTTP endpoints do
— `sapphire-agent`'s `/mcp`, `/acp` and `/a2a`. Those still need a bearer token checked before
the router sees the request. Framework issue **#103** tracks this extraction.

- [ ] **Step 1: Move the code**

```bash
git mv crates/sapphire-framework-remote-server/src/keys.rs crates/sapphire-framework-keys/src/keys.rs
git mv crates/sapphire-framework-remote-server/src/auth.rs crates/sapphire-framework-keys/src/auth.rs
```

Use `git mv` so the history follows the file. Write the manifest with `axum` behind a feature,
so a caller that only wants `KeyStore` does not link a web framework:

```toml
[features]
default = []
axum = ["dep:axum"]

[dependencies]
axum = { workspace = true, optional = true }
base64.workspace = true
chrono = { workspace = true }
getrandom.workspace = true
serde.workspace = true
thiserror.workspace = true
toml.workspace = true
tracing.workspace = true
uuid = { workspace = true }
```

- [ ] **Step 2: Write the tests that pin what must not change in the move**

```rust
#[cfg(test)]
mod extraction_tests {
    use super::*;

    #[test]
    fn a_key_file_written_by_the_old_crate_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        std::fs::write(
            &path,
            r#"
[[key]]
token = "sjt_abc123"
id = "3f2a4b5c-6d7e-4f80-9112-233445566778"
label = "laptop"
created_at = "2026-08-25T00:00:00Z"
"#,
        )
        .unwrap();

        let store = KeyStore::load(&path).unwrap();
        assert!(store.authenticate("sjt_abc123").is_some());
        assert_eq!(store.entries()[0].label.as_deref(), Some("laptop"));
    }

    #[test]
    fn a_key_with_only_a_token_is_completed_and_written_back() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        std::fs::write(&path, "[[key]]\ntoken = \"sjt_xyz\"\n").unwrap();

        let store = KeyStore::load(&path).unwrap();
        assert!(store.entries()[0].created_at.timestamp() > 0);

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("created_at"), "the completion must be written back:\n{text}");
    }

    #[test]
    fn an_expired_key_does_not_authenticate_but_stays_in_the_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        std::fs::write(
            &path,
            "[[key]]\ntoken = \"sjt_old\"\nexpires_at = \"2020-01-01T00:00:00Z\"\n",
        )
        .unwrap();

        let store = KeyStore::load(&path).unwrap();
        assert!(store.authenticate("sjt_old").is_none());
        assert_eq!(
            store.entries().len(),
            1,
            "a key that vanished would leave nobody able to see why they cannot connect"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_generated_key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.toml");
        let mut store = KeyStore::load(&path).unwrap();
        store.generate("sjt", Some("laptop".into()), None).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "mode was {:o}", mode & 0o777);
    }

    #[test]
    fn the_crate_does_not_pull_in_axum_by_default() {
        let manifest = include_str!("../Cargo.toml");
        assert!(
            manifest.contains("axum = { workspace = true, optional = true }"),
            "a caller that only wants KeyStore should not link a web framework"
        );
    }
}
```

`a_key_file_written_by_the_old_crate_still_loads` is the point of writing tests for a move at
all: every existing deployment has one of these files, and a silent change to the format would
lock people out of their own servers.

- [ ] **Step 3: Point the remaining users at the new crate, verify, commit**

```bash
cargo test --all-features --locked
git add crates Cargo.toml Cargo.lock
git commit -m "refactor(keys)!: lift KeyStore out of the remote server crate (#103)"
```

---

### Task 2: Delete the HTTP sync stack

**Files:**
- Delete: `crates/sapphire-framework-rpc/`
- Delete: `crates/sapphire-framework-remote-client/`
- Delete: `crates/sapphire-framework-remote-server/`
- Delete: `crates/sapphire-framework-blob/`
- Modify: `Cargo.toml`, `crates/sapphire-framework-backend/{Cargo.toml,src/*}`, facade,
  `release-plz.toml`

**What goes, and why it is safe now:**

| Crate | Replaced by |
|---|---|
| `-rpc` | `-ipc` for local calls, `-session` for replication |
| `-remote-client` | `IpcBackend` for local, the session for peers |
| `-remote-server` | the app server and the bridge |
| `-blob` | content is served from the origin; the session carries it (sync spec §2.3) |

Also gone: `RemoteBackend`, `Error::Remote`, `Error::Conflict`, `WorkspaceLocator`'s URL form,
and the search RPC (`search.fts` / `search.semantic`) — every node has its own index now.

- [ ] **Step 1: Write the test that will fail if any of it comes back**

`crates/sapphire-framework/tests/surface.rs`:

```rust
//! What the framework is made of, stated where a reviewer will see it.

#[test]
fn the_http_sync_stack_is_gone() {
    let manifest = include_str!("../../../Cargo.toml");
    for gone in [
        "sapphire-framework-rpc",
        "sapphire-framework-remote-client",
        "sapphire-framework-remote-server",
        "sapphire-framework-blob",
    ] {
        assert!(
            !manifest.contains(gone),
            "{gone} was replaced by the process architecture; see \
             docs/superpowers/specs/2026-09-16-process-architecture-design.md §6"
        );
    }
}

#[test]
fn the_crates_that_replaced_it_are_present() {
    let manifest = include_str!("../../../Cargo.toml");
    for present in [
        "sapphire-framework-ipc",
        "sapphire-framework-server",
        "sapphire-framework-session",
        "sapphire-framework-bridge",
        "sapphire-framework-keys",
    ] {
        assert!(manifest.contains(present), "{present} is missing from the workspace");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p sapphire-framework --test surface`
Expected: FAIL — the crates are still listed.

- [ ] **Step 3: Delete**

```bash
git rm -r crates/sapphire-framework-rpc \
          crates/sapphire-framework-remote-client \
          crates/sapphire-framework-remote-server \
          crates/sapphire-framework-blob
```

Then remove their `members` entries, their dependencies from `-backend` (and delete
`-backend/src/remote.rs`, the `Remote` and `Conflict` error variants, and the `RemoteClient`
re-export), and their `release-plz.toml` entries if any name them.

`WorkspaceLocator` becomes `Path`-only, as the sync spec's §1 already recorded. Its `url` form
and `WorkspaceSource::Remote` go with it.

- [ ] **Step 4: Verify and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add -A
git commit -m "refactor!: remove the HTTP sync stack

Replaced by the process architecture: -ipc and -server for local calls,
-session and -bridge for replication. Content is served from the origin, so
-blob has no user left."
```

---

### Task 3: Undo the per-kind directory split

**Files:**
- Modify: `crates/sapphire-framework-workspace/src/{app_dirs.rs,context.rs}`
- Test: inline `#[cfg(test)] mod tests` in `app_dirs.rs`

**What changes (spec §7):**

- Cache returns to `<platform cache root>/<app>/<uuid>/`; data and config to
  `<platform data root>/<app>/` and `<platform config root>/<app>/`.
- `AppKind` survives as a description of what a process does. **It no longer appears in a path.**
- The environment overrides keep their #129 names and still replace only the platform root.
- A **second one-shot migration** moves `<app>/<kind>/…` back up to `<app>/…`.

**Why the split can go:** it existed so that a desktop app and a server would not open one
database. The server is now the only process that opens one, so the split has no remaining
purpose — and it never solved the case that mattered, since a CLI invocation and the stdio MCP
server were both `cli`.

**The migration deletes nothing.** Where two kinds both left a cache for one workspace, the
server's copy wins and the others are left where they are, with a warning naming them. A cache
is rebuildable; a directory silently removed is not.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod unsplit_tests {
    use super::*;

    /// Build `<root>/<app>/<kind>/<uuid>/` with a file in it.
    fn seed(root: &std::path::Path, app: &str, kind: &str, uuid: &str, file: &str) {
        let dir = root.join(app).join(kind).join(uuid);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(file), b"x").unwrap();
    }

    #[test]
    fn a_per_kind_directory_moves_up_one_level() {
        let tmp = tempfile::tempdir().unwrap();
        seed(tmp.path(), "sapphire-journal", "server", "ws-1", "docs.redb");

        let app_dir = unsplit_app_dir(&tmp.path().join("sapphire-journal")).unwrap();

        assert_eq!(app_dir, tmp.path().join("sapphire-journal"));
        assert!(app_dir.join("ws-1").join("docs.redb").exists());
        assert!(
            !app_dir.join("server").join("ws-1").exists(),
            "the moved directory must not be left behind as well"
        );
    }

    #[test]
    fn migrating_twice_changes_nothing_the_second_time() {
        let tmp = tempfile::tempdir().unwrap();
        seed(tmp.path(), "sapphire-journal", "server", "ws-1", "docs.redb");
        let app_dir = tmp.path().join("sapphire-journal");

        unsplit_app_dir(&app_dir).unwrap();
        unsplit_app_dir(&app_dir).unwrap();

        assert!(app_dir.join("ws-1").join("docs.redb").exists());
    }

    #[test]
    fn the_server_copy_wins_when_two_kinds_left_one() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("sapphire-journal");
        std::fs::create_dir_all(app_dir.join("cli").join("ws-1")).unwrap();
        std::fs::write(app_dir.join("cli").join("ws-1").join("mark"), b"cli").unwrap();
        std::fs::create_dir_all(app_dir.join("server").join("ws-1")).unwrap();
        std::fs::write(app_dir.join("server").join("ws-1").join("mark"), b"server").unwrap();

        unsplit_app_dir(&app_dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(app_dir.join("ws-1").join("mark")).unwrap(),
            "server"
        );
    }

    #[test]
    fn a_losing_copy_is_left_in_place_rather_than_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("sapphire-journal");
        std::fs::create_dir_all(app_dir.join("cli").join("ws-1")).unwrap();
        std::fs::write(app_dir.join("cli").join("ws-1").join("mark"), b"cli").unwrap();
        std::fs::create_dir_all(app_dir.join("server").join("ws-1")).unwrap();
        std::fs::write(app_dir.join("server").join("ws-1").join("mark"), b"server").unwrap();

        unsplit_app_dir(&app_dir).unwrap();

        assert!(
            app_dir.join("cli").join("ws-1").join("mark").exists(),
            "a directory this migration could not move must be left for the user to look at"
        );
    }

    #[test]
    fn keys_move_with_their_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("sapphire-agent");
        std::fs::create_dir_all(app_dir.join("server").join("ws-1")).unwrap();
        std::fs::write(app_dir.join("server").join("ws-1").join("keys.toml"), b"[[key]]\n")
            .unwrap();

        unsplit_app_dir(&app_dir).unwrap();

        assert!(
            app_dir.join("ws-1").join("keys.toml").exists(),
            "a server that lost its keys refuses to start"
        );
    }

    #[test]
    fn an_app_directory_that_was_never_split_is_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("sapphire-journal");
        std::fs::create_dir_all(app_dir.join("ws-1")).unwrap();
        std::fs::write(app_dir.join("ws-1").join("docs.redb"), b"x").unwrap();

        unsplit_app_dir(&app_dir).unwrap();

        assert!(app_dir.join("ws-1").join("docs.redb").exists());
    }

    #[test]
    fn a_missing_app_directory_is_created_not_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("brand-new");
        assert_eq!(unsplit_app_dir(&app_dir).unwrap(), app_dir);
        assert!(app_dir.is_dir());
    }

    #[test]
    fn a_directory_that_is_not_a_kind_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("sapphire-journal");
        std::fs::create_dir_all(app_dir.join("ws-1")).unwrap();
        // A workspace uuid could look like anything; only the three known kinds move.
        std::fs::create_dir_all(app_dir.join("desktop-notes")).unwrap();

        unsplit_app_dir(&app_dir).unwrap();

        assert!(app_dir.join("desktop-notes").is_dir());
    }

    #[test]
    fn the_resolved_cache_path_no_longer_contains_a_kind() {
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: the test binary sets this before any other thread reads it.
        unsafe { std::env::set_var("SAPPHIRE_UNSPLIT_CACHE_DIR", tmp.path()) };
        static CTX: crate::AppContext = crate::AppContext::new("sapphire-unsplit");
        CTX.init(AppKind::Server);
        unsafe { std::env::remove_var("SAPPHIRE_UNSPLIT_CACHE_DIR") };

        let cache = CTX.cache_dir();
        for kind in ["/cli", "/server", "/desktop"] {
            assert!(
                !cache.to_string_lossy().contains(kind),
                "the kind is still in the path: {}",
                cache.display()
            );
        }
    }
}
```

`a_directory_that_is_not_a_kind_is_left_alone` matters because the migration walks a directory
whose other entries are workspace uuids. Moving anything that is not one of the three known
kind names would scatter a user's caches.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-workspace --all-features unsplit`
Expected: FAIL — `unsplit_app_dir` does not exist.

- [ ] **Step 3: Implement**

Replace `migrate_app_dir` with `unsplit_app_dir`:

```rust
/// Move `<app>/<kind>/…` back up to `<app>/…` (spec §7), and return the app directory.
///
/// #129 split these per binary kind so a desktop app and a server would not open one
/// database. The server is now the only process that opens one, so the split has no
/// remaining purpose — and it never covered the case that mattered, since a CLI invocation
/// and the stdio MCP server were both `cli`.
///
/// Idempotent, and it deletes nothing: where two kinds left a directory for one workspace,
/// the server's wins and the others stay where they are, named in a warning.
pub fn unsplit_app_dir(app_dir: &Path) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(app_dir)?;

    // Most specific first: the server's copy is the one the new architecture keeps writing.
    for kind in [AppKind::Server, AppKind::Desktop, AppKind::Cli] {
        let from = app_dir.join(kind.as_str());
        if !from.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&from)? {
            let entry = entry?;
            let target = app_dir.join(entry.file_name());
            if target.exists() {
                tracing::warn!(
                    kept = %target.display(),
                    left = %entry.path().display(),
                    "two kinds left a directory for one workspace; the first kept wins and \
                     the other is left in place — delete it once you are satisfied"
                );
                continue;
            }
            if let Err(err) = std::fs::rename(entry.path(), &target) {
                // A cross-device rename fails; fall back to a copy, and still delete nothing
                // on failure.
                tracing::warn!(
                    from = %entry.path().display(),
                    to = %target.display(),
                    "could not move: {err}"
                );
            }
        }
        // Only if it emptied out.
        let _ = std::fs::remove_dir(&from);
    }
    Ok(app_dir.to_owned())
}
```

`context.rs`'s `init_category` calls `unsplit_app_dir` instead of `migrate_app_dir`, and
`init` no longer passes the kind down to path resolution. `migrate_keys_to_data` stays as it
is — it moves keys between the cache and data trees, which is orthogonal.

- [ ] **Step 4: Verify and commit**

```bash
cargo test --all-features --locked
git add crates/sapphire-framework-workspace
git commit -m "refactor(workspace)!: undo the per-kind directory split

#129 split cache, data and config per binary kind so two kinds would not open
one database. The process architecture removes the collision itself, and the
split never covered the case that caused it: a CLI invocation and the stdio MCP
server are both \`cli\`. The migration moves directories back up and deletes
nothing."
```

---

### Task 4: The facade

**Files:**
- Modify: `crates/sapphire-framework/{Cargo.toml,src/lib.rs}`
- Test: `crates/sapphire-framework/tests/features.rs`

**Interfaces:**
- Features become: `workspace`, `retrieve`, `track`, `sync`, `session`, `ipc`, `server`,
  `bridge`, `keys`, `service`, `registry`, `backend`, `gui`, plus the passthroughs
  `redb-store` and `fastembed-embed`.
- `native` becomes `["workspace", "backend", "server", "bridge", "registry", "service"]`.
- Gone: `rpc`, `remote-client`, `remote-server`, `blob`.

- [ ] **Step 1: Write the failing tests**

```rust
//! The facade's features, and what each one brings.

#[test]
fn the_old_features_are_gone() {
    let manifest = include_str!("../Cargo.toml");
    for gone in ["remote-client", "remote-server", "\nrpc =", "\nblob ="] {
        assert!(!manifest.contains(gone), "the feature {gone:?} still exists");
    }
}

#[test]
fn every_module_feature_has_a_matching_optional_dependency() {
    let manifest = include_str!("../Cargo.toml");
    for feature in [
        "workspace", "retrieve", "track", "sync", "session", "ipc", "server", "bridge",
        "keys", "service", "registry", "backend", "gui",
    ] {
        assert!(
            manifest.contains(&format!("sapphire-framework-{feature} =")),
            "the feature {feature} has no dependency behind it"
        );
    }
}

#[test]
fn native_includes_what_a_host_needs() {
    let manifest = include_str!("../Cargo.toml");
    let line = manifest
        .lines()
        .find(|l| l.starts_with("native ="))
        .expect("a native feature");
    for part in ["workspace", "backend", "server", "bridge"] {
        assert!(line.contains(part), "native is missing {part}: {line}");
    }
}
```

Then, because a manifest test only proves the text, add a compile check per feature to CI:

```yaml
      - name: Each facade feature builds on its own
        run: |
          for f in workspace retrieve track sync session ipc server bridge keys service registry backend; do
            cargo check -p sapphire-framework --no-default-features --features "$f" || exit 1
          done
```

That is what catches a feature that names a dependency but does not compile without its
neighbours — the failure mode a manifest test cannot see.

- [ ] **Step 2–4: Implement, verify, commit**

```bash
cargo test -p sapphire-framework --all-features
git add crates/sapphire-framework .github/workflows/ci.yml
git commit -m "refactor(facade): replace the remote features with ipc, server and bridge"
```

---

### Task 5: Rewrite `ARCHITECTURE.md`

**Files:**
- Modify: `docs/ARCHITECTURE.md`

The document still describes a central server, a WASM target and a per-kind directory layout,
none of which exist. It is in Japanese and grandfathered by `CONTRIBUTING.md`; keep it in
Japanese, and rewrite only the sections that are now wrong.

- [ ] **Step 1: Replace the sync sections**

Replace `## remote 同期 API（JSON-RPC・実装済み）` in full with a section describing the
current shape: app servers own workspaces, the bridge carries identity and routing, sessions
run end to end between app servers, and the two specs that define it. Delete the JSON-RPC
method table; it describes an API that no longer exists.

- [ ] **Step 2: Correct the directory section**

`## アプリディレクトリ構成と CLI 規約（#128 / #129）` describes the per-kind layout Task 3
removed. Rewrite it to the current one, and **keep a paragraph saying the split existed and
why it went** — someone will find a `<app>/server/` directory on their disk and need to know
what it is.

- [ ] **Step 3: Correct the crate table and the phases**

Every crate this series added or removed. The implementation-phase list at the bottom describes
Phases 0–4 of a plan that has been superseded twice; replace it with a pointer to the
process-architecture spec's §9 and a note of what is done.

Mark WASM out of scope, citing the sync spec's decision 7, rather than leaving Phase 4 looking
like upcoming work.

- [ ] **Step 4: Check nothing else still points at the old world**

```bash
grep -rn "remote-server\|remote-client\|sapphire-framework-rpc\|sapphire-framework-blob" \
    --include='*.md' --include='*.rs' --include='*.toml' . \
  | grep -v '^./docs/superpowers/specs/2026-09-15' \
  | grep -v '^./docs/superpowers/plans/' \
  | grep -v '^./CHANGELOG.md'
```

Expected: no output. The two exclusions are deliberate — the 2026-09-15 spec and the plans are
historical records, and rewriting history to match the present is how a repository loses the
ability to explain itself. `CHANGELOG.md` likewise.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add docs/ARCHITECTURE.md
git commit -m "docs(architecture): describe the framework as it now is"
```

---

## What this plan does not cover

| | Left for |
|---|---|
| Migrating each application onto the new crates | each application's own repository and spec: journal, ledger, timer, agent |
| Publishing to crates.io | blocked until the applications stop depending on git revisions; a separate release decision |
| `Workspace::devices_path` | removed here only if no application still reads it. If one does, leave it with a deprecation note and remove it in that application's migration |
| The `README.md` quick start | it still shows `sapphire-workspace = "0.10"` and a synchronous API; worth a pass, but it is documentation for consumers who do not exist yet |
