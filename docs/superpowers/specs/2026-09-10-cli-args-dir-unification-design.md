# Unified directory resolution and CLI argument conventions (issues #128 / #129)

- Date: 2026-09-10
- Scope: `sapphire-framework-workspace` (+ facade re-exports), then migration of the four
  consumer apps (`sapphire-journal`, `-ledger`, `-tally`, `-agent`)
- Related: issues #128 (CLI args / config primitives in the framework) and #129
  (config/cache/data directory resolution; server/cli/desktop directory collision)

## Background

All four apps ship the same shape of binaries (cli / server, later desktop / mobile /
wasm) but resolve their app directories differently, and their CLI argument and
environment-variable names disagree across apps:

| app | binary | workspace-dir option | env | app dir layout today |
|---|---|---|---|---|
| journal | cli | `--journal-dir` | `SAPPHIRE_JOURNAL_DIR` | `~/.cache/sapphire-journal/<uuid>/` (shared by cli & server) |
| journal | server | `--journal-dir` | `SAPPHIRE_JOURNAL_SERVER_DIR` | same directory as cli; `keys.toml` lives in the shared cache dir |
| ledger | cli | `--ledger-dir` | `SAPPHIRE_LEDGER_DIR` | `~/.cache/sapphire-ledger/<uuid>/`, `~/.local/share/sapphire-ledger/<uuid>/` (shared) |
| ledger | server | `--ledger-dir` | `SAPPHIRE_LEDGER_SERVER_DIR` | same directories as cli; `keys.toml` in the shared cache dir |
| tally | server | `--data-dir` | `SAPPHIRE_TALLY_DIR` | raw data dir given on the command line (no framework `AppContext` yet) |
| agent | server | `--config` (file) | — (workspace dir comes from config file) | `~/.cache/sapphire-agent-server/`, `~/.local/share/sapphire-agent-server/` (pkg-name suffix — "option A") |
| agent | cli | `--config` (file) | `SAPPHIRE_AGENT_TOKEN`, `SAPPHIRE_AGENT_CLI_CACHE_DIR`, `SAPPHIRE_AGENT_CLI_DEVICE_ID_PATH` | `~/.config/sapphire-agent-cli/` (pkg-name suffix) |

Two structural problems:

1. **Binary-type collision.** journal and ledger put cli and server data in the *same*
   app directory, so the same redb/SQLite cache files are opened by two binaries
   concurrently.
2. **Layout inconsistency.** journal/ledger use `~/.cache/<app>/` (app name only);
   agent already uses `~/.cache/<pkg-name>/` (option A: package name including the
   binary-type suffix).

The framework already hosts `AppContext` (`set_cache_dir` / `set_data_dir`, first-writer
wins, `cache_dir_for(root)` = `{cache_dir}/{uuid}`), injected by each app at startup.
The "no `dirs` dependency" policy it was built around is retired: `dirs` moves into the
framework and the injection is absorbed.

## Decisions (agreed 2026-09-08, refined here)

1. **Layout: option B** — app-name directory containing a per-binary-type subdirectory:
   `~/.cache/sapphire-journal/server/<uuid>/`, `~/.config/sapphire-agent/server/`, …
   Chosen over option A (`~/.cache/sapphire-journal-server/`) because the existing
   app directory becomes the parent, so migration is a single rename.
2. **Migration: automatic on first launch, plus a deprecation window for old names.**
   Old layouts are renamed into the new layout exactly once; deprecated option/env names
   are accepted with a `warn!` for one release cycle, then removed.
3. **Naming:** the workspace-directory option is `--workspace-dir` everywhere. The
   current names — `--journal-dir` (journal cli + server), `--ledger-dir` (ledger cli +
   server), `--data-dir` (tally server) — are accepted as clap aliases, deprecated with
   a warning for one release cycle. Environment variables are `SAPPHIRE_<APP>_<NAME>` with
   no binary-type qualifier (cli and server mean the same thing):
   `SAPPHIRE_JOURNAL_DIR`, `SAPPHIRE_LEDGER_DIR`, `SAPPHIRE_TALLY_DIR`,
   `SAPPHIRE_AGENT_TOKEN`. Deprecated for one cycle (warned):
   `SAPPHIRE_JOURNAL_SERVER_DIR`, `SAPPHIRE_LEDGER_SERVER_DIR`,
   `SAPPHIRE_JOURNAL_SERVER_ADDR` → `SAPPHIRE_JOURNAL_ADDR`,
   `SAPPHIRE_JOURNAL_SERVER_KEYS` → `SAPPHIRE_JOURNAL_KEYS`,
   `SAPPHIRE_JOURNAL_SERVER_ALLOWED_HOSTS` → `SAPPHIRE_JOURNAL_ALLOWED_HOSTS`,
   `SAPPHIRE_AGENT_CLI_CACHE_DIR` → `SAPPHIRE_AGENT_CACHE_DIR`,
   `SAPPHIRE_AGENT_CLI_DEVICE_ID_PATH` → `SAPPHIRE_AGENT_DEVICE_ID_PATH`.
4. **Everything lands in `sapphire-framework-workspace`** (no new crate): the `dirs`
   re-export, the `AppContext` initialisation helper (platform resolution + layout +
   migration), the shared clap arg struct, and the env-naming rule. The facade crate
   re-exports everything.

## Design — framework (`sapphire-framework-workspace`)

### New module `app_dirs`

```rust
pub enum AppKind { Cli, Server, Desktop }   // AsRef<str>: "cli" | "server" | "desktop"
```

`AppContext` gains two more `OnceLock<PathBuf>` fields (`config_dir`, filled by the new
`init`) and one initialiser that replaces every app-side `init_app_context()` /
`init_app_ctx()`:

```rust
/// Resolve the platform directories via `dirs`, apply the per-binary-type layout,
/// migrate any legacy layout, create the directories and set all three fields.
/// First writer wins, as before. Env overrides: `SAPPHIRE_<APP-UPPER>_CACHE_DIR`,
/// `..._DATA_DIR`, `..._CONFIG_DIR` replace the platform root for that category.
pub fn init(&self, kind: AppKind);
```

- App directory name: the app name passed to `AppContext::new` (e.g. `sapphire-journal`).
  The per-binary-type directory is `kind`'s string, one level below it.
- Env override replaces the **platform root** (`dirs::cache_dir()` etc.), not the
  per-app or per-kind level.
- `cache_dir_for(root)` semantics are unchanged; it now returns
  `{cache_dir}/{uuid}` where `cache_dir` already carries the `/{kind}` segment.
- `model_cache_dir()` moves under the kind segment as well (`{cache_dir}/models`
  where `cache_dir` = `~/.cache/<app>/<kind>/`), so each binary type keeps its own
  model cache copy — accepted trade-off: cache directories are rebuildable.
- If a platform root cannot be resolved, fall back to `temp_dir()` as the apps do today.

### Migration (run inside `init`, best-effort, idempotent, logged with `warn!`)

- **Option A → option B** (agent): if `{platform_root}/{app_name}-{kind}` exists and
  `{platform_root}/{app_name}/{kind}` does not, rename it.
- **Shared → per-kind** (journal / ledger): if the app directory contains UUID-named
  subdirectories directly (old layout) and no `{kind}` directories exist yet, rename the
  whole old app directory into place is *not* possible (both kinds share it) — instead:
  if the binary's kind is the one that owns the legacy data (server, which owns
  `keys.toml` and the shared caches on the machines we know) move it; otherwise create a
  fresh kind directory and let caches rebuild. Concretely: `init` checks for legacy UUID
  directories directly under the app directory, and on the **first** kind that runs,
  moves them into `{app}/{kind}/`. Every kind afterwards creates its own empty
  `{uuid}` on demand (caches rebuild; `keys.toml` lands in the first-migrated kind's
  data directory and every binary reads through the same `data_dir`-based lookup, so
  the key file is no longer split by kind — see “keys.toml” below).
- Migration is a `std::fs::rename` (same-filesystem rename), never a copy; if the rename
  fails (cross-device), fall back to copy + delete-old-name of the contents.

### keys.toml moves from cache to data

`keys.toml` (device keys — secrets, not a cache) moves from the cache directory to the
**per-workspace data directory**: `{data_dir}/{kind}/{uuid}/keys.toml`. Because the file
is per-workspace and read by whichever binary needs it, migration moves it into the data
tree once; after migration all binaries resolve it through `AppContext` and the split
cache directories no longer affect it. This is a behaviour change covered by the
migration (old path is checked and moved), not a breaking change for users.

### clap / serde re-exports

- `sapphire-framework-workspace` declares `clap = { workspace = true }` (already in
  `[workspace.dependencies]` with `derive` + `env` features) and `serde`, and re-exports
  both: `pub use clap; pub use serde; pub use dirs;` (dirs moves in as a regular
  dependency — the “no dirs” policy note in `context.rs` is removed).
- New shared argument:

```rust
#[derive(clap::Args)]
pub struct WorkspaceArgs {
    /// Workspace root directory. Overrides the upward marker search.
    #[arg(long = "workspace-dir", global = true, value_name = "DIR")]
    pub workspace_dir: Option<PathBuf>,
}
```

  Apps embed it with `#[command(flatten)]`. Apps keep ownership of their own clap derive
  definitions (per #128); only this shared argument and its env resolution move into the
  framework.

### Shared `resolve` (already exists — becomes *the* resolution path)

`Workspace::resolve(ctx, explicit)` already prefers the explicit path and falls back to
the upward marker search (`.sapphire-journal/` / `.sapphire-ledger/` via `find_from`).
Apps with a fallback-root fallback (journal core's `Journal::from_root` callers) are
migrated to route through `Workspace::resolve` (see app migration below).

### Facade crate (`sapphire-framework`)

Re-exports at the facade level so apps can depend on a single crate:
`sapphire_framework::{clap, serde, dirs}` (feature-gated on `workspace`) plus everything
already exported from `workspace` (now including `AppKind`, `WorkspaceArgs`).

## Design — app migration (4 PRs, same template)

Every app:

1. Drop its direct `dirs` / `directories` dependency and its `init_app_context` /
   `init_app_ctx` implementation.
2. Call `<APP>_CTX.init(AppKind::Cli | AppKind::Server | AppKind::Desktop)` at the top of
   each binary's `main`. `AppContext::new("<app-name>")` keeps the app name unchanged
   (`sapphire-journal`, `sapphire-ledger`, `sapphire-agent`) — the new per-kind segment
   comes from `init`.
3. Depend on `sapphire-framework` (facade) instead of `sapphire-framework-workspace`
   directly; import `clap` / `serde` / `dirs` through it. (ledger currently depends on
   the inner crate via git; switch to the crates.io facade alongside the version bump.)
4. CLI: replace the app-specific directory argument with the framework's
   `WorkspaceArgs` (embedded via `#[command(flatten)]`), keeping the old flag name as a
   clap alias; route resolution through `Workspace::resolve` (journal/ledger) or the
   equivalent single resolver (tally: replace its ad-hoc `data_dir` resolution with the
   `AppContext`-based path; agent keeps its config-file-based resolution but moves to
   the framework-resolved dirs).
5. Env: accept the unified name; keep the old name working with a one-time `warn!` for
   one release cycle.

### Per-app notes

- **journal**: cli + server both switch to `--workspace-dir` (server alias
  `--journal-dir`, deprecated). `SAPPHIRE_JOURNAL_DIR` replaces `SAPPHIRE_JOURNAL_*`
  env names per §3. `keys.toml` moves to the data dir (framework resolves it); the
  server's `default_keys_path` becomes a thin wrapper. Desktop calls `init(AppKind::Desktop)`.
- **ledger**: same template; `--ledger-dir` becomes an alias; `SAPPHIRE_LEDGER_DIR`
  already matches the rule (the `_SERVER_DIR` variant is the deprecated alias).
- **tally**: first framework dependency at all — server binary only, so `init(AppKind::Server)`;
  `--data-dir` kept as a deprecated alias of `--workspace-dir`; `SAPPHIRE_TALLY_DIR`
  already matches the rule. `stamps/` + `activities/` layout inside the workspace dir is
  unchanged (tally is file-based; the per-kind directories hold only its future caches).
- **agent**: option A → option B handled entirely by the framework migration
  (`~/.cache/sapphire-agent-server` → `~/.cache/sapphire-agent/server`). The CLI gains an
  `AppContext` too (`AppKind::Cli`, app name `sapphire-agent`) for the device-id file and
  voice cache; `SAPPHIRE_AGENT_CLI_CACHE_DIR` / `SAPPHIRE_AGENT_CLI_DEVICE_ID_PATH` get the
  deprecated-alias path per §3. `config.toml` search becomes
  `~/.config/sapphire-agent/<kind>/config.toml` with the legacy `~/.config/sapphire-agent/`
  (and `~/.config/sapphire-agent-cli/`) locations still read, in order, during the
  deprecation window. The per-app cache sub-directories (`images/`, `ambient/`,
  `subagents/`, `tool-payloads/`) hang off the per-kind cache directory.

## Migration policy details

- One release cycle of deprecation warnings (`warn!`, once per process per name), then a
  follow-up commit (referenced by the same issues) removes the aliases.
- Migration runs lazily at `init`, is idempotent (existence checks guard every rename),
  and never deletes: the legacy name is consumed by the rename itself; nothing else is
  removed.
- Tests (framework, `tempfile`-based): legacy-option-A rename; legacy shared-UUID move
  for the first kind; env overrides; `cache_dir_for` layout; idempotent second `init`.

## Out of scope

- mobile / wasm platform backends (framework-internal `dirs` replacement for Android/iOS
  stays as a follow-up issue once the mobile crates exist).
- per-binary cache *reuse* strategies (cache rebuild per binary type is accepted as-is).
- any behaviour change in sync, keys rotation, or the registry.
