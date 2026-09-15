# sapphire-sync — sync-only app and dedicated node

- Date: 2026-09-15
- Scope: new application crate `sapphire-sync` in `apps/sapphire-sync/` of the
  `sapphire-framework` repository; a one-line addition to `CONTRIBUTING.md`; release-plz
  configuration
- Depends on: [`2026-09-15-p2p-sync-iroh-design.md`](./2026-09-15-p2p-sync-iroh-design.md)
  (called "the framework spec" below). Everything about replication, workgroups, pairing,
  the node directory, `NodeCommand` and `ServiceCommand` is defined there; this spec only
  covers the application.

## Background

The framework spec replaces server-mediated RPC sync with peer-to-peer replication over
iroh, run by whichever process on a host holds the shared node lock. Before migrating the
existing apps (timer, ledger, journal, agent), a sync-only application is built first. It
serves three roles:

1. **Reference implementation and E2E test bed** of the framework's `sync` / `net` crates,
   and proof that they work without `-workspace`'s search stack (`-retrieve`).
2. **Headless node for servers** — the always-on peer (and, with the embedded relay, the
   relay) of a workgroup. It can be the production central server.
3. **Dedicated background sync service** on everyday machines (the framework spec's
   dedicated service mode, `embedded_node = false`) — the role iCloud's background daemon
   plays for apps.

Because of roles 2 and 3 it is a core component and lives in the framework repository,
versioned in lockstep with the framework crates.

## Decisions

Agreed during brainstorming on 2026-09-15:

1. **One binary**, `sapphire-sync`, Syncthing-style: running `sapphire-sync` **without a
   subcommand** starts the long-running node (`run` is an equivalent explicit form)
   (`AppKind::Server`); every other subcommand is a one-shot CLI invocation
   (`AppKind::Cli`). A desktop/tray app, if ever built, is a separate crate.
2. **Location**: `apps/sapphire-sync/`; a future desktop crate goes to
   `apps/sapphire-sync-desktop/`. `CONTRIBUTING.md` gains a rule for this repository.
3. **First-release scope**: the framework's node commands, the node itself, `init`, and service
   installation. Out of scope: send-only / receive-only folders, file versioning, a web UI,
   a desktop app, binary release artifacts.
4. **Any app's workspaces can be hosted**, not only sapphire-sync's own: mapping a journal
   workspace on a server that runs only sapphire-sync is the normal way to give journal an
   always-on peer.
5. **Service installation** uses `sapphire-framework-service` with
   `system_run_as = InvokingUser` and a post-install step that turns off embedded nodes for
   the target user.
6. **No application config file.** Everything lives in the framework's node directory
   (`net.toml`, `workspaces.toml`) and in each workspace (`.sapphireignore`).

## 1. Layout and dependencies

```
apps/sapphire-sync/
    Cargo.toml          # package `sapphire-sync`, bin `sapphire-sync`
    README.md
    README.ja.md
    src/
        main.rs         # AppContext::init, dispatch, exit codes
        cli.rs          # clap: Run, Init, #[command(flatten)] NodeCommand, ServiceCommand
        run.rs          # dedicated node lifecycle
        init.rs         # init <path>
    tests/
        e2e/            # multi-host scenarios (§5)
```

Dependencies: the `sapphire-framework` facade with features `sync`, `net` (with `cli`),
`registry`, `service`, plus `-workspace` for `AppContext` only. **Not** `retrieve` or
`backend`. A CI check (`cargo tree -p sapphire-sync -i sapphire-framework-retrieve` finds
nothing) keeps it that way.

`CONTRIBUTING.md` (repository layout, both languages): "In the `sapphire-framework`
repository, application crates live under `apps/`, in a directory named after the full
crate name."

## 2. Commands

| command | kind | behaviour |
|---|---|---|
| `sapphire-sync` (no subcommand; `run` is an equivalent explicit form) | server | Runs the node as a **dedicated** holder: tries the lock regardless of `embedded_node`, retries every 10 s while another process holds it, logs to stdout and to the shared `node.log`. |
| `sapphire-sync init <path> [--name <name>] [--workgroup <name\|id>]` | cli | Turns a directory into a sapphire-sync workspace and shares it (below). |
| framework `NodeCommand` | cli | `sync`, `node status`, `node log [--follow]`, `workgroup create` / `join` / `list`, `device invite` / `list` / `retire`, `workspace list` / `share` / `map` / `unmap`. |
| `sapphire-sync service install` / `uninstall` / `status` | cli | framework `ServiceCommand` (§3). |

`init <path>`:

1. Create `<path>` if missing, and the marker `<path>/.sapphire-sync/`.
2. If `<path>/.sapphireignore` does not exist, write a template that is only comments:
   what the file does, gitignore syntax, that it is synced to every device, and two
   commented-out examples (`*.tmp`, `node_modules/`).
3. Call the framework's `share_workspace(workgroup, "sapphire-sync", path, name)`; `name`
   defaults to the directory name.

It is sapphire-sync's name for `workspace share`, which remains available and behaves the
same for a directory that already has the marker.

**Hosting other apps' workspaces**: `workspace map <name|id> <dir>` takes `app_name` from
the workgroup listing and creates that app's marker (e.g. `.journal/`). Filtering uses the
framework's built-in rule for that app name, so no app code is needed.

**Node lifecycle** (`sapphire-sync` / `sapphire-sync run`):

- Start: `AppContext::init(AppKind::Server)`, open the node directory, take the lock or
  wait as a follower.
- SIGTERM / SIGINT (Ctrl-C; on Windows, console control events): stop accepting new work,
  finish in-flight materializations, close sessions, release the lock, exit 0.
- A node failure (the framework's fault isolation reports it) is logged; the process exits
  non-zero so the service manager's `Restart=on-failure` restarts it.

**Output**: human-readable tables and messages. No `--json`; machine consumers (including
the E2E tests) read `status.json`.

**Exit codes**: 0 success; 1 runtime error; 2 usage error (clap). `sync` prints and exits 0
when another process already runs the node.

## 3. Service installation

`ServiceSpec` for sapphire-sync:

- `args = []` — the unit runs the bare binary
- `system_run_as = RunAs::InvokingUser` — `sudo sapphire-sync service install` creates
  `/etc/systemd/system/sapphire-sync.service` with `User=$SUDO_USER`; `--run-as <user>`
  overrides; without either, install fails with an explanation.
- `post_install`: write `embedded_node = false` into the target user's `net.toml` (created
  if missing, chowned to the target user when installing for another user), so apps on
  that host defer to the service. `--keep-embedded` skips it.

Everything else (user-level units, linger hint, LaunchAgent, Task Scheduler, refusing
system-wide install off Linux) is the framework's behaviour.

## 4. Example setup (README quick start)

```
server$ sudo sapphire-sync service install              # system unit, User=alice
server$ sapphire-workgroup create --name home
server$ sapphire-sync device invite --name laptop       # prints a ticket
laptop$ sapphire-workgroup join <ticket>
laptop$ sapphire-sync init ~/Documents/notes            # workspace "notes"
server$ sapphire-sync workspace list                     # notes appears
server$ sapphire-sync workspace map notes /srv/sync/notes
```

README sections (English and Japanese, cross-linked at the top, per `CONTRIBUTING.md`):
quick start; server setup (system-wide install, `loginctl enable-linger` for user units,
embedded relay and publishing relay URLs to the workgroup); hosting other apps' workspaces;
`.sapphireignore`; how conflicts appear (conflict copies); checking status and logs
(`node status`, `node log --follow`, which process runs the node).

## 5. Testing

### 5.1 E2E (`apps/sapphire-sync/tests/e2e/`)

Harness:

- Each simulated host is a set of real `sapphire-sync` processes sharing one temporary
  `SAPPHIRE_NODE_DIR` (and a temporary home for workspaces).
- `net.toml` per host: relays disabled, n0 discovery disabled, mDNS disabled (unreliable on
  CI), static peer entries (`NodeId` + `127.0.0.1:<port>`), fixed listen ports allocated
  by the harness.
- Assertions poll `status.json` and the file trees with timeouts; no sleeps without a
  condition.

Scenarios:

1. **Pair and propagate**: `workgroup create` → `device invite` → `workgroup join` → `init` →
   `workspace map`; files created, modified and deleted on either host appear on the other.
2. **Conflict copy**: stop both nodes, edit the same file differently on each, restart;
   both hosts end with the same winner and one `*.conflict-<grain-id>-<n>.*` copy.
3. **Takeover**: two node processes (`sapphire-sync`) on one host; kill the holder; the other takes the
   lock, `status.json` names it, `node.log` records `holder changed`.
4. **Missing root guard**: rename a mapped root away while running; the replica pauses and
   the peer's files are untouched; rename it back; syncing resumes.
5. **Other app's workspace**: share a workspace with a `.journal/` marker from one host
   (via `workspace share`), `workspace map` it on the other; hidden files other than
   `.journal/` do not sync.
6. **One-shot sync**: with no node process running, `sapphire-sync sync` on each host converges
   both.
7. **Filtering and limits**: `.sapphireignore` patterns and a file above `max_file_size`
   are not synced and appear as skipped in `status.json`.
8. **Retire**: `device retire laptop` on the server; after it propagates, changes stop
   flowing between them.
9. **Dependency check**: `sapphire-sync` does not depend on `sapphire-framework-retrieve`
   (run as a CI step rather than a Rust test).

CI runs the E2E suite on Linux and Windows. Installing a real service (systemd unit,
LaunchAgent, scheduled task) is an `#[ignore]` test run manually; unit-level coverage of
service file generation lives in the framework crate.

### 5.2 Unit

- `init`: marker and template creation, existing `.sapphireignore` left untouched,
  idempotent on an already-initialized directory.
- `post_install`: `net.toml` created or updated preserving other keys; `--keep-embedded`
  leaves it alone.
- Node shutdown: a signal while a materialization is in flight completes it before the lock
  is released (with the framework's fault-injection hook).

## 6. Release

- Published to crates.io (`cargo install sapphire-sync`), in release-plz's `framework`
  version group, so its version always matches the framework it was built with.
- Binary artifacts (GitHub Releases, packages) are out of scope for the first release.

## 7. Implementation order

Development tracks the framework spec's implementation order:

1. After framework step 3 (node directory): crate skeleton, `init`, the node (bare invocation) as a holder
   without networking; unit tests.
2. After framework step 4 (net basics): E2E harness and scenarios 3–7 and 9.
3. After framework step 5 (pairing): scenarios 1, 2 and 8.
4. After framework step 7 (service): service integration and its unit tests.
5. README (en/ja), `CONTRIBUTING.md` rule, release-plz entry.

## Risks

1. **Port and timing flakiness in multi-process E2E tests**, especially on Windows;
   mitigated by harness-allocated ports, condition polling and generous CI timeouts.
2. **Signal handling differences on Windows** (no SIGTERM; service stop arrives differently
   under Task Scheduler) — the graceful path must be verified on each platform.
3. **Scope creep toward Syncthing parity** (versioning, send/receive-only, web UI); each is
   its own spec when needed.
