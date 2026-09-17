# Service Installation (`sapphire-framework-service`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> If your harness has no such skill, execute the tasks in order, one at a time, running the
> listed commands and committing at the end of each task. Do not skip the "run the test and
> watch it fail" steps: they are what proves the test exercises the new code.

**Goal:** Let any sapphire app — and the bridge — register itself with the OS service manager
in one command, including the case that needs it most: a server started as root that drops to
a human user and runs its tools as another.

**Architecture:** An app describes itself with a `ServiceSpec`; the crate turns that into a
unit file and hands it to a `ServiceManager`. The manager is a trait with one real
implementation per platform and a recording one for tests, so every generated file is checked
against a golden copy and **no test ever touches the host's service manager**.

**Tech Stack:** Rust 2024 (toolchain 1.98.0), clap 4, `sapphire-framework-workspace`
(`AppContext`), thiserror 2, tracing; dev: tempfile 3.

**Spec:** `docs/superpowers/specs/2026-09-15-p2p-sync-iroh-design.md` §5.5, read through the
substitution table at the head of its §3; `docs/superpowers/specs/2026-09-16-process-architecture-design.md`
§3.2 (a `ServiceSpec` carries `run_as` and `helper_as`). Implementation order step 10 of the
process-architecture spec's §9.

**Depends on:** step 5 (`2026-09-16-privilege-separation-plan.md`) for `PrivilegeConfig`, and
steps 4 and 6 for the commands this is flattened into.

**Branch:** work on `feat/p2p-sync-iroh` (the current branch).

## Global Constraints

- Code, comments, commit messages and tests in **English** (`CONTRIBUTING.md`).
- CI runs `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  and `cargo test --all-features --locked`. All three must pass after every task.
- Every public item carries a doc comment.
- **No test may invoke `systemctl`, `launchctl` or `schtasks`.** Every call goes through the
  `ServiceManager` trait, and tests use `RecordingManager`. A test that shelled out would
  change the machine it ran on.
- Unit files are compared against **golden files** in `tests/golden/`. Regenerate them
  deliberately, never with a blanket "update snapshots" pass.
- `ExecStart` is the **absolute path of the running executable** plus the spec's `args`.
- System-wide installation is **Linux only**. Elsewhere, running the install as root fails
  with "system-wide installation is supported on Linux only".

## The decision table this crate implements

| Invoked as | Unit | Activation |
|---|---|---|
| a regular user | `~/.config/systemd/user/<app>.service` | `systemctl --user enable --now`, plus a hint about `loginctl enable-linger` for a machine that should run it without a login |
| root, including `sudo` | `/etc/systemd/system/<app>.service`, `After=network-online.target` | `systemctl enable --now` |

And for the user a system unit runs as:

- `RunAs::Root` — **no `User=`**. For an app that starts as root and drops privileges itself
  (`sapphire-agent`, agent issue #257). The unit instead carries the app's `run_as` and
  `helper_as` in its environment, so the drop has something to read.
- `RunAs::InvokingUser` — `User=$SUDO_USER`. Without `SUDO_USER` and without `--run-as`, the
  install **fails with an explanation**. Running `sapphire-bridge` as root would put the
  bridge directory under `/root` and create synced files owned by root.

## File Structure

```
crates/sapphire-framework-service/
    Cargo.toml
    src/
        lib.rs       # ServiceSpec, RunAs, InstallContext, ServiceCommand
        error.rs     # Error, Result
        scope.rs     # scope and target-user detection
        manager.rs   # ServiceManager trait, RecordingManager
        systemd.rs   # unit generation, user and system
        launchd.rs   # LaunchAgent plist
        windows.rs   # Task Scheduler XML
    tests/
        golden/      # one file per case
        golden.rs
```

---

### Task 1: The crate, the spec type, and scope detection

**Files:**
- Create: `crates/sapphire-framework-service/{Cargo.toml,src/lib.rs,src/error.rs,src/scope.rs}`
- Modify: `Cargo.toml` (workspace `members`), facade
- Test: inline `#[cfg(test)] mod tests` in `scope.rs`

**Interfaces:**
- Produces:
  - `RunAs::{Root, InvokingUser}`
  - `ServiceSpec { app_name: &'static str, description: String, args: Vec<String>, system_run_as: RunAs, privileges: Option<PrivilegeConfig>, post_install: Option<PostInstall> }`
  - `type PostInstall = Box<dyn Fn(&InstallContext) -> Result<()> + Send + Sync>`
  - `InstallContext { scope: Scope, target_user: Option<String>, unit_path: PathBuf, exe: PathBuf }`
  - `Scope::{User, System}`
  - `Environment { euid: u32, sudo_user: Option<String>, os: Os }` — every environment fact
    the decisions need, injected so the tests can vary it
  - `fn resolve_scope(env: &Environment, requested: Option<Scope>) -> Result<Scope>`
  - `fn resolve_target_user(env: &Environment, scope: Scope, spec: &ServiceSpec, override_user: Option<&str>) -> Result<Option<String>>`
  - `Error::{Io, Unsupported, MissingUser, Manager, Config}`

**Why `Environment` is a struct and not a set of calls:** every rule in the table above turns
on the effective uid, `SUDO_USER` and the platform. Reading them through a value makes all
eight combinations testable on one machine; reading them directly would make the table
untestable and it would rot.

- [ ] **Step 1: Create the manifest**

`crates/sapphire-framework-service/Cargo.toml`:

```toml
[package]
name = "sapphire-framework-service"
version.workspace = true
edition.workspace = true
description = "Register a sapphire-framework application with the OS service manager"
license.workspace = true
repository.workspace = true
keywords = ["service", "systemd", "launchd", "daemon"]
categories = ["command-line-utilities"]

[dependencies]
sapphire-server = { package = "sapphire-framework-server", version = "0.14.0", path = "../sapphire-framework-server", default-features = false }
clap.workspace = true
thiserror.workspace = true
tracing.workspace = true

[dev-dependencies]
tempfile = "3"
```

Root `Cargo.toml`: add `"crates/sapphire-framework-service",`. Facade: feature `service`.

> `-service` depends on `-server` only for `PrivilegeConfig`. If that drags in tokio and the
> workspace stack, move `PrivilegeConfig`, `UserSpec` and `HelperSpec` into their own module
> with no other dependencies, or into `-service` itself and re-export from `-server`. Decide
> when you see the dependency graph; do not leave `-service` pulling in redb to describe a
> unit file.

- [ ] **Step 2: Write the failing tests**

`crates/sapphire-framework-service/src/scope.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn linux(euid: u32, sudo_user: Option<&str>) -> Environment {
        Environment {
            euid,
            sudo_user: sudo_user.map(str::to_owned),
            os: Os::Linux,
        }
    }

    fn spec(run_as: RunAs) -> ServiceSpec {
        ServiceSpec {
            app_name: "sapphire-bridge",
            description: "test".into(),
            args: vec!["run".into()],
            system_run_as: run_as,
            privileges: None,
            post_install: None,
        }
    }

    #[test]
    fn a_regular_user_gets_a_user_unit() {
        assert_eq!(resolve_scope(&linux(1000, None), None).unwrap(), Scope::User);
    }

    #[test]
    fn root_gets_a_system_unit() {
        assert_eq!(resolve_scope(&linux(0, None), None).unwrap(), Scope::System);
    }

    #[test]
    fn sudo_gets_a_system_unit() {
        assert_eq!(resolve_scope(&linux(0, Some("alice")), None).unwrap(), Scope::System);
    }

    #[test]
    fn the_scope_can_be_asked_for_explicitly() {
        assert_eq!(
            resolve_scope(&linux(0, Some("alice")), Some(Scope::User)).unwrap(),
            Scope::User
        );
    }

    #[test]
    fn a_regular_user_cannot_ask_for_a_system_unit() {
        let err = resolve_scope(&linux(1000, None), Some(Scope::System)).unwrap_err();
        assert!(err.to_string().contains("root"), "{err}");
    }

    #[test]
    fn a_system_unit_off_linux_is_refused() {
        let env = Environment { euid: 0, sudo_user: None, os: Os::MacOs };
        let err = resolve_scope(&env, None).unwrap_err();
        assert!(
            err.to_string().contains("supported on Linux only"),
            "the message must say what is possible instead: {err}"
        );
    }

    #[test]
    fn a_user_unit_needs_no_target_user() {
        let user = resolve_target_user(
            &linux(1000, None),
            Scope::User,
            &spec(RunAs::InvokingUser),
            None,
        )
        .unwrap();
        assert!(user.is_none(), "a user unit already runs as the right person");
    }

    #[test]
    fn a_system_unit_for_an_app_that_drops_its_own_privileges_has_no_user_line() {
        let user =
            resolve_target_user(&linux(0, Some("alice")), Scope::System, &spec(RunAs::Root), None)
                .unwrap();
        assert!(
            user.is_none(),
            "RunAs::Root means the app becomes someone else itself"
        );
    }

    #[test]
    fn a_system_unit_runs_as_the_invoking_user() {
        let user = resolve_target_user(
            &linux(0, Some("alice")),
            Scope::System,
            &spec(RunAs::InvokingUser),
            None,
        )
        .unwrap();
        assert_eq!(user.as_deref(), Some("alice"));
    }

    #[test]
    fn an_explicit_run_as_wins() {
        let user = resolve_target_user(
            &linux(0, Some("alice")),
            Scope::System,
            &spec(RunAs::InvokingUser),
            Some("bob"),
        )
        .unwrap();
        assert_eq!(user.as_deref(), Some("bob"));
    }

    #[test]
    fn root_without_sudo_user_is_refused_with_an_explanation() {
        let err =
            resolve_target_user(&linux(0, None), Scope::System, &spec(RunAs::InvokingUser), None)
                .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("--run-as"), "the message must say how to fix it: {message}");
        assert!(
            message.contains("root"),
            "and why it matters: files would be owned by root: {message}"
        );
    }
}
```

`root_without_sudo_user_is_refused_with_an_explanation` is the one that earns its keep. A
`sapphire-bridge` installed as a root system unit would put the bridge directory under `/root`
and create synced files owned by root — recoverable, but only after someone works out what
happened. Failing at install time with a sentence naming `--run-as` costs nothing.

- [ ] **Step 3: Run the tests to verify they fail, implement, verify, commit**

```bash
cargo test -p sapphire-framework-service scope
git commit -m "feat(service): decide the scope and the target user"
```

---

### Task 2: systemd units, against golden files

**Files:**
- Create: `crates/sapphire-framework-service/src/systemd.rs`
- Create: `crates/sapphire-framework-service/tests/{golden.rs,golden/*.service}`
- Test: `tests/golden.rs`

**Interfaces:**
- Produces:
  - `fn unit_path(app_name: &str, scope: Scope, home: &Path) -> PathBuf`
  - `fn render_unit(spec: &ServiceSpec, ctx: &InstallContext) -> String`
  - `fn activation(app_name: &str, scope: Scope) -> Vec<Vec<String>>` — the commands to run
  - `fn linger_hint(scope: Scope, user: Option<&str>) -> Option<String>`

- [ ] **Step 1: Write the failing tests**

`crates/sapphire-framework-service/tests/golden.rs`:

```rust
//! Generated unit files, compared against copies checked into the repository.
//!
//! When one of these fails, read the diff before regenerating: a unit file is the contract
//! between this crate and the machine, and a change to it is a change of behaviour.

use std::path::{Path, PathBuf};

use sapphire_framework_service::{
    InstallContext, RunAs, Scope, ServiceSpec, render_unit,
};

fn golden(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden").join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}; create it from the failure output", path.display()))
}

fn check(name: &str, rendered: &str) {
    let want = golden(name);
    assert_eq!(
        rendered.trim_end(),
        want.trim_end(),
        "\n--- generated ---\n{rendered}\n--- {name} ---\n{want}\n"
    );
}

fn spec(run_as: RunAs, privileges: bool) -> ServiceSpec {
    ServiceSpec {
        app_name: "sapphire-agent",
        description: "Sapphire agent server".into(),
        args: vec!["server".into(), "run".into()],
        system_run_as: run_as,
        privileges: privileges.then(|| sapphire_server::PrivilegeConfig {
            run_as: "alice".parse().unwrap(),
            helper: Some(sapphire_server::HelperSpec {
                user: "sapphire-agent-tools".parse().unwrap(),
                program: PathBuf::from("/usr/lib/sapphire-agent/tool-broker"),
                args: vec![],
            }),
        }),
        post_install: None,
    }
}

fn ctx(scope: Scope, target_user: Option<&str>) -> InstallContext {
    InstallContext {
        scope,
        target_user: target_user.map(str::to_owned),
        unit_path: PathBuf::from("/dev/null"),
        exe: PathBuf::from("/usr/bin/sapphire-agent"),
    }
}

#[test]
fn a_user_unit() {
    check(
        "user.service",
        &render_unit(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None)),
    );
}

#[test]
fn a_system_unit_running_as_a_named_user() {
    check(
        "system-user.service",
        &render_unit(
            &spec(RunAs::InvokingUser, false),
            &ctx(Scope::System, Some("alice")),
        ),
    );
}

#[test]
fn a_system_unit_that_drops_its_own_privileges() {
    check(
        "system-privsep.service",
        &render_unit(&spec(RunAs::Root, true), &ctx(Scope::System, None)),
    );
}

#[test]
fn a_user_unit_has_no_network_ordering() {
    let rendered = render_unit(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None));
    assert!(
        !rendered.contains("network-online.target"),
        "a user unit starts after the session is up already"
    );
}

#[test]
fn a_system_unit_waits_for_the_network() {
    let rendered =
        render_unit(&spec(RunAs::InvokingUser, false), &ctx(Scope::System, Some("alice")));
    assert!(rendered.contains("After=network-online.target"), "{rendered}");
}

#[test]
fn exec_start_is_absolute_and_carries_the_arguments() {
    let rendered = render_unit(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None));
    assert!(
        rendered.contains("ExecStart=/usr/bin/sapphire-agent server run"),
        "{rendered}"
    );
}

#[test]
fn a_privilege_separated_unit_has_no_user_line() {
    let rendered = render_unit(&spec(RunAs::Root, true), &ctx(Scope::System, None));
    assert!(
        !rendered.contains("\nUser="),
        "the app becomes someone else itself; a User= line would stop it being able to:\n{rendered}"
    );
}

#[test]
fn a_privilege_separated_unit_names_both_users() {
    let rendered = render_unit(&spec(RunAs::Root, true), &ctx(Scope::System, None));
    assert!(rendered.contains("alice"), "{rendered}");
    assert!(rendered.contains("sapphire-agent-tools"), "{rendered}");
}
```

`crates/sapphire-framework-service/tests/golden/user.service`:

```ini
[Unit]
Description=Sapphire agent server

[Service]
Type=simple
ExecStart=/usr/bin/sapphire-agent server run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```

`tests/golden/system-user.service`:

```ini
[Unit]
Description=Sapphire agent server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=alice
ExecStart=/usr/bin/sapphire-agent server run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

`tests/golden/system-privsep.service`:

```ini
[Unit]
Description=Sapphire agent server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# No User=: this service starts as root in order to become two different users, and
# drops privileges itself. See the process-architecture spec, section 3.
Environment=SAPPHIRE_RUN_AS=alice
Environment=SAPPHIRE_HELPER_AS=sapphire-agent-tools
ExecStart=/usr/bin/sapphire-agent server run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sapphire-framework-service --test golden`
Expected: FAIL — `render_unit` does not exist.

- [ ] **Step 3: Implement, verify, commit**

`unit_path` is `~/.config/systemd/user/<app>.service` for `Scope::User` and
`/etc/systemd/system/<app>.service` for `Scope::System`. `activation` returns
`[["systemctl", "--user", "daemon-reload"], ["systemctl", "--user", "enable", "--now", "<app>"]]`
or the system equivalents. `linger_hint` returns the `loginctl enable-linger` sentence for a
user unit, and `None` for a system one.

```bash
cargo test -p sapphire-framework-service --test golden
git commit -m "feat(service): render systemd units"
```

---

### Task 3: macOS and Windows

**Files:**
- Create: `crates/sapphire-framework-service/src/{launchd.rs,windows.rs}`
- Create: `crates/sapphire-framework-service/tests/golden/{launchagent.plist,task.xml}`
- Test: `tests/golden.rs`

**Interfaces:**
- `fn render_launch_agent(spec: &ServiceSpec, ctx: &InstallContext) -> String`
- `fn render_task(spec: &ServiceSpec, ctx: &InstallContext) -> String`
- `fn agent_path(app_name: &str, home: &Path) -> PathBuf` — `~/Library/LaunchAgents/<label>.plist`
- The label is `net.fireturtle.sapphire.<app>`, matching the repository owner in
  `Cargo.toml`'s `repository` — a LaunchAgent label is a global namespace and a generic one
  would collide.

**User level only on both.** LaunchDaemons and real Windows services can be added when someone
needs them; an install run with administrator rights fails with the Linux-only message rather
than quietly making a user-level thing.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_launch_agent() {
    check(
        "launchagent.plist",
        &render_launch_agent(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None)),
    );
}

#[test]
fn a_launch_agent_label_is_namespaced() {
    let rendered = render_launch_agent(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None));
    assert!(
        rendered.contains("net.fireturtle.sapphire.sapphire-agent"),
        "a LaunchAgent label is a global namespace: {rendered}"
    );
}

#[test]
fn a_scheduled_task() {
    check(
        "task.xml",
        &render_task(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None)),
    );
}

#[test]
fn a_scheduled_task_runs_at_logon() {
    let rendered = render_task(&spec(RunAs::InvokingUser, false), &ctx(Scope::User, None));
    assert!(rendered.contains("LogonTrigger"), "{rendered}");
}

#[test]
fn an_argument_with_a_space_survives_the_xml() {
    let mut with_space = spec(RunAs::InvokingUser, false);
    with_space.args = vec!["server".into(), "--note".into(), "a b".into()];
    let rendered = render_task(&with_space, &ctx(Scope::User, None));
    assert!(rendered.contains("\"a b\""), "{rendered}");
}

#[test]
fn an_ampersand_in_a_description_is_escaped() {
    let mut awkward = spec(RunAs::InvokingUser, false);
    awkward.description = "Notes & ledger".into();
    let rendered = render_task(&awkward, &ctx(Scope::User, None));
    assert!(rendered.contains("Notes &amp; ledger"), "{rendered}");
    assert!(!rendered.contains("Notes & ledger"), "unescaped XML: {rendered}");
}
```

The last two are the ones that break in the field: a path with a space, and an app description
with an ampersand, both produce a file the platform rejects with a message that says nothing
useful.

- [ ] **Step 2–4: Write the golden files, implement, verify, commit**

```bash
cargo test -p sapphire-framework-service --test golden
git commit -m "feat(service): render a LaunchAgent and a scheduled task"
```

---

### Task 4: `ServiceManager` and `ServiceCommand`

**Files:**
- Create: `crates/sapphire-framework-service/src/manager.rs`
- Modify: `crates/sapphire-framework-service/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `manager.rs`

**Interfaces:**
- Produces:
  - `trait ServiceManager { fn write_unit(&self, path: &Path, body: &str) -> Result<()>; fn run(&self, command: &[String]) -> Result<String>; fn remove_unit(&self, path: &Path) -> Result<()>; }`
  - `SystemManager` — the real one
  - `RecordingManager` — records calls and returns canned output
  - `ServiceCommand::{Install(InstallArgs), Uninstall, Status}` (`clap::Subcommand`)
  - `InstallArgs { user: bool, system: bool, run_as: Option<String>, keep_helper: bool }`
  - `fn install(spec: &ServiceSpec, args: &InstallArgs, env: &Environment, manager: &dyn ServiceManager) -> Result<InstallContext>`
  - `fn uninstall(...)`, `fn status(...)`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ServiceSpec {
        ServiceSpec {
            app_name: "sapphire-agent",
            description: "Sapphire agent server".into(),
            args: vec!["server".into(), "run".into()],
            system_run_as: RunAs::InvokingUser,
            privileges: None,
            post_install: None,
        }
    }

    fn privileges_for(run_as: &str, helper: &str) -> sapphire_server::PrivilegeConfig {
        sapphire_server::PrivilegeConfig {
            run_as: run_as.parse().unwrap(),
            helper: Some(sapphire_server::HelperSpec {
                user: helper.parse().unwrap(),
                program: std::path::PathBuf::from("/usr/lib/sapphire-agent/tool-broker"),
                args: vec![],
            }),
        }
    }

    fn linux_user() -> Environment {
        Environment { euid: 1000, sudo_user: None, os: Os::Linux }
    }

    fn linux_root() -> Environment {
        Environment { euid: 0, sudo_user: Some("alice".into()), os: Os::Linux }
    }

    #[test]
    fn installing_writes_a_unit_and_activates_it() {
        let manager = RecordingManager::default();
        install(&spec(), &InstallArgs::default(), &linux_user(), &manager).unwrap();

        let calls = manager.calls();
        assert_eq!(calls.units.len(), 1);
        assert!(calls.units[0].0.ends_with("sapphire-agent.service"), "{:?}", calls.units[0].0);
        assert!(
            calls.commands.iter().any(|c| c.contains(&"enable".to_owned())),
            "{:?}",
            calls.commands
        );
    }

    #[test]
    fn a_user_install_uses_the_user_flag() {
        let manager = RecordingManager::default();
        install(&spec(), &InstallArgs::default(), &linux_user(), &manager).unwrap();
        assert!(
            manager.calls().commands.iter().all(|c| c.contains(&"--user".to_owned())),
            "{:?}",
            manager.calls().commands
        );
    }

    #[test]
    fn a_system_install_does_not() {
        let manager = RecordingManager::default();
        install(&spec(), &InstallArgs::default(), &linux_root(), &manager).unwrap();
        assert!(
            manager.calls().commands.iter().all(|c| !c.contains(&"--user".to_owned())),
            "{:?}",
            manager.calls().commands
        );
    }

    #[test]
    fn a_failing_activation_leaves_no_unit_behind() {
        let manager = RecordingManager::failing_on("enable");
        assert!(install(&spec(), &InstallArgs::default(), &linux_user(), &manager).is_err());
        assert!(
            !manager.calls().removed.is_empty(),
            "a half-installed service is worse than none: the unit must be cleaned up"
        );
    }

    #[test]
    fn uninstalling_stops_disables_and_removes() {
        let manager = RecordingManager::default();
        uninstall(&spec(), &InstallArgs::default(), &linux_user(), &manager).unwrap();

        let calls = manager.calls();
        let flat: Vec<String> = calls.commands.iter().flatten().cloned().collect();
        assert!(flat.contains(&"disable".to_owned()), "{flat:?}");
        assert_eq!(calls.removed.len(), 1);
    }

    #[test]
    fn uninstalling_something_that_is_not_installed_is_not_an_error() {
        let manager = RecordingManager::failing_on("disable");
        uninstall(&spec(), &InstallArgs::default(), &linux_user(), &manager)
            .expect("uninstall is idempotent");
    }

    #[test]
    fn status_reports_what_the_manager_said() {
        let manager = RecordingManager::returning("active");
        let text = status(&spec(), &InstallArgs::default(), &linux_user(), &manager).unwrap();
        assert!(text.contains("active"), "{text}");
    }

    #[test]
    fn asking_for_both_scopes_is_refused() {
        let args = InstallArgs { user: true, system: true, ..InstallArgs::default() };
        let err = install(&spec(), &args, &linux_root(), &RecordingManager::default()).unwrap_err();
        assert!(err.to_string().contains("--user"), "{err}");
    }

    #[test]
    fn no_test_touches_the_real_service_manager() {
        // Stated as a test so the intent is visible where it can be read. `SystemManager` is
        // the only type that runs anything, and it is never constructed above.
        let source = include_str!("manager.rs");
        let constructions = source.matches("SystemManager").count();
        assert!(
            constructions <= 2,
            "SystemManager appears {constructions} times; tests must use RecordingManager"
        );
    }
}
```

`a_failing_activation_leaves_no_unit_behind` is the one to get right: a unit file written but
never enabled is invisible to `systemctl status` and springs to life at the next reboot.

- [ ] **Step 2–5: Implement, verify, commit**

```bash
cargo test -p sapphire-framework-service
git commit -m "feat(service): install, uninstall and report through a manager trait"
```

---

### Task 5: `post_install` and privilege separation

**Files:**
- Modify: `crates/sapphire-framework-service/src/{lib.rs,manager.rs}`
- Test: inline tests in `manager.rs`

**Interfaces:**
- `post_install` runs **after** activation, with the resolved `InstallContext`
- Files it writes into another user's directories are **chowned to that user**
- `InstallArgs::keep_helper` skips it

**What uses it:** an app that wants to leave something in the target user's configuration. The
sync spec's example — `sapphire-sync` writing `embedded_node = false` — is gone with
`embedded_node` itself, but the hook is not: `sapphire-agent` uses it to write the app's
`run_as` and `helper_as` into its config file so the server reads the same values the unit
names.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn post_install_runs_after_activation() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&order);
    let mut spec = spec();
    spec.post_install = Some(Box::new(move |_| {
        recorded.lock().unwrap().push("post_install");
        Ok(())
    }));

    let manager = RecordingManager::ordered(Arc::clone(&order));
    install(&spec, &InstallArgs::default(), &linux_user(), &manager).unwrap();

    let order = order.lock().unwrap().clone();
    assert_eq!(order.last().map(String::as_str), Some("post_install"), "{order:?}");
}

#[test]
fn post_install_sees_the_resolved_target_user() {
    let seen = Arc::new(Mutex::new(None));
    let recorded = Arc::clone(&seen);
    let mut spec = spec();
    spec.post_install = Some(Box::new(move |ctx| {
        *recorded.lock().unwrap() = ctx.target_user.clone();
        Ok(())
    }));

    install(&spec, &InstallArgs::default(), &linux_root(), &RecordingManager::default()).unwrap();
    assert_eq!(seen.lock().unwrap().as_deref(), Some("alice"));
}

#[test]
fn keep_helper_skips_post_install() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);
    let mut spec = spec();
    spec.post_install = Some(Box::new(move |_| {
        flag.store(true, Ordering::Relaxed);
        Ok(())
    }));

    let args = InstallArgs { keep_helper: true, ..InstallArgs::default() };
    install(&spec, &args, &linux_user(), &RecordingManager::default()).unwrap();
    assert!(!ran.load(Ordering::Relaxed));
}

#[test]
fn a_failing_post_install_fails_the_install_and_says_what_was_done() {
    let mut spec = spec();
    spec.post_install = Some(Box::new(|_| Err(Error::Config("no room".into()))));

    let err =
        install(&spec, &InstallArgs::default(), &linux_user(), &RecordingManager::default())
            .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("no room"), "{message}");
    assert!(
        message.contains("installed"),
        "the service is installed and running; say so rather than leaving it ambiguous: {message}"
    );
}

#[test]
fn a_privilege_separated_spec_installs_as_a_root_unit_whatever_run_as_says() {
    let mut spec = spec();
    spec.system_run_as = RunAs::InvokingUser;
    spec.privileges = Some(privileges_for("alice", "sapphire-agent-tools"));

    let manager = RecordingManager::default();
    install(&spec, &InstallArgs::default(), &linux_root(), &manager).unwrap();

    let body = &manager.calls().units[0].1;
    assert!(
        !body.contains("\nUser="),
        "an app that drops privileges itself must start as root: {body}"
    );
}
```

The last one closes a hole: a spec that asks for both `RunAs::InvokingUser` and privilege
separation is contradictory, and silently honouring `User=` would produce a service that
cannot do what it was configured to do. Privilege separation wins, and the doc comment says so.

- [ ] **Step 2–5: Implement, verify, commit**

```bash
cargo test -p sapphire-framework-service
git commit -m "feat(service): run post_install, and keep privilege separation coherent"
```

---

### Task 6: Wiring it into the apps

**Files:**
- Modify: `crates/sapphire-framework-server/src/command.rs`
- Modify: `crates/sapphire-framework-bridge/src/command.rs`
- Modify: `apps/sapphire-bridge/{Cargo.toml,src/main.rs}`
- Modify: `docs/ARCHITECTURE.md`
- Test: inline tests in both `command.rs` files

**Interfaces:**
- `ServerCommand::Service(ServiceCommand)` — the `service install | uninstall | status` the
  app-server plan left for this step
- `BridgeCommand::Service(ServiceCommand)`
- `AppServer::service_spec(&self) -> ServiceSpec` — so an app does not assemble one by hand

- [ ] **Step 1: Write the failing tests**

```rust
// In -server:
#[test]
fn the_service_subcommands_parse() {
    for args in [
        vec!["app", "service", "install"],
        vec!["app", "service", "install", "--system"],
        vec!["app", "service", "install", "--run-as", "alice"],
        vec!["app", "service", "uninstall"],
        vec!["app", "service", "status"],
    ] {
        assert!(Probe::try_parse_from(&args).is_ok(), "{args:?}");
    }
}

#[test]
fn the_generated_spec_runs_the_server_not_the_cli() {
    let spec = AppServer::new(&CTX, "0.0.0").service_spec();
    assert_eq!(spec.args, vec!["server".to_owned(), "run".to_owned()]);
}

#[test]
fn the_generated_spec_carries_the_apps_privileges() {
    let privileges = privileges_for("alice", "tools");
    let spec = AppServer::new(&CTX, "0.0.0")
        .privileges(privileges.clone())
        .service_spec();
    assert!(spec.privileges.is_some());
}

// In -bridge:
#[test]
fn the_bridge_service_spec_runs_the_bridge() {
    let spec = bridge_service_spec("0.0.0");
    assert_eq!(spec.args, vec!["run".to_owned()]);
    assert!(
        matches!(spec.system_run_as, RunAs::InvokingUser),
        "a root bridge would put the bridge directory under /root"
    );
}
```

- [ ] **Step 2–4: Implement, verify**

`AppServer` gains `privileges(PrivilegeConfig)` as a stored field for this purpose — it does
not apply them, which stays `privilege::apply`'s job in `main` (step 5's deviation) — and
`service_spec` reads it.

- [ ] **Step 5: Note it in `ARCHITECTURE.md` and commit**

```markdown
| `sapphire-framework-service` | OS のサービスマネージャへの登録（systemd user/system・LaunchAgent・タスクスケジューラ） | ✅ |
```

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
git add crates apps docs/ARCHITECTURE.md Cargo.lock
git commit -m "feat(service): add service install to the app servers and the bridge"
```

---

## What this plan does not cover

| | Left for |
|---|---|
| LaunchDaemons and real Windows services | when someone needs a service that runs without a login on those platforms |
| Verifying an install by actually starting the service | needs a machine to change; the privileged CI job of step 5 is the closest thing, and adding a service to it is a separate decision |
| Uninstalling a unit written by an older version whose path differs | there is no older version; when there is, `uninstall` gains a list of historical paths |
| Packaging (`.deb`, Homebrew, MSI) | out of scope for the framework |
