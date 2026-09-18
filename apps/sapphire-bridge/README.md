# sapphire-bridge

The host-wide sapphire daemon. One process per OS user, shared by every sapphire application
on the machine.

> 日本語版: [README.ja.md](README.ja.md)

## What it is

Apps each run their own app server, and that server owns a workspace's cache — a file only one
process may open. The bridge sits above them. It holds this host's device identity, knows which
workgroups the host belongs to, and tells an app server where its peers are.

It never looks inside a workspace: it routes *to* the app server that owns one, and decides
*whether* a peer's bytes may flow, never what they mean.

## Running it

```console
$ sapphire-bridge            # same as `sapphire-bridge run`
$ sapphire-bridge status
$ sapphire-bridge workgroup create home --device-name laptop
$ sapphire-bridge device list
```

With no subcommand at all, the bridge starts and stays in the foreground. Every other
subcommand is a one-shot command against a running bridge — except `workgroup create` and
`device forget`, which work directly on the bridge directory because there is nothing to ask
yet (or, for `forget`, no control-plane method).

Starting a second bridge is not an error: it reports the pid of the one already running and
exits non-zero.

## Commands

| Command | What it does |
|---|---|
| `run` (default) | Run the bridge in the foreground |
| `status` | Report the running bridge's version, node id, workgroup and registered workspaces |
| `device list` | List the workgroup's devices, and which are reachable |
| `device forget <selector>` | Retire a device by name or id |
| `workgroup create <name> --device-name <name>` | Found a workgroup, recording this host as its first device |
| `workgroup list` | Show the workgroup this host belongs to |
| `workspace list` | List the workspaces this host serves (read-only) |

`pair`, `workgroup join` and invites are not implemented yet.

## Where it keeps things

`<platform data root>/sapphire/bridge/`, overridden whole by `SAPPHIRE_BRIDGE_DIR`:

```
bridge/
    format          # directory format version
    node.key        # iroh secret key -> this device's node id
    bridge.lock     # single-instance guard, not a role election
    net.toml        # discovery, relays, wake_on_sync
    routes.toml     # workspace_id -> the app server that owns it
    workgroups/<workgroup-id>/
        root/       # device ledger
        root/devices/<device-id>.toml
```

The directory is mode `0700` on Unix, and `node.key` is `0600`: that file **is** this device's
identity.

## Configuration

`net.toml`, every field optional:

```toml
wake_on_sync = true    # start a stopped app server when a peer asks for its workspace
discovery = true       # use discovery services to find peers
relays = []            # relay URLs; empty disables relays
```

`RUST_LOG` sets the log filter, as everywhere else in the framework.

## License

MIT OR Apache-2.0.
