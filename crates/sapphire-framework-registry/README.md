# sapphire-framework-registry

The per-app device ledger. One directory, one record file per device —
`<dir>/<grain-id>.toml` — read and written through `Devices`.

```rust
use sapphire_framework::registry::Devices;

let mut devices = Devices::open(&workgroup_dir.join("devices"))?;
let pendant = devices.add("pendant", None, Some("首から下げるやつ".into()))?;
println!("{}", pendant.id); // e.g. "a3f9k2p"
```

## Why one file per device

Each mutation rewrites exactly one record file, so two hosts mutating the
ledger at the same moment never collide — they write different files. The
id is the file name and is never repeated inside the file.

## IDs close inside the app

A `Device` id means something only inside that app's ledger; apps do not
share ids. sapphire-journal / sapphire-ledger / sapphire-agent each appear
to one another as a single client device, so there is nothing to align.

`device.id` is **persisted into content** (a journal entry's `updated_by`,
say). So removal is a tombstone (`retired_at`) by default, and a record is
deleted physically only by an explicit `purge`. Revoking access is not the
ledger's job — that is the server's key file (`KeyStore::revoke`).

## Relation to keys

`KeyEntry.device_id` points at a ledger entry, not the other way round,
because the key file exists per host while the ledger exists per workspace —
if one physical device talks to two servers, it has two keys in two separate
files.

## Migration

`migrate_single_file` converts a legacy single-file `devices.toml` (with its
`[[device]]` tables and optional per-record `id` / `user_id` fields) into
per-device record files, idempotently and without touching the old file.
