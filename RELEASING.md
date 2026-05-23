# Releasing

Releases for this repository are automated with [release-plz](https://release-plz.dev/).

## Normal flow

1. When changes are pushed to `main`, release-plz automatically creates or updates a release PR.
2. Review the release PR (version, `CHANGELOG.md`, `Cargo.toml`).
3. Merging the PR creates the `v{version}` tag, the GitHub release, publishes to crates.io, and builds CLI binaries for each target platform.

## Version bump rules

release-plz infers the version from conventional commits (assuming pre-1.0 `0.x.y`).

- `feat:` / `fix:` / `chore(deps):` and other non-breaking changes → patch bump
- `feat!:` / `chore!:` or a `BREAKING CHANGE:` footer → minor bump
- Public API changes you make yourself (function signatures, public struct fields, etc.) → minor bump (mark the commit with `!`)

When an error type re-exposes a foreign crate via `#[from] some_crate::Error`, a major bump of that dependency is technically breaking, but the practical impact is usually nil. Patch is generally fine — this is the pragmatic stance most of the Rust ecosystem takes.

## Special case: `-sys` crate updates

A major bump of a `-sys` crate (`libgit2-sys`, `libsqlite3-sys`, `openssl-sys`, anything with `links = "..."` in its Cargo.toml) **must** become a minor bump on our side.

Reason: Cargo refuses to resolve a dependency graph that contains two different majors of the same `links`-bearing crate. If we ship a patch release that pulls in a new `-sys` major, downstream crates that pin a different `-sys` major fail to build. This is a hard error, not an API compatibility break, and the user cannot work around it.

The [Cargo SemVer guide's `-sys` section](https://doc.rust-lang.org/cargo/reference/semver.html) explicitly treats this as a major change.

### Dependencies to watch for

`-sys` crates currently reachable from this workspace:

- `git2` → `libgit2-sys`
- `rusqlite` → `libsqlite3-sys`
- `lancedb` may transitively pull in additional `-sys` crates through `arrow-*`

### How to handle it

When a release PR includes a dependency update, check the CHANGELOG. If any of the above are involved:

1. Push to the release PR branch and change `workspace.package.version` in `Cargo.toml` (and the matching internal dependency versions) from patch to minor.
2. Update the version heading (and the compare URL) in `CHANGELOG.md` (and any per-crate `CHANGELOG.md`) to match.
3. Add a one-line note to the CHANGELOG entry explaining why (e.g. "pulls in a new major of `libgit2-sys`").

## Required secrets

- `RELEASE_PLZ_TOKEN`: fine-grained PAT with Contents + Pull requests read/write.
- `CARGO_REGISTRY_TOKEN`: used for crates.io publishing.
