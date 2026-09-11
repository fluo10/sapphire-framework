# Changelog

All notable changes to `sapphire-workspace` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.14.1](https://github.com/fluo10/sapphire-framework/compare/v0.14.0...v0.14.1) - 2026-09-11

### Added

- *(workspace)* WorkspaceArgs + clap/serde/dirs re-exports; facade passthrough ([#128](https://github.com/fluo10/sapphire-framework/pull/128))
- *(workspace)* AppContext::init — per-kind layout, migration, unified env resolution ([#129](https://github.com/fluo10/sapphire-framework/pull/129))
- *(workspace)* app_dirs module — per-kind layout, migration, env-name rule (#129, #128)

### Fixed

- *(workspace)* final-review findings — AppKind prelude export, init panic messages, keys.toml doc, test hygiene
- *(workspace)* app_dirs move fallback — remove_file for file sources, else-if collapse, error context
- *(workspace)* app_dirs review fixes — cross-device move fallback, once-per-uuid keys guard, AppKind::as_str by value

## [0.14.0](https://github.com/fluo10/sapphire-framework/compare/v0.13.0...v0.14.0) - 2026-09-08

### Fixed

- *(track)* store mtimes in nanoseconds and compare file size too ([#118](https://github.com/fluo10/sapphire-framework/pull/118))
