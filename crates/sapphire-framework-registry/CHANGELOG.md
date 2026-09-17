# Changelog

All notable changes to `sapphire-workspace` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.15.0](https://github.com/fluo10/sapphire-framework/compare/sapphire-framework-registry-v0.14.0...sapphire-framework-registry-v0.15.0) - 2026-09-17

### Added

- *(registry)* migrate a devices.toml into per-device record files
- *(registry)* [**breaking**] store one file per device so concurrent pairings never collide
- *(registry)* record each device's iroh node id
- *(registry)* [**breaking**] drop users; every device belongs to one person

### Fixed

- *(registry)* refuse non-canonical record file names; English comments in store/error

### Other

- *(registry)* rewrite the README for the per-device file layout
