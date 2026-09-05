# Changelog

All notable changes to `sapphire-workspace` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.14.0](https://github.com/fluo10/sapphire-framework/compare/sapphire-framework-v0.13.0...sapphire-framework-v0.14.0) - 2026-09-05

### Added

- *(registry)* device table, whose ids get written into content
- *(registry)* user table with the key file's own discipline
- *(remote-server)* [**breaking**] identify API keys by UUID again
- *(remote-server)* [**breaking**] identify API keys by grain-id instead of UUID
- *(gui)* shared sapphire-framework-gui crate — WorkspaceManager ([#86](https://github.com/fluo10/sapphire-framework/pull/86))
- *(backend)* shared workspace registry + selection resolver
- add sapphire-framework facade crate skeleton ([#95](https://github.com/fluo10/sapphire-framework/pull/95))

### Fixed

- *(registry,remote-server)* address code-review findings A-J
- *(remote-server)* the minors from the whole-branch review

### Other

- *(release)* resume publishing at 0.13.0, with every crate in the framework group ([#123](https://github.com/fluo10/sapphire-framework/pull/123))
- Merge pull request #115 from fluo10/feat/api-key-rotation
