# Changelog

All notable changes to `sapphire-workspace` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.1.0](https://github.com/fluo10/sapphire-framework/releases/tag/sapphire-framework-sync-v0.1.0) - 2026-07-16

### Added

- migrate sapphire-workspace into sapphire-framework; pure-Rust redb+tantivy cache

## [0.12.1](https://github.com/fluo10/sapphire-workspace/compare/sapphire-sync-v0.12.0...sapphire-sync-v0.12.1) - 2026-05-24

### Fixed

- *(sync)* enable git2 ssh/https features explicitly

## [0.12.0](https://github.com/fluo10/sapphire-workspace/compare/sapphire-sync-v0.11.0...sapphire-sync-v0.12.0) - 2026-05-23

### Changed

- Bump `git2` from 0.20 to 0.21. This pulls in a new major of `libgit2-sys` (a `-sys` crate with a `links` key); released as a minor bump (not patch) to prevent build failures for downstream crates that depend on a different `libgit2-sys` major.
