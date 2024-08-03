# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/philipcristiano/declare-schema/compare/v0.0.2...v0.0.3) - 2024-08-03

### Fixed
- Remove lockfile

## [0.0.2](https://github.com/philipcristiano/declare-schema/compare/v0.0.1...v0.0.2) - 2024-08-02

### Fixed
- *(deps)* update rust crate serde_json to v1.0.122
- *(deps)* update rust crate clap to v4.5.13
- *(deps)* update rust crate toml to v0.8.19
- *(deps)* update rust crate toml to v0.8.18
- *(deps)* update rust crate clap to v4.5.12
- *(deps)* update rust crate toml to v0.8.17
- *(deps)* update rust crate serde_json to v1.0.121
- *(deps)* update rust crate tokio to v1.39.2
- *(deps)* update rust crate toml to v0.8.16
- *(deps)* update rust crate clap to v4.5.11
- *(deps)* update rust crate sqlparser to 0.49.0
- *(deps)* update rust crate tokio to v1.39.1
- *(deps)* update rust crate clap to v4.5.10
- *(deps)* update rust crate sqlx to 0.8.0
- *(deps)* update rust crate toml to v0.8.15
- *(deps)* update rust crate thiserror to v1.0.63
- *(deps)* update rust crate tokio to v1.38.1
- *(deps)* update rust crate thiserror to v1.0.62
- *(deps)* update rust crate uuid to v1.10.0
- *(deps)* update rust crate tokio to v1.38.0
- *(deps)* update rust crate url to v2.5.2
- *(deps)* update rust crate toml to v0.8.14
- *(deps)* update rust crate serde_json to v1.0.120
- Remove unneeded dep service_conventions

### Other
- *(deps)* lock file maintenance
- *(deps)* update rust docker tag to v1.80
- *(deps)* lock file maintenance
- *(deps)* lock file maintenance
- *(deps)* lock file maintenance
- *(deps)* update rust docker tag to v1.79

## [0.0.1](https://github.com/philipcristiano/declare-schema/releases/tag/v0.0.1) - 2024-07-09

### Added
- Support CREATE EXTENSION
- Add `dump` command
- Expose way to generate migraitons but not run them
- ADD/ DROP CHECK CONSTRAINT

### Fixed
- Don't rely on oidc feature
- Remove debug
- Remove debug statement
- Get extensions from PG

### Other
- Enable publish
- Use released version of sqlparser
- ADD/DROP UNIQUE CONSTRAINT
- format
- Don't default to executing
- `migrate_from_string` fn
- Add info to README
- Fix main docker build
- Push to crates
- CLI
- module for postgres source
- ALTER TABLE SET/DROP DEFAULT
- Column SET NOT NULL / DROP NOT NULL
- Use sqlparser struct createtable
- A and B input files
- add/drop fk
- Add remove column, add pk
- Initial commit
