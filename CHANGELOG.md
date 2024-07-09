# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
