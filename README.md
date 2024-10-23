# declare-schema
Experiments with Rust declarative schemas

Use [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) for SQL -> AST, then diff ASTs to generate ALTER ASTs, then output SQL.

WARNING: Alpha software, you will likely lose data with this library.

## Goal

### In application

Include database schema management in to Rust applications without the need for migration steps.

### As a CLI

Provide a CLI tool that can generate diffs of schemas and the required ALTER statements for review to detect schema drift.

### Easy development experience

When embedded in an application or with a CLI tool keep an easy SQL -> DB flow that is clear to developers with an easy to modify schema.

## Current State

### Limitations

`CREATE EXTENSION` - Can be created by name only. Cannot be `DROP`ed.
