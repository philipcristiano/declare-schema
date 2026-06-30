//! Diff SQL and a Postgres server to execute migrations
//!
//! Example
//! ```ignore
//! let pool = sqlx::PgPool::connect(env::var(key)?.as_str()).await?;
//! let target_schema = include_str!("schema.sql");
//! declare_schema::migrate_from_string(&target_schema, &pool).await?;
//! ```
//! Diff SQL and a Postgres server to generate migration statements
//!
//! Example
//! ```ignore
//! let pool = sqlx::PgPool::connect(env::var(key)?.as_str()).await?;
//! let target_schema = include_str!("schema.sql");
//! let steps = declare_schema::generate_migrations_from_string(&target_schema, &pool).await?;
//! for step in steps {
//!     println!("{}", step)
//! }
//! ```

/// Diff'ing of ASTs and statement generation
pub mod altertable;
mod declared_types;
/// str parsing to generate sqlparser ASTs
pub mod schema;
/// Postgres Server reading to generate sqlparser ASTs
pub mod source_postgres;
mod sqlparser_helpers;

use crate::declared_types::index::DeclaredIndex;
use altertable::Wrapped;
use sqlparser::ast::CreateTable;
use sqlparser::ast::TableConstraint;
use sqlx::PgPool;
use thiserror::Error;

/// The common error type for migration errors
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum MigrationError {
    #[error(
        "The table index cannot be modified yet: `From: {0} To: {1}`. Try adding a new index then dropping the old one"
    )]
    CannotModifyIndex(DeclaredIndex, DeclaredIndex),
    #[error(
        "The table constraint cannot be modified yet: From: `{0}` To: {1}. Try adding a new constraint then dropping the old one"
    )]
    CannotModifyTableConstraint(TableConstraint, TableConstraint),
    #[error("These are not the same tables {0} {1}")]
    TablesNotMatching(CreateTable, CreateTable),
    #[error("Problems while connecting/executing SQL")]
    ExecSqlError(#[from] sqlx::Error),
    #[error("Problems while parsing SQL")]
    SqlParseError(#[from] sqlparser::parser::ParserError),
    #[error("Problems while parsing SQL type: {0}")]
    SqlParseTypeError(String),
    #[error("Unsupported statement {0}")]
    UnsupportedStatementType(sqlparser::ast::Statement),
    #[error("Unsupported statement {0}")]
    UnnamedObject(altertable::Wrapped),
}
/// Diff a str with a DB and apply changes required to get the DB to match `str`
/// Postgres schema is detected with current_schema()

pub async fn migrate_from_string(to_schema: &str, pool: &PgPool) -> Result<(), MigrationError> {
    let current_schema = sqlx::query!("SELECT current_schema();")
        .fetch_one(pool)
        .await?
        .current_schema
        .expect("Couldn't get current schema");
    migrate_schema_from_string(&current_schema, to_schema, pool).await
}

/// Diff a str with a DB and apply changes required to get the DB to match `str`. Used when you want
/// to migrate a schema other than your current connection schema.
pub async fn migrate_schema_from_string(
    schema_name: &str,
    to_src: &str,
    pool: &PgPool,
) -> Result<(), MigrationError> {
    let src_state = crate::source_postgres::from_pool_schema(&pool, schema_name).await?;
    migrate_from_src(src_state, to_src, schema_name, &pool).await // ← pass schema_name
}

async fn migrate_from_src(
    src_state: Vec<Wrapped>,
    to_schema: &str,
    schema_name: &str, // ← add this
    pool: &PgPool,
) -> Result<(), MigrationError> {
    let end_statements = schema::app_schema(to_schema)?;
    let end_state: Result<Vec<Wrapped>, MigrationError> = end_statements
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_state = end_state?;
    let steps = crate::altertable::from_to(src_state, end_state)?;

    let mut conn = pool.acquire().await?;
    let q = format!("SET search_path TO \"{}\"", schema_name);
    let safe = sqlx::AssertSqlSafe(q);
    sqlx::query(safe).execute(&mut *conn).await?;
    sqlx::query("SET lock_timeout TO 5000")
        .execute(&mut *conn)
        .await?;
    for s in steps {
        #[cfg(test)]
        println!("{:?}", s.to_string());
        let safe = sqlx::AssertSqlSafe(s.to_string());
        sqlx::query(safe).execute(&mut *conn).await?;
    }
    Ok(())
}

/// Diff a str with a DB and return SQL changes required to get the DB to match `str`

pub async fn generate_migrations_from_string(
    src: &str,
    pool: &PgPool,
) -> Result<Vec<String>, MigrationError> {
    let src_state = crate::source_postgres::from_pool(&pool).await?;
    generate_migrations_for_source(src_state, &src).await
}
pub async fn generate_migrations_from_string_for_schema(
    schema: &str,
    to_src: &str,
    pool: &PgPool,
) -> Result<Vec<String>, MigrationError> {
    let src_state = crate::source_postgres::from_pool_schema(&pool, &schema).await?;
    generate_migrations_for_source(src_state, to_src).await
}

async fn generate_migrations_for_source(
    src_state: Vec<Wrapped>,
    to_schema: &str,
) -> Result<Vec<String>, MigrationError> {
    let end_statements = schema::app_schema(to_schema)?;
    let end_state: Result<Vec<Wrapped>, MigrationError> = end_statements
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_state = end_state?;
    let steps = crate::altertable::from_to(src_state, end_state)?;
    Ok(steps.into_iter().map(|f| f.to_string()).collect())
}
