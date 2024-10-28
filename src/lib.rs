pub mod altertable;
pub mod schema;
pub mod source_postgres;

use altertable::Wrapped;
use sqlparser::ast::CreateTable;
use sqlparser::ast::TableConstraint;
use sqlx::PgPool;
use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("The table index cannot be modified yet: `From: {0} To: {1}`. Try adding a new index then dropping the old one")]
    CannotModifyIndex(sqlparser::ast::CreateIndex, sqlparser::ast::CreateIndex),
    #[error("The table constraint cannot be modified yet: `{0}`. Try adding a new constraint then dropping the old one")]
    CannotModifyTableConstraint(TableConstraint),
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

pub async fn migrate_from_string(src: &str, pool: &PgPool) -> Result<(), MigrationError> {
    let src_state = crate::source_postgres::from_pool(&pool).await?;
    let end_statements = schema::app_schema(src)?;
    let end_state: Result<Vec<Wrapped>, MigrationError> = end_statements
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_state = end_state?;
    let steps = crate::altertable::from_to(src_state, end_state)?;

    let mut conn = pool.acquire().await?;
    sqlx::query("SET lock_timeout TO 5000")
        .execute(&mut *conn)
        .await?;
    for s in steps {
        sqlx::query(&s.to_string()).execute(&mut *conn).await?;
    }
    Ok(())
}

pub async fn generate_migrations_from_string(
    src: &str,
    pool: &PgPool,
) -> Result<Vec<String>, MigrationError> {
    let src_state = crate::source_postgres::from_pool(&pool).await?;
    let end_statements = schema::app_schema(src)?;
    let end_state: Result<Vec<Wrapped>, MigrationError> = end_statements
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_state = end_state?;
    let steps = crate::altertable::from_to(src_state, end_state)?;

    Ok(steps.into_iter().map(|f| f.to_string()).collect())
}
