pub mod altertable;
pub mod schema;
pub mod source_postgres;

use altertable::Wrapped;
use sqlx::PgPool;

pub async fn migrate_from_string(src: &String, pool: &PgPool) -> anyhow::Result<()> {
    let src_state = crate::source_postgres::from_pool(&pool).await?;
    let end_statements = schema::app_schema(src)?;
    let end_state: anyhow::Result<Vec<Wrapped>> = end_statements
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
    src: &String,
    pool: &PgPool,
) -> anyhow::Result<Vec<String>> {
    let src_state = crate::source_postgres::from_pool(&pool).await?;
    let end_statements = schema::app_schema(src)?;
    let end_state: anyhow::Result<Vec<Wrapped>> = end_statements
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_state = end_state?;
    let steps = crate::altertable::from_to(src_state, end_state)?;

    Ok(steps.into_iter().map(|f| f.to_string()).collect())
}
