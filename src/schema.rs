use crate::MigrationError;
use sqlparser::ast::Statement;

pub fn app_schema(src: &str) -> Result<Vec<Statement>, MigrationError> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let parser = sqlparser::parser::Parser::new(&dialect);
    let mut parser = parser.try_with_sql(src)?;
    let ast = parser.parse_statements()?;

    Ok(ast)
}
