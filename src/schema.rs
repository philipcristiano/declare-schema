use crate::MigrationError;
use sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use sqlparser::ast::{ColumnDef, Ident, ObjectName, Statement};

pub fn app_schema(src: &str) -> Result<Vec<Statement>, MigrationError> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let parser = sqlparser::parser::Parser::new(&dialect);
    let mut parser = parser.try_with_sql(src)?;
    let ast = parser.parse_statements()?;
    //let name = ObjectName(vec![Ident::new("items")]);
    //let ct = CreateTableBuilder::new(name);
    //let id = ColumnDef{

    //};
    //ct.columns

    Ok(ast)
}
