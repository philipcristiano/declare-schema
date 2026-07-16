use crate::MigrationError;
use crate::altertable::Wrapped;
use sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use sqlparser::ast::{ColumnDef, CreateExtension, CreateView, Ident, ObjectName};
use std::collections::HashMap;

#[derive(Clone, Debug)]
struct PGIndex {
    schemaname: Option<String>,
    tablename: Option<String>,
    indexname: Option<String>,
    indexdef: Option<String>,
}

async fn pg_indexes(pool: &sqlx::PgPool, schema: String) -> Result<Vec<Wrapped>, MigrationError> {
    let mut r = Vec::new();
    let db_indexes = sqlx::query_as!(
        PGIndex,
        "SELECT
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_catalog.pg_indexes AS pgi
        LEFT JOIN information_schema.table_constraints as tc
        ON pgi.indexname = tc.constraint_name
        WHERE schemaname = $1
        AND constraint_name IS NULL ",
        schema
    )
    .fetch_all(pool)
    .await?;

    for dbi in db_indexes {
        if let Some(def) = dbi.indexdef.clone() {
            eprintln!("Index {def}");
        }
        let c = string_to_create_index(dbi.indexdef)?;
        let w = Wrapped::try_from(c)?;
        r.push(w)
    }
    Ok(r)
}

async fn table_constraints(
    c: &sqlx::PgPool,
    schema: String,
    table_name: String,
) -> Result<Vec<sqlparser::ast::TableConstraint>, MigrationError> {
    let mut r = Vec::new();

    let name = format!("{schema}.{table_name}");
    let db_table_constraints = sqlx::query_as!(
        PGTableConstraint,
        "
        SELECT
            conname,
            pg_catalog.pg_get_constraintdef(r.oid, true) as definition
        FROM pg_catalog.pg_constraint r
        WHERE r.conrelid = $1::regclass
        ",
        name as _
    )
    .fetch_all(c)
    .await?;

    for dbtc in db_table_constraints {
        let s = format!(
            "CONSTRAINT {} {};",
            dbtc.conname.unwrap(),
            dbtc.definition.clone().unwrap()
        );

        // NOT NULL constraints are included but cannot be processed here
        if !dbtc.definition.unwrap().starts_with("NOT NULL") {
            let c = string_to_table_constraint(Some(s))?;
            r.push(c)
        }
    }
    Ok(r)
}

#[derive(Clone, Debug)]
struct PGExtension {
    extname: Option<String>,
}

async fn pg_extensions(c: &sqlx::PgPool) -> Result<Vec<Wrapped>, MigrationError> {
    let mut r = Vec::new();

    let db_extensions = sqlx::query_as!(
        PGExtension,
        "
        SELECT
            extname

        FROM pg_extension pge
        "
    )
    .fetch_all(c)
    .await?;

    for ext in db_extensions {
        let name_ident = string_to_ident(ext.extname)?;
        let statement = sqlparser::ast::Statement::CreateExtension(CreateExtension {
            name: name_ident,
            cascade: false,
            if_not_exists: false,
            schema: None,
            version: None,
        });
        let wrapped = Wrapped::try_from(statement)?;
        r.push(wrapped);
    }
    Ok(r)
}

struct PGTable {
    table_schema: Option<String>,
    table_name: Option<String>,
    table_type: Option<PGTableType>,
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "UPPERCASE")]
enum PGTableType {
    #[sqlx(rename = "BASE TABLE")]
    BaseTable,
    #[sqlx(rename = "VIEW")]
    View,
}
#[derive(Clone, Debug)]
struct PGTableColumn {
    table_schema: Option<String>,
    table_name: Option<String>,
    column_name: Option<String>,
    ordinal_position: Option<i32>,
    column_default: Option<String>,
    is_nullable: Option<String>,
    data_type: Option<String>,
}
#[derive(Clone, Debug)]
struct PGTableConstraint {
    conname: Option<String>,
    definition: Option<String>,
}

#[derive(Clone, Debug)]
struct PGView {
    view_definition: Option<String>,
}

async fn pg_view(
    c: &sqlx::PgPool,
    schema: &str,
    view_name: Option<String>,
) -> Result<Wrapped, MigrationError> {
    if let Some(name) = view_name {
        let db_view = sqlx::query_as!(
            PGView,
            "SELECT
                view_definition
            FROM information_schema.views
            WHERE table_schema = $1
            AND table_name = $2",
            schema,
            name
        )
        .fetch_one(c)
        .await?;
        #[cfg(test)]
        println!("view def source {:?}", db_view.view_definition);
        let statement = string_to_query(db_view.view_definition)?;
        let v = CreateView {
            cluster_by: vec![],
            columns: vec![],
            comment: None,
            copy_grants: false,
            if_not_exists: false,
            name: string_to_object_name(Some(name))?,
            materialized: false,
            name_before_not_exists: false,
            or_alter: false,
            query: statement,
            or_replace: false,
            secure: false,
            temporary: false,
            to: None,
            options: sqlparser::ast::CreateTableOptions::None,
            with_no_schema_binding: false,
            params: None,
        };
        Ok(Wrapped::CreateView(v))
    } else {
        Err(MigrationError::PGSourceViewError(
            "No name for view found".to_string(),
        ))
    }
}
async fn table_columns(
    c: &sqlx::PgPool,
    schema: String,
    table_name: String,
) -> Result<Vec<ColumnDef>, MigrationError> {
    let mut r = Vec::new();

    let db_table_columns = sqlx::query_as!(
        PGTableColumn,
        "SELECT
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            column_default,
            is_nullable,
            data_type
        FROM information_schema.columns
        WHERE table_schema = $1
        AND table_name = $2
        ORDER BY ordinal_position",
        schema,
        table_name
    )
    .fetch_all(c)
    .await?;
    for dbtc in db_table_columns {
        let mut column_options = Vec::new();
        match dbtc.is_nullable {
            Some(val) => {
                if val == "NO" {
                    column_options.push(sqlparser::ast::ColumnOptionDef {
                        name: None,
                        option: sqlparser::ast::ColumnOption::NotNull,
                    })
                } else if val == "YES" {
                } else {
                    eprintln!("UNHANDLED VALUE is_nullable {val}",)
                }
            }
            None => {
                eprintln!("Column unknown nullable TODO FIXME")
            }
        };

        if dbtc.column_default.is_some() {
            match string_to_expr(dbtc.column_default) {
                Ok(expr) => column_options.push(sqlparser::ast::ColumnOptionDef {
                    name: None,
                    option: sqlparser::ast::ColumnOption::Default(expr),
                }),
                Err(e) => {
                    eprintln!("Column unknown default /error {e}")
                }
            }
        }
        r.push(ColumnDef {
            name: string_to_ident(dbtc.column_name)?,
            data_type: string_to_datatype(dbtc.data_type)?,
            options: column_options,
        })
    }
    Ok(r)
}

pub async fn from_pool(pool: &sqlx::PgPool) -> Result<Vec<Wrapped>, MigrationError> {
    let current_schema = sqlx::query!("SELECT current_schema();")
        .fetch_one(pool)
        .await?
        .current_schema
        .expect("Couldn't get current schema");
    let db_tables = sqlx::query_as!(
        PGTable,
        "select table_schema, table_name, table_type as \"table_type: PGTableType\" from information_schema.tables where table_schema = current_schema()",
    )
    .fetch_all(pool)
    .await?;
    tables_to_wrapped(pool, db_tables, current_schema.as_str()).await
}
pub async fn from_pool_schema(
    pool: &sqlx::PgPool,
    schema: &str,
) -> Result<Vec<Wrapped>, MigrationError> {
    let db_tables = sqlx::query_as!(
        PGTable,
        "select table_schema, table_name, table_type as \"table_type: PGTableType\" from information_schema.tables where table_schema = $1",
        schema
    )
    .fetch_all(pool)
    .await?;
    tables_to_wrapped(pool, db_tables, schema).await
}

async fn tables_to_wrapped(
    pool: &sqlx::PgPool,
    db_tables: Vec<PGTable>,
    schema: &str,
) -> Result<Vec<Wrapped>, MigrationError> {
    let mut table_map: HashMap<ObjectName, CreateTableBuilder> = HashMap::new();
    let mut views: Vec<Wrapped> = vec![];
    for db_table in db_tables {
        let table_schema = db_table.table_schema.expect("Table needs a schema");
        #[cfg(test)]
        println!("table {:?}.{:?}", schema, db_table.table_name);
        match &db_table.table_type {
            Some(PGTableType::BaseTable) => {
                if let Some(table_name) = db_table.table_name {
                    let object_name = string_to_object_name(Some(table_name.clone()))?;
                    let columns =
                        table_columns(pool, table_schema.to_string(), table_name.clone()).await?;
                    let constraints =
                        table_constraints(pool, schema.to_string(), table_name.clone()).await?;
                    let b = CreateTableBuilder::new(object_name.clone())
                        .columns(columns)
                        .constraints(constraints);

                    table_map.insert(object_name, b);
                }
            }
            Some(PGTableType::View) => {
                let create_view = pg_view(pool, &schema, db_table.table_name).await?;
                #[cfg(test)]
                println!("create_view: {create_view}");
                views.push(create_view);
            }

            _ => {
                panic!("Unhandled!")
            }
        }
    }

    let re: Result<Vec<Wrapped>, MigrationError> = table_map
        .values()
        .map(|v| Wrapped::try_from(sqlparser::ast::Statement::CreateTable(v.to_owned().build())))
        .collect();
    let mut re = re?;

    re.append(&mut views);
    let mut indexes = pg_indexes(&pool, schema.to_string()).await?;
    re.append(&mut indexes);
    let mut extensions = pg_extensions(&pool).await?;
    re.append(&mut extensions);
    Ok(re)
}

fn string_to_ident(os: Option<String>) -> Result<Ident, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_identifier()?)
    } else {
        Err(MigrationError::SqlParseTypeError(
            "No string value".to_string(),
        ))
    }
}

fn string_to_object_name(os: Option<String>) -> Result<ObjectName, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_object_name(false)?)
    } else {
        Err(MigrationError::SqlParseTypeError(
            "No string value".to_string(),
        ))
    }
}

fn string_to_expr(os: Option<String>) -> Result<sqlparser::ast::Expr, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_expr()?)
    } else {
        Err(MigrationError::SqlParseTypeError(
            "No expr value".to_string(),
        ))
    }
}

fn string_to_datatype(os: Option<String>) -> Result<sqlparser::ast::DataType, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_data_type()?)
    } else {
        Err(MigrationError::SqlParseTypeError(
            "No string value".to_string(),
        ))
    }
}

fn string_to_create_index(os: Option<String>) -> Result<sqlparser::ast::Statement, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;

        Ok(parser.parse_statement()?)
    } else {
        Err(MigrationError::SqlParseTypeError(
            "No string value".to_string(),
        ))
    }
}

fn string_to_query(os: Option<String>) -> Result<Box<sqlparser::ast::Query>, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;

        let (tf, q) = parser.parse_as_query()?;
        Ok(q)
    } else {
        Err(MigrationError::SqlParseTypeError(
            "No string value".to_string(),
        ))
    }
}

fn string_to_table_constraint(
    os: Option<String>,
) -> Result<sqlparser::ast::TableConstraint, MigrationError> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        if let Ok(Some(tc)) = parser.parse_optional_table_constraint() {
            return Ok(tc);
        } else {
            return Err(MigrationError::SqlParseTypeError(
                "Could not parse constraint".to_string(),
            ));
        }
    } else {
        return Err(MigrationError::SqlParseTypeError(
            "No string value".to_string(),
        ));
    }
}
