use clap::Parser;
use delcare_schema::altertable::{from_to, Wrapped};
use delcare_schema::schema::app_schema;
use serde::{Deserialize, Serialize};
use sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use sqlparser::ast::{ColumnDef, Ident, ObjectName, Statement};
use std::collections::HashMap;
use std::fs;

#[derive(Parser, Debug)]
pub struct Args {
    //#[arg(short, long)]
    //a_file: String,
    #[arg(short, long)]
    b_file: String,
    #[arg(short, long, value_enum, default_value = "DEBUG")]
    log_level: tracing::Level,
    #[arg(long, action)]
    log_json: bool,
    #[arg(short, long, default_value = "folio.toml")]
    config_file: String,
}

#[derive(Clone, Debug, Deserialize)]
struct AppConfig {
    database_url: String,
}

pub fn read_app_config(path: String) -> crate::AppConfig {
    let config_file_error_msg = format!("Could not read config file {}", path);
    let config_file_contents = fs::read_to_string(path).expect(&config_file_error_msg);
    let app_config: crate::AppConfig =
        toml::from_str(&config_file_contents).expect("Problems parsing config file");

    app_config
}
pub fn read_file(path: String) -> String {
    let file_error_msg = format!("Could not read file {}", path);
    let file_contents = fs::read_to_string(path).expect(&file_error_msg);
    file_contents
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("Args {args:?}");
    //let a_file = read_file(args.a_file);
    let b_file = read_file(args.b_file);
    let config = read_app_config(args.config_file);
    let pool = sqlx::PgPool::connect(&config.database_url).await?;

    let start_from_db = from_db(&pool).await?;
    //let mut current = sqlmo::Schema::try_from_postgres(&mut conn, "public").await.expect("Get schema");
    //current.name_schema("public");
    //
    //let start_state = app_schema(&a_file)?;
    let end_state = app_schema(&b_file)?;
    //let mut options = sqlmo::MigrationOptions::default();
    //options.allow_destructive = true;
    //let migration = current.migrate_to(end_state, &options).expect("Generate migrations");
    let end_tables: anyhow::Result<Vec<Wrapped>> = end_state
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_tables = end_tables.unwrap();

    //let from_tables: anyhow::Result<Vec<Wrapped>> = start_state
    //    .clone()
    //    .into_iter()
    //    .map(|s| Wrapped::try_from(s))
    //    .collect();
    //let from_tables = from_tables.unwrap();

    //for create in from_tables.clone() {

    //    //eprintln!("AST: {:?}", create);
    //    eprintln!("Creates: {}", create.to_string());
    //}

    //let a = Wrapped::try_from(start_state.first().unwrap().to_owned())?;
    //let b = Wrapped::try_from(end_state.first().unwrap().to_owned())?;
    //for s in from_to(from_tables, end_tables)? {
    //    println!("{}", s.to_string());
    //}

    for s in from_to(start_from_db, end_tables)? {
        println!("{}", s.to_string());
    }

    Ok(())
}

struct PGTable {
    table_schema: Option<String>,
    table_name: Option<String>,
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

async fn table_columns(
    c: &sqlx::PgPool,
    schema: String,
    table_name: String,
) -> anyhow::Result<Vec<ColumnDef>> {
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
        }
        r.push(ColumnDef {
            name: string_to_ident(dbtc.column_name)?,
            data_type: string_to_datatype(dbtc.data_type)?,
            collation: None,
            options: column_options,
        })
    }
    Ok(r)
}

#[derive(Clone, Debug)]
struct PGIndex {
    schemaname: Option<String>,
    tablename: Option<String>,
    indexname: Option<String>,
    indexdef: Option<String>,
}

async fn pg_indexes(pool: &sqlx::PgPool, schema: String) -> anyhow::Result<Vec<Wrapped>> {
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
        WHERE schemaname =  $1
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
) -> anyhow::Result<Vec<sqlparser::ast::TableConstraint>> {
    let mut r = Vec::new();

    let db_table_constraints = sqlx::query_as!(
        PGTableConstraint,
        "
        SELECT
            conname,
            pg_catalog.pg_get_constraintdef(r.oid, true) as definition
        FROM pg_catalog.pg_constraint r
        WHERE r.conrelid = $1::regclass
        ",
        table_name as _
    )
    .fetch_all(c)
    .await?;

    for dbtc in db_table_constraints {
        let s = format!(
            "CONSTRAINT {} {}",
            dbtc.conname.unwrap(),
            dbtc.definition.unwrap()
        );

        let c = string_to_table_constraint(Some(s))?;
        r.push(c)
    }
    Ok(r)
}

async fn from_db(pool: &sqlx::PgPool) -> anyhow::Result<Vec<Wrapped>> {
    //let r = Vec::new();
    let mut table_map: HashMap<ObjectName, CreateTableBuilder> = HashMap::new();
    let schema = "public";
    let db_tables = sqlx::query_as!(
        PGTable,
        "select table_schema, table_name from information_schema.tables where table_schema = $1",
        schema
    )
    .fetch_all(pool)
    .await?;

    for db_table in db_tables {
        if let Some(table_name) = db_table.table_name {
            let object_name = string_to_object_name(Some(table_name.clone()))?;
            let columns = table_columns(pool, schema.to_string(), table_name.clone()).await?;
            let constraints =
                table_constraints(pool, schema.to_string(), table_name.clone()).await?;
            let b = CreateTableBuilder::new(object_name.clone())
                .columns(columns)
                .constraints(constraints);

            table_map.insert(object_name, b);
        }
    }

    let re: anyhow::Result<Vec<Wrapped>> = table_map
        .values()
        .map(|v| Wrapped::try_from(v.to_owned().build()))
        .collect();
    let mut re = re?;

    let mut indexes = pg_indexes(&pool, schema.to_string()).await?;
    re.append(&mut indexes);
    Ok(re)
}

fn string_to_ident(os: Option<String>) -> anyhow::Result<Ident> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_identifier(false)?)
    } else {
        Err(anyhow::anyhow!("No string value"))
    }
}

fn string_to_object_name(os: Option<String>) -> anyhow::Result<ObjectName> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_object_name(false)?)
    } else {
        Err(anyhow::anyhow!("No string value"))
    }
}

fn string_to_datatype(os: Option<String>) -> anyhow::Result<sqlparser::ast::DataType> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        Ok(parser.parse_data_type()?)
    } else {
        Err(anyhow::anyhow!("No string value"))
    }
}

fn string_to_create_index(os: Option<String>) -> anyhow::Result<sqlparser::ast::Statement> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;

        println!("{:}", s);
        Ok(parser.parse_statement()?)
    } else {
        Err(anyhow::anyhow!("No string value"))
    }
}

fn string_to_table_constraint(
    os: Option<String>,
) -> anyhow::Result<sqlparser::ast::TableConstraint> {
    if let Some(s) = os {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(&s)?;
        if let Ok(Some(tc)) = parser.parse_optional_table_constraint() {
            return Ok(tc);
        } else {
            return Err(anyhow::anyhow!("Could not parse constraint"));
        }
    } else {
        return Err(anyhow::anyhow!("No string value"));
    }
}
