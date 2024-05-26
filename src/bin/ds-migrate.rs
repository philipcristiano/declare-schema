use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fs;
use delcare_schema::altertable::{WrappedCreateTable, from_to};
use delcare_schema::schema::app_schema;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long, default_value = "127.0.0.1:3002")]
    bind_addr: String,
    #[arg(short, long, default_value = "declare-schema.toml")]
    config_file: String,
    #[arg(short, long, value_enum, default_value = "DEBUG")]
    log_level: tracing::Level,
    #[arg(long, action)]
    log_json: bool,
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


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("Args {args:?}");
    let config = read_app_config(args.config_file);
    let pool = sqlx::PgPool::connect(&config.database_url).await.expect("Connection attempt");
    let mut conn = pool.acquire().await.expect("Get a connection");
    //let mut current = sqlmo::Schema::try_from_postgres(&mut conn, "public").await.expect("Get schema");
    //current.name_schema("public");
    //
    let schema_str = include_str!("../../schema/schema.sql");
    let from_str = include_str!("../../schema/from.sql");
    let end_state = app_schema(&schema_str)?;
    let start_state = app_schema(&from_str)?;
    //let mut options = sqlmo::MigrationOptions::default();
    //options.allow_destructive = true;
    //let migration = current.migrate_to(end_state, &options).expect("Generate migrations");
    let end_tables: anyhow::Result<Vec<WrappedCreateTable>> = end_state.clone().into_iter().map(|s| {WrappedCreateTable::try_from(s)}).collect();
    let end_tables = end_tables.unwrap();

    let from_tables: anyhow::Result<Vec<WrappedCreateTable>> = start_state.clone().into_iter().map(|s| {WrappedCreateTable::try_from(s)}).collect();
    let from_tables = from_tables.unwrap();

    //for create in from_tables.clone() {

    //    //eprintln!("AST: {:?}", create);
    //    eprintln!("Creates: {}", create.to_string());
    //}

    let a = WrappedCreateTable::try_from(start_state.first().unwrap().to_owned())?;
    let b = WrappedCreateTable::try_from(end_state.first().unwrap().to_owned())?;
    for s in from_to(from_tables, end_tables)? {
        eprintln!("I should execute: {}", s.to_string());

    };

    Ok(())

}

fn create_table_debug(statement: &sqlparser::ast::Statement) {
    let a = std::matches!(statement, sqlparser::ast::Statement::CreateTable {..});
    eprintln!("Wha? {:?}", a);
    match statement {
        sqlparser::ast::Statement::CreateTable{name, ..} => {
            eprintln!("Table Name {name}")
        },
        _ => panic!("")
    }

}
