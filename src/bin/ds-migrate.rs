use clap::Parser;
use delcare_schema::altertable::{from_to, Wrapped};
use delcare_schema::schema::app_schema;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    a_file: String,
    #[arg(short, long)]
    b_file: String,
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
pub fn read_file(path: String) -> String {
    let file_error_msg = format!("Could not read file {}", path);
    let file_contents = fs::read_to_string(path).expect(&file_error_msg);
    file_contents
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("Args {args:?}");
    let a_file = read_file(args.a_file);
    let b_file = read_file(args.b_file);
    //let config = read_app_config(args.config_file);
    //let pool = sqlx::PgPool::connect(&config.database_url)
    //    .await
    //    .expect("Connection attempt");
    //let mut conn = pool.acquire().await.expect("Get a connection");
    //let mut current = sqlmo::Schema::try_from_postgres(&mut conn, "public").await.expect("Get schema");
    //current.name_schema("public");
    //
    let start_state = app_schema(&a_file)?;
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

    let from_tables: anyhow::Result<Vec<Wrapped>> = start_state
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let from_tables = from_tables.unwrap();

    //for create in from_tables.clone() {

    //    //eprintln!("AST: {:?}", create);
    //    eprintln!("Creates: {}", create.to_string());
    //}

    let a = Wrapped::try_from(start_state.first().unwrap().to_owned())?;
    let b = Wrapped::try_from(end_state.first().unwrap().to_owned())?;
    for s in from_to(from_tables, end_tables)? {
        println!("{}", s.to_string());
    }

    Ok(())
}
