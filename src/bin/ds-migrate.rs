use clap::Parser;
use declare_schema::altertable::{from_to, Wrapped};
use declare_schema::schema::app_schema;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{stdin,stdout,Write};

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    to: String,
    #[arg(short, long, value_enum, default_value = "DEBUG")]
    log_level: tracing::Level,
    #[arg(long, action)]
    log_json: bool,
    #[arg(long, action)]
    execute: bool,
    #[arg(long, action, default_value = "true")]
    apply_execute: bool
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
    let to_file = read_file(args.to);
    let pg_connect_opts = sqlx::postgres::PgConnectOptions::new();
    let pool = sqlx::PgPool::connect_with(pg_connect_opts).await?;

    let start_from_db = declare_schema::source_postgres::from_pool(&pool).await?;
    let end_state = app_schema(&to_file)?;
    let end_tables: anyhow::Result<Vec<Wrapped>> = end_state
        .clone()
        .into_iter()
        .map(|s| Wrapped::try_from(s))
        .collect();
    let end_tables = end_tables.unwrap();

    let steps = from_to(start_from_db, end_tables)?;
    for s in steps.clone() {
        println!("{};", s.to_string());
    };
    if args.execute {
        if !args.apply_execute {
            println!("Apply? (y/N)");
            let mut input=String::new();
            let _=stdout().flush();
            stdin().read_line(&mut input).expect("Did not enter a correct string");
            if input.to_lowercase().trim() != "y".to_string() {
                println!("Not executing");
                return Ok(())
            }
        }
        println!("Executing!");

        let mut conn = pool.acquire().await?;
        sqlx::query("SET lock_timeout TO 5000").execute(&mut *conn).await?;
        for s in steps {
            println!("Executing statement: {}", s);
            sqlx::query(&s.to_string()).execute(&mut *conn).await?;
            println!("Executed.");
        }
    }

    Ok(())
}
