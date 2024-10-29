use clap::{Parser, Subcommand};
use std::env;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long, default_value = "et.toml")]
    config_file: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Migrate,
    Print,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("Args {args:?}");
    let pool = sqlx::PgPool::connect(env::var("DATABASE_URL")?.as_str())
        .await
        .expect("Connection attempt");
    let target_schema = include_str!("../../schema/schema.sql").to_string();

    match &args.command {
        Commands::Migrate {} => {
            declare_schema::migrate_from_string(&target_schema, &pool).await?;
        }
        Commands::Print => {
            let steps =
                declare_schema::generate_migrations_from_string(&target_schema, &pool).await?;
            for step in steps {
                println!("{}", step)
            }
        }
    }
    Ok(())
}
