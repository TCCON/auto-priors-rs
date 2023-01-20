use std::env;

use anyhow::{self, Context};
use dotenv;
use log;
use sqlx;
use sqlx::migrate::Migrator;

pub mod config;
pub mod utils;
pub mod geos;
pub mod siteinfo;
pub mod jobs;
pub mod stdsitejobs;

pub type MySqlPool = sqlx::pool::Pool<sqlx::MySql>;
pub type MySqlPC = sqlx::pool::PoolConnection<sqlx::MySql>;
pub type MySqlConn = sqlx::MySqlConnection;
static DB_ENV_VARS: [&'static str; 2] = ["PRIORS_DATABASE_URL", "DATABASE_URL"];

static MIGRATOR: Migrator = sqlx::migrate!();

pub async fn apply_migrations(conn: &mut MySqlConn) -> anyhow::Result<()> {
    Ok(MIGRATOR.run(conn).await?)
}

pub async fn unapply_migrations(conn: &mut MySqlConn, target: i64) -> anyhow::Result<()> {
    Ok(MIGRATOR.undo(conn, target).await?)
}

pub fn get_database_url(url_in: Option<String>) -> anyhow::Result<String> {
    if let Some(url) = url_in {
        return Ok(url)
    }

    // First, try the regular environmental variables
    for key in DB_ENV_VARS {
        if let Ok(val) = env::var(key) {
            log::info!("Using database URL {val} from the environmental variable {key}");
            return Ok(val)
        }
    }

    // If we can't find the URL in existing environmental variables, try using dotenv.
    let env_path = dotenv::dotenv().context("No database URL defined in existing environmental variables, and no .env file found.")?;
    for key in DB_ENV_VARS {
        if let Ok(val) = dotenv::var(key) {
            let epd = env_path.display();
            log::info!("Using database URL {val} from the variable {key} in {epd}");
            return Ok(val)
        }
    }

    return Err(anyhow::anyhow!("Unable to find database URL."))
}

pub async fn get_database_pool(url_in: Option<String>) -> anyhow::Result<sqlx::MySqlPool> {
    let url = get_database_url(url_in)?;
    let pool = sqlx::MySqlPool::connect(&url).await?;
    // This was how I tried to make this synchronous
    // let pool = Runtime::new()?.block_on(future)?;
    return Ok(pool)
}

pub fn hello(name: &str) -> String {
    return format!("Hello, {name}!")
}