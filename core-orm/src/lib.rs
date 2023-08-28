use std::env;

use anyhow::{self, Context};
use dotenv;
use log;
use sqlx;
use sqlx::migrate::Migrator;

pub mod error;
pub mod config;
pub mod utils;
pub mod email;
pub mod met;
pub mod siteinfo;
pub mod jobs;
pub mod input_files;
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
            log::debug!("Using database URL {val} from the environmental variable {key}");
            return Ok(val)
        }
    }

    // If we can't find the URL in existing environmental variables, try using dotenv.
    let env_path = dotenv::dotenv().context("No database URL defined in existing environmental variables, and no .env file found.")?;
    for key in DB_ENV_VARS {
        if let Ok(val) = dotenv::var(key) {
            let epd = env_path.display();
            log::debug!("Using database URL {val} from the variable {key} in {epd}");
            return Ok(val)
        }
    }

    return Err(anyhow::anyhow!("Unable to find database URL."))
}

/// A wrapper around a [`sqlx::MySqlPool`] that helps enforce certain access conditions.
/// 
/// For the priors code, we want to enforce the safest behavior regarding transactions'
/// interaction with each other. That means setting the isolation level to `SERIALIZABLE`.
/// This wrapper ensures that any connections returned via the `get_connection` method
/// have that setting applied.
#[derive(Debug, Clone)]
pub struct PoolWrapper(sqlx::MySqlPool);

impl PoolWrapper {
    pub async fn get_connection(&self) -> anyhow::Result<MySqlPC> {
        let mut conn = self.0.acquire().await?;
        // This, theoretically, should provide the maximum protection against
        // transactions interfering with each other, see
        // https://www.databasestar.com/sql-transactions/
        sqlx::query!("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *conn)
            .await?;
        Ok(conn)
    }
}

/// Returns access to a pool of database connections
/// 
/// All access to the database for the priors should use this function to ensure 
/// certain per-session settings are enabled.
pub async fn get_database_pool(url_in: Option<String>) -> anyhow::Result<PoolWrapper> {
    let url = get_database_url(url_in)
        .context("Error occurred getting database URL within get_database_pool")?;
    let pool = sqlx::MySqlPool::connect(&url).await
        .with_context(|| format!("Error occurred connecting to MySqlPool at url = {url}"))?;
    log::info!("Database pool established with URL = {url}");
    let wrapper = PoolWrapper(pool);
    return Ok(wrapper)
}
