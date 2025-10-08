use std::env;

use anyhow::{self, Context};
use dotenv;
use log;
use sqlx;
use sqlx::migrate::Migrator;

pub mod auth;
pub mod config;
pub mod email;
pub mod error;
pub mod export;
pub mod input_files;
pub mod jobs;
pub mod met;
pub mod siteinfo;
pub mod stdsitejobs;
pub mod utils;

pub mod test_utils;

pub type MySqlPool = sqlx::pool::Pool<sqlx::MySql>;
pub type MySqlPC = sqlx::pool::PoolConnection<sqlx::MySql>;
pub type MySqlConn = sqlx::MySqlConnection;
static DB_ENV_VARS: [&'static str; 2] = ["PRIORS_DATABASE_URL", "DATABASE_URL"];

static MIGRATOR: Migrator = sqlx::migrate!();
static CI_MIGRATOR: Migrator = sqlx::migrate!("./migrations-ci");

/// Apply the database migrations to the database connected to with `conn`
///
/// Setting `use_ci` will use an alternate set of migrations designed for automated testing
/// (in continuous integration).
pub async fn apply_migrations(conn: &mut MySqlConn, use_ci: bool) -> anyhow::Result<()> {
    if use_ci {
        log::info!("Using CI migrations");
        Ok(CI_MIGRATOR.run(conn).await?)
    } else {
        log::info!("Using production migrations");
        Ok(MIGRATOR.run(conn).await?)
    }
}

/// Unapply the database migrations to the database connected to with `conn` back to the
/// `target` migration.
///
/// Setting `use_ci` will use an alternate set of migrations designed for automated testing
/// (in continuous integration).
pub async fn unapply_migrations(
    conn: &mut MySqlConn,
    target: i64,
    use_ci: bool,
) -> anyhow::Result<()> {
    if use_ci {
        log::info!("Undoing CI migrations");
        Ok(CI_MIGRATOR.undo(conn, target).await?)
    } else {
        log::info!("Undoing production migrations");
        Ok(MIGRATOR.undo(conn, target).await?)
    }
}

pub fn get_database_url(url_in: Option<String>) -> anyhow::Result<String> {
    if let Some(url) = url_in {
        return Ok(url);
    }

    // First, try the regular environmental variables
    for key in DB_ENV_VARS {
        if let Ok(val) = env::var(key) {
            log::debug!(
                "Using database URL {} from the environmental variable {key}",
                sanitize_db_url(&val)
            );
            return Ok(val);
        }
    }

    // If we can't find the URL in existing environmental variables, try using dotenv.
    let env_path = dotenv::dotenv().context(
        "No database URL defined in existing environmental variables, and no .env file found.",
    )?;
    for key in DB_ENV_VARS {
        if let Ok(val) = dotenv::var(key) {
            let epd = env_path.display();
            log::debug!(
                "Using database URL {} from the variable {key} in {epd}",
                sanitize_db_url(&val)
            );
            return Ok(val);
        }
    }

    return Err(anyhow::anyhow!("Unable to find database URL."));
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
    pub async fn get_connection(&self) -> Result<MySqlPC, sqlx::Error> {
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
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .with_context(|| format!("Error occurred connecting to MySqlPool at url = {url}"))?;
    log::debug!(
        "Database pool established with URL = {}",
        sanitize_db_url(&url)
    );
    let wrapper = PoolWrapper(pool);
    return Ok(wrapper);
}

pub fn sanitize_db_url(url: &str) -> String {
    use url::Url;
    let mut url = if let Ok(s) = Url::parse(url) {
        s
    } else {
        return "****".to_string();
    };

    // Ignore errors, if we get an error, it just means this URL couldn't have a username or password
    let _ = url.set_username("****");
    let _ = url.set_password(Some("****"));

    url.to_string()
}
