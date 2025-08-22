use std::env;

use anyhow::Context;
#[cfg(feature = "container-tests")]
use testcontainers_modules::{
    mariadb, testcontainers::core::ContainerAsync, testcontainers::runners::AsyncRunner,
};

use crate::PoolWrapper;

static TEST_DB_ENV_VARS: [&'static str; 2] = ["PRIORS_TEST_DATABASE_URL", "TEST_DATABASE_URL"];

pub enum TestDb {
    Persistent(String),
    #[cfg(feature = "container-tests")]
    Container(ContainerAsync<mariadb::Mariadb>, String),
}

impl TestDb {
    fn new_persistent(url: String) -> Self {
        Self::Persistent(url)
    }

    #[cfg(feature = "container-tests")]
    async fn new_container() -> anyhow::Result<Self> {
        let instance = mariadb::Mariadb::default().start().await?;
        let url = format!(
            "mariadb://{}:{}/test",
            instance.get_host().await?,
            instance.get_host_port_ipv4(3306).await?
        );
        Ok(Self::Container(instance, url))
    }

    #[cfg(not(feature = "container-tests"))]
    async fn new_container() -> anyhow::Result<Self> {
        panic!("Must compile tests with --features=container-tests to use testcontainers");
    }

    fn db_url(&self) -> &str {
        match self {
            Self::Persistent(url) => url,
            #[cfg(feature = "container-tests")]
            Self::Container(_, url) => url,
        }
    }
}

/// Get the test database
///
/// The default behavior is to use [`testcontainers`] to create a
/// ephemeral database to test in. If the environmental variable
/// "PRIORS_TEST_PERSISTENT_DB" is set to 1 however, this will revert
/// to using the URL specified by one of the [`TEST_DB_ENV_VARS`].
/// It will look first in the existing environmental variables, then
/// in those specified in a .env file.
///
/// **Note:** it is intended that the returned [`TestDb`] value be kept
/// alive through the duration of the test. Because this holds the test
/// container instance, when it is dropped, the container should be cleaned
/// up.
async fn get_test_db() -> anyhow::Result<TestDb> {
    let use_persistent_db = env::var("PRIORS_TEST_PERSISTENT_DB")
        .map(|s| s == "1")
        .unwrap_or(false);

    if !use_persistent_db {
        // Prefer to use test containers to avoid messing up persistent databases
        // and to let tests run on GitHub
        let container = TestDb::new_container().await?;
        log::debug!(
            "Using database URL {} provided by test container",
            container.db_url()
        );
        Ok(container)
    } else {
        // First, try the regular environmental variables
        for key in TEST_DB_ENV_VARS {
            if let Ok(val) = env::var(key) {
                log::debug!("Using database URL {val} from the environmental variable {key}");
                return Ok(TestDb::new_persistent(val));
            }
        }

        // If we can't find the URL in existing environmental variables, try using dotenv.
        let env_path = dotenv::dotenv().context(
            "No database URL defined in existing environmental variables, and no .env file found.",
        )?;
        for key in TEST_DB_ENV_VARS {
            if let Ok(val) = dotenv::var(key) {
                let epd = env_path.display();
                log::debug!("Using database URL {val} from the variable {key} in {epd}");
                return Ok(TestDb::new_persistent(val));
            }
        }

        return Err(anyhow::anyhow!("Unable to find database URL."));
    }
}

/// Open a pool of connections to the test database.
///
/// The default behavior is to use [`testcontainers`] to create a
/// ephemeral database to test in. If the environmental variable
/// "PRIORS_TEST_PERSISTENT_DB" is set to 1 however, this will revert
/// to using the URL specified by one of the [`TEST_DB_ENV_VARS`].
/// It will look first in the existing environmental variables, then
/// in those specified in a .env file.
///
/// `reset_db` controls whether migrations are applied or not. If `false`,
/// then the database is left in whatever state it was when the connection
/// was established. When using test containers, this will be an empty
/// database, whereas when using a non-container database, it may hold existing
/// data. Setting this to `true` will result in a database with all the tables
/// created but empty. In most cases, this argument should be `true`.
///
/// Since we can use test containers, and the container is cleaned up when the
/// variable holding it goes out of scope, this returns a `TestDb` instance as
/// well. That variable should be held until the pool is no longer needed.
///
/// # Returns
/// If successful, returns a [`PoolWrapper`] instance from which connections
/// can be obtained and a [`TestDb`] instance. **Note:** it is intended that this
/// [`TestDb`] value be kept alive through the duration of the test. Because it
/// holds the test container instance, when it is dropped, the container should be
/// cleaned up.
///
/// This will return an error if the connection to the database could not be established,
/// or if applying the migrations failed.
pub async fn open_test_database(reset_db: bool) -> anyhow::Result<(PoolWrapper, TestDb)> {
    let test_db = get_test_db().await?;
    let db_url = test_db.db_url();
    println!("db_url = {db_url}");
    let pool = crate::get_database_pool(Some(db_url.to_string())).await?;

    if reset_db {
        let mut conn = pool.get_connection().await?;
        crate::unapply_migrations(&mut conn, 0).await?;
        crate::apply_migrations(&mut conn).await?;
    }

    Ok((pool, test_db))
}

