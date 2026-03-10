use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::Context;
#[cfg(feature = "container-tests")]
use testcontainers_modules::{
    mariadb, testcontainers::core::ContainerAsync, testcontainers::runners::AsyncRunner,
};

use crate::PoolWrapper;

static TEST_DB_ENV_VARS: [&'static str; 2] = ["PRIORS_TEST_DATABASE_URL", "TEST_DATABASE_URL"];
static TEST_FILE_DIR_VAR: &'static str = "PRIORS_TEST_FILE_ROOT";

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
        crate::unapply_migrations(&mut conn, 0, true).await?;
        crate::apply_migrations(&mut conn, true).await?;
    }

    Ok((pool, test_db))
}

pub fn make_dummy_config(scratch_root: PathBuf) -> anyhow::Result<crate::config::Config> {
    let s = include_str!("test_config.toml");
    let mut cfg: crate::config::Config = toml::from_str(s)?;

    cfg.execution.ftp_download_root = scratch_root.clone();
    cfg.execution.o2_file_path = scratch_root.join("o2_mean_dmf.dat");
    for (_, dl_cfg) in cfg.data.met_download.iter_mut() {
        dl_cfg.download_dir = scratch_root.join(dl_cfg.ginput_met_type.standard_subdir());
    }

    cfg.execution.success_input_file_dir = scratch_root.join("input_success");
    cfg.execution.failure_input_file_dir = scratch_root.join("input_failure");

    Ok(cfg)
}

pub fn make_dummy_config_with_temp_dirs(
    prefix: &str,
) -> anyhow::Result<(crate::config::Config, TestRootDir)> {
    let test_dir =
        TestRootDir::new(prefix).with_context(|| "Failed to make parent temporary directory")?;
    let cfg = make_dummy_config(test_dir.path().to_owned())?;
    for (_, dl_cfg) in cfg.data.met_download.iter() {
        std::fs::create_dir_all(dl_cfg.download_dir.clone()).with_context(|| {
            "Failed to create a subdirectory for one of the file sets to download"
        })?;
    }

    std::fs::create_dir_all(&cfg.execution.success_input_file_dir).with_context(|| {
        "Failed to create the subdirectory for successful input files to be moved to"
    })?;
    std::fs::create_dir_all(&cfg.execution.failure_input_file_dir).with_context(|| {
        "Failed to create the subdirectory for failed input files to be moved to"
    })?;

    Ok((cfg, test_dir))
}

/// Return the path the workspace root directory
pub fn get_workspace_root_dir() -> PathBuf {
    // Get the workspace root. The manifest dir points to the actual package where the
    // tests run, so we want the parent.
    let crate_root_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("expected CARGO_MANIFEST_DIR to have a parent")
        .to_path_buf();
    crate_root_dir
}

/// Return the path to the "testing" directory under the workspace root.
pub fn get_workspace_testing_dir() -> PathBuf {
    let root_dir = get_workspace_root_dir();
    root_dir.join("testing")
}

#[derive(Debug)]
pub enum TestRootDir {
    Temporary(tempdir::TempDir),
    Persistent(PathBuf),
}

impl TestRootDir {
    pub fn path(&self) -> &Path {
        match self {
            TestRootDir::Temporary(temp) => temp.path(),
            TestRootDir::Persistent(p) => p.as_ref(),
        }
    }

    /// Creates an output directory that can either be persistent or temporary.
    /// If the environmental variable defined by [`TEST_FILE_DIR_VAR`] is set,
    /// the output will be under that directory in a subdirectory `prefix`.
    /// Otherwise, it will be in a temporary directory using `prefix`.
    pub fn new(prefix: &str) -> anyhow::Result<Self> {
        if let Some(root) = get_env_var(TEST_FILE_DIR_VAR) {
            Self::new_persistent(&root, prefix)
        } else {
            Self::new_temporary(prefix)
        }
    }

    fn new_temporary(prefix: &str) -> anyhow::Result<Self> {
        let temp_dir = tempdir::TempDir::new(prefix)?;
        Ok(Self::Temporary(temp_dir))
    }

    fn new_persistent(root: &str, prefix: &str) -> anyhow::Result<Self> {
        let p = PathBuf::from(root).join(prefix);
        std::fs::create_dir_all(&p)?;
        Ok(Self::Persistent(p))
    }
}

fn get_env_var(varname: &str) -> Option<String> {
    if let Ok(val) = env::var(varname) {
        return Some(val);
    }

    if let Ok(val) = dotenv::var(varname) {
        return Some(val);
    }

    None
}

pub fn init_logging() {
    let _ = env_logger::builder()
        .filter_module("sqlx", log::LevelFilter::Warn)
        .format_source_path(true)
        .is_test(true)
        .try_init();
}

/// Execute a multi-statement SQL file on the database behind a connections.
///
/// This macro takes two inputs: a literal string path to the SQL file to read
/// from and a connection to the database to execute on. The connection must be
/// able to be passed to the `execute` method from [`sqlx`] as `&mut conn`.
///
/// The path given as the first argument should follow the [`include_str!`] rules
/// for relative paths, i.e. it will be interpreted relative to the file in which
/// it is written. The contents of the file are read and split on semicolons, and
/// each element of the split passed as a statement to [`sqlx::query`] so long as
/// the statement is not all whitespace. This prevents passing empty commands
/// to the database, which usually causes an error.
///
/// # Panics
/// Any errors in SQL will cause a panic. The panic message will include the original
/// SQL message plus which statement (not line number) in the file caused it. The
/// statement index will be 1-based.
#[macro_export]
macro_rules! multiline_sql {
    ($path:literal, $conn:ident) => {
        let read_sql = include_str!($path);
        for (i, statement) in read_sql.split(';').enumerate() {
            if !statement.trim().is_empty() {
                sqlx::query(statement.trim())
                    .execute(&mut *$conn)
                    .await
                    .map_err(|e| format!("Error in or around statement {}: {e}", i + 1))
                    .unwrap();
            }
        }
    };
}

/// Execute a file containing multiple SQL statements to initialize the test database.
///
/// This does exactly the same thing as [`multiline_sql!`], except that this opens a
/// connection to the test database itself (resetting it in the process, i.e. passes
/// `true` to [`open_test_database`]). The connection acquired will be returned from
/// this macro.
///
/// See [`multiline_sql!`] for information on how the SQL from the file is passed to
/// the database. If you need control over how the connection to the database is
///
/// # Panics
/// In addition to the panics that occur for [`multiline_sql!`], this will panic if
/// it could not open or connect to the test database.
///
/// # Returns
/// Returns a connection to the database which can be used to run further queries
/// and a [`TestDb`] instance. **Note:** it is intended that this [`TestDb`] value
/// be kept alive through the duration of the test. Because it holds the test container
/// instance, when it is dropped, the container should be cleaned up.
#[macro_export]
macro_rules! multiline_sql_init {
    ($path:literal) => {{
        let (pool, test_db) = orm::test_utils::open_test_database(true)
            .await
            .expect("Failed to open test database");
        let mut conn = pool
            .get_connection()
            .await
            .expect("Failed to acquire connection to database");
        multiline_sql!($path, conn);
        (conn, test_db)
    }};
}

/// Like [`multiline_sql_init`], except this returns the [`PoolWrapper`]
/// instead of an individual connection.
#[macro_export]
macro_rules! multiline_sql_init_pool {
    ($path:literal) => {{
        let (pool, test_db) = orm::test_utils::open_test_database(true)
            .await
            .expect("Failed to open test database");
        let mut conn = pool
            .get_connection()
            .await
            .expect("Failed to acquire connection to database");
        orm::multiline_sql!($path, conn);
        (pool, test_db)
    }};
}

// Per https://stackoverflow.com/a/31749071 this is necessary to
// use macros across modules
#[allow(unused_imports)]
pub use multiline_sql;
#[allow(unused_imports)]
pub use multiline_sql_init;
#[allow(unused_imports)]
pub use multiline_sql_init_pool;
