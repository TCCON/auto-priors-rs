#![allow(dead_code)]
use std::env;
use std::fs::File;
use std::io::{Write, Read};
use std::path::{PathBuf, Path};
use anyhow::Context;
use orm::{self, MySqlConn, PoolWrapper};
use orm::met::MetFile;
use tccon_priors_cli::utils::Downloader;

static TEST_DB_ENV_VARS: [&'static str; 2] = ["PRIORS_TEST_DATABASE_URL", "TEST_DATABASE_URL"];
static TEST_FILE_DIR_VAR: &'static str = "PRIORS_TEST_FILE_ROOT";
pub const TEST_MET_KEY: &'static str = "geosfpit";

pub(crate) fn make_dummy_config(download_root: PathBuf) -> anyhow::Result<orm::config::Config> {
    let s = include_str!("test_config.toml");
    let mut cfg: orm::config::Config = toml::from_slice(s.as_bytes())?;

    cfg.execution.ftp_download_root = download_root.clone();
    for (_, dl_cfgs) in cfg.data.download.iter_mut() {
        for dl_cfg in dl_cfgs.iter_mut() {
            dl_cfg.download_dir = download_root.join(dl_cfg.levels.standard_subdir());
        }
    }

    Ok(cfg)
}

pub(crate) fn make_dummy_config_with_temp_dirs(prefix: &str) -> anyhow::Result<(orm::config::Config, TestRootDir)> {
    let test_dir = TestRootDir::new(prefix)
        .with_context(|| "Failed to make parent temporary directory")?;
    dbg!(&test_dir);
    let cfg = make_dummy_config(test_dir.path().to_owned())?;
    for (_, dl_cfgs) in cfg.data.download.iter() {
        for dl_cfg in dl_cfgs.iter() {
            std::fs::create_dir_all(dl_cfg.download_dir.clone())
                .with_context(|| "Failed to create a subdirectory for one of the file sets to download")?;
        }
    }
    
    Ok((cfg, test_dir))
}

pub(crate) fn get_test_db_url() -> anyhow::Result<String> {
    // First, try the regular environmental variables
    for key in TEST_DB_ENV_VARS {
        if let Ok(val) = env::var(key) {
            log::info!("Using database URL {val} from the environmental variable {key}");
            return Ok(val)
        }
    }

    // If we can't find the URL in existing environmental variables, try using dotenv.
    let env_path = dotenv::dotenv().context("No database URL defined in existing environmental variables, and no .env file found.")?;
    for key in TEST_DB_ENV_VARS {
        if let Ok(val) = dotenv::var(key) {
            let epd = env_path.display();
            log::info!("Using database URL {val} from the variable {key} in {epd}");
            return Ok(val)
        }
    }

    return Err(anyhow::anyhow!("Unable to find database URL."))
}

pub(crate) async fn open_test_database(reset_db: bool) -> anyhow::Result<PoolWrapper> {
    
    let db_url = get_test_db_url()?;
    println!("db_url = {db_url}");
    let pool = orm::get_database_pool(Some(db_url)).await?;

    if reset_db {
        let mut conn = pool.get_connection().await?;
        orm::unapply_migrations(&mut conn, 0).await?;
        orm::apply_migrations(&mut conn).await?;
    }

    Ok(pool)
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
        use anyhow::Context;
        let read_sql = include_str!($path);
        for (i, statement) in read_sql.split(';').enumerate() {
            if !statement.trim().is_empty() {
                sqlx::query(statement.trim()).execute(&mut *$conn).await.with_context(|| format!("Error in or around statement {}", i+1)).unwrap();
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
/// In addition to the 
#[macro_export]
macro_rules! multiline_sql_init {
    ($path:literal) => {
        {
            let pool = common::open_test_database(true).await.expect("Failed to open test database");
            let mut conn = pool.get_connection().await.expect("Failed to acquire connection to database");
            multiline_sql!($path, conn);
            conn
        }
    };
}

// Per https://stackoverflow.com/a/31749071 this is necessary to
// use macros across modules
#[allow(unused_imports)]
pub(crate) use multiline_sql;
#[allow(unused_imports)]
pub(crate) use multiline_sql_init;

pub(crate) fn md5sum(p: &Path) -> anyhow::Result<Vec<u8>> {
    use md5::Digest;
    let mut hasher = md5::Md5::new();
    let mut buf: Vec<u8> = vec![0; 1_000_000];
    let mut f = File::open(p)?;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        } else {
            hasher.update(&buf[..n]);
        }
    }

    Ok(hasher.finalize().to_vec())
}

#[derive(Debug, Clone)]
pub(crate) struct TestDownloader {
    files: Vec<String>
}

impl TestDownloader {
    pub(crate) fn new() -> Self {
        Self { files: vec![] }
    }
}

impl Downloader for TestDownloader {
    fn add_file_to_download(&mut self, url: String) -> anyhow::Result<()> {
        self.files.push(url);
        Ok(())
    }

    fn download_files(&mut self, save_dir: &std::path::Path) -> anyhow::Result<()> {
        for url in self.files.iter() {
            let basename = url.split('/').last()
                .ok_or_else(|| anyhow::Error::msg(format!("Could not determine basename of URL {url}")))?;
            let new_file = save_dir.join(basename);
            let mut h = std::fs::File::create(&new_file)
                .with_context(|| format!("Error occurred while trying to create dummy file {}", new_file.display()))?;
            write!(h, "Dummy file created for tccon-priors-cli testing")
            .with_context(|| format!("Error occurred while trying to write to dummy file {}", new_file.display()))?;
        }

        Ok(())
    }

    fn iter_files(&self) -> std::slice::Iter<'_, String> {
        self.files.iter()
    }
}

#[derive(Debug)]
pub(crate) enum TestRootDir {
    Temporary(tempdir::TempDir),
    Persistent(PathBuf)
}

impl TestRootDir {
    pub(crate) fn path(&self) -> &Path {
        match self {
            TestRootDir::Temporary(temp) => temp.path(),
            TestRootDir::Persistent(p) => p.as_ref(),
        }
    }

    pub(crate) fn new(prefix: &str) -> anyhow::Result<Self> {
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

pub(crate) fn are_met_files_present_on_disk(root: &Path, files: &[&str]) -> anyhow::Result<()> {
    let missing: Vec<_> = files.iter()
        .filter_map(|f| {
            let p = root.join(f);
            if !p.exists() {
                Some(*f)
            } else {
                None
            }
        }).collect();

    if missing.is_empty() {
        Ok(())
    } else {
        let nmiss = missing.len();
        let nexpect = files.len();
        let mstr = missing.join(", ");
        anyhow::bail!("Out of {nexpect} expected files, {nmiss} were missing from {}: {mstr}", root.display())
    }
}

pub(crate) async fn are_met_file_present_in_database(conn: &mut MySqlConn, files: &[&str]) -> anyhow::Result<()> {
    // First strip off any leading directories - that way this works more easily with 
    // `are_met_files_present_on_disk`, where we might need to specify subdirectories
    let nexpected = files.len();
    let files: Vec<String> = files.iter()
        .map(|&f| {
            PathBuf::from(f).file_name().expect("Test file name must not terminate in `..`").to_string_lossy().to_string()
        }).collect();

    
    let mut missing = vec![];

    for file in files {
        let check = MetFile::get_file_by_name(conn, &file)
            .await
            .with_context(|| format!("Query for {file} failed"))?;
        if check.is_none() {
            missing.push(file);
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        let nmiss = missing.len();
        let mstr = missing.join(", ");
        anyhow::bail!("Out of {nexpected} expected files, {nmiss} were missing from the database: {mstr}")
    }

}