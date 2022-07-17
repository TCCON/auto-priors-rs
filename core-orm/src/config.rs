//! Configurable options for the core object relational model and priors jobs
//! 
//! The functions [`load_config_file`] and [`load_env_config_file`] provide a
//! simple mechanism to load the [`Config`] struct which holds the configuration
//! data for the priors code. [`load_config_file_or_default`] provides an infallible
//! option which returns a default configuration if no configuration file is available
//! or there is a problem reading the configuration.
//! 
//! A default (mostly blank) configuration file can be created by calling [`generate_config_file`].
//! 
use std::{path::{PathBuf, Path}, fs::File, io::{Write, Read}};
use anyhow::{self, Context};
use hostname;
use serde::{Serialize, Deserialize};
use toml;
use url::Url;

/// Name of the environmental variable to look at for the path to the configuration file
pub static CFG_FILE_ENV_VAR: &str = "PRIOR_CONFIG_FILE";


/// Top level configuration structure.
/// 
/// [`ExecutionConfig`], [`DataConfig`], and [`AdminConfig`] objects comprise
/// subsections.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub execution: ExecutionConfig,
    pub data: DataConfig,
    pub admin: AdminConfig
}

/// Configuration section dealing with how jobs are run
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Maximum number of jobs to run simultaneously
    pub max_ntasks: u32, 

    /// Maximum number of threads to let numpy use
    pub max_numpy_threads: u32,

    /// Hours to retain requested jobs before deleting
    pub hours_to_keep: u32,

    /// The base URL of the the FTP server from which users download jobs
    pub download_server: Url,

    /// The root path for the FTP server, i.e. where users start when they log in.
    /// Used to map file paths to FTP URLs.
    pub download_root: PathBuf,

    /// The path to write requested job output files to, must be somewhere that users
    /// can download them from.
    pub output_path: PathBuf,

    /// Path where standard sites' output tarballs shall be stored. This will have subdirectories
    /// named by site ID
    pub std_sites_tar_output: PathBuf,

    /// Run directory for standard site jobs.
    pub std_sites_output_base: PathBuf,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        let host = hostname::get()
            .unwrap_or("localhost".into());
        let host = host.to_string_lossy();

        Self { 
            max_ntasks: 4, 
            max_numpy_threads: 2, 
            hours_to_keep: 168,
            download_server: Url::parse(&format!("ftp://{host}/")).unwrap_or_else(|_| Url::parse("ftp://localhost/").unwrap()), 
            download_root: Default::default(), 
            output_path: Default::default(), 
            std_sites_tar_output: Default::default(), 
            std_sites_output_base: Default::default() 
        }
    }
}

/// Configuration section dealing with input data for jobs
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DataConfig {
    /// The path to the GEOS FPIT data. This directory must contain the Nx and 
    /// Nv subdirectories (Np instead of Nv if using fixed-pressure level files)
    pub geos_path: PathBuf,

    /// The path to the GEOS FPIT chemistry files. Must contain an Nv subdirectory. 
    pub chem_path: PathBuf,

    /// The path to an integral.gnd file that specifies an altitude grid. If omitted, 
    /// or an empty string, then the priors are produced on the native GEOS grid.
    pub zgrid_file: Option<PathBuf>,

    /// The path to a summer, 35N .vmr file that will be used for the secondary 
    /// gases. If omitted or an empty string, the secondary gases are not included.
    pub base_vmr_file: Option<PathBuf>
}

/// Configuration section dealing with error reporting and job limits
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminConfig {
    /// A list of email addresses to contact if an unexpected error occurs.
    pub admin_emails: Vec<String>,

    /// Number of jobs any one user may have pending at a given moment before 
    /// a note is sent to the admins informing them of excessive usage.
    pub soft_job_limit: u32,

    /// Maximum number of locations in a single job request
    pub hard_job_limit: u32,

    /// A message to send to users when their input file is successfully parsed.
    /// If this option is absent or is an empty string, no message will be sent.
    pub acknowledgement_message: Option<String>
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self { 
            admin_emails: Default::default(), 
            soft_job_limit: 100,
            hard_job_limit: 100,
            acknowledgement_message: Default::default() 
        }
    }
}


/// Generate a default configuration .toml file at `path`
/// 
/// # Errors
/// Returns an `Err` if:
/// 
/// * the default configuration could not be serialized
/// * the file could not be created or written to at `path`
pub fn generate_config_file<T>(path: T) -> anyhow::Result<()> 
where T: AsRef<Path>
{
    // TODO: make a macro that will copy docstring comments from the structs to the file
    let mut default_cfg = Config::default();
    default_cfg.data.base_vmr_file = Some(PathBuf::new());
    default_cfg.data.zgrid_file = Some(PathBuf::new());
    default_cfg.admin.acknowledgement_message = Some(String::new());

    let toml_str = toml::to_string_pretty(&default_cfg)?;
    let mut f = File::create(path).context("Could not create the configuration file.")?;
    f.write_all(toml_str.as_bytes()).context("Could not write the configuration file.")?;

    Ok(())
}


/// Load an existing configuration .toml file from `path`
/// 
/// # Errors
/// An `Err` is returned if:
/// 
/// * it could not open the file at `path`
/// * it could not read the contents of `path`
/// * the .toml file could not be decoded
/// 
/// # See also
/// * [`load_env_config_file`]
/// * [`load_config_file_or_default`]
pub fn load_config_file<T>(path: T) -> anyhow::Result<Config> 
where T: AsRef<Path>
{
    let mut f = File::open(path).context("Failed to open configuration file.")?;
    let mut toml_str = vec![];
    f.read_to_end(&mut toml_str)?;
    Ok(toml::from_slice(&toml_str)?)
}


/// Load an existing configuration at the path pointed to by [`CFG_FILE_ENV_VAR`]
/// 
/// This is a convenience function that uses [`dotenv`] to augment existing environmental
/// variables with any in a `.env` file, then gets the path to the configuration file
/// from the environmental variable `$PRIOR_CONFIG_FILE`.
/// 
/// # Errors
/// Returns an `Err` if:
/// 
/// * there was a problem getting the `$PRIOR_CONFIG_FILE` value
/// * there was a problem reading the file (e.g. didn't exist or lacked read permissions)
/// 
/// # See also
/// * [`load_config_file`]
/// * [`load_config_file_or_default`]
pub fn load_env_config_file() -> anyhow::Result<Config> {
    dotenv::dotenv().ok();
    let path = PathBuf::from(std::env::var(CFG_FILE_ENV_VAR)?);
    return load_config_file(path);
}

/// Load an existing configuration file *or* provide defaults.
/// 
/// This is intended as a convenience function for either (a) testing or
/// (b) cases where a configuration object is expected, but the precise 
/// configuration is not essential. Since it silently falls back on the default
/// if there is an error reading the configuration file, this is not the
/// best function to use when it would be helpful to know if a default was
/// used. In such a case, using [`load_config_file`] with `unwrap_or_else`
/// might be better:
/// 
/// ```
/// use tccon_priors_orm::config::{Config, load_config_file};
/// use log::warn;
/// let path = "does_not_exist.toml";
/// let config = load_config_file(path).unwrap_or_else(|_| {
///     warn!("Using default configuration due to error reading {path}");
///     Config::default()
/// });
/// ```
/// 
/// # Parameters
/// 
/// * `path` - the path to the configuration file to read. If this is `None`,
///   then a default configuration will be returned.
/// 
/// # Returns
/// 
/// * [`Config`] - a configuration object, either read from the .toml file or
///   a default if the file does not exist or there is an error reading it.
/// 
/// # See also
/// 
/// * [`load_config_file`]
/// * [`load_env_config_file`]
pub fn load_config_file_or_default<T>(path: Option<T>) -> Config
where T: AsRef<Path>
{
    if let Some(p) = path {
        if p.as_ref().exists() {
            return load_config_file(p).unwrap_or_default()
        }else{
            return Config::default()
        }
    }else{
        return Config::default()
    }
}