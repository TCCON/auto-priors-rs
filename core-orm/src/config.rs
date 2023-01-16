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
use std::{path::{PathBuf, Path}, fs::File, io::{Write, Read}, collections::HashMap, fmt::Display};
use anyhow::{self, Context};
use chrono::{NaiveDate, NaiveDateTime, Duration};
use hostname;
use log::debug;
use serde::{Serialize, Deserialize};
use toml;
use url::Url;

use crate::geos;

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

impl Config {
    /// Get the slice of download configuration needed for a given met type
    /// 
    /// Returns an `Err` if the given met_key is not present in the config.
    pub fn get_met_configs(&self, met_key: &str) -> anyhow::Result<&[DownloadConfig]> {
        self.data.download.get(met_key)
        .and_then(|cfgs| Some(cfgs.as_slice()))
        .ok_or_else(|| anyhow::Error::msg(format!("No meteorology with with '{met_key}' found.")))
    }

    /// Get the earliest start date for all the file types of a given met configuration
    /// 
    /// When a met type has multiple files (e.g. 3D assimilation, 2D assimilation, and 3D chemistry for GEOS),
    /// it is possible that different file types start at different times, so the each file has an earliest
    /// date it is available for in the config. This returns the minimum among all of those for the given
    /// `met_key`, or `None` if `met_key` exists but has no files defined.
    /// 
    /// Will also return an `Err` is `met_key` is not in the config.
    pub fn get_met_start_date(&self, met_key: &str) -> anyhow::Result<Option<NaiveDate>> {
        let met_cfgs = self.get_met_configs(met_key)?;

        if met_cfgs.len() == 0 {
            // This is redundant (Iterator::min() returns None if the iterator is empty) but I find this clearer
            Ok(None)
        }else {
            let x = met_cfgs.iter().map(|c| c.earliest_date);
            Ok(x.min())
        }
    }
    
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

    /// Frequency in seconds for the job service to check for pending jobs
    pub start_jobs_freq: f32,

    /// Run a simulation, do not execute ginput, but generate mock output files for testing
    #[serde(default)]
    pub simulate: bool,
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
            std_sites_output_base: Default::default(),
            start_jobs_freq: 60.0,
            simulate: false
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

    /// A map of arrays of configurations that specify how to download reanalysis files.
    /// Each config in the array represents a file that needs to be downloaded for ginput
    /// to run. The keys to the map must be strings that will be passed as arguments to
    /// the downloader to specify which file type to download. See [`DownloadConfig`] for 
    /// details on the array elements.
    pub download: HashMap<String, Vec<DownloadConfig>>,

    /// The path to an integral.gnd file that specifies an altitude grid. If omitted, 
    /// or an empty string, then the priors are produced on the native GEOS grid.
    pub zgrid_file: Option<PathBuf>,

    /// The path to a summer, 35N .vmr file that will be used for the secondary 
    /// gases. If omitted or an empty string, the secondary gases are not included.
    pub base_vmr_file: Option<PathBuf>
}

/// Configuration for how to download input reanalysis files for ginput
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DownloadConfig {
    /// What stream this is from (FP or FP-IT, currently)
    pub product: geos::GeosProduct,

    /// Whether this set of files is meteorology or chemistry
    pub data_type: geos::GeosDataType,

    /// What set of vertical levels these files represent.
    pub levels: geos::GeosLevels,

    /// A URL pattern that can be passed to wget to download the desired file.
    /// Use [Chrono format strings](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    /// (e.g. "%Y", "%d") to insert date/time elements into the URL.
    pub url_pattern: String,

    /// The expected pattern for the downloaded file names.
    /// Use [Chrono format strings](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    /// (e.g. "%Y", "%d") to insert date/time elements into the name. The default is to assume that 
    /// the last part of the URL in `url_pattern` will become the basename, use this to override that
    /// if that assumption is not true.
    /// 
    /// NOTE: in order for this program to properly parse the date & time from the filename for the database,
    /// the pattern MUST include year, month, day, hour, and minute.
    pub basename_pattern: Option<String>,

    /// The number of minutes between subsequent reanalysis files, e.g. for files every three hours,
    /// set this to 180. Note that the current implementation assumes that there will always be a
    /// file for midnight, so values greater than 24 * 60 = 1440 are not supported.
    pub file_freq_min: i64,

    /// The earliest date for which this met data is available for download
    pub earliest_date: NaiveDate,

    /// The subdirectory in the met or chemistry data directory to save the files to. If not given,
    /// the correct subdirectory is chosen based on the levels value.
    pub subdir: Option<PathBuf>,
}

impl Display for DownloadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "product = {}, type = {}, levels = {}", self.product, self.data_type, self.levels)
    }
}

impl DownloadConfig {
    /// Get the pattern for file names of this type of file, with no leading path.
    /// 
    /// If the configuration has a value for the `basename_pattern` specified, that is
    /// returned. Otherwise, the URL pattern is split on the last "/" and everything after
    /// that slash is used as the pattern.
    /// 
    /// Can return an `Err` if it could not identify a part after the final slash in the URL.
    pub fn get_basename_pattern(&self) -> anyhow::Result<&str> {
        if let Some(pat) = &self.basename_pattern {
            return Ok(&pat)
        }else{
            // let full_url = url::Url::parse(&self.url_pattern)?
            //     .path_segments()
            //     .ok_or_else(|| anyhow::Error::msg(format!("Could not find the file base name from URL pattern {}", self.url_pattern)))?
            //     .last()
            //     .ok_or_else(|| anyhow::Error::msg(format!("Could not find the file base name from URL pattern {}", self.url_pattern)))?;
            // Ok(full_url)

            // Preferring this over the URL library because the latter makes it difficult to
            // return a &str.
            self.url_pattern
                .split('/')
                .last()
                .ok_or_else(|| anyhow::Error::msg(format!("Could not find the file base name from URL pattern {}", self.url_pattern)))
        }
    }

    /// Get the directory in which files of this type should be saved.
    /// 
    /// This uses the root directory for the appropriate data type (e.g. met or chem)
    /// then adds a subdirectory for the levels type. 
    /// 
    /// # Returns
    /// The canonical path to the save directory. Returns an `Err` if the path could not
    /// be canonicalized (e.g. the path does not exist or an intermediate component is
    /// not a directory).
    pub fn get_save_dir(&self, data_cfg: &DataConfig) -> anyhow::Result<PathBuf> {
        let root_save_dir = match self.data_type {
            geos::GeosDataType::Met => data_cfg.geos_path.as_path(),
            geos::GeosDataType::Chm => data_cfg.chem_path.as_path(),
        };
        
        let subdir = if let Some(sd) = &self.subdir {
            sd.clone()
        }else{
            self.levels.standard_subdir()
        };
        
        root_save_dir.join(subdir)
            .canonicalize()
            .with_context(|| format!("Failed to canonicalized the reanalysis save directory path '{}'", root_save_dir.display()))
    }

    /// Provide a vector of datetimes when this file type is expected to exist on a given date
    pub fn times_on_day(&self, date: NaiveDate) -> Vec<NaiveDateTime> {
        let end = date.and_hms(0, 0, 0) + Duration::days(1);
        let mut file_time = date.and_hms(0, 0, 0);
        let file_time_del = Duration::minutes(self.file_freq_min);
        
        let mut times = vec![];
        while file_time < end {
            times.push(file_time);
            file_time += file_time_del;
        }
        
        times
    }

    /// Provide a vector of the files of this type expected to exist on a given date.
    /// 
    /// The returned paths point to files for the times given by [`DownloadConfig::times_on_day`]
    /// in the path gievn by [`DownloadConfig::get_save_dir`].
    /// 
    /// Returns an `Err` if it cannot get the save directory or the filename pattern.
    pub fn expected_files_on_day(&self, date: NaiveDate, data_cfg: &DataConfig) -> anyhow::Result<Vec<PathBuf>> {
        let save_dir = self.get_save_dir(data_cfg)?;
        let basename_pat = self.get_basename_pattern()?;
        let mut files = vec![];
        for time in self.times_on_day(date) {
            let file_name = time.format(basename_pat).to_string();
            files.push(save_dir.join(file_name));
        }
        Ok(files)
    }
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


pub fn get_env_config_path() -> anyhow::Result<PathBuf> {
    dotenv::dotenv().ok();
    let key = std::env::var(CFG_FILE_ENV_VAR)?;
    return Ok(PathBuf::from(key))
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
    let path = get_env_config_path()?;
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
pub fn load_config_file_or_default<T>(path: Option<T>) -> anyhow::Result<Config>
where T: AsRef<Path>
{
    if let Some(p) = path {
        if p.as_ref().exists() {
            debug!("Reading config file from {}", p.as_ref().display());
            return load_config_file(p.as_ref()).with_context(|| {
                format!("Error loading configuration file {}", p.as_ref().display())
            })
        }else{
            debug!("Given config file ({}) does not exist, using default", p.as_ref().display());
            return Ok(Config::default())
        }
    }else{
        debug!("No config file path given, using default");
        return Ok(Config::default())
    }
}