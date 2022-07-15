use std::{path::{PathBuf, Path}, fs::File, io::{Write, Read}};
use anyhow::{self, Context};
use hostname;
use serde::{Serialize, Deserialize};
use toml;
use url::Url;


#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub execution: ExecutionConfig,
    pub data: DataConfig,
    pub admin: AdminConfig
}

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


pub fn generate_config_file(path: &Path) -> anyhow::Result<()> {
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


pub fn load_config_file(path: &Path) -> anyhow::Result<Config> {
    let mut f = File::open(path).context("Failed to open configuration file.")?;
    let mut toml_str = vec![];
    f.read_to_end(&mut toml_str)?;
    Ok(toml::from_slice(&toml_str)?)
}