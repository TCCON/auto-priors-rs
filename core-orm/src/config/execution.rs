use std::{collections::HashMap, fmt::Display, path::PathBuf, str::FromStr};

use serde::{Deserialize, Serialize};
use url::Url;

use super::processing_config::{self, ProcessingConfig};

/// Configuration section dealing with how jobs are run
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExecutionConfig {
    /// Maximum number of threads to let numpy use
    pub max_numpy_threads: u32,

    /// Full glob pattern (including directory) to use to find input files
    pub input_file_pattern: String,

    /// Full glob pattern (including directory) to use to find status request files
    pub status_request_file_pattern: String,

    /// Directory to which successfully parsed input files go
    pub success_input_file_dir: PathBuf,

    /// Directory to which input files that fail parsing go
    pub failure_input_file_dir: PathBuf,

    /// Hours to retain requested jobs before deleting
    pub hours_to_keep: u32,

    /// The base URL of the the FTP server from which users download jobs
    pub ftp_download_server: Url,

    /// The root path for the FTP server, i.e. where users start when they log in.
    /// Used to map file paths to FTP URLs.
    pub ftp_download_root: PathBuf,

    /// The path to write requested job output files to, must be somewhere that users
    /// can download them from.
    pub output_path: PathBuf,

    /// Path where standard sites' output tarballs shall be stored. This will have subdirectories
    /// named by site ID
    pub std_sites_tar_output: PathBuf,

    /// Run directory for standard site jobs.
    pub std_sites_output_base: PathBuf,

    /// Maximum number of days allowed in a single job request. If omitted, no limit is imposed.
    #[serde(default)]
    pub job_max_days: Option<u32>,

    /// If included, requests submitted by users will be split into jobs of this many days.
    #[serde(default)]
    pub job_split_into_days: Option<u32>,

    /// Run a simulation, do not execute ginput, but generate mock output files for testing
    #[serde(default)]
    pub simulate: bool,

    #[serde(default = "default_sim_delay")]
    pub simulation_delay: u32,

    /// The queue that submitted jobs go into
    pub submitted_job_queue: String,

    /// The queue that standard site jobs go into
    pub std_site_job_queue: String,

    /// When adding jobs for standard sites, jobs covering dates from today - N days on
    /// will have extra priority. If this is not specified, then these "current" jobs
    /// get no extra priority.
    pub std_site_priority_days: Option<i64>,

    /// Which error handler to use. Note that this cannot be changed by a config reload.
    pub error_handler: ErrorHandlerChoice,

    /// If given, where to write the flat version of the standard site JSON file for users to download.
    /// If omitted, the service will not write the flat JSON file.
    #[serde(default)]
    pub flat_stdsite_json_file: Option<PathBuf>,

    /// If given, where to write the grouped version of the standard site JSON file for users to download.
    /// If omitted, the service will not write the grouped JSON file.
    #[serde(default)]
    pub grouped_stdsite_json_file: Option<PathBuf>,

    /// Determines maximum number of jobs allowed to run simultaneously for different work sets
    #[serde(default)]
    pub queues: HashMap<String, JobQueueOptions>,

    /// Map of available ginput versions to use.
    pub ginput: HashMap<String, GinputConfig>,

    #[serde(with = "processing_config")]
    pub processing_configurations: HashMap<String, ProcessingConfig>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        let host = hostname::get()
            .unwrap_or("localhost".into());
        let host = host.to_string_lossy();

        Self { 
            error_handler: Default::default(),
            queues: Default::default(), 
            max_numpy_threads: 2, 
            hours_to_keep: 168,
            input_file_pattern: "input_file_2020*.txt".to_owned(),
            status_request_file_pattern: "ginput_status*.txt".to_owned(),
            success_input_file_dir: PathBuf::from("."),
            failure_input_file_dir: PathBuf::from("."),
            ftp_download_server: Url::parse(&format!("ftp://{host}/")).unwrap_or_else(|_| Url::parse("ftp://localhost/").unwrap()), 
            ftp_download_root: Default::default(), 
            output_path: Default::default(), 
            std_sites_tar_output: Default::default(), 
            std_sites_output_base: Default::default(),
            flat_stdsite_json_file: Default::default(),
            grouped_stdsite_json_file: Default::default(),
            ginput: Default::default(),
            job_max_days: None,
            job_split_into_days: None,
            simulate: false,
            simulation_delay: default_sim_delay(),
            submitted_job_queue: "submitted".to_string(),
            std_site_job_queue: "std-sites".to_string(),
            std_site_priority_days: Default::default(),
            processing_configurations: Default::default(),
        }
    }
}

fn default_sim_delay() -> u32 {
    60
}

/// Configuration describing an available version of ginput that the automation can call.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum GinputConfig {
    /// A ginput installation to be called via its `run_ginput.py` entry point. Requires
    /// one option, `entry_point_path`, which is the path to the `run_ginput.py` file.
    Script{entry_point_path: PathBuf}
}


#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ErrorHandlerChoice {
    Logging,
    EmailAdmins
}

impl Default for ErrorHandlerChoice {
    fn default() -> Self {
        Self::EmailAdmins
    }
}

impl Display for ErrorHandlerChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorHandlerChoice::Logging => write!(f, "Logging"),
            ErrorHandlerChoice::EmailAdmins => write!(f, "EmailAdmins"),
        }
    }
}

impl FromStr for ErrorHandlerChoice {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "logging" => Ok(Self::Logging),
            "emailadmins" => Ok(Self::EmailAdmins),
            _ => anyhow::bail!("Unknown error handler choice: {s}")
        }
    }
}


/// A structure describing configuration of a job queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobQueueOptions {
    /// The maximum number of processors that this queue can use (default = 1)
    pub max_num_procs: usize,

    /// The fair share policy to use for this queue
    #[serde(default)]
    pub fair_share_policy: FairSharePolicy
}

impl Default for JobQueueOptions {
    fn default() -> Self {
        Self { 
            max_num_procs: 1,
            fair_share_policy: Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]

#[serde(tag = "type")]
pub enum FairSharePolicy {
    Simple(crate::jobs::PrioritySubmitFS)
}

impl Default for FairSharePolicy {
    fn default() -> Self {
        Self::Simple(crate::jobs::PrioritySubmitFS{})
    }
}

#[async_trait::async_trait]
impl crate::jobs::FairShare for FairSharePolicy {
    async fn next_job_in_queue(&self, conn: &mut crate::MySqlConn, queue: &str) -> crate::error::JobResult<Option<crate::jobs::Job>> {
        match self {
            Self::Simple(policy) => policy.next_job_in_queue(conn, queue).await
        }
    }
}