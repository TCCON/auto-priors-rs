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
use itertools::Itertools;
use lettre::message::{Mailbox, Mailboxes};
use log::{debug, warn, info};
use serde::{Serialize, Deserialize};
use toml;
use url::Url;

use crate::{met::{self, MetDataType}, error::{DefaultOptsQueryError, JobResult, EmailError}, MySqlConn, email::SendMail};

/// Name of the environmental variable to look at for the path to the configuration file
pub static CFG_FILE_ENV_VAR: &str = "PRIOR_CONFIG_FILE";


/// Top level configuration structure, comprised of subsections represented by other structures:
/// 
/// - `execution`: an [`ExecutionConfig`] containing options about how the automation runs
/// - `data`: a [`DataConfig`] containing options about where input data is located
/// - `default_options`: a `Vec` of [`DefaultOptions`] that specify which ginput and met version
///   to use by default for different time periods.
/// - `email`: an [`EmailConfig`] that determines how emails are sent (both for usual operation and
///   if something goes wrong)
/// - `admin`: an [`AdminConfig`] that contains settings about how use of this service is controlled
/// - `timing`: a [`ServiceTimingOptions`] that controls how often different parts of the service run.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    pub execution: ExecutionConfig,
    pub data: DataConfig,
    pub default_options: Vec<DefaultOptions>,
    #[serde(default)]
    pub email: EmailConfig,
    pub admin: AdminConfig,
    #[serde(default)]
    pub timing: ServiceTimingOptions
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

    /// Get the GEOS and chemistry directories required to pass to version 1 ginput instances
    /// 
    /// Version 1.x ginput instances expect GEOS data to be organized in a specific way: all files
    /// are to be directly stored in directories that match the levels their data is on: Nx for
    /// 2D files, Nv for eta-hybrid level files, and Np for fixed pressure level files. Further,
    /// all met files must be in subdirectories of the same parent. This function will return
    /// the paths to provide to ginput for a given met key.
    /// 
    /// # Returns
    /// If successful, a tuple containing the `geos_path` and `chem_path` in that order. If the
    /// met configuration requested does not define any chemistry files, a warning will be logged
    /// and the `chem_path` will be the same as the `geos_path`.
    /// 
    /// This returns an error in a number of conditions:
    /// 1. Any of the `download_dir` paths cannot be canonicalized
    /// 2. Any of the `download_dir` paths does not contain at least 1 component
    /// 3. The final component of any of the `download_dir` paths does not match the levels defined
    ///    for that file type.
    /// 4. Any of the `download_dir` paths does not have a parent directory
    /// 5. Inconsistent `geos_path` or `chem_path` values are defined.
    pub fn get_geos_and_chem_paths(&self, met_key: &str) -> anyhow::Result<(PathBuf, PathBuf)> {
        let dl_cfgs = self.get_met_configs(met_key)?;
        let mut geos_path = None;
        let mut chem_path = None;

        for (i, cfg) in dl_cfgs.iter().enumerate() {
            let i = i + 1;
            let download_dir = cfg.download_dir.canonicalize()
                .map_err(|e| anyhow::Error::msg(format!("In met type {met_key}, could not canonicalize download path for file type {i}: {e}")))?;
            let final_dir = download_dir.components()
                .last()
                .ok_or_else(|| anyhow::Error::msg(format!("In met type {met_key}, file {i} does not have a final component to its download path")))?;

            if final_dir.as_os_str() != cfg.levels.standard_subdir().as_os_str() {
                let final_dir = final_dir.as_os_str().to_string_lossy();
                anyhow::bail!(format!("In met type {met_key}, file type {i}'s final component ({final_dir}) is not consistent with its declared levels ({})", cfg.levels))
            }

            let parent_dir = download_dir.parent()
                .ok_or_else(|| anyhow::Error::msg(format!("In met type {met_key}, cannot get parent directory of file type {i}'s download path")))?;

            match &cfg.data_type {
                MetDataType::Met => {
                    if geos_path.is_none() {
                        geos_path = Some(parent_dir.to_owned());
                    } else {
                        anyhow::bail!("Met type {met_key} defines inconsistent parent directories for its met files");
                    }
                },
                MetDataType::Chm => {
                    if chem_path.is_none() {
                        chem_path = Some(parent_dir.to_owned());
                    } else {
                        anyhow::bail!("Met type {met_key} defines inconsistent parent directories for its chem files");
                    }
                },
                MetDataType::Other(v) => {
                    info!("Ignoring met type of {v} for GEOS met/chm paths")
                }
            }
        }

        let geos_path = geos_path.ok_or_else(|| anyhow::Error::msg(
            format!("Met type {met_key} defines no met files for download")
        ))?;

        let chem_path = chem_path.unwrap_or_else(|| {
            warn!("Met type {met_key} defines no chem files for download");
            geos_path.clone()
        });

        Ok((geos_path, chem_path))
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
    

    /// Get the sequence of [`DefaultOptions`] in time order.
    pub fn get_all_defaults(&self) -> Vec<&DefaultOptions> {
        let mut all_options: Vec<&DefaultOptions> = self.default_options.iter()
            .collect();

        // Order by start date, treating None as the earliest possible 
        all_options.sort_by(|a, b| {
            match (a.start_date, b.start_date) {
                (None, None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(d1), Some(d2)) => d1.cmp(&d2),
            }
        });

        all_options
    }

    /// Get the sequence of [`DefaultOptions`] in time order and check that none overlap in time. 
    /// 
    /// Returns an `Err` (with a [`DefaultOptsQueryError::MatchesOverlap`] inner value) if any two
    /// sets of default options do overlap in time.
    pub fn get_all_defaults_check_overlap(&self) -> Result<Vec<&DefaultOptions>, DefaultOptsQueryError> {
        let all_options = self.get_all_defaults();
        for pair in all_options.iter().combinations(2) {
            if pair[0].overlaps(pair[1]) {
                return Err(DefaultOptsQueryError::MatchesOverlap(pair[0].to_string(), pair[1].to_string()))
            }
        }
        Ok(all_options)
    }


    /// Get the [`DefaultOptions`] instance for a given date.
    /// 
    /// Returns a `Err` if 0 or >1 option set matches the date. The inner value will be a
    /// [`DefaultOptsQueryError::NoMatches`] for 0 and [`DefaultOptsQueryError::MultipleMatches`]
    /// for >1.
    pub fn get_defaults_for_date(&self, date: NaiveDate) -> Result<&DefaultOptions, DefaultOptsQueryError> {
        let all_options = self.default_options.as_slice();

        // Filter down to the rows which apply to this date. If >1 or 0, that is an error.
        let all_options: Vec<&DefaultOptions> = all_options.into_iter()
            .filter(|o| {
                match (o.start_date, o.end_date) {
                    (None, None) => true,
                    (None, Some(end)) => date < end,
                    (Some(start), None) => date >= start,
                    (Some(start), Some(end)) => start <= date && date < end,
                }
            }).collect();

        if all_options.len() == 1 {
            Ok(all_options[0])
        } else if all_options.is_empty() {
            Err(DefaultOptsQueryError::NoMatches(date))
        } else {
            let matches = all_options.iter()
                .map(|o| o.to_string())
                .collect_vec();
            Err(DefaultOptsQueryError::MultipleMatches { date, matches })
        }
    }

    /// Get the information about a job queue by name
    /// 
    /// If the queue does not have a section defined in the configuration, then the
    /// default queue (allocated 1 processor) is returned.
    pub fn get_queue(&self, queue_name: &str) -> Option<JobQueueOptions> {
        self.execution.queues
            .get(queue_name)
            .map(|q| q.to_owned())
    }
}

/// Configuration section dealing with how jobs are run
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExecutionConfig {
    /// Maximum number of jobs to run simultaneously
    #[serde(default)]
    pub queues: HashMap<String, JobQueueOptions>,

    /// Maximum number of threads to let numpy use
    pub max_numpy_threads: u32,

    /// Full glob pattern (including directory) to use to find input files
    pub input_file_pattern: String,

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

    /// Map of available ginput versions to use.
    pub ginput: HashMap<String, GinputConfig>,

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
            queues: Default::default(), 
            max_numpy_threads: 2, 
            hours_to_keep: 168,
            input_file_pattern: "input_file_2020*.txt".to_owned(),
            download_server: Url::parse(&format!("ftp://{host}/")).unwrap_or_else(|_| Url::parse("ftp://localhost/").unwrap()), 
            download_root: Default::default(), 
            output_path: Default::default(), 
            std_sites_tar_output: Default::default(), 
            std_sites_output_base: Default::default(),
            start_jobs_freq: 60.0,
            ginput: HashMap::new(),
            simulate: false
        }
    }
}

/// Configuration describing an available version of ginput that the automation can call.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum GinputConfig {
    /// A ginput installation to be called via its `run_ginput.py` entry point. Requires
    /// one option, `entry_point_path`, which is the path to the `run_ginput.py` file.
    Script{entry_point_path: PathBuf}
}

impl GinputConfig {
    pub async fn start_job_for_date(&self, 
        conn: &mut MySqlConn,
        date: NaiveDate,
        job: &crate::jobs::Job,
        config: &Config
    ) -> JobResult<crate::jobs::GinputRunner> {
        match self {
            GinputConfig::Script { entry_point_path } => {
                crate::jobs::start_job_for_date_through_shell(conn, date, job, config, &entry_point_path)
                .await
            },
        }
    }

    pub async fn start_lut_regen(&self) -> JobResult<crate::jobs::GinputRunner> {
        match self {
            GinputConfig::Script { entry_point_path } => {
                crate::jobs::start_lut_regen_through_shell(&entry_point_path)
                    .await
            }
        }
    }
}

/// Configuration section dealing with input data for jobs
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DataConfig {
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
    pub product: met::MetProduct,

    /// Whether this set of files is meteorology or chemistry
    pub data_type: met::MetDataType,

    /// What set of vertical levels these files represent.
    pub levels: met::MetLevels,

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

    /// The directory to download the data into. For met products intended to be used with ginput v1.y.z,
    /// this must still follow the expected directory structure for GEOS data. Namely:
    /// * Surface, hybrid eta level, and fixed pressure level files must reside in `Nx`, `Nv`, and `Np`
    ///   subdirectories, respectively.
    /// * Met data for a given met data source must be in subdirectories of the same path; that is, if
    ///   the eta level files are in `/data/met/Nv`, the surface/2D files must be in `/data/met/Nx`.
    ///   Chemistry data can be in the same directory or separate, e.g. in this example, the eta-level
    ///   chem files could go in `/data/met/Nv` or a separate directory such as `/data/chm/Nv`. The
    ///   latter is recommended.
    pub download_dir: PathBuf
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

    /// Provide a vector of datetimes when this file type is expected to exist on a given date
    pub fn times_on_day(&self, date: NaiveDate) -> Vec<NaiveDateTime> {
        let end = date.and_hms_opt(0, 0, 0).unwrap() + Duration::days(1);
        let mut file_time = date.and_hms_opt(0, 0, 0).unwrap();
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
    pub fn expected_files_on_day(&self, date: NaiveDate) -> anyhow::Result<Vec<PathBuf>> {
        let save_dir = &self.download_dir;
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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AdminConfig {
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
            soft_job_limit: 100,
            hard_job_limit: 100,
            acknowledgement_message: Default::default() 
        }
    }
}


/// Configuration for default choices of Ginput version, meteorology files, etc. for different time periods.
/// 
/// The time range is determined by `start_date` and `end_date`. If both are given, the this applies to all
/// dates `start_date <= date < end_date`. If `start_date` is `None`, then this applies to all dates up to
/// (but not including) the end date, and vice versa if `end_date` is `None`. If both are `None` this applies
/// to all dates.
/// 
/// `ginput` and `met` must be valid keys for the `.execution.ginput` and `.data.download` maps, respectively.
/// These specify which installation of ginput and which set of met data to use for that time period.
/// 
/// Note that if you access the vector of these directly there is no guarantee that they are time-ordered or
/// non-overlapping. Use the methods [`Config::get_all_defaults`] and [`Config::get_all_defaults_check_overlap`]
/// for that.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DefaultOptions {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub ginput: String,
    pub met: String,
}


impl DefaultOptions {
    // Test whether this `DefaultOptions` instance overlaps another in time
    fn overlaps(&self, other: &Self) -> bool {
        // Can't use the utils::date_range_overlap function because that doesn't handle optional dates
        // TODO: make this a util function?
        match (self.start_date, self.end_date, other.start_date, other.end_date) {
            (None, None, _, _) => true,
            (_, _, None, None) => true,
            (None, Some(_), None, Some(_)) => true,
            (None, Some(a2), Some(b1), None) => a2 > b1,
            (None, Some(a2), Some(b1), Some(_)) => a2 > b1,
            (Some(a1), None, None, Some(b2)) => a1 < b2,
            (Some(_), None, Some(_), None) => true,
            (Some(a1), None, Some(_), Some(b2)) => a1 < b2,
            (Some(a1), Some(_), None, Some(b2)) => a1 > b2,
            (Some(_), Some(a2), Some(b1), None) => a2 < b1,
            (Some(a1), Some(a2), Some(b1), Some(b2)) => {
                if a2 < b1 || b2 < a1 { false } else { true }
            },
        }
    }
}

impl Display for DefaultOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} to {}: ginput = {}, met = {}]", 
            self.start_date.map(|d| d.to_string()).unwrap_or_else(|| "None".to_owned()),
            self.end_date.map(|d| d.to_string()).unwrap_or_else(|| "None".to_owned()),
            self.ginput,
            self.met
        )
    }
}


/// A structure describing configuration of a job queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobQueueOptions {
    /// The maximum number of processors that this queue can use (default = 1)
    pub max_num_procs: usize
}

impl Default for JobQueueOptions {
    fn default() -> Self {
        Self { max_num_procs: 1 }
    }
}


/// Configuration for how frequently elements of the systemd service run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceTimingOptions {
    /// How many hours between attempts to download the met data. 
    /// The met download will run on even multiples of that hour,
    /// e.g. if this is 6, then the met download will run at 00:00,
    /// 06:00, 12:00, and 18:00
    pub met_download_hours: u32,

    /// How many seconds between attempts to start jobs. As with met,
    /// the attempts run on even multiples of this value.
    pub job_start_seconds: u32,

    /// How frequently (in days) to insert jobs to regenerate the stratosphere
    /// look up tables for ginput. 
    pub lut_regen_days: u32,

    /// What time of the day, in HH:MM format, to run the LUT regen. If
    /// omitted, then that will run at midnight.
    pub lut_regen_at: Option<String>,

    /// How frequently (in hours) to check for new days to generate standard
    /// site priors and submit jobs.
    pub std_site_gen_hours: u32,

    /// How many minutes to add to the hours when determining when to run the
    /// standard sites. E.g., setting this to 180 when `std_site_gen_hours` is 24
    /// would run the standard sites at 03:00 every day.
    pub std_site_gen_offset_minutes: Option<u32>
}

impl Default for ServiceTimingOptions {
    fn default() -> Self {
        Self { 
            met_download_hours: 6, 
            job_start_seconds: 60, 
            lut_regen_days: 24, 
            lut_regen_at: Some("00:00".to_string()), 
            std_site_gen_hours: 24, 
            std_site_gen_offset_minutes: Some(180) 
        }
    }
}

/// Configuration for how to send emails and who to contact if there is a severe problem
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailConfig {
    /// The email address that emails from this system come from.
    /// The default is "noreplay@<hostname>", where <hostname> is
    /// the host name of the system.
    from_address: Mailbox,

    /// A list of emails to contact in the event of a severe error
    /// that needs addressed by the administrators.
    admin_emails: Mailboxes,

    /// Which email backend to use to send the emails.
    backend: EmailBackend
}

impl Default for EmailConfig {
    fn default() -> Self {
        let user = "noreply";
        let host = whoami::hostname();
        let email = lettre::Address::new(user, host)
            .expect("user@hostname cannot be used as a valid email address, you will need to configure the 'from_address' in the 'email' section of the config");
        let from_addr = Mailbox::new(None, email);
        Self { 
            from_address: from_addr, 
            admin_emails: Default::default(), 
            backend: Default::default() 
        }
    }
}

impl EmailConfig {
    /// Send an email, using the configured backend, from the configured address.
    pub fn send_mail(&self, to: &[&str], cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<(), EmailError> {
        let from = self.from_address.to_string();
        match &self.backend {
            EmailBackend::Internal(backend) => {
                backend.send_mail(to, &from, cc, bcc, subject, message)
            },
            EmailBackend::Mailx(backend) => {
                backend.send_mail(to, &from, cc, bcc, subject, message)
            },
        }
    }

    /// Send an email to the admins, using the configured backed and from address
    pub fn send_mail_to_admins(&self, subject: &str, message: &str) -> Result<(), EmailError> {
        let to_strings: Vec<_> = self.admin_emails.iter()
            .map(|e| e.to_string())
            .collect();
        let to: Vec<_> = to_strings.iter()
            .map(|s| s.as_str())
            .collect();
        self.send_mail(to.as_slice(), None, None, subject, message)
    }
}

/// An enum specifying which method to use to send emails
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EmailBackend {
    /// Uses the `Lettre` crate to connect to a local SMTP server
    /// Note that this is *unencrypted*, but that is assumed to be acceptible
    /// since the connection is on the local machine. This is the default.
    Internal(crate::email::Lettre),

    /// Calls the `mailx` command line client via the shell to send
    /// emails.
    Mailx(crate::email::Mailx)
}

impl Default for EmailBackend {
    fn default() -> Self {
        Self::Internal(crate::email::Lettre::default())
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
