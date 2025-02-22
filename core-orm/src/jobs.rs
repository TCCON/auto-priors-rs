//! The main ORM interface to the jobs queue.
//! 
//! 
use std::{collections::{HashMap, HashSet}, fmt::{Debug, Display}, hash::RandomState, path::{Path, PathBuf}, process::Stdio, str::FromStr};

use anyhow::Context;
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use itertools::{Itertools, izip};
use log::{info, warn, debug};
use serde::{Deserialize, Serialize};
use serde_json;
use sqlx::{self, FromRow, Type, Acquire};
use tabled::Tabled;
use tokio::task::JoinHandle;

use crate::{config::{Config, EmailConfig}, error::{JobAddError, JobError, JobPriorityError, JobResult}, siteinfo, utils, MySqlConn, PoolWrapper};

// TODO: change times from Naive to Local (needs changing SQL to timestamp?)

/// Deserialize a JSON string into a vector of a deserializable type
fn str_to_json_arr<'a, T: Deserialize<'a>> (s: &'a str) -> JobResult<Vec<T>> {
    Ok(serde_json::from_str(s)?)
}

/// An enum representing possible states for a priors job
#[derive(Debug, Type, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i8)]
pub enum JobState {
    /// **\[default\]** This job is queued but has not begun to execute. `i8` value = `0`.
    Pending = 0,
    /// This job is currently processing. `i8` value = `1`.
    Running = 1,
    /// This job has finished successfully. `i8` value = `2`.
    Complete = 2,
    /// This job failed while running. `i8` value = `3`.
    Errored = 3,
    /// The output from the job has been deleted. `i8` value = `4`.
    Cleaned = 4,
}

impl JobState {
    pub fn is_over(&self) -> bool {
        match self {
            JobState::Pending => false,
            JobState::Running => false,
            JobState::Complete => true,
            JobState::Errored => true,
            JobState::Cleaned => true,
        }
    }
}

impl Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Errored => "errored",
            Self::Cleaned => "cleaned"
        };

        write!(f, "{s}")
    }
}

impl Default for JobState {
    /// Return the default [`JobState`], `Pending`.
    fn default() -> Self {
        JobState::Pending
    }
}

impl From<JobState> for i8 {
    fn from(v: JobState) -> Self {
        match v {
            JobState::Pending => 0,
            JobState::Running => 1,
            JobState::Complete => 2,
            JobState::Errored => 3,
            JobState::Cleaned => 4
        }
    }
}

impl FromStr for JobState {
    type Err = JobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" | "p" => Ok(Self::Pending),
            "running" | "r" => Ok(Self::Running),
            "complete" | "d" => Ok(Self::Complete),
            "errored" | "e" => Ok(Self::Errored),
            "cleaned" | "x" => Ok(Self::Cleaned),
            _ => Err(JobError::InvalidStateName(s.to_string()))
        }
    }
}

impl TryFrom<i8> for JobState {
    type Error = JobError;

    /// Convert an i8 to a [`JobState`]
    /// 
    /// # Errors
    /// 
    /// An `Err` is returned if the i8 value does not correspond to
    /// one of the variants of [`JobState`]
    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Complete),
            3 => Ok(Self::Errored),
            4 => Ok(Self::Cleaned),
            _ => Err(JobError::InvalidState(value))
        }
    }
}


/// An enum representing the possible options for creating a tarball of job output
#[derive(Debug, Type, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TarChoice {
    /// Do not make a tarball of the job output. `i8` value = `0`.
    No = 0,
    /// **\[default\]** Do make a tarball of the job output. `i8` value = `1`.
    Yes = 1,
    /// Make a tarball with a special name, compatible with the EGI automation
    /// for EM27s. `i8` value = `2`.
    Egi = 2
}

impl Display for TarChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::No => "no",
            Self::Yes => "yes",
            Self::Egi => "EGI"
        };

        write!(f, "{s}")
    }
}

impl Default for TarChoice {
    /// Return the default variant of [`TarChoice`] (`Yes`)
    fn default() -> Self {
        Self::Yes
    }
}

impl From<TarChoice> for i8 {
    fn from(v: TarChoice) -> Self {
        match v {
            TarChoice::No => 0,
            TarChoice::Yes => 1,
            TarChoice::Egi => 2
        }
    }
}

impl TryFrom<i8> for TarChoice {
    type Error = JobError;

    /// Convert a `i8` into the equivalent [`TarChoice`] variant
    /// 
    /// # Errors
    /// 
    /// An `Err` is returned is the `i8` value does not correspond to
    /// any [`TarChoice`] variant.
    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::No),
            1 => Ok(Self::Yes),
            2 => Ok(Self::Egi),
            _ => Err(JobError::InvalidTar(value))
        }
    }
}

/// An enum representing the possible output file types for the model (`.mod`) files.
#[derive(Debug, Type, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum ModFmt {
    /// Do not create `.mod` files. String representation = `"None"`.
    None,
    /// **\[default\]** Create text `.mod` files. String representation = `"Text"`.
    Text
}

impl Default for ModFmt {
    /// Return the default variant of [`ModFmt`], `Text`.
    fn default() -> Self {
        Self::Text
    }
}

impl Display for ModFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s =match self {
            ModFmt::None => "None".to_owned(),
            ModFmt::Text => "Text".to_owned(),
        };
        write!(f, "{s}")
    }
}

impl FromStr for ModFmt {
    type Err = JobError;

    /// Convert a string into a [`ModFmt`] variant.
    /// 
    /// Possible values are "none" and "text" (case insensitive).
    /// 
    /// # Errors
    /// An `Err` is returned if the given string does not match any of the
    /// [`ModFmt`] variants.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            _ => Err(JobError::InvalidModFmt(s.to_owned()))
        }
    }
}


/// An enum representing the possible output file types for the `.vmr` files.
#[derive(Debug, Type, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum VmrFmt {
    /// Do not create `.vmr` files. String representation = `"None"`.
    None,
    /// **\[default\]** Create text `.vmr` files. String representation = `"Text"`.
    Text
}

impl Default for VmrFmt {
    /// Return the default variant of [`VmrFmt`], `Text`.
    fn default() -> Self {
        Self::Text
    }
}

impl Display for VmrFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            VmrFmt::None => "None".to_owned(),
            VmrFmt::Text => "Text".to_owned(),
        };
        
        write!(f, "{s}")
    }
}

impl FromStr for VmrFmt {
    type Err = JobError;

    /// Convert a string into a [`VmrFmt`] variant.
    /// 
    /// Possible values are "none" and "text" (case insensitive).
    /// 
    /// # Errors
    /// An `Err` is returned if the given string does not match any of the
    /// [`VmrFmt`] variants.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            _ => Err(JobError::InvalidVmrFmt(s.to_owned()))
        }
    }
}


/// An enum representing the possible output file types for the model a priori (`.map`) files.
#[derive(Debug, Type, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum MapFmt {
    /// Do not create `.map` files. String representation = `"None"`.
    None,
    /// **\[default\]** Create text `.map` files. String representation = `"Text"`.
    Text,
    /// Create netCDF4 `.map` files. String representation = `"NetCDF"`.
    NetCDF,
    /// Create both text and netCDF files. String representation = "TxtAndNc"
    TextAndNetCDF
}

impl Default for MapFmt {
    /// Return the default variant of [`MapFmt`], `Text`.
    fn default() -> Self {
        Self::Text
    }
}

impl Display for MapFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MapFmt::None => "None".to_owned(),
            MapFmt::Text => "Text".to_owned(),
            MapFmt::NetCDF => "NetCDF".to_owned(),
            MapFmt::TextAndNetCDF => "TxtAndNc".to_owned()
        };

        write!(f, "{s}")
    }
}

impl FromStr for MapFmt {
    type Err = JobError;

    /// Convert a string into a [`MapFmt`] variant.
    /// 
    /// Possible values are "none", "text", and "netcdf" (case insensitive).
    /// 
    /// # Errors
    /// An `Err` is returned if the given string does not match any of the
    /// [`MapFmt`] variants.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "none" => Ok(Self::None),
            "text" => Ok(Self::Text),
            "netcdf" => Ok(Self::NetCDF),
            "txtandnc" => Ok(Self::TextAndNetCDF),
            _ => Err(JobError::InvalidMapFmt(s.to_owned()))
        }
    }
}


/// An intermediate job representation that maps directly to the MySQL table.
/// 
/// External crates should interact with the [`Job`] struct, and that should
/// have methods that internally work with a `QJob` instance as needed to
/// interface with the MySQL table.
#[derive(Debug, FromRow, Serialize, Deserialize)]
pub(crate) struct QJob { 
    pub(crate) job_id: i32,
    pub(crate) state: i8,
    pub(crate) site_id: String,
    pub(crate) start_date: NaiveDate,
    pub(crate) end_date: NaiveDate,
    pub(crate) lat: String,
    pub(crate) lon: String,
    pub(crate) email: Option<String>,
    pub(crate) delete_time: Option<NaiveDateTime>,
    pub(crate) priority: i32,
    pub(crate) queue: String,
    pub(crate) met_key: Option<String>,
    pub(crate) ginput_key: Option<String>,
    pub(crate) save_dir: String,
    pub(crate) save_tarball: i8,
    pub(crate) mod_fmt: String,
    pub(crate) vmr_fmt: String,
    pub(crate) map_fmt: String,
    pub(crate) submit_time: NaiveDateTime,
    pub(crate) complete_time: Option<NaiveDateTime>,
    pub(crate) output_file: Option<String>
}

impl TryFrom<Job> for QJob {
    type Error = anyhow::Error;

    /// Try making a `QJob` instance from a [`Job`]
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * Could not convert the `save_dir` or `output_file` paths to UTF strings
    /// * Could not serialize the `site_id`, `lat`, or `lon` vectors to JSON strings.
    fn try_from(j: Job) -> Result<Self, Self::Error> {
        let save_dir = j.root_save_dir
            .to_str()
            .ok_or(anyhow::anyhow!("Failed to convert save_dir to a UTF string"))?
            .to_owned();
        
        let output_file = if let Some(o) = j.output_file {
            Some(o.to_str()
                  .ok_or(anyhow::anyhow!("Failed to convert output_file to UTF string"))?
                  .to_owned())
        }else{
            None
        };

        Ok(QJob { 
            job_id: j.job_id,
            state: j.state as i8,
            site_id: serde_json::to_string(&j.site_id)?,
            start_date: j.start_date,
            end_date: j.end_date,
            lat: serde_json::to_string(&j.lat)?,
            lon: serde_json::to_string(&j.lon)?,
            email: j.email,
            delete_time: j.delete_time,
            priority: j.priority,
            queue: j.queue,
            met_key: j.met_key,
            ginput_key: j.ginput_key,
            save_dir: save_dir,
            save_tarball: j.save_tarball as i8,
            mod_fmt: j.mod_fmt.to_string(),
            vmr_fmt: j.vmr_fmt.to_string(),
            map_fmt: j.map_fmt.to_string(),
            submit_time: j.submit_time,
            complete_time: j.complete_time,
            output_file: output_file
        })
    }
}

pub struct VerboseDisplayJob<'j> {
    job: &'j Job
}

impl<'j> Display for VerboseDisplayJob<'j> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lats = self.job.lat.iter()
            .map(|v| {
                v.map(|y| format!("{y:.3}"))
                .unwrap_or_else(|| "DEF".to_string())
            }).join(", ");

        let lons = self.job.lon.iter()
            .map(|v| {
                v.map(|x| format!("{x:.3}"))
                .unwrap_or_else(|| "DEF".to_string())
            }).join(", ");

        let complete_string = if let Some(time) = self.job.complete_time {
            format!("completed at {time}")
        } else {
            "not completed yet".to_string()
        };

        let delete_string = if let Some(time) = self.job.delete_time {
            if let JobState::Cleaned = self.job.state {
                format!("Deletion time was {time}.")
            } else {
                format!("Will be cleaned up after {time}.")
            }
        } else {
            "Has no deletion time.".to_string()
        };
        
        writeln!(f, "Job {} ({}):", self.job.job_id, self.job.state)?;
        writeln!(f, "  Submitted by {} at {}, {}", 
            self.job.email.as_deref().unwrap_or("NONE"),
            self.job.submit_time,
            complete_string
        )?;
        writeln!(f, "  {delete_string}")?;
        writeln!(f, "  Priority {} in queue {}", self.job.priority, self.job.queue)?;
        writeln!(f, "  Dates: {} to {}", self.job.start_date, self.job.end_date)?;
        writeln!(f, "  Site IDs:   {}", self.job.site_id.join(", "))?;
        writeln!(f, "  Latitudes:  {lats}")?;
        writeln!(f, "  Longitudes: {lons}")?;
        if let Some(out) = &self.job.output_file {
            writeln!(f, "  Output stored in {}", out.display())?;
        } else {
            writeln!(f, "  Save under {} as tarball {}", self.job.root_save_dir.display(), self.job.save_tarball)?;
        }
        writeln!(f, "  Formats: mod = {}, vmr = {}, map = {}", self.job.mod_fmt, self.job.vmr_fmt, self.job.map_fmt)?;
        writeln!(f, "  Met = {}, ginput = {}", 
            self.job.met_key.as_deref().unwrap_or("DEFAULT"),
            self.job.ginput_key.as_deref().unwrap_or("DEFAULT")
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// The public interface to the Jobs MySQL table.
pub struct Job {
    /// **\[primary key\]** The unique integer ID of this job
    pub job_id: i32,

    /// State of the job, i.e. pending, running, etc.
    pub state: JobState,

    /// The site IDs (generally two characters each) of each location to generate priors
    /// for in this job. The MySQL table enforces that this and the `lat`/`lon` vectors
    /// are the same length.
    pub site_id: Vec<String>,

    /// First date to generate priors for
    pub start_date: NaiveDate,

    /// Date after the last one to generate priors for, i.e. an exclusive end date
    pub end_date: NaiveDate,

    /// Latitudes to generate priors for. Must be the same length as the `site_id` field. 
    /// If one of these is `None`, the job will use the latitude defined in the `StdSitesInfo`
    /// table.
    pub lat: Vec<Option<f32>>,

    /// Longitudes to generate priors for. Same caveats as the `lat` field.
    pub lon: Vec<Option<f32>>,

    /// Email to contact when the job is complete. May be `None` for automatic/background jobs.
    pub email: Option<String>,

    /// Local date & time to clean up output from this job. If `None`, output will never be deleted.
    pub delete_time: Option<NaiveDateTime>,

    /// Priority to give this job, greater values will run first.
    pub priority: i32,

    /// Name of the queue in which this job should run; queues are defined in the configuration.
    pub queue: String,

    /// The key from the configuration file corresponding to which meteorology data to use for this
    /// job. If `None`, that indicates that the default met for the given dates should be used.
    pub met_key: Option<String>,

    /// The key from the configuration file corresponding to which version of ginput to use for this
    /// job. If `None`, that indicates that the default version for the given dates should be used.
    pub ginput_key: Option<String>,

    /// Where to save the output. This will be the output directory for ALL jobs, a particular job
    /// will have a subdirectory or tarfile under here.
    root_save_dir: PathBuf,

    /// Whether to generate a tarball of the output or not.
    /// May also indicate to give it an EGI-compatible name.
    pub save_tarball: TarChoice,

    /// Format to save the `.mod` files in.
    pub mod_fmt: ModFmt,

    /// Format to save the `.vmr` files in.
    pub vmr_fmt: VmrFmt,

    /// Format to save the `.map` files in.
    pub map_fmt: MapFmt,

    /// Time that this job was submitted to the queue.
    pub submit_time: NaiveDateTime,

    /// Time that this job was completed. `None` indicates the job is waiting.
    pub complete_time: Option<NaiveDateTime>,

    /// Location of the output data, either a directory or tarball.
    pub output_file: Option<PathBuf>
}

impl Tabled for Job {
    const LENGTH: usize = 10;

    fn fields(&self) -> Vec<std::borrow::Cow<'_, str>> {
        vec![
            format!("{}", self.job_id).into(),
            self.state.to_string().into(),
            self.site_id.join(", ").into(),
            self.start_date.to_string().into(),
            self.end_date.to_string().into(),
            self.email.as_deref().unwrap_or("-").into(),
            format!("{}", self.priority).into(),
            self.queue.as_str().into(),
            self.submit_time.to_string().into(),
            self.complete_time.map(|dt| dt.to_string()).unwrap_or_else(|| "-".to_string()).into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "job_id".into(),
            "state".into(),
            "site_id".into(),
            "start_date".into(),
            "end_date".into(),
            "email".into(),
            "priority".into(),
            "queue".into(),
            "submit_time".into(),
            "complete_time".into(),
        ]
    }
}

impl TryFrom<QJob> for Job {
    type Error = JobError;

    /// Try creating a `Job` instance from the SQL-mapped `QJob`.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * the job state was an unknown integer
    /// * the `site_id` string could not be deserialized to an array
    /// * the `lat` or `lon` values could not be deserialized to arrays
    /// * the `save_tarball` value was an unknown integer
    /// * the `mod_fmt`, `vmr_fmr`, or `map_fmt` value was an unknown string.
    /// 
    /// Generally, errors from this function are fatal and should trigger a message
    /// to the admins, since that indicates the mapping between SQL types and Rust
    /// types has gotten out of sync.
    fn try_from(q: QJob) -> Result<Self, Self::Error> {
        Ok(Job {
            job_id: q.job_id,
            state: JobState::try_from(q.state)?,
            site_id: str_to_json_arr(&q.site_id)?,
            start_date: q.start_date,
            end_date: q.end_date,
            lat: str_to_json_arr(&q.lat)?,
            lon: str_to_json_arr(&q.lon)?,
            email: q.email,
            delete_time: q.delete_time,
            priority: q.priority,
            queue: q.queue,
            met_key: q.met_key,
            ginput_key: q.ginput_key,
            root_save_dir: PathBuf::from(q.save_dir),
            save_tarball: TarChoice::try_from(q.save_tarball)?,
            mod_fmt: ModFmt::from_str(&q.mod_fmt)?,
            vmr_fmt: VmrFmt::from_str(&q.vmr_fmt)?,
            map_fmt: MapFmt::from_str(&q.map_fmt)?,
            submit_time: q.submit_time,
            complete_time: q.complete_time,
            output_file: q.output_file.and_then(|p| Some(PathBuf::from(p)))
        })
    }
}

impl Display for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let job_id = self.job_id;
        let dates = format!("{}-{}", self.start_date.format("%Y-%m-%d 00:00"), self.end_date.format("%Y-%m-%d 00:00"));
        let n = self.site_id.len();
        write!(f, "AutoModMaker Job {job_id} ({dates}, {n} sites)")
    }
}

impl Job {
    pub fn verbose_display(&self) -> VerboseDisplayJob {
        VerboseDisplayJob { job: self }
    }

    pub fn to_long_string(&self) -> String {
        let job_id = self.job_id;
        let dates = format!("{}-{}", self.start_date.format("%Y-%m-%d 00:00"), self.end_date.format("%Y-%m-%d 00:00"));
        let lons = self.lon.iter()
            .map(|x| {
                if let Some(x) = x {
                    format!("{x:.2}")
                } else {
                    "[default]".to_string()
                }
            }).join(",");
        let lats = self.lat.iter()
            .map(|x| {
                if let Some(x) = x {
                    format!("{x:.2}")
                } else {
                    "[default]".to_string()
                }
            })
            .join(",");
        let sites = self.site_id.join(",");
        format!("AutoModMaker Job {job_id} ({dates}, lons = {lons}, lats = {lats}, sites = {sites})")
    }

    pub fn run_dir(&self, basename_only: bool) -> PathBuf {
        let job_subdir = format!("job{:09}", self.job_id);
        if basename_only {
            return PathBuf::from(job_subdir);
        }
        let base_run_dir = self.root_save_dir.join(".running").join(job_subdir);
        base_run_dir
    }

    pub fn mod_run_dir(&self, site_id: &str, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
        self.run_or_output_subdir(site_id, config, true, "vertical")
            .with_context(|| format!("Error occurred while trying to infer .mod run storage sub-directory for job #{}", self.job_id))
    }

    pub fn mod_output_dir(&self, site_id: &str, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
        self.run_or_output_subdir(site_id, config, false, "vertical")
        .with_context(|| format!("Error occurred while trying to infer .mod output sub-directory for job #{}", self.job_id))
    }

    pub fn vmr_run_dir(&self, site_id: &str, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
        self.run_or_output_subdir(site_id, config, true, "vmrs-vertical")
        .with_context(|| format!("Error occurred while trying to infer .vmr run storage sub-directory for job #{}", self.job_id))
    }

    pub fn vmr_output_dir(&self, site_id: &str, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
        self.run_or_output_subdir(site_id, config, false, "vmrs-vertical")
        .with_context(|| format!("Error occurred while trying to infer .vmr output sub-directory for job #{}", self.job_id))
    }

    pub fn map_run_dir(&self, site_id: &str, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
        self.run_or_output_subdir(site_id, config, true, "maps-vertical")
        .with_context(|| format!("Error occurred while trying to infer .map/.map.nc run storage sub-directory for job #{}", self.job_id))
    }

    pub fn map_output_dir(&self, site_id: &str, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
        self.run_or_output_subdir(site_id, config, false, "maps-vertical")
        .with_context(|| format!("Error occurred while trying to infer .map/.map.nc output sub-directory for job #{}", self.job_id))   
    }

    fn run_or_output_subdir(&self, site_id: &str, config: &Config, run_dir: bool, bottom_subdir: &str) -> anyhow::Result<Vec<PathBuf>> {
        let base_dir = if run_dir {
            self.run_dir(false)
        } else if let Some(p) = &self.output_file {
            if p.is_file() {
                anyhow::bail!("Output is a tar file for job {}, cannot get a subdirectory of it", self.job_id);
            } else {
                p.to_path_buf()
            }
        } else {
            anyhow::bail!("Output file/directory not yet set for job {}", self.job_id);
        };

        let subdirs = self.get_possible_output_subdirs(config)?;
        let output_dirs = subdirs.into_iter()
            .map(|d| base_dir.join(d).join(site_id).join(bottom_subdir))
            .collect_vec();
        Ok(output_dirs)
    }

    fn get_possible_output_subdirs(&self, config: &Config) -> anyhow::Result<Vec<String>> {
        if let Some(met_key) = &self.met_key {
            let subdir = config.get_ginput_output_subdirs_for_met(met_key)?;
            Ok(vec![subdir])
        } else {
            let mut all_subdirs = HashSet::new();
            for date in utils::DateIterator::new_one_range(self.start_date, self.end_date) {
                let met_key = &config.get_defaults_for_date(date)?.met;
                let subdir = config.get_ginput_output_subdirs_for_met(met_key)?;
                all_subdirs.insert(subdir);
            }
            Ok(Vec::from_iter(all_subdirs.into_iter()))
        }
    }

    pub async fn get_jobs_list(conn: &mut MySqlConn, pending_and_running_only: bool) -> JobResult<Vec<Job>> {
        let qjobs = if pending_and_running_only{
            sqlx::query_as!(
                QJob,
                "SELECT * FROM Jobs WHERE state = ? OR state = ?",
                JobState::Pending,
                JobState::Running
            ).fetch_all(conn)
            .await?
        } else {
            sqlx::query_as!(
                QJob,
                "SELECT * FROM Jobs"
            ).fetch_all(conn)
            .await?
        };

        let jobs: Result<Vec<_>, _> = qjobs.into_iter()
            .map(|qjob| Job::try_from(qjob))
            .collect();

        jobs
    }

    pub async fn get_jobs_in_queue(conn: &mut MySqlConn, queue: &str) -> JobResult<Vec<Job>> {
        let qjobs = sqlx::query_as!(
            QJob,
            "SELECT * FROM Jobs WHERE queue = ?",
            queue
        ).fetch_all(conn)
        .await?;

        let jobs: Result<Vec<_>, _> = qjobs.into_iter()
            .map(|qjob| Job::try_from(qjob))
            .collect();

        jobs
    }

    /// Return a `Job` instance with the given `job_id`.
    /// 
    /// # Parameters
    /// * `conn` - a connection to the MySQL database.
    /// * `id` - the ID value to search for.
    /// 
    /// # Errors
    /// Returns an `Err` if
    /// 
    /// * no job with that ID was found.
    /// * the query could not be converted into the Rust `Job` type.
    pub async fn get_job_with_id(conn: &mut MySqlConn, id: i32) -> JobResult<Job> {
        let result = sqlx::query_as!(
                QJob,
                "SELECT * FROM Jobs WHERE job_id = ?",
                id
            ).fetch_one(conn)
            .await?;
    
        return Ok(Job::try_from(result)?)
    }

    pub async fn get_queues_with_pending_jobs(conn: &mut MySqlConn) -> anyhow::Result<Vec<String>> {
        let queues = sqlx::query!(
            "SELECT DISTINCT(queue) AS q FROM Jobs WHERE state = ?",
            JobState::Pending
        ).fetch_all(conn)
        .await?
        .into_iter()
        .map(|r| r.q)
        .collect();

        Ok(queues)
    }

    pub async fn get_jobs_in_state(conn: &mut MySqlConn, state: JobState) -> anyhow::Result<Vec<Job>> {
        let jobs: Result<Vec<Job>, _> = sqlx::query_as!(
            QJob,
            "SELECT * FROM Jobs WHERE state = ?",
            state
        ).fetch_all(conn)
        .await?
        .into_iter()
        .map(|q| q.try_into())
        .collect();

        Ok(jobs?)
    }

    pub async fn get_jobs_for_user_submitted_after(conn: &mut MySqlConn, user: &str, submitted_after: NaiveDate) -> anyhow::Result<Vec<Job>> {
        let jobs: Result<Vec<Job>, _> = sqlx::query_as!(
            QJob,
            "SELECT * FROM Jobs WHERE email = ? AND submit_time >= ?",
            user,
            submitted_after.and_hms_opt(0, 0, 0).unwrap()
        ).fetch_all(conn)
        .await?
        .into_iter()
        .map(|q| q.try_into())
        .collect();
        
        Ok(jobs?)
    }

    pub async fn summarize_active_jobs_by_submitter(conn: &mut MySqlConn) -> anyhow::Result<JobSummary> {
        let active_jobs = sqlx::query!(
            "SELECT email,COUNT(*) as num FROM Jobs WHERE state = ? OR state = ? GROUP BY email",
            JobState::Pending,
            JobState::Running,
        ).fetch_all(conn)
        .await?
        .into_iter()
        .map(|rec| {
            let submitter = rec.email.unwrap_or_else(|| "Command line".to_string());
            (submitter, rec.num as u64)
        }).collect_vec();
        Ok(JobSummary(active_jobs))
    }

    pub async fn summarize_jobs_completed_between(conn: &mut MySqlConn, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<(JobSummary, JobSummary)> {
        let end_date = end_date.unwrap_or_else(|| chrono::Local::now().date_naive() + chrono::Duration::days(1));
        let successes = sqlx::query!(
            "SELECT email,COUNT(*) AS num FROM Jobs WHERE (state = ? OR state = ?) AND complete_time >= ? AND complete_time < ? GROUP BY email",
            JobState::Complete,
            JobState::Cleaned,
            start_date.and_hms_opt(0, 0, 0).unwrap(),
            end_date.and_hms_opt(0, 0, 0).unwrap()
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|rec| {
            let submitter = rec.email.unwrap_or_else(|| "Command line".to_string());
            (submitter, rec.num as u64)
        }).collect_vec();

        let failures = sqlx::query!(
            "SELECT email,COUNT(*) AS num FROM Jobs WHERE state = ? AND complete_time >= ? AND complete_time <= ? GROUP BY email",
            JobState::Errored,
            start_date,
            end_date
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|rec| {
            let submitter = rec.email.unwrap_or_else(|| "Command line".to_string());
            (submitter, rec.num as u64)
        }).collect_vec();

        Ok((JobSummary(successes), JobSummary(failures)))
    }

    /// Get the next job in `queue` which should be run
    /// 
    /// This will return the job with its queue value equal to `queue` that has
    /// the highest priority then earliest submission time. If there are no
    /// jobs in that queue, the return value will be `None`.
    /// 
    /// # Note
    /// This does *not* set the job state to running; if you are querying for the
    /// next job with the intent of starting it running, prefer [`Job::claim_next_job_in_queue`]
    /// or [`Job::claim_next_job_in_queue_with_opts`], which prevent race conditions
    /// if multiple connections try to get the next job for the same queue too closely in time.
    pub async fn get_next_job_in_queue<F: FairShare>(conn: &mut MySqlConn, queue: &str, fair_share: &F) -> JobResult<Option<Job>> {
        fair_share.next_job_in_queue(conn, queue).await
    }

    /// Try to "claim" the next job in a given queue by settings its state to 'running'
    /// 
    /// In a case where multiple connections to the database try to start the next job in a queue
    /// at similar times, we can end up in a race-like condition, where both connections query the
    /// database, get the same job ID, and both start running the same job because the second query
    /// came before the first one could set the job state to 'running'.
    /// 
    /// The solution is to use transactions, which prevent multiple connections from interacting with
    /// overlapping parts of the database at the same time. However, testing showed that this approach
    /// could deadlock if connection A held a "read" lock on a row when connection "B" went to set the
    /// state - B could not update the state because it was waiting for A to release its read lock,
    /// while A could not upgrade its read lock to write while B also held a lock on that row.
    /// 
    /// This function handles that by starting a transaction to get the next job, then querying and
    /// updating the job state within the transaction. Should either action deadlock, it waits for
    /// `delay` seconds, then tries again. If it tries `ntries` times and fails every time, then
    /// it returns with an error.
    /// 
    /// # Inputs
    /// - `conn`: connection to the MySQL database
    /// - `queue`: which queue in the Jobs table to get the next job to run from
    /// - `delay`: delay in seconds to wait after a deadlock before trying again
    /// - `ntries`: number of times to try resolving a deadlock before giving up and returning an error
    /// 
    /// # Returns
    /// - `Ok(Some(Job))` if there is a job to run and it successfully claimed it. The state will be 
    ///   [`JobState::Running`]
    /// - `Ok(None)` if there is no job in the given queue
    /// - `Err` if the queries fail or the deadlock cannot be resolved in `ntries` attempts.
    /// 
    /// # See also
    /// - [`Job::claim_next_job_in_queue`] for a version of this function with defaults for `delay` and `ntries`
    /// - [`Job::get_next_job_in_queue`] for a function that does not set the job state to 'running'
    pub async fn claim_next_job_in_queue_with_opts<F: FairShare>(conn: &mut MySqlConn, queue: &str, fair_share: &F, delay: f32, ntries: usize) -> JobResult<Option<Job>> {
        let delay_val = std::time::Duration::from_secs_f32(delay);
        
        let mut n = 0;
        while n < ntries {
            n += 1;

            let mut trans = conn.begin().await?;

            let mut job = match Self::get_next_job_in_queue(&mut trans, queue, fair_share).await {
                Ok(Some(j)) => {
                    j
                },
                Ok(None) => {
                    return Ok(None)
                },
                Err(JobError::DeadlockError(_)) => {
                    info!("SQL database deadlocked while getting next job, waiting {delay} s before querying again");
                    tokio::time::sleep(delay_val).await;
                    continue;
                },
                Err(e) => {
                    return Err(e)
                }
            };

            match job.set_state(&mut trans, JobState::Running).await {
                Ok(_) => {
                    trans.commit().await?;
                    return Ok(Some(job))
                },
                Err(JobError::DeadlockError(_)) => {
                    info!("SQL database deadlocked while setting job state, waiting {delay} s before querying again");
                    tokio::time::sleep(delay_val).await;
                    continue;
                },
                Err(e) => {
                    return Err(e)
                }
            }
        }

        Err(JobError::Other("Failed to claim next job due to repeated deadlocks".to_owned()))
    }

    /// Try to "claim" the next job in a given queue by settings its state to 'running'
    /// 
    /// This is the same as [`Job::claim_next_job_in_queue_with_opts`] with `delay = 1.0` and
    /// `ntries = 5`.
    pub async fn claim_next_job_in_queue<F: FairShare>(conn: &mut MySqlConn, queue: &str, fair_share: &F) -> JobResult<Option<Job>> {
        Self::claim_next_job_in_queue_with_opts(conn, queue, fair_share, 1.0, 5).await
    }

    pub async fn get_distinct_submitter_emails(conn: &mut MySqlConn) -> JobResult<Vec<String>> {
        let emails = sqlx::query!(
            "SELECT DISTINCT(email) FROM Jobs WHERE email IS NOT NULL",
        ).fetch_all(conn)
        .await?
        .into_iter()
        .map(|rec| rec.email.unwrap()) // SQL query ensures email is not null
        .collect_vec();

        Ok(emails)
    }

    /// Convert a user-inputted string of site IDs into a proper vector of site IDs
    /// 
    /// # Parameters
    /// * `site_id_str` - a comma-separated list of site IDs, e.g. "pa,oc,ci"
    pub fn parse_site_id_str(site_id_str: &str) -> JobResult<Vec<String>> {
        return site_id_str
                .split(',')
                .map(|s| s.trim().to_owned())
                .map(|s| if s.len() == 2 { 
                    Ok(s)
                } else { 
                    Err(JobError::CannotParseSiteId(format!(
                        "Cannot parse '{site_id_str}': must be a single two-character site ID or a comma-separated list of such IDs"
                    )))
                })
                .try_collect();
    }

    /// Convert a user-inputted string of latitudes into a proper vector of latitudes
    /// 
    /// # Parameters
    /// * `lat_str` - a comma-separated list of latitudes, e.g. "45,12.3,-8"
    /// 
    /// # Returns
    /// If the input string was empty, the inner return type will be `None`. Otherwise,
    /// it will be a vector of `Some<f32>`s.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * Any of the values could not be parsed into a float
    /// * Any of the values is outside the range \[-90, 90\].
    /// 
    /// # Notes
    /// This does *not* allow for a mix of numeric and null values; currently users
    /// must either input coordinates for all sites or none of them. In the latter case,
    /// an empty string is the only acceptable input.
    pub fn parse_lat_str(lat_str: &str) -> anyhow::Result<Option<Vec<Option<f32>>>> {
        return Self::parse_latlon_str(lat_str, 90.0, "Latitudes");
    }

    /// Convert a user-inputted string of longitudes into a proper vector of longitudes
    /// 
    /// # Parameters
    /// * `lon_str` - a comma-separated list of longitudes, e.g. "45,12.3,-8"
    /// 
    /// # Returns
    /// If the input string was empty, the inner return type will be `None`. Otherwise,
    /// it will be a vector of `Some<f32>`s.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * Any of the values could not be parsed into a float
    /// * Any of the values is outside the range \[-180, 180\].
    /// 
    /// # Notes
    /// This does *not* allow for a mix of numeric and null values; currently users
    /// must either input coordinates for all sites or none of them. In the latter case,
    /// an empty string is the only acceptable input.
    pub fn parse_lon_str(lon_str: &str) -> anyhow::Result<Option<Vec<Option<f32>>>> {
        return Self::parse_latlon_str(lon_str, 180.0, "Longitudes");
    }

    /// Inner helper function for [`Job::parse_lat_str`] and [`Job::parse_lon_str`]
    /// 
    /// # Parameters
    /// * `coord_str` - the comma-separated list of coordinates
    /// * `limit` - the absolute (positive) limit for the float values; values outside `\[-limit, limit\]` return an `Err`
    /// * `varname` - "Latitude" or "Longitude", to use in the error message.
    fn parse_latlon_str(coord_str: &str, limit: f32, varname: &str) -> anyhow::Result<Option<Vec<Option<f32>>>> {
        if coord_str.len() == 0 {
            return Ok(None)
        }

        let mut values = vec![];
        for s in coord_str.split(',') {
            let v = s.parse()?;
            if v < -limit || v > limit {
                anyhow::bail!("{varname} must be between -{limit:.1} and +{limit:.1}")
            }
            values.push(Some(v))
        }

        return Ok(Some(values));
    }

    /// Convert vectors of site IDs, latitudes, and longitudes to equal lengths.
    /// 
    /// The Jobs SQL table requires that the input vectors of site IDs, latitudes, and longitudes
    /// be of equal length. This function expands user-inputted vectors of these values to be
    /// equal length according to the following rules:
    /// 
    ///  1. Lat and lon must either both or neither be given
    ///  2. If lat/lon not given, they default to vectors of `None`s the same length as site IDs;
    ///     this means that we will infer their lat/lon from the site ID. 
    ///  3. If lat/lon are given, then the site ID vector must be length 1 *or* the same length as the lat/lons
    /// 
    /// # Returns
    /// * The vector of site IDs
    /// * The vector of optional latitudes
    /// * The vector of optional longitudes
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * one of `lat` and `lon` is `None`, but not both
    /// * the lengths of `lat` and `lon` are not equal
    /// * `site_id` is not length 1 or the same length as `lat` and `lon`
    pub fn expand_site_lat_lon(site_id: Vec<String>, lat: Option<Vec<Option<f32>>>, lon: Option<Vec<Option<f32>>>) 
    -> anyhow::Result<(Vec<String>, Vec<Option<f32>>, Vec<Option<f32>>)> {
        // Rules:
        // 

        let lat_given = lat.is_some();
        let lon_given = lon.is_some();

        if lat_given != lon_given {
            anyhow::bail!("lat and lon must both be given or not, cannot have one given and not the other")
        }

        if !lat_given && !lon_given {
            let lat = vec![None; site_id.len()];
            let lon = vec![None; site_id.len()];
            return Ok((site_id, lat, lon))
        }

        let lat = lat.unwrap();
        let lon = lon.unwrap();

        if lat.len() != lon.len() {
            anyhow::bail!("If given, lat and lon must have the same number of elements.")
        }

        if site_id.len() == lat.len() {
            return Ok((site_id, lat, lon))
        }

        if site_id.len() == 1 {
            let site_id = vec![site_id[0].clone(); lat.len()];
            return Ok((site_id, lat, lon))
        }

        anyhow::bail!("site_id must have length 1 or the same number of elements as lat & lon (got {} site ID, {} lat/lon)", 
                      site_id.len(), lat.len());
    }

    pub async fn lats_lons_sids_filled_for_date(&self, conn: &mut MySqlConn, date: NaiveDate) -> anyhow::Result<(Vec<String>, Vec<f32>, Vec<f32>)> {
        let (site_ids, lats, lons) = Self::expand_site_lat_lon(
                self.site_id.clone(), 
                Some(self.lat.clone()), 
                Some(self.lon.clone())
            )?;

        let (lats, lons) = crate::siteinfo::SiteInfo::fill_null_latlons(
            conn, 
            &site_ids, 
            &lats, 
            &lons, 
            date, 
            Some(date + chrono::Duration::days(1))
        ).await?;

        Ok((site_ids, lats, lons))
    }

    /// Add a new job to the database
    /// 
    /// # Parameters
    /// * `conn` - a connection to the MySQL database with the Jobs table
    /// * `site_id` - the vector of site IDs to generate priors for
    /// * `start_date` - first date to generate priors for
    /// * `end_date` - Date after the last one to generate priors for, i.e. an exclusive end date
    /// * `save_dir` - location to save the output
    /// * `email` - optional email to contact when the job is complete or errors
    /// * `lat`, `lon` - vectors of latitude and longitude to generate priors at. May contain `None`s
    ///   if the corresponding site ID is a known standard site with a defined lat/lon. These and
    ///   `site_id` must be vectors of equal length, use [`Job::expand_site_lat_lon`] to expand them
    ///   before passing them in if needed.
    /// * `mod_fmt`, `vmr_fmt`, `map_fmt` - the output formats of the `.mod`, `.vmr`, and `.map` files.
    ///   If `None`, then the default format (usually text) is used.
    /// * `priority` - job priority (greater = higher). If `None`, then a default of 0 is used.
    /// * `delete_time` - date & time after which to delete the output from this job. If `None`, the output
    ///   will never be deleted.
    /// * `save_tarball` - whether to save the output as a tarball or a directory. If `None`, the default
    ///   of [`TarChoice`] is used.
    /// 
    /// # Returns
    /// If successful, returns the `job_id` of the new Job.
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * `site_id`, `lat`, and `lon` are not all the same length.
    /// * a lat/lon pair is `None` in one vector but not both.
    /// * a lat/lon pair is `None` but no standard site information is available
    ///   for the corresponding site ID or access to the standard site information table
    ///   fails.
    /// * serializing the `site_id`, `lat`, or `lon` vectors to the SQL table fails
    /// * converting the `save_dir` path to UTF-8 fails
    /// * the INSERT query fails (e.g. if any constraints are violated)
    pub async fn add_job_from_args(
        conn: &mut MySqlConn,
        site_id: Vec<String>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        save_dir: PathBuf,
        email: Option<String>,
        lat: Vec<Option<f32>>,
        lon: Vec<Option<f32>>,
        queue: &str,
        mod_fmt: Option<ModFmt>,
        vmr_fmt: Option<VmrFmt>,
        map_fmt: Option<MapFmt>,
        priority: Option<i32>,
        delete_time: Option<NaiveDateTime>,
        save_tarball: Option<TarChoice>
    ) -> Result<i32, JobAddError> {

        // Verify that we have matching site_id, lat, lon vectors. Any expansion needs to be done outside of this function.
        if site_id.len() != lat.len() || site_id.len() != lon.len() {
            return Err(JobAddError::DifferentNumSidLatLon { n_sid: site_id.len(), n_lat: lat.len(), n_lon: lon.len() });
        }

        // Originally, I thought we needed to confirm that the job's date range does not cover multiple locations 
        // for any requested site, since for the Python interface it must fill in any lat/lons once for the whole job.
        // This should not be the case now: I had to break the jobs down into one-day sub-jobs anyway to deal
        // with changing default met and ginput versions, and in doing so also allow the default lat/lons to change
        // from day to day.
        // TODO: should run a test on this, but not critical - jobs coming in as requests should not be for standard
        // sites.

        // Also verify that any site_ids for which we do not have defined lat/lons in the inputs are
        // standard sites with at least one time period defined. At the same time, check that we don't 
        // have any lat/lon pairs where only one is None.
        let mut unknown_sids = vec![];
        for (sid, x, y) in itertools::izip!(site_id.iter(), lat.iter(), lon.iter()) {
            if x.is_none() != y.is_none() {
                return Err(JobAddError::HalfNullCoord);
            }

            if x.is_none() {
                if !siteinfo::SiteInfo::verify_info_available_for_site(conn, sid).await? {
                    unknown_sids.push(&sid[..]);
                }
            }
        }

        if unknown_sids.len() > 0 {
            let unknown_ids = unknown_sids.into_iter().map(|sid| sid.to_string()).collect();
            return Err(JobAddError::UnknownStdSid(unknown_ids));
        }


        let now = chrono::Local::now().naive_local();
        let mod_fmt: String = mod_fmt.unwrap_or_default().to_string();
        let vmr_fmt: String = vmr_fmt.unwrap_or_default().to_string();
        let map_fmt: String = map_fmt.unwrap_or_default().to_string();
        let save_tarball: i8 = save_tarball.unwrap_or_default().into();
        let complete_time: Option<NaiveDateTime> = None;
        let output_file: Option<String> = None;

        let new_id = sqlx::query!(
            r#"INSERT INTO Jobs (state, site_id, start_date, end_date, lat, lon, email, queue, delete_time, priority, save_dir, save_tarball, mod_fmt, vmr_fmt, map_fmt, submit_time, complete_time, output_file)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            JobState::Pending as i8, // state
            serde_json::to_string(&site_id)?, // site_id
            start_date, // start_date
            end_date, // end_date
            serde_json::to_string(&lat)?, // lat
            serde_json::to_string(&lon)?, // lon
            email, // email
            queue,
            delete_time, // delete_time
            priority.unwrap_or(0), // priority
            save_dir.to_str().ok_or(JobAddError::InvalidUtf("save_dir"))?, // save_dir
            save_tarball, // save_tarball
            mod_fmt,
            vmr_fmt,
            map_fmt,
            now, // submit_time
            complete_time, // complete_time
            output_file, // output_file
        ).execute(conn)
        .await?
        .last_insert_id();

        Ok(new_id as i32)
    }

    /// Add a job with extra options, such as the met key and ginput key.
    /// 
    /// Note that this DOES NOT check that the values of met_key and ginput_key are valid.
    pub async fn add_job_from_args_with_options(
        conn: &mut MySqlConn,
        site_id: Vec<String>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        save_dir: PathBuf,
        email: Option<String>,
        lat: Vec<Option<f32>>,
        lon: Vec<Option<f32>>,
        queue: &str,
        mod_fmt: Option<ModFmt>,
        vmr_fmt: Option<VmrFmt>,
        map_fmt: Option<MapFmt>,
        priority: Option<i32>,
        delete_time: Option<NaiveDateTime>,
        save_tarball: Option<TarChoice>,
        met_key: Option<&str>,
        ginput_key: Option<&str>
    ) -> Result<i32, JobAddError> {
        let mut transaction = conn.begin().await?;
        let job_id = Self::add_job_from_args(&mut transaction, site_id, start_date, end_date, save_dir, email, lat, lon, queue, mod_fmt, vmr_fmt, map_fmt, priority, delete_time, save_tarball).await?;

        if let Some(met) = met_key {
            sqlx::query!("UPDATE Jobs SET met_key = ? WHERE job_id = ?", met, job_id)
                .execute(&mut *transaction)
                .await?;
        }

        if let Some(ginput) = ginput_key {
            sqlx::query!("UPDATE Jobs SET ginput_key = ? WHERE job_id = ?", ginput, job_id)
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;
        Ok(job_id)
    }

    /// For any job whose delete_time is in the past, remove its output and set the state to "Cleaned"
    /// 
    /// Note that if an error occurs while cleaning up one job, any following jobs will not be cleaned
    /// up. However, rerunning this function (after the root cause of the error has been fixed) should
    /// pick up where it left off.
    pub async fn clean_up_expired_jobs(conn: &mut MySqlConn, dry_run: bool) -> anyhow::Result<()> {
        let jobs: Vec<_> = sqlx::query_as!(
            QJob,
            "SELECT * FROM Jobs WHERE delete_time IS NOT NULL AND delete_time <= ? AND state != ?",
            chrono::Local::now().naive_local(),
            JobState::Cleaned
        ).fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|q| Job::try_from(q))
        .try_collect()?;

        info!("{} jobs output expired, starting clean up", jobs.len());

        for mut job in jobs {
            if dry_run {
                println!("Would clean up job {}", job.job_id);
            } else {
                job.set_cleaned(conn).await?;
                debug!("Job {} cleaned up", job.job_id);
            }
        }

        Ok(())
    }

    /// Delete a job from the queue
    /// 
    /// # Parameters
    /// `conn` - a connection to the MySQL database.
    /// `id` - the numeric ID (primary key) of the job to delete.
    /// 
    /// # Returns
    /// If no errors encountered, returns the number of jobs deleted (0 or 1).
    /// 
    /// # Errors
    /// Returns an `Err` if:
    /// 
    /// * querying for the number of jobs in the table fails
    /// * deleting the row in the SQL table fails
    pub async fn delete_job_with_id(conn: &mut MySqlConn, id: i32) -> anyhow::Result<u64> {
        let job = Self::get_job_with_id(conn, id).await?;
        job.delete(conn).await
    }

    /// Reset a job, deleting any output and changing its status back to "pending"
    pub async fn reset_job_with_id(conn: &mut MySqlConn, id: i32) -> anyhow::Result<()> {
        let mut job = Self::get_job_with_id(conn, id).await?;
        job.reset(conn).await
    }

    /// Set the state of a job by its `job_id`
    /// 
    /// Note that the method [`Job::set_completed_by_id`] is preferred when
    /// setting the state to [`JobState::Complete`], as that method ensures
    /// that the completion time and output file are updated correctly as well.
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `job_id` - the ID of the job to update
    /// * `state` - the state to set the job to.
    /// 
    /// # Returns
    /// Will return the number of rows in the `Jobs` table that were updated. This can be
    /// useful to check if the given `job_id` did match a job. 
    /// 
    /// # Errors
    /// Returns an `Err` if the SQL query to update the state failed.
    pub async fn set_state_by_id(conn: &mut MySqlConn, job_id: i32, state: JobState) -> JobResult<u64> {
        let nrows = sqlx::query!(
            "UPDATE Jobs SET state = ? WHERE job_id = ?",
            state,
            job_id
        ).execute(conn)
        .await?
        .rows_affected();

        Ok(nrows)
    }

    /// Update the state for this job instance.
    /// 
    /// Calls [`Job::set_state_by_id`] to update the database, then updates the state of this instance.
    /// See [`Job::set_state_by_id`] for information on parameters.
    /// 
    /// Note that [`Job::set_completed`] is preferred when
    /// setting the state to [`JobState::Complete`], as that method ensures
    /// that the completion time and output file are updated correctly as well.
    pub async fn set_state(&mut self, conn: &mut MySqlConn, state: JobState) -> JobResult<u64> {
        let n = Self::set_state_by_id(conn, self.job_id, state).await?;
        self.state = state;
        return Ok(n)
    }

    /// Set a job's state to [`JobState::Complete`] and update the output file and completion time.
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `job_id` - the ID of the job to update
    /// * `output_path` - location of the output data. This should be the tarball file if one was made,
    ///   or the directory created if the output was not compressed into a tarball.
    /// * `complete_time` - time when the job finished. If `None`, then the current local time will be
    ///   used, but you may pass your own time if it must be in a different time zone or there is a
    ///   lag between the actual completion of the job and calling this method.
    pub async fn set_completed_by_id(conn: &mut MySqlConn, job_id: i32, output_path: &Path, complete_time: Option<NaiveDateTime>) -> anyhow::Result<(u64, NaiveDateTime)> {

        let complete_time = if let Some(time) = complete_time {
            time
        }else{
            chrono::Local::now().naive_local()
        };

        let nrows = sqlx::query!(
            "UPDATE Jobs SET state = ?, output_file = ?, complete_time = ? WHERE job_id = ?",
            JobState::Complete,
            output_path.to_str().ok_or(anyhow::anyhow!("Could not convert output_file to UTF string"))?,
            complete_time,
            job_id
        ).execute(conn)
        .await?
        .rows_affected();

        return Ok((nrows, complete_time))
    }

    /// Update the state, output file, and completion time for this job instance.
    /// 
    /// Calls [`Job::set_completed_by_id`] to update the database, then sets the
    /// state, output file, and completion time on this instance to the updated
    /// values. See [`Job::set_completed_by_id`] for parameter information.
    pub async fn set_completed(&mut self, conn: &mut MySqlConn, output_path: &Path, complete_time: Option<NaiveDateTime>) -> anyhow::Result<(u64, NaiveDateTime)> {
        let (n, t) = Self::set_completed_by_id(conn, self.job_id, output_path, complete_time).await?;
        self.state = JobState::Complete;
        self.output_file = Some(output_path.to_owned());
        self.complete_time = Some(t);
        return Ok((n, t))
    }

    /// Remove output from this job and set its state to "Cleaned"
    pub async fn set_cleaned(&mut self, conn: &mut MySqlConn) -> anyhow::Result<()> {
        self.delete_output_and_run_dir()?;
        self.set_state(conn, JobState::Cleaned).await?;
        self.state = JobState::Cleaned;
        Ok(())
    }

    pub async fn set_errored<E: Debug>(&mut self, conn: &mut MySqlConn, err: &E, email_config: Option<&EmailConfig>) -> anyhow::Result<()> {
        self.set_state(conn, JobState::Errored).await?;
        if let Some(config) = email_config {
            send_job_error_email(self, err, config)?;
        }
        Ok(())
    }

    pub async fn set_priority_by_id(job_id: i32, conn: &mut MySqlConn, new_priority: i32, allow_any_state: bool) -> Result<(), JobPriorityError> {
        let mut job = Job::get_job_with_id(conn, job_id).await
            .with_context(|| format!("Error occurred while trying to retrieve job #{job_id} to set its priority"))?;
        job.set_priority(conn, new_priority, allow_any_state).await
    }

    pub async fn set_priority(&mut self, conn: &mut MySqlConn, new_priority: i32, allow_any_state: bool) -> Result<(), JobPriorityError> {
        let can_set = match self.state {
            JobState::Pending => true,
            JobState::Running => allow_any_state,
            JobState::Complete => allow_any_state,
            JobState::Errored => allow_any_state,
            JobState::Cleaned => allow_any_state,
        };

        if !can_set {
            return Err(JobPriorityError::StateNotPending)
        }

        sqlx::query!(
            "UPDATE Jobs SET priority = ? WHERE job_id = ?",
            new_priority,
            self.job_id
        ).execute(conn)
        .await
        .context("Error occurred while setting job priority")?;

        self.priority = new_priority;

        Ok(())
    }

    pub async fn set_delete_time_by_id(job_id: i32, conn: &mut MySqlConn, deletion_time: Option<NaiveDateTime>) -> JobResult<()> {
        sqlx::query!(
            "UPDATE Jobs SET delete_time = ? WHERE job_id = ?",
            deletion_time,
            job_id
        ).execute(conn)
        .await
        .map_err(|e| JobError::QueryError(e))?;

        Ok(())
    }


    /// Reset this job to pending, deleting the run directory or output directory/file as well
    pub async fn reset(&mut self, conn: &mut MySqlConn) -> anyhow::Result<()> {
        self.delete_output_and_run_dir()
            .unwrap_or_else(|e| warn!("Failed to clean up output files, error was: {e}"));

        sqlx::query!(
            "UPDATE Jobs SET state = ?, output_file = ?, complete_time = ? WHERE job_id = ?",
            JobState::Pending,
            None::<String>,
            None::<NaiveDateTime>,
            self.job_id
        ).execute(conn)
        .await?;

        self.state = JobState::Pending;
        self.output_file = None;
        self.complete_time = None;

        Ok(())
    }

    pub fn delete_output_and_run_dir(&self) -> anyhow::Result<()> {
        if let Some(output) = &self.output_file {
            debug!("Cleaning out output {} for job {}", output.display(), self.job_id);
            if output.is_dir() {
                std::fs::remove_dir_all(output)
                .with_context(|| format!(
                    "Error occurred while trying to remove output directory ({}) for job {}.",
                    output.display(), self.job_id
                ))?;
            }

            if output.is_file() {
                std::fs::remove_file(output)
                .with_context(|| format!(
                    "Error occurred while trying to remove output file ({}) for job {}.",
                    output.display(), self.job_id
                ))?;
            }
        } else {
            debug!("No output to clean up for job {}", self.job_id);
        }

        let run_dir = self.run_dir(false);
        if run_dir.exists() {
            debug!("Cleaning up run directory {} for job {}", run_dir.display(), self.job_id);
            std::fs::remove_dir_all(&run_dir)
            .with_context(|| format!(
                "Error occurred while trying to remove run directory ({}) for job {}.",
                run_dir.display(), self.job_id
            ))?;
        } else {
            debug!("Not cleaning up run directory {} for job {}, does not exist", run_dir.display(), self.job_id);
        }

        Ok(())
    }

    /// Delete this job, removing output/run files and deleting the job itself from the Jobs table
    pub async fn delete(self, conn: &mut MySqlConn) -> anyhow::Result<u64> {
        // Only error is if setting the state fails, and since we're deleting the job, that doesn't matter
        self.delete_output_and_run_dir()?;
        let res = sqlx::query!("DELETE FROM Jobs WHERE job_id = ?", self.job_id)
            .execute(conn)
            .await?;
        Ok(res.rows_affected())
    }

}


pub struct JobSummary(Vec<(String, u64)>);

impl JobSummary {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_table(&self) -> String {
        let mut builder = tabled::Table::builder(self.0.iter());
        builder.set_header(["Submitted by", "# jobs"]);
        let tab = builder.build();
        utils::table_to_std_string(tab)
    }

    pub fn total_num_jobs(&self) -> u64 {
        self.0.iter().map(|x| x.1).sum()
    }
}

#[async_trait]
pub trait FairShare {
    async fn next_job_in_queue(&self, conn: &mut MySqlConn, queue: &str) -> JobResult<Option<Job>>;
    async fn order_jobs_for_display(&self, conn: &mut MySqlConn, queue: &str, mut jobs: Vec<Job>) -> JobResult<Vec<(Job, HashMap<&'static str, String>)>>;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PrioritySubmitFS {}

#[async_trait]
impl FairShare for PrioritySubmitFS {
    async fn next_job_in_queue(&self, conn: &mut MySqlConn, queue: &str) -> JobResult<Option<Job>> {
        let job: Option<Job>  = sqlx::query_as!(
            QJob,
            "SELECT * FROM Jobs WHERE state = ? AND queue = ? ORDER BY priority desc, submit_time LIMIT 1",
            JobState::Pending,
            queue
        ).fetch_optional(conn)
        .await?
        .map(|qjob| qjob.try_into())
        .transpose()?;

        Ok(job)
    }

    async fn order_jobs_for_display(&self, _conn: &mut MySqlConn, _queue: &str, mut jobs: Vec<Job>) -> JobResult<Vec<(Job, HashMap<&'static str, String>)>> {
        jobs.sort_by_key(|j| (-j.priority, j.submit_time));
        let out: Vec<(Job, HashMap<&'static str, String>)> = jobs.into_iter()
            .map(|j| (j, HashMap::new()))
            .collect();
        Ok(out)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PsuedoRoundRobinFS {
    time_period_days: u32
}

impl PsuedoRoundRobinFS {
    pub fn new(time_period_days: u32) -> Self {
        Self { time_period_days }
    }

    fn cutoff_time(&self) -> NaiveDateTime {
        chrono::Local::now().naive_local() - chrono::TimeDelta::days(self.time_period_days as i64)
    }
}

impl Default for PsuedoRoundRobinFS {
    fn default() -> Self {
        Self { time_period_days: 14 }
    }
}

#[async_trait]
impl FairShare for PsuedoRoundRobinFS {
    async fn next_job_in_queue(&self, conn: &mut MySqlConn, queue: &str) -> JobResult<Option<Job>> {
        // This will count all running, completed, or cleaned jobs submitted in the last N days for each
        // unique email and join those counts with all jobs waiting to start. It will then order the pending
        // jobs first by priority less the number of jobs from the last N days, then by submission time.
        // Note that the COALESCE in the ORDER BY statement is necessary so that any users with no recent jobs
        // correctly get 0 penalty - otherwise priority + NULL seems to produce NULL, which is ordered last!
        let job: Option<Job> = sqlx::query_as!(
            QJob,
            r#"
            SELECT j.* FROM (
                SELECT * FROM Jobs WHERE state = ? AND queue = ?
            ) as j
            LEFT JOIN (
                SELECT COUNT(*) as fs_penalty,email as fs_email
                FROM Jobs
                WHERE (state = ? OR state = ? OR state = ?) AND queue = ? AND submit_time > ?
                GROUP BY email
            ) as fs
            ON j.email = fs.fs_email
            ORDER BY (priority - COALESCE(fs_penalty, 0)) desc,submit_time
            LIMIT 1
            "#,
            JobState::Pending, // state in first subquery
            queue, // queue in first subquery
            JobState::Running, // state 1/3 in second subquery
            JobState::Complete, // state 2/3 in second subquery
            JobState::Cleaned, // state 3/3 in second subquery
            queue, // queue in second subquery
            self.cutoff_time() // submit_time comparison in second subquery
        ).fetch_optional(conn)
        .await?
        .map(|q| q.try_into())
        .transpose()?;
        
        Ok(job)
    }

    async fn order_jobs_for_display(&self, conn: &mut MySqlConn, queue: &str, mut jobs: Vec<Job>) -> JobResult<Vec<(Job, HashMap<&'static str, String>)>> {
        let penalties = sqlx::query_as!(
            QPseudoRRPenalty,
            r#"
            SELECT COUNT(*) AS fs_penalty,email FROM Jobs
            WHERE (state = ? OR state = ? OR state = ?) AND queue = ? AND submit_time > ?
            GROUP BY email
            "#,
            JobState::Running,
            JobState::Complete,
            JobState::Cleaned,
            queue,
            self.cutoff_time()
        ).fetch_all(conn)
        .await?;

        let penalites: HashMap<String, i64, RandomState> = HashMap::from_iter(
            penalties.into_iter()
            .filter_map(|p| {
                if let Some(e) = p.email {
                    Some((e, p.fs_penalty))
                } else {
                    None
                }
            })
        );

        jobs.sort_by_cached_key(|j| {
            let fs_pen = if let Some(ref e) = j.email {
                penalites.get(e).map(|i| *i as i32).unwrap_or(0)
            } else {
                0
            };

            (-(j.priority - fs_pen), j.submit_time)
        });

        let out: Vec<(Job, HashMap<&'static str, String>)> = jobs.into_iter()
            .map(|j| {
                let fs_pen = if let Some(ref e) = j.email {
                    penalites.get(e).map(|i| -*i).unwrap_or(0)
                } else {
                    0
                };

                let fs_pen_str = match j.state {
                    JobState::Cleaned | JobState::Errored | JobState::Complete | JobState::Running => "n/a".to_string(),
                    JobState::Pending => fs_pen.to_string()
                };

                let extra_fields = HashMap::from_iter([
                    ("FS adjustment", fs_pen_str)
                ]);

                (j, extra_fields)
            }).collect();

        Ok(out)
    }
}

struct QPseudoRRPenalty {
    email: Option<String>,
    fs_penalty: i64
}

pub async fn cleanup_cancelled_ginput_job(conn: &mut MySqlConn, job: &mut Job) -> JobResult<()> {
    std::fs::remove_dir_all(&job.run_dir(false))
        .unwrap_or_else(|e| warn!(
            "Error occurred while trying to remove run directory ({}) for job {}. Error was: {e}",
            job.run_dir(false).display(), job.job_id
        ));

    job.set_state(conn, JobState::Pending).await?;
    Ok(())
}

#[async_trait]
trait InnerGinputRunner: Display + Send + Sync {
    async fn run_gen_priors_for_date(&self, ginput_args: GinputAutomationArgs, simulation_delay: Option<u32>) -> JobResult<()>;
    async fn run_lut_regen(&self) -> JobResult<()>;
}

pub struct ShellGinputRunner {
    run_ginput_path: PathBuf
}

impl ShellGinputRunner {
    pub fn new(run_ginput_path: PathBuf) -> Self {
        Self { run_ginput_path }
    }
}

impl Display for ShellGinputRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShellGinputRunner({})", self.run_ginput_path.display())
    }
}

#[async_trait]
impl InnerGinputRunner for ShellGinputRunner {
    async fn run_gen_priors_for_date(&self, ginput_args: GinputAutomationArgs, simulation_delay: Option<u32>) -> JobResult<()> {
        let args_file = run_arg_file(&ginput_args.save_path, ginput_args.start_date);
        let args_file_h = std::fs::File::create(&args_file)
            .map_err(|e| JobError::RunDirectoryError(e))?;
        serde_json::to_writer_pretty(args_file_h, &ginput_args)?;
    
        let log_file = std::fs::File::create(
            ginput_args.save_path.join(format!("ginput_job_{}_{}.out", ginput_args.job_id, ginput_args.start_date))
        ).map_err(|e| JobError::RunDirectoryError(e))?;
        let log_stdout = Stdio::from(log_file);
    
        let log_file = std::fs::File::create(
            ginput_args.save_path.join(format!("ginput_job_{}_{}.err", ginput_args.job_id, ginput_args.start_date))
        ).map_err(|e| JobError::RunDirectoryError(e))?;
        let log_stderr = Stdio::from(log_file);

        let mut cmd = tokio::process::Command::new(&self.run_ginput_path);
        cmd.arg("auto")
            .arg("run")
            .arg(args_file)
            .stdout(log_stdout)
            .stderr(log_stderr);

        if let Some(delay) = simulation_delay {
            cmd.args(["--simulate-with-delay".to_string(), format!("{delay}")]);
        }

        let status = cmd
            .status()
            .await
            .map_err(|e| JobError::RunDirectoryError(e))?;

        if status.success() {
            Ok(())
        } else if let Some(code) = status.code() {
            Err(JobError::GinputFailureError(code))
        } else {
            Err(JobError::WasCancelled)
        }
    }

    async fn run_lut_regen(&self) -> JobResult<()> {
        // Set up output files for the job: ideally put the logs in the same directory as the
        // LUT tables, falling back to the ginput root directory. If we can't even get that, fall
        // back to the current directory.
        let ginput_dir = if let Some(parent) = self.run_ginput_path.parent() {
            parent.to_path_buf()
        } else {
            warn!("Could not determine ginput directory from the ginput run path, outputting LUT logs to current directory");
            PathBuf::from(".")
        };

        let ginput_data_dir = ginput_dir.join("ginput").join("data");

        let lut_log_dir = if ginput_data_dir.exists() {
            ginput_data_dir
        } else {
            warn!("Unexpected ginput directory structure (missing ginput/data subdirectory), writing LUT logs to {}", ginput_dir.display());
            ginput_dir
        };

        let log_file = std::fs::File::create(
            lut_log_dir.join("automation_lut_regen.out")
        ).map_err(|e| JobError::RunDirectoryError(e))?;
        let log_stdout = Stdio::from(log_file);
    
        let log_file = std::fs::File::create(
            lut_log_dir.join("automation_lut_regen.err")
        ).map_err(|e| JobError::RunDirectoryError(e))?;
        let log_stderr = Stdio::from(log_file);

        // Finally we can run the job

        let status = tokio::process::Command::new(&self.run_ginput_path)
            .arg("auto")
            .arg("regen-lut")
            .stdout(log_stdout)
            .stderr(log_stderr)
            .status()
            .await
            .map_err(|e| JobError::RunDirectoryError(e))?;

        if status.success() {
            Ok(())
        } else if let Some(code) = status.code() {
            Err(JobError::GinputFailureError(code))
        } else {
            Err(JobError::WasCancelled)
        }
    }
}

pub fn run_arg_file(save_path: &Path, start_date: NaiveDate) -> PathBuf {
    save_path.join(format!("ginput_run_args_{}.json", start_date))
}

#[derive(Debug, Serialize)]
struct GinputAutomationArgs {
    job_id: i32,
    ginput_met_key: String, 

    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    met_path: PathBuf,
    chem_path: PathBuf,
    save_path: PathBuf,
    site_ids: Vec<String>,
    site_lons: Vec<f32>,
    site_lats: Vec<f32>,
    site_alts: Vec<f32>,

    base_vmr_file: Option<PathBuf>,
    zgrid_file: Option<PathBuf>,

    map_file_format: String,

    n_threads: u32
}

pub type GinputHandle = JoinHandle<anyhow::Result<()>>;

pub fn start_priors_gen_job(pool: PoolWrapper, job: Job, config: Config) -> GinputHandle {
    tokio::spawn(async move {
        run_priors_gen_job(pool, job, config).await
    })
}

async fn run_priors_gen_job(pool: PoolWrapper, mut job: Job, config: Config) -> anyhow::Result<()> {
    // This function must take an SQL pool rather than a connection because holding the
    // connection opening while long jobs run seems to cause a "connection reset by peer"
    // issue.
    info!("Beginning job {} for dates {} to {}", job.job_id, job.start_date, job.end_date);
    let date_iter = crate::utils::DateIterator::new(
        vec![(job.start_date, job.end_date)]
    );

    for date in date_iter {
        // Use this connection just to get the arguments, running the priors for many
        // dates takes long enough for the connection to reset.
        let res = {
            let mut conn = pool.get_connection().await?;
            setup_ginput_args_for_date(&mut conn, date, &job, &config).await
        };
        let res = res.with_context(|| format!("Error occurred setting up arguments to run date {date} in job {}", job.job_id));
        let ginput_args = res?;
        let runner = get_runner_for_date(date, &job, &config)
            .with_context(|| format!("Error occurred setting up inner runner for date {date} in job {}", job.job_id))?;
        info!("Running {date} for job {} using {runner}", job.job_id);
        runner.run_gen_priors_for_date(ginput_args, config.get_sim_delay())
            .await
            .with_context(|| format!("Error occurred while running ginput for date {date} in job {}", job.job_id))?;
    }

    // Now we should be okay to get a connection from the pool for the rest of the function
    // since the rest of the function should be quick enough.
    let mut conn = pool.get_connection().await?;
    let (make_tarball, output_path) = match job.save_tarball {
        TarChoice::No => {
            let root_save_dir = &job.root_save_dir;
            let dir_basename = job.run_dir(true);
            (false, root_save_dir.join(dir_basename))
        },
        TarChoice::Yes => {
            (true, make_std_tar_file_name(&mut conn, &job).await?)
        },
        TarChoice::Egi => {
            (true, make_egi_tar_file_name(&mut conn, &job).await?)
        },
    };

    if !make_tarball {
        // NB: the run directory MUST be on the same file system for this to work, which is why run_dir()
        // is defined as a subdirectory of root_save_dir
        std::fs::rename(job.run_dir(false), &output_path)
            .with_context(|| format!("Failed to move run directory for job {} to its download location", job.job_id))?;
    } else {

        let job_dir = job.run_dir(false);
        if !job_dir.exists() {
            std::fs::create_dir_all(&job_dir)?;
        }
        let job_dir_name = job.run_dir(true);

        // Clever combination of tar archive builder and gzip compressor taken from
        // https://stackoverflow.com/a/46521163
        let tgz_file = std::fs::File::create(&output_path)
            .with_context(|| format!("Error occurred trying to create the initial .tgz file for job {}", job.job_id))?;
        let encoder = flate2::write::GzEncoder::new(tgz_file, flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);

        // ginput will output the .mod/.vmr/.map files into subdirectories, the only files directly in the
        // top directory will be the output logs and the args JSON. By appending only top level subdirectories,
        // we ensure that even if the met name in the top subdirectory changes, we get all the output files.
        for entry in std::fs::read_dir(&job_dir)
            .with_context(|| format!("Error occurred while selecting output files for tarball in job {}", job.job_id))? 
        {
            let entry = entry.with_context(|| format!("Error getting directory entry while making tarball for job {}", job.job_id))?;
            let src_path = entry.path();
            if src_path.is_dir() {
                let dest_path = src_path.file_name()
                    .with_context(|| format!("Error getting source directory file name from {} for job {}", src_path.display(), job.job_id))?;
                let dest_path = job_dir_name.join(dest_path);
                archive.append_dir_all(dest_path, &src_path)
                    .with_context(|| format!("Error adding directory {} to tarball for job {}", src_path.display(), job.job_id))?;
            }
        }

        let encoder = archive.into_inner()
            .with_context(|| format!("Error occurred while trying to finalize tar archive for job {}", job.job_id))?;

        // Unsure if this is needed, but doesn't seem to hurt
        encoder.finish()
            .with_context(|| format!("Error occurred while trying to finalize the gzip compression for job {}", job.job_id))?;

        std::fs::remove_dir_all(&job_dir)
            .unwrap_or_else(|_| warn!("Failed to remove output directory for job {} after creating the tarball", job.job_id));
    }

    job.set_completed(&mut conn, &output_path, None).await?;
    send_job_completion_email(&job, &config)?;
    Ok(())
}

pub fn start_lut_regen_job(ginput_key: String, config: Config) -> GinputHandle {
    tokio::spawn(async move {
        run_lut_regen_job(ginput_key, config).await
    })
}

async fn run_lut_regen_job(ginput_key: String, config: Config) -> anyhow::Result<()> {
    let ginput = config.execution.ginput.get(&ginput_key)
        .ok_or_else(|| anyhow::anyhow!("Ginput key '{ginput_key}', passed to regenerate LUTs, is not defined in the configuration"))?;
    let runner = get_runner_for_ginput(ginput);
    runner.run_lut_regen().await?;
    Ok(())
}


fn get_runner_for_date(date: NaiveDate, job: &Job, config: &Config) -> anyhow::Result<Box<dyn InnerGinputRunner>> {
    let ginput_key = if let Some(key) = &job.ginput_key {
        key
    } else {
        let defaults = config.get_defaults_for_date(date)
            .with_context(|| format!("Could not get defaults for date {date}; occurred while trying to start ginput for job {}", job.job_id))?;
        &defaults.ginput
    };

    let ginput = config.execution.ginput.get(ginput_key)
        .ok_or_else(|| anyhow::anyhow!("Ginput key '{ginput_key}', required by job #{}, is not defined in the configuration", job.job_id))?;

    Ok(get_runner_for_ginput(ginput))
}

fn get_runner_for_ginput(ginput: &crate::config::GinputConfig) -> Box<dyn InnerGinputRunner> {
    match ginput {
        crate::config::GinputConfig::Script { entry_point_path } => {
            Box::new(ShellGinputRunner{ run_ginput_path: entry_point_path.to_owned() })
        },
    }
}

async fn setup_ginput_args_for_date(conn: &mut MySqlConn, date: NaiveDate, job: &Job, config: &Config) -> anyhow::Result<GinputAutomationArgs> {
    debug!("Job {}: Getting met key for job", job.job_id);
    let met_key = if let Some(k) = &job.met_key {
        k.to_owned()
    } else {
        let defaults = config.get_defaults_for_date(date)
            .map_err(|e| JobError::ConfigurationError(e.into()))?;
        defaults.met.clone()
    };

    debug!("Job {}: Getting met and chem paths for met key '{met_key}'", job.job_id);
    let (met_path, chem_path, ginput_met_key) = config.get_ginput_met_args(&met_key)
        .map_err(|e| JobError::ConfigurationError(e))?;

    debug!("Job {}: Standardize site IDs, lats, and lons", job.job_id);
    let (site_ids, lats, lons) = job.lats_lons_sids_filled_for_date(conn, date)
        .await
        .map_err(|e| JobError::InvalidSiteLocation(e))?;

    debug!("Job {}: setting up run directory", job.job_id);
    let run_dir = job.run_dir(false);
    if !run_dir.exists() {
        std::fs::create_dir_all(&run_dir)?;
    }

    let nlocs = lons.len();
    
    debug!("Job {}: creating automation arguments", job.job_id);
    let ginput_args = GinputAutomationArgs {
        job_id: job.job_id,
        ginput_met_key,
        start_date: date,
        end_date: None,
        met_path: met_path,
        chem_path: chem_path,
        save_path: run_dir,
        site_ids: site_ids,
        site_lats: lats,
        site_lons: lons,
        site_alts: vec![0.0; nlocs],
        base_vmr_file: config.data.base_vmr_file.to_owned(),
        zgrid_file: config.data.zgrid_file.to_owned(),
        map_file_format: job.map_fmt.to_string(),
        n_threads: config.execution.max_numpy_threads
    };

    Ok(ginput_args)
}

async fn make_std_tar_file_name(conn: &mut MySqlConn, job: &Job) -> JobResult<PathBuf> {
    let (site_ids, site_lats, site_lons) = job.lats_lons_sids_filled_for_date(conn, job.start_date)
        .await
        .map_err(|e| JobError::InvalidSiteLocation(e))?;

    let nsite = site_ids.len();
    let nlon = site_lons.len();
    let nlat = site_lats.len();

    let locstr = if nsite == 1 && nlon == 1 && nlat == 1 {
        format!("{}_{}_{}", site_ids[0], utils::format_lat_str(site_lats[0], 2), utils::format_lon_str(site_lons[0], 2))
    } else if nsite == nlon && nsite == nlat {
        "multisite".to_string()
    } else {
        warn!("Inconsistent number of sites, lats, and lons for tarball name. Defaulting to 'multisite' for location string");
        "multisite".to_string()
    };

    let job_id = job.job_id;
    let start = job.start_date.format("%Y%m%d").to_string();
    let end = job.end_date.format("%Y%m%d").to_string();

    let filename = format!("job_{job_id:09}_{locstr}_{start}-{end}.tgz");
    Ok(job.root_save_dir.join(filename))
}

async fn make_egi_tar_file_name(conn: &mut MySqlConn, job: &Job) -> JobResult<PathBuf> {
    let (site_ids, site_lats, site_lons) = job.lats_lons_sids_filled_for_date(conn, job.start_date)
        .await
        .map_err(|e| JobError::InvalidSiteLocation(e))?;

    let locstr = izip!(site_ids, site_lats, site_lons)
        .map(|(i, y, x)| {
            let lat = utils::format_lat_str(y, 0);
            let lon = utils::format_lon_str(x, 0);
            format!("{i}{lat}{lon}")
        }).join("_");

    let start = job.start_date.format("%Y%m%d").to_string();
    let filename = format!("EGI_{locstr}_{start}.tgz");
    Ok(job.root_save_dir.join(filename))
}

fn send_job_completion_email(job: &Job, config: &Config) -> anyhow::Result<()> {
    let email = if let Some(e) = &job.email {
        e
    } else {
        info!("No email for job {}, not sending completion email", job.job_id);
        return Ok(())
    };

    let output = if let Some(path) = &job.output_file {
        path
    } else {
        anyhow::bail!("Cannot call `send_completion_email` with a job that does not have an output file assigned")
    };

    let ftp_url = get_ftp_path(output, config)?;
    let download_command = format!("wget --user=anonymous --password=your@email.address.com -r -nH {ftp_url}");
    let subject = format!("{job} succeeded");
    let mut body = format!("{} succeeded. You may retrieve it with:\n\n {download_command}\n\n", job.to_long_string());
    if let Some(deldate) = job.delete_time {
        println!("deldate");
        body.push_str(&format!("Please note that it will be deleted at {deldate}"));
    }
    
    debug!("Sending completion email to {email}");
    config.email.send_mail(&[email], None, None, &subject, &body)
        .with_context(|| format!("Failed to send email about job {} to {}", job.job_id, email))
}

fn send_job_error_email<E: Debug>(job: &Job, err: &E, email_config: &EmailConfig) -> anyhow::Result<()> {
    let email = if let Some(e) = &job.email {
        e
    } else {
        return Ok(())
    };

    let subject = format!("{job} failed");
    let body = format!("{} failed. The error was:\n\n{err:?}\n\nPlease review the input file for errors or contact the admins ({})", job.job_id, email_config.admin_emails_string_list_for_display());
    email_config.send_mail(&[email], None, None, &subject, &body)
        .with_context(|| format!("Failed to send error email for job {}", job.job_id))
}

fn get_ftp_path(output: &Path, config: &Config) -> anyhow::Result<url::Url> {
    let server = &config.execution.ftp_download_server;
    let ftp_root = &config.execution.ftp_download_root.canonicalize()
        .context("Cannot get canonical representation of FTP root")?;
    let output = output
        .canonicalize()
        .context("Cannot get canonical representation of output path")?;
    let output = output
        .strip_prefix(&ftp_root)
        .with_context(|| format!("Could not make output {} relative to FTP root {}", output.display(), ftp_root.display()))?;

    let output = if output.is_dir() {
        format!("{}/", output.display())
    } else {
        output.to_string_lossy().to_string()
    };
    
    server.join(&output)
        .with_context(|| format!("Could not join FTP url and output relative path {output}"))
}
