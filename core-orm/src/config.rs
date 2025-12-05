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
use anyhow::{anyhow, Context};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use hostname;
use itertools::Itertools;
use lettre::message::{Mailbox, Mailboxes};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    fs::File,
    io::{Read, Write},
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};
use toml;
use url::Url;

use crate::{
    email::SendMail,
    error::{DefaultOptsQueryError, EmailError},
    met::GinputMetType,
};

mod processing;

use processing::ProcessingConfig;

/// Name of the environmental variable to look at for the path to the configuration file
pub static CFG_FILE_ENV_VAR: &str = "PRIOR_CONFIG_FILE";

#[derive(Debug, Default)]
pub struct ConfigValidationError(Vec<ConfigValErrorCause>);

impl From<ConfigValidationError> for Result<(), ConfigValidationError> {
    fn from(value: ConfigValidationError) -> Self {
        if value.0.is_empty() {
            Ok(())
        } else {
            Err(value)
        }
    }
}

impl Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Configuration failed validation:")?;
        for (idx, cause) in self.0.iter().enumerate() {
            writeln!(f, " {}) {cause}", idx + 1)?;
        }
        Ok(())
    }
}

impl ConfigValidationError {
    fn push(&mut self, err: ConfigValErrorCause) {
        self.0.push(err);
    }

    fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

#[derive(Debug)]
pub enum ConfigValErrorCause {
    UnknownGinputKey {
        key: GinputCfgKey,
        location: String,
    },
    UnknownMetKey {
        met_key: String,
        processing_key: String,
    },
    UnknownProcCfgKey {
        proc_cfg_key: ProcCfgKey,
        location: String,
    },
    BadMetConfig {
        key: String,
        reason: String,
    },
    DateRangeInverted {
        location: String,
    },
    DefaultsOverlap(Vec<(String, String)>),
    FtpPathsMismatch {
        ftp_root: PathBuf,
        output_path: PathBuf,
    },
    NoncanonicalPath {
        description: &'static str,
        path: PathBuf,
    },
    MissingPath(String),
    MissingOptPath(String),
    MissingParentPath(String),
    ExpectedFileNotDir(String),
    QueueSameName {
        q1: &'static str,
        q2: &'static str,
        name: String,
    },
    MissingEmail(&'static str),
    DuplicateKey {
        field: &'static str,
        key: String,
    },
    ProcCfgConflict {
        key1: String,
        key2: String,
    },
    Other(String),
}

impl Display for ConfigValErrorCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigValErrorCause::UnknownGinputKey { key, location } => {
                write!(f, "Undefined ginput key '{key}' in {location}")
            }
            ConfigValErrorCause::UnknownMetKey {
                met_key: key,
                processing_key,
            } => {
                write!(
                    f,
                    "Undefined met key '{key}' in processing configuration '{processing_key}'",
                )
            }
            ConfigValErrorCause::UnknownProcCfgKey {
                proc_cfg_key,
                location,
            } => {
                write!(
                    f,
                    "Undefined processing configuration key '{proc_cfg_key}' in {location}"
                )
            }
            ConfigValErrorCause::BadMetConfig { key, reason } => {
                write!(
                    f,
                    "The met download config '{key}' is not valid for use with ginput: {reason}"
                )
            }
            ConfigValErrorCause::DateRangeInverted { location } => {
                write!(f, "{location} has the end date before the start date",)
            }
            ConfigValErrorCause::DefaultsOverlap(pairs) => {
                let overlaps = pairs
                    .into_iter()
                    .map(|(a, b)| format!("{a} and {b}"))
                    .join(", ");
                write!(f, "Some default options overlap: {overlaps}")
            }
            ConfigValErrorCause::FtpPathsMismatch {
                ftp_root,
                output_path,
            } => {
                write!(
                    f,
                    "The output path ({}) is not under the FTP root ({})",
                    output_path.display(),
                    ftp_root.display()
                )
            }
            ConfigValErrorCause::NoncanonicalPath { description, path } => {
                write!(
                    f,
                    "The {description} path ({}) cannot be canonicalized",
                    path.display()
                )
            }
            ConfigValErrorCause::MissingPath(description) => {
                write!(f, "The {description} path is undefined or does not point to an extant file/directory")
            }
            ConfigValErrorCause::MissingOptPath(description) => {
                write!(f, "The {description} path does not point to a valid file; remove the option entirely to make this a None")
            }
            ConfigValErrorCause::MissingParentPath(description) => {
                write!(f, "The parent directory of the {description} path does not exist, is a file, or could not be determined from the given path.")
            }
            ConfigValErrorCause::ExpectedFileNotDir(description) => {
                write!(f, "The {description} path is expected to be a file (extant or not), but points to an existing directory")
            }
            ConfigValErrorCause::QueueSameName { q1, q2, name } => {
                write!(f, "The {q1} and {q2} queues have the same name: {name}")
            }
            ConfigValErrorCause::MissingEmail(description) => {
                write!(f, "The {description} email is empty")
            }
            ConfigValErrorCause::DuplicateKey { field, key } => {
                write!(f, "The key {key} occurs multiple times in {field}")
            }
            ConfigValErrorCause::ProcCfgConflict { key1, key2 } => {
                write!(f, "The processing configurations {key1} and {key2} output to the same tarball directory and overlap in time")
            }
            ConfigValErrorCause::Other(msg) => {
                write!(f, "{msg}")
            }
        }
    }
}

#[derive(Debug, Default, Hash, Deserialize, Serialize, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(transparent)]
pub struct ProcCfgKey(pub String);

impl Display for ProcCfgKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ProcCfgKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl FromStr for ProcCfgKey {
    // used so that this can be a type in a CLI
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl Deref for ProcCfgKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

#[derive(
    Debug, Hash, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::Type,
)]
#[sqlx(transparent)]
pub struct MetCfgKey(pub String);

impl Display for MetCfgKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MetCfgKey {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct GinputCfgKey(String);

impl Display for GinputCfgKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for GinputCfgKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl FromStr for GinputCfgKey {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

/// Top level configuration structure, comprised of subsections represented by other structures:
///
/// - `blacklist`: a list of [`BlacklistEntry`] instances that block specific users from
///    requesting jobs.
/// - `default_options`: a `Vec` of [`DefaultOptions`] that specify which ginput and met version
///   to use by default for different time periods.
/// - `execution`: an [`ExecutionConfig`] containing options about how the automation runs
/// - `requests`: a [`UserRequestConfig`] that specifies extra options about what users can
///   ask for in their runs.
/// - `data`: a [`DataConfig`] containing options about where input data is located
/// - `email`: an [`EmailConfig`] that determines how emails are sent (both for usual operation and
///   if something goes wrong)
/// - `timing`: a [`ServiceTimingOptions`] that controls how often different parts of the service run.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub blacklist: Vec<BlacklistEntry>, // errors if later in the struct (might be okay after default_options now)
    pub default_options: Vec<DefaultOptions>, // errors if after data
    pub execution: ExecutionConfig,
    pub data: DataConfig,
    #[serde(default)]
    pub processing_configuration: HashMap<ProcCfgKey, ProcessingConfig>,
    #[serde(default)]
    pub email: EmailConfig,
    #[serde(default)]
    pub timing: ServiceTimingOptions,
    pub auth: AuthConfig,
}

impl Config {
    /// Return the span of dates covered by the default meteorologies
    ///
    /// Returns the earliest start date and latest end date of all meteorologies
    /// used in the default option sets. If at least one default met defines no
    /// start date, then the returned start date will be `None`, and likewise for
    /// the end date. Note that this function does not check for overlap among the
    /// default ranges (i.e. it assumes a validated configuration), nor does it check
    /// for gaps in the defaults.
    pub fn get_default_met_date_range(&self) -> (Option<NaiveDate>, Option<NaiveDate>) {
        let mut start_date = None;
        let mut no_start = false;
        let mut end_date = None;
        let mut no_end = false;

        for default_set in self.default_options.iter() {
            if no_start {
                // no op
            } else if let (None, Some(sd)) = (start_date, default_set.start_date) {
                start_date = Some(sd);
            } else if let (Some(curr_sd), Some(sd)) = (start_date, default_set.start_date) {
                if sd < curr_sd {
                    start_date = Some(sd);
                }
            } else if default_set.start_date.is_none() {
                start_date = None;
                no_start = true;
            }

            if no_end {
                // no op
            } else if let (None, Some(ed)) = (end_date, default_set.end_date) {
                end_date = Some(ed);
            } else if let (Some(curr_ed), Some(ed)) = (end_date, default_set.end_date) {
                if ed > curr_ed {
                    end_date = Some(ed);
                }
            } else if default_set.end_date.is_none() {
                end_date = None;
                no_end = true;
            }
        }

        (start_date, end_date)
    }

    /// Return references to each met file type required by a single processing configuration.
    pub fn get_mets_for_processing_config<'cfg>(
        &'cfg self,
        proc_cfg_key: &ProcCfgKey,
    ) -> anyhow::Result<Vec<KeyedMetDownloadConfig<'cfg>>> {
        let proc_cfg = self
            .processing_configuration
            .get(proc_cfg_key)
            .ok_or_else(|| {
                anyhow!("Requested processing configuration '{proc_cfg_key}' not defined")
            })?;
        proc_cfg.get_met_configs(self).with_context(|| anyhow!("Error occurred while getting required mets for processing configuration '{proc_cfg_key}'"))
    }

    /// Return references to each met file type required by the given processing configurations.
    pub fn get_unique_mets_for_processing_configs<'cfg>(
        &'cfg self,
        proc_cfg_keys: &[&ProcCfgKey],
    ) -> anyhow::Result<Vec<KeyedMetDownloadConfig<'cfg>>> {
        let mut all_mets = vec![];
        for key in proc_cfg_keys {
            let mets = self.get_mets_for_processing_config(&key)?;
            all_mets.extend(mets.into_iter());
        }
        // Sorting is necessary for de-deduplication to work.
        all_mets.sort_unstable_by_key(|met| met.product_key);
        all_mets.dedup_by_key(|met| met.product_key);
        debug!(
            "Found {} unique mets for the given product configs ({proc_cfg_keys:?}",
            all_mets.len()
        );
        Ok(all_mets)
    }

    /// Return references to each met file type required by any of the processing configurations
    /// set to need met downloaded automatically. Providing `start_date` and `end_date` will limit
    /// the values returned to processing configurations that could be run for that period.
    pub fn get_unique_mets_for_auto_proc_cfgs(
        &self,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> anyhow::Result<Vec<KeyedMetDownloadConfig>> {
        let proc_cfgs = self
            .get_proc_cfgs_with_auto_met_download(start_date, end_date)
            .into_iter()
            .collect_vec();
        self.get_unique_mets_for_processing_configs(&proc_cfgs)
    }

    /// Return references to all met file types defined in the configuration along with their config keys.
    pub fn get_all_mets(&self) -> Vec<KeyedMetDownloadConfig<'_>> {
        let mut mets = vec![];
        for (product_key, cfg) in self.data.met_download.iter() {
            mets.push(KeyedMetDownloadConfig { product_key, cfg });
        }
        mets
    }

    /// Return the earliest and latest dates a particular met is required, based on
    /// the defined processing configurations. The end date may be `None` if the
    /// need is open-ended. The entire return value will be `None` if the met is
    /// not required by any processing configurations.
    pub fn get_dates_met_needed_for_processing(
        &self,
        met_key: &MetCfgKey,
    ) -> Option<(NaiveDate, Option<NaiveDate>)> {
        let mut cfg_dates = self.processing_configuration.iter().filter_map(|(pk, pc)| {
            if pc.required_mets.contains(met_key) {
                debug!(
                    "{pk} needs met {met_key} for {:?} to {:?}",
                    pc.start_date, pc.end_date
                );
                Some((pc.start_date, pc.end_date))
            } else {
                None
            }
        });

        // This lets us return early if there are no processing configs that rely on this
        // met without allocating a vector to check its length.
        let (mut start, mut end) = cfg_dates.next()?;
        for (this_start, this_end) in cfg_dates {
            start = start.min(this_start);
            end = crate::utils::later_opt_end_date(end, this_end);
        }
        debug!("Met {met_key} is needed between {start:?} and {end:?}");
        Some((start, end))
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
    /// If successful, a tuple containing the `geos_path`, `chem_path`, and the string telling ginput
    /// which met type these paths are for, in that order. If the met configuration requested does not
    /// define any chemistry files, a warning will be logged and the `chem_path` will be the same as the
    /// `geos_path`.
    ///
    /// This returns an error in a number of conditions:
    /// 1. Any of the `download_dir` paths cannot be canonicalized
    /// 2. Any of the `download_dir` paths does not contain at least 1 component
    /// 3. The final component of any of the `download_dir` paths does not match the levels defined
    ///    for that file type.
    /// 4. Any of the `download_dir` paths does not have a parent directory
    /// 5. Inconsistent `geos_path`, `chem_path`, or `ginput_met_key` values are defined.
    pub fn get_ginput_met_args(
        &self,
        proc_cfg_key: &ProcCfgKey,
    ) -> anyhow::Result<(PathBuf, PathBuf, String)> {
        let proc_cfg = self
            .processing_configuration
            .get(proc_cfg_key)
            .ok_or_else(|| anyhow!("Unknown processing configuration '{proc_cfg_key}'"))?;
        let dl_cfgs = proc_cfg.get_met_configs(self).with_context(|| {
            anyhow!(
                "Error occurred while getting mets for processing configuration '{proc_cfg_key}'"
            )
        })?;

        let mut geos_path = None;
        let mut chem_path = None;
        let ginput_met_key = proc_cfg.ginput_met_key.clone();

        let mut found_eta_met = false;
        let mut found_eta_chem = false;
        let mut found_2d_met = false;

        for (i, met_cfg) in dl_cfgs.iter().enumerate() {
            let i = i + 1;
            let download_dir = met_cfg.cfg.download_dir.canonicalize()
                .map_err(|e| anyhow::Error::msg(format!("In processing configuration '{proc_cfg_key}', could not canonicalize download path for met file type #{i}: {e}")))?;
            let final_dir = download_dir.components()
                .last()
                .ok_or_else(|| anyhow::Error::msg(format!("In processing configuration '{proc_cfg_key}', file {i} does not have a final component to its download path")))?;

            if final_dir.as_os_str() != met_cfg.cfg.ginput_met_type.standard_subdir().as_os_str() {
                let final_dir = final_dir.as_os_str().to_string_lossy();
                anyhow::bail!(format!("In processing configuration {proc_cfg_key}, file type {i}'s final component ({final_dir}) is not consistent with its declared met subtype ({})", met_cfg.cfg.ginput_met_type))
            }

            let parent_dir = download_dir.parent()
                .ok_or_else(|| anyhow::Error::msg(format!("In processing configuration '{proc_cfg_key}', cannot get parent directory of file type {i}'s download path")))?;

            match &met_cfg.cfg.ginput_met_type {
                GinputMetType::MetEta => {
                    found_eta_met = true;
                    if geos_path.is_none() {
                        geos_path = Some(parent_dir.to_owned());
                    } else if geos_path.as_deref() != Some(parent_dir) {
                        anyhow::bail!(
                            "Processing config '{proc_cfg_key}' defines inconsistent parent directories for its met files: {} vs {}",
                            parent_dir.display(), geos_path.unwrap().display()
                        );
                    }
                }

                GinputMetType::Met2D => found_2d_met = true,

                GinputMetType::ChemEta => {
                    found_eta_chem = true;
                    if chem_path.is_none() {
                        chem_path = Some(parent_dir.to_owned());
                    } else if chem_path.as_deref() != Some(parent_dir) {
                        anyhow::bail!(
                            "Processing config '{proc_cfg_key}' defines inconsistent parent directories for its chem files: {} vs {}",
                            parent_dir.display(), chem_path.unwrap().display()
                        );
                    }
                }

                GinputMetType::Other => {
                    info!("Ignoring non-standard met #{i} for processing config '{proc_cfg_key}'")
                }
            }
        }

        let geos_path = geos_path.ok_or_else(|| {
            anyhow::anyhow!("Processing config '{proc_cfg_key}' defines no met files for download")
        })?;

        let chem_path = chem_path.ok_or_else(|| {
            anyhow::anyhow!("Processing config '{proc_cfg_key}' defines no chem files for download")
        })?;

        if !found_2d_met {
            anyhow::bail!("2D met files not defined for download");
        }

        if !found_eta_met {
            anyhow::bail!("Eta met files not defined for download");
        }

        if !found_eta_chem {
            anyhow::bail!("Eta chem files not defined for download");
        }

        Ok((geos_path, chem_path, ginput_met_key))
    }

    pub fn get_possible_proc_cfgs_for_date(&self, date: NaiveDate) -> Vec<&ProcCfgKey> {
        let mut proc_cfgs = vec![];
        for (key, proc_cfg) in self.processing_configuration.iter() {
            if proc_cfg.contains_date(date) {
                proc_cfgs.push(key)
            }
        }
        proc_cfgs
    }

    /// Return a list of processing configurations that require met data to be automatically downloaded.
    /// Providing `start_date` and `end_date` will limit the values returned to processing configurations
    /// that could be run for that period.
    pub fn get_proc_cfgs_with_auto_met_download(
        &self,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Vec<&ProcCfgKey> {
        let mut proc_cfgs = vec![];
        for (key, proc_cfg) in self.processing_configuration.iter() {
            // We only want to return a processing configuration if it overlaps with the date range requested.
            // That way if we're only interested in downloaded mets for a period that covers a subset of
            // processing configs, we don't accidentally download uninvolved processing configs' data.
            if proc_cfg.download_met_automatically()
                && crate::utils::date_ranges_overlap(
                    start_date,
                    end_date,
                    Some(proc_cfg.start_date),
                    proc_cfg.end_date,
                )
            {
                proc_cfgs.push(key);
            }
        }
        proc_cfgs
    }

    /// The top-level subdirectory that `ginput` places output for this met type, e.g. "fpit" for GEOS FP-IT.
    /// If the `ginput_met_key` value is "XXX-eta", then this is usually "XXX".
    pub fn get_proc_cfg_ginput_output_subdirs<'a>(
        &'a self,
        proc_cfg_key: &ProcCfgKey,
    ) -> anyhow::Result<&'a str> {
        let proc_cfg = self
            .processing_configuration
            .get(proc_cfg_key)
            .ok_or_else(|| anyhow!("Processing configuration '{proc_cfg_key}' not defined"))?;
        Ok(&proc_cfg.ginput_output_subdir)
    }

    /// Get the earliest start date for all the file types of a given met configuration
    ///
    /// When a met type has multiple files (e.g. 3D assimilation, 2D assimilation, and 3D chemistry for GEOS),
    /// it is possible that different file types start at different times, so the each file has an earliest
    /// date it is available for in the config. This returns the minimum among all of those for the given
    /// `met_key`, or `None` if `met_key` exists but has no files defined.
    ///
    /// Will also return an `Err` is `met_key` is not in the config.
    pub fn get_proc_cfg_start_date(
        &self,
        proc_cfg_key: &ProcCfgKey,
    ) -> anyhow::Result<Option<NaiveDate>> {
        let met_cfgs = self
            .get_mets_for_processing_config(proc_cfg_key)
            .with_context(|| {
                anyhow!("Error occurred while getting mets for processing config '{proc_cfg_key}'")
            })?;

        let maybe_min_date = met_cfgs.iter().map(|c| c.cfg.earliest_date).min();
        Ok(maybe_min_date)
    }

    /// Get the sequence of [`DefaultOptions`] in time order.
    pub fn get_all_defaults(&self) -> Vec<&DefaultOptions> {
        let mut all_options: Vec<&DefaultOptions> = self.default_options.iter().collect();

        // Order by start date, treating None as the earliest possible
        all_options.sort_by(|a, b| match (a.start_date, b.start_date) {
            (None, None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(d1), Some(d2)) => d1.cmp(&d2),
        });

        all_options
    }

    /// Get the sequence of [`DefaultOptions`] in time order and check that none overlap in time.
    ///
    /// Returns an `Err` (with a [`DefaultOptsQueryError::MatchesOverlap`] inner value) if any two
    /// sets of default options do overlap in time.
    pub fn get_all_defaults_check_overlap(
        &self,
    ) -> Result<Vec<&DefaultOptions>, DefaultOptsQueryError> {
        let all_options = self.get_all_defaults();
        let pairs = Self::check_defaults_overlap(&all_options, true);
        if !pairs.is_empty() {
            return Err(DefaultOptsQueryError::MatchesOverlap(
                pairs[0].0.to_string(),
                pairs[0].1.to_string(),
            ));
        }

        Ok(all_options)
    }

    fn check_defaults_overlap<'d>(
        all_options: &[&'d DefaultOptions],
        short_circuit: bool,
    ) -> Vec<(&'d DefaultOptions, &'d DefaultOptions)> {
        let mut overlaps = vec![];
        for pair in all_options.iter().combinations(2) {
            if pair[0].overlaps(pair[1]) {
                if short_circuit {
                    return vec![(pair[0], pair[1])];
                } else {
                    overlaps.push((*pair[0], *pair[1]));
                }
            }
        }

        return overlaps;
    }

    /// Get the [`DefaultOptions`] instance for a given date.
    ///
    /// Returns a `Err` if 0 or >1 option set matches the date. The inner value will be a
    /// [`DefaultOptsQueryError::NoMatches`] for 0 and [`DefaultOptsQueryError::MultipleMatches`]
    /// for >1.
    pub fn get_defaults_for_date(
        &self,
        date: NaiveDate,
    ) -> Result<&DefaultOptions, DefaultOptsQueryError> {
        let all_options = self.default_options.as_slice();

        // Filter down to the rows which apply to this date. If >1 or 0, that is an error.
        let all_options: Vec<&DefaultOptions> = all_options
            .into_iter()
            .filter(|o| match (o.start_date, o.end_date) {
                (None, None) => true,
                (None, Some(end)) => date < end,
                (Some(start), None) => date >= start,
                (Some(start), Some(end)) => start <= date && date < end,
            })
            .collect();

        if all_options.len() == 1 {
            Ok(all_options[0])
        } else if all_options.is_empty() {
            Err(DefaultOptsQueryError::NoMatches(date))
        } else {
            let matches = all_options.iter().map(|o| o.to_string()).collect_vec();
            Err(DefaultOptsQueryError::MultipleMatches { date, matches })
        }
    }

    /// Return the first date for which any processing configurations are set to generate
    /// automatically. If no automatic processing configurations exist, returns an error.
    pub fn get_first_date_for_automatic_processing(&self) -> anyhow::Result<NaiveDate> {
        let start = self
            .processing_configuration
            .iter()
            .filter_map(|(_, pc)| {
                if pc.generate_automatically {
                    Some(pc.auto_start_date())
                } else {
                    None
                }
            })
            .min();
        start.ok_or_else(|| anyhow!("No automatic processing configurations defined"))
    }

    /// Return the last date for which any processing configurations are set to generate
    /// automatically. This will be `None` if at least one processing configuration is
    /// open-ended. If no automatic processing configurations exist, returns an error.
    pub fn get_last_date_for_automatic_processing(&self) -> anyhow::Result<Option<NaiveDate>> {
        let end = self
            .processing_configuration
            .iter()
            .filter_map(|(_, pc)| {
                if pc.generate_automatically {
                    Some(pc.auto_end_date())
                } else {
                    None
                }
            })
            .fold(None, |opt_max, date| {
                if let Some(max) = opt_max {
                    Some(crate::utils::later_opt_end_date(max, date))
                } else {
                    None
                }
            });

        // The outer option indicates if we found at least one automatic config. Turn that
        // into an error if we've found none.
        end.ok_or_else(|| anyhow!("No automatic processing configurations defined"))
    }

    /// Get a vector of all processing configurations that are set to generate automatically.
    pub fn get_auto_proc_cfgs(&self) -> Vec<&ProcCfgKey> {
        self.processing_configuration
            .iter()
            .filter_map(|(key, pc)| {
                if pc.generate_automatically {
                    Some(key)
                } else {
                    None
                }
            })
            .collect_vec()
    }

    /// Return the list of processing configurations that must be generated
    /// automatically for a given date.
    pub fn get_auto_proc_cfgs_for_date(&self, date: NaiveDate) -> Vec<&ProcCfgKey> {
        self.processing_configuration
            .iter()
            .filter_map(|(key, proc_cfg)| {
                if proc_cfg.auto_for_date(date) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect_vec()
    }

    pub fn get_auto_proc_cfgs_for_date_range(
        &self,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Vec<&ProcCfgKey> {
        self.processing_configuration
            .iter()
            .filter_map(|(key, proc_cfg)| {
                if proc_cfg.auto_for_date_range(start_date, end_date) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect_vec()
    }

    /// Get the information about a job queue by name
    ///
    /// If the queue does not have a section defined in the configuration, then the
    /// `None` is returned. Use `unwrap_or_default` to get the default queue with
    /// one processor allocated.
    pub fn get_queue(&self, queue_name: &str) -> Option<JobQueueOptions> {
        self.execution.queues.get(queue_name).map(|q| q.to_owned())
    }

    /// If the configuration is set for simulation, return the number of seconds to delay
    /// ginput outputting sim files by. If not set for a simulation, return `None`.
    pub fn get_sim_delay(&self) -> Option<u32> {
        if !self.execution.simulate {
            None
        } else {
            Some(self.execution.simulation_delay)
        }
    }

    pub fn check_user_proc_request(
        &self,
        proc_cfg_key: &ProcCfgKey,
        request_start: NaiveDate,
        request_end: NaiveDate,
    ) -> anyhow::Result<()> {
        let proc_cfg = self
            .processing_configuration
            .get(proc_cfg_key)
            .ok_or_else(|| anyhow!("Unknown processing configuration: '{proc_cfg_key}'"))?;

        match crate::utils::DateRangeOverlap::classify(
            Some(proc_cfg.start_date),
            proc_cfg.end_date,
            Some(request_start),
            Some(request_end),
        ) {
            crate::utils::DateRangeOverlap::AContainsB
            | crate::utils::DateRangeOverlap::AEqualsB => Ok(()),
            crate::utils::DateRangeOverlap::AInsideB
            | crate::utils::DateRangeOverlap::AEndsInB
            | crate::utils::DateRangeOverlap::AStartsInB
            | crate::utils::DateRangeOverlap::None => {
                let range_str = proc_cfg.describe_date_range();
                Err(anyhow!("processing configuration '{proc_cfg_key}' spans {range_str} but you requested dates ({request_start} to {request_end}) outside this range"))
            }
        }
    }

    fn validate(&self) -> Result<(), ConfigValidationError> {
        let mut errors = ConfigValidationError::default();

        let default_opts_errors = self.validate_default_options();
        errors.extend(default_opts_errors);

        let exec_errors = self.validate_execution();
        errors.extend(exec_errors);

        let ginput_errors = self.validate_ginputs();
        errors.extend(ginput_errors);

        let processing_errors = processing::validate_processing_configs(self);
        errors.extend(processing_errors);

        let data_errors = self.validate_data();
        errors.extend(data_errors);

        let email_errors = self.validate_emails();
        errors.extend(email_errors);

        errors.into()
    }

    fn validate_default_options(&self) -> ConfigValidationError {
        let mut errors = ConfigValidationError::default();

        for (idx, default_opts) in self.default_options.iter().enumerate() {
            if !self
                .processing_configuration
                .contains_key(&default_opts.processing_configuration)
            {
                errors.push(ConfigValErrorCause::UnknownProcCfgKey {
                    proc_cfg_key: default_opts.processing_configuration.clone(),
                    location: format!("default options #{}", idx + 1),
                });
            }

            if default_opts.start_date >= default_opts.end_date {
                errors.push(ConfigValErrorCause::DateRangeInverted {
                    location: format!("default options #{}", idx + 1),
                });
            }
        }

        let pairs = Self::check_defaults_overlap(&self.get_all_defaults(), false);
        if !pairs.is_empty() {
            let pairs = pairs
                .into_iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect_vec();
            errors.push(ConfigValErrorCause::DefaultsOverlap(pairs));
        }

        errors
    }

    fn validate_execution(&self) -> ConfigValidationError {
        let mut errors = ConfigValidationError::default();

        // Check paths are not empty
        if !self.execution.ftp_download_root.exists() {
            errors.push(ConfigValErrorCause::MissingPath(
                "execution.ftp_download_root".to_string(),
            ));
        }
        if !self.execution.output_path.exists() {
            errors.push(ConfigValErrorCause::MissingPath(
                "execution.output_path".to_string(),
            ));
        }
        if !self.execution.std_sites_output_base.exists() {
            errors.push(ConfigValErrorCause::MissingPath(
                "execution.std_sites_output_base".to_string(),
            ));
        }

        // Check output is in the FTP directory
        let output = self.execution.output_path.canonicalize();
        let ftp_root = self.execution.ftp_download_root.canonicalize();
        if let (Ok(out), Ok(ftp)) = (&output, &ftp_root) {
            if out.strip_prefix(ftp).is_err() {
                errors.push(ConfigValErrorCause::FtpPathsMismatch {
                    ftp_root: ftp.to_path_buf(),
                    output_path: out.to_path_buf(),
                });
            }
        } else {
            if output.is_err() {
                errors.push(ConfigValErrorCause::NoncanonicalPath {
                    description: "execution.output_path",
                    path: self.execution.output_path.clone(),
                });
            }

            if ftp_root.is_err() {
                errors.push(ConfigValErrorCause::NoncanonicalPath {
                    description: "execution.ftp_download_root",
                    path: self.execution.ftp_download_root.clone(),
                })
            }
        }

        // Check JSON paths' parent directories exist
        if let Some(p) = &self.execution.flat_stdsite_json_file {
            if !p.parent().map(|p| p.exists()).unwrap_or(false) {
                errors.push(ConfigValErrorCause::MissingParentPath(
                    "execution.flat_stdsite_json_file".to_string(),
                ));
            }

            if p.is_dir() {
                errors.push(ConfigValErrorCause::ExpectedFileNotDir(
                    "execution.flat_stdsite_json_file".to_string(),
                ));
            }
        }

        if let Some(p) = &self.execution.grouped_stdsite_json_file {
            if !p.parent().map(|p| p.exists()).unwrap_or(false) {
                errors.push(ConfigValErrorCause::MissingParentPath(
                    "execution.grouped_stdsite_json_file".to_string(),
                ));
            }

            if p.is_dir() {
                errors.push(ConfigValErrorCause::ExpectedFileNotDir(
                    "execution.grouped_stdsite_json_file".to_string(),
                ));
            }
        }

        // Check queues
        if self.execution.submitted_job_queue == self.execution.std_site_job_queue {
            errors.push(ConfigValErrorCause::QueueSameName {
                q1: "submitted",
                q2: "std_site",
                name: self.execution.submitted_job_queue.clone(),
            });
        }

        errors
    }

    fn validate_ginputs(&self) -> ConfigValidationError {
        let mut errors = ConfigValidationError::default();

        for (key, ginput) in self.execution.ginput.iter() {
            match ginput {
                GinputConfig::Script { entry_point_path } => {
                    if !entry_point_path.is_file() {
                        errors.push(ConfigValErrorCause::MissingPath(format!(
                            "execution.ginput.{key}.entry_point_path"
                        )));
                    }
                }
            }
        }

        errors
    }

    fn validate_data(&self) -> ConfigValidationError {
        let mut errors = ConfigValidationError::default();

        if self
            .data
            .zgrid_file
            .as_deref()
            .map(|p| !p.exists())
            .unwrap_or(false)
        {
            errors.push(ConfigValErrorCause::MissingPath(
                "data.zgrid_file".to_string(),
            ));
        }

        if self
            .data
            .base_vmr_file
            .as_deref()
            .map(|p| !p.exists())
            .unwrap_or(false)
        {
            errors.push(ConfigValErrorCause::MissingPath(
                "data.base_vmr_file".to_string(),
            ));
        }

        // Confirm that all processing configurations are valid for ginput
        for proc_cfg_key in self.processing_configuration.keys() {
            if let Err(e) = self.get_ginput_met_args(proc_cfg_key) {
                errors.push(ConfigValErrorCause::BadMetConfig {
                    key: proc_cfg_key.to_string(),
                    reason: e.to_string(),
                });
            }

            if let Err(e) = self.get_proc_cfg_ginput_output_subdirs(proc_cfg_key) {
                errors.push(ConfigValErrorCause::BadMetConfig {
                    key: proc_cfg_key.to_string(),
                    reason: e.to_string(),
                });
            }
        }

        errors
    }

    fn validate_emails(&self) -> ConfigValidationError {
        let mut errors = ConfigValidationError::default();

        if self.email.admin_emails.iter().count() == 0 {
            errors.push(ConfigValErrorCause::MissingEmail("admin emails list"));
        }

        errors
    }
}

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
    pub ginput: HashMap<GinputCfgKey, GinputConfig>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        let host = hostname::get().unwrap_or("localhost".into());
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
            ftp_download_server: Url::parse(&format!("ftp://{host}/"))
                .unwrap_or_else(|_| Url::parse("ftp://localhost/").unwrap()),
            ftp_download_root: Default::default(),
            output_path: Default::default(),
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
    Script { entry_point_path: PathBuf },
}

/// Configuration section dealing with input data for jobs
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DataConfig {
    /// The path to an integral.gnd file that specifies an altitude grid. If omitted,
    /// or an empty string, then the priors are produced on the native GEOS grid.
    pub zgrid_file: Option<PathBuf>,

    /// The path to a summer, 35N .vmr file that will be used for the secondary
    /// gases. If omitted or an empty string, the secondary gases are not included.
    pub base_vmr_file: Option<PathBuf>,

    /// A map of configurations that specify how to download a particular type of met files.
    /// Note that this is NOT a full set of met files that ginput requires. For instance,
    /// from GEOS IT, we require 3D met, 2D met, and 3D chemistry files. Each entry in this
    /// map specifies one of those files. The processing configuration section specifies
    /// how we combine them together.
    pub met_download: HashMap<MetCfgKey, MetDownloadConfig>,
}

/// Configuration for how to download input reanalysis files for ginput
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetDownloadConfig {
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

    /// The latest date for which this met data is available for download (exclusive).
    /// If omitted, the met is assumed to have no end date.
    pub latest_date: Option<NaiveDate>,

    /// The directory to download the data into. For met products intended to be used with ginput v1.y.z,
    /// this must still follow the expected directory structure for GEOS data. Namely:
    /// * Surface, hybrid eta level, and fixed pressure level files must reside in `Nx`, `Nv`, and `Np`
    ///   subdirectories, respectively.
    /// * Met data for a given met data source must be in subdirectories of the same path; that is, if
    ///   the eta level files are in `/data/met/Nv`, the surface/2D files must be in `/data/met/Nx`.
    ///   Chemistry data can be in the same directory or separate, e.g. in this example, the eta-level
    ///   chem files could go in `/data/met/Nv` or a separate directory such as `/data/chm/Nv`. The
    ///   latter is recommended.
    pub download_dir: PathBuf,

    /// A string describing which of the GEOS file types ginput requires this represents.
    /// See [`GinputMetType`] for allowed values.
    pub ginput_met_type: GinputMetType,

    /// How many days to allow before failing to download files is an error
    pub days_latency: u32,
}

/// A structure combining the product key of a met download configuration with its
/// configuration values.
#[derive(Debug, Clone, Copy)]
pub struct KeyedMetDownloadConfig<'cfg> {
    pub product_key: &'cfg MetCfgKey,
    pub cfg: &'cfg MetDownloadConfig,
}

impl Display for MetDownloadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(pat) = self.get_basename_pattern() {
            write!(f, "{pat} ({})", self.ginput_met_type)
        } else {
            let end = self
                .latest_date
                .map(|d| d.to_string())
                .unwrap_or_else(|| "now".to_string());
            write!(
                f,
                "{} to {end} ({})",
                self.earliest_date, self.ginput_met_type
            )
        }
    }
}

impl MetDownloadConfig {
    /// Get the pattern for file names of this type of file, with no leading path.
    ///
    /// If the configuration has a value for the `basename_pattern` specified, that is
    /// returned. Otherwise, the URL pattern is split on the last "/" and everything after
    /// that slash is used as the pattern.
    ///
    /// Can return an `Err` if it could not identify a part after the final slash in the URL.
    pub fn get_basename_pattern(&self) -> anyhow::Result<&str> {
        if let Some(pat) = &self.basename_pattern {
            return Ok(&pat);
        } else {
            // let full_url = url::Url::parse(&self.url_pattern)?
            //     .path_segments()
            //     .ok_or_else(|| anyhow::Error::msg(format!("Could not find the file base name from URL pattern {}", self.url_pattern)))?
            //     .last()
            //     .ok_or_else(|| anyhow::Error::msg(format!("Could not find the file base name from URL pattern {}", self.url_pattern)))?;
            // Ok(full_url)

            // Preferring this over the URL library because the latter makes it difficult to
            // return a &str.
            self.url_pattern.split('/').last().ok_or_else(|| {
                anyhow::Error::msg(format!(
                    "Could not find the file base name from URL pattern {}",
                    self.url_pattern
                ))
            })
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
    /// The returned paths point to files for the times given by [`MetDownloadConfig::times_on_day`]
    /// in the path gievn by [`MetDownloadConfig::get_save_dir`].
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
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DefaultOptions {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub processing_configuration: ProcCfgKey,
}

impl DefaultOptions {
    // Test whether this `DefaultOptions` instance overlaps another in time
    fn overlaps(&self, other: &Self) -> bool {
        let class = crate::utils::DateRangeOverlap::classify(
            self.start_date,
            self.end_date,
            other.start_date,
            other.end_date,
        );
        class.has_overlap()
    }
}

impl Display for DefaultOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{} to {}: {}]",
            self.start_date
                .map(|d| d.to_string())
                .unwrap_or_else(|| "None".to_owned()),
            self.end_date
                .map(|d| d.to_string())
                .unwrap_or_else(|| "None".to_owned()),
            self.processing_configuration
        )
    }
}

/// A structure describing configuration of a job queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobQueueOptions {
    /// The maximum number of processors that this queue can use (default = 1)
    pub max_num_procs: usize,

    /// The fair share policy to use for this queue
    #[serde(default)]
    pub fair_share_policy: FairSharePolicy,
}

impl Default for JobQueueOptions {
    fn default() -> Self {
        Self {
            max_num_procs: 1,
            fair_share_policy: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FairSharePolicy {
    Simple(crate::jobs::PrioritySubmitFS),
    RoundRobin(crate::jobs::PsuedoRoundRobinFS),
}

impl Default for FairSharePolicy {
    fn default() -> Self {
        Self::Simple(crate::jobs::PrioritySubmitFS {})
    }
}

#[async_trait::async_trait]
impl crate::jobs::FairShare for FairSharePolicy {
    async fn next_job_in_queue(
        &self,
        conn: &mut crate::MySqlConn,
        queue: &str,
    ) -> crate::error::JobResult<Option<crate::jobs::Job>> {
        match self {
            Self::Simple(policy) => policy.next_job_in_queue(conn, queue).await,
            Self::RoundRobin(policy) => policy.next_job_in_queue(conn, queue).await,
        }
    }

    async fn order_jobs_for_display(
        &self,
        conn: &mut crate::MySqlConn,
        queue: &str,
        jobs: Vec<crate::jobs::Job>,
    ) -> crate::error::JobResult<Vec<(crate::jobs::Job, HashMap<&'static str, String>)>> {
        match self {
            Self::Simple(policy) => policy.order_jobs_for_display(conn, queue, jobs).await,
            Self::RoundRobin(policy) => policy.order_jobs_for_display(conn, queue, jobs).await,
        }
    }
}

/// Configuration for how frequently elements of the systemd service run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceTimingOptions {
    /// Set to true to disable met downloading
    pub disable_met_download: bool,

    /// How many hours between attempts to download the met data.
    /// The met download will run on even multiples of that hour,
    /// e.g. if this is 6, then the met download will run at 00:00,
    /// 06:00, 12:00, and 18:00
    pub met_download_hours: u32,

    /// Set to true to disable all aspects of running jobs: parsing input
    /// files, starting jobs, regenerating the stratospheric LUTs, etc.
    pub disable_job: bool,

    /// How many seconds between attempts to start jobs. As with met,
    /// the attempts run on even multiples of this value.
    pub job_start_seconds: u32,

    /// How many seconds between scans for status reports
    pub status_report_seconds: u32,

    /// How frequently (in days) to insert jobs to regenerate the stratosphere
    /// look up tables for ginput.
    pub lut_regen_days: u32,

    /// What time of the day, in HH:MM:SS format, to run the LUT regen. If
    /// omitted, then that will run at midnight.
    pub lut_regen_at: Option<NaiveTime>,

    /// How many hours between tries to clean up jobs whose output is ready
    /// to be deleted.
    pub delete_expired_jobs_hours: u32,

    /// How many minutes to add to the time when determining when to clean
    /// up expired jobs' output. For example, setting this to 15 when
    /// delete_expired_jobs_minutes` is 60 would run the standard sites at
    /// 15 minutes past each hour.
    #[serde(default)]
    pub delete_expired_jobs_offset_minutes: Option<u32>,

    /// Set to true to disable generating standard site jobs/priors
    pub disable_std_site_gen: bool,

    /// How frequently (in hours) to check for new days to generate standard
    /// site priors and submit jobs.
    pub std_site_gen_hours: u32,

    /// How many minutes to add to the hours when determining when to run the
    /// standard sites. E.g., setting this to 180 when `std_site_gen_hours` is 24
    /// would run the standard sites at 03:00 every day.
    #[serde(default)]
    pub std_site_gen_offset_minutes: Option<u32>,

    /// How frequently (in minutes) to check for standard site days ready to be
    /// compressed into tarballs
    pub std_site_tar_minutes: u32,

    /// Set to true to disable the daily/weekly reports.
    pub disable_reports: bool,

    /// Local time of day to deliver the daily reports, in HH:MM:SS format.
    /// Default is 8:00 am.
    #[serde(default)]
    pub daily_report_time: NaiveTime,

    /// Local time of day to deliver the weekly reports, in HH:MM:SS format.
    /// Default is 8:00 am. Weekly reports are always delivered on a Monday.
    #[serde(default)]
    pub weekly_report_time: NaiveTime,

    /// How frequently (in hours) to regenerate the standard site JSON files.
    /// Note that if the paths for the JSON files are not given in the `execution`
    /// section, they will not be generated.
    pub std_site_json_hours: u32,
}

impl Default for ServiceTimingOptions {
    fn default() -> Self {
        Self {
            disable_met_download: false,
            met_download_hours: 6,
            disable_job: false,
            job_start_seconds: 60,
            status_report_seconds: 60,
            lut_regen_days: 24,
            lut_regen_at: NaiveTime::from_hms_opt(0, 0, 0),
            delete_expired_jobs_hours: 12,
            delete_expired_jobs_offset_minutes: None,
            disable_std_site_gen: false,
            std_site_gen_hours: 24,
            std_site_gen_offset_minutes: Some(180),
            std_site_tar_minutes: 30,
            disable_reports: false,
            daily_report_time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            weekly_report_time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            std_site_json_hours: 1,
        }
    }
}

/// Configuration for how to send emails and who to contact if there is a severe problem
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailConfig {
    /// The email address that emails from this system come from.
    /// The default is "noreplay@<hostname>", where <hostname> is
    /// the host name of the system.
    pub from_address: Mailbox,

    /// A list of emails to contact in the event of a severe error
    /// that needs addressed by the administrators.
    admin_emails: Mailboxes,

    /// A list of emails to send weekly reports to. If not given, then
    /// the admin emails are used.
    #[serde(default)]
    report_emails: Mailboxes,

    /// A list of emails to send standard site requests to. If not present,
    /// they'll need specified on the command line.
    pub std_site_req_emails: Option<Mailboxes>,

    /// Which email backend to use to send the emails.
    pub backend: EmailBackend,

    /// Additional emails to send to when sending an email to all past submitters
    pub extra_submitters: Mailboxes,
}

impl Default for EmailConfig {
    fn default() -> Self {
        let user = "noreply";
        let host = whoami::fallible::hostname().unwrap_or_else(|_| "127.0.0.1".to_string());
        let email = lettre::Address::new(user, host)
            .expect("user@hostname cannot be used as a valid email address, you will need to configure the 'from_address' in the 'email' section of the config");
        let from_addr = Mailbox::new(None, email);
        Self {
            from_address: from_addr,
            admin_emails: Default::default(),
            report_emails: Default::default(),
            std_site_req_emails: Default::default(),
            backend: Default::default(),
            extra_submitters: Default::default(),
        }
    }
}

impl EmailConfig {
    /// Send an email, using the configured backend, from the configured address.
    pub fn send_mail(
        &self,
        to: &[&str],
        cc: Option<&[&str]>,
        bcc: Option<&[&str]>,
        subject: &str,
        message: &str,
    ) -> Result<(), EmailError> {
        let from = self.from_address.to_string();
        match &self.backend {
            EmailBackend::Internal(backend) => {
                backend.send_mail(to, &from, cc, bcc, subject, message)
            }
            EmailBackend::Mailx(backend) => backend.send_mail(to, &from, cc, bcc, subject, message),
            EmailBackend::Mock(backend) => backend.send_mail(to, &from, cc, bcc, subject, message),
            EmailBackend::Testing(backend) => {
                backend.send_mail(to, &from, cc, bcc, subject, message)
            }
        }
    }

    /// Send an email to the admins, using the configured backed and from address
    pub fn send_mail_to_admins(&self, subject: &str, message: &str) -> Result<(), EmailError> {
        let to_strings: Vec<_> = self.admin_emails.iter().map(|e| e.to_string()).collect();
        let to: Vec<_> = to_strings.iter().map(|s| s.as_str()).collect();
        self.send_mail(to.as_slice(), None, None, subject, message)
    }

    pub fn report_emails_string_list(&self, fall_back_on_admin: bool) -> Vec<String> {
        let emails = self
            .report_emails
            .iter()
            .map(|email| email.to_string())
            .collect_vec();

        if emails.is_empty() && fall_back_on_admin {
            self.admin_emails_string_list()
        } else {
            emails
        }
    }

    pub fn admin_emails_string_list(&self) -> Vec<String> {
        self.admin_emails
            .iter()
            .map(|email| email.to_string())
            .collect_vec()
    }

    pub fn admin_emails_string_list_for_display(&self) -> String {
        self.admin_emails_string_list().join(", ")
    }
}

/// An enum specifying which method to use to send emails
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EmailBackend {
    /// Uses the `Lettre` crate to connect to a SMTP server.
    /// Note that connections to a local server are *unencrypted*, but that
    /// is assumed to be acceptible since the connection is on the local
    /// machine. This is the default. Alternatively, you can authenticate
    /// over a TLS connection using a password, intended for a remote SMTP
    /// host.
    Internal(crate::email::Lettre),

    /// Calls the `mailx` command line client via the shell to send
    /// emails.
    Mailx(crate::email::Mailx),

    /// Prints the email to the terminal (intended for development)
    Mock(crate::email::MockEmail),

    /// Store the emails [`lettre::Message`], or a the string version of an
    /// error from constructing the message, in a queue. Useful for unit/integration
    /// tests to inspect the emails.
    Testing(crate::email::TestingEmail),
}

impl Default for EmailBackend {
    fn default() -> Self {
        Self::Internal(crate::email::Lettre::default())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ErrorHandlerChoice {
    Logging,
    EmailAdmins,
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
            _ => anyhow::bail!("Unknown error handler choice: {s}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlacklistEntry {
    pub identifier: BlacklistIdentifier,
    pub silent: bool,
    pub reason: Option<String>,
}

impl Display for BlacklistEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(reason) = &self.reason {
            write!(f, "{} (reason = '{reason}')", self.identifier)
        } else {
            write!(f, "{}", self.identifier)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BlacklistIdentifier {
    SubmitterEmail { submitter: String },
}

impl Display for BlacklistIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SubmitterEmail { submitter } => write!(f, "submitter email = {submitter}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// A path to a binary file containing a 256 bit key for use with HMAC signing
    pub hmac_secret_file: PathBuf,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            hmac_secret_file: PathBuf::from("./hmac_secret.dat"),
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
where
    T: AsRef<Path>,
{
    // TODO: make a macro that will copy docstring comments from the structs to the file
    // Note: if you get a "values must be emitted before tables" error, it has to do with the
    // ordering of fields. Putting HashMaps at the end seems to help. See
    // https://github.com/toml-rs/toml-rs/issues/142
    let mut default_cfg = Config::default();

    let demo_met_cfg = MetDownloadConfig {
        url_pattern: "".to_string(),
        basename_pattern: Some("(omit to infer from url_pattern)".to_string()),
        file_freq_min: 180,
        earliest_date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        ginput_met_type: GinputMetType::MetEta,
        latest_date: None,
        download_dir: PathBuf::new(),
        days_latency: 1,
    };

    default_cfg.data.base_vmr_file = Some(PathBuf::new());
    default_cfg.data.zgrid_file = Some(PathBuf::new());
    default_cfg
        .data
        .met_download
        .insert(MetCfgKey("geosfpit-met-eta".to_string()), demo_met_cfg);

    default_cfg.default_options = vec![DefaultOptions {
        start_date: NaiveDate::from_ymd_opt(2000, 1, 1),
        end_date: NaiveDate::from_ymd_opt(2038, 1, 1),
        processing_configuration: ProcCfgKey("ginput-v1.0.6-std".to_string()),
    }];

    let sub_queue = default_cfg.execution.submitted_job_queue.clone();
    let std_queue = default_cfg.execution.std_site_job_queue.clone();
    default_cfg.execution.queues.insert(
        sub_queue,
        JobQueueOptions {
            max_num_procs: 4,
            ..Default::default()
        },
    );
    default_cfg.execution.queues.insert(
        std_queue,
        JobQueueOptions {
            max_num_procs: 4,
            ..Default::default()
        },
    );

    default_cfg.execution.ginput.insert(
        GinputCfgKey::from("v1.0.6".to_string()),
        GinputConfig::Script {
            entry_point_path: PathBuf::new(),
        },
    );

    let toml_str = toml::to_string_pretty(&default_cfg)
        .context("Could not convert default config to string")?;
    let mut f = File::create(path).context("Could not create the configuration file.")?;
    f.write_all(toml_str.as_bytes())
        .context("Could not write the configuration file.")?;

    Ok(())
}

/// Load an existing configuration .toml file from `path`.
///
/// Disable validation by passing `false` as the second argument. The
/// TOML file must still deserialize successfully for an `Ok(_)` to be
/// returned.
///
/// # Errors
/// An `Err` is returned if:
///
/// * it could not open the file at `path`
/// * it could not read the contents of `path`
/// * the .toml file could not be decoded or failed validation
///
/// # See also
/// * [`load_env_config_file`]
/// * [`load_config_file_or_default`]
pub fn load_config_file<T>(path: T, validate: bool) -> anyhow::Result<Config>
where
    T: AsRef<Path>,
{
    let mut f = File::open(path).context("Failed to open configuration file.")?;
    let mut toml_str = String::new();
    f.read_to_string(&mut toml_str)?;
    let config: Config = toml::from_str(&toml_str)?;
    if !validate {
        return Ok(config);
    } else if let Err(e) = config.validate() {
        anyhow::bail!("{e}")
    } else {
        Ok(config)
    }
}

pub fn get_env_config_path() -> anyhow::Result<PathBuf> {
    dotenv::dotenv().ok();
    let key = std::env::var(CFG_FILE_ENV_VAR)?;
    return Ok(PathBuf::from(key));
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
pub fn load_env_config_file(validate: bool) -> anyhow::Result<Config> {
    let path = get_env_config_path()?;
    return load_config_file(path, validate);
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
/// let config = load_config_file(path, true).unwrap_or_else(|_| {
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
where
    T: AsRef<Path>,
{
    if let Some(p) = path {
        if p.as_ref().exists() {
            debug!("Reading config file from {}", p.as_ref().display());
            return load_config_file(p.as_ref(), true).with_context(|| {
                format!("Error loading configuration file {}", p.as_ref().display())
            });
        } else {
            debug!(
                "Given config file ({}) does not exist, using default",
                p.as_ref().display()
            );
            return Ok(Config::default());
        }
    } else {
        debug!("No config file path given, using default");
        return Ok(Config::default());
    }
}
