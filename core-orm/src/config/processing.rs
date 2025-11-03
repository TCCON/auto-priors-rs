use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use chrono::NaiveDate;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    config::{
        ConfigValErrorCause, ConfigValidationError, GinputCfgKey, KeyedMetDownloadConfig,
        MetCfgKey, ProcCfgKey,
    },
    utils::date_range_contains,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// The keys from the met download section of the configuration defining
    /// which met files are required for this configuration to run.
    pub required_mets: Vec<MetCfgKey>,

    /// The key for the ginput section from the execution configuration
    /// to use to run this processing.
    pub ginput: GinputCfgKey,

    /// The earliest date that this configuration may be requested.
    pub start_date: NaiveDate,

    /// The latest date (exclusive) that this configuration may be requested.
    /// If `None`, this configuration is assumed to be open-ended.
    pub end_date: Option<NaiveDate>,

    /// Controls whether this configuration will be generated for the standard sites
    /// automatically.
    pub generate_automatically: bool,

    /// If this configuration will be generated automatically, this defines the
    /// earliest date that will be produced automatically. If omitted, it falls
    /// back to `start_date`.
    auto_start_date: Option<NaiveDate>,

    /// If this configuration will be generated automatically, this defines the
    /// latest date that will be produced automatically. If omitted, it falls
    /// back to `end_date`.
    auto_end_date: Option<NaiveDate>,

    /// If this configuration will be generated automatically, this defines the
    /// root directory where the tarballs will be output. Note that two configurations
    /// that may be produced for the same time must output to different directories,
    /// but two configurations that cannot output the same date may output to the
    /// same directory.
    pub auto_tarball_dir: Option<PathBuf>,

    /// /// The string that ginput's `mod` subcommand's `mode` argument takes to tell it to produce files
    /// from this meteorology.
    pub ginput_met_key: String,

    /// The top-level subdirectory that `ginput` places output for this met type, e.g. "fpit" for GEOS FP-IT.
    /// If the `ginput_met_key` value is "XXX-eta", then this is usually "XXX".
    pub ginput_output_subdir: String,

    /// Set to false to avoid downloading the required mets automatically. Defaults to true when not specified
    /// in a config file.
    #[serde(default = "crate::utils::default_true")]
    pub download_met_automatically: bool,
}

impl ProcessingConfig {
    /// If this configuration will be generated automatically, this defines the
    /// earliest date that will be produced automatically.
    pub fn auto_start_date(&self) -> NaiveDate {
        self.auto_start_date.unwrap_or(self.start_date)
    }

    /// If this configuration will be generated automatically, this defines the
    /// latest date that will be produced automatically. If this returns `None`,
    /// then this configuration should be produced indefinitely.
    pub fn auto_end_date(&self) -> Option<NaiveDate> {
        self.auto_end_date.or(self.end_date)
    }

    pub(super) fn contains_date(&self, date: NaiveDate) -> bool {
        if let Some(end) = self.end_date {
            date >= self.start_date && date < end
        } else {
            date >= self.start_date
        }
    }

    pub fn get_met_configs<'a>(
        &'a self,
        cfg: &'a super::Config,
    ) -> anyhow::Result<Vec<KeyedMetDownloadConfig<'a>>> {
        let mut met_cfgs = vec![];
        for key in self.required_mets.iter() {
            let c = cfg.data.met_download.get(key)
                .ok_or_else(|| anyhow!("Met configuration key '{key}', required by a processing configuration, not found on the parent configuration"))?;
            met_cfgs.push(KeyedMetDownloadConfig {
                product_key: key,
                cfg: c,
            });
        }
        Ok(met_cfgs)
    }

    pub fn describe_date_range(&self) -> String {
        if let Some(end) = self.end_date {
            format!(
                "dates from {} up to but not including {end}",
                self.start_date
            )
        } else {
            format!("dates from {} on", self.start_date)
        }
    }

    // ------------------ //
    // VALIDATION METHODS //
    // ------------------ //

    fn location(my_key: &ProcCfgKey) -> String {
        format!("processing configuration '{my_key}'")
    }

    fn validate(
        &self,
        cfg: &super::Config,
        my_key: &ProcCfgKey,
        errors: &mut ConfigValidationError,
    ) {
        // Check that all the required mets are defined in the parent config
        for met in self.required_mets.iter() {
            if !cfg.data.met_download.keys().any(|key| key == met) {
                errors.push(ConfigValErrorCause::UnknownMetKey {
                    met_key: met.to_string(),
                    processing_key: my_key.to_string(),
                });
            }
        }

        // Check that the required ginput key is in the parent config
        if !cfg.execution.ginput.contains_key(&self.ginput) {
            errors.push(ConfigValErrorCause::UnknownGinputKey {
                key: self.ginput.clone(),
                location: Self::location(my_key),
            });
        }

        // Check that the date ranges are sane and inside the dates for which
        // the met products should be available
        self.check_date_ranges(cfg, my_key, errors);

        // Check that this has an output directory if generating automatically.
        if self.generate_automatically && self.auto_tarball_dir.is_none() {
            errors.push(ConfigValErrorCause::MissingPath(format!(
                "automatically-generating {} auto_tarball_dir",
                Self::location(my_key)
            )));
        }
    }

    fn check_date_ranges(
        &self,
        cfg: &super::Config,
        my_key: &ProcCfgKey,
        errors: &mut ConfigValidationError,
    ) {
        // Check that defined date ranges have the start dates first
        if self.end_date.is_some_and(|d| d <= self.start_date) {
            errors.push(ConfigValErrorCause::DateRangeInverted {
                location: format!("{} (start_date and end_date)", Self::location(my_key)),
            });
        }

        if self
            .auto_end_date()
            .is_some_and(|d| d < self.auto_start_date())
        {
            errors.push(ConfigValErrorCause::DateRangeInverted {
                location: format!("{} (automatic start and end dates)", Self::location(my_key)),
            });
        }

        // Check that the start and end dates are within the range for which
        // the met files are available. Assume that if get_met_configs errors,
        // it is because of an unknown met key that is handled elsewhere in
        // validation.
        if let Ok(met_cfgs) = self.get_met_configs(cfg) {
            if !met_cfgs.is_empty() {
                let last_met_start_date = met_cfgs
                    .iter()
                    .map(|c| c.cfg.earliest_date)
                    .max()
                    .expect("should be at least one met defined");
                let first_met_end_date = met_cfgs.iter().filter_map(|c| c.cfg.latest_date).min();

                if !date_range_contains(
                    Some(last_met_start_date),
                    first_met_end_date,
                    Some(self.start_date),
                    self.end_date,
                ) {
                    errors.push(ConfigValErrorCause::Other(format!("The start and/or end dates for processing '{my_key}' extend outside at least one of its required mets")));
                }
                if !date_range_contains(
                    Some(last_met_start_date),
                    first_met_end_date,
                    Some(self.auto_start_date()),
                    self.auto_end_date(),
                ) {
                    errors.push(ConfigValErrorCause::Other(format!("The automatic start and/or end dates for processing '{my_key}' extend outside at least one of its required mets")));
                }
            }
        }
    }

    fn output_conficts(&self, other: &Self) -> bool {
        if self.tar_path_for_comparison() == other.tar_path_for_comparison() {
            let overlap_class = crate::utils::DateRangeOverlap::classify(
                Some(self.auto_start_date()),
                self.auto_end_date(),
                Some(other.auto_start_date()),
                other.auto_end_date(),
            );
            if overlap_class.has_overlap() {
                return true;
            }
        }
        false
    }

    fn tar_path_for_comparison(&self) -> Option<Cow<'_, Path>> {
        match self.auto_tarball_dir.as_deref().map(|p| p.canonicalize()) {
            Some(Ok(p)) => Some(Cow::Owned(p)),
            Some(Err(e)) => {
                log::warn!(
                    "Could not canonicalize auto tarball path '{:?}' for comparison ({e})",
                    self.auto_tarball_dir.as_deref().map(|p| p.display())
                );
                self.auto_tarball_dir.as_deref().map(|p| Cow::Borrowed(p))
            }
            None => None,
        }
    }
}

pub(super) fn validate_processing_configs(cfg: &super::Config) -> ConfigValidationError {
    let mut errors = ConfigValidationError::default();
    for (processing_key, processing_cfg) in cfg.processing_configuration.iter() {
        processing_cfg.validate(cfg, &processing_key, &mut errors);
    }
    check_for_conflicting_output_paths(&cfg.processing_configuration, &mut errors);
    errors
}

fn check_for_conflicting_output_paths(
    proc_cfgs: &HashMap<ProcCfgKey, ProcessingConfig>,
    errors: &mut ConfigValidationError,
) {
    for ((k1, proc1), (k2, proc2)) in proc_cfgs.iter().tuple_combinations() {
        if proc1.output_conficts(proc2) {
            errors.push(ConfigValErrorCause::ProcCfgConflict {
                key1: k1.to_string(),
                key2: k2.to_string(),
            });
        }
    }
}
