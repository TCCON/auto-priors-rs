use std::{collections::HashMap, path::PathBuf};

use chrono::NaiveDate;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::YesOrNo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// A list of keys under `data.met_download` needed for this set of priors.
    /// If any of the keys listed here are not present in `data.met_download`,
    /// the configuration will fail validation checks.
    pub mets_required: Vec<String>,

    /// Which version of ginput to use in this processing, it must be one of the
    /// keys under `.execution.ginput`.
    pub ginput: String,

    /// The earliest date for which this processing shall be available. With `end_date`,
    /// this sets both (1) the period which users may request this set of priors and (2)
    /// the period for which the met data needed by these priors will be automatically
    /// downloaded.
    pub start_date: NaiveDate,

    /// The latest date (exclusive) for which this processing will be available. If omitted,
    /// that implied that it will be available up to the current date, minus the latency of
    /// the input meteorology. See `start_date` for more information.
    pub end_date: Option<NaiveDate>,

    /// Where to write the tarballs for this product if producing it automatically, or the string
    /// "NO" (case insensitive) to disable automatic generation.
    pub auto_generate_to: YesOrNo<PathBuf>,

    /// The earliest date for which to automatically generate these priors for ALL standard sites.
    /// If omitted, then it will be set equal to `start_date`. Priors will be automatically generated
    /// only if `auto_generate_to` is not "NO".
    auto_start_date: Option<NaiveDate>,

    /// The latest date (exclusive) for which to automatically generate these priors for ALL standard
    /// sites. If omitted, then it will be set equal to `end_date`. Priors will be automatically
    /// generated only if `auto_generate_to` is not "NO".
    auto_end_date: Option<NaiveDate>,

    /// The string that ginput's `mod` subcommand's `mode` argument takes to tell it to produce files
    /// from this meteorology.
    pub ginput_met_key: String,

    /// The top-level subdirectory that `ginput` places output for this met type, e.g. "fpit" for GEOS FP-IT.
    /// If the `ginput_met_key` value is "XXX-eta", then this is usually "XXX".
    pub ginput_output_subdir: String,
}

impl ProcessingConfig {
    /// Return the first date for which to automatically generate these priors.
    /// This method automatically handles the fallback to `start_date` if 
    /// `auto_start_date` was not given.
    pub fn auto_start_date(&self) -> NaiveDate {
        self.auto_start_date.unwrap_or(self.start_date)
    }

    /// Return the last date (exclusive) for which to automatically generate
    /// these priors. This method automatically handles the fallback to `end_date`
    /// if `auto_end_date` was not given. This returns `None` if the priors should
    /// be generated up the current date.
    pub fn auto_end_date(&self) -> Option<NaiveDate> {
        match self.auto_end_date {
            Some(d) => Some(d),
            None => self.end_date,
        }
    }
}

pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, ProcessingConfig>, D::Error>
where D: Deserializer<'de>
{
    super::helpers::deserialize_subfile(deserializer, "processing configuration")
}

pub(super) fn serialize<S>(pc_cfg: &HashMap<String, ProcessingConfig>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer
{
    // I can't think of any way to get the path we are serializing to in order to
    // write the subfile next to it. However, in most cases we should be serializing
    // the default configuration, which will be empty, in which case we just won't create
    // the file at all
    let p = PathBuf::from("processing-configuration.toml");
    if pc_cfg.is_empty() {
        p.serialize(serializer)
    } else {
        super::helpers::serialize_subfile(pc_cfg, serializer, &p, "processing configuration")
    }
}