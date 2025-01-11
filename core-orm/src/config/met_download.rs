use std::{collections::HashMap, fmt::Display, path::PathBuf};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::met;

/// Configuration for how to download input reanalysis files for ginput
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetDownloadConfig {
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

    /// The latest date (exclusive) for which this met is available. If omitted,
    /// it is assumed that it will be kept up to date with the normal latency
    /// defined by `days_latency`.
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

    /// How many days to allow before failing to download files is an error
    pub days_latency: u32,
}

impl Display for MetDownloadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "product = {}, type = {}, levels = {}", self.product, self.data_type, self.levels)
    }
}

impl MetDownloadConfig {
    pub fn to_short_string(&self) -> String {
        format!("{}, {}, {}", self.product, self.data_type, self.levels)
    }
    
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

pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, MetDownloadConfig>, D::Error>
where D: Deserializer<'de>
{
    super::helpers::deserialize_subfile(deserializer, "met download configuration")
}

pub(super) fn serialize<S>(pc_cfg: &HashMap<String, MetDownloadConfig>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer
{
    // I can't think of any way to get the path we are serializing to in order to
    // write the subfile next to it. However, in most cases we should be serializing
    // the default configuration, which will be empty, in which case we just won't create
    // the file at all
    let p = PathBuf::from("met-download-configuration.toml");
    if pc_cfg.is_empty() {
        p.serialize(serializer)
    } else {
        super::helpers::serialize_subfile(pc_cfg, serializer, &p, "met download configuration")
    }
}