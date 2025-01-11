use std::{path::{PathBuf, Path}, str::FromStr, fmt::Display};

use anyhow::Context;
use chrono::{NaiveDateTime, NaiveDate};
use log::{warn, debug, trace, info};
use serde::{Deserialize, Serialize};
use sqlx::{self, Type, FromRow};

use crate::{MySqlConn, config, error::DefaultOptsQueryError};

/// Indicates a problem adding a met file to the database
#[derive(Debug)]
pub enum AddMetFileError {
    /// Indicates the path to the file given (which is also contained in this variant) is not present on disk
    FileDoesNotExist(PathBuf),

    /// Indicates that the met file was already present in the database (all characteristics matched)
    FileAlreadyInDb(PathBuf),

    /// Indicates that there is already an entry for this file path in the database, but one or more of the
    /// characteristics (datetime, levels, data type, or product) does not match.
    FileCharacteristicMismatch(PathBuf),

    /// Indicates an uncategorized error (e.g. a database query failure)
    Other(anyhow::Error)
}

impl From<anyhow::Error> for AddMetFileError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value)
    }
}

impl Display for AddMetFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddMetFileError::FileDoesNotExist(p) => write!(f, "Cannot add file {} to met file database, file does not exist on disk", p.display()),
            AddMetFileError::FileAlreadyInDb(p) => write!(f, "Cannot add file {} to met file database, file path already present", p.display()),
            AddMetFileError::FileCharacteristicMismatch(p) => write!(f, "File {} is already in the met database, but with different characteristics", p.display()),
            AddMetFileError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AddMetFileError {}


#[derive(Debug)]
pub enum CheckMetAvailableError {
    NoDefaultsDefined(NaiveDate),
    Other(anyhow::Error)
}

impl From<anyhow::Error> for CheckMetAvailableError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value)
    }
}

impl From<DefaultOptsQueryError> for CheckMetAvailableError {
    fn from(value: DefaultOptsQueryError) -> Self {
        if let DefaultOptsQueryError::NoMatches(date) = value {
            Self::NoDefaultsDefined(date)
        } else {
            Self::Other(value.into())
        }
    }
}

impl Display for CheckMetAvailableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckMetAvailableError::NoDefaultsDefined(date) => write!(f, "No default meteorology defined for {date}"),
            CheckMetAvailableError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CheckMetAvailableError {}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MetDayState {
    Complete,
    Incomplete(i64, i64),
    Missing
}

impl AsRef<str> for MetDayState {
    fn as_ref(&self) -> &str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete(_, _) => "incomplete",
            Self::Missing => "missing"
        }
    }
}

impl Display for MetDayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetDayState::Complete => write!(f, "complete"),
            MetDayState::Incomplete(found, expected) => write!(f, "incomplete ({found}/{expected}"),
            MetDayState::Missing => write!(f, "missing"),
        }
    }
}

impl MetDayState {
    pub fn is_complete(&self) -> bool {
        match self {
            Self::Complete => true,
            Self::Incomplete(_, _) | Self::Missing => false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String", into = "String")]
pub enum MetProduct {
    GeosFp,
    GeosFpit,
    GeosIt,
    Other(String)
}

impl Into<String> for MetProduct {
    fn into(self) -> String {
        format!("{}", self)
    }
}


impl From<String> for MetProduct {

    fn from(value: String) -> Self {
        Self::from_str(value.as_str()).unwrap_or_else(|_| Self::Other(value))
    }
}

impl FromStr for MetProduct {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "geosfp" => Ok(Self::GeosFp),
            "geosfpit" => Ok(Self::GeosFpit),
            "geosit" => Ok(Self::GeosIt),
            _ => anyhow::bail!("Unknown string value for GeosProduct enum: {s}")
        }
    }
}

impl Display for MetProduct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GeosFp => write!(f, "geosfp"),
            Self::GeosFpit => write!(f, "geosfpit"),
            Self::GeosIt => write!(f, "geosit"),
            Self::Other(v) => write!(f, "other({v})")
        }
    }
}

#[derive(Debug, Type, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String", into = "String")]
pub enum MetLevels {
    Pres,
    Surf,
    Eta,
    Unknown
}

impl MetLevels {
    pub fn standard_subdir(&self) -> PathBuf {
        match self {
            Self::Pres => PathBuf::from("Np"),
            Self::Surf => PathBuf::from("Nx"),
            Self::Eta => PathBuf::from("Nv"),
            Self::Unknown => PathBuf::from("UNKNOWN")
        }
    }
}

impl Into<String> for MetLevels {
    fn into(self) -> String {
        format!("{}", self)
    }
}

// impl TryFrom<String> for MetLevels {
//     type Error = anyhow::Error;

//     fn try_from(value: String) -> Result<Self, Self::Error> {
//         Self::from_str(value.as_str())
//     }
// }

impl FromStr for MetLevels {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "pres" => Ok(Self::Pres),
            "surf" => Ok(Self::Surf),
            "eta" => Ok(Self::Eta),
            _ => anyhow::bail!("Unknown string value for MetLevels: {s}")
        }
    }
}

impl From<String> for MetLevels {
    fn from(value: String) -> Self {
        Self::from_str(&value).unwrap_or(Self::Unknown)
    }
}

impl Display for MetLevels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pres => "pres",
            Self::Surf => "surf",
            Self::Eta => "eta",
            Self::Unknown => "UNKNOWN"
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String", into = "String")]
pub enum MetDataType {
    Met,
    Chm,
    Other(String)
}

impl Into<String> for MetDataType {
    fn into(self) -> String {
        format!("{}", self)
    }
}

impl From<String> for MetDataType {
    fn from(value: String) -> Self {
        Self::from_str(value.as_str()).unwrap_or_else(|_| Self::Other(value))
    }
}

impl FromStr for MetDataType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "met" => Ok(Self::Met),
            "chm" => Ok(Self::Chm),
            _ => anyhow::bail!("Unknown string value for MetDataType: {s}")
        }
    }
}

impl Display for MetDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Met => write!(f, "met"),
            Self::Chm => write!(f, "chm"),
            Self::Other(v) => write!(f, "other({v})")
        }
    }
}

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct MetFile {
    pub file_id: i32,
    #[sqlx(try_from = "String")]
    pub file_path: PathBuf,
    #[allow(dead_code)]
    pub file_path_sha256: Option<String>,
    #[sqlx(try_from = "String")]
    pub product: MetProduct,
    pub filedate: NaiveDateTime,
    #[sqlx(try_from = "String")]
    pub levels: MetLevels,
    #[sqlx(try_from = "String")]
    pub data_type: MetDataType,
}

impl MetFile {
    /// Returns the number of met files expected per day, based on the configuration
    /// 
    /// Will error if the frequency specified in the configuration does not divide evenly
    /// into a day (e.g. if the files are provided every 300 minutes)
    fn num_expected_daily_files(cfg: &config::MetDownloadConfig) -> anyhow::Result<i64> {
        if 1440 % cfg.file_freq_min != 0 {
            let remainder = 1440 % cfg.file_freq_min;
            let msg = format!("A met configuration has a file frequency that does not evenly divide per day, remaining minutes were {remainder} ({cfg})");
            return Err(anyhow::Error::msg(msg))
        }

        Ok(1440 / cfg.file_freq_min)
    }

    pub async fn get_first_complete_date_for_config(conn: &mut MySqlConn, cfg: &config::MetDownloadConfig) -> anyhow::Result<Option<NaiveDate>> {
        let n_expected = Self::num_expected_daily_files(cfg)?;

        trace!("Querying first complete date ({n_expected} files) for {}, {}, {}", cfg.levels, cfg.data_type, cfg.product);
        let this_min_date = sqlx::query!(
            r#"SELECT MIN(tbl.date) as min_date
                FROM (
                    SELECT DATE(filedate) AS date,COUNT(filedate) AS count
                    FROM MetFiles
                    WHERE levels = ? AND data_type = ? AND product = ?
                    GROUP BY DATE(filedate)
                ) AS tbl
                WHERE tbl.count = ?"#,
            cfg.levels.to_string(),
            cfg.data_type.to_string(),
            cfg.product.to_string(),
            n_expected
        ).fetch_one(conn)
        .await?
        .min_date;

        Ok(this_min_date)
    }

    /// Given a configuration for downloading reanalysis data, find the last date for which that data was downloaded
    /// 
    /// This is most useful for figuring out  data needs downloaded. To figure out if all the different data sets
    /// (different levels, variables, etc.) needed to actually run the priors for a day are available, use
    /// [`get_last_complete_date_for_config_set`]
    /// 
    /// # Returns
    /// The last date for which that data was downloaded. Returns `None` if that data has never been downloaded.
    /// Returns an `Err` if querying the database fails.
    pub async fn get_last_complete_date_for_config(conn: &mut MySqlConn, cfg: &config::MetDownloadConfig) -> anyhow::Result<Option<NaiveDate>> {
        let n_expected = Self::num_expected_daily_files(cfg)?;

        trace!("Querying most recent complete date ({n_expected} files) for {}, {}, {}", cfg.levels, cfg.data_type, cfg.product);
        let this_max_date = sqlx::query!(
            r#"SELECT MAX(tbl.date) as max_date
                FROM (
                    SELECT DATE(filedate) AS date,COUNT(filedate) AS count
                    FROM MetFiles
                    WHERE levels = ? AND data_type = ? AND product = ?
                    GROUP BY DATE(filedate)
                ) AS tbl
                WHERE tbl.count = ?"#,
            cfg.levels.to_string(),
            cfg.data_type.to_string(),
            cfg.product.to_string(),
            n_expected
        ).fetch_one(conn)
        .await?
        .max_date;

        Ok(this_max_date)
    }

    /// Given a list of reanalysis download configurations, find the first or last date where all the data sets were downloaded.
    /// 
    /// This is meant for finding the first or last date that the priors can be generated for. To figure out the first or last date a
    /// specific reanalysis data set was downloaded for, use [`get_first_complete_date_for_config`] or [`get_last_complete_date_for_config`].
    /// 
    /// # Returns
    /// The most recent date for which all the datasets specified by `cfgs` are complete. There can be several cases:
    /// 
    /// 1. If none of those datasets have any data downloaded, returns `None` 
    /// 2. If some (but not all) of those datasets have data downloaded, still returns `None` but prints a warning
    /// 3. If all those datasets have data downloaded, but the start or end dates differ, returns the latest start date or earliest 
    ///    end date and prints a warning.
    /// 4. If all those datasets have same start or end date, return that date.
    /// 
    /// This will return an `Err` if the database query fails.
    pub async fn get_first_or_last_complete_date_for_config_set(conn: &mut MySqlConn, cfgs: &[config::MetDownloadConfig], first: bool) -> anyhow::Result<Option<NaiveDate>> {
        let mut dates = vec![];
        for cfg in cfgs {
            let (descr, opt_date) = if first {
                ("First", Self::get_first_complete_date_for_config(conn, cfg).await?)
            } else {
                ("Last", Self::get_last_complete_date_for_config(conn, cfg).await?)
            };

            if let Some(d) = opt_date {
                debug!("{descr} complete day for {cfg} was {d}");
                dates.push(d);
            }else{
                debug!("No complete days found for {cfg}");
            }
        }

        // Case 1: everything returned None, there is no "last date"
        if dates.len() == 0 {
            return Ok(None)
        }

        // Case 2: something returned None, so we need to return None, but issue warning that things are inconsistent
        if dates.len() != cfgs.len() {
            warn!("While trying to identify the last complete date of meteorology, some required products had existing data and others did not.");
            return Ok(None)
        }

        // Case 3: not all of the dates are the same so issue a warning and return the earliest/latest date
        // Case 4: all products have the same first/last time
        // We know that there is at least one date so this is okay to unwrap
        if first {
            let latest_first_date = dates.iter().max().unwrap().to_owned();
            if !dates.iter().all(|&d| d == latest_first_date) {
                warn!("While trying to identify the first complete date of meteorology, the required products had different initial dates, so defaulting to the latest.");
            }
            Ok(Some(latest_first_date))
        } else {
            let earliest_last_date = dates.iter().min().unwrap().to_owned();
            if !dates.iter().all(|&d| d == earliest_last_date) {
                warn!("While trying to identify the last complete date of meteorology, the required products had different final dates, so defaulting to the earliest.");
            }
            return Ok(Some(earliest_last_date));
        }

    }

    /// Get the most first date for which the meteorology files expected based on the default options are all available.
    /// 
    /// Because different time periods may use different meteorology, figuring out the most first day for which we can generate
    /// priors requires knowing which met files to check for. This function uses the defined default options to check for the first
    /// day with all the needed met files.
    /// 
    /// # Returns
    /// - `Ok(Some(date))` if it finds a date with all the needed met files
    /// - `Ok(None)` if no dates have all the needed met files
    /// - `Err` if any database queries fail or any of the default option sets defined in the configuration overlap in time.
    pub async fn get_first_complete_day_for_default_mets(conn: &mut MySqlConn, cfg: &config::Config) -> anyhow::Result<Option<NaiveDate>> {
        let option_sets = cfg.get_all_defaults_check_overlap()?;
        // Since these are date-ordered and do not overlap, we know we can start from the first set and check for complete met data
        let today = chrono::Utc::now().date_naive();
        for (iopt, options) in option_sets.iter().enumerate() {
            if iopt == 0 && options.start_date.map(|d| d > today).unwrap_or(false) {
                warn!("First default set {options} has a start date in the future, no complete day will be determined for any met.")
            }
            let met_key = &options.met;
            let met_configs = cfg.get_met_configs(met_key)?;
            if let Some(first_date) = Self::get_first_or_last_complete_date_for_config_set(conn, met_configs, true).await? {
                return Ok(Some(first_date))
            }
        }

        Ok(None)
    }

    /// Get the most recent date for which the meteorology files expected based on the default options are all available.
    /// 
    /// Because different time periods may use different meteorology, figuring out the most recent day for which we can generate
    /// priors requires knowing which met files to check for. This function uses the defined default options to check for the most
    /// recent day with all the needed met files.
    /// 
    /// # Returns
    /// - `Ok(Some(date))` if it finds a date with all the needed met files
    /// - `Ok(None)` if no dates have all the needed met files
    /// - `Err` if any database queries fail or any of the default option sets defined in the configuration overlap in time.
    pub async fn get_last_complete_date_for_default_mets(conn: &mut MySqlConn, cfg: &config::Config) -> anyhow::Result<Option<NaiveDate>> {
        let option_sets = cfg.get_all_defaults_check_overlap()?;
        // Since these are date-ordered and do not overlap, we know we can start from the last set and check for complete met data
        // However, if the start date for a given set remains in the future, we shouldn't count it.
        let today = chrono::Utc::now().date_naive();
        for options in option_sets.iter().rev() {
            if options.start_date.map(|d| d > today).unwrap_or(false) {
                debug!("Default set {options} starts in the future, not considering it when determining last complete date for met files");
                continue;
            }
            let met_key = &options.met;
            let met_configs = cfg.get_met_configs(met_key)?;
            if let Some(last_date) = Self::get_first_or_last_complete_date_for_config_set(conn, met_configs, false).await? {
                return Ok(Some(last_date))
            }
        }

        Ok(None)
    }

    pub async fn is_date_complete_for_default_mets(conn: &mut MySqlConn, cfg: &config::Config, date: NaiveDate) -> Result<MetDayState, CheckMetAvailableError> {
        let opts = cfg.get_defaults_for_date(date)?;
        let met_opts = cfg.get_met_configs(&opts.met)?;
        Ok(Self::is_date_complete_for_config_set(conn, date, met_opts).await?)
    }

    /// Returns whether a given date is complete, incomplete, or wholly missing for a given reanalysis download configuration.
    /// 
    /// Note that this only checks a single set of files, e.g. the 2D met files for GEOS FP-IT or GEOS IT. Assume that a met
    /// dataset may require multiple files for a day to be ready for priors generation. For GEOS for example, we need the 
    /// 2D assimilated met, 3D assimilated met, and 3D chemistry files. To check that, use [`MetFile::is_date_complete_for_config_set`].
    /// 
    /// Will return an `Err` if the database query fails.
    pub async fn is_date_complete_for_config(conn: &mut MySqlConn, date: NaiveDate, cfg: &config::MetDownloadConfig) -> anyhow::Result<MetDayState> {
        let n_expected = Self::num_expected_daily_files(cfg)?;
        let n_found = sqlx::query!(
            r#"SELECT COUNT(filedate) as count FROM MetFiles
               WHERE DATE(filedate) = ? and levels = ? AND data_type = ? AND product = ?"#,
            date,
            cfg.levels.to_string(),
            cfg.data_type.to_string(),
            cfg.product.to_string()
        ).fetch_one(conn)
        .await?
        .count;

        debug!(
            "Checked met (levels = {}, data type = {}, product = {}) files for {date}: expected {n_expected}, found {n_found}",
            cfg.levels, cfg.data_type, cfg.product
        );

        if n_found == 0 {
            Ok(MetDayState::Missing)
        }else if n_found < n_expected {
            Ok(MetDayState::Incomplete(n_found, n_expected))
        }else{
            Ok(MetDayState::Complete)
        }
    }

    /// Returns whether a given date has all of the met files needed for a given set of configurations.
    /// 
    /// This method should be preferred over [`MetFile::is_date_complete_for_config`] if you just need to know whether we have all
    /// the met files of a certain type needed to generate priors for a given day. 
    /// 
    /// # Returns
    /// 
    /// If there is an error connecting to the database, this returns an `Err`. Otherwise, this returns `MetDayState::Complete` if 
    /// all the necessary met files are in the database, `MetDayState::Missing` if none of the met files are present, and 
    /// `MetDayState::Incomplete` otherwise (even if only one of several file sets is incomplete).
    pub async fn is_date_complete_for_config_set(conn: &mut MySqlConn, date: NaiveDate, cfgs: &[config::MetDownloadConfig]) -> anyhow::Result<MetDayState> {
        let mut states = vec![];
        let mut num_expected = vec![];
        for cfg in cfgs {
            let this_state = Self::is_date_complete_for_config(conn, date, cfg).await?;
            debug!("Met {cfg} {date} -> {this_state:?}");
            states.push(this_state);
            num_expected.push(Self::num_expected_daily_files(cfg)?);
        }

        if states.iter().all(|&s| s == MetDayState::Complete) {
            Ok(MetDayState::Complete)
        } else if states.iter().all(|&s| s == MetDayState::Missing) {
            Ok(MetDayState::Missing)
        } else {
            let (total_found, total_expected) = states.iter().zip(num_expected.into_iter())
                .fold((0i64, 0i64), |mut acc, el| {
                    match el.0 {
                        MetDayState::Complete => acc.0 += el.1, // complete day, add the number expected to the number found
                        MetDayState::Incomplete(found, _) => acc.0 += found,
                        MetDayState::Missing => (), // missing day, add nothing to found
                    }
                    // Assume that the second integer in Incomplete will match the number expected (it should)
                    acc.1 += el.1;
                    acc
                });
            Ok(MetDayState::Incomplete(total_found, total_expected))
        }
    }

    /// Get the [`MetFile`] instance for a met file from the database with the basename `filename`
    /// 
    /// # Returns
    /// - `Ok(Some(MetFile))` if it finds exactly one file with the basename `filename`
    /// - `Ok(None)` if it finds no file with that basename
    /// - `Err` if the database query fails or there is >1 file with that basename.
    /// 
    /// # See also
    /// [`get_file_by_full_path`] if you have a full path to a met file that you want information on.
    pub async fn get_file_by_name(conn: &mut MySqlConn, filename: &str) -> anyhow::Result<Option<MetFile>> {
        let mut file = sqlx::query_as!(
            MetFile,
            "SELECT * FROM MetFiles WHERE file_path LIKE ?",
            format!("%{filename}")
        ).fetch_all(conn).await?;

        if file.is_empty() {
            Ok(None)
        } else if file.len() == 1 {
            Ok(file.pop())
        } else {
            anyhow::bail!("Multiple files matched the name {filename}")
        }
    }

    /// Get the [`MetFile`] instance for a met file from the database with the full path `path`
    /// 
    /// # Returns
    /// - `Ok(Some(MetFile))` if it finds exactly one file with the path `path`
    /// - `Ok(None)` if it finds no file with that path. Note that this can happen if you give a
    ///   different path to the file than the one stored in the database (i.e. through links)
    /// - `Err` if the database query fails or there is >1 file with that path. Note that the latter
    ///   case should not happen since the database has a UNIQUE constraint on the file path hashes.
    /// 
    /// # See also
    /// [`get_file_by_name`] if you only have the basename of the file.
    pub async fn get_file_by_full_path(conn: &mut MySqlConn, path: &Path) -> anyhow::Result<Option<MetFile>> {
        let path = path.to_string_lossy();
        let mut file = sqlx::query_as!(
            MetFile,
            "SELECT * FROM MetFiles WHERE file_path = ?",
            path
        ).fetch_all(conn).await?;

        if file.is_empty() {
            Ok(None)
        } else if file.len() == 1 {
            Ok(file.pop())
        } else {
            anyhow::bail!("Multiple files matched the name {path}")
        }
    }

    /// Get a vector of [`MetFile`] instances representing downloaded met files in the database
    /// 
    /// All met files in the database with file datetimes between `start_date` (inclusive) and `end_date`
    /// (exclusive). If `met_product` is not None, then only files for that product are returned. Otherwise
    /// all files with file dates between those times are returned.
    /// 
    /// This function will return an error if the database query fails.
    pub async fn get_files_by_dates(conn: &mut MySqlConn, start_date: NaiveDate, end_date: NaiveDate, met_product: Option<MetProduct>) -> anyhow::Result<Vec<MetFile>> {
        let files = if let Some(prod) = met_product {
            sqlx::query_as!(
                MetFile,
                "SELECT * From MetFiles WHERE filedate >= ? AND filedate < ? AND product = ?",
                start_date,
                end_date,
                prod.to_string()
            ).fetch_all(conn)
            .await?
        } else {
            sqlx::query_as!(
                MetFile,
                "SELECT * From MetFiles WHERE filedate >= ? AND filedate < ?",
                start_date,
                end_date,
            ).fetch_all(conn)
            .await?
        };
        
        Ok(files)
    }

    /// Add a new met file to the database
    /// 
    /// The file must exist at the path given; if not, returns an error.
    /// 
    /// # Inputs
    /// * `conn` - connection to the database
    /// * `file` - path to the file being added. Must be an absolute path, recommend always using 
    ///   [`config::DownloadConfig::get_save_dir`] to get the canonical save directory path.
    /// * `datetime` - the datetime of the data in the file.
    /// * `download_cfg` - the configuration section that specififies how to download these files,
    ///   used to get the product, levels, data type, etc. 
    /// 
    /// # Returns
    /// Returns an `Err` if the file does not exist or the insert in the database fails. 
    /// 
    /// # Panics
    /// Panics if `file` is not an absolute path.
    /// 
    /// # See also
    /// [`MetFile::add_met_file_infer_date`] if the file date must be retrieved from the file name.
    pub async fn add_met_file(conn: &mut MySqlConn, file: &Path, datetime: NaiveDateTime, download_cfg: &config::MetDownloadConfig) -> Result<(), AddMetFileError> {
        if !file.exists() {
            return Err(AddMetFileError::FileDoesNotExist(file.to_path_buf()));
        }else if !file.is_absolute() {
            // I decided to make this a panic rather than a recoverable error because this should be something
            // in the program design, not a runtime issue.
            panic!("Given file path ({}) must be absolute", file.display());
        }

        let file_str = file.to_str().ok_or_else(|| anyhow::Error::msg(format!("Unable to convert path to UTF-8 string: {}", file.display())))?;

        let extant_record = sqlx::query_as!(
            MetFile,
            "SELECT * FROM MetFiles WHERE file_path = ?",
            file_str
        ).fetch_optional(&mut *conn)
        .await
        .with_context(|| format!("Error occurred checking if {} is already present in the MetFiles table", file.display()))?;

        if let Some(record) = extant_record {
            if datetime != record.filedate ||
               download_cfg.product != record.product ||
               download_cfg.levels != record.levels ||
               download_cfg.data_type != record.data_type 
            {
                // For now, I'm considering this an error. If we've downloaded the same file, it should have the
                // same characteristics.
                return Err(AddMetFileError::FileCharacteristicMismatch(file.to_path_buf()))
            } 
            else 
            {
                return Err(AddMetFileError::FileAlreadyInDb(file.to_path_buf()))
            }

        }

        // TODO: make a method to insert a new metfile, use it here and update export::import_db_inner
        sqlx::query!(
            "INSERT INTO MetFiles (file_path, filedate, product, levels, data_type) VALUES (?, ?, ?, ?, ?)",
            file_str,
            datetime,
            download_cfg.product.to_string(),
            download_cfg.levels.to_string(),
            download_cfg.data_type.to_string()
        ).execute(conn)
        .await
        .with_context(|| format!("Error occurred trying to insert {} into MetFiles table", file.display()))?;
        
        Ok(())
        
    }

    /// Get the date of a file from its file name. File name must contain at least up to minutes.
    /// 
    /// Returns the file datetime, or an `Err` if it could not get the chrono format for the file names
    /// or if the parsing of the file name fails.
    fn date_from_filename(file: &Path, download_cfg: &config::MetDownloadConfig) -> anyhow::Result<NaiveDateTime> {
        let basename = file.file_name()
            .ok_or_else(|| anyhow::Error::msg(format!("No base name for file {}", file.display())))?
            .to_string_lossy();

        let date_fmt = download_cfg.get_basename_pattern()?;
        trace!("Trying to get time from {basename} with format {date_fmt}");
        // There is a limitation in v0.4 of chrono that it cannot parse strings that don't at least go up to minutes
        // An issue exists on this topic (https://github.com/chronotope/chrono/issues/191) but there doesn't seem to
        // have been much movement since 2019.
        Ok(NaiveDateTime::parse_from_str(&basename, date_fmt)?)
    }

    /// Similar to [`MetFile::add_met_file`], but infers the date & time from the file name.
    /// 
    /// Note that the file's basename must match the time format pattern in the download config, and
    /// must contain time components at least up to minutes. All other behavior follows
    /// [`MetFile::add_met_file`] including panics - `file` must be an absolute path.
    pub async fn add_met_file_infer_date(conn: &mut MySqlConn, file: &Path, download_cfg: &config::MetDownloadConfig) -> Result<(), AddMetFileError> {
        let datetime = Self::date_from_filename(file, download_cfg)?;
        Self::add_met_file(conn, file, datetime, download_cfg).await
    }

    /// Check whether a given file is already in the database based on what data it has
    /// 
    /// This checks if a row already exists in the database that has the file datetime, product,
    /// levels, and data type specified in the `file` and `download_cfg`. It does *not* check the
    /// filename itself, with the intent that this avoids issues of different paths pointing to the
    /// same file (e.g. due to symlinks).
    /// 
    /// # Returns
    /// A boolean, true if the file is already in the database. It returns an `Err` if the file datetime
    /// couldn't be inferred from the file name (either because the file name and time format pattern didn't
    /// match or the file name/pattern didn't have all the needed time components) or if the database query 
    /// fails.
    pub async fn file_exists_by_type(conn: &mut MySqlConn, file: &Path, file_cfg: &config::MetDownloadConfig) -> anyhow::Result<bool> {
        let datetime = Self::date_from_filename(file, file_cfg)?;

        let n = sqlx::query!(
            r#"SELECT COUNT(*) as count FROM MetFiles
               WHERE filedate = ? AND product = ? AND levels = ? and data_type = ?"#,
            datetime,
            file_cfg.product.to_string(),
            file_cfg.levels.to_string(),
            file_cfg.data_type.to_string()
        ).fetch_one(conn)
        .await?
        .count;

        Ok(n > 0)
    }

    /// Delete the met file represented by this instance from both the file system and the database.
    /// 
    /// This function will return an error if the file cannot be deleted or the database delete query fails.
    /// 
    /// # Notes
    /// 1. It is *not* an error if the file does not exist; this method will still try to remove the database entry.
    ///    This way you can use this function to also clean up database entries for met files missing from the file system.
    ///    You will see a warning in this case.
    /// 2. If the database query fails, the met file will not be deleted. This way a user can either delete it manually
    ///    or re-add it to the database. 
    pub async fn delete_me(&self, conn: &mut MySqlConn) -> anyhow::Result<()> {
        sqlx::query!("DELETE FROM MetFiles WHERE file_id = ?", self.file_id)
            .execute(conn)
            .await
            .with_context(|| format!("Failed to delete MetFile row {}, either delete the file ({}) manually or re-add it to the database.", self.file_id, self.file_path.display()))?;

        info!("Deleted MetFile row {}", self.file_id);

        if self.file_path.exists() {
            std::fs::remove_file(&self.file_path)
                .with_context(|| format!("Failed to deleted met file at {}", self.file_path.display()))?;
            info!("Deleted {}", self.file_path.display());
        } else {
            warn!("Met file {} does not exist, nothing to delete.", self.file_path.display());
        }

        Ok(())
    }
}