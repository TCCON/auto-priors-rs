use std::{path::{PathBuf, Path}, str::FromStr, fmt::Display};

use chrono::{NaiveDateTime, NaiveDate};
use log::{warn, debug, trace};
use serde::{Deserialize, Serialize};
use sqlx::{self, Type, FromRow};

use crate::{MySqlConn, config};

const REQ_FILES_PER_DAY: i64 = 8;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum GeosDayState {
    Complete,
    Incomplete,
    Missing
}

impl AsRef<str> for GeosDayState {
    fn as_ref(&self) -> &str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete => "incomplete",
            Self::Missing => "missing"
        }
    }
}

#[derive(Debug, Type, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum GeosProduct {
    Fp,
    Fpit
}

impl Into<String> for GeosProduct {
    fn into(self) -> String {
        format!("{}", self)
    }
}


impl TryFrom<String> for GeosProduct {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(value.as_str())
    }
}

impl FromStr for GeosProduct {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "fp" => Ok(Self::Fp),
            "fpit" => Ok(Self::Fpit),
            _ => anyhow::bail!("Unknown string value for GeosProduct enum: {s}")
        }
    }
}

impl Display for GeosProduct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Fp => "fp",
            Self::Fpit => "fpit"
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, Type, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum GeosLevels {
    Pres,
    Surf,
    Eta
}

impl GeosLevels {
    pub fn standard_subdir(&self) -> PathBuf {
        match self {
            Self::Pres => PathBuf::from("Np"),
            Self::Surf => PathBuf::from("Nx"),
            Self::Eta => PathBuf::from("Nv")
        }
    }
}

impl Into<String> for GeosLevels {
    fn into(self) -> String {
        format!("{}", self)
    }
}

impl TryFrom<String> for GeosLevels {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(value.as_str())
    }
}

impl FromStr for GeosLevels {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "pres" => Ok(Self::Pres),
            "surf" => Ok(Self::Surf),
            "eta" => Ok(Self::Eta),
            _ => anyhow::bail!("Unknown string value for GeosLevels: {s}")
        }
    }
}

impl Display for GeosLevels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pres => "pres",
            Self::Surf => "surf",
            Self::Eta => "eta"
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, Type, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum GeosDataType {
    Met,
    Chm
}

impl Into<String> for GeosDataType {
    fn into(self) -> String {
        format!("{}", self)
    }
}

impl TryFrom<String> for GeosDataType {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(value.as_str())
    }
}

impl FromStr for GeosDataType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "met" => Ok(Self::Met),
            "chm" => Ok(Self::Chm),
            _ => anyhow::bail!("Unknown string value for GeosDataType: {s}")
        }
    }
}

impl Display for GeosDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Met => "met",
            Self::Chm => "chm"
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, FromRow)]
pub struct GeosFile {
    pub file_id: i32,
    pub root_path: PathBuf,
    pub product: GeosProduct,
    pub filedate: NaiveDateTime,
    pub levels: GeosLevels,
    pub data_type: GeosDataType,
}

impl GeosFile {
    /// Returns the number of met files expected per day, based on the configuration
    /// 
    /// Will error if the frequency specified in the configuration does not divide evenly
    /// into a day (e.g. if the files are provided every 300 minutes)
    fn num_expected_daily_files(cfg: &config::DownloadConfig) -> anyhow::Result<i64> {
        if 1440 % cfg.file_freq_min != 0 {
            let remainder = 1440 % cfg.file_freq_min;
            let msg = format!("A met configuration has a file frequency that does not evenly divide per day, remaining minutes were {remainder} ({cfg})");
            return Err(anyhow::Error::msg(msg))
        }

        Ok(1440 / cfg.file_freq_min)
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
    pub async fn get_last_complete_date_for_config(conn: &mut MySqlConn, cfg: &config::DownloadConfig) -> anyhow::Result<Option<NaiveDate>> {
        let n_expected = Self::num_expected_daily_files(cfg)?;

        trace!("Querying most recent complete date ({n_expected} files) for {}, {}, {}", cfg.levels, cfg.data_type, cfg.product);
        let this_max_date = sqlx::query!(
            r#"SELECT MAX(tbl.date) as max_date
                FROM (
                    SELECT DATE(filedate) AS date,COUNT(filedate) AS count
                    FROM GeosFiles
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

    /// Given a list of reanalysis download configurations, find the last date where all the data sets were downloaded.
    /// 
    /// This is meant for finding the last date that the priors can be generated for. To figure out the last date a
    /// specific reanalysis data set was downloaded for, use [`get_last_complete_date_for_config`]
    /// 
    /// # Returns
    /// The most recent date for which all the datasets specified by `cfgs` are complete. There can be several cases:
    /// 
    ///     1. If none of those datasets have any data downloaded, returns `None` 
    ///     2. If some (but not all) of those datasets have data downloaded, still returns `None` but prints a warning
    ///     3. If all those datasets have data downloaded, but the end dates differ, returns the earliest end date and
    ///        prints a warning.
    ///     4. If all those datasets have data downloaded through the same date, returns that date.
    /// 
    /// This will return an `Err` if the database query fails.
    pub async fn get_last_complete_date_for_config_set(conn: &mut MySqlConn, cfgs: &[config::DownloadConfig]) -> anyhow::Result<Option<NaiveDate>> {
        let mut dates = vec![];
        for cfg in cfgs {
            if let Some(d) = Self::get_last_complete_date_for_config(conn, cfg).await? {
                debug!("Last complete day for {cfg} was {d}");
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

        // Case 3: not all of the dates are the same so issue a warning and return the earliest date
        // We know that there is at least one date so this is okay to unwrap
        let earliest_date = dates.iter().min().unwrap().to_owned();
        if !dates.iter().all(|&d| d == earliest_date) {
            warn!("While trying to identify the last complete date of meteorology, the required products had different final dates, so defaulting to the earliest.");
            return Ok(Some(earliest_date));
        }
        
        // Case 4: all products have the same last time.
        Ok(Some(earliest_date))
    }

    /// Get the most recent date that has a complete set of GEOS files
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `met_levels` - which 3D levels to require for the meteorology, [`GeosLevels::Eta`] or [`GeosLevels::Pres`]
    /// * `geos_product` - which product to search for
    /// * `req_chm` - whether to require chemistry files.
    /// 
    /// # Returns
    /// If a day with complete GEOS files is found, then that date is returned. If there is no such day,
    /// `None` is returned.
    /// 
    /// # Errors
    /// Returns an `Err` if the database query fails for any reason.
    #[deprecated(since = "d9d77ed", note="Replace with `get_last_complete_date_for_config_set`")]
    pub async fn get_last_complete_date(conn: &mut MySqlConn, met_levels: GeosLevels, geos_product: GeosProduct, req_chm: bool) -> anyhow::Result<Option<NaiveDate>> {
        // We find the date that has a complete set of 8 3D met files, 2D met files, and (optionally)
        // 3D chemistry files by making subqueries for each file type where we count the number of 
        // files for each date and join the subqueries on their dates. We limit the result to where
        // there are the right number of files and take the max date. Note: for the chemistry files
        // ONLY the WHERE clause uses >= instead of = to allow for chemistry files to be present if
        // the amount wanted is 0.
        let product_string = geos_product.to_string();
        let max_date = sqlx::query!(
            r#"SELECT MAX(met3d.date) as max_date
               FROM (
                   SELECT DATE(filedate) AS date,COUNT(filedate) AS count
                   FROM GeosFiles 
                   WHERE levels = ? AND data_type = "met" AND product = ?
                   GROUP BY DATE(filedate)
               ) AS met3d 
               INNER JOIN (
                   SELECT DATE(filedate) AS date,COUNT(filedate) AS count
                   FROM GeosFiles 
                   WHERE levels = "surf" AND data_type = "met" AND product = ?
                   GROUP BY DATE(filedate)
               ) AS met2d
               ON met3d.date = met2d.date 
               INNER JOIN (
                   SELECT DATE(filedate) AS date,COUNT(filedate) AS count
                   FROM GeosFiles
                   WHERE levels = "eta" AND data_type = "chm" AND product = ?
                   GROUP BY DATE(filedate)
               ) AS chm3d
               ON met3d.date = chm3d.date
               WHERE met3d.count = ? AND met2d.count = ? AND chm3d.count >= ?"#,
            met_levels.to_string(),
            &product_string,
            &product_string,
            &product_string,
            REQ_FILES_PER_DAY,
            REQ_FILES_PER_DAY,
            if req_chm { REQ_FILES_PER_DAY } else { 0 }
        ).fetch_one(conn)
        .await?
        .max_date;

        return Ok(max_date)
    }

    /// Returns whether a given date is complete, incomplete, or wholly missing for a given reanalysis download configuration
    /// 
    /// Will return an `Err` if the database query fails.
    pub async fn is_date_complete_for_config(conn: &mut MySqlConn, date: NaiveDate, cfg: &config::DownloadConfig) -> anyhow::Result<GeosDayState> {
        let n_expected = Self::num_expected_daily_files(cfg)?;
        let n_found = sqlx::query!(
            r#"SELECT COUNT(filedate) as count FROM GeosFiles
               WHERE DATE(filedate) = ? and levels = ? AND data_type = ? AND product = ?"#,
            date,
            cfg.levels.to_string(),
            cfg.data_type.to_string(),
            cfg.product.to_string()
        ).fetch_one(conn)
        .await?
        .count;

        if n_found == 0 {
            Ok(GeosDayState::Missing)
        }else if n_found < n_expected {
            Ok(GeosDayState::Incomplete)
        }else{
            Ok(GeosDayState::Complete)
        }
    }

    #[deprecated(since="d9d77ed", note="Replace with `is_date_complete_for_config`")]
    pub async fn is_date_complete(conn: &mut MySqlConn, date: NaiveDate, met_levels: GeosLevels, geos_product: GeosProduct, req_chm: bool) -> anyhow::Result<GeosDayState> {
        let mut n_files = 0;
        n_files += sqlx::query!(
            r#"SELECT COUNT(filedate) as count FROM GeosFiles
               WHERE DATE(filedate) = ? AND levels = ? AND data_type = "met" AND product = ? "#,
            date,
            met_levels.to_string(),
            geos_product.to_string()
        ).fetch_one(&mut *conn)
        .await?
        .count;

        n_files += sqlx::query!(
            r#"SELECT COUNT(filedate) as count FROM GeosFiles
               WHERE DATE(filedate) = ? AND levels = "surf" AND data_type = "met" AND product = ? "#,
            date,
            geos_product.to_string()
        ).fetch_one(&mut *conn)
        .await?
        .count;

        n_files += if req_chm {
            sqlx::query!(
                r#"SELECT COUNT(filedate) as count FROM GeosFiles
                   WHERE DATE(filedate) = ? AND levels = "eta" AND data_type = "chm" AND product = ? "#,
                date,
                geos_product.to_string()
            ).fetch_one(conn)
            .await?
            .count
        }else{
            0
        };

        let n_req = if req_chm { 3 * REQ_FILES_PER_DAY } else { 2 * REQ_FILES_PER_DAY };

        if n_files == 0 {
            return Ok(GeosDayState::Missing)
        }else if n_files < n_req {
            return Ok(GeosDayState::Incomplete)
        }else{
            return Ok(GeosDayState::Complete)
        }
    }

    /// Add a new GEOS file to the database
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
    /// [`GeosFile::add_geos_file_infer_date`] if the file date must be retrieved from the file name.
    pub async fn add_geos_file(conn: &mut MySqlConn, file: &Path, datetime: NaiveDateTime, download_cfg: &config::DownloadConfig) -> anyhow::Result<()> {
        if !file.exists() {
            return Err(anyhow::Error::msg(format!("Not adding nonexistant met file to database: {}", file.display())));
        }else if !file.is_absolute() {
            // I decided to make this a panic rather than a recoverable error because this should be something
            // in the program design, not a runtime issue.
            panic!("Given file path ({}) must be absolute", file.display());
        }

        sqlx::query!(
            "INSERT INTO GeosFiles (file_path, filedate, product, levels, data_type) VALUES (?, ?, ?, ?, ?)",
            file.to_str().ok_or_else(|| anyhow::Error::msg(format!("Unable to convert path to UTF-8 string: {}", file.display())))?,
            datetime,
            download_cfg.product,
            download_cfg.levels,
            download_cfg.data_type
        ).execute(conn)
        .await?;
        
        Ok(())
    }

    /// Get the date of a file from its file name. File name must contain at least up to minutes.
    /// 
    /// Returns the file datetime, or an `Err` if it could not get the chrono format for the file names
    /// or if the parsing of the file name fails.
    fn date_from_filename(file: &Path, download_cfg: &config::DownloadConfig) -> anyhow::Result<NaiveDateTime> {
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

    /// Similar to [`GeosFile::add_geos_file`], but infers the date & time from the file name.
    /// 
    /// Note that the file's basename must match the time format pattern in the download config, and
    /// must contain time components at least up to minutes. All other behavior follows
    /// [`GeosFile::add_geos_file`] including panics - `file` must be an absolute path.
    pub async fn add_geos_file_infer_date(conn: &mut MySqlConn, file: &Path, download_cfg: &config::DownloadConfig) -> anyhow::Result<()> {
        let datetime = Self::date_from_filename(file, download_cfg)?;
        Self::add_geos_file(conn, file, datetime, download_cfg).await
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
    pub async fn file_exists_by_type(conn: &mut MySqlConn, file: &Path, file_cfg: &config::DownloadConfig) -> anyhow::Result<bool> {
        let datetime = Self::date_from_filename(file, file_cfg)?;

        let n = sqlx::query!(
            r#"SELECT COUNT(*) as count FROM GeosFiles
               WHERE filedate = ? AND product = ? AND levels = ? and data_type = ?"#,
            datetime,
            file_cfg.product,
            file_cfg.levels,
            file_cfg.data_type
        ).fetch_one(conn)
        .await?
        .count;

        Ok(n > 0)
    }
}