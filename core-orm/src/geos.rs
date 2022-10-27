use std::{path::PathBuf, str::FromStr, fmt::Display};

use chrono::{NaiveDateTime, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::{self, Type, FromRow};

use crate::MySqlConn;

const REQ_FILES_PER_DAY: i64 = 8;

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
}