//! Interface to the standard site information tables
use std::{collections::HashMap, str::FromStr, borrow::Cow, fmt::Display, path::{PathBuf, Path}};

use anyhow::{self, Context};
use chrono::{NaiveDate, Duration};
use itertools::Itertools;
use log::error;
use serde::Serialize;
use sqlx::{self, FromRow, Type, Connection};
use tabled::Tabled;

use crate::{utils, stdsitejobs::StdSiteJob, jobs::Job};

use super::MySqlConn;

/// An enum describing the type of site
#[derive(Debug, Type, Clone, Copy)]
#[repr(i8)]  // NB: SQL enums start at 1
pub enum SiteType {
    /// This site is neither TCCON nor an EM27. (`i8` value = `0`.)
    Unknown = 1,
    /// This is a TCCON site. (`i8` value = `1`.)
    TCCON = 2,
    /// This is a permanent EM27 site. (`i8` value = `2`.)
    EM27 = 3
}

impl From<String> for SiteType {
    fn from(s: String) -> Self {
        return Self::from_str(&s).unwrap_or(Self::Unknown)
    }
}

impl FromStr for SiteType {
    type Err = anyhow::Error;
    /// Convert a string to [`SiteType`]
    /// 
    /// Matches the strings "tccon" or "em27" (case insensitive).
    /// Anything else returns `SiteType::Unknown`.
    /// 
    /// # Notes
    /// This will never return an `Err`, so can be safely unwrapped.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tccon" => Ok(Self::TCCON),
            "em27" => Ok(Self::EM27),
            _ => Ok(Self::Unknown)
        }
    }
}

impl Display for SiteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SiteType::Unknown => write!(f, "UNKNOWN"),
            SiteType::TCCON => write!(f, "TCCON"),
            SiteType::EM27 => write!(f, "EM27"),
        }
    }
}

#[derive(Debug, Type, Clone, Copy)]
#[repr(i8)]  // NB: SQL enums start at 1
pub enum StdOutputStructure {
    FlatModVmr = 1,
    FlatAll = 2,
    FlatAllMapNc = 3,
    TreeModVmr = 4,
    TreeAll = 5,
    TreeAllMapNc = 6
}

impl FromStr for StdOutputStructure {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "flatmodvmr" => Ok(Self::FlatModVmr),
            "flatall" => Ok(Self::FlatAll),
            "flatallmapnc" => Ok(Self::FlatAllMapNc),
            "treemodvmr" => Ok(Self::TreeModVmr),
            "treeall" => Ok(Self::TreeAll),
            "treeallmapnc" => Ok(Self::TreeAllMapNc),
            _ => anyhow::bail!("Invalid string for StdOutputStructure: {s}")
        }
    }
}

impl Default for StdOutputStructure {
    fn default() -> Self {
        Self::TreeAll
    }
}

impl Display for StdOutputStructure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StdOutputStructure::FlatModVmr => write!(f, "FlatModVmr"),
            StdOutputStructure::FlatAll => write!(f, "FlatAll"),
            StdOutputStructure::FlatAllMapNc => write!(f, "FlatAllMapNc"),
            StdOutputStructure::TreeModVmr => write!(f, "TreeModVmr"),
            StdOutputStructure::TreeAll => write!(f, "TreeAll"),
            StdOutputStructure::TreeAllMapNc => write!(f, "TreeAllMapNc"),
        }
    }
}

impl StdOutputStructure {
    pub fn make_std_site_tarball(&self, root_save_dir: &Path, site_id: &str, job: &Job) -> anyhow::Result<PathBuf> {
        if !root_save_dir.is_dir() {
            anyhow::bail!("root_save_dir must be a preexisting directory");
        }

        let site_save_dir = root_save_dir.join(site_id);
        if !site_save_dir.exists() {
            std::fs::create_dir(&site_save_dir)
                .with_context(|| format!("Error occurred while trying to create standard site save directory for site {site_id}"))?;
        }

        let tarball = site_save_dir.join(format!("{site_id}_ggg_inputs_{}.tgz", job.start_date.format("%Y%m%d")));

        let files_for_tarball = self.get_files_for_tarball(job, Some(site_id))?;

        // Clever combination of tar archive builder and gzip compressor taken from
        // https://stackoverflow.com/a/46521163
        let tgz_file = std::fs::File::create(&tarball)
            .with_context(|| format!("Error occurred trying to create the initial .tgz file for job {}", job.job_id))?;
        let encoder = flate2::write::GzEncoder::new(tgz_file, flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);

        for (src, dest) in files_for_tarball {
            archive.append_path_with_name(src, dest)?;
        }

        let encoder = archive.into_inner()
            .with_context(|| format!("Error occurred while trying to finalize tar archive for job {}", job.job_id))?;

        // Unsure if this is needed, but doesn't seem to hurt
        encoder.finish()
            .with_context(|| format!("Error occurred while trying to finalize the gzip compression for job {}", job.job_id))?;
        
        Ok(tarball)
    }

    pub fn get_files_for_tarball(&self, job: &Job, site_id: Option<&str>) -> anyhow::Result<Vec<(PathBuf, PathBuf)>> {
        let site_ids = if let Some(sid) = site_id {
            vec![sid]
        } else {
            job.site_id.iter().map(|s| s.as_str()).collect_vec()
        };
        match self {
            StdOutputStructure::FlatModVmr => {
                Self::get_files(
                    Self::add_files_flat,
                    job,
                    &site_ids,
                    true, true, false, false
                )
            },
            StdOutputStructure::FlatAll => {
                Self::get_files(
                    Self::add_files_flat,
                    job,
                    &site_ids,
                    true, true, true, false
                )
            },
            StdOutputStructure::FlatAllMapNc => {
                Self::get_files(
                    Self::add_files_flat,
                    job,
                    &site_ids,
                    true, true, false, true
                )
            },
            StdOutputStructure::TreeModVmr => {
                Self::get_files(
                    Self::add_files_in_tree,
                    job,
                    &site_ids,
                    true, true, false, false
                )
            },
            StdOutputStructure::TreeAll => {
                Self::get_files(
                    Self::add_files_in_tree,
                    job,
                    &site_ids,
                    true, true, true, false
                )
            },
            StdOutputStructure::TreeAllMapNc => {
                Self::get_files(
                    Self::add_files_in_tree,
                    job,
                    &site_ids,
                    true, true, false, true
                )
            },
        }
    }

    fn get_files<F>(getter_fxn: F, job: &Job, site_ids: &[&str], mod_files: bool, vmr_files: bool, map_files: bool, map_nc_files: bool) -> anyhow::Result<Vec<(PathBuf, PathBuf)>>
    where F: Fn(&mut Vec<(PathBuf, PathBuf)>, glob::Paths) -> anyhow::Result<()> 
    {
        let mut files = vec![];
        for &sid in site_ids {
            if mod_files {
                let mod_dir = job.mod_output_dir(sid);
                if !mod_dir.exists() {
                    anyhow::bail!(".mod output directory does not exist for site {sid} in job #{}", job.job_id);
                }
                let mod_glob = glob::glob(&mod_dir.join("*.mod").to_string_lossy())
                    .context("Error occurred while trying to make the glob pattern for .mod files")?;

                getter_fxn(&mut files, mod_glob)
                    .context("Error occurred while trying to add .mod files from glob pattern")?;
            }

            if vmr_files {
                let vmr_dir = job.vmr_output_dir(sid);
                if !vmr_dir.exists() {
                    anyhow::bail!(".vmr output directory does not exist for site {sid} in job #{}", job.job_id);
                }
                let vmr_glob = glob::glob(&vmr_dir.join("*.vmr").to_string_lossy())
                    .context("Error occurred while trying to make the glob pattern for .vmr files")?;

                getter_fxn(&mut files, vmr_glob)
                    .context("Error occurred while trying to add .vmr files from glob pattern")?;
            }

            if map_files {
                let map_dir = job.map_output_dir(sid);
                if !map_dir.exists() {
                    anyhow::bail!(".map output directory does not exist for site {sid} in job #{}", job.job_id);
                }
                let map_glob = glob::glob(&map_dir.join("*.map").to_string_lossy())
                    .context("Error occurred while trying to make the glob pattern for .map files")?;

                getter_fxn(&mut files, map_glob)
                    .context("Error occurred while trying to add .map files from glob pattern")?;
            }

            if map_nc_files {
                let map_dir = job.mod_output_dir(sid);
                if !map_dir.exists() {
                    anyhow::bail!(".mod output directory does not exist for site {sid} in job #{}", job.job_id);
                }
                let map_glob = glob::glob(&map_dir.join("*.map.nc").to_string_lossy())
                    .context("Error occurred while trying to make the glob pattern for .mod files")?;

                getter_fxn(&mut files, map_glob)
                    .context("Error occurred while trying to add .map.nc files from glob pattern")?;
            }
        }
        Ok(files)
    }

    fn add_files_in_tree(files: &mut Vec<(PathBuf, PathBuf)>, glob_entries: glob::Paths) -> anyhow::Result<()> {
        for entry in glob_entries {
            let src_path = entry?.canonicalize()?;
            let components = src_path.components()
                .rev()
                .take(5)
                .collect_vec();

            // We're assuming a directory structure like `jobN/fpit/site_id/vertical/*.mod`, hence taking the last five
            // components will get us what we want
            if components.len() != 5 {
                anyhow::bail!("Fewer than expected number of path components (expected 5, got {})", components.len());
            }

            let dest_path = components.into_iter()
                .fold(PathBuf::new(), |full_path, comp| full_path.join(comp));

            files.push((src_path, dest_path));
        }
        Ok(())
    }

    fn add_files_flat(files: &mut Vec<(PathBuf, PathBuf)>, glob_entries: glob::Paths) -> anyhow::Result<()> {
        for entry in glob_entries {
            let src_path = entry?;
            let basename = src_path.file_name()
                .map(PathBuf::from)
                .ok_or_else(|| anyhow::anyhow!("Could not get file name of {}", src_path.display()))?;

            files.push((src_path, basename));
        }
        Ok(())
    }
}


/// A struct representing a standard (permanent) TCCON, EM27, or other site.
#[derive(Debug, Tabled)]
pub struct StdSite {
    /// **\[primary key\]** the primary key in the SQL StdSiteList table.
    pub id: i32,
    /// The (generally) two-character site ID for this site
    pub site_id: String,
    /// The long form name for this site
    pub name: String,
    /// Whether this is a TCCON, EM27, or other site type.
    pub site_type: SiteType,
    /// How this site expects the output tarballs' file structure to be organized
    pub output_structure: StdOutputStructure
}

impl TryFrom<QStdSite> for StdSite {
    type Error = anyhow::Error;

    fn try_from(value: QStdSite) -> Result<Self, Self::Error> {
        let output_structure = StdOutputStructure::from_str(&value.output_structure)?;
        Ok(Self { id: value.id, site_id: value.site_id, name: value.name, site_type: SiteType::from(value.site_type), output_structure })
        
    }
}

impl StdSite {
    pub async fn primary_key_to_site_id(conn: &mut MySqlConn, site_prim_key: i32) -> anyhow::Result<String> {
        let site = sqlx::query_as!(
            QStdSite,
            "SELECT * FROM StdSiteList WHERE id = ?",
            site_prim_key
        ).fetch_one(conn)
        .await?;

        return Ok(site.site_id)
    }

    pub async fn site_id_to_primary_key(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<i32> {
        let site = sqlx::query_as!(
            QStdSite,
            "SELECT * FROM StdSiteList WHERE site_id = ?",
            site_id
        ).fetch_one(conn)
        .await?;

        return Ok(site.id)
    }

    /// Returns a list of currently defined site IDs in alphabetical order
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `site_type` - optionally, which site type to return. If `None`, all sites are returned regardless of type.
    pub async fn get_site_ids(conn: &mut MySqlConn, site_type: Option<SiteType>) -> anyhow::Result<Vec<String>> {
        let sites = if let Some(stype) = site_type {
            sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList WHERE site_type = ? ORDER BY site_id",
                stype
            ).fetch_all(conn)
            .await?
        }else{
            sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList ORDER BY site_id"
            ).fetch_all(conn)
            .await?
        };
        

        let site_ids = sites.into_iter().map(|s| s.site_id).collect();
        return Ok(site_ids)
    }

    pub async fn create(conn: &mut MySqlConn, site_id: &str, site_name: &str, site_type: SiteType) -> anyhow::Result<Self> {
        let q = sqlx::query!(
            "INSERT INTO StdSiteList(site_id, name, site_type) VALUES (?, ?, ?)",
            site_id,
            site_name,
            site_type
        ).execute(&mut *conn)
        .await?;

        let new_id = q.last_insert_id();

        let new_site = sqlx::query_as!(
            QStdSite,
            "SELECT * FROM StdSiteList WHERE id = ?",
            new_id
        ).fetch_one(conn)
        .await?;
        
        Self::try_from(new_site)
    }

    /// Retrieve a list of standard sites, optionally filtered by site type
    pub async fn get_by_type(conn: &mut MySqlConn, site_type: Option<SiteType>) -> anyhow::Result<Vec<Self>> {
        let qsites = if let Some(stype) = site_type {
            sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList WHERE site_type = ? ORDER BY site_id",
                stype
            ).fetch_all(conn).await?
        } else {
            sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList ORDER BY site_id"
            ).fetch_all(conn).await?
        };

        let sites = qsites.into_iter()
            .map(|q| q.try_into())
            .try_collect();
        sites
    }

    pub async fn get_by_site_id(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<Option<Self>> {
        let site = sqlx::query_as!(
            QStdSite,
            "SELECT * FROM StdSiteList WHERE site_id = ?",
            site_id
        ).fetch_optional(conn)
        .await?
        .map(StdSite::try_from)
        .transpose()?;

        Ok(site)
    }

    pub async fn set_name(&mut self, conn: &mut MySqlConn, site_name: String) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteList SET name = ? WHERE id = ?",
            site_name,
            self.id
        ).execute(conn)
        .await?;

        self.name = site_name;
        Ok(())
    }

    pub async fn set_type(&mut self, conn: &mut MySqlConn, site_type: SiteType) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteList SET site_type = ? WHERE id = ?",
            site_type,
            self.id
        ).execute(conn)
        .await?;

        self.site_type = site_type;
        Ok(())
    }

    pub async fn set_output_structure(&mut self, conn: &mut MySqlConn, output_structure: StdOutputStructure) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteList SET output_structure = ? WHERE id = ?",
            output_structure,
            self.id
        ).execute(conn)
        .await?;

        self.output_structure = output_structure;
        Ok(())
    }
}


/// An internal query struct that represents the result of a SQL query on the StdSiteList table.
/// 
/// This should be converted to a [`StdSite`] instance for any public-facing functions.
#[derive(Debug, FromRow)]
struct QStdSite {
    id: i32,
    site_id: String,
    name: String,
    site_type: String,
    output_structure: String,
}

/// A structure representing a single information row for a standard site
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct SiteInfo {
    #[serde(skip)]
    pub id: i32,
    /// The two-character site ID. May be `None` if a row's site foreign key failed 
    /// to match a site in the StdSiteList table.
    pub site_id: Option<String>,
    #[serde(skip)]
    site: i32,
    /// The long name of this site. May be `None` for the same reason as `site_id`.
    pub name: Option<String>,
    /// The human-readable location of this site (e.g. "Park Falls, WI, USA")
    pub location: String,
    /// The latitude of this site, south is negative.
    pub latitude: f32,
    /// The longitude of this site, west is negative.
    pub longitude: f32,
    /// The first date when this site was operational at this location.
    pub start_date: NaiveDate,
    /// The last date (exclusive) when this site was operational at this location.
    /// `None` indicates that the site remains operational here.
    pub end_date: Option<NaiveDate>,
    /// A comment to describe any special considerations with this site.
    pub comment: Option<String>
}

impl Tabled for SiteInfo {
    const LENGTH: usize = 6;

    fn fields(&self) -> Vec<std::borrow::Cow<'_, str>> {
        let start_date = self.start_date.to_string();
        let end_date = self.end_date
            .map(|d| d.to_string())
            .unwrap_or_else(|| "".to_string());
        let comment = self.comment
            .as_deref()
            .unwrap_or("");

        vec![
            Cow::from(start_date),
            Cow::from(end_date),
            Cow::from(self.location.clone()),
            Cow::from(format!("{:.3}", self.longitude)),
            Cow::from(format!("{:.3}", self.latitude)),
            Cow::from(comment)
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            Cow::from("start_date"),
            Cow::from("end_date"),
            Cow::from("location"),
            Cow::from("longitude"),
            Cow::from("latitude"),
            Cow::from("comment")
        ]
    }
}

impl SiteInfo {
    /// Return the standard site table entry associated with this site information.
    /// 
    /// If a standard site cannot be found, the returned result will be and `Err`.
    pub async fn get_std_site(&self, conn: &mut MySqlConn) -> anyhow::Result<StdSite> {
        let result = sqlx::query_as!(
                QStdSite,
                "SELECT * FROM StdSiteList WHERE id = ?",
                self.site
            ).fetch_one(conn)
            .await?;

        StdSite::try_from(result)
    }

    /// Create a JSON string representing a list of SiteInfo instances
    /// 
    /// # Parameters
    /// 
    /// * `infos` - a slice of `SiteInfo` instances (e.g. returned by [`SiteInfo::get_all_site_info`])
    /// * `pretty` - whether to format the JSON in pretty style or not
    /// 
    /// # Returns
    /// 
    /// A JSON string representing a sequence of `SiteInfo` instances, with each one serialized directly
    /// to JSON.
    /// 
    /// # See also
    /// 
    /// * [`SiteInfo::to_grouped_json`]
    /// 
    /// # Errors
    /// 
    /// Returns an `Err` if the serialization failed for any reason.
    pub fn to_flat_json(infos: &[SiteInfo], pretty: bool) -> anyhow::Result<String> {
        if pretty {
            Ok(serde_json::to_string_pretty(infos)?)
        }else{
            Ok(serde_json::to_string(infos)?)
        }
    }

    /// Create a JSON string representing a list of SiteInfo instances
    /// 
    /// Unlike [`SiteInfo::to_flat_json`], the JSON returned here will be a map with one
    /// entry per site ID. Each time period listed for that site will be given in a list of
    /// sub-maps. If the site's name or location changes in different time periods, then only
    /// the values in the first time period are retained.
    /// 
    /// # Parameters
    /// 
    /// * `infos` - a slice of `SiteInfo` instances (e.g. returned by [`SiteInfo::get_all_site_info`])
    /// * `pretty` - whether to format the JSON in pretty style or not
    /// 
    /// # See also
    /// 
    /// * [`SiteInfo::to_flat_json`]
    /// 
    /// # Errors
    /// 
    /// Returns an `Err` if the serialization failed for any reason.
    pub fn to_grouped_json(infos: &[SiteInfo], pretty: bool) -> anyhow::Result<String> {
        #[derive(Debug, Serialize)]
        struct SiteTimePeriod {
            latitude: f32,
            longitude: f32,
            start_date: NaiveDate,
            end_date: Option<NaiveDate>,
            comment: Option<String>
        }

        #[derive(Debug, Serialize)]
        struct Site {
            name: String,
            location: String,
            time_periods: Vec<SiteTimePeriod>
        }

        fn info_to_tp(s: &SiteInfo) -> SiteTimePeriod {
            SiteTimePeriod { latitude: s.latitude, longitude: s.longitude, start_date: s.start_date, end_date: s.end_date, comment: s.comment.clone() }
        }

        let mut json_map = HashMap::new();
        let mut i_no_id = 0;
        for this_info in infos {
            let sid = if let Some(x) = &this_info.site_id {
                x.clone()
            }else{
                let x = format!("no_id_{i_no_id}");
                i_no_id += 1;
                x
            };


            if !json_map.contains_key(&sid) {
                json_map.insert(
                    sid,
                    Site {
                        name: this_info.name.as_ref().map(|name| name.to_string()).unwrap_or_default(),
                        location: this_info.location.clone(),
                        time_periods: vec![info_to_tp(this_info)]
                    }
                );
            }else{
                // TODO: warn if the name & location do not match
                let new_tp = info_to_tp(this_info);
                json_map.get_mut(&sid).unwrap().time_periods.push(new_tp);
            }
        }
        
        if pretty {
            return Ok(serde_json::to_string_pretty(&json_map)?)
        }else{
            return Ok(serde_json::to_string(&json_map)?);
        }
    }

    /// Get the current site information for a given site, i.e. the information with the most recent start date
    /// 
    /// # Parameters
    /// 
    /// * `pool` - the MySQL pool or connection object to perform the query with
    /// * `site_id` - the two letter site ID of the site to query, e.g. "pa"
    pub async fn get_most_recent_site_location(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<SiteInfo> {
        let result = sqlx::query_as!(
                SiteInfo, 
                "SELECT * FROM v_StdSiteInfo WHERE site_id = ? ORDER BY start_date DESC LIMIT 1",
                site_id
            ).fetch_one(conn)
            .await?;
    
        Ok(result)
    }

    pub async fn get_all_site_info(pool: &mut MySqlConn) -> anyhow::Result<Vec<SiteInfo>> {
        let result = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo"
        ).fetch_all(pool)
        .await?;

        return Ok(result)
    }

    /// Get the information about standard sites for a given date.
    /// 
    /// # Parameters
    /// 
    /// * `pool` - the MySQL pool or connection object to perform the query with
    /// * `date` - the date to get site information for.
    /// * `active` - determines whether to include sites that are not active on the requested date.
    ///   If this is `true`, then only sites whose start and end dates bracket `date` are included.
    ///   If this is `false`, then all sites are included once, and sites whose start and end dates
    ///   do not bracket `date` will use the information closest in time (forward or back).
    /// 
    /// # Notes
    /// 
    /// Whether `active` is `true` or `false`, if a site has more than 1 instance of information that
    /// brackets the given `date`, the one with the most recent start date will be returned.
    pub async fn get_site_info_for_date(conn: &mut MySqlConn, date: NaiveDate, active: bool) -> anyhow::Result<Vec<SiteInfo>> {
        if active {
            let result = sqlx::query_as!(
                SiteInfo,
                "SELECT * FROM v_StdSiteInfo WHERE start_date <= ? AND (end_date IS NULL OR end_date > ?)",
                date,
                date
            ).fetch_all(conn)
            .await?;

            let mut sites = HashMap::new();
            let mut infos = vec![];
            for this_info in result.iter() {
                if !sites.contains_key(&this_info.site) {
                    sites.insert(this_info.site, infos.len());
                    infos.push(this_info.clone())
                }else{
                    let oidx = *sites.get(&this_info.site).unwrap();
                    if this_info.start_date > infos[oidx].start_date {
                        infos[oidx] = this_info.clone();
                    }
                }
            }

            return Ok(infos)
        }else{
            let result = sqlx::query_as!(
                SiteInfo,
                "SELECT * FROM v_StdSiteInfo",
            ).fetch_all(conn)
            .await?;

            let mut final_site = HashMap::new();
            let mut site_order = vec![];
            for (idx, info) in result.iter().enumerate() {
                let sid = info.site;
                if !final_site.contains_key(&sid) {
                    final_site.insert(sid, idx);
                    site_order.push(sid); // each time we add a new site, record the order so that the return is deterministic
                }else{
                    // Choose between two site infos to decide which one gives the best match for this site. 
                    // If one's start/end dates bracket the requested date and the other doesn't use the one
                    // that brackets it. If both bracket the date, then select the one with the later start date.
                    // Otherwise, choose the one closer in time to the date.
                    let cidx = *final_site.get(&sid).unwrap();
                    let curr = &result[cidx];
                    let curr_brackets = info_brackets_date(date, curr.start_date, curr.end_date);
                    let new_brackets = info_brackets_date(date, info.start_date, info.end_date);

                    if curr_brackets && !new_brackets {
                        // do nothing, keep current info
                    }else if new_brackets && !curr_brackets {
                        // new info brackets the date, use it instead
                        final_site.insert(sid, idx);
                    }else if curr_brackets && new_brackets && curr.start_date >= info.start_date {
                        // do nothing, keep current info
                    }else if curr_brackets && new_brackets {
                        // the new info has a later start time, use it instead
                        final_site.insert(sid, idx);
                    }else if new_info_closer_in_time(date, curr, info){
                        // use whichever one is closer in time
                        final_site.insert(sid, idx);
                    }
                }
            }

            let mut infos = vec![];
            for idx in site_order {
                let ridx = *final_site.get(&idx).unwrap();
                infos.push(result[ridx].clone());
            }
            return Ok(infos)
        }
    }

    /// Get all locations for a given site
    pub async fn get_site_locations(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<Vec<SiteInfo>> {
        let site_infos = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo WHERE site_id = ?",
            site_id
        ).fetch_all(conn)
        .await?;

        Ok(site_infos)
    }

    pub async fn get_site_locations_for_date_range(conn: &mut MySqlConn, site_id: &str, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<Vec<SiteInfo>> {
        let site_infos = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo WHERE site_id = ?",
            site_id
        ).fetch_all(conn)
        .await?
        .into_iter()
        .filter(|info| utils::date_ranges_overlap(Some(start_date), end_date, Some(info.start_date), info.end_date))
        .collect();

        Ok(site_infos)
    }


    pub async fn get_one_site_location_for_date_range(conn: &mut MySqlConn, site_id: &str, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<SiteInfo> {
        let mut site_infos = Self::get_site_locations_for_date_range(conn, site_id, start_date, end_date).await?;
        if site_infos.is_empty() {
            anyhow::bail!("No location defined for site {site_id} between {start_date} and {end_date:?}");
        } else if site_infos.len() > 1 {
            anyhow::bail!("Multiple locations defined for site {site_id} between {start_date} and {end_date:?}");
        } else {
            // length == 1
            Ok(site_infos.pop().unwrap())
        }
    }

    pub async fn verify_info_available_for_site(conn: &mut MySqlConn, site_id: &str) -> anyhow::Result<bool> {
        let n_match = sqlx::query!("SELECT COUNT(*) as count FROM v_StdSiteInfo WHERE site_id = ?", site_id)
            .fetch_one(conn)
            .await?
            .count;

        return Ok(n_match > 0);
    }

    pub async fn fill_null_latlons<T: AsRef<str>>(conn: &mut MySqlConn, site_ids: &[T], lats: &[Option<f32>], lons: &[Option<f32>], start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<(Vec<f32>, Vec<f32>)> {
        if lats.len() != site_ids.len() || lons.len() != site_ids.len() {
            anyhow::bail!("site_ids, lats, and lons must be the same length; got {}, {}, and {}, respectively",
                          site_ids.len(), lats.len(), lons.len());
        }

        let mut out_lats = vec![];
        let mut out_lons = vec![];

        for (idx, sid) in site_ids.iter().enumerate() {
            let y = lats[idx];
            let x = lons[idx];
            if x.is_some() != y.is_some() {
                anyhow::bail!("The lat and lon at index {idx} are not both `None` or both `Some`");
            }

            if x.is_none() {
                let info = Self::get_one_site_location_for_date_range(&mut *conn, sid.as_ref(), start_date, end_date).await?;
                out_lats.push(info.latitude);
                out_lons.push(info.longitude);
            }else{
                out_lats.push(y.unwrap());
                out_lons.push(x.unwrap());
            }
        }

        return Ok((out_lats, out_lons))
    }

    /// Return how many locations are defined for a given site ID in a date range.
    /// 
    /// # Parameters
    /// * `conn` - connection to the MySQL database
    /// * `site_id` - the two-letter site ID for which we are checking locations
    /// * `start_date` - the beginning of the date range to check.
    /// * `end_date` - the end of the date range to check. This can be `None` to indicate
    ///   an open-ended date range. If this is `Some(date)`, then it is exclusive - that is,
    ///   if a site location's start date is the same as this end date, that is *not* considered
    ///   an overlap.
    /// 
    /// # Returns
    /// The number of locations defined for the site with `site_id` in the given date range. If
    /// `site_id` is not a known site ID, the result will be 0.
    /// 
    /// # Errors
    /// Returns an `Err` if the database query fails. 
    pub async fn check_number_locations_in_date_range(conn: &mut MySqlConn, site_id: &str, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<u32> {
        let infos = sqlx::query_as!(
            SiteInfo,
            "SELECT * FROM v_StdSiteInfo WHERE site_id = ?",
            site_id
        ).fetch_all(conn)
        .await?;

        let mut n = 0;
        for info in infos {
            if utils::date_ranges_overlap(Some(start_date), end_date, Some(info.start_date), info.end_date) {
                n += 1;
            }
        }
        
        return Ok(n)
    }

    /// Adds a new entry in the `StdSiteInfo` table for a given range of dates
    /// 
    /// If the new entry overlaps existing entries for the same site in time, then
    /// the existing entries are truncated in time to fit this entry. Should any
    /// existing entrys be completely covered by the new entry, those existing existing
    /// entries are deleted from the table.
    /// 
    /// A start date is always required, as any site will have started observing at a
    /// definite date, and priors cannot be generated infinitely far back in time. An
    /// end date is optional; if not given, the time period will be open-ended (i.e.
    /// will always extend to the current date).
    /// 
    /// When adding the first entry for a given site, location, longitude, and latitude
    /// must all be given. When adding other entries, any or all of these values can be
    /// omitted. Omitted values are copied from the existing entries overlapped in time
    /// by the new entry. If the overlapped entries have different values for an omitted
    /// value, an error is returned.
    /// 
    /// A comment is always optional; comments are *not* copied from extisting overlapped 
    /// entries.
    /// 
    /// If priors were already generated or queued for the site/time period modified,
    /// those rows in the `StdSiteJobs` table will be flagged for regeneration.
    /// 
    /// This is the only method that should be used to add new site information entries
    /// in order to ensure that overlapping entries are not inadvertently created. Hence
    /// there is no public interface to change the start/end dates of a `SiteInfo` instance
    /// directly.
    pub async fn set_site_info_for_dates(
        conn: &mut MySqlConn, 
        site_id: &str, 
        start_date: NaiveDate, 
        end_date: Option<NaiveDate>, 
        location: Option<String>, 
        longitude: Option<f32>, 
        latitude: Option<f32>, 
        comment: Option<&str>
    ) -> anyhow::Result<()> {
        let overlapped_locs = Self::get_site_locations_for_date_range(conn, site_id, start_date, end_date).await?;

        // Get the location string, longitude, and latitude from existing overlapped site information if these
        // values are not provided as inputs. Note that changing the location description or comment does not 
        // require regenerating priors
        let regen_needed = longitude.is_some() || latitude.is_some();

        let location = location
            .map(|loc| Ok(loc))
            .unwrap_or_else(|| {
                get_consistent_value_from_infos(&overlapped_locs, |info| {info.location.clone()}, |a, b| a == b)
            }).with_context(|| "Could not infer location from overlapped site information ranges")?;

        let longitude = longitude
            .map(|lon| Ok(lon))
            .unwrap_or_else(|| {
                get_consistent_value_from_infos(&overlapped_locs, |info| info.longitude, |a, b| (a - b).abs() < 1e-6)
            }).with_context(|| "Could not infer longitude from overlapped site information ranges")?;

        let latitude = latitude
            .map(|lat| Ok(lat))
            .unwrap_or_else(|| {
                get_consistent_value_from_infos(&overlapped_locs, |info| info.latitude, |a, b| (a - b).abs() < 1e-6)
            }).with_context(|| "Could not infer latitude from overlapped site information ranges")?;

        // We do not want to copy comments; we assume that comments are unique to each time period

        // Begin modifying the tables
        let mut trans = conn.begin().await?;

        // Go ahead and create our new info range first; if something later fails, the transaction will roll it back.
        Self::create_from_site_id(
            &mut trans,
            site_id,
            &location,
            latitude,
            longitude,
            start_date,
            end_date,
            comment
        ).await
        .with_context(|| "Error creating new site info range")?;

        // Now modify any overlapped ranges
        for mut oloc in overlapped_locs {
            match utils::DateRangeOverlap::classify(Some(start_date), end_date, Some(oloc.start_date), oloc.end_date) {
                // The old site info is entirely inside the new one, so delete the old one
                utils::DateRangeOverlap::AContainsB => oloc.delete(&mut trans).await?,

                // Split the previous standard site information block and insert the new one in the middle
                utils::DateRangeOverlap::AInsideB => {
                    if let Some(end) = end_date {
                        // It's possible that the new range has no end date, in which case we don't need to create a second
                        // copy of the original info range being split
                        oloc.duplicate_with_new_dates(&mut trans, end, oloc.end_date).await?;
                    }
                    oloc.set_end_date(&mut trans, Some(start_date)).await?;
                },

                // Need to change the start date for the previous info range
                utils::DateRangeOverlap::AEndsInB => {
                    if let Some(end) = end_date {
                        oloc.set_start_date(&mut trans, end).await?;
                    } else {
                        // This really should not happen, but log just in case it does.
                        // Don't want to panic, since that would cause the service to crash.
                        error!("New site info range ends inside a preexisting one, but the new range has no end date! (This should not happen.)");
                    }
                },

                // Need to change the end date for the overlapped info range so that it ends when we start
                utils::DateRangeOverlap::AStartsInB => oloc.set_end_date(&mut trans, Some(start_date)).await?,

                // Complete overwrite - just remove the old one
                utils::DateRangeOverlap::AEqualsB => {
                    oloc.delete(&mut trans).await?;
                },

                // No overlap - nothing to do
                utils::DateRangeOverlap::None => (),
            }
        }

        // Finally, mark any past or pending job as needed regenerated
        // TODO: make sure that when a job runs, it doesn't set the state to "complete" if it was marked as "regen needed", since presumably the job was launched before that regen needed flag was set
        if regen_needed{
            StdSiteJob::set_regen_flag(&mut trans, site_id, start_date, end_date).await?;
        }

        trans.commit().await?;

        Ok(())
    }

    /// Create a new entry in the standard site information table
    /// 
    /// Returns the row ID of the newly created row. Note that `site` must be the 
    /// foreign key to the `StdSiteList` table. To add by the more standard two-character
    /// site ID, see [`SiteInfo::create_from_site_id`].
    async fn create(conn: &mut MySqlConn, site: i32, location: &str, latitude: f32, longitude: f32, start_date: NaiveDate, end_date: Option<NaiveDate>, comment: Option<&str>) -> anyhow::Result<u64> {
        let q = sqlx::query!(
            r#"INSERT INTO StdSiteInfo(site, location, latitude, longitude, start_date, end_date, comment)
               VALUES(?, ?, ?, ?, ?, ?, ?)"#,
            site,
            location,
            latitude,
            longitude,
            start_date,
            end_date,
            comment.unwrap_or("")
        ).execute(&mut *conn)
        .await?;

        // Originally tried to query the newly added row and actually return an instance of `Self`,
        // but the row didn't seem to be in place at this point, so that failed.
        Ok(q.last_insert_id())
    }

    /// Create a new entry in the standard site table with the given two-character site ID
    /// 
    /// This will look up the correct foreign key from the `StdSiteList` table for the given
    /// `site_id`. If `site_id` does not exist in that table, this returns an error.
    async fn create_from_site_id(conn: &mut MySqlConn, site_id: &str, location: &str, latitude: f32, longitude: f32, start_date: NaiveDate, end_date: Option<NaiveDate>, comment: Option<&str>) -> anyhow::Result<u64> {
        let site = sqlx::query!(
            "SELECT id FROM StdSiteList WHERE site_id = ?",
            site_id
        ).fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No site known matching site ID '{site_id}'"))?
        .id;

        Self::create(conn, site, location, latitude, longitude, start_date, end_date, comment).await
    }

    /// Set the location string of this instance and the corresponding row in the database table.
    pub async fn set_location(&mut self, conn: &mut MySqlConn, location: String) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteInfo SET location = ? WHERE id = ?",
            location,
            self.id
        ).execute(conn)
        .await?;

        self.location = location;

        Ok(())
    }

    /// Set the longitude of this instance and the corresponding row in the database table.
    /// 
    /// Longitudes must be between -180 and 360 or this returns an error. Values between
    /// 180 and 360 are rectified to -180 to 0.
    pub async fn set_longitude(&mut self, conn: &mut MySqlConn, longitude: f32) -> anyhow::Result<()> {
        let longitude = if longitude > 180.0 && longitude <= 360.0 {
            longitude - 360.0
        } else if longitude >= -180.0 && longitude <= 180.0 {
            longitude
        } else {
            anyhow::bail!("Longitude must be in the range -180 to +360")
        };

        sqlx::query!(
            "UPDATE StdSiteInfo SET longitude = ? WHERE id = ?",
            longitude,
            self.id
        ).execute(conn)
        .await?;

        self.longitude = longitude;

        Ok(())
    }

    /// Set the latitude of this instance and the corresponding row in the database table.
    /// 
    /// Latitudes must be between -90 and +90 or this returns an error.
    pub async fn set_latitude(&mut self, conn: &mut MySqlConn, latitude: f32) -> anyhow::Result<()> {
        let latitude = if latitude >= -90.0 && latitude <= 90.0 {
            latitude
        } else {
            anyhow::bail!("Latitude must be in the range -90 to +90")
        };

        sqlx::query!(
            "UPDATE StdSiteInfo SET latitude = ? WHERE id = ?",
            latitude,
            self.id
        ).execute(conn)
        .await?;

        self.latitude = latitude;

        Ok(())
    }

    /// Set the start date of this instance and the corresponding row in the database table.
    async fn set_start_date(&mut self, conn: &mut MySqlConn, start_date: NaiveDate) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteInfo SET start_date = ? WHERE id = ?",
            start_date,
            self.id
        ).execute(conn)
        .await?;

        self.start_date = start_date;
        Ok(())
    }

    /// Set the end date of this instance and the corresponding row in the database table.
    async fn set_end_date(&mut self, conn: &mut MySqlConn, end_date: Option<NaiveDate>) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE StdSiteInfo SET end_date = ? WHERE id = ?",
            end_date,
            self.id
        ).execute(conn)
        .await?;

        self.end_date = end_date;
        Ok(())
    }


    /// Create a new entry that has the same attributes as this instance except for the start and end dates.
    /// 
    /// The new entry is also added to the database table.
    async fn duplicate_with_new_dates(&self, conn: &mut MySqlConn, start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<u64> {
        Self::create(
            conn,
            self.site,
            &self.location,
            self.latitude,
            self.longitude,
            start_date,
            end_date, 
            self.comment.as_deref()
        ).await
    }

    /// Delete this entry from the database table.
    /// 
    /// The instance is consumed by this method, as once it is deleted from the table it
    /// is no longer a valid instance.
    pub async fn delete(self, conn: &mut MySqlConn) -> anyhow::Result<()> {
        sqlx::query!(
            "DELETE FROM StdSiteInfo WHERE id = ?",
            self.id
        ).execute(conn)
        .await?;

        Ok(())
    }
}

/// Check if a date range of a `SiteInfo` instance brackets a given date.
fn info_brackets_date(date: NaiveDate, start_date: NaiveDate, end_date: Option<NaiveDate>) -> bool {
    if let Some(end) = end_date {
        return (date >= start_date) && (date < end)
    }else{
        return date >= start_date
    }
}


/// Return `true` if `new` defines a date range closer to `date` than `curr` does.
/// 
/// This assumes that `date` is outside the date range of both `curr` and `new`; it
/// is intended to help identify a `SiteInfo` instance closest to `date` when no
/// other instance is available that actually contains `date` in its date range.
fn new_info_closer_in_time(date: NaiveDate, curr: &SiteInfo, new: &SiteInfo) -> bool {
    let curr_delta = if date < curr.start_date {
        curr.start_date - date
    }else{
        if let Some(end) = curr.end_date { date - end } else { Duration::days(0) }
    };

    let new_delta = if date < new.start_date {
        new.start_date - date
    }else{
        if let Some(end) = new.end_date { date - end } else { Duration::days(0) }
    };

    return new_delta < curr_delta
}

/// Given a slice of `SiteInfo` instances, get a particular value from all of them and return it if it is the same for all instances.
/// 
/// This is a helper function for copying `SiteInfo` values from existing instances to a new instance
/// that overlaps 1 or more existing instances. `getter` is a function that, given a reference to a `SiteInfo`
/// instance, returns an owned version of the desired value from that instance. `is_same` is a function that,
/// given references to two of the values returned by `getter`, returns `true` if both values are the same
/// and `false` otherwise.
/// 
/// This returns an error if either (a) `infos` is an empty slice (meaning there are no existing values to extract)
/// or (b) `infos` returns 2 or more different values.
fn get_consistent_value_from_infos<T, F, G>(infos: &[SiteInfo], getter: F, is_same: G) -> anyhow::Result<T>
where
    F: Fn(&SiteInfo) -> T,
    G: Fn(&T, &T) -> bool,
{
    let mut val: Option<T> = None;
    for info in infos {
        let new_val = getter(info);
        if let Some(v) = &val {
            if !is_same(&v, &new_val) {
                anyhow::bail!("overlapping site info ranges have different values")
            }
        } else {
            val = Some(new_val);
        }
    }

    if let Some(v) = val {
        Ok(v)
    } else {
        anyhow::bail!("no overlapping site info ranges")
    }
    
}